package dag

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	prefixNode = []byte("n/")
	keyHeads   = []byte("meta/heads")

	ErrNotFound      = errors.New("not found")
	ErrCIDMismatch   = errors.New("cid mismatch")
	ErrMissingParent = errors.New("missing parent")
	ErrDataTooLarge  = errors.New("data too large")
)

const MaxNodeSize = 1 << 20 // 1MB

type CID [32]byte

func (c CID) String() string {
	return hex.EncodeToString(c[:])
}

func (c CID) Short() string {
	return hex.EncodeToString(c[:4])
}

func ParseCID(s string) (CID, error) {
	var c CID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return c, errors.New("invalid cid")
	}
	copy(c[:], b)
	return c, nil
}

type Node struct {
	Data    []byte
	Parents []CID
}

func (n *Node) CID() CID {
	h := sha256.New()
	// Длина данных для однозначности
	binary.Write(h, binary.BigEndian, uint32(len(n.Data)))
	h.Write(n.Data)
	binary.Write(h, binary.BigEndian, uint32(len(n.Parents)))
	for _, p := range n.Parents {
		h.Write(p[:])
	}
	var c CID
	copy(c[:], h.Sum(nil))
	return c
}

func (n *Node) Marshal() []byte {
	size := 4 + len(n.Data) + 4 + len(n.Parents)*32
	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(n.Data)))
	offset += 4
	copy(buf[offset:], n.Data)
	offset += len(n.Data)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(n.Parents)))
	offset += 4
	for _, p := range n.Parents {
		copy(buf[offset:], p[:])
		offset += 32
	}
	return buf
}

func UnmarshalNode(b []byte) (*Node, error) {
	if len(b) < 8 {
		return nil, errors.New("data too short")
	}

	offset := 0
	dataLen := binary.BigEndian.Uint32(b[offset:])
	offset += 4

	if len(b) < int(4+dataLen+4) {
		return nil, errors.New("invalid data length")
	}

	data := make([]byte, dataLen)
	copy(data, b[offset:offset+int(dataLen)])
	offset += int(dataLen)

	parentsCount := binary.BigEndian.Uint32(b[offset:])
	offset += 4

	if len(b) < offset+int(parentsCount)*32 {
		return nil, errors.New("invalid parents length")
	}

	parents := make([]CID, parentsCount)
	for i := range parents {
		copy(parents[i][:], b[offset:offset+32])
		offset += 32
	}

	return &Node{Data: data, Parents: parents}, nil
}

type DAG struct {
	db    *badger.DB
	mu    sync.RWMutex
	heads map[CID]struct{}
}

func Open(path string) (*DAG, error) {
	opts := badger.DefaultOptions(path).
		WithLoggingLevel(badger.ERROR)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	d := &DAG{
		db:    db,
		heads: make(map[CID]struct{}),
	}

	if err := d.loadHeads(); err != nil {
		db.Close()
		return nil, err
	}

	// Валидация heads при старте
	if err := d.validateHeads(); err != nil {
		db.Close()
		return nil, err
	}

	return d, nil
}

func (d *DAG) Close() error {
	return d.db.Close()
}

func (d *DAG) loadHeads() error {
	return d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyHeads)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			for i := 0; i+32 <= len(val); i += 32 {
				var c CID
				copy(c[:], val[i:i+32])
				d.heads[c] = struct{}{}
			}
			return nil
		})
	})
}

func (d *DAG) validateHeads() error {
	valid := make(map[CID]struct{})
	for h := range d.heads {
		if d.has(h) {
			valid[h] = struct{}{}
		}
	}

	if len(valid) != len(d.heads) {
		d.heads = valid
		return d.db.Update(func(txn *badger.Txn) error {
			return d.saveHeads(txn)
		})
	}
	return nil
}

func (d *DAG) saveHeads(txn *badger.Txn) error {
	// Сортируем для детерминизма
	sorted := make([]CID, 0, len(d.heads))
	for c := range d.heads {
		sorted = append(sorted, c)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})

	buf := make([]byte, len(sorted)*32)
	for i, c := range sorted {
		copy(buf[i*32:], c[:])
	}
	return txn.Set(keyHeads, buf)
}

func (d *DAG) has(cid CID) bool {
	err := d.db.View(func(txn *badger.Txn) error {
		key := append(prefixNode, cid[:]...)
		_, err := txn.Get(key)
		return err
	})
	return err == nil
}

func (d *DAG) Append(data []byte) (CID, error) {
	if len(data) > MaxNodeSize {
		return CID{}, ErrDataTooLarge
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	parents := make([]CID, 0, len(d.heads))
	for h := range d.heads {
		parents = append(parents, h)
	}
	// Сортируем parents для детерминированного CID
	sort.Slice(parents, func(i, j int) bool {
		return bytes.Compare(parents[i][:], parents[j][:]) < 0
	})

	node := &Node{Data: data, Parents: parents}
	cid := node.CID()

	err := d.db.Update(func(txn *badger.Txn) error {
		key := append(prefixNode, cid[:]...)

		// Проверяем дубликат
		if _, err := txn.Get(key); err == nil {
			return nil // уже есть
		}

		if err := txn.Set(key, node.Marshal()); err != nil {
			return err
		}

		d.heads = map[CID]struct{}{cid: {}}
		return d.saveHeads(txn)
	})

	if err != nil {
		return CID{}, err
	}
	return cid, nil
}

func (d *DAG) Put(expected CID, node *Node) error {
	if len(node.Data) > MaxNodeSize {
		return ErrDataTooLarge
	}

	// Верификация CID
	if node.CID() != expected {
		return ErrCIDMismatch
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	return d.db.Update(func(txn *badger.Txn) error {
		key := append(prefixNode, expected[:]...)

		// Идемпотентность
		if _, err := txn.Get(key); err == nil {
			return nil
		}

		return txn.Set(key, node.Marshal())
	})
}

func (d *DAG) Get(cid CID) (*Node, error) {
	var node *Node

	err := d.db.View(func(txn *badger.Txn) error {
		key := append(prefixNode, cid[:]...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var err error
			node, err = UnmarshalNode(val)
			return err
		})
	})

	return node, err
}

func (d *DAG) Has(cid CID) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.has(cid)
}

func (d *DAG) Heads() []CID {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make([]CID, 0, len(d.heads))
	for h := range d.heads {
		result = append(result, h)
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i][:], result[j][:]) < 0
	})
	return result
}

func (d *DAG) Merge(heads []CID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Добавляем только существующие
	added := false
	for _, h := range heads {
		if d.has(h) {
			if _, exists := d.heads[h]; !exists {
				d.heads[h] = struct{}{}
				added = true
			}
		}
	}

	if !added {
		return nil
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return d.saveHeads(txn)
	})
}

func (d *DAG) Walk(fn func(cid CID, node *Node) error) error {
	heads := d.Heads() // уже отсортированы

	visited := make(map[CID]struct{})
	var order []CID

	var visit func(cid CID) error
	visit = func(cid CID) error {
		if _, ok := visited[cid]; ok {
			return nil
		}
		visited[cid] = struct{}{}

		node, err := d.Get(cid)
		if err != nil {
			return nil // пропускаем missing
		}

		// Сначала родители
		// Сортируем для детерминизма
		parents := make([]CID, len(node.Parents))
		copy(parents, node.Parents)
		sort.Slice(parents, func(i, j int) bool {
			return bytes.Compare(parents[i][:], parents[j][:]) < 0
		})

		for _, p := range parents {
			if err := visit(p); err != nil {
				return err
			}
		}

		order = append(order, cid)
		return nil
	}

	for _, h := range heads {
		if err := visit(h); err != nil {
			return err
		}
	}

	for _, cid := range order {
		node, _ := d.Get(cid)
		if err := fn(cid, node); err != nil {
			return err
		}
	}

	return nil
}

func (d *DAG) Missing(cids []CID) []CID {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var missing []CID
	for _, c := range cids {
		if !d.has(c) {
			missing = append(missing, c)
		}
	}
	return missing
}

// RunGC запускает сборку мусора Badger
func (d *DAG) RunGC() error {
	return d.db.RunValueLogGC(0.5)
}
