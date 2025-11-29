package evstore

import (
	"bytes"
	"errors"
	"mqtt-http-tunnel/internal/hlc"
	"sort"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

var ErrNotFound = errors.New("not found")

var (
	prefixEvents   = []byte("e/") // e/{TID} → Event
	prefixDAG      = []byte("d/") // d/{CID} → []CID (parents)
	prefixCIDIndex = []byte("i/") // i/{CID} → TID
	keyHeads       = []byte("meta/heads")
	keyHLC         = []byte("meta/hlc")
)

type Store struct {
	db    *badger.DB
	clock *hlc.Clock
	heads map[CID]struct{}
	//
	mu     sync.RWMutex
	logger zerolog.Logger
}

type StoreConfig struct {
	Path   string
	Logger zerolog.Logger
}

func OpenStore(cfg *StoreConfig) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Path).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	clock := hlc.New()
	s := &Store{
		db:     db,
		clock:  clock,
		heads:  make(map[CID]struct{}),
		logger: cfg.Logger.With().Str("component", "store").Logger(),
	}
	if err := s.loadState(); err != nil {
		db.Close()
		return nil, err
	}
	s.logger.Info().Int("heads", len(s.heads)).Msg("store opened")
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) loadState() error {
	return s.db.View(func(txn *badger.Txn) error {
		// Load HLC
		if item, err := txn.Get(keyHLC); err == nil {
			item.Value(func(val []byte) error {
				if ts, err := hlc.UnmarshalTimestamp(val); err == nil {
					s.clock.Set(ts)
				}
				return nil
			})
		}
		// Load Heads
		if item, err := txn.Get(keyHeads); err == nil {
			item.Value(func(val []byte) error {
				for i := 0; i+32 <= len(val); i += 32 {
					var c CID
					copy(c[:], val[i:i+32])
					s.heads[c] = struct{}{}
				}
				return nil
			})
		}
		return nil
	})
}

func (s *Store) saveHeads(txn *badger.Txn) error {
	sorted := s.sortedHeads()
	buf := make([]byte, len(sorted)*32)
	for i, c := range sorted {
		copy(buf[i*32:], c[:])
	}
	return txn.Set(keyHeads, buf)
}

func (s *Store) saveHLC(txn *badger.Txn) error {
	return txn.Set(keyHLC, s.clock.Timestamp().Marshal())
}

func (s *Store) sortedHeads() []CID {
	result := make([]CID, 0, len(s.heads))
	for h := range s.heads {
		result = append(result, h)
	}
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i][:], result[j][:]) < 0
	})
	return result
}

// Append добавляет новое событие
func (s *Store) Append(data []byte) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tid := s.clock.TID()
	parents := s.sortedHeads()
	event := &Event{
		TID:     tid,
		Parents: parents,
		Data:    data,
	}
	cid := event.CID()
	err := s.db.Update(func(txn *badger.Txn) error {
		// Event
		eventKey := append(prefixEvents, tid.Bytes()...)
		if err := txn.Set(eventKey, event.Marshal()); err != nil {
			return err
		}
		// DAG
		dagVal := make([]byte, len(parents)*32)
		for i, p := range parents {
			copy(dagVal[i*32:], p[:])
		}
		dagKey := append(prefixDAG, cid[:]...)
		if err := txn.Set(dagKey, dagVal); err != nil {
			return err
		}
		// CID Index
		indexKey := append(prefixCIDIndex, cid[:]...)
		if err := txn.Set(indexKey, tid.Bytes()); err != nil {
			return err
		}
		// Heads
		s.heads = map[CID]struct{}{cid: {}}
		if err := s.saveHeads(txn); err != nil {
			return err
		}
		// HLC
		return s.saveHLC(txn)
	})
	if err != nil {
		return nil, err
	}
	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", tid.String()).
		Int("parents", len(parents)).
		Msg("appended")
	return event, nil
}

// Put добавляет событие от другого узла
func (s *Store) Put(event *Event) (CID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cid := event.CID()
	// Проверяем дубликат
	if s.hasCID(cid) {
		return cid, nil
	}
	// Обновляем HLC
	s.clock.UpdateTID(event.TID)
	err := s.db.Update(func(txn *badger.Txn) error {
		// Event
		eventKey := append(prefixEvents, event.TID.Bytes()...)
		if err := txn.Set(eventKey, event.Marshal()); err != nil {
			return err
		}
		// DAG
		dagVal := make([]byte, len(event.Parents)*32)
		for i, p := range event.Parents {
			copy(dagVal[i*32:], p[:])
		}
		dagKey := append(prefixDAG, cid[:]...)
		if err := txn.Set(dagKey, dagVal); err != nil {
			return err
		}
		// CID Index
		indexKey := append(prefixCIDIndex, cid[:]...)
		if err := txn.Set(indexKey, event.TID.Bytes()); err != nil {
			return err
		}
		// HLC
		return s.saveHLC(txn)
	})
	if err != nil {
		return CID{}, err
	}
	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", event.TID.String()).
		Msg("put")

	return cid, nil
}

// Merge добавляет CID в heads и убирает его parents из heads
func (s *Store) Merge(cids []CID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, c := range cids {
		// Проверяем что событие существует
		if !s.hasCID(c) {
			continue
		}
		// Получаем parents
		parents, err := s.parents(c)
		if err != nil {
			continue
		}
		// Проверяем что все parents есть
		allParentsExist := true
		for _, p := range parents {
			if !s.hasCID(p) {
				allParentsExist = false
				break
			}
		}
		if !allParentsExist {
			continue
		}
		// Добавляем новый head
		if _, exists := s.heads[c]; !exists {
			s.heads[c] = struct{}{}
			changed = true
		}
		// Убираем parents из heads
		for _, p := range parents {
			if _, exists := s.heads[p]; exists {
				delete(s.heads, p)
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		return s.saveHeads(txn)
	})
	if err == nil {
		s.logger.Debug().
			Int("heads", len(s.heads)).
			Msg("heads updated")
	}
	return err
}

// Parents возвращает родителей по CID
func (s *Store) Parents(cid CID) ([]CID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.parents(cid)
}

// parents без блокировки
func (s *Store) parents(cid CID) ([]CID, error) {
	var parents []CID
	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixDAG, cid[:]...)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			for i := 0; i+32 <= len(val); i += 32 {
				var c CID
				copy(c[:], val[i:i+32])
				parents = append(parents, c)
			}
			return nil
		})
	})
	return parents, err
}

// Get возвращает событие по CID
func (s *Store) Get(cid CID) (*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.get(cid)
}

// get без блокировки (внутренний)
func (s *Store) get(cid CID) (*Event, error) {
	var event *Event
	err := s.db.View(func(txn *badger.Txn) error {
		// CID → TID
		indexKey := append(prefixCIDIndex, cid[:]...)
		item, err := txn.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var tid hlc.TID
		if err := item.Value(func(val []byte) error {
			var err error
			tid, err = hlc.TIDFromBytes(val)
			return err
		}); err != nil {
			return err
		}
		// TID → Event
		eventKey := append(prefixEvents, tid.Bytes()...)
		item, err = txn.Get(eventKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			event, err = UnmarshalEvent(val)
			return err
		})
	})
	return event, err
}

// GetByTID возвращает событие по TID
func (s *Store) GetByTID(tid hlc.TID) (*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var event *Event
	err := s.db.View(func(txn *badger.Txn) error {
		eventKey := append(prefixEvents, tid.Bytes()...)
		item, err := txn.Get(eventKey)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			event, err = UnmarshalEvent(val)
			return err
		})
	})
	return event, err
}

// Has проверяет наличие события по CID
func (s *Store) Has(cid CID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasCID(cid)
}

func (s *Store) hasCID(cid CID) bool {
	err := s.db.View(func(txn *badger.Txn) error {
		key := append(prefixCIDIndex, cid[:]...)
		_, err := txn.Get(key)
		return err
	})
	return err == nil
}

// Heads возвращает текущие heads
func (s *Store) Heads() []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedHeads()
}

// Missing возвращает CID которых нет
func (s *Store) Missing(cids []CID) []CID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []CID
	for _, c := range cids {
		if !s.hasCID(c) {
			missing = append(missing, c)
		}
	}
	return missing
}

// Clock возвращает HLC
func (s *Store) Clock() *hlc.Clock {
	return s.clock
}

// Range итерирует события по диапазону TID
func (s *Store) Range(from, to hlc.TID, fn func(*Event) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefixEvents, from.Bytes()...)
		endKey := append(prefixEvents, to.Bytes()...)

		for it.Seek(startKey); it.Valid(); it.Next() {
			key := it.Item().Key()
			if bytes.Compare(key, endKey) > 0 {
				break
			}

			var event *Event
			if err := it.Item().Value(func(val []byte) error {
				var err error
				event, err = UnmarshalEvent(val)
				return err
			}); err != nil {
				return err
			}

			if err := fn(event); err != nil {
				return err
			}
		}

		return nil
	})
}

// Stats возвращает статистику
func (s *Store) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := map[string]int{
		"heads": len(s.heads),
	}

	s.db.View(func(txn *badger.Txn) error {
		// Events count
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefixEvents
		it := txn.NewIterator(opts)
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		stats["events"] = count

		// Index count
		opts.Prefix = prefixCIDIndex
		it = txn.NewIterator(opts)
		count = 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		stats["index"] = count

		return nil
	})

	return stats
}
