package evstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	"mqtt-http-tunnel/improved/hlc"
	"mqtt-http-tunnel/improved/vclock"
)

var (
	ErrInvalidRecord  = errors.New("invalid record")
	ErrMissingParents = errors.New("missing parent records")
	ErrInvalidCausal  = errors.New("invalid causal order")
)

// RecordType определяет тип записи
type RecordType uint8

const (
	RecordTypeData   RecordType = 0 // Обычные данные
	RecordTypeMerge  RecordType = 1 // Merge-commit (разрешение форка)
	RecordTypeAnchor RecordType = 2 // Anchor (синхронизация с внешней системой)
)

// Record - запись в DAG логе
// Соответствует "событию" из теории распределённых систем
type Record struct {
	// Идентификация
	TID       hlc.TID             // Hybrid Logical Clock timestamp
	ProcessID string              // ID процесса-создателя
	VC        *vclock.VectorClock // Векторные часы для точной причинности

	// DAG структура (реализация отношения ≺)
	Parents []CID // Родительские записи (причины)

	// Содержимое
	Type RecordType // Тип записи
	Data []byte     // Полезная нагрузка

	// Опционально: криптография
	Signature []byte // Подпись создателя (если есть)
}

// NewRecord создаёт новую запись
func NewRecord(processID string, tid hlc.TID, vc *vclock.VectorClock, parents []CID, data []byte) *Record {
	return &Record{
		TID:       tid,
		ProcessID: processID,
		VC:        vc.Clone(),
		Parents:   sortCIDs(parents),
		Type:      RecordTypeData,
		Data:      data,
	}
}

// NewMergeRecord создаёт merge-запись для разрешения форка
func NewMergeRecord(processID string, tid hlc.TID, vc *vclock.VectorClock, heads []CID, data []byte) *Record {
	return &Record{
		TID:       tid,
		ProcessID: processID,
		VC:        vc.Clone(),
		Parents:   sortCIDs(heads),
		Type:      RecordTypeMerge,
		Data:      data,
	}
}

// sortCIDs сортирует CID для детерминизма
func sortCIDs(cids []CID) []CID {
	sorted := make([]CID, len(cids))
	copy(sorted, cids)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Less(sorted[j])
	})
	return sorted
}

// CID вычисляет Content Identifier
// Детерминистично: одинаковое содержимое → одинаковый CID
func (r *Record) CID() CID {
	h := sha256.New()

	// TID
	binary.Write(h, binary.BigEndian, uint64(r.TID))

	// ProcessID
	binary.Write(h, binary.BigEndian, uint32(len(r.ProcessID)))
	h.Write([]byte(r.ProcessID))

	// Vector Clock (если есть)
	if r.VC != nil {
		vcData := r.VC.Marshal()
		binary.Write(h, binary.BigEndian, uint32(len(vcData)))
		h.Write(vcData)
	} else {
		binary.Write(h, binary.BigEndian, uint32(0))
	}

	// Parents (уже отсортированы)
	binary.Write(h, binary.BigEndian, uint32(len(r.Parents)))
	for _, p := range r.Parents {
		h.Write(p[:])
	}

	// Type
	h.Write([]byte{byte(r.Type)})

	// Data
	binary.Write(h, binary.BigEndian, uint32(len(r.Data)))
	h.Write(r.Data)

	var c CID
	copy(c[:], h.Sum(nil))
	return c
}

// Marshal сериализует запись
func (r *Record) Marshal() []byte {
	var buf bytes.Buffer

	// Version (для будущей совместимости)
	buf.WriteByte(1)

	// TID (8 байт)
	binary.Write(&buf, binary.BigEndian, uint64(r.TID))

	// ProcessID
	binary.Write(&buf, binary.BigEndian, uint32(len(r.ProcessID)))
	buf.WriteString(r.ProcessID)

	// Vector Clock
	if r.VC != nil {
		vcData := r.VC.Marshal()
		binary.Write(&buf, binary.BigEndian, uint32(len(vcData)))
		buf.Write(vcData)
	} else {
		binary.Write(&buf, binary.BigEndian, uint32(0))
	}

	// Parents
	binary.Write(&buf, binary.BigEndian, uint32(len(r.Parents)))
	for _, p := range r.Parents {
		buf.Write(p[:])
	}

	// Type
	buf.WriteByte(byte(r.Type))

	// Data
	binary.Write(&buf, binary.BigEndian, uint32(len(r.Data)))
	buf.Write(r.Data)

	// Signature (опционально)
	binary.Write(&buf, binary.BigEndian, uint32(len(r.Signature)))
	buf.Write(r.Signature)

	return buf.Bytes()
}

// UnmarshalRecord десериализует запись
func UnmarshalRecord(data []byte) (*Record, error) {
	if len(data) < 13 { // минимум: version + TID + counts
		return nil, ErrInvalidRecord
	}

	buf := bytes.NewReader(data)
	r := &Record{}

	// Version
	version, _ := buf.ReadByte()
	if version != 1 {
		return nil, ErrInvalidRecord
	}

	// TID
	var tid uint64
	binary.Read(buf, binary.BigEndian, &tid)
	r.TID = hlc.TID(tid)

	// ProcessID
	var pidLen uint32
	binary.Read(buf, binary.BigEndian, &pidLen)
	if pidLen > 1024 { // разумное ограничение
		return nil, ErrInvalidRecord
	}
	pidBytes := make([]byte, pidLen)
	buf.Read(pidBytes)
	r.ProcessID = string(pidBytes)

	// Vector Clock
	var vcLen uint32
	binary.Read(buf, binary.BigEndian, &vcLen)
	if vcLen > 0 {
		vcData := make([]byte, vcLen)
		buf.Read(vcData)
		vc, err := vclock.Unmarshal(vcData)
		if err != nil {
			return nil, err
		}
		r.VC = vc
	}

	// Parents
	var parentsCount uint32
	binary.Read(buf, binary.BigEndian, &parentsCount)
	if parentsCount > 1000 { // разумное ограничение
		return nil, ErrInvalidRecord
	}
	r.Parents = make([]CID, parentsCount)
	for i := range r.Parents {
		buf.Read(r.Parents[i][:])
	}

	// Type
	typeByte, _ := buf.ReadByte()
	r.Type = RecordType(typeByte)

	// Data
	var dataLen uint32
	binary.Read(buf, binary.BigEndian, &dataLen)
	if dataLen > 10*1024*1024 { // 10MB max
		return nil, ErrInvalidRecord
	}
	r.Data = make([]byte, dataLen)
	buf.Read(r.Data)

	// Signature
	var sigLen uint32
	binary.Read(buf, binary.BigEndian, &sigLen)
	if sigLen > 0 {
		r.Signature = make([]byte, sigLen)
		buf.Read(r.Signature)
	}

	return r, nil
}

// IsMerge проверяет, является ли запись merge-коммитом
func (r *Record) IsMerge() bool {
	return r.Type == RecordTypeMerge || len(r.Parents) > 1
}

// HasParent проверяет наличие родителя
func (r *Record) HasParent(cid CID) bool {
	for _, p := range r.Parents {
		if p.Equal(cid) {
			return true
		}
	}
	return false
}

// ValidateCausality проверяет причинно-следственную корректность
// относительно родительских записей
func (r *Record) ValidateCausality(parents []*Record) error {
	if len(parents) != len(r.Parents) {
		return ErrMissingParents
	}

	if r.VC == nil {
		return nil // Без VC не можем проверить
	}

	// Проверяем, что наши VC > всех родительских
	for _, parent := range parents {
		if parent.VC == nil {
			continue
		}

		rel := r.VC.Compare(parent.VC)
		if rel != vclock.After && rel != vclock.Equal {
			return ErrInvalidCausal
		}
	}

	return nil
}

// CompareByVC сравнивает две записи по векторным часам
func CompareByVC(a, b *Record) vclock.Relation {
	if a.VC == nil || b.VC == nil {
		// Fallback на TID
		if a.TID < b.TID {
			return vclock.Before
		}
		if a.TID > b.TID {
			return vclock.After
		}
		return vclock.Concurrent
	}
	return a.VC.Compare(b.VC)
}

// IsConcurrent проверяет, параллельны ли две записи
func (r *Record) IsConcurrent(other *Record) bool {
	return CompareByVC(r, other) == vclock.Concurrent
}

// HappensBefore проверяет r ≺ other
func (r *Record) HappensBefore(other *Record) bool {
	return CompareByVC(r, other) == vclock.Before
}
