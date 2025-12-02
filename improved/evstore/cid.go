package evstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var (
	ErrInvalidCID = errors.New("invalid cid")
)

// CID - Content Identifier (32 байта SHA256)
// Представляет собой криптографический хеш содержимого Record
type CID [32]byte

// EmptyCID возвращает пустой CID
func EmptyCID() CID {
	return CID{}
}

// CIDFromBytes создаёт CID из байтов
func CIDFromBytes(b []byte) (CID, error) {
	if len(b) != 32 {
		return CID{}, ErrInvalidCID
	}
	var c CID
	copy(c[:], b)
	return c, nil
}

// CIDFromHex парсит CID из hex строки
func CIDFromHex(s string) (CID, error) {
	if len(s) != 64 {
		return CID{}, ErrInvalidCID
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return CID{}, err
	}
	return CIDFromBytes(b)
}

// CIDFromData вычисляет CID для произвольных данных
func CIDFromData(data []byte) CID {
	hash := sha256.Sum256(data)
	return CID(hash)
}

// String возвращает hex представление
func (c CID) String() string {
	return hex.EncodeToString(c[:])
}

// Short возвращает короткое представление (8 символов)
func (c CID) Short() string {
	return c.String()[:8]
}

// Bytes возвращает байтовое представление
func (c CID) Bytes() []byte {
	return c[:]
}

// IsZero проверяет, пустой ли CID
func (c CID) IsZero() bool {
	return c == CID{}
}

// Equal сравнивает два CID
func (c CID) Equal(other CID) bool {
	return c == other
}

// Compare сравнивает два CID лексикографически
func (c CID) Compare(other CID) int {
	for i := 0; i < 32; i++ {
		if c[i] < other[i] {
			return -1
		}
		if c[i] > other[i] {
			return 1
		}
	}
	return 0
}

// Less для сортировки
func (c CID) Less(other CID) bool {
	return c.Compare(other) < 0
}

// CIDSet - множество CID с быстрым поиском
type CIDSet map[CID]struct{}

func NewCIDSet(cids ...CID) CIDSet {
	s := make(CIDSet, len(cids))
	for _, c := range cids {
		s[c] = struct{}{}
	}
	return s
}

func (s CIDSet) Add(c CID) {
	s[c] = struct{}{}
}

func (s CIDSet) Remove(c CID) {
	delete(s, c)
}

func (s CIDSet) Has(c CID) bool {
	_, ok := s[c]
	return ok
}

func (s CIDSet) Len() int {
	return len(s)
}

func (s CIDSet) ToSlice() []CID {
	result := make([]CID, 0, len(s))
	for c := range s {
		result = append(result, c)
	}
	return result
}

// Union возвращает объединение множеств
func (s CIDSet) Union(other CIDSet) CIDSet {
	result := NewCIDSet(s.ToSlice()...)
	for c := range other {
		result.Add(c)
	}
	return result
}

// Intersection возвращает пересечение
func (s CIDSet) Intersection(other CIDSet) CIDSet {
	result := NewCIDSet()
	for c := range s {
		if other.Has(c) {
			result.Add(c)
		}
	}
	return result
}

// Difference возвращает разность (s - other)
func (s CIDSet) Difference(other CIDSet) CIDSet {
	result := NewCIDSet()
	for c := range s {
		if !other.Has(c) {
			result.Add(c)
		}
	}
	return result
}
