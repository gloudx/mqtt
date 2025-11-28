package hlc

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"
)

// Clock - Hybrid Logical Clock с генерацией TID
type Clock struct {
	mu      sync.Mutex
	maxDiff time.Duration
	pt      uint64 // physical time (μs)
	lc      uint16 // logical counter
	node    uint16 // node id для уникальности
	now     func() uint64
}

// Timestamp - метка времени HLC
type Timestamp struct {
	PT uint64 // physical time (μs since epoch)
	LC uint16 // logical counter
}

// TID - Timestamp Identifier (совместим с AT Protocol)
// 64 бита: 53 бита timestamp (μs) + 10 бит clock + 1 бит reserved
type TID uint64

var (
	ErrClockDrift = errors.New("clock drift too large")
	ErrFuture     = errors.New("timestamp too far in future")
	ErrInvalidTID = errors.New("invalid tid")
)

// Base32 sortable (исключены похожие символы)
const base32Alphabet = "234567abcdefghijklmnopqrstuvwxyz"

// New создаёт новый HLC
func New() *Clock {
	var nodeID uint16
	b := make([]byte, 2)
	rand.Read(b)
	nodeID = binary.BigEndian.Uint16(b) & 0x03FF // 10 бит

	return &Clock{
		maxDiff: 1 * time.Minute,
		node:    nodeID,
		now:     func() uint64 { return uint64(time.Now().UnixMicro()) },
	}
}

// WithNodeID устанавливает ID узла (10 бит, 0-1023)
func (c *Clock) WithNodeID(id uint16) *Clock {
	c.node = id & 0x03FF
	return c
}

// WithMaxDrift устанавливает максимально допустимое расхождение
func (c *Clock) WithMaxDrift(d time.Duration) *Clock {
	c.maxDiff = d
	return c
}

// Now генерирует новую метку
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	if now > c.pt {
		c.pt = now
		c.lc = 0
	} else {
		c.lc++
	}

	return Timestamp{PT: c.pt, LC: c.lc}
}

// TID генерирует уникальный идентификатор
func (c *Clock) TID() TID {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	if now > c.pt {
		c.pt = now
		c.lc = 0
	} else {
		c.lc++
	}

	// 53 бита timestamp + 10 бит node + 1 бит reserved
	return TID((c.pt << 11) | (uint64(c.node) << 1))
}

// Update обновляет часы при получении сообщения
func (c *Clock) Update(received Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	maxPT := now + uint64(c.maxDiff.Microseconds())
	if received.PT > maxPT {
		return Timestamp{}, ErrFuture
	}

	if now > c.pt && now > received.PT {
		c.pt = now
		c.lc = 0
	} else if c.pt == received.PT {
		if received.LC > c.lc {
			c.lc = received.LC
		}
		c.lc++
	} else if received.PT > c.pt {
		c.pt = received.PT
		c.lc = received.LC + 1
	} else {
		c.lc++
	}

	return Timestamp{PT: c.pt, LC: c.lc}, nil
}

// UpdateTID обновляет часы из полученного TID
func (c *Clock) UpdateTID(tid TID) (TID, error) {
	ts := tid.Timestamp()
	newTs, err := c.Update(ts)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	node := c.node
	c.mu.Unlock()

	return TID((newTs.PT << 11) | (uint64(node) << 1)), nil
}

// Timestamp возвращает текущее значение без инкремента
func (c *Clock) Timestamp() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Timestamp{PT: c.pt, LC: c.lc}
}

// Set устанавливает значение из персистентного хранилища
func (c *Clock) Set(ts Timestamp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	maxPT := now + uint64(c.maxDiff.Microseconds())

	if ts.PT > maxPT {
		return ErrFuture
	}

	c.pt = ts.PT
	c.lc = ts.LC
	return nil
}

// NodeID возвращает ID узла
func (c *Clock) NodeID() uint16 {
	return c.node
}

// --- Timestamp методы ---

func (a Timestamp) Compare(b Timestamp) int {
	if a.PT < b.PT {
		return -1
	}
	if a.PT > b.PT {
		return 1
	}
	if a.LC < b.LC {
		return -1
	}
	if a.LC > b.LC {
		return 1
	}
	return 0
}

func (a Timestamp) Before(b Timestamp) bool { return a.Compare(b) < 0 }
func (a Timestamp) After(b Timestamp) bool  { return a.Compare(b) > 0 }
func (a Timestamp) Equal(b Timestamp) bool  { return a.Compare(b) == 0 }
func (ts Timestamp) IsZero() bool           { return ts.PT == 0 && ts.LC == 0 }

func (ts Timestamp) Time() time.Time {
	return time.UnixMicro(int64(ts.PT))
}

func (ts Timestamp) Marshal() []byte {
	b := make([]byte, 10)
	binary.BigEndian.PutUint64(b[0:8], ts.PT)
	binary.BigEndian.PutUint16(b[8:10], ts.LC)
	return b
}

func UnmarshalTimestamp(b []byte) (Timestamp, error) {
	if len(b) < 10 {
		return Timestamp{}, errors.New("invalid timestamp data")
	}
	return Timestamp{
		PT: binary.BigEndian.Uint64(b[0:8]),
		LC: binary.BigEndian.Uint16(b[8:10]),
	}, nil
}

// --- TID методы ---

// Timestamp извлекает HLC timestamp из TID
func (t TID) Timestamp() Timestamp {
	return Timestamp{
		PT: uint64(t) >> 11,
		LC: 0, // LC не хранится в TID напрямую
	}
}

// NodeID извлекает ID узла
func (t TID) NodeID() uint16 {
	return uint16((t >> 1) & 0x03FF)
}

// Time возвращает время создания
func (t TID) Time() time.Time {
	return time.UnixMicro(int64(uint64(t) >> 11))
}

// Compare сравнивает два TID
func (a TID) Compare(b TID) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (a TID) Before(b TID) bool { return a < b }
func (a TID) After(b TID) bool  { return a > b }
func (t TID) IsZero() bool      { return t == 0 }

// String кодирует TID в base32 (13 символов)
func (t TID) String() string {
	var b strings.Builder
	b.Grow(13)

	v := uint64(t)
	for i := 12; i >= 0; i-- {
		b.WriteByte(base32Alphabet[(v>>(i*5))&0x1F])
	}

	return b.String()
}

// Bytes возвращает бинарное представление (8 байт)
func (t TID) Bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t))
	return b
}

// ParseTID парсит TID из строки
func ParseTID(s string) (TID, error) {
	if len(s) != 13 {
		return 0, ErrInvalidTID
	}

	var v uint64
	for i := 0; i < 13; i++ {
		idx := strings.IndexByte(base32Alphabet, s[i])
		if idx < 0 {
			return 0, ErrInvalidTID
		}
		v = (v << 5) | uint64(idx)
	}

	return TID(v), nil
}

// TIDFromBytes парсит TID из байтов
func TIDFromBytes(b []byte) (TID, error) {
	if len(b) < 8 {
		return 0, ErrInvalidTID
	}
	return TID(binary.BigEndian.Uint64(b)), nil
}

// --- Утилиты ---

// TIDFromTime создаёт TID из времени (для запросов по диапазону)
func TIDFromTime(t time.Time, nodeID uint16) TID {
	return TID((uint64(t.UnixMicro()) << 11) | (uint64(nodeID&0x03FF) << 1))
}

// TIDRange возвращает границы для временного диапазона
func TIDRange(from, to time.Time) (TID, TID) {
	return TIDFromTime(from, 0), TIDFromTime(to, 0x03FF)
}
