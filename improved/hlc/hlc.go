package hlc

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrClockDrift = errors.New("clock drift too large")
	ErrFuture     = errors.New("timestamp too far in future")
	ErrInvalidTID = errors.New("invalid tid")
)

// Clock - Hybrid Logical Clock
// Комбинирует физическое время с логическим счётчиком
// Гарантирует: a ≺ b ⟹ HLC(a) < HLC(b)
// Но НЕ гарантирует обратное (для этого нужны Vector Clocks)
type Clock struct {
	mu        sync.Mutex
	processID string        // ID процесса для распределённой идентификации
	maxDiff   time.Duration // Максимальное допустимое расхождение часов
	pt        uint64        // Physical time (μs)
	lc        uint16        // Logical counter
	now       func() uint64 // Функция получения времени (для тестов)
}

// Timestamp - метка времени HLC
type Timestamp struct {
	PT uint64 // Physical time (μs since epoch)
	LC uint16 // Logical counter
}

// TID - Timestamp Identifier
// 64 бита: 48 бит timestamp (μs) + 16 бит для уникальности
// Структура: [timestamp:48][counter:8][random:8]
type TID uint64

// Base32 sortable alphabet (Crockford's base32, без I, L, O, U)
const base32Alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

// New создаёт новый HLC
func New(processID string) *Clock {
	if processID == "" {
		processID = generateProcessID()
	}
	return &Clock{
		processID: processID,
		maxDiff:   1 * time.Minute,
		now:       func() uint64 { return uint64(time.Now().UnixMicro()) },
	}
}

// generateProcessID генерирует случайный ID процесса
func generateProcessID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return string(b)
}

// ProcessID возвращает ID процесса
func (c *Clock) ProcessID() string {
	return c.processID
}

// WithMaxDrift устанавливает максимальное расхождение
func (c *Clock) WithMaxDrift(d time.Duration) *Clock {
	c.maxDiff = d
	return c
}

// WithTimeFunc устанавливает функцию времени (для тестирования)
func (c *Clock) WithTimeFunc(f func() uint64) *Clock {
	c.now = f
	return c
}

// Now генерирует новую метку времени
// Реализует правило 1 и 2 из алгоритма Лэмпорта:
// - Если физическое время продвинулось: сбрасываем LC
// - Иначе: инкрементируем LC
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

// TID генерирует уникальный Timestamp ID
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

	return c.makeTID()
}

// makeTID создаёт TID из текущего состояния (без блокировки)
func (c *Clock) makeTID() TID {
	// Генерация 8 случайных бит для уникальности
	var rnd byte
	b := make([]byte, 1)
	rand.Read(b)
	rnd = b[0]

	// Структура TID:
	// [48 бит: microseconds][8 бит: LC low byte][8 бит: random]
	ts48 := c.pt & 0xFFFFFFFFFFFF // 48 бит timestamp
	lc8 := uint64(c.lc & 0xFF)   // 8 бит LC

	return TID((ts48 << 16) | (lc8 << 8) | uint64(rnd))
}

// Update обновляет часы при получении сообщения
// Реализует правило 3 из алгоритма Лэмпорта:
// Θ(a) = max(Θ(local), Θ(received)) + 1
func (c *Clock) Update(received Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	// Проверка на слишком большое расхождение (защита от byzantine)
	maxPT := now + uint64(c.maxDiff.Microseconds())
	if received.PT > maxPT {
		return Timestamp{}, ErrFuture
	}

	// Алгоритм из лекции:
	// 1. Если локальное время больше всех - используем его
	// 2. Если равны PT - берём max(LC) + 1
	// 3. Если received больше - берём его и инкрементируем LC
	if now > c.pt && now > received.PT {
		// Физическое время продвинулось больше всех
		c.pt = now
		c.lc = 0
	} else if c.pt == received.PT {
		// Одинаковое PT - инкрементируем LC
		if received.LC > c.lc {
			c.lc = received.LC
		}
		c.lc++
	} else if received.PT > c.pt {
		// Received впереди - догоняем
		c.pt = received.PT
		c.lc = received.LC + 1
	} else {
		// Локальный PT впереди - просто инкрементируем
		c.lc++
	}

	return Timestamp{PT: c.pt, LC: c.lc}, nil
}

// UpdateTID обновляет часы из TID
func (c *Clock) UpdateTID(tid TID) (TID, error) {
	ts := tid.Timestamp()
	_, err := c.Update(ts)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	result := c.makeTID()
	c.mu.Unlock()

	return result, nil
}

// Timestamp возвращает текущее значение без инкремента
func (c *Clock) Timestamp() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Timestamp{PT: c.pt, LC: c.lc}
}

// Set устанавливает значение (при восстановлении из хранилища)
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

// --- Timestamp методы ---

// Compare сравнивает две метки
// Возвращает: -1 (a < b), 0 (a == b), 1 (a > b)
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

// Time конвертирует в time.Time
func (ts Timestamp) Time() time.Time {
	return time.UnixMicro(int64(ts.PT))
}

// Marshal сериализует в байты
func (ts Timestamp) Marshal() []byte {
	b := make([]byte, 10)
	binary.BigEndian.PutUint64(b[0:8], ts.PT)
	binary.BigEndian.PutUint16(b[8:10], ts.LC)
	return b
}

// UnmarshalTimestamp десериализует из байтов
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

// Timestamp извлекает HLC timestamp
func (t TID) Timestamp() Timestamp {
	return Timestamp{
		PT: uint64(t) >> 16,
		LC: uint16((t >> 8) & 0xFF),
	}
}

// Random возвращает случайную часть
func (t TID) Random() uint8 {
	return uint8(t & 0xFF)
}

// Time возвращает время создания
func (t TID) Time() time.Time {
	return time.UnixMicro(int64(uint64(t) >> 16))
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

// Bytes возвращает бинарное представление
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

// TIDFromTime создаёт TID из времени (для range queries)
func TIDFromTime(t time.Time, random uint8) TID {
	ts := uint64(t.UnixMicro())
	return TID((ts << 16) | uint64(random))
}

// TIDRange возвращает границы для временного диапазона
func TIDRange(from, to time.Time) (TID, TID) {
	return TIDFromTime(from, 0), TIDFromTime(to, 0xFF)
}

// MinTID возвращает минимальный возможный TID
func MinTID() TID { return 0 }

// MaxTID возвращает максимальный возможный TID
func MaxTID() TID { return ^TID(0) }
