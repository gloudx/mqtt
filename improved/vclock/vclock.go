package vclock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
)

var (
	ErrInvalidData = errors.New("invalid vector clock data")
)

// VectorClock - векторные часы для точного определения причинности
// Свойство: a ≺ b ⟺ VC(a) < VC(b)
// В отличие от HLC, позволяет точно определить параллельность событий
type VectorClock struct {
	mu     sync.RWMutex
	clocks map[string]uint64
}

// New создаёт новые векторные часы
func New() *VectorClock {
	return &VectorClock{
		clocks: make(map[string]uint64),
	}
}

// FromMap создаёт часы из существующей карты
func FromMap(m map[string]uint64) *VectorClock {
	vc := New()
	for k, v := range m {
		vc.clocks[k] = v
	}
	return vc
}

// Clone создаёт копию часов
func (vc *VectorClock) Clone() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	clone := New()
	for k, v := range vc.clocks {
		clone.clocks[k] = v
	}
	return clone
}

// Tick инкрементирует счётчик для данного процесса (локальное событие)
// Соответствует правилу 2 из лекции: Θ(a) = Θ(a') + 1
func (vc *VectorClock) Tick(processID string) *VectorClock {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.clocks[processID]++
	return vc
}

// Get возвращает значение счётчика для процесса
func (vc *VectorClock) Get(processID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.clocks[processID]
}

// Set устанавливает значение счётчика
func (vc *VectorClock) Set(processID string, value uint64) *VectorClock {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clocks[processID] = value
	return vc
}

// Merge объединяет с другими часами (при получении сообщения)
// Соответствует правилу 3: Θ(a) = max(Θ(a'), Θ(b)) + 1
func (vc *VectorClock) Merge(other *VectorClock) *VectorClock {
	if other == nil {
		return vc
	}

	vc.mu.Lock()
	other.mu.RLock()
	defer vc.mu.Unlock()
	defer other.mu.RUnlock()

	for pid, count := range other.clocks {
		if count > vc.clocks[pid] {
			vc.clocks[pid] = count
		}
	}
	return vc
}

// MergeAndTick - Merge + Tick в одной атомарной операции
func (vc *VectorClock) MergeAndTick(other *VectorClock, processID string) *VectorClock {
	if other == nil {
		return vc.Tick(processID)
	}

	vc.mu.Lock()
	other.mu.RLock()
	defer vc.mu.Unlock()
	defer other.mu.RUnlock()

	for pid, count := range other.clocks {
		if count > vc.clocks[pid] {
			vc.clocks[pid] = count
		}
	}
	vc.clocks[processID]++
	return vc
}

// Relation описывает отношение между двумя событиями
type Relation int

const (
	Before     Relation = -1 // a ≺ b (a happened-before b)
	Concurrent Relation = 0  // a ∥ b (параллельны)
	After      Relation = 1  // a ≻ b (a happened-after b)
	Equal      Relation = 2  // a = b
)

func (r Relation) String() string {
	switch r {
	case Before:
		return "before"
	case Concurrent:
		return "concurrent"
	case After:
		return "after"
	case Equal:
		return "equal"
	default:
		return "unknown"
	}
}

// Compare определяет причинно-следственное отношение
// Это ключевая функция из теории - реализует проверку ≺
func (vc *VectorClock) Compare(other *VectorClock) Relation {
	if other == nil {
		return After
	}

	vc.mu.RLock()
	other.mu.RLock()
	defer vc.mu.RUnlock()
	defer other.mu.RUnlock()

	// Собираем все ключи
	allKeys := make(map[string]struct{})
	for k := range vc.clocks {
		allKeys[k] = struct{}{}
	}
	for k := range other.clocks {
		allKeys[k] = struct{}{}
	}

	var less, greater bool

	for k := range allKeys {
		v1 := vc.clocks[k]
		v2 := other.clocks[k]

		if v1 < v2 {
			less = true
		}
		if v1 > v2 {
			greater = true
		}

		// Ранний выход если уже знаем что параллельны
		if less && greater {
			return Concurrent
		}
	}

	switch {
	case less && !greater:
		return Before // vc ≺ other
	case greater && !less:
		return After // vc ≻ other
	case !less && !greater:
		return Equal // vc = other
	default:
		return Concurrent // vc ∥ other
	}
}

// HappensBefore проверяет vc ≺ other
func (vc *VectorClock) HappensBefore(other *VectorClock) bool {
	return vc.Compare(other) == Before
}

// HappensAfter проверяет vc ≻ other
func (vc *VectorClock) HappensAfter(other *VectorClock) bool {
	return vc.Compare(other) == After
}

// IsConcurrent проверяет vc ∥ other (параллельность)
// Это ключевое понятие из лекции - независимые события
func (vc *VectorClock) IsConcurrent(other *VectorClock) bool {
	return vc.Compare(other) == Concurrent
}

// IsEqual проверяет равенство
func (vc *VectorClock) IsEqual(other *VectorClock) bool {
	return vc.Compare(other) == Equal
}

// CanDeliverAfter проверяет, можно ли доставить событие с этими часами
// после события с часами other (причинная доставка)
func (vc *VectorClock) CanDeliverAfter(other *VectorClock) bool {
	rel := vc.Compare(other)
	return rel == After || rel == Concurrent
}

// Map возвращает копию внутренней карты
func (vc *VectorClock) Map() map[string]uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	m := make(map[string]uint64, len(vc.clocks))
	for k, v := range vc.clocks {
		m[k] = v
	}
	return m
}

// Size возвращает количество процессов в часах
func (vc *VectorClock) Size() int {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return len(vc.clocks)
}

// ProcessIDs возвращает отсортированный список ID процессов
func (vc *VectorClock) ProcessIDs() []string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	ids := make([]string, 0, len(vc.clocks))
	for k := range vc.clocks {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	return ids
}

// --- Сериализация ---

// Marshal сериализует для передачи по сети
// Формат: [count:4][pid_len:4][pid:N][value:8]...
func (vc *VectorClock) Marshal() []byte {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	// Сортируем для детерминизма
	keys := make([]string, 0, len(vc.clocks))
	for k := range vc.clocks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(vc.clocks)))

	for _, k := range keys {
		binary.Write(&buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
		binary.Write(&buf, binary.BigEndian, vc.clocks[k])
	}

	return buf.Bytes()
}

// Unmarshal десериализует из байтов
func Unmarshal(data []byte) (*VectorClock, error) {
	if len(data) < 4 {
		return nil, ErrInvalidData
	}

	buf := bytes.NewReader(data)
	vc := New()

	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, keyBytes); err != nil {
			return nil, err
		}

		var value uint64
		if err := binary.Read(buf, binary.BigEndian, &value); err != nil {
			return nil, err
		}

		vc.clocks[string(keyBytes)] = value
	}

	return vc, nil
}

// --- Вспомогательные функции ---

// LeastUpperBound вычисляет LUB (наименьшую верхнюю границу)
// для множества векторных часов - полезно для определения
// "последнего общего предка" в DAG
func LeastUpperBound(clocks ...*VectorClock) *VectorClock {
	result := New()

	for _, vc := range clocks {
		if vc == nil {
			continue
		}
		vc.mu.RLock()
		for k, v := range vc.clocks {
			if v > result.clocks[k] {
				result.clocks[k] = v
			}
		}
		vc.mu.RUnlock()
	}

	return result
}

// GreatestLowerBound вычисляет GLB (наибольшую нижнюю границу)
func GreatestLowerBound(clocks ...*VectorClock) *VectorClock {
	if len(clocks) == 0 {
		return New()
	}

	// Начинаем с первых часов
	result := clocks[0].Clone()

	for i := 1; i < len(clocks); i++ {
		if clocks[i] == nil {
			continue
		}
		clocks[i].mu.RLock()
		result.mu.Lock()

		// Оставляем только минимумы
		for k := range result.clocks {
			if v, exists := clocks[i].clocks[k]; exists {
				if v < result.clocks[k] {
					result.clocks[k] = v
				}
			} else {
				result.clocks[k] = 0
			}
		}

		result.mu.Unlock()
		clocks[i].mu.RUnlock()
	}

	return result
}
