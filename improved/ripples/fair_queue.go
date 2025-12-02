package ripples

import (
	"container/list"
	"sync"
	"time"
)

// FairQueue - очередь со справедливым планированием по источникам
// Реализует "слабую справедливость" из теории:
// - Ни один источник не может монополизировать очередь
// - Round-robin обход гарантирует обслуживание всех источников
type FairQueue struct {
	mu        sync.Mutex
	queues    map[string]*list.List // Очередь на каждый DID
	order     []string              // Порядок обхода (round-robin)
	position  int                   // Текущая позиция в round-robin
	maxPerDID int                   // Макс. сообщений от одного источника
	maxTotal  int                   // Макс. общий размер очереди
	total     int                   // Текущий общий размер
	stats     FairQueueStats        // Статистика
}

// FairQueueStats статистика очереди
type FairQueueStats struct {
	Enqueued     uint64 // Всего добавлено
	Dequeued     uint64 // Всего извлечено
	Dropped      uint64 // Отброшено из-за лимитов
	SourcesCount int    // Количество источников
}

// FairQueueConfig конфигурация
type FairQueueConfig struct {
	MaxPerSource int // Макс. сообщений от одного источника (default: 100)
	MaxTotal     int // Макс. общий размер (default: 10000)
}

// NewFairQueue создаёт новую справедливую очередь
func NewFairQueue(cfg *FairQueueConfig) *FairQueue {
	if cfg == nil {
		cfg = &FairQueueConfig{}
	}
	if cfg.MaxPerSource == 0 {
		cfg.MaxPerSource = 100
	}
	if cfg.MaxTotal == 0 {
		cfg.MaxTotal = 10000
	}

	return &FairQueue{
		queues:    make(map[string]*list.List),
		maxPerDID: cfg.MaxPerSource,
		maxTotal:  cfg.MaxTotal,
	}
}

// queueItem элемент очереди
type queueItem struct {
	envelope *Envelope
	addedAt  time.Time
}

// Push добавляет сообщение в очередь
// Возвращает false если сообщение отброшено из-за лимитов
func (fq *FairQueue) Push(env *Envelope) bool {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	// Проверяем общий лимит
	if fq.total >= fq.maxTotal {
		fq.stats.Dropped++
		return false
	}

	// Получаем или создаём очередь для источника
	q, exists := fq.queues[env.FromDID]
	if !exists {
		q = list.New()
		fq.queues[env.FromDID] = q
		fq.order = append(fq.order, env.FromDID)
	}

	// Проверяем лимит на источник (слабая справедливость)
	if q.Len() >= fq.maxPerDID {
		fq.stats.Dropped++
		return false
	}

	// Добавляем
	q.PushBack(&queueItem{
		envelope: env,
		addedAt:  time.Now(),
	})
	fq.total++
	fq.stats.Enqueued++
	fq.stats.SourcesCount = len(fq.queues)

	return true
}

// Pop извлекает следующее сообщение (round-robin по источникам)
// Гарантирует справедливое обслуживание всех источников
func (fq *FairQueue) Pop() *Envelope {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if len(fq.order) == 0 || fq.total == 0 {
		return nil
	}

	// Round-robin обход источников
	startPos := fq.position
	for {
		did := fq.order[fq.position]
		q := fq.queues[did]

		if q != nil && q.Len() > 0 {
			// Извлекаем первый элемент
			elem := q.Front()
			q.Remove(elem)
			item := elem.Value.(*queueItem)

			fq.total--
			fq.stats.Dequeued++

			// Удаляем пустую очередь
			if q.Len() == 0 {
				delete(fq.queues, did)
				fq.removeFromOrder(did)
				fq.stats.SourcesCount = len(fq.queues)
				// Корректируем позицию
				if fq.position >= len(fq.order) && len(fq.order) > 0 {
					fq.position = 0
				}
			} else {
				// Переходим к следующему источнику
				fq.position = (fq.position + 1) % len(fq.order)
			}

			return item.envelope
		}

		// Переходим к следующему
		fq.position = (fq.position + 1) % len(fq.order)

		// Полный круг - выходим
		if fq.position == startPos {
			break
		}
	}

	return nil
}

// PopBatch извлекает до n сообщений
func (fq *FairQueue) PopBatch(n int) []*Envelope {
	result := make([]*Envelope, 0, n)
	for i := 0; i < n; i++ {
		env := fq.Pop()
		if env == nil {
			break
		}
		result = append(result, env)
	}
	return result
}

// Peek возвращает следующее сообщение без извлечения
func (fq *FairQueue) Peek() *Envelope {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if len(fq.order) == 0 {
		return nil
	}

	// Ищем первую непустую очередь
	for i := 0; i < len(fq.order); i++ {
		idx := (fq.position + i) % len(fq.order)
		q := fq.queues[fq.order[idx]]
		if q != nil && q.Len() > 0 {
			item := q.Front().Value.(*queueItem)
			return item.envelope
		}
	}

	return nil
}

// Len возвращает общий размер очереди
func (fq *FairQueue) Len() int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return fq.total
}

// LenBySource возвращает размер очереди для конкретного источника
func (fq *FairQueue) LenBySource(did string) int {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	if q, ok := fq.queues[did]; ok {
		return q.Len()
	}
	return 0
}

// SourceCount возвращает количество активных источников
func (fq *FairQueue) SourceCount() int {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return len(fq.queues)
}

// Stats возвращает статистику
func (fq *FairQueue) Stats() FairQueueStats {
	fq.mu.Lock()
	defer fq.mu.Unlock()
	return fq.stats
}

// Clear очищает очередь
func (fq *FairQueue) Clear() {
	fq.mu.Lock()
	defer fq.mu.Unlock()

	fq.queues = make(map[string]*list.List)
	fq.order = nil
	fq.position = 0
	fq.total = 0
	fq.stats.SourcesCount = 0
}

// removeFromOrder удаляет DID из порядка обхода
func (fq *FairQueue) removeFromOrder(did string) {
	for i, d := range fq.order {
		if d == did {
			fq.order = append(fq.order[:i], fq.order[i+1:]...)
			return
		}
	}
}

// PriorityFairQueue - очередь с приоритетами + справедливостью
// Высокоприоритетные сообщения обрабатываются первыми,
// но внутри приоритета - справедливый round-robin
type PriorityFairQueue struct {
	high   *FairQueue // Высокий приоритет
	normal *FairQueue // Нормальный приоритет
	low    *FairQueue // Низкий приоритет
	mu     sync.Mutex
}

// Priority уровень приоритета
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

// NewPriorityFairQueue создаёт приоритетную справедливую очередь
func NewPriorityFairQueue(cfg *FairQueueConfig) *PriorityFairQueue {
	return &PriorityFairQueue{
		high:   NewFairQueue(cfg),
		normal: NewFairQueue(cfg),
		low:    NewFairQueue(cfg),
	}
}

// Push добавляет с указанным приоритетом
func (pq *PriorityFairQueue) Push(env *Envelope, priority Priority) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	switch priority {
	case PriorityHigh:
		return pq.high.Push(env)
	case PriorityLow:
		return pq.low.Push(env)
	default:
		return pq.normal.Push(env)
	}
}

// Pop извлекает с учётом приоритетов
func (pq *PriorityFairQueue) Pop() *Envelope {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Сначала высокий приоритет
	if env := pq.high.Pop(); env != nil {
		return env
	}
	// Потом нормальный
	if env := pq.normal.Pop(); env != nil {
		return env
	}
	// Потом низкий
	return pq.low.Pop()
}

// Len общий размер
func (pq *PriorityFairQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.high.Len() + pq.normal.Len() + pq.low.Len()
}
