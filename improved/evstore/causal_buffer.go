package evstore

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// CausalBuffer обеспечивает доставку событий в причинном порядке
// Буферизует события, пока все их причины (parents) не будут доставлены
// Это реализация "causal delivery" из теории распределённых систем
type CausalBuffer struct {
	mu       sync.Mutex
	store    RecordStore          // Интерфейс для проверки наличия записей
	pending  map[CID]*pendingItem // Ожидающие записи
	waiting  map[CID]CIDSet       // CID → какие записи ждут этот CID
	logger   zerolog.Logger
	maxAge   time.Duration // Максимальное время ожидания
	onExpire func(CID, *Record) // Callback при истечении таймаута
}

type pendingItem struct {
	record    *Record
	addedAt   time.Time
	missing   CIDSet // Отсутствующие родители
}

// RecordStore - интерфейс для проверки наличия записей
type RecordStore interface {
	Has(CID) bool
	Put(*Record) (CID, error)
}

// CausalBufferConfig конфигурация буфера
type CausalBufferConfig struct {
	MaxAge   time.Duration
	Logger   zerolog.Logger
	OnExpire func(CID, *Record) // Вызывается при истечении таймаута
}

// NewCausalBuffer создаёт новый буфер причинной доставки
func NewCausalBuffer(store RecordStore, cfg *CausalBufferConfig) *CausalBuffer {
	if cfg == nil {
		cfg = &CausalBufferConfig{}
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 5 * time.Minute
	}

	cb := &CausalBuffer{
		store:    store,
		pending:  make(map[CID]*pendingItem),
		waiting:  make(map[CID]CIDSet),
		logger:   cfg.Logger,
		maxAge:   cfg.MaxAge,
		onExpire: cfg.OnExpire,
	}

	// Запускаем периодическую очистку
	go cb.cleanupLoop()

	return cb
}

// Receive обрабатывает входящую запись с гарантией причинного порядка
// Возвращает:
// - delivered: записи, готовые к применению (в причинном порядке)
// - missing: CID, которые нужно запросить у источника
func (cb *CausalBuffer) Receive(r *Record) (delivered []*Record, missing []CID) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cid := r.CID()

	// Уже есть в store?
	if cb.store.Has(cid) {
		return nil, nil
	}

	// Уже в pending?
	if _, exists := cb.pending[cid]; exists {
		return nil, nil
	}

	// Проверяем причинную готовность
	missingParents := NewCIDSet()
	for _, parent := range r.Parents {
		if !cb.store.Has(parent) && parent != EmptyCID() {
			// Проверяем, есть ли parent в pending (возможно ещё не доставлен)
			if _, inPending := cb.pending[parent]; !inPending {
				missingParents.Add(parent)
			}
		}
	}

	if missingParents.Len() > 0 {
		// Буферизуем - не все родители готовы
		cb.pending[cid] = &pendingItem{
			record:  r,
			addedAt: time.Now(),
			missing: missingParents,
		}

		// Регистрируем ожидание
		for parent := range missingParents {
			if cb.waiting[parent] == nil {
				cb.waiting[parent] = NewCIDSet()
			}
			cb.waiting[parent].Add(cid)
		}

		cb.logger.Debug().
			Str("cid", cid.Short()).
			Int("missing", missingParents.Len()).
			Msg("buffered record, waiting for parents")

		return nil, missingParents.ToSlice()
	}

	// Все родители есть - доставляем
	delivered = append(delivered, r)
	cb.store.Put(r)

	cb.logger.Debug().
		Str("cid", cid.Short()).
		Msg("delivered record immediately")

	// Проверяем, разблокировали ли мы что-то в pending
	delivered = append(delivered, cb.tryDeliver(cid)...)

	return delivered, nil
}

// tryDeliver пытается доставить записи, ожидавшие данный CID
// Возвращает все записи, которые удалось доставить (в причинном порядке)
func (cb *CausalBuffer) tryDeliver(delivered CID) []*Record {
	var result []*Record

	// Кто ждал этот CID?
	waiters := cb.waiting[delivered]
	delete(cb.waiting, delivered)

	if waiters == nil {
		return nil
	}

	// Для каждого ожидающего убираем delivered из missing
	for waiter := range waiters {
		item, exists := cb.pending[waiter]
		if !exists {
			continue
		}

		item.missing.Remove(delivered)

		// Если все родители теперь есть - доставляем
		if item.missing.Len() == 0 {
			result = append(result, item.record)
			cb.store.Put(item.record)
			delete(cb.pending, waiter)

			cb.logger.Debug().
				Str("cid", waiter.Short()).
				Msg("delivered previously buffered record")

			// Рекурсивно проверяем, разблокировали ли мы ещё кого-то
			result = append(result, cb.tryDeliver(waiter)...)
		}
	}

	return result
}

// NotifyDelivered уведомляет буфер о доставке записи извне
// (например, через другой канал синхронизации)
func (cb *CausalBuffer) NotifyDelivered(cid CID) []*Record {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Удаляем из pending если был там
	delete(cb.pending, cid)

	return cb.tryDeliver(cid)
}

// GetMissing возвращает все отсутствующие CID
func (cb *CausalBuffer) GetMissing() []CID {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	missing := NewCIDSet()
	for _, item := range cb.pending {
		for cid := range item.missing {
			missing.Add(cid)
		}
	}

	return missing.ToSlice()
}

// PendingCount возвращает количество ожидающих записей
func (cb *CausalBuffer) PendingCount() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return len(cb.pending)
}

// GetPendingCIDs возвращает CID всех ожидающих записей
func (cb *CausalBuffer) GetPendingCIDs() []CID {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	result := make([]CID, 0, len(cb.pending))
	for cid := range cb.pending {
		result = append(result, cid)
	}
	return result
}

// cleanupLoop периодически удаляет слишком старые записи
func (cb *CausalBuffer) cleanupLoop() {
	ticker := time.NewTicker(cb.maxAge / 2)
	defer ticker.Stop()

	for range ticker.C {
		cb.cleanup()
	}
}

func (cb *CausalBuffer) cleanup() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	var expired []CID

	for cid, item := range cb.pending {
		if now.Sub(item.addedAt) > cb.maxAge {
			expired = append(expired, cid)
		}
	}

	for _, cid := range expired {
		item := cb.pending[cid]

		// Убираем из waiting
		for parent := range item.missing {
			if cb.waiting[parent] != nil {
				cb.waiting[parent].Remove(cid)
				if cb.waiting[parent].Len() == 0 {
					delete(cb.waiting, parent)
				}
			}
		}

		delete(cb.pending, cid)

		cb.logger.Warn().
			Str("cid", cid.Short()).
			Dur("age", now.Sub(item.addedAt)).
			Int("missing_parents", item.missing.Len()).
			Msg("expired pending record")

		if cb.onExpire != nil {
			cb.onExpire(cid, item.record)
		}
	}
}

// Stats возвращает статистику буфера
type BufferStats struct {
	PendingCount     int
	WaitingForCount  int
	OldestPendingAge time.Duration
}

func (cb *CausalBuffer) Stats() BufferStats {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	stats := BufferStats{
		PendingCount:    len(cb.pending),
		WaitingForCount: len(cb.waiting),
	}

	now := time.Now()
	for _, item := range cb.pending {
		age := now.Sub(item.addedAt)
		if age > stats.OldestPendingAge {
			stats.OldestPendingAge = age
		}
	}

	return stats
}
