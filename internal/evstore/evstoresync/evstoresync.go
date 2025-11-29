// Package evstoresync реализует P2P синхронизацию event store через Ripples.
//
// # Протокол синхронизации
//
// 1. Discovery: узлы периодически broadcast своих heads
// 2. Request: при получении неизвестных heads запрашивают события через Want
// 3. Transfer: события передаются через Event messages
// 4. Merge: после получения всех родителей событие добавляется в heads
//
// # Защита от атак
//
// - MaxPending: ограничение количества одновременных запросов
// - Rate limiting: ограничение скорости отправки событий
// - Retry с backoff: повторная попытка получения недостающих событий
// - Верификация CID: проверка целостности полученных данных
//
// # Отложенный merge (Pending Children)
//
// События могут приходить в произвольном порядке. Если событие E3 с родителями
// [E1, E2] приходит раньше E2, оно не может быть добавлено в heads.
//
// Механизм pendingChildren отслеживает такие ситуации:
// 1. При получении E3: если E2 отсутствует, регистрируем E3 как ребенка E2
// 2. При получении E2: после merge проверяем pendingChildren[E2]
// 3. Рекурсивно пытаемся смержить всех ожидающих детей
//
// Это гарантирует что события будут добавлены в heads независимо от
// порядка получения, как только вся их история станет доступна.
//
// # Автоматическая интеграция
//
// Sync автоматически подписывается на события Store и broadcast их
// при создании, не требуя ручного вызова OnAppend().
package evstoresync

import (
	"context"
	"encoding/json"
	"errors"
	"mqtt-http-tunnel/internal/evstore"
	"mqtt-http-tunnel/internal/ripples"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

const (
	TypeHeads = "evstore.heads"
	TypeWant  = "evstore.want"
	TypeEvent = "evstore.event"
)

const (
	MaxPending         = 10000           // Максимум одновременных pending запросов
	PendingTTL         = 2 * time.Minute // TTL для pending запросов
	RetryInterval      = 5 * time.Second // Интервал для retry
	MaxRetries         = 3               // Максимум попыток retry
	RateLimitPerSecond = 100             // Максимум 100 событий в секунду
	RateLimitBurst     = 200             // Burst до 200 событий
)

var (
	ErrTooManyPending = errors.New("too many pending requests")
)

// --- Payloads ---

type HeadsPayload struct {
	LogID string   `json:"log"`
	Heads [][]byte `json:"heads"` // []CID as bytes
}

type WantPayload struct {
	LogID string   `json:"log"`
	CIDs  [][]byte `json:"cids"`
}

type EventPayload struct {
	LogID   string   `json:"log"`
	TID     uint64   `json:"tid"`
	Parents [][]byte `json:"parents"`
	Data    []byte   `json:"data"`
}

// --- Sync ---

// pendingRequest содержит информацию о запрошенном CID
type pendingRequest struct {
	requestedAt time.Time
	retries     int
}

type Sync struct {
	store           *evstore.Store
	ripples         *ripples.Ripples
	logID           string
	logger          zerolog.Logger
	pending         map[evstore.CID]*pendingRequest // запрошенные CID с retry счетчиком
	pendingChildren map[evstore.CID][]evstore.CID   // parent → children ожидающие родителя
	rateLimiter     *rate.Limiter                   // rate limiting для отправки событий
	unsubscribe     func()                          // функция отписки от Store
	//
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

type SyncConfig struct {
	Store          *evstore.Store
	Ripples        *ripples.Ripples
	LogID          string
	BroadcastEvery time.Duration
	Logger         zerolog.Logger
}

func NewSync(cfg *SyncConfig) (*Sync, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sync{
		store:           cfg.Store,
		ripples:         cfg.Ripples,
		logID:           cfg.LogID,
		pending:         make(map[evstore.CID]*pendingRequest),
		pendingChildren: make(map[evstore.CID][]evstore.CID),
		rateLimiter:     rate.NewLimiter(RateLimitPerSecond, RateLimitBurst),
		logger:          cfg.Logger.With().Str("component", "sync").Str("log", cfg.LogID).Logger(),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Автоматическая подписка на новые события из Store
	s.unsubscribe = cfg.Store.Subscribe(func(cid evstore.CID, event *evstore.Event) {
		// Broadcast heads после каждого нового события
		s.BroadcastHeads()
	})

	if err := s.register(cfg.BroadcastEvery); err != nil {
		cancel()
		s.unsubscribe()
		return nil, err
	}

	go s.cleanupLoop()
	go s.retryLoop()

	s.logger.Info().Msg("sync started")
	return s, nil
}

func (s *Sync) register(broadcastEvery time.Duration) error {
	// Heads
	headsCfg := &ripples.Config{
		Type:     TypeHeads,
		Handlers: []ripples.Handler{s.handleHeads},
		QoS:      1,
	}
	if broadcastEvery > 0 {
		headsCfg.Publisher = s.publishHeads
		headsCfg.Interval = broadcastEvery
	}
	if err := s.ripples.Register(headsCfg); err != nil {
		return err
	}
	// Want
	if err := s.ripples.Register(&ripples.Config{
		Type:     TypeWant,
		Handlers: []ripples.Handler{s.handleWant},
		QoS:      1,
	}); err != nil {
		return err
	}
	// Event
	if err := s.ripples.Register(&ripples.Config{
		Type:     TypeEvent,
		Handlers: []ripples.Handler{s.handleEvent},
		QoS:      1,
	}); err != nil {
		return err
	}

	return nil
}

func (s *Sync) requestCIDs(cids []evstore.CID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toRequest [][]byte
	now := time.Now()

	for _, c := range cids {
		// Проверка лимита pending
		if len(s.pending) >= MaxPending {
			s.logger.Warn().
				Int("pending", len(s.pending)).
				Int("max", MaxPending).
				Msg("max pending limit reached, skipping new requests")
			return ErrTooManyPending
		}

		// Пропускаем уже запрошенные
		if _, ok := s.pending[c]; !ok {
			s.pending[c] = &pendingRequest{
				requestedAt: now,
				retries:     0,
			}
			toRequest = append(toRequest, c[:])
		}
	}

	if len(toRequest) == 0 {
		return nil
	}

	payload := &WantPayload{
		LogID: s.logID,
		CIDs:  toRequest,
	}

	s.logger.Debug().Int("count", len(toRequest)).Msg("requested cids")
	return s.ripples.Send(TypeWant, TypeWant, payload)
}

func (s *Sync) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			expired := 0
			failed := 0

			for c, req := range s.pending {
				age := now.Sub(req.requestedAt)

				// Удаляем если превышен TTL или исчерпаны попытки
				if age > PendingTTL || req.retries >= MaxRetries {
					delete(s.pending, c)
					if req.retries >= MaxRetries {
						failed++
					} else {
						expired++
					}
				}
			}

			// Очистка pendingChildren для которых родитель так и не пришел
			// Проверяем родителей которые находятся в pending больше TTL
			childrenCleaned := 0
			for parentCID := range s.pendingChildren {
				if req, exists := s.pending[parentCID]; exists {
					if now.Sub(req.requestedAt) > PendingTTL {
						// Родитель в pending слишком долго - удаляем детей
						childrenCleaned += len(s.pendingChildren[parentCID])
						delete(s.pendingChildren, parentCID)
					}
				} else if !s.store.Has(parentCID) {
					// Родитель не в pending и не в store - удаляем
					childrenCleaned += len(s.pendingChildren[parentCID])
					delete(s.pendingChildren, parentCID)
				}
			}

			s.mu.Unlock()

			if expired > 0 || failed > 0 || childrenCleaned > 0 {
				s.logger.Debug().
					Int("expired", expired).
					Int("failed", failed).
					Int("pending", len(s.pending)).
					Int("pending_children_cleaned", childrenCleaned).
					Int("pending_children", len(s.pendingChildren)).
					Msg("cleanup pending")
			}
		}
	}
}

// retryLoop периодически повторяет запросы для pending CID
func (s *Sync) retryLoop() {
	ticker := time.NewTicker(RetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			var toRetry []evstore.CID

			for c, req := range s.pending {
				// Retry если прошло достаточно времени и не превышен лимит
				if req.retries < MaxRetries && now.Sub(req.requestedAt) > RetryInterval {
					req.retries++
					req.requestedAt = now
					toRetry = append(toRetry, c)
				}
			}
			s.mu.Unlock()

			if len(toRetry) > 0 {
				s.logger.Debug().
					Int("count", len(toRetry)).
					Msg("retrying pending requests")

				// Запрашиваем без блокировки мьютекса
				payload := &WantPayload{
					LogID: s.logID,
					CIDs:  make([][]byte, len(toRetry)),
				}
				for i, c := range toRetry {
					cidCopy := c
					payload.CIDs[i] = cidCopy[:]
				}
				if err := s.ripples.Send(TypeWant, TypeWant, payload); err != nil {
					s.logger.Error().Err(err).Msg("retry send failed")
				}
			}
		}
	}
}

// BroadcastHeads отправляет heads вручную
func (s *Sync) BroadcastHeads() {
	heads := s.store.Heads()
	if len(heads) == 0 {
		return
	}

	headsBytes := make([][]byte, len(heads))
	for i, h := range heads {
		headsBytes[i] = h[:]
	}

	payload, err := json.Marshal(&HeadsPayload{
		LogID: s.logID,
		Heads: headsBytes,
	})
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to marshal heads payload")
		return
	}

	if err := s.ripples.Send(TypeHeads, TypeHeads, json.RawMessage(payload)); err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast heads")
		return
	}

	s.logger.Debug().Int("heads", len(heads)).Msg("broadcast heads")
}

func (s *Sync) Close() {
	s.cancel()
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
	s.logger.Info().Msg("sync stopped")
}
