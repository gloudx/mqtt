// sync.go
package elog

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/hlc"
	"mqtt-http-tunnel/internal/ripples"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	TypeHeads = "elog.heads"
	TypeWant  = "elog.want"
	TypeEvent = "elog.event"
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

type Sync struct {
	store   *Store
	ripples *ripples.Ripples
	logID   string
	logger  zerolog.Logger

	mu      sync.Mutex
	pending map[CID]time.Time // запрошенные CID

	ctx    context.Context
	cancel context.CancelFunc
}

type SyncConfig struct {
	Store          *Store
	Ripples        *ripples.Ripples
	LogID          string
	BroadcastEvery time.Duration
	Logger         zerolog.Logger
}

func NewSync(cfg *SyncConfig) (*Sync, error) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Sync{
		store:   cfg.Store,
		ripples: cfg.Ripples,
		logID:   cfg.LogID,
		pending: make(map[CID]time.Time),
		logger:  cfg.Logger.With().Str("component", "sync").Str("log", cfg.LogID).Logger(),
		ctx:     ctx,
		cancel:  cancel,
	}

	if err := s.register(cfg.BroadcastEvery); err != nil {
		cancel()
		return nil, err
	}

	go s.cleanupLoop()

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

func (s *Sync) publishHeads(ctx context.Context) (*ripples.Envelope, error) {
	heads := s.store.Heads()
	if len(heads) == 0 {
		return nil, nil
	}

	headsBytes := make([][]byte, len(heads))
	for i, h := range heads {
		headsBytes[i] = h[:]
	}

	payload, _ := json.Marshal(&HeadsPayload{
		LogID: s.logID,
		Heads: headsBytes,
	})

	return &ripples.Envelope{Payload: payload}, nil
}

func (s *Sync) handleHeads(ctx context.Context, env *ripples.Envelope) error {
	var p HeadsPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}

	if p.LogID != s.logID {
		return nil
	}

	// Собираем недостающие
	var missing []CID
	for _, hb := range p.Heads {
		cid, err := CIDFromBytes(hb)
		if err != nil {
			continue
		}
		if !s.store.Has(cid) {
			missing = append(missing, cid)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	s.logger.Debug().
		Str("from", env.FromDID).
		Int("missing", len(missing)).
		Msg("received heads")

	s.requestCIDs(missing)
	return nil
}

func (s *Sync) handleWant(ctx context.Context, env *ripples.Envelope) error {
	var p WantPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}

	if p.LogID != s.logID {
		return nil
	}

	for _, cidBytes := range p.CIDs {
		cid, err := CIDFromBytes(cidBytes)
		if err != nil {
			continue
		}

		event, err := s.store.Get(cid)
		if err != nil {
			continue
		}

		s.sendEvent(event)
	}

	return nil
}

func (s *Sync) handleEvent(ctx context.Context, env *ripples.Envelope) error {
	var p EventPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}

	if p.LogID != s.logID {
		return nil
	}

	parents := make([]CID, len(p.Parents))
	for i, pb := range p.Parents {
		cid, err := CIDFromBytes(pb)
		if err != nil {
			return err
		}
		parents[i] = cid
	}

	event := &Event{
		TID:     hlc.TID(p.TID),
		Parents: parents,
		Data:    p.Data,
	}

	expectedCID := event.CID()

	// Проверяем что мы запрашивали этот CID
	s.mu.Lock()
	_, wasPending := s.pending[expectedCID]
	delete(s.pending, expectedCID)
	s.mu.Unlock()

	if !wasPending {
		return nil
	}

	// Сохраняем
	cid, err := s.store.Put(event)
	if err != nil {
		return err
	}

	// Верификация
	if cid != expectedCID {
		s.logger.Warn().
			Str("expected", expectedCID.Short()).
			Str("got", cid.Short()).
			Msg("cid mismatch")
		return nil
	}

	s.logger.Debug().
		Str("cid", cid.Short()).
		Str("tid", event.TID.String()).
		Msg("received event")

	// Запрашиваем недостающих родителей
	missingParents := s.store.Missing(event.Parents)
	if len(missingParents) > 0 {
		s.requestCIDs(missingParents)
	}

	// Проверяем можно ли мержить
	s.tryMerge(cid)

	return nil
}

func (s *Sync) tryMerge(cid CID) {
	// Можно мержить если все parents есть
	parents, err := s.store.Parents(cid)
	if err != nil {
		return
	}

	for _, p := range parents {
		if !s.store.Has(p) {
			return // ещё не все parents
		}
	}

	if err := s.store.Merge([]CID{cid}); err != nil {
		s.logger.Error().Err(err).Str("cid", cid.Short()).Msg("merge failed")
	}
}

func (s *Sync) requestCIDs(cids []CID) {
	s.mu.Lock()
	var toRequest [][]byte
	now := time.Now()

	for _, c := range cids {
		if _, ok := s.pending[c]; !ok {
			s.pending[c] = now
			toRequest = append(toRequest, c[:])
		}
	}
	s.mu.Unlock()

	if len(toRequest) == 0 {
		return
	}

	payload, _ := json.Marshal(&WantPayload{
		LogID: s.logID,
		CIDs:  toRequest,
	})

	s.ripples.Send(TypeWant, TypeWant, json.RawMessage(payload))

	s.logger.Debug().Int("count", len(toRequest)).Msg("requested cids")
}

func (s *Sync) sendEvent(event *Event) {
	parentsBytes := make([][]byte, len(event.Parents))
	for i, p := range event.Parents {
		parentsBytes[i] = p[:]
	}

	payload, _ := json.Marshal(&EventPayload{
		LogID:   s.logID,
		TID:     uint64(event.TID),
		Parents: parentsBytes,
		Data:    event.Data,
	})

	s.ripples.Send(TypeEvent, TypeEvent, json.RawMessage(payload))
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
			for c, t := range s.pending {
				if now.Sub(t) > time.Minute {
					delete(s.pending, c)
					expired++
				}
			}
			s.mu.Unlock()

			if expired > 0 {
				s.logger.Debug().Int("expired", expired).Msg("cleanup pending")
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

	payload, _ := json.Marshal(&HeadsPayload{
		LogID: s.logID,
		Heads: headsBytes,
	})

	s.ripples.Send(TypeHeads, TypeHeads, json.RawMessage(payload))
	s.logger.Debug().Int("heads", len(heads)).Msg("broadcast heads")
}

// OnAppend вызывается после локального Append
func (s *Sync) OnAppend() {
	s.BroadcastHeads()
}

func (s *Sync) Close() {
	s.cancel()
	s.logger.Info().Msg("sync stopped")
}
