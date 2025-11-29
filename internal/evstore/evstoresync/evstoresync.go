package evstoresync

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/evstore"
	"mqtt-http-tunnel/internal/ripples"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	TypeHeads = "evstore.heads"
	TypeWant  = "evstore.want"
	TypeEvent = "evstore.event"
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
	store   *evstore.Store
	ripples *ripples.Ripples
	logID   string
	logger  zerolog.Logger
	pending map[evstore.CID]time.Time // запрошенные CID
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
		store:   cfg.Store,
		ripples: cfg.Ripples,
		logID:   cfg.LogID,
		pending: make(map[evstore.CID]time.Time),
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

func (s *Sync) requestCIDs(cids []evstore.CID) error {
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
