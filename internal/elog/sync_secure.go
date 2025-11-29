// sync_secure.go
package elog

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/hlc"
	"mqtt-http-tunnel/internal/ripples"
	"time"

	"github.com/rs/zerolog"
)

// SecureSync - синхронизация с верификацией
type SecureSync struct {
	*Sync
	secureLog *SecureLog
}

type SecureSyncConfig struct {
	SecureLog      *SecureLog
	Ripples        *ripples.Ripples
	LogID          string
	BroadcastEvery time.Duration
	Logger         zerolog.Logger
}

func NewSecureSync(cfg *SecureSyncConfig) (*SecureSync, error) {
	// Создаём базовый Sync
	baseSync, err := NewSync(&SyncConfig{
		Store:          cfg.SecureLog.Store(),
		Ripples:        cfg.Ripples,
		LogID:          cfg.LogID,
		BroadcastEvery: cfg.BroadcastEvery,
		Logger:         cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	ss := &SecureSync{
		Sync:      baseSync,
		secureLog: cfg.SecureLog,
	}

	// Переопределяем обработчик событий
	cfg.Ripples.Register(&ripples.Config{
		Type:     TypeEvent,
		Handlers: []ripples.Handler{ss.handleSecureEvent},
		QoS:      1,
	})

	return ss, nil
}

func (ss *SecureSync) handleSecureEvent(ctx context.Context, env *ripples.Envelope) error {
	var p EventPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}

	if p.LogID != ss.logID {
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

	ss.mu.Lock()
	_, wasPending := ss.pending[expectedCID]
	delete(ss.pending, expectedCID)
	ss.mu.Unlock()

	if !wasPending {
		return nil
	}

	// Верификация через SecureLog (включает проверку подписи)
	cid, err := ss.secureLog.Put(event)
	if err != nil {
		ss.logger.Warn().Err(err).Str("cid", expectedCID.Short()).Msg("rejected event")
		return nil // отклоняем молча
	}

	if cid != expectedCID {
		ss.logger.Warn().Msg("cid mismatch")
		return nil
	}

	ss.logger.Debug().
		Str("cid", cid.Short()).
		Msg("verified and stored event")

	// Запрашиваем недостающих родителей
	missingParents := ss.store.Missing(event.Parents)
	if len(missingParents) > 0 {
		ss.requestCIDs(missingParents)
	}

	ss.tryMerge(cid)

	return nil
}
