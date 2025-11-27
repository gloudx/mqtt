package eventlog

import (
	"context"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/tid"
	"time"
)

// Snapshot представляет снимок состояния лога
type Snapshot struct {
	OwnerDID  *identity.DID  `json:"node_did"`
	Heads     []tid.TID      `json:"heads"`
	Events    []*event.Event `json:"events"`
	CreatedAt time.Time      `json:"created_at"`
}

// Snapshot создает снимок текущего состояния
func (el *EventLog) Snapshot(ctx context.Context) (*Snapshot, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	events, err := el.config.Storage.LoadAll(ctx)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		OwnerDID:  el.config.OwnerDID,
		Heads:     append([]tid.TID{}, el.heads...),
		Events:    events,
		CreatedAt: time.Now().UTC(),
	}, nil
}
