package evstoresync

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/evstore"
	"mqtt-http-tunnel/internal/ripples"
)

func (s *Sync) handleWant(ctx context.Context, env *ripples.Envelope) error {
	var p WantPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if p.LogID != s.logID {
		return nil
	}
	for _, cidBytes := range p.CIDs {
		cid, err := evstore.CIDFromBytes(cidBytes)
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

func (s *Sync) sendEvent(event *evstore.Event) {
	parentsBytes := make([][]byte, len(event.Parents))
	for i, p := range event.Parents {
		parentsBytes[i] = p[:]
	}
	payload := &EventPayload{
		LogID:   s.logID,
		TID:     uint64(event.TID),
		Parents: parentsBytes,
		Data:    event.Data,
	}
	s.ripples.Send(TypeEvent, TypeEvent, payload)
}
