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
			s.logger.Warn().Err(err).Msg("invalid CID in want payload")
			continue
		}
		event, err := s.store.Get(cid)
		if err != nil {
			// Событие не найдено - это нормально, пропускаем
			continue
		}
		if err := s.sendEvent(event); err != nil {
			s.logger.Error().
				Err(err).
				Str("cid", cid.Short()).
				Msg("failed to send requested event")
			// Продолжаем обработку остальных
		}
	}
	return nil
}

func (s *Sync) sendEvent(event *evstore.Event) error {
	// Rate limiting
	if err := s.rateLimiter.Wait(context.Background()); err != nil {
		s.logger.Warn().Err(err).Msg("rate limiter wait failed")
		return err
	}

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

	if err := s.ripples.Send(TypeEvent, TypeEvent, payload); err != nil {
		s.logger.Error().Err(err).Msg("failed to send event")
		return err
	}

	return nil
}
