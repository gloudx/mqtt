package evstoresync

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/evstore"
	"mqtt-http-tunnel/internal/ripples"
)

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
	var missing []evstore.CID
	for _, hb := range p.Heads {
		cid, err := evstore.CIDFromBytes(hb)
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
	return s.requestCIDs(missing)
}
