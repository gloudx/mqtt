package evstoresync

import (
	"context"
	"encoding/json"
	"mqtt-http-tunnel/internal/evstore"
	"mqtt-http-tunnel/internal/hlc"
	"mqtt-http-tunnel/internal/ripples"
)

// handleEvent обрабатывает полученное событие
//
// Алгоритм:
// 1. Десериализация EventPayload
// 2. Проверка что событие было запрошено (защита от спама)
// 3. Сохранение через Store.Put()
// 4. Верификация CID для проверки целостности
// 5. Рекурсивный запрос недостающих родителей
// 6. Попытка merge если все родители получены
func (s *Sync) handleEvent(ctx context.Context, env *ripples.Envelope) error {
	var p EventPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}
	if p.LogID != s.logID {
		return nil
	}
	parents := make([]evstore.CID, len(p.Parents))
	for i, pb := range p.Parents {
		cid, err := evstore.CIDFromBytes(pb)
		if err != nil {
			return err
		}
		parents[i] = cid
	}
	event := &evstore.Event{
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
		if err := s.requestCIDs(missingParents); err != nil {
			s.logger.Error().Err(err).
				Str("cid", cid.Short()).
				Msg("failed to request missing parents")
		}
	}
	// Проверяем можно ли мержить
	s.tryMerge(cid)
	return nil
}

// tryMerge пытается добавить CID в heads если все родители получены
//
// Событие может быть добавлено в heads только когда вся его история
// (все родители) уже присутствует в локальном Store. Это гарантирует
// что DAG остается связным.
//
// Если не все родители получены, событие регистрируется в pendingChildren
// у недостающего родителя и будет повторно проверено когда родитель придет.
func (s *Sync) tryMerge(cid evstore.CID) {
	// Можно мержить если все parents есть
	parents, err := s.store.Parents(cid)
	if err != nil {
		return
	}

	// Проверяем наличие всех родителей
	var missingParent evstore.CID
	hasMissing := false
	for _, p := range parents {
		if !s.store.Has(p) {
			missingParent = p
			hasMissing = true
			break
		}
	}

	if hasMissing {
		// Регистрируем как ожидающего ребенка недостающего родителя
		s.mu.Lock()
		s.pendingChildren[missingParent] = append(s.pendingChildren[missingParent], cid)
		s.mu.Unlock()

		s.logger.Debug().
			Str("cid", cid.Short()).
			Str("waiting_for", missingParent.Short()).
			Msg("event waiting for parent")
		return
	}

	// Все родители есть - можно мержить
	if err := s.store.Merge([]evstore.CID{cid}); err != nil {
		s.logger.Error().Err(err).Str("cid", cid.Short()).Msg("merge failed")
		return
	}

	s.logger.Debug().
		Str("cid", cid.Short()).
		Msg("event merged")

	// Проверяем есть ли дети, ожидающие это событие
	s.mu.Lock()
	children := s.pendingChildren[cid]
	delete(s.pendingChildren, cid)
	s.mu.Unlock()

	if len(children) > 0 {
		s.logger.Debug().
			Str("parent", cid.Short()).
			Int("children", len(children)).
			Msg("trying to merge waiting children")

		// Рекурсивно пытаемся смержить всех детей
		for _, child := range children {
			s.tryMerge(child)
		}
	}
}
