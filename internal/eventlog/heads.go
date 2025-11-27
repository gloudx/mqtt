package eventlog

import (
	"context"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/tid"
)

// GetHeadsByCollection возвращает текущие головы DAG для конкретной коллекции
func (el *EventLog) GetHeadsByCollection(ctx context.Context, collection string) ([]*event.Event, error) {
	el.mu.RLock()
	headIDs := el.headsByCollection[collection]
	if len(headIDs) == 0 {
		headIDs = el.heads // fallback на глобальные heads
	}
	headIDsCopy := append([]tid.TID{}, headIDs...)
	el.mu.RUnlock()

	events := make([]*event.Event, 0, len(headIDsCopy))
	for _, id := range headIDsCopy {
		event, err := el.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// GetHeads возвращает текущие головы DAG
func (el *EventLog) GetHeads(ctx context.Context) ([]*event.Event, error) {
	el.mu.RLock()
	headIDs := append([]tid.TID{}, el.heads...)
	el.mu.RUnlock()

	events := make([]*event.Event, 0, len(headIDs))
	for _, id := range headIDs {
		event, err := el.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// updateHeads обновляет головы DAG после добавления нового события
func (el *EventLog) updateHeads(ctx context.Context, event *event.Event) error {
	// Удаляем родительские события из глобальных голов
	newHeads := make([]tid.TID, 0, len(el.heads))
	for _, headID := range el.heads {
		isParent := false
		for _, parentIDStr := range event.Parents {
			parentTID, err := tid.ParseTID(parentIDStr)
			if err == nil && headID == parentTID {
				isParent = true
				break
			}
		}
		if !isParent {
			newHeads = append(newHeads, headID)
		}
	}

	// Добавляем новое событие в головы
	newHeads = append(newHeads, event.EventTID)
	el.heads = newHeads

	// Обновляем головы для конкретной коллекции
	if event.Collection != "" {
		collectionHeads := el.headsByCollection[event.Collection]
		newCollectionHeads := make([]tid.TID, 0)
		for _, headID := range collectionHeads {
			isParent := false
			for _, parentIDStr := range event.Parents {
				parentTID, err := tid.ParseTID(parentIDStr)
				if err == nil && headID == parentTID {
					isParent = true
					break
				}
			}
			if !isParent {
				newCollectionHeads = append(newCollectionHeads, headID)
			}
		}
		newCollectionHeads = append(newCollectionHeads, event.EventTID)
		el.headsByCollection[event.Collection] = newCollectionHeads
	}

	return el.config.Storage.UpdateHeads(ctx, el.heads)
}

// loadHeads загружает головы DAG из хранилища
func (el *EventLog) loadHeads(ctx context.Context) error {
	heads, err := el.config.Storage.GetHeads(ctx)
	if err != nil {
		return err
	}

	el.heads = make([]tid.TID, len(heads))
	for i, event := range heads {
		el.heads[i] = event.EventTID
		el.eventCache[event.EventTID] = event
	}

	return nil
}
