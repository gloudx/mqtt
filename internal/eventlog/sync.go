package eventlog

import (
	"context"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/tid"
	"time"
)

// ConflictResolver функция для пользовательского разрешения конфликтов
type ConflictResolver func(events []*event.Event) (*event.Event, error)

// syncLoop периодически синхронизируется с другими узлами
func (el *EventLog) syncLoop() {
	defer el.wg.Done()

	ticker := time.NewTicker(el.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			el.performSync()
		case <-el.ctx.Done():
			return
		}
	}
}

// performSync выполняет синхронизацию с peer-узлами
func (el *EventLog) performSync() {
	ctx := context.Background()

	peers, err := el.config.Synchronizer.GetPeers(ctx)
	if err != nil {
		fmt.Printf("failed to get peers: %v\n", err)
		return
	}

	// Находим самый старый TID среди наших heads для синхронизации
	el.mu.RLock()
	var oldestTID tid.TID
	if len(el.heads) > 0 {
		// Берем первый head как starting point
		oldestTID = el.heads[0]
		// Можно также использовать самый старый TID из всех heads
		for _, headTID := range el.heads {
			if headTID.Time().Before(oldestTID.Time()) {
				oldestTID = headTID
			}
		}
	}
	el.mu.RUnlock()

	for _, peerDID := range peers {
		// Запрашиваем события после нашего самого старого head
		events, err := el.config.Synchronizer.Sync(ctx, peerDID, oldestTID)
		if err != nil {
			fmt.Printf("failed to sync with peer %s: %v\n", peerDID.String(), err)
			continue
		}

		for _, event := range events {
			if err := el.handleRemoteEvent(event); err != nil {
				fmt.Printf("failed to handle remote event: %v\n", err)
			}
		}
	}
}

// handleRemoteEvent обрабатывает событие от другого узла
func (el *EventLog) handleRemoteEvent(event *event.Event) error {
	ctx := context.Background()

	// Валидируем событие
	if el.config.Validator != nil {
		if err := el.config.Validator.Validate(ctx, event); err != nil {
			return fmt.Errorf("remote event validation failed: %w", err)
		}
	}

	el.mu.Lock()
	defer el.mu.Unlock()

	// Проверяем, есть ли событие уже в хранилище
	existing, err := el.config.Storage.Load(ctx, event.EventTID)
	if err == nil && existing != nil {
		// Событие уже существует, проверяем конфликт
		if err := el.resolveConflict(ctx, existing, event); err != nil {
			return fmt.Errorf("conflict resolution failed: %w", err)
		}
		return nil
	}

	// Сохраняем событие
	if err := el.config.Storage.Store(ctx, event); err != nil {
		return fmt.Errorf("failed to store remote event: %w", err)
	}

	// Обновляем головы DAG
	if err := el.updateHeads(ctx, event); err != nil {
		return fmt.Errorf("failed to update heads: %w", err)
	}

	// Кешируем событие
	el.eventCache[event.EventTID] = event

	// Вызываем callback для применения события (например, к коллекции)
	if el.config.OnRemoteEvent != nil {
		if err := el.config.OnRemoteEvent(event); err != nil {
			// Логируем ошибку, но не прерываем обработку события
			fmt.Printf("warning: failed to apply remote event %s: %v\n", event.EventTID.String(), err)
		}
	}

	return nil
}

// resolveConflict разрешает конфликт между событиями
func (el *EventLog) resolveConflict(ctx context.Context, existing, incoming *event.Event) error {
	switch el.config.ConflictResolution {
	case LastWriteWins:
		// Сравниваем по времени из TID
		if incoming.EventTID.Time().After(existing.EventTID.Time()) {
			return el.config.Storage.Store(ctx, incoming)
		}
		return nil

	case DIDPriority:
		// Сравниваем DID лексикографически
		if incoming.AuthorDID < existing.AuthorDID {
			return el.config.Storage.Store(ctx, incoming)
		}
		return nil

	case Custom:
		if el.config.CustomResolver == nil {
			return fmt.Errorf("custom resolver not provided")
		}
		resolved, err := el.config.CustomResolver([]*event.Event{existing, incoming})
		if err != nil {
			return err
		}
		return el.config.Storage.Store(ctx, resolved)

	default:
		return fmt.Errorf("unknown conflict resolution strategy")
	}
}
