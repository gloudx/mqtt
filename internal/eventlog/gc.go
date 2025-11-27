package eventlog

import (
	"context"
	"fmt"
	"time"
)

// gcLoop периодически удаляет старые события
func (el *EventLog) gcLoop() {
	defer el.wg.Done()
	ticker := time.NewTicker(24 * time.Hour) // Запускаем раз в день
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			el.performGC()
		case <-el.ctx.Done():
			return
		}
	}
}

// performGC выполняет сборку мусора старых событий
func (el *EventLog) performGC() {
	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-el.config.GCRetention)

	events, err := el.config.Storage.LoadAll(ctx)
	if err != nil {
		fmt.Printf("failed to load events for GC: %v\n", err)
		return
	}

	for _, event := range events {
		// Используем время из TID
		if event.EventTID.Time().Before(cutoff) {
			// Проверяем, не является ли событие головой DAG
			isHead := false
			el.mu.RLock()
			for _, headID := range el.heads {
				if event.EventTID == headID {
					isHead = true
					break
				}
			}
			el.mu.RUnlock()

			if !isHead {
				if err := el.config.Storage.Delete(ctx, event.EventTID); err != nil {
					fmt.Printf("failed to delete event %s: %v\n", event.EventTID, err)
				}
			}
		}
	}
}
