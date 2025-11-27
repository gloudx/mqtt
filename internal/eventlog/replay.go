package eventlog

import (
	"context"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/tid"
	"time"
)

// Replay воспроизводит события с начала времени с применением функции
func (el *EventLog) ReplayAll(ctx context.Context, handler func(*event.Event) error) error {
	events, err := el.config.Storage.LoadAll(ctx)
	if err != nil {
		return err
	}
	// Сортируем события по причинно-следственной связи (топологическая сортировка)
	sorted, err := el.topologicalSort(events)
	if err != nil {
		return fmt.Errorf("failed to sort events: %w", err)
	}
	for _, event := range sorted {
		if err := handler(event); err != nil {
			return fmt.Errorf("handler failed for event %s: %w", event.EventTID, err)
		}
	}
	return nil
}

// ReplayFrom воспроизводит события, начиная с указанного TID
func (el *EventLog) ReplayFrom(ctx context.Context, from tid.TID, handler func(*event.Event) error) error {
	events, err := el.config.Storage.LoadFrom(ctx, from)
	if err != nil {
		return err
	}
	// Сортируем события по причинно-следственной связи (топологическая сортировка)
	sorted, err := el.topologicalSort(events)
	if err != nil {
		return fmt.Errorf("failed to sort events: %w", err)
	}
	for _, event := range sorted {
		if err := handler(event); err != nil {
			return fmt.Errorf("handler failed for event %s: %w", event.EventTID, err)
		}
	}
	return nil
}

// ReplayRange воспроизводит события в указанном диапазоне времени
func (el *EventLog) ReplayRange(ctx context.Context, start, end time.Time, handler func(*event.Event) error) error {
	events, err := el.config.Storage.LoadRange(ctx, start, end)
	if err != nil {
		return err
	}
	// Сортируем события по причинно-следственной связи (топологическая сортировка)
	sorted, err := el.topologicalSort(events)
	if err != nil {
		return fmt.Errorf("failed to sort events: %w", err)
	}
	for _, event := range sorted {
		if err := handler(event); err != nil {
			return fmt.Errorf("handler failed for event %s: %w", event.EventTID, err)
		}
	}
	return nil
}

// topologicalSort сортирует события по причинно-следственной связи
func (el *EventLog) topologicalSort(events []*event.Event) ([]*event.Event, error) {
	eventMap := make(map[tid.TID]*event.Event)
	inDegree := make(map[tid.TID]int)
	children := make(map[tid.TID][]tid.TID)
	for _, event := range events {
		eventMap[event.EventTID] = event
		if _, ok := inDegree[event.EventTID]; !ok {
			inDegree[event.EventTID] = 0
		}
		for _, parentIDStr := range event.Parents {
			parentTID, err := tid.ParseTID(parentIDStr)
			if err != nil {
				continue // Пропускаем невалидные TID
			}
			children[parentTID] = append(children[parentTID], event.EventTID)
			inDegree[event.EventTID]++
		}
	}
	// Алгоритм Кана для топологической сортировки
	queue := make([]tid.TID, 0)
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	sorted := make([]*event.Event, 0, len(events))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		if event, ok := eventMap[id]; ok {
			sorted = append(sorted, event)
		}

		for _, childID := range children[id] {
			inDegree[childID]--
			if inDegree[childID] == 0 {
				queue = append(queue, childID)
			}
		}
	}
	if len(sorted) != len(events) {
		return nil, fmt.Errorf("cycle detected in event graph")
	}
	return sorted, nil
}
