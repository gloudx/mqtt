package eventlog

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mqtt-http-tunnel/internal/event"
	"os"
	"time"
)

// ExportOptions опции для экспорта событий
type ExportOptions struct {
	// StartTime начало временного диапазона (опционально)
	StartTime *time.Time
	// EndTime конец временного диапазона (опционально)
	EndTime *time.Time
	// Collection фильтр по коллекции (опционально)
	Collection string
	// AuthorDID фильтр по автору (опционально)
	AuthorDID string
	// IncludeMetadata включить метаданные (heads, timestamp экспорта)
	IncludeMetadata bool
}

// ExportMetadata метаданные экспорта
type ExportMetadata struct {
	Version    string     `json:"version"`
	ExportedAt time.Time  `json:"exported_at"`
	OwnerDID   string     `json:"owner_did,omitempty"`
	Heads      []string   `json:"heads,omitempty"`
	EventCount int        `json:"event_count"`
	StartTime  *time.Time `json:"start_time,omitempty"`
	EndTime    *time.Time `json:"end_time,omitempty"`
	Collection string     `json:"collection,omitempty"`
}

// Export экспортирует события в формате JSON Lines
// Каждая строка - это отдельное событие в JSON формате
// Первая строка (если IncludeMetadata=true) содержит метаданные с префиксом "# "
func (el *EventLog) Export(ctx context.Context, writer io.Writer, opts *ExportOptions) error {
	el.mu.RLock()
	defer el.mu.RUnlock()

	if opts == nil {
		opts = &ExportOptions{}
	}

	// Загружаем события с учетом фильтров
	var events []*event.Event
	var err error

	if opts.StartTime != nil || opts.EndTime != nil {
		start := time.Time{}
		end := time.Now()
		if opts.StartTime != nil {
			start = *opts.StartTime
		}
		if opts.EndTime != nil {
			end = *opts.EndTime
		}
		events, err = el.config.Storage.LoadRange(ctx, start, end)
	} else {
		events, err = el.config.Storage.LoadAll(ctx)
	}

	if err != nil {
		return fmt.Errorf("failed to load events: %w", err)
	}

	// Применяем фильтры
	filtered := make([]*event.Event, 0, len(events))
	for _, ev := range events {
		if opts.Collection != "" && ev.Collection != opts.Collection {
			continue
		}
		if opts.AuthorDID != "" && ev.AuthorDID != opts.AuthorDID {
			continue
		}
		filtered = append(filtered, ev)
	}

	bw := bufio.NewWriter(writer)
	defer bw.Flush()

	// Записываем метаданные, если запрошено
	if opts.IncludeMetadata {
		heads := make([]string, len(el.heads))
		for i, h := range el.heads {
			heads[i] = h.String()
		}

		metadata := ExportMetadata{
			Version:    "1.0",
			ExportedAt: time.Now().UTC(),
			OwnerDID:   el.config.OwnerDID.String(),
			Heads:      heads,
			EventCount: len(filtered),
			StartTime:  opts.StartTime,
			EndTime:    opts.EndTime,
			Collection: opts.Collection,
		}

		metaJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		// Метаданные с префиксом для легкого отличия
		if _, err := bw.WriteString("# "); err != nil {
			return err
		}
		if _, err := bw.Write(metaJSON); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}

	// Записываем события построчно
	for _, ev := range filtered {
		eventJSON, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("failed to marshal event %s: %w", ev.EventTID, err)
		}

		if _, err := bw.Write(eventJSON); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}

	return nil
}

// ExportToFile экспортирует события в файл (вспомогательная функция)
func (el *EventLog) ExportToFile(ctx context.Context, filename string, opts *ExportOptions) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return el.Export(ctx, file, opts)
}
