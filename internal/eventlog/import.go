package eventlog

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mqtt-http-tunnel/internal/event"
	"os"
)

// ImportOptions опции для импорта событий
type ImportOptions struct {
	// SkipValidation пропустить валидацию событий
	SkipValidation bool
	// SkipDuplicates пропустить дубликаты (не возвращать ошибку)
	SkipDuplicates bool
	// UpdateHeads обновить heads после импорта
	UpdateHeads bool
}

// ImportResult результат импорта
type ImportResult struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Failed   int      `json:"failed"`
	Errors   []string `json:"errors,omitempty"`
}

// Import импортирует события из формата JSON Lines
func (el *EventLog) Import(ctx context.Context, reader io.Reader, opts *ImportOptions) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}

	result := &ImportResult{
		Errors: make([]string, 0),
	}

	scanner := bufio.NewScanner(reader)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		// Пропускаем пустые строки
		if len(line) == 0 {
			continue
		}

		// Пропускаем метаданные (строки с префиксом "# ")
		if len(line) >= 2 && line[0] == '#' && line[1] == ' ' {
			// Можно опционально распарсить метаданные
			continue
		}

		// Парсим событие
		var ev event.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: failed to parse event: %v", lineNum, err))
			continue
		}

		// Проверяем, не существует ли уже событие
		existing, err := el.config.Storage.Load(ctx, ev.EventTID)
		if err == nil && existing != nil {
			if opts.SkipDuplicates {
				result.Skipped++
				continue
			}
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: event %s already exists", lineNum, ev.EventTID))
			continue
		}

		// Валидация (если включена)
		if !opts.SkipValidation && el.config.Validator != nil {
			if err := el.config.Validator.Validate(ctx, &ev); err != nil {
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("line %d: validation failed for event %s: %v", lineNum, ev.EventTID, err))
				continue
			}
		}

		// Сохраняем событие
		if err := el.config.Storage.Store(ctx, &ev); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: failed to store event %s: %v", lineNum, ev.EventTID, err))
			continue
		}

		result.Imported++
	}

	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("scanner error: %w", err)
	}

	// Обновляем heads, если запрошено
	if opts.UpdateHeads && result.Imported > 0 {
		if err := el.loadHeads(ctx); err != nil {
			return result, fmt.Errorf("failed to update heads: %w", err)
		}
	}

	return result, nil
}

// ImportFromFile импортирует события из файла (вспомогательная функция)
func (el *EventLog) ImportFromFile(ctx context.Context, filename string, opts *ImportOptions) (*ImportResult, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return el.Import(ctx, file, opts)
}
