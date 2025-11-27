package eventlog

import (
	"context"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/storage"
	"mqtt-http-tunnel/internal/synchronizer"
	"mqtt-http-tunnel/internal/tid"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// ConflictResolution определяет стратегию разрешения конфликтов
type ConflictResolution int

const (
	LastWriteWins ConflictResolution = iota // LastWriteWins последняя запись побеждает (по timestamp из TID)
	DIDPriority                             // DIDPriority приоритет по DID (лексикографический порядок)
	Custom                                  // Custom пользовательская функция разрешения
)

// EventValidator интерфейс для валидации событий
type EventValidator interface {
	Validate(ctx context.Context, event *event.Event) error
}

// EventLogConfig конфигурация event log
type EventLogConfig struct {
	OwnerDID      *identity.DID     // DID текущего узла
	KeyPair       *identity.KeyPair // Ключевая пара для подписи событий
	EncryptionKey []byte            // Ключ для шифрования приватных событий (32 байта для XChaCha20-Poly1305)
	Storage       storage.Storage
	Synchronizer  synchronizer.Synchronizer
	Logger        zerolog.Logger
	ClockID       uint // ClockID для генерации TID (0-1023, по умолчанию 0)
	Validator     EventValidator
	MaxBatchSize  int // Максимальный размер батча для синхронизации
	// GC
	EnableGC    bool          // Включить сборку мусора старых событий
	GCRetention time.Duration // Время хранения событий
	// sync
	SyncInterval       time.Duration // Интервал автоматической синхронизации
	ConflictResolution ConflictResolution
	CustomResolver     ConflictResolver
	OnRemoteEvent      func(*event.Event) error // Callback для обработки удаленных событий

}

// EventLog распределенный лог событий
type EventLog struct {
	config            EventLogConfig
	heads             []tid.TID            // Текущие головы DAG
	headsByCollection map[string][]tid.TID // Головы DAG по коллекциям
	eventCache        map[tid.TID]*event.Event
	logger            zerolog.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.RWMutex
	wg                sync.WaitGroup
}

// NewEventLog создает новый распределенный лог событий
func NewEventLog(config EventLogConfig) (*EventLog, error) {

	ctx, cancel := context.WithCancel(context.Background())

	if config.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}

	if config.MaxBatchSize == 0 {
		config.MaxBatchSize = 100
	}

	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}

	if config.GCRetention == 0 {
		config.GCRetention = 30 * 24 * time.Hour // 30 дней по умолчанию
	}

	if config.OwnerDID == nil {
		return nil, fmt.Errorf("node DID is required")
	}

	el := &EventLog{
		config:            config,
		eventCache:        make(map[tid.TID]*event.Event),
		headsByCollection: make(map[string][]tid.TID),
		logger:            config.Logger,
		ctx:               ctx,
		cancel:            cancel,
	}

	if err := el.loadHeads(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to load heads: %w", err)
	}

	if config.Synchronizer != nil {
		el.wg.Add(1)
		go el.syncLoop()
		if err := config.Synchronizer.Subscribe(context.Background(), el.handleRemoteEvent); err != nil {
			return nil, fmt.Errorf("failed to subscribe to events: %w", err)
		}
	}

	if config.EnableGC {
		el.wg.Add(1)
		go el.gcLoop()
	}

	return el, nil
}

// Get получает событие по ID и автоматически расшифровывает, если необходимо
func (el *EventLog) Get(ctx context.Context, id tid.TID) (*event.Event, error) {
	el.mu.RLock()
	if event, ok := el.eventCache[id]; ok {
		el.mu.RUnlock()
		// Расшифровываем, если событие зашифровано
		if event.Encrypted && len(el.config.EncryptionKey) > 0 {
			if err := el.DecryptEvent(ctx, event); err != nil {
				return nil, fmt.Errorf("failed to decrypt cached event: %w", err)
			}
		}
		return event, nil
	}
	el.mu.RUnlock()

	event, err := el.config.Storage.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	// Расшифровываем, если событие зашифровано
	if event.Encrypted && len(el.config.EncryptionKey) > 0 {
		if err := el.DecryptEvent(ctx, event); err != nil {
			return nil, fmt.Errorf("failed to decrypt event: %w", err)
		}
	}

	el.mu.Lock()
	el.eventCache[id] = event
	el.mu.Unlock()
	return event, nil
}

// Close закрывает event log
func (el *EventLog) Close() error {
	el.cancel()
	el.wg.Wait()
	return nil
}
