package eventlog

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/keymanager"
	"mqtt-http-tunnel/internal/tid"
	"os"
	"sync"
	"time"
)

// ConflictResolution определяет стратегию разрешения конфликтов
type ConflictResolution int

const (
	// LastWriteWins последняя запись побеждает (по timestamp из TID)
	LastWriteWins ConflictResolution = iota
	// DIDPriority приоритет по DID (лексикографический порядок)
	DIDPriority
	// Custom пользовательская функция разрешения
	Custom
)

// ConflictResolver функция для пользовательского разрешения конфликтов
type ConflictResolver func(events []*event.Event) (*event.Event, error)

// Storage интерфейс для персистентности событий
type Storage interface {
	// Store сохраняет событие
	Store(ctx context.Context, event *event.Event) error
	// Load загружает событие по ID
	Load(ctx context.Context, id tid.TID) (*event.Event, error)
	// LoadRange загружает события в диапазоне
	LoadRange(ctx context.Context, start, end time.Time) ([]*event.Event, error)
	// LoadByDID загружает все события узла по DID
	LoadByDID(ctx context.Context, did *identity.DID) ([]*event.Event, error)
	// LoadAll загружает все события
	LoadAll(ctx context.Context) ([]*event.Event, error)
	// Delete удаляет событие
	Delete(ctx context.Context, id tid.TID) error
	// GetHead возвращает последние события (головы DAG)
	GetHeads(ctx context.Context) ([]*event.Event, error)
	// UpdateHeads обновляет головы DAG
	UpdateHeads(ctx context.Context, heads []tid.TID) error
}

// Synchronizer интерфейс для синхронизации между узлами
type Synchronizer interface {
	// Publish публикует событие другим узлам
	Publish(ctx context.Context, event *event.Event) error
	// Subscribe подписывается на события от других узлов
	Subscribe(ctx context.Context, handler func(*event.Event) error) error
	// Sync синхронизирует состояние с другим узлом, возвращает события после указанного TID
	Sync(ctx context.Context, did *identity.DID, since tid.TID) ([]*event.Event, error)
	// GetPeers возвращает список подключенных узлов (их DID)
	GetPeers(ctx context.Context) ([]*identity.DID, error)
	// RequestEvents запрашивает конкретные события у других узлов
	RequestEvents(ctx context.Context, ids []tid.TID) ([]*event.Event, error)
}

// EventValidator интерфейс для валидации событий
type EventValidator interface {
	Validate(ctx context.Context, event *event.Event) error
}

// EventLogConfig конфигурация event log
type EventLogConfig struct {
	OwnerDID           *identity.DID      // DID текущего узла
	KeyPair            *identity.KeyPair  // Ключевая пара для подписи событий
	EventSigner        *event.EventSigner // Опциональный внешний signer (если nil, создастся автоматически)
	ClockID            uint               // ClockID для генерации TID (0-1023, по умолчанию 0)
	Storage            Storage
	Synchronizer       Synchronizer
	Validator          EventValidator
	ConflictResolution ConflictResolution
	CustomResolver     ConflictResolver
	OnRemoteEvent      func(*event.Event) error // Callback для обработки удаленных событий
	MaxBatchSize       int                      // Максимальный размер батча для синхронизации
	SyncInterval       time.Duration            // Интервал автоматической синхронизации
	EnableGC           bool                     // Включить сборку мусора старых событий
	GCRetention        time.Duration            // Время хранения событий
	KeyManager         *keymanager.KeyManager   // KeyManager для управления ключами шифрования (опционально)
}

// EventLog распределенный лог событий
type EventLog struct {
	config            EventLogConfig
	mu                sync.RWMutex
	heads             []tid.TID            // Текущие головы DAG
	headsByCollection map[string][]tid.TID // Головы DAG по коллекциям
	eventCache        map[tid.TID]*event.Event
	eventSigner       *event.EventSigner     // Signer для подписи событий
	keyManager        *keymanager.KeyManager // KeyManager для управления ключами
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewEventLog создает новый распределенный лог событий
func NewEventLog(config EventLogConfig) (*EventLog, error) {

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

	// Проверяем наличие DID узла
	if config.OwnerDID == nil {
		return nil, fmt.Errorf("node DID is required")
	}

	el := &EventLog{
		config:            config,
		eventCache:        make(map[tid.TID]*event.Event),
		headsByCollection: make(map[string][]tid.TID),
		keyManager:        config.KeyManager,
		stopCh:            make(chan struct{}),
	}

	// Инициализируем EventSigner
	if config.EventSigner != nil {
		el.eventSigner = config.EventSigner
	} else if config.KeyPair != nil {
		// Создаем EventSigner автоматически, если есть KeyPair
		// ClockID по умолчанию 0, если не указан явно
		clockID := config.ClockID // Используем из конфига (по умолчанию 0)
		el.eventSigner = event.NewEventSigner(
			config.OwnerDID.String(),
			config.KeyPair,
			config.KeyManager, // передаем keyManager как interface{}
			clockID,           // используем clockID из конфига
		)
	}

	// Загружаем головы DAG из хранилища
	if err := el.loadHeads(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to load heads: %w", err)
	}

	// Запускаем фоновые процессы
	if config.Synchronizer != nil {
		// Устанавливаем хранилище для синхронизатора, если это MQTT синхронизатор
		if mqttSync, ok := config.Synchronizer.(*MQTTSynchronizer); ok {
			mqttSync.SetStorage(config.Storage)
		}

		el.wg.Add(1)
		go el.syncLoop()

		// Подписываемся на события от других узлов
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

// Append добавляет новое событие в лог с указанием коллекции
func (el *EventLog) Append(ctx context.Context, collection, eventType string, data any, options *event.EventOptions) (*event.Event, error) {
	el.mu.Lock()
	defer el.mu.Unlock()

	ownerDIDStr := el.config.OwnerDID.String()

	// Получаем heads для данной коллекции
	collectionHeads := el.headsByCollection[collection]
	if len(collectionHeads) == 0 {
		// Если нет специфичных heads для коллекции, используем глобальные
		collectionHeads = el.heads
	}

	// Подготавливаем options с родительскими событиями
	if options == nil {
		options = &event.EventOptions{}
	}
	if len(options.Parents) == 0 && len(collectionHeads) > 0 {
		options.Parents = make([]string, len(collectionHeads))
		for i, headID := range collectionHeads {
			options.Parents[i] = headID.String()
		}
	}

	// Создаем событие через EventSigner (если есть) или вручную
	var ev *event.Event
	var err error

	if el.eventSigner != nil {
		// Используем EventSigner для создания и подписи
		ev, err = el.eventSigner.CreateEvent(collection, eventType, data, options)
		if err != nil {
			return nil, fmt.Errorf("failed to create event with signer: %w", err)
		}
	} else {
		// Создаем событие вручную (без подписи)
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data: %w", err)
		}

		ev = event.NewEvent(
			tid.NewTIDNow(0),
			ownerDIDStr,
			collection,
			eventType,
			dataBytes,
		)
		ev.Parents = options.Parents
	}

	// Валидируем событие
	if el.config.Validator != nil {
		if err := el.config.Validator.Validate(ctx, ev); err != nil {
			return nil, fmt.Errorf("event validation failed: %w", err)
		}
	}

	// Сохраняем событие

	fmt.Println(string(ev.Payload))

	if err := el.config.Storage.Store(ctx, ev); err != nil {
		return nil, fmt.Errorf("failed to store event: %w", err)
	}

	// Обновляем головы DAG (глобальные и по коллекциям)
	el.heads = []tid.TID{ev.EventTID}
	el.headsByCollection[collection] = []tid.TID{ev.EventTID}

	if err := el.config.Storage.UpdateHeads(ctx, el.heads); err != nil {
		return nil, fmt.Errorf("failed to update heads: %w", err)
	}

	// Кешируем событие
	el.eventCache[ev.EventTID] = ev

	// Публикуем событие другим узлам
	if el.config.Synchronizer != nil {
		if err := el.config.Synchronizer.Publish(ctx, ev); err != nil {
			// Логируем ошибку, но не прерываем операцию
			fmt.Printf("warning: failed to publish event: %v\n", err)
		}
	}

	return ev, nil
}

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

// CreatePublicEvent создает публичное событие (shortcut)
func (el *EventLog) CreatePublicEvent(ctx context.Context, collection, eventType string, data any) (*event.Event, error) {
	return el.Append(ctx, collection, eventType, data, &event.EventOptions{
		Visibility: "public",
		Encrypt:    false,
	})
}

// CreatePrivateEvent создает приватное зашифрованное событие (shortcut)
func (el *EventLog) CreatePrivateEvent(ctx context.Context, collection, eventType string, data any) (*event.Event, error) {
	return el.Append(ctx, collection, eventType, data, &event.EventOptions{
		Visibility: "private",
		Encrypt:    true,
	})
}

// CreateSharedEvent создает shared событие (shortcut)
func (el *EventLog) CreateSharedEvent(ctx context.Context, collection, eventType string, data any, sharedWith []string) (*event.Event, error) {
	return el.Append(ctx, collection, eventType, data, &event.EventOptions{
		Visibility: "shared",
		SharedWith: sharedWith,
		Encrypt:    true,
	})
}

// Get получает событие по ID
func (el *EventLog) Get(ctx context.Context, id tid.TID) (*event.Event, error) {
	el.mu.RLock()
	// Проверяем кеш
	if event, ok := el.eventCache[id]; ok {
		el.mu.RUnlock()
		return event, nil
	}
	el.mu.RUnlock()

	// Загружаем из хранилища
	event, err := el.config.Storage.Load(ctx, id)
	if err != nil {
		return nil, err
	}

	// Кешируем
	el.mu.Lock()
	el.eventCache[id] = event
	el.mu.Unlock()

	return event, nil
}

// GetRange получает события в диапазоне времени
func (el *EventLog) GetRange(ctx context.Context, start, end time.Time) ([]*event.Event, error) {
	return el.config.Storage.LoadRange(ctx, start, end)
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

// Replay воспроизводит события с начала времени с применением функции
func (el *EventLog) Replay(ctx context.Context, handler func(*event.Event) error) error {
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

// syncLoop периодически синхронизируется с другими узлами
func (el *EventLog) syncLoop() {
	defer el.wg.Done()

	ticker := time.NewTicker(el.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			el.performSync()
		case <-el.stopCh:
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

// gcLoop периодически удаляет старые события
func (el *EventLog) gcLoop() {
	defer el.wg.Done()

	ticker := time.NewTicker(24 * time.Hour) // Запускаем раз в день
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			el.performGC()
		case <-el.stopCh:
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

// topologicalSort сортирует события по причинно-следственной связи
func (el *EventLog) topologicalSort(events []*event.Event) ([]*event.Event, error) {
	// Строим граф зависимостей
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

// Close закрывает event log
func (el *EventLog) Close() error {
	close(el.stopCh)
	el.wg.Wait()
	return nil
}

// Snapshot создает снимок текущего состояния
func (el *EventLog) Snapshot(ctx context.Context) (*Snapshot, error) {
	el.mu.RLock()
	defer el.mu.RUnlock()

	events, err := el.config.Storage.LoadAll(ctx)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		OwnerDID:  el.config.OwnerDID,
		Heads:     append([]tid.TID{}, el.heads...),
		Events:    events,
		CreatedAt: time.Now().UTC(),
	}, nil
}

// Snapshot представляет снимок состояния лога
type Snapshot struct {
	OwnerDID  *identity.DID  `json:"node_did"`
	Heads     []tid.TID      `json:"heads"`
	Events    []*event.Event `json:"events"`
	CreatedAt time.Time      `json:"created_at"`
}

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

// ExportToFile экспортирует события в файл (вспомогательная функция)
func (el *EventLog) ExportToFile(ctx context.Context, filename string, opts *ExportOptions) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return el.Export(ctx, file, opts)
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
