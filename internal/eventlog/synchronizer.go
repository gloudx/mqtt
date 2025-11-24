package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/tid"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTSynchronizer реализация Synchronizer через MQTT для EventLog
type MQTTSynchronizer struct {
	client      mqtt.Client
	ownerDID    *identity.DID
	keyPair     *identity.KeyPair // Ключевая пара для обмена публичными ключами
	topicPrefix string
	handlers    []func(*event.Event) error
	handlerMu   sync.RWMutex
	eventQueue  chan *event.Event
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	peers       map[string]*peerInfo // Подключенные узлы с TTL
	peersMu     sync.RWMutex
	storage     Storage     // Хранилище для ответа на запросы синхронизации
	keyManager  interface{} // KeyManager для управления ключами
}

// peerInfo хранит информацию о пире с временем последней активности
type peerInfo struct {
	DID       *identity.DID
	PublicKey []byte // Публичный ключ пира
	LastSeen  time.Time
}

// MQTTConfig конфигурация для MQTT
type MQTTConfig struct {
	Broker       string
	ClientID     string
	Username     string
	Password     string
	TopicPrefix  string
	QoS          byte
	CleanSession bool
	KeyPair      *identity.KeyPair // Ключевая пара для обмена публичными ключами
	KeyManager   interface{}       // KeyManager для управления ключами
}

// NewMQTTSynchronizer создает новый MQTT synchronizer
func NewMQTTSynchronizer(ownerDID *identity.DID, config MQTTConfig) (*MQTTSynchronizer, error) {
	if config.TopicPrefix == "" {
		config.TopicPrefix = "eventlog"
	}
	if config.QoS > 2 {
		config.QoS = 1
	}

	ctx, cancel := context.WithCancel(context.Background())

	ms := &MQTTSynchronizer{
		ownerDID:    ownerDID,
		keyPair:     config.KeyPair,
		topicPrefix: config.TopicPrefix,
		handlers:    make([]func(*event.Event) error, 0),
		eventQueue:  make(chan *event.Event, 1000),
		peers:       make(map[string]*peerInfo),
		ctx:         ctx,
		cancel:      cancel,
		storage:     nil,               // Будет установлено через SetStorage
		keyManager:  config.KeyManager, // KeyManager для управления ключами
	}

	// Настраиваем MQTT клиент
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.ClientID)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	opts.SetCleanSession(config.CleanSession)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(ms.onConnect)

	ms.client = mqtt.NewClient(opts)

	// Подключаемся
	if token := ms.client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	// Запускаем обработчик очереди
	ms.wg.Add(1)
	go ms.processEventQueue()

	// Запускаем heartbeat для discovery peers
	ms.wg.Add(1)
	go ms.heartbeatLoop()

	// Запускаем периодическую очистку устаревших пиров
	ms.wg.Add(1)
	go ms.cleanupPeersLoop()

	return ms, nil
}

func (ms *MQTTSynchronizer) onConnect(client mqtt.Client) {
	// Подписываемся на топики после подключения
	topic := fmt.Sprintf("%s/events/#", ms.topicPrefix)
	token := client.Subscribe(topic, 1, ms.handleMQTTMessage)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("failed to subscribe to topic %s: %v\n", topic, token.Error())
	}

	// Подписываемся на запросы синхронизации
	syncTopic := fmt.Sprintf("%s/sync/request", ms.topicPrefix)
	token = client.Subscribe(syncTopic, 1, ms.handleSyncRequest)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("failed to subscribe to sync topic %s: %v\n", syncTopic, token.Error())
	}

	// Подписываемся на heartbeat для discovery
	heartbeatTopic := fmt.Sprintf("%s/heartbeat", ms.topicPrefix)
	token = client.Subscribe(heartbeatTopic, 0, ms.handleHeartbeat)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("failed to subscribe to heartbeat topic %s: %v\n", heartbeatTopic, token.Error())
	}
}

func (ms *MQTTSynchronizer) handleMQTTMessage(client mqtt.Client, msg mqtt.Message) {
	var ev event.Event
	if err := json.Unmarshal(msg.Payload(), &ev); err != nil {
		fmt.Printf("failed to unmarshal event: %v\n", err)
		return
	}

	// Игнорируем собственные события
	if ev.AuthorDID == ms.ownerDID.String() {
		return
	}

	// Добавляем в очередь
	select {
	case ms.eventQueue <- &ev:
	default:
		fmt.Printf("event queue is full, dropping event %s\n", ev.EventTID.String())
	}
}

func (ms *MQTTSynchronizer) handleSyncRequest(client mqtt.Client, msg mqtt.Message) {
	if ms.storage == nil {
		fmt.Printf("storage not set, cannot handle sync request\n")
		return
	}

	var request struct {
		RequestDID string `json:"request_did"`
		TargetDID  string `json:"target_did"`
		SinceTID   string `json:"since_tid"`
	}

	if err := json.Unmarshal(msg.Payload(), &request); err != nil {
		fmt.Printf("failed to unmarshal sync request: %v\n", err)
		return
	}

	// Проверяем, что запрос адресован нам
	if request.TargetDID != ms.ownerDID.String() {
		return
	}

	fmt.Printf("handling sync request from %s since %s\n", request.RequestDID, request.SinceTID)

	// Загружаем события начиная с указанного времени
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var filteredEvents []*event.Event

	// Если sinceTID пустой - это новая нода, отправляем все события
	if request.SinceTID == "" {
		fmt.Printf("empty sinceTID - sending all events to new node\n")
		// Загружаем все события (с самого начала времени)
		events, err := ms.storage.LoadRange(ctx, time.Time{}, time.Now().Add(time.Hour))
		if err != nil {
			fmt.Printf("failed to load all events: %v\n", err)
			return
		}
		filteredEvents = events
	} else {
		// Парсим TID
		sinceTID, err := tid.ParseTID(request.SinceTID)
		if err != nil {
			fmt.Printf("failed to parse since TID: %v\n", err)
			return
		}

		// Получаем все события после указанного TID
		sinceTime := sinceTID.Time()
		events, err := ms.storage.LoadRange(ctx, sinceTime, time.Now().Add(time.Hour))
		if err != nil {
			fmt.Printf("failed to load events: %v\n", err)
			return
		}

		// Фильтруем события, оставляя только те, что после sinceTID
		filteredEvents = make([]*event.Event, 0)
		sinceInt := sinceTID.Integer()
		for _, ev := range events {
			if ev.EventTID.Integer() > sinceInt {
				filteredEvents = append(filteredEvents, ev)
			}
		}
	}

	fmt.Printf("responding with %d events to %s\n", len(filteredEvents), request.RequestDID)

	// Отправляем ответ
	responseData, err := json.Marshal(filteredEvents)
	if err != nil {
		fmt.Printf("failed to marshal events: %v\n", err)
		return
	}

	responseTopic := fmt.Sprintf("%s/sync/response/%s", ms.topicPrefix, request.RequestDID)
	token := ms.client.Publish(responseTopic, 1, false, responseData)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("failed to publish sync response: %v\n", token.Error())
	}
}

func (ms *MQTTSynchronizer) handleHeartbeat(client mqtt.Client, msg mqtt.Message) {
	var heartbeat struct {
		DID       string `json:"did"`
		Timestamp int64  `json:"timestamp"`
		PublicKey string `json:"publicKey,omitempty"` // Публичный ключ в base64
	}
	if err := json.Unmarshal(msg.Payload(), &heartbeat); err != nil {
		return
	}
	// Игнорируем свой heartbeat
	if heartbeat.DID == ms.ownerDID.String() {
		return
	}
	// Добавляем peer
	did, err := identity.ParseDID(heartbeat.DID)
	if err != nil {
		return
	}

	ms.peersMu.Lock()
	peerEntry := &peerInfo{
		DID:      did,
		LastSeen: time.Now(),
	}

	// Сохраняем публичный ключ если есть
	if heartbeat.PublicKey != "" {
		if pubKey, err := identity.DecodePublicKey(heartbeat.PublicKey); err == nil {
			peerEntry.PublicKey = pubKey

			// Добавляем ключ в KeyManager если он доступен
			if ms.keyManager != nil {
				if km, ok := ms.keyManager.(interface {
					SetPersonalKey(string, []byte) error
				}); ok {
					km.SetPersonalKey(heartbeat.DID, pubKey)
				}
			}
		}
	}

	ms.peers[heartbeat.DID] = peerEntry
	ms.peersMu.Unlock()
}

func (ms *MQTTSynchronizer) heartbeatLoop() {
	defer ms.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ms.sendHeartbeat()
		case <-ms.ctx.Done():
			return
		}
	}
}

func (ms *MQTTSynchronizer) sendHeartbeat() {
	heartbeat := struct {
		DID       string `json:"did"`
		Timestamp int64  `json:"timestamp"`
		PublicKey string `json:"publicKey,omitempty"` // Публичный ключ в base64
	}{
		DID:       ms.ownerDID.String(),
		Timestamp: time.Now().UnixMilli(),
	}

	// Добавляем публичный ключ если доступен KeyPair
	if ms.keyPair != nil {
		heartbeat.PublicKey = identity.EncodePublicKey(ms.keyPair.PublicKey)
	}

	data, err := json.Marshal(heartbeat)
	if err != nil {
		return
	}

	topic := fmt.Sprintf("%s/heartbeat", ms.topicPrefix)
	ms.client.Publish(topic, 0, false, data)
}

func (ms *MQTTSynchronizer) cleanupPeersLoop() {
	defer ms.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	const peerTTL = 90 * time.Second // TTL для пиров (3 пропущенных heartbeat)

	for {
		select {
		case <-ticker.C:
			ms.peersMu.Lock()
			now := time.Now()
			for did, info := range ms.peers {
				if now.Sub(info.LastSeen) > peerTTL {
					fmt.Printf("removing inactive peer %s (last seen: %v ago)\n", did, now.Sub(info.LastSeen))
					delete(ms.peers, did)
				}
			}
			ms.peersMu.Unlock()
		case <-ms.ctx.Done():
			return
		}
	}
}

func (ms *MQTTSynchronizer) processEventQueue() {
	defer ms.wg.Done()

	for {
		select {
		case ev := <-ms.eventQueue:
			ms.handlerMu.RLock()
			handlers := ms.handlers
			ms.handlerMu.RUnlock()

			for _, handler := range handlers {
				if err := handler(ev); err != nil {
					fmt.Printf("handler error for event %s: %v\n", ev.EventTID.String(), err)
				}
			}

		case <-ms.ctx.Done():
			return
		}
	}
}

// Publish публикует событие другим узлам
func (ms *MQTTSynchronizer) Publish(ctx context.Context, ev *event.Event) error {
	data, err := ev.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	topic := fmt.Sprintf("%s/events/%s", ms.topicPrefix, ev.Collection)
	token := ms.client.Publish(topic, 1, false, data)

	// Ждем с таймаутом
	select {
	case <-token.Done():
		if token.Error() != nil {
			return fmt.Errorf("failed to publish event: %w", token.Error())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("publish timeout")
	}
}

// Subscribe подписывается на события от других узлов
func (ms *MQTTSynchronizer) Subscribe(ctx context.Context, handler func(*event.Event) error) error {
	ms.handlerMu.Lock()
	defer ms.handlerMu.Unlock()
	ms.handlers = append(ms.handlers, handler)
	return nil
}

// Sync синхронизирует состояние с другим узлом
func (ms *MQTTSynchronizer) Sync(ctx context.Context, did *identity.DID, since tid.TID) ([]*event.Event, error) {

	fmt.Printf("starting sync with peer %s since = %s\n", did.String(), since.String())

	// Отправляем запрос на синхронизацию
	request := struct {
		RequestDID string `json:"request_did"`
		TargetDID  string `json:"target_did"`
		SinceTID   string `json:"since_tid"`
	}{
		RequestDID: ms.ownerDID.String(),
		TargetDID:  did.String(),
		SinceTID:   since.String(),
	}

	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Подписываемся на ответ
	responseTopic := fmt.Sprintf("%s/sync/response/%s", ms.topicPrefix, ms.ownerDID.String())
	responseCh := make(chan []*event.Event, 1)

	token := ms.client.Subscribe(responseTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		var events []*event.Event
		if err := json.Unmarshal(msg.Payload(), &events); err == nil {
			responseCh <- events
		}
	})

	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	defer ms.client.Unsubscribe(responseTopic)

	// Отправляем запрос
	requestTopic := fmt.Sprintf("%s/sync/request", ms.topicPrefix)
	token = ms.client.Publish(requestTopic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	// Ждем ответа с таймаутом (увеличиваем до 30 секунд для больших синхронизаций)
	select {
	case events := <-responseCh:
		fmt.Printf("received %d events from sync response\n", len(events))
		return events, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("sync timeout--")
	}
}

// GetPeers возвращает список подключенных узлов
func (ms *MQTTSynchronizer) GetPeers(ctx context.Context) ([]*identity.DID, error) {
	ms.peersMu.RLock()
	defer ms.peersMu.RUnlock()

	peers := make([]*identity.DID, 0, len(ms.peers))
	for _, info := range ms.peers {
		peers = append(peers, info.DID)
	}

	return peers, nil
}

// PeerInfo структура с информацией о пире
type PeerInfo struct {
	DID       *identity.DID
	PublicKey []byte
	LastSeen  time.Time
}

// GetPeersWithKeys возвращает список подключенных узлов с их публичными ключами
func (ms *MQTTSynchronizer) GetPeersWithKeys(ctx context.Context) ([]PeerInfo, error) {
	ms.peersMu.RLock()
	defer ms.peersMu.RUnlock()

	peers := make([]PeerInfo, 0, len(ms.peers))
	for _, info := range ms.peers {
		peers = append(peers, PeerInfo{
			DID:       info.DID,
			PublicKey: info.PublicKey,
			LastSeen:  info.LastSeen,
		})
	}

	return peers, nil
}

// GetPeerPublicKey возвращает публичный ключ конкретного пира
func (ms *MQTTSynchronizer) GetPeerPublicKey(did string) ([]byte, error) {
	ms.peersMu.RLock()
	defer ms.peersMu.RUnlock()

	if peer, ok := ms.peers[did]; ok {
		if peer.PublicKey != nil {
			return peer.PublicKey, nil
		}
		return nil, fmt.Errorf("public key not available for peer %s", did)
	}
	return nil, fmt.Errorf("peer %s not found", did)
}

// RequestEvents запрашивает конкретные события у других узлов
func (ms *MQTTSynchronizer) RequestEvents(ctx context.Context, ids []tid.TID) ([]*event.Event, error) {
	// Преобразуем TID в строки
	tidStrs := make([]string, len(ids))
	for i, id := range ids {
		tidStrs[i] = id.String()
	}

	request := struct {
		RequestDID string   `json:"request_did"`
		EventIDs   []string `json:"event_ids"`
	}{
		RequestDID: ms.ownerDID.String(),
		EventIDs:   tidStrs,
	}

	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Подписываемся на ответ
	responseTopic := fmt.Sprintf("%s/events/response/%s", ms.topicPrefix, ms.ownerDID.String())
	responseCh := make(chan []*event.Event, 1)

	token := ms.client.Subscribe(responseTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		var events []*event.Event
		if err := json.Unmarshal(msg.Payload(), &events); err == nil {
			responseCh <- events
		}
	})

	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	defer ms.client.Unsubscribe(responseTopic)

	// Отправляем запрос
	requestTopic := fmt.Sprintf("%s/events/request", ms.topicPrefix)
	token = ms.client.Publish(requestTopic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	// Ждем ответа
	select {
	case events := <-responseCh:
		return events, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("request timeout")
	}
}

// SetStorage устанавливает хранилище для синхронизатора
func (ms *MQTTSynchronizer) SetStorage(storage Storage) {
	ms.storage = storage
}

// Close закрывает synchronizer
func (ms *MQTTSynchronizer) Close() error {
	ms.cancel()
	ms.wg.Wait()
	ms.client.Disconnect(250)
	return nil
}
