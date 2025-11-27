package synchronizer

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/storage"
	"mqtt-http-tunnel/internal/tid"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var _ Synchronizer = (*MQTTSynchronizer)(nil)

// Synchronizer интерфейс для синхронизации между узлами
type Synchronizer interface {
	Publish(ctx context.Context, event *event.Event) error                              // Publish публикует событие другим узлам
	Subscribe(ctx context.Context, handler func(*event.Event) error) error              // Subscribe подписывается на события от других узлов
	Sync(ctx context.Context, did *identity.DID, since tid.TID) ([]*event.Event, error) // Sync синхронизирует состояние с другим узлом, возвращает события после указанного TID
	GetPeers(ctx context.Context) ([]*identity.DID, error)                              // GetPeers возвращает список подключенных узлов (их DID)
	RequestEvents(ctx context.Context, ids []tid.TID) ([]*event.Event, error)           // RequestEvents запрашивает конкретные события у других узлов
}

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
	peers       map[string]*PeerInfo // Подключенные узлы с TTL
	peersMu     sync.RWMutex
	storage     storage.Storage
	// keyManager  interface{} // KeyManager для управления ключами
}

// PeerInfo структура с информацией о пире
type PeerInfo struct {
	DID       *identity.DID
	PublicKey []byte
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
	// KeyManager   interface{}       // KeyManager для управления ключами
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
		peers:       make(map[string]*PeerInfo),
		ctx:         ctx,
		cancel:      cancel,
		storage:     nil, // Будет установлено через SetStorage
		// keyManager:  config.KeyManager, // KeyManager для управления ключами
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
// TODO: Реализовать метод c RPC через MQTT
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

// Close закрывает synchronizer
func (ms *MQTTSynchronizer) Close() error {
	ms.cancel()
	ms.wg.Wait()
	ms.client.Disconnect(250)
	return nil
}
