package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"mqtt-http-tunnel/internal/ripples"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func main() {
	ownerDID := flag.String("ownerDid", "ownerDID123", "Owner DID identifier")
	flag.Parse()

	// Пример использования SyncManager
	syncManager, err := NewSyncManager("tcp://localhost:1883", *ownerDID)
	if err != nil {
		fmt.Printf("Error creating SyncManager: %v\n", err)
		return
	}
	defer syncManager.Close()

	// Запуск основного цикла или других операций...
	select {}
}

type HeartbeatPayload struct {
	PublicKey string `json:"publicKey,omitempty"`
	Status    string `json:"status"`
}

type PeerInfo struct {
	DID       string
	LastSeen  time.Time
	PublicKey string
}

type SyncManager struct {
	ripples *ripples.Ripples
	peers   map[string]*PeerInfo
	mu      sync.RWMutex
}

func NewSyncManager(mqttBroker, ownerDID string) (*SyncManager, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID(ownerDID).
		SetCleanSession(true).
		SetAutoReconnect(true)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()

	sm := &SyncManager{
		ripples: ripples.New(client, &ripples.RipplesConfig{
			OwnerDID: ownerDID,
			Prefix:   "sync",
			Logger:   logger,
		}),
		peers: make(map[string]*PeerInfo),
	}

	// Регистрируем heartbeat
	sm.ripples.Register(&ripples.Config{
		Type:     "heartbeat",
		Topic:    "heartbeat",
		QoS:      0,
		Interval: 30 * time.Second,
		Handlers: []ripples.Handler{
			sm.handleHeartbeat,
			func(ctx context.Context, env *ripples.Envelope) error {
				fmt.Println("=============")
				return nil
			},
		},
		Publisher: sm.publishHeartbeat,
	})

	// Регистрируем запросы списка пиров
	// sm.ripples.Register(&ripples.RippleConfig{
	// 	Type:    "peer.list.request",
	// 	Topic:   "peers/list/+", // + для requestID
	// 	QoS:     1,
	// 	Handler: sm.handlePeerListRequest,
	// })

	// // Регистрируем ответы со списком пиров
	// sm.ripples.Register(&ripples.RippleConfig{
	// 	Type:    "peer.list.response",
	// 	Topic:   "peers/list/+",
	// 	QoS:     1,
	// 	Handler: sm.handlePeerListResponse,
	// })

	// // Регистрируем события синхронизации
	// sm.ripples.Register(&ripples.RippleConfig{
	// 	Type:    "sync.event",
	// 	Topic:   "events/+", // + для collection
	// 	QoS:     1,
	// 	Handler: sm.handleSyncEvent,
	// })

	return sm, nil
}

func (sm *SyncManager) publishHeartbeat(ctx context.Context) (*ripples.Envelope, error) {
	payload := HeartbeatPayload{
		PublicKey: "base64encodedkey...",
		Status:    "online",
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &ripples.Envelope{
		ID:      uuid.New().String(),
		Payload: payloadData,
	}, nil
}

func (sm *SyncManager) handleHeartbeat(ctx context.Context, env *ripples.Envelope) error {
	var payload HeartbeatPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return err
	}

	sm.mu.Lock()
	sm.peers[env.FromDID] = &PeerInfo{
		DID:       env.FromDID,
		LastSeen:  time.Now(),
		PublicKey: payload.PublicKey,
	}
	sm.mu.Unlock()

	fmt.Printf("Heartbeat from %s\n", env.FromDID)
	fmt.Printf("Payload: %+v\n", payload.PublicKey)
	return nil
}

// func (sm *SyncManager) handlePeerListRequest(ctx context.Context, env *ripples.Envelope) error {
// 	sm.mu.RLock()
// 	peerList := make([]*PeerInfo, 0, len(sm.peers))
// 	for _, p := range sm.peers {
// 		peerList = append(peerList, p)
// 	}
// 	sm.mu.RUnlock()

// 	// Отправляем ответ
// 	return sm.ripples.Send("peer.list.response", fmt.Sprintf("peers/list/%s", env.ID), peerList)
// }

// func (sm *SyncManager) handlePeerListResponse(ctx context.Context, env *ripples.Envelope) error {
// 	var peers []*PeerInfo
// 	if err := json.Unmarshal(env.Payload, &peers); err != nil {
// 		return err
// 	}

// 	fmt.Printf("Received peer list: %d peers\n", len(peers))
// 	return nil
// }

// func (sm *SyncManager) handleSyncEvent(ctx context.Context, env *ripples.Envelope) error {
// 	fmt.Printf("Sync event from %s at %d\n", env.From, env.Timestamp)
// 	// Обработка события синхронизации
// 	return nil
// }

// func (sm *SyncManager) RequestPeerList() error {
// 	requestID := uuid.New().String()
// 	return sm.ripples.Send("peer.list.request", fmt.Sprintf("peers/list/%s", requestID), nil)
// }

func (sm *SyncManager) Close() {
	sm.ripples.Close()
}
