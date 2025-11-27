package synchronizer

import (
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/identity"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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

			// // Добавляем ключ в KeyManager если он доступен
			// if ms.keyManager != nil {
			// 	if km, ok := ms.keyManager.(interface {
			// 		SetPersonalKey(string, []byte) error
			// 	}); ok {
			// 		km.SetPersonalKey(heartbeat.DID, pubKey)
			// 	}
			// }
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
