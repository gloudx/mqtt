package synchronizer

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/tid"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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
