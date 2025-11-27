package synchronizer

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

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

// Subscribe подписывается на события от других узлов
func (ms *MQTTSynchronizer) Subscribe(ctx context.Context, handler func(*event.Event) error) error {
	ms.handlerMu.Lock()
	defer ms.handlerMu.Unlock()
	ms.handlers = append(ms.handlers, handler)
	return nil
}
