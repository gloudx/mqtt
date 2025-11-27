package eventlog

import (
	"context"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/tid"
)

// DecryptEvent расшифровывает зашифрованное событие
func (el *EventLog) DecryptEvent(ctx context.Context, ev *event.Event) error {
	if !ev.Encrypted {
		return nil // Событие не зашифровано
	}

	if len(el.config.EncryptionKey) == 0 {
		return fmt.Errorf("encryption key not configured")
	}

	decrypted, err := decryptPayload(ev.Payload, el.config.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt event: %w", err)
	}

	ev.Payload = decrypted
	ev.Encrypted = false

	return nil
}

// GetDecrypted получает и расшифровывает событие
func (el *EventLog) GetDecrypted(ctx context.Context, eventID tid.TID) (*event.Event, error) {
	// Загружаем событие
	ev, err := el.config.Storage.Load(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("failed to load event: %w", err)
	}

	// Расшифровываем, если необходимо
	if err := el.DecryptEvent(ctx, ev); err != nil {
		return nil, err
	}

	return ev, nil
}
