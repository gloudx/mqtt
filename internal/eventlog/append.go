package eventlog

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/tid"

	"golang.org/x/crypto/chacha20poly1305"
)

// Append добавляет новое событие в лог с указанием коллекции
func (el *EventLog) Append(ctx context.Context, collection, eventType string, data any, options *event.EventOptions) (*event.Event, error) {
	el.mu.Lock()
	defer el.mu.Unlock()

	ownerDid := el.config.OwnerDID.String()

	// Определяем родительские события из heads коллекции
	collectionHeads := el.headsByCollection[collection]
	if len(collectionHeads) == 0 {
		collectionHeads = el.heads
	}

	if options == nil {
		options = &event.EventOptions{}
	}

	// Устанавливаем Parents только если они не указаны явно
	if len(options.Parents) == 0 && len(collectionHeads) > 0 {
		options.Parents = make([]string, len(collectionHeads))
		for i, headID := range collectionHeads {
			options.Parents[i] = headID.String()
		}
	}

	// Маршалим данные
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	// Шифруем, если требуется
	if options.Encrypt && len(el.config.EncryptionKey) > 0 {
		encrypted, err := encryptPayload(dataBytes, el.config.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt data: %w", err)
		}
		dataBytes = encrypted
	}

	// Создаем событие
	ev := event.NewEvent(tid.NewTIDNow(el.config.ClockID), ownerDid, collection, eventType, dataBytes)
	ev.Parents = options.Parents
	ev.Encrypted = options.Encrypt

	// Проверяем дубликаты
	if _, exists := el.eventCache[ev.EventTID]; exists {
		return nil, fmt.Errorf("event %s already exists", ev.EventTID)
	}

	// Валидируем событие
	if el.config.Validator != nil {
		if err := el.config.Validator.Validate(ctx, ev); err != nil {
			return nil, fmt.Errorf("event validation failed: %w", err)
		}
	}

	// Подписываем событие
	if err := event.SignEvent(ev, el.config.KeyPair); err != nil {
		return nil, fmt.Errorf("failed to sign event: %w", err)
	}

	// Сохраняем в storage
	if err := el.config.Storage.Store(ctx, ev); err != nil {
		return nil, fmt.Errorf("failed to store event: %w", err)
	}

	// Корректно обновляем heads через updateHeads
	if err := el.updateHeads(ctx, ev); err != nil {
		return nil, fmt.Errorf("failed to update heads: %w", err)
	}

	// Добавляем в кеш после успешного сохранения
	el.eventCache[ev.EventTID] = ev

	// Публикуем событие (не критичная операция)
	if el.config.Synchronizer != nil {
		if err := el.config.Synchronizer.Publish(ctx, ev); err != nil {
			fmt.Printf("warning: failed to publish event: %v\n", err)
		}
	}

	return ev, nil
}

// CreatePublicEvent создает публичное событие (shortcut)
func (el *EventLog) CreatePublicEvent(ctx context.Context, collection, eventType string, data any) (*event.Event, error) {
	return el.Append(ctx, collection, eventType, data, &event.EventOptions{
		Encrypt: false,
	})
}

// CreatePrivateEvent создает приватное зашифрованное событие (shortcut)
func (el *EventLog) CreatePrivateEvent(ctx context.Context, collection, eventType string, data any) (*event.Event, error) {
	return el.Append(ctx, collection, eventType, data, &event.EventOptions{
		Encrypt: true,
	})
}

// encryptPayload шифрует данные с использованием XChaCha20-Poly1305
func encryptPayload(plaintext, key []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", chacha20poly1305.KeySize, len(key))
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// decryptPayload расшифровывает данные с использованием XChaCha20-Poly1305
func decryptPayload(ciphertext, key []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", chacha20poly1305.KeySize, len(key))
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	if len(ciphertext) < aead.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := ciphertext[:aead.NonceSize()]
	encryptedData := ciphertext[aead.NonceSize():]

	plaintext, err := aead.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}
