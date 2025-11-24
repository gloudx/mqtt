package event

import (
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/keymanager"
	"mqtt-http-tunnel/internal/permissions"
	"mqtt-http-tunnel/internal/tid"
)

// EventSigner подписывает и создаёт события
type EventSigner struct {
	myDID      string
	keyPair    *identity.KeyPair
	keyManager *keymanager.KeyManager
	tidClock   *tid.TIDClock
}

// NewEventSigner создаёт новый signer
func NewEventSigner(myDID string, keyPair *identity.KeyPair, keyManager *keymanager.KeyManager, clockId uint) *EventSigner {
	return &EventSigner{
		myDID:      myDID,
		keyManager: keyManager,
		keyPair:    keyPair,
		tidClock:   tid.NewTIDClock(clockId),
	}
}

// CreateEvent создаёт и подписывает новое событие
func (s *EventSigner) CreateEvent(collection, eventType string, payload any, options *EventOptions) (*Event, error) {

	// Сериализуем payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Получаем следующий sequence number
	tID := s.tidClock.Next()

	// Создаём базовое событие
	event := NewEvent(tID, s.myDID, collection, eventType, payloadBytes)

	// Применяем опции
	if options != nil {
		if err := s.applyOptions(event, options); err != nil {
			return nil, fmt.Errorf("failed to apply options: %w", err)
		}
	}

	// Шифруем если нужно
	if options != nil && options.Encrypt {
		if err := s.encryptEvent(event, options.EncryptionKeyID); err != nil {
			return nil, fmt.Errorf("failed to encrypt event: %w", err)
		}
	}

	// Подписываем
	if err := s.signEvent(event); err != nil {
		return nil, fmt.Errorf("failed to sign event: %w", err)
	}

	return event, nil
}

// EventOptions опции создания события
type EventOptions struct {
	Visibility      permissions.Visibility
	SharedWith      []string
	Encrypt         bool
	EncryptionKeyID string
	Parents         []string
}

// applyOptions применяет опции к событию
func (s *EventSigner) applyOptions(event *Event, options *EventOptions) error {
	// Применяем Visibility
	if options.Visibility != "" {
		event.AccessControl.Visibility = options.Visibility
	}

	// Применяем SharedWith
	if len(options.SharedWith) > 0 {
		event.AccessControl.SharedWith = options.SharedWith
	}

	// Применяем Parents с проверкой на циклы
	if len(options.Parents) > 0 {
		if err := validateParents(options.Parents); err != nil {
			return fmt.Errorf("invalid parents: %w", err)
		}
		event.Parents = options.Parents
	}

	return nil
}

// encryptEvent шифрует payload события
func (s *EventSigner) encryptEvent(event *Event, keyID string) error {

	// Если keyID не указан - используем default
	if keyID == "" {
		// Определяем по visibility
		switch event.AccessControl.Visibility {
		case permissions.VisibilityPrivate:
			keyID = fmt.Sprintf("personal:%s", s.myDID)

		case permissions.VisibilityShared:
			// Для shared нужно создать новый ключ
			sharedKey, err := s.keyManager.CreateSharedKey(event.AccessControl.SharedWith)
			if err != nil {
				return fmt.Errorf("failed to create shared key: %w", err)
			}
			keyID = sharedKey.KeyID

		case permissions.VisibilityPublic:
			// Для public используем семейный ключ
			familyKey, err := s.keyManager.GetFamilyKey()
			if err != nil {
				return fmt.Errorf("failed to get family key: %w", err)
			}
			keyID = familyKey.KeyID
		}
	}

	// Шифруем payload
	encryptedPayload, err := s.keyManager.EncryptWithKey(event.Payload, keyID)
	if err != nil {
		return fmt.Errorf("failed to encrypt payload: %w", err)
	}

	// Обновляем событие
	event.Payload = encryptedPayload
	event.MarkAsEncrypted(keyID)

	return nil
}

// signEvent подписывает событие
func (s *EventSigner) signEvent(event *Event) error {

	eventBytes, err := event.BytesForSigning()
	if err != nil {
		return fmt.Errorf("failed to get bytes for signing: %w", err)
	}

	signature, err := s.keyPair.Sign(eventBytes)
	if err != nil {
		return fmt.Errorf("failed to sign: %w", err)
	}

	event.Signature = signature
	return nil
}

// CreatePublicEvent создаёт публичное событие (shortcut)
func (s *EventSigner) CreatePublicEvent(collection, eventType string, payload any) (*Event, error) {
	return s.CreateEvent(collection, eventType, payload, &EventOptions{
		Visibility: permissions.VisibilityPublic,
		Encrypt:    false,
	})
}

// CreatePrivateEvent создаёт приватное зашифрованное событие (shortcut)
func (s *EventSigner) CreatePrivateEvent(collection, eventType string, payload any) (*Event, error) {
	return s.CreateEvent(collection, eventType, payload, &EventOptions{
		Visibility: permissions.VisibilityPrivate,
		Encrypt:    true,
	})
}

// CreateSharedEvent создаёт событие shared с указанными участниками (shortcut)
func (s *EventSigner) CreateSharedEvent(collection, eventType string, payload any, sharedWith []string) (*Event, error) {
	return s.CreateEvent(collection, eventType, payload, &EventOptions{
		Visibility: permissions.VisibilityShared,
		SharedWith: sharedWith,
		Encrypt:    true,
	})
}

// UpdateEvent создаёт событие обновления (с parent)
func (s *EventSigner) UpdateEvent(collection, eventType string, payload any, parentEventID string, options *EventOptions) (*Event, error) {
	if options == nil {
		options = &EventOptions{}
	}
	options.Parents = []string{parentEventID}
	return s.CreateEvent(collection, eventType, payload, options)
}

// validateParents проверяет корректность списка parents
func validateParents(parents []string) error {
	if len(parents) == 0 {
		return nil
	}

	// Проверка на дубликаты (простейшая защита от циклов)
	seen := make(map[string]bool, len(parents))
	for _, parent := range parents {
		if parent == "" {
			return fmt.Errorf("empty parent ID")
		}
		if seen[parent] {
			return fmt.Errorf("duplicate parent ID: %s", parent)
		}
		seen[parent] = true
	}

	return nil
}
