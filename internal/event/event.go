package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/permissions"
	"mqtt-http-tunnel/internal/tid"
	"time"
)

// Event представляет событие в op-log
type Event struct {
	EventTID        tid.TID       `json:"eventId"`
	AuthorDID       string        `json:"authorDid"` // did:key:z6Mk... (стабильный)
	Collection      string        `json:"collection"`
	EventType       string        `json:"event_type"`
	Parents         []string      `json:"parents,omitempty"` // Причинно-следственные связи
	Payload         []byte        `json:"payload"`           // Может быть зашифровано
	Encrypted       bool          `json:"encrypted"`
	EncryptionKeyID string        `json:"encryptionKeyId,omitempty"`
	AccessControl   AccessControl `json:"accessControl"`
	Signature       []byte        `json:"signature"` // Подпись устройства
}

type AccessControl struct {
	Visibility permissions.Visibility `json:"visibility"`           // "public", "private", "shared"
	SharedWith []string               `json:"sharedWith,omitempty"` // DIDs
}

// NewEvent создаёт новое событие
func NewEvent(tID tid.TID, authorDID, collection, eventType string, payload []byte) *Event {
	return &Event{
		EventTID:   tID,
		AuthorDID:  authorDID,
		Collection: collection,
		EventType:  eventType,
		Payload:    payload,
		AccessControl: AccessControl{
			Visibility: permissions.VisibilityPublic,
		},
		Encrypted: false,
	}
}

// BytesForSigning возвращает байты для подписи
func (e *Event) BytesForSigning() ([]byte, error) {
	// Убираем Signature перед подписью
	canonical := struct {
		EventTID        string        `json:"eventId"`
		AuthorDID       string        `json:"authorDid"`
		Collection      string        `json:"collection"`
		EventType       string        `json:"event_type"`
		Parents         []string      `json:"parents,omitempty"`
		Payload         []byte        `json:"payload"`
		AccessControl   AccessControl `json:"accessControl"`
		Encrypted       bool          `json:"encrypted"`
		EncryptionKeyID string        `json:"encryptionKeyId,omitempty"`
	}{
		EventTID:        e.EventTID.String(),
		AuthorDID:       e.AuthorDID,
		Collection:      e.Collection,
		EventType:       e.EventType,
		Parents:         e.Parents,
		Payload:         e.Payload,
		AccessControl:   e.AccessControl,
		Encrypted:       e.Encrypted,
		EncryptionKeyID: e.EncryptionKeyID,
	}
	return json.Marshal(canonical)
}

// Hash вычисляет хеш события для использования в Merkle DAG
func (e *Event) Hash() ([]byte, error) {
	data, err := e.BytesForSigning()
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return hash[:], nil
}

// MarkAsEncrypted помечает событие как зашифрованное
func (e *Event) MarkAsEncrypted(keyID string) {
	e.Encrypted = true
	e.EncryptionKeyID = keyID
}

// SetVisibility устанавливает видимость
func (e *Event) SetVisibility(visibility permissions.Visibility) {
	e.AccessControl.Visibility = visibility
}

// ToJSON сериализует в JSON
func (e *Event) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON десериализует из JSON
func FromJSON(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// VerifySignature проверяет подпись события
func (e *Event) VerifySignature(publicKey interface {
	Verify([]byte, []byte) (bool, error)
}) error {
	if len(e.Signature) == 0 {
		return fmt.Errorf("event has no signature")
	}

	eventBytes, err := e.BytesForSigning()
	if err != nil {
		return fmt.Errorf("failed to get bytes for signing: %w", err)
	}

	valid, err := publicKey.Verify(eventBytes, e.Signature)
	if err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}
	if !valid {
		return fmt.Errorf("invalid signature")
	}

	return nil
}

// EventBatch пакет событий для эффективной передачи
type EventBatch struct {
	Events    []*Event `json:"events"`
	BatchID   string   `json:"batchId"`
	Timestamp int64    `json:"timestamp"`
}

// NewEventBatch создаёт новый batch
func NewEventBatch(events []*Event) *EventBatch {
	return &EventBatch{
		Events:    events,
		BatchID:   generateBatchID(),
		Timestamp: time.Now().Unix(),
	}
}

func generateBatchID() string {
	randomBytes := make([]byte, 16)
	hash := sha256.Sum256(randomBytes)
	return hex.EncodeToString(hash[:16])
}

// EventFilter фильтр для событий
type EventFilter struct {
	Collection *string
	AuthorDID  *string
	AfterSeq   *uint64
	BeforeSeq  *uint64
	AfterTS    *int64
	BeforeTS   *int64
}

// Matches проверяет соответствие события фильтру
func (f *EventFilter) Matches(e *Event) bool {
	if f.Collection != nil && e.Collection != *f.Collection {
		return false
	}
	if f.AuthorDID != nil && e.AuthorDID != *f.AuthorDID {
		return false
	}

	// Проверка временных меток
	eventTime := e.EventTID.Time().Unix()
	if f.AfterTS != nil && eventTime <= *f.AfterTS {
		return false
	}
	if f.BeforeTS != nil && eventTime >= *f.BeforeTS {
		return false
	}

	// Проверка sequence (через TID integer)
	eventSeq := e.EventTID.Integer()
	if f.AfterSeq != nil && eventSeq <= *f.AfterSeq {
		return false
	}
	if f.BeforeSeq != nil && eventSeq >= *f.BeforeSeq {
		return false
	}

	return true
}
