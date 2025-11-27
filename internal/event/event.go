package event

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/tid"
)

// EventOptions опции создания события
type EventOptions struct {
	Encrypt bool // Для legacy метода CreateEvent
	Parents []string
}

// Event представляет событие в op-log
type Event struct {
	EventTID   tid.TID  `json:"eventId"`
	AuthorDID  string   `json:"authorDid"` // did:key:z6Mk... (стабильный)
	Collection string   `json:"collection"`
	EventType  string   `json:"event_type"`
	Parents    []string `json:"parents,omitempty"` // Причинно-следственные связи
	Payload    []byte   `json:"payload"`           // Может быть зашифровано
	Signature  []byte   `json:"signature"`         // Подпись устройства
	//
	Encrypted bool `json:"encrypted"`
}

// NewEvent создаёт новое событие
func NewEvent(tID tid.TID, authorDID, collection, eventType string, payload []byte) *Event {
	return &Event{
		EventTID:   tID,
		AuthorDID:  authorDID,
		Collection: collection,
		EventType:  eventType,
		Payload:    payload,
		Encrypted:  false,
	}
}

// BytesForSigning возвращает байты для подписи
func (e *Event) BytesForSigning() ([]byte, error) {
	canonical := struct {
		EventTID   string   `json:"eventId"`
		AuthorDID  string   `json:"authorDID"`
		Collection string   `json:"collection"`
		EventType  string   `json:"eventType"`
		Parents    []string `json:"parents"`
		Payload    []byte   `json:"payload"`
		Encrypted  bool     `json:"encrypted"`
	}{
		EventTID:   e.EventTID.String(),
		AuthorDID:  e.AuthorDID,
		Collection: e.Collection,
		EventType:  e.EventType,
		Parents:    e.Parents,
		Payload:    e.Payload,
		Encrypted:  e.Encrypted,
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

func SignEvent(e *Event, keyPair *identity.KeyPair) error {
	eventBytes, err := e.BytesForSigning()
	if err != nil {
		return fmt.Errorf("failed to get bytes for signing: %w", err)
	}
	signature, err := keyPair.Sign(eventBytes)
	if err != nil {
		return fmt.Errorf("failed to sign: %w", err)
	}
	e.Signature = signature
	return nil
}

func VerifyEventSignature(e *Event, publicKey interface {
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
