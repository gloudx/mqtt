// secure.go
package elog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-http-tunnel/internal/hlc"
	"sync"

	"github.com/rs/zerolog"
	"golang.org/x/crypto/chacha20poly1305"
)

var (
	ErrNotEncrypted     = errors.New("event not encrypted")
	ErrNoEncryptionKey  = errors.New("encryption key not configured")
	ErrInvalidSignature = errors.New("invalid signature")
	ErrNoSigningKey     = errors.New("signing key not configured")
	ErrValidationFailed = errors.New("validation failed")
)

// SignedPayload - подписанная обёртка данных
type SignedPayload struct {
	Version    uint8  `json:"v"`
	Author     string `json:"a"`           // DID автора
	Collection string `json:"c,omitempty"` // Коллекция
	Type       string `json:"t,omitempty"` // Тип события
	Encrypted  bool   `json:"e,omitempty"` // Флаг шифрования
	Payload    []byte `json:"p"`           // Данные (возможно зашифрованные)
	Signature  []byte `json:"s"`           // Ed25519 подпись
}

const payloadVersion = 1

// SecureLog - слой над Store с криптографией
type SecureLog struct {
	store  *Store
	logger zerolog.Logger

	// Identity
	did        string
	signingKey ed25519.PrivateKey
	verifyKeys map[string]ed25519.PublicKey // DID → public key

	// Encryption
	encryptionKey []byte // 32 байта для XChaCha20-Poly1305

	// Validation
	validator EventValidator

	// Collections heads
	mu                sync.RWMutex
	headsByCollection map[string][]CID

	// Cache decrypted
	decryptCache map[CID][]byte
}

// EventValidator интерфейс валидации
type EventValidator interface {
	Validate(ctx context.Context, payload *SignedPayload) error
}

// SecureLogConfig конфигурация
type SecureLogConfig struct {
	Store         *Store
	DID           string
	SigningKey    ed25519.PrivateKey
	EncryptionKey []byte // 32 байта, опционально
	Validator     EventValidator
	Logger        zerolog.Logger
}

// NewSecureLog создаёт SecureLog
func NewSecureLog(cfg *SecureLogConfig) (*SecureLog, error) {
	if cfg.Store == nil {
		return nil, errors.New("store is required")
	}
	if cfg.DID == "" {
		return nil, errors.New("DID is required")
	}
	if cfg.SigningKey == nil {
		return nil, errors.New("signing key is required")
	}

	if cfg.EncryptionKey != nil && len(cfg.EncryptionKey) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("encryption key must be %d bytes", chacha20poly1305.KeySize)
	}

	sl := &SecureLog{
		store:             cfg.Store,
		did:               cfg.DID,
		signingKey:        cfg.SigningKey,
		encryptionKey:     cfg.EncryptionKey,
		validator:         cfg.Validator,
		verifyKeys:        make(map[string]ed25519.PublicKey),
		headsByCollection: make(map[string][]CID),
		decryptCache:      make(map[CID][]byte),
		logger:            cfg.Logger.With().Str("component", "securelog").Logger(),
	}

	// Добавляем свой публичный ключ
	sl.verifyKeys[cfg.DID] = cfg.SigningKey.Public().(ed25519.PublicKey)

	sl.logger.Info().Str("did", cfg.DID).Msg("secure log initialized")
	return sl, nil
}

// AddVerifyKey добавляет публичный ключ для верификации
func (sl *SecureLog) AddVerifyKey(did string, pubKey ed25519.PublicKey) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.verifyKeys[did] = pubKey
}

// Append добавляет подписанное событие
func (sl *SecureLog) Append(ctx context.Context, collection, eventType string, data []byte, encrypt bool) (*Event, error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	payload := data

	// Шифруем если нужно
	if encrypt {
		if sl.encryptionKey == nil {
			return nil, ErrNoEncryptionKey
		}
		encrypted, err := sl.encrypt(data)
		if err != nil {
			return nil, fmt.Errorf("encrypt: %w", err)
		}
		payload = encrypted
	}

	// Создаём SignedPayload
	sp := &SignedPayload{
		Version:    payloadVersion,
		Author:     sl.did,
		Collection: collection,
		Type:       eventType,
		Encrypted:  encrypt,
		Payload:    payload,
	}

	// Подписываем
	if err := sl.sign(sp); err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}

	// Валидируем
	if sl.validator != nil {
		if err := sl.validator.Validate(ctx, sp); err != nil {
			return nil, fmt.Errorf("validate: %w", err)
		}
	}

	// Сериализуем
	spBytes, err := json.Marshal(sp)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	// Сохраняем через Store
	event, err := sl.store.Append(spBytes)
	if err != nil {
		return nil, fmt.Errorf("store append: %w", err)
	}

	cid := event.CID()

	// Обновляем heads коллекции
	sl.updateCollectionHeads(collection, cid, event.Parents)

	sl.logger.Debug().
		Str("cid", cid.Short()).
		Str("collection", collection).
		Str("type", eventType).
		Bool("encrypted", encrypt).
		Msg("appended")

	return event, nil
}

// AppendJSON добавляет событие с JSON данными
func (sl *SecureLog) AppendJSON(ctx context.Context, collection, eventType string, data any, encrypt bool) (*Event, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal data: %w", err)
	}
	return sl.Append(ctx, collection, eventType, bytes, encrypt)
}

// Get получает и верифицирует событие
func (sl *SecureLog) Get(ctx context.Context, cid CID) (*SignedPayload, error) {
	event, err := sl.store.Get(cid)
	if err != nil {
		return nil, err
	}

	var sp SignedPayload
	if err := json.Unmarshal(event.Data, &sp); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	// Верифицируем подпись
	if err := sl.verify(&sp); err != nil {
		return nil, err
	}

	return &sp, nil
}

// GetDecrypted получает, верифицирует и расшифровывает событие
func (sl *SecureLog) GetDecrypted(ctx context.Context, cid CID) (*SignedPayload, error) {
	// Проверяем кеш
	sl.mu.RLock()
	if cached, ok := sl.decryptCache[cid]; ok {
		sl.mu.RUnlock()
		sp, _ := sl.Get(ctx, cid)
		sp.Payload = cached
		sp.Encrypted = false
		return sp, nil
	}
	sl.mu.RUnlock()

	sp, err := sl.Get(ctx, cid)
	if err != nil {
		return nil, err
	}

	if !sp.Encrypted {
		return sp, nil
	}

	if sl.encryptionKey == nil {
		return nil, ErrNoEncryptionKey
	}

	decrypted, err := sl.decrypt(sp.Payload)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	// Кешируем
	sl.mu.Lock()
	sl.decryptCache[cid] = decrypted
	sl.mu.Unlock()

	sp.Payload = decrypted
	sp.Encrypted = false

	return sp, nil
}

// Put добавляет внешнее событие с верификацией
func (sl *SecureLog) Put(event *Event) (CID, error) {
	var sp SignedPayload
	if err := json.Unmarshal(event.Data, &sp); err != nil {
		return CID{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	// Верифицируем подпись
	if err := sl.verify(&sp); err != nil {
		return CID{}, err
	}

	// Валидируем
	if sl.validator != nil {
		if err := sl.validator.Validate(context.Background(), &sp); err != nil {
			return CID{}, fmt.Errorf("validate: %w", err)
		}
	}

	// Сохраняем через Store
	cid, err := sl.store.Put(event)
	if err != nil {
		return CID{}, err
	}

	// Обновляем heads коллекции
	sl.mu.Lock()
	sl.updateCollectionHeads(sp.Collection, cid, event.Parents)
	sl.mu.Unlock()

	return cid, nil
}

// --- Криптография ---

func (sl *SecureLog) sign(sp *SignedPayload) error {
	// Подписываем: Author || Collection || Type || Encrypted || Payload
	msg := sl.signatureMessage(sp)
	sp.Signature = ed25519.Sign(sl.signingKey, msg)
	return nil
}

func (sl *SecureLog) verify(sp *SignedPayload) error {
	pubKey, ok := sl.verifyKeys[sp.Author]
	if !ok {
		// Пробуем извлечь из DID (если это did:key)
		var err error
		pubKey, err = pubKeyFromDID(sp.Author)
		if err != nil {
			return fmt.Errorf("unknown author: %s", sp.Author)
		}
		sl.mu.Lock()
		sl.verifyKeys[sp.Author] = pubKey
		sl.mu.Unlock()
	}

	msg := sl.signatureMessage(sp)
	if !ed25519.Verify(pubKey, msg, sp.Signature) {
		return ErrInvalidSignature
	}

	return nil
}

func (sl *SecureLog) signatureMessage(sp *SignedPayload) []byte {
	// Детерминированное сообщение для подписи
	msg := make([]byte, 0, len(sp.Author)+len(sp.Collection)+len(sp.Type)+1+len(sp.Payload))
	msg = append(msg, []byte(sp.Author)...)
	msg = append(msg, []byte(sp.Collection)...)
	msg = append(msg, []byte(sp.Type)...)
	if sp.Encrypted {
		msg = append(msg, 1)
	} else {
		msg = append(msg, 0)
	}
	msg = append(msg, sp.Payload...)
	return msg
}

func (sl *SecureLog) encrypt(plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(sl.encryptionKey)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (sl *SecureLog) decrypt(ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(sl.encryptionKey)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < aead.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}

	nonce := ciphertext[:aead.NonceSize()]
	encrypted := ciphertext[aead.NonceSize():]

	return aead.Open(nil, nonce, encrypted, nil)
}

// --- Collections ---

func (sl *SecureLog) updateCollectionHeads(collection string, newCID CID, parents []CID) {
	if collection == "" {
		return
	}

	heads := sl.headsByCollection[collection]

	// Добавляем новый head
	newHeads := []CID{newCID}

	// Убираем parents из heads
	parentSet := make(map[CID]struct{})
	for _, p := range parents {
		parentSet[p] = struct{}{}
	}

	for _, h := range heads {
		if _, isParent := parentSet[h]; !isParent && h != newCID {
			newHeads = append(newHeads, h)
		}
	}

	sl.headsByCollection[collection] = newHeads
}

// CollectionHeads возвращает heads для коллекции
func (sl *SecureLog) CollectionHeads(collection string) []CID {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	heads := sl.headsByCollection[collection]
	result := make([]CID, len(heads))
	copy(result, heads)
	return result
}

// --- Делегация к Store ---

func (sl *SecureLog) Heads() []CID             { return sl.store.Heads() }
func (sl *SecureLog) Has(cid CID) bool         { return sl.store.Has(cid) }
func (sl *SecureLog) Missing(cids []CID) []CID { return sl.store.Missing(cids) }
func (sl *SecureLog) Merge(cids []CID) error   { return sl.store.Merge(cids) }
func (sl *SecureLog) Clock() *hlc.Clock        { return sl.store.Clock() }
func (sl *SecureLog) Store() *Store            { return sl.store }

// Walk обходит события коллекции
func (sl *SecureLog) WalkCollection(collection string, fn func(cid CID, sp *SignedPayload) error) error {
	return sl.store.Walk(func(cid CID, event *Event) error {
		var sp SignedPayload
		if err := json.Unmarshal(event.Data, &sp); err != nil {
			return nil // пропускаем битые
		}
		if sp.Collection != collection {
			return nil // другая коллекция
		}
		return fn(cid, &sp)
	})
}

// --- Helpers ---

// pubKeyFromDID извлекает публичный ключ из did:key
func pubKeyFromDID(did string) (ed25519.PublicKey, error) {
	// Упрощённая реализация для did:key:z6Mk...
	// В реальности нужен полноценный парсер
	// TODO: использовать библиотеку did-key
	return nil, errors.New("did:key parsing not implemented")
}
