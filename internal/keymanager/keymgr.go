package keymanager

import (
	"encoding/json"
	"fmt"
	"sync"
)

// KeyManager управляет ключами шифрования
type KeyManager struct {
	mu sync.RWMutex
	// Семейный ключ (все члены семьи знают)
	familyKey *EncryptionKey
	// Персональные ключи (только владелец знает)
	personalKeys map[string]*EncryptionKey // DID → EncryptionKey
	// Shared ключи (ключ для группы людей)
	sharedKeys map[string]*EncryptionKey // keyID → EncryptionKey
	// Builder для создания keyID
	keyIDBuilder *KeyIDBuilder
}

// NewKeyManager создаёт новый менеджер ключей
func NewKeyManager() *KeyManager {
	return &KeyManager{
		personalKeys: make(map[string]*EncryptionKey),
		sharedKeys:   make(map[string]*EncryptionKey),
		keyIDBuilder: &KeyIDBuilder{},
	}
}

// SetFamilyKey устанавливает семейный ключ
func (km *KeyManager) SetFamilyKey(familyID string, key []byte) error {
	km.mu.Lock()
	defer km.mu.Unlock()
	keyID := km.keyIDBuilder.FamilyKeyID(familyID)
	km.familyKey = &EncryptionKey{
		KeyID:   keyID,
		KeyType: string(KeyTypeFamily),
		Key:     key,
	}
	return nil
}

// GetFamilyKey возвращает семейный ключ
func (km *KeyManager) GetFamilyKey() (*EncryptionKey, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()
	if km.familyKey == nil {
		return nil, fmt.Errorf("family key not set")
	}
	return km.familyKey, nil
}

// SetPersonalKey устанавливает персональный ключ
func (km *KeyManager) SetPersonalKey(did string, key []byte) error {
	km.mu.Lock()
	defer km.mu.Unlock()
	keyID := km.keyIDBuilder.PersonalKeyID(did)
	km.personalKeys[did] = &EncryptionKey{
		KeyID:    keyID,
		KeyType:  string(KeyTypePersonal),
		Key:      key,
		OwnerDID: did,
	}
	return nil
}

// GetPersonalKey возвращает персональный ключ
func (km *KeyManager) GetPersonalKey(did string) (*EncryptionKey, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	key, exists := km.personalKeys[did]
	if !exists {
		return nil, fmt.Errorf("personal key not found for DID: %s", did)
	}

	return key, nil
}

// CreateSharedKey создаёт новый shared ключ
func (km *KeyManager) CreateSharedKey(authorizedDIDs []string) (*EncryptionKey, error) {
	km.mu.Lock()
	defer km.mu.Unlock()

	// Генерируем ключ
	key, err := GenerateEncryptionKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate shared key: %w", err)
	}

	// Генерируем ID
	keyID := km.keyIDBuilder.SharedKeyID()

	encKey := &EncryptionKey{
		KeyID:   keyID,
		KeyType: string(KeyTypeShared),
		Key:     key,
	}

	km.sharedKeys[keyID] = encKey

	return encKey, nil
}

// AddSharedKey добавляет существующий shared ключ (получен от другой ноды)
func (km *KeyManager) AddSharedKey(keyID string, key []byte) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	km.sharedKeys[keyID] = &EncryptionKey{
		KeyID:   keyID,
		KeyType: string(KeyTypeShared),
		Key:     key,
	}

	return nil
}

// GetSharedKey возвращает shared ключ
func (km *KeyManager) GetSharedKey(keyID string) (*EncryptionKey, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	key, exists := km.sharedKeys[keyID]
	if !exists {
		return nil, fmt.Errorf("shared key not found: %s", keyID)
	}

	return key, nil
}

// GetKey возвращает ключ по ID (автоматически определяет тип)
func (km *KeyManager) GetKey(keyID string) (*EncryptionKey, error) {
	keyType, identifier, err := ParseKeyID(keyID)
	if err != nil {
		return nil, err
	}

	switch keyType {
	case "family":
		return km.GetFamilyKey()

	case "personal":
		return km.GetPersonalKey(identifier)

	case "shared":
		return km.GetSharedKey(keyID)

	default:
		return nil, fmt.Errorf("unknown key type: %s", keyType)
	}
}

// HasKey проверяет наличие ключа
func (km *KeyManager) HasKey(keyID string) bool {
	_, err := km.GetKey(keyID)
	return err == nil
}

// EncryptWithKey шифрует данные используя указанный ключ
func (km *KeyManager) EncryptWithKey(plaintext []byte, keyID string) ([]byte, error) {
	key, err := km.GetKey(keyID)
	if err != nil {
		return nil, fmt.Errorf("key not found: %w", err)
	}

	return EncryptPayload(plaintext, key.Key)
}

// DecryptWithKey расшифровывает данные используя указанный ключ
func (km *KeyManager) DecryptWithKey(ciphertext []byte, keyID string) ([]byte, error) {
	key, err := km.GetKey(keyID)
	if err != nil {
		return nil, fmt.Errorf("key not found: %w", err)
	}

	return DecryptPayload(ciphertext, key.Key)
}

// ExportPersonalKey экспортирует персональный ключ (для бэкапа)
func (km *KeyManager) ExportPersonalKey(did string) ([]byte, error) {
	key, err := km.GetPersonalKey(did)
	if err != nil {
		return nil, err
	}

	return json.Marshal(key)
}

// ImportPersonalKey импортирует персональный ключ
func (km *KeyManager) ImportPersonalKey(data []byte) error {
	var key EncryptionKey
	if err := json.Unmarshal(data, &key); err != nil {
		return fmt.Errorf("failed to unmarshal key: %w", err)
	}

	if key.KeyType != string(KeyTypePersonal) {
		return fmt.Errorf("not a personal key")
	}

	return km.SetPersonalKey(key.OwnerDID, key.Key)
}

// ListKeys возвращает список всех известных ключей
func (km *KeyManager) ListKeys() []string {
	km.mu.RLock()
	defer km.mu.RUnlock()

	keys := make([]string, 0)

	if km.familyKey != nil {
		keys = append(keys, km.familyKey.KeyID)
	}

	for did := range km.personalKeys {
		keys = append(keys, km.keyIDBuilder.PersonalKeyID(did))
	}

	for keyID := range km.sharedKeys {
		keys = append(keys, keyID)
	}

	return keys
}

// KeyInfo информация о ключе
type KeyInfo struct {
	KeyID     string `json:"keyId"`
	KeyType   string `json:"keyType"`
	OwnerDID  string `json:"ownerDid,omitempty"`
	Available bool   `json:"available"`
}

// GetKeyInfo возвращает информацию о ключе без раскрытия самого ключа
func (km *KeyManager) GetKeyInfo(keyID string) (*KeyInfo, error) {
	key, err := km.GetKey(keyID)
	if err != nil {
		// Ключ недоступен
		keyType, identifier, parseErr := ParseKeyID(keyID)
		if parseErr != nil {
			return nil, parseErr
		}

		return &KeyInfo{
			KeyID:     keyID,
			KeyType:   keyType,
			OwnerDID:  identifier,
			Available: false,
		}, nil
	}

	return &KeyInfo{
		KeyID:     key.KeyID,
		KeyType:   key.KeyType,
		OwnerDID:  key.OwnerDID,
		Available: true,
	}, nil
}
