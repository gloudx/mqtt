package keymanager

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// EncryptionKey представляет ключ шифрования
type EncryptionKey struct {
	KeyID    string `json:"keyId"`              // Идентификатор ключа
	KeyType  string `json:"keyType"`            // "family", "personal", "shared"
	Key      []byte `json:"key"`                // Сам ключ (32 bytes для ChaCha20-Poly1305)
	OwnerDID string `json:"ownerDid,omitempty"` // Для personal ключей
}

// KeyType типы ключей
type KeyType string

const (
	KeyTypeFamily   KeyType = "family"   // Общий ключ семьи
	KeyTypePersonal KeyType = "personal" // Личный ключ участника
	KeyTypeShared   KeyType = "shared"   // Ключ для группы
)

// GenerateEncryptionKey генерирует новый 32-байтовый ключ
func GenerateEncryptionKey() ([]byte, error) {
	key := make([]byte, chacha20poly1305.KeySize) // 32 bytes
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	return key, nil
}

// EncryptPayload шифрует данные с использованием ChaCha20-Poly1305
// Формат: nonce (12 bytes) || ciphertext || tag (16 bytes)
func EncryptPayload(plaintext []byte, key []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d",
			chacha20poly1305.KeySize, len(key))
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Генерируем nonce
	nonce := make([]byte, aead.NonceSize()) // 12 bytes for ChaCha20-Poly1305
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Шифруем (nonce добавляется в начало)
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)

	return ciphertext, nil
}

// DecryptPayload расшифровывает данные
func DecryptPayload(ciphertext []byte, key []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d",
			chacha20poly1305.KeySize, len(key))
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Извлекаем nonce и ciphertext
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	// Расшифровываем
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// EncryptionMetadata метаданные шифрования в событии
type EncryptionMetadata struct {
	Encrypted bool   `json:"encrypted"`       // Зашифровано ли
	KeyID     string `json:"keyId,omitempty"` // ID ключа
	Algorithm string `json:"algorithm"`       // "chacha20poly1305"
}

// CreateEncryptionMetadata создаёт метаданные шифрования
func CreateEncryptionMetadata(keyID string) EncryptionMetadata {
	return EncryptionMetadata{
		Encrypted: true,
		KeyID:     keyID,
		Algorithm: "chacha20poly1305",
	}
}

// KeyIDBuilder строит ID ключа
type KeyIDBuilder struct{}

// FamilyKeyID возвращает ID семейного ключа
func (kb *KeyIDBuilder) FamilyKeyID(familyID string) string {
	return fmt.Sprintf("family:%s", familyID)
}

// PersonalKeyID возвращает ID личного ключа
func (kb *KeyIDBuilder) PersonalKeyID(did string) string {
	return fmt.Sprintf("personal:%s", did)
}

// SharedKeyID генерирует ID для shared ключа
func (kb *KeyIDBuilder) SharedKeyID() string {
	randomBytes := make([]byte, 16)
	rand.Read(randomBytes)
	return fmt.Sprintf("shared:%s", hex.EncodeToString(randomBytes))
}

// ParseKeyID парсит ID ключа
func ParseKeyID(keyID string) (keyType string, identifier string, err error) {
	for i, c := range keyID {
		if c == ':' {
			return keyID[:i], keyID[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid keyID format: %s", keyID)
}
