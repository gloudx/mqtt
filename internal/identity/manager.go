package identity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type IdentityManager struct {
	keyPair     *KeyPair
	did         *DID
	didDocument *DIDDocument
	resolver    DIDResolver // Резолвер для внешних DID
	//
	storePath string // Путь для сохранения данных
}

// PersistedIdentityManager структура для сохранения в файл
type PersistedIdentityManager struct {
	DID           *DID         `json:"did"`
	DIDDocument   *DIDDocument `json:"did_document"`
	PrivateKeyRaw []byte       `json:"private_key_raw"`
	PublicKeyRaw  []byte       `json:"public_key_raw"`
	LastUpdated   time.Time    `json:"last_updated"`
}

// NewIdentityManager создает новый IdentityManager с заданной парой ключей
func NewIdentityManager(keyPair *KeyPair, storagePath string, resolver DIDResolver) (*IdentityManager, error) {
	did, err := GenerateDID(keyPair.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DID: %w", err)
	}
	doc, err := GenerateDIDDocument(did, keyPair.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create DID document: %w", err)
	}
	m := &IdentityManager{
		keyPair:     keyPair,
		did:         did,
		didDocument: doc,
		resolver:    resolver,
		//
		storePath: storagePath,
	}
	if storagePath != "" {
		if err := m.Save(); err != nil {
			return nil, fmt.Errorf("failed to save identity: %w", err)
		}
	}
	return m, nil
}

// Save сохраняет текущее состояние IdentityManager в файл
func (dm *IdentityManager) Save() error {
	if dm.storePath == "" {
		return fmt.Errorf("storage path not configured")
	}
	privKeyBytes, err := dm.keyPair.PrivateKey.Raw()
	if err != nil {
		return fmt.Errorf("failed to get private key bytes: %w", err)
	}
	pubKeyBytes, err := dm.keyPair.PublicKey.Raw()
	if err != nil {
		return fmt.Errorf("failed to get public key bytes: %w", err)
	}
	persisted := PersistedIdentityManager{
		DID:           dm.did,
		DIDDocument:   dm.didDocument,
		PrivateKeyRaw: privKeyBytes,
		PublicKeyRaw:  pubKeyBytes,
		LastUpdated:   dm.didDocument.Updated,
	}
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal identity: %w", err)
	}
	dir := filepath.Dir(dm.storePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.WriteFile(dm.storePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write identity file: %w", err)
	}
	return nil
}

// ResolveDID разрешает DID в DID документ
func (dm *IdentityManager) ResolveDID(didString string) (*DIDDocument, error) {
	did, err := ParseDID(didString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DID: %w", err)
	}
	// Если это наш собственный DID, возвращаем локальный документ
	if didString == dm.did.String() {
		return dm.didDocument, nil
	}
	// Для did:key можем извлечь публичный ключ напрямую
	if did.Method == DIDMethodKey {
		pubKey, err := ExtractPublicKey(*did)
		if err != nil {
			return nil, fmt.Errorf("failed to extract public key: %w", err)
		}
		// Создаем базовый DID документ из публичного ключа
		return GenerateDIDDocument(did, pubKey)
	}
	// Используем внешний резолвер если доступен
	if dm.resolver != nil {
		return dm.resolver.Resolve(didString)
	}
	return nil, fmt.Errorf("unsupported DID method '%s' and no resolver configured", did.Method)
}

func (dm *IdentityManager) GetDID() *DID {
	return dm.did
}

func (dm *IdentityManager) GetDIDDocument() *DIDDocument {
	return dm.didDocument
}

func (dm *IdentityManager) GetKeyPair() *KeyPair {
	return dm.keyPair
}
