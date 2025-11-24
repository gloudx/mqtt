package identity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// NewIdentityManagerWithStorage создает менеджер с поддержкой персистентности
func NewIdentityManager(privateKey crypto.PrivKey, publicKey crypto.PubKey, storagePath string, resolver DIDResolver) (*IdentityManager, error) {
	keyPair := &KeyPair{
		PublicKey:  publicKey,
		PrivateKey: privateKey,
	}

	did, err := GenerateDIDKey(publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DID: %w", err)
	}

	doc, err := GenerateDIDDocument(did, publicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create DID document: %w", err)
	}

	m := &IdentityManager{
		did:       did,
		didDoc:    doc,
		keyPair:   keyPair,
		storePath: storagePath,
		resolver:  resolver,
	}

	// Сохраняем при создании
	if storagePath != "" {
		if err := m.Save(); err != nil {
			return nil, fmt.Errorf("failed to save identity: %w", err)
		}
	}

	return m, nil
}

// ResolveDID разрешает DID в DID документ
func (dm *IdentityManager) ResolveDID(didString string) (*DIDDocument, error) {
	did, err := ParseDID(didString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DID: %w", err)
	}

	// Если это наш собственный DID, возвращаем локальный документ
	if didString == dm.did.String() {
		return dm.didDoc, nil
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

func (dm *IdentityManager) RotateKeys() error {
	keyPair, err := GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate new keys: %w", err)
	}
	newDID, err := GenerateDIDKey(keyPair.PublicKey)
	if err != nil {
		return fmt.Errorf("failed to generate new DID: %w", err)
	}
	newDIDDoc, err := GenerateDIDDocument(newDID, keyPair.PublicKey)
	if err != nil {
		return fmt.Errorf("failed to create new DID document: %w", err)
	}
	dm.keyPair = keyPair
	dm.did = newDID
	dm.didDoc = newDIDDoc
	return nil
}

func (dm *IdentityManager) GetDID() *DID {
	return dm.did
}

func (dm *IdentityManager) GetDIDDocument() *DIDDocument {
	return dm.didDoc
}

// GetKeyPair возвращает пару ключей
func (dm *IdentityManager) GetKeyPair() *KeyPair {
	return dm.keyPair
}

// SetResolver устанавливает резолвер для внешних DID
func (dm *IdentityManager) SetResolver(resolver DIDResolver) {
	dm.resolver = resolver
}

// Save сохраняет идентификатор в файл
func (dm *IdentityManager) Save() error {
	if dm.storePath == "" {
		return fmt.Errorf("storage path not configured")
	}

	// Получаем raw bytes ключей
	privKeyBytes, err := dm.keyPair.PrivateKey.Raw()
	if err != nil {
		return fmt.Errorf("failed to get private key bytes: %w", err)
	}

	pubKeyBytes, err := dm.keyPair.PublicKey.Raw()
	if err != nil {
		return fmt.Errorf("failed to get public key bytes: %w", err)
	}

	persisted := PersistedIdentity{
		DID:           dm.did,
		DIDDocument:   dm.didDoc,
		PrivateKeyRaw: privKeyBytes,
		PublicKeyRaw:  pubKeyBytes,
		LastUpdated:   dm.didDoc.Updated,
	}

	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal identity: %w", err)
	}

	// Создаем директорию если не существует
	dir := filepath.Dir(dm.storePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Сохраняем с безопасными правами доступа
	if err := os.WriteFile(dm.storePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write identity file: %w", err)
	}

	return nil
}

// LoadIdentityManager загружает идентификатор из файла
func LoadIdentityManager(storagePath string, resolver DIDResolver) (*IdentityManager, error) {
	data, err := os.ReadFile(storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read identity file: %w", err)
	}

	var persisted PersistedIdentity
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, fmt.Errorf("failed to unmarshal identity: %w", err)
	}

	// Восстанавливаем ключи
	privKey, err := crypto.UnmarshalEd25519PrivateKey(persisted.PrivateKeyRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	pubKey, err := crypto.UnmarshalEd25519PublicKey(persisted.PublicKeyRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal public key: %w", err)
	}

	return &IdentityManager{
		keyPair: &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		},
		did:       persisted.DID,
		didDoc:    persisted.DIDDocument,
		storePath: storagePath,
		resolver:  resolver,
	}, nil
}

// LoadOrCreateIdentityManager загружает существующий или создает новый идентификатор
func LoadOrCreateIdentityManager(storagePath string, resolver DIDResolver) (*IdentityManager, error) {
	// Пробуем загрузить существующий
	if _, err := os.Stat(storagePath); err == nil {
		return LoadIdentityManager(storagePath, resolver)
	}

	// Создаем новый
	privKey, pubKey, err := GenerateKeyPairs()
	if err != nil {
		return nil, fmt.Errorf("failed to generate keys: %w", err)
	}

	return NewIdentityManager(privKey, pubKey, storagePath, resolver)
}
