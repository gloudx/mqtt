package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multibase"
)

func GenerateKeyPairs() (crypto.PrivKey, crypto.PubKey, error) {
	privKey, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate keypair: %w", err)
	}
	return privKey, pubKey, nil
}

func ParseDID(didString string) (*DID, error) {
	if !strings.HasPrefix(didString, "did:") {
		return nil, fmt.Errorf("invalid DID format: must start with 'did:'")
	}
	parts := strings.Split(didString, ":")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid DID format: insufficient parts")
	}
	did := &DID{
		Method:     DIDMethod(parts[1]),
		Identifier: parts[2],
		fullDID:    didString,
	}
	if len(parts) > 3 {
		remaining := strings.Join(parts[3:], ":")
		if strings.Contains(remaining, "#") {
			fragmentParts := strings.Split(remaining, "#")
			did.Identifier = parts[2] + ":" + fragmentParts[0]
			did.Fragment = fragmentParts[1]
		}
		if strings.Contains(remaining, "?") {
			queryParts := strings.Split(remaining, "?")
			did.Query = queryParts[1]
		}
	}
	return did, nil
}

func GenerateDIDKey(pubKey crypto.PubKey) (*DID, error) {

	// Получаем raw bytes публичного ключа
	pubKeyBytes, err := pubKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("failed to get raw pubkey bytes: %w", err)
	}

	// Проверяем тип ключа
	if pubKey.Type() != crypto.Ed25519 {
		return nil, fmt.Errorf("only Ed25519 keys supported, got: %d", pubKey.Type())
	}

	// Multicodec для Ed25519-pub: 0xed (237 decimal)
	// Согласно https://github.com/multiformats/multicodec/blob/master/table.csv
	multicodecPrefix := []byte{0xed, 0x01}

	// Добавляем multicodec prefix к pubkey
	multicodecPubKey := append(multicodecPrefix, pubKeyBytes...)

	// Кодируем в base58btc (z prefix)
	encoded, err := multibase.Encode(multibase.Base58BTC, multicodecPubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encode: %w", err)
	}

	return &DID{
		Method:     DIDMethodKey,
		Identifier: encoded,
	}, nil
}

func ExtractPublicKey(did DID) (crypto.PubKey, error) {
	if did.Method != DIDMethodKey {
		return nil, fmt.Errorf("only did:key supported for key extraction")
	}

	// Декодируем multibase
	_, decoded, err := multibase.Decode(did.Identifier)
	if err != nil {
		return nil, fmt.Errorf("failed to decode multibase: %w", err)
	}

	// Проверяем multicodec prefix (0xed, 0x01 для Ed25519)
	if len(decoded) < 2 || decoded[0] != 0xed || decoded[1] != 0x01 {
		return nil, fmt.Errorf("invalid multicodec prefix")
	}

	// Извлекаем сам ключ (без prefix)
	pubKeyBytes := decoded[2:]

	// Создаём libp2p crypto.PubKey
	pubKey, err := crypto.UnmarshalEd25519PublicKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pubkey: %w", err)
	}

	return pubKey, nil
}

func VerifyDIDSignature(did DID, message []byte, signature []byte) (bool, error) {
	pubKey, err := ExtractPublicKey(did)
	if err != nil {
		return false, err
	}

	ok, err := pubKey.Verify(message, signature)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func GenerateDIDDocument(did *DID, pubKey crypto.PubKey) (*DIDDocument, error) {

	now := time.Now()

	pubKeyBytes, err := pubKey.Raw()
	if err != nil {
		return nil, err
	}

	pubKeyBase58, err := multibase.Encode(multibase.Base58BTC, pubKeyBytes)
	if err != nil {
		return nil, err
	}

	didString := did.String()
	keyID := didString + "#key-1"

	verificationMethod := VerificationMethod{
		ID:                 keyID,
		Type:               "Ed25519VerificationKey2020",
		Controller:         didString,
		PublicKeyMultibase: pubKeyBase58,
	}

	services := []ServiceEndpoint{}

	// services = append(services, ServiceEndpoint{
	// 	ID:              didString + "#pds-node",
	// 	Type:            "PDSNode",
	// 	ServiceEndpoint: fmt.Sprintf("/p2p/%s", dm.node.host.ID().String()),
	// 	Description:     "PDS Node P2P endpoint",
	// })

	// services = append(services, ServiceEndpoint{
	// 	ID:              didString + "#http-api",
	// 	Type:            "PDSHttpAPI",
	// 	ServiceEndpoint: fmt.Sprintf("http://localhost:%d", dm.node.cfg.WebPort),
	// 	Description:     "PDS Node HTTP API endpoint",
	// })

	doc := &DIDDocument{
		Context: []string{
			"https://www.w3.org/ns/did/v1",
			"https://w3id.org/security/suites/ed25519-2020/v1",
		},
		ID:                 didString,
		VerificationMethod: []VerificationMethod{verificationMethod},
		Authentication:     []string{keyID},
		KeyAgreement:       []string{keyID},
		AssertionMethod:    []string{keyID},
		Service:            services,
		Created:            now,
		Updated:            now,
	}

	return doc, nil
}

func SignDIDDocument(doc *DIDDocument, privateKey crypto.PrivKey) (*DIDProof, error) {

	docCopy := *doc
	docCopy.Proof = nil

	docBytes, err := json.Marshal(docCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal DID document: %w", err)
	}

	signature, err := privateKey.Sign(docBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign DID document: %w", err)
	}

	proof := &DIDProof{
		Type:               "Ed25519Signature2020",
		Created:            time.Now(),
		VerificationMethod: doc.ID + "#key-1",
		ProofPurpose:       "assertionMethod",
		ProofValue:         base64.StdEncoding.EncodeToString(signature),
	}

	return proof, nil
}

func Hash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}
