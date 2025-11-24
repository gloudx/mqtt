package identity

import (
	"encoding/base64"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
)

func GenerateKeyPair() (*KeyPair, error) {
	privateKey, publicKey, err := GenerateKeyPairs()
	if err != nil {
		return nil, fmt.Errorf("failed to generate keypair: %w", err)
	}
	return &KeyPair{
		PublicKey:  publicKey,
		PrivateKey: privateKey,
	}, nil
}

type KeyPair struct {
	PublicKey  crypto.PubKey
	PrivateKey crypto.PrivKey
}

func (kp *KeyPair) Sign(data []byte) ([]byte, error) {
	return kp.PrivateKey.Sign(data)
}

func (kp *KeyPair) Verify(data []byte, signature []byte) (bool, error) {
	return kp.PublicKey.Verify(data, signature)
}

func (kp *KeyPair) SignHashData(data []byte) ([]byte, error) {
	return kp.Sign(Hash(data))
}

func (kp *KeyPair) VerifyHashData(data []byte, signature []byte) (bool, error) {
	return kp.Verify(Hash(data), signature)
}

// EncodePublicKey кодирует публичный ключ в base64
func EncodePublicKey(pubKey crypto.PubKey) string {
	if pubKey == nil {
		return ""
	}
	pubKeyBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(pubKeyBytes)
}

// DecodePublicKey декодирует публичный ключ из base64
func DecodePublicKey(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, fmt.Errorf("empty encoded key")
	}
	return base64.StdEncoding.DecodeString(encoded)
}

// UnmarshalPublicKey десериализует публичный ключ
func UnmarshalPublicKey(data []byte) (crypto.PubKey, error) {
	return crypto.UnmarshalPublicKey(data)
}
