package identity

import (
	"github.com/libp2p/go-libp2p/core/crypto"
)

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

func (kp *KeyPair) HashAndSign(data []byte) ([]byte, error) {
	return kp.Sign(Hash(data))
}

func (kp *KeyPair) HashAndVerify(data []byte, signature []byte) (bool, error) {
	return kp.Verify(Hash(data), signature)
}
