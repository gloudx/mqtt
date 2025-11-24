package identity

import (
	"fmt"
	"time"
)

// DIDMethod определяет метод DID
type DIDMethod string

const (
	DIDMethodKey  DIDMethod = "key"
	DIDMethodPeer DIDMethod = "peer"
)

type DID struct {
	Method     DIDMethod `json:"method"`
	Identifier string    `json:"method_id"`
	Fragment   string    `json:"fragment,omitempty"`
	Query      string    `json:"query,omitempty"`
	fullDID    string
}

func (d *DID) String() string {
	if d.fullDID != "" {
		return d.fullDID
	}
	didStr := fmt.Sprintf("did:%s:%s", d.Method, d.Identifier)
	if d.Fragment != "" {
		didStr += "#" + d.Fragment
	}
	if d.Query != "" {
		didStr += "?" + d.Query
	}
	d.fullDID = didStr
	return didStr
}

type DIDDocument struct {
	Context            []string             `json:"@context"`
	ID                 string               `json:"id"`
	Controller         []string             `json:"controller,omitempty"`
	VerificationMethod []VerificationMethod `json:"verificationMethod"`
	Authentication     []string             `json:"authentication,omitempty"`
	AssertionMethod    []string             `json:"assertionMethod,omitempty"`
	KeyAgreement       []string             `json:"keyAgreement,omitempty"`
	Service            []ServiceEndpoint    `json:"service,omitempty"`
	Created            time.Time            `json:"created"`
	Updated            time.Time            `json:"updated"`
	Proof              *DIDProof            `json:"proof,omitempty"`
}

type VerificationMethod struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase,omitempty"`
	PublicKeyBase64    string `json:"publicKeyBase64,omitempty"`
}

type ServiceEndpoint struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
	Description     string `json:"description,omitempty"`
}

type DIDProof struct {
	Type               string    `json:"type"`
	Created            time.Time `json:"created"`
	VerificationMethod string    `json:"verificationMethod"`
	ProofPurpose       string    `json:"proofPurpose"`
	ProofValue         string    `json:"proofValue"`
}

type IdentityManager struct {
	keyPair   *KeyPair
	did       *DID
	didDoc    *DIDDocument
	storePath string      // Путь для сохранения данных
	resolver  DIDResolver // Резолвер для внешних DID
}

// DIDResolver интерфейс для резолюции внешних DID
type DIDResolver interface {
	Resolve(didString string) (*DIDDocument, error)
}

// PersistedIdentity структура для сохранения в файл
type PersistedIdentity struct {
	DID           *DID         `json:"did"`
	DIDDocument   *DIDDocument `json:"did_document"`
	PrivateKeyRaw []byte       `json:"private_key_raw"`
	PublicKeyRaw  []byte       `json:"public_key_raw"`
	LastUpdated   time.Time    `json:"last_updated"`
}
