package schema

import "time"

type SchemaDefinition struct {
	Name             string
	Version          int
	Fields           []FieldDef
	SchemaDirectives []DirectiveConfig
	Indexes          []IndexDef
	ConflictPolicy   ConflictPolicy
	PrivacyConfig    *PrivacyConfig
	//
	GraphQLSchema string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type FieldDef struct {
	Name       string
	Type       FieldType
	Nullable   bool
	IsList     bool
	CRDTType   string
	Directives []DirectiveConfig // все директивы поля
}

type FieldType string

const (
	TypeString   FieldType = "String"
	TypeInt      FieldType = "Int"
	TypeFloat    FieldType = "Float"
	TypeBoolean  FieldType = "Boolean"
	TypeID       FieldType = "ID"
	TypeDateTime FieldType = "DateTime"
	TypeJSON     FieldType = "JSON"
)

type IndexDef struct {
	Field string
	Type  IndexType
}

type IndexType string

const (
	IndexExact    IndexType = "exact"
	IndexPrefix   IndexType = "prefix"
	IndexRange    IndexType = "range"
	IndexFullText IndexType = "fulltext"
)

type ConflictPolicy string

const (
	PolicyLWW  ConflictPolicy = "LWW"
	PolicyCRDT ConflictPolicy = "CRDT"
)

type DirectiveConfig struct {
	Name      string
	Arguments map[string]any
}

// CollectionEncryptionType определяет тип шифрования коллекции
type CollectionEncryptionType string

const (
	EncryptionNone     CollectionEncryptionType = "none"     // EncryptionNone - публичная коллекция без шифрования
	EncryptionPersonal CollectionEncryptionType = "personal" // EncryptionPersonal - приватная, шифруется личным ключом автора
	EncryptionGroup    CollectionEncryptionType = "group"    // EncryptionGroup - групповая, шифруется общим групповым ключом
	EncryptionP2P      CollectionEncryptionType = "p2p"      // EncryptionP2P - peer-to-peer, шифруется производным ключом между двумя участниками
	EncryptionHybrid   CollectionEncryptionType = "hybrid"   // EncryptionHybrid - гибридный режим (разные записи могут иметь разное шифрование)
)

// PrivacyConfig конфигурация шифрования для коллекции
type PrivacyConfig struct {
	Type                   CollectionEncryptionType `json:"type"`                             // Type тип шифрования коллекции
	KeyID                  string                   `json:"keyId,omitempty"`                  // KeyID идентификатор ключа (для group/p2p)
	Participants           []string                 `json:"participants,omitempty"`           // Participants список DID участников (для group/p2p)
	EncryptFields          []string                 `json:"encryptFields,omitempty"`          // EncryptFields список полей для шифрования (если пустой - шифруется весь payload)
	AllowRecipientOverride bool                     `json:"allowRecipientOverride,omitempty"` // AllowRecipientOverride разрешает автору указывать дополнительных получателей
}
