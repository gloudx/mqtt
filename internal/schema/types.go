package schema

import "time"

type SchemaDefinition struct {
	Name             string
	Version          int
	Fields           []FieldDef
	SchemaDirectives []DirectiveConfig
	Indexes          []IndexDef
	ConflictPolicy   ConflictPolicy
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
