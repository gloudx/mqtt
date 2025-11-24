// internal/collection/index_types.go
package collection

import "mqtt-http-tunnel/internal/schema"

type IndexManager struct {
	storage *IndexStorage
	schema  *schema.SchemaDefinition
	indexes map[string]Index
}

type Index interface {
	Add(docID string, value interface{}) error
	Remove(docID string, value interface{}) error
	Search(filter interface{}) ([]string, error)
}

type ExactIndex struct {
	storage *IndexStorage
	field   string
}

type PrefixIndex struct {
	storage *IndexStorage
	field   string
}

type RangeIndex struct {
	storage *IndexStorage
	field   string
}

type FullTextIndex struct {
	storage *IndexStorage
	field   string
}
