// internal/collection/index_manager.go
package collection

import (
	"fmt"
	"mqtt-http-tunnel/internal/schema"

	"github.com/dgraph-io/badger/v4"
)

func NewIndexManager(db *badger.DB, schemaDef *schema.SchemaDefinition) *IndexManager {
	storage := NewIndexStorage(db, schemaDef.Name)
	im := &IndexManager{
		storage: storage,
		schema:  schemaDef,
		indexes: make(map[string]Index),
	}

	for _, indexDef := range schemaDef.Indexes {
		im.createIndex(indexDef)
	}

	return im
}

func (im *IndexManager) createIndex(indexDef schema.IndexDef) {
	var idx Index

	switch indexDef.Type {
	case schema.IndexExact:
		idx = NewExactIndex(im.storage, indexDef.Field)
	case schema.IndexPrefix:
		idx = NewPrefixIndex(im.storage, indexDef.Field)
	case schema.IndexRange:
		idx = NewRangeIndex(im.storage, indexDef.Field)
	case schema.IndexFullText:
		idx = NewFullTextIndex(im.storage, indexDef.Field)
	default:
		return
	}

	im.indexes[indexDef.Field] = idx
}

func (im *IndexManager) Index(docID string, doc Document) error {
	for field, idx := range im.indexes {
		value := doc[field]
		if err := idx.Add(docID, value); err != nil {
			return err
		}
	}
	return nil
}

func (im *IndexManager) Remove(docID string) error {
	return nil
}

func (im *IndexManager) Query(q Query) (*QueryResult, error) {
	var docIDs []string
	var err error

	for field, filterValue := range q.Filter {
		idx, exists := im.indexes[field]
		if !exists {
			return nil, fmt.Errorf("no index on field %s", field)
		}

		docIDs, err = idx.Search(filterValue)
		if err != nil {
			return nil, err
		}

		break
	}

	if q.Limit > 0 && len(docIDs) > q.Limit {
		docIDs = docIDs[:q.Limit]
	}

	docs := make([]Document, 0, len(docIDs))
	storage := NewStorage(im.storage.db, im.schema.Name)

	for _, docID := range docIDs {
		doc, err := storage.Get(docID)
		if err != nil {
			continue
		}
		docs = append(docs, doc)
	}

	return &QueryResult{
		Documents: docs,
		Total:     len(docs),
	}, nil
}

func (im *IndexManager) Rebuild(docs []Document) error {
	for _, doc := range docs {
		docID, ok := doc["id"].(string)
		if !ok {
			continue
		}

		if err := im.Index(docID, doc); err != nil {
			return err
		}
	}
	return nil
}
