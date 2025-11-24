// internal/collection/index_exact.go
package collection

import (
	"fmt"
)

func NewExactIndex(storage *IndexStorage, field string) *ExactIndex {
	return &ExactIndex{
		storage: storage,
		field:   field,
	}
}

func (idx *ExactIndex) Add(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%v", value)
	indexName := fmt.Sprintf("exact:%s", idx.field)
	return idx.storage.Put(indexName, strValue, docID)
}

func (idx *ExactIndex) Remove(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%v", value)
	indexName := fmt.Sprintf("exact:%s", idx.field)
	return idx.storage.Delete(indexName, strValue, docID)
}

func (idx *ExactIndex) Search(filter interface{}) ([]string, error) {
	strValue := fmt.Sprintf("%v", filter)
	indexName := fmt.Sprintf("exact:%s", idx.field)
	prefix := idx.storage.indexPrefix(indexName, strValue)
	return idx.storage.Scan(prefix)
}
