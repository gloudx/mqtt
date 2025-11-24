// internal/collection/index_prefix.go
package collection

import (
	"fmt"
)

func NewPrefixIndex(storage *IndexStorage, field string) *PrefixIndex {
	return &PrefixIndex{
		storage: storage,
		field:   field,
	}
}

func (idx *PrefixIndex) Add(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%v", value)
	indexName := fmt.Sprintf("prefix:%s", idx.field)
	return idx.storage.Put(indexName, strValue, docID)
}

func (idx *PrefixIndex) Remove(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%v", value)
	indexName := fmt.Sprintf("prefix:%s", idx.field)
	return idx.storage.Delete(indexName, strValue, docID)
}

func (idx *PrefixIndex) Search(filter interface{}) ([]string, error) {
	prefixValue := fmt.Sprintf("%v", filter)
	indexName := fmt.Sprintf("prefix:%s", idx.field)
	prefix := idx.storage.indexPrefix(indexName, prefixValue)
	return idx.storage.Scan(prefix)
}
