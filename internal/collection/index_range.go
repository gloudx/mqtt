// internal/collection/index_range.go
package collection

import (
	"fmt"
)

func NewRangeIndex(storage *IndexStorage, field string) *RangeIndex {
	return &RangeIndex{
		storage: storage,
		field:   field,
	}
}

func (idx *RangeIndex) Add(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%020v", value)
	indexName := fmt.Sprintf("range:%s", idx.field)
	return idx.storage.Put(indexName, strValue, docID)
}

func (idx *RangeIndex) Remove(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	strValue := fmt.Sprintf("%020v", value)
	indexName := fmt.Sprintf("range:%s", idx.field)
	return idx.storage.Delete(indexName, strValue, docID)
}

func (idx *RangeIndex) Search(filter interface{}) ([]string, error) {
	rangeFilter, ok := filter.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("range filter must be map")
	}

	min := rangeFilter["min"]
	max := rangeFilter["max"]

	indexName := fmt.Sprintf("range:%s", idx.field)
	return idx.storage.ScanRange(indexName, idx.field, min, max)
}