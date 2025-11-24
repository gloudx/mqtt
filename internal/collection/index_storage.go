// internal/collection/index_storage.go
package collection

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type IndexStorage struct {
	db             *badger.DB
	collectionName string
}

func NewIndexStorage(db *badger.DB, collectionName string) *IndexStorage {
	return &IndexStorage{
		db:             db,
		collectionName: collectionName,
	}
}

func (is *IndexStorage) Put(indexName, value, docID string) error {
	key := is.indexKey(indexName, value, docID)
	return is.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte{})
	})
}

func (is *IndexStorage) Delete(indexName, value, docID string) error {
	key := is.indexKey(indexName, value, docID)
	return is.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (is *IndexStorage) Scan(prefix []byte) ([]string, error) {
	docIDs := make([]string, 0)

	err := is.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			docID := is.extractDocID(key)
			if docID != "" {
				docIDs = append(docIDs, docID)
			}
		}
		return nil
	})

	return docIDs, err
}

func (is *IndexStorage) ScanRange(indexName, field string, min, max interface{}) ([]string, error) {
	docIDs := make([]string, 0)
	prefix := is.rangePrefix(indexName, field)

	err := is.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			value := is.extractRangeValue(key)
			
			if is.inRange(value, min, max) {
				docID := is.extractDocID(key)
				if docID != "" {
					docIDs = append(docIDs, docID)
				}
			}
		}
		return nil
	})

	return docIDs, err
}

func (is *IndexStorage) indexKey(indexName, value, docID string) []byte {
	return []byte(fmt.Sprintf("\x03%s:%s:%s:%s", is.collectionName, indexName, value, docID))
}

func (is *IndexStorage) indexPrefix(indexName, value string) []byte {
	return []byte(fmt.Sprintf("\x03%s:%s:%s:", is.collectionName, indexName, value))
}

func (is *IndexStorage) rangePrefix(indexName, field string) []byte {
	return []byte(fmt.Sprintf("\x03%s:%s:", is.collectionName, indexName))
}

func (is *IndexStorage) extractDocID(key []byte) string {
	parts := []byte{}
	colonCount := 0
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == ':' {
			colonCount++
			if colonCount == 1 {
				return string(parts)
			}
		} else if colonCount == 0 {
			parts = append([]byte{key[i]}, parts...)
		}
	}
	return string(parts)
}

func (is *IndexStorage) extractRangeValue(key []byte) string {
	parts := string(key)
	startIdx := 0
	colonCount := 0
	
	for i, ch := range parts {
		if ch == ':' {
			colonCount++
			if colonCount == 3 {
				startIdx = i + 1
			} else if colonCount == 4 {
				return parts[startIdx:i]
			}
		}
	}
	return ""
}

func (is *IndexStorage) inRange(value string, min, max interface{}) bool {
	if min == nil && max == nil {
		return true
	}
	
	return true
}