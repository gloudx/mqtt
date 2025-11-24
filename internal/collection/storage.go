package collection

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type Storage struct {
	db             *badger.DB
	collectionName string
}

func NewStorage(db *badger.DB, collectionName string) *Storage {
	return &Storage{
		db:             db,
		collectionName: collectionName,
	}
}

func (s *Storage) Put(docID string, doc Document) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	key := s.docKey(docID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (s *Storage) Get(docID string) (Document, error) {
	var doc Document
	key := s.docKey(docID)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &doc)
		})
	})
	return doc, err
}

func (s *Storage) Delete(docID string) error {
	key := s.docKey(docID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *Storage) Exists(docID string) (bool, error) {
	key := s.docKey(docID)
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Storage) Scan(prefix []byte, fn func(key, value []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			err := item.Value(func(val []byte) error {
				return fn(key, val)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Storage) docKey(docID string) []byte {
	return fmt.Appendf(nil, "\x02%s:doc:%s", s.collectionName, docID)
}

func (s *Storage) docPrefix() []byte {
	return fmt.Appendf(nil, "\x02%s:doc:", s.collectionName)
}

func (s *Storage) Count() (int, error) {
	count := 0
	prefix := s.docPrefix()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}
