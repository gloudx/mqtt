package collection

import (
	"fmt"
	"mqtt-http-tunnel/internal/schema"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

type IndexManager struct {
	db      *badger.DB // Добавлено для доступа при Remove
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

func NewIndexManager(db *badger.DB, schemaDef *schema.SchemaDefinition) *IndexManager {
	storage := NewIndexStorage(db, schemaDef.Name)
	im := &IndexManager{
		db:      db,
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

// Remove удаляет документ из всех индексов
func (im *IndexManager) Remove(docID string) error {
	// Сначала получаем документ из storage чтобы знать значения для удаления
	mainStorage := NewStorage(im.db, im.schema.Name)
	doc, err := mainStorage.Get(docID)
	if err != nil {
		// Документ уже удалён или не существует - это нормально
		return nil
	}

	for field, idx := range im.indexes {
		value := doc[field]
		if value != nil {
			if err := idx.Remove(docID, value); err != nil {
				return fmt.Errorf("failed to remove from index %s: %w", field, err)
			}
		}
	}
	return nil
}

func (im *IndexManager) Query(q Query) (*QueryResult, error) {
	var docIDs []string
	firstFilter := true

	// Пересекаем результаты по всем фильтрам
	for field, filterValue := range q.Filter {
		idx, exists := im.indexes[field]
		if !exists {
			return nil, fmt.Errorf("no index on field %s", field)
		}

		fieldDocIDs, err := idx.Search(filterValue)
		if err != nil {
			return nil, err
		}

		if firstFilter {
			docIDs = fieldDocIDs
			firstFilter = false
		} else {
			// Пересечение множеств
			docIDs = intersect(docIDs, fieldDocIDs)
		}

		if len(docIDs) == 0 {
			break // Нет смысла продолжать
		}
	}

	if len(docIDs) == 0 {
		return &QueryResult{
			Documents: []Document{},
			Total:     0,
		}, nil
	}

	// Применяем лимит до загрузки документов (оптимизация)
	if q.Limit > 0 && len(docIDs) > q.Limit+q.Offset {
		docIDs = docIDs[:q.Limit+q.Offset]
	}

	docs := make([]Document, 0, len(docIDs))
	storage := NewStorage(im.db, im.schema.Name)

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

// intersect возвращает пересечение двух слайсов
func intersect(a, b []string) []string {
	set := make(map[string]bool)
	for _, v := range a {
		set[v] = true
	}

	result := make([]string, 0)
	for _, v := range b {
		if set[v] {
			result = append(result, v)
		}
	}
	return result
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

// HasIndex проверяет наличие индекса для поля
func (im *IndexManager) HasIndex(field string) bool {
	_, exists := im.indexes[field]
	return exists
}

// ============ ExactIndex ============

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

// ============ FullTextIndex ============

func NewFullTextIndex(storage *IndexStorage, field string) *FullTextIndex {
	return &FullTextIndex{
		storage: storage,
		field:   field,
	}
}

func (idx *FullTextIndex) Add(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	text := fmt.Sprintf("%v", value)
	tokens := idx.tokenize(text)

	indexName := fmt.Sprintf("fulltext:%s", idx.field)
	for _, token := range tokens {
		if err := idx.storage.Put(indexName, token, docID); err != nil {
			return err
		}
	}

	return nil
}

func (idx *FullTextIndex) Remove(docID string, value interface{}) error {
	if value == nil {
		return nil
	}

	text := fmt.Sprintf("%v", value)
	tokens := idx.tokenize(text)

	indexName := fmt.Sprintf("fulltext:%s", idx.field)
	for _, token := range tokens {
		if err := idx.storage.Delete(indexName, token, docID); err != nil {
			return err
		}
	}

	return nil
}

func (idx *FullTextIndex) Search(filter interface{}) ([]string, error) {
	query := fmt.Sprintf("%v", filter)
	tokens := idx.tokenize(query)

	if len(tokens) == 0 {
		return []string{}, nil
	}

	indexName := fmt.Sprintf("fulltext:%s", idx.field)

	allResults := make(map[string]int)
	for _, token := range tokens {
		prefix := idx.storage.indexPrefix(indexName, token)
		docIDs, err := idx.storage.Scan(prefix)
		if err != nil {
			return nil, err
		}

		for _, docID := range docIDs {
			allResults[docID]++
		}
	}

	result := make([]string, 0, len(allResults))
	for docID := range allResults {
		result = append(result, docID)
	}

	return result, nil
}

func (idx *FullTextIndex) tokenize(text string) []string {
	text = strings.ToLower(text)
	words := strings.Fields(text)

	tokens := make([]string, 0, len(words))
	for _, word := range words {
		word = strings.Trim(word, ".,!?;:\"'")
		if len(word) > 2 {
			tokens = append(tokens, word)
		}
	}

	return tokens
}

// ============ PrefixIndex ============

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

// ============ RangeIndex ============

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

// ============ IndexStorage ============

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

	// TODO: Реализовать сравнение для разных типов
	if min != nil {
		minStr := fmt.Sprintf("%020v", min)
		if value < minStr {
			return false
		}
	}

	if max != nil {
		maxStr := fmt.Sprintf("%020v", max)
		if value > maxStr {
			return false
		}
	}

	return true
}
