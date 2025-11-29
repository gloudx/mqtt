// internal/collection/collection.go
package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/schema"
	"mqtt-http-tunnel/internal/tid"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

type Collection struct {
	name         string
	schema       *schema.SchemaDefinition
	storage      *Storage
	indexManager *IndexManager
	validator    *Validator
	eventLog     *eventlog.EventLog
}

type Document map[string]any

type CollectionConfig struct {
	DB        *badger.DB
	SchemaDef *schema.SchemaDefinition
	EventLog  *eventlog.EventLog
}

func NewCollection(db *badger.DB, schemaDef *schema.SchemaDefinition, eventLog *eventlog.EventLog) *Collection {
	return NewCollectionWithConfig(CollectionConfig{
		DB:        db,
		SchemaDef: schemaDef,
		EventLog:  eventLog,
	})
}

func NewCollectionWithConfig(config CollectionConfig) *Collection {
	storage := NewStorage(config.DB, config.SchemaDef.Name)
	indexManager := NewIndexManager(config.DB, config.SchemaDef)

	// Создаём валидатор с поддержкой индексов для оптимизации проверки уникальности
	validator := NewValidatorWithIndex(config.SchemaDef, storage, indexManager)

	col := &Collection{
		name:         config.SchemaDef.Name,
		schema:       config.SchemaDef,
		storage:      storage,
		indexManager: indexManager,
		validator:    validator,
		eventLog:     config.EventLog,
	}
	return col
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) Schema() *schema.SchemaDefinition {
	return c.schema
}

// Insert вставляет документ с транзакционной семантикой
func (c *Collection) Insert(ctx context.Context, doc Document) (*event.Event, error) {
	docID, ok := doc["id"].(string)
	if !ok || docID == "" {
		return nil, fmt.Errorf("document must have 'id' field")
	}

	exists, err := c.storage.Exists(docID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence: %w", err)
	}
	if exists {
		return nil, fmt.Errorf("document %s already exists", docID)
	}

	// Применяем значения по умолчанию
	doc = c.validator.ApplyDefaults(doc)

	// Валидация документа
	if err := c.validator.Validate(doc); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpCreate), doc, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to append event: %w", err)
	}

	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		// Транзакционность: помечаем событие как failed (если поддерживается)
		// или логируем для последующего восстановления
		return nil, fmt.Errorf("failed to apply event (event %s recorded but not applied): %w", ev.EventTID, err)
	}

	return ev, nil
}

// Update обновляет документ с транзакционной семантикой
func (c *Collection) Update(ctx context.Context, docID string, changes Document) (*event.Event, error) {
	existing, err := c.storage.Get(docID)
	if err != nil {
		return nil, fmt.Errorf("document not found: %w", err)
	}

	// Валидация изменений (включая проверку @immutable)
	if err := c.validator.ValidateUpdate(changes, existing); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Применяем изменения к копии
	updated := make(Document, len(existing))
	for k, v := range existing {
		updated[k] = v
	}
	for k, v := range changes {
		updated[k] = v
	}

	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpUpdate), updated, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to append event: %w", err)
	}

	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, fmt.Errorf("failed to apply event (event %s recorded but not applied): %w", ev.EventTID, err)
	}

	return ev, nil
}

// Delete удаляет документ с транзакционной семантикой
func (c *Collection) Delete(ctx context.Context, docID string) (*event.Event, error) {
	exists, err := c.storage.Exists(docID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("document %s not found", docID)
	}

	doc := Document{"id": docID}

	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpDelete), doc, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to append event: %w", err)
	}

	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, fmt.Errorf("failed to apply event (event %s recorded but not applied): %w", ev.EventTID, err)
	}

	return ev, nil
}

func (c *Collection) Get(ctx context.Context, docID string) (Document, error) {
	return c.storage.Get(docID)
}

// Query параметры запроса к коллекции
type Query struct {
	Filter  map[string]interface{}
	Limit   int
	Offset  int
	OrderBy string // Поле для сортировки
	Desc    bool   // Сортировка по убыванию
}

// QueryResult результат запроса
type QueryResult struct {
	Documents []Document
	Total     int
}

func (c *Collection) Query(ctx context.Context, q Query) (*QueryResult, error) {
	var docs []Document
	var err error

	// Пробуем использовать индексы
	if c.indexManager != nil && len(q.Filter) > 0 {
		docs, err = c.queryWithIndex(ctx, q)
		if err == nil {
			return c.applyPaginationAndSort(docs, q)
		}
		// Если индекс не найден - fallback на полный скан
	}

	// Полный скан с фильтрацией
	docs, err = c.queryFullScan(ctx, q)
	if err != nil {
		return nil, err
	}

	return c.applyPaginationAndSort(docs, q)
}

// queryWithIndex использует индексы для запроса
func (c *Collection) queryWithIndex(ctx context.Context, q Query) ([]Document, error) {
	result, err := c.indexManager.Query(q)
	if err != nil {
		return nil, err
	}
	return result.Documents, nil
}

// queryFullScan выполняет полный скан с фильтрацией
func (c *Collection) queryFullScan(ctx context.Context, q Query) ([]Document, error) {
	docs := make([]Document, 0)
	prefix := c.storage.docPrefix()

	err := c.storage.Scan(prefix, func(key, value []byte) error {
		var doc Document
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}

		matches, err := c.matchesFilter(doc, q.Filter)
		if err != nil {
			// Логируем ошибку, но продолжаем сканирование
			return nil
		}
		if matches {
			docs = append(docs, doc)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	return docs, nil
}

// applyPaginationAndSort применяет сортировку и пагинацию
func (c *Collection) applyPaginationAndSort(docs []Document, q Query) (*QueryResult, error) {
	total := len(docs)

	// Сортировка
	if q.OrderBy != "" {
		c.sortDocuments(docs, q.OrderBy, q.Desc)
	}

	// Пагинация
	if q.Offset > 0 {
		if q.Offset >= len(docs) {
			docs = []Document{}
		} else {
			docs = docs[q.Offset:]
		}
	}

	if q.Limit > 0 && q.Limit < len(docs) {
		docs = docs[:q.Limit]
	}

	return &QueryResult{
		Documents: docs,
		Total:     total,
	}, nil
}

// sortDocuments сортирует документы по полю
func (c *Collection) sortDocuments(docs []Document, field string, desc bool) {
	sort.Slice(docs, func(i, j int) bool {
		vi := docs[i][field]
		vj := docs[j][field]

		less := compareValues(vi, vj)
		if desc {
			return !less
		}
		return less
	})
}

// compareValues сравнивает два значения для сортировки
func compareValues(a, b interface{}) bool {
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			return va < vb
		}
	case float64:
		if vb, ok := b.(float64); ok {
			return va < vb
		}
	case int:
		if vb, ok := b.(int); ok {
			return va < vb
		}
	case bool:
		if vb, ok := b.(bool); ok {
			return !va && vb // false < true
		}
	}
	// Для несравнимых типов - по строковому представлению
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// matchesFilter проверяет соответствие документа фильтру
// Возвращает ошибку для отладки проблем с фильтрацией
func (c *Collection) matchesFilter(doc Document, filter map[string]interface{}) (bool, error) {
	if len(filter) == 0 {
		return true, nil
	}

	for key, filterVal := range filter {
		docVal, exists := doc[key]
		if !exists {
			return false, nil
		}

		// Проверяем вложенные операторы
		if filterMap, ok := filterVal.(map[string]interface{}); ok {
			matches, err := c.matchesOperators(docVal, filterMap)
			if err != nil {
				return false, fmt.Errorf("field %s: %w", key, err)
			}
			if !matches {
				return false, nil
			}
		} else {
			// Простое сравнение равенства
			if !valuesEqual(filterVal, docVal) {
				return false, nil
			}
		}
	}

	return true, nil
}

// valuesEqual сравнивает два значения с учётом типов
func valuesEqual(a, b interface{}) bool {
	// Обработка числовых типов (JSON парсит числа как float64)
	switch va := a.(type) {
	case float64:
		switch vb := b.(type) {
		case float64:
			return va == vb
		case int:
			return va == float64(vb)
		case int64:
			return va == float64(vb)
		}
	case int:
		switch vb := b.(type) {
		case float64:
			return float64(va) == vb
		case int:
			return va == vb
		case int64:
			return int64(va) == vb
		}
	}
	return a == b
}

// matchesOperators проверяет соответствие значения операторам фильтра
// Возвращает ошибку для отладки
func (c *Collection) matchesOperators(docVal interface{}, operators map[string]interface{}) (bool, error) {
	for op, opVal := range operators {
		switch op {
		case "equals":
			if !valuesEqual(docVal, opVal) {
				return false, nil
			}
		case "contains":
			strDoc, okDoc := docVal.(string)
			strOp, okOp := opVal.(string)
			if !okDoc || !okOp {
				return false, fmt.Errorf("'contains' operator requires string values")
			}
			if !contains(strDoc, strOp) {
				return false, nil
			}
		case "startsWith":
			strDoc, okDoc := docVal.(string)
			strOp, okOp := opVal.(string)
			if !okDoc || !okOp {
				return false, fmt.Errorf("'startsWith' operator requires string values")
			}
			if !startsWith(strDoc, strOp) {
				return false, nil
			}
		case "endsWith":
			strDoc, okDoc := docVal.(string)
			strOp, okOp := opVal.(string)
			if !okDoc || !okOp {
				return false, fmt.Errorf("'endsWith' operator requires string values")
			}
			if !endsWith(strDoc, strOp) {
				return false, nil
			}
		case "gt":
			if !compareNumbers(docVal, opVal, ">") {
				return false, nil
			}
		case "gte":
			if !compareNumbers(docVal, opVal, ">=") {
				return false, nil
			}
		case "lt":
			if !compareNumbers(docVal, opVal, "<") {
				return false, nil
			}
		case "lte":
			if !compareNumbers(docVal, opVal, "<=") {
				return false, nil
			}
		case "in":
			if !inSlice(docVal, opVal) {
				return false, nil
			}
		case "ne", "notEquals":
			if valuesEqual(docVal, opVal) {
				return false, nil
			}
		case "notIn":
			if inSlice(docVal, opVal) {
				return false, nil
			}
		default:
			return false, fmt.Errorf("unknown operator: %s", op)
		}
	}
	return true, nil
}

func (c *Collection) Count(ctx context.Context) (int, error) {
	return c.storage.Count()
}

// updateSchema обновляет схему коллекции
func (c *Collection) updateSchema(schemaDef *schema.SchemaDefinition) {
	c.schema = schemaDef
	c.validator = NewValidatorWithIndex(schemaDef, c.storage, c.indexManager)
	// TODO: Возможно нужен RebuildIndexes для новых индексов
}

func (c *Collection) ApplyEvent(ctx context.Context, id tid.TID) error {
	ev, err := c.eventLog.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get event: %w", err)
	}

	// Проверка: событие должно относиться к этой коллекции
	if ev.Collection != c.name {
		return fmt.Errorf("event collection mismatch: event for '%s', but collection is '%s'", ev.Collection, c.name)
	}

	var doc Document
	if err := json.Unmarshal(ev.Payload, &doc); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	docID, ok := doc["id"].(string)
	if !ok {
		return fmt.Errorf("event payload must have 'id' field")
	}

	op := Operation(ev.EventType)
	switch op {
	case OpCreate:
		if err := c.storage.Put(docID, doc); err != nil {
			return fmt.Errorf("failed to store document: %w", err)
		}
		// Обновляем индексы
		if c.indexManager != nil {
			if err := c.indexManager.Index(docID, doc); err != nil {
				return fmt.Errorf("failed to index document: %w", err)
			}
		}
		return nil

	case OpUpdate:
		// Удаляем старые индексы
		if c.indexManager != nil {
			if err := c.indexManager.Remove(docID); err != nil {
				// Логируем, но не прерываем - документ может быть новым
			}
		}
		if err := c.storage.Put(docID, doc); err != nil {
			return fmt.Errorf("failed to store document: %w", err)
		}
		// Добавляем новые индексы
		if c.indexManager != nil {
			if err := c.indexManager.Index(docID, doc); err != nil {
				return fmt.Errorf("failed to index document: %w", err)
			}
		}
		return nil

	case OpDelete:
		// Удаляем индексы
		if c.indexManager != nil {
			if err := c.indexManager.Remove(docID); err != nil {
				// Логируем, но не прерываем
			}
		}
		return c.storage.Delete(docID)

	default:
		return fmt.Errorf("unknown operation: %s", op)
	}
}

// RebuildIndexes перестраивает все индексы
func (c *Collection) RebuildIndexes(ctx context.Context) error {
	if c.indexManager == nil {
		return nil
	}

	docs := make([]Document, 0)
	prefix := c.storage.docPrefix()

	err := c.storage.Scan(prefix, func(key, value []byte) error {
		var doc Document
		if err := json.Unmarshal(value, &doc); err != nil {
			return err
		}
		docs = append(docs, doc)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scan documents: %w", err)
	}

	return c.indexManager.Rebuild(docs)
}

// Вспомогательные функции для фильтрации

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func compareNumbers(a, b interface{}, op string) bool {
	aFloat := toFloat64(a)
	bFloat := toFloat64(b)

	switch op {
	case ">":
		return aFloat > bFloat
	case ">=":
		return aFloat >= bFloat
	case "<":
		return aFloat < bFloat
	case "<=":
		return aFloat <= bFloat
	}
	return false
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

func inSlice(val interface{}, slice interface{}) bool {
	s, ok := slice.([]interface{})
	if !ok {
		return false
	}
	for _, item := range s {
		if valuesEqual(item, val) {
			return true
		}
	}
	return false
}