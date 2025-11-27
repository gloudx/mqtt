// internal/collection/collection.go
package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/eventlog"
	"mqtt-http-tunnel/internal/schema"
	"mqtt-http-tunnel/internal/tid"

	"github.com/dgraph-io/badger/v4"
)

type Collection struct {
	name     string
	schema   *schema.SchemaDefinition
	storage  *Storage
	eventLog *eventlog.EventLog // Ссылка на общий EventLog системы
}

type Document map[string]any

type CollectionConfig struct {
	DB        *badger.DB
	SchemaDef *schema.SchemaDefinition
	EventLog  *eventlog.EventLog // Ссылка на общий EventLog системы
}

func NewCollection(db *badger.DB, schemaDef *schema.SchemaDefinition, eventLog *eventlog.EventLog) *Collection {
	return NewCollectionWithConfig(CollectionConfig{
		DB:        db,
		SchemaDef: schemaDef,
		EventLog:  eventLog,
	})
}

func NewCollectionWithConfig(config CollectionConfig) *Collection {
	col := &Collection{
		name:     config.SchemaDef.Name,
		schema:   config.SchemaDef,
		storage:  NewStorage(config.DB, config.SchemaDef.Name),
		eventLog: config.EventLog, // Используем общий EventLog
	}
	return col
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) Schema() *schema.SchemaDefinition {
	return c.schema
}

func (c *Collection) Insert(ctx context.Context, doc Document) (*event.Event, error) {
	docID, ok := doc["id"].(string)
	if !ok || docID == "" {
		return nil, fmt.Errorf("document must have 'id' field")
	}
	exists, err := c.storage.Exists(docID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("document %s already exists", docID)
	}

	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpCreate), doc, nil)
	if err != nil {
		return nil, err
	}
	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, err
	}
	return ev, nil
}

func (c *Collection) Update(ctx context.Context, docID string, changes Document) (*event.Event, error) {
	existing, err := c.storage.Get(docID)
	if err != nil {
		return nil, err
	}
	for k, v := range changes {
		existing[k] = v
	}

	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpUpdate), existing, nil)
	if err != nil {
		return nil, err
	}
	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, err
	}
	return ev, nil
}

func (c *Collection) Delete(ctx context.Context, docID string) (*event.Event, error) {
	exists, err := c.storage.Exists(docID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("document %s not found", docID)
	}
	doc := Document{"id": docID}
	// Создаем событие через EventLog
	ev, err := c.eventLog.Append(ctx, c.name, string(OpDelete), doc, nil)
	if err != nil {
		return nil, err
	}
	// Применяем событие к локальному состоянию
	if err := c.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, err
	}
	return ev, nil
}

func (c *Collection) Get(ctx context.Context, docID string) (Document, error) {
	return c.storage.Get(docID)
}

// Query параметры запроса к коллекции
type Query struct {
	Filter map[string]interface{}
	Limit  int
	Offset int
}

// QueryResult результат запроса
type QueryResult struct {
	Documents []Document
	Total     int
}

func (c *Collection) Query(ctx context.Context, q Query) (*QueryResult, error) {

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
		return nil, err
	}

	if q.Limit > 0 && q.Offset < len(docs) {
		end := q.Offset + q.Limit
		if end > len(docs) {
			end = len(docs)
		}
		docs = docs[q.Offset:end]
	}

	return &QueryResult{
		Documents: docs,
		Total:     len(docs),
	}, nil
}

func (c *Collection) Count(ctx context.Context) (int, error) {
	return c.storage.Count()
}

// updateSchema обновляет схему коллекции
func (c *Collection) updateSchema(schemaDef *schema.SchemaDefinition) {
	c.schema = schemaDef
}

func (c *Collection) ApplyEvent(ctx context.Context, id tid.TID) error {
	ev, err := c.eventLog.Get(ctx, id)
	if err != nil {
		return err
	}
	// Проверка: событие должно относиться к этой коллекции
	if ev.Collection != c.name {
		return fmt.Errorf("event collection mismatch: event for '%s', but collection is '%s'", ev.Collection, c.name)
	}
	var doc Document
	if err := json.Unmarshal(ev.Payload, &doc); err != nil {
		return err
	}
	docID, ok := doc["id"].(string)
	if !ok {
		return fmt.Errorf("event payload must have 'id' field")
	}
	op := Operation(ev.EventType)
	switch op {
	case OpCreate, OpUpdate:
		if err := c.storage.Put(docID, doc); err != nil {
			return err
		}
		return nil
	case OpDelete:
		return c.storage.Delete(docID)
	default:
		return fmt.Errorf("unknown operation: %s", op)
	}
}
