// internal/collection/engine.go
package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/eventlog"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/schema"
	"mqtt-http-tunnel/internal/tid"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type Operation string

const (
	OpCreate       Operation = "create"
	OpUpdate       Operation = "update"
	OpDelete       Operation = "delete"
	OpCreateSchema Operation = "create_schema"
	OpUpdateSchema Operation = "update_schema"
)

type Engine struct {
	db          *badger.DB
	registry    *schema.Registry
	collections map[string]*Collection
	ownerDID    *identity.DID
	keyPair     *identity.KeyPair
	eventLog    *eventlog.EventLog
	clockID     uint16
	mu          sync.RWMutex
}

type EngineConfig struct {
	DB       *badger.DB
	OwnerDID *identity.DID
	KeyPair  *identity.KeyPair
	Registry *schema.Registry
	EventLog *eventlog.EventLog
	ClockID  uint16
}

func NewEngine(db *badger.DB, ownerDID *identity.DID, keyPair *identity.KeyPair, registry *schema.Registry, eventLog *eventlog.EventLog) *Engine {
	return NewEngineWithConfig(EngineConfig{
		DB:       db,
		OwnerDID: ownerDID,
		KeyPair:  keyPair,
		Registry: registry,
		EventLog: eventLog,
		ClockID:  0,
	})
}

func NewEngineWithConfig(config EngineConfig) *Engine {
	eng := &Engine{
		db:          config.DB,
		registry:    config.Registry,
		collections: make(map[string]*Collection),
		ownerDID:    config.OwnerDID,
		keyPair:     config.KeyPair,
		eventLog:    config.EventLog,
		clockID:     config.ClockID,
	}
	return eng
}

func (e *Engine) CreateCollection(ctx context.Context, gqlSchema string) (*event.Event, error) {
	eventData := map[string]any{
		"schema": gqlSchema,
	}
	ev, err := e.eventLog.Append(ctx, "sys_schemas", string(OpCreateSchema), eventData, nil)
	if err != nil {
		return nil, fmt.Errorf("append schema event: %w", err)
	}
	if err := e.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, err
	}
	return ev, nil
}

func (e *Engine) UpdateCollection(ctx context.Context, name string, gqlSchema string) (*event.Event, error) {
	eventData := map[string]any{
		"name":   name,
		"schema": gqlSchema,
	}
	ev, err := e.eventLog.Append(ctx, "sys_schemas", string(OpUpdateSchema), eventData, nil)
	if err != nil {
		return nil, fmt.Errorf("append schema update event: %w", err)
	}
	if err := e.ApplyEvent(ctx, ev.EventTID); err != nil {
		return nil, err
	}
	return ev, nil
}

// GetCollection возвращает коллекцию по имени с double-check locking
func (e *Engine) GetCollection(name string) (*Collection, error) {
	// Первая проверка с read lock
	e.mu.RLock()
	col, ok := e.collections[name]
	e.mu.RUnlock()
	if ok {
		return col, nil
	}

	// Получаем write lock для создания
	e.mu.Lock()
	defer e.mu.Unlock()

	// Double-check: проверяем ещё раз под write lock
	if col, ok := e.collections[name]; ok {
		return col, nil
	}

	schemaDef, ok := e.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("collection %s not found", name)
	}

	col = NewCollectionWithConfig(CollectionConfig{
		DB:        e.db,
		SchemaDef: schemaDef,
		EventLog:  e.eventLog,
	})

	e.collections[name] = col

	return col, nil
}

func (e *Engine) ListCollections() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.collections))
	for name := range e.collections {
		names = append(names, name)
	}
	return names
}

func (e *Engine) LoadAll(ctx context.Context) error {
	schemas := e.registry.List()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, schemaDef := range schemas {
		col := NewCollectionWithConfig(CollectionConfig{
			DB:        e.db,
			SchemaDef: schemaDef,
			EventLog:  e.eventLog,
		})
		e.collections[schemaDef.Name] = col
	}
	return nil
}

func (e *Engine) ApplyEvent(ctx context.Context, id tid.TID) error {
	ev, err := e.eventLog.Get(ctx, id)
	if err != nil {
		return err
	}

	// Системные события схем
	if ev.Collection == "sys_schemas" {
		return e.applySchemaEvent(ctx, ev)
	}

	// События коллекций
	col, err := e.GetCollection(ev.Collection)
	if err != nil {
		return err
	}
	return col.ApplyEvent(ctx, id)
}

func (e *Engine) applySchemaEvent(ctx context.Context, ev *event.Event) error {
	var data map[string]any
	if err := json.Unmarshal(ev.Payload, &data); err != nil {
		return fmt.Errorf("unmarshal schema event: %w", err)
	}

	op := Operation(ev.EventType)
	switch op {
	case OpCreateSchema:
		return e.schemaCreate(ctx, data)
	case OpUpdateSchema:
		return e.schemaUpdate(ctx, data)
	default:
		return nil
	}
}

func (e *Engine) schemaCreate(ctx context.Context, data map[string]any) error {
	gqlSchema, ok := data["schema"].(string)
	if !ok {
		return fmt.Errorf("invalid schema event data")
	}

	schemaDef, err := e.registry.Create(ctx, gqlSchema)
	if err != nil {
		return fmt.Errorf("create schema from event: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.collections[schemaDef.Name]; exists {
		return nil
	}

	col := NewCollectionWithConfig(CollectionConfig{
		DB:        e.db,
		SchemaDef: schemaDef,
		EventLog:  e.eventLog,
	})
	e.collections[schemaDef.Name] = col

	return nil
}

func (e *Engine) schemaUpdate(ctx context.Context, data map[string]any) error {
	gqlSchema, ok := data["schema"].(string)
	if !ok {
		return fmt.Errorf("invalid schema update event data")
	}

	name, ok := data["name"].(string)
	if !ok {
		return fmt.Errorf("invalid schema update event data: missing name")
	}

	schemaDef, err := e.registry.Update(ctx, name, gqlSchema)
	if err != nil {
		return fmt.Errorf("update schema from event: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if col, exists := e.collections[name]; exists {
		col.updateSchema(schemaDef)
	}

	return nil
}