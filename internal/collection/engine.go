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
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type Engine struct {
	db           *badger.DB
	registry     *schema.Registry
	collections  map[string]*Collection
	ownerDID     *identity.DID
	keyPair      *identity.KeyPair
	eventLog     *eventlog.EventLog // Единый EventLog для всей системы
	synchronizer eventlog.Synchronizer
	clockID      uint16
	mu           sync.RWMutex
}

type EngineConfig struct {
	DB           *badger.DB
	OwnerDID     *identity.DID
	KeyPair      *identity.KeyPair
	Registry     *schema.Registry
	EventLog     *eventlog.EventLog // Единый EventLog системы
	Synchronizer eventlog.Synchronizer
	ClockID      uint16
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
		db:           config.DB,
		registry:     config.Registry,
		collections:  make(map[string]*Collection),
		ownerDID:     config.OwnerDID,
		keyPair:      config.KeyPair,
		eventLog:     config.EventLog, // Используем общий EventLog
		synchronizer: config.Synchronizer,
		clockID:      config.ClockID,
	}
	return eng
}

func (e *Engine) HandleRemoteEvent(ev *event.Event) error {
	if ev.Collection == "_schemas" {
		return e.handleRemoteSchemaEvent(ev)
	}
	col, err := e.GetCollection(ev.Collection)
	if err != nil {
		return fmt.Errorf("get collection %s: %w", ev.Collection, err)
	}
	return col.handleRemoteEvent(ev)
}

func (e *Engine) CreateCollection(ctx context.Context, gqlSchema string) (*Collection, error) {
	schemaDef, err := e.registry.Create(ctx, gqlSchema)
	if err != nil {
		return nil, err
	}
	if e.eventLog != nil {
		schemaData := map[string]any{
			"operation": OpCreateSchema,
			"schema":    gqlSchema,
			"name":      schemaDef.Name,
		}
		if _, err := e.eventLog.Append(ctx, "_schemas", string(OpCreateSchema), schemaData, nil); err != nil {
			return nil, fmt.Errorf("append schema event: %w", err)
		}
	}
	col := e.createCollectionInstance(schemaDef)
	e.mu.Lock()
	e.collections[schemaDef.Name] = col
	e.mu.Unlock()
	return col, nil
}

func (e *Engine) UpdateCollection(ctx context.Context, name string, gqlSchema string) (*Collection, error) {
	schemaDef, err := e.registry.Update(ctx, name, gqlSchema)
	if err != nil {
		return nil, err
	}
	if e.eventLog != nil {
		schemaData := map[string]any{
			"operation": OpUpdateSchema,
			"schema":    gqlSchema,
			"name":      schemaDef.Name,
			"version":   schemaDef.Version,
		}
		if _, err := e.eventLog.Append(ctx, "_schemas", string(OpUpdateSchema), schemaData, nil); err != nil {
			return nil, fmt.Errorf("append schema update event: %w", err)
		}
	}
	// Обновляем коллекцию в памяти
	e.mu.Lock()
	if existingCol, ok := e.collections[name]; ok {
		// Обновляем схему в существующей коллекции
		existingCol.updateSchema(schemaDef)
		e.mu.Unlock()
		return existingCol, nil
	}
	// Если коллекции не было в памяти, создаем новую
	col := e.createCollectionInstance(schemaDef)
	e.collections[name] = col
	e.mu.Unlock()
	return col, nil
}

func (e *Engine) GetCollection(name string) (*Collection, error) {
	e.mu.RLock()
	col, ok := e.collections[name]
	e.mu.RUnlock()
	if ok {
		return col, nil
	}
	schemaDef, ok := e.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("collection %s not found", name)
	}
	col = e.createCollectionInstance(schemaDef)
	e.mu.Lock()
	e.collections[name] = col
	e.mu.Unlock()
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
		col := e.createCollectionInstance(schemaDef)
		e.collections[schemaDef.Name] = col
	}
	return nil
}

func (e *Engine) createCollectionInstance(schemaDef *schema.SchemaDefinition) *Collection {
	return NewCollectionWithConfig(CollectionConfig{
		DB:        e.db,
		SchemaDef: schemaDef,
		EventLog:  e.eventLog, // Передаем общий EventLog
	})
}

func (e *Engine) handleRemoteSchemaEvent(ev *event.Event) error {
	var data map[string]any
	if err := json.Unmarshal(ev.Payload, &data); err != nil {
		return fmt.Errorf("unmarshal schema event: %w", err)
	}
	op, _ := data["operation"].(string)
	switch Operation(op) {
	case OpCreateSchema:
		return e.handleRemoteSchemaCreate(data)
	case OpUpdateSchema:
		return e.handleRemoteSchemaUpdate(data)
	default:
		return nil
	}
}

func (e *Engine) handleRemoteSchemaCreate(data map[string]any) error {
	gqlSchema, _ := data["schema"].(string)
	name, _ := data["name"].(string)
	if gqlSchema == "" || name == "" {
		return fmt.Errorf("invalid schema event data")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.collections[name]; exists {
		return nil // Уже создана
	}
	schemaDef, err := e.registry.Create(context.Background(), gqlSchema)
	if err != nil {
		if existingSchema, ok := e.registry.Get(name); ok {
			schemaDef = existingSchema
		} else {
			return fmt.Errorf("create schema from remote event: %w", err)
		}
	}
	col := e.createCollectionInstance(schemaDef)
	e.collections[name] = col
	fmt.Printf("Created collection %s from remote event\n", name)
	return nil
}

func (e *Engine) handleRemoteSchemaUpdate(data map[string]interface{}) error {
	gqlSchema, _ := data["schema"].(string)
	name, _ := data["name"].(string)
	if gqlSchema == "" || name == "" {
		return fmt.Errorf("invalid schema update event data")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	schemaDef, err := e.registry.Update(context.Background(), name, gqlSchema)
	if err != nil {
		return fmt.Errorf("update schema from remote event: %w", err)
	}
	if col, exists := e.collections[name]; exists {
		col.updateSchema(schemaDef)
		fmt.Printf("Updated collection %s schema to version %d from remote event\n", name, schemaDef.Version)
	}
	return nil
}
