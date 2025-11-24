package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Registry struct {
	db      *badger.DB
	schemas map[string]*SchemaDefinition
	mu      sync.RWMutex
	parser  *Parser
}

func NewRegistry(db *badger.DB) *Registry {
	return &Registry{
		db:      db,
		schemas: make(map[string]*SchemaDefinition),
		parser:  NewParser(),
	}
}

// LoadAll loads all schema definitions from the database into the registry
func (r *Registry) LoadAll(ctx context.Context) error {
	return r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{0x01}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var def SchemaDefinition
				if err := json.Unmarshal(val, &def); err != nil {
					return err
				}
				// pretty print for debugging
				// b, _ := json.MarshalIndent(def, "", "  ")
				// fmt.Printf("Loaded schema:\n%s\n", string(b))
				// Store in registry
				r.mu.Lock()
				r.schemas[def.Name] = &def
				r.mu.Unlock()
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Create parses and saves a new schema definition
func (r *Registry) Create(ctx context.Context, gqlSchema string) (*SchemaDefinition, error) {
	def, err := r.parser.Parse(gqlSchema)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schemas[def.Name]; exists {
		return nil, fmt.Errorf("schema %s already exists", def.Name)
	}
	def.Version = 1
	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	def.ConflictPolicy = PolicyLWW
	if err := r.save(def); err != nil {
		return nil, fmt.Errorf("save schema: %w", err)
	}
	r.schemas[def.Name] = def
	return def, nil
}

// Get retrieves a schema definition by name
func (r *Registry) Get(name string) (*SchemaDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.schemas[name]
	return def, ok
}

// List returns all registered schema definitions
func (r *Registry) List() []*SchemaDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*SchemaDefinition, 0, len(r.schemas))
	for _, def := range r.schemas {
		result = append(result, def)
	}
	return result
}

// Update parses and updates an existing schema definition
func (r *Registry) Update(ctx context.Context, name string, gqlSchema string) (*SchemaDefinition, error) {
	def, err := r.parser.Parse(gqlSchema)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema %s not found", name)
	}
	// Validate schema compatibility
	if err := r.validateUpdate(existing, def); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}
	def.Version = existing.Version + 1
	def.CreatedAt = existing.CreatedAt
	def.UpdatedAt = time.Now()
	def.ConflictPolicy = existing.ConflictPolicy
	if err := r.save(def); err != nil {
		return nil, fmt.Errorf("save schema: %w", err)
	}
	r.schemas[def.Name] = def
	return def, nil
}

// Delete removes a schema definition by name
func (r *Registry) Delete(ctx context.Context, name string) error {
	key := r.schemaKey(name)
	err := r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.schemas, name)
	r.mu.Unlock()
	return nil
}

func (r *Registry) save(def *SchemaDefinition) error {
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	key := r.schemaKey(def.Name)
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (r *Registry) schemaKey(name string) []byte {
	return append([]byte{0x01}, []byte(name)...)
}

// Exists checks if a schema with the given name exists
func (r *Registry) Exists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.schemas[name]
	return exists
}

// validateUpdate checks for breaking changes between old and new schema
func (r *Registry) validateUpdate(old, new *SchemaDefinition) error {
	if old.Name != new.Name {
		return fmt.Errorf("schema name cannot be changed: %s -> %s", old.Name, new.Name)
	}
	// Check for removed required fields (breaking change)
	oldFields := make(map[string]FieldDef)
	for _, field := range old.Fields {
		oldFields[field.Name] = field
	}
	for _, oldField := range old.Fields {
		if !oldField.Nullable {
			// Check if required field still exists
			found := false
			for _, newField := range new.Fields {
				if newField.Name == oldField.Name {
					found = true
					// Check type compatibility
					if newField.Type != oldField.Type {
						return fmt.Errorf("field %s type changed from %s to %s", oldField.Name, oldField.Type, newField.Type)
					}
					// Check if field became nullable (allowed)
					// Check if field became non-nullable (breaking)
					if !newField.Nullable && oldField.Nullable {
						return fmt.Errorf("field %s cannot become required (non-nullable)", oldField.Name)
					}
					break
				}
			}
			if !found {
				return fmt.Errorf("required field %s was removed", oldField.Name)
			}
		}
	}
	return nil
}

// ValidateSchema validates a GraphQL schema string without saving it
func (r *Registry) ValidateSchema(gqlSchema string) error {
	_, err := r.parser.Parse(gqlSchema)
	if err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	return nil
}
