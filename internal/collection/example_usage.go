package collection

import (
	"context"
	"fmt"
	"mqtt-http-tunnel/internal/eventlog"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/schema"

	"github.com/dgraph-io/badger/v4"
)

// ExampleUsage демонстрирует использование обновленной коллекции с EventLog
func ExampleUsage() error {
	// 1. Открываем базу данных BadgerDB
	opts := badger.DefaultOptions("./data/badger")
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// 2. Создаем DID и KeyPair для узла
	keyPair, err := identity.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("failed to generate key pair: %w", err)
	}

	did, err := identity.GenerateDIDKey(keyPair.PublicKey)
	if err != nil {
		return fmt.Errorf("failed to create DID: %w", err)
	}

	// 3. Создаем registry для схем
	registry := schema.NewRegistry(db)

	// 3.5 Создаем EventLog
	storage := eventlog.NewBadgerStorage(db, "eventlog:system")
	eventLogConfig := eventlog.EventLogConfig{
		OwnerDID:           did,
		KeyPair:            keyPair,
		ClockID:            0,
		Storage:            storage,
		ConflictResolution: eventlog.LastWriteWins,
		MaxBatchSize:       100,
	}

	eventLog, err := eventlog.NewEventLog(eventLogConfig)
	if err != nil {
		panic(err)
	}

	// 4. Создаем engine для управления коллекциями
	engine := NewEngine(db, did, keyPair, registry, eventLog)

	// 5. Создаем схему для коллекции "users"
	userSchema := `
		type User {
			id: ID!
			name: String!
			email: String!
			age: Int
		}
	`

	collection, err := engine.CreateCollection(context.Background(), userSchema)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	// 6. Вставляем документ
	ctx := context.Background()
	user := Document{
		"id":    "user1",
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	}

	event, err := collection.Insert(ctx, user)
	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	fmt.Printf("Created event: %s\n", event.EventTID.String())
	fmt.Printf("Author DID: %s\n", event.AuthorDID)
	fmt.Printf("Collection: %s\n", event.Collection)
	fmt.Printf("EventType: %s\n", event.EventType)

	// 7. Обновляем документ
	updates := Document{
		"age": 31,
	}

	updateEvent, err := collection.Update(ctx, "user1", updates)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	fmt.Printf("Updated event: %s\n", updateEvent.EventTID.String())

	// 8. Получаем документ
	retrievedUser, err := collection.Get(ctx, "user1")
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}

	fmt.Printf("Retrieved user: %+v\n", retrievedUser)

	// 9. Удаляем документ
	deleteEvent, err := collection.Delete(ctx, "user1")
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	fmt.Printf("Deleted event: %s\n", deleteEvent.EventTID.String())

	return nil
}
