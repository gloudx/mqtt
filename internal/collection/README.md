# Collection Package - Integration with EventLog v2.1

## Обзор изменений

Пакет `collection` был полностью переработан для интеграции с новым распределенным EventLog на базе DID (Decentralized Identifiers).

## Ключевые изменения

### 1. Замена nodeID на ownerDID

**Было:**
```go
type Engine struct {
    db          *badger.DB
    registry    *schema.Registry
    collections map[string]*Collection
    nodeID      string  // Строковый идентификатор узла
    mu          sync.RWMutex
}

func NewEngine(db *badger.DB, nodeID string, registry *schema.Registry) *Engine
```

**Стало:**
```go
type Engine struct {
    db          *badger.DB
    registry    *schema.Registry
    collections map[string]*Collection
    ownerDID    *identity.DID      // DID узла
    keyPair     *identity.KeyPair  // Ключевая пара для подписи
    mu          sync.RWMutex
}

func NewEngine(db *badger.DB, ownerDID *identity.DID, keyPair *identity.KeyPair, registry *schema.Registry) *Engine
```

### 2. Интеграция с EventLog v2.1

**Было:** Старый EventLog (неопределенная функция `NewEventLog`)

**Стало:** Полная интеграция с `internal/eventlog`:

```go
func NewCollection(db *badger.DB, ownerDID *identity.DID, keyPair *identity.KeyPair, schemaDef *schema.SchemaDefinition) *Collection {
    eventLogConfig := eventlog.EventLogConfig{
        OwnerDID:           ownerDID,
        KeyPair:            keyPair,
        ClockID:            0,
        Storage:            nil, // TODO: Реализовать Storage adapter для badger
        Synchronizer:       nil, // Опционально
        ConflictResolution: eventlog.LastWriteWins,
        MaxBatchSize:       100,
        EnableGC:           false,
    }
    
    eventLog, err := eventlog.NewEventLog(eventLogConfig)
    if err != nil {
        panic(fmt.Sprintf("failed to create event log: %v", err))
    }

    return &Collection{
        name:      schemaDef.Name,
        schema:    schemaDef,
        storage:   NewStorage(db, schemaDef.Name),
        eventLog:  eventLog,
        indexes:   NewIndexManager(db, schemaDef),
        validator: NewValidator(schemaDef),
    }
}
```

### 3. Обновленные методы операций

#### Insert
```go
func (c *Collection) Insert(ctx context.Context, doc Document) (*event.Event, error) {
    // Валидация документа
    if err := c.Validate(doc); err != nil {
        return nil, err
    }

    // Создание события через EventLog
    ev, err := c.eventLog.Append(ctx, c.name, string(OpCreate), doc, nil)
    if err != nil {
        return nil, err
    }

    // Применение события к локальному состоянию
    if err := c.applyEvent(ev); err != nil {
        return nil, err
    }

    return ev, nil
}
```

#### Update
```go
func (c *Collection) Update(ctx context.Context, docID string, changes Document) (*event.Event, error) {
    // Получаем текущий документ
    existing, err := c.storage.Get(docID)
    if err != nil {
        return nil, err
    }

    // Применяем изменения
    for k, v := range changes {
        existing[k] = v
    }

    // Создаем событие обновления
    ev, err := c.eventLog.Append(ctx, c.name, string(OpUpdate), existing, nil)
    if err != nil {
        return nil, err
    }

    // Применяем событие
    if err := c.applyEvent(ev); err != nil {
        return nil, err
    }

    return ev, nil
}
```

#### Delete
```go
func (c *Collection) Delete(ctx context.Context, docID string) (*event.Event, error) {
    doc := Document{"id": docID}

    // Создаем событие удаления
    ev, err := c.eventLog.Append(ctx, c.name, string(OpDelete), doc, nil)
    if err != nil {
        return nil, err
    }

    // Применяем событие
    if err := c.applyEvent(ev); err != nil {
        return nil, err
    }

    return ev, nil
}
```

### 4. Обновленный applyEvent

**Было:** Работал со старой структурой `Event` с полями `Op`, `Payload`, `Timestamp`

**Стало:** Работает с новой структурой `event.Event`:

```go
func (c *Collection) applyEvent(ev *event.Event) error {
    // Десериализуем payload
    var doc Document
    if err := json.Unmarshal(ev.Payload, &doc); err != nil {
        return err
    }

    docID, ok := doc["id"].(string)
    if !ok {
        return fmt.Errorf("event payload must have 'id' field")
    }

    // Определяем операцию по EventType
    op := Operation(ev.EventType)

    switch op {
    case OpCreate, OpUpdate:
        if err := c.storage.Put(docID, doc); err != nil {
            return err
        }
        return c.indexes.Index(docID, doc)

    case OpDelete:
        if err := c.indexes.Remove(docID); err != nil {
            return err
        }
        return c.storage.Delete(docID)

    default:
        return fmt.Errorf("unknown operation: %s", op)
    }
}
```

### 5. Новые типы

Добавлены в `types.go`:

```go
// Operation представляет тип операции над документом
type Operation string

const (
    OpCreate Operation = "create"
    OpUpdate Operation = "update"
    OpDelete Operation = "delete"
)

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
```

## Преимущества новой архитектуры

### 1. Распределенная синхронизация
- События подписываются DID владельца
- Поддержка причинно-следственных связей через Parents
- DAG структура для разрешения конфликтов

### 2. Криптографическая верификация
- Каждое событие подписано приватным ключом
- Возможность проверки подлинности автора
- Защита от подделки истории

### 3. Timestamp-based ID (TID)
- Глобальное упорядочивание событий
- Уникальные идентификаторы на основе времени
- Поддержка ClockID для множественных устройств

### 4. Контроль доступа
- Public/Private/Shared события
- Шифрование приватных данных
- Гибкое управление правами доступа

### 5. Независимые DAG по коллекциям
- Каждая коллекция имеет свой DAG
- Параллельное развитие разных типов данных
- Упрощенная синхронизация по типам

## Пример использования

См. `example_usage.go`:

```go
// 1. Создаем DID и KeyPair
keyPair, _ := identity.GenerateKeyPair()
did, _ := identity.CreateDID(keyPair.PublicKeyMultibase())

// 2. Создаем Engine
engine := NewEngine(db, did, keyPair, registry)

// 3. Создаем коллекцию
collection, _ := engine.CreateCollection(ctx, userSchema)

// 4. Работаем с документами
event, _ := collection.Insert(ctx, document)
event, _ := collection.Update(ctx, docID, changes)
event, _ := collection.Delete(ctx, docID)

// Все операции возвращают *event.Event с полной информацией:
// - EventTID: уникальный timestamp-based ID
// - AuthorDID: DID автора события
// - Collection: имя коллекции
// - EventType: тип операции (create/update/delete)
// - Parents: причинно-следственные связи
// - Signature: подпись события
```

## TODO

### Критические задачи
1. **Реализовать Storage adapter** для EventLog на базе BadgerDB
   - Необходим для персистентности событий
   - Должен реализовать интерфейс `eventlog.Storage`

2. **Добавить Synchronizer** для распределенной синхронизации
   - MQTT/PubSub для обмена событиями между узлами
   - Реализация интерфейса `eventlog.Synchronizer`

### Улучшения
3. Добавить конфигурацию ClockID для множественных устройств
4. Реализовать garbage collection для старых событий
5. Добавить snapshot механизм для оптимизации
6. Улучшить обработку ошибок в NewCollection (убрать panic)
7. Добавить метрики и логирование
8. Написать юнит-тесты

## Миграция с предыдущей версии

### Изменение вызова NewEngine

**Было:**
```go
engine := collection.NewEngine(db, "node-id-123", registry)
```

**Стало:**
```go
keyPair, err := identity.GenerateKeyPair()
if err != nil {
    return err
}

did, err := identity.CreateDID(keyPair.PublicKeyMultibase())
if err != nil {
    return err
}

engine := collection.NewEngine(db, did, keyPair, registry)
```

### Изменение типа возвращаемого события

**Было:**
```go
type Event struct {
    ID        string
    Timestamp int64
    Op        Operation
    Payload   []byte
}

event, err := collection.Insert(ctx, doc) // возвращал *collection.Event
```

**Стало:**
```go
event, err := collection.Insert(ctx, doc) // возвращает *event.Event

// Доступны новые поля:
event.EventTID        // tid.TID - timestamp-based ID
event.AuthorDID       // string - DID автора
event.Collection      // string - имя коллекции
event.EventType       // string - тип операции
event.Parents         // []string - родительские события
event.Signature       // []byte - подпись
event.AccessControl   // структура контроля доступа
```

## Зависимости

- `mqtt-http-tunnel/internal/eventlog` - Новый распределенный EventLog
- `mqtt-http-tunnel/internal/event` - Типы и signer для событий
- `mqtt-http-tunnel/internal/identity` - DID и криптография
- `mqtt-http-tunnel/internal/tid` - Timestamp-based IDs
- `github.com/dgraph-io/badger/v4` - База данных

## Дополнительная документация

- [EventLog v2.1](../eventlog/EVENTLOG_V2.1.md)
- [EventLog Architecture](../eventlog/ARCHITECTURE.md)
- [Identity Package](../identity/README.md)
- [Event Package](../event/)
