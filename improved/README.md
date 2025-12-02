# Improved Distributed Event Store

Улучшенная реализация распределённого хранилища событий на основе теории распределённых алгоритмов (курс В.А. Захарова, МГУ).

## Компоненты

### 1. Vector Clocks (`vclock/`)

Векторные часы для **точного** определения причинно-следственного порядка.

```go
// Свойство: a ≺ b ⟺ VC(a) < VC(b)
vc1 := vclock.New()
vc1.Tick("p1")  // Локальное событие

vc2 := vclock.New()
vc2.Tick("p2")

// Проверка параллельности (a ∥ b)
if vc1.IsConcurrent(vc2) {
    // События независимы - нет причинной связи
}

// При получении сообщения
vc1.MergeAndTick(vc2, "p1")
```

**Ключевые методы:**
- `Compare(other)` — определяет отношение: Before, After, Concurrent, Equal
- `HappensBefore(other)` — проверяет a ≺ b
- `IsConcurrent(other)` — проверяет a ∥ b
- `Merge(other)` — объединение при получении сообщения
- `LeastUpperBound(clocks...)` — наименьшая верхняя граница

### 2. Hybrid Logical Clock (`hlc/`)

HLC комбинирует физическое время с логическим счётчиком.

```go
clock := hlc.New("node1")

// Локальное событие
ts := clock.Now()

// При получении сообщения
clock.Update(receivedTimestamp)

// Уникальный идентификатор
tid := clock.TID()  // 64 бита: 48 time + 8 LC + 8 random
```

**Гарантия:** `a ≺ b ⟹ HLC(a) < HLC(b)` (но не наоборот!)

### 3. Event Store (`evstore/`)

Merkle-DAG хранилище с поддержкой:
- Причинного порядка через Vector Clocks
- Детерминистического разрешения форков
- Causal Buffer для гарантии порядка доставки

```go
store, _ := evstore.OpenStore(&evstore.StoreConfig{
    Path:          "./data",
    MergeStrategy: evstore.MergeStrategyLWW,
})

// Добавление записи
record, _ := store.Append([]byte("data"))

// Проверка форка
if store.HasFork() {
    store.ResolveFork(nil)  // Создаёт merge-commit
}

// Или автоматическое разрешение
winner, _ := store.AutoResolveFork()
```

#### Record структура

```go
type Record struct {
    TID       hlc.TID              // Timestamp ID
    ProcessID string               // Создатель
    VC        *vclock.VectorClock  // Векторные часы
    Parents   []CID                // DAG связи (причины)
    Type      RecordType           // Data/Merge/Anchor
    Data      []byte               // Payload
    Signature []byte               // Опционально
}
```

#### Causal Buffer

Буферизует записи до получения всех родителей:

```go
buffer := evstore.NewCausalBuffer(store, &evstore.CausalBufferConfig{
    MaxAge: 5 * time.Minute,
})

// При получении записи
delivered, missing := buffer.Receive(record)
// delivered — записи готовые к применению (в причинном порядке)
// missing — CID которые нужно запросить
```

#### Стратегии разрешения форков

```go
// Last-Writer-Wins (по TID)
resolver := evstore.NewMergeResolver(evstore.MergeStrategyLWW)

// Лексикографически по CID (полностью детерминистично)
resolver := evstore.NewMergeResolver(evstore.MergeStrategyLexical)

// По Vector Clocks
resolver := evstore.NewMergeResolver(evstore.MergeStrategyVC)

// Кастомная
resolver := evstore.NewCustomMergeResolver(func(records []*Record) *Record {
    // Своя логика выбора победителя
})

winner := resolver.Resolve(concurrentRecords)
```

### 4. Fair Queue (`ripples/fair_queue.go`)

Очередь со справедливым планированием — реализует **слабую справедливость** из теории.

```go
queue := ripples.NewFairQueue(&ripples.FairQueueConfig{
    MaxPerSource: 100,   // Лимит на источник
    MaxTotal:     10000, // Общий лимит
})

queue.Push(envelope)       // Добавить
envelope := queue.Pop()    // Round-robin извлечение
```

**Гарантии:**
- Ни один источник не может монополизировать очередь
- Все источники обслуживаются честно (round-robin)

### 5. Ripples (`ripples/`)

MQTT коммуникация с HLC интеграцией:

```go
r := ripples.New(mqttClient, &ripples.RipplesConfig{
    OwnerDID: "node1",
    Prefix:   "myapp",
    Workers:  4,
})

// Регистрация типа сообщений
r.Register(&ripples.Config{
    Type:     "events",
    Interval: 5 * time.Second,
    Priority: ripples.PriorityHigh,
    Handlers: []ripples.Handler{
        ripples.TypedHandler(func(ctx context.Context, from string, msg MyMessage) error {
            // Обработка
            return nil
        }),
    },
})

// Отправка
r.SendJSON("events", myMessage)
```

**Улучшения:**
- HLC вместо Unix timestamp
- Fair Queue для обработки
- Приоритеты сообщений
- `WithCausalOrder()` wrapper для гарантии порядка

### 6. RPC (`rpc/`)

RPC поверх MQTT с HLC:

```go
rpc, _ := rpc.New(mqttClient, &rpc.Config{
    OwnerDID:       "node1",
    RequestTimeout: 10 * time.Second,
})

// Регистрация метода
rpc.RegisterTyped(rpc, "GetUser", func(ctx context.Context, req GetUserRequest) (User, error) {
    return db.GetUser(req.ID)
})

// Вызов
user, err := rpc.CallTyped[GetUserRequest, User](rpc, ctx, "node2", "GetUser", GetUserRequest{ID: 123})
```

**Middleware:**
```go
rpc.Use(rpc.LoggingMiddleware(logger))
rpc.Use(rpc.RecoveryMiddleware(logger))
rpc.Use(rpc.TimeoutMiddleware(5 * time.Second))
```

## Теоретические основы

### Система переходов

```
S = (C, →, I)
- C: множество конфигураций
- →: отношение переходов  
- I ⊆ C: начальные конфигурации
```

Каждая запись в Store = событие/переход системы.

### Причинно-следственный порядок (≺)

1. Если e и f на одном процессе и e раньше f → e ≺ f
2. Если s — отправка, r — приём того же сообщения → s ≺ r
3. Транзитивность: e ≺ f ∧ f ≺ g → e ≺ g

**Параллельность:** a ∥ b ⟺ ¬(a ≺ b) ∧ ¬(b ≺ a)

### Логические часы

**Лэмпорта:** a ≺ b ⟹ Θ(a) < Θ(b) (но не наоборот!)

**Векторные:** a ≺ b ⟺ VC(a) < VC(b) (точное определение причинности)

### Справедливость (Fairness)

**Слабая:** если событие постоянно допустимо — оно произойдёт

**Сильная:** если событие бесконечно часто допустимо — оно произойдёт

Fair Queue реализует слабую справедливость через round-robin и лимиты на источник.

## Структура проекта

```
improved/
├── go.mod
├── README.md
├── vclock/
│   ├── vclock.go       # Vector Clocks
│   └── vclock_test.go  # Тесты
├── hlc/
│   └── hlc.go          # Hybrid Logical Clock
├── evstore/
│   ├── cid.go          # Content Identifier
│   ├── record.go       # Record с VC
│   ├── store.go        # Event Store
│   ├── causal_buffer.go # Causal delivery
│   └── merge.go        # Fork resolution
├── ripples/
│   ├── ripples.go      # MQTT messaging
│   └── fair_queue.go   # Fair scheduling
├── rpc/
│   └── rpc.go          # RPC over MQTT
└── example/
    └── main.go         # Демонстрация
```

## Использование

```bash
go mod tidy
go build ./...
go test ./...
```

## Зависимости

- `github.com/dgraph-io/badger/v4` — KV хранилище
- `github.com/eclipse/paho.mqtt.golang` — MQTT клиент
- `github.com/rs/zerolog` — Логирование

## Литература

1. В.А. Захаров — Распределённые алгоритмы (МГУ)
2. L. Lamport — Time, Clocks, and the Ordering of Events in a Distributed System (1978)
3. C. Fidge, F. Mattern — Vector Clocks (1988)
4. Kulkarni et al. — Logical Physical Clocks and Consistent Snapshots (HLC, 2014)
