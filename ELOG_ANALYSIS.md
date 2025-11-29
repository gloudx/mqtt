# Анализ пакета `internal/elog`

## 📋 Обзор

`internal/elog` - это **низкоуровневый пакет для работы с распределенным event log**, основанным на **DAG (Directed Acyclic Graph)** структуре с поддержкой **CRDT (Conflict-free Replicated Data Type)** семантики.

### Назначение

Пакет предоставляет фундаментальную инфраструктуру для:
- Хранения событий в append-only логе
- Организации событий в DAG структуру с parent-child связями
- Content-адресуемого хранилища (CID - Content Identifier)
- Синхронизации между узлами через MQTT
- Автоматического разрешения конфликтов при мерже

---

## 🏗️ Архитектура

### Основные компоненты

```
┌─────────────────────────────────────────────────────────────┐
│                      elog.Store                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────┐  │
│  │  Event   │──▶│   CID    │──▶│   DAG    │──▶│  Heads  │  │
│  │ (TID+    │   │ (SHA256) │   │(Parents) │   │ (Tips)  │  │
│  │  Data)   │   │          │   │          │   │         │  │
│  └──────────┘   └──────────┘   └──────────┘   └─────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │        BadgerDB Storage Layer                        │   │
│  │  • e/{TID} → Event                                   │   │
│  │  • d/{CID} → []CID (parents)                         │   │
│  │  • i/{CID} → TID                                     │   │
│  │  • meta/heads → []CID                                │   │
│  │  • meta/hlc → Timestamp                              │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└──────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      elog.Sync                               │
├─────────────────────────────────────────────────────────────┤
│  MQTT-based synchronization protocol                         │
│  • TypeHeads: broadcast current heads                        │
│  • TypeWant: request missing CIDs                            │
│  • TypeEvent: send event data                                │
└──────────────────────────────────────────────────────────────┘
```

---

## 📦 Структура файлов

### 1. **event.go** - Определение события и сериализация

**Основные типы:**

```go
type Event struct {
    TID     hlc.TID    // Timestamp ID (глобально упорядоченный)
    Parents []CID      // Родительские события (DAG)
    Data    []byte     // Полезная нагрузка
}
```

**Ключевые особенности:**

- **CID вычисление**: `SHA256(TID + sorted(Parents) + Data)`
  - Детерминированное хэширование
  - Parents сортируются для устранения неоднозначности
- **Сериализация**: Бинарный формат (BigEndian)
  ```
  [8 bytes: TID]
  [4 bytes: parent count]
  [N * 32 bytes: parent CIDs]
  [4 bytes: data length]
  [N bytes: data]
  ```
- **Валидация**: Проверка корректности при десериализации

**Определённые ошибки:**
- `ErrInvalidEvent` - некорректный формат события
- `ErrCIDMismatch` - несоответствие CID
- `ErrMissingParent` - отсутствует родительское событие
- `ErrEventExists` - событие уже существует

---

### 2. **cid.go** - Content Identifier

**Тип:**

```go
type CID [32]byte  // SHA-256 hash
```

**Методы:**

- `String()` - полное hex представление (64 символа)
- `Short()` - сокращённое представление (первые 8 символов)
- `Bytes()` - байтовое представление
- `IsZero()` - проверка на нулевой CID
- `CIDFromBytes()` - создание из байтов

**Использование:**
- Уникальная идентификация события
- Content-адресуемое хранилище
- Детерминированный граф зависимостей

---

### 3. **store.go** - Основное хранилище событий

**Ключевые префиксы BadgerDB:**

```
e/{TID}      → Event          // События по TID
d/{CID}      → []CID          // DAG: родители по CID
i/{CID}      → TID            // Индекс: CID → TID
meta/heads   → []CID          // Текущие головы графа
meta/hlc     → Timestamp      // HLC состояние
```

**Основные операции:**

#### **Append(data []byte) (*Event, error)**
- Создаёт новое событие с текущим HLC timestamp
- Parents = текущие heads
- Автоматически обновляет heads (новое событие становится единственным head)
- **Атомарная транзакция**: событие + DAG + индекс + heads + HLC

```go
event, err := store.Append([]byte("hello world"))
// TID: 2zef5s3dvr2dm
// Parents: [prev_cid]
// Data: "hello world"
```

#### **Put(event *Event) (CID, error)**
- Добавляет событие от другого узла
- Обновляет HLC на основе события
- Проверка дубликатов
- **НЕ обновляет heads автоматически** (требуется Merge)

#### **Merge(cids []CID) error**
- Обновляет heads:
  - Добавляет новые CIDs в heads
  - Удаляет их parents из heads
- Проверяет наличие всех родителей
- Идемпотентная операция

#### **Get(cid CID) (*Event, error)**
- Получение события по CID
- Двухэтапный поиск: CID → TID → Event

#### **GetByTID(tid hlc.TID) (*Event, error)**
- Прямое получение по TID

#### **Heads() []CID**
- Возвращает текущие головы графа (отсортированы)

#### **Parents(cid CID) ([]CID, error)**
- Возвращает родителей события

#### **Missing(cids []CID) []CID**
- Фильтрует отсутствующие CIDs

#### **Range(from, to hlc.TID, fn func(*Event) error) error**
- Итерация по событиям в диапазоне TID

**Статистика:**

```go
stats := store.Stats()
// map[string]int{
//   "events": 1000,
//   "heads": 1,
//   "index": 1000,
// }
```

---

### 4. **compact.go** - Компакция heads

**Проблема:**
При параллельном добавлении событий количество heads может расти, что затрудняет синхронизацию и увеличивает сложность мержа.

**Решение:**

#### **CompactHeads() error**
- Удаляет из heads те CID, которые являются ancestors других heads
- Алгоритм:
  1. Для каждого head собираем всех ancestors
  2. Проверяем, является ли head ancestor другого head
  3. Оставляем только "настоящие" головы

**Пример:**

```
До компакции:
heads = [A, B, C]
   C
  / \
 A   B

После компакции:
heads = [C]  // A и B - ancestors C
```

#### **AutoCompact(threshold int) error**
- Автоматическая компакция при превышении порога
- Используется в `AppendWithCompact()`

---

### 5. **sync.go** - MQTT синхронизация

**Протокол синхронизации:**

```
Node A                    MQTT Broker                Node B
   │                           │                         │
   │───TypeHeads: [cid1]──────▶│────────────────────────▶│
   │                           │                         │
   │                           │◀───TypeWant: [cid1]─────│
   │                           │                         │
   │───TypeEvent: Event────────▶│────────────────────────▶│
   │                           │                         │
   │                           │◀───TypeHeads: [cid1]────│
```

**Типы сообщений:**

1. **TypeHeads** (`elog.heads`)
   - Broadcast текущих heads
   - Периодическая отправка (configurable)
   - Запускает процесс синхронизации

2. **TypeWant** (`elog.want`)
   - Запрос отсутствующих CIDs
   - Отправляется при получении неизвестных heads

3. **TypeEvent** (`elog.event`)
   - Передача полного события
   - Отправляется в ответ на TypeWant

**Основные компоненты:**

#### **NewSync(cfg *SyncConfig) (*Sync, error)**
```go
sync, err := elog.NewSync(&elog.SyncConfig{
    Store:          store,
    Ripples:        mqttRipples,
    LogID:          "my-log",
    BroadcastEvery: 30 * time.Second,
    Logger:         logger,
})
```

**Автоматические процессы:**

- **Broadcast heads**: Периодическая отправка текущих heads
- **Cleanup loop**: Удаление expired pending запросов (timeout: 1 минута)
- **Auto-merge**: Автоматический мердж при получении всех parents

**Гарантии:**

- QoS 1 (at least once)
- Идемпотентность операций
- Защита от дублирования
- Автоматический retry при сетевых ошибках

---

### 6. **integrity.go** - Проверка целостности

**IntegrityReport:**

```go
type IntegrityReport struct {
    TotalEvents    int
    ValidEvents    int
    OrphanHeads    []CID           // heads без событий
    MissingParents map[CID][]CID   // события с отсутствующими parents
    CIDMismatches  []CID           // несоответствие CID
    DanglingIndex  []CID           // индексы без событий
}
```

**Операции:**

#### **CheckIntegrity() (*IntegrityReport, error)**
- Проверяет всё хранилище на корректность
- Находит:
  - Orphan heads (heads без событий)
  - Missing parents (события с отсутствующими родителями)
  - Dangling indexes (индексы без событий)
  - CID mismatches (некорректные CID)

#### **Repair() error**
- Автоматическое исправление проблем:
  - Удаляет orphan heads
  - Обновляет метаданные
- **Не удаляет события** (только метаданные)

---

### 7. **export.go** - Экспорт/импорт

**Формат экспорта:**

```
[4 bytes: version = 1]
[4 bytes: event count]
For each event:
  [4 bytes: event length]
  [N bytes: serialized event]
[4 bytes: heads count]
For each head:
  [32 bytes: CID]
```

**Методы:**

#### **Export(path string) error**
- Сохранение всего лога в файл
- Включает все события + heads

#### **Import(path string) error**
- Загрузка из файла
- Автоматический Put событий
- Мердж heads

#### **ExportWriter(w io.Writer) error**
- Экспорт в произвольный writer

#### **ImportReader(r io.Reader) error**
- Импорт из произвольного reader

**Применение:**
- Backup/restore
- Миграция между узлами
- Отладка и инспекция

---

### 8. **walk.go** - Обход графа

**Алгоритмы обхода:**

#### **Walk(fn func(cid CID, event *Event) error) error**
- **Топологический обход** от genesis к heads
- BFS (Breadth-First Search)
- Гарантирует обработку родителей перед детьми
- Детерминированный порядок (CID сортировка)

```go
store.Walk(func(cid CID, event *Event) error {
    fmt.Printf("%s: %s\n", cid.Short(), event.Data)
    return nil
})
```

#### **WalkFrom(cid CID, fn func(cid CID, event *Event) error) error**
- Обход от CID к genesis (ancestors)
- Рекурсивный DFS
- Полезно для "history" операций

#### **WalkHeads(fn func(cid CID, event *Event) error) error**
- Обход только текущих heads
- Быстрая операция

**Применение:**
- Replay событий
- Экспорт состояния
- Аудит и анализ

---

## 🔄 Жизненный цикл события

### 1. Локальное создание

```go
// Node A
event, err := store.Append([]byte("data"))
// → TID: 2zef5s3dvr2dm
// → CID: abc123...
// → Parents: [prev_heads]
// → Heads: [abc123...]
```

### 2. Broadcast

```go
sync.BroadcastHeads()
// → TypeHeads: {logID: "my-log", heads: ["abc123..."]}
```

### 3. Приём на Node B

```go
// handleHeads
// → Получен heads: ["abc123..."]
// → Проверка: !store.Has(abc123)
// → requestCIDs(["abc123..."])
// → TypeWant: {logID: "my-log", cids: ["abc123..."]}
```

### 4. Ответ с Node A

```go
// handleWant
// → event := store.Get(abc123)
// → sendEvent(event)
// → TypeEvent: {tid, parents, data}
```

### 5. Приём события на Node B

```go
// handleEvent
// → store.Put(event)
// → Проверка parents
// → Если все parents есть: store.Merge([cid])
// → Иначе: requestCIDs(missing_parents)
```

---

## 🎯 Ключевые особенности

### ✅ Преимущества

1. **Content-адресуемость**
   - CID = SHA256(content)
   - Детерминированная идентификация
   - Защита от подделки

2. **DAG структура**
   - Поддержка ветвления
   - Автоматическое разрешение конфликтов через мердж
   - История изменений

3. **CRDT семантика**
   - Eventually consistent
   - Append-only (immutable)
   - Автоматический мердж heads

4. **Эффективная синхронизация**
   - Инкрементальная репликация
   - Только недостающие события
   - QoS 1 гарантии

5. **Типобезопасность**
   - Строгая типизация
   - Валидация при десериализации
   - Integrity checks

### ⚠️ Ограничения

1. **Нет удаления событий**
   - Append-only дизайн
   - Требуется GC на уровне приложения

2. **Рост heads**
   - При параллельных операциях heads растёт
   - Требуется периодическая компакция

3. **Отсутствие шифрования на уровне elog**
   - Data передаётся как есть
   - Шифрование должно быть на уровне приложения

4. **Нет приоритетов событий**
   - Все события равнозначны
   - Логика приоритетов - на уровне приложения

---

## 📊 Примеры использования

### Базовое использование

```go
// Создание хранилища
store, err := elog.OpenStore(&elog.StoreConfig{
    Path:   "./data/elog",
    NodeID: 1,
    Logger: logger,
})
defer store.Close()

// Добавление события
event1, _ := store.Append([]byte("First event"))
event2, _ := store.Append([]byte("Second event"))

// Получение heads
heads := store.Heads()
fmt.Printf("Heads: %d\n", len(heads)) // 1

// Получение события
event, _ := store.Get(heads[0])
fmt.Printf("Data: %s\n", event.Data)

// Статистика
stats := store.Stats()
fmt.Printf("Events: %d, Heads: %d\n", stats["events"], stats["heads"])
```

### Синхронизация между узлами

```go
// Node A
storeA, _ := elog.OpenStore(&elog.StoreConfig{
    Path:   "./data/nodeA",
    NodeID: 1,
    Logger: logger,
})

syncA, _ := elog.NewSync(&elog.SyncConfig{
    Store:          storeA,
    Ripples:        mqttRipples,
    LogID:          "shared-log",
    BroadcastEvery: 10 * time.Second,
    Logger:         logger,
})

// Добавление события
storeA.Append([]byte("data from A"))
syncA.OnAppend() // Broadcast

// Node B (аналогично)
// → Автоматически получит событие через MQTT
```

### Компакция heads

```go
// Параллельное добавление
for i := 0; i < 100; i++ {
    go store.Append([]byte(fmt.Sprintf("event-%d", i)))
}

// Heads может вырасти до 100
fmt.Printf("Heads before: %d\n", len(store.Heads()))

// Компакция
store.CompactHeads()
fmt.Printf("Heads after: %d\n", len(store.Heads())) // Меньше

// Или автоматическая компакция
store.AppendWithCompact([]byte("data"), 10) // threshold = 10
```

### Обход графа

```go
// Топологический обход
store.Walk(func(cid elog.CID, event *elog.Event) error {
    fmt.Printf("[%s] %s: %s\n",
        event.TID.String(),
        cid.Short(),
        event.Data)
    return nil
})

// Обход от конкретного CID
store.WalkFrom(someCID, func(cid elog.CID, event *elog.Event) error {
    fmt.Printf("Ancestor: %s\n", cid.Short())
    return nil
})
```

### Экспорт/импорт

```go
// Экспорт
err := store.Export("backup.elog")

// Импорт
newStore, _ := elog.OpenStore(&elog.StoreConfig{
    Path:   "./data/restored",
    NodeID: 2,
    Logger: logger,
})
err = newStore.Import("backup.elog")

// Проверка
newStore.CheckIntegrity()
```

### Проверка целостности

```go
report, err := store.CheckIntegrity()

if !report.IsHealthy() {
    fmt.Printf("Total events: %d, Valid: %d\n",
        report.TotalEvents,
        report.ValidEvents)

    fmt.Printf("Orphan heads: %d\n", len(report.OrphanHeads))
    fmt.Printf("Missing parents: %d\n", len(report.MissingParents))

    // Автоматическое исправление
    store.Repair()
}
```

---

## 🔍 Взаимодействие с другими пакетами

### internal/hlc
- **TID (Timestamp ID)**: Глобально упорядоченные идентификаторы
- **Clock**: Гибридные логические часы для синхронизации

### internal/ripples
- **MQTT publish/subscribe** инфраструктура
- Регистрация handlers для типов сообщений
- QoS и reliability

### github.com/dgraph-io/badger/v4
- Key-Value хранилище
- Транзакции (ACID)
- Итераторы для обхода

---

## 🧪 Тестирование

### Рекомендуемые тесты

1. **Базовые операции**
   - Append → heads обновляются
   - Put → событие сохраняется
   - Merge → heads корректно обновляются

2. **Конкурентность**
   - Параллельные Append
   - Одновременные Put/Merge
   - Race conditions на heads

3. **Синхронизация**
   - Двунаправленная репликация
   - Сетевые разрывы
   - Повторная отправка

4. **Целостность**
   - CheckIntegrity выявляет проблемы
   - Repair исправляет orphan heads
   - CID валидация

5. **Экспорт/импорт**
   - Round-trip (export → import)
   - Версионность формата
   - Большие объёмы данных

---

## 🚀 Производительность

### Оптимизации

1. **Батчинг**
   - Несколько событий в одной транзакции
   - Снижение накладных расходов BadgerDB

2. **Компакция heads**
   - AutoCompact с порогом
   - Периодическая компакция

3. **Индексы**
   - CID → TID для быстрого поиска
   - DAG индекс для Parents

4. **Синхронизация**
   - Pending map для дедупликации запросов
   - Cleanup expired requests

### Метрики

```go
stats := store.Stats()
// "events": количество событий
// "heads": количество heads (должно быть близко к 1)
// "index": количество индексных записей
```

---

## 📝 Лучшие практики

### 1. Управление heads
```go
// Периодическая компакция
go func() {
    ticker := time.NewTicker(5 * time.Minute)
    for range ticker.C {
        store.CompactHeads()
    }
}()
```

### 2. Мониторинг
```go
// Логирование статистики
stats := store.Stats()
logger.Info().
    Int("events", stats["events"]).
    Int("heads", stats["heads"]).
    Msg("elog stats")

// Алерты при аномалиях
if stats["heads"] > 10 {
    logger.Warn().Msg("Too many heads, compaction needed")
}
```

### 3. Graceful shutdown
```go
// При shutdown
sync.Close()      // Остановка синхронизации
store.Close()     // Закрытие хранилища
```

### 4. Error handling
```go
event, err := store.Append(data)
if err != nil {
    logger.Error().Err(err).Msg("append failed")
    // Retry или fallback логика
    return
}
```

---

## 🔗 Связь с высокоуровневым eventlog

`internal/elog` - это **storage layer**, над которым построен `internal/eventlog`:

```
┌─────────────────────────────────────────┐
│      internal/eventlog                  │  ← Бизнес-логика
│  • Collections                          │
│  • Signatures (Ed25519)                 │
│  • Conflict resolution                  │
│  • AuthorDID, EventType                 │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│      internal/elog                      │  ← Хранилище
│  • DAG                                  │
│  • CID                                  │
│  • TID                                  │
│  • Sync (MQTT)                          │
└─────────────────────────────────────────┘
```

**Разделение ответственности:**
- `elog` - фундаментальная DAG структура и синхронизация
- `eventlog` - семантика событий, подписи, коллекции

---

## ✅ Выводы

### Сильные стороны пакета

1. **Хорошо спроектирован**
   - Чёткое разделение ответственности
   - Модульная архитектура
   - Расширяемость

2. **Надёжность**
   - ACID транзакции (BadgerDB)
   - Integrity checks
   - Repair механизм

3. **Распределённость**
   - MQTT синхронизация
   - CRDT семантика
   - Eventually consistent

4. **Производительность**
   - Content-адресуемость
   - Эффективные индексы
   - Компакция heads

### Потенциальные улучшения

1. **Тестирование**
   - Отсутствуют unit тесты (не найдены)
   - Требуются интеграционные тесты

2. **Документация**
   - Нет примеров в коде (godoc)
   - Отсутствует описание протокола синхронизации

3. **Метрики**
   - Добавить Prometheus metrics
   - Трейсинг операций

4. **GC**
   - Реализовать garbage collection старых событий
   - Pruning DAG

5. **Безопасность**
   - Добавить аутентификацию узлов
   - Ограничение размера Data

---

## 📚 Ссылки

- **Связанные пакеты:**
  - `internal/eventlog` - высокоуровневый event log
  - `internal/hlc` - гибридные логические часы
  - `internal/ripples` - MQTT publish/subscribe

- **Зависимости:**
  - `github.com/dgraph-io/badger/v4` - key-value хранилище
  - `github.com/rs/zerolog` - структурное логирование

- **Документация проекта:**
  - [README.md](README.md) - общее описание проекта
  - [QUICKSTART.md](QUICKSTART.md) - быстрый старт

---

**Дата анализа:** 2025-11-29
**Версия:** 1.0.0
**Автор:** Claude Code Analysis
