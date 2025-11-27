# Улучшения функции Append

## Внесенные исправления

### 1. ✅ Добавлена поддержка шифрования
**Проблема**: Опция `options.Encrypt` игнорировалась, шифрование не выполнялось.

**Решение**:
- Добавлено поле `EncryptionKey []byte` в `EventLogConfig`
- Реализованы функции `encryptPayload()` и `decryptPayload()` с использованием XChaCha20-Poly1305
- Шифрование применяется автоматически при `options.Encrypt = true`

```go
// Пример использования шифрования
eventLog, _ := eventlog.NewEventLog(eventlog.EventLogConfig{
    OwnerDID:      did,
    KeyPair:       keyPair,
    EncryptionKey: make([]byte, 32), // 32 байта для XChaCha20-Poly1305
    Storage:       storage,
})

// Создание приватного события
event, err := eventLog.CreatePrivateEvent(ctx, "diary", "entry.created", data)
```

### 2. ✅ Исправлено обновление heads в DAG
**Проблема**: `el.heads = []tid.TID{ev.EventTID}` заменял все heads, теряя параллельные ветки.

**Решение**:
- Реализована функция `mergeTIDs()` для правильного добавления heads
- Теперь используется `el.heads = mergeTIDs(el.heads, ev.EventTID)`
- Сохраняется корректная структура DAG при параллельных записях

### 3. ✅ Добавлена проверка дубликатов
**Проблема**: Отсутствовала проверка существующих событий с таким же TID.

**Решение**:
```go
// Проверяем дубликаты перед сохранением
if _, exists := el.eventCache[ev.EventTID]; exists {
    return nil, fmt.Errorf("event %s already exists", ev.EventTID)
}
```

### 4. ✅ Убраны лишние переменные
**Проблема**: Неиспользуемая переменная `ev` объявлялась дважды.

**Решение**: Убрана лишняя декларация, код стал чище.

### 5. ✅ Улучшена обработка ошибок
**Проблема**: Ошибка `SignEvent` не проверялась на возврат.

**Решение**:
```go
if err := event.SignEvent(ev, el.config.KeyPair); err != nil {
    return nil, fmt.Errorf("failed to sign event: %w", err)
}
```

## Новые возможности

### Расшифровка событий

Добавлен файл `decrypt.go` с функциями:

```go
// Расшифровать событие
err := eventLog.DecryptEvent(ctx, event)

// Получить и расшифровать событие
event, err := eventLog.GetDecrypted(ctx, eventTID)
```

## Оставшиеся ограничения

### ⚠️ Отсутствие транзакционности
Последовательность операций не атомарна:
1. `Storage.Store()` - может успеть
2. `UpdateHeads()` - может упасть
3. `Publish()` - может упасть

**Рекомендация**: Добавить транзакционный API в интерфейс `Storage`:
```go
type Storage interface {
    BeginTx(ctx context.Context) (Transaction, error)
}

type Transaction interface {
    Store(ctx context.Context, event *Event) error
    UpdateHeads(ctx context.Context, heads []TID) error
    Commit() error
    Rollback() error
}
```

### ⚠️ Простое шифрование
Текущая реализация использует один ключ для всех событий.

**Для продакшена рекомендуется**:
- Групповое шифрование (разные ключи для разных групп пользователей)
- P2P шифрование (производные ключи для пар участников)
- Частичное шифрование полей (см. примеры в `examples/encryption/`)

## Использование

```go
package main

import (
    "context"
    "crypto/rand"
    "mqtt-http-tunnel/internal/eventlog"
    "mqtt-http-tunnel/internal/identity"
)

func main() {
    // Генерация ключа шифрования
    encryptionKey := make([]byte, 32)
    rand.Read(encryptionKey)

    // Создание EventLog с поддержкой шифрования
    config := eventlog.EventLogConfig{
        OwnerDID:      myDID,
        KeyPair:       myKeyPair,
        EncryptionKey: encryptionKey,
        Storage:       myStorage,
        ClockID:       0,
    }
    
    el, err := eventlog.NewEventLog(config)
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    // Публичное событие
    pubEvent, _ := el.CreatePublicEvent(ctx, "blog", "post.created", map[string]any{
        "title": "Public Post",
        "text":  "Everyone can read this",
    })

    // Приватное зашифрованное событие
    privEvent, _ := el.CreatePrivateEvent(ctx, "diary", "entry.created", map[string]any{
        "title": "Private Entry",
        "text":  "This is encrypted",
    })

    // Расшифровка
    decrypted, _ := el.GetDecrypted(ctx, privEvent.EventTID)
}
```

## Тесты

Рекомендуется добавить тесты для:
- ✅ Шифрования/расшифровки
- ✅ Merge heads в параллельных ветках
- ✅ Проверки дубликатов
- ⚠️ Транзакционности (после реализации)
- ⚠️ Race conditions в многопоточных сценариях
