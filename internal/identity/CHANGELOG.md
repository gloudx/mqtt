# Changelog - Identity Package Improvements

## Дата: 23 ноября 2025

### Исправлены критические проблемы

#### 1. ✅ NewIdentityManager() теперь использует переданные ключи
**Проблема:** Функция игнорировала параметры `privateKey` и `publicKey`, генерируя новые ключи.

**Решение:** Теперь использует переданные ключи для создания KeyPair.

```go
// До
func NewIdentityManager(privateKey crypto.PrivKey, publicKey crypto.PubKey) (*IdentityManager, error) {
    keyPair, err := GenerateKeyPair() // Игнорировались параметры!
    ...
}

// После
func NewIdentityManager(privateKey crypto.PrivKey, publicKey crypto.PubKey) (*IdentityManager, error) {
    keyPair := &KeyPair{
        PublicKey:  publicKey,
        PrivateKey: privateKey,
    }
    ...
}
```

#### 2. ✅ Добавлена персистентность (сохранение/загрузка)

**Новые функции:**
- `NewIdentityManagerWithStorage()` - создание с поддержкой персистентности
- `Save()` - сохранение идентификатора в файл (JSON)
- `LoadIdentityManager()` - загрузка из файла
- `LoadOrCreateIdentityManager()` - загрузка или создание нового

**Новый тип:**
```go
type PersistedIdentity struct {
    DID            *DID         `json:"did"`
    DIDDocument    *DIDDocument `json:"did_document"`
    PrivateKeyRaw  []byte       `json:"private_key_raw"`
    PublicKeyRaw   []byte       `json:"public_key_raw"`
    LastUpdated    time.Time    `json:"last_updated"`
}
```

**Безопасность:**
- Файлы сохраняются с правами `0600` (только владелец)
- Директории создаются с правами `0700`

#### 3. ✅ Улучшена резолюция DID

**Проблема:** Резолюция работала только для локального DID, внешние DID возвращали ошибку.

**Решение:**
- Добавлен интерфейс `DIDResolver` для подключаемых resolver'ов
- `did:key` резолвится напрямую через `ExtractPublicKey()` и `GenerateDIDDocument()`
- Поддержка внешних resolver'ов через `SetResolver()`

```go
type DIDResolver interface {
    Resolve(didString string) (*DIDDocument, error)
}
```

**Новая логика ResolveDID:**
1. Если это локальный DID → возврат локального документа
2. Если `did:key` → извлечение публичного ключа и генерация документа
3. Если настроен resolver → использование внешнего resolver'а
4. Иначе → ошибка

#### 4. ✅ Зафиксирован и документирован multicodec префикс

**Уточнение:**
- Ed25519 публичный ключ: `0xed 0x01`
- Источник: https://github.com/multiformats/multicodec/blob/master/table.csv
- Кодирование: Base58BTC (multibase prefix `z`)

**Добавлен комментарий в код:**
```go
// Multicodec для Ed25519-pub: 0xed (237 decimal)
// Согласно https://github.com/multiformats/multicodec/blob/master/table.csv
multicodecPrefix := []byte{0xed, 0x01}
```

### Дополнительные улучшения

#### Расширен IdentityManager
```go
type IdentityManager struct {
    keyPair  *KeyPair
    did      *DID
    didDoc   *DIDDocument
    storePath string      // Путь для сохранения данных
    resolver DIDResolver  // Резолвер для внешних DID
}
```

#### Новые методы
- `GetKeyPair()` - получение пары ключей
- `SetResolver()` - установка resolver'а
- `Save()` - сохранение в файл

### Документация

Создан подробный **README.md** включающий:
- Описание компонентов
- Примеры использования
- Формат хранения
- Технические детали (multicodec, криптография)
- Стандарты и спецификации
- Рекомендации по безопасности
- Известные ограничения

### Примеры использования

```go
// Создание с персистентностью
manager, err := identity.LoadOrCreateIdentityManager("./identity.json", nil)

// Резолюция внешних DID
externalDID := "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
doc, err := manager.ResolveDID(externalDID)

// Custom resolver
type HTTPResolver struct{ baseURL string }
func (r *HTTPResolver) Resolve(did string) (*identity.DIDDocument, error) { ... }
manager.SetResolver(&HTTPResolver{baseURL: "https://resolver.example.com"})

// Ротация с сохранением
manager.RotateKeys()
manager.Save()
```

### Обратная совместимость

✅ Все изменения обратно совместимы:
- `NewIdentityManager()` работает как раньше, но теперь корректно
- Добавлены новые функции, старые не изменены
- Новые поля в структурах имеют значения по умолчанию

### Тестирование

- ✅ Компиляция без ошибок: `go build ./internal/identity/...`
- ✅ Проверены все измененные файлы
- ✅ Lint ошибок нет

### Файлы изменены

1. `internal/identity/types.go` - добавлены типы для персистентности и resolver'а
2. `internal/identity/identity.go` - документирован multicodec префикс
3. `internal/identity/manager.go` - исправления и новая функциональность
4. `internal/identity/README.md` - новая документация

### Рекомендации для использования

1. **Для новых проектов:** используйте `LoadOrCreateIdentityManager()` с явным путем хранения
2. **Для миграции:** добавьте вызов `Save()` после создания identity
3. **Для распределенных систем:** реализуйте `DIDResolver` для резолюции через DHT/HTTP
4. **Безопасность:** используйте шифрование файловой системы для дополнительной защиты

### TODO (будущие улучшения)

- [ ] Поддержка других криптографических алгоритмов (secp256k1, RSA)
- [ ] Реализация did:peer метода
- [ ] Встроенная интеграция с universal resolver
- [ ] Механизм revocation для ключей
- [ ] Key rotation history с аудитом
