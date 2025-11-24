# Identity Package

Пакет `identity` предоставляет функциональность для работы с децентрализованными идентификаторами (DID) и криптографическими ключами.

## Основные компоненты

### DID (Decentralized Identifier)

Поддерживаемые методы:
- `did:key` - DID на основе публичного ключа (основной метод)
- `did:peer` - DID для peer-to-peer коммуникации (зарезервирован)

### IdentityManager

Центральный компонент для управления идентификаторами:
- Создание и управление DID
- Генерация и ротация ключей
- Резолюция DID документов
- Персистентность (сохранение/загрузка)

## Использование

### Создание нового идентификатора

```go
import "github.com/yourusername/mqtt/internal/identity"

// Генерация новых ключей
privKey, pubKey, err := identity.GenerateKeyPairs()
if err != nil {
    log.Fatal(err)
}

// Создание менеджера
manager, err := identity.NewIdentityManager(privKey, pubKey)
if err != nil {
    log.Fatal(err)
}

// Получение DID
did := manager.GetDID()
fmt.Println("My DID:", did.String())
```

### Создание с персистентностью

```go
// Создание или загрузка существующего идентификатора
storagePath := "/path/to/identity.json"
manager, err := identity.LoadOrCreateIdentityManager(storagePath, nil)
if err != nil {
    log.Fatal(err)
}

// Идентификатор автоматически сохраняется при создании
// и может быть загружен при следующем запуске
```

### Резолюция DID

```go
// Резолюция локального DID
doc, err := manager.ResolveDID(manager.GetDID().String())
if err != nil {
    log.Fatal(err)
}

// Резолюция did:key напрямую (без внешнего resolver)
externalDID := "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
doc, err = manager.ResolveDID(externalDID)
if err != nil {
    log.Fatal(err)
}

// Для других методов требуется настройка resolver
type MyResolver struct{}

func (r *MyResolver) Resolve(didString string) (*identity.DIDDocument, error) {
    // Реализация резолюции через HTTP, DHT, и т.д.
    // ...
}

manager.SetResolver(&MyResolver{})
```

### Работа с ключами

```go
// Подпись данных
keyPair := manager.GetKeyPair()
data := []byte("Hello, World!")
signature, err := keyPair.Sign(data)
if err != nil {
    log.Fatal(err)
}

// Верификация подписи
valid, err := keyPair.Verify(data, signature)
if err != nil {
    log.Fatal(err)
}
fmt.Println("Signature valid:", valid)

// Подпись с хешированием
signature, err = keyPair.SignHashData(data)
if err != nil {
    log.Fatal(err)
}

valid, err = keyPair.VerifyHashData(data, signature)
if err != nil {
    log.Fatal(err)
}
```

### Ротация ключей

```go
// Генерация новых ключей (старые безопасно заменяются)
err := manager.RotateKeys()
if err != nil {
    log.Fatal(err)
}

// Сохранение после ротации
if err := manager.Save(); err != nil {
    log.Fatal(err)
}
```

### Работа с DID документами

```go
// Получение DID документа
didDoc := manager.GetDIDDocument()

// Подпись DID документа
privKey := manager.GetKeyPair().PrivateKey
proof, err := identity.SignDIDDocument(didDoc, privKey)
if err != nil {
    log.Fatal(err)
}
didDoc.Proof = proof

// Верификация подписи через DID
message := []byte("test message")
signature, _ := privKey.Sign(message)
valid, err := identity.VerifyDIDSignature(*manager.GetDID(), message, signature)
if err != nil {
    log.Fatal(err)
}
```

## Формат хранения

Идентификаторы сохраняются в JSON формате:

```json
{
  "did": {
    "method": "key",
    "method_id": "z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
  },
  "did_document": {
    "@context": [
      "https://www.w3.org/ns/did/v1",
      "https://w3id.org/security/suites/ed25519-2020/v1"
    ],
    "id": "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH",
    "verificationMethod": [...],
    "authentication": [...],
    "created": "2025-11-23T10:00:00Z",
    "updated": "2025-11-23T10:00:00Z"
  },
  "private_key_raw": "...",
  "public_key_raw": "...",
  "last_updated": "2025-11-23T10:00:00Z"
}
```

## Технические детали

### Multicodec Prefix

Для метода `did:key` используется стандартный multicodec префикс:
- Ed25519 публичный ключ: `0xed 0x01`
- Кодирование: Base58BTC (multibase prefix `z`)

Согласно спецификации: https://github.com/multiformats/multicodec/blob/master/table.csv

### Криптография

- **Алгоритм**: Ed25519 (эллиптическая кривая)
- **Библиотека**: libp2p/go-libp2p/core/crypto
- **Хеширование**: SHA-256 (для SignHashData/VerifyHashData)

### Стандарты

- [W3C DID Core](https://www.w3.org/TR/did-core/)
- [did:key Method Specification](https://w3c-ccg.github.io/did-method-key/)
- [Multibase](https://github.com/multiformats/multibase)
- [Multicodec](https://github.com/multiformats/multicodec)

## Безопасность

- Приватные ключи сохраняются с правами доступа `0600` (только владелец)
- Директории создаются с правами `0700`
- Рекомендуется использовать шифрование файловой системы для дополнительной защиты
- При ротации ключей старые ключи полностью заменяются

## Расширение функциональности

### Реализация custom DID Resolver

```go
type HTTPResolver struct {
    baseURL string
}

func (r *HTTPResolver) Resolve(didString string) (*identity.DIDDocument, error) {
    resp, err := http.Get(r.baseURL + "/resolve?did=" + didString)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var doc identity.DIDDocument
    if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
        return nil, err
    }
    
    return &doc, nil
}

// Использование
resolver := &HTTPResolver{baseURL: "https://resolver.example.com"}
manager.SetResolver(resolver)
```

## Известные ограничения

1. Поддерживается только криптография Ed25519
2. Метод `did:peer` объявлен но не реализован
3. Service endpoints в DID документах не используются (закомментированы)
4. Нет встроенной поддержки удаленной резолюции (требуется custom resolver)

## TODO

- [ ] Поддержка других криптографических алгоритмов (secp256k1, RSA)
- [ ] Реализация did:peer метода
- [ ] Интеграция с universal resolver
- [ ] Поддержка DID document updates
- [ ] Механизм revocation для ключей
- [ ] Key rotation history
