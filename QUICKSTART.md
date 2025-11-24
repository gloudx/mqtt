# Быстрый старт HTTP-MQTT Tunnel

## За 5 минут до работающего туннеля

### 1. Сборка (30 секунд)

```bash
make build
```

### 2. Запуск компонентов

Откройте 4 терминала:

#### Терминал 1: Сервер туннеля

```bash
./bin/server
```

Вы увидите:
```
2025/11/20 19:30:00 MQTT broker started on port 1883
2025/11/20 19:30:01 HTTP reverse proxy started on port 8080
2025/11/20 19:30:01 Ready to forward requests to clients via /<client-id>/*
```

#### Терминал 2: Тестовый HTTP сервер

```bash
go run examples/test_server.go
```

Вы увидите:
```
2025/11/20 19:30:02 Test HTTP server started on http://localhost:3000
```

#### Терминал 3: Клиент туннеля

```bash
./bin/client -client-id=demo -target=http://localhost:3000
```

Вы увидите:
```
2025/11/20 19:30:05 Your local server is now accessible via:
2025/11/20 19:30:05   http://tunnel-server:8080/demo/
2025/11/20 19:30:05 Tunnel established! Waiting for requests...
```

#### Терминал 4: Тестирование

```bash
# Простой GET запрос
curl http://localhost:8080/demo/api/hello

# POST запрос
curl -X POST http://localhost:8080/demo/api/echo \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'

# Или запустите автоматический тест
./examples/demo.sh demo
```

## Как это работает

```
curl http://localhost:8080/demo/api/hello
  │
  ├─> Tunnel Server (localhost:8080)
  │     │
  │     ├─> MQTT Broker (localhost:1883)
  │     │     │
  │     │     ├─> Tunnel Client
  │     │     │     │
  │     │     │     ├─> Local HTTP Server (localhost:3000)
  │     │     │     │     │
  │     │     │     │     └─> Response: {"message": "Hello!"}
  │     │     │     │
  │     │     │     └─< Response through MQTT
  │     │     │
  │     │     └─< Response through MQTT
  │     │
  │     └─< HTTP Response
  │
  └─< {"message": "Hello!"}
```

## Примеры использования

### Туннель для вашего локального приложения

```bash
# Если ваше приложение на порту 3000
./bin/client -client-id=myapp -target=http://localhost:3000

# Доступ через туннель:
curl http://localhost:8080/myapp/
```

### Несколько туннелей одновременно

```bash
# Терминал 1: Frontend (порт 3000)
./bin/client -client-id=frontend -target=http://localhost:3000

# Терминал 2: Backend API (порт 8000)
./bin/client -client-id=backend -target=http://localhost:8000

# Доступ:
curl http://localhost:8080/frontend/
curl http://localhost:8080/backend/api/users
```

### Webhook разработка

```bash
# Запустите туннель
./bin/client -client-id=webhook -target=http://localhost:4000

# Настройте webhook в вашем сервисе на:
http://your-server:8080/webhook/incoming
```

## Что дальше?

- Прочитайте полную документацию в [README.md](README.md)
- Изучите примеры в папке `examples/`
- Попробуйте подключиться к удаленному MQTT брокеру

## Частые проблемы

**Ошибка: "Failed to connect to MQTT broker"**
- Убедитесь, что сервер туннеля запущен
- Проверьте, что порт 1883 не занят другим процессом

**Ошибка: "timeout waiting for response from client"**
- Убедитесь, что клиент туннеля запущен с правильным client-id
- Проверьте, что локальный HTTP сервер отвечает

**Запросы не доходят до локального сервера**
- Проверьте URL в параметре `-target`
- Убедитесь, что путь в запросе правильный: `/<client-id>/path`

## Архитектура в двух словах

1. **Сервер** = MQTT брокер + HTTP реверс-прокси
2. **Клиент** = MQTT подписчик + HTTP proxy
3. **Связь** = Request через MQTT → Response через MQTT

Все HTTP запросы сериализуются в JSON, отправляются через MQTT, десериализуются на клиенте, проксируются на локальный сервер, и ответ идет обратно тем же путем!
