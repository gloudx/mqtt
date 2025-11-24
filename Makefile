.PHONY: build clean run-server run-client run-test-server run-pds demo deps test fmt vet check help

# Сборка всех компонентов
build: build-server build-client build-pds build-eventlog-tool

# Сборка PDS сервера
build-pds:
	@echo "Building PDS server..."
	@mkdir -p bin
	@go build -o bin/pds ./cmd/pds

# Сборка сервера
build-server:
	@echo "Building server..."
	@mkdir -p bin
	@go build -o bin/server ./cmd/server

# Сборка клиента
build-client:
	@echo "Building client..."
	@mkdir -p bin
	@go build -o bin/client ./cmd/client

# Сборка eventlog-tool
build-eventlog-tool:
	@echo "Building eventlog-tool..."
	@mkdir -p bin
	@go build -o bin/eventlog-tool ./cmd/eventlog-tool

# Очистка
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@go clean

# Запуск туннель сервера
run-server: build-server
	@echo "Starting HTTP-MQTT tunnel server..."
	@./bin/server

# Запуск PDS сервера
run-pds: build-pds
	@echo "Starting PDS server..."
	@./bin/pds --data=./data/pds --http-port=8080

# Запуск PDS с MQTT
run-pds-mqtt: build-pds
	@echo "Starting PDS server with MQTT sync..."
	@./bin/pds --data=./data/pds --http-port=8080 --mqtt-broker=tcp://localhost:1883 --mqtt-enable

# Запуск клиента туннеля
run-client: build-client
	@echo "Starting tunnel client (default: client-id=demo, target=http://localhost:3000)..."
	@./bin/client -client-id=demo -target=http://localhost:3000

# Запуск тестового HTTP сервера
run-test-server:
	@echo "Starting test HTTP server on port 3000..."
	@go run examples/test_server.go

# Запуск демонстрации
demo: build
	@echo "Running demo (make sure server, test-server and client are running)..."
	@./examples/demo.sh demo

# Полный запуск для демо (в одном терминале через tmux)
demo-all:
	@echo "Starting full demo environment..."
	@echo "1. Start server: make run-server"
	@echo "2. Start test server: make run-test-server"
	@echo "3. Start client: make run-client"
	@echo "4. Run demo: make demo"
	@echo ""
	@echo "Or run each in separate terminals"

# Установка зависимостей
deps:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy

# Тестирование
test:
	@echo "Running tests..."
	@go test -v ./...

# Форматирование кода
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Проверка кода
vet:
	@echo "Vetting code..."
	@go vet ./...

# Полная проверка
check: fmt vet test

# Справка
help:
	@echo "HTTP-MQTT Reverse Proxy Tunnel - Makefile targets:"
	@echo ""
	@echo "Building:"
	@echo "  build           - Build all components (server + client + pds)"
	@echo "  build-server    - Build tunnel server only"
	@echo "  build-client    - Build tunnel client only"
	@echo "  build-pds       - Build PDS server only"
	@echo "  clean           - Remove build artifacts"
	@echo ""
	@echo "Running:"
	@echo "  run-server      - Start tunnel server (MQTT broker + HTTP proxy)"
	@echo "  run-client      - Start tunnel client (forwards to localhost:3000)"
	@echo "  run-pds         - Start PDS server"
	@echo "  run-pds-mqtt    - Start PDS server with MQTT synchronization"
	@echo "  run-test-server - Start test HTTP server on port 3000"
	@echo "  demo            - Run demonstration script"
	@echo "  demo-all        - Show instructions for full demo"
	@echo ""
	@echo "Development:"
	@echo "  deps            - Install/update dependencies"
	@echo "  test            - Run tests"
	@echo "  fmt             - Format code"
	@echo "  vet             - Vet code"
	@echo "  check           - Run fmt, vet, and tests"
	@echo ""
	@echo "Quick Start PDS:"
	@echo "  Terminal 1: make run-pds"
	@echo "  Terminal 2: curl http://localhost:8080/health"
	@echo ""
	@echo "Quick Start Tunnel:"
	@echo "  Terminal 1: make run-server"
	@echo "  Terminal 2: make run-test-server"
	@echo "  Terminal 3: make run-client"
	@echo "  Terminal 4: make demo"
	@echo ""
	@echo "For more info see README.md and QUICKSTART.md"
