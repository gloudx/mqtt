package main

import (
	"flag"
	"fmt"
	"log"
	"mqtt-http-tunnel/internal/tunnel"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/auth"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
)

var (
	mqttPort = flag.String("mqtt-port", "1883", "MQTT broker port")
	httpPort = flag.String("http-port", "8081", "HTTP tunnel port")
)

type Server struct {
	mqttServer *mqtt.Server
	proxy      *tunnel.HTTPReverseProxy
}

func main() {
	flag.Parse()

	// Создаем MQTT сервер (comqtt)
	mqttServer := mqtt.New(&mqtt.Options{
		InlineClient: false,
	})

	// Добавляем хук аутентификации (разрешаем всех для простоты)
	err := mqttServer.AddHook(new(auth.AllowHook), nil)
	if err != nil {
		log.Fatalf("Failed to add auth hook: %v", err)
	}

	// Создаем TCP listener для MQTT
	tcp := listeners.NewTCP("tcp", ":"+*mqttPort, nil)
	err = mqttServer.AddListener(tcp)
	if err != nil {
		log.Fatalf("Failed to add TCP listener: %v", err)
	}

	// Запускаем MQTT сервер
	go func() {
		err := mqttServer.Serve()
		if err != nil {
			log.Fatalf("Failed to start MQTT server: %v", err)
		}
	}()

	log.Printf("MQTT broker started on port %s", *mqttPort)

	// Ждем, пока MQTT сервер запустится
	time.Sleep(1 * time.Second)

	// Создаем HTTP реверс-прокси
	proxy, err := tunnel.NewHTTPReverseProxy("tcp://localhost:"+*mqttPort, "http-reverse-proxy")
	if err != nil {
		log.Fatalf("Failed to create HTTP reverse proxy: %v", err)
	}

	server := &Server{
		mqttServer: mqttServer,
		proxy:      proxy,
	}

	// Настраиваем HTTP сервер с catch-all обработчиком
	http.HandleFunc("/health", server.handleHealth)
	http.HandleFunc("/", server.handleProxy)

	// Запускаем HTTP сервер
	go func() {
		log.Printf("HTTP reverse proxy started on port %s", *httpPort)
		log.Printf("Ready to forward requests to clients via /<client-id>/*")
		if err := http.ListenAndServe(":"+*httpPort, nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Ожидаем сигнал завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	proxy.Close()
	mqttServer.Close()
	log.Println("Server stopped")
}

// handleProxy обрабатывает все HTTP запросы и форвардит их клиентам
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	// Пропускаем health check
	if r.URL.Path == "/health" {
		return
	}

	// Парсим путь: /<client-id>/остальной/путь
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "Invalid path format. Use: /<client-id>/path", http.StatusBadRequest)
		return
	}

	clientID := parts[0]

	// Модифицируем путь для клиента (убираем префикс с client-id)
	if len(parts) > 1 {
		r.URL.Path = "/" + parts[1]
	} else {
		r.URL.Path = "/"
	}

	log.Printf("Forwarding %s %s to client %s", r.Method, r.URL.Path, clientID)

	// Форвардим запрос клиенту через MQTT
	resp, err := s.proxy.ForwardRequest(clientID, r)
	if err != nil {
		log.Printf("Failed to forward request to client %s: %v", clientID, err)
		http.Error(w, fmt.Sprintf("Failed to forward request: %v", err), http.StatusBadGateway)
		return
	}

	// Копируем заголовки из ответа
	for key, values := range resp.Headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Отправляем ответ
	w.WriteHeader(resp.StatusCode)
	w.Write(resp.Body)

	log.Printf("Forwarded response from client %s: status %d, body size %d bytes", clientID, resp.StatusCode, len(resp.Body))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "healthy"}`))
}
