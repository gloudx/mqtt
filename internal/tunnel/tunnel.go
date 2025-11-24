package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/xid"
)

// HTTPRequest представляет HTTP запрос для туннелирования
type HTTPRequest struct {
	ID      string              `json:"id"`       // Уникальный ID запроса для correlation
	Method  string              `json:"method"`   // HTTP метод
	Path    string              `json:"path"`     // Путь (без префикса клиента)
	Headers map[string][]string `json:"headers"`  // HTTP заголовки
	Body    []byte              `json:"body"`     // Тело запроса
	Query   string              `json:"query"`    // Query string
}

// HTTPResponse представляет HTTP ответ
type HTTPResponse struct {
	ID         string              `json:"id"`          // ID запроса для correlation
	StatusCode int                 `json:"status_code"` // HTTP статус код
	Headers    map[string][]string `json:"headers"`     // HTTP заголовки
	Body       []byte              `json:"body"`        // Тело ответа
}

// HTTPReverseProxy реализует реверс-прокси через MQTT
type HTTPReverseProxy struct {
	mqttClient     mqtt.Client
	mu             sync.RWMutex
	pendingRequests map[string]chan *HTTPResponse // requestID -> response channel
	clientID       string
}

// NewHTTPReverseProxy создает новый HTTP реверс-прокси
func NewHTTPReverseProxy(brokerURL string, clientID string) (*HTTPReverseProxy, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID + "-proxy")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	proxy := &HTTPReverseProxy{
		pendingRequests: make(map[string]chan *HTTPResponse),
		clientID:        clientID,
	}

	// Настраиваем обработчик ответов
	opts.SetDefaultPublishHandler(proxy.handleResponse)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	proxy.mqttClient = client

	// Подписываемся на ответы от всех клиентов
	topic := "tunnel/+/response"
	if token := client.Subscribe(topic, 1, proxy.handleResponse); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to subscribe to responses: %w", token.Error())
	}

	log.Printf("HTTP reverse proxy connected to MQTT broker: %s", brokerURL)

	return proxy, nil
}

// ForwardRequest отправляет HTTP запрос клиенту через MQTT и ждет ответа
func (p *HTTPReverseProxy) ForwardRequest(clientID string, req *http.Request) (*HTTPResponse, error) {
	// Создаем структуру запроса
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	defer req.Body.Close()

	requestID := xid.New().String()
	httpReq := &HTTPRequest{
		ID:      requestID,
		Method:  req.Method,
		Path:    req.URL.Path,
		Headers: req.Header,
		Body:    body,
		Query:   req.URL.RawQuery,
	}

	// Создаем канал для ответа
	respChan := make(chan *HTTPResponse, 1)
	p.mu.Lock()
	p.pendingRequests[requestID] = respChan
	p.mu.Unlock()

	// Очищаем после завершения
	defer func() {
		p.mu.Lock()
		delete(p.pendingRequests, requestID)
		p.mu.Unlock()
		close(respChan)
	}()

	// Сериализуем запрос
	data, err := json.Marshal(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Отправляем запрос клиенту
	topic := fmt.Sprintf("tunnel/%s/request", clientID)
	token := p.mqttClient.Publish(topic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to publish request: %w", token.Error())
	}

	log.Printf("Forwarded request %s to client %s: %s %s", requestID, clientID, req.Method, req.URL.Path)

	// Ждем ответа с таймаутом
	select {
	case resp := <-respChan:
		if resp == nil {
			return nil, fmt.Errorf("received nil response")
		}
		return resp, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for response from client")
	}
}

// handleResponse обрабатывает ответы от клиентов
func (p *HTTPReverseProxy) handleResponse(client mqtt.Client, msg mqtt.Message) {
	var resp HTTPResponse
	if err := json.Unmarshal(msg.Payload(), &resp); err != nil {
		log.Printf("Failed to unmarshal response: %v", err)
		return
	}

	log.Printf("Received response for request %s: status %d", resp.ID, resp.StatusCode)

	p.mu.RLock()
	respChan, exists := p.pendingRequests[resp.ID]
	p.mu.RUnlock()

	if exists && respChan != nil {
		select {
		case respChan <- &resp:
		default:
			log.Printf("Response channel full or closed for request %s", resp.ID)
		}
	} else {
		log.Printf("No pending request found for ID %s", resp.ID)
	}
}

// Close закрывает прокси
func (p *HTTPReverseProxy) Close() {
	if p.mqttClient != nil && p.mqttClient.IsConnected() {
		p.mqttClient.Disconnect(250)
	}
}

// TunnelClient представляет клиента туннеля
type TunnelClient struct {
	mqttClient   mqtt.Client
	clientID     string
	targetURL    string
	httpClient   *http.Client
	requestTopic string
}

// NewTunnelClient создает нового клиента туннеля
func NewTunnelClient(brokerURL, clientID, targetURL string) (*TunnelClient, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID + "-client")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	tc := &TunnelClient{
		clientID:     clientID,
		targetURL:    targetURL,
		requestTopic: fmt.Sprintf("tunnel/%s/request", clientID),
		httpClient: &http.Client{
			Timeout: 25 * time.Second,
		},
	}

	// Настраиваем обработчик запросов
	opts.SetDefaultPublishHandler(tc.handleRequest)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	tc.mqttClient = client

	// Подписываемся на запросы для этого клиента
	if token := client.Subscribe(tc.requestTopic, 1, tc.handleRequest); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to subscribe to requests: %w", token.Error())
	}

	log.Printf("Tunnel client %s connected, forwarding to %s", clientID, targetURL)

	return tc, nil
}

// handleRequest обрабатывает входящие HTTP запросы через MQTT
func (tc *TunnelClient) handleRequest(client mqtt.Client, msg mqtt.Message) {
	var httpReq HTTPRequest
	if err := json.Unmarshal(msg.Payload(), &httpReq); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return
	}

	log.Printf("Received request %s: %s %s", httpReq.ID, httpReq.Method, httpReq.Path)

	// Создаем HTTP запрос к целевому серверу
	targetURL := tc.targetURL + httpReq.Path
	if httpReq.Query != "" {
		targetURL += "?" + httpReq.Query
	}

	req, err := http.NewRequestWithContext(context.Background(), httpReq.Method, targetURL, bytes.NewReader(httpReq.Body))
	if err != nil {
		tc.sendErrorResponse(httpReq.ID, http.StatusInternalServerError, fmt.Sprintf("Failed to create request: %v", err))
		return
	}

	// Копируем заголовки
	for key, values := range httpReq.Headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	// Выполняем запрос
	resp, err := tc.httpClient.Do(req)
	if err != nil {
		tc.sendErrorResponse(httpReq.ID, http.StatusBadGateway, fmt.Sprintf("Failed to forward request: %v", err))
		return
	}
	defer resp.Body.Close()

	// Читаем ответ
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		tc.sendErrorResponse(httpReq.ID, http.StatusInternalServerError, fmt.Sprintf("Failed to read response: %v", err))
		return
	}

	// Отправляем ответ обратно
	httpResp := &HTTPResponse{
		ID:         httpReq.ID,
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       body,
	}

	tc.sendResponse(httpResp)
}

// sendResponse отправляет HTTP ответ через MQTT
func (tc *TunnelClient) sendResponse(resp *HTTPResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Failed to marshal response: %v", err)
		return
	}

	topic := fmt.Sprintf("tunnel/%s/response", tc.clientID)
	token := tc.mqttClient.Publish(topic, 1, false, data)
	if token.Wait() && token.Error() != nil {
		log.Printf("Failed to publish response: %v", token.Error())
		return
	}

	log.Printf("Sent response for request %s: status %d", resp.ID, resp.StatusCode)
}

// sendErrorResponse отправляет ошибку как HTTP ответ
func (tc *TunnelClient) sendErrorResponse(requestID string, statusCode int, message string) {
	resp := &HTTPResponse{
		ID:         requestID,
		StatusCode: statusCode,
		Headers:    make(map[string][]string),
		Body:       []byte(message),
	}
	resp.Headers["Content-Type"] = []string{"text/plain"}
	tc.sendResponse(resp)
}

// Close закрывает клиента
func (tc *TunnelClient) Close() {
	if tc.mqttClient != nil && tc.mqttClient.IsConnected() {
		tc.mqttClient.Unsubscribe(tc.requestTopic)
		tc.mqttClient.Disconnect(250)
	}
}

// Wait ждет прерывания
func (tc *TunnelClient) Wait() {
	select {}
}
