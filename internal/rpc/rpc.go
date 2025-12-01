package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Message struct {
	ID      string `json:"id"`
	FromDID string `json:"from_did,omitempty"`
	Method  string `json:"method,omitempty"`
	Params  []byte `json:"params,omitempty"`
	Result  []byte `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

type HandlerFunc func(ctx context.Context, params []byte) ([]byte, error)

type Config struct {
	OwnerDID       string
	Prefix         string
	QoS            byte
	RequestTimeout time.Duration
	Logger         zerolog.Logger
}

type Rpc struct {
	mqtt           mqtt.Client
	ownerDID       string         // cfg.OwnerDID
	prefix         string         // cfg.Prefix
	qos            byte           // cfg.QoS
	requestTimeout time.Duration  // cfg.RequestTimeout
	logger         zerolog.Logger // cfg.Logger
	//
	handlers      map[string]HandlerFunc
	subscriptions map[string]struct{} // отслеживание активных подписок
	//
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// New ...
func New(mqttClient mqtt.Client, cfg *Config) (*Rpc, error) {

	if cfg == nil {
		cfg = &Config{}
	}

	if cfg.OwnerDID == "" {
		cfg.OwnerDID = uuid.New().String()
	}

	if cfg.Prefix == "" {
		cfg.Prefix = "rpc"
	}

	if cfg.QoS > 2 {
		return nil, fmt.Errorf("invalid QoS: %d, must be 0, 1, or 2", cfg.QoS)
	}

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}

	if !mqttClient.IsConnected() {
		return nil, fmt.Errorf("mqtt client is not connected")
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Rpc{
		mqtt:           mqttClient,
		ownerDID:       cfg.OwnerDID,
		prefix:         cfg.Prefix,
		qos:            cfg.QoS,
		requestTimeout: cfg.RequestTimeout,
		logger:         cfg.Logger,
		//
		handlers:      make(map[string]HandlerFunc),
		subscriptions: make(map[string]struct{}),
		//
		ctx:    ctx,
		cancel: cancel,
	}

	c.logger.Info().Str("owner_did", c.ownerDID).Str("prefix", c.prefix).Msg("RPC initialized")

	return c, nil
}

// Register регистрирует обработчик метода
func (c *Rpc) Register(method string, handler HandlerFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.handlers[method]; exists {
		return fmt.Errorf("handler for method %s already registered", method)
	}

	c.handlers[method] = handler

	requestTopic := fmt.Sprintf("%s/request/%s/%s", c.prefix, c.ownerDID, method)
	if token := c.mqtt.Subscribe(requestTopic, c.qos, c.handleMessage); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	c.subscriptions[requestTopic] = struct{}{}
	c.logger.Info().Str("method", method).Str("topic", requestTopic).Msg("Registered method")
	return nil
}

// Unregister отменяет регистрацию обработчика метода
func (c *Rpc) Unregister(method string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.handlers[method]; !exists {
		return fmt.Errorf("handler for method %s not registered", method)
	}

	delete(c.handlers, method)

	requestTopic := fmt.Sprintf("%s/request/%s/%s", c.prefix, c.ownerDID, method)
	if token := c.mqtt.Unsubscribe(requestTopic); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	delete(c.subscriptions, requestTopic)
	c.logger.Info().Str("method", method).Str("topic", requestTopic).Msg("Unregistered method")
	return nil
}

// Call вызывает метод на удаленном узле
func (c *Rpc) Call(ctx context.Context, targetDID, method string, params, result any) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	paramsData, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal params: %w", err)
	}

	requestID := uuid.New().String()
	msg := &Message{
		ID:      requestID,
		FromDID: c.ownerDID,
		Method:  method,
		Params:  paramsData,
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	responseTopic := fmt.Sprintf("%s/response/%s/%s/%s", c.prefix, c.ownerDID, method, requestID)

	responseCh := make(chan Message, 1)

	// Подписываемся ДО отправки запроса для предотвращения race condition
	handler := func(client mqtt.Client, msg mqtt.Message) {
		var m Message
		if err := json.Unmarshal(msg.Payload(), &m); err == nil {
			select {
			case responseCh <- m:
			default:
				c.logger.Warn().Str("request_id", requestID).Msg("Response channel full")
			}
		} else {
			c.logger.Error().Err(err).Msg("Error unmarshaling response")
		}
	}

	// token.Wait() блокирует до завершения подписки - race condition предотвращен
	if token := c.mqtt.Subscribe(responseTopic, c.qos, handler); token.Wait() && token.Error() != nil {
		return fmt.Errorf("subscribe to response topic: %w", token.Error())
	}

	// Гарантированная отписка при выходе
	defer func() {
		if token := c.mqtt.Unsubscribe(responseTopic); token.Wait() && token.Error() != nil {
			c.logger.Error().Err(token.Error()).Str("topic", responseTopic).Msg("Error unsubscribing")
		}
	}()

	requestTopic := fmt.Sprintf("%s/request/%s/%s", c.prefix, targetDID, method)
	c.logger.Debug().Str("target_did", targetDID).Str("method", method).Str("request_id", requestID).Msg("Calling RPC method")

	if token := c.mqtt.Publish(requestTopic, c.qos, false, msgData); token.Wait() && token.Error() != nil {
		return fmt.Errorf("publish request: %w", token.Error())
	}

	select {
	case m := <-responseCh:
		if m.Error != "" {
			c.logger.Error().Str("target_did", targetDID).Str("method", method).Str("error", m.Error).Msg("Remote error")
			return errors.New(m.Error)
		}
		if result != nil && len(m.Result) > 0 {
			if err := json.Unmarshal(m.Result, result); err != nil {
				return fmt.Errorf("unmarshal result: %w", err)
			}
		}
		c.logger.Debug().Str("target_did", targetDID).Str("method", method).Msg("RPC call completed")
		return nil

	case <-ctx.Done():
		c.logger.Warn().Str("target_did", targetDID).Str("method", method).Err(ctx.Err()).Msg("Context cancelled")
		return ctx.Err()

	case <-c.ctx.Done():
		return fmt.Errorf("rpc client shutting down")
	}
}

func (c *Rpc) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	var message Message
	if err := json.Unmarshal(msg.Payload(), &message); err != nil {
		c.logger.Error().Err(err).Msg("Error unmarshaling message")
		return
	}

	if message.Method != "" {
		c.handleRequest(&message)
	} else {
		c.logger.Warn().Str("topic", msg.Topic()).Msg("Received message without method")
	}
}

func (c *Rpc) handleRequest(msg *Message) {
	c.mu.RLock()
	handler, exists := c.handlers[msg.Method]
	c.mu.RUnlock()

	response := &Message{ID: msg.ID}

	if !exists {
		response.Error = fmt.Sprintf("method %s not found", msg.Method)
		c.logger.Warn().Str("method", msg.Method).Str("from_did", msg.FromDID).Msg("Method not found")
	} else {
		c.logger.Debug().Str("method", msg.Method).Str("from_did", msg.FromDID).Str("request_id", msg.ID).Msg("Handling request")
		ctx, cancel := context.WithTimeout(c.ctx, c.requestTimeout)
		defer cancel()

		result, err := handler(ctx, msg.Params)
		if err != nil {
			response.Error = err.Error()
			c.logger.Error().Err(err).Str("method", msg.Method).Msg("Handler error")
		} else {
			response.Result = result
			c.logger.Debug().Str("method", msg.Method).Str("from_did", msg.FromDID).Msg("Request handled successfully")
		}
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		c.logger.Error().Err(err).Msg("Error marshaling response")
		return
	}

	responseTopic := fmt.Sprintf("%s/response/%s/%s/%s", c.prefix, msg.FromDID, msg.Method, msg.ID)
	if token := c.mqtt.Publish(responseTopic, c.qos, false, responseData); token.Wait() && token.Error() != nil {
		c.logger.Error().Err(token.Error()).Str("topic", responseTopic).Msg("Error publishing response")
	}
}

// Close ...
func (c *Rpc) Close() error {
	c.logger.Info().Msg("RPC shutting down")

	// Отменяем контекст для прерывания активных операций
	c.cancel()

	// Отписываемся от всех топиков
	c.mu.Lock()
	for topic := range c.subscriptions {
		if token := c.mqtt.Unsubscribe(topic); token.Wait() && token.Error() != nil {
			c.logger.Error().Err(token.Error()).Str("topic", topic).Msg("Error unsubscribing")
		}
	}
	c.subscriptions = make(map[string]struct{})
	c.handlers = make(map[string]HandlerFunc)
	c.mu.Unlock()

	c.logger.Info().Msg("RPC shutdown complete")
	return nil
}
