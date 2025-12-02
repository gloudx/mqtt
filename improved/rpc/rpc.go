package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-http-tunnel/improved/hlc"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

var (
	ErrTimeout        = errors.New("request timeout")
	ErrShuttingDown   = errors.New("rpc shutting down")
	ErrMethodNotFound = errors.New("method not found")
)

// Message RPC сообщение
type Message struct {
	ID      string  `json:"id"`  // Request ID
	TID     hlc.TID `json:"tid"` // HLC Timestamp
	FromDID string  `json:"from_did,omitempty"`
	Method  string  `json:"method,omitempty"` // Только для request
	Params  []byte  `json:"params,omitempty"`
	Result  []byte  `json:"result,omitempty"`
	Error   string  `json:"error,omitempty"`
}

// HandlerFunc обработчик RPC метода
type HandlerFunc func(ctx context.Context, params []byte) ([]byte, error)

// Middleware для обработки запросов
type Middleware func(HandlerFunc) HandlerFunc

// Config конфигурация RPC
type Config struct {
	OwnerDID       string
	Prefix         string
	QoS            byte
	RequestTimeout time.Duration
	Logger         zerolog.Logger
}

// RPC клиент/сервер
type RPC struct {
	mqtt           mqtt.Client
	clock          *hlc.Clock
	ownerDID       string
	prefix         string
	qos            byte
	requestTimeout time.Duration
	logger         zerolog.Logger
	//
	handlers      map[string]HandlerFunc
	middlewares   []Middleware
	subscriptions map[string]struct{}
	pendingCalls  map[string]chan *Message // Ожидающие ответа
	//
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// New создаёт новый RPC
func New(mqttClient mqtt.Client, cfg *Config) (*RPC, error) {
	if cfg == nil {
		cfg = &Config{}
	}

	if cfg.OwnerDID == "" {
		cfg.OwnerDID = hlc.New("").TID().String()[:13]
	}

	if cfg.Prefix == "" {
		cfg.Prefix = "rpc"
	}

	if cfg.QoS > 2 {
		return nil, fmt.Errorf("invalid QoS: %d", cfg.QoS)
	}

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}

	if !mqttClient.IsConnected() {
		return nil, errors.New("mqtt client not connected")
	}

	ctx, cancel := context.WithCancel(context.Background())

	rpc := &RPC{
		mqtt:           mqttClient,
		clock:          hlc.New(cfg.OwnerDID),
		ownerDID:       cfg.OwnerDID,
		prefix:         cfg.Prefix,
		qos:            cfg.QoS,
		requestTimeout: cfg.RequestTimeout,
		logger:         cfg.Logger,
		handlers:       make(map[string]HandlerFunc),
		subscriptions:  make(map[string]struct{}),
		pendingCalls:   make(map[string]chan *Message),
		ctx:            ctx,
		cancel:         cancel,
	}

	rpc.logger.Info().
		Str("owner_did", rpc.ownerDID).
		Str("prefix", rpc.prefix).
		Msg("RPC initialized")

	return rpc, nil
}

// Use добавляет middleware
func (r *RPC) Use(mw Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middlewares = append(r.middlewares, mw)
}

// Register регистрирует обработчик метода
func (r *RPC) Register(method string, handler HandlerFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[method]; exists {
		return fmt.Errorf("method %s already registered", method)
	}

	// Применяем middlewares
	wrappedHandler := handler
	for i := len(r.middlewares) - 1; i >= 0; i-- {
		wrappedHandler = r.middlewares[i](wrappedHandler)
	}
	r.handlers[method] = wrappedHandler

	// Подписываемся на запросы
	topic := fmt.Sprintf("%s/request/%s/%s", r.prefix, r.ownerDID, method)
	token := r.mqtt.Subscribe(topic, r.qos, r.handleRequest)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	r.subscriptions[topic] = struct{}{}
	r.logger.Info().Str("method", method).Str("topic", topic).Msg("registered")

	return nil
}

// RegisterTyped регистрирует типизированный обработчик
func RegisterTyped[Req, Resp any](r *RPC, method string, fn func(context.Context, Req) (Resp, error)) error {
	return r.Register(method, func(ctx context.Context, params []byte) ([]byte, error) {
		var req Req
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, fmt.Errorf("unmarshal request: %w", err)
		}

		resp, err := fn(ctx, req)
		if err != nil {
			return nil, err
		}

		return json.Marshal(resp)
	})
}

// Unregister отменяет регистрацию
func (r *RPC) Unregister(method string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[method]; !exists {
		return fmt.Errorf("method %s not registered", method)
	}

	delete(r.handlers, method)

	topic := fmt.Sprintf("%s/request/%s/%s", r.prefix, r.ownerDID, method)
	if token := r.mqtt.Unsubscribe(topic); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	delete(r.subscriptions, topic)
	r.logger.Info().Str("method", method).Msg("unregistered")

	return nil
}

// Call вызывает удалённый метод
func (r *RPC) Call(ctx context.Context, targetDID, method string, params, result any) error {
	paramsData, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal params: %w", err)
	}

	tid := r.clock.TID()
	requestID := tid.String()

	msg := &Message{
		ID:      requestID,
		TID:     tid,
		FromDID: r.ownerDID,
		Method:  method,
		Params:  paramsData,
	}

	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Создаём канал для ответа
	responseCh := make(chan *Message, 1)

	r.mu.Lock()
	r.pendingCalls[requestID] = responseCh
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.pendingCalls, requestID)
		r.mu.Unlock()
	}()

	// Подписываемся на ответ
	responseTopic := fmt.Sprintf("%s/response/%s/%s/%s", r.prefix, r.ownerDID, method, requestID)

	responseHandler := func(_ mqtt.Client, msg mqtt.Message) {
		var m Message
		if err := json.Unmarshal(msg.Payload(), &m); err != nil {
			r.logger.Error().Err(err).Msg("unmarshal response failed")
			return
		}

		// Обновляем HLC
		if !m.TID.IsZero() {
			r.clock.UpdateTID(m.TID)
		}

		select {
		case responseCh <- &m:
		default:
		}
	}

	if token := r.mqtt.Subscribe(responseTopic, r.qos, responseHandler); token.Wait() && token.Error() != nil {
		return fmt.Errorf("subscribe response: %w", token.Error())
	}
	defer r.mqtt.Unsubscribe(responseTopic)

	// Отправляем запрос
	requestTopic := fmt.Sprintf("%s/request/%s/%s", r.prefix, targetDID, method)

	r.logger.Debug().
		Str("target", targetDID).
		Str("method", method).
		Str("request_id", requestID).
		Msg("calling")

	if token := r.mqtt.Publish(requestTopic, r.qos, false, msgData); token.Wait() && token.Error() != nil {
		return fmt.Errorf("publish request: %w", token.Error())
	}

	// Ждём ответа
	select {
	case m := <-responseCh:
		if m.Error != "" {
			return errors.New(m.Error)
		}
		if result != nil && len(m.Result) > 0 {
			if err := json.Unmarshal(m.Result, result); err != nil {
				return fmt.Errorf("unmarshal result: %w", err)
			}
		}
		r.logger.Debug().
			Str("target", targetDID).
			Str("method", method).
			Msg("call completed")
		return nil

	case <-ctx.Done():
		return ctx.Err()

	case <-r.ctx.Done():
		return ErrShuttingDown
	}
}

// CallTyped вызывает типизированный метод
func CallTyped[Req, Resp any](r *RPC, ctx context.Context, targetDID, method string, req Req) (Resp, error) {
	var resp Resp
	err := r.Call(ctx, targetDID, method, req, &resp)
	return resp, err
}

// handleRequest обрабатывает входящий запрос
func (r *RPC) handleRequest(_ mqtt.Client, msg mqtt.Message) {
	var message Message
	if err := json.Unmarshal(msg.Payload(), &message); err != nil {
		r.logger.Error().Err(err).Msg("unmarshal request failed")
		return
	}

	// Обновляем HLC
	if !message.TID.IsZero() {
		r.clock.UpdateTID(message.TID)
	}

	r.mu.RLock()
	handler, exists := r.handlers[message.Method]
	r.mu.RUnlock()

	response := &Message{
		ID:  message.ID,
		TID: r.clock.TID(),
	}

	if !exists {
		response.Error = fmt.Sprintf("method %s not found", message.Method)
		r.logger.Warn().
			Str("method", message.Method).
			Str("from", message.FromDID).
			Msg("method not found")
	} else {
		r.logger.Debug().
			Str("method", message.Method).
			Str("from", message.FromDID).
			Str("request_id", message.ID).
			Msg("handling request")

		ctx, cancel := context.WithTimeout(r.ctx, r.requestTimeout)
		result, err := handler(ctx, message.Params)
		cancel()

		if err != nil {
			response.Error = err.Error()
			r.logger.Error().Err(err).Str("method", message.Method).Msg("handler error")
		} else {
			response.Result = result
		}
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		r.logger.Error().Err(err).Msg("marshal response failed")
		return
	}

	responseTopic := fmt.Sprintf("%s/response/%s/%s/%s", r.prefix, message.FromDID, message.Method, message.ID)
	if token := r.mqtt.Publish(responseTopic, r.qos, false, responseData); token.Wait() && token.Error() != nil {
		r.logger.Error().Err(token.Error()).Str("topic", responseTopic).Msg("publish response failed")
	}
}

// Clock возвращает HLC
func (r *RPC) Clock() *hlc.Clock {
	return r.clock
}

// OwnerDID возвращает DID
func (r *RPC) OwnerDID() string {
	return r.ownerDID
}

// Close завершает работу
func (r *RPC) Close() error {
	r.logger.Info().Msg("shutting down")

	r.cancel()

	r.mu.Lock()
	for topic := range r.subscriptions {
		r.mqtt.Unsubscribe(topic)
	}
	r.subscriptions = make(map[string]struct{})
	r.handlers = make(map[string]HandlerFunc)
	r.mu.Unlock()

	r.logger.Info().Msg("shutdown complete")
	return nil
}

// --- Middleware примеры ---

// LoggingMiddleware логирует вызовы
func LoggingMiddleware(logger zerolog.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params []byte) ([]byte, error) {
			start := time.Now()
			result, err := next(ctx, params)
			logger.Debug().
				Dur("duration", time.Since(start)).
				Bool("error", err != nil).
				Msg("rpc call")
			return result, err
		}
	}
}

// RecoveryMiddleware перехватывает паники
func RecoveryMiddleware(logger zerolog.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params []byte) (result []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error().Interface("panic", r).Msg("handler panic")
					err = fmt.Errorf("internal error")
				}
			}()
			return next(ctx, params)
		}
	}
}

// TimeoutMiddleware добавляет таймаут
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, params []byte) ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			done := make(chan struct {
				result []byte
				err    error
			}, 1)

			go func() {
				result, err := next(ctx, params)
				done <- struct {
					result []byte
					err    error
				}{result, err}
			}()

			select {
			case r := <-done:
				return r.result, r.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}
