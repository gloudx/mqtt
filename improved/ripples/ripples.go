package ripples

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/improved/hlc"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

// Envelope - общая обёртка для всех сообщений
// Использует HLC вместо Unix timestamp для причинного упорядочивания
type Envelope struct {
	ID      string  `json:"id"`      // Уникальный ID сообщения
	Type    string  `json:"type"`    // Тип сообщения
	FromDID string  `json:"fromDID"` // DID отправителя
	TID     hlc.TID `json:"tid"`     // HLC Timestamp ID
	Payload []byte  `json:"payload"` // Полезная нагрузка
}

// Handler обрабатывает входящие сообщения
type Handler func(ctx context.Context, env *Envelope) error

// Publisher генерирует исходящие сообщения
type Publisher func(ctx context.Context) (*Envelope, error)

// MessageFilter фильтрует входящие сообщения
type MessageFilter func(env *Envelope) bool

// Config конфигурация для типа сообщений
type Config struct {
	Type      string        // Тип сообщения
	Handlers  []Handler     // Обработчики
	Publisher Publisher     // Генератор (опционально)
	Interval  time.Duration // Интервал публикации
	QoS       byte          // MQTT QoS
	Filter    MessageFilter // Фильтр сообщений (опционально)
	Priority  Priority      // Приоритет обработки
}

// RipplesConfig конфигурация Ripples
type RipplesConfig struct {
	OwnerDID    string
	Prefix      string
	Logger      zerolog.Logger
	QueueConfig *FairQueueConfig // Конфигурация очереди
	Workers     int              // Количество воркеров обработки
}

// Ripples управляет обменом сообщениями через MQTT
// Улучшения относительно оригинала:
// - HLC вместо Unix timestamp
// - Fair Queue для справедливой обработки
// - Приоритеты сообщений
// - Фильтрация сообщений
type Ripples struct {
	mqtt     mqtt.Client
	clock    *hlc.Clock
	ownerDID string
	prefix   string
	queue    *PriorityFairQueue
	configs  map[string]*Config
	//
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex
	logger zerolog.Logger
}

// New создаёт новый Ripples
func New(mqttClient mqtt.Client, cfg *RipplesConfig) *Ripples {
	if cfg == nil {
		cfg = &RipplesConfig{}
	}

	if cfg.OwnerDID == "" {
		cfg.OwnerDID = hlc.New("").TID().String()[:13]
	}

	if cfg.Prefix == "" {
		cfg.Prefix = "ripples"
	}

	if cfg.Workers == 0 {
		cfg.Workers = 4
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Ripples{
		mqtt:     mqttClient,
		clock:    hlc.New(cfg.OwnerDID),
		ownerDID: cfg.OwnerDID,
		prefix:   cfg.Prefix,
		queue:    NewPriorityFairQueue(cfg.QueueConfig),
		configs:  make(map[string]*Config),
		ctx:      ctx,
		cancel:   cancel,
		logger:   cfg.Logger,
	}

	// Запускаем воркеры обработки
	for i := 0; i < cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}

	r.logger.Info().
		Str("owner_did", r.ownerDID).
		Str("prefix", r.prefix).
		Int("workers", cfg.Workers).
		Msg("Ripples initialized")

	return r
}

// Register регистрирует тип сообщений
func (r *Ripples) Register(cfg *Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.configs[cfg.Type]; exists {
		return fmt.Errorf("ripple type %s already registered", cfg.Type)
	}

	// Подписываемся на топик
	if len(cfg.Handlers) > 0 {
		topic := fmt.Sprintf("%s/%s", r.prefix, cfg.Type)
		token := r.mqtt.Subscribe(topic, cfg.QoS, r.makeHandler(cfg))
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("subscribe failed: %w", token.Error())
		}
		r.logger.Info().
			Str("type", cfg.Type).
			Str("topic", topic).
			Int("handlers", len(cfg.Handlers)).
			Msg("subscribed")
	}

	r.configs[cfg.Type] = cfg

	// Запускаем publisher если задан
	if cfg.Publisher != nil && cfg.Interval > 0 {
		r.wg.Add(1)
		go r.publishLoop(cfg)
	}

	return nil
}

// makeHandler создаёт MQTT handler
func (r *Ripples) makeHandler(cfg *Config) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var env Envelope
		if err := json.Unmarshal(msg.Payload(), &env); err != nil {
			r.logger.Error().Err(err).Str("topic", msg.Topic()).Msg("unmarshal failed")
			return
		}

		// Игнорируем свои сообщения
		if env.FromDID == r.ownerDID {
			return
		}

		// Проверяем тип
		if env.Type != cfg.Type {
			return
		}

		// Применяем фильтр
		if cfg.Filter != nil && !cfg.Filter(&env) {
			r.logger.Debug().
				Str("type", env.Type).
				Str("from", env.FromDID).
				Msg("filtered out")
			return
		}

		// Обновляем HLC
		if !env.TID.IsZero() {
			r.clock.UpdateTID(env.TID)
		}

		// Добавляем в очередь с приоритетом
		if !r.queue.Push(&env, cfg.Priority) {
			r.logger.Warn().
				Str("type", env.Type).
				Str("from", env.FromDID).
				Msg("queue full, dropped")
		}
	}
}

// worker обрабатывает сообщения из очереди
func (r *Ripples) worker(id int) {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			env := r.queue.Pop()
			if env == nil {
				// Очередь пуста - ждём
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}

			r.processEnvelope(env)
		}
	}
}

// processEnvelope обрабатывает одно сообщение
func (r *Ripples) processEnvelope(env *Envelope) {
	r.mu.RLock()
	cfg, exists := r.configs[env.Type]
	r.mu.RUnlock()

	if !exists {
		return
	}

	r.logger.Debug().
		Str("type", env.Type).
		Str("id", env.ID).
		Str("from", env.FromDID).
		Msg("processing")

	for _, handler := range cfg.Handlers {
		ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)

		if err := handler(ctx, env); err != nil {
			r.logger.Error().
				Err(err).
				Str("type", env.Type).
				Str("id", env.ID).
				Msg("handler error")
		}

		cancel()
	}
}

// publishLoop периодически публикует сообщения
func (r *Ripples) publishLoop(cfg *Config) {
	defer r.wg.Done()

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	// Первая публикация сразу
	r.publish(cfg)

	for {
		select {
		case <-ticker.C:
			r.publish(cfg)
		case <-r.ctx.Done():
			return
		}
	}
}

// publish публикует одно сообщение
func (r *Ripples) publish(cfg *Config) {
	if cfg.Publisher == nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	env, err := cfg.Publisher(ctx)
	if err != nil {
		r.logger.Error().Err(err).Str("type", cfg.Type).Msg("publisher error")
		return
	}

	if env == nil {
		return
	}

	if err := r.Send(cfg.Type, env.Payload); err != nil {
		r.logger.Error().Err(err).Str("type", cfg.Type).Msg("send failed")
	}
}

// Send отправляет сообщение
func (r *Ripples) Send(msgType string, payload []byte) error {
	env := &Envelope{
		ID:      r.clock.TID().String(),
		Type:    msgType,
		FromDID: r.ownerDID,
		TID:     r.clock.TID(),
		Payload: payload,
	}

	return r.sendEnvelope(env)
}

// SendJSON отправляет JSON payload
func (r *Ripples) SendJSON(msgType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return r.Send(msgType, data)
}

// Broadcast отправляет broadcast сообщение (для совместимости)
func (r *Ripples) Broadcast(msgType string, payload any) error {
	return r.SendJSON(msgType, payload)
}

// sendEnvelope отправляет envelope
func (r *Ripples) sendEnvelope(env *Envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	topic := fmt.Sprintf("%s/%s", r.prefix, env.Type)

	r.mu.RLock()
	cfg := r.configs[env.Type]
	qos := byte(1)
	if cfg != nil {
		qos = cfg.QoS
	}
	r.mu.RUnlock()

	token := r.mqtt.Publish(topic, qos, false, data)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	r.logger.Debug().
		Str("type", env.Type).
		Str("topic", topic).
		Str("id", env.ID).
		Msg("sent")

	return nil
}

// Clock возвращает HLC
func (r *Ripples) Clock() *hlc.Clock {
	return r.clock
}

// OwnerDID возвращает DID владельца
func (r *Ripples) OwnerDID() string {
	return r.ownerDID
}

// QueueStats возвращает статистику очереди
func (r *Ripples) QueueStats() int {
	return r.queue.Len()
}

// Close завершает работу
func (r *Ripples) Close() {
	r.logger.Info().Msg("shutting down")

	r.cancel()
	r.wg.Wait()

	r.logger.Info().Msg("shutdown complete")
}

// TypedHandler создаёт типизированный handler
func TypedHandler[T any](fn func(ctx context.Context, from string, msg T) error) Handler {
	return func(ctx context.Context, env *Envelope) error {
		var msg T
		if err := json.Unmarshal(env.Payload, &msg); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}
		return fn(ctx, env.FromDID, msg)
	}
}

// WithCausalOrder создаёт handler с гарантией причинного порядка
// Сообщения от одного источника обрабатываются в порядке TID
func WithCausalOrder(handler Handler) Handler {
	var mu sync.Mutex
	lastTID := make(map[string]hlc.TID)

	return func(ctx context.Context, env *Envelope) error {
		mu.Lock()
		last := lastTID[env.FromDID]

		// Проверяем причинный порядок
		if !last.IsZero() && env.TID.Before(last) {
			mu.Unlock()
			// Сообщение из прошлого - пропускаем или буферизуем
			return nil
		}

		lastTID[env.FromDID] = env.TID
		mu.Unlock()

		return handler(ctx, env)
	}
}
