package ripples

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Envelope - общая обёртка для всех сообщений
type Envelope struct {
	ID        string `json:"id"`        // Уникальный ID сообщения
	Type      string `json:"type"`      // Тип сообщения (heartbeat, request, response, event, etc)
	FromDID   string `json:"fromDID"`   // DID отправителя
	Payload   []byte `json:"payload"`   // Полезная нагрузка
	Timestamp int64  `json:"timestamp"` // Unix timestamp
}

// Handler обрабатывает входящие сообщения определённого типа
type Handler func(ctx context.Context, env *Envelope) error

// Publisher периодически отправляет сообщения
type Publisher func(ctx context.Context) (*Envelope, error)

// Config конфигурация для одного типа ripple
type Config struct {
	Type      string        // Тип сообщения
	Handlers  []Handler     // Обработчик входящих сообщений
	Publisher Publisher     // Функция генерации исходящих сообщений (опционально)
	Interval  time.Duration // Интервал публикации (0 = не публиковать)
	QoS       byte          // MQTT QoS
}

// RipplesConfig конфигурация Ripples
type RipplesConfig struct {
	OwnerDID string
	Prefix   string
	Logger   zerolog.Logger
}

// Ripples управляет несколькими типами сообщений
type Ripples struct {
	mqtt     mqtt.Client
	ownerDID string
	prefix   string
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan *Envelope
	wg       sync.WaitGroup
	logger   zerolog.Logger
	configs  map[string]*Config
	mu       sync.RWMutex
}

func New(mqttClient mqtt.Client, cfg *RipplesConfig) *Ripples {
	if cfg == nil {
		cfg = &RipplesConfig{}
	}

	if cfg.OwnerDID == "" {
		cfg.OwnerDID = uuid.New().String()
	}

	if cfg.Prefix == "" {
		cfg.Prefix = "ripples"
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Ripples{
		mqtt:     mqttClient,
		ownerDID: cfg.OwnerDID,
		prefix:   cfg.Prefix,
		queue:    make(chan *Envelope, 1000),
		ctx:      ctx,
		cancel:   cancel,
		configs:  make(map[string]*Config),
		logger:   cfg.Logger,
	}

	r.wg.Add(1)
	go r.processQueue()

	r.logger.Info().Str("owner_did", r.ownerDID).Str("prefix", r.prefix).Msg("Ripples initialized")
	return r
}

// Register регистрирует новый тип ripple
func (r *Ripples) Register(cfg *Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.configs[cfg.Type]; exists {
		return fmt.Errorf("ripple type %s already registered", cfg.Type)
	}

	if len(cfg.Handlers) > 0 {
		topic := fmt.Sprintf("%s/%s", r.prefix, cfg.Type)
		token := r.mqtt.Subscribe(topic, cfg.QoS, r.makeMessageHandler(cfg))
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("subscribe failed: %w", token.Error())
		}
		r.logger.Info().Str("type", cfg.Type).Str("topic", topic).Int("handlers", len(cfg.Handlers)).Msg("Subscribed to ripple type")
	}

	r.configs[cfg.Type] = cfg

	if cfg.Publisher != nil && cfg.Interval > 0 {
		r.wg.Add(1)
		go r.publishLoop(cfg)
		r.logger.Info().Str("type", cfg.Type).Dur("interval", cfg.Interval).Msg("Started publisher")
	}

	return nil
}

func (r *Ripples) makeMessageHandler(cfg *Config) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var env Envelope
		if err := json.Unmarshal(msg.Payload(), &env); err != nil {
			r.logger.Error().Err(err).Str("topic", msg.Topic()).Msg("Failed to unmarshal envelope")
			return
		}
		if env.FromDID == r.ownerDID {
			return
		}
		if env.Type != cfg.Type {
			return
		}
		r.logger.Debug().Str("envelope_id", env.ID).Str("type", env.Type).Str("from_did", env.FromDID).Msg("Received envelope")
		select {
		case r.queue <- &env:
		default:
			r.logger.Warn().Str("envelope_id", env.ID).Str("type", env.Type).Msg("Queue full, dropping envelope")
		}
	}
}

func (r *Ripples) processQueue() {
	defer r.wg.Done()
	for {
		select {
		case env := <-r.queue:
			cfg, exists := r.configs[env.Type]
			if !exists {
				continue
			}
			r.mu.RLock()
			handlers := cfg.Handlers
			r.mu.RUnlock()
			r.logger.Debug().Str("type", env.Type).Str("envelope_id", env.ID).Str("from_did", env.FromDID).Int("handlers", len(handlers)).Msg("Processing envelope")
			for _, handler := range handlers {
				ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
				if err := handler(ctx, env); err != nil {
					r.logger.Error().Err(err).Str("type", cfg.Type).Str("envelope_id", env.ID).Msg("Handler error")
				}
				cancel()
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Ripples) publishLoop(cfg *Config) {
	defer r.wg.Done()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
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

func (r *Ripples) publish(cfg *Config) {
	if cfg.Publisher == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	env, err := cfg.Publisher(ctx)
	if err != nil {
		r.logger.Error().Err(err).Str("type", cfg.Type).Msg("Publisher error")
		return
	}
	env.Type = cfg.Type
	env.FromDID = r.ownerDID
	env.Timestamp = time.Now().UnixMilli()
	data, err := json.Marshal(env)
	if err != nil {
		r.logger.Error().Err(err).Str("type", cfg.Type).Msg("Failed to marshal envelope")
		return
	}
	topic := fmt.Sprintf("%s/%s", r.prefix, cfg.Type)
	token := r.mqtt.Publish(topic, cfg.QoS, false, data)
	if token.Wait() && token.Error() != nil {
		r.logger.Error().Err(token.Error()).Str("type", cfg.Type).Str("topic", topic).Msg("Failed to publish")
		return
	}
	r.logger.Debug().Str("type", cfg.Type).Str("topic", topic).Msg("Published envelope")
}

// Send отправляет сообщение вручную (для разовых отправок)
func (r *Ripples) Send(msgType, topic string, payload any) error {
	r.mu.RLock()
	cfg, exists := r.configs[msgType]
	r.mu.RUnlock()
	if !exists {
		return fmt.Errorf("unknown message type: %s", msgType)
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	env := &Envelope{
		ID:        fmt.Sprintf("%s-%d", r.ownerDID, time.Now().UnixNano()),
		Type:      msgType,
		FromDID:   r.ownerDID,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payloadData,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	fullTopic := fmt.Sprintf("%s/%s", r.prefix, topic)
	token := r.mqtt.Publish(fullTopic, cfg.QoS, false, data)
	if token.Wait() && token.Error() != nil {
		r.logger.Error().Err(token.Error()).Str("type", msgType).Str("topic", fullTopic).Msg("Failed to send")
		return token.Error()
	}
	r.logger.Debug().Str("type", msgType).Str("topic", fullTopic).Str("envelope_id", env.ID).Msg("Sent envelope")
	return nil
}

func (r *Ripples) Close() {
	r.logger.Info().Msg("Ripples shutting down")
	r.cancel()
	close(r.queue)
	r.wg.Wait()
	r.logger.Info().Msg("Ripples shutdown complete")
}
