package main

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/improved/evstore"
	"mqtt-http-tunnel/improved/hlc"
	"mqtt-http-tunnel/improved/ripples"
	"mqtt-http-tunnel/improved/rpc"
	"mqtt-http-tunnel/improved/vclock"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

// Пример: Распределённый лог событий с MQTT синхронизацией
// Демонстрирует все улучшения из теории распределённых систем

// --- Сообщения синхронизации ---

// HeartbeatPayload heartbeat с текущим состоянием
type HeartbeatPayload struct {
	ProcessID string   `json:"process_id"`
	Heads     []string `json:"heads"` // Текущие heads (CID)
	Count     int      `json:"count"` // Количество записей
	TID       string   `json:"tid"`   // Последний TID
}

// SyncRequest запрос на синхронизацию
type SyncRequest struct {
	Missing []string `json:"missing"` // CID которые нужны
}

// SyncResponse ответ с записями
type SyncResponse struct {
	Records [][]byte `json:"records"` // Сериализованные записи
}

// --- Нода распределённой системы ---

type Node struct {
	store   *evstore.Store
	causal  *evstore.CausalBuffer
	ripples *ripples.Ripples
	rpc     *rpc.RPC
	logger  zerolog.Logger
}

func NewNode(mqttClient mqtt.Client, dataPath string, logger zerolog.Logger) (*Node, error) {
	// Создаём хранилище
	store, err := evstore.OpenStore(&evstore.StoreConfig{
		Path:          dataPath,
		MergeStrategy: evstore.MergeStrategyLWW, // Last-Writer-Wins
		Logger:        logger,
	})
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	// Создаём causal buffer
	causal := evstore.NewCausalBuffer(store, &evstore.CausalBufferConfig{
		MaxAge: 5 * time.Minute,
		Logger: logger,
		OnExpire: func(cid evstore.CID, r *evstore.Record) {
			logger.Warn().Str("cid", cid.Short()).Msg("record expired from causal buffer")
		},
	})

	// Создаём Ripples
	ripplesInst := ripples.New(mqttClient, &ripples.RipplesConfig{
		OwnerDID: store.ProcessID(),
		Prefix:   "evlog",
		Logger:   logger,
		Workers:  4,
		QueueConfig: &ripples.FairQueueConfig{
			MaxPerSource: 100,
			MaxTotal:     10000,
		},
	})

	// Создаём RPC
	rpcInst, err := rpc.New(mqttClient, &rpc.Config{
		OwnerDID:       store.ProcessID(),
		Prefix:         "evlog/rpc",
		RequestTimeout: 10 * time.Second,
		Logger:         logger,
	})
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("create rpc: %w", err)
	}

	node := &Node{
		store:   store,
		causal:  causal,
		ripples: ripplesInst,
		rpc:     rpcInst,
		logger:  logger,
	}

	// Регистрируем handlers
	if err := node.setupHandlers(); err != nil {
		store.Close()
		return nil, err
	}

	return node, nil
}

func (n *Node) setupHandlers() error {
	// Heartbeat - периодическая публикация состояния
	if err := n.ripples.Register(&ripples.Config{
		Type:     "heartbeat",
		Interval: 5 * time.Second,
		QoS:      0,
		Publisher: func(ctx context.Context) (*ripples.Envelope, error) {
			heads := n.store.Heads()
			headStrs := make([]string, len(heads))
			for i, h := range heads {
				headStrs[i] = h.String()
			}

			payload := HeartbeatPayload{
				ProcessID: n.store.ProcessID(),
				Heads:     headStrs,
				Count:     n.store.Count(),
				TID:       n.store.Clock().TID().String(),
			}

			data, _ := json.Marshal(payload)
			return &ripples.Envelope{Payload: data}, nil
		},
		Handlers: []ripples.Handler{
			ripples.TypedHandler(n.handleHeartbeat),
		},
	}); err != nil {
		return err
	}

	// Record broadcast - новые записи
	if err := n.ripples.Register(&ripples.Config{
		Type:     "record",
		QoS:      1,
		Priority: ripples.PriorityHigh,
		Handlers: []ripples.Handler{
			ripples.WithCausalOrder(n.handleRecordBroadcast),
		},
	}); err != nil {
		return err
	}

	// RPC: GetRecords - получение записей по CID
	if err := rpc.RegisterTyped(n.rpc, "GetRecords", n.handleGetRecords); err != nil {
		return err
	}

	// Подписываемся на новые локальные записи для broadcast
	n.store.Subscribe(func(cid evstore.CID, record *evstore.Record) {
		// Broadcast новой записи
		data := record.Marshal()
		if err := n.ripples.Send("record", data); err != nil {
			n.logger.Error().Err(err).Msg("failed to broadcast record")
		}
	})

	return nil
}

// handleHeartbeat обрабатывает heartbeat от других нод
func (n *Node) handleHeartbeat(ctx context.Context, from string, hb HeartbeatPayload) error {
	n.logger.Debug().
		Str("from", from).
		Int("heads", len(hb.Heads)).
		Int("count", hb.Count).
		Msg("received heartbeat")

	// Проверяем, есть ли у нас все heads
	var missing []evstore.CID
	for _, headStr := range hb.Heads {
		cid, err := evstore.CIDFromHex(headStr)
		if err != nil {
			continue
		}
		if !n.store.Has(cid) {
			missing = append(missing, cid)
		}
	}

	if len(missing) > 0 {
		// Запрашиваем недостающие записи
		go n.requestMissing(ctx, from, missing)
	}

	return nil
}

// handleRecordBroadcast обрабатывает broadcast записи
func (n *Node) handleRecordBroadcast(ctx context.Context, env *ripples.Envelope) error {
	record, err := evstore.UnmarshalRecord(env.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal record: %w", err)
	}

	cid := record.CID()

	// Используем causal buffer для гарантии порядка
	delivered, missing := n.causal.Receive(record)

	n.logger.Debug().
		Str("cid", cid.Short()).
		Int("delivered", len(delivered)).
		Int("missing", len(missing)).
		Msg("received record")

	// Запрашиваем недостающие
	if len(missing) > 0 {
		go n.requestMissing(ctx, env.FromDID, missing)
	}

	// Merge доставленные записи в heads
	if len(delivered) > 0 {
		cids := make([]evstore.CID, len(delivered))
		for i, r := range delivered {
			cids[i] = r.CID()
		}
		n.store.Merge(cids)
	}

	return nil
}

// handleGetRecords RPC handler для получения записей
func (n *Node) handleGetRecords(ctx context.Context, req SyncRequest) (SyncResponse, error) {
	var records [][]byte

	for _, cidStr := range req.Missing {
		cid, err := evstore.CIDFromHex(cidStr)
		if err != nil {
			continue
		}

		record, err := n.store.Get(cid)
		if err != nil {
			continue
		}

		records = append(records, record.Marshal())
	}

	return SyncResponse{Records: records}, nil
}

// requestMissing запрашивает недостающие записи
func (n *Node) requestMissing(ctx context.Context, targetDID string, missing []evstore.CID) {
	missingStrs := make([]string, len(missing))
	for i, cid := range missing {
		missingStrs[i] = cid.String()
	}

	req := SyncRequest{Missing: missingStrs}
	var resp SyncResponse

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := n.rpc.Call(ctx, targetDID, "GetRecords", req, &resp); err != nil {
		n.logger.Warn().Err(err).Str("target", targetDID).Msg("failed to get records")
		return
	}

	// Обрабатываем полученные записи через causal buffer
	for _, data := range resp.Records {
		record, err := evstore.UnmarshalRecord(data)
		if err != nil {
			continue
		}

		delivered, _ := n.causal.Receive(record)
		if len(delivered) > 0 {
			cids := make([]evstore.CID, len(delivered))
			for i, r := range delivered {
				cids[i] = r.CID()
			}
			n.store.Merge(cids)
		}
	}
}

// Append добавляет новую запись
func (n *Node) Append(data []byte) (*evstore.Record, error) {
	return n.store.Append(data)
}

// Close закрывает ноду
func (n *Node) Close() {
	n.ripples.Close()
	n.rpc.Close()
	n.store.Close()
}

// --- Демонстрация Vector Clocks ---

func demonstrateVectorClocks() {
	fmt.Println("=== Vector Clocks Demo ===")

	// Три процесса
	vc1 := vclock.New() // Process 1
	vc2 := vclock.New() // Process 2
	vc3 := vclock.New() // Process 3

	// События на процессе 1
	vc1.Tick("p1") // a
	vc1.Tick("p1") // b

	// События на процессе 2
	vc2.Tick("p2") // c

	fmt.Printf("После a,b на p1: %v\n", vc1.Map())
	fmt.Printf("После c на p2: %v\n", vc2.Map())
	fmt.Printf("a,b || c (параллельны): %v\n", vc1.IsConcurrent(vc2))

	// Сообщение от p1 к p2
	vc2Copy := vc2.Clone()
	vc2Copy.MergeAndTick(vc1, "p2") // d - после получения от p1

	fmt.Printf("После d (получил от p1): %v\n", vc2Copy.Map())
	fmt.Printf("b ≺ d: %v\n", vc1.HappensBefore(vc2Copy))

	// Сообщение от p2 к p3
	vc3.MergeAndTick(vc2Copy, "p3") // e

	fmt.Printf("После e (получил от p2): %v\n", vc3.Map())
	fmt.Printf("a ≺ e (транзитивность): %v\n", vclock.FromMap(map[string]uint64{"p1": 1}).HappensBefore(vc3))

	fmt.Println()
}

// --- Демонстрация HLC ---

func demonstrateHLC() {
	fmt.Println("=== HLC Demo ===")

	clock1 := hlc.New("node1")
	clock2 := hlc.New("node2")

	// Локальные события
	ts1 := clock1.Now()
	ts2 := clock1.Now()
	ts3 := clock1.Now()

	fmt.Printf("ts1: PT=%d LC=%d\n", ts1.PT, ts1.LC)
	fmt.Printf("ts2: PT=%d LC=%d\n", ts2.PT, ts2.LC)
	fmt.Printf("ts3: PT=%d LC=%d\n", ts3.PT, ts3.LC)
	fmt.Printf("ts1 < ts2: %v\n", ts1.Before(ts2))

	// Получение сообщения
	ts4, _ := clock2.Update(ts3)
	fmt.Printf("clock2 после Update(ts3): PT=%d LC=%d\n", ts4.PT, ts4.LC)

	// TID
	tid := clock1.TID()
	fmt.Printf("TID: %s (time: %v)\n", tid.String(), tid.Time())

	fmt.Println()
}

// --- Main ---

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Демонстрации
	demonstrateVectorClocks()
	demonstrateHLC()

	fmt.Println("=== Node Demo ===")
	fmt.Println("Для полной демонстрации нужен MQTT брокер.")
	fmt.Println("Пример подключения:")
	fmt.Println()
	fmt.Println(`
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("node1")
	
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	
	node, err := NewNode(client, "./data", logger)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()
	
	// Добавляем запись
	record, _ := node.Append([]byte("hello world"))
	fmt.Printf("Created: %s\n", record.CID().Short())
	`)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info().Msg("Press Ctrl+C to exit")
	<-sigCh
	logger.Info().Msg("Shutting down...")
}
