package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"

	"mqtt-http-tunnel/internal/rpc"
)

// Структуры для примера
type PingRequest struct {
	Message string `json:"message"`
}

type PongResponse struct {
	Reply     string    `json:"reply"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Настраиваем логер с красивым цветным выводом
	logger := zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}).
		With().
		Timestamp().
		Logger().
		Level(zerolog.DebugLevel)

	// MQTT брокер
	broker := "tcp://localhost:1883"

	// Создаем две ноды
	logger.Info().Msg("Starting example: Node A calls Node B")

	// Запускаем Node B (сервер)
	go runNodeB(broker, logger)
	time.Sleep(20 * time.Second) // Даем время на подключение

	// Запускаем Node A (клиент)
	runNodeA(broker, logger)
}

// runNodeB - нода, которая отвечает на запросы
func runNodeB(broker string, logger zerolog.Logger) {
	nodeLogger := logger.With().Str("node", "B").Logger()

	// MQTT клиент для Node B
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("node-b").
		SetCleanSession(true)

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		nodeLogger.Fatal().Err(token.Error()).Msg("Failed to connect Node B")
	}

	// Создаем RPC клиент
	rpcClient, err := rpc.New(mqttClient, &rpc.Config{
		OwnerDID:       "node-b-did",
		Prefix:         "example",
		QoS:            1,
		RequestTimeout: 10 * time.Second,
		Logger:         nodeLogger,
	})
	if err != nil {
		nodeLogger.Fatal().Err(err).Msg("Failed to create RPC client for Node B")
	}
	defer rpcClient.Close()

	// Регистрируем метод "ping"
	err = rpcClient.Register("ping", func(ctx context.Context, params []byte) ([]byte, error) {
		var req PingRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, fmt.Errorf("invalid request: %w", err)
		}

		nodeLogger.Info().Str("message", req.Message).Msg("Received ping")

		// Формируем ответ
		resp := PongResponse{
			Reply:     fmt.Sprintf("Pong! Received: %s", req.Message),
			Timestamp: time.Now(),
		}

		return json.Marshal(resp)
	})
	if err != nil {
		nodeLogger.Fatal().Err(err).Msg("Failed to register ping handler")
	}

	nodeLogger.Info().Msg("Node B is ready and listening for RPC calls")

	// Держим Node B работающим
	select {}
}

// runNodeA - нода, которая вызывает методы на Node B
func runNodeA(broker string, logger zerolog.Logger) {
	nodeLogger := logger.With().Str("node", "A").Logger()

	// MQTT клиент для Node A
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("node-a").
		SetCleanSession(true)

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		nodeLogger.Fatal().Err(token.Error()).Msg("Failed to connect Node A")
	}

	// Создаем RPC клиент
	rpcClient, err := rpc.New(mqttClient, &rpc.Config{
		OwnerDID:       "node-a-did",
		Prefix:         "example",
		QoS:            1,
		RequestTimeout: 10 * time.Second,
		Logger:         nodeLogger,
	})
	if err != nil {
		nodeLogger.Fatal().Err(err).Msg("Failed to create RPC client for Node A")
	}
	defer rpcClient.Close()

	nodeLogger.Info().Msg("Node A is ready")

	// Вызываем метод "ping" на Node B
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := PingRequest{
		Message: "Hello from Node A!",
	}

	var resp PongResponse

	nodeLogger.Info().Msg("Calling ping on Node B...")
	err = rpcClient.Call(ctx, "node-b-did", "ping", req, &resp)
	if err != nil {
		nodeLogger.Fatal().Err(err).Msg("RPC call failed")
	}

	nodeLogger.Info().
		Str("reply", resp.Reply).
		Time("timestamp", resp.Timestamp).
		Msg("Received pong from Node B")

	fmt.Println("\n✅ Success!")
	fmt.Printf("Node A sent: %s\n", req.Message)
	fmt.Printf("Node B replied: %s\n", resp.Reply)
	fmt.Printf("Timestamp: %s\n", resp.Timestamp.Format(time.RFC3339))

	// Даем время на обработку логов
	time.Sleep(1 * time.Second)
}
