package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mqtt-http-tunnel/internal/pds"
)

var (
	configPath       = flag.String("config", "", "Path to config file (JSON)")
	nodeID           = flag.String("node-id", "", "Node ID (overrides config)")
	dataPath         = flag.String("data", "./data/pds", "Data directory path")
	httpPort         = flag.Int("http-port", 8080, "HTTP server port")
	mqttBroker       = flag.String("mqtt-broker", "", "MQTT broker URL (e.g., tcp://localhost:1883)")
	mqttEnable       = flag.Bool("mqtt-enable", false, "Enable MQTT synchronization")
	debugExportEvent = flag.Bool("debug-export-eventlog", true, "Export eventlog on startup for debugging")
)

func main() {
	flag.Parse()

	// Загружаем конфигурацию
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Создаем и запускаем сервер
	server, err := pds.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	fmt.Println("PDS server started successfully")

	// Ожидаем сигнала остановки
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	if err := server.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}
	fmt.Println("Server stopped")
}

func loadConfig() (*pds.Config, error) {
	var config *pds.Config

	// Если указан файл конфигурации, загружаем из него
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}

		config = &pds.Config{}
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	} else {
		// Используем дефолтную конфигурацию
		config = pds.DefaultConfig()
	}

	// Переопределяем параметры из флагов
	if *nodeID != "" {
		config.NodeID = *nodeID
	}
	if *dataPath != "" {
		config.DataPath = *dataPath
	}
	if *httpPort > 0 {
		config.HTTPPort = *httpPort
	}
	if *mqttBroker != "" {
		config.MQTTConfig.Broker = *mqttBroker
		config.MQTTConfig.Enabled = true
	}
	if *mqttEnable {
		config.MQTTConfig.Enabled = true
	}

	// Переопределяем debug опции
	if debugExportEvent != nil {
		config.DebugConfig.ExportEventLogOnStartup = *debugExportEvent
	}

	// Генерируем NodeID если не указан
	if config.NodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}
		config.NodeID = fmt.Sprintf("pds-%s", hostname)
	}

	return config, nil
}
