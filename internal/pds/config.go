// internal/pds/config.go
package pds

type Config struct {
	NodeID       string
	DataPath     string
	HTTPPort     int
	GraphQLPath  string
	BadgerConfig BadgerConfig
	MQTTConfig   MQTTConfig
	DebugConfig  DebugConfig
}

type BadgerConfig struct {
	GCInterval   string
	MaxTableSize int64
	Compression  bool
}

type MQTTConfig struct {
	Enabled      bool
	Broker       string
	ClientID     string
	Username     string
	Password     string
	TopicPrefix  string
	QoS          byte
	CleanSession bool
}

type DebugConfig struct {
	ExportEventLogOnStartup bool // Экспортировать eventlog при старте для отладки
}

func DefaultConfig() *Config {
	return &Config{
		DataPath:    "/data/pds",
		HTTPPort:    8080,
		GraphQLPath: "/graphql",
		BadgerConfig: BadgerConfig{
			GCInterval:   "10m",
			MaxTableSize: 4 * 1024 * 1024,
			Compression:  true,
		},
		MQTTConfig: MQTTConfig{
			Enabled:      false,
			Broker:       "tcp://localhost:1883",
			ClientID:     "",
			Username:     "",
			Password:     "",
			TopicPrefix:  "eventlog",
			QoS:          1,
			CleanSession: true,
		},
		DebugConfig: DebugConfig{
			ExportEventLogOnStartup: true, // По умолчанию включен для отладки
		},
	}
}
