// internal/pds/server.go
package pds

import (
	"context"
	"encoding/json"
	"fmt"
	"mqtt-http-tunnel/internal/collection"
	"mqtt-http-tunnel/internal/event"
	"mqtt-http-tunnel/internal/eventlog"
	"mqtt-http-tunnel/internal/graphql"
	"mqtt-http-tunnel/internal/identity"
	"mqtt-http-tunnel/internal/schema"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Server struct {
	config     *Config
	db         *badger.DB
	schema     *schema.Registry
	engine     *collection.Engine
	resolver   *graphql.Resolver
	httpServer *http.Server
	ownerDID   *identity.DID
	keyPair    *identity.KeyPair
	// keyManager   *keymanager.KeyManager
	eventLog     *eventlog.EventLog
	synchronizer *eventlog.MQTTSynchronizer
}

func NewServer(config *Config) (*Server, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}
	return &Server{
		config: config,
	}, nil
}

func (s *Server) Start(ctx context.Context) error {

	if err := s.initStorage(); err != nil {
		return fmt.Errorf("init storage: %w", err)
	}

	if err := s.initIdentity(); err != nil {
		return fmt.Errorf("init identity: %w", err)
	}

	if err := s.initSynchronizer(); err != nil {
		return fmt.Errorf("init synchronizer: %w", err)
	}

	if err := s.initEngine(); err != nil {
		return fmt.Errorf("init engine: %w", err)
	}

	if err := s.initGraphQL(); err != nil {
		return fmt.Errorf("init graphql: %w", err)
	}

	if err := s.startHTTP(); err != nil {
		return fmt.Errorf("start http: %w", err)
	}

	go s.runGC(ctx)

	return nil
}

func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(ctx)
	}

	if s.synchronizer != nil {
		if err := s.synchronizer.Close(); err != nil {
			fmt.Printf("Error closing synchronizer: %v\n", err)
		}
	}

	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

func (s *Server) initStorage() error {
	dbPath := filepath.Join(s.config.DataPath, "badger")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	fmt.Printf("BadgerDB initialized at %s\n", dbPath)
	return nil
}

func (s *Server) initIdentity() error {
	identityPath := filepath.Join(s.config.DataPath, "identity.json")
	if err := os.MkdirAll(s.config.DataPath, 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	manager, err := identity.LoadOrCreateIdentityManager(identityPath, nil)
	if err != nil {
		return fmt.Errorf("load or create identity: %w", err)
	}
	s.ownerDID = manager.GetDID()
	s.keyPair = manager.GetKeyPair()

	// Инициализируем KeyManager
	// s.keyManager = keymanager.NewKeyManager()

	fmt.Printf("Node DID: %s\n", s.ownerDID.String())
	fmt.Printf("Identity persisted at: %s\n", identityPath)
	return nil
}

func (s *Server) initSynchronizer() error {
	if !s.config.MQTTConfig.Enabled {
		fmt.Println("MQTT synchronizer disabled")
		return nil
	}
	clientID := s.config.MQTTConfig.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("pds-%s", s.ownerDID.String()[:16])
	}
	config := eventlog.MQTTConfig{
		Broker:       s.config.MQTTConfig.Broker,
		ClientID:     clientID,
		Username:     s.config.MQTTConfig.Username,
		Password:     s.config.MQTTConfig.Password,
		TopicPrefix:  s.config.MQTTConfig.TopicPrefix,
		QoS:          s.config.MQTTConfig.QoS,
		CleanSession: s.config.MQTTConfig.CleanSession,
		KeyPair:      s.keyPair,
		// KeyManager:   s.keyManager,
	}
	sync, err := eventlog.NewMQTTSynchronizer(s.ownerDID, config)
	if err != nil {
		return fmt.Errorf("create MQTT synchronizer: %w", err)
	}
	s.synchronizer = sync
	fmt.Printf("MQTT synchronizer connected to %s\n", s.config.MQTTConfig.Broker)
	return nil
}

func (s *Server) initEngine() error {
	s.schema = schema.NewRegistry(s.db)
	if err := s.schema.LoadAll(context.Background()); err != nil {
		return err
	}
	storage := eventlog.NewBadgerStorage(s.db, "eventlog:system")
	var synchronizer eventlog.Synchronizer
	if s.synchronizer != nil {
		synchronizer = s.synchronizer
	}
	eventLogConfig := eventlog.EventLogConfig{
		OwnerDID:           s.ownerDID,
		KeyPair:            s.keyPair,
		ClockID:            0,
		Storage:            storage,
		Synchronizer:       synchronizer,
		ConflictResolution: eventlog.LastWriteWins,
		MaxBatchSize:       100,
		EnableGC:           false,
		// KeyManager:         s.keyManager,
		OnRemoteEvent: func(ev *event.Event) error {
			if s.engine != nil {
				return s.engine.HandleRemoteEvent(ev)
			}
			return nil
		},
	}
	eventLog, err := eventlog.NewEventLog(eventLogConfig)
	if err != nil {
		return fmt.Errorf("create eventlog: %w", err)
	}
	s.eventLog = eventLog
	s.engine = collection.NewEngine(s.db, s.ownerDID, s.keyPair, s.schema, s.eventLog)
	if err := s.engine.LoadAll(context.Background()); err != nil {
		return err
	}
	fmt.Println("Collection engine initialized")

	// Экспортируем eventlog для отладки (если включено в конфиге)
	if s.config.DebugConfig.ExportEventLogOnStartup {
		if err := s.exportEventLogDebug(); err != nil {
			fmt.Printf("Warning: failed to export eventlog debug: %v\n", err)
		}
	}

	return nil
}

func (s *Server) initGraphQL() error {
	s.resolver = graphql.NewResolver(s.engine)
	if err := s.resolver.Build(); err != nil {
		return err
	}
	fmt.Println("GraphQL resolver initialized")
	return nil
}

func (s *Server) startHTTP() error {
	mux := http.NewServeMux()
	graphqlHandler := graphql.NewHandler(s.resolver)
	mux.Handle(s.config.GraphQLPath, graphqlHandler)
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/collections", s.collectionsHandler)
	mux.HandleFunc("/collections/create", s.createCollectionHandler)
	mux.HandleFunc("/collections/update", s.updateCollectionHandler)
	mux.HandleFunc("/peers", s.peersHandler)
	addr := fmt.Sprintf(":%d", s.config.HTTPPort)
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
	fmt.Printf("HTTP server started on %s\n", addr)
	fmt.Printf("GraphQL endpoint: http://localhost%s%s\n", addr, s.config.GraphQLPath)
	return nil
}

func (s *Server) runGC(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := s.db.RunValueLogGC(0.5)
			if err != nil && err != badger.ErrNoRewrite {
				fmt.Printf("GC error: %v\n", err)
			}
		}
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) collectionsHandler(w http.ResponseWriter, r *http.Request) {
	collections := s.engine.ListCollections()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"collections": collections,
	})
}

func (s *Server) createCollectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	col, err := s.engine.CreateCollection(r.Context(), req.Schema)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.resolver.Build(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"name":    col.Name(),
		"message": "Collection created successfully",
	})
}

func (s *Server) updateCollectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name   string `json:"name"`
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "collection name is required", http.StatusBadRequest)
		return
	}
	if req.Schema == "" {
		http.Error(w, "schema is required", http.StatusBadRequest)
		return
	}
	col, err := s.engine.UpdateCollection(r.Context(), req.Name, req.Schema)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.resolver.Build(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"name":    col.Name(),
		"version": col.Schema().Version,
		"message": "Collection schema updated successfully",
	})
}

func (s *Server) peersHandler(w http.ResponseWriter, r *http.Request) {
	if s.synchronizer == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"peers":   []interface{}{},
			"message": "MQTT synchronizer not enabled",
		})
		return
	}

	peers, err := s.synchronizer.GetPeersWithKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type peerResponse struct {
		DID       string `json:"did"`
		PublicKey string `json:"publicKey,omitempty"`
		LastSeen  string `json:"lastSeen"`
	}

	response := make([]peerResponse, 0, len(peers))
	for _, peer := range peers {
		pr := peerResponse{
			DID:      peer.DID.String(),
			LastSeen: peer.LastSeen.Format(time.RFC3339),
		}
		if peer.PublicKey != nil {
			pr.PublicKey = identity.EncodePublicKey(nil) // Encode bytes to base64
			// Actually we need to encode the raw bytes
			pr.PublicKey = fmt.Sprintf("%x", peer.PublicKey)
		}
		response = append(response, pr)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peers": response,
		"count": len(response),
	})
}

func (s *Server) Schema() *schema.Registry {
	return s.schema
}

func (s *Server) Engine() *collection.Engine {
	return s.engine
}

// exportEventLogDebug экспортирует eventlog в файл для отладки
func (s *Server) exportEventLogDebug() error {
	if s.eventLog == nil {
		return nil
	}

	debugDir := filepath.Join(s.config.DataPath, "debug")
	if err := os.MkdirAll(debugDir, 0755); err != nil {
		return fmt.Errorf("create debug directory: %w", err)
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(debugDir, fmt.Sprintf("eventlog_export_%s.jsonl", timestamp))

	ctx := context.Background()
	err := s.eventLog.ExportToFile(ctx, filename, &eventlog.ExportOptions{
		IncludeMetadata: true,
	})
	if err != nil {
		return fmt.Errorf("export eventlog: %w", err)
	}

	fmt.Printf("EventLog exported to: %s\n", filename)
	return nil
}

func (s *Server) DB() *badger.DB {
	return s.db
}
