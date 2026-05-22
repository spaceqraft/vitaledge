package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher"
	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
	"github.com/paegun/vitaledge/internal/tcp"
)

const defaultListenAddress = ":6379"
const defaultGraphPath = "data/graph.db"
const defaultTenant = "default"
const defaultMetricsReportInterval = 30 * time.Second

const indexSchemaConfigEnv = "VITALEDGE_INDEX_SCHEMA_CONFIG"
const metricsReportIntervalEnv = "VITALEDGE_METRICS_REPORT_INTERVAL"
const graphPathEnv = "VITALEDGE_GRAPH_PATH"
const defaultTenantEnv = "VITALEDGE_DEFAULT_TENANT"

type Config struct {
	ListenAddress         string
	GraphPath             string
	DefaultTenant         string
	IndexConfigPath       string
	IndexCatalog          *indexschema.Catalog
	ExecutorMetrics       *executor.Collector
	MetricsReportInterval time.Duration
	Store                 graph.GraphStore
	Executor              *executor.Executor
}

type Server struct {
	Config
	ln        net.Listener
	peers     map[*tcp.Peer]bool
	addPeerCh chan *tcp.Peer
	quitCh    chan struct{}
	msgCh     chan tcp.PeerMessage
	metrics   *executor.Collector
	executor  *executor.Executor
}

type wireStats struct {
	RowsReturned int   `json:"rowsReturned"`
	DurationMS   int64 `json:"durationMs"`
}

type wireResponse struct {
	OK      bool           `json:"ok"`
	Error   string         `json:"error,omitempty"`
	Columns []string       `json:"columns,omitempty"`
	Rows    []executor.Row `json:"rows,omitempty"`
	Stats   wireStats      `json:"stats,omitempty"`
}

func NewServer(config Config) *Server {
	if strings.TrimSpace(config.ListenAddress) == "" {
		config.ListenAddress = defaultListenAddress
	}
	if strings.TrimSpace(config.DefaultTenant) == "" {
		config.DefaultTenant = defaultTenant
	}
	return &Server{
		Config:    config,
		peers:     make(map[*tcp.Peer]bool),
		addPeerCh: make(chan *tcp.Peer),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan tcp.PeerMessage),
		metrics:   config.ExecutorMetrics,
		executor:  config.Executor,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddress)
	if err != nil {
		return err
	}
	s.ln = ln

	log.Printf("Server is listening on %s", s.ListenAddress)
	if s.metrics != nil && s.MetricsReportInterval > 0 {
		go s.reportMetricsLoop(s.MetricsReportInterval)
	}
	go s.loop()
	return s.accept()
}

func (s *Server) loop() {
	for {
		select {
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				log.Printf("Error handling message: %v", err)
			}
		case <-s.quitCh:
			return
		}
	}
}

func (s *Server) accept() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			continue
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	peer := tcp.NewPeer(conn, s.msgCh)
	s.addPeerCh <- peer
	log.Printf("New peer connected: %s", conn.RemoteAddr())
	peer.ReadLoop()
}

func (s *Server) handleMessage(msg tcp.PeerMessage) error {
	query := strings.TrimSpace(string(msg.Data))
	if query == "" {
		return s.writeResponse(msg.Peer, wireResponse{OK: false, Error: "empty query"})
	}
	if s.executor == nil {
		return s.writeResponse(msg.Peer, wireResponse{OK: false, Error: "executor is not configured"})
	}

	stmt, err := cypher.ParseStatement(query)
	if err != nil {
		return s.writeResponse(msg.Peer, wireResponse{OK: false, Error: err.Error()})
	}

	result, err := s.executor.ExecuteStatement(context.Background(), stmt, executor.Params{"tenant": s.DefaultTenant})
	if err != nil {
		return s.writeResponse(msg.Peer, wireResponse{OK: false, Error: err.Error()})
	}

	return s.writeResponse(msg.Peer, wireResponse{
		OK:      true,
		Columns: result.Columns,
		Rows:    result.Rows,
		Stats: wireStats{
			RowsReturned: result.Stats.RowsReturned,
			DurationMS:   result.Stats.Duration.Milliseconds(),
		},
	})
}

func (s *Server) writeResponse(peer *tcp.Peer, response wireResponse) error {
	if peer == nil || peer.Conn == nil {
		return fmt.Errorf("peer connection is not available")
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	_, err = peer.Conn.Write(payload)
	return err
}

func (s *Server) reportMetricsLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			top := s.metrics.TopUnindexedCandidates(5)
			for _, candidate := range top {
				log.Printf("index-recommendation tenant=%s schema=%s property=%s observed=%d", candidate.Tenant, candidate.Schema, candidate.Property, candidate.Count)
			}
		case <-s.quitCh:
			return
		}
	}
}

func main() {
	config, err := loadConfigFromStartup()
	if err != nil {
		log.Fatalf("startup config error: %v", err)
	}

	store, err := openGraphStore(config.GraphPath)
	if err != nil {
		log.Fatalf("startup graph store error: %v", err)
	}
	config.Store = store
	config.Executor = executor.New(store, executor.Options{Metrics: config.ExecutorMetrics, IndexCatalog: config.IndexCatalog})

	if config.IndexCatalog != nil {
		log.Printf("Loaded index schema catalog")
	}
	server := NewServer(config)
	if err := server.Start(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func loadConfigFromStartup() (Config, error) {
	var listenAddress string
	var graphPath string
	var tenant string
	var indexConfigPath string
	var metricsReportInterval time.Duration

	flag.StringVar(&listenAddress, "listen", defaultListenAddress, "Listen address")
	flag.StringVar(&graphPath, "graph-path", defaultGraphPath, "Path to graph store directory")
	flag.StringVar(&tenant, "tenant", defaultTenant, "Default tenant used for query execution")
	flag.StringVar(&indexConfigPath, "index-schema-config", "", "Path to index schema configuration JSON file")
	flag.DurationVar(&metricsReportInterval, "metrics-report-interval", defaultMetricsReportInterval, "Interval for logging index recommendation metrics")
	flag.Parse()

	if strings.TrimSpace(indexConfigPath) == "" {
		indexConfigPath = strings.TrimSpace(os.Getenv(indexSchemaConfigEnv))
	}
	if env := strings.TrimSpace(os.Getenv(graphPathEnv)); env != "" {
		graphPath = env
	}
	if env := strings.TrimSpace(os.Getenv(defaultTenantEnv)); env != "" {
		tenant = env
	}
	if env := strings.TrimSpace(os.Getenv(metricsReportIntervalEnv)); env != "" {
		parsed, err := time.ParseDuration(env)
		if err != nil {
			return Config{}, err
		}
		metricsReportInterval = parsed
	}

	cfg := Config{
		ListenAddress:         listenAddress,
		GraphPath:             graphPath,
		DefaultTenant:         tenant,
		IndexConfigPath:       indexConfigPath,
		MetricsReportInterval: metricsReportInterval,
		ExecutorMetrics:       executor.NewCollector(),
	}
	if strings.TrimSpace(indexConfigPath) == "" {
		return cfg, nil
	}

	catalog, err := indexschema.LoadCatalogFromFile(indexConfigPath)
	if err != nil {
		return Config{}, err
	}
	cfg.IndexCatalog = catalog
	return cfg, nil
}

func openGraphStore(path string) (graph.GraphStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("graph path is required")
	}
	if err := os.MkdirAll(filepath.Clean(path), 0o755); err != nil {
		return nil, err
	}
	return pebblestore.Open(path)
}
