package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
	"google.golang.org/grpc"
)

const defaultGRPCListenAddress = ":7443"
const defaultGraphPath = "data/graph.db"
const defaultTenant = "default"
const defaultMaxWriteBatchBytes = pebblestore.DefaultMaxWriteBatchBytes

const indexSchemaConfigEnv = "VITALEDGE_INDEX_SCHEMA_CONFIG"
const metricsListenEnv = "VITALEDGE_METRICS_LISTEN"
const grpcListenEnv = "VITALEDGE_GRPC_LISTEN"
const graphPathEnv = "VITALEDGE_GRAPH_PATH"
const defaultTenantEnv = "VITALEDGE_DEFAULT_TENANT"
const maxWriteBatchBytesEnv = "VITALEDGE_MAX_WRITE_BATCH_BYTES"

type Config struct {
	GraphPath            string
	DefaultTenant        string
	IndexConfigPath      string
	IndexCatalog         *indexschema.Catalog
	ExecutorMetrics      *executor.Collector
	MetricsListenAddress string
	GRPCListenAddress    string
	MaxWriteBatchBytes   int
	Store                graph.GraphStore
	Executor             *executor.Executor
}

type Server struct {
	Config
	metrics   *executor.Collector
	executor  *executor.Executor
	metricsSV *http.Server
	grpcSV    *grpc.Server
	grpcLN    net.Listener
}

func NewServer(config Config) *Server {
	if strings.TrimSpace(config.GRPCListenAddress) == "" {
		config.GRPCListenAddress = defaultGRPCListenAddress
	}
	if strings.TrimSpace(config.DefaultTenant) == "" {
		config.DefaultTenant = defaultTenant
	}
	return &Server{
		Config:   config,
		metrics:  config.ExecutorMetrics,
		executor: config.Executor,
	}
}

func (s *Server) Start() error {
	log.Println("(v:Vital)ﮩ٨ـﮩﮩ٨ـ[e:Edge]ﮩ٨ـﮩﮩ٨ـ()")
	if strings.TrimSpace(s.MetricsListenAddress) != "" {
		metricsServer, err := startMetricsServer(s.MetricsListenAddress, s.metrics)
		if err != nil {
			return err
		}
		s.metricsSV = metricsServer
		log.Printf("Metrics endpoint is listening on %s/metrics", s.MetricsListenAddress)
	}
	grpcServer, grpcListener, err := startGRPCServer(s.GRPCListenAddress, &grpcQueryHandler{executor: s.executor, defaultTenant: s.DefaultTenant, maxWriteBatchBytes: int64(s.MaxWriteBatchBytes)})
	if err != nil {
		if s.metricsSV != nil {
			_ = s.metricsSV.Close()
		}
		return err
	}
	s.grpcSV = grpcServer
	s.grpcLN = grpcListener
	log.Printf("gRPC endpoint is listening on %s", grpcListener.Addr().String())
	err = s.grpcSV.Serve(s.grpcLN)
	if s.metricsSV != nil {
		_ = s.metricsSV.Close()
	}
	return err
}

func main() {
	config, err := loadConfigFromStartup()
	if err != nil {
		log.Fatalf("startup config error: %v", err)
	}

	store, err := openGraphStore(config.GraphPath, config.MaxWriteBatchBytes)
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
	var graphPath string
	var tenant string
	var indexConfigPath string
	var metricsListenAddress string
	var grpcListenAddress string
	var maxWriteBatchBytes int

	flag.StringVar(&graphPath, "graph-path", defaultGraphPath, "Path to graph store directory")
	flag.StringVar(&tenant, "tenant", defaultTenant, "Default tenant used for query execution")
	flag.StringVar(&indexConfigPath, "index-schema-config", "", "Path to index schema configuration JSON file")
	flag.StringVar(&metricsListenAddress, "metrics-listen", "", "Optional HTTP listen address for Prometheus metrics endpoint (for example :9100)")
	flag.StringVar(&grpcListenAddress, "grpc-listen", defaultGRPCListenAddress, "gRPC listen address for QueryService")
	flag.IntVar(&maxWriteBatchBytes, "max-write-batch-bytes", defaultMaxWriteBatchBytes, "Maximum bytes allowed in a single write transaction batch")
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
	if env := strings.TrimSpace(os.Getenv(metricsListenEnv)); env != "" {
		metricsListenAddress = env
	}
	if env := strings.TrimSpace(os.Getenv(grpcListenEnv)); env != "" {
		grpcListenAddress = env
	}
	if env := strings.TrimSpace(os.Getenv(maxWriteBatchBytesEnv)); env != "" {
		parsed, err := strconv.Atoi(env)
		if err != nil {
			return Config{}, err
		}
		maxWriteBatchBytes = parsed
	}
	if maxWriteBatchBytes <= 0 {
		return Config{}, fmt.Errorf("max write batch bytes must be > 0")
	}

	cfg := Config{
		GraphPath:            graphPath,
		DefaultTenant:        tenant,
		IndexConfigPath:      indexConfigPath,
		IndexCatalog:         indexschema.NewCatalog(),
		MetricsListenAddress: metricsListenAddress,
		GRPCListenAddress:    grpcListenAddress,
		MaxWriteBatchBytes:   maxWriteBatchBytes,
		ExecutorMetrics:      executor.NewCollector(),
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

func openGraphStore(path string, maxWriteBatchBytes int) (graph.GraphStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("graph path is required")
	}
	if err := os.MkdirAll(filepath.Clean(path), 0o755); err != nil {
		return nil, err
	}
	if maxWriteBatchBytes <= 0 {
		return nil, fmt.Errorf("max write batch bytes must be > 0")
	}
	return pebblestore.OpenWithOptions(path, pebblestore.StoreOptions{MaxWriteBatchBytes: maxWriteBatchBytes})
}
