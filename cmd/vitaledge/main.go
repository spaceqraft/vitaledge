package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/spaceqraft/vitaledge/internal/cypher/executor"
	"github.com/spaceqraft/vitaledge/internal/cypher/indexschema"
	"github.com/spaceqraft/vitaledge/internal/graph"
	pebblestore "github.com/spaceqraft/vitaledge/internal/graph/store/pebble"
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
const goMemoryLimitBytesEnv = "VITALEDGE_GO_MEMORY_LIMIT_BYTES"
const pebbleBlockCacheBytesEnv = "VITALEDGE_PEBBLE_BLOCK_CACHE_BYTES"
const pebbleMemTableSizeBytesEnv = "VITALEDGE_PEBBLE_MEMTABLE_SIZE_BYTES"
const pebbleMemTableStopWritesThresholdEnv = "VITALEDGE_PEBBLE_MEMTABLE_STOP_WRITES_THRESHOLD"

type Config struct {
	GraphPath                         string
	DefaultTenant                     string
	IndexConfigPath                   string
	IndexCatalog                      *indexschema.Catalog
	ExecutorMetrics                   *executor.Collector
	MetricsListenAddress              string
	GRPCListenAddress                 string
	ConfiguredMaxWriteBatchBytes      int
	MaxWriteBatchBytes                int
	MaxWriteBatchBytesAutoTuned       bool
	HostMemoryBytes                   uint64
	GoMemoryLimitBytes                int64
	PebbleBlockCacheBytes             int64
	PebbleMemTableSizeBytes           int
	PebbleMemTableStopWritesThreshold int
	Store                             graph.GraphStore
	Executor                          *executor.Executor
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
	grpcServer, grpcListener, err := startGRPCServer(s.GRPCListenAddress,
		&grpcDdlHandler{
			executor:      s.executor,
			defaultTenant: s.DefaultTenant,
		},
		&grpcDmlHandler{
			executor:                     s.executor,
			defaultTenant:                s.DefaultTenant,
			configuredMaxWriteBatchBytes: int64(s.ConfiguredMaxWriteBatchBytes),
			maxWriteBatchBytes:           int64(s.MaxWriteBatchBytes),
			maxWriteBatchBytesTuned:      s.MaxWriteBatchBytesAutoTuned,
		})
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
	if config.MaxWriteBatchBytesAutoTuned {
		direction := "up"
		if config.MaxWriteBatchBytes < config.ConfiguredMaxWriteBatchBytes {
			direction = "down"
		}
		log.Printf("WARNING: auto-tuned max write batch bytes %s from %d to %d based on host memory %d bytes", direction, config.ConfiguredMaxWriteBatchBytes, config.MaxWriteBatchBytes, config.HostMemoryBytes)
	}
	if config.GoMemoryLimitBytes > 0 {
		previous := debug.SetMemoryLimit(config.GoMemoryLimitBytes)
		log.Printf("Configured Go runtime memory limit: %d bytes (previous=%d)", config.GoMemoryLimitBytes, previous)
	}

	store, err := openGraphStore(
		config.GraphPath,
		config.MaxWriteBatchBytes,
		config.PebbleBlockCacheBytes,
		config.PebbleMemTableSizeBytes,
		config.PebbleMemTableStopWritesThreshold,
	)
	if err != nil {
		log.Fatalf("startup graph store error: %v", err)
	}
	config.Store = store
	config.Executor = executor.New(store, executor.Options{
		Metrics:      config.ExecutorMetrics,
		IndexCatalog: config.IndexCatalog,
	})
	if err := applyIndexMigrations(context.Background(), config.Executor, config.IndexCatalog); err != nil {
		log.Fatalf("startup index migration error: %v", err)
	}
	config.Executor.StartIndexBuildWorker(context.Background())

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
	var goMemoryLimitBytes int64
	var pebbleBlockCacheBytes int64
	var pebbleMemTableSizeBytes int
	var pebbleMemTableStopWritesThreshold int

	flag.StringVar(&graphPath, "graph-path", defaultGraphPath, "Path to graph store directory")
	flag.StringVar(&tenant, "tenant", defaultTenant, "Default tenant used for query execution")
	flag.StringVar(&indexConfigPath, "index-schema-config", "", "Path to index schema configuration JSON file")
	flag.StringVar(&metricsListenAddress, "metrics-listen", "", "Optional HTTP listen address for Prometheus metrics endpoint (for example :9100)")
	flag.StringVar(&grpcListenAddress, "grpc-listen", defaultGRPCListenAddress, "gRPC listen address for QueryService")
	flag.IntVar(&maxWriteBatchBytes, "max-write-batch-bytes", 0, "Maximum bytes allowed in a single write transaction batch (0 auto-tunes from host memory)")
	flag.Int64Var(&goMemoryLimitBytes, "go-memory-limit-bytes", 0, "Optional Go runtime soft memory limit in bytes (0 disables)")
	flag.Int64Var(&pebbleBlockCacheBytes, "pebble-block-cache-bytes", 0, "Optional Pebble block cache size in bytes (0 uses Pebble defaults)")
	flag.IntVar(&pebbleMemTableSizeBytes, "pebble-memtable-size-bytes", 0, "Optional Pebble memtable size in bytes (0 uses Pebble defaults)")
	flag.IntVar(&pebbleMemTableStopWritesThreshold, "pebble-memtable-stop-writes-threshold", 0, "Optional Pebble memtable stop-writes threshold (0 uses Pebble defaults)")
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
	if env := strings.TrimSpace(os.Getenv(goMemoryLimitBytesEnv)); env != "" {
		parsed, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			return Config{}, err
		}
		goMemoryLimitBytes = parsed
	}
	if env := strings.TrimSpace(os.Getenv(pebbleBlockCacheBytesEnv)); env != "" {
		parsed, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			return Config{}, err
		}
		pebbleBlockCacheBytes = parsed
	}
	if env := strings.TrimSpace(os.Getenv(pebbleMemTableSizeBytesEnv)); env != "" {
		parsed, err := strconv.Atoi(env)
		if err != nil {
			return Config{}, err
		}
		pebbleMemTableSizeBytes = parsed
	}
	if env := strings.TrimSpace(os.Getenv(pebbleMemTableStopWritesThresholdEnv)); env != "" {
		parsed, err := strconv.Atoi(env)
		if err != nil {
			return Config{}, err
		}
		pebbleMemTableStopWritesThreshold = parsed
	}
	configuredMaxWriteBatchBytes, effectiveMaxWriteBatchBytes, autoTuned, hostMemoryBytes, err := resolveMaxWriteBatchBytes(maxWriteBatchBytes)
	if err != nil {
		return Config{}, err
	}
	if goMemoryLimitBytes < 0 {
		return Config{}, fmt.Errorf("go memory limit bytes must be >= 0")
	}
	if pebbleBlockCacheBytes < 0 {
		return Config{}, fmt.Errorf("pebble block cache bytes must be >= 0")
	}
	if pebbleMemTableSizeBytes < 0 {
		return Config{}, fmt.Errorf("pebble memtable size bytes must be >= 0")
	}
	if pebbleMemTableStopWritesThreshold < 0 {
		return Config{}, fmt.Errorf("pebble memtable stop writes threshold must be >= 0")
	}

	cfg := Config{
		GraphPath:                         graphPath,
		DefaultTenant:                     tenant,
		IndexConfigPath:                   indexConfigPath,
		IndexCatalog:                      indexschema.NewCatalog(),
		MetricsListenAddress:              metricsListenAddress,
		GRPCListenAddress:                 grpcListenAddress,
		ConfiguredMaxWriteBatchBytes:      configuredMaxWriteBatchBytes,
		MaxWriteBatchBytes:                effectiveMaxWriteBatchBytes,
		MaxWriteBatchBytesAutoTuned:       autoTuned,
		HostMemoryBytes:                   hostMemoryBytes,
		GoMemoryLimitBytes:                goMemoryLimitBytes,
		PebbleBlockCacheBytes:             pebbleBlockCacheBytes,
		PebbleMemTableSizeBytes:           pebbleMemTableSizeBytes,
		PebbleMemTableStopWritesThreshold: pebbleMemTableStopWritesThreshold,
		ExecutorMetrics:                   executor.NewCollector(),
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

func openGraphStore(path string, maxWriteBatchBytes int, pebbleBlockCacheBytes int64, pebbleMemTableSizeBytes int, pebbleMemTableStopWritesThreshold int) (graph.GraphStore, error) {
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
	if pebbleBlockCacheBytes < 0 {
		return nil, fmt.Errorf("pebble block cache bytes must be >= 0")
	}
	if pebbleMemTableSizeBytes < 0 {
		return nil, fmt.Errorf("pebble memtable size bytes must be >= 0")
	}
	if pebbleMemTableStopWritesThreshold < 0 {
		return nil, fmt.Errorf("pebble memtable stop writes threshold must be >= 0")
	}
	return pebblestore.OpenWithOptions(path, pebblestore.StoreOptions{
		MaxWriteBatchBytes:                maxWriteBatchBytes,
		PebbleBlockCacheBytes:             pebbleBlockCacheBytes,
		PebbleMemTableSizeBytes:           pebbleMemTableSizeBytes,
		PebbleMemTableStopWritesThreshold: pebbleMemTableStopWritesThreshold,
	})
}

func applyIndexMigrations(ctx context.Context, exec *executor.Executor, catalog *indexschema.Catalog) error {
	if exec == nil || catalog == nil {
		return nil
	}
	vertexBackfilled := 0
	edgeBackfilled := 0
	for _, idx := range catalog.PropertyIndexes() {
		count, err := exec.BackfillPropertyIndex(ctx, idx.Tenant, idx.Schema, idx.Property)
		if err != nil {
			return err
		}
		vertexBackfilled += count
	}
	for _, idx := range catalog.EdgePropertyIndexes() {
		count, err := exec.BackfillEdgePropertyIndex(ctx, idx.Tenant, idx.EdgeType, idx.Property)
		if err != nil {
			return err
		}
		edgeBackfilled += count
	}
	if vertexBackfilled > 0 || edgeBackfilled > 0 {
		log.Printf("Applied index migration backfill: vertex_entries=%d edge_entries=%d", vertexBackfilled, edgeBackfilled)
	}
	return nil
}
