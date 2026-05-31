package main

import (
	"context"
	"flag"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/paegun/vitaledge/api/proto/vitaledge/v1"
	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestNewServerDefaultsGRPCListenAddress(t *testing.T) {
	server := NewServer(Config{})
	if server.GRPCListenAddress != defaultGRPCListenAddress {
		t.Fatalf("expected default gRPC listen %q, got %q", defaultGRPCListenAddress, server.GRPCListenAddress)
	}
}

func openTestStore(t *testing.T) graph.GraphStore {
	t.Helper()

	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := pebblestore.Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}

	if err := store.Update(context.Background(), func(tx graph.Tx) error {
		return tx.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "seed", Labels: []string{"Seed"}})
	}); err != nil {
		t.Fatalf("seed store failed: %v", err)
	}

	return store
}

func TestWritePrometheusMetricsFromCollector(t *testing.T) {
	collector := executor.NewCollector()
	collector.ObserveStatement(ast.StatementKindQuery, "ok", 25*time.Millisecond)
	collector.ObserveRowsReturned(3)
	collector.ObserveIndexCandidate("acme", "User", "email", false)
	collector.ObserveIndexLookup("property_index", "hit", 2)

	recorder := httptest.NewRecorder()
	writePrometheusMetrics(recorder, collector)

	body := recorder.Body.String()
	expectedSubstrings := []string{
		"# HELP vitaledge_go_goroutines",
		"# HELP vitaledge_go_gc_cycles_total",
		"# HELP vitaledge_host_cpu_seconds_total",
		"# HELP vitaledge_host_memory_total_bytes",
		"# HELP vitaledge_host_network_receive_bytes_total",
		"# HELP vitaledge_executor_statements_total",
		"vitaledge_executor_statements_total{kind=\"QUERY\",outcome=\"ok\"} 1",
		"vitaledge_executor_statement_duration_seconds_total{kind=\"QUERY\",outcome=\"ok\"}",
		"# TYPE vitaledge_executor_statement_duration_seconds histogram",
		"vitaledge_executor_statement_duration_seconds_bucket{kind=\"QUERY\",outcome=\"ok\",le=\"0.025\"} 1",
		"vitaledge_executor_statement_duration_seconds_bucket{kind=\"QUERY\",outcome=\"ok\",le=\"+Inf\"} 1",
		"vitaledge_executor_statement_duration_seconds_sum{kind=\"QUERY\",outcome=\"ok\"}",
		"vitaledge_executor_statement_duration_seconds_count{kind=\"QUERY\",outcome=\"ok\"} 1",
		"vitaledge_executor_rows_returned_total 3",
		"vitaledge_executor_index_candidates_total{tenant=\"acme\",schema=\"User\",property=\"email\",indexed=\"false\"} 1",
		"vitaledge_executor_unindexed_candidate_observations{tenant=\"acme\",schema=\"User\",property=\"email\"} 1",
		"vitaledge_executor_index_lookups_total{strategy=\"property_index\",outcome=\"hit\"} 1",
		"vitaledge_executor_index_lookup_matches_total{strategy=\"property_index\",outcome=\"hit\"} 2",
	}
	for _, expected := range expectedSubstrings {
		if !strings.Contains(body, expected) {
			t.Fatalf("expected metrics output to contain %q; body=%s", expected, body)
		}
	}
}

func TestWritePrometheusMetricsWithNilCollector(t *testing.T) {
	recorder := httptest.NewRecorder()
	writePrometheusMetrics(recorder, nil)

	body := recorder.Body.String()
	if !strings.Contains(body, "vitaledge_executor_rows_returned_total 0") {
		t.Fatalf("expected rows counter line in nil-collector output, got: %s", body)
	}
}

func TestGRPCQueryServiceExecuteAndCapabilities(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0", &grpcQueryHandler{executor: exec, defaultTenant: "acme"})
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewQueryServiceClient(conn)
	capResp, err := client.GetCapabilities(ctx, &v1.CapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}
	if capResp.GetProtocolVersion() != "v1" {
		t.Fatalf("unexpected protocolVersion: %#v", capResp.GetProtocolVersion())
	}
	if capResp.GetParameterBinding() != "server_side" {
		t.Fatalf("unexpected parameterBinding: %#v", capResp.GetParameterBinding())
	}
	if !capResp.GetPreparedQuerySupported() {
		t.Fatalf("expected prepared query support to be true")
	}
	if !capResp.GetIndexDdlSupported() {
		t.Fatalf("expected index DDL support to be true")
	}
	if capResp.GetMaxWriteBatchBytes() != int64(pebblestore.DefaultMaxWriteBatchBytes) {
		t.Fatalf("unexpected max_write_batch_bytes capability: %#v", capResp.GetMaxWriteBatchBytes())
	}

	execResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (n:Seed) RETURN n.id AS id"}},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(execResp.GetColumns()) != 1 || execResp.GetColumns()[0] != "id" {
		t.Fatalf("unexpected columns: %#v", execResp.GetColumns())
	}
	if len(execResp.GetRows()) != 1 {
		t.Fatalf("unexpected rows: %#v", execResp.GetRows())
	}
	rowValue := execResp.GetRows()[0].GetValues()["id"]
	if rowValue == nil {
		t.Fatalf("missing row value for id")
	}
	if rowValue.GetStringValue() != "seed" {
		t.Fatalf("unexpected row value: %#v", rowValue)
	}

	preparedResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: &v1.PreparedQuery{
			ParserVersion: "cypher-m23",
			IrVersion:     "query-pipeline-v1",
			Payload:       []byte("MATCH (n:Seed) RETURN n.id AS id"),
		}}},
	})
	if err != nil {
		t.Fatalf("prepared Execute failed: %v", err)
	}
	if len(preparedResp.GetRows()) != 1 {
		t.Fatalf("unexpected prepared rows: %#v", preparedResp.GetRows())
	}
	preparedRowValue := preparedResp.GetRows()[0].GetValues()["id"]
	if preparedRowValue == nil || preparedRowValue.GetStringValue() != "seed" {
		t.Fatalf("unexpected prepared row value: %#v", preparedRowValue)
	}

	_, err = client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: &v1.PreparedQuery{
			ParserVersion: "cypher-m99",
			IrVersion:     "query-pipeline-v99",
			Payload:       []byte("MATCH (n:Seed) RETURN n.id AS id"),
		}}},
	})
	if err == nil {
		t.Fatalf("expected version mismatch error for prepared query without fallback")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}

	fallbackResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: &v1.PreparedQuery{
			ParserVersion:  "cypher-m99",
			IrVersion:      "query-pipeline-v99",
			Payload:        []byte("MATCH (n:Never) RETURN n.id AS id"),
			FallbackCypher: "MATCH (n:Seed) RETURN n.id AS id",
		}}},
		Options: &v1.RequestOptions{AllowFallbackToCypher: true},
	})
	if err != nil {
		t.Fatalf("expected fallback execution to succeed, got: %v", err)
	}
	fallbackRowValue := fallbackResp.GetRows()[0].GetValues()["id"]
	if fallbackRowValue == nil || fallbackRowValue.GetStringValue() != "seed" {
		t.Fatalf("unexpected fallback row value: %#v", fallbackRowValue)
	}
}

func TestGRPCQueryServiceCapabilitiesReflectConfiguredMaxWriteBatch(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	const configuredMaxWriteBatchBytes = 123456
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0", &grpcQueryHandler{
		executor:           exec,
		defaultTenant:      "acme",
		maxWriteBatchBytes: configuredMaxWriteBatchBytes,
	})
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewQueryServiceClient(conn)
	capResp, err := client.GetCapabilities(ctx, &v1.CapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}
	if capResp.GetMaxWriteBatchBytes() != configuredMaxWriteBatchBytes {
		t.Fatalf("unexpected max_write_batch_bytes capability: %#v", capResp.GetMaxWriteBatchBytes())
	}
}

func TestOpenGraphStoreRejectsNonPositiveBatchLimit(t *testing.T) {
	_, err := openGraphStore(t.TempDir(), 0, 0, 0, 0)
	if err == nil {
		t.Fatalf("expected error when max write batch bytes is non-positive")
	}
}

func TestOpenGraphStoreAcceptsConfiguredBatchLimit(t *testing.T) {
	store, err := openGraphStore(t.TempDir(), 1024, 0, 0, 0)
	if err != nil {
		t.Fatalf("openGraphStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.Update(context.Background(), func(tx graph.Tx) error {
		return tx.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "ok"})
	})
	if err != nil {
		t.Fatalf("expected write under limit to succeed, got: %v", err)
	}
}

func TestLoadConfigFromStartupParsesGoMemoryLimitFlag(t *testing.T) {
	cfg, err := loadConfigFromStartupForTest(t, []string{"vitaledge-test", "-go-memory-limit-bytes", "1048576"})
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got, want := cfg.GoMemoryLimitBytes, int64(1048576); got != want {
		t.Fatalf("expected GoMemoryLimitBytes=%d, got %d", want, got)
	}
}

func TestLoadConfigFromStartupParsesGoMemoryLimitEnv(t *testing.T) {
	t.Setenv(goMemoryLimitBytesEnv, "2097152")

	cfg, err := loadConfigFromStartupForTest(t, []string{"vitaledge-test"})
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got, want := cfg.GoMemoryLimitBytes, int64(2097152); got != want {
		t.Fatalf("expected GoMemoryLimitBytes=%d, got %d", want, got)
	}
}

func TestLoadConfigFromStartupRejectsNegativeGoMemoryLimit(t *testing.T) {
	t.Setenv(goMemoryLimitBytesEnv, "-1")

	_, err := loadConfigFromStartupForTest(t, []string{"vitaledge-test"})
	if err == nil {
		t.Fatalf("expected negative go memory limit to fail")
	}
	if !strings.Contains(err.Error(), "go memory limit bytes must be >= 0") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func loadConfigFromStartupForTest(t *testing.T, args []string) (Config, error) {
	t.Helper()

	oldArgs := os.Args
	oldCommandLine := flag.CommandLine

	if len(args) == 0 {
		args = []string{"vitaledge-test"}
	}
	os.Args = args

	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	flag.CommandLine = fs

	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	})

	return loadConfigFromStartup()
}

func TestGRPCQueryServiceCreatePropertyIndex(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(context.Background(), func(tx graph.Tx) error {
		seed := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: graph.PropertyMap{"email": []byte("alice@example.com")}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: graph.PropertyMap{"email": []byte("bob@example.com")}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Device"}, Properties: graph.PropertyMap{"email": []byte("ignored@example.com")}},
		}
		for _, vertex := range seed {
			if err := tx.PutVertex(context.Background(), vertex); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed store failed: %v", err)
	}

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector(), IndexCatalog: indexschema.NewCatalog()})
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0", &grpcQueryHandler{executor: exec, defaultTenant: "acme"})
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewQueryServiceClient(conn)
	createResp, err := client.CreatePropertyIndex(ctx, &v1.CreatePropertyIndexRequest{
		Tenant:      "acme",
		Schema:      "User",
		Property:    "email",
		IfNotExists: false,
	})
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}
	if !createResp.GetCreated() {
		t.Fatalf("expected created=true")
	}
	if createResp.GetIndexedEntities() != 2 {
		t.Fatalf("expected indexed_entities=2, got %d", createResp.GetIndexedEntities())
	}

	if err := store.View(context.Background(), func(tx graph.Tx) error {
		ids := map[string]struct{}{}
		err := tx.ScanPropertyIndex(ctx, "acme", "User", "email", []byte("alice@example.com"), 0, func(entry *graph.PropertyIndexEntry) error {
			ids[entry.EntityID] = struct{}{}
			return nil
		})
		if err != nil {
			return err
		}
		if _, ok := ids["u1"]; !ok {
			t.Fatalf("expected u1 to be indexed for alice@example.com")
		}
		if len(ids) != 1 {
			t.Fatalf("expected one indexed entity for alice@example.com, got %d", len(ids))
		}
		return nil
	}); err != nil {
		t.Fatalf("verify property index failed: %v", err)
	}

	idempotentResp, err := client.CreatePropertyIndex(ctx, &v1.CreatePropertyIndexRequest{
		Tenant:      "acme",
		Schema:      "User",
		Property:    "email",
		IfNotExists: true,
	})
	if err != nil {
		t.Fatalf("CreatePropertyIndex IF NOT EXISTS failed: %v", err)
	}
	if idempotentResp.GetCreated() {
		t.Fatalf("expected created=false when index already exists")
	}

	_, err = client.CreatePropertyIndex(ctx, &v1.CreatePropertyIndexRequest{
		Tenant:      "acme",
		Schema:      "User",
		Property:    "email",
		IfNotExists: false,
	})
	if err == nil {
		t.Fatalf("expected duplicate create without IF NOT EXISTS to fail")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists for duplicate index create, got %v", err)
	}
}
