package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

func TestTCPQueryExecutionSuccess(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	server := NewServer(Config{
		DefaultTenant:         "acme",
		Executor:              exec,
		ExecutorMetrics:       executor.NewCollector(),
		MetricsReportInterval: 0,
	})
	defer close(server.quitCh)
	go server.loop()

	conn := openServerPeerConnection(t, server)
	defer conn.Close()

	response := sendTCPQuery(t, conn, "UNWIND [1,2] AS n RETURN n AS value")
	if !response.OK {
		t.Fatalf("expected ok response, got error: %s", response.Error)
	}
	if len(response.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(response.Rows))
	}
	if got := response.Rows[0]["value"]; got != float64(1) {
		t.Fatalf("unexpected first row value: %#v", got)
	}
	if got := response.Rows[1]["value"]; got != float64(2) {
		t.Fatalf("unexpected second row value: %#v", got)
	}
}

func TestTCPQueryExecutionParseError(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	server := NewServer(Config{
		DefaultTenant:         "acme",
		Executor:              exec,
		ExecutorMetrics:       executor.NewCollector(),
		MetricsReportInterval: 0,
	})
	defer close(server.quitCh)
	go server.loop()

	conn := openServerPeerConnection(t, server)
	defer conn.Close()

	response := sendTCPQuery(t, conn, "MATCH (n RETURN n")
	if response.OK {
		t.Fatalf("expected error response")
	}
	if response.Error == "" {
		t.Fatalf("expected non-empty error message")
	}
}

func openServerPeerConnection(t *testing.T, server *Server) net.Conn {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	select {
	case serverConn := <-accepted:
		go server.handle(serverConn)
	case <-time.After(2 * time.Second):
		_ = clientConn.Close()
		t.Fatalf("timed out waiting for accepted connection")
	}

	return clientConn
}

func sendTCPQuery(t *testing.T, conn net.Conn, query string) wireResponse {
	t.Helper()

	if _, err := conn.Write([]byte(query + "\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline failed: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	var response wireResponse
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatalf("unmarshal response failed: %v; payload=%q", err, string(line))
	}
	return response
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
