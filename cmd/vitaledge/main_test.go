package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func TestTCPQueryExecutionLargeMultilineCreate(t *testing.T) {
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

	var b strings.Builder
	b.WriteString("CREATE\n")
	for i := 0; i < 80; i++ {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, "  (p%d:Person:Actor {name: 'Actor %d'})", i, i)
	}

	response := sendTCPQuery(t, conn, b.String())
	if !response.OK {
		t.Fatalf("expected ok response, got error: %s", response.Error)
	}
}

func TestTCPQueryExecutionSlowManualMultilineCreateThenMatch(t *testing.T) {
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

	createLines := []string{
		"CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),",
		"       (martin:Person:Actor {name: 'Martin Sheen'}),",
		"       (michael:Person:Actor {name: 'Michael Douglas'}),",
		"       (oliver:Person:Director {name: 'Oliver Stone'}),",
		"       (rob:Person:Director {name: 'Rob Reiner'}),",
		"       (wallStreet:Movie {title: 'Wall Street'}),",
		"       (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),",
		"       (martin)-[:ACTED_IN {role: 'Carl Fox'}]->(wallStreet),",
		"       (michael)-[:ACTED_IN {role: 'Gordon Gekko'}]->(wallStreet),",
		"       (oliver)-[:DIRECTED]->(wallStreet),",
		"       (thePresident:Movie {title: 'The American President'}),",
		"       (martin)-[:ACTED_IN {role: 'A.J. MacInerney'}]->(thePresident),",
		"       (michael)-[:ACTED_IN {role: 'President Andrew Shepherd'}]->(thePresident),",
		"       (rob)-[:DIRECTED]->(thePresident)",
	}

	createResponse := sendTCPQueryLineByLine(t, conn, createLines, 350*time.Millisecond)
	if !createResponse.OK {
		t.Fatalf("expected create ok response, got error: %s", createResponse.Error)
	}
	charlie, ok := createResponse.Rows[0]["charlie"].(map[string]any)
	if !ok {
		t.Fatalf("expected create binding charlie as map, got %T", createResponse.Rows[0]["charlie"])
	}
	charlieProps, ok := charlie["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected charlie properties map, got %T", charlie["properties"])
	}
	if got := stringPropertyValue(charlieProps, "name"); got != "Charlie Sheen" {
		t.Fatalf("expected plain string Charlie Sheen in create response, got %#v", charlieProps["name"])
	}

	matchResponse := sendTCPQuery(t, conn, "MATCH (n) RETURN n")
	if !matchResponse.OK {
		t.Fatalf("expected match ok response, got error: %s", matchResponse.Error)
	}

	names := make([]string, 0, len(matchResponse.Rows))
	for _, row := range matchResponse.Rows {
		node, ok := row["n"].(map[string]any)
		if !ok {
			t.Fatalf("expected projected node map, got %T", row["n"])
		}
		props, ok := node["properties"].(map[string]any)
		if !ok {
			continue
		}
		if name := stringPropertyValue(props, "name"); name != "" {
			names = append(names, name)
		}
		if title := stringPropertyValue(props, "title"); title != "" {
			names = append(names, title)
		}
	}
	sort.Strings(names)
	for _, expected := range []string{
		"Charlie Sheen",
		"Martin Sheen",
		"Michael Douglas",
		"Oliver Stone",
		"Rob Reiner",
		"The American President",
		"Wall Street",
	} {
		if !containsString(names, expected) {
			t.Fatalf("expected MATCH readback to contain %q; got %v", expected, names)
		}
	}
}

func TestTCPQueryExecutionSlowMultilineMatchLabelAlternation(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(context.Background(), func(tx graph.Tx) error {
		if err := tx.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Charlie Sheen")}}); err != nil {
			return err
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

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

	response := sendTCPQueryLineByLine(t, conn, []string{
		"MATCH (n:Movie|Person)",
		"RETURN n.name AS name, n.title AS title",
	}, 350*time.Millisecond)
	if !response.OK {
		t.Fatalf("expected ok response, got error: %s", response.Error)
	}
	if len(response.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(response.Rows))
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

func sendTCPQueryLineByLine(t *testing.T, conn net.Conn, lines []string, delay time.Duration) wireResponse {
	t.Helper()

	for _, line := range lines {
		if _, err := conn.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		time.Sleep(delay)
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

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func stringPropertyValue(props map[string]any, key string) string {
	raw, ok := props[key]
	if !ok {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return value
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
