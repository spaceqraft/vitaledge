package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/cypher/indexschema"
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
	if len(createResponse.Rows) > 0 {
		if charlie, ok := createResponse.Rows[0]["charlie"].(map[string]any); ok {
			if charlieProps, ok := charlie["properties"].(map[string]any); ok {
				if got := stringPropertyValue(charlieProps, "name"); got != "Charlie Sheen" {
					t.Fatalf("expected plain string Charlie Sheen in create response, got %#v", charlieProps["name"])
				}
			}
		}
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

func TestTCPExplainQueryReturnsExplainPayload(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(context.Background(), func(tx graph.Tx) error {
		if err := tx.PutVertex(context.Background(), &graph.Vertex{
			Tenant:     "acme",
			ID:         "p-neo",
			Labels:     []string{"Person"},
			Properties: graph.PropertyMap{"name": []byte("Neo")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(context.Background(), &graph.Vertex{
			Tenant:     "acme",
			ID:         "m-matrix",
			Labels:     []string{"Movie"},
			Properties: graph.PropertyMap{"title": []byte("The Matrix")},
		}); err != nil {
			return err
		}
		if err := tx.PutEdge(context.Background(), &graph.Edge{
			Tenant: "acme",
			ID:     "e-1",
			Type:   "ACTED_IN",
			SrcID:  "p-neo",
			DstID:  "m-matrix",
		}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(context.Background(), &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "Person",
			Property:    "name",
			Value:       []byte("Neo"),
			EntityID:    "p-neo",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Person", "name")
	server := NewServer(Config{
		DefaultTenant:         "acme",
		Executor:              executor.New(store, executor.Options{Metrics: executor.NewCollector(), IndexCatalog: catalog}),
		ExecutorMetrics:       executor.NewCollector(),
		MetricsReportInterval: 0,
	})
	defer close(server.quitCh)
	go server.loop()

	conn := openServerPeerConnection(t, server)
	defer conn.Close()

	response := sendTCPQuery(t, conn, "EXPLAIN MATCH (n:Person {name: $name}) WITH n.name AS alias ORDER BY alias DESC SKIP 1 LIMIT 2 RETURN DISTINCT alias AS name ORDER BY name ASC LIMIT $maxLimit")
	if !response.OK {
		t.Fatalf("expected ok response, got error: %s", response.Error)
	}
	if len(response.Columns) != 1 || response.Columns[0] != "explain" {
		t.Fatalf("unexpected columns: %#v", response.Columns)
	}
	if len(response.Rows) != 1 {
		t.Fatalf("expected one explain row, got %d", len(response.Rows))
	}

	explain, ok := response.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", response.Rows[0]["explain"])
	}
	query, ok := explain["query"].(map[string]any)
	if !ok {
		t.Fatalf("expected query map, got %T", explain["query"])
	}
	if query["statementKind"] != "QUERY" {
		t.Fatalf("unexpected statementKind: %#v", query["statementKind"])
	}
	options, ok := query["options"].(map[string]any)
	if !ok {
		t.Fatalf("expected query.options map, got %T", query["options"])
	}
	if distinct, _ := options["distinct"].(bool); !distinct {
		t.Fatalf("expected distinct option to be true")
	}
	if _, ok := options["skip"]; ok {
		t.Fatalf("did not expect a top-level skip option from the final RETURN clause, got %#v", options["skip"])
	}
	if limit, _ := options["limit"].(string); limit != "$maxLimit" {
		t.Fatalf("expected limit option to preserve parameter reference, got %#v", options["limit"])
	}
	if orderBy, _ := options["orderBy"].([]any); len(orderBy) != 1 || orderBy[0] != "name ASC" {
		t.Fatalf("unexpected orderBy option: %#v", options["orderBy"])
	}
	projectionClauses, ok := options["projectionClauses"].([]any)
	if !ok {
		t.Fatalf("expected query.options.projectionClauses array, got %T", options["projectionClauses"])
	}
	if len(projectionClauses) != 2 {
		t.Fatalf("expected 2 projection clauses, got %#v", options["projectionClauses"])
	}
	withClause, ok := projectionClauses[0].(map[string]any)
	if !ok {
		t.Fatalf("expected WITH clause map, got %T", projectionClauses[0])
	}
	if withClause["kind"] != "WITH" {
		t.Fatalf("expected first projection clause to be WITH, got %#v", withClause["kind"])
	}
	if withProjection, _ := withClause["projection"].([]any); len(withProjection) != 1 || withProjection[0] != "n.name AS alias" {
		t.Fatalf("unexpected WITH projection: %#v", withClause["projection"])
	}
	if withOrderBy, _ := withClause["orderBy"].([]any); len(withOrderBy) != 1 || withOrderBy[0] != "alias DESC" {
		t.Fatalf("unexpected WITH orderBy: %#v", withClause["orderBy"])
	}
	returnClause, ok := projectionClauses[1].(map[string]any)
	if !ok {
		t.Fatalf("expected RETURN clause map, got %T", projectionClauses[1])
	}
	if returnClause["kind"] != "RETURN" {
		t.Fatalf("expected second projection clause to be RETURN, got %#v", returnClause["kind"])
	}
	if returnProjection, _ := returnClause["projection"].([]any); len(returnProjection) != 1 || returnProjection[0] != "alias AS name" {
		t.Fatalf("unexpected RETURN projection: %#v", returnClause["projection"])
	}

	influencers, ok := explain["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explain["influencers"])
	}
	logicalPlan, ok := explain["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explain["logicalPlan"])
	}
	nodes, ok := logicalPlan["nodes"].([]any)
	if !ok || len(nodes) == 0 {
		t.Fatalf("expected logicalPlan.nodes to be populated, got %#v", logicalPlan["nodes"])
	}
	firstNode, ok := nodes[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first logical node map, got %T", nodes[0])
	}
	firstOp, _ := firstNode["op"].(string)
	if firstOp != "INDEX_SCAN" && firstOp != "LABEL_SCAN" && firstOp != "ALL_NODES_SCAN" && firstOp != "OPTIONAL_INDEX_SCAN" && firstOp != "OPTIONAL_LABEL_SCAN" && firstOp != "OPTIONAL_ALL_NODES_SCAN" {
		t.Fatalf("expected first logical node to be a scan-family operator, got %#v", firstNode["op"])
	}
	if accessPath, _ := firstNode["accessPath"].(string); accessPath == "" {
		t.Fatalf("expected first logical node accessPath, got %#v", firstNode["accessPath"])
	}
	if nodeCounts, ok := influencers["nodeCounts"].([]any); !ok || len(nodeCounts) == 0 {
		t.Fatalf("expected nodeCounts to be populated, got %#v", influencers["nodeCounts"])
	}
	if edgeCounts, ok := influencers["edgeCounts"].([]any); !ok || len(edgeCounts) == 0 {
		t.Fatalf("expected edgeCounts to be populated, got %#v", influencers["edgeCounts"])
	}
	indexDecisions, ok := explain["indexDecisions"].([]any)
	if !ok || len(indexDecisions) == 0 {
		t.Fatalf("expected indexDecisions to be populated, got %#v", explain["indexDecisions"])
	}
	firstDecision, ok := indexDecisions[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first index decision map, got %T", indexDecisions[0])
	}
	if recommendation, _ := firstDecision["recommendation"].(string); recommendation == "" {
		t.Fatalf("expected index decision recommendation, got %#v", firstDecision["recommendation"])
	}
	if quality, _ := firstDecision["quality"].(string); quality != "exact" && quality != "estimate" {
		t.Fatalf("expected index decision quality exact or estimate, got %#v", firstDecision["quality"])
	}
	if cardinality, ok := explain["cardinality"].([]any); !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality to be populated, got %#v", explain["cardinality"])
	}
	costEstimate, ok := explain["costEstimate"].(map[string]any)
	if !ok {
		t.Fatalf("expected costEstimate map, got %T", explain["costEstimate"])
	}
	if unit, _ := costEstimate["unit"].(string); unit != "work_units" {
		t.Fatalf("expected costEstimate unit work_units, got %#v", costEstimate["unit"])
	}
	if quality, _ := costEstimate["quality"].(string); quality != "estimate" {
		t.Fatalf("expected costEstimate quality estimate, got %#v", costEstimate["quality"])
	}
	runtimeStats, ok := explain["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explain["runtimeStats"])
	}
	if _, ok := runtimeStats["store"].(map[string]any); !ok {
		t.Fatalf("expected runtimeStats.store map, got %#v", runtimeStats["store"])
	}
	if _, ok := runtimeStats["plan"].(map[string]any); !ok {
		t.Fatalf("expected runtimeStats.plan map, got %#v", runtimeStats["plan"])
	}
	if _, ok := runtimeStats["index"].(map[string]any); !ok {
		t.Fatalf("expected runtimeStats.index map, got %#v", runtimeStats["index"])
	}
	if cardinalityStats, ok := runtimeStats["cardinality"].(map[string]any); !ok {
		t.Fatalf("expected runtimeStats.cardinality map, got %#v", runtimeStats["cardinality"])
	} else if quality, _ := cardinalityStats["quality"].(string); quality != "estimate" {
		t.Fatalf("expected runtimeStats.cardinality quality estimate, got %#v", cardinalityStats["quality"])
	}
	warnings, ok := explain["warnings"].([]any)
	if !ok || len(warnings) == 0 {
		t.Fatalf("expected warnings to be populated, got %#v", explain["warnings"])
	}
	foundWarningCode := false
	for _, warning := range warnings {
		entry, ok := warning.(map[string]any)
		if !ok {
			continue
		}
		code, _ := entry["code"].(string)
		if code == "PLAN_ANALYSIS_PARTIAL" || code == "ESTIMATE_ONLY_INDEX_SIGNAL" || code == "MISSING_PROPERTY_INDEX" || code == "FULL_SCAN_FALLBACK" {
			foundWarningCode = true
			break
		}
	}
	if !foundWarningCode {
		t.Fatalf("expected at least one fallback warning code, got %#v", warnings)
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
		"# HELP vitaledge_executor_statements_total",
		"vitaledge_executor_statements_total{kind=\"QUERY\",outcome=\"ok\"} 1",
		"vitaledge_executor_statement_duration_seconds_total{kind=\"QUERY\",outcome=\"ok\"}",
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
