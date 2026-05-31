package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"

	v1 "github.com/paegun/vitaledge/api/proto/vitaledge/v1"
	"github.com/paegun/vitaledge/internal/cypher"
	"google.golang.org/grpc"
)

// stubQueryServiceClient is a minimal QueryServiceClient for use in unit tests.
// Each Execute call invokes executeFunc; all other methods are no-ops.
type stubQueryServiceClient struct {
	executeFunc func(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.QueryResponse, error)
	explainFunc func(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.ExplainResponse, error)
}

func (s *stubQueryServiceClient) Execute(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.QueryResponse, error) {
	if s.executeFunc != nil {
		return s.executeFunc(ctx, in, opts...)
	}
	return &v1.QueryResponse{}, nil
}

func (s *stubQueryServiceClient) Explain(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.ExplainResponse, error) {
	if s.explainFunc != nil {
		return s.explainFunc(ctx, in, opts...)
	}
	return &v1.ExplainResponse{}, nil
}

func TestRunExplainRendersHumanReadableNarrative(t *testing.T) {
	payload := map[string]any{
		"query": map[string]any{"text": "MATCH (n) RETURN count(n)"},
		"logicalPlan": map[string]any{
			"rootVertexId": "N2",
			"vertexes": []map[string]any{
				{"id": "N1", "op": "ALL_VERTEXES_SCAN", "accessPath": "all_vertices", "children": []string{}},
				{"id": "N2", "op": "AGGREGATE", "implementation": "fast_vertex_count", "children": []string{"N1"}},
			},
		},
		"influencers": map[string]any{
			"statsSnapshot": map[string]any{
				"source":           "stats-snapshot+store-scan",
				"completeness":     "complete",
				"backfillStatus":   "complete",
				"backfillRequired": false,
			},
		},
		"warnings": []map[string]any{{"code": "FULL_SCAN_FALLBACK", "message": "example"}},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}

	client := &stubQueryServiceClient{
		explainFunc: func(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.ExplainResponse, error) {
			return &v1.ExplainResponse{ExplainJson: encoded, Stats: &v1.QueryStats{RowsReturned: 1, DurationMs: 7}}, nil
		},
	}

	var out bytes.Buffer
	if err := runExplain(context.Background(), client, "acme", "EXPLAIN MATCH (n) RETURN count(n)", true, &out); err != nil {
		t.Fatalf("runExplain returned error: %v", err)
	}
	rendered := out.String()
	if !strings.Contains(rendered, "EXPLAIN") {
		t.Fatalf("expected EXPLAIN header, got: %s", rendered)
	}
	if !strings.Contains(rendered, "Execution path:") {
		t.Fatalf("expected execution path section, got: %s", rendered)
	}
	if !strings.Contains(rendered, "1. ALL_VERTEXES_SCAN") || !strings.Contains(rendered, "2. AGGREGATE") {
		t.Fatalf("expected ordered plan steps, got: %s", rendered)
	}
	if !strings.Contains(rendered, "backfillRequired: false") {
		t.Fatalf("expected stats snapshot details, got: %s", rendered)
	}
	if !strings.Contains(rendered, "[FULL_SCAN_FALLBACK] example") {
		t.Fatalf("expected warnings section, got: %s", rendered)
	}
	if !strings.Contains(rendered, "stats: rows=1 durationMs=7") {
		t.Fatalf("expected stats footer, got: %s", rendered)
	}
}

func TestRunExplainTerseOmitsSupportingDataSections(t *testing.T) {
	payload := map[string]any{
		"query": map[string]any{"text": "MATCH (n) RETURN count(n)"},
		"logicalPlan": map[string]any{
			"rootVertexId": "N2",
			"vertexes": []map[string]any{
				{"id": "N1", "op": "ALL_VERTEXES_SCAN", "children": []string{}},
				{"id": "N2", "op": "AGGREGATE", "implementation": "fast_vertex_count", "children": []string{"N1"}},
			},
		},
		"influencers": map[string]any{
			"statsSnapshot": map[string]any{
				"source":           "stats-snapshot",
				"completeness":     "complete",
				"backfillStatus":   "complete",
				"backfillRequired": false,
			},
		},
		"warnings": []map[string]any{{"code": "FULL_SCAN_FALLBACK", "message": "example"}},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}

	client := &stubQueryServiceClient{
		explainFunc: func(ctx context.Context, in *v1.QueryRequest, opts ...grpc.CallOption) (*v1.ExplainResponse, error) {
			return &v1.ExplainResponse{ExplainJson: encoded, Stats: &v1.QueryStats{RowsReturned: 1, DurationMs: 7}}, nil
		},
	}

	var out bytes.Buffer
	if err := runExplain(context.Background(), client, "acme", "EXPLAIN MATCH (n) RETURN count(n)", false, &out); err != nil {
		t.Fatalf("runExplain returned error: %v", err)
	}
	rendered := out.String()
	if !strings.Contains(rendered, "Execution path:") {
		t.Fatalf("expected execution path section, got: %s", rendered)
	}
	if strings.Contains(rendered, "Statistics snapshot:") {
		t.Fatalf("did not expect statistics snapshot in terse mode, got: %s", rendered)
	}
	if strings.Contains(rendered, "Warnings:") {
		t.Fatalf("did not expect warnings section in terse mode, got: %s", rendered)
	}
	if !strings.Contains(rendered, "stats: rows=1 durationMs=7") {
		t.Fatalf("expected stats footer, got: %s", rendered)
	}
}

func (s *stubQueryServiceClient) GetCapabilities(ctx context.Context, in *v1.CapabilitiesRequest, opts ...grpc.CallOption) (*v1.CapabilitiesResponse, error) {
	return &v1.CapabilitiesResponse{}, nil
}

func (s *stubQueryServiceClient) CreatePropertyIndex(ctx context.Context, in *v1.CreatePropertyIndexRequest, opts ...grpc.CallOption) (*v1.CreatePropertyIndexResponse, error) {
	return &v1.CreatePropertyIndexResponse{}, nil
}

func TestHandleCLICommandSetListUnset(t *testing.T) {
	state := &cliState{variables: map[string]any{}}
	var out bytes.Buffer

	handled, err := handleCLICommand("SET user='seed'", state, &out)
	if err != nil {
		t.Fatalf("SET returned error: %v", err)
	}
	if !handled {
		t.Fatalf("SET should be handled")
	}
	if got, ok := state.variables["user"]; !ok || got != "seed" {
		t.Fatalf("expected user variable to be set")
	}

	out.Reset()
	handled, err = handleCLICommand("SET", state, &out)
	if err != nil {
		t.Fatalf("SET list returned error: %v", err)
	}
	if !handled {
		t.Fatalf("SET list should be handled")
	}
	if !strings.Contains(out.String(), "$user =") {
		t.Fatalf("expected variable list output, got: %s", out.String())
	}

	out.Reset()
	handled, err = handleCLICommand("UNSET user", state, &out)
	if err != nil {
		t.Fatalf("UNSET returned error: %v", err)
	}
	if !handled {
		t.Fatalf("UNSET should be handled")
	}
	if _, ok := state.variables["user"]; ok {
		t.Fatalf("expected user variable to be removed")
	}
}

func TestHandleCLICommandSetRejectsNonScalar(t *testing.T) {
	state := &cliState{variables: map[string]any{}}
	var out bytes.Buffer

	_, err := handleCLICommand("SET payload={\"id\":\"seed\"}", state, &out)
	if err == nil {
		t.Fatalf("expected SET with object literal to fail")
	}

	_, err = handleCLICommand("SET payload=[1,2,3]", state, &out)
	if err == nil {
		t.Fatalf("expected SET with list literal to fail")
	}
}

func TestBindVariablesSkipsStringsAndComments(t *testing.T) {
	vars := map[string]any{"id": "seed", "limit": 2}

	query := "MATCH (n {id: $id}) // $id ignored in comment\nRETURN '$id literal' AS lit, n.id AS id LIMIT $limit"
	bound, err := bindVariables(query, vars)
	if err != nil {
		t.Fatalf("bindVariables returned error: %v", err)
	}

	if !strings.Contains(bound, "id: 'seed'") {
		t.Fatalf("expected id parameter substitution, got: %s", bound)
	}
	if !strings.Contains(bound, "'$id literal'") {
		t.Fatalf("expected string literal untouched, got: %s", bound)
	}
	if !strings.Contains(bound, "// $id ignored in comment") {
		t.Fatalf("expected comment untouched, got: %s", bound)
	}
	if !strings.Contains(bound, "LIMIT 2") {
		t.Fatalf("expected numeric substitution, got: %s", bound)
	}
}

func TestBindVariablesThenParseBatch(t *testing.T) {
	vars := map[string]any{"movieTitle": "Wall Street", "actorRole": "Fox"}
	query := "MATCH (:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)\nWHERE r.role CONTAINS $actorRole\nRETURN p.name AS actor, r.role AS role"

	bound, err := bindVariables(query, vars)
	if err != nil {
		t.Fatalf("bindVariables returned error: %v", err)
	}
	if !strings.Contains(bound, "title: 'Wall Street'") {
		t.Fatalf("expected title substitution, got: %s", bound)
	}
	if !strings.Contains(bound, "CONTAINS 'Fox'") {
		t.Fatalf("expected role substitution, got: %s", bound)
	}

	if _, err := cypher.ParseBatch(bound); err != nil {
		t.Fatalf("expected bound query to parse, got: %v", err)
	}
}

func TestStatementReadyCompleteness(t *testing.T) {
	ready, err := statementReady("MATCH (n\n", nil)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if ready {
		t.Fatalf("expected incomplete statement to be not ready")
	}

	ready, err = statementReady("MATCH (n) RETURN n;", nil)
	if err != nil {
		t.Fatalf("expected complete valid statement, got error: %v", err)
	}
	if !ready {
		t.Fatalf("expected complete statement to be ready")
	}

	ready, err = statementReady("CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),", nil)
	if err != nil {
		t.Fatalf("unexpected parse error for multiline CREATE prefix: %v", err)
	}
	if ready {
		t.Fatalf("expected trailing-comma CREATE prefix to be incomplete")
	}

	ready, err = statementReady("CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),\n(martin:Person:Actor {name: 'Martin Sheen'})", nil)
	if err != nil {
		t.Fatalf("expected complete multiline CREATE to parse, got: %v", err)
	}
	if !ready {
		t.Fatalf("expected multiline CREATE statement to be ready")
	}
}

func TestStatementReadyMultilineParameterizedMatch(t *testing.T) {
	vars := map[string]any{"movieTitle": "Wall Street", "actorRole": "Fox"}

	ready, err := statementReady("MATCH (m:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)", vars)
	if err != nil {
		t.Fatalf("unexpected parse error on first line: %v", err)
	}
	if ready {
		t.Fatalf("expected first MATCH line to be incomplete")
	}

	ready, err = statementReady("MATCH (m:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)\nWHERE r.role CONTAINS $actorRole", vars)
	if err != nil {
		t.Fatalf("unexpected parse error on second line: %v", err)
	}
	if ready {
		t.Fatalf("expected MATCH+WHERE lines to still be incomplete")
	}

	ready, err = statementReady("MATCH (m:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)\nWHERE r.role CONTAINS $actorRole\nRETURN p.name AS actor, r.role AS role", vars)
	if err != nil {
		t.Fatalf("unexpected parse error on full query: %v", err)
	}
	if !ready {
		t.Fatalf("expected full MATCH/WHERE/RETURN query to be ready")
	}
}

func TestRenderTableWideAndNullValues(t *testing.T) {
	longValue := strings.Repeat("x", 100)
	rows := []*v1.Row{{
		Values: map[string]*v1.Value{
			"id":   {Kind: &v1.Value_StringValue{StringValue: longValue}},
			"note": {Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}},
		},
	}}

	var out bytes.Buffer
	renderTable(&out, []string{"id", "note"}, rows, 20)
	rendered := out.String()

	if !strings.Contains(rendered, "NULL") {
		t.Fatalf("expected null marker in output, got: %s", rendered)
	}
	if !strings.Contains(rendered, "...") {
		t.Fatalf("expected truncation marker for wide cell, got: %s", rendered)
	}
	if !strings.Contains(rendered, "(1 rows)") {
		t.Fatalf("expected row count footer, got: %s", rendered)
	}
}

func TestRenderTableColumnWidthUsesHeaderAndRowsWithCap(t *testing.T) {
	rows := []*v1.Row{{
		Values: map[string]*v1.Value{
			"path": {Kind: &v1.Value_StringValue{StringValue: strings.Repeat("p", 120)}},
		},
	}}

	var out bytes.Buffer
	renderTable(&out, []string{"path"}, rows, 80)
	rendered := out.String()
	lines := strings.Split(strings.TrimSpace(rendered), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected header and separator lines, got: %q", rendered)
	}
	if got := len(lines[1]); got != 80 {
		t.Fatalf("expected separator width to match max column width 80, got %d\noutput:\n%s", got, rendered)
	}
}

func TestFormatProtoValueVertexAutoIDSuppressed(t *testing.T) {
	value := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id": {Kind: &v1.Value_StringValue{StringValue: "auto-charlie-1"}},
		"labels": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
			{Kind: &v1.Value_StringValue{StringValue: "Person"}},
		}}}},
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
			"name": {Kind: &v1.Value_BytesValue{BytesValue: []byte("Charlie Sheen")}},
		}}}},
	}}}}

	got := formatProtoValue(value)
	if got != "(:Person {\"name\":\"Charlie Sheen\"})" {
		t.Fatalf("unexpected vertex rendering: %s", got)
	}
}

func TestFormatProtoValueVertexKeepsExplicitID(t *testing.T) {
	value := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id": {Kind: &v1.Value_StringValue{StringValue: "charlie"}},
		"labels": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
			{Kind: &v1.Value_StringValue{StringValue: "Person"}},
			{Kind: &v1.Value_StringValue{StringValue: "Actor"}},
		}}}},
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
			"name": {Kind: &v1.Value_StringValue{StringValue: "Charlie Sheen"}},
		}}}},
	}}}}

	got := formatProtoValue(value)
	if got != "(charlie:Person {\"name\":\"Charlie Sheen\"})" {
		t.Fatalf("unexpected vertex rendering: %s", got)
	}
}

func TestFormatProtoValueEdgeAutoIDSuppressed(t *testing.T) {
	value := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id":   {Kind: &v1.Value_StringValue{StringValue: "acme|auto-charlie-1|ACTED_IN|auto-wallStreet-6|1"}},
		"type": {Kind: &v1.Value_StringValue{StringValue: "ACTED_IN"}},
		"src":  {Kind: &v1.Value_StringValue{StringValue: "charlie"}},
		"dst":  {Kind: &v1.Value_StringValue{StringValue: "wallStreet"}},
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
			"role": {Kind: &v1.Value_StringValue{StringValue: "Bud Fox"}},
		}}}},
	}}}}

	got := formatProtoValue(value)
	if got != "[:ACTED_IN {\"role\":\"Bud Fox\"}]" {
		t.Fatalf("unexpected edge rendering: %s", got)
	}
}

func TestFormatProtoValueEdgeKeepsExplicitID(t *testing.T) {
	value := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id":         {Kind: &v1.Value_StringValue{StringValue: "rel1"}},
		"type":       {Kind: &v1.Value_StringValue{StringValue: "ACTED_IN"}},
		"src":        {Kind: &v1.Value_StringValue{StringValue: "charlie"}},
		"dst":        {Kind: &v1.Value_StringValue{StringValue: "wallStreet"}},
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{}}}},
	}}}}

	got := formatProtoValue(value)
	if got != "[rel1:ACTED_IN]" {
		t.Fatalf("unexpected edge rendering: %s", got)
	}
}

func makeVertexValue(id, label string, props map[string]*v1.Value) *v1.Value {
	labelList := &v1.Value{Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
		{Kind: &v1.Value_StringValue{StringValue: label}},
	}}}}
	return &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id":         {Kind: &v1.Value_StringValue{StringValue: id}},
		"labels":     labelList,
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: props}}},
	}}}}
}

func makeEdgeValue(id, edgeType string, props map[string]*v1.Value) *v1.Value {
	return &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"id":         {Kind: &v1.Value_StringValue{StringValue: id}},
		"type":       {Kind: &v1.Value_StringValue{StringValue: edgeType}},
		"src":        {Kind: &v1.Value_StringValue{StringValue: ""}},
		"dst":        {Kind: &v1.Value_StringValue{StringValue: ""}},
		"properties": {Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: props}}},
	}}}}
}

func TestFormatProtoValuePathForward(t *testing.T) {
	charlie := makeVertexValue("auto-charlie-1", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Charlie Sheen"}},
	})
	actedIn := makeEdgeValue("acme|auto-charlie-1|ACTED_IN|auto-wallStreet-6|1", "ACTED_IN", map[string]*v1.Value{
		"role": {Kind: &v1.Value_StringValue{StringValue: "Bud Fox"}},
	})
	wallStreet := makeVertexValue("auto-wallStreet-6", "Movie", map[string]*v1.Value{
		"title": {Kind: &v1.Value_StringValue{StringValue: "Wall Street"}},
	})

	pathValue := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"__path__": {Kind: &v1.Value_BoolValue{BoolValue: true}},
		"vertexes": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{charlie, wallStreet}}}},
		"edges":    {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{actedIn}}}},
		"directions": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
			{Kind: &v1.Value_StringValue{StringValue: "forward"}},
		}}}},
	}}}}

	got := formatProtoValue(pathValue)
	want := `(:Person {"name":"Charlie Sheen"})-[:ACTED_IN {"role":"Bud Fox"}]->(:Movie {"title":"Wall Street"})`
	if got != want {
		t.Fatalf("unexpected path rendering:\n got:  %s\n want: %s", got, want)
	}
}

func TestFormatProtoValuePathMultiHop(t *testing.T) {
	charlie := makeVertexValue("auto-charlie-1", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Charlie Sheen"}},
	})
	actedIn := makeEdgeValue("acme|auto-charlie-1|ACTED_IN|auto-wallStreet-6|1", "ACTED_IN", map[string]*v1.Value{})
	wallStreet := makeVertexValue("auto-wallStreet-6", "Movie", map[string]*v1.Value{
		"title": {Kind: &v1.Value_StringValue{StringValue: "Wall Street"}},
	})
	directed := makeEdgeValue("acme|auto-oliver-2|DIRECTED|auto-wallStreet-6|2", "DIRECTED", map[string]*v1.Value{})
	oliver := makeVertexValue("auto-oliver-2", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Oliver Stone"}},
	})

	pathValue := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"__path__": {Kind: &v1.Value_BoolValue{BoolValue: true}},
		"vertexes": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{charlie, wallStreet, oliver}}}},
		"edges":    {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{actedIn, directed}}}},
		"directions": {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
			{Kind: &v1.Value_StringValue{StringValue: "forward"}},
			{Kind: &v1.Value_StringValue{StringValue: "reverse"}},
		}}}},
	}}}}

	got := formatProtoValue(pathValue)
	want := `(:Person {"name":"Charlie Sheen"})-[:ACTED_IN]->(:Movie {"title":"Wall Street"})<-[:DIRECTED]-(:Person {"name":"Oliver Stone"})`
	if got != want {
		t.Fatalf("unexpected path rendering:\n got:  %s\n want: %s", got, want)
	}
}

func TestNextLoadQueryWriteModeAlternatesCreateDelete(t *testing.T) {
	cfg := cliConfig{loadPrefix: "soak"}
	rng := rand.New(rand.NewSource(7))
	state := &loadGenState{}

	q1, k1 := nextLoadQuery(cfg, "write", 0, rng, state)
	q2, k2 := nextLoadQuery(cfg, "write", 1, rng, state)

	if k1 != "create" || !strings.Contains(q1, "CREATE (:SoakVertex") || !strings.Contains(q1, "soak-write-0") {
		t.Fatalf("unexpected first write op: kind=%s query=%s", k1, q1)
	}
	if k2 != "delete" || !strings.Contains(q2, "MATCH (n:SoakVertex") || !strings.Contains(q2, "soak-write-0") {
		t.Fatalf("unexpected second write op: kind=%s query=%s", k2, q2)
	}
}

func TestNextLoadQueryNoopWriteMode(t *testing.T) {
	cfg := cliConfig{loadPrefix: "soak"}
	rng := rand.New(rand.NewSource(7))

	query, kind := nextLoadQuery(cfg, "noop-write", 42, rng, &loadGenState{})
	if kind != "create" {
		t.Fatalf("expected create kind, got %s", kind)
	}
	if !strings.Contains(query, "CREATE (:SoakNoopVertex") || !strings.Contains(query, "soak-noop") {
		t.Fatalf("unexpected noop-write query: %s", query)
	}
}

func TestNextLoadQueryReadModeRespectsHopBoundsAndLimit(t *testing.T) {
	cfg := cliConfig{loadReadMinHop: 2, loadReadMaxHop: 4, loadReadLimit: 10}
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 20; i++ {
		query, kind := nextLoadQuery(cfg, "read", i, rng, &loadGenState{})
		if kind != "read" {
			t.Fatalf("expected read kind, got %s", kind)
		}
		if !strings.Contains(query, "LIMIT 10") {
			t.Fatalf("expected read limit in query, got %s", query)
		}
		if !strings.Contains(query, "-[*2]-") && !strings.Contains(query, "-[*3]-") && !strings.Contains(query, "-[*4]-") {
			t.Fatalf("expected hop within [2,4], got %s", query)
		}
	}
}

func TestParsePurgeArgsValid(t *testing.T) {
	cases := []struct {
		line          string
		wantLabel     string
		wantBatchSize int
	}{
		{":purge Movie", "Movie", 0},
		{":purge Movie|Genre|User", "Movie|Genre|User", 0},
		{":purge *", "*", 0},
		{":purge Movie|Genre 500", "Movie|Genre", 500},
		{":PURGE movie_vertex", "movie_vertex", 0},
		{":purge _Private|Public 250", "_Private|Public", 250},
	}
	for _, tc := range cases {
		label, batch, err := parsePurgeArgs(tc.line)
		if err != nil {
			t.Errorf("parsePurgeArgs(%q) unexpected error: %v", tc.line, err)
			continue
		}
		if label != tc.wantLabel {
			t.Errorf("parsePurgeArgs(%q) label = %q, want %q", tc.line, label, tc.wantLabel)
		}
		if batch != tc.wantBatchSize {
			t.Errorf("parsePurgeArgs(%q) batch = %d, want %d", tc.line, batch, tc.wantBatchSize)
		}
	}
}

func TestParsePurgeArgsInvalid(t *testing.T) {
	cases := []string{
		":purge",
		":purge 123badlabel",
		":purge Movie|",
		":purge |Genre",
	}
	for _, tc := range cases {
		_, _, err := parsePurgeArgs(tc)
		if err == nil {
			t.Errorf("parsePurgeArgs(%q) expected error, got nil", tc)
		}
	}
}

func TestValidatePurgeLabelExpr(t *testing.T) {
	valid := []string{"Movie", "Movie|Genre", "*", "_Under", "Label123"}
	for _, v := range valid {
		if err := validatePurgeLabelExpr(v); err != nil {
			t.Errorf("validatePurgeLabelExpr(%q) unexpected error: %v", v, err)
		}
	}

	invalid := []string{"", "Movie|", "|Genre", "123Bad", "has space", "bad-dash"}
	for _, v := range invalid {
		if err := validatePurgeLabelExpr(v); err == nil {
			t.Errorf("validatePurgeLabelExpr(%q) expected error, got nil", v)
		}
	}
}

// intValue wraps an int64 as a v1.Value with IntValue kind.
func intValue(n int64) *v1.Value {
	return &v1.Value{Kind: &v1.Value_IntValue{IntValue: n}}
}

// countResponse builds a QueryResponse containing a single "remaining" int column.
func countResponse(n int64) *v1.QueryResponse {
	return &v1.QueryResponse{
		Columns: []string{"remaining"},
		Rows: []*v1.Row{{
			Values: map[string]*v1.Value{"remaining": intValue(n)},
		}},
		Stats: &v1.QueryStats{RowsReturned: 1, DurationMs: 1},
	}
}

func TestRunPurgeNoVertices(t *testing.T) {
	stub := &stubQueryServiceClient{
		executeFunc: func(_ context.Context, in *v1.QueryRequest, _ ...grpc.CallOption) (*v1.QueryResponse, error) {
			// Only count query should be called; returns 0.
			return countResponse(0), nil
		},
	}
	cfg := cliConfig{tenant: "default", timeout: 2 * time.Second, purgeBatchSize: 10}
	var out bytes.Buffer
	if err := runPurge(context.Background(), stub, cfg, "Movie", 10, &out, &out); err != nil {
		t.Fatalf("runPurge returned error: %v", err)
	}
	if !strings.Contains(out.String(), "total_deleted=0") {
		t.Fatalf("expected zero-deleted message, got: %s", out.String())
	}
}

func TestRunPurgeSingleBatch(t *testing.T) {
	// Simulate: initial count=5, after delete count=0 → done in 1 batch.
	callNum := 0
	stub := &stubQueryServiceClient{
		executeFunc: func(_ context.Context, in *v1.QueryRequest, _ ...grpc.CallOption) (*v1.QueryResponse, error) {
			callNum++
			q := strings.ToUpper(in.GetInput().GetCypher())
			if strings.Contains(q, "RETURN COUNT") || strings.Contains(q, "RETURN count") || strings.Contains(in.GetInput().GetCypher(), "RETURN count") {
				if callNum == 1 {
					return countResponse(5), nil // initial count
				}
				return countResponse(0), nil // after-batch count
			}
			// delete batch → empty response
			return &v1.QueryResponse{}, nil
		},
	}

	cfg := cliConfig{tenant: "default", timeout: 2 * time.Second, purgeBatchSize: 100}
	var out bytes.Buffer
	if err := runPurge(context.Background(), stub, cfg, "Movie", 100, &out, &out); err != nil {
		t.Fatalf("runPurge returned error: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "purge-start") {
		t.Errorf("expected purge-start line, got: %s", got)
	}
	if !strings.Contains(got, "purge-done") {
		t.Errorf("expected purge-done line, got: %s", got)
	}
	if !strings.Contains(got, "batch=1") {
		t.Errorf("expected batch=1 in progress, got: %s", got)
	}
}

func TestRunPurgeMultiBatch(t *testing.T) {
	// Simulate: 2500 vertexes, batch size 1000 → 3 batches needed.
	remaining := int64(2500)
	batchSize := int64(1000)

	stub := &stubQueryServiceClient{
		executeFunc: func(_ context.Context, in *v1.QueryRequest, _ ...grpc.CallOption) (*v1.QueryResponse, error) {
			cypher := in.GetInput().GetCypher()
			if strings.Contains(cypher, "RETURN count") {
				return countResponse(remaining), nil
			}
			// delete batch
			if remaining >= batchSize {
				remaining -= batchSize
			} else {
				remaining = 0
			}
			return &v1.QueryResponse{}, nil
		},
	}

	cfg := cliConfig{tenant: "default", timeout: 5 * time.Second, purgeBatchSize: int(batchSize)}
	var out bytes.Buffer
	if err := runPurge(context.Background(), stub, cfg, "Movie|Genre", int(batchSize), &out, &out); err != nil {
		t.Fatalf("runPurge returned error: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "batch=3") {
		t.Errorf("expected batch=3 in progress output, got: %s", got)
	}
	if !strings.Contains(got, "batches=3") {
		t.Errorf("expected batches=3 in done line, got: %s", got)
	}
}

func TestRunPurgeAllVertices(t *testing.T) {
	// Verify "*" generates MATCH (n) without label filter.
	var capturedQuery string
	stub := &stubQueryServiceClient{
		executeFunc: func(_ context.Context, in *v1.QueryRequest, _ ...grpc.CallOption) (*v1.QueryResponse, error) {
			capturedQuery = in.GetInput().GetCypher()
			return countResponse(0), nil // 0 vertexes → exits immediately
		},
	}
	cfg := cliConfig{tenant: "default", timeout: 2 * time.Second, purgeBatchSize: 100}
	var out bytes.Buffer
	if err := runPurge(context.Background(), stub, cfg, "*", 100, &out, &out); err != nil {
		t.Fatalf("runPurge returned error: %v", err)
	}
	if !strings.Contains(capturedQuery, "MATCH (n)") {
		t.Errorf("expected MATCH (n) without label filter, got: %s", capturedQuery)
	}
	if strings.Contains(capturedQuery, "MATCH (n:") {
		t.Errorf("expected no label filter for *, got: %s", capturedQuery)
	}
}
