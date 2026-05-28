package main

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"

	v1 "github.com/paegun/vitaledge/api/proto/vitaledge/v1"
	"github.com/paegun/vitaledge/internal/cypher"
)

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

func TestFormatProtoValueNodeAutoIDSuppressed(t *testing.T) {
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
		t.Fatalf("unexpected node rendering: %s", got)
	}
}

func TestFormatProtoValueNodeKeepsExplicitID(t *testing.T) {
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
		t.Fatalf("unexpected node rendering: %s", got)
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

func makeNodeValue(id, label string, props map[string]*v1.Value) *v1.Value {
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
	charlie := makeNodeValue("auto-charlie-1", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Charlie Sheen"}},
	})
	actedIn := makeEdgeValue("acme|auto-charlie-1|ACTED_IN|auto-wallStreet-6|1", "ACTED_IN", map[string]*v1.Value{
		"role": {Kind: &v1.Value_StringValue{StringValue: "Bud Fox"}},
	})
	wallStreet := makeNodeValue("auto-wallStreet-6", "Movie", map[string]*v1.Value{
		"title": {Kind: &v1.Value_StringValue{StringValue: "Wall Street"}},
	})

	pathValue := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"__path__": {Kind: &v1.Value_BoolValue{BoolValue: true}},
		"nodes":    {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{charlie, wallStreet}}}},
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
	charlie := makeNodeValue("auto-charlie-1", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Charlie Sheen"}},
	})
	actedIn := makeEdgeValue("acme|auto-charlie-1|ACTED_IN|auto-wallStreet-6|1", "ACTED_IN", map[string]*v1.Value{})
	wallStreet := makeNodeValue("auto-wallStreet-6", "Movie", map[string]*v1.Value{
		"title": {Kind: &v1.Value_StringValue{StringValue: "Wall Street"}},
	})
	directed := makeEdgeValue("acme|auto-oliver-2|DIRECTED|auto-wallStreet-6|2", "DIRECTED", map[string]*v1.Value{})
	oliver := makeNodeValue("auto-oliver-2", "Person", map[string]*v1.Value{
		"name": {Kind: &v1.Value_StringValue{StringValue: "Oliver Stone"}},
	})

	pathValue := &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
		"__path__": {Kind: &v1.Value_BoolValue{BoolValue: true}},
		"nodes":    {Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{charlie, wallStreet, oliver}}}},
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

	if k1 != "create" || !strings.Contains(q1, "CREATE (:SoakNode") || !strings.Contains(q1, "soak-write-0") {
		t.Fatalf("unexpected first write op: kind=%s query=%s", k1, q1)
	}
	if k2 != "delete" || !strings.Contains(q2, "MATCH (n:SoakNode") || !strings.Contains(q2, "soak-write-0") {
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
	if !strings.Contains(query, "CREATE (:SoakNoopNode") || !strings.Contains(query, "soak-noop") {
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
