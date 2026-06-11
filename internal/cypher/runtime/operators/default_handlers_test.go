package operators

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

type antiProbeCountingTx struct {
	graph.Tx
	batchDirectedCalls int
	rowDirectedCalls   int
}

func (t *antiProbeCountingTx) HasDirectedEdgeBetween(context.Context, string, string, string, string) (bool, error) {
	t.rowDirectedCalls++
	return false, nil
}

func (t *antiProbeCountingTx) BatchHasDirectedEdgeBetween(context.Context, string, []graph.DirectedEdgeProbe) ([]bool, error) {
	t.batchDirectedCalls++
	return []bool{false}, nil
}

func (t *antiProbeCountingTx) HasUndirectedEdgeBetween(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}

func (t *antiProbeCountingTx) BatchHasUndirectedEdgeBetween(context.Context, string, []graph.UndirectedEdgeProbe) ([]bool, error) {
	return nil, nil
}

func TestWriteHandlerRecordsEventAndMarksRows(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows:                     []map[string]any{{"u": "u1"}},
		Params:                   map[string]any{"peer": "u2", "id": "u1"},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p2", map[string]any{
		"kind":         "MERGE",
		"raw":          "MERGE (u)-[:KNOWS]->(:User {id:$peer})",
		"mergePattern": "(u)-[:KNOWS]->(:User {id:$peer})",
		"mergeOnCreate": []string{
			"SET r.createdAt = timestamp()",
		},
		"mergeOnMatch": []any{
			"SET r.touchedAt = timestamp()",
		},
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if len(state.ExecutedOps) != 1 || state.ExecutedOps[0] != "PHY_WRITE" {
		t.Fatalf("unexpected executed ops: %#v", state.ExecutedOps)
	}
	if state.OperatorExecCount != 1 {
		t.Fatalf("unexpected operator count: %d", state.OperatorExecCount)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}

	event := state.WriteEvents[0]
	if event.NodeID != "p2" {
		t.Fatalf("unexpected event node id: %#v", event)
	}
	if event.Kind != "MERGE" {
		t.Fatalf("unexpected event kind: %#v", event)
	}
	if event.MergePattern == "" {
		t.Fatalf("expected merge pattern in write event: %#v", event)
	}
	if event.MutationType != MutationTypeEdge {
		t.Fatalf("expected edge mutation classification, got %#v", event)
	}
	if event.Edge == nil || event.Edge.Type != "KNOWS" {
		t.Fatalf("expected edge mutation payload, got %#v", event)
	}
	if event.Edge.LeftVar != "u" || event.Edge.RightIDParam != "peer" {
		t.Fatalf("expected bridge-ready endpoint hints, got %#v", event.Edge)
	}
	if len(event.Edge.RightLabels) != 1 || event.Edge.RightLabels[0] != "User" {
		t.Fatalf("expected right endpoint label User, got %#v", event.Edge)
	}
	if event.Vertex != nil {
		t.Fatalf("did not expect vertex payload on edge mutation, got %#v", event)
	}
	if got := event.Bindings["u"]; got != "u1" {
		t.Fatalf("expected captured row binding for u, got %#v", event.Bindings)
	}
	if got := event.ResolvedParams["peer"]; got != "u2" {
		t.Fatalf("expected resolved peer param in payload, got %#v", event.ResolvedParams)
	}
	if _, ok := event.ResolvedParams["id"]; ok {
		t.Fatalf("did not expect unrelated id param in payload, got %#v", event.ResolvedParams)
	}
	if len(event.ParamKeys) != 1 || event.ParamKeys[0] != "peer" {
		t.Fatalf("expected extracted param keys [peer], got %#v", event.ParamKeys)
	}
	if got, ok := state.Rows[0]["__write_event_count"].(int); !ok || got != 1 {
		t.Fatalf("expected row write marker=1, got row=%#v", state.Rows[0])
	}
}

func TestCallHandlerExecutesInQueryCall(t *testing.T) {
	h := NewCallHandler()
	called := false
	state := &State{
		Rows:   []map[string]any{{"x": 1}},
		Params: map[string]any{"tenant": "acme"},
		InQueryCallExecutor: func(ctx context.Context, inputRows []map[string]any, callRaw string, params map[string]any, inQuery bool) ([]map[string]any, []string, error) {
			called = true
			if callRaw != "CALL db.stats.vertexCount() YIELD vertexCount" {
				t.Fatalf("unexpected call raw: %q", callRaw)
			}
			if !inQuery {
				t.Fatalf("expected inQuery=true")
			}
			return []map[string]any{{"x": 1, "vertexCount": 3}}, []string{"x", "vertexCount"}, nil
		},
	}

	err := h.Execute("p-call", map[string]any{
		"raw": "CALL db.stats.vertexCount() YIELD vertexCount",
	}, state)
	if err != nil {
		t.Fatalf("call execute failed: %v", err)
	}
	if !called {
		t.Fatalf("expected in-query call executor to be invoked")
	}
	if len(state.Rows) != 1 || state.Rows[0]["vertexCount"] != 3 {
		t.Fatalf("unexpected rows after call: %#v", state.Rows)
	}
	if len(state.ExecutedOps) != 1 || state.ExecutedOps[0] != "PHY_CALL" {
		t.Fatalf("unexpected executed ops: %#v", state.ExecutedOps)
	}
}

func TestCallHandlerSeedsSingleRowForEmptyInitialInput(t *testing.T) {
	h := NewCallHandler()
	state := &State{
		Rows: []map[string]any{},
		InQueryCallExecutor: func(ctx context.Context, inputRows []map[string]any, callRaw string, params map[string]any, inQuery bool) ([]map[string]any, []string, error) {
			if len(inputRows) != 1 {
				t.Fatalf("expected one seeded row for initial empty input, got %#v", inputRows)
			}
			return inputRows, nil, nil
		},
	}

	err := h.Execute("p-call", map[string]any{"raw": "CALL test.doNothing()"}, state)
	if err != nil {
		t.Fatalf("call execute failed: %v", err)
	}
}

func TestCallHandlerPreservesEmptyInputRows(t *testing.T) {
	h := NewCallHandler()
	state := &State{
		Rows:        []map[string]any{},
		ExecutedOps: []string{"PHY_EXPAND_MATCH"},
		InQueryCallExecutor: func(ctx context.Context, inputRows []map[string]any, callRaw string, params map[string]any, inQuery bool) ([]map[string]any, []string, error) {
			if len(inputRows) != 0 {
				t.Fatalf("expected empty rows after prior operators to remain empty, got %#v", inputRows)
			}
			return inputRows, nil, nil
		},
	}

	err := h.Execute("p-call", map[string]any{"raw": "CALL test.doNothing()"}, state)
	if err != nil {
		t.Fatalf("call execute failed: %v", err)
	}
}

func TestCallHandlerDoesNotNullExistingBindings(t *testing.T) {
	h := NewCallHandler()
	state := &State{
		Rows: []map[string]any{{"n": "v1", "n.id": "v1"}},
		InQueryCallExecutor: func(ctx context.Context, inputRows []map[string]any, callRaw string, params map[string]any, inQuery bool) ([]map[string]any, []string, error) {
			return []map[string]any{{"n": nil, "n.id": nil}}, nil, nil
		},
	}

	err := h.Execute("p-call", map[string]any{"raw": "CALL test.doNothing()"}, state)
	if err != nil {
		t.Fatalf("call execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["n"]; got != "v1" {
		t.Fatalf("expected n binding preserved as v1, got %#v", got)
	}
	if got := state.Rows[0]["n.id"]; got != "v1" {
		t.Fatalf("expected n.id binding preserved as v1, got %#v", got)
	}
}

func TestParseProjectionSpecStripsBacktickedAlias(t *testing.T) {
	expr, alias := parseProjectionSpec("n.name AS `name`")
	if expr != "n.name" {
		t.Fatalf("expected expr n.name, got %q", expr)
	}
	if alias != "name" {
		t.Fatalf("expected alias name, got %q", alias)
	}
}

func TestSplitProjectionConnectedEdgePatternsPreservesConnectorBinding(t *testing.T) {
	pattern := "(a)-[:LIKES]->()-[:LIKES*3]->(c)"
	edges := splitProjectionConnectedEdgePatterns(pattern)
	if len(edges) != 2 {
		t.Fatalf("expected 2 edge segments, got %d: %#v", len(edges), edges)
	}
	if !strings.Contains(edges[0], "__ve_conn_1") || !strings.Contains(edges[1], "__ve_conn_1") {
		t.Fatalf("expected synthetic connector var in both segments, got %#v", edges)
	}
	second := parseEdgeMutation(edges[1])
	if second == nil {
		t.Fatalf("expected second segment to parse: %q", edges[1])
	}
	if second.LeftVar != "__ve_conn_1" {
		t.Fatalf("expected connector to bind second segment left var, got %q in %q", second.LeftVar, edges[1])
	}
	minHops, maxHops, ok := parseProjectionVariableLengthBounds(edges[1])
	if !ok || minHops != 3 || maxHops != 3 {
		t.Fatalf("expected fixed bounds 3..3 on second segment, got ok=%v min=%d max=%d", ok, minHops, maxHops)
	}
}

func TestExpandMatchHandlerBindsVertexOnlyPattern(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-ops-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-begin", Labels: []string{"Begin"}})
	}); err != nil {
		t.Fatalf("seed begin vertex failed: %v", err)
	}

	h := NewExpandMatchHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-match", map[string]any{"pattern": "(x:Begin)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one bound row for vertex-only pattern, got %#v", state.Rows)
		}
		if got := state.Rows[0]["x"]; got != "v-begin" {
			t.Fatalf("expected x binding v-begin, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["x.id"]; got != "v-begin" {
			t.Fatalf("expected x.id binding v-begin, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("expand execute failed: %v", err)
	}
}

func TestResolveProjectionExprValueBoundVertexPropertyFromIDBinding(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-ops-bound-prop-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n2", Labels: []string{"Vertex"}, Properties: graph.PropertyMap{"flag": []byte("true"), "role": []byte("business")}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tenant: "acme", Tx: tx}
		row := map[string]any{"n": "n2", "n.id": "n2"}

		gotFlag, ok := resolveProjectionExprValue("n.flag", row, state)
		if !ok {
			t.Fatalf("expected n.flag to resolve")
		}
		if gotFlag != true {
			t.Fatalf("expected n.flag=true, got %#v", gotFlag)
		}

		gotRole, ok := resolveProjectionExprValue("n.role", row, state)
		if !ok {
			t.Fatalf("expected n.role to resolve")
		}
		if gotRole != "business" {
			t.Fatalf("expected n.role=business, got %#v", gotRole)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValueSimpleCaseWithBoundVertexProperty(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-ops-case-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: graph.PropertyMap{"role": []byte("business")}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d1", Labels: []string{"Dept"}, Properties: graph.PropertyMap{"departmentPhone": []byte("555-DEPT"), "businessPhone": []byte("555-BIZ")}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tenant: "acme", Tx: tx}
		row := map[string]any{"p": "p1", "p.id": "p1", "d": "d1", "d.id": "d1"}
		expr := "CASE p.role WHEN 'management' THEN d.departmentPhone WHEN 'business' THEN d.businessPhone ELSE d.departmentPhone END"

		got, ok := resolveProjectionExprValue(expr, row, state)
		if !ok {
			t.Fatalf("expected CASE expression to resolve")
		}
		if got != "555-BIZ" {
			t.Fatalf("expected CASE result 555-BIZ, got %#v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestMaterializeWriteSetBodyBindingsSimpleCaseOnBoundProperty(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-ops-set-case-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n2", Labels: []string{"Vertex"}, Properties: graph.PropertyMap{"flag": []byte("true")}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tenant: "acme", Tx: tx}
		row := map[string]any{"n": "n2", "n.id": "n2"}
		body := "n.label = CASE n.flag WHEN true THEN 'active' ELSE 'inactive' END"
		materializeWriteSetBodyBindings(row, body, state)
		if got := row["n.label"]; got != "active" {
			t.Fatalf("expected n.label=active, got %#v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestExpandMatchHandlerResolvesBoundVertexPropertyValue(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-prop-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1", Properties: graph.PropertyMap{"num": []byte("42")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		h := NewExpandMatchHandler()
		if err := h.Execute("p-match-prop", map[string]any{"pattern": "(n)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one bound row for vertex-only pattern, got %#v", state.Rows)
		}
		value, ok, err := resolveProjectionExprValueChecked("n.num", state.Rows[0], state)
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected n.num to resolve, got ok=false row=%#v", state.Rows[0])
		}
		if got := fmt.Sprint(value); got != "42" {
			t.Fatalf("expected n.num=42, got %#v row=%#v", value, state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("expand property execute failed: %v", err)
	}
}

func TestProjectHandlerAliasSameAsSourceVariablePreservesPropertyValue(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-proj-alias-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1", Properties: graph.PropertyMap{"num": []byte("42")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		expand := NewExpandMatchHandler()
		project := NewProjectHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := expand.Execute("p-match-alias", map[string]any{"pattern": "(n)"}, state); err != nil {
			return err
		}
		if err := project.Execute("p-proj-alias", map[string]any{"kind": "RETURN", "items": []string{"n.num AS n"}}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one projected row, got %#v", state.Rows)
		}
		if got := fmt.Sprint(state.Rows[0]["n"]); got != "42" {
			t.Fatalf("expected projected alias n=42, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("alias projection execute failed: %v", err)
	}
}

func TestProjectHandlerAliasSameAsSourceVariableWithAggregatePreservesPropertyValue(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-proj-alias-agg-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1", Properties: graph.PropertyMap{"num": []byte("42")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		expand := NewExpandMatchHandler()
		project := NewProjectHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := expand.Execute("p-match-alias-agg", map[string]any{"pattern": "(n)"}, state); err != nil {
			return err
		}
		if value, ok, err := resolveProjectionExprValueChecked("n.num", state.Rows[0], state); err != nil || !ok || fmt.Sprint(value) != "42" {
			t.Fatalf("expected direct n.num resolution to be 42 before aggregate projection, got value=%#v ok=%v err=%v row=%#v", value, ok, err, state.Rows[0])
		}
		if err := project.Execute("p-proj-alias-agg", map[string]any{"kind": "RETURN", "items": []string{"n.num AS n", "count(n) AS count"}}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one aggregated row, got %#v", state.Rows)
		}
		if got := fmt.Sprint(state.Rows[0]["n"]); got != "42" {
			t.Fatalf("expected aggregated alias n=42, got %#v", state.Rows[0])
		}
		if got := fmt.Sprint(state.Rows[0]["count"]); got != "1" {
			t.Fatalf("expected count=1, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("aggregate alias projection execute failed: %v", err)
	}
}

func TestExpandMatchHandlerMatchesConjunctiveMultiVariableJoinPattern(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-join-pattern-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		for i, id := range []string{"a", "b", "c", "d"} {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: id, Properties: graph.PropertyMap{"id": []byte(fmt.Sprint(i + 1))}}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", SrcID: "a", DstID: "b", Type: "T"},
			{Tenant: "acme", ID: "e2", SrcID: "b", DstID: "c", Type: "T"},
			{Tenant: "acme", ID: "e3", SrcID: "c", DstID: "d", Type: "T"},
			{Tenant: "acme", ID: "e4", SrcID: "d", DstID: "a", Type: "T"},
			{Tenant: "acme", ID: "e5", SrcID: "b", DstID: "d", Type: "T"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		h := NewExpandMatchHandler()
		if err := h.Execute("p-join", map[string]any{"pattern": "(a)--(b)--(c)--(d)--(a), (b)--(d)"}, state); err != nil {
			return err
		}
		if len(state.Rows) == 0 {
			t.Fatalf("expected join pattern to produce rows, got %#v", state.Rows)
		}
		if value, ok, err := resolveProjectionExprValueChecked("a.id", state.Rows[0], state); err != nil || !ok {
			t.Fatalf("expected a.id to resolve on join row, got value=%#v ok=%v err=%v row=%#v", value, ok, err, state.Rows[0])
		}
		if value, ok, err := resolveProjectionExprValueChecked("c.id", state.Rows[0], state); err != nil || !ok {
			t.Fatalf("expected c.id to resolve on join row, got value=%#v ok=%v err=%v row=%#v", value, ok, err, state.Rows[0])
		}
		filtered, err := filterRowsByProjectionPredicate(state.Rows, "a.id = 1 AND c.id = 3", state)
		if err != nil {
			return err
		}
		if len(filtered) == 0 {
			t.Fatalf("expected join pattern rows to satisfy conjunctive id predicate, got %#v", state.Rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("join pattern execute failed: %v", err)
	}
}

func TestExpandMatchHandlerMatchesTCKConjunctiveMultiVariablePredicateFixture(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-tck-matchwhere-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		verts := []*graph.Vertex{
			{Tenant: "acme", ID: "a", Labels: []string{"A"}},
			{Tenant: "acme", ID: "b", Labels: []string{"B"}, Properties: graph.PropertyMap{"id": []byte("1")}},
			{Tenant: "acme", ID: "c", Labels: []string{"C"}, Properties: graph.PropertyMap{"id": []byte("2")}},
			{Tenant: "acme", ID: "d", Labels: []string{"D"}},
		}
		for _, vertex := range verts {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", SrcID: "a", DstID: "b", Type: "T"},
			{Tenant: "acme", ID: "e2", SrcID: "a", DstID: "c", Type: "T"},
			{Tenant: "acme", ID: "e3", SrcID: "a", DstID: "d", Type: "T"},
			{Tenant: "acme", ID: "e4", SrcID: "b", DstID: "c", Type: "T"},
			{Tenant: "acme", ID: "e5", SrcID: "b", DstID: "d", Type: "T"},
			{Tenant: "acme", ID: "e6", SrcID: "c", DstID: "d", Type: "T"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		h := NewExpandMatchHandler()
		firstOnly := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-first", map[string]any{"pattern": "(a)--(b)--(c)--(d)--(a)"}, firstOnly); err != nil {
			return err
		}
		if len(firstOnly.Rows) == 0 {
			t.Fatalf("expected first pattern to produce rows, got %#v", firstOnly.Rows)
		}
		if _, ok := firstOnly.Rows[0]["c"]; !ok {
			t.Fatalf("expected first pattern rows to retain c binding, got %#v", firstOnly.Rows[0])
		}
		secondOnly := &State{Tx: tx, Tenant: "acme", Rows: firstOnly.Rows}
		if err := h.Execute("p-second", map[string]any{"pattern": "(b)--(d)"}, secondOnly); err != nil {
			return err
		}
		if len(secondOnly.Rows) == 0 {
			t.Fatalf("expected second pattern to produce rows, got %#v", secondOnly.Rows)
		}
		if _, ok := secondOnly.Rows[0]["c"]; !ok {
			t.Fatalf("expected second pattern rows to preserve c binding, got %#v", secondOnly.Rows[0])
		}
		aMatchCount := 0
		cMatchCount := 0
		bothMatchCount := 0
		for _, row := range secondOnly.Rows {
			aValue, aOK, aErr := resolveProjectionExprValueChecked("a.id", row, state)
			cValue, cOK, cErr := resolveProjectionExprValueChecked("c.id", row, state)
			aMatches := aErr == nil && aOK && fmt.Sprint(aValue) == "1"
			cMatches := cErr == nil && cOK && fmt.Sprint(cValue) == "2"
			if aMatches {
				aMatchCount++
			}
			if cMatches {
				cMatchCount++
			}
			if aMatches && cMatches {
				bothMatchCount++
			}
		}
		if aMatchCount == 0 || cMatchCount == 0 {
			t.Fatalf("expected both conjuncts to appear individually in the exact fixture rows, got aMatch=%d cMatch=%d rows=%#v", aMatchCount, cMatchCount, secondOnly.Rows)
		}
		if bothMatchCount == 0 {
			t.Fatalf("expected at least one row to satisfy both conjuncts, got aMatch=%d cMatch=%d both=%d rows=%#v", aMatchCount, cMatchCount, bothMatchCount, secondOnly.Rows)
		}
		if err := h.Execute("p-match", map[string]any{"pattern": "(a)--(b)--(c)--(d)--(a), (b)--(d)"}, state); err != nil {
			return err
		}
		if len(state.Rows) == 0 {
			t.Fatalf("expected rows from exact TCK fixture pattern, got %#v", state.Rows)
		}
		foundMatchingBinding := false
		for _, row := range state.Rows {
			value, ok, err := resolveProjectionExprValueChecked("a.id", row, state)
			if err == nil && ok && fmt.Sprint(value) == "1" {
				foundMatchingBinding = true
				break
			}
		}
		if !foundMatchingBinding {
			t.Fatalf("expected one of the fixture rows to resolve a.id to 1, got %#v", state.Rows)
		}
		filtered, err := filterRowsByProjectionPredicate(state.Rows, "a.id = 1 AND c.id = 2", state)
		if err != nil {
			return err
		}
		if len(filtered) == 0 {
			t.Fatalf("expected at least one row matching conjunctive predicate, got %#v", state.Rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("fixture pattern execute failed: %v", err)
	}
}

func TestExpandMatchHandlerAppliesWherePredicate(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-where-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a-b", Labels: []string{"A", "B"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a-c", Labels: []string{"A", "C"}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c", Labels: []string{"C"}})
	}); err != nil {
		t.Fatalf("seed vertices failed: %v", err)
	}

	h := NewExpandMatchHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-match-where", map[string]any{"pattern": "(a)", "where": "a:C:A:A:C"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one row after expand where predicate, got %#v", state.Rows)
		}
		if got := state.Rows[0]["a"]; got != "a-c" {
			t.Fatalf("expected surviving binding a-c, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("expand where execute failed: %v", err)
	}
}

func TestExpandMatchHandlerSupportsConnectedMultiHopPattern(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-multihop-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Labels: []string{"C"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a1", DstID: "b1", Type: "KNOWS"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "b1", DstID: "c1", Type: "FOLLOWS"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	h := NewExpandMatchHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-match-multihop", map[string]any{"pattern": "(a:A)-[:KNOWS]->(b)-->(c)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one row for connected two-hop pattern, got %#v", state.Rows)
		}
		if got := state.Rows[0]["a"]; got != "a1" {
			t.Fatalf("expected a binding a1, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["b"]; got != "b1" {
			t.Fatalf("expected b binding b1, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["c"]; got != "c1" {
			t.Fatalf("expected c binding c1, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("expand multi-hop execute failed: %v", err)
	}
}

func TestExpandMatchHandlerSupportsTypeAlternation(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-type-alt-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b2"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a1", DstID: "b1", Type: "KNOWS"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "a1", DstID: "b2", Type: "FOLLOWS"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", SrcID: "a1", DstID: "b2", Type: "OTHER"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	h := NewExpandMatchHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-match-type-alt", map[string]any{"pattern": "(a)-[:KNOWS|FOLLOWS]->(b)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 2 {
			t.Fatalf("expected two rows for two allowed relationship types, got %#v", state.Rows)
		}
		seen := map[string]struct{}{}
		for _, row := range state.Rows {
			seen[fmt.Sprint(row["b"])] = struct{}{}
		}
		if _, ok := seen["b1"]; !ok {
			t.Fatalf("expected b1 in results, got %#v", state.Rows)
		}
		if _, ok := seen["b2"]; !ok {
			t.Fatalf("expected b2 in results, got %#v", state.Rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("expand type alternation execute failed: %v", err)
	}
}

func TestExpandMatchHandlerChainsAnonymousConnectorVertex(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-anon-chain-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a", DstID: "b", Type: "T1"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "b", DstID: "c", Type: "T2"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", SrcID: "d", DstID: "c", Type: "T2"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	h := NewExpandMatchHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := h.Execute("p-match-anon-chain", map[string]any{"pattern": "()-[r1:T1]->()-[r2:T2]->()"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected exactly one chained row, got %#v", state.Rows)
		}
		if got := state.Rows[0]["r1.id"]; got != "e1" {
			t.Fatalf("expected r1 bound to e1, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["r2.id"]; got != "e2" {
			t.Fatalf("expected r2 bound to e2, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("expand anonymous chain execute failed: %v", err)
	}
}

func TestOptionalExpandHandlerPreservesBoundEdgeVariableOnMismatch(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-optional-edge-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1"}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	h := NewOptionalExpandHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{
			"a":         "a1",
			"a.id":      "a1",
			"c":         "c1",
			"c.id":      "c1",
			"r":         "stale",
			"r.id":      "stale",
			"__edge.id": "stale-edge",
		}}}
		if err := h.Execute("p-optional", map[string]any{"pattern": "(a)-[r:KNOWS]->(c)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one preserved row for optional mismatch, got %#v", state.Rows)
		}
		if got := state.Rows[0]["r"]; got != "stale" {
			t.Fatalf("expected edge var r to remain bound on optional mismatch, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["r.id"]; got != "stale" {
			t.Fatalf("expected edge var r.id to remain bound on optional mismatch, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["__edge.id"]; got != nil {
			t.Fatalf("expected __edge.id=nil on optional mismatch, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("optional expand execute failed: %v", err)
	}
}

func TestOptionalExpandHandlerPreservesBoundNodeVariableThroughProjection(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-expand-optional-node-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"id": []byte("2")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		optional := NewOptionalExpandHandler()
		project := NewProjectHandler()
		edge := parseEdgeMutation("(a)-[r]->(other)")
		if edge == nil || edge.LeftVar != "a" || edge.RightVar != "other" || edge.Var != "r" {
			t.Fatalf("unexpected parsed edge mutation: %#v", edge)
		}
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{
			"other":    "b1",
			"other.id": "b1",
		}}}
		if err := optional.Execute("p-optional-node", map[string]any{"pattern": "(a)-[r]->(other)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one preserved row after optional expand, got %#v", state.Rows)
		}
		if got := state.Rows[0]["other"]; got == nil {
			t.Fatalf("expected other to survive optional expand, got %#v", state.Rows[0])
		}
		if err := project.Execute("p-proj-node", map[string]any{"kind": "WITH", "items": []string{"other"}}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one projected row, got %#v", state.Rows)
		}
		if got := state.Rows[0]["other"]; got == nil {
			t.Fatalf("expected other to survive projection, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("optional projection execute failed: %v", err)
	}
}

func TestOptionalVertexExpandNullBindsMissingVariableForSubsequentMatch(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-optional-vertex-null-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a1", DstID: "b1", Type: "T"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		optional := NewOptionalExpandHandler()
		project := NewProjectHandler()
		expand := NewExpandMatchHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := optional.Execute("p-optional-vertex", map[string]any{"pattern": "(a:Missing)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one preserved row after optional vertex miss, got %#v", state.Rows)
		}
		if got := state.Rows[0]["a"]; got != nil {
			t.Fatalf("expected a=nil after optional vertex miss, got %#v", state.Rows[0])
		}
		if err := project.Execute("p-proj-optional-vertex", map[string]any{"kind": "WITH", "items": []string{"a"}}, state); err != nil {
			return err
		}
		if err := expand.Execute("p-match-after-optional-vertex", map[string]any{"pattern": "(a)-->(b)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 0 {
			t.Fatalf("expected no rows when subsequent MATCH consumes nil-bound a, got %#v", state.Rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("optional vertex null-binding execute failed: %v", err)
	}
}

func TestExpandMatchHandlerMaterializesPathForNodesAndRelationshipsFunctions(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-path-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "s", Labels: []string{"SNodes"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("a")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "s", DstID: "a", Type: "RA", Properties: graph.PropertyMap{"name": []byte("a")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		expand := NewExpandMatchHandler()
		project := NewProjectHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}

		if err := expand.Execute("p-match-path", map[string]any{"pattern": "p = (:SNodes)-[*0..1]->(x)"}, state); err != nil {
			return err
		}
		if len(state.Rows) < 2 {
			t.Fatalf("expected at least two path rows (0-hop and 1-hop), got %#v", state.Rows)
		}

		if err := project.Execute("p-proj-nodes", map[string]any{"kind": "WITH", "items": []string{"tail(nodes(p)) AS nodes"}}, state); err != nil {
			return err
		}
		if len(state.Rows) == 0 {
			t.Fatalf("expected rows after nodes projection")
		}
		foundEmpty := false
		foundOne := false
		for _, row := range state.Rows {
			list, ok := row["nodes"].([]any)
			if !ok {
				continue
			}
			if len(list) == 0 {
				foundEmpty = true
			}
			if len(list) == 1 {
				foundOne = true
			}
		}
		if !foundEmpty || !foundOne {
			t.Fatalf("expected both empty and single-element node tails, got %#v", state.Rows)
		}

		state2 := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := expand.Execute("p-match-path-2", map[string]any{"pattern": "p = (:SNodes)-[*0..1]->(x)"}, state2); err != nil {
			return err
		}
		if err := project.Execute("p-proj-rels", map[string]any{"kind": "WITH", "items": []string{"relationships(p) AS relationships"}}, state2); err != nil {
			return err
		}
		seenRel := false
		for _, row := range state2.Rows {
			list, ok := row["relationships"].([]any)
			if !ok {
				continue
			}
			if len(list) == 1 {
				seenRel = true
				break
			}
		}
		if !seenRel {
			t.Fatalf("expected at least one row with a materialized relationship tail, got %#v", state2.Rows)
		}

		return nil
	}); err != nil {
		t.Fatalf("path materialization execute failed: %v", err)
	}
}

func TestExpandMatchHandlerHonorsBoundEdgeSequenceVariable(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-bound-varlen-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a", DstID: "b", Type: "T"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "b", DstID: "c", Type: "T"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		expand := NewExpandMatchHandler()
		project := NewProjectHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := expand.Execute("p-match-bound-varlen-source", map[string]any{"pattern": "()-[r1]->()-[r2]->()"}, state); err != nil {
			return err
		}
		if len(state.Rows) == 0 {
			t.Fatalf("expected rows from two-hop source match")
		}
		if err := project.Execute("p-proj-bound-varlen", map[string]any{"kind": "WITH", "items": []string{"[r1, r2] AS rs"}, "limit": "1"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one projected row, got %#v", state.Rows)
		}
		bound, boundIDs := resolveBoundEdgeSequenceIDs(state.Rows[0], "rs")
		if !bound || !edgeSequenceIDsMatch([]*graph.Edge{{ID: "e1"}, {ID: "e2"}}, boundIDs) {
			t.Fatalf("expected helper to accept the projected rs sequence: bound=%t ids=%#v row=%#v", bound, boundIDs, state.Rows[0]["rs"])
		}
		t.Logf("projected rs type=%T value=%#v", state.Rows[0]["rs"], state.Rows[0]["rs"])
		control := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}
		if err := expand.Execute("p-match-unbound-varlen", map[string]any{"pattern": "(first)-[*]->(second)"}, control); err != nil {
			return err
		}
		t.Logf("unbound varlen row count=%d", len(control.Rows))
		if err := expand.Execute("p-match-bound-varlen", map[string]any{"pattern": "(first)-[rs*]->(second)"}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one bound variable-length match, got %#v", state.Rows)
		}
		if got := state.Rows[0]["first.id"]; got != "a" {
			t.Fatalf("expected first.id=a, got %#v", got)
		}
		if got := state.Rows[0]["second.id"]; got != "c" {
			t.Fatalf("expected second.id=c, got %#v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("bound variable-length execute failed: %v", err)
	}
}

func TestProjectHandlerLastFunctionOverVariableLengthRelationshipVariable(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-last-varlen-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a", DstID: "b", Type: "T", Properties: graph.PropertyMap{}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		expand := NewExpandMatchHandler()
		project := NewProjectHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}}

		if err := expand.Execute("p-match-varlen-r", map[string]any{"pattern": "()-[r*0..1]-()"}, state); err != nil {
			return err
		}
		hasBoundR := false
		hasNonEmptyR := false
		for _, row := range state.Rows {
			if row == nil {
				continue
			}
			if rv, ok := row["r"]; ok {
				hasBoundR = true
				if list, ok := rv.([]any); ok && len(list) > 0 {
					hasNonEmptyR = true
				}
			}
		}
		if !hasBoundR {
			t.Fatalf("expected variable-length expansion to bind r, got %#v", state.Rows)
		}
		if !hasNonEmptyR {
			t.Fatalf("expected at least one expanded row with non-empty r list, got %#v", state.Rows)
		}
		if err := project.Execute("p-proj-last", map[string]any{"kind": "RETURN", "items": []string{"last(r) AS l"}}, state); err != nil {
			return err
		}

		hasRelationship := false
		for _, row := range state.Rows {
			if row == nil {
				continue
			}
			if row["l"] == nil {
				continue
			}
			hasRelationship = true
			switch typed := row["l"].(type) {
			case *graph.Edge:
				if typed == nil || typed.Type != "T" {
					t.Fatalf("unexpected edge from last(r): %#v", row["l"])
				}
			case map[string]any:
				if relType, _ := typed["type"].(string); relType != "T" {
					t.Fatalf("unexpected relationship map from last(r): %#v", row["l"])
				}
			default:
				t.Fatalf("unexpected value type from last(r): %T %#v", row["l"], row["l"])
			}
		}
		if !hasRelationship {
			t.Fatalf("expected at least one non-null last(r) value, got %#v", state.Rows)
		}

		return nil
	}); err != nil {
		t.Fatalf("var-length last(r) evaluation failed: %v", err)
	}
}

func TestProjectionPathValueRendersIncomingDirection(t *testing.T) {
	p := projectionPathValue{
		nodes: []any{
			&graph.Vertex{ID: "liker"},
			&graph.Vertex{ID: "peer"},
		},
		relationships: []any{
			&graph.Edge{ID: "e1", SrcID: "peer", DstID: "liker", Type: "T"},
		},
	}
	if got := p.String(); got != "<()-[:T]-()>" && !strings.Contains(got, "<-[:T]-") {
		t.Fatalf("expected incoming-direction rendering, got %q", got)
	}
}

func TestResolveWritePropertyBindingsResolvesDottedMapPath(t *testing.T) {
	bindings := map[string]any{
		"event": map[string]any{"id": 1, "year": 2016},
	}
	props := resolveWritePropertyBindings("(e:Event {id: event.id, year: event.year})", nil, bindings)
	if props == nil {
		t.Fatalf("expected properties map")
	}
	if got := props["id"]; got != 1 {
		t.Fatalf("expected id=1, got %#v", got)
	}
	if got := props["year"]; got != 2016 {
		t.Fatalf("expected year=2016, got %#v", got)
	}
}

func TestWriteHandlerSeedsRowsWhenEmpty(t *testing.T) {
	h := NewWriteHandler()
	state := &State{MaterializeWriteBindings: true}

	err := h.Execute("p3", map[string]any{"kind": "CREATE"}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if len(state.Rows) != 1 {
		t.Fatalf("expected seeded row for write path, got %#v", state.Rows)
	}
	if got, ok := state.Rows[0]["__write_event_count"].(int); !ok || got != 1 {
		t.Fatalf("expected seeded row write marker=1, got %#v", state.Rows[0])
	}
	event := state.WriteEvents[0]
	if event.MutationType != MutationTypeUnknown {
		t.Fatalf("expected unknown mutation type for bare CREATE kind, got %#v", event)
	}
}

func TestWriteHandlerEmitsOneEventPerInputRow(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows: []map[string]any{
			{"a": "u1"},
			{"a": "u2"},
		},
		MaterializeWriteBindings: false,
	}

	err := h.Execute("p3b", map[string]any{
		"kind":    "MERGE",
		"pattern": "(a)-[:KNOWS]->(:User)",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if len(state.WriteEvents) != 2 {
		t.Fatalf("expected one write event per row, got %#v", state.WriteEvents)
	}
	if got := state.WriteEvents[0].Bindings["a"]; got != "u1" {
		t.Fatalf("expected first row binding a=u1, got %#v", state.WriteEvents[0].Bindings)
	}
	if got := state.WriteEvents[1].Bindings["a"]; got != "u2" {
		t.Fatalf("expected second row binding a=u2, got %#v", state.WriteEvents[1].Bindings)
	}
}

func TestWriteHandlerBuildsVertexMutationPayload(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"id": "u1"}, MaterializeWriteBindings: true}

	err := h.Execute("p4", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (:User:Admin {id:$id})",
		"mergePattern": "(:User:Admin {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	event := state.WriteEvents[0]
	if event.MutationType != MutationTypeVertex {
		t.Fatalf("expected vertex mutation type, got %#v", event)
	}
	if event.Vertex == nil {
		t.Fatalf("expected vertex mutation payload, got %#v", event)
	}
	if event.Vertex.IDParam != "id" {
		t.Fatalf("expected vertex id param hint, got %#v", event.Vertex)
	}
	if len(event.Vertex.Labels) != 2 || event.Vertex.Labels[0] != "User" || event.Vertex.Labels[1] != "Admin" {
		t.Fatalf("unexpected vertex labels: %#v", event.Vertex)
	}
	if event.Edge != nil {
		t.Fatalf("did not expect edge payload on vertex mutation, got %#v", event)
	}
	if got := event.ResolvedParams["id"]; got != "u1" {
		t.Fatalf("expected resolved id param, got %#v", event.ResolvedParams)
	}
}

func TestWriteHandlerPrefersExplicitPatternOverRawForMutationPayload(t *testing.T) {
	h := NewWriteHandler()
	state := &State{MaterializeWriteBindings: true}

	err := h.Execute("p4b", map[string]any{
		"kind":    "CREATE",
		"raw":     "MATCH (u) CREATE (u)-[:KNOWS]->(v) RETURN v",
		"pattern": "(u)-[:KNOWS]->(v)",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	event := state.WriteEvents[0]
	if event.Pattern != "(u)-[:KNOWS]->(v)" {
		t.Fatalf("expected explicit event pattern, got %#v", event)
	}
	if event.MutationType != MutationTypeEdge {
		t.Fatalf("expected edge mutation classification, got %#v", event)
	}
	if event.Edge == nil || event.Edge.LeftVar != "u" || event.Edge.RightVar != "v" {
		t.Fatalf("expected edge mutation payload from explicit pattern, got %#v", event.Edge)
	}
}

func TestWriteHandlerBindsAnonymousMergeVarFromRow(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows:                     []map[string]any{{"a": "u1", "a.id": "u1"}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p4c", map[string]any{
		"kind":    "MERGE",
		"pattern": "(x)",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["x"]; got != "u1" {
		t.Fatalf("expected anonymous MERGE variable x to bind from row, got %#v", state.Rows[0])
	}
}

func TestWriteHandlerMaterializesVertexVariableBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"id": "u7"}, MaterializeWriteBindings: true}

	err := h.Execute("p5", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$id})",
		"mergePattern": "(u:User {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u7" {
		t.Fatalf("expected variable binding u=u7, got %#v", row)
	}
	if got := row["u.id"]; got != "u7" {
		t.Fatalf("expected dotted id binding u.id=u7, got %#v", row)
	}
}

func resolveBoundEdgeSequenceIDs(row map[string]any, variable string) (bool, []string) {
	variable = strings.TrimSpace(variable)
	if row == nil || variable == "" {
		return false, nil
	}
	value, ok := row[variable]
	if !ok {
		value, ok = row[variable+".id"]
		if !ok {
			return false, nil
		}
	}
	switch typed := value.(type) {
	case []*graph.Edge:
		ids := make([]string, 0, len(typed))
		for _, edge := range typed {
			if edge == nil {
				return false, nil
			}
			ids = append(ids, strings.TrimSpace(edge.ID))
		}
		return true, ids
	case []any:
		ids := make([]string, 0, len(typed))
		for _, item := range typed {
			switch edge := item.(type) {
			case *graph.Edge:
				if edge == nil {
					return false, nil
				}
				ids = append(ids, strings.TrimSpace(edge.ID))
			case map[string]any:
				ids = append(ids, strings.TrimSpace(fmt.Sprint(edge["id"])))
			default:
				ids = append(ids, strings.TrimSpace(fmt.Sprint(item)))
			}
		}
		return true, ids
	case []string:
		ids := make([]string, 0, len(typed))
		for _, id := range typed {
			ids = append(ids, strings.TrimSpace(id))
		}
		return true, ids
	case *graph.Edge:
		if typed == nil {
			return false, nil
		}
		return true, []string{strings.TrimSpace(typed.ID)}
	case map[string]any:
		if id := strings.TrimSpace(fmt.Sprint(typed["id"])); id != "" {
			return true, []string{id}
		}
		return false, nil
	default:
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			return true, []string{id}
		}
		return false, nil
	}
}

func edgeSequenceIDsMatch(edges []*graph.Edge, boundIDs []string) bool {
	if len(edges) != len(boundIDs) {
		return false
	}
	for i, edge := range edges {
		if edge == nil {
			return false
		}
		if strings.TrimSpace(edge.ID) != strings.TrimSpace(boundIDs[i]) {
			return false
		}
	}
	return true
}

func TestWriteHandlerMaterializesPropertyFromBoundVariable(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Rows: []map[string]any{{"x": 42}}, MaterializeWriteBindings: true}

	err := h.Execute("p5x", map[string]any{
		"kind": "CREATE",
		"raw":  "CREATE (n:N {num: x})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["n.num"]; got != 42 {
		t.Fatalf("expected n.num to resolve from bound variable x=42, got %#v", state.Rows[0])
	}
}

func TestWriteHandlerMaterializesEdgeEndpointBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u8", "dst": "u9"}, MaterializeWriteBindings: true}

	err := h.Execute("p6", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})-[:KNOWS]->(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u8" {
		t.Fatalf("expected left endpoint variable binding u=u8, got %#v", row)
	}
	if got := row["u.id"]; got != "u8" {
		t.Fatalf("expected left dotted id binding u.id=u8, got %#v", row)
	}
	if got := row["v"]; got != "u9" {
		t.Fatalf("expected right endpoint variable binding v=u9, got %#v", row)
	}
	if got := row["v.id"]; got != "u9" {
		t.Fatalf("expected right dotted id binding v.id=u9, got %#v", row)
	}
}

func TestWriteHandlerMaterializesReverseEdgeEndpointBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u14", "dst": "u15"}, MaterializeWriteBindings: true}

	err := h.Execute("p6r", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u14" {
		t.Fatalf("expected left endpoint variable binding u=u14, got %#v", row)
	}
	if got := row["u.id"]; got != "u14" {
		t.Fatalf("expected left dotted id binding u.id=u14, got %#v", row)
	}
	if got := row["v"]; got != "u15" {
		t.Fatalf("expected right endpoint variable binding v=u15, got %#v", row)
	}
	if got := row["v.id"]; got != "u15" {
		t.Fatalf("expected right dotted id binding v.id=u15, got %#v", row)
	}
}

func TestWriteHandlerMaterializesMergeVertexPathBinding(t *testing.T) {
	h := NewWriteHandler()
	state := &State{MaterializeWriteBindings: true}

	err := h.Execute("p6mv", map[string]any{
		"kind":         "MERGE",
		"raw":          "MERGE p = (a {num: 1})",
		"mergePattern": "p = (a {num: 1})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	pathValue, ok := state.Rows[0]["p"].(projectionPathValue)
	if !ok {
		t.Fatalf("expected projection path binding for p, got %#v", state.Rows[0]["p"])
	}
	if got := pathValue.String(); got != "<({num: 1})>" {
		t.Fatalf("expected MERGE path rendering <({num: 1})>, got %s", got)
	}
}

func TestWriteHandlerMaterializesMergeEdgePathBinding(t *testing.T) {
	h := NewWriteHandler()
	state := &State{MaterializeWriteBindings: true}

	err := h.Execute("p6me", map[string]any{
		"kind":         "MERGE",
		"raw":          "MERGE p = (a {num: 1})-[:R]->(b {num: 2})",
		"mergePattern": "p = (a {num: 1})-[:R]->(b {num: 2})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	pathValue, ok := state.Rows[0]["p"].(projectionPathValue)
	if !ok {
		t.Fatalf("expected projection path binding for p, got %#v", state.Rows[0]["p"])
	}
	if got := pathValue.String(); got != "<({num: 1})-[:R]->({num: 2})>" {
		t.Fatalf("expected MERGE path rendering <({num: 1})-[:R]->({num: 2})>, got %s", got)
	}
}

func TestWriteHandlerMaterializesMergeEdgeBindingForAllMatchedEdges(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-ops-merge-edge-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "edge-1", Type: "R", SrcID: "a1", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "edge-2", Type: "R", SrcID: "a1", DstID: "b1"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	h := NewWriteHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{
			Tx:                       tx,
			Tenant:                   "acme",
			Rows:                     []map[string]any{{"a": "a1", "b": "b1"}},
			MaterializeWriteBindings: true,
		}
		if err := h.Execute("p6me-single", map[string]any{
			"kind":         "MERGE",
			"raw":          "MERGE (a)-[r:R]->(b)",
			"mergePattern": "(a)-[r:R]->(b)",
		}, state); err != nil {
			return err
		}
		if len(state.Rows) != 2 {
			t.Fatalf("expected two materialized rows, got %#v", state.Rows)
		}
		seen := map[string]struct{}{}
		for _, outRow := range state.Rows {
			edgeID := strings.TrimSpace(fmt.Sprint(outRow["r.id"]))
			if edgeID == "" {
				t.Fatalf("expected r.id to bind a stored edge id, got row %#v", outRow)
			}
			if edgeID != "edge-1" && edgeID != "edge-2" {
				t.Fatalf("expected r.id to bind a stored edge id, got %q in row %#v", edgeID, outRow)
			}
			seen[edgeID] = struct{}{}
		}
		if len(seen) != 2 {
			t.Fatalf("expected two distinct matched edge ids, got %#v", seen)
		}
		return nil
	}); err != nil {
		t.Fatalf("merge materialization failed: %v", err)
	}
}

func TestWriteHandlerSkipsMaterializationWhenDisabled(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Params:                   map[string]any{"id": "u21"},
		MaterializeWriteBindings: false,
	}

	err := h.Execute("p9", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$id})",
		"mergePattern": "(u:User {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 0 {
		t.Fatalf("expected no row materialization when disabled, got %#v", state.Rows)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}
}

func TestFilterRowsByProjectionPredicateSupportsDisjunctivePatternPredicates(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-pattern-predicate-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "a0", DstID: "b1", Type: "T"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "b1", DstID: "c2", Type: "T"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		rows := []map[string]any{
			{"a": "a0", "b": "b1"},
			{"a": "a0", "b": "c2"},
			{"a": "b1", "b": "c2"},
		}

		filtered, err := filterRowsByProjectionPredicate(rows, "a.id = 0 AND (a)-[:T]->(b:TheLabel) OR (a)-[:T*]->(b:MissingLabel)", state)
		if err != nil {
			t.Fatalf("filter rows failed: %v", err)
		}
		if len(filtered) != 1 {
			t.Fatalf("expected one matching row, got %#v", filtered)
		}
		if got := filtered[0]["b"]; got != "b1" {
			t.Fatalf("expected b1 row to match, got %#v", filtered[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValuePrefersPathOverSyntheticDottedBinding(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-path-priority-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-a", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		row := map[string]any{"a": "v-a", "a.id": "v-a"}
		value, ok, err := resolveProjectionExprValueChecked("a.id", row, state)
		if err != nil {
			t.Fatalf("resolve projection expr failed: %v", err)
		}
		if !ok {
			t.Fatalf("expected expression to resolve, got ok=false")
		}
		if _, stringID := value.(string); stringID {
			t.Fatalf("expected typed property id, got string %#v", value)
		}
		if got := fmt.Sprint(value); got != "0" {
			t.Fatalf("expected property id 0, got %v", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveWritePropertyBindingsPreservesLiteralZeroIdProperty(t *testing.T) {
	props := resolveWritePropertyBindingsState("(:End {num: 42, id: 0})", nil, map[string]any{}, nil)
	if got := fmt.Sprint(props["num"]); got != "42" {
		t.Fatalf("expected num property 42, got %#v", props)
	}
	if got := fmt.Sprint(props["id"]); got != "0" {
		t.Fatalf("expected literal id property 0, got %#v", props)
	}
}

func TestResolveProjectionExprValueMissingIDPropertyReturnsNull(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-proj-missing-id-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-a", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"name": []byte("neo")}})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		row := map[string]any{"a": "v-a", "a.id": "v-a"}
		value, ok, err := resolveProjectionExprValueChecked("a.id", row, state)
		if err != nil {
			t.Fatalf("resolve projection expr failed: %v", err)
		}
		if !ok {
			t.Fatalf("expected expression to resolve, got ok=false")
		}
		if value != nil {
			t.Fatalf("expected missing property id to resolve to null, got %#v", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionPatternPredicateExprValueDirected(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-pattern-expr-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a0", DstID: "b1"}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		row := map[string]any{"a": "a0", "a.id": "a0", "b": "b1", "b.id": "b1"}
		value, ok := resolveProjectionPatternPredicateExprValue("(a)-[:T]->(b:TheLabel)", row, state)
		if !ok {
			t.Fatalf("expected predicate expression to be recognized")
		}
		matched, boolOK := value.(bool)
		if !boolOK || !matched {
			t.Fatalf("expected predicate to evaluate true, got %#v", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValueDisjunctivePatternPredicate(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-disj-pattern-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a0", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T", SrcID: "b1", DstID: "c2"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		row := map[string]any{"a": "a0", "a.id": "a0", "b": "b1", "b.id": "b1"}
		value, ok, err := resolveProjectionExprValueChecked("a.id = 0 AND (a)-[:T]->(b:TheLabel) OR (a)-[:T*]->(b:MissingLabel)", row, state)
		if err != nil {
			t.Fatalf("resolve projection expr failed: %v", err)
		}
		if !ok {
			t.Fatalf("expected expression to resolve")
		}
		matched, boolOK := value.(bool)
		if !boolOK || !matched {
			t.Fatalf("expected disjunctive expression true, got %#v", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestProjectHandlerWithWhereDisjunctivePatternPredicate(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-with-where-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a0", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T", SrcID: "b1", DstID: "c2"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{"a": "a0", "b": "b1"}}}
		h := NewProjectHandler()
		if err := h.Execute("p", map[string]any{
			"kind":  "WITH",
			"items": []string{"a", "b"},
			"where": "a.id = 0\n  AND (a)-[:T]->(b:TheLabel)\n  OR (a)-[:T*]->(b:MissingLabel)",
		}, state); err != nil {
			return err
		}
		if len(state.Rows) != 1 {
			t.Fatalf("expected one row after WITH WHERE filter, got %#v", state.Rows)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValueDisjunctivePatternPredicateWithEntityMaps(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-disj-map-row-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "0", DstID: "1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T", SrcID: "1", DstID: "2"})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme"}
		row := map[string]any{
			"a":    map[string]any{"id": "0", "labels": []string{"TheLabel"}, "properties": map[string]any{"id": json.Number("0")}, "tenant": "acme"},
			"a.id": "0",
			"b":    map[string]any{"id": "1", "labels": []string{"TheLabel"}, "properties": map[string]any{"id": json.Number("1")}, "tenant": "acme"},
			"b.id": "1",
		}
		if v, ok, err := resolveProjectionExprValueChecked("a.id", row, state); err != nil || !ok {
			t.Fatalf("expected a.id to resolve, got value=%#v ok=%v err=%v", v, ok, err)
		} else {
			t.Logf("resolved a.id => %#v (%T)", v, v)
		}
		if v, ok, err := resolveProjectionExprValueChecked("a.id = 0", row, state); err != nil || !ok || v != true {
			t.Fatalf("expected a.id = 0 true, got value=%#v ok=%v err=%v", v, ok, err)
		}
		if v, ok, err := resolveProjectionExprValueChecked("(a)-[:T]->(b:TheLabel)", row, state); err != nil || !ok || v != true {
			t.Fatalf("expected directed pattern predicate true, got value=%#v ok=%v err=%v", v, ok, err)
		}
		value, ok, err := resolveProjectionExprValueChecked("a.id = 0 AND (a)-[:T]->(b:TheLabel) OR (a)-[:T*]->(b:MissingLabel)", row, state)
		if err != nil {
			t.Fatalf("resolve projection expr failed: %v", err)
		}
		if !ok {
			t.Fatalf("expected expression to resolve")
		}
		matched, boolOK := value.(bool)
		if !boolOK || !matched {
			t.Fatalf("expected disjunctive expression true for entity map rows, got %#v", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestParseProjectWhereExprFromAttrsFallsBackForPatternPredicates(t *testing.T) {
	expr, has, err := parseProjectWhereExprFromAttrs(map[string]any{
		"where": "a.id = 0\n  AND (a)-[:T]->(b:TheLabel)\n  OR (a)-[:T*]->(b:MissingLabel)",
	})
	if err != nil {
		t.Fatalf("parseProjectWhereExprFromAttrs failed: %v", err)
	}
	if !has {
		t.Fatalf("expected where expression to be present")
	}
	if !expr.useRawExpr {
		t.Fatalf("expected raw-expression fallback for pattern predicate WHERE")
	}
}

func TestWriteHandlerBuildsReverseEdgeMutationPayload(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u10", "dst": "u11"}, MaterializeWriteBindings: true}

	err := h.Execute("p8", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}
	event := state.WriteEvents[0]
	if event.Edge == nil {
		t.Fatalf("expected edge mutation payload, got %#v", event)
	}
	if !event.Edge.Reverse {
		t.Fatalf("expected reverse edge mutation, got %#v", event.Edge)
	}
	if event.Edge.LeftVar != "u" || event.Edge.RightVar != "v" {
		t.Fatalf("unexpected endpoint variables in edge payload: %#v", event.Edge)
	}
}

func TestWriteHandlerMaterializesSetLabelBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows:                     []map[string]any{{"n": "v1"}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p10", map[string]any{
		"kind": "SET",
		"raw":  "SET n:Foo:Bar",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	labels, ok := state.Rows[0]["n.labels"].([]string)
	if !ok {
		t.Fatalf("expected n.labels materialized as []string, got %#v", state.Rows[0]["n.labels"])
	}
	have := map[string]struct{}{}
	for _, label := range labels {
		have[label] = struct{}{}
	}
	if _, ok := have["Foo"]; !ok {
		t.Fatalf("expected Foo label in %#v", labels)
	}
	if _, ok := have["Bar"]; !ok {
		t.Fatalf("expected Bar label in %#v", labels)
	}
}

func TestWriteHandlerMaterializesSetPropertyBindingsOnMapEntity(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows: []map[string]any{{
			"n": map[string]any{
				"id":         "v1",
				"labels":     []string{"A"},
				"properties": map[string]any{"name": "Andres"},
			},
			"n.id": "v1",
		}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p11", map[string]any{
		"kind": "SET",
		"raw":  "SET n.name = 'Michael'",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if got := fmt.Sprint(state.Rows[0]["n.name"]); got != "Michael" {
		t.Fatalf("expected n.name binding Michael, got %#v", state.Rows[0])
	}
	entity, ok := state.Rows[0]["n"].(map[string]any)
	if !ok {
		t.Fatalf("expected map-backed entity binding, got %#v", state.Rows[0]["n"])
	}
	props, _ := entity["properties"].(map[string]any)
	if got := fmt.Sprint(props["name"]); got != "Michael" {
		t.Fatalf("expected entity property Michael, got %#v", entity)
	}
}

func TestWriteHandlerMaterializesSetMapReplaceBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows: []map[string]any{{
			"n": map[string]any{
				"id":         "v1",
				"labels":     []string{"A"},
				"properties": map[string]any{"name": "Andres", "name2": "X"},
			},
			"n.id":    "v1",
			"n.name":  "Andres",
			"n.name2": "X",
		}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p11-map-replace", map[string]any{
		"kind": "SET",
		"raw":  "SET n = {name: 'B', baz: 'C'}",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	row := state.Rows[0]
	if got := fmt.Sprint(row["n.name"]); got != "B" {
		t.Fatalf("expected n.name=B after map replace, got %#v", row)
	}
	if got := fmt.Sprint(row["n.baz"]); got != "C" {
		t.Fatalf("expected n.baz=C after map replace, got %#v", row)
	}
	if value, exists := row["n.name2"]; !exists || value != nil {
		t.Fatalf("expected n.name2 tombstone nil after map replace, got %#v", row)
	}
}

func TestWriteHandlerMaterializesSetMapAppendBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows: []map[string]any{{
			"n": map[string]any{
				"id":         "v1",
				"labels":     []string{"A"},
				"properties": map[string]any{"name": "Andres", "name2": "X"},
			},
			"n.id":    "v1",
			"n.name":  "Andres",
			"n.name2": "X",
		}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p11-map-append", map[string]any{
		"kind": "SET",
		"raw":  "SET n += {name: null, baz: 'C'}",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	row := state.Rows[0]
	if value, exists := row["n.name"]; !exists || value != nil {
		t.Fatalf("expected n.name tombstone nil after map append null, got %#v", row)
	}
	if got := fmt.Sprint(row["n.name2"]); got != "X" {
		t.Fatalf("expected n.name2 retained after map append, got %#v", row)
	}
	if got := fmt.Sprint(row["n.baz"]); got != "C" {
		t.Fatalf("expected n.baz=C after map append, got %#v", row)
	}
}

func TestWriteHandlerMaterializesMergeOnCreateSetBindings(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-merge-on-create-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.View(ctx, func(tx graph.Tx) error {
		h := NewWriteHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}, MaterializeWriteBindings: true}
		return h.Execute("p-merge-create-set", map[string]any{
			"kind":          "MERGE",
			"pattern":       "(a:TheLabel)",
			"mergePattern":  "(a:TheLabel)",
			"mergeOnCreate": []string{"SET a:Foo", "SET a.num = 42"},
		}, state)
	}); err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	row := map[string]any{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		h := NewWriteHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}, MaterializeWriteBindings: true}
		if err := h.Execute("p-merge-create-set", map[string]any{
			"kind":          "MERGE",
			"pattern":       "(a:TheLabel)",
			"mergePattern":  "(a:TheLabel)",
			"mergeOnCreate": []string{"SET a:Foo", "SET a.num = 42"},
		}, state); err != nil {
			return err
		}
		row = state.Rows[0]
		return nil
	}); err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	labels := writePathRowLabels(row, "a")
	if !reflect.DeepEqual(labels, []string{"TheLabel", "Foo"}) {
		t.Fatalf("expected materialized labels [TheLabel Foo], got %#v", labels)
	}
	if got := row["a.num"]; got != 42 {
		t.Fatalf("expected materialized a.num=42, got %#v", row)
	}
}

func TestWriteHandlerMaterializesMergeExistingVertexSemanticID(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-merge-existing-id-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "acme",
			ID:     "auto-v-1",
			Labels: []string{"TheLabel"},
			Properties: graph.PropertyMap{
				"id": []byte("0"),
			},
		})
	}); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	row := map[string]any{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		h := NewWriteHandler()
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{}}, MaterializeWriteBindings: true}
		if err := h.Execute("p-merge-existing-id", map[string]any{
			"kind":         "MERGE",
			"pattern":      "(a:TheLabel)",
			"mergePattern": "(a:TheLabel)",
		}, state); err != nil {
			return err
		}
		row = state.Rows[0]
		return nil
	}); err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if got := fmt.Sprint(row["a.id"]); got != "0" {
		t.Fatalf("expected materialized semantic property a.id=0, got %#v", row["a.id"])
	}
}

func TestWriteHandlerMaterializesRemoveBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows: []map[string]any{{
			"n": map[string]any{
				"id":         "v1",
				"labels":     []string{"Foo", "Bar"},
				"properties": map[string]any{"name": "Andres", "num": 7},
			},
			"n.id":     "v1",
			"n.labels": []string{"Foo", "Bar"},
			"n.name":   "Andres",
			"n.num":    7,
		}},
		MaterializeWriteBindings: true,
	}

	err := h.Execute("p-remove-bindings", map[string]any{
		"kind": "REMOVE",
		"raw":  "REMOVE n.num, n:Foo",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	row := state.Rows[0]
	if value, exists := row["n.num"]; !exists || value != nil {
		t.Fatalf("expected n.num tombstone nil after REMOVE, got %#v", row)
	}
	labels := writePathRowLabels(row, "n")
	if !reflect.DeepEqual(labels, []string{"Bar"}) {
		t.Fatalf("expected labels [Bar] after REMOVE, got %#v", labels)
	}
}

func TestProjectHandlerDistinct(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"uid": "u1", "vid": "u2"},
		{"uid": "u1", "vid": "u2"},
		{"uid": "u1", "vid": "u3"},
	}}

	err := h.Execute("p7", map[string]any{
		"items":    []string{"uid", "vid"},
		"distinct": true,
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 2 {
		t.Fatalf("expected deduplicated rows, got %#v", state.Rows)
	}
	if state.Rows[0]["uid"] != "u1" || state.Rows[0]["vid"] != "u2" {
		t.Fatalf("unexpected first distinct row: %#v", state.Rows[0])
	}
	if state.Rows[1]["uid"] != "u1" || state.Rows[1]["vid"] != "u3" {
		t.Fatalf("unexpected second distinct row: %#v", state.Rows[1])
	}
}

func TestAntiProbeHandlerUsesRowProbeForLowVariant(t *testing.T) {
	h := NewAntiProbeHandler()
	tx := &antiProbeCountingTx{}
	state := &State{
		Tx:     tx,
		Tenant: "acme",
		Rows:   []map[string]any{{"u": "u1", "v": "u2"}},
	}

	err := h.Execute("p1", map[string]any{
		"leftVar":  "u",
		"rightVar": "v",
		"edgeType": "BLOCKED",
		"mode":     "directed",
		"variant":  "anti_probe_row_low",
	}, state)
	if err != nil {
		t.Fatalf("anti-probe execute failed: %v", err)
	}
	if tx.rowDirectedCalls == 0 {
		t.Fatalf("expected row probe path to call HasDirectedEdgeBetween")
	}
	if tx.batchDirectedCalls != 0 {
		t.Fatalf("expected row probe path to avoid batch probes")
	}
}

func TestAntiProbeHandlerUsesBatchProbeForHighVariant(t *testing.T) {
	h := NewAntiProbeHandler()
	tx := &antiProbeCountingTx{}
	state := &State{
		Tx:     tx,
		Tenant: "acme",
		Rows:   []map[string]any{{"u": "u1", "v": "u2"}},
	}

	err := h.Execute("p1", map[string]any{
		"leftVar":  "u",
		"rightVar": "v",
		"edgeType": "BLOCKED",
		"mode":     "directed",
		"variant":  "anti_probe_batch_high",
	}, state)
	if err != nil {
		t.Fatalf("anti-probe execute failed: %v", err)
	}
	if tx.batchDirectedCalls == 0 {
		t.Fatalf("expected batch probe path to call BatchHasDirectedEdgeBetween")
	}
	if tx.rowDirectedCalls != 0 {
		t.Fatalf("expected batch probe path to avoid row probes")
	}
}

func TestProjectHandlerResolvesParamAndNullExpressions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{
		Rows:   []map[string]any{{"uid": "u1"}},
		Params: map[string]any{"src": "u1"},
	}

	err := h.Execute("p9", map[string]any{
		"items": []string{"null AS n", "$src AS s", "uid AS u"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["n"]; got != nil {
		t.Fatalf("expected null projection for n, got %#v", row)
	}
	if got := row["s"]; got != "u1" {
		t.Fatalf("expected parameter projection s=u1, got %#v", row)
	}
	if got := row["u"]; got != "u1" {
		t.Fatalf("expected row projection u=u1, got %#v", row)
	}
}

func TestProjectHandlerResolvesQuotedAndNumericLiteralExpressions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p10", map[string]any{
		"items": []string{"'a''b' AS s", "42 AS i", "3.5 AS f", "0x1A AS hx", "0o12 AS oc", "'\\u01FF' AS u"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["s"]; got != "a'b" {
		t.Fatalf("expected escaped single-quote literal for s, got %#v", row)
	}
	if got := row["i"]; got != 42 {
		t.Fatalf("expected integer literal i=42, got %#v", row)
	}
	if got := row["f"]; got != float64(3.5) {
		t.Fatalf("expected float literal f=3.5, got %#v", row)
	}
	if got := row["hx"]; got != 26 {
		t.Fatalf("expected hexadecimal literal hx=26, got %#v", row)
	}
	if got := row["oc"]; got != 10 {
		t.Fatalf("expected octal literal oc=10, got %#v", row)
	}
	if got := row["u"]; got != "\u01FF" {
		t.Fatalf("expected unicode escape literal u=ǿ, got %#v", row)
	}
}

func TestProjectHandlerEvaluatesSubstringAndSplitFunctions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p10-string-fns", map[string]any{
		"items": []string{"substring('0123456789', 1) AS sub", "split('one1two', '1') AS parts"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["sub"]; got != "123456789" {
		t.Fatalf("expected substring result 123456789, got %#v", row)
	}
	parts, ok := row["parts"].([]any)
	if !ok {
		t.Fatalf("expected split result list, got %#v", row)
	}
	if !reflect.DeepEqual(parts, []any{"one", "two"}) {
		t.Fatalf("unexpected split result: %#v", parts)
	}
}

func TestProjectHandlerIdentifierProjectionPreservesDottedBindings(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"n": "v1", "n.id": "v1", "n.num": 42}}}

	err := h.Execute("p10-id-proj", map[string]any{"items": []string{"n"}}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["n.num"]; got != 42 {
		t.Fatalf("expected dotted binding n.num to be preserved, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerStarProjectionExcludesDottedAndInternalKeys(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"a":         "v1",
		"a.id":      "v1",
		"b.id":      "v2",
		"__edge.id": "e1",
	}}}

	err := h.Execute("p10-star-clean", map[string]any{"items": []string{"*"}}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if _, ok := row["a.id"]; ok {
		t.Fatalf("expected dotted key a.id to be excluded, got %#v", row)
	}
	if _, ok := row["__edge.id"]; ok {
		t.Fatalf("expected internal key __edge.id to be excluded, got %#v", row)
	}
	a, ok := row["a"].(map[string]any)
	if !ok || a["id"] != "v1" {
		t.Fatalf("expected hydrated a binding, got %#v", row)
	}
	b, ok := row["b"].(map[string]any)
	if !ok || b["id"] != "v2" {
		t.Fatalf("expected hydrated b binding reconstructed from b.id, got %#v", row)
	}
}

func TestProjectHandlerEvaluatesCountAggregateProjection(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"a": "u1"}, {"a": nil}, {"a": "u2"}}}

	err := h.Execute("p10b", map[string]any{
		"items": []string{"count(a) AS c", "count(*) AS all"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one aggregate row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["c"]; got != 2 {
		t.Fatalf("expected count(a)=2, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["all"]; got != 3 {
		t.Fatalf("expected count(*)=3, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerEvaluatesCountDistinctAggregateProjection(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"r": "e1"}, {"r": "e1"}, {"r": "e2"}, {"r": nil}}}

	err := h.Execute("p10b-distinct", map[string]any{
		"items": []string{"count(DISTINCT r) AS c", "collect(DISTINCT r) AS ids"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one aggregate row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["c"]; got != 2 {
		t.Fatalf("expected count(DISTINCT r)=2, got %#v", state.Rows[0])
	}
	ids, ok := state.Rows[0]["ids"].([]any)
	if !ok || len(ids) != 2 {
		t.Fatalf("expected collect(DISTINCT r) length 2, got %#v", state.Rows[0]["ids"])
	}
}

func TestProjectHandlerPureCollectOnFirstClauseUsesImplicitSingleRow(t *testing.T) {
	h := NewProjectHandler()
	state := &State{}

	err := h.Execute("p10b-collect-implicit-row", map[string]any{
		"items": []string{"collect([0, 0.0]) AS numbers"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one aggregate row, got %#v", state.Rows)
	}
	numbers, ok := state.Rows[0]["numbers"].([]any)
	if !ok || len(numbers) != 1 {
		t.Fatalf("expected collect([0,0.0]) to produce one item, got %#v", state.Rows[0]["numbers"])
	}
	inner, ok := numbers[0].([]any)
	if !ok || len(inner) != 2 {
		t.Fatalf("expected collected item to remain [0,0.0], got %#v", numbers)
	}
	if fmt.Sprint(inner[0]) != "0" || fmt.Sprint(inner[1]) != "0" {
		t.Fatalf("expected numeric values [0,0.0], got %#v", inner)
	}
}

func TestProjectHandlerEvaluatesAggregateExpressionsInsideFunctionsAndArithmetic(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"a": "u1"}, {"a": nil}, {"a": "u2"}}}

	err := h.Execute("p10b-agg-expr", map[string]any{
		"items": []string{"size(collect(a)) AS s", "count(*) * 10 AS c"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one aggregate row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["s"]; got != 2 {
		t.Fatalf("expected size(collect(a))=2, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["c"]; got != 30 {
		t.Fatalf("expected count(*)*10=30, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerAggregateExpressionOnEmptyInputStillProjectsSingleRow(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Params: map[string]any{"age": 38}}

	err := h.Execute("p10b-agg-empty", map[string]any{
		"items": []string{"$age + avg(person.age) - 1000 AS agg"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row for aggregate over empty input, got %#v", state.Rows)
	}
	if got := state.Rows[0]["agg"]; got != nil {
		t.Fatalf("expected null aggregate expression over empty input, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerEvaluatesPercentileAggregates(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"price": float64(10)},
		{"price": float64(20)},
		{"price": float64(30)},
	}}

	err := h.Execute("p10b-percentile", map[string]any{
		"items": []string{"percentileDisc(price, 0.5) AS d", "percentileCont(price, 0.5) AS c"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one aggregate row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["d"]; got != float64(20) {
		t.Fatalf("expected percentileDisc(price,0.5)=20.0, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["c"]; got != float64(20) {
		t.Fatalf("expected percentileCont(price,0.5)=20.0, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerPercentileAggregatesRejectOutOfRangePercentile(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"price": float64(10), "p": 1.1}}}

	err := h.Execute("p10b-percentile-range", map[string]any{
		"items": []string{"percentileDisc(price, p) AS d"},
	}, state)
	if err == nil {
		t.Fatalf("expected NumberOutOfRange error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "numberoutofrange") {
		t.Fatalf("expected NumberOutOfRange error, got %v", err)
	}
}

func TestProjectHandlerResolvesStartAndEndNodeFunctions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"r": "acme|2|KNOWS|1"}}}

	err := h.Execute("p10c", map[string]any{
		"items": []string{"startNode(r).id AS s", "endNode(r).id AS e"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["s"]; got != 2 {
		t.Fatalf("expected startNode(r).id=2, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["e"]; got != 1 {
		t.Fatalf("expected endNode(r).id=1, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerEvaluatesDateTruncateFunction(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"d": map[string]any{"__temporal_type": "date", "year": 1984, "month": 10, "day": 11},
	}}}

	err := h.Execute("p10d", map[string]any{
		"items": []string{"date.truncate('year', d, {}) AS r"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	result, ok := state.Rows[0]["r"].(map[string]any)
	if !ok {
		t.Fatalf("expected map result for date truncate, got %#v", state.Rows[0]["r"])
	}
	if got := result["__temporal_type"]; got != "date" {
		t.Fatalf("expected temporal type date, got %#v", got)
	}
	if got := result["year"]; got != 1984 {
		t.Fatalf("expected year 1984, got %#v", got)
	}
	if got := result["month"]; got != 1 {
		t.Fatalf("expected month 1 after year truncate, got %#v", got)
	}
	if got := result["day"]; got != 1 {
		t.Fatalf("expected day 1 after year truncate, got %#v", got)
	}
}

func TestProjectHandlerEvaluatesTimeTruncateFunctionWithOverrides(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"dt": map[string]any{
			"__temporal_type": "datetime",
			"year":            1984,
			"month":           10,
			"day":             11,
			"hour":            12,
			"minute":          31,
			"second":          14,
			"nanosecond":      645876123,
			"timezone":        "+01:00",
		},
	}}}

	err := h.Execute("p10e", map[string]any{
		"items": []string{"time.truncate('minute', dt, {second: 42, timezone: '+05:00'}) AS r"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	result, ok := state.Rows[0]["r"].(map[string]any)
	if !ok {
		t.Fatalf("expected map result for time truncate, got %#v", state.Rows[0]["r"])
	}
	if got := result["__temporal_type"]; got != "time" {
		t.Fatalf("expected temporal type time, got %#v", got)
	}
	if got := result["hour"]; got != 12 {
		t.Fatalf("expected hour 12, got %#v", got)
	}
	if got := result["minute"]; got != 31 {
		t.Fatalf("expected minute 31, got %#v", got)
	}
	if got := result["second"]; got != 42 {
		t.Fatalf("expected overridden second 42, got %#v", got)
	}
	if got := result["timezone"]; got != "+05:00" {
		t.Fatalf("expected overridden timezone +05:00, got %#v", got)
	}
}

func TestProjectHandlerDurationMethodsPropagateNull(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p10f", map[string]any{
		"items": []string{"duration.inSeconds(null, null) AS r"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["r"]; got != nil {
		t.Fatalf("expected null for duration.inSeconds(null, null), got %#v", got)
	}
}

func TestProjectHandlerDurationInSecondsWithZeroArgTemporalsIsZero(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p10g", map[string]any{
		"items": []string{"duration.inSeconds(localtime(), localtime()) AS r"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	result, ok := state.Rows[0]["r"].(map[string]any)
	if !ok {
		t.Fatalf("expected duration map result, got %#v", state.Rows[0]["r"])
	}
	if got := result["seconds"]; got != int(0) && got != int64(0) {
		t.Fatalf("expected zero seconds, got %#v", result)
	}
	if got := result["nanoseconds"]; got != int(0) {
		t.Fatalf("expected zero nanoseconds, got %#v", result)
	}
}

func TestProjectHandlerHydratesStoredTemporalPropertyAccessors(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-temporal-accessors-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "acme",
			ID:     "v1",
			Labels: []string{"Val"},
			Properties: graph.PropertyMap{
				"date": []byte("1984-10-11"),
			},
		})
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	h := NewProjectHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{"v": "v1", "v.id": "v1"}}}
		if err := h.Execute("p-temporal-hydrate", map[string]any{
			"items": []string{"v.date.year AS y", "v.date.weekDay AS wd"},
		}, state); err != nil {
			return err
		}
		if got := state.Rows[0]["y"]; got != 1984 {
			t.Fatalf("expected year accessor 1984, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["wd"]; got != 4 {
			t.Fatalf("expected weekday accessor 4, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
}

func TestProjectHandlerHydratesStoredTemporalPropertyTypes(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-temporal-types-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "acme",
			ID:     "v1",
			Labels: []string{"Val"},
			Properties: graph.PropertyMap{
				"birthDate": []byte("1984-10-11"),
				"wakeTime":  []byte("12:00"),
			},
		})
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	h := NewProjectHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{"v": "v1", "v.id": "v1"}}}
		if err := h.Execute("p-temporal-types", map[string]any{
			"items": []string{"toString(v.birthDate) AS ds", "toString(v.wakeTime) AS ts"},
		}, state); err != nil {
			return err
		}
		if got := state.Rows[0]["ds"]; got != "1984-10-11" {
			t.Fatalf("expected stored date to stay a date, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["ts"]; got != "12:00" {
			t.Fatalf("expected stored localtime to stay a localtime, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
}

func TestProjectHandlerSerializesTemporalToString(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"d": map[string]any{"__temporal_type": "datetime", "year": 1984, "month": 10, "day": 11, "hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123, "timezone": "+01:00"},
	}}}

	err := h.Execute("p-temporal-string", map[string]any{
		"items": []string{"toString(d) AS ts"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["ts"]; got != "1984-10-11T12:31:14.645876123+01:00" {
		t.Fatalf("expected serialized datetime, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerTemporalConstructorPropagatesNull(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p-temporal-null", map[string]any{
		"items": []string{"date(null) AS d", "time(null) AS t", "datetime(null) AS dt"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if state.Rows[0]["d"] != nil || state.Rows[0]["t"] != nil || state.Rows[0]["dt"] != nil {
		t.Fatalf("expected null propagation for temporal constructors, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerTemporalEqualityRoundTrip(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"d": map[string]any{"__temporal_type": "date", "year": 1984, "month": 10, "day": 11},
	}}}

	err := h.Execute("p-temporal-eq", map[string]any{
		"items": []string{"date(toString(d)) = d AS ok"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["ok"]; got != true {
		t.Fatalf("expected temporal equality round-trip true, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerNormalizesDurationAccessorsAndString(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"d":  map[string]any{"__temporal_type": "duration", "years": 1, "months": 4, "hours": 1, "minutes": 1, "seconds": 1, "milliseconds": 111},
		"d2": map[string]any{"__temporal_type": "duration", "minutes": 12, "seconds": -60},
		"d3": map[string]any{"__temporal_type": "duration", "years": 12, "months": 5, "days": 14, "hours": 16, "minutes": 12, "seconds": 70, "nanoseconds": 1},
	}}}

	err := h.Execute("p-duration-normalize", map[string]any{
		"items": []string{"d.months AS months", "d.minutes AS minutes", "d.seconds AS seconds", "toString(d2) AS s2", "toString(d3) AS s3"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["months"]; got != 16 {
		t.Fatalf("expected total months accessor 16, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["minutes"]; got != 61 {
		t.Fatalf("expected total minutes accessor 61, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["seconds"]; got != 3661 {
		t.Fatalf("expected canonical whole seconds accessor 3661, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["s2"]; got != "PT11M" {
		t.Fatalf("expected normalized duration string PT11M, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["s3"]; got != "P12Y5M14DT16H13M10.000000001S" {
		t.Fatalf("expected normalized duration string carry, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerTemporalOrderingAndEquality(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"d1":   map[string]any{"__temporal_type": "date", "year": 1984, "month": 10, "day": 11},
		"d2":   map[string]any{"__temporal_type": "date", "year": 1985, "month": 10, "day": 11},
		"ldt":  map[string]any{"__temporal_type": "localdatetime", "year": 1984, "month": 10, "day": 11, "hour": 12, "minute": 31, "second": 14},
		"dt1":  map[string]any{"__temporal_type": "datetime", "year": 1984, "month": 10, "day": 11, "hour": 10, "minute": 0, "second": 0, "timezone": "+01:00"},
		"dt2":  map[string]any{"__temporal_type": "datetime", "year": 1984, "month": 10, "day": 11, "hour": 9, "minute": 35, "second": 14, "timezone": "+00:00"},
		"dur1": map[string]any{"__temporal_type": "duration", "minutes": 12, "seconds": 70},
		"dur2": map[string]any{"__temporal_type": "duration", "minutes": 13, "seconds": 10},
	}}}

	err := h.Execute("p-temporal-compare", map[string]any{
		"items": []string{"d1 < d2 AS dates", "localdatetime(toString(ldt)) = ldt AS ldtEq", "dt1 < dt2 AS datetimes", "dur1 = dur2 AS durations"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["dates"]; got != true {
		t.Fatalf("expected date ordering true, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["ldtEq"]; got != true {
		t.Fatalf("expected localdatetime round-trip equality true, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["datetimes"]; got != true {
		t.Fatalf("expected datetime ordering true, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["durations"]; got != true {
		t.Fatalf("expected normalized duration equality true, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerParsesDurationStringsCanonically(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p-duration-parse", map[string]any{
		"items": []string{"toString(duration('P0.75M')) AS a", "toString(duration('P2012-02-02T14:37:21.545')) AS b"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["a"]; got != "P22DT19H51M49.5S" {
		t.Fatalf("expected P0.75M canonical form, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["b"]; got != "P2012Y2M2DT14H37M21.545S" {
		t.Fatalf("expected calendar-style duration parse, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerDurationArithmeticCanonically(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{
		"x":  map[string]any{"__temporal_type": "date", "year": 1984, "month": 10, "day": 11},
		"d1": map[string]any{"__temporal_type": "duration", "years": 12, "months": 5, "days": 14, "hours": 16, "minutes": 12, "seconds": 70, "nanoseconds": 1},
		"d2": map[string]any{"__temporal_type": "duration", "years": 12.5, "months": 5.5, "days": 14.5, "hours": 16.5, "minutes": 12.5, "seconds": 70.5, "nanoseconds": 3},
	}}}

	err := h.Execute("p-duration-arith", map[string]any{
		"items": []string{"toString(x + d2) AS plusDate", "toString(x - d2) AS minusDate", "toString(d1 + d2) AS plusDur", "toString(d1 - d2) AS minusDur", "toString(d1 / 2) AS half"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["plusDate"]; got != "1997-10-11" {
		t.Fatalf("expected date + fractional duration canonical result, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["minusDate"]; got != "1971-10-12" {
		t.Fatalf("expected date - fractional duration canonical result, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["plusDur"]; got != "P25Y4M43DT50H11M23.500000004S" {
		t.Fatalf("expected duration sum canonical result, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["minusDur"]; got != "P-6M-15DT-17H-45M-3.500000002S" {
		t.Fatalf("expected duration diff canonical result, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["half"]; got != "P6Y2M22DT13H21M8S" {
		t.Fatalf("expected duration divide canonical result, got %#v", state.Rows[0])
	}
}

func TestOrderByComparisonConsistencyPattern(t *testing.T) {
	project := NewProjectHandler()
	unwind := NewProjectHandler()
	sortHandler := NewSortHandler()

	state := &State{Rows: []map[string]any{{
		"values": []any{true, false},
	}}}

	if err := project.Execute("p0", map[string]any{
		"items": []string{"values", "size(values) AS numOfValues"},
	}, state); err != nil {
		t.Fatalf("project p0 failed: %v", err)
	}

	if err := unwind.Execute("u0", map[string]any{
		"kind":  "UNWIND",
		"items": []string{"values AS value"},
	}, state); err != nil {
		t.Fatalf("unwind failed: %v", err)
	}

	if err := project.Execute("p1", map[string]any{
		"items": []string{"size([ x IN values WHERE x < value ]) AS x", "value", "numOfValues"},
	}, state); err != nil {
		t.Fatalf("project p1 failed: %v", err)
	}
	p1Rows := append([]map[string]any(nil), state.Rows...)

	if err := sortHandler.Execute("s0", map[string]any{
		"ordering": []map[string]any{{"expression": "value", "direction": "ASC"}},
	}, state); err != nil {
		t.Fatalf("sort failed: %v", err)
	}
	sortedRows := append([]map[string]any(nil), state.Rows...)

	if err := project.Execute("p2", map[string]any{
		"items": []string{"numOfValues", "collect(x) AS orderedX"},
	}, state); err != nil {
		t.Fatalf("project p2 failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one grouped row after p2, got %#v", state.Rows)
	}
	if got := state.Rows[0]["orderedX"]; !reflect.DeepEqual(got, []any{0, 1}) {
		t.Fatalf("expected orderedX [0 1], got %#v (p1=%#v sorted=%#v)", got, p1Rows, sortedRows)
	}

	if err := project.Execute("p3", map[string]any{
		"items": []string{"orderedX = range(0, numOfValues-1) AS equal"},
	}, state); err != nil {
		t.Fatalf("project p3 failed: %v", err)
	}

	if len(state.Rows) != 1 {
		t.Fatalf("expected one row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["equal"]; got != true {
		t.Fatalf("expected ORDER BY/comparison consistency true, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerDurationArithmeticFallsBackFromNilDottedRowKey(t *testing.T) {
	h := NewProjectHandler()
	row := map[string]any{
		"dur":      map[string]any{"date": "P12Y5M14DT16H13M10.000000001S"},
		"dur2":     map[string]any{"date": "P12Y5M14DT16H13M10.000000001S"},
		"dur.date": nil,
	}

	state := &State{Rows: []map[string]any{row}}

	err := h.Execute("p-duration-nil-shadow", map[string]any{
		"items": []string{"toString(dur.date) AS raw1", "toString(dur2.date) AS raw2", "toString(dur.date + dur2.date) AS sum", "toString(dur.date - dur2.date) AS diff"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if got := state.Rows[0]["raw1"]; got != "P12Y5M14DT16H13M10.000000001S" {
		t.Fatalf("expected raw1 duration hydration, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["raw2"]; got != "P12Y5M14DT16H13M10.000000001S" {
		t.Fatalf("expected raw2 duration hydration, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["sum"]; got != "P24Y10M28DT32H26M20.000000002S" {
		t.Fatalf("expected sum through dotted-key fallback, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["diff"]; got != "PT0S" {
		t.Fatalf("expected diff through dotted-key fallback, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerDurationArithmeticWithMapIDBinding(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-duration-idbinding-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d1", Labels: []string{"Duration1"}, Properties: graph.PropertyMap{"date": []byte("P12Y5M14DT16H13M10.000000001S")}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d2", Labels: []string{"Duration2"}, Properties: graph.PropertyMap{"date": []byte("P12Y5M14DT16H13M10.000000001S")}})
	}); err != nil {
		t.Fatalf("seed vertices failed: %v", err)
	}

	h := NewProjectHandler()
	if err := store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{"dur": map[string]any{"id": "d1"}, "dur2": map[string]any{"id": "d2"}}}}
		if err := h.Execute("p-duration-idbinding", map[string]any{
			"items": []string{"toString(dur.date + dur2.date) AS sum", "toString(dur.date - dur2.date) AS diff"},
		}, state); err != nil {
			return err
		}
		if got := state.Rows[0]["sum"]; got != "P24Y10M28DT32H26M20.000000002S" {
			t.Fatalf("expected sum for map{id} binding, got %#v", state.Rows[0])
		}
		if got := state.Rows[0]["diff"]; got != "PT0S" {
			t.Fatalf("expected diff for map{id} binding, got %#v", state.Rows[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
}

func TestSortHandlerPreservesStableOrderForConstantExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "numeric literal", expression: "1"},
		{name: "quoted literal", expression: "'x'"},
		{name: "keyword null", expression: "null"},
		{name: "parameter", expression: "$sort"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewSortHandler()
			state := &State{Rows: []map[string]any{
				{"id": "u1", "rank": 2},
				{"id": "u2", "rank": 1},
				{"id": "u3", "rank": 3},
			}}

			err := h.Execute("s1", map[string]any{
				"ordering": []map[string]any{{"expression": tc.expression, "direction": "DESC"}},
			}, state)
			if err != nil {
				t.Fatalf("sort execute failed: %v", err)
			}
			if len(state.Rows) != 3 {
				t.Fatalf("expected 3 rows after sort, got %#v", state.Rows)
			}
			if got := state.Rows[0]["id"]; got != "u1" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
			if got := state.Rows[1]["id"]; got != "u2" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
			if got := state.Rows[2]["id"]; got != "u3" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
		})
	}
}

func TestSortHandlerSortsByBoundIdentifier(t *testing.T) {
	h := NewSortHandler()
	state := &State{Rows: []map[string]any{
		{"id": "u1", "rank": 2},
		{"id": "u2", "rank": 1},
		{"id": "u3", "rank": 3},
	}}

	err := h.Execute("s2", map[string]any{
		"ordering": []map[string]any{{"expression": "rank", "direction": "DESC"}},
	}, state)
	if err != nil {
		t.Fatalf("sort execute failed: %v", err)
	}
	if len(state.Rows) != 3 {
		t.Fatalf("expected 3 rows after sort, got %#v", state.Rows)
	}
	if got := state.Rows[0]["id"]; got != "u3" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
	if got := state.Rows[1]["id"]; got != "u1" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
	if got := state.Rows[2]["id"]; got != "u2" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
}

func TestPaginationHandlerResolvesParametrizedSkipLimit(t *testing.T) {
	h := NewPaginationHandler()
	state := &State{
		Rows: []map[string]any{
			{"n": 0},
			{"n": 1},
			{"n": 2},
			{"n": 3},
			{"n": 4},
		},
		Params: map[string]any{"s": 2, "l": 2},
	}

	err := h.Execute("p-page", map[string]any{"skip": "$s", "limit": "$l"}, state)
	if err != nil {
		t.Fatalf("pagination execute failed: %v", err)
	}
	if len(state.Rows) != 2 {
		t.Fatalf("expected two rows after SKIP/LIMIT params, got %#v", state.Rows)
	}
	if state.Rows[0]["n"] != 2 || state.Rows[1]["n"] != 3 {
		t.Fatalf("unexpected paginated rows: %#v", state.Rows)
	}
}

func TestPaginationHandlerRejectsNegativeSkip(t *testing.T) {
	h := NewPaginationHandler()
	state := &State{
		Rows:   []map[string]any{{"n": 0}, {"n": 1}},
		Params: map[string]any{"s": -1},
	}

	err := h.Execute("p-page", map[string]any{"skip": "$s"}, state)
	if err == nil {
		t.Fatal("expected negative SKIP to fail")
	}
	if !graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected unsupported error kind, got %v", err)
	}
	if !strings.Contains(err.Error(), "NegativeIntegerArgument") {
		t.Fatalf("expected NegativeIntegerArgument error, got %v", err)
	}
}

func TestPaginationHandlerRejectsFloatLimit(t *testing.T) {
	h := NewPaginationHandler()
	state := &State{
		Rows:   []map[string]any{{"n": 0}, {"n": 1}},
		Params: map[string]any{"l": 1.5},
	}

	err := h.Execute("p-page", map[string]any{"limit": "$l"}, state)
	if err == nil {
		t.Fatal("expected float LIMIT to fail")
	}
	if !graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected unsupported error kind, got %v", err)
	}
	if !strings.Contains(err.Error(), "InvalidArgumentType") {
		t.Fatalf("expected InvalidArgumentType error, got %v", err)
	}
}

func TestSortHandlerResolvesOrderingByTerminalAlias(t *testing.T) {
	h := NewSortHandler()
	state := &State{Rows: []map[string]any{
		{"count": 10},
		{"count": 2},
		{"count": 5},
	}}

	err := h.Execute("s-alias", map[string]any{
		"ordering": []map[string]any{{"expression": "a.count", "direction": "ASC"}},
	}, state)
	if err != nil {
		t.Fatalf("sort execute failed: %v", err)
	}
	if len(state.Rows) != 3 {
		t.Fatalf("expected 3 rows after sort, got %#v", state.Rows)
	}
	if state.Rows[0]["count"] != 2 || state.Rows[1]["count"] != 5 || state.Rows[2]["count"] != 10 {
		t.Fatalf("unexpected ORDER BY alias resolution order: %#v", state.Rows)
	}
}

func TestResolveProjectionExprValueNullPredicatesAndNot(t *testing.T) {
	row := map[string]any{
		"n": nil,
		"x": 1,
	}

	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "is null true", expr: "n IS NULL", want: true},
		{name: "is null false", expr: "x IS NULL", want: false},
		{name: "is not null true", expr: "x IS NOT NULL", want: true},
		{name: "is not null false", expr: "n IS NOT NULL", want: false},
		{name: "not is null", expr: "NOT (n IS NULL)", want: false},
		{name: "not is not null", expr: "NOT (x IS NOT NULL)", want: false},
		{name: "not null", expr: "NOT null", want: nil},
		{name: "not non boolean", expr: "NOT 1", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, row, nil)
			if !ok {
				t.Fatalf("expected expression to resolve: %q", tc.expr)
			}
			if got != tc.want {
				t.Fatalf("unexpected value for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueQuantifierNullPredicateSemantics(t *testing.T) {
	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "all with is not null", expr: "all(x IN [null, 1] WHERE x IS NOT NULL)", want: false},
		{name: "all with is null", expr: "all(x IN [null] WHERE x IS NULL)", want: true},
		{name: "any with is null", expr: "any(x IN [null] WHERE x IS NULL)", want: true},
		{name: "none with not equality", expr: "none(x IN [1, 2] WHERE NOT (x = 1))", want: false},
		{name: "any modulo predicate", expr: "any(x IN [1, 2, 3] WHERE x % 2 = 0)", want: true},
		{name: "all modulo predicate", expr: "all(x IN [2, 4, 6] WHERE x % 2 = 0)", want: true},
		{name: "none statically false", expr: "none(x IN [1, null, true] WHERE false)", want: true},
		{name: "none statically true", expr: "none(x IN [1, null, true] WHERE true)", want: false},
		{name: "any equals single or all", expr: "any(x IN [2] WHERE x = 2) = (single(x IN [2] WHERE x = 2) OR all(x IN [2] WHERE x = 2))", want: true},
		{name: "nested quantifier abs predicate", expr: "none(x IN [1,2,3,4,5,6,7,8,9] WHERE single(y IN [1,2,3,4,5,6,7,8,9] WHERE abs(x - y) < 3))", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, map[string]any{}, nil)
			if !ok {
				t.Fatalf("expected quantifier expression to resolve: %q", tc.expr)
			}
			if got != tc.want {
				t.Fatalf("unexpected quantifier result for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueINSemanticsWithSubscriptsAndNulls(t *testing.T) {
	row := map[string]any{
		"list": []any{[]any{1, 2, 3}},
	}
	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "direct nested list subscript", expr: "list[0]", want: []any{1, 2, 3}},
		{name: "direct literal nested list subscript", expr: "[[1, 2, 3]][0]", want: []any{1, 2, 3}},
		{name: "direct list slice", expr: "[1, 2, 3][0..1]", want: []any{1}},
		{name: "in nested list subscript", expr: "3 IN list[0]", want: true},
		{name: "in literal nested list subscript", expr: "3 IN [[1, 2, 3]][0]", want: true},
		{name: "in list slice", expr: "3 IN [1, 2, 3][0..1]", want: false},
		{name: "in empty list with null lhs", expr: "null IN []", want: false},
		{name: "in null list item unknown", expr: "5 IN [1, 2, 3, null]", want: nil},
		{name: "in numeric vs string", expr: "1 IN ['1', 2]", want: false},
		{name: "in list numeric vs string", expr: "[1, 2] IN [1, [1, '2']]", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, row, nil)
			if !ok {
				t.Fatalf("expected expression to resolve: %q", tc.expr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected value for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueComparisonNullAndComparabilitySemantics(t *testing.T) {
	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "list equality with null element is unknown", expr: "[null] = [1]", want: nil},
		{name: "map equality with null values is unknown", expr: "{k: null} = {k: null}", want: nil},
		{name: "map inequality with null values is unknown", expr: "{k: null} <> {k: null}", want: nil},
		{name: "cross type number string less than is null", expr: "'1' < 1", want: nil},
		{name: "numeric cross type int float less than true", expr: "1 < 3.14", want: true},
		{name: "nan compared to number is always false", expr: "0.0 / 0.0 > 1", want: false},
		{name: "nan compared to string is null", expr: "0.0 / 0.0 < 'a'", want: nil},
		{name: "list ordering with null element is unknown", expr: "[1, 2] >= [1, null]", want: nil},
		{name: "list ordering decides before null", expr: "[1, null] >= [1]", want: true},
		{name: "in precedence over comparison yields null", expr: "[1, 2] < [3, 4] IN [[3, 4], false]", want: nil},
		{name: "in precedence over comparison equality", expr: "[1, 2] = [3, 4] IN [[3, 4], false]", want: false},
		{name: "parenthesized exponent subtraction", expr: "(4 ^ 3) - (2 ^ 3)", want: 56.0},
		{name: "parenthesized exponent subtraction compact", expr: "(4^3)-(2^3)", want: 56.0},
		{name: "null predicate precedence inequality", expr: "(null IS NULL) <> (null IS NOT NULL)", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, map[string]any{}, nil)
			if !ok {
				t.Fatalf("expected expression to resolve: %q", tc.expr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected value for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueComparisonChains(t *testing.T) {
	row := map[string]any{
		"n": map[string]any{"num": 2, "name": "b", "prop1": 1, "prop2": 2},
		"m": map[string]any{"prop1": 2, "prop2": 3},
	}

	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "numeric strict range", expr: "1 < n.num < 3", want: true},
		{name: "numeric inclusive range", expr: "1 <= n.num <= 3", want: true},
		{name: "string range", expr: "'a' < n.name < 'c'", want: true},
		{name: "long operator chain", expr: "n.prop1 < m.prop1 = n.prop2 <> m.prop2", want: true},
		{name: "chain false when one comparator fails", expr: "1 < n.num > 3", want: false},
		{name: "chain unknown when comparator unknown", expr: "1 < null < 3", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, row, nil)
			if !ok {
				t.Fatalf("expected expression to resolve: %q", tc.expr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected value for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueTypeFunctionSemantics(t *testing.T) {
	row := map[string]any{
		"r": map[string]any{"type": "T", "src": "a", "dst": "b"},
	}

	got, ok := resolveProjectionExprValue("type(r)", row, &State{})
	if !ok {
		t.Fatalf("expected type(r) to resolve")
	}
	if got != "T" {
		t.Fatalf("expected relationship type T, got %#v", got)
	}

	got, ok = resolveProjectionExprValue("type(null)", row, &State{})
	if !ok {
		t.Fatalf("expected type(null) to resolve")
	}
	if got != nil {
		t.Fatalf("expected null from type(null), got %#v", got)
	}
}

func TestResolveProjectionExprValueTypeFunctionInvalidArgumentFails(t *testing.T) {
	_, _, err := resolveProjectionExprValueChecked("type(1)", map[string]any{}, &State{})
	if err == nil {
		t.Fatalf("expected InvalidArgumentValue runtime error for type(1)")
	}
	if !strings.Contains(err.Error(), "InvalidArgumentValue") {
		t.Fatalf("expected InvalidArgumentValue error, got %v", err)
	}
}

func TestResolveProjectionExprValueLabelsFunctionSemantics(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "operators-labels-fn-")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}
	defer os.RemoveAll(dir)
	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1", Labels: []string{"Foo", "Bar"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tenant: "acme", Tx: tx}
		row := map[string]any{
			"a":    "v1",
			"list": []any{"v1", 1},
		}
		got, ok := resolveProjectionExprValue("labels(list[0])", row, state)
		if !ok {
			t.Fatalf("expected labels(list[0]) to resolve")
		}
		labels, ok := got.([]string)
		if !ok || len(labels) != 2 {
			t.Fatalf("expected two labels, got %#v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValueLabelsFunctionInvalidArgumentFails(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "operators-labels-invalid-")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}
	defer os.RemoveAll(dir)
	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tenant: "acme", Tx: tx}
		_, ok := resolveProjectionExprValue("labels(1)", map[string]any{}, state)
		if !ok {
			t.Fatalf("expected labels(1) expression to be handled")
		}
		evalErr := projectionEvalError(state)
		if evalErr == nil {
			t.Fatalf("expected InvalidArgumentValue runtime error for labels(1)")
		}
		if !strings.Contains(evalErr.Error(), "InvalidArgumentValue") {
			t.Fatalf("expected InvalidArgumentValue error, got %v", evalErr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestResolveProjectionExprValueLabelPredicateConjunction(t *testing.T) {
	row := map[string]any{
		"a":        "v1",
		"a.labels": []any{"A", "C"},
		"r":        map[string]any{"type": "REL"},
		"n":        nil,
	}

	tests := []struct {
		name string
		expr string
		want any
	}{
		{name: "ordered labels", expr: "a:A:C", want: true},
		{name: "reordered labels", expr: "a:C:A", want: true},
		{name: "repeated labels", expr: "a:A:C:A", want: true},
		{name: "missing label", expr: "a:A:B", want: false},
		{name: "relationship type", expr: "r:REL", want: true},
		{name: "relationship type mismatch", expr: "r:OTHER", want: false},
		{name: "null input remains null", expr: "n:A", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := resolveProjectionExprValue(tc.expr, row, nil)
			if !ok {
				t.Fatalf("expected label predicate expression to resolve: %q", tc.expr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected label predicate value for %q: got=%#v want=%#v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestResolveProjectionExprValueDurationComparedToTemporalIsFalse(t *testing.T) {
	got, ok := resolveProjectionExprValue(
		"duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}) = date({year: 1984, month: 10, day: 11})",
		map[string]any{},
		&State{},
	)
	if !ok {
		t.Fatalf("expected expression to resolve")
	}
	if got != false {
		t.Fatalf("expected false for duration vs date equality, got %#v", got)
	}
}

func TestResolveProjectionExprValueSizePatternComprehensionCountsOutDegree(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "operators-size-pattern-")
	if err != nil {
		t.Fatalf("mkdir temp failed: %v", err)
	}
	defer os.RemoveAll(dir)
	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n1", Labels: []string{"S"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n2"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n3"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", SrcID: "n1", DstID: "n2", Type: "REL"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", SrcID: "n1", DstID: "n3", Type: "REL"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	var got any
	err = store.View(ctx, func(tx graph.Tx) error {
		value, ok := resolveProjectionExprValue("size([(n)-->() | 1])", map[string]any{"n": "n1"}, &State{Tenant: "acme", Tx: tx})
		if !ok {
			t.Fatalf("expected size pattern-comprehension expression to resolve")
		}
		got = value
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected out-degree 2 from size([(n)-->() | 1]), got %#v", got)
	}
}

func TestProjectHandlerUnwindKindExpandsRows(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{"input": []any{1, 2, 3}}}}

	err := h.Execute("p-unwind", map[string]any{
		"kind":  "UNWIND",
		"items": []string{"input AS x"},
	}, state)
	if err != nil {
		t.Fatalf("project unwind execute failed: %v", err)
	}
	if len(state.Rows) != 3 {
		t.Fatalf("expected 3 unwound rows, got %#v", state.Rows)
	}
	if got := state.Rows[0]["x"]; got != 1 {
		t.Fatalf("expected first x=1, got %#v", state.Rows[0])
	}
	if got := state.Rows[1]["x"]; got != 2 {
		t.Fatalf("expected second x=2, got %#v", state.Rows[1])
	}
	if got := state.Rows[2]["x"]; got != 3 {
		t.Fatalf("expected third x=3, got %#v", state.Rows[2])
	}
}

func TestProjectHandlerWhereFallbackEvaluatesComplexPredicates(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"list": []any{}},
		{"list": []any{1}},
		{"list": []any{1, 2}},
	}}

	err := h.Execute("p-where-fallback", map[string]any{
		"kind":  "WITH",
		"items": []string{"list AS list"},
		"where": "size(list) > 0",
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 2 {
		t.Fatalf("expected rows filtered to non-empty lists, got %#v", state.Rows)
	}

	state2 := &State{Rows: []map[string]any{
		{"list": []any{}},
		{"list": []any{1}},
	}}
	err = h.Execute("p-where-fallback-not", map[string]any{
		"kind":  "WITH",
		"items": []string{"list AS list"},
		"where": "NOT (size(list) = 0)",
	}, state2)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state2.Rows) != 1 {
		t.Fatalf("expected one row after NOT(size(list)=0), got %#v", state2.Rows)
	}
}

func TestProjectHandlerWhereFallbackPreservesWhitespaceAndNewlineProperties(t *testing.T) {
	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "ve-runtime-projection-whitespace-")
	if err != nil {
		t.Fatalf("create temp dir failed: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	store, err := pebblestore.Open(tempDir)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-space", Properties: graph.PropertyMap{"name": []byte(" Foo ")}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-newline", Properties: graph.PropertyMap{"name": []byte("\nFoo\n")}})
	})
	if err != nil {
		t.Fatalf("seed vertices failed: %v", err)
	}

	h := NewProjectHandler()
	err = store.View(ctx, func(tx graph.Tx) error {
		state := &State{Tx: tx, Tenant: "acme", Rows: []map[string]any{{"a": "v-space", "a.id": "v-space"}, {"a": "v-newline", "a.id": "v-newline"}}}
		if err := h.Execute("p-where-string-whitespace", map[string]any{
			"kind":  "WITH",
			"items": []string{"a.name AS name"},
			"where": "a.name STARTS WITH ' ' OR a.name STARTS WITH '\\n'",
		}, state); err != nil {
			return err
		}
		if len(state.Rows) != 2 {
			t.Fatalf("expected both whitespace-preserving rows, got %#v", state.Rows)
		}
		got := []any{state.Rows[0]["name"], state.Rows[1]["name"]}
		want := []any{" Foo ", "\nFoo\n"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected projected names: got %#v want %#v", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
}

func TestProjectHandlerWhereFallbackEvaluatesLabelPredicates(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"a": map[string]any{"labels": []any{"A", "B"}}},
		{"a": map[string]any{"labels": []any{"A", "C"}}},
		{"a": map[string]any{"labels": []any{"C"}}},
	}}

	err := h.Execute("p-where-labels", map[string]any{
		"kind":  "WITH",
		"items": []string{"a AS a"},
		"where": "a:C:A:A:C",
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one row after label predicate filter, got %#v", state.Rows)
	}
	labels, ok := state.Rows[0]["a"].(map[string]any)["labels"].([]any)
	if !ok || len(labels) != 2 || labels[0] != "A" || labels[1] != "C" {
		t.Fatalf("unexpected remaining row after label predicate filter: %#v", state.Rows[0])
	}
}

func TestFilterHandlerEvaluatesLabelPredicates(t *testing.T) {
	h := NewFilterHandler()
	state := &State{Rows: []map[string]any{
		{"a": map[string]any{"labels": []any{"A", "B"}}},
		{"a": map[string]any{"labels": []any{"A", "C"}}},
		{"a": map[string]any{"labels": []any{"C"}}},
	}}

	err := h.Execute("f-labels", map[string]any{"predicate": "a:C:A:A:C"}, state)
	if err != nil {
		t.Fatalf("filter execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one row after filter predicate, got %#v", state.Rows)
	}
}

func TestProjectHandlerMixedAggregateCollapsesInvariantRows(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"x": 1},
		{"x": 2},
		{"x": 3},
		{"x": 4},
	}}

	err := h.Execute("p-mixed-agg-invariant", map[string]any{
		"kind":  "WITH",
		"items": []string{"x > 0 AS result", "count(*) AS cnt"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected single grouped row, got %#v", state.Rows)
	}
	if got := state.Rows[0]["result"]; got != true {
		t.Fatalf("expected result=true, got %#v", state.Rows[0])
	}
	if got := state.Rows[0]["cnt"]; got != 4 {
		t.Fatalf("expected cnt=4, got %#v", state.Rows[0])
	}
}

func TestProjectHandlerMixedAggregateGroupsByScalarProjection(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"x": 1},
		{"x": 2},
		{"x": 3},
	}}

	err := h.Execute("p-mixed-agg-groups", map[string]any{
		"kind":  "WITH",
		"items": []string{"x = 1 AS result", "count(*) AS cnt"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 2 {
		t.Fatalf("expected two grouped rows, got %#v", state.Rows)
	}

	counts := map[bool]int{}
	for _, row := range state.Rows {
		result, _ := row["result"].(bool)
		cnt, _ := row["cnt"].(int)
		counts[result] = cnt
	}
	if counts[true] != 1 || counts[false] != 2 {
		t.Fatalf("unexpected grouped counts: %#v", state.Rows)
	}
}

func TestResolveProjectionExprValueQuantifierListPipelineSemantics(t *testing.T) {
	row := map[string]any{}

	parts := []string{
		"[y IN [1, 2, 3, 4] WHERE y % 2 = 1 | y]",
		"reverse([y IN [1, 2, 3, 4] WHERE y % 2 = 1 | y])",
		"CASE WHEN rand() > -1 THEN reverse([y IN [1, 2, 3, 4] WHERE y % 2 = 1 | y]) ELSE [] END",
		"CASE WHEN rand() > -1 THEN reverse([y IN [1, 2, 3, 4] WHERE y % 2 = 1 | y]) ELSE [] END + 6",
	}
	for _, part := range parts {
		if _, ok := resolveProjectionExprValue(part, row, nil); !ok {
			t.Fatalf("expected subexpression to resolve: %q", part)
		}
	}

	expr := "none(x IN (CASE WHEN rand() > -1 THEN reverse([y IN [1, 2, 3, 4] WHERE y % 2 = 1 | y]) ELSE [] END + 6) WHERE false)"
	got, ok := resolveProjectionExprValue(expr, row, nil)
	if !ok {
		t.Fatalf("expected expression to resolve: %q", expr)
	}
	if got != true {
		t.Fatalf("unexpected value for %q: got=%#v want=%#v", expr, got, true)
	}

	expr2 := "none(x IN coalesce(null, [1, 2, 3]) WHERE true)"
	got2, ok := resolveProjectionExprValue(expr2, row, nil)
	if !ok {
		t.Fatalf("expected expression to resolve: %q", expr2)
	}
	if got2 != false {
		t.Fatalf("unexpected value for %q: got=%#v want=%#v", expr2, got2, false)
	}
}

func TestResolveProjectionExprValueQuantifierEntityPropertyPredicates(t *testing.T) {
	row := map[string]any{
		"nodes": []any{
			&graph.Vertex{ID: "v1", Properties: graph.PropertyMap{"name": []byte("a")}},
			&graph.Vertex{ID: "v2", Properties: graph.PropertyMap{"name": []byte("a")}},
		},
		"rels": []any{
			&graph.Edge{ID: "e1", Properties: graph.PropertyMap{"name": []byte("a")}},
			&graph.Edge{ID: "e2", Properties: graph.PropertyMap{"name": []byte("a")}},
		},
	}

	nodesExpr := "all(x IN nodes WHERE x.name = 'a')"
	nodesGot, ok := resolveProjectionExprValue(nodesExpr, row, nil)
	if !ok {
		t.Fatalf("expected expression to resolve: %q", nodesExpr)
	}
	if nodesGot != true {
		t.Fatalf("unexpected value for %q: got=%#v want=%#v", nodesExpr, nodesGot, true)
	}

	relsExpr := "all(x IN rels WHERE x.name = 'a')"
	relsGot, ok := resolveProjectionExprValue(relsExpr, row, nil)
	if !ok {
		t.Fatalf("expected expression to resolve: %q", relsExpr)
	}
	if relsGot != true {
		t.Fatalf("unexpected value for %q: got=%#v want=%#v", relsExpr, relsGot, true)
	}
}
