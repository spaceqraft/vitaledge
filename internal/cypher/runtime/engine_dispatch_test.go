package runtime

import (
	"reflect"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/physical"
	"github.com/paegun/vitaledge/internal/cypher/pipeline"
	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

type recordingHandler struct {
	name string
	hit  *bool
}

func (h *recordingHandler) OpName() string { return h.name }

func (h *recordingHandler) Execute(nodeID string, attrs map[string]any, state *operators.State) error {
	_ = nodeID
	_ = attrs
	if h.hit != nil {
		*h.hit = true
	}
	if state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.name)
		state.OperatorExecCount++
	}
	return nil
}

type seedRowsHandler struct {
	name string
	rows []map[string]any
}

func (h *seedRowsHandler) OpName() string { return h.name }

func (h *seedRowsHandler) Execute(nodeID string, attrs map[string]any, state *operators.State) error {
	_ = nodeID
	_ = attrs
	if state == nil {
		return nil
	}
	state.ExecutedOps = append(state.ExecutedOps, h.name)
	state.OperatorExecCount++
	out := make([]map[string]any, 0, len(h.rows))
	for _, row := range h.rows {
		copied := map[string]any{}
		for k, v := range row {
			copied[k] = v
		}
		out = append(out, copied)
	}
	state.Rows = out
	return nil
}

func TestEngineDispatchUsesRegisteredHandlers(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name ORDER BY name ASC LIMIT 5")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	engine := New()
	hit := false
	engine.RegisterHandler(&recordingHandler{name: "PHY_PROJECT", hit: &hit})

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if !hit {
		t.Fatalf("expected registered project handler to be invoked")
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_PROJECT", "PHY_SORT", "PHY_PAGINATION"}
	if !reflect.DeepEqual(res.ExecutedOps, expected) {
		t.Fatalf("unexpected executed ops: got=%v want=%v", res.ExecutedOps, expected)
	}
}

func TestEngineDispatchFallsBackWhenHandlerMissing(t *testing.T) {
	engine := &Engine{handlers: map[string]operators.Handler{}}
	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_UNKNOWN", Children: nil, Attrs: map[string]any{}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_UNKNOWN"}) {
		t.Fatalf("expected fallback execution to record unknown op, got %#v", res.ExecutedOps)
	}
	if res.Stats.OperatorsExecuted != 1 {
		t.Fatalf("expected one executed operator, got %#v", res.Stats)
	}
}

func TestEngineDispatchSortAndPaginationRowFlow(t *testing.T) {
	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{
			{"score": 30, "name": "c"},
			{"score": 10, "name": "a"},
			{"score": 20, "name": "b"},
		},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p3",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_SORT", Children: []string{"p1"}, Attrs: map[string]any{
				"ordering": []map[string]any{{"expression": "score", "direction": "ASC"}},
			}},
			{ID: "p3", Op: "PHY_PAGINATION", Children: []string{"p2"}, Attrs: map[string]any{"skip": 1, "limit": 1}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_EXPAND_MATCH", "PHY_SORT", "PHY_PAGINATION"}) {
		t.Fatalf("unexpected executed ops: %#v", res.ExecutedOps)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected exactly one row after pagination, got %#v", res.Rows)
	}
	if got, _ := res.Rows[0]["score"].(int); got != 20 {
		t.Fatalf("unexpected paginated row: %#v", res.Rows[0])
	}
}

func TestEngineDispatchSurfacesWriteEvents(t *testing.T) {
	engine := New()
	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_WRITE", Children: []string{"p1"}, Attrs: map[string]any{
				"kind":         "MERGE",
				"raw":          "MERGE (u)-[:KNOWS]->(:User {id:$peer})",
				"mergePattern": "(u)-[:KNOWS]->(:User {id:$peer})",
			}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{"peer": "u2"}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected surfaced write event, got %#v", res.WriteEvents)
	}
	event := res.WriteEvents[0]
	if event.MutationType != operators.MutationTypeEdge {
		t.Fatalf("expected edge mutation type, got %#v", event)
	}
	if got := event.ResolvedParams["peer"]; got != "u2" {
		t.Fatalf("expected resolved peer param, got %#v", event.ResolvedParams)
	}
	if res.Stats.WritesRecorded != 1 {
		t.Fatalf("expected one recorded write, got %#v", res.Stats)
	}
}
