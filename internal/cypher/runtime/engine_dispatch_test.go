package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/logical"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/cypher/pipeline"
	"github.com/spaceqraft/vitaledge/internal/cypher/runtime/operators"
	"github.com/spaceqraft/vitaledge/internal/cypher/semantic"
	"github.com/spaceqraft/vitaledge/internal/graph"
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

type variantRecordingHandler struct {
	name        string
	variantHits []string
}

type antiProbeDispatchTx struct {
	*recordingTx
	rowCalls   int
	batchCalls int
}

type variantDecisionHandler struct {
	name           string
	handled        bool
	variantErr     error
	defaultErr     error
	variantCalls   int
	defaultCalls   int
	lastVariantKey string
}

func (t *antiProbeDispatchTx) HasDirectedEdgeBetween(_ context.Context, _ string, srcID, dstID, edgeType string) (bool, error) {
	t.rowCalls++
	return srcID == "u1" && dstID == "u2" && edgeType == "KNOWS", nil
}

func (t *antiProbeDispatchTx) BatchHasDirectedEdgeBetween(_ context.Context, _ string, probes []graph.DirectedEdgeProbe) ([]bool, error) {
	t.batchCalls++
	out := make([]bool, len(probes))
	for i, probe := range probes {
		out[i] = probe.SrcID == "u1" && probe.DstID == "u2" && probe.EdgeType == "KNOWS"
	}
	return out, nil
}

func (h *variantRecordingHandler) OpName() string { return h.name }

func (h *variantRecordingHandler) Execute(nodeID string, attrs map[string]any, state *operators.State) error {
	_ = nodeID
	_ = attrs
	if state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.name+":default")
		state.OperatorExecCount++
	}
	return nil
}

func (h *variantRecordingHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *operators.State) (bool, error) {
	_ = nodeID
	_ = attrs
	h.variantHits = append(h.variantHits, variant)
	if state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.name+":variant:"+variant)
		state.OperatorExecCount++
	}
	return true, nil
}

func (h *variantDecisionHandler) OpName() string { return h.name }

func (h *variantDecisionHandler) Execute(nodeID string, attrs map[string]any, state *operators.State) error {
	_ = nodeID
	_ = attrs
	h.defaultCalls++
	if h.defaultErr != nil {
		return h.defaultErr
	}
	if state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.name+":default")
		state.OperatorExecCount++
	}
	return nil
}

func (h *variantDecisionHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *operators.State) (bool, error) {
	_ = nodeID
	_ = attrs
	h.variantCalls++
	h.lastVariantKey = variant
	if h.variantErr != nil {
		return true, h.variantErr
	}
	if h.handled && state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.name+":variant:"+variant)
		state.OperatorExecCount++
	}
	return h.handled, nil
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

func TestEngineDispatchSortTopKVariantTruncatesRows(t *testing.T) {
	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{{"score": 50}, {"score": 10}, {"score": 40}, {"score": 30}, {"score": 20}},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_SORT", Children: []string{"p1"}, Attrs: map[string]any{
				"variant":  "sort_topk_heap",
				"strategy": "topk_heap",
				"topK":     3,
				"ordering": []map[string]any{{"expression": "score", "direction": "DESC"}},
			}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected sort_topk_heap to retain only topK rows, got %#v", res.Rows)
	}
	if got, _ := res.Rows[0]["score"].(int); got != 50 {
		t.Fatalf("unexpected first row after topk sort: %#v", res.Rows)
	}
	if got, _ := res.Rows[2]["score"].(int); got != 30 {
		t.Fatalf("unexpected third row after topk sort: %#v", res.Rows)
	}
}

func TestEngineDispatchInvokesVariantHandlerWhenVariantPresent(t *testing.T) {
	engine := New()
	h := &variantRecordingHandler{name: "PHY_PROJECT"}
	engine.RegisterHandler(h)

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{{
			ID:       "p1",
			Op:       "PHY_PROJECT",
			Children: nil,
			Attrs:    map[string]any{"variant": "project_fast"},
		}},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(h.variantHits) != 1 || h.variantHits[0] != "project_fast" {
		t.Fatalf("expected variant handler to receive project_fast, got %#v", h.variantHits)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_PROJECT:variant:project_fast"}) {
		t.Fatalf("expected variant execution to bypass default path, got %#v", res.ExecutedOps)
	}
}

func TestEngineDispatchFallsBackToDefaultWhenVariantNotHandled(t *testing.T) {
	engine := New()
	h := &variantDecisionHandler{name: "PHY_PROJECT", handled: false}
	engine.RegisterHandler(h)

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{{
			ID:       "p1",
			Op:       "PHY_PROJECT",
			Children: nil,
			Attrs:    map[string]any{"variant": "project_unknown"},
		}},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if h.variantCalls != 1 || h.lastVariantKey != "project_unknown" {
		t.Fatalf("expected variant dispatch attempt before fallback, got calls=%d variant=%q", h.variantCalls, h.lastVariantKey)
	}
	if h.defaultCalls != 1 {
		t.Fatalf("expected default handler fallback for unhandled variant")
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_PROJECT:default"}) {
		t.Fatalf("expected default execution after unhandled variant, got %#v", res.ExecutedOps)
	}
}

func TestEngineDispatchSurfacesVariantHandlerError(t *testing.T) {
	engine := New()
	h := &variantDecisionHandler{name: "PHY_PROJECT", handled: true, variantErr: errors.New("variant dispatch failure")}
	engine.RegisterHandler(h)

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{{
			ID:       "p1",
			Op:       "PHY_PROJECT",
			Children: nil,
			Attrs:    map[string]any{"variant": "project_fast"},
		}},
	}

	_, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err == nil {
		t.Fatalf("expected variant handler error to surface")
	}
	if h.defaultCalls != 0 {
		t.Fatalf("expected variant error to bypass default fallback")
	}
}

func TestEngineDispatchSurfacesDefaultHandlerError(t *testing.T) {
	engine := New()
	h := &variantDecisionHandler{name: "PHY_PROJECT", defaultErr: errors.New("default execution failure")}
	engine.RegisterHandler(h)

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{{
			ID:       "p1",
			Op:       "PHY_PROJECT",
			Children: nil,
			Attrs:    map[string]any{},
		}},
	}

	_, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err == nil {
		t.Fatalf("expected default handler error to surface")
	}
	if h.defaultCalls != 1 {
		t.Fatalf("expected default handler execution attempt")
	}
}

func TestEngineDispatchInvokesSortVariantHandlerPath(t *testing.T) {
	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{{"score": 10}, {"score": 30}, {"score": 20}, {"score": 40}},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_SORT", Children: []string{"p1"}, Attrs: map[string]any{
				"variant":  "sort_topk_heap",
				"strategy": "topk_heap",
				"topK":     2,
				"ordering": []map[string]any{{"expression": "score", "direction": "DESC"}},
			}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_EXPAND_MATCH", "PHY_SORT"}) {
		t.Fatalf("unexpected executed ops: %#v", res.ExecutedOps)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected topk variant to retain 2 rows, got %#v", res.Rows)
	}
	if got, _ := res.Rows[0]["score"].(int); got != 40 {
		t.Fatalf("unexpected top row after topk variant dispatch: %#v", res.Rows)
	}
	if got, _ := res.Rows[1]["score"].(int); got != 30 {
		t.Fatalf("unexpected second row after topk variant dispatch: %#v", res.Rows)
	}
}

func TestEngineDispatchStrictVariantRejectsUnsupportedRuntimeVariant(t *testing.T) {
	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{{"score": 10}, {"score": 30}},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_SORT", Children: []string{"p1"}, Attrs: map[string]any{
				"variant":  "sort_unknown_variant",
				"ordering": []map[string]any{{"expression": "score", "direction": "DESC"}},
			}},
		},
	}

	_, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{StrictVariantDispatchParam: true}})
	if err == nil {
		t.Fatalf("expected strict variant mode to reject unsupported runtime variant")
	}
}

func TestEngineDispatchStrictVariantIgnoresNonVariantizedOpFamilies(t *testing.T) {
	engine := New()
	h := &variantRecordingHandler{name: "PHY_PROJECT"}
	engine.RegisterHandler(h)

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p1",
		Nodes: []pipeline.PhysicalNode{{
			ID:       "p1",
			Op:       "PHY_PROJECT",
			Children: nil,
			Attrs:    map[string]any{"variant": "project_fast"},
		}},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{StrictVariantDispatchParam: true}})
	if err != nil {
		t.Fatalf("expected strict mode to ignore non-variantized op family variant attrs, got %v", err)
	}
	if len(h.variantHits) != 1 || h.variantHits[0] != "project_fast" {
		t.Fatalf("expected project variant handler path to execute, got %#v", h.variantHits)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_PROJECT:variant:project_fast"}) {
		t.Fatalf("expected variant handler execution for project op, got %#v", res.ExecutedOps)
	}
}

func TestEngineDispatchNonStrictVariantFallsBackForUnsupportedRuntimeVariant(t *testing.T) {
	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{{"score": 10}, {"score": 30}, {"score": 20}},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_SORT", Children: []string{"p1"}, Attrs: map[string]any{
				"variant":  "sort_unknown_variant",
				"ordering": []map[string]any{{"expression": "score", "direction": "DESC"}},
			}},
		},
	}

	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("expected non-strict mode to fall back, got error: %v", err)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_EXPAND_MATCH", "PHY_SORT"}) {
		t.Fatalf("unexpected executed ops under non-strict fallback: %#v", res.ExecutedOps)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected full sort fallback path to preserve row count, got %#v", res.Rows)
	}
}

func TestEngineDispatchInvokesAntiProbeVariantHandlerPath(t *testing.T) {
	tx := &antiProbeDispatchTx{recordingTx: &recordingTx{}}

	engine := New()
	engine.RegisterHandler(&seedRowsHandler{
		name: "PHY_EXPAND_MATCH",
		rows: []map[string]any{
			{"a.id": "u1", "b.id": "u2"},
			{"a.id": "u1", "b.id": "u3"},
		},
	})

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Children: nil, Attrs: map[string]any{}},
			{ID: "p2", Op: "PHY_ANTI_PROBE", Children: []string{"p1"}, Attrs: map[string]any{
				"variant":  "anti_probe_row_low",
				"leftVar":  "a",
				"rightVar": "b",
				"edgeType": "KNOWS",
				"mode":     "directed",
			}},
		},
	}

	res, err := engine.ExecuteWithTx(t.Context(), pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if !reflect.DeepEqual(res.ExecutedOps, []string{"PHY_EXPAND_MATCH", "PHY_ANTI_PROBE"}) {
		t.Fatalf("unexpected executed ops: %#v", res.ExecutedOps)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row to survive anti-probe row variant, got %#v", res.Rows)
	}
	if got, _ := res.Rows[0]["b.id"].(string); got != "u3" {
		t.Fatalf("expected non-neighbor row to survive anti-probe, got %#v", res.Rows)
	}
	if tx.rowCalls == 0 {
		t.Fatalf("expected anti-probe row variant to call row probe path")
	}
	if tx.batchCalls != 0 {
		t.Fatalf("expected anti-probe row variant to bypass batch probe path")
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
