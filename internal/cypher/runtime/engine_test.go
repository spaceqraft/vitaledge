package runtime

import (
	"os"
	"reflect"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/logical"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/cypher/pipeline"
	"github.com/spaceqraft/vitaledge/internal/cypher/runtime/operators"
	"github.com/spaceqraft/vitaledge/internal/cypher/semantic"
	"github.com/spaceqraft/vitaledge/internal/graph"
	pebblestore "github.com/spaceqraft/vitaledge/internal/graph/store/pebble"
)

func TestExecutePhysicalPlanMatchQuery(t *testing.T) {
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
	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_PROJECT", "PHY_SORT", "PHY_PAGINATION"}
	if !reflect.DeepEqual(res.ExecutedOps, expected) {
		t.Fatalf("unexpected executed ops: got=%v want=%v", res.ExecutedOps, expected)
	}
	if res.Stats.OperatorsExecuted != len(expected) {
		t.Fatalf("unexpected operator count stats: %#v", res.Stats)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", res.Rows)
	}
	if _, ok := res.Rows[0]["name"]; !ok {
		t.Fatalf("expected projected alias key 'name', got %#v", res.Rows[0])
	}
}

func TestExecutePhysicalPlanWriteQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
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
	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_WRITE", "PHY_PROJECT", "PHY_PROJECT"}
	if !reflect.DeepEqual(res.ExecutedOps, expected) {
		t.Fatalf("unexpected executed ops: got=%v want=%v", res.ExecutedOps, expected)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one projected row after write path, got %#v", res.Rows)
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected alias key 'uid', got %#v", res.Rows[0])
	}
	if res.Stats.WritesRecorded != 1 {
		t.Fatalf("expected one write event recorded, got %#v", res.Stats)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	event := res.WriteEvents[0]
	if event.MutationType != operators.MutationTypeEdge {
		t.Fatalf("expected edge write event, got %#v", event)
	}
	if event.Edge == nil || event.Edge.Type != "KNOWS" {
		t.Fatalf("expected KNOWS edge payload, got %#v", event)
	}
	if len(event.Edge.RightLabels) != 1 || event.Edge.RightLabels[0] != "User" {
		t.Fatalf("expected User right endpoint label, got %#v", event.Edge)
	}
	if got := event.ResolvedParams["peer"]; got != "u2" {
		t.Fatalf("expected peer param in surfaced write event, got %#v", event.ResolvedParams)
	}
	if event.NodeID == "" {
		t.Fatalf("expected write event node id, got %#v", event)
	}
}

func TestExecuteWithTxAppliesWriteEvents(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
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
	engine.RegisterHandler(&seedRowsHandler{name: "PHY_EXPAND_MATCH", rows: []map[string]any{{"u": "u1"}}})
	tx := &recordingTx{}
	res, err := engine.ExecuteWithTx(t.Context(), pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}}, tx)
	if err != nil {
		t.Fatalf("runtime execute with tx failed: %v", err)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one labeled endpoint vertex write, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", tx.edges)
	}
	if tx.vertexes[0].ID != "u2" {
		t.Fatalf("expected labeled right endpoint upsert, got %#v", tx.vertexes[0])
	}
	if tx.edges[0].SrcID != "u1" || tx.edges[0].DstID != "u2" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied edge: %#v", tx.edges[0])
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected uid in result rows, got %#v", res.Rows)
	}
}

func TestExecuteWithTxAppliesReverseWriteEvents(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)<-[:KNOWS]-(:User {id:$peer}) WITH u RETURN u.id AS uid")
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
	engine.RegisterHandler(&seedRowsHandler{name: "PHY_EXPAND_MATCH", rows: []map[string]any{{"u": "u1"}}})
	tx := &recordingTx{}
	res, err := engine.ExecuteWithTx(t.Context(), pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}}, tx)
	if err != nil {
		t.Fatalf("runtime execute with tx failed: %v", err)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one labeled endpoint vertex write, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", tx.edges)
	}
	if tx.vertexes[0].ID != "u2" {
		t.Fatalf("expected labeled right endpoint upsert, got %#v", tx.vertexes[0])
	}
	if tx.edges[0].SrcID != "u2" || tx.edges[0].DstID != "u1" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied reverse edge: %#v", tx.edges[0])
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected uid in result rows, got %#v", res.Rows)
	}
}

func TestExecuteWithTxExpandMatchUsesAdjacency(t *testing.T) {
	ctx := t.Context()
	dir, err := os.MkdirTemp("", "ve-runtime-expand-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, v := range []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}},
		} {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{v}); err != nil {
				return err
			}
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Attrs: map[string]any{"pattern": "(u:User)-[:KNOWS]->(v:User)"}},
			{ID: "p2", Op: "PHY_PROJECT", Children: []string{"p1"}, Attrs: map[string]any{"items": []string{"id(u) AS uid", "id(v) AS vid"}}},
		},
	}

	engine := New()
	res, err := engine.ExecuteWithTx(ctx, pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("execute with tx failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one expanded row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u1" {
		t.Fatalf("expected uid u1, got %#v", res.Rows[0])
	}
	if got := res.Rows[0]["vid"]; got != "u2" {
		t.Fatalf("expected vid u2, got %#v", res.Rows[0])
	}
}

func TestExecuteWithTxOptionalExpandPreservesRowWhenNoMatch(t *testing.T) {
	ctx := t.Context()
	dir, err := os.MkdirTemp("", "ve-runtime-optional-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, v := range []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}},
		} {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{v}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p3",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Attrs: map[string]any{"pattern": "(u:User)"}},
			{ID: "p2", Op: "PHY_EXPAND_OPTIONAL", Children: []string{"p1"}, Attrs: map[string]any{"pattern": "(u:User)-[:KNOWS]->(v:User)"}},
			{ID: "p3", Op: "PHY_PROJECT", Children: []string{"p2"}, Attrs: map[string]any{"items": []string{"id(u) AS uid", "id(v) AS vid"}}},
		},
	}

	engine := New()
	res, err := engine.ExecuteWithTx(ctx, pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("execute with tx failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected optional expansion to preserve rows, got %#v", res.Rows)
	}
	if foundNil := func() bool {
		for _, row := range res.Rows {
			if row["vid"] == nil {
				return true
			}
		}
		return false
	}(); !foundNil {
		t.Fatalf("expected at least one preserved row with null vid, got %#v", res.Rows)
	}
}

func TestExecuteWithTxExpandVariantControlsUndirectedUnboundBindingOrientation(t *testing.T) {
	ctx := t.Context()
	dir, err := os.MkdirTemp("", "ve-runtime-expand-variant-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
		}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	engine := New()

	outPlan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Attrs: map[string]any{"pattern": "(u:User)-[:KNOWS]-(v:User)", "variant": "expand_out_first"}},
			{ID: "p2", Op: "PHY_PROJECT", Children: []string{"p1"}, Attrs: map[string]any{"items": []string{"id(u) AS uid", "id(v) AS vid"}}},
		},
	}
	outRes, err := engine.ExecuteWithTx(ctx, pipeline.PhysicalExecutionInput{Plan: outPlan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("execute out_first failed: %v", err)
	}
	if len(outRes.Rows) == 0 {
		t.Fatalf("expected out_first rows, got %#v", outRes.Rows)
	}
	foundOutPreferred := false
	for _, row := range outRes.Rows {
		if row["uid"] == "u1" && row["vid"] == "u2" {
			foundOutPreferred = true
			break
		}
	}
	if !foundOutPreferred {
		t.Fatalf("expected out_first variant to include uid=u1,vid=u2 orientation, got %#v", outRes.Rows)
	}

	inPlan := pipeline.PhysicalPlan{
		RootNodeID: "p2",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Attrs: map[string]any{"pattern": "(u:User)-[:KNOWS]-(v:User)", "variant": "expand_in_first"}},
			{ID: "p2", Op: "PHY_PROJECT", Children: []string{"p1"}, Attrs: map[string]any{"items": []string{"id(u) AS uid", "id(v) AS vid"}}},
		},
	}
	inRes, err := engine.ExecuteWithTx(ctx, pipeline.PhysicalExecutionInput{Plan: inPlan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("execute in_first failed: %v", err)
	}
	if len(inRes.Rows) == 0 {
		t.Fatalf("expected in_first rows, got %#v", inRes.Rows)
	}
	foundInPreferred := false
	for _, row := range inRes.Rows {
		if row["uid"] == "u2" && row["vid"] == "u1" {
			foundInPreferred = true
			break
		}
	}
	if !foundInPreferred {
		t.Fatalf("expected in_first variant to include uid=u2,vid=u1 orientation, got %#v", inRes.Rows)
	}
}

func TestExecuteWithTxAntiProbeFiltersBlockedRelationships(t *testing.T) {
	ctx := t.Context()
	dir, err := os.MkdirTemp("", "ve-runtime-antiprobe-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := pebblestore.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, v := range []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}},
		} {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{v}); err != nil {
				return err
			}
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "u1", DstID: "u3"}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "b1", Type: "BLOCKED", SrcID: "u1", DstID: "u2"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	defer tx.Rollback()

	plan := pipeline.PhysicalPlan{
		RootNodeID: "p3",
		Nodes: []pipeline.PhysicalNode{
			{ID: "p1", Op: "PHY_EXPAND_MATCH", Attrs: map[string]any{"pattern": "(u:User)-[:KNOWS]->(v:User)"}},
			{ID: "p2", Op: "PHY_ANTI_PROBE", Children: []string{"p1"}, Attrs: map[string]any{"leftVar": "u", "rightVar": "v", "edgeType": "BLOCKED", "mode": "undirected"}},
			{ID: "p3", Op: "PHY_PROJECT", Children: []string{"p2"}, Attrs: map[string]any{"items": []string{"id(u) AS uid", "id(v) AS vid"}}},
		},
	}

	engine := New()
	res, err := engine.ExecuteWithTx(ctx, pipeline.PhysicalExecutionInput{Plan: plan, Tenant: "acme", Params: map[string]any{}}, tx)
	if err != nil {
		t.Fatalf("execute with tx failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after anti-probe filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u1" {
		t.Fatalf("expected uid=u1, got %#v", res.Rows)
	}
	if got := res.Rows[0]["vid"]; got != "u3" {
		t.Fatalf("expected only unblocked vid=u3, got %#v", res.Rows)
	}
}
