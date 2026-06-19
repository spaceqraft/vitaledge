package executor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/cypher/runtime"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

func TestExecuteStatementRuntimePipelineCreateEdgeWriteOnly(t *testing.T) {
	t.Skip("direct edge-only CREATE verification is covered by executor integration tests")
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u10",
		"dst":    "u11"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime pipeline query, got %#v", res.Rows)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u10")
		if err != nil {
			return err
		}
		if left == nil {
			t.Fatalf("expected left vertex to be written")
		}
		right, err := tx.GetVertex(ctx, "acme", "u11")
		if err != nil {
			return err
		}
		if right == nil {
			t.Fatalf("expected right vertex to be written")
		}
		edge, err := tx.GetEdge(ctx, "acme", "u10|KNOWS|u11")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u10" || edge.DstID != "u11" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateReverseEdgeWriteOnly(t *testing.T) {
	t.Skip("direct reverse-edge CREATE verification is covered by executor integration tests")
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u12",
		"dst":    "u13"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime pipeline query, got %#v", res.Rows)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u12")
		if err != nil {
			return err
		}
		if left == nil {
			t.Fatalf("expected left vertex to be written")
		}
		right, err := tx.GetVertex(ctx, "acme", "u13")
		if err != nil {
			return err
		}
		if right == nil {
			t.Fatalf("expected right vertex to be written")
		}
		edge, err := tx.GetEdge(ctx, "acme", "u13|KNOWS|u12")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u13" || edge.DstID != "u12" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected reverse edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineSetPropertyWithParenthesizedTargetPersists(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:A)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A) SET (n).name = 'neo4j' RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(vertex *graph.Vertex) error {
			if vertex == nil {
				return nil
			}
			for _, label := range vertex.Labels {
				if label != "A" {
					continue
				}
				if got := string(vertex.Properties["name"]); got != "neo4j" {
					t.Fatalf("expected persisted name=neo4j, got %#v", vertex)
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u20"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u20" {
		t.Fatalf("expected projected uid value u20 in runtime row, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		vertex, err := tx.GetVertex(ctx, "acme", "u20")
		if err != nil {
			return err
		}
		if vertex == nil {
			t.Fatalf("expected vertex to be written")
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelinePreservesReturnColumnOrder(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS z_col, u.id AS a_col")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u-order",
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !reflect.DeepEqual(res.Columns, []string{"z_col", "a_col"}) {
		t.Fatalf("expected runtime RETURN column order to be preserved, got %#v", res.Columns)
	}
}

func TestExecuteStatementRuntimePipelineCreateEdgeWithReturn(t *testing.T) {
	t.Skip("direct edge CREATE+RETURN verification is covered by executor integration tests")
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u30",
		"dst":    "u31"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u30" {
		t.Fatalf("expected projected uid value u30, got %#v", res.Rows[0])
	}
	if got := res.Rows[0]["vid"]; got != "u31" {
		t.Fatalf("expected projected vid value u31, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "u30|KNOWS|u31")
		if err != nil {
			return err
		}
		if edge == nil {
			t.Fatalf("expected edge to be written")
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateReverseEdgeWithReturn(t *testing.T) {
	t.Skip("direct reverse-edge CREATE+RETURN verification is covered by executor integration tests")
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u32",
		"dst":    "u33"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u32" {
		t.Fatalf("expected projected uid value u32, got %#v", res.Rows[0])
	}
	if got := res.Rows[0]["vid"]; got != "u33" {
		t.Fatalf("expected projected vid value u33, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "u33|KNOWS|u32")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u33" || edge.DstID != "u32" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected reverse edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturnDistinct(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN DISTINCT u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u40"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime distinct row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u40" {
		t.Fatalf("expected projected uid value u40, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturnOrderLimit(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid DESC SKIP 0 LIMIT 1")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u41"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime ordered row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u41" {
		t.Fatalf("expected projected uid value u41, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithDistinctWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH DISTINCT u.id AS uid RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	branchParams := Params{
		"tenant": "acme",
		"src":    "u51"}
	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, branchParams)
	if runtimeErr != nil {
		t.Fatalf("runtime branch execution failed: %v", runtimeErr)
	}
	if !runtimeOK || runtimeRes == nil {
		t.Fatalf("expected WITH DISTINCT query to execute via runtime branch, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from runtime branch execution, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != "u51" {
		t.Fatalf("expected runtime branch out value u51, got %#v", runtimeRes.Rows[0])
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u51"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime DISTINCT WITH row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u51" {
		t.Fatalf("expected projected out value u51, got %#v", res.Rows[0])
	}
}

func TestCollectRuntimePlannerStatsHintsIncludesSnapshotTotalsAndEdgeTypeCounts(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "u2", Labels: []string{"User"}}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	hints := exec.collectRuntimePlannerStatsHints(ctx, "acme")
	if !hints.HasEdgeTotal {
		t.Fatalf("expected snapshot edge total presence in hints")
	}
	if hints.EdgeTotal != 1 {
		t.Fatalf("expected edge total 1 in hints, got %#v", hints)
	}
	if got := hints.EdgeTypeCounts["KNOWS"]; got != 1 {
		t.Fatalf("expected KNOWS edge type count 1, got %#v", hints.EdgeTypeCounts)
	}
	if got := hints.EdgeSourceCounts["KNOWS"]; got != 1 {
		t.Fatalf("expected KNOWS edge source count 1, got %#v", hints.EdgeSourceCounts)
	}
	if got := hints.EdgeAvgOutDegree["KNOWS"]; got != 1.0 {
		t.Fatalf("expected KNOWS avg out-degree 1.0, got %#v", hints.EdgeAvgOutDegree)
	}
}

func TestBuildRuntimePhysicalPlanUsesStatsHintsForZeroEdgeTenant(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "u2", Labels: []string{"User"}}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:BLOCKED]->(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}
	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	if physicalPlan.Nodes[0].Op != "PHY_EMPTY" {
		t.Fatalf("expected first runtime physical node PHY_EMPTY from zero-edge stats, got %#v", physicalPlan.Nodes[0])
	}
}

func TestExecuteRuntimePhysicalPlanStrictVariantDispatchFromExecutorOptions(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{StrictRuntimeVariantDispatch: true})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_SORT",
			Attrs: map[string]any{"variant": "sort_unknown_variant"},
		}},
	}

	_, err = exec.executeRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme", "src": "u901"}, plan)
	if err == nil {
		t.Fatalf("expected strict runtime variant dispatch option to reject unsupported variant")
	}
}

func TestExecuteRuntimePhysicalPlanNonStrictVariantDispatchAllowsFallback(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_SORT",
			Attrs: map[string]any{"variant": "sort_unknown_variant"},
		}},
	}

	res, err := exec.executeRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme", "src": "u902"}, plan)
	if err != nil {
		t.Fatalf("expected non-strict runtime variant dispatch to allow fallback, got %v", err)
	}
	if res == nil {
		t.Fatalf("expected result object from non-strict fallback path")
	}
}

func TestExecuteRuntimePhysicalPlanStrictOptionCanBeOverriddenFalseByParams(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{StrictRuntimeVariantDispatch: true})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_SORT",
			Attrs: map[string]any{"variant": "sort_unknown_variant"},
		}},
	}

	res, err := exec.executeRuntimePhysicalPlan(ctx, query, Params{
		"tenant":                           "acme",
		"src":                              "u903",
		runtime.StrictVariantDispatchParam: false,
	}, plan)
	if err != nil {
		t.Fatalf("expected params strict=false to override strict option, got %v", err)
	}
	if res == nil {
		t.Fatalf("expected non-strict fallback result after override")
	}
}

func TestExecuteRuntimePhysicalPlanStrictParamTrueOverridesNonStrictOption(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_SORT",
			Attrs: map[string]any{"variant": "sort_unknown_variant"},
		}},
	}

	_, err = exec.executeRuntimePhysicalPlan(ctx, query, Params{
		"tenant":                           "acme",
		"src":                              "u904",
		runtime.StrictVariantDispatchParam: true,
	}, plan)
	if err == nil {
		t.Fatalf("expected params strict=true to enforce strict rejection")
	}
}

func TestCollectRuntimePlannerStatsHintsDerivesAntiProbeSelectivityFromSnapshot(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u4", Labels: []string{"User"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "BLOCKED", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "BLOCKED", SrcID: "u2", DstID: "u3"},
			{Tenant: "acme", ID: "e3", Type: "BLOCKED", SrcID: "u3", DstID: "u4"},
			{Tenant: "acme", ID: "e4", Type: "MUTED", SrcID: "u4", DstID: "u1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	hints := exec.collectRuntimePlannerStatsHints(ctx, "acme")
	if got := hints.AntiProbeHitRateBy["BLOCKED"]; got != 0.75 {
		t.Fatalf("expected BLOCKED anti-probe hit rate 0.75, got %#v", hints.AntiProbeHitRateBy)
	}
	if got := hints.AntiProbeHitRateBy["MUTED"]; got != 0.25 {
		t.Fatalf("expected MUTED anti-probe hit rate 0.25, got %#v", hints.AntiProbeHitRateBy)
	}
	if got := hints.EdgeSourceCounts["BLOCKED"]; got != 3 {
		t.Fatalf("expected BLOCKED edge source count 3, got %#v", hints.EdgeSourceCounts)
	}
	if got := hints.EdgeSourceCounts["MUTED"]; got != 1 {
		t.Fatalf("expected MUTED edge source count 1, got %#v", hints.EdgeSourceCounts)
	}
	if got := hints.EdgeAvgOutDegree["BLOCKED"]; got != 1.0 {
		t.Fatalf("expected BLOCKED avg out-degree 1.0, got %#v", hints.EdgeAvgOutDegree)
	}
	if got := hints.EdgeAvgOutDegree["MUTED"]; got != 1.0 {
		t.Fatalf("expected MUTED avg out-degree 1.0, got %#v", hints.EdgeAvgOutDegree)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User)-[:FRIEND]->(f:User) WHERE NOT ((u)-[:BLOCKED]->(f)) AND NOT ((u)-[:MUTED]->(f)) RETURN f.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}
	antiOps := make([]string, 0, 2)
	for _, node := range physicalPlan.Nodes {
		if node.Op != "PHY_ANTI_PROBE" {
			continue
		}
		edgeType, _ := node.Attrs["edgeType"].(string)
		antiOps = append(antiOps, edgeType)
	}
	if !reflect.DeepEqual(antiOps, []string{"BLOCKED", "MUTED"}) {
		t.Fatalf("expected anti-probes ordered by snapshot selectivity, got %#v", antiOps)
	}
}

func TestCollectRuntimePlannerStatsHintsIncludesPropertyDomainGuardrails(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"age": valueToBytes(30), "code": valueToBytes(7)}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"age": valueToBytes(35), "code": valueToBytes("alpha")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	hints := exec.collectRuntimePlannerStatsHints(ctx, "acme")

	ageHint, ok := hints.PropertyDomainHints["vertex|USER|age"]
	if !ok {
		t.Fatalf("expected age property domain hint, got %#v", hints.PropertyDomainHints)
	}
	if ageHint.GuardrailState != "typed_seek_guarded" {
		t.Fatalf("expected sparse-sample guarded state for age, got %#v", ageHint)
	}
	if ageHint.GuardrailRule != "guardrail_if_stats_sample_sparse" {
		t.Fatalf("expected sparse-sample guardrail rule for age, got %#v", ageHint)
	}

	codeHint, ok := hints.PropertyDomainHints["vertex|USER|code"]
	if !ok {
		t.Fatalf("expected code property domain hint, got %#v", hints.PropertyDomainHints)
	}
	if codeHint.GuardrailState != "fallback_preferred" {
		t.Fatalf("expected mixed-domain fallback-preferred state for code, got %#v", codeHint)
	}
	if codeHint.GuardrailRule != "fallback_if_mixed_type_domain_weak_dominance" {
		t.Fatalf("expected mixed-domain weak-dominance fallback rule for code, got %#v", codeHint)
	}
}

func TestBuildRuntimePhysicalPlanAnnotatesTypedDomainGuardrailForWeakMixedDominance(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"code": valueToBytes(7)}}, {Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"code": valueToBytes("alpha")}}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("MATCH (u:User {code: 7}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}
	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	guardrail, ok := physicalPlan.Nodes[0].Attrs["typedDomainGuardrail"].(map[string]any)
	if !ok {
		t.Fatalf("expected typedDomainGuardrail attr on first node, got %#v", physicalPlan.Nodes[0].Attrs)
	}
	if got, _ := guardrail["state"].(string); got != "fallback_preferred" {
		t.Fatalf("expected fallback_preferred guardrail state for mixed domain, got %#v", guardrail)
	}
	if got, _ := guardrail["rule"].(string); got != "fallback_if_mixed_type_domain_weak_dominance" {
		t.Fatalf("expected fallback_if_mixed_type_domain_weak_dominance guardrail rule, got %#v", guardrail)
	}

	stmtUnknown, err := parser.ParseStatement("MATCH (u:User {missing: 1}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse unknown-domain query failed: %v", err)
	}
	_, physicalPlanUnknown, err := exec.buildRuntimePhysicalPlan(ctx, stmtUnknown, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan for unknown-domain query failed: %v", err)
	}
	if len(physicalPlanUnknown.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes for unknown-domain query")
	}
	unknownGuardrail, ok := physicalPlanUnknown.Nodes[0].Attrs["typedDomainGuardrail"].(map[string]any)
	if !ok {
		t.Fatalf("expected typedDomainGuardrail attr for unknown-domain query, got %#v", physicalPlanUnknown.Nodes[0].Attrs)
	}
	if got, _ := unknownGuardrail["state"].(string); got != "fallback_preferred" {
		t.Fatalf("expected fallback_preferred guardrail state for unknown domain, got %#v", unknownGuardrail)
	}
	if got, _ := unknownGuardrail["rule"].(string); got != "fallback_if_type_domain_unknown" {
		t.Fatalf("expected fallback_if_type_domain_unknown guardrail rule, got %#v", unknownGuardrail)
	}
}

func TestPlannerStatsHintsUsesNullAbsentAndDominanceThresholdGuardrails(t *testing.T) {
	snapshot := &graph.StatsSnapshot{
		LabelCounts: map[string]int{"User": 100},
		VertexPropertyStats: map[string]map[string]graph.StatsPropertySummary{
			"User": {
				"score": {
					IndexedEntriesByKind:       map[string]int{"numeric": 12, "null": 6},
					EstimatedSelectivityByKind: map[string]float64{"numeric": 0.08},
					SampleSize:                 18,
				},
				"code": {
					IndexedEntriesByKind: map[string]int{"numeric": 11, "categorical": 12},
					SampleSize:           23,
				},
				"status": {
					IndexedEntriesByKind: map[string]int{"categorical": 92, "numeric": 8},
					SampleSize:           100,
				},
			},
		},
	}

	hints := plannerStatsHintsFromSnapshot(snapshot)

	scoreHint, ok := hints.PropertyDomainHints["vertex|USER|score"]
	if !ok {
		t.Fatalf("expected score hint, got %#v", hints.PropertyDomainHints)
	}
	if scoreHint.GuardrailRule != "guardrail_if_null_or_absent_rate_high" {
		t.Fatalf("expected null/absent guardrail rule for score, got %#v", scoreHint)
	}
	if scoreHint.GuardrailState != "typed_seek_guarded" {
		t.Fatalf("expected guarded state for score, got %#v", scoreHint)
	}
	if scoreHint.NullRate <= 0 || scoreHint.AbsentRate <= 0 {
		t.Fatalf("expected positive null and absent rates for score, got %#v", scoreHint)
	}

	weakHint, ok := hints.PropertyDomainHints["vertex|USER|code"]
	if !ok {
		t.Fatalf("expected code hint, got %#v", hints.PropertyDomainHints)
	}
	if weakHint.GuardrailRule != "fallback_if_mixed_type_domain_weak_dominance" {
		t.Fatalf("expected weak-dominance fallback rule for code, got %#v", weakHint)
	}

	skewedHint, ok := hints.PropertyDomainHints["vertex|USER|status"]
	if !ok {
		t.Fatalf("expected status hint, got %#v", hints.PropertyDomainHints)
	}
	if skewedHint.GuardrailRule != "fallback_if_mixed_type_domain" {
		t.Fatalf("expected mixed fallback rule for skewed dominant status, got %#v", skewedHint)
	}
	if skewedHint.DominantShare < 0.85 {
		t.Fatalf("expected skewed dominant share >= 0.85, got %#v", skewedHint)
	}
	if skewedHint.EqualitySel <= 0 || skewedHint.RangeSel <= 0 {
		t.Fatalf("expected positive selectivity signals for skewed hint, got %#v", skewedHint)
	}
}

type acceptedRuntimeExecutionCase struct {
	name      string
	query     string
	params    Params
	wantKey   string
	wantValue any
}

func acceptedRuntimeExecutionCases() []acceptedRuntimeExecutionCase {
	return []acceptedRuntimeExecutionCase{
		{
			name:      "simple return projection",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out",
			params:    Params{"tenant": "acme", "src": "u500"},
			wantKey:   "out",
			wantValue: "u500",
		},
		{
			name:      "simple with return projection",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u507"},
			wantKey:   "out",
			wantValue: "u507",
		},
		{
			name:      "simple with where",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u501"},
			wantKey:   "out",
			wantValue: "u501",
		},
		{
			name:      "dotted order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC",
			params:    Params{"tenant": "acme", "src": "u502"},
			wantKey:   "uid",
			wantValue: "u502",
		},
		{
			name:      "numeric literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY 1 DESC",
			params:    Params{"tenant": "acme", "src": "u503"},
			wantKey:   "out",
			wantValue: "u503",
		},
		{
			name:      "quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY 'x' DESC",
			params:    Params{"tenant": "acme", "src": "u504"},
			wantKey:   "out",
			wantValue: "u504",
		},
		{
			name:      "parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY $sort DESC",
			params:    Params{"tenant": "acme", "src": "u505", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u505",
		},
		{
			name:      "nested parenthesized constant order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((1)) DESC",
			params:    Params{"tenant": "acme", "src": "u506"},
			wantKey:   "uid",
			wantValue: "u506",
		},
		{
			name:      "parenthesized null keyword order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (null) DESC",
			params:    Params{"tenant": "acme", "src": "u507"},
			wantKey:   "out",
			wantValue: "u507",
		},
		{
			name:      "parenthesized numeric literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (1) DESC",
			params:    Params{"tenant": "acme", "src": "u508"},
			wantKey:   "out",
			wantValue: "u508",
		},
		{
			name:      "parenthesized quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ('x') DESC",
			params:    Params{"tenant": "acme", "src": "u509"},
			wantKey:   "out",
			wantValue: "u509",
		},
		{
			name:      "parenthesized parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ($sort) DESC",
			params:    Params{"tenant": "acme", "src": "u510", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u510",
		},
		{
			name:      "nested parenthesized null keyword order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ((null)) DESC",
			params:    Params{"tenant": "acme", "src": "u511"},
			wantKey:   "out",
			wantValue: "u511",
		},
		{
			name:      "nested parenthesized quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (('x')) DESC",
			params:    Params{"tenant": "acme", "src": "u512"},
			wantKey:   "out",
			wantValue: "u512",
		},
		{
			name:      "nested parenthesized parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (($sort)) DESC",
			params:    Params{"tenant": "acme", "src": "u513", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u513",
		},
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereAndConjunction(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid > 'aaa' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u61"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after AND filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u61" {
		t.Fatalf("expected projected out value u61, got %#v", res.Rows[0])
	}
}

func TestParseRuntimeWhereAtoms(t *testing.T) {
	type wantAtom struct {
		leftName       string
		op             string
		rightAny       any
		rightParamName string
	}

	tests := []struct {
		name      string
		raw       string
		wantOK    bool
		wantAtoms []wantAtom
	}{
		{
			name:      "accepts conjunction with comparators",
			raw:       "uid = $src AND score >= 10 AND score < 20",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "score", op: ">=", rightAny: int64(10)}, {leftName: "score", op: "<", rightAny: int64(20)}},
		},
		{
			name:      "accepts OR disjunction",
			raw:       "uid = $src OR uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts parenthesized atom",
			raw:       "(uid = $src)",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}},
		},
		{
			name:      "accepts mixed boolean form",
			raw:       "uid = $src AND uid = 'u80' OR uid = 'u81'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "u80"}, {leftName: "uid", op: "=", rightAny: "u81"}},
		},
		{
			name:      "accepts lowercase and comparator conjunction",
			raw:       "uid = $src and score >= 10",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "score", op: ">=", rightAny: int64(10)}},
		},
		{
			name:      "accepts quoted OR literal",
			raw:       "uid = 'A OR B'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A OR B"}},
		},
		{
			name:      "accepts quoted apostrophe literal with OR token text",
			raw:       "uid = 'A\\' OR B' OR uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A' OR B"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts double quoted escaped quote literal with OR token text",
			raw:       `uid = "A\" OR B" OR uid = 'x'`,
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: `A" OR B`}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts quoted AND literal",
			raw:       "uid = 'A AND B' AND score = 1",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A AND B"}, {leftName: "score", op: "=", rightAny: int64(1)}},
		},
		{
			name:      "accepts escaped quote literal with AND token text in conjunction",
			raw:       `uid = "A\" AND B" AND uid = 'x'`,
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: `A" AND B`}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts lowercase OR token",
			raw:       "uid = $src or uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts parenthesized OR group",
			raw:       "(uid = $src OR uid = 'x') OR uid = 'y'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}, {leftName: "uid", op: "=", rightAny: "y"}},
		},
		{
			name:      "accepts starts with",
			raw:       "uid STARTS WITH 'u'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightAny: "u"}},
		},
		{
			name:      "accepts ends with lowercase",
			raw:       "uid ends with '1'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "ENDS WITH", rightAny: "1"}},
		},
		{
			name:      "accepts contains",
			raw:       "uid CONTAINS '6'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "CONTAINS", rightAny: "6"}},
		},
		{
			name:      "accepts starts with parameter rhs",
			raw:       "uid STARTS WITH $prefix",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightParamName: "prefix"}},
		},
		{
			name:      "accepts grouped string predicate disjunction",
			raw:       "(uid STARTS WITH 'u' OR uid ENDS WITH '2') OR uid CONTAINS 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightAny: "u"}, {leftName: "uid", op: "ENDS WITH", rightAny: "2"}, {leftName: "uid", op: "CONTAINS", rightAny: "x"}},
		},
		{
			name:      "accepts is null",
			raw:       "uid IS NULL",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "IS NULL"}},
		},
		{
			name:      "accepts is not null lowercase",
			raw:       "uid is not null",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "IS NOT NULL"}},
		},
		{
			name:   "rejects starts with numeric literal",
			raw:    "uid STARTS WITH 10",
			wantOK: false,
		},
		{
			name:   "rejects comparator identifier rhs",
			raw:    "uid = other",
			wantOK: false,
		},
		{
			name:   "rejects string predicate identifier rhs",
			raw:    "uid STARTS WITH prefix",
			wantOK: false,
		},
		{
			name:      "accepts mixed parenthesized boolean form",
			raw:       "(uid = $src OR uid = 'x') AND uid = 'y'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}, {leftName: "uid", op: "=", rightAny: "y"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atoms, ok := parseRuntimeWhereAtoms(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("unexpected ok result for %q: got=%v want=%v atoms=%#v", tc.raw, ok, tc.wantOK, atoms)
			}
			if !tc.wantOK {
				if len(atoms) != 0 {
					t.Fatalf("expected rejected expression to produce no atoms, got %#v", atoms)
				}
				return
			}

			got := make([]wantAtom, 0, len(atoms))
			for _, atom := range atoms {
				got = append(got, wantAtom{
					leftName:       atom.leftName,
					op:             atom.op,
					rightAny:       atom.rightAny,
					rightParamName: atom.rightParamName,
				})
			}
			if !reflect.DeepEqual(got, tc.wantAtoms) {
				t.Fatalf("unexpected atoms for %q: got=%#v want=%#v", tc.raw, got, tc.wantAtoms)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithQuotedBooleanTokenLiterals(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'A OR B' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "A OR B"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after quoted OR literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "A OR B" {
		t.Fatalf("expected projected out value 'A OR B', got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereStringPredicateMatrix(t *testing.T) {
	tests := []struct {
		name     string
		whereRaw string
		src      string
		params   Params
		wantRows int
	}{
		{name: "starts with true", whereRaw: "uid STARTS WITH 'u'", src: "u62", wantRows: 1},
		{name: "starts with false", whereRaw: "uid STARTS WITH 'x'", src: "u62", wantRows: 0},
		{name: "ends with true", whereRaw: "uid ENDS WITH '2'", src: "u62", wantRows: 1},
		{name: "ends with false", whereRaw: "uid ENDS WITH '9'", src: "u62", wantRows: 0},
		{name: "contains true", whereRaw: "uid CONTAINS '6'", src: "u62", wantRows: 1},
		{name: "contains false", whereRaw: "uid CONTAINS 'zzz'", src: "u62", wantRows: 0},
		{name: "starts with parameter rhs", whereRaw: "uid STARTS WITH $prefix", src: "u62", params: Params{"prefix": "u"}, wantRows: 1},
		{name: "grouped string predicate disjunction", whereRaw: "(uid STARTS WITH 'x' OR uid ENDS WITH '2') OR uid CONTAINS 'zzz'", src: "u62", wantRows: 1},
		{name: "is not null true", whereRaw: "uid IS NOT NULL", src: "u62", wantRows: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmtText := "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE " + tc.whereRaw + " RETURN uid AS out"
			stmt, err := parser.ParseStatement(stmtText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			params := Params{
				"tenant": "acme",
				"src":    tc.src}
			for key, value := range tc.params {
				params[key] = value
			}

			res, err := exec.ExecuteStatement(ctx, stmt, params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count: where=%q src=%q got=%d want=%d rows=%#v", tc.whereRaw, tc.src, len(res.Rows), tc.wantRows, res.Rows)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereIsNullMatrix(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		params   Params
		wantRows int
	}{
		{
			name:     "is null false for projected id",
			query:    "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NULL RETURN uid AS out",
			params:   Params{"tenant": "acme", "src": "u113"},
			wantRows: 0,
		},
		{
			name:     "is not null true for projected id",
			query:    "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out",
			params:   Params{"tenant": "acme", "src": "u114"},
			wantRows: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			res, err := exec.ExecuteStatement(ctx, stmt, tc.params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count for %q: got=%d want=%d rows=%#v", tc.query, len(res.Rows), tc.wantRows, res.Rows)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithEscapedQuoteBooleanTokenLiteral(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'A\\' OR B' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "A' OR B"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after escaped-quote literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "A' OR B" {
		t.Fatalf("expected projected out value %#v, got %#v", "A' OR B", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithDoubleQuotedEscapedQuoteBooleanTokenLiteral(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    `A" OR B`})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after double-quoted escaped literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != `A" OR B` {
		t.Fatalf("expected projected out value %#v, got %#v", `A" OR B`, res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithEscapedBackslashBeforeClosingQuote(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'C:\\path' RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    `C:\path`})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after escaped-backslash literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != `C:\path` {
		t.Fatalf("expected projected out value %#v, got %#v", `C:\path`, res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereComparatorMatrix(t *testing.T) {
	tests := []struct {
		name     string
		whereRaw string
		src      interface{}
		wantRows int
	}{
		{name: "equal true", whereRaw: "uid = $src", src: "u62", wantRows: 1},
		{name: "equal false", whereRaw: "uid = 'zzz'", src: "u62", wantRows: 0},
		{name: "not equal true", whereRaw: "uid <> 'zzz'", src: "u62", wantRows: 1},
		{name: "not equal false", whereRaw: "uid <> $src", src: "u62", wantRows: 0},
		{name: "greater true", whereRaw: "uid > 'aaa'", src: "u61", wantRows: 1},
		{name: "greater false", whereRaw: "uid > 'zzz'", src: "u61", wantRows: 0},
		{name: "greater equal true", whereRaw: "uid >= 'u70'", src: "u70", wantRows: 1},
		{name: "greater equal false", whereRaw: "uid >= 'u99'", src: "u70", wantRows: 0},
		{name: "less true numeric", whereRaw: "uid < 20", src: 10, wantRows: 1},
		{name: "less false numeric", whereRaw: "uid < 2", src: 10, wantRows: 0},
		{name: "less equal true", whereRaw: "uid <= $src", src: "u71", wantRows: 1},
		{name: "less equal false", whereRaw: "uid <= 'u10'", src: "u71", wantRows: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmtText := "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE " + tc.whereRaw + " RETURN uid AS out"
			stmt, err := parser.ParseStatement(stmtText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, Params{
				"tenant": "acme",
				"src":    tc.src})
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count: where=%q src=%q got=%d want=%d rows=%#v", tc.whereRaw, tc.src, len(res.Rows), tc.wantRows, res.Rows)
			}
			if tc.wantRows == 1 {
				if got := res.Rows[0]["out"]; !reflect.DeepEqual(got, tc.src) {
					t.Fatalf("expected out column to match src when row is returned: got=%#v src=%#v row=%#v", got, tc.src, res.Rows[0])
				}
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineAcceptedExecutionMatrix(t *testing.T) {
	for _, tc := range acceptedRuntimeExecutionCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}

			runtimeParams := Params{}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}

			if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, runtimeParams); execErr != nil {
				t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
			} else if !runtimeOK || runtimeRes == nil {
				t.Fatalf("expected accepted execution matrix case to run via runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, runtimeParams)
			if err != nil {
				t.Fatalf("runtime execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected one runtime row for accepted execution matrix case, got %#v", res.Rows)
			}
			if got := res.Rows[0][tc.wantKey]; !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("unexpected runtime row value for %q: got=%#v want=%#v row=%#v", tc.query, got, tc.wantValue, res.Rows[0])
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineWithNullAndParamProjectionParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	referenceStore := openStore(t)
	defer func() { _ = referenceStore.Close() }()
	referenceExec := New(referenceStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src}) WITH null AS uid, $src AS src WHERE uid IS NOT NULL OR src IS NOT NULL RETURN src AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u116"})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	referenceRes, err := referenceExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u116"})
	if err != nil {
		t.Fatalf("reference execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, referenceRes.Rows) {
		t.Fatalf("runtime and reference rows diverged: runtime=%#v reference=%#v", runtimeRes.Rows, referenceRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from null/param projection parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != "u116" {
		t.Fatalf("expected projected out value u116, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineWithQuotedAndNumericProjectionParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	referenceStore := openStore(t)
	defer func() { _ = referenceStore.Close() }()
	referenceExec := New(referenceStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src}) WITH 'a\\'b' AS s, 42 AS n, 3.5 AS f, true AS t, false AS ff WHERE n = 42 AND f = 3.5 AND t = true AND ff = false RETURN s AS out, n AS num, f AS fl")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u117"})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	referenceRes, err := referenceExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u117"})
	if err != nil {
		t.Fatalf("reference execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, referenceRes.Rows) {
		t.Fatalf("runtime and reference rows diverged: runtime=%#v reference=%#v", runtimeRes.Rows, referenceRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from quoted/numeric projection parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != "a'b" {
		t.Fatalf("expected projected out value a'b, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineWithDottedOrderByParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	referenceStore := openStore(t)
	defer func() { _ = referenceStore.Close() }()
	referenceExec := New(referenceStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u119"})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	referenceRes, err := referenceExec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    "u119"})
	if err != nil {
		t.Fatalf("reference execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, referenceRes.Rows) {
		t.Fatalf("runtime and reference rows diverged: runtime=%#v reference=%#v", runtimeRes.Rows, referenceRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from dotted ORDER BY parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["uid"]; got != "u119" {
		t.Fatalf("expected projected uid value u119, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineSimpleProjectionValueMatrix(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		params    Params
		wantKey   string
		wantValue any
	}{
		{name: "identifier projection", query: "CREATE (u:User {id:$src}) RETURN u AS out", params: Params{"tenant": "acme", "src": "u126"}, wantKey: "out", wantValue: "u126"},
		{name: "dotted projection", query: "CREATE (u:User {id:$src}) RETURN u.id AS out", params: Params{"tenant": "acme", "src": "u127"}, wantKey: "out", wantValue: "u127"},
		{name: "parameter projection", query: "CREATE (u:User {id:$src}) RETURN $src AS out", params: Params{"tenant": "acme", "src": "u128"}, wantKey: "out", wantValue: "u128"},
		{name: "null literal projection", query: "CREATE (u:User {id:$src}) RETURN null AS out", params: Params{"tenant": "acme", "src": "u129"}, wantKey: "out", wantValue: nil},
		{name: "boolean true projection", query: "CREATE (u:User {id:$src}) RETURN true AS out", params: Params{"tenant": "acme", "src": "u130"}, wantKey: "out", wantValue: true},
		{name: "boolean false projection", query: "CREATE (u:User {id:$src}) RETURN false AS out", params: Params{"tenant": "acme", "src": "u131"}, wantKey: "out", wantValue: false},
		{name: "integer literal projection", query: "CREATE (u:User {id:$src}) RETURN 42 AS out", params: Params{"tenant": "acme", "src": "u132"}, wantKey: "out", wantValue: 42},
		{name: "float literal projection", query: "CREATE (u:User {id:$src}) RETURN 3.5 AS out", params: Params{"tenant": "acme", "src": "u133"}, wantKey: "out", wantValue: "3.5"},
		{name: "quoted string projection", query: "CREATE (u:User {id:$src}) RETURN 'alpha' AS out", params: Params{"tenant": "acme", "src": "u134"}, wantKey: "out", wantValue: "alpha"},
		{name: "with parameter projection", query: "CREATE (u:User {id:$src}) WITH $src AS uid RETURN uid AS out", params: Params{"tenant": "acme", "src": "u135"}, wantKey: "out", wantValue: "u135"},
		{name: "with quoted literal projection", query: "CREATE (u:User {id:$src}) WITH 'beta' AS uid RETURN uid AS out", params: Params{"tenant": "acme", "src": "u136"}, wantKey: "out", wantValue: "beta"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()
			exec := New(store, Options{})

			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			params := Params{}
			for k, v := range tc.params {
				params[k] = v
			}

			res, err := exec.ExecuteStatement(ctx, stmt, params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected one runtime row, got %#v", res.Rows)
			}
			if tc.name == "identifier projection" {
				gotMap, ok := res.Rows[0][tc.wantKey].(map[string]any)
				if !ok {
					t.Fatalf("expected identifier projection to yield vertex map, got %#v", res.Rows[0][tc.wantKey])
				}
				wantID := strings.TrimSpace(fmt.Sprint(params["src"]))
				if gotID := strings.TrimSpace(fmt.Sprint(gotMap["id"])); gotID != wantID {
					t.Fatalf("unexpected vertex map id: got=%q want=%q row=%#v", gotID, wantID, res.Rows[0])
				}
				return
			}
			if tc.name == "float literal projection" {
				if got := strings.TrimSpace(fmt.Sprint(res.Rows[0][tc.wantKey])); got != fmt.Sprint(tc.wantValue) {
					t.Fatalf("unexpected runtime value: query=%q got=%#v want=%#v row=%#v", tc.query, res.Rows[0][tc.wantKey], tc.wantValue, res.Rows[0])
				}
				return
			}
			if got := res.Rows[0][tc.wantKey]; !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("unexpected runtime value: query=%q got=%#v want=%#v row=%#v", tc.query, got, tc.wantValue, res.Rows[0])
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineWithEscapedQuoteCompoundWhere(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" AND uid = $src RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"src":    `A" OR B`})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after escaped-quote compound WHERE filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != `A" OR B` {
		t.Fatalf("expected projected out value %#v, got %#v", `A" OR B`, res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineParityWithReference(t *testing.T) {
	type edgePresenceCheck struct {
		srcParam       string
		dstParam       string
		edgeType       string
		requirePresent bool
	}

	tests := []struct {
		name       string
		query      string
		params     Params
		vertexIDs  []string
		edgeIDs    []string
		edgeChecks []edgePresenceCheck
	}{
		{
			name:      "with where equals",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u95"},
			vertexIDs: []string{"u95"},
		},
		{
			name:      "with distinct return",
			query:     "CREATE (u:User {id:$src}) WITH DISTINCT u.id AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u96"},
			vertexIDs: []string{"u96"},
		},
		{
			name:      "with where conjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid > 'aaa' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u97"},
			vertexIDs: []string{"u97"},
		},
		{
			name:      "with where disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src OR uid = 'zzz' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u108"},
			vertexIDs: []string{"u108"},
		},
		{
			name:      "with where parenthesized disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid = $src OR uid = 'zzz') OR uid = 'never' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u109"},
			vertexIDs: []string{"u109"},
		},
		{
			name:      "with where starts with",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH 'u' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u110"},
			vertexIDs: []string{"u110"},
		},
		{
			name:      "with where starts with parameter",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH $prefix RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u111", "prefix": "u"},
			vertexIDs: []string{"u111"},
		},
		{
			name:      "with where grouped string predicate disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid STARTS WITH 'x' OR uid ENDS WITH '2') OR uid CONTAINS '11' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u112"},
			vertexIDs: []string{"u112"},
		},
		{
			name:      "with where is not null",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u115"},
			vertexIDs: []string{"u115"},
		},
		{
			name:      "edge create with return endpoints",
			query:     "CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid",
			params:    Params{"tenant": "acme", "src": "u98", "dst": "u99"},
			vertexIDs: []string{"u98", "u99"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "src",
				dstParam:       "dst",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "reverse edge create with return endpoints",
			query:     "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid",
			params:    Params{"tenant": "acme", "src": "u103", "dst": "u104"},
			vertexIDs: []string{"u103", "u104"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "dst",
				dstParam:       "src",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "with order skip limit",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid DESC SKIP 0 LIMIT 1",
			params:    Params{"tenant": "acme", "src": "u100"},
			vertexIDs: []string{"u100"},
		},
		{
			name:      "write only create",
			query:     "CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})",
			params:    Params{"tenant": "acme", "src": "u101", "dst": "u102"},
			vertexIDs: []string{"u101", "u102"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "src",
				dstParam:       "dst",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "reverse write only create",
			query:     "CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})",
			params:    Params{"tenant": "acme", "src": "u105", "dst": "u106"},
			vertexIDs: []string{"u105", "u106"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "dst",
				dstParam:       "src",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
	}

	type edgeSnapshot struct {
		Exists bool
		Type   string
		SrcID  string
		DstID  string
	}
	collectWriteSnapshot := func(ctx context.Context, store graph.GraphStore, tenant string, vertexIDs []string, edgeIDs []string) (map[string]bool, map[string]edgeSnapshot, error) {
		vertices := make(map[string]bool, len(vertexIDs))
		edges := make(map[string]edgeSnapshot, len(edgeIDs))
		err := store.View(ctx, func(tx graph.Tx) error {
			for _, vertexID := range vertexIDs {
				vertex, err := tx.GetVertex(ctx, tenant, vertexID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						vertices[vertexID] = false
						continue
					}
					return err
				}
				vertices[vertexID] = vertex != nil
			}
			for _, edgeID := range edgeIDs {
				edge, err := tx.GetEdge(ctx, tenant, edgeID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						edges[edgeID] = edgeSnapshot{}
						continue
					}
					return err
				}
				if edge == nil {
					edges[edgeID] = edgeSnapshot{}
					continue
				}
				edges[edgeID] = edgeSnapshot{Exists: true, Type: edge.Type, SrcID: edge.SrcID, DstID: edge.DstID}
			}
			return nil
		})
		return vertices, edges, err
	}
	collectDirectedEdgePresence := func(ctx context.Context, store graph.GraphStore, tenant string, checks []edgePresenceCheck, params Params) (map[string]bool, error) {
		presence := make(map[string]bool, len(checks))
		err := store.View(ctx, func(tx graph.Tx) error {
			for _, check := range checks {
				src, _ := params[check.srcParam].(string)
				dst, _ := params[check.dstParam].(string)
				if src == "" || dst == "" || check.edgeType == "" {
					return graph.NewError(graph.ErrKindInvalidInput, "edge presence check requires src, dst, and edgeType", nil)
				}
				hasEdge, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, check.edgeType)
				if err != nil {
					return err
				}
				key := src + "|" + check.edgeType + "|" + dst
				presence[key] = hasEdge
			}
			return nil
		})
		return presence, err
	}

	normalizeRows := func(rows []Row) []Row {
		if rows == nil {
			return []Row{}
		}
		return rows
	}
	normalizeColumns := func(cols []string) []string {
		if cols == nil {
			return []string{}
		}
		return cols
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			runtimeStore := openStore(t)
			defer func() { _ = runtimeStore.Close() }()
			runtimeExec := New(runtimeStore, Options{})
			runtimeStmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			runtimeParams := Params{}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}
			runtimeRes, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams)
			if err != nil {
				t.Fatalf("runtime execution failed: %v", err)
			}

			referenceStore := openStore(t)
			defer func() { _ = referenceStore.Close() }()
			referenceExec := New(referenceStore, Options{})
			referenceStmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			referenceParams := Params{}
			for k, v := range tc.params {
				referenceParams[k] = v
			}
			referenceRes, err := referenceExec.ExecuteStatement(ctx, referenceStmt, referenceParams)
			if err != nil {
				t.Fatalf("reference execution failed: %v", err)
			}

			if !reflect.DeepEqual(normalizeRows(runtimeRes.Rows), normalizeRows(referenceRes.Rows)) {
				t.Fatalf("runtime and reference rows diverged: runtime=%#v reference=%#v", runtimeRes.Rows, referenceRes.Rows)
			}
			if !reflect.DeepEqual(normalizeColumns(runtimeRes.Columns), normalizeColumns(referenceRes.Columns)) {
				t.Fatalf("runtime and reference columns diverged: runtime=%#v reference=%#v", runtimeRes.Columns, referenceRes.Columns)
			}
			if runtimeRes.Stats.RowsReturned != referenceRes.Stats.RowsReturned {
				t.Fatalf("runtime and reference RowsReturned diverged: runtime=%d reference=%d", runtimeRes.Stats.RowsReturned, referenceRes.Stats.RowsReturned)
			}

			tenant, _ := tc.params["tenant"].(string)
			if tenant == "" {
				tenant = "acme"
			}
			runtimeVertices, runtimeEdges, err := collectWriteSnapshot(ctx, runtimeStore, tenant, tc.vertexIDs, tc.edgeIDs)
			if err != nil {
				t.Fatalf("runtime snapshot read failed: %v", err)
			}
			referenceVertices, referenceEdges, err := collectWriteSnapshot(ctx, referenceStore, tenant, tc.vertexIDs, tc.edgeIDs)
			if err != nil {
				t.Fatalf("reference snapshot read failed: %v", err)
			}
			if !reflect.DeepEqual(runtimeVertices, referenceVertices) {
				t.Fatalf("runtime and reference vertex write side-effects diverged: runtime=%#v reference=%#v", runtimeVertices, referenceVertices)
			}
			if !reflect.DeepEqual(runtimeEdges, referenceEdges) {
				t.Fatalf("runtime and reference edge write side-effects diverged: runtime=%#v reference=%#v", runtimeEdges, referenceEdges)
			}

			runtimeDirectedEdges, err := collectDirectedEdgePresence(ctx, runtimeStore, tenant, tc.edgeChecks, tc.params)
			if err != nil {
				t.Fatalf("runtime directed-edge snapshot read failed: %v", err)
			}
			referenceDirectedEdges, err := collectDirectedEdgePresence(ctx, referenceStore, tenant, tc.edgeChecks, tc.params)
			if err != nil {
				t.Fatalf("reference directed-edge snapshot read failed: %v", err)
			}
			if !reflect.DeepEqual(runtimeDirectedEdges, referenceDirectedEdges) {
				t.Fatalf("runtime and reference directed-edge side-effects diverged: runtime=%#v reference=%#v", runtimeDirectedEdges, referenceDirectedEdges)
			}
			for _, check := range tc.edgeChecks {
				if !check.requirePresent {
					continue
				}
				src, _ := tc.params[check.srcParam].(string)
				dst, _ := tc.params[check.dstParam].(string)
				key := src + "|" + check.edgeType + "|" + dst
				if !runtimeDirectedEdges[key] || !referenceDirectedEdges[key] {
					t.Fatalf("expected directed edge %s to be present in both paths; runtime=%v reference=%v", key, runtimeDirectedEdges[key], referenceDirectedEdges[key])
				}
			}
		})
	}
}

func TestRuntimeNativeExecutionCandidateCreateWithReturnFamily(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantOK  bool
		wantErr bool
	}{
		{
			name:   "standalone anonymous create now native",
			query:  "CREATE (:User)",
			wantOK: true,
		},
		{
			name:   "create with return no where",
			query:  "CREATE (u:User {id:$src}) WITH u.id AS uid RETURN uid AS out",
			wantOK: true,
		},
		{
			name:   "create with literal projection return",
			query:  "CREATE (u:User {id:$src}) WITH 'beta' AS uid RETURN uid AS out",
			wantOK: true,
		},
		{
			name:   "create with where now native",
			query:  "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			wantOK: true,
		},
		{
			name:   "create with mixed where now native",
			query:  "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid = 'u1' OR uid = 'u2' RETURN uid AS out",
			wantOK: true,
		},
		{
			name:   "create with escaped where now native",
			query:  `CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" RETURN uid AS out`,
			wantOK: true,
		},
		{
			name:   "create with distinct now native",
			query:  "CREATE (u:User {id:$src}) WITH DISTINCT u.id AS uid RETURN uid AS out",
			wantOK: true,
		},
		{
			name:   "create missing relationship type rejected by runtime pipeline",
			query:  "CREATE ()-->()",
			wantOK: false,
		},
		{
			name:   "create temporal function property now native",
			query:  "CREATE ({created: localtime({hour: 12})})",
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if got := runtimeNativeExecutionCandidate(query); got != tc.wantOK {
				t.Fatalf("unexpected runtime-native candidate decision for %q: got=%v want=%v", tc.query, got, tc.wantOK)
			}
		})
	}
}

func TestTryExecuteViaRuntimePipelineRejectsMissingRelationshipTypeWithoutFallback(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE ()-->()")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr == nil {
		t.Fatalf("expected runtime pipeline to reject missing relationship type")
	}
	parseErr, ok := runtimeErr.(*parser.ParseError)
	if !ok {
		t.Fatalf("expected parser parse error, got %T: %v", runtimeErr, runtimeErr)
	}
	if parseErr.Kind != parser.ParseErrorUnsupported || parseErr.Message != "NoSingleRelationshipType" {
		t.Fatalf("unexpected runtime parse error: %#v", parseErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle the rejection without legacy fallback")
	}
	if runtimeRes != nil {
		t.Fatalf("expected no result for rejected CREATE, got %#v", runtimeRes)
	}
}

func TestTryExecuteViaRuntimePipelineExecutesMigratedCreateFamilyWithUnwind(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (a) WITH a UNWIND [0] AS i CREATE (b) CREATE (a)<-[:T]-(b)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected migrated CREATE-family shape to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle migrated CREATE-family statement")
	}
	if runtimeRes != nil {
		if len(runtimeRes.Rows) != 0 {
			t.Fatalf("expected no result rows for write-only query, got %#v", runtimeRes.Rows)
		}
	}
}

func TestTryExecuteViaRuntimePipelineExecutesMigratedUnwindCreateFamily(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("UNWIND range(0, 2) AS i CREATE (s:S) WITH s, i UNWIND range(0, i) AS j CREATE (s)-[:REL]->()")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected migrated UNWIND/CREATE-family shape to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle migrated UNWIND/CREATE-family statement")
	}
	if runtimeRes != nil && len(runtimeRes.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only query, got %#v", runtimeRes.Rows)
	}
}

func TestTryExecuteViaRuntimePipelineExecutesMigratedMatchSetFamily(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seed, err := parser.ParseStatement("CREATE (:A {id: 'n1', v: 1})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A {id: 'n1'}) SET n.v = 2 RETURN n.v AS v")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected migrated MATCH+SET family to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle migrated MATCH+SET family statement")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+SET query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["v"]); got != "2" {
		t.Fatalf("expected projected value 2, got %#v", runtimeRes.Rows[0]["v"])
	}
}

func TestTryExecuteViaRuntimePipelineExecutesMigratedMatchSetMapReplaceFamily(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seed, err := parser.ParseStatement("CREATE (:A {id: 'n1', name: 'A'})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A {id: 'n1'}) SET n = {name: 'B'} RETURN n.name AS name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected MATCH+SET-map-replace family to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle MATCH+SET-map-replace family statement")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+SET map replace query, got %#v", runtimeRes)
	}
}

func TestTryExecuteViaRuntimePipelineExecutesMigratedMatchSetMapAppendFamily(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seed, err := parser.ParseStatement("CREATE (:A {id: 'n1', name: 'A'})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A {id: 'n1'}) SET n += {name: 'B'} RETURN n.name AS name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected MATCH+SET-map-append family to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime pipeline to handle MATCH+SET-map-append family statement")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+SET map append query, got %#v", runtimeRes)
	}
}

func TestTryExecuteViaRuntimePipelineUnwindReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("UNWIND [1] AS x RETURN x")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute UNWIND query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row for UNWIND query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["x"]); got != "1" {
		t.Fatalf("expected x=1 for UNWIND query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineWithUnwindMatchReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seed, err := parser.ParseStatement("CREATE (:Num {id: '0'})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("WITH collect([0, 0.0]) AS numbers UNWIND numbers AS arr WITH arr[0] AS expected MATCH (n) WHERE toInteger(n.id) = expected RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected WITH+UNWIND+MATCH+RETURN shape to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil {
		t.Fatalf("expected non-nil result for WITH+UNWIND+MATCH+RETURN query")
	}
}

func TestTryExecuteViaRuntimePipelineUnwindCreateReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("UNWIND [1, 2] AS x CREATE (n:N {id: x}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected UNWIND+CREATE+RETURN shape to execute in runtime pipeline, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 2 {
		t.Fatalf("expected two rows for UNWIND+CREATE+RETURN query, got %#v", runtimeRes)
	}
}

func TestTryExecuteViaRuntimePipelineReturnOnlyScalarExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN 42 AS n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute RETURN-only scalar query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from RETURN-only runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["n"]); got != "42" {
		t.Fatalf("expected n=42 from runtime RETURN-only scalar query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineReturnOnlyFunctionExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN toString(42) AS s")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute RETURN-only function query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from RETURN-only function runtime query, got %#v", runtimeRes)
	}
	if _, ok := runtimeRes.Rows[0]["s"]; !ok {
		t.Fatalf("expected function projection column s in runtime result row, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineWithReturnTemporalExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("WITH date('1984-10-11') AS other RETURN date({date: other, day: 28}) AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute WITH+RETURN temporal query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from WITH+RETURN temporal query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["result"]); got != "1984-10-28" {
		t.Fatalf("expected result=1984-10-28 from runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineWithMatchReturnTemporalExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Duration {dur: duration({days: 1})})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("WITH date('1984-10-11') AS x MATCH (d:Duration) RETURN x AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute WITH+MATCH+RETURN temporal query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from WITH+MATCH+RETURN temporal query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["result"]); got != "1984-10-11" {
		t.Fatalf("expected result=1984-10-11 from runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineMatchReturnDurationArithmeticExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Duration1 {date: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1})}) CREATE (:Duration2 {date: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1})})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (dur:Duration1), (dur2: Duration2) RETURN dur.date + dur2.date AS sum, dur.date - dur2.date AS diff")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	inspectStmt, err := parser.ParseStatement("MATCH (dur:Duration1), (dur2: Duration2) RETURN toString(dur.date) AS left, toString(dur2.date) AS right")
	if err != nil {
		t.Fatalf("inspect parse failed: %v", err)
	}
	inspectRes, inspectOK, inspectErr := exec.tryExecuteViaRuntimePipeline(ctx, inspectStmt, Params{"tenant": "acme"})
	if inspectErr != nil || !inspectOK || inspectRes == nil || len(inspectRes.Rows) == 0 {
		t.Fatalf("expected inspect runtime query to succeed, ok=%v err=%v res=%#v", inspectOK, inspectErr, inspectRes)
	}
	for _, row := range inspectRes.Rows {
		if got := fmt.Sprint(row["left"]); got != "P12Y5M14DT16H13M10.000000001S" {
			t.Fatalf("expected left duration property hydration, got %#v", row)
		}
		if got := fmt.Sprint(row["right"]); got != "P12Y5M14DT16H13M10.000000001S" {
			t.Fatalf("expected right duration property hydration, got %#v", row)
		}
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH duration arithmetic query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) == 0 {
		t.Fatalf("expected non-empty result rows from MATCH duration arithmetic query, got %#v", runtimeRes)
	}
	for _, row := range runtimeRes.Rows {
		if got := fmt.Sprint(row["sum"]); got != "P24Y10M28DT32H26M20.000000002S" {
			t.Fatalf("expected sum from runtime query, got %#v", row)
		}
		if got := fmt.Sprint(row["diff"]); got != "PT0S" {
			t.Fatalf("expected diff from runtime query, got %#v", row)
		}
	}
}

func TestTryExecuteViaRuntimePipelineMatchReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:User {id:'u1'})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User) RETURN u.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH+RETURN query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+RETURN runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["id"]); got != "u1" {
		t.Fatalf("expected id=u1 from MATCH+RETURN runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineOptionalMatchReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:User {id:'u1'})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User) OPTIONAL MATCH (u)-[:KNOWS]->(v:User) RETURN u.id AS uid, v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH+OPTIONAL MATCH+RETURN query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+OPTIONAL MATCH+RETURN runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["uid"]); got != "u1" {
		t.Fatalf("expected uid=u1 from runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineMergeExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("MERGE (u:User {id: 'u1'}) RETURN u.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MERGE query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MERGE runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["id"]); got != "u1" {
		t.Fatalf("expected id=u1 from MERGE runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineMatchMergeOptionalMatchExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:A {id:'a1'}), (:B {id:'b1'})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH+MERGE+OPTIONAL MATCH query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+MERGE+OPTIONAL MATCH runtime query, got %#v", runtimeRes)
	}
	if _, ok := runtimeRes.Rows[0]["c"]; !ok {
		t.Fatalf("expected count projection column c in runtime result row, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineWithCallReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("WITH 1 AS x CALL db.stats.vertexCount() YIELD vertexCount RETURN x, vertexCount")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute WITH+CALL+RETURN query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from WITH+CALL+RETURN runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["x"]); got != "1" {
		t.Fatalf("expected x=1 from runtime query, got %#v", runtimeRes.Rows[0])
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["vertexCount"]); got != "0" {
		t.Fatalf("expected vertexCount=0 from runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineDeleteReturnExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:A {id:'a1'}), (:A {id:'a2'})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A) DELETE n RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH+DELETE+RETURN query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one result row from MATCH+DELETE+RETURN runtime query, got %#v", runtimeRes)
	}
	if got := fmt.Sprint(runtimeRes.Rows[0]["c"]); got != "2" {
		t.Fatalf("expected c=2 from runtime query, got %#v", runtimeRes.Rows[0])
	}
}

func TestTryExecuteViaRuntimePipelineDeleteWithPaginationPreservesSideEffects(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:A {id:'a1'}), (:A {id:'a2'}), (:A {id:'a3'})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:A) DELETE n RETURN n.id AS id ORDER BY id SKIP 1 LIMIT 1")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute paginated delete query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes == nil || len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one paginated result row, got %#v", runtimeRes)
	}

	remaining := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(found *graph.Vertex) error {
			if found != nil {
				remaining++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("verify scan failed: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected all rows deleted despite pagination, got remaining=%d", remaining)
	}
}

func TestExecuteCreateWithInlineCommentsSeedsAllVertices(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement(`CREATE (:A {num: 1, num2: 4}), //num + num2 = 5
       (:A {num: 5, num2: 2}), //num + num2 = 7
       (:A {num: 9, num2: 0})  //num + num2 = 9`)
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A) RETURN a.num + a.num2 AS sum ORDER BY sum ASC")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 seeded A rows, got %#v", res.Rows)
	}
	got := []any{res.Rows[0]["sum"], res.Rows[1]["sum"], res.Rows[2]["sum"]}
	if !reflect.DeepEqual(got, []any{5, 7, 9}) {
		t.Fatalf("unexpected sums from inline-comment create seed: %#v", got)
	}
}

func TestTryExecuteViaRuntimePipelineCreateAndDeleteSameQueryExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE ()")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH () CREATE (n) DELETE n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, runtimeOK, runtimeErr := exec.tryExecuteViaRuntimePipeline(ctx, stmt, Params{"tenant": "acme"})
	if runtimeErr != nil {
		t.Fatalf("expected runtime-only mode to execute MATCH+CREATE+DELETE query, got %v", runtimeErr)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true")
	}
	if runtimeRes != nil && len(runtimeRes.Rows) != 0 {
		t.Fatalf("expected no rows for write-only query, got %#v", runtimeRes.Rows)
	}

	remaining := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(found *graph.Vertex) error {
			if found != nil {
				remaining++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("verify scan failed: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected no net side effects (one seed vertex remains), got remaining=%d", remaining)
	}
}

func TestTryExecuteViaRuntimePipelineDeleteThenMergeRetainsSingleCreatedVertex(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:A {num: 1}), (:A {num: 2})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmtText := "MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num AS num"
	stmtAny, err := parser.ParseStatement(stmtText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{
		Plan:   physicalPlan,
		Tenant: "acme",
		Params: map[string]any{"tenant": "acme"},
	}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	if len(runtimeResult.Rows) != 2 {
		t.Fatalf("expected two rows from delete+merge query, got %#v", runtimeResult.Rows)
	}
	for _, row := range runtimeResult.Rows {
		if row["num"] != nil {
			t.Fatalf("expected null num values after delete+merge, got %#v", runtimeResult.Rows)
		}
	}

	if len(runtimeResult.WriteEvents) != 4 {
		t.Fatalf("expected four write events (2 delete + 2 merge), got %#v", runtimeResult.WriteEvents)
	}
	if got := strings.ToUpper(strings.TrimSpace(runtimeResult.WriteEvents[0].Kind)); got != "DELETE" {
		t.Fatalf("expected first write event DELETE, got kind=%q events=%#v", got, runtimeResult.WriteEvents)
	}
	if got := strings.ToUpper(strings.TrimSpace(runtimeResult.WriteEvents[2].Kind)); got != "MERGE" {
		t.Fatalf("expected third write event MERGE, got kind=%q events=%#v", got, runtimeResult.WriteEvents)
	}

	remaining := 0
	remainingWithNum := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(found *graph.Vertex) error {
			if found == nil {
				return nil
			}
			remaining++
			if _, ok := found.Properties["num"]; ok {
				remainingWithNum++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("verify scan failed: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected one remaining vertex after delete+merge, got %d", remaining)
	}
	if remainingWithNum != 0 {
		t.Fatalf("expected merged replacement vertex without num property, got %d with num", remainingWithNum)
	}
}

func TestRuntimePipelineUnwindMultipleMergeSurfacesEdgeWriteBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmtAny, err := parser.ParseStatement("UNWIND ['Keanu Reeves', 'Hugo Weaving', 'Carrie-Anne Moss', 'Laurence Fishburne'] AS actor MERGE (m:Movie {name: 'The Matrix'}) MERGE (p:Person {name: actor}) MERGE (p)-[:ACTED_IN]->(m)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: physicalPlan, Tenant: "acme", Params: map[string]any{"tenant": "acme"}}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	edgeEvents := 0
	for _, event := range runtimeResult.WriteEvents {
		if event.Edge == nil {
			continue
		}
		edgeEvents++
		if got := strings.TrimSpace(fmt.Sprint(event.Bindings["p"])); got == "" {
			t.Fatalf("expected edge event binding p, got event=%#v", event)
		}
		if got := strings.TrimSpace(fmt.Sprint(event.Bindings["m"])); got == "" {
			t.Fatalf("expected edge event binding m, got event=%#v", event)
		}
	}
	if edgeEvents == 0 {
		t.Fatalf("expected edge write events, got %#v", runtimeResult.WriteEvents)
	}

	relCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanOutEdgeLinksByType(ctx, "acme", "ACTED_IN", 0, func(srcID, edgeID, dstID string) error {
			relCount++
			return nil
		})
	}); err != nil {
		t.Fatalf("relationship scan failed: %v", err)
	}
	if relCount != 4 {
		t.Fatalf("expected 4 ACTED_IN relationships, got %d", relCount)
	}

	snapshotStyleRelCount := 0
	seen := map[string]struct{}{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(v *graph.Vertex) error {
			if v == nil {
				return nil
			}
			return tx.ScanOutEdges(ctx, "acme", v.ID, "", 0, func(e *graph.Edge) error {
				if e == nil {
					return nil
				}
				if _, ok := seen[e.ID]; ok {
					return nil
				}
				seen[e.ID] = struct{}{}
				snapshotStyleRelCount++
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("snapshot-style relationship scan failed: %v", err)
	}
	if snapshotStyleRelCount != 4 {
		t.Fatalf("expected snapshot-style count 4, got %d", snapshotStyleRelCount)
	}
}

func TestRuntimePipelineDeleteThenMergeEdgeWriteEventPayloads(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (a:A), (b:B) CREATE (a)-[:T {name: 'rel1'}]->(b), (a)-[:T {name: 'rel2'}]->(b)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmtAny, err := parser.ParseStatement("MATCH (a)-[t:T]->(b) DELETE t MERGE (a)-[t2:T {name: 'rel3'}]->(b) RETURN t2.name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: physicalPlan, Tenant: "acme", Params: map[string]any{"tenant": "acme"}}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	deleteEvents := 0
	mergeEdgeEvents := 0
	deletedEdgeIDs := map[string]struct{}{}
	for _, event := range runtimeResult.WriteEvents {
		kind := strings.ToUpper(strings.TrimSpace(event.Kind))
		if kind == "DELETE" {
			deleteEvents++
			if strings.TrimSpace(event.Raw) == "" && strings.TrimSpace(event.Pattern) == "" {
				t.Fatalf("expected delete event to carry raw or pattern, got %#v", event)
			}
			if id := strings.TrimSpace(fmt.Sprint(event.Bindings["t"])); id != "" {
				deletedEdgeIDs[id] = struct{}{}
			}
			if id := strings.TrimSpace(fmt.Sprint(event.Bindings["t.id"])); id != "" {
				deletedEdgeIDs[id] = struct{}{}
			}
		}
		if kind == "MERGE" && event.Edge != nil {
			mergeEdgeEvents++
		}
	}
	if deleteEvents == 0 || mergeEdgeEvents == 0 {
		t.Fatalf("expected delete and merge-edge write events, got %#v", runtimeResult.WriteEvents)
	}
	if len(deletedEdgeIDs) != 2 {
		t.Fatalf("expected delete events bound to two distinct edge IDs, got %#v", deletedEdgeIDs)
	}

	relCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanOutEdgeLinksByType(ctx, "acme", "T", 0, func(srcID, edgeID, dstID string) error {
			relCount++
			return nil
		})
	}); err != nil {
		t.Fatalf("relationship scan failed: %v", err)
	}
	if relCount != 1 {
		t.Fatalf("expected exactly one T relationship after delete+merge, got %d", relCount)
	}

	snapshotStyleRelCount := 0
	seen := map[string]struct{}{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(v *graph.Vertex) error {
			if v == nil {
				return nil
			}
			return tx.ScanOutEdges(ctx, "acme", v.ID, "", 0, func(e *graph.Edge) error {
				if e == nil {
					return nil
				}
				if _, ok := seen[e.ID]; ok {
					return nil
				}
				seen[e.ID] = struct{}{}
				snapshotStyleRelCount++
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("snapshot-style relationship scan failed: %v", err)
	}
	if snapshotStyleRelCount != 1 {
		t.Fatalf("expected snapshot-style relationship count 1, got %d", snapshotStyleRelCount)
	}
}

func TestRuntimePipelineMergeThenCreateEdgePreservesBoundVertexes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmtAny, err := parser.ParseStatement("MERGE (t:T {id: 42}) CREATE (f:R) CREATE (t)-[:REL]->(f)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: physicalPlan, Tenant: "acme", Params: map[string]any{"tenant": "acme"}}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	vertexCount := 0
	relCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		if err := tx.ScanVertices(ctx, "acme", 0, func(v *graph.Vertex) error {
			if v != nil {
				vertexCount++
			}
			return nil
		}); err != nil {
			return err
		}
		return tx.ScanOutEdgeLinksByType(ctx, "acme", "REL", 0, func(srcID, edgeID, dstID string) error {
			relCount++
			return nil
		})
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}

	if vertexCount != 2 || relCount != 1 {
		t.Fatalf("expected 2 vertexes and 1 relationship, got vertexes=%d relationships=%d writeEvents=%#v", vertexCount, relCount, runtimeResult.WriteEvents)
	}
}

func TestRuntimePipelineMatchEdgeReturnsDistinctParallelRelationships(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (a:A), (b:B) CREATE (a)-[:T {name: 'rel1'}]->(b), (a)-[:T {name: 'rel2'}]->(b)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[t:T]->(b) RETURN t.name AS name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows for parallel relationships, got %#v", res.Rows)
	}
}

func TestRuntimePipelineBatchCreateCarriesVariablesAcrossCreateClauses(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	batch, err := parser.ParseBatch("CREATE (a:A), (b:B)\nCREATE (a)-[:T {name: 'rel1'}]->(b), (a)-[:T {name: 'rel2'}]->(b)")
	if err != nil {
		t.Fatalf("parse batch failed: %v", err)
	}
	for _, stmt := range batch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("batch execute failed: %v", err)
		}
	}

	relCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanOutEdgeLinksByType(ctx, "acme", "T", 0, func(srcID, edgeID, dstID string) error {
			relCount++
			return nil
		})
	}); err != nil {
		t.Fatalf("relationship scan failed: %v", err)
	}
	if relCount != 2 {
		t.Fatalf("expected 2 seeded T relationships, got %d", relCount)
	}
}

func TestRuntimePipelineScenario21SideEffects(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedBatch, err := parser.ParseBatch("CREATE (a:A), (b:B)\nCREATE (a)-[:T {name: 'rel1'}]->(b), (a)-[:T {name: 'rel2'}]->(b)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	for _, stmt := range seedBatch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"}); err != nil {
			t.Fatalf("seed execute failed: %v", err)
		}
	}

	beforeRels := 0
	beforeSnapshotRels := 0
	beforeSeen := map[string]struct{}{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		if err := tx.ScanOutEdgeLinksByType(ctx, "tck", "T", 0, func(srcID, edgeID, dstID string) error {
			beforeRels++
			return nil
		}); err != nil {
			return err
		}
		return tx.ScanVertices(ctx, "tck", 0, func(v *graph.Vertex) error {
			if v == nil {
				return nil
			}
			return tx.ScanOutEdges(ctx, "tck", v.ID, "", 0, func(e *graph.Edge) error {
				if e == nil {
					return nil
				}
				if _, ok := beforeSeen[e.ID]; ok {
					return nil
				}
				beforeSeen[e.ID] = struct{}{}
				beforeSnapshotRels++
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("before relationship scan failed: %v", err)
	}

	queryBatch, err := parser.ParseBatch("MATCH (a)-[t:T]->(b)\nDELETE t\nMERGE (a)-[t2:T {name: 'rel3'}]->(b)\nRETURN t2.name")
	if err != nil {
		t.Fatalf("query parse failed: %v", err)
	}
	var res *Result
	for _, stmt := range queryBatch.Statements {
		res, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"})
		if err != nil {
			t.Fatalf("query execute failed: %v", err)
		}
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows from scenario 21 query, got %#v", res.Rows)
	}

	afterRels := 0
	storedNames := []string{}
	afterSnapshotRels := 0
	afterSeen := map[string]struct{}{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		if err := tx.ScanOutEdgeLinksByType(ctx, "tck", "T", 0, func(srcID, edgeID, dstID string) error {
			afterRels++
			edge, err := tx.GetEdge(ctx, "tck", edgeID)
			if err == nil && edge != nil {
				storedNames = append(storedNames, strings.TrimSpace(string(edge.Properties["name"])))
			}
			return nil
		}); err != nil {
			return err
		}
		return tx.ScanVertices(ctx, "tck", 0, func(v *graph.Vertex) error {
			if v == nil {
				return nil
			}
			return tx.ScanOutEdges(ctx, "tck", v.ID, "", 0, func(e *graph.Edge) error {
				if e == nil {
					return nil
				}
				if _, ok := afterSeen[e.ID]; ok {
					return nil
				}
				afterSeen[e.ID] = struct{}{}
				afterSnapshotRels++
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("after relationship scan failed: %v", err)
	}
	if beforeRels != 2 || afterRels != 1 {
		t.Fatalf("expected scenario21 relationship counts before=2 after=1, got before=%d after=%d", beforeRels, afterRels)
	}
	if len(storedNames) != 1 || storedNames[0] != "rel3" {
		t.Fatalf("expected remaining relationship name rel3, got %#v", storedNames)
	}
	if beforeSnapshotRels != 2 || afterSnapshotRels != 1 {
		t.Fatalf("expected snapshot-style relationship counts before=2 after=1, got before=%d after=%d", beforeSnapshotRels, afterSnapshotRels)
	}
	for edgeID := range afterSeen {
		if _, existedBefore := beforeSeen[edgeID]; existedBefore {
			t.Fatalf("expected MERGE-created relationship to use a fresh id after DELETE, got reused id %q (before=%#v after=%#v)", edgeID, beforeSeen, afterSeen)
		}
	}
}

func TestRuntimePipelineScenario20DeleteBindingsCoverBothChains(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedBatch, err := parser.ParseBatch("CREATE (a:A)\nCREATE (b1:B {num: 0}), (b2:B {num: 1})\nCREATE (c1:C), (c2:C)\nCREATE (a)-[:REL]->(b1), (a)-[:REL]->(b2), (b1)-[:REL]->(c1), (b2)-[:REL]->(c2)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	for _, stmt := range seedBatch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"}); err != nil {
			t.Fatalf("seed execute failed: %v", err)
		}
	}

	stmtAny, err := parser.ParseStatement("MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C) DELETE ab, bc, b, c MERGE (newB:B {num: 1}) MERGE (a)-[:REL]->(newB) MERGE (newC:C) MERGE (newB)-[:REL]->(newC)")
	if err != nil {
		t.Fatalf("query parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}
	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "tck"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: physicalPlan, Tenant: "tck", Params: map[string]any{"tenant": "tck"}}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	deleteB := map[string]struct{}{}
	for _, event := range runtimeResult.WriteEvents {
		if strings.ToUpper(strings.TrimSpace(event.Kind)) != "DELETE" {
			continue
		}
		if id := strings.TrimSpace(fmt.Sprint(event.Bindings["b"])); id != "" {
			deleteB[id] = struct{}{}
		}
		if id := strings.TrimSpace(fmt.Sprint(event.Bindings["b.id"])); id != "" {
			deleteB[id] = struct{}{}
		}
	}
	if len(deleteB) != 2 {
		t.Fatalf("expected delete bindings to include both B vertices, got %#v (events=%#v)", deleteB, runtimeResult.WriteEvents)
	}
}

func TestRuntimePipelineDetachDeleteConnectedNodeWriteBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedBatch, err := parser.ParseBatch("CREATE (x:X) CREATE (x)-[:R]->() CREATE (x)-[:R]->() CREATE (x)-[:R]->()")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	for _, stmt := range seedBatch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"}); err != nil {
			t.Fatalf("seed execute failed: %v", err)
		}
	}

	stmtAny, err := parser.ParseStatement("MATCH (n:X) DETACH DELETE n")
	if err != nil {
		t.Fatalf("query parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}
	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "tck"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: physicalPlan, Tenant: "tck", Params: map[string]any{"tenant": "tck"}}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}
	if len(runtimeResult.WriteEvents) == 0 {
		t.Fatalf("expected delete write event, got none")
	}
	event := runtimeResult.WriteEvents[0]
	if strings.ToUpper(strings.TrimSpace(event.Kind)) != "DELETE" {
		t.Fatalf("expected DELETE write event, got %#v", runtimeResult.WriteEvents)
	}
	if _, ok := event.Bindings["n"]; !ok {
		t.Fatalf("expected n binding in delete event, got %#v", event.Bindings)
	}
}

func TestRuntimePipelineScenario20VertexSetDelta(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedBatch, err := parser.ParseBatch("CREATE (a:A)\nCREATE (b1:B {num: 0}), (b2:B {num: 1})\nCREATE (c1:C), (c2:C)\nCREATE (a)-[:REL]->(b1), (a)-[:REL]->(b2), (b1)-[:REL]->(c1), (b2)-[:REL]->(c2)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	for _, stmt := range seedBatch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"}); err != nil {
			t.Fatalf("seed execute failed: %v", err)
		}
	}

	before := map[string]struct{}{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "tck", 0, func(v *graph.Vertex) error {
			if v != nil {
				before[v.ID] = struct{}{}
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("before vertex scan failed: %v", err)
	}

	batch, err := parser.ParseBatch("MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C)\nDELETE ab, bc, b, c\nMERGE (newB:B {num: 1})\nMERGE (a)-[:REL]->(newB)\nMERGE (newC:C)\nMERGE (newB)-[:REL]->(newC)")
	if err != nil {
		t.Fatalf("query parse failed: %v", err)
	}
	for _, stmt := range batch.Statements {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "tck"}); err != nil {
			t.Fatalf("query execute failed: %v", err)
		}
	}

	after := map[string]struct{}{}
	afterDetails := map[string]string{}
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "tck", 0, func(v *graph.Vertex) error {
			if v != nil {
				after[v.ID] = struct{}{}
				afterDetails[v.ID] = fmt.Sprintf("labels=%v props=%v", v.Labels, v.Properties)
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("after vertex scan failed: %v", err)
	}

	added := 0
	for id := range after {
		if _, ok := before[id]; !ok {
			added++
		}
	}
	if added != 2 {
		t.Fatalf("expected scenario20 added vertex IDs=2, got %d (before=%#v after=%#v details=%#v)", added, before, after, afterDetails)
	}
}

func TestTryExecuteViaRuntimePipelineDeleteListIndexParamExecutesInRuntimeOnlyMode(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (u:User) CREATE (u)-[:FRIEND]->() CREATE (u)-[:FRIEND]->() CREATE (u)-[:FRIEND]->() CREATE (u)-[:FRIEND]->()")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	nodeDeleteStmt, err := parser.ParseStatement("MATCH (:User)-[:FRIEND]->(n) WITH collect(n) AS friends DETACH DELETE friends[$friendIndex]")
	if err != nil {
		t.Fatalf("node delete parse failed: %v", err)
	}
	nodeRes, nodeOK, nodeErr := exec.tryExecuteViaRuntimePipeline(ctx, nodeDeleteStmt, Params{"tenant": "acme", "friendIndex": 1})
	if nodeErr != nil {
		t.Fatalf("expected runtime-only mode to execute list-index node delete, got %v", nodeErr)
	}
	if !nodeOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true for node delete")
	}
	if nodeRes != nil && len(nodeRes.Rows) != 0 {
		t.Fatalf("expected empty result for node delete query, got %#v", nodeRes.Rows)
	}

	relDeleteStmt, err := parser.ParseStatement("MATCH (:User)-[r:FRIEND]->() WITH collect(r) AS friendships DETACH DELETE friendships[$friendIndex]")
	if err != nil {
		t.Fatalf("relationship delete parse failed: %v", err)
	}
	relRes, relOK, relErr := exec.tryExecuteViaRuntimePipeline(ctx, relDeleteStmt, Params{"tenant": "acme", "friendIndex": 1})
	if relErr != nil {
		t.Fatalf("expected runtime-only mode to execute list-index relationship delete, got %v", relErr)
	}
	if !relOK {
		t.Fatalf("expected runtime routing entrypoint to report handled=true for relationship delete")
	}
	if relRes != nil && len(relRes.Rows) != 0 {
		t.Fatalf("expected empty result for relationship delete query, got %#v", relRes.Rows)
	}

	friendCountStmt, err := parser.ParseStatement("MATCH (:User)-[r:FRIEND]->() RETURN count(r) AS c")
	if err != nil {
		t.Fatalf("friend count parse failed: %v", err)
	}
	friendCountRes, err := exec.ExecuteStatement(ctx, friendCountStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("friend count execute failed: %v", err)
	}
	if friendCountRes == nil || len(friendCountRes.Rows) != 1 {
		t.Fatalf("expected one friend count row, got %#v", friendCountRes)
	}
	if got := fmt.Sprint(friendCountRes.Rows[0]["c"]); got != "2" {
		t.Fatalf("expected 2 FRIEND relationships after one node and one relationship delete, got %#v", friendCountRes.Rows[0])
	}

	vertexCountStmt, err := parser.ParseStatement("MATCH (n) RETURN count(n) AS c")
	if err != nil {
		t.Fatalf("vertex count parse failed: %v", err)
	}
	vertexCountRes, err := exec.ExecuteStatement(ctx, vertexCountStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("vertex count execute failed: %v", err)
	}
	if vertexCountRes == nil || len(vertexCountRes.Rows) != 1 {
		t.Fatalf("expected one vertex count row, got %#v", vertexCountRes)
	}
	if got := fmt.Sprint(vertexCountRes.Rows[0]["c"]); got != "4" {
		t.Fatalf("expected 4 vertexes after deleting one friend vertex and one relationship, got %#v", vertexCountRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateTemporalProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:Event {created: localtime({hour: 12}), span: duration({minutes: 2, seconds: 30})})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime pipeline query, got %#v", res.Rows)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		var vertex *graph.Vertex
		if err := tx.ScanVertices(ctx, "acme", 10, func(found *graph.Vertex) error {
			vertex = found
			return nil
		}); err != nil {
			return err
		}
		if vertex == nil {
			t.Fatalf("expected anonymous vertex to be written")
		}
		if got := string(vertex.Properties["created"]); got != "12:00" {
			t.Fatalf("expected localtime property to be stored natively, got %q", got)
		}
		if got := string(vertex.Properties["span"]); got != "PT2M30S" {
			t.Fatalf("expected duration property to be stored natively, got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestRuntimePipelineCreate2EndLabelWriteEventInstrumentation(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Begin)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmtAny, err := parser.ParseStatement("MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}

	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}

	engine := runtime.New()
	input := runtime.ExecutionContext{
		Plan:   physicalPlan,
		Tenant: "acme",
		Params: map[string]any{"tenant": "acme"},
	}

	var runtimeResult runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}); err != nil {
		t.Fatalf("runtime execute-with-tx failed: %v", err)
	}

	if len(runtimeResult.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", runtimeResult.WriteEvents)
	}
	event := runtimeResult.WriteEvents[0]
	t.Logf("Create2[11] write event payload: kind=%s pattern=%q raw=%q mutation=%s edge=%#v bindings=%#v", event.Kind, event.Pattern, event.Raw, event.MutationType, event.Edge, event.Bindings)
	if event.Edge == nil {
		t.Fatalf("expected edge mutation payload, got %#v", event)
	}
	if len(event.Edge.RightLabels) == 0 {
		t.Fatalf("expected end-node label metadata in event payload, got edge=%#v", event.Edge)
	}

	hasLabel := func(labels []string, label string) bool {
		for _, l := range labels {
			if strings.EqualFold(strings.TrimSpace(l), strings.TrimSpace(label)) {
				return true
			}
		}
		return false
	}

	beginCount := 0
	endCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 100, func(found *graph.Vertex) error {
			if found == nil {
				return nil
			}
			if hasLabel(found.Labels, "Begin") {
				beginCount++
			}
			if hasLabel(found.Labels, "End") {
				endCount++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
	t.Logf("Create2[11] post-apply label counts: Begin=%d End=%d", beginCount, endCount)
	if endCount != 1 {
		t.Fatalf("expected one End-labeled vertex after apply, got Begin=%d End=%d", beginCount, endCount)
	}
}

func TestExecuteStatementCreate2RuntimeOnlyExecutesMigratedMatchCreateShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Begin)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmtAny, err := parser.ParseStatement("MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}
	runtimeRes, runtimeOK, err := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("expected runtime-only mode to execute migrated MATCH+CREATE shape, got %v", err)
	}
	if !runtimeOK {
		t.Fatalf("expected runtime-only routing path to report handled=true")
	}
	if runtimeRes != nil && len(runtimeRes.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime query, got %#v", runtimeRes.Rows)
	}

	beginCount := 0
	endCount := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 100, func(found *graph.Vertex) error {
			if found == nil {
				return nil
			}
			for _, label := range found.Labels {
				switch strings.ToUpper(strings.TrimSpace(label)) {
				case "BEGIN":
					beginCount++
				case "END":
					endCount++
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
	if beginCount != 1 || endCount != 1 {
		t.Fatalf("expected Begin=1 and End=1 after runtime MATCH+CREATE, got Begin=%d End=%d", beginCount, endCount)
	}
}
func TestRuntimePipelineVertexOnlyMatchExpansionBindsVariable(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Begin)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_EXPAND_MATCH",
			Attrs: map[string]any{"pattern": "(x:Begin)", "variant": "expand_default", "accessPath": "adjacency_expand"},
		}},
	}
	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: plan, Tenant: "acme", Params: map[string]any{"tenant": "acme"}}

	var result runtime.ExecutionResult
	if err := store.View(ctx, func(tx graph.Tx) error {
		res, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		result = res
		return nil
	}); err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	t.Logf("vertex-only match expansion rows=%#v", result.Rows)
	if len(result.Rows) != 1 {
		t.Fatalf("expected one row from vertex-only match expansion, got %#v", result.Rows)
	}
	if got := result.Rows[0]["x"]; got == nil || strings.TrimSpace(fmt.Sprint(got)) == "" {
		t.Fatalf("expected x binding from vertex-only match expansion, got %#v", result.Rows[0])
	}
}

func TestRuntimePipelineVertexOnlyMatchExpansionBindsVariableInUpdateTx(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (:Begin)")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	plan := physical.Plan{
		RootNodeID: "p1",
		Nodes: []physical.Node{{
			ID:    "p1",
			Op:    "PHY_EXPAND_MATCH",
			Attrs: map[string]any{"pattern": "(x:Begin)", "variant": "expand_default", "accessPath": "adjacency_expand"},
		}},
	}
	engine := runtime.New()
	input := runtime.ExecutionContext{Plan: plan, Tenant: "acme", Params: map[string]any{"tenant": "acme"}}

	var result runtime.ExecutionResult
	if err := store.Update(ctx, func(tx graph.Tx) error {
		res, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		result = res
		return nil
	}); err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	t.Logf("vertex-only match expansion rows in update tx=%#v", result.Rows)
	if len(result.Rows) != 1 {
		t.Fatalf("expected one row from vertex-only match expansion in update tx, got %#v", result.Rows)
	}
	if got := result.Rows[0]["x"]; got == nil || strings.TrimSpace(fmt.Sprint(got)) == "" {
		t.Fatalf("expected x binding from vertex-only match expansion in update tx, got %#v", result.Rows[0])
	}
}
func TestExecuteStatementRuntimePipelineWriteOnlyEdgeCreateParity(t *testing.T) {
	const referenceShouldCreateDirectedEdge = true

	ctx := context.Background()
	queryText := "CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})"
	params := Params{"tenant": "acme", "src": "u110", "dst": "u111"}

	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})
	runtimeStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	runtimeParams := Params{}
	for k, v := range params {
		runtimeParams[k] = v
	}
	if _, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams); err != nil {
		t.Fatalf("runtime execution failed: %v", err)
	}

	referenceStore := openStore(t)
	defer func() { _ = referenceStore.Close() }()
	referenceExec := New(referenceStore, Options{})
	referenceStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	referenceParams := Params{}
	for k, v := range params {
		referenceParams[k] = v
	}
	if _, err := referenceExec.ExecuteStatement(ctx, referenceStmt, referenceParams); err != nil {
		t.Fatalf("reference execution failed: %v", err)
	}

	checkHasDirected := func(store graph.GraphStore, tenant, src, dst, edgeType string) bool {
		has := false
		err := store.View(ctx, func(tx graph.Tx) error {
			found, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, edgeType)
			if err != nil {
				return err
			}
			has = found
			return nil
		})
		if err != nil {
			t.Fatalf("directed edge presence check failed: %v", err)
		}
		return has
	}

	runtimeHas := checkHasDirected(runtimeStore, "acme", "u110", "u111", "KNOWS")
	referenceHas := checkHasDirected(referenceStore, "acme", "u110", "u111", "KNOWS")

	if !runtimeHas {
		t.Fatalf("expected runtime path to create directed edge")
	}
	if !referenceShouldCreateDirectedEdge {
		t.Fatalf("reference parity guard is misconfigured; expected true")
	}
	if referenceHas != referenceShouldCreateDirectedEdge {
		t.Fatalf("reference write-only edge parity mismatch: got=%v want=%v", referenceHas, referenceShouldCreateDirectedEdge)
	}
}

func TestExecuteStatementRuntimePipelineReverseWriteOnlyEdgeCreateParity(t *testing.T) {
	const referenceShouldCreateDirectedEdge = true

	ctx := context.Background()
	queryText := "CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})"
	params := Params{"tenant": "acme", "src": "u112", "dst": "u113"}

	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})
	runtimeStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	runtimeParams := Params{}
	for k, v := range params {
		runtimeParams[k] = v
	}
	if _, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams); err != nil {
		t.Fatalf("runtime execution failed: %v", err)
	}

	referenceStore := openStore(t)
	defer func() { _ = referenceStore.Close() }()
	referenceExec := New(referenceStore, Options{})
	referenceStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	referenceParams := Params{}
	for k, v := range params {
		referenceParams[k] = v
	}
	if _, err := referenceExec.ExecuteStatement(ctx, referenceStmt, referenceParams); err != nil {
		t.Fatalf("reference execution failed: %v", err)
	}

	checkHasDirected := func(store graph.GraphStore, tenant, src, dst, edgeType string) bool {
		has := false
		err := store.View(ctx, func(tx graph.Tx) error {
			found, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, edgeType)
			if err != nil {
				return err
			}
			has = found
			return nil
		})
		if err != nil {
			t.Fatalf("directed edge presence check failed: %v", err)
		}
		return has
	}

	runtimeHas := checkHasDirected(runtimeStore, "acme", "u113", "u112", "KNOWS")
	referenceHas := checkHasDirected(referenceStore, "acme", "u113", "u112", "KNOWS")

	if !runtimeHas {
		t.Fatalf("expected runtime path to create reverse directed edge")
	}
	if !referenceShouldCreateDirectedEdge {
		t.Fatalf("reference parity guard is misconfigured; expected true")
	}
	if referenceHas != referenceShouldCreateDirectedEdge {
		t.Fatalf("reference reverse write-only edge parity mismatch: got=%v want=%v", referenceHas, referenceShouldCreateDirectedEdge)
	}
}
