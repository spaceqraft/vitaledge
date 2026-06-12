package physical

import (
	"reflect"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	runtimeoperators "github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

func assertPlannerVariantsRuntimeSupported(t *testing.T, physicalPlan Plan) {
	t.Helper()
	for _, n := range physicalPlan.Nodes {
		variant, _ := n.Attrs["variant"].(string)
		variant = strings.TrimSpace(variant)
		if variant == "" {
			continue
		}
		if !runtimeoperators.SupportsVariant(n.Op, variant) {
			t.Fatalf("planner emitted unsupported runtime variant: op=%s variant=%s attrs=%#v", n.Op, variant, n.Attrs)
		}
	}
}

func TestBuildPhysicalPlanFromLogicalMatchQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN DISTINCT dst.id AS dstID ORDER BY dstID DESC SKIP 1 LIMIT 2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	if physicalPlan.RootNodeID == "" {
		t.Fatalf("expected non-empty physical root node id")
	}

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_PROJECT", "PHY_SORT", "PHY_PAGINATION"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical op sequence: got=%v want=%v", ops, expected)
	}

	first := physicalPlan.Nodes[0]
	if got, _ := first.Attrs["accessPath"].(string); got != "adjacency_expand" {
		t.Fatalf("unexpected access path attr: %#v", first.Attrs)
	}
}

func TestBuildPhysicalPlanFromLogicalWriteQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_WRITE", "PHY_PROJECT", "PHY_PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical op sequence: got=%v want=%v", ops, expected)
	}

	write := physicalPlan.Nodes[1]
	if got, _ := write.Attrs["strategy"].(string); got != "typed_write_operator" {
		t.Fatalf("unexpected write strategy attr: %#v", write.Attrs)
	}
}

func TestBuildPhysicalPlanFromLogicalInQueryCall(t *testing.T) {
	stmt, err := parser.ParseStatement("WITH 1 AS x CALL db.stats.vertexCount() YIELD vertexCount RETURN x, vertexCount")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}

	expected := []string{"PHY_PROJECT", "PHY_CALL", "PHY_PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical op sequence: got=%v want=%v", ops, expected)
	}

	call := physicalPlan.Nodes[1]
	if got, _ := call.Attrs["strategy"].(string); got != "procedure_call" {
		t.Fatalf("unexpected call strategy attr: %#v", call.Attrs)
	}
}

func TestBuildPhysicalPlanRewritesMatchWhereFalseToEmpty(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE false RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical nodes")
	}
	if physicalPlan.Nodes[0].Op != "PHY_EMPTY" {
		t.Fatalf("expected first op PHY_EMPTY after prune rewrite, got %#v", physicalPlan.Nodes[0])
	}
	if got, _ := physicalPlan.Nodes[0].Attrs["pruneReason"].(string); got != "where_always_false" {
		t.Fatalf("expected prune reason where_always_false, got %#v", physicalPlan.Nodes[0].Attrs)
	}
}

func TestBuildPhysicalPlanAddsAntiProbeForNotRelationshipWhere(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:BLOCKED]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}
	expected := []string{"PHY_EXPAND_MATCH", "PHY_ANTI_PROBE", "PHY_PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical ops for anti rewrite: got=%v want=%v", ops, expected)
	}
	anti := physicalPlan.Nodes[1]
	if got, _ := anti.Attrs["leftVar"].(string); got != "u" {
		t.Fatalf("expected anti leftVar u, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["rightVar"].(string); got != "v" {
		t.Fatalf("expected anti rightVar v, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["edgeType"].(string); got != "BLOCKED" {
		t.Fatalf("expected anti edgeType BLOCKED, got %#v", anti.Attrs)
	}
}

func TestBuildPhysicalPlanAddsDirectedAntiProbeForOutPattern(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:BLOCKED]->(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}
	expected := []string{"PHY_EXPAND_MATCH", "PHY_ANTI_PROBE", "PHY_PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical ops for directed anti rewrite: got=%v want=%v", ops, expected)
	}
	anti := physicalPlan.Nodes[1]
	if got, _ := anti.Attrs["mode"].(string); got != "directed" {
		t.Fatalf("expected directed anti mode, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["leftVar"].(string); got != "u" {
		t.Fatalf("expected anti leftVar u, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["rightVar"].(string); got != "v" {
		t.Fatalf("expected anti rightVar v, got %#v", anti.Attrs)
	}
}

func TestBuildPhysicalPlanAddsDirectedAntiProbeForInPatternBySwappingVars(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)<-[:BLOCKED]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	anti := physicalPlan.Nodes[1]
	if got, _ := anti.Attrs["mode"].(string); got != "directed" {
		t.Fatalf("expected directed anti mode, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["leftVar"].(string); got != "v" {
		t.Fatalf("expected swapped source var v for <- pattern, got %#v", anti.Attrs)
	}
	if got, _ := anti.Attrs["rightVar"].(string); got != "u" {
		t.Fatalf("expected swapped destination var u for <- pattern, got %#v", anti.Attrs)
	}
}

func TestBuildPhysicalPlanAddsMultipleAntiProbesFromAndWhere(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:BLOCKED]-(v)) AND NOT ((u)-[:MUTED]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	ops := make([]string, 0, len(physicalPlan.Nodes))
	for _, n := range physicalPlan.Nodes {
		ops = append(ops, n.Op)
	}
	expected := []string{"PHY_EXPAND_MATCH", "PHY_ANTI_PROBE", "PHY_ANTI_PROBE", "PHY_PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected physical ops for multi anti rewrite: got=%v want=%v", ops, expected)
	}
	first := physicalPlan.Nodes[1]
	second := physicalPlan.Nodes[2]
	if got, _ := first.Attrs["edgeType"].(string); got != "BLOCKED" {
		t.Fatalf("expected first anti edgeType BLOCKED, got %#v", first.Attrs)
	}
	if got, _ := second.Attrs["edgeType"].(string); got != "MUTED" {
		t.Fatalf("expected second anti edgeType MUTED, got %#v", second.Attrs)
	}
}

func TestBuildPhysicalPlanSkipsAntiRewriteWhenWhereContainsOr(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:BLOCKED]-(v)) OR u.id = 'u1' RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := Build(logicalPlan)

	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_ANTI_PROBE" {
			t.Fatalf("did not expect anti rewrite when OR exists in where, got plan %#v", physicalPlan.Nodes)
		}
	}
}

func TestBuildPhysicalPlanOrdersAntiProbesBySelectivityHints(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:MUTED]-(v)) AND NOT ((u)-[:BLOCKED]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		AntiProbeHitRateBy: map[string]float64{
			"MUTED":   0.20,
			"BLOCKED": 0.95,
		},
	})

	if len(physicalPlan.Nodes) < 4 {
		t.Fatalf("expected expand + 2 anti + project, got %#v", physicalPlan.Nodes)
	}
	firstAnti := physicalPlan.Nodes[1]
	secondAnti := physicalPlan.Nodes[2]
	if got, _ := firstAnti.Attrs["edgeType"].(string); got != "BLOCKED" {
		t.Fatalf("expected first anti edgeType BLOCKED by selectivity sort, got %#v", firstAnti.Attrs)
	}
	if got, _ := secondAnti.Attrs["edgeType"].(string); got != "MUTED" {
		t.Fatalf("expected second anti edgeType MUTED by selectivity sort, got %#v", secondAnti.Attrs)
	}
	if got, _ := firstAnti.Attrs["variant"].(string); got != "anti_probe_batch_high" {
		t.Fatalf("expected first anti variant anti_probe_batch_high, got %#v", firstAnti.Attrs)
	}
	if got, _ := secondAnti.Attrs["variant"].(string); got != "anti_probe_row_low" {
		t.Fatalf("expected second anti variant anti_probe_row_low, got %#v", secondAnti.Attrs)
	}
}

func TestBuildPhysicalPlanOrdersAntiProbesByDerivedCountStats(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:FOLLOWED]-(v)) AND NOT ((u)-[:FRIEND]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeTypeCounts: map[string]int{
			"FOLLOWED": 12,
			"FRIEND":   2,
		},
		EdgeSourceCounts: map[string]int{
			"FOLLOWED": 2,
			"FRIEND":   2,
		},
	})

	if len(physicalPlan.Nodes) < 4 {
		t.Fatalf("expected expand + 2 anti + project, got %#v", physicalPlan.Nodes)
	}
	firstAnti := physicalPlan.Nodes[1]
	secondAnti := physicalPlan.Nodes[2]
	if got, _ := firstAnti.Attrs["edgeType"].(string); got != "FOLLOWED" {
		t.Fatalf("expected first anti edgeType FOLLOWED by derived count stats, got %#v", firstAnti.Attrs)
	}
	if got, _ := secondAnti.Attrs["edgeType"].(string); got != "FRIEND" {
		t.Fatalf("expected second anti edgeType FRIEND by derived count stats, got %#v", secondAnti.Attrs)
	}
	if got, _ := firstAnti.Attrs["variant"].(string); got != "anti_probe_batch_high" {
		t.Fatalf("expected first anti variant anti_probe_batch_high from derived count stats, got %#v", firstAnti.Attrs)
	}
	if got, _ := secondAnti.Attrs["variant"].(string); got != "anti_probe_row_low" {
		t.Fatalf("expected second anti variant anti_probe_row_low from derived count stats, got %#v", secondAnti.Attrs)
	}
}

func TestBuildWithStatsRewritesMatchToEmptyWhenEdgeTypeCountZero(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:BLOCKED]->(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeTypeCounts: map[string]int{"BLOCKED": 0},
	})

	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical nodes")
	}
	if physicalPlan.Nodes[0].Op != "PHY_EMPTY" {
		t.Fatalf("expected MATCH rewrite to PHY_EMPTY from stats hint, got %#v", physicalPlan.Nodes[0])
	}
}

func TestBuildWithStatsChoosesTopKSortStrategyWhenLimitIsSmallRelativeToEdgeTotal(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{HasEdgeTotal: true, EdgeTotal: 1000})

	var sortNode *Node
	for i := range physicalPlan.Nodes {
		if physicalPlan.Nodes[i].Op == "PHY_SORT" {
			sortNode = &physicalPlan.Nodes[i]
			break
		}
	}
	if sortNode == nil {
		t.Fatalf("expected PHY_SORT node in plan %#v", physicalPlan.Nodes)
	}
	if got, _ := sortNode.Attrs["strategy"].(string); got != "topk_heap" {
		t.Fatalf("expected topk_heap sort strategy, got attrs=%#v", sortNode.Attrs)
	}
	if got, _ := sortNode.Attrs["topK"].(int); got != 10 {
		t.Fatalf("expected topK=10 attr for topk_heap strategy, got attrs=%#v", sortNode.Attrs)
	}
}

func TestBuildWithStatsKeepsInMemorySortWhenLimitIsLargeRelativeToEdgeTotal(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{HasEdgeTotal: true, EdgeTotal: 20})

	var sortNode *Node
	for i := range physicalPlan.Nodes {
		if physicalPlan.Nodes[i].Op == "PHY_SORT" {
			sortNode = &physicalPlan.Nodes[i]
			break
		}
	}
	if sortNode == nil {
		t.Fatalf("expected PHY_SORT node in plan %#v", physicalPlan.Nodes)
	}
	if got, _ := sortNode.Attrs["strategy"].(string); got != "in_memory_sort" {
		t.Fatalf("expected in_memory_sort strategy, got attrs=%#v", sortNode.Attrs)
	}
	if _, exists := sortNode.Attrs["topK"]; exists {
		t.Fatalf("did not expect topK attr for in_memory_sort strategy, got attrs=%#v", sortNode.Attrs)
	}
	if got, _ := sortNode.Attrs["variant"].(string); got != "sort_full" {
		t.Fatalf("expected sort_full variant, got attrs=%#v", sortNode.Attrs)
	}
}

func TestBuildWithStatsSortTopKUsesSkipPlusLimitWindow(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC SKIP 5 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{HasEdgeTotal: true, EdgeTotal: 1000})

	var sortNode *Node
	for i := range physicalPlan.Nodes {
		if physicalPlan.Nodes[i].Op == "PHY_SORT" {
			sortNode = &physicalPlan.Nodes[i]
			break
		}
	}
	if sortNode == nil {
		t.Fatalf("expected PHY_SORT node in plan %#v", physicalPlan.Nodes)
	}
	if got, _ := sortNode.Attrs["strategy"].(string); got != "topk_heap" {
		t.Fatalf("expected topk_heap strategy, got attrs=%#v", sortNode.Attrs)
	}
	if got, _ := sortNode.Attrs["variant"].(string); got != "sort_topk_heap" {
		t.Fatalf("expected sort_topk_heap variant, got attrs=%#v", sortNode.Attrs)
	}
	if got, _ := sortNode.Attrs["topK"].(int); got != 15 {
		t.Fatalf("expected effective topK=15 from skip+limit, got attrs=%#v", sortNode.Attrs)
	}
}

func TestBuildWithStatsUndirectedExpandPrefersInboundFirstWhenOutDegreeHigh(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]-(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"KNOWS": 3.0},
	})

	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	first := physicalPlan.Nodes[0]
	if first.Op != "PHY_EXPAND_MATCH" {
		t.Fatalf("expected first op PHY_EXPAND_MATCH, got %#v", first)
	}
	if got, _ := first.Attrs["accessPath"].(string); got != "adjacency_expand_in_first" {
		t.Fatalf("expected undirected in_first access path, got attrs=%#v", first.Attrs)
	}
}

func TestBuildWithStatsUndirectedExpandPrefersOutFirstWhenOutDegreeLow(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]-(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"KNOWS": 1.0},
	})

	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	first := physicalPlan.Nodes[0]
	if first.Op != "PHY_EXPAND_MATCH" {
		t.Fatalf("expected first op PHY_EXPAND_MATCH, got %#v", first)
	}
	if got, _ := first.Attrs["accessPath"].(string); got != "adjacency_expand_out_first" {
		t.Fatalf("expected undirected out_first access path, got attrs=%#v", first.Attrs)
	}
}

func TestBuildWithStatsDirectedExpandAccessPathRemainsStable(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"KNOWS": 9.0},
	})

	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	first := physicalPlan.Nodes[0]
	if got, _ := first.Attrs["accessPath"].(string); got != "adjacency_expand" {
		t.Fatalf("expected directed access path to remain adjacency_expand, got attrs=%#v", first.Attrs)
	}
}

func TestBuildWithStatsUsesCountDerivedDegreeHintsAndSortFallbacks(t *testing.T) {
	t.Run("count-derived degree hints drive traversal and join choices", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]-(v:User) MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{
			EdgeTypeCounts:   map[string]int{"KNOWS": 3, "LIKES": 3},
			EdgeSourceCounts: map[string]int{"KNOWS": 3, "LIKES": 3},
		})

		expands := make([]Node, 0, 2)
		for _, n := range physicalPlan.Nodes {
			if n.Op == "PHY_EXPAND_MATCH" {
				expands = append(expands, n)
			}
		}
		if len(expands) < 2 {
			t.Fatalf("expected at least two PHY_EXPAND_MATCH nodes, got %#v", physicalPlan.Nodes)
		}
		if got, _ := expands[0].Attrs["accessPath"].(string); got != "adjacency_expand_out_first" {
			t.Fatalf("expected count-derived undirected traversal to prefer out_first, got attrs=%#v", expands[0].Attrs)
		}
		if got, _ := expands[1].Attrs["joinStrategy"].(string); got != "indexed_bind_join" {
			t.Fatalf("expected count-derived join strategy to prefer indexed_bind_join, got attrs=%#v", expands[1].Attrs)
		}
	})

	t.Run("optional count-derived degree hints remain runtime-stable", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) OPTIONAL MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{
			EdgeTypeCounts:   map[string]int{"LIKES": 2},
			EdgeSourceCounts: map[string]int{"LIKES": 2},
		})

		var optionalNode *Node
		for i := range physicalPlan.Nodes {
			if physicalPlan.Nodes[i].Op == "PHY_EXPAND_OPTIONAL" {
				optionalNode = &physicalPlan.Nodes[i]
				break
			}
		}
		if optionalNode == nil {
			t.Fatalf("expected PHY_EXPAND_OPTIONAL node, got %#v", physicalPlan.Nodes)
		}
		if got, _ := optionalNode.Attrs["accessPath"].(string); got != "adjacency_expand_optional_out_first" {
			t.Fatalf("expected optional traversal to prefer out_first from count-derived hints, got attrs=%#v", optionalNode.Attrs)
		}
		if got, _ := optionalNode.Attrs["joinStrategy"].(string); got != "indexed_bind_join" {
			t.Fatalf("expected optional join strategy to use indexed_bind_join from count-derived hints, got attrs=%#v", optionalNode.Attrs)
		}
	})

	t.Run("sort falls back to in-memory without edge-total stats", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC LIMIT 10")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{})

		var sortNode *Node
		for i := range physicalPlan.Nodes {
			if physicalPlan.Nodes[i].Op == "PHY_SORT" {
				sortNode = &physicalPlan.Nodes[i]
				break
			}
		}
		if sortNode == nil {
			t.Fatalf("expected PHY_SORT node in plan %#v", physicalPlan.Nodes)
		}
		if got, _ := sortNode.Attrs["strategy"].(string); got != "in_memory_sort" {
			t.Fatalf("expected in_memory_sort fallback without edge total stats, got attrs=%#v", sortNode.Attrs)
		}
		if got, _ := sortNode.Attrs["variant"].(string); got != "sort_full" {
			t.Fatalf("expected sort_full fallback variant without edge total stats, got attrs=%#v", sortNode.Attrs)
		}
		if _, exists := sortNode.Attrs["topK"]; exists {
			t.Fatalf("did not expect topK attr without edge total stats, got attrs=%#v", sortNode.Attrs)
		}
	})
}

func TestBuildWithStatsChainedMatchChoosesIndexedBindJoinForLowDegree(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) MATCH (v)-[:LIKES]->(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"LIKES": 1.0},
	})

	expands := make([]Node, 0, 2)
	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_EXPAND_MATCH" {
			expands = append(expands, n)
		}
	}
	if len(expands) < 2 {
		t.Fatalf("expected at least two PHY_EXPAND_MATCH nodes for chained MATCH, got %#v", physicalPlan.Nodes)
	}
	if got, _ := expands[1].Attrs["joinStrategy"].(string); got != "indexed_bind_join" {
		t.Fatalf("expected indexed_bind_join on second expand, got attrs=%#v", expands[1].Attrs)
	}
}

func TestBuildWithStatsChainedMatchChoosesNestedLoopJoinForHighDegree(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) MATCH (v)-[:LIKES]->(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"LIKES": 4.0},
	})

	expands := make([]Node, 0, 2)
	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_EXPAND_MATCH" {
			expands = append(expands, n)
		}
	}
	if len(expands) < 2 {
		t.Fatalf("expected at least two PHY_EXPAND_MATCH nodes for chained MATCH, got %#v", physicalPlan.Nodes)
	}
	if got, _ := expands[1].Attrs["joinStrategy"].(string); got != "nested_loop_join" {
		t.Fatalf("expected nested_loop_join on second expand, got attrs=%#v", expands[1].Attrs)
	}
}

func TestBuildWithStatsOptionalExpandChoosesIndexedBindJoinForLowDegree(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) OPTIONAL MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"LIKES": 1.0},
	})

	optionalExpands := make([]Node, 0, 1)
	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_EXPAND_OPTIONAL" {
			optionalExpands = append(optionalExpands, n)
		}
	}
	if len(optionalExpands) == 0 {
		t.Fatalf("expected at least one PHY_EXPAND_OPTIONAL node, got %#v", physicalPlan.Nodes)
	}
	if got, _ := optionalExpands[0].Attrs["joinStrategy"].(string); got != "indexed_bind_join" {
		t.Fatalf("expected indexed_bind_join on optional expand, got attrs=%#v", optionalExpands[0].Attrs)
	}
}

func TestBuildWithStatsOptionalExpandChoosesNestedLoopJoinForHighDegree(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) OPTIONAL MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(model)
	physicalPlan := BuildWithStats(logicalPlan, StatsHints{
		EdgeAvgOutDegree: map[string]float64{"LIKES": 3.5},
	})

	optionalExpands := make([]Node, 0, 1)
	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_EXPAND_OPTIONAL" {
			optionalExpands = append(optionalExpands, n)
		}
	}
	if len(optionalExpands) == 0 {
		t.Fatalf("expected at least one PHY_EXPAND_OPTIONAL node, got %#v", physicalPlan.Nodes)
	}
	if got, _ := optionalExpands[0].Attrs["joinStrategy"].(string); got != "nested_loop_join" {
		t.Fatalf("expected nested_loop_join on optional expand, got attrs=%#v", optionalExpands[0].Attrs)
	}
}

func TestBuildWithStatsExpandSetsExplicitVariantField(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]-(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	physicalPlan := BuildWithStats(logical.Build(model), StatsHints{EdgeAvgOutDegree: map[string]float64{"KNOWS": 2.5}})
	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	first := physicalPlan.Nodes[0]
	if got, _ := first.Attrs["variant"].(string); got != "expand_in_first" {
		t.Fatalf("expected explicit expand variant expand_in_first, got attrs=%#v", first.Attrs)
	}
}

func TestBuildWithStatsChainedExpandVariantCarriesJoinFlavor(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) MATCH (v)-[:LIKES]->(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	physicalPlan := BuildWithStats(logical.Build(model), StatsHints{EdgeAvgOutDegree: map[string]float64{"LIKES": 1.0}})

	expands := make([]Node, 0, 2)
	for _, n := range physicalPlan.Nodes {
		if n.Op == "PHY_EXPAND_MATCH" {
			expands = append(expands, n)
		}
	}
	if len(expands) < 2 {
		t.Fatalf("expected at least two expand nodes, got %#v", physicalPlan.Nodes)
	}
	if got, _ := expands[1].Attrs["variant"].(string); got != "expand_default_indexed" {
		t.Fatalf("expected chained expand variant with join flavor, got attrs=%#v", expands[1].Attrs)
	}
}

func TestBuildWithStatsPlannerVariantsAreRuntimeSupported(t *testing.T) {
	t.Run("expand undirected", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]-(v:User) RETURN v.id AS vid")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{EdgeAvgOutDegree: map[string]float64{"KNOWS": 2.5}})
		assertPlannerVariantsRuntimeSupported(t, physicalPlan)
	})

	t.Run("optional expand chained", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) OPTIONAL MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{EdgeAvgOutDegree: map[string]float64{"LIKES": 1.0}})
		assertPlannerVariantsRuntimeSupported(t, physicalPlan)
	})

	t.Run("sort topk", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC SKIP 5 LIMIT 10")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{HasEdgeTotal: true, EdgeTotal: 1000})
		assertPlannerVariantsRuntimeSupported(t, physicalPlan)
	})

	t.Run("anti probe", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:MUTED]-(v)) AND NOT ((u)-[:BLOCKED]-(v)) RETURN v.id AS vid")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		model, err := semantic.Build(stmt)
		if err != nil {
			t.Fatalf("semantic build failed: %v", err)
		}
		physicalPlan := BuildWithStats(logical.Build(model), StatsHints{
			AntiProbeHitRateBy: map[string]float64{
				"MUTED":   0.20,
				"BLOCKED": 0.95,
			},
		})
		assertPlannerVariantsRuntimeSupported(t, physicalPlan)
	})
}

func TestBuildWithStatsAnnotatesTypedDomainGuardrailEqualityBoundary(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {age: 30}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	physicalPlan := BuildWithStats(logical.Build(model), StatsHints{
		PropertyDomainHints: map[string]PropertyDomainHint{
			"vertex|USER|age": {
				EntityClass:    "vertex",
				Schema:         "User",
				Property:       "age",
				TypeDomain:     "numeric",
				Strategy:       "typed_property_index_seek",
				GuardrailState: "typed_seek_preferred",
				GuardrailRule:  "typed_seek_if_single_known_domain",
				Reason:         "single known property type domain supports typed seek",
				DominantKind:   "numeric",
				DominantShare:  1.0,
				SampleSize:     40,
				EqualitySel:    0.02,
				RangeSel:       0.08,
			},
		},
	})
	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	guardrail, ok := physicalPlan.Nodes[0].Attrs["typedDomainGuardrail"].(map[string]any)
	if !ok {
		t.Fatalf("expected typedDomainGuardrail attr, got %#v", physicalPlan.Nodes[0].Attrs)
	}
	if got, _ := guardrail["predicateClass"].(string); got != "equality" {
		t.Fatalf("expected equality predicateClass, got %#v", guardrail)
	}
	if got, _ := guardrail["planBoundary"].(string); got != "equality_predicate" {
		t.Fatalf("expected equality_predicate boundary, got %#v", guardrail)
	}
}

func TestBuildWithStatsAnnotatesTypedDomainGuardrailRangeBoundary(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {age: 30}) WHERE u.age > 18 RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	physicalPlan := BuildWithStats(logical.Build(model), StatsHints{
		PropertyDomainHints: map[string]PropertyDomainHint{
			"vertex|USER|age": {
				EntityClass:    "vertex",
				Schema:         "User",
				Property:       "age",
				TypeDomain:     "numeric",
				Strategy:       "typed_property_index_seek",
				GuardrailState: "typed_seek_guarded",
				GuardrailRule:  "guardrail_if_null_or_absent_rate_high",
				Reason:         "null/absent rates are high; prefer guarded typed seek with conservative selectivity",
				DominantKind:   "numeric",
				DominantShare:  1.0,
				SampleSize:     40,
				NullRate:       0.35,
				AbsentRate:     0.65,
				EqualitySel:    0.01,
				RangeSel:       0.04,
			},
		},
	})
	if len(physicalPlan.Nodes) == 0 {
		t.Fatalf("expected physical plan nodes")
	}
	guardrail, ok := physicalPlan.Nodes[0].Attrs["typedDomainGuardrail"].(map[string]any)
	if !ok {
		t.Fatalf("expected typedDomainGuardrail attr, got %#v", physicalPlan.Nodes[0].Attrs)
	}
	if got, _ := guardrail["predicateClass"].(string); got != "range" {
		t.Fatalf("expected range predicateClass, got %#v", guardrail)
	}
	if got, _ := guardrail["planBoundary"].(string); got != "range_predicate" {
		t.Fatalf("expected range_predicate boundary, got %#v", guardrail)
	}
	if got, _ := guardrail["rule"].(string); got != "guardrail_if_null_or_absent_rate_high" {
		t.Fatalf("expected null/absent guardrail rule, got %#v", guardrail)
	}
}
