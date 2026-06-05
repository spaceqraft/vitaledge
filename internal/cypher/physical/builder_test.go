package physical

import (
	"reflect"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

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
