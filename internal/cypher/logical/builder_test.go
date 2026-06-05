package logical

import (
	"reflect"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

func TestBuildLogicalPlanForMatchQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN DISTINCT dst.id AS dstID ORDER BY dstID DESC SKIP 1 LIMIT 2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}

	plan := Build(model)
	if plan.RootNodeID == "" {
		t.Fatalf("expected non-empty root node id")
	}

	ops := make([]string, 0, len(plan.Nodes))
	for _, node := range plan.Nodes {
		ops = append(ops, node.Op)
	}

	expected := []string{"MATCH", "PROJECT", "SORT", "PAGINATION"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected logical op sequence: got=%v want=%v", ops, expected)
	}
}

func TestBuildLogicalPlanForWriteQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}

	plan := Build(model)
	ops := make([]string, 0, len(plan.Nodes))
	for _, node := range plan.Nodes {
		ops = append(ops, node.Op)
	}

	expected := []string{"MATCH", "WRITE", "PROJECT", "PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected logical op sequence: got=%v want=%v", ops, expected)
	}

	writeNode := plan.Nodes[1]
	if kind, _ := writeNode.Attrs["kind"].(string); kind != "MERGE" {
		t.Fatalf("expected WRITE kind MERGE, got attrs=%#v", writeNode.Attrs)
	}
}
