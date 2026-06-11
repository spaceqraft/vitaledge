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
	if pattern, _ := writeNode.Attrs["pattern"].(string); pattern == "" {
		t.Fatalf("expected WRITE pattern attr, got attrs=%#v", writeNode.Attrs)
	}
}

func TestBuildLogicalPlanForInQueryCall(t *testing.T) {
	stmt, err := parser.ParseStatement("WITH 1 AS x CALL db.stats.vertexCount() YIELD vertexCount RETURN x, vertexCount")
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

	expected := []string{"PROJECT", "CALL", "PROJECT"}
	if !reflect.DeepEqual(ops, expected) {
		t.Fatalf("unexpected logical op sequence: got=%v want=%v", ops, expected)
	}

	callNode := plan.Nodes[1]
	if kind, _ := callNode.Attrs["kind"].(string); kind != "IN_QUERY_CALL" {
		t.Fatalf("expected CALL kind IN_QUERY_CALL, got attrs=%#v", callNode.Attrs)
	}
}
