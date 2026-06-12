package semantic

import (
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/pipeline"
)

func TestBuildMatchQuerySemanticModel(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN DISTINCT dst.id AS dstID ORDER BY dstID DESC SKIP 1 LIMIT 2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := Build(stmt)
	if err != nil {
		t.Fatalf("build semantic model failed: %v", err)
	}

	if model.StatementKind != ast.StatementKindMatchQuery {
		t.Fatalf("unexpected statement kind: %s", model.StatementKind)
	}
	if len(model.Patterns) != 1 {
		t.Fatalf("expected one pattern intent, got %d", len(model.Patterns))
	}
	if model.Patterns[0].Kind != ast.ClauseKindMatch {
		t.Fatalf("expected match clause kind on pattern intent, got %#v", model.Patterns[0])
	}
	if model.Patterns[0].Pattern == "" {
		t.Fatalf("expected non-empty pattern intent")
	}
	if len(model.Projections) != 1 {
		t.Fatalf("expected one projection intent, got %d", len(model.Projections))
	}
	if !model.Projections[0].Distinct {
		t.Fatalf("expected distinct projection intent")
	}
	if len(model.Projections[0].Items) != 1 {
		t.Fatalf("expected one projection item, got %#v", model.Projections[0].Items)
	}
	if model.Projections[0].Items[0].Expression != "dst.id" || model.Projections[0].Items[0].Alias != "dstID" {
		t.Fatalf("unexpected projection item intent: %#v", model.Projections[0].Items[0])
	}
	if len(model.Projections[0].OrderBy) != 1 || model.Projections[0].OrderBy[0].Expression != "dstID" {
		t.Fatalf("unexpected projection order by intents: %#v", model.Projections[0].OrderBy)
	}
	if model.Projections[0].Pagination.SkipExpr != "1" || model.Projections[0].Pagination.LimitExpr != "2" {
		t.Fatalf("unexpected projection pagination intent: %#v", model.Projections[0].Pagination)
	}
	if len(model.Ordering) != 1 || model.Ordering[0].Expression != "dstID" {
		t.Fatalf("unexpected ordering intents: %#v", model.Ordering)
	}
	if model.Pagination.SkipExpr != "1" || model.Pagination.LimitExpr != "2" {
		t.Fatalf("unexpected pagination intent: %#v", model.Pagination)
	}
}

func TestBuildQueryStatementSemanticModelWithWrites(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := Build(stmt)
	if err != nil {
		t.Fatalf("build semantic model failed: %v", err)
	}

	if model.StatementKind != ast.StatementKindQuery {
		t.Fatalf("unexpected statement kind: %s", model.StatementKind)
	}
	if len(model.Patterns) == 0 {
		t.Fatalf("expected non-empty pattern intents")
	}
	if len(model.WriteActions) != 1 {
		t.Fatalf("expected one write action, got %d", len(model.WriteActions))
	}
	if model.WriteActions[0].ClauseKind != ast.ClauseKindMerge {
		t.Fatalf("expected merge write action, got %#v", model.WriteActions[0])
	}
	if model.WriteActions[0].MergePattern == "" {
		t.Fatalf("expected merge pattern metadata, got %#v", model.WriteActions[0])
	}
	if model.WriteActions[0].Pattern == "" {
		t.Fatalf("expected write pattern metadata, got %#v", model.WriteActions[0])
	}
	if len(model.Projections) != 2 {
		t.Fatalf("expected WITH and RETURN projection intents, got %d", len(model.Projections))
	}
}

func TestBuildQueryStatementSemanticModelWithWithWhere(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) WITH u WHERE u.id = $id RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := Build(stmt)
	if err != nil {
		t.Fatalf("build semantic model failed: %v", err)
	}

	if len(model.Projections) != 2 {
		t.Fatalf("expected two projection intents, got %d", len(model.Projections))
	}
	if model.Projections[0].Kind != ast.ClauseKindWith {
		t.Fatalf("expected first projection to be WITH, got %#v", model.Projections[0])
	}
	if model.Projections[0].WhereExpr != "u.id = $id" {
		t.Fatalf("unexpected WITH where expression: %#v", model.Projections[0])
	}
}

func TestBuildFromParseOutput(t *testing.T) {
	stmt, err := parser.ParseStatement("RETURN 1 AS one")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := BuildFromParseOutput(pipeline.ParseOutput{Statement: stmt})
	if err != nil {
		t.Fatalf("build from parse output failed: %v", err)
	}
	if model.StatementKind == "" {
		t.Fatalf("expected non-empty statement kind")
	}
}

func TestBuildQueryStatementSemanticModelWithUnwindProjectionIntent(t *testing.T) {
	stmt, err := parser.ParseStatement("UNWIND [1,2,3] AS x RETURN x")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	model, err := Build(stmt)
	if err != nil {
		t.Fatalf("build semantic model failed: %v", err)
	}

	if len(model.Projections) != 2 {
		t.Fatalf("expected UNWIND and RETURN projection intents, got %d", len(model.Projections))
	}
	if model.Projections[0].Kind != ast.ClauseKindUnwind {
		t.Fatalf("expected first projection kind UNWIND, got %#v", model.Projections[0])
	}
	if len(model.Projections[0].Items) != 1 {
		t.Fatalf("expected one UNWIND projection item, got %#v", model.Projections[0].Items)
	}
	if model.Projections[0].Items[0].Expression != "[1,2,3]" || model.Projections[0].Items[0].Alias != "x" {
		t.Fatalf("unexpected UNWIND projection item: %#v", model.Projections[0].Items[0])
	}
}
