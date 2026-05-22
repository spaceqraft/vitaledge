package parser

import (
	"errors"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

func TestParseBatchMatchWhereReturn(t *testing.T) {
	query := "match (n:Person {name: $name}) where n.age > $minAge return distinct n.name as personName, n.age order by n.age desc skip 1 limit $maxLimit"

	batch, err := ParseBatch(query)
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}
	if len(batch.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Statements))
	}

	stmt, ok := batch.Statements[0].(*ast.MatchQueryStatement)
	if !ok {
		t.Fatalf("expected *ast.MatchQueryStatement, got %T", batch.Statements[0])
	}

	if stmt.Kind() != ast.StatementKindMatchQuery {
		t.Fatalf("unexpected statement kind: %s", stmt.Kind())
	}
	if len(stmt.MatchClauses) != 1 {
		t.Fatalf("expected 1 MATCH clause, got %d", len(stmt.MatchClauses))
	}
	if stmt.MatchClauses[0].Where == nil {
		t.Fatalf("expected WHERE expression")
	}
	if !stmt.Return.Distinct {
		t.Fatalf("expected DISTINCT to be true")
	}
	if len(stmt.Return.Items) != 2 {
		t.Fatalf("expected 2 RETURN items, got %d", len(stmt.Return.Items))
	}
	if len(stmt.Return.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(stmt.Return.OrderBy))
	}
	if stmt.Return.OrderBy[0].Direction != ast.SortDirectionDesc {
		t.Fatalf("expected DESC direction, got %s", stmt.Return.OrderBy[0].Direction)
	}
	if stmt.Return.Skip == nil || stmt.Return.Limit == nil {
		t.Fatalf("expected both SKIP and LIMIT expressions")
	}

	gotParams := make(map[string]bool)
	for _, p := range stmt.Parameters {
		gotParams[p.Name] = true
	}
	for _, expected := range []string{"name", "minAge", "maxLimit"} {
		if !gotParams[expected] {
			t.Fatalf("missing parameter %q in statement parameters", expected)
		}
	}
}

func TestParseBatchSemicolonSeparated(t *testing.T) {
	query := "MATCH (n) RETURN n; MATCH (m) RETURN m;"
	batch, err := ParseBatch(query)
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}
	if len(batch.Statements) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(batch.Statements))
	}
}

func TestParseBatchCreateReturnSupported(t *testing.T) {
	batch, err := ParseBatch("CREATE (n:Person {name: $name}) RETURN n")
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}

	stmt, ok := batch.Statements[0].(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", batch.Statements[0])
	}
	if len(stmt.Parts) != 1 {
		t.Fatalf("expected 1 query part, got %d", len(stmt.Parts))
	}
	if len(stmt.Parts[0].Clauses) != 2 {
		t.Fatalf("expected 2 clauses, got %d", len(stmt.Parts[0].Clauses))
	}
	if stmt.Parts[0].Clauses[0].Kind != ast.ClauseKindCreate {
		t.Fatalf("expected CREATE clause, got %s", stmt.Parts[0].Clauses[0].Kind)
	}
	if stmt.Parts[0].Clauses[1].Kind != ast.ClauseKindReturn {
		t.Fatalf("expected RETURN clause, got %s", stmt.Parts[0].Clauses[1].Kind)
	}
}

func TestParseBatchSyntaxError(t *testing.T) {
	_, err := ParseBatch("MATCH (n RETURN n")
	if err == nil {
		t.Fatalf("expected syntax error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorSyntax {
		t.Fatalf("expected syntax parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseBatchMultiPartQuerySupported(t *testing.T) {
	query := "MATCH (n:Person) WITH n CREATE (m:Mirror {name: n.name}) RETURN m"
	batch, err := ParseBatch(query)
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}

	stmt, ok := batch.Statements[0].(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", batch.Statements[0])
	}

	if len(stmt.Parts) != 1 {
		t.Fatalf("expected 1 query part, got %d", len(stmt.Parts))
	}

	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 4 {
		t.Fatalf("expected at least 4 clauses, got %d", len(clauses))
	}
	if clauses[0].Kind != ast.ClauseKindMatch {
		t.Fatalf("expected first clause MATCH, got %s", clauses[0].Kind)
	}
	if clauses[1].Kind != ast.ClauseKindWith {
		t.Fatalf("expected second clause WITH, got %s", clauses[1].Kind)
	}
	if clauses[2].Kind != ast.ClauseKindCreate {
		t.Fatalf("expected third clause CREATE, got %s", clauses[2].Kind)
	}
	if clauses[3].Kind != ast.ClauseKindReturn {
		t.Fatalf("expected fourth clause RETURN, got %s", clauses[3].Kind)
	}
}

func TestParseBatchUnionSupported(t *testing.T) {
	batch, err := ParseBatch("MATCH (n:Person) RETURN n UNION ALL MATCH (m:Movie) RETURN m")
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}

	stmt, ok := batch.Statements[0].(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", batch.Statements[0])
	}

	if len(stmt.Unions) != 1 || stmt.Unions[0] != ast.UnionKindAll {
		t.Fatalf("expected one UNION ALL boundary, got %#v", stmt.Unions)
	}
	if len(stmt.Parts) != 2 {
		t.Fatalf("expected 2 query parts, got %d", len(stmt.Parts))
	}
}

func TestParseStatementStandaloneCallSupported(t *testing.T) {
	stmt, err := ParseStatement("CALL db.labels() YIELD label")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}

	call, ok := stmt.(*ast.StandaloneCallStatement)
	if !ok {
		t.Fatalf("expected *ast.StandaloneCallStatement, got %T", stmt)
	}
	if call.Call.Kind != ast.ClauseKindStandaloneCall {
		t.Fatalf("expected STANDALONE_CALL clause kind, got %s", call.Call.Kind)
	}
}
