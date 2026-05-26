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

func TestParseBatchMatchLabelAlternation(t *testing.T) {
	batch, err := ParseBatch("MATCH (n:Movie|Person) RETURN n")
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}
	if len(batch.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(batch.Statements))
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

func TestParseStatementVariableTypeConflictWithMatchNode(t *testing.T) {
	_, err := ParseStatement("WITH 1 AS n MATCH (n) RETURN n")
	if err == nil {
		t.Fatalf("expected variable type conflict parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementOptionalMatchForwardedNodeAllowed(t *testing.T) {
	_, err := ParseStatement("OPTIONAL MATCH (a:Start) WITH a MATCH (a)-->(b) RETURN b")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementOptionalMatchNullableAnchorAllowed(t *testing.T) {
	_, err := ParseStatement("WITH null AS a OPTIONAL MATCH p = (a)-[r]->() RETURN p")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementMatchAfterOptionalCoalesceAliasAllowed(t *testing.T) {
	_, err := ParseStatement("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:NonExistent) OPTIONAL MATCH (a)-->(c:NonExistent) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementVariableLengthRelationshipListBindingAllowed(t *testing.T) {
	_, err := ParseStatement("MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementCreateAlreadyBoundNode(t *testing.T) {
	_, err := ParseStatement("MATCH (a) CREATE (a) RETURN a")
	if err == nil {
		t.Fatalf("expected variable already bound parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementCreateAlreadyBoundNodeWithLabels(t *testing.T) {
	_, err := ParseStatement("CREATE (n:Foo)-[:T1]->(), (n:Bar)-[:T2]->()")
	if err == nil {
		t.Fatalf("expected variable already bound parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementMergeAllowsBoundEndpointWithoutNewPredicates(t *testing.T) {
	_, err := ParseStatement("MATCH (a), (b) MERGE (a)-[:KNOWS]->(b) RETURN a")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementMergeRejectsNewPredicateOnBoundNode(t *testing.T) {
	_, err := ParseStatement("CREATE (a:Foo) MERGE (a)-[r:KNOWS]->(a:Bar)")
	if err == nil {
		t.Fatalf("expected variable already bound parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementMergeRejectsStandaloneAlreadyBoundNode(t *testing.T) {
	_, err := ParseStatement("MATCH (a) MERGE (a) RETURN a")
	if err == nil {
		t.Fatalf("expected variable already bound parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementCreateAllowsBoundEndpointsInRelationshipPattern(t *testing.T) {
	_, err := ParseStatement("CREATE (a), (b) CREATE (a)-[:KNOWS]->(b)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementRejectsSizeOnPatternPredicate(t *testing.T) {
	_, err := ParseStatement("MATCH (a), (b), (c) RETURN size((a)-[:REL]->(b))")
	if err == nil {
		t.Fatalf("expected unexpected syntax parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementAllowsSizeOnPatternComprehension(t *testing.T) {
	_, err := ParseStatement("MATCH (a) RETURN size([(a)-->() | 1]) AS degree")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementRejectsPatternParameterUseAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n$param) RETURN n")
	if err == nil {
		t.Fatalf("expected invalid parameter use parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsReturnStarWithoutScope(t *testing.T) {
	_, err := ParseStatement("RETURN *")
	if err == nil {
		t.Fatalf("expected no variables in scope parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsDuplicateProjectionNamesAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN n AS x, n AS x")
	if err == nil {
		t.Fatalf("expected column name conflict parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsPatternInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN (n)-[]->()")
	if err == nil {
		t.Fatalf("expected unexpected syntax parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsPatternInWithProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WITH (n)-[]->() AS x RETURN x")
	if err == nil {
		t.Fatalf("expected unexpected syntax parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsPatternInSetValueExpression(t *testing.T) {
	_, err := ParseStatement("MATCH (n) SET n.prop = head(nodes(head((n)-[:REL]->()))).foo")
	if err == nil {
		t.Fatalf("expected unexpected syntax parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsUnknownFunctionInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN foo(n)")
	if err == nil {
		t.Fatalf("expected unknown function parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementAllowsKnownFunctionInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN labels(n)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsPassAFunctionSurfaceInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN keys(n), head([1,2]), tail([1,2,3]), abs(-2)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsRemainingFunctionSurfaceInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH p=(a)-[r]->(b) RETURN nodes(p), relationships(p), length(p), startNode(r).id, endNode(r).id, last([1,2]), sign(1)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsPercentileAggregateFunctions(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN percentileDisc(n.price, 0.5), percentileCont(n.price, 0.5)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsDateTimeFromEpochMillis(t *testing.T) {
	_, err := ParseStatement("RETURN datetime.fromepochmillis(237821673987) AS d")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsOrderByParenthesizedExpression(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WITH n ORDER BY (n.id) RETURN count(n)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementRejectsHexIntegerOverflow(t *testing.T) {
	_, err := ParseStatement("RETURN 0x8000000000000000 AS n")
	if err == nil {
		t.Fatalf("expected integer overflow parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsNegativeOctalOverflow(t *testing.T) {
	_, err := ParseStatement("RETURN -0o1000000000000000000001 AS n")
	if err == nil {
		t.Fatalf("expected integer overflow parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsFloatingPointOverflow(t *testing.T) {
	_, err := ParseStatement("RETURN 1e309 AS n")
	if err == nil {
		t.Fatalf("expected floating point overflow parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsLengthOnNodeAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN length(n)")
	if err == nil {
		t.Fatalf("expected compile-time invalid argument type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsLengthOnRelationshipAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH ()-[r]->() RETURN length(r)")
	if err == nil {
		t.Fatalf("expected compile-time invalid argument type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}
