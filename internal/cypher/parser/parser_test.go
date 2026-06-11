package parser

import (
	"errors"
	"strings"
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
	createPattern := strings.ReplaceAll(strings.TrimSpace(stmt.Parts[0].Clauses[0].MatchPattern), " ", "")
	if createPattern != "(n:Person{name:$name})" {
		t.Fatalf("unexpected CREATE pattern metadata: %q", stmt.Parts[0].Clauses[0].MatchPattern)
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

func TestParseBatchProjectionMetadataForWithAndReturnClauses(t *testing.T) {
	query := "MATCH (n:Person) WITH n.name AS name ORDER BY name DESC SKIP 1 LIMIT 2 RETURN name AS value ORDER BY value ASC SKIP 3 LIMIT 4"
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
	if len(clauses) != 3 {
		t.Fatalf("expected 3 clauses, got %d", len(clauses))
	}

	withClause := clauses[1]
	if withClause.Kind != ast.ClauseKindWith {
		t.Fatalf("expected WITH clause, got %s", withClause.Kind)
	}
	if withClause.Projection == nil {
		t.Fatalf("expected WITH projection metadata")
	}
	if withClause.Where != nil {
		t.Fatalf("did not expect WITH WHERE metadata")
	}
	if len(withClause.Projection.OrderBy) != 1 {
		t.Fatalf("expected 1 WITH ORDER BY item, got %d", len(withClause.Projection.OrderBy))
	}
	if withClause.Projection.Skip == nil || strings.TrimSpace(withClause.Projection.Skip.Raw) != "1" {
		t.Fatalf("expected WITH SKIP 1 metadata")
	}
	if withClause.Projection.Limit == nil || strings.TrimSpace(withClause.Projection.Limit.Raw) != "2" {
		t.Fatalf("expected WITH LIMIT 2 metadata")
	}

	returnClause := clauses[2]
	if returnClause.Kind != ast.ClauseKindReturn {
		t.Fatalf("expected RETURN clause, got %s", returnClause.Kind)
	}
	if returnClause.Projection == nil {
		t.Fatalf("expected RETURN projection metadata")
	}
	if returnClause.Where != nil {
		t.Fatalf("did not expect RETURN WHERE metadata")
	}
	if len(returnClause.Projection.OrderBy) != 1 {
		t.Fatalf("expected 1 RETURN ORDER BY item, got %d", len(returnClause.Projection.OrderBy))
	}
	if returnClause.Projection.Skip == nil || strings.TrimSpace(returnClause.Projection.Skip.Raw) != "3" {
		t.Fatalf("expected RETURN SKIP 3 metadata")
	}
	if returnClause.Projection.Limit == nil || strings.TrimSpace(returnClause.Projection.Limit.Raw) != "4" {
		t.Fatalf("expected RETURN LIMIT 4 metadata")
	}
}

func TestParseBatchMatchClauseMetadataForQueryStatement(t *testing.T) {
	batch, err := ParseBatch("WITH 1 AS one OPTIONAL MATCH (n:Person)-[:KNOWS]->(m) WHERE m.name = $name RETURN m")
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}
	stmt, ok := batch.Statements[0].(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", batch.Statements[0])
	}
	if len(stmt.Parts) != 1 || len(stmt.Parts[0].Clauses) < 3 {
		t.Fatalf("expected with, optional match, and return clauses, got %#v", stmt.Parts)
	}
	matchClause := stmt.Parts[0].Clauses[1]
	if matchClause.Kind != ast.ClauseKindOptionalMatch {
		t.Fatalf("expected OPTIONAL_MATCH clause, got %s", matchClause.Kind)
	}
	if !matchClause.MatchOptional {
		t.Fatalf("expected MatchOptional metadata")
	}
	if strings.TrimSpace(matchClause.MatchPattern) != "(n:Person)-[:KNOWS]->(m)" {
		t.Fatalf("unexpected MatchPattern metadata: %q", matchClause.MatchPattern)
	}
	whereRaw := ""
	if matchClause.Where != nil {
		whereRaw = strings.ReplaceAll(strings.TrimSpace(matchClause.Where.Raw), " ", "")
	}
	if whereRaw != "m.name=$name" {
		t.Fatalf("expected WHERE metadata on match clause, got %#v", matchClause.Where)
	}
}

func TestParseBatchMergeClauseMetadataForQueryStatement(t *testing.T) {
	batch, err := ParseBatch("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name='foo' ON MATCH SET r.name='bar' RETURN r")
	if err != nil {
		t.Fatalf("ParseBatch() unexpected error: %v", err)
	}
	stmt, ok := batch.Statements[0].(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", batch.Statements[0])
	}
	if len(stmt.Parts) != 1 || len(stmt.Parts[0].Clauses) < 3 {
		t.Fatalf("expected match, merge, return clauses, got %#v", stmt.Parts)
	}
	mergeClause := stmt.Parts[0].Clauses[1]
	if mergeClause.Kind != ast.ClauseKindMerge {
		t.Fatalf("expected MERGE clause, got %s", mergeClause.Kind)
	}
	if strings.TrimSpace(mergeClause.MergePattern) != "(a)-[r:TYPE]->(b)" {
		t.Fatalf("unexpected merge pattern metadata: %q", mergeClause.MergePattern)
	}
	if strings.TrimSpace(mergeClause.MatchPattern) != "(a)-[r:TYPE]->(b)" {
		t.Fatalf("unexpected merge match-pattern metadata: %q", mergeClause.MatchPattern)
	}
	if strings.TrimSpace(mergeClause.MergeOnCreate) != "r.name='foo'" {
		t.Fatalf("unexpected merge on-create metadata: %q", mergeClause.MergeOnCreate)
	}
	if strings.TrimSpace(mergeClause.MergeOnMatch) != "r.name='bar'" {
		t.Fatalf("unexpected merge on-match metadata: %q", mergeClause.MergeOnMatch)
	}
}

func TestParseStatementMergeClauseMetadataBeforeSetClause(t *testing.T) {
	stmtAny, err := ParseStatement("MERGE (u { id: $id }) SET u.name = $name")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
	stmt, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected *ast.QueryStatement, got %T", stmtAny)
	}
	if len(stmt.Parts) != 1 || len(stmt.Parts[0].Clauses) != 2 {
		t.Fatalf("expected merge+set clauses, got %#v", stmt.Parts)
	}
	mergeClause := stmt.Parts[0].Clauses[0]
	if mergeClause.Kind != ast.ClauseKindMerge {
		t.Fatalf("expected MERGE clause, got %s", mergeClause.Kind)
	}
	if strings.TrimSpace(mergeClause.MergePattern) != "(u { id: $id })" {
		t.Fatalf("unexpected merge pattern metadata: %q", mergeClause.MergePattern)
	}
	if strings.TrimSpace(mergeClause.MergeOnCreate) != "" || strings.TrimSpace(mergeClause.MergeOnMatch) != "" {
		t.Fatalf("expected empty merge action metadata, got onCreate=%q onMatch=%q", mergeClause.MergeOnCreate, mergeClause.MergeOnMatch)
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

func TestParseStatementExplainWrapsInnerStatement(t *testing.T) {
	stmt, err := ParseStatement("EXPLAIN MATCH (n:Person) RETURN n.name AS name")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}

	explain, ok := stmt.(*ast.ExplainStatement)
	if !ok {
		t.Fatalf("expected *ast.ExplainStatement, got %T", stmt)
	}
	if explain.Kind() != ast.StatementKindExplain {
		t.Fatalf("expected EXPLAIN kind, got %s", explain.Kind())
	}
	if strings.TrimSpace(explain.Query) != "MATCH (n:Person) RETURN n.name AS name" {
		t.Fatalf("unexpected explain query payload: %q", explain.Query)
	}
	if explain.Statement == nil {
		t.Fatalf("expected wrapped statement")
	}
	if explain.Statement.Kind() != ast.StatementKindMatchQuery {
		t.Fatalf("expected wrapped MATCH_QUERY statement, got %s", explain.Statement.Kind())
	}
}

func TestParseStatementExplainRequiresInnerQuery(t *testing.T) {
	_, err := ParseStatement("EXPLAIN")
	if err == nil {
		t.Fatalf("expected semantic parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorSemantic {
		t.Fatalf("expected semantic parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementVariableTypeConflictWithMatchVertex(t *testing.T) {
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

func TestParseStatementOptionalMatchForwardedVertexAllowed(t *testing.T) {
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

func TestParseStatementCreateAlreadyBoundVertex(t *testing.T) {
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

func TestParseStatementCreateAlreadyBoundVertexWithLabels(t *testing.T) {
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

func TestParseStatementMergeRejectsNewPredicateOnBoundVertex(t *testing.T) {
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

func TestParseStatementMergeRejectsStandaloneAlreadyBoundVertex(t *testing.T) {
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

func TestParseStatementCreateRejectsUndirectedRelationship(t *testing.T) {
	_, err := ParseStatement("CREATE (a)-[:KNOWS]-(b)")
	if err == nil {
		t.Fatalf("expected requires directed relationship parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "RequiresDirectedRelationship" {
		t.Fatalf("expected RequiresDirectedRelationship unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementCreateRejectsTwoDirectedRelationship(t *testing.T) {
	_, err := ParseStatement("CREATE (a)<-[:KNOWS]->(b)")
	if err == nil {
		t.Fatalf("expected requires directed relationship parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "RequiresDirectedRelationship" {
		t.Fatalf("expected RequiresDirectedRelationship unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementCreateAllowsMixedDirectionDirectedRelationshipChain(t *testing.T) {
	_, err := ParseStatement("CREATE (a)<-[:KNOWS]-(b)-[:FOLLOWS]->(c)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementCreateRejectsRelationshipWithMultipleTypes(t *testing.T) {
	_, err := ParseStatement("CREATE (a)-[:A|B]->(b)")
	if err == nil {
		t.Fatalf("expected no single relationship type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NoSingleRelationshipType" {
		t.Fatalf("expected NoSingleRelationshipType unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementCreateRejectsVariableLengthRelationship(t *testing.T) {
	_, err := ParseStatement("CREATE (a)-[:KNOWS*1..2]->(b)")
	if err == nil {
		t.Fatalf("expected creating var length parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "CreatingVarLength" {
		t.Fatalf("expected CreatingVarLength unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementMergeRejectsRelationshipWithoutType(t *testing.T) {
	_, err := ParseStatement("MERGE (a)-[r]->(b)")
	if err == nil {
		t.Fatalf("expected no single relationship type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NoSingleRelationshipType" {
		t.Fatalf("expected NoSingleRelationshipType unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementMergeRejectsRelationshipWithMultipleTypes(t *testing.T) {
	_, err := ParseStatement("MERGE (a)-[r:A|B]->(b)")
	if err == nil {
		t.Fatalf("expected no single relationship type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NoSingleRelationshipType" {
		t.Fatalf("expected NoSingleRelationshipType unsupported parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
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

func TestParseStatementRejectsSizeOnPathVariable(t *testing.T) {
	_, err := ParseStatement("MATCH p = (a)-[*]->(b) RETURN size(p)")
	if err == nil {
		t.Fatalf("expected invalid argument type parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsAggregationInListComprehensionProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN [x IN [1, 2, 3] | count(*)]")
	if err == nil {
		t.Fatalf("expected invalid aggregation parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
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

func TestParseStatementRejectsNonConstantSkipAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN n SKIP n.count")
	if err == nil {
		t.Fatalf("expected non-constant SKIP parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NonConstantExpression" {
		t.Fatalf("expected NonConstantExpression parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementRejectsNegativeLimitLiteralAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN n LIMIT -1")
	if err == nil {
		t.Fatalf("expected negative LIMIT parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NegativeIntegerArgument" {
		t.Fatalf("expected NegativeIntegerArgument parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementRejectsFloatLimitLiteralAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN n LIMIT 1.5")
	if err == nil {
		t.Fatalf("expected float LIMIT parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementRejectsAnyQuantifierPredicateTypeMismatchAtCompileTime(t *testing.T) {
	_, err := ParseStatement("RETURN any(x IN ['Clara'] WHERE x % 2 = 0) AS result")
	if err == nil {
		t.Fatalf("expected InvalidArgumentType parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementRejectsAllQuantifierPredicateTypeMismatchAtCompileTime(t *testing.T) {
	_, err := ParseStatement("RETURN all(x IN [false, true] WHERE x % 2 = 0) AS result")
	if err == nil {
		t.Fatalf("expected InvalidArgumentType parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
	}
}

func TestParseStatementAllowsConstantSkipLimitExpressions(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WITH n SKIP toInteger(rand()*9) LIMIT toInteger(ceil(1.7)) RETURN count(*) AS count")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
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

func TestParseStatementRejectsUndefinedVariableInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH () RETURN foo")
	if err == nil {
		t.Fatalf("expected undefined variable parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsOrderByVariableRemovedByDistinct(t *testing.T) {
	_, err := ParseStatement("MATCH (a) RETURN DISTINCT a.name ORDER BY a.age")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsOrderByAggregationUsingUnreturnedVariable(t *testing.T) {
	_, err := ParseStatement("MATCH (me:Person)--(you:Person) RETURN count(you.age) AS agg ORDER BY me.age + count(you.age)")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsOrderByAmbiguousAggregationExpression(t *testing.T) {
	_, err := ParseStatement("MATCH (me:Person)--(you:Person) RETURN me.age + you.age, count(*) AS cnt ORDER BY me.age + you.age + count(*)")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "AmbiguousAggregationExpression" {
		t.Fatalf("expected AmbiguousAggregationExpression message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsPropertiesOnIntegerLiteral(t *testing.T) {
	_, err := ParseStatement("RETURN properties(1)")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsPropertiesOnStringLiteral(t *testing.T) {
	_, err := ParseStatement("RETURN properties('Cypher')")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsPropertiesOnListLiteral(t *testing.T) {
	_, err := ParseStatement("RETURN properties([true, false])")
	if err == nil {
		t.Fatalf("expected parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "InvalidArgumentType" {
		t.Fatalf("expected InvalidArgumentType message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsUndefinedVariableInCreatePatternPropertyMap(t *testing.T) {
	_, err := ParseStatement("MATCH (a) CREATE (a)-[:KNOWS]->(b {name: missing}) RETURN b")
	if err == nil {
		t.Fatalf("expected undefined variable parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsUndefinedVariableInMergeOnCreateAction(t *testing.T) {
	_, err := ParseStatement("MERGE (a {id: '1'}) ON CREATE SET missing.name = 'x' RETURN a")
	if err == nil {
		t.Fatalf("expected undefined variable parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsUnaliasedWithExpression(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WITH n.age + 1 RETURN *")
	if err == nil {
		t.Fatalf("expected no expression alias parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementAllowsOrderByIncomingScopeVariableInWith(t *testing.T) {
	_, err := ParseStatement("MATCH (a)-[:KNOWS]->(b) WITH a ORDER BY b RETURN a")
	if err != nil {
		t.Fatalf("expected query to parse, got error: %v", err)
	}
}

func TestParseStatementRejectsUndefinedOrderByVariableNeverDefined(t *testing.T) {
	_, err := ParseStatement("MATCH (a) RETURN a ORDER BY d")
	if err == nil {
		t.Fatalf("expected undefined variable parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementRejectsMultipleUndefinedOrderByVariables(t *testing.T) {
	_, err := ParseStatement("MATCH (a) RETURN a ORDER BY a.id, c, d")
	if err == nil {
		t.Fatalf("expected undefined variable parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
	if parseErr.Message != "UndefinedVariable" {
		t.Fatalf("expected UndefinedVariable message, got %q", parseErr.Message)
	}
}

func TestParseStatementAllowsReturnOrderByNonProjectedBoundVariable(t *testing.T) {
	_, err := ParseStatement("MATCH (a) RETURN a.id AS id ORDER BY a")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsProjectedPropertyAccessWithAggregation(t *testing.T) {
	_, err := ParseStatement("MATCH (me:Person)--(you:Person) WITH me.age AS age, me.age + count(you.age) AS agg RETURN *")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementRejectsAmbiguousAggregationWithNoGroupingProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (me:Person)--(you:Person) WITH me.age + count(you.age) AS agg RETURN *")
	if err == nil {
		t.Fatalf("expected ambiguous aggregation parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsAmbiguousAggregationWithComplexGroupingProjection(t *testing.T) {
	_, err := ParseStatement("MATCH (me:Person)--(you:Person) WITH me.age + you.age AS grp, me.age + you.age + count(*) AS agg RETURN *")
	if err == nil {
		t.Fatalf("expected ambiguous aggregation parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsNestedAggregationAtCompileTime(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN count(count(*))")
	if err == nil {
		t.Fatalf("expected nested aggregation parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported {
		t.Fatalf("expected unsupported parse error kind, got %s", parseErr.Kind)
	}
}

func TestParseStatementRejectsNonConstantRandInsideAggregationAtCompileTime(t *testing.T) {
	_, err := ParseStatement("RETURN count(rand())")
	if err == nil {
		t.Fatalf("expected non-constant expression parse error")
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ParseError, got %T", err)
	}
	if parseErr.Kind != ParseErrorUnsupported || parseErr.Message != "NonConstantExpression" {
		t.Fatalf("expected NonConstantExpression parse error, got kind=%s message=%q", parseErr.Kind, parseErr.Message)
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
	_, err := ParseStatement("MATCH (n) SET n.prop = head(vertexes(head((n)-[:REL]->()))).foo")
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

func TestParseStatementRejectsStandaloneVertexPatternInWhere(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WHERE (n) RETURN n")
	if err == nil {
		t.Fatalf("expected invalid argument type parse error")
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
	_, err := ParseStatement("MATCH (n) RETURN keys(n), head([1,2]), tail([1,2,3]), abs(-2), sqrt(12.96)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsRemainingFunctionSurfaceInReturnProjection(t *testing.T) {
	_, err := ParseStatement("MATCH p=(a)-[r]->(b) RETURN vertexes(p), nOdEs(p), relationships(p), length(p), startVertex(r).id, startNode(r).id, endVertex(r).id, endNode(r).id, last([1,2]), sign(1)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsAdditionalBuiltInFunctionSurfaceInReturnProjection(t *testing.T) {
	_, err := ParseStatement("RETURN lower('A'), upper('a'), ceiling(1.2), left('abc',1), right('abc',1), replace('a','a','b'), trim(' x '), ltrim(' x '), rtrim(' x '), char_length('x'), character_length('x'), path_length(1), isEmpty([]), nullIf(1,2), toStringOrNull(1), toIntegerOrNull('x'), toFloatOrNull('x'), toBooleanOrNull('x')")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsMathematicalBuiltInFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN floor(1.2), round(1.2, 1), exp(1), log(1), ln(1), log10(10), e(), pi(), sin(1), cos(1), tan(1), asin(1), acos(1), atan(1), atan2(1, 1), degrees(1), radians(1), cot(1), haversin(1), isNaN(0/0)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsPredicateScalarBuiltInFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN exists(1), elementId({id:'1'}), id({id:'1'}), valueType(1), randomUUID(), timestamp()")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsExpandedBuiltInFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN reduce(total = 0, n IN [1,2,3] | total + n), toBooleanList(['true','x']), toIntegerList(['1','x']), toFloatList(['1.5','x']), toStringList([1,true,{a:1}]), btrim('__x__', '_'), normalize('e\u0301', 'NFC'), zoned_datetime('2024-01-01T00:00:00Z'), local_datetime('2024-01-01T00:00:00'), local_time('12:34:56'), zoned_time('12:34:56Z'), duration_between(datetime('2024-01-01T00:00:00Z'), datetime('2024-01-02T00:00:00Z'))")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsSpatialPointFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN point({x: 1, y: 2}), point({longitude: 12.3, latitude: 45.6})")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsSpatialDistanceFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN distance(point({x: 0, y: 0}), point({x: 3, y: 4}))")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsSpatialNamespaceFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN point.distance(point({x: 0, y: 0}), point({x: 3, y: 4})), point.withinBBox(point({x: 5, y: 5}), point({x: 0, y: 0}), point({x: 10, y: 10}))")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsVectorFunctionSurface(t *testing.T) {
	_, err := ParseStatement("RETURN vector([1.0, 2.0], 2, FLOAT32), vector.similarity.cosine([1.0,0.0], [0.0,1.0]), vector.similarity.euclidean([1.0,0.0], [1.0,0.0]), vector_dimension_count(vector([1.0,2.0], 2, FLOAT32)), vector_distance([1.0,2.0], [2.0,4.0], EUCLIDEAN), vector_norm([1.0,2.0], MANHATTAN)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementAllowsAggregateAliasBuiltIns(t *testing.T) {
	_, err := ParseStatement("MATCH (n) RETURN stDev(n.score), stDevP(n.score), stdev_samp(n.score), stdev_pop(n.score), collect_list(n.score), percentile_cont(n.score, 0.5), percentile_disc(n.score, 0.5)")
	if err != nil {
		t.Fatalf("ParseStatement() unexpected error: %v", err)
	}
}

func TestParseStatementPreservesExistsSubqueryGrammar(t *testing.T) {
	_, err := ParseStatement("MATCH (n) WHERE EXISTS { (n)-->() } RETURN n")
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

func TestParseStatementRejectsLengthOnVertexAtCompileTime(t *testing.T) {
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
