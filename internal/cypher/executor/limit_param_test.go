package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

func TestLimitParamWithAggregateOrderBy(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	// seed: Host vertexes connected to Flow vertexes
	setup := []string{
		"CREATE (src:Host {ip: '10.0.0.1'})-[:SENT]->(f:Flow {threat_score: '1.5', detected_malicious: 'true'})",
		"CREATE (src:Host {ip: '10.0.0.1'})-[:SENT]->(f:Flow {threat_score: '1.2', detected_malicious: 'true'})",
		"CREATE (src:Host {ip: '10.0.0.2'})-[:SENT]->(f:Flow {threat_score: '0.8', detected_malicious: 'true'})",
	}
	exec := New(store, Options{})
	for _, q := range setup {
		s, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		_, err = exec.ExecuteStatement(ctx, s, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}

	huntQuery := `
MATCH (src:Host)-[:SENT]->(f:Flow)
WHERE f.detected_malicious = true
RETURN src.ip AS source_ip,
       count(f) AS suspicious_flows,
       avg(f.threat_score) AS avg_score,
       max(f.threat_score) AS max_score
ORDER BY suspicious_flows DESC, avg_score DESC
LIMIT $limit_value
`
	stmt, err := parser.ParseStatement(huntQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":      "acme",
		"limit_value": 5,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected at least one row")
	}
	t.Logf("rows: %+v", res.Rows)
}

func TestWhereThresholdParam(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})

	// seed flows with different threat_score values
	setup := []string{
		"CREATE (:Flow {threat_score: 1.5, detected_malicious: true})",
		"CREATE (:Flow {threat_score: 0.8, detected_malicious: false})",
		"CREATE (:Flow {threat_score: 1.1, detected_malicious: true})",
	}
	for _, q := range setup {
		s, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		_, err = exec.ExecuteStatement(ctx, s, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}

	thresholdQuery := `MATCH (f:Flow) WHERE f.threat_score >= $threshold RETURN count(f) AS ct`
	stmt, err := parser.ParseStatement(thresholdQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":    "acme",
		"threshold": 1.0,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	t.Logf("threshold filter result: %+v", res.Rows)
	if len(res.Rows) == 0 {
		t.Fatal("expected result row")
	}
	ct, _ := res.Rows[0]["ct"]
	t.Logf("count with threshold >= 1.0: %v", ct)
}

func TestEvalExpressionWithScopeParameterLiteral(t *testing.T) {
	value, err := evalExpressionWithScope("$threshold", Row{}, Params{"threshold": 1.0})
	if err != nil {
		t.Fatalf("evalExpressionWithScope failed: %v", err)
	}
	if value == nil {
		t.Fatal("expected parameter value, got nil")
	}
}

func TestEvalExpressionWithScopePropertyAccess(t *testing.T) {
	row := Row{"f": map[string]any{"threat_score": json.Number("1.5")}}
	value, err := evalExpressionWithScope("f.threat_score", row, Params{})
	if err != nil {
		t.Fatalf("evalExpressionWithScope failed: %v", err)
	}
	if value == nil {
		t.Fatal("expected property value, got nil")
	}
}

func TestEvalWhereExpressionThresholdComparison(t *testing.T) {
	exec := New(nil, Options{})
	row := Row{"f": map[string]any{"threat_score": json.Number("1.5")}}
	ok, err := exec.evalWhereExpression(context.Background(), nil, "f.threat_score >= $threshold", row, Params{"threshold": 1.0})
	if err != nil {
		t.Fatalf("evalWhereExpression failed: %v", err)
	}
	if !ok {
		t.Fatal("expected threshold comparison to evaluate true")
	}
}

func TestEvalExpressionWithScopeThresholdComparison(t *testing.T) {
	row := Row{"f": map[string]any{"threat_score": json.Number("1.5")}}
	value, err := evalExpressionWithScope("f.threat_score >= $threshold", row, Params{"threshold": 1.0})
	if err != nil {
		t.Fatalf("evalExpressionWithScope failed: %v", err)
	}
	b, ok := value.(bool)
	if !ok {
		t.Fatalf("expected bool result, got %T (%v)", value, value)
	}
	if !b {
		t.Fatal("expected threshold comparison to evaluate true")
	}
}

func TestSplitTopLevelOperatorThresholdComparison(t *testing.T) {
	left, right, ok := splitTopLevelOperator("f.threat_score >= $threshold", ">=")
	if !ok {
		t.Fatal("expected splitTopLevelOperator to split >= expression")
	}
	if left != "f.threat_score" || right != "$threshold" {
		t.Fatalf("unexpected split: left=%q right=%q", left, right)
	}
}

func TestSplitTopLevelKeywordDoesNotSplitIdentifierSubstring(t *testing.T) {
	_, _, ok := splitTopLevelKeyword("f.threat_score >= $threshold", "OR")
	if ok {
		t.Fatal("expected splitTopLevelKeyword to ignore lowercase 'or' inside threat_score")
	}
}

func TestEvalExpressionWithScopePrefersFieldAccessOverDottedBinding(t *testing.T) {
	row := Row{
		"a":    map[string]any{"id": json.Number("0")},
		"a.id": "synthetic-id",
	}
	value, err := evalExpressionWithScope("a.id", row, Params{})
	if err != nil {
		t.Fatalf("evalExpressionWithScope failed: %v", err)
	}
	if got := fmt.Sprint(value); got != "0" {
		t.Fatalf("expected semantic field access value 0, got %#v", value)
	}
}

func TestEvalWhereExpressionDisjunctivePatternPredicateDirect(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}, {Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}, {Tenant: "acme", ID: "c2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a0", DstID: "b1"}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "T", SrcID: "b1", DstID: "c2"}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	if err := store.View(ctx, func(tx graph.Tx) error {
		ok, err := exec.evalWhereExpression(ctx, tx, "a.id = 0 AND (a)-[:T]->(b:TheLabel) OR (a)-[:T*]->(b:MissingLabel)", Row{"a": map[string]any{"id": json.Number("0")}, "b": map[string]any{"id": json.Number("1")}}, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("evalWhereExpression failed: %v", err)
		}
		if !ok {
			t.Fatal("expected where expression to evaluate true")
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestExecuteStatementDisjunctivePatternPredicateJoin(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "a0", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("0")}}, {Tenant: "acme", ID: "b1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("1")}}, {Tenant: "acme", ID: "c2", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("2")}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a0", DstID: "b1"}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "T", SrcID: "b1", DstID: "c2"}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement(`
MATCH (a), (b)
WHERE a.id = 0
  AND (a)-[:T]->(b:TheLabel)
  OR (a)-[:T*]->(b:MissingLabel)
RETURN DISTINCT b
`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %#v", res.Rows)
	}
}

func TestExecuteStatementDisjunctivePatternPredicateAfterTwoCreates(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup, err := parser.ParseStatement(`
CREATE (a:TheLabel{id:0}), (b:TheLabel{id:1}), (c:TheLabel{id:2})
CREATE (a)-[:T]->(b), (b)-[:T]->(c)
`)
	if err != nil {
		t.Fatalf("parse setup failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute setup failed: %v", err)
	}

	stmt, err := parser.ParseStatement(`
MATCH (a), (b)
WHERE a.id = 0
  AND (a)-[:T]->(b:TheLabel)
  OR (a)-[:T*]->(b:MissingLabel)
RETURN DISTINCT b
`)
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after two CREATE clauses, got %#v", res.Rows)
	}
}

func TestExecuteStatementWithWhereDisjunctivePatternPredicateAfterTwoCreates(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup, err := parser.ParseStatement(`
CREATE (a:TheLabel {id: 0}), (b:TheLabel {id: 1}), (c:TheLabel {id: 2})
CREATE (a)-[:T]->(b),
       (b)-[:T]->(c)
`)
	if err != nil {
		t.Fatalf("parse setup failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute setup failed: %v", err)
	}

	stmt, err := parser.ParseStatement(`
MATCH (a), (b)
WITH a, b
WHERE a.id = 0
  AND (a)-[:T]->(b:TheLabel)
  OR (a)-[:T*]->(b:MissingLabel)
RETURN DISTINCT b
`)
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row for WITH-WHERE disjunction, got %#v", res.Rows)
	}
}
