package executor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
)

func TestLimitParamWithAggregateOrderBy(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	// seed: Host nodes connected to Flow nodes
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
