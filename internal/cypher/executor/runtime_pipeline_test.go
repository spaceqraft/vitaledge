package executor

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

func TestExecuteStatementRuntimePipelineCreateEdgeWriteOnly(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u10",
		"dst":                       "u11",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime pipeline query, got %#v", res.Rows)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u10")
		if err != nil {
			return err
		}
		if left == nil {
			t.Fatalf("expected left vertex to be written")
		}
		right, err := tx.GetVertex(ctx, "acme", "u11")
		if err != nil {
			return err
		}
		if right == nil {
			t.Fatalf("expected right vertex to be written")
		}
		edge, err := tx.GetEdge(ctx, "acme", "u10|KNOWS|u11")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u10" || edge.DstID != "u11" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateReverseEdgeWriteOnly(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u12",
		"dst":                       "u13",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no result rows for write-only runtime pipeline query, got %#v", res.Rows)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u12")
		if err != nil {
			return err
		}
		if left == nil {
			t.Fatalf("expected left vertex to be written")
		}
		right, err := tx.GetVertex(ctx, "acme", "u13")
		if err != nil {
			return err
		}
		if right == nil {
			t.Fatalf("expected right vertex to be written")
		}
		edge, err := tx.GetEdge(ctx, "acme", "u13|KNOWS|u12")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u13" || edge.DstID != "u12" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected reverse edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u20",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u20" {
		t.Fatalf("expected projected uid value u20 in runtime row, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		vertex, err := tx.GetVertex(ctx, "acme", "u20")
		if err != nil {
			return err
		}
		if vertex == nil {
			t.Fatalf("expected vertex to be written")
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateEdgeWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u30",
		"dst":                       "u31",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u30" {
		t.Fatalf("expected projected uid value u30, got %#v", res.Rows[0])
	}
	if got := res.Rows[0]["vid"]; got != "u31" {
		t.Fatalf("expected projected vid value u31, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "u30|KNOWS|u31")
		if err != nil {
			return err
		}
		if edge == nil {
			t.Fatalf("expected edge to be written")
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateReverseEdgeWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u32",
		"dst":                       "u33",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime return row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u32" {
		t.Fatalf("expected projected uid value u32, got %#v", res.Rows[0])
	}
	if got := res.Rows[0]["vid"]; got != "u33" {
		t.Fatalf("expected projected vid value u33, got %#v", res.Rows[0])
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "u33|KNOWS|u32")
		if err != nil {
			return err
		}
		if edge == nil || edge.SrcID != "u33" || edge.DstID != "u32" || edge.Type != "KNOWS" {
			t.Fatalf("unexpected reverse edge written: %#v", edge)
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturnDistinct(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN DISTINCT u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u40",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime distinct row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u40" {
		t.Fatalf("expected projected uid value u40, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithReturnOrderLimit(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid DESC SKIP 0 LIMIT 1")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u41",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime ordered row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["uid"]; got != "u41" {
		t.Fatalf("expected projected uid value u41, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithDistinctWithReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH DISTINCT u.id AS uid RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u51",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime DISTINCT WITH row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u51" {
		t.Fatalf("expected projected out value u51, got %#v", res.Rows[0])
	}
}

func TestSupportsRuntimePipelineQueryAcceptsWithWhereMatrix(t *testing.T) {
	for _, tc := range acceptedRuntimeWhereGuardCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if !supportsRuntimePipelineQuery(query) {
				t.Fatalf("expected accepted WHERE matrix query to be supported for guarded runtime path: %q", tc.query)
			}
		})
	}
}

func TestSupportsRuntimePipelineQueryRejectsWithWhereMatrix(t *testing.T) {
	for _, tc := range rejectedRuntimeWhereGuardCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if supportsRuntimePipelineQuery(query) {
				t.Fatalf("expected rejected WHERE matrix query to be rejected for guarded runtime path: %q", tc.query)
			}
		})
	}
}

func TestSupportsRuntimePipelineQueryRejectsParenthesizedReturnProjection(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN (u.id) AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected parenthesized RETURN projection shape to be rejected for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryRejectsParenthesizedWithProjection(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH (u.id) AS uid RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected parenthesized WITH projection shape to be rejected for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryRejectsParenthesizedOrderByExpression(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (uid) DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected parenthesized identifier ORDER BY expression shape to be rejected for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryAcceptsParenthesizedConstantOrderByExpression(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (1) DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if !supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected parenthesized constant ORDER BY expression shape to be supported for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryAcceptsDottedOrderByExpression(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if !supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected dotted ORDER BY expression shape to be supported for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryRejectsArithmeticOrderByExpression(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid + 'x' DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected arithmetic ORDER BY expression shape to be rejected for guarded runtime path")
	}
}

func TestSupportsRuntimePipelineQueryRejectsFunctionCallOrderByExpression(t *testing.T) {
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY toUpper(uid) DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}
	if supportsRuntimePipelineQuery(query) {
		t.Fatalf("expected function-call ORDER BY expression shape to be rejected for guarded runtime path")
	}
}

type runtimeAcceptedGuardCase struct {
	name  string
	query string
}

func acceptedRuntimeProjectionGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "return identifier", query: "CREATE (u:User {id:$src}) RETURN u AS out"},
		{name: "return dotted identifier", query: "CREATE (u:User {id:$src}) RETURN u.id AS out"},
		{name: "return parameter", query: "CREATE (u:User {id:$src}) RETURN $src AS out"},
		{name: "return null literal", query: "CREATE (u:User {id:$src}) RETURN null AS out"},
		{name: "return true literal", query: "CREATE (u:User {id:$src}) RETURN true AS out"},
		{name: "return false literal", query: "CREATE (u:User {id:$src}) RETURN false AS out"},
		{name: "return integer literal", query: "CREATE (u:User {id:$src}) RETURN 42 AS out"},
		{name: "return float literal", query: "CREATE (u:User {id:$src}) RETURN 3.5 AS out"},
		{name: "return quoted string literal", query: "CREATE (u:User {id:$src}) RETURN 'alpha' AS out"},
		{name: "return include all", query: "CREATE (u:User {id:$src}) RETURN *"},
		{name: "with parameter", query: "CREATE (u:User {id:$src}) WITH $src AS uid RETURN uid AS out"},
		{name: "with null literal", query: "CREATE (u:User {id:$src}) WITH null AS uid RETURN uid AS out"},
		{name: "with boolean literal", query: "CREATE (u:User {id:$src}) WITH true AS uid RETURN uid AS out"},
		{name: "with numeric literal", query: "CREATE (u:User {id:$src}) WITH 7 AS uid RETURN uid AS out"},
		{name: "with quoted literal", query: "CREATE (u:User {id:$src}) WITH 'beta' AS uid RETURN uid AS out"},
	}
}

func acceptedRuntimeOrderByGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "accept alias identifier", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid DESC"},
		{name: "accept dotted identifier", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC"},
		{name: "accept null keyword", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY null DESC"},
		{name: "accept true keyword", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY true DESC"},
		{name: "accept numeric literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY 1 DESC"},
		{name: "accept quoted literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY 'x' DESC"},
		{name: "accept parameter", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY $src DESC"},
		{name: "accept parenthesized null keyword", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (null) DESC"},
		{name: "accept parenthesized numeric literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (1) DESC"},
		{name: "accept parenthesized quoted literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ('x') DESC"},
		{name: "accept parenthesized parameter", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ($src) DESC"},
		{name: "accept nested parenthesized null keyword", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((null)) DESC"},
		{name: "accept nested parenthesized numeric literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((1)) DESC"},
		{name: "accept nested parenthesized quoted literal", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (('x')) DESC"},
		{name: "accept nested parenthesized parameter", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (($src)) DESC"},
	}
}

func rejectedRuntimeOrderByGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "reject arithmetic", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid + 'x' DESC"},
		{name: "reject nested parenthesized computed", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((uid + 'x')) DESC"},
		{name: "reject function call", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY toUpper(uid) DESC"},
		{name: "reject nested parenthesized function call", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((toUpper(uid))) DESC"},
		{name: "reject parenthesized identifier", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY (uid) DESC"},
		{name: "reject nested parenthesized identifier", query: "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((uid)) DESC"},
	}
}

func rejectedRuntimeProjectionGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "reject arithmetic return projection", query: "CREATE (u:User {id:$src}) RETURN u.id + 'x' AS out"},
		{name: "reject function-call return projection", query: "CREATE (u:User {id:$src}) RETURN toUpper(u.id) AS out"},
		{name: "reject arithmetic with projection", query: "CREATE (u:User {id:$src}) WITH u.id + 'x' AS uid RETURN uid AS out"},
		{name: "reject function-call with projection", query: "CREATE (u:User {id:$src}) WITH toUpper(u.id) AS uid RETURN uid AS out"},
	}
}

func acceptedRuntimeWhereGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "accept simple with where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out"},
		{name: "accept starts with where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH 'u' RETURN uid AS out"},
		{name: "accept is not null where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out"},
		{name: "accept simple or where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src OR uid = 'x' RETURN uid AS out"},
		{name: "accept parenthesized where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid = $src) RETURN uid AS out"},
		{name: "accept parenthesized or-group where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid = $src OR uid = 'x') OR uid = 'y' RETURN uid AS out"},
	}
}

func rejectedRuntimeWhereGuardCases() []runtimeAcceptedGuardCase {
	return []runtimeAcceptedGuardCase{
		{name: "reject string predicate numeric literal", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH 10 RETURN uid AS out"},
		{name: "reject where identifier rhs", query: "CREATE (u:User {id:$src}) WITH u.id AS uid, u.id AS other WHERE uid = other RETURN uid AS out"},
		{name: "reject string predicate identifier rhs", query: "CREATE (u:User {id:$src}) WITH u.id AS uid, 'u' AS prefix WHERE uid STARTS WITH prefix RETURN uid AS out"},
		{name: "reject mixed boolean where", query: "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid = 'u80' OR uid = 'u81' RETURN uid AS out"},
	}
}

type acceptedRuntimeExecutionCase struct {
	name      string
	query     string
	params    Params
	wantKey   string
	wantValue any
}

func acceptedRuntimeExecutionCases() []acceptedRuntimeExecutionCase {
	return []acceptedRuntimeExecutionCase{
		{
			name:      "simple return projection",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out",
			params:    Params{"tenant": "acme", "src": "u500"},
			wantKey:   "out",
			wantValue: "u500",
		},
		{
			name:      "simple with return projection",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u507"},
			wantKey:   "out",
			wantValue: "u507",
		},
		{
			name:      "simple with where",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u501"},
			wantKey:   "out",
			wantValue: "u501",
		},
		{
			name:      "dotted order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC",
			params:    Params{"tenant": "acme", "src": "u502"},
			wantKey:   "uid",
			wantValue: "u502",
		},
		{
			name:      "numeric literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY 1 DESC",
			params:    Params{"tenant": "acme", "src": "u503"},
			wantKey:   "out",
			wantValue: "u503",
		},
		{
			name:      "quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY 'x' DESC",
			params:    Params{"tenant": "acme", "src": "u504"},
			wantKey:   "out",
			wantValue: "u504",
		},
		{
			name:      "parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY $sort DESC",
			params:    Params{"tenant": "acme", "src": "u505", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u505",
		},
		{
			name:      "nested parenthesized constant order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY ((1)) DESC",
			params:    Params{"tenant": "acme", "src": "u506"},
			wantKey:   "uid",
			wantValue: "u506",
		},
		{
			name:      "parenthesized null keyword order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (null) DESC",
			params:    Params{"tenant": "acme", "src": "u507"},
			wantKey:   "out",
			wantValue: "u507",
		},
		{
			name:      "parenthesized numeric literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (1) DESC",
			params:    Params{"tenant": "acme", "src": "u508"},
			wantKey:   "out",
			wantValue: "u508",
		},
		{
			name:      "parenthesized quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ('x') DESC",
			params:    Params{"tenant": "acme", "src": "u509"},
			wantKey:   "out",
			wantValue: "u509",
		},
		{
			name:      "parenthesized parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ($sort) DESC",
			params:    Params{"tenant": "acme", "src": "u510", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u510",
		},
		{
			name:      "nested parenthesized null keyword order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ((null)) DESC",
			params:    Params{"tenant": "acme", "src": "u511"},
			wantKey:   "out",
			wantValue: "u511",
		},
		{
			name:      "nested parenthesized quoted literal order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (('x')) DESC",
			params:    Params{"tenant": "acme", "src": "u512"},
			wantKey:   "out",
			wantValue: "u512",
		},
		{
			name:      "nested parenthesized parameter order by",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY (($sort)) DESC",
			params:    Params{"tenant": "acme", "src": "u513", "sort": "constant"},
			wantKey:   "out",
			wantValue: "u513",
		},
	}
}

func TestSupportsRuntimePipelineQueryOrderByExpressionMatrix(t *testing.T) {
	type orderByCase struct {
		name   string
		query  string
		accept bool
	}
	tests := []orderByCase{}
	for _, tc := range acceptedRuntimeOrderByGuardCases() {
		tests = append(tests, orderByCase{name: tc.name, query: tc.query, accept: true})
	}
	for _, tc := range rejectedRuntimeOrderByGuardCases() {
		tests = append(tests, orderByCase{name: tc.name, query: tc.query, accept: false})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			got := supportsRuntimePipelineQuery(query)
			if got != tc.accept {
				t.Fatalf("unexpected ORDER BY guard result for %q: got=%v want=%v", tc.query, got, tc.accept)
			}
		})
	}
}

func TestRuntimeProjectionAndOrderHelperMatrix(t *testing.T) {
	tests := []struct {
		name                    string
		expr                    string
		wantProjectionAllowStar bool
		wantProjectionNoStar    bool
		wantOrderIdentifierPath bool
		wantOrderByExpr         bool
	}{
		{name: "identifier", expr: "uid", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: true, wantOrderByExpr: true},
		{name: "dotted identifier", expr: "u.id", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: true, wantOrderByExpr: true},
		{name: "trimmed dotted identifier", expr: " u.id ", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: true, wantOrderByExpr: true},
		{name: "star projection", expr: "*", wantProjectionAllowStar: true, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "parameter", expr: "$src", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "null literal", expr: "null", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: true, wantOrderByExpr: true},
		{name: "boolean literal", expr: "true", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: true, wantOrderByExpr: true},
		{name: "numeric literal", expr: "42", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "quoted literal", expr: "'alpha'", wantProjectionAllowStar: true, wantProjectionNoStar: true, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "parenthesized numeric literal", expr: "(42)", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "parenthesized quoted literal", expr: "('alpha')", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "parenthesized parameter", expr: "($src)", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "nested parenthesized numeric literal", expr: "((42))", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "nested parenthesized quoted literal", expr: "(('alpha'))", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "nested parenthesized parameter", expr: "(($src))", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: true},
		{name: "computed arithmetic", expr: "uid + 'x'", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "function call", expr: "toUpper(uid)", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "nested parenthesized function call", expr: "((toUpper(uid)))", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "parenthesized identifier", expr: "(uid)", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "nested parenthesized identifier", expr: "((uid))", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
		{name: "invalid path", expr: "u..id", wantProjectionAllowStar: false, wantProjectionNoStar: false, wantOrderIdentifierPath: false, wantOrderByExpr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSimpleRuntimeProjectionExpr(tc.expr, true); got != tc.wantProjectionAllowStar {
				t.Fatalf("unexpected isSimpleRuntimeProjectionExpr(expr, true) for %q: got=%v want=%v", tc.expr, got, tc.wantProjectionAllowStar)
			}
			if got := isSimpleRuntimeProjectionExpr(tc.expr, false); got != tc.wantProjectionNoStar {
				t.Fatalf("unexpected isSimpleRuntimeProjectionExpr(expr, false) for %q: got=%v want=%v", tc.expr, got, tc.wantProjectionNoStar)
			}
			if got := isRuntimeIdentifierPath(tc.expr); got != tc.wantOrderIdentifierPath {
				t.Fatalf("unexpected isRuntimeIdentifierPath for %q: got=%v want=%v", tc.expr, got, tc.wantOrderIdentifierPath)
			}
			if got := isRuntimeOrderByExpr(tc.expr); got != tc.wantOrderByExpr {
				t.Fatalf("unexpected isRuntimeOrderByExpr for %q: got=%v want=%v", tc.expr, got, tc.wantOrderByExpr)
			}
		})
	}
}

func TestSupportsRuntimePipelineQueryRejectsProjectionExpressionMatrix(t *testing.T) {
	for _, tc := range rejectedRuntimeProjectionGuardCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if supportsRuntimePipelineQuery(query) {
				t.Fatalf("expected projection rejection matrix query to be rejected for guarded runtime path: %q", tc.query)
			}
		})
	}
}

func TestSupportsRuntimePipelineQueryAcceptsSimpleProjectionExpressionMatrix(t *testing.T) {
	for _, tc := range acceptedRuntimeProjectionGuardCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if !supportsRuntimePipelineQuery(query) {
				t.Fatalf("expected simple projection expression to be supported for guarded runtime path: %q", tc.query)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereAndConjunction(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid > 'aaa' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u61",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after AND filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u61" {
		t.Fatalf("expected projected out value u61, got %#v", res.Rows[0])
	}
}

func TestParseRuntimeWhereAtoms(t *testing.T) {
	type wantAtom struct {
		leftName       string
		op             string
		rightAny       any
		rightParamName string
	}

	tests := []struct {
		name      string
		raw       string
		wantOK    bool
		wantAtoms []wantAtom
	}{
		{
			name:      "accepts conjunction with comparators",
			raw:       "uid = $src AND score >= 10 AND score < 20",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "score", op: ">=", rightAny: int64(10)}, {leftName: "score", op: "<", rightAny: int64(20)}},
		},
		{
			name:      "accepts OR disjunction",
			raw:       "uid = $src OR uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts parenthesized atom",
			raw:       "(uid = $src)",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}},
		},
		{
			name:   "rejects mixed boolean form",
			raw:    "uid = $src AND uid = 'u80' OR uid = 'u81'",
			wantOK: false,
		},
		{
			name:      "accepts lowercase and comparator conjunction",
			raw:       "uid = $src and score >= 10",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "score", op: ">=", rightAny: int64(10)}},
		},
		{
			name:      "accepts quoted OR literal",
			raw:       "uid = 'A OR B'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A OR B"}},
		},
		{
			name:      "accepts quoted apostrophe literal with OR token text",
			raw:       "uid = 'A\\' OR B' OR uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A' OR B"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts double quoted escaped quote literal with OR token text",
			raw:       `uid = "A\" OR B" OR uid = 'x'`,
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: `A" OR B`}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts quoted AND literal",
			raw:       "uid = 'A AND B' AND score = 1",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: "A AND B"}, {leftName: "score", op: "=", rightAny: int64(1)}},
		},
		{
			name:      "accepts escaped quote literal with AND token text in conjunction",
			raw:       `uid = "A\" AND B" AND uid = 'x'`,
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightAny: `A" AND B`}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts lowercase OR token",
			raw:       "uid = $src or uid = 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}},
		},
		{
			name:      "accepts parenthesized OR group",
			raw:       "(uid = $src OR uid = 'x') OR uid = 'y'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "=", rightParamName: "src"}, {leftName: "uid", op: "=", rightAny: "x"}, {leftName: "uid", op: "=", rightAny: "y"}},
		},
		{
			name:      "accepts starts with",
			raw:       "uid STARTS WITH 'u'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightAny: "u"}},
		},
		{
			name:      "accepts ends with lowercase",
			raw:       "uid ends with '1'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "ENDS WITH", rightAny: "1"}},
		},
		{
			name:      "accepts contains",
			raw:       "uid CONTAINS '6'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "CONTAINS", rightAny: "6"}},
		},
		{
			name:      "accepts starts with parameter rhs",
			raw:       "uid STARTS WITH $prefix",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightParamName: "prefix"}},
		},
		{
			name:      "accepts grouped string predicate disjunction",
			raw:       "(uid STARTS WITH 'u' OR uid ENDS WITH '2') OR uid CONTAINS 'x'",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "STARTS WITH", rightAny: "u"}, {leftName: "uid", op: "ENDS WITH", rightAny: "2"}, {leftName: "uid", op: "CONTAINS", rightAny: "x"}},
		},
		{
			name:      "accepts is null",
			raw:       "uid IS NULL",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "IS NULL"}},
		},
		{
			name:      "accepts is not null lowercase",
			raw:       "uid is not null",
			wantOK:    true,
			wantAtoms: []wantAtom{{leftName: "uid", op: "IS NOT NULL"}},
		},
		{
			name:   "rejects starts with numeric literal",
			raw:    "uid STARTS WITH 10",
			wantOK: false,
		},
		{
			name:   "rejects comparator identifier rhs",
			raw:    "uid = other",
			wantOK: false,
		},
		{
			name:   "rejects string predicate identifier rhs",
			raw:    "uid STARTS WITH prefix",
			wantOK: false,
		},
		{
			name:   "rejects mixed parenthesized boolean form",
			raw:    "(uid = $src OR uid = 'x') AND uid = 'y'",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atoms, ok := parseRuntimeWhereAtoms(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("unexpected ok result for %q: got=%v want=%v atoms=%#v", tc.raw, ok, tc.wantOK, atoms)
			}
			if !tc.wantOK {
				if len(atoms) != 0 {
					t.Fatalf("expected rejected expression to produce no atoms, got %#v", atoms)
				}
				return
			}

			got := make([]wantAtom, 0, len(atoms))
			for _, atom := range atoms {
				got = append(got, wantAtom{
					leftName:       atom.leftName,
					op:             atom.op,
					rightAny:       atom.rightAny,
					rightParamName: atom.rightParamName,
				})
			}
			if !reflect.DeepEqual(got, tc.wantAtoms) {
				t.Fatalf("unexpected atoms for %q: got=%#v want=%#v", tc.raw, got, tc.wantAtoms)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithQuotedBooleanTokenLiterals(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'A OR B' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "A OR B",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after quoted OR literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "A OR B" {
		t.Fatalf("expected projected out value 'A OR B', got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereStringPredicateMatrix(t *testing.T) {
	tests := []struct {
		name     string
		whereRaw string
		src      string
		params   Params
		wantRows int
	}{
		{name: "starts with true", whereRaw: "uid STARTS WITH 'u'", src: "u62", wantRows: 1},
		{name: "starts with false", whereRaw: "uid STARTS WITH 'x'", src: "u62", wantRows: 0},
		{name: "ends with true", whereRaw: "uid ENDS WITH '2'", src: "u62", wantRows: 1},
		{name: "ends with false", whereRaw: "uid ENDS WITH '9'", src: "u62", wantRows: 0},
		{name: "contains true", whereRaw: "uid CONTAINS '6'", src: "u62", wantRows: 1},
		{name: "contains false", whereRaw: "uid CONTAINS 'zzz'", src: "u62", wantRows: 0},
		{name: "starts with parameter rhs", whereRaw: "uid STARTS WITH $prefix", src: "u62", params: Params{"prefix": "u"}, wantRows: 1},
		{name: "grouped string predicate disjunction", whereRaw: "(uid STARTS WITH 'x' OR uid ENDS WITH '2') OR uid CONTAINS 'zzz'", src: "u62", wantRows: 1},
		{name: "is not null true", whereRaw: "uid IS NOT NULL", src: "u62", wantRows: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmtText := "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE " + tc.whereRaw + " RETURN uid AS out"
			stmt, err := parser.ParseStatement(stmtText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			params := Params{
				"tenant":                    "acme",
				"src":                       tc.src,
				"__ve_use_runtime_pipeline": true,
			}
			for key, value := range tc.params {
				params[key] = value
			}

			res, err := exec.ExecuteStatement(ctx, stmt, params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count: where=%q src=%q got=%d want=%d rows=%#v", tc.whereRaw, tc.src, len(res.Rows), tc.wantRows, res.Rows)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereIsNullMatrix(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		params   Params
		wantRows int
	}{
		{
			name:     "is null false for projected id",
			query:    "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NULL RETURN uid AS out",
			params:   Params{"tenant": "acme", "src": "u113", "__ve_use_runtime_pipeline": true},
			wantRows: 0,
		},
		{
			name:     "is not null true for projected id",
			query:    "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out",
			params:   Params{"tenant": "acme", "src": "u114", "__ve_use_runtime_pipeline": true},
			wantRows: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			res, err := exec.ExecuteStatement(ctx, stmt, tc.params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count for %q: got=%d want=%d rows=%#v", tc.query, len(res.Rows), tc.wantRows, res.Rows)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineCreateWithEscapedQuoteBooleanTokenLiteral(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'A\\' OR B' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "A' OR B",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after escaped-quote literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "A' OR B" {
		t.Fatalf("expected projected out value \"A' OR B\", got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithDoubleQuotedEscapedQuoteBooleanTokenLiteral(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       `A" OR B`,
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after double-quoted escaped literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != `A" OR B` {
		t.Fatalf("expected projected out value %#v, got %#v", `A" OR B`, res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineCreateWithEscapedBackslashBeforeClosingQuote(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'C:\\path' RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       `C:\path`,
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row after escaped-backslash literal filter, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != `C:\path` {
		t.Fatalf("expected projected out value %#v, got %#v", `C:\path`, res.Rows[0])
	}
}

func TestRuntimeCompareWhereNumericTruthTable(t *testing.T) {
	if !runtimeCompareWhere(10, ">", 2) {
		t.Fatalf("expected 10 > 2")
	}
	if !runtimeCompareWhere("10", ">=", int64(10)) {
		t.Fatalf("expected string numeric compare to coerce")
	}
	if runtimeCompareWhere(1, "<", 0) {
		t.Fatalf("expected 1 < 0 to be false")
	}
	if runtimeCompareWhere(1, "=", 2) {
		t.Fatalf("expected 1 = 2 to be false")
	}
}

func TestExecuteStatementRuntimePipelineCreateWithWhereComparatorMatrix(t *testing.T) {
	tests := []struct {
		name     string
		whereRaw string
		src      string
		wantRows int
	}{
		{name: "equal true", whereRaw: "uid = $src", src: "u62", wantRows: 1},
		{name: "equal false", whereRaw: "uid = 'zzz'", src: "u62", wantRows: 0},
		{name: "not equal true", whereRaw: "uid <> 'zzz'", src: "u62", wantRows: 1},
		{name: "not equal false", whereRaw: "uid <> $src", src: "u62", wantRows: 0},
		{name: "greater true", whereRaw: "uid > 'aaa'", src: "u61", wantRows: 1},
		{name: "greater false", whereRaw: "uid > 'zzz'", src: "u61", wantRows: 0},
		{name: "greater equal true", whereRaw: "uid >= 'u70'", src: "u70", wantRows: 1},
		{name: "greater equal false", whereRaw: "uid >= 'u99'", src: "u70", wantRows: 0},
		{name: "less true numeric", whereRaw: "uid < 20", src: "10", wantRows: 1},
		{name: "less false numeric", whereRaw: "uid < 2", src: "10", wantRows: 0},
		{name: "less equal true", whereRaw: "uid <= $src", src: "u71", wantRows: 1},
		{name: "less equal false", whereRaw: "uid <= 'u10'", src: "u71", wantRows: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmtText := "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE " + tc.whereRaw + " RETURN uid AS out"
			stmt, err := parser.ParseStatement(stmtText)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, Params{
				"tenant":                    "acme",
				"src":                       tc.src,
				"__ve_use_runtime_pipeline": true,
			})
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != tc.wantRows {
				t.Fatalf("unexpected row count: where=%q src=%q got=%d want=%d rows=%#v", tc.whereRaw, tc.src, len(res.Rows), tc.wantRows, res.Rows)
			}
			if tc.wantRows == 1 {
				if got := res.Rows[0]["out"]; got != tc.src {
					t.Fatalf("expected out column to match src when row is returned: got=%#v src=%q row=%#v", got, tc.src, res.Rows[0])
				}
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineFallsBackForRejectedMixedBooleanWhereShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid = 'u80' OR uid = 'u81' RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u81",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if runtimeOK || runtimeRes != nil {
		t.Fatalf("expected rejected mixed boolean WHERE shape to bypass runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u81",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected fallback execution to return one row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u81" {
		t.Fatalf("expected fallback execution row out=u81, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineFallsBackForRejectedIdentifierRHSWhereShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) WITH u.id AS uid, u.id AS other WHERE uid = other RETURN uid AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u82",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if runtimeOK || runtimeRes != nil {
		t.Fatalf("expected identifier RHS WHERE shape to bypass runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u82",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected fallback execution to return one row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u82" {
		t.Fatalf("expected fallback execution row out=u82, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineFallsBackForRejectedProjectionExpressionShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN (u.id) AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u118",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if runtimeOK || runtimeRes != nil {
		t.Fatalf("expected projection expression shape to bypass runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u118",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected fallback execution to return one row, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u118" {
		t.Fatalf("expected fallback execution row out=u118, got %#v", res.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineExecutesNullKeywordOrderByShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY null DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u126",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if !runtimeOK || runtimeRes == nil {
		t.Fatalf("expected null-keyword ORDER BY shape to execute via runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u126",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime row for ORDER BY null, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u126" {
		t.Fatalf("expected runtime row out=u126, got %#v", res.Rows[0])
	}

	// Legacy is known-broken for ORDER BY null; document this explicitly.
	_, err = exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u126",
		"__ve_use_runtime_pipeline": false,
	})
	if err == nil {
		t.Fatalf("expected legacy execution to reject ORDER BY null")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "undefined variable") {
		t.Fatalf("expected undefined-variable legacy error for ORDER BY null, got %v", err)
	}
}

func TestExecuteStatementRuntimePipelineExecutesTrueKeywordOrderByShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY true DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u127",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if !runtimeOK || runtimeRes == nil {
		t.Fatalf("expected true-keyword ORDER BY shape to execute via runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u127",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime row for ORDER BY true, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u127" {
		t.Fatalf("expected runtime row out=u127, got %#v", res.Rows[0])
	}

	// Legacy is known-broken for ORDER BY true; document this explicitly.
	_, err = exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u127",
		"__ve_use_runtime_pipeline": false,
	})
	if err == nil {
		t.Fatalf("expected legacy execution to reject ORDER BY true")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "undefined variable") {
		t.Fatalf("expected undefined-variable legacy error for ORDER BY true, got %v", err)
	}
}

func TestExecuteStatementRuntimePipelineExecutesFalseKeywordOrderByShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY false DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{
		"tenant":                    "acme",
		"src":                       "u128",
		"__ve_use_runtime_pipeline": true,
	}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if !runtimeOK || runtimeRes == nil {
		t.Fatalf("expected false-keyword ORDER BY shape to execute via runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u128",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one runtime row for ORDER BY false, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u128" {
		t.Fatalf("expected runtime row out=u128, got %#v", res.Rows[0])
	}

	// Legacy is known-broken for ORDER BY false; document this explicitly.
	_, err = exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u128",
		"__ve_use_runtime_pipeline": false,
	})
	if err == nil {
		t.Fatalf("expected legacy execution to reject ORDER BY false")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "undefined variable") {
		t.Fatalf("expected undefined-variable legacy error for ORDER BY false, got %v", err)
	}
}

func TestExecuteStatementRuntimePipelineAcceptedExecutionMatrix(t *testing.T) {
	for _, tc := range acceptedRuntimeExecutionCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()

			exec := New(store, Options{})
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}

			runtimeParams := Params{"__ve_use_runtime_pipeline": true}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}

			if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, runtimeParams); execErr != nil {
				t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
			} else if !runtimeOK || runtimeRes == nil {
				t.Fatalf("expected accepted execution matrix case to run via runtime pipeline, got ok=%v res=%#v", runtimeOK, runtimeRes)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, runtimeParams)
			if err != nil {
				t.Fatalf("runtime execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected one runtime row for accepted execution matrix case, got %#v", res.Rows)
			}
			if got := res.Rows[0][tc.wantKey]; !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("unexpected runtime row value for %q: got=%#v want=%#v row=%#v", tc.query, got, tc.wantValue, res.Rows[0])
			}
		})
	}
}

type rejectedRuntimeFallbackCase struct {
	name      string
	query     string
	params    Params
	wantKey   string
	wantValue any
}

func rejectedRuntimeFallbackCases() []rejectedRuntimeFallbackCase {
	return []rejectedRuntimeFallbackCase{
		{
			name:      "where mixed boolean form",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid = 'u80' OR uid = 'u81' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u81"},
			wantKey:   "out",
			wantValue: "u81",
		},
		{
			name:      "return arithmetic projection",
			query:     "CREATE (u:User {id:$src}) RETURN u.id + 'x' AS out",
			params:    Params{"tenant": "acme", "src": "u220"},
			wantKey:   "out",
			wantValue: "u220x",
		},
		{
			name:      "return function-call projection",
			query:     "CREATE (u:User {id:$src}) RETURN toUpper(u.id) AS out",
			params:    Params{"tenant": "acme", "src": "u221"},
			wantKey:   "out",
			wantValue: "U221",
		},
		{
			name:      "with arithmetic projection",
			query:     "CREATE (u:User {id:$src}) WITH u.id + 'x' AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u222"},
			wantKey:   "out",
			wantValue: "u222x",
		},
		{
			name:      "with function-call projection",
			query:     "CREATE (u:User {id:$src}) WITH toUpper(u.id) AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u223"},
			wantKey:   "out",
			wantValue: "U223",
		},
		{
			name:      "order by arithmetic",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY out + 'x' DESC",
			params:    Params{"tenant": "acme", "src": "u224"},
			wantKey:   "out",
			wantValue: "u224",
		},
		{
			name:      "order by function call",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY toUpper(out) DESC",
			params:    Params{"tenant": "acme", "src": "u225"},
			wantKey:   "out",
			wantValue: "u225",
		},
		{
			name:      "order by nested parenthesized computed",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ((out + 'x')) DESC",
			params:    Params{"tenant": "acme", "src": "u226"},
			wantKey:   "out",
			wantValue: "u226",
		},
		{
			name:      "order by nested parenthesized function call",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS out ORDER BY ((toUpper(out))) DESC",
			params:    Params{"tenant": "acme", "src": "u227"},
			wantKey:   "out",
			wantValue: "u227",
		},
	}
}

func TestExecuteStatementRuntimePipelineFallbackMatrixRepresentativeRejectedShapes(t *testing.T) {
	tests := rejectedRuntimeFallbackCases()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()
			exec := New(store, Options{})

			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}

			runtimeParams := Params{"__ve_use_runtime_pipeline": true}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}

			if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, runtimeParams); execErr != nil {
				t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
			} else if runtimeOK || runtimeRes != nil {
				t.Fatalf("expected rejected shape to bypass runtime pipeline, got ok=%v res=%#v query=%q", runtimeOK, runtimeRes, tc.query)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, runtimeParams)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected fallback execution to return one row, got %#v", res.Rows)
			}
			if got := res.Rows[0][tc.wantKey]; !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("unexpected fallback output: query=%q got=%#v want=%#v row=%#v", tc.query, got, tc.wantValue, res.Rows[0])
			}
		})
	}
}

func TestRejectedFallbackMatrixQueriesAreGuardRejected(t *testing.T) {
	for _, tc := range rejectedRuntimeFallbackCases() {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}
			if supportsRuntimePipelineQuery(query) {
				t.Fatalf("expected rejected fallback matrix query to fail guarded runtime gate: %q", tc.query)
			}
		})
	}
}

func TestRuntimePipelineGateDecisionAlignsWithTryExecute(t *testing.T) {
	type gateCase struct {
		name      string
		query     string
		params    Params
		expectRun bool
	}

	accepted := []gateCase{
		{name: "accepted simple return projection", query: "CREATE (u:User {id:$src}) RETURN u.id AS out", params: Params{"tenant": "acme", "src": "u300"}, expectRun: true},
	}
	for _, tc := range acceptedRuntimeExecutionCases() {
		accepted = append(accepted, gateCase{name: tc.name, query: tc.query, params: tc.params, expectRun: true})
	}

	tests := []gateCase{}
	for _, tc := range accepted {
		tests = append(tests, gateCase{name: tc.name, query: tc.query, params: tc.params, expectRun: tc.expectRun})
	}

	for _, tc := range acceptedRuntimeWhereGuardCases() {
		tests = append(tests, gateCase{name: tc.name, query: tc.query, params: Params{"tenant": "acme", "src": "u301"}, expectRun: true})
	}

	for _, tc := range rejectedRuntimeWhereGuardCases() {
		tests = append(tests, gateCase{name: tc.name, query: tc.query, params: Params{"tenant": "acme", "src": "u303"}, expectRun: false})
	}

	for _, tc := range rejectedRuntimeProjectionGuardCases() {
		tests = append(tests, gateCase{
			name:      tc.name,
			query:     tc.query,
			params:    Params{"tenant": "acme", "src": "u304"},
			expectRun: false,
		})
	}

	for _, tc := range rejectedRuntimeOrderByGuardCases() {
		tests = append(tests, gateCase{
			name:      tc.name,
			query:     tc.query,
			params:    Params{"tenant": "acme", "src": "u399"},
			expectRun: false,
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()
			exec := New(store, Options{})

			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}

			gate := supportsRuntimePipelineQuery(query)
			if gate != tc.expectRun {
				t.Fatalf("unexpected gate decision for %q: got=%v want=%v", tc.query, gate, tc.expectRun)
			}

			runtimeParams := Params{"__ve_use_runtime_pipeline": true}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}

			runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, runtimeParams)
			if execErr != nil {
				t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
			}
			if runtimeOK != tc.expectRun {
				t.Fatalf("unexpected runtime decision for %q: ok=%v want=%v", tc.query, runtimeOK, tc.expectRun)
			}
			if !tc.expectRun && runtimeRes != nil {
				t.Fatalf("expected rejected shape to return nil runtime result, got %#v", runtimeRes)
			}
			if tc.expectRun && runtimeRes == nil {
				t.Fatalf("expected accepted shape to produce runtime result")
			}
		})
	}
}

func TestRuntimePipelineDefaultRoutingOptInRunsWithoutParam(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{EnableRuntimePipelineDefault: true})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	params := Params{"tenant": "acme", "src": "u410"}
	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, params); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if !runtimeOK || runtimeRes == nil {
		t.Fatalf("expected default-enabled runtime route to execute supported shape, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, params)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row with default runtime routing, got %#v", res.Rows)
	}
	if got := res.Rows[0]["out"]; got != "u410" {
		t.Fatalf("expected out=u410, got %#v", res.Rows[0])
	}
}

func TestRuntimePipelineDefaultRoutingRespectsExplicitDisableParam(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{EnableRuntimePipelineDefault: true})
	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	query, ok := stmt.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmt)
	}

	if runtimeRes, runtimeOK, execErr := exec.tryExecuteViaRuntimePipeline(ctx, query, Params{"tenant": "acme", "src": "u411", "__ve_use_runtime_pipeline": false}); execErr != nil {
		t.Fatalf("runtime try execute failed unexpectedly: %v", execErr)
	} else if runtimeOK || runtimeRes != nil {
		t.Fatalf("expected explicit false param to disable default runtime routing, got ok=%v res=%#v", runtimeOK, runtimeRes)
	}
}

func TestExecuteStatementRuntimePipelineWithNullAndParamProjectionParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	legacyStore := openStore(t)
	defer func() { _ = legacyStore.Close() }()
	legacyExec := New(legacyStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src}) WITH null AS uid, $src AS src WHERE uid IS NOT NULL OR src IS NOT NULL RETURN src AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u116",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	legacyRes, err := legacyExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u116",
		"__ve_use_runtime_pipeline": false,
	})
	if err != nil {
		t.Fatalf("legacy execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, legacyRes.Rows) {
		t.Fatalf("runtime and legacy rows diverged: runtime=%#v legacy=%#v", runtimeRes.Rows, legacyRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from null/param projection parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != "u116" {
		t.Fatalf("expected projected out value u116, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineWithQuotedAndNumericProjectionParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	legacyStore := openStore(t)
	defer func() { _ = legacyStore.Close() }()
	legacyExec := New(legacyStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src}) WITH 'a\\'b' AS s, 42 AS n, 3.5 AS f, true AS t, false AS ff WHERE n = 42 AND f = 3.5 AND t = true AND ff = false RETURN s AS out, n AS num, f AS fl")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u117",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	legacyRes, err := legacyExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u117",
		"__ve_use_runtime_pipeline": false,
	})
	if err != nil {
		t.Fatalf("legacy execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, legacyRes.Rows) {
		t.Fatalf("runtime and legacy rows diverged: runtime=%#v legacy=%#v", runtimeRes.Rows, legacyRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from quoted/numeric projection parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != "a'b" {
		t.Fatalf("expected projected out value a'b, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineWithDottedOrderByParity(t *testing.T) {
	ctx := context.Background()
	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})

	legacyStore := openStore(t)
	defer func() { _ = legacyStore.Close() }()
	legacyExec := New(legacyStore, Options{})

	stmt, err := parser.ParseStatement("CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY u.id DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := runtimeExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u119",
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	legacyRes, err := legacyExec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       "u119",
		"__ve_use_runtime_pipeline": false,
	})
	if err != nil {
		t.Fatalf("legacy execute failed: %v", err)
	}

	if !reflect.DeepEqual(runtimeRes.Rows, legacyRes.Rows) {
		t.Fatalf("runtime and legacy rows diverged: runtime=%#v legacy=%#v", runtimeRes.Rows, legacyRes.Rows)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from dotted ORDER BY parity shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["uid"]; got != "u119" {
		t.Fatalf("expected projected uid value u119, got %#v", runtimeRes.Rows[0])
	}
}

func TestExecuteStatementRuntimePipelineSimpleProjectionValueMatrix(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		params    Params
		wantKey   string
		wantValue any
	}{
		{name: "identifier projection", query: "CREATE (u:User {id:$src}) RETURN u AS out", params: Params{"tenant": "acme", "src": "u126"}, wantKey: "out", wantValue: "u126"},
		{name: "dotted projection", query: "CREATE (u:User {id:$src}) RETURN u.id AS out", params: Params{"tenant": "acme", "src": "u127"}, wantKey: "out", wantValue: "u127"},
		{name: "parameter projection", query: "CREATE (u:User {id:$src}) RETURN $src AS out", params: Params{"tenant": "acme", "src": "u128"}, wantKey: "out", wantValue: "u128"},
		{name: "null literal projection", query: "CREATE (u:User {id:$src}) RETURN null AS out", params: Params{"tenant": "acme", "src": "u129"}, wantKey: "out", wantValue: nil},
		{name: "boolean true projection", query: "CREATE (u:User {id:$src}) RETURN true AS out", params: Params{"tenant": "acme", "src": "u130"}, wantKey: "out", wantValue: true},
		{name: "boolean false projection", query: "CREATE (u:User {id:$src}) RETURN false AS out", params: Params{"tenant": "acme", "src": "u131"}, wantKey: "out", wantValue: false},
		{name: "integer literal projection", query: "CREATE (u:User {id:$src}) RETURN 42 AS out", params: Params{"tenant": "acme", "src": "u132"}, wantKey: "out", wantValue: int64(42)},
		{name: "float literal projection", query: "CREATE (u:User {id:$src}) RETURN 3.5 AS out", params: Params{"tenant": "acme", "src": "u133"}, wantKey: "out", wantValue: float64(3.5)},
		{name: "quoted string projection", query: "CREATE (u:User {id:$src}) RETURN 'alpha' AS out", params: Params{"tenant": "acme", "src": "u134"}, wantKey: "out", wantValue: "alpha"},
		{name: "with parameter projection", query: "CREATE (u:User {id:$src}) WITH $src AS uid RETURN uid AS out", params: Params{"tenant": "acme", "src": "u135"}, wantKey: "out", wantValue: "u135"},
		{name: "with quoted literal projection", query: "CREATE (u:User {id:$src}) WITH 'beta' AS uid RETURN uid AS out", params: Params{"tenant": "acme", "src": "u136"}, wantKey: "out", wantValue: "beta"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t)
			defer func() { _ = store.Close() }()
			exec := New(store, Options{})

			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			params := Params{"__ve_use_runtime_pipeline": true}
			for k, v := range tc.params {
				params[k] = v
			}

			res, err := exec.ExecuteStatement(ctx, stmt, params)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected one runtime row, got %#v", res.Rows)
			}
			if got := res.Rows[0][tc.wantKey]; !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("unexpected runtime value: query=%q got=%#v want=%#v row=%#v", tc.query, got, tc.wantValue, res.Rows[0])
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineWithEscapedQuoteCompoundWhere(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement(`CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" AND uid = $src RETURN uid AS out`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	runtimeRes, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       `A" OR B`,
		"__ve_use_runtime_pipeline": true,
	})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}
	if len(runtimeRes.Rows) != 1 {
		t.Fatalf("expected one row from escaped-quote compound WHERE runtime shape, got %#v", runtimeRes.Rows)
	}
	if got := runtimeRes.Rows[0]["out"]; got != `A" OR B` {
		t.Fatalf("expected projected out value %#v, got %#v", `A" OR B`, runtimeRes.Rows[0])
	}

	_, err = exec.ExecuteStatement(ctx, stmt, Params{
		"tenant":                    "acme",
		"src":                       `A" OR B`,
		"__ve_use_runtime_pipeline": false,
	})
	if err == nil {
		t.Fatalf("expected legacy execution to reject escaped-quote compound WHERE shape")
	}
	if !strings.Contains(err.Error(), "UNSUPPORTED") {
		t.Fatalf("expected unsupported error from legacy execution, got %v", err)
	}
}

func TestApplyRuntimeWithWhereFilter(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		rows       []Row
		params     Params
		wantRows   []Row
		wantFilter bool
	}{
		{
			name:       "filters rows using return alias mapping",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}},
			wantFilter: true,
		},
		{
			name:       "returns original rows when where name not present in return",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid, u.id AS other WHERE uid = $src RETURN other AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}, {"out": "u2"}},
			wantFilter: false,
		},
		{
			name:       "returns original rows when where param is missing",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $missing RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}, {"out": "u2"}},
			wantFilter: false,
		},
		{
			name:       "no with where leaves rows unchanged",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}, {"out": "u2"}},
			wantFilter: false,
		},
		{
			name:       "filters rows using OR disjunction",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src OR uid = 'u2' RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}, {"out": "u3"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}, {"out": "u2"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using parenthesized OR group",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid = $src OR uid = 'u2') OR uid = 'u4' RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}, {"out": "u3"}, {"out": "u4"}},
			params:     Params{"src": "u1"},
			wantRows:   []Row{{"out": "u1"}, {"out": "u2"}, {"out": "u4"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using escaped quote literal",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'A\\' OR B' RETURN uid AS out",
			rows:       []Row{{"out": "A' OR B"}, {"out": "A'' OR B"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "A' OR B"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using double quoted escaped quote literal",
			query:      `CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = "A\" OR B" RETURN uid AS out`,
			rows:       []Row{{"out": `A" OR B`}, {"out": `A\" OR B`}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": `A" OR B`}},
			wantFilter: true,
		},
		{
			name:       "filters rows using escaped backslash literal",
			query:      `CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = 'C:\\path' RETURN uid AS out`,
			rows:       []Row{{"out": `C:\path`}, {"out": `C:\\path`}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": `C:\path`}},
			wantFilter: true,
		},
		{
			name:       "filters rows using starts with",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH 'u' RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "x1"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "u1"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using ends with",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid ENDS WITH '2' RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "u2"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "u2"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using contains",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid CONTAINS 'bc' RETURN uid AS out",
			rows:       []Row{{"out": "abc"}, {"out": "def"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "abc"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using starts with parameter rhs",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH $prefix RETURN uid AS out",
			rows:       []Row{{"out": "u1"}, {"out": "x1"}},
			params:     Params{"prefix": "u"},
			wantRows:   []Row{{"out": "u1"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using grouped string predicate disjunction",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid STARTS WITH 'x' OR uid ENDS WITH '2') OR uid CONTAINS 'bc' RETURN uid AS out",
			rows:       []Row{{"out": "abc"}, {"out": "u2"}, {"out": "u1"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "abc"}, {"out": "u2"}},
			wantFilter: true,
		},
		{
			name:       "filters rows using is null",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NULL RETURN uid AS out",
			rows:       []Row{{"out": nil}, {"out": "u1"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": nil}},
			wantFilter: true,
		},
		{
			name:       "filters rows using is not null",
			query:      "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out",
			rows:       []Row{{"out": nil}, {"out": "u1"}},
			params:     Params{"src": "ignored"},
			wantRows:   []Row{{"out": "u1"}},
			wantFilter: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			query, ok := stmt.(*ast.QueryStatement)
			if !ok {
				t.Fatalf("expected query statement, got %T", stmt)
			}

			gotRows, gotFilter := applyRuntimeWithWhereFilter(query, tc.rows, tc.params)
			if gotFilter != tc.wantFilter {
				t.Fatalf("unexpected filter applied flag: got=%v want=%v", gotFilter, tc.wantFilter)
			}
			if !reflect.DeepEqual(gotRows, tc.wantRows) {
				t.Fatalf("unexpected filtered rows: got=%#v want=%#v", gotRows, tc.wantRows)
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineParityWithLegacy(t *testing.T) {
	type edgePresenceCheck struct {
		srcParam       string
		dstParam       string
		edgeType       string
		requirePresent bool
	}

	tests := []struct {
		name       string
		query      string
		params     Params
		vertexIDs  []string
		edgeIDs    []string
		edgeChecks []edgePresenceCheck
	}{
		{
			name:      "with where equals",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u95"},
			vertexIDs: []string{"u95"},
		},
		{
			name:      "with distinct return",
			query:     "CREATE (u:User {id:$src}) WITH DISTINCT u.id AS uid RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u96"},
			vertexIDs: []string{"u96"},
		},
		{
			name:      "with where conjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src AND uid > 'aaa' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u97"},
			vertexIDs: []string{"u97"},
		},
		{
			name:      "with where disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid = $src OR uid = 'zzz' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u108"},
			vertexIDs: []string{"u108"},
		},
		{
			name:      "with where parenthesized disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid = $src OR uid = 'zzz') OR uid = 'never' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u109"},
			vertexIDs: []string{"u109"},
		},
		{
			name:      "with where starts with",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH 'u' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u110"},
			vertexIDs: []string{"u110"},
		},
		{
			name:      "with where starts with parameter",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid STARTS WITH $prefix RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u111", "prefix": "u"},
			vertexIDs: []string{"u111"},
		},
		{
			name:      "with where grouped string predicate disjunction",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE (uid STARTS WITH 'x' OR uid ENDS WITH '2') OR uid CONTAINS '11' RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u112"},
			vertexIDs: []string{"u112"},
		},
		{
			name:      "with where is not null",
			query:     "CREATE (u:User {id:$src}) WITH u.id AS uid WHERE uid IS NOT NULL RETURN uid AS out",
			params:    Params{"tenant": "acme", "src": "u115"},
			vertexIDs: []string{"u115"},
		},
		{
			name:      "edge create with return endpoints",
			query:     "CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid",
			params:    Params{"tenant": "acme", "src": "u98", "dst": "u99"},
			vertexIDs: []string{"u98", "u99"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "src",
				dstParam:       "dst",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "reverse edge create with return endpoints",
			query:     "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst}) RETURN u.id AS uid, v.id AS vid",
			params:    Params{"tenant": "acme", "src": "u103", "dst": "u104"},
			vertexIDs: []string{"u103", "u104"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "dst",
				dstParam:       "src",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "with order skip limit",
			query:     "CREATE (u:User {id:$src}) RETURN u.id AS uid ORDER BY uid DESC SKIP 0 LIMIT 1",
			params:    Params{"tenant": "acme", "src": "u100"},
			vertexIDs: []string{"u100"},
		},
		{
			name:      "write only create",
			query:     "CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})",
			params:    Params{"tenant": "acme", "src": "u101", "dst": "u102"},
			vertexIDs: []string{"u101", "u102"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "src",
				dstParam:       "dst",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
		{
			name:      "reverse write only create",
			query:     "CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})",
			params:    Params{"tenant": "acme", "src": "u105", "dst": "u106"},
			vertexIDs: []string{"u105", "u106"},
			edgeIDs:   nil,
			edgeChecks: []edgePresenceCheck{{
				srcParam:       "dst",
				dstParam:       "src",
				edgeType:       "KNOWS",
				requirePresent: true,
			}},
		},
	}

	type edgeSnapshot struct {
		Exists bool
		Type   string
		SrcID  string
		DstID  string
	}
	collectWriteSnapshot := func(ctx context.Context, store graph.GraphStore, tenant string, vertexIDs []string, edgeIDs []string) (map[string]bool, map[string]edgeSnapshot, error) {
		vertices := make(map[string]bool, len(vertexIDs))
		edges := make(map[string]edgeSnapshot, len(edgeIDs))
		err := store.View(ctx, func(tx graph.Tx) error {
			for _, vertexID := range vertexIDs {
				vertex, err := tx.GetVertex(ctx, tenant, vertexID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						vertices[vertexID] = false
						continue
					}
					return err
				}
				vertices[vertexID] = vertex != nil
			}
			for _, edgeID := range edgeIDs {
				edge, err := tx.GetEdge(ctx, tenant, edgeID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						edges[edgeID] = edgeSnapshot{}
						continue
					}
					return err
				}
				if edge == nil {
					edges[edgeID] = edgeSnapshot{}
					continue
				}
				edges[edgeID] = edgeSnapshot{Exists: true, Type: edge.Type, SrcID: edge.SrcID, DstID: edge.DstID}
			}
			return nil
		})
		return vertices, edges, err
	}
	collectDirectedEdgePresence := func(ctx context.Context, store graph.GraphStore, tenant string, checks []edgePresenceCheck, params Params) (map[string]bool, error) {
		presence := make(map[string]bool, len(checks))
		err := store.View(ctx, func(tx graph.Tx) error {
			for _, check := range checks {
				src, _ := params[check.srcParam].(string)
				dst, _ := params[check.dstParam].(string)
				if src == "" || dst == "" || check.edgeType == "" {
					return graph.NewError(graph.ErrKindInvalidInput, "edge presence check requires src, dst, and edgeType", nil)
				}
				hasEdge, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, check.edgeType)
				if err != nil {
					return err
				}
				key := src + "|" + check.edgeType + "|" + dst
				presence[key] = hasEdge
			}
			return nil
		})
		return presence, err
	}

	normalizeRows := func(rows []Row) []Row {
		if rows == nil {
			return []Row{}
		}
		return rows
	}
	normalizeColumns := func(cols []string) []string {
		if cols == nil {
			return []string{}
		}
		return cols
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			runtimeStore := openStore(t)
			defer func() { _ = runtimeStore.Close() }()
			runtimeExec := New(runtimeStore, Options{})
			runtimeStmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			runtimeParams := Params{}
			for k, v := range tc.params {
				runtimeParams[k] = v
			}
			runtimeParams["__ve_use_runtime_pipeline"] = true
			runtimeRes, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams)
			if err != nil {
				t.Fatalf("runtime execution failed: %v", err)
			}

			legacyStore := openStore(t)
			defer func() { _ = legacyStore.Close() }()
			legacyExec := New(legacyStore, Options{})
			legacyStmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			legacyParams := Params{}
			for k, v := range tc.params {
				legacyParams[k] = v
			}
			legacyParams["__ve_use_runtime_pipeline"] = false
			legacyRes, err := legacyExec.ExecuteStatement(ctx, legacyStmt, legacyParams)
			if err != nil {
				t.Fatalf("legacy execution failed: %v", err)
			}

			if !reflect.DeepEqual(normalizeRows(runtimeRes.Rows), normalizeRows(legacyRes.Rows)) {
				t.Fatalf("runtime and legacy rows diverged: runtime=%#v legacy=%#v", runtimeRes.Rows, legacyRes.Rows)
			}
			if !reflect.DeepEqual(normalizeColumns(runtimeRes.Columns), normalizeColumns(legacyRes.Columns)) {
				t.Fatalf("runtime and legacy columns diverged: runtime=%#v legacy=%#v", runtimeRes.Columns, legacyRes.Columns)
			}
			if runtimeRes.Stats.RowsReturned != legacyRes.Stats.RowsReturned {
				t.Fatalf("runtime and legacy RowsReturned diverged: runtime=%d legacy=%d", runtimeRes.Stats.RowsReturned, legacyRes.Stats.RowsReturned)
			}

			tenant, _ := tc.params["tenant"].(string)
			if tenant == "" {
				tenant = "acme"
			}
			runtimeVertices, runtimeEdges, err := collectWriteSnapshot(ctx, runtimeStore, tenant, tc.vertexIDs, tc.edgeIDs)
			if err != nil {
				t.Fatalf("runtime snapshot read failed: %v", err)
			}
			legacyVertices, legacyEdges, err := collectWriteSnapshot(ctx, legacyStore, tenant, tc.vertexIDs, tc.edgeIDs)
			if err != nil {
				t.Fatalf("legacy snapshot read failed: %v", err)
			}
			if !reflect.DeepEqual(runtimeVertices, legacyVertices) {
				t.Fatalf("runtime and legacy vertex write side-effects diverged: runtime=%#v legacy=%#v", runtimeVertices, legacyVertices)
			}
			if !reflect.DeepEqual(runtimeEdges, legacyEdges) {
				t.Fatalf("runtime and legacy edge write side-effects diverged: runtime=%#v legacy=%#v", runtimeEdges, legacyEdges)
			}

			runtimeDirectedEdges, err := collectDirectedEdgePresence(ctx, runtimeStore, tenant, tc.edgeChecks, tc.params)
			if err != nil {
				t.Fatalf("runtime directed-edge snapshot read failed: %v", err)
			}
			legacyDirectedEdges, err := collectDirectedEdgePresence(ctx, legacyStore, tenant, tc.edgeChecks, tc.params)
			if err != nil {
				t.Fatalf("legacy directed-edge snapshot read failed: %v", err)
			}
			if !reflect.DeepEqual(runtimeDirectedEdges, legacyDirectedEdges) {
				t.Fatalf("runtime and legacy directed-edge side-effects diverged: runtime=%#v legacy=%#v", runtimeDirectedEdges, legacyDirectedEdges)
			}
			for _, check := range tc.edgeChecks {
				if !check.requirePresent {
					continue
				}
				src, _ := tc.params[check.srcParam].(string)
				dst, _ := tc.params[check.dstParam].(string)
				key := src + "|" + check.edgeType + "|" + dst
				if !runtimeDirectedEdges[key] || !legacyDirectedEdges[key] {
					t.Fatalf("expected directed edge %s to be present in both paths; runtime=%v legacy=%v", key, runtimeDirectedEdges[key], legacyDirectedEdges[key])
				}
			}
		})
	}
}

func TestExecuteStatementRuntimePipelineWriteOnlyEdgeCreateParity(t *testing.T) {
	const legacyShouldCreateDirectedEdge = true

	ctx := context.Background()
	queryText := "CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})"
	params := Params{"tenant": "acme", "src": "u110", "dst": "u111"}

	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})
	runtimeStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	runtimeParams := Params{}
	for k, v := range params {
		runtimeParams[k] = v
	}
	runtimeParams["__ve_use_runtime_pipeline"] = true
	if _, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams); err != nil {
		t.Fatalf("runtime execution failed: %v", err)
	}

	legacyStore := openStore(t)
	defer func() { _ = legacyStore.Close() }()
	legacyExec := New(legacyStore, Options{})
	legacyStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	legacyParams := Params{}
	for k, v := range params {
		legacyParams[k] = v
	}
	legacyParams["__ve_use_runtime_pipeline"] = false
	if _, err := legacyExec.ExecuteStatement(ctx, legacyStmt, legacyParams); err != nil {
		t.Fatalf("legacy execution failed: %v", err)
	}

	checkHasDirected := func(store graph.GraphStore, tenant, src, dst, edgeType string) bool {
		has := false
		err := store.View(ctx, func(tx graph.Tx) error {
			found, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, edgeType)
			if err != nil {
				return err
			}
			has = found
			return nil
		})
		if err != nil {
			t.Fatalf("directed edge presence check failed: %v", err)
		}
		return has
	}

	runtimeHas := checkHasDirected(runtimeStore, "acme", "u110", "u111", "KNOWS")
	legacyHas := checkHasDirected(legacyStore, "acme", "u110", "u111", "KNOWS")

	if !runtimeHas {
		t.Fatalf("expected runtime path to create directed edge")
	}
	if !legacyShouldCreateDirectedEdge {
		t.Fatalf("legacy parity guard is misconfigured; expected true")
	}
	if legacyHas != legacyShouldCreateDirectedEdge {
		t.Fatalf("legacy write-only edge parity mismatch: got=%v want=%v", legacyHas, legacyShouldCreateDirectedEdge)
	}
}

func TestExecuteStatementRuntimePipelineReverseWriteOnlyEdgeCreateParity(t *testing.T) {
	const legacyShouldCreateDirectedEdge = true

	ctx := context.Background()
	queryText := "CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})"
	params := Params{"tenant": "acme", "src": "u112", "dst": "u113"}

	runtimeStore := openStore(t)
	defer func() { _ = runtimeStore.Close() }()
	runtimeExec := New(runtimeStore, Options{})
	runtimeStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	runtimeParams := Params{}
	for k, v := range params {
		runtimeParams[k] = v
	}
	runtimeParams["__ve_use_runtime_pipeline"] = true
	if _, err := runtimeExec.ExecuteStatement(ctx, runtimeStmt, runtimeParams); err != nil {
		t.Fatalf("runtime execution failed: %v", err)
	}

	legacyStore := openStore(t)
	defer func() { _ = legacyStore.Close() }()
	legacyExec := New(legacyStore, Options{})
	legacyStmt, err := parser.ParseStatement(queryText)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	legacyParams := Params{}
	for k, v := range params {
		legacyParams[k] = v
	}
	legacyParams["__ve_use_runtime_pipeline"] = false
	if _, err := legacyExec.ExecuteStatement(ctx, legacyStmt, legacyParams); err != nil {
		t.Fatalf("legacy execution failed: %v", err)
	}

	checkHasDirected := func(store graph.GraphStore, tenant, src, dst, edgeType string) bool {
		has := false
		err := store.View(ctx, func(tx graph.Tx) error {
			found, err := tx.HasDirectedEdgeBetween(ctx, tenant, src, dst, edgeType)
			if err != nil {
				return err
			}
			has = found
			return nil
		})
		if err != nil {
			t.Fatalf("directed edge presence check failed: %v", err)
		}
		return has
	}

	runtimeHas := checkHasDirected(runtimeStore, "acme", "u113", "u112", "KNOWS")
	legacyHas := checkHasDirected(legacyStore, "acme", "u113", "u112", "KNOWS")

	if !runtimeHas {
		t.Fatalf("expected runtime path to create reverse directed edge")
	}
	if !legacyShouldCreateDirectedEdge {
		t.Fatalf("legacy parity guard is misconfigured; expected true")
	}
	if legacyHas != legacyShouldCreateDirectedEdge {
		t.Fatalf("legacy reverse write-only edge parity mismatch: got=%v want=%v", legacyHas, legacyShouldCreateDirectedEdge)
	}
}
