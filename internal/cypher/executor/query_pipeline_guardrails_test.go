package executor

import (
	"context"
	"reflect"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
)

// Query Pipeline guardrail checklist (QP-0 baseline):
// 1) Freeze supported query-shape behavior before clause-structure migration.
// 2) Keep EXPLAIN behavior stable while pipeline internals evolve.
// 3) During migrated slices, avoid adding new raw-text semantic recovery paths.
func TestQueryPipelineBaselineSupportedShapes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	exec := New(store, Options{})

	t.Run("order-skip-limit-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID ORDER BY dstID DESC SKIP 1 LIMIT 1")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0]["dstID"] != "g1" {
			t.Fatalf("unexpected ORDER/SKIP/LIMIT rows: %#v", res.Rows)
		}
	})

	t.Run("distinct-projection-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN DISTINCT dst.id AS dstID ORDER BY dstID ASC")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		got := []any{res.Rows[0]["dstID"], res.Rows[1]["dstID"]}
		if !reflect.DeepEqual(got, []any{"g1", "g2"}) {
			t.Fatalf("unexpected DISTINCT projection rows: %#v", res.Rows)
		}
	})

	t.Run("return-star-projection-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN * ORDER BY dst.id ASC LIMIT 1")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected one RETURN * row, got %#v", res.Rows)
		}
		if _, ok := res.Rows[0]["src"]; !ok {
			t.Fatalf("expected RETURN * row to include src binding, got %#v", res.Rows[0])
		}
		if _, ok := res.Rows[0]["dst"]; !ok {
			t.Fatalf("expected RETURN * row to include dst binding, got %#v", res.Rows[0])
		}
	})

	t.Run("optional-match-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("OPTIONAL MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u2"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0]["dstID"] != nil {
			t.Fatalf("unexpected OPTIONAL MATCH rows: %#v", res.Rows)
		}
	})

	t.Run("merge-sequencing-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MERGE (u:User {id: $id}) ON CREATE SET u.role = 'new' ON MATCH SET u.role = 'existing' RETURN u.role AS role")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		first, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u3"})
		if err != nil {
			t.Fatalf("first execute failed: %v", err)
		}
		if len(first.Rows) != 1 || first.Rows[0]["role"] != "new" {
			t.Fatalf("unexpected ON CREATE role result: %#v", first.Rows)
		}

		second, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u3"})
		if err != nil {
			t.Fatalf("second execute failed: %v", err)
		}
		if len(second.Rows) != 1 || second.Rows[0]["role"] != "existing" {
			t.Fatalf("unexpected ON MATCH role result: %#v", second.Rows)
		}
	})

	t.Run("merge-map-action-sequencing-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MERGE (u:User {id: $id}) ON CREATE SET u += {role:'new', age: 1} ON MATCH SET u += {role:'existing'} RETURN u.role AS role")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		first, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u4"})
		if err != nil {
			t.Fatalf("first execute failed: %v", err)
		}
		if len(first.Rows) != 1 || first.Rows[0]["role"] != "new" {
			t.Fatalf("unexpected ON CREATE map action result: %#v", first.Rows)
		}

		second, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u4"})
		if err != nil {
			t.Fatalf("second execute failed: %v", err)
		}
		if len(second.Rows) != 1 || second.Rows[0]["role"] != "existing" {
			t.Fatalf("unexpected ON MATCH map action result: %#v", second.Rows)
		}
	})
}
