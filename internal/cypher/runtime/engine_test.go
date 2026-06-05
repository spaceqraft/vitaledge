package runtime

import (
	"reflect"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/physical"
	"github.com/paegun/vitaledge/internal/cypher/pipeline"
	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

func TestExecutePhysicalPlanMatchQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name ORDER BY name ASC LIMIT 5")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	engine := New()
	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_PROJECT", "PHY_SORT", "PHY_PAGINATION"}
	if !reflect.DeepEqual(res.ExecutedOps, expected) {
		t.Fatalf("unexpected executed ops: got=%v want=%v", res.ExecutedOps, expected)
	}
	if res.Stats.OperatorsExecuted != len(expected) {
		t.Fatalf("unexpected operator count stats: %#v", res.Stats)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", res.Rows)
	}
	if _, ok := res.Rows[0]["name"]; !ok {
		t.Fatalf("expected projected alias key 'name', got %#v", res.Rows[0])
	}
}

func TestExecutePhysicalPlanWriteQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	engine := New()
	res, err := engine.Execute(pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}})
	if err != nil {
		t.Fatalf("runtime execute failed: %v", err)
	}

	expected := []string{"PHY_EXPAND_MATCH", "PHY_WRITE", "PHY_PROJECT", "PHY_PROJECT"}
	if !reflect.DeepEqual(res.ExecutedOps, expected) {
		t.Fatalf("unexpected executed ops: got=%v want=%v", res.ExecutedOps, expected)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one projected row after write path, got %#v", res.Rows)
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected alias key 'uid', got %#v", res.Rows[0])
	}
	if res.Stats.WritesRecorded != 1 {
		t.Fatalf("expected one write event recorded, got %#v", res.Stats)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	event := res.WriteEvents[0]
	if event.MutationType != operators.MutationTypeEdge {
		t.Fatalf("expected edge write event, got %#v", event)
	}
	if event.Edge == nil || event.Edge.Type != "KNOWS" {
		t.Fatalf("expected KNOWS edge payload, got %#v", event)
	}
	if len(event.Edge.RightLabels) != 1 || event.Edge.RightLabels[0] != "User" {
		t.Fatalf("expected User right endpoint label, got %#v", event.Edge)
	}
	if got := event.ResolvedParams["peer"]; got != "u2" {
		t.Fatalf("expected peer param in surfaced write event, got %#v", event.ResolvedParams)
	}
	if event.NodeID == "" {
		t.Fatalf("expected write event node id, got %#v", event)
	}
}

func TestExecuteWithTxAppliesWriteEvents(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)-[:KNOWS]->(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	engine := New()
	engine.RegisterHandler(&seedRowsHandler{name: "PHY_EXPAND_MATCH", rows: []map[string]any{{"u": "u1"}}})
	tx := &recordingTx{}
	res, err := engine.ExecuteWithTx(t.Context(), pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}}, tx)
	if err != nil {
		t.Fatalf("runtime execute with tx failed: %v", err)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one labeled endpoint vertex write, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", tx.edges)
	}
	if tx.vertexes[0].ID != "u2" {
		t.Fatalf("expected labeled right endpoint upsert, got %#v", tx.vertexes[0])
	}
	if tx.edges[0].SrcID != "u1" || tx.edges[0].DstID != "u2" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied edge: %#v", tx.edges[0])
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected uid in result rows, got %#v", res.Rows)
	}
}

func TestExecuteWithTxAppliesReverseWriteEvents(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User {id:$id}) MERGE (u)<-[:KNOWS]-(:User {id:$peer}) WITH u RETURN u.id AS uid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	engine := New()
	engine.RegisterHandler(&seedRowsHandler{name: "PHY_EXPAND_MATCH", rows: []map[string]any{{"u": "u1"}}})
	tx := &recordingTx{}
	res, err := engine.ExecuteWithTx(t.Context(), pipeline.PhysicalExecutionInput{Plan: pp, Tenant: "acme", Params: map[string]any{"id": "u1", "peer": "u2"}}, tx)
	if err != nil {
		t.Fatalf("runtime execute with tx failed: %v", err)
	}
	if len(res.WriteEvents) != 1 {
		t.Fatalf("expected one surfaced write event, got %#v", res.WriteEvents)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one labeled endpoint vertex write, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", tx.edges)
	}
	if tx.vertexes[0].ID != "u2" {
		t.Fatalf("expected labeled right endpoint upsert, got %#v", tx.vertexes[0])
	}
	if tx.edges[0].SrcID != "u2" || tx.edges[0].DstID != "u1" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied reverse edge: %#v", tx.edges[0])
	}
	if _, ok := res.Rows[0]["uid"]; !ok {
		t.Fatalf("expected projected uid in result rows, got %#v", res.Rows)
	}
}
