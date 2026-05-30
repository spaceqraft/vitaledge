package executor

import (
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

func TestCollectorSnapshot(t *testing.T) {
	collector := NewCollector()

	collector.ObserveStatement(ast.StatementKindMatchQuery, "ok", 10*time.Millisecond)
	collector.ObserveStatement(ast.StatementKindMatchQuery, "ok", 5*time.Millisecond)
	collector.ObserveRowsReturned(3)
	collector.ObserveIndexCandidate("acme", "User", "email", false)
	collector.ObserveIndexCandidate("acme", "User", "email", false)
	collector.ObserveIndexLookup("property_index", "miss", 0)
	collector.ObserveIndexLookup("property_index", "hit", 2)
	collector.ObserveDeleteCounter("rows_seen", 10)
	collector.ObserveDeleteCounter("edges_deleted", 7)

	snapshot := collector.Snapshot()

	stmt := snapshot.Statements[StatementMetricKey{Kind: ast.StatementKindMatchQuery, Outcome: "ok"}]
	if stmt.Count != 2 {
		t.Fatalf("expected statement count 2, got %d", stmt.Count)
	}
	if stmt.TotalDuration != 15*time.Millisecond {
		t.Fatalf("expected total duration 15ms, got %s", stmt.TotalDuration)
	}
	if snapshot.RowsReturned != 3 {
		t.Fatalf("expected rows returned 3, got %d", snapshot.RowsReturned)
	}
	candidateCount := snapshot.IndexCandidates[IndexCandidateKey{Tenant: "acme", Schema: "User", Property: "email", Indexed: false}]
	if candidateCount != 2 {
		t.Fatalf("expected candidate count 2, got %d", candidateCount)
	}
	lookup := snapshot.IndexLookups[IndexLookupKey{Strategy: "property_index", Outcome: "hit"}]
	if lookup.Count != 1 || lookup.TotalMatches != 2 {
		t.Fatalf("unexpected lookup aggregate: %#v", lookup)
	}
	if snapshot.DeleteCounters["rows_seen"] != 10 {
		t.Fatalf("expected rows_seen 10, got %d", snapshot.DeleteCounters["rows_seen"])
	}
	if snapshot.DeleteCounters["edges_deleted"] != 7 {
		t.Fatalf("expected edges_deleted 7, got %d", snapshot.DeleteCounters["edges_deleted"])
	}
}

func TestCollectorTopUnindexedCandidates(t *testing.T) {
	collector := NewCollector()
	collector.ObserveIndexCandidate("acme", "User", "email", false)
	collector.ObserveIndexCandidate("acme", "User", "email", false)
	collector.ObserveIndexCandidate("acme", "User", "name", false)
	collector.ObserveIndexCandidate("acme", "User", "id", true)

	top := collector.TopUnindexedCandidates(2)
	if len(top) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(top))
	}
	if top[0].Property != "email" || top[0].Count != 2 {
		t.Fatalf("unexpected first candidate: %#v", top[0])
	}
	if top[1].Property != "name" || top[1].Count != 1 {
		t.Fatalf("unexpected second candidate: %#v", top[1])
	}
}
