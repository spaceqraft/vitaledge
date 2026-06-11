package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

// TestSuggestFriendsScanWorkSlopeBounded guards the optimized suggest_friends
// query against hidden superlinear scan-work growth.
func TestSuggestFriendsScanWorkSlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping suggest_friends slope test in -short mode")
	}

	ctx := context.Background()
	query := "MATCH (a:Person)-[:KNOWS]->(peer:Person)-[:KNOWS]->(suggested:Person) WHERE suggested <> a AND NOT (a)-[:KNOWS]-(suggested) WITH DISTINCT a, suggested MERGE (a)-[:SUGGESTED_FRIEND]->(suggested)"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse suggest_friends query failed: %v", err)
	}

	type sample struct {
		count          int
		normalizedWork float64
		rawWork        int64
	}
	samples := make([]sample, 0, 3)

	for _, count := range []int{100, 1000, 10000} {
		store := newScanCountingStore(openBenchmarkStore(t))
		exec := New(store, Options{})

		if err := seedSuggestFriendsGraph(ctx, store, benchmarkTenant, count); err != nil {
			_ = store.Close()
			t.Fatalf("seed suggest_friends graph failed for count=%d: %v", count, err)
		}

		startWork := store.scannedAdjacencyItems()
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": benchmarkTenant}); err != nil {
			_ = store.Close()
			t.Fatalf("execute suggest_friends failed for count=%d: %v", count, err)
		}
		work := store.scannedAdjacencyItems() - startWork
		if err := store.Close(); err != nil {
			t.Fatalf("close store failed for count=%d: %v", count, err)
		}

		normalized := float64(work) / float64(count)
		samples = append(samples, sample{count: count, normalizedWork: normalized, rawWork: work})
	}

	first := samples[0]
	last := samples[len(samples)-1]
	if first.normalizedWork <= 0 {
		t.Fatalf("invalid normalized scan work at first sample: %#v", first)
	}

	ratio := last.normalizedWork / first.normalizedWork
	t.Logf("suggest_friends scan-work slope: first=%d work=%d norm=%.2f last=%d work=%d norm=%.2f ratio=%.2fx", first.count, first.rawWork, first.normalizedWork, last.count, last.rawWork, last.normalizedWork, ratio)

	const maxAllowedNormalizedGrowthRatio = 3.0
	if ratio > maxAllowedNormalizedGrowthRatio {
		t.Fatalf("suggest_friends normalized scan work growth too high: %.2fx exceeds %.2fx (samples=%#v)", ratio, maxAllowedNormalizedGrowthRatio, samples)
	}
}

func seedSuggestFriendsGraph(ctx context.Context, store graph.GraphStore, tenant string, count int) error {
	if count < 4 {
		count = 4
	}
	return store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < count; i++ {
			v := &graph.Vertex{
				Tenant: tenant,
				ID:     fmt.Sprintf("p-%d", i),
				Labels: []string{"Person"},
				Properties: map[string][]byte{
					"name": valueToBytes(fmt.Sprintf("Person %d", i)),
				},
			}
			if err := tx.PutVertex(ctx, v); err != nil {
				return err
			}
		}

		edgeSeq := 0
		nextEdgeID := func() string {
			edgeSeq++
			return fmt.Sprintf("k-%d", edgeSeq)
		}

		// Each person knows the next two people, wrapping around.
		for i := 0; i < count; i++ {
			src := fmt.Sprintf("p-%d", i)
			dst1 := fmt.Sprintf("p-%d", (i+1)%count)
			dst2 := fmt.Sprintf("p-%d", (i+2)%count)

			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "KNOWS", SrcID: src, DstID: dst1}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "KNOWS", SrcID: src, DstID: dst2}); err != nil {
				return err
			}
		}
		return nil
	})
}
