package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

// TestPrintSuggestedFriendsScanWorkSlopeBounded guards the print_suggested_friends
// query against hidden superlinear scan-work growth.
func TestPrintSuggestedFriendsScanWorkSlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping print_suggested_friends slope test in -short mode")
	}

	ctx := context.Background()
	query := "MATCH (a:Person)-[:SUGGESTED_FRIEND]->(suggested:Person) RETURN a.name AS person, collect(DISTINCT suggested.name) AS suggested_friends ORDER BY person"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse print_suggested_friends query failed: %v", err)
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

		if err := seedPrintSuggestedFriendsGraph(ctx, store, benchmarkTenant, count); err != nil {
			_ = store.Close()
			t.Fatalf("seed print_suggested_friends graph failed for count=%d: %v", count, err)
		}

		startWork := store.scannedAdjacencyItems()
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": benchmarkTenant, "emit_runtime_counters": true})
		if err != nil {
			_ = store.Close()
			t.Fatalf("execute print_suggested_friends failed for count=%d: %v", count, err)
		}
		counters, err := runtimeCountersFromWarnings(res.Warnings)
		if err != nil {
			_ = store.Close()
			t.Fatalf("decode runtime counters failed for count=%d: %v", count, err)
		}
		if counters["runtime.suggested_friends.print.fastpath_applied"] <= 0 {
			_ = store.Close()
			t.Fatalf("expected print fast path counter > 0 for count=%d, counters=%v", count, counters)
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
	t.Logf("print_suggested_friends scan-work slope: first=%d work=%d norm=%.2f last=%d work=%d norm=%.2f ratio=%.2fx", first.count, first.rawWork, first.normalizedWork, last.count, last.rawWork, last.normalizedWork, ratio)

	const maxAllowedNormalizedGrowthRatio = 3.0
	if ratio > maxAllowedNormalizedGrowthRatio {
		t.Fatalf("print_suggested_friends normalized scan work growth too high: %.2fx exceeds %.2fx (samples=%#v)", ratio, maxAllowedNormalizedGrowthRatio, samples)
	}
}

func seedPrintSuggestedFriendsGraph(ctx context.Context, store graph.GraphStore, tenant string, count int) error {
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
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{v}); err != nil {
				return err
			}
		}

		edgeSeq := 0
		nextEdgeID := func() string {
			edgeSeq++
			return fmt.Sprintf("s-%d", edgeSeq)
		}

		// Each person gets two suggestions to keep per-vertex out-degree bounded.
		for i := 0; i < count; i++ {
			src := fmt.Sprintf("p-%d", i)
			dst1 := fmt.Sprintf("p-%d", (i+2)%count)
			dst2 := fmt.Sprintf("p-%d", (i+3)%count)

			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: tenant, ID: nextEdgeID(), Type: "SUGGESTED_FRIEND", SrcID: src, DstID: dst1}}); err != nil {
				return err
			}
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: tenant, ID: nextEdgeID(), Type: "SUGGESTED_FRIEND", SrcID: src, DstID: dst2}}); err != nil {
				return err
			}
		}
		return nil
	})
}

// BenchmarkCollectDistinctSuggestedFriendsLargeGraph measures collect(DISTINCT)
// query latency and allocation behavior as graph size scales.
func BenchmarkCollectDistinctSuggestedFriendsLargeGraph(b *testing.B) {
	ctx := context.Background()
	query := "MATCH (a:Person)-[:SUGGESTED_FRIEND]->(suggested:Person) RETURN a.name AS person, collect(DISTINCT suggested.name) AS suggested_friends ORDER BY person"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		b.Fatalf("parse print_suggested_friends query failed: %v", err)
	}

	for _, count := range []int{10000, 25000} {
		count := count
		b.Run(fmt.Sprintf("vertices_%d", count), func(b *testing.B) {
			store := openBenchmarkStore(b)
			defer func() { _ = store.Close() }()

			if err := seedPrintSuggestedFriendsGraph(ctx, store, benchmarkTenant, count); err != nil {
				b.Fatalf("seed print_suggested_friends graph failed for count=%d: %v", count, err)
			}

			exec := New(store, Options{})
			params := Params{"tenant": benchmarkTenant}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := exec.ExecuteStatement(ctx, stmt, params)
				if err != nil {
					b.Fatalf("execute print_suggested_friends failed for count=%d: %v", count, err)
				}
				if len(res.Rows) != count {
					b.Fatalf("expected %d rows, got %d", count, len(res.Rows))
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
