package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
)

// TestSeedDataLatencySlopeBounded guards the friend-suggestion seed workload
// against hidden graph-size-dependent growth in UNWIND..CREATE plus
// UNWIND..MATCH..MERGE ingest.
func TestSeedDataLatencySlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping seed data latency slope test in -short mode")
	}

	ctx := context.Background()
	store := newScanCountingStore(openBenchmarkStore(t))
	defer func() { _ = store.Close() }()

	peopleStmt, err := parser.ParseStatement("UNWIND $people AS person CREATE (p:Person {id: person.id, name: person.name})")
	if err != nil {
		t.Fatalf("parse people statement failed: %v", err)
	}
	edgesStmt, err := parser.ParseStatement("UNWIND $edges AS edge MATCH (a:Person {id: edge.source}) MATCH (b:Person {id: edge.target}) MERGE (a)-[:KNOWS]->(b)")
	if err != nil {
		t.Fatalf("parse edges statement failed: %v", err)
	}

	exec := New(store, Options{})

	const warmupBatches = 1
	const measuredBatches = 4
	const peopleBatchSize = 64
	work := make([]int64, 0, measuredBatches)

	for i := 0; i < warmupBatches+measuredBatches; i++ {
		people, edges := buildFriendSeedBatch(i, peopleBatchSize)

		if _, err := exec.ExecuteStatement(ctx, peopleStmt, Params{"tenant": benchmarkTenant, "people": people}); err != nil {
			t.Fatalf("people seed failed at batch %d: %v", i, err)
		}

		startScanned := store.scannedVertices()
		if _, err := exec.ExecuteStatement(ctx, edgesStmt, Params{"tenant": benchmarkTenant, "edges": edges}); err != nil {
			t.Fatalf("edges seed failed at batch %d: %v", i, err)
		}
		if i >= warmupBatches {
			work = append(work, store.scannedVertices()-startScanned)
		}
	}

	firstMedian := medianInt64(work[:2])
	lastMedian := medianInt64(work[len(work)-2:])
	if firstMedian < 0 {
		t.Fatalf("invalid first median work: %d", firstMedian)
	}
	if firstMedian == 0 && lastMedian > 0 {
		t.Fatalf("seed_data scan work regressed from 0 to %d (work=%v)", lastMedian, work)
	}
	if firstMedian == 0 {
		t.Logf("seed_data scan-work check: first_median=%d last_median=%d ratio=1.00x work=%v", firstMedian, lastMedian, work)
		return
	}

	ratio := float64(lastMedian) / float64(firstMedian)
	t.Logf("seed_data scan-work slope check: first_median=%d last_median=%d ratio=%.2fx work=%v", firstMedian, lastMedian, ratio, work)

	const maxAllowedGrowthRatio = 2.0
	if ratio > maxAllowedGrowthRatio {
		t.Fatalf("seed_data scan work growth too high: ratio %.2fx exceeds %.2fx (work=%v)", ratio, maxAllowedGrowthRatio, work)
	}
}

func buildFriendSeedBatch(iter, peopleBatchSize int) ([]map[string]any, []map[string]any) {
	people := make([]map[string]any, 0, peopleBatchSize)
	edges := make([]map[string]any, 0, peopleBatchSize)
	base := iter * peopleBatchSize

	for j := 0; j < peopleBatchSize; j++ {
		id := fmt.Sprintf("p-%d", base+j)
		people = append(people, map[string]any{
			"id":   id,
			"name": fmt.Sprintf("Person %d", base+j),
		})
		if j > 0 {
			edges = append(edges, map[string]any{
				"source": fmt.Sprintf("p-%d", base+j-1),
				"target": id,
			})
		}
	}

	if iter > 0 {
		edges = append(edges, map[string]any{
			"source": fmt.Sprintf("p-%d", base-1),
			"target": fmt.Sprintf("p-%d", base),
		})
	}

	return people, edges
}
