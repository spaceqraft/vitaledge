package executor

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/indexschema"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

// TestIngestPairLatencySlopeBounded guards against hidden graph-size-dependent
// growth in the mixed write path used by ingest pipelines.
func TestIngestPairLatencySlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ingest latency slope test in -short mode")
	}

	ctx := context.Background()
	store := newScanCountingStore(openBenchmarkStore(t))
	defer func() { _ = store.Close() }()

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex(benchmarkTenant, "Movie", "movie_id")
	catalog.AddPropertyIndex(benchmarkTenant, "Genre", "genre")

	if err := seedBenchmarkGraph(ctx, store, true); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	movieStmt, err := parser.ParseStatement("UNWIND $movies AS m MERGE (mov:Movie {movie_id: m.movie_id}) SET mov.title = m.title, mov.year = m.year")
	if err != nil {
		t.Fatalf("parse movies statement failed: %v", err)
	}
	pairStmt, err := parser.ParseStatement("UNWIND $pairs AS p MATCH (mov:Movie {movie_id: p.movie_id}) MERGE (g:Genre {genre: p.genre}) MERGE (mov)-[:GENRED]->(g)")
	if err != nil {
		t.Fatalf("parse pairs statement failed: %v", err)
	}

	exec := New(store, Options{IndexCatalog: catalog})

	// Keep this guard lightweight while asserting work performed, not wall time.
	const warmupBatches = 1
	const measuredBatches = 3
	const pairSampleSize = 16
	work := make([]int64, 0, measuredBatches)

	for i := 0; i < warmupBatches+measuredBatches; i++ {
		movies, pairs, _ := buildBenchmarkIngestBatch(i)
		if len(pairs) > pairSampleSize {
			pairs = pairs[:pairSampleSize]
		}

		startScanned := store.scannedVertices()
		if _, err := exec.ExecuteStatement(ctx, movieStmt, Params{"tenant": benchmarkTenant, "movies": movies}); err != nil {
			t.Fatalf("movies ingest failed at batch %d: %v", i, err)
		}

		if _, err := exec.ExecuteStatement(ctx, pairStmt, Params{"tenant": benchmarkTenant, "pairs": pairs}); err != nil {
			t.Fatalf("pairs ingest failed at batch %d: %v", i, err)
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
		t.Fatalf("pair ingest scan work regressed from 0 to %d (work=%v)", lastMedian, work)
	}
	if firstMedian == 0 {
		t.Logf("pair scan-work check: first_median=%d last_median=%d ratio=1.00x work=%v", firstMedian, lastMedian, work)
		return
	}

	ratio := float64(lastMedian) / float64(firstMedian)
	t.Logf("pair scan-work slope check: first_median=%d last_median=%d ratio=%.2fx work=%v", firstMedian, lastMedian, ratio, work)

	const maxAllowedGrowthRatio = 2.0
	if ratio > maxAllowedGrowthRatio {
		t.Fatalf("pair ingest scan work growth too high: ratio %.2fx exceeds %.2fx (work=%v)", ratio, maxAllowedGrowthRatio, work)
	}
}

// TestIngestRatingLatencySlopeBounded guards against hidden graph-size-dependent
// growth in the mixed write rating ingest path.
func TestIngestRatingLatencySlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ingest latency slope test in -short mode")
	}

	ctx := context.Background()
	store := newScanCountingStore(openBenchmarkStore(t))
	defer func() { _ = store.Close() }()

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex(benchmarkTenant, "Movie", "movie_id")
	catalog.AddPropertyIndex(benchmarkTenant, "User", "user_id")

	if err := seedBenchmarkGraph(ctx, store, true); err != nil {
		t.Fatalf("seed graph failed: %v", err)
	}

	movieStmt, err := parser.ParseStatement("UNWIND $movies AS m MERGE (mov:Movie {movie_id: m.movie_id}) SET mov.title = m.title, mov.year = m.year")
	if err != nil {
		t.Fatalf("parse movies statement failed: %v", err)
	}
	ratingStmt, err := parser.ParseStatement("UNWIND $ratings AS r MERGE (u:User {user_id: r.user_id}) WITH u, r MATCH (mov:Movie {movie_id: r.movie_id}) MERGE (u)-[rated:RATED]->(mov) SET rated.rating = r.rating, rated.ts = r.ts")
	if err != nil {
		t.Fatalf("parse ratings statement failed: %v", err)
	}

	exec := New(store, Options{IndexCatalog: catalog})

	// Keep this guard lightweight while asserting work performed, not wall time.
	const warmupBatches = 1
	const measuredBatches = 3
	const ratingSampleSize = 16
	work := make([]int64, 0, measuredBatches)

	for i := 0; i < warmupBatches+measuredBatches; i++ {
		movies, _, ratings := buildBenchmarkIngestBatch(i)
		if len(ratings) > ratingSampleSize {
			ratings = ratings[:ratingSampleSize]
		}

		startScanned := store.scannedVertices()
		if _, err := exec.ExecuteStatement(ctx, movieStmt, Params{"tenant": benchmarkTenant, "movies": movies}); err != nil {
			t.Fatalf("movies ingest failed at batch %d: %v", i, err)
		}

		if _, err := exec.ExecuteStatement(ctx, ratingStmt, Params{"tenant": benchmarkTenant, "ratings": ratings}); err != nil {
			t.Fatalf("ratings ingest failed at batch %d: %v", i, err)
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
		t.Fatalf("rating ingest scan work regressed from 0 to %d (work=%v)", lastMedian, work)
	}
	if firstMedian == 0 {
		t.Logf("rating scan-work check: first_median=%d last_median=%d ratio=1.00x work=%v", firstMedian, lastMedian, work)
		return
	}

	ratio := float64(lastMedian) / float64(firstMedian)
	t.Logf("rating scan-work slope check: first_median=%d last_median=%d ratio=%.2fx work=%v", firstMedian, lastMedian, ratio, work)

	const maxAllowedGrowthRatio = 2.0
	if ratio > maxAllowedGrowthRatio {
		t.Fatalf("rating ingest scan work growth too high: ratio %.2fx exceeds %.2fx (work=%v)", ratio, maxAllowedGrowthRatio, work)
	}
}

func medianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]int64(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool {
		return copyValues[i] < copyValues[j]
	})
	mid := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[mid]
	}
	return (copyValues[mid-1] + copyValues[mid]) / 2
}

type scanCountingStore struct {
	inner           graph.GraphStore
	scannedVertexes atomic.Int64
	scannedAdj      atomic.Int64
}

func newScanCountingStore(inner graph.GraphStore) *scanCountingStore {
	return &scanCountingStore{inner: inner}
}

func (s *scanCountingStore) BeginTx(ctx context.Context, opts graph.TxOptions) (graph.Tx, error) {
	tx, err := s.inner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &scanCountingTx{Tx: tx, store: s}, nil
}

func (s *scanCountingStore) View(ctx context.Context, fn func(graph.Tx) error) error {
	return s.inner.View(ctx, func(tx graph.Tx) error {
		return fn(&scanCountingTx{Tx: tx, store: s})
	})
}

func (s *scanCountingStore) Update(ctx context.Context, fn func(graph.Tx) error) error {
	return s.inner.Update(ctx, func(tx graph.Tx) error {
		return fn(&scanCountingTx{Tx: tx, store: s})
	})
}

func (s *scanCountingStore) Close() error {
	return s.inner.Close()
}

func (s *scanCountingStore) scannedVertices() int64 {
	return s.scannedVertexes.Load()
}

func (s *scanCountingStore) scannedAdjacencyItems() int64 {
	return s.scannedAdj.Load()
}

type scanCountingTx struct {
	graph.Tx
	store *scanCountingStore
}

func (t *scanCountingTx) ScanVertices(ctx context.Context, tenant string, limit int, fn func(*graph.Vertex) error) error {
	return t.Tx.ScanVertices(ctx, tenant, limit, func(vertex *graph.Vertex) error {
		t.store.scannedVertexes.Add(1)
		if fn == nil {
			return nil
		}
		return fn(vertex)
	})
}

func (t *scanCountingTx) ScanVerticesFrom(ctx context.Context, tenant, startAfterVertexID string, limit int, fn func(*graph.Vertex) error) error {
	return t.Tx.ScanVerticesFrom(ctx, tenant, startAfterVertexID, limit, func(vertex *graph.Vertex) error {
		t.store.scannedVertexes.Add(1)
		if fn == nil {
			return nil
		}
		return fn(vertex)
	})
}

func (t *scanCountingTx) ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*graph.Edge) error) error {
	return t.Tx.ScanOutEdges(ctx, tenant, srcID, edgeType, limit, func(edge *graph.Edge) error {
		t.store.scannedAdj.Add(1)
		if fn == nil {
			return nil
		}
		return fn(edge)
	})
}

func (t *scanCountingTx) ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*graph.Edge) error) error {
	return t.Tx.ScanInEdges(ctx, tenant, dstID, edgeType, limit, func(edge *graph.Edge) error {
		t.store.scannedAdj.Add(1)
		if fn == nil {
			return nil
		}
		return fn(edge)
	})
}

func (t *scanCountingTx) ScanOutEdgeSourceIDs(ctx context.Context, tenant, edgeType string, limit int, fn func(string) error) error {
	return t.Tx.ScanOutEdgeSourceIDs(ctx, tenant, edgeType, limit, func(sourceID string) error {
		t.store.scannedAdj.Add(1)
		if fn == nil {
			return nil
		}
		return fn(sourceID)
	})
}

func (t *scanCountingTx) ScanOutEdgeLinksByType(ctx context.Context, tenant, edgeType string, limit int, fn func(srcID, edgeID, dstID string) error) error {
	return t.Tx.ScanOutEdgeLinksByType(ctx, tenant, edgeType, limit, func(srcID, edgeID, dstID string) error {
		t.store.scannedAdj.Add(1)
		if fn == nil {
			return nil
		}
		return fn(srcID, edgeID, dstID)
	})
}
