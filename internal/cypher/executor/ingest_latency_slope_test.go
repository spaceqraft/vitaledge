package executor

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/cypher/parser"
)

// TestIngestPairLatencySlopeBounded guards against hidden graph-size-dependent
// growth in the mixed write path used by ingest pipelines.
func TestIngestPairLatencySlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ingest latency slope test in -short mode")
	}

	ctx := context.Background()
	store := openBenchmarkStore(t)
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

	const warmupBatches = 2
	const measuredBatches = 10
	latencies := make([]time.Duration, 0, measuredBatches)

	for i := 0; i < warmupBatches+measuredBatches; i++ {
		movies, pairs, _ := buildBenchmarkIngestBatch(i)

		if _, err := exec.ExecuteStatement(ctx, movieStmt, Params{"tenant": benchmarkTenant, "movies": movies}); err != nil {
			t.Fatalf("movies ingest failed at batch %d: %v", i, err)
		}

		started := time.Now()
		if _, err := exec.ExecuteStatement(ctx, pairStmt, Params{"tenant": benchmarkTenant, "pairs": pairs}); err != nil {
			t.Fatalf("pairs ingest failed at batch %d: %v", i, err)
		}
		if i >= warmupBatches {
			latencies = append(latencies, time.Since(started))
		}
	}

	firstMedian := medianDuration(latencies[:3])
	lastMedian := medianDuration(latencies[len(latencies)-3:])
	if firstMedian <= 0 {
		t.Fatalf("invalid first median latency: %v", firstMedian)
	}

	ratio := float64(lastMedian) / float64(firstMedian)
	t.Logf("pair latency slope check: first_median=%s last_median=%s ratio=%.2fx latencies=%v", firstMedian, lastMedian, ratio, latencies)

	const maxAllowedGrowthRatio = 2.0
	if ratio > maxAllowedGrowthRatio {
		t.Fatalf("pair ingest latency growth too high: ratio %.2fx exceeds %.2fx", ratio, maxAllowedGrowthRatio)
	}
}

// TestIngestRatingLatencySlopeBounded guards against hidden graph-size-dependent
// growth in the mixed write rating ingest path.
func TestIngestRatingLatencySlopeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ingest latency slope test in -short mode")
	}

	ctx := context.Background()
	store := openBenchmarkStore(t)
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

	const warmupBatches = 2
	const measuredBatches = 10
	latencies := make([]time.Duration, 0, measuredBatches)

	for i := 0; i < warmupBatches+measuredBatches; i++ {
		movies, _, ratings := buildBenchmarkIngestBatch(i)

		if _, err := exec.ExecuteStatement(ctx, movieStmt, Params{"tenant": benchmarkTenant, "movies": movies}); err != nil {
			t.Fatalf("movies ingest failed at batch %d: %v", i, err)
		}

		started := time.Now()
		if _, err := exec.ExecuteStatement(ctx, ratingStmt, Params{"tenant": benchmarkTenant, "ratings": ratings}); err != nil {
			t.Fatalf("ratings ingest failed at batch %d: %v", i, err)
		}
		if i >= warmupBatches {
			latencies = append(latencies, time.Since(started))
		}
	}

	firstMedian := medianDuration(latencies[:3])
	lastMedian := medianDuration(latencies[len(latencies)-3:])
	if firstMedian <= 0 {
		t.Fatalf("invalid first median latency: %v", firstMedian)
	}

	ratio := float64(lastMedian) / float64(firstMedian)
	t.Logf("rating latency slope check: first_median=%s last_median=%s ratio=%.2fx latencies=%v", firstMedian, lastMedian, ratio, latencies)

	const maxAllowedGrowthRatio = 2.0
	if ratio > maxAllowedGrowthRatio {
		t.Fatalf("rating ingest latency growth too high: ratio %.2fx exceeds %.2fx", ratio, maxAllowedGrowthRatio)
	}
}

func medianDuration(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]time.Duration(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool {
		return copyValues[i] < copyValues[j]
	})
	mid := len(copyValues) / 2
	if len(copyValues)%2 == 1 {
		return copyValues[mid]
	}
	return (copyValues[mid-1] + copyValues[mid]) / 2
}
