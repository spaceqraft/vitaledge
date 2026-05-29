package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

const (
	benchmarkTenant          = "acme"
	benchmarkBaselineMovies  = 5000
	benchmarkBaselineUsers   = 2000
	benchmarkGenreCount      = 32
	benchmarkMovieBatchSize  = 128
	benchmarkPairBatchSize   = 128
	benchmarkRatingBatchSize = 256
)

func BenchmarkUnwindMergeIngestIndexedVsNonIndexed(b *testing.B) {
	for _, tc := range []struct {
		name    string
		indexed bool
	}{
		{name: "with_property_indexes", indexed: true},
		{name: "without_property_indexes", indexed: false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkUnwindMergeIngest(b, tc.indexed)
		})
	}
}

func benchmarkUnwindMergeIngest(b *testing.B, indexed bool) {
	ctx := context.Background()
	store := openBenchmarkStore(b)
	b.Cleanup(func() { _ = store.Close() })

	catalog := indexschema.NewCatalog()
	if indexed {
		catalog.AddPropertyIndex(benchmarkTenant, "Movie", "movie_id")
		catalog.AddPropertyIndex(benchmarkTenant, "User", "user_id")
		catalog.AddPropertyIndex(benchmarkTenant, "Genre", "genre")
	}

	if err := seedBenchmarkGraph(ctx, store, indexed); err != nil {
		b.Fatalf("seed graph failed: %v", err)
	}

	movieStmt, err := parser.ParseStatement("UNWIND $movies AS m MERGE (mov:Movie {movie_id: m.movie_id}) SET mov.title = m.title, mov.year = m.year")
	if err != nil {
		b.Fatalf("parse movies statement failed: %v", err)
	}
	pairStmt, err := parser.ParseStatement("UNWIND $pairs AS p MATCH (mov:Movie {movie_id: p.movie_id}) MERGE (g:Genre {genre: p.genre}) MERGE (mov)-[:GENRED]->(g)")
	if err != nil {
		b.Fatalf("parse pairs statement failed: %v", err)
	}
	ratingStmt, err := parser.ParseStatement("UNWIND $ratings AS r MERGE (u:User {user_id: r.user_id}) WITH u, r MATCH (mov:Movie {movie_id: r.movie_id}) MERGE (u)-[rated:RATED]->(mov) SET rated.rating = r.rating, rated.ts = r.ts")
	if err != nil {
		b.Fatalf("parse ratings statement failed: %v", err)
	}

	exec := New(store, Options{IndexCatalog: catalog})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		movies, pairs, ratings := buildBenchmarkIngestBatch(i)

		if _, err := exec.ExecuteStatement(ctx, movieStmt, Params{"tenant": benchmarkTenant, "movies": movies}); err != nil {
			b.Fatalf("movies ingest failed at iter %d: %v", i, err)
		}
		if _, err := exec.ExecuteStatement(ctx, pairStmt, Params{"tenant": benchmarkTenant, "pairs": pairs}); err != nil {
			b.Fatalf("pairs ingest failed at iter %d: %v", i, err)
		}
		if _, err := exec.ExecuteStatement(ctx, ratingStmt, Params{"tenant": benchmarkTenant, "ratings": ratings}); err != nil {
			b.Fatalf("ratings ingest failed at iter %d: %v", i, err)
		}
	}

	rowsPerIter := benchmarkMovieBatchSize + benchmarkPairBatchSize + benchmarkRatingBatchSize
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(rowsPerIter*b.N)/elapsed, "rows/s")
	}
}

func openBenchmarkStore(tb testing.TB) graph.GraphStore {
	tb.Helper()
	base := tb.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		tb.Fatalf("mkdir failed: %v", err)
	}
	store, err := pebblestore.Open(dbPath)
	if err != nil {
		tb.Fatalf("open store failed: %v", err)
	}
	return store
}

func seedBenchmarkGraph(ctx context.Context, store graph.GraphStore, indexed bool) error {
	return store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < benchmarkBaselineMovies; i++ {
			vertexID := fmt.Sprintf("movie-%d", i)
			movieID := strconv.Itoa(i)
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant: benchmarkTenant,
				ID:     vertexID,
				Labels: []string{"Movie"},
				Properties: graph.PropertyMap{
					"movie_id": []byte(movieID),
					"title":    []byte(fmt.Sprintf("Movie %d", i)),
					"year":     []byte(strconv.Itoa(1980 + (i % 40))),
				},
			}); err != nil {
				return err
			}
			if indexed {
				if err := tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
					Tenant:      benchmarkTenant,
					Schema:      "Movie",
					Property:    "movie_id",
					Value:       []byte(movieID),
					EntityID:    vertexID,
					EntityClass: "vertex",
				}); err != nil {
					return err
				}
			}
		}

		for i := 0; i < benchmarkBaselineUsers; i++ {
			vertexID := fmt.Sprintf("user-%d", i)
			userID := strconv.Itoa(i)
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant: benchmarkTenant,
				ID:     vertexID,
				Labels: []string{"User"},
				Properties: graph.PropertyMap{
					"user_id": []byte(userID),
				},
			}); err != nil {
				return err
			}
			if indexed {
				if err := tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
					Tenant:      benchmarkTenant,
					Schema:      "User",
					Property:    "user_id",
					Value:       []byte(userID),
					EntityID:    vertexID,
					EntityClass: "vertex",
				}); err != nil {
					return err
				}
			}
		}

		for i := 0; i < benchmarkGenreCount; i++ {
			vertexID := fmt.Sprintf("genre-%d", i)
			genre := fmt.Sprintf("Genre-%02d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant: benchmarkTenant,
				ID:     vertexID,
				Labels: []string{"Genre"},
				Properties: graph.PropertyMap{
					"genre": []byte(genre),
				},
			}); err != nil {
				return err
			}
			if indexed {
				if err := tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
					Tenant:      benchmarkTenant,
					Schema:      "Genre",
					Property:    "genre",
					Value:       []byte(genre),
					EntityID:    vertexID,
					EntityClass: "vertex",
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func buildBenchmarkIngestBatch(iter int) ([]map[string]any, []map[string]any, []map[string]any) {
	movies := make([]map[string]any, 0, benchmarkMovieBatchSize)
	pairs := make([]map[string]any, 0, benchmarkPairBatchSize)
	ratings := make([]map[string]any, 0, benchmarkRatingBatchSize)
	movieIDs := make([]int, 0, benchmarkMovieBatchSize)

	for j := 0; j < benchmarkMovieBatchSize; j++ {
		movieID := 0
		if j%2 == 0 {
			movieID = (iter + j) % benchmarkBaselineMovies
		} else {
			movieID = benchmarkBaselineMovies + (iter * benchmarkMovieBatchSize) + j
		}
		movieIDs = append(movieIDs, movieID)
		movies = append(movies, map[string]any{
			"movie_id": movieID,
			"title":    fmt.Sprintf("Movie %d", movieID),
			"year":     1980 + (movieID % 40),
		})
	}

	for j := 0; j < benchmarkPairBatchSize; j++ {
		movieID := movieIDs[j%len(movieIDs)]
		genre := fmt.Sprintf("Genre-%02d", (iter+j)%benchmarkGenreCount)
		pairs = append(pairs, map[string]any{
			"movie_id": movieID,
			"genre":    genre,
		})
	}

	for j := 0; j < benchmarkRatingBatchSize; j++ {
		userID := 0
		if j%2 == 0 {
			userID = (iter + j) % benchmarkBaselineUsers
		} else {
			userID = benchmarkBaselineUsers + (iter * benchmarkRatingBatchSize) + j
		}
		movieID := movieIDs[j%len(movieIDs)]
		ratings = append(ratings, map[string]any{
			"user_id":  userID,
			"movie_id": movieID,
			"rating":   (j % 5) + 1,
			"ts":       int64(1700000000 + (iter * 1000) + j),
		})
	}

	return movies, pairs, ratings
}
