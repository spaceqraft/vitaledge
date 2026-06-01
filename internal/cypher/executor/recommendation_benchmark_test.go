package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

const recommendationBenchmarkQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
const recommendationBenchmarkSelectiveQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"

var recommendationBenchmarkMultiTargetQuery = strings.Replace(recommendationBenchmarkQuery, "{user_id: 1}", "{cohort: 1}", 1)

type recommendationBenchmarkMode int

const (
	recommendationBenchmarkModeStep1TopK recommendationBenchmarkMode = iota
	recommendationBenchmarkModeStep2LateMaterialization
	recommendationBenchmarkModeStep2IndexPushdownBaseline
	recommendationBenchmarkModeStep2IndexPushdownEnabled
)

type recommendationBenchmarkTargetMode int

const (
	recommendationBenchmarkTargetModeSingle recommendationBenchmarkTargetMode = iota
	recommendationBenchmarkTargetModeMulti
)

type recommendationBenchmarkStage1ExpansionMode int

const (
	recommendationBenchmarkStage1ExpansionAuto recommendationBenchmarkStage1ExpansionMode = iota
	recommendationBenchmarkStage1ExpansionNested
	recommendationBenchmarkStage1ExpansionSharedSeed
)

func BenchmarkRecommendationQueryStep1TopKPushdown(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep1TopK, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto)
}

func BenchmarkRecommendationQueryStep2LateMaterialization(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2LateMaterialization, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto)
}

func BenchmarkRecommendationQueryMultiTargetStage1NestedBaseline(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2LateMaterialization, recommendationBenchmarkTargetModeMulti, recommendationBenchmarkStage1ExpansionNested)
}

func BenchmarkRecommendationQueryMultiTargetStage1SharedSeedExpansion(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2LateMaterialization, recommendationBenchmarkTargetModeMulti, recommendationBenchmarkStage1ExpansionSharedSeed)
}

func BenchmarkRecommendationQueryStep2IndexPushdownBaseline(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2IndexPushdownBaseline, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto)
}

func BenchmarkRecommendationQueryStep2IndexPushdownEnabled(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2IndexPushdownEnabled, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto)
}

func BenchmarkRecommendationQuerySelectiveStep2IndexPushdownBaseline(b *testing.B) {
	benchmarkRecommendationQueryWithQuery(b, recommendationBenchmarkModeStep2IndexPushdownBaseline, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto, recommendationBenchmarkSelectiveQuery)
}

func BenchmarkRecommendationQuerySelectiveStep2IndexPushdownEnabled(b *testing.B) {
	benchmarkRecommendationQueryWithQuery(b, recommendationBenchmarkModeStep2IndexPushdownEnabled, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkStage1ExpansionAuto, recommendationBenchmarkSelectiveQuery)
}

func benchmarkRecommendationQuery(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode, stage1ExpansionMode recommendationBenchmarkStage1ExpansionMode) {
	benchmarkRecommendationQueryWithQuery(b, mode, targetMode, stage1ExpansionMode, "")
}

func benchmarkRecommendationQueryWithQuery(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode, stage1ExpansionMode recommendationBenchmarkStage1ExpansionMode, queryOverride string) {
	ctx := context.Background()
	store := openBenchmarkStore(b)
	defer func() { _ = store.Close() }()

	if err := seedRecommendationBenchmarkGraph(ctx, store); err != nil {
		b.Fatalf("seed benchmark graph failed: %v", err)
	}

	query := recommendationBenchmarkQuery
	if strings.TrimSpace(queryOverride) != "" {
		query = queryOverride
	}
	if targetMode == recommendationBenchmarkTargetModeMulti {
		query = recommendationBenchmarkMultiTargetQuery
	}
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		b.Fatalf("parse failed: %v", err)
	}

	collector := NewCollector()
	catalog := indexschema.NewCatalog()
	opts := Options{Metrics: collector, IndexCatalog: catalog}
	if mode == recommendationBenchmarkModeStep1TopK {
		opts.DisableStage2LateMaterialization = true
	}
	if mode == recommendationBenchmarkModeStep2IndexPushdownBaseline {
		opts.DisableStage2EdgeIndexPushdown = true
	}
	if stage1ExpansionMode == recommendationBenchmarkStage1ExpansionNested {
		opts.DisableStage1SharedSeedExpansion = true
	}
	exec := New(store, opts)
	if mode == recommendationBenchmarkModeStep2IndexPushdownBaseline || mode == recommendationBenchmarkModeStep2IndexPushdownEnabled {
		catalog.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
		if _, err := exec.BackfillEdgePropertyIndex(ctx, "bench-rec", "RATED", "rating"); err != nil {
			b.Fatalf("backfill edge index failed: %v", err)
		}
	}
	params := Params{"tenant": "bench-rec"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.ExecuteStatement(ctx, stmt, params)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) == 0 {
			b.Fatalf("expected non-empty recommendation rows")
		}
	}
	b.StopTimer()

	snapshot := collector.Snapshot()
	runtimeCounters := snapshot.RuntimeCounters
	reportCounterPerOp := func(metricName, counterName string) {
		if b.N <= 0 {
			return
		}
		b.ReportMetric(float64(runtimeCounters[counterName])/float64(b.N), metricName)
	}
	reportCounterPerOp("stage1_first_edges/op", "fast_path.stage1.first_hop.edges_visited")
	reportCounterPerOp("stage1_second_edges/op", "fast_path.stage1.second_hop.edges_visited")
	reportCounterPerOp("stage1_shared_seed_rows/op", "fast_path.stage1.first_hop.shared_seed_rows")
	reportCounterPerOp("stage1_shared_vertices/op", "fast_path.stage1.shared_vertices_seeded")
	reportCounterPerOp("stage1_seed_rows_considered/op", "fast_path.stage1.seed_rows_considered")
	reportCounterPerOp("stage1_seed_rows_bucket_dropped/op", "fast_path.stage1.seed_rows_bucket_dropped")
	reportCounterPerOp("stage1_where_shortcuts/op", "fast_path.stage1.where_eval_shortcuts")
	reportCounterPerOp("stage1_where_checks/op", "fast_path.stage1.where_eval_checks")
	reportCounterPerOp("stage2_edges_visited/op", "fast_path.stage2.edges_visited")
	reportCounterPerOp("stage2_index_candidates_total/op", "fast_path.stage2.index_candidates_total")
	reportCounterPerOp("stage2_index_pushdown_rows/op", "fast_path.stage2.index_pushdown_rows")
	reportCounterPerOp("stage2_index_edges_considered/op", "fast_path.stage2.index_edges_considered")
	reportCounterPerOp("stage2_index_pushdown_skipped_unselective/op", "fast_path.stage2.index_pushdown_skipped_unselective")
	reportCounterPerOp("stage2_index_pushdown_skipped_predicate_shape/op", "fast_path.stage2.index_pushdown_skipped_predicate_shape")
	reportCounterPerOp("stage2_index_cache_hits/op", "fast_path.stage2.index_lookup_cache_hits")
	reportCounterPerOp("stage2_index_cache_misses/op", "fast_path.stage2.index_lookup_cache_misses")
}

func seedRecommendationBenchmarkGraph(ctx context.Context, store graph.GraphStore) error {
	const (
		tenant              = "bench-rec"
		peerCount           = 500
		sharedMovieCount    = 220
		candidateMovieCount = 600
		sharedPerPeer       = 24
		candidatePerPeer    = 24
	)

	edgeSeq := 0
	nextEdgeID := func() string {
		edgeSeq++
		return fmt.Sprintf("e-%d", edgeSeq)
	}

	return store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: "u-1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1), "cohort": valueToBytes(1)}}); err != nil {
			return err
		}
		for p := 2; p <= peerCount+1; p++ {
			cohort := 0
			if p <= 17 {
				cohort = 1
			}
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("u-%d", p), Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(p), "cohort": valueToBytes(cohort)}}); err != nil {
				return err
			}
		}

		for m := 1; m <= sharedMovieCount+candidateMovieCount; m++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant: tenant,
				ID:     fmt.Sprintf("m-%d", m),
				Labels: []string{"Movie"},
				Properties: map[string][]byte{
					"movie_id":   valueToBytes(m),
					"title":      valueToBytes(fmt.Sprintf("Movie %d", m)),
					"year":       valueToBytes(1980 + (m % 40)),
					"base_score": valueToBytes(float64((m%10)+1) / 10.0),
				},
			}); err != nil {
				return err
			}
		}

		for m := 1; m <= sharedMovieCount; m++ {
			rating := 3.0 + float64(m%3)
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: "u-1", DstID: fmt.Sprintf("m-%d", m), Properties: map[string][]byte{"rating": valueToBytes(rating)}}); err != nil {
				return err
			}
		}
		for m := 1; m <= 60; m++ {
			candidateMovieID := sharedMovieCount + m
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: "u-1", DstID: fmt.Sprintf("m-%d", candidateMovieID), Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
				return err
			}
		}

		for p := 2; p <= peerCount+1; p++ {
			peerID := fmt.Sprintf("u-%d", p)
			for j := 0; j < sharedPerPeer; j++ {
				movieID := 1 + ((p*7 + j*11) % sharedMovieCount)
				rating := 3.0 + float64((p+j)%3)
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", movieID), Properties: map[string][]byte{"rating": valueToBytes(rating)}}); err != nil {
					return err
				}
			}
			for j := 0; j < candidatePerPeer; j++ {
				movieID := sharedMovieCount + 1 + ((p*13 + j*17) % candidateMovieCount)
				rating := 4.0 + float64((p+j)%2)
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", movieID), Properties: map[string][]byte{"rating": valueToBytes(rating)}}); err != nil {
					return err
				}
			}
		}

		return nil
	})
}
