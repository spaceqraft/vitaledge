package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/indexschema"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

const recommendationBenchmarkQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
const recommendationBenchmarkSelectiveQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
const recommendationBenchmarkIndexedSelectiveQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 5 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"

var recommendationBenchmarkMultiTargetQuery = strings.Replace(recommendationBenchmarkQuery, "{user_id: 1}", "{cohort: 1}", 1)

type recommendationBenchmarkMode int

const (
	recommendationBenchmarkModeStage1TopKAdaptive recommendationBenchmarkMode = iota
	recommendationBenchmarkModeStage2LateMaterialization
	recommendationBenchmarkModeStage2IndexPushdown
)

type recommendationBenchmarkTargetMode int

const (
	recommendationBenchmarkTargetModeSingle recommendationBenchmarkTargetMode = iota
	recommendationBenchmarkTargetModeMulti
)

type recommendationBenchmarkSeedProfile int

const (
	recommendationBenchmarkSeedProfileDefault recommendationBenchmarkSeedProfile = iota
	recommendationBenchmarkSeedProfileIndexedSelectiveActivation
	recommendationBenchmarkSeedProfileRangePredicateActivation
)

func BenchmarkRecommendationQueryStage1TopKAdaptive(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage1TopKAdaptive, recommendationBenchmarkTargetModeSingle)
}

func BenchmarkRecommendationQueryStage1TopKPushdownCurrent(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage2LateMaterialization, recommendationBenchmarkTargetModeSingle)
}

func BenchmarkRecommendationQueryStage2LateMaterialization(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage2LateMaterialization, recommendationBenchmarkTargetModeSingle)
}

func BenchmarkRecommendationQueryMultiTargetStage1Nested(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage2LateMaterialization, recommendationBenchmarkTargetModeMulti)
}

func BenchmarkRecommendationQueryMultiTargetStage1SharedSeedExpansion(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage2LateMaterialization, recommendationBenchmarkTargetModeMulti)
}

func BenchmarkRecommendationQueryStage2IndexPushdown(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStage2IndexPushdown, recommendationBenchmarkTargetModeSingle)
}

func BenchmarkRecommendationQuerySelectiveStage2IndexPushdown(b *testing.B) {
	benchmarkRecommendationQueryWithQuery(b, recommendationBenchmarkModeStage2IndexPushdown, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkSelectiveQuery)
}

func BenchmarkRecommendationQueryIndexedSelectiveStage2IndexPushdown(b *testing.B) {
	// Uses a dedicated seed profile that keeps stage2 index candidates selective
	// enough to exercise the stage2 early-stop path.
	benchmarkRecommendationQueryWithQueryAndSeed(b, recommendationBenchmarkModeStage2IndexPushdown, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkIndexedSelectiveQuery, seedRecommendationBenchmarkGraphStage2Activation)
}

func BenchmarkRecommendationQueryRangePredicateStage2IndexPushdown(b *testing.B) {
	// Exercises the production recommendation query shape with a dedicated seed
	// profile so the one-sided numeric range can activate stage2 index pushdown
	// without tripping the generic probe cap.
	benchmarkRecommendationQueryWithQueryAndSeedProfile(b, recommendationBenchmarkModeStage2IndexPushdown, recommendationBenchmarkTargetModeSingle, recommendationBenchmarkQuery, recommendationBenchmarkSeedProfileRangePredicateActivation)
}

func benchmarkRecommendationQuery(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode) {
	benchmarkRecommendationQueryWithQueryAndSeedProfile(b, mode, targetMode, "", recommendationBenchmarkSeedProfileDefault)
}

func benchmarkRecommendationQueryWithQuery(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode, queryOverride string) {
	benchmarkRecommendationQueryWithQueryAndSeedProfile(b, mode, targetMode, queryOverride, recommendationBenchmarkSeedProfileDefault)
}

func benchmarkRecommendationQueryWithQueryAndSeedProfile(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode, queryOverride string, seedProfile recommendationBenchmarkSeedProfile) {
	seedFn := seedRecommendationBenchmarkGraphProfile(seedProfile)
	benchmarkRecommendationQueryWithQueryAndSeed(b, mode, targetMode, queryOverride, seedFn)
}

func seedRecommendationBenchmarkGraphProfile(profile recommendationBenchmarkSeedProfile) func(context.Context, graph.GraphStore) error {
	switch profile {
	case recommendationBenchmarkSeedProfileIndexedSelectiveActivation:
		return seedRecommendationBenchmarkGraphStage2Activation
	case recommendationBenchmarkSeedProfileRangePredicateActivation:
		return seedRecommendationBenchmarkRangePredicateActivationGraph
	default:
		return seedRecommendationBenchmarkGraph
	}
}

func benchmarkRecommendationQueryWithQueryAndSeed(b *testing.B, mode recommendationBenchmarkMode, targetMode recommendationBenchmarkTargetMode, queryOverride string, seedFn func(context.Context, graph.GraphStore) error) {
	ctx := context.Background()
	store := openBenchmarkStore(b)
	defer func() { _ = store.Close() }()

	if seedFn == nil {
		seedFn = seedRecommendationBenchmarkGraph
	}
	if err := seedFn(ctx, store); err != nil {
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
	exec := New(store, opts)
	if mode == recommendationBenchmarkModeStage2IndexPushdown {
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
	reportCounterPerOp("stage2_index_pushdown_eligible_one_sided_range/op", "fast_path.stage2.index_pushdown_eligible_one_sided_range")
	reportCounterPerOp("stage2_index_probe_cap_exceeded/op", "fast_path.stage2.index_probe_cap_exceeded")
	reportCounterPerOp("stage2_index_probe_source_scope_skipped_wide/op", "fast_path.stage2.index_probe_source_scope_skipped_wide")
	reportCounterPerOp("stage2_index_cache_hits/op", "fast_path.stage2.index_lookup_cache_hits")
	reportCounterPerOp("stage2_index_cache_misses/op", "fast_path.stage2.index_lookup_cache_misses")
	reportCounterPerOp("stage2_early_stop_checks/op", "fast_path.stage2.early_stop_checks")
	reportCounterPerOp("stage2_early_stop_triggers/op", "fast_path.stage2.early_stop_triggers")
	reportCounterPerOp("stage2_early_stop_edges_skipped/op", "fast_path.stage2.early_stop_edges_skipped")
}

func seedRecommendationBenchmarkGraph(ctx context.Context, store graph.GraphStore) error {
	const (
		tenant              = "bench-rec"
		peerCount           = 6
		sharedMovieCount    = 24
		candidateMovieCount = 80
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
		for p := 2; p <= peerCount; p++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("u-%d", p), Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(p), "cohort": valueToBytes(0)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= sharedMovieCount; i++ {
			movieID := fmt.Sprintf("m-shared-%d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: movieID, Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(100 + i), "title": valueToBytes(fmt.Sprintf("Shared %d", i)), "year": valueToBytes(2000 + i), "base_score": valueToBytes(0.2)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: "u-1", DstID: movieID, Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= candidateMovieCount; i++ {
			movieID := fmt.Sprintf("m-candidate-%d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: movieID, Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(1000 + i), "title": valueToBytes(fmt.Sprintf("Candidate %d", i)), "year": valueToBytes(1990 + (i % 25)), "base_score": valueToBytes(0.7)}}); err != nil {
				return err
			}
		}

		for _, peerID := range []string{"u-2", "u-3"} {
			for i := 1; i <= sharedMovieCount; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := 1; i <= 20; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-candidate-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
		}

		for _, peerID := range []string{"u-4", "u-5", "u-6"} {
			for i := 1; i <= 10; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
			for i := 21; i <= 30; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-candidate-%d", i), Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func seedRecommendationBenchmarkRangePredicateActivationGraph(ctx context.Context, store graph.GraphStore) error {
	const tenant = "bench-rec"
	const (
		noiseCandidateStart = 200
		noiseCandidateCount = 400
	)

	edgeSeq := 0
	nextEdgeID := func() string {
		edgeSeq++
		return fmt.Sprintf("e-range-%d", edgeSeq)
	}

	return store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: "u-1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1), "cohort": valueToBytes(1)}}); err != nil {
			return err
		}
		for p := 2; p <= 6; p++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("u-%d", p), Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(p), "cohort": valueToBytes(1)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= 6; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("m-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i), "title": valueToBytes(fmt.Sprintf("Anchor %d", i)), "year": valueToBytes(2000 + i), "base_score": valueToBytes(0.5)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: "u-1", DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		for i := 101; i <= 110; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("m-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i), "title": valueToBytes(fmt.Sprintf("Candidate %d", i)), "year": valueToBytes(1990 + (i % 20)), "base_score": valueToBytes(0.8)}}); err != nil {
				return err
			}
		}
		for i := noiseCandidateStart; i < noiseCandidateStart+noiseCandidateCount; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("m-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i), "title": valueToBytes(fmt.Sprintf("Noise %d", i)), "year": valueToBytes(1980 + (i % 30)), "base_score": valueToBytes(0.1)}}); err != nil {
				return err
			}
		}

		for _, peerID := range []string{"u-2", "u-3"} {
			for i := 1; i <= 6; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := 101; i <= 102; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := noiseCandidateStart; i < noiseCandidateStart+noiseCandidateCount; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
		}

		for _, peerID := range []string{"u-4", "u-5", "u-6"} {
			for i := 1; i <= 6; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: "m-105", Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
				return err
			}
			if peerID == "u-4" {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: "m-106", Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
					return err
				}
			}
			for i := noiseCandidateStart; i < noiseCandidateStart+noiseCandidateCount; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func seedRecommendationBenchmarkGraphStage2Activation(ctx context.Context, store graph.GraphStore) error {
	const (
		tenant            = "bench-rec"
		sharedMovieCount  = 6
		topCandidateCount = 10
	)

	edgeSeq := 0
	nextEdgeID := func() string {
		edgeSeq++
		return fmt.Sprintf("e-act-%d", edgeSeq)
	}

	return store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: "u-1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1), "cohort": valueToBytes(1)}}); err != nil {
			return err
		}

		for i := 1; i <= sharedMovieCount; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("m-shared-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(100 + i), "title": valueToBytes(fmt.Sprintf("Shared %d", i)), "year": valueToBytes(2000 + i), "base_score": valueToBytes(0.2)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: "u-1", DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= topCandidateCount; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("m-top-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i), "title": valueToBytes(fmt.Sprintf("Top %d", i)), "year": valueToBytes(2010 + i), "base_score": valueToBytes(0.9)}}); err != nil {
				return err
			}
		}

		strongPeers := []string{"u-2"}
		weakPeers := []string{"u-3", "u-4", "u-5", "u-6"}
		allPeers := append(append([]string{}, strongPeers...), weakPeers...)
		for idx, peerID := range allPeers {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: peerID, Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(idx + 2), "cohort": valueToBytes(0)}}); err != nil {
				return err
			}
		}

		for _, peerID := range strongPeers {
			for i := 1; i <= sharedMovieCount; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := 1; i <= topCandidateCount; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-top-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
		}

		for _, peerID := range weakPeers {
			for i := 1; i <= 3; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
			noiseID := fmt.Sprintf("m-weak-noise-%s", peerID)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: noiseID, Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(2000 + edgeSeq), "title": valueToBytes(noiseID), "year": valueToBytes(2021), "base_score": valueToBytes(0.1)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: noiseID, Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		return nil
	})
}
