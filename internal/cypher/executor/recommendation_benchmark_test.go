package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

const recommendationBenchmarkQuery = "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, candidate.title AS title, candidate.year AS year, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"

type recommendationBenchmarkMode int

const (
	recommendationBenchmarkModeStep1TopK recommendationBenchmarkMode = iota
	recommendationBenchmarkModeStep2LateMaterialization
)

func BenchmarkRecommendationQueryStep1TopKPushdown(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep1TopK)
}

func BenchmarkRecommendationQueryStep2LateMaterialization(b *testing.B) {
	benchmarkRecommendationQuery(b, recommendationBenchmarkModeStep2LateMaterialization)
}

func benchmarkRecommendationQuery(b *testing.B, mode recommendationBenchmarkMode) {
	ctx := context.Background()
	store := openBenchmarkStore(b)
	defer func() { _ = store.Close() }()

	if err := seedRecommendationBenchmarkGraph(ctx, store); err != nil {
		b.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkQuery)
	if err != nil {
		b.Fatalf("parse failed: %v", err)
	}

	opts := Options{Metrics: NewCollector()}
	if mode == recommendationBenchmarkModeStep1TopK {
		opts.DisableStage2LateMaterialization = true
	}
	exec := New(store, opts)
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
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: "u-1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		for p := 2; p <= peerCount+1; p++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: fmt.Sprintf("u-%d", p), Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(p)}}); err != nil {
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
