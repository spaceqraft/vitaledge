package pebblestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/spaceqraft/vitaledge/internal/graph"
	"github.com/spaceqraft/vitaledge/internal/graph/keyspace"
)

func TestVertexEdgeCRUDAndAdjacency(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "g1", Labels: []string{"Group"}}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{
			Tenant: "acme",
			ID:     "e1",
			Type:   "MEMBER_OF",
			SrcID:  "u1",
			DstID:  "g1",
		}})
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if !dbHasKey(t, store, keyspace.VertexLabelKey("acme", "u1", "User")) {
		t.Fatalf("expected vertex label forward index for u1/User")
	}
	if !dbHasKey(t, store, keyspace.VertexLabelMembershipKey("acme", "User", "u1")) {
		t.Fatalf("expected label vertex reverse index for User/u1")
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "e1")
		if err != nil {
			return err
		}
		if edge.Type != "MEMBER_OF" {
			t.Fatalf("unexpected edge type: %s", edge.Type)
		}

		outCount := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			outCount++
			return nil
		}); err != nil {
			return err
		}
		if outCount != 1 {
			t.Fatalf("expected 1 out edge, got %d", outCount)
		}

		inCount := 0
		if err := tx.ScanInEdges(ctx, "acme", "g1", "MEMBER_OF", 10, func(edge *graph.Edge) error {
			inCount++
			return nil
		}); err != nil {
			return err
		}
		if inCount != 1 {
			t.Fatalf("expected 1 in edge, got %d", inCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdgeBatch(ctx, "acme", []string{"e1"})
	})
	if err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		if _, err := tx.GetEdge(ctx, "acme", "e1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return errors.New("expected edge to be absent")
		}
		count := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("expected no out edges after delete")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
	}
}

func TestDeleteVertexDetachBatchDeletesVertexSetAndIncidentEdges(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		for _, vertexID := range []string{"u1", "u2", "u3", "u4"} {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: vertexID, Labels: []string{"Person"}}}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u3", DstID: "u1"},
			{Tenant: "acme", ID: "e3", Type: "KNOWS", SrcID: "u2", DstID: "u4"},
			{Tenant: "acme", ID: "e4", Type: "KNOWS", SrcID: "u3", DstID: "u4"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteVertexDetachBatch(ctx, "acme", []string{"u1", "u2", "u1"})
	})
	if err != nil {
		t.Fatalf("delete vertex detach batch failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		for _, vertexID := range []string{"u1", "u2"} {
			if _, err := tx.GetVertex(ctx, "acme", vertexID); !graph.IsKind(err, graph.ErrKindNotFound) {
				return fmt.Errorf("expected vertex %s to be absent", vertexID)
			}
		}
		for _, vertexID := range []string{"u3", "u4"} {
			if _, err := tx.GetVertex(ctx, "acme", vertexID); err != nil {
				return fmt.Errorf("expected vertex %s to remain: %w", vertexID, err)
			}
		}
		for _, edgeID := range []string{"e1", "e2", "e3"} {
			if _, err := tx.GetEdge(ctx, "acme", edgeID); !graph.IsKind(err, graph.ErrKindNotFound) {
				return fmt.Errorf("expected edge %s to be absent", edgeID)
			}
		}
		if _, err := tx.GetEdge(ctx, "acme", "e4"); err != nil {
			return fmt.Errorf("expected non-incident edge to remain: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
	}
}

func TestDeleteVertexDetachDeletesSingleVertexAndIncidentEdges(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"Person"}}, {Tenant: "acme", ID: "u2", Labels: []string{"Person"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteVertexDetachBatch(ctx, "acme", []string{"u1"})
	})
	if err != nil {
		t.Fatalf("delete vertex detach failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		if _, err := tx.GetVertex(ctx, "acme", "u1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return fmt.Errorf("expected deleted vertex to be absent")
		}
		if _, err := tx.GetEdge(ctx, "acme", "e1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return fmt.Errorf("expected incident edge to be absent")
		}
		if _, err := tx.GetVertex(ctx, "acme", "u2"); err != nil {
			return fmt.Errorf("expected peer vertex to remain: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
	}
}

func TestEdgePHashStorageAndPropertyHydration(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	initial := &graph.Edge{
		Tenant: "acme",
		ID:     "e1",
		Type:   "RATED",
		SrcID:  "u1",
		DstID:  "m1",
		Properties: graph.PropertyMap{
			"rating": []byte("4.5"),
			"note":   []byte("great"),
		},
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{initial})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	raw, closer, err := store.db.Get(keyspace.EdgeKey("acme", "e1"))
	if err != nil {
		t.Fatalf("read raw edge failed: %v", err)
	}
	defer closer.Close()
	if got := string(raw); got != string(edgePHash(initial)) {
		t.Fatalf("unexpected edge phash: got %q want %q", got, string(edgePHash(initial)))
	}
	if json.Valid(raw) {
		t.Fatalf("expected edge value to be phash, got json payload %q", string(raw))
	}
	if got := countByPrefix(t, store, keyspace.EdgePropertyPrefix("acme", "e1", "RATED", "rating")); got != 1 {
		t.Fatalf("expected one edge property forward key, got %d", got)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "e1")
		if err != nil {
			return err
		}
		if edge.Type != "RATED" || edge.SrcID != "u1" || edge.DstID != "m1" {
			return fmt.Errorf("unexpected hydrated edge: %+v", edge)
		}
		if got := string(edge.Properties["rating"]); got != "4.5" {
			return fmt.Errorf("unexpected rating property: %q", got)
		}
		if got := string(edge.Properties["note"]); got != "great" {
			return fmt.Errorf("unexpected note property: %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}

	updated := &graph.Edge{
		Tenant: "acme",
		ID:     "e1",
		Type:   "LIKED",
		SrcID:  "u1",
		DstID:  "m1",
		Properties: graph.PropertyMap{
			"weight": []byte("0.9"),
		},
	}
	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutEdgeBatch(ctx, []*graph.Edge{updated})
	})
	if err != nil {
		t.Fatalf("update edge failed: %v", err)
	}
	if got := countByPrefix(t, store, keyspace.EdgePropertyPrefix("acme", "e1", "RATED", "rating")); got != 0 {
		t.Fatalf("expected stale edge property forward keys removed, got %d", got)
	}
	if got := countByPrefix(t, store, keyspace.PropertyEdgePrefix("acme", "RATED", "rating")); got != 0 {
		t.Fatalf("expected stale property edge reverse keys removed, got %d", got)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "e1")
		if err != nil {
			return err
		}
		if edge.Type != "LIKED" {
			return fmt.Errorf("unexpected updated edge type: %s", edge.Type)
		}
		if _, ok := edge.Properties["rating"]; ok {
			return fmt.Errorf("unexpected stale rating property after update")
		}
		if got := string(edge.Properties["weight"]); got != "0.9" {
			return fmt.Errorf("unexpected weight property: %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-update view failed: %v", err)
	}
}

func TestScanOutEdgeSourceIDsByType(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u4", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u1", DstID: "u3"},
			{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "u1", DstID: "u4"},
			{Tenant: "acme", ID: "e4", Type: "KNOWS", SrcID: "u2", DstID: "u3"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		knowSources := make([]string, 0)
		if err := tx.ScanOutEdgeSourceIDs(ctx, "acme", "KNOWS", 0, func(sourceID string) error {
			knowSources = append(knowSources, sourceID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(knowSources)
		if got := strings.Join(knowSources, ","); got != "u1,u2" {
			return fmt.Errorf("unexpected KNOWS sources: %s", got)
		}

		allSources := make([]string, 0)
		if err := tx.ScanOutEdgeSourceIDs(ctx, "acme", "", 0, func(sourceID string) error {
			allSources = append(allSources, sourceID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(allSources)
		if got := strings.Join(allSources, ","); got != "u1,u2" {
			return fmt.Errorf("unexpected all-type sources: %s", got)
		}

		limited := make([]string, 0)
		if err := tx.ScanOutEdgeSourceIDs(ctx, "acme", "KNOWS", 1, func(sourceID string) error {
			limited = append(limited, sourceID)
			return nil
		}); err != nil {
			return err
		}
		if len(limited) != 1 {
			return fmt.Errorf("expected one limited source, got %d", len(limited))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestScanOutEdgeLinksByType(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u4", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u1", DstID: "u3"},
			{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "u1", DstID: "u4"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		knowLinks := make([]string, 0)
		if err := tx.ScanOutEdgeLinks(ctx, "acme", "u1", "KNOWS", 0, func(edgeID, dstID string) error {
			knowLinks = append(knowLinks, edgeID+"->"+dstID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(knowLinks)
		if got := strings.Join(knowLinks, ","); got != "e1->u2,e2->u3" {
			return fmt.Errorf("unexpected KNOWS links: %s", got)
		}

		allLinks := make([]string, 0)
		if err := tx.ScanOutEdgeLinks(ctx, "acme", "u1", "", 0, func(edgeID, dstID string) error {
			allLinks = append(allLinks, edgeID+"->"+dstID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(allLinks)
		if got := strings.Join(allLinks, ","); got != "e1->u2,e2->u3,e3->u4" {
			return fmt.Errorf("unexpected all-type links: %s", got)
		}

		limited := make([]string, 0)
		if err := tx.ScanOutEdgeLinks(ctx, "acme", "u1", "KNOWS", 1, func(edgeID, dstID string) error {
			limited = append(limited, edgeID+"->"+dstID)
			return nil
		}); err != nil {
			return err
		}
		if len(limited) != 1 {
			return fmt.Errorf("expected one limited link, got %d", len(limited))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestScanVerticesByLabelAndIDs(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User", "Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		typed, ok := tx.(interface {
			ScanVerticesByLabel(ctx context.Context, tenant, label string, limit int, fn func(*graph.Vertex) error) error
			ScanVertexIDsByLabel(ctx context.Context, tenant, label string, limit int, fn func(string) error) error
		})
		if !ok {
			return fmt.Errorf("tx does not expose label scan extensions")
		}

		users := make([]string, 0)
		if err := typed.ScanVerticesByLabel(ctx, "acme", "User", 0, func(vertex *graph.Vertex) error {
			if vertex != nil {
				users = append(users, vertex.ID)
			}
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(users)
		if got := strings.Join(users, ","); got != "u1,u2" {
			return fmt.Errorf("unexpected user vertex ids: %s", got)
		}

		userIDs := make([]string, 0)
		if err := typed.ScanVertexIDsByLabel(ctx, "acme", "User", 0, func(vertexID string) error {
			userIDs = append(userIDs, vertexID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(userIDs)
		if got := strings.Join(userIDs, ","); got != "u1,u2" {
			return fmt.Errorf("unexpected user vertex ids from id scan: %s", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestScanOutEdgeLinksByTypeBulk(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u4", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u2", DstID: "u3"},
			{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "u3", DstID: "u4"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		knowLinks := make([]string, 0)
		if err := tx.ScanOutEdgeLinksByType(ctx, "acme", "KNOWS", 0, func(srcID, edgeID, dstID string) error {
			knowLinks = append(knowLinks, srcID+":"+edgeID+"->"+dstID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(knowLinks)
		if got := strings.Join(knowLinks, ","); got != "u1:e1->u2,u2:e2->u3" {
			return fmt.Errorf("unexpected KNOWS bulk links: %s", got)
		}

		limited := make([]string, 0)
		if err := tx.ScanOutEdgeLinksByType(ctx, "acme", "KNOWS", 1, func(srcID, edgeID, dstID string) error {
			limited = append(limited, srcID+":"+edgeID+"->"+dstID)
			return nil
		}); err != nil {
			return err
		}
		if len(limited) != 1 {
			return fmt.Errorf("expected one limited bulk link, got %d", len(limited))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestHasUndirectedEdgeBetween(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "u3", DstID: "u1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		connected, err := tx.HasUndirectedEdgeBetween(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if !connected {
			return fmt.Errorf("expected u1 and u2 to be connected by KNOWS")
		}

		reverseConnected, err := tx.HasUndirectedEdgeBetween(ctx, "acme", "u2", "u1", "KNOWS")
		if err != nil {
			return err
		}
		if !reverseConnected {
			return fmt.Errorf("expected reverse check to be connected by KNOWS")
		}

		notConnected, err := tx.HasUndirectedEdgeBetween(ctx, "acme", "u1", "u3", "KNOWS")
		if err != nil {
			return err
		}
		if notConnected {
			return fmt.Errorf("expected u1 and u3 to be disconnected by KNOWS")
		}

		wrongTypeConnected, err := tx.HasUndirectedEdgeBetween(ctx, "acme", "u1", "u2", "LIKES")
		if err != nil {
			return err
		}
		if wrongTypeConnected {
			return fmt.Errorf("expected u1 and u2 to be disconnected by LIKES")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestHasDirectedEdgeBetween(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u2", DstID: "u1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		exists, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("expected directed u1->u2 KNOWS edge")
		}
		reverseExists, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u2", "u1", "KNOWS")
		if err != nil {
			return err
		}
		if !reverseExists {
			return fmt.Errorf("expected directed u2->u1 KNOWS edge")
		}
		missing, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u3", "KNOWS")
		if err != nil {
			return err
		}
		if missing {
			return fmt.Errorf("expected no directed u1->u3 KNOWS edge")
		}
		wrongType, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u2", "LIKES")
		if err != nil {
			return err
		}
		if wrongType {
			return fmt.Errorf("expected no directed u1->u2 LIKES edge")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestHasDirectedEdgeBetweenTracksEdgeUpdatesAndDeletes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u3"}})
	})
	if err != nil {
		t.Fatalf("update edge failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		existsOld, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if existsOld {
			return fmt.Errorf("expected no directed u1->u2 KNOWS edge after update")
		}
		existsNew, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u3", "KNOWS")
		if err != nil {
			return err
		}
		if !existsNew {
			return fmt.Errorf("expected directed u1->u3 KNOWS edge after update")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-update view failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdgeBatch(ctx, "acme", []string{"e1"})
	})
	if err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		existsNew, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u3", "KNOWS")
		if err != nil {
			return err
		}
		if existsNew {
			return fmt.Errorf("expected no directed u1->u3 KNOWS edge after delete")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete view failed: %v", err)
	}
}

func TestDeleteEdgeBatchDeletesMultipleEdges(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"Person"}}, {Tenant: "acme", ID: "u2", Labels: []string{"Person"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdgeBatch(ctx, "acme", []string{"e1", "e2"})
	})
	if err != nil {
		t.Fatalf("delete edge batch failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		for _, edgeID := range []string{"e1", "e2"} {
			if _, err := tx.GetEdge(ctx, "acme", edgeID); !graph.IsKind(err, graph.ErrKindNotFound) {
				return fmt.Errorf("expected edge %s to be absent", edgeID)
			}
		}
		count := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "KNOWS", 10, func(edge *graph.Edge) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("expected no out edges after batch delete")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
	}
}

func TestHasDirectedEdgeBetweenWithParallelEdges(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"Person"}}, {Tenant: "acme", ID: "u2", Labels: []string{"Person"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdgeBatch(ctx, "acme", []string{"e1"})
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		exists, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("expected directed u1->u2 KNOWS edge to remain after deleting one of parallel edges")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestAdjacencyPrimitivesAndBatchEndpointProbes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{vertex}); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "e3", Type: "KNOWS", SrcID: "u2", DstID: "u1"},
			{Tenant: "acme", ID: "e4", Type: "KNOWS", SrcID: "u3", DstID: "u1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		out := []string{}
		if err := tx.ScanAdjacencyLinks(ctx, "acme", "u1", graph.EdgeDirectionOut, "KNOWS", 0, func(edgeID, peerID string) error {
			out = append(out, edgeID+"->"+peerID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(out)
		if got := strings.Join(out, ","); got != "e1->u2,e2->u2" {
			return fmt.Errorf("unexpected out adjacency links: %s", got)
		}

		in := []string{}
		if err := tx.ScanAdjacencyLinks(ctx, "acme", "u1", graph.EdgeDirectionIn, "KNOWS", 0, func(edgeID, peerID string) error {
			in = append(in, edgeID+"<-"+peerID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(in)
		if got := strings.Join(in, ","); got != "e3<-u2,e4<-u3" {
			return fmt.Errorf("unexpected in adjacency links: %s", got)
		}

		any := []string{}
		if err := tx.ScanAdjacencyLinks(ctx, "acme", "u1", graph.EdgeDirectionAny, "KNOWS", 0, func(edgeID, peerID string) error {
			any = append(any, edgeID+":"+peerID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(any)
		if got := strings.Join(any, ","); got != "e1:u2,e2:u2,e3:u2,e4:u3" {
			return fmt.Errorf("unexpected any-direction adjacency links: %s", got)
		}

		directedCount, err := tx.DirectedEdgePairCount(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if directedCount != 2 {
			return fmt.Errorf("expected directed pair count 2 for u1->u2 KNOWS, got %d", directedCount)
		}

		undirectedCount, err := tx.UndirectedEdgePairCount(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if undirectedCount != 3 {
			return fmt.Errorf("expected undirected pair count 3 for u1/u2 KNOWS, got %d", undirectedCount)
		}

		directedBatch, err := tx.BatchHasDirectedEdgeBetween(ctx, "acme", []graph.DirectedEdgeProbe{
			{SrcID: "u1", DstID: "u2", EdgeType: "KNOWS"},
			{SrcID: "u2", DstID: "u1", EdgeType: "KNOWS"},
			{SrcID: "u1", DstID: "u3", EdgeType: "KNOWS"},
		})
		if err != nil {
			return err
		}
		if len(directedBatch) != 3 || !directedBatch[0] || !directedBatch[1] || directedBatch[2] {
			return fmt.Errorf("unexpected directed batch result: %#v", directedBatch)
		}

		undirectedBatch, err := tx.BatchHasUndirectedEdgeBetween(ctx, "acme", []graph.UndirectedEdgeProbe{
			{LeftID: "u1", RightID: "u2", EdgeType: "KNOWS"},
			{LeftID: "u1", RightID: "u3", EdgeType: "KNOWS"},
		})
		if err != nil {
			return err
		}
		if len(undirectedBatch) != 2 || !undirectedBatch[0] || !undirectedBatch[1] {
			return fmt.Errorf("unexpected undirected batch result: %#v", undirectedBatch)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestHasUndirectedEdgeBetweenTracksDirectionDeletes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"Person"}}, {Tenant: "acme", ID: "u2", Labels: []string{"Person"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "u2", DstID: "u1"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdgeBatch(ctx, "acme", []string{"e1"})
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		exists, err := tx.HasUndirectedEdgeBetween(ctx, "acme", "u1", "u2", "KNOWS")
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("expected undirected u1-u2 KNOWS edge to remain after deleting one direction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestScanVertices(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "v1", Labels: []string{"User"}}, {Tenant: "acme", ID: "v2", Labels: []string{"Group"}}, {Tenant: "other", ID: "v3", Labels: []string{"User"}}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		seen := map[string]bool{}
		if err := tx.ScanVertices(ctx, "acme", 0, func(v *graph.Vertex) error {
			seen[v.ID] = true
			return nil
		}); err != nil {
			return err
		}
		if len(seen) != 2 || !seen["v1"] || !seen["v2"] {
			return fmt.Errorf("unexpected scanned vertices: %#v", seen)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan vertices failed: %v", err)
	}
}

func TestHasVertexLabelTracksUpdatesAndDeletes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person", "User"}},
		})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		hasPerson, err := tx.HasVertexLabel(ctx, "acme", "u1", "Person")
		if err != nil {
			return err
		}
		if !hasPerson {
			return fmt.Errorf("expected u1 to have label Person")
		}
		hasAdmin, err := tx.HasVertexLabel(ctx, "acme", "u1", "Admin")
		if err != nil {
			return err
		}
		if hasAdmin {
			return fmt.Errorf("expected u1 to not have label Admin")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("initial view failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"Person", "Admin"}},
		})
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		hasUser, err := tx.HasVertexLabel(ctx, "acme", "u1", "User")
		if err != nil {
			return err
		}
		if hasUser {
			return fmt.Errorf("expected u1 to not have label User after update")
		}
		hasAdmin, err := tx.HasVertexLabel(ctx, "acme", "u1", "Admin")
		if err != nil {
			return err
		}
		if !hasAdmin {
			return fmt.Errorf("expected u1 to have label Admin after update")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-update view failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteVertexBatch(ctx, "acme", []string{"u1"})
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		hasPerson, err := tx.HasVertexLabel(ctx, "acme", "u1", "Person")
		if err != nil {
			return err
		}
		if hasPerson {
			return fmt.Errorf("expected deleted u1 to have no label membership")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete view failed: %v", err)
	}
}

func TestStatsSnapshotTotalsTrackMutations(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "v1"},
			{Tenant: "acme", ID: "v2"},
		}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "REL", SrcID: "v1", DstID: "v2"},
			{Tenant: "acme", ID: "e2", Type: "REL", SrcID: "v2", DstID: "v1"},
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if snapshot.VertexTotal != 2 {
			return fmt.Errorf("expected VertexTotal=2, got %d", snapshot.VertexTotal)
		}
		if snapshot.EdgeTotal != 2 {
			return fmt.Errorf("expected EdgeTotal=2, got %d", snapshot.EdgeTotal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot verification failed: %v", err)
	}

	metrics := newRecordingMetrics()
	storeWithMetrics := openTempStoreWithMetrics(t, metrics)
	defer func() { _ = storeWithMetrics.Close() }()

	err = storeWithMetrics.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u-metric", Labels: []string{"User", "Admin"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e-metric", Type: "REL", SrcID: "u-metric", DstID: "v-metric"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("metric seed failed: %v", err)
	}

	err = storeWithMetrics.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u-metric", Labels: []string{"User", "Admin"}}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e-metric", Type: "LINKS", SrcID: "u-metric", DstID: "v-metric"}})
	})
	if err != nil {
		t.Fatalf("metric overwrite failed: %v", err)
	}
	if got := metrics.opCount("get_vertex", "ok"); got != 0 {
		t.Fatalf("expected vertex overwrite to avoid get_vertex hydration, got %d", got)
	}
	if got := metrics.opCount("get_edge", "ok"); got != 0 {
		t.Fatalf("expected edge overwrite to avoid get_edge hydration, got %d", got)
	}

	err = storeWithMetrics.Update(ctx, func(tx graph.Tx) error {
		if err := tx.DeleteEdgeBatch(ctx, "acme", []string{"e-metric"}); err != nil {
			return err
		}
		return tx.DeleteVertexBatch(ctx, "acme", []string{"u-metric"})
	})
	if err != nil {
		t.Fatalf("metric delete failed: %v", err)
	}
	if got := metrics.opCount("get_vertex", "ok"); got != 0 {
		t.Fatalf("expected delete path to avoid get_vertex hydration, got %d", got)
	}
	if got := metrics.opCount("get_edge", "ok"); got != 0 {
		t.Fatalf("expected delete path to avoid get_edge hydration, got %d", got)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.DeleteEdgeBatch(ctx, "acme", []string{"e1"}); err != nil {
			return err
		}
		if err := tx.DeleteEdgeBatch(ctx, "acme", []string{"e2"}); err != nil {
			return err
		}
		return tx.DeleteVertexBatch(ctx, "acme", []string{"v2"})
	})
	if err != nil {
		t.Fatalf("delete mutation failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if snapshot.VertexTotal != 1 {
			return fmt.Errorf("expected VertexTotal=1 after delete, got %d", snapshot.VertexTotal)
		}
		if snapshot.EdgeTotal != 0 {
			return fmt.Errorf("expected EdgeTotal=0 after delete, got %d", snapshot.EdgeTotal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete snapshot verification failed: %v", err)
	}
}

func TestStatsSnapshotMissingForTenant(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.View(ctx, func(tx graph.Tx) error {
		_, err := tx.GetStatsSnapshot(ctx, "unknown")
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return fmt.Errorf("expected not found for missing stats snapshot, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("missing snapshot check failed: %v", err)
	}
}

func TestStatsSnapshotLabelAndEdgeTypeCounters(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "v1", Labels: []string{"Movie", "Featured"}},
			{Tenant: "acme", ID: "v2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "v3"},
		}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "v1", DstID: "v2"},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "v2", DstID: "v3"},
			{Tenant: "acme", ID: "e3", Type: "GENRED", SrcID: "v1", DstID: "v3"},
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "v2", Labels: []string{"User"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e2", Type: "LIKED", SrcID: "v2", DstID: "v3"}}); err != nil {
			return err
		}
		if err := tx.DeleteEdgeBatch(ctx, "acme", []string{"e2"}); err != nil {
			return err
		}
		if err := tx.DeleteEdgeBatch(ctx, "acme", []string{"e3"}); err != nil {
			return err
		}
		if err := tx.DeleteVertexBatch(ctx, "acme", []string{"v3"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("mutation failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if got := snapshot.LabelCounts["Movie"]; got != 1 {
			return fmt.Errorf("expected Movie label count=1, got %d", got)
		}
		if got := snapshot.LabelCounts["Featured"]; got != 1 {
			return fmt.Errorf("expected Featured label count=1, got %d", got)
		}
		if got := snapshot.LabelCounts["User"]; got != 1 {
			return fmt.Errorf("expected User label count=1, got %d", got)
		}
		if got := snapshot.LabelCounts["UNLABELED"]; got != 0 {
			return fmt.Errorf("expected UNLABELED label count=0, got %d", got)
		}
		if got := snapshot.EdgeCounts["RATED"]; got != 1 {
			return fmt.Errorf("expected RATED edge count=1, got %d", got)
		}
		if got := snapshot.EdgeCounts["LIKED"]; got != 0 {
			return fmt.Errorf("expected LIKED edge count=0, got %d", got)
		}
		if got := snapshot.EdgeCounts["GENRED"]; got != 0 {
			return fmt.Errorf("expected GENRED edge count=0, got %d", got)
		}
		if got := snapshot.EdgeSourceCounts["RATED"]; got != 1 {
			return fmt.Errorf("expected RATED source count=1, got %d", got)
		}
		if got := snapshot.EdgeSourceCounts["LIKED"]; got != 0 {
			return fmt.Errorf("expected LIKED source count=0, got %d", got)
		}
		if got := snapshot.EdgeSourceCounts["GENRED"]; got != 0 {
			return fmt.Errorf("expected GENRED source count=0, got %d", got)
		}
		if got := snapshot.EdgeAvgOutDegree["RATED"]; got != 1.0 {
			return fmt.Errorf("expected RATED avg out-degree=1.0, got %v", got)
		}
		if got := snapshot.EdgeAvgOutDegree["LIKED"]; got != 0.0 {
			return fmt.Errorf("expected LIKED avg out-degree=0.0, got %v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot validation failed: %v", err)
	}
}

func TestDurabilityAcrossRestart(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "v-durable"}})
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.View(ctx, func(tx graph.Tx) error {
		v, err := tx.GetVertex(ctx, "acme", "v-durable")
		if err != nil {
			return err
		}
		if v.ID != "v-durable" {
			return errors.New("unexpected vertex id")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("durability verification failed: %v", err)
	}
}

func TestStatsSnapshotPropertySummariesAndFreshness(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "u2", Labels: []string{"User"}}}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "u2"}}); err != nil {
			return err
		}
		entries := []*graph.PropertyIndexEntry{
			{Tenant: "acme", Schema: "User", Property: "age", Value: []byte("20"), EntityID: "u1", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "age", Value: []byte("20"), EntityID: "u2", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "age", Value: []byte("30"), EntityID: "u3", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "active", Value: []byte("true"), EntityID: "u1", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "active", Value: []byte("false"), EntityID: "u2", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "country", Value: []byte("US"), EntityID: "u1", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "country", Value: []byte("US"), EntityID: "u2", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "User", Property: "country", Value: []byte("CA"), EntityID: "u3", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "RATED", Property: "createdAt", Value: []byte("map[__temporal_type:datetime year:2024 month:1 day:2 hour:3 minute:4 second:5 nanosecond:0 timezone:UTC]"), EntityID: "e1", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "u2"},
			{Tenant: "acme", Schema: "RATED", Property: "createdAt", Value: []byte("map[__temporal_type:datetime year:2024 month:1 day:3 hour:3 minute:4 second:5 nanosecond:0 timezone:UTC]"), EntityID: "e2", EntityClass: "edge", EdgeSrcID: "u2", EdgeDstID: "u1"},
		}
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if snapshot.StatsEpoch <= 0 {
			return fmt.Errorf("expected positive stats epoch, got %d", snapshot.StatsEpoch)
		}
		if snapshot.SampleSize != 3 {
			return fmt.Errorf("expected sample size 3, got %d", snapshot.SampleSize)
		}
		if snapshot.LastRefreshTS.IsZero() {
			return fmt.Errorf("expected non-zero last refresh timestamp")
		}
		vertexAge := snapshot.VertexPropertyStats["User"]["age"]
		if vertexAge.DistinctValues != 2 || vertexAge.IndexedEntries != 3 {
			return fmt.Errorf("unexpected vertex property stats: %#v", vertexAge)
		}
		if vertexAge.EstimatedSelectivity != 0.5 {
			return fmt.Errorf("expected vertex property selectivity 0.5, got %v", vertexAge.EstimatedSelectivity)
		}
		if vertexAge.Histogram == nil || vertexAge.Histogram.Kind != "numeric" {
			return fmt.Errorf("expected numeric histogram for vertex age, got %#v", vertexAge.Histogram)
		}
		if vertexAge.StatsEpoch <= 0 || vertexAge.SampleSize != 3 || vertexAge.LastRefreshTS.IsZero() {
			return fmt.Errorf("expected property freshness metadata for vertex age, got epoch=%d sample=%d refresh=%v", vertexAge.StatsEpoch, vertexAge.SampleSize, vertexAge.LastRefreshTS)
		}
		if got := vertexAge.DistinctValuesByKind["numeric"]; got != 2 {
			return fmt.Errorf("expected numeric distinct count 2 for vertex age, got %d", got)
		}
		if got := vertexAge.IndexedEntriesByKind["numeric"]; got != 3 {
			return fmt.Errorf("expected numeric entries 3 for vertex age, got %d", got)
		}
		if got := vertexAge.EstimatedSelectivityByKind["numeric"]; got != 0.5 {
			return fmt.Errorf("expected numeric selectivity 0.5 for vertex age, got %v", got)
		}
		if vertexAge.Histograms["numeric"] == nil {
			return fmt.Errorf("expected numeric histogram in histogram map for vertex age")
		}
		vertexHistogramCount := 0
		for _, bucket := range vertexAge.Histogram.Buckets {
			vertexHistogramCount += bucket.Count
		}
		if vertexHistogramCount != 3 {
			return fmt.Errorf("expected vertex histogram count 3, got %d", vertexHistogramCount)
		}
		vertexActive := snapshot.VertexPropertyStats["User"]["active"]
		if vertexActive.Histograms["boolean"] == nil {
			return fmt.Errorf("expected boolean histogram for vertex active, got %#v", vertexActive.Histograms)
		}
		if got := vertexActive.DistinctValuesByKind["boolean"]; got != 2 {
			return fmt.Errorf("expected boolean distinct count 2 for active, got %d", got)
		}
		vertexCountry := snapshot.VertexPropertyStats["User"]["country"]
		if vertexCountry.Histograms["categorical"] == nil {
			return fmt.Errorf("expected categorical histogram for vertex country, got %#v", vertexCountry.Histograms)
		}
		if got := vertexCountry.DistinctValuesByKind["categorical"]; got != 2 {
			return fmt.Errorf("expected categorical distinct count 2 for country, got %d", got)
		}
		edgeCreatedAt := snapshot.EdgePropertyStats["RATED"]["createdAt"]
		if edgeCreatedAt.DistinctValues != 2 || edgeCreatedAt.IndexedEntries != 2 {
			return fmt.Errorf("unexpected edge property stats: %#v", edgeCreatedAt)
		}
		if edgeCreatedAt.Histogram == nil || edgeCreatedAt.Histogram.Kind != "datetime" {
			return fmt.Errorf("expected datetime histogram for edge createdAt, got %#v", edgeCreatedAt.Histogram)
		}
		if edgeCreatedAt.Histograms["datetime"] == nil {
			return fmt.Errorf("expected datetime histogram in histogram map for edge createdAt")
		}
		if edgeCreatedAt.StatsEpoch <= 0 || edgeCreatedAt.SampleSize != 2 || edgeCreatedAt.LastRefreshTS.IsZero() {
			return fmt.Errorf("expected property freshness metadata for edge createdAt, got epoch=%d sample=%d refresh=%v", edgeCreatedAt.StatsEpoch, edgeCreatedAt.SampleSize, edgeCreatedAt.LastRefreshTS)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("property stats snapshot validation failed: %v", err)
	}
}

func TestOpenRunsStatsMigrationForLegacyData(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "x1"},
		}); err != nil {
			return err
		}
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1"},
			{Tenant: "acme", ID: "e2", Type: "TAGGED", SrcID: "m1", DstID: "x1"},
		}); err != nil {
			return err
		}
		entries := []*graph.PropertyIndexEntry{
			{Tenant: "acme", Schema: "Movie", Property: "score", Value: []byte("9.5"), EntityID: "m1", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "Movie", Property: "score", Value: []byte("7.0"), EntityID: "m2", EntityClass: "vertex"},
			{Tenant: "acme", Schema: "RATED", Property: "createdAt", Value: []byte("map[__temporal_type:datetime year:2024 month:2 day:1 hour:10 minute:0 second:0 nanosecond:0 timezone:UTC]"), EntityID: "e1", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m1"},
		}
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	wipePrefixKeys(t, store, []byte("s/"))
	if err := store.db.Delete(keyspace.SchemaVersionKey(), nil); err != nil && !errors.Is(err, cpebble.ErrNotFound) {
		t.Fatalf("delete schema version key failed: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if snapshot.VertexTotal != 3 {
			return fmt.Errorf("expected VertexTotal=3, got %d", snapshot.VertexTotal)
		}
		if snapshot.EdgeTotal != 2 {
			return fmt.Errorf("expected EdgeTotal=2, got %d", snapshot.EdgeTotal)
		}
		if snapshot.LabelCounts["Movie"] != 1 || snapshot.LabelCounts["User"] != 1 {
			return fmt.Errorf("unexpected label counts: %#v", snapshot.LabelCounts)
		}
		if snapshot.EdgeCounts["RATED"] != 1 || snapshot.EdgeCounts["TAGGED"] != 1 {
			return fmt.Errorf("unexpected edge counts: %#v", snapshot.EdgeCounts)
		}
		if snapshot.StatsEpoch <= 0 || snapshot.LastRefreshTS.IsZero() {
			return fmt.Errorf("expected migrated freshness metadata, got epoch=%d refresh=%v", snapshot.StatsEpoch, snapshot.LastRefreshTS)
		}
		vertexScore := snapshot.VertexPropertyStats["Movie"]["score"]
		if vertexScore.DistinctValues != 2 || vertexScore.IndexedEntries != 2 {
			return fmt.Errorf("unexpected migrated vertex property stats: %#v", vertexScore)
		}
		edgeCreatedAt := snapshot.EdgePropertyStats["RATED"]["createdAt"]
		if edgeCreatedAt.DistinctValues != 1 || edgeCreatedAt.IndexedEntries != 1 {
			return fmt.Errorf("unexpected migrated edge property stats: %#v", edgeCreatedAt)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stats snapshot verification failed: %v", err)
	}

	if !dbHasKey(t, store, keyspace.SchemaVersionKey()) {
		t.Fatalf("expected schema version key to exist after migration")
	}
}

func TestPropertyIndexRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entry := &graph.PropertyIndexEntry{
		Tenant:      "acme",
		Schema:      "User",
		Property:    "email",
		Value:       []byte("alice@acme.io"),
		EntityID:    "u1",
		EntityClass: "vertex",
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutPropertyIndex(ctx, entry)
	})
	if err != nil {
		t.Fatalf("put index failed: %v", err)
	}

	prefix := keyspace.PropertyIndexPrefix("acme", "User", "email")
	if got := countByPrefix(t, store, prefix); got != 1 {
		t.Fatalf("expected one index key, got %d", got)
	}
	if got := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "User", "email")); got != 1 {
		t.Fatalf("expected one property->vertex reverse key, got %d", got)
	}
	if got := countByPrefix(t, store, keyspace.VertexPropertyPrefix("acme", "u1", "User", "email")); got != 1 {
		t.Fatalf("expected one vertex->property forward key, got %d", got)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeletePropertyIndex(ctx, entry)
	})
	if err != nil {
		t.Fatalf("delete index failed: %v", err)
	}

	if got := countByPrefix(t, store, prefix); got != 0 {
		t.Fatalf("expected zero index keys, got %d", got)
	}
	if got := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "User", "email")); got != 0 {
		t.Fatalf("expected zero property->vertex reverse keys, got %d", got)
	}
	if got := countByPrefix(t, store, keyspace.VertexPropertyPrefix("acme", "u1", "User", "email")); got != 0 {
		t.Fatalf("expected zero vertex->property forward keys, got %d", got)
	}
}

func TestPropertyIndexNumericRangeScan(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entries := []*graph.PropertyIndexEntry{
		{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("3.5"), EntityID: "e35", EntityClass: "edge"},
		{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("4.0"), EntityID: "e40", EntityClass: "edge"},
		{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("5.0"), EntityID: "e50", EntityClass: "edge"},
		{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("top"), EntityID: "es", EntityClass: "edge"},
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("put index entries failed: %v", err)
	}

	var got []string
	err = store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexNumericRange(ctx, "acme", "RATED", "rating", 4.0, true, true, 0, false, false, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				got = append(got, entry.EntityID)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("numeric range scan failed: %v", err)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "e40,e50" {
		t.Fatalf("unexpected numeric range entity ids: %#v", got)
	}

	numericPrefix := keyspace.PropertyIndexNumericPrefix("acme", "RATED", "rating")
	if gotCount := countByPrefix(t, store, numericPrefix); gotCount != 3 {
		t.Fatalf("expected three numeric shadow index keys, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyEdgePrefix("acme", "RATED", "rating")); gotCount != 4 {
		t.Fatalf("expected four property->edge reverse index keys, got %d", gotCount)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.DeletePropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete index entries failed: %v", err)
	}
	if gotCount := countByPrefix(t, store, numericPrefix); gotCount != 0 {
		t.Fatalf("expected numeric shadow index keys to be deleted, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyEdgePrefix("acme", "RATED", "rating")); gotCount != 0 {
		t.Fatalf("expected property->edge reverse index keys to be deleted, got %d", gotCount)
	}
}

func TestPropertyIndexBooleanRangeScan(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entries := []*graph.PropertyIndexEntry{
		{Tenant: "acme", Schema: "Feature", Property: "enabled", Value: []byte("false"), EntityID: "f0", EntityClass: "vertex"},
		{Tenant: "acme", Schema: "Feature", Property: "enabled", Value: []byte("true"), EntityID: "f1", EntityClass: "vertex"},
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("put boolean index entries failed: %v", err)
	}

	var got []string
	err = store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexBooleanRange(ctx, "acme", "Feature", "enabled", true, true, true, true, true, true, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				got = append(got, entry.EntityID)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("boolean range scan failed: %v", err)
	}
	if strings.Join(got, ",") != "f1" {
		t.Fatalf("unexpected boolean range entity ids: %#v", got)
	}

	booleanPrefix := keyspace.PropertyIndexBooleanPrefix("acme", "Feature", "enabled")
	if gotCount := countByPrefix(t, store, booleanPrefix); gotCount != 2 {
		t.Fatalf("expected two boolean shadow index keys, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "Feature", "enabled")); gotCount != 2 {
		t.Fatalf("expected two property->vertex reverse index keys, got %d", gotCount)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.DeletePropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete boolean index entries failed: %v", err)
	}
	if gotCount := countByPrefix(t, store, booleanPrefix); gotCount != 0 {
		t.Fatalf("expected boolean shadow index keys to be deleted, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "Feature", "enabled")); gotCount != 0 {
		t.Fatalf("expected property->vertex reverse index keys to be deleted, got %d", gotCount)
	}
}

func TestPropertyIndexDateTimeRangeScan(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entries := []*graph.PropertyIndexEntry{
		{Tenant: "acme", Schema: "Event", Property: "startedAt", Value: []byte("map[__temporal_type:datetime day:1 hour:9 minute:0 month:5 nanosecond:0 second:0 timezone:+00:00 year:2024]"), EntityID: "e1", EntityClass: "vertex"},
		{Tenant: "acme", Schema: "Event", Property: "startedAt", Value: []byte("map[__temporal_type:datetime day:1 hour:12 minute:0 month:5 nanosecond:0 second:0 timezone:+00:00 year:2024]"), EntityID: "e2", EntityClass: "vertex"},
		{Tenant: "acme", Schema: "Event", Property: "startedAt", Value: []byte("map[__temporal_type:datetime day:1 hour:15 minute:0 month:5 nanosecond:0 second:0 timezone:+00:00 year:2024]"), EntityID: "e3", EntityClass: "vertex"},
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("put datetime index entries failed: %v", err)
	}

	var got []string
	start := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 5, 1, 14, 0, 0, 0, time.UTC)
	err = store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexDateTimeRange(ctx, "acme", "Event", "startedAt", start, true, true, end, true, true, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				got = append(got, entry.EntityID)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("datetime range scan failed: %v", err)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "e2" {
		t.Fatalf("unexpected datetime range entity ids: %#v", got)
	}

	datetimePrefix := keyspace.PropertyIndexDateTimePrefix("acme", "Event", "startedAt")
	if gotCount := countByPrefix(t, store, datetimePrefix); gotCount != 3 {
		t.Fatalf("expected three datetime shadow index keys, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "Event", "startedAt")); gotCount != 3 {
		t.Fatalf("expected three property->vertex reverse index keys, got %d", gotCount)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.DeletePropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete datetime index entries failed: %v", err)
	}
	if gotCount := countByPrefix(t, store, datetimePrefix); gotCount != 0 {
		t.Fatalf("expected datetime shadow index keys to be deleted, got %d", gotCount)
	}
	if gotCount := countByPrefix(t, store, keyspace.PropertyVertexPrefix("acme", "Event", "startedAt")); gotCount != 0 {
		t.Fatalf("expected property->vertex reverse index keys to be deleted, got %d", gotCount)
	}
}

func TestEdgePropertyIndexPayloadCarriesEndpoints(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entry := &graph.PropertyIndexEntry{
		Tenant:      "acme",
		Schema:      "RATED",
		Property:    "rating",
		Value:       []byte("4.5"),
		EntityID:    "e45",
		EntityClass: "edge",
		EdgeSrcID:   "u1",
		EdgeDstID:   "m1",
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutPropertyIndex(ctx, entry)
	})
	if err != nil {
		t.Fatalf("put edge index entry failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, "acme", "RATED", "rating", []byte("4.5"), 0, func(found *graph.PropertyIndexEntry) error {
			if found == nil {
				t.Fatalf("expected edge property index entry")
			}
			if found.EdgeSrcID != "u1" || found.EdgeDstID != "m1" {
				t.Fatalf("expected src/dst metadata u1->m1, got %q->%q", found.EdgeSrcID, found.EdgeDstID)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexNumericRange(ctx, "acme", "RATED", "rating", 4.5, true, true, 4.5, true, true, 0, func(found *graph.PropertyIndexEntry) error {
			if found == nil {
				t.Fatalf("expected edge numeric shadow entry")
			}
			if found.EdgeSrcID != "u1" || found.EdgeDstID != "m1" {
				t.Fatalf("expected numeric shadow src/dst metadata u1->m1, got %q->%q", found.EdgeSrcID, found.EdgeDstID)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("scan numeric property index failed: %v", err)
	}
}

func TestScanOutEdgeProperty(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}, {Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}, {Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}}); err != nil {
			return err
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1"},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2"},
			{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m3"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		entries := []*graph.PropertyIndexEntry{
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("5.0"), EntityID: "e1", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m1"},
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("4.5"), EntityID: "e2", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m2"},
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("5.0"), EntityID: "e3", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m3"},
		}
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		ids := make([]string, 0)
		if err := tx.ScanOutEdgeProperty(ctx, "acme", "u1", "RATED", "rating", []byte("5.0"), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry == nil {
				t.Fatalf("expected edge property entry")
			}
			if entry.EdgeSrcID != "u1" {
				t.Fatalf("expected source u1, got %q", entry.EdgeSrcID)
			}
			ids = append(ids, entry.EntityID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(ids)
		if len(ids) != 2 || ids[0] != "e1" || ids[1] != "e3" {
			return fmt.Errorf("unexpected out-edge property scan result: %#v", ids)
		}

		limited := make([]string, 0)
		if err := tx.ScanOutEdgeProperty(ctx, "acme", "u1", "RATED", "rating", []byte("5.0"), 1, func(entry *graph.PropertyIndexEntry) error {
			limited = append(limited, entry.EntityID)
			return nil
		}); err != nil {
			return err
		}
		if len(limited) != 1 {
			return fmt.Errorf("expected one limited result, got %d", len(limited))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan out edge property failed: %v", err)
	}
}

func TestScanOutEdgePropertyNumericRange(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "u1", Labels: []string{"User"}}, {Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}, {Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}, {Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}}); err != nil {
			return err
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1"},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2"},
			{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m3"},
		}
		for _, edge := range edges {
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{edge}); err != nil {
				return err
			}
		}
		entries := []*graph.PropertyIndexEntry{
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("3.9"), EntityID: "e1", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m1"},
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("4.6"), EntityID: "e2", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m2"},
			{Tenant: "acme", Schema: "RATED", Property: "rating", Value: []byte("5.0"), EntityID: "e3", EntityClass: "edge", EdgeSrcID: "u1", EdgeDstID: "m3"},
		}
		for _, entry := range entries {
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		ids := make([]string, 0)
		if err := tx.ScanOutEdgePropertyNumericRange(ctx, "acme", "u1", "RATED", "rating", 4.5, true, true, 0, false, false, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry == nil {
				t.Fatalf("expected edge property entry")
			}
			ids = append(ids, entry.EntityID)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(ids)
		if len(ids) != 2 || ids[0] != "e2" || ids[1] != "e3" {
			return fmt.Errorf("unexpected numeric range result: %#v", ids)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan out edge property numeric range failed: %v", err)
	}
}

func TestReadOnlyTxRejectsWrites(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin readonly tx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "v-ro"}})
	if !graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected unsupported error kind, got %v", err)
	}
}

func TestUpdateRollsBackOnCallbackError(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	boom := errors.New("boom")
	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "v-rollback"},
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected callback error, got %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		_, err := tx.GetVertex(ctx, "acme", "v-rollback")
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return fmt.Errorf("expected not found after rollback, got %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("rollback verification failed: %v", err)
	}
}

func TestUpdateRejectsBatchLargerThanConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	store, err := OpenWithOptions(dbPath, StoreOptions{MaxWriteBatchBytes: 1024})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	largeValue := strings.Repeat("a", 2048)
	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{{
			Tenant:     "acme",
			ID:         "too-large",
			Properties: graph.PropertyMap{"payload": []byte(largeValue)},
		}})
	})
	if err == nil {
		t.Fatalf("expected oversized batch error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected invalid input error kind, got %v", err)
	}
	if !strings.Contains(err.Error(), "max_write_batch_bytes") {
		t.Fatalf("expected max_write_batch_bytes in error, got %v", err)
	}
}

func TestDeleteBatchCanExceedConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	store, err := OpenWithOptions(dbPath, StoreOptions{MaxWriteBatchBytes: 256})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	const vertexCount = 16
	for i := 0; i < vertexCount; i++ {
		err = store.Update(ctx, func(tx graph.Tx) error {
			return tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: fmt.Sprintf("v-%d", i)}})
		})
		if err != nil {
			t.Fatalf("seed vertex %d failed: %v", i, err)
		}
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		vertexIDs := make([]string, vertexCount)
		for i := 0; i < vertexCount; i++ {
			vertexIDs[i] = fmt.Sprintf("v-%d", i)
		}
		return tx.DeleteVertexBatch(ctx, "acme", vertexIDs)
	})
	if err != nil {
		t.Fatalf("delete batch failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		for i := 0; i < vertexCount; i++ {
			_, err := tx.GetVertex(ctx, "acme", fmt.Sprintf("v-%d", i))
			if !graph.IsKind(err, graph.ErrKindNotFound) {
				return fmt.Errorf("expected vertex v-%d to be absent, got %w", i, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete verification failed: %v", err)
	}
}

func TestOpenWithOptionsAppliesPebbleMemoryOverrides(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	pebbleOpts := &cpebble.Options{}
	store, err := OpenWithOptions(dbPath, StoreOptions{
		PebbleOptions:                     pebbleOpts,
		PebbleBlockCacheBytes:             1 << 20,
		PebbleMemTableSizeBytes:           1 << 19,
		PebbleMemTableStopWritesThreshold: 4,
	})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}

	if store.ownedCache == nil {
		t.Fatalf("expected store to own configured Pebble cache")
	}
	if pebbleOpts.Cache == nil {
		t.Fatalf("expected Pebble options cache to be configured")
	}
	if got, want := pebbleOpts.MemTableSize, uint64(1<<19); got != want {
		t.Fatalf("expected MemTableSize=%d, got %d", want, got)
	}
	if got, want := pebbleOpts.MemTableStopWritesThreshold, 4; got != want {
		t.Fatalf("expected MemTableStopWritesThreshold=%d, got %d", want, got)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store failed: %v", err)
	}
	if store.ownedCache != nil {
		t.Fatalf("expected owned cache to be released on close")
	}
}

func TestOpenWithOptionsDoesNotOverrideProvidedPebbleCache(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	providedCache := cpebble.NewCache(2 << 20)
	defer providedCache.Unref()
	pebbleOpts := &cpebble.Options{Cache: providedCache}

	store, err := OpenWithOptions(dbPath, StoreOptions{
		PebbleOptions:         pebbleOpts,
		PebbleBlockCacheBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.ownedCache != nil {
		t.Fatalf("expected store not to own cache when PebbleOptions.Cache is preconfigured")
	}
	if pebbleOpts.Cache != providedCache {
		t.Fatalf("expected provided cache pointer to be preserved")
	}
}

func TestEdgeUpdateRewritesAdjacencyIndexes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{
			Tenant: "acme",
			ID:     "e-rewrite",
			Type:   "MEMBER_OF",
			SrcID:  "u1",
			DstID:  "g1",
		}}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{{
			Tenant: "acme",
			ID:     "e-rewrite",
			Type:   "OWNS",
			SrcID:  "u2",
			DstID:  "g2",
		}})
	})
	if err != nil {
		t.Fatalf("edge rewrite failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		oldOut := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			oldOut++
			return nil
		}); err != nil {
			return err
		}
		if oldOut != 0 {
			return fmt.Errorf("expected stale out adjacency removed, got %d", oldOut)
		}

		newOut := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u2", "OWNS", 10, func(edge *graph.Edge) error {
			newOut++
			if edge.ID != "e-rewrite" {
				return fmt.Errorf("unexpected edge id %s", edge.ID)
			}
			return nil
		}); err != nil {
			return err
		}
		if newOut != 1 {
			return fmt.Errorf("expected one rewritten out adjacency, got %d", newOut)
		}

		oldIn := 0
		if err := tx.ScanInEdges(ctx, "acme", "g1", "", 10, func(edge *graph.Edge) error {
			oldIn++
			return nil
		}); err != nil {
			return err
		}
		if oldIn != 0 {
			return fmt.Errorf("expected stale in adjacency removed, got %d", oldIn)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("adjacency rewrite verification failed: %v", err)
	}
}

func TestCanceledContextReturnsTimeoutKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: "v-canceled"}})
	})
	if !graph.IsKind(err, graph.ErrKindTimeout) {
		t.Fatalf("expected timeout error kind, got %v", err)
	}
}

func TestConcurrentUpdateWritesDeterministicRecords(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const writers = 24
	var wg sync.WaitGroup
	errCh := make(chan error, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("v-%02d", i)
			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "acme", ID: id}})
			})
			errCh <- err
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent update failed: %v", err)
		}
	}

	err := store.View(ctx, func(tx graph.Tx) error {
		for i := 0; i < writers; i++ {
			id := fmt.Sprintf("v-%02d", i)
			if _, err := tx.GetVertex(ctx, "acme", id); err != nil {
				return fmt.Errorf("vertex %s missing: %w", id, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("concurrent write verification failed: %v", err)
	}
}

func TestConcurrentEdgeMutationStressSameIDPoolCompletes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const (
		workers        = 12
		opsPerWorker   = 120
		edgeIDPool     = 16
		vertexIDPool   = 10
		relTypeVariety = 4
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				edgeID := fmt.Sprintf("e-%02d", (worker+i)%edgeIDPool)
				srcID := fmt.Sprintf("u-%02d", (worker+(i*3))%vertexIDPool)
				dstID := fmt.Sprintf("g-%02d", (worker+(i*5))%vertexIDPool)
				typeName := fmt.Sprintf("REL_%d", (worker+i)%relTypeVariety)

				if (worker+i)%3 == 0 {
					err := store.Update(ctx, func(tx graph.Tx) error {
						return tx.DeleteEdgeBatch(ctx, "acme", []string{edgeID})
					})
					if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
						errCh <- err
						return
					}
					continue
				}

				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.PutEdgeBatch(ctx, []*graph.Edge{{
						Tenant: "acme",
						ID:     edgeID,
						Type:   typeName,
						SrcID:  srcID,
						DstID:  dstID,
					}})
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent edge mutation failed: %v", err)
		}
	}

	if err := store.View(context.Background(), func(tx graph.Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("final read failed: %v", err)
	}
}

func TestConcurrentEdgeMutationWithReadersStressCompletes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const (
		writerWorkers  = 8
		readerWorkers  = 6
		opsPerWriter   = 100
		edgeIDPool     = 20
		vertexIDPool   = 12
		relTypeVariety = 3
	)

	var writers sync.WaitGroup
	writeErrCh := make(chan error, writerWorkers)

	for w := 0; w < writerWorkers; w++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for i := 0; i < opsPerWriter; i++ {
				edgeID := fmt.Sprintf("e-rw-%02d", (worker+i)%edgeIDPool)
				srcID := fmt.Sprintf("ru-%02d", (worker+i)%vertexIDPool)
				dstID := fmt.Sprintf("rg-%02d", (worker+(i*7))%vertexIDPool)
				typeName := fmt.Sprintf("RW_%d", (worker+i)%relTypeVariety)

				if (worker+i)%4 == 0 {
					err := store.Update(ctx, func(tx graph.Tx) error {
						return tx.DeleteEdgeBatch(ctx, "acme", []string{edgeID})
					})
					if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
						writeErrCh <- err
						return
					}
					continue
				}

				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.PutEdgeBatch(ctx, []*graph.Edge{{
						Tenant: "acme",
						ID:     edgeID,
						Type:   typeName,
						SrcID:  srcID,
						DstID:  dstID,
					}})
				})
				if err != nil {
					writeErrCh <- err
					return
				}
			}
		}(w)
	}

	var readers sync.WaitGroup
	readErrCh := make(chan error, readerWorkers)
	for r := 0; r < readerWorkers; r++ {
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			for i := 0; i < 120; i++ {
				vertexID := fmt.Sprintf("ru-%02d", (reader+i)%vertexIDPool)
				err := store.View(ctx, func(tx graph.Tx) error {
					return tx.ScanOutEdges(ctx, "acme", vertexID, "", 25, func(edge *graph.Edge) error {
						if edge == nil {
							return errors.New("nil edge observed during scan")
						}
						if edge.ID == "" || edge.SrcID == "" || edge.DstID == "" || edge.Type == "" {
							return errors.New("incomplete edge observed during scan")
						}
						return nil
					})
				})
				if err != nil {
					readErrCh <- err
					return
				}
			}
		}(r)
	}

	writers.Wait()
	close(writeErrCh)
	for err := range writeErrCh {
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
	}

	readers.Wait()
	close(readErrCh)
	for err := range readErrCh {
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
	}

	if err := store.View(context.Background(), func(tx graph.Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("final read failed: %v", err)
	}
}

func TestInjectedMetricsObserveTxAndOperations(t *testing.T) {
	ctx := context.Background()
	metrics := newRecordingMetrics()
	store := openTempStoreWithMetrics(t, metrics)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
			{Tenant: "acme", ID: "u-m1"},
		}); err != nil {
			return err
		}
		return tx.PutEdgeBatch(ctx, []*graph.Edge{
			{
				Tenant: "acme",
				ID:     "e-m1",
				Type:   "LINKS",
				SrcID:  "u-m1",
				DstID:  "u-m2",
			},
		})
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	_ = store.View(ctx, func(tx graph.Tx) error {
		_, _ = tx.GetVertex(ctx, "acme", "missing")
		return tx.ScanOutEdges(ctx, "acme", "u-m1", "", 10, func(edge *graph.Edge) error {
			return nil
		})
	})

	if got := metrics.txCount(graph.TxReadWrite, "ok"); got == 0 {
		t.Fatalf("expected at least one successful read-write tx observation")
	}
	if got := metrics.txCount(graph.TxReadOnly, "ok"); got == 0 {
		t.Fatalf("expected at least one successful read-only tx observation")
	}
	if got := metrics.opCount("put_vertex_batch", "ok"); got == 0 {
		t.Fatalf("expected put_vertex_batch operation observation")
	}
	if got := metrics.opCount("put_edge_batch", "ok"); got == 0 {
		t.Fatalf("expected put_edge_batch operation observation")
	}
	if got := metrics.opCount("get_vertex", "not_found"); got == 0 {
		t.Fatalf("expected get_vertex not_found observation")
	}
	if got := metrics.opCount("scan_out_edges", "ok"); got == 0 {
		t.Fatalf("expected scan_out_edges observation")
	}
}

func BenchmarkEdgeMutationLowContentionParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			edgeID := fmt.Sprintf("e-low-%d", n)
			srcID := fmt.Sprintf("u-low-%d", n%1024)
			dstID := fmt.Sprintf("g-low-%d", (n*7)%1024)
			typeName := fmt.Sprintf("REL_%d", n%8)

			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdgeBatch(ctx, []*graph.Edge{{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				}})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark write failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func BenchmarkEdgeMutationHighContentionParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	const (
		edgeIDPool = 16
		vertexPool = 64
	)

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			edgeID := fmt.Sprintf("e-hot-%02d", n%edgeIDPool)
			srcID := fmt.Sprintf("u-hot-%02d", n%vertexPool)
			dstID := fmt.Sprintf("g-hot-%02d", (n*5)%vertexPool)
			typeName := fmt.Sprintf("HOT_%d", n%4)

			if n%4 == 0 {
				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.DeleteEdgeBatch(ctx, "acme", []string{edgeID})
				})
				if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				continue
			}

			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdgeBatch(ctx, []*graph.Edge{{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				}})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark hot mutation failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func BenchmarkEdgeMutationMixedReadWriteParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	const (
		seedEdges  = 256
		edgeIDPool = 64
		vertexPool = 64
	)

	for i := 0; i < seedEdges; i++ {
		edgeID := fmt.Sprintf("e-seed-%03d", i)
		srcID := fmt.Sprintf("u-seed-%02d", i%vertexPool)
		dstID := fmt.Sprintf("g-seed-%02d", (i*3)%vertexPool)
		err := store.Update(ctx, func(tx graph.Tx) error {
			return tx.PutEdgeBatch(ctx, []*graph.Edge{{
				Tenant: "acme",
				ID:     edgeID,
				Type:   "SEEDED",
				SrcID:  srcID,
				DstID:  dstID,
			}})
		})
		if err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			if n%5 == 0 {
				vertexID := fmt.Sprintf("u-seed-%02d", n%vertexPool)
				err := store.View(ctx, func(tx graph.Tx) error {
					return tx.ScanOutEdges(ctx, "acme", vertexID, "", 20, func(edge *graph.Edge) error {
						if edge == nil {
							return errors.New("nil edge observed")
						}
						return nil
					})
				})
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				continue
			}

			edgeID := fmt.Sprintf("e-mix-%02d", n%edgeIDPool)
			srcID := fmt.Sprintf("u-seed-%02d", n%vertexPool)
			dstID := fmt.Sprintf("g-seed-%02d", (n*11)%vertexPool)
			typeName := fmt.Sprintf("MIX_%d", n%6)
			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdgeBatch(ctx, []*graph.Edge{{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				}})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark mixed workload failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func openTempStore(t *testing.T) *Store {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}

func openTempStoreWithMetrics(t *testing.T, metrics Metrics) *Store {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := OpenWithOptions(dbPath, StoreOptions{Metrics: metrics})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}

func openTempStoreB(b *testing.B) *Store {
	b.Helper()
	base := b.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		b.Fatalf("mkdir failed: %v", err)
	}
	store, err := Open(dbPath)
	if err != nil {
		b.Fatalf("open store failed: %v", err)
	}
	return store
}

func countByPrefix(t *testing.T, store *Store, prefix []byte) int {
	t.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		t.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return count
}

func wipePrefixKeys(t *testing.T, store *Store, prefix []byte) {
	t.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		t.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	keys := make([][]byte, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	for _, key := range keys {
		if err := store.db.Delete(key, nil); err != nil {
			t.Fatalf("delete key %q failed: %v", key, err)
		}
	}
}

func assertAdjacencyConsistencyB(b *testing.B, store *Store, tenant string) {
	b.Helper()

	edges := readAllEdgesByIDB(b, store, tenant)

	for edgeID, edge := range edges {
		edgeTypeKey := keyspace.EdgeTypeKey(tenant, edgeID, edge.Type)
		typeEdgeKey := keyspace.TypeEdgeKey(tenant, edge.Type, edgeID)
		outKey := keyspace.OutAdjacencyKey(tenant, edge.SrcID, edge.Type, edgeID)
		inKey := keyspace.InAdjacencyKey(tenant, edge.DstID, edge.Type, edgeID)
		if !dbHasKeyB(b, store, edgeTypeKey) {
			b.Fatalf("missing edge type index for edge %s", edgeID)
		}
		if !dbHasKeyB(b, store, typeEdgeKey) {
			b.Fatalf("missing type edge index for edge %s", edgeID)
		}
		if !dbHasKeyB(b, store, outKey) {
			b.Fatalf("missing out adjacency for edge %s", edgeID)
		}
		if !dbHasKeyB(b, store, inKey) {
			b.Fatalf("missing in adjacency for edge %s", edgeID)
		}
	}

	outAdjCount := 0
	iteratePrefixB(b, store, keyspace.OutAdjacencyTenantPrefix(tenant), func(key, _ []byte) {
		outAdjCount++
		kTenant, srcID, edgeType, edgeID, ok := parseOutAdjacencyKey(key)
		if !ok {
			b.Fatalf("malformed out adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			b.Fatalf("orphan out adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.SrcID != srcID || edge.Type != edgeType {
			b.Fatalf("stale out adjacency key for edge %s", edgeID)
		}
	})

	inAdjCount := 0
	iteratePrefixB(b, store, keyspace.InAdjacencyTenantPrefix(tenant), func(key, _ []byte) {
		inAdjCount++
		kTenant, dstID, edgeType, edgeID, ok := parseInAdjacencyKey(key)
		if !ok {
			b.Fatalf("malformed in adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			b.Fatalf("orphan in adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.DstID != dstID || edge.Type != edgeType {
			b.Fatalf("stale in adjacency key for edge %s", edgeID)
		}
	})

	if outAdjCount != len(edges) {
		b.Fatalf("out adjacency count mismatch: got=%d expected=%d", outAdjCount, len(edges))
	}
	if inAdjCount != len(edges) {
		b.Fatalf("in adjacency count mismatch: got=%d expected=%d", inAdjCount, len(edges))
	}
}

func readAllEdgesByIDB(b *testing.B, store *Store, tenant string) map[string]*graph.Edge {
	b.Helper()

	out := make(map[string]*graph.Edge)
	edgeIDs := make([]string, 0)
	iteratePrefixB(b, store, keyspace.EdgePrefix(tenant), func(key, value []byte) {
		edgeID := edgeIDFromAdjKey(key)
		if edgeID == "" {
			b.Fatalf("malformed edge key %q", key)
		}
		edgeIDs = append(edgeIDs, edgeID)
	})
	if err := store.View(context.Background(), func(tx graph.Tx) error {
		for _, edgeID := range edgeIDs {
			edge, err := tx.GetEdge(context.Background(), tenant, edgeID)
			if err != nil {
				return err
			}
			out[edgeID] = edge
		}
		return nil
	}); err != nil {
		b.Fatalf("hydrate edges failed: %v", err)
	}
	return out
}

func dbHasKey(t *testing.T, store *Store, key []byte) bool {
	t.Helper()
	_, closer, err := store.db.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("db get failed for key %q: %v", key, err)
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true
}

func dbHasKeyB(b *testing.B, store *Store, key []byte) bool {
	b.Helper()
	_, closer, err := store.db.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return false
	}
	if err != nil {
		b.Fatalf("db get failed for key %q: %v", key, err)
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true
}

func iteratePrefixB(b *testing.B, store *Store, prefix []byte, fn func(key, value []byte)) {
	b.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		b.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		fn(k, v)
	}
	if err := iter.Error(); err != nil {
		b.Fatalf("iter error: %v", err)
	}
}

func parseOutAdjacencyKey(key []byte) (tenant, srcID, edgeType, edgeID string, ok bool) {
	parts := strings.Split(string(key), "/")
	if len(parts) < 5 {
		return "", "", "", "", false
	}
	return parts[len(parts)-4], parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1], true
}

func parseInAdjacencyKey(key []byte) (tenant, dstID, edgeType, edgeID string, ok bool) {
	parts := strings.Split(string(key), "/")
	if len(parts) < 5 {
		return "", "", "", "", false
	}
	return parts[len(parts)-4], parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1], true
}

type recordingMetrics struct {
	mu           sync.Mutex
	txCounts     map[string]int
	opCounts     map[string]int
	conflictIncs int
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{
		txCounts: make(map[string]int),
		opCounts: make(map[string]int),
	}
}

func (m *recordingMetrics) ObserveTx(mode graph.TxMode, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txCounts[fmt.Sprintf("%d|%s", mode, outcome)]++
}

func (m *recordingMetrics) ObserveOperation(name, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opCounts[name+"|"+outcome]++
}

func (m *recordingMetrics) IncTxConflict() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conflictIncs++
}

func (m *recordingMetrics) txCount(mode graph.TxMode, outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.txCounts[fmt.Sprintf("%d|%s", mode, outcome)]
}

func (m *recordingMetrics) opCount(name, outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opCounts[name+"|"+outcome]
}
