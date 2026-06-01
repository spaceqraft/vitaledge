package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

func TestCreateEdgePropertyIndexBackfillsExistingEdges(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e1",
			Type:   "RATED",
			SrcID:  "u1",
			DstID:  "m1",
			Properties: map[string][]byte{
				"rating": valueToBytes(5.0),
			},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	created, indexed, err := exec.CreateEdgePropertyIndex(ctx, "acme", "RATED", "rating", false)
	if err != nil {
		t.Fatalf("CreateEdgePropertyIndex failed: %v", err)
	}
	if !created {
		t.Fatalf("expected index to be created")
	}
	if indexed != 1 {
		t.Fatalf("expected one indexed edge, got %d", indexed)
	}
	if !catalog.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected edge property index in catalog")
	}

	found := false
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, "acme", "RATED", "rating", valueToBytes(5.0), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" && entry.EntityID == "e1" {
				found = true
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if !found {
		t.Fatalf("expected backfilled edge index entry for e1")
	}
}

func TestCallCreatePropertyIndexBackfillsExistingVertexes(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "acme",
			ID:     "u1",
			Labels: []string{"User"},
			Properties: map[string][]byte{
				"email": valueToBytes("alice@example.com"),
			},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	stmt, err := parser.ParseStatement("CALL db.index.createProperty('User', 'email') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one result row, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["created"] != true {
		t.Fatalf("expected created=true, got %#v", res.Rows[0]["created"])
	}
	if res.Rows[0]["indexedEntities"] != 1 {
		t.Fatalf("expected indexedEntities=1, got %#v", res.Rows[0]["indexedEntities"])
	}
	if !catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected vertex property index in catalog")
	}

	found := false
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, "acme", "User", "email", valueToBytes("alice@example.com"), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "vertex" && entry.EntityID == "u1" {
				found = true
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if !found {
		t.Fatalf("expected backfilled vertex index entry for u1")
	}
}

func TestCallCreateEdgePropertyIndexEnqueuesBackgroundBuild(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e1",
			Type:   "RATED",
			SrcID:  "u1",
			DstID:  "m1",
			Properties: map[string][]byte{
				"rating": valueToBytes(5.0),
			},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	stmt, err := parser.ParseStatement("CALL db.index.createEdgeProperty('RATED', 'rating') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one result row, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["created"] != true {
		t.Fatalf("expected created=true, got %#v", res.Rows[0]["created"])
	}
	if res.Rows[0]["indexedEntities"] != 0 {
		t.Fatalf("expected indexedEntities=0 for async enqueue, got %#v", res.Rows[0]["indexedEntities"])
	}
	if !catalog.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected edge property index in catalog")
	}
	jobs, err := exec.listEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one pending edge index job, got %#v", jobs)
	}

	processed, err := exec.processPendingEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("process jobs failed: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job, got %d", processed)
	}

	found := false
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, "acme", "RATED", "rating", valueToBytes(5.0), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" && entry.EntityID == "e1" {
				found = true
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if !found {
		t.Fatalf("expected backfilled edge index entry for e1")
	}
}

func TestEdgeIndexBuildJobResumesAcrossExecutorRestart(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e1",
			Type:   "RATED",
			SrcID:  "u1",
			DstID:  "m1",
			Properties: map[string][]byte{
				"rating": valueToBytes(5.0),
			},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalogA := indexschema.NewCatalog()
	execA := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalogA})
	created, indexed, err := execA.CreateEdgePropertyIndexAsync(ctx, "acme", "RATED", "rating", false)
	if err != nil {
		t.Fatalf("CreateEdgePropertyIndexAsync failed: %v", err)
	}
	if !created || indexed != 0 {
		t.Fatalf("expected created async job with indexedEntities=0, got created=%v indexed=%d", created, indexed)
	}

	jobs, err := execA.listEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one pending job before restart, got %#v", jobs)
	}

	catalogB := indexschema.NewCatalog()
	execB := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalogB})
	processed, err := execB.processPendingEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("process jobs after restart failed: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed job after restart, got %d", processed)
	}
	if !catalogB.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected resumed executor to load edge index into runtime catalog")
	}

	jobs, err = execB.listEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs after processing failed: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no pending jobs after processing, got %#v", jobs)
	}

	found := false
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, "acme", "RATED", "rating", valueToBytes(5.0), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" && entry.EntityID == "e1" {
				found = true
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if !found {
		t.Fatalf("expected backfilled edge index entry after restart processing")
	}
}

func TestEdgeIndexBuildJobResumesFromCheckpointAcrossRestart(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "movie", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		for i := 0; i < 70; i++ {
			vertexID := fmt.Sprintf("u%03d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: vertexID, Labels: []string{"User"}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: fmt.Sprintf("e%03d", i), Type: "RATED", SrcID: vertexID, DstID: "movie", Properties: map[string][]byte{"rating": valueToBytes(float64(i))}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalogA := indexschema.NewCatalog()
	execA := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalogA})
	created, indexed, err := execA.CreateEdgePropertyIndexAsync(ctx, "acme", "RATED", "rating", false)
	if err != nil {
		t.Fatalf("CreateEdgePropertyIndexAsync failed: %v", err)
	}
	if !created || indexed != 0 {
		t.Fatalf("expected created async job with indexedEntities=0, got created=%v indexed=%d", created, indexed)
	}

	processed, err := execA.processPendingEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("first processing pass failed: %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected no completed jobs after first pass, got %d", processed)
	}

	progress, err := execA.listPendingEdgeIndexBuildProgress(ctx)
	if err != nil {
		t.Fatalf("list progress failed: %v", err)
	}
	if len(progress) != 1 {
		t.Fatalf("expected one pending job, got %#v", progress)
	}
	if progress[0].CheckpointVertexID == "" {
		t.Fatalf("expected checkpoint vertex ID after first pass, got %#v", progress[0])
	}
	if progress[0].IndexedEdges == 0 {
		t.Fatalf("expected some indexed edges after first pass, got %#v", progress[0])
	}

	catalogB := indexschema.NewCatalog()
	execB := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalogB})
	processed, err = execB.processPendingEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("second processing pass failed: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected completed job after restart pass, got %d", processed)
	}
	if !catalogB.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected resumed executor to load edge index into runtime catalog")
	}

	jobs, err := execB.listEdgeIndexBuildJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs after completion failed: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no pending jobs after completion, got %#v", jobs)
	}

	count := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, "acme", "RATED", "rating", 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" {
				count++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if count != 70 {
		t.Fatalf("expected 70 edge index entries, got %d", count)
	}
}

func TestExplainRelationshipUsesEdgePropertyIndexAccessPath(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(3.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (u:User)-[r:RATED {rating:5.0}]->(m:Movie) RETURN m.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute explain failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected explain output row")
	}

	explain, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	physical, ok := explain["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %#v", explain["physicalPlan"])
	}
	vertexes, ok := physical["vertexes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected plan vertexes, got %#v", physical["vertexes"])
	}

	found := false
	for _, vertex := range vertexes {
		op, _ := vertex["op"].(string)
		if op != "EDGE_SCAN" && op != "OPTIONAL_EDGE_SCAN" {
			continue
		}
		if accessPath, _ := vertex["accessPath"].(string); accessPath == "property_index(RATED.rating)" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected edge scan accessPath property_index(RATED.rating), got %#v", vertexes)
	}
}

func TestExplainRelationshipRangeWhereUsesEdgePropertyIndexAccessPath(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(3.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating >= 4.0 RETURN m.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute explain failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected explain output row")
	}

	explain, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	physical, ok := explain["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %#v", explain["physicalPlan"])
	}
	vertexes, ok := physical["vertexes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected plan vertexes, got %#v", physical["vertexes"])
	}

	found := false
	for _, vertex := range vertexes {
		op, _ := vertex["op"].(string)
		if op != "EDGE_SCAN" && op != "OPTIONAL_EDGE_SCAN" {
			continue
		}
		if accessPath, _ := vertex["accessPath"].(string); accessPath == "property_index(RATED.rating)" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected edge scan accessPath property_index(RATED.rating), got %#v", vertexes)
	}
}

func TestEdgeRangeWherePredicateUsesIndexPushdown(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(2.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating >= 4.0 RETURN r.id AS id ORDER BY id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["id"] != "e1" || res.Rows[1]["id"] != "e2" {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}

	snapshot := collector.Snapshot()
	lookup := snapshot.IndexLookups[IndexLookupKey{Strategy: "edge_property_index_range", Outcome: "hit"}]
	if lookup.Count < 1 {
		t.Fatalf("expected edge_property_index_range hit lookup, got %#v", snapshot.IndexLookups)
	}
}

func TestEdgeBoundedRangeWherePredicateUsesIndexPushdown(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m4", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.5)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e4", Type: "RATED", SrcID: "u1", DstID: "m4", Properties: map[string][]byte{"rating": valueToBytes(2.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating > 4.0 AND r.rating <= 5.0 RETURN r.id AS id ORDER BY id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["id"] != "e1" || res.Rows[1]["id"] != "e2" {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}

	snapshot := collector.Snapshot()
	lookup := snapshot.IndexLookups[IndexLookupKey{Strategy: "edge_property_index_range", Outcome: "hit"}]
	if lookup.Count < 1 {
		t.Fatalf("expected edge_property_index_range hit lookup, got %#v", snapshot.IndexLookups)
	}
}

func TestExtractEdgeWhereNumericConstraintsSupportsAbsDifference(t *testing.T) {
	row := Row{
		"r1": &graph.Edge{Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
	}

	constraints, ok := extractEdgeWhereNumericConstraints("abs(r1.rating - r2.rating) <= 1.5", "r2", row, Params{})
	if !ok {
		t.Fatalf("expected numeric constraints to be extracted")
	}
	rating, exists := constraints["rating"]
	if !exists {
		t.Fatalf("expected rating constraint, got %#v", constraints)
	}
	if !rating.matchesValue(3.0) || !rating.matchesValue(6.0) {
		t.Fatalf("expected bounds [3.0, 6.0], got %#v", rating)
	}
	if rating.matchesValue(2.9) || rating.matchesValue(6.1) {
		t.Fatalf("expected out-of-range values to fail, got %#v", rating)
	}
}

func TestEdgeContradictoryRangeWherePredicateShortCircuitsToMiss(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating > 5.0 AND r.rating < 4.0 RETURN r.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d (%#v)", len(res.Rows), res.Rows)
	}

	snapshot := collector.Snapshot()
	lookup := snapshot.IndexLookups[IndexLookupKey{Strategy: "edge_property_index_range", Outcome: "miss"}]
	if lookup.Count < 1 {
		t.Fatalf("expected edge_property_index_range miss lookup, got %#v", snapshot.IndexLookups)
	}
}

func TestEdgeRangePushdownWithResidualWherePredicate(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0), "note": valueToBytes("top")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.0), "note": valueToBytes("low")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(2.0), "note": valueToBytes("top")}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating >= 4.0 AND r.note = 'top' RETURN r.id AS id ORDER BY id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["id"] != "e1" {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}

	snapshot := collector.Snapshot()
	lookup := snapshot.IndexLookups[IndexLookupKey{Strategy: "edge_property_index_range", Outcome: "hit"}]
	if lookup.Count < 1 {
		t.Fatalf("expected edge_property_index_range hit lookup, got %#v", snapshot.IndexLookups)
	}
}

func TestTargetSharedPeerAggregationShapeProducesExpectedSimilaritySeed(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(2)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u3", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(3)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u2", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e4", Type: "RATED", SrcID: "u2", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e5", Type: "RATED", SrcID: "u3", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(1.0)}},
			{Tenant: "acme", ID: "e6", Type: "RATED", SrcID: "u3", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: indexschema.NewCatalog()})
	stmt, err := parser.ParseStatement("MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff RETURN peer.user_id AS peer_id, shared_count AS shared_count, avg_diff AS avg_diff ORDER BY peer_id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d (%#v)", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["peer_id"]; got != 2 {
		t.Fatalf("expected peer_id 2, got %#v", got)
	}
	if got := res.Rows[0]["shared_count"]; got != 2 {
		t.Fatalf("expected shared_count 2, got %#v", got)
	}
	avg, ok := numericValue(res.Rows[0]["avg_diff"])
	if !ok {
		t.Fatalf("expected numeric avg_diff, got %#v", res.Rows[0]["avg_diff"])
	}
	if math.Abs(avg-1.0) > 1e-9 {
		t.Fatalf("expected avg_diff 1.0, got %v", avg)
	}
}

func TestRecommendationReturnAggregationShapeProducesExpectedCandidateScores(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(2)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(1), "base_score": valueToBytes(0.1)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(2), "base_score": valueToBytes(0.1)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(3), "base_score": valueToBytes(0.7)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m4", Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(4), "base_score": valueToBytes(0.9)}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u1", DstID: "m4", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e4", Type: "RATED", SrcID: "u2", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e5", Type: "RATED", SrcID: "u2", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e6", Type: "RATED", SrcID: "u2", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
			{Tenant: "acme", ID: "e7", Type: "RATED", SrcID: "u2", DstID: "m4", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: indexschema.NewCatalog()})
	stmt, err := parser.ParseStatement("MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 2 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, coalesce(candidate.base_score, 0.0) AS base_score, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d (%#v)", len(res.Rows), res.Rows)
	}

	if got := res.Rows[0]["movie_id"]; got != 3 {
		t.Fatalf("expected movie_id 3, got %#v", got)
	}
	baseScore, ok := numericValue(res.Rows[0]["base_score"])
	if !ok || math.Abs(baseScore-0.7) > 1e-9 {
		t.Fatalf("expected base_score 0.7, got %#v", res.Rows[0]["base_score"])
	}
	peerAvg, ok := numericValue(res.Rows[0]["peer_avg"])
	if !ok || math.Abs(peerAvg-4.5) > 1e-9 {
		t.Fatalf("expected peer_avg 4.5, got %#v", res.Rows[0]["peer_avg"])
	}
	if got := res.Rows[0]["peer_count"]; got != 1 {
		t.Fatalf("expected peer_count 1, got %#v", got)
	}
	totalSim, ok := numericValue(res.Rows[0]["total_sim"])
	if !ok || math.Abs(totalSim-1.8) > 1e-9 {
		t.Fatalf("expected total_sim 1.8, got %#v", res.Rows[0]["total_sim"])
	}

	if len(res.Warnings) == 0 {
		t.Fatalf("expected runtime counter diagnostic warning")
	}
	var payload map[string]int64
	foundRuntimeCounters := false
	for _, warning := range res.Warnings {
		if warning.Code != "RUNTIME_COUNTERS" {
			continue
		}
		if err := json.Unmarshal([]byte(warning.Message), &payload); err != nil {
			t.Fatalf("runtime counter warning payload must be json: %v", err)
		}
		foundRuntimeCounters = true
		break
	}
	if !foundRuntimeCounters {
		t.Fatalf("expected RUNTIME_COUNTERS warning, got %#v", res.Warnings)
	}
	if payload["fast_path.stage2.edges_visited"] <= 0 {
		t.Fatalf("expected fast_path.stage2.edges_visited counter > 0, got %#v", payload)
	}
}

func TestBackfillEdgePropertyIndexRespectsWriteBatchLimit(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := pebblestore.OpenWithOptions(dbPath, pebblestore.StoreOptions{MaxWriteBatchBytes: 1024})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed vertexes failed: %v", err)
	}

	payload := strings.Repeat("x", 96)
	for i := 0; i < 12; i++ {
		edgeID := fmt.Sprintf("e%d", i)
		err := store.Update(ctx, func(tx graph.Tx) error {
			return tx.PutEdge(ctx, &graph.Edge{
				Tenant: "acme",
				ID:     edgeID,
				Type:   "RATED",
				SrcID:  "u1",
				DstID:  "m1",
				Properties: map[string][]byte{
					"rating": valueToBytes(payload + edgeID),
				},
			})
		})
		if err != nil {
			t.Fatalf("seed edge %s failed: %v", edgeID, err)
		}
	}

	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: indexschema.NewCatalog()})
	indexed, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating")
	if err != nil {
		t.Fatalf("BackfillEdgePropertyIndex failed under low batch size: %v", err)
	}
	if indexed != 12 {
		t.Fatalf("expected 12 indexed edges, got %d", indexed)
	}

	count := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, "acme", "RATED", "rating", 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" {
				count++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}
	if count != 12 {
		t.Fatalf("expected 12 edge index entries, got %d", count)
	}
}

func TestRecommendationStage2EdgeIndexPushdownMatchesBaseline(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(2)}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u3", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(3)}}); err != nil {
			return err
		}
		for i := 1; i <= 6; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i), "base_score": valueToBytes(float64(i) / 10.0)}}); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u2", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
			{Tenant: "acme", ID: "e4", Type: "RATED", SrcID: "u2", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e5", Type: "RATED", SrcID: "u2", DstID: "m3", Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
			{Tenant: "acme", ID: "e6", Type: "RATED", SrcID: "u2", DstID: "m5", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e7", Type: "RATED", SrcID: "u3", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e8", Type: "RATED", SrcID: "u3", DstID: "m2", Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
			{Tenant: "acme", ID: "e9", Type: "RATED", SrcID: "u3", DstID: "m4", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e10", Type: "RATED", SrcID: "u3", DstID: "m6", Properties: map[string][]byte{"rating": valueToBytes(4.5)}},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 2 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalogBaseline := indexschema.NewCatalog()
	catalogBaseline.AddEdgePropertyIndex("acme", "RATED", "rating")
	collectorBaseline := NewCollector()
	execBaseline := New(store, Options{Metrics: collectorBaseline, IndexCatalog: catalogBaseline, DisableStage2EdgeIndexPushdown: true})
	if _, err := execBaseline.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("baseline backfill failed: %v", err)
	}
	resBaseline, err := execBaseline.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("baseline execute failed: %v", err)
	}

	catalogIndexed := indexschema.NewCatalog()
	catalogIndexed.AddEdgePropertyIndex("acme", "RATED", "rating")
	collectorIndexed := NewCollector()
	execIndexed := New(store, Options{Metrics: collectorIndexed, IndexCatalog: catalogIndexed})
	if _, err := execIndexed.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("indexed backfill failed: %v", err)
	}
	resIndexed, err := execIndexed.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("indexed execute failed: %v", err)
	}

	baselineRows := append([]Row(nil), resBaseline.Rows...)
	indexedRows := append([]Row(nil), resIndexed.Rows...)
	sortRowsByMovieID(baselineRows)
	sortRowsByMovieID(indexedRows)

	baselineJSON, err := json.Marshal(baselineRows)
	if err != nil {
		t.Fatalf("marshal baseline rows failed: %v", err)
	}
	indexedJSON, err := json.Marshal(indexedRows)
	if err != nil {
		t.Fatalf("marshal indexed rows failed: %v", err)
	}
	if string(baselineJSON) != string(indexedJSON) {
		t.Fatalf("expected identical rows, baseline=%s indexed=%s", string(baselineJSON), string(indexedJSON))
	}

	baselineCounters, err := runtimeCountersFromWarnings(resBaseline.Warnings)
	if err != nil {
		t.Fatalf("baseline runtime counters parse failed: %v", err)
	}
	indexedCounters, err := runtimeCountersFromWarnings(resIndexed.Warnings)
	if err != nil {
		t.Fatalf("indexed runtime counters parse failed: %v", err)
	}
	if baselineCounters["fast_path.stage2.index_pushdown_applied"] != 0 {
		t.Fatalf("expected baseline without index pushdown, got %#v", baselineCounters)
	}
	if indexedCounters["fast_path.stage2.index_pushdown_applied"] <= 0 {
		t.Fatalf("expected indexed run to apply stage2 index pushdown, got %#v", indexedCounters)
	}
	if indexedCounters["fast_path.stage2.index_pushdown_rows"] <= 0 {
		t.Fatalf("expected indexed run to process index-backed rows, got %#v", indexedCounters)
	}
	if indexedCounters["fast_path.stage2.index_lookup_cache_hits"] <= 0 {
		t.Fatalf("expected indexed run to reuse stage2 index lookup cache, got %#v", indexedCounters)
	}
}

func TestRecommendationStage2EdgeIndexPushdownAdaptiveSkipsUnselectiveWorkload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkGraph(ctx, store); err != nil {
		t.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "bench-rec", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("runtime counters parse failed: %v", err)
	}
	if counters["fast_path.stage2.index_pushdown_skipped_unselective"] <= 0 && counters["fast_path.stage2.index_pushdown_skipped_predicate_shape"] <= 0 {
		t.Fatalf("expected adaptive skip counter > 0, got %#v", counters)
	}
	if counters["fast_path.stage2.index_pushdown_applied"] != 0 {
		t.Fatalf("expected pushdown not applied for unselective workload, got %#v", counters)
	}
	if counters["fast_path.stage2.edges_visited"] <= 0 {
		t.Fatalf("expected fallback scan path to visit edges, got %#v", counters)
	}
}

func TestRecommendationStage2EdgeIndexPushdownSelectiveWorkloadBuildsScopedIndexCandidates(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkGraph(ctx, store); err != nil {
		t.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkSelectiveQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
	collector := NewCollector()
	exec := New(store, Options{Metrics: collector, IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "bench-rec", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("runtime counters parse failed: %v", err)
	}
	if counters["fast_path.stage2.index_lookup_cache_misses"] <= 0 {
		t.Fatalf("expected selective workload to exercise stage2 index probe path, got %#v", counters)
	}
	if counters["fast_path.stage2.index_candidates_total"] <= 0 && counters["fast_path.stage2.index_probe_cap_exceeded"] <= 0 && counters["fast_path.stage2.index_probe_source_scope_skipped_wide"] <= 0 {
		t.Fatalf("expected selective workload to report probe diagnostics (candidates, cap, or wide-scope skip), got %#v", counters)
	}
	if counters["fast_path.stage2.index_pushdown_applied"] != 0 && counters["fast_path.stage2.index_pushdown_rows"] <= 0 {
		t.Fatalf("expected pushdown rows when pushdown is applied, got %#v", counters)
	}
}

func runtimeCountersFromWarnings(warnings []Diagnostic) (map[string]int64, error) {
	payload := map[string]int64{}
	for _, warning := range warnings {
		if warning.Code != "RUNTIME_COUNTERS" {
			continue
		}
		if err := json.Unmarshal([]byte(warning.Message), &payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	return nil, fmt.Errorf("RUNTIME_COUNTERS warning not found")
}

func sortRowsByMovieID(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		left, _ := comparableNumericValue(rows[i]["movie_id"])
		right, _ := comparableNumericValue(rows[j]["movie_id"])
		return left < right
	})
}

func TestEdgeBuildJobProceduresExposeProgressAndManualControls(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(4.0)}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, _, err := exec.CreateEdgePropertyIndexAsync(ctx, "acme", "RATED", "rating", false); err != nil {
		t.Fatalf("create async edge index failed: %v", err)
	}

	statusStmt, err := parser.ParseStatement("CALL db.index.edgeBuildJobs() YIELD tenant, edgeType, property, pending, totalEdges, indexedEdges RETURN tenant, edgeType, property, pending, totalEdges, indexedEdges")
	if err != nil {
		t.Fatalf("parse status call failed: %v", err)
	}
	statusRes, err := exec.ExecuteStatement(ctx, statusStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute status call failed: %v", err)
	}
	if len(statusRes.Rows) != 1 {
		t.Fatalf("expected one pending job row, got %d (%#v)", len(statusRes.Rows), statusRes.Rows)
	}
	row := statusRes.Rows[0]
	if row["tenant"] != "acme" || row["edgeType"] != "RATED" || row["property"] != "rating" {
		t.Fatalf("unexpected status row identity: %#v", row)
	}
	if row["pending"] != true {
		t.Fatalf("expected pending=true, got %#v", row["pending"])
	}
	if row["totalEdges"] != 2 {
		t.Fatalf("expected totalEdges=2, got %#v", row["totalEdges"])
	}

	processStmt, err := parser.ParseStatement("CALL db.index.processEdgeBuildJobs() YIELD processed, pending RETURN processed, pending")
	if err != nil {
		t.Fatalf("parse process call failed: %v", err)
	}
	processRes, err := exec.ExecuteStatement(ctx, processStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute process call failed: %v", err)
	}
	if len(processRes.Rows) != 1 {
		t.Fatalf("expected one process row, got %d (%#v)", len(processRes.Rows), processRes.Rows)
	}
	if processRes.Rows[0]["processed"] != 1 || processRes.Rows[0]["pending"] != 0 {
		t.Fatalf("unexpected process result: %#v", processRes.Rows[0])
	}

	restartStmt, err := parser.ParseStatement("CALL db.index.restartEdgePropertyBuild('RATED', 'rating') YIELD enqueued RETURN enqueued")
	if err != nil {
		t.Fatalf("parse restart call failed: %v", err)
	}
	restartRes, err := exec.ExecuteStatement(ctx, restartStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute restart call failed: %v", err)
	}
	if len(restartRes.Rows) != 1 || restartRes.Rows[0]["enqueued"] != true {
		t.Fatalf("unexpected restart result: %#v", restartRes.Rows)
	}

	statusRes, err = exec.ExecuteStatement(ctx, statusStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute status call after restart failed: %v", err)
	}
	if len(statusRes.Rows) != 1 {
		t.Fatalf("expected one pending job row after restart enqueue, got %d (%#v)", len(statusRes.Rows), statusRes.Rows)
	}
}
