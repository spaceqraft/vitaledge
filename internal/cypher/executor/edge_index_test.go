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

func TestCallDropPropertyIndexRemovesEntriesAndCatalog(t *testing.T) {
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

	createStmt, err := parser.ParseStatement("CALL db.index.createProperty('User', 'email') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse create failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, createStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute create failed: %v", err)
	}

	dropStmt, err := parser.ParseStatement("CALL db.index.dropProperty('User', 'email') YIELD dropped, deletedEntities")
	if err != nil {
		t.Fatalf("parse drop failed: %v", err)
	}
	dropRes, err := exec.ExecuteStatement(ctx, dropStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute drop failed: %v", err)
	}
	if len(dropRes.Rows) != 1 {
		t.Fatalf("expected one drop result row, got %d (%#v)", len(dropRes.Rows), dropRes.Rows)
	}
	if dropRes.Rows[0]["dropped"] != true {
		t.Fatalf("expected dropped=true, got %#v", dropRes.Rows[0]["dropped"])
	}
	if dropRes.Rows[0]["deletedEntities"] != 1 {
		t.Fatalf("expected deletedEntities=1, got %#v", dropRes.Rows[0]["deletedEntities"])
	}
	if catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected vertex property index to be removed from catalog")
	}

	remaining := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, "acme", "User", "email", 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				remaining++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index all failed: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected no remaining vertex property index entries, got %d", remaining)
	}

	ifExistsStmt, err := parser.ParseStatement("CALL db.index.dropProperty('User', 'email', true) YIELD dropped, deletedEntities")
	if err != nil {
		t.Fatalf("parse ifExists drop failed: %v", err)
	}
	ifExistsRes, err := exec.ExecuteStatement(ctx, ifExistsStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute ifExists drop failed: %v", err)
	}
	if len(ifExistsRes.Rows) != 1 {
		t.Fatalf("expected one ifExists drop row, got %d (%#v)", len(ifExistsRes.Rows), ifExistsRes.Rows)
	}
	if ifExistsRes.Rows[0]["dropped"] != false {
		t.Fatalf("expected dropped=false with ifExists on missing index, got %#v", ifExistsRes.Rows[0]["dropped"])
	}
	if ifExistsRes.Rows[0]["deletedEntities"] != 0 {
		t.Fatalf("expected deletedEntities=0 with ifExists on missing index, got %#v", ifExistsRes.Rows[0]["deletedEntities"])
	}
}

func TestCallDropEdgePropertyIndexRemovesEntriesAndNumericShadow(t *testing.T) {
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
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	createStmt, err := parser.ParseStatement("CALL db.index.createEdgeProperty('RATED', 'rating') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse create failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, createStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute create failed: %v", err)
	}
	if _, err := exec.processPendingEdgeIndexBuildJobs(ctx); err != nil {
		t.Fatalf("process jobs failed: %v", err)
	}

	dropStmt, err := parser.ParseStatement("CALL db.index.dropEdgeProperty('RATED', 'rating') YIELD dropped, deletedEntities")
	if err != nil {
		t.Fatalf("parse drop failed: %v", err)
	}
	dropRes, err := exec.ExecuteStatement(ctx, dropStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute drop failed: %v", err)
	}
	if len(dropRes.Rows) != 1 {
		t.Fatalf("expected one drop result row, got %d (%#v)", len(dropRes.Rows), dropRes.Rows)
	}
	if dropRes.Rows[0]["dropped"] != true {
		t.Fatalf("expected dropped=true, got %#v", dropRes.Rows[0]["dropped"])
	}
	if dropRes.Rows[0]["deletedEntities"] != 1 {
		t.Fatalf("expected deletedEntities=1, got %#v", dropRes.Rows[0]["deletedEntities"])
	}
	if catalog.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected edge property index to be removed from catalog")
	}

	remaining := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, "acme", "RATED", "rating", 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				remaining++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index all failed: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected no remaining edge property index entries, got %d", remaining)
	}

	numericShadow := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexNumericRange(ctx, "acme", "RATED", "rating", 5.0, true, true, 5.0, true, true, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				numericShadow++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan numeric property index range failed: %v", err)
	}
	if numericShadow != 0 {
		t.Fatalf("expected no remaining numeric shadow entries, got %d", numericShadow)
	}

	ifExistsStmt, err := parser.ParseStatement("CALL db.index.dropEdgeProperty('RATED', 'rating', true) YIELD dropped, deletedEntities")
	if err != nil {
		t.Fatalf("parse ifExists drop failed: %v", err)
	}
	ifExistsRes, err := exec.ExecuteStatement(ctx, ifExistsStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute ifExists drop failed: %v", err)
	}
	if len(ifExistsRes.Rows) != 1 {
		t.Fatalf("expected one ifExists drop row, got %d (%#v)", len(ifExistsRes.Rows), ifExistsRes.Rows)
	}
	if ifExistsRes.Rows[0]["dropped"] != false {
		t.Fatalf("expected dropped=false with ifExists on missing index, got %#v", ifExistsRes.Rows[0]["dropped"])
	}
	if ifExistsRes.Rows[0]["deletedEntities"] != 0 {
		t.Fatalf("expected deletedEntities=0 with ifExists on missing edge index, got %#v", ifExistsRes.Rows[0]["deletedEntities"])
	}
}

func TestDeleteVertexRemovesPropertyIndexEntries(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	tenant := "acme_delete_vertex_index_entries"
	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant: tenant,
			ID:     "u1",
			Labels: []string{"User"},
			Properties: map[string][]byte{
				"email": valueToBytes("alice@example.com"),
			},
		}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: tenant,
			ID:     "u2",
			Labels: []string{"User"},
			Properties: map[string][]byte{
				"email": valueToBytes("bob@example.com"),
			},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	createStmt, err := parser.ParseStatement("CALL db.index.createProperty('User', 'email') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse create failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, createStmt, Params{"tenant": tenant}); err != nil {
		t.Fatalf("execute create failed: %v", err)
	}

	deleteStmt, err := parser.ParseStatement("MATCH (u:User {email: 'alice@example.com'}) DELETE u")
	if err != nil {
		t.Fatalf("parse delete failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, deleteStmt, Params{"tenant": tenant}); err != nil {
		t.Fatalf("execute delete failed: %v", err)
	}

	u1EntryStillPresent := false
	remainingForDeletedValue := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, tenant, "User", "email", valueToBytes("alice@example.com"), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				remainingForDeletedValue++
				if entry.EntityClass == "vertex" && entry.EntityID == "u1" {
					u1EntryStillPresent = true
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index failed: %v", err)
	}

	if u1EntryStillPresent {
		t.Fatalf("expected index entry for deleted vertex u1 to be removed; remaining entries for deleted value=%d", remainingForDeletedValue)
	}

	bobEntryPresent := false
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndex(ctx, tenant, "User", "email", valueToBytes("bob@example.com"), 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "vertex" && entry.EntityID == "u2" {
				bobEntryPresent = true
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan control property index failed: %v", err)
	}
	if !bobEntryPresent {
		t.Fatalf("expected control index entry for unaffected vertex u2 to remain")
	}
}

func TestDeleteEdgeRemovesPropertyIndexEntries(t *testing.T) {
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
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})

	createStmt, err := parser.ParseStatement("CALL db.index.createEdgeProperty('RATED', 'rating') YIELD created, indexedEntities")
	if err != nil {
		t.Fatalf("parse create failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, createStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute create failed: %v", err)
	}
	if _, err := exec.processPendingEdgeIndexBuildJobs(ctx); err != nil {
		t.Fatalf("process jobs failed: %v", err)
	}

	deleteStmt, err := parser.ParseStatement("MATCH ()-[r:RATED]->() DELETE r")
	if err != nil {
		t.Fatalf("parse delete failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, deleteStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute delete failed: %v", err)
	}

	remaining := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, "acme", "RATED", "rating", 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				remaining++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan property index all failed: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected no remaining edge property index entries after delete, got %d", remaining)
	}

	numericShadow := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexNumericRange(ctx, "acme", "RATED", "rating", 5.0, true, true, 5.0, true, true, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil {
				numericShadow++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan numeric property index range failed: %v", err)
	}
	if numericShadow != 0 {
		t.Fatalf("expected no remaining edge numeric shadow entries after delete, got %d", numericShadow)
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
	physicalNodes, ok := physical["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physical["nodes"])
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
	physicalNodes, ok := physical["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physical["nodes"])
	}
}

func TestExplainRelationshipUsesEdgePropertyIndexAccessPathPipelinePayload(t *testing.T) {
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

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundReturnProject := false
	foundReturnItem := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
				items, _ := attrs["items"].([]string)
				for _, item := range items {
					if strings.Contains(item, "m.id") {
						foundReturnItem = true
						break
					}
				}
			}
		}
	}
	if !foundMatch || !foundReturnProject || !foundReturnItem {
		t.Fatalf("expected MATCH plus PROJECT(RETURN) with m.id projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExplainRelationshipRangeWhereUsesEdgePropertyIndexAccessPathPipelinePayload(t *testing.T) {
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

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundReturnProject := false
	foundReturnItem := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
				items, _ := attrs["items"].([]string)
				for _, item := range items {
					if strings.Contains(item, "m.id") {
						foundReturnItem = true
						break
					}
				}
			}
		}
	}
	if !foundMatch || !foundReturnProject || !foundReturnItem {
		t.Fatalf("expected MATCH plus PROJECT(RETURN) with m.id projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
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

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating >= 4.0 RETURN id(r) AS id ORDER BY id")
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

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating > 4.0 AND r.rating <= 5.0 RETURN id(r) AS id ORDER BY id")
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

	stmt, err := parser.ParseStatement("MATCH (u:User)-[r:RATED]->(m:Movie) WHERE r.rating >= 4.0 AND r.note = 'top' RETURN id(r) AS id ORDER BY id")
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
		t.Fatalf("seed vertices failed: %v", err)
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

func TestRecommendationStage2EdgeIndexPushdownActivates(t *testing.T) {
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
	if len(resIndexed.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	indexedRows := append([]Row(nil), resIndexed.Rows...)
	sortRowsByMovieID(indexedRows)

	indexedJSON, err := json.Marshal(indexedRows)
	if err != nil {
		t.Fatalf("marshal indexed rows failed: %v", err)
	}
	if len(indexedJSON) == 0 {
		t.Fatalf("expected non-empty serialized rows")
	}

	indexedCounters, err := runtimeCountersFromWarnings(resIndexed.Warnings)
	if err != nil {
		t.Fatalf("indexed runtime counters parse failed: %v", err)
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

func TestRecommendationStage2EdgeIndexPushdownActivatesPipelinePayload(t *testing.T) {
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

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + query)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestRecommendationStage1TopKPushdownActivates(t *testing.T) {
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

	enabledCollector := NewCollector()
	enabledExec := New(store, Options{Metrics: enabledCollector})
	enabledRes, err := enabledExec.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("enabled execute failed: %v", err)
	}
	if len(enabledRes.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	enabledRows := append([]Row(nil), enabledRes.Rows...)
	sortRowsByMovieID(enabledRows)

	enabledJSON, err := json.Marshal(enabledRows)
	if err != nil {
		t.Fatalf("marshal enabled rows failed: %v", err)
	}
	if len(enabledJSON) == 0 {
		t.Fatalf("expected non-empty serialized rows")
	}

	enabledCounters, err := runtimeCountersFromWarnings(enabledRes.Warnings)
	if err != nil {
		t.Fatalf("enabled runtime counters parse failed: %v", err)
	}
	if enabledCounters["fast_path.stage1.topk_pushdown_applied"] <= 0 {
		t.Fatalf("expected enabled run to apply stage1 top-k pushdown, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage1.rows_output"] > 30 {
		t.Fatalf("expected stage1 top-k output to respect LIMIT 30, got %#v", enabledCounters)
	}
}

func TestRecommendationStage1TopKPushdownActivatesPipelinePayload(t *testing.T) {
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

	exec := New(store, Options{Metrics: NewCollector()})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestRecommendationStage1TopKPushdownAdaptiveDisablesHighSelectivityWorkload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		for _, id := range []string{"u-peer-1", "u-peer-2"} {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: id, Labels: []string{"User"}}); err != nil {
				return err
			}
		}
		for i := 1; i <= 3; i++ {
			movieID := fmt.Sprintf("m-shared-%d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: movieID, Labels: []string{"Movie"}}); err != nil {
				return err
			}
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-t-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-t-2", Type: "RATED", SrcID: "u-target", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-t-3", Type: "RATED", SrcID: "u-target", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-1", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-2", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-p1-3", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-c", Type: "RATED", SrcID: "u-peer-1", DstID: "m-candidate", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-1", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-2", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-p2-3", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-c", Type: "RATED", SrcID: "u-peer-2", DstID: "m-candidate", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
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

	query := "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 1 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	adaptiveCollector := NewCollector()
	adaptiveExec := New(store, Options{Metrics: adaptiveCollector})
	for i := 0; i < stage1TopKPushdownAdaptiveDisableMinSamples; i++ {
		if _, err := adaptiveExec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("warmup execute failed: %v", err)
		}
	}
	adaptiveRes, err := adaptiveExec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("adaptive execute failed: %v", err)
	}
	if len(adaptiveRes.Rows) == 0 {
		t.Fatalf("expected non-empty adaptive rows")
	}

	adaptiveRows := append([]Row(nil), adaptiveRes.Rows...)
	sortRowsByMovieID(adaptiveRows)
	adaptiveJSON, err := json.Marshal(adaptiveRows)
	if err != nil {
		t.Fatalf("marshal adaptive rows failed: %v", err)
	}
	if len(adaptiveJSON) == 0 {
		t.Fatalf("expected non-empty serialized adaptive rows")
	}

	adaptiveCounters, err := runtimeCountersFromWarnings(adaptiveRes.Warnings)
	if err != nil {
		t.Fatalf("adaptive runtime counters parse failed: %v", err)
	}
	if adaptiveCounters["fast_path.stage1.topk_pushdown_skipped_adaptive"] <= 0 {
		t.Fatalf("expected adaptive top-k skip counter > 0, got %#v", adaptiveCounters)
	}
	if adaptiveCounters["fast_path.stage1.topk_pushdown_applied"] != 0 {
		t.Fatalf("expected adaptive run to skip top-k pushdown, got %#v", adaptiveCounters)
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + query)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := adaptiveExec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	fastPaths, ok := explainPayload["executionStrategies"].([]map[string]any)
	if !ok {
		t.Fatalf("expected executionStrategies []map[string]any, got %T", explainPayload["executionStrategies"])
	}
	foundAdaptive := false
	for _, path := range fastPaths {
		if impl, _ := path["implementation"].(string); impl != stage1TopKPushdownImplementation {
			continue
		}
		if disabled, _ := path["adaptiveTopKDisabled"].(bool); !disabled {
			t.Fatalf("expected adaptiveTopKDisabled=true, got %#v", path)
		}
		foundAdaptive = true
	}
	if !foundAdaptive {
		t.Fatalf("expected stage1 top-k strategy entry in EXPLAIN, got %#v", fastPaths)
	}
	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	execution, ok := runtimeStats["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.execution map, got %T", runtimeStats["execution"])
	}
	if disabledCandidates, _ := execution["topKAdaptiveDisabledCandidates"].(int); disabledCandidates < 1 {
		t.Fatalf("expected topKAdaptiveDisabledCandidates >= 1, got %#v", execution["topKAdaptiveDisabledCandidates"])
	}
}

func TestRecommendationStage1TopKPushdownAdaptiveDisablesHighSelectivityWorkloadPipelinePayload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		for _, id := range []string{"u-peer-1", "u-peer-2"} {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: id, Labels: []string{"User"}}); err != nil {
				return err
			}
		}
		for i := 1; i <= 3; i++ {
			movieID := fmt.Sprintf("m-shared-%d", i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: movieID, Labels: []string{"Movie"}}); err != nil {
				return err
			}
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-t-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-t-2", Type: "RATED", SrcID: "u-target", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-t-3", Type: "RATED", SrcID: "u-target", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-1", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-2", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-p1-3", Type: "RATED", SrcID: "u-peer-1", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p1-c", Type: "RATED", SrcID: "u-peer-1", DstID: "m-candidate", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-1", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-1", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-2", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-2", Properties: map[string][]byte{"rating": valueToBytes(4.0)}},
			{Tenant: "acme", ID: "e-p2-3", Type: "RATED", SrcID: "u-peer-2", DstID: "m-shared-3", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
			{Tenant: "acme", ID: "e-p2-c", Type: "RATED", SrcID: "u-peer-2", DstID: "m-candidate", Properties: map[string][]byte{"rating": valueToBytes(5.0)}},
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

	query := "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 1 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	adaptiveExec := New(store, Options{Metrics: NewCollector()})
	for i := 0; i < stage1TopKPushdownAdaptiveDisableMinSamples; i++ {
		if _, err := adaptiveExec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("warmup execute failed: %v", err)
		}
	}
	res, err := adaptiveExec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("adaptive execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty adaptive rows")
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + query)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := adaptiveExec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
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
	if counters["fast_path.stage2.index_pushdown_skipped_unselective"] <= 0 && counters["fast_path.stage2.index_pushdown_skipped_predicate_shape"] <= 0 && counters["fast_path.stage2.index_pushdown_skipped_probe_cap"] <= 0 && counters["fast_path.stage2.index_pushdown_skipped_wide_non_range"] <= 0 {
		t.Fatalf("expected adaptive skip counter > 0, got %#v", counters)
	}
	if counters["fast_path.stage2.index_pushdown_applied"] != 0 {
		t.Fatalf("expected pushdown not applied for unselective workload, got %#v", counters)
	}
	if counters["fast_path.stage2.edges_visited"] <= 0 {
		t.Fatalf("expected fallback scan path to visit edges, got %#v", counters)
	}
}

func TestRecommendationStage2EdgeIndexPushdownAdaptiveSkipsUnselectiveWorkloadPipelinePayload(t *testing.T) {
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
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
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

	explainStmt, err := parser.ParseStatement("EXPLAIN " + recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestRecommendationStage2EdgeIndexPushdownSelectiveWorkloadBuildsScopedIndexCandidates(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkRangePredicateActivationGraph(ctx, store); err != nil {
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

func TestRecommendationStage2EdgeIndexPushdownSelectiveWorkloadBuildsScopedIndexCandidatesPipelinePayload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkRangePredicateActivationGraph(ctx, store); err != nil {
		t.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkSelectiveQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
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

	explainStmt, err := parser.ParseStatement("EXPLAIN " + recommendationBenchmarkSelectiveQuery)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestRecommendationStage2EdgeIndexPushdownRangePredicateActivatesPushdown(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkRangePredicateActivationGraph(ctx, store); err != nil {
		t.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalogEnabled := indexschema.NewCatalog()
	catalogEnabled.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
	collectorEnabled := NewCollector()
	execEnabled := New(store, Options{Metrics: collectorEnabled, IndexCatalog: catalogEnabled})
	if _, err := execEnabled.BackfillEdgePropertyIndex(ctx, "bench-rec", "RATED", "rating"); err != nil {
		t.Fatalf("enabled backfill failed: %v", err)
	}
	resEnabled, err := execEnabled.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("enabled execute failed: %v", err)
	}
	if len(resEnabled.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	enabledRows := append([]Row(nil), resEnabled.Rows...)
	sortRowsByMovieID(enabledRows)
	enabledJSON, err := json.Marshal(enabledRows)
	if err != nil {
		t.Fatalf("marshal enabled rows failed: %v", err)
	}
	if len(enabledJSON) == 0 {
		t.Fatalf("expected non-empty serialized rows")
	}

	enabledCounters, err := runtimeCountersFromWarnings(resEnabled.Warnings)
	if err != nil {
		t.Fatalf("enabled runtime counters parse failed: %v", err)
	}
	if enabledCounters["fast_path.stage2.index_pushdown_applied"] <= 0 {
		t.Fatalf("expected enabled run to apply stage2 index pushdown, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.index_pushdown_eligible_one_sided_range"] <= 0 {
		t.Fatalf("expected enabled run to qualify one-sided numeric range eligibility, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.index_pushdown_skipped_predicate_shape"] != 0 {
		t.Fatalf("expected range predicate to bypass predicate-shape skip, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.index_pushdown_rows"] <= 0 {
		t.Fatalf("expected enabled run to process indexed rows, got %#v", enabledCounters)
	}
}

func TestRecommendationStage2EdgeIndexPushdownRangePredicateActivatesPushdownPipelinePayload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := seedRecommendationBenchmarkRangePredicateActivationGraph(ctx, store); err != nil {
		t.Fatalf("seed benchmark graph failed: %v", err)
	}

	stmt, err := parser.ParseStatement(recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("bench-rec", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "bench-rec", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "bench-rec"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + recommendationBenchmarkQuery)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "bench-rec"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestRecommendationStage2TopKEarlyStopActivates(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		for i := 1; i <= 3; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-shared-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(100 + i)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: fmt.Sprintf("e-target-%d", i), Type: "RATED", SrcID: "u-target", DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= 10; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-top-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i)}}); err != nil {
				return err
			}
		}
		for i := 1; i <= 5; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-weak-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(1000 + i)}}); err != nil {
				return err
			}
		}

		edgeSeq := 0
		nextEdgeID := func() string {
			edgeSeq++
			return fmt.Sprintf("e-%d", edgeSeq)
		}

		for p := 1; p <= 20; p++ {
			peerID := fmt.Sprintf("u-strong-%d", p)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: peerID, Labels: []string{"User"}}); err != nil {
				return err
			}
			for i := 1; i <= 3; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := 1; i <= 10; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-top-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
		}

		for p := 1; p <= 5; p++ {
			peerID := fmt.Sprintf("u-weak-%d", p)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: peerID, Labels: []string{"User"}}); err != nil {
				return err
			}
			for i := 1; i <= 3; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-weak-%d", p), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	enabledCatalog := indexschema.NewCatalog()
	enabledCatalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	enabledCollector := NewCollector()
	enabledExec := New(store, Options{Metrics: enabledCollector, IndexCatalog: enabledCatalog})
	if _, err := enabledExec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("enabled backfill failed: %v", err)
	}
	enabledRes, err := enabledExec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("enabled execute failed: %v", err)
	}
	if len(enabledRes.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}
	if len(enabledRes.Rows) > 10 {
		t.Fatalf("expected top-k bounded result size <= 10, got %d", len(enabledRes.Rows))
	}

	enabledRows := append([]Row(nil), enabledRes.Rows...)
	sortRowsByMovieID(enabledRows)

	enabledJSON, err := json.Marshal(enabledRows)
	if err != nil {
		t.Fatalf("marshal enabled rows failed: %v", err)
	}
	if len(enabledJSON) == 0 {
		t.Fatalf("expected non-empty serialized rows")
	}

	enabledCounters, err := runtimeCountersFromWarnings(enabledRes.Warnings)
	if err != nil {
		t.Fatalf("enabled runtime counters parse failed: %v", err)
	}
	if enabledCounters["fast_path.stage2.early_stop_checks"] <= 0 {
		t.Fatalf("expected stage2 early-stop checks > 0, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.early_stop_triggers"] <= 0 {
		t.Fatalf("expected stage2 early-stop triggers > 0, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.early_stop_edges_skipped"] <= 0 {
		t.Fatalf("expected stage2 early-stop skipped edges > 0, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.index_pushdown_applied"] <= 0 {
		t.Fatalf("expected stage2 index pushdown applied for early-stop scenario, got %#v", enabledCounters)
	}
	if enabledCounters["fast_path.stage2.edges_visited"] <= 0 {
		t.Fatalf("expected stage2 edges visited > 0, got %#v", enabledCounters)
	}
}

func TestRecommendationStage2TopKEarlyStopActivatesPipelinePayload(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}, Properties: map[string][]byte{"user_id": valueToBytes(1)}}); err != nil {
			return err
		}
		for i := 1; i <= 3; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-shared-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(100 + i)}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: fmt.Sprintf("e-target-%d", i), Type: "RATED", SrcID: "u-target", DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}

		for i := 1; i <= 10; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-top-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(i)}}); err != nil {
				return err
			}
		}
		for i := 1; i <= 5; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: fmt.Sprintf("m-weak-%d", i), Labels: []string{"Movie"}, Properties: map[string][]byte{"movie_id": valueToBytes(1000 + i)}}); err != nil {
				return err
			}
		}

		edgeSeq := 0
		nextEdgeID := func() string {
			edgeSeq++
			return fmt.Sprintf("e-%d", edgeSeq)
		}

		for p := 1; p <= 20; p++ {
			peerID := fmt.Sprintf("u-strong-%d", p)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: peerID, Labels: []string{"User"}}); err != nil {
				return err
			}
			for i := 1; i <= 3; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
			for i := 1; i <= 10; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-top-%d", i), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
					return err
				}
			}
		}

		for p := 1; p <= 5; p++ {
			peerID := fmt.Sprintf("u-weak-%d", p)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: peerID, Labels: []string{"User"}}); err != nil {
				return err
			}
			for i := 1; i <= 3; i++ {
				if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-shared-%d", i), Properties: map[string][]byte{"rating": valueToBytes(3.5)}}); err != nil {
					return err
				}
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: nextEdgeID(), Type: "RATED", SrcID: peerID, DstID: fmt.Sprintf("m-weak-%d", p), Properties: map[string][]byte{"rating": valueToBytes(5.0)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := "MATCH (target:User {user_id: 1})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User) WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5 WITH target, peer, count(shared) AS shared_count, avg(abs(r1.rating - r2.rating)) AS avg_diff WHERE shared_count >= 3 WITH target, peer, shared_count * (1.0 / (1.0 + avg_diff)) AS similarity ORDER BY similarity DESC LIMIT 30 MATCH (peer)-[rp:RATED]->(candidate:Movie) WHERE rp.rating = 5.0 AND NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movie_id, avg(rp.rating) AS peer_avg, count(rp) AS peer_count, sum(similarity) AS total_sim ORDER BY total_sim DESC LIMIT 10"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddEdgePropertyIndex("acme", "RATED", "rating")
	exec := New(store, Options{Metrics: NewCollector(), IndexCatalog: catalog})
	if _, err := exec.BackfillEdgePropertyIndex(ctx, "acme", "RATED", "rating"); err != nil {
		t.Fatalf("backfill edge index failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty recommendation rows")
	}

	explainStmt, err := parser.ParseStatement("EXPLAIN " + query)
	if err != nil {
		t.Fatalf("explain parse failed: %v", err)
	}
	explainRes, err := exec.ExecuteStatement(ctx, explainStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("explain execute failed: %v", err)
	}
	explainPayload, ok := explainRes.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", explainRes.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundWithProject := false
	foundReturnProject := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundReturnProject = true
			}
		}
	}
	if !foundMatch || !foundWithProject || !foundReturnProject {
		t.Fatalf("expected MATCH plus PROJECT(WITH/RETURN) nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "executionStrategies", "runtimeStats", "warnings")
}

func TestTwoHopAntiJoinShortcutAndRuntimeFastPathApplyAndPreserveResults(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "a", Labels: []string{"Person"}, Properties: map[string][]byte{"name": valueToBytes("a")}},
			{Tenant: "acme", ID: "peer", Labels: []string{"Person"}, Properties: map[string][]byte{"name": valueToBytes("peer")}},
			{Tenant: "acme", ID: "s1", Labels: []string{"Person"}, Properties: map[string][]byte{"name": valueToBytes("s1")}},
			{Tenant: "acme", ID: "s2", Labels: []string{"Person"}, Properties: map[string][]byte{"name": valueToBytes("s2")}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "a", DstID: "peer"},
			{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "peer", DstID: "s1"},
			{Tenant: "acme", ID: "e3", Type: "KNOWS", SrcID: "peer", DstID: "s2"},
			{Tenant: "acme", ID: "e4", Type: "KNOWS", SrcID: "a", DstID: "s2"},
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

	exec := New(store, Options{Metrics: NewCollector()})

	query := "MATCH (a:Person)-[:KNOWS]->(peer:Person)-[:KNOWS]->(suggested:Person) WHERE suggested <> a AND NOT (a)-[:KNOWS]-(suggested) WITH DISTINCT a, suggested MERGE (a)-[:SUGGESTED_FRIEND]->(suggested)"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse recommend query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute recommend query failed: %v", err)
	}

	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("runtime counters parse failed: %v", err)
	}
	if counters["runtime.id_first.fastpath_applied"] <= 0 {
		t.Fatalf("expected id-first fast path to apply, got counters %#v", counters)
	}
	if counters["runtime.antijoin.shortcut_applied"] <= 0 {
		t.Fatalf("expected anti-join shortcut to apply, got counters %#v", counters)
	}
	if counters["runtime.antijoin.endpoint_probe_applied"] <= 0 && counters["runtime.antijoin.prefetch_applied"] <= 0 {
		t.Fatalf("expected anti-join endpoint probes or prefetch path to apply, got counters %#v", counters)
	}
	if counters["runtime.merge.batch_probe_applied"] <= 0 {
		t.Fatalf("expected merge batch probe to apply, got counters %#v", counters)
	}

	verifyStmt, err := parser.ParseStatement("MATCH (a:Person)-[:SUGGESTED_FRIEND]->(s:Person) RETURN a.name AS source, s.name AS suggested ORDER BY suggested")
	if err != nil {
		t.Fatalf("parse verify query failed: %v", err)
	}
	verifyRes, err := exec.ExecuteStatement(ctx, verifyStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute verify query failed: %v", err)
	}
	if len(verifyRes.Rows) != 1 {
		t.Fatalf("expected exactly one suggested friend edge, got %d rows: %#v", len(verifyRes.Rows), verifyRes.Rows)
	}
	if got := verifyRes.Rows[0]["source"]; got != "a" {
		t.Fatalf("expected source a, got %#v", got)
	}
	if got := verifyRes.Rows[0]["suggested"]; got != "s1" {
		t.Fatalf("expected suggested s1, got %#v", got)
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

	statusStmt, err := parser.ParseStatement("CALL db.index.edgeBuildJobs() YIELD tenant, edgeType, property, pending, indexedEdges RETURN tenant, edgeType, property, pending, indexedEdges")
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
