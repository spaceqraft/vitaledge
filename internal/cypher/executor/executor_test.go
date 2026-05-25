package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/indexschema"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

func TestExecuteMatchReturnIDs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"srcID":  "u1",
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(res.Columns) != 1 || res.Columns[0] != "dstID" {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0]["dstID"] != "g1" || res.Rows[1]["dstID"] != "g2" {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}
}

func TestExecuteMatchReturnLimitParam(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID LIMIT $max")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"srcID":  "u1",
		"max":    1,
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

func TestExecuteMatchUsesPropertyIndexPlanner(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "u-indexed",
			Labels:     []string{"User"},
			Properties: graph.PropertyMap{"email": []byte("alice@acme.io")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "g-indexed", Labels: []string{"Group"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-indexed", Type: "MEMBER_OF", SrcID: "u-indexed", DstID: "g-indexed"}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "User",
			Property:    "email",
			Value:       []byte("alice@acme.io"),
			EntityID:    "u-indexed",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "User", "email")
	exec := New(store, Options{IndexCatalog: catalog})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "email": "alice@acme.io"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["dstID"]; got != "g-indexed" {
		t.Fatalf("unexpected row: %#v", got)
	}
}

func TestExecuteMatchPropertyLookupWithoutIndexReportsUnsupported(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "u-indexed",
			Labels:     []string{"User"},
			Properties: graph.PropertyMap{"email": []byte("alice@acme.io")},
		}); err != nil {
			return err
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{Metrics: recorder})
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "email": "alice@acme.io"})
	if !graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected unsupported error, got %v", err)
	}
	if len(recorder.indexCandidates) == 0 {
		t.Fatalf("expected index candidate metric")
	}
	candidate := recorder.indexCandidates[0]
	if candidate.schema != "User" || candidate.property != "email" || candidate.indexed {
		t.Fatalf("unexpected index candidate metric: %#v", candidate)
	}
}

func TestExecuteMatchIndexMetricsRecorded(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "u-indexed",
			Labels:     []string{"User"},
			Properties: graph.PropertyMap{"email": []byte("alice@acme.io")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "g-indexed", Labels: []string{"Group"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-indexed", Type: "MEMBER_OF", SrcID: "u-indexed", DstID: "g-indexed"}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "User",
			Property:    "email",
			Value:       []byte("alice@acme.io"),
			EntityID:    "u-indexed",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "User", "email")
	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{IndexCatalog: catalog, Metrics: recorder})
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "email": "alice@acme.io"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(recorder.indexCandidates) == 0 {
		t.Fatalf("expected index candidate metrics")
	}
	if len(recorder.indexLookups) == 0 {
		t.Fatalf("expected index lookup metrics")
	}
	if recorder.indexLookups[0].strategy != "property_index" || recorder.indexLookups[0].outcome != "hit" {
		t.Fatalf("unexpected index lookup metric: %#v", recorder.indexLookups[0])
	}
}

func TestExecuteMatchWhereFiltersRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) WHERE dst.id = 'g2' RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"srcID":  "u1",
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["dstID"]; got != "g2" {
		t.Fatalf("unexpected row: %#v", got)
	}
}

func TestExecuteMatchWhereNotExistsSubquery(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-martin", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-coppola", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Francis Ford Coppola")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-wall", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-apoc", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Apocalypse Now")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-wall"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-apoc"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-wall"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e4", Type: "DIRECTED", SrcID: "p-coppola", DstID: "m-apoc"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (martin:Person)-[:ACTED_IN]->(movie:Movie) WHERE martin.name = 'Martin Sheen' AND NOT EXISTS { MATCH (movie)<-[:DIRECTED]-(director:Person {name: 'Oliver Stone'}) } RETURN movie.title AS movieTitle")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["movieTitle"]; got != "Apocalypse Now" {
		t.Fatalf("unexpected movieTitle: %#v", got)
	}
}

func TestExecuteOptionalMatchPreservesRowWhenNoMatches(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("OPTIONAL MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"srcID":  "u2",
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["dstID"]; got != nil {
		t.Fatalf("expected nil dstID, got %#v", got)
	}
}

func TestExecuteUnsupportedShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN DISTINCT dst.id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
	if graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected DISTINCT shape to be handled, got unsupported error: %v", err)
	}
}

func TestExecuteCreateSetAndPersistVertex(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (u { id: $id }) SET u.name = $name SET u.active = true")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u-create", "name": "Alice"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		v, err := tx.GetVertex(ctx, "acme", "u-create")
		if err != nil {
			return err
		}
		if v.ID != "u-create" {
			return errUnexpected("unexpected vertex id")
		}
		if got := string(v.Properties["name"]); got != "Alice" {
			return errUnexpected("unexpected vertex name")
		}
		if got := string(v.Properties["active"]); got != "true" {
			return errUnexpected("unexpected vertex active flag")
		}
		return nil
	}); err != nil {
		t.Fatalf("store verification failed: %v", err)
	}
}

func TestExecuteMatchSetRemoveAndDelete(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	setStmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) SET dst.active = $active REMOVE dst.active")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setStmt, Params{"tenant": "acme", "srcID": "u1", "active": true}); err != nil {
		t.Fatalf("execute set/remove failed: %v", err)
	}

	deleteStmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) DELETE dst")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, deleteStmt, Params{"tenant": "acme", "srcID": "u1"}); err != nil {
		t.Fatalf("execute delete failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		if _, err := tx.GetVertex(ctx, "acme", "g1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return errUnexpected("expected g1 to be deleted")
		}
		if _, err := tx.GetVertex(ctx, "acme", "g2"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return errUnexpected("expected g2 to be deleted")
		}
		count := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		if count != 0 {
			return errUnexpected("expected adjacency to be deleted with vertex")
		}
		return nil
	}); err != nil {
		t.Fatalf("delete verification failed: %v", err)
	}
}

func TestExecuteMergeIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("MERGE (u { id: $id }) SET u.name = $name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	for i := 0; i < 2; i++ {
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u-merge", "name": "Alice"}); err != nil {
			t.Fatalf("merge execute %d failed: %v", i, err)
		}
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		v, err := tx.GetVertex(ctx, "acme", "u-merge")
		if err != nil {
			return err
		}
		if got := string(v.Properties["name"]); got != "Alice" {
			return errUnexpected("unexpected merge name")
		}
		return nil
	}); err != nil {
		t.Fatalf("merge verification failed: %v", err)
	}
}

func TestExecuteCreateEdgePattern(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (src { id: $srcID })-[:MEMBER_OF]->(dst { id: $dstID })")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u-edge", "dstID": "g-edge"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", syntheticEdgeID("acme", "u-edge", "MEMBER_OF", "g-edge"))
		if err != nil {
			return err
		}
		if edge.SrcID != "u-edge" || edge.DstID != "g-edge" || edge.Type != "MEMBER_OF" {
			return errUnexpected("unexpected created edge")
		}
		return nil
	}); err != nil {
		t.Fatalf("edge verification failed: %v", err)
	}
}

func TestExecuteCreateMultiPatternWithRelationshipProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	query := "CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),\r\n" +
		"       (wallStreet:Movie {title: 'Wall Street'}),\r\n" +
		"       (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet)"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}

	charlieRaw, ok := res.Rows[0]["charlie"]
	if !ok {
		t.Fatalf("expected charlie binding")
	}
	charlie, ok := charlieRaw.(map[string]any)
	if !ok {
		t.Fatalf("expected normalized charlie vertex map, got %T", charlieRaw)
	}
	charlieProps, ok := charlie["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected charlie properties map, got %T", charlie["properties"])
	}
	if got := charlieProps["name"]; got != "Charlie Sheen" {
		t.Fatalf("unexpected charlie name: %q", got)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		targetRaw, ok := res.Rows[0]["wallStreet"]
		if !ok {
			return errUnexpected("expected wallStreet binding")
		}
		target, ok := targetRaw.(map[string]any)
		if !ok {
			return errUnexpected("expected wallStreet vertex binding")
		}
		charlieID, _ := charlie["id"].(string)
		targetID, _ := target["id"].(string)

		edge, err := tx.GetEdge(ctx, "acme", syntheticEdgeID("acme", charlieID, "ACTED_IN", targetID))
		if err != nil {
			return err
		}
		if got := string(edge.Properties["role"]); got != "Bud Fox" {
			return errUnexpected("unexpected relationship role property")
		}
		return nil
	}); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestExecuteUnwindWithReturnProjectsRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND [1,2,3] AS n WITH n RETURN n AS value")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["value"]; got != 1 {
		t.Fatalf("unexpected first row: %#v", got)
	}
	if got := res.Rows[1]["value"]; got != 2 {
		t.Fatalf("unexpected second row: %#v", got)
	}
	if got := res.Rows[2]["value"]; got != 3 {
		t.Fatalf("unexpected third row: %#v", got)
	}
}

func TestExecuteMatchAllNodesReturnBinding(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 rows for seeded vertices, got %d", len(res.Rows))
	}
	for i, row := range res.Rows {
		v, ok := row["n"].(map[string]any)
		if !ok {
			t.Fatalf("row %d expected map-shaped vertex projection, got %T", i, row["n"])
		}
		if _, ok := v["id"]; !ok {
			t.Fatalf("row %d projected vertex missing id field", i)
		}
	}
}

func TestExecuteMatchByLabel(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (n:User) RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 User rows, got %d", len(res.Rows))
	}
}

func TestExecuteMatchReturnBindingEmitsStringProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "u-projected",
			Labels:     []string{"User"},
			Properties: graph.PropertyMap{"name": []byte("Alice")},
		})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}

	node, ok := res.Rows[0]["n"].(map[string]any)
	if !ok {
		t.Fatalf("expected projected node map, got %T", res.Rows[0]["n"])
	}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected projected node properties map, got %T", node["properties"])
	}
	if got, ok := props["name"].(string); !ok || got != "Alice" {
		t.Fatalf("expected string property Alice, got %#v", props["name"])
	}
}

func TestExecuteMatchMovieTitleProjection(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("The American President")}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (movie:Movie) RETURN movie.title AS title")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Columns[0] != "title" {
		t.Fatalf("expected title column, got %#v", res.Columns)
	}
}

func TestExecuteMatchActorByNameWithSpaceNoIndex(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "default",
			ID:     "auto-charlie-1",
			Labels: []string{"Person", "Actor"},
			Properties: graph.PropertyMap{
				"name": []byte("Charlie Sheen"),
			},
		})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (actor:Actor { name: \"Charlie Sheen\" }) RETURN actor")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "default"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}

	actor, ok := res.Rows[0]["actor"].(map[string]any)
	if !ok {
		t.Fatalf("expected actor map, got %T", res.Rows[0]["actor"])
	}
	if got, _ := actor["id"].(string); got != "auto-charlie-1" {
		t.Fatalf("unexpected actor id: %#v", actor["id"])
	}
	props, ok := actor["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected actor properties map, got %T", actor["properties"])
	}
	if got, _ := props["name"].(string); got != "Charlie Sheen" {
		t.Fatalf("unexpected actor name: %#v", props["name"])
	}
}

func TestExecuteMatchLabelAlternationProjection(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Charlie Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"Device"}, Properties: graph.PropertyMap{"name": []byte("router")}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:Movie|Person) RETURN n.name AS name, n.title AS title")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows (Movie|Person), got %d", len(res.Rows))
	}
}

func TestExecuteChainedMatchClauses(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-martin", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-wall", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-apoc", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Apocalypse Now")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-wall"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-apoc"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-wall"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Martin Sheen'})-[:ACTED_IN]->(movie:Movie) MATCH (director:Person)-[:DIRECTED]->(movie) RETURN director.name AS director, movie.title AS movieTitle")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["director"]; got != "Oliver Stone" {
		t.Fatalf("unexpected director: %#v", got)
	}
	if got := res.Rows[0]["movieTitle"]; got != "Wall Street" {
		t.Fatalf("unexpected movieTitle: %#v", got)
	}
}

func TestExecuteMatchNegatedLabelProjection(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"Device"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:!Movie) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows (!Movie), got %d", len(res.Rows))
	}
	ids := map[string]bool{}
	for _, row := range res.Rows {
		id, _ := row["id"].(string)
		ids[id] = true
	}
	if !ids["p1"] || !ids["x1"] || ids["m1"] {
		t.Fatalf("unexpected ids for !Movie: %#v", ids)
	}
}

func TestExecuteMatchLabelsAndCountGrouping(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p2", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"Device"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:!Movie) RETURN labels(n) AS label, count(n) AS labelCount")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 grouped rows, got %d", len(res.Rows))
	}

	counts := map[string]int{}
	for _, row := range res.Rows {
		labels, ok := row["label"].([]string)
		if !ok || len(labels) != 1 {
			t.Fatalf("unexpected labels projection: %#v", row["label"])
		}
		count, ok := row["labelCount"].(int)
		if !ok {
			t.Fatalf("unexpected count projection type: %T", row["labelCount"])
		}
		counts[labels[0]] = count
	}
	if counts["Person"] != 2 || counts["Device"] != 1 || len(counts) != 2 {
		t.Fatalf("unexpected grouped counts: %#v", counts)
	}
}

func TestExecuteMatchUndirectedAdjacentAnonymousLeft(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-other", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Someone Else")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d1", Labels: []string{"Device"}, Properties: graph.PropertyMap{"name": []byte("camera")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"City"}, Properties: graph.PropertyMap{"name": []byte("LA")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m1"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LOCATED_IN", SrcID: "d1", DstID: "p-oliver"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "CONNECTED", SrcID: "p-other", DstID: "x1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Oliver Stone'})--(n) RETURN n AS connectedNodes")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 connected nodes, got %d", len(res.Rows))
	}

	ids := map[string]bool{}
	for _, row := range res.Rows {
		node, ok := row["connectedNodes"].(map[string]any)
		if !ok {
			t.Fatalf("expected connectedNodes map, got %T", row["connectedNodes"])
		}
		id, _ := node["id"].(string)
		ids[id] = true
	}
	if !ids["m1"] || !ids["d1"] || len(ids) != 2 {
		t.Fatalf("unexpected connected node ids: %#v", ids)
	}
}

func TestExecuteMatchUndirectedAdjacentBoundLeft(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d1", Labels: []string{"Device"}, Properties: graph.PropertyMap{"name": []byte("camera")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LOCATED_IN", SrcID: "d1", DstID: "p-oliver"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (person:Person {name: 'Oliver Stone'})--(n) RETURN n AS connectedNodes")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 connected nodes, got %d", len(res.Rows))
	}
}

func TestExecuteMatchDirectedAdjacentAnonymousLeft(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-out", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-in", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Platoon")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d-out", Labels: []string{"Device"}, Properties: graph.PropertyMap{"name": []byte("camera")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-out"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "MENTIONED_IN", SrcID: "m-in", DstID: "p-oliver"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "USES", SrcID: "p-oliver", DstID: "d-out"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Oliver Stone'})-->(movie:Movie) RETURN movie.title AS movieTitle")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 movie row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["movieTitle"]; got != "Wall Street" {
		t.Fatalf("unexpected movie title: %#v", got)
	}
}

func TestExecuteMatchReverseDirectedAdjacentBoundLeft(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-out", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-in", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Platoon")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-out"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "MENTIONED_IN", SrcID: "m-in", DstID: "p-oliver"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (movie:Movie)<--(:Person {name: 'Oliver Stone'}) RETURN movie.title AS movieTitle")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 movie row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["movieTitle"]; got != "Wall Street" {
		t.Fatalf("unexpected movie title: %#v", got)
	}
}

func TestExecuteMatchRelationshipVarAndTypeFunction(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d1", Labels: []string{"Device"}, Properties: graph.PropertyMap{"name": []byte("camera")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "USES", SrcID: "p-oliver", DstID: "d1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Oliver Stone'})-[r]->() RETURN type(r) AS relType")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 relationship rows, got %d", len(res.Rows))
	}

	types := map[string]bool{}
	for _, row := range res.Rows {
		relType, _ := row["relType"].(string)
		types[relType] = true
	}
	if !types["DIRECTED"] || !types["USES"] || len(types) != 2 {
		t.Fatalf("unexpected relationship types: %#v", types)
	}
}

func TestExecuteMatchReverseRelationshipVarAndTypeFunction(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Platoon")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "MENTIONED_IN", SrcID: "m2", DstID: "p-oliver"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()<-[r]-(:Person {name: 'Oliver Stone'}) RETURN type(r) AS relType")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 relationship row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["relType"]; got != "DIRECTED" {
		t.Fatalf("unexpected relationship type: %#v", got)
	}
}

func TestExecuteMatchUndirectedRelationshipWithEdgeProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-charlie", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Charlie Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-wall", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-platoon", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Platoon")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p-charlie", DstID: "m-wall", Properties: graph.PropertyMap{"role": []byte("Bud Fox")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p-charlie", DstID: "m-platoon", Properties: graph.PropertyMap{"role": []byte("Chris Taylor")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "DIRECTED", SrcID: "p-charlie", DstID: "m-wall"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[:ACTED_IN {role: 'Bud Fox'}]-(b) RETURN a, b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 matched rows for undirected binding, got %d", len(res.Rows))
	}

	pairCounts := map[string]int{}
	for _, row := range res.Rows {
		a, ok := row["a"].(map[string]any)
		if !ok {
			t.Fatalf("expected a to be a node map, got %T", row["a"])
		}
		b, ok := row["b"].(map[string]any)
		if !ok {
			t.Fatalf("expected b to be a node map, got %T", row["b"])
		}
		aID, _ := a["id"].(string)
		bID, _ := b["id"].(string)
		pairCounts[aID+"->"+bID]++
	}
	if pairCounts["p-charlie->m-wall"] != 1 || pairCounts["m-wall->p-charlie"] != 1 || len(pairCounts) != 2 {
		t.Fatalf("unexpected undirected bindings: %#v", pairCounts)
	}
}

func TestExecuteMatchReverseRelationshipEdgeTypeAlternation(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-wall", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-charlie", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Charlie Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-marty", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p-charlie", DstID: "m-wall"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-wall"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "PRODUCED", SrcID: "p-marty", DstID: "m-wall"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Movie {title: 'Wall Street'})<-[:ACTED_IN|DIRECTED]-(person:Person) RETURN person.name AS person")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 matched people, got %d", len(res.Rows))
	}

	names := map[string]bool{}
	for _, row := range res.Rows {
		name, _ := row["person"].(string)
		names[name] = true
	}
	if !names["Charlie Sheen"] || !names["Oliver Stone"] || names["Martin Sheen"] || len(names) != 2 {
		t.Fatalf("unexpected people set: %#v", names)
	}
}

func TestExecuteMatchTwoHopDirectedChain(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-charlie", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Charlie Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-oliver", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Oliver Stone")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-marty", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-wall", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Wall Street")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p-charlie", DstID: "m-wall"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "DIRECTED", SrcID: "p-oliver", DstID: "m-wall"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "ACTED_IN", SrcID: "p-marty", DstID: "m-wall"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Charlie Sheen'})-[:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person) RETURN movie.title AS movieTitle, director.name AS director")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["movieTitle"]; got != "Wall Street" {
		t.Fatalf("unexpected movieTitle: %#v", got)
	}
	if got := res.Rows[0]["director"]; got != "Oliver Stone" {
		t.Fatalf("unexpected director: %#v", got)
	}
}

func TestExecuteMatchProjectionNoRowsIsNotError(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("MATCH (movie:Movie) WITH movie RETURN movie.title AS title")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("expected no error for no-match projection, got: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Rows))
	}
}

func TestExecuteWithCountAliasOrderByLimitThenCollect(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Actor One")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p2", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Actor Two")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Movie A")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Movie B")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "ACTED_IN", SrcID: "p1", DstID: "m1"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p1", DstID: "m2"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "ACTED_IN", SrcID: "p2", DstID: "m1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := "MATCH (actors:Person)-[:ACTED_IN]->(movies:Movie) WITH actors, count(movies) AS movieCount ORDER BY movieCount DESC LIMIT 1 MATCH (actors)-[:ACTED_IN]->(movies) RETURN actors.name AS actor, movieCount, collect(movies.title) AS movies"
	stmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["actor"]; got != "Actor One" {
		t.Fatalf("unexpected actor: %#v", got)
	}
	if got := res.Rows[0]["movieCount"]; got != 2 {
		t.Fatalf("unexpected movieCount: %#v", got)
	}
	movies, ok := res.Rows[0]["movies"].([]any)
	if !ok {
		t.Fatalf("expected movies to be []any, got %T", res.Rows[0]["movies"])
	}
	if len(movies) != 2 {
		t.Fatalf("expected 2 movies, got %d", len(movies))
	}
	seen := map[string]bool{}
	for _, item := range movies {
		seen[item.(string)] = true
	}
	if !seen["Movie A"] || !seen["Movie B"] {
		t.Fatalf("unexpected movies: %#v", movies)
	}
}

func TestParseNodePatternBareAndLabel(t *testing.T) {
	if _, err := parseNodePattern("(n)"); err != nil {
		t.Fatalf("expected bare node pattern to parse: %v", err)
	}
	if _, err := parseNodePattern("(n:User)"); err != nil {
		t.Fatalf("expected labeled node pattern to parse: %v", err)
	}
}

func TestExecuteUnwindCreateVertices(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND ['u-unwind-1','u-unwind-2'] AS id CREATE (u { id: id }) WITH id RETURN id AS createdID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		for _, id := range []string{"u-unwind-1", "u-unwind-2"} {
			v, err := tx.GetVertex(ctx, "acme", id)
			if err != nil {
				return err
			}
			if v.ID != id {
				return errUnexpected("unexpected created vertex")
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("vertex verification failed: %v", err)
	}
}

func TestExecuteBareIdentifierAndMapNullChecks(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	bareStmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, bareStmt, Params{"tenant": "acme", "srcID": "u1"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if _, ok := res.Rows[0]["result"]; !ok {
		t.Fatalf("expected bare identifier result column, got %#v", res.Rows[0])
	}

	nullStmt, err := parser.ParseStatement("WITH {name: null} AS map RETURN map.name IS NULL AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, nullStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["result"] != true {
		t.Fatalf("unexpected null-check result: %#v", res.Rows)
	}
}

func errUnexpected(message string) error {
	return &testError{message: message}
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }

func openStore(t *testing.T) graph.GraphStore {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := pebblestore.Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}

func seedGraph(t *testing.T, ctx context.Context, store graph.GraphStore) {
	t.Helper()
	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u2", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "g1", Labels: []string{"Group"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "g2", Labels: []string{"Group"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "MEMBER_OF", SrcID: "u1", DstID: "g1"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "MEMBER_OF", SrcID: "u1", DstID: "g2"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}
}

type indexCandidateMetric struct {
	tenant   string
	schema   string
	property string
	indexed  bool
}

type indexLookupMetric struct {
	strategy string
	outcome  string
	matches  int
}

type executorMetricsRecorder struct {
	indexCandidates []indexCandidateMetric
	indexLookups    []indexLookupMetric
}

func (r *executorMetricsRecorder) ObserveStatement(_ ast.StatementKind, _ string, _ time.Duration) {}

func (r *executorMetricsRecorder) ObserveRowsReturned(_ int) {}

func (r *executorMetricsRecorder) ObserveIndexCandidate(tenant, schema, property string, indexed bool) {
	r.indexCandidates = append(r.indexCandidates, indexCandidateMetric{tenant: tenant, schema: schema, property: property, indexed: indexed})
}

func (r *executorMetricsRecorder) ObserveIndexLookup(strategy, outcome string, matches int) {
	r.indexLookups = append(r.indexLookups, indexLookupMetric{strategy: strategy, outcome: outcome, matches: matches})
}
