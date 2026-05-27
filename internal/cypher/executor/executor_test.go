package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
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

func TestExecuteExplainDryRunWriteQueryDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("EXPLAIN CREATE (n:Person {id: 'dry-run'}) RETURN n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "explain" {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %d", len(res.Rows))
	}
	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	summary, ok := explainPayload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary map, got %T", explainPayload["summary"])
	}
	if writesDetected, _ := summary["writesDetected"].(bool); !writesDetected {
		t.Fatalf("expected writesDetected=true, got %#v", summary["writesDetected"])
	}
	if readOnly, _ := summary["readOnly"].(bool); !readOnly {
		t.Fatalf("expected readOnly=true, got %#v", summary["readOnly"])
	}

	verifyStmt, err := parser.ParseStatement("MATCH (n:Person {id: 'dry-run'}) RETURN n")
	if err != nil {
		t.Fatalf("verification parse failed: %v", err)
	}
	verifyRes, err := exec.ExecuteStatement(ctx, verifyStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("verification execute failed: %v", err)
	}
	if len(verifyRes.Rows) != 0 {
		t.Fatalf("expected dry-run to avoid mutations, got rows: %#v", verifyRes.Rows)
	}
}

func TestExecuteExplainOutputContainsPlanAndParams(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "p-neo",
			Labels:     []string{"Person"},
			Properties: graph.PropertyMap{"name": []byte("Neo")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "p-trinity",
			Labels:     []string{"Person"},
			Properties: graph.PropertyMap{"name": []byte("Trinity")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant: "acme",
			ID:     "m-matrix",
			Labels: []string{"Movie"},
		}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "ACTED_IN", SrcID: "p-neo", DstID: "m-matrix"}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "Person",
			Property:    "name",
			Value:       []byte("Neo"),
			EntityID:    "p-neo",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Person", "name")

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:Person {name: $name}) RETURN DISTINCT n.name AS name ORDER BY name ASC SKIP 1 LIMIT $maxLimit")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{IndexCatalog: catalog})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "Neo", "maxLimit": 5})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	query, ok := explainPayload["query"].(map[string]any)
	if !ok {
		t.Fatalf("expected query map, got %T", explainPayload["query"])
	}
	params, ok := query["params"].([]string)
	if !ok {
		t.Fatalf("expected query.params []string, got %T", query["params"])
	}
	if !reflect.DeepEqual(params, []string{"maxLimit", "name"}) {
		t.Fatalf("unexpected params: %#v", params)
	}
	options, ok := query["options"].(map[string]any)
	if !ok {
		t.Fatalf("expected query.options map, got %T", query["options"])
	}
	if distinct, _ := options["distinct"].(bool); !distinct {
		t.Fatalf("expected distinct query option")
	}
	if skip, _ := options["skip"].(string); skip != "1" {
		t.Fatalf("expected skip=1, got %#v", options["skip"])
	}
	if limit, _ := options["limit"].(string); limit != "$maxLimit" {
		t.Fatalf("expected limit parameter, got %#v", options["limit"])
	}
	if orderBy, _ := options["orderBy"].([]string); !reflect.DeepEqual(orderBy, []string{"name ASC"}) {
		t.Fatalf("unexpected orderBy: %#v", options["orderBy"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	nodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}
	if len(nodes) == 0 {
		t.Fatalf("expected non-empty logical plan nodes")
	}
	if firstOp, _ := nodes[0]["op"].(string); firstOp != "INDEX_SCAN" {
		t.Fatalf("expected first logical node to be INDEX_SCAN, got %#v", nodes[0]["op"])
	}
	if accessPath, _ := nodes[0]["accessPath"].(string); accessPath == "" {
		t.Fatalf("expected first logical node to include accessPath")
	}
	foundProject := false
	foundSort := false
	foundLimit := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "PROJECT":
			foundProject = true
		case "SORT":
			foundSort = true
		case "LIMIT":
			foundLimit = true
		}
	}
	if !foundProject || !foundSort || !foundLimit {
		t.Fatalf("expected operator-shaped plan to include PROJECT/SORT/LIMIT, got nodes %#v", nodes)
	}
	if rootNodeID, _ := logicalPlan["rootNodeId"].(string); rootNodeID == "" {
		t.Fatalf("expected non-empty rootNodeId")
	}
	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	nodeCounts, ok := influencers["nodeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected nodeCounts []map[string]any, got %T", influencers["nodeCounts"])
	}
	if len(nodeCounts) == 0 {
		t.Fatalf("expected non-empty nodeCounts")
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) == 0 {
		t.Fatalf("expected non-empty edgeCounts")
	}
	predicateSignals, ok := influencers["predicateSignals"].([]map[string]any)
	if !ok {
		t.Fatalf("expected predicateSignals []map[string]any, got %T", influencers["predicateSignals"])
	}
	if len(predicateSignals) == 0 {
		t.Fatalf("expected non-empty predicateSignals")
	}
	indexDecisions, ok := explainPayload["indexDecisions"].([]map[string]any)
	if !ok {
		t.Fatalf("expected indexDecisions []map[string]any, got %T", explainPayload["indexDecisions"])
	}
	if len(indexDecisions) == 0 {
		t.Fatalf("expected non-empty indexDecisions")
	}
	if selected, _ := indexDecisions[0]["selected"].(bool); !selected {
		t.Fatalf("expected indexed decision to be selected")
	}
	if recommendation, _ := indexDecisions[0]["recommendation"].(string); recommendation != "keep-index" {
		t.Fatalf("expected recommendation keep-index for selected index, got %#v", indexDecisions[0]["recommendation"])
	}
	if tuningImpact, _ := indexDecisions[0]["tuningImpact"].(string); tuningImpact != "none" {
		t.Fatalf("expected tuningImpact none for selected index, got %#v", indexDecisions[0]["tuningImpact"])
	}
	if accessPath, _ := indexDecisions[0]["accessPath"].(string); accessPath == "" {
		t.Fatalf("expected index decision to include accessPath")
	}
	if quality, _ := indexDecisions[0]["quality"].(string); quality != "exact" {
		t.Fatalf("expected index decision quality exact, got %#v", indexDecisions[0]["quality"])
	}
	if scanPopulation, _ := indexDecisions[0]["scanPopulation"].(int); scanPopulation < 1 {
		t.Fatalf("expected scanPopulation >= 1, got %#v", indexDecisions[0]["scanPopulation"])
	}
	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok {
		t.Fatalf("expected cardinality []map[string]any, got %T", explainPayload["cardinality"])
	}
	if len(cardinality) != len(nodes) {
		t.Fatalf("expected cardinality entries to match nodes, got %d and %d", len(cardinality), len(nodes))
	}
	if rowsOut, _ := cardinality[0]["rowsOut"].(int); rowsOut != 1 {
		t.Fatalf("expected first cardinality rowsOut=1, got %#v", cardinality[0]["rowsOut"])
	}
	costEstimate, ok := explainPayload["costEstimate"].(map[string]any)
	if !ok {
		t.Fatalf("expected costEstimate map, got %T", explainPayload["costEstimate"])
	}
	if unit, _ := costEstimate["unit"].(string); unit != "work_units" {
		t.Fatalf("expected costEstimate unit work_units, got %#v", costEstimate["unit"])
	}
	if quality, _ := costEstimate["quality"].(string); quality != "estimate" {
		t.Fatalf("expected costEstimate quality estimate, got %#v", costEstimate["quality"])
	}
	if value, _ := costEstimate["value"].(int); value < 1 {
		t.Fatalf("expected costEstimate value >= 1, got %#v", costEstimate["value"])
	}
	components, ok := costEstimate["components"].(map[string]any)
	if !ok {
		t.Fatalf("expected costEstimate components map, got %T", costEstimate["components"])
	}
	if _, ok := components["scanRows"].(int); !ok {
		t.Fatalf("expected scanRows component, got %#v", components["scanRows"])
	}
	if _, ok := components["outputRows"].(int); !ok {
		t.Fatalf("expected outputRows component, got %#v", components["outputRows"])
	}
	if _, ok := components["missingIndexPenalty"].(int); !ok {
		t.Fatalf("expected missingIndexPenalty component, got %#v", components["missingIndexPenalty"])
	}
	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	storeStats, ok := runtimeStats["store"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.store map, got %T", runtimeStats["store"])
	}
	if _, ok := storeStats["verticesScanned"].(int); !ok {
		t.Fatalf("expected verticesScanned int, got %#v", storeStats["verticesScanned"])
	}
	if _, ok := storeStats["edgesScanned"].(int); !ok {
		t.Fatalf("expected edgesScanned int, got %#v", storeStats["edgesScanned"])
	}
	planStats, ok := runtimeStats["plan"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.plan map, got %T", runtimeStats["plan"])
	}
	if totalNodes, _ := planStats["totalNodes"].(int); totalNodes < 1 {
		t.Fatalf("expected totalNodes >= 1, got %#v", planStats["totalNodes"])
	}
	indexStats, ok := runtimeStats["index"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.index map, got %T", runtimeStats["index"])
	}
	if _, ok := indexStats["candidates"].(int); !ok {
		t.Fatalf("expected index candidates int, got %#v", indexStats["candidates"])
	}
	cardinalityStats, ok := runtimeStats["cardinality"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.cardinality map, got %T", runtimeStats["cardinality"])
	}
	if quality, _ := cardinalityStats["quality"].(string); quality != "estimate" {
		t.Fatalf("expected runtimeStats.cardinality quality estimate, got %#v", cardinalityStats["quality"])
	}
}

func TestExecuteExplainIndexTuningSignalsForMissingIndex(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "p-neo",
			Labels:     []string{"Person"},
			Properties: graph.PropertyMap{"name": []byte("Neo")},
		}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "p-trinity",
			Labels:     []string{"Person"},
			Properties: graph.PropertyMap{"name": []byte("Trinity")},
		}); err != nil {
			return err
		}
		for i := 0; i < 18; i++ {
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant:     "acme",
				ID:         fmt.Sprintf("p-extra-%d", i),
				Labels:     []string{"Person"},
				Properties: graph.PropertyMap{"name": []byte(fmt.Sprintf("Extra%d", i))},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:Person {name: $name}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "Neo"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	indexDecisions, ok := explainPayload["indexDecisions"].([]map[string]any)
	if !ok || len(indexDecisions) == 0 {
		t.Fatalf("expected non-empty indexDecisions, got %#v", explainPayload["indexDecisions"])
	}
	decision := indexDecisions[0]
	if selected, _ := decision["selected"].(bool); selected {
		t.Fatalf("expected selected=false for missing index")
	}
	if recommendation, _ := decision["recommendation"].(string); recommendation != "create-index" {
		t.Fatalf("expected recommendation create-index for high-impact missing index, got %#v", decision["recommendation"])
	}
	if tuningImpact, _ := decision["tuningImpact"].(string); tuningImpact != "high" {
		t.Fatalf("expected tuningImpact high, got %#v", decision["tuningImpact"])
	}
	if suggestedIndex, _ := decision["suggestedIndex"].(string); suggestedIndex != "Person.name" {
		t.Fatalf("expected suggestedIndex Person.name, got %#v", decision["suggestedIndex"])
	}
	warnings, ok := explainPayload["warnings"].([]map[string]any)
	if !ok || len(warnings) == 0 {
		t.Fatalf("expected warnings to be populated for missing-index fallback, got %#v", explainPayload["warnings"])
	}
	foundMissingIndexWarning := false
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "MISSING_PROPERTY_INDEX" {
			foundMissingIndexWarning = true
			break
		}
	}
	if !foundMissingIndexWarning {
		t.Fatalf("expected MISSING_PROPERTY_INDEX warning, got %#v", warnings)
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

func TestExecuteMatchWhereExistsSubqueryWithOrderedPagination(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-martin", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
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
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-apoc"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (martin:Person) WHERE martin.name = 'Martin Sheen' AND EXISTS { MATCH (martin)-[:ACTED_IN]->(movie:Movie) WITH movie ORDER BY movie.title ASC SKIP 1 LIMIT 1 RETURN movie } RETURN martin.name AS name")
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
	if got := res.Rows[0]["name"]; got != "Martin Sheen" {
		t.Fatalf("unexpected name: %#v", got)
	}
}

func TestExecuteMatchWhereNotExistsSubqueryWithOrderedPagination(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-martin", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Martin Sheen")}}); err != nil {
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
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "ACTED_IN", SrcID: "p-martin", DstID: "m-apoc"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (martin:Person) WHERE martin.name = 'Martin Sheen' AND NOT EXISTS { MATCH (martin)-[:ACTED_IN]->(movie:Movie) WITH movie ORDER BY movie.title ASC SKIP 2 LIMIT 1 RETURN movie } RETURN martin.name AS name")
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
	if got := res.Rows[0]["name"]; got != "Martin Sheen" {
		t.Fatalf("unexpected name: %#v", got)
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

func TestExecuteOptionalMatchKeepsBoundNodeOnMiss(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a1", DstID: "b1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A) OPTIONAL MATCH (a)<-[r:T]-(b) RETURN a, r, b")
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
	if res.Rows[0]["a"] == nil {
		t.Fatalf("expected bound node a to be preserved on OPTIONAL miss, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["r"] != nil {
		t.Fatalf("expected r to be nil on OPTIONAL miss, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["b"] != nil {
		t.Fatalf("expected b to be nil on OPTIONAL miss, row=%#v", res.Rows[0])
	}
}

func TestExecuteCollectSkipsNullValues(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND [1, null, 2] AS x RETURN collect(x) AS xs")
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
	if got := fmt.Sprint(res.Rows[0]["xs"]); got != "[1 2]" {
		t.Fatalf("collect(x) = %v, want [1 2]", res.Rows[0]["xs"])
	}
}

func TestExecuteNestedCollectInsideListComprehension(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN size([x IN collect(r) WHERE x <> null]) AS cn")
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
	if got := fmt.Sprint(res.Rows[0]["cn"]); got != "0" {
		t.Fatalf("cn = %v, want 0", res.Rows[0]["cn"])
	}
}

func TestExecuteListComprehensionProjectionToLower(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}, Properties: map[string][]byte{"name": []byte("c")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c", Labels: []string{"C"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T", SrcID: "a", DstID: "c"}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n)-->(b) WHERE n.name IN [x IN labels(b) | toLower(x)] RETURN b")
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
	binding, ok := res.Rows[0]["b"].(map[string]any)
	if !ok {
		t.Fatalf("expected map binding for b, got %T", res.Rows[0]["b"])
	}
	if got := fmt.Sprint(binding["id"]); got != "c" {
		t.Fatalf("expected b.id=c, got %s", got)
	}
}

func TestExtractAggregateCallsFromNestedListExpressions(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{expr: "size([x IN collect(r) WHERE x <> null])", want: "collect(r)"},
		{expr: "ALL(ok IN collect((size(list) = 0) = empty) WHERE ok)", want: "collect((size(list) = 0) = empty)"},
	}

	for _, tc := range cases {
		calls := extractAggregateCalls(tc.expr)
		if len(calls) == 0 {
			t.Fatalf("extractAggregateCalls(%q) returned no calls", tc.expr)
		}
		found := false
		for _, call := range calls {
			if strings.EqualFold(strings.TrimSpace(call), strings.TrimSpace(tc.want)) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("extractAggregateCalls(%q)=%v, want to contain %q", tc.expr, calls, tc.want)
		}
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

	deleteStmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) DETACH DELETE dst")
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

func TestExecuteSetNodeLabels(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (n {id: 'v'}) SET n:Foo:Bar RETURN labels(n) AS labels")
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

	given, ok := res.Rows[0]["labels"].([]string)
	if !ok {
		t.Fatalf("expected labels list, got %#v", res.Rows[0]["labels"])
	}
	have := map[string]struct{}{}
	for _, label := range given {
		have[label] = struct{}{}
	}
	if _, ok := have["Foo"]; !ok {
		t.Fatalf("expected Foo label in %#v", given)
	}
	if _, ok := have["Bar"]; !ok {
		t.Fatalf("expected Bar label in %#v", given)
	}
}

func TestExecuteSetNodeLabelIgnoresNullBinding(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("OPTIONAL MATCH (a:DoesNotExist) SET a:L RETURN a")
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
	if got := res.Rows[0]["a"]; got != nil {
		t.Fatalf("expected null row value, got %#v", got)
	}
}

func TestExecuteUnionDistinct(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("RETURN 2 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x")
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
	values := []string{fmt.Sprint(res.Rows[0]["x"]), fmt.Sprint(res.Rows[1]["x"])}
	sort.Strings(values)
	if !reflect.DeepEqual(values, []string{"1", "2"}) {
		t.Fatalf("unexpected UNION DISTINCT rows: %#v", res.Rows)
	}
}

func TestExecuteUnionAll(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("RETURN 2 AS x UNION ALL RETURN 1 AS x UNION ALL RETURN 2 AS x")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	counts := map[string]int{}
	for _, row := range res.Rows {
		counts[fmt.Sprint(row["x"])]++
	}
	if counts["1"] != 1 || counts["2"] != 2 {
		t.Fatalf("unexpected UNION ALL rows: %#v", res.Rows)
	}
}

func TestExecuteUnionDifferentColumnsReturnsParseError(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("RETURN 1 AS a UNION RETURN 2 AS b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected DifferentColumnsInUnion error")
	}
	var parseErr *parser.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected parse error, got %T", err)
	}
	if parseErr.Kind != parser.ParseErrorUnsupported || parseErr.Message != "DifferentColumnsInUnion" {
		t.Fatalf("unexpected parse error: %#v", parseErr)
	}
}

func TestExecuteUnionMixedKindsReturnsParseError(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected InvalidClauseComposition error")
	}
	var parseErr *parser.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected parse error, got %T", err)
	}
	if parseErr.Kind != parser.ParseErrorUnsupported || parseErr.Message != "InvalidClauseComposition" {
		t.Fatalf("unexpected parse error: %#v", parseErr)
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

func TestExecuteMergeRelationshipOnCreateSetProperty(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (:A {name:'A'}), (:B {name:'B'})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name='foo'")
	if err != nil {
		t.Fatalf("parse merge failed: %v", err)
	}

	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("merge execute failed: %v", err)
	}
}

func TestExecuteMergeRelationshipOnCreateSetMapForms(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (:A {name:'A'}), (:B {name:'B'})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	replaceStmt, err := parser.ParseStatement("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r=a")
	if err != nil {
		t.Fatalf("parse merge replace failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, replaceStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("merge replace execute failed: %v", err)
	}

	appendStmt, err := parser.ParseStatement("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r2:TYPE2]->(b) ON CREATE SET r2+={name:'bar',name2:'baz'}")
	if err != nil {
		t.Fatalf("parse merge append failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, appendStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("merge append execute failed: %v", err)
	}
}

func TestExecuteMergeMatchAnonymousNodePattern(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (), ()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH () MERGE (a:L) ON MATCH SET a:M1 ON CREATE SET a:M2 RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse merge failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("merge execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one aggregation row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["c"]; got != int64(2) && got != 2 {
		t.Fatalf("expected count 2, got %#v", got)
	}
}

func TestExecuteMergeUndirectedRelationshipNoDuplicateRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (a {id:'a'}), (b {id:'b'}), (c {id:'c'}), (d {id:'d'})")
	if err != nil {
		t.Fatalf("parse seed vertices failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed vertices execute failed: %v", err)
	}

	seedRels, err := parser.ParseStatement("MATCH (a {id:'a'}), (b {id:'b'}), (c {id:'c'}), (d {id:'d'}) CREATE (a)-[:TYPE]->(b), (c)-[:TYPE]->(d)")
	if err != nil {
		t.Fatalf("parse seed relationships failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedRels, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed relationships execute failed: %v", err)
	}

	matchOnly, err := parser.ParseStatement("MATCH (a)--(b) RETURN a, b")
	if err != nil {
		t.Fatalf("parse match-only failed: %v", err)
	}
	matchRes, err := exec.ExecuteStatement(ctx, matchOnly, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("match-only execute failed: %v", err)
	}
	if len(matchRes.Rows) != 4 {
		t.Fatalf("expected 4 rows from MATCH (a)--(b) orientation expansion, got %d", len(matchRes.Rows))
	}

	stmt, err := parser.ParseStatement("MATCH (a)--(b) MERGE (a)-[r:TYPE]-(b) RETURN r")
	if err != nil {
		t.Fatalf("parse merge failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("merge execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	seen := map[string]struct{}{}
	for _, row := range res.Rows {
		raw, ok := row["r"]
		if !ok {
			t.Fatalf("expected r in row: %#v", row)
		}
		switch rel := raw.(type) {
		case *graph.Edge:
			if rel == nil {
				t.Fatalf("expected non-nil relationship edge in row: %#v", row)
			}
			seen[rel.ID] = struct{}{}
		case map[string]any:
			id, ok := rel["id"].(string)
			if !ok || id == "" {
				t.Fatalf("expected relationship map to include non-empty id, got %#v", rel)
			}
			seen[id] = struct{}{}
		default:
			t.Fatalf("expected relationship edge in row, got %#v", raw)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 unique relationships, got %d", len(seen))
	}
}

func TestExecuteMergeOnMatchAndOnCreateLabels(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (), ()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH () MERGE (a:L) ON MATCH SET a:M1 ON CREATE SET a:M2")
	if err != nil {
		t.Fatalf("parse merge failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("merge execute failed: %v", err)
	}

	labelCounts := map[string]int{}
	totalNodes := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(vertex *graph.Vertex) error {
			totalNodes++
			for _, label := range vertex.Labels {
				labelCounts[label]++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if totalNodes != 3 {
		t.Fatalf("expected 3 total nodes after merge, got %d", totalNodes)
	}
	if labelCounts["L"] != 1 || labelCounts["M1"] != 1 || labelCounts["M2"] != 1 {
		t.Fatalf("expected one L/M1/M2 label each, got %#v", labelCounts)
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
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows for CREATE without RETURN, got %d", len(res.Rows))
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		charlieID := ""
		targetID := ""
		err := tx.ScanVertices(ctx, "acme", 0, func(vertex *graph.Vertex) error {
			if got := string(vertex.Properties["name"]); got == "Charlie Sheen" {
				charlieID = vertex.ID
			}
			if got := string(vertex.Properties["title"]); got == "Wall Street" {
				targetID = vertex.ID
			}
			return nil
		})
		if err != nil {
			return err
		}
		if charlieID == "" || targetID == "" {
			return errUnexpected("expected created vertices were not found")
		}

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

func TestExecuteMatchTwoHopForwardDirectedChain(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("a1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Properties: graph.PropertyMap{"name": []byte("b1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Properties: graph.PropertyMap{"name": []byte("c1")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "a1", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "b1", DstID: "c1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A)-[:KNOWS]->(b)-->(c) RETURN c.name AS name")
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
	if got := res.Rows[0]["name"]; got != "c1" {
		t.Fatalf("unexpected name: %#v", got)
	}
}

func TestExecuteMatchTwoHopForwardDirectedChainOptionalMissPreservesC(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("a1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Properties: graph.PropertyMap{"name": []byte("b1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Properties: graph.PropertyMap{"name": []byte("c1")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "a1", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "b1", DstID: "c1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A)-[:KNOWS]->(b)-->(c) OPTIONAL MATCH (a)-[r:KNOWS]->(c) WITH c, r WHERE r IS NULL RETURN c.name AS name")
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
	if got := res.Rows[0]["name"]; got != "c1" {
		t.Fatalf("unexpected name: %#v", got)
	}
}

func TestExecuteReturnZeroLengthNamedPath(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH p = (a) RETURN p")
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
	if got := fmt.Sprint(res.Rows[0]["p"]); got != "<()>" {
		t.Fatalf("unexpected path: %#v", got)
	}
}

func TestExecuteReturnSimpleNamedPath(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("A")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"name": []byte("B")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "a1", DstID: "b1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH p = (a {name: 'A'})-->(b) RETURN p")
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
	if got := fmt.Sprint(res.Rows[0]["p"]); got != "<(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})>" {
		t.Fatalf("unexpected path: %#v", got)
	}
}

func TestExecuteReturnThreeNodeNamedPath(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("A")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"name": []byte("B")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Labels: []string{"C"}, Properties: graph.PropertyMap{"name": []byte("C")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "KNOWS", SrcID: "a1", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "KNOWS", SrcID: "b1", DstID: "c1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c) RETURN p")
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
	if got := fmt.Sprint(res.Rows[0]["p"]); got != "<(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:KNOWS]->(:C {name: 'C'})>" {
		t.Fatalf("unexpected path: %#v", got)
	}
}

func TestExecuteOptionalReturnNamedPathIncludesNullMiss(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Properties: graph.PropertyMap{"name": []byte("A")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Properties: graph.PropertyMap{"name": []byte("B")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Properties: graph.PropertyMap{"name": []byte("C")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "X", SrcID: "a1", DstID: "b1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a {name: 'A'}), (x) WHERE x.name IN ['B', 'C'] OPTIONAL MATCH p = (a)-->(x) RETURN x, p")
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
	seenPath := false
	seenNil := false
	for _, row := range res.Rows {
		switch got := fmt.Sprint(row["p"]); got {
		case "<({name: 'A'})-[:X]->({name: 'B'})>":
			seenPath = true
		case "<nil>", "null":
			seenNil = true
		}
	}
	if !seenPath || !seenNil {
		t.Fatalf("unexpected optional named path rows: %#v", res.Rows)
	}
}

func TestExecuteOptionalTypedRelationshipNamedPathIncludesNullMiss(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Properties: graph.PropertyMap{"name": []byte("A")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Properties: graph.PropertyMap{"name": []byte("B")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Properties: graph.PropertyMap{"name": []byte("C")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "X", SrcID: "a1", DstID: "b1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a {name: 'A'}), (x) WHERE x.name IN ['B', 'C'] OPTIONAL MATCH p = (a)-[:X]->(x) RETURN x, p")
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
	seenPath := false
	seenNil := false
	for _, row := range res.Rows {
		switch got := fmt.Sprint(row["p"]); got {
		case "<({name: 'A'})-[:X]->({name: 'B'})>":
			seenPath = true
		case "<nil>", "null":
			seenNil = true
		}
	}
	if !seenPath || !seenNil {
		t.Fatalf("unexpected optional typed named path rows: %#v", res.Rows)
	}
}

func TestExecuteCountAfterMatchMergeOptionalMatch(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T1", SrcID: "a1", DstID: "b1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T2", SrcID: "b1", DstID: "a1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*) AS c")
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
	if got := fmt.Sprint(res.Rows[0]["c"]); got != "6" {
		t.Fatalf("expected count 6, got %s (rows=%#v)", got, res.Rows)
	}
}

func TestExecuteCountDirectedAndUndirectedSelfLoop(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "loop", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "y"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-loop", Type: "LOOP", SrcID: "loop", DstID: "loop"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-other", Type: "T", SrcID: "x", DstID: "y"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})

	directed, err := parser.ParseStatement("MATCH (n)-[r]->(n) RETURN count(r) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	directedRes, err := exec.ExecuteStatement(ctx, directed, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(directedRes.Rows[0]["c"]); got != "1" {
		t.Fatalf("expected directed self-loop count 1, got %s", got)
	}

	undirected, err := parser.ParseStatement("MATCH (n)-[r]-(n) RETURN count(r) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	undirectedRes, err := exec.ExecuteStatement(ctx, undirected, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(undirectedRes.Rows[0]["c"]); got != "1" {
		t.Fatalf("expected undirected self-loop count 1, got %s", got)
	}

	adjDirected, err := parser.ParseStatement("MATCH (n)-->(n) RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	adjDirectedRes, err := exec.ExecuteStatement(ctx, adjDirected, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(adjDirectedRes.Rows[0]["c"]); got != "1" {
		t.Fatalf("expected directed adjacent self-loop count 1, got %s", got)
	}

	adjUndirected, err := parser.ParseStatement("MATCH (n)--(n) RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	adjUndirectedRes, err := exec.ExecuteStatement(ctx, adjUndirected, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(adjUndirectedRes.Rows[0]["c"]); got != "1" {
		t.Fatalf("expected undirected adjacent self-loop count 1, got %s", got)
	}
}

func TestExecuteCountTwoHopUndirectedRelationshipChain(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "l", Labels: []string{"Looper"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T1", SrcID: "a", DstID: "l"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "eloop", Type: "LOOP", SrcID: "l", DstID: "l"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T2", SrcID: "l", DstID: "b"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()-[]-()-[]-() RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(res.Rows[0]["c"]); got != "6" {
		t.Fatalf("expected count 6, got %s (rows=%#v)", got, res.Rows)
	}
}

func TestParseMixedRelationshipChainPatternBounds(t *testing.T) {
	pattern, err := parseMixedRelationshipChainPattern("(a)-[:LIKES*2]->()-[:LIKES]->(c)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(pattern.Segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(pattern.Segments))
	}
	if !pattern.Segments[0].IsVariableLength {
		t.Fatalf("expected first segment to be variable length")
	}
	if pattern.Segments[0].Direction != "forward" {
		t.Fatalf("expected first segment direction forward, got %q", pattern.Segments[0].Direction)
	}
	if pattern.Segments[0].MinHops != 2 || pattern.Segments[0].MaxHops != 2 {
		t.Fatalf("expected first segment bounds 2..2, got %d..%d", pattern.Segments[0].MinHops, pattern.Segments[0].MaxHops)
	}
	if pattern.Segments[1].IsVariableLength {
		t.Fatalf("expected second segment to be a standard relationship")
	}
	if pattern.Segments[1].Direction != "forward" {
		t.Fatalf("expected second segment direction forward, got %q", pattern.Segments[1].Direction)
	}

	pattern2, err := parseMixedRelationshipChainPattern("(a)-[:LIKES]->()-[:LIKES*2]->(c)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(pattern2.Segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(pattern2.Segments))
	}
	if !pattern2.Segments[1].IsVariableLength {
		t.Fatalf("expected second segment to be variable length")
	}
	if pattern2.Segments[1].MinHops != 2 || pattern2.Segments[1].MaxHops != 2 {
		t.Fatalf("expected second segment bounds 2..2, got %d..%d", pattern2.Segments[1].MinHops, pattern2.Segments[1].MaxHops)
	}
}

func TestExecuteMixedChainVariableThenStandardHonorsExactBounds(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("n0")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Properties: graph.PropertyMap{"name": []byte("n00")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c", Properties: graph.PropertyMap{"name": []byte("n000")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d", Properties: graph.PropertyMap{"name": []byte("n0000")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "LIKES", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "b", DstID: "c"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "c", DstID: "d"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A)\nMATCH (a)-[:LIKES*2]->()-[:LIKES]->(c)\nRETURN c.name AS name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	_, mixedErr := parseMixedRelationshipChainPattern("(a)-[:LIKES*2]->()-[:LIKES]->(c)")
	if mixedErr != nil {
		t.Fatalf("mixed parse failed: %v", mixedErr)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["name"]); got != "n0000" {
		t.Fatalf("expected n0000, got %q", got)
	}
}

func TestApplyMatchClauseMixedChainVariableThenStandard(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "LIKES", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "b", DstID: "c"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "c", DstID: "d"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	err = store.View(ctx, func(tx graph.Tx) error {
		a, err := tx.GetVertex(ctx, "acme", "a")
		if err != nil {
			return err
		}
		rows, err := exec.applyMatchClause(ctx, tx, []Row{{"a": a}}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH (a)-[:LIKES*2]->()-[:LIKES]->(c)"}, Params{"tenant": "acme"})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("expected 1 row, got %d: %#v", len(rows), rows)
		}
		v, ok := rows[0]["c"].(*graph.Vertex)
		if !ok || v == nil || v.ID != "d" {
			return fmt.Errorf("expected c=d, got %#v", rows[0]["c"])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("applyMatchClause failed: %v", err)
	}
}

// ─── Multi-hop adjacent chain regressions ────────────────────────────────────

// TestExecuteReturnNamedPathMixedDirection verifies MATCH p=(a)-->(b)<--(c) RETURN p.
// Graph: a-->b, c-->b  (both incoming to b)
// Expected: one path <(a)-->(b)<--(c)>
func TestExecuteReturnNamedPathMixedDirection(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup := []string{
		"CREATE (:A {name:'a'})-[:R]->(:B {name:'b'})",
		"MATCH (b:B {name:'b'}) CREATE (:C {name:'c'})-[:R]->(b)",
	}
	for _, q := range setup {
		stmt, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	stmt, err := parser.ParseStatement("MATCH p=(a:A)-->(b:B)<--(c:C) RETURN p")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	p := fmt.Sprint(res.Rows[0]["p"])
	if !strings.Contains(p, "->") || !strings.Contains(p, "<-") {
		t.Fatalf("path string does not contain both directions: %q", p)
	}
}

// TestExecuteReturnNamedPathFwdUndirected verifies MATCH p=(a)-->(b)--(c) RETURN p.
func TestExecuteReturnNamedPathFwdUndirected(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup := []string{
		"CREATE (:A {name:'a'})-[:R]->(:B {name:'b'})-[:R]->(:C {name:'c'})",
	}
	for _, q := range setup {
		stmt, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	stmt, err := parser.ParseStatement("MATCH p=(a:A)-->(b:B)--(c:C) RETURN p")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected at least 1 row, got 0")
	}
	p := fmt.Sprint(res.Rows[0]["p"])
	if !strings.Contains(p, ":A") || !strings.Contains(p, ":C") {
		t.Fatalf("path string missing expected nodes: %q", p)
	}
}

func TestExecuteDirectedThenUndirectedRelationshipChainBindsEdgeVars(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup := []string{
		"CREATE (:A {name:'x'})-[:REL1]->(:B {name:'y'})-[:REL2]->(:C {name:'z'})",
	}
	for _, q := range setup {
		stmt, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	stmt, err := parser.ParseStatement("MATCH (x:A)-[r1]->(y:B)-[r2]-(z:C) RETURN type(r1) AS t1, type(r2) AS t2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["t1"]); got != "REL1" {
		t.Fatalf("expected t1=REL1, got %q", got)
	}
	if got := fmt.Sprint(res.Rows[0]["t2"]); got != "REL2" {
		t.Fatalf("expected t2=REL2, got %q", got)
	}
}

func TestExecuteNamedPathReverseFixedThenUndirectedBoundedVariableLength(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	setup := []string{
		"CREATE (:Mid {id: 0})-[:CONNECTED_TO]->(:Start {id: 1})",
		"MATCH (m:Mid {id: 0}) CREATE (m)-[:CONNECTED_TO]->(:N1 {id: 2})-[:CONNECTED_TO]->(:N2 {id: 3})-[:CONNECTED_TO]->(:End {id: 4})",
	}
	for _, q := range setup {
		stmt, err := parser.ParseStatement(q)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}

	stmt, err := parser.ParseStatement("MATCH p = (:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(:End) RETURN length(p) AS l, size(relationships(p)) AS nr")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["l"]; got != 4 {
		t.Fatalf("expected length 4, got %#v", got)
	}
	if got := res.Rows[0]["nr"]; got != 4 {
		t.Fatalf("expected relationships size 4, got %#v", got)
	}
}

// TestExecuteReturnNamedPathConvergentEmptyWhenNoMatch verifies that
// MATCH p=(n)-->(k)<--(n) returns no rows when the convergent path doesn't exist.
func TestExecuteReturnNamedPathConvergentEmptyWhenNoMatch(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	// a-->b only; no second incoming edge to b, so no convergent path where start==end
	// (Cypher path-uniqueness prevents reusing the a->b edge for both hops)
	stmt, err := parser.ParseStatement("CREATE (:A {name:'a'})-[:R]->(:B {name:'b'})")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH p=(n)-->(k)<--(n) RETURN p")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows for non-existent convergent path, got %d", len(res.Rows))
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

func TestExecuteWithRelationshipAliasAndAggregationPreservesEdgeBinding(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE ()-[:T1]->(:X), ()-[:T2]->(:X), ()-[:T3]->()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()-[r1]->(:X) WITH r1 AS r2, count(*) AS c MATCH ()-[r2]->() RETURN type(r2) AS relType ORDER BY relType")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["relType"]; got != "T1" {
		t.Fatalf("unexpected first relType: %#v", got)
	}
	if got := res.Rows[1]["relType"]; got != "T2" {
		t.Fatalf("unexpected second relType: %#v", got)
	}
}

func TestExecuteWithForwardingNullPreservesStarColumns(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("OPTIONAL MATCH (a:Start) WITH a MATCH (a)-->(b) RETURN *")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Rows))
	}
	if !reflect.DeepEqual(res.Columns, []string{"a", "b"}) {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
}

func TestExecuteWithDistinctOrderByLimitOnProjectedVariable(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	for _, tc := range []struct {
		name  string
		query string
		want  any
	}{
		{
			name:  "asc",
			query: "UNWIND [0, 2, 1, 2, 0, 1] AS x WITH DISTINCT x ORDER BY x ASC LIMIT 1 RETURN x",
			want:  0,
		},
		{
			name:  "desc",
			query: "UNWIND [0, 2, 1, 2, 0, 1] AS x WITH DISTINCT x ORDER BY x DESC LIMIT 1 RETURN x",
			want:  2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseStatement(tc.query)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(res.Rows))
			}
			if got := res.Rows[0]["x"]; got != tc.want {
				t.Fatalf("unexpected x: %#v", got)
			}
		})
	}
}

func TestExecuteSumPreservesLargeIntegerResult(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND range(1000000, 2000000) AS i WITH i LIMIT 3000 RETURN sum(i) AS total")
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

	got := res.Rows[0]["total"]
	switch typed := got.(type) {
	case int:
		if typed != 3004498500 {
			t.Fatalf("unexpected integer sum: %v", typed)
		}
	case int64:
		if typed != 3004498500 {
			t.Fatalf("unexpected integer sum: %v", typed)
		}
	case json.Number:
		if typed.String() != "3004498500" {
			t.Fatalf("unexpected numeric sum: %v", typed)
		}
	default:
		t.Fatalf("unexpected sum type/value: %T %#v", got, got)
	}
}

func TestExecuteMatchCommaSeparatedPatternsWithForwardedBinding(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (:A)-[:REL]->(:B) CREATE (:X)")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A) WITH a MATCH (x:X), (a)-->(b) RETURN *")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if !reflect.DeepEqual(res.Columns, []string{"a", "b", "x"}) {
		t.Fatalf("unexpected columns: %#v", res.Columns)
	}
}

func TestExecuteReverseAdjacentThenReverseRelationshipChain(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seed, err := parser.ParseStatement("CREATE (a:Person), (b:Person), (m:Message {id: 10}) CREATE (a)-[:LIKE {creationDate: 20160614}]->(m)-[:POSTED_BY]->(b)")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (person:Person)<--(message)<-[like]-(:Person) RETURN like.creationDate AS likeTime ORDER BY message.id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["likeTime"]; got != 20160614 {
		t.Fatalf("unexpected likeTime: %#v", got)
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

func TestExecuteQuantifierAndListPredicateScoping(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("WITH 2 AS r RETURN any(x IN [1, 2, 3] WHERE x < r) AS anyResult, [x IN [1, 2, 3] WHERE x < r | x] AS filtered")
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
	if got := res.Rows[0]["anyResult"]; got != true {
		t.Fatalf("unexpected anyResult: %#v", got)
	}
	filtered, ok := res.Rows[0]["filtered"].([]any)
	if !ok {
		t.Fatalf("unexpected filtered type: %#v", res.Rows[0]["filtered"])
	}
	if len(filtered) != 1 || filtered[0] != 1 {
		t.Fatalf("unexpected filtered result: %#v", filtered)
	}
}

func TestExecuteWithWherePreservesOuterScope(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("WITH 1 AS r, 2 AS c WITH c WHERE r = 1 RETURN c AS result")
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
	if got := res.Rows[0]["result"]; got != 2 {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestExecuteRandExpressionAndQuantifierSetup(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})

	randStmt, err := parser.ParseStatement("RETURN rand() > -1 AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, randStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["result"] != true {
		t.Fatalf("unexpected rand result: %#v", res.Rows)
	}

	listStmt, err := parser.ParseStatement("WITH [1, 2, 3] AS inputList RETURN [y IN inputList WHERE rand() > 0.5 | y] AS list")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, listStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if _, ok := res.Rows[0]["list"].([]any); !ok {
		t.Fatalf("unexpected list result: %#v", res.Rows[0]["list"])
	}

	quantStmt, err := parser.ParseStatement("WITH [1, 2, 3] AS inputList UNWIND inputList AS x WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list WITH any(x IN list WHERE x >= 1) AS result RETURN result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, quantStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected at least 1 row, got %d", len(res.Rows))
	}
	for _, row := range res.Rows {
		if _, ok := row["result"].(bool); !ok {
			t.Fatalf("unexpected quantifier result: %#v", row["result"])
		}
	}

	caseStmt, err := parser.ParseStatement("WITH [1, 2, 3] AS list, 9 AS x RETURN CASE WHEN rand() < 2 THEN reverse(list) ELSE list END + x AS out")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, caseStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	out, ok := res.Rows[0]["out"].([]any)
	if !ok {
		t.Fatalf("unexpected CASE+reverse output type: %#v", res.Rows[0]["out"])
	}
	if len(out) != 4 || out[3] != 9 {
		t.Fatalf("unexpected CASE+reverse output: %#v", out)
	}

	coalesceStmt, err := parser.ParseStatement("WITH null AS fixedList, [1, 2, 3] AS list RETURN coalesce(fixedList,list) AS chosen")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, coalesceStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	chosen, ok := res.Rows[0]["chosen"].([]any)
	if !ok {
		t.Fatalf("unexpected coalesce output type: %#v", res.Rows[0]["chosen"])
	}
	if len(chosen) != 3 || chosen[0] != 1 || chosen[1] != 2 || chosen[2] != 3 {
		t.Fatalf("unexpected coalesce output: %#v", chosen)
	}

	legacyValue, err := evalExpression("coalesce(a.title, a.name)", map[string]any{"a": map[string]any{"name": "u1"}})
	if err != nil {
		t.Fatalf("legacy evalExpression coalesce failed: %v", err)
	}
	if legacyValue != "u1" {
		t.Fatalf("unexpected legacy coalesce fallback: %#v", legacyValue)
	}

	legacyBoolean, err := evalExpression("aOR(bANDc)", map[string]any{"a": true, "b": false, "c": false})
	if err != nil {
		t.Fatalf("legacy evalExpression boolean failed: %v", err)
	}
	if legacyBoolean != true {
		t.Fatalf("unexpected legacy boolean result: %#v", legacyBoolean)
	}
}

func TestEvalExpressionWithScopeBooleanSemantics(t *testing.T) {
	row := Row{
		"n0":  0,
		"s":   "xx",
		"a":   true,
		"b":   false,
		"c":   false,
		"nil": nil,
		"n":   nil,
		"l1":  []any{1, nil},
		"l2":  []any{1, 2},
		"m1":  map[string]any{"k": nil},
		"m2":  map[string]any{"k": 1},
	}

	cases := []struct {
		expr string
		want any
	}{
		{expr: "nil = nil", want: nil},
		{expr: "nil <> nil", want: nil},
		{expr: "nil > n0", want: nil},
		{expr: "s > n0", want: nil},
		{expr: "l1 = l2", want: nil},
		{expr: "m1 = m2", want: nil},
		{expr: "aOR(bANDc)", want: true},
		{expr: "aXORb", want: true},
		{expr: "aISNULLORnilISNULLORb", want: true},
		{expr: "n.missing IS NULL", want: true},
		{expr: "a OR b = c", want: true},
		{expr: "a OR (b = c)", want: true},
		{expr: "(a OR b) = c", want: false},
		{expr: "single(x IN [1, 2] WHERE x = 1)", want: true},
		{expr: "single(x IN [1, 2] WHERE x > 0)", want: false},
		{expr: "single(x IN [null, 1] WHERE x = 1)", want: nil},
		{expr: "single(x IN [null] WHERE x = 1)", want: nil},
	}

	for _, tc := range cases {
		got, err := evalExpressionWithScope(tc.expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.expr, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %#v", tc.expr, got, tc.want)
		}
	}
}

func TestEvalExpressionWithScopeRangeArgumentErrors(t *testing.T) {
	row := Row{}
	cases := []struct {
		expr    string
		message string
	}{
		{expr: "range(true, 3)", message: "range() start must be an integer"},
		{expr: "range(1, false)", message: "range() end must be an integer"},
		{expr: "range(1, 3, 0)", message: "range() step cannot be zero"},
	}

	for _, tc := range cases {
		_, err := evalExpressionWithScope(tc.expr, row, nil)
		if err == nil {
			t.Fatalf("evalExpressionWithScope(%q) expected error", tc.expr)
		}
		if !graph.IsKind(err, graph.ErrKindInvalidInput) {
			t.Fatalf("evalExpressionWithScope(%q) error kind = %v, want invalid input", tc.expr, err)
		}
		if !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("evalExpressionWithScope(%q) error = %v, want message containing %q", tc.expr, err, tc.message)
		}
	}
}

func TestEvalExpressionWithScopeSubscriptTypeErrors(t *testing.T) {
	row := Row{
		"list": []any{1, 2, 3},
		"mapv": map[string]any{"name": "Apa"},
	}

	cases := []struct {
		expr    string
		message string
	}{
		{expr: "true[0]", message: "InvalidArgumentType"},
		{expr: "'1'[0]", message: "InvalidArgumentType"},
		{expr: "list[true]", message: "InvalidArgumentType"},
		{expr: "mapv[0]", message: "MapElementAccessByNonString"},
	}

	for _, tc := range cases {
		_, err := evalExpressionWithScope(tc.expr, row, nil)
		if err == nil {
			t.Fatalf("evalExpressionWithScope(%q) expected error", tc.expr)
		}
		if !graph.IsKind(err, graph.ErrKindInvalidInput) {
			t.Fatalf("evalExpressionWithScope(%q) error kind = %v, want invalid input", tc.expr, err)
		}
		if !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("evalExpressionWithScope(%q) error = %v, want message containing %q", tc.expr, err, tc.message)
		}
	}
}

func TestEvalExpressionWithScopeMapSubscript(t *testing.T) {
	row := Row{"expr": map[string]any{"name": "Apa"}}
	got, err := evalExpressionWithScope("expr['name']", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(map subscript) failed: %v", err)
	}
	if got != "Apa" {
		t.Fatalf("evalExpressionWithScope(map subscript) = %#v, want %q", got, "Apa")
	}
}

func TestEvalExpressionWithScopeToIntegerAndProperties(t *testing.T) {
	row := Row{
		"m": map[string]any{"name": "Popeye", "level": 9001},
		"v": &graph.Vertex{Properties: graph.PropertyMap{"name": []byte("Popeye")}},
	}

	idx, err := evalExpressionWithScope("toInteger('2')", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(toInteger) failed: %v", err)
	}
	if idx != 2 {
		t.Fatalf("evalExpressionWithScope(toInteger) = %#v, want 2", idx)
	}

	idx, err = evalExpressionWithScope("toInteger(82.9)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(toInteger(float)) failed: %v", err)
	}
	if idx != 82 {
		t.Fatalf("evalExpressionWithScope(toInteger(float)) = %#v, want 82", idx)
	}

	idx, err = evalExpressionWithScope("toInteger('2.9')", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(toInteger(decimal string)) failed: %v", err)
	}
	if idx != 2 {
		t.Fatalf("evalExpressionWithScope(toInteger(decimal string)) = %#v, want 2", idx)
	}

	nullValue, err := evalExpressionWithScope("toInteger('foo')", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(toInteger(non-numerical string)) failed: %v", err)
	}
	if nullValue != nil {
		t.Fatalf("evalExpressionWithScope(toInteger(non-numerical string)) = %#v, want nil", nullValue)
	}

	mapProps, err := evalExpressionWithScope("properties(m)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(properties(map)) failed: %v", err)
	}
	mapValue, ok := mapProps.(map[string]any)
	if !ok || mapValue["name"] != "Popeye" || mapValue["level"] != 9001 {
		t.Fatalf("unexpected properties(map) result: %#v", mapProps)
	}

	vertexProps, err := evalExpressionWithScope("properties(v)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(properties(vertex)) failed: %v", err)
	}
	vertexMap, ok := vertexProps.(map[string]any)
	if !ok || vertexMap["name"] != "Popeye" {
		t.Fatalf("unexpected properties(vertex) result: %#v", vertexProps)
	}

	_, err = evalExpressionWithScope("properties(1)", row, nil)
	if err == nil {
		t.Fatalf("evalExpressionWithScope(properties(1)) expected error")
	}
	if !graph.IsKind(err, graph.ErrKindSemantic) || !strings.Contains(strings.ToLower(err.Error()), "invalid argument type") {
		t.Fatalf("unexpected properties(1) error: %v", err)
	}
}

func TestEvalExpressionWithScopeConversionRejectsStructuralValues(t *testing.T) {
	row := Row{
		"list": []any{1, 2, 3},
		"mapv": map[string]any{"name": "Apa"},
		"n":    &graph.Vertex{ID: "n1"},
		"r":    &graph.Edge{ID: "r1", SrcID: "n1", DstID: "n2"},
		"p":    cypherPathValue{Left: &graph.Vertex{ID: "n1"}, Edge: &graph.Edge{ID: "r1", SrcID: "n1", DstID: "n2"}, Right: &graph.Vertex{ID: "n2"}},
	}

	cases := []struct {
		expr string
	}{
		{expr: "toInteger(list)"},
		{expr: "toInteger(mapv)"},
		{expr: "toInteger(n)"},
		{expr: "toInteger(r)"},
		{expr: "toInteger(p)"},
		{expr: "toString(list)"},
		{expr: "toString(mapv)"},
		{expr: "toString(n)"},
		{expr: "toString(r)"},
		{expr: "toString(p)"},
	}

	for _, tc := range cases {
		_, err := evalExpressionWithScope(tc.expr, row, nil)
		if err == nil {
			t.Fatalf("evalExpressionWithScope(%q) expected error", tc.expr)
		}
		if !graph.IsKind(err, graph.ErrKindInvalidInput) {
			t.Fatalf("evalExpressionWithScope(%q) error kind = %v, want invalid input", tc.expr, err)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "invalidargumentvalue") {
			t.Fatalf("evalExpressionWithScope(%q) error = %v, want InvalidArgumentValue", tc.expr, err)
		}
	}
}

func TestPrecedenceComparisonVsBooleanProbe(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("UNWIND [true, false, null] AS a UNWIND [true, false, null] AS b UNWIND [true, false, null] AS c WITH collect((a OR b = c) = (a OR (b = c))) AS eq, collect((a OR b = c) <> ((a OR b) = c)) AS neq RETURN size([x IN eq WHERE x IS NULL]) AS eqNull, size([x IN eq WHERE x = false]) AS eqFalse, size([x IN eq WHERE x = true]) AS eqTrue, all(x IN eq WHERE x) AS allEq, any(x IN neq WHERE x) AS anyNeq, all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["result"]; got != true {
		t.Fatalf("unexpected precedence probe row: %#v", res.Rows[0])
	}
}

func TestEvalExpressionWithScopeExponentPrecedenceAndNullPropagation(t *testing.T) {
	row := Row{}

	value, err := evalExpressionWithScope("4 ^ 3 * 2 ^ 3", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 ^ 3 * 2 ^ 3) failed: %v", err)
	}
	if fmt.Sprint(value) != "512.0" {
		t.Fatalf("unexpected exponent multiplicative precedence result: %#v", value)
	}

	value, err = evalExpressionWithScope("4 ^ 3 + 2 ^ 3", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 ^ 3 + 2 ^ 3) failed: %v", err)
	}
	if fmt.Sprint(value) != "72.0" {
		t.Fatalf("unexpected exponent additive precedence result: %#v", value)
	}

	value, err = evalExpressionWithScope("-3 ^ 2", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(-3 ^ 2) failed: %v", err)
	}
	if fmt.Sprint(value) != "9.0" {
		t.Fatalf("unexpected unary-negative exponent result: %#v", value)
	}

	value, err = evalExpressionWithScope("4 ^ (3 + 2) ^ 3", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 ^ (3 + 2) ^ 3) failed: %v", err)
	}
	if fmt.Sprint(value) != "1073741824.0" {
		t.Fatalf("unexpected left-associative exponent result: %#v", value)
	}

	value, err = evalExpressionWithScope("4 ^ (3 / 2) ^ 3", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 ^ (3 / 2) ^ 3) failed: %v", err)
	}
	if fmt.Sprint(value) != "64.0" {
		t.Fatalf("unexpected integer-division exponent precedence result: %#v", value)
	}

	value, err = evalExpressionWithScope("-(3 ^ 2)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(-(3 ^ 2)) failed: %v", err)
	}
	if fmt.Sprint(value) != "-9.0" {
		t.Fatalf("unexpected grouped unary-negative exponent result: %#v", value)
	}

	value, err = evalExpressionWithScope("-(3 + 2)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(-(3 + 2)) failed: %v", err)
	}
	if value != -5 {
		t.Fatalf("unexpected grouped unary-negative additive result: %#v", value)
	}

	value, err = evalExpressionWithScope("5 ^ (6 % null)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(5 ^ (6 %% null)) failed: %v", err)
	}
	if value != nil {
		t.Fatalf("expected null arithmetic propagation, got %#v", value)
	}

	legacyValue, err := evalExpression("4 ^ 3 + 2 ^ 3", map[string]any{})
	if err != nil {
		t.Fatalf("legacy evalExpression exponent precedence failed: %v", err)
	}
	if fmt.Sprint(legacyValue) != "72.0" {
		t.Fatalf("unexpected legacy exponent precedence result: %#v", legacyValue)
	}

	value, err = evalExpressionWithScope("4 / 2 + 3 / 2", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 / 2 + 3 / 2) failed: %v", err)
	}
	if value != 3 {
		t.Fatalf("unexpected integer division precedence result: %#v", value)
	}

	value, err = evalExpressionWithScope("4 % (2 + 3) % 2", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(4 %% (2 + 3) %% 2) failed: %v", err)
	}
	if value != 0 {
		t.Fatalf("unexpected integer modulo precedence result: %#v", value)
	}
}

func TestEvalExpressionWithScopeLabelPredicate(t *testing.T) {
	row := Row{"n": &graph.Vertex{Labels: []string{"Foo", "Bar"}}}

	value, err := evalExpressionWithScope("n:Foo", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(n:Foo) failed: %v", err)
	}
	if value != true {
		t.Fatalf("unexpected label predicate result: %#v", value)
	}

	value, err = evalExpressionWithScope("n:Baz", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(n:Baz) failed: %v", err)
	}
	if value != false {
		t.Fatalf("unexpected missing label predicate result: %#v", value)
	}
}

func TestExecuteMatchReturnCountComparisonOnEmptyGraph(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("MATCH (a) RETURN count(a) > 0 AS result")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["result"]; got != false {
		t.Fatalf("unexpected count(a) > 0 result row: %#v", res.Rows[0])
	}
}

func TestDeleteBindingSemanticsForReturn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("CREATE ()-[:T {num: 1}]->()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH ()-[r]->() DELETE r RETURN type(r) AS relType")
	if err != nil {
		t.Fatalf("parse rel type failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute rel type failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["relType"] != "T" {
		t.Fatalf("unexpected deleted relationship type result: %#v", res.Rows)
	}

	stmt, err = parser.ParseStatement("CREATE (:A {num: 0})")
	if err != nil {
		t.Fatalf("parse node seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute node seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n:A) DELETE n RETURN n.num")
	if err != nil {
		t.Fatalf("parse deleted property failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected deleted node property access error")
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		t.Fatalf("expected ErrKindNotFound, got: %v", err)
	}
}

func TestExecuteReturnHexAndOctalLiterals(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN 0x1A AS hx, -0o10 AS oc")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["hx"]; got != 26 {
		t.Fatalf("expected hx=26, got %#v", got)
	}
	if got := res.Rows[0]["oc"]; got != -8 {
		t.Fatalf("expected oc=-8, got %#v", got)
	}
}

func TestApplySkipLimitLimitZeroReturnsNoRows(t *testing.T) {
	rows := []Row{{"n": 1}, {"n": 2}, {"n": 3}}
	trimmed := applySkipLimit(rows, 0, 0, true)
	if len(trimmed) != 0 {
		t.Fatalf("expected empty rows for LIMIT 0, got %#v", trimmed)
	}
}

func TestProjectionClauseSpecFromClausePrefersStructuredProjectionMetadata(t *testing.T) {
	clause := ast.Clause{
		Kind: ast.ClauseKindReturn,
		Raw:  "RETURN n ORDER BY wrong DESC SKIP 99 LIMIT 88",
		Projection: &ast.ReturnClause{
			Items: []ast.ProjectionItem{{Expression: ast.Expression{Raw: "n"}}},
			OrderBy: []ast.SortItem{{
				Expression: ast.Expression{Raw: "n"},
				Direction:  ast.SortDirectionAsc,
			}},
			Skip:  &ast.Expression{Raw: "1"},
			Limit: &ast.Expression{Raw: "2"},
		},
	}

	spec, err := projectionClauseSpecFromClause(clause)
	if err != nil {
		t.Fatalf("projectionClauseSpecFromClause failed: %v", err)
	}
	if spec.SkipRaw != "1" || spec.LimitRaw != "2" {
		t.Fatalf("expected structured skip/limit to win, got skip=%q limit=%q", spec.SkipRaw, spec.LimitRaw)
	}
	if len(spec.OrderBy) != 1 || spec.OrderBy[0].Expression != "n" || spec.OrderBy[0].Descending {
		t.Fatalf("expected structured order by metadata, got %#v", spec.OrderBy)
	}
}

func TestProjectionClauseSpecFromClauseRejectsCompactRawFallbackWithoutMetadata(t *testing.T) {
	clause := ast.Clause{
		Kind: ast.ClauseKindReturn,
		Raw:  "RETURNnORDERBYnDESCSKIP1LIMIT1",
	}

	_, err := projectionClauseSpecFromClause(clause)
	if err == nil {
		t.Fatalf("expected structured-projection error when metadata is unavailable")
	}
}

func TestProjectionClauseSpecFromClauseRejectsNonCompactRawFallback(t *testing.T) {
	clause := ast.Clause{
		Kind: ast.ClauseKindReturn,
		Raw:  "RETURN n ORDER BY n DESC SKIP 1 LIMIT 1",
	}

	_, err := projectionClauseSpecFromClause(clause)
	if err == nil {
		t.Fatalf("expected structured-projection error when metadata is unavailable")
	}
}

func TestParseProjectionItemsDoesNotSplitIdentifiersContainingAS(t *testing.T) {
	items, err := parseProjectionItems("a.name, a.age, a.seasons")
	if err != nil {
		t.Fatalf("parseProjectionItems failed: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 projection items, got %d", len(items))
	}
	if items[0].Expression != "a.name" || items[0].Alias != "" {
		t.Fatalf("unexpected first projection item: %#v", items[0])
	}
	if items[1].Expression != "a.age" || items[1].Alias != "" {
		t.Fatalf("unexpected second projection item: %#v", items[1])
	}
	if items[2].Expression != "a.seasons" || items[2].Alias != "" {
		t.Fatalf("unexpected third projection item: %#v", items[2])
	}
}

func TestExecuteCreateAnonymousNodesWithSameIDPropertyDoNotOverwrite(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 2}), (:B {id: 3})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	joinStmt, err := parser.ParseStatement("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b")
	if err != nil {
		t.Fatalf("join parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, joinStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("join execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one joined row, got %d (%#v)", len(res.Rows), res.Rows)
	}
}

func TestBuildStructuredProjectionClauseWith(t *testing.T) {
	clause, err := buildStructuredProjectionClause(ast.ClauseKindWith, "WITH n AS v ORDER BY v DESC SKIP 1 LIMIT 2", []string{"n"})
	if err != nil {
		t.Fatalf("buildStructuredProjectionClause failed: %v", err)
	}
	if clause.Kind != ast.ClauseKindWith {
		t.Fatalf("expected WITH clause, got %s", clause.Kind)
	}
	if clause.Projection == nil {
		t.Fatalf("expected structured projection on WITH clause")
	}
	if clause.Projection.Skip == nil || clause.Projection.Skip.Raw != "1" {
		t.Fatalf("expected WITH SKIP metadata")
	}
	if clause.Projection.Limit == nil || clause.Projection.Limit.Raw != "2" {
		t.Fatalf("expected WITH LIMIT metadata")
	}
	if len(clause.Projection.OrderBy) != 1 || clause.Projection.OrderBy[0].Direction != ast.SortDirectionDesc {
		t.Fatalf("expected WITH ORDER BY DESC metadata")
	}
}

func TestBuildStructuredProjectionClauseReturnWithScopePrelude(t *testing.T) {
	clause, err := buildStructuredProjectionClause(ast.ClauseKindReturn, "RETURN n ORDER BY n SKIP 1 LIMIT 2", []string{"n"})
	if err != nil {
		t.Fatalf("buildStructuredProjectionClause failed: %v", err)
	}
	if clause.Kind != ast.ClauseKindReturn {
		t.Fatalf("expected RETURN clause, got %s", clause.Kind)
	}
	if clause.Projection == nil {
		t.Fatalf("expected structured projection on RETURN clause")
	}
	if clause.Projection.Skip == nil || clause.Projection.Skip.Raw != "1" {
		t.Fatalf("expected RETURN SKIP metadata")
	}
	if clause.Projection.Limit == nil || clause.Projection.Limit.Raw != "2" {
		t.Fatalf("expected RETURN LIMIT metadata")
	}
}

func TestExecuteWithConstantLimitExpression(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	seed, err := parser.ParseStatement("UNWIND range(1, 3) AS i CREATE ({nr: i})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n) WITH n LIMIT toInteger(ceil(1.7)) RETURN n.nr AS nr ORDER BY nr")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["nr"]; got != 1 {
		t.Fatalf("expected first nr=1, got %#v", got)
	}
	if got := res.Rows[1]["nr"]; got != 2 {
		t.Fatalf("expected second nr=2, got %#v", got)
	}
}

func TestExecuteReturnScientificNotationLiterals(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN .1 AS a, 2E-01 AS b, 1e9 AS c, 1e-305 AS d")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["a"]; got != json.Number("0.1") {
		t.Fatalf("expected a=0.1, got %#v", got)
	}
	if got := res.Rows[0]["b"]; got != json.Number("0.2") {
		t.Fatalf("expected b=0.2, got %#v", got)
	}
	if got := res.Rows[0]["c"]; got != json.Number("1000000000.0") {
		t.Fatalf("expected c=1000000000.0, got %#v", got)
	}
	if got := res.Rows[0]["d"]; got != json.Number("1e-305") {
		t.Fatalf("expected d=1e-305, got %#v", got)
	}
}

func TestExecuteDurationPreservesNanosecondPrecision(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN duration({days: 14, seconds: 70, nanoseconds: 1}) AS d")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["d"]; got != "P14DT1M10.000000001S" {
		t.Fatalf("expected duration with nanos, got %#v", got)
	}
}

func TestExecuteReturnPassAFunctionSurface(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:K {z: 1, a: 2})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n:K) RETURN keys(n) AS nk, keys({b: 1, a: 2}) AS mk, head([10, 20]) AS h, head([]) AS hn, tail([10, 20, 30]) AS t, tail([10]) AS te, abs(-3) AS ai, abs(-1.5) AS af, sqrt(12.96) AS sq")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}

	if got := res.Rows[0]["nk"]; !reflect.DeepEqual(got, []any{"a", "z"}) {
		t.Fatalf("expected nk=[a z], got %#v", got)
	}
	if got := res.Rows[0]["mk"]; !reflect.DeepEqual(got, []any{"a", "b"}) {
		t.Fatalf("expected mk=[a b], got %#v", got)
	}
	if got := res.Rows[0]["h"]; got != 10 {
		t.Fatalf("expected h=10, got %#v", got)
	}
	if got := res.Rows[0]["hn"]; got != nil {
		t.Fatalf("expected hn=nil, got %#v", got)
	}
	if got := res.Rows[0]["t"]; !reflect.DeepEqual(got, []any{20, 30}) {
		t.Fatalf("expected t=[20 30], got %#v", got)
	}
	if got := res.Rows[0]["te"]; !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("expected te=[], got %#v", got)
	}
	if got := res.Rows[0]["ai"]; got != 3 {
		t.Fatalf("expected ai=3, got %#v", got)
	}
	if got := res.Rows[0]["af"]; got != json.Number("1.5") {
		t.Fatalf("expected af=1.5, got %#v", got)
	}
	if got := res.Rows[0]["sq"]; got != json.Number("3.6") {
		t.Fatalf("expected sq=3.6, got %#v", got)
	}
}

func TestExecuteReturnToBooleanAndToFloat(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN toBoolean('true') AS bt, toBoolean('f alse') AS bi, toBoolean(null) AS bn, toFloat('5') AS fs, toFloat('foo') AS ff, toFloat(3) AS fi, toFloat(3.4) AS fd")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}

	row := res.Rows[0]
	if got := row["bt"]; got != true {
		t.Fatalf("expected bt=true, got %#v", got)
	}
	if got := row["bi"]; got != nil {
		t.Fatalf("expected bi=nil, got %#v", got)
	}
	if got := row["bn"]; got != nil {
		t.Fatalf("expected bn=nil, got %#v", got)
	}
	if got := row["fs"]; got != json.Number("5.0") {
		t.Fatalf("expected fs=5.0, got %#v", got)
	}
	if got := row["ff"]; got != nil {
		t.Fatalf("expected ff=nil, got %#v", got)
	}
	if got := row["fi"]; got != json.Number("3.0") {
		t.Fatalf("expected fi=3.0, got %#v", got)
	}
	if got := row["fd"]; got != json.Number("3.4") {
		t.Fatalf("expected fd=3.4, got %#v", got)
	}
}

func TestExecuteToBooleanAndToFloatRejectInvalidTypes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("RETURN toBoolean([true]) AS b")
	if err != nil {
		t.Fatalf("parse toBoolean invalid failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected toBoolean invalid type error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected ErrKindInvalidInput for toBoolean, got %v", err)
	}

	stmt, err = parser.ParseStatement("RETURN toFloat(true) AS f")
	if err != nil {
		t.Fatalf("parse toFloat invalid failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected toFloat invalid type error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected ErrKindInvalidInput for toFloat, got %v", err)
	}
}

func TestExecuteRemainingFunctionSurfacePathAndRelationship(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (a {id: 2})-[:REL {num: 1}]->(b {id: 1})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH p=(a)-[r:REL]->(b) RETURN size(nodes(p)) AS nn, size(relationships(p)) AS nr, length(p) AS l, startNode(r).id AS s, endNode(r).id AS e, sign(-5) AS sn, sign(0) AS sz, sign(4) AS sp, last([1,2,3]) AS lv, last([]) AS le")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}

	row := res.Rows[0]
	if got := row["nn"]; got != 2 {
		t.Fatalf("expected nn=2, got %#v", got)
	}
	if got := row["nr"]; got != 1 {
		t.Fatalf("expected nr=1, got %#v", got)
	}
	if got := row["l"]; got != 1 {
		t.Fatalf("expected l=1, got %#v", got)
	}
	if got := row["s"]; got != 2 {
		t.Fatalf("expected s=2, got %#v", got)
	}
	if got := row["e"]; got != 1 {
		t.Fatalf("expected e=1, got %#v", got)
	}
	if got := row["sn"]; got != -1 {
		t.Fatalf("expected sn=-1, got %#v", got)
	}
	if got := row["sz"]; got != 0 {
		t.Fatalf("expected sz=0, got %#v", got)
	}
	if got := row["sp"]; got != 1 {
		t.Fatalf("expected sp=1, got %#v", got)
	}
	if got := row["lv"]; got != 3 {
		t.Fatalf("expected lv=3, got %#v", got)
	}
	if got := row["le"]; got != nil {
		t.Fatalf("expected le=nil, got %#v", got)
	}
}

func TestExecutePathFunctionsReturnNullOnNullPath(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:A)")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (a:A) OPTIONAL MATCH p = (a)-[r]->() RETURN nodes(p) AS np, relationships(p) AS rp, length(p) AS lp")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["np"]; got != nil {
		t.Fatalf("expected np=nil, got %#v", got)
	}
	if got := res.Rows[0]["rp"]; got != nil {
		t.Fatalf("expected rp=nil, got %#v", got)
	}
	if got := res.Rows[0]["lp"]; got != nil {
		t.Fatalf("expected lp=nil, got %#v", got)
	}
}

func TestExecuteDirectedBoundedVariableLengthMatchPathFunctions(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (s:Start {id: 1})-[:REL {num: 1}]->(:Mid {id: 2})-[:REL {num: 2}]->(e:End {id: 3})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH p = (a:Start)-[:REL*2..2]->(b:End) RETURN size(relationships(p)) AS nr, length(p) AS l")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["nr"]; got != 2 {
		t.Fatalf("expected nr=2, got %#v", got)
	}
	if got := res.Rows[0]["l"]; got != 2 {
		t.Fatalf("expected l=2, got %#v", got)
	}
}

func TestExecuteUndirectedBoundedVariableLengthMatchReturnsEdgeSequences(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (a:End {id: 1})-[:REL {num: 1}]->(:Mid {id: 2})-[:REL {num: 2}]->(b:End {id: 3})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (a)-[r:REL*2..2]-(b:End) RETURN size(r) AS nr")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected two rows, got %d", len(res.Rows))
	}
	for i, row := range res.Rows {
		if got := row["nr"]; got != 2 {
			t.Fatalf("row %d expected nr=2, got %#v", i, got)
		}
	}
}

func TestExecutePercentileAggregates(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE ({price: 10.0}), ({price: 20.0}), ({price: 30.0})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n) RETURN percentileDisc(n.price, 0.5) AS d, percentileCont(n.price, 0.5) AS c")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["d"]; got != json.Number("20.0") {
		t.Fatalf("expected d=20.0, got %#v", got)
	}
	if got := res.Rows[0]["c"]; got != json.Number("20.0") {
		t.Fatalf("expected c=20.0, got %#v", got)
	}
}

func TestExecutePercentileAggregatesRejectOutOfRangePercentile(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE ({price: 10.0})")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n) RETURN percentileDisc(n.price, 1.1) AS p")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected out-of-range percentileDisc error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected ErrKindInvalidInput for percentileDisc, got %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n) RETURN percentileCont(n.price, -1) AS p")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected out-of-range percentileCont error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected ErrKindInvalidInput for percentileCont, got %v", err)
	}
}

func TestExecutePatternComprehensionProjectionLiteral(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("CREATE (:S)-[:REL]->(), (:S)-[:REL]->(), (:S)")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n:S) RETURN size([(n)-->() | 1]) AS deg ORDER BY deg DESC")
	if err != nil {
		t.Fatalf("parse query failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute query failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["deg"]; got != 1 {
		t.Fatalf("expected first deg=1, got %#v", got)
	}
}

func TestDecodeStoredPropertyValuePreservesWhitespace(t *testing.T) {
	if got := decodeStoredPropertyValue([]byte(" Foo ")); got != " Foo " {
		t.Fatalf("expected preserved whitespace, got %#v", got)
	}
	if got := decodeStoredPropertyValue([]byte("\nFoo\n")); got != "\nFoo\n" {
		t.Fatalf("expected preserved newlines, got %#v", got)
	}
}

func TestUnquoteCypherStringSingleQuotedEscapes(t *testing.T) {
	cases := map[string]string{
		"'\\nFoo\\n'":            "\nFoo\n",
		"'\\tFoo\\t'":            "\tFoo\t",
		"'\\u004Aohn'":           "John",
		"'Foo''Bar'":             "Foo'Bar",
		"'\\\\path\\\\x'":        "\\path\\x",
		`'\''`:                   "'",
		`'a\\bcn5t\'"\\//\\"\''`: `a\bcn5t'"\//\"'`,
	}
	for raw, want := range cases {
		got, err := unquoteCypherString(raw)
		if err != nil {
			t.Fatalf("unquoteCypherString(%q) failed: %v", raw, err)
		}
		if got != want {
			t.Fatalf("unquoteCypherString(%q) = %#v, want %#v", raw, got, want)
		}
	}
}

func TestEvalWhereExpressionNotPropagatesNull(t *testing.T) {
	exec := &Executor{}
	value, err := exec.evalWhereExpression(context.Background(), nil, "NOT null", Row{}, nil)
	if err != nil {
		t.Fatalf("evalWhereExpression failed: %v", err)
	}
	if value {
		t.Fatalf("expected NOT null to be filtered out, got true")
	}
}

func TestEvalExpressionWithScopeDecodesSingleQuotedEscapes(t *testing.T) {
	value, err := evalExpressionWithScope("'\\nFoo\\n'", Row{}, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope failed: %v", err)
	}
	if value != "\nFoo\n" {
		t.Fatalf("expected decoded newline string, got %#v", value)
	}

	props, err := parsePropertyMap("name: '\\nFoo\\n'", nil, Row{})
	if err != nil {
		t.Fatalf("parsePropertyMap failed: %v", err)
	}
	if got := props["name"]; got != "\nFoo\n" {
		t.Fatalf("expected parsed property newline string, got %#v", got)
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
