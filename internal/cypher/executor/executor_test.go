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
	statsSnapshot, ok := influencers["statsSnapshot"].(map[string]any)
	if !ok {
		t.Fatalf("expected statsSnapshot map, got %T", influencers["statsSnapshot"])
	}
	coverage, ok := statsSnapshot["coverage"].(map[string]any)
	if !ok {
		t.Fatalf("expected statsSnapshot.coverage map, got %T", statsSnapshot["coverage"])
	}
	if totals, _ := coverage["totals"].(string); totals != "snapshot" {
		t.Fatalf("expected statsSnapshot coverage totals=snapshot, got %#v", coverage["totals"])
	}
	if nodeCountsCoverage, _ := coverage["nodeCounts"].(string); nodeCountsCoverage != "snapshot" {
		t.Fatalf("expected statsSnapshot coverage nodeCounts=snapshot, got %#v", coverage["nodeCounts"])
	}
	if edgeCountsCoverage, _ := coverage["edgeCounts"].(string); edgeCountsCoverage != "snapshot" {
		t.Fatalf("expected statsSnapshot coverage edgeCounts=snapshot, got %#v", coverage["edgeCounts"])
	}
	if completeness, _ := statsSnapshot["completeness"].(string); completeness != "complete" {
		t.Fatalf("expected statsSnapshot completeness=complete, got %#v", statsSnapshot["completeness"])
	}
	if backfillStatus, _ := statsSnapshot["backfillStatus"].(string); backfillStatus != "complete" {
		t.Fatalf("expected statsSnapshot backfillStatus=complete, got %#v", statsSnapshot["backfillStatus"])
	}
	if backfillRequired, _ := statsSnapshot["backfillRequired"].(bool); backfillRequired {
		t.Fatalf("expected statsSnapshot backfillRequired=false, got %#v", statsSnapshot["backfillRequired"])
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

func TestExecuteExplainFastLabelHistogramPlan(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n) RETURN labels(n) AS l, count(labels(n)) AS lc")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %d", len(res.Rows))
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	nodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}
	if len(nodes) < 2 {
		t.Fatalf("expected at least 2 plan nodes, got %d", len(nodes))
	}
	firstOp, _ := nodes[0]["op"].(string)
	if firstOp != "ALL_NODES_SCAN" {
		t.Fatalf("expected first plan node ALL_NODES_SCAN, got %#v", nodes[0]["op"])
	}

	foundFastAggregate := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		if op != "AGGREGATE" {
			continue
		}
		impl, _ := node["implementation"].(string)
		if impl != "fast_label_histogram" {
			continue
		}
		projection, _ := node["projection"].([]string)
		if !reflect.DeepEqual(projection, []string{"l", "lc"}) {
			t.Fatalf("expected fast aggregate projection [l lc], got %#v", node["projection"])
		}
		foundFastAggregate = true
		break
	}
	if !foundFastAggregate {
		t.Fatalf("expected AGGREGATE node with implementation=fast_label_histogram, got nodes %#v", nodes)
	}
}

func TestExecuteExplainFastEdgeCountPlan(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "R", SrcID: "m1", DstID: "p1"},
			{Tenant: "acme", ID: "e2", Type: "R", SrcID: "p1", DstID: "m2"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	tests := []string{
		"EXPLAIN MATCH ()-[e]-() RETURN count(e)",
		"EXPLAIN MATCH (:Movie)-[e]-() RETURN count(e)",
		"EXPLAIN MATCH ()-[e]-(:Movie) RETURN count(e)",
	}

	exec := New(store, Options{})
	for _, query := range tests {
		stmt, err := parser.ParseStatement(query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", query, err)
		}
		explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
		if !ok {
			t.Fatalf("expected explain payload map for %q, got %T", query, res.Rows[0]["explain"])
		}
		logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan map for %q, got %T", query, explainPayload["logicalPlan"])
		}
		nodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		found := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			if op != "AGGREGATE" {
				continue
			}
			impl, _ := node["implementation"].(string)
			if impl == "fast_edge_count" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected AGGREGATE fast_edge_count for %q, got nodes %#v", query, nodes)
		}
	}
}

func TestExecuteExplainFastEdgeDeletePlan(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "R", SrcID: "m1", DstID: "p1"},
			{Tenant: "acme", ID: "e2", Type: "R", SrcID: "p1", DstID: "m2"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	tests := []string{
		"EXPLAIN MATCH ()-[e]-() DELETE e",
		"EXPLAIN MATCH (:Movie)-[e]-() DELETE e",
		"EXPLAIN MATCH ()-[e]-(:Movie) DELETE e",
	}

	exec := New(store, Options{})
	for _, query := range tests {
		stmt, err := parser.ParseStatement(query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", query, err)
		}
		explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
		if !ok {
			t.Fatalf("expected explain payload map for %q, got %T", query, res.Rows[0]["explain"])
		}
		logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan map for %q, got %T", query, explainPayload["logicalPlan"])
		}
		nodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		found := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			if op != "DELETE" {
				continue
			}
			impl, _ := node["implementation"].(string)
			if impl == "fast_edge_delete" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected DELETE fast_edge_delete for %q, got nodes %#v", query, nodes)
		}
	}
}

func TestExecuteExplainFastNodeCountPlan(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "v1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "v2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "v3", Labels: []string{"Genre"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	queries := []string{
		"EXPLAIN MATCH (n) RETURN count(n)",
		"EXPLAIN MATCH (n) RETURN count(n) AS total",
	}

	exec := New(store, Options{})
	for _, query := range queries {
		stmt, err := parser.ParseStatement(query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", query, err)
		}
		explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
		if !ok {
			t.Fatalf("expected explain payload map for %q, got %T", query, res.Rows[0]["explain"])
		}
		logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan map for %q, got %T", query, explainPayload["logicalPlan"])
		}
		nodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		found := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			if op != "AGGREGATE" {
				continue
			}
			impl, _ := node["implementation"].(string)
			if impl == "fast_node_count" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected AGGREGATE fast_node_count for %q, got nodes %#v", query, nodes)
		}

		cardinality, ok := explainPayload["cardinality"].([]map[string]any)
		if !ok || len(cardinality) < 2 {
			t.Fatalf("expected cardinality entries for %q, got %#v", query, explainPayload["cardinality"])
		}
		if rowsOut, _ := cardinality[0]["rowsOut"].(int); rowsOut != 3 {
			t.Fatalf("expected ALL_NODES_SCAN rowsOut=3 for %q, got %#v", query, cardinality[0]["rowsOut"])
		}
		if quality, _ := cardinality[0]["quality"].(string); quality != "exact" {
			t.Fatalf("expected ALL_NODES_SCAN quality exact for %q, got %#v", query, cardinality[0]["quality"])
		}
		if rowsOut, _ := cardinality[1]["rowsOut"].(int); rowsOut != 1 {
			t.Fatalf("expected AGGREGATE rowsOut=1 for %q, got %#v", query, cardinality[1]["rowsOut"])
		}

		costEstimate, ok := explainPayload["costEstimate"].(map[string]any)
		if !ok {
			t.Fatalf("expected costEstimate map for %q, got %T", query, explainPayload["costEstimate"])
		}
		components, ok := costEstimate["components"].(map[string]any)
		if !ok {
			t.Fatalf("expected costEstimate components map for %q, got %T", query, costEstimate["components"])
		}
		if scanRows, _ := components["scanRows"].(int); scanRows != 3 {
			t.Fatalf("expected scanRows=3 for %q, got %#v", query, components["scanRows"])
		}

		runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
		if !ok {
			t.Fatalf("expected runtimeStats map for %q, got %T", query, explainPayload["runtimeStats"])
		}
		storeStats, ok := runtimeStats["store"].(map[string]any)
		if !ok {
			t.Fatalf("expected runtimeStats.store map for %q, got %T", query, runtimeStats["store"])
		}
		if verticesScanned, _ := storeStats["verticesScanned"].(int); verticesScanned != 3 {
			t.Fatalf("expected verticesScanned=3 for %q, got %#v", query, storeStats["verticesScanned"])
		}
		if edgesScanned, _ := storeStats["edgesScanned"].(int); edgesScanned != 0 {
			t.Fatalf("expected edgesScanned=0 for %q, got %#v", query, storeStats["edgesScanned"])
		}

		warnings, ok := explainPayload["warnings"].([]map[string]any)
		if !ok {
			t.Fatalf("expected warnings []map for %q, got %T", query, explainPayload["warnings"])
		}
		for _, warning := range warnings {
			if code, _ := warning["code"].(string); code == "FULL_SCAN_FALLBACK" {
				t.Fatalf("did not expect FULL_SCAN_FALLBACK for %q, got warnings %#v", query, warnings)
			}
		}
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

func TestExecuteMergeNodePropertyIndexMetricsRecorded(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "m-existing",
			Labels:     []string{"Movie"},
			Properties: graph.PropertyMap{"movie_id": []byte("1"), "title": []byte("Old")},
		}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "Movie",
			Property:    "movie_id",
			Value:       []byte("1"),
			EntityID:    "m-existing",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("MERGE (m:Movie {movie_id: $movie_id}) SET m.title = $title RETURN m.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Movie", "movie_id")
	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{IndexCatalog: catalog, Metrics: recorder})

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "movie_id": "1", "title": "Updated"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["id"] != "m-existing" {
		t.Fatalf("expected merge to match existing movie, got %#v", res.Rows)
	}

	if len(recorder.indexCandidates) == 0 {
		t.Fatalf("expected index candidate metrics")
	}
	if len(recorder.indexLookups) == 0 {
		t.Fatalf("expected index lookup metrics")
	}

	foundHit := false
	for _, lookup := range recorder.indexLookups {
		if lookup.strategy == "property_index" && lookup.outcome == "hit" {
			foundHit = true
			break
		}
	}
	if !foundHit {
		t.Fatalf("expected property_index hit lookup metric, got %#v", recorder.indexLookups)
	}
}

func TestExecuteMergeMaintainsPropertyIndexEntriesForRepeatedRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("MERGE (m:Movie {movie_id: $movie_id}) SET m.title = $title RETURN m.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Movie", "movie_id")
	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{IndexCatalog: catalog, Metrics: recorder})

	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "movie_id": "1", "title": "One"}); err != nil {
		t.Fatalf("first merge failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "movie_id": "1", "title": "Two"}); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	verifyErr := store.View(ctx, func(tx graph.Tx) error {
		count := 0
		var found *graph.Vertex
		if err := tx.ScanVertices(ctx, "acme", 0, func(vertex *graph.Vertex) error {
			if !vertexHasLabel(vertex, "Movie") {
				return nil
			}
			if value, ok := vertex.Properties["movie_id"]; !ok || string(value) != "1" {
				return nil
			}
			count++
			found = vertex
			return nil
		}); err != nil {
			return err
		}
		if count != 1 {
			return errUnexpected(fmt.Sprintf("expected one merged movie vertex, got %d", count))
		}
		if found == nil || string(found.Properties["title"]) != "Two" {
			return errUnexpected(fmt.Sprintf("unexpected merged vertex state: %#v", found))
		}
		return nil
	})
	if verifyErr != nil {
		t.Fatalf("verification failed: %v", verifyErr)
	}

	foundHit := false
	for _, lookup := range recorder.indexLookups {
		if lookup.strategy == "property_index" && lookup.outcome == "hit" {
			foundHit = true
			break
		}
	}
	if !foundHit {
		t.Fatalf("expected property_index hit lookup metric across repeated merges, got %#v", recorder.indexLookups)
	}
}

func TestExecuteWriteContextMatchUsesPropertyIndexLookup(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "m-42",
			Labels:     []string{"Movie"},
			Properties: graph.PropertyMap{"movie_id": []byte("42"), "title": []byte("The Answer")},
		}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "Movie",
			Property:    "movie_id",
			Value:       []byte("42"),
			EntityID:    "m-42",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("UNWIND $rows AS r CREATE (:IngestLog {id: r.log_id}) WITH r MATCH (m:Movie {movie_id: r.movie_id}) RETURN count(m) AS matched")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Movie", "movie_id")
	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{IndexCatalog: catalog, Metrics: recorder})

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"rows": []any{
			map[string]any{"movie_id": "42", "log_id": "l1"},
			map[string]any{"movie_id": "42", "log_id": "l2"},
		},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %#v", res.Rows)
	}

	foundHit := false
	for _, lookup := range recorder.indexLookups {
		if lookup.strategy == "property_index" && lookup.outcome == "hit" {
			foundHit = true
			break
		}
	}
	if !foundHit {
		t.Fatalf("expected property_index hit for write-context MATCH, got %#v", recorder.indexLookups)
	}
}

func TestExecuteExplainWriteContextMatchReportsIndexScan(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "m-42",
			Labels:     []string{"Movie"},
			Properties: graph.PropertyMap{"movie_id": []byte("42")},
		}); err != nil {
			return err
		}
		return tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      "acme",
			Schema:      "Movie",
			Property:    "movie_id",
			Value:       []byte("42"),
			EntityID:    "m-42",
			EntityClass: "vertex",
		})
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("EXPLAIN UNWIND $rows AS r CREATE (:IngestLog {id: r.log_id}) WITH r MATCH (m:Movie {movie_id: r.movie_id}) RETURN m.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "Movie", "movie_id")
	exec := New(store, Options{IndexCatalog: catalog})

	res, err := exec.ExecuteStatement(ctx, stmt, Params{
		"tenant": "acme",
		"rows":   []any{map[string]any{"movie_id": "42", "log_id": "l1"}},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	payload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := payload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", payload["logicalPlan"])
	}
	nodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}

	foundIndexScan := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		if op != "INDEX_SCAN" {
			continue
		}
		accessPath, _ := node["accessPath"].(string)
		if accessPath == "property_index(Movie.movie_id)" {
			foundIndexScan = true
			break
		}
	}
	if !foundIndexScan {
		t.Fatalf("expected write-context MATCH to report property index scan, got nodes %#v", nodes)
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

func TestExecuteExistsFunctionAndExistsSubqueryRemainDistinct(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-1", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Ada"), "nickname": []byte("Ace")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p-2", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("Bob")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"title": []byte("Graph Ops")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "ACTED_IN", SrcID: "p-2", DstID: "m-1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n:Person) WHERE exists(n.nickname) RETURN n.name AS name ORDER BY name ASC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row for exists(n.nickname), got %d", len(res.Rows))
	}
	if got := res.Rows[0]["name"]; got != "Ada" {
		t.Fatalf("unexpected exists(n.nickname) row: %#v", got)
	}

	stmt, err = parser.ParseStatement("MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:ACTED_IN]->(:Movie) } RETURN n.name AS name ORDER BY name ASC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row for EXISTS { ... }, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["name"]; got != "Bob" {
		t.Fatalf("unexpected EXISTS { ... } row: %#v", got)
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

// TestSetCaseSimple verifies that a CASE expression may appear on the RHS of a
// SET property assignment (simple CASE — switch on a comparison value).
func TestSetCaseSimple(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	// Create two nodes: a "person" with role and a "dept" with two phone fields.
	setup, err := parser.ParseStatement(
		"CREATE (p:Person {id:'p1', role:'business'}), (d:Dept {id:'d1', departmentPhone:'555-DEPT', businessPhone:'555-BIZ'})")
	if err != nil {
		t.Fatalf("setup parse failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	// Run a MATCH + SET that assigns x.phone via a CASE on p.role.
	stmt, err := parser.ParseStatement(
		"MATCH (p:Person {id:'p1'}), (d:Dept {id:'d1'}), (x:Person {id:'p1'}) " +
			"SET x.phone = CASE p.role " +
			"  WHEN 'management' THEN d.departmentPhone " +
			"  WHEN 'business'   THEN d.businessPhone " +
			"  ELSE d.departmentPhone END " +
			"RETURN x.phone AS phone")
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
	got, _ := res.Rows[0]["phone"].(string)
	if got != "555-BIZ" {
		t.Fatalf("expected '555-BIZ' (business phone from CASE), got %q", got)
	}
}

// TestSetCaseGeneric verifies a generic CASE WHEN expression (boolean
// conditions) on the RHS of a SET property assignment.
func TestSetCaseGeneric(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	setup, err := parser.ParseStatement(
		"CREATE (n:Node {id:'n1', score:42})")
	if err != nil {
		t.Fatalf("setup parse failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement(
		"MATCH (n:Node {id:'n1'}) " +
			"SET n.grade = CASE " +
			"  WHEN n.score >= 90 THEN 'A' " +
			"  WHEN n.score >= 70 THEN 'B' " +
			"  ELSE 'C' END " +
			"RETURN n.grade AS grade")
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
	got, _ := res.Rows[0]["grade"].(string)
	if got != "C" {
		t.Fatalf("expected grade 'C' (score 42), got %q", got)
	}
}

// TestSetCaseMultiProperty verifies that multiple SET items can be combined
// where one or more use CASE expressions.
func TestSetCaseMultiProperty(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	setup, err := parser.ParseStatement(
		"CREATE (n:Node {id:'n2', flag:true})")
	if err != nil {
		t.Fatalf("setup parse failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement(
		"MATCH (n:Node {id:'n2'}) " +
			"SET n.label = CASE n.flag WHEN true THEN 'active' ELSE 'inactive' END, " +
			"    n.note = 'set' " +
			"RETURN n.label AS label, n.note AS note")
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
	if got, _ := res.Rows[0]["label"].(string); got != "active" {
		t.Fatalf("expected label 'active', got %q", got)
	}
	if got, _ := res.Rows[0]["note"].(string); got != "set" {
		t.Fatalf("expected note 'set', got %q", got)
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

func TestExecuteFastEdgeCountQueries(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p2", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"Other"}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "R", SrcID: "m1", DstID: "p1"},
			{Tenant: "acme", ID: "e2", Type: "R", SrcID: "p2", DstID: "m1"},
			{Tenant: "acme", ID: "e3", Type: "R", SrcID: "m1", DstID: "m2"},
			{Tenant: "acme", ID: "e4", Type: "R", SrcID: "p1", DstID: "x1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	tests := []struct {
		query string
		want  string
	}{
		{query: "MATCH ()-[e]-() RETURN count(e) AS c", want: "8"},
		{query: "MATCH (:Movie)-[e]-() RETURN count(e) AS c", want: "4"},
		{query: "MATCH ()-[e]-(:Movie) RETURN count(e) AS c", want: "4"},
	}

	for _, tc := range tests {
		stmt, err := parser.ParseStatement(tc.query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", tc.query, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected one row for %q, got %d", tc.query, len(res.Rows))
		}
		if got := fmt.Sprint(res.Rows[0]["c"]); got != tc.want {
			t.Fatalf("unexpected count for %q: got %s want %s", tc.query, got, tc.want)
		}
	}
}

func TestExecuteBuiltinProcedureUniquePhysicalEdgeCount(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"Other"}}); err != nil {
			return err
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e1", Type: "R", SrcID: "m1", DstID: "p1"},
			{Tenant: "acme", ID: "e2", Type: "R", SrcID: "p1", DstID: "m1"},
			{Tenant: "acme", ID: "e3", Type: "R", SrcID: "m1", DstID: "m2"},
			{Tenant: "acme", ID: "e4", Type: "R", SrcID: "p1", DstID: "x1"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	tests := []struct {
		query string
		want  string
	}{
		{query: "CALL db.stats.edgeCount()", want: "4"},
		{query: "CALL db.stats.edgeCount('Movie')", want: "3"},
		{query: "CALL db.stats.edgeCount('Movie') YIELD edgeCount AS c", want: "3"},
	}

	for _, tc := range tests {
		stmt, err := parser.ParseStatement(tc.query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", tc.query, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected one row for %q, got %d", tc.query, len(res.Rows))
		}

		column := "edgeCount"
		if strings.Contains(tc.query, " AS c") {
			column = "c"
		}
		if got := fmt.Sprint(res.Rows[0][column]); got != tc.want {
			t.Fatalf("unexpected count for %q: got %s want %s", tc.query, got, tc.want)
		}
	}
}

func TestExecuteBuiltinProcedureNodeCount(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "x1", Labels: []string{"Other"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	tests := []struct {
		query string
		want  string
	}{
		{query: "CALL db.stats.nodeCount()", want: "4"},
		{query: "CALL db.stats.nodeCount('Movie')", want: "2"},
		{query: "CALL db.stats.nodeCount('Movie') YIELD nodeCount AS c", want: "2"},
	}

	for _, tc := range tests {
		stmt, err := parser.ParseStatement(tc.query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", tc.query, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected one row for %q, got %d", tc.query, len(res.Rows))
		}

		column := "nodeCount"
		if strings.Contains(tc.query, " AS c") {
			column = "c"
		}
		if got := fmt.Sprint(res.Rows[0][column]); got != tc.want {
			t.Fatalf("unexpected count for %q: got %s want %s", tc.query, got, tc.want)
		}
	}
}

func TestExecuteFastLabelHistogramQuery(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}},
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "mf1", Labels: []string{"Movie", "Featured"}},
			{Tenant: "acme", ID: "u1"},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	tests := []struct {
		query        string
		wantColumns  []string
		labelsColumn string
		countColumn  string
	}{
		{
			query:        "MATCH (n) RETURN count(labels(n)), labels(n)",
			wantColumns:  []string{"count(labels(n))", "labels(n)"},
			labelsColumn: "labels(n)",
			countColumn:  "count(labels(n))",
		},
		{
			query:        "MATCH (n) RETURN labels(n), count(labels(n))",
			wantColumns:  []string{"labels(n)", "count(labels(n))"},
			labelsColumn: "labels(n)",
			countColumn:  "count(labels(n))",
		},
		{
			query:        "MATCH (n) RETURN count(labels(n)) AS ct, labels(n) AS lbs",
			wantColumns:  []string{"ct", "lbs"},
			labelsColumn: "lbs",
			countColumn:  "ct",
		},
		{
			query:        "MATCH (n) RETURN labels(n) AS lbs, count(labels(n)) AS ct",
			wantColumns:  []string{"lbs", "ct"},
			labelsColumn: "lbs",
			countColumn:  "ct",
		},
	}

	wantHistogram := map[string]string{
		`["Movie"]`:            "2",
		`["Person"]`:           "1",
		`["Movie","Featured"]`: "1",
		`null`:                 "1",
	}

	for _, tc := range tests {
		stmt, err := parser.ParseStatement(tc.query)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.query, err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
		if err != nil {
			t.Fatalf("execute failed for %q: %v", tc.query, err)
		}
		if !reflect.DeepEqual(res.Columns, tc.wantColumns) {
			t.Fatalf("unexpected columns for %q: got %#v want %#v", tc.query, res.Columns, tc.wantColumns)
		}

		gotHistogram := map[string]string{}
		for _, row := range res.Rows {
			labelsRaw := row[tc.labelsColumn]
			labelsKeyBytes, marshalErr := json.Marshal(labelsRaw)
			if marshalErr != nil {
				t.Fatalf("json.Marshal(labels) failed for %q: %v", tc.query, marshalErr)
			}
			gotHistogram[string(labelsKeyBytes)] = fmt.Sprint(row[tc.countColumn])
		}
		if !reflect.DeepEqual(gotHistogram, wantHistogram) {
			t.Fatalf("unexpected histogram for %q: got %#v want %#v", tc.query, gotHistogram, wantHistogram)
		}
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

func TestEvalExpressionWithScopeAdditionalBuiltInFunctions(t *testing.T) {
	row := Row{
		"mapv": map[string]any{"name": "Apa"},
		"p": cypherPathValue{
			Left:  &graph.Vertex{ID: "n1"},
			Edge:  &graph.Edge{ID: "r1", SrcID: "n1", DstID: "n2"},
			Right: &graph.Vertex{ID: "n2"},
		},
	}

	tests := []struct {
		expr string
		want any
	}{
		{expr: "lower('ABC')", want: "abc"},
		{expr: "upper('abc')", want: "ABC"},
		{expr: "left('vitaledge', 5)", want: "vital"},
		{expr: "right('vitaledge', 4)", want: "edge"},
		{expr: "replace('vital-edge', '-', '_')", want: "vital_edge"},
		{expr: "trim('  keep  ')", want: "keep"},
		{expr: "ltrim('  keep  ')", want: "keep  "},
		{expr: "rtrim('  keep  ')", want: "  keep"},
		{expr: "char_length('é🙂')", want: 2},
		{expr: "character_length('abc')", want: 3},
		{expr: "path_length(p)", want: 1},
		{expr: "isEmpty([])", want: true},
		{expr: "isEmpty([1])", want: false},
		{expr: "isEmpty('')", want: true},
		{expr: "isEmpty('x')", want: false},
		{expr: "nullIf(1, 1)", want: nil},
		{expr: "nullIf(1, 2)", want: 1},
		{expr: "toStringOrNull(mapv)", want: nil},
		{expr: "toIntegerOrNull(mapv)", want: nil},
		{expr: "toFloatOrNull(mapv)", want: nil},
		{expr: "toBooleanOrNull(mapv)", want: nil},
	}

	for _, tc := range tests {
		got, err := evalExpressionWithScope(tc.expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.expr, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %#v", tc.expr, got, tc.want)
		}
	}

	value, err := evalExpressionWithScope("ceiling(1.2)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(ceiling(1.2)) failed: %v", err)
	}
	if fmt.Sprint(value) != "2.0" {
		t.Fatalf("evalExpressionWithScope(ceiling(1.2)) = %#v, want 2.0", value)
	}
}

func TestEvalExpressionWithScopeMathematicalBuiltInsSecondTranche(t *testing.T) {
	row := Row{}

	tests := []struct {
		expr string
		want string
	}{
		{expr: "floor(3.9)", want: "3.0"},
		{expr: "round(1.5)", want: "2.0"},
		{expr: "round(15.55, 1)", want: "15.6"},
		{expr: "round(15.55, 1, 'DOWN')", want: "15.5"},
		{expr: "round(log(e()), 6)", want: "1.0"},
		{expr: "round(ln(e()), 6)", want: "1.0"},
		{expr: "log10(1000)", want: "3.0"},
		{expr: "degrees(pi())", want: "180.0"},
		{expr: "round(radians(180), 6)", want: "3.141593"},
		{expr: "round(sin(pi()/2), 6)", want: "1.0"},
		{expr: "round(cos(0), 6)", want: "1.0"},
		{expr: "round(tan(0), 6)", want: "0.0"},
		{expr: "round(asin(1), 6)", want: "1.570796"},
		{expr: "acos(1)", want: "0.0"},
		{expr: "round(atan(1), 6)", want: "0.785398"},
		{expr: "round(atan2(0, -1), 6)", want: "3.141593"},
		{expr: "round(cot(pi()/4), 6)", want: "1.0"},
		{expr: "haversin(0)", want: "0.0"},
	}

	for _, tc := range tests {
		got, err := evalExpressionWithScope(tc.expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.expr, err)
		}
		if fmt.Sprint(got) != tc.want {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %s", tc.expr, got, tc.want)
		}
	}

	isNaNResult, err := evalExpressionWithScope("isNaN(sqrt(-1))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(isNaN(sqrt(-1))) failed: %v", err)
	}
	if isNaNResult != true {
		t.Fatalf("evalExpressionWithScope(isNaN(sqrt(-1))) = %#v, want true", isNaNResult)
	}

	nullResult, err := evalExpressionWithScope("floor(null)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(floor(null)) failed: %v", err)
	}
	if nullResult != nil {
		t.Fatalf("evalExpressionWithScope(floor(null)) = %#v, want nil", nullResult)
	}

	_, err = evalExpressionWithScope("round(1.2, 0, 'UNKNOWN')", row, nil)
	if err == nil {
		t.Fatalf("evalExpressionWithScope(round(..., UNKNOWN)) expected error")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("round(..., UNKNOWN) error kind = %v, want invalid input", err)
	}
}

func TestEvalExpressionWithScopePredicateScalarBuiltInsThirdTranche(t *testing.T) {
	row := Row{
		"n":  &graph.Vertex{ID: "42", Labels: []string{"Person"}, Properties: graph.PropertyMap{"name": []byte("neo")}},
		"m":  &graph.Vertex{ID: "u1"},
		"r":  &graph.Edge{ID: "7", Type: "KNOWS", SrcID: "42", DstID: "43"},
		"p":  cypherPathValue{Left: &graph.Vertex{ID: "42"}, Edge: &graph.Edge{ID: "7", SrcID: "42", DstID: "43"}, Right: &graph.Vertex{ID: "43"}},
		"mn": map[string]any{"id": "99", "labels": []any{"X"}},
		"mr": map[string]any{"id": "11", "type": "T", "src": "1", "dst": "2"},
	}

	tests := []struct {
		expr string
		want any
	}{
		{expr: "exists(n.id)", want: true},
		{expr: "exists(null)", want: false},
		{expr: "elementId(n)", want: "42"},
		{expr: "elementId(r)", want: "7"},
		{expr: "elementId(mn)", want: "99"},
		{expr: "id(n)", want: int64(42)},
		{expr: "id(r)", want: int64(7)},
		{expr: "id(mn)", want: int64(99)},
		{expr: "id(m)", want: nil},
		{expr: "valueType(null)", want: "NULL"},
		{expr: "valueType(true)", want: "BOOLEAN"},
		{expr: "valueType(1)", want: "INTEGER"},
		{expr: "valueType(1.5)", want: "FLOAT"},
		{expr: "valueType('x')", want: "STRING"},
		{expr: "valueType([1,2])", want: "LIST"},
		{expr: "valueType({a:1})", want: "MAP"},
		{expr: "valueType(n)", want: "NODE"},
		{expr: "valueType(r)", want: "RELATIONSHIP"},
		{expr: "valueType(p)", want: "PATH"},
	}

	for _, tc := range tests {
		got, err := evalExpressionWithScope(tc.expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.expr, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %#v", tc.expr, got, tc.want)
		}
	}

	uuidValue, err := evalExpressionWithScope("randomUUID()", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(randomUUID()) failed: %v", err)
	}
	uuidStr, ok := uuidValue.(string)
	if !ok {
		t.Fatalf("randomUUID() returned %T, want string", uuidValue)
	}
	if len(uuidStr) != 36 || strings.Count(uuidStr, "-") != 4 {
		t.Fatalf("randomUUID() returned invalid format: %q", uuidStr)
	}

	tsValue, err := evalExpressionWithScope("timestamp()", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(timestamp()) failed: %v", err)
	}
	timestampInt, ok := tsValue.(int64)
	if !ok {
		t.Fatalf("timestamp() returned %T, want int64", tsValue)
	}
	if timestampInt <= 0 {
		t.Fatalf("timestamp() returned non-positive value: %d", timestampInt)
	}
}

func TestEvalExpressionWithScopeExpandedBuiltIns(t *testing.T) {
	row := Row{}

	tests := []struct {
		expr string
		want any
	}{
		{expr: "reduce(total = 0, n IN [1,2,3] | total + n)", want: 6},
		{expr: "toBooleanList(['true','false','x',null])", want: []any{true, false, nil, nil}},
		{expr: "toStringList([1,true,{a:1}])", want: []any{"1", "true", nil}},
		{expr: "btrim('__name__', '_')", want: "name"},
		{expr: "normalize('e\u0301', 'NFC')", want: "é"},
	}

	for _, tc := range tests {
		got, err := evalExpressionWithScope(tc.expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.expr, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %#v", tc.expr, got, tc.want)
		}
	}

	temporalAliases := []struct {
		aliasExpr string
		baseExpr  string
	}{
		{aliasExpr: "zoned_datetime('2024-01-01T00:00:00Z')", baseExpr: "datetime('2024-01-01T00:00:00Z')"},
		{aliasExpr: "local_datetime('2024-01-01T12:34:56')", baseExpr: "localdatetime('2024-01-01T12:34:56')"},
		{aliasExpr: "local_time('12:34:56')", baseExpr: "localtime('12:34:56')"},
		{aliasExpr: "zoned_time('12:34:56Z')", baseExpr: "time('12:34:56Z')"},
	}

	for _, tc := range temporalAliases {
		got, err := evalExpressionWithScope(tc.aliasExpr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.aliasExpr, err)
		}
		want, err := evalExpressionWithScope(tc.baseExpr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", tc.baseExpr, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %#v", tc.aliasExpr, got, want)
		}
	}

	durationAlias, err := evalExpressionWithScope("duration_between(datetime('2024-01-01T00:00:00Z'), datetime('2024-01-02T00:00:00Z'))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(duration_between(...)) failed: %v", err)
	}
	durationBase, err := evalExpressionWithScope("duration.between(datetime('2024-01-01T00:00:00Z'), datetime('2024-01-02T00:00:00Z'))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(duration.between(...)) failed: %v", err)
	}
	if !reflect.DeepEqual(durationAlias, durationBase) {
		t.Fatalf("duration_between(...) = %#v, want %#v", durationAlias, durationBase)
	}
}

func TestEvalExpressionWithScopeSpatialPointBuiltIns(t *testing.T) {
	row := Row{}

	value, err := evalExpressionWithScope("point({x: 1, y: 2})", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(point({x:1,y:2})) failed: %v", err)
	}
	pointValue, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("point(...) returned %T, want map[string]any", value)
	}
	if got := fmt.Sprint(pointValue["srid"]); got != "7203" {
		t.Fatalf("unexpected point srid: %#v", pointValue["srid"])
	}
	if got := fmt.Sprint(pointValue["x"]); got != "1.0" {
		t.Fatalf("unexpected point x: %#v", pointValue["x"])
	}
	if got := fmt.Sprint(pointValue["y"]); got != "2.0" {
		t.Fatalf("unexpected point y: %#v", pointValue["y"])
	}

	valueType, err := evalExpressionWithScope("valueType(point({longitude: 12.3, latitude: 45.6}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(valueType(point(...))) failed: %v", err)
	}
	if valueType != "POINT" {
		t.Fatalf("unexpected valueType(point(...)): %#v", valueType)
	}

	stringValue, err := evalExpressionWithScope("toString(point({longitude: 12.3, latitude: 45.6}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(toString(point(...))) failed: %v", err)
	}
	if stringValue != "point({srid: 4326, x: 12.3, y: 45.6})" {
		t.Fatalf("unexpected point string: %#v", stringValue)
	}

	distanceValue, err := evalExpressionWithScope("distance(point({x: 0, y: 0}), point({x: 3, y: 4}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(distance(...)) failed: %v", err)
	}
	if fmt.Sprint(distanceValue) != "5.0" {
		t.Fatalf("unexpected cartesian distance: %#v", distanceValue)
	}

	geoDistance, err := evalExpressionWithScope("distance(point({longitude: 0, latitude: 0}), point({longitude: 0, latitude: 1}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(distance(geographic...)) failed: %v", err)
	}
	geoNumeric, ok := numericValue(geoDistance)
	if !ok {
		t.Fatalf("unexpected geographic distance type: %#v", geoDistance)
	}
	if geoNumeric < 111000 || geoNumeric > 112500 {
		t.Fatalf("unexpected geographic distance magnitude: %#v", geoDistance)
	}
}

func TestEvalExpressionWithScopeSpatialPointValidation(t *testing.T) {
	row := Row{}
	tests := []string{
		"point({longitude: 12.3, latitude: 45.6, crs: 'cartesian'})",
		"point({longitude: 12.3, latitude: 45.6, srid: 4979})",
		"point({longitude: 0, latitude: 91})",
		"point({x: 'NaN', y: 1})",
		"distance(point({x: 0, y: 0, srid: 7203}), point({x: 0, y: 0, z: 1, srid: 9157}))",
		"distance(point({longitude: 0, latitude: 0}), point({longitude: 0, latitude: 0, height: 1}))",
	}

	for _, expr := range tests {
		_, err := evalExpressionWithScope(expr, row, nil)
		if err == nil {
			t.Fatalf("evalExpressionWithScope(%q) expected error", expr)
		}
		if !graph.IsKind(err, graph.ErrKindInvalidInput) {
			t.Fatalf("evalExpressionWithScope(%q) error kind = %v, want invalid input", expr, err)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "invalidargumentvalue") {
			t.Fatalf("evalExpressionWithScope(%q) error = %v, want InvalidArgumentValue", expr, err)
		}
	}
}

func TestEvalExpressionWithScopeSpatialPointAccessorsAndNamespaceFunctions(t *testing.T) {
	row := Row{}

	for expr, want := range map[string]string{
		"point({x: 2.3, y: 4.5}).crs":                                "cartesian",
		"point({x: 2.3, y: 4.5}).srid":                               "7203",
		"point({longitude: 4, latitude: 3, height: 4321}).longitude": "4.0",
		"point({longitude: 4, latitude: 3, height: 4321}).latitude":  "3.0",
		"point({longitude: 4, latitude: 3, height: 4321}).height":    "4321.0",
		"point({longitude: 4, latitude: 3, height: 4321}).x":         "4.0",
		"point({longitude: 4, latitude: 3, height: 4321}).y":         "3.0",
		"point({longitude: 4, latitude: 3, height: 4321}).z":         "4321.0",
		"toString(point({longitude: 12.3, latitude: 45.6}))":         "point({srid: 4326, x: 12.3, y: 45.6})",
		"toString(point({x: 2.3, y: 4.5, crs: 'WGS-84'}))":           "point({srid: 4326, x: 2.3, y: 4.5})",
		"toString(point({longitude: 181, latitude: 0}))":             "point({srid: 4326, x: -179.0, y: 0.0})",
	} {
		got, err := evalExpressionWithScope(expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", expr, err)
		}
		if fmt.Sprint(got) != want {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %s", expr, got, want)
		}
	}

	within, err := evalExpressionWithScope("point.withinBBox(point({x: 5, y: 5}), point({x: 0, y: 0}), point({x: 10, y: 10}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(point.withinBBox(...)) failed: %v", err)
	}
	if within != true {
		t.Fatalf("unexpected point.withinBBox result: %#v", within)
	}

	datelineWithin, err := evalExpressionWithScope("point.withinBBox(point({longitude: 180, latitude: 55.66}), point({longitude: 179, latitude: 55.66}), point({longitude: -179, latitude: 55.70}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(point.withinBBox(dateline...)) failed: %v", err)
	}
	if datelineWithin != true {
		t.Fatalf("unexpected dateline point.withinBBox result: %#v", datelineWithin)
	}

	distanceAlias, err := evalExpressionWithScope("point.distance(point({x: 0, y: 0}), point({x: 3, y: 4}))", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(point.distance(...)) failed: %v", err)
	}
	if fmt.Sprint(distanceAlias) != "5.0" {
		t.Fatalf("unexpected point.distance result: %#v", distanceAlias)
	}
}

func TestEvalExpressionWithScopeVectorBuiltIns(t *testing.T) {
	row := Row{}

	for expr, want := range map[string]string{
		"valueType(vector([1.0, 2.0], 2, FLOAT32))":                                              "VECTOR",
		"toString(vector([1.0, 2.0], 2, FLOAT32))":                                               "vector([1.0, 2.0], 2, FLOAT32)",
		"vector_dimension_count(vector([1.0, 2.0, 3.0], 3, FLOAT32))":                            "3",
		"vector_distance([1.0,2.0], [4.0,6.0], EUCLIDEAN)":                                       "5.0",
		"vector_distance([1.0,2.0], [4.0,6.0], EUCLIDEAN_SQUARED)":                               "25.0",
		"vector_distance([1.0,2.0], [4.0,6.0], MANHATTAN)":                                       "7.0",
		"vector_distance([1.0,2.0], [4.0,6.0], HAMMING)":                                         "2.0",
		"vector_distance([1.0,2.0], [4.0,6.0], DOT)":                                             "-16.0",
		"vector_distance([1.0,0.0], [0.0,1.0], COSINE)":                                          "1.0",
		"vector_norm([3.0,4.0], EUCLIDEAN)":                                                      "5.0",
		"vector_norm([3.0,-4.0], MANHATTAN)":                                                     "7.0",
		"vector.similarity.euclidean([1.0,0.0], [1.0,0.0])":                                      "1.0",
		"vector.similarity.euclidean([1.0,0.0], [0.0,1.0])":                                      "0.3333333333333333",
		"vector.similarity.cosine([1.0,0.0], [0.0,1.0])":                                         "0.5",
		"vector.similarity.cosine(vector([1.0,0.0], 2, FLOAT32), vector([1.0,0.0], 2, FLOAT32))": "1.0",
	} {
		got, err := evalExpressionWithScope(expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", expr, err)
		}
		if fmt.Sprint(got) != want {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %s", expr, got, want)
		}
	}

	nullValue, err := evalExpressionWithScope("vector.similarity.cosine(null, [1.0,0.0])", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(vector.similarity.cosine(null,...)) failed: %v", err)
	}
	if nullValue != nil {
		t.Fatalf("expected null for null vector similarity input, got %#v", nullValue)
	}

	nullDistance, err := evalExpressionWithScope("vector_distance(null, [1.0,0.0], EUCLIDEAN)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(vector_distance(null,...)) failed: %v", err)
	}
	if nullDistance != nil {
		t.Fatalf("expected null for null vector distance input, got %#v", nullDistance)
	}

	nullNorm, err := evalExpressionWithScope("vector_norm(null, EUCLIDEAN)", row, nil)
	if err != nil {
		t.Fatalf("evalExpressionWithScope(vector_norm(null,...)) failed: %v", err)
	}
	if nullNorm != nil {
		t.Fatalf("expected null for null vector norm input, got %#v", nullNorm)
	}

	invalidExprs := []string{
		"vector([1.0, null], 2, FLOAT32)",
		"vector([1.0, 2.0], 3, FLOAT32)",
		"vector.similarity.cosine([1.0,0.0], [1.0])",
		"vector.similarity.cosine([0.0,0.0], [1.0,0.0])",
		"vector_distance([1.0,0.0], [1.0], EUCLIDEAN)",
		"vector_distance([1.0,0.0], [0.0,1.0], BOGUS)",
		"vector_distance([0.0,0.0], [1.0,0.0], COSINE)",
		"vector_norm([1.0,2.0], DOT)",
	}
	for _, expr := range invalidExprs {
		_, err := evalExpressionWithScope(expr, row, nil)
		if err == nil {
			t.Fatalf("evalExpressionWithScope(%q) expected error", expr)
		}
		if !graph.IsKind(err, graph.ErrKindInvalidInput) {
			t.Fatalf("evalExpressionWithScope(%q) error kind = %v, want invalid input", expr, err)
		}
	}
}

func TestEvalExpressionWithScopeVectorAliasCanonicalization(t *testing.T) {
	row := Row{}

	canonicalCases := map[string]string{
		"toString(vector([1.0, 2.0], 2, INTEGER))": "vector([1.0, 2.0], 2, INTEGER64)",
		"toString(vector([1.0, 2.0], 2, FLOAT))":   "vector([1.0, 2.0], 2, FLOAT64)",
	}
	for expr, want := range canonicalCases {
		got, err := evalExpressionWithScope(expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", expr, err)
		}
		if got != want {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %q", expr, got, want)
		}
	}

	metricAliases := map[string]string{
		"vector_distance([1.0, 2.0], [4.0, 6.0], 'euclidean')": "5.0",
		"vector_norm([3.0, 4.0], 'manhattan')":                 "7.0",
	}
	for expr, want := range metricAliases {
		got, err := evalExpressionWithScope(expr, row, nil)
		if err != nil {
			t.Fatalf("evalExpressionWithScope(%q) failed: %v", expr, err)
		}
		if fmt.Sprint(got) != want {
			t.Fatalf("evalExpressionWithScope(%q) = %#v, want %s", expr, got, want)
		}
	}
}

func TestExecuteAggregateAliasBuiltIns(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND [1,2,3] AS n RETURN stDev(n) AS sample, stDevP(n) AS population, stdev_samp(n) AS sample_alias, stdev_pop(n) AS population_alias, collect_list(n) AS collected, percentile_cont(n, 0.5) AS percentile_continuous, percentile_disc(n, 0.5) AS percentile_discrete")
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
	row := res.Rows[0]
	if fmt.Sprint(row["sample"]) != "1.0" {
		t.Fatalf("unexpected stDev(n): %#v", row["sample"])
	}
	if fmt.Sprint(row["population"]) != "0.816496580927726" {
		t.Fatalf("unexpected stDevP(n): %#v", row["population"])
	}
	if !reflect.DeepEqual(row["sample_alias"], row["sample"]) {
		t.Fatalf("unexpected stdev_samp alias: %#v vs %#v", row["sample_alias"], row["sample"])
	}
	if !reflect.DeepEqual(row["population_alias"], row["population"]) {
		t.Fatalf("unexpected stdev_pop alias: %#v vs %#v", row["population_alias"], row["population"])
	}
	if !reflect.DeepEqual(row["collected"], []any{1, 2, 3}) {
		t.Fatalf("unexpected collect_list(n): %#v", row["collected"])
	}
	if fmt.Sprint(row["percentile_continuous"]) != "2.0" {
		t.Fatalf("unexpected percentile_cont(n, 0.5): %#v", row["percentile_continuous"])
	}
	if fmt.Sprint(row["percentile_discrete"]) != "2.0" {
		t.Fatalf("unexpected percentile_disc(n, 0.5): %#v", row["percentile_discrete"])
	}
}

func TestExecuteSumCaseWhenAggregation(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND [0,1,2,3] AS n RETURN sum(CASE WHEN n >= 2 THEN 1 ELSE 0 END) AS highCount")
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
	if got := fmt.Sprint(res.Rows[0]["highCount"]); got != "2" && got != "2.0" {
		t.Fatalf("sum(CASE WHEN ...) = %q, want 2", got)
	}
}

func TestExecuteCompositeAggregateCaseExpression(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("UNWIND [0,1,2,3] AS n RETURN coalesce(sum(CASE WHEN n >= 2 THEN 1 ELSE 0 END), 0) * 1.0 / count(*) AS ratio")
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
	if got := fmt.Sprint(res.Rows[0]["ratio"]); got != "0.5" {
		t.Fatalf("coalesce(sum(CASE WHEN ...),0)/count(*) = %q, want 0.5", got)
	}
}

func TestExecuteMatchWherePointWithinBBox(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	seed, err := parser.ParseStatement("CREATE (:Place {name:'inside', longitude: 5.0, latitude: 5.0}), (:Place {name:'outside', longitude: 20.0, latitude: 5.0}), (:Place {name:'dateEast', longitude: 179.6, latitude: 55.67}), (:Place {name:'dateWest', longitude: -179.6, latitude: 55.67}), (:Place {name:'dateOutside', longitude: 170.0, latitude: 55.67}), (:Place {name:'missingLatitude', longitude: 6.0})")
	if err != nil {
		t.Fatalf("seed parse failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("seed execute failed: %v", err)
	}

	regularBBox, err := parser.ParseStatement("MATCH (p:Place) WHERE point.withinBBox(point({longitude: p.longitude, latitude: p.latitude}), point({longitude: 0.0, latitude: 0.0}), point({longitude: 10.0, latitude: 10.0})) RETURN p.name AS name ORDER BY name ASC")
	if err != nil {
		t.Fatalf("regular bbox parse failed: %v", err)
	}
	regularRes, err := exec.ExecuteStatement(ctx, regularBBox, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("regular bbox execute failed: %v", err)
	}
	if len(regularRes.Rows) != 1 {
		t.Fatalf("regular bbox expected 1 row, got %d: %#v", len(regularRes.Rows), regularRes.Rows)
	}
	if got := fmt.Sprint(regularRes.Rows[0]["name"]); got != "inside" {
		t.Fatalf("regular bbox expected inside, got %q", got)
	}

	datelineBBox, err := parser.ParseStatement("MATCH (p:Place) WHERE point.withinBBox(point({longitude: p.longitude, latitude: p.latitude}), point({longitude: 179.0, latitude: 55.66}), point({longitude: -179.0, latitude: 55.70})) RETURN p.name AS name ORDER BY name ASC")
	if err != nil {
		t.Fatalf("dateline bbox parse failed: %v", err)
	}
	datelineRes, err := exec.ExecuteStatement(ctx, datelineBBox, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("dateline bbox execute failed: %v", err)
	}
	if len(datelineRes.Rows) != 2 {
		t.Fatalf("dateline bbox expected 2 rows, got %d: %#v", len(datelineRes.Rows), datelineRes.Rows)
	}
	if got := fmt.Sprint(datelineRes.Rows[0]["name"]); got != "dateEast" {
		t.Fatalf("dateline bbox first row mismatch: %q", got)
	}
	if got := fmt.Sprint(datelineRes.Rows[1]["name"]); got != "dateWest" {
		t.Fatalf("dateline bbox second row mismatch: %q", got)
	}
}

func TestExecuteMatchWhereVectorSimilarity(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("UNWIND [[1.0,0.0],[0.9,0.1],[0.0,1.0]] AS embedding WITH embedding, vector.similarity.euclidean([1.0,0.0], embedding) AS score WHERE score >= 0.5 RETURN embedding AS embedding, score ORDER BY score DESC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["score"]); got != "1.0" {
		t.Fatalf("first score mismatch: %q", got)
	}
	if got := fmt.Sprint(res.Rows[1]["score"]); got != "0.9803921568627451" {
		t.Fatalf("second score mismatch: %q", got)
	}
}

func TestExecuteMatchWhereVectorDistance(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("UNWIND [[1.0,0.0],[0.9,0.1],[0.0,1.0]] AS embedding WITH embedding, vector_distance([1.0,0.0], embedding, EUCLIDEAN) AS dist WHERE dist <= 0.15 RETURN embedding AS embedding, dist ORDER BY dist ASC")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["dist"]); got != "0.0" {
		t.Fatalf("first distance mismatch: %q", got)
	}
	if got := fmt.Sprint(res.Rows[1]["dist"]); got != "0.1414213562373095" {
		t.Fatalf("second distance mismatch: %q", got)
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

func (r *executorMetricsRecorder) ObserveDeleteCounter(_ string, _ int64) {}
