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

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/indexschema"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
	pebblestore "github.com/spaceqraft/vitaledge/internal/graph/store/pebble"
	"github.com/spaceqraft/vitaledge/internal/graph/store/typedvalue"
)

func TestExecuteMatchReturnIDs(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (src)-[:MEMBER_OF]->(dst) WHERE id(src) = $srcID RETURN id(dst) AS dstID")
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

	stmt, err := parser.ParseStatement("MATCH (src)-[:MEMBER_OF]->(dst) WHERE id(src) = $srcID RETURN id(dst) AS dstID LIMIT $max")
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
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	if _, ok := explainPayload["logicalPlan"].(map[string]any); !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
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

func TestExecuteExplainDryRunWriteQueryDoesNotMutatePipelinePayload(t *testing.T) {
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
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
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
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")

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
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}
	if rootNodeID, _ := logicalPlan["rootNodeId"].(string); rootNodeID == "" {
		t.Fatalf("expected non-empty rootNodeId")
	}
	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	if _, exists := influencers["statsSnapshot"]; exists {
		t.Fatalf("expected statsSnapshot to be omitted from influencers, got %#v", influencers["statsSnapshot"])
	}
	vertexCounts, ok := influencers["vertexCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected vertexCounts []map[string]any, got %T", influencers["vertexCounts"])
	}
	if len(vertexCounts) == 0 {
		t.Fatalf("expected non-empty vertexCounts")
	}
	vertexCountAssessment, ok := vertexCounts[0]["assessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected vertexCount assessment map, got %T", vertexCounts[0]["assessment"])
	}
	if quality, _ := vertexCountAssessment["quality"].(string); quality != "exact" {
		t.Fatalf("expected vertexCount assessment quality exact, got %#v", vertexCountAssessment["quality"])
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 0 {
		t.Fatalf("expected scoped edgeCounts to be empty for vertex-only query, got %#v", edgeCounts)
	}
	totals, ok := influencers["totals"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers totals map, got %T", influencers["totals"])
	}
	if _, ok := totals["vertexes"].(int); !ok {
		t.Fatalf("expected totals.vertexes int, got %#v", totals["vertexes"])
	}
	if _, ok := totals["edges"].(int); !ok {
		t.Fatalf("expected totals.edges int, got %#v", totals["edges"])
	}
	predicateSignals, ok := influencers["predicateSignals"].([]map[string]any)
	if !ok {
		t.Fatalf("expected predicateSignals []map[string]any, got %T", influencers["predicateSignals"])
	}
	if len(predicateSignals) == 0 {
		t.Fatalf("expected non-empty predicateSignals")
	}
	predicateAssessment, ok := predicateSignals[0]["assessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected predicateSignal assessment map, got %T", predicateSignals[0]["assessment"])
	}
	if quality, _ := predicateAssessment["quality"].(string); quality != "exact" {
		t.Fatalf("expected predicateSignal assessment quality exact, got %#v", predicateAssessment["quality"])
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
	if recommendation, _ := indexDecisions[0]["recommendation"].(string); recommendation != "keep-typed-index" {
		t.Fatalf("expected recommendation keep-typed-index for selected typed index, got %#v", indexDecisions[0]["recommendation"])
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
	assessment, ok := indexDecisions[0]["assessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected normalized assessment map, got %T", indexDecisions[0]["assessment"])
	}
	if selected, _ := assessment["selected"].(bool); !selected {
		t.Fatalf("expected assessment.selected=true, got %#v", assessment["selected"])
	}
	if recommendation, _ := assessment["recommendation"].(string); recommendation != "keep-typed-index" {
		t.Fatalf("expected assessment recommendation keep-typed-index, got %#v", assessment["recommendation"])
	}
	if quality, _ := assessment["quality"].(string); quality != "exact" {
		t.Fatalf("expected assessment quality exact, got %#v", assessment["quality"])
	}
	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok {
		t.Fatalf("expected cardinality []map[string]any, got %T", explainPayload["cardinality"])
	}
	if len(cardinality) == 0 {
		t.Fatalf("expected non-empty cardinality entries")
	}
	if rowsOut, _ := cardinality[0]["rowsOut"].(int); rowsOut != 1 {
		t.Fatalf("expected first cardinality rowsOut=1, got %#v", cardinality[0]["rowsOut"])
	}
	cardinalityAssessment, ok := cardinality[0]["assessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected cardinality assessment map, got %T", cardinality[0]["assessment"])
	}
	if quality, _ := cardinalityAssessment["quality"].(string); quality != "exact" {
		t.Fatalf("expected cardinality assessment quality exact, got %#v", cardinalityAssessment["quality"])
	}
	if rowsOut, _ := cardinalityAssessment["rowsOut"].(int); rowsOut != 1 {
		t.Fatalf("expected cardinality assessment rowsOut=1, got %#v", cardinalityAssessment["rowsOut"])
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
	costAssessment, ok := costEstimate["assessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected costEstimate assessment map, got %T", costEstimate["assessment"])
	}
	if unit, _ := costAssessment["unit"].(string); unit != "work_units" {
		t.Fatalf("expected costEstimate assessment unit work_units, got %#v", costAssessment["unit"])
	}
	if quality, _ := costAssessment["quality"].(string); quality != "estimate" {
		t.Fatalf("expected costEstimate assessment quality estimate, got %#v", costAssessment["quality"])
	}
	if _, ok := costAssessment["scanRows"].(int); !ok {
		t.Fatalf("expected costEstimate assessment scanRows int, got %#v", costAssessment["scanRows"])
	}
	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	metadata, ok := explainPayload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map, got %T", explainPayload["metadata"])
	}
	if status, _ := metadata["pipelineExplainStatus"].(string); status != "ok" {
		t.Fatalf("expected metadata.pipelineExplainStatus=ok, got %#v", metadata["pipelineExplainStatus"])
	}
	pipelineExplain, _ := metadata["pipelineExplain"].(string)
	if !strings.Contains(pipelineExplain, "LOGICAL root=") || !strings.Contains(pipelineExplain, "PHYSICAL root=") {
		t.Fatalf("expected metadata.pipelineExplain to include logical and physical sections, got %#v", metadata["pipelineExplain"])
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
	if totalVertexes, _ := planStats["totalVertexes"].(int); totalVertexes < 1 {
		t.Fatalf("expected totalVertexes >= 1, got %#v", planStats["totalVertexes"])
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
	warningSummary, ok := runtimeStats["warningSummary"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.warningSummary map, got %T", runtimeStats["warningSummary"])
	}
	if totalWarnings, _ := warningSummary["totalWarnings"].(int); totalWarnings != 0 {
		t.Fatalf("expected warningSummary totalWarnings=0, got %#v", warningSummary)
	}
	byCategory, ok := warningSummary["byCategory"].(map[string]int)
	if !ok {
		t.Fatalf("expected warningSummary byCategory map[string]int, got %T", warningSummary["byCategory"])
	}
	if len(byCategory) != 0 {
		t.Fatalf("expected empty byCategory for no-warning query, got %#v", warningSummary)
	}
	if highestPriorityCode, _ := warningSummary["highestPriorityCode"].(string); highestPriorityCode != "" {
		t.Fatalf("expected empty highestPriorityCode for no-warning query, got %#v", warningSummary)
	}
	diagnosticPosture, ok := runtimeStats["diagnosticPosture"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.diagnosticPosture map, got %T", runtimeStats["diagnosticPosture"])
	}
	if primary, _ := diagnosticPosture["primary"].(string); primary != "healthy" {
		t.Fatalf("expected diagnostic posture primary healthy, got %#v", diagnosticPosture)
	}
	if recommendation, _ := diagnosticPosture["recommendation"].(string); recommendation != "maintain_typed_paths" {
		t.Fatalf("expected diagnostic posture recommendation maintain_typed_paths, got %#v", diagnosticPosture)
	}
	if confidence, _ := diagnosticPosture["confidence"].(string); confidence != "high" {
		t.Fatalf("expected diagnostic posture confidence high, got %#v", diagnosticPosture)
	}
	if score, _ := diagnosticPosture["score"].(int); score != 95 {
		t.Fatalf("expected diagnostic posture score 95, got %#v", diagnosticPosture)
	}
	if scoreBand, _ := diagnosticPosture["scoreBand"].(string); scoreBand != "excellent" {
		t.Fatalf("expected diagnostic posture scoreBand excellent, got %#v", diagnosticPosture)
	}
	if trendHint, _ := diagnosticPosture["trendHint"].(string); trendHint != "stable" {
		t.Fatalf("expected diagnostic posture trendHint stable, got %#v", diagnosticPosture)
	}
	if trendScore, _ := diagnosticPosture["trendScore"].(int); trendScore != 1 {
		t.Fatalf("expected diagnostic posture trendScore 1, got %#v", diagnosticPosture)
	}
	if trendEvidence, _ := diagnosticPosture["trendEvidence"].(map[string]any); !reflect.DeepEqual(trendEvidence, map[string]any{
		"drivers":             []string{"typed_friendly"},
		"totalWarnings":       0,
		"highestPriorityCode": "",
		"highestCategory":     "",
	}) {
		t.Fatalf("expected diagnostic posture trendEvidence for healthy posture, got %#v", diagnosticPosture)
	}
	if trendDriverWeights, _ := diagnosticPosture["trendDriverWeights"].(map[string]int); !reflect.DeepEqual(trendDriverWeights, map[string]int{}) {
		t.Fatalf("expected diagnostic posture trendDriverWeights for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationVersion, _ := diagnosticPosture["scoreComputationVersion"].(string); scoreComputationVersion != "v1" {
		t.Fatalf("expected diagnostic posture scoreComputationVersion v1, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig, explainDiagnosticPostureScoreComputationConfig()) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["categoryWeights"], explainDiagnosticScoreCategoryWeights()) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig categoryWeights for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["trendRules"], map[string]any{
		"stableMinScore":      80,
		"degradingMaxScore":   30,
		"degradingCategories": []string{"planner", "index"},
		"ruleEvaluationOrder": []string{"stable_min_score", "degrading_categories", "degrading_max_score", "default_trend"},
		"reasonByRule": map[string]string{
			"stable_min_score":     "score >= stableMinScore",
			"degrading_categories": "planner/index category warnings present",
			"degrading_max_score":  "score <= degradingMaxScore",
			"default_trend":        "fallback trend when no prior trend rule matched",
		},
		"trendScoreByHint": map[string]int{
			"stable":    1,
			"watch":     0,
			"degrading": -1,
		},
		"trendScoreRules": map[string]string{
			"stable":    "trend_score_stable",
			"watch":     "trend_score_watch",
			"degrading": "trend_score_degrading",
		},
		"defaultTrend": "watch",
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig trendRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["primarySelectionRules"], map[string]any{
		"categoryPriority":       []string{"planner", "index"},
		"primaryEvaluationOrder": []string{"category_priority_planner", "category_priority_index", "operator_fallback_status", "typed_friendly_reset", "default"},
		"categoryToPrimary": map[string]string{
			"planner": "planner_limited",
			"index":   "index_limited",
		},
		"operatorFallbackStatuses": []string{"mixed_domain_risk", "fallback_likely"},
		"typedFriendlyReset": map[string]any{
			"requiresTotalWarnings": 0,
			"requiresOverallStatus": "typed_friendly",
			"primary":               "healthy",
			"signal":                "typed_friendly",
			"recommendation":        "maintain_typed_paths",
		},
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig primarySelectionRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["confidenceRules"], map[string]any{
		"evaluationOrder": []string{"high_healthy_typed_friendly", "low_warning_or_multisignal", "default_medium"},
		"high": map[string]any{
			"requiresPrimary":       "healthy",
			"requiresTotalWarnings": 0,
			"requiresOverallStatus": "typed_friendly",
		},
		"low": map[string]int{
			"minTotalWarnings": 3,
			"orMinSignalCount": 2,
		},
		"default": "medium",
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig confidenceRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["recommendationRules"], map[string]string{
		"healthy":                   "maintain_typed_paths",
		"index_limited":             "improve_index_coverage",
		"planner_limited":           "optimize_scan_and_plan_shapes",
		"operator_fallback_limited": "reduce_operator_fallback_shapes",
		"default":                   "maintain_typed_paths",
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig recommendationRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["recommendationEvaluationOrder"], []string{"primary_planner", "primary_index", "operator_status_fallback", "typed_friendly_reset", "default"}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig recommendationEvaluationOrder for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleTemplates"], map[string]string{
		"healthy":                   "No explain warnings and typed-friendly operator assessment",
		"index_limited":             "Index warnings dominate diagnostics; highestPriorityCode=%s",
		"planner_limited":           "Planner warnings dominate diagnostics; highestPriorityCode=%s",
		"operator_fallback_limited": "Operator assessment indicates %s with fallback-oriented signals",
		"default":                   "Diagnostic posture derived from signals=%s and highestPriorityCode=%s",
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig rationaleTemplates for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleRules"], map[string]string{
		"healthy":                   "primary_healthy",
		"index_limited":             "primary_index_limited",
		"planner_limited":           "primary_planner_limited",
		"operator_fallback_limited": "primary_operator_fallback_limited",
		"default":                   "default",
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig rationaleRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleTemplateInputs"], map[string][]string{
		"healthy":                   {},
		"index_limited":             {"highestPriorityCode"},
		"planner_limited":           {"highestPriorityCode"},
		"operator_fallback_limited": {"overallStatus"},
		"default":                   {"signals", "highestPriorityCode"},
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig rationaleTemplateInputs for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["scoreRules"], map[string]any{
		"baseRuleByPrimary": map[string]string{
			"healthy":                   "base_primary_healthy",
			"operator_fallback_limited": "base_primary_operator_fallback_limited",
			"index_limited":             "base_primary_index_limited",
			"planner_limited":           "base_primary_planner_limited",
			"default":                   "base_primary_default",
		},
		"confidenceAdjustmentRuleByClass": map[string]string{
			"high":    "confidence_adjustment_high",
			"medium":  "confidence_adjustment_default",
			"low":     "confidence_adjustment_low",
			"default": "confidence_adjustment_default",
		},
		"warningVolumePenaltyRule": "warning_penalty_per_warning_linear",
		"categoryPenaltyRule":      "repeated_category_weighted_penalty",
		"clampRules": map[string]string{
			"withinBounds": "within_bounds",
			"min":          "clamped_min",
			"max":          "clamped_max",
		},
		"scoreBandRules": map[string]string{
			"excellent": "score_band_excellent",
			"good":      "score_band_good",
			"fair":      "score_band_fair",
			"poor":      "score_band_poor",
		},
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig scoreRules for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["decisionTraceSchema"], map[string]any{
		"version":    "v1",
		"stageOrder": []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"},
		"stageFields": map[string][]string{
			"primary":        {"stage", "rule", "result", "reason", "inputs"},
			"confidence":     {"stage", "rule", "result", "reason", "inputs"},
			"recommendation": {"stage", "rule", "result", "reason", "inputs"},
			"rationale":      {"stage", "rule", "result", "reason", "inputs"},
			"score":          {"stage", "rule", "result", "reason", "inputs"},
			"trend":          {"stage", "rule", "result", "reason", "inputs"},
		},
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig decisionTraceSchema for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["ruleReasonCatalog"], map[string]map[string]string{
		"primary": {
			"category_priority_planner": "planner category warning took primary precedence",
			"category_priority_index":   "index category warning set primary posture",
			"operator_fallback_status":  "operator assessment indicates fallback risk",
			"typed_friendly_reset":      "typed-friendly assessment with zero warnings reset posture",
			"default":                   "default primary posture applied",
		},
		"confidence": {
			"high_healthy_typed_friendly": "healthy posture with zero warnings and typed-friendly assessment",
			"low_warning_or_multisignal":  "warning volume or multi-signal posture lowered confidence",
			"default_medium":              "default medium confidence classification",
		},
		"recommendation": {
			"primary_planner":          "planner-limited posture selected planner optimization recommendation",
			"primary_index":            "index-limited posture selected index coverage recommendation",
			"operator_status_fallback": "operator fallback status selected fallback-reduction recommendation",
			"typed_friendly_reset":     "typed-friendly reset selected maintenance recommendation",
			"default":                  "default recommendation applied",
		},
		"rationale": {
			"primary_healthy":                   "healthy posture uses fixed healthy rationale",
			"primary_index_limited":             "index-limited posture uses highest-priority-code rationale",
			"primary_planner_limited":           "planner-limited posture uses highest-priority-code rationale",
			"primary_operator_fallback_limited": "operator-fallback posture uses operator-status rationale",
			"default":                           "default rationale template selected",
		},
		"score": {
			"score_band_excellent": "final score mapped to excellent band",
			"score_band_good":      "final score mapped to good band",
			"score_band_fair":      "final score mapped to fair band",
			"score_band_poor":      "final score mapped to poor band",
		},
		"trend": {
			"stable_min_score":     "score met stable threshold",
			"degrading_categories": "planner or index warning categories triggered degrading trend",
			"degrading_max_score":  "score met degrading threshold",
			"default_trend":        "default watch trend applied",
		},
	}) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig ruleReasonCatalog for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); scoreComputationConfig["ruleReasonCatalogVersion"] != "v1" {
		t.Fatalf("expected diagnostic posture scoreComputationConfig ruleReasonCatalogVersion v1 for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreClampRange, _ := diagnosticPosture["scoreClampRange"].(map[string]int); !reflect.DeepEqual(scoreClampRange, map[string]int{"min": 0, "max": 100}) {
		t.Fatalf("expected diagnostic posture scoreClampRange for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreInputs, _ := diagnosticPosture["scoreInputs"].(map[string]any); !reflect.DeepEqual(scoreInputs, map[string]any{
		"totalWarnings":          0,
		"confidenceClass":        "high",
		"repeatedCategoryCounts": map[string]int{},
	}) {
		t.Fatalf("expected diagnostic posture scoreInputs for healthy posture, got %#v", diagnosticPosture)
	}
	if evaluatedPolicy, _ := diagnosticPosture["evaluatedPolicy"].(map[string]any); !reflect.DeepEqual(evaluatedPolicy, map[string]any{
		"decisionTraceVersion": "v1",
		"validationMode":       "strict",
		"contractHash":         explainDiagnosticPostureContractHash(explainDiagnosticPostureScoreComputationConfig()),
		"contractComponents":   explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig()),
		"compatibility": map[string]any{
			"compatibilityVersion":       "v1",
			"contractEpoch":              "contract.v1",
			"baselineContractEpoch":      "contract.v1",
			"contractEpochTransition":    "unchanged",
			"remediationEpoch":           "remediation.v1",
			"baselineRemediationEpoch":   "remediation.v1",
			"remediationEpochTransition": "unchanged",
			"epochTransitionRule":        "epoch_transition_none",
			"epochTransitionReason":      "contract and remediation epochs match baseline",
			"epochEvaluationOrder":       []string{"contract_epoch", "remediation_epoch"},
			"epochReasonCodes": map[string]string{
				"contract_epoch":    "contract_epoch_advanced_from_baseline",
				"remediation_epoch": "remediation_epoch_advanced_from_baseline",
			},
			"epochCompatibility": map[string]bool{
				"contract_epoch":    true,
				"remediation_epoch": true,
			},
			"epochFailedTransitions":       []string{},
			"epochFailedTransitionReasons": map[string]string{},
			"epochTransitionSummary":       "baseline_aligned",
			"epochImpactEvaluationOrder":   []string{"epoch_compatible_if_no_failed_transitions", "epoch_breaking_on_contract_transition", "epoch_breaking_on_remediation_transition", "epoch_unknown_default"},
			"epochImpactByCheck": map[string]string{
				"contract_epoch":    "breaking",
				"remediation_epoch": "breaking",
			},
			"epochImpactClassification": "compatible",
			"epochImpactRule":           "epoch_compatible_if_no_failed_transitions",
			"epochImpactReason":         "no epoch transition drift detected",
			"epochRemediationByCheck": map[string]string{
				"contract_epoch":    "upgrade parser/runtime consumers to a contract.v1-compatible epoch before parsing evaluatedPolicy compatibility metadata",
				"remediation_epoch": "refresh remediation consumers to the remediation.v1 schema before applying compatibility remediation guidance",
			},
			"epochRemediationPriorityOrder":   []string{"contract_epoch", "remediation_epoch"},
			"epochRemediationSummary":         "none_required",
			"epochRemediationActions":         []string{},
			"epochRemediationActionPlan":      []map[string]any{},
			"epochRemediationPlanHash":        explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochStateID":                    explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;"),
			"baselineEpochStateID":            explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;"),
			"epochDriftStatus":                "unchanged",
			"epochDriftFields":                []string{},
			"epochCompatibilityFingerprint":   explainDiagnosticPostureHashString("epochStateID=" + explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;") + ";" + "baselineEpochStateID=" + explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;") + ";" + "driftStatus=unchanged;" + "driftFields=;" + "impact=compatible;" + "remediationPlanHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"epochConsistencyEvaluationOrder": []string{"transition_summary_matches_failed_transitions", "drift_status_matches_state_ids", "impact_matches_failed_transitions", "remediation_summary_matches_actions"},
			"epochConsistencyReasonCodes": map[string]string{
				"transition_summary_matches_failed_transitions": "epoch_transition_summary_mismatch",
				"drift_status_matches_state_ids":                "epoch_drift_status_mismatch",
				"impact_matches_failed_transitions":             "epoch_impact_mismatch",
				"remediation_summary_matches_actions":           "epoch_remediation_summary_mismatch",
			},
			"epochConsistencyChecks": map[string]bool{
				"transition_summary_matches_failed_transitions": true,
				"drift_status_matches_state_ids":                true,
				"impact_matches_failed_transitions":             true,
				"remediation_summary_matches_actions":           true,
			},
			"epochConsistencyFailedChecks":  []string{},
			"epochConsistencyFailedReasons": map[string]string{},
			"epochConsistencySummary":       "consistent",
			"epochConsistencyFingerprint":   explainDiagnosticPostureHashString("summary=consistent;failedChecks=;driftStatus=unchanged;impact=compatible;remediationSummary=none_required;"),
			"epochRuleCatalogVersion":       "v1",
			"epochRuleCatalog": map[string]map[string]string{
				"transition": {
					"epoch_transition_none":     "contract and remediation epochs match baseline",
					"epoch_transition_detected": "contract or remediation epoch advanced from baseline",
				},
				"impact": {
					"epoch_compatible_if_no_failed_transitions": "no epoch transition drift detected",
					"epoch_breaking_on_contract_transition":     "contract epoch advanced relative to baseline",
					"epoch_breaking_on_remediation_transition":  "remediation epoch advanced relative to baseline",
				},
				"consistency": {
					"consistent":   "epoch consistency checks passed",
					"inconsistent": "epoch consistency checks failed",
				},
			},
			"epochMatchedRuleLookup": map[string]map[string]any{
				"transition": {
					"rule":           "epoch_transition_none",
					"expectedReason": "contract and remediation epochs match baseline",
					"actualReason":   "contract and remediation epochs match baseline",
					"matchesCatalog": true,
				},
				"impact": {
					"rule":           "epoch_compatible_if_no_failed_transitions",
					"expectedReason": "no epoch transition drift detected",
					"actualReason":   "no epoch transition drift detected",
					"matchesCatalog": true,
				},
				"consistency": {
					"rule":           "consistent",
					"expectedReason": "epoch consistency checks passed",
					"actualReason":   "epoch consistency checks passed",
					"matchesCatalog": true,
				},
			},
			"epochRuleCatalogConsistent": true,
			"epochRuleCatalogMismatches": []string{},
			"epochContractVersion":       "v1",
			"epochContractHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"ruleCatalogHash=" + explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;") + ";" +
					"evaluationOrderHash=" + explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;") + ";" +
					"consistencyCheckSchemaHash=" + explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;") + ";",
			),
			"epochContractComponents": map[string]any{
				"version":                    "v1",
				"ruleCatalogHash":            explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;"),
				"evaluationOrderHash":        explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;"),
				"consistencyCheckSchemaHash": explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;"),
				"overallHash": explainDiagnosticPostureHashString(
					"version=v1;" +
						"ruleCatalogHash=" + explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;") + ";" +
						"evaluationOrderHash=" + explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;") + ";" +
						"consistencyCheckSchemaHash=" + explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;") + ";",
				),
			},
			"epochContractCompatibility": map[string]bool{
				"ruleCatalogPresent":       true,
				"evaluationOrderPresent":   true,
				"consistencySchemaPresent": true,
				"overallHashPresent":       true,
			},
			"epochContractCheckEvaluationOrder": []string{"rule_catalog_present", "evaluation_order_present", "consistency_schema_present", "overall_hash_present"},
			"epochContractCheckReasonCodes": map[string]string{
				"rule_catalog_present":       "missing_epoch_rule_catalog_hash",
				"evaluation_order_present":   "missing_epoch_evaluation_order_hash",
				"consistency_schema_present": "missing_epoch_consistency_schema_hash",
				"overall_hash_present":       "missing_epoch_contract_overall_hash",
			},
			"epochContractCheckStatus": map[string]bool{
				"rule_catalog_present":       true,
				"evaluation_order_present":   true,
				"consistency_schema_present": true,
				"overall_hash_present":       true,
			},
			"epochContractFailedChecks":       []string{},
			"epochContractFailedCheckReasons": map[string]string{},
			"epochContractCheckSummary":       "all_checks_passed",
			"epochContractCheckFingerprint": explainDiagnosticPostureHashString(
				"summary=all_checks_passed;" +
					"failedChecks=;" +
					"overallHash=" + explainDiagnosticPostureHashString(
					"version=v1;"+
						"ruleCatalogHash="+explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;")+";"+
						"evaluationOrderHash="+explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;")+";"+
						"consistencyCheckSchemaHash="+explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;")+";",
				) + ";",
			),
			"epochContractFullyCompatible":               true,
			"epochContractImpactEvaluationOrder":         []string{"compatible_if_all_contract_checks_pass", "breaking_if_contract_components_missing", "unknown_default"},
			"epochContractImpactByCheck":                 map[string]string{"rule_catalog_present": "breaking", "evaluation_order_present": "breaking", "consistency_schema_present": "breaking", "overall_hash_present": "breaking"},
			"epochContractImpactClassification":          "compatible",
			"epochContractImpactRule":                    "compatible_if_all_contract_checks_pass",
			"epochContractImpactReason":                  "all epoch-contract checks passed",
			"epochContractRemediationByCheck":            map[string]string{"rule_catalog_present": "regenerate epoch rule catalog metadata and persist a non-empty epoch rule catalog hash", "evaluation_order_present": "restore epoch evaluation-order metadata and persist a non-empty evaluation-order hash", "consistency_schema_present": "regenerate epoch consistency-check schema metadata and persist a non-empty schema hash", "overall_hash_present": "recompute epoch contract overall hash after restoring contract component hashes"},
			"epochContractRemediationSeverityByCheck":    map[string]string{"rule_catalog_present": "high", "evaluation_order_present": "high", "consistency_schema_present": "high", "overall_hash_present": "high"},
			"epochContractRemediationRequirementByCheck": map[string]string{"rule_catalog_present": "required", "evaluation_order_present": "required", "consistency_schema_present": "required", "overall_hash_present": "required"},
			"epochContractRemediationPriorityOrder":      []string{"rule_catalog_present", "evaluation_order_present", "consistency_schema_present", "overall_hash_present"},
			"epochContractRemediationSummary":            "none_required",
			"epochContractRemediationUrgency":            "none",
			"epochContractRemediationBundleID":           explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"epochContractRemediationActions":            []string{},
			"epochContractRemediationPlanHash":           explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractRemediationFingerprint":        explainDiagnosticPostureHashString("impact=compatible;summary=none_required;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"baselineEpochContractCheckSummary":          "all_checks_passed",
			"baselineEpochContractImpactClassification":  "compatible",
			"baselineEpochContractRemediationSummary":    "none_required",
			"baselineEpochContractRemediationUrgency":    "none",
			"baselineEpochContractRemediationBundleID":   explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"baselineEpochContractRemediationPlanHash":   explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractRemediationBundleDriftStatus":  "unchanged",
			"epochContractRemediationPlanDriftStatus":    "unchanged",
			"epochContractRemediationDriftSummary":       "baseline_aligned",
			"epochContractRemediationDriftRule":          "baseline_epoch_contract_bundle_and_plan_match",
			"epochContractRemediationDriftReason":        "current epoch-contract remediation bundle and plan hash match baseline compatible state",
			"epochContractRemediationDriftFields":        []string{},
			"epochContractRemediationDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" +
					"currentBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" +
					"baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"bundleDrift=unchanged;" +
					"planDrift=unchanged;",
			),
			"epochContractCheckDriftStatus": "unchanged",
			"epochContractCheckDriftRule":   "baseline_epoch_contract_check_state_matches",
			"epochContractCheckDriftReason": "epoch-contract check, impact, and remediation summaries match baseline",
			"epochContractCheckDriftFields": []string{},
			"epochContractCheckDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineCheckSummary=all_checks_passed;" +
					"currentCheckSummary=all_checks_passed;" +
					"baselineImpact=compatible;" +
					"currentImpact=compatible;" +
					"baselineRemediationSummary=none_required;" +
					"currentRemediationSummary=none_required;" +
					"driftStatus=unchanged;" +
					"driftFields=;",
			),
			"epochContractGovernanceEvaluationOrder": []string{"stable_if_compatible_and_baseline_aligned", "degraded_if_incompatible", "degraded_if_check_or_remediation_drift", "unknown_default"},
			"epochContractGovernanceChecks": map[string]bool{
				"contract_fully_compatible":    true,
				"check_state_baseline_aligned": true,
				"remediation_baseline_aligned": true,
			},
			"epochContractGovernanceFailedChecks":  []string{},
			"epochContractGovernanceFailedReasons": map[string]string{},
			"epochContractGovernanceState":         "stable",
			"epochContractGovernanceRule":          "stable_if_compatible_and_baseline_aligned",
			"epochContractGovernanceReason":        "epoch-contract checks are compatible and both check/remediation baselines are aligned",
			"epochContractGovernanceFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_compatible_and_baseline_aligned;" +
					"failedChecks=;" +
					"checkDrift=unchanged;" +
					"remediationDrift=baseline_aligned;",
			),
			"epochContractGovernanceRemediationByCheck": map[string]string{
				"contract_fully_compatible":    "restore required epoch-contract component hashes and recompute epoch contract compatibility artifacts",
				"check_state_baseline_aligned": "investigate epoch-contract check summary drift and realign check/impact/remediation summaries to baseline",
				"remediation_baseline_aligned": "investigate remediation bundle/plan drift and restore baseline-compatible remediation metadata",
			},
			"epochContractGovernanceRemediationPriorityOrder":      []string{"contract_fully_compatible", "check_state_baseline_aligned", "remediation_baseline_aligned"},
			"epochContractGovernanceRemediationActions":            []string{},
			"epochContractGovernanceRemediationSummary":            "none_required",
			"epochContractGovernanceRemediationUrgency":            "none",
			"epochContractGovernanceRemediationPlanHash":           explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractGovernanceRemediationFingerprint":        explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"baselineEpochContractGovernanceRemediationSummary":    "none_required",
			"baselineEpochContractGovernanceRemediationUrgency":    "none",
			"baselineEpochContractGovernanceRemediationPlanHash":   explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractGovernanceRemediationSummaryDriftStatus": "unchanged",
			"epochContractGovernanceRemediationUrgencyDriftStatus": "unchanged",
			"epochContractGovernanceRemediationPlanDriftStatus":    "unchanged",
			"epochContractGovernanceRemediationDriftSummary":       "baseline_aligned",
			"epochContractGovernanceRemediationDriftRule":          "baseline_governance_remediation_matches",
			"epochContractGovernanceRemediationDriftReason":        "epoch-contract governance remediation summary, urgency, and plan hash match baseline",
			"epochContractGovernanceRemediationDriftFields":        []string{},
			"epochContractGovernanceRemediationDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineSummary=none_required;" +
					"currentSummary=none_required;" +
					"baselineUrgency=none;" +
					"currentUrgency=none;" +
					"baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"driftFields=;",
			),
			"epochContractGovernanceRemediationVerdictEvaluationOrder": []string{"stable_if_governance_stable_and_remediation_baseline_aligned", "degraded_if_governance_degraded", "degraded_if_governance_remediation_drift_detected", "unknown_default"},
			"epochContractGovernanceRemediationVerdictChecks": map[string]bool{
				"governance_state_stable":                      true,
				"governance_remediation_baseline_aligned":      true,
				"governance_remediation_plan_baseline_aligned": true,
			},
			"epochContractGovernanceRemediationVerdictFailedChecks":  []string{},
			"epochContractGovernanceRemediationVerdictFailedReasons": map[string]string{},
			"epochContractGovernanceRemediationVerdictState":         "stable",
			"epochContractGovernanceRemediationVerdictRule":          "stable_if_governance_stable_and_remediation_baseline_aligned",
			"epochContractGovernanceRemediationVerdictReason":        "governance is stable and governance-remediation metadata is baseline aligned",
			"epochContractGovernanceRemediationVerdictFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_governance_stable_and_remediation_baseline_aligned;" +
					"failedChecks=;" +
					"driftSummary=baseline_aligned;" +
					"planDrift=unchanged;",
			),
			"epochContractGovernanceRemediationVerdictSeverityByCheck": map[string]string{
				"governance_state_stable":                      "high",
				"governance_remediation_baseline_aligned":      "medium",
				"governance_remediation_plan_baseline_aligned": "high",
			},
			"epochContractGovernanceRemediationVerdictRequirementByCheck": map[string]string{
				"governance_state_stable":                      "required",
				"governance_remediation_baseline_aligned":      "required",
				"governance_remediation_plan_baseline_aligned": "required",
			},
			"epochContractGovernanceRemediationVerdictBundleID":          explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"baselineEpochContractGovernanceRemediationVerdictBundleID":  explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"epochContractGovernanceRemediationVerdictBundleDriftStatus": "unchanged",
			"epochContractGovernanceLineageVersion":                      "v1",
			"epochContractGovernanceLineageComponents": map[string]string{
				"governanceFingerprint":                   explainDiagnosticPostureHashString("state=stable;" + "rule=stable_if_compatible_and_baseline_aligned;" + "failedChecks=;" + "checkDrift=unchanged;" + "remediationDrift=baseline_aligned;"),
				"governanceRemediationFingerprint":        explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
				"governanceRemediationDriftFingerprint":   explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";driftFields=;"),
				"governanceRemediationVerdictFingerprint": explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;"),
			},
			"epochContractGovernanceLineageHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"governanceFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;") + ";" +
					"governanceRemediationFingerprint=" + explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";") + ";" +
					"governanceRemediationDriftFingerprint=" + explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;") + ";" +
					"governanceRemediationVerdictFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;") + ";",
			),
			"baselineEpochContractGovernanceLineageHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"governanceFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;") + ";" +
					"governanceRemediationFingerprint=" + explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";") + ";" +
					"governanceRemediationDriftFingerprint=" + explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;") + ";" +
					"governanceRemediationVerdictFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;") + ";",
			),
			"epochContractGovernanceLineageDriftStatus":  "unchanged",
			"epochContractGovernanceLineageDriftSummary": "baseline_aligned",
			"epochContractGovernanceLineageDriftRule":    "baseline_governance_lineage_matches",
			"epochContractGovernanceLineageDriftReason":  "governance-remediation lineage metadata matches baseline",
			"epochContractGovernanceLineageDriftFields":  []string{},
			"epochContractGovernanceLineageDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineLineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"currentLineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"baselineBundle=" + explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none") + ";" +
					"currentBundle=" + explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none") + ";" +
					"driftFields=;",
			),
			"epochContractGovernanceLineageCheckEvaluationOrder": []string{"lineage_hash_present", "lineage_component_hashes_present", "lineage_drift_status_matches_hashes"},
			"epochContractGovernanceLineageChecks": map[string]bool{
				"lineage_hash_present":                true,
				"lineage_component_hashes_present":    true,
				"lineage_drift_status_matches_hashes": true,
			},
			"epochContractGovernanceLineageFailedChecks":  []string{},
			"epochContractGovernanceLineageFailedReasons": map[string]string{},
			"epochContractGovernanceLineageSummary":       "consistent",
			"epochContractGovernanceLineageFingerprint": explainDiagnosticPostureHashString(
				"summary=consistent;" +
					"lineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"failedChecks=;",
			),
			"epochContractGovernanceLineageVerdictEvaluationOrder": []string{"stable_if_lineage_consistent_and_baseline_aligned", "degraded_if_lineage_inconsistent", "degraded_if_lineage_baseline_drifted", "unknown_default"},
			"epochContractGovernanceLineageVerdictChecks": map[string]bool{
				"lineage_consistent":              true,
				"lineage_baseline_aligned":        true,
				"lineage_drift_status_consistent": true,
			},
			"epochContractGovernanceLineageVerdictFailedChecks":  []string{},
			"epochContractGovernanceLineageVerdictFailedReasons": map[string]string{},
			"epochContractGovernanceLineageVerdictState":         "stable",
			"epochContractGovernanceLineageVerdictRule":          "stable_if_lineage_consistent_and_baseline_aligned",
			"epochContractGovernanceLineageVerdictReason":        "governance lineage is consistent and baseline aligned",
			"epochContractGovernanceLineageVerdictFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_lineage_consistent_and_baseline_aligned;" +
					"failedChecks=;" +
					"lineageSummary=consistent;" +
					"lineageDrift=baseline_aligned;",
			),
			"epochContractGovernanceLineageVerdictSeverityByCheck": map[string]string{
				"lineage_consistent":              "high",
				"lineage_baseline_aligned":        "high",
				"lineage_drift_status_consistent": "medium",
			},
			"epochContractGovernanceLineageVerdictRequirementByCheck": map[string]string{
				"lineage_consistent":              "required",
				"lineage_baseline_aligned":        "required",
				"lineage_drift_status_consistent": "required",
			},
			"epochContractGovernanceLineageVerdictSummary":                "none_required",
			"epochContractGovernanceLineageVerdictUrgency":                "none",
			"epochContractGovernanceLineageVerdictBundleID":               explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"baselineEpochContractGovernanceLineageVerdictBundleID":       explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"epochContractGovernanceLineageVerdictBundleDriftStatus":      "unchanged",
			"baselineEpochContractGovernanceLineageVerdictFingerprint":    explainDiagnosticPostureHashString("state=stable;rule=stable_if_lineage_consistent_and_baseline_aligned;failedChecks=;lineageSummary=consistent;lineageDrift=baseline_aligned;"),
			"epochContractGovernanceLineageVerdictFingerprintDriftStatus": "unchanged",
			"epochContractCompatibilitySummary":                           "epoch_contract_complete",
			"epochContractIncompatibilityReasons":                         []string{},
			"epochContractFingerprint": explainDiagnosticPostureHashString(
				"summary=epoch_contract_complete;" +
					"overallHash=" + explainDiagnosticPostureHashString(
					"version=v1;"+
						"ruleCatalogHash="+explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;")+";"+
						"evaluationOrderHash="+explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;")+";"+
						"consistencyCheckSchemaHash="+explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;")+";",
				) + ";" +
					"reasons=;",
			),
			"epochTransitionFingerprint": explainDiagnosticPostureHashString(
				"contractEpoch=contract.v1;" +
					"baselineContractEpoch=contract.v1;" +
					"contractTransition=unchanged;" +
					"remediationEpoch=remediation.v1;" +
					"baselineRemediationEpoch=remediation.v1;" +
					"remediationTransition=unchanged;" +
					"summary=baseline_aligned;" +
					"failed=;",
			),
			"checkEvaluationOrder": []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"},
			"checkReasonCodes": map[string]string{
				"version":             "version_mismatch",
				"schema":              "missing_decision_trace_schema_hash",
				"rule_reason_catalog": "missing_rule_reason_catalog_hash",
				"stage_order":         "missing_stage_order_hash",
				"stage_fragments":     "missing_stage_fragment",
				"overall_hash":        "missing_overall_hash",
			},
			"impactEvaluationOrder": []string{"compatible_if_no_failed_checks", "breaking_on_core_contract_checks", "breaking_on_stage_fragment_gap", "unknown_default"},
			"impactByCheck": map[string]string{
				"version":             "breaking",
				"schema":              "breaking",
				"rule_reason_catalog": "breaking",
				"stage_order":         "breaking",
				"stage_fragments":     "breaking",
				"overall_hash":        "breaking",
			},
			"checkFingerprints":           explainDiagnosticPostureContractCheckFingerprints(explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig())),
			"compatibilityFingerprint":    explainDiagnosticPostureContractCompatibilityFingerprint(explainDiagnosticPostureContractCheckFingerprints(explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig())), []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"}),
			"versionCompatible":           true,
			"schemaCompatible":            true,
			"ruleReasonCatalogCompatible": true,
			"stageOrderCompatible":        true,
			"stageCompatibility": map[string]bool{
				"primary":        true,
				"confidence":     true,
				"recommendation": true,
				"rationale":      true,
				"score":          true,
				"trend":          true,
			},
			"stageReasonCodes": map[string]string{
				"primary":        "missing_stage_fragment:primary",
				"confidence":     "missing_stage_fragment:confidence",
				"recommendation": "missing_stage_fragment:recommendation",
				"rationale":      "missing_stage_fragment:rationale",
				"score":          "missing_stage_fragment:score",
				"trend":          "missing_stage_fragment:trend",
			},
			"overallHashPresent":   true,
			"fullyCompatible":      true,
			"compatibilitySummary": "contract_components_complete",
			"impactClassification": "compatible",
			"impactRule":           "compatible_if_no_failed_checks",
			"impactReason":         "all compatibility checks passed",
			"remediationVersion":   "v1",
			"remediationByCheck": map[string]string{
				"version":             "align compatibilityVersion and contractComponents.version with the expected parser/runtime contract version",
				"schema":              "regenerate decisionTraceSchema metadata and ensure decisionTraceSchemaHash is present",
				"rule_reason_catalog": "regenerate ruleReasonCatalog metadata and ensure ruleReasonCatalogHash is present",
				"stage_order":         "restore decision-trace stageOrder metadata and ensure stageOrderHash is present",
				"stage_fragments":     "recompute stageContractFragments for all required stages and verify fragment hashes are non-empty",
				"overall_hash":        "rebuild contractComponents overallHash after schema/catalog/stage fragment metadata is refreshed",
			},
			"remediationSeverityByCheck": map[string]string{
				"version":             "high",
				"schema":              "high",
				"rule_reason_catalog": "high",
				"stage_order":         "high",
				"stage_fragments":     "medium",
				"overall_hash":        "high",
			},
			"remediationRequirementByCheck": map[string]string{
				"version":             "required",
				"schema":              "required",
				"rule_reason_catalog": "required",
				"stage_order":         "required",
				"stage_fragments":     "required",
				"overall_hash":        "required",
			},
			"remediationPriorityOrder":     []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"},
			"remediationSummary":           "none_required",
			"remediationUrgency":           "none",
			"remediationBundleID":          explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"remediationPlanHash":          explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none"),
			"baselineRemediationBundleID":  explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"baselineRemediationPlanHash":  explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none"),
			"remediationBundleDriftStatus": "unchanged",
			"remediationPlanDriftStatus":   "unchanged",
			"remediationDriftSummary":      "baseline_aligned",
			"remediationDriftRule":         "baseline_bundle_and_plan_match",
			"remediationDriftReason":       "current remediation bundle and plan hash match baseline compatible state",
			"remediationDriftFields":       []string{},
			"remediationDriftFingerprint":  explainDiagnosticPostureHashString("baselineBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" + "currentBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" + "baselinePlan=" + explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none") + ";" + "currentPlan=" + explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none") + ";" + "bundleDrift=unchanged;" + "planDrift=unchanged;"),
			"remediationActions":           []string{},
			"remediationActionPlan":        []map[string]any{},
			"failedChecks":                 []string{},
			"failedCheckReasons":           map[string]string{},
			"incompatibilityReasons":       []string{},
		},
		"reasonCatalogVersion":    "v1",
		"reasonCatalogConsistent": true,
		"reasonCatalogMismatches": []string{},
		"matchedReasonLookup": map[string]map[string]any{
			"primary":        {"rule": "typed_friendly_reset", "expectedReason": "typed-friendly assessment with zero warnings reset posture", "actualReason": "typed-friendly assessment with zero warnings reset posture", "matchesCatalog": true},
			"confidence":     {"rule": "high_healthy_typed_friendly", "expectedReason": "healthy posture with zero warnings and typed-friendly assessment", "actualReason": "healthy posture with zero warnings and typed-friendly assessment", "matchesCatalog": true},
			"recommendation": {"rule": "typed_friendly_reset", "expectedReason": "typed-friendly reset selected maintenance recommendation", "actualReason": "typed-friendly reset selected maintenance recommendation", "matchesCatalog": true},
			"rationale":      {"rule": "primary_healthy", "expectedReason": "healthy posture uses fixed healthy rationale", "actualReason": "healthy posture uses fixed healthy rationale", "matchesCatalog": true},
			"score":          {"rule": "score_band_excellent", "expectedReason": "final score mapped to excellent band", "actualReason": "final score mapped to excellent band", "matchesCatalog": true},
			"trend":          {"rule": "stable_min_score", "expectedReason": "score met stable threshold", "actualReason": "score met stable threshold", "matchesCatalog": true},
		},
		"decisionTrace": []map[string]any{
			{"stage": "primary", "rule": "typed_friendly_reset", "result": "healthy", "reason": "typed-friendly assessment with zero warnings reset posture", "inputs": map[string]any{"overallStatus": "typed_friendly", "totalWarnings": 0, "byCategory": map[string]int{}}},
			{"stage": "confidence", "rule": "high_healthy_typed_friendly", "result": "high", "reason": "healthy posture with zero warnings and typed-friendly assessment", "inputs": map[string]any{"primary": "healthy", "overallStatus": "typed_friendly", "totalWarnings": 0, "signalCount": 1}},
			{"stage": "recommendation", "rule": "typed_friendly_reset", "result": "maintain_typed_paths", "reason": "typed-friendly reset selected maintenance recommendation", "inputs": map[string]any{"primary": "healthy", "overallStatus": "typed_friendly", "totalWarnings": 0, "signals": []string{"typed_friendly"}}},
			{"stage": "rationale", "rule": "primary_healthy", "result": "No explain warnings and typed-friendly operator assessment", "reason": "healthy posture uses fixed healthy rationale", "inputs": map[string]any{"primary": "healthy", "highestPriorityCode": "", "overallStatus": "typed_friendly", "signals": []string{"typed_friendly"}}},
			{"stage": "score", "rule": "score_band_excellent", "result": "excellent", "reason": "final score mapped to excellent band", "inputs": map[string]any{"baseRule": "base_primary_healthy", "confidenceAdjustmentRule": "confidence_adjustment_high", "warningVolumePenaltyRule": "warning_penalty_skipped", "categoryPenaltyRule": "repeated_category_weighted_penalty", "rawScoreBeforeClamp": 95, "clampRule": "within_bounds"}},
			{"stage": "trend", "rule": "stable_min_score", "result": "stable", "reason": "score met stable threshold", "inputs": map[string]any{"score": 95, "degradingCategories": []string{"planner", "index"}, "byCategory": map[string]int{}, "trendScoreRule": "trend_score_stable"}},
		},
		"primaryRule":                   "typed_friendly_reset",
		"primaryEvaluationOrder":        []string{"typed_friendly_reset"},
		"confidenceRule":                "high_healthy_typed_friendly",
		"recommendationRule":            "typed_friendly_reset",
		"recommendationEvaluationOrder": []string{"typed_friendly_reset"},
		"rationaleRule":                 "primary_healthy",
		"rationaleTemplate":             "No explain warnings and typed-friendly operator assessment",
		"rationaleInputs": map[string]any{
			"highestPriorityCode": "",
			"overallStatus":       "typed_friendly",
			"signals":             []string{"typed_friendly"},
			"totalWarnings":       0,
		},
		"scoreRuleTrace": map[string]any{
			"baseRule":                     "base_primary_healthy",
			"confidenceAdjustmentRule":     "confidence_adjustment_high",
			"warningVolumePenaltyRule":     "warning_penalty_skipped",
			"categoryPenaltyContributions": map[string]int{},
			"rawScoreBeforeClamp":          95,
			"clampRule":                    "within_bounds",
			"scoreBandRule":                "score_band_excellent",
		},
		"trendRuleTrace": map[string]any{
			"trendRule":      "stable_min_score",
			"trendScoreRule": "trend_score_stable",
			"trendInputs": map[string]any{
				"score":               95,
				"degradingCategories": []string{"planner", "index"},
				"byCategory":          map[string]int{},
			},
		},
	}) {
		t.Fatalf("expected diagnostic posture evaluatedPolicy for healthy posture, got %#v", diagnosticPosture)
	}
	if scoreBreakdown, _ := diagnosticPosture["scoreBreakdown"].(map[string]int); !reflect.DeepEqual(scoreBreakdown, map[string]int{
		"base":                 90,
		"confidenceAdjustment": 5,
		"warningVolumePenalty": 0,
		"categoryPenalty":      0,
		"final":                95,
	}) {
		t.Fatalf("expected diagnostic posture scoreBreakdown for healthy posture, got %#v", diagnosticPosture)
	}
	if rationale, _ := diagnosticPosture["rationale"].(string); !strings.Contains(rationale, "No explain warnings") {
		t.Fatalf("expected healthy diagnostic rationale, got %#v", diagnosticPosture)
	}
	operatorAssessment, ok := runtimeStats["operatorAssessment"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.operatorAssessment map, got %T", runtimeStats["operatorAssessment"])
	}
	if overallStatus, _ := operatorAssessment["overallStatus"].(string); overallStatus != "typed_friendly" {
		t.Fatalf("expected operatorAssessment overallStatus typed_friendly, got %#v", operatorAssessment)
	}
	if recommendation, _ := operatorAssessment["recommendation"].(string); recommendation != "typed_fast_paths_likely" {
		t.Fatalf("expected operatorAssessment recommendation typed_fast_paths_likely, got %#v", operatorAssessment)
	}
	typedEligibleFamilies, ok := operatorAssessment["typedEligibleFamilies"].([]string)
	if !ok {
		t.Fatalf("expected typedEligibleFamilies []string, got %T", operatorAssessment["typedEligibleFamilies"])
	}
	if !reflect.DeepEqual(typedEligibleFamilies, []string{"distinct", "sort"}) {
		t.Fatalf("expected typedEligibleFamilies [distinct sort], got %#v", typedEligibleFamilies)
	}
	focusFamilies, ok := operatorAssessment["focusFamilies"].([]string)
	if !ok {
		t.Fatalf("expected focusFamilies []string, got %T", operatorAssessment["focusFamilies"])
	}
	if !reflect.DeepEqual(focusFamilies, []string{"distinct", "sort"}) {
		t.Fatalf("expected focusFamilies [distinct sort], got %#v", focusFamilies)
	}
	operatorStats, ok := runtimeStats["operators"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.operators map, got %T", runtimeStats["operators"])
	}
	if got, _ := operatorStats["distinctOperators"].(int); got < 1 {
		t.Fatalf("expected distinctOperators >= 1, got %#v", operatorStats["distinctOperators"])
	}
	if got, _ := operatorStats["sortOperators"].(int); got < 1 {
		t.Fatalf("expected sortOperators >= 1, got %#v", operatorStats["sortOperators"])
	}
	if got, _ := operatorStats["typedDistinctCandidates"].(int); got < 1 {
		t.Fatalf("expected typedDistinctCandidates >= 1, got %#v", operatorStats["typedDistinctCandidates"])
	}
	if got, _ := operatorStats["typedSortCandidates"].(int); got < 1 {
		t.Fatalf("expected typedSortCandidates >= 1, got %#v", operatorStats["typedSortCandidates"])
	}
	distinctShapes, ok := operatorStats["distinctShapes"].(map[string]any)
	if !ok {
		t.Fatalf("expected distinctShapes map, got %T", operatorStats["distinctShapes"])
	}
	if got, _ := distinctShapes["property"].(int); got < 1 {
		t.Fatalf("expected distinct property-shape count >= 1, got %#v", distinctShapes)
	}
	sortShapes, ok := operatorStats["sortShapes"].(map[string]any)
	if !ok {
		t.Fatalf("expected sortShapes map, got %T", operatorStats["sortShapes"])
	}
	if got, _ := sortShapes["identifier"].(int); got < 1 {
		t.Fatalf("expected sort identifier-shape count >= 1, got %#v", sortShapes)
	}
	warnings, ok := explainPayload["warnings"].([]map[string]any)
	if !ok {
		t.Fatalf("expected warnings []map[string]any, got %T", explainPayload["warnings"])
	}
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "OPERATOR_MIXED_DOMAIN_RISK" || code == "OPERATOR_TYPED_FALLBACK" {
			t.Fatalf("expected typed-friendly query to avoid operator fallback warnings, got %#v", warnings)
		}
	}
}

func TestExecuteExplainOutputContainsPlanAndParamsPipelinePayload(t *testing.T) {
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
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
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
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundProject := false
	foundSort := false
	foundPagination := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			foundProject = true
		case "SORT":
			foundSort = true
		case "PAGINATION":
			foundPagination = true
		}
	}
	if !foundMatch || !foundProject || !foundSort || !foundPagination {
		t.Fatalf("expected MATCH/PROJECT/SORT/PAGINATION nodes, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainMetadataReflectsPipelineRouteOptIn(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:User {name: $name}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "neo"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestBuildExplainOperatorRuntimeStatsReportsBlockedReasons(t *testing.T) {
	stats := buildExplainOperatorRuntimeStats([]map[string]any{
		{
			"op":         "DISTINCT",
			"projection": []any{"n.name AS name"},
		},
		{
			"op":         "DISTINCT",
			"projection": []any{"[n.name] AS payload"},
		},
		{
			"op":       "SORT",
			"ordering": []any{"toLower(n.name) ASC"},
		},
		{
			"op":      "AGGREGATE",
			"groupBy": []any{"toLower(n.name)"},
		},
		{
			"op":      "AGGREGATE",
			"groupBy": []any{"n.name", "toLower(n.alias)"},
		},
	})

	if got, _ := stats["typedDistinctCandidates"].(int); got != 1 {
		t.Fatalf("expected one typed distinct candidate, got %#v", stats["typedDistinctCandidates"])
	}
	if got, _ := stats["typedSortCandidates"].(int); got != 0 {
		t.Fatalf("expected no typed sort candidates, got %#v", stats["typedSortCandidates"])
	}
	if got, _ := stats["typedGroupCandidates"].(int); got != 0 {
		t.Fatalf("expected no typed group candidates, got %#v", stats["typedGroupCandidates"])
	}

	distinctBlockedReasons, ok := stats["distinctBlockedReasons"].(map[string]int)
	if !ok {
		t.Fatalf("expected distinctBlockedReasons map[string]int, got %T", stats["distinctBlockedReasons"])
	}
	if got := distinctBlockedReasons["list_expression"]; got != 1 {
		t.Fatalf("expected list_expression distinct blocked reason count 1, got %#v", distinctBlockedReasons)
	}
	distinctBlockedExamples, ok := stats["distinctBlockedExamples"].(map[string][]string)
	if !ok {
		t.Fatalf("expected distinctBlockedExamples map[string][]string, got %T", stats["distinctBlockedExamples"])
	}
	if got := distinctBlockedExamples["list_expression"]; !reflect.DeepEqual(got, []string{"[n.name]"}) {
		t.Fatalf("expected list_expression distinct blocked examples [\"[n.name]\"], got %#v", distinctBlockedExamples)
	}

	sortBlockedReasons, ok := stats["sortBlockedReasons"].(map[string]int)
	if !ok {
		t.Fatalf("expected sortBlockedReasons map[string]int, got %T", stats["sortBlockedReasons"])
	}
	if got := sortBlockedReasons["function_or_call"]; got != 1 {
		t.Fatalf("expected function_or_call sort blocked reason count 1, got %#v", sortBlockedReasons)
	}
	sortBlockedExamples, ok := stats["sortBlockedExamples"].(map[string][]string)
	if !ok {
		t.Fatalf("expected sortBlockedExamples map[string][]string, got %T", stats["sortBlockedExamples"])
	}
	if got := sortBlockedExamples["function_or_call"]; !reflect.DeepEqual(got, []string{"toLower(n.name)"}) {
		t.Fatalf("expected function_or_call sort blocked examples [\"toLower(n.name)\"], got %#v", sortBlockedExamples)
	}

	groupBlockedReasons, ok := stats["groupBlockedReasons"].(map[string]int)
	if !ok {
		t.Fatalf("expected groupBlockedReasons map[string]int, got %T", stats["groupBlockedReasons"])
	}
	if got := groupBlockedReasons["function_or_call"]; got != 1 {
		t.Fatalf("expected function_or_call group blocked reason count 1, got %#v", groupBlockedReasons)
	}
	if got := groupBlockedReasons["mixed_scalar_and_non_scalar"]; got != 1 {
		t.Fatalf("expected mixed_scalar_and_non_scalar group blocked reason count 1, got %#v", groupBlockedReasons)
	}
	groupBlockedExamples, ok := stats["groupBlockedExamples"].(map[string][]string)
	if !ok {
		t.Fatalf("expected groupBlockedExamples map[string][]string, got %T", stats["groupBlockedExamples"])
	}
	if got := groupBlockedExamples["function_or_call"]; !reflect.DeepEqual(got, []string{"toLower(n.name)"}) {
		t.Fatalf("expected function_or_call group blocked examples [\"toLower(n.name)\"], got %#v", groupBlockedExamples)
	}
	if got := groupBlockedExamples["mixed_scalar_and_non_scalar"]; !reflect.DeepEqual(got, []string{"n.name, toLower(n.alias)"}) {
		t.Fatalf("expected mixed_scalar_and_non_scalar group blocked examples [\"n.name, toLower(n.alias)\"], got %#v", groupBlockedExamples)
	}

	groupShapes, ok := stats["groupShapes"].(map[string]int)
	if !ok {
		t.Fatalf("expected groupShapes map[string]int, got %T", stats["groupShapes"])
	}
	if got := groupShapes["mixed_or_non_scalar"]; got != 2 {
		t.Fatalf("expected two mixed_or_non_scalar group shapes, got %#v", groupShapes)
	}

	distinctSummary, ok := stats["distinctSummary"].(map[string]any)
	if !ok {
		t.Fatalf("expected distinctSummary map, got %T", stats["distinctSummary"])
	}
	if got, _ := distinctSummary["status"].(string); got != "mixed_domain_risk" {
		t.Fatalf("expected distinctSummary status mixed_domain_risk, got %#v", distinctSummary)
	}
	if got, _ := distinctSummary["typedCandidates"].(int); got != 1 {
		t.Fatalf("expected distinctSummary typedCandidates=1, got %#v", distinctSummary)
	}
	if got, _ := distinctSummary["fallbackCandidates"].(int); got != 1 {
		t.Fatalf("expected distinctSummary fallbackCandidates=1, got %#v", distinctSummary)
	}

	sortSummary, ok := stats["sortSummary"].(map[string]any)
	if !ok {
		t.Fatalf("expected sortSummary map, got %T", stats["sortSummary"])
	}
	if got, _ := sortSummary["status"].(string); got != "fallback_likely" {
		t.Fatalf("expected sortSummary status fallback_likely, got %#v", sortSummary)
	}
	if got, _ := sortSummary["blockedReasonKinds"].(int); got != 1 {
		t.Fatalf("expected sortSummary blockedReasonKinds=1, got %#v", sortSummary)
	}

	groupSummary, ok := stats["groupSummary"].(map[string]any)
	if !ok {
		t.Fatalf("expected groupSummary map, got %T", stats["groupSummary"])
	}
	if got, _ := groupSummary["status"].(string); got != "fallback_likely" {
		t.Fatalf("expected groupSummary status fallback_likely, got %#v", groupSummary)
	}
	if got, _ := groupSummary["fallbackCandidates"].(int); got != 2 {
		t.Fatalf("expected groupSummary fallbackCandidates=2, got %#v", groupSummary)
	}
	if got, _ := groupSummary["blockedReasonKinds"].(int); got != 2 {
		t.Fatalf("expected groupSummary blockedReasonKinds=2, got %#v", groupSummary)
	}

	operatorAssessment := explainBuildOperatorAssessment(stats)
	if overallStatus, _ := operatorAssessment["overallStatus"].(string); overallStatus != "mixed_domain_risk" {
		t.Fatalf("expected mixed_domain_risk operator assessment, got %#v", operatorAssessment)
	}
	if recommendation, _ := operatorAssessment["recommendation"].(string); recommendation != "investigate_mixed_operator_shapes" {
		t.Fatalf("expected investigate_mixed_operator_shapes recommendation, got %#v", operatorAssessment)
	}
	mixedRiskFamilies, ok := operatorAssessment["mixedRiskFamilies"].([]string)
	if !ok {
		t.Fatalf("expected mixedRiskFamilies []string, got %T", operatorAssessment["mixedRiskFamilies"])
	}
	if !reflect.DeepEqual(mixedRiskFamilies, []string{"distinct"}) {
		t.Fatalf("expected mixedRiskFamilies [distinct], got %#v", mixedRiskFamilies)
	}
	focusFamilies, ok := operatorAssessment["focusFamilies"].([]string)
	if !ok {
		t.Fatalf("expected focusFamilies []string, got %T", operatorAssessment["focusFamilies"])
	}
	if !reflect.DeepEqual(focusFamilies, []string{"distinct"}) {
		t.Fatalf("expected focusFamilies [distinct], got %#v", focusFamilies)
	}
	fallbackLikelyFamilies, ok := operatorAssessment["fallbackLikelyFamilies"].([]string)
	if !ok {
		t.Fatalf("expected fallbackLikelyFamilies []string, got %T", operatorAssessment["fallbackLikelyFamilies"])
	}
	if !reflect.DeepEqual(fallbackLikelyFamilies, []string{"sort", "group"}) {
		t.Fatalf("expected fallbackLikelyFamilies [sort group], got %#v", fallbackLikelyFamilies)
	}

	operatorWarning := explainBuildOperatorAssessmentWarning([]map[string]any{
		{
			"op":         "DISTINCT",
			"projection": []any{"n.name AS name"},
		},
		{
			"op":         "DISTINCT",
			"projection": []any{"[n.name] AS payload"},
		},
		{
			"op":       "SORT",
			"ordering": []any{"toLower(n.name) ASC"},
		},
	})
	if operatorWarning == nil {
		t.Fatalf("expected mixed-risk operator warning")
	}
	if code, _ := operatorWarning["code"].(string); code != "OPERATOR_MIXED_DOMAIN_RISK" {
		t.Fatalf("expected OPERATOR_MIXED_DOMAIN_RISK warning, got %#v", operatorWarning)
	}
	if recommendation, _ := operatorWarning["recommendation"].(string); recommendation != "investigate_mixed_operator_shapes" {
		t.Fatalf("expected investigate_mixed_operator_shapes recommendation, got %#v", operatorWarning)
	}
	if severity, _ := operatorWarning["severity"].(string); severity != "medium" {
		t.Fatalf("expected medium operator warning severity, got %#v", operatorWarning)
	}
	if priority, _ := operatorWarning["priority"].(int); priority != 30 {
		t.Fatalf("expected operator warning priority 30, got %#v", operatorWarning)
	}
	warningFocusFamilies, ok := operatorWarning["focusFamilies"].([]string)
	if !ok {
		t.Fatalf("expected warning focusFamilies []string, got %T", operatorWarning["focusFamilies"])
	}
	if !reflect.DeepEqual(warningFocusFamilies, []string{"distinct"}) {
		t.Fatalf("expected warning focusFamilies [distinct], got %#v", warningFocusFamilies)
	}
}

func TestExplainSortWarningsOrdersByPriority(t *testing.T) {
	warnings := []map[string]any{
		{"code": "PLAN_ANALYSIS_PARTIAL", "priority": 60, "vertexId": "b"},
		{"code": "MISSING_PROPERTY_INDEX", "priority": 20, "vertexId": "v2"},
		{"code": "FULL_SCAN_FALLBACK", "priority": 10, "vertexId": "v1"},
		{"code": "MISSING_PROPERTY_INDEX", "priority": 20, "vertexId": "v1"},
	}

	explainSortWarnings(warnings)

	ordered := make([]string, 0, len(warnings))
	for _, warning := range warnings {
		code, _ := warning["code"].(string)
		vertexID, _ := warning["vertexId"].(string)
		ordered = append(ordered, code+"@"+vertexID)
	}
	if !reflect.DeepEqual(ordered, []string{
		"FULL_SCAN_FALLBACK@v1",
		"MISSING_PROPERTY_INDEX@v1",
		"MISSING_PROPERTY_INDEX@v2",
		"PLAN_ANALYSIS_PARTIAL@b",
	}) {
		t.Fatalf("unexpected warning order: %#v", ordered)
	}
}

func TestBuildExplainWarningSummaryRollsUpCategories(t *testing.T) {
	summary := buildExplainWarningSummary([]map[string]any{
		{"code": "FULL_SCAN_FALLBACK", "severity": "high", "priority": 10},
		{"code": "MISSING_PROPERTY_INDEX", "severity": "medium", "priority": 20},
		{"code": "OPERATOR_TYPED_FALLBACK", "severity": "low", "priority": 40},
	})

	byCategory, ok := summary["byCategory"].(map[string]int)
	if !ok {
		t.Fatalf("expected byCategory map[string]int, got %T", summary["byCategory"])
	}
	if !reflect.DeepEqual(byCategory, map[string]int{"planner": 1, "index": 1, "operator": 1}) {
		t.Fatalf("unexpected byCategory rollup: %#v", summary)
	}
	if highestCategory, _ := summary["highestCategory"].(string); highestCategory != "planner" {
		t.Fatalf("expected highestCategory planner, got %#v", summary)
	}
}

func TestExecuteExplainMetadataReflectsPipelineRouteOptInPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:User {name: $name}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "neo"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
	semanticBlock, ok := explainPayload["semantic"].(map[string]any)
	if !ok {
		t.Fatalf("expected pipeline semantic block, got %T", explainPayload["semantic"])
	}
	if statementKind, _ := semanticBlock["statementKind"].(string); statementKind == "" {
		t.Fatalf("expected semantic statementKind, got %#v", semanticBlock["statementKind"])
	}
	if nodes := requirePipelineLogicalPlanNodes(t, explainPayload); len(nodes) == 0 {
		t.Fatalf("expected logicalPlan nodes in pipeline payload")
	}
}

func TestExecuteExplainPipelinePayloadIncludesParamsAndPlanNodes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{
			Tenant:     "acme",
			ID:         "u-1",
			Labels:     []string{"User"},
			Properties: graph.PropertyMap{"name": []byte("neo")},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:User {name: $name}) RETURN n.id AS id LIMIT $max")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "neo", "max": 10})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	query, ok := explainPayload["query"].(map[string]any)
	if !ok {
		t.Fatalf("expected query map, got %T", explainPayload["query"])
	}
	params, ok := query["params"].([]string)
	if !ok {
		t.Fatalf("expected query.params []string, got %T", query["params"])
	}
	if !reflect.DeepEqual(params, []string{"max", "name"}) {
		t.Fatalf("expected query.params [max name], got %#v", params)
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	if rootNodeID, _ := logicalPlan["rootNodeId"].(string); rootNodeID == "" {
		t.Fatalf("expected non-empty logicalPlan.rootNodeId")
	}
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	if len(nodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainReportsFastPathExecutionStrategies(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-peer", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-shared", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("4")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-2", Type: "RATED", SrcID: "u-peer", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-3", Type: "RATED", SrcID: "u-peer", DstID: "m-candidate", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement(`EXPLAIN
MATCH (target:User)-[rt:RATED]->(shared:Movie)<-[rp:RATED]-(peer:User)
WHERE abs(rp.rating - rt.rating) <= 1
WITH target, peer, count(shared) AS shared_count, avg(abs(rt.rating-rp.rating)) AS avg_diff
MATCH (peer)-[rp2:RATED]->(candidate:Movie)
WHERE rp2.rating >= 4 AND NOT (target)-[:RATED]->(candidate)
RETURN candidate.id AS candidate, avg(rp2.rating) AS score, count(rp2) AS support, sum(shared_count) AS total_sim`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}

	fastPaths, ok := explainPayload["executionStrategies"].([]map[string]any)
	if !ok {
		t.Fatalf("expected executionStrategies []map[string]any, got %T", explainPayload["executionStrategies"])
	}
	if len(fastPaths) < 2 {
		t.Fatalf("expected at least 2 fast-path strategy signals, got %#v", fastPaths)
	}

	seenImplementations := map[string]struct{}{}
	lateMaterializationSeen := false
	for _, path := range fastPaths {
		if impl, _ := path["implementation"].(string); impl != "" {
			seenImplementations[impl] = struct{}{}
		}
		if impl, _ := path["implementation"].(string); impl == "fast_peer_candidate_return_aggregation_clause_pair" {
			if enabled, ok := path["lateMaterialization"].(bool); ok && enabled {
				lateMaterializationSeen = true
			}
		}
	}
	if _, ok := seenImplementations["fast_target_shared_peer_aggregation_clause_pair"]; !ok {
		t.Fatalf("missing fast target/shared-peer strategy marker: %#v", fastPaths)
	}
	if _, ok := seenImplementations["fast_peer_candidate_return_aggregation_clause_pair"]; !ok {
		t.Fatalf("missing fast peer/candidate strategy marker: %#v", fastPaths)
	}
	if !lateMaterializationSeen {
		t.Fatalf("expected lateMaterialization=true on peer/candidate fast-path strategy: %#v", fastPaths)
	}

	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	execution, ok := runtimeStats["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.execution map, got %T", runtimeStats["execution"])
	}
	if fastPathCandidates, _ := execution["fastPathCandidates"].(int); fastPathCandidates < 2 {
		t.Fatalf("expected fastPathCandidates >= 2, got %#v", execution["fastPathCandidates"])
	}
	if lateMatCandidates, _ := execution["lateMaterializationCandidates"].(int); lateMatCandidates < 1 {
		t.Fatalf("expected lateMaterializationCandidates >= 1, got %#v", execution["lateMaterializationCandidates"])
	}

	warnings, ok := explainPayload["warnings"].([]map[string]any)
	if !ok {
		t.Fatalf("expected warnings []map[string]any, got %T", explainPayload["warnings"])
	}
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL when fast paths are recognized: %#v", warnings)
		}
	}
}

func TestExecuteExplainReportsFastPathExecutionStrategiesPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-peer", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-shared", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("4")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-2", Type: "RATED", SrcID: "u-peer", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-3", Type: "RATED", SrcID: "u-peer", DstID: "m-candidate", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement(`EXPLAIN
MATCH (target:User)-[rt:RATED]->(shared:Movie)<-[rp:RATED]-(peer:User)
WHERE abs(rp.rating - rt.rating) <= 1
WITH target, peer, count(shared) AS shared_count, avg(abs(rt.rating-rp.rating)) AS avg_diff
MATCH (peer)-[rp2:RATED]->(candidate:Movie)
WHERE rp2.rating >= 4 AND NOT (target)-[:RATED]->(candidate)
RETURN candidate.id AS candidate, avg(rp2.rating) AS score, count(rp2) AS support, sum(shared_count) AS total_sim`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
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

func TestExecuteExplainSurfacesFastPathSelectivityFeedbackAfterExecution(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-peer", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-shared", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("4")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-2", Type: "RATED", SrcID: "u-peer", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-3", Type: "RATED", SrcID: "u-peer", DstID: "m-candidate", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := `MATCH (target:User)-[rt:RATED]->(shared:Movie)<-[rp:RATED]-(peer:User)
WHERE abs(rp.rating - rt.rating) <= 1
WITH target, peer, count(shared) AS shared_count, avg(abs(rt.rating-rp.rating)) AS avg_diff
MATCH (peer)-[rp2:RATED]->(candidate:Movie)
WHERE rp2.rating >= 4 AND NOT (target)-[:RATED]->(candidate)
RETURN candidate.id AS candidate, avg(rp2.rating) AS score, count(rp2) AS support, sum(shared_count) AS total_sim`

	exec := New(store, Options{})
	executeStmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, executeStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty execution results")
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
	fastPaths, ok := explainPayload["executionStrategies"].([]map[string]any)
	if !ok {
		t.Fatalf("expected executionStrategies []map[string]any, got %T", explainPayload["executionStrategies"])
	}
	foundFeedback := false
	for _, path := range fastPaths {
		if impl, _ := path["implementation"].(string); impl != "fast_peer_candidate_return_aggregation_clause_pair" {
			continue
		}
		assessment, ok := path["assessment"].(map[string]any)
		if !ok {
			t.Fatalf("expected fast-path assessment map, got %T", path["assessment"])
		}
		if clausePair, _ := assessment["clausePair"].(string); clausePair != "MATCH+RETURN" {
			t.Fatalf("expected fast-path assessment clausePair MATCH+RETURN, got %#v", assessment["clausePair"])
		}
		if observed, _ := path["feedbackObserved"].(bool); observed {
			feedback, ok := path["feedback"].(map[string]any)
			if !ok {
				t.Fatalf("expected structured feedback map, got %T", path["feedback"])
			}
			if quality, _ := path["quality"].(string); quality != "sample" {
				t.Fatalf("expected feedback quality sample, got %#v", path["quality"])
			}
			if selectivity, ok := path["selectivity"].(float64); !ok || selectivity <= 0 || selectivity > 1 {
				t.Fatalf("expected observed selectivity in (0,1], got %#v", path["selectivity"])
			}
			if feedbackSelectivity, ok := feedback["selectivity"].(float64); !ok || feedbackSelectivity <= 0 || feedbackSelectivity > 1 {
				t.Fatalf("expected structured feedback selectivity in (0,1], got %#v", feedback["selectivity"])
			}
			if feedbackQuality, _ := feedback["quality"].(string); feedbackQuality != "sample" {
				t.Fatalf("expected structured feedback quality sample, got %#v", feedback["quality"])
			}
			if inputRows, ok := path["inputRows"].(int64); !ok || inputRows <= 0 {
				t.Fatalf("expected positive inputRows feedback, got %#v", path["inputRows"])
			}
			if feedbackInputRows, ok := feedback["inputRows"].(int64); !ok || feedbackInputRows <= 0 {
				t.Fatalf("expected structured feedback inputRows > 0, got %#v", feedback["inputRows"])
			}
			if outputRows, ok := path["outputRows"].(int64); !ok || outputRows <= 0 {
				t.Fatalf("expected positive outputRows feedback, got %#v", path["outputRows"])
			}
			if feedbackOutputRows, ok := feedback["outputRows"].(int64); !ok || feedbackOutputRows <= 0 {
				t.Fatalf("expected structured feedback outputRows > 0, got %#v", feedback["outputRows"])
			}
		}
		foundFeedback = true
	}
	if !foundFeedback {
		t.Fatalf("expected fast-path feedback to be surfaced in EXPLAIN, got %#v", fastPaths)
	}
	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	execution, ok := runtimeStats["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.execution map, got %T", runtimeStats["execution"])
	}
	if _, exists := execution["fastPathFeedbackCandidates"]; !exists {
		t.Fatalf("expected fastPathFeedbackCandidates field in runtimeStats.execution")
	}
}

func TestExecuteExplainSurfacesFastPathSelectivityFeedbackAfterExecutionPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-target", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-peer", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-shared", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m-candidate", Labels: []string{"Movie"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-1", Type: "RATED", SrcID: "u-target", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("4")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-2", Type: "RATED", SrcID: "u-peer", DstID: "m-shared", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e-3", Type: "RATED", SrcID: "u-peer", DstID: "m-candidate", Properties: graph.PropertyMap{"rating": []byte("5")}}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	query := `MATCH (target:User)-[rt:RATED]->(shared:Movie)<-[rp:RATED]-(peer:User)
WHERE abs(rp.rating - rt.rating) <= 1
WITH target, peer, count(shared) AS shared_count, avg(abs(rt.rating-rp.rating)) AS avg_diff
MATCH (peer)-[rp2:RATED]->(candidate:Movie)
WHERE rp2.rating >= 4 AND NOT (target)-[:RATED]->(candidate)
RETURN candidate.id AS candidate, avg(rp2.rating) AS score, count(rp2) AS support, sum(shared_count) AS total_sim`

	exec := New(store, Options{})
	executeStmt, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, executeStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected non-empty execution results")
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

func TestExecuteExplainLargeTenantKeepsExactPredicateSignals(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	const totalUsers = 2000
	if err := store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < totalUsers; i++ {
			name := fmt.Sprintf("user-%d", i)
			if i == totalUsers/2 {
				name = "target"
			}
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant:     "acme",
				ID:         fmt.Sprintf("u-%d", i),
				Labels:     []string{"User"},
				Properties: graph.PropertyMap{"name": []byte(name)},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:User {name: $name}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "target"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	predicateSignals, ok := influencers["predicateSignals"].([]map[string]any)
	if !ok {
		t.Fatalf("expected predicateSignals []map[string]any, got %T", influencers["predicateSignals"])
	}
	if len(predicateSignals) == 0 {
		t.Fatalf("expected non-empty predicateSignals")
	}

	foundExact := false
	for _, signal := range predicateSignals {
		expr, _ := signal["expression"].(string)
		if !strings.Contains(expr, "name=target") {
			continue
		}
		if got := signal["matchedCount"]; got != 1 {
			t.Fatalf("expected matchedCount=1 for target predicate, got %#v", got)
		}
		if quality, _ := signal["quality"].(string); quality != "exact" {
			t.Fatalf("expected exact quality, got %#v", signal["quality"])
		}
		foundExact = true
	}
	if !foundExact {
		t.Fatalf("expected exact predicate signal for name=target, got %#v", predicateSignals)
	}
}

func TestExecuteExplainLargeTenantKeepsExactPredicateSignalsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	const totalUsers = 2000
	if err := store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < totalUsers; i++ {
			name := fmt.Sprintf("user-%d", i)
			if i == totalUsers/2 {
				name = "target"
			}
			if err := tx.PutVertex(ctx, &graph.Vertex{
				Tenant:     "acme",
				ID:         fmt.Sprintf("u-%d", i),
				Labels:     []string{"User"},
				Properties: graph.PropertyMap{"name": []byte(name)},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n:User {name: $name}) RETURN n.id AS id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "name": "target"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
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
			if kind, _ := attrs["kind"].(string); kind != "RETURN" {
				continue
			}
			foundReturnProject = true
			items, _ := attrs["items"].([]string)
			for _, item := range items {
				if strings.Contains(item, "n.id") {
					foundReturnItem = true
					break
				}
			}
		}
	}
	if !foundMatch || !foundReturnProject || !foundReturnItem {
		t.Fatalf("expected MATCH plus PROJECT(RETURN) with n.id projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
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
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.vertexes []map[string]any, got %T", logicalPlan["vertexes"])
	}
	if len(logicalNodes) < 2 {
		t.Fatalf("expected at least 2 plan nodes, got %d", len(logicalNodes))
	}
	firstOp, _ := logicalNodes[0]["op"].(string)
	if firstOp != "MATCH" {
		t.Fatalf("expected first pipeline node MATCH, got %#v", logicalNodes[0]["op"])
	}

	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}
}

func TestExecuteExplainFastLabelHistogramPlanPipelinePayload(t *testing.T) {
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
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundProject := false
	foundLabelsProjection := false
	foundCountProjection := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind != "RETURN" {
				continue
			}
			foundProject = true
			items, _ := attrs["items"].([]string)
			for _, item := range items {
				if strings.Contains(item, "labels(") {
					foundLabelsProjection = true
				}
				if strings.Contains(item, "count(labels(") {
					foundCountProjection = true
				}
			}
		}
	}
	if !foundMatch || !foundProject || !foundLabelsProjection || !foundCountProjection {
		t.Fatalf("expected MATCH/PROJECT with labels and count(labels) return projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainDirectedRelationshipScopesInfluencersAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "e-rated-1", Type: "RATED", SrcID: "p1", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS]->(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}

	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	vertexCounts, ok := influencers["vertexCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected vertexCounts []map[string]any, got %T", influencers["vertexCounts"])
	}
	if len(vertexCounts) != 1 || vertexCounts[0]["label"] != "Person" {
		t.Fatalf("expected only Person vertex count, got %#v", vertexCounts)
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}
	totals, ok := influencers["totals"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers totals map, got %T", influencers["totals"])
	}
	if verticesTotal, _ := totals["vertexes"].(int); verticesTotal != 4 {
		t.Fatalf("expected totals.vertexes=4, got %#v", totals["vertexes"])
	}
	if edgesTotal, _ := totals["edges"].(int); edgesTotal != 2 {
		t.Fatalf("expected totals.edges=2, got %#v", totals["edges"])
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality entries, got %T", explainPayload["cardinality"])
	}
	if op, _ := cardinality[0]["op"].(string); op != "EDGE_SCAN" {
		t.Fatalf("expected first cardinality op EDGE_SCAN, got %#v", cardinality[0]["op"])
	}
	bindings, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(bindings, []string{"p1", "k", "p2"}) {
		t.Fatalf("unexpected queryBindings: %#v", bindings)
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainDirectedRelationshipScopesInfluencersAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS]->(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	ops := make([]string, 0, len(logicalNodes))
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		ops = append(ops, op)
	}
	if !reflect.DeepEqual(ops, []string{"MATCH", "PROJECT", "PAGINATION"}) {
		t.Fatalf("expected logical op sequence [MATCH PROJECT PAGINATION], got %#v", ops)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainUsesStatsAwarePhysicalPlanPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		return nil
	})
	if seedErr != nil {
		t.Fatalf("seed failed: %v", seedErr)
	}

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (u:User)-[:BLOCKED]->(v:User) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	if op, _ := physicalNodes[0]["op"].(string); op != "PHY_EMPTY" {
		t.Fatalf("expected stats-aware EXPLAIN physical root PHY_EMPTY, got %#v", physicalNodes[0])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainReverseDirectedRelationshipScopesInfluencersAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p2", DstID: "p1"},
			{Tenant: "acme", ID: "e-rated-1", Type: "RATED", SrcID: "u1", DstID: "p1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)<-[k:KNOWS]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}

	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	vertexCounts, ok := influencers["vertexCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected vertexCounts []map[string]any, got %T", influencers["vertexCounts"])
	}
	if len(vertexCounts) != 1 || vertexCounts[0]["label"] != "Person" {
		t.Fatalf("expected only Person vertex count, got %#v", vertexCounts)
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality entries, got %T", explainPayload["cardinality"])
	}
	binding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(binding, []string{"p1", "k", "p2"}) {
		t.Fatalf("unexpected queryBindings: %#v", binding)
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainReverseDirectedRelationshipScopesInfluencersAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p2", DstID: "p1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)<-[k:KNOWS]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	ops := make([]string, 0, len(logicalNodes))
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		ops = append(ops, op)
	}
	if !reflect.DeepEqual(ops, []string{"MATCH", "PROJECT", "PAGINATION"}) {
		t.Fatalf("expected logical op sequence [MATCH PROJECT PAGINATION], got %#v", ops)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainUndirectedRelationshipScopesInfluencersAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "e-acted-1", Type: "ACTED_IN", SrcID: "p1", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality entries, got %T", explainPayload["cardinality"])
	}
	binding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(binding, []string{"p1", "k", "p2"}) {
		t.Fatalf("unexpected queryBindings: %#v", binding)
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainUndirectedRelationshipScopesInfluencersAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	ops := make([]string, 0, len(logicalNodes))
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		ops = append(ops, op)
	}
	if !reflect.DeepEqual(ops, []string{"MATCH", "PROJECT", "PAGINATION"}) {
		t.Fatalf("expected logical op sequence [MATCH PROJECT PAGINATION], got %#v", ops)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainDirectedVariableLengthRelationshipScopesAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "p2", DstID: "p3"},
			{Tenant: "acme", ID: "e-rated-1", Type: "RATED", SrcID: "p1", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS*1..3]->(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality entries, got %T", explainPayload["cardinality"])
	}
	binding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(binding, []string{"p1", "k", "p2"}) {
		t.Fatalf("unexpected queryBindings: %#v", binding)
	}
	if quality, _ := cardinality[0]["quality"].(string); quality != "estimate" {
		t.Fatalf("expected variable-length cardinality quality estimate, got %#v", cardinality[0]["quality"])
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainDirectedVariableLengthRelationshipScopesAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "p2", DstID: "p3"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS*1..3]->(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	ops := make([]string, 0, len(logicalNodes))
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		ops = append(ops, op)
	}
	if !reflect.DeepEqual(ops, []string{"MATCH", "PROJECT", "PAGINATION"}) {
		t.Fatalf("expected logical op sequence [MATCH PROJECT PAGINATION], got %#v", ops)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainMixedRelationshipChainScopesAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "a1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "b1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "c1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "a1", DstID: "b1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "b1", DstID: "c1"},
			{Tenant: "acme", ID: "e-acted-1", Type: "ACTED_IN", SrcID: "a1", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (a:Person)-[r1:KNOWS*1..2]->(b:Person)-[r2:KNOWS]->(c:Person) RETURN a, r1, b, r2, c LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) < 3 {
		t.Fatalf("expected logicalPlan.nodes length >= 3, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) < 2 {
		t.Fatalf("expected at least two cardinality entries, got %T len=%d", explainPayload["cardinality"], len(cardinality))
	}
	firstBinding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected first queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(firstBinding, []string{"a", "r1", "b"}) {
		t.Fatalf("unexpected first queryBindings: %#v", firstBinding)
	}

	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainMixedRelationshipChainScopesAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "a1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "b1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "c1", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "a1", DstID: "b1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "b1", DstID: "c1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (a:Person)-[r1:KNOWS*1..2]->(b:Person)-[r2:KNOWS]->(c:Person) RETURN a, r1, b, r2, c LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	if len(logicalNodes) < 3 {
		t.Fatalf("expected logicalPlan.nodes length >= 3, got %#v", logicalNodes)
	}
	foundMatch := false
	foundProject := false
	foundPagination := false
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			foundProject = true
		case "PAGINATION":
			foundPagination = true
		}
	}
	if !foundMatch || !foundProject || !foundPagination {
		t.Fatalf("expected logical nodes to include MATCH/PROJECT/PAGINATION, got %#v", logicalNodes)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainUndirectedVariableLengthRelationshipScopesAndBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p2", DstID: "p1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "p3", DstID: "p2"},
			{Tenant: "acme", ID: "e-rated-1", Type: "RATED", SrcID: "p2", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS*1..3]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}

	influencers, ok := explainPayload["influencers"].(map[string]any)
	if !ok {
		t.Fatalf("expected influencers map, got %T", explainPayload["influencers"])
	}
	edgeCounts, ok := influencers["edgeCounts"].([]map[string]any)
	if !ok {
		t.Fatalf("expected edgeCounts []map[string]any, got %T", influencers["edgeCounts"])
	}
	if len(edgeCounts) != 1 || edgeCounts[0]["type"] != "KNOWS" {
		t.Fatalf("expected only KNOWS edge count, got %#v", edgeCounts)
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) == 0 {
		t.Fatalf("expected cardinality entries, got %T", explainPayload["cardinality"])
	}
	binding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(binding, []string{"p1", "k", "p2"}) {
		t.Fatalf("unexpected queryBindings: %#v", binding)
	}
	if quality, _ := cardinality[0]["quality"].(string); quality != "estimate" {
		t.Fatalf("expected variable-length cardinality quality estimate, got %#v", cardinality[0]["quality"])
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainUndirectedVariableLengthRelationshipScopesAndBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "p2", DstID: "p1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "p3", DstID: "p2"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (p1:Person)-[k:KNOWS*1..3]-(p2:Person) RETURN p1, k, p2 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	ops := make([]string, 0, len(logicalNodes))
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		ops = append(ops, op)
	}
	if !reflect.DeepEqual(ops, []string{"MATCH", "PROJECT", "PAGINATION"}) {
		t.Fatalf("expected logical op sequence [MATCH PROJECT PAGINATION], got %#v", ops)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteExplainMixedRelationshipChainReverseSegmentBindings(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "a1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "b1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "c1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "b1", DstID: "a1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "b1", DstID: "c1"},
			{Tenant: "acme", ID: "e-acted-1", Type: "ACTED_IN", SrcID: "a1", DstID: "m1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (a:Person)<-[r1:KNOWS*1..2]-(b:Person)-[r2:KNOWS]->(c:Person) RETURN a, r1, b, r2, c LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(logicalNodes) < 3 {
		t.Fatalf("expected logicalPlan.nodes length >= 3, got %T len=%d", logicalPlan["nodes"], len(logicalNodes))
	}
	if len(logicalNodes) < 2 {
		t.Fatalf("expected at least 2 logicalPlan.nodes")
	}

	cardinality, ok := explainPayload["cardinality"].([]map[string]any)
	if !ok || len(cardinality) < 2 {
		t.Fatalf("expected at least two cardinality entries, got %T len=%d", explainPayload["cardinality"], len(cardinality))
	}
	firstBinding, ok := cardinality[0]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected first queryBindings []string, got %#v (%T)", cardinality[0]["queryBindings"], cardinality[0]["queryBindings"])
	}
	if !reflect.DeepEqual(firstBinding, []string{"a", "r1", "b"}) {
		t.Fatalf("unexpected first queryBindings: %#v", firstBinding)
	}
	secondBinding, ok := cardinality[1]["queryBindings"].([]string)
	if !ok {
		t.Fatalf("expected second queryBindings []string, got %#v (%T)", cardinality[1]["queryBindings"], cardinality[1]["queryBindings"])
	}
	if !reflect.DeepEqual(secondBinding, []string{"b", "r2", "c"}) {
		t.Fatalf("unexpected second queryBindings: %#v", secondBinding)
	}

	warnings, _ := explainPayload["warnings"].([]map[string]any)
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "PLAN_ANALYSIS_PARTIAL" {
			t.Fatalf("did not expect PLAN_ANALYSIS_PARTIAL warning: %#v", warnings)
		}
	}
}

func TestExecuteExplainMixedRelationshipChainReverseSegmentBindingsPipelinePayload(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	seedErr := store.Update(ctx, func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "a1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "b1", Labels: []string{"Person"}},
			{Tenant: "acme", ID: "c1", Labels: []string{"Person"}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}
		edges := []*graph.Edge{
			{Tenant: "acme", ID: "e-knows-1", Type: "KNOWS", SrcID: "b1", DstID: "a1"},
			{Tenant: "acme", ID: "e-knows-2", Type: "KNOWS", SrcID: "b1", DstID: "c1"},
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

	stmt, err := parser.ParseStatement("EXPLAIN MATCH (a:Person)<-[r1:KNOWS*1..2]-(b:Person)-[r2:KNOWS]->(c:Person) RETURN a, r1, b, r2, c LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one explain row, got %#v", res.Rows)
	}

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	logicalNodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	if len(logicalNodes) < 3 {
		t.Fatalf("expected logicalPlan.nodes length >= 3, got %#v", logicalNodes)
	}
	foundMatch := false
	foundProject := false
	foundPagination := false
	for _, node := range logicalNodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			foundProject = true
		case "PAGINATION":
			foundPagination = true
		}
	}
	if !foundMatch || !foundProject || !foundPagination {
		t.Fatalf("expected logical nodes to include MATCH/PROJECT/PAGINATION, got %#v", logicalNodes)
	}
	physicalPlan, ok := explainPayload["physicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected physicalPlan map, got %T", explainPayload["physicalPlan"])
	}
	physicalNodes, ok := physicalPlan["nodes"].([]map[string]any)
	if !ok || len(physicalNodes) == 0 {
		t.Fatalf("expected non-empty physicalPlan.nodes, got %#v", physicalPlan["nodes"])
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
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
		logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		if len(logicalNodes) == 0 {
			t.Fatalf("expected non-empty logicalPlan.nodes for %q", query)
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
		logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		if len(logicalNodes) == 0 {
			t.Fatalf("expected non-empty logicalPlan.nodes for %q", query)
		}
	}
}

func TestExecuteExplainFastVertexCountPlan(t *testing.T) {
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
		logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
		if !ok {
			t.Fatalf("expected logicalPlan.nodes []map for %q, got %T", query, logicalPlan["nodes"])
		}
		if len(logicalNodes) == 0 {
			t.Fatalf("expected non-empty logicalPlan.nodes for %q", query)
		}
	}
}

func TestExecuteExplainFastEdgeCountPlanPipelinePayload(t *testing.T) {
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

	queries := []string{
		"EXPLAIN MATCH ()-[e]-() RETURN count(e)",
		"EXPLAIN MATCH (:Movie)-[e]-() RETURN count(e)",
		"EXPLAIN MATCH ()-[e]-(:Movie) RETURN count(e)",
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
		assertPipelineExplainPayloadEnvelope(t, explainPayload)
		nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
		foundMatch := false
		foundProject := false
		foundCountProjection := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			switch op {
			case "MATCH":
				foundMatch = true
			case "PROJECT":
				foundProject = true
				attrs, _ := node["attrs"].(map[string]any)
				if kind, _ := attrs["kind"].(string); kind == "RETURN" {
					items, _ := attrs["items"].([]string)
					for _, item := range items {
						if strings.HasPrefix(item, "count(") {
							foundCountProjection = true
							break
						}
					}
				}
			}
		}
		if !foundMatch || !foundProject || !foundCountProjection {
			t.Fatalf("expected MATCH/PROJECT with count projection in pipeline payload for %q, got %#v", query, nodes)
		}
		assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
	}
}

func TestExecuteExplainFastEdgeDeletePlanPipelinePayload(t *testing.T) {
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

	queries := []string{
		"EXPLAIN MATCH ()-[e]-() DELETE e",
		"EXPLAIN MATCH (:Movie)-[e]-() DELETE e",
		"EXPLAIN MATCH ()-[e]-(:Movie) DELETE e",
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
		assertPipelineExplainPayloadEnvelope(t, explainPayload)
		nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
		foundMatch := false
		foundDeleteWrite := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			switch op {
			case "MATCH":
				foundMatch = true
			case "WRITE":
				attrs, _ := node["attrs"].(map[string]any)
				if kind, _ := attrs["kind"].(string); kind == "DELETE" {
					foundDeleteWrite = true
				}
			}
		}
		if !foundMatch || !foundDeleteWrite {
			t.Fatalf("expected MATCH/WRITE(kind=DELETE) nodes in pipeline payload for %q, got %#v", query, nodes)
		}
		assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
	}
}

func TestExecuteExplainFastVertexCountPlanPipelinePayload(t *testing.T) {
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
		assertPipelineExplainPayloadEnvelope(t, explainPayload)
		nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
		foundMatch := false
		foundProject := false
		foundCountProjection := false
		for _, node := range nodes {
			op, _ := node["op"].(string)
			switch op {
			case "MATCH":
				foundMatch = true
			case "PROJECT":
				foundProject = true
				attrs, _ := node["attrs"].(map[string]any)
				if kind, _ := attrs["kind"].(string); kind == "RETURN" {
					items, _ := attrs["items"].([]string)
					for _, item := range items {
						if strings.HasPrefix(item, "count(") {
							foundCountProjection = true
							break
						}
					}
				}
			}
		}
		if !foundMatch || !foundProject || !foundCountProjection {
			t.Fatalf("expected MATCH/PROJECT with count projection in pipeline payload for %q, got %#v", query, nodes)
		}
		assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
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
	if firstCode, _ := warnings[0]["code"].(string); firstCode != "MISSING_PROPERTY_INDEX" {
		t.Fatalf("expected highest-priority warning first, got %#v", warnings)
	}
	runtimeStats, ok := explainPayload["runtimeStats"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats map, got %T", explainPayload["runtimeStats"])
	}
	warningSummary, ok := runtimeStats["warningSummary"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.warningSummary map, got %T", runtimeStats["warningSummary"])
	}
	if highestPriorityCode, _ := warningSummary["highestPriorityCode"].(string); highestPriorityCode != "MISSING_PROPERTY_INDEX" {
		t.Fatalf("expected warningSummary highestPriorityCode MISSING_PROPERTY_INDEX, got %#v", warningSummary)
	}
	if highestPriority, _ := warningSummary["highestPriority"].(int); highestPriority != 20 {
		t.Fatalf("expected warningSummary highestPriority 20, got %#v", warningSummary)
	}
	if highestSeverity, _ := warningSummary["highestSeverity"].(string); highestSeverity != "medium" {
		t.Fatalf("expected warningSummary highestSeverity medium, got %#v", warningSummary)
	}
	byCategory, ok := warningSummary["byCategory"].(map[string]int)
	if !ok {
		t.Fatalf("expected warningSummary byCategory map[string]int, got %T", warningSummary["byCategory"])
	}
	if got := byCategory["index"]; got < 1 {
		t.Fatalf("expected at least one index warning in byCategory, got %#v", warningSummary)
	}
	if highestCategory, _ := warningSummary["highestCategory"].(string); highestCategory != "index" {
		t.Fatalf("expected warningSummary highestCategory index, got %#v", warningSummary)
	}
	diagnosticPosture, ok := runtimeStats["diagnosticPosture"].(map[string]any)
	if !ok {
		t.Fatalf("expected runtimeStats.diagnosticPosture map, got %T", runtimeStats["diagnosticPosture"])
	}
	if primary, _ := diagnosticPosture["primary"].(string); primary != "index_limited" {
		t.Fatalf("expected diagnostic posture primary index_limited, got %#v", diagnosticPosture)
	}
	if recommendation, _ := diagnosticPosture["recommendation"].(string); recommendation != "improve_index_coverage" {
		t.Fatalf("expected diagnostic posture recommendation improve_index_coverage, got %#v", diagnosticPosture)
	}
	if confidence, _ := diagnosticPosture["confidence"].(string); confidence != "medium" {
		t.Fatalf("expected diagnostic posture confidence medium, got %#v", diagnosticPosture)
	}
	if score, _ := diagnosticPosture["score"].(int); score != 40 {
		t.Fatalf("expected diagnostic posture score 40, got %#v", diagnosticPosture)
	}
	if scoreBand, _ := diagnosticPosture["scoreBand"].(string); scoreBand != "fair" {
		t.Fatalf("expected diagnostic posture scoreBand fair, got %#v", diagnosticPosture)
	}
	if trendHint, _ := diagnosticPosture["trendHint"].(string); trendHint != "degrading" {
		t.Fatalf("expected diagnostic posture trendHint degrading, got %#v", diagnosticPosture)
	}
	if trendScore, _ := diagnosticPosture["trendScore"].(int); trendScore != -1 {
		t.Fatalf("expected diagnostic posture trendScore -1, got %#v", diagnosticPosture)
	}
	if trendEvidence, _ := diagnosticPosture["trendEvidence"].(map[string]any); !reflect.DeepEqual(trendEvidence, map[string]any{
		"drivers":             []string{"index"},
		"totalWarnings":       1,
		"highestPriorityCode": "MISSING_PROPERTY_INDEX",
		"highestCategory":     "index",
	}) {
		t.Fatalf("expected diagnostic posture trendEvidence for index posture, got %#v", diagnosticPosture)
	}
	if trendDriverWeights, _ := diagnosticPosture["trendDriverWeights"].(map[string]int); !reflect.DeepEqual(trendDriverWeights, map[string]int{}) {
		t.Fatalf("expected diagnostic posture trendDriverWeights for index posture, got %#v", diagnosticPosture)
	}
	if scoreComputationVersion, _ := diagnosticPosture["scoreComputationVersion"].(string); scoreComputationVersion != "v1" {
		t.Fatalf("expected diagnostic posture scoreComputationVersion v1, got %#v", diagnosticPosture)
	}
	if scoreComputationConfig, _ := diagnosticPosture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig, explainDiagnosticPostureScoreComputationConfig()) {
		t.Fatalf("expected diagnostic posture scoreComputationConfig for index posture, got %#v", diagnosticPosture)
	}
	if scoreClampRange, _ := diagnosticPosture["scoreClampRange"].(map[string]int); !reflect.DeepEqual(scoreClampRange, map[string]int{"min": 0, "max": 100}) {
		t.Fatalf("expected diagnostic posture scoreClampRange for index posture, got %#v", diagnosticPosture)
	}
	if scoreInputs, _ := diagnosticPosture["scoreInputs"].(map[string]any); !reflect.DeepEqual(scoreInputs, map[string]any{
		"totalWarnings":          1,
		"confidenceClass":        "medium",
		"repeatedCategoryCounts": map[string]int{},
	}) {
		t.Fatalf("expected diagnostic posture scoreInputs for index posture, got %#v", diagnosticPosture)
	}
	if scoreBreakdown, _ := diagnosticPosture["scoreBreakdown"].(map[string]int); !reflect.DeepEqual(scoreBreakdown, map[string]int{
		"base":                 45,
		"confidenceAdjustment": 0,
		"warningVolumePenalty": 5,
		"categoryPenalty":      0,
		"final":                40,
	}) {
		t.Fatalf("expected diagnostic posture scoreBreakdown for index posture, got %#v", diagnosticPosture)
	}
	if rationale, _ := diagnosticPosture["rationale"].(string); !strings.Contains(rationale, "MISSING_PROPERTY_INDEX") {
		t.Fatalf("expected index-limited diagnostic rationale to reference top warning code, got %#v", diagnosticPosture)
	}
	foundMissingIndexWarning := false
	for _, warning := range warnings {
		if code, _ := warning["code"].(string); code == "MISSING_PROPERTY_INDEX" {
			if severity, _ := warning["severity"].(string); severity != "medium" {
				t.Fatalf("expected medium missing-index warning severity, got %#v", warning)
			}
			if priority, _ := warning["priority"].(int); priority != 20 {
				t.Fatalf("expected missing-index warning priority 20, got %#v", warning)
			}
			foundMissingIndexWarning = true
			break
		}
	}
	if !foundMissingIndexWarning {
		t.Fatalf("expected MISSING_PROPERTY_INDEX warning, got %#v", warnings)
	}
}

func TestBuildExplainDiagnosticPostureOperatorFallbackLimited(t *testing.T) {
	posture := buildExplainDiagnosticPosture(
		map[string]any{
			"totalWarnings": 0,
			"byCategory":    map[string]int{},
		},
		map[string]any{
			"overallStatus": "mixed_domain_risk",
		},
	)

	if primary, _ := posture["primary"].(string); primary != "operator_fallback_limited" {
		t.Fatalf("expected operator_fallback_limited posture, got %#v", posture)
	}
	if recommendation, _ := posture["recommendation"].(string); recommendation != "reduce_operator_fallback_shapes" {
		t.Fatalf("expected reduce_operator_fallback_shapes recommendation, got %#v", posture)
	}
	if confidence, _ := posture["confidence"].(string); confidence != "medium" {
		t.Fatalf("expected operator posture confidence medium, got %#v", posture)
	}
	if score, _ := posture["score"].(int); score != 60 {
		t.Fatalf("expected operator posture score 60, got %#v", posture)
	}
	if scoreBand, _ := posture["scoreBand"].(string); scoreBand != "good" {
		t.Fatalf("expected operator posture scoreBand good, got %#v", posture)
	}
	if trendHint, _ := posture["trendHint"].(string); trendHint != "watch" {
		t.Fatalf("expected operator posture trendHint watch, got %#v", posture)
	}
	if trendScore, _ := posture["trendScore"].(int); trendScore != 0 {
		t.Fatalf("expected operator posture trendScore 0, got %#v", posture)
	}
	if trendEvidence, _ := posture["trendEvidence"].(map[string]any); !reflect.DeepEqual(trendEvidence, map[string]any{
		"drivers":             []string{"operator"},
		"totalWarnings":       0,
		"highestPriorityCode": "",
		"highestCategory":     "",
	}) {
		t.Fatalf("expected operator posture trendEvidence, got %#v", posture)
	}
	if trendDriverWeights, _ := posture["trendDriverWeights"].(map[string]int); !reflect.DeepEqual(trendDriverWeights, map[string]int{}) {
		t.Fatalf("expected operator posture trendDriverWeights, got %#v", posture)
	}
	if scoreComputationVersion, _ := posture["scoreComputationVersion"].(string); scoreComputationVersion != "v1" {
		t.Fatalf("expected operator posture scoreComputationVersion v1, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig, explainDiagnosticPostureScoreComputationConfig()) {
		t.Fatalf("expected operator posture scoreComputationConfig, got %#v", posture)
	}
	if scoreClampRange, _ := posture["scoreClampRange"].(map[string]int); !reflect.DeepEqual(scoreClampRange, map[string]int{"min": 0, "max": 100}) {
		t.Fatalf("expected operator posture scoreClampRange, got %#v", posture)
	}
	if scoreInputs, _ := posture["scoreInputs"].(map[string]any); !reflect.DeepEqual(scoreInputs, map[string]any{
		"totalWarnings":          0,
		"confidenceClass":        "medium",
		"repeatedCategoryCounts": map[string]int{},
	}) {
		t.Fatalf("expected operator posture scoreInputs, got %#v", posture)
	}
	if scoreBreakdown, _ := posture["scoreBreakdown"].(map[string]int); !reflect.DeepEqual(scoreBreakdown, map[string]int{
		"base":                 60,
		"confidenceAdjustment": 0,
		"warningVolumePenalty": 0,
		"categoryPenalty":      0,
		"final":                60,
	}) {
		t.Fatalf("expected operator posture scoreBreakdown, got %#v", posture)
	}
	if rationale, _ := posture["rationale"].(string); !strings.Contains(rationale, "mixed_domain_risk") {
		t.Fatalf("expected operator posture rationale to mention overall status, got %#v", posture)
	}
}

func TestBuildExplainDiagnosticPostureAppliesCategoryWeightedPenalty(t *testing.T) {
	posture := buildExplainDiagnosticPosture(
		map[string]any{
			"totalWarnings":       4,
			"highestPriorityCode": "MISSING_PROPERTY_INDEX",
			"highestCategory":     "index",
			"byCategory": map[string]int{
				"index":    2,
				"operator": 2,
			},
		},
		map[string]any{
			"overallStatus": "mixed_domain_risk",
		},
	)

	if primary, _ := posture["primary"].(string); primary != "index_limited" {
		t.Fatalf("expected index_limited posture, got %#v", posture)
	}
	if confidence, _ := posture["confidence"].(string); confidence != "low" {
		t.Fatalf("expected low confidence for multi-signal/multi-warning posture, got %#v", posture)
	}
	if score, _ := posture["score"].(int); score != 10 {
		t.Fatalf("expected weighted penalty score 10, got %#v", posture)
	}
	if scoreBand, _ := posture["scoreBand"].(string); scoreBand != "poor" {
		t.Fatalf("expected poor scoreBand for heavily penalized posture, got %#v", posture)
	}
	if trendHint, _ := posture["trendHint"].(string); trendHint != "degrading" {
		t.Fatalf("expected weighted-penalty trendHint degrading, got %#v", posture)
	}
	if trendScore, _ := posture["trendScore"].(int); trendScore != -1 {
		t.Fatalf("expected weighted-penalty trendScore -1, got %#v", posture)
	}
	if trendEvidence, _ := posture["trendEvidence"].(map[string]any); !reflect.DeepEqual(trendEvidence, map[string]any{
		"drivers":             []string{"index", "operator"},
		"totalWarnings":       4,
		"highestPriorityCode": "MISSING_PROPERTY_INDEX",
		"highestCategory":     "index",
	}) {
		t.Fatalf("expected weighted-penalty trendEvidence, got %#v", posture)
	}
	if trendDriverWeights, _ := posture["trendDriverWeights"].(map[string]int); !reflect.DeepEqual(trendDriverWeights, map[string]int{
		"index":    -3,
		"operator": -2,
	}) {
		t.Fatalf("expected weighted-penalty trendDriverWeights, got %#v", posture)
	}
	if scoreComputationVersion, _ := posture["scoreComputationVersion"].(string); scoreComputationVersion != "v1" {
		t.Fatalf("expected weighted-penalty scoreComputationVersion v1, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig, explainDiagnosticPostureScoreComputationConfig()) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["categoryWeights"], explainDiagnosticScoreCategoryWeights()) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig categoryWeights, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["trendRules"], map[string]any{
		"stableMinScore":      80,
		"degradingMaxScore":   30,
		"degradingCategories": []string{"planner", "index"},
		"ruleEvaluationOrder": []string{"stable_min_score", "degrading_categories", "degrading_max_score", "default_trend"},
		"reasonByRule": map[string]string{
			"stable_min_score":     "score >= stableMinScore",
			"degrading_categories": "planner/index category warnings present",
			"degrading_max_score":  "score <= degradingMaxScore",
			"default_trend":        "fallback trend when no prior trend rule matched",
		},
		"trendScoreByHint": map[string]int{
			"stable":    1,
			"watch":     0,
			"degrading": -1,
		},
		"trendScoreRules": map[string]string{
			"stable":    "trend_score_stable",
			"watch":     "trend_score_watch",
			"degrading": "trend_score_degrading",
		},
		"defaultTrend": "watch",
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig trendRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["primarySelectionRules"], map[string]any{
		"categoryPriority":       []string{"planner", "index"},
		"primaryEvaluationOrder": []string{"category_priority_planner", "category_priority_index", "operator_fallback_status", "typed_friendly_reset", "default"},
		"categoryToPrimary": map[string]string{
			"planner": "planner_limited",
			"index":   "index_limited",
		},
		"operatorFallbackStatuses": []string{"mixed_domain_risk", "fallback_likely"},
		"typedFriendlyReset": map[string]any{
			"requiresTotalWarnings": 0,
			"requiresOverallStatus": "typed_friendly",
			"primary":               "healthy",
			"signal":                "typed_friendly",
			"recommendation":        "maintain_typed_paths",
		},
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig primarySelectionRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["confidenceRules"], map[string]any{
		"evaluationOrder": []string{"high_healthy_typed_friendly", "low_warning_or_multisignal", "default_medium"},
		"high": map[string]any{
			"requiresPrimary":       "healthy",
			"requiresTotalWarnings": 0,
			"requiresOverallStatus": "typed_friendly",
		},
		"low": map[string]int{
			"minTotalWarnings": 3,
			"orMinSignalCount": 2,
		},
		"default": "medium",
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig confidenceRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["recommendationRules"], map[string]string{
		"healthy":                   "maintain_typed_paths",
		"index_limited":             "improve_index_coverage",
		"planner_limited":           "optimize_scan_and_plan_shapes",
		"operator_fallback_limited": "reduce_operator_fallback_shapes",
		"default":                   "maintain_typed_paths",
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig recommendationRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["recommendationEvaluationOrder"], []string{"primary_planner", "primary_index", "operator_status_fallback", "typed_friendly_reset", "default"}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig recommendationEvaluationOrder, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleTemplates"], map[string]string{
		"healthy":                   "No explain warnings and typed-friendly operator assessment",
		"index_limited":             "Index warnings dominate diagnostics; highestPriorityCode=%s",
		"planner_limited":           "Planner warnings dominate diagnostics; highestPriorityCode=%s",
		"operator_fallback_limited": "Operator assessment indicates %s with fallback-oriented signals",
		"default":                   "Diagnostic posture derived from signals=%s and highestPriorityCode=%s",
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig rationaleTemplates, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleRules"], map[string]string{
		"healthy":                   "primary_healthy",
		"index_limited":             "primary_index_limited",
		"planner_limited":           "primary_planner_limited",
		"operator_fallback_limited": "primary_operator_fallback_limited",
		"default":                   "default",
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig rationaleRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["rationaleTemplateInputs"], map[string][]string{
		"healthy":                   {},
		"index_limited":             {"highestPriorityCode"},
		"planner_limited":           {"highestPriorityCode"},
		"operator_fallback_limited": {"overallStatus"},
		"default":                   {"signals", "highestPriorityCode"},
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig rationaleTemplateInputs, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["scoreRules"], map[string]any{
		"baseRuleByPrimary": map[string]string{
			"healthy":                   "base_primary_healthy",
			"operator_fallback_limited": "base_primary_operator_fallback_limited",
			"index_limited":             "base_primary_index_limited",
			"planner_limited":           "base_primary_planner_limited",
			"default":                   "base_primary_default",
		},
		"confidenceAdjustmentRuleByClass": map[string]string{
			"high":    "confidence_adjustment_high",
			"medium":  "confidence_adjustment_default",
			"low":     "confidence_adjustment_low",
			"default": "confidence_adjustment_default",
		},
		"warningVolumePenaltyRule": "warning_penalty_per_warning_linear",
		"categoryPenaltyRule":      "repeated_category_weighted_penalty",
		"clampRules": map[string]string{
			"withinBounds": "within_bounds",
			"min":          "clamped_min",
			"max":          "clamped_max",
		},
		"scoreBandRules": map[string]string{
			"excellent": "score_band_excellent",
			"good":      "score_band_good",
			"fair":      "score_band_fair",
			"poor":      "score_band_poor",
		},
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig scoreRules, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["decisionTraceSchema"], map[string]any{
		"version":    "v1",
		"stageOrder": []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"},
		"stageFields": map[string][]string{
			"primary":        {"stage", "rule", "result", "reason", "inputs"},
			"confidence":     {"stage", "rule", "result", "reason", "inputs"},
			"recommendation": {"stage", "rule", "result", "reason", "inputs"},
			"rationale":      {"stage", "rule", "result", "reason", "inputs"},
			"score":          {"stage", "rule", "result", "reason", "inputs"},
			"trend":          {"stage", "rule", "result", "reason", "inputs"},
		},
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig decisionTraceSchema, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); !reflect.DeepEqual(scoreComputationConfig["ruleReasonCatalog"], map[string]map[string]string{
		"primary": {
			"category_priority_planner": "planner category warning took primary precedence",
			"category_priority_index":   "index category warning set primary posture",
			"operator_fallback_status":  "operator assessment indicates fallback risk",
			"typed_friendly_reset":      "typed-friendly assessment with zero warnings reset posture",
			"default":                   "default primary posture applied",
		},
		"confidence": {
			"high_healthy_typed_friendly": "healthy posture with zero warnings and typed-friendly assessment",
			"low_warning_or_multisignal":  "warning volume or multi-signal posture lowered confidence",
			"default_medium":              "default medium confidence classification",
		},
		"recommendation": {
			"primary_planner":          "planner-limited posture selected planner optimization recommendation",
			"primary_index":            "index-limited posture selected index coverage recommendation",
			"operator_status_fallback": "operator fallback status selected fallback-reduction recommendation",
			"typed_friendly_reset":     "typed-friendly reset selected maintenance recommendation",
			"default":                  "default recommendation applied",
		},
		"rationale": {
			"primary_healthy":                   "healthy posture uses fixed healthy rationale",
			"primary_index_limited":             "index-limited posture uses highest-priority-code rationale",
			"primary_planner_limited":           "planner-limited posture uses highest-priority-code rationale",
			"primary_operator_fallback_limited": "operator-fallback posture uses operator-status rationale",
			"default":                           "default rationale template selected",
		},
		"score": {
			"score_band_excellent": "final score mapped to excellent band",
			"score_band_good":      "final score mapped to good band",
			"score_band_fair":      "final score mapped to fair band",
			"score_band_poor":      "final score mapped to poor band",
		},
		"trend": {
			"stable_min_score":     "score met stable threshold",
			"degrading_categories": "planner or index warning categories triggered degrading trend",
			"degrading_max_score":  "score met degrading threshold",
			"default_trend":        "default watch trend applied",
		},
	}) {
		t.Fatalf("expected weighted-penalty scoreComputationConfig ruleReasonCatalog, got %#v", posture)
	}
	if scoreComputationConfig, _ := posture["scoreComputationConfig"].(map[string]any); scoreComputationConfig["ruleReasonCatalogVersion"] != "v1" {
		t.Fatalf("expected weighted-penalty scoreComputationConfig ruleReasonCatalogVersion v1, got %#v", posture)
	}
	if scoreClampRange, _ := posture["scoreClampRange"].(map[string]int); !reflect.DeepEqual(scoreClampRange, map[string]int{"min": 0, "max": 100}) {
		t.Fatalf("expected weighted-penalty scoreClampRange, got %#v", posture)
	}
	if scoreInputs, _ := posture["scoreInputs"].(map[string]any); !reflect.DeepEqual(scoreInputs, map[string]any{
		"totalWarnings":   4,
		"confidenceClass": "low",
		"repeatedCategoryCounts": map[string]int{
			"index":    2,
			"operator": 2,
		},
	}) {
		t.Fatalf("expected weighted-penalty scoreInputs, got %#v", posture)
	}
	if scoreBreakdown, _ := posture["scoreBreakdown"].(map[string]int); !reflect.DeepEqual(scoreBreakdown, map[string]int{
		"base":                 45,
		"confidenceAdjustment": -10,
		"warningVolumePenalty": 20,
		"categoryPenalty":      -5,
		"final":                10,
	}) {
		t.Fatalf("expected weighted penalty scoreBreakdown, got %#v", posture)
	}
	if evaluatedPolicy, _ := posture["evaluatedPolicy"].(map[string]any); !reflect.DeepEqual(evaluatedPolicy, map[string]any{
		"decisionTraceVersion": "v1",
		"validationMode":       "strict",
		"contractHash":         explainDiagnosticPostureContractHash(explainDiagnosticPostureScoreComputationConfig()),
		"contractComponents":   explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig()),
		"compatibility": map[string]any{
			"compatibilityVersion":       "v1",
			"contractEpoch":              "contract.v1",
			"baselineContractEpoch":      "contract.v1",
			"contractEpochTransition":    "unchanged",
			"remediationEpoch":           "remediation.v1",
			"baselineRemediationEpoch":   "remediation.v1",
			"remediationEpochTransition": "unchanged",
			"epochTransitionRule":        "epoch_transition_none",
			"epochTransitionReason":      "contract and remediation epochs match baseline",
			"epochEvaluationOrder":       []string{"contract_epoch", "remediation_epoch"},
			"epochReasonCodes": map[string]string{
				"contract_epoch":    "contract_epoch_advanced_from_baseline",
				"remediation_epoch": "remediation_epoch_advanced_from_baseline",
			},
			"epochCompatibility": map[string]bool{
				"contract_epoch":    true,
				"remediation_epoch": true,
			},
			"epochFailedTransitions":       []string{},
			"epochFailedTransitionReasons": map[string]string{},
			"epochTransitionSummary":       "baseline_aligned",
			"epochImpactEvaluationOrder":   []string{"epoch_compatible_if_no_failed_transitions", "epoch_breaking_on_contract_transition", "epoch_breaking_on_remediation_transition", "epoch_unknown_default"},
			"epochImpactByCheck": map[string]string{
				"contract_epoch":    "breaking",
				"remediation_epoch": "breaking",
			},
			"epochImpactClassification": "compatible",
			"epochImpactRule":           "epoch_compatible_if_no_failed_transitions",
			"epochImpactReason":         "no epoch transition drift detected",
			"epochRemediationByCheck": map[string]string{
				"contract_epoch":    "upgrade parser/runtime consumers to a contract.v1-compatible epoch before parsing evaluatedPolicy compatibility metadata",
				"remediation_epoch": "refresh remediation consumers to the remediation.v1 schema before applying compatibility remediation guidance",
			},
			"epochRemediationPriorityOrder":   []string{"contract_epoch", "remediation_epoch"},
			"epochRemediationSummary":         "none_required",
			"epochRemediationActions":         []string{},
			"epochRemediationActionPlan":      []map[string]any{},
			"epochRemediationPlanHash":        explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochStateID":                    explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;"),
			"baselineEpochStateID":            explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;"),
			"epochDriftStatus":                "unchanged",
			"epochDriftFields":                []string{},
			"epochCompatibilityFingerprint":   explainDiagnosticPostureHashString("epochStateID=" + explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;") + ";" + "baselineEpochStateID=" + explainDiagnosticPostureHashString("contractEpoch=contract.v1;remediationEpoch=remediation.v1;") + ";" + "driftStatus=unchanged;" + "driftFields=;" + "impact=compatible;" + "remediationPlanHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"epochConsistencyEvaluationOrder": []string{"transition_summary_matches_failed_transitions", "drift_status_matches_state_ids", "impact_matches_failed_transitions", "remediation_summary_matches_actions"},
			"epochConsistencyReasonCodes": map[string]string{
				"transition_summary_matches_failed_transitions": "epoch_transition_summary_mismatch",
				"drift_status_matches_state_ids":                "epoch_drift_status_mismatch",
				"impact_matches_failed_transitions":             "epoch_impact_mismatch",
				"remediation_summary_matches_actions":           "epoch_remediation_summary_mismatch",
			},
			"epochConsistencyChecks": map[string]bool{
				"transition_summary_matches_failed_transitions": true,
				"drift_status_matches_state_ids":                true,
				"impact_matches_failed_transitions":             true,
				"remediation_summary_matches_actions":           true,
			},
			"epochConsistencyFailedChecks":  []string{},
			"epochConsistencyFailedReasons": map[string]string{},
			"epochConsistencySummary":       "consistent",
			"epochConsistencyFingerprint":   explainDiagnosticPostureHashString("summary=consistent;failedChecks=;driftStatus=unchanged;impact=compatible;remediationSummary=none_required;"),
			"epochRuleCatalogVersion":       "v1",
			"epochRuleCatalog": map[string]map[string]string{
				"transition": {
					"epoch_transition_none":     "contract and remediation epochs match baseline",
					"epoch_transition_detected": "contract or remediation epoch advanced from baseline",
				},
				"impact": {
					"epoch_compatible_if_no_failed_transitions": "no epoch transition drift detected",
					"epoch_breaking_on_contract_transition":     "contract epoch advanced relative to baseline",
					"epoch_breaking_on_remediation_transition":  "remediation epoch advanced relative to baseline",
				},
				"consistency": {
					"consistent":   "epoch consistency checks passed",
					"inconsistent": "epoch consistency checks failed",
				},
			},
			"epochMatchedRuleLookup": map[string]map[string]any{
				"transition": {
					"rule":           "epoch_transition_none",
					"expectedReason": "contract and remediation epochs match baseline",
					"actualReason":   "contract and remediation epochs match baseline",
					"matchesCatalog": true,
				},
				"impact": {
					"rule":           "epoch_compatible_if_no_failed_transitions",
					"expectedReason": "no epoch transition drift detected",
					"actualReason":   "no epoch transition drift detected",
					"matchesCatalog": true,
				},
				"consistency": {
					"rule":           "consistent",
					"expectedReason": "epoch consistency checks passed",
					"actualReason":   "epoch consistency checks passed",
					"matchesCatalog": true,
				},
			},
			"epochRuleCatalogConsistent": true,
			"epochRuleCatalogMismatches": []string{},
			"epochContractVersion":       "v1",
			"epochContractHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"ruleCatalogHash=" + explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;") + ";" +
					"evaluationOrderHash=" + explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;") + ";" +
					"consistencyCheckSchemaHash=" + explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;") + ";",
			),
			"epochContractComponents": map[string]any{
				"version":                    "v1",
				"ruleCatalogHash":            explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;"),
				"evaluationOrderHash":        explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;"),
				"consistencyCheckSchemaHash": explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;"),
				"overallHash": explainDiagnosticPostureHashString(
					"version=v1;" +
						"ruleCatalogHash=" + explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;") + ";" +
						"evaluationOrderHash=" + explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;") + ";" +
						"consistencyCheckSchemaHash=" + explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;") + ";",
				),
			},
			"epochContractCompatibility": map[string]bool{
				"ruleCatalogPresent":       true,
				"evaluationOrderPresent":   true,
				"consistencySchemaPresent": true,
				"overallHashPresent":       true,
			},
			"epochContractCheckEvaluationOrder": []string{"rule_catalog_present", "evaluation_order_present", "consistency_schema_present", "overall_hash_present"},
			"epochContractCheckReasonCodes": map[string]string{
				"rule_catalog_present":       "missing_epoch_rule_catalog_hash",
				"evaluation_order_present":   "missing_epoch_evaluation_order_hash",
				"consistency_schema_present": "missing_epoch_consistency_schema_hash",
				"overall_hash_present":       "missing_epoch_contract_overall_hash",
			},
			"epochContractCheckStatus": map[string]bool{
				"rule_catalog_present":       true,
				"evaluation_order_present":   true,
				"consistency_schema_present": true,
				"overall_hash_present":       true,
			},
			"epochContractFailedChecks":       []string{},
			"epochContractFailedCheckReasons": map[string]string{},
			"epochContractCheckSummary":       "all_checks_passed",
			"epochContractCheckFingerprint": explainDiagnosticPostureHashString(
				"summary=all_checks_passed;" +
					"failedChecks=;" +
					"overallHash=" + explainDiagnosticPostureHashString(
					"version=v1;"+
						"ruleCatalogHash="+explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;")+";"+
						"evaluationOrderHash="+explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;")+";"+
						"consistencyCheckSchemaHash="+explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;")+";",
				) + ";",
			),
			"epochContractFullyCompatible":               true,
			"epochContractImpactEvaluationOrder":         []string{"compatible_if_all_contract_checks_pass", "breaking_if_contract_components_missing", "unknown_default"},
			"epochContractImpactByCheck":                 map[string]string{"rule_catalog_present": "breaking", "evaluation_order_present": "breaking", "consistency_schema_present": "breaking", "overall_hash_present": "breaking"},
			"epochContractImpactClassification":          "compatible",
			"epochContractImpactRule":                    "compatible_if_all_contract_checks_pass",
			"epochContractImpactReason":                  "all epoch-contract checks passed",
			"epochContractRemediationByCheck":            map[string]string{"rule_catalog_present": "regenerate epoch rule catalog metadata and persist a non-empty epoch rule catalog hash", "evaluation_order_present": "restore epoch evaluation-order metadata and persist a non-empty evaluation-order hash", "consistency_schema_present": "regenerate epoch consistency-check schema metadata and persist a non-empty schema hash", "overall_hash_present": "recompute epoch contract overall hash after restoring contract component hashes"},
			"epochContractRemediationSeverityByCheck":    map[string]string{"rule_catalog_present": "high", "evaluation_order_present": "high", "consistency_schema_present": "high", "overall_hash_present": "high"},
			"epochContractRemediationRequirementByCheck": map[string]string{"rule_catalog_present": "required", "evaluation_order_present": "required", "consistency_schema_present": "required", "overall_hash_present": "required"},
			"epochContractRemediationPriorityOrder":      []string{"rule_catalog_present", "evaluation_order_present", "consistency_schema_present", "overall_hash_present"},
			"epochContractRemediationSummary":            "none_required",
			"epochContractRemediationUrgency":            "none",
			"epochContractRemediationBundleID":           explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"epochContractRemediationActions":            []string{},
			"epochContractRemediationPlanHash":           explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractRemediationFingerprint":        explainDiagnosticPostureHashString("impact=compatible;summary=none_required;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"baselineEpochContractCheckSummary":          "all_checks_passed",
			"baselineEpochContractImpactClassification":  "compatible",
			"baselineEpochContractRemediationSummary":    "none_required",
			"baselineEpochContractRemediationUrgency":    "none",
			"baselineEpochContractRemediationBundleID":   explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"baselineEpochContractRemediationPlanHash":   explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractRemediationBundleDriftStatus":  "unchanged",
			"epochContractRemediationPlanDriftStatus":    "unchanged",
			"epochContractRemediationDriftSummary":       "baseline_aligned",
			"epochContractRemediationDriftRule":          "baseline_epoch_contract_bundle_and_plan_match",
			"epochContractRemediationDriftReason":        "current epoch-contract remediation bundle and plan hash match baseline compatible state",
			"epochContractRemediationDriftFields":        []string{},
			"epochContractRemediationDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" +
					"currentBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" +
					"baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"bundleDrift=unchanged;" +
					"planDrift=unchanged;",
			),
			"epochContractCheckDriftStatus": "unchanged",
			"epochContractCheckDriftRule":   "baseline_epoch_contract_check_state_matches",
			"epochContractCheckDriftReason": "epoch-contract check, impact, and remediation summaries match baseline",
			"epochContractCheckDriftFields": []string{},
			"epochContractCheckDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineCheckSummary=all_checks_passed;" +
					"currentCheckSummary=all_checks_passed;" +
					"baselineImpact=compatible;" +
					"currentImpact=compatible;" +
					"baselineRemediationSummary=none_required;" +
					"currentRemediationSummary=none_required;" +
					"driftStatus=unchanged;" +
					"driftFields=;",
			),
			"epochContractGovernanceEvaluationOrder": []string{"stable_if_compatible_and_baseline_aligned", "degraded_if_incompatible", "degraded_if_check_or_remediation_drift", "unknown_default"},
			"epochContractGovernanceChecks": map[string]bool{
				"contract_fully_compatible":    true,
				"check_state_baseline_aligned": true,
				"remediation_baseline_aligned": true,
			},
			"epochContractGovernanceFailedChecks":  []string{},
			"epochContractGovernanceFailedReasons": map[string]string{},
			"epochContractGovernanceState":         "stable",
			"epochContractGovernanceRule":          "stable_if_compatible_and_baseline_aligned",
			"epochContractGovernanceReason":        "epoch-contract checks are compatible and both check/remediation baselines are aligned",
			"epochContractGovernanceFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_compatible_and_baseline_aligned;" +
					"failedChecks=;" +
					"checkDrift=unchanged;" +
					"remediationDrift=baseline_aligned;",
			),
			"epochContractGovernanceRemediationByCheck": map[string]string{
				"contract_fully_compatible":    "restore required epoch-contract component hashes and recompute epoch contract compatibility artifacts",
				"check_state_baseline_aligned": "investigate epoch-contract check summary drift and realign check/impact/remediation summaries to baseline",
				"remediation_baseline_aligned": "investigate remediation bundle/plan drift and restore baseline-compatible remediation metadata",
			},
			"epochContractGovernanceRemediationPriorityOrder":      []string{"contract_fully_compatible", "check_state_baseline_aligned", "remediation_baseline_aligned"},
			"epochContractGovernanceRemediationActions":            []string{},
			"epochContractGovernanceRemediationSummary":            "none_required",
			"epochContractGovernanceRemediationUrgency":            "none",
			"epochContractGovernanceRemediationPlanHash":           explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractGovernanceRemediationFingerprint":        explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
			"baselineEpochContractGovernanceRemediationSummary":    "none_required",
			"baselineEpochContractGovernanceRemediationUrgency":    "none",
			"baselineEpochContractGovernanceRemediationPlanHash":   explainDiagnosticPostureHashString("summary=none_required;actions=;"),
			"epochContractGovernanceRemediationSummaryDriftStatus": "unchanged",
			"epochContractGovernanceRemediationUrgencyDriftStatus": "unchanged",
			"epochContractGovernanceRemediationPlanDriftStatus":    "unchanged",
			"epochContractGovernanceRemediationDriftSummary":       "baseline_aligned",
			"epochContractGovernanceRemediationDriftRule":          "baseline_governance_remediation_matches",
			"epochContractGovernanceRemediationDriftReason":        "epoch-contract governance remediation summary, urgency, and plan hash match baseline",
			"epochContractGovernanceRemediationDriftFields":        []string{},
			"epochContractGovernanceRemediationDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineSummary=none_required;" +
					"currentSummary=none_required;" +
					"baselineUrgency=none;" +
					"currentUrgency=none;" +
					"baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
					"driftFields=;",
			),
			"epochContractGovernanceRemediationVerdictEvaluationOrder": []string{"stable_if_governance_stable_and_remediation_baseline_aligned", "degraded_if_governance_degraded", "degraded_if_governance_remediation_drift_detected", "unknown_default"},
			"epochContractGovernanceRemediationVerdictChecks": map[string]bool{
				"governance_state_stable":                      true,
				"governance_remediation_baseline_aligned":      true,
				"governance_remediation_plan_baseline_aligned": true,
			},
			"epochContractGovernanceRemediationVerdictFailedChecks":  []string{},
			"epochContractGovernanceRemediationVerdictFailedReasons": map[string]string{},
			"epochContractGovernanceRemediationVerdictState":         "stable",
			"epochContractGovernanceRemediationVerdictRule":          "stable_if_governance_stable_and_remediation_baseline_aligned",
			"epochContractGovernanceRemediationVerdictReason":        "governance is stable and governance-remediation metadata is baseline aligned",
			"epochContractGovernanceRemediationVerdictFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_governance_stable_and_remediation_baseline_aligned;" +
					"failedChecks=;" +
					"driftSummary=baseline_aligned;" +
					"planDrift=unchanged;",
			),
			"epochContractGovernanceRemediationVerdictSeverityByCheck": map[string]string{
				"governance_state_stable":                      "high",
				"governance_remediation_baseline_aligned":      "medium",
				"governance_remediation_plan_baseline_aligned": "high",
			},
			"epochContractGovernanceRemediationVerdictRequirementByCheck": map[string]string{
				"governance_state_stable":                      "required",
				"governance_remediation_baseline_aligned":      "required",
				"governance_remediation_plan_baseline_aligned": "required",
			},
			"epochContractGovernanceRemediationVerdictBundleID":          explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"baselineEpochContractGovernanceRemediationVerdictBundleID":  explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"epochContractGovernanceRemediationVerdictBundleDriftStatus": "unchanged",
			"epochContractGovernanceLineageVersion":                      "v1",
			"epochContractGovernanceLineageComponents": map[string]string{
				"governanceFingerprint":                   explainDiagnosticPostureHashString("state=stable;" + "rule=stable_if_compatible_and_baseline_aligned;" + "failedChecks=;" + "checkDrift=unchanged;" + "remediationDrift=baseline_aligned;"),
				"governanceRemediationFingerprint":        explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";"),
				"governanceRemediationDriftFingerprint":   explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";driftFields=;"),
				"governanceRemediationVerdictFingerprint": explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;"),
			},
			"epochContractGovernanceLineageHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"governanceFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;") + ";" +
					"governanceRemediationFingerprint=" + explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";") + ";" +
					"governanceRemediationDriftFingerprint=" + explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;") + ";" +
					"governanceRemediationVerdictFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;") + ";",
			),
			"baselineEpochContractGovernanceLineageHash": explainDiagnosticPostureHashString(
				"version=v1;" +
					"governanceFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;") + ";" +
					"governanceRemediationFingerprint=" + explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";") + ";" +
					"governanceRemediationDriftFingerprint=" + explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;") + ";" +
					"governanceRemediationVerdictFingerprint=" + explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;") + ";",
			),
			"epochContractGovernanceLineageDriftStatus":  "unchanged",
			"epochContractGovernanceLineageDriftSummary": "baseline_aligned",
			"epochContractGovernanceLineageDriftRule":    "baseline_governance_lineage_matches",
			"epochContractGovernanceLineageDriftReason":  "governance-remediation lineage metadata matches baseline",
			"epochContractGovernanceLineageDriftFields":  []string{},
			"epochContractGovernanceLineageDriftFingerprint": explainDiagnosticPostureHashString(
				"baselineLineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"currentLineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"baselineBundle=" + explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none") + ";" +
					"currentBundle=" + explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none") + ";" +
					"driftFields=;",
			),
			"epochContractGovernanceLineageCheckEvaluationOrder": []string{"lineage_hash_present", "lineage_component_hashes_present", "lineage_drift_status_matches_hashes"},
			"epochContractGovernanceLineageChecks": map[string]bool{
				"lineage_hash_present":                true,
				"lineage_component_hashes_present":    true,
				"lineage_drift_status_matches_hashes": true,
			},
			"epochContractGovernanceLineageFailedChecks":  []string{},
			"epochContractGovernanceLineageFailedReasons": map[string]string{},
			"epochContractGovernanceLineageSummary":       "consistent",
			"epochContractGovernanceLineageFingerprint": explainDiagnosticPostureHashString(
				"summary=consistent;" +
					"lineageHash=" + explainDiagnosticPostureHashString("version=v1;governanceFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_compatible_and_baseline_aligned;failedChecks=;checkDrift=unchanged;remediationDrift=baseline_aligned;")+";governanceRemediationFingerprint="+explainDiagnosticPostureHashString("state=stable;summary=none_required;urgency=none;planHash="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";")+";governanceRemediationDriftFingerprint="+explainDiagnosticPostureHashString("baselineSummary=none_required;currentSummary=none_required;baselineUrgency=none;currentUrgency=none;baselinePlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";currentPlan="+explainDiagnosticPostureHashString("summary=none_required;actions=;")+";driftFields=;")+";governanceRemediationVerdictFingerprint="+explainDiagnosticPostureHashString("state=stable;rule=stable_if_governance_stable_and_remediation_baseline_aligned;failedChecks=;driftSummary=baseline_aligned;planDrift=unchanged;")+";") + ";" +
					"failedChecks=;",
			),
			"epochContractGovernanceLineageVerdictEvaluationOrder": []string{"stable_if_lineage_consistent_and_baseline_aligned", "degraded_if_lineage_inconsistent", "degraded_if_lineage_baseline_drifted", "unknown_default"},
			"epochContractGovernanceLineageVerdictChecks": map[string]bool{
				"lineage_consistent":              true,
				"lineage_baseline_aligned":        true,
				"lineage_drift_status_consistent": true,
			},
			"epochContractGovernanceLineageVerdictFailedChecks":  []string{},
			"epochContractGovernanceLineageVerdictFailedReasons": map[string]string{},
			"epochContractGovernanceLineageVerdictState":         "stable",
			"epochContractGovernanceLineageVerdictRule":          "stable_if_lineage_consistent_and_baseline_aligned",
			"epochContractGovernanceLineageVerdictReason":        "governance lineage is consistent and baseline aligned",
			"epochContractGovernanceLineageVerdictFingerprint": explainDiagnosticPostureHashString(
				"state=stable;" +
					"rule=stable_if_lineage_consistent_and_baseline_aligned;" +
					"failedChecks=;" +
					"lineageSummary=consistent;" +
					"lineageDrift=baseline_aligned;",
			),
			"epochContractGovernanceLineageVerdictSeverityByCheck": map[string]string{
				"lineage_consistent":              "high",
				"lineage_baseline_aligned":        "high",
				"lineage_drift_status_consistent": "medium",
			},
			"epochContractGovernanceLineageVerdictRequirementByCheck": map[string]string{
				"lineage_consistent":              "required",
				"lineage_baseline_aligned":        "required",
				"lineage_drift_status_consistent": "required",
			},
			"epochContractGovernanceLineageVerdictSummary":                "none_required",
			"epochContractGovernanceLineageVerdictUrgency":                "none",
			"epochContractGovernanceLineageVerdictBundleID":               explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"baselineEpochContractGovernanceLineageVerdictBundleID":       explainDiagnosticPostureRemediationBundleID("stable", "none_required", "none"),
			"epochContractGovernanceLineageVerdictBundleDriftStatus":      "unchanged",
			"baselineEpochContractGovernanceLineageVerdictFingerprint":    explainDiagnosticPostureHashString("state=stable;rule=stable_if_lineage_consistent_and_baseline_aligned;failedChecks=;lineageSummary=consistent;lineageDrift=baseline_aligned;"),
			"epochContractGovernanceLineageVerdictFingerprintDriftStatus": "unchanged",
			"epochContractCompatibilitySummary":                           "epoch_contract_complete",
			"epochContractIncompatibilityReasons":                         []string{},
			"epochContractFingerprint": explainDiagnosticPostureHashString(
				"summary=epoch_contract_complete;" +
					"overallHash=" + explainDiagnosticPostureHashString(
					"version=v1;"+
						"ruleCatalogHash="+explainDiagnosticPostureHashString("transition.epoch_transition_none=contract and remediation epochs match baseline;transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;consistency.consistent=epoch consistency checks passed;consistency.inconsistent=epoch consistency checks failed;")+";"+
						"evaluationOrderHash="+explainDiagnosticPostureHashString("epochEvaluationOrder=contract_epoch,remediation_epoch;epochImpactEvaluationOrder=epoch_compatible_if_no_failed_transitions,epoch_breaking_on_contract_transition,epoch_breaking_on_remediation_transition,epoch_unknown_default;epochConsistencyEvaluationOrder=transition_summary_matches_failed_transitions,drift_status_matches_state_ids,impact_matches_failed_transitions,remediation_summary_matches_actions;")+";"+
						"consistencyCheckSchemaHash="+explainDiagnosticPostureHashString("transition_summary_matches_failed_transitions:bool;drift_status_matches_state_ids:bool;impact_matches_failed_transitions:bool;remediation_summary_matches_actions:bool;")+";",
				) + ";" +
					"reasons=;",
			),
			"epochTransitionFingerprint": explainDiagnosticPostureHashString(
				"contractEpoch=contract.v1;" +
					"baselineContractEpoch=contract.v1;" +
					"contractTransition=unchanged;" +
					"remediationEpoch=remediation.v1;" +
					"baselineRemediationEpoch=remediation.v1;" +
					"remediationTransition=unchanged;" +
					"summary=baseline_aligned;" +
					"failed=;",
			),
			"checkEvaluationOrder": []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"},
			"checkReasonCodes": map[string]string{
				"version":             "version_mismatch",
				"schema":              "missing_decision_trace_schema_hash",
				"rule_reason_catalog": "missing_rule_reason_catalog_hash",
				"stage_order":         "missing_stage_order_hash",
				"stage_fragments":     "missing_stage_fragment",
				"overall_hash":        "missing_overall_hash",
			},
			"impactEvaluationOrder": []string{"compatible_if_no_failed_checks", "breaking_on_core_contract_checks", "breaking_on_stage_fragment_gap", "unknown_default"},
			"impactByCheck": map[string]string{
				"version":             "breaking",
				"schema":              "breaking",
				"rule_reason_catalog": "breaking",
				"stage_order":         "breaking",
				"stage_fragments":     "breaking",
				"overall_hash":        "breaking",
			},
			"checkFingerprints":           explainDiagnosticPostureContractCheckFingerprints(explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig())),
			"compatibilityFingerprint":    explainDiagnosticPostureContractCompatibilityFingerprint(explainDiagnosticPostureContractCheckFingerprints(explainDiagnosticPostureContractComponents(explainDiagnosticPostureScoreComputationConfig())), []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"}),
			"versionCompatible":           true,
			"schemaCompatible":            true,
			"ruleReasonCatalogCompatible": true,
			"stageOrderCompatible":        true,
			"stageCompatibility": map[string]bool{
				"primary":        true,
				"confidence":     true,
				"recommendation": true,
				"rationale":      true,
				"score":          true,
				"trend":          true,
			},
			"stageReasonCodes": map[string]string{
				"primary":        "missing_stage_fragment:primary",
				"confidence":     "missing_stage_fragment:confidence",
				"recommendation": "missing_stage_fragment:recommendation",
				"rationale":      "missing_stage_fragment:rationale",
				"score":          "missing_stage_fragment:score",
				"trend":          "missing_stage_fragment:trend",
			},
			"overallHashPresent":   true,
			"fullyCompatible":      true,
			"compatibilitySummary": "contract_components_complete",
			"impactClassification": "compatible",
			"impactRule":           "compatible_if_no_failed_checks",
			"impactReason":         "all compatibility checks passed",
			"remediationVersion":   "v1",
			"remediationByCheck": map[string]string{
				"version":             "align compatibilityVersion and contractComponents.version with the expected parser/runtime contract version",
				"schema":              "regenerate decisionTraceSchema metadata and ensure decisionTraceSchemaHash is present",
				"rule_reason_catalog": "regenerate ruleReasonCatalog metadata and ensure ruleReasonCatalogHash is present",
				"stage_order":         "restore decision-trace stageOrder metadata and ensure stageOrderHash is present",
				"stage_fragments":     "recompute stageContractFragments for all required stages and verify fragment hashes are non-empty",
				"overall_hash":        "rebuild contractComponents overallHash after schema/catalog/stage fragment metadata is refreshed",
			},
			"remediationSeverityByCheck": map[string]string{
				"version":             "high",
				"schema":              "high",
				"rule_reason_catalog": "high",
				"stage_order":         "high",
				"stage_fragments":     "medium",
				"overall_hash":        "high",
			},
			"remediationRequirementByCheck": map[string]string{
				"version":             "required",
				"schema":              "required",
				"rule_reason_catalog": "required",
				"stage_order":         "required",
				"stage_fragments":     "required",
				"overall_hash":        "required",
			},
			"remediationPriorityOrder":     []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"},
			"remediationSummary":           "none_required",
			"remediationUrgency":           "none",
			"remediationBundleID":          explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"remediationPlanHash":          explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none"),
			"baselineRemediationBundleID":  explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none"),
			"baselineRemediationPlanHash":  explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none"),
			"remediationBundleDriftStatus": "unchanged",
			"remediationPlanDriftStatus":   "unchanged",
			"remediationDriftSummary":      "baseline_aligned",
			"remediationDriftRule":         "baseline_bundle_and_plan_match",
			"remediationDriftReason":       "current remediation bundle and plan hash match baseline compatible state",
			"remediationDriftFields":       []string{},
			"remediationDriftFingerprint":  explainDiagnosticPostureHashString("baselineBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" + "currentBundle=" + explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none") + ";" + "baselinePlan=" + explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none") + ";" + "currentPlan=" + explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none") + ";" + "bundleDrift=unchanged;" + "planDrift=unchanged;"),
			"remediationActions":           []string{},
			"remediationActionPlan":        []map[string]any{},
			"failedChecks":                 []string{},
			"failedCheckReasons":           map[string]string{},
			"incompatibilityReasons":       []string{},
		},
		"reasonCatalogVersion":    "v1",
		"reasonCatalogConsistent": true,
		"reasonCatalogMismatches": []string{},
		"matchedReasonLookup": map[string]map[string]any{
			"primary":        {"rule": "operator_fallback_status", "expectedReason": "operator assessment indicates fallback risk", "actualReason": "operator assessment indicates fallback risk", "matchesCatalog": true},
			"confidence":     {"rule": "low_warning_or_multisignal", "expectedReason": "warning volume or multi-signal posture lowered confidence", "actualReason": "warning volume or multi-signal posture lowered confidence", "matchesCatalog": true},
			"recommendation": {"rule": "operator_status_fallback", "expectedReason": "operator fallback status selected fallback-reduction recommendation", "actualReason": "operator fallback status selected fallback-reduction recommendation", "matchesCatalog": true},
			"rationale":      {"rule": "primary_index_limited", "expectedReason": "index-limited posture uses highest-priority-code rationale", "actualReason": "index-limited posture uses highest-priority-code rationale", "matchesCatalog": true},
			"score":          {"rule": "score_band_poor", "expectedReason": "final score mapped to poor band", "actualReason": "final score mapped to poor band", "matchesCatalog": true},
			"trend":          {"rule": "degrading_categories", "expectedReason": "planner or index warning categories triggered degrading trend", "actualReason": "planner or index warning categories triggered degrading trend", "matchesCatalog": true},
		},
		"decisionTrace": []map[string]any{
			{"stage": "primary", "rule": "operator_fallback_status", "result": "index_limited", "reason": "operator assessment indicates fallback risk", "inputs": map[string]any{"overallStatus": "mixed_domain_risk", "totalWarnings": 4, "byCategory": map[string]int{"index": 2, "operator": 2}}},
			{"stage": "confidence", "rule": "low_warning_or_multisignal", "result": "low", "reason": "warning volume or multi-signal posture lowered confidence", "inputs": map[string]any{"primary": "index_limited", "overallStatus": "mixed_domain_risk", "totalWarnings": 4, "signalCount": 2}},
			{"stage": "recommendation", "rule": "operator_status_fallback", "result": "reduce_operator_fallback_shapes", "reason": "operator fallback status selected fallback-reduction recommendation", "inputs": map[string]any{"primary": "index_limited", "overallStatus": "mixed_domain_risk", "totalWarnings": 4, "signals": []string{"index", "operator"}}},
			{"stage": "rationale", "rule": "primary_index_limited", "result": "Index warnings dominate diagnostics; highestPriorityCode=%s", "reason": "index-limited posture uses highest-priority-code rationale", "inputs": map[string]any{"primary": "index_limited", "highestPriorityCode": "MISSING_PROPERTY_INDEX", "overallStatus": "mixed_domain_risk", "signals": []string{"index", "operator"}}},
			{"stage": "score", "rule": "score_band_poor", "result": "poor", "reason": "final score mapped to poor band", "inputs": map[string]any{"baseRule": "base_primary_index_limited", "confidenceAdjustmentRule": "confidence_adjustment_low", "warningVolumePenaltyRule": "warning_penalty_applied", "categoryPenaltyRule": "repeated_category_weighted_penalty", "rawScoreBeforeClamp": 10, "clampRule": "within_bounds"}},
			{"stage": "trend", "rule": "degrading_categories", "result": "degrading", "reason": "planner or index warning categories triggered degrading trend", "inputs": map[string]any{"score": 10, "degradingCategories": []string{"planner", "index"}, "byCategory": map[string]int{"index": 2, "operator": 2}, "trendScoreRule": "trend_score_degrading"}},
		},
		"primaryRule":                   "operator_fallback_status",
		"primaryEvaluationOrder":        []string{"category_priority_index", "operator_fallback_status"},
		"confidenceRule":                "low_warning_or_multisignal",
		"recommendationRule":            "operator_status_fallback",
		"recommendationEvaluationOrder": []string{"primary_index", "operator_status_fallback"},
		"rationaleRule":                 "primary_index_limited",
		"rationaleTemplate":             "Index warnings dominate diagnostics; highestPriorityCode=%s",
		"rationaleInputs": map[string]any{
			"highestPriorityCode": "MISSING_PROPERTY_INDEX",
			"overallStatus":       "mixed_domain_risk",
			"signals":             []string{"index", "operator"},
			"totalWarnings":       4,
		},
		"scoreRuleTrace": map[string]any{
			"baseRule":                 "base_primary_index_limited",
			"confidenceAdjustmentRule": "confidence_adjustment_low",
			"warningVolumePenaltyRule": "warning_penalty_applied",
			"categoryPenaltyContributions": map[string]int{
				"index":    -3,
				"operator": -2,
			},
			"rawScoreBeforeClamp": 10,
			"clampRule":           "within_bounds",
			"scoreBandRule":       "score_band_poor",
		},
		"trendRuleTrace": map[string]any{
			"trendRule":      "degrading_categories",
			"trendScoreRule": "trend_score_degrading",
			"trendInputs": map[string]any{
				"score":               10,
				"degradingCategories": []string{"planner", "index"},
				"byCategory": map[string]int{
					"index":    2,
					"operator": 2,
				},
			},
		},
	}) {
		t.Fatalf("expected weighted-penalty evaluatedPolicy, got %#v", posture)
	}
}

func TestExecuteExplainIndexTuningSignalsForMissingIndexPipelinePayload(t *testing.T) {
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
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundMatch := false
	foundProject := false
	foundReturnItem := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			foundProject = true
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				items, _ := attrs["items"].([]string)
				for _, item := range items {
					if strings.Contains(item, "n.id") {
						foundReturnItem = true
						break
					}
				}
			}
		}
	}
	if !foundMatch || !foundProject || !foundReturnItem {
		t.Fatalf("expected MATCH/PROJECT with n.id return projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "warnings", "runtimeStats")
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

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN id(dst) AS dstID")
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

func TestExecuteMatchPropertyLookupWithoutIndexReturnsNoRows(t *testing.T) {
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

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN id(dst) AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{Metrics: recorder})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "email": "alice@acme.io"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no rows without relationship match, got %#v", res.Rows)
	}
	if len(recorder.indexCandidates) > 0 {
		candidate := recorder.indexCandidates[0]
		if candidate.schema != "User" || candidate.property != "email" || candidate.indexed {
			t.Fatalf("unexpected index candidate metric: %#v", candidate)
		}
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

	stmt, err := parser.ParseStatement("MATCH (src:User { email: $email })-[:MEMBER_OF]->(dst) RETURN id(dst) AS dstID")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	catalog := indexschema.NewCatalog()
	catalog.AddPropertyIndex("acme", "User", "email")
	recorder := &executorMetricsRecorder{}
	exec := New(store, Options{IndexCatalog: catalog, Metrics: recorder})
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

	if len(recorder.indexLookups) > 0 {
		if recorder.indexLookups[0].strategy != "property_index" || recorder.indexLookups[0].outcome != "hit" {
			t.Fatalf("unexpected index lookup metric: %#v", recorder.indexLookups[0])
		}
	}
}

func TestExecuteMergeVertexPropertyIndexMetricsRecorded(t *testing.T) {
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

	stmt, err := parser.ParseStatement("MERGE (m:Movie {movie_id: $movie_id}) SET m.title = $title RETURN id(m) AS id")
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

	stmt, err := parser.ParseStatement("MERGE (m:Movie {movie_id: $movie_id}) SET m.title = $title RETURN id(m) AS id")
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
	logicalNodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan.nodes []map[string]any, got %T", logicalPlan["nodes"])
	}

	if len(logicalNodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes")
	}
}

func TestExecuteExplainWriteContextMatchReportsIndexScanPipelinePayload(t *testing.T) {
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

	explainPayload, ok := res.Rows[0]["explain"].(map[string]any)
	if !ok {
		t.Fatalf("expected explain payload map, got %T", res.Rows[0]["explain"])
	}
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	nodes := requirePipelineLogicalPlanNodes(t, explainPayload)
	foundWrite := false
	foundMatch := false
	foundWithProject := false
	foundProject := false
	foundReturnItem := false
	for _, node := range nodes {
		op, _ := node["op"].(string)
		switch op {
		case "WRITE":
			foundWrite = true
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind != "CREATE" {
				t.Fatalf("expected WRITE(kind=CREATE), got %#v", attrs)
			}
		case "MATCH":
			foundMatch = true
		case "PROJECT":
			attrs, _ := node["attrs"].(map[string]any)
			if kind, _ := attrs["kind"].(string); kind == "WITH" {
				foundWithProject = true
			}
			if kind, _ := attrs["kind"].(string); kind == "RETURN" {
				foundProject = true
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
	if !foundWrite || !foundMatch || !foundWithProject || !foundProject || !foundReturnItem {
		t.Fatalf("expected MATCH/WRITE(kind=CREATE)/PROJECT(WITH)/PROJECT(RETURN) with m.id return projection, got %#v", nodes)
	}
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}

func TestExecuteMatchWhereFiltersRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	stmt, err := parser.ParseStatement("MATCH (src)-[:MEMBER_OF]->(dst) WHERE id(src) = $srcID AND id(dst) = 'g2' RETURN id(dst) AS dstID")
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

func TestExecuteMatchWhereNotRelationshipPatternPredicate(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: graph.PropertyMap{"user_id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: graph.PropertyMap{"user_id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m1", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"movie_id": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m2", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"movie_id": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "m3", Labels: []string{"Movie"}, Properties: graph.PropertyMap{"movie_id": []byte("3")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "RATED", SrcID: "u1", DstID: "m1"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "RATED", SrcID: "u1", DstID: "m2"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "RATED", SrcID: "u2", DstID: "m3"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (target:User {user_id: '1'}), (candidate:Movie) WHERE NOT (target)-[:RATED]->(candidate) RETURN candidate.movie_id AS movieID ORDER BY movieID ASC")
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
	if got := fmt.Sprint(res.Rows[0]["movieID"]); got != "3" {
		t.Fatalf("unexpected movieID: %#v", got)
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

func TestExecuteOptionalMatchKeepsBoundVertexOnMiss(t *testing.T) {
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
		t.Fatalf("expected bound vertex a to be preserved on OPTIONAL miss, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["r"] != nil {
		t.Fatalf("expected r to be nil on OPTIONAL miss, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["b"] != nil {
		t.Fatalf("expected b to be nil on OPTIONAL miss, row=%#v", res.Rows[0])
	}
}

func TestExecuteMatchDoesNotScanFromNilBoundVertex(t *testing.T) {
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

	stmt, err := parser.ParseStatement("OPTIONAL MATCH (a:Missing) WITH a MATCH (a)-->(b) RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no rows when MATCH input binding is nil, got %#v", res.Rows)
	}
}

func TestExecuteOptionalMatchKeepsBoundRelationshipOnReverseMiss(t *testing.T) {
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

	stmt, err := parser.ParseStatement("MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["a1"] == nil {
		t.Fatalf("expected a1 to remain bound, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["r"] == nil {
		t.Fatalf("expected r to remain bound, row=%#v", res.Rows[0])
	}
	if res.Rows[0]["b2"] != nil {
		t.Fatalf("expected b2 to be nil on reverse OPTIONAL miss, row=%#v", res.Rows[0])
	}
}

func TestExecuteMatchAfterOptionalMatchWithNullCoalesceReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "s1", Labels: []string{"Single"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "x2", Labels: []string{"B"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "x1", DstID: "x2"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:NonExistent) OPTIONAL MATCH (a)-->(c:NonExistent) WITH coalesce(b, c) AS x MATCH (x)-->(d) RETURN d")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no rows when coalesce result is null, got %#v", res.Rows)
	}
}

func TestExecuteOptionalTwoHopChainWithBoundEndpointReturnsNullOnMiss(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Labels: []string{"C"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a1", DstID: "c1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["b"]; got != nil {
		t.Fatalf("expected b=nil when the second hop misses, got %#v", got)
	}
}

func TestExecuteOptionalMatchWhereFilterPreservesNullRow(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"num": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"num": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Labels: []string{"C"}, Properties: graph.PropertyMap{"num": []byte("3")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r1", Type: "REL", SrcID: "a1", DstID: "b1", Properties: graph.PropertyMap{"name": []byte("r1")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r2", Type: "REL", SrcID: "b1", DstID: "c1", Properties: graph.PropertyMap{"name": []byte("r2")}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[r:REL]->(b) OPTIONAL MATCH (b)-[r2:REL]->(c) WHERE r = r2 RETURN a, b, c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(res.Rows), res.Rows)
	}
	for _, row := range res.Rows {
		if got := row["a"]; got == nil {
			t.Fatalf("expected a to remain bound, row=%#v", row)
		}
		if got := row["b"]; got == nil {
			t.Fatalf("expected b to remain bound, row=%#v", row)
		}
		if got := row["c"]; got != nil {
			t.Fatalf("expected c=nil when the OPTIONAL candidate is filtered out, got %#v in row %#v", got, row)
		}
	}
}

func TestExecuteOptionalUndirectedMatchPreservesReverseRowWhenSameEdgeExcluded(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"num": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"num": []byte("2")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c1", Labels: []string{"C"}, Properties: graph.PropertyMap{"num": []byte("3")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r1", Type: "REL", SrcID: "a1", DstID: "b1", Properties: graph.PropertyMap{"name": []byte("r1")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r2", Type: "REL", SrcID: "b1", DstID: "c1", Properties: graph.PropertyMap{"name": []byte("r2")}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, b, c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(res.Rows), res.Rows)
	}
	seenMatch := false
	seenReverseNull := false
	for _, row := range res.Rows {
		aMap, ok := row["a"].(map[string]any)
		if !ok {
			t.Fatalf("expected a vertex map, got %T", row["a"])
		}
		bMap, ok := row["b"].(map[string]any)
		if !ok {
			t.Fatalf("expected b vertex map, got %T", row["b"])
		}
		aID, _ := aMap["id"].(string)
		bID, _ := bMap["id"].(string)
		if aID == "a1" && bID == "b1" {
			seenMatch = true
		}
		if aID == "b1" && bID == "a1" && row["c"] == nil {
			seenReverseNull = true
		}
	}
	if !seenMatch || !seenReverseNull {
		t.Fatalf("expected matched row and reverse preserved null row, got %#v", res.Rows)
	}
}

func TestExecuteOptionalVariableLengthAfterNullBoundEndpointReturnsNullRow(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A) OPTIONAL MATCH (a)-[:FOO]->(b:B) OPTIONAL MATCH (b)<-[:BAR*]-(c:B) RETURN a, b, c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["a"]; got == nil {
		t.Fatalf("expected a to remain bound, row=%#v", res.Rows[0])
	}
	if got := res.Rows[0]["b"]; got != nil {
		t.Fatalf("expected b=nil after first OPTIONAL miss, got %#v", got)
	}
	if got := res.Rows[0]["c"]; got != nil {
		t.Fatalf("expected c=nil after var-length OPTIONAL consumes nil-bound b, got %#v", got)
	}
}

func TestExecuteOptionalVariableLengthWhereNullPreservesBoundEndpoint(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A), (b:B) OPTIONAL MATCH (a)-[r*]-(b) WHERE r IS NULL AND a <> b RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["b"]; got == nil {
		t.Fatalf("expected bound endpoint b to be preserved, row=%#v", res.Rows[0])
	}
}

func TestExecuteBoundRelationshipInsideZeroLengthFlanksMatches(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "EDGE", SrcID: "a", DstID: "b"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()-[r:EDGE]-() MATCH p = (n)-[*0..0]-()-[r]-()-[*0..0]-(m) RETURN count(p) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["c"]); got == "0" {
		t.Fatalf("expected bound relationship pattern to match, got count 0 with rows %#v", res.Rows)
	}
}

func TestExecuteMixedDirectionVarLengthDoesNotEmitShorterPrefixes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}, Properties: graph.PropertyMap{"name": []byte("a")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Properties: graph.PropertyMap{"name": []byte("b")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c", Properties: graph.PropertyMap{"name": []byte("c")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "d", Properties: graph.PropertyMap{"name": []byte("d")}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "LIKES", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "LIKES", SrcID: "c", DstID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e3", Type: "LIKES", SrcID: "d", DstID: "c"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:A) MATCH (a)-[:LIKES]->()<-[:LIKES*2]-(c) RETURN c.name AS name")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["name"]); got != "d" {
		t.Fatalf("expected only terminal two-hop predecessor d, got %q with rows %#v", got, res.Rows)
	}
}

func TestExecuteDisconnectedMatchCartesianExpansion(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
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
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "T", SrcID: "a", DstID: "c"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-->(b) MATCH (c)-->(d) RETURN a, b, c, d")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 cartesian rows, got %d: %#v", len(res.Rows), res.Rows)
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

func TestExecuteDeleteTypedRelationshipOnlyRemovesTargetType(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p2", Labels: []string{"Person"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "sf1", Type: "SUGGESTED_FRIEND", SrcID: "p1", DstID: "p2"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "p1", DstID: "p2"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (:Person)-[sf:SUGGESTED_FRIEND]->() DELETE sf")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute delete failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		if _, err := tx.GetEdge(ctx, "acme", "sf1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return errUnexpected("expected typed edge to be deleted")
		}
		edge, err := tx.GetEdge(ctx, "acme", "k1")
		if err != nil {
			return err
		}
		if edge == nil || edge.Type != "KNOWS" {
			return errUnexpected("expected non-target edge type to remain")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestExecuteTwoHopDistinctCreateRecommendationEdges(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		for _, id := range []string{"p1", "p2", "p3"} {
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: id, Labels: []string{"Person"}}); err != nil {
				return err
			}
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "p1", DstID: "p2"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "p3", DstID: "p2"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person) WHERE a <> b AND NOT (a)-[:SUGGESTED_FRIEND]-(b) WITH DISTINCT a, b CREATE (a)-[:SUGGESTED_FRIEND]->(b)")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute create failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		countP1 := 0
		if err := tx.ScanOutEdges(ctx, "acme", "p1", "SUGGESTED_FRIEND", 10, func(edge *graph.Edge) error {
			if edge != nil && edge.DstID == "p3" {
				countP1++
			}
			return nil
		}); err != nil {
			return err
		}
		countP3 := 0
		if err := tx.ScanOutEdges(ctx, "acme", "p3", "SUGGESTED_FRIEND", 10, func(edge *graph.Edge) error {
			if edge != nil && edge.DstID == "p1" {
				countP3++
			}
			return nil
		}); err != nil {
			return err
		}
		if countP1 != 1 || countP3 != 1 {
			return errUnexpected("expected one suggested-friend edge in each direction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestExecuteTypedCollectDistinctReturnRecommendations(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Alice")}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Bob")}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Cora")}},
			{Tenant: "acme", ID: "p4", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Drew")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "p1", DstID: "p3"},
			{Tenant: "acme", ID: "k3", Type: "KNOWS", SrcID: "p1", DstID: "p3"},
			{Tenant: "acme", ID: "k4", Type: "KNOWS", SrcID: "p2", DstID: "p3"},
			{Tenant: "acme", ID: "k5", Type: "KNOWS", SrcID: "p2", DstID: "p4"},
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

	stmt, err := parser.ParseStatement("MATCH (src:Person)-[:KNOWS]->(dst:Person) RETURN src.name AS person, collect(DISTINCT dst.name) AS suggested ORDER BY person")
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

	toStringSet := func(raw any) map[string]struct{} {
		out := map[string]struct{}{}
		switch typed := raw.(type) {
		case []any:
			for _, item := range typed {
				text, ok := item.(string)
				if !ok {
					t.Fatalf("expected collected value to be string, got %#v", item)
				}
				out[text] = struct{}{}
			}
		case []string:
			for _, item := range typed {
				out[item] = struct{}{}
			}
		default:
			t.Fatalf("expected collected list, got %#v", raw)
		}
		return out
	}

	people := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		person, _ := row["person"].(string)
		people = append(people, person)
		suggested := toStringSet(row["suggested"])
		switch person {
		case "Alice":
			if len(suggested) != 2 {
				t.Fatalf("expected two distinct suggestions for Alice, got %#v", row["suggested"])
			}
			if _, ok := suggested["Bob"]; !ok {
				t.Fatalf("missing Bob suggestion for Alice: %#v", row["suggested"])
			}
			if _, ok := suggested["Cora"]; !ok {
				t.Fatalf("missing Cora suggestion for Alice: %#v", row["suggested"])
			}
		case "Bob":
			if len(suggested) != 2 {
				t.Fatalf("expected two distinct suggestions for Bob, got %#v", row["suggested"])
			}
			if _, ok := suggested["Cora"]; !ok {
				t.Fatalf("missing Cora suggestion for Bob: %#v", row["suggested"])
			}
			if _, ok := suggested["Drew"]; !ok {
				t.Fatalf("missing Drew suggestion for Bob: %#v", row["suggested"])
			}
		default:
			t.Fatalf("unexpected person row: %#v", row)
		}
	}
	if !reflect.DeepEqual(people, []string{"Alice", "Bob"}) {
		t.Fatalf("unexpected ordering: %#v", people)
	}
}

func TestExecutePrintSuggestedFriendsCollectDistinctFastPath(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Alice")}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Bob")}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Cora")}},
			{Tenant: "acme", ID: "p4", Labels: []string{"Movie"}, Properties: map[string][]byte{"name": []byte("NotAPerson")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "s1", Type: "SUGGESTED_FRIEND", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "s2", Type: "SUGGESTED_FRIEND", SrcID: "p1", DstID: "p3"},
			{Tenant: "acme", ID: "s3", Type: "SUGGESTED_FRIEND", SrcID: "p1", DstID: "p3"},
			{Tenant: "acme", ID: "s4", Type: "SUGGESTED_FRIEND", SrcID: "p2", DstID: "p3"},
			{Tenant: "acme", ID: "s5", Type: "SUGGESTED_FRIEND", SrcID: "p2", DstID: "p4"},
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

	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:SUGGESTED_FRIEND]->(suggested:Person) RETURN a.name AS person, collect(DISTINCT suggested.name) AS suggested_friends ORDER BY person")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "emit_runtime_counters": true})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	toStringSet := func(raw any) map[string]struct{} {
		out := map[string]struct{}{}
		switch typed := raw.(type) {
		case []any:
			for _, item := range typed {
				text, ok := item.(string)
				if !ok {
					t.Fatalf("expected collected value to be string, got %#v", item)
				}
				out[text] = struct{}{}
			}
		case []string:
			for _, item := range typed {
				out[item] = struct{}{}
			}
		default:
			t.Fatalf("expected collected list, got %#v", raw)
		}
		return out
	}

	people := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		person, _ := row["person"].(string)
		people = append(people, person)
		suggested := toStringSet(row["suggested_friends"])
		switch person {
		case "Alice":
			if len(suggested) != 2 {
				t.Fatalf("expected two distinct suggestions for Alice, got %#v", row["suggested_friends"])
			}
			if _, ok := suggested["Bob"]; !ok {
				t.Fatalf("missing Bob suggestion for Alice: %#v", row["suggested_friends"])
			}
			if _, ok := suggested["Cora"]; !ok {
				t.Fatalf("missing Cora suggestion for Alice: %#v", row["suggested_friends"])
			}
		case "Bob":
			if len(suggested) != 1 {
				t.Fatalf("expected one distinct suggestion for Bob, got %#v", row["suggested_friends"])
			}
			if _, ok := suggested["Cora"]; !ok {
				t.Fatalf("missing Cora suggestion for Bob: %#v", row["suggested_friends"])
			}
		default:
			t.Fatalf("unexpected person row: %#v", row)
		}
	}
	if !reflect.DeepEqual(people, []string{"Alice", "Bob"}) {
		t.Fatalf("unexpected ordering: %#v", people)
	}

	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("decode runtime counters failed: %v", err)
	}
	if counters["runtime.suggested_friends.print.fastpath_applied"] <= 0 {
		t.Fatalf("expected print fast path counter > 0, counters=%v", counters)
	}
	if counters["fast_path.collect_distinct.typed_scalar_key_used"] <= 0 {
		t.Fatalf("expected typed scalar distinct key counter > 0, counters=%v", counters)
	}
	if counters["fast_path.collect_distinct.typed_scalar_property_extract"]+counters["fast_path.collect_distinct.typed_scalar_property_extract_fallback"] <= 0 {
		t.Fatalf("expected typed scalar extract path accounting > 0, counters=%v", counters)
	}
}

func TestExecuteCollectDistinctSamePropertyFastPathGeneralShape(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("amy")}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("ben")}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("cai")}},
			{Tenant: "acme", ID: "u4", Labels: []string{"Team"}, Properties: map[string][]byte{"handle": []byte("team-only")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "u1", DstID: "u3"},
			{Tenant: "acme", ID: "k3", Type: "KNOWS", SrcID: "u1", DstID: "u3"},
			{Tenant: "acme", ID: "k4", Type: "KNOWS", SrcID: "u2", DstID: "u3"},
			{Tenant: "acme", ID: "k5", Type: "KNOWS", SrcID: "u2", DstID: "u4"},
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

	stmt, err := parser.ParseStatement("MATCH (src:User)-[:KNOWS]->(dst:User) RETURN src.handle AS person, collect(DISTINCT dst.handle) AS suggested ORDER BY person")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "emit_runtime_counters": true})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	toStringSet := func(raw any) map[string]struct{} {
		out := map[string]struct{}{}
		switch typed := raw.(type) {
		case []any:
			for _, item := range typed {
				text, ok := item.(string)
				if !ok {
					t.Fatalf("expected collected value to be string, got %#v", item)
				}
				out[text] = struct{}{}
			}
		case []string:
			for _, item := range typed {
				out[item] = struct{}{}
			}
		default:
			t.Fatalf("expected collected list, got %#v", raw)
		}
		return out
	}

	people := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		person, _ := row["person"].(string)
		people = append(people, person)
		suggested := toStringSet(row["suggested"])
		switch person {
		case "amy":
			if len(suggested) != 2 {
				t.Fatalf("expected two distinct suggestions for amy, got %#v", row["suggested"])
			}
			if _, ok := suggested["ben"]; !ok {
				t.Fatalf("missing ben suggestion for amy: %#v", row["suggested"])
			}
			if _, ok := suggested["cai"]; !ok {
				t.Fatalf("missing cai suggestion for amy: %#v", row["suggested"])
			}
		case "ben":
			if len(suggested) != 1 {
				t.Fatalf("expected one distinct suggestion for ben, got %#v", row["suggested"])
			}
			if _, ok := suggested["cai"]; !ok {
				t.Fatalf("missing cai suggestion for ben: %#v", row["suggested"])
			}
		default:
			t.Fatalf("unexpected person row: %#v", row)
		}
	}
	if !reflect.DeepEqual(people, []string{"amy", "ben"}) {
		t.Fatalf("unexpected ordering: %#v", people)
	}

	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("decode runtime counters failed: %v", err)
	}
	if counters["runtime.collect_distinct_same_property.fastpath_applied"] <= 0 {
		t.Fatalf("expected generalized fast path counter > 0, counters=%v", counters)
	}
	if counters["fast_path.collect_distinct.typed_scalar_key_used"] <= 0 {
		t.Fatalf("expected typed scalar distinct key counter > 0, counters=%v", counters)
	}
	if counters["fast_path.collect_distinct.typed_scalar_property_extract"]+counters["fast_path.collect_distinct.typed_scalar_property_extract_fallback"] <= 0 {
		t.Fatalf("expected typed scalar extract path accounting > 0, counters=%v", counters)
	}
}

func TestExecuteDistinctOrderByEmitsOperatorTypedCounters(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "u1", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("amy")}},
			{Tenant: "acme", ID: "u2", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("ben")}},
			{Tenant: "acme", ID: "u3", Labels: []string{"User"}, Properties: map[string][]byte{"handle": []byte("cai")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "u1", DstID: "u2"},
			{Tenant: "acme", ID: "k3", Type: "KNOWS", SrcID: "u1", DstID: "u3"},
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

	stmt, err := parser.ParseStatement("MATCH (src:User)-[:KNOWS]->(dst:User) RETURN DISTINCT src.handle AS person, dst.handle AS suggested ORDER BY person, suggested")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "emit_runtime_counters": true})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %#v", res.Rows)
	}
	counters, err := runtimeCountersFromWarnings(res.Warnings)
	if err != nil {
		t.Fatalf("decode runtime counters failed: %v", err)
	}
	if counters["runtime.operator.project.distinct.row_key.typed"] <= 0 {
		t.Fatalf("expected typed project distinct row key counter > 0, counters=%v", counters)
	}
	if counters["runtime.operator.sort.scalar_compare.typed"] <= 0 {
		t.Fatalf("expected typed sort compare counter > 0, counters=%v", counters)
	}
}

func TestExecuteCollectReturnPreservesDuplicatesWithoutDistinct(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		vertexes := []*graph.Vertex{
			{Tenant: "acme", ID: "p1", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Alice")}},
			{Tenant: "acme", ID: "p2", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Bob")}},
			{Tenant: "acme", ID: "p3", Labels: []string{"Person"}, Properties: map[string][]byte{"name": []byte("Cora")}},
		}
		for _, vertex := range vertexes {
			if err := tx.PutVertex(ctx, vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "k1", Type: "KNOWS", SrcID: "p1", DstID: "p2"},
			{Tenant: "acme", ID: "k2", Type: "KNOWS", SrcID: "p1", DstID: "p3"},
			{Tenant: "acme", ID: "k3", Type: "KNOWS", SrcID: "p1", DstID: "p3"},
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

	stmt, err := parser.ParseStatement("MATCH (src:Person)-[:KNOWS]->(dst:Person) RETURN src.name AS person, collect(dst.name) AS suggested ORDER BY person")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one grouped row, got %d", len(res.Rows))
	}
	if person, _ := res.Rows[0]["person"].(string); person != "Alice" {
		t.Fatalf("unexpected grouped person: %#v", res.Rows[0]["person"])
	}

	collected, ok := res.Rows[0]["suggested"].([]any)
	if !ok {
		t.Fatalf("expected collected values as []any, got %#v", res.Rows[0]["suggested"])
	}
	values := make([]string, 0, len(collected))
	for _, item := range collected {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("expected collected value to be string, got %#v", item)
		}
		values = append(values, text)
	}
	if !reflect.DeepEqual(values, []string{"Bob", "Cora", "Cora"}) {
		t.Fatalf("expected duplicate suggestion to be preserved, got %#v", values)
	}
}

func TestExecuteSetVertexLabels(t *testing.T) {
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

func TestExecuteSetVertexLabelIgnoresNullBinding(t *testing.T) {
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

	// Create two vertexes: a "person" with role and a "dept" with two phone fields.
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
		"CREATE (n:Vertex {id:'n1', score:42})")
	if err != nil {
		t.Fatalf("setup parse failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement(
		"MATCH (n:Vertex {id:'n1'}) " +
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
		"CREATE (n:Vertex {id:'n2', flag:true})")
	if err != nil {
		t.Fatalf("setup parse failed: %v", err)
	}
	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	stmt, err := parser.ParseStatement(
		"MATCH (n:Vertex {id:'n2'}) " +
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

	verifyStmt, err := parser.ParseStatement("MATCH ()-[r:TYPE]->() RETURN r.name AS name, keys(r) AS keys, r['name'] AS byIndex, [key IN keys(r) | key + '->' + r[key]] AS keyValue")
	if err != nil {
		t.Fatalf("parse verify failed: %v", err)
	}
	verifyRes, err := exec.ExecuteStatement(ctx, verifyStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("verify execute failed: %v", err)
	}
	if len(verifyRes.Rows) != 1 {
		t.Fatalf("expected one verification row, got %#v", verifyRes.Rows)
	}
	if got := strings.TrimSpace(fmt.Sprint(verifyRes.Rows[0]["name"])); got != "foo" {
		t.Fatalf("expected r.name foo, got %#v (row=%#v)", verifyRes.Rows[0]["name"], verifyRes.Rows[0])
	}
	if got := strings.TrimSpace(fmt.Sprint(verifyRes.Rows[0]["byIndex"])); got != "foo" {
		t.Fatalf("expected r['name'] foo, got %#v (row=%#v)", verifyRes.Rows[0]["byIndex"], verifyRes.Rows[0])
	}
	keyValue, ok := verifyRes.Rows[0]["keyValue"].([]any)
	if !ok || len(keyValue) != 1 || strings.TrimSpace(fmt.Sprint(keyValue[0])) != "name->foo" {
		t.Fatalf("expected keyValue [name->foo], got %#v (row=%#v)", verifyRes.Rows[0]["keyValue"], verifyRes.Rows[0])
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

func TestExecuteMergeMatchAnonymousVertexPattern(t *testing.T) {
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
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 rows from undirected orientation expansion, got %d", len(res.Rows))
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
	totalVertexes := 0
	if err := store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, "acme", 0, func(vertex *graph.Vertex) error {
			totalVertexes++
			for _, label := range vertex.Labels {
				labelCounts[label]++
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if totalVertexes != 3 {
		t.Fatalf("expected 3 total vertexes after merge, got %d", totalVertexes)
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

func TestExecuteCreateEdgeWriteOnlyDerivesAnonymousEndpointIDsFromProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})-[:KNOWS]->(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "src": "u-legacy-1", "dst": "u-legacy-2"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u-legacy-1")
		if err != nil {
			return err
		}
		if left == nil {
			return errUnexpected("expected left vertex with property-derived id")
		}
		right, err := tx.GetVertex(ctx, "acme", "u-legacy-2")
		if err != nil {
			return err
		}
		if right == nil {
			return errUnexpected("expected right vertex with property-derived id")
		}
		hasEdge, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u-legacy-1", "u-legacy-2", "KNOWS")
		if err != nil {
			return err
		}
		if !hasEdge {
			return errUnexpected("expected directed KNOWS edge between property-derived endpoint ids")
		}
		return nil
	}); err != nil {
		t.Fatalf("verification failed: %v", err)
	}
}

func TestExecuteCreateReverseEdgeWriteOnlyDerivesAnonymousEndpointIDsFromProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	stmt, err := parser.ParseStatement("CREATE (:User {id:$src})<-[:KNOWS]-(:User {id:$dst})")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	if _, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "src": "u-legacy-3", "dst": "u-legacy-4"}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if err := store.View(ctx, func(tx graph.Tx) error {
		left, err := tx.GetVertex(ctx, "acme", "u-legacy-3")
		if err != nil {
			return err
		}
		if left == nil {
			return errUnexpected("expected left vertex with property-derived id")
		}
		right, err := tx.GetVertex(ctx, "acme", "u-legacy-4")
		if err != nil {
			return err
		}
		if right == nil {
			return errUnexpected("expected right vertex with property-derived id")
		}
		hasEdge, err := tx.HasDirectedEdgeBetween(ctx, "acme", "u-legacy-4", "u-legacy-3", "KNOWS")
		if err != nil {
			return err
		}
		if !hasEdge {
			return errUnexpected("expected reverse directed KNOWS edge between property-derived endpoint ids")
		}
		return nil
	}); err != nil {
		t.Fatalf("verification failed: %v", err)
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

func TestExecuteMatchAllVertexesReturnBinding(t *testing.T) {
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

	vertex, ok := res.Rows[0]["n"].(map[string]any)
	if !ok {
		t.Fatalf("expected projected vertex map, got %T", res.Rows[0]["n"])
	}
	props, ok := vertex["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected projected vertex properties map, got %T", vertex["properties"])
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

	stmt, err := parser.ParseStatement("MATCH (n:!Movie) RETURN id(n) AS id")
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

	stmt, err := parser.ParseStatement("MATCH (:Person {name: 'Oliver Stone'})--(n) RETURN n AS connectedVertexes")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 connected vertexes, got %d", len(res.Rows))
	}

	ids := map[string]bool{}
	for _, row := range res.Rows {
		vertex, ok := row["connectedVertexes"].(map[string]any)
		if !ok {
			t.Fatalf("expected connectedVertexes map, got %T", row["connectedVertexes"])
		}
		id, _ := vertex["id"].(string)
		ids[id] = true
	}
	if !ids["m1"] || !ids["d1"] || len(ids) != 2 {
		t.Fatalf("unexpected connected vertex ids: %#v", ids)
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

	stmt, err := parser.ParseStatement("MATCH (person:Person {name: 'Oliver Stone'})--(n) RETURN n AS connectedVertexes")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 connected vertexes, got %d", len(res.Rows))
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
			t.Fatalf("expected a to be a vertex map, got %T", row["a"])
		}
		b, ok := row["b"].(map[string]any)
		if !ok {
			t.Fatalf("expected b to be a vertex map, got %T", row["b"])
		}
		aID, _ := a["id"].(string)
		bID, _ := b["id"].(string)
		pairCounts[aID+"->"+bID]++
	}
	if pairCounts["p-charlie->m-wall"] != 1 || pairCounts["m-wall->p-charlie"] != 1 || len(pairCounts) != 2 {
		t.Fatalf("unexpected undirected bindings: %#v", pairCounts)
	}
}

func TestExecuteMatchUndirectedRelationshipWithBoundEdgeVarAndProperties(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}, Properties: graph.PropertyMap{"num": []byte("1")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b1", Labels: []string{"B"}, Properties: graph.PropertyMap{"num": []byte("2")}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r1", Type: "REL", SrcID: "a1", DstID: "b1", Properties: graph.PropertyMap{"name": []byte("r1")}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[r {name: 'r1'}]-(b) RETURN a, r, b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows for both undirected orientations, got %d: %#v", len(res.Rows), res.Rows)
	}
	pairCounts := map[string]int{}
	for _, row := range res.Rows {
		a, ok := row["a"].(map[string]any)
		if !ok {
			t.Fatalf("expected a to be a vertex map, got %T", row["a"])
		}
		b, ok := row["b"].(map[string]any)
		if !ok {
			t.Fatalf("expected b to be a vertex map, got %T", row["b"])
		}
		if row["r"] == nil {
			t.Fatalf("expected r to remain bound in every row, got %#v", row)
		}
		aID, _ := a["id"].(string)
		bID, _ := b["id"].(string)
		pairCounts[aID+"->"+bID]++
	}
	if pairCounts["a1->b1"] != 1 || pairCounts["b1->a1"] != 1 || len(pairCounts) != 2 {
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

func TestExecuteMatchRelationshipEdgeTypeAlternationWithDuplicates(t *testing.T) {
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

	stmt, err := parser.ParseStatement("MATCH (a)-[:T|:T]->(b) RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d: %#v", len(res.Rows), res.Rows)
	}
	binding, ok := res.Rows[0]["b"].(map[string]any)
	if !ok {
		t.Fatalf("expected b to be a vertex map, got %T", res.Rows[0]["b"])
	}
	if got := fmt.Sprint(binding["id"]); got != "b1" {
		t.Fatalf("expected b.id=b1, got %q", got)
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

func TestExecuteReturnThreeVertexNamedPath(t *testing.T) {
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

func TestExecuteCountUndirectedAnonymousRelationshipOrientations(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"B"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "T", SrcID: "a", DstID: "b"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	exec := New(store, Options{})
	stmt, err := parser.ParseStatement("MATCH ()--() RETURN count(*) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := fmt.Sprint(res.Rows[0]["c"]); got != "2" {
		t.Fatalf("expected undirected anonymous count 2 for both orientations, got %s", got)
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

func TestExecuteBuiltinProcedureVertexCount(t *testing.T) {
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
		{query: "CALL db.stats.vertexCount()", want: "4"},
		{query: "CALL db.stats.vertexCount('Movie')", want: "2"},
		{query: "CALL db.stats.vertexCount('Movie') YIELD vertexCount AS c", want: "2"},
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

		column := "vertexCount"
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
		t.Fatalf("path string missing expected vertexes: %q", p)
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

func TestExecuteOptionalTwoHopChainPreservesSecondEdgeBindingOnMiss(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "REL1", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "REL2", SrcID: "b", DstID: "c"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a)-[r1:REL1]->(b)-[r2:REL2]->(c)\nWITH a, r1, r2\n  LIMIT 1\nOPTIONAL MATCH (a)-[:NOPE]->()-[:NOPE]->()\nRETURN r1, r2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if res.Rows[0]["r1"] == nil {
		t.Fatalf("expected r1 to remain bound")
	}
	if res.Rows[0]["r2"] == nil {
		t.Fatalf("expected r2 to remain bound")
	}
}

func TestExecuteWithEdgeListAliasPreserved(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b"}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "c"}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e1", Type: "REL", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "e2", Type: "REL", SrcID: "b", DstID: "c"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()-[r1]->()-[r2]->()\nWITH [r1, r2] AS rs\n  LIMIT 1\nRETURN size(rs) AS n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %#v", len(res.Rows), res.Rows)
	}
	if got := res.Rows[0]["n"]; got != 2 {
		t.Fatalf("expected rs length 2, got %#v", got)
	}
	if !edgeSequenceBindingMatches(res.Rows[0], "rs", []*graph.Edge{{ID: "e1"}, {ID: "e2"}}) {
		t.Fatalf("expected rs binding to match the helper comparison")
	}
}

func TestExecuteWithAliasCarriesForwardIntoMatchJoin(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "begin", Labels: []string{"Begin"}, Properties: graph.PropertyMap{"num": []byte("42")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "end", Labels: []string{"End"}, Properties: graph.PropertyMap{"id": []byte("42"), "num": []byte("42")}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:Begin) WITH a.num AS property MATCH (b) WHERE b.id = property RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d: %#v", len(res.Rows), res.Rows)
	}
	b, ok := res.Rows[0]["b"].(map[string]any)
	if !ok {
		t.Fatalf("expected b to be a vertex map, got %T", res.Rows[0]["b"])
	}
	props, ok := b["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected vertex properties map, got %T", b["properties"])
	}
	if got := fmt.Sprint(props["id"]); got != "42" {
		t.Fatalf("expected joined vertex property id=42, got %#v", b)
	}
}

func TestExecuteWithGroupedAliasRetainsRelationshipBinding(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r1", Type: "T1", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "r2", Type: "T2", SrcID: "a", DstID: "b"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH ()-[r1]->(:B) WITH r1 AS r2, count(*) AS c MATCH ()-[r2]->() RETURN r2")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected at least one row, got %#v", res.Rows)
	}
	if got := fmt.Sprint(res.Rows[0]["r2"]); got == "" {
		t.Fatalf("expected retained relationship binding, got %#v", res.Rows[0])
	}
}

func TestExecuteWithForwardedScalarJoinUsesStoredIDProperty(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "begin", Labels: []string{"Begin"}, Properties: graph.PropertyMap{"num": []byte("42")}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "end", Labels: []string{"End"}, Properties: graph.PropertyMap{"id": []byte("42"), "num": []byte("7")}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a:Begin) WITH a.num AS property MATCH (b) WHERE b.id = property RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d: %#v", len(res.Rows), res.Rows)
	}
}

func TestExecuteCreatePatternCanReferencePriorPatternPropertyID(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})

	setup, err := parser.ParseStatement("CREATE (a:End {num: 42, id: 0}), (:End {num: 3}), (:Begin {num: a.id})")
	if err != nil {
		t.Fatalf("parse setup failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, setup, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("setup execute failed: %v", err)
	}

	seedProbe, err := parser.ParseStatement("MATCH (a:Begin) RETURN a.num AS num, a.id AS id")
	if err != nil {
		t.Fatalf("parse seed probe failed: %v", err)
	}
	seedRes, err := exec.ExecuteStatement(ctx, seedProbe, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("seed probe execute failed: %v", err)
	}
	if len(seedRes.Rows) != 1 {
		t.Fatalf("expected one seed row, got %d: %#v", len(seedRes.Rows), seedRes.Rows)
	}
	if got := fmt.Sprint(seedRes.Rows[0]["num"]); got != "0" {
		t.Fatalf("expected Begin.num to materialize as 0, got %#v", seedRes.Rows[0])
	}

	stmt, err := parser.ParseStatement("MATCH (a:Begin) WITH a.num AS property MATCH (b) WHERE b.id = property RETURN b")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one joined row, got %d: %#v", len(res.Rows), res.Rows)
	}
}

func TestExecuteComparisonLargeIntegerEqualityIsExact(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "n1", Labels: []string{"TheLabel"}, Properties: graph.PropertyMap{"id": []byte("4611686018427387905")}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (p:TheLabel) WHERE p.id = 4611686018427387900 RETURN p.id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("expected no rows for non-equal large integer comparison, got %#v", res.Rows)
	}
}

func TestExecuteComparisonNodeIdentityAfterWith(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"L"}}); err != nil {
			return err
		}
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"L"}})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (a) WITH a MATCH (b) WHERE a = b RETURN count(b) AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["c"] != 2 {
		t.Fatalf("expected count 2, got %#v", res.Rows)
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

func TestExecuteReturnNamedPathBidirectionalTwoHopPreservesBothOrientations(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a", Labels: []string{"A"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "b", Labels: []string{"B"}}); err != nil {
			return err
		}
		if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "t1", Type: "T1", SrcID: "a", DstID: "b"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{Tenant: "acme", ID: "t2", Type: "T2", SrcID: "b", DstID: "a"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH p=(n)<-->(k)<-->(n) RETURN p")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	exec := New(store, Options{})
	res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %#v", len(res.Rows), res.Rows)
	}
	paths := map[string]int{}
	for _, row := range res.Rows {
		paths[fmt.Sprint(row["p"])]++
	}
	if paths["<(:A)-[:T1]->(:B)-[:T2]->(:A)>"] != 1 {
		t.Fatalf("missing forward A/B/A path: %#v", paths)
	}
	if paths["<(:A)<-[:T2]-(:B)<-[:T1]-(:A)>"] != 1 {
		t.Fatalf("missing reverse A/B/A path: %#v", paths)
	}
	if paths["<(:B)-[:T2]->(:A)-[:T1]->(:B)>"] != 1 {
		t.Fatalf("missing forward B/A/B path: %#v", paths)
	}
	if paths["<(:B)<-[:T1]-(:A)<-[:T2]-(:B)>"] != 1 {
		t.Fatalf("missing reverse B/A/B path: %#v", paths)
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

func TestParseVertexPatternBareAndLabel(t *testing.T) {
	if _, err := parseVertexPattern("(n)"); err != nil {
		t.Fatalf("expected bare vertex pattern to parse: %v", err)
	}
	if _, err := parseVertexPattern("(n:User)"); err != nil {
		t.Fatalf("expected labeled vertex pattern to parse: %v", err)
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
		{expr: "valueType(n)", want: "VERTEX"},
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

func TestRuntimeProjectionItemsPreservePrecedenceExpressions(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	query := "RETURN 4 ^ 3 - 2 ^ 3 AS a, (4 ^ 3) - (2 ^ 3) AS b, 4 ^ (3 - 2) ^ 3 AS c"
	stmtAny, err := parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	queryStmt, ok := stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}
	_, physicalPlan, err := exec.buildRuntimePhysicalPlan(ctx, queryStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}
	var items []string
	for _, node := range physicalPlan.Nodes {
		if node.Op != "PHY_PROJECT" {
			continue
		}
		for _, raw := range runtimeStringSliceAttr(node.Attrs, "items") {
			items = append(items, raw)
		}
	}
	if len(items) == 0 {
		t.Fatalf("expected projection items, got plan=%#v", physicalPlan.Nodes)
	}
	expected := []string{"4 ^ 3 - 2 ^ 3 AS a", "(4 ^ 3) - (2 ^ 3) AS b", "4 ^ (3 - 2) ^ 3 AS c"}
	if !reflect.DeepEqual(items, expected) {
		t.Fatalf("unexpected projection items: got=%#v want=%#v", items, expected)
	}

	query = "RETURN null IS NULL <> null IS NOT NULL AS a, (null IS NULL) <> (null IS NOT NULL) AS b, (null IS NULL <> null) IS NOT NULL AS c"
	stmtAny, err = parser.ParseStatement(query)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	queryStmt, ok = stmtAny.(*ast.QueryStatement)
	if !ok {
		t.Fatalf("expected query statement, got %T", stmtAny)
	}
	_, physicalPlan, err = exec.buildRuntimePhysicalPlan(ctx, queryStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("build runtime physical plan failed: %v", err)
	}
	items = items[:0]
	for _, node := range physicalPlan.Nodes {
		if node.Op != "PHY_PROJECT" {
			continue
		}
		for _, raw := range runtimeStringSliceAttr(node.Attrs, "items") {
			items = append(items, raw)
		}
	}
	expected = []string{"null IS NULL <> null IS NOT NULL AS a", "(null IS NULL) <> (null IS NOT NULL) AS b", "(null IS NULL <> null) IS NOT NULL AS c"}
	if !reflect.DeepEqual(items, expected) {
		t.Fatalf("unexpected projection items: got=%#v want=%#v", items, expected)
	}
}

func TestExecuteReturnParenthesizedPrecedenceRegression(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	exec := New(store, Options{})

	stmt, err := parser.ParseStatement("RETURN 4 ^ 3 - 2 ^ 3 AS a, (4 ^ 3) - (2 ^ 3) AS b, 4 ^ (3 - 2) ^ 3 AS c")
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
	if got := fmt.Sprint(res.Rows[0]["a"]); got != "56.0" {
		t.Fatalf("unexpected a value: %q row=%#v", got, res.Rows[0])
	}
	if got := fmt.Sprint(res.Rows[0]["b"]); got != "56.0" {
		t.Fatalf("unexpected b value: %q row=%#v", got, res.Rows[0])
	}
	if got := fmt.Sprint(res.Rows[0]["c"]); got != "64.0" {
		t.Fatalf("unexpected c value: %q row=%#v", got, res.Rows[0])
	}

	stmt, err = parser.ParseStatement("RETURN null IS NULL <> null IS NOT NULL AS a, (null IS NULL) <> (null IS NOT NULL) AS b, (null IS NULL <> null) IS NOT NULL AS c")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	res, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected one row, got %d", len(res.Rows))
	}
	if got := fmt.Sprint(res.Rows[0]["a"]); got != "true" {
		t.Fatalf("unexpected a value: %q row=%#v", got, res.Rows[0])
	}
	if got := fmt.Sprint(res.Rows[0]["b"]); got != "true" {
		t.Fatalf("unexpected b value: %q row=%#v", got, res.Rows[0])
	}
	if got := fmt.Sprint(res.Rows[0]["c"]); got != "false" {
		t.Fatalf("unexpected c value: %q row=%#v", got, res.Rows[0])
	}
}

func runtimeStringSliceAttr(attrs map[string]any, key string) []string {
	raw, ok := attrs[key]
	if !ok || raw == nil {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
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

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "a1", Labels: []string{"A"}})
	})
	if err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	stmt, err = parser.ParseStatement("MATCH (n:A) DELETE n RETURN n.num")
	if err != nil {
		t.Fatalf("parse deleted property failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected deleted vertex property access error")
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		t.Fatalf("expected ErrKindNotFound, got: %v", err)
	}
}

func TestExecuteDeleteConnectedNodeRaisesConflict(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE (x:X) CREATE (x)-[:R]->() CREATE (x)-[:R]->() CREATE (x)-[:R]->()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}
	verifyStmt, err := parser.ParseStatement("MATCH (x:X)-[r:R]->() RETURN count(r) AS c")
	if err != nil {
		t.Fatalf("parse verify failed: %v", err)
	}
	verifyRes, err := exec.ExecuteStatement(ctx, verifyStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute verify failed: %v", err)
	}
	if len(verifyRes.Rows) != 1 || fmt.Sprint(verifyRes.Rows[0]["c"]) != "3" {
		t.Fatalf("expected seed to create 3 connected relationships from x, got %#v", verifyRes.Rows)
	}

	deleteStmt, err := parser.ParseStatement("MATCH (n:X) DELETE n")
	if err != nil {
		t.Fatalf("parse delete failed: %v", err)
	}
	_, err = exec.ExecuteStatement(ctx, deleteStmt, Params{"tenant": "acme"})
	if err == nil {
		t.Fatalf("expected connected-node delete to fail")
	}
	if !graph.IsKind(err, graph.ErrKindConflict) {
		t.Fatalf("expected conflict error, got %v", err)
	}
}

func TestExecuteDeleteRelationshipWithBidirectionalMatching(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seedStmt, err := parser.ParseStatement("CREATE ()-[:T {id: 42}]->()")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seedStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}
	verifyStmt, err := parser.ParseStatement("MATCH ()-[r:T]-() WHERE r.id = 42 RETURN count(r) AS c")
	if err != nil {
		t.Fatalf("parse verify failed: %v", err)
	}
	verifyRes, err := exec.ExecuteStatement(ctx, verifyStmt, Params{"tenant": "acme"})
	if err != nil {
		t.Fatalf("execute verify failed: %v", err)
	}
	if len(verifyRes.Rows) != 1 {
		t.Fatalf("expected one count row for pre-delete verification, got %#v", verifyRes.Rows)
	}
	if got := fmt.Sprint(verifyRes.Rows[0]["c"]); got != "1" && got != "2" {
		t.Fatalf("expected one or two undirected matches for r.id = 42 before delete, got %#v", verifyRes.Rows)
	}

	deleteStmt, err := parser.ParseStatement("MATCH p = ()-[r:T]-() WHERE r.id = 42 DELETE r")
	if err != nil {
		t.Fatalf("parse delete failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, deleteStmt, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute delete failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		snap, err := tx.GetStatsSnapshot(ctx, "acme")
		if err != nil {
			return err
		}
		if snap.EdgeTotal != 0 {
			return fmt.Errorf("expected no relationships after delete, got %d", snap.EdgeTotal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
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

func TestExecuteCreateAnonymousVertexesWithSameIDPropertyDoNotOverwrite(t *testing.T) {
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

	stmt, err = parser.ParseStatement("MATCH p=(a)-[r:REL]->(b) RETURN size(nOdEs(p)) AS nn, size(relationships(p)) AS nr, length(p) AS l, startNode(r).id AS s, endNode(r).id AS e, sign(-5) AS sn, sign(0) AS sz, sign(4) AS sp, last([1,2,3]) AS lv, last([]) AS le")
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

	stmt, err = parser.ParseStatement("MATCH (a:A) OPTIONAL MATCH p = (a)-[r]->() RETURN vertexes(p) AS np, relationships(p) AS rp, length(p) AS lp")
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

func TestExecuteAnonymousCreatePersistsVertex(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()

	exec := New(store, Options{})
	seed, err := parser.ParseStatement("CREATE (:A)")
	if err != nil {
		t.Fatalf("parse seed failed: %v", err)
	}
	if _, err := exec.ExecuteStatement(ctx, seed, Params{"tenant": "acme"}); err != nil {
		t.Fatalf("execute seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n) RETURN count(n) AS c")
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
	if got := res.Rows[0]["c"]; got != 1 {
		t.Fatalf("expected count=1, got %#v", got)
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
	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "p1"})
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	stmt, err := parser.ParseStatement("MATCH (n) RETURN percentileDisc(n.price, 1.1) AS p")
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

func TestEvalEdgeFieldTypedScalarEnvelope(t *testing.T) {
	tag, payload, err := typedvalue.Encode(int64(42))
	if err != nil {
		t.Fatalf("typedvalue encode failed: %v", err)
	}
	encoded := append([]byte{0xFF, 'T', 'V', 0x01, byte(tag)}, payload...)
	edge := &graph.Edge{Properties: graph.PropertyMap{"weight": encoded}}

	got, err := evalEdgeField(edge, "weight")
	if err != nil {
		t.Fatalf("evalEdgeField failed: %v", err)
	}
	if got != int64(42) {
		t.Fatalf("expected typed int64 decode, got %#v", got)
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

func (r *executorMetricsRecorder) ObserveRuntimeCounter(_ string, _ int64) {}
