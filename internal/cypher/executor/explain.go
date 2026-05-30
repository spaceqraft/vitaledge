package executor

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
)

type explainAnalysis struct {
	nodeCounts       []map[string]any
	edgeCounts       []map[string]any
	predicateSignals []map[string]any
	indexDecisions   []map[string]any
	cardinality      []map[string]any
	costEstimate     map[string]any
	runtimeStats     map[string]any
	warnings         []map[string]any
	source           string
	capturedAt       string
	statsSnapshot    map[string]any
}

type explainStoreStats struct {
	vertices      map[string]*graph.Vertex
	labelCounts   map[string]int
	edgeCounts    map[string]int
	vertexTotal   int
	edgeTotal     int
	snapshotFound bool
}

func (e *Executor) executeExplainStatement(ctx context.Context, stmt *ast.ExplainStatement, params Params) (*Result, error) {
	if stmt == nil || stmt.Statement == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "EXPLAIN requires an inner statement", nil)
	}

	analysis, err := e.buildExplainAnalysis(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}

	payload := e.buildExplainPayload(stmt, params, analysis)
	return &Result{
		Columns: []string{"explain"},
		Rows:    []Row{{"explain": payload}},
		Stats:   Stats{RowsReturned: 1},
	}, nil
}

func (e *Executor) buildExplainAnalysis(ctx context.Context, stmt ast.Statement, params Params) (*explainAnalysis, error) {
	analysis := &explainAnalysis{
		source:     "store-scan",
		capturedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	tenant := tenantFromParams(params)
	var stats explainStoreStats
	if err := e.store.View(ctx, func(tx graph.Tx) error {
		collected, err := collectExplainStoreStats(ctx, tx, tenant)
		if err != nil {
			return err
		}
		stats = collected
		return nil
	}); err != nil {
		return nil, err
	}
	analysis.source = explainStatsSnapshotSource(stats)
	analysis.statsSnapshot = buildExplainStatsSnapshotDetails(stats)

	analysis.nodeCounts = buildExplainNodeCounts(stats)
	analysis.edgeCounts = buildExplainEdgeCounts(stats)
	analysis.predicateSignals = buildExplainPredicateSignals(stmt, params, tenant, stats)
	planNodes := buildExplainPlanNodes(stmt, e.indexCatalog, tenant, params)
	analysis.indexDecisions = buildExplainIndexDecisions(stmt, params, e.indexCatalog, tenant, stats, planNodes)
	analysis.cardinality = buildExplainCardinalityFromPlanNodes(planNodes, params, stats)
	analysis.costEstimate = buildExplainCostEstimate(planNodes, analysis.cardinality, analysis.indexDecisions)
	analysis.warnings = buildExplainWarnings(stmt, analysis, planNodes, tenant)
	analysis.runtimeStats = buildExplainRuntimeStats(planNodes, analysis.cardinality, analysis.indexDecisions, analysis.warnings, stats)
	return analysis, nil
}

func (e *Executor) buildExplainPayload(stmt *ast.ExplainStatement, params Params, analysis *explainAnalysis) map[string]any {
	tenant := tenantFromParams(params)
	nodes := buildExplainPlanNodes(stmt.Statement, e.indexCatalog, tenant, params)
	rootNodeID := ""
	if len(nodes) > 0 {
		rootNodeID = nodes[len(nodes)-1]["id"].(string)
	}

	payload := map[string]any{
		"version": "v1",
		"query": map[string]any{
			"text":          explainedQueryText(stmt),
			"fingerprint":   explainFingerprint(stmt),
			"statementKind": string(stmt.Statement.Kind()),
			"tenant":        tenantFromParams(params),
			"params":        parameterNamesForStatement(stmt.Statement),
			"options":       buildExplainQueryOptions(stmt.Statement),
		},
		"summary": map[string]any{
			"dryRun":              true,
			"readOnly":            true,
			"writesDetected":      statementMayWrite(stmt.Statement),
			"semanticPhaseStatus": "ok",
			"planningPhaseStatus": "ok",
		},
		"logicalPlan": map[string]any{
			"rootNodeId": rootNodeID,
			"nodes":      nodes,
		},
		"physicalPlan": map[string]any{
			"rootNodeId": rootNodeID,
			"nodes":      nodes,
		},
		"influencers": map[string]any{
			"nodeCounts":       analysis.nodeCounts,
			"edgeCounts":       analysis.edgeCounts,
			"predicateSignals": analysis.predicateSignals,
			"statsSnapshot": map[string]any{
				"source":           analysis.source,
				"capturedAt":       analysis.capturedAt,
				"coverage":         analysis.statsSnapshot["coverage"],
				"completeness":     analysis.statsSnapshot["completeness"],
				"backfillStatus":   analysis.statsSnapshot["backfillStatus"],
				"backfillRequired": analysis.statsSnapshot["backfillRequired"],
			},
		},
		"cardinality":    analysis.cardinality,
		"costEstimate":   analysis.costEstimate,
		"runtimeStats":   analysis.runtimeStats,
		"indexDecisions": analysis.indexDecisions,
		"warnings":       analysis.warnings,
		"metadata": map[string]any{
			"transport": "json",
		},
	}

	return payload
}

func collectExplainStoreStats(ctx context.Context, tx graph.Tx, tenant string) (explainStoreStats, error) {
	stats := explainStoreStats{
		vertices:    map[string]*graph.Vertex{},
		labelCounts: map[string]int{},
		edgeCounts:  map[string]int{},
	}
	if strings.TrimSpace(tenant) == "" {
		return stats, nil
	}
	hasSnapshotTotals := false
	if snapshot, err := tx.GetStatsSnapshot(ctx, tenant); err == nil && snapshot != nil {
		stats.snapshotFound = true
		stats.vertexTotal = snapshot.VertexTotal
		stats.edgeTotal = snapshot.EdgeTotal
		for label, count := range snapshot.LabelCounts {
			label = strings.TrimSpace(label)
			if label == "" || count <= 0 {
				continue
			}
			stats.labelCounts[label] = count
		}
		for edgeType, count := range snapshot.EdgeCounts {
			edgeType = strings.TrimSpace(edgeType)
			if edgeType == "" || count <= 0 {
				continue
			}
			stats.edgeCounts[edgeType] = count
		}
		hasSnapshotTotals = true
	}

	if err := tx.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		clone := *v
		stats.vertices[v.ID] = &clone
		if !hasSnapshotTotals {
			stats.vertexTotal++
		}
		if !hasSnapshotTotals {
			if err := tx.ScanOutEdges(ctx, tenant, v.ID, "", 0, func(edge *graph.Edge) error {
				if edge == nil {
					return nil
				}
				stats.edgeTotal++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return explainStoreStats{}, err
	}

	return stats, nil
}

func buildExplainNodeCounts(stats explainStoreStats) []map[string]any {
	labels := make([]string, 0, len(stats.labelCounts))
	for label := range stats.labelCounts {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	out := make([]map[string]any, 0, len(labels))
	for _, label := range labels {
		out = append(out, map[string]any{
			"label":   label,
			"count":   stats.labelCounts[label],
			"quality": "exact",
		})
	}
	return out
}

func explainStatsSnapshotSource(stats explainStoreStats) string {
	if stats.snapshotFound {
		return "stats-snapshot+store-scan"
	}
	return "store-scan"
}

func buildExplainStatsSnapshotDetails(stats explainStoreStats) map[string]any {
	totalsCoverage := "scan_fallback"
	if stats.snapshotFound {
		totalsCoverage = "snapshot"
	}
	nodeCountsCoverage := "missing"
	if stats.snapshotFound {
		nodeCountsCoverage = "snapshot"
		if stats.vertexTotal > 0 && len(stats.labelCounts) == 0 {
			nodeCountsCoverage = "incomplete"
		}
	}
	edgeCountsCoverage := "missing"
	if stats.snapshotFound {
		edgeCountsCoverage = "snapshot"
		if stats.edgeTotal > 0 && len(stats.edgeCounts) == 0 {
			edgeCountsCoverage = "incomplete"
		}
	}

	completeness := "partial"
	if totalsCoverage == "snapshot" && nodeCountsCoverage == "snapshot" && edgeCountsCoverage == "snapshot" {
		completeness = "complete"
	}
	if totalsCoverage == "scan_fallback" && nodeCountsCoverage == "missing" && edgeCountsCoverage == "missing" {
		completeness = "missing"
	}
	backfillStatus := "required"
	if completeness == "complete" {
		backfillStatus = "complete"
	}

	return map[string]any{
		"coverage": map[string]any{
			"totals":     totalsCoverage,
			"nodeCounts": nodeCountsCoverage,
			"edgeCounts": edgeCountsCoverage,
		},
		"completeness":     completeness,
		"backfillStatus":   backfillStatus,
		"backfillRequired": backfillStatus != "complete",
	}
}

func buildExplainEdgeCounts(stats explainStoreStats) []map[string]any {
	types := make([]string, 0, len(stats.edgeCounts))
	for edgeType := range stats.edgeCounts {
		types = append(types, edgeType)
	}
	sort.Strings(types)

	out := make([]map[string]any, 0, len(types))
	for _, edgeType := range types {
		out = append(out, map[string]any{
			"type":      edgeType,
			"direction": "out",
			"count":     stats.edgeCounts[edgeType],
			"quality":   "exact",
		})
	}
	return out
}

func buildExplainPredicateSignals(stmt ast.Statement, params Params, tenant string, stats explainStoreStats) []map[string]any {
	signals := make([]map[string]any, 0)
	appendFromClause := func(nodeID string, pattern nodePattern, expression string) {
		matched := countMatchingVertices(stats, pattern, params)
		signals = append(signals, map[string]any{
			"expression":   expression,
			"matchedCount": matched,
			"quality":      "exact",
		})
		_ = nodeID
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return buildExplainPredicateSignals(s.Statement, params, tenant, stats)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			if pattern, ok := tryParseExplainNodePattern(match.Pattern); ok {
				if expr, ok := explainPatternExpression(pattern, params); ok {
					appendFromClause("", pattern, expr)
				}
			}
			if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				if expr, ok := explainAnchoredExpression(anchored, params); ok {
					appendFromClause("", anchored.asNodePattern(), expr)
				}
			}
		}
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				if pattern, ok := tryParseExplainNodePattern(clause.Raw); ok {
					if expr, ok := explainPatternExpression(pattern, params); ok {
						appendFromClause("", pattern, expr)
					}
				}
				if anchored, ok := tryParseExplainAnchoredPattern(clause.Raw); ok {
					if expr, ok := explainAnchoredExpression(anchored, params); ok {
						appendFromClause("", anchored.asNodePattern(), expr)
					}
				}
			}
		}
	}

	return signals
}

func buildExplainIndexDecisions(stmt ast.Statement, params Params, catalog IndexCatalog, tenant string, stats explainStoreStats, planNodes []map[string]any) []map[string]any {
	candidates := collectExplainIndexCandidates(stmt, params, stats)
	if len(candidates) == 0 {
		return nil
	}

	scanNodes := explainScanPlanNodes(planNodes)
	decisions := make([]map[string]any, 0, len(candidates))
	for i, candidate := range candidates {
		nodeID := candidate.NodeID
		accessPath := ""
		if i < len(scanNodes) {
			nodeID = scanNodes[i].ID
			accessPath = scanNodes[i].AccessPath
		}
		if strings.TrimSpace(nodeID) == "" {
			nodeID = fmt.Sprintf("N%d", i+1)
		}

		schema := strings.TrimSpace(candidate.Schema)
		property := strings.TrimSpace(candidate.Property)
		if schema == "" || property == "" {
			continue
		}

		selected := catalog != nil && catalog.HasPropertyIndex(tenant, schema, property)
		scanPopulation := explainSchemaPopulation(stats, schema)
		matchedCount := candidate.MatchedCount
		if matchedCount < 0 {
			matchedCount = 0
		}
		estimatedRowsSaved := scanPopulation - matchedCount
		if estimatedRowsSaved < 0 {
			estimatedRowsSaved = 0
		}
		estimatedSelectivity := 1.0
		if scanPopulation > 0 {
			estimatedSelectivity = float64(matchedCount) / float64(scanPopulation)
		}
		quality := strings.TrimSpace(candidate.Quality)
		if quality == "" {
			quality = "exact"
		}
		if quality != "exact" {
			estimatedSelectivity = 0.5
		}
		reason := "selected-property-index"
		recommendation := "keep-index"
		tuningImpact := "none"
		if !selected {
			reason = "missing-property-index"
			recommendation = "consider-index"
			tuningImpact = explainIndexTuningImpact(estimatedSelectivity)
			if quality != "exact" {
				tuningImpact = "medium"
			}
			switch tuningImpact {
			case "high":
				recommendation = "create-index"
			case "medium":
				recommendation = "consider-index"
			case "low":
				recommendation = "optional-index"
			}
		}

		decision := map[string]any{
			"nodeId":               nodeID,
			"schema":               schema,
			"property":             property,
			"candidate":            true,
			"selected":             selected,
			"reason":               reason,
			"estimatedSelectivity": estimatedSelectivity,
			"accessPath":           accessPath,
			"recommendation":       recommendation,
			"tuningImpact":         tuningImpact,
			"scanPopulation":       scanPopulation,
			"matchedCount":         matchedCount,
			"estimatedRowsSaved":   estimatedRowsSaved,
			"quality":              quality,
		}
		if !selected {
			decision["suggestedIndex"] = fmt.Sprintf("%s.%s", schema, property)
		}
		decisions = append(decisions, decision)
	}

	return decisions
}

type explainIndexCandidate struct {
	NodeID       string
	Schema       string
	Property     string
	MatchedCount int
	Quality      string
}

type explainScanPlanNode struct {
	ID         string
	AccessPath string
}

func collectExplainIndexCandidates(stmt ast.Statement, params Params, stats explainStoreStats) []explainIndexCandidate {
	candidates := make([]explainIndexCandidate, 0)
	appendNodePattern := func(schema string, props map[string]any, matchedCount int, quality string) {
		for property := range props {
			if strings.EqualFold(property, "id") {
				continue
			}
			candidates = append(candidates, explainIndexCandidate{
				Schema:       strings.TrimSpace(schema),
				Property:     strings.TrimSpace(property),
				MatchedCount: matchedCount,
				Quality:      quality,
			})
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return collectExplainIndexCandidates(s.Statement, params, stats)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			if pattern, ok := tryParseExplainNodePattern(match.Pattern); ok {
				props, err := explainPatternProperties(pattern.PropertiesRaw, params)
				if err == nil && len(props) > 0 {
					appendNodePattern(explainPatternSchema(pattern), props, countMatchingVertices(stats, pattern, params), "exact")
				} else if keys := explainPropertyKeys(pattern.PropertiesRaw); len(keys) > 0 {
					appendNodePattern(explainPatternSchema(pattern), keys, 0, "estimate")
				}
			}
			if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				props, err := explainPropertyMap(anchored.SourcePropertiesRaw, params)
				if err == nil && len(props) > 0 {
					appendNodePattern(anchored.SourceLabel, props, countAnchoredRows(stats, anchored, params), "exact")
				} else if keys := explainPropertyKeys(anchored.SourcePropertiesRaw); len(keys) > 0 {
					appendNodePattern(anchored.SourceLabel, keys, 0, "estimate")
				}
			}
		}
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				patternRaw := strings.TrimSpace(clause.Raw)
				if clause.Kind == ast.ClauseKindMatch {
					patternRaw = strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, "MATCH")))
				}
				if clause.Kind == ast.ClauseKindOptionalMatch {
					patternRaw = strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, "OPTIONAL MATCH")))
				}
				if pattern, ok := tryParseExplainNodePattern(patternRaw); ok {
					props, err := explainPatternProperties(pattern.PropertiesRaw, params)
					if err == nil && len(props) > 0 {
						appendNodePattern(explainPatternSchema(pattern), props, countMatchingVertices(stats, pattern, params), "exact")
					} else if keys := explainPropertyKeys(pattern.PropertiesRaw); len(keys) > 0 {
						appendNodePattern(explainPatternSchema(pattern), keys, 0, "estimate")
					}
				}
				if anchored, ok := tryParseExplainAnchoredPattern(patternRaw); ok {
					props, err := explainPropertyMap(anchored.SourcePropertiesRaw, params)
					if err == nil && len(props) > 0 {
						appendNodePattern(anchored.SourceLabel, props, countAnchoredRows(stats, anchored, params), "exact")
					} else if keys := explainPropertyKeys(anchored.SourcePropertiesRaw); len(keys) > 0 {
						appendNodePattern(anchored.SourceLabel, keys, 0, "estimate")
					}
				}
			}
		}
	}

	return candidates
}

func explainPropertyKeys(raw string) map[string]any {
	out := map[string]any{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out
	}
	for _, pair := range splitTopLevelCommaSeparated(raw) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		out[key] = true
	}
	return out
}

func explainScanPlanNodes(nodes []map[string]any) []explainScanPlanNode {
	out := make([]explainScanPlanNode, 0)
	for _, node := range nodes {
		op, _ := node["op"].(string)
		if !isScanOperator(op) {
			continue
		}
		id, _ := node["id"].(string)
		accessPath, _ := node["accessPath"].(string)
		out = append(out, explainScanPlanNode{ID: id, AccessPath: accessPath})
	}
	return out
}

func isScanOperator(op string) bool {
	switch op {
	case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN", "ALL_NODES_SCAN", "OPTIONAL_ALL_NODES_SCAN", "EDGE_SCAN", "OPTIONAL_EDGE_SCAN":
		return true
	default:
		return false
	}
}

func explainSchemaPopulation(stats explainStoreStats, schema string) int {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return stats.vertexTotal
	}
	if count, ok := stats.labelCounts[schema]; ok {
		return count
	}
	return stats.vertexTotal
}

func explainIndexTuningImpact(selectivity float64) string {
	if selectivity <= 0.10 {
		return "high"
	}
	if selectivity <= 0.30 {
		return "medium"
	}
	return "low"
}

func buildExplainCardinalityFromPlanNodes(nodes []map[string]any, params Params, stats explainStoreStats) []map[string]any {
	entries := make([]map[string]any, 0, len(nodes))
	rows := 1
	for _, node := range nodes {
		nodeID, _ := node["id"].(string)
		op, _ := node["op"].(string)
		rowsIn := rows
		rowsOut := rows
		quality := "estimate"
		switch op {
		case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN":
			quality = "exact"
			if predicate, _ := node["predicate"].(string); strings.TrimSpace(predicate) != "" {
				rowsOut = 1
			} else {
				rowsOut = rowsIn
			}
		case "ALL_NODES_SCAN", "OPTIONAL_ALL_NODES_SCAN":
			quality = "exact"
			rowsOut = stats.vertexTotal
			if op == "OPTIONAL_ALL_NODES_SCAN" && rowsOut == 0 {
				rowsOut = 1
			}
		case "AGGREGATE":
			if impl, _ := node["implementation"].(string); impl == "fast_node_count" {
				rowsOut = 1
				quality = "exact"
			}
		case "SKIP":
			if page, ok := node["pagination"].(map[string]any); ok {
				if skipRaw, ok := page["skip"].(string); ok {
					if skip, ok := explainPaginationValue(skipRaw, params); ok {
						rowsOut = rowsIn - skip
						if rowsOut < 0 {
							rowsOut = 0
						}
					}
				}
			}
		case "LIMIT":
			if page, ok := node["pagination"].(map[string]any); ok {
				if limitRaw, ok := page["limit"].(string); ok {
					if limit, ok := explainPaginationValue(limitRaw, params); ok && rowsOut > limit {
						rowsOut = limit
					}
				}
			}
		default:
			rowsOut = rowsIn
		}
		entries = append(entries, map[string]any{
			"nodeId":  nodeID,
			"rowsIn":  rowsIn,
			"rowsOut": rowsOut,
			"quality": quality,
		})
		rows = rowsOut
	}
	return entries
}

func buildExplainCostEstimate(planNodes []map[string]any, cardinality []map[string]any, indexDecisions []map[string]any) map[string]any {
	rowsOutByNode := map[string]int{}
	exactCardinality := len(cardinality) > 0
	for _, entry := range cardinality {
		nodeID, _ := entry["nodeId"].(string)
		if strings.TrimSpace(nodeID) == "" {
			continue
		}
		rowsOut, _ := entry["rowsOut"].(int)
		rowsOutByNode[nodeID] = rowsOut
		if quality, _ := entry["quality"].(string); quality != "exact" {
			exactCardinality = false
		}
	}

	scanRows := 0
	outputRows := 0
	for _, node := range planNodes {
		nodeID, _ := node["id"].(string)
		op, _ := node["op"].(string)
		rowsOut := rowsOutByNode[nodeID]
		if isScanOperator(op) {
			scanRows += rowsOut
		}
		outputRows = rowsOut
	}

	missingIndexPenalty := 0
	for _, decision := range indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			continue
		}
		impact, _ := decision["tuningImpact"].(string)
		switch impact {
		case "high":
			missingIndexPenalty += 100
		case "medium":
			missingIndexPenalty += 50
		case "low":
			missingIndexPenalty += 20
		default:
			missingIndexPenalty += 10
		}
	}

	total := scanRows + outputRows + missingIndexPenalty
	if total < 1 {
		total = 1
	}
	quality := "estimate"
	if exactCardinality {
		quality = "exact"
	}

	return map[string]any{
		"model":   "heuristic-v1",
		"value":   total,
		"unit":    "work_units",
		"quality": quality,
		"components": map[string]any{
			"scanRows":            scanRows,
			"outputRows":          outputRows,
			"missingIndexPenalty": missingIndexPenalty,
		},
	}
}

func buildExplainRuntimeStats(planNodes []map[string]any, cardinality []map[string]any, indexDecisions []map[string]any, warnings []map[string]any, stats explainStoreStats) map[string]any {
	scanNodes := 0
	filterNodes := 0
	projectionNodes := 0
	sortNodes := 0
	paginationNodes := 0
	writeNodes := 0

	for _, node := range planNodes {
		op, _ := node["op"].(string)
		if isScanOperator(op) {
			scanNodes++
		}
		switch op {
		case "FILTER":
			filterNodes++
		case "PROJECT", "AGGREGATE":
			projectionNodes++
		case "SORT":
			sortNodes++
		case "SKIP", "LIMIT":
			paginationNodes++
		case "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH_DELETE":
			writeNodes++
		}
	}

	indexCandidates := len(indexDecisions)
	indexSelected := 0
	for _, decision := range indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			indexSelected++
		}
	}

	rowsRead := 0
	rowsOutput := 0
	rowsOutByNode := map[string]int{}
	allExactCardinality := len(cardinality) > 0
	for _, entry := range cardinality {
		nodeID, _ := entry["nodeId"].(string)
		rowsIn, _ := entry["rowsIn"].(int)
		rowsOut, _ := entry["rowsOut"].(int)
		rowsRead += rowsIn
		rowsOutput = rowsOut
		if strings.TrimSpace(nodeID) != "" {
			rowsOutByNode[nodeID] = rowsOut
		}
		if quality, _ := entry["quality"].(string); quality != "exact" {
			allExactCardinality = false
		}
	}
	cardinalityQuality := "estimate"
	if allExactCardinality {
		cardinalityQuality = "exact"
	}

	verticesScanned := 0
	edgesScanned := 0
	for _, node := range planNodes {
		nodeID, _ := node["id"].(string)
		op, _ := node["op"].(string)
		rowsOut := rowsOutByNode[nodeID]
		switch op {
		case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN", "ALL_NODES_SCAN", "OPTIONAL_ALL_NODES_SCAN":
			verticesScanned += rowsOut
		case "EDGE_SCAN", "OPTIONAL_EDGE_SCAN":
			edgesScanned += rowsOut
		}
	}
	if verticesScanned == 0 && edgesScanned == 0 {
		verticesScanned = stats.vertexTotal
		edgesScanned = stats.edgeTotal
	}

	return map[string]any{
		"store": map[string]any{
			"verticesScanned": verticesScanned,
			"edgesScanned":    edgesScanned,
		},
		"plan": map[string]any{
			"totalNodes":      len(planNodes),
			"scanNodes":       scanNodes,
			"filterNodes":     filterNodes,
			"projectionNodes": projectionNodes,
			"sortNodes":       sortNodes,
			"paginationNodes": paginationNodes,
			"writeNodes":      writeNodes,
			"warningCount":    len(warnings),
		},
		"index": map[string]any{
			"candidates": indexCandidates,
			"selected":   indexSelected,
			"missing":    indexCandidates - indexSelected,
		},
		"cardinality": map[string]any{
			"rowsRead":   rowsRead,
			"rowsOutput": rowsOutput,
			"quality":    cardinalityQuality,
		},
	}
}

func buildExplainWarnings(stmt ast.Statement, analysis *explainAnalysis, planNodes []map[string]any, tenant string) []map[string]any {
	warnings := make([]map[string]any, 0, len(analysis.warnings)+4)
	seen := map[string]struct{}{}
	addWarning := func(w map[string]any) {
		code, _ := w["code"].(string)
		nodeID, _ := w["nodeId"].(string)
		key := code + "|" + nodeID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		warnings = append(warnings, w)
	}

	for _, existing := range analysis.warnings {
		addWarning(existing)
	}

	if statementMayWrite(stmt) {
		addWarning(map[string]any{
			"code":    "WRITE_QUERY_DRY_RUN",
			"message": "Write clauses detected; EXPLAIN did not execute mutations",
		})
	}

	if strings.TrimSpace(tenant) == "" {
		addWarning(map[string]any{
			"code":    "MISSING_TENANT_CONTEXT",
			"message": "No tenant parameter supplied; plan influencers are computed from an empty stats snapshot",
		})
	}

	for _, node := range planNodes {
		op, _ := node["op"].(string)
		nodeID, _ := node["id"].(string)
		if op == "ALL_NODES_SCAN" || op == "OPTIONAL_ALL_NODES_SCAN" {
			if explainAllNodesScanBackedByFastNodeCount(planNodes, nodeID) {
				continue
			}
			addWarning(map[string]any{
				"code":    "FULL_SCAN_FALLBACK",
				"message": "Planner selected an all-nodes scan access path",
				"nodeId":  nodeID,
			})
		}
	}

	for _, decision := range analysis.indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			continue
		}
		nodeID, _ := decision["nodeId"].(string)
		schema, _ := decision["schema"].(string)
		property, _ := decision["property"].(string)
		recommendation, _ := decision["recommendation"].(string)
		if recommendation == "" {
			recommendation = "consider-index"
		}
		addWarning(map[string]any{
			"code":    "MISSING_PROPERTY_INDEX",
			"message": fmt.Sprintf("No property index selected for %s.%s; recommendation=%s", schema, property, recommendation),
			"nodeId":  nodeID,
		})
		if quality, _ := decision["quality"].(string); quality == "estimate" {
			addWarning(map[string]any{
				"code":    "ESTIMATE_ONLY_INDEX_SIGNAL",
				"message": fmt.Sprintf("Index signal for %s.%s is estimate quality; bind parameters for exact selectivity", schema, property),
				"nodeId":  nodeID,
			})
		}
	}

	if len(warnings) == 0 {
		addWarning(map[string]any{
			"code":    "PLAN_ANALYSIS_PARTIAL",
			"message": "Plan analysis is partial for unsupported query shapes",
		})
	}
	return warnings
}

func explainAllNodesScanBackedByFastNodeCount(planNodes []map[string]any, nodeID string) bool {
	if strings.TrimSpace(nodeID) == "" {
		return false
	}
	for _, node := range planNodes {
		op, _ := node["op"].(string)
		if op != "AGGREGATE" {
			continue
		}
		impl, _ := node["implementation"].(string)
		if impl != "fast_node_count" {
			continue
		}
		children, _ := node["children"].([]string)
		for _, child := range children {
			if child == nodeID {
				return true
			}
		}
	}
	return false
}

func buildExplainQueryOptions(stmt ast.Statement) map[string]any {
	projectionClauses := explainQueryProjectionClauses(stmt)
	if len(projectionClauses) == 0 {
		return nil
	}

	options := map[string]any{}
	finalClause := projectionClauses[len(projectionClauses)-1]
	if finalClause.Projection != nil && finalClause.Projection.Distinct {
		options["distinct"] = true
	}
	if finalClause.Projection != nil && finalClause.Projection.IncludeAll {
		options["includeAll"] = true
	}
	if projection := finalClause.Projection; projection != nil {
		if projectionSummary := explainProjectionClauseOptions(finalClause.Kind, projection); len(projectionSummary) > 0 {
			for key, value := range projectionSummary {
				options[key] = value
			}
		}
	}
	options["projectionClauses"] = buildExplainProjectionClauses(projectionClauses)
	if len(options) == 1 {
		if clauses, ok := options["projectionClauses"]; ok {
			if list, ok := clauses.([]map[string]any); ok && len(list) == 0 {
				return nil
			}
		}
	}
	if len(options) == 0 {
		return nil
	}
	return options
}

func explainQueryProjectionClauses(stmt ast.Statement) []projectionClauseOptions {
	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return explainQueryProjectionClauses(s.Statement)
	case *ast.MatchQueryStatement:
		return []projectionClauseOptions{{Kind: ast.ClauseKindReturn, Projection: &s.Return}}
	case *ast.QueryStatement:
		clauses := make([]projectionClauseOptions, 0)
		for i := 0; i < len(s.Parts); i++ {
			part := s.Parts[i]
			for j := 0; j < len(part.Clauses); j++ {
				clause := part.Clauses[j]
				if clause.Projection == nil {
					continue
				}
				if clause.Kind == ast.ClauseKindReturn || clause.Kind == ast.ClauseKindWith {
					clauses = append(clauses, projectionClauseOptions{Kind: clause.Kind, Projection: clause.Projection})
				}
			}
		}
		return clauses
	}
	return nil
}

type projectionClauseOptions struct {
	Kind       ast.ClauseKind
	Projection *ast.ReturnClause
}

func buildExplainProjectionClauses(clauses []projectionClauseOptions) []map[string]any {
	out := make([]map[string]any, 0, len(clauses))
	for _, clause := range clauses {
		if clause.Projection == nil {
			continue
		}
		entry := explainProjectionClauseOptions(clause.Kind, clause.Projection)
		if entry == nil {
			continue
		}
		entry["kind"] = string(clause.Kind)
		out = append(out, entry)
	}
	return out
}

func explainProjectionClauseOptions(kind ast.ClauseKind, projection *ast.ReturnClause) map[string]any {
	if projection == nil {
		return nil
	}
	options := map[string]any{}
	if kind == ast.ClauseKindReturn || kind == ast.ClauseKindWith {
		options["kind"] = string(kind)
	}
	if projection.Distinct {
		options["distinct"] = true
	}
	if projection.IncludeAll {
		options["includeAll"] = true
	}
	items := make([]string, 0, len(projection.Items))
	for _, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		if alias := strings.TrimSpace(item.Alias); alias != "" {
			expr = expr + " AS " + alias
		}
		items = append(items, expr)
	}
	if len(items) > 0 {
		options["projection"] = items
	}
	if len(projection.OrderBy) > 0 {
		ordering := make([]string, 0, len(projection.OrderBy))
		for _, item := range projection.OrderBy {
			expr := strings.TrimSpace(item.Expression.Raw)
			if expr == "" {
				continue
			}
			dir := strings.TrimSpace(string(item.Direction))
			if dir != "" && dir != string(ast.SortDirectionNone) {
				expr = expr + " " + dir
			}
			ordering = append(ordering, expr)
		}
		if len(ordering) > 0 {
			options["orderBy"] = ordering
		}
	}
	if projection.Skip != nil {
		if raw := strings.TrimSpace(projection.Skip.Raw); raw != "" {
			options["skip"] = raw
		}
	}
	if projection.Limit != nil {
		if raw := strings.TrimSpace(projection.Limit.Raw); raw != "" {
			options["limit"] = raw
		}
	}
	if len(options) == 0 {
		return nil
	}
	return options
}

func buildExplainPlanNodes(stmt ast.Statement, catalog IndexCatalog, tenant string, params Params) []map[string]any {
	builder := newExplainPlanBuilder(catalog, tenant, params)
	builder.build(stmt)
	return builder.nodes
}

type explainPlanBuilder struct {
	catalog IndexCatalog
	tenant  string
	params  Params
	nodes   []map[string]any
	nextID  int
}

func newExplainPlanBuilder(catalog IndexCatalog, tenant string, params Params) *explainPlanBuilder {
	return &explainPlanBuilder{catalog: catalog, tenant: tenant, params: params, nodes: make([]map[string]any, 0), nextID: 1}
}

func (b *explainPlanBuilder) build(stmt ast.Statement) {
	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		b.build(s.Statement)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			b.addMatchClause(match.Optional, match.Pattern, match.Where)
		}
		b.addProjectionClause(ast.Clause{Kind: ast.ClauseKindReturn, Projection: &s.Return})
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				b.addClause(clause)
			}
		}
	case *ast.StandaloneCallStatement:
		b.add("CALL", map[string]any{"predicate": strings.TrimSpace(s.Call.Raw)})
	default:
		b.add(string(stmt.Kind()), map[string]any{"predicate": strings.TrimSpace(explainedQueryText(&ast.ExplainStatement{Raw: "", Query: "", Statement: stmt}))})
	}
}

func (b *explainPlanBuilder) addClause(clause ast.Clause) {
	switch clause.Kind {
	case ast.ClauseKindMatch:
		b.addMatchClause(false, clause.Raw, clause.Where)
	case ast.ClauseKindOptionalMatch:
		b.addMatchClause(true, clause.Raw, clause.Where)
	case ast.ClauseKindWith, ast.ClauseKindReturn:
		b.addProjectionClause(clause)
	case ast.ClauseKindUnwind:
		b.add("UNWIND", map[string]any{"predicate": strings.TrimSpace(clause.Raw)})
	case ast.ClauseKindInQueryCall, ast.ClauseKindStandaloneCall:
		b.add("CALL", map[string]any{"predicate": strings.TrimSpace(clause.Raw)})
	case ast.ClauseKindDelete:
		b.addDeleteClause(clause)
	case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove:
		attrs := map[string]any{"writeAction": string(clause.Kind)}
		if raw := strings.TrimSpace(clause.Raw); raw != "" {
			attrs["predicate"] = raw
		}
		b.add("WRITE", attrs)
	default:
		attrs := map[string]any{}
		if raw := strings.TrimSpace(clause.Raw); raw != "" {
			attrs["predicate"] = raw
		}
		b.add(string(clause.Kind), attrs)
	}
}

func (b *explainPlanBuilder) addMatchClause(optional bool, raw string, where *ast.Expression) {
	patternBody := strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(raw, "MATCH")))
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(raw)), "OPTIONAL MATCH") || optional {
		patternBody = strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(raw, "OPTIONAL MATCH")))
	}
	if patternBody == "" {
		patternBody = strings.TrimSpace(raw)
	}

	if pattern, err := parseNodePattern(patternBody); err == nil {
		b.addNodeScan(optional, pattern, where)
		return
	}
	if anchored, err := parseAnchoredOutPattern(patternBody); err == nil {
		b.addAnchoredScan(optional, anchored, where)
		return
	}
	if edgePattern, ok := explainFastUndirectedEdgePatternFromRaw(patternBody); ok {
		b.addFastUndirectedEdgeScan(optional, edgePattern, where)
		return
	}

	op := "MATCH"
	if optional {
		op = "OPTIONAL_MATCH"
	}
	attrs := map[string]any{}
	if patternBody != "" {
		attrs["predicate"] = patternBody
	}
	b.add(op, attrs)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

type explainFastUndirectedEdgePattern struct {
	EdgeVar    string
	LeftLabel  string
	RightLabel string
}

func explainFastUndirectedEdgePatternFromRaw(raw string) (explainFastUndirectedEdgePattern, bool) {
	pattern, err := parseUndirectedRelationshipPattern(strings.TrimSpace(raw))
	if err != nil {
		return explainFastUndirectedEdgePattern{}, false
	}
	if strings.TrimSpace(pattern.EdgeVar) == "" || strings.TrimSpace(pattern.EdgeProps) != "" || pattern.EdgeType != "" || len(pattern.EdgeAnyOf) != 0 {
		return explainFastUndirectedEdgePattern{}, false
	}
	if strings.TrimSpace(pattern.Left.Var) != "" || strings.TrimSpace(pattern.Right.Var) != "" {
		return explainFastUndirectedEdgePattern{}, false
	}
	leftLabel, leftAny, ok := fastEdgeCountNodeLabelFilter(pattern.Left)
	if !ok {
		return explainFastUndirectedEdgePattern{}, false
	}
	rightLabel, rightAny, ok := fastEdgeCountNodeLabelFilter(pattern.Right)
	if !ok {
		return explainFastUndirectedEdgePattern{}, false
	}
	if !leftAny && !rightAny {
		return explainFastUndirectedEdgePattern{}, false
	}
	return explainFastUndirectedEdgePattern{EdgeVar: strings.TrimSpace(pattern.EdgeVar), LeftLabel: leftLabel, RightLabel: rightLabel}, true
}

func (b *explainPlanBuilder) addFastUndirectedEdgeScan(optional bool, pattern explainFastUndirectedEdgePattern, where *ast.Expression) {
	op := "EDGE_SCAN"
	if optional {
		op = "OPTIONAL_EDGE_SCAN"
	}
	attrs := map[string]any{
		"accessPath":       "edge_adjacency",
		"edgeVar":          pattern.EdgeVar,
		"implementation":   "fast_candidate",
		"matchDirection":   "undirected",
		"leftLabelFilter":  pattern.LeftLabel,
		"rightLabelFilter": pattern.RightLabel,
	}
	b.add(op, attrs)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addNodeScan(optional bool, pattern nodePattern, where *ast.Expression) {
	schema := explainPatternSchema(pattern)
	props, err := explainPropertyMap(pattern.PropertiesRaw, b.params)
	propKeyMap := explainPropertyKeys(pattern.PropertiesRaw)
	propKeys := make([]string, 0, len(propKeyMap))
	for prop := range propKeyMap {
		if strings.EqualFold(prop, "id") {
			continue
		}
		propKeys = append(propKeys, prop)
	}
	if err == nil {
		propKeys = propKeys[:0]
		for prop := range props {
			if strings.EqualFold(prop, "id") {
				continue
			}
			propKeys = append(propKeys, prop)
		}
	}
	sort.Strings(propKeys)
	indexed := false
	indexPath := ""
	if schema != "" {
		for _, prop := range propKeys {
			if b.catalog != nil && b.catalog.HasPropertyIndex(b.tenant, schema, prop) {
				indexed = true
				indexPath = fmt.Sprintf("property_index(%s.%s)", schema, prop)
				break
			}
		}
	}
	op := "INDEX_SCAN"
	if !indexed {
		if schema != "" {
			op = "LABEL_SCAN"
			indexPath = fmt.Sprintf("label(%s)", schema)
		} else {
			op = "ALL_NODES_SCAN"
			indexPath = "all_nodes"
		}
	}
	if optional {
		op = "OPTIONAL_" + op
	}
	attrs := map[string]any{"accessPath": indexPath}
	if nodeVar := strings.TrimSpace(pattern.Var); nodeVar != "" {
		attrs["nodeVar"] = nodeVar
	}
	if indexed && strings.TrimSpace(pattern.PropertiesRaw) != "" {
		attrs["predicate"] = strings.TrimSpace(pattern.PropertiesRaw)
		if err != nil {
			attrs["predicateQuality"] = "estimate"
		}
	}
	b.add(op, attrs)
	if !indexed && strings.TrimSpace(pattern.PropertiesRaw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(pattern.PropertiesRaw)})
	}
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addAnchoredScan(optional bool, pattern anchoredOutPattern, where *ast.Expression) {
	schema := strings.TrimSpace(pattern.SourceLabel)
	prop, value, ok := anchoredSourcePropertyEquality(pattern, b.params, nil)
	indexed := ok && b.catalog != nil && schema != "" && b.catalog.HasPropertyIndex(b.tenant, schema, prop)
	op := "INDEX_SCAN"
	accessPath := "all_nodes"
	if indexed {
		accessPath = fmt.Sprintf("property_index(%s.%s)", schema, prop)
	} else if schema != "" {
		op = "LABEL_SCAN"
		accessPath = fmt.Sprintf("label(%s)", schema)
	} else {
		op = "ALL_NODES_SCAN"
	}
	if optional {
		op = "OPTIONAL_" + op
	}
	attrs := map[string]any{"accessPath": accessPath}
	if indexed && prop != "" && value != nil {
		attrs["predicate"] = pattern.SourcePropertiesRaw
	}
	b.add(op, attrs)
	if !indexed && strings.TrimSpace(pattern.SourcePropertiesRaw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(pattern.SourcePropertiesRaw)})
	}
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addProjectionClause(clause ast.Clause) {
	projection := clause.Projection
	if projection == nil {
		return
	}
	if nodeVar, ok := b.canUseFastNodeCount(); ok {
		if output, ok := explainFastNodeCountProjection(projection, nodeVar); ok {
			attrs := map[string]any{
				"aggregates":     []string{fmt.Sprintf("count(%s)", nodeVar)},
				"projection":     []string{output},
				"implementation": "fast_node_count",
			}
			b.add("AGGREGATE", attrs)
			return
		}
	}
	if edgeVar, ok := b.canUseFastEdgeCount(); ok {
		if output, ok := explainFastEdgeCountProjection(projection, edgeVar); ok {
			attrs := map[string]any{
				"aggregates":     []string{fmt.Sprintf("count(%s)", edgeVar)},
				"projection":     []string{output},
				"implementation": "fast_edge_count",
			}
			b.add("AGGREGATE", attrs)
			return
		}
	}
	if spec, ok := explainFastLabelHistogramProjection(projection); ok && b.canUseFastLabelHistogram() {
		attrs := map[string]any{
			"groupBy":        []string{spec.labelsOutput},
			"aggregates":     []string{fmt.Sprintf("count(%s)", spec.countInputExpr)},
			"projection":     []string{spec.labelsOutput, spec.countOutput},
			"implementation": "fast_label_histogram",
		}
		b.add("AGGREGATE", attrs)
		return
	}
	items := make([]string, 0, len(projection.Items))
	for _, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		if alias := strings.TrimSpace(item.Alias); alias != "" {
			expr = expr + " AS " + alias
		}
		items = append(items, expr)
	}
	projectAttrs := map[string]any{}
	if len(items) > 0 {
		projectAttrs["projection"] = items
	}
	if projection.IncludeAll {
		projectAttrs["projection"] = []string{"*"}
	}
	b.add("PROJECT", projectAttrs)
	if projection.Distinct {
		b.add("DISTINCT", map[string]any{"projection": items})
	}
	if len(projection.OrderBy) > 0 {
		ordering := make([]string, 0, len(projection.OrderBy))
		for _, item := range projection.OrderBy {
			expr := strings.TrimSpace(item.Expression.Raw)
			if expr == "" {
				continue
			}
			dir := strings.TrimSpace(string(item.Direction))
			if dir != "" && dir != string(ast.SortDirectionNone) {
				expr = expr + " " + dir
			}
			ordering = append(ordering, expr)
		}
		if len(ordering) > 0 {
			b.add("SORT", map[string]any{"ordering": ordering})
		}
	}
	if projection.Skip != nil && strings.TrimSpace(projection.Skip.Raw) != "" {
		b.add("SKIP", map[string]any{"pagination": map[string]any{"skip": strings.TrimSpace(projection.Skip.Raw)}})
	}
	if projection.Limit != nil && strings.TrimSpace(projection.Limit.Raw) != "" {
		b.add("LIMIT", map[string]any{"pagination": map[string]any{"limit": strings.TrimSpace(projection.Limit.Raw)}})
	}
}

func (b *explainPlanBuilder) addDeleteClause(clause ast.Clause) {
	if edgeVar, ok := b.canUseFastEdgeDelete(); ok {
		if target, ok := explainDeleteSingleTarget(clause.Raw); ok && target == edgeVar {
			attrs := map[string]any{"writeAction": string(clause.Kind), "implementation": "fast_edge_delete"}
			if raw := strings.TrimSpace(clause.Raw); raw != "" {
				attrs["predicate"] = raw
			}
			b.add("DELETE", attrs)
			return
		}
	}
	attrs := map[string]any{"writeAction": string(clause.Kind)}
	if raw := strings.TrimSpace(clause.Raw); raw != "" {
		attrs["predicate"] = raw
	}
	b.add("WRITE", attrs)
}

func explainDeleteSingleTarget(raw string) (string, bool) {
	normalized := normalizeClauseBody(raw)
	upper := strings.ToUpper(normalized)
	if strings.HasPrefix(upper, "DETACHDELETE") {
		return "", false
	}
	if !strings.HasPrefix(upper, "DELETE") {
		return "", false
	}
	body := strings.TrimSpace(normalized[len("DELETE"):])
	if body == "" || strings.Contains(body, ",") || !isIdentifierLike(body) {
		return "", false
	}
	return body, true
}

func explainFastEdgeCountProjection(projection *ast.ReturnClause, edgeVar string) (string, bool) {
	if projection == nil || projection.Distinct || projection.IncludeAll || projection.Skip != nil || projection.Limit != nil || len(projection.OrderBy) != 0 || len(projection.Items) != 1 {
		return "", false
	}
	item := projection.Items[0]
	expr := strings.TrimSpace(item.Expression.Raw)
	countArg, ok := parseFunctionCall(expr, "count")
	if !ok {
		return "", false
	}
	countArg = strings.TrimSpace(countArg)
	if strings.HasPrefix(strings.ToUpper(countArg), "DISTINCT") {
		return "", false
	}
	if countArg != edgeVar {
		return "", false
	}
	if alias := strings.TrimSpace(item.Alias); alias != "" {
		return alias, true
	}
	return expr, true
}

func explainFastNodeCountProjection(projection *ast.ReturnClause, nodeVar string) (string, bool) {
	if projection == nil || projection.Distinct || projection.IncludeAll || projection.Skip != nil || projection.Limit != nil || len(projection.OrderBy) != 0 || len(projection.Items) != 1 {
		return "", false
	}
	item := projection.Items[0]
	expr := strings.TrimSpace(item.Expression.Raw)
	countArg, ok := parseFunctionCall(expr, "count")
	if !ok {
		return "", false
	}
	countArg = strings.TrimSpace(countArg)
	if strings.HasPrefix(strings.ToUpper(countArg), "DISTINCT") {
		return "", false
	}
	if countArg != nodeVar {
		return "", false
	}
	if alias := strings.TrimSpace(item.Alias); alias != "" {
		return alias, true
	}
	return expr, true
}

type explainFastLabelHistogramSpec struct {
	labelsOutput   string
	countOutput    string
	countInputExpr string
}

func explainFastLabelHistogramProjection(projection *ast.ReturnClause) (explainFastLabelHistogramSpec, bool) {
	if projection == nil || projection.Distinct || projection.IncludeAll || projection.Skip != nil || projection.Limit != nil || len(projection.OrderBy) != 0 || len(projection.Items) != 2 {
		return explainFastLabelHistogramSpec{}, false
	}

	labelsIdx := -1
	countIdx := -1
	labelsVar := ""
	for idx, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if arg, ok := parseFunctionCall(expr, "labels"); ok {
			arg = strings.TrimSpace(arg)
			if arg == "" {
				return explainFastLabelHistogramSpec{}, false
			}
			labelsIdx = idx
			labelsVar = arg
			continue
		}
		countArg, ok := parseFunctionCall(expr, "count")
		if !ok {
			continue
		}
		countArg = strings.TrimSpace(countArg)
		if strings.HasPrefix(strings.ToUpper(countArg), "DISTINCT") {
			return explainFastLabelHistogramSpec{}, false
		}
		inner, ok := parseFunctionCall(countArg, "labels")
		if !ok {
			continue
		}
		inner = strings.TrimSpace(inner)
		if inner == "" {
			return explainFastLabelHistogramSpec{}, false
		}
		countIdx = idx
		if labelsVar == "" {
			labelsVar = inner
		} else if labelsVar != inner {
			return explainFastLabelHistogramSpec{}, false
		}
	}
	if labelsIdx < 0 || countIdx < 0 || labelsIdx == countIdx || labelsVar == "" {
		return explainFastLabelHistogramSpec{}, false
	}

	labelsOutput := strings.TrimSpace(projection.Items[labelsIdx].Alias)
	if labelsOutput == "" {
		labelsOutput = strings.TrimSpace(projection.Items[labelsIdx].Expression.Raw)
	}
	countOutput := strings.TrimSpace(projection.Items[countIdx].Alias)
	if countOutput == "" {
		countOutput = strings.TrimSpace(projection.Items[countIdx].Expression.Raw)
	}
	if labelsOutput == "" || countOutput == "" {
		return explainFastLabelHistogramSpec{}, false
	}

	return explainFastLabelHistogramSpec{
		labelsOutput:   labelsOutput,
		countOutput:    countOutput,
		countInputExpr: strings.TrimSpace(projection.Items[labelsIdx].Expression.Raw),
	}, true
}

func (b *explainPlanBuilder) canUseFastLabelHistogram() bool {
	if len(b.nodes) == 0 {
		return false
	}
	for _, node := range b.nodes {
		op, _ := node["op"].(string)
		if op == "LABEL_SCAN" || op == "OPTIONAL_LABEL_SCAN" || op == "INDEX_SCAN" || op == "OPTIONAL_INDEX_SCAN" || op == "FILTER" {
			return false
		}
	}
	last := b.nodes[len(b.nodes)-1]
	op, _ := last["op"].(string)
	return op == "ALL_NODES_SCAN"
}

func (b *explainPlanBuilder) canUseFastEdgeCount() (string, bool) {
	if len(b.nodes) == 0 {
		return "", false
	}
	last := b.nodes[len(b.nodes)-1]
	op, _ := last["op"].(string)
	if op != "EDGE_SCAN" {
		return "", false
	}
	edgeVar, _ := last["edgeVar"].(string)
	edgeVar = strings.TrimSpace(edgeVar)
	if edgeVar == "" {
		return "", false
	}
	return edgeVar, true
}

func (b *explainPlanBuilder) canUseFastNodeCount() (string, bool) {
	if len(b.nodes) == 0 {
		return "", false
	}
	last := b.nodes[len(b.nodes)-1]
	op, _ := last["op"].(string)
	if op != "ALL_NODES_SCAN" {
		return "", false
	}
	nodeVar, _ := last["nodeVar"].(string)
	nodeVar = strings.TrimSpace(nodeVar)
	if nodeVar == "" {
		return "", false
	}
	return nodeVar, true
}

func (b *explainPlanBuilder) canUseFastEdgeDelete() (string, bool) {
	if len(b.nodes) == 0 {
		return "", false
	}
	last := b.nodes[len(b.nodes)-1]
	op, _ := last["op"].(string)
	if op != "EDGE_SCAN" {
		return "", false
	}
	edgeVar, _ := last["edgeVar"].(string)
	edgeVar = strings.TrimSpace(edgeVar)
	if edgeVar == "" {
		return "", false
	}
	return edgeVar, true
}

func (b *explainPlanBuilder) add(op string, attrs map[string]any) string {
	if attrs == nil {
		attrs = map[string]any{}
	}
	node := map[string]any{
		"id":       fmt.Sprintf("N%d", b.nextID),
		"op":       op,
		"children": []string{},
	}
	if len(b.nodes) > 0 {
		node["children"] = []string{b.nodes[len(b.nodes)-1]["id"].(string)}
	}
	for key, value := range attrs {
		node[key] = value
	}
	b.nodes = append(b.nodes, node)
	b.nextID++
	return node["id"].(string)
}

func statementMayWrite(stmt ast.Statement) bool {
	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return statementMayWrite(s.Statement)
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			if hasWriteClause(part) {
				return true
			}
		}
		return false
	case *ast.MatchQueryStatement:
		return false
	case *ast.StandaloneCallStatement:
		return false
	default:
		return false
	}
}

func parameterNamesForStatement(stmt ast.Statement) []string {
	refs := make([]ast.ParameterRef, 0)
	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return parameterNamesForStatement(s.Statement)
	case *ast.MatchQueryStatement:
		refs = append(refs, s.Parameters...)
	case *ast.QueryStatement:
		refs = append(refs, s.Parameters...)
	case *ast.StandaloneCallStatement:
		refs = append(refs, s.Parameters...)
	}
	seen := make(map[string]struct{}, len(refs))
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		name := strings.TrimSpace(ref.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func explainFingerprint(stmt *ast.ExplainStatement) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToUpper(strings.TrimSpace(explainedQueryText(stmt)))))
	return fmt.Sprintf("fnv1a64:%x", h.Sum64())
}

func explainedQueryText(stmt *ast.ExplainStatement) string {
	if stmt == nil {
		return ""
	}
	if trimmed := strings.TrimSpace(stmt.Query); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(stmt.Raw)
}

func tenantFromParams(params Params) string {
	if params == nil {
		return ""
	}
	if tenant, ok := params["tenant"]; ok {
		return strings.TrimSpace(fmt.Sprint(tenant))
	}
	return ""
}

func tryParseExplainNodePattern(raw string) (nodePattern, bool) {
	pattern, err := parseNodePattern(raw)
	if err != nil {
		return nodePattern{}, false
	}
	return pattern, true
}

func tryParseExplainAnchoredPattern(raw string) (anchoredOutPattern, bool) {
	pattern, err := parseAnchoredOutPattern(raw)
	if err != nil {
		return anchoredOutPattern{}, false
	}
	return pattern, true
}

func (p anchoredOutPattern) asNodePattern() nodePattern {
	pattern := nodePattern{
		Var:           p.SourceVar,
		AllOfLabels:   nil,
		PropertiesRaw: p.SourcePropertiesRaw,
	}
	if p.SourceLabel != "" {
		pattern.AllOfLabels = []string{p.SourceLabel}
	}
	return pattern
}

func explainPatternSchema(pattern nodePattern) string {
	if len(pattern.AllOfLabels) > 0 {
		return pattern.AllOfLabels[0]
	}
	if len(pattern.AnyOfLabels) > 0 {
		return pattern.AnyOfLabels[0]
	}
	return ""
}

func explainPatternProperties(raw string, params Params) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	return explainPropertyMap(raw, params)
}

func explainPropertyMap(raw string, params Params) (map[string]any, error) {
	parsed, err := parsePropertyMap(raw, params, nil)
	if err != nil {
		return nil, err
	}
	return parsed, nil
}

func explainAnchoredExpression(pattern anchoredOutPattern, params Params) (string, bool) {
	props, err := explainPropertyMap(pattern.SourcePropertiesRaw, params)
	if err != nil || len(props) == 0 {
		return "", false
	}
	pairs := make([]string, 0, len(props))
	for key, value := range props {
		if strings.EqualFold(key, "id") {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(pairs)
	if len(pairs) == 0 {
		return "", false
	}
	return strings.Join(pairs, ","), true
}

func explainPatternExpression(pattern nodePattern, params Params) (string, bool) {
	props, err := explainPatternProperties(pattern.PropertiesRaw, params)
	if err != nil || len(props) == 0 {
		return "", false
	}
	pairs := make([]string, 0, len(props))
	for key, value := range props {
		if strings.EqualFold(key, "id") {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(pairs)
	if len(pairs) == 0 {
		return "", false
	}
	return strings.Join(pairs, ","), true
}

func countMatchingVertices(stats explainStoreStats, pattern nodePattern, params Params) int {
	count := 0
	for _, vertex := range stats.vertices {
		if matchesExplainNodePattern(vertex, pattern, params) {
			count++
		}
	}
	return count
}

func countAnchoredRows(stats explainStoreStats, pattern anchoredOutPattern, params Params) int {
	left := pattern.asNodePattern()
	return countMatchingVertices(stats, left, params)
}

func matchesExplainNodePattern(vertex *graph.Vertex, pattern nodePattern, params Params) bool {
	if vertex == nil {
		return false
	}
	if len(pattern.AllOfLabels) > 0 {
		for _, label := range pattern.AllOfLabels {
			if !explainVertexHasLabel(vertex, label) {
				return false
			}
		}
	}
	if len(pattern.AnyOfLabels) > 0 {
		matched := false
		for _, label := range pattern.AnyOfLabels {
			if vertexHasLabel(vertex, label) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, excluded := range pattern.ExcludedLabels {
		if vertexHasLabel(vertex, excluded) {
			return false
		}
	}
	if strings.TrimSpace(pattern.PropertiesRaw) == "" {
		return true
	}
	props, err := explainPropertyMap(pattern.PropertiesRaw, params)
	if err != nil {
		return false
	}
	for key, value := range props {
		if strings.EqualFold(key, "id") {
			if fmt.Sprint(value) != vertex.ID {
				return false
			}
			continue
		}
		current, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(current, valueToBytes(value)) {
			return false
		}
	}
	return true
}

func explainVertexHasLabel(vertex *graph.Vertex, label string) bool {
	label = strings.TrimSpace(label)
	if vertex == nil || label == "" {
		return false
	}
	for _, current := range vertex.Labels {
		if current == label {
			return true
		}
	}
	return false
}

func explainPaginationValue(raw string, params Params) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		if params == nil {
			return 0, false
		}
		value, ok := params[name]
		if !ok {
			return 0, false
		}
		switch v := value.(type) {
		case int:
			return v, true
		case int8:
			return int(v), true
		case int16:
			return int(v), true
		case int32:
			return int(v), true
		case int64:
			return int(v), true
		case float32:
			return int(v), true
		case float64:
			return int(v), true
		case string:
			parsed, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				return 0, false
			}
			return parsed, true
		default:
			parsed, err := strconv.Atoi(fmt.Sprint(v))
			if err != nil {
				return 0, false
			}
			return parsed, true
		}
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
