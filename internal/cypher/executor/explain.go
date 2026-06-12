package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	cypherexplain "github.com/spaceqraft/vitaledge/internal/cypher/explain"
	"github.com/spaceqraft/vitaledge/internal/cypher/semantic"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

type explainAnalysis struct {
	vertexCounts     []map[string]any
	edgeCounts       []map[string]any
	predicateSignals []map[string]any
	indexDecisions   []map[string]any
	fastPaths        []map[string]any
	cardinality      []map[string]any
	costEstimate     map[string]any
	runtimeStats     map[string]any
	warnings         []map[string]any
	vertexTotal      int
	edgeTotal        int
}

type explainStoreStats struct {
	labelCounts         map[string]int
	edgeCounts          map[string]int
	patternMatchCounts  map[string]int
	vertexPropertyStats map[string]map[string]graph.StatsPropertySummary
	edgePropertyStats   map[string]map[string]graph.StatsPropertySummary
	vertexTotal         int
	edgeTotal           int
	snapshotFound       bool
}

type explainPatternRefs struct {
	labels    map[string]struct{}
	edgeTypes map[string]struct{}
	hasVertex bool
	hasEdge   bool
}

func newExplainPatternRefs() explainPatternRefs {
	return explainPatternRefs{
		labels:    map[string]struct{}{},
		edgeTypes: map[string]struct{}{},
	}
}

func (e *Executor) executeExplainStatement(ctx context.Context, stmt *ast.ExplainStatement, params Params) (*Result, error) {
	if stmt == nil || stmt.Statement == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "EXPLAIN requires an inner statement", nil)
	}
	payload, err := buildPipelineExplainPayload(ctx, e, stmt, params)
	if err != nil {
		return nil, err
	}
	return &Result{
		Columns: []string{"explain"},
		Rows:    []Row{{"explain": payload}},
		Stats:   Stats{RowsReturned: 1},
	}, nil
}

func (e *Executor) executeProfileStatement(ctx context.Context, stmt *ast.ProfileStatement, params Params) (*Result, error) {
	if stmt == nil || stmt.Statement == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "PROFILE requires an inner statement", nil)
	}
	payload, err := buildPipelineProfilePayload(ctx, e, stmt, params)
	if err != nil {
		return nil, err
	}
	return &Result{
		Columns: []string{"profile"},
		Rows:    []Row{{"profile": payload}},
		Stats:   Stats{RowsReturned: 1},
	}, nil
}

func (e *Executor) buildExplainAnalysis(ctx context.Context, stmt ast.Statement, params Params) (*explainAnalysis, error) {
	analysis := &explainAnalysis{}

	tenant := tenantFromParams(params)
	var stats explainStoreStats
	if err := e.store.View(ctx, func(tx graph.Tx) error {
		collected, err := collectExplainStoreStats(ctx, tx, tenant, stmt, params)
		if err != nil {
			return err
		}
		stats = collected
		return nil
	}); err != nil {
		return nil, err
	}
	refs := collectExplainPatternRefs(stmt)
	analysis.vertexCounts = buildExplainVertexCounts(stats, refs)
	analysis.edgeCounts = buildExplainEdgeCounts(stats, refs)
	analysis.vertexTotal = stats.vertexTotal
	analysis.edgeTotal = stats.edgeTotal
	analysis.predicateSignals = buildExplainPredicateSignals(stmt, params, tenant, stats)
	planNodes := buildExplainPlanNodes(stmt, e.indexCatalog, tenant, params)
	analysis.indexDecisions = buildExplainIndexDecisions(stmt, params, e.indexCatalog, tenant, stats, planNodes)
	analysis.fastPaths = collectExplainFastPaths(stmt, params, e.fastPathFeedbackSnapshot)
	analysis.cardinality = buildExplainCardinalityFromPlanNodes(planNodes, params, stats)
	analysis.costEstimate = buildExplainCostEstimate(planNodes, analysis.cardinality, analysis.indexDecisions, e.fastPathFeedbackSnapshot)
	analysis.warnings = buildExplainWarnings(stmt, analysis, planNodes, tenant)
	analysis.runtimeStats = buildExplainRuntimeStats(planNodes, analysis.cardinality, analysis.indexDecisions, analysis.fastPaths, analysis.warnings, stats)
	return analysis, nil
}

func buildPipelineExplainPayload(ctx context.Context, e *Executor, stmt *ast.ExplainStatement, params Params) (map[string]any, error) {
	analysis, err := e.buildExplainAnalysis(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}

	semanticModel, err := semantic.Build(stmt.Statement)
	if err != nil {
		return nil, err
	}
	logicalPlan, physicalPlan, err := e.buildRuntimePhysicalPlan(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}
	pipelineExplainText := cypherexplain.RenderPipeline(logicalPlan, physicalPlan)

	logicalNodes := make([]map[string]any, 0, len(logicalPlan.Nodes))
	for _, node := range logicalPlan.Nodes {
		nodePayload := map[string]any{
			"id":       node.ID,
			"op":       node.Op,
			"children": append([]string(nil), node.Children...),
			"attrs":    node.Attrs,
		}
		for key, value := range node.Attrs {
			nodePayload[key] = value
		}
		logicalNodes = append(logicalNodes, nodePayload)
	}
	physicalNodes := make([]map[string]any, 0, len(physicalPlan.Nodes))
	for _, node := range physicalPlan.Nodes {
		nodePayload := map[string]any{
			"id":       node.ID,
			"op":       node.Op,
			"children": append([]string(nil), node.Children...),
			"attrs":    node.Attrs,
		}
		for key, value := range node.Attrs {
			nodePayload[key] = value
		}
		physicalNodes = append(physicalNodes, nodePayload)
	}

	payload := map[string]any{
		"version": "v2-pipeline",
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
		"semantic": map[string]any{
			"statementKind": string(semanticModel.StatementKind),
			"patterns":      semanticModel.Patterns,
			"projections":   semanticModel.Projections,
			"ordering":      semanticModel.Ordering,
			"pagination":    semanticModel.Pagination,
			"writeActions":  semanticModel.WriteActions,
		},
		"logicalPlan": map[string]any{
			"rootNodeId": logicalPlan.RootNodeID,
			"nodes":      logicalNodes,
		},
		"physicalPlan": map[string]any{
			"rootNodeId": physicalPlan.RootNodeID,
			"nodes":      physicalNodes,
		},
		"influencers": map[string]any{
			"vertexCounts": analysis.vertexCounts,
			"edgeCounts":   analysis.edgeCounts,
			"totals": map[string]any{
				"vertexes": analysis.vertexTotal,
				"edges":    analysis.edgeTotal,
			},
			"predicateSignals": analysis.predicateSignals,
		},
		"cardinality":         analysis.cardinality,
		"costEstimate":        analysis.costEstimate,
		"runtimeStats":        analysis.runtimeStats,
		"indexDecisions":      analysis.indexDecisions,
		"executionStrategies": analysis.fastPaths,
		"warnings":            analysis.warnings,
		"metadata": map[string]any{
			"transport":             "json",
			"pipelineExplainStatus": "ok",
			"pipelineExplain":       pipelineExplainText,
		},
	}

	return payload, nil
}

func buildPipelineProfilePayload(ctx context.Context, e *Executor, stmt *ast.ProfileStatement, params Params) (map[string]any, error) {
	analysis, err := e.buildExplainAnalysis(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}

	semanticModel, err := semantic.Build(stmt.Statement)
	if err != nil {
		return nil, err
	}
	logicalPlan, physicalPlan, err := e.buildRuntimePhysicalPlan(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}
	pipelineExplainText := cypherexplain.RenderPipeline(logicalPlan, physicalPlan)

	execRes, err := e.ExecuteStatement(ctx, stmt.Statement, params)
	if err != nil {
		return nil, err
	}

	logicalNodes := make([]map[string]any, 0, len(logicalPlan.Nodes))
	for _, node := range logicalPlan.Nodes {
		nodePayload := map[string]any{
			"id":       node.ID,
			"op":       node.Op,
			"children": append([]string(nil), node.Children...),
			"attrs":    node.Attrs,
		}
		for key, value := range node.Attrs {
			nodePayload[key] = value
		}
		logicalNodes = append(logicalNodes, nodePayload)
	}
	physicalNodes := make([]map[string]any, 0, len(physicalPlan.Nodes))
	for _, node := range physicalPlan.Nodes {
		nodePayload := map[string]any{
			"id":       node.ID,
			"op":       node.Op,
			"children": append([]string(nil), node.Children...),
			"attrs":    node.Attrs,
		}
		for key, value := range node.Attrs {
			nodePayload[key] = value
		}
		physicalNodes = append(physicalNodes, nodePayload)
	}

	resultRows := make([]map[string]any, 0, len(execRes.Rows))
	for _, row := range execRes.Rows {
		if row == nil {
			resultRows = append(resultRows, map[string]any{})
			continue
		}
		copyRow := make(map[string]any, len(row))
		for key, value := range row {
			copyRow[key] = value
		}
		resultRows = append(resultRows, copyRow)
	}

	payload := map[string]any{
		"version": "v1-profile",
		"query": map[string]any{
			"text":          profiledQueryText(stmt),
			"fingerprint":   profileFingerprint(stmt),
			"statementKind": string(stmt.Statement.Kind()),
			"tenant":        tenantFromParams(params),
			"params":        parameterNamesForStatement(stmt.Statement),
			"options":       buildExplainQueryOptions(stmt.Statement),
		},
		"summary": map[string]any{
			"dryRun":               false,
			"readOnly":             !statementMayWrite(stmt.Statement),
			"writesDetected":       statementMayWrite(stmt.Statement),
			"semanticPhaseStatus":  "ok",
			"planningPhaseStatus":  "ok",
			"executionPhaseStatus": "ok",
		},
		"semantic": map[string]any{
			"statementKind": string(semanticModel.StatementKind),
			"patterns":      semanticModel.Patterns,
			"projections":   semanticModel.Projections,
			"ordering":      semanticModel.Ordering,
			"pagination":    semanticModel.Pagination,
			"writeActions":  semanticModel.WriteActions,
		},
		"logicalPlan": map[string]any{
			"rootNodeId": logicalPlan.RootNodeID,
			"nodes":      logicalNodes,
		},
		"physicalPlan": map[string]any{
			"rootNodeId": physicalPlan.RootNodeID,
			"nodes":      physicalNodes,
		},
		"indexDecisions":      analysis.indexDecisions,
		"executionStrategies": analysis.fastPaths,
		"runtimeStats":        analysis.runtimeStats,
		"warnings":            analysis.warnings,
		"result": map[string]any{
			"columns":       append([]string(nil), execRes.Columns...),
			"rows":          resultRows,
			"rowsReturned":  execRes.Stats.RowsReturned,
			"durationNanos": execRes.Stats.Duration.Nanoseconds(),
		},
		"metadata": map[string]any{
			"transport":             "json",
			"pipelineProfileStatus": "ok",
			"pipelineProfile":       pipelineExplainText,
		},
	}

	return payload, nil
}

func collectExplainStoreStats(ctx context.Context, tx graph.Tx, tenant string, stmt ast.Statement, params Params) (explainStoreStats, error) {
	stats := explainStoreStats{
		labelCounts:        map[string]int{},
		edgeCounts:         map[string]int{},
		patternMatchCounts: map[string]int{},
	}
	if strings.TrimSpace(tenant) == "" {
		return stats, nil
	}
	matchers := collectExplainVertexMatchers(stmt)
	for _, matcher := range matchers {
		stats.patternMatchCounts[matcher.key] = 0
	}
	hasSnapshotTotals := false
	if snapshot, err := tx.GetStatsSnapshot(ctx, tenant); err == nil && snapshot != nil {
		stats.snapshotFound = true
		stats.vertexTotal = snapshot.VertexTotal
		stats.edgeTotal = snapshot.EdgeTotal
		stats.vertexPropertyStats = snapshot.VertexPropertyStats
		stats.edgePropertyStats = snapshot.EdgePropertyStats
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
	if len(stats.vertexPropertyStats) == 0 && len(stats.edgePropertyStats) == 0 {
		vertexSummary, edgeSummary, liveErr := collectExplainLivePropertySummaries(ctx, tx, tenant)
		if liveErr != nil {
			return explainStoreStats{}, liveErr
		}
		if len(stats.vertexPropertyStats) == 0 {
			stats.vertexPropertyStats = vertexSummary
		}
		if len(stats.edgePropertyStats) == 0 {
			stats.edgePropertyStats = edgeSummary
		}
	}

	if err := tx.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		for _, matcher := range matchers {
			if matchesExplainVertexPattern(v, matcher.pattern, params) {
				stats.patternMatchCounts[matcher.key]++
			}
		}
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

func collectExplainLivePropertySummaries(ctx context.Context, tx graph.Tx, tenant string) (map[string]map[string]graph.StatsPropertySummary, map[string]map[string]graph.StatsPropertySummary, error) {
	vertexSummary := map[string]map[string]graph.StatsPropertySummary{}
	edgeSummary := map[string]map[string]graph.StatsPropertySummary{}
	vertexDistinctSets := map[string]map[string]map[string]map[string]struct{}{}
	edgeDistinctSets := map[string]map[string]map[string]map[string]struct{}{}
	vertexKindCounts := map[string]map[string]map[string]int{}
	edgeKindCounts := map[string]map[string]map[string]int{}

	collect := func(summary map[string]map[string]graph.StatsPropertySummary, distinctSets map[string]map[string]map[string]map[string]struct{}, kindCounts map[string]map[string]map[string]int, schema, property string, raw []byte) {
		if schema == "" || property == "" {
			return
		}
		kind := explainStoredPropertyValueKind(raw)
		if summary[schema] == nil {
			summary[schema] = map[string]graph.StatsPropertySummary{}
		}
		if distinctSets[schema] == nil {
			distinctSets[schema] = map[string]map[string]map[string]struct{}{}
		}
		if distinctSets[schema][property] == nil {
			distinctSets[schema][property] = map[string]map[string]struct{}{}
		}
		if distinctSets[schema][property][kind] == nil {
			distinctSets[schema][property][kind] = map[string]struct{}{}
		}
		if kindCounts[schema] == nil {
			kindCounts[schema] = map[string]map[string]int{}
		}
		if kindCounts[schema][property] == nil {
			kindCounts[schema][property] = map[string]int{}
		}
		distinctSets[schema][property][kind][string(raw)] = struct{}{}
		kindCounts[schema][property][kind]++
	}

	if err := tx.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		schema := inferVertexSchemaFromVertex(v)
		for property, raw := range v.Properties {
			collect(vertexSummary, vertexDistinctSets, vertexKindCounts, schema, property, raw)
		}
		return tx.ScanOutEdges(ctx, tenant, v.ID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			for property, raw := range edge.Properties {
				collect(edgeSummary, edgeDistinctSets, edgeKindCounts, edge.Type, property, raw)
			}
			return nil
		})
	}); err != nil {
		return nil, nil, err
	}

	finalize := func(summary map[string]map[string]graph.StatsPropertySummary, distinctSets map[string]map[string]map[string]map[string]struct{}, kindCounts map[string]map[string]map[string]int) {
		for schema, properties := range kindCounts {
			for property, countsByKind := range properties {
				s := summary[schema][property]
				if s.DistinctValuesByKind == nil {
					s.DistinctValuesByKind = map[string]int{}
				}
				if s.IndexedEntriesByKind == nil {
					s.IndexedEntriesByKind = map[string]int{}
				}
				if s.EstimatedSelectivityByKind == nil {
					s.EstimatedSelectivityByKind = map[string]float64{}
				}
				totalDistinct := 0
				for kind, count := range countsByKind {
					s.IndexedEntriesByKind[kind] = count
					if kindSets := distinctSets[schema][property][kind]; kindSets != nil {
						distinctCount := len(kindSets)
						s.DistinctValuesByKind[kind] = distinctCount
						if distinctCount > 0 {
							s.EstimatedSelectivityByKind[kind] = 1 / float64(distinctCount)
						}
						totalDistinct += distinctCount
					}
				}
				s.DistinctValues = totalDistinct
				if totalDistinct > 0 {
					s.EstimatedSelectivity = 1 / float64(totalDistinct)
				}
				summary[schema][property] = s
			}
		}
	}
	finalize(vertexSummary, vertexDistinctSets, vertexKindCounts)
	finalize(edgeSummary, edgeDistinctSets, edgeKindCounts)
	return vertexSummary, edgeSummary, nil
}

func inferVertexSchemaFromVertex(v *graph.Vertex) string {
	if v == nil || len(v.Labels) == 0 {
		return "UNLABELED"
	}
	return v.Labels[0]
}

func explainStoredPropertyValueKind(raw []byte) string {
	value := decodeStoredPropertyValue(raw)
	return explainValueKind(value)
}

func explainValueKind(value any) string {
	switch v := value.(type) {
	case nil:
		return "categorical"
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64:
		return "numeric"
	case uint, uint8, uint16, uint32, uint64:
		return "numeric"
	case float32, float64:
		return "numeric"
	case json.Number:
		return "numeric"
	case map[string]any:
		if typ, ok := v["__temporal_type"]; ok {
			if text := strings.ToLower(strings.TrimSpace(fmt.Sprint(typ))); text != "" {
				return "datetime"
			}
		}
		return "categorical"
	case string:
		return "categorical"
	default:
		return "categorical"
	}
}

func buildExplainVertexCounts(stats explainStoreStats, refs explainPatternRefs) []map[string]any {
	labels := make([]string, 0)
	if len(refs.labels) == 0 {
		if refs.hasVertex {
			return nil
		}
		for label := range stats.labelCounts {
			labels = append(labels, label)
		}
	} else {
		for label := range refs.labels {
			labels = append(labels, label)
		}
	}
	sort.Strings(labels)

	out := make([]map[string]any, 0, len(labels))
	for _, label := range labels {
		entry := map[string]any{
			"label":   label,
			"count":   stats.labelCounts[label],
			"quality": "exact",
		}
		entry["assessment"] = map[string]any{
			"label":   label,
			"count":   stats.labelCounts[label],
			"quality": "exact",
		}
		out = append(out, entry)
	}
	return out
}

func collectExplainPatternRefs(stmt ast.Statement) explainPatternRefs {
	refs := newExplainPatternRefs()
	collectVertexLabels := func(pattern vertexPattern) {
		refs.hasVertex = true
		for _, label := range pattern.AllOfLabels {
			label = strings.TrimSpace(label)
			if label != "" {
				refs.labels[label] = struct{}{}
			}
		}
		for _, label := range pattern.AnyOfLabels {
			label = strings.TrimSpace(label)
			if label != "" {
				refs.labels[label] = struct{}{}
			}
		}
	}
	collect := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if pattern, err := parseVertexPattern(raw); err == nil {
			collectVertexLabels(pattern)
			return
		}
		if pattern, err := parseAnchoredOutPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.asNodePattern())
			if label := strings.TrimSpace(pattern.SourceLabel); label != "" {
				refs.labels[label] = struct{}{}
			}
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			return
		}
		if pattern, err := parseDirectedRelationshipPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.EdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if pattern, err := parseDirectedVariableLengthRelationshipPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.EdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if pattern, err := parseReverseDirectedRelationshipPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.EdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if pattern, err := parseUndirectedRelationshipPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.EdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if pattern, err := parseUndirectedVariableLengthRelationshipPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.EdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.EdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if pattern, err := parseDirectedVariableLengthThenDirectedVariableLengthPattern(raw); err == nil {
			refs.hasEdge = true
			collectVertexLabels(pattern.Left)
			collectVertexLabels(pattern.Mid)
			collectVertexLabels(pattern.Right)
			if edgeType := strings.TrimSpace(pattern.FirstEdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.FirstEdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			if edgeType := strings.TrimSpace(pattern.SecondEdgeType); edgeType != "" {
				refs.edgeTypes[edgeType] = struct{}{}
			}
			for _, edgeType := range pattern.SecondEdgeAnyOf {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
			}
			return
		}
		if chain, err := parseMixedRelationshipChainPattern(raw); err == nil {
			refs.hasEdge = true
			for _, vertex := range chain.Vertexes {
				collectVertexLabels(vertex)
			}
			for _, segment := range chain.Segments {
				if edgeType := strings.TrimSpace(segment.EdgeType); edgeType != "" {
					refs.edgeTypes[edgeType] = struct{}{}
				}
				for _, edgeType := range segment.EdgeAnyOf {
					edgeType = strings.TrimSpace(edgeType)
					if edgeType != "" {
						refs.edgeTypes[edgeType] = struct{}{}
					}
				}
			}
			return
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return collectExplainPatternRefs(s.Statement)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			collect(match.Pattern)
		}
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				if clause.Kind != ast.ClauseKindMatch && clause.Kind != ast.ClauseKindOptionalMatch {
					continue
				}
				raw := strings.TrimSpace(clause.Raw)
				if clause.Kind == ast.ClauseKindMatch {
					raw = strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, "MATCH")))
				}
				if clause.Kind == ast.ClauseKindOptionalMatch {
					raw = strings.TrimSpace(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, "OPTIONAL MATCH")))
				}
				collect(raw)
			}
		}
	}

	return refs
}

func buildExplainEdgeCounts(stats explainStoreStats, refs explainPatternRefs) []map[string]any {
	if !refs.hasEdge && len(refs.edgeTypes) == 0 {
		return nil
	}
	types := make([]string, 0)
	if len(refs.edgeTypes) == 0 {
		if refs.hasEdge {
			return nil
		}
		for edgeType := range stats.edgeCounts {
			types = append(types, edgeType)
		}
	} else {
		for edgeType := range refs.edgeTypes {
			types = append(types, edgeType)
		}
	}
	sort.Strings(types)

	out := make([]map[string]any, 0, len(types))
	for _, edgeType := range types {
		entry := map[string]any{
			"type":      edgeType,
			"direction": "out",
			"count":     stats.edgeCounts[edgeType],
			"quality":   "exact",
		}
		entry["assessment"] = map[string]any{
			"type":      edgeType,
			"direction": "out",
			"count":     stats.edgeCounts[edgeType],
			"quality":   "exact",
		}
		out = append(out, entry)
	}
	return out
}

func buildExplainPredicateSignals(stmt ast.Statement, params Params, tenant string, stats explainStoreStats) []map[string]any {
	signals := make([]map[string]any, 0)
	appendFromClause := func(pattern vertexPattern, expression string) {
		matched := countMatchingVertices(stats, pattern, params)
		entry := map[string]any{
			"expression":   expression,
			"matchedCount": matched,
			"quality":      "exact",
		}
		entry["assessment"] = map[string]any{
			"expression":   expression,
			"matchedCount": matched,
			"quality":      "exact",
		}
		signals = append(signals, entry)
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return buildExplainPredicateSignals(s.Statement, params, tenant, stats)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			if pattern, ok := tryParseExplainVertexPattern(match.Pattern); ok {
				if expr, ok := explainPatternExpression(pattern, params); ok {
					appendFromClause(pattern, expr)
				}
			}
			if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				if expr, ok := explainAnchoredExpression(anchored, params); ok {
					appendFromClause(anchored.asNodePattern(), expr)
				}
			}
		}
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				if pattern, ok := tryParseExplainVertexPattern(clause.Raw); ok {
					if expr, ok := explainPatternExpression(pattern, params); ok {
						appendFromClause(pattern, expr)
					}
				}
				if anchored, ok := tryParseExplainAnchoredPattern(clause.Raw); ok {
					if expr, ok := explainAnchoredExpression(anchored, params); ok {
						appendFromClause(anchored.asNodePattern(), expr)
					}
				}
			}
		}
	}

	return signals
}

func buildExplainIndexDecisions(stmt ast.Statement, params Params, catalog IndexCatalog, tenant string, stats explainStoreStats, planVertexes []map[string]any) []map[string]any {
	candidates := collectExplainIndexCandidates(stmt, params, stats)
	if len(candidates) == 0 {
		return nil
	}

	scanVertexes := explainScanPlanVertexes(planVertexes)
	decisions := make([]map[string]any, 0, len(candidates))
	for i, candidate := range candidates {
		vertexID := candidate.VertexID
		accessPath := ""
		if i < len(scanVertexes) {
			vertexID = scanVertexes[i].ID
			accessPath = scanVertexes[i].AccessPath
		}
		if strings.TrimSpace(vertexID) == "" {
			vertexID = fmt.Sprintf("N%d", i+1)
		}

		schema := strings.TrimSpace(candidate.Schema)
		property := strings.TrimSpace(candidate.Property)
		entityClass := strings.TrimSpace(candidate.EntityClass)
		if entityClass == "" {
			entityClass = "vertex"
		}
		if schema == "" || property == "" {
			continue
		}

		selected := false
		scanPopulation := explainSchemaPopulation(stats, schema)
		typeDomain, typedSeekStrategy, typeCounts := explainPropertyTypeDomain(stats, entityClass, schema, property)
		predicateClass := explainNormalizePredicateClass(candidate.PredicateClass)
		planBoundary := explainPlanBoundaryFromPredicateClass(predicateClass)
		plannerTypedDecision := explainPlannerTypedDecision(stats, entityClass, schema, property, typeDomain, typedSeekStrategy, typeCounts, scanPopulation, predicateClass)
		typedIndexEligible := typedSeekStrategy == "typed_property_index_seek"
		if entityClass == "edge" {
			selected = catalog != nil && catalog.HasEdgePropertyIndex(tenant, schema, property)
			scanPopulation = explainEdgeTypePopulation(stats, schema)
		} else {
			selected = catalog != nil && catalog.HasPropertyIndex(tenant, schema, property)
		}
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
		if typedIndexEligible && selected {
			reason = "selected-typed-property-index"
			recommendation = "keep-typed-index"
		}
		if typeDomain == "mixed" && selected {
			reason = "selected-mixed-property-index"
			recommendation = "keep-index-with-mixed-type-fallback"
		}
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
			"vertexId":             vertexID,
			"entityClass":          entityClass,
			"schema":               schema,
			"property":             property,
			"predicateClass":       predicateClass,
			"planBoundary":         planBoundary,
			"typeDomain":           typeDomain,
			"typedSeekStrategy":    typedSeekStrategy,
			"typeCounts":           typeCounts,
			"plannerTypedDecision": plannerTypedDecision,
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
		assessment := map[string]any{
			"selected":             selected,
			"reason":               reason,
			"recommendation":       recommendation,
			"tuningImpact":         tuningImpact,
			"predicateClass":       predicateClass,
			"planBoundary":         planBoundary,
			"typeDomain":           typeDomain,
			"typedSeekStrategy":    typedSeekStrategy,
			"typeCounts":           typeCounts,
			"plannerTypedDecision": plannerTypedDecision,
			"estimatedSelectivity": estimatedSelectivity,
			"scanPopulation":       scanPopulation,
			"matchedCount":         matchedCount,
			"estimatedRowsSaved":   estimatedRowsSaved,
			"quality":              quality,
		}
		if !selected {
			assessment["suggestedIndex"] = fmt.Sprintf("%s.%s", schema, property)
		}
		decision["assessment"] = assessment
		decisions = append(decisions, decision)
	}

	return decisions
}

func explainPropertyTypeDomain(stats explainStoreStats, entityClass, schema, property string) (string, string, map[string]int) {
	summary, ok := explainPropertySummaryFor(stats, entityClass, schema, property)
	if !ok {
		return "unknown", "unknown", map[string]int{}
	}
	counts := explainPropertyTypeCounts(summary)
	if len(counts) == 0 {
		return "unknown", "unknown", counts
	}
	if len(counts) > 1 {
		return "mixed", "mixed_type_fallback", counts
	}
	for kind := range counts {
		return kind, "typed_property_index_seek", counts
	}
	return "unknown", "unknown", counts
}

func explainPlannerTypedDecision(stats explainStoreStats, entityClass, schema, property, typeDomain, typedSeekStrategy string, typeCounts map[string]int, population int, predicateClass string) map[string]any {
	summary, hasSummary := explainPropertySummaryFor(stats, entityClass, schema, property)
	totalCount := 0
	dominantKind := "unknown"
	dominantCount := 0
	for kind, count := range typeCounts {
		totalCount += count
		if count > dominantCount || (count == dominantCount && (dominantKind == "unknown" || kind < dominantKind)) {
			dominantKind = kind
			dominantCount = count
		}
	}
	dominantShare := 0.0
	if totalCount > 0 {
		dominantShare = float64(dominantCount) / float64(totalCount)
	}
	nullRate, absentRate := explainNullAndAbsentRates(summary, population, totalCount)
	equalitySel := explainDomainEqualitySelectivity(summary, dominantKind, typeDomain, nullRate, absentRate)
	rangeSel := explainDomainRangeSelectivity(summary, dominantKind, typeDomain, nullRate, absentRate)
	predicateClass = explainNormalizePredicateClass(predicateClass)
	planBoundary := explainPlanBoundaryFromPredicateClass(predicateClass)
	selectedSelectivity := equalitySel
	if predicateClass == "range" {
		selectedSelectivity = rangeSel
	}

	evaluationOrder := []string{
		"stats_snapshot_available",
		"sample_size_sufficient",
		"type_domain_known",
		"single_domain_for_typed_seek",
		"dominant_domain_confident",
	}
	checks := map[string]bool{
		"stats_snapshot_available":     hasSummary,
		"sample_size_sufficient":       (summary.SampleSize >= 10) || (totalCount >= 10),
		"type_domain_known":            strings.TrimSpace(typeDomain) != "" && typeDomain != "unknown",
		"single_domain_for_typed_seek": typeDomain != "mixed" && typeDomain != "unknown",
		"dominant_domain_confident":    dominantShare >= 0.70 || typeDomain != "mixed",
	}

	failedChecks := make([]string, 0, len(evaluationOrder))
	failedReasons := map[string]string{}
	for _, check := range evaluationOrder {
		if checks[check] {
			continue
		}
		failedChecks = append(failedChecks, check)
		switch check {
		case "stats_snapshot_available":
			failedReasons[check] = "property stats summary is unavailable"
		case "sample_size_sufficient":
			failedReasons[check] = "property stats sample size is too small for confident typed planning"
		case "type_domain_known":
			failedReasons[check] = "type domain is unknown"
		case "single_domain_for_typed_seek":
			failedReasons[check] = "type domain is mixed and requires fallback"
		case "dominant_domain_confident":
			failedReasons[check] = "no dominant type domain confidence for typed seek"
		}
	}

	rule := "typed_seek_if_single_known_domain"
	reason := "single known property type domain supports typed seek"
	state := "typed_seek_preferred"
	if typeDomain == "unknown" || typedSeekStrategy == "unknown" {
		rule = "fallback_if_type_domain_unknown"
		reason = "property type domain is unknown so planner should use conservative fallback"
		state = "fallback_preferred"
	} else if typeDomain == "mixed" || typedSeekStrategy == "mixed_type_fallback" {
		if dominantShare < 0.85 {
			rule = "fallback_if_mixed_type_domain_weak_dominance"
			reason = "property type domain is mixed and dominant-type confidence is weak"
		} else {
			rule = "fallback_if_mixed_type_domain"
			reason = "property type domain is mixed so planner should use mixed-domain fallback"
		}
		state = "fallback_preferred"
	} else if !checks["sample_size_sufficient"] {
		rule = "guardrail_if_stats_sample_sparse"
		reason = "property stats sample is sparse; typed seek remains eligible but should be guarded"
		state = "typed_seek_guarded"
	} else if nullRate >= 0.30 || absentRate >= 0.60 {
		rule = "guardrail_if_null_or_absent_rate_high"
		reason = "null/absent rates are high; prefer guarded typed seek with conservative selectivity"
		state = "typed_seek_guarded"
	}

	fingerprint := explainDiagnosticPostureHashString(
		"state=" + state + ";" +
			"rule=" + rule + ";" +
			"domain=" + typeDomain + ";" +
			"strategy=" + typedSeekStrategy + ";" +
			"dominantKind=" + dominantKind + ";" +
			"dominantShare=" + strconv.FormatFloat(dominantShare, 'f', 4, 64) + ";" +
			"failedChecks=" + strings.Join(failedChecks, ",") + ";",
	)

	return map[string]any{
		"evaluationOrder":     evaluationOrder,
		"checks":              checks,
		"failedChecks":        failedChecks,
		"failedReasons":       failedReasons,
		"state":               state,
		"rule":                rule,
		"reason":              reason,
		"dominantKind":        dominantKind,
		"dominantShare":       dominantShare,
		"sampleSize":          summary.SampleSize,
		"statsEpoch":          summary.StatsEpoch,
		"nullRate":            nullRate,
		"absentRate":          absentRate,
		"equalitySel":         equalitySel,
		"rangeSel":            rangeSel,
		"selectedSelectivity": selectedSelectivity,
		"predicateClass":      predicateClass,
		"planBoundary":        planBoundary,
		"strategy":            typedSeekStrategy,
		"fingerprint":         fingerprint,
	}
}

func explainNormalizePredicateClass(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "range" {
		return "range"
	}
	return "equality"
}

func explainPlanBoundaryFromPredicateClass(predicateClass string) string {
	if explainNormalizePredicateClass(predicateClass) == "range" {
		return "range_predicate"
	}
	return "equality_predicate"
}

func explainNullAndAbsentRates(summary graph.StatsPropertySummary, population int, nonNullCount int) (float64, float64) {
	nullCount := 0
	totalIndexed := 0
	for kind, count := range summary.IndexedEntriesByKind {
		if count <= 0 {
			continue
		}
		totalIndexed += count
		if strings.EqualFold(strings.TrimSpace(kind), "null") {
			nullCount += count
		}
	}
	if totalIndexed <= 0 {
		totalIndexed = summary.IndexedEntries
	}
	if totalIndexed <= 0 {
		totalIndexed = summary.SampleSize
	}
	if totalIndexed <= 0 && nonNullCount > 0 {
		totalIndexed = nonNullCount
	}
	nullRate := 0.0
	if totalIndexed > 0 && nullCount > 0 {
		nullRate = float64(nullCount) / float64(totalIndexed)
	}
	absentRate := 0.0
	if population > 0 {
		present := totalIndexed
		if present < 0 {
			present = 0
		}
		if present > population {
			present = population
		}
		absentRate = float64(population-present) / float64(population)
	}
	return nullRate, absentRate
}

func explainDomainEqualitySelectivity(summary graph.StatsPropertySummary, dominantKind, typeDomain string, nullRate, absentRate float64) float64 {
	base := summary.EstimatedSelectivity
	if base <= 0 && dominantKind != "" {
		if byKind := summary.EstimatedSelectivityByKind[dominantKind]; byKind > 0 {
			base = byKind
		}
	}
	if base <= 0 {
		if summary.DistinctValues > 0 {
			base = 1.0 / float64(summary.DistinctValues)
		} else {
			base = 0.5
		}
	}
	if typeDomain == "mixed" {
		base = 0.50
	}
	if typeDomain == "unknown" {
		base = 0.65
	}
	adjusted := base * (1.0 - nullRate) * (1.0 - absentRate)
	if adjusted <= 0 {
		adjusted = base * 0.1
	}
	if adjusted < 0.0001 {
		adjusted = 0.0001
	}
	if adjusted > 1.0 {
		adjusted = 1.0
	}
	return adjusted
}

func explainDomainRangeSelectivity(summary graph.StatsPropertySummary, dominantKind, typeDomain string, nullRate, absentRate float64) float64 {
	equality := explainDomainEqualitySelectivity(summary, dominantKind, typeDomain, nullRate, absentRate)
	base := equality * 3.0
	if base <= 0 {
		base = 0.35
	}
	if base > 1.0 {
		base = 1.0
	}
	adjusted := base * (1.0 - nullRate*0.5) * (1.0 - absentRate*0.5)
	if adjusted < 0.0001 {
		adjusted = 0.0001
	}
	if adjusted > 1.0 {
		adjusted = 1.0
	}
	return adjusted
}

func explainPropertySummaryFor(stats explainStoreStats, entityClass, schema, property string) (graph.StatsPropertySummary, bool) {
	schema = strings.TrimSpace(schema)
	property = strings.TrimSpace(property)
	if schema == "" || property == "" {
		return graph.StatsPropertySummary{}, false
	}
	switch strings.ToLower(strings.TrimSpace(entityClass)) {
	case "edge":
		if stats.edgePropertyStats == nil || stats.edgePropertyStats[schema] == nil {
			return graph.StatsPropertySummary{}, false
		}
		summary, ok := stats.edgePropertyStats[schema][property]
		return summary, ok
	default:
		if stats.vertexPropertyStats == nil || stats.vertexPropertyStats[schema] == nil {
			return graph.StatsPropertySummary{}, false
		}
		summary, ok := stats.vertexPropertyStats[schema][property]
		return summary, ok
	}
}

func explainPropertyTypeCounts(summary graph.StatsPropertySummary) map[string]int {
	counts := map[string]int{}
	for kind, value := range summary.IndexedEntriesByKind {
		if strings.EqualFold(strings.TrimSpace(kind), "null") {
			continue
		}
		if value > 0 {
			counts[kind] += value
		}
	}
	if len(counts) == 0 {
		for kind, value := range summary.DistinctValuesByKind {
			if strings.EqualFold(strings.TrimSpace(kind), "null") {
				continue
			}
			if value > 0 {
				counts[kind] += value
			}
		}
	}
	return counts
}

type explainIndexCandidate struct {
	EntityClass    string
	VertexID       string
	Schema         string
	Property       string
	PredicateClass string
	MatchedCount   int
	Quality        string
}

type explainScanPlanVertex struct {
	ID         string
	AccessPath string
}

func collectExplainIndexCandidates(stmt ast.Statement, params Params, stats explainStoreStats) []explainIndexCandidate {
	candidates := make([]explainIndexCandidate, 0)
	seen := map[string]struct{}{}
	appendCandidate := func(candidate explainIndexCandidate) {
		candidate.EntityClass = strings.TrimSpace(candidate.EntityClass)
		candidate.Schema = strings.TrimSpace(candidate.Schema)
		candidate.Property = strings.TrimSpace(candidate.Property)
		candidate.PredicateClass = explainNormalizePredicateClass(candidate.PredicateClass)
		if candidate.EntityClass == "" || candidate.Schema == "" || candidate.Property == "" {
			return
		}
		key := strings.ToLower(candidate.EntityClass) + "|" + strings.ToUpper(candidate.Schema) + "|" + strings.ToLower(candidate.Property) + "|" + candidate.PredicateClass
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, candidate)
	}
	appendVertexPattern := func(schema string, props map[string]any, matchedCount int, quality string, predicateClass string) {
		for property := range props {
			if strings.EqualFold(property, "id") {
				continue
			}
			appendCandidate(explainIndexCandidate{
				EntityClass:    "vertex",
				Schema:         strings.TrimSpace(schema),
				Property:       strings.TrimSpace(property),
				PredicateClass: explainNormalizePredicateClass(predicateClass),
				MatchedCount:   matchedCount,
				Quality:        quality,
			})
		}
	}
	appendEdgePattern := func(edgeType string, props map[string]any, quality string, predicateClass string) {
		edgeType = strings.TrimSpace(edgeType)
		if edgeType == "" {
			return
		}
		for property := range props {
			if strings.EqualFold(property, "id") {
				continue
			}
			appendCandidate(explainIndexCandidate{
				EntityClass:    "edge",
				Schema:         edgeType,
				Property:       strings.TrimSpace(property),
				PredicateClass: explainNormalizePredicateClass(predicateClass),
				MatchedCount:   0,
				Quality:        quality,
			})
		}
	}
	appendVertexWherePattern := func(vertexVar, schema, whereRaw string) {
		predicateByProperty := explainVertexWherePredicateClasses(whereRaw, vertexVar, params)
		if len(predicateByProperty) == 0 {
			return
		}
		for property, predicateClass := range predicateByProperty {
			appendVertexPattern(schema, map[string]any{property: true}, 0, "estimate", predicateClass)
		}
	}
	appendEdgePatternAnyOf := func(edgeTypes []string, props map[string]any, quality string, predicateClass string) {
		for _, edgeType := range edgeTypes {
			appendEdgePattern(edgeType, props, quality, predicateClass)
		}
	}
	appendEdgeWhereNumericPattern := func(edgeVar, edgeType string, edgeAnyOf []string, whereRaw string) {
		constraints, ok := extractEdgeWhereNumericConstraints(whereRaw, edgeVar, Row{}, params)
		if !ok || len(constraints) == 0 {
			return
		}
		for property, constraint := range constraints {
			class := "range"
			if constraint.lowerSet && constraint.upperSet && constraint.lower == constraint.upper && constraint.lowerInclusive && constraint.upperInclusive {
				class = "equality"
			}
			appendEdgePattern(edgeType, map[string]any{property: true}, "estimate", class)
			appendEdgePatternAnyOf(edgeAnyOf, map[string]any{property: true}, "estimate", class)
		}
	}
	appendEdgePatternFromRaw := func(raw string, whereRaw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		whereRaw = strings.TrimSpace(whereRaw)
		if pattern, err := parseDirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate", "equality")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate", "equality")
			}
			appendEdgeWhereNumericPattern(pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, whereRaw)
		}
		if pattern, err := parseReverseDirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate", "equality")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate", "equality")
			}
			appendEdgeWhereNumericPattern(pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, whereRaw)
		}
		if pattern, err := parseUndirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate", "equality")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate", "equality")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate", "equality")
			}
			appendEdgeWhereNumericPattern(pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, whereRaw)
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return collectExplainIndexCandidates(s.Statement, params, stats)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			whereRaw := ""
			if match.Where != nil {
				whereRaw = match.Where.Raw
			}
			if pattern, ok := tryParseExplainVertexPattern(match.Pattern); ok {
				props, err := explainPatternProperties(pattern.PropertiesRaw, params)
				if err == nil && len(props) > 0 {
					appendVertexPattern(explainPatternSchema(pattern), props, countMatchingVertices(stats, pattern, params), "exact", "equality")
				} else if keys := explainPropertyKeys(pattern.PropertiesRaw); len(keys) > 0 {
					appendVertexPattern(explainPatternSchema(pattern), keys, 0, "estimate", "equality")
				}
				appendVertexWherePattern(pattern.Var, explainPatternSchema(pattern), whereRaw)
			}
			if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				props, err := explainPropertyMap(anchored.SourcePropertiesRaw, params)
				if err == nil && len(props) > 0 {
					appendVertexPattern(anchored.SourceLabel, props, countAnchoredRows(stats, anchored, params), "exact", "equality")
				} else if keys := explainPropertyKeys(anchored.SourcePropertiesRaw); len(keys) > 0 {
					appendVertexPattern(anchored.SourceLabel, keys, 0, "estimate", "equality")
				}
				appendVertexWherePattern(anchored.SourceVar, anchored.SourceLabel, whereRaw)
			}
			appendEdgePatternFromRaw(match.Pattern, whereRaw)
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
				whereRaw := ""
				if clause.Where != nil {
					whereRaw = clause.Where.Raw
				}
				if pattern, ok := tryParseExplainVertexPattern(patternRaw); ok {
					props, err := explainPatternProperties(pattern.PropertiesRaw, params)
					if err == nil && len(props) > 0 {
						appendVertexPattern(explainPatternSchema(pattern), props, countMatchingVertices(stats, pattern, params), "exact", "equality")
					} else if keys := explainPropertyKeys(pattern.PropertiesRaw); len(keys) > 0 {
						appendVertexPattern(explainPatternSchema(pattern), keys, 0, "estimate", "equality")
					}
					appendVertexWherePattern(pattern.Var, explainPatternSchema(pattern), whereRaw)
				}
				if anchored, ok := tryParseExplainAnchoredPattern(patternRaw); ok {
					props, err := explainPropertyMap(anchored.SourcePropertiesRaw, params)
					if err == nil && len(props) > 0 {
						appendVertexPattern(anchored.SourceLabel, props, countAnchoredRows(stats, anchored, params), "exact", "equality")
					} else if keys := explainPropertyKeys(anchored.SourcePropertiesRaw); len(keys) > 0 {
						appendVertexPattern(anchored.SourceLabel, keys, 0, "estimate", "equality")
					}
					appendVertexWherePattern(anchored.SourceVar, anchored.SourceLabel, whereRaw)
				}
				appendEdgePatternFromRaw(patternRaw, whereRaw)
			}
		}
	}

	return candidates
}

func explainVertexWherePredicateClasses(whereRaw, vertexVar string, params Params) map[string]string {
	vertexVar = strings.TrimSpace(vertexVar)
	whereRaw = strings.TrimSpace(whereRaw)
	if vertexVar == "" || whereRaw == "" {
		return nil
	}
	branches, ok := explainVertexWherePredicateBranches(whereRaw)
	if !ok || len(branches) == 0 {
		return nil
	}
	merged := map[string]string{}
	for idx, branch := range branches {
		classes := explainVertexWherePredicateClassesForConjunction(branch, vertexVar, params)
		if len(classes) == 0 {
			return nil
		}
		if idx == 0 {
			for property, predicateClass := range classes {
				merged[property] = predicateClass
			}
			continue
		}
		if len(classes) != len(merged) {
			return nil
		}
		for property, existing := range merged {
			branchClass, ok := classes[property]
			if !ok {
				return nil
			}
			if existing == "range" || branchClass == "range" {
				merged[property] = "range"
				continue
			}
			merged[property] = "equality"
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func explainVertexWherePredicateBranches(whereRaw string) ([]string, bool) {
	whereRaw = strings.TrimSpace(whereRaw)
	if whereRaw == "" {
		return nil, false
	}
	if inner, wrapped := unwrapOuterParentheses(whereRaw); wrapped {
		return explainVertexWherePredicateBranches(inner)
	}
	if _, _, ok := splitTopLevelCompressedBoolean(whereRaw, "XOR"); ok {
		return nil, false
	}
	if left, right, ok := splitTopLevelKeyword(whereRaw, "XOR"); ok {
		_, _ = left, right
		return nil, false
	}
	if left, right, ok := splitTopLevelCompressedBoolean(whereRaw, "OR"); ok {
		leftBranches, leftOK := explainVertexWherePredicateBranches(left)
		rightBranches, rightOK := explainVertexWherePredicateBranches(right)
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(leftBranches, rightBranches...), true
	}
	if left, right, ok := splitTopLevelKeyword(whereRaw, "OR"); ok {
		leftBranches, leftOK := explainVertexWherePredicateBranches(left)
		rightBranches, rightOK := explainVertexWherePredicateBranches(right)
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(leftBranches, rightBranches...), true
	}
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return nil, false
	}
	return []string{whereRaw}, true
}

func explainVertexWherePredicateClassesForConjunction(whereRaw, vertexVar string, params Params) map[string]string {
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return nil
	}
	classes := map[string]string{}
	for _, conjunct := range conjuncts {
		conjunct = strings.TrimSpace(conjunct)
		if conjunct == "" || hasLogicalNotPrefix(conjunct) {
			continue
		}
		left, right, op, ok := splitTopLevelComparison(conjunct)
		if !ok {
			continue
		}
		leftProp, leftIsVertex := explainVertexPropertyReference(left, vertexVar)
		rightProp, rightIsVertex := explainVertexPropertyReference(right, vertexVar)
		if leftIsVertex == rightIsVertex {
			continue
		}
		property := leftProp
		scalarExpr := right
		normalizedOp := strings.TrimSpace(op)
		if rightIsVertex {
			property = rightProp
			scalarExpr = left
			normalizedOp = reverseComparisonOperator(normalizedOp)
			if normalizedOp == "" {
				continue
			}
		}
		if strings.TrimSpace(property) == "" {
			continue
		}
		scalarValue, err := evalExpressionWithScope(scalarExpr, Row{}, params)
		if err != nil {
			continue
		}
		predicateClass := ""
		switch normalizedOp {
		case "=":
			predicateClass = "equality"
		case ">", ">=", "<", "<=":
			if _, numeric := comparableNumericValue(scalarValue); !numeric {
				continue
			}
			predicateClass = "range"
		default:
			continue
		}
		if existing, has := classes[property]; !has || (existing == "equality" && predicateClass == "range") {
			classes[property] = predicateClass
		}
	}
	if len(classes) == 0 {
		return nil
	}
	return classes
}

func explainVertexPropertyReference(expr, vertexVar string) (string, bool) {
	vertexVar = strings.TrimSpace(vertexVar)
	if vertexVar == "" {
		return "", false
	}
	base, fields, ok := splitTopLevelFieldAccess(expr)
	if !ok || len(fields) != 1 {
		return "", false
	}
	base = strings.TrimSpace(base)
	if inner, wrapped := unwrapOuterParentheses(base); wrapped {
		base = strings.TrimSpace(inner)
	}
	if base != vertexVar {
		return "", false
	}
	property := strings.TrimSpace(fields[0])
	if property == "" || strings.EqualFold(property, "id") {
		return "", false
	}
	return property, true
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

type explainVertexMatcher struct {
	key     string
	pattern vertexPattern
}

func collectExplainVertexMatchers(stmt ast.Statement) []explainVertexMatcher {
	out := make([]explainVertexMatcher, 0)
	seen := map[string]struct{}{}
	appendMatcher := func(pattern vertexPattern) {
		key := explainPatternFingerprint(pattern)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, explainVertexMatcher{key: key, pattern: pattern})
	}
	collectFromRaw := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if pattern, ok := tryParseExplainVertexPattern(raw); ok {
			appendMatcher(pattern)
		}
		if anchored, ok := tryParseExplainAnchoredPattern(raw); ok {
			appendMatcher(anchored.asNodePattern())
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return collectExplainVertexMatchers(s.Statement)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			collectFromRaw(match.Pattern)
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
				collectFromRaw(patternRaw)
			}
		}
	}

	return out
}

func explainPatternFingerprint(pattern vertexPattern) string {
	props := strings.TrimSpace(pattern.PropertiesRaw)
	if props == "" && len(pattern.AllOfLabels) == 0 && len(pattern.AnyOfLabels) == 0 && len(pattern.ExcludedLabels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("all=")
	b.WriteString(strings.Join(pattern.AllOfLabels, "\x1f"))
	b.WriteString("|any=")
	b.WriteString(strings.Join(pattern.AnyOfLabels, "\x1f"))
	b.WriteString("|none=")
	b.WriteString(strings.Join(pattern.ExcludedLabels, "\x1f"))
	b.WriteString("|props=")
	b.WriteString(props)
	return b.String()
}

func explainScanPlanVertexes(vertexes []map[string]any) []explainScanPlanVertex {
	out := make([]explainScanPlanVertex, 0)
	for _, vertex := range vertexes {
		op, _ := vertex["op"].(string)
		if !isScanOperator(op) {
			continue
		}
		id, _ := vertex["id"].(string)
		accessPath, _ := vertex["accessPath"].(string)
		out = append(out, explainScanPlanVertex{ID: id, AccessPath: accessPath})
	}
	return out
}

func isScanOperator(op string) bool {
	switch op {
	case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN", "ALL_VERTEXES_SCAN", "OPTIONAL_ALL_VERTEXES_SCAN", "EDGE_SCAN", "OPTIONAL_EDGE_SCAN":
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

func explainEdgeTypePopulation(stats explainStoreStats, edgeType string) int {
	edgeType = strings.TrimSpace(edgeType)
	if edgeType == "" {
		return stats.edgeTotal
	}
	if count, ok := stats.edgeCounts[edgeType]; ok {
		return count
	}
	return stats.edgeTotal
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

func buildExplainCardinalityFromPlanNodes(vertexes []map[string]any, params Params, stats explainStoreStats) []map[string]any {
	entries := make([]map[string]any, 0, len(vertexes))
	rows := 1
	for _, vertex := range vertexes {
		vertexID, _ := vertex["id"].(string)
		op, _ := vertex["op"].(string)
		rowsIn := rows
		rowsOut := rows
		quality := "estimate"
		switch op {
		case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN":
			quality = "exact"
			if predicate, _ := vertex["predicate"].(string); strings.TrimSpace(predicate) != "" {
				rowsOut = 1
			} else {
				rowsOut = rowsIn
			}
		case "ALL_VERTEXES_SCAN", "OPTIONAL_ALL_VERTEXES_SCAN":
			quality = "exact"
			rowsOut = stats.vertexTotal
			if op == "OPTIONAL_ALL_VERTEXES_SCAN" && rowsOut == 0 {
				rowsOut = 1
			}
		case "EDGE_SCAN", "OPTIONAL_EDGE_SCAN":
			quality = "exact"
			edgeRows := stats.edgeTotal
			rowsOut = edgeRows
			if variableLength, _ := vertex["variableLength"].(bool); variableLength {
				quality = "estimate"
			}
			if edgeType, _ := vertex["edgeType"].(string); strings.TrimSpace(edgeType) != "" {
				if count, ok := stats.edgeCounts[edgeType]; ok {
					edgeRows = count
					rowsOut = count
				}
			}
			if edgeAnyOf, ok := vertex["edgeAnyOf"].([]string); ok && len(edgeAnyOf) > 0 {
				total := 0
				for _, edgeType := range edgeAnyOf {
					total += stats.edgeCounts[edgeType]
				}
				edgeRows = total
				rowsOut = total
			}
			if sourceVar, _ := vertex["sourceVar"].(string); strings.TrimSpace(sourceVar) != "" && rowsIn > 0 {
				sourcePopulation := explainEdgeScanSourcePopulation(vertex, stats)
				if sourcePopulation > 0 {
					avgFanout := float64(edgeRows) / float64(sourcePopulation)
					rowsOut = int(math.Round(float64(rowsIn) * avgFanout))
					if rowsOut < 0 {
						rowsOut = 0
					}
					quality = "estimate"
				}
			}
			if variableLength, _ := vertex["variableLength"].(bool); variableLength {
				if maxHops, ok := vertex["maxHops"].(int); ok {
					if maxHops > 1 {
						rowsOut *= maxHops
					}
					if maxHops < 0 {
						rowsOut *= 2
					}
				}
			}
			if op == "OPTIONAL_EDGE_SCAN" && rowsOut == 0 {
				rowsOut = 1
			}
		case "AGGREGATE":
			if impl, _ := vertex["implementation"].(string); impl == "fast_vertex_count" {
				rowsOut = 1
				quality = "exact"
			}
		case "FILTER":
			rowsOut = rowsIn
			if bookkeepingOnly, _ := vertex["bookkeepingOnly"].(bool); bookkeepingOnly {
				quality = "exact"
				break
			}
			if covered, _ := vertex["wherePrefilterCoverage"].(bool); covered {
				quality = "exact"
				break
			}
			if covered, _ := vertex["whereShortcutCoverage"].(bool); covered {
				// Shortcut-covered predicates are still enforced in the execution path;
				// retain row count unless additional heuristic signals apply.
				quality = "estimate"
			}
			predicate := strings.ToLower(strings.TrimSpace(fmt.Sprint(vertex["predicate"])))
			if predicate == "" || rowsIn <= 0 {
				break
			}
			selectivity := 1.0
			if strings.Contains(predicate, "user_id:") || strings.Contains(predicate, "movie_id:") {
				rowsOut = 1
				quality = "estimate"
				break
			}
			if strings.Contains(predicate, "<>") || strings.Contains(predicate, "!=") {
				selectivity *= 0.98
			}
			if strings.Contains(predicate, "abs(") && (strings.Contains(predicate, "<=") || strings.Contains(predicate, "<")) {
				selectivity *= 0.55
			}
			if strings.Contains(predicate, "not (") {
				selectivity *= 0.80
			}
			if selectivity < 1.0 {
				rowsOut = int(math.Round(float64(rowsIn) * selectivity))
				if rowsOut < 0 {
					rowsOut = 0
				}
				quality = "estimate"
			}
		case "SKIP":
			if page, ok := vertex["pagination"].(map[string]any); ok {
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
			if page, ok := vertex["pagination"].(map[string]any); ok {
				if limitRaw, ok := page["limit"].(string); ok {
					if limit, ok := explainPaginationValue(limitRaw, params); ok && rowsOut > limit {
						rowsOut = limit
					}
				}
			}
		default:
			rowsOut = rowsIn
		}
		entry := map[string]any{
			"vertexId": vertexID,
			"rowsIn":   rowsIn,
			"rowsOut":  rowsOut,
			"quality":  quality,
			"op":       op,
		}
		entry["assessment"] = map[string]any{
			"vertexId": vertexID,
			"rowsIn":   rowsIn,
			"rowsOut":  rowsOut,
			"quality":  quality,
			"op":       op,
		}
		if bindings := explainQueryBindingsForPlanVertex(vertex); len(bindings) == 1 {
			entry["queryBinding"] = bindings[0]
		} else if len(bindings) > 1 {
			entry["queryBindings"] = bindings
		}
		entries = append(entries, entry)
		rows = rowsOut
	}
	return entries
}

func explainEdgeScanSourcePopulation(vertex map[string]any, stats explainStoreStats) int {
	direction, _ := vertex["matchDirection"].(string)
	labelKey := "leftLabelFilter"
	if strings.EqualFold(strings.TrimSpace(direction), "in") {
		labelKey = "rightLabelFilter"
	}
	label, _ := vertex[labelKey].(string)
	label = strings.TrimSpace(label)
	if label != "" {
		if count, ok := stats.labelCounts[label]; ok && count > 0 {
			return count
		}
	}
	if stats.vertexTotal > 0 {
		return stats.vertexTotal
	}
	return 0
}

func explainQueryBindingsForPlanVertex(vertex map[string]any) []string {
	bindings := make([]string, 0, 3)
	appendBinding := func(raw any) {
		name := strings.TrimSpace(fmt.Sprint(raw))
		if name == "" || name == "<nil>" {
			return
		}
		for _, existing := range bindings {
			if existing == name {
				return
			}
		}
		bindings = append(bindings, name)
	}
	appendBinding(vertex["leftVar"])
	appendBinding(vertex["edgeVar"])
	appendBinding(vertex["rightVar"])
	appendBinding(vertex["vertexVar"])
	appendBinding(vertex["sourceVar"])
	appendBinding(vertex["targetVar"])
	return bindings
}

func buildExplainCostEstimate(planVertexes []map[string]any, cardinality []map[string]any, indexDecisions []map[string]any, feedbackFn func(string) (map[string]any, bool)) map[string]any {
	rowsOutByVertex := map[string]int{}
	exactCardinality := len(cardinality) > 0
	for _, entry := range cardinality {
		vertexID, _ := entry["vertexId"].(string)
		if strings.TrimSpace(vertexID) == "" {
			continue
		}
		rowsOut, _ := entry["rowsOut"].(int)
		rowsOutByVertex[vertexID] = rowsOut
		if quality, _ := entry["quality"].(string); quality != "exact" {
			exactCardinality = false
		}
	}

	scanRows := 0
	outputRows := 0
	feedbackObserved := false
	feedbackImplementation := ""
	feedbackSamples := int64(0)
	feedbackSelectivity := 0.0
	feedbackInputRows := int64(0)
	feedbackOutputRows := int64(0)
	for _, vertex := range planVertexes {
		vertexID, _ := vertex["id"].(string)
		op, _ := vertex["op"].(string)
		rowsOut := rowsOutByVertex[vertexID]
		if isScanOperator(op) {
			scanRows += rowsOut
		}
		outputRows = rowsOut
		if feedbackObserved || feedbackFn == nil {
			continue
		}
		implementation, _ := vertex["implementation"].(string)
		implementation = strings.TrimSpace(implementation)
		if implementation == "" {
			continue
		}
		feedback, ok := feedbackFn(implementation)
		if !ok {
			continue
		}
		if samples, ok := feedback["samples"].(int64); ok && samples > 0 {
			feedbackSamples = samples
		}
		if inputRows, ok := feedback["inputRows"].(int64); ok && inputRows > 0 {
			feedbackInputRows = inputRows
		}
		if outputRowsObserved, ok := feedback["outputRows"].(int64); ok && outputRowsObserved >= 0 {
			feedbackOutputRows = outputRowsObserved
			outputRows = int(outputRowsObserved)
		}
		if selectivity, ok := feedback["selectivity"].(float64); ok && selectivity >= 0 {
			feedbackSelectivity = selectivity
			if feedbackOutputRows == 0 && feedbackInputRows > 0 {
				feedbackOutputRows = int64(math.Round(float64(feedbackInputRows) * selectivity))
				outputRows = int(feedbackOutputRows)
			}
		}
		feedbackImplementation = implementation
		feedbackObserved = true
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
	if feedbackObserved {
		feedbackAdjustment := 0
		if feedbackOutputRows > 0 {
			feedbackAdjustment = int(feedbackOutputRows) - rowsOutByVertex[lastScanVertexID(planVertexes)]
		}
		total = scanRows + outputRows + missingIndexPenalty
		if feedbackAdjustment != 0 {
			total += feedbackAdjustment
		}
	}
	if total < 1 {
		total = 1
	}
	quality := "estimate"
	if exactCardinality {
		quality = "exact"
	}
	if feedbackObserved {
		quality = "sample"
	}

	components := map[string]any{
		"scanRows":            scanRows,
		"outputRows":          outputRows,
		"missingIndexPenalty": missingIndexPenalty,
	}
	assessment := map[string]any{
		"quality":             quality,
		"value":               total,
		"unit":                "work_units",
		"scanRows":            scanRows,
		"outputRows":          outputRows,
		"missingIndexPenalty": missingIndexPenalty,
	}
	if feedbackObserved {
		components["fastPathObservedImplementation"] = feedbackImplementation
		components["fastPathObservedSelectivity"] = feedbackSelectivity
		components["fastPathObservedSamples"] = feedbackSamples
		components["fastPathObservedInputRows"] = feedbackInputRows
		components["fastPathObservedOutputRows"] = feedbackOutputRows
		assessment["feedback"] = map[string]any{
			"implementation": feedbackImplementation,
			"samples":        feedbackSamples,
			"inputRows":      feedbackInputRows,
			"outputRows":     feedbackOutputRows,
			"selectivity":    feedbackSelectivity,
			"quality":        quality,
			"source":         "runtime",
		}
	}

	return map[string]any{
		"model":      "heuristic-v1",
		"value":      total,
		"unit":       "work_units",
		"quality":    quality,
		"components": components,
		"assessment": assessment,
	}
}

func lastScanVertexID(planVertexes []map[string]any) string {
	for i := len(planVertexes) - 1; i >= 0; i-- {
		vertexID, _ := planVertexes[i]["id"].(string)
		if strings.TrimSpace(vertexID) != "" {
			return vertexID
		}
	}
	return ""
}

func buildExplainRuntimeStats(planVertexes []map[string]any, cardinality []map[string]any, indexDecisions []map[string]any, fastPaths []map[string]any, warnings []map[string]any, stats explainStoreStats) map[string]any {
	scanVertexes := 0
	filterVertexes := 0
	projectionVertexes := 0
	sortVertexes := 0
	paginationVertexes := 0
	writeVertexes := 0

	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		if isScanOperator(op) {
			scanVertexes++
		}
		switch op {
		case "FILTER":
			filterVertexes++
		case "PROJECT", "AGGREGATE":
			projectionVertexes++
		case "SORT":
			sortVertexes++
		case "SKIP", "LIMIT":
			paginationVertexes++
		case "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH_DELETE":
			writeVertexes++
		}
	}

	indexCandidates := len(indexDecisions)
	indexSelected := 0
	typedIndexSelected := 0
	mixedTypeFallbacks := 0
	for _, decision := range indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			indexSelected++
		}
		if strategy, _ := decision["typedSeekStrategy"].(string); strategy == "typed_property_index_seek" {
			typedIndexSelected++
		}
		if strategy, _ := decision["typedSeekStrategy"].(string); strategy == "mixed_type_fallback" {
			mixedTypeFallbacks++
		}
	}

	rowsRead := 0
	rowsOutput := 0
	rowsOutByVertex := map[string]int{}
	bookkeepingOnlyFilterVertexes := map[string]struct{}{}
	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		if op != "FILTER" {
			continue
		}
		if bookkeepingOnly, _ := vertex["bookkeepingOnly"].(bool); !bookkeepingOnly {
			continue
		}
		vertexID, _ := vertex["id"].(string)
		if strings.TrimSpace(vertexID) == "" {
			continue
		}
		bookkeepingOnlyFilterVertexes[vertexID] = struct{}{}
	}
	allExactCardinality := len(cardinality) > 0
	for _, entry := range cardinality {
		vertexID, _ := entry["vertexId"].(string)
		rowsIn, _ := entry["rowsIn"].(int)
		rowsOut, _ := entry["rowsOut"].(int)
		if _, skip := bookkeepingOnlyFilterVertexes[vertexID]; !skip {
			rowsRead += rowsIn
		}
		rowsOutput = rowsOut
		if strings.TrimSpace(vertexID) != "" {
			rowsOutByVertex[vertexID] = rowsOut
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
	for _, vertex := range planVertexes {
		vertexID, _ := vertex["id"].(string)
		op, _ := vertex["op"].(string)
		rowsOut := rowsOutByVertex[vertexID]
		switch op {
		case "INDEX_SCAN", "OPTIONAL_INDEX_SCAN", "LABEL_SCAN", "OPTIONAL_LABEL_SCAN", "ALL_VERTEXES_SCAN", "OPTIONAL_ALL_VERTEXES_SCAN":
			verticesScanned += rowsOut
		case "EDGE_SCAN", "OPTIONAL_EDGE_SCAN":
			edgesScanned += rowsOut
		}
	}
	if verticesScanned == 0 && edgesScanned == 0 {
		verticesScanned = stats.vertexTotal
		edgesScanned = stats.edgeTotal
	}

	implementations := make([]string, 0, len(fastPaths))
	prefilterBypassCandidates := 0
	whereShortcutCandidates := 0
	topKPushdownCandidates := 0
	topKAdaptiveDisabledCandidates := 0
	lateMaterializationCandidates := 0
	fastPathFeedbackCandidates := 0
	for _, path := range fastPaths {
		implementation, _ := path["implementation"].(string)
		if strings.TrimSpace(implementation) != "" {
			implementations = append(implementations, implementation)
		}
		if covered, _ := path["wherePrefilterCoverage"].(bool); covered {
			prefilterBypassCandidates++
		}
		if covered, _ := path["whereShortcutCoverage"].(bool); covered {
			whereShortcutCandidates++
		}
		if eligible, _ := path["topKPushdown"].(bool); eligible {
			topKPushdownCandidates++
		}
		if disabled, _ := path["adaptiveTopKDisabled"].(bool); disabled {
			topKAdaptiveDisabledCandidates++
		}
		if eligible, _ := path["lateMaterialization"].(bool); eligible {
			lateMaterializationCandidates++
		}
		if quality, _ := path["quality"].(string); quality == "sample" {
			fastPathFeedbackCandidates++
		}
	}
	operatorStats := buildExplainOperatorRuntimeStats(planVertexes)
	operatorAssessment := explainBuildOperatorAssessment(operatorStats)
	warningSummary := buildExplainWarningSummary(warnings)
	diagnosticPosture := buildExplainDiagnosticPosture(warningSummary, operatorAssessment)

	return map[string]any{
		"store": map[string]any{
			"verticesScanned": verticesScanned,
			"edgesScanned":    edgesScanned,
		},
		"plan": map[string]any{
			"totalVertexes":      len(planVertexes),
			"scanVertexes":       scanVertexes,
			"filterVertexes":     filterVertexes,
			"projectionVertexes": projectionVertexes,
			"sortVertexes":       sortVertexes,
			"paginationVertexes": paginationVertexes,
			"writeVertexes":      writeVertexes,
			"warningCount":       len(warnings),
		},
		"index": map[string]any{
			"candidates":     indexCandidates,
			"selected":       indexSelected,
			"typedSelected":  typedIndexSelected,
			"mixedFallbacks": mixedTypeFallbacks,
			"missing":        indexCandidates - indexSelected,
		},
		"cardinality": map[string]any{
			"rowsRead":   rowsRead,
			"rowsOutput": rowsOutput,
			"quality":    cardinalityQuality,
		},
		"execution": map[string]any{
			"fastPathCandidates":             len(fastPaths),
			"implementations":                implementations,
			"prefilterBypassCandidates":      prefilterBypassCandidates,
			"whereShortcutCandidates":        whereShortcutCandidates,
			"topKPushdownCandidates":         topKPushdownCandidates,
			"topKAdaptiveDisabledCandidates": topKAdaptiveDisabledCandidates,
			"lateMaterializationCandidates":  lateMaterializationCandidates,
			"fastPathFeedbackCandidates":     fastPathFeedbackCandidates,
		},
		"diagnosticPosture":  diagnosticPosture,
		"warningSummary":     warningSummary,
		"operatorAssessment": operatorAssessment,
		"operators":          explainNormalizeOperatorStatsForPayload(operatorStats),
	}
}

func explainNormalizeOperatorStatsForPayload(stats map[string]any) map[string]any {
	if len(stats) == 0 {
		return map[string]any{}
	}
	normalized := make(map[string]any, len(stats))
	for key, value := range stats {
		normalized[key] = value
	}
	for _, key := range []string{"distinctShapes", "sortShapes", "groupShapes", "distinctBlockedReasons", "sortBlockedReasons", "groupBlockedReasons"} {
		counts, ok := normalized[key].(map[string]int)
		if !ok {
			continue
		}
		normalized[key] = explainIntCountMapAsAny(counts)
	}
	return normalized
}

func buildExplainDiagnosticPosture(warningSummary map[string]any, operatorAssessment map[string]any) map[string]any {
	primary := "healthy"
	postureSignals := []string{}
	recommendation := "maintain_typed_paths"
	confidence := "medium"
	rationale := ""
	score := 50
	scoreBand := "fair"
	baseScore := 50
	confidenceAdjustment := 0
	warningVolumePenalty := 0
	categoryPenalty := 0
	totalWarnings, _ := warningSummary["totalWarnings"].(int)
	overallStatus, _ := operatorAssessment["overallStatus"].(string)
	highestPriorityCode, _ := warningSummary["highestPriorityCode"].(string)
	highestCategory, _ := warningSummary["highestCategory"].(string)
	byCategory := map[string]int{}

	if categoryCounts, ok := warningSummary["byCategory"].(map[string]int); ok {
		byCategory = categoryCounts
		if byCategory["planner"] > 0 {
			primary = "planner_limited"
			postureSignals = append(postureSignals, "planner")
			recommendation = "optimize_scan_and_plan_shapes"
		} else if byCategory["index"] > 0 {
			primary = "index_limited"
			postureSignals = append(postureSignals, "index")
			recommendation = "improve_index_coverage"
		}
	}

	if overallStatus == "mixed_domain_risk" || overallStatus == "fallback_likely" {
		if primary == "healthy" {
			primary = "operator_fallback_limited"
		}
		postureSignals = append(postureSignals, "operator")
		recommendation = "reduce_operator_fallback_shapes"
	}

	if totalWarnings == 0 {
		if overallStatus == "typed_friendly" {
			primary = "healthy"
			postureSignals = []string{"typed_friendly"}
			recommendation = "maintain_typed_paths"
		}
	}

	if len(postureSignals) == 0 {
		postureSignals = append(postureSignals, primary)
	}

	switch {
	case primary == "healthy" && totalWarnings == 0 && overallStatus == "typed_friendly":
		confidence = "high"
	case totalWarnings >= 3 || len(postureSignals) > 1:
		confidence = "low"
	default:
		confidence = "medium"
	}

	switch primary {
	case "healthy":
		rationale = "No explain warnings and typed-friendly operator assessment"
	case "index_limited":
		rationale = fmt.Sprintf("Index warnings dominate diagnostics; highestPriorityCode=%s", highestPriorityCode)
	case "planner_limited":
		rationale = fmt.Sprintf("Planner warnings dominate diagnostics; highestPriorityCode=%s", highestPriorityCode)
	case "operator_fallback_limited":
		rationale = fmt.Sprintf("Operator assessment indicates %s with fallback-oriented signals", overallStatus)
	default:
		rationale = fmt.Sprintf("Diagnostic posture derived from signals=%s and highestPriorityCode=%s", strings.Join(postureSignals, ","), highestPriorityCode)
	}

	switch primary {
	case "healthy":
		baseScore = 90
	case "operator_fallback_limited":
		baseScore = 60
	case "index_limited":
		baseScore = 45
	case "planner_limited":
		baseScore = 35
	default:
		baseScore = 50
	}
	score = baseScore
	switch confidence {
	case "high":
		confidenceAdjustment = 5
	case "low":
		confidenceAdjustment = -10
	}
	score += confidenceAdjustment
	warningVolumePenalty = totalWarnings * 5
	score -= warningVolumePenalty
	weights := explainDiagnosticScoreCategoryWeights()
	for category, count := range byCategory {
		if count <= 1 {
			continue
		}
		categoryPenalty += weights[category] * (count - 1)
	}
	score += categoryPenalty
	rawScoreBeforeClamp := score
	clampRule := "within_bounds"
	if score < 0 {
		score = 0
		clampRule = "clamped_min"
	}
	if score > 100 {
		score = 100
		clampRule = "clamped_max"
	}

	switch {
	case score >= 80:
		scoreBand = "excellent"
	case score >= 60:
		scoreBand = "good"
	case score >= 40:
		scoreBand = "fair"
	default:
		scoreBand = "poor"
	}
	trendHint := explainDiagnosticPostureTrendHint(score, byCategory)
	trendScore := explainDiagnosticPostureTrendScore(trendHint)
	trendRule := explainDiagnosticPostureTrendRule(score, byCategory)
	trendScoreRule := explainDiagnosticPostureTrendScoreRule(trendHint)
	trendEvidence := explainDiagnosticPostureTrendEvidence(postureSignals, totalWarnings, highestPriorityCode, highestCategory)
	trendDriverWeights := explainDiagnosticPostureTrendDriverWeights(byCategory)
	baseRule := explainDiagnosticPostureBaseRule(primary)
	confidenceAdjustmentRule := explainDiagnosticPostureConfidenceAdjustmentRule(confidence)
	warningVolumePenaltyRule := explainDiagnosticPostureWarningVolumePenaltyRule(totalWarnings)
	scoreBandRule := explainDiagnosticPostureScoreBandRule(scoreBand)
	scoreInputs := explainDiagnosticPostureScoreInputs(totalWarnings, confidence, byCategory)
	primaryEvaluationOrder := explainDiagnosticPosturePrimaryEvaluationOrder(byCategory, totalWarnings, overallStatus)
	primaryRule := "default"
	if len(primaryEvaluationOrder) > 0 {
		primaryRule = primaryEvaluationOrder[len(primaryEvaluationOrder)-1]
	}
	confidenceRule := explainDiagnosticPostureConfidenceRule(primary, totalWarnings, overallStatus, len(postureSignals))
	recommendationEvaluationOrder := explainDiagnosticPostureRecommendationEvaluationOrder(byCategory, totalWarnings, overallStatus)
	recommendationRule := "default"
	if len(recommendationEvaluationOrder) > 0 {
		recommendationRule = recommendationEvaluationOrder[len(recommendationEvaluationOrder)-1]
	}
	rationaleRule := explainDiagnosticPostureRationaleRule(primary)
	rationaleTemplate := explainDiagnosticPostureRationaleTemplate(primary)
	scoreComputationVersion := "v1"
	scoreComputationConfig := explainDiagnosticPostureScoreComputationConfig()
	scoreClampRange := explainDiagnosticPostureScoreClampRange()
	contractComponents := explainDiagnosticPostureContractComponents(scoreComputationConfig)
	contractCompatibility := explainDiagnosticPostureContractCompatibility(contractComponents)
	contractHash := ""
	if overallHash, _ := contractComponents["overallHash"].(string); overallHash != "" {
		contractHash = overallHash
	} else {
		contractHash = explainDiagnosticPostureContractHash(scoreComputationConfig)
	}
	decisionTraceVersion := "v1"
	decisionTrace := []map[string]any{
		{
			"stage":  "primary",
			"rule":   primaryRule,
			"result": primary,
			"reason": explainDiagnosticPosturePrimaryRuleReason(primaryRule),
			"inputs": map[string]any{
				"overallStatus": overallStatus,
				"totalWarnings": totalWarnings,
				"byCategory":    byCategory,
			},
		},
		{
			"stage":  "confidence",
			"rule":   confidenceRule,
			"result": confidence,
			"reason": explainDiagnosticPostureConfidenceRuleReason(confidenceRule),
			"inputs": map[string]any{
				"primary":       primary,
				"overallStatus": overallStatus,
				"totalWarnings": totalWarnings,
				"signalCount":   len(postureSignals),
			},
		},
		{
			"stage":  "recommendation",
			"rule":   recommendationRule,
			"result": recommendation,
			"reason": explainDiagnosticPostureRecommendationRuleReason(recommendationRule),
			"inputs": map[string]any{
				"primary":       primary,
				"overallStatus": overallStatus,
				"totalWarnings": totalWarnings,
				"signals":       append([]string(nil), postureSignals...),
			},
		},
		{
			"stage":  "rationale",
			"rule":   rationaleRule,
			"result": rationaleTemplate,
			"reason": explainDiagnosticPostureRationaleRuleReason(rationaleRule),
			"inputs": map[string]any{
				"primary":             primary,
				"highestPriorityCode": highestPriorityCode,
				"overallStatus":       overallStatus,
				"signals":             append([]string(nil), postureSignals...),
			},
		},
		{
			"stage":  "score",
			"rule":   scoreBandRule,
			"result": scoreBand,
			"reason": explainDiagnosticPostureScoreBandRuleReason(scoreBandRule),
			"inputs": map[string]any{
				"baseRule":                 baseRule,
				"confidenceAdjustmentRule": confidenceAdjustmentRule,
				"warningVolumePenaltyRule": warningVolumePenaltyRule,
				"categoryPenaltyRule":      "repeated_category_weighted_penalty",
				"rawScoreBeforeClamp":      rawScoreBeforeClamp,
				"clampRule":                clampRule,
			},
		},
		{
			"stage":  "trend",
			"rule":   trendRule,
			"result": trendHint,
			"reason": explainDiagnosticPostureTrendRuleReason(trendRule),
			"inputs": map[string]any{
				"score":               score,
				"degradingCategories": []string{"planner", "index"},
				"byCategory":          byCategory,
				"trendScoreRule":      trendScoreRule,
			},
		},
	}
	ruleReasonCatalog := explainDiagnosticPostureRuleReasonCatalog()
	matchedReasonLookup := explainDiagnosticPostureMatchedReasonLookup(decisionTrace, ruleReasonCatalog)
	reasonCatalogMismatches := explainDiagnosticPostureReasonCatalogMismatches(matchedReasonLookup)

	return map[string]any{
		"primary":                 primary,
		"signals":                 postureSignals,
		"recommendation":          recommendation,
		"confidence":              confidence,
		"rationale":               rationale,
		"score":                   score,
		"scoreBand":               scoreBand,
		"trendHint":               trendHint,
		"trendScore":              trendScore,
		"trendEvidence":           trendEvidence,
		"trendDriverWeights":      trendDriverWeights,
		"scoreInputs":             scoreInputs,
		"scoreComputationVersion": scoreComputationVersion,
		"scoreComputationConfig":  scoreComputationConfig,
		"scoreClampRange":         scoreClampRange,
		"evaluatedPolicy": map[string]any{
			"decisionTraceVersion":          decisionTraceVersion,
			"validationMode":                "strict",
			"contractHash":                  contractHash,
			"contractComponents":            contractComponents,
			"compatibility":                 contractCompatibility,
			"reasonCatalogVersion":          "v1",
			"reasonCatalogConsistent":       len(reasonCatalogMismatches) == 0,
			"reasonCatalogMismatches":       reasonCatalogMismatches,
			"matchedReasonLookup":           matchedReasonLookup,
			"decisionTrace":                 decisionTrace,
			"primaryRule":                   primaryRule,
			"primaryEvaluationOrder":        primaryEvaluationOrder,
			"confidenceRule":                confidenceRule,
			"recommendationRule":            recommendationRule,
			"recommendationEvaluationOrder": recommendationEvaluationOrder,
			"rationaleRule":                 rationaleRule,
			"rationaleTemplate":             rationaleTemplate,
			"rationaleInputs": map[string]any{
				"highestPriorityCode": highestPriorityCode,
				"overallStatus":       overallStatus,
				"signals":             append([]string(nil), postureSignals...),
				"totalWarnings":       totalWarnings,
			},
			"scoreRuleTrace": map[string]any{
				"baseRule":                     baseRule,
				"confidenceAdjustmentRule":     confidenceAdjustmentRule,
				"warningVolumePenaltyRule":     warningVolumePenaltyRule,
				"categoryPenaltyContributions": trendDriverWeights,
				"rawScoreBeforeClamp":          rawScoreBeforeClamp,
				"clampRule":                    clampRule,
				"scoreBandRule":                scoreBandRule,
			},
			"trendRuleTrace": map[string]any{
				"trendRule":      trendRule,
				"trendScoreRule": trendScoreRule,
				"trendInputs": map[string]any{
					"score":               score,
					"degradingCategories": []string{"planner", "index"},
					"byCategory":          byCategory,
				},
			},
		},
		"scoreBreakdown": map[string]int{
			"base":                 baseScore,
			"confidenceAdjustment": confidenceAdjustment,
			"warningVolumePenalty": warningVolumePenalty,
			"categoryPenalty":      categoryPenalty,
			"final":                score,
		},
	}
}

func explainDiagnosticPostureBaseRule(primary string) string {
	return fmt.Sprintf("base_primary_%s", primary)
}

func explainDiagnosticPostureConfidenceAdjustmentRule(confidence string) string {
	switch confidence {
	case "high":
		return "confidence_adjustment_high"
	case "low":
		return "confidence_adjustment_low"
	default:
		return "confidence_adjustment_default"
	}
}

func explainDiagnosticPostureWarningVolumePenaltyRule(totalWarnings int) string {
	if totalWarnings > 0 {
		return "warning_penalty_applied"
	}
	return "warning_penalty_skipped"
}

func explainDiagnosticPostureScoreBandRule(scoreBand string) string {
	return fmt.Sprintf("score_band_%s", scoreBand)
}

func explainDiagnosticPostureScoreBandRuleReason(rule string) string {
	switch rule {
	case "score_band_excellent":
		return "final score mapped to excellent band"
	case "score_band_good":
		return "final score mapped to good band"
	case "score_band_fair":
		return "final score mapped to fair band"
	default:
		return "final score mapped to poor band"
	}
}

func explainDiagnosticPosturePrimaryEvaluationOrder(byCategory map[string]int, totalWarnings int, overallStatus string) []string {
	order := make([]string, 0, 3)
	if byCategory["planner"] > 0 {
		order = append(order, "category_priority_planner")
	} else if byCategory["index"] > 0 {
		order = append(order, "category_priority_index")
	}
	if overallStatus == "mixed_domain_risk" || overallStatus == "fallback_likely" {
		order = append(order, "operator_fallback_status")
	}
	if totalWarnings == 0 && overallStatus == "typed_friendly" {
		order = append(order, "typed_friendly_reset")
	}
	if len(order) == 0 {
		order = append(order, "default")
	}
	return order
}

func explainDiagnosticPosturePrimaryRuleReason(rule string) string {
	switch rule {
	case "category_priority_planner":
		return "planner category warning took primary precedence"
	case "category_priority_index":
		return "index category warning set primary posture"
	case "operator_fallback_status":
		return "operator assessment indicates fallback risk"
	case "typed_friendly_reset":
		return "typed-friendly assessment with zero warnings reset posture"
	default:
		return "default primary posture applied"
	}
}

func explainDiagnosticPostureConfidenceRule(primary string, totalWarnings int, overallStatus string, signalCount int) string {
	switch {
	case primary == "healthy" && totalWarnings == 0 && overallStatus == "typed_friendly":
		return "high_healthy_typed_friendly"
	case totalWarnings >= 3 || signalCount > 1:
		return "low_warning_or_multisignal"
	default:
		return "default_medium"
	}
}

func explainDiagnosticPostureConfidenceRuleReason(rule string) string {
	switch rule {
	case "high_healthy_typed_friendly":
		return "healthy posture with zero warnings and typed-friendly assessment"
	case "low_warning_or_multisignal":
		return "warning volume or multi-signal posture lowered confidence"
	default:
		return "default medium confidence classification"
	}
}

func explainDiagnosticPostureRecommendationEvaluationOrder(byCategory map[string]int, totalWarnings int, overallStatus string) []string {
	order := make([]string, 0, 4)
	if byCategory["planner"] > 0 {
		order = append(order, "primary_planner")
	} else if byCategory["index"] > 0 {
		order = append(order, "primary_index")
	}
	if overallStatus == "mixed_domain_risk" || overallStatus == "fallback_likely" {
		order = append(order, "operator_status_fallback")
	}
	if totalWarnings == 0 && overallStatus == "typed_friendly" {
		order = append(order, "typed_friendly_reset")
	}
	if len(order) == 0 {
		order = append(order, "default")
	}
	return order
}

func explainDiagnosticPostureRecommendationRuleReason(rule string) string {
	switch rule {
	case "primary_planner":
		return "planner-limited posture selected planner optimization recommendation"
	case "primary_index":
		return "index-limited posture selected index coverage recommendation"
	case "operator_status_fallback":
		return "operator fallback status selected fallback-reduction recommendation"
	case "typed_friendly_reset":
		return "typed-friendly reset selected maintenance recommendation"
	default:
		return "default recommendation applied"
	}
}

func explainDiagnosticPostureRationaleRule(primary string) string {
	switch primary {
	case "healthy":
		return "primary_healthy"
	case "index_limited":
		return "primary_index_limited"
	case "planner_limited":
		return "primary_planner_limited"
	case "operator_fallback_limited":
		return "primary_operator_fallback_limited"
	default:
		return "default"
	}
}

func explainDiagnosticPostureRationaleRuleReason(rule string) string {
	switch rule {
	case "primary_healthy":
		return "healthy posture uses fixed healthy rationale"
	case "primary_index_limited":
		return "index-limited posture uses highest-priority-code rationale"
	case "primary_planner_limited":
		return "planner-limited posture uses highest-priority-code rationale"
	case "primary_operator_fallback_limited":
		return "operator-fallback posture uses operator-status rationale"
	default:
		return "default rationale template selected"
	}
}

func explainDiagnosticPostureRationaleTemplate(primary string) string {
	templates := map[string]string{
		"healthy":                   "No explain warnings and typed-friendly operator assessment",
		"index_limited":             "Index warnings dominate diagnostics; highestPriorityCode=%s",
		"planner_limited":           "Planner warnings dominate diagnostics; highestPriorityCode=%s",
		"operator_fallback_limited": "Operator assessment indicates %s with fallback-oriented signals",
		"default":                   "Diagnostic posture derived from signals=%s and highestPriorityCode=%s",
	}
	template, ok := templates[primary]
	if !ok {
		return templates["default"]
	}
	return template
}

func explainDiagnosticPostureTrendHint(score int, byCategory map[string]int) string {
	if score >= 80 {
		return "stable"
	}
	if byCategory["planner"] > 0 || byCategory["index"] > 0 {
		return "degrading"
	}
	if score <= 30 {
		return "degrading"
	}
	return "watch"
}

func explainDiagnosticPostureTrendRule(score int, byCategory map[string]int) string {
	if score >= 80 {
		return "stable_min_score"
	}
	if byCategory["planner"] > 0 || byCategory["index"] > 0 {
		return "degrading_categories"
	}
	if score <= 30 {
		return "degrading_max_score"
	}
	return "default_trend"
}

func explainDiagnosticPostureTrendRuleReason(rule string) string {
	switch rule {
	case "stable_min_score":
		return "score met stable threshold"
	case "degrading_categories":
		return "planner or index warning categories triggered degrading trend"
	case "degrading_max_score":
		return "score met degrading threshold"
	default:
		return "default watch trend applied"
	}
}

func explainDiagnosticPostureTrendScoreRule(trendHint string) string {
	return fmt.Sprintf("trend_score_%s", trendHint)
}

func explainDiagnosticPostureTrendScore(trendHint string) int {
	switch trendHint {
	case "stable":
		return 1
	case "degrading":
		return -1
	default:
		return 0
	}
}

func explainDiagnosticPostureTrendEvidence(postureSignals []string, totalWarnings int, highestPriorityCode string, highestCategory string) map[string]any {
	return map[string]any{
		"drivers":             append([]string(nil), postureSignals...),
		"totalWarnings":       totalWarnings,
		"highestPriorityCode": highestPriorityCode,
		"highestCategory":     highestCategory,
	}
}

func explainDiagnosticPostureTrendDriverWeights(byCategory map[string]int) map[string]int {
	weights := explainDiagnosticScoreCategoryWeights()
	applied := map[string]int{}
	for category, count := range byCategory {
		if count <= 1 {
			continue
		}
		applied[category] = weights[category] * (count - 1)
	}
	return applied
}

func explainDiagnosticPostureScoreInputs(totalWarnings int, confidence string, byCategory map[string]int) map[string]any {
	repeatedCategoryCounts := map[string]int{}
	for category, count := range byCategory {
		if count <= 1 {
			continue
		}
		repeatedCategoryCounts[category] = count
	}
	return map[string]any{
		"totalWarnings":          totalWarnings,
		"confidenceClass":        confidence,
		"repeatedCategoryCounts": repeatedCategoryCounts,
	}
}

func explainDiagnosticPostureScoreComputationConfig() map[string]any {
	return map[string]any{
		"baseByPrimary": map[string]int{
			"healthy":                   90,
			"operator_fallback_limited": 60,
			"index_limited":             45,
			"planner_limited":           35,
			"default":                   50,
		},
		"confidenceAdjustment": map[string]int{
			"high":    5,
			"medium":  0,
			"low":     -10,
			"default": 0,
		},
		"categoryWeights":          explainDiagnosticScoreCategoryWeights(),
		"warningPenaltyPerWarning": 5,
		"trendRules": map[string]any{
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
		},
		"primarySelectionRules": map[string]any{
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
		},
		"confidenceRules": map[string]any{
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
		},
		"recommendationRules": map[string]string{
			"healthy":                   "maintain_typed_paths",
			"index_limited":             "improve_index_coverage",
			"planner_limited":           "optimize_scan_and_plan_shapes",
			"operator_fallback_limited": "reduce_operator_fallback_shapes",
			"default":                   "maintain_typed_paths",
		},
		"recommendationEvaluationOrder": []string{
			"primary_planner",
			"primary_index",
			"operator_status_fallback",
			"typed_friendly_reset",
			"default",
		},
		"rationaleTemplates": map[string]string{
			"healthy":                   "No explain warnings and typed-friendly operator assessment",
			"index_limited":             "Index warnings dominate diagnostics; highestPriorityCode=%s",
			"planner_limited":           "Planner warnings dominate diagnostics; highestPriorityCode=%s",
			"operator_fallback_limited": "Operator assessment indicates %s with fallback-oriented signals",
			"default":                   "Diagnostic posture derived from signals=%s and highestPriorityCode=%s",
		},
		"rationaleRules": map[string]string{
			"healthy":                   "primary_healthy",
			"index_limited":             "primary_index_limited",
			"planner_limited":           "primary_planner_limited",
			"operator_fallback_limited": "primary_operator_fallback_limited",
			"default":                   "default",
		},
		"rationaleTemplateInputs": map[string][]string{
			"healthy":                   {},
			"index_limited":             {"highestPriorityCode"},
			"planner_limited":           {"highestPriorityCode"},
			"operator_fallback_limited": {"overallStatus"},
			"default":                   {"signals", "highestPriorityCode"},
		},
		"scoreBands": map[string]map[string]int{
			"excellent": {"min": 80, "max": 100},
			"good":      {"min": 60, "max": 79},
			"fair":      {"min": 40, "max": 59},
			"poor":      {"min": 0, "max": 39},
		},
		"scoreRules": map[string]any{
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
		},
		"decisionTraceSchema": map[string]any{
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
		},
		"ruleReasonCatalogVersion": "v1",
		"ruleReasonCatalog":        explainDiagnosticPostureRuleReasonCatalog(),
	}
}

func explainDiagnosticPostureRuleReasonCatalog() map[string]map[string]string {
	return map[string]map[string]string{
		"primary": {
			"category_priority_planner": explainDiagnosticPosturePrimaryRuleReason("category_priority_planner"),
			"category_priority_index":   explainDiagnosticPosturePrimaryRuleReason("category_priority_index"),
			"operator_fallback_status":  explainDiagnosticPosturePrimaryRuleReason("operator_fallback_status"),
			"typed_friendly_reset":      explainDiagnosticPosturePrimaryRuleReason("typed_friendly_reset"),
			"default":                   explainDiagnosticPosturePrimaryRuleReason("default"),
		},
		"confidence": {
			"high_healthy_typed_friendly": explainDiagnosticPostureConfidenceRuleReason("high_healthy_typed_friendly"),
			"low_warning_or_multisignal":  explainDiagnosticPostureConfidenceRuleReason("low_warning_or_multisignal"),
			"default_medium":              explainDiagnosticPostureConfidenceRuleReason("default_medium"),
		},
		"recommendation": {
			"primary_planner":          explainDiagnosticPostureRecommendationRuleReason("primary_planner"),
			"primary_index":            explainDiagnosticPostureRecommendationRuleReason("primary_index"),
			"operator_status_fallback": explainDiagnosticPostureRecommendationRuleReason("operator_status_fallback"),
			"typed_friendly_reset":     explainDiagnosticPostureRecommendationRuleReason("typed_friendly_reset"),
			"default":                  explainDiagnosticPostureRecommendationRuleReason("default"),
		},
		"rationale": {
			"primary_healthy":                   explainDiagnosticPostureRationaleRuleReason("primary_healthy"),
			"primary_index_limited":             explainDiagnosticPostureRationaleRuleReason("primary_index_limited"),
			"primary_planner_limited":           explainDiagnosticPostureRationaleRuleReason("primary_planner_limited"),
			"primary_operator_fallback_limited": explainDiagnosticPostureRationaleRuleReason("primary_operator_fallback_limited"),
			"default":                           explainDiagnosticPostureRationaleRuleReason("default"),
		},
		"score": {
			"score_band_excellent": explainDiagnosticPostureScoreBandRuleReason("score_band_excellent"),
			"score_band_good":      explainDiagnosticPostureScoreBandRuleReason("score_band_good"),
			"score_band_fair":      explainDiagnosticPostureScoreBandRuleReason("score_band_fair"),
			"score_band_poor":      explainDiagnosticPostureScoreBandRuleReason("score_band_poor"),
		},
		"trend": {
			"stable_min_score":     explainDiagnosticPostureTrendRuleReason("stable_min_score"),
			"degrading_categories": explainDiagnosticPostureTrendRuleReason("degrading_categories"),
			"degrading_max_score":  explainDiagnosticPostureTrendRuleReason("degrading_max_score"),
			"default_trend":        explainDiagnosticPostureTrendRuleReason("default_trend"),
		},
	}
}

func explainDiagnosticPostureMatchedReasonLookup(decisionTrace []map[string]any, ruleReasonCatalog map[string]map[string]string) map[string]map[string]any {
	lookup := map[string]map[string]any{}
	for _, decision := range decisionTrace {
		stage, _ := decision["stage"].(string)
		rule, _ := decision["rule"].(string)
		actualReason, _ := decision["reason"].(string)
		expectedReason := ""
		if stageReasons, ok := ruleReasonCatalog[stage]; ok {
			expectedReason = stageReasons[rule]
		}
		lookup[stage] = map[string]any{
			"rule":           rule,
			"expectedReason": expectedReason,
			"actualReason":   actualReason,
			"matchesCatalog": expectedReason == actualReason,
		}
	}
	return lookup
}

func explainDiagnosticPostureReasonCatalogMismatches(matchedReasonLookup map[string]map[string]any) []string {
	stages := []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"}
	mismatches := make([]string, 0, len(stages))
	for _, stage := range stages {
		entry, ok := matchedReasonLookup[stage]
		if !ok {
			mismatches = append(mismatches, stage)
			continue
		}
		matches, _ := entry["matchesCatalog"].(bool)
		if !matches {
			mismatches = append(mismatches, stage)
		}
	}
	return mismatches
}

func explainDiagnosticPostureContractHash(scoreComputationConfig map[string]any) string {
	components := explainDiagnosticPostureContractComponents(scoreComputationConfig)
	overallHash, _ := components["overallHash"].(string)
	if overallHash != "" {
		return overallHash
	}
	return ""
}

func explainDiagnosticPostureContractComponents(scoreComputationConfig map[string]any) map[string]any {
	schema, _ := scoreComputationConfig["decisionTraceSchema"].(map[string]any)
	catalog, _ := scoreComputationConfig["ruleReasonCatalog"].(map[string]map[string]string)
	b := strings.Builder{}
	if version, _ := schema["version"].(string); version != "" {
		b.WriteString("schema.version=")
		b.WriteString(version)
		b.WriteString(";")
	}
	if stageOrder, _ := schema["stageOrder"].([]string); len(stageOrder) > 0 {
		b.WriteString("schema.stageOrder=")
		b.WriteString(strings.Join(stageOrder, ","))
		b.WriteString(";")
	}
	if stageFields, _ := schema["stageFields"].(map[string][]string); len(stageFields) > 0 {
		stageKeys := make([]string, 0, len(stageFields))
		for stage := range stageFields {
			stageKeys = append(stageKeys, stage)
		}
		sort.Strings(stageKeys)
		for _, stage := range stageKeys {
			b.WriteString("schema.stageFields.")
			b.WriteString(stage)
			b.WriteString("=")
			b.WriteString(strings.Join(stageFields[stage], ","))
			b.WriteString(";")
		}
	}
	decisionTraceSchemaHash := explainDiagnosticPostureHashString(b.String())

	b.Reset()
	if len(catalog) > 0 {
		catalogKeys := make([]string, 0, len(catalog))
		for family := range catalog {
			catalogKeys = append(catalogKeys, family)
		}
		sort.Strings(catalogKeys)
		for _, family := range catalogKeys {
			reasonMap := catalog[family]
			ruleKeys := make([]string, 0, len(reasonMap))
			for rule := range reasonMap {
				ruleKeys = append(ruleKeys, rule)
			}
			sort.Strings(ruleKeys)
			for _, rule := range ruleKeys {
				b.WriteString("catalog.")
				b.WriteString(family)
				b.WriteString(".")
				b.WriteString(rule)
				b.WriteString("=")
				b.WriteString(reasonMap[rule])
				b.WriteString(";")
			}
		}
	}
	ruleReasonCatalogHash := explainDiagnosticPostureHashString(b.String())

	stageOrder := []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"}
	stageFields, _ := schema["stageFields"].(map[string][]string)
	stageContractFragments := map[string]string{}
	for _, stage := range stageOrder {
		fragmentBuilder := strings.Builder{}
		if version, _ := schema["version"].(string); version != "" {
			fragmentBuilder.WriteString("schema.version=")
			fragmentBuilder.WriteString(version)
			fragmentBuilder.WriteString(";")
		}
		fragmentBuilder.WriteString("stage=")
		fragmentBuilder.WriteString(stage)
		fragmentBuilder.WriteString(";")
		if fields := stageFields[stage]; len(fields) > 0 {
			fragmentBuilder.WriteString("schema.stageFields.")
			fragmentBuilder.WriteString(stage)
			fragmentBuilder.WriteString("=")
			fragmentBuilder.WriteString(strings.Join(fields, ","))
			fragmentBuilder.WriteString(";")
		}
		if reasonMap := catalog[stage]; len(reasonMap) > 0 {
			ruleKeys := make([]string, 0, len(reasonMap))
			for rule := range reasonMap {
				ruleKeys = append(ruleKeys, rule)
			}
			sort.Strings(ruleKeys)
			for _, rule := range ruleKeys {
				fragmentBuilder.WriteString("catalog.")
				fragmentBuilder.WriteString(stage)
				fragmentBuilder.WriteString(".")
				fragmentBuilder.WriteString(rule)
				fragmentBuilder.WriteString("=")
				fragmentBuilder.WriteString(reasonMap[rule])
				fragmentBuilder.WriteString(";")
			}
		}
		stageContractFragments[stage] = explainDiagnosticPostureHashString(fragmentBuilder.String())
	}

	b.Reset()
	if schemaStageOrder, _ := schema["stageOrder"].([]string); len(schemaStageOrder) > 0 {
		b.WriteString("schema.stageOrder=")
		b.WriteString(strings.Join(schemaStageOrder, ","))
		b.WriteString(";")
	}
	stageOrderHash := explainDiagnosticPostureHashString(b.String())

	overallBuilder := strings.Builder{}
	overallBuilder.WriteString("decisionTraceSchemaHash=")
	overallBuilder.WriteString(decisionTraceSchemaHash)
	overallBuilder.WriteString(";")
	overallBuilder.WriteString("ruleReasonCatalogHash=")
	overallBuilder.WriteString(ruleReasonCatalogHash)
	overallBuilder.WriteString(";")
	overallBuilder.WriteString("stageOrderHash=")
	overallBuilder.WriteString(stageOrderHash)
	overallBuilder.WriteString(";")
	for _, stage := range stageOrder {
		overallBuilder.WriteString("stageContractFragments.")
		overallBuilder.WriteString(stage)
		overallBuilder.WriteString("=")
		overallBuilder.WriteString(stageContractFragments[stage])
		overallBuilder.WriteString(";")
	}

	return map[string]any{
		"version":                 "v1",
		"decisionTraceSchemaHash": decisionTraceSchemaHash,
		"ruleReasonCatalogHash":   ruleReasonCatalogHash,
		"stageOrderHash":          stageOrderHash,
		"stageContractFragments":  stageContractFragments,
		"overallHash":             explainDiagnosticPostureHashString(overallBuilder.String()),
	}
}

func explainDiagnosticPostureContractCompatibility(contractComponents map[string]any) map[string]any {
	requiredStages := []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"}
	checkEvaluationOrder := []string{"version", "schema", "rule_reason_catalog", "stage_order", "stage_fragments", "overall_hash"}
	checkReasonCodes := map[string]string{
		"version":             "version_mismatch",
		"schema":              "missing_decision_trace_schema_hash",
		"rule_reason_catalog": "missing_rule_reason_catalog_hash",
		"stage_order":         "missing_stage_order_hash",
		"stage_fragments":     "missing_stage_fragment",
		"overall_hash":        "missing_overall_hash",
	}
	impactEvaluationOrder := []string{"compatible_if_no_failed_checks", "breaking_on_core_contract_checks", "breaking_on_stage_fragment_gap", "unknown_default"}
	impactByCheck := map[string]string{
		"version":             "breaking",
		"schema":              "breaking",
		"rule_reason_catalog": "breaking",
		"stage_order":         "breaking",
		"stage_fragments":     "breaking",
		"overall_hash":        "breaking",
	}
	remediationByCheck := map[string]string{
		"version":             "align compatibilityVersion and contractComponents.version with the expected parser/runtime contract version",
		"schema":              "regenerate decisionTraceSchema metadata and ensure decisionTraceSchemaHash is present",
		"rule_reason_catalog": "regenerate ruleReasonCatalog metadata and ensure ruleReasonCatalogHash is present",
		"stage_order":         "restore decision-trace stageOrder metadata and ensure stageOrderHash is present",
		"stage_fragments":     "recompute stageContractFragments for all required stages and verify fragment hashes are non-empty",
		"overall_hash":        "rebuild contractComponents overallHash after schema/catalog/stage fragment metadata is refreshed",
	}
	remediationSeverityByCheck := map[string]string{
		"version":             "high",
		"schema":              "high",
		"rule_reason_catalog": "high",
		"stage_order":         "high",
		"stage_fragments":     "medium",
		"overall_hash":        "high",
	}
	remediationRequirementByCheck := map[string]string{
		"version":             "required",
		"schema":              "required",
		"rule_reason_catalog": "required",
		"stage_order":         "required",
		"stage_fragments":     "required",
		"overall_hash":        "required",
	}

	schemaHash, _ := contractComponents["decisionTraceSchemaHash"].(string)
	ruleReasonCatalogHash, _ := contractComponents["ruleReasonCatalogHash"].(string)
	stageOrderHash, _ := contractComponents["stageOrderHash"].(string)
	overallHash, _ := contractComponents["overallHash"].(string)
	version, _ := contractComponents["version"].(string)

	stageFragments := map[string]string{}
	switch fragments := contractComponents["stageContractFragments"].(type) {
	case map[string]string:
		for stage, hash := range fragments {
			stageFragments[stage] = hash
		}
	case map[string]any:
		for stage, hashAny := range fragments {
			hash, _ := hashAny.(string)
			stageFragments[stage] = hash
		}
	}

	stageCompatibility := map[string]bool{}
	stageReasonCodes := map[string]string{}
	incompatibilityReasons := make([]string, 0, len(requiredStages)+5)
	failedChecks := make([]string, 0, len(checkEvaluationOrder))
	failedCheckReasons := map[string]string{}
	stagesCompatible := true
	missingStageFragments := make([]string, 0, len(requiredStages))
	for _, stage := range requiredStages {
		ok := strings.TrimSpace(stageFragments[stage]) != ""
		stageCompatibility[stage] = ok
		stageReasonCodes[stage] = "missing_stage_fragment:" + stage
		if !ok {
			stagesCompatible = false
			missingStageFragments = append(missingStageFragments, stage)
			incompatibilityReasons = append(incompatibilityReasons, "missing_stage_fragment:"+stage)
		}
	}

	schemaCompatible := strings.TrimSpace(schemaHash) != ""
	ruleReasonCatalogCompatible := strings.TrimSpace(ruleReasonCatalogHash) != ""
	stageOrderCompatible := strings.TrimSpace(stageOrderHash) != ""
	overallHashPresent := strings.TrimSpace(overallHash) != ""
	versionCompatible := version == "v1"
	if !versionCompatible {
		failedChecks = append(failedChecks, "version")
		failedCheckReasons["version"] = checkReasonCodes["version"]
		incompatibilityReasons = append(incompatibilityReasons, "version_mismatch")
	}
	if !schemaCompatible {
		failedChecks = append(failedChecks, "schema")
		failedCheckReasons["schema"] = checkReasonCodes["schema"]
		incompatibilityReasons = append(incompatibilityReasons, "missing_decision_trace_schema_hash")
	}
	if !ruleReasonCatalogCompatible {
		failedChecks = append(failedChecks, "rule_reason_catalog")
		failedCheckReasons["rule_reason_catalog"] = checkReasonCodes["rule_reason_catalog"]
		incompatibilityReasons = append(incompatibilityReasons, "missing_rule_reason_catalog_hash")
	}
	if !stageOrderCompatible {
		failedChecks = append(failedChecks, "stage_order")
		failedCheckReasons["stage_order"] = checkReasonCodes["stage_order"]
		incompatibilityReasons = append(incompatibilityReasons, "missing_stage_order_hash")
	}
	if !stagesCompatible {
		failedChecks = append(failedChecks, "stage_fragments")
		failedCheckReasons["stage_fragments"] = checkReasonCodes["stage_fragments"] + ":" + strings.Join(missingStageFragments, ",")
	}
	if !overallHashPresent {
		failedChecks = append(failedChecks, "overall_hash")
		failedCheckReasons["overall_hash"] = checkReasonCodes["overall_hash"]
		incompatibilityReasons = append(incompatibilityReasons, "missing_overall_hash")
	}

	checkFingerprints := explainDiagnosticPostureContractCheckFingerprints(contractComponents)
	compatibilityFingerprint := explainDiagnosticPostureContractCompatibilityFingerprint(checkFingerprints, checkEvaluationOrder)

	fullyCompatible := versionCompatible && schemaCompatible && ruleReasonCatalogCompatible &&
		stageOrderCompatible && stagesCompatible && overallHashPresent
	compatibilitySummary := "contract_components_complete"
	if !fullyCompatible {
		compatibilitySummary = "contract_components_incomplete"
	}

	impactClassification := "unknown"
	impactRule := "unknown_default"
	impactReason := "no impact rule matched"
	if len(failedChecks) == 0 {
		impactClassification = "compatible"
		impactRule = "compatible_if_no_failed_checks"
		impactReason = "all compatibility checks passed"
	} else {
		for _, check := range failedChecks {
			if check == "version" || check == "schema" || check == "rule_reason_catalog" || check == "stage_order" || check == "overall_hash" {
				impactClassification = "breaking"
				impactRule = "breaking_on_core_contract_checks"
				impactReason = "core compatibility checks failed"
				break
			}
		}
		if impactClassification == "unknown" {
			for _, check := range failedChecks {
				if check == "stage_fragments" {
					impactClassification = "breaking"
					impactRule = "breaking_on_stage_fragment_gap"
					impactReason = "stage fragment compatibility checks failed"
					break
				}
			}
		}
	}

	failedCheckSet := map[string]struct{}{}
	for _, check := range failedChecks {
		failedCheckSet[check] = struct{}{}
	}
	remediationActions := make([]string, 0, len(failedChecks))
	remediationActionPlan := make([]map[string]any, 0, len(failedChecks))
	for _, check := range checkEvaluationOrder {
		if _, ok := failedCheckSet[check]; !ok {
			continue
		}
		hint, ok := remediationByCheck[check]
		if ok {
			remediationActions = append(remediationActions, hint)
		}
		remediationActionPlan = append(remediationActionPlan, map[string]any{
			"check":       check,
			"reasonCode":  checkReasonCodes[check],
			"severity":    remediationSeverityByCheck[check],
			"requirement": remediationRequirementByCheck[check],
			"hint":        hint,
		})
	}
	remediationSummary := "none_required"
	if len(remediationActions) > 0 {
		remediationSummary = "action_required"
	}
	remediationUrgency := "none"
	for _, action := range remediationActionPlan {
		if severity, _ := action["severity"].(string); severity == "high" {
			remediationUrgency = "high"
			break
		}
		if severity, _ := action["severity"].(string); severity == "medium" && remediationUrgency == "none" {
			remediationUrgency = "medium"
		}
	}
	remediationBundleID := explainDiagnosticPostureRemediationBundleID(impactClassification, remediationSummary, remediationUrgency)
	remediationPlanHash := explainDiagnosticPostureRemediationPlanHash(remediationActionPlan, remediationSummary, remediationUrgency)
	baselineRemediationBundleID := explainDiagnosticPostureRemediationBundleID("compatible", "none_required", "none")
	baselineRemediationPlanHash := explainDiagnosticPostureRemediationPlanHash([]map[string]any{}, "none_required", "none")
	remediationBundleDriftStatus := "unchanged"
	if remediationBundleID != baselineRemediationBundleID {
		remediationBundleDriftStatus = "changed"
	}
	remediationPlanDriftStatus := "unchanged"
	if remediationPlanHash != baselineRemediationPlanHash {
		remediationPlanDriftStatus = "changed"
	}
	remediationDriftFields := make([]string, 0, 2)
	if remediationBundleDriftStatus == "changed" {
		remediationDriftFields = append(remediationDriftFields, "bundle_id")
	}
	if remediationPlanDriftStatus == "changed" {
		remediationDriftFields = append(remediationDriftFields, "plan_hash")
	}
	remediationDriftSummary := "baseline_aligned"
	remediationDriftRule := "baseline_bundle_and_plan_match"
	remediationDriftReason := "current remediation bundle and plan hash match baseline compatible state"
	if len(remediationDriftFields) > 0 {
		remediationDriftSummary = "baseline_drift_detected"
		remediationDriftRule = "baseline_bundle_or_plan_changed"
		remediationDriftReason = "current remediation bundle or plan hash differs from baseline compatible state"
	}
	remediationDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineBundle=" + baselineRemediationBundleID + ";" +
			"currentBundle=" + remediationBundleID + ";" +
			"baselinePlan=" + baselineRemediationPlanHash + ";" +
			"currentPlan=" + remediationPlanHash + ";" +
			"bundleDrift=" + remediationBundleDriftStatus + ";" +
			"planDrift=" + remediationPlanDriftStatus + ";",
	)
	contractEpoch := "contract.v1"
	baselineContractEpoch := "contract.v1"
	contractEpochTransition := "unchanged"
	if contractEpoch != baselineContractEpoch {
		contractEpochTransition = "advanced"
	}
	remediationEpoch := "remediation.v1"
	baselineRemediationEpoch := "remediation.v1"
	remediationEpochTransition := "unchanged"
	if remediationEpoch != baselineRemediationEpoch {
		remediationEpochTransition = "advanced"
	}
	epochTransitionRule := "epoch_transition_none"
	epochTransitionReason := "contract and remediation epochs match baseline"
	if contractEpochTransition != "unchanged" || remediationEpochTransition != "unchanged" {
		epochTransitionRule = "epoch_transition_detected"
		epochTransitionReason = "contract or remediation epoch advanced from baseline"
	}
	epochEvaluationOrder := []string{"contract_epoch", "remediation_epoch"}
	epochReasonCodes := map[string]string{
		"contract_epoch":    "contract_epoch_advanced_from_baseline",
		"remediation_epoch": "remediation_epoch_advanced_from_baseline",
	}
	epochCompatibility := map[string]bool{
		"contract_epoch":    contractEpochTransition == "unchanged",
		"remediation_epoch": remediationEpochTransition == "unchanged",
	}
	epochFailedTransitions := make([]string, 0, len(epochEvaluationOrder))
	epochFailedTransitionReasons := map[string]string{}
	for _, check := range epochEvaluationOrder {
		if ok, exists := epochCompatibility[check]; exists && !ok {
			epochFailedTransitions = append(epochFailedTransitions, check)
			switch check {
			case "contract_epoch":
				epochFailedTransitionReasons[check] = "contract epoch differs from baseline"
			case "remediation_epoch":
				epochFailedTransitionReasons[check] = "remediation epoch differs from baseline"
			default:
				epochFailedTransitionReasons[check] = "epoch transition differs from baseline"
			}
		}
	}
	epochTransitionSummary := "baseline_aligned"
	if len(epochFailedTransitions) > 0 {
		epochTransitionSummary = "baseline_transition_detected"
	}
	epochImpactEvaluationOrder := []string{"epoch_compatible_if_no_failed_transitions", "epoch_breaking_on_contract_transition", "epoch_breaking_on_remediation_transition", "epoch_unknown_default"}
	epochImpactByCheck := map[string]string{
		"contract_epoch":    "breaking",
		"remediation_epoch": "breaking",
	}
	epochImpactClassification := "compatible"
	epochImpactRule := "epoch_compatible_if_no_failed_transitions"
	epochImpactReason := "no epoch transition drift detected"
	for _, check := range epochFailedTransitions {
		if check == "contract_epoch" {
			epochImpactClassification = "breaking"
			epochImpactRule = "epoch_breaking_on_contract_transition"
			epochImpactReason = "contract epoch advanced relative to baseline"
			break
		}
		if check == "remediation_epoch" {
			epochImpactClassification = "breaking"
			epochImpactRule = "epoch_breaking_on_remediation_transition"
			epochImpactReason = "remediation epoch advanced relative to baseline"
		}
	}
	epochRemediationByCheck := map[string]string{
		"contract_epoch":    "upgrade parser/runtime consumers to a contract.v1-compatible epoch before parsing evaluatedPolicy compatibility metadata",
		"remediation_epoch": "refresh remediation consumers to the remediation.v1 schema before applying compatibility remediation guidance",
	}
	epochRemediationPriorityOrder := []string{"contract_epoch", "remediation_epoch"}
	epochRemediationActions := make([]string, 0, len(epochFailedTransitions))
	epochRemediationActionPlan := make([]map[string]any, 0, len(epochFailedTransitions))
	for _, check := range epochRemediationPriorityOrder {
		if ok, exists := epochCompatibility[check]; !exists || ok {
			continue
		}
		hint := epochRemediationByCheck[check]
		epochRemediationActions = append(epochRemediationActions, hint)
		epochRemediationActionPlan = append(epochRemediationActionPlan, map[string]any{
			"check":      check,
			"reasonCode": epochReasonCodes[check],
			"impact":     epochImpactByCheck[check],
			"hint":       hint,
		})
	}
	epochRemediationSummary := "none_required"
	if len(epochRemediationActions) > 0 {
		epochRemediationSummary = "action_required"
	}
	epochRemediationPlanHash := explainDiagnosticPostureHashString(
		"summary=" + epochRemediationSummary + ";" +
			"actions=" + strings.Join(epochRemediationActions, "||") + ";",
	)
	epochStateID := explainDiagnosticPostureHashString(
		"contractEpoch=" + contractEpoch + ";" +
			"remediationEpoch=" + remediationEpoch + ";",
	)
	baselineEpochStateID := explainDiagnosticPostureHashString(
		"contractEpoch=" + baselineContractEpoch + ";" +
			"remediationEpoch=" + baselineRemediationEpoch + ";",
	)
	epochDriftStatus := "unchanged"
	if epochStateID != baselineEpochStateID {
		epochDriftStatus = "changed"
	}
	epochDriftFields := make([]string, 0, len(epochFailedTransitions))
	if contractEpochTransition != "unchanged" {
		epochDriftFields = append(epochDriftFields, "contract_epoch")
	}
	if remediationEpochTransition != "unchanged" {
		epochDriftFields = append(epochDriftFields, "remediation_epoch")
	}
	epochCompatibilityFingerprint := explainDiagnosticPostureHashString(
		"epochStateID=" + epochStateID + ";" +
			"baselineEpochStateID=" + baselineEpochStateID + ";" +
			"driftStatus=" + epochDriftStatus + ";" +
			"driftFields=" + strings.Join(epochDriftFields, ",") + ";" +
			"impact=" + epochImpactClassification + ";" +
			"remediationPlanHash=" + epochRemediationPlanHash + ";",
	)
	epochConsistencyEvaluationOrder := []string{"transition_summary_matches_failed_transitions", "drift_status_matches_state_ids", "impact_matches_failed_transitions", "remediation_summary_matches_actions"}
	epochConsistencyReasonCodes := map[string]string{
		"transition_summary_matches_failed_transitions": "epoch_transition_summary_mismatch",
		"drift_status_matches_state_ids":                "epoch_drift_status_mismatch",
		"impact_matches_failed_transitions":             "epoch_impact_mismatch",
		"remediation_summary_matches_actions":           "epoch_remediation_summary_mismatch",
	}
	epochConsistencyChecks := map[string]bool{
		"transition_summary_matches_failed_transitions": (len(epochFailedTransitions) == 0 && epochTransitionSummary == "baseline_aligned") || (len(epochFailedTransitions) > 0 && epochTransitionSummary == "baseline_transition_detected"),
		"drift_status_matches_state_ids":                (epochStateID == baselineEpochStateID && epochDriftStatus == "unchanged") || (epochStateID != baselineEpochStateID && epochDriftStatus == "changed"),
		"impact_matches_failed_transitions":             (len(epochFailedTransitions) == 0 && epochImpactClassification == "compatible") || (len(epochFailedTransitions) > 0 && epochImpactClassification == "breaking"),
		"remediation_summary_matches_actions":           (len(epochRemediationActions) == 0 && epochRemediationSummary == "none_required") || (len(epochRemediationActions) > 0 && epochRemediationSummary == "action_required"),
	}
	epochConsistencyFailedChecks := make([]string, 0, len(epochConsistencyEvaluationOrder))
	epochConsistencyFailedReasons := map[string]string{}
	for _, check := range epochConsistencyEvaluationOrder {
		if ok, exists := epochConsistencyChecks[check]; exists && !ok {
			epochConsistencyFailedChecks = append(epochConsistencyFailedChecks, check)
			switch check {
			case "transition_summary_matches_failed_transitions":
				epochConsistencyFailedReasons[check] = "epoch transition summary does not match failed transition set"
			case "drift_status_matches_state_ids":
				epochConsistencyFailedReasons[check] = "epoch drift status does not match state-id comparison"
			case "impact_matches_failed_transitions":
				epochConsistencyFailedReasons[check] = "epoch impact classification does not match failed transition set"
			case "remediation_summary_matches_actions":
				epochConsistencyFailedReasons[check] = "epoch remediation summary does not match remediation action list"
			default:
				epochConsistencyFailedReasons[check] = "epoch consistency check failed"
			}
		}
	}
	epochConsistencySummary := "consistent"
	if len(epochConsistencyFailedChecks) > 0 {
		epochConsistencySummary = "inconsistent"
	}
	epochConsistencyFingerprint := explainDiagnosticPostureHashString(
		"summary=" + epochConsistencySummary + ";" +
			"failedChecks=" + strings.Join(epochConsistencyFailedChecks, ",") + ";" +
			"driftStatus=" + epochDriftStatus + ";" +
			"impact=" + epochImpactClassification + ";" +
			"remediationSummary=" + epochRemediationSummary + ";",
	)
	epochRuleCatalogVersion := "v1"
	epochRuleCatalog := map[string]map[string]string{
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
	}
	epochMatchedRuleLookup := map[string]map[string]any{
		"transition": {
			"rule":           epochTransitionRule,
			"expectedReason": epochRuleCatalog["transition"][epochTransitionRule],
			"actualReason":   epochTransitionReason,
			"matchesCatalog": epochRuleCatalog["transition"][epochTransitionRule] == epochTransitionReason,
		},
		"impact": {
			"rule":           epochImpactRule,
			"expectedReason": epochRuleCatalog["impact"][epochImpactRule],
			"actualReason":   epochImpactReason,
			"matchesCatalog": epochRuleCatalog["impact"][epochImpactRule] == epochImpactReason,
		},
		"consistency": {
			"rule":           epochConsistencySummary,
			"expectedReason": epochRuleCatalog["consistency"][epochConsistencySummary],
			"actualReason":   "epoch consistency checks passed",
			"matchesCatalog": epochRuleCatalog["consistency"][epochConsistencySummary] == "epoch consistency checks passed",
		},
	}
	epochRuleCatalogMismatches := []string{}
	epochRuleCatalogConsistent := true
	for domain, entry := range epochMatchedRuleLookup {
		if matchesCatalog, _ := entry["matchesCatalog"].(bool); !matchesCatalog {
			epochRuleCatalogMismatches = append(epochRuleCatalogMismatches, domain)
			epochRuleCatalogConsistent = false
		}
	}
	epochContractVersion := "v1"
	epochRuleCatalogHash := explainDiagnosticPostureHashString(
		"transition.epoch_transition_none=contract and remediation epochs match baseline;" +
			"transition.epoch_transition_detected=contract or remediation epoch advanced from baseline;" +
			"impact.epoch_compatible_if_no_failed_transitions=no epoch transition drift detected;" +
			"impact.epoch_breaking_on_contract_transition=contract epoch advanced relative to baseline;" +
			"impact.epoch_breaking_on_remediation_transition=remediation epoch advanced relative to baseline;" +
			"consistency.consistent=epoch consistency checks passed;" +
			"consistency.inconsistent=epoch consistency checks failed;",
	)
	epochEvaluationOrderHash := explainDiagnosticPostureHashString(
		"epochEvaluationOrder=" + strings.Join(epochEvaluationOrder, ",") + ";" +
			"epochImpactEvaluationOrder=" + strings.Join(epochImpactEvaluationOrder, ",") + ";" +
			"epochConsistencyEvaluationOrder=" + strings.Join(epochConsistencyEvaluationOrder, ",") + ";",
	)
	epochConsistencyCheckSchemaHash := explainDiagnosticPostureHashString(
		"transition_summary_matches_failed_transitions:bool;" +
			"drift_status_matches_state_ids:bool;" +
			"impact_matches_failed_transitions:bool;" +
			"remediation_summary_matches_actions:bool;",
	)
	epochContractOverallHash := explainDiagnosticPostureHashString(
		"version=" + epochContractVersion + ";" +
			"ruleCatalogHash=" + epochRuleCatalogHash + ";" +
			"evaluationOrderHash=" + epochEvaluationOrderHash + ";" +
			"consistencyCheckSchemaHash=" + epochConsistencyCheckSchemaHash + ";",
	)
	epochContractComponents := map[string]any{
		"version":                    epochContractVersion,
		"ruleCatalogHash":            epochRuleCatalogHash,
		"evaluationOrderHash":        epochEvaluationOrderHash,
		"consistencyCheckSchemaHash": epochConsistencyCheckSchemaHash,
		"overallHash":                epochContractOverallHash,
	}
	epochContractCompatibility := map[string]bool{
		"ruleCatalogPresent":       epochRuleCatalogHash != "",
		"evaluationOrderPresent":   epochEvaluationOrderHash != "",
		"consistencySchemaPresent": epochConsistencyCheckSchemaHash != "",
		"overallHashPresent":       epochContractOverallHash != "",
	}
	epochContractCheckEvaluationOrder := []string{"rule_catalog_present", "evaluation_order_present", "consistency_schema_present", "overall_hash_present"}
	epochContractCheckReasonCodes := map[string]string{
		"rule_catalog_present":       "missing_epoch_rule_catalog_hash",
		"evaluation_order_present":   "missing_epoch_evaluation_order_hash",
		"consistency_schema_present": "missing_epoch_consistency_schema_hash",
		"overall_hash_present":       "missing_epoch_contract_overall_hash",
	}
	epochContractCheckStatus := map[string]bool{
		"rule_catalog_present":       epochContractCompatibility["ruleCatalogPresent"],
		"evaluation_order_present":   epochContractCompatibility["evaluationOrderPresent"],
		"consistency_schema_present": epochContractCompatibility["consistencySchemaPresent"],
		"overall_hash_present":       epochContractCompatibility["overallHashPresent"],
	}
	epochContractFailedChecks := make([]string, 0, len(epochContractCheckEvaluationOrder))
	epochContractFailedCheckReasons := map[string]string{}
	for _, check := range epochContractCheckEvaluationOrder {
		if ok, exists := epochContractCheckStatus[check]; exists && !ok {
			epochContractFailedChecks = append(epochContractFailedChecks, check)
			switch check {
			case "rule_catalog_present":
				epochContractFailedCheckReasons[check] = "epoch rule catalog hash is missing"
			case "evaluation_order_present":
				epochContractFailedCheckReasons[check] = "epoch evaluation-order hash is missing"
			case "consistency_schema_present":
				epochContractFailedCheckReasons[check] = "epoch consistency-check schema hash is missing"
			case "overall_hash_present":
				epochContractFailedCheckReasons[check] = "epoch contract overall hash is missing"
			default:
				epochContractFailedCheckReasons[check] = "epoch contract check failed"
			}
		}
	}
	epochContractCheckSummary := "all_checks_passed"
	if len(epochContractFailedChecks) > 0 {
		epochContractCheckSummary = "missing_required_components"
	}
	epochContractFullyCompatible := len(epochContractFailedChecks) == 0
	epochContractImpactEvaluationOrder := []string{"compatible_if_all_contract_checks_pass", "breaking_if_contract_components_missing", "unknown_default"}
	epochContractImpactByCheck := map[string]string{
		"rule_catalog_present":       "breaking",
		"evaluation_order_present":   "breaking",
		"consistency_schema_present": "breaking",
		"overall_hash_present":       "breaking",
	}
	epochContractImpactClassification := "compatible"
	epochContractImpactRule := "compatible_if_all_contract_checks_pass"
	epochContractImpactReason := "all epoch-contract checks passed"
	if len(epochContractFailedChecks) > 0 {
		epochContractImpactClassification = "breaking"
		epochContractImpactRule = "breaking_if_contract_components_missing"
		epochContractImpactReason = "one or more required epoch-contract components are missing"
	}
	epochContractRemediationByCheck := map[string]string{
		"rule_catalog_present":       "regenerate epoch rule catalog metadata and persist a non-empty epoch rule catalog hash",
		"evaluation_order_present":   "restore epoch evaluation-order metadata and persist a non-empty evaluation-order hash",
		"consistency_schema_present": "regenerate epoch consistency-check schema metadata and persist a non-empty schema hash",
		"overall_hash_present":       "recompute epoch contract overall hash after restoring contract component hashes",
	}
	epochContractRemediationPriorityOrder := append([]string(nil), epochContractCheckEvaluationOrder...)
	epochContractRemediationActions := make([]string, 0, len(epochContractFailedChecks))
	epochContractFailedCheckSet := map[string]struct{}{}
	for _, check := range epochContractFailedChecks {
		epochContractFailedCheckSet[check] = struct{}{}
	}
	for _, check := range epochContractRemediationPriorityOrder {
		if _, ok := epochContractFailedCheckSet[check]; ok {
			epochContractRemediationActions = append(epochContractRemediationActions, epochContractRemediationByCheck[check])
		}
	}
	epochContractRemediationSummary := "none_required"
	if len(epochContractRemediationActions) > 0 {
		epochContractRemediationSummary = "action_required"
	}
	epochContractRemediationSeverityByCheck := map[string]string{
		"rule_catalog_present":       "high",
		"evaluation_order_present":   "high",
		"consistency_schema_present": "high",
		"overall_hash_present":       "high",
	}
	epochContractRemediationRequirementByCheck := map[string]string{
		"rule_catalog_present":       "required",
		"evaluation_order_present":   "required",
		"consistency_schema_present": "required",
		"overall_hash_present":       "required",
	}
	epochContractRemediationUrgency := "none"
	if epochContractRemediationSummary == "action_required" {
		epochContractRemediationUrgency = "high"
	}
	epochContractRemediationBundleID := explainDiagnosticPostureRemediationBundleID(
		epochContractImpactClassification,
		epochContractRemediationSummary,
		epochContractRemediationUrgency,
	)
	epochContractRemediationPlanHash := explainDiagnosticPostureHashString(
		"summary=" + epochContractRemediationSummary + ";" +
			"actions=" + strings.Join(epochContractRemediationActions, "||") + ";",
	)
	epochContractRemediationFingerprint := explainDiagnosticPostureHashString(
		"impact=" + epochContractImpactClassification + ";" +
			"summary=" + epochContractRemediationSummary + ";" +
			"planHash=" + epochContractRemediationPlanHash + ";",
	)
	baselineEpochContractCheckSummary := "all_checks_passed"
	baselineEpochContractImpactClassification := "compatible"
	baselineEpochContractRemediationSummary := "none_required"
	baselineEpochContractRemediationUrgency := "none"
	baselineEpochContractRemediationBundleID := explainDiagnosticPostureRemediationBundleID(
		baselineEpochContractImpactClassification,
		baselineEpochContractRemediationSummary,
		baselineEpochContractRemediationUrgency,
	)
	baselineEpochContractRemediationPlanHash := explainDiagnosticPostureHashString("summary=none_required;actions=;")
	epochContractRemediationBundleDriftStatus := "unchanged"
	if epochContractRemediationBundleID != baselineEpochContractRemediationBundleID {
		epochContractRemediationBundleDriftStatus = "changed"
	}
	epochContractRemediationPlanDriftStatus := "unchanged"
	if epochContractRemediationPlanHash != baselineEpochContractRemediationPlanHash {
		epochContractRemediationPlanDriftStatus = "changed"
	}
	epochContractRemediationDriftFields := []string{}
	if epochContractRemediationBundleDriftStatus == "changed" {
		epochContractRemediationDriftFields = append(epochContractRemediationDriftFields, "bundle_id")
	}
	if epochContractRemediationPlanDriftStatus == "changed" {
		epochContractRemediationDriftFields = append(epochContractRemediationDriftFields, "plan_hash")
	}
	epochContractRemediationDriftSummary := "baseline_aligned"
	epochContractRemediationDriftRule := "baseline_epoch_contract_bundle_and_plan_match"
	epochContractRemediationDriftReason := "current epoch-contract remediation bundle and plan hash match baseline compatible state"
	if len(epochContractRemediationDriftFields) > 0 {
		epochContractRemediationDriftSummary = "baseline_drifted"
		epochContractRemediationDriftRule = "baseline_epoch_contract_bundle_or_plan_differs"
		epochContractRemediationDriftReason = "epoch-contract remediation bundle or plan hash differs from baseline compatible state"
	}
	epochContractRemediationDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineBundle=" + baselineEpochContractRemediationBundleID + ";" +
			"currentBundle=" + epochContractRemediationBundleID + ";" +
			"baselinePlan=" + baselineEpochContractRemediationPlanHash + ";" +
			"currentPlan=" + epochContractRemediationPlanHash + ";" +
			"bundleDrift=" + epochContractRemediationBundleDriftStatus + ";" +
			"planDrift=" + epochContractRemediationPlanDriftStatus + ";",
	)
	epochContractCheckDriftFields := []string{}
	if epochContractCheckSummary != baselineEpochContractCheckSummary {
		epochContractCheckDriftFields = append(epochContractCheckDriftFields, "check_summary")
	}
	if epochContractImpactClassification != baselineEpochContractImpactClassification {
		epochContractCheckDriftFields = append(epochContractCheckDriftFields, "impact_classification")
	}
	if epochContractRemediationSummary != baselineEpochContractRemediationSummary {
		epochContractCheckDriftFields = append(epochContractCheckDriftFields, "remediation_summary")
	}
	epochContractCheckDriftStatus := "unchanged"
	epochContractCheckDriftRule := "baseline_epoch_contract_check_state_matches"
	epochContractCheckDriftReason := "epoch-contract check, impact, and remediation summaries match baseline"
	if len(epochContractCheckDriftFields) > 0 {
		epochContractCheckDriftStatus = "changed"
		epochContractCheckDriftRule = "baseline_epoch_contract_check_state_differs"
		epochContractCheckDriftReason = "epoch-contract check, impact, or remediation summaries differ from baseline"
	}
	epochContractCheckDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineCheckSummary=" + baselineEpochContractCheckSummary + ";" +
			"currentCheckSummary=" + epochContractCheckSummary + ";" +
			"baselineImpact=" + baselineEpochContractImpactClassification + ";" +
			"currentImpact=" + epochContractImpactClassification + ";" +
			"baselineRemediationSummary=" + baselineEpochContractRemediationSummary + ";" +
			"currentRemediationSummary=" + epochContractRemediationSummary + ";" +
			"driftStatus=" + epochContractCheckDriftStatus + ";" +
			"driftFields=" + strings.Join(epochContractCheckDriftFields, ",") + ";",
	)
	epochContractGovernanceEvaluationOrder := []string{"stable_if_compatible_and_baseline_aligned", "degraded_if_incompatible", "degraded_if_check_or_remediation_drift", "unknown_default"}
	epochContractGovernanceChecks := map[string]bool{
		"contract_fully_compatible":    epochContractFullyCompatible,
		"check_state_baseline_aligned": epochContractCheckDriftStatus == "unchanged",
		"remediation_baseline_aligned": epochContractRemediationDriftSummary == "baseline_aligned",
	}
	epochContractGovernanceFailedChecks := []string{}
	epochContractGovernanceFailedReasons := map[string]string{}
	if !epochContractGovernanceChecks["contract_fully_compatible"] {
		epochContractGovernanceFailedChecks = append(epochContractGovernanceFailedChecks, "contract_fully_compatible")
		epochContractGovernanceFailedReasons["contract_fully_compatible"] = "epoch-contract compatibility checks are not fully satisfied"
	}
	if !epochContractGovernanceChecks["check_state_baseline_aligned"] {
		epochContractGovernanceFailedChecks = append(epochContractGovernanceFailedChecks, "check_state_baseline_aligned")
		epochContractGovernanceFailedReasons["check_state_baseline_aligned"] = "epoch-contract check baseline drift was detected"
	}
	if !epochContractGovernanceChecks["remediation_baseline_aligned"] {
		epochContractGovernanceFailedChecks = append(epochContractGovernanceFailedChecks, "remediation_baseline_aligned")
		epochContractGovernanceFailedReasons["remediation_baseline_aligned"] = "epoch-contract remediation baseline drift was detected"
	}
	epochContractGovernanceState := "stable"
	epochContractGovernanceRule := "stable_if_compatible_and_baseline_aligned"
	epochContractGovernanceReason := "epoch-contract checks are compatible and both check/remediation baselines are aligned"
	if !epochContractGovernanceChecks["contract_fully_compatible"] {
		epochContractGovernanceState = "degraded"
		epochContractGovernanceRule = "degraded_if_incompatible"
		epochContractGovernanceReason = "epoch-contract governance is degraded because required contract checks failed"
	} else if !epochContractGovernanceChecks["check_state_baseline_aligned"] || !epochContractGovernanceChecks["remediation_baseline_aligned"] {
		epochContractGovernanceState = "degraded"
		epochContractGovernanceRule = "degraded_if_check_or_remediation_drift"
		epochContractGovernanceReason = "epoch-contract governance is degraded because baseline drift was detected"
	}
	epochContractGovernanceFingerprint := explainDiagnosticPostureHashString(
		"state=" + epochContractGovernanceState + ";" +
			"rule=" + epochContractGovernanceRule + ";" +
			"failedChecks=" + strings.Join(epochContractGovernanceFailedChecks, ",") + ";" +
			"checkDrift=" + epochContractCheckDriftStatus + ";" +
			"remediationDrift=" + epochContractRemediationDriftSummary + ";",
	)
	epochContractGovernanceRemediationByCheck := map[string]string{
		"contract_fully_compatible":    "restore required epoch-contract component hashes and recompute epoch contract compatibility artifacts",
		"check_state_baseline_aligned": "investigate epoch-contract check summary drift and realign check/impact/remediation summaries to baseline",
		"remediation_baseline_aligned": "investigate remediation bundle/plan drift and restore baseline-compatible remediation metadata",
	}
	epochContractGovernanceRemediationPriorityOrder := []string{"contract_fully_compatible", "check_state_baseline_aligned", "remediation_baseline_aligned"}
	epochContractGovernanceRemediationActions := make([]string, 0, len(epochContractGovernanceFailedChecks))
	epochContractGovernanceFailedCheckSet := map[string]struct{}{}
	for _, check := range epochContractGovernanceFailedChecks {
		epochContractGovernanceFailedCheckSet[check] = struct{}{}
	}
	for _, check := range epochContractGovernanceRemediationPriorityOrder {
		if _, ok := epochContractGovernanceFailedCheckSet[check]; ok {
			epochContractGovernanceRemediationActions = append(epochContractGovernanceRemediationActions, epochContractGovernanceRemediationByCheck[check])
		}
	}
	epochContractGovernanceRemediationSummary := "none_required"
	if len(epochContractGovernanceRemediationActions) > 0 {
		epochContractGovernanceRemediationSummary = "action_required"
	}
	epochContractGovernanceRemediationUrgency := "none"
	if epochContractGovernanceState == "degraded" {
		epochContractGovernanceRemediationUrgency = "high"
	}
	epochContractGovernanceRemediationPlanHash := explainDiagnosticPostureHashString(
		"summary=" + epochContractGovernanceRemediationSummary + ";" +
			"actions=" + strings.Join(epochContractGovernanceRemediationActions, "||") + ";",
	)
	epochContractGovernanceRemediationFingerprint := explainDiagnosticPostureHashString(
		"state=" + epochContractGovernanceState + ";" +
			"summary=" + epochContractGovernanceRemediationSummary + ";" +
			"urgency=" + epochContractGovernanceRemediationUrgency + ";" +
			"planHash=" + epochContractGovernanceRemediationPlanHash + ";",
	)
	baselineEpochContractGovernanceRemediationSummary := "none_required"
	baselineEpochContractGovernanceRemediationUrgency := "none"
	baselineEpochContractGovernanceRemediationPlanHash := explainDiagnosticPostureHashString("summary=none_required;actions=;")
	epochContractGovernanceRemediationSummaryDriftStatus := "unchanged"
	if epochContractGovernanceRemediationSummary != baselineEpochContractGovernanceRemediationSummary {
		epochContractGovernanceRemediationSummaryDriftStatus = "changed"
	}
	epochContractGovernanceRemediationUrgencyDriftStatus := "unchanged"
	if epochContractGovernanceRemediationUrgency != baselineEpochContractGovernanceRemediationUrgency {
		epochContractGovernanceRemediationUrgencyDriftStatus = "changed"
	}
	epochContractGovernanceRemediationPlanDriftStatus := "unchanged"
	if epochContractGovernanceRemediationPlanHash != baselineEpochContractGovernanceRemediationPlanHash {
		epochContractGovernanceRemediationPlanDriftStatus = "changed"
	}
	epochContractGovernanceRemediationDriftFields := []string{}
	if epochContractGovernanceRemediationSummaryDriftStatus == "changed" {
		epochContractGovernanceRemediationDriftFields = append(epochContractGovernanceRemediationDriftFields, "summary")
	}
	if epochContractGovernanceRemediationUrgencyDriftStatus == "changed" {
		epochContractGovernanceRemediationDriftFields = append(epochContractGovernanceRemediationDriftFields, "urgency")
	}
	if epochContractGovernanceRemediationPlanDriftStatus == "changed" {
		epochContractGovernanceRemediationDriftFields = append(epochContractGovernanceRemediationDriftFields, "plan_hash")
	}
	epochContractGovernanceRemediationDriftSummary := "baseline_aligned"
	epochContractGovernanceRemediationDriftRule := "baseline_governance_remediation_matches"
	epochContractGovernanceRemediationDriftReason := "epoch-contract governance remediation summary, urgency, and plan hash match baseline"
	if len(epochContractGovernanceRemediationDriftFields) > 0 {
		epochContractGovernanceRemediationDriftSummary = "baseline_drifted"
		epochContractGovernanceRemediationDriftRule = "baseline_governance_remediation_differs"
		epochContractGovernanceRemediationDriftReason = "epoch-contract governance remediation summary, urgency, or plan hash differs from baseline"
	}
	epochContractGovernanceRemediationDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineSummary=" + baselineEpochContractGovernanceRemediationSummary + ";" +
			"currentSummary=" + epochContractGovernanceRemediationSummary + ";" +
			"baselineUrgency=" + baselineEpochContractGovernanceRemediationUrgency + ";" +
			"currentUrgency=" + epochContractGovernanceRemediationUrgency + ";" +
			"baselinePlan=" + baselineEpochContractGovernanceRemediationPlanHash + ";" +
			"currentPlan=" + epochContractGovernanceRemediationPlanHash + ";" +
			"driftFields=" + strings.Join(epochContractGovernanceRemediationDriftFields, ",") + ";",
	)
	epochContractGovernanceRemediationVerdictEvaluationOrder := []string{"stable_if_governance_stable_and_remediation_baseline_aligned", "degraded_if_governance_degraded", "degraded_if_governance_remediation_drift_detected", "unknown_default"}
	epochContractGovernanceRemediationVerdictChecks := map[string]bool{
		"governance_state_stable":                      epochContractGovernanceState == "stable",
		"governance_remediation_baseline_aligned":      epochContractGovernanceRemediationDriftSummary == "baseline_aligned",
		"governance_remediation_plan_baseline_aligned": epochContractGovernanceRemediationPlanDriftStatus == "unchanged",
	}
	epochContractGovernanceRemediationVerdictFailedChecks := []string{}
	epochContractGovernanceRemediationVerdictFailedReasons := map[string]string{}
	if !epochContractGovernanceRemediationVerdictChecks["governance_state_stable"] {
		epochContractGovernanceRemediationVerdictFailedChecks = append(epochContractGovernanceRemediationVerdictFailedChecks, "governance_state_stable")
		epochContractGovernanceRemediationVerdictFailedReasons["governance_state_stable"] = "governance state is degraded"
	}
	if !epochContractGovernanceRemediationVerdictChecks["governance_remediation_baseline_aligned"] {
		epochContractGovernanceRemediationVerdictFailedChecks = append(epochContractGovernanceRemediationVerdictFailedChecks, "governance_remediation_baseline_aligned")
		epochContractGovernanceRemediationVerdictFailedReasons["governance_remediation_baseline_aligned"] = "governance remediation summary or urgency drifted from baseline"
	}
	if !epochContractGovernanceRemediationVerdictChecks["governance_remediation_plan_baseline_aligned"] {
		epochContractGovernanceRemediationVerdictFailedChecks = append(epochContractGovernanceRemediationVerdictFailedChecks, "governance_remediation_plan_baseline_aligned")
		epochContractGovernanceRemediationVerdictFailedReasons["governance_remediation_plan_baseline_aligned"] = "governance remediation plan hash drifted from baseline"
	}
	epochContractGovernanceRemediationVerdictState := "stable"
	epochContractGovernanceRemediationVerdictRule := "stable_if_governance_stable_and_remediation_baseline_aligned"
	epochContractGovernanceRemediationVerdictReason := "governance is stable and governance-remediation metadata is baseline aligned"
	if !epochContractGovernanceRemediationVerdictChecks["governance_state_stable"] {
		epochContractGovernanceRemediationVerdictState = "degraded"
		epochContractGovernanceRemediationVerdictRule = "degraded_if_governance_degraded"
		epochContractGovernanceRemediationVerdictReason = "governance-remediation verdict is degraded because governance state is degraded"
	} else if !epochContractGovernanceRemediationVerdictChecks["governance_remediation_baseline_aligned"] || !epochContractGovernanceRemediationVerdictChecks["governance_remediation_plan_baseline_aligned"] {
		epochContractGovernanceRemediationVerdictState = "degraded"
		epochContractGovernanceRemediationVerdictRule = "degraded_if_governance_remediation_drift_detected"
		epochContractGovernanceRemediationVerdictReason = "governance-remediation verdict is degraded because governance-remediation drift was detected"
	}
	epochContractGovernanceRemediationVerdictFingerprint := explainDiagnosticPostureHashString(
		"state=" + epochContractGovernanceRemediationVerdictState + ";" +
			"rule=" + epochContractGovernanceRemediationVerdictRule + ";" +
			"failedChecks=" + strings.Join(epochContractGovernanceRemediationVerdictFailedChecks, ",") + ";" +
			"driftSummary=" + epochContractGovernanceRemediationDriftSummary + ";" +
			"planDrift=" + epochContractGovernanceRemediationPlanDriftStatus + ";",
	)
	epochContractGovernanceRemediationVerdictSeverityByCheck := map[string]string{
		"governance_state_stable":                      "high",
		"governance_remediation_baseline_aligned":      "medium",
		"governance_remediation_plan_baseline_aligned": "high",
	}
	epochContractGovernanceRemediationVerdictRequirementByCheck := map[string]string{
		"governance_state_stable":                      "required",
		"governance_remediation_baseline_aligned":      "required",
		"governance_remediation_plan_baseline_aligned": "required",
	}
	epochContractGovernanceRemediationVerdictBundleID := explainDiagnosticPostureRemediationBundleID(
		epochContractGovernanceRemediationVerdictState,
		epochContractGovernanceRemediationSummary,
		epochContractGovernanceRemediationUrgency,
	)
	baselineEpochContractGovernanceRemediationVerdictBundleID := explainDiagnosticPostureRemediationBundleID(
		"stable",
		"none_required",
		"none",
	)
	epochContractGovernanceRemediationVerdictBundleDriftStatus := "unchanged"
	if epochContractGovernanceRemediationVerdictBundleID != baselineEpochContractGovernanceRemediationVerdictBundleID {
		epochContractGovernanceRemediationVerdictBundleDriftStatus = "changed"
	}
	baselineEpochContractGovernanceFingerprint := explainDiagnosticPostureHashString(
		"state=stable;" +
			"rule=stable_if_compatible_and_baseline_aligned;" +
			"failedChecks=;" +
			"checkDrift=unchanged;" +
			"remediationDrift=baseline_aligned;",
	)
	baselineEpochContractGovernanceRemediationFingerprint := explainDiagnosticPostureHashString(
		"state=stable;" +
			"summary=none_required;" +
			"urgency=none;" +
			"planHash=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";",
	)
	baselineEpochContractGovernanceRemediationDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineSummary=none_required;" +
			"currentSummary=none_required;" +
			"baselineUrgency=none;" +
			"currentUrgency=none;" +
			"baselinePlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
			"currentPlan=" + explainDiagnosticPostureHashString("summary=none_required;actions=;") + ";" +
			"driftFields=;",
	)
	baselineEpochContractGovernanceRemediationVerdictFingerprint := explainDiagnosticPostureHashString(
		"state=stable;" +
			"rule=stable_if_governance_stable_and_remediation_baseline_aligned;" +
			"failedChecks=;" +
			"driftSummary=baseline_aligned;" +
			"planDrift=unchanged;",
	)
	epochContractGovernanceLineageComponents := map[string]string{
		"governanceFingerprint":                   epochContractGovernanceFingerprint,
		"governanceRemediationFingerprint":        epochContractGovernanceRemediationFingerprint,
		"governanceRemediationDriftFingerprint":   epochContractGovernanceRemediationDriftFingerprint,
		"governanceRemediationVerdictFingerprint": epochContractGovernanceRemediationVerdictFingerprint,
	}
	epochContractGovernanceLineageVersion := "v1"
	epochContractGovernanceLineageHash := explainDiagnosticPostureHashString(
		"version=" + epochContractGovernanceLineageVersion + ";" +
			"governanceFingerprint=" + epochContractGovernanceFingerprint + ";" +
			"governanceRemediationFingerprint=" + epochContractGovernanceRemediationFingerprint + ";" +
			"governanceRemediationDriftFingerprint=" + epochContractGovernanceRemediationDriftFingerprint + ";" +
			"governanceRemediationVerdictFingerprint=" + epochContractGovernanceRemediationVerdictFingerprint + ";",
	)
	baselineEpochContractGovernanceLineageHash := explainDiagnosticPostureHashString(
		"version=" + epochContractGovernanceLineageVersion + ";" +
			"governanceFingerprint=" + baselineEpochContractGovernanceFingerprint + ";" +
			"governanceRemediationFingerprint=" + baselineEpochContractGovernanceRemediationFingerprint + ";" +
			"governanceRemediationDriftFingerprint=" + baselineEpochContractGovernanceRemediationDriftFingerprint + ";" +
			"governanceRemediationVerdictFingerprint=" + baselineEpochContractGovernanceRemediationVerdictFingerprint + ";",
	)
	epochContractGovernanceLineageDriftStatus := "unchanged"
	if epochContractGovernanceLineageHash != baselineEpochContractGovernanceLineageHash {
		epochContractGovernanceLineageDriftStatus = "changed"
	}
	epochContractGovernanceLineageDriftFields := []string{}
	if epochContractGovernanceRemediationVerdictBundleDriftStatus == "changed" {
		epochContractGovernanceLineageDriftFields = append(epochContractGovernanceLineageDriftFields, "verdict_bundle_id")
	}
	if epochContractGovernanceLineageHash != baselineEpochContractGovernanceLineageHash {
		epochContractGovernanceLineageDriftFields = append(epochContractGovernanceLineageDriftFields, "lineage_hash")
	}
	epochContractGovernanceLineageDriftSummary := "baseline_aligned"
	epochContractGovernanceLineageDriftRule := "baseline_governance_lineage_matches"
	epochContractGovernanceLineageDriftReason := "governance-remediation lineage metadata matches baseline"
	if len(epochContractGovernanceLineageDriftFields) > 0 {
		epochContractGovernanceLineageDriftSummary = "baseline_drifted"
		epochContractGovernanceLineageDriftRule = "baseline_governance_lineage_differs"
		epochContractGovernanceLineageDriftReason = "governance-remediation lineage metadata differs from baseline"
	}
	epochContractGovernanceLineageDriftFingerprint := explainDiagnosticPostureHashString(
		"baselineLineageHash=" + baselineEpochContractGovernanceLineageHash + ";" +
			"currentLineageHash=" + epochContractGovernanceLineageHash + ";" +
			"baselineBundle=" + baselineEpochContractGovernanceRemediationVerdictBundleID + ";" +
			"currentBundle=" + epochContractGovernanceRemediationVerdictBundleID + ";" +
			"driftFields=" + strings.Join(epochContractGovernanceLineageDriftFields, ",") + ";",
	)
	lineageComponentHashesPresent := true
	for _, component := range []string{"governanceFingerprint", "governanceRemediationFingerprint", "governanceRemediationDriftFingerprint", "governanceRemediationVerdictFingerprint"} {
		if strings.TrimSpace(epochContractGovernanceLineageComponents[component]) == "" {
			lineageComponentHashesPresent = false
			break
		}
	}
	lineageDriftStatusMatchesHashes := (epochContractGovernanceLineageDriftStatus == "changed") == (epochContractGovernanceLineageHash != baselineEpochContractGovernanceLineageHash)
	epochContractGovernanceLineageCheckEvaluationOrder := []string{"lineage_hash_present", "lineage_component_hashes_present", "lineage_drift_status_matches_hashes"}
	epochContractGovernanceLineageChecks := map[string]bool{
		"lineage_hash_present":                strings.TrimSpace(epochContractGovernanceLineageHash) != "",
		"lineage_component_hashes_present":    lineageComponentHashesPresent,
		"lineage_drift_status_matches_hashes": lineageDriftStatusMatchesHashes,
	}
	epochContractGovernanceLineageFailedChecks := []string{}
	epochContractGovernanceLineageFailedReasons := map[string]string{}
	for _, check := range epochContractGovernanceLineageCheckEvaluationOrder {
		if ok := epochContractGovernanceLineageChecks[check]; !ok {
			epochContractGovernanceLineageFailedChecks = append(epochContractGovernanceLineageFailedChecks, check)
			switch check {
			case "lineage_hash_present":
				epochContractGovernanceLineageFailedReasons[check] = "governance lineage hash is empty"
			case "lineage_component_hashes_present":
				epochContractGovernanceLineageFailedReasons[check] = "one or more governance lineage component hashes are empty"
			case "lineage_drift_status_matches_hashes":
				epochContractGovernanceLineageFailedReasons[check] = "governance lineage drift status does not match lineage hash comparison"
			}
		}
	}
	epochContractGovernanceLineageSummary := "consistent"
	if len(epochContractGovernanceLineageFailedChecks) > 0 {
		epochContractGovernanceLineageSummary = "inconsistent"
	}
	epochContractGovernanceLineageFingerprint := explainDiagnosticPostureHashString(
		"summary=" + epochContractGovernanceLineageSummary + ";" +
			"lineageHash=" + epochContractGovernanceLineageHash + ";" +
			"failedChecks=" + strings.Join(epochContractGovernanceLineageFailedChecks, ",") + ";",
	)
	epochContractGovernanceLineageVerdictEvaluationOrder := []string{"stable_if_lineage_consistent_and_baseline_aligned", "degraded_if_lineage_inconsistent", "degraded_if_lineage_baseline_drifted", "unknown_default"}
	epochContractGovernanceLineageVerdictChecks := map[string]bool{
		"lineage_consistent":              epochContractGovernanceLineageSummary == "consistent",
		"lineage_baseline_aligned":        epochContractGovernanceLineageDriftSummary == "baseline_aligned",
		"lineage_drift_status_consistent": (epochContractGovernanceLineageDriftStatus == "changed") == (len(epochContractGovernanceLineageDriftFields) > 0),
	}
	epochContractGovernanceLineageVerdictFailedChecks := []string{}
	epochContractGovernanceLineageVerdictFailedReasons := map[string]string{}
	for _, check := range []string{"lineage_consistent", "lineage_baseline_aligned", "lineage_drift_status_consistent"} {
		if ok := epochContractGovernanceLineageVerdictChecks[check]; !ok {
			epochContractGovernanceLineageVerdictFailedChecks = append(epochContractGovernanceLineageVerdictFailedChecks, check)
			switch check {
			case "lineage_consistent":
				epochContractGovernanceLineageVerdictFailedReasons[check] = "governance lineage checks are inconsistent"
			case "lineage_baseline_aligned":
				epochContractGovernanceLineageVerdictFailedReasons[check] = "governance lineage drift summary is baseline_drifted"
			case "lineage_drift_status_consistent":
				epochContractGovernanceLineageVerdictFailedReasons[check] = "governance lineage drift status does not match drift fields"
			}
		}
	}
	epochContractGovernanceLineageVerdictState := "stable"
	epochContractGovernanceLineageVerdictRule := "stable_if_lineage_consistent_and_baseline_aligned"
	epochContractGovernanceLineageVerdictReason := "governance lineage is consistent and baseline aligned"
	if !epochContractGovernanceLineageVerdictChecks["lineage_consistent"] {
		epochContractGovernanceLineageVerdictState = "degraded"
		epochContractGovernanceLineageVerdictRule = "degraded_if_lineage_inconsistent"
		epochContractGovernanceLineageVerdictReason = "governance lineage verdict is degraded because lineage checks are inconsistent"
	} else if !epochContractGovernanceLineageVerdictChecks["lineage_baseline_aligned"] {
		epochContractGovernanceLineageVerdictState = "degraded"
		epochContractGovernanceLineageVerdictRule = "degraded_if_lineage_baseline_drifted"
		epochContractGovernanceLineageVerdictReason = "governance lineage verdict is degraded because baseline drift was detected"
	}
	epochContractGovernanceLineageVerdictFingerprint := explainDiagnosticPostureHashString(
		"state=" + epochContractGovernanceLineageVerdictState + ";" +
			"rule=" + epochContractGovernanceLineageVerdictRule + ";" +
			"failedChecks=" + strings.Join(epochContractGovernanceLineageVerdictFailedChecks, ",") + ";" +
			"lineageSummary=" + epochContractGovernanceLineageSummary + ";" +
			"lineageDrift=" + epochContractGovernanceLineageDriftSummary + ";",
	)
	epochContractGovernanceLineageVerdictSeverityByCheck := map[string]string{
		"lineage_consistent":              "high",
		"lineage_baseline_aligned":        "high",
		"lineage_drift_status_consistent": "medium",
	}
	epochContractGovernanceLineageVerdictRequirementByCheck := map[string]string{
		"lineage_consistent":              "required",
		"lineage_baseline_aligned":        "required",
		"lineage_drift_status_consistent": "required",
	}
	epochContractGovernanceLineageVerdictSummary := "none_required"
	epochContractGovernanceLineageVerdictUrgency := "none"
	if epochContractGovernanceLineageVerdictState != "stable" {
		epochContractGovernanceLineageVerdictSummary = "investigate_lineage_integrity"
		epochContractGovernanceLineageVerdictUrgency = "high"
	}
	epochContractGovernanceLineageVerdictBundleID := explainDiagnosticPostureRemediationBundleID(
		epochContractGovernanceLineageVerdictState,
		epochContractGovernanceLineageVerdictSummary,
		epochContractGovernanceLineageVerdictUrgency,
	)
	baselineEpochContractGovernanceLineageVerdictBundleID := explainDiagnosticPostureRemediationBundleID(
		"stable",
		"none_required",
		"none",
	)
	epochContractGovernanceLineageVerdictBundleDriftStatus := "unchanged"
	if epochContractGovernanceLineageVerdictBundleID != baselineEpochContractGovernanceLineageVerdictBundleID {
		epochContractGovernanceLineageVerdictBundleDriftStatus = "changed"
	}
	baselineEpochContractGovernanceLineageVerdictFingerprint := explainDiagnosticPostureHashString(
		"state=stable;" +
			"rule=stable_if_lineage_consistent_and_baseline_aligned;" +
			"failedChecks=;" +
			"lineageSummary=consistent;" +
			"lineageDrift=baseline_aligned;",
	)
	epochContractGovernanceLineageVerdictFingerprintDriftStatus := "unchanged"
	if epochContractGovernanceLineageVerdictFingerprint != baselineEpochContractGovernanceLineageVerdictFingerprint {
		epochContractGovernanceLineageVerdictFingerprintDriftStatus = "changed"
	}
	epochContractIncompatibilityReasons := []string{}
	for _, check := range []string{"ruleCatalogPresent", "evaluationOrderPresent", "consistencySchemaPresent", "overallHashPresent"} {
		if ok, exists := epochContractCompatibility[check]; !exists || !ok {
			epochContractIncompatibilityReasons = append(epochContractIncompatibilityReasons, check)
		}
	}
	epochContractCompatibilitySummary := "epoch_contract_complete"
	if len(epochContractIncompatibilityReasons) > 0 {
		epochContractCompatibilitySummary = "epoch_contract_incomplete"
	}
	epochContractCheckFingerprint := explainDiagnosticPostureHashString(
		"summary=" + epochContractCheckSummary + ";" +
			"failedChecks=" + strings.Join(epochContractFailedChecks, ",") + ";" +
			"overallHash=" + epochContractOverallHash + ";",
	)
	epochContractFingerprint := explainDiagnosticPostureHashString(
		"summary=" + epochContractCompatibilitySummary + ";" +
			"overallHash=" + epochContractOverallHash + ";" +
			"reasons=" + strings.Join(epochContractIncompatibilityReasons, ",") + ";",
	)
	epochTransitionFingerprint := explainDiagnosticPostureHashString(
		"contractEpoch=" + contractEpoch + ";" +
			"baselineContractEpoch=" + baselineContractEpoch + ";" +
			"contractTransition=" + contractEpochTransition + ";" +
			"remediationEpoch=" + remediationEpoch + ";" +
			"baselineRemediationEpoch=" + baselineRemediationEpoch + ";" +
			"remediationTransition=" + remediationEpochTransition + ";" +
			"summary=" + epochTransitionSummary + ";" +
			"failed=" + strings.Join(epochFailedTransitions, ",") + ";",
	)

	return map[string]any{
		"compatibilityVersion":                                        "v1",
		"contractEpoch":                                               contractEpoch,
		"baselineContractEpoch":                                       baselineContractEpoch,
		"contractEpochTransition":                                     contractEpochTransition,
		"remediationEpoch":                                            remediationEpoch,
		"baselineRemediationEpoch":                                    baselineRemediationEpoch,
		"remediationEpochTransition":                                  remediationEpochTransition,
		"epochTransitionRule":                                         epochTransitionRule,
		"epochTransitionReason":                                       epochTransitionReason,
		"epochEvaluationOrder":                                        epochEvaluationOrder,
		"epochReasonCodes":                                            epochReasonCodes,
		"epochCompatibility":                                          epochCompatibility,
		"epochFailedTransitions":                                      epochFailedTransitions,
		"epochFailedTransitionReasons":                                epochFailedTransitionReasons,
		"epochTransitionSummary":                                      epochTransitionSummary,
		"epochImpactEvaluationOrder":                                  epochImpactEvaluationOrder,
		"epochImpactByCheck":                                          epochImpactByCheck,
		"epochImpactClassification":                                   epochImpactClassification,
		"epochImpactRule":                                             epochImpactRule,
		"epochImpactReason":                                           epochImpactReason,
		"epochRemediationByCheck":                                     epochRemediationByCheck,
		"epochRemediationPriorityOrder":                               epochRemediationPriorityOrder,
		"epochRemediationSummary":                                     epochRemediationSummary,
		"epochRemediationActions":                                     epochRemediationActions,
		"epochRemediationActionPlan":                                  epochRemediationActionPlan,
		"epochRemediationPlanHash":                                    epochRemediationPlanHash,
		"epochStateID":                                                epochStateID,
		"baselineEpochStateID":                                        baselineEpochStateID,
		"epochDriftStatus":                                            epochDriftStatus,
		"epochDriftFields":                                            epochDriftFields,
		"epochCompatibilityFingerprint":                               epochCompatibilityFingerprint,
		"epochConsistencyEvaluationOrder":                             epochConsistencyEvaluationOrder,
		"epochConsistencyReasonCodes":                                 epochConsistencyReasonCodes,
		"epochConsistencyChecks":                                      epochConsistencyChecks,
		"epochConsistencyFailedChecks":                                epochConsistencyFailedChecks,
		"epochConsistencyFailedReasons":                               epochConsistencyFailedReasons,
		"epochConsistencySummary":                                     epochConsistencySummary,
		"epochConsistencyFingerprint":                                 epochConsistencyFingerprint,
		"epochRuleCatalogVersion":                                     epochRuleCatalogVersion,
		"epochRuleCatalog":                                            epochRuleCatalog,
		"epochMatchedRuleLookup":                                      epochMatchedRuleLookup,
		"epochRuleCatalogConsistent":                                  epochRuleCatalogConsistent,
		"epochRuleCatalogMismatches":                                  epochRuleCatalogMismatches,
		"epochContractVersion":                                        epochContractVersion,
		"epochContractHash":                                           epochContractOverallHash,
		"epochContractComponents":                                     epochContractComponents,
		"epochContractCompatibility":                                  epochContractCompatibility,
		"epochContractCheckEvaluationOrder":                           epochContractCheckEvaluationOrder,
		"epochContractCheckReasonCodes":                               epochContractCheckReasonCodes,
		"epochContractCheckStatus":                                    epochContractCheckStatus,
		"epochContractFailedChecks":                                   epochContractFailedChecks,
		"epochContractFailedCheckReasons":                             epochContractFailedCheckReasons,
		"epochContractCheckSummary":                                   epochContractCheckSummary,
		"epochContractCheckFingerprint":                               epochContractCheckFingerprint,
		"epochContractFullyCompatible":                                epochContractFullyCompatible,
		"epochContractImpactEvaluationOrder":                          epochContractImpactEvaluationOrder,
		"epochContractImpactByCheck":                                  epochContractImpactByCheck,
		"epochContractImpactClassification":                           epochContractImpactClassification,
		"epochContractImpactRule":                                     epochContractImpactRule,
		"epochContractImpactReason":                                   epochContractImpactReason,
		"epochContractRemediationByCheck":                             epochContractRemediationByCheck,
		"epochContractRemediationSeverityByCheck":                     epochContractRemediationSeverityByCheck,
		"epochContractRemediationRequirementByCheck":                  epochContractRemediationRequirementByCheck,
		"epochContractRemediationPriorityOrder":                       epochContractRemediationPriorityOrder,
		"epochContractRemediationSummary":                             epochContractRemediationSummary,
		"epochContractRemediationUrgency":                             epochContractRemediationUrgency,
		"epochContractRemediationBundleID":                            epochContractRemediationBundleID,
		"epochContractRemediationActions":                             epochContractRemediationActions,
		"epochContractRemediationPlanHash":                            epochContractRemediationPlanHash,
		"epochContractRemediationFingerprint":                         epochContractRemediationFingerprint,
		"baselineEpochContractCheckSummary":                           baselineEpochContractCheckSummary,
		"baselineEpochContractImpactClassification":                   baselineEpochContractImpactClassification,
		"baselineEpochContractRemediationSummary":                     baselineEpochContractRemediationSummary,
		"baselineEpochContractRemediationUrgency":                     baselineEpochContractRemediationUrgency,
		"baselineEpochContractRemediationBundleID":                    baselineEpochContractRemediationBundleID,
		"baselineEpochContractRemediationPlanHash":                    baselineEpochContractRemediationPlanHash,
		"epochContractRemediationBundleDriftStatus":                   epochContractRemediationBundleDriftStatus,
		"epochContractRemediationPlanDriftStatus":                     epochContractRemediationPlanDriftStatus,
		"epochContractRemediationDriftSummary":                        epochContractRemediationDriftSummary,
		"epochContractRemediationDriftRule":                           epochContractRemediationDriftRule,
		"epochContractRemediationDriftReason":                         epochContractRemediationDriftReason,
		"epochContractRemediationDriftFields":                         epochContractRemediationDriftFields,
		"epochContractRemediationDriftFingerprint":                    epochContractRemediationDriftFingerprint,
		"epochContractCheckDriftStatus":                               epochContractCheckDriftStatus,
		"epochContractCheckDriftRule":                                 epochContractCheckDriftRule,
		"epochContractCheckDriftReason":                               epochContractCheckDriftReason,
		"epochContractCheckDriftFields":                               epochContractCheckDriftFields,
		"epochContractCheckDriftFingerprint":                          epochContractCheckDriftFingerprint,
		"epochContractGovernanceEvaluationOrder":                      epochContractGovernanceEvaluationOrder,
		"epochContractGovernanceChecks":                               epochContractGovernanceChecks,
		"epochContractGovernanceFailedChecks":                         epochContractGovernanceFailedChecks,
		"epochContractGovernanceFailedReasons":                        epochContractGovernanceFailedReasons,
		"epochContractGovernanceState":                                epochContractGovernanceState,
		"epochContractGovernanceRule":                                 epochContractGovernanceRule,
		"epochContractGovernanceReason":                               epochContractGovernanceReason,
		"epochContractGovernanceFingerprint":                          epochContractGovernanceFingerprint,
		"epochContractGovernanceRemediationByCheck":                   epochContractGovernanceRemediationByCheck,
		"epochContractGovernanceRemediationPriorityOrder":             epochContractGovernanceRemediationPriorityOrder,
		"epochContractGovernanceRemediationActions":                   epochContractGovernanceRemediationActions,
		"epochContractGovernanceRemediationSummary":                   epochContractGovernanceRemediationSummary,
		"epochContractGovernanceRemediationUrgency":                   epochContractGovernanceRemediationUrgency,
		"epochContractGovernanceRemediationPlanHash":                  epochContractGovernanceRemediationPlanHash,
		"epochContractGovernanceRemediationFingerprint":               epochContractGovernanceRemediationFingerprint,
		"baselineEpochContractGovernanceRemediationSummary":           baselineEpochContractGovernanceRemediationSummary,
		"baselineEpochContractGovernanceRemediationUrgency":           baselineEpochContractGovernanceRemediationUrgency,
		"baselineEpochContractGovernanceRemediationPlanHash":          baselineEpochContractGovernanceRemediationPlanHash,
		"epochContractGovernanceRemediationSummaryDriftStatus":        epochContractGovernanceRemediationSummaryDriftStatus,
		"epochContractGovernanceRemediationUrgencyDriftStatus":        epochContractGovernanceRemediationUrgencyDriftStatus,
		"epochContractGovernanceRemediationPlanDriftStatus":           epochContractGovernanceRemediationPlanDriftStatus,
		"epochContractGovernanceRemediationDriftSummary":              epochContractGovernanceRemediationDriftSummary,
		"epochContractGovernanceRemediationDriftRule":                 epochContractGovernanceRemediationDriftRule,
		"epochContractGovernanceRemediationDriftReason":               epochContractGovernanceRemediationDriftReason,
		"epochContractGovernanceRemediationDriftFields":               epochContractGovernanceRemediationDriftFields,
		"epochContractGovernanceRemediationDriftFingerprint":          epochContractGovernanceRemediationDriftFingerprint,
		"epochContractGovernanceRemediationVerdictEvaluationOrder":    epochContractGovernanceRemediationVerdictEvaluationOrder,
		"epochContractGovernanceRemediationVerdictChecks":             epochContractGovernanceRemediationVerdictChecks,
		"epochContractGovernanceRemediationVerdictFailedChecks":       epochContractGovernanceRemediationVerdictFailedChecks,
		"epochContractGovernanceRemediationVerdictFailedReasons":      epochContractGovernanceRemediationVerdictFailedReasons,
		"epochContractGovernanceRemediationVerdictState":              epochContractGovernanceRemediationVerdictState,
		"epochContractGovernanceRemediationVerdictRule":               epochContractGovernanceRemediationVerdictRule,
		"epochContractGovernanceRemediationVerdictReason":             epochContractGovernanceRemediationVerdictReason,
		"epochContractGovernanceRemediationVerdictFingerprint":        epochContractGovernanceRemediationVerdictFingerprint,
		"epochContractGovernanceRemediationVerdictSeverityByCheck":    epochContractGovernanceRemediationVerdictSeverityByCheck,
		"epochContractGovernanceRemediationVerdictRequirementByCheck": epochContractGovernanceRemediationVerdictRequirementByCheck,
		"epochContractGovernanceRemediationVerdictBundleID":           epochContractGovernanceRemediationVerdictBundleID,
		"baselineEpochContractGovernanceRemediationVerdictBundleID":   baselineEpochContractGovernanceRemediationVerdictBundleID,
		"epochContractGovernanceRemediationVerdictBundleDriftStatus":  epochContractGovernanceRemediationVerdictBundleDriftStatus,
		"epochContractGovernanceLineageVersion":                       epochContractGovernanceLineageVersion,
		"epochContractGovernanceLineageComponents":                    epochContractGovernanceLineageComponents,
		"epochContractGovernanceLineageHash":                          epochContractGovernanceLineageHash,
		"baselineEpochContractGovernanceLineageHash":                  baselineEpochContractGovernanceLineageHash,
		"epochContractGovernanceLineageDriftStatus":                   epochContractGovernanceLineageDriftStatus,
		"epochContractGovernanceLineageDriftSummary":                  epochContractGovernanceLineageDriftSummary,
		"epochContractGovernanceLineageDriftRule":                     epochContractGovernanceLineageDriftRule,
		"epochContractGovernanceLineageDriftReason":                   epochContractGovernanceLineageDriftReason,
		"epochContractGovernanceLineageDriftFields":                   epochContractGovernanceLineageDriftFields,
		"epochContractGovernanceLineageDriftFingerprint":              epochContractGovernanceLineageDriftFingerprint,
		"epochContractGovernanceLineageCheckEvaluationOrder":          epochContractGovernanceLineageCheckEvaluationOrder,
		"epochContractGovernanceLineageChecks":                        epochContractGovernanceLineageChecks,
		"epochContractGovernanceLineageFailedChecks":                  epochContractGovernanceLineageFailedChecks,
		"epochContractGovernanceLineageFailedReasons":                 epochContractGovernanceLineageFailedReasons,
		"epochContractGovernanceLineageSummary":                       epochContractGovernanceLineageSummary,
		"epochContractGovernanceLineageFingerprint":                   epochContractGovernanceLineageFingerprint,
		"epochContractGovernanceLineageVerdictEvaluationOrder":        epochContractGovernanceLineageVerdictEvaluationOrder,
		"epochContractGovernanceLineageVerdictChecks":                 epochContractGovernanceLineageVerdictChecks,
		"epochContractGovernanceLineageVerdictFailedChecks":           epochContractGovernanceLineageVerdictFailedChecks,
		"epochContractGovernanceLineageVerdictFailedReasons":          epochContractGovernanceLineageVerdictFailedReasons,
		"epochContractGovernanceLineageVerdictState":                  epochContractGovernanceLineageVerdictState,
		"epochContractGovernanceLineageVerdictRule":                   epochContractGovernanceLineageVerdictRule,
		"epochContractGovernanceLineageVerdictReason":                 epochContractGovernanceLineageVerdictReason,
		"epochContractGovernanceLineageVerdictFingerprint":            epochContractGovernanceLineageVerdictFingerprint,
		"epochContractGovernanceLineageVerdictSeverityByCheck":        epochContractGovernanceLineageVerdictSeverityByCheck,
		"epochContractGovernanceLineageVerdictRequirementByCheck":     epochContractGovernanceLineageVerdictRequirementByCheck,
		"epochContractGovernanceLineageVerdictSummary":                epochContractGovernanceLineageVerdictSummary,
		"epochContractGovernanceLineageVerdictUrgency":                epochContractGovernanceLineageVerdictUrgency,
		"epochContractGovernanceLineageVerdictBundleID":               epochContractGovernanceLineageVerdictBundleID,
		"baselineEpochContractGovernanceLineageVerdictBundleID":       baselineEpochContractGovernanceLineageVerdictBundleID,
		"epochContractGovernanceLineageVerdictBundleDriftStatus":      epochContractGovernanceLineageVerdictBundleDriftStatus,
		"baselineEpochContractGovernanceLineageVerdictFingerprint":    baselineEpochContractGovernanceLineageVerdictFingerprint,
		"epochContractGovernanceLineageVerdictFingerprintDriftStatus": epochContractGovernanceLineageVerdictFingerprintDriftStatus,
		"epochContractCompatibilitySummary":                           epochContractCompatibilitySummary,
		"epochContractIncompatibilityReasons":                         epochContractIncompatibilityReasons,
		"epochContractFingerprint":                                    epochContractFingerprint,
		"epochTransitionFingerprint":                                  epochTransitionFingerprint,
		"checkEvaluationOrder":                                        checkEvaluationOrder,
		"checkReasonCodes":                                            checkReasonCodes,
		"impactEvaluationOrder":                                       impactEvaluationOrder,
		"impactByCheck":                                               impactByCheck,
		"checkFingerprints":                                           checkFingerprints,
		"compatibilityFingerprint":                                    compatibilityFingerprint,
		"versionCompatible":                                           versionCompatible,
		"schemaCompatible":                                            schemaCompatible,
		"ruleReasonCatalogCompatible":                                 ruleReasonCatalogCompatible,
		"stageOrderCompatible":                                        stageOrderCompatible,
		"stageCompatibility":                                          stageCompatibility,
		"stageReasonCodes":                                            stageReasonCodes,
		"overallHashPresent":                                          overallHashPresent,
		"fullyCompatible":                                             fullyCompatible,
		"compatibilitySummary":                                        compatibilitySummary,
		"impactClassification":                                        impactClassification,
		"impactRule":                                                  impactRule,
		"impactReason":                                                impactReason,
		"remediationVersion":                                          "v1",
		"remediationByCheck":                                          remediationByCheck,
		"remediationSeverityByCheck":                                  remediationSeverityByCheck,
		"remediationRequirementByCheck":                               remediationRequirementByCheck,
		"remediationPriorityOrder":                                    checkEvaluationOrder,
		"remediationSummary":                                          remediationSummary,
		"remediationUrgency":                                          remediationUrgency,
		"remediationBundleID":                                         remediationBundleID,
		"remediationPlanHash":                                         remediationPlanHash,
		"baselineRemediationBundleID":                                 baselineRemediationBundleID,
		"baselineRemediationPlanHash":                                 baselineRemediationPlanHash,
		"remediationBundleDriftStatus":                                remediationBundleDriftStatus,
		"remediationPlanDriftStatus":                                  remediationPlanDriftStatus,
		"remediationDriftSummary":                                     remediationDriftSummary,
		"remediationDriftRule":                                        remediationDriftRule,
		"remediationDriftReason":                                      remediationDriftReason,
		"remediationDriftFields":                                      remediationDriftFields,
		"remediationDriftFingerprint":                                 remediationDriftFingerprint,
		"remediationActions":                                          remediationActions,
		"remediationActionPlan":                                       remediationActionPlan,
		"failedChecks":                                                failedChecks,
		"failedCheckReasons":                                          failedCheckReasons,
		"incompatibilityReasons":                                      incompatibilityReasons,
	}
}

func explainDiagnosticPostureContractCheckFingerprints(contractComponents map[string]any) map[string]string {
	requiredStages := []string{"primary", "confidence", "recommendation", "rationale", "score", "trend"}
	version, _ := contractComponents["version"].(string)
	schemaHash, _ := contractComponents["decisionTraceSchemaHash"].(string)
	ruleReasonCatalogHash, _ := contractComponents["ruleReasonCatalogHash"].(string)
	stageOrderHash, _ := contractComponents["stageOrderHash"].(string)
	overallHash, _ := contractComponents["overallHash"].(string)

	stageFragments := map[string]string{}
	switch fragments := contractComponents["stageContractFragments"].(type) {
	case map[string]string:
		for stage, hash := range fragments {
			stageFragments[stage] = hash
		}
	case map[string]any:
		for stage, hashAny := range fragments {
			hash, _ := hashAny.(string)
			stageFragments[stage] = hash
		}
	}

	stageFragmentsBuilder := strings.Builder{}
	for _, stage := range requiredStages {
		stageFragmentsBuilder.WriteString("stageContractFragments.")
		stageFragmentsBuilder.WriteString(stage)
		stageFragmentsBuilder.WriteString("=")
		stageFragmentsBuilder.WriteString(stageFragments[stage])
		stageFragmentsBuilder.WriteString(";")
	}

	return map[string]string{
		"version":             explainDiagnosticPostureHashString("version=" + version + ";"),
		"schema":              schemaHash,
		"rule_reason_catalog": ruleReasonCatalogHash,
		"stage_order":         stageOrderHash,
		"stage_fragments":     explainDiagnosticPostureHashString(stageFragmentsBuilder.String()),
		"overall_hash":        overallHash,
	}
}

func explainDiagnosticPostureContractCompatibilityFingerprint(checkFingerprints map[string]string, checkEvaluationOrder []string) string {
	b := strings.Builder{}
	for _, check := range checkEvaluationOrder {
		b.WriteString(check)
		b.WriteString("=")
		b.WriteString(checkFingerprints[check])
		b.WriteString(";")
	}
	return explainDiagnosticPostureHashString(b.String())
}

func explainDiagnosticPostureRemediationBundleID(impactClassification string, remediationSummary string, remediationUrgency string) string {
	b := strings.Builder{}
	b.WriteString("compat-remediation-v1")
	b.WriteString("-")
	b.WriteString(strings.ReplaceAll(strings.TrimSpace(impactClassification), "_", "-"))
	b.WriteString("-")
	b.WriteString(strings.ReplaceAll(strings.TrimSpace(remediationSummary), "_", "-"))
	b.WriteString("-")
	b.WriteString(strings.ReplaceAll(strings.TrimSpace(remediationUrgency), "_", "-"))
	return b.String()
}

func explainDiagnosticPostureRemediationPlanHash(remediationActionPlan []map[string]any, remediationSummary string, remediationUrgency string) string {
	b := strings.Builder{}
	b.WriteString("remediationSummary=")
	b.WriteString(remediationSummary)
	b.WriteString(";")
	b.WriteString("remediationUrgency=")
	b.WriteString(remediationUrgency)
	b.WriteString(";")
	for idx, action := range remediationActionPlan {
		check, _ := action["check"].(string)
		reasonCode, _ := action["reasonCode"].(string)
		severity, _ := action["severity"].(string)
		requirement, _ := action["requirement"].(string)
		hint, _ := action["hint"].(string)
		b.WriteString("action.")
		b.WriteString(strconv.Itoa(idx))
		b.WriteString(".check=")
		b.WriteString(check)
		b.WriteString(";")
		b.WriteString("action.")
		b.WriteString(strconv.Itoa(idx))
		b.WriteString(".reasonCode=")
		b.WriteString(reasonCode)
		b.WriteString(";")
		b.WriteString("action.")
		b.WriteString(strconv.Itoa(idx))
		b.WriteString(".severity=")
		b.WriteString(severity)
		b.WriteString(";")
		b.WriteString("action.")
		b.WriteString(strconv.Itoa(idx))
		b.WriteString(".requirement=")
		b.WriteString(requirement)
		b.WriteString(";")
		b.WriteString("action.")
		b.WriteString(strconv.Itoa(idx))
		b.WriteString(".hint=")
		b.WriteString(hint)
		b.WriteString(";")
	}
	return explainDiagnosticPostureHashString(b.String())
}

func explainDiagnosticPostureHashString(s string) string {
	digest := sha256.Sum256([]byte(s))
	return hex.EncodeToString(digest[:])
}

func explainDiagnosticPostureScoreClampRange() map[string]int {
	return map[string]int{
		"min": 0,
		"max": 100,
	}
}

func explainDiagnosticScoreCategoryWeights() map[string]int {
	return map[string]int{
		"planner":  -4,
		"index":    -3,
		"operator": -2,
		"general":  -1,
	}
}

func buildExplainWarningSummary(warnings []map[string]any) map[string]any {
	severityCounts := map[string]int{}
	categoryCounts := map[string]int{}
	highestPriority := 0
	highestPriorityCode := ""
	highestSeverity := ""
	highestCategory := ""
	for idx, warning := range warnings {
		severity, _ := warning["severity"].(string)
		if strings.TrimSpace(severity) != "" {
			severityCounts[severity]++
		}
		category := explainWarningCategory(warning)
		if strings.TrimSpace(category) != "" {
			categoryCounts[category]++
		}
		priority, _ := warning["priority"].(int)
		code, _ := warning["code"].(string)
		if idx == 0 || priority < highestPriority {
			highestPriority = priority
			highestPriorityCode = code
			highestSeverity = severity
			highestCategory = category
		}
	}
	return map[string]any{
		"totalWarnings":       len(warnings),
		"bySeverity":          severityCounts,
		"byCategory":          categoryCounts,
		"highestPriority":     highestPriority,
		"highestPriorityCode": highestPriorityCode,
		"highestSeverity":     highestSeverity,
		"highestCategory":     highestCategory,
	}
}

func explainWarningCategory(warning map[string]any) string {
	code, _ := warning["code"].(string)
	switch strings.TrimSpace(code) {
	case "FULL_SCAN_FALLBACK", "PLAN_ANALYSIS_PARTIAL":
		return "planner"
	case "MISSING_PROPERTY_INDEX", "ESTIMATE_ONLY_INDEX_SIGNAL":
		return "index"
	case "OPERATOR_MIXED_DOMAIN_RISK", "OPERATOR_TYPED_FALLBACK":
		return "operator"
	case "WRITE_QUERY_DRY_RUN", "MISSING_TENANT_CONTEXT":
		return "general"
	default:
		return "general"
	}
}

func explainBuildOperatorAssessment(operatorStats map[string]any) map[string]any {
	typedEligibleFamilies := make([]string, 0, 3)
	fallbackLikelyFamilies := make([]string, 0, 3)
	mixedRiskFamilies := make([]string, 0, 3)
	presentFamilies := 0

	families := []struct {
		name       string
		summaryKey string
	}{
		{name: "distinct", summaryKey: "distinctSummary"},
		{name: "sort", summaryKey: "sortSummary"},
		{name: "group", summaryKey: "groupSummary"},
	}

	for _, family := range families {
		summary, _ := operatorStats[family.summaryKey].(map[string]any)
		status, _ := summary["status"].(string)
		switch status {
		case "typed_eligible":
			presentFamilies++
			typedEligibleFamilies = append(typedEligibleFamilies, family.name)
		case "fallback_likely":
			presentFamilies++
			fallbackLikelyFamilies = append(fallbackLikelyFamilies, family.name)
		case "mixed_domain_risk":
			presentFamilies++
			mixedRiskFamilies = append(mixedRiskFamilies, family.name)
		case "unknown":
			presentFamilies++
		case "not_present":
			// not present
		}
	}

	overallStatus := "not_applicable"
	switch {
	case len(mixedRiskFamilies) > 0:
		overallStatus = "mixed_domain_risk"
	case len(fallbackLikelyFamilies) > 0:
		overallStatus = "fallback_likely"
	case len(typedEligibleFamilies) > 0:
		overallStatus = "typed_friendly"
	case presentFamilies > 0:
		overallStatus = "unknown"
	}
	recommendation := "no_operator_signal"
	focusFamilies := []string{}
	switch overallStatus {
	case "mixed_domain_risk":
		recommendation = "investigate_mixed_operator_shapes"
		focusFamilies = append(focusFamilies, mixedRiskFamilies...)
	case "fallback_likely":
		recommendation = "expect_dynamic_fallback"
		focusFamilies = append(focusFamilies, fallbackLikelyFamilies...)
	case "typed_friendly":
		recommendation = "typed_fast_paths_likely"
		focusFamilies = append(focusFamilies, typedEligibleFamilies...)
	case "unknown":
		recommendation = "review_operator_shapes"
	case "not_applicable":
		recommendation = "no_operator_signal"
	}

	return map[string]any{
		"overallStatus":          overallStatus,
		"recommendation":         recommendation,
		"focusFamilies":          focusFamilies,
		"presentFamilies":        presentFamilies,
		"typedEligibleFamilies":  typedEligibleFamilies,
		"fallbackLikelyFamilies": fallbackLikelyFamilies,
		"mixedRiskFamilies":      mixedRiskFamilies,
	}
}

func explainBuildOperatorAssessmentWarning(planVertexes []map[string]any) map[string]any {
	operatorStats := buildExplainOperatorRuntimeStats(planVertexes)
	assessment := explainBuildOperatorAssessment(operatorStats)
	overallStatus, _ := assessment["overallStatus"].(string)
	recommendation, _ := assessment["recommendation"].(string)
	focusFamilies, _ := assessment["focusFamilies"].([]string)
	if len(focusFamilies) == 0 {
		return nil
	}
	focusText := strings.Join(focusFamilies, ",")
	switch overallStatus {
	case "mixed_domain_risk":
		warning := map[string]any{
			"code":           "OPERATOR_MIXED_DOMAIN_RISK",
			"message":        fmt.Sprintf("Operator shapes mix typed-friendly and fallback-heavy families; focus=%s recommendation=%s", focusText, recommendation),
			"recommendation": recommendation,
			"focusFamilies":  focusFamilies,
		}
		if metadata := explainWarningMetadata("OPERATOR_MIXED_DOMAIN_RISK"); metadata != nil {
			warning["severity"] = metadata["severity"]
			warning["priority"] = metadata["priority"]
		}
		return warning
	case "fallback_likely":
		warning := map[string]any{
			"code":           "OPERATOR_TYPED_FALLBACK",
			"message":        fmt.Sprintf("Operator shapes are likely to use dynamic fallback paths; focus=%s recommendation=%s", focusText, recommendation),
			"recommendation": recommendation,
			"focusFamilies":  focusFamilies,
		}
		if metadata := explainWarningMetadata("OPERATOR_TYPED_FALLBACK"); metadata != nil {
			warning["severity"] = metadata["severity"]
			warning["priority"] = metadata["priority"]
		}
		return warning
	default:
		return nil
	}
}

func buildExplainOperatorRuntimeStats(planVertexes []map[string]any) map[string]any {
	distinctOperators := 0
	sortOperators := 0
	groupOperators := 0
	typedDistinctCandidates := 0
	typedSortCandidates := 0
	typedGroupCandidates := 0
	distinctShapes := map[string]int{}
	sortShapes := map[string]int{}
	groupShapes := map[string]int{}
	distinctBlockedReasons := map[string]int{}
	sortBlockedReasons := map[string]int{}
	groupBlockedReasons := map[string]int{}
	distinctBlockedExamples := map[string][]string{}
	sortBlockedExamples := map[string][]string{}
	groupBlockedExamples := map[string][]string{}

	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		switch op {
		case "DISTINCT":
			distinctOperators++
			if exprs, ok := explainStringListField(vertex, "projection"); ok {
				if shape, scalar, blockedReason, blockedExample := explainOperatorExprOpportunity(exprs, true); scalar {
					typedDistinctCandidates++
					distinctShapes[shape]++
				} else {
					distinctShapes["mixed_or_non_scalar"]++
					if blockedReason != "" {
						distinctBlockedReasons[blockedReason]++
						explainAppendBlockedExample(distinctBlockedExamples, blockedReason, blockedExample)
					}
				}
			}
		case "SORT":
			sortOperators++
			if exprs, ok := explainStringListField(vertex, "ordering"); ok {
				if shape, scalar, blockedReason, blockedExample := explainOperatorExprOpportunity(exprs, false); scalar {
					typedSortCandidates++
					sortShapes[shape]++
				} else {
					sortShapes["mixed_or_non_scalar"]++
					if blockedReason != "" {
						sortBlockedReasons[blockedReason]++
						explainAppendBlockedExample(sortBlockedExamples, blockedReason, blockedExample)
					}
				}
			}
		case "AGGREGATE":
			groupOperators++
			if exprs, ok := explainStringListField(vertex, "groupBy"); ok && len(exprs) > 0 {
				if shape, scalar, blockedReason, blockedExample := explainOperatorExprOpportunity(exprs, false); scalar {
					typedGroupCandidates++
					groupShapes[shape]++
				} else {
					groupShapes["mixed_or_non_scalar"]++
					if blockedReason != "" {
						groupBlockedReasons[blockedReason]++
						explainAppendBlockedExample(groupBlockedExamples, blockedReason, blockedExample)
					}
				}
			}
		}
	}

	return map[string]any{
		"distinctOperators":       distinctOperators,
		"sortOperators":           sortOperators,
		"groupOperators":          groupOperators,
		"typedDistinctCandidates": typedDistinctCandidates,
		"typedSortCandidates":     typedSortCandidates,
		"typedGroupCandidates":    typedGroupCandidates,
		"distinctShapes":          distinctShapes,
		"sortShapes":              sortShapes,
		"groupShapes":             groupShapes,
		"distinctBlockedReasons":  distinctBlockedReasons,
		"sortBlockedReasons":      sortBlockedReasons,
		"groupBlockedReasons":     groupBlockedReasons,
		"distinctBlockedExamples": distinctBlockedExamples,
		"sortBlockedExamples":     sortBlockedExamples,
		"groupBlockedExamples":    groupBlockedExamples,
		"distinctSummary":         explainBuildOperatorFamilySummary(distinctOperators, typedDistinctCandidates, distinctShapes, distinctBlockedReasons),
		"sortSummary":             explainBuildOperatorFamilySummary(sortOperators, typedSortCandidates, sortShapes, sortBlockedReasons),
		"groupSummary":            explainBuildOperatorFamilySummary(groupOperators, typedGroupCandidates, groupShapes, groupBlockedReasons),
	}
}

func explainIntCountMapAsAny(counts map[string]int) map[string]any {
	if len(counts) == 0 {
		return map[string]any{}
	}
	converted := make(map[string]any, len(counts))
	for key, value := range counts {
		converted[key] = value
	}
	return converted
}

func explainBuildOperatorFamilySummary(totalOperators, typedCandidates int, shapes map[string]int, blockedReasons map[string]int) map[string]any {
	fallbackCandidates := 0
	if shapes != nil {
		fallbackCandidates = shapes["mixed_or_non_scalar"]
	}
	blockedReasonKinds := 0
	for _, count := range blockedReasons {
		if count > 0 {
			blockedReasonKinds++
		}
	}
	status := "not_present"
	switch {
	case totalOperators == 0:
		status = "not_present"
	case typedCandidates > 0 && fallbackCandidates == 0:
		status = "typed_eligible"
	case typedCandidates > 0 && fallbackCandidates > 0:
		status = "mixed_domain_risk"
	case typedCandidates == 0 && fallbackCandidates > 0:
		status = "fallback_likely"
	default:
		status = "unknown"
	}
	return map[string]any{
		"status":             status,
		"operators":          totalOperators,
		"typedCandidates":    typedCandidates,
		"fallbackCandidates": fallbackCandidates,
		"blockedReasonKinds": blockedReasonKinds,
	}
}

func explainStringListField(node map[string]any, key string) ([]string, bool) {
	if node == nil {
		return nil, false
	}
	raw, ok := node[key]
	if !ok || raw == nil {
		return nil, false
	}
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...), true
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text == "" {
				continue
			}
			out = append(out, text)
		}
		return out, len(out) > 0
	default:
		return nil, false
	}
}

func explainOperatorExprOpportunity(exprs []string, stripAlias bool) (string, bool, string, string) {
	if len(exprs) == 0 {
		return "", false, "", ""
	}
	shape := ""
	hasScalar := false
	nonScalarReasons := map[string]string{}
	normalizedExprs := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if stripAlias {
			expr = explainStripAlias(expr)
		}
		expr = explainStripOrderingDirection(expr)
		normalizedExprs = append(normalizedExprs, expr)
		current, ok := explainScalarExprShape(expr)
		if !ok {
			reason := explainNonScalarExprReason(expr)
			if _, exists := nonScalarReasons[reason]; !exists {
				nonScalarReasons[reason] = expr
			}
			continue
		}
		hasScalar = true
		if shape == "" {
			shape = current
			continue
		}
		if shape != current {
			shape = "mixed_scalar_shapes"
		}
	}
	if len(nonScalarReasons) == 0 {
		if shape == "" {
			return "", false, "", ""
		}
		return shape, true, "", ""
	}
	if hasScalar {
		return "mixed_or_non_scalar", false, "mixed_scalar_and_non_scalar", strings.Join(normalizedExprs, ", ")
	}
	if len(nonScalarReasons) == 1 {
		for reason, sample := range nonScalarReasons {
			return "mixed_or_non_scalar", false, reason, sample
		}
	}
	return "mixed_or_non_scalar", false, "mixed_non_scalar_shapes", strings.Join(normalizedExprs, ", ")
}

func explainAppendBlockedExample(dst map[string][]string, reason, sample string) {
	reason = strings.TrimSpace(reason)
	sample = strings.TrimSpace(sample)
	if reason == "" || sample == "" {
		return
	}
	existing := dst[reason]
	for _, current := range existing {
		if current == sample {
			return
		}
	}
	if len(existing) >= 3 {
		return
	}
	dst[reason] = append(existing, sample)
}

func explainStripAlias(expr string) string {
	upper := strings.ToUpper(expr)
	if idx := strings.LastIndex(upper, " AS "); idx >= 0 {
		return strings.TrimSpace(expr[:idx])
	}
	return strings.TrimSpace(expr)
}

func explainStripOrderingDirection(expr string) string {
	trimmed := strings.TrimSpace(expr)
	upper := strings.ToUpper(trimmed)
	if strings.HasSuffix(upper, " ASC") {
		return strings.TrimSpace(trimmed[:len(trimmed)-4])
	}
	if strings.HasSuffix(upper, " DESC") {
		return strings.TrimSpace(trimmed[:len(trimmed)-5])
	}
	return trimmed
}

func explainScalarExprShape(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false
	}
	if expr == "null" || expr == "true" || expr == "false" {
		return "literal", true
	}
	if _, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return "literal", true
	}
	if _, err := strconv.ParseFloat(expr, 64); err == nil {
		return "literal", true
	}
	if (strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) || (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) {
		return "literal", true
	}
	parts := strings.Split(expr, ".")
	if len(parts) == 1 {
		if explainIsIdentifierToken(parts[0]) {
			return "identifier", true
		}
		return "", false
	}
	for _, part := range parts {
		if !explainIsIdentifierToken(strings.TrimSpace(part)) {
			return "", false
		}
	}
	return "property", true
}

func explainNonScalarExprReason(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "other_non_scalar"
	}
	if strings.HasPrefix(expr, "$") && explainIsIdentifierToken(strings.TrimPrefix(expr, "$")) {
		return "parameter"
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return "list_expression"
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return "map_literal"
	}
	upper := strings.ToUpper(expr)
	if strings.Contains(expr, "(") && strings.HasSuffix(expr, ")") {
		return "function_or_call"
	}
	if strings.Contains(upper, " IS ") || strings.Contains(upper, " IN ") || strings.Contains(upper, " AND ") || strings.Contains(upper, " OR ") || strings.Contains(upper, " NOT ") || strings.ContainsAny(expr, "=<>!") {
		return "predicate_expression"
	}
	if strings.ContainsAny(expr, "+-*/%") {
		return "arithmetic_expression"
	}
	return "other_non_scalar"
}

func explainIsIdentifierToken(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for i, r := range raw {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

func buildExplainWarnings(stmt ast.Statement, analysis *explainAnalysis, planVertexes []map[string]any, tenant string) []map[string]any {
	warnings := make([]map[string]any, 0, len(analysis.warnings)+5)
	seen := map[string]struct{}{}
	addWarning := func(w map[string]any) {
		code, _ := w["code"].(string)
		if metadata := explainWarningMetadata(code); metadata != nil {
			if _, exists := w["severity"]; !exists {
				w["severity"] = metadata["severity"]
			}
			if _, exists := w["priority"]; !exists {
				w["priority"] = metadata["priority"]
			}
		}
		vertexID, _ := w["vertexId"].(string)
		key := code + "|" + vertexID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		assessment := map[string]any{}
		for k, v := range w {
			assessment[k] = v
		}
		w["assessment"] = assessment
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

	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		vertexID, _ := vertex["id"].(string)
		if op == "ALL_VERTEXES_SCAN" || op == "OPTIONAL_ALL_VERTEXES_SCAN" {
			if explainAllNodesScanBackedByFastVertexCount(planVertexes, vertexID) {
				continue
			}
			addWarning(map[string]any{
				"code":     "FULL_SCAN_FALLBACK",
				"message":  "Planner selected an all-vertices scan access path",
				"vertexId": vertexID,
			})
		}
	}

	for _, decision := range analysis.indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			continue
		}
		vertexID, _ := decision["vertexId"].(string)
		schema, _ := decision["schema"].(string)
		property, _ := decision["property"].(string)
		recommendation, _ := decision["recommendation"].(string)
		if recommendation == "" {
			recommendation = "consider-index"
		}
		addWarning(map[string]any{
			"code":     "MISSING_PROPERTY_INDEX",
			"message":  fmt.Sprintf("No property index selected for %s.%s; recommendation=%s", schema, property, recommendation),
			"vertexId": vertexID,
		})
		if quality, _ := decision["quality"].(string); quality == "estimate" {
			addWarning(map[string]any{
				"code":     "ESTIMATE_ONLY_INDEX_SIGNAL",
				"message":  fmt.Sprintf("Index signal for %s.%s is estimate quality; bind parameters for exact selectivity", schema, property),
				"vertexId": vertexID,
			})
		}
	}

	if operatorWarning := explainBuildOperatorAssessmentWarning(planVertexes); operatorWarning != nil {
		addWarning(operatorWarning)
	}

	if len(warnings) == 0 && explainPlanHasUnsupportedShape(planVertexes) && len(analysis.fastPaths) == 0 {
		addWarning(map[string]any{
			"code":    "PLAN_ANALYSIS_PARTIAL",
			"message": "Plan analysis is partial for unsupported query shapes",
		})
	}
	explainSortWarnings(warnings)
	return warnings
}

func explainSortWarnings(warnings []map[string]any) {
	sort.SliceStable(warnings, func(i, j int) bool {
		leftPriority, _ := warnings[i]["priority"].(int)
		rightPriority, _ := warnings[j]["priority"].(int)
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		leftCode, _ := warnings[i]["code"].(string)
		rightCode, _ := warnings[j]["code"].(string)
		if leftCode != rightCode {
			return leftCode < rightCode
		}
		leftVertexID, _ := warnings[i]["vertexId"].(string)
		rightVertexID, _ := warnings[j]["vertexId"].(string)
		return leftVertexID < rightVertexID
	})
}

func explainWarningMetadata(code string) map[string]any {
	switch strings.TrimSpace(code) {
	case "FULL_SCAN_FALLBACK":
		return map[string]any{"severity": "high", "priority": 10}
	case "MISSING_PROPERTY_INDEX":
		return map[string]any{"severity": "medium", "priority": 20}
	case "OPERATOR_MIXED_DOMAIN_RISK":
		return map[string]any{"severity": "medium", "priority": 30}
	case "OPERATOR_TYPED_FALLBACK":
		return map[string]any{"severity": "low", "priority": 40}
	case "ESTIMATE_ONLY_INDEX_SIGNAL":
		return map[string]any{"severity": "low", "priority": 50}
	case "WRITE_QUERY_DRY_RUN", "MISSING_TENANT_CONTEXT", "PLAN_ANALYSIS_PARTIAL":
		return map[string]any{"severity": "info", "priority": 60}
	default:
		return nil
	}
}

func collectExplainFastPaths(stmt ast.Statement, params Params, feedbackFn func(string) (map[string]any, bool)) []map[string]any {
	signals := make([]map[string]any, 0)
	seen := map[string]struct{}{}
	addSignal := func(signal map[string]any) {
		if signal == nil {
			return
		}
		implementation, _ := signal["implementation"].(string)
		if strings.TrimSpace(implementation) == "" {
			return
		}
		if _, ok := seen[implementation]; ok {
			return
		}
		seen[implementation] = struct{}{}
		signals = append(signals, signal)
	}

	collectFromClauses := func(clauses []ast.Clause) {
		for idx := 0; idx < len(clauses); idx++ {
			if idx+2 < len(clauses) {
				if signal, ok := explainFastPathClauseTripletSignal(clauses[idx], clauses[idx+1], clauses[idx+2], params, feedbackFn); ok {
					addSignal(signal)
					idx += 2
					continue
				}
			}
			if idx+1 < len(clauses) {
				signal, ok := explainFastPathClausePairSignal(clauses[idx], clauses[idx+1], params, feedbackFn)
				if ok {
					addSignal(signal)
				}
			}
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return collectExplainFastPaths(s.Statement, params, feedbackFn)
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			collectFromClauses(part.Clauses)
		}
	case *ast.MatchQueryStatement:
		if len(s.MatchClauses) == 0 {
			return signals
		}
		matchPattern := strings.TrimSpace(s.MatchClauses[0].Pattern)
		matchRaw := "MATCH " + matchPattern
		if s.MatchClauses[0].Optional {
			matchRaw = "OPTIONAL MATCH " + matchPattern
		}
		collectFromClauses([]ast.Clause{
			{Kind: ast.ClauseKindMatch, Raw: matchRaw, MatchPattern: matchPattern, MatchOptional: s.MatchClauses[0].Optional, Where: s.MatchClauses[0].Where},
			{Kind: ast.ClauseKindReturn, Projection: &s.Return},
		})
	}

	return signals
}

func explainFastPathClauseTripletSignal(matchClause, withClause, topKWithClause ast.Clause, params Params, feedbackFn func(string) (map[string]any, bool)) (map[string]any, bool) {
	if matchClause.Kind != ast.ClauseKindMatch || withClause.Kind != ast.ClauseKindWith || topKWithClause.Kind != ast.ClauseKindWith {
		return nil, false
	}

	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil || matchSpec.Optional {
		return nil, false
	}
	chain, err := parseTwoHopDirectedChainPattern(matchSpec.Pattern)
	if err != nil || chain.SecondForward {
		return nil, false
	}

	withSpec, err := projectionClauseSpecFromClause(withClause)
	if err != nil || withSpec.Distinct || len(withSpec.OrderBy) != 0 || strings.TrimSpace(withSpec.SkipRaw) != "" || strings.TrimSpace(withSpec.LimitRaw) != "" {
		return nil, false
	}
	items, err := parseProjectionItems(withSpec.ProjectionRaw)
	if err != nil {
		return nil, false
	}
	projection, ok := parseFastTargetSharedPeerProjection(items, chain)
	if !ok {
		return nil, false
	}

	_, topKSpec, ok, err := parseFastTargetSharedPeerTopKWithClause(topKWithClause, projection, params)
	if err != nil || !ok {
		return nil, false
	}
	adaptiveTopKDisabled := false
	if feedbackFn != nil {
		if feedback, ok := feedbackFn(stage1TopKPushdownImplementation); ok {
			adaptiveTopKDisabled = stage1TopKPushdownShouldDisableFromFeedback(feedback)
		}
	}
	status := "eligible"
	if adaptiveTopKDisabled {
		status = "adaptive-disabled"
	}

	whereShortcut := buildStage1WhereShortcutPlan(matchSpec.Where, chain)
	signal := map[string]any{
		"name":                   "target_shared_peer_aggregation",
		"implementation":         stage1TopKPushdownImplementation,
		"clausePair":             "MATCH+WITH+WITH",
		"status":                 status,
		"whereShortcutCoverage":  whereShortcut.enabled,
		"peerInequalityShortcut": whereShortcut.requirePeerNotTarget,
		"topKPushdown":           true,
		"topKLimit":              topKSpec.limit,
		"topKSkip":               topKSpec.skip,
		"adaptiveTopKDisabled":   adaptiveTopKDisabled,
		"assessment": map[string]any{
			"name":                   "target_shared_peer_aggregation",
			"implementation":         stage1TopKPushdownImplementation,
			"clausePair":             "MATCH+WITH+WITH",
			"status":                 status,
			"whereShortcutCoverage":  whereShortcut.enabled,
			"peerInequalityShortcut": whereShortcut.requirePeerNotTarget,
			"topKPushdown":           true,
			"topKLimit":              topKSpec.limit,
			"topKSkip":               topKSpec.skip,
			"adaptiveTopKDisabled":   adaptiveTopKDisabled,
		},
	}
	if feedbackFn != nil {
		if feedback, ok := feedbackFn(stage1TopKPushdownImplementation); ok {
			normalizedFeedback := map[string]any{}
			for key, value := range feedback {
				signal[key] = value
				normalizedFeedback[key] = value
			}
			signal["feedback"] = normalizedFeedback
			signal["feedbackObserved"] = true
		}
	}
	return signal, true
}

func explainDirectedWhereHasRightExclusionPattern(whereRaw, rightVar string) bool {
	rightVar = strings.TrimSpace(rightVar)
	if rightVar == "" {
		return false
	}
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return false
	}
	for _, conjunct := range conjuncts {
		conjunct = strings.TrimSpace(conjunct)
		if !hasLogicalNotPrefix(conjunct) {
			continue
		}
		operand := strings.TrimSpace(conjunct[3:])
		pattern, err := parseDirectedRelationshipPattern(operand)
		if err != nil {
			continue
		}
		if strings.TrimSpace(pattern.EdgeVar) != "" || strings.TrimSpace(pattern.EdgeProps) != "" || len(pattern.EdgeAnyOf) != 0 {
			continue
		}
		if strings.TrimSpace(pattern.Right.Var) != rightVar {
			continue
		}
		return true
	}
	return false
}

func explainPlanHasUnsupportedShape(planVertexes []map[string]any) bool {
	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		if op == "MATCH" || op == "OPTIONAL_MATCH" {
			return true
		}
	}
	return false
}

func explainAllNodesScanBackedByFastVertexCount(planVertexes []map[string]any, vertexID string) bool {
	if strings.TrimSpace(vertexID) == "" {
		return false
	}
	for _, vertex := range planVertexes {
		op, _ := vertex["op"].(string)
		if op != "AGGREGATE" {
			continue
		}
		impl, _ := vertex["implementation"].(string)
		if impl != "fast_vertex_count" {
			continue
		}
		children, _ := vertex["children"].([]string)
		for _, child := range children {
			if child == vertexID {
				return true
			}
		}
	}
	return false
}

func explainFastPathClausePairSignal(matchClause, nextClause ast.Clause, params Params, feedbackFn func(string) (map[string]any, bool)) (map[string]any, bool) {
	if matchClause.Kind != ast.ClauseKindMatch {
		return nil, false
	}

	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil || matchSpec.Optional {
		return nil, false
	}

	switch nextClause.Kind {
	case ast.ClauseKindWith:
		chain, err := parseTwoHopDirectedChainPattern(matchSpec.Pattern)
		if err != nil || chain.SecondForward {
			return nil, false
		}
		withSpec, err := projectionClauseSpecFromClause(nextClause)
		if err != nil {
			return nil, false
		}
		if withSpec.Distinct || len(withSpec.OrderBy) != 0 || strings.TrimSpace(withSpec.SkipRaw) != "" || strings.TrimSpace(withSpec.LimitRaw) != "" {
			return nil, false
		}
		items, err := parseProjectionItems(withSpec.ProjectionRaw)
		if err != nil {
			return nil, false
		}
		if _, ok := parseFastTargetSharedPeerProjection(items, chain); !ok {
			return nil, false
		}
		whereShortcut := buildStage1WhereShortcutPlan(matchSpec.Where, chain)
		return map[string]any{
			"name":                   "target_shared_peer_aggregation",
			"implementation":         "fast_target_shared_peer_aggregation_clause_pair",
			"clausePair":             "MATCH+WITH",
			"status":                 "eligible",
			"whereShortcutCoverage":  whereShortcut.enabled,
			"peerInequalityShortcut": whereShortcut.requirePeerNotTarget,
			"assessment": map[string]any{
				"name":                   "target_shared_peer_aggregation",
				"implementation":         "fast_target_shared_peer_aggregation_clause_pair",
				"clausePair":             "MATCH+WITH",
				"status":                 "eligible",
				"whereShortcutCoverage":  whereShortcut.enabled,
				"peerInequalityShortcut": whereShortcut.requirePeerNotTarget,
			},
		}, true

	case ast.ClauseKindReturn:
		pattern, err := parseDirectedRelationshipPattern(matchSpec.Pattern)
		if err != nil {
			return nil, false
		}
		if strings.TrimSpace(pattern.Left.Var) == "" || strings.TrimSpace(pattern.Right.Var) == "" || strings.TrimSpace(pattern.EdgeVar) == "" {
			return nil, false
		}
		retSpec, err := projectionClauseSpecFromClause(nextClause)
		if err != nil || retSpec.Distinct || strings.TrimSpace(retSpec.WhereRaw) != "" {
			return nil, false
		}
		items, err := parseProjectionItems(retSpec.ProjectionRaw)
		if err != nil {
			return nil, false
		}
		projection, ok := parseFastPeerCandidateReturnProjection(items, pattern)
		if !ok {
			return nil, false
		}
		_, topKPushdown, err := fastPeerCandidateTopKSpecFromProjection(retSpec, projection, params)
		if err != nil {
			topKPushdown = false
		}

		_, hasNumericConstraints := extractEdgeWhereNumericConstraints(matchSpec.Where, pattern.EdgeVar, Row{}, params)
		hasRightExclusion := explainDirectedWhereHasRightExclusionPattern(matchSpec.Where, pattern.Right.Var)
		wherePrefilterCoverage := directedWhereCoveredByExtractedPrefilters(matchSpec.Where, pattern.EdgeVar, pattern.Right.Var, Row{}, params, hasNumericConstraints, hasRightExclusion)

		signal := map[string]any{
			"name":                   "peer_candidate_return_aggregation",
			"implementation":         "fast_peer_candidate_return_aggregation_clause_pair",
			"clausePair":             "MATCH+RETURN",
			"status":                 "eligible",
			"numericPrefilter":       hasNumericConstraints,
			"antiJoinPrefilter":      hasRightExclusion,
			"wherePrefilterCoverage": wherePrefilterCoverage,
			"topKPushdown":           topKPushdown,
			"lateMaterialization":    projection.lateMaterializeNonAggregates,
			"assessment": map[string]any{
				"name":                   "peer_candidate_return_aggregation",
				"implementation":         "fast_peer_candidate_return_aggregation_clause_pair",
				"clausePair":             "MATCH+RETURN",
				"status":                 "eligible",
				"numericPrefilter":       hasNumericConstraints,
				"antiJoinPrefilter":      hasRightExclusion,
				"wherePrefilterCoverage": wherePrefilterCoverage,
				"topKPushdown":           topKPushdown,
				"lateMaterialization":    projection.lateMaterializeNonAggregates,
			},
		}
		if feedbackFn != nil {
			if feedback, ok := feedbackFn("fast_peer_candidate_return_aggregation_clause_pair"); ok {
				normalizedFeedback := map[string]any{}
				for key, value := range feedback {
					signal[key] = value
					normalizedFeedback[key] = value
				}
				signal["feedback"] = normalizedFeedback
				signal["feedbackObserved"] = true
			}
		}
		return signal, true
	}

	return nil, false
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
			for idx := 0; idx < len(part.Clauses); idx++ {
				if idx+2 < len(part.Clauses) && b.addFastPathClauseTriplet(part.Clauses[idx], part.Clauses[idx+1], part.Clauses[idx+2]) {
					idx += 2
					continue
				}
				if idx+1 < len(part.Clauses) && b.addFastPathClausePair(part.Clauses[idx], part.Clauses[idx+1]) {
					idx++
					continue
				}
				b.addClause(part.Clauses[idx])
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

func (b *explainPlanBuilder) addFastPathClausePair(matchClause, nextClause ast.Clause) bool {
	if matchClause.Kind != ast.ClauseKindMatch {
		return false
	}

	signal, ok := explainFastPathClausePairSignal(matchClause, nextClause, b.params, nil)
	if !ok {
		return false
	}
	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil {
		return false
	}

	matchStart := len(b.nodes)
	whereExpr := matchClause.Where
	if whereExpr == nil && strings.TrimSpace(matchSpec.Where) != "" {
		whereExpr = &ast.Expression{Raw: strings.TrimSpace(matchSpec.Where)}
	}
	b.addMatchClause(matchSpec.Optional, "MATCH "+matchSpec.Pattern, whereExpr)
	projectionStart := len(b.nodes)
	b.annotateMatchRangeWithFastPath(matchStart, projectionStart, signal)
	b.addClause(nextClause)
	b.annotateProjectionRangeWithFastPath(projectionStart, signal)
	return true
}

func (b *explainPlanBuilder) annotateMatchRangeWithFastPath(start, end int, signal map[string]any) {
	if start < 0 || start >= len(b.nodes) || signal == nil {
		return
	}
	if end > len(b.nodes) {
		end = len(b.nodes)
	}
	clausePair, _ := signal["clausePair"].(string)

	residualFilterIdx := -1
	for idx := start; idx < end; idx++ {
		node := b.nodes[idx]
		op, _ := node["op"].(string)
		if op == "FILTER" {
			residualFilterIdx = idx
		}
	}

	for idx := start; idx < end; idx++ {
		node := b.nodes[idx]
		op, _ := node["op"].(string)

		switch clausePair {
		case "MATCH+RETURN":
			if op == "EDGE_SCAN" || op == "OPTIONAL_EDGE_SCAN" {
				if existing, _ := node["implementation"].(string); strings.TrimSpace(existing) == "" {
					if covered, _ := signal["wherePrefilterCoverage"].(bool); covered {
						node["implementation"] = "prefilter_covered_directed_relationship_scan"
					} else {
						node["implementation"] = "prefiltered_directed_relationship_scan"
					}
				}
				if implementation, _ := signal["implementation"].(string); strings.TrimSpace(implementation) != "" {
					node["executionStrategy"] = implementation
				}
				if name, _ := signal["name"].(string); strings.TrimSpace(name) != "" {
					node["strategyName"] = name
				}
				if status, _ := signal["status"].(string); strings.TrimSpace(status) != "" {
					node["strategyStatus"] = status
				}
				if value, ok := signal["wherePrefilterCoverage"].(bool); ok {
					node["wherePrefilterCoverage"] = value
				}
				if value, ok := signal["numericPrefilter"].(bool); ok {
					node["numericPrefilter"] = value
				}
				if value, ok := signal["antiJoinPrefilter"].(bool); ok {
					node["antiJoinPrefilter"] = value
				}
			}
			if op == "FILTER" && idx == residualFilterIdx {
				if value, ok := signal["wherePrefilterCoverage"].(bool); ok {
					node["wherePrefilterCoverage"] = value
					if value {
						node["implementation"] = "prefilter_covered_filter"
						node["bookkeepingOnly"] = true
					}
				}
			}
		case "MATCH+WITH":
			if op == "FILTER" && idx == residualFilterIdx {
				if value, ok := signal["whereShortcutCoverage"].(bool); ok {
					node["whereShortcutCoverage"] = value
					if value {
						node["implementation"] = "where_shortcut_filter"
						node["bookkeepingOnly"] = true
					}
				}
				if value, ok := signal["peerInequalityShortcut"].(bool); ok {
					node["peerInequalityShortcut"] = value
				}
			}
		case "MATCH+WITH+WITH":
			if op == "PROJECT" || op == "AGGREGATE" || op == "SORT" || op == "PAGINATION" {
				if implementation, _ := signal["implementation"].(string); strings.TrimSpace(implementation) != "" {
					if existing, _ := node["implementation"].(string); strings.TrimSpace(existing) == "" {
						node["implementation"] = implementation
					} else {
						node["executionStrategy"] = implementation
					}
				}
				if name, _ := signal["name"].(string); strings.TrimSpace(name) != "" {
					node["strategyName"] = name
				}
				if status, _ := signal["status"].(string); strings.TrimSpace(status) != "" {
					node["strategyStatus"] = status
				}
				if value, ok := signal["topKPushdown"].(bool); ok {
					node["topKPushdown"] = value
				}
				if value, ok := signal["topKLimit"].(int); ok {
					node["topKLimit"] = value
				}
				if value, ok := signal["topKSkip"].(int); ok {
					node["topKSkip"] = value
				}
				if value, ok := signal["adaptiveTopKDisabled"].(bool); ok {
					node["adaptiveTopKDisabled"] = value
				}
				if value, ok := signal["adaptiveTopKDisabled"].(bool); ok {
					node["adaptiveTopKDisabled"] = value
				}
			}
		}
	}
}

func (b *explainPlanBuilder) addFastPathClauseTriplet(matchClause, withClause, topKWithClause ast.Clause) bool {
	if matchClause.Kind != ast.ClauseKindMatch || withClause.Kind != ast.ClauseKindWith || topKWithClause.Kind != ast.ClauseKindWith {
		return false
	}

	signal, ok := explainFastPathClauseTripletSignal(matchClause, withClause, topKWithClause, b.params, nil)
	if !ok {
		return false
	}
	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil {
		return false
	}

	matchStart := len(b.nodes)
	whereExpr := matchClause.Where
	if whereExpr == nil && strings.TrimSpace(matchSpec.Where) != "" {
		whereExpr = &ast.Expression{Raw: strings.TrimSpace(matchSpec.Where)}
	}
	b.addMatchClause(matchSpec.Optional, "MATCH "+matchSpec.Pattern, whereExpr)
	projectionStart := len(b.nodes)
	b.annotateMatchRangeWithFastPath(matchStart, projectionStart, signal)
	b.addClause(withClause)
	b.addClause(topKWithClause)
	b.annotateProjectionRangeWithFastPath(projectionStart, signal)
	return true
}

func (b *explainPlanBuilder) annotateProjectionRangeWithFastPath(start int, signal map[string]any) {
	if start < 0 || start >= len(b.nodes) || signal == nil {
		return
	}
	for idx := start; idx < len(b.nodes); idx++ {
		node := b.nodes[idx]
		op, _ := node["op"].(string)
		if op != "PROJECT" && op != "AGGREGATE" {
			continue
		}
		if implementation, _ := signal["implementation"].(string); strings.TrimSpace(implementation) != "" {
			if existing, _ := node["implementation"].(string); strings.TrimSpace(existing) == "" {
				node["implementation"] = implementation
			} else {
				node["executionStrategy"] = implementation
			}
		}
		if name, _ := signal["name"].(string); strings.TrimSpace(name) != "" {
			node["strategyName"] = name
		}
		if status, _ := signal["status"].(string); strings.TrimSpace(status) != "" {
			node["strategyStatus"] = status
		}
		if value, ok := signal["wherePrefilterCoverage"].(bool); ok {
			node["wherePrefilterCoverage"] = value
		}
		if value, ok := signal["topKPushdown"].(bool); ok {
			node["topKPushdown"] = value
		}
		if value, ok := signal["lateMaterialization"].(bool); ok {
			node["lateMaterialization"] = value
		}
		return
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

	if pattern, err := parseVertexPattern(patternBody); err == nil {
		b.addNodeScan(optional, pattern, where)
		return
	}
	if anchored, err := parseAnchoredOutPattern(patternBody); err == nil {
		b.addAnchoredScan(optional, anchored, where)
		return
	}
	if relationship, err := parseDirectedRelationshipPattern(patternBody); err == nil {
		b.addDirectedRelationshipScan(optional, relationship, where)
		return
	}
	if relationship, err := parseDirectedVariableLengthRelationshipPattern(patternBody); err == nil {
		b.addDirectedVariableLengthRelationshipScan(optional, relationship, where)
		return
	}
	if relationship, err := parseReverseDirectedRelationshipPattern(patternBody); err == nil {
		b.addReverseDirectedRelationshipScan(optional, relationship, where)
		return
	}
	if edgePattern, ok := explainFastUndirectedEdgePatternFromRaw(patternBody); ok {
		b.addFastUndirectedEdgeScan(optional, edgePattern, where)
		return
	}
	if relationship, err := parseUndirectedRelationshipPattern(patternBody); err == nil {
		b.addUndirectedRelationshipScan(optional, relationship, where)
		return
	}
	if relationship, err := parseUndirectedVariableLengthRelationshipPattern(patternBody); err == nil {
		b.addUndirectedVariableLengthRelationshipScan(optional, relationship, where)
		return
	}
	if relationship, err := parseDirectedVariableLengthThenDirectedVariableLengthPattern(patternBody); err == nil {
		b.addDirectedVariableLengthThenDirectedVariableLengthScan(optional, relationship, where)
		return
	}
	if chain, err := parseMixedRelationshipChainPattern(patternBody); err == nil {
		b.addMixedRelationshipChainScan(optional, chain, where)
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
	leftLabel, leftAny, ok := fastEdgeCountVertexLabelFilter(pattern.Left)
	if !ok {
		return explainFastUndirectedEdgePattern{}, false
	}
	rightLabel, rightAny, ok := fastEdgeCountVertexLabelFilter(pattern.Right)
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

func (b *explainPlanBuilder) addDirectedRelationshipScan(optional bool, pattern directedRelationshipPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "out", pattern.Left, pattern.Right, pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, where, 0, 0)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addDirectedVariableLengthRelationshipScan(optional bool, pattern directedVariableLengthRelationshipPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "out", pattern.Left, pattern.Right, pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, where, pattern.MinHops, pattern.MaxHops)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addReverseDirectedRelationshipScan(optional bool, pattern reverseDirectedRelationshipPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "in", pattern.Left, pattern.Right, pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, where, 0, 0)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addUndirectedRelationshipScan(optional bool, pattern undirectedRelationshipPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "undirected", pattern.Left, pattern.Right, pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, where, 0, 0)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addUndirectedVariableLengthRelationshipScan(optional bool, pattern undirectedVariableLengthRelationshipPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "undirected", pattern.Left, pattern.Right, pattern.EdgeVar, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, where, pattern.MinHops, pattern.MaxHops)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addDirectedVariableLengthThenDirectedVariableLengthScan(optional bool, pattern directedVariableLengthThenDirectedVariableLengthPattern, where *ast.Expression) {
	b.addRelationshipSegmentScan(optional, "out", pattern.Left, pattern.Mid, pattern.FirstEdgeVar, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf, pattern.FirstEdgeProps, where, pattern.FirstMinHops, pattern.FirstMaxHops)
	b.addRelationshipSegmentScan(optional, "out", pattern.Mid, pattern.Right, pattern.SecondEdgeVar, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf, pattern.SecondEdgeProps, where, pattern.SecondMinHops, pattern.SecondMaxHops)
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addMixedRelationshipChainScan(optional bool, pattern mixedRelationshipChainPattern, where *ast.Expression) {
	if len(pattern.Segments) == 0 || len(pattern.Vertexes) < 2 {
		return
	}
	for i, segment := range pattern.Segments {
		if i+1 >= len(pattern.Vertexes) {
			break
		}
		direction := "out"
		switch segment.Direction {
		case "reverse":
			direction = "in"
		case "undirected":
			direction = "undirected"
		}
		minHops := 0
		maxHops := 0
		if segment.IsVariableLength {
			minHops = segment.MinHops
			maxHops = segment.MaxHops
		}
		b.addRelationshipSegmentScan(optional, direction, pattern.Vertexes[i], pattern.Vertexes[i+1], segment.EdgeVar, segment.EdgeType, segment.EdgeAnyOf, segment.EdgeProps, where, minHops, maxHops)
	}
	if where != nil && strings.TrimSpace(where.Raw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(where.Raw)})
	}
}

func (b *explainPlanBuilder) addRelationshipSegmentScan(optional bool, direction string, left vertexPattern, right vertexPattern, edgeVar, edgeType string, edgeAnyOf []string, edgeProps string, where *ast.Expression, minHops, maxHops int) {
	op := "EDGE_SCAN"
	if optional {
		op = "OPTIONAL_EDGE_SCAN"
	}
	accessPath := "edge_adjacency"
	edgeType = strings.TrimSpace(edgeType)
	if edgeType != "" {
		accessPath = fmt.Sprintf("edge_type(%s)", edgeType)
		if b.catalog != nil {
			if prop, _, ok := explainEdgePropertyEquality(edgeProps, b.params); ok && b.catalog.HasEdgePropertyIndex(b.tenant, edgeType, prop) {
				accessPath = fmt.Sprintf("property_index(%s.%s)", edgeType, prop)
			} else if where != nil {
				whereRaw := strings.TrimSpace(where.Raw)
				constraints, ok := extractEdgeWhereNumericConstraints(whereRaw, edgeVar, Row{}, b.params)
				if ok {
					properties := make([]string, 0, len(constraints))
					for property := range constraints {
						properties = append(properties, property)
					}
					sort.Strings(properties)
					for _, property := range properties {
						if b.catalog.HasEdgePropertyIndex(b.tenant, edgeType, property) {
							accessPath = fmt.Sprintf("property_index(%s.%s)", edgeType, property)
							break
						}
					}
				}
			}
		}
	}
	matchDirection := strings.TrimSpace(direction)
	if matchDirection == "" {
		matchDirection = "out"
	}
	attrs := map[string]any{
		"accessPath":     accessPath,
		"matchDirection": matchDirection,
	}
	if leftVar := strings.TrimSpace(left.Var); leftVar != "" {
		attrs["leftVar"] = leftVar
	}
	if rightVar := strings.TrimSpace(right.Var); rightVar != "" {
		attrs["rightVar"] = rightVar
	}
	if matchDirection == "out" {
		if sourceVar := strings.TrimSpace(left.Var); sourceVar != "" {
			attrs["sourceVar"] = sourceVar
		}
		if targetVar := strings.TrimSpace(right.Var); targetVar != "" {
			attrs["targetVar"] = targetVar
		}
	}
	if matchDirection == "in" {
		if sourceVar := strings.TrimSpace(right.Var); sourceVar != "" {
			attrs["sourceVar"] = sourceVar
		}
		if targetVar := strings.TrimSpace(left.Var); targetVar != "" {
			attrs["targetVar"] = targetVar
		}
	}
	if edgeVar := strings.TrimSpace(edgeVar); edgeVar != "" {
		attrs["edgeVar"] = edgeVar
	}
	if edgeType != "" {
		attrs["edgeType"] = edgeType
	}
	if len(edgeAnyOf) > 0 {
		edgeTypes := make([]string, 0, len(edgeAnyOf))
		for _, edgeType := range edgeAnyOf {
			edgeType = strings.TrimSpace(edgeType)
			if edgeType == "" {
				continue
			}
			edgeTypes = append(edgeTypes, edgeType)
		}
		if len(edgeTypes) > 0 {
			attrs["edgeAnyOf"] = edgeTypes
		}
	}
	if minHops > 0 || maxHops != 0 {
		attrs["variableLength"] = true
		attrs["minHops"] = minHops
		attrs["maxHops"] = maxHops
	}
	if len(left.AllOfLabels) > 0 {
		attrs["leftLabelFilter"] = left.AllOfLabels[0]
	}
	if len(right.AllOfLabels) > 0 {
		attrs["rightLabelFilter"] = right.AllOfLabels[0]
	}
	b.add(op, attrs)
	if strings.TrimSpace(left.PropertiesRaw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(left.PropertiesRaw)})
	}
	if strings.TrimSpace(edgeProps) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(edgeProps)})
	}
	if strings.TrimSpace(right.PropertiesRaw) != "" {
		b.add("FILTER", map[string]any{"predicate": strings.TrimSpace(right.PropertiesRaw)})
	}
}

func explainEdgePropertyEquality(raw string, params Params) (string, any, bool) {
	props, err := explainPropertyMap(raw, params)
	if err != nil || len(props) != 1 {
		return "", nil, false
	}
	for key, value := range props {
		if strings.EqualFold(key, "id") {
			return "", nil, false
		}
		return strings.TrimSpace(key), value, true
	}
	return "", nil, false
}

func (b *explainPlanBuilder) addNodeScan(optional bool, pattern vertexPattern, where *ast.Expression) {
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
			op = "ALL_VERTEXES_SCAN"
			indexPath = "all_vertices"
		}
	}
	if optional {
		op = "OPTIONAL_" + op
	}
	attrs := map[string]any{"accessPath": indexPath}
	if vertexVar := strings.TrimSpace(pattern.Var); vertexVar != "" {
		attrs["vertexVar"] = vertexVar
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
	accessPath := "all_vertices"
	if indexed {
		accessPath = fmt.Sprintf("property_index(%s.%s)", schema, prop)
	} else if schema != "" {
		op = "LABEL_SCAN"
		accessPath = fmt.Sprintf("label(%s)", schema)
	} else {
		op = "ALL_VERTEXES_SCAN"
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
	if vertexVar, ok := b.canUseFastVertexCount(); ok {
		if output, ok := explainFastVertexCountProjection(projection, vertexVar); ok {
			attrs := map[string]any{
				"aggregates":     []string{fmt.Sprintf("count(%s)", vertexVar)},
				"projection":     []string{output},
				"implementation": "fast_vertex_count",
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

func explainFastVertexCountProjection(projection *ast.ReturnClause, vertexVar string) (string, bool) {
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
	if countArg != vertexVar {
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
	return op == "ALL_VERTEXES_SCAN"
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

func (b *explainPlanBuilder) canUseFastVertexCount() (string, bool) {
	if len(b.nodes) == 0 {
		return "", false
	}
	last := b.nodes[len(b.nodes)-1]
	op, _ := last["op"].(string)
	if op != "ALL_VERTEXES_SCAN" {
		return "", false
	}
	vertexVar, _ := last["vertexVar"].(string)
	vertexVar = strings.TrimSpace(vertexVar)
	if vertexVar == "" {
		return "", false
	}
	return vertexVar, true
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
	case *ast.ProfileStatement:
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
	case *ast.ProfileStatement:
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

func profileFingerprint(stmt *ast.ProfileStatement) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToUpper(strings.TrimSpace(profiledQueryText(stmt)))))
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

func profiledQueryText(stmt *ast.ProfileStatement) string {
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

func tryParseExplainVertexPattern(raw string) (vertexPattern, bool) {
	pattern, err := parseVertexPattern(raw)
	if err != nil {
		return vertexPattern{}, false
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

func (p anchoredOutPattern) asNodePattern() vertexPattern {
	pattern := vertexPattern{
		Var:           p.SourceVar,
		AllOfLabels:   nil,
		PropertiesRaw: p.SourcePropertiesRaw,
	}
	if p.SourceLabel != "" {
		pattern.AllOfLabels = []string{p.SourceLabel}
	}
	return pattern
}

func explainPatternSchema(pattern vertexPattern) string {
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

func explainPatternExpression(pattern vertexPattern, params Params) (string, bool) {
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

func countMatchingVertices(stats explainStoreStats, pattern vertexPattern, _ Params) int {
	key := explainPatternFingerprint(pattern)
	if key == "" {
		return 0
	}
	return stats.patternMatchCounts[key]
}

func countAnchoredRows(stats explainStoreStats, pattern anchoredOutPattern, params Params) int {
	left := pattern.asNodePattern()
	return countMatchingVertices(stats, left, params)
}

func matchesExplainVertexPattern(vertex *graph.Vertex, pattern vertexPattern, params Params) bool {
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
