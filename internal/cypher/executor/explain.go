package executor

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
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
	labelCounts        map[string]int
	edgeCounts         map[string]int
	patternMatchCounts map[string]int
	vertexTotal        int
	edgeTotal          int
	snapshotFound      bool
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

func (e *Executor) buildExplainPayload(stmt *ast.ExplainStatement, params Params, analysis *explainAnalysis) map[string]any {
	tenant := tenantFromParams(params)
	planVertexes := buildExplainPlanNodes(stmt.Statement, e.indexCatalog, tenant, params)
	rootVertexID := ""
	if len(planVertexes) > 0 {
		rootVertexID = planVertexes[len(planVertexes)-1]["id"].(string)
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
			"rootVertexId": rootVertexID,
			"vertexes":     planVertexes,
		},
		"physicalPlan": map[string]any{
			"rootVertexId": rootVertexID,
			"vertexes":     planVertexes,
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
			"transport": "json",
		},
	}

	return payload
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
		for _, matcher := range matchers {
			if matchesExplainNodePattern(v, matcher.pattern, params) {
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
			if pattern, ok := tryParseExplainNodePattern(match.Pattern); ok {
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
				if pattern, ok := tryParseExplainNodePattern(clause.Raw); ok {
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

type explainIndexCandidate struct {
	EntityClass  string
	VertexID     string
	Schema       string
	Property     string
	MatchedCount int
	Quality      string
}

type explainScanPlanVertex struct {
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
				EntityClass:  "vertex",
				Schema:       strings.TrimSpace(schema),
				Property:     strings.TrimSpace(property),
				MatchedCount: matchedCount,
				Quality:      quality,
			})
		}
	}
	appendEdgePattern := func(edgeType string, props map[string]any, quality string) {
		edgeType = strings.TrimSpace(edgeType)
		if edgeType == "" {
			return
		}
		for property := range props {
			if strings.EqualFold(property, "id") {
				continue
			}
			candidates = append(candidates, explainIndexCandidate{
				EntityClass:  "edge",
				Schema:       edgeType,
				Property:     strings.TrimSpace(property),
				MatchedCount: 0,
				Quality:      quality,
			})
		}
	}
	appendEdgePatternAnyOf := func(edgeTypes []string, props map[string]any, quality string) {
		for _, edgeType := range edgeTypes {
			appendEdgePattern(edgeType, props, quality)
		}
	}
	appendEdgePatternFromRaw := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if pattern, err := parseDirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate")
			}
		}
		if pattern, err := parseReverseDirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate")
			}
		}
		if pattern, err := parseUndirectedRelationshipPattern(raw); err == nil {
			if props, err := explainPropertyMap(pattern.EdgeProps, params); err == nil && len(props) > 0 {
				appendEdgePattern(pattern.EdgeType, props, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, props, "estimate")
			} else if keys := explainPropertyKeys(pattern.EdgeProps); len(keys) > 0 {
				appendEdgePattern(pattern.EdgeType, keys, "estimate")
				appendEdgePatternAnyOf(pattern.EdgeAnyOf, keys, "estimate")
			}
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
			appendEdgePatternFromRaw(match.Pattern)
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
				appendEdgePatternFromRaw(patternRaw)
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
		if pattern, ok := tryParseExplainNodePattern(raw); ok {
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
	appendBinding(vertex["nodeVar"])
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
	for _, decision := range indexDecisions {
		selected, _ := decision["selected"].(bool)
		if selected {
			indexSelected++
		}
	}

	rowsRead := 0
	rowsOutput := 0
	rowsOutByNode := map[string]int{}
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
			rowsOutByNode[vertexID] = rowsOut
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
		rowsOut := rowsOutByNode[vertexID]
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
			"candidates": indexCandidates,
			"selected":   indexSelected,
			"missing":    indexCandidates - indexSelected,
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
	}
}

func buildExplainWarnings(stmt ast.Statement, analysis *explainAnalysis, planVertexes []map[string]any, tenant string) []map[string]any {
	warnings := make([]map[string]any, 0, len(analysis.warnings)+4)
	seen := map[string]struct{}{}
	addWarning := func(w map[string]any) {
		code, _ := w["code"].(string)
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

	if len(warnings) == 0 && explainPlanHasUnsupportedShape(planVertexes) && len(analysis.fastPaths) == 0 {
		addWarning(map[string]any{
			"code":    "PLAN_ANALYSIS_PARTIAL",
			"message": "Plan analysis is partial for unsupported query shapes",
		})
	}
	return warnings
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
	if nodeVar, ok := b.canUseFastVertexCount(); ok {
		if output, ok := explainFastVertexCountProjection(projection, nodeVar); ok {
			attrs := map[string]any{
				"aggregates":     []string{fmt.Sprintf("count(%s)", nodeVar)},
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

func explainFastVertexCountProjection(projection *ast.ReturnClause, nodeVar string) (string, bool) {
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

func tryParseExplainNodePattern(raw string) (vertexPattern, bool) {
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

func matchesExplainNodePattern(vertex *graph.Vertex, pattern vertexPattern, params Params) bool {
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
