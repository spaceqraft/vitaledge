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
	warnings         []map[string]any
	source           string
	capturedAt       string
}

type explainStoreStats struct {
	vertices    map[string]*graph.Vertex
	labelCounts map[string]int
	edgeCounts  map[string]int
	vertexTotal int
	edgeTotal   int
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

	analysis.nodeCounts = buildExplainNodeCounts(stats)
	analysis.edgeCounts = buildExplainEdgeCounts(stats)
	analysis.predicateSignals = buildExplainPredicateSignals(stmt, params, tenant, stats)
	analysis.indexDecisions = buildExplainIndexDecisions(stmt, params, e.indexCatalog, tenant, stats)
	analysis.cardinality = buildExplainCardinality(stmt, params, stats)
	analysis.warnings = buildExplainWarnings(stmt, analysis)
	return analysis, nil
}

func (e *Executor) buildExplainPayload(stmt *ast.ExplainStatement, params Params, analysis *explainAnalysis) map[string]any {
	nodes := buildExplainPlanNodes(stmt.Statement)
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
			"options":       buildExplainQueryOptions(stmt.Statement, params),
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
				"source":     analysis.source,
				"capturedAt": analysis.capturedAt,
			},
		},
		"cardinality":    analysis.cardinality,
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

	if err := tx.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		clone := *v
		stats.vertices[v.ID] = &clone
		stats.vertexTotal++
		if len(clone.Labels) == 0 {
			stats.labelCounts["UNLABELED"]++
		}
		for _, label := range clone.Labels {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			stats.labelCounts[label]++
		}
		return tx.ScanOutEdges(ctx, tenant, v.ID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			stats.edgeTotal++
			edgeType := strings.TrimSpace(edge.Type)
			if edgeType == "" {
				edgeType = "UNTYPED"
			}
			stats.edgeCounts[edgeType]++
			return nil
		})
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

func buildExplainIndexDecisions(stmt ast.Statement, params Params, catalog IndexCatalog, tenant string, stats explainStoreStats) []map[string]any {
	decisions := make([]map[string]any, 0)
	addDecision := func(nodeID, schema, property string, value any, matchedCount int) {
		schema = strings.TrimSpace(schema)
		property = strings.TrimSpace(property)
		if schema == "" || property == "" {
			return
		}
		selected := catalog != nil && catalog.HasPropertyIndex(tenant, schema, property)
		candidate := true
		reason := "property-equality"
		if !selected {
			reason = "missing-property-index"
		}
		selectivity := 0.0
		if stats.vertexTotal > 0 {
			selectivity = float64(matchedCount) / float64(stats.vertexTotal)
		}
		decisions = append(decisions, map[string]any{
			"nodeId":               nodeID,
			"schema":               schema,
			"property":             property,
			"candidate":            candidate,
			"selected":             selected,
			"reason":               reason,
			"estimatedSelectivity": selectivity,
		})
		_ = value
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return buildExplainIndexDecisions(s.Statement, params, catalog, tenant, stats)
	case *ast.MatchQueryStatement:
		for idx, match := range s.MatchClauses {
			if pattern, ok := tryParseExplainNodePattern(match.Pattern); ok {
				props, err := explainPatternProperties(pattern.PropertiesRaw, params)
				if err == nil {
					for property, value := range props {
						if strings.EqualFold(property, "id") {
							continue
						}
						addDecision(fmt.Sprintf("N%d", idx+1), explainPatternSchema(pattern), property, value, countMatchingVertices(stats, pattern, params))
					}
				}
			}
			if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				props, err := explainPropertyMap(anchored.SourcePropertiesRaw, params)
				if err == nil {
					for property, value := range props {
						if strings.EqualFold(property, "id") {
							continue
						}
						addDecision(fmt.Sprintf("N%d", idx+1), anchored.SourceLabel, property, value, countAnchoredRows(stats, anchored, params))
					}
				}
			}
		}
	case *ast.QueryStatement:
		for idx, part := range s.Parts {
			for clauseIdx, clause := range part.Clauses {
				nodeID := fmt.Sprintf("N%d", idx+clauseIdx+1)
				if pattern, ok := tryParseExplainNodePattern(clause.Raw); ok {
					props, err := explainPatternProperties(pattern.PropertiesRaw, params)
					if err == nil {
						for property, value := range props {
							if strings.EqualFold(property, "id") {
								continue
							}
							addDecision(nodeID, explainPatternSchema(pattern), property, value, countMatchingVertices(stats, pattern, params))
						}
					}
				}
			}
		}
	}

	return decisions
}

func buildExplainCardinality(stmt ast.Statement, params Params, stats explainStoreStats) []map[string]any {
	entries := make([]map[string]any, 0)
	appendEntry := func(nodeID string, rowsIn, rowsOut int, quality string) {
		if rowsIn < 0 {
			rowsIn = 0
		}
		if rowsOut < 0 {
			rowsOut = 0
		}
		entries = append(entries, map[string]any{
			"nodeId":  nodeID,
			"rowsIn":  rowsIn,
			"rowsOut": rowsOut,
			"quality": quality,
		})
	}

	currentRows := 1
	nodeIndex := 1
	consumeProjection := func(projection *ast.ReturnClause, rowsIn int) int {
		rowsOut := rowsIn
		if projection == nil {
			return rowsOut
		}
		if projection.Skip != nil {
			if skip, ok := explainPaginationValue(projection.Skip.Raw, params); ok {
				rowsOut -= skip
			}
		}
		if rowsOut < 0 {
			rowsOut = 0
		}
		if projection.Limit != nil {
			if limit, ok := explainPaginationValue(projection.Limit.Raw, params); ok && rowsOut > limit {
				rowsOut = limit
			}
		}
		return rowsOut
	}

	advanceFromClause := func(clause ast.Clause, rowsIn int) (int, string) {
		quality := "estimate"
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
			if pattern, ok := tryParseExplainNodePattern(clause.Raw); ok {
				quality = "exact"
				return countMatchingVertices(stats, pattern, params), quality
			}
			if anchored, ok := tryParseExplainAnchoredPattern(clause.Raw); ok {
				quality = "exact"
				return countAnchoredRows(stats, anchored, params), quality
			}
			return rowsIn, quality
		case ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Projection != nil {
				quality = "estimate"
				return consumeProjection(clause.Projection, rowsIn), quality
			}
			return rowsIn, quality
		case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete:
			return rowsIn, quality
		case ast.ClauseKindUnwind, ast.ClauseKindInQueryCall, ast.ClauseKindStandaloneCall:
			return rowsIn, quality
		default:
			return rowsIn, quality
		}
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return buildExplainCardinality(s.Statement, params, stats)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			nodeID := fmt.Sprintf("N%d", nodeIndex)
			rowsOut := currentRows
			quality := "estimate"
			if pattern, ok := tryParseExplainNodePattern(match.Pattern); ok {
				rowsOut = countMatchingVertices(stats, pattern, params)
				quality = "exact"
			} else if anchored, ok := tryParseExplainAnchoredPattern(match.Pattern); ok {
				rowsOut = countAnchoredRows(stats, anchored, params)
				quality = "exact"
			}
			appendEntry(nodeID, currentRows, rowsOut, quality)
			currentRows = rowsOut
			nodeIndex++
		}
		appendEntry(fmt.Sprintf("N%d", nodeIndex), currentRows, consumeProjection(&s.Return, currentRows), "estimate")
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				rowsOut, quality := advanceFromClause(clause, currentRows)
				appendEntry(fmt.Sprintf("N%d", nodeIndex), currentRows, rowsOut, quality)
				currentRows = rowsOut
				nodeIndex++
			}
		}
	case *ast.StandaloneCallStatement:
		appendEntry("N1", currentRows, currentRows, "estimate")
	default:
		appendEntry("N1", currentRows, currentRows, "estimate")
	}

	return entries
}

func buildExplainWarnings(stmt ast.Statement, analysis *explainAnalysis) []map[string]any {
	warnings := make([]map[string]any, 0, len(analysis.warnings)+1)
	warnings = append(warnings, analysis.warnings...)
	if statementMayWrite(stmt) {
		warnings = append(warnings, map[string]any{
			"code":    "WRITE_QUERY_DRY_RUN",
			"message": "Write clauses detected; EXPLAIN did not execute mutations",
		})
	}
	if len(warnings) == 0 {
		warnings = append(warnings, map[string]any{
			"code":    "PLAN_ANALYSIS_PARTIAL",
			"message": "Plan analysis is partial for unsupported query shapes",
		})
	}
	return warnings
}

func buildExplainQueryOptions(stmt ast.Statement, params Params) map[string]any {
	projection := explainQueryProjection(stmt)
	if projection == nil {
		return nil
	}

	options := map[string]any{}
	if projection.Distinct {
		options["distinct"] = true
	}
	if projection.IncludeAll {
		options["includeAll"] = true
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

func explainQueryProjection(stmt ast.Statement) *ast.ReturnClause {
	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return explainQueryProjection(s.Statement)
	case *ast.MatchQueryStatement:
		return &s.Return
	case *ast.QueryStatement:
		for i := len(s.Parts) - 1; i >= 0; i-- {
			part := s.Parts[i]
			for j := len(part.Clauses) - 1; j >= 0; j-- {
				clause := part.Clauses[j]
				if clause.Projection == nil {
					continue
				}
				if clause.Kind == ast.ClauseKindReturn || clause.Kind == ast.ClauseKindWith {
					return clause.Projection
				}
			}
		}
	}
	return nil
}

func buildExplainPlanNodes(stmt ast.Statement) []map[string]any {
	nodes := make([]map[string]any, 0)
	nextID := 1

	appendNode := func(node map[string]any) {
		node["id"] = fmt.Sprintf("N%d", nextID)
		node["children"] = []string{}
		if len(nodes) > 0 {
			node["children"] = []string{nodes[len(nodes)-1]["id"].(string)}
		}
		nodes = append(nodes, node)
		nextID++
	}

	switch s := stmt.(type) {
	case *ast.ExplainStatement:
		return buildExplainPlanNodes(s.Statement)
	case *ast.MatchQueryStatement:
		for _, match := range s.MatchClauses {
			node := map[string]any{"op": "MATCH", "predicate": strings.TrimSpace(match.Pattern)}
			if match.Where != nil && strings.TrimSpace(match.Where.Raw) != "" {
				node["predicate"] = strings.TrimSpace(match.Where.Raw)
			}
			appendNode(node)
		}
		appendNode(planNodeForProjectionClause(ast.Clause{Kind: ast.ClauseKindReturn, Projection: &s.Return}))
	case *ast.QueryStatement:
		for _, part := range s.Parts {
			for _, clause := range part.Clauses {
				appendNode(planNodeForClause(clause))
			}
		}
	case *ast.StandaloneCallStatement:
		appendNode(map[string]any{"op": string(ast.ClauseKindStandaloneCall), "predicate": strings.TrimSpace(s.Call.Raw)})
	default:
		appendNode(map[string]any{"op": string(stmt.Kind())})
	}

	return nodes
}

func planNodeForClause(clause ast.Clause) map[string]any {
	node := map[string]any{"op": string(clause.Kind)}
	if clause.Where != nil && strings.TrimSpace(clause.Where.Raw) != "" {
		node["predicate"] = strings.TrimSpace(clause.Where.Raw)
	}
	if clause.Projection != nil {
		for key, value := range planNodeForProjectionClause(clause) {
			node[key] = value
		}
	} else if raw := strings.TrimSpace(clause.Raw); raw != "" {
		node["predicate"] = raw
	}
	if isWriteClauseKind(clause.Kind) {
		node["writeAction"] = string(clause.Kind)
	}
	return node
}

func planNodeForProjectionClause(clause ast.Clause) map[string]any {
	node := map[string]any{"op": string(clause.Kind)}
	projection := clause.Projection
	if projection == nil {
		return node
	}
	projectionItems := make([]string, 0, len(projection.Items))
	for _, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		if alias := strings.TrimSpace(item.Alias); alias != "" {
			expr = expr + " AS " + alias
		}
		projectionItems = append(projectionItems, expr)
	}
	if len(projectionItems) > 0 {
		node["projection"] = projectionItems
	}
	ordering := make([]string, 0, len(projection.OrderBy))
	for _, item := range projection.OrderBy {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		dir := strings.TrimSpace(string(item.Direction))
		if dir == "" || dir == string(ast.SortDirectionNone) {
			ordering = append(ordering, expr)
			continue
		}
		ordering = append(ordering, expr+" "+dir)
	}
	if len(ordering) > 0 {
		node["ordering"] = ordering
	}
	pagination := map[string]any{}
	if projection.Skip != nil && strings.TrimSpace(projection.Skip.Raw) != "" {
		pagination["skip"] = strings.TrimSpace(projection.Skip.Raw)
	}
	if projection.Limit != nil && strings.TrimSpace(projection.Limit.Raw) != "" {
		pagination["limit"] = strings.TrimSpace(projection.Limit.Raw)
	}
	if len(pagination) > 0 {
		node["pagination"] = pagination
	}
	return node
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

func isWriteClauseKind(kind ast.ClauseKind) bool {
	switch kind {
	case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete:
		return true
	default:
		return false
	}
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
