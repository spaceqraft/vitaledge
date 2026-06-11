package physical

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/logical"
)

var antiProbeWhereUndirectedRe = regexp.MustCompile(`(?i)NOT\s*\(\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*-\s*\[[^\]]*?(?::([A-Za-z_][A-Za-z0-9_]*))?[^\]]*\]\s*-\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*\)`)
var antiProbeWhereDirectedOutRe = regexp.MustCompile(`(?i)NOT\s*\(\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*-\s*\[[^\]]*?(?::([A-Za-z_][A-Za-z0-9_]*))?[^\]]*\]\s*->\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*\)`)
var antiProbeWhereDirectedInRe = regexp.MustCompile(`(?i)NOT\s*\(\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*<-\s*\[[^\]]*?(?::([A-Za-z_][A-Za-z0-9_]*))?[^\]]*\]\s*-\s*\(([A-Za-z_][A-Za-z0-9_]*)[^)]*\)\s*\)`)
var patternUndirectedRe = regexp.MustCompile(`\)\s*-\s*\[[^\]]*\]\s*-\s*\(`)

// StatsHints carries optional persisted stats for physical rewrite decisions.
type StatsHints struct {
	EdgeTypeCounts     map[string]int
	EdgeSourceCounts   map[string]int
	EdgeAvgOutDegree   map[string]float64
	AntiProbeHitRateBy map[string]float64
	HasEdgeTotal       bool
	EdgeTotal          int
}

// Build constructs a deterministic physical plan from a logical plan.
func Build(plan logical.Plan) Plan {
	return BuildWithStats(plan, StatsHints{})
}

// BuildWithStats constructs a deterministic physical plan with optional
// stats-assisted rewrite decisions.
func BuildWithStats(plan logical.Plan, hints StatsHints) Plan {
	out := Plan{RootNodeID: "", Nodes: []Node{}}
	idMap := map[string]string{}
	opByPhysicalID := map[string]string{}
	nextID := 1

	for idx, ln := range plan.Nodes {
		pid := fmt.Sprintf("p%d", nextID)
		nextID++

		children := make([]string, 0, len(ln.Children))
		for _, child := range ln.Children {
			if mapped, ok := idMap[child]; ok {
				children = append(children, mapped)
			}
		}

		sortSkipExpr := ""
		sortLimitExpr := ""
		if ln.Op == "SORT" {
			sortSkipExpr, sortLimitExpr = sortPaginationExprsFromNextNode(plan.Nodes, idx, ln.ID)
		}

		op, attrs := lowerLogicalNode(ln.Op, ln.Attrs, hints, sortSkipExpr, sortLimitExpr)
		if (op == "PHY_EXPAND_MATCH" || op == "PHY_EXPAND_OPTIONAL") && len(children) > 0 {
			if parentOp, ok := opByPhysicalID[children[0]]; ok {
				if parentOp == "PHY_EXPAND_MATCH" || parentOp == "PHY_EXPAND_OPTIONAL" {
					joinStrategy := expandJoinStrategyFromHints(attrs, hints)
					attrs["joinStrategy"] = joinStrategy
					baseVariant, _ := attrs["variant"].(string)
					attrs["variant"] = appendExpandJoinStrategyToVariant(baseVariant, joinStrategy)
				}
			}
		}
		if shouldRewriteToEmpty(op, attrs, hints) {
			op = "PHY_EMPTY"
			if attrs == nil {
				attrs = map[string]any{}
			}
			attrs["pruneReason"] = "where_always_false"
		}
		out.Nodes = append(out.Nodes, Node{ID: pid, Op: op, Children: children, Attrs: attrs})
		mappedID := pid

		if op == "PHY_EXPAND_MATCH" {
			for _, antiAttrs := range antiProbeAttrsFromWhere(attrs, hints) {
				antiID := fmt.Sprintf("p%d", nextID)
				nextID++
				out.Nodes = append(out.Nodes, Node{ID: antiID, Op: "PHY_ANTI_PROBE", Children: []string{mappedID}, Attrs: antiAttrs})
				mappedID = antiID
			}
		}

		idMap[ln.ID] = mappedID
		opByPhysicalID[mappedID] = op
	}

	if mappedRoot, ok := idMap[plan.RootNodeID]; ok {
		out.RootNodeID = mappedRoot
	}
	if out.RootNodeID == "" && len(out.Nodes) > 0 {
		out.RootNodeID = out.Nodes[len(out.Nodes)-1].ID
	}

	return out
}

func expandJoinStrategyFromHints(attrs map[string]any, hints StatsHints) string {
	if attrs == nil {
		return "nested_loop_join"
	}
	pattern, _ := attrs["pattern"].(string)
	edgeType := strings.ToUpper(strings.TrimSpace(patternEdgeType(pattern)))
	if edgeType == "" {
		return "nested_loop_join"
	}
	avgOut := edgeAvgOutDegreeFromHints(edgeType, hints)
	if avgOut > 0 && avgOut <= 1.5 {
		return "indexed_bind_join"
	}
	return "nested_loop_join"
}

func edgeAvgOutDegreeFromHints(edgeType string, hints StatsHints) float64 {
	edgeType = strings.ToUpper(strings.TrimSpace(edgeType))
	if edgeType == "" {
		return 0
	}
	if hints.EdgeAvgOutDegree != nil {
		if avgOut := hints.EdgeAvgOutDegree[edgeType]; avgOut > 0 {
			return avgOut
		}
	}
	if hints.EdgeTypeCounts != nil && hints.EdgeSourceCounts != nil {
		typeCount := hints.EdgeTypeCounts[edgeType]
		sourceCount := hints.EdgeSourceCounts[edgeType]
		if typeCount > 0 && sourceCount > 0 {
			return float64(typeCount) / float64(sourceCount)
		}
	}
	return 0
}

func lowerLogicalNode(logicalOp string, logicalAttrs map[string]any, hints StatsHints, sortSkipExpr string, sortLimitExpr string) (string, map[string]any) {
	attrs := map[string]any{}
	for k, v := range logicalAttrs {
		attrs[k] = v
	}

	switch logicalOp {
	case "MATCH":
		pattern, _ := attrs["pattern"].(string)
		accessPath := expandAccessPathForPattern(pattern, hints, false)
		attrs["accessPath"] = accessPath
		attrs["variant"] = expandVariantForAccessPath(accessPath)
		return "PHY_EXPAND_MATCH", attrs
	case "OPTIONAL_MATCH":
		pattern, _ := attrs["pattern"].(string)
		accessPath := expandAccessPathForPattern(pattern, hints, true)
		attrs["accessPath"] = accessPath
		attrs["variant"] = expandVariantForAccessPath(accessPath)
		return "PHY_EXPAND_OPTIONAL", attrs
	case "WRITE":
		attrs["strategy"] = "typed_write_operator"
		return "PHY_WRITE", attrs
	case "CALL":
		attrs["strategy"] = "procedure_call"
		return "PHY_CALL", attrs
	case "PROJECT":
		attrs["strategy"] = "projection_eval"
		return "PHY_PROJECT", attrs
	case "SORT":
		attrs["strategy"] = sortStrategyFromHints(hints, sortLimitExpr)
		attrs["variant"] = sortVariantFromStrategy(attrs["strategy"])
		if attrs["strategy"] == "topk_heap" {
			if topK, ok := effectiveTopKFromPagination(sortSkipExpr, sortLimitExpr); ok {
				attrs["topK"] = topK
			}
		}
		return "PHY_SORT", attrs
	case "PAGINATION":
		attrs["strategy"] = "offset_limit"
		return "PHY_PAGINATION", attrs
	default:
		attrs["strategy"] = "passthrough"
		return "PHY_" + logicalOp, attrs
	}
}

func expandVariantForAccessPath(accessPath string) string {
	accessPath = strings.TrimSpace(strings.ToLower(accessPath))
	switch accessPath {
	case "adjacency_expand_in_first":
		return "expand_in_first"
	case "adjacency_expand_out_first":
		return "expand_out_first"
	case "adjacency_expand_optional_in_first":
		return "optional_expand_in_first"
	case "adjacency_expand_optional_out_first":
		return "optional_expand_out_first"
	case "adjacency_expand_optional":
		return "optional_expand_default"
	default:
		return "expand_default"
	}
}

func appendExpandJoinStrategyToVariant(baseVariant string, joinStrategy string) string {
	baseVariant = strings.TrimSpace(baseVariant)
	joinStrategy = strings.TrimSpace(strings.ToLower(joinStrategy))
	if baseVariant == "" || joinStrategy == "" {
		return baseVariant
	}
	if strings.Contains(baseVariant, "_indexed") || strings.Contains(baseVariant, "_nested") {
		return baseVariant
	}
	suffix := "nested"
	if joinStrategy == "indexed_bind_join" {
		suffix = "indexed"
	}
	return baseVariant + "_" + suffix
}

func sortPaginationExprsFromNextNode(nodes []logical.Node, sortNodeIdx int, sortNodeID string) (skipExpr string, limitExpr string) {
	if sortNodeIdx < 0 || sortNodeIdx >= len(nodes)-1 {
		return "", ""
	}
	next := nodes[sortNodeIdx+1]
	if next.Op != "PAGINATION" {
		return "", ""
	}
	hasSortParent := false
	for _, child := range next.Children {
		if strings.TrimSpace(child) == strings.TrimSpace(sortNodeID) {
			hasSortParent = true
			break
		}
	}
	if !hasSortParent {
		return "", ""
	}
	skipExpr, _ = next.Attrs["skip"].(string)
	limitExpr, _ = next.Attrs["limit"].(string)
	return strings.TrimSpace(skipExpr), strings.TrimSpace(limitExpr)
}

func sortStrategyFromHints(hints StatsHints, limitExpr string) string {
	limit, ok := parsePositiveIntLiteral(limitExpr)
	if !ok || !hints.HasEdgeTotal || hints.EdgeTotal <= 0 {
		return "in_memory_sort"
	}
	ratio := float64(limit) / float64(hints.EdgeTotal)
	if ratio <= 0.25 {
		return "topk_heap"
	}
	return "in_memory_sort"
}

func sortVariantFromStrategy(strategy any) string {
	s, _ := strategy.(string)
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "topk_heap" {
		return "sort_topk_heap"
	}
	return "sort_full"
}

func effectiveTopKFromPagination(skipExpr string, limitExpr string) (int, bool) {
	limit, ok := parsePositiveIntLiteral(limitExpr)
	if !ok {
		return 0, false
	}
	skip := 0
	if parsedSkip, ok := parseNonNegativeIntLiteral(skipExpr); ok {
		skip = parsedSkip
	}
	return skip + limit, true
}

func parsePositiveIntLiteral(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	for strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && len(raw) >= 2 {
		raw = strings.TrimSpace(raw[1 : len(raw)-1])
	}
	if raw == "" {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

func parseNonNegativeIntLiteral(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	for strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && len(raw) >= 2 {
		raw = strings.TrimSpace(raw[1 : len(raw)-1])
	}
	if raw == "" {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, false
	}
	return value, true
}

func expandAccessPathForPattern(pattern string, hints StatsHints, optional bool) string {
	base := "adjacency_expand"
	if optional {
		base = "adjacency_expand_optional"
	}
	if !patternIsUndirected(pattern) {
		return base
	}
	if !preferInboundFirstForUndirectedPattern(pattern, hints) {
		return base + "_out_first"
	}
	return base + "_in_first"
}

func patternIsUndirected(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	if strings.Contains(pattern, "->") || strings.Contains(pattern, "<-") {
		return false
	}
	return patternUndirectedRe.MatchString(pattern)
}

func preferInboundFirstForUndirectedPattern(pattern string, hints StatsHints) bool {
	edgeType := strings.ToUpper(strings.TrimSpace(patternEdgeType(pattern)))
	if edgeType == "" {
		return false
	}
	avgOut := edgeAvgOutDegreeFromHints(edgeType, hints)
	if avgOut <= 0 {
		return false
	}
	// Higher out-degree implies scanning outbound first tends to fan out more rows;
	// prefer inbound-first for undirected traversal in that case.
	return avgOut > 1.5
}

func shouldRewriteToEmpty(op string, attrs map[string]any, hints StatsHints) bool {
	if op != "PHY_EXPAND_MATCH" {
		return false
	}
	if attrs == nil {
		return false
	}
	where, _ := attrs["where"].(string)
	where = strings.TrimSpace(strings.ToLower(where))
	if where == "" {
		return edgeTypeKnownEmpty(attrs, hints)
	}
	if where == "false" || where == "1=0" || where == "0=1" {
		return true
	}
	return edgeTypeKnownEmpty(attrs, hints)
}

func edgeTypeKnownEmpty(attrs map[string]any, hints StatsHints) bool {
	if attrs == nil {
		return false
	}
	pattern, _ := attrs["pattern"].(string)
	edgeType := patternEdgeType(pattern)
	if edgeType == "" {
		return false
	}
	if hints.HasEdgeTotal && hints.EdgeTotal <= 0 {
		return true
	}
	if len(hints.EdgeTypeCounts) == 0 {
		return false
	}
	count, ok := hints.EdgeTypeCounts[strings.ToUpper(edgeType)]
	return ok && count <= 0
}

func patternEdgeType(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return ""
	}
	if m := antiProbeWhereDirectedOutRe.FindStringSubmatch("NOT(" + pattern + ")"); len(m) == 4 {
		return strings.TrimSpace(m[2])
	}
	if m := antiProbeWhereDirectedInRe.FindStringSubmatch("NOT(" + pattern + ")"); len(m) == 4 {
		return strings.TrimSpace(m[2])
	}
	if m := antiProbeWhereUndirectedRe.FindStringSubmatch("NOT(" + pattern + ")"); len(m) == 4 {
		return strings.TrimSpace(m[2])
	}
	return ""
}

func antiProbeAttrsFromWhere(attrs map[string]any, hints StatsHints) []map[string]any {
	if attrs == nil {
		return nil
	}
	where, _ := attrs["where"].(string)
	where = strings.TrimSpace(where)
	if where == "" {
		return nil
	}
	if whereContainsStandaloneOr(where) {
		return nil
	}

	type antiSpec struct {
		leftVar  string
		rightVar string
		edgeType string
		mode     string
	}

	collect := func(re *regexp.Regexp, mode string, swap bool) []antiSpec {
		matches := re.FindAllStringSubmatch(where, -1)
		if len(matches) == 0 {
			return nil
		}
		out := make([]antiSpec, 0, len(matches))
		for _, m := range matches {
			if len(m) != 4 {
				continue
			}
			leftVar := strings.TrimSpace(m[1])
			edgeType := strings.TrimSpace(m[2])
			rightVar := strings.TrimSpace(m[3])
			if leftVar == "" || rightVar == "" || edgeType == "" {
				continue
			}
			if swap {
				leftVar, rightVar = rightVar, leftVar
			}
			out = append(out, antiSpec{leftVar: leftVar, rightVar: rightVar, edgeType: edgeType, mode: mode})
		}
		return out
	}

	allSpecs := make([]antiSpec, 0)
	allSpecs = append(allSpecs, collect(antiProbeWhereUndirectedRe, "undirected", false)...)
	allSpecs = append(allSpecs, collect(antiProbeWhereDirectedOutRe, "directed", false)...)
	allSpecs = append(allSpecs, collect(antiProbeWhereDirectedInRe, "directed", true)...)
	sort.SliceStable(allSpecs, func(i, j int) bool {
		left := antiProbeSelectivityScore(allSpecs[i].edgeType, hints)
		right := antiProbeSelectivityScore(allSpecs[j].edgeType, hints)
		if left == right {
			if allSpecs[i].edgeType == allSpecs[j].edgeType {
				if allSpecs[i].leftVar == allSpecs[j].leftVar {
					return allSpecs[i].rightVar < allSpecs[j].rightVar
				}
				return allSpecs[i].leftVar < allSpecs[j].leftVar
			}
			return allSpecs[i].edgeType < allSpecs[j].edgeType
		}
		// Higher predicted hit-rate first prunes rows earlier.
		return left > right
	})

	seen := map[string]struct{}{}
	out := make([]map[string]any, 0, len(allSpecs))
	for _, spec := range allSpecs {
		key := spec.mode + "|" + spec.leftVar + "|" + spec.edgeType + "|" + spec.rightVar
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, map[string]any{
			"leftVar":  spec.leftVar,
			"rightVar": spec.rightVar,
			"edgeType": spec.edgeType,
			"mode":     spec.mode,
			"variant":  antiProbeVariantFromHints(spec.edgeType, hints),
			"source":   "where_not_relationship",
		})
	}
	return out
}

func antiProbeVariantFromHints(edgeType string, hints StatsHints) string {
	score := antiProbeSelectivityScore(edgeType, hints)
	if score >= 0.5 {
		return "anti_probe_batch_high"
	}
	return "anti_probe_row_low"
}

func antiProbeSelectivityScore(edgeType string, hints StatsHints) float64 {
	edgeType = strings.ToUpper(strings.TrimSpace(edgeType))
	if edgeType == "" {
		return 0
	}
	if hints.EdgeSourceCounts != nil {
		if sourceCount, ok := hints.EdgeSourceCounts[edgeType]; ok && sourceCount <= 0 {
			return 0
		}
	}
	if hints.AntiProbeHitRateBy != nil {
		if value, ok := hints.AntiProbeHitRateBy[edgeType]; ok {
			return value
		}
	}
	if avgOutDegree := edgeAvgOutDegreeFromHints(edgeType, hints); avgOutDegree > 0 {
		// Degree-derived fallback score in [0,1] when global hit-rate is unavailable.
		if avgOutDegree >= 4 {
			return 1
		}
		return avgOutDegree / 4
	}
	// Fallback heuristic when no stats hints are wired yet.
	switch edgeType {
	case "BLOCKED", "MUTED", "REPORTED", "HIDDEN":
		return 0.90
	case "FOLLOWED", "LIKED", "VIEWED":
		return 0.35
	case "KNOWS", "FRIEND", "CONNECTED":
		return 0.20
	default:
		return 0.50
	}
}

func whereContainsStandaloneOr(where string) bool {
	where = strings.TrimSpace(where)
	if where == "" {
		return false
	}
	upper := strings.ToUpper(where)
	inSingle := false
	inDouble := false
	for i := 0; i < len(upper); i++ {
		ch := upper[i]
		if ch == '\'' && !inDouble {
			if i > 0 && upper[i-1] == '\\' {
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && upper[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		if i+1 < len(upper) && upper[i] == 'O' && upper[i+1] == 'R' {
			leftOK := i == 0 || !isIdentifierChar(rune(upper[i-1]))
			rightOK := i+2 >= len(upper) || !isIdentifierChar(rune(upper[i+2]))
			if leftOK && rightOK {
				return true
			}
		}
	}
	return false
}

func isIdentifierChar(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}
