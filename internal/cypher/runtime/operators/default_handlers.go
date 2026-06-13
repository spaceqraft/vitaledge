package operators

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/spaceqraft/vitaledge/internal/graph"
	"github.com/spaceqraft/vitaledge/internal/graph/store/typedvalue"
)

var edgePatternRe = regexp.MustCompile(`^\s*\(([^)]*)\)\s*[-<]*\s*\[.*?:([A-Za-z_][A-Za-z0-9_]*)[^\]]*\]\s*[->]*\s*\(([^)]*)\)\s*$`)
var edgePatternEndpointsRe = regexp.MustCompile(`^\s*\(([^)]*)\)\s*[-<]*\s*(?:\[[^\]]*\])?\s*[->]*\s*\(([^)]*)\)\s*$`)
var writeSetLabelAssignmentRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[A-Za-z_][A-Za-z0-9_]*)*)$`)
var writeSetPropertyAssignmentRE = regexp.MustCompile(`^\(?\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)?\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
var writeRemovePropertyAssignmentRE = regexp.MustCompile(`^\(?\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)?\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)$`)
var writeSetMapAppendAssignmentRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*\+=\s*(.+)$`)
var writeSetMapReplaceAssignmentRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
var projectWhereSimpleRe = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|<>|<=|>=|<|>)\s*(.+?)\s*$`)
var projectWhereStringRe = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(STARTS\s+WITH|ENDS\s+WITH|CONTAINS)\s*(.+?)\s*$`)
var projectWhereNullRe = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(IS\s+NULL|IS\s+NOT\s+NULL)\s*$`)

type projectWhereAtom struct {
	leftName       string
	op             string
	rightAny       any
	rightParamName string
}

type projectWhereExpr struct {
	atoms      []projectWhereAtom
	useOrLogic bool
	logicToken string
	root       *projectWhereNode
	rawExpr    string
	useRawExpr bool
}

type projectWhereNode struct {
	op    string
	atom  *projectWhereAtom
	left  *projectWhereNode
	right *projectWhereNode
}

type projectionPathValue struct {
	nodes         []any
	relationships []any
}

func (p projectionPathValue) String() string {
	if len(p.nodes) == 0 {
		return "<>"
	}
	var b strings.Builder
	b.WriteString("<")
	b.WriteString(renderProjectionPathNode(p.nodes[0]))
	for i := 0; i < len(p.relationships); i++ {
		edgeText := "[]"
		if i < len(p.relationships) {
			edgeText = renderProjectionPathEdge(p.relationships[i])
		}
		direction := "->"
		if i+1 < len(p.nodes) {
			if dir, ok := renderProjectionPathEdgeDirection(p.relationships[i], p.nodes[i], p.nodes[i+1]); ok {
				direction = dir
			}
		}
		nextNode := "()"
		if i+1 < len(p.nodes) {
			nextNode = renderProjectionPathNode(p.nodes[i+1])
		}
		if direction == "<-" {
			b.WriteString("<-")
			b.WriteString(edgeText)
			b.WriteString("-")
		} else {
			b.WriteString("-")
			b.WriteString(edgeText)
			b.WriteString("->")
		}
		b.WriteString(nextNode)
	}
	b.WriteString(">")
	return b.String()
}

func renderProjectionPathEdgeDirection(edge any, leftNode any, rightNode any) (string, bool) {
	leftID, leftOK := projectionPathNodeID(leftNode)
	rightID, rightOK := projectionPathNodeID(rightNode)
	if !leftOK || !rightOK {
		return "", false
	}
	srcID, dstID, ok := projectionPathEdgeEndpoints(edge)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(srcID) == leftID && strings.TrimSpace(dstID) == rightID {
		return "->", true
	}
	if strings.TrimSpace(srcID) == rightID && strings.TrimSpace(dstID) == leftID {
		return "<-", true
	}
	return "", false
}

func projectionPathNodeID(node any) (string, bool) {
	switch typed := node.(type) {
	case *graph.Vertex:
		if typed == nil {
			return "", false
		}
		id := strings.TrimSpace(typed.ID)
		return id, id != ""
	case map[string]any:
		if rawID, ok := typed["id"]; ok {
			id := strings.TrimSpace(scalarString(rawID))
			return id, id != ""
		}
	}
	return "", false
}

func projectionPathEdgeEndpoints(edge any) (string, string, bool) {
	switch typed := edge.(type) {
	case *graph.Edge:
		if typed == nil {
			return "", "", false
		}
		return strings.TrimSpace(typed.SrcID), strings.TrimSpace(typed.DstID), true
	case map[string]any:
		src := ""
		dst := ""
		if raw, ok := typed["src"]; ok {
			src = strings.TrimSpace(scalarString(raw))
		}
		if raw, ok := typed["dst"]; ok {
			dst = strings.TrimSpace(scalarString(raw))
		}
		if src == "" {
			if raw, ok := typed["srcID"]; ok {
				src = strings.TrimSpace(scalarString(raw))
			}
		}
		if dst == "" {
			if raw, ok := typed["dstID"]; ok {
				dst = strings.TrimSpace(scalarString(raw))
			}
		}
		if src != "" && dst != "" {
			return src, dst, true
		}
	}
	return "", "", false
}

func renderProjectionPathNode(value any) string {
	vertex, ok := value.(*graph.Vertex)
	if !ok || vertex == nil {
		return "()"
	}
	var b strings.Builder
	b.WriteString("(")
	hasLabels := false
	for _, label := range vertex.Labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		hasLabels = true
		b.WriteString(":")
		b.WriteString(label)
	}
	if props := renderProjectionPathProperties(vertex.Properties); props != "" {
		if hasLabels {
			b.WriteString(" ")
		}
		b.WriteString(props)
	}
	b.WriteString(")")
	return b.String()
}

func renderProjectionPathEdge(value any) string {
	edge, ok := value.(*graph.Edge)
	if !ok || edge == nil {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	typ := strings.TrimSpace(edge.Type)
	if typ != "" {
		b.WriteString(":")
		b.WriteString(typ)
	}
	if props := renderProjectionPathProperties(edge.Properties); props != "" {
		if typ != "" {
			b.WriteString(" ")
		}
		b.WriteString(props)
	}
	b.WriteString("]")
	return b.String()
}

func renderProjectionPathProperties(props graph.PropertyMap) string {
	if len(props) == 0 {
		return ""
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+": "+renderProjectionPathLiteral(decodeProjectionPropertyValue(props[key])))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func renderProjectionPathLiteral(value any) string {
	if value == nil {
		return "null"
	}
	switch typed := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "\\'") + "'"
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case int, int32, int64, float32, float64:
		return fmt.Sprint(typed)
	default:
		return fmt.Sprint(typed)
	}
}

type baseHandler struct {
	op string
}

func (h baseHandler) OpName() string { return h.op }

func (h baseHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	_ = nodeID
	_ = attrs
	if state != nil {
		state.ExecutedOps = append(state.ExecutedOps, h.op)
		state.OperatorExecCount++
	}
	return nil
}

type expandHandler struct{ baseHandler }

type expandVariantExecutor func(nodeID string, attrs map[string]any, state *State, optional bool) error

var expandVariantExecutors = map[string]expandVariantExecutor{
	"expand_default":           executeExpandVariantRows,
	"expand_out_first":         executeExpandVariantRows,
	"expand_in_first":          executeExpandVariantRows,
	"expand_default_indexed":   executeExpandVariantRows,
	"expand_default_nested":    executeExpandVariantRows,
	"expand_out_first_indexed": executeExpandVariantRows,
	"expand_out_first_nested":  executeExpandVariantRows,
	"expand_in_first_indexed":  executeExpandVariantRows,
	"expand_in_first_nested":   executeExpandVariantRows,
}

var optionalExpandVariantExecutors = map[string]expandVariantExecutor{
	"optional_expand_default":           executeExpandVariantRows,
	"optional_expand_out_first":         executeExpandVariantRows,
	"optional_expand_in_first":          executeExpandVariantRows,
	"optional_expand_default_indexed":   executeExpandVariantRows,
	"optional_expand_default_nested":    executeExpandVariantRows,
	"optional_expand_out_first_indexed": executeExpandVariantRows,
	"optional_expand_out_first_nested":  executeExpandVariantRows,
	"optional_expand_in_first_indexed":  executeExpandVariantRows,
	"optional_expand_in_first_nested":   executeExpandVariantRows,
}

func SupportsVariant(op string, variant string) bool {
	op = strings.TrimSpace(strings.ToUpper(op))
	variant = strings.TrimSpace(strings.ToLower(variant))
	if variant == "" {
		return false
	}
	if !IsVariantDispatchOp(op) {
		return false
	}
	switch op {
	case "PHY_EXPAND_MATCH":
		_, ok := expandVariantExecutors[variant]
		return ok
	case "PHY_EXPAND_OPTIONAL":
		_, ok := optionalExpandVariantExecutors[variant]
		return ok
	case "PHY_SORT":
		_, ok := sortVariantExecutors[variant]
		return ok
	case "PHY_ANTI_PROBE":
		_, ok := antiProbeVariantExecutors[variant]
		return ok
	default:
		return false
	}
}

func IsVariantDispatchOp(op string) bool {
	op = strings.TrimSpace(strings.ToUpper(op))
	switch op {
	case "PHY_EXPAND_MATCH":
		return true
	case "PHY_EXPAND_OPTIONAL":
		return true
	case "PHY_SORT":
		return true
	case "PHY_ANTI_PROBE":
		return true
	default:
		return false
	}
}

func NewExpandMatchHandler() Handler { return expandHandler{baseHandler{op: "PHY_EXPAND_MATCH"}} }

func (h expandHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *State) (bool, error) {
	executor, ok := expandVariantExecutors[strings.ToLower(strings.TrimSpace(variant))]
	if !ok {
		return false, nil
	}
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return true, err
	}
	return true, executor(nodeID, attrs, state, false)
}

func (h expandHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	return executeExpandVariantRows(nodeID, attrs, state, false)
}

func executeExpandVariantRows(nodeID string, attrs map[string]any, state *State, optional bool) error {
	_ = nodeID
	if state == nil {
		return nil
	}
	wherePredicate := strings.TrimSpace(stringAttr(attrs, "where"))
	if len(state.Rows) == 0 {
		state.Rows = []map[string]any{{}}
	}
	if state.Tx == nil {
		return nil
	}
	pattern, _ := attrs["pattern"].(string)
	pathVar, assignedPattern, hasPathAssignment := splitProjectionPathAssignment(pattern)
	if hasPathAssignment {
		pattern = assignedPattern
	}
	if len(state.Rows) > 0 {
		rows := make([]map[string]any, 0, len(state.Rows))
		for _, row := range state.Rows {
			if row == nil {
				rows = append(rows, map[string]any{})
				continue
			}
			next := cloneRow(row)
			delete(next, "__ve_chain_cursor")
			delete(next, "__edge.id")
			rows = append(rows, next)
		}
		state.Rows = rows
	}
	if minHops, maxHops, ok := parseProjectionVariableLengthBounds(pattern); ok && len(splitProjectionConnectedEdgePatterns(pattern)) <= 1 {
		out := make([]map[string]any, 0, len(state.Rows))
		for _, row := range state.Rows {
			if row == nil {
				row = map[string]any{}
			}
			expanded, err := expandVariableLengthPathRows(state, row, pattern, pathVar, minHops, maxHops, optional)
			if err != nil {
				return err
			}
			out = append(out, expanded...)
		}
		if filtered, err := filterRowsByProjectionPredicate(out, wherePredicate, state); err != nil {
			return err
		} else {
			state.Rows = filtered
		}
		return nil
	}
	if edgePatterns := splitProjectionConnectedEdgePatterns(pattern); len(edgePatterns) > 1 {
		rows := make([]map[string]any, 0, len(state.Rows))
		originalInputRows := make([]map[string]any, 0, len(state.Rows))
		optionalSourceKey := "__ve_optional_source_idx"
		for idx, row := range state.Rows {
			if row == nil {
				row = map[string]any{}
			}
			next := cloneRow(row)
			next[optionalSourceKey] = idx
			rows = append(rows, next)
			originalInputRows = append(originalInputRows, next)
		}
		// For multi-segment optional patterns, don't pass optional to individual segments
		// Only handle optional at the orchestration level
		segmentOptional := false
		if optional {
			segmentOptional = false // Individual segments won't generate null rows
		}
		for _, edgePattern := range edgePatterns {
			out := make([]map[string]any, 0, len(rows))
			for _, row := range rows {
				if row == nil {
					row = map[string]any{}
				}
				var (
					expanded []map[string]any
					err      error
				)
				if minHops, maxHops, isVarLength := parseProjectionVariableLengthBounds(edgePattern); isVarLength {
					expanded, err = expandVariableLengthPathRows(state, row, edgePattern, pathVar, minHops, maxHops, segmentOptional)
				} else {
					edge := parseEdgeMutation(edgePattern)
					if edge == nil {
						rows = nil
						break
					}
					expanded, err = expandPatternRow(state, row, edge, pathVar, attrs, segmentOptional)
				}
				if err != nil {
					return err
				}
				out = append(out, expanded...)
			}
			rows = out
		}
		filtered, err := filterRowsByProjectionPredicate(rows, wherePredicate, state)
		if err != nil {
			return err
		}
		if optional && len(originalInputRows) > 0 {
			matchedSources := map[int]struct{}{}
			for _, row := range filtered {
				if row == nil {
					continue
				}
				switch typed := row[optionalSourceKey].(type) {
				case int:
					matchedSources[typed] = struct{}{}
				case int64:
					matchedSources[int(typed)] = struct{}{}
				}
			}

			allPatternVars := map[string]struct{}{}
			for _, edgePattern := range edgePatterns {
				edge := parseEdgeMutation(edgePattern)
				if edge == nil {
					continue
				}
				if variable := strings.TrimSpace(edge.LeftVar); variable != "" {
					allPatternVars[variable] = struct{}{}
				}
				if variable := strings.TrimSpace(edge.RightVar); variable != "" {
					allPatternVars[variable] = struct{}{}
				}
				if variable := strings.TrimSpace(edge.Var); variable != "" {
					allPatternVars[variable] = struct{}{}
				}
			}

			out := make([]map[string]any, 0, len(filtered)+len(originalInputRows))
			out = append(out, filtered...)
			for idx, origRow := range originalInputRows {
				if _, exists := matchedSources[idx]; exists {
					continue
				}
				next := cloneRow(origRow)
				next["__edge.id"] = nil
				for variable := range allPatternVars {
					if variable == "" {
						continue
					}
					if bound, _ := resolveBoundEntityID(origRow, variable); bound {
						continue
					}
					next[variable] = nil
					next[variable+".id"] = nil
				}
				out = append(out, next)
			}
			filtered = out
		}
		for _, row := range filtered {
			if row != nil {
				delete(row, optionalSourceKey)
			}
		}
		state.Rows = filtered
		return nil
	}
	vertexPatterns := splitProjectionTopLevelByComma(pattern)
	edge := parseEdgeMutation(pattern)
	if edge == nil || len(vertexPatterns) > 1 {
		if len(vertexPatterns) == 0 {
			vertexPatterns = []string{pattern}
		}
		rows := state.Rows
		for _, rawPattern := range vertexPatterns {
			if edgePatterns := splitProjectionConnectedEdgePatterns(rawPattern); len(edgePatterns) > 1 {
				for _, edgePattern := range edgePatterns {
					out := make([]map[string]any, 0, len(rows))
					for _, row := range rows {
						if row == nil {
							row = map[string]any{}
						}
						var (
							expanded []map[string]any
							err      error
						)
						if minHops, maxHops, isVarLength := parseProjectionVariableLengthBounds(edgePattern); isVarLength {
							expanded, err = expandVariableLengthPathRows(state, row, edgePattern, pathVar, minHops, maxHops, optional)
						} else {
							edge := parseEdgeMutation(edgePattern)
							if edge == nil {
								rows = nil
								break
							}
							expanded, err = expandPatternRow(state, row, edge, pathVar, attrs, optional)
						}
						if err != nil {
							return err
						}
						out = append(out, expanded...)
					}
					rows = out
				}
				continue
			}
			if minHops, maxHops, isVarLength := parseProjectionVariableLengthBounds(rawPattern); isVarLength {
				out := make([]map[string]any, 0, len(rows))
				for _, row := range rows {
					if row == nil {
						row = map[string]any{}
					}
					expanded, err := expandVariableLengthPathRows(state, row, rawPattern, pathVar, minHops, maxHops, optional)
					if err != nil {
						return err
					}
					out = append(out, expanded...)
				}
				rows = out
				continue
			}
			if edgePattern := parseEdgeMutation(rawPattern); edgePattern != nil {
				out := make([]map[string]any, 0, len(rows))
				for _, row := range rows {
					if row == nil {
						row = map[string]any{}
					}
					expanded, err := expandPatternRow(state, row, edgePattern, pathVar, attrs, optional)
					if err != nil {
						return err
					}
					out = append(out, expanded...)
				}
				rows = out
				continue
			}
			vertex := parseVertexMutation(rawPattern)
			if vertex == nil {
				continue
			}
			out := make([]map[string]any, 0, len(rows))
			for _, row := range rows {
				if row == nil {
					row = map[string]any{}
				}
				expanded, err := expandVertexPatternRow(state, row, vertex, optional)
				if err != nil {
					return err
				}
				if hasPathAssignment && strings.TrimSpace(pathVar) != "" {
					for _, expandedRow := range expanded {
						bindProjectionVertexPath(expandedRow, pathVar, vertex.Var, state)
					}
				}
				out = append(out, expanded...)
			}
			rows = out
		}
		if filtered, err := filterRowsByProjectionPredicate(rows, wherePredicate, state); err != nil {
			return err
		} else {
			state.Rows = filtered
		}
		return nil
	}
	out := make([]map[string]any, 0, len(state.Rows))
	sourceRows := make([]map[string]any, 0, len(state.Rows))
	sourceKeys := make([]string, 0, len(state.Rows))
	for _, row := range state.Rows {
		if row == nil {
			row = map[string]any{}
		}
		expanded, err := expandPatternRow(state, row, edge, pathVar, attrs, optional)
		if err != nil {
			return err
		}
		sourceKey := projectionDistinctRowKeyObserved(state, row, "runtime.operator.optional_expand.row_key")
		for _, expandedRow := range expanded {
			out = append(out, expandedRow)
			sourceRows = append(sourceRows, row)
			sourceKeys = append(sourceKeys, sourceKey)
		}
	}
	if filtered, err := filterRowsByProjectionPredicate(out, wherePredicate, state); err != nil {
		return err
	} else {
		if optional && len(out) > len(filtered) {
			surviving := map[string]int{}
			for _, row := range filtered {
				surviving[projectionDistinctRowKeyObserved(state, row, "runtime.operator.optional_expand.row_key")]++
			}
			hasSurvivor := map[string]bool{}
			for i, row := range out {
				key := projectionDistinctRowKeyObserved(state, row, "runtime.operator.optional_expand.row_key")
				if count := surviving[key]; count > 0 {
					surviving[key] = count - 1
					hasSurvivor[sourceKeys[i]] = true
				}
			}
			nullRows := make([]map[string]any, 0, len(sourceRows))
			seenSources := map[string]bool{}
			for i, sourceRow := range sourceRows {
				sourceKey := sourceKeys[i]
				if seenSources[sourceKey] || hasSurvivor[sourceKey] {
					continue
				}
				seenSources[sourceKey] = true
				leftVar := strings.TrimSpace(edge.LeftVar)
				rightVar := strings.TrimSpace(edge.RightVar)
				leftBound := leftVar == "" || (sourceRow != nil && sourceRow[leftVar] != nil)
				rightBound := rightVar == "" || (sourceRow != nil && sourceRow[rightVar] != nil)
				nullRows = append(nullRows, optionalNullBoundRow(sourceRow, edge, leftBound, rightBound))
			}
			state.Rows = append(filtered, nullRows...)
		} else {
			state.Rows = filtered
		}
	}
	return nil
}

func filterRowsByProjectionPredicate(rows []map[string]any, predicate string, state *State) ([]map[string]any, error) {
	predicate = strings.TrimSpace(predicate)
	if predicate == "" || len(rows) == 0 {
		return rows, nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			row = map[string]any{}
		}
		value, ok, err := resolveProjectionExprValueChecked(predicate, row, state)
		if err != nil {
			return nil, err
		}
		if !ok || value == nil {
			continue
		}
		matched, boolOK := value.(bool)
		if !boolOK || !matched {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

func splitProjectionPathAssignment(pattern string) (string, string, bool) {
	raw := strings.TrimSpace(pattern)
	if raw == "" {
		return "", "", false
	}
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i == 0 || raw[i-1] != '\\' {
				inSingle = !inSingle
			}
			continue
		}
		if ch == '"' && !inSingle {
			if i == 0 || raw[i-1] != '\\' {
				inDouble = !inDouble
			}
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case '=':
			if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+1:])
			if !isIdentifierToken(left) || right == "" || !strings.HasPrefix(right, "(") {
				return "", "", false
			}
			return left, right, true
		}
	}
	return "", "", false
}

func splitProjectionConnectedEdgePatterns(pattern string) []string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	currentVertex, pos, ok := nextProjectionVertexSegment(pattern, 0)
	if !ok {
		return nil
	}
	vertices := []string{currentVertex}
	connectors := make([]string, 0, 2)
	for {
		nextStart := -1
		bracketDepth := 0
		for i := pos; i < len(pattern); i++ {
			switch pattern[i] {
			case '[':
				bracketDepth++
			case ']':
				if bracketDepth > 0 {
					bracketDepth--
				}
			case '(':
				if bracketDepth == 0 {
					nextStart = i
					i = len(pattern)
				}
			}
		}
		if nextStart < 0 {
			break
		}
		nextVertex, nextPos, ok := nextProjectionVertexSegment(pattern, nextStart)
		if !ok {
			return nil
		}
		connector := strings.TrimSpace(pattern[pos:nextStart])
		if connector == "" {
			return nil
		}
		connectors = append(connectors, connector)
		vertices = append(vertices, nextVertex)
		currentVertex = nextVertex
		pos = nextPos
	}
	if strings.TrimSpace(pattern[pos:]) != "" {
		return nil
	}
	if len(connectors) == 0 {
		return nil
	}
	for i := 1; i < len(vertices)-1; i++ {
		if strings.TrimSpace(variableFromPatternSegment(vertices[i])) != "" {
			continue
		}
		vertices[i] = projectionInjectPatternVariable(vertices[i], fmt.Sprintf("__ve_conn_%d", i))
	}
	out := make([]string, 0, len(connectors))
	for i := 0; i < len(connectors); i++ {
		edgePattern := strings.TrimSpace(vertices[i] + connectors[i] + vertices[i+1])
		if parseEdgeMutation(edgePattern) == nil {
			return nil
		}
		out = append(out, edgePattern)
	}
	return out
}

func projectionInjectPatternVariable(segment string, variable string) string {
	segment = strings.TrimSpace(segment)
	variable = strings.TrimSpace(variable)
	if segment == "" || variable == "" || !strings.HasPrefix(segment, "(") || !strings.HasSuffix(segment, ")") {
		return segment
	}
	body := strings.TrimSpace(segment[1 : len(segment)-1])
	if body == "" {
		return "(" + variable + ")"
	}
	if strings.HasPrefix(body, ":") || strings.HasPrefix(body, "{") {
		return "(" + variable + body + ")"
	}
	return "(" + variable + " " + body + ")"
}

func nextProjectionVertexSegment(pattern string, start int) (string, int, bool) {
	if start < 0 {
		start = 0
	}
	for start < len(pattern) && unicode.IsSpace(rune(pattern[start])) {
		start++
	}
	if start >= len(pattern) || pattern[start] != '(' {
		return "", 0, false
	}
	depth := 0
	for i := start; i < len(pattern); i++ {
		switch pattern[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(pattern[start : i+1]), i + 1, true
			}
		}
	}
	return "", 0, false
}

func parseProjectionVariableLengthBounds(pattern string) (int, int, bool) {
	raw := strings.TrimSpace(pattern)
	start := strings.IndexByte(raw, '[')
	end := strings.IndexByte(raw, ']')
	if start < 0 || end <= start {
		return 0, 0, false
	}
	body := strings.TrimSpace(raw[start+1 : end])
	star := strings.IndexByte(body, '*')
	if star < 0 {
		return 0, 0, false
	}
	bounds := strings.TrimSpace(body[star+1:])
	// maxVarLengthHops is the cap used when no upper bound is specified (*n.., *.., *).
	const maxVarLengthHops = 64
	if bounds == "" {
		// [*] means 1..∞
		return 1, maxVarLengthHops, true
	}
	if strings.Contains(bounds, "..") {
		parts := strings.SplitN(bounds, "..", 2)
		left := strings.TrimSpace(parts[0])
		right := strings.TrimSpace(parts[1])
		minHops := 1
		if left != "" {
			if parsed, err := strconv.Atoi(left); err == nil {
				minHops = parsed
			}
		}
		if right != "" {
			// Preserve the raw parsed value even for inverted intervals (min > max):
			// expandVariableLengthPathRows will naturally emit nothing because
			// walk stops at depth>=maxHops before ever reaching depth>=minHops.
			if parsed, err := strconv.Atoi(right); err == nil {
				return minHops, parsed, true
			}
		}
		// right is empty: [*n..] or [*..] — unbounded upper
		return minHops, maxVarLengthHops, true
	}
	if parsed, err := strconv.Atoi(bounds); err == nil && parsed >= 0 {
		return parsed, parsed, true
	}
	return 0, 0, false
}

func expandVariableLengthPathRows(state *State, row map[string]any, pattern string, pathVar string, minHops int, maxHops int, optional bool) ([]map[string]any, error) {
	if state == nil || state.Tx == nil {
		return nil, nil
	}
	edge := parseEdgeMutation(pattern)
	if edge == nil {
		return nil, nil
	}
	direction := detectPatternDirection(edge.Pattern)
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	leftExpectedProps := map[string]any{}
	rightExpectedProps := map[string]any{}
	if endpointMatches := edgePatternEndpointsRe.FindStringSubmatch(strings.TrimSpace(edge.Pattern)); len(endpointMatches) == 3 {
		leftExpectedProps = resolveWritePropertyBindingsState("("+strings.TrimSpace(endpointMatches[1])+")", state.Params, row, state)
		rightExpectedProps = resolveWritePropertyBindingsState("("+strings.TrimSpace(endpointMatches[2])+")", state.Params, row, state)
	}
	edgeTypeSet := projectionEdgeTypeSet(edge.Type)
	vertexMatchesConstraints := func(vertexID string, labels []string, expectedProps map[string]any) bool {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			return false
		}
		vertex, err := state.Tx.GetVertex(ctx, tenant, vertexID)
		if err != nil || vertex == nil {
			return false
		}
		if !projectionVertexHasAllLabels(vertex, labels) {
			return false
		}
		if len(expectedProps) != 0 && !vertexHasExpectedProperties(vertex, expectedProps) {
			return false
		}
		return true
	}
	edgeMatchesConstraints := func(edgeID string) bool {
		edgeID = strings.TrimSpace(edgeID)
		if edgeID == "" {
			return false
		}
		if len(edgeTypeSet) != 0 {
			found, err := state.Tx.GetEdge(ctx, tenant, edgeID)
			if err != nil || found == nil {
				return false
			}
			if _, allowed := edgeTypeSet[strings.ToUpper(strings.TrimSpace(found.Type))]; !allowed {
				return false
			}
		}
		if endpointMatches := edgePatternEndpointsRe.FindStringSubmatch(strings.TrimSpace(edge.Pattern)); len(endpointMatches) == 3 {
			_ = endpointMatches
		}
		return true
	}
	buildPathValue := func(leftID, rightID string, rels []*graph.Edge) projectionPathValue {
		nodes := make([]any, 0, len(rels)+1)
		currentID := strings.TrimSpace(leftID)
		appendVertex := func(vertexID string) {
			if vertex, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(vertexID)); err == nil && vertex != nil {
				nodes = append(nodes, clonePathVertex(vertex))
				return
			}
			nodes = append(nodes, map[string]any{"id": strings.TrimSpace(vertexID)})
		}
		appendVertex(currentID)
		for _, rel := range rels {
			if rel == nil {
				continue
			}
			if strings.TrimSpace(rel.SrcID) == currentID {
				currentID = strings.TrimSpace(rel.DstID)
			} else {
				currentID = strings.TrimSpace(rel.SrcID)
			}
			appendVertex(currentID)
		}
		if len(nodes) == 0 {
			appendVertex(leftID)
		}
		if len(nodes) == 1 {
			appendVertex(rightID)
		}
		relVals := make([]any, 0, len(rels))
		for _, rel := range rels {
			relVals = append(relVals, clonePathEdge(rel))
		}
		return projectionPathValue{nodes: nodes, relationships: relVals}
	}
	if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
		if sequenceIDs, ok := resolveBoundEdgeSequenceIDList(row, edgeVar); ok {
			matchSequence := func(forward bool) (map[string]any, bool, error) {
				if len(sequenceIDs) < minHops || len(sequenceIDs) > maxHops {
					return nil, false, nil
				}
				rels := make([]*graph.Edge, 0, len(sequenceIDs))
				for _, edgeID := range sequenceIDs {
					if !edgeMatchesConstraints(edgeID) {
						return nil, false, nil
					}
					rel, err := state.Tx.GetEdge(ctx, tenant, strings.TrimSpace(edgeID))
					if err != nil || rel == nil {
						return nil, false, err
					}
					rels = append(rels, rel)
				}
				startID := strings.TrimSpace(rels[0].SrcID)
				endID := strings.TrimSpace(rels[len(rels)-1].DstID)
				for i := 0; i < len(rels)-1; i++ {
					if forward {
						if strings.TrimSpace(rels[i].DstID) != strings.TrimSpace(rels[i+1].SrcID) {
							return nil, false, nil
						}
					} else {
						if strings.TrimSpace(rels[i].SrcID) != strings.TrimSpace(rels[i+1].DstID) {
							return nil, false, nil
						}
						startID = strings.TrimSpace(rels[0].DstID)
						endID = strings.TrimSpace(rels[len(rels)-1].SrcID)
					}
				}
				if !vertexMatchesConstraints(startID, edge.LeftLabels, leftExpectedProps) || !vertexMatchesConstraints(endID, edge.RightLabels, rightExpectedProps) {
					return nil, false, nil
				}
				if bound, id := resolveBoundEntityID(row, edge.LeftVar); bound && strings.TrimSpace(id) != startID {
					return nil, false, nil
				}
				if bound, id := resolveBoundEntityID(row, edge.RightVar); bound && strings.TrimSpace(id) != endID {
					return nil, false, nil
				}
				next := cloneRow(row)
				if !bindPatternVarChecked(next, edge.LeftVar, startID) || !bindPatternVarChecked(next, edge.RightVar, endID) {
					return nil, false, nil
				}
				relVals := make([]any, 0, len(rels))
				for _, rel := range rels {
					relVals = append(relVals, rel)
				}
				if edgeVar != "" {
					next[edgeVar] = relVals
					next[edgeVar+".id"] = sequenceIDs
				}
				if strings.TrimSpace(pathVar) != "" {
					appendProjectionPathValue(next, pathVar, buildPathValue(startID, endID, rels))
				}
				next["__ve_chain_cursor"] = endID
				return next, true, nil
			}
			if next, ok, err := matchSequence(true); err != nil {
				return nil, err
			} else if ok {
				return []map[string]any{next}, nil
			}
			if direction == patternDirAny {
				if next, ok, err := matchSequence(false); err != nil {
					return nil, err
				} else if ok {
					return []map[string]any{next}, nil
				}
			}
			if optional {
				return []map[string]any{cloneRow(row)}, nil
			}
			return nil, nil
		}
	}

	startVertices := make([]*graph.Vertex, 0)
	if edge.LeftVar != "" {
		if bound, id := resolveBoundEntityID(row, edge.LeftVar); bound && strings.TrimSpace(id) != "" {
			v, err := state.Tx.GetVertex(ctx, tenant, id)
			if err != nil {
				return nil, err
			}
			if v != nil && projectionVertexHasAllLabels(v, edge.LeftLabels) && vertexHasExpectedProperties(v, leftExpectedProps) {
				startVertices = append(startVertices, v)
			}
		}
	}
	if len(startVertices) == 0 {
		err := state.Tx.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
			if v == nil {
				return nil
			}
			if projectionVertexHasAllLabels(v, edge.LeftLabels) && vertexHasExpectedProperties(v, leftExpectedProps) {
				startVertices = append(startVertices, v)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	rightBound := false
	rightBoundID := ""
	if edge.RightVar != "" {
		rightBound, rightBoundID = resolveBoundEntityID(row, edge.RightVar)
	}
	if edge.LeftVar == "" {
		if cursorID := strings.TrimSpace(scalarString(row["__ve_chain_cursor"])); cursorID != "" {
			if v, err := state.Tx.GetVertex(ctx, tenant, cursorID); err == nil && v != nil {
				if projectionVertexHasAllLabels(v, edge.LeftLabels) && vertexHasExpectedProperties(v, leftExpectedProps) {
					startVertices = append(startVertices, v)
				}
			}
		}
	}

	out := make([]map[string]any, 0)
	for _, start := range startVertices {
		if start == nil {
			continue
		}
		nodes := []*graph.Vertex{start}
		edges := []*graph.Edge{}
		var walk func(current *graph.Vertex, depth int) error
		walk = func(current *graph.Vertex, depth int) error {
			if depth >= minHops {
				if !rightBound || strings.TrimSpace(current.ID) == rightBoundID {
					if projectionVertexHasAllLabels(current, edge.RightLabels) && vertexHasExpectedProperties(current, rightExpectedProps) {
						next := cloneRow(row)
						if !bindPatternVarChecked(next, edge.LeftVar, start.ID) || !bindPatternVarChecked(next, edge.RightVar, current.ID) {
							return nil
						}
						if pathVar != "" {
							nodeVals := make([]any, 0, len(nodes))
							for _, n := range nodes {
								nodeVals = append(nodeVals, n)
							}
							relVals := make([]any, 0, len(edges))
							for _, e := range edges {
								relVals = append(relVals, e)
							}
							appendProjectionPathValue(next, pathVar, projectionPathValue{nodes: nodeVals, relationships: relVals})
						}
						next["__ve_chain_cursor"] = strings.TrimSpace(current.ID)
						if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
							relVals := make([]any, 0, len(edges))
							relIDs := make([]any, 0, len(edges))
							for _, e := range edges {
								relVals = append(relVals, e)
								if e != nil {
									relIDs = append(relIDs, strings.TrimSpace(e.ID))
								}
							}
							next[edgeVar] = relVals
							next[edgeVar+".id"] = relIDs
						}
						out = append(out, next)
					}
				}
			}
			if depth >= maxHops {
				return nil
			}

			switch direction {
			case patternDirOut:
				if err := state.Tx.ScanAdjacencyLinks(ctx, tenant, current.ID, graph.EdgeDirectionOut, strings.TrimSpace(edge.Type), 0, func(edgeID, peerID string) error {
					if rowContainsEdgeID(row, edgeID) {
						return nil
					}
					if projectionPathContainsEdgeID(edges, edgeID) {
						return nil
					}
					nextVertex, err := state.Tx.GetVertex(ctx, tenant, peerID)
					if err != nil || nextVertex == nil {
						return err
					}
					rel, err := state.Tx.GetEdge(ctx, tenant, edgeID)
					if err != nil || rel == nil {
						return err
					}
					nodes = append(nodes, nextVertex)
					edges = append(edges, rel)
					err = walk(nextVertex, depth+1)
					nodes = nodes[:len(nodes)-1]
					edges = edges[:len(edges)-1]
					return err
				}); err != nil {
					return err
				}
			case patternDirIn:
				if err := state.Tx.ScanAdjacencyLinks(ctx, tenant, current.ID, graph.EdgeDirectionIn, strings.TrimSpace(edge.Type), 0, func(edgeID, peerID string) error {
					if rowContainsEdgeID(row, edgeID) {
						return nil
					}
					if projectionPathContainsEdgeID(edges, edgeID) {
						return nil
					}
					nextVertex, err := state.Tx.GetVertex(ctx, tenant, peerID)
					if err != nil || nextVertex == nil {
						return err
					}
					rel, err := state.Tx.GetEdge(ctx, tenant, edgeID)
					if err != nil || rel == nil {
						return err
					}
					nodes = append(nodes, nextVertex)
					edges = append(edges, rel)
					err = walk(nextVertex, depth+1)
					nodes = nodes[:len(nodes)-1]
					edges = edges[:len(edges)-1]
					return err
				}); err != nil {
					return err
				}
			default:
				if err := state.Tx.ScanAdjacencyLinks(ctx, tenant, current.ID, graph.EdgeDirectionAny, strings.TrimSpace(edge.Type), 0, func(edgeID, peerID string) error {
					if rowContainsEdgeID(row, edgeID) {
						return nil
					}
					if projectionPathContainsEdgeID(edges, edgeID) {
						return nil
					}
					nextVertex, err := state.Tx.GetVertex(ctx, tenant, peerID)
					if err != nil || nextVertex == nil {
						return err
					}
					rel, err := state.Tx.GetEdge(ctx, tenant, edgeID)
					if err != nil || rel == nil {
						return err
					}
					nodes = append(nodes, nextVertex)
					edges = append(edges, rel)
					err = walk(nextVertex, depth+1)
					nodes = nodes[:len(nodes)-1]
					edges = edges[:len(edges)-1]
					return err
				}); err != nil {
					return err
				}
			}
			return nil
		}
		if err := walk(start, 0); err != nil {
			return nil, err
		}
	}

	if len(out) == 0 && optional {
		next := cloneRow(row)
		if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
			if edgeBound, _ := resolveBoundEntityID(row, edgeVar); !edgeBound {
				next[edgeVar] = nil
				next[edgeVar+".id"] = nil
			}
		}
		if pathVar != "" {
			next[pathVar] = nil
		}
		return []map[string]any{next}, nil
	}
	return out, nil
}

func projectionPathContainsEdgeID(edges []*graph.Edge, edgeID string) bool {
	edgeID = strings.TrimSpace(edgeID)
	if edgeID == "" {
		return false
	}
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if strings.TrimSpace(edge.ID) == edgeID {
			return true
		}
	}
	return false
}

func projectionVertexHasAllLabels(v *graph.Vertex, labels []string) bool {
	if v == nil || len(labels) == 0 {
		return v != nil
	}
	have := map[string]struct{}{}
	for _, label := range v.Labels {
		have[strings.ToLower(strings.TrimSpace(label))] = struct{}{}
	}
	matchesLabelExpr := func(expr string) bool {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return true
		}
		alts := strings.Split(expr, "|")
		matched := false
		for _, alt := range alts {
			alt = strings.TrimSpace(alt)
			if alt == "" {
				continue
			}
			negated := strings.HasPrefix(alt, "!")
			if negated {
				alt = strings.TrimSpace(alt[1:])
			}
			if alt == "" {
				continue
			}
			_, present := have[strings.ToLower(alt)]
			if negated {
				if !present {
					matched = true
					break
				}
				continue
			}
			if present {
				matched = true
				break
			}
		}
		return matched
	}
	for _, labelExpr := range labels {
		if !matchesLabelExpr(labelExpr) {
			return false
		}
	}
	return true
}

func expandVertexPatternRow(state *State, row map[string]any, vertex *VertexMutation, optional bool) ([]map[string]any, error) {
	if state == nil || state.Tx == nil || vertex == nil {
		return nil, nil
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	expectedProps := resolveWritePropertyBindingsState(vertex.Pattern, state.Params, row, state)
	hasPropertyPredicate := len(expectedProps) > 0

	if bound, id := resolveBoundEntityID(row, vertex.Var); bound {
		candidate, err := state.Tx.GetVertex(ctx, tenant, id)
		if err != nil || candidate == nil {
			if optional {
				next := cloneRow(row)
				bindPatternVar(next, vertex.Var, "")
				return []map[string]any{next}, nil
			}
			return nil, nil
		}
		if !vertexHasAllLabels(candidate, vertex.Labels) {
			if optional {
				next := cloneRow(row)
				bindPatternVar(next, vertex.Var, "")
				return []map[string]any{next}, nil
			}
			return nil, nil
		}
		if !vertexHasExpectedProperties(candidate, expectedProps) {
			if optional {
				next := cloneRow(row)
				bindPatternVar(next, vertex.Var, "")
				return []map[string]any{next}, nil
			}
			return nil, nil
		}
		next := cloneRow(row)
		bindPatternVar(next, vertex.Var, candidate.ID)
		return []map[string]any{next}, nil
	}

	out := make([]map[string]any, 0)
	indexProbeAttempted := false
	if tenant != "" && len(vertex.Labels) > 0 && len(expectedProps) > 0 {
		keys := make([]string, 0, len(expectedProps))
		for key := range expectedProps {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		seenVertexes := map[string]struct{}{}
		for _, probeKey := range keys {
			probeValue, valueOK := writePathPropertyValueToBytes(expectedProps[probeKey])
			if !valueOK {
				continue
			}
			for _, label := range vertex.Labels {
				label = strings.TrimSpace(label)
				if label == "" {
					continue
				}
				indexProbeAttempted = true
				if err := state.Tx.ScanPropertyIndex(ctx, tenant, label, probeKey, probeValue, 0, func(entry *graph.PropertyIndexEntry) error {
					if entry == nil {
						return nil
					}
					if !strings.EqualFold(strings.TrimSpace(entry.EntityClass), "vertex") {
						return nil
					}
					candidateID := strings.TrimSpace(entry.EntityID)
					if candidateID == "" {
						return nil
					}
					if _, seen := seenVertexes[candidateID]; seen {
						return nil
					}
					candidate, err := state.Tx.GetVertex(ctx, tenant, candidateID)
					if err != nil || candidate == nil {
						return nil
					}
					if !vertexHasAllLabels(candidate, vertex.Labels) {
						return nil
					}
					if !vertexHasExpectedProperties(candidate, expectedProps) {
						return nil
					}
					next := cloneRow(row)
					bindPatternVar(next, vertex.Var, candidate.ID)
					seenVertexes[candidateID] = struct{}{}
					out = append(out, next)
					return nil
				}); err != nil {
					return nil, err
				}
			}
		}
		if len(out) > 0 {
			if state.Metrics != nil {
				state.Metrics.ObserveIndexLookup("property_index", "hit", len(out))
			}
			return out, nil
		}
	}

	fallbackCacheAttempted := false
	if tenant != "" && len(vertex.Labels) > 0 && len(expectedProps) > 0 {
		cacheKeys := make([]string, 0, len(expectedProps))
		for key := range expectedProps {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			cacheKeys = append(cacheKeys, key)
		}
		sort.Strings(cacheKeys)
		for _, cacheKey := range cacheKeys {
			fallbackCacheAttempted = true
			cachedRows, cacheErr := expandVertexPatternRowsFromFallbackCache(ctx, state, row, tenant, vertex, expectedProps, cacheKey)
			if cacheErr != nil {
				return nil, cacheErr
			}
			if len(cachedRows) > 0 {
				out = cachedRows
				break
			}
		}
	}

	if !fallbackCacheAttempted {
		err := state.Tx.ScanVertices(ctx, tenant, 0, func(found *graph.Vertex) error {
			if found == nil {
				return nil
			}
			candidate := found
			if !vertexHasAllLabels(candidate, vertex.Labels) {
				return nil
			}
			if len(expectedProps) > 0 && len(candidate.Properties) == 0 {
				if hydrated, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(candidate.ID)); err == nil && hydrated != nil {
					candidate = hydrated
				}
			}
			if !vertexHasExpectedProperties(candidate, expectedProps) {
				return nil
			}
			next := cloneRow(row)
			bindPatternVar(next, vertex.Var, candidate.ID)
			out = append(out, next)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if hasPropertyPredicate && state.Metrics != nil {
		if len(out) == 0 {
			if indexProbeAttempted {
				state.Metrics.ObserveIndexLookup("property_index", "miss", 0)
			}
		} else {
			if indexProbeAttempted {
				state.Metrics.ObserveIndexLookup("property_index", "hit", len(out))
			}
		}
	}
	if len(out) == 0 && optional {
		return []map[string]any{cloneRow(row)}, nil
	}
	return out, nil
}

func expandVertexPatternRowsFromFallbackCache(ctx context.Context, state *State, row map[string]any, tenant string, vertex *VertexMutation, expectedProps map[string]any, propKey string) ([]map[string]any, error) {
	if state == nil || state.Tx == nil || vertex == nil {
		return nil, nil
	}
	if strings.TrimSpace(propKey) == "" {
		return nil, nil
	}
	encodedValue, ok := writePathPropertyValueToBytes(expectedProps[propKey])
	if !ok {
		return nil, nil
	}
	indexByValue, err := runtimeVertexFallbackLookupByProperty(state, ctx, tenant, vertex.Labels, propKey)
	if err != nil {
		return nil, err
	}
	candidateIDs := indexByValue[string(encodedValue)]
	if len(candidateIDs) == 0 {
		return nil, nil
	}
	out := make([]map[string]any, 0, len(candidateIDs))
	seen := map[string]struct{}{}
	for _, candidateID := range candidateIDs {
		candidateID = strings.TrimSpace(candidateID)
		if candidateID == "" {
			continue
		}
		if _, dup := seen[candidateID]; dup {
			continue
		}
		candidate, getErr := state.Tx.GetVertex(ctx, tenant, candidateID)
		if getErr != nil || candidate == nil {
			continue
		}
		if !vertexHasAllLabels(candidate, vertex.Labels) {
			continue
		}
		if !vertexHasExpectedProperties(candidate, expectedProps) {
			continue
		}
		next := cloneRow(row)
		bindPatternVar(next, vertex.Var, candidate.ID)
		out = append(out, next)
		seen[candidateID] = struct{}{}
	}
	return out, nil
}

func runtimeVertexFallbackLookupByProperty(state *State, ctx context.Context, tenant string, labels []string, propKey string) (map[string][]string, error) {
	if state == nil || state.Tx == nil || tenant == "" {
		return nil, nil
	}
	normalizedLabels := make([]string, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		normalizedLabels = append(normalizedLabels, label)
	}
	if len(normalizedLabels) == 0 || strings.TrimSpace(propKey) == "" {
		return nil, nil
	}
	sort.Strings(normalizedLabels)

	cache := runtimeVertexFallbackLookupCache(state)
	cacheKey := tenant + "|" + strings.Join(normalizedLabels, ":") + "|" + strings.TrimSpace(propKey)
	if existing, ok := cache[cacheKey]; ok {
		return existing, nil
	}

	built := map[string][]string{}
	err := state.Tx.ScanVertices(ctx, tenant, 0, func(found *graph.Vertex) error {
		if found == nil {
			return nil
		}
		if !vertexHasAllLabels(found, normalizedLabels) {
			return nil
		}
		rawValue, ok := found.Properties[propKey]
		if !ok {
			return nil
		}
		vertexID := strings.TrimSpace(found.ID)
		if vertexID == "" {
			return nil
		}
		encoded := string(rawValue)
		built[encoded] = append(built[encoded], vertexID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	cache[cacheKey] = built
	return built, nil
}

func runtimeVertexFallbackLookupCache(state *State) map[string]map[string][]string {
	if state == nil {
		return map[string]map[string][]string{}
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	const cacheParamKey = "__ve_runtime_vertex_fallback_lookup"
	if existing, ok := state.Params[cacheParamKey]; ok {
		if typed, ok := existing.(map[string]map[string][]string); ok && typed != nil {
			return typed
		}
	}
	built := map[string]map[string][]string{}
	state.Params[cacheParamKey] = built
	return built
}

func vertexHasExpectedProperties(vertex *graph.Vertex, expected map[string]any) bool {
	if len(expected) == 0 {
		return true
	}
	if vertex == nil {
		return false
	}
	for key, expectedValue := range expected {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if strings.EqualFold(key, "id") {
			if raw, ok := vertex.Properties[key]; ok {
				actualValue := decodeProjectionPropertyValue(raw)
				if !projectionWriteValuesEqual(actualValue, expectedValue) {
					return false
				}
				continue
			}
			if !projectionWriteValuesEqual(strings.TrimSpace(vertex.ID), expectedValue) {
				return false
			}
			continue
		}
		raw, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		actualValue := decodeProjectionPropertyValue(raw)
		if !projectionWriteValuesEqual(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

func projectionWriteValuesEqual(actual any, expected any) bool {
	if reflect.DeepEqual(actual, expected) {
		return true
	}
	if ai, ok := projectionNumericToInt(actual); ok {
		if ei, ok := projectionNumericToInt(expected); ok {
			return ai == ei
		}
	}
	return strings.TrimSpace(fmt.Sprint(actual)) == strings.TrimSpace(fmt.Sprint(expected))
}

func bindProjectionVertexPath(row map[string]any, pathVar string, vertexVar string, state *State) {
	if row == nil || strings.TrimSpace(pathVar) == "" || strings.TrimSpace(vertexVar) == "" {
		return
	}
	_, vertexID := resolveBoundEntityID(row, vertexVar)
	if strings.TrimSpace(vertexID) == "" {
		return
	}
	vertexAny := any(map[string]any{"id": strings.TrimSpace(vertexID)})
	if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
		ctx := context.Background()
		if state.Context != nil {
			ctx = state.Context
		}
		if found, err := state.Tx.GetVertex(ctx, state.Tenant, strings.TrimSpace(vertexID)); err == nil && found != nil {
			vertexAny = found
		}
	}
	appendProjectionPathValue(row, pathVar, projectionPathValue{nodes: []any{vertexAny}, relationships: nil})
}

func appendProjectionPathValue(row map[string]any, pathVar string, segment projectionPathValue) {
	if row == nil || strings.TrimSpace(pathVar) == "" {
		return
	}
	current, hasCurrent := row[pathVar]
	if !hasCurrent || current == nil {
		row[pathVar] = segment
		return
	}
	existing, ok := current.(projectionPathValue)
	if !ok {
		row[pathVar] = segment
		return
	}
	if len(existing.nodes) == 0 {
		row[pathVar] = segment
		return
	}
	if len(segment.nodes) == 0 {
		row[pathVar] = existing
		return
	}
	merged := projectionPathValue{
		nodes:         append(append([]any(nil), existing.nodes...), segment.nodes[1:]...),
		relationships: append(append([]any(nil), existing.relationships...), segment.relationships...),
	}
	row[pathVar] = merged
}

func vertexHasAllLabels(vertex *graph.Vertex, labels []string) bool {
	return projectionVertexHasAllLabels(vertex, labels)
}

type optionalExpandHandler struct{ baseHandler }

func NewOptionalExpandHandler() Handler {
	return optionalExpandHandler{baseHandler{op: "PHY_EXPAND_OPTIONAL"}}
}

func (h optionalExpandHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *State) (bool, error) {
	executor, ok := optionalExpandVariantExecutors[strings.ToLower(strings.TrimSpace(variant))]
	if !ok {
		return false, nil
	}
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return true, err
	}
	return true, executor(nodeID, attrs, state, true)
}

func (h optionalExpandHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	return executeExpandVariantRows(nodeID, attrs, state, true)
}

type emptyHandler struct{ baseHandler }

func NewEmptyHandler() Handler { return emptyHandler{baseHandler{op: "PHY_EMPTY"}} }

func (h emptyHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	state.Rows = []map[string]any{}
	return nil
}

type callHandler struct{ baseHandler }

func NewCallHandler() Handler { return callHandler{baseHandler{op: "PHY_CALL"}} }

func (h callHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	rawCall := strings.TrimSpace(stringAttr(attrs, "raw"))
	if rawCall == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "runtime call operator missing raw call clause", nil)
	}
	if len(state.Rows) == 0 {
		if len(state.ExecutedOps) == 1 && state.ExecutedOps[0] == "PHY_CALL" {
			state.Rows = []map[string]any{{}}
		}
	}
	exec := state.InQueryCallExecutor
	if exec == nil {
		return graph.NewError(graph.ErrKindUnsupported, "runtime call operator requires in-query call executor", nil)
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	inputRows := state.Rows
	nextRows, _, err := exec(ctx, inputRows, rawCall, state.Params, true)
	if err != nil {
		return err
	}
	if len(inputRows) > 0 && len(nextRows) == len(inputRows) {
		merged := make([]map[string]any, 0, len(nextRows))
		for i := range nextRows {
			next := map[string]any{}
			if i < len(inputRows) {
				next = cloneRow(inputRows[i])
			}
			for key, value := range nextRows[i] {
				if value == nil {
					if _, exists := next[key]; exists {
						continue
					}
				}
				next[key] = value
			}
			merged = append(merged, next)
		}
		state.Rows = merged
		return nil
	}
	state.Rows = nextRows
	return nil
}

type antiProbeHandler struct{ baseHandler }

type antiProbeVariantExecutor func(nodeID string, attrs map[string]any, state *State) error

var antiProbeVariantExecutors = map[string]antiProbeVariantExecutor{
	"anti_probe_batch_high": executeAntiProbeVariantBatch,
	"anti_probe_row_low":    executeAntiProbeVariantRow,
}

func NewAntiProbeHandler() Handler { return antiProbeHandler{baseHandler{op: "PHY_ANTI_PROBE"}} }

func (h antiProbeHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *State) (bool, error) {
	executor, ok := antiProbeVariantExecutors[strings.ToLower(strings.TrimSpace(variant))]
	if !ok {
		return false, nil
	}
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return true, err
	}
	return true, executor(nodeID, attrs, state)
}

func (h antiProbeHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	variant := strings.TrimSpace(strings.ToLower(stringAttr(attrs, "variant")))
	if variant == "anti_probe_row_low" {
		return executeAntiProbeVariantRow(nodeID, attrs, state)
	}
	return executeAntiProbeVariantBatch(nodeID, attrs, state)
}

func executeAntiProbeVariantBatch(nodeID string, attrs map[string]any, state *State) error {
	return executeAntiProbe(nodeID, attrs, state, false)
}

func executeAntiProbeVariantRow(nodeID string, attrs map[string]any, state *State) error {
	return executeAntiProbe(nodeID, attrs, state, true)
}

func executeAntiProbe(nodeID string, attrs map[string]any, state *State, rowProbe bool) error {
	_ = nodeID
	if state == nil || len(state.Rows) == 0 {
		return nil
	}
	if state.Tx == nil {
		return nil
	}
	leftVar := strings.TrimSpace(stringAttr(attrs, "leftVar"))
	rightVar := strings.TrimSpace(stringAttr(attrs, "rightVar"))
	edgeType := strings.TrimSpace(stringAttr(attrs, "edgeType"))
	mode := strings.TrimSpace(strings.ToLower(stringAttr(attrs, "mode")))
	if leftVar == "" || rightVar == "" || edgeType == "" {
		return nil
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)

	rows := state.Rows
	rowIndex := make([]int, 0, len(rows))
	undirectedProbes := make([]graph.UndirectedEdgeProbe, 0, len(rows))
	directedProbes := make([]graph.DirectedEdgeProbe, 0, len(rows))

	for idx, row := range rows {
		leftBound, leftID := resolveBoundEntityID(row, leftVar)
		rightBound, rightID := resolveBoundEntityID(row, rightVar)
		if !leftBound || !rightBound {
			continue
		}
		rowIndex = append(rowIndex, idx)
		if mode == "directed" {
			directedProbes = append(directedProbes, graph.DirectedEdgeProbe{SrcID: leftID, DstID: rightID, EdgeType: edgeType})
			continue
		}
		undirectedProbes = append(undirectedProbes, graph.UndirectedEdgeProbe{LeftID: leftID, RightID: rightID, EdgeType: edgeType})
	}

	if len(rowIndex) == 0 {
		return nil
	}

	exists := make([]bool, len(rowIndex))
	if rowProbe {
		for i, rowPos := range rowIndex {
			if rowPos < 0 || rowPos >= len(rows) {
				continue
			}
			row := rows[rowPos]
			leftBound, leftID := resolveBoundEntityID(row, leftVar)
			rightBound, rightID := resolveBoundEntityID(row, rightVar)
			if !leftBound || !rightBound {
				continue
			}
			var (
				hit bool
				err error
			)
			if mode == "directed" {
				hit, err = state.Tx.HasDirectedEdgeBetween(ctx, tenant, leftID, rightID, edgeType)
			} else {
				hit, err = state.Tx.HasUndirectedEdgeBetween(ctx, tenant, leftID, rightID, edgeType)
			}
			if err != nil {
				return err
			}
			exists[i] = hit
		}
	} else {
		if mode == "directed" {
			probeResults, err := state.Tx.BatchHasDirectedEdgeBetween(ctx, tenant, directedProbes)
			if err != nil {
				return err
			}
			copy(exists, probeResults)
		} else {
			probeResults, err := state.Tx.BatchHasUndirectedEdgeBetween(ctx, tenant, undirectedProbes)
			if err != nil {
				return err
			}
			copy(exists, probeResults)
		}
	}

	blocked := map[int]struct{}{}
	for i, rowPos := range rowIndex {
		if i < len(exists) && exists[i] {
			blocked[rowPos] = struct{}{}
		}
	}
	out := make([]map[string]any, 0, len(rows)-len(blocked))
	for i, row := range rows {
		if _, found := blocked[i]; found {
			continue
		}
		out = append(out, row)
	}
	state.Rows = out
	return nil
}

type patternDirection int

const (
	patternDirOut patternDirection = iota
	patternDirIn
	patternDirAny
)

func detectPatternDirection(pattern string) patternDirection {
	trimmed := strings.ReplaceAll(strings.TrimSpace(pattern), " ", "")
	hasIn := strings.Contains(trimmed, "<-")
	hasOut := strings.Contains(trimmed, "->")
	switch {
	case hasIn && hasOut:
		return patternDirAny
	case hasIn:
		return patternDirIn
	case hasOut:
		return patternDirOut
	default:
		return patternDirAny
	}
}

func expandPatternRow(state *State, row map[string]any, edge *EdgeMutation, pathVar string, attrs map[string]any, optional bool) ([]map[string]any, error) {
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	leftExpectedProps := map[string]any{}
	rightExpectedProps := map[string]any{}
	edgeExpectedProps := edgePatternExpectedProperties(edge.Pattern, state.Params, row, state)
	if endpointMatches := edgePatternEndpointsRe.FindStringSubmatch(strings.TrimSpace(edge.Pattern)); len(endpointMatches) == 3 {
		leftExpectedProps = resolveWritePropertyBindingsState("("+strings.TrimSpace(endpointMatches[1])+")", state.Params, row, state)
		rightExpectedProps = resolveWritePropertyBindingsState("("+strings.TrimSpace(endpointMatches[2])+")", state.Params, row, state)
	}
	vertexMatchesConstraints := func(vertexID string, labels []string, expectedProps map[string]any) bool {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			return false
		}
		vertex, err := state.Tx.GetVertex(ctx, tenant, vertexID)
		if err != nil || vertex == nil {
			return false
		}
		if !projectionVertexHasAllLabels(vertex, labels) {
			return false
		}
		if len(expectedProps) != 0 && !vertexHasExpectedProperties(vertex, expectedProps) {
			return false
		}
		return true
	}
	buildPathValue := func(leftID, rightID, edgeID string) any {
		if strings.TrimSpace(pathVar) == "" {
			return nil
		}
		nodes := make([]any, 0, 2)
		if left, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(leftID)); err == nil && left != nil {
			nodes = append(nodes, clonePathVertex(left))
		} else {
			nodes = append(nodes, map[string]any{"id": strings.TrimSpace(leftID)})
		}
		if right, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(rightID)); err == nil && right != nil {
			nodes = append(nodes, clonePathVertex(right))
		} else {
			nodes = append(nodes, map[string]any{"id": strings.TrimSpace(rightID)})
		}
		rels := make([]any, 0, 1)
		if rel, err := state.Tx.GetEdge(ctx, tenant, strings.TrimSpace(edgeID)); err == nil && rel != nil {
			rels = append(rels, clonePathEdge(rel))
		} else if strings.TrimSpace(edgeID) != "" {
			rels = append(rels, map[string]any{"id": strings.TrimSpace(edgeID)})
		}
		return projectionPathValue{nodes: nodes, relationships: rels}
	}
	edgeTypeSet := projectionEdgeTypeSet(edge.Type)
	edgeTypeScanFilter := projectionEdgeTypeScanFilter(edge.Type)
	lookupEdgeTypeAllowed := func(edgeID string, cache map[string]bool) bool {
		if len(edgeTypeSet) == 0 {
			return true
		}
		edgeID = strings.TrimSpace(edgeID)
		if edgeID == "" {
			return false
		}
		if allowed, ok := cache[edgeID]; ok {
			return allowed
		}
		found, err := state.Tx.GetEdge(ctx, tenant, edgeID)
		if err != nil || found == nil {
			cache[edgeID] = false
			return false
		}
		_, allowed := edgeTypeSet[strings.ToUpper(strings.TrimSpace(found.Type))]
		cache[edgeID] = allowed
		return allowed
	}
	edgeMatchesConstraints := func(edgeID string, cache map[string]bool) bool {
		edgeID = strings.TrimSpace(edgeID)
		if edgeID == "" {
			return len(edgeExpectedProps) == 0
		}
		if len(edgeExpectedProps) == 0 {
			return true
		}
		if allowed, ok := cache[edgeID]; ok {
			return allowed
		}
		found, err := state.Tx.GetEdge(ctx, tenant, edgeID)
		if err != nil || found == nil {
			cache[edgeID] = false
			return false
		}
		allowed := edgeHasExpectedProperties(found, edgeExpectedProps)
		cache[edgeID] = allowed
		return allowed
	}
	scanTypeFilter := edgeTypeScanFilter

	leftBound, leftID := resolveBoundEntityID(row, edge.LeftVar)
	rightBound, rightID := resolveBoundEntityID(row, edge.RightVar)
	edgeBound := false
	boundEdgeID := ""
	if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
		edgeBound, boundEdgeID = resolveBoundEntityID(row, edgeVar)
		boundEdgeID = strings.TrimSpace(boundEdgeID)
	}
	if !leftBound && strings.TrimSpace(edge.LeftVar) == "" {
		if cursorID := strings.TrimSpace(scalarString(row["__ve_chain_cursor"])); cursorID != "" {
			leftBound = true
			leftID = cursorID
		}
	}
	direction := detectPatternDirection(edge.Pattern)
	undirectedInFirst, indexedJoin := expandExecutionHints(attrs)
	enforceRowEdgeUniqueness := strings.TrimSpace(scalarString(row["__ve_chain_cursor"])) != "" && (strings.TrimSpace(edge.Var) != "" || strings.TrimSpace(edge.LeftVar) != "" || strings.TrimSpace(edge.RightVar) != "")

	scanAndBind := func(anchorID, anchorVar, peerVar string, scanDir graph.EdgeDirection, anchorBound bool, peerBound bool, expectedPeerID string) ([]map[string]any, error) {
		out := make([]map[string]any, 0)
		edgeTypeCache := map[string]bool{}
		edgePropsCache := map[string]bool{}
		seenMatches := map[string]struct{}{}
		anchorLabels := edge.LeftLabels
		peerLabels := edge.RightLabels
		if strings.TrimSpace(anchorVar) == strings.TrimSpace(edge.RightVar) {
			anchorLabels = edge.RightLabels
			peerLabels = edge.LeftLabels
		}
		anchorProps := leftExpectedProps
		peerProps := rightExpectedProps
		if strings.TrimSpace(anchorVar) == strings.TrimSpace(edge.RightVar) {
			anchorProps = rightExpectedProps
			peerProps = leftExpectedProps
		}
		if !vertexMatchesConstraints(anchorID, anchorLabels, anchorProps) {
			if optional {
				return []map[string]any{optionalNullBoundRow(row, edge, anchorBound, peerBound)}, nil
			}
			return out, nil
		}
		err := state.Tx.ScanAdjacencyLinks(ctx, tenant, anchorID, scanDir, strings.TrimSpace(scanTypeFilter), 0, func(edgeID, peerID string) error {
			edgeID = strings.TrimSpace(edgeID)
			if edgeBound && edgeID != boundEdgeID {
				return nil
			}
			if !lookupEdgeTypeAllowed(edgeID, edgeTypeCache) {
				return nil
			}
			if !edgeMatchesConstraints(edgeID, edgePropsCache) {
				return nil
			}
			if enforceRowEdgeUniqueness && rowContainsEdgeID(row, edgeID) {
				return nil
			}
			if expectedPeerID != "" && peerID != expectedPeerID {
				return nil
			}
			if !vertexMatchesConstraints(peerID, peerLabels, peerProps) {
				return nil
			}
			matchKey := strings.TrimSpace(edgeID) + "|" + strings.TrimSpace(anchorID) + "|" + strings.TrimSpace(peerID)
			if _, ok := seenMatches[matchKey]; ok {
				return nil
			}
			seenMatches[matchKey] = struct{}{}
			next := cloneRow(row)
			if !bindPatternVarChecked(next, anchorVar, anchorID) || !bindPatternVarChecked(next, peerVar, peerID) {
				return nil
			}
			if !anchorBound {
				setPatternLabelOrder(next, anchorVar, anchorLabels)
			}
			if !peerBound {
				setPatternLabelOrder(next, peerVar, peerLabels)
			}
			if edgeID != "" {
				next["__edge.id"] = edgeID
				if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
					next[edgeVar] = edgeID
					next[edgeVar+".id"] = edgeID
				}
			}
			if strings.TrimSpace(pathVar) != "" {
				leftIDForPath := anchorID
				rightIDForPath := peerID
				if strings.TrimSpace(anchorVar) == strings.TrimSpace(edge.RightVar) {
					leftIDForPath = peerID
					rightIDForPath = anchorID
				}
				appendProjectionPathValue(next, pathVar, buildPathValue(leftIDForPath, rightIDForPath, edgeID).(projectionPathValue))
			}
			cursorID := strings.TrimSpace(peerID)
			if strings.TrimSpace(anchorVar) == strings.TrimSpace(edge.RightVar) {
				cursorID = strings.TrimSpace(anchorID)
			}
			next["__ve_chain_cursor"] = cursorID
			out = append(out, next)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(out) == 0 && optional {
			leftAlreadyBound, _ := resolveBoundEntityID(row, edge.LeftVar)
			rightAlreadyBound, _ := resolveBoundEntityID(row, edge.RightVar)
			return []map[string]any{optionalNullBoundRow(row, edge, leftAlreadyBound, rightAlreadyBound)}, nil
		}
		return out, nil
	}

	scanUnbound := func(bindLeftFromSrc bool) ([]map[string]any, error) {
		out := make([]map[string]any, 0)
		edgeType := strings.TrimSpace(scanTypeFilter)
		edgeTypeCache := map[string]bool{}
		edgePropsCache := map[string]bool{}
		seenMatches := map[string]struct{}{}
		appendMatch := func(srcID, edgeID, dstID string) error {
			edgeID = strings.TrimSpace(edgeID)
			if edgeBound && edgeID != boundEdgeID {
				return nil
			}
			if !lookupEdgeTypeAllowed(edgeID, edgeTypeCache) {
				return nil
			}
			if !edgeMatchesConstraints(edgeID, edgePropsCache) {
				return nil
			}
			if enforceRowEdgeUniqueness && rowContainsEdgeID(row, edgeID) {
				return nil
			}
			leftID := srcID
			rightID := dstID
			if !bindLeftFromSrc {
				leftID = dstID
				rightID = srcID
			}
			if !vertexMatchesConstraints(leftID, edge.LeftLabels, leftExpectedProps) || !vertexMatchesConstraints(rightID, edge.RightLabels, rightExpectedProps) {
				return nil
			}
			matchKey := strings.TrimSpace(edgeID) + "|" + strings.TrimSpace(leftID) + "|" + strings.TrimSpace(rightID)
			if _, ok := seenMatches[matchKey]; ok {
				return nil
			}
			seenMatches[matchKey] = struct{}{}
			next := cloneRow(row)
			if bindLeftFromSrc {
				if !bindPatternVarChecked(next, edge.LeftVar, srcID) || !bindPatternVarChecked(next, edge.RightVar, dstID) {
					return nil
				}
			} else {
				if !bindPatternVarChecked(next, edge.LeftVar, dstID) || !bindPatternVarChecked(next, edge.RightVar, srcID) {
					return nil
				}
			}
			setPatternLabelOrder(next, edge.LeftVar, edge.LeftLabels)
			setPatternLabelOrder(next, edge.RightVar, edge.RightLabels)
			if edgeID != "" {
				next["__edge.id"] = edgeID
				if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
					next[edgeVar] = edgeID
					next[edgeVar+".id"] = edgeID
				}
			}
			if strings.TrimSpace(pathVar) != "" {
				appendProjectionPathValue(next, pathVar, buildPathValue(leftID, rightID, edgeID).(projectionPathValue))
			}
			next["__ve_chain_cursor"] = strings.TrimSpace(rightID)
			out = append(out, next)
			return nil
		}
		appendUndirectedMatch := func(srcID, edgeID, dstID string) error {
			if strings.TrimSpace(srcID) == strings.TrimSpace(dstID) {
				return appendMatch(srcID, edgeID, dstID)
			}
			if bindLeftFromSrc {
				if err := appendMatch(srcID, edgeID, dstID); err != nil {
					return err
				}
				return appendMatch(dstID, edgeID, srcID)
			}
			if err := appendMatch(dstID, edgeID, srcID); err != nil {
				return err
			}
			return appendMatch(srcID, edgeID, dstID)
		}
		var err error
		if edgeType == "" {
			err = state.Tx.ScanOutEdgeSourceIDs(ctx, tenant, edgeType, 0, func(srcID string) error {
				return state.Tx.ScanOutEdgeLinks(ctx, tenant, srcID, edgeType, 0, func(edgeID, dstID string) error {
					if direction == patternDirAny {
						return appendUndirectedMatch(srcID, edgeID, dstID)
					}
					return appendMatch(srcID, edgeID, dstID)
				})
			})
		} else {
			err = state.Tx.ScanOutEdgeLinksByType(ctx, tenant, edgeType, 0, func(srcID, edgeID, dstID string) error {
				if direction == patternDirAny {
					return appendUndirectedMatch(srcID, edgeID, dstID)
				}
				return appendMatch(srcID, edgeID, dstID)
			})
		}
		if err != nil {
			return nil, err
		}
		if len(out) == 0 && optional {
			return []map[string]any{optionalNullBoundRow(row, edge, false, false)}, nil
		}
		return out, nil
	}

	switch direction {
	case patternDirOut:
		switch {
		case leftBound:
			return scanAndBind(leftID, edge.LeftVar, edge.RightVar, graph.EdgeDirectionOut, true, rightBound, rightID)
		case rightBound:
			return scanAndBind(rightID, edge.RightVar, edge.LeftVar, graph.EdgeDirectionIn, true, leftBound, leftID)
		default:
			return scanUnbound(true)
		}
	case patternDirIn:
		switch {
		case rightBound:
			return scanAndBind(rightID, edge.RightVar, edge.LeftVar, graph.EdgeDirectionOut, true, leftBound, leftID)
		case leftBound:
			return scanAndBind(leftID, edge.LeftVar, edge.RightVar, graph.EdgeDirectionIn, true, rightBound, rightID)
		default:
			return scanUnbound(false)
		}
	default:
		switch {
		case leftBound && rightBound:
			_ = indexedJoin
			return expandUndirectedBoundPairRowsByEdge(ctx, state, row, edge, pathVar, leftID, rightID, optional)
		case leftBound:
			return scanAndBind(leftID, edge.LeftVar, edge.RightVar, graph.EdgeDirectionAny, true, rightBound, rightID)
		case rightBound:
			return scanAndBind(rightID, edge.RightVar, edge.LeftVar, graph.EdgeDirectionAny, true, leftBound, leftID)
		default:
			return scanUnbound(!undirectedInFirst)
		}
	}
}

func expandUndirectedBoundPairRowsByEdge(ctx context.Context, state *State, row map[string]any, edge *EdgeMutation, pathVar string, leftID string, rightID string, optional bool) ([]map[string]any, error) {
	if state == nil || state.Tx == nil || edge == nil {
		if optional {
			return []map[string]any{optionalNullBoundRow(row, edge, true, true)}, nil
		}
		return nil, nil
	}
	tenant := strings.TrimSpace(state.Tenant)
	edgeTypeSet := projectionEdgeTypeSet(edge.Type)
	edgeTypeScanFilter := projectionEdgeTypeScanFilter(edge.Type)
	edgeExpectedProps := edgePatternExpectedProperties(edge.Pattern, state.Params, row, state)
	edgeType := strings.TrimSpace(edgeTypeScanFilter)
	edgeBound := false
	boundEdgeID := ""
	if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
		edgeBound, boundEdgeID = resolveBoundEntityID(row, edgeVar)
		boundEdgeID = strings.TrimSpace(boundEdgeID)
	}
	seen := map[string]struct{}{}
	edgeTypeCache := map[string]bool{}
	edgePropsCache := map[string]bool{}
	out := make([]map[string]any, 0)
	addRow := func(edgeID string) {
		next := cloneRow(row)
		if !bindPatternVarChecked(next, edge.LeftVar, leftID) || !bindPatternVarChecked(next, edge.RightVar, rightID) {
			return
		}
		edgeID = strings.TrimSpace(edgeID)
		if edgeID != "" {
			next["__edge.id"] = edgeID
			if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
				next[edgeVar] = edgeID
				next[edgeVar+".id"] = edgeID
			}
		}
		if strings.TrimSpace(pathVar) != "" {
			nodes := make([]any, 0, 2)
			if left, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(leftID)); err == nil && left != nil {
				nodes = append(nodes, clonePathVertex(left))
			} else {
				nodes = append(nodes, map[string]any{"id": strings.TrimSpace(leftID)})
			}
			if right, err := state.Tx.GetVertex(ctx, tenant, strings.TrimSpace(rightID)); err == nil && right != nil {
				nodes = append(nodes, clonePathVertex(right))
			} else {
				nodes = append(nodes, map[string]any{"id": strings.TrimSpace(rightID)})
			}
			rels := make([]any, 0, 1)
			if rel, err := state.Tx.GetEdge(ctx, tenant, edgeID); err == nil && rel != nil {
				rels = append(rels, clonePathEdge(rel))
			} else if edgeID != "" {
				rels = append(rels, map[string]any{"id": edgeID})
			}
			appendProjectionPathValue(next, pathVar, projectionPathValue{nodes: nodes, relationships: rels})
		}
		next["__ve_chain_cursor"] = strings.TrimSpace(rightID)
		out = append(out, next)
	}
	scanOutTo := func(srcID, dstID string) error {
		return state.Tx.ScanOutEdgeLinks(ctx, tenant, srcID, edgeType, 0, func(edgeID, peerID string) error {
			edgeID = strings.TrimSpace(edgeID)
			if edgeBound && edgeID != boundEdgeID {
				return nil
			}
			if len(edgeTypeSet) > 0 {
				if edgeID == "" {
					return nil
				}
				if allowed, ok := edgeTypeCache[edgeID]; ok {
					if !allowed {
						return nil
					}
				} else {
					found, err := state.Tx.GetEdge(ctx, tenant, edgeID)
					if err != nil || found == nil {
						edgeTypeCache[edgeID] = false
						return nil
					}
					_, allowed = edgeTypeSet[strings.ToUpper(strings.TrimSpace(found.Type))]
					edgeTypeCache[edgeID] = allowed
					if !allowed {
						return nil
					}
				}
			}
			if strings.TrimSpace(peerID) != dstID {
				return nil
			}
			if rowContainsEdgeID(row, edgeID) {
				return nil
			}
			if len(edgeExpectedProps) != 0 {
				if allowed, ok := edgePropsCache[edgeID]; ok {
					if !allowed {
						return nil
					}
				} else {
					found, err := state.Tx.GetEdge(ctx, tenant, edgeID)
					if err != nil || found == nil {
						edgePropsCache[edgeID] = false
						return nil
					}
					allowed := edgeHasExpectedProperties(found, edgeExpectedProps)
					edgePropsCache[edgeID] = allowed
					if !allowed {
						return nil
					}
				}
			}
			edgeID = strings.TrimSpace(edgeID)
			if edgeID != "" {
				if _, ok := seen[edgeID]; ok {
					return nil
				}
				seen[edgeID] = struct{}{}
			}
			addRow(edgeID)
			return nil
		})
	}
	if err := scanOutTo(leftID, rightID); err != nil {
		return nil, err
	}
	if err := scanOutTo(rightID, leftID); err != nil {
		return nil, err
	}
	if len(out) == 0 && optional {
		leftWasAlreadyBound, _ := resolveBoundEntityID(row, edge.LeftVar)
		rightWasAlreadyBound, _ := resolveBoundEntityID(row, edge.RightVar)
		return []map[string]any{optionalNullBoundRow(row, edge, leftWasAlreadyBound, rightWasAlreadyBound)}, nil
	}
	return out, nil
}

func expandExecutionHints(attrs map[string]any) (undirectedInFirst bool, indexedJoin bool) {
	variant := strings.ToLower(strings.TrimSpace(stringAttr(attrs, "variant")))
	accessPath := strings.ToLower(strings.TrimSpace(stringAttr(attrs, "accessPath")))
	joinStrategy := strings.ToLower(strings.TrimSpace(stringAttr(attrs, "joinStrategy")))

	if strings.Contains(variant, "in_first") || strings.Contains(accessPath, "_in_first") {
		undirectedInFirst = true
	}
	if joinStrategy == "indexed_bind_join" || strings.Contains(variant, "indexed") {
		indexedJoin = true
	}
	return undirectedInFirst, indexedJoin
}

func resolveBoundEntityID(row map[string]any, variable string) (bool, string) {
	variable = strings.TrimSpace(variable)
	if variable == "" || row == nil {
		return false, ""
	}
	if value, ok := row[variable]; ok {
		if value == nil {
			return true, ""
		}
		if id := resolveBoundEntityIDFromValue(value); id != "" {
			return true, id
		}
	}
	if value, ok := row[variable+".id"]; ok {
		if value == nil {
			return true, ""
		}
		if id := resolveBoundEntityIDFromValue(value); id != "" {
			return true, id
		}
	}
	return false, ""
}

func resolveBoundEntityIDFromValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case map[string]any:
		if id := strings.TrimSpace(scalarString(typed["id"])); id != "" {
			return id
		}
		if id := strings.TrimSpace(scalarString(typed["ID"])); id != "" {
			return id
		}
		return ""
	case map[string]string:
		if id := strings.TrimSpace(typed["id"]); id != "" {
			return id
		}
		if id := strings.TrimSpace(typed["ID"]); id != "" {
			return id
		}
		return ""
	default:
		return strings.TrimSpace(scalarString(value))
	}
}

func resolveBoundEntityIDFromListComprehensionItem(value any) string {
	switch value.(type) {
	case *graph.Vertex, *graph.Edge, map[string]any, map[string]string:
		return resolveBoundEntityIDFromValue(value)
	default:
		return ""
	}
}

func bindPatternVar(row map[string]any, variable, id string) {
	variable = strings.TrimSpace(variable)
	id = strings.TrimSpace(id)
	if row == nil || variable == "" || id == "" {
		return
	}
	row[variable] = id
	row[variable+".id"] = id
}

func resolveBoundEdgeSequenceIDList(row map[string]any, variable string) ([]string, bool) {
	variable = strings.TrimSpace(variable)
	if row == nil || variable == "" {
		return nil, false
	}
	value, ok := row[variable]
	if !ok {
		value, ok = row[variable+".id"]
		if !ok {
			return nil, false
		}
	}
	switch typed := value.(type) {
	case []*graph.Edge:
		ids := make([]string, 0, len(typed))
		for _, edge := range typed {
			if edge == nil {
				return nil, false
			}
			ids = append(ids, strings.TrimSpace(edge.ID))
		}
		return ids, true
	case []any:
		ids := make([]string, 0, len(typed))
		for _, item := range typed {
			switch edge := item.(type) {
			case *graph.Edge:
				if edge == nil {
					return nil, false
				}
				ids = append(ids, strings.TrimSpace(edge.ID))
			case map[string]any:
				ids = append(ids, strings.TrimSpace(fmt.Sprint(edge["id"])))
			default:
				ids = append(ids, strings.TrimSpace(fmt.Sprint(item)))
			}
		}
		return ids, true
	case []string:
		ids := make([]string, 0, len(typed))
		for _, id := range typed {
			ids = append(ids, strings.TrimSpace(id))
		}
		return ids, true
	case *graph.Edge:
		if typed == nil {
			return nil, false
		}
		return []string{strings.TrimSpace(typed.ID)}, true
	case map[string]any:
		if id := strings.TrimSpace(fmt.Sprint(typed["id"])); id != "" {
			return []string{id}, true
		}
		return nil, false
	default:
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			return []string{id}, true
		}
		return nil, false
	}
}

func bindPatternVarChecked(row map[string]any, variable, id string) bool {
	variable = strings.TrimSpace(variable)
	id = strings.TrimSpace(id)
	if variable == "" || id == "" {
		return true
	}
	if bound, existingID := resolveBoundEntityID(row, variable); bound && strings.TrimSpace(existingID) != id {
		return false
	}
	bindPatternVar(row, variable, id)
	return true
}

func patternLabelOrderRowKey(variable string) string {
	variable = strings.TrimSpace(variable)
	if variable == "" {
		return ""
	}
	return "__ve_label_order." + variable
}

func setPatternLabelOrder(row map[string]any, variable string, labels []string) {
	if row == nil {
		return
	}
	key := patternLabelOrderRowKey(variable)
	if key == "" || len(labels) == 0 {
		return
	}
	if _, exists := row[key]; exists {
		return
	}
	out := make([]string, 0, len(labels))
	seen := map[string]struct{}{}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	if len(out) == 0 {
		return
	}
	row[key] = out
}

func readPatternLabelOrder(row map[string]any, variable string) []string {
	if row == nil {
		return nil
	}
	key := patternLabelOrderRowKey(variable)
	if key == "" {
		return nil
	}
	raw, ok := row[key]
	if !ok || raw == nil {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, label := range typed {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			out = append(out, label)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			label := strings.TrimSpace(scalarString(item))
			if label == "" {
				continue
			}
			out = append(out, label)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	default:
		label := strings.TrimSpace(scalarString(raw))
		if label == "" {
			return nil
		}
		return []string{label}
	}
}

func reorderLabelsByPreferredOrder(labels []string, preferred []string) []string {
	if len(labels) == 0 {
		return nil
	}
	if len(preferred) == 0 {
		return append([]string(nil), labels...)
	}

	remaining := map[string]int{}
	for _, label := range labels {
		remaining[label]++
	}
	out := make([]string, 0, len(labels))
	for _, want := range preferred {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}
		if remaining[want] > 0 {
			out = append(out, want)
			remaining[want]--
		}
	}
	for _, label := range labels {
		if remaining[label] > 0 {
			out = append(out, label)
			remaining[label]--
		}
	}
	return out
}

func rowContainsEdgeID(row map[string]any, edgeID string) bool {
	edgeID = strings.TrimSpace(edgeID)
	if row == nil || edgeID == "" {
		return false
	}
	if strings.TrimSpace(scalarString(row["__edge.id"])) == edgeID {
		return true
	}
	for _, value := range row {
		if projectionValueContainsEdgeID(value, edgeID) {
			return true
		}
	}
	return false
}

func projectionValueContainsEdgeID(value any, edgeID string) bool {
	switch typed := value.(type) {
	case projectionPathValue:
		for _, rel := range typed.relationships {
			if projectionRelationshipID(rel) == edgeID {
				return true
			}
		}
	case map[string]any:
		relsRaw, ok := typed["relationships"]
		if !ok {
			return false
		}
		rels, ok := relsRaw.([]any)
		if !ok {
			return false
		}
		for _, rel := range rels {
			if projectionRelationshipID(rel) == edgeID {
				return true
			}
		}
	}
	return false
}

func projectionRelationshipID(value any) string {
	switch typed := value.(type) {
	case *graph.Edge:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case map[string]any:
		return strings.TrimSpace(scalarString(typed["id"]))
	default:
		return strings.TrimSpace(scalarString(value))
	}
}

func optionalNullBoundRow(row map[string]any, edge *EdgeMutation, leftAlreadyBound bool, rightAlreadyBound bool) map[string]any {
	next := cloneRow(row)
	if edge == nil {
		return next
	}
	next["__edge.id"] = nil
	if edgeVar := strings.TrimSpace(edge.Var); edgeVar != "" {
		if edgeBound, _ := resolveBoundEntityID(row, edgeVar); !edgeBound {
			next[edgeVar] = nil
			next[edgeVar+".id"] = nil
		}
	}
	if variable := strings.TrimSpace(edge.LeftVar); variable != "" && !leftAlreadyBound {
		next[variable] = nil
		next[variable+".id"] = nil
	}
	if variable := strings.TrimSpace(edge.RightVar); variable != "" && !rightAlreadyBound {
		next[variable] = nil
		next[variable+".id"] = nil
	}
	return next
}

type projectHandler struct{ baseHandler }

func NewProjectHandler() Handler { return projectHandler{baseHandler{op: "PHY_PROJECT"}} }

type filterHandler struct{ baseHandler }

func NewFilterHandler() Handler { return filterHandler{baseHandler{op: "PHY_FILTER"}} }

func (h filterHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil || len(state.Rows) == 0 {
		return nil
	}
	predicate := strings.TrimSpace(stringAttr(attrs, "predicate"))
	if predicate == "" {
		predicate = strings.TrimSpace(stringAttr(attrs, "where"))
	}
	if predicate == "" {
		return nil
	}

	out := make([]map[string]any, 0, len(state.Rows))
	for _, row := range state.Rows {
		if row == nil {
			row = map[string]any{}
		}
		value, ok, err := resolveProjectionExprValueChecked(predicate, row, state)
		if err != nil {
			return err
		}
		if !ok || value == nil {
			continue
		}
		matched, boolOK := value.(bool)
		if !boolOK || !matched {
			continue
		}
		out = append(out, row)
	}
	state.Rows = out
	return nil
}

func (h projectHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	items := stringSliceAttr(attrs, "items")
	if strings.EqualFold(strings.TrimSpace(stringAttr(attrs, "kind")), "UNWIND") {
		state.SortSourceRows = append([]map[string]any(nil), state.Rows...)
		state.Rows = projectUnwindRows(items, state.Rows, state)
		if err := projectionEvalError(state); err != nil {
			return err
		}
		return nil
	}
	if len(state.Rows) == 0 && len(state.ExecutedOps) == 1 {
		state.Rows = []map[string]any{{}}
	}
	if len(state.Rows) == 0 && len(items) > 0 {
		if aggregated, ok := tryProjectPureAggregate(items, state.Rows, state); ok {
			if err := projectionEvalError(state); err != nil {
				return err
			}
			state.SortSourceRows = nil
			state.Rows = []map[string]any{aggregated}
			return nil
		}
	}
	if len(state.Rows) == 0 {
		return nil
	}
	in := state.Rows
	whereExpr, hasWhereExpr, whereErr := parseProjectWhereExprFromAttrs(attrs)
	if whereErr != nil {
		return whereErr
	}
	if len(items) > 0 {
		if aggregated, ok := tryProjectPureAggregate(items, in, state); ok {
			if err := projectionEvalError(state); err != nil {
				return err
			}
			state.SortSourceRows = append([]map[string]any(nil), in...)
			state.Rows = []map[string]any{aggregated}
			return nil
		}
		distinct := boolAttr(attrs, "distinct")
		if aggregatedRows, sortSourceRows, ok := tryProjectGroupedAggregate(items, in, state, distinct, hasWhereExpr, whereExpr); ok {
			if err := projectionEvalError(state); err != nil {
				return err
			}
			state.SortSourceRows = sortSourceRows
			state.Rows = aggregatedRows
			return nil
		}
		if err := projectionEvalError(state); err != nil {
			return err
		}
	}
	out := make([]map[string]any, 0, len(in))
	distinct := boolAttr(attrs, "distinct")
	seen := map[string]struct{}{}
	for _, row := range in {
		if row == nil {
			row = map[string]any{}
		}
		if len(items) == 0 {
			next := cloneRow(row)
			if distinct {
				key := projectionDistinctRowKeyObserved(state, next, "runtime.operator.project.distinct.row_key")
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
			}
			out = append(out, next)
			continue
		}
		projected := map[string]any{}
		for _, item := range items {
			expr, alias := parseProjectionSpec(item)
			if expr == "*" {
				vars := map[string]struct{}{}
				for key := range row {
					if strings.HasPrefix(key, "__") {
						continue
					}
					if strings.Contains(key, ".") {
						if strings.HasSuffix(key, ".id") {
							base := strings.TrimSpace(strings.TrimSuffix(key, ".id"))
							if isIdentifierToken(base) {
								vars[base] = struct{}{}
							}
						}
						continue
					}
					if isIdentifierToken(key) {
						vars[key] = struct{}{}
					}
				}
				ordered := make([]string, 0, len(vars))
				for name := range vars {
					ordered = append(ordered, name)
				}
				sort.Strings(ordered)
				for _, name := range ordered {
					value, ok := row[name]
					if !ok {
						if idValue, idOK := row[name+".id"]; idOK {
							value = idValue
							ok = true
						}
					}
					if !ok {
						value = nil
					}
					if hydrated, hydratedOK := hydrateProjectionIdentifierValue(name, value, row, state); hydratedOK {
						value = hydrated
					}
					projected[name] = value
				}
				continue
			}
			value, ok, err := resolveProjectionExprValueChecked(expr, row, state)
			if err != nil {
				return err
			}
			if !ok && alias != "" {
				value, ok = row[alias]
			}
			if !ok {
				value = nil
			}
			if alias != "" {
				projected[alias] = value
				if isIdentifierToken(expr) {
					copyProjectionEntityBindings(projected, row, expr, alias)
				}
			} else {
				projected[expr] = value
				if isIdentifierToken(expr) {
					copyProjectionEntityBindings(projected, row, expr, expr)
				}
			}
		}
		if distinct {
			key := projectionDistinctRowKeyObserved(state, projected, "runtime.operator.project.distinct.row_key")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		whereRow := mergeProjectionWhereScope(row, projected)
		if hasWhereExpr && !evaluateProjectWhereExpr(whereExpr, whereRow, state) {
			if err := projectionEvalError(state); err != nil {
				return err
			}
			continue
		}
		out = append(out, projected)
	}
	state.SortSourceRows = append([]map[string]any(nil), in...)
	state.Rows = out
	return nil
}

func copyProjectionEntityBindings(dst map[string]any, src map[string]any, sourceVar string, targetVar string) {
	sourceVar = strings.TrimSpace(sourceVar)
	targetVar = strings.TrimSpace(targetVar)
	if dst == nil || src == nil || sourceVar == "" || targetVar == "" {
		return
	}
	prefix := sourceVar + "."
	targetPrefix := targetVar + "."
	for key, value := range src {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		suffix := strings.TrimSpace(strings.TrimPrefix(key, prefix))
		if suffix == "" {
			continue
		}
		dst[targetPrefix+suffix] = value
	}
}

type projectAggregateItem struct {
	expr  string
	alias string
	isAgg bool
}

type projectionSpan struct {
	start int
	end   int
}

func tryProjectGroupedAggregate(items []string, rows []map[string]any, state *State, distinct bool, hasWhereExpr bool, whereExpr projectWhereExpr) ([]map[string]any, []map[string]any, bool) {
	if len(items) == 0 {
		return nil, nil, false
	}
	parsed := make([]projectAggregateItem, 0, len(items))
	hasAgg := false
	hasNonAgg := false
	for _, item := range items {
		expr, alias := parseProjectionSpec(item)
		isAgg := projectionExprHasAggregateCall(expr)
		if isAgg {
			hasAgg = true
		} else {
			hasNonAgg = true
		}
		parsed = append(parsed, projectAggregateItem{expr: expr, alias: alias, isAgg: isAgg})
	}
	if !hasAgg || !hasNonAgg {
		return nil, nil, false
	}

	type aggregateGroup struct {
		groupRow map[string]any
		rows     []map[string]any
	}
	groups := map[string]*aggregateGroup{}
	order := make([]string, 0)

	for _, row := range rows {
		if row == nil {
			row = map[string]any{}
		}
		groupRow := map[string]any{}
		for _, item := range parsed {
			if item.isAgg {
				continue
			}
			value, ok, err := resolveProjectionExprValueChecked(item.expr, row, state)
			if err != nil {
				projectionSetEvalError(state, err)
				return nil, nil, false
			}
			if !ok && item.alias != "" {
				value, ok = row[item.alias]
			}
			if !ok {
				value = nil
			}
			if item.alias != "" {
				groupRow[item.alias] = value
				groupRow[item.expr] = value
			} else {
				groupRow[item.expr] = value
			}
		}
		key := projectionDistinctRowKeyObserved(state, groupRow, "runtime.operator.project.group.row_key")
		group, exists := groups[key]
		if !exists {
			group = &aggregateGroup{groupRow: groupRow, rows: make([]map[string]any, 0, 1)}
			groups[key] = group
			order = append(order, key)
		}
		group.rows = append(group.rows, row)
	}

	out := make([]map[string]any, 0, len(order))
	sortSource := make([]map[string]any, 0, len(order))
	seen := map[string]struct{}{}
	for _, key := range order {
		group := groups[key]
		projected := cloneRow(group.groupRow)
		baseRow := mergeProjectionWhereScope(nil, group.groupRow)
		if len(group.rows) > 0 {
			baseRow = mergeProjectionWhereScope(group.rows[0], projected)
		}
		for _, item := range parsed {
			if !item.isAgg {
				continue
			}
			aggValue, ok := evalProjectionExprWithAggregates(item.expr, group.rows, baseRow, state)
			if !ok {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
				return nil, nil, false
			}
			if item.alias != "" {
				projected[item.alias] = aggValue
			} else {
				projected[item.expr] = aggValue
			}
		}
		if distinct {
			outKey := projectionDistinctRowKeyObserved(state, projected, "runtime.operator.project.distinct.row_key")
			if _, ok := seen[outKey]; ok {
				continue
			}
			seen[outKey] = struct{}{}
		}
		whereBase := map[string]any{}
		if len(group.rows) > 0 {
			whereBase = group.rows[0]
		}
		whereRow := mergeProjectionWhereScope(whereBase, projected)
		if hasWhereExpr && !evaluateProjectWhereExpr(whereExpr, whereRow, state) {
			continue
		}
		out = append(out, projected)
		sortSource = append(sortSource, group.rows[0])
	}

	return out, sortSource, true
}

func mergeProjectionWhereScope(base map[string]any, projected map[string]any) map[string]any {
	if len(base) == 0 {
		return projected
	}
	merged := cloneRow(base)
	if merged == nil {
		merged = map[string]any{}
	}
	for key, value := range projected {
		merged[key] = value
	}
	return merged
}

func projectUnwindRows(items []string, input []map[string]any, state *State) []map[string]any {
	if len(input) == 0 {
		if state != nil && len(state.ExecutedOps) == 1 {
			input = []map[string]any{{}}
		} else {
			return []map[string]any{}
		}
	}
	if len(items) != 1 {
		return []map[string]any{}
	}
	expr, alias := parseProjectionSpec(items[0])
	if strings.TrimSpace(expr) == "" {
		return []map[string]any{}
	}
	if alias == "" {
		alias = strings.TrimSpace(expr)
	}
	if alias == "" {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(input))
	for _, row := range input {
		if row == nil {
			row = map[string]any{}
		}
		value, ok, err := resolveProjectionExprValueChecked(expr, row, state)
		if err != nil {
			projectionSetEvalError(state, err)
			return []map[string]any{}
		}
		if !ok {
			value = nil
		}
		if value == nil {
			continue
		}
		if list, ok := projectionListValue(value); ok {
			if len(list) == 0 {
				continue
			}
			for _, item := range list {
				next := cloneRow(row)
				next[alias] = item
				out = append(out, next)
			}
			continue
		}
		next := cloneRow(row)
		next[alias] = value
		out = append(out, next)
	}
	return out
}

func tryProjectPureAggregate(items []string, rows []map[string]any, state *State) (map[string]any, bool) {
	if len(items) == 0 {
		return nil, false
	}
	result := map[string]any{}
	hasAgg := false
	hasNonAgg := false
	for _, item := range items {
		expr, alias := parseProjectionSpec(item)
		if projectionExprHasAggregateCall(expr) {
			hasAgg = true
		} else {
			hasNonAgg = true
		}
		aggValue, ok := evalProjectionExprWithAggregates(expr, rows, nil, state)
		if !ok {
			return nil, false
		}
		if alias != "" {
			result[alias] = aggValue
		} else {
			result[expr] = aggValue
		}
	}
	if !hasAgg || hasNonAgg {
		return nil, false
	}
	return result, true
}

func evalProjectionExprWithAggregates(expr string, rows []map[string]any, baseRow map[string]any, state *State) (any, bool) {
	rewritten := strings.TrimSpace(expr)
	aggValues := map[string]any{}
	idx := 0
	for {
		spans := findProjectionAggregateCallSpans(rewritten)
		if len(spans) == 0 {
			break
		}
		span := spans[0]
		for _, candidate := range spans[1:] {
			if candidate.start > span.start {
				span = candidate
			}
		}
		if span.start < 0 || span.end > len(rewritten) || span.start >= span.end {
			return nil, false
		}
		callExpr := strings.TrimSpace(rewritten[span.start:span.end])
		value, ok := evalProjectionAggregateExpr(callExpr, rows, state)
		if !ok {
			return nil, false
		}
		placeholder := fmt.Sprintf("__ve_agg_%d", idx)
		aggValues[placeholder] = value
		rewritten = rewritten[:span.start] + placeholder + rewritten[span.end:]
		idx++
	}
	evalRow := cloneRow(baseRow)
	if evalRow == nil {
		evalRow = map[string]any{}
	}
	for key, value := range aggValues {
		evalRow[key] = value
	}
	value, ok := resolveProjectionExprValue(rewritten, evalRow, state)
	if !ok {
		return nil, false
	}
	return value, true
}

func projectionExprHasAggregateCall(expr string) bool {
	return len(findProjectionAggregateCallSpans(expr)) > 0
}

func findProjectionAggregateCallSpans(expr string) []projectionSpan {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return nil
	}
	spans := make([]projectionSpan, 0, 2)
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(raw); {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				if quoteIsEscaped(raw, i) {
					i++
					continue
				}
				inSingle = !inSingle
			}
			i++
			continue
		case '"':
			if !inSingle && !inBacktick {
				if quoteIsEscaped(raw, i) {
					i++
					continue
				}
				inDouble = !inDouble
			}
			i++
			continue
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
			i++
			continue
		}
		if inSingle || inDouble || inBacktick {
			i++
			continue
		}
		if !((raw[i] >= 'a' && raw[i] <= 'z') || (raw[i] >= 'A' && raw[i] <= 'Z') || raw[i] == '_') {
			i++
			continue
		}
		start := i
		for i < len(raw) && ((raw[i] >= 'a' && raw[i] <= 'z') || (raw[i] >= 'A' && raw[i] <= 'Z') || (raw[i] >= '0' && raw[i] <= '9') || raw[i] == '_') {
			i++
		}
		name := strings.ToLower(strings.TrimSpace(raw[start:i]))
		j := i
		for j < len(raw) && unicode.IsSpace(rune(raw[j])) {
			j++
		}
		if j >= len(raw) || raw[j] != '(' {
			continue
		}
		if !isProjectionAggregateFunctionName(name) {
			continue
		}
		close := findProjectionMatchingParen(raw, j)
		if close < 0 {
			continue
		}
		spans = append(spans, projectionSpan{start: start, end: close + 1})
	}
	return spans
}

func isProjectionAggregateFunctionName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "count", "collect", "collect_list", "sum", "avg", "min", "max", "percentiledisc", "percentilecont", "percentile_disc", "percentile_cont", "stdev", "stdevp", "stdev_samp", "stdev_pop":
		return true
	default:
		return false
	}
}

func findProjectionMatchingParen(raw string, open int) int {
	if open < 0 || open >= len(raw) || raw[open] != '(' {
		return -1
	}
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := open; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				if quoteIsEscaped(raw, i) {
					continue
				}
				inSingle = !inSingle
			}
			continue
		case '"':
			if !inSingle && !inBacktick {
				if quoteIsEscaped(raw, i) {
					continue
				}
				inDouble = !inDouble
			}
			continue
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
			continue
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
			if depth < 0 {
				return -1
			}
		}
	}
	return -1
}

func evalProjectionAggregateExpr(expr string, rows []map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "count(") && strings.HasSuffix(lower, ")") {
		arg, distinct := parseProjectionAggregateDistinctArg(strings.TrimSpace(raw[len("count(") : len(raw)-1]))
		if arg == "" || arg == "*" {
			if !distinct {
				return len(rows), true
			}
			seen := make(map[string]struct{}, len(rows))
			for _, row := range rows {
				seen[projectionDistinctRowKeyObserved(state, row, "runtime.operator.aggregate.count_distinct_star.row_key")] = struct{}{}
			}
			return len(seen), true
		}
		count := 0
		var seen map[string]struct{}
		if distinct {
			seen = make(map[string]struct{}, len(rows))
		}
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionAggregateArgValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			if distinct {
				key := projectionDistinctValueKey(value)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
			}
			count++
		}
		return count, true
	}
	if (strings.HasPrefix(lower, "collect(") || strings.HasPrefix(lower, "collect_list(")) && strings.HasSuffix(lower, ")") {
		prefix := "collect("
		if strings.HasPrefix(lower, "collect_list(") {
			prefix = "collect_list("
		}
		arg, distinct := parseProjectionAggregateDistinctArg(strings.TrimSpace(raw[len(prefix) : len(raw)-1]))
		if arg == "" {
			return []any{}, true
		}
		values := make([]any, 0, len(rows))
		var seen map[string]struct{}
		if distinct {
			seen = make(map[string]struct{}, len(rows))
		}
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionAggregateArgValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			if distinct {
				key := projectionDistinctValueKey(value)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
			}
			values = append(values, value)
		}
		return values, true
	}
	if (strings.HasPrefix(lower, "stdev(") || strings.HasPrefix(lower, "stdev_samp(")) && strings.HasSuffix(lower, ")") {
		prefix := "stDev("
		if strings.HasPrefix(lower, "stdev_samp(") {
			prefix = "stdev_samp("
		}
		arg := strings.TrimSpace(raw[len(prefix) : len(raw)-1])
		values := make([]float64, 0, len(rows))
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			numeric, ok := projectionNumericToFloat(value)
			if !ok {
				continue
			}
			values = append(values, numeric)
		}
		if len(values) < 2 {
			return nil, true
		}
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))
		sumSquares := 0.0
		for _, v := range values {
			delta := v - mean
			sumSquares += delta * delta
		}
		return math.Sqrt(sumSquares / float64(len(values)-1)), true
	}
	if (strings.HasPrefix(lower, "stdevp(") || strings.HasPrefix(lower, "stdev_pop(")) && strings.HasSuffix(lower, ")") {
		prefix := "stDevP("
		if strings.HasPrefix(lower, "stdev_pop(") {
			prefix = "stdev_pop("
		}
		arg := strings.TrimSpace(raw[len(prefix) : len(raw)-1])
		values := make([]float64, 0, len(rows))
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			numeric, ok := projectionNumericToFloat(value)
			if !ok {
				continue
			}
			values = append(values, numeric)
		}
		if len(values) == 0 {
			return nil, true
		}
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))
		sumSquares := 0.0
		for _, v := range values {
			delta := v - mean
			sumSquares += delta * delta
		}
		return math.Sqrt(sumSquares / float64(len(values))), true
	}
	if strings.HasPrefix(lower, "sum(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(raw[len("sum(") : len(raw)-1])
		total := 0.0
		hasValue := false
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			num, ok := projectionNumericToFloat(value)
			if !ok {
				continue
			}
			total += num
			hasValue = true
		}
		if !hasValue {
			return nil, true
		}
		if math.Trunc(total) == total {
			if int64(total) == int64(int(total)) {
				return int(total), true
			}
			return int64(total), true
		}
		return total, true
	}
	if strings.HasPrefix(lower, "avg(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(raw[len("avg(") : len(raw)-1])
		total := 0.0
		count := 0
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			num, ok := projectionNumericToFloat(value)
			if !ok {
				continue
			}
			total += num
			count++
		}
		if count == 0 {
			return nil, true
		}
		return total / float64(count), true
	}
	if strings.HasPrefix(lower, "min(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(raw[len("min(") : len(raw)-1])
		var best any
		hasValue := false
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			if !hasValue || compareScalarValues(value, best) < 0 {
				best = value
				hasValue = true
			}
		}
		if !hasValue {
			return nil, true
		}
		return best, true
	}
	if strings.HasPrefix(lower, "max(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(raw[len("max(") : len(raw)-1])
		var best any
		hasValue := false
		for _, row := range rows {
			if row == nil {
				continue
			}
			value, ok := resolveProjectionExprValue(arg, row, state)
			if !ok || value == nil {
				continue
			}
			if !hasValue || compareScalarValues(value, best) > 0 {
				best = value
				hasValue = true
			}
		}
		if !hasValue {
			return nil, true
		}
		return best, true
	}
	if strings.HasPrefix(lower, "percentiledisc(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(raw[len("percentileDisc(") : len(raw)-1])
		return projectionPercentileAggregateValue(argList, rows, state, true)
	}
	if strings.HasPrefix(lower, "percentile_disc(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(raw[len("percentile_disc(") : len(raw)-1])
		return projectionPercentileAggregateValue(argList, rows, state, true)
	}
	if strings.HasPrefix(lower, "percentilecont(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(raw[len("percentileCont(") : len(raw)-1])
		return projectionPercentileAggregateValue(argList, rows, state, false)
	}
	if strings.HasPrefix(lower, "percentile_cont(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(raw[len("percentile_cont(") : len(raw)-1])
		return projectionPercentileAggregateValue(argList, rows, state, false)
	}
	return nil, false
}

func parseProjectionAggregateDistinctArg(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= len("DISTINCT") && strings.EqualFold(raw[:len("DISTINCT")], "DISTINCT") {
		return strings.TrimSpace(raw[len("DISTINCT"):]), true
	}
	return raw, false
}

func resolveProjectionAggregateArgValue(arg string, row map[string]any, state *State) (any, bool) {
	if isSimpleOrderingReferenceExpression(arg) {
		if value, exists := valueFromRowWithPresence(row, arg); exists {
			if hydrated, hydratedOK := hydrateProjectionIdentifierValue(arg, value, row, state); hydratedOK {
				return hydrated, true
			}
			return value, true
		}
	}
	return resolveProjectionExprValue(arg, row, state)
}

func projectionPercentileAggregateValue(argList string, rows []map[string]any, state *State, discrete bool) (any, bool) {
	parts := splitProjectionTopLevelByComma(argList)
	if len(parts) != 2 {
		return nil, false
	}
	valueExpr := strings.TrimSpace(parts[0])
	percentileExpr := strings.TrimSpace(parts[1])
	if valueExpr == "" || percentileExpr == "" {
		return nil, false
	}

	percentileValueResolved := false
	percentile := 0.0
	values := make([]float64, 0, len(rows))

	for _, row := range rows {
		if row == nil {
			continue
		}
		if !percentileValueResolved {
			pValue, ok := resolveProjectionExprValue(percentileExpr, row, state)
			if !ok {
				return nil, false
			}
			if pValue == nil {
				return nil, true
			}
			pNumeric, ok := projectionNumericToFloat(pValue)
			if !ok {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
				return nil, true
			}
			if pNumeric < 0 || pNumeric > 1 {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "NumberOutOfRange", nil))
				return nil, true
			}
			percentile = pNumeric
			percentileValueResolved = true
		}

		value, ok := resolveProjectionExprValue(valueExpr, row, state)
		if !ok || value == nil {
			continue
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok {
			continue
		}
		values = append(values, numeric)
	}

	if !percentileValueResolved || len(values) == 0 {
		return nil, true
	}

	sort.Float64s(values)
	if discrete {
		if percentile == 0 {
			return values[0], true
		}
		idx := int(math.Ceil(percentile*float64(len(values)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(values) {
			idx = len(values) - 1
		}
		return values[idx], true
	}

	position := percentile * float64(len(values)-1)
	lowerIdx := int(math.Floor(position))
	upperIdx := int(math.Ceil(position))
	if lowerIdx < 0 {
		lowerIdx = 0
	}
	if upperIdx >= len(values) {
		upperIdx = len(values) - 1
	}
	if lowerIdx == upperIdx {
		return values[lowerIdx], true
	}
	weight := position - float64(lowerIdx)
	return values[lowerIdx] + (values[upperIdx]-values[lowerIdx])*weight, true
}

func parseProjectWhereExprFromAttrs(attrs map[string]any) (projectWhereExpr, bool, error) {
	raw := strings.TrimSpace(stringAttr(attrs, "where"))
	if raw == "" {
		return projectWhereExpr{}, false, nil
	}
	raw = stripLeadingProjectWhereKeyword(raw)
	expr, ok := parseProjectWhereExpr(raw)
	if !ok || (expr.root == nil && len(expr.atoms) == 0) {
		return projectWhereExpr{rawExpr: raw, useRawExpr: true}, true, nil
	}
	return expr, true, nil
}

func stripLeadingProjectWhereKeyword(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "WHERE ") {
		return strings.TrimSpace(raw[len("WHERE"):])
	}
	if upper == "WHERE" {
		return ""
	}
	return raw
}

func evaluateProjectWhereExpr(expr projectWhereExpr, row map[string]any, state *State) bool {
	if expr.useRawExpr {
		return evaluateProjectWhereRawExpr(expr.rawExpr, row, state)
	}
	if expr.root != nil {
		return evalProjectWhereNode(expr.root, row, state)
	}
	if len(expr.atoms) == 0 {
		return true
	}
	matches := !expr.useOrLogic
	for _, atom := range expr.atoms {
		left := projectWhereLeftValue(strings.TrimSpace(atom.leftName), row, state)
		right, ok := projectWhereValue(atom, state)
		if !ok {
			return false
		}
		atomMatches := projectCompareWhere(left, atom.op, right)
		if expr.useOrLogic {
			if atomMatches {
				matches = true
				break
			}
			continue
		}
		if !atomMatches {
			matches = false
			break
		}
	}
	return matches
}

func evaluateProjectWhereRawExpr(raw string, row map[string]any, state *State) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	value, ok, err := resolveProjectionExprValueChecked(raw, row, state)
	if err != nil {
		projectionSetEvalError(state, err)
		return false
	}
	if !ok || value == nil {
		return false
	}
	matched, ok := value.(bool)
	if !ok {
		return false
	}
	return matched
}

func evalProjectWhereNode(node *projectWhereNode, row map[string]any, state *State) bool {
	if node == nil {
		return false
	}
	if node.atom != nil {
		left := projectWhereLeftValue(strings.TrimSpace(node.atom.leftName), row, state)
		right, ok := projectWhereValue(*node.atom, state)
		if !ok {
			return false
		}
		return projectCompareWhere(left, node.atom.op, right)
	}
	switch node.op {
	case "AND":
		return evalProjectWhereNode(node.left, row, state) && evalProjectWhereNode(node.right, row, state)
	case "OR":
		return evalProjectWhereNode(node.left, row, state) || evalProjectWhereNode(node.right, row, state)
	default:
		return false
	}
}

func projectWhereLeftValue(leftName string, row map[string]any, state *State) any {
	leftName = strings.TrimSpace(leftName)
	if leftName == "" {
		return nil
	}
	if row != nil {
		if value, ok := row[leftName]; ok {
			return value
		}
	}
	if value, ok := resolveProjectionExprValue(leftName, row, state); ok {
		return value
	}
	return nil
}

func parseProjectWhereExpr(raw string) (projectWhereExpr, bool) {
	raw = stripProjectWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return projectWhereExpr{}, false
	}
	node, ok := parseProjectWhereNode(raw)
	if !ok || node == nil {
		return projectWhereExpr{}, false
	}
	atoms := collectProjectWhereAtoms(node)
	if len(atoms) == 0 {
		return projectWhereExpr{}, false
	}
	logicToken, uniform := projectWhereNodeUniformLogic(node)
	return projectWhereExpr{atoms: atoms, useOrLogic: uniform && logicToken == "OR", logicToken: logicToken, root: node}, true
}

func parseProjectWhereNode(raw string) (*projectWhereNode, bool) {
	raw = stripProjectWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return nil, false
	}
	if parts := splitProjectWhereBoolean(raw, "OR"); len(parts) > 1 {
		var root *projectWhereNode
		for _, part := range parts {
			node, ok := parseProjectWhereNode(part)
			if !ok {
				return nil, false
			}
			if root == nil {
				root = node
				continue
			}
			root = &projectWhereNode{op: "OR", left: root, right: node}
		}
		return root, true
	}
	return parseProjectWhereAndNode(raw)
}

func parseProjectWhereAndNode(raw string) (*projectWhereNode, bool) {
	raw = stripProjectWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return nil, false
	}
	if parts := splitProjectWhereBoolean(raw, "AND"); len(parts) > 1 {
		var root *projectWhereNode
		for _, part := range parts {
			node, ok := parseProjectWhereNode(part)
			if !ok {
				return nil, false
			}
			if root == nil {
				root = node
				continue
			}
			root = &projectWhereNode{op: "AND", left: root, right: node}
		}
		return root, true
	}

	m := projectWhereSimpleRe.FindStringSubmatch(raw)
	if len(m) != 4 {
		m = projectWhereStringRe.FindStringSubmatch(raw)
	}
	if len(m) != 4 {
		nullMatch := projectWhereNullRe.FindStringSubmatch(raw)
		if len(nullMatch) == 3 {
			lhs := strings.TrimSpace(nullMatch[1])
			op := normalizeProjectWhereOp(nullMatch[2])
			if lhs == "" || !isProjectWhereIdentifier(lhs) {
				return nil, false
			}
			atom := projectWhereAtom{leftName: lhs, op: op}
			return &projectWhereNode{atom: &atom}, true
		}
		return nil, false
	}

	lhs := strings.TrimSpace(m[1])
	op := normalizeProjectWhereOp(m[2])
	rhs := strings.TrimSpace(m[3])
	if lhs == "" || rhs == "" || !isProjectWhereIdentifier(lhs) {
		return nil, false
	}
	if strings.HasPrefix(rhs, "$") {
		if !isProjectWhereIdentifier(rhs[1:]) {
			return nil, false
		}
		atom := projectWhereAtom{leftName: lhs, op: op, rightParamName: rhs[1:]}
		return &projectWhereNode{atom: &atom}, true
	}
	if isProjectStringWhereOp(op) && !isQuotedLiteral(rhs) {
		return nil, false
	}
	if isProjectWhereIdentifier(rhs) && !strings.EqualFold(rhs, "true") && !strings.EqualFold(rhs, "false") && !strings.EqualFold(rhs, "null") {
		return nil, false
	}
	rightValue, ok := parseProjectWhereLiteralValue(rhs)
	if !ok {
		return nil, false
	}
	atom := projectWhereAtom{leftName: lhs, op: op, rightAny: rightValue}
	return &projectWhereNode{atom: &atom}, true
}

func collectProjectWhereAtoms(node *projectWhereNode) []projectWhereAtom {
	if node == nil {
		return nil
	}
	out := []projectWhereAtom{}
	var walk func(*projectWhereNode)
	walk = func(current *projectWhereNode) {
		if current == nil {
			return
		}
		walk(current.left)
		if current.atom != nil {
			out = append(out, *current.atom)
		}
		walk(current.right)
	}
	walk(node)
	return out
}

func projectWhereNodeUniformLogic(node *projectWhereNode) (string, bool) {
	if node == nil {
		return "", true
	}
	logic := ""
	uniform := true
	var walk func(*projectWhereNode)
	walk = func(current *projectWhereNode) {
		if current == nil || !uniform {
			return
		}
		walk(current.left)
		if current.op == "AND" || current.op == "OR" {
			if logic == "" {
				logic = current.op
			} else if logic != current.op {
				uniform = false
				return
			}
		}
		walk(current.right)
	}
	walk(node)
	return logic, uniform
}

func splitProjectWhereBoolean(raw string, token string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := make([]string, 0, 2)
	start := 0
	inSingle := false
	inDouble := false
	parenDepth := 0

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inDouble && i+1 < len(raw) && raw[i+1] == '"' {
				i++
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' {
			if parenDepth == 0 {
				return nil
			}
			parenDepth--
			continue
		}
		if parenDepth != 0 {
			continue
		}
		if !matchBooleanTokenAt(raw, i, token) {
			continue
		}
		part := strings.TrimSpace(raw[start:i])
		if part == "" {
			return nil
		}
		parts = append(parts, part)
		i += len(token) - 1
		start = i + 1
	}
	if inSingle || inDouble || parenDepth != 0 {
		return nil
	}

	last := strings.TrimSpace(raw[start:])
	if last == "" {
		return nil
	}
	parts = append(parts, last)
	return parts
}

func stripProjectWhereOuterParens(raw string) string {
	raw = strings.TrimSpace(raw)
	for {
		inner, ok := trimProjectWhereOuterParensOnce(raw)
		if !ok {
			return raw
		}
		raw = strings.TrimSpace(inner)
	}
}

func trimProjectWhereOuterParensOnce(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '(' || raw[len(raw)-1] != ')' {
		return "", false
	}
	inSingle := false
	inDouble := false
	parenDepth := 0
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inDouble && i+1 < len(raw) && raw[i+1] == '"' {
				i++
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' {
			parenDepth--
			if parenDepth < 0 {
				return "", false
			}
			if parenDepth == 0 && i != len(raw)-1 {
				return "", false
			}
		}
	}
	if inSingle || inDouble || parenDepth != 0 {
		return "", false
	}
	return raw[1 : len(raw)-1], true
}

func matchBooleanTokenAt(raw string, idx int, token string) bool {
	if idx < 0 || idx+len(token) > len(raw) {
		return false
	}
	slice := raw[idx : idx+len(token)]
	if !strings.EqualFold(slice, token) {
		return false
	}
	if idx > 0 {
		prev := rune(raw[idx-1])
		if isIdentifierChar(prev) {
			return false
		}
	}
	if idx+len(token) < len(raw) {
		next := rune(raw[idx+len(token)])
		if isIdentifierChar(next) {
			return false
		}
	}
	return true
}

func isIdentifierChar(r rune) bool {
	if r == '_' {
		return true
	}
	if r >= '0' && r <= '9' {
		return true
	}
	if r >= 'a' && r <= 'z' {
		return true
	}
	if r >= 'A' && r <= 'Z' {
		return true
	}
	return false
}

func isIdentifierToken(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for idx, r := range raw {
		if idx == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}
		if !isIdentifierChar(r) {
			return false
		}
	}
	return true
}

func quoteIsEscaped(raw string, idx int) bool {
	if idx <= 0 || idx > len(raw) {
		return false
	}
	backslashes := 0
	for i := idx - 1; i >= 0 && raw[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func isProjectWhereIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for idx, r := range value {
		if idx == 0 {
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

func parseProjectWhereLiteralValue(raw string) (any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if isQuotedLiteral(raw) {
		value, ok := parseProjectionStringLiteral(raw)
		if !ok {
			return nil, false
		}
		return value, true
	}
	if strings.EqualFold(raw, "true") {
		return true, true
	}
	if strings.EqualFold(raw, "false") {
		return false, true
	}
	if value, ok := parseProjectionNumericLiteral(raw); ok {
		return value, true
	}
	return raw, true
}

func projectWhereValue(atom projectWhereAtom, state *State) (any, bool) {
	if atom.rightParamName != "" {
		name := strings.TrimSpace(atom.rightParamName)
		if !isProjectWhereIdentifier(name) || state == nil || state.Params == nil {
			return nil, false
		}
		value, ok := state.Params[name]
		return value, ok
	}
	return atom.rightAny, true
}

func projectCompareWhere(left any, op string, right any) bool {
	switch op {
	case "IS NULL":
		return left == nil
	case "IS NOT NULL":
		return left != nil
	}
	if isProjectStringWhereOp(op) {
		leftString, lok := runtimeStringValue(left)
		rightString, rok := runtimeStringValue(right)
		if !lok || !rok {
			return false
		}
		switch op {
		case "STARTS WITH":
			return strings.HasPrefix(leftString, rightString)
		case "ENDS WITH":
			return strings.HasSuffix(leftString, rightString)
		case "CONTAINS":
			return strings.Contains(leftString, rightString)
		default:
			return false
		}
	}

	value, ok := applyProjectionComparisonOp(op, left, right)
	if !ok {
		return false
	}
	matched, isBool := value.(bool)
	if !isBool {
		return false
	}
	return matched
}

func isProjectStringWhereOp(op string) bool {
	switch normalizeProjectWhereOp(op) {
	case "STARTS WITH", "ENDS WITH", "CONTAINS":
		return true
	default:
		return false
	}
}

func normalizeProjectWhereOp(op string) string {
	fields := strings.Fields(strings.TrimSpace(op))
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(strings.Join(fields, " "))
}

func isQuotedLiteral(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return false
	}
	return (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')
}

func runtimeStringValue(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return fmt.Sprint(value), true
	}
}

func normalizeProjectionPropertyKey(key string) string {
	key = strings.TrimSpace(key)
	if len(key) >= 2 && key[0] == '`' && key[len(key)-1] == '`' {
		key = key[1 : len(key)-1]
		key = strings.ReplaceAll(key, "``", "`")
	}
	return key
}

func projectionClearEvalError(state *State) {
	if state != nil {
		state.EvalError = nil
	}
}

func projectionSetEvalError(state *State, err error) {
	if state == nil || err == nil || state.EvalError != nil {
		return
	}
	state.EvalError = err
}

func projectionEvalError(state *State) error {
	if state == nil {
		return nil
	}
	return state.EvalError
}

func resolveProjectionExprValueChecked(expr string, row map[string]any, state *State) (any, bool, error) {
	projectionClearEvalError(state)
	value, ok := resolveProjectionExprValue(expr, row, state)
	if err := projectionEvalError(state); err != nil {
		return nil, false, err
	}
	return value, ok, nil
}

func resolveProjectionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	expr = stripProjectWhereOuterParens(expr)
	if value, ok := resolveProjectionExistsSubqueryExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionCaseExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionPatternComprehensionExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionListComprehensionExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionLogicalExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionPatternPredicateExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionLabelPredicateExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionUnaryNotExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionUnarySignExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionBinaryExprValue(expr, row, state); ok {
		return value, true
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		if value, ok := parseProjectionListLiteral(expr[1:len(expr)-1], row, state); ok {
			return value, true
		}
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		if value, ok := parseProjectionMapLiteral(expr[1:len(expr)-1], row, state); ok {
			return value, true
		}
	}
	if value, ok := resolveProjectionIsNullExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionIndexExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionFunctionPathValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionFunctionExprValue(expr, row, state); ok {
		return value, true
	}
	if row != nil {
		if strings.Contains(expr, ".") {
			if value, ok := resolveProjectionPathValue(expr, row, state); ok {
				return value, true
			}
		}
		if value, ok := row[expr]; ok {
			if hydrated, hydratedOK := hydrateProjectionIdentifierValue(expr, value, row, state); hydratedOK {
				return hydrated, true
			}
			if coerced, ok := coerceProjectionStoredTemporalValue(value); ok {
				value = coerced
			}
			return value, true
		}
		if value, ok := row[expr+".id"]; ok {
			if hydrated, hydratedOK := hydrateProjectionIdentifierValue(expr, value, row, state); hydratedOK {
				return hydrated, true
			}
			if coerced, ok := coerceProjectionStoredTemporalValue(value); ok {
				value = coerced
			}
			return value, true
		}
	}
	if strings.HasPrefix(expr, "$") {
		name := strings.TrimSpace(expr[1:])
		if name == "" || state == nil || state.Params == nil {
			return nil, false
		}
		value, ok := state.Params[name]
		return value, ok
	}
	if strings.EqualFold(expr, "null") {
		return nil, true
	}
	if strings.EqualFold(expr, "true") {
		return true, true
	}
	if strings.EqualFold(expr, "false") {
		return false, true
	}
	if value, ok := parseProjectionStringLiteral(expr); ok {
		return value, true
	}
	if value, ok := parseProjectionNumericLiteral(expr); ok {
		return value, true
	}
	return nil, false
}

func resolveProjectionExistsSubqueryExprValue(expr string, row map[string]any, state *State) (any, bool) {
	body, ok := parseProjectionExistsSubqueryBody(expr)
	if !ok {
		return nil, false
	}
	if state == nil || state.ExistsSubqueryEvaluator == nil {
		projectionSetEvalError(state, graph.NewError(graph.ErrKindUnsupported, "ExistsSubqueryEvaluatorUnavailable", nil))
		return nil, true
	}
	tx := state.Tx
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	params := map[string]any{}
	if state.Params != nil {
		params = state.Params
	}
	matched, err := state.ExistsSubqueryEvaluator(ctx, tx, body, row, params)
	if err != nil {
		projectionSetEvalError(state, err)
		return nil, true
	}
	return matched, true
}

func parseProjectionExistsSubqueryBody(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < len("EXISTS{}") {
		return "", false
	}
	if !strings.EqualFold(raw[:6], "EXISTS") {
		return "", false
	}
	rest := strings.TrimSpace(raw[6:])
	if len(rest) < 2 || !strings.HasPrefix(rest, "{") || !strings.HasSuffix(rest, "}") {
		return "", false
	}
	body := strings.TrimSpace(rest[1 : len(rest)-1])
	if body == "" {
		return "", false
	}
	return body, true
}

func resolveProjectionPatternPredicateExprValue(expr string, row map[string]any, state *State) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if state == nil || state.Tx == nil {
		return nil, false
	}
	evalRow := projectionPatternEvalRow(row)

	if minHops, maxHops, isVarLength := parseProjectionVariableLengthBounds(expr); isVarLength {
		matches, err := expandVariableLengthPathRows(state, evalRow, expr, "", minHops, maxHops, false)
		if err != nil {
			projectionSetEvalError(state, err)
			return nil, true
		}
		return len(matches) > 0, true
	}

	edge := parseEdgeMutation(expr)
	if edge == nil {
		return nil, false
	}
	matches, err := expandPatternRow(state, evalRow, edge, "", nil, false)
	if err != nil {
		projectionSetEvalError(state, err)
		return nil, true
	}
	return len(matches) > 0, true
}

func projectionPatternEvalRow(row map[string]any) map[string]any {
	if row == nil {
		return map[string]any{}
	}
	next := cloneRow(row)
	delete(next, "__ve_chain_cursor")
	delete(next, "__edge.id")
	for key, value := range next {
		if strings.HasPrefix(key, "__") || strings.Contains(key, ".") {
			continue
		}
		if boundID, ok := next[key+".id"]; ok {
			if id := resolveBoundEntityIDFromValue(boundID); id != "" {
				next[key] = id
				continue
			}
		}
		if id := resolveBoundEntityIDFromValue(value); id != "" {
			next[key] = id
			next[key+".id"] = id
		}
	}
	return next
}

func resolveProjectionLabelPredicateExprValue(expr string, row map[string]any, state *State) (any, bool) {
	leftExpr, labels, ok := splitProjectionTopLevelLabelPredicate(expr)
	if !ok {
		return nil, false
	}
	value, valueOK := resolveProjectionExprValue(leftExpr, row, state)
	if !valueOK {
		return nil, false
	}
	if value == nil {
		return nil, true
	}

	if labelSet, ok := projectionLabelsForLabelPredicate(leftExpr, value, row, state); ok {
		for _, label := range labels {
			if _, exists := labelSet[label]; !exists {
				return false, true
			}
		}
		return true, true
	}

	if relType, ok := projectionRelationshipTypeForLabelPredicate(value); ok {
		for _, label := range labels {
			if relType != label {
				return false, true
			}
		}
		return true, true
	}

	return nil, true
}

func splitProjectionTopLevelLabelPredicate(raw string) (string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ':':
			if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+1:])
			if left == "" || right == "" {
				return "", nil, false
			}
			parts := strings.Split(right, ":")
			labels := make([]string, 0, len(parts))
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				labels = append(labels, strings.Trim(part, "`"))
			}
			if len(labels) == 0 {
				return "", nil, false
			}
			return left, labels, true
		}
	}
	return "", nil, false
}

func projectionLabelsForLabelPredicate(leftExpr string, value any, row map[string]any, state *State) (map[string]struct{}, bool) {
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return nil, false
		}
		out := make(map[string]struct{}, len(typed.Labels))
		for _, label := range typed.Labels {
			out[label] = struct{}{}
		}
		return out, true
	case map[string]any:
		if raw, exists := typed["labels"]; exists {
			return projectionLabelSetFromValue(raw)
		}
		if row != nil {
			if raw, exists := row[leftExpr+".labels"]; exists {
				return projectionLabelSetFromValue(raw)
			}
		}
	case string:
		if row != nil {
			if raw, exists := row[leftExpr+".labels"]; exists {
				return projectionLabelSetFromValue(raw)
			}
		}
		if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
			if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, strings.TrimSpace(typed)); err == nil && vertex != nil {
				out := make(map[string]struct{}, len(vertex.Labels))
				for _, label := range vertex.Labels {
					out[label] = struct{}{}
				}
				return out, true
			}
		}
	}
	return nil, false
}

func projectionLabelSetFromValue(value any) (map[string]struct{}, bool) {
	switch typed := value.(type) {
	case []string:
		out := make(map[string]struct{}, len(typed))
		for _, label := range typed {
			out[label] = struct{}{}
		}
		return out, true
	case []any:
		out := make(map[string]struct{}, len(typed))
		for _, raw := range typed {
			label := strings.TrimSpace(fmt.Sprint(raw))
			if label != "" {
				out[label] = struct{}{}
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func projectionRelationshipTypeForLabelPredicate(value any) (string, bool) {
	switch typed := value.(type) {
	case *graph.Edge:
		if typed == nil {
			return "", false
		}
		return strings.TrimSpace(typed.Type), true
	case map[string]any:
		raw, exists := typed["type"]
		if !exists {
			return "", false
		}
		return strings.TrimSpace(fmt.Sprint(raw)), true
	default:
		return "", false
	}
}

func resolveProjectionIndexExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	if !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	open := findProjectionTopLevelIndexOpen(raw)
	if open <= 0 || open >= len(raw)-1 {
		return nil, false
	}
	baseExpr := strings.TrimSpace(raw[:open])
	indexExpr := strings.TrimSpace(raw[open+1 : len(raw)-1])
	if baseExpr == "" || indexExpr == "" {
		return nil, false
	}

	base, ok := resolveProjectionExprValue(baseExpr, row, state)
	if !ok {
		return nil, false
	}
	if base == nil {
		return nil, true
	}
	if sliceStartExpr, sliceEndExpr, sliceOK := splitProjectionSliceExpr(indexExpr); sliceOK {
		return resolveProjectionSliceValue(base, sliceStartExpr, sliceEndExpr, row, state)
	}

	indexValue, indexOK := resolveProjectionExprValue(indexExpr, row, state)
	if !indexOK {
		return nil, false
	}
	if indexValue == nil {
		return nil, true
	}
	if key, keyOK := projectionStringIndexKey(indexValue); keyOK {
		if value, handled := resolveProjectionStringIndexValue(baseExpr, base, key, row, state); handled {
			return value, true
		}
	}

	if m, ok := base.(map[string]any); ok {
		if key, ok := projectionStringIndexKey(indexValue); ok {
			key = normalizeProjectionPropertyKey(key)
			v, exists := m[key]
			if !exists {
				return nil, true
			}
			return v, true
		}
		projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil))
		return nil, true
	}

	if list, ok := projectionListValue(base); ok {
		idx, ok := projectionStrictIntegerValue(indexValue)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		if idx < 0 {
			idx = len(list) + idx
		}
		if idx < 0 || idx >= len(list) {
			return nil, true
		}
		return list[idx], true
	}

	projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))

	return nil, true
}

func projectionStringIndexKey(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}

func resolveProjectionStringIndexValue(baseExpr string, base any, key string, row map[string]any, state *State) (any, bool) {
	key = normalizeProjectionPropertyKey(key)
	if key == "" {
		return nil, true
	}
	switch typed := base.(type) {
	case map[string]any:
		if nested, ok := typed["properties"].(map[string]any); ok {
			if value, exists := nested[key]; exists {
				if encoded, ok := value.([]byte); ok {
					decoded := decodeProjectionPropertyValue(encoded)
					if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
						return temporal, true
					}
					return decoded, true
				}
				return value, true
			}
		}
		if nested, ok := typed["properties"].(map[string][]byte); ok {
			if encoded, exists := nested[key]; exists {
				decoded := decodeProjectionPropertyValue(encoded)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					return temporal, true
				}
				return decoded, true
			}
		}
		if nested, ok := typed["properties"].(graph.PropertyMap); ok {
			if encoded, exists := nested[key]; exists {
				decoded := decodeProjectionPropertyValue(encoded)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					return temporal, true
				}
				return decoded, true
			}
		}
		if value, exists := typed[key]; exists {
			return value, true
		}
		if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, typed, row, state); ok {
			return value, true
		}
		return nil, true
	case map[string]string:
		if value, exists := typed[key]; exists {
			return value, true
		}
		return nil, true
	case *graph.Vertex:
		if typed == nil {
			return nil, true
		}
		if value, exists := typed.Properties[key]; exists {
			decoded := decodeProjectionPropertyValue(value)
			if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
				return temporal, true
			}
			return decoded, true
		}
		return nil, true
	case *graph.Edge:
		if typed == nil {
			return nil, true
		}
		if value, exists := typed.Properties[key]; exists {
			decoded := decodeProjectionPropertyValue(value)
			if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
				return temporal, true
			}
			return decoded, true
		}
		return nil, true
	default:
		if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, typed, row, state); ok {
			return value, true
		}
		return nil, false
	}
}

func splitProjectionSliceExpr(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	idx := strings.Index(raw, "..")
	if idx < 0 {
		return "", "", false
	}
	left := strings.TrimSpace(raw[:idx])
	right := strings.TrimSpace(raw[idx+2:])
	return left, right, true
}

func resolveProjectionSliceValue(base any, startExpr string, endExpr string, row map[string]any, state *State) (any, bool) {
	if base == nil {
		return nil, true
	}
	resolveBound := func(expr string, fallback int) (int, bool) {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return fallback, true
		}
		value, ok := resolveProjectionExprValue(expr, row, state)
		if !ok || value == nil {
			return 0, false
		}
		idx, ok := projectionNumericToInt(value)
		if !ok {
			return 0, false
		}
		return idx, true
	}

	if list, ok := projectionListValue(base); ok {
		start, startOK := resolveBound(startExpr, 0)
		if !startOK {
			return nil, true
		}
		end, endOK := resolveBound(endExpr, len(list))
		if !endOK {
			return nil, true
		}
		if start < 0 {
			start = len(list) + start
		}
		if end < 0 {
			end = len(list) + end
		}
		if start < 0 {
			start = 0
		}
		if end < 0 {
			end = 0
		}
		if start > len(list) {
			start = len(list)
		}
		if end > len(list) {
			end = len(list)
		}
		if end < start {
			end = start
		}
		out := append([]any(nil), list[start:end]...)
		return out, true
	}
	if text, ok := base.(string); ok {
		runes := []rune(text)
		start, startOK := resolveBound(startExpr, 0)
		if !startOK {
			return nil, true
		}
		end, endOK := resolveBound(endExpr, len(runes))
		if !endOK {
			return nil, true
		}
		if start < 0 {
			start = len(runes) + start
		}
		if end < 0 {
			end = len(runes) + end
		}
		if start < 0 {
			start = 0
		}
		if end < 0 {
			end = 0
		}
		if start > len(runes) {
			start = len(runes)
		}
		if end > len(runes) {
			end = len(runes)
		}
		if end < start {
			end = start
		}
		return string(runes[start:end]), true
	}

	return nil, true
}

func findProjectionTopLevelIndexOpen(raw string) int {
	depthParen := 0
	depthBrace := 0
	depthBracket := 0
	inSingle := false
	inDouble := false
	for i := len(raw) - 1; i >= 0; i-- {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case ']':
			depthBracket++
		case '[':
			if depthBracket == 1 && depthParen == 0 && depthBrace == 0 {
				return i
			}
			if depthBracket > 0 {
				depthBracket--
			}
		case ')':
			depthParen++
		case '(':
			if depthParen > 0 {
				depthParen--
			}
		case '}':
			depthBrace++
		case '{':
			if depthBrace > 0 {
				depthBrace--
			}
		}
	}
	return -1
}

func resolveProjectionCaseExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, false
	}
	body := strings.TrimSpace(raw[len("CASE") : len(raw)-len("END")])
	if body == "" {
		return nil, false
	}

	isSimpleCase := false
	var simpleCaseOperand any
	if !strings.HasPrefix(strings.ToUpper(body), "WHEN") {
		whenPos := findProjectionTopLevelKeyword(body, "WHEN")
		if whenPos <= 0 {
			return nil, false
		}
		simpleOperandExpr := strings.TrimSpace(body[:whenPos])
		if simpleOperandExpr == "" {
			return nil, false
		}
		operandValue, ok := resolveProjectionExprValue(simpleOperandExpr, row, state)
		if !ok {
			return nil, false
		}
		simpleCaseOperand = operandValue
		isSimpleCase = true
		body = strings.TrimSpace(body[whenPos:])
	}

	for {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(body)), "WHEN") {
			break
		}
		body = strings.TrimSpace(body[len("WHEN"):])
		thenPos := findProjectionTopLevelKeyword(body, "THEN")
		if thenPos <= 0 {
			return nil, false
		}
		condExpr := strings.TrimSpace(body[:thenPos])
		rest := strings.TrimSpace(body[thenPos+len("THEN"):])
		if condExpr == "" || rest == "" {
			return nil, false
		}

		nextWhen := findProjectionTopLevelKeyword(rest, "WHEN")
		nextElse := findProjectionTopLevelKeyword(rest, "ELSE")
		nextPos := len(rest)
		if nextWhen >= 0 && nextWhen < nextPos {
			nextPos = nextWhen
		}
		if nextElse >= 0 && nextElse < nextPos {
			nextPos = nextElse
		}
		thenExpr := strings.TrimSpace(rest[:nextPos])
		if thenExpr == "" {
			return nil, false
		}

		matches := false
		if isSimpleCase {
			whenValue, ok := resolveProjectionExprValue(condExpr, row, state)
			if !ok {
				return nil, false
			}
			if comparison, ok := applyProjectionComparisonOp("=", simpleCaseOperand, whenValue); ok {
				if equal, ok := comparison.(bool); ok && equal {
					matches = true
				}
			}
		} else {
			condValue, ok := resolveProjectionExprValue(condExpr, row, state)
			if !ok {
				return nil, false
			}
			if condBool, ok := condValue.(bool); ok && condBool {
				matches = true
			}
		}
		if matches {
			thenValue, thenOK := resolveProjectionExprValue(thenExpr, row, state)
			if !thenOK {
				return nil, false
			}
			return thenValue, true
		}

		if nextPos == len(rest) {
			body = ""
			break
		}
		body = strings.TrimSpace(rest[nextPos:])
	}

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(body)), "ELSE") {
		elseExpr := strings.TrimSpace(strings.TrimSpace(body)[len("ELSE"):])
		if elseExpr == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(elseExpr, row, state)
		if !ok {
			return nil, false
		}
		return value, true
	}

	return nil, true
}

func resolveProjectionListComprehensionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return nil, false
	}
	left := body
	mapExpr := ""
	if pipePos := findProjectionTopLevelRune(body, '|'); pipePos >= 0 {
		if pipePos <= 0 || pipePos >= len(body)-1 {
			return nil, false
		}
		left = strings.TrimSpace(body[:pipePos])
		mapExpr = strings.TrimSpace(body[pipePos+1:])
	}
	if left == "" {
		return nil, false
	}
	inPos := findProjectionTopLevelKeyword(left, "IN")
	if inPos <= 0 {
		return nil, false
	}
	varName := strings.TrimSpace(left[:inPos])
	if !isIdentifierToken(varName) {
		return nil, false
	}
	if mapExpr == "" {
		mapExpr = varName
	}
	rest := strings.TrimSpace(left[inPos+len("IN"):])
	if rest == "" {
		return nil, false
	}
	whereExpr := ""
	listExpr := rest
	if wherePos := findProjectionTopLevelKeyword(rest, "WHERE"); wherePos > 0 {
		listExpr = strings.TrimSpace(rest[:wherePos])
		whereExpr = strings.TrimSpace(rest[wherePos+len("WHERE"):])
		if whereExpr == "" {
			return nil, false
		}
	}
	listValue, ok := resolveProjectionExprValue(listExpr, row, state)
	if !ok {
		return nil, false
	}
	if listValue == nil {
		return nil, true
	}
	list, ok := projectionListValue(listValue)
	if !ok {
		return nil, true
	}

	out := make([]any, 0, len(list))
	for _, item := range list {
		next := cloneRow(row)
		if next == nil {
			next = map[string]any{}
		}
		next[varName] = item
		if boundID := resolveBoundEntityIDFromListComprehensionItem(item); boundID != "" {
			next[varName+".id"] = boundID
		}
		if whereExpr != "" {
			whereValue, whereOK := resolveProjectionExprValue(whereExpr, next, state)
			if !whereOK {
				continue
			}
			whereBool, isBool := whereValue.(bool)
			if !isBool || !whereBool {
				continue
			}
		}
		mapped, mappedOK := resolveProjectionExprValue(mapExpr, next, state)
		if !mappedOK {
			return nil, false
		}
		out = append(out, mapped)
	}

	return out, true
}

func resolveProjectionPatternComprehensionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return nil, false
	}
	pipePos := findProjectionTopLevelRune(body, '|')
	if pipePos <= 0 || pipePos >= len(body)-1 {
		return nil, false
	}
	left := strings.TrimSpace(body[:pipePos])
	mapExpr := strings.TrimSpace(body[pipePos+1:])
	if left == "" || mapExpr == "" {
		return nil, false
	}
	if findProjectionTopLevelKeyword(left, "IN") > 0 {
		return nil, false
	}

	patternExpr := left
	whereExpr := ""
	if wherePos := findProjectionTopLevelKeyword(left, "WHERE"); wherePos > 0 {
		patternExpr = strings.TrimSpace(left[:wherePos])
		whereExpr = strings.TrimSpace(left[wherePos+len("WHERE"):])
		if whereExpr == "" {
			return nil, true
		}
	}
	if patternExpr == "" {
		return nil, true
	}

	pathVar := ""
	if assignedPathVar, assignedPattern, hasAssignment := splitProjectionPathAssignment(patternExpr); hasAssignment {
		pathVar = strings.TrimSpace(assignedPathVar)
		patternExpr = strings.TrimSpace(assignedPattern)
	}
	if patternExpr == "" {
		return nil, true
	}

	var (
		expanded []map[string]any
		err      error
	)
	evalRow := projectionPatternEvalRow(row)
	if minHops, maxHops, isVarLength := parseProjectionVariableLengthBounds(patternExpr); isVarLength {
		expanded, err = expandVariableLengthPathRows(state, evalRow, patternExpr, pathVar, minHops, maxHops, false)
	} else if edge := parseEdgeMutation(patternExpr); edge != nil {
		expanded, err = expandPatternRow(state, evalRow, edge, pathVar, nil, false)
	} else if vertex := parseVertexMutation(patternExpr); vertex != nil {
		expanded, err = expandVertexPatternRow(state, evalRow, vertex, false)
		if err == nil && pathVar != "" {
			for _, expandedRow := range expanded {
				bindProjectionVertexPath(expandedRow, pathVar, vertex.Var, state)
			}
		}
	} else {
		return nil, false
	}
	if err != nil {
		projectionSetEvalError(state, err)
		return nil, true
	}

	out := make([]any, 0, len(expanded))
	for _, expandedRow := range expanded {
		if expandedRow == nil {
			expandedRow = map[string]any{}
		}
		if whereExpr != "" {
			whereValue, whereOK := resolveProjectionExprValue(whereExpr, expandedRow, state)
			if !whereOK {
				continue
			}
			whereBool, isBool := whereValue.(bool)
			if !isBool || !whereBool {
				continue
			}
		}
		mapped, mappedOK := resolveProjectionExprValue(mapExpr, expandedRow, state)
		if !mappedOK {
			return nil, false
		}
		out = append(out, mapped)
	}

	return out, true
}

func resolveProjectionIsNullExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := stripProjectWhereOuterParens(strings.TrimSpace(expr))
	if raw == "" {
		return nil, false
	}
	idx := findProjectionTopLevelKeyword(raw, "IS")
	if idx <= 0 {
		return nil, false
	}
	leftExpr := strings.TrimSpace(raw[:idx])
	if leftExpr == "" {
		return nil, false
	}
	rightRaw := strings.TrimSpace(raw[idx+2:])
	normalizedRight := strings.Join(strings.Fields(strings.ToUpper(rightRaw)), " ")
	isNot := false
	switch normalizedRight {
	case "NULL":
		isNot = false
	case "NOT NULL":
		isNot = true
	default:
		return nil, false
	}

	left, ok := resolveProjectionExprValue(leftExpr, row, state)
	if !ok {
		return nil, false
	}
	if isNot {
		return left != nil, true
	}
	return left == nil, true
}

func resolveProjectionUnaryNotExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := stripProjectWhereOuterParens(strings.TrimSpace(expr))
	if len(raw) < 3 {
		return nil, false
	}
	if !strings.EqualFold(raw[:3], "NOT") {
		return nil, false
	}
	if len(raw) > 3 {
		next := rune(raw[3])
		if !unicode.IsSpace(next) && next != '(' {
			return nil, false
		}
	}

	operandExpr := strings.TrimSpace(raw[3:])
	if operandExpr == "" {
		return nil, false
	}
	value, ok := resolveProjectionExprValue(operandExpr, row, state)
	if !ok {
		return nil, false
	}
	if value == nil {
		return nil, true
	}
	b, ok := value.(bool)
	if !ok {
		// Non-boolean operand to NOT; treat as unknown (null)
		return nil, true
	}
	return !b, true
}

func resolveProjectionUnarySignExprValue(expr string, row map[string]any, state *State) (any, bool) {
	raw := strings.TrimSpace(expr)
	if len(raw) < 2 {
		return nil, false
	}
	sign := raw[0]
	if sign != '+' && sign != '-' {
		return nil, false
	}
	rest := strings.TrimSpace(raw[1:])
	if !strings.HasPrefix(rest, "(") {
		return nil, false
	}
	operandExpr := rest
	if operandExpr == "" {
		return nil, false
	}
	operand, ok := resolveProjectionExprValue(operandExpr, row, state)
	if !ok {
		return nil, false
	}
	if operand == nil {
		return nil, true
	}
	if iv, ok := projectionStrictIntegerValue(operand); ok {
		if sign == '+' {
			return iv, true
		}
		return -iv, true
	}
	fv, ok := projectionNumericToFloat(operand)
	if !ok {
		return nil, false
	}
	if sign == '+' {
		return fv, true
	}
	return -fv, true
}

func resolveProjectionBinaryExprValue(expr string, row map[string]any, state *State) (any, bool) {
	if chainExprs, chainOps, ok := splitProjectionTopLevelComparisonChain(expr); ok {
		sawUnknown := false
		for i, op := range chainOps {
			left, leftOK := resolveProjectionExprValue(chainExprs[i], row, state)
			if !leftOK {
				return nil, false
			}
			right, rightOK := resolveProjectionExprValue(chainExprs[i+1], row, state)
			if !rightOK {
				return nil, false
			}
			comparison, cmpOK := applyProjectionComparisonOp(op, left, right)
			if !cmpOK {
				return nil, false
			}
			if comparison == nil {
				sawUnknown = true
				continue
			}
			matched, boolOK := comparison.(bool)
			if !boolOK {
				return nil, false
			}
			if !matched {
				return false, true
			}
		}
		if sawUnknown {
			return nil, true
		}
		return true, true
	}
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelComparison(expr); ok {
		left, leftOK := resolveProjectionExprValue(leftExpr, row, state)
		if leftOK {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				return applyProjectionComparisonOp(op, left, right)
			}
		}
	}
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelPredicate(expr); ok {
		left, ok := resolveProjectionExprValue(leftExpr, row, state)
		if ok {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				return applyProjectionPredicateOp(op, left, right)
			}
		}
	}
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelBinary(expr, "+-"); ok {
		left, ok := resolveProjectionExprValue(leftExpr, row, state)
		if ok {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				return applyProjectionBinaryOp(op, left, right)
			}
		}
	}
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelBinary(expr, "*/%"); ok {
		left, ok := resolveProjectionExprValue(leftExpr, row, state)
		if ok {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				return applyProjectionBinaryOp(op, left, right)
			}
		}
	}
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelPower(expr); ok {
		left, ok := resolveProjectionExprValue(leftExpr, row, state)
		if ok {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				return applyProjectionBinaryOp(op, left, right)
			}
		}
	}
	return nil, false
}

func splitProjectionTopLevelPower(expr string) (string, string, string, bool) {
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := len(expr) - 1; i >= 0; i-- {
		ch := expr[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
			continue
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
			continue
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
			continue
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		switch ch {
		case ')':
			depthParen++
		case '(':
			if depthParen > 0 {
				depthParen--
			}
		case ']':
			depthBracket++
		case '[':
			if depthBracket > 0 {
				depthBracket--
			}
		case '}':
			depthBrace++
		case '{':
			if depthBrace > 0 {
				depthBrace--
			}
		default:
			if ch != '^' || depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
				continue
			}
			if i == 0 {
				continue
			}
			left := strings.TrimSpace(expr[:i])
			right := strings.TrimSpace(expr[i+1:])
			if left == "" || right == "" {
				continue
			}
			return left, "^", right, true
		}
	}
	return "", "", "", false
}

func resolveProjectionLogicalExprValue(expr string, row map[string]any, state *State) (any, bool) {
	if leftExpr, op, rightExpr, ok := splitProjectionTopLevelLogical(expr); ok {
		left, leftOK := resolveProjectionExprValue(leftExpr, row, state)
		if leftOK {
			right, rightOK := resolveProjectionExprValue(rightExpr, row, state)
			if rightOK {
				// In Cypher, AND/OR operators work with boolean operands.
				// Non-boolean, non-null values should be treated as unknown (null) rather than throwing an error.
				if left != nil {
					if _, ok := left.(bool); !ok {
						left = nil
					}
				}
				if right != nil {
					if _, ok := right.(bool); !ok {
						right = nil
					}
				}
				return applyProjectionLogicalOp(op, left, right)
			}
		}
	}
	return nil, false
}

func splitProjectionTopLevelPredicate(expr string) (string, string, string, bool) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return "", "", "", false
	}
	for _, op := range []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "IN"} {
		idx := findProjectionTopLevelKeyword(raw, op)
		if idx <= 0 {
			continue
		}
		left := strings.TrimSpace(raw[:idx])
		right := strings.TrimSpace(raw[idx+len(op):])
		if left == "" || right == "" {
			continue
		}
		return left, op, right, true
	}
	return "", "", "", false
}

func applyProjectionPredicateOp(op string, left any, right any) (any, bool) {
	switch op {
	case "IN":
		if right == nil {
			return nil, true
		}
		list, ok := projectionListValue(right)
		if !ok {
			return nil, true
		}
		if len(list) == 0 {
			return false, true
		}
		if left == nil {
			return nil, true
		}
		sawUnknown := false
		for _, item := range list {
			match, known := projectionValuesEqualTernary(left, item)
			if known && match {
				return true, true
			}
			if !known {
				sawUnknown = true
			}
		}
		if sawUnknown {
			return nil, true
		}
		return false, true
	case "STARTS WITH", "ENDS WITH", "CONTAINS":
		if left == nil || right == nil {
			return nil, true
		}
		leftString, lok := left.(string)
		rightString, rok := right.(string)
		if !lok || !rok {
			return nil, true
		}
		switch op {
		case "STARTS WITH":
			return strings.HasPrefix(leftString, rightString), true
		case "ENDS WITH":
			return strings.HasSuffix(leftString, rightString), true
		case "CONTAINS":
			return strings.Contains(leftString, rightString), true
		}
	}
	return nil, false
}

func splitProjectionTopLevelLogical(expr string) (string, string, string, bool) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return "", "", "", false
	}
	for _, op := range []string{"OR", "XOR", "AND"} {
		idx := findProjectionTopLevelKeyword(raw, op)
		if idx <= 0 {
			continue
		}
		left := strings.TrimSpace(raw[:idx])
		right := strings.TrimSpace(raw[idx+len(op):])
		if left == "" || right == "" {
			continue
		}
		return left, op, right, true
	}
	return "", "", "", false
}

func applyProjectionLogicalOp(op string, left any, right any) (any, bool) {
	lv, lok := left.(bool)
	rv, rok := right.(bool)
	leftNil := left == nil || !lok
	rightNil := right == nil || !rok

	switch op {
	case "OR":
		if lok && lv {
			return true, true
		}
		if rok && rv {
			return true, true
		}
		if leftNil || rightNil {
			return nil, true
		}
		return false, true
	case "AND":
		if lok && !lv {
			return false, true
		}
		if rok && !rv {
			return false, true
		}
		if leftNil || rightNil {
			return nil, true
		}
		return true, true
	case "XOR":
		if leftNil || rightNil {
			return nil, true
		}
		return lv != rv, true
	default:
		return nil, false
	}
}

func splitProjectionTopLevelComparison(expr string) (string, string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			if quoteIsEscaped(expr, i) {
				continue
			}
			if i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if quoteIsEscaped(expr, i) {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		for _, op := range []string{"<=", ">=", "<>", "!=", "=", "<", ">"} {
			if strings.HasPrefix(expr[i:], op) {
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+len(op):])
				if left == "" || right == "" {
					return "", "", "", false
				}
				return left, op, right, true
			}
		}
	}
	return "", "", "", false
}

func splitProjectionTopLevelComparisonChain(expr string) ([]string, []string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	parts := make([]string, 0, 3)
	ops := make([]string, 0, 2)
	last := 0
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			if quoteIsEscaped(expr, i) {
				continue
			}
			if i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if quoteIsEscaped(expr, i) {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		matchedOp := ""
		for _, op := range []string{"<=", ">=", "<>", "!=", "=", "<", ">"} {
			if strings.HasPrefix(expr[i:], op) {
				matchedOp = op
				break
			}
		}
		if matchedOp == "" {
			continue
		}
		part := strings.TrimSpace(expr[last:i])
		if part == "" {
			return nil, nil, false
		}
		parts = append(parts, part)
		ops = append(ops, matchedOp)
		i += len(matchedOp) - 1
		last = i + 1
	}
	if len(ops) < 2 {
		return nil, nil, false
	}
	lastPart := strings.TrimSpace(expr[last:])
	if lastPart == "" {
		return nil, nil, false
	}
	parts = append(parts, lastPart)
	if len(parts) != len(ops)+1 {
		return nil, nil, false
	}
	return parts, ops, true
}

func applyProjectionComparisonOp(op string, left any, right any) (any, bool) {
	if left == nil || right == nil {
		return nil, true
	}
	switch op {
	case "=":
		match, known := projectionValuesEqualTernary(left, right)
		if !known {
			return nil, true
		}
		return match, true
	case "!=", "<>":
		match, known := projectionValuesEqualTernary(left, right)
		if !known {
			return nil, true
		}
		return !match, true
	case "<", "<=", ">", ">=":
		return applyProjectionOrderingComparisonOp(op, left, right)
	}
	return nil, false
}

func applyProjectionOrderingComparisonOp(op string, left any, right any) (any, bool) {
	if left == nil || right == nil {
		return nil, true
	}
	if cmp, ok := projectionCompareNumericValues(left, right); ok {
		switch op {
		case "<":
			return cmp < 0, true
		case "<=":
			return cmp <= 0, true
		case ">":
			return cmp > 0, true
		case ">=":
			return cmp >= 0, true
		}
		return nil, false
	}

	if ln, lok := projectionComparableNumericToFloat(left); lok {
		rn, rok := projectionComparableNumericToFloat(right)
		if !rok {
			return nil, true
		}
		if math.IsNaN(ln) || math.IsNaN(rn) {
			return false, true
		}
		switch op {
		case "<":
			return ln < rn, true
		case "<=":
			return ln <= rn, true
		case ">":
			return ln > rn, true
		case ">=":
			return ln >= rn, true
		}
		return nil, false
	}
	if _, rok := projectionComparableNumericToFloat(right); rok {
		return nil, true
	}

	cmp, known := projectionCompareOrderedValuesTernary(left, right)
	if !known {
		return nil, true
	}
	switch op {
	case "<":
		return cmp < 0, true
	case "<=":
		return cmp <= 0, true
	case ">":
		return cmp > 0, true
	case ">=":
		return cmp >= 0, true
	default:
		return nil, false
	}
}

func projectionCompareOrderedValuesTernary(left any, right any) (int, bool) {
	if left == nil || right == nil {
		return 0, false
	}
	if lf, lok := projectionComparableNumericToFloat(left); lok {
		rf, rok := projectionComparableNumericToFloat(right)
		if !rok || math.IsNaN(lf) || math.IsNaN(rf) {
			return 0, false
		}
		switch {
		case lf < rf:
			return -1, true
		case lf > rf:
			return 1, true
		default:
			return 0, true
		}
	}
	if _, rok := projectionComparableNumericToFloat(right); rok {
		return 0, false
	}

	if leftTemporal, ok := projectionTemporalMap(left); ok {
		rightTemporal, rok := projectionTemporalMap(right)
		if !rok {
			return 0, false
		}
		if cmp, ok := projectionCompareTemporalValues(leftTemporal, rightTemporal); ok {
			return cmp, true
		}
		return 0, false
	}

	if ls, lok := left.(string); lok {
		rs, rok := right.(string)
		if !rok {
			return 0, false
		}
		return strings.Compare(ls, rs), true
	}

	if lb, lok := left.(bool); lok {
		rb, rok := right.(bool)
		if !rok {
			return 0, false
		}
		if lb == rb {
			return 0, true
		}
		if !lb && rb {
			return -1, true
		}
		return 1, true
	}

	leftList, leftIsList := projectionListValue(left)
	rightList, rightIsList := projectionListValue(right)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return 0, false
		}
		minLen := len(leftList)
		if len(rightList) < minLen {
			minLen = len(rightList)
		}
		for i := 0; i < minLen; i++ {
			cmp, known := projectionCompareOrderedValuesTernary(leftList[i], rightList[i])
			if !known {
				return 0, false
			}
			if cmp != 0 {
				return cmp, true
			}
		}
		if len(leftList) < len(rightList) {
			return -1, true
		}
		if len(leftList) > len(rightList) {
			return 1, true
		}
		return 0, true
	}

	return 0, false
}

func projectionValuesEqual(left any, right any) bool {
	if left == nil || right == nil {
		return left == right
	}
	if leftID, leftEntity := projectionValueEntityID(left); leftEntity {
		if rightID, rightEntity := projectionValueEntityID(right); rightEntity {
			return leftID == rightID
		}
	}
	if lt, ok := coerceProjectionStoredTemporalValue(left); ok {
		left = lt
	}
	if rt, ok := coerceProjectionStoredTemporalValue(right); ok {
		right = rt
	}
	if leftTemporal, ok := projectionTemporalMap(left); ok {
		if rightTemporal, ok := projectionTemporalMap(right); ok {
			if cmp, ok := projectionCompareTemporalValues(leftTemporal, rightTemporal); ok {
				return cmp == 0
			}
			leftRendered, leftOK := projectionTemporalToString(leftTemporal)
			rightRendered, rightOK := projectionTemporalToString(rightTemporal)
			if leftOK && rightOK {
				return leftRendered == rightRendered
			}
		}
	}
	if cmp, ok := projectionCompareNumericValues(left, right); ok {
		return cmp == 0
	}
	return reflect.DeepEqual(left, right)
}

func projectionComparableNumericToFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		n, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return n, true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func projectionComparableInteger(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func projectionCompareNumericValues(left any, right any) (int, bool) {
	if li, lok := projectionComparableInteger(left); lok {
		if ri, rok := projectionComparableInteger(right); rok {
			switch {
			case li < ri:
				return -1, true
			case li > ri:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	lf, lok := projectionComparableNumericToFloat(left)
	rf, rok := projectionComparableNumericToFloat(right)
	if !lok || !rok || math.IsNaN(lf) || math.IsNaN(rf) {
		return 0, false
	}
	switch {
	case lf < rf:
		return -1, true
	case lf > rf:
		return 1, true
	default:
		return 0, true
	}
}

func projectionValueEntityID(value any) (string, bool) {
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return "", false
		}
		id := strings.TrimSpace(typed.ID)
		return id, id != ""
	case *graph.Edge:
		if typed == nil {
			return "", false
		}
		id := strings.TrimSpace(typed.ID)
		return id, id != ""
	case map[string]any:
		id := strings.TrimSpace(scalarString(typed["id"]))
		if id == "" {
			return "", false
		}
		if _, ok := typed["labels"]; ok {
			return id, true
		}
		if _, ok := typed["properties"]; ok {
			return id, true
		}
		if _, ok := typed["type"]; ok {
			return id, true
		}
		if _, ok := typed["src"]; ok {
			return id, true
		}
		if _, ok := typed["dst"]; ok {
			return id, true
		}
		if _, ok := typed["tenant"]; ok {
			return id, true
		}
		return "", false
	default:
		return "", false
	}
}

func projectionValuesEqualTernary(left any, right any) (bool, bool) {
	if left == nil || right == nil {
		return false, false
	}
	if leftID, leftEntity := projectionValueEntityID(left); leftEntity {
		if rightID, rightEntity := projectionValueEntityID(right); rightEntity {
			return leftID == rightID, true
		}
	}
	if lt, ok := coerceProjectionStoredTemporalValue(left); ok {
		left = lt
	}
	if rt, ok := coerceProjectionStoredTemporalValue(right); ok {
		right = rt
	}
	if leftTemporal, ok := projectionTemporalMap(left); ok {
		if rightTemporal, ok := projectionTemporalMap(right); ok {
			if cmp, ok := projectionCompareTemporalValues(leftTemporal, rightTemporal); ok {
				return cmp == 0, true
			}
			return false, true
		}
	}
	if cmp, ok := projectionCompareNumericValues(left, right); ok {
		return cmp == 0, true
	}
	leftList, leftIsList := projectionListValue(left)
	rightList, rightIsList := projectionListValue(right)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return false, true
		}
		if len(leftList) != len(rightList) {
			return false, true
		}
		sawUnknown := false
		for i := range leftList {
			match, known := projectionValuesEqualTernary(leftList[i], rightList[i])
			if known && !match {
				return false, true
			}
			if !known {
				sawUnknown = true
			}
		}
		if sawUnknown {
			return false, false
		}
		return true, true
	}
	leftMap, leftIsMap := left.(map[string]any)
	rightMap, rightIsMap := right.(map[string]any)
	if leftIsMap || rightIsMap {
		if !leftIsMap || !rightIsMap {
			return false, true
		}
		if len(leftMap) != len(rightMap) {
			return false, true
		}
		sawUnknown := false
		for key, leftValue := range leftMap {
			rightValue, exists := rightMap[key]
			if !exists {
				return false, true
			}
			match, known := projectionValuesEqualTernary(leftValue, rightValue)
			if known && !match {
				return false, true
			}
			if !known {
				sawUnknown = true
			}
		}
		if sawUnknown {
			return false, false
		}
		return true, true
	}
	return projectionValuesEqual(left, right), true
}

func splitProjectionTopLevelBinary(expr string, ops string) (string, string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := len(expr) - 1; i >= 0; i-- {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && expr[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case ')':
			depthParen++
		case '(':
			if depthParen > 0 {
				depthParen--
			}
		case ']':
			depthBracket++
		case '[':
			if depthBracket > 0 {
				depthBracket--
			}
		case '}':
			depthBrace++
		case '{':
			if depthBrace > 0 {
				depthBrace--
			}
		default:
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.ContainsRune(ops, rune(ch)) {
				if i == 0 {
					continue
				}
				if (ch == '+' || ch == '-') && projectionUnarySignPosition(expr, i) {
					continue
				}
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+1:])
				if left == "" || right == "" {
					continue
				}
				return left, string(ch), right, true
			}
		}
	}
	return "", "", "", false
}

func projectionUnarySignPosition(expr string, index int) bool {
	if index <= 0 || index >= len(expr) {
		return false
	}
	for i := index - 1; i >= 0; i-- {
		ch := expr[i]
		if unicode.IsSpace(rune(ch)) {
			continue
		}
		switch ch {
		case '(', '[', '{', ',', ':', '+', '-', '*', '/', '%', '=', '<', '>', '!':
			return true
		default:
			return false
		}
	}
	return true
}

func applyProjectionBinaryOp(op string, left any, right any) (any, bool) {
	if left == nil || right == nil {
		if op == "+" {
			if leftList, ok := projectionListValue(left); ok {
				out := make([]any, 0, len(leftList)+1)
				out = append(out, leftList...)
				if rightList, rightOK := projectionListValue(right); rightOK {
					out = append(out, rightList...)
				} else {
					out = append(out, right)
				}
				return out, true
			}
			if rightList, ok := projectionListValue(right); ok {
				out := make([]any, 0, len(rightList)+1)
				out = append(out, left)
				out = append(out, rightList...)
				return out, true
			}
		}
		return nil, true
	}
	if op == "+" {
		if ls, lok := left.(string); lok {
			if rs, rok := right.(string); rok {
				return ls + rs, true
			}
		}
		if leftList, ok := projectionListValue(left); ok {
			out := make([]any, 0, len(leftList)+1)
			out = append(out, leftList...)
			if rightList, rightOK := projectionListValue(right); rightOK {
				out = append(out, rightList...)
			} else {
				out = append(out, right)
			}
			return out, true
		}
		if rightList, ok := projectionListValue(right); ok {
			out := make([]any, 0, len(rightList)+1)
			out = append(out, left)
			out = append(out, rightList...)
			return out, true
		}
	}

	if li, lok := projectionStrictIntegerValue(left); lok {
		if ri, rok := projectionStrictIntegerValue(right); rok {
			switch op {
			case "+":
				return li + ri, true
			case "-":
				return li - ri, true
			case "*":
				return li * ri, true
			case "/":
				if ri == 0 {
					if li == 0 {
						return math.NaN(), true
					}
					if li > 0 {
						return math.Inf(1), true
					}
					return math.Inf(-1), true
				}
				return li / ri, true
			case "%":
				if ri == 0 {
					return nil, false
				}
				return li % ri, true
			}
		}
	}

	if l, ok := projectionNumericToFloat(left); ok {
		if r, ok := projectionNumericToFloat(right); ok {
			switch op {
			case "+":
				return l + r, true
			case "-":
				return l - r, true
			case "*":
				return l * r, true
			case "/":
				if r == 0 {
					if l == 0 {
						return math.NaN(), true
					}
					if l > 0 {
						return math.Inf(1), true
					}
					return math.Inf(-1), true
				}
				return l / r, true
			case "%":
				if r == 0 {
					return nil, false
				}
				return math.Mod(l, r), true
			case "^":
				return math.Pow(l, r), true
			}
		}
	}

	if ld, ok := projectionDurationMap(left); ok {
		if rd, ok := projectionDurationMap(right); ok {
			switch op {
			case "+":
				return projectionDurationAdd(ld, rd), true
			case "-":
				return projectionDurationSubtract(ld, rd), true
			}
		}
		if scale, ok := projectionNumericToFloat(right); ok {
			switch op {
			case "*":
				return projectionDurationScale(ld, scale), true
			case "/":
				if scale == 0 {
					return nil, false
				}
				return projectionDurationScale(ld, 1/scale), true
			}
		}
	}
	if rd, ok := projectionDurationMap(right); ok {
		if lt, ok := projectionTemporalMap(left); ok {
			switch op {
			case "+":
				return projectionTemporalAddDuration(lt, rd)
			case "-":
				return projectionTemporalAddDuration(lt, projectionDurationScale(rd, -1))
			}
		}
	}
	if ld, ok := projectionDurationMap(left); ok {
		if rt, ok := projectionTemporalMap(right); ok && op == "+" {
			return projectionTemporalAddDuration(rt, ld)
		}
	}

	return nil, false
}

func projectionDurationMap(value any) (map[string]any, bool) {
	typed, ok := projectionTemporalMap(value)
	if !ok {
		return nil, false
	}
	if strings.ToLower(strings.TrimSpace(fmt.Sprint(typed["__temporal_type"]))) != "duration" {
		return nil, false
	}
	return typed, true
}

func projectionDurationAdd(a, b map[string]any) map[string]any {
	return projectionBuildDurationResult(projectionDurationComponentsFromMap(a).add(projectionDurationComponentsFromMap(b)))
}

func projectionDurationSubtract(a, b map[string]any) map[string]any {
	return projectionBuildDurationResult(projectionDurationComponentsFromMap(a).sub(projectionDurationComponentsFromMap(b)))
}

func projectionDurationScale(a map[string]any, factor float64) map[string]any {
	return projectionBuildDurationResult(projectionDurationComponentsFromMap(a).scale(factor))
}

func projectionTemporalAddDuration(temporal map[string]any, duration map[string]any) (any, bool) {
	return projectionApplyTemporalAndDuration(temporal, projectionDurationComponentsFromMap(duration), "+")
}

func resolveProjectionFunctionPathValue(expr string, row map[string]any, state *State) (any, bool) {
	expr = strings.TrimSpace(expr)
	closeIdx := strings.IndexByte(expr, ')')
	if closeIdx <= 0 || closeIdx+1 >= len(expr) || expr[closeIdx+1] != '.' {
		return nil, false
	}
	callExpr := strings.TrimSpace(expr[:closeIdx+1])
	base, ok := resolveProjectionFunctionExprValue(callExpr, row, state)
	if !ok || base == nil {
		return nil, false
	}
	tail := strings.TrimSpace(expr[closeIdx+2:])
	if tail == "" {
		return nil, false
	}
	parts := strings.Split(tail, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return nil, false
		}
	}
	return resolveProjectionPropertyChain(callExpr, base, parts, row, state)
}

func resolveProjectionPathValue(expr string, row map[string]any, state *State) (any, bool) {
	if row == nil {
		return nil, false
	}
	if baseExpr, keys, ok := splitProjectionPathExpression(expr); ok {
		base, baseOK := resolveProjectionExprValue(baseExpr, row, state)
		if !baseOK {
			return nil, false
		}
		if base == nil {
			return nil, true
		}
		return resolveProjectionPropertyChain(baseExpr, base, keys, row, state)
	}
	parts := strings.Split(expr, ".")
	if len(parts) < 2 {
		return nil, false
	}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
	}

	current, ok := row[parts[0]]
	if !ok {
		if flatValue, hasFlat := row[expr]; hasFlat {
			return flatValue, true
		}
		if boundID, hasBoundID := row[parts[0]+".id"]; hasBoundID {
			current = boundID
		} else {
			if !isProjectWhereIdentifier(parts[0]) {
				return nil, false
			}
			return nil, true
		}
	}
	if len(parts) == 2 {
		if entityMap, mapOK := current.(map[string]any); mapOK {
			if nested, nestedOK := entityMap["properties"].(map[string]any); nestedOK {
				if value, exists := nested[parts[1]]; exists {
					return value, true
				}
			}
		}
	}
	return resolveProjectionPropertyChain(parts[0], current, parts[1:], row, state)
}

func splitProjectionPathExpression(expr string) (string, []string, bool) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return "", nil, false
	}
	if _, ok := parseProjectionNumericLiteral(raw); ok {
		return "", nil, false
	}
	idx := findProjectionTopLevelRune(raw, '.')
	if idx <= 0 || idx >= len(raw)-1 {
		return "", nil, false
	}
	baseExpr := strings.TrimSpace(raw[:idx])
	if baseExpr == "" || isProjectWhereIdentifier(baseExpr) {
		return "", nil, false
	}
	keys := strings.Split(raw[idx+1:], ".")
	if len(keys) == 0 {
		return "", nil, false
	}
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
		if keys[i] == "" {
			return "", nil, false
		}
	}
	return baseExpr, keys, true
}

func resolveProjectionPropertyChain(baseExpr string, current any, keys []string, row map[string]any, state *State) (any, bool) {
	for i := 0; i < len(keys); i++ {
		key := normalizeProjectionPropertyKey(keys[i])
		switch typed := current.(type) {
		case projectionPathValue:
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "nodes":
				current = append([]any(nil), typed.nodes...)
				continue
			case "relationships":
				current = append([]any(nil), typed.relationships...)
				continue
			}
			return nil, false
		case *graph.Vertex:
			if typed == nil {
				return nil, true
			}
			if value, exists := typed.Properties[key]; exists {
				decoded := decodeProjectionPropertyValue(value)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					current = temporal
				} else {
					current = decoded
				}
				continue
			}
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "labels":
				current = append([]string(nil), typed.Labels...)
				continue
			}
			return nil, true
		case *graph.Edge:
			if typed == nil {
				return nil, true
			}
			if value, exists := typed.Properties[key]; exists {
				decoded := decodeProjectionPropertyValue(value)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					current = temporal
				} else {
					current = decoded
				}
				continue
			}
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "type":
				current = typed.Type
				continue
			case "src", "srcid":
				current = typed.SrcID
				continue
			case "dst", "dstid":
				current = typed.DstID
				continue
			}
			return nil, true
		case map[string]any:
			if value, ok := projectionTemporalAccessorValue(typed, key); ok {
				current = value
				continue
			}
			if nested, ok := typed["properties"].(map[string]any); ok {
				if value, exists := nested[key]; exists {
					current = value
					continue
				}
			}
			next, exists := typed[key]
			if exists {
				if next == nil && i == 0 {
					if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, typed, row, state); ok {
						current = value
						continue
					}
					if idValue, hasID := typed["id"]; hasID {
						if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, idValue, row, state); ok {
							current = value
							continue
						}
					}
				}
				if _, temporal := projectionTemporalMap(typed); !temporal {
					if coerced, ok := coerceProjectionStoredTemporalValue(next); ok {
						next = coerced
					}
				}
				current = next
				continue
			}
			if i == 0 {
				if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, typed, row, state); ok {
					current = value
					continue
				}
				if idValue, hasID := typed["id"]; hasID {
					if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, idValue, row, state); ok {
						current = value
						continue
					}
				}
			}
			return nil, true
		case map[string]string:
			next, exists := typed[key]
			if !exists {
				return nil, true
			}
			current = next
		default:
			if value, ok := projectionTemporalAccessorFromScalar(typed, key); ok {
				current = value
				continue
			}
			if i == 0 {
				if value, ok := resolveProjectionBoundEntityProperty(baseExpr, key, typed, row, state); ok {
					current = value
					continue
				}
			}
			return nil, true
		}
	}
	if len(keys) == 1 {
		if rootMap, ok := row[baseExpr].(map[string]any); ok {
			if _, temporal := projectionTemporalMap(rootMap); !temporal {
				if coerced, ok := coerceProjectionStoredTemporalValue(current); ok {
					current = coerced
				}
			}
		}
	}
	return current, true
}

func coerceProjectionStoredTemporalValue(value any) (any, bool) {
	if value == nil {
		return nil, false
	}
	if temporal, ok := projectionTemporalMap(value); ok {
		return temporal, true
	}
	var raw string
	switch typed := value.(type) {
	case string:
		raw = typed
	case []byte:
		raw = string(typed)
	default:
		return nil, false
	}
	return projectionTemporalFromString(raw)
}

func projectionTemporalFromString(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(strings.ToUpper(raw), "P") {
		if temporal, ok := parseProjectionTemporalLiteralToMap("duration", raw); ok {
			temporal["__temporal_type"] = "duration"
			return temporal, true
		}
	}
	if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
		if _, _, _, dateOK := parseProjectionDateParts(datePart); dateOK {
			if _, _, _, _, tz, timeOK := parseProjectionClockAndZone(timePart); timeOK {
				typeName := "localdatetime"
				if tz != "" {
					typeName = "datetime"
				}
				if temporal, ok := parseProjectionTemporalLiteralToMap(typeName, raw); ok {
					temporal["__temporal_type"] = typeName
					return temporal, true
				}
			}
		}
	}
	if strings.Contains(raw, ":") {
		if _, _, _, _, tz, ok := parseProjectionClockAndZone(raw); ok {
			typeName := "localtime"
			if tz != "" {
				typeName = "time"
			}
			if temporal, ok := parseProjectionTemporalLiteralToMap(typeName, raw); ok {
				temporal["__temporal_type"] = typeName
				return temporal, true
			}
		}
	}
	if strings.Count(raw, "-") >= 2 {
		if temporal, ok := parseProjectionTemporalLiteralToMap("date", raw); ok {
			temporal["__temporal_type"] = "date"
			return temporal, true
		}
	}
	return nil, false
}

func projectionTemporalAccessorFromScalar(value any, field string) (any, bool) {
	temporal, ok := coerceProjectionStoredTemporalValue(value)
	if !ok {
		return nil, false
	}
	typed, ok := temporal.(map[string]any)
	if !ok {
		return nil, false
	}
	return projectionTemporalAccessorValue(typed, field)
}

func resolveProjectionBoundEntityProperty(varName, key string, current any, row map[string]any, state *State) (any, bool) {
	if row != nil {
		if strings.EqualFold(key, "id") {
			if value, ok := row[varName+".id"]; ok {
				boundID := ""
				if bound, boundOK := row[varName]; boundOK {
					boundID = strings.TrimSpace(scalarString(bound))
				}
				idBinding := strings.TrimSpace(scalarString(value))
				if idBinding != "" && (boundID == "" || idBinding != boundID) {
					return value, true
				}
			}
		} else {
			if value, ok := row[varName+"."+key]; ok {
				if value != nil {
					return value, true
				}
			}
		}
	}
	if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
		entityID := ""
		switch typed := current.(type) {
		case map[string]any:
			entityID = strings.TrimSpace(scalarString(typed["id"]))
			if entityID == "" {
				entityID = strings.TrimSpace(scalarString(typed["ID"]))
			}
		}
		if entityID == "" {
			entityID = scalarString(current)
		}
		if entityID == "" && row != nil {
			if boundID, ok := row[varName+".id"]; ok {
				entityID = scalarString(boundID)
			}
		}
		if entityID != "" {
			trimmedEntityID := strings.TrimSpace(entityID)
			if !strings.EqualFold(key, "id") {
				if projectionIsDeletedVertexID(state, row, trimmedEntityID) {
					projectionSetEvalError(state, graph.NewError(graph.ErrKindNotFound, "vertex not found", nil))
					return nil, true
				}
				if projectionIsDeletedEdgeID(state, row, trimmedEntityID) {
					projectionSetEvalError(state, graph.NewError(graph.ErrKindNotFound, "relationship not found", nil))
					return nil, true
				}
			}
			if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
				if value, exists := vertex.Properties[key]; exists {
					decoded := decodeProjectionPropertyValue(value)
					if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
						return temporal, true
					}
					return decoded, true
				}
			}
			if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
				if value, exists := edge.Properties[key]; exists {
					decoded := decodeProjectionPropertyValue(value)
					if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
						return temporal, true
					}
					return decoded, true
				}
			}
			if srcID, edgeType, dstID, ok := projectionCanonicalEdgeIDParts(entityID); ok {
				matched := any(nil)
				_ = state.Tx.ScanOutEdges(context.Background(), state.Tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
					if edge == nil {
						return nil
					}
					if strings.TrimSpace(edge.DstID) != dstID {
						return nil
					}
					if value, exists := edge.Properties[key]; exists {
						decoded := decodeProjectionPropertyValue(value)
						if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
							matched = temporal
						} else {
							matched = decoded
						}
					}
					return nil
				})
				if matched != nil {
					return matched, true
				}
			}
		}
	}
	return nil, false
}

func projectionPropertiesMapFromValue(varName string, value any, row map[string]any, state *State) (map[string]any, bool) {
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return nil, false
		}
		out := make(map[string]any, len(typed.Properties))
		for key, encoded := range typed.Properties {
			decoded := decodeProjectionPropertyValue(encoded)
			if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
				out[key] = temporal
				continue
			}
			out[key] = decoded
		}
		return out, true
	case *graph.Edge:
		if typed == nil {
			return nil, false
		}
		out := make(map[string]any, len(typed.Properties))
		for key, encoded := range typed.Properties {
			decoded := decodeProjectionPropertyValue(encoded)
			if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
				out[key] = temporal
				continue
			}
			out[key] = decoded
		}
		return out, true
	case map[string]any:
		if nested, ok := typed["properties"].(map[string]any); ok {
			out := make(map[string]any, len(nested))
			for key, v := range nested {
				if bytesValue, bytesOK := v.([]byte); bytesOK {
					decoded := decodeProjectionPropertyValue(bytesValue)
					if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
						out[key] = temporal
					} else {
						out[key] = decoded
					}
					continue
				}
				out[key] = v
			}
			return out, true
		}
		if nested, ok := typed["properties"].(map[string][]byte); ok {
			out := make(map[string]any, len(nested))
			for key, encoded := range nested {
				decoded := decodeProjectionPropertyValue(encoded)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					out[key] = temporal
				} else {
					out[key] = decoded
				}
			}
			return out, true
		}
		if nested, ok := typed["properties"].(graph.PropertyMap); ok {
			out := make(map[string]any, len(nested))
			for key, encoded := range nested {
				decoded := decodeProjectionPropertyValue(encoded)
				if temporal, ok := coerceProjectionStoredTemporalValue(decoded); ok {
					out[key] = temporal
				} else {
					out[key] = decoded
				}
			}
			return out, true
		}
		if _, temporal := projectionTemporalMap(typed); temporal {
			return nil, false
		}
		out := make(map[string]any, len(typed))
		for key, v := range typed {
			if key == "id" || key == "labels" || key == "type" || key == "src" || key == "dst" {
				continue
			}
			out[key] = v
		}
		if len(out) == 0 && state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
			entityID := strings.TrimSpace(scalarString(typed["id"]))
			if entityID == "" {
				entityID = strings.TrimSpace(scalarString(typed["ID"]))
			}
			if entityID != "" {
				if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
					return projectionPropertiesMapFromValue(varName, vertex, row, state)
				}
				if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
					return projectionPropertiesMapFromValue(varName, edge, row, state)
				}
				if srcID, edgeType, dstID, ok := projectionCanonicalEdgeIDParts(entityID); ok {
					var matched *graph.Edge
					_ = state.Tx.ScanOutEdges(context.Background(), state.Tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
						if edge == nil {
							return nil
						}
						if strings.TrimSpace(edge.DstID) != dstID {
							return nil
						}
						matched = edge
						return nil
					})
					if matched != nil {
						return projectionPropertiesMapFromValue(varName, matched, row, state)
					}
				}
			}
		}
		return out, true
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, v := range typed {
			out[key] = v
		}
		return out, true
	}

	entityID := scalarString(value)
	if entityID == "" && row != nil {
		entityID = scalarString(row[varName+".id"])
	}
	if entityID == "" || state == nil || state.Tx == nil || strings.TrimSpace(state.Tenant) == "" {
		return nil, false
	}
	if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
		return projectionPropertiesMapFromValue(varName, vertex, row, state)
	}
	if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
		return projectionPropertiesMapFromValue(varName, edge, row, state)
	}
	if srcID, edgeType, dstID, ok := projectionCanonicalEdgeIDParts(entityID); ok {
		var matched *graph.Edge
		_ = state.Tx.ScanOutEdges(context.Background(), state.Tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			if strings.TrimSpace(edge.DstID) != dstID {
				return nil
			}
			matched = edge
			return nil
		})
		if matched != nil {
			return projectionPropertiesMapFromValue(varName, matched, row, state)
		}
	}
	return nil, false
}

func projectionCanonicalEdgeIDParts(edgeID string) (string, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(edgeID), "|")
	if len(parts) != 4 {
		return "", "", "", false
	}
	srcID := strings.TrimSpace(parts[1])
	edgeType := strings.TrimSpace(parts[2])
	dstID := strings.TrimSpace(parts[3])
	if srcID == "" || edgeType == "" || dstID == "" {
		return "", "", "", false
	}
	return srcID, edgeType, dstID, true
}

func resolveProjectionFunctionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	lower := strings.ToLower(strings.TrimSpace(expr))
	if strings.HasPrefix(lower, "exists(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("exists(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		return value != nil, true
	}
	if strings.HasPrefix(lower, "id(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("id(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case *graph.Vertex:
			if typed == nil {
				return nil, true
			}
			return strings.TrimSpace(typed.ID), true
		case *graph.Edge:
			if typed == nil {
				return nil, true
			}
			return strings.TrimSpace(typed.ID), true
		}
		id := strings.TrimSpace(scalarString(value))
		if id == "" && row != nil {
			if boundID, hasBound := row[arg+".id"]; hasBound {
				id = strings.TrimSpace(scalarString(boundID))
			}
		}
		if id == "" {
			return nil, true
		}
		return id, true
	}
	if strings.HasPrefix(lower, "tolower(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toLower(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		raw, stringOK := runtimeStringValue(value)
		if !stringOK {
			return nil, true
		}
		return strings.ToLower(raw), true
	}
	if strings.HasPrefix(lower, "toupper(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toUpper(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		raw, stringOK := runtimeStringValue(value)
		if !stringOK {
			return nil, true
		}
		return strings.ToUpper(raw), true
	}
	if strings.HasPrefix(lower, "head(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("head(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		list, ok := projectionListValue(value)
		if !ok {
			return nil, true
		}
		if len(list) == 0 {
			return nil, true
		}
		return list[0], true
	}
	if strings.HasPrefix(lower, "tail(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("tail(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		list, ok := projectionListValue(value)
		if !ok {
			return nil, true
		}
		if len(list) == 0 {
			return []any{}, true
		}
		tail := append([]any(nil), list[1:]...)
		if len(tail) == 0 {
			return []any{}, true
		}
		return tail, true
	}
	if strings.HasPrefix(lower, "last(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("last(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		list, ok := projectionListValue(value)
		if !ok {
			return nil, true
		}
		if len(list) == 0 {
			return nil, true
		}
		return list[len(list)-1], true
	}
	if strings.HasPrefix(lower, "nodes(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("nodes(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		if path, ok := value.(projectionPathValue); ok {
			return append([]any(nil), path.nodes...), true
		}
		if path, ok := value.(map[string]any); ok {
			if nodesRaw, exists := path["nodes"]; exists {
				if list, listOK := projectionListValue(nodesRaw); listOK {
					return append([]any(nil), list...), true
				}
			}
		}
		return nil, true
	}
	if strings.HasPrefix(lower, "relationships(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("relationships(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		if path, ok := value.(projectionPathValue); ok {
			return append([]any(nil), path.relationships...), true
		}
		if path, ok := value.(map[string]any); ok {
			if relsRaw, exists := path["relationships"]; exists {
				if list, listOK := projectionListValue(relsRaw); listOK {
					return append([]any(nil), list...), true
				}
			}
		}
		return nil, true
	}
	if strings.HasPrefix(lower, "type(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("type(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case *graph.Edge:
			if typed == nil {
				return nil, true
			}
			return strings.TrimSpace(typed.Type), true
		case string:
			if edgeType, ok := projectionEdgeTypeFromID(typed); ok {
				return edgeType, true
			}
		case map[string]any:
			rawType, hasType := typed["type"]
			_, hasSrc := typed["src"]
			_, hasDst := typed["dst"]
			if hasType && hasSrc && hasDst {
				if edgeType, ok := rawType.(string); ok {
					return strings.TrimSpace(edgeType), true
				}
			}
			if rawID, hasID := typed["id"]; hasID {
				if edgeType, ok := projectionEdgeTypeFromID(scalarString(rawID)); ok {
					return edgeType, true
				}
			}
		}
		if row != nil {
			if boundID, exists := row[arg+".id"]; exists {
				if edgeType, ok := projectionEdgeTypeFromID(scalarString(boundID)); ok {
					return edgeType, true
				}
			}
		}
		projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
		return nil, true
	}
	if strings.HasPrefix(lower, "abs(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("abs(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case int:
			if typed < 0 {
				return -typed, true
			}
			return typed, true
		case int32:
			if typed < 0 {
				return int(-typed), true
			}
			return int(typed), true
		case int64:
			if typed < 0 {
				return int(-typed), true
			}
			return int(typed), true
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok {
			return nil, true
		}
		return math.Abs(numeric), true
	}
	if strings.HasPrefix(lower, "sign(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("sign(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok {
			return nil, true
		}
		switch {
		case numeric < 0:
			return -1, true
		case numeric > 0:
			return 1, true
		default:
			return 0, true
		}
	}
	if strings.HasPrefix(lower, "sqrt(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("sqrt(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok || numeric < 0 {
			return nil, true
		}
		return math.Sqrt(numeric), true
	}
	if strings.HasPrefix(lower, "ceil(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("ceil(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		return math.Ceil(numeric), true
	}
	if strings.HasPrefix(lower, "ceiling(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("ceiling(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		numeric, ok := projectionNumericToFloat(value)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		return math.Ceil(numeric), true
	}
	if strings.HasPrefix(lower, "vector.similarity.euclidean(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("vector.similarity.euclidean(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) != 2 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "vector.similarity.euclidean() expects exactly two arguments", nil))
			return nil, true
		}
		left, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		right, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		if left == nil || right == nil {
			return nil, true
		}
		leftValues, ok := projectionVectorCoordinateList(left)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		rightValues, ok := projectionVectorCoordinateList(right)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		if len(leftValues) != len(rightValues) || len(leftValues) == 0 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		sumSquares := 0.0
		for i := range leftValues {
			delta := leftValues[i] - rightValues[i]
			sumSquares += delta * delta
		}
		return 1.0 / (1.0 + sumSquares), true
	}
	if strings.HasPrefix(lower, "vector_distance(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("vector_distance(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) != 3 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "vector_distance() expects exactly three arguments", nil))
			return nil, true
		}
		left, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		right, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		if left == nil || right == nil {
			return nil, true
		}
		metric := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
		if !strings.EqualFold(metric, "EUCLIDEAN") {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		leftValues, ok := projectionVectorCoordinateList(left)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		rightValues, ok := projectionVectorCoordinateList(right)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		if len(leftValues) != len(rightValues) || len(leftValues) == 0 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		sumSquares := 0.0
		for i := range leftValues {
			delta := leftValues[i] - rightValues[i]
			sumSquares += delta * delta
		}
		return math.Sqrt(sumSquares), true
	}
	if strings.HasPrefix(lower, "point(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("point(") : len(expr)-1])
		if arg == "" {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "point() expects one argument", nil))
			return nil, true
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		m, ok := value.(map[string]any)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		if lonRaw, hasLon := m["longitude"]; hasLon {
			latRaw, hasLat := m["latitude"]
			if !hasLat || lonRaw == nil || latRaw == nil {
				return nil, true
			}
			lon, lonOK := projectionNumericToFloat(lonRaw)
			lat, latOK := projectionNumericToFloat(latRaw)
			if !lonOK || !latOK {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
				return nil, true
			}
			return map[string]any{"longitude": lon, "latitude": lat, "x": lon, "y": lat}, true
		}
		xRaw, hasX := m["x"]
		yRaw, hasY := m["y"]
		if !hasX || !hasY || xRaw == nil || yRaw == nil {
			return nil, true
		}
		x, xOK := projectionNumericToFloat(xRaw)
		y, yOK := projectionNumericToFloat(yRaw)
		if !xOK || !yOK {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		return map[string]any{"x": x, "y": y}, true
	}
	if strings.HasPrefix(lower, "point.withinbbox(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("point.withinBBox(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) != 3 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "point.withinBBox() expects exactly three arguments", nil))
			return nil, true
		}
		pointValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		lowerLeftValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		upperRightValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[2]), row, state)
		if !ok {
			return nil, false
		}
		if pointValue == nil || lowerLeftValue == nil || upperRightValue == nil {
			return nil, true
		}
		px, py, pGeo, ok := projectionPointXY(pointValue)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		llx, lly, llGeo, ok := projectionPointXY(lowerLeftValue)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		urx, ury, urGeo, ok := projectionPointXY(upperRightValue)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		if pGeo != llGeo || pGeo != urGeo {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		inY := py >= math.Min(lly, ury) && py <= math.Max(lly, ury)
		inX := false
		if pGeo && llx > urx {
			inX = px >= llx || px <= urx
		} else {
			inX = px >= math.Min(llx, urx) && px <= math.Max(llx, urx)
		}
		return inX && inY, true
	}
	if strings.HasPrefix(lower, "rand(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("rand(") : len(expr)-1])
		if arg != "" {
			return nil, false
		}
		return rand.Float64(), true
	}
	if strings.HasPrefix(lower, "reverse(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("reverse(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		if text, ok := value.(string); ok {
			runes := []rune(text)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes), true
		}
		list, ok := projectionListValue(value)
		if !ok {
			return nil, true
		}
		out := make([]any, len(list))
		for i := range list {
			out[len(list)-1-i] = list[i]
		}
		return out, true
	}
	if strings.HasPrefix(lower, "split(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("split(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) != 2 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "split() expects exactly two arguments", nil))
			return nil, true
		}
		input, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		delim, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		if input == nil || delim == nil {
			return nil, true
		}
		inputStr, ok := input.(string)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		delimStr, ok := delim.(string)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		split := strings.Split(inputStr, delimStr)
		out := make([]any, 0, len(split))
		for _, item := range split {
			out = append(out, item)
		}
		return out, true
	}
	if strings.HasPrefix(lower, "substring(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("substring(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) != 2 && len(parts) != 3 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindSemantic, "substring() expects two or three arguments", nil))
			return nil, true
		}
		input, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		startValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		if input == nil || startValue == nil {
			return nil, true
		}
		inputStr, ok := input.(string)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		start, ok := projectionNumericToInt(startValue)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		runes := []rune(inputStr)
		if start < 0 {
			start = len(runes) + start
		}
		if start < 0 {
			start = 0
		}
		if start > len(runes) {
			return "", true
		}
		end := len(runes)
		if len(parts) == 3 {
			lengthValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[2]), row, state)
			if !ok {
				return nil, false
			}
			if lengthValue == nil {
				return nil, true
			}
			length, ok := projectionNumericToInt(lengthValue)
			if !ok {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
				return nil, true
			}
			if length <= 0 {
				return "", true
			}
			end = start + length
			if end > len(runes) {
				end = len(runes)
			}
		}
		if start > end {
			return "", true
		}
		return string(runes[start:end]), true
	}
	if strings.HasPrefix(lower, "coalesce(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("coalesce(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, ok := resolveProjectionExprValue(part, row, state)
			if !ok {
				return nil, false
			}
			if value != nil {
				return value, true
			}
		}
		return nil, true
	}
	if strings.HasPrefix(lower, "size(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("size(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		if count, handled := resolveProjectionSizePatternComprehension(arg, row, state); handled {
			return count, true
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case string:
			return len([]rune(typed)), true
		case []any:
			return len(typed), true
		case []string:
			return len(typed), true
		case map[string]any:
			return len(typed), true
		case map[string]string:
			return len(typed), true
		default:
			return nil, true
		}
	}
	if strings.HasPrefix(lower, "length(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("length(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case projectionPathValue:
			return len(typed.relationships), true
		case map[string]any:
			if relsRaw, exists := typed["relationships"]; exists {
				if list, listOK := projectionListValue(relsRaw); listOK {
					return len(list), true
				}
			}
			if nodesRaw, exists := typed["nodes"]; exists {
				if list, listOK := projectionListValue(nodesRaw); listOK {
					if len(list) == 0 {
						return 0, true
					}
					return len(list) - 1, true
				}
			}
			return nil, true
		default:
			return nil, true
		}
	}
	if strings.HasPrefix(lower, "keys(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("keys(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		props, ok := projectionPropertiesMapFromValue(arg, value, row, state)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		keys := make([]string, 0, len(props))
		for key := range props {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]any, 0, len(keys))
		for _, key := range keys {
			out = append(out, key)
		}
		return out, true
	}
	if strings.HasPrefix(lower, "properties(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("properties(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		props, ok := projectionPropertiesMapFromValue(arg, value, row, state)
		if !ok {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil))
			return nil, true
		}
		return props, true
	}
	if strings.HasPrefix(lower, "range(") && strings.HasSuffix(lower, ")") {
		argList := strings.TrimSpace(expr[len("range(") : len(expr)-1])
		parts := splitProjectionTopLevelByComma(argList)
		if len(parts) < 2 || len(parts) > 3 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "range() expects 2 or 3 arguments", nil))
			return nil, true
		}
		startValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
		if !ok {
			return nil, false
		}
		endValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
		if !ok {
			return nil, false
		}
		if startValue == nil || endValue == nil {
			return nil, true
		}
		startExpr := strings.TrimSpace(parts[0])
		endExpr := strings.TrimSpace(parts[1])
		start, startOK := projectionRangeIntegerArg(startExpr, startValue)
		end, endOK := projectionRangeIntegerArg(endExpr, endValue)
		if !startOK {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "range() start must be an integer", nil))
			return nil, true
		}
		if !endOK {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "range() end must be an integer", nil))
			return nil, true
		}

		step := 1
		if len(parts) == 3 {
			stepValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[2]), row, state)
			if !ok {
				return nil, false
			}
			if stepValue == nil {
				return nil, true
			}
			stepExpr := strings.TrimSpace(parts[2])
			step, ok = projectionRangeIntegerArg(stepExpr, stepValue)
			if !ok {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "range() step must be an integer", nil))
				return nil, true
			}
		}
		if step == 0 {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "range() step cannot be zero", nil))
			return nil, true
		}

		if len(parts) == 2 && start > end {
			return []any{}, true
		}
		if len(parts) == 3 {
			if (step > 0 && start > end) || (step < 0 && start < end) {
				return []any{}, true
			}
		}

		out := make([]any, 0)
		if step > 0 {
			for current := start; current <= end; current += step {
				out = append(out, int(current))
			}
		} else {
			for current := start; current >= end; current += step {
				out = append(out, int(current))
			}
		}
		return out, true
	}
	if value, ok := resolveProjectionQuantifierExprValue(expr, row, state); ok {
		return value, true
	}
	if strings.HasPrefix(lower, "toboolean(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toBoolean(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case bool:
			return typed, true
		case string:
			lowerText := strings.ToLower(typed)
			if lowerText == "true" {
				return true, true
			}
			if lowerText == "false" {
				return false, true
			}
			return nil, true
		default:
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
	}
	if strings.HasPrefix(lower, "tointeger(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toInteger(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case int:
			return typed, true
		case int32:
			return int(typed), true
		case int64:
			return int(typed), true
		case float32:
			return int(math.Trunc(float64(typed))), true
		case float64:
			return int(math.Trunc(typed)), true
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
			if err != nil {
				return nil, true
			}
			return int(math.Trunc(parsed)), true
		default:
			if projectionIsInvalidScalarConversionType(value) {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
				return nil, true
			}
			return nil, true
		}
	}
	if strings.HasPrefix(lower, "tofloat(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toFloat(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		switch typed := value.(type) {
		case int:
			return float64(typed), true
		case int32:
			return float64(typed), true
		case int64:
			return float64(typed), true
		case float32:
			return float64(typed), true
		case float64:
			return typed, true
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
			if err != nil {
				return nil, true
			}
			return parsed, true
		case bool:
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		default:
			if projectionIsInvalidScalarConversionType(value) {
				projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
				return nil, true
			}
			return nil, true
		}
	}
	if strings.HasPrefix(lower, "tostring(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("toString(") : len(expr)-1])
		if arg == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(arg, row, state)
		if !ok {
			return nil, false
		}
		if value == nil {
			return nil, true
		}
		if projectionIsInvalidScalarConversionType(value) {
			projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
			return nil, true
		}
		if temporal, ok := coerceProjectionStoredTemporalValue(value); ok {
			if typed, ok := temporal.(map[string]any); ok {
				if rendered, ok := projectionTemporalToString(typed); ok {
					return rendered, true
				}
			}
		}
		switch typed := value.(type) {
		case bool:
			if typed {
				return "true", true
			}
			return "false", true
		case int:
			return strconv.Itoa(typed), true
		case int32:
			return strconv.FormatInt(int64(typed), 10), true
		case int64:
			return strconv.FormatInt(typed, 10), true
		case float32:
			return strconv.FormatFloat(float64(typed), 'f', -1, 64), true
		case float64:
			return strconv.FormatFloat(typed, 'f', -1, 64), true
		case string:
			return typed, true
		default:
			return fmt.Sprint(value), true
		}
	}
	if strings.HasPrefix(lower, "startnode(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("startNode(") : len(expr)-1])
		if id, ok := resolveProjectionEdgeEndpointID(arg, true, row, state); ok {
			return buildProjectionNodeValue(id, row, state), true
		}
		return nil, false
	}
	if strings.HasPrefix(lower, "endnode(") && strings.HasSuffix(lower, ")") {
		arg := strings.TrimSpace(expr[len("endNode(") : len(expr)-1])
		if id, ok := resolveProjectionEdgeEndpointID(arg, false, row, state); ok {
			return buildProjectionNodeValue(id, row, state), true
		}
		return nil, false
	}
	if value, ok := resolveProjectionTemporalNamespaceMethodExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionTemporalTruncateExprValue(expr, row, state); ok {
		return value, true
	}
	if value, ok := resolveProjectionTemporalFunctionExprValue(expr, row, state); ok {
		return value, true
	}
	if !strings.HasPrefix(lower, "labels(") || !strings.HasSuffix(lower, ")") {
		return nil, false
	}
	arg := strings.TrimSpace(expr[len("labels(") : len(expr)-1])
	if arg == "" {
		return nil, false
	}
	if state == nil || state.Tx == nil || strings.TrimSpace(state.Tenant) == "" {
		return nil, false
	}
	if row != nil {
		if labels, ok := row[arg+".labels"]; ok {
			switch typed := labels.(type) {
			case []string:
				return append([]string(nil), typed...), true
			case []any:
				out := make([]string, 0, len(typed))
				for _, item := range typed {
					out = append(out, fmt.Sprint(item))
				}
				return out, true
			}
		}
	}
	base, ok := resolveProjectionExprValue(arg, row, state)
	if !ok {
		if row != nil {
			if bound, exists := row[arg]; exists {
				base = bound
				ok = true
			} else if boundID, exists := row[arg+".id"]; exists {
				base = boundID
				ok = true
			}
		}
		if !ok {
			return nil, false
		}
	}
	if base == nil {
		return nil, true
	}

	switch typed := base.(type) {
	case *graph.Vertex:
		return append([]string(nil), typed.Labels...), true
	case map[string]any:
		if rawID, exists := typed["id"]; exists {
			vertexID := strings.TrimSpace(scalarString(rawID))
			if vertexID != "" {
				vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, vertexID)
				if err == nil && vertex != nil {
					return append([]string(nil), vertex.Labels...), true
				}
			}
		}
		if labels, ok := typed["labels"]; ok {
			switch l := labels.(type) {
			case []string:
				return append([]string(nil), l...), true
			case []any:
				out := make([]string, 0, len(l))
				for _, item := range l {
					out = append(out, fmt.Sprint(item))
				}
				return out, true
			}
		}
	}
	if _, isString := base.(string); !isString {
		if row != nil {
			if _, exists := row[arg]; exists {
				if len(materializedDeletedVertexIDs(row)) > 0 || len(materializedGlobalDeletedVertexIDs(state)) > 0 {
					projectionSetEvalError(state, graph.NewError(graph.ErrKindNotFound, "vertex not found", nil))
					return nil, true
				}
			}
		}
		projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
		return nil, true
	}
	vertexID := ""
	if row != nil {
		vertexID = strings.TrimSpace(scalarString(row[arg+".id"]))
	}
	if vertexID == "" {
		vertexID = strings.TrimSpace(scalarString(base))
	}
	if vertexID == "" {
		if row != nil {
			if _, exists := row[arg]; exists {
				if len(materializedDeletedVertexIDs(row)) > 0 || len(materializedGlobalDeletedVertexIDs(state)) > 0 {
					projectionSetEvalError(state, graph.NewError(graph.ErrKindNotFound, "vertex not found", nil))
					return nil, true
				}
			}
		}
		projectionSetEvalError(state, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil))
		return nil, true
	}
	if projectionIsDeletedVertexID(state, row, vertexID) {
		projectionSetEvalError(state, graph.NewError(graph.ErrKindNotFound, "vertex not found", nil))
		return nil, true
	}
	vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, vertexID)
	if err == nil && vertex != nil {
		return append([]string(nil), vertex.Labels...), true
	}
	if err != nil {
		return nil, false
	}
	return nil, false
}

func resolveProjectionSizePatternComprehension(arg string, row map[string]any, state *State) (int, bool) {
	raw := strings.TrimSpace(arg)
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return 0, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	parts := strings.SplitN(body, "|", 2)
	if len(parts) != 2 {
		return 0, false
	}
	pattern := strings.TrimSpace(parts[0])
	projection := strings.TrimSpace(parts[1])
	if projection != "1" {
		return 0, false
	}
	if !strings.HasPrefix(pattern, "(") || !strings.Contains(pattern, ")-->") {
		return 0, false
	}
	// Only take the fast path when the right-node pattern is unconstrained "()"
	// or trivially anonymous without labels/props.  Any label constraint on the
	// destination (e.g. (:Y)) requires the full evaluator so label filtering is
	// respected.
	arrowIdx := strings.Index(pattern, ")-->")
	if arrowIdx < 0 {
		return 0, false
	}
	rightPart := strings.TrimSpace(pattern[arrowIdx+len(")-->"):])
	if rightPart != "()" {
		// Non-trivial right-node constraint; let the full evaluator handle it.
		return 0, false
	}
	closeIdx := strings.Index(pattern, ")")
	if closeIdx <= 1 {
		return 0, false
	}
	varName := strings.TrimSpace(pattern[1:closeIdx])
	if !isIdentifierToken(varName) {
		return 0, false
	}
	nodeID, ok := projectionBoundNodeID(varName, row)
	if !ok {
		return 0, false
	}
	if state == nil || state.Tx == nil || strings.TrimSpace(state.Tenant) == "" {
		return 0, false
	}
	count := 0
	err := state.Tx.ScanOutEdges(context.Background(), state.Tenant, nodeID, "", 0, func(_ *graph.Edge) error {
		count++
		return nil
	})
	if err != nil {
		return 0, false
	}
	return count, true
}

func projectionBoundNodeID(varName string, row map[string]any) (string, bool) {
	if row == nil || strings.TrimSpace(varName) == "" {
		return "", false
	}
	if value, ok := row[varName+".id"]; ok {
		if id := strings.TrimSpace(scalarString(value)); id != "" {
			return id, true
		}
	}
	value, ok := row[varName]
	if !ok || value == nil {
		return "", false
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return "", false
		}
		if id := strings.TrimSpace(typed.ID); id != "" {
			return id, true
		}
	case map[string]any:
		if rawID, exists := typed["id"]; exists {
			if id := strings.TrimSpace(scalarString(rawID)); id != "" {
				return id, true
			}
		}
	}
	if id := strings.TrimSpace(scalarString(value)); id != "" {
		return id, true
	}
	return "", false
}

func resolveProjectionTemporalNamespaceMethodExprValue(expr string, row map[string]any, state *State) (any, bool) {
	name, argList, ok := splitProjectionTopLevelFunctionCall(expr)
	if !ok {
		return nil, false
	}
	parts := strings.SplitN(strings.ToLower(strings.TrimSpace(name)), ".", 2)
	if len(parts) != 2 {
		return nil, false
	}
	namespace := parts[0]
	method := strings.TrimSpace(parts[1])
	if namespace != "datetime" && namespace != "duration" {
		return nil, false
	}

	argsRaw := splitProjectionTopLevelByComma(argList)
	args := make([]any, 0, len(argsRaw))
	for _, raw := range argsRaw {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		value, ok := resolveProjectionExprValue(raw, row, state)
		if !ok {
			return nil, false
		}
		args = append(args, value)
	}

	if namespace == "duration" {
		if value, ok := projectionEvalDurationMethod(method, args); ok {
			return value, true
		}
		return nil, false
	}

	switch method {
	case "fromepoch":
		if len(args) < 1 || len(args) > 2 {
			return nil, false
		}
		seconds, ok := projectionNumericToFloat(args[0])
		if !ok {
			return nil, false
		}
		nanos := 0.0
		if len(args) == 2 {
			if v, ok := projectionNumericToFloat(args[1]); ok {
				nanos = v
			} else {
				return nil, false
			}
		}
		t := time.Unix(int64(seconds), int64(nanos)).UTC()
		return map[string]any{
			"__temporal_type": "datetime",
			"year":            t.Year(),
			"month":           int(t.Month()),
			"day":             t.Day(),
			"hour":            t.Hour(),
			"minute":          t.Minute(),
			"second":          t.Second(),
			"nanosecond":      t.Nanosecond(),
			"timezone":        "Z",
		}, true
	case "fromepochmillis":
		if len(args) != 1 {
			return nil, false
		}
		millis, ok := projectionNumericToFloat(args[0])
		if !ok {
			return nil, false
		}
		t := time.Unix(0, int64(millis*1_000_000)).UTC()
		return map[string]any{
			"__temporal_type": "datetime",
			"year":            t.Year(),
			"month":           int(t.Month()),
			"day":             t.Day(),
			"hour":            t.Hour(),
			"minute":          t.Minute(),
			"second":          t.Second(),
			"nanosecond":      t.Nanosecond(),
			"timezone":        "Z",
		}, true
	default:
		return nil, false
	}
}

type projectionDurationInstant struct {
	year     int
	month    int
	day      int
	hour     int
	minute   int
	second   int
	nano     int
	timezone string
	hasDate  bool
	hasTime  bool
	hasZone  bool
}

type projectionDurationClock struct {
	secondOfDay float64
	hasZone     bool
	offset      int
}

func projectionEvalDurationMethod(method string, args []any) (any, bool) {
	if len(args) != 2 {
		return nil, false
	}
	if args[0] == nil || args[1] == nil {
		return nil, true
	}
	left, ok := projectionCoerceDurationInstant(args[0])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, true
	}
	right, ok := projectionCoerceDurationInstant(args[1])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, true
	}

	if left.hasZone != right.hasZone {
		if left.hasZone {
			right.timezone = left.timezone
			right.hasZone = true
		} else {
			left.timezone = right.timezone
			left.hasZone = true
		}
	}

	methodKey := strings.ToLower(strings.TrimSpace(method))
	if methodKey == "inseconds" || methodKey == "between" {
		if left.hasDate && !right.hasDate {
			right.year, right.month, right.day = left.year, left.month, left.day
			right.hasDate = true
		}
		if right.hasDate && !left.hasDate {
			left.year, left.month, left.day = right.year, right.month, right.day
			left.hasDate = true
		}
	}

	if (methodKey == "inseconds" || methodKey == "between") && !left.hasDate && !right.hasDate {
		lClock := projectionDurationInstantClock(left)
		rClock := projectionDurationInstantClock(right)
		delta := rClock.secondOfDay - lClock.secondOfDay
		if lClock.hasZone && rClock.hasZone {
			delta += float64(lClock.offset - rClock.offset)
		}
		return projectionBuildDurationResult(projectionDurationComponents{seconds: delta}), true
	}

	if methodKey == "inseconds" {
		if whole, nanos, ok := projectionDurationSecondsBetweenExact(left, right); ok {
			return map[string]any{
				"__temporal_type":     "duration",
				"years":               0,
				"months":              0,
				"days":                0,
				"seconds":             whole,
				"nanoseconds":         nanos,
				"nanosecondsOfSecond": nanos,
				"__duration_exact":    true,
			}, true
		}
	}

	t1, ok1 := projectionDurationInstantToTime(left)
	t2, ok2 := projectionDurationInstantToTime(right)
	if !ok1 || !ok2 {
		return map[string]any{"__temporal_type": "duration"}, true
	}

	switch methodKey {
	case "inseconds":
		return projectionBuildDurationResult(projectionDurationComponents{seconds: t2.Sub(t1).Seconds()}), true
	case "indays":
		if !(left.hasDate && right.hasDate) {
			return projectionBuildDurationResult(projectionDurationComponents{}), true
		}
		return projectionBuildDurationResult(projectionDurationComponents{days: math.Trunc(t2.Sub(t1).Hours() / 24)}), true
	case "inmonths":
		if !(left.hasDate && right.hasDate) {
			return projectionBuildDurationResult(projectionDurationComponents{}), true
		}
		months := (right.year-left.year)*12 + (right.month - left.month)
		anchor := t1.AddDate(0, months, 0)
		if months > 0 && anchor.After(t2) {
			months--
		}
		if months < 0 && anchor.Before(t2) {
			months++
		}
		return projectionBuildDurationResult(projectionDurationComponents{months: float64(months)}), true
	case "between":
		if left.hasDate && right.hasDate {
			months := (right.year-left.year)*12 + (right.month - left.month)
			anchor := t1.AddDate(0, months, 0)
			if months > 0 && anchor.After(t2) {
				months--
				anchor = t1.AddDate(0, months, 0)
			}
			if months < 0 && anchor.Before(t2) {
				months++
				anchor = t1.AddDate(0, months, 0)
			}
			days := int(math.Trunc(t2.Sub(anchor).Hours() / 24))
			anchor = anchor.AddDate(0, 0, days)
			return projectionBuildDurationResult(projectionDurationComponents{
				months:  float64(months),
				days:    float64(days),
				seconds: t2.Sub(anchor).Seconds(),
			}), true
		}
		return projectionBuildDurationResult(projectionDurationComponents{seconds: t2.Sub(t1).Seconds()}), true
	default:
		return nil, false
	}
}

type projectionDurationComponents struct {
	months  float64
	days    float64
	seconds float64
}

func (d projectionDurationComponents) add(other projectionDurationComponents) projectionDurationComponents {
	return projectionDurationComponents{months: d.months + other.months, days: d.days + other.days, seconds: d.seconds + other.seconds}
}

func (d projectionDurationComponents) sub(other projectionDurationComponents) projectionDurationComponents {
	return projectionDurationComponents{months: d.months - other.months, days: d.days - other.days, seconds: d.seconds - other.seconds}
}

func (d projectionDurationComponents) scale(factor float64) projectionDurationComponents {
	return projectionDurationComponents{months: d.months * factor, days: d.days * factor, seconds: d.seconds * factor}
}

func projectionBuildDurationResult(dur projectionDurationComponents) map[string]any {
	dur = projectionNormalizeDurationComponents(dur)
	out := map[string]any{"__temporal_type": "duration"}
	totalMonths := int(projectionTruncTowardZero(dur.months))
	out["years"] = int(projectionTruncTowardZero(float64(totalMonths) / 12))
	out["months"] = totalMonths - out["years"].(int)*12
	out["days"] = int(projectionTruncTowardZero(dur.days))
	whole, nanos := projectionSplitSecondsAndNanos(dur.seconds)
	out["seconds"] = whole
	out["nanoseconds"] = nanos
	out["nanosecondsOfSecond"] = nanos
	return out
}

func projectionNormalizeDurationComponents(dur projectionDurationComponents) projectionDurationComponents {
	const avgMonthSeconds = 2629746.0
	wholeMonths := projectionTruncTowardZero(dur.months)
	fracMonths := dur.months - wholeMonths
	if fracMonths != 0 {
		monthSeconds := fracMonths * avgMonthSeconds
		wholeDaysFromMonth := projectionTruncTowardZero(monthSeconds / 86400)
		dur.days += wholeDaysFromMonth
		dur.seconds += monthSeconds - wholeDaysFromMonth*86400
	}
	wholeDays := projectionTruncTowardZero(dur.days)
	fracDays := dur.days - wholeDays
	if fracDays != 0 {
		dur.seconds += fracDays * 86400
		dur.days = wholeDays
	}
	secWhole, secNanos := projectionSplitSecondsAndNanos(dur.seconds)
	dur.months = wholeMonths
	dur.days = projectionTruncTowardZero(dur.days)
	dur.seconds = float64(secWhole) + float64(secNanos)/1_000_000_000
	return dur
}

func projectionSplitSecondsAndNanos(seconds float64) (int, int) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, 0
	}
	whole := int(math.Floor(seconds))
	frac := seconds - float64(whole)
	nanos := int(math.Round(frac * 1_000_000_000))
	if nanos >= 1_000_000_000 {
		whole++
		nanos -= 1_000_000_000
	}
	if nanos < 0 {
		nanos = 0
	}
	return whole, nanos
}

func projectionDurationComponentsFromMap(value map[string]any) projectionDurationComponents {
	seconds := 3600*mapFloatProjection(value, "hours") + 60*mapFloatProjection(value, "minutes") + mapFloatProjection(value, "seconds")
	months := 12*mapFloatProjection(value, "years") + mapFloatProjection(value, "months")
	days := 7*mapFloatProjection(value, "weeks") + mapFloatProjection(value, "days")

	const avgMonthSeconds = 2629746.0
	wholeMonths := projectionTruncTowardZero(months)
	fracMonths := months - wholeMonths
	if fracMonths != 0 {
		monthSeconds := fracMonths * avgMonthSeconds
		wholeDays := projectionTruncTowardZero(monthSeconds / 86400)
		days += wholeDays
		seconds += monthSeconds - wholeDays*86400
	}
	wholeDays := projectionTruncTowardZero(days)
	fracDays := days - wholeDays
	if fracDays != 0 {
		seconds += fracDays * 86400
		days = wholeDays
	}

	var nanosAcc int64
	if ms, ok := projectionMapWholeInt64(value, "milliseconds"); ok {
		nanosAcc += ms * 1_000_000
	} else {
		seconds += mapFloatProjection(value, "milliseconds") / 1_000
	}
	if us, ok := projectionMapWholeInt64(value, "microseconds"); ok {
		nanosAcc += us * 1_000
	} else {
		seconds += mapFloatProjection(value, "microseconds") / 1_000_000
	}
	if ns, ok := projectionMapWholeInt64(value, "nanoseconds"); ok {
		nanosAcc += ns
	} else {
		seconds += mapFloatProjection(value, "nanoseconds") / 1_000_000_000
	}
	seconds += float64(nanosAcc) / 1_000_000_000
	secWhole, secNanos := projectionSplitSecondsAndNanos(seconds)
	seconds = float64(secWhole) + float64(secNanos)/1_000_000_000

	return projectionDurationComponents{months: wholeMonths, days: days, seconds: seconds}
}

func projectionMapWholeInt64(value map[string]any, key string) (int64, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	iv, ok := projectionNumericToInt(raw)
	if !ok {
		return 0, false
	}
	if fv, ok := projectionNumericToFloat(raw); ok {
		if math.Abs(fv-float64(iv)) > 1e-12 {
			return 0, false
		}
	}
	return int64(iv), true
}

func projectionApplyTemporalAndDuration(temporal map[string]any, dur projectionDurationComponents, op string) (any, bool) {
	if op != "+" && op != "-" {
		return nil, false
	}
	if op == "-" {
		dur = dur.scale(-1)
	}

	temporalType := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	year, yOK := projectionNumericToInt(temporal["year"])
	month, mOK := projectionNumericToInt(temporal["month"])
	day, dOK := projectionNumericToInt(temporal["day"])
	hour, _ := projectionNumericToInt(temporal["hour"])
	minute, _ := projectionNumericToInt(temporal["minute"])
	second, _ := projectionNumericToInt(temporal["second"])
	nanosecond := projectionTemporalNanoseconds(temporal)

	loc := time.UTC
	if tz := projectionTemporalTimezone(temporal); tz != "" {
		if offset, err := projectionParseOffsetSeconds(tz); err == nil {
			loc = time.FixedZone("", offset)
		}
	}

	baseYear, baseMonth, baseDay := 2000, 1, 1
	if yOK {
		baseYear = year
	}
	if mOK {
		baseMonth = month
	}
	if dOK {
		baseDay = day
	}

	base := time.Date(baseYear, time.Month(baseMonth), baseDay, hour, minute, second, nanosecond, loc)
	addY, addM, addD, addSeconds := projectionDecomposeDuration(dur)
	dateAdjusted := base.AddDate(addY, addM, addD)
	adjusted := dateAdjusted.Add(time.Duration(addSeconds * float64(time.Second)))

	return projectionTemporalResultFromTime(temporalType, adjusted, dateAdjusted, addSeconds, temporal)
}

func projectionTemporalResultFromTime(typeName string, adjusted time.Time, dateAdjusted time.Time, addSeconds float64, source map[string]any) (any, bool) {
	out := map[string]any{"__temporal_type": typeName}
	switch typeName {
	case "date":
		dayCarry := int(projectionTruncTowardZero(addSeconds / 86400))
		resolved := dateAdjusted.AddDate(0, 0, dayCarry)
		out["year"] = resolved.Year()
		out["month"] = int(resolved.Month())
		out["day"] = resolved.Day()
	case "localtime":
		out["hour"] = adjusted.Hour()
		out["minute"] = adjusted.Minute()
		out["second"] = adjusted.Second()
		out["nanosecond"] = adjusted.Nanosecond()
	case "time":
		out["hour"] = adjusted.Hour()
		out["minute"] = adjusted.Minute()
		out["second"] = adjusted.Second()
		out["nanosecond"] = adjusted.Nanosecond()
		if tz := projectionTemporalTimezone(source); tz != "" {
			out["timezone"] = tz
		} else {
			_, off := adjusted.Zone()
			out["timezone"] = projectionFormatOffsetString(off)
		}
	case "localdatetime":
		out["year"] = adjusted.Year()
		out["month"] = int(adjusted.Month())
		out["day"] = adjusted.Day()
		out["hour"] = adjusted.Hour()
		out["minute"] = adjusted.Minute()
		out["second"] = adjusted.Second()
		out["nanosecond"] = adjusted.Nanosecond()
	case "datetime":
		out["year"] = adjusted.Year()
		out["month"] = int(adjusted.Month())
		out["day"] = adjusted.Day()
		out["hour"] = adjusted.Hour()
		out["minute"] = adjusted.Minute()
		out["second"] = adjusted.Second()
		out["nanosecond"] = adjusted.Nanosecond()
		if tz := projectionTemporalTimezone(source); tz != "" {
			out["timezone"] = tz
		} else {
			_, off := adjusted.Zone()
			out["timezone"] = projectionFormatOffsetString(off)
		}
	default:
		return nil, false
	}
	return out, true
}

func projectionFormatOffsetString(offset int) string {
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	hours := offset / 3600
	minutes := (offset % 3600) / 60
	seconds := offset % 60
	if seconds == 0 {
		return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
}

func projectionDecomposeDuration(dur projectionDurationComponents) (int, int, int, float64) {
	const avgMonthSeconds = 2629746.0
	totalMonths := dur.months
	years := int(projectionTruncTowardZero(totalMonths / 12))
	remainingMonths := totalMonths - float64(years*12)
	months := int(projectionTruncTowardZero(remainingMonths))
	fracMonths := remainingMonths - float64(months)

	totalDays := dur.days + (fracMonths*avgMonthSeconds)/86400
	days := int(projectionTruncTowardZero(totalDays))
	fracDays := totalDays - float64(days)

	seconds := dur.seconds + fracDays*86400
	return years, months, days, seconds
}

func projectionCoerceDurationInstant(value any) (projectionDurationInstant, bool) {
	temporal, ok := projectionTemporalMap(value)
	if !ok {
		return projectionDurationInstant{}, false
	}
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	if typeName == "" || typeName == "duration" {
		return projectionDurationInstant{}, false
	}
	inst := projectionDurationInstant{}
	if y, yOK := projectionNumericToInt(temporal["year"]); yOK {
		if m, mOK := projectionNumericToInt(temporal["month"]); mOK {
			if d, dOK := projectionNumericToInt(temporal["day"]); dOK {
				inst.year, inst.month, inst.day = y, m, d
				inst.hasDate = true
			}
		}
	}
	if h, hOK := projectionNumericToInt(temporal["hour"]); hOK {
		inst.hour = h
		inst.hasTime = true
	}
	if m, mOK := projectionNumericToInt(temporal["minute"]); mOK {
		inst.minute = m
		inst.hasTime = true
	}
	if s, sOK := projectionNumericToInt(temporal["second"]); sOK {
		inst.second = s
		inst.hasTime = true
	}
	inst.nano = projectionTemporalNanoseconds(temporal)
	if inst.nano != 0 {
		inst.hasTime = true
	}
	inst.timezone = projectionTemporalTimezone(temporal)
	inst.hasZone = inst.timezone != ""

	switch typeName {
	case "date":
		inst.hasTime = false
	case "localtime":
		inst.hasDate = false
	case "time":
		inst.hasDate = false
		inst.hasZone = true
		if inst.timezone == "" {
			inst.timezone = "Z"
		}
	case "datetime":
		if inst.timezone == "" {
			inst.timezone = "Z"
			inst.hasZone = true
		}
	}
	return inst, true
}

func projectionDurationInstantClock(v projectionDurationInstant) projectionDurationClock {
	sec := float64(v.hour*3600+v.minute*60+v.second) + float64(v.nano)/1_000_000_000
	off, _ := projectionDurationInstantOffsetSeconds(v)
	return projectionDurationClock{secondOfDay: sec, hasZone: v.hasZone, offset: off}
}

func projectionDurationInstantOffsetSeconds(v projectionDurationInstant) (int, bool) {
	if !v.hasZone {
		return 0, false
	}
	if parsed, err := projectionParseOffsetSeconds(v.timezone); err == nil {
		return parsed, true
	}
	if v.hasDate {
		if loc, err := time.LoadLocation(v.timezone); err == nil {
			h, mi, s, n := 0, 0, 0, 0
			if v.hasTime {
				h, mi, s, n = v.hour, v.minute, v.second, v.nano
			}
			t := time.Date(v.year, time.Month(v.month), v.day, h, mi, s, n, loc)
			_, off := t.Zone()
			return off, true
		}
	}
	return 0, false
}

func projectionDurationInstantToTime(v projectionDurationInstant) (time.Time, bool) {
	year, month, day := 1970, 1, 1
	if v.hasDate {
		year, month, day = v.year, v.month, v.day
	}
	hour, minute, second, nano := 0, 0, 0, 0
	if v.hasTime {
		hour, minute, second, nano = v.hour, v.minute, v.second, v.nano
	}
	loc := time.UTC
	if v.hasZone {
		if off, err := projectionParseOffsetSeconds(v.timezone); err == nil {
			loc = time.FixedZone("", off)
		} else if named, err := time.LoadLocation(v.timezone); err == nil {
			loc = named
		}
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nano, loc), true
}

func projectionDurationSecondsBetweenExact(left, right projectionDurationInstant) (int64, int, bool) {
	if !(left.hasDate && right.hasDate) {
		return 0, 0, false
	}
	leftDays, ok := projectionDaysSinceEpoch(left.year, left.month, left.day)
	if !ok {
		return 0, 0, false
	}
	rightDays, ok := projectionDaysSinceEpoch(right.year, right.month, right.day)
	if !ok {
		return 0, 0, false
	}
	leftSec := int64(left.hour*3600 + left.minute*60 + left.second)
	rightSec := int64(right.hour*3600 + right.minute*60 + right.second)
	whole := (rightDays-leftDays)*86400 + (rightSec - leftSec)
	if left.hasZone && right.hasZone {
		leftOffset, _ := projectionDurationInstantOffsetSeconds(left)
		rightOffset, _ := projectionDurationInstantOffsetSeconds(right)
		whole += int64(leftOffset - rightOffset)
	}
	nanos := right.nano - left.nano
	if nanos < 0 {
		nanos += 1_000_000_000
		whole--
	}
	if nanos >= 1_000_000_000 {
		nanos -= 1_000_000_000
		whole++
	}
	return whole, nanos, true
}

func projectionDaysSinceEpoch(year, month, day int) (int64, bool) {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, false
	}
	a := (14 - month) / 12
	y := year + 4800 - a
	m := month + 12*a - 3
	jd := day + (153*m+2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
	const unixEpochJDN = 2440588
	return int64(jd - unixEpochJDN), true
}

func resolveProjectionTemporalTruncateExprValue(expr string, row map[string]any, state *State) (any, bool) {
	name, arg, ok := splitProjectionTopLevelFunctionCall(expr)
	if !ok {
		return nil, false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if !strings.HasSuffix(name, ".truncate") {
		return nil, false
	}
	targetType := strings.TrimSpace(strings.TrimSuffix(name, ".truncate"))
	switch targetType {
	case "date", "localdatetime", "datetime", "localtime", "time":
	default:
		return nil, false
	}

	parts := splitProjectionTopLevelByComma(arg)
	if len(parts) < 2 || len(parts) > 3 {
		return nil, false
	}

	unitValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[0]), row, state)
	if !ok {
		return nil, false
	}
	unit, ok := runtimeStringValue(unitValue)
	if !ok {
		return nil, false
	}
	unit = strings.ToLower(strings.TrimSpace(unit))
	if unit == "" {
		return nil, false
	}

	otherValue, ok := resolveProjectionExprValue(strings.TrimSpace(parts[1]), row, state)
	if !ok {
		return nil, false
	}
	otherTemporal, ok := projectionTemporalMap(otherValue)
	if !ok {
		return nil, false
	}

	base := map[string]any{"__temporal_type": targetType}
	mergeProjectionDateComponents(base, otherTemporal)
	mergeProjectionTimeComponents(base, otherTemporal)
	if tz, exists := otherTemporal["timezone"]; exists {
		base["timezone"] = tz
	}

	if targetType == "time" || targetType == "localtime" {
		if _, ok := base["hour"]; !ok {
			base["hour"] = 0
		}
		if _, ok := base["minute"]; !ok {
			base["minute"] = 0
		}
		if _, ok := base["second"]; !ok {
			base["second"] = 0
		}
		if _, ok := base["nanosecond"]; !ok {
			base["nanosecond"] = 0
		}
	}

	if targetType == "time" || targetType == "datetime" {
		if _, ok := base["timezone"]; !ok {
			base["timezone"] = "Z"
		}
	}

	overrides := map[string]any{}
	if len(parts) == 3 {
		mapExpr := strings.TrimSpace(parts[2])
		if strings.HasPrefix(mapExpr, "{") && strings.HasSuffix(mapExpr, "}") {
			parsedOverrides, ok := parseProjectionMapLiteral(mapExpr[1:len(mapExpr)-1], row, state)
			if !ok {
				return nil, false
			}
			overrides = parsedOverrides
		} else if value, ok := resolveProjectionExprValue(mapExpr, row, state); ok {
			if typed, ok := value.(map[string]any); ok {
				overrides = typed
			}
		}
	}

	truncated := applyProjectionTemporalTruncation(targetType, base, unit)
	if len(overrides) > 0 {
		applyProjectionTemporalOverrides(truncated, unit, overrides)
	}
	return truncated, true
}

func resolveProjectionTemporalFunctionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	name, arg, ok := splitProjectionTopLevelFunctionCall(expr)
	if !ok {
		return nil, false
	}
	targetType := strings.ToLower(strings.TrimSpace(name))
	switch targetType {
	case "date", "localtime", "time", "localdatetime", "datetime", "duration":
	default:
		return nil, false
	}
	arg = strings.TrimSpace(arg)
	if arg == "" {
		switch targetType {
		case "date":
			return map[string]any{"__temporal_type": "date", "year": 1970, "month": 1, "day": 1}, true
		case "localtime":
			return map[string]any{"__temporal_type": "localtime", "hour": 0, "minute": 0, "second": 0, "nanosecond": 0}, true
		case "time":
			return map[string]any{"__temporal_type": "time", "hour": 0, "minute": 0, "second": 0, "nanosecond": 0, "timezone": "Z"}, true
		case "localdatetime":
			return map[string]any{"__temporal_type": "localdatetime", "year": 1970, "month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0}, true
		case "datetime":
			return map[string]any{"__temporal_type": "datetime", "year": 1970, "month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0, "timezone": "Z"}, true
		default:
			return nil, false
		}
	}

	if literal, ok := parseProjectionStringLiteral(arg); ok {
		if parsed, ok := parseProjectionTemporalLiteralToMap(targetType, literal); ok {
			if targetType != "duration" {
				parsed["__temporal_type"] = targetType
			}
			return parsed, true
		}
		return literal, true
	}

	if strings.HasPrefix(arg, "{") && strings.HasSuffix(arg, "}") {
		values, ok := parseProjectionMapLiteral(arg[1:len(arg)-1], row, state)
		if !ok {
			return nil, false
		}
		temporal, ok := buildProjectionTemporalValue(targetType, values)
		if !ok {
			return nil, false
		}
		return temporal, true
	}

	value, ok := resolveProjectionExprValue(arg, row, state)
	if !ok {
		return nil, false
	}
	if value == nil {
		return nil, true
	}
	if targetType == "duration" {
		if temporal, ok := projectionTemporalMap(value); ok {
			if strings.EqualFold(fmt.Sprint(temporal["__temporal_type"]), "duration") {
				return cloneProjectionMap(temporal), true
			}
		}
		return value, true
	}
	if text, ok := value.(string); ok {
		if parsed, ok := parseProjectionTemporalLiteralToMap(targetType, text); ok {
			if targetType != "duration" {
				parsed["__temporal_type"] = targetType
			}
			return parsed, true
		}
	}

	wrapperKey := "datetime"
	switch targetType {
	case "date":
		wrapperKey = "date"
	case "localtime", "time":
		wrapperKey = "time"
	}
	temporal, ok := buildProjectionTemporalValue(targetType, map[string]any{wrapperKey: value})
	if !ok {
		return value, true
	}
	return temporal, true
}

func buildProjectionTemporalValue(targetType string, values map[string]any) (map[string]any, bool) {
	if values == nil {
		return nil, false
	}
	normalized, ok := normalizeProjectionTemporalConstructorMap(targetType, values)
	if !ok {
		return nil, false
	}
	normalized["__temporal_type"] = targetType
	if len(normalized) == 1 {
		return nil, false
	}
	return normalized, true
}

func mergeProjectionDateComponents(target map[string]any, source any) {
	temporal, ok := projectionTemporalMap(source)
	if !ok {
		if embedded, ok := parseProjectionEmbeddedDate(source); ok {
			target["year"] = embedded.Year()
			target["month"] = int(embedded.Month())
			target["day"] = embedded.Day()
		}
		return
	}
	for _, key := range []string{"year", "month", "day", "week", "dayOfWeek", "ordinalDay", "quarter", "dayOfQuarter"} {
		if value, exists := temporal[key]; exists {
			target[key] = value
		}
	}
}

func mergeProjectionTimeComponents(target map[string]any, source any) {
	temporal, ok := projectionTemporalMap(source)
	if !ok {
		if hour, minute, second, nano, tz, ok := parseProjectionEmbeddedTime(source); ok {
			target["hour"] = hour
			target["minute"] = minute
			target["second"] = second
			target["nanosecond"] = nano
			if tz != "" {
				target["timezone"] = tz
			}
		}
		return
	}
	for _, key := range []string{"hour", "minute", "second", "millisecond", "microsecond", "nanosecond", "nanoseconds"} {
		if value, exists := temporal[key]; exists {
			target[key] = value
		}
	}
	if tz, exists := temporal["timezone"]; exists {
		target["timezone"] = tz
	}
}

func normalizeProjectionTemporalConstructorMap(typeName string, in map[string]any) (map[string]any, bool) {
	typeName = strings.ToLower(strings.TrimSpace(typeName))
	out := cloneProjectionMap(in)
	if out == nil {
		out = map[string]any{}
	}

	if typeName == "duration" {
		return out, true
	}

	if _, hasDate := out["date"]; !hasDate {
		if embeddedDateTime, ok := out["datetime"]; ok {
			out["date"] = embeddedDateTime
		}
	}

	if embeddedDate, ok := parseProjectionEmbeddedDate(out["date"]); ok {
		if _, exists := out["year"]; !exists {
			out["year"] = embeddedDate.Year()
		}
		if _, exists := out["month"]; !exists {
			out["month"] = int(embeddedDate.Month())
		}
		if _, exists := out["day"]; !exists {
			out["day"] = embeddedDate.Day()
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		timeSource := out["time"]
		sourceTZ := ""
		if timeSource == nil {
			timeSource = out["datetime"]
		}
		if h, m, s, n, tz, ok := parseProjectionEmbeddedTime(timeSource); ok {
			sourceTZ = tz
			if sourceMap, ok := projectionTemporalMap(timeSource); ok {
				sourceType := strings.ToLower(strings.TrimSpace(fmt.Sprint(sourceMap["__temporal_type"])))
				if sourceType != "time" && sourceType != "datetime" {
					sourceTZ = ""
				}
			}
			if _, exists := out["hour"]; !exists {
				out["hour"] = h
			}
			if _, exists := out["minute"]; !exists {
				out["minute"] = m
			}
			if _, exists := out["second"]; !exists {
				out["second"] = s
			}
			if _, exists := out["nanosecond"]; !exists {
				out["nanosecond"] = n
			}
			if tz != "" {
				if _, exists := out["timezone"]; !exists {
					out["timezone"] = tz
				}
			}
		}

		if typeName == "time" || typeName == "datetime" {
			targetTZ := projectionTemporalTimezone(out)
			if sourceTZ != "" && targetTZ != "" && sourceTZ != targetTZ {
				year, month, day := 1970, 1, 1
				if typeName == "datetime" {
					if y, mo, d, ok := resolveProjectionDateFromTemporalMap(out); ok {
						year, month, day = y, mo, d
					}
				}
				hour, _ := projectionNumericToInt(out["hour"])
				minute, _ := projectionNumericToInt(out["minute"])
				second, _ := projectionNumericToInt(out["second"])
				nano := projectionTemporalNanoseconds(out)
				if converted, ok := convertProjectionTemporalClockTimezone(year, month, day, hour, minute, second, nano, sourceTZ, targetTZ); ok {
					out["hour"] = converted.Hour()
					out["minute"] = converted.Minute()
					out["second"] = converted.Second()
					out["nanosecond"] = converted.Nanosecond()
					if typeName == "datetime" {
						out["year"] = converted.Year()
						out["month"] = int(converted.Month())
						out["day"] = converted.Day()
					}
				}
			}
		}
	}

	if typeName == "date" || typeName == "localdatetime" || typeName == "datetime" {
		if y, m, d, ok := resolveProjectionDateFromTemporalMap(out); ok {
			out["year"] = y
			out["month"] = m
			out["day"] = d
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		hour, _ := projectionNumericToInt(out["hour"])
		minute, _ := projectionNumericToInt(out["minute"])
		second, _ := projectionNumericToInt(out["second"])
		nano := projectionTemporalNanoseconds(out)
		out["hour"] = hour
		out["minute"] = minute
		out["second"] = second
		out["nanosecond"] = nano
		delete(out, "microsecond")
		delete(out, "millisecond")
	}

	if typeName == "time" || typeName == "datetime" {
		if projectionTemporalTimezone(out) == "" {
			out["timezone"] = "Z"
		}
	}

	return out, true
}

func resolveProjectionDateFromTemporalMap(in map[string]any) (int, int, int, bool) {
	if y, ord, ok := projectionYearAndOrdinal(in); ok {
		base := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, ord-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if y, q, doq, ok := projectionYearQuarterDayOfQuarter(in); ok {
		month := (q-1)*3 + 1
		base := time.Date(y, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, doq-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if week, ok := projectionNumericToInt(in["week"]); ok {
		weekYear, hasWeekYear := projectionNumericToInt(in["year"])
		baseDate, hasBaseDate := parseProjectionEmbeddedDate(in["date"])
		if !hasWeekYear && hasBaseDate {
			isoYear, _ := baseDate.ISOWeek()
			weekYear = isoYear
			hasWeekYear = true
		}
		if hasWeekYear {
			dayOfWeek, hasDOW := projectionNumericToInt(in["dayOfWeek"])
			if !hasDOW {
				if hasBaseDate {
					wd := int(baseDate.Weekday())
					if wd == 0 {
						wd = 7
					}
					dayOfWeek = wd
				} else {
					dayOfWeek = 1
				}
			}
			if resolved, ok := projectionISOWeekDate(weekYear, week, dayOfWeek); ok {
				return resolved.Year(), int(resolved.Month()), resolved.Day(), true
			}
		}
	}
	if y, m, d, ok := projectionDirectYMD(in); ok {
		return y, m, d, true
	}
	if y, ok := projectionNumericToInt(in["year"]); ok {
		return y, 1, 1, true
	}
	if embedded, ok := parseProjectionEmbeddedDate(in["date"]); ok {
		return embedded.Year(), int(embedded.Month()), embedded.Day(), true
	}
	return 0, 0, 0, false
}

func projectionDirectYMD(in map[string]any) (int, int, int, bool) {
	y, yOK := projectionNumericToInt(in["year"])
	m, mOK := projectionNumericToInt(in["month"])
	if yOK && mOK {
		d, dOK := projectionNumericToInt(in["day"])
		if !dOK {
			d = 1
		}
		return y, m, d, true
	}
	return 0, 0, 0, false
}

func projectionYearAndOrdinal(in map[string]any) (int, int, bool) {
	y, yOK := projectionNumericToInt(in["year"])
	ord, ordOK := projectionNumericToInt(in["ordinalDay"])
	if !yOK || !ordOK {
		return 0, 0, false
	}
	return y, ord, true
}

func projectionYearQuarterDayOfQuarter(in map[string]any) (int, int, int, bool) {
	y, yOK := projectionNumericToInt(in["year"])
	q, qOK := projectionNumericToInt(in["quarter"])
	if !yOK || !qOK {
		return 0, 0, 0, false
	}
	doq, doqOK := projectionNumericToInt(in["dayOfQuarter"])
	if !doqOK {
		if m, mOK := projectionNumericToInt(in["month"]); mOK {
			if d, dOK := projectionNumericToInt(in["day"]); dOK {
				base := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
				qStartMonth := ((int(base.Month())-1)/3)*3 + 1
				qStart := time.Date(base.Year(), time.Month(qStartMonth), 1, 0, 0, 0, 0, time.UTC)
				doq = int(base.Sub(qStart).Hours()/24) + 1
				doqOK = true
			}
		}
	}
	if !doqOK {
		doq = 1
	}
	return y, q, doq, true
}

func parseProjectionEmbeddedDate(raw any) (time.Time, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		if y, m, d, ok := resolveProjectionDateFromTemporalMap(typed); ok {
			return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), true
		}
		if v, ok := typed["value"]; ok {
			s := strings.TrimSpace(fmt.Sprint(v))
			if idx := strings.IndexAny(s, "Tt"); idx > 0 {
				s = strings.TrimSpace(s[:idx])
			}
			if t, err := time.Parse("2006-01-02", s); err == nil {
				return t, true
			}
		}
	case string:
		s := strings.TrimSpace(typed)
		if idx := strings.IndexAny(s, "Tt"); idx > 0 {
			s = strings.TrimSpace(s[:idx])
		}
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseProjectionEmbeddedTime(raw any) (int, int, int, int, string, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		mapped := typed
		if _, hasTemporal := mapped["__temporal_type"]; !hasTemporal {
			if v, ok := mapped["value"]; ok {
				if parsed, ok := projectionTemporalMap(v); ok {
					mapped = parsed
				}
			}
		}
		hour, hasHour := projectionNumericToInt(mapped["hour"])
		minute, hasMinute := projectionNumericToInt(mapped["minute"])
		second, _ := projectionNumericToInt(mapped["second"])
		nano := projectionTemporalNanoseconds(mapped)
		tz := projectionTemporalTimezone(mapped)
		if hasHour || hasMinute || second != 0 || nano != 0 {
			return hour, minute, second, nano, tz, true
		}
	case string:
		if parsed, ok := parseProjectionTimeLiteral(typed); ok {
			return parsed.hour, parsed.minute, parsed.second, parsed.nanosecond, parsed.timezone, true
		}
	}
	return 0, 0, 0, 0, "", false
}

type projectionParsedTime struct {
	hour       int
	minute     int
	second     int
	nanosecond int
	timezone   string
}

func parseProjectionTimeLiteral(raw string) (projectionParsedTime, bool) {
	h, m, s, n, tz, ok := parseProjectionClockAndZone(raw)
	if !ok {
		return projectionParsedTime{}, false
	}
	return projectionParsedTime{hour: h, minute: m, second: s, nanosecond: n, timezone: tz}, true
}

func convertProjectionTemporalClockTimezone(year, month, day, hour, minute, second, nanosecond int, sourceTZ, targetTZ string) (time.Time, bool) {
	srcLoc := time.UTC
	if off, err := projectionParseOffsetSeconds(sourceTZ); err == nil {
		srcLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(sourceTZ); err == nil {
		srcLoc = l
	}
	dstLoc := time.UTC
	if off, err := projectionParseOffsetSeconds(targetTZ); err == nil {
		dstLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(targetTZ); err == nil {
		dstLoc = l
	}
	src := time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, srcLoc)
	return src.In(dstLoc), true
}

func formatProjectionOffsetString(totalSeconds int) string {
	if totalSeconds == 0 {
		return "Z"
	}
	sign := "+"
	if totalSeconds < 0 {
		sign = "-"
		totalSeconds = -totalSeconds
	}
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	if seconds == 0 {
		return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
}

func parseProjectionTemporalLiteralToMap(typeName, raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	typeName = strings.ToLower(strings.TrimSpace(typeName))
	switch typeName {
	case "date":
		y, m, d, ok := parseProjectionDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": m, "day": d}, true
	case "localtime":
		h, m, s, n, _, ok := parseProjectionClockAndZone(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n}, true
	case "time":
		h, m, s, n, tz, ok := parseProjectionClockAndZone(raw)
		if !ok {
			return nil, false
		}
		if tz == "" {
			tz = "Z"
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n, "timezone": tz}, true
	case "localdatetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseProjectionDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, _, ok := parseProjectionClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n}, true
		}
		y, mo, d, ok := parseProjectionDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0}, true
	case "datetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseProjectionDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, tz, ok := parseProjectionClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			if tz == "" {
				tz = "Z"
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n, "timezone": tz}, true
		}
		y, mo, d, ok := parseProjectionDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0, "timezone": "Z"}, true
	case "duration":
		parsed, ok := parseProjectionDurationLiteralToMap(raw)
		if !ok {
			return nil, false
		}
		return projectionBuildDurationResult(projectionDurationComponentsFromMap(parsed)), true
	default:
		return nil, false
	}
}

func parseProjectionDurationLiteralToMap(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !(strings.HasPrefix(raw, "P") || strings.HasPrefix(raw, "p")) {
		return nil, false
	}
	raw = raw[1:]
	out := map[string]any{}
	hasValue := false
	datePart := raw
	timePart := ""
	if idx := strings.IndexAny(raw, "Tt"); idx >= 0 {
		datePart = raw[:idx]
		timePart = raw[idx+1:]
	}
	if datePart != "" {
		if years, months, days, ok := parseProjectionDurationCalendarDatePart(datePart); ok {
			out["years"] = mapFloatProjection(out, "years") + years
			out["months"] = mapFloatProjection(out, "months") + months
			out["days"] = mapFloatProjection(out, "days") + days
			hasValue = true
		} else if parsed, ok := parseProjectionDurationUnitSection(datePart, false); ok {
			for k, v := range parsed {
				out[k] = v
				hasValue = true
			}
		}
	}
	if timePart != "" {
		if strings.Contains(timePart, ":") {
			h, m, s, n, _, ok := parseProjectionClockAndZone(timePart)
			if ok {
				out["hours"] = float64(h)
				out["minutes"] = float64(m)
				out["seconds"] = float64(s) + float64(n)/1_000_000_000
				hasValue = true
			}
		} else if parsed, ok := parseProjectionDurationUnitSection(timePart, true); ok {
			for k, v := range parsed {
				out[k] = v
				hasValue = true
			}
		}
	}
	if !hasValue {
		return nil, false
	}
	return out, true
}

func parseProjectionDurationCalendarDatePart(raw string) (float64, float64, float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, 0, false
	}
	pattern := regexp.MustCompile(`^([+-]?\d+)-(\d+)-(\d+)$`)
	match := pattern.FindStringSubmatch(raw)
	if len(match) != 4 {
		return 0, 0, 0, false
	}
	years, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, 0, 0, false
	}
	months, err := strconv.ParseFloat(match[2], 64)
	if err != nil {
		return 0, 0, 0, false
	}
	days, err := strconv.ParseFloat(match[3], 64)
	if err != nil {
		return 0, 0, 0, false
	}
	return years, months, days, true
}

func parseProjectionDurationUnitSection(raw string, timeSection bool) (map[string]float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	result := map[string]float64{}
	pattern := regexp.MustCompile(`([+-]?\d+(?:\.\d+)?)([YMWDHSymwdhs])`)
	matches := pattern.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil, false
	}
	for _, match := range matches {
		value, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			return nil, false
		}
		switch strings.ToUpper(match[2]) {
		case "Y":
			result["years"] += value
		case "M":
			if timeSection {
				result["minutes"] += value
			} else {
				result["months"] += value
			}
		case "W":
			result["weeks"] += value
		case "D":
			result["days"] += value
		case "H":
			result["hours"] += value
		case "S":
			result["seconds"] += value
		}
	}
	return result, true
}

func projectionNumericToFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		n, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return n, true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func projectionIsInvalidScalarConversionType(value any) bool {
	if value == nil {
		return false
	}
	switch value.(type) {
	case []any, []string, map[string]string, *graph.Vertex, *graph.Edge, projectionPathValue:
		return true
	}
	if typed, ok := value.(map[string]any); ok {
		if _, ok := projectionTemporalMap(typed); ok {
			return false
		}
		return true
	}
	return false
}

func parseProjectionDateParts(raw string) (int, int, int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, 0, false
	}
	sign := 1
	if strings.HasPrefix(raw, "+") {
		raw = raw[1:]
	} else if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = raw[1:]
	}
	if idx := strings.IndexAny(raw, "Ww"); idx > 0 {
		yearPart := strings.TrimSuffix(raw[:idx], "-")
		rest := raw[idx+1:]
		year, err := strconv.Atoi(yearPart)
		if err != nil {
			return 0, 0, 0, false
		}
		year *= sign
		dayOfWeek := 1
		rest = strings.TrimPrefix(rest, "-")
		weekPart := rest
		if dash := strings.Index(rest, "-"); dash >= 0 {
			weekPart = rest[:dash]
			if parsedDay, err := strconv.Atoi(rest[dash+1:]); err == nil {
				dayOfWeek = parsedDay
			}
		} else if len(rest) > 2 {
			weekPart = rest[:2]
			if parsedDay, err := strconv.Atoi(rest[2:]); err == nil {
				dayOfWeek = parsedDay
			}
		}
		week, err := strconv.Atoi(weekPart)
		if err != nil {
			return 0, 0, 0, false
		}
		if resolved, ok := projectionISOWeekDate(year, week, dayOfWeek); ok {
			return resolved.Year(), int(resolved.Month()), resolved.Day(), true
		}
		return 0, 0, 0, false
	}
	if strings.Contains(raw, "-") {
		parts := strings.Split(raw, "-")
		if len(parts) == 3 {
			y, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, 0, 0, false
			}
			m, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, 0, 0, false
			}
			d, err := strconv.Atoi(parts[2])
			if err != nil {
				return 0, 0, 0, false
			}
			return sign * y, m, d, true
		}
		if len(parts) == 2 {
			y, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, 0, 0, false
			}
			if len(parts[1]) == 3 {
				ord, err := strconv.Atoi(parts[1])
				if err != nil {
					return 0, 0, 0, false
				}
				resolved := time.Date(sign*y, 1, ord, 0, 0, 0, 0, time.UTC)
				return resolved.Year(), int(resolved.Month()), resolved.Day(), true
			}
			if m, err := strconv.Atoi(parts[1]); err == nil {
				return sign * y, m, 1, true
			}
		}
	}
	if len(raw) == 8 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		m, err := strconv.Atoi(raw[4:6])
		if err != nil {
			return 0, 0, 0, false
		}
		d, err := strconv.Atoi(raw[6:8])
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, m, d, true
	}
	if len(raw) == 7 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		ord, err := strconv.Atoi(raw[4:])
		if err != nil {
			return 0, 0, 0, false
		}
		resolved := time.Date(sign*y, 1, ord, 0, 0, 0, 0, time.UTC)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if len(raw) == 6 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		m, err := strconv.Atoi(raw[4:6])
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, m, 1, true
	}
	if len(raw) == 4 {
		y, err := strconv.Atoi(raw)
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, 1, 1, true
	}
	return 0, 0, 0, false
}

func parseProjectionClockAndZone(raw string) (int, int, int, int, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, 0, 0, "", false
	}
	tz := ""
	hasNamedZone := false
	if idx := strings.LastIndex(raw, "["); idx > 0 && strings.HasSuffix(raw, "]") {
		tz = strings.TrimSpace(raw[idx+1 : len(raw)-1])
		hasNamedZone = tz != ""
		raw = strings.TrimSpace(raw[:idx])
	}
	offsetIdx := -1
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] == '+' || raw[i] == '-' {
			offsetIdx = i
			break
		}
	}
	if strings.HasSuffix(raw, "Z") || strings.HasSuffix(raw, "z") {
		tz = "Z"
		raw = raw[:len(raw)-1]
	} else if offsetIdx > 0 {
		offset := raw[offsetIdx:]
		norm, ok := normalizeProjectionOffsetToken(offset)
		if ok {
			if !hasNamedZone {
				tz = norm
			}
			raw = raw[:offsetIdx]
		}
	}
	clock := strings.SplitN(raw, ".", 2)
	h, m, s := 0, 0, 0
	var err error
	if strings.Contains(clock[0], ":") {
		hms := strings.Split(clock[0], ":")
		if len(hms) < 2 || len(hms) > 3 {
			return 0, 0, 0, 0, "", false
		}
		h, err = strconv.Atoi(hms[0])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		m, err = strconv.Atoi(hms[1])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		if len(hms) == 3 {
			s, err = strconv.Atoi(hms[2])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
	} else {
		digits := clock[0]
		if len(digits) != 2 && len(digits) != 4 && len(digits) != 6 {
			return 0, 0, 0, 0, "", false
		}
		h, err = strconv.Atoi(digits[:2])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		if len(digits) >= 4 {
			m, err = strconv.Atoi(digits[2:4])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
		if len(digits) == 6 {
			s, err = strconv.Atoi(digits[4:6])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
	}
	n := 0
	if len(clock) == 2 {
		frac := strings.TrimSpace(clock[1])
		if frac == "" {
			return 0, 0, 0, 0, "", false
		}
		if len(frac) > 9 {
			frac = frac[:9]
		}
		for len(frac) < 9 {
			frac += "0"
		}
		n, err = strconv.Atoi(frac)
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
	}
	return h, m, s, n, tz, true
}

func normalizeProjectionOffsetToken(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if raw == "Z" || raw == "z" {
		return "Z", true
	}
	if raw[0] != '+' && raw[0] != '-' {
		return "", false
	}
	sign := raw[:1]
	body := strings.ReplaceAll(raw[1:], ":", "")
	if len(body) != 2 && len(body) != 4 && len(body) != 6 {
		return "", false
	}
	for _, ch := range body {
		if ch < '0' || ch > '9' {
			return "", false
		}
	}
	hours, _ := strconv.Atoi(body[:2])
	minutes := 0
	seconds := 0
	if len(body) >= 4 {
		minutes, _ = strconv.Atoi(body[2:4])
	}
	if len(body) == 6 {
		seconds, _ = strconv.Atoi(body[4:6])
	}
	if hours > 18 || minutes > 59 || seconds > 59 {
		return "", false
	}
	if len(body) == 2 {
		return fmt.Sprintf("%s%02d:00", sign, hours), true
	}
	if len(body) == 4 {
		return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes), true
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds), true
}

func projectionTemporalMap(value any) (map[string]any, bool) {
	typed, ok := value.(map[string]any)
	if !ok || typed == nil {
		return nil, false
	}
	if _, exists := typed["__temporal_type"]; !exists {
		return nil, false
	}
	return typed, true
}

func projectionTemporalAccessorValue(base map[string]any, field string) (any, bool) {
	if base == nil {
		return nil, false
	}
	field = strings.TrimSpace(field)
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(base["__temporal_type"])))
	if field == "" || typeName == "" {
		return nil, false
	}
	if typeName != "duration" {
		if value, exists := base[field]; exists {
			return value, true
		}
	}

	switch typeName {
	case "date", "localdatetime", "datetime":
		y, m, d, ok := resolveProjectionDateFromTemporalMap(base)
		if !ok {
			return nil, false
		}
		dt := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		if typeName == "localdatetime" || typeName == "datetime" {
			if resolved, ok := projectionTemporalDateTime(base, typeName == "datetime"); ok {
				dt = resolved
			}
		}
		_, zoneOffset := dt.Zone()
		switch field {
		case "year":
			return y, true
		case "quarter":
			return ((m - 1) / 3) + 1, true
		case "month":
			return m, true
		case "week":
			_, week := dt.ISOWeek()
			return week, true
		case "weekYear":
			weekYear, _ := dt.ISOWeek()
			return weekYear, true
		case "day":
			return d, true
		case "ordinalDay":
			return dt.YearDay(), true
		case "weekDay":
			wd := int(dt.Weekday())
			if wd == 0 {
				wd = 7
			}
			return wd, true
		case "dayOfQuarter":
			startMonth := time.Month(((m-1)/3)*3 + 1)
			start := time.Date(y, startMonth, 1, 0, 0, 0, 0, time.UTC)
			return int(dt.Sub(start).Hours()/24) + 1, true
		case "hour":
			return dt.Hour(), true
		case "minute":
			return dt.Minute(), true
		case "second":
			return dt.Second(), true
		case "millisecond":
			return dt.Nanosecond() / 1_000_000, true
		case "microsecond":
			return dt.Nanosecond() / 1_000, true
		case "nanosecond":
			return dt.Nanosecond(), true
		case "timezone":
			tz := projectionTemporalTimezone(base)
			if tz != "" {
				return tz, true
			}
			if typeName == "datetime" {
				return "Z", true
			}
			return nil, true
		case "offset":
			return formatProjectionOffsetString(zoneOffset), true
		case "offsetMinutes":
			return zoneOffset / 60, true
		case "offsetSeconds":
			return zoneOffset, true
		case "epochSeconds":
			return dt.Unix(), true
		case "epochMillis":
			return dt.Unix()*1000 + int64(dt.Nanosecond()/1_000_000), true
		}
	case "localtime", "time":
		hour, _ := projectionNumericToInt(base["hour"])
		minute, _ := projectionNumericToInt(base["minute"])
		second, _ := projectionNumericToInt(base["second"])
		nano := projectionTemporalNanoseconds(base)
		tz := projectionTemporalTimezone(base)
		offset := 0
		if typeName == "time" {
			if dt, ok := projectionTemporalDateTime(base, true); ok {
				_, offset = dt.Zone()
			}
		}
		switch field {
		case "hour":
			return hour, true
		case "minute":
			return minute, true
		case "second":
			return second, true
		case "millisecond":
			return nano / 1_000_000, true
		case "microsecond":
			return nano / 1_000, true
		case "nanosecond":
			return nano, true
		case "timezone":
			if tz != "" {
				return tz, true
			}
			if typeName == "time" {
				return "Z", true
			}
			return nil, true
		case "offset":
			if typeName == "time" {
				return formatProjectionOffsetString(offset), true
			}
			return nil, true
		case "offsetMinutes":
			if typeName == "time" {
				return offset / 60, true
			}
			return nil, true
		case "offsetSeconds":
			if typeName == "time" {
				return offset, true
			}
			return nil, true
		}
	case "duration":
		return projectionDurationAccessorValue(base, field)
	}

	return nil, false
}

func resolveProjectionQuantifierExprValue(expr string, row map[string]any, state *State) (any, bool) {
	name, arg, ok := splitProjectionTopLevelFunctionCall(expr)
	if !ok {
		return nil, false
	}
	quantifier := strings.ToLower(strings.TrimSpace(name))
	switch quantifier {
	case "all", "any", "none", "single":
	default:
		return nil, false
	}
	varName, listExpr, predicateExpr, ok := splitProjectionQuantifierArg(arg)
	if !ok {
		return nil, false
	}
	listValue, ok := resolveProjectionExprValue(listExpr, row, state)
	if !ok {
		return nil, false
	}
	if listValue == nil {
		return nil, true
	}
	list, ok := projectionListValue(listValue)
	if !ok {
		return nil, true
	}

	trueCount := 0
	falseCount := 0
	nullCount := 0
	for _, item := range list {
		next := cloneRow(row)
		if next == nil {
			next = map[string]any{}
		}
		next[varName] = item
		predValue, predOK := resolveProjectionExprValue(predicateExpr, next, state)
		if !predOK {
			nullCount++
			continue
		}
		switch typed := predValue.(type) {
		case nil:
			nullCount++
		case bool:
			if typed {
				trueCount++
			} else {
				falseCount++
			}
		default:
			nullCount++
		}
	}

	switch quantifier {
	case "all":
		if falseCount > 0 {
			return false, true
		}
		if nullCount > 0 {
			return nil, true
		}
		return true, true
	case "any":
		if trueCount > 0 {
			return true, true
		}
		if nullCount > 0 {
			return nil, true
		}
		return false, true
	case "none":
		if trueCount > 0 {
			return false, true
		}
		if nullCount > 0 {
			return nil, true
		}
		return true, true
	case "single":
		if trueCount > 1 {
			return false, true
		}
		if trueCount == 1 {
			if nullCount > 0 {
				return nil, true
			}
			return true, true
		}
		if nullCount > 0 {
			return nil, true
		}
		return false, true
	}

	return nil, false
}

func splitProjectionQuantifierArg(arg string) (string, string, string, bool) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", "", "", false
	}
	inPos := findProjectionTopLevelKeyword(arg, "IN")
	if inPos <= 0 {
		return "", "", "", false
	}
	varName := strings.TrimSpace(arg[:inPos])
	if !isIdentifierToken(varName) {
		return "", "", "", false
	}
	rest := strings.TrimSpace(arg[inPos+2:])
	if rest == "" {
		return "", "", "", false
	}
	wherePos := findProjectionTopLevelKeyword(rest, "WHERE")
	if wherePos <= 0 {
		return "", "", "", false
	}
	listExpr := strings.TrimSpace(rest[:wherePos])
	predicateExpr := strings.TrimSpace(rest[wherePos+5:])
	if listExpr == "" || predicateExpr == "" {
		return "", "", "", false
	}
	return varName, listExpr, predicateExpr, true
}

func findProjectionTopLevelKeyword(input string, keyword string) int {
	upper := strings.ToUpper(input)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" {
		return -1
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
		ch := input[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(input) && input[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && input[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		if !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		leftBound := i == 0 || unicode.IsSpace(rune(input[i-1]))
		rightIndex := i + len(keyword)
		rightBound := rightIndex >= len(input) || unicode.IsSpace(rune(input[rightIndex]))
		if leftBound && rightBound {
			return i
		}
	}
	return -1
}

func findProjectionTopLevelRune(input string, needle rune) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i, ch := range input {
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			continue
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		if ch == needle {
			return i
		}
	}
	return -1
}

func projectionListValue(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case []int:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case []int64:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case []float64:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case []bool:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return nil, false
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		return out, true
	}
}

func parseProjectionListLiteral(raw string, row map[string]any, state *State) ([]any, bool) {
	body := strings.TrimSpace(raw)
	if body == "" {
		return []any{}, true
	}
	parts := splitProjectionTopLevelByComma(body)
	values := make([]any, 0, len(parts))
	for _, part := range parts {
		value, ok := resolveProjectionExprValue(strings.TrimSpace(part), row, state)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func projectionDurationAccessorValue(base map[string]any, field string) (any, bool) {
	years := mapFloatProjection(base, "years")
	months := mapFloatProjection(base, "months")
	weeks := mapFloatProjection(base, "weeks")
	days := mapFloatProjection(base, "days")
	hours := mapFloatProjection(base, "hours")
	minutes := mapFloatProjection(base, "minutes")
	seconds := mapFloatProjection(base, "seconds")
	milliseconds := mapFloatProjection(base, "milliseconds")
	microseconds := mapFloatProjection(base, "microseconds")
	nanoseconds := mapFloatProjection(base, "nanoseconds")

	seconds += mapFloatProjection(base, "milliseconds") / 1000
	seconds += mapFloatProjection(base, "microseconds") / 1_000_000
	seconds += mapFloatProjection(base, "nanoseconds") / 1_000_000_000
	totalMonths := years*12 + months
	totalDays := weeks*7 + days
	timeSeconds := hours*3600 + minutes*60 + mapFloatProjection(base, "seconds")
	timeNanos := timeSecondsToProjectionNanoseconds(timeSeconds) + milliseconds*1_000_000 + microseconds*1_000 + nanoseconds
	timeNanosOfSecond := math.Mod(timeNanos, 1_000_000_000)
	canonicalSeconds, canonicalNanos := projectionSplitSecondsAndNanosExact(timeSeconds + (milliseconds / 1_000) + (microseconds / 1_000_000) + (nanoseconds / 1_000_000_000))
	switch field {
	case "years":
		return int(projectionTruncTowardZero(totalMonths / 12)), true
	case "quarters":
		return int(projectionTruncTowardZero(totalMonths / 3)), true
	case "months":
		return int(projectionTruncTowardZero(totalMonths)), true
	case "weeks":
		return int(projectionTruncTowardZero(totalDays / 7)), true
	case "days":
		return int(projectionTruncTowardZero(totalDays)), true
	case "hours":
		return int(projectionTruncTowardZero(timeSeconds / 3600)), true
	case "minutes":
		return int(projectionTruncTowardZero(timeSeconds / 60)), true
	case "seconds":
		return canonicalSeconds, true
	case "milliseconds":
		return int(projectionTruncTowardZero(timeNanos / 1_000_000)), true
	case "microseconds":
		return int(projectionTruncTowardZero(timeNanos / 1_000)), true
	case "nanoseconds":
		return int(projectionTruncTowardZero(timeNanos)), true
	case "quartersOfYear":
		return int(projectionTruncTowardZero(math.Mod(totalMonths, 12) / 3)), true
	case "monthsOfQuarter":
		return int(projectionTruncTowardZero(math.Mod(totalMonths, 3))), true
	case "monthsOfYear":
		return int(projectionTruncTowardZero(math.Mod(totalMonths, 12))), true
	case "daysOfWeek":
		return int(projectionTruncTowardZero(math.Mod(totalDays, 7))), true
	case "minutesOfHour":
		return int(projectionTruncTowardZero(math.Mod(timeSeconds/60, 60))), true
	case "secondsOfMinute":
		return int(projectionTruncTowardZero(math.Mod(timeSeconds, 60))), true
	case "millisecondsOfSecond":
		return int(projectionTruncTowardZero(timeNanosOfSecond / 1_000_000)), true
	case "microsecondsOfSecond":
		return int(projectionTruncTowardZero(timeNanosOfSecond / 1_000)), true
	case "nanosecondsOfSecond":
		return canonicalNanos, true
	}
	return nil, false
}

func timeSecondsToProjectionNanoseconds(seconds float64) float64 {
	return seconds * 1_000_000_000
}

func projectionSplitSecondsAndNanosExact(seconds float64) (int, int) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, 0
	}
	whole := int(math.Floor(seconds))
	frac := seconds - float64(whole)
	rawNanos := frac * 1_000_000_000
	nanos := int(math.Round(rawNanos))
	if nanos == 0 {
		if rawNanos > 0 {
			nanos = 1
		} else if rawNanos < 0 {
			nanos = -1
		}
	}
	if nanos >= 1_000_000_000 {
		whole++
		nanos -= 1_000_000_000
	}
	if nanos < 0 {
		nanos = 0
	}
	return whole, nanos
}

func projectionTruncTowardZero(v float64) float64 {
	if v < 0 {
		return math.Ceil(v)
	}
	return math.Floor(v)
}

func projectionCompareTemporalValues(left map[string]any, right map[string]any) (int, bool) {
	leftType := strings.ToLower(strings.TrimSpace(fmt.Sprint(left["__temporal_type"])))
	rightType := strings.ToLower(strings.TrimSpace(fmt.Sprint(right["__temporal_type"])))
	if leftType == "" || rightType == "" || leftType != rightType {
		return 0, false
	}
	switch leftType {
	case "date", "localdatetime":
		lt, lok := projectionTemporalDateTime(left, false)
		rt, rok := projectionTemporalDateTime(right, false)
		if !lok || !rok {
			return 0, false
		}
		return projectionCompareTimes(lt, rt), true
	case "datetime":
		lt, lok := projectionTemporalDateTime(left, true)
		rt, rok := projectionTemporalDateTime(right, true)
		if !lok || !rok {
			return 0, false
		}
		return projectionCompareTimes(lt, rt), true
	case "localtime":
		lt, lok := projectionTemporalDateTime(left, false)
		rt, rok := projectionTemporalDateTime(right, false)
		if !lok || !rok {
			return 0, false
		}
		return projectionCompareTimes(lt, rt), true
	case "time":
		lt, lok := projectionTemporalDateTime(left, true)
		rt, rok := projectionTemporalDateTime(right, true)
		if !lok || !rok {
			return 0, false
		}
		return projectionCompareTimes(lt, rt), true
	case "duration":
		leftRendered := formatProjectionDurationString(left)
		rightRendered := formatProjectionDurationString(right)
		switch {
		case leftRendered < rightRendered:
			return -1, true
		case leftRendered > rightRendered:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

func projectionCompareTimes(left time.Time, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case left.After(right):
		return 1
	default:
		return 0
	}
}

func mapFloatProjection(src map[string]any, key string) float64 {
	if src == nil {
		return 0
	}
	value, exists := src[key]
	if !exists || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint64:
		return float64(typed)
	case uint32:
		return float64(typed)
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
			return parsed
		}
	}
	return 0
}

func projectionTemporalToString(temporal map[string]any) (string, bool) {
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	switch typeName {
	case "date":
		y, m, d, ok := resolveProjectionDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), true
	case "localtime":
		hour, _ := projectionNumericToInt(temporal["hour"])
		minute, _ := projectionNumericToInt(temporal["minute"])
		second, _ := projectionNumericToInt(temporal["second"])
		nano := projectionTemporalNanoseconds(temporal)
		return formatProjectionClockString(hour, minute, second, nano), true
	case "time":
		hour, _ := projectionNumericToInt(temporal["hour"])
		minute, _ := projectionNumericToInt(temporal["minute"])
		second, _ := projectionNumericToInt(temporal["second"])
		nano := projectionTemporalNanoseconds(temporal)
		tz := projectionTemporalTimezone(temporal)
		if tz == "" {
			tz = "Z"
		}
		if offset, err := projectionParseOffsetSeconds(tz); err == nil {
			tz = formatProjectionOffsetString(offset)
		}
		return formatProjectionClockString(hour, minute, second, nano) + tz, true
	case "localdatetime":
		y, m, d, ok := resolveProjectionDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := projectionNumericToInt(temporal["hour"])
		minute, _ := projectionNumericToInt(temporal["minute"])
		second, _ := projectionNumericToInt(temporal["second"])
		nano := projectionTemporalNanoseconds(temporal)
		return fmt.Sprintf("%04d-%02d-%02dT%s", y, m, d, formatProjectionClockString(hour, minute, second, nano)), true
	case "datetime":
		y, m, d, ok := resolveProjectionDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := projectionNumericToInt(temporal["hour"])
		minute, _ := projectionNumericToInt(temporal["minute"])
		second, _ := projectionNumericToInt(temporal["second"])
		nano := projectionTemporalNanoseconds(temporal)
		tz := projectionTemporalTimezone(temporal)
		if tz == "" {
			tz = "Z"
		}
		clock := formatProjectionClockString(hour, minute, second, nano)
		if offset, err := projectionParseOffsetSeconds(tz); err == nil {
			return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, formatProjectionOffsetString(offset)), true
		}
		if loc, err := time.LoadLocation(tz); err == nil {
			t := time.Date(y, time.Month(m), d, hour, minute, second, nano, loc)
			_, offset := t.Zone()
			return fmt.Sprintf("%04d-%02d-%02dT%s%s[%s]", y, m, d, clock, formatProjectionOffsetString(offset), tz), true
		}
		return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, tz), true
	case "duration":
		return formatProjectionDurationString(temporal), true
	}
	return "", false
}

func formatProjectionClockString(hour, minute, second, nano int) string {
	base := fmt.Sprintf("%02d:%02d", hour, minute)
	if second == 0 && nano == 0 {
		return base
	}
	base = fmt.Sprintf("%s:%02d", base, second)
	if nano == 0 {
		return base
	}
	fraction := strings.TrimRight(fmt.Sprintf("%09d", nano), "0")
	if fraction == "" {
		return base
	}
	return base + "." + fraction
}

func formatProjectionDurationString(temporal map[string]any) string {
	years, months, days, seconds := projectionDecomposeDurationComponents(temporal)
	wholeSeconds := int64(projectionTruncTowardZero(seconds))
	frac := seconds - float64(wholeSeconds)

	hours := int(wholeSeconds / 3600)
	remainingSeconds := wholeSeconds - int64(hours*3600)
	minutes := int(remainingSeconds / 60)
	secInt := int(remainingSeconds - int64(minutes*60))

	fracSign := 1
	if frac < 0 {
		fracSign = -1
	}
	absNanosFloat := math.Abs(frac) * 1_000_000_000
	absNanos := int(math.Floor(absNanosFloat))
	nearest := math.Round(absNanosFloat)
	if math.Abs(absNanosFloat-nearest) < 0.02 {
		absNanos = int(nearest)
	}
	if absNanos >= 1_000_000_000 {
		if fracSign > 0 {
			secInt++
		} else {
			secInt--
		}
		absNanos -= 1_000_000_000
	}
	nanos := fracSign * absNanos
	if nanos >= 1_000_000_000 {
		secInt++
		nanos -= 1_000_000_000
	}
	if nanos <= -1_000_000_000 {
		secInt--
		nanos += 1_000_000_000
	}

	var b strings.Builder
	b.WriteString("P")
	if years != 0 {
		b.WriteString(fmt.Sprintf("%dY", years))
	}
	if months != 0 {
		b.WriteString(fmt.Sprintf("%dM", months))
	}
	if days != 0 {
		b.WriteString(fmt.Sprintf("%dD", days))
	}
	hasTime := hours != 0 || minutes != 0 || secInt != 0 || nanos != 0
	if hasTime || (years == 0 && months == 0 && days == 0) {
		b.WriteString("T")
		if hours != 0 {
			b.WriteString(fmt.Sprintf("%dH", hours))
		}
		if minutes != 0 {
			b.WriteString(fmt.Sprintf("%dM", minutes))
		}
		if secInt != 0 || nanos != 0 || (hours == 0 && minutes == 0) {
			if nanos == 0 {
				b.WriteString(fmt.Sprintf("%dS", secInt))
			} else {
				sign := ""
				if secInt < 0 || (secInt == 0 && nanos < 0) {
					sign = "-"
				}
				absSec := secInt
				if absSec < 0 {
					absSec = -absSec
				}
				absFrac := nanos
				if absFrac < 0 {
					absFrac = -absFrac
				}
				fraction := strings.TrimRight(fmt.Sprintf("%09d", absFrac), "0")
				b.WriteString(fmt.Sprintf("%s%d.%sS", sign, absSec, fraction))
			}
		}
	}
	out := b.String()
	if out == "P" {
		return "PT0S"
	}
	if strings.HasSuffix(out, "T") {
		return out + "0S"
	}
	return out
}

func projectionDecomposeDurationComponents(temporal map[string]any) (int, int, int, float64) {
	const avgMonthSeconds = 2629746.0
	totalMonths := mapFloatProjection(temporal, "years")*12 + mapFloatProjection(temporal, "months")
	years := int(projectionTruncTowardZero(totalMonths / 12))
	remainingMonths := totalMonths - float64(years*12)
	months := int(projectionTruncTowardZero(remainingMonths))
	fracMonths := remainingMonths - float64(months)

	totalDays := mapFloatProjection(temporal, "weeks")*7 + mapFloatProjection(temporal, "days") + (fracMonths*avgMonthSeconds)/86400
	days := int(projectionTruncTowardZero(totalDays))
	fracDays := totalDays - float64(days)

	seconds := mapFloatProjection(temporal, "hours")*3600 + mapFloatProjection(temporal, "minutes")*60 + mapFloatProjection(temporal, "seconds")
	seconds += mapFloatProjection(temporal, "milliseconds") / 1000
	seconds += mapFloatProjection(temporal, "microseconds") / 1_000_000
	seconds += mapFloatProjection(temporal, "nanoseconds") / 1_000_000_000
	seconds += fracDays * 86400

	return years, months, days, seconds
}

func cloneProjectionMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	clone := make(map[string]any, len(src))
	for key, value := range src {
		clone[key] = value
	}
	return clone
}

func parseProjectionMapLiteral(raw string, row map[string]any, state *State) (map[string]any, bool) {
	body := strings.TrimSpace(raw)
	if body == "" {
		return map[string]any{}, true
	}
	parts := splitProjectionTopLevelByComma(body)
	values := make(map[string]any, len(parts))
	for _, pair := range parts {
		key, valueExpr, ok := splitProjectionTopLevelKeyValue(pair)
		if !ok {
			return nil, false
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, false
		}
		value, ok := resolveProjectionExprValue(strings.TrimSpace(valueExpr), row, state)
		if !ok {
			return nil, false
		}
		values[key] = value
	}
	return values, true
}

func splitProjectionTopLevelFunctionCall(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}
	open := strings.IndexByte(expr, '(')
	if open <= 0 || !strings.HasSuffix(expr, ")") {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := open; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && expr[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && i == len(expr)-1 {
			name := strings.TrimSpace(expr[:open])
			if name == "" {
				return "", "", false
			}
			return name, expr[open+1 : len(expr)-1], true
		}
	}
	return "", "", false
}

func splitProjectionTopLevelByComma(input string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	parts := make([]string, 0, 4)
	start := 0
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(input) && input[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && input[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ',':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				part := strings.TrimSpace(input[start:i])
				if part != "" {
					parts = append(parts, part)
				}
				start = i + 1
			}
		}
	}
	last := strings.TrimSpace(input[start:])
	if last != "" {
		parts = append(parts, last)
	}
	return parts
}

func splitProjectionTopLevelKeyValue(pair string) (string, string, bool) {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(pair); i++ {
		ch := pair[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(pair) && pair[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && pair[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ':':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return strings.TrimSpace(pair[:i]), strings.TrimSpace(pair[i+1:]), true
			}
		}
	}
	return "", "", false
}

func applyProjectionTemporalTruncation(typeName string, src map[string]any, unit string) map[string]any {
	out := cloneProjectionMap(src)
	if out == nil {
		out = map[string]any{"__temporal_type": typeName}
	}
	unit = strings.ToLower(strings.TrimSpace(unit))

	truncateTimeFields := func(target map[string]any, truncUnit string) {
		nano := projectionTemporalNanoseconds(target)
		switch truncUnit {
		case "day":
			target["hour"] = 0
			target["minute"] = 0
			target["second"] = 0
			target["nanosecond"] = 0
		case "hour":
			target["minute"] = 0
			target["second"] = 0
			target["nanosecond"] = 0
		case "minute":
			target["second"] = 0
			target["nanosecond"] = 0
		case "second":
			target["nanosecond"] = 0
		case "millisecond":
			target["nanosecond"] = (nano / 1_000_000) * 1_000_000
		case "microsecond":
			target["nanosecond"] = (nano / 1_000) * 1_000
		}
	}

	supportsDateTruncation := typeName == "date" || typeName == "localdatetime" || typeName == "datetime"
	if supportsDateTruncation {
		switch unit {
		case "millennium", "century", "decade", "year", "weekyear", "quarter", "month", "week", "day":
			dt, ok := projectionTemporalDateTime(out, typeName == "datetime")
			if ok {
				switch unit {
				case "millennium":
					y := dt.Year()
					dt = time.Date((y/1000)*1000, 1, 1, 0, 0, 0, 0, dt.Location())
				case "century":
					y := dt.Year()
					dt = time.Date((y/100)*100, 1, 1, 0, 0, 0, 0, dt.Location())
				case "decade":
					y := dt.Year()
					dt = time.Date((y/10)*10, 1, 1, 0, 0, 0, 0, dt.Location())
				case "year":
					dt = time.Date(dt.Year(), 1, 1, 0, 0, 0, 0, dt.Location())
				case "weekyear":
					y, _ := dt.ISOWeek()
					if isoStart, ok := projectionISOWeekDate(y, 1, 1); ok {
						dt = time.Date(isoStart.Year(), isoStart.Month(), isoStart.Day(), 0, 0, 0, 0, dt.Location())
					}
				case "quarter":
					m := ((int(dt.Month())-1)/3)*3 + 1
					dt = time.Date(dt.Year(), time.Month(m), 1, 0, 0, 0, 0, dt.Location())
				case "month":
					dt = time.Date(dt.Year(), dt.Month(), 1, 0, 0, 0, 0, dt.Location())
				case "week":
					y, w := dt.ISOWeek()
					if monday, ok := projectionISOWeekDate(y, w, 1); ok {
						dt = time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, dt.Location())
					}
				case "day":
					dt = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, dt.Location())
				}
				out["year"] = dt.Year()
				out["month"] = int(dt.Month())
				out["day"] = dt.Day()
				out["hour"] = dt.Hour()
				out["minute"] = dt.Minute()
				out["second"] = dt.Second()
				out["nanosecond"] = dt.Nanosecond()
			}
		}
	}

	supportsTimeTruncation := typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime"
	if supportsTimeTruncation {
		switch unit {
		case "day", "hour", "minute", "second", "millisecond", "microsecond":
			truncateTimeFields(out, unit)
		}
	}

	if typeName == "date" {
		delete(out, "hour")
		delete(out, "minute")
		delete(out, "second")
		delete(out, "nanosecond")
		delete(out, "nanoseconds")
		delete(out, "millisecond")
		delete(out, "microsecond")
		delete(out, "timezone")
	}
	if typeName == "localdatetime" || typeName == "localtime" {
		delete(out, "timezone")
	}
	if typeName == "time" || typeName == "datetime" {
		if _, ok := out["timezone"]; !ok {
			out["timezone"] = "Z"
		}
	}

	return out
}

func applyProjectionTemporalOverrides(target map[string]any, unit string, overrides map[string]any) {
	if target == nil || len(overrides) == 0 {
		return
	}
	unit = strings.ToLower(strings.TrimSpace(unit))
	for key, value := range overrides {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if key == "nanosecond" {
			if add, ok := projectionNumericToInt(value); ok {
				base, _ := projectionNumericToInt(target["nanosecond"])
				target["nanosecond"] = base + add
				continue
			}
		}
		target[key] = value
	}

	if unit == "week" {
		dowRaw, hasDow := overrides["dayOfWeek"]
		if !hasDow {
			return
		}
		dow, ok := projectionNumericToInt(dowRaw)
		if !ok || dow < 1 || dow > 7 {
			return
		}
		year, yOK := projectionNumericToInt(target["year"])
		month, mOK := projectionNumericToInt(target["month"])
		day, dOK := projectionNumericToInt(target["day"])
		if !yOK || !mOK || !dOK {
			return
		}
		base := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		base = base.AddDate(0, 0, dow-1)
		target["year"] = base.Year()
		target["month"] = int(base.Month())
		target["day"] = base.Day()
	}
}

func projectionTemporalDateTime(src map[string]any, withTimezone bool) (time.Time, bool) {
	if src == nil {
		return time.Time{}, false
	}
	year, hasYear := projectionNumericToInt(src["year"])
	if !hasYear {
		year = 1970
	}
	month, hasMonth := projectionNumericToInt(src["month"])
	if !hasMonth {
		month = 1
	}
	day, hasDay := projectionNumericToInt(src["day"])
	if !hasDay {
		day = 1
	}
	hour, _ := projectionNumericToInt(src["hour"])
	minute, _ := projectionNumericToInt(src["minute"])
	second, _ := projectionNumericToInt(src["second"])
	nano := projectionTemporalNanoseconds(src)

	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}

	loc := time.UTC
	if withTimezone {
		tz := projectionTemporalTimezone(src)
		if tz != "" {
			if offset, err := projectionParseOffsetSeconds(tz); err == nil {
				loc = time.FixedZone("", offset)
			} else if named, err := time.LoadLocation(tz); err == nil {
				loc = named
			}
		}
	}

	return time.Date(year, time.Month(month), day, hour, minute, second, nano, loc), true
}

func projectionTemporalNanoseconds(src map[string]any) int {
	if src == nil {
		return 0
	}
	nano := 0
	if n, ok := projectionNumericToInt(src["nanosecond"]); ok {
		nano += n
	}
	if n, ok := projectionNumericToInt(src["nanoseconds"]); ok {
		nano += n
	}
	if ms, ok := projectionNumericToInt(src["millisecond"]); ok {
		nano += ms * 1_000_000
	}
	if us, ok := projectionNumericToInt(src["microsecond"]); ok {
		nano += us * 1_000
	}
	if nano < 0 {
		return 0
	}
	if nano >= 1_000_000_000 {
		nano = nano % 1_000_000_000
	}
	return nano
}

func projectionTemporalTimezone(src map[string]any) string {
	if src == nil {
		return ""
	}
	raw, ok := src["timezone"]
	if !ok || raw == nil {
		return ""
	}
	tz := strings.TrimSpace(fmt.Sprint(raw))
	if strings.EqualFold(tz, "<nil>") {
		return ""
	}
	return tz
}

func projectionParseOffsetSeconds(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "Z" || raw == "z" {
		return 0, nil
	}
	if len(raw) != 6 && len(raw) != 9 {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[0] != '+' && raw[0] != '-' {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[3] != ':' {
		return 0, fmt.Errorf("invalid offset")
	}
	hours, err := strconv.Atoi(raw[1:3])
	if err != nil {
		return 0, err
	}
	minutes, err := strconv.Atoi(raw[4:6])
	if err != nil {
		return 0, err
	}
	seconds := 0
	if len(raw) == 9 {
		if raw[6] != ':' {
			return 0, fmt.Errorf("invalid offset")
		}
		seconds, err = strconv.Atoi(raw[7:9])
		if err != nil {
			return 0, err
		}
	}
	if hours > 18 || minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("invalid offset")
	}
	total := hours*3600 + minutes*60 + seconds
	if raw[0] == '-' {
		total = -total
	}
	return total, nil
}

func projectionISOWeekDate(year, week, dayOfWeek int) (time.Time, bool) {
	if week < 1 || week > 53 || dayOfWeek < 1 || dayOfWeek > 7 {
		return time.Time{}, false
	}
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	weekday := int(jan4.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	week1Monday := jan4.AddDate(0, 0, -(weekday - 1))
	return week1Monday.AddDate(0, 0, (week-1)*7+(dayOfWeek-1)), true
}

func projectionNumericToInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float32:
		return int(math.Trunc(float64(typed))), true
	case float64:
		return int(math.Trunc(typed)), true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return int(math.Trunc(n)), true
	default:
		return 0, false
	}
}

func projectionStrictIntegerValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	default:
		return 0, false
	}
}

func projectionRangeIntegerArg(rawExpr string, value any) (int, bool) {
	if parsed, ok := projectionStrictIntegerValue(value); ok {
		return parsed, true
	}
	allowIntegralFloat := true
	rawExpr = strings.TrimSpace(rawExpr)
	if rawExpr != "" {
		if _, parsedOK := parseProjectionNumericLiteral(rawExpr); parsedOK {
			if strings.ContainsAny(rawExpr, ".eE") {
				allowIntegralFloat = false
			}
		}
	}
	if !allowIntegralFloat {
		return 0, false
	}
	switch typed := value.(type) {
	case float32:
		if math.Trunc(float64(typed)) != float64(typed) {
			return 0, false
		}
		return int(typed), true
	case float64:
		if math.Trunc(typed) != typed {
			return 0, false
		}
		return int(typed), true
	default:
		return 0, false
	}
}

func resolveProjectionEdgeEndpointID(arg string, start bool, row map[string]any, state *State) (string, bool) {
	if row == nil {
		return "", false
	}
	if start {
		if value, ok := row[arg+".src"]; ok {
			if id := scalarString(value); id != "" {
				return id, true
			}
		}
		if value, ok := row[arg+".srcid"]; ok {
			if id := scalarString(value); id != "" {
				return id, true
			}
		}
	} else {
		if value, ok := row[arg+".dst"]; ok {
			if id := scalarString(value); id != "" {
				return id, true
			}
		}
		if value, ok := row[arg+".dstid"]; ok {
			if id := scalarString(value); id != "" {
				return id, true
			}
		}
	}
	if value, ok := row[arg]; ok {
		if id, ok := edgeEndpointIDFromValue(value, start); ok {
			return id, true
		}
		if state != nil && state.Tx != nil {
			if edgeID := scalarString(value); edgeID != "" {
				if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, edgeID); err == nil && edge != nil {
					if start {
						return strings.TrimSpace(edge.SrcID), strings.TrimSpace(edge.SrcID) != ""
					}
					return strings.TrimSpace(edge.DstID), strings.TrimSpace(edge.DstID) != ""
				}
			}
		}
	}
	if boundID, ok := row[arg+".id"]; ok {
		if id, ok := edgeEndpointIDFromValue(boundID, start); ok {
			return id, true
		}
		if state != nil && state.Tx != nil {
			if edgeID := scalarString(boundID); edgeID != "" {
				if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, edgeID); err == nil && edge != nil {
					if start {
						return strings.TrimSpace(edge.SrcID), strings.TrimSpace(edge.SrcID) != ""
					}
					return strings.TrimSpace(edge.DstID), strings.TrimSpace(edge.DstID) != ""
				}
			}
		}
	}
	if value, ok := resolveProjectionPathValue(arg, row, nil); ok {
		if id, ok := edgeEndpointIDFromValue(value, start); ok {
			return id, true
		}
	}
	return "", false
}

func edgeEndpointIDFromValue(value any, start bool) (string, bool) {
	switch typed := value.(type) {
	case *graph.Edge:
		if typed == nil {
			return "", false
		}
		if start {
			return strings.TrimSpace(typed.SrcID), strings.TrimSpace(typed.SrcID) != ""
		}
		return strings.TrimSpace(typed.DstID), strings.TrimSpace(typed.DstID) != ""
	case map[string]any:
		key := "src"
		if !start {
			key = "dst"
		}
		if v, ok := typed[key]; ok {
			if id := scalarString(v); id != "" {
				return id, true
			}
		}
		if v, ok := typed[key+"ID"]; ok {
			if id := scalarString(v); id != "" {
				return id, true
			}
		}
	case string:
		parts := strings.Split(strings.TrimSpace(typed), "|")
		if len(parts) < 4 {
			return "", false
		}
		if start {
			id := strings.TrimSpace(parts[1])
			return id, id != ""
		}
		id := strings.TrimSpace(parts[3])
		return id, id != ""
	}
	return "", false
}

func projectionIsDeletedVertexID(state *State, row map[string]any, vertexID string) bool {
	vertexID = strings.TrimSpace(vertexID)
	if vertexID == "" {
		return false
	}
	if _, deleted := materializedGlobalDeletedVertexIDs(state)[vertexID]; deleted {
		return true
	}
	if row != nil {
		if _, deleted := materializedDeletedVertexIDs(row)[vertexID]; deleted {
			return true
		}
	}
	return false
}

func projectionIsDeletedEdgeID(state *State, row map[string]any, edgeID string) bool {
	edgeID = strings.TrimSpace(edgeID)
	if edgeID == "" {
		return false
	}
	if _, deleted := materializedGlobalDeletedEdgeIDs(state)[edgeID]; deleted {
		return true
	}
	if row != nil {
		if _, deleted := materializedDeletedEdgeIDs(row)[edgeID]; deleted {
			return true
		}
	}
	return false
}

func projectionEdgeTypeFromID(edgeID string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(edgeID), "|")
	if len(parts) < 4 {
		return "", false
	}
	edgeType := strings.TrimSpace(parts[2])
	if edgeType == "" {
		return "", false
	}
	return edgeType, true
}

func buildProjectionNodeValue(vertexID string, row map[string]any, state *State) any {
	vertexID = strings.TrimSpace(vertexID)
	if vertexID == "" {
		return map[string]any{"id": nil}
	}
	if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
		if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, vertexID); err == nil && vertex != nil {
			return vertex
		}
	}

	for key, value := range row {
		if strings.HasPrefix(key, "__") {
			continue
		}
		if strings.TrimSpace(scalarString(value)) != vertexID {
			continue
		}
		if boundID, ok := row[key+".id"]; ok {
			if id := scalarString(boundID); id != "" {
				if numeric, numericOK := parseProjectionNumericLiteral(id); numericOK {
					return map[string]any{"id": numeric}
				}
				return map[string]any{"id": id}
			}
		}
	}

	if numeric, ok := parseProjectionNumericLiteral(vertexID); ok {
		return map[string]any{"id": numeric}
	}
	return map[string]any{"id": vertexID}
}

func decodeProjectionPropertyValue(encoded []byte) any {
	if len(encoded) > 5 && encoded[0] == 0xFF && encoded[1] == 'T' && encoded[2] == 'V' && encoded[3] == 0x01 {
		tag := typedvalue.TypeTag(encoded[4])
		payload := encoded[5:]
		switch tag {
		case typedvalue.TypeBool:
			if string(payload) == "true" {
				return true
			}
			if string(payload) == "false" {
				return false
			}
			return string(payload)
		case typedvalue.TypeInt64:
			if i, err := strconv.ParseInt(string(payload), 10, 64); err == nil {
				if int64(int(i)) == i {
					return int(i)
				}
				return i
			}
			return string(payload)
		case typedvalue.TypeFloat64:
			if f, err := strconv.ParseFloat(string(payload), 64); err == nil {
				return f
			}
			return string(payload)
		case typedvalue.TypeString, typedvalue.TypeBytes:
			return string(payload)
		default:
			encoded = payload
		}
	}
	text := string(encoded)
	if text == "" {
		return ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return text
	}
	if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
		var decoded any
		if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
			return normalizeProjectionDecodedJSONValue(decoded)
		}
	}
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
		if list, ok := parseProjectionLegacyListString(trimmed); ok {
			return list
		}
	}
	if text == "null" {
		return nil
	}
	if text == "true" {
		return true
	}
	if text == "false" {
		return false
	}
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		if int64(int(i)) == i {
			return int(i)
		}
		return i
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		return f
	}
	return text
}

func normalizeProjectionDecodedJSONValue(value any) any {
	switch typed := value.(type) {
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, normalizeProjectionDecodedJSONValue(item))
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeProjectionDecodedJSONValue(item)
		}
		return out
	case float64:
		if math.Trunc(typed) == typed {
			if int64(int(typed)) == int64(typed) {
				return int(typed)
			}
			return int64(typed)
		}
		return typed
	default:
		return typed
	}
}

func parseProjectionLegacyListString(raw string) ([]any, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return []any{}, true
	}

	parts := make([]string, 0)
	start := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(body) && body[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && body[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ',', ' ':
			if depthBracket == 0 && depthBrace == 0 {
				token := strings.TrimSpace(body[start:i])
				if token != "" {
					parts = append(parts, token)
				}
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(body[start:]); tail != "" {
		parts = append(parts, tail)
	}
	if len(parts) == 0 {
		return []any{}, true
	}

	out := make([]any, 0, len(parts))
	for _, token := range parts {
		if list, ok := parseProjectionLegacyListString(token); ok {
			out = append(out, list)
			continue
		}
		if value, ok := parseProjectionStringLiteral(token); ok {
			out = append(out, value)
			continue
		}
		if strings.EqualFold(token, "null") {
			out = append(out, nil)
			continue
		}
		if strings.EqualFold(token, "true") {
			out = append(out, true)
			continue
		}
		if strings.EqualFold(token, "false") {
			out = append(out, false)
			continue
		}
		if i, err := strconv.ParseInt(token, 10, 64); err == nil {
			if int64(int(i)) == i {
				out = append(out, int(i))
			} else {
				out = append(out, i)
			}
			continue
		}
		if f, err := strconv.ParseFloat(token, 64); err == nil {
			out = append(out, f)
			continue
		}
		out = append(out, token)
	}
	return out, true
}

func parseProjectionStringLiteral(expr string) (string, bool) {
	if len(expr) < 2 {
		return "", false
	}
	quote := expr[0]
	if (quote != '\'' && quote != '"') || expr[len(expr)-1] != quote {
		return "", false
	}
	body := expr[1 : len(expr)-1]
	if body == "" {
		return "", true
	}
	var b strings.Builder
	b.Grow(len(body))
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if ch == '\\' {
			if i+1 >= len(body) {
				return "", false
			}
			i++
			next := body[i]
			switch next {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u', 'U':
				hexLen := 4
				if next == 'U' {
					hexLen = 8
				}
				if i+hexLen >= len(body) {
					return "", false
				}
				rawHex := body[i+1 : i+1+hexLen]
				codepoint, err := strconv.ParseInt(rawHex, 16, 32)
				if err != nil {
					return "", false
				}
				b.WriteRune(rune(codepoint))
				i += hexLen
			default:
				b.WriteByte(next)
			}
			continue
		}
		if quote == '\'' && ch == '\'' && i+1 < len(body) && body[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		b.WriteByte(ch)
	}
	return b.String(), true
}

func parseProjectionNumericLiteral(expr string) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	negative := false
	if strings.HasPrefix(expr, "-") {
		negative = true
		expr = strings.TrimSpace(expr[1:])
	} else if strings.HasPrefix(expr, "+") {
		expr = strings.TrimSpace(expr[1:])
	}
	if expr == "" {
		return nil, false
	}

	base := 10
	lower := strings.ToLower(expr)
	if strings.HasPrefix(lower, "0x") {
		base = 16
		expr = expr[2:]
	} else if strings.HasPrefix(lower, "0o") {
		base = 8
		expr = expr[2:]
	}
	if expr == "" {
		return nil, false
	}
	if base != 10 {
		u, err := strconv.ParseUint(expr, base, 64)
		if err == nil {
			if negative {
				if u == 1<<63 {
					return int64(-1 << 63), true
				}
				if u <= uint64(math.MaxInt64) {
					n := -int64(u)
					if int64(int(n)) == n {
						return int(n), true
					}
					return n, true
				}
			} else if u <= uint64(math.MaxInt64) {
				n := int64(u)
				if int64(int(n)) == n {
					return int(n), true
				}
				return n, true
			}
		}
	}

	decimalExpr := expr
	if negative {
		decimalExpr = "-" + decimalExpr
	}
	if n, err := strconv.ParseInt(decimalExpr, 10, 64); err == nil {
		if int64(int(n)) == n {
			return int(n), true
		}
		return n, true
	}
	if f, err := strconv.ParseFloat(decimalExpr, 64); err == nil {
		return f, true
	}
	return nil, false
}

func hydrateProjectionIdentifierValue(expr string, value any, row map[string]any, state *State) (any, bool) {
	if row == nil {
		return nil, false
	}
	if _, hasIDBinding := row[expr+".id"]; !hasIDBinding {
		if _, hasLabelsBinding := row[expr+".labels"]; !hasLabelsBinding {
			return nil, false
		}
	}
	switch value.(type) {
	case []any, []string, []int, []int64, []float64, []bool, map[string]any, map[string]string, *graph.Vertex, *graph.Edge:
		return nil, false
	}
	id := scalarString(value)
	if id == "" {
		if boundID, ok := row[expr+".id"]; ok {
			id = scalarString(boundID)
		}
	}
	if id == "" {
		return nil, false
	}
	if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
		if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, id); err == nil && vertex != nil {
			if patched := overlayHydratedEntityWithRowBindings(expr, vertex, row); patched != nil {
				return patched, true
			}
			return vertex, true
		}
		if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, id); err == nil && edge != nil {
			if patched := overlayHydratedEntityWithRowBindings(expr, edge, row); patched != nil {
				return patched, true
			}
			return edge, true
		}
		if reverseID := projectionReverseEdgeID(id); reverseID != "" {
			if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, reverseID); err == nil && edge != nil {
				if patched := overlayHydratedEntityWithRowBindings(expr, edge, row); patched != nil {
					return patched, true
				}
				return edge, true
			}
		}
	}
	return map[string]any{"id": id}, true
}

func projectionReverseEdgeID(edgeID string) string {
	parts := strings.Split(strings.TrimSpace(edgeID), "|")
	if len(parts) != 4 {
		return ""
	}
	tenant := strings.TrimSpace(parts[0])
	src := strings.TrimSpace(parts[1])
	typeName := strings.TrimSpace(parts[2])
	dst := strings.TrimSpace(parts[3])
	if tenant == "" || src == "" || typeName == "" || dst == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s", tenant, dst, typeName, src)
}

func overlayHydratedEntityWithRowBindings(varName string, entity any, row map[string]any) any {
	if row == nil || strings.TrimSpace(varName) == "" {
		return nil
	}
	prefix := varName + "."
	if prefix == "." {
		return nil
	}
	updates := map[string]any{}
	var labels []string
	hasLabels := false
	labelOrder := readPatternLabelOrder(row, varName)
	hasLabelOrder := len(labelOrder) > 0
	for key, value := range row {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		suffix := strings.TrimSpace(key[len(prefix):])
		if suffix == "" || suffix == "id" {
			continue
		}
		if suffix == "labels" {
			labels = writePathRowLabels(row, varName)
			hasLabels = true
			continue
		}
		updates[suffix] = value
	}
	if len(updates) == 0 && !hasLabels && !hasLabelOrder {
		return nil
	}

	switch typed := entity.(type) {
	case *graph.Vertex:
		if typed == nil {
			return nil
		}
		clone := *typed
		clone.Properties = graph.PropertyMap{}
		for key, value := range typed.Properties {
			clone.Properties[key] = append([]byte(nil), value...)
		}
		for key, value := range updates {
			if value == nil {
				delete(clone.Properties, key)
				continue
			}
			clone.Properties[key] = writeEncodeBindingPropertyValue(value)
		}
		if hasLabels {
			clone.Labels = append([]string(nil), labels...)
		}
		if hasLabelOrder {
			clone.Labels = reorderLabelsByPreferredOrder(clone.Labels, labelOrder)
		}
		return &clone
	case *graph.Edge:
		if typed == nil {
			return nil
		}
		clone := *typed
		clone.Properties = graph.PropertyMap{}
		for key, value := range typed.Properties {
			clone.Properties[key] = append([]byte(nil), value...)
		}
		for key, value := range updates {
			if value == nil {
				delete(clone.Properties, key)
				continue
			}
			clone.Properties[key] = writeEncodeBindingPropertyValue(value)
		}
		return &clone
	default:
		return nil
	}
}

type sortHandler struct{ baseHandler }

type sortVariantExecutor func(nodeID string, attrs map[string]any, state *State) error

var sortVariantExecutors = map[string]sortVariantExecutor{
	"sort_full":      executeSortVariantFull,
	"sort_topk_heap": executeSortVariantTopK,
}

func NewSortHandler() Handler { return sortHandler{baseHandler{op: "PHY_SORT"}} }

func (h sortHandler) ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *State) (bool, error) {
	executor, ok := sortVariantExecutors[strings.ToLower(strings.TrimSpace(variant))]
	if !ok {
		return false, nil
	}
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return true, err
	}
	return true, executor(nodeID, attrs, state)
}

func (h sortHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	variant := sortVariantFromAttrs(attrs)
	if variant == "sort_topk_heap" {
		return executeSortVariantTopK(nodeID, attrs, state)
	}
	return executeSortVariantFull(nodeID, attrs, state)
}

func sortVariantFromAttrs(attrs map[string]any) string {
	variant := strings.ToLower(strings.TrimSpace(stringAttr(attrs, "variant")))
	if variant != "" {
		return variant
	}
	strategy := strings.ToLower(strings.TrimSpace(stringAttr(attrs, "strategy")))
	if strategy == "topk_heap" {
		return "sort_topk_heap"
	}
	return "sort_full"
}

func executeSortVariantFull(nodeID string, attrs map[string]any, state *State) error {
	return executeSort(nodeID, attrs, state, false)
}

func executeSortVariantTopK(nodeID string, attrs map[string]any, state *State) error {
	return executeSort(nodeID, attrs, state, true)
}

func executeSort(nodeID string, attrs map[string]any, state *State, trimTopK bool) error {
	_ = nodeID
	if state == nil || len(state.Rows) <= 1 {
		return nil
	}
	ordering := parseOrderingSpec(attrs)
	if len(ordering) == 0 {
		return nil
	}
	sourceRows := state.SortSourceRows
	if len(sourceRows) != len(state.Rows) {
		sourceRows = nil
	}
	type sortableRow struct {
		row  map[string]any
		keys []any
	}
	sortRows := make([]sortableRow, 0, len(state.Rows))
	for idx, row := range state.Rows {
		keys := make([]any, 0, len(ordering))
		for _, ord := range ordering {
			if isSimpleOrderingReferenceExpression(ord.expression) {
				if v, exists := valueFromRowWithPresence(row, ord.expression); exists {
					keys = append(keys, v)
					continue
				}
				if sourceRows != nil {
					if v, exists := valueFromRowWithPresence(sourceRows[idx], ord.expression); exists {
						keys = append(keys, v)
						continue
					}
				}
			}

			value, ok, err := resolveProjectionExprValueChecked(ord.expression, row, state)
			if err != nil {
				return err
			}
			if !ok || value == nil {
				if v, exists := valueFromRowWithPresence(row, ord.expression); exists {
					value = v
					ok = true
				}
			}
			if !ok || value == nil {
				if v, exists := projectionOrderingTerminalAliasValue(ord.expression, row); exists {
					value = v
					ok = true
				}
			}
			if (!ok || value == nil) && sourceRows != nil {
				source := sourceRows[idx]
				if resolved, resolvedOK, resolvedErr := resolveProjectionExprValueChecked(ord.expression, source, state); resolvedErr != nil {
					return resolvedErr
				} else if resolvedOK {
					value = resolved
					ok = true
				} else if v, exists := valueFromRowWithPresence(source, ord.expression); exists {
					value = v
					ok = true
				}
			}
			if !ok {
				value = nil
			}
			keys = append(keys, value)
		}
		sortRows = append(sortRows, sortableRow{row: row, keys: keys})
	}

	sort.SliceStable(sortRows, func(i, j int) bool {
		left := sortRows[i]
		right := sortRows[j]
		for _, ord := range ordering {
			lv := left.keys[0]
			rv := right.keys[0]
			left.keys = left.keys[1:]
			right.keys = right.keys[1:]
			cmp := compareScalarValuesObserved(state, lv, rv)
			if cmp == 0 {
				continue
			}
			if ord.descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	rows := make([]map[string]any, 0, len(sortRows))
	for _, item := range sortRows {
		rows = append(rows, item.row)
	}
	state.Rows = rows

	if trimTopK {
		topK := intAttr(attrs, "topK")
		if topK > 0 && len(state.Rows) > topK {
			state.Rows = append([]map[string]any(nil), state.Rows[:topK]...)
		}
	}
	state.SortSourceRows = nil

	return nil
}

func projectionOrderingTerminalAliasValue(expr string, row map[string]any) (any, bool) {
	if row == nil {
		return nil, false
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	idx := strings.LastIndex(expr, ".")
	if idx <= 0 || idx >= len(expr)-1 {
		return nil, false
	}
	key := normalizeProjectionPropertyKey(expr[idx+1:])
	if key == "" {
		return nil, false
	}
	v, ok := row[key]
	return v, ok
}

type paginationHandler struct{ baseHandler }

func NewPaginationHandler() Handler { return paginationHandler{baseHandler{op: "PHY_PAGINATION"}} }

func (h paginationHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil || len(state.Rows) == 0 {
		return nil
	}
	skip, _, err := resolvePaginationIntAttr(attrs, "skip", state)
	if err != nil {
		return err
	}
	limit, hasLimit, err := resolvePaginationIntAttr(attrs, "limit", state)
	if err != nil {
		return err
	}
	if skip >= len(state.Rows) {
		state.Rows = []map[string]any{}
		return nil
	}
	rows := state.Rows[skip:]
	if hasLimit {
		if limit <= 0 {
			state.Rows = []map[string]any{}
			return nil
		}
		if limit < len(rows) {
			rows = rows[:limit]
		}
	}
	state.Rows = rows
	return nil
}

func resolvePaginationIntAttr(attrs map[string]any, key string, state *State) (int, bool, error) {
	if attrs == nil {
		return 0, false, nil
	}
	raw, exists := attrs[key]
	if !exists || raw == nil {
		return 0, false, nil
	}
	if n, ok := projectionStrictIntegerValue(raw); ok {
		if n < 0 {
			return 0, false, graph.NewError(graph.ErrKindUnsupported, "NegativeIntegerArgument", nil)
		}
		return n, true, nil
	}
	if text, ok := raw.(string); ok {
		text = strings.TrimSpace(text)
		if text == "" {
			return 0, false, nil
		}
		if state != nil {
			value, ok, err := resolveProjectionExprValueChecked(text, map[string]any{}, state)
			if err == nil && ok {
				if n, nOK := projectionStrictIntegerValue(value); nOK {
					if n < 0 {
						return 0, false, graph.NewError(graph.ErrKindUnsupported, "NegativeIntegerArgument", nil)
					}
					return n, true, nil
				}
				return 0, false, graph.NewError(graph.ErrKindUnsupported, "InvalidArgumentType", nil)
			}
			if err != nil {
				return 0, false, err
			}
		}
		if n, err := strconv.Atoi(text); err == nil {
			if n < 0 {
				return 0, false, graph.NewError(graph.ErrKindUnsupported, "NegativeIntegerArgument", nil)
			}
			return n, true, nil
		}
		if _, err := strconv.ParseFloat(text, 64); err == nil {
			return 0, false, graph.NewError(graph.ErrKindUnsupported, "InvalidArgumentType", nil)
		}
	}
	return 0, false, graph.NewError(graph.ErrKindUnsupported, "InvalidArgumentType", nil)
}

type writeHandler struct{ baseHandler }

func NewWriteHandler() Handler { return writeHandler{baseHandler{op: "PHY_WRITE"}} }

func (h writeHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}

	base := WriteEvent{NodeID: nodeID}
	kind := ""
	raw := ""
	pattern := ""
	mergePattern := ""
	onCreate := []string{}
	onMatch := []string{}
	if attrs != nil {
		if v, ok := attrs["kind"].(string); ok {
			kind = strings.TrimSpace(v)
		}
		if v, ok := attrs["raw"].(string); ok {
			raw = strings.TrimSpace(v)
			base.Raw = raw
		}
		if v, ok := attrs["pattern"].(string); ok {
			pattern = strings.TrimSpace(v)
			base.Pattern = pattern
		}
		if v, ok := attrs["mergePattern"].(string); ok {
			mergePattern = strings.TrimSpace(v)
			base.MergePattern = mergePattern
		}
		onCreate = stringSliceAttr(attrs, "mergeOnCreate")
		if len(onCreate) > 0 {
			base.MergeOnCreate = append([]string(nil), onCreate...)
		}
		onMatch = stringSliceAttr(attrs, "mergeOnMatch")
		if len(onMatch) > 0 {
			base.MergeOnMatch = append([]string(nil), onMatch...)
		}
	}
	if strings.TrimSpace(kind) == "" {
		kind = inferWriteKindFromRaw(raw)
	}
	base.Kind = kind
	base.MutationType = classifyMutationType(kind, pattern, mergePattern, raw)
	base.Vertex, base.Edge = buildMutationPayloads(base.MutationType, kind, pattern, mergePattern, raw)
	base.ResolvedParams = resolveWriteParams(state, raw, mergePattern, onCreate, onMatch)
	base.ParamKeys = referencedParamKeys(raw, pattern, mergePattern, strings.Join(onCreate, " "), strings.Join(onMatch, " "))
	writeEvents := expandWriteEventsForClause(base)

	materialize := shouldMaterializeWriteBindings(state)
	rows := state.Rows
	if len(rows) == 0 {
		if materialize {
			state.Rows = []map[string]any{{}}
			rows = state.Rows
		} else {
			rows = []map[string]any{{}}
		}
	}
	materializedRows := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		rowBatch := []map[string]any{row}
		if materialize && row != nil {
			if expanded := expandUnconstrainedMergeVertexRowsForMaterialization(state, row, base); len(expanded) > 0 {
				rowBatch = expanded
			}
			if expanded := expandMergeEdgeRowsForMaterialization(state, rowBatch, base); len(expanded) > 0 {
				rowBatch = expanded
			}
		}
		preserveSequentialBindings := len(writeEvents) > 1
		for _, eventTemplate := range writeEvents {
			nextBatch := make([]map[string]any, 0, len(rowBatch))
			for _, expandedRow := range rowBatch {
				workingRow := expandedRow
				if workingRow == nil {
					workingRow = map[string]any{}
				}
				event := eventTemplate
				event.Bindings = captureBindingsFromRow(workingRow)
				if materialize || preserveSequentialBindings {
					materializeWriteBindings(workingRow, event, state)
					event.Bindings = captureBindingsFromRow(workingRow)
				}
				state.WriteEvents = append(state.WriteEvents, event)
				if materialize {
					workingRow["__write_event_count"] = len(state.WriteEvents)
				}
				nextBatch = append(nextBatch, workingRow)
			}
			rowBatch = nextBatch
		}
		if materialize {
			materializedRows = append(materializedRows, rowBatch...)
		}
	}
	if materialize {
		state.Rows = materializedRows
	}

	return nil
}

func expandWriteEventsForClause(base WriteEvent) []WriteEvent {
	if !strings.EqualFold(strings.TrimSpace(base.Kind), "CREATE") {
		return []WriteEvent{base}
	}
	body := strings.TrimSpace(base.Pattern)
	if body == "" {
		body = extractWritePatternFromRaw(base.Raw, base.Kind)
	}
	segments := writeSplitTopLevelComma(body)
	if len(segments) <= 1 {
		return []WriteEvent{base}
	}
	out := make([]WriteEvent, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		event := base
		event.Raw = strings.TrimSpace(base.Kind) + " " + segment
		event.Pattern = segment
		event.MergePattern = ""
		event.MergeOnCreate = nil
		event.MergeOnMatch = nil
		event.MutationType = classifyMutationType(event.Kind, event.Pattern, event.MergePattern, event.Raw)
		event.Vertex, event.Edge = buildMutationPayloads(event.MutationType, event.Kind, event.Pattern, event.MergePattern, event.Raw)
		event.ParamKeys = referencedParamKeys(event.Raw, event.Pattern, event.MergePattern, "", "")
		out = append(out, event)
	}
	if len(out) == 0 {
		return []WriteEvent{base}
	}
	return out
}

func expandUnconstrainedMergeVertexRowsForMaterialization(state *State, row map[string]any, event WriteEvent) []map[string]any {
	if state == nil || state.Tx == nil || event.Vertex == nil {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
		return nil
	}
	varName := strings.TrimSpace(event.Vertex.Var)
	if varName == "" {
		return nil
	}
	if bound, _ := resolveBoundEntityID(row, varName); bound {
		return nil
	}
	if strings.TrimSpace(event.Vertex.IDParam) != "" {
		return nil
	}
	if len(event.Vertex.Labels) != 0 {
		return nil
	}
	if len(resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)) != 0 {
		return nil
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	ids := make([]string, 0)
	_ = state.Tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil {
			return nil
		}
		id := strings.TrimSpace(vertex.ID)
		if id != "" {
			ids = append(ids, id)
		}
		return nil
	})
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		next := cloneRow(row)
		bindWriteVariable(next, varName, id)
		out = append(out, next)
	}
	return out
}

func expandMergeEdgeRowsForMaterialization(state *State, rows []map[string]any, event WriteEvent) []map[string]any {
	if state == nil || state.Tx == nil || event.Edge == nil {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
		return nil
	}
	if len(rows) == 0 {
		return nil
	}
	varName := strings.TrimSpace(event.Edge.Var)
	if varName == "" {
		return nil
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	if edgeType == "" {
		return nil
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	if tenant == "" {
		return nil
	}
	undirected := writePatternIsUndirected(event.Edge.Pattern)
	if undirected {
		return nil
	}
	if len(materializedGlobalDeletedEdgeIDs(state)) != 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		bindings := captureBindingsFromRow(row)
		for k, v := range event.Bindings {
			if _, ok := bindings[k]; !ok {
				bindings[k] = v
			}
		}
		boundEdgeID := strings.TrimSpace(resolveWriteEntityID(varName, "", bindings, event.ResolvedParams))
		if writePathCanonicalMergeEdgeBaseID(boundEdgeID) {
			boundEdgeID = ""
		}
		if boundEdgeID != "" {
			if edge, err := state.Tx.GetEdge(ctx, tenant, boundEdgeID); err == nil && edge != nil {
				next := cloneRow(row)
				bindWriteVariable(next, varName, boundEdgeID)
				out = append(out, next)
				continue
			}
		}
		expectedProps := edgePatternExpectedProperties(event.Edge.Pattern, event.ResolvedParams, bindings, state)
		leftID := resolveWriteEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, bindings, event.ResolvedParams)
		rightID := resolveWriteEntityID(event.Edge.RightVar, event.Edge.RightIDParam, bindings, event.ResolvedParams)
		if leftID == "" || rightID == "" {
			out = append(out, row)
			continue
		}
		srcID := leftID
		dstID := rightID
		if event.Edge.Reverse {
			srcID = rightID
			dstID = leftID
		}
		matched := collectMergeDirectedEdgeIDs(ctx, state.Tx, tenant, srcID, dstID, edgeType, expectedProps)
		if undirected {
			reverse := collectMergeDirectedEdgeIDs(ctx, state.Tx, tenant, dstID, srcID, edgeType, expectedProps)
			matched = append(matched, reverse...)
		}
		deletedEdges := materializedGlobalDeletedEdgeIDs(state)
		if len(deletedEdges) != 0 {
			filtered := make([]string, 0, len(matched))
			for _, edgeID := range matched {
				if _, deleted := deletedEdges[strings.TrimSpace(edgeID)]; deleted {
					continue
				}
				filtered = append(filtered, edgeID)
			}
			matched = filtered
		}
		if len(matched) == 0 {
			out = append(out, row)
			continue
		}
		for _, edgeID := range matched {
			next := cloneRow(row)
			bindWriteVariable(next, varName, edgeID)
			out = append(out, next)
		}
	}
	return out
}

func collectMergeDirectedEdgeIDs(ctx context.Context, tx graph.Tx, tenant, srcID, dstID, edgeType string, expectedProps map[string]any) []string {
	if tx == nil || tenant == "" || srcID == "" || dstID == "" || edgeType == "" {
		return nil
	}
	ids := make([]string, 0)
	_ = tx.ScanOutEdgeLinksByType(ctx, tenant, edgeType, 0, func(linkSrcID, edgeID, linkDstID string) error {
		if strings.TrimSpace(linkSrcID) != srcID || strings.TrimSpace(linkDstID) != dstID {
			return nil
		}
		id := strings.TrimSpace(edgeID)
		if id == "" {
			return nil
		}
		edge, err := tx.GetEdge(ctx, tenant, id)
		if err != nil || edge == nil {
			return nil
		}
		if len(expectedProps) != 0 && !edgeHasExpectedProperties(edge, expectedProps) {
			return nil
		}
		storedID := strings.TrimSpace(edge.ID)
		if storedID == "" {
			return nil
		}
		ids = append(ids, storedID)
		return nil
	})
	return ids
}

func writePathCanonicalMergeEdgeBaseID(edgeID string) bool {
	edgeID = strings.TrimSpace(edgeID)
	if edgeID == "" {
		return false
	}
	parts := strings.Split(edgeID, "|")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return false
		}
	}
	return true
}

func writePatternIsUndirected(pattern string) bool {
	raw := strings.TrimSpace(pattern)
	if raw == "" {
		return false
	}
	return strings.Contains(raw, "]-(") && !strings.Contains(raw, "]->") && !strings.Contains(raw, "<-[")
}

func captureBindingsFromRow(row map[string]any) map[string]any {
	if len(row) == 0 {
		return nil
	}
	bindings := map[string]any{}
	for key, value := range row {
		if strings.HasPrefix(key, "__") {
			continue
		}
		bindings[key] = value
	}
	if len(bindings) == 0 {
		return nil
	}
	return bindings
}

func shouldMaterializeWriteBindings(state *State) bool {
	if state == nil {
		return true
	}
	if state.Params == nil {
		return state.MaterializeWriteBindings
	}
	value, ok := state.Params["__ve_materialize_write_bindings"]
	if !ok || value == nil {
		return state.MaterializeWriteBindings
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		default:
			return state.MaterializeWriteBindings
		}
	default:
		return state.MaterializeWriteBindings
	}
}

func materializeWriteBindings(row map[string]any, event WriteEvent, state *State) {
	if row == nil {
		return
	}
	kind := strings.TrimSpace(event.Kind)
	materializeWriteDeleteTombstones(row, event, state)
	if strings.EqualFold(kind, "CREATE") || strings.EqualFold(kind, "MERGE") {
		materializeCreatePatternBindings(row, event, state)
	}
	if event.Vertex != nil {
		vertexID := resolveWriteEntityID(event.Vertex.Var, event.Vertex.IDParam, event.Bindings, event.ResolvedParams)
		if vertexID == "" && strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
			if matchedID := resolveMergeVertexIDForWriteMaterialization(event, state); matchedID != "" {
				vertexID = matchedID
			}
		}
		if vertexID == "" && strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
			allowGenericFallback := false
			if event.Vertex != nil {
				allowGenericFallback = strings.TrimSpace(event.Vertex.IDParam) == "" && len(event.Vertex.Labels) == 0 && len(resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)) == 0
			}
			if boundID := resolveMergeBoundEntityIDForWriteMaterialization(event.Bindings, event.Vertex.Var, allowGenericFallback); boundID != "" {
				vertexID = boundID
			}
		}
		if vertexID == "" && strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
			if syntheticID := resolveMergeSyntheticVertexIDForWriteMaterialization(event, state); syntheticID != "" {
				vertexID = syntheticID
			} else if event.Vertex != nil && len(resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)) > 0 {
				vertexID = resolveMergePropertySyntheticVertexIDForWriteMaterialization(event, state)
			}
		}
		if vertexID != "" {
			bindWriteVariable(row, event.Vertex.Var, vertexID)
		}
		if varName := strings.TrimSpace(event.Vertex.Var); varName != "" {
			labels := append([]string(nil), event.Vertex.Labels...)
			if labels == nil {
				labels = []string{}
			}
			row[varName+".labels"] = labels
			props := resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)
			for key, value := range props {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				if strings.EqualFold(key, "id") {
					continue
				}
				row[varName+"."+key] = value
			}
			if state != nil && state.Tx != nil {
				if entityID := strings.TrimSpace(scalarString(row[varName])); entityID != "" {
					if vertex, err := state.Tx.GetVertex(context.Background(), strings.TrimSpace(state.Tenant), entityID); err == nil && vertex != nil {
						if rawID, ok := vertex.Properties["id"]; ok {
							row[varName+".id"] = decodeProjectionPropertyValue(rawID)
						}
					}
				}
			}
		}
	}
	if event.Edge != nil {
		leftID := resolveWriteEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
		rightID := resolveWriteEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
		if leftID != "" {
			bindWriteVariable(row, event.Edge.LeftVar, leftID)
		}
		if rightID != "" {
			bindWriteVariable(row, event.Edge.RightVar, rightID)
		}
		if varName := strings.TrimSpace(event.Edge.Var); varName != "" {
			edgeID := materializeWriteEdgeID(event, state, leftID, rightID)
			if edgeID != "" {
				bindWriteVariable(row, varName, edgeID)
			}
			props := resolveWritePropertyBindingsState(event.Edge.Pattern, event.ResolvedParams, event.Bindings, state)
			for key, value := range props {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				row[varName+"."+key] = value
			}
		}
		if endpointMatches := edgePatternEndpointsRe.FindStringSubmatch(strings.TrimSpace(event.Edge.Pattern)); len(endpointMatches) == 3 {
			leftPattern := "(" + strings.TrimSpace(endpointMatches[1]) + ")"
			rightPattern := "(" + strings.TrimSpace(endpointMatches[2]) + ")"
			leftVar := strings.TrimSpace(variableFromPatternSegment(leftPattern))
			rightVar := strings.TrimSpace(variableFromPatternSegment(rightPattern))
			if leftVar != "" {
				if labels := labelsFromPatternSegment(leftPattern); len(labels) > 0 {
					row[leftVar+".labels"] = append([]string(nil), labels...)
				}
			}
			if rightVar != "" {
				if labels := labelsFromPatternSegment(rightPattern); len(labels) > 0 {
					row[rightVar+".labels"] = append([]string(nil), labels...)
				}
			}
		}
	}
	materializeWriteMergePathBinding(row, event, state)
	materializeWriteMergeActionBindings(row, event, state)
	materializeWriteSetBindings(row, event, state)
	materializeWriteRemoveBindings(row, event, state)
}

func materializeWriteMergeActionBindings(row map[string]any, event WriteEvent, state *State) {
	if row == nil || !strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
		return
	}
	if len(event.MergeOnCreate) == 0 && len(event.MergeOnMatch) == 0 {
		return
	}
	actions := event.MergeOnCreate
	if writeMergeMatchedForMaterialization(event, state) {
		actions = event.MergeOnMatch
	}
	for _, action := range actions {
		body := extractMaterializedWriteActionBody(action, "SET")
		if body == "" {
			continue
		}
		materializeWriteSetBodyBindings(row, body, state)
	}
}

func writeMergeMatchedForMaterialization(event WriteEvent, state *State) bool {
	if state == nil || state.Tx == nil {
		return false
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	if tenant == "" {
		return false
	}
	if event.Vertex != nil {
		if vertexID := resolveWriteEntityID(event.Vertex.Var, event.Vertex.IDParam, event.Bindings, event.ResolvedParams); vertexID != "" {
			if vertex, err := state.Tx.GetVertex(ctx, tenant, vertexID); err == nil && vertex != nil {
				return true
			}
		}
		return resolveMergeVertexIDForWriteMaterialization(event, state) != ""
	}
	if event.Edge != nil {
		leftID := resolveWriteEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
		rightID := resolveWriteEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
		edgeType := strings.TrimSpace(event.Edge.Type)
		if leftID == "" || rightID == "" || edgeType == "" {
			return false
		}
		srcID := leftID
		dstID := rightID
		if event.Edge.Reverse {
			srcID = rightID
			dstID = leftID
		}
		edgeID := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
		if edge, err := state.Tx.GetEdge(ctx, tenant, edgeID); err == nil && edge != nil {
			return true
		}
	}
	return false
}

func extractMaterializedWriteActionBody(raw string, kind string) string {
	raw = strings.TrimSpace(raw)
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if raw == "" || kind == "" {
		return ""
	}
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "ON CREATE ") {
		raw = strings.TrimSpace(raw[len("ON CREATE "):])
		upper = strings.ToUpper(raw)
	} else if strings.HasPrefix(upper, "ON MATCH ") {
		raw = strings.TrimSpace(raw[len("ON MATCH "):])
		upper = strings.ToUpper(raw)
	}
	if strings.HasPrefix(upper, kind+" ") {
		return strings.TrimSpace(raw[len(kind):])
	}
	if upper == kind {
		return ""
	}
	return raw
}

func materializeWriteMergePathBinding(row map[string]any, event WriteEvent, state *State) {
	if row == nil || !strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
		return
	}
	pathVar, assignedPattern, ok := splitProjectionPathAssignment(strings.TrimSpace(event.MergePattern))
	if !ok {
		pathVar, assignedPattern, ok = splitProjectionPathAssignment(strings.TrimSpace(event.Pattern))
	}
	if !ok {
		pathVar, assignedPattern, ok = splitProjectionPathAssignment(extractWritePatternFromRaw(event.Raw, "MERGE"))
	}
	if !ok || strings.TrimSpace(pathVar) == "" {
		return
	}

	if edgeMutation := parseEdgeMutation(assignedPattern); edgeMutation != nil {
		endpointMatches := edgePatternEndpointsRe.FindStringSubmatch(assignedPattern)
		if len(endpointMatches) != 3 {
			return
		}
		left := buildWritePathVertexValue("("+strings.TrimSpace(endpointMatches[1])+")", event, row, state)
		right := buildWritePathVertexValue("("+strings.TrimSpace(endpointMatches[2])+")", event, row, state)
		relationship := buildWritePathEdgeValue(edgeMutation, event, row, state)
		row[pathVar] = projectionPathValue{
			nodes:         []any{left, right},
			relationships: []any{relationship},
		}
		return
	}

	vertexPattern := firstVertexPatternSegment(assignedPattern)
	if vertexPattern == "" {
		vertexPattern = strings.TrimSpace(assignedPattern)
	}
	if vertexPattern == "" {
		return
	}
	row[pathVar] = projectionPathValue{
		nodes:         []any{buildWritePathVertexValue(vertexPattern, event, row, state)},
		relationships: nil,
	}
}

func buildWritePathVertexValue(pattern string, event WriteEvent, row map[string]any, state *State) *graph.Vertex {
	pattern = strings.TrimSpace(pattern)
	vertex := &graph.Vertex{}
	varName := strings.TrimSpace(variableFromPatternSegment(pattern))
	idParam := strings.TrimSpace(idParamFromPatternSegment(pattern))

	if varName != "" {
		if bound, ok := row[varName].(*graph.Vertex); ok && bound != nil {
			return bound
		}
		if bound, ok := event.Bindings[varName].(*graph.Vertex); ok && bound != nil {
			return bound
		}
	}

	if id := resolveWritePathEntityID(varName, idParam, row, event.Bindings, event.ResolvedParams); id != "" {
		vertex.ID = id
		if state != nil && state.Tx != nil {
			tenant := strings.TrimSpace(state.Tenant)
			if tenant != "" {
				ctx := context.Background()
				if state.Context != nil {
					ctx = state.Context
				}
				if stored, err := state.Tx.GetVertex(ctx, tenant, id); err == nil && stored != nil {
					return stored
				}
			}
		}
	}
	if labels := labelsFromPatternSegment(pattern); len(labels) > 0 {
		vertex.Labels = append([]string(nil), labels...)
	} else if inherited := writePathRowLabels(row, varName); len(inherited) > 0 {
		vertex.Labels = inherited
	}
	props := resolveWritePropertyBindingsState(pattern, event.ResolvedParams, row, state)
	if len(props) == 0 {
		props = writePathRowProperties(row, varName)
	}
	if len(props) > 0 {
		vertex.Properties = writePathPropertiesToGraphPropertyMap(props)
	}
	return vertex
}

func writePathRowLabels(row map[string]any, varName string) []string {
	varName = strings.TrimSpace(varName)
	if row == nil || varName == "" {
		return nil
	}
	raw, ok := row[varName+".labels"]
	if !ok || raw == nil {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, label := range typed {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			out = append(out, label)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			label := strings.TrimSpace(scalarString(item))
			if label == "" {
				continue
			}
			out = append(out, label)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	default:
		label := strings.TrimSpace(scalarString(raw))
		if label == "" {
			return nil
		}
		return []string{label}
	}
}

func writePathRowProperties(row map[string]any, varName string) map[string]any {
	varName = strings.TrimSpace(varName)
	if row == nil || varName == "" {
		return nil
	}
	prefix := varName + "."
	out := map[string]any{}
	for key, value := range row {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		name := strings.TrimSpace(strings.TrimPrefix(key, prefix))
		if name == "" || name == "id" || name == "labels" {
			continue
		}
		out[name] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func resolveWritePathEntityID(varName, idParam string, row, eventBindings, resolvedParams map[string]any) string {
	varName = strings.TrimSpace(varName)
	if varName != "" {
		if id := writePathEntityIDFromValue(row[varName+".id"]); id != "" {
			return id
		}
		if id := writePathEntityIDFromValue(eventBindings[varName+".id"]); id != "" {
			return id
		}
		if id := writePathEntityIDFromValue(row[varName]); id != "" {
			return id
		}
		if id := writePathEntityIDFromValue(eventBindings[varName]); id != "" {
			return id
		}
	}
	idParam = strings.TrimSpace(idParam)
	if idParam != "" {
		if id := writePathEntityIDFromValue(resolvedParams[idParam]); id != "" {
			return id
		}
	}
	return ""
}

func writePathEntityIDFromValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case *graph.Edge:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case map[string]any:
		if id := scalarString(typed["id"]); id != "" {
			return strings.TrimSpace(id)
		}
		if id := scalarString(typed["ID"]); id != "" {
			return strings.TrimSpace(id)
		}
		return ""
	default:
		return strings.TrimSpace(scalarString(value))
	}
}

func buildWritePathEdgeValue(edgeMutation *EdgeMutation, event WriteEvent, row map[string]any, state *State) *graph.Edge {
	edge := &graph.Edge{}
	if edgeMutation != nil {
		edge.Type = strings.TrimSpace(edgeMutation.Type)
		bindings := map[string]any{}
		for key, value := range event.Bindings {
			bindings[key] = value
		}
		for key, value := range row {
			bindings[key] = value
		}
		if edgeID := resolveWriteEntityID(strings.TrimSpace(edgeMutation.Var), "", bindings, event.ResolvedParams); edgeID != "" {
			edge.ID = edgeID
		}
		if props := resolveWritePropertyBindingsState(writePathEdgeBracketSegment(edgeMutation.Pattern), event.ResolvedParams, row, state); len(props) > 0 {
			edge.Properties = writePathPropertiesToGraphPropertyMap(props)
		}
	}
	return edge
}

func writePathEdgeBracketSegment(pattern string) string {
	start := strings.IndexByte(pattern, '[')
	end := strings.IndexByte(pattern, ']')
	if start < 0 || end <= start {
		return ""
	}
	return strings.TrimSpace(pattern[start : end+1])
}

func writePathPropertiesToGraphPropertyMap(props map[string]any) graph.PropertyMap {
	if len(props) == 0 {
		return nil
	}
	out := graph.PropertyMap{}
	for key, value := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		encoded, ok := writePathPropertyValueToBytes(value)
		if !ok {
			continue
		}
		out[key] = encoded
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writePathPropertyValueToBytes(value any) ([]byte, bool) {
	if value == nil {
		return nil, false
	}
	switch typed := value.(type) {
	case string:
		return []byte(typed), true
	case bool:
		if typed {
			return []byte("true"), true
		}
		return []byte("false"), true
	case int:
		return []byte(strconv.Itoa(typed)), true
	case int8:
		return []byte(strconv.FormatInt(int64(typed), 10)), true
	case int16:
		return []byte(strconv.FormatInt(int64(typed), 10)), true
	case int32:
		return []byte(strconv.FormatInt(int64(typed), 10)), true
	case int64:
		return []byte(strconv.FormatInt(typed, 10)), true
	case uint:
		return []byte(strconv.FormatUint(uint64(typed), 10)), true
	case uint8:
		return []byte(strconv.FormatUint(uint64(typed), 10)), true
	case uint16:
		return []byte(strconv.FormatUint(uint64(typed), 10)), true
	case uint32:
		return []byte(strconv.FormatUint(uint64(typed), 10)), true
	case uint64:
		return []byte(strconv.FormatUint(typed, 10)), true
	case float32:
		return []byte(strconv.FormatFloat(float64(typed), 'f', -1, 32)), true
	case float64:
		return []byte(strconv.FormatFloat(typed, 'f', -1, 64)), true
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return []byte(fmt.Sprint(typed)), true
		}
		return encoded, true
	}
}

func resolveMergeVertexIDForWriteMaterialization(event WriteEvent, state *State) string {
	if state == nil || state.Tx == nil || event.Vertex == nil {
		return ""
	}
	labels := append([]string(nil), event.Vertex.Labels...)
	filteredLabels := make([]string, 0, len(labels))
	for i := range labels {
		label := strings.TrimSpace(labels[i])
		if label == "" {
			continue
		}
		filteredLabels = append(filteredLabels, label)
	}
	labels = filteredLabels
	sort.Strings(labels)
	props := resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)
	if len(labels) == 0 && len(props) == 0 {
		return ""
	}
	ctx := context.Background()
	if state.Context != nil {
		ctx = state.Context
	}
	tenant := strings.TrimSpace(state.Tenant)
	matchedID := ""
	cacheKey := ""
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	cacheKey = "__ve_merge_vertex_lookup|" + tenant + "|" + strings.Join(labels, ":") + "|" + stableRowKey(props)
	deleted := materializedDeletedVertexIDs(event.Bindings)
	for id := range materializedGlobalDeletedVertexIDs(state) {
		deleted[id] = struct{}{}
	}
	if cached, ok := state.Params[cacheKey]; ok {
		cachedID := strings.TrimSpace(fmt.Sprint(cached))
		if cachedID == "__none__" {
			return ""
		}
		if cachedID != "" {
			if _, removed := deleted[cachedID]; !removed {
				return cachedID
			}
		}
	}
	if tenant != "" && len(labels) > 0 && len(props) > 0 {
		keys := make([]string, 0, len(props))
		for key := range props {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			for _, probeKey := range keys {
				if matchedID != "" {
					break
				}
				encoded, ok := writePathPropertyValueToBytes(props[probeKey])
				if !ok {
					continue
				}
				for _, label := range labels {
					if matchedID != "" {
						break
					}
					_ = state.Tx.ScanPropertyIndex(ctx, tenant, label, probeKey, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
						if matchedID != "" || entry == nil {
							return nil
						}
						if !strings.EqualFold(strings.TrimSpace(entry.EntityClass), "vertex") {
							return nil
						}
						candidateID := strings.TrimSpace(entry.EntityID)
						if candidateID == "" {
							return nil
						}
						if _, skipped := deleted[candidateID]; skipped {
							return nil
						}
						vertex, err := state.Tx.GetVertex(ctx, tenant, candidateID)
						if err != nil || vertex == nil {
							return nil
						}
						if !vertexHasAllLabels(vertex, labels) {
							return nil
						}
						if !vertexHasExpectedProperties(vertex, props) {
							return nil
						}
						matchedID = candidateID
						return nil
					})
				}
			}
		}
		if matchedID != "" {
			state.Params[cacheKey] = matchedID
			return matchedID
		}
	}
	_ = state.Tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if matchedID != "" {
			return nil
		}
		if vertex == nil {
			return nil
		}
		if _, ok := deleted[strings.TrimSpace(vertex.ID)]; ok {
			return nil
		}
		if !vertexHasAllLabels(vertex, labels) {
			return nil
		}
		for key, expected := range props {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			raw, ok := vertex.Properties[key]
			if !ok {
				return nil
			}
			if scalarString(expected) != string(raw) {
				return nil
			}
		}
		matchedID = strings.TrimSpace(vertex.ID)
		return nil
	})
	if matchedID != "" {
		state.Params[cacheKey] = matchedID
	} else {
		state.Params[cacheKey] = "__none__"
	}
	return matchedID
}

func materializeWriteDeleteTombstones(row map[string]any, event WriteEvent, state *State) {
	if row == nil {
		return
	}
	kind := strings.ToUpper(strings.TrimSpace(event.Kind))
	if kind != "DELETE" && kind != "DETACH DELETE" {
		return
	}
	ids := materializedDeletedVertexIDs(row)
	if ids == nil {
		ids = map[string]struct{}{}
	}
	edgeIDs := materializedDeletedEdgeIDs(row)
	if edgeIDs == nil {
		edgeIDs = map[string]struct{}{}
	}
	deleteRoots := deleteOperandRootsForWriteEvent(event)
	if len(deleteRoots) == 0 {
		return
	}
	ctx := context.Background()
	tenant := ""
	if state != nil {
		if state.Context != nil {
			ctx = state.Context
		}
		tenant = strings.TrimSpace(state.Tenant)
	}
	for key, value := range event.Bindings {
		key = strings.TrimSpace(key)
		if key == "" || strings.Contains(key, ".") {
			continue
		}
		if _, ok := deleteRoots[key]; !ok {
			continue
		}
		if id := writePathEntityIDFromValue(value); id != "" {
			if _, isEdgeBinding := event.Bindings[key+".type"]; isEdgeBinding {
				edgeIDs[id] = struct{}{}
				continue
			}
			if state != nil && state.Tx != nil && tenant != "" {
				if found, err := state.Tx.GetEdge(ctx, tenant, id); err == nil && found != nil {
					edgeIDs[id] = struct{}{}
					continue
				}
			}
			ids[id] = struct{}{}
		}
	}
	if len(ids) == 0 && len(edgeIDs) == 0 {
		return
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	if len(out) != 0 {
		row["ve_deleted_vertex_ids"] = out
	}
	edgeOut := make([]string, 0, len(edgeIDs))
	for id := range edgeIDs {
		edgeOut = append(edgeOut, id)
	}
	sort.Strings(edgeOut)
	if len(edgeOut) != 0 {
		row["ve_deleted_edge_ids"] = edgeOut
	}
	if state == nil {
		return
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	global := materializedGlobalDeletedVertexIDs(state)
	for _, id := range out {
		global[id] = struct{}{}
	}
	merged := make([]string, 0, len(global))
	for id := range global {
		merged = append(merged, id)
	}
	sort.Strings(merged)
	state.Params["ve_deleted_vertex_ids_global"] = merged

	globalEdges := materializedGlobalDeletedEdgeIDs(state)
	for _, id := range edgeOut {
		globalEdges[id] = struct{}{}
	}
	mergedEdges := make([]string, 0, len(globalEdges))
	for id := range globalEdges {
		mergedEdges = append(mergedEdges, id)
	}
	sort.Strings(mergedEdges)
	state.Params["ve_deleted_edge_ids_global"] = mergedEdges
}

func deleteOperandRootsForWriteEvent(event WriteEvent) map[string]struct{} {
	roots := map[string]struct{}{}
	kind := strings.ToUpper(strings.TrimSpace(event.Kind))
	if kind != "DELETE" && kind != "DETACH DELETE" {
		return roots
	}
	body := strings.TrimSpace(event.Pattern)
	raw := strings.TrimSpace(event.Raw)
	if raw != "" {
		upper := strings.ToUpper(raw)
		switch {
		case strings.HasPrefix(upper, "DETACH DELETE"):
			body = strings.TrimSpace(raw[len("DETACH DELETE"):])
		case strings.HasPrefix(upper, "DETACHDELETE"):
			body = strings.TrimSpace(raw[len("DETACHDELETE"):])
		case strings.HasPrefix(upper, "DELETE"):
			body = strings.TrimSpace(raw[len("DELETE"):])
		}
	}
	for _, item := range writeSplitTopLevelComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		root := ""
		for i := 0; i < len(item); i++ {
			ch := item[i]
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				root += string(ch)
				continue
			}
			break
		}
		root = strings.TrimSpace(root)
		if root != "" {
			roots[root] = struct{}{}
		}
	}
	return roots
}

func materializedDeletedVertexIDs(bindings map[string]any) map[string]struct{} {
	out := map[string]struct{}{}
	if len(bindings) == 0 {
		return out
	}
	raw, ok := bindings["ve_deleted_vertex_ids"]
	if !ok || raw == nil {
		return out
	}
	appendID := func(value any) {
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			out[id] = struct{}{}
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, id := range typed {
			appendID(id)
		}
	case []any:
		for _, id := range typed {
			appendID(id)
		}
	default:
		appendID(typed)
	}
	return out
}

func materializedGlobalDeletedVertexIDs(state *State) map[string]struct{} {
	out := map[string]struct{}{}
	if state == nil || len(state.Params) == 0 {
		return out
	}
	raw, ok := state.Params["ve_deleted_vertex_ids_global"]
	if !ok || raw == nil {
		return out
	}
	appendID := func(value any) {
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			out[id] = struct{}{}
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, id := range typed {
			appendID(id)
		}
	case []any:
		for _, id := range typed {
			appendID(id)
		}
	default:
		appendID(typed)
	}
	return out
}

func materializedDeletedEdgeIDs(bindings map[string]any) map[string]struct{} {
	out := map[string]struct{}{}
	if len(bindings) == 0 {
		return out
	}
	raw, ok := bindings["ve_deleted_edge_ids"]
	if !ok || raw == nil {
		return out
	}
	appendID := func(value any) {
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			out[id] = struct{}{}
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, id := range typed {
			appendID(id)
		}
	case []any:
		for _, id := range typed {
			appendID(id)
		}
	default:
		appendID(typed)
	}
	return out
}

func materializedGlobalDeletedEdgeIDs(state *State) map[string]struct{} {
	out := map[string]struct{}{}
	if state == nil || len(state.Params) == 0 {
		return out
	}
	raw, ok := state.Params["ve_deleted_edge_ids_global"]
	if !ok || raw == nil {
		return out
	}
	appendID := func(value any) {
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			out[id] = struct{}{}
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, id := range typed {
			appendID(id)
		}
	case []any:
		for _, id := range typed {
			appendID(id)
		}
	default:
		appendID(typed)
	}
	return out
}

func resolveMergeSyntheticVertexIDForWriteMaterialization(event WriteEvent, state *State) string {
	if state == nil || event.Vertex == nil {
		return ""
	}
	varName := strings.TrimSpace(event.Vertex.Var)
	if varName == "" || strings.TrimSpace(event.Vertex.IDParam) != "" {
		return ""
	}
	labels := append([]string(nil), event.Vertex.Labels...)
	if len(labels) == 0 {
		return ""
	}
	props := resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)
	if len(props) != 0 {
		return ""
	}
	sort.Strings(labels)
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	cacheKey := "__ve_merge_synthetic_vertex_id|" + varName + "|" + strings.Join(labels, ":")
	if existing, ok := state.Params[cacheKey]; ok {
		if id := strings.TrimSpace(fmt.Sprint(existing)); id != "" {
			return id
		}
	}
	id := nextWriteBindingSyntheticMergeVertexID(state)
	state.Params[cacheKey] = id
	return id
}

func resolveMergePropertySyntheticVertexIDForWriteMaterialization(event WriteEvent, state *State) string {
	if state == nil || event.Vertex == nil {
		return ""
	}
	props := resolveWritePropertyBindingsState(event.Vertex.Pattern, event.ResolvedParams, event.Bindings, state)
	if len(props) == 0 {
		return ""
	}
	labels := append([]string(nil), event.Vertex.Labels...)
	for i := range labels {
		labels[i] = strings.TrimSpace(labels[i])
	}
	sort.Strings(labels)
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	cacheKey := "__ve_merge_prop_vertex_id|" + strings.Join(labels, ":") + "|" + stableRowKey(props)
	if existing, ok := state.Params[cacheKey]; ok {
		if id := strings.TrimSpace(fmt.Sprint(existing)); id != "" {
			return id
		}
	}
	id := nextWriteBindingAutoVertexID(state)
	state.Params[cacheKey] = id
	return id
}

func resolveMergeBoundEntityIDForWriteMaterialization(bindings map[string]any, varName string, allowGenericFallback bool) string {
	if len(bindings) == 0 {
		return ""
	}
	varName = strings.TrimSpace(varName)
	if varName != "" {
		hasExplicitBinding := false
		if _, ok := bindings[varName+".id"]; ok {
			hasExplicitBinding = true
		}
		if _, ok := bindings[varName]; ok {
			hasExplicitBinding = true
		}
		if id := writePathEntityIDFromValue(bindings[varName+".id"]); id != "" {
			return id
		}
		if id := writePathEntityIDFromValue(bindings[varName]); id != "" {
			return id
		}
		if hasExplicitBinding {
			return ""
		}
		if !allowGenericFallback {
			return ""
		}
	}
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".id") {
			continue
		}
		if id := scalarString(bindings[key]); id != "" {
			return id
		}
	}
	for _, key := range keys {
		if strings.Contains(key, ".") {
			continue
		}
		if id := scalarString(bindings[key]); id != "" {
			return id
		}
	}
	return ""
}

func nextWriteBindingSyntheticMergeVertexID(state *State) string {
	if state == nil {
		return "auto-mv-1"
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	const counterKey = "__ve_next_auto_merge_vertex_id"
	next := 1
	switch typed := state.Params[counterKey].(type) {
	case int:
		next = typed
	case int64:
		next = int(typed)
	case float64:
		next = int(typed)
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			next = parsed
		}
	}
	if next < 1 {
		next = 1
	}
	id := fmt.Sprintf("auto-mv-%d", next)
	state.Params[counterKey] = next + 1
	return id
}

func materializeWriteEdgeID(event WriteEvent, state *State, leftID, rightID string) string {
	if event.Edge == nil {
		return ""
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	if edgeType == "" {
		return ""
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	tenant := ""
	if state != nil {
		tenant = strings.TrimSpace(state.Tenant)
	}
	if strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE") {
		if srcID != "" && dstID != "" {
			return nextWriteBindingCreateEdgeID(state, tenant, srcID, edgeType, dstID)
		}
		seq := nextWriteBindingAutoEdgeID(state)
		return fmt.Sprintf("auto-e-%d", seq)
	}
	if srcID == "" || dstID == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
}

func materializeCreatePatternBindings(row map[string]any, event WriteEvent, state *State) {
	if row == nil || !strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE") {
		return
	}
	bindings := map[string]any{}
	for key, value := range event.Bindings {
		bindings[key] = value
	}
	for key, value := range row {
		bindings[key] = value
	}
	body := strings.TrimSpace(event.Pattern)
	if body == "" {
		body = extractWritePatternFromRaw(event.Raw, "CREATE")
	}
	if body == "" {
		return
	}
	for _, segment := range writeSplitTopLevelComma(body) {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		for _, vertexPattern := range extractCreateVertexPatternSegments(segment) {
			for key, value := range row {
				bindings[key] = value
			}
			varName := strings.TrimSpace(variableFromPatternSegment(vertexPattern))
			if varName == "" {
				continue
			}
			vertexID := resolveWriteEntityID(varName, idParamFromPatternSegment(vertexPattern), bindings, event.ResolvedParams)
			if vertexID == "" {
				if existing := scalarString(row[varName]); existing != "" {
					vertexID = existing
				}
			}
			props := resolveWritePropertyBindingsState(vertexPattern, event.ResolvedParams, bindings, state)
			if vertexID == "" {
				vertexID = nextWriteBindingAutoVertexID(state)
			}
			if vertexID != "" {
				bindWriteVariable(row, varName, vertexID)
				bindings[varName] = row[varName]
				bindings[varName+".id"] = row[varName+".id"]
			}
			if idValue, hasID := props["id"]; hasID {
				row[varName+".id"] = idValue
				bindings[varName+".id"] = idValue
			}
			for key, value := range props {
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				if strings.EqualFold(key, "id") {
					continue
				}
				row[varName+"."+key] = value
				bindings[varName+"."+key] = value
			}
		}
	}
}

func extractCreateVertexPatternSegments(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := []string{}
	inSingle := false
	inDouble := false
	depth := 0
	start := -1
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if quoteIsEscaped(raw, i) {
				continue
			}
			if inDouble && i+1 < len(raw) && raw[i+1] == '"' {
				i++
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		if ch == '(' {
			if depth == 0 {
				start = i
			}
			depth++
			continue
		}
		if ch == ')' {
			if depth == 0 {
				continue
			}
			depth--
			if depth == 0 && start >= 0 {
				out = append(out, strings.TrimSpace(raw[start:i+1]))
				start = -1
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func nextWriteBindingAutoVertexID(state *State) string {
	if state == nil {
		return "auto-v-1"
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	const counterKey = "__ve_next_auto_write_binding_id"
	const tagKey = "__ve_write_binding_session_tag"
	next := 1
	switch typed := state.Params[counterKey].(type) {
	case int:
		next = typed
	case int64:
		next = int(typed)
	case float64:
		next = int(typed)
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			next = parsed
		}
	}
	if next < 1 {
		next = 1
	}
	tag := ""
	if rawTag, ok := state.Params[tagKey]; ok && rawTag != nil {
		tag = strings.TrimSpace(fmt.Sprint(rawTag))
		if strings.EqualFold(tag, "<nil>") {
			tag = ""
		}
	}
	if tag == "" {
		tag = fmt.Sprintf("%d", time.Now().UnixNano())
		state.Params[tagKey] = tag
	}
	id := fmt.Sprintf("__ve_write_v_%s_%d", tag, next)
	state.Params[counterKey] = next + 1
	return id
}

func nextWriteBindingAutoEdgeID(state *State) int {
	if state == nil {
		return 1
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	const counterKey = "__ve_next_auto_write_binding_edge_id"
	next := 1
	switch typed := state.Params[counterKey].(type) {
	case int:
		next = typed
	case int64:
		next = int(typed)
	case float64:
		next = int(typed)
	case string:
		if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
			next = parsed
		}
	}
	if next < 1 {
		next = 1
	}
	state.Params[counterKey] = next + 1
	return next
}

func nextWriteBindingCreateEdgeID(state *State, tenant, srcID, edgeType, dstID string) string {
	base := fmt.Sprintf("%s|%s|%s|%s", strings.TrimSpace(tenant), srcID, edgeType, dstID)
	if state == nil {
		return base
	}
	if state.Params == nil {
		state.Params = map[string]any{}
	}
	const counterKey = "__ve_create_edge_binding_counts"
	counts, _ := state.Params[counterKey].(map[string]int)
	if counts == nil {
		counts = map[string]int{}
	}
	counts[base] = counts[base] + 1
	state.Params[counterKey] = counts
	if counts[base] <= 1 {
		return base
	}
	return fmt.Sprintf("%s|auto-e-%d", base, counts[base])
}

func materializeWriteSetBindings(row map[string]any, event WriteEvent, state *State) {
	if row == nil || !strings.EqualFold(strings.TrimSpace(event.Kind), "SET") {
		return
	}
	body := strings.TrimSpace(event.Raw)
	if body == "" {
		return
	}
	extracted := extractWritePatternFromRaw(body, "SET")
	if strings.TrimSpace(extracted) != "" {
		body = extracted
	}
	if body == "" {
		return
	}
	materializeWriteSetBodyBindings(row, body, state)
}

func materializeWriteSetBodyBindings(row map[string]any, body string, state *State) {
	if row == nil {
		return
	}
	for _, item := range writeSplitTopLevelComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if match := writeSetPropertyAssignmentRE.FindStringSubmatch(item); len(match) == 4 {
			varName := strings.TrimSpace(match[1])
			field := strings.TrimSpace(match[2])
			expr := strings.TrimSpace(match[3])
			if varName == "" || field == "" {
				continue
			}
			value, ok := resolveWriteSetBindingValue(expr, row, state)
			if !ok {
				continue
			}
			applyWriteSetBindingProperty(row, varName, field, value, state)
			continue
		}
		if varName, op, expr, ok := parseWriteSetMapAssignment(item); ok {
			value, valueOK := resolveWriteSetBindingValue(expr, row, state)
			if !valueOK {
				continue
			}
			props, propsOK := writeSetBindingMapValue(value)
			if !propsOK {
				continue
			}
			if op == "+=" {
				applyWriteSetBindingMapAppend(row, varName, props, state)
			} else {
				applyWriteSetBindingMapReplace(row, varName, props, state)
			}
			continue
		}
		match := writeSetLabelAssignmentRE.FindStringSubmatch(item)
		if len(match) != 3 {
			continue
		}
		varName := strings.TrimSpace(match[1])
		if varName == "" {
			continue
		}
		labels := parseWriteSetLabels(match[2])
		if len(labels) == 0 {
			continue
		}
		mergeWriteSetRowLabels(row, varName, labels, state)
	}
}

func materializeWriteRemoveBindings(row map[string]any, event WriteEvent, state *State) {
	if row == nil || !strings.EqualFold(strings.TrimSpace(event.Kind), "REMOVE") {
		return
	}
	body := strings.TrimSpace(event.Raw)
	if body == "" {
		return
	}
	extracted := extractWritePatternFromRaw(body, "REMOVE")
	if strings.TrimSpace(extracted) != "" {
		body = extracted
	}
	if body == "" {
		return
	}
	for _, item := range writeSplitTopLevelComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if match := writeRemovePropertyAssignmentRE.FindStringSubmatch(item); len(match) == 3 {
			applyWriteSetBindingProperty(row, strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), nil, state)
			continue
		}
		if match := writeSetLabelAssignmentRE.FindStringSubmatch(item); len(match) == 3 {
			removeWriteBindingLabels(row, strings.TrimSpace(match[1]), parseWriteSetLabels(match[2]), state)
		}
	}
}

func resolveWriteSetBindingValue(expr string, row map[string]any, state *State) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if value, ok, err := resolveProjectionExprValueChecked(expr, row, state); err == nil && ok {
		return value, true
	}
	if strings.HasPrefix(expr, "$") {
		if state == nil || state.Params == nil {
			return nil, false
		}
		key := strings.TrimSpace(expr[1:])
		if key == "" {
			return nil, false
		}
		value, ok := state.Params[key]
		return value, ok
	}
	if strings.EqualFold(expr, "null") {
		return nil, true
	}
	if strings.EqualFold(expr, "true") {
		return true, true
	}
	if strings.EqualFold(expr, "false") {
		return false, true
	}
	if s, ok := parseProjectionStringLiteral(expr); ok {
		return s, true
	}
	if i, err := strconv.ParseInt(expr, 10, 64); err == nil {
		if int64(int(i)) == i {
			return int(i), true
		}
		return i, true
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, true
	}
	if row != nil {
		if value, ok := row[expr]; ok {
			return value, true
		}
	}
	return expr, true
}

func applyWriteSetBindingProperty(row map[string]any, varName, field string, value any, state *State) {
	if row == nil {
		return
	}
	propKey := varName + "." + field
	row[propKey] = value
	entity, ok := row[varName]
	if ok {
		switch typed := entity.(type) {
		case string:
			if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
				if entityID := strings.TrimSpace(typed); entityID != "" {
					if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
						entity = vertex
						row[varName] = entity
						ok = true
					} else if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
						entity = edge
						row[varName] = entity
						ok = true
					}
				}
			}
		}
	}
	if !ok {
		if state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
			if boundID, exists := row[varName+".id"]; exists {
				if entityID := scalarString(boundID); entityID != "" {
					if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
						entity = vertex
						row[varName] = entity
						ok = true
					} else if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
						entity = edge
						row[varName] = entity
						ok = true
					}
				}
			}
		}
	}
	if !ok {
		return
	}
	applyWriteSetBindingPropertyToEntity(entity, field, value)
}

func parseWriteSetMapAssignment(item string) (string, string, string, bool) {
	if match := writeSetMapAppendAssignmentRE.FindStringSubmatch(strings.TrimSpace(item)); len(match) == 3 {
		return strings.TrimSpace(match[1]), "+=", strings.TrimSpace(match[2]), true
	}
	if match := writeSetMapReplaceAssignmentRE.FindStringSubmatch(strings.TrimSpace(item)); len(match) == 3 {
		return strings.TrimSpace(match[1]), "=", strings.TrimSpace(match[2]), true
	}
	return "", "", "", false
}

func writeSetBindingMapValue(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(key)] = item
		}
		return out, true
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[strings.TrimSpace(key)] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func applyWriteSetBindingMapAppend(row map[string]any, varName string, props map[string]any, state *State) {
	if row == nil || strings.TrimSpace(varName) == "" {
		return
	}
	for field, value := range props {
		field = strings.TrimSpace(field)
		if field == "" || writeSetReservedBindingField(field) {
			continue
		}
		applyWriteSetBindingProperty(row, varName, field, value, state)
	}
}

func applyWriteSetBindingMapReplace(row map[string]any, varName string, props map[string]any, state *State) {
	if row == nil || strings.TrimSpace(varName) == "" {
		return
	}
	existing := writeSetBindingEntityPropertyKeys(row, varName, state)
	for _, field := range existing {
		if _, exists := props[field]; exists {
			continue
		}
		applyWriteSetBindingProperty(row, varName, field, nil, state)
	}
	applyWriteSetBindingMapAppend(row, varName, props, state)
}

func writeSetBindingEntityPropertyKeys(row map[string]any, varName string, state *State) []string {
	keys := map[string]struct{}{}
	prefix := strings.TrimSpace(varName) + "."
	for key := range row {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		field := strings.TrimSpace(strings.TrimPrefix(key, prefix))
		if field == "" || writeSetReservedBindingField(field) {
			continue
		}
		keys[field] = struct{}{}
	}
	if row != nil {
		var entityID string
		if entity, ok := row[strings.TrimSpace(varName)]; ok {
			for _, field := range writeSetEntityPropertyKeys(entity) {
				if field != "" {
					keys[field] = struct{}{}
				}
			}
			switch typed := entity.(type) {
			case *graph.Vertex:
				if typed != nil {
					entityID = strings.TrimSpace(typed.ID)
				}
			case *graph.Edge:
				if typed != nil {
					entityID = strings.TrimSpace(typed.ID)
				}
			case map[string]any:
				if rawID, ok := typed["id"]; ok {
					entityID = strings.TrimSpace(scalarString(rawID))
				}
			default:
				entityID = strings.TrimSpace(scalarString(entity))
			}
			if entityID == "" {
				entityID = strings.TrimSpace(scalarString(entity))
			}
		}
		if entityID == "" {
			if boundID, ok := row[strings.TrimSpace(varName)+".id"]; ok {
				entityID = strings.TrimSpace(scalarString(boundID))
			}
		}
		if entityID != "" && state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
			if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
				for key := range vertex.Properties {
					keys[strings.TrimSpace(key)] = struct{}{}
				}
			} else if edge, err := state.Tx.GetEdge(context.Background(), state.Tenant, entityID); err == nil && edge != nil {
				for key := range edge.Properties {
					keys[strings.TrimSpace(key)] = struct{}{}
				}
			}
		}
	}
	out := make([]string, 0, len(keys))
	for key := range keys {
		if key == "" || writeSetReservedBindingField(key) {
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func writeSetEntityPropertyKeys(entity any) []string {
	keys := []string{}
	switch typed := entity.(type) {
	case *graph.Vertex:
		if typed != nil {
			for key := range typed.Properties {
				keys = append(keys, strings.TrimSpace(key))
			}
		}
	case *graph.Edge:
		if typed != nil {
			for key := range typed.Properties {
				keys = append(keys, strings.TrimSpace(key))
			}
		}
	case map[string]any:
		if props, ok := typed["properties"].(map[string]any); ok {
			for key := range props {
				keys = append(keys, strings.TrimSpace(key))
			}
		}
	}
	return keys
}

func writeSetReservedBindingField(field string) bool {
	field = strings.TrimSpace(field)
	return field == "id" || field == "labels" || field == "type" || field == "src" || field == "dst"
}

func applyWriteSetBindingPropertyToEntity(entity any, field string, value any) {
	switch typed := entity.(type) {
	case *graph.Vertex:
		if typed == nil {
			return
		}
		if typed.Properties == nil {
			typed.Properties = graph.PropertyMap{}
		}
		if value == nil {
			delete(typed.Properties, field)
			return
		}
		typed.Properties[field] = writeEncodeBindingPropertyValue(value)
	case *graph.Edge:
		if typed == nil {
			return
		}
		if typed.Properties == nil {
			typed.Properties = graph.PropertyMap{}
		}
		if value == nil {
			delete(typed.Properties, field)
			return
		}
		typed.Properties[field] = writeEncodeBindingPropertyValue(value)
	case map[string]any:
		if props, ok := typed["properties"].(map[string]any); ok {
			if value == nil {
				delete(props, field)
			} else {
				props[field] = value
			}
			typed["properties"] = props
			return
		}
		if value == nil {
			delete(typed, field)
			return
		}
		typed[field] = value
	}
}

func writeEncodeBindingPropertyValue(value any) []byte {
	switch typed := value.(type) {
	case nil:
		return []byte("null")
	case []byte:
		return append([]byte(nil), typed...)
	case string:
		return []byte(typed)
	case bool:
		if typed {
			return []byte("true")
		}
		return []byte("false")
	case int:
		return []byte(strconv.Itoa(typed))
	case int64:
		return []byte(strconv.FormatInt(typed, 10))
	case int32:
		return []byte(strconv.FormatInt(int64(typed), 10))
	case float64:
		return []byte(strconv.FormatFloat(typed, 'f', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	default:
		return []byte(fmt.Sprint(value))
	}
}

func parseWriteSetLabels(raw string) []string {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mergeWriteSetRowLabels(row map[string]any, varName string, labels []string, state *State) {
	key := strings.TrimSpace(varName) + ".labels"
	if key == ".labels" {
		return
	}
	merged := []string{}
	seen := map[string]struct{}{}
	if existing, ok := row[key]; ok {
		switch typed := existing.(type) {
		case []string:
			for _, item := range typed {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				if _, ok := seen[item]; ok {
					continue
				}
				seen[item] = struct{}{}
				merged = append(merged, item)
			}
		case []any:
			for _, item := range typed {
				label := strings.TrimSpace(fmt.Sprint(item))
				if label == "" {
					continue
				}
				if _, ok := seen[label]; ok {
					continue
				}
				seen[label] = struct{}{}
				merged = append(merged, label)
			}
		}
	}
	if len(merged) == 0 {
		switch entity := row[strings.TrimSpace(varName)].(type) {
		case *graph.Vertex:
			if entity != nil {
				for _, item := range entity.Labels {
					item = strings.TrimSpace(item)
					if item == "" {
						continue
					}
					if _, ok := seen[item]; ok {
						continue
					}
					seen[item] = struct{}{}
					merged = append(merged, item)
				}
			}
		case map[string]any:
			if raw, ok := entity["labels"]; ok {
				switch typed := raw.(type) {
				case []string:
					for _, item := range typed {
						item = strings.TrimSpace(item)
						if item == "" {
							continue
						}
						if _, ok := seen[item]; ok {
							continue
						}
						seen[item] = struct{}{}
						merged = append(merged, item)
					}
				case []any:
					for _, item := range typed {
						label := strings.TrimSpace(fmt.Sprint(item))
						if label == "" {
							continue
						}
						if _, ok := seen[label]; ok {
							continue
						}
						seen[label] = struct{}{}
						merged = append(merged, label)
					}
				}
			}
		}
		if len(merged) == 0 && state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
			entityID := ""
			switch entity := row[strings.TrimSpace(varName)].(type) {
			case string:
				entityID = strings.TrimSpace(entity)
			case *graph.Vertex:
				if entity != nil {
					entityID = strings.TrimSpace(entity.ID)
				}
			case *graph.Edge:
				if entity != nil {
					entityID = strings.TrimSpace(entity.ID)
				}
			}
			if entityID == "" {
				if boundID, ok := row[strings.TrimSpace(varName)+".id"]; ok {
					entityID = strings.TrimSpace(scalarString(boundID))
				}
			}
			if entityID != "" {
				if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
					for _, item := range vertex.Labels {
						item = strings.TrimSpace(item)
						if item == "" {
							continue
						}
						if _, ok := seen[item]; ok {
							continue
						}
						seen[item] = struct{}{}
						merged = append(merged, item)
					}
					row[varName] = vertex
				}
			}
		}
	}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		merged = append(merged, label)
	}
	switch entity := row[strings.TrimSpace(varName)].(type) {
	case *graph.Vertex:
		if entity != nil {
			entity.Labels = append([]string(nil), merged...)
		}
	case map[string]any:
		entity["labels"] = append([]string(nil), merged...)
	}
	if len(merged) > 0 {
		row[key] = merged
	}
}

func removeWriteBindingLabels(row map[string]any, varName string, labels []string, state *State) {
	if row == nil || strings.TrimSpace(varName) == "" || len(labels) == 0 {
		return
	}
	removeSet := map[string]struct{}{}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		removeSet[label] = struct{}{}
	}
	if len(removeSet) == 0 {
		return
	}
	existing := writePathRowLabels(row, varName)
	if len(existing) == 0 && state != nil && state.Tx != nil && strings.TrimSpace(state.Tenant) != "" {
		if boundID, ok := row[varName+".id"]; ok {
			if entityID := scalarString(boundID); entityID != "" {
				if vertex, err := state.Tx.GetVertex(context.Background(), state.Tenant, entityID); err == nil && vertex != nil {
					existing = append([]string(nil), vertex.Labels...)
				}
			}
		}
	}
	filtered := make([]string, 0, len(existing))
	for _, label := range existing {
		if _, remove := removeSet[strings.TrimSpace(label)]; remove {
			continue
		}
		filtered = append(filtered, label)
	}
	row[varName+".labels"] = filtered
	if entity, ok := row[varName]; ok {
		switch typed := entity.(type) {
		case *graph.Vertex:
			if typed != nil {
				typed.Labels = append([]string(nil), filtered...)
			}
		case map[string]any:
			typed["labels"] = append([]string(nil), filtered...)
		}
	}
}

func bindWriteVariable(row map[string]any, varName, entityID string) {
	varName = strings.TrimSpace(varName)
	entityID = strings.TrimSpace(entityID)
	if varName == "" || entityID == "" {
		return
	}
	row[varName] = entityID
	idKey := varName + ".id"
	if existing, ok := row[idKey]; ok {
		if strings.TrimSpace(scalarString(existing)) != "" {
			return
		}
	}
	row[idKey] = entityID
}

func resolveWriteEntityID(varName, idParam string, bindings map[string]any, resolvedParams map[string]any) string {
	varName = strings.TrimSpace(varName)
	if varName != "" {
		if value, ok := bindings[varName]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
		if value, ok := bindings[varName+".id"]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
	}
	idParam = strings.TrimSpace(idParam)
	if idParam != "" {
		if value, ok := resolvedParams[idParam]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
	}
	return ""
}

func resolveWritePropertyBindings(pattern string, resolvedParams map[string]any, bindings map[string]any) map[string]any {
	return resolveWritePropertyBindingsState(pattern, resolvedParams, bindings, nil)
}

func resolveWritePropertyBindingsState(pattern string, resolvedParams map[string]any, bindings map[string]any, state *State) map[string]any {
	body, ok := writePatternPropertyMapBody(pattern)
	if !ok || strings.TrimSpace(body) == "" {
		return nil
	}
	out := map[string]any{}
	for _, pair := range writeSplitTopLevelComma(body) {
		key, rawValue, ok := writeSplitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		rawValue = strings.TrimSpace(rawValue)
		var value any
		var resolved bool
		// When state is available, use the full projection expression evaluator
		// which can fetch vertex/edge properties from the store (needed for
		// expressions like  d.name + '0'  where d is bound as a vertex ID).
		if state != nil {
			value, resolved = resolveProjectionExprValue(rawValue, bindings, state)
			if resolved && value == nil && !strings.EqualFold(strings.TrimSpace(rawValue), "null") {
				resolved = false
			}
		}
		if !resolved {
			value, resolved = resolveWriteBindingValue(rawValue, resolvedParams, bindings)
		}
		if !resolved {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writePatternPropertyMapBody(pattern string) (string, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", false
	}
	open := strings.Index(pattern, "{")
	if open < 0 {
		return "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := open; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(pattern) && pattern[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && pattern[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return pattern[open+1 : i], true
			}
		}
	}
	return "", false
}

func writeSplitTopLevelComma(raw string) []string {
	raw = strings.TrimSpace(writeStripLineComments(raw))
	if raw == "" {
		return nil
	}
	parts := []string{}
	start := 0
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ',':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	return parts
}

func writeStripLineComments(raw string) string {
	if !strings.Contains(raw, "//") {
		return raw
	}
	var b strings.Builder
	b.Grow(len(raw))
	inSingle := false
	inDouble := false
	inLineComment := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inLineComment {
			if ch == '\n' || ch == '\r' {
				inLineComment = false
				b.WriteByte(ch)
			}
			continue
		}
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				b.WriteByte(ch)
				i++
				b.WriteByte(raw[i])
				continue
			}
			inSingle = !inSingle
			b.WriteByte(ch)
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				b.WriteByte(ch)
				continue
			}
			inDouble = !inDouble
			b.WriteByte(ch)
			continue
		}
		if !inSingle && !inDouble && ch == '/' && i+1 < len(raw) && raw[i+1] == '/' {
			inLineComment = true
			i++
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func writeSplitTopLevelKeyValue(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ':':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return strings.TrimSpace(raw[:i]), strings.TrimSpace(raw[i+1:]), true
			}
		}
	}
	return "", "", false
}

func resolveWriteBindingValue(raw string, resolvedParams map[string]any, bindings map[string]any) (any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimSpace(raw[1:])
		if name == "" || resolvedParams == nil {
			return nil, false
		}
		value, ok := resolvedParams[name]
		return value, ok
	}
	// Try binary operator expressions (e.g. d.name + '0') before path lookup so
	// that the '.' inside a property-path segment doesn't interfere with the split.
	if v, ok := resolveWriteBinaryExpression(raw, resolvedParams, bindings); ok {
		return v, true
	}
	if bindings != nil && strings.Contains(raw, ".") {
		if value, ok := resolveWriteBindingPathValue(raw, bindings); ok {
			return value, true
		}
	}
	if bindings != nil && strings.Contains(raw, ".") {
		if value, ok := resolveProjectionPathValue(raw, bindings, nil); ok {
			return value, true
		}
	}
	if bindings != nil && isIdentifierToken(raw) {
		if value, ok := bindings[raw]; ok {
			return value, true
		}
	}
	if strings.EqualFold(raw, "null") {
		return nil, true
	}
	if strings.EqualFold(raw, "true") {
		return true, true
	}
	if strings.EqualFold(raw, "false") {
		return false, true
	}
	if s, ok := parseProjectionStringLiteral(raw); ok {
		return s, true
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n, true
	}
	if n, err := strconv.ParseFloat(raw, 64); err == nil {
		return n, true
	}
	return raw, true
}

// resolveWriteBinaryExpression evaluates top-level binary operator expressions
// such as `var.prop + 'suffix'` or `x + y` as encountered in CREATE/SET property maps.
func resolveWriteBinaryExpression(raw string, resolvedParams map[string]any, bindings map[string]any) (any, bool) {
	type opPos struct {
		op  byte
		pos int
	}
	// Scan for +, -, *, / at depth 0 (outside strings/brackets/parens).
	// Collect all candidate positions, then split at the last one (left-to-right assoc).
	var candidates []opPos
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case '+', '-', '*', '/':
			if depth == 0 && i > 0 {
				candidates = append(candidates, opPos{ch, i})
			}
		}
	}
	if len(candidates) == 0 {
		return nil, false
	}
	// Use the last candidate for left-to-right associativity.
	best := candidates[len(candidates)-1]
	leftRaw := strings.TrimSpace(raw[:best.pos])
	rightRaw := strings.TrimSpace(raw[best.pos+1:])
	if leftRaw == "" || rightRaw == "" {
		return nil, false
	}
	leftVal, leftOk := resolveWriteBindingValue(leftRaw, resolvedParams, bindings)
	rightVal, rightOk := resolveWriteBindingValue(rightRaw, resolvedParams, bindings)
	if !leftOk || !rightOk {
		return nil, false
	}
	switch best.op {
	case '+':
		// Numeric add if both are numeric, otherwise string concat.
		lf, lIsFloat := toFloat64(leftVal)
		rf, rIsFloat := toFloat64(rightVal)
		if lIsFloat && rIsFloat {
			result := lf + rf
			if result == float64(int64(result)) {
				return int64(result), true
			}
			return result, true
		}
		return fmt.Sprintf("%v%v", leftVal, rightVal), true
	case '-':
		lf, lOk := toFloat64(leftVal)
		rf, rOk := toFloat64(rightVal)
		if lOk && rOk {
			result := lf - rf
			if result == float64(int64(result)) {
				return int64(result), true
			}
			return result, true
		}
	case '*':
		lf, lOk := toFloat64(leftVal)
		rf, rOk := toFloat64(rightVal)
		if lOk && rOk {
			result := lf * rf
			if result == float64(int64(result)) {
				return int64(result), true
			}
			return result, true
		}
	case '/':
		lf, lOk := toFloat64(leftVal)
		rf, rOk := toFloat64(rightVal)
		if lOk && rOk && rf != 0 {
			result := lf / rf
			if result == float64(int64(result)) {
				return int64(result), true
			}
			return result, true
		}
	}
	return nil, false
}

func toFloat64(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

func resolveWriteBindingPathValue(expr string, bindings map[string]any) (any, bool) {
	parts := strings.Split(strings.TrimSpace(expr), ".")
	if len(parts) < 2 {
		return nil, false
	}
	base := strings.TrimSpace(parts[0])
	if base == "" {
		return nil, false
	}
	current, ok := bindings[base]
	if !ok {
		return nil, false
	}
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		next, ok := resolveWriteBindingPathSegment(current, part)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func resolveWriteBindingPathSegment(current any, part string) (any, bool) {
	if current == nil {
		return nil, false
	}
	switch typed := current.(type) {
	case *graph.Vertex:
		if typed == nil {
			return nil, false
		}
		if value, exists := typed.Properties[part]; exists {
			return decodeProjectionPropertyValue(value), true
		}
		return nil, false
	case *graph.Edge:
		if typed == nil {
			return nil, false
		}
		if value, exists := typed.Properties[part]; exists {
			return decodeProjectionPropertyValue(value), true
		}
		return nil, false
	case map[string]any:
		value, ok := typed[part]
		return value, ok
	case map[string]string:
		value, ok := typed[part]
		if !ok {
			return nil, false
		}
		return value, true
	}

	rv := reflect.ValueOf(current)
	if !rv.IsValid() {
		return nil, false
	}
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}

	if rv.Kind() == reflect.Map {
		iter := rv.MapRange()
		for iter.Next() {
			key := strings.TrimSpace(fmt.Sprint(iter.Key().Interface()))
			if strings.EqualFold(key, part) {
				return iter.Value().Interface(), true
			}
		}
		return nil, false
	}

	if rv.Kind() == reflect.Struct {
		typ := rv.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(field.Name), part) {
				return rv.Field(i).Interface(), true
			}
		}
	}

	return nil, false
}

func scalarString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		return strings.TrimSpace(typed.ID)
	case graph.Vertex:
		return strings.TrimSpace(typed.ID)
	case *graph.Edge:
		return strings.TrimSpace(typed.ID)
	case graph.Edge:
		return strings.TrimSpace(typed.ID)
	case map[string]any:
		if id := strings.TrimSpace(fmt.Sprint(typed["id"])); id != "" && id != "<nil>" {
			return id
		}
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return ""
	}
	return text
}

func boolAttr(attrs map[string]any, key string) bool {
	if attrs == nil {
		return false
	}
	v, ok := attrs[key]
	if !ok || v == nil {
		return false
	}
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func stableRowKey(row map[string]any) string {
	if row == nil {
		return "{}"
	}
	var b strings.Builder
	b.Grow(len(row) * 24)
	b.WriteString("rk:{")
	appendStableRowKeyMap(&b, row)
	b.WriteByte('}')
	return b.String()
}

func projectionDistinctRowKey(row map[string]any) string {
	if typedKey, ok := projectionTypedScalarRowKey(row); ok {
		return typedKey
	}
	return stableRowKey(row)
}

func projectionDistinctRowKeyObserved(state *State, row map[string]any, prefix string) string {
	if typedKey, ok := projectionTypedScalarRowKey(row); ok {
		observeRuntimeCounter(state, prefix+".typed", 1)
		return typedKey
	}
	observeRuntimeCounter(state, prefix+".fallback", 1)
	return stableRowKey(row)
}

func projectionTypedScalarRowKey(row map[string]any) (string, bool) {
	if row == nil {
		return "{}", true
	}
	var small [8]string
	keys := small[:0]
	if len(row) > len(small) {
		keys = make([]string, 0, len(row))
	}
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.Grow(len(keys) * 16)
	b.WriteString("ts:")
	for _, key := range keys {
		if !appendProjectionTypedScalarDistinctKey(&b, row[key]) {
			return "", false
		}
		writeBuilderInt(&b, len(key))
		b.WriteByte(':')
		b.WriteString(key)
		b.WriteByte(';')
	}
	return b.String(), true
}

func projectionDistinctValueKey(value any) string {
	if key, ok := projectionTypedScalarDistinctKey(value); ok {
		return key
	}
	var b strings.Builder
	b.Grow(24)
	b.Reset()
	b.WriteString("v:")
	appendStableRowKeyValue(&b, value)
	return b.String()
}

func projectionTypedScalarDistinctKey(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return "s:" + typed, true
	case bool:
		if typed {
			return "b:1", true
		}
		return "b:0", true
	case int:
		return "i:" + strconv.FormatInt(int64(typed), 10), true
	case int8:
		return "i:" + strconv.FormatInt(int64(typed), 10), true
	case int16:
		return "i:" + strconv.FormatInt(int64(typed), 10), true
	case int32:
		return "i:" + strconv.FormatInt(int64(typed), 10), true
	case int64:
		return "i:" + strconv.FormatInt(typed, 10), true
	case uint:
		return "u:" + strconv.FormatUint(uint64(typed), 10), true
	case uint8:
		return "u:" + strconv.FormatUint(uint64(typed), 10), true
	case uint16:
		return "u:" + strconv.FormatUint(uint64(typed), 10), true
	case uint32:
		return "u:" + strconv.FormatUint(uint64(typed), 10), true
	case uint64:
		return "u:" + strconv.FormatUint(typed, 10), true
	case float32:
		return "f:" + strconv.FormatFloat(float64(typed), 'g', -1, 32), true
	case float64:
		return "f:" + strconv.FormatFloat(typed, 'g', -1, 64), true
	case json.Number:
		return "n:" + typed.String(), true
	default:
		return "", false
	}
}

func appendProjectionTypedScalarDistinctKey(b *strings.Builder, value any) bool {
	switch typed := value.(type) {
	case string:
		b.WriteString("s:")
		b.WriteString(typed)
		return true
	case bool:
		if typed {
			b.WriteString("b:1")
		} else {
			b.WriteString("b:0")
		}
		return true
	case int:
		b.WriteString("i:")
		writeBuilderInt64(b, int64(typed))
		return true
	case int8:
		b.WriteString("i:")
		writeBuilderInt64(b, int64(typed))
		return true
	case int16:
		b.WriteString("i:")
		writeBuilderInt64(b, int64(typed))
		return true
	case int32:
		b.WriteString("i:")
		writeBuilderInt64(b, int64(typed))
		return true
	case int64:
		b.WriteString("i:")
		writeBuilderInt64(b, typed)
		return true
	case uint:
		b.WriteString("u:")
		writeBuilderUint64(b, uint64(typed))
		return true
	case uint8:
		b.WriteString("u:")
		writeBuilderUint64(b, uint64(typed))
		return true
	case uint16:
		b.WriteString("u:")
		writeBuilderUint64(b, uint64(typed))
		return true
	case uint32:
		b.WriteString("u:")
		writeBuilderUint64(b, uint64(typed))
		return true
	case uint64:
		b.WriteString("u:")
		writeBuilderUint64(b, typed)
		return true
	case float32:
		b.WriteString("f:")
		writeBuilderFloat64(b, float64(typed), 32)
		return true
	case float64:
		b.WriteString("f:")
		writeBuilderFloat64(b, typed, 64)
		return true
	case json.Number:
		b.WriteString("n:")
		b.WriteString(typed.String())
		return true
	default:
		return false
	}
}

func appendStableRowKeyMap(b *strings.Builder, value map[string]any) {
	var small [8]string
	keys := small[:0]
	if len(value) > len(small) {
		keys = make([]string, 0, len(value))
	}
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeBuilderInt(b, len(key))
		b.WriteByte(':')
		b.WriteString(key)
		b.WriteByte('=')
		appendStableRowKeyValue(b, value[key])
		b.WriteByte(';')
	}
}

func appendStableRowKeyValue(b *strings.Builder, value any) {
	if appendProjectionTypedScalarDistinctKey(b, value) {
		return
	}
	switch typed := value.(type) {
	case nil:
		b.WriteString("n")
	case projectionPathValue:
		b.WriteString("p{")
		b.WriteString("nodes=[")
		for _, node := range typed.nodes {
			id := ""
			if resolved, ok := projectionPathNodeID(node); ok {
				id = resolved
			} else {
				id = strings.TrimSpace(fmt.Sprint(node))
			}
			appendStableRowKeyValue(b, id)
			b.WriteByte(',')
		}
		b.WriteString("];rels=[")
		for _, rel := range typed.relationships {
			id := projectionRelationshipID(rel)
			if id == "" {
				id = strings.TrimSpace(fmt.Sprint(rel))
			}
			appendStableRowKeyValue(b, id)
			b.WriteByte(',')
		}
		b.WriteString("]}")
	case []any:
		b.WriteByte('[')
		for _, item := range typed {
			appendStableRowKeyValue(b, item)
			b.WriteByte(',')
		}
		b.WriteByte(']')
	case map[string]any:
		b.WriteByte('{')
		appendStableRowKeyMap(b, typed)
		b.WriteByte('}')
	case map[string]string:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			converted[key] = item
		}
		b.WriteByte('{')
		appendStableRowKeyMap(b, converted)
		b.WriteByte('}')
	case *graph.Vertex:
		if typed == nil {
			b.WriteString("nv")
			return
		}
		b.WriteString("vertex:")
		b.WriteString(strings.TrimSpace(typed.ID))
	case graph.Vertex:
		b.WriteString("vertex:")
		b.WriteString(strings.TrimSpace(typed.ID))
	case *graph.Edge:
		if typed == nil {
			b.WriteString("ne")
			return
		}
		b.WriteString("edge:")
		b.WriteString(strings.TrimSpace(typed.ID))
	case graph.Edge:
		b.WriteString("edge:")
		b.WriteString(strings.TrimSpace(typed.ID))
	default:
		text := strings.TrimSpace(fmt.Sprint(stableRowKeyValue(typed)))
		writeBuilderInt(b, len(text))
		b.WriteByte(':')
		b.WriteString(text)
	}
}

func writeBuilderInt(b *strings.Builder, value int) {
	writeBuilderInt64(b, int64(value))
}

func writeBuilderInt64(b *strings.Builder, value int64) {
	var digits [32]byte
	b.Write(strconv.AppendInt(digits[:0], value, 10))
}

func writeBuilderUint64(b *strings.Builder, value uint64) {
	var digits [32]byte
	b.Write(strconv.AppendUint(digits[:0], value, 10))
}

func writeBuilderFloat64(b *strings.Builder, value float64, bitSize int) {
	var digits [64]byte
	b.Write(strconv.AppendFloat(digits[:0], value, 'g', -1, bitSize))
}

func stableRowKeyValue(value any) any {
	switch typed := value.(type) {
	case projectionPathValue:
		nodes := make([]string, 0, len(typed.nodes))
		for _, node := range typed.nodes {
			if id, ok := projectionPathNodeID(node); ok {
				nodes = append(nodes, id)
				continue
			}
			nodes = append(nodes, strings.TrimSpace(fmt.Sprint(node)))
		}
		rels := make([]string, 0, len(typed.relationships))
		for _, rel := range typed.relationships {
			if id := projectionRelationshipID(rel); id != "" {
				rels = append(rels, id)
				continue
			}
			rels = append(rels, strings.TrimSpace(fmt.Sprint(rel)))
		}
		return map[string]any{"nodes": nodes, "relationships": rels}
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, stableRowKeyValue(item))
		}
		return out
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		pairs := make([][2]any, 0, len(keys))
		for _, key := range keys {
			pairs = append(pairs, [2]any{key, stableRowKeyValue(typed[key])})
		}
		return pairs
	default:
		return value
	}
}

func classifyMutationType(kind, pattern, mergePattern, raw string) MutationType {
	if strings.EqualFold(strings.TrimSpace(kind), "SET") || strings.EqualFold(strings.TrimSpace(kind), "REMOVE") {
		return MutationTypeProperty
	}
	text := strings.ToUpper(strings.TrimSpace(pattern))
	if text == "" {
		text = strings.ToUpper(strings.TrimSpace(mergePattern))
	}
	if text == "" {
		text = strings.ToUpper(strings.TrimSpace(raw))
	}
	if strings.Contains(text, "-[") && (strings.Contains(text, "]->") || strings.Contains(text, "<-[") || strings.Contains(text, "]-")) {
		return MutationTypeEdge
	}
	if strings.Contains(text, "(") && strings.Contains(text, ")") {
		return MutationTypeVertex
	}
	return MutationTypeUnknown
}

func inferWriteKindFromRaw(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "DETACH DELETE") || strings.Contains(upper, "\nDETACH DELETE ") || strings.Contains(upper, " DETACH DELETE ") {
		return "DETACH DELETE"
	}
	kinds := []string{"CREATE", "MERGE", "SET", "REMOVE", "DELETE"}
	for _, kind := range kinds {
		if strings.HasPrefix(upper, kind) || strings.Contains(upper, "\n"+kind+" ") || strings.Contains(upper, " "+kind+" ") {
			return kind
		}
	}
	return ""
}

func buildMutationPayloads(mutationType MutationType, kind, clausePattern, mergePattern, raw string) (*VertexMutation, *EdgeMutation) {
	pattern := strings.TrimSpace(clausePattern)
	if pattern == "" {
		pattern = strings.TrimSpace(mergePattern)
	}
	if pattern == "" {
		pattern = extractWritePatternFromRaw(raw, kind)
	}
	if _, assignedPattern, hasPathAssignment := splitProjectionPathAssignment(pattern); hasPathAssignment {
		pattern = assignedPattern
	}
	switch mutationType {
	case MutationTypeEdge:
		return nil, parseEdgeMutation(pattern)
	case MutationTypeVertex:
		return parseVertexMutation(pattern), nil
	default:
		return nil, nil
	}
}

func extractWritePatternFromRaw(raw, kind string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if kind == "" {
		return extractLeadingPattern(raw)
	}

	upper := strings.ToUpper(raw)
	idx := -1
	if strings.HasPrefix(upper, kind) {
		idx = 0
	} else {
		if lineIdx := strings.Index(upper, "\n"+kind+" "); lineIdx >= 0 {
			idx = lineIdx + 1
		} else if spacedIdx := strings.Index(upper, " "+kind+" "); spacedIdx >= 0 {
			idx = spacedIdx + 1
		}
	}
	if idx < 0 {
		return extractLeadingPattern(raw)
	}
	body := strings.TrimSpace(raw[idx+len(kind):])
	if body == "" {
		return ""
	}
	if cut := nextWriteClauseBoundary(body); cut >= 0 {
		body = strings.TrimSpace(body[:cut])
	}
	return body
}

func nextWriteClauseBoundary(body string) int {
	upper := strings.ToUpper(body)
	separators := []string{
		"\nWITH ", "\nRETURN ", "\nMATCH ", "\nOPTIONAL MATCH ", "\nUNWIND ", "\nCALL ",
		"\nCREATE ", "\nMERGE ", "\nSET ", "\nREMOVE ", "\nDELETE ", "\nDETACH DELETE ",
		" WITH ", " RETURN ", " MATCH ", " OPTIONAL MATCH ", " UNWIND ", " CALL ",
		" CREATE ", " MERGE ", " SET ", " REMOVE ", " DELETE ", " DETACH DELETE ",
	}
	best := -1
	for _, sep := range separators {
		if idx := strings.Index(upper, sep); idx >= 0 {
			if best < 0 || idx < best {
				best = idx
			}
		}
	}
	return best
}

func parseEdgeMutation(pattern string) *EdgeMutation {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	if !hasRelationshipConnectorSyntax(pattern) {
		return nil
	}
	reverse := strings.Contains(pattern, "<-[")
	edgeVar := edgeVarFromPattern(pattern)
	matches := edgePatternRe.FindStringSubmatch(pattern)
	if len(matches) != 4 {
		loose := edgePatternEndpointsRe.FindStringSubmatch(pattern)
		if len(loose) != 3 {
			return nil
		}
		return &EdgeMutation{
			Pattern:      pattern,
			Reverse:      reverse,
			Var:          edgeVar,
			LeftVar:      variableFromPatternSegment(loose[1]),
			RightVar:     variableFromPatternSegment(loose[2]),
			LeftIDParam:  idParamFromPatternSegment(loose[1]),
			RightIDParam: idParamFromPatternSegment(loose[2]),
			LeftLabels:   labelsFromPatternSegment(loose[1]),
			RightLabels:  labelsFromPatternSegment(loose[2]),
		}
	}
	return &EdgeMutation{
		Pattern:      pattern,
		Reverse:      reverse,
		Var:          edgeVar,
		LeftVar:      variableFromPatternSegment(matches[1]),
		RightVar:     variableFromPatternSegment(matches[3]),
		LeftIDParam:  idParamFromPatternSegment(matches[1]),
		RightIDParam: idParamFromPatternSegment(matches[3]),
		Type:         edgeTypeExprFromPattern(pattern),
		LeftLabels:   labelsFromPatternSegment(matches[1]),
		RightLabels:  labelsFromPatternSegment(matches[3]),
	}
}

func hasRelationshipConnectorSyntax(pattern string) bool {
	compact := strings.ReplaceAll(strings.TrimSpace(pattern), " ", "")
	compact = strings.ReplaceAll(compact, "\n", "")
	compact = strings.ReplaceAll(compact, "\t", "")
	if compact == "" {
		return false
	}
	return strings.Contains(compact, "--") ||
		strings.Contains(compact, "->") ||
		strings.Contains(compact, "<-") ||
		strings.Contains(compact, "-[") ||
		strings.Contains(compact, "]-")
}

func edgeTypeExprFromPattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return ""
	}
	start := strings.IndexByte(pattern, '[')
	end := strings.IndexByte(pattern, ']')
	if start < 0 || end <= start {
		return ""
	}
	body := strings.TrimSpace(pattern[start+1 : end])
	if body == "" {
		return ""
	}
	colon := strings.IndexByte(body, ':')
	if colon < 0 {
		return ""
	}
	body = strings.TrimSpace(body[colon+1:])
	if body == "" {
		return ""
	}
	if cut := strings.IndexByte(body, '{'); cut >= 0 {
		body = strings.TrimSpace(body[:cut])
	}
	if cut := strings.IndexByte(body, '*'); cut >= 0 {
		body = strings.TrimSpace(body[:cut])
	}
	return body
}

func projectionEdgeTypeSet(typeExpr string) map[string]struct{} {
	typeExpr = strings.TrimSpace(typeExpr)
	if typeExpr == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, raw := range strings.Split(typeExpr, "|") {
		token := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(raw, ":")))
		if token == "" {
			continue
		}
		out[token] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func projectionEdgeTypeScanFilter(typeExpr string) string {
	types := projectionEdgeTypeSet(typeExpr)
	if len(types) != 1 {
		return ""
	}
	for typeName := range types {
		return typeName
	}
	return ""
}

func projectionVectorCoordinateList(value any) ([]float64, bool) {
	list, ok := projectionListValue(value)
	if !ok {
		return nil, false
	}
	out := make([]float64, 0, len(list))
	for _, item := range list {
		numeric, ok := projectionNumericToFloat(item)
		if !ok {
			return nil, false
		}
		out = append(out, numeric)
	}
	return out, true
}

func projectionPointXY(value any) (float64, float64, bool, bool) {
	typed, ok := value.(map[string]any)
	if !ok || typed == nil {
		return 0, 0, false, false
	}
	if lonRaw, hasLon := typed["longitude"]; hasLon {
		latRaw, hasLat := typed["latitude"]
		if !hasLat {
			return 0, 0, true, false
		}
		lon, lonOK := projectionNumericToFloat(lonRaw)
		lat, latOK := projectionNumericToFloat(latRaw)
		if !lonOK || !latOK {
			return 0, 0, true, false
		}
		return lon, lat, true, true
	}
	xRaw, hasX := typed["x"]
	yRaw, hasY := typed["y"]
	if !hasX || !hasY {
		return 0, 0, false, false
	}
	x, xOK := projectionNumericToFloat(xRaw)
	y, yOK := projectionNumericToFloat(yRaw)
	if !xOK || !yOK {
		return 0, 0, false, false
	}
	return x, y, false, true
}

func edgePatternExpectedProperties(pattern string, params map[string]any, row map[string]any, state *State) map[string]any {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	open := strings.IndexByte(pattern, '[')
	close := strings.IndexByte(pattern, ']')
	if open < 0 || close <= open {
		return nil
	}
	return resolveWritePropertyBindingsState(pattern[open:close+1], params, row, state)
}

func edgeHasExpectedProperties(edge *graph.Edge, expected map[string]any) bool {
	if len(expected) == 0 {
		return true
	}
	if edge == nil {
		return false
	}
	for key, expectedValue := range expected {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if strings.EqualFold(key, "id") {
			if raw, ok := edge.Properties[key]; ok {
				actualValue := decodeProjectionPropertyValue(raw)
				if !projectionWriteValuesEqual(actualValue, expectedValue) {
					return false
				}
				continue
			}
			if !projectionWriteValuesEqual(strings.TrimSpace(edge.ID), expectedValue) {
				return false
			}
			continue
		}
		raw, ok := edge.Properties[key]
		if !ok {
			return false
		}
		actualValue := decodeProjectionPropertyValue(raw)
		if !projectionWriteValuesEqual(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

func edgeVarFromPattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return ""
	}
	start := strings.IndexByte(pattern, '[')
	end := strings.IndexByte(pattern, ']')
	if start < 0 || end <= start {
		return ""
	}
	body := strings.TrimSpace(pattern[start+1 : end])
	if body == "" || body[0] == ':' || body[0] == '{' {
		return ""
	}
	idx := strings.IndexByte(body, ':')
	if idx >= 0 {
		body = body[:idx]
	}
	idx = strings.IndexByte(body, '{')
	if idx >= 0 {
		body = body[:idx]
	}
	idx = strings.IndexByte(body, '*')
	if idx >= 0 {
		body = body[:idx]
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	for i := 0; i < len(body); i++ {
		if i == 0 {
			if body[i] == '_' || ((body[i] >= 'a' && body[i] <= 'z') || (body[i] >= 'A' && body[i] <= 'Z')) {
				continue
			}
			return ""
		}
		if !isIdentifierChar(rune(body[i])) {
			return ""
		}
	}
	return body
}

func parseVertexMutation(pattern string) *VertexMutation {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || strings.Contains(pattern, "-[") {
		return nil
	}
	segment := firstVertexPatternSegment(pattern)
	if segment != "" {
		pattern = segment
	}
	return &VertexMutation{Pattern: pattern, Var: variableFromPatternSegment(pattern), IDParam: idParamFromPatternSegment(pattern), Labels: labelsFromPatternSegment(pattern)}
}

func firstVertexPatternSegment(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return ""
	}
	start := strings.Index(pattern, "(")
	if start < 0 {
		return ""
	}
	depth := 0
	for idx := start; idx < len(pattern); idx++ {
		switch pattern[idx] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(pattern[start : idx+1])
			}
		}
	}
	return ""
}

func extractLeadingPattern(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parts := strings.Fields(raw)
	if len(parts) <= 1 {
		return raw
	}
	return strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
}

func labelsFromPatternSegment(segment string) []string {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return nil
	}
	if brace := strings.IndexByte(segment, '{'); brace >= 0 {
		segment = strings.TrimSpace(segment[:brace])
	}
	labels := []string{}
	seen := map[string]struct{}{}
	runes := []rune(segment)
	for idx := 0; idx < len(runes); idx++ {
		if runes[idx] != ':' {
			continue
		}
		start := idx + 1
		for start < len(runes) && unicode.IsSpace(runes[start]) {
			start++
		}
		if start >= len(runes) {
			continue
		}
		end := start
		hasExpr := false
		for end < len(runes) {
			r := runes[end]
			if unicode.IsSpace(r) {
				break
			}
			if r == '|' || r == '!' || unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
				end++
				hasExpr = true
				continue
			}
			break
		}
		if !hasExpr {
			continue
		}
		labelExpr := strings.TrimSpace(string(runes[start:end]))
		if labelExpr == "" {
			continue
		}
		if _, ok := seen[labelExpr]; ok {
			idx = end - 1
			continue
		}
		seen[labelExpr] = struct{}{}
		labels = append(labels, labelExpr)
		idx = end - 1
	}
	return labels
}

func variableFromPatternSegment(segment string) string {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return ""
	}
	if strings.HasPrefix(segment, "(") && strings.HasSuffix(segment, ")") {
		segment = strings.TrimSpace(segment[1 : len(segment)-1])
	}
	runes := []rune(segment)
	end := 0
	for end < len(runes) {
		r := runes[end]
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	name := string(runes[:end])
	if name == "" {
		return ""
	}
	if first := runes[0]; !(unicode.IsLetter(first) || first == '_') {
		return ""
	}
	return name
}

func idParamFromPatternSegment(segment string) string {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return ""
	}
	body, ok := writePatternPropertyMapBody(segment)
	if !ok || strings.TrimSpace(body) == "" {
		return ""
	}
	for _, pair := range writeSplitTopLevelComma(body) {
		key, value, ok := writeSplitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(key), "id") {
			continue
		}
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "$") {
			param := strings.TrimSpace(strings.TrimPrefix(value, "$"))
			if isIdentifierToken(param) {
				return param
			}
		}
		return ""
	}
	return ""
}

func resolveWriteParams(state *State, raw, mergePattern string, onCreate, onMatch []string) map[string]any {
	resolved := map[string]any{}
	if state == nil || len(state.Params) == 0 {
		return resolved
	}
	keys := referencedParamKeys(raw, mergePattern, strings.Join(onCreate, " "), strings.Join(onMatch, " "))
	for _, key := range keys {
		if value, ok := state.Params[key]; ok {
			resolved[key] = value
		}
	}
	return resolved
}

func referencedParamKeys(texts ...string) []string {
	if len(texts) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, text := range texts {
		if text == "" {
			continue
		}
		runes := []rune(text)
		for idx := 0; idx < len(runes); idx++ {
			if runes[idx] != '$' {
				continue
			}
			start := idx + 1
			if start >= len(runes) {
				continue
			}
			if !(unicode.IsLetter(runes[start]) || runes[start] == '_') {
				continue
			}
			end := start + 1
			for end < len(runes) {
				r := runes[end]
				if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
					end++
					continue
				}
				break
			}
			key := string(runes[start:end])
			if key != "" {
				seen[key] = struct{}{}
			}
			idx = end - 1
		}
	}
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func parseProjectionSpec(raw string) (expr, alias string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	upper := strings.ToUpper(raw)
	idx := strings.LastIndex(upper, " AS ")
	if idx < 0 {
		return raw, ""
	}
	expr = strings.TrimSpace(raw[:idx])
	alias = strings.TrimSpace(raw[idx+4:])
	if strings.HasPrefix(alias, "`") && strings.HasSuffix(alias, "`") && len(alias) >= 2 {
		alias = strings.TrimSpace(alias[1 : len(alias)-1])
	}
	if expr == "" {
		expr = raw
		alias = ""
	}
	return expr, alias
}

func stringSliceAttr(attrs map[string]any, key string) []string {
	if attrs == nil {
		return nil
	}
	value, ok := attrs[key]
	if !ok || value == nil {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			out = append(out, item)
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				continue
			}
			text = strings.TrimSpace(text)
			if text == "" {
				continue
			}
			out = append(out, text)
		}
		return out
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil
		}
		return []string{text}
	default:
		return nil
	}
}

func intAttr(attrs map[string]any, key string) int {
	value, _ := intAttrWithPresence(attrs, key)
	return value
}

func intAttrWithPresence(attrs map[string]any, key string) (int, bool) {
	if attrs == nil {
		return 0, false
	}
	v, ok := attrs[key]
	if !ok || v == nil {
		return 0, false
	}
	switch typed := v.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		n, err := strconv.Atoi(trimmed)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func stringAttr(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	v, ok := attrs[key]
	if !ok || v == nil {
		return ""
	}
	if text, ok := v.(string); ok {
		return text
	}
	return ""
}

type orderingSpec struct {
	expression string
	descending bool
}

func parseOrderingSpec(attrs map[string]any) []orderingSpec {
	if attrs == nil {
		return nil
	}
	raw, ok := attrs["ordering"]
	if !ok || raw == nil {
		return nil
	}

	parseDirection := func(v any) bool {
		s, ok := v.(string)
		if !ok {
			return false
		}
		s = strings.ToUpper(strings.TrimSpace(s))
		return s == "DESC" || s == "DESCENDING"
	}

	parseOrderingText := func(raw string) (orderingSpec, bool) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return orderingSpec{}, false
		}
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			return orderingSpec{}, false
		}
		last := strings.ToUpper(fields[len(fields)-1])
		switch last {
		case "ASC", "ASCENDING":
			expr := strings.TrimSpace(strings.Join(fields[:len(fields)-1], " "))
			if expr == "" {
				return orderingSpec{}, false
			}
			return orderingSpec{expression: expr, descending: false}, true
		case "DESC", "DESCENDING":
			expr := strings.TrimSpace(strings.Join(fields[:len(fields)-1], " "))
			if expr == "" {
				return orderingSpec{}, false
			}
			return orderingSpec{expression: expr, descending: true}, true
		default:
			return orderingSpec{expression: raw, descending: false}, true
		}
	}

	out := []orderingSpec{}
	switch typed := raw.(type) {
	case []map[string]any:
		for _, item := range typed {
			expr, _ := item["expression"].(string)
			expr = strings.TrimSpace(expr)
			if expr == "" {
				continue
			}
			out = append(out, orderingSpec{expression: expr, descending: parseDirection(item["direction"])})
		}
	case []any:
		for _, rawItem := range typed {
			if text, ok := rawItem.(string); ok {
				if spec, specOK := parseOrderingText(text); specOK {
					out = append(out, spec)
				}
				continue
			}
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			expr, _ := item["expression"].(string)
			expr = strings.TrimSpace(expr)
			if expr == "" {
				continue
			}
			out = append(out, orderingSpec{expression: expr, descending: parseDirection(item["direction"])})
		}
	case []string:
		for _, item := range typed {
			if spec, ok := parseOrderingText(item); ok {
				out = append(out, spec)
			}
		}
	case string:
		for _, item := range splitProjectionTopLevelByComma(typed) {
			if spec, ok := parseOrderingText(item); ok {
				out = append(out, spec)
			}
		}
	}

	return out
}

func valueFromRowWithPresence(row map[string]any, key string) (any, bool) {
	if row == nil {
		return nil, false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	if v, ok := row[key]; ok {
		return v, true
	}
	if idx := strings.LastIndex(key, "."); idx >= 0 && idx < len(key)-1 {
		if v, ok := row[key[idx+1:]]; ok {
			return v, true
		}
	}
	return nil, false
}

func isSimpleOrderingReferenceExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" || strings.HasPrefix(expr, "$") {
		return false
	}
	hasIdentifierRune := false
	for _, ch := range expr {
		switch {
		case ch == '_' || ch == '.':
		case ch >= '0' && ch <= '9':
		case (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'):
			hasIdentifierRune = true
		default:
			return false
		}
	}
	return hasIdentifierRune
}

func compareScalarValues(left, right any) int {
	if cmp, ok := compareTypedScalarValues(left, right); ok {
		return cmp
	}
	return compareCypherOrderValues(left, right)
}

func compareScalarValuesObserved(state *State, left, right any) int {
	if cmp, ok := compareTypedScalarValues(left, right); ok {
		observeRuntimeCounter(state, "runtime.operator.sort.scalar_compare.typed", 1)
		return cmp
	}
	observeRuntimeCounter(state, "runtime.operator.sort.scalar_compare.fallback", 1)
	return compareCypherOrderValues(left, right)
}

func observeRuntimeCounter(state *State, name string, delta int64) {
	if state == nil || state.Metrics == nil || strings.TrimSpace(name) == "" || delta == 0 {
		return
	}
	state.Metrics.ObserveRuntimeCounter(name, delta)
}

func compareTypedScalarValues(left, right any) (int, bool) {
	if left == nil && right == nil {
		return 0, true
	}
	if ln, lok := numericValue(left); lok {
		rn, rok := numericValue(right)
		if !rok {
			return 0, false
		}
		leftNaN := math.IsNaN(ln)
		rightNaN := math.IsNaN(rn)
		if leftNaN || rightNaN {
			switch {
			case leftNaN && rightNaN:
				return 0, true
			case leftNaN:
				return 1, true
			default:
				return -1, true
			}
		}
		switch {
		case ln < rn:
			return -1, true
		case ln > rn:
			return 1, true
		default:
			return 0, true
		}
	}
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.Compare(ls, rs), true
	}
	lb, lok := left.(bool)
	rb, rok := right.(bool)
	if lok && rok {
		if lb == rb {
			return 0, true
		}
		if !lb && rb {
			return -1, true
		}
		return 1, true
	}
	return 0, false
}

func compareCypherOrderValues(left, right any) int {
	if leftTemporal, ok := projectionTemporalMap(left); ok {
		if rightTemporal, ok := projectionTemporalMap(right); ok {
			if cmp, ok := projectionCompareTemporalValues(leftTemporal, rightTemporal); ok {
				return cmp
			}
		}
	}

	leftRank := cypherOrderRank(left)
	rightRank := cypherOrderRank(right)
	if leftRank != rightRank {
		if leftRank < rightRank {
			return -1
		}
		return 1
	}

	switch leftRank {
	case 1: // map
		lm, lok := left.(map[string]any)
		rm, rok := right.(map[string]any)
		if !lok || !rok {
			break
		}
		lk := make([]string, 0, len(lm))
		for k := range lm {
			lk = append(lk, k)
		}
		rk := make([]string, 0, len(rm))
		for k := range rm {
			rk = append(rk, k)
		}
		sort.Strings(lk)
		sort.Strings(rk)
		minLen := len(lk)
		if len(rk) < minLen {
			minLen = len(rk)
		}
		for i := 0; i < minLen; i++ {
			if lk[i] != rk[i] {
				if lk[i] < rk[i] {
					return -1
				}
				return 1
			}
			cmp := compareCypherOrderValues(lm[lk[i]], rm[rk[i]])
			if cmp != 0 {
				return cmp
			}
		}
		if len(lk) < len(rk) {
			return -1
		}
		if len(lk) > len(rk) {
			return 1
		}
		return 0
	case 2: // node
		lv, _ := left.(*graph.Vertex)
		rv, _ := right.(*graph.Vertex)
		if lv == nil || rv == nil {
			break
		}
		return strings.Compare(strings.TrimSpace(lv.ID), strings.TrimSpace(rv.ID))
	case 3: // relationship
		le, _ := left.(*graph.Edge)
		re, _ := right.(*graph.Edge)
		if le == nil || re == nil {
			break
		}
		return strings.Compare(strings.TrimSpace(le.ID), strings.TrimSpace(re.ID))
	case 4: // list
		ll, lok := projectionListValue(left)
		rl, rok := projectionListValue(right)
		if !lok || !rok {
			break
		}
		minLen := len(ll)
		if len(rl) < minLen {
			minLen = len(rl)
		}
		for i := 0; i < minLen; i++ {
			cmp := compareCypherOrderValues(ll[i], rl[i])
			if cmp != 0 {
				return cmp
			}
		}
		if len(ll) < len(rl) {
			return -1
		}
		if len(ll) > len(rl) {
			return 1
		}
		return 0
	case 6: // string
		ls, lok := left.(string)
		rs, rok := right.(string)
		if lok && rok {
			return strings.Compare(ls, rs)
		}
	case 7: // bool
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if lok && rok {
			if lb == rb {
				return 0
			}
			if !lb && rb {
				return -1
			}
			return 1
		}
	case 8: // numeric
		ln, lok := numericValue(left)
		rn, rok := numericValue(right)
		if lok && rok {
			switch {
			case ln < rn:
				return -1
			case ln > rn:
				return 1
			default:
				return 0
			}
		}
	case 9, 10: // NaN, null
		return 0
	}

	ls := fmt.Sprint(left)
	rs := fmt.Sprint(right)
	if ls < rs {
		return -1
	}
	if ls > rs {
		return 1
	}
	return 0
}

func cypherOrderRank(value any) int {
	if value == nil {
		return 10
	}
	if n, ok := numericValue(value); ok && math.IsNaN(n) {
		return 9
	}
	if _, ok := numericValue(value); ok {
		return 8
	}
	if _, ok := value.(bool); ok {
		return 7
	}
	if _, ok := value.(string); ok {
		return 6
	}
	if _, ok := value.(projectionPathValue); ok {
		return 5
	}
	if path, ok := value.(map[string]any); ok {
		if _, hasNodes := path["nodes"]; hasNodes {
			if _, hasRels := path["relationships"]; hasRels {
				return 5
			}
		}
	}
	if _, ok := projectionListValue(value); ok {
		return 4
	}
	if _, ok := value.(*graph.Edge); ok {
		return 3
	}
	if _, ok := value.(*graph.Vertex); ok {
		return 2
	}
	if _, ok := value.(map[string]any); ok {
		return 1
	}
	if _, ok := value.(map[string]string); ok {
		return 1
	}
	return 8
}

func numericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		n, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return n, true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func cloneRow(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clonePathVertex(vertex *graph.Vertex) *graph.Vertex {
	if vertex == nil {
		return nil
	}
	clone := *vertex
	clone.Labels = append([]string(nil), vertex.Labels...)
	clone.Properties = clonePathPropertyMap(vertex.Properties)
	return &clone
}

func clonePathEdge(edge *graph.Edge) *graph.Edge {
	if edge == nil {
		return nil
	}
	clone := *edge
	clone.Properties = clonePathPropertyMap(edge.Properties)
	return &clone
}

func clonePathPropertyMap(in graph.PropertyMap) graph.PropertyMap {
	if len(in) == 0 {
		return nil
	}
	out := make(graph.PropertyMap, len(in))
	for key, raw := range in {
		out[key] = append([]byte(nil), raw...)
	}
	return out
}
