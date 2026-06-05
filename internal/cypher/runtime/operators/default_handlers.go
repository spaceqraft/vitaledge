package operators

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

var edgePatternRe = regexp.MustCompile(`^\s*\(([^)]*)\)\s*[-<]*\s*\[.*?:([A-Za-z_][A-Za-z0-9_]*)[^\]]*\]\s*[->]*\s*\(([^)]*)\)\s*$`)

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

func NewExpandMatchHandler() Handler { return expandHandler{baseHandler{op: "PHY_EXPAND_MATCH"}} }

func (h expandHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if len(state.Rows) == 0 {
		state.Rows = []map[string]any{{}}
	}
	return nil
}

type optionalExpandHandler struct{ baseHandler }

func NewOptionalExpandHandler() Handler {
	return optionalExpandHandler{baseHandler{op: "PHY_EXPAND_OPTIONAL"}}
}

func (h optionalExpandHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if len(state.Rows) == 0 {
		state.Rows = []map[string]any{{}}
	}
	return nil
}

type projectHandler struct{ baseHandler }

func NewProjectHandler() Handler { return projectHandler{baseHandler{op: "PHY_PROJECT"}} }

func (h projectHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	items := stringSliceAttr(attrs, "items")
	if len(state.Rows) == 0 {
		state.Rows = []map[string]any{{}}
	}
	in := state.Rows
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
				key := stableRowKey(next)
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
				for k, v := range row {
					projected[k] = v
				}
				continue
			}
			value, ok := resolveProjectionExprValue(expr, row, state)
			if !ok && alias != "" {
				value, ok = row[alias]
			}
			if !ok {
				value = nil
			}
			if alias != "" {
				projected[alias] = value
			} else {
				projected[expr] = value
			}
		}
		if distinct {
			key := stableRowKey(projected)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		out = append(out, projected)
	}
	state.Rows = out
	return nil
}

func resolveProjectionExprValue(expr string, row map[string]any, state *State) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if row != nil {
		if value, ok := row[expr]; ok {
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
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, true
	}
	return nil, false
}

type sortHandler struct{ baseHandler }

func NewSortHandler() Handler { return sortHandler{baseHandler{op: "PHY_SORT"}} }

func (h sortHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil || len(state.Rows) <= 1 {
		return nil
	}
	ordering := parseOrderingSpec(attrs)
	if len(ordering) == 0 {
		return nil
	}

	sort.SliceStable(state.Rows, func(i, j int) bool {
		left := state.Rows[i]
		right := state.Rows[j]
		for _, ord := range ordering {
			lv := valueFromRow(left, ord.expression)
			rv := valueFromRow(right, ord.expression)
			cmp := compareScalarValues(lv, rv)
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

	return nil
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
	skip := intAttr(attrs, "skip")
	limit, hasLimit := intAttrWithPresence(attrs, "limit")
	if skip < 0 {
		skip = 0
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

type writeHandler struct{ baseHandler }

func NewWriteHandler() Handler { return writeHandler{baseHandler{op: "PHY_WRITE"}} }

func (h writeHandler) Execute(nodeID string, attrs map[string]any, state *State) error {
	if err := h.baseHandler.Execute(nodeID, attrs, state); err != nil {
		return err
	}
	if state == nil {
		return nil
	}

	event := WriteEvent{NodeID: nodeID}
	kind := ""
	raw := ""
	mergePattern := ""
	onCreate := []string{}
	onMatch := []string{}
	if attrs != nil {
		if v, ok := attrs["kind"].(string); ok {
			kind = strings.TrimSpace(v)
			event.Kind = kind
		}
		if v, ok := attrs["raw"].(string); ok {
			raw = strings.TrimSpace(v)
			event.Raw = raw
		}
		if v, ok := attrs["mergePattern"].(string); ok {
			mergePattern = strings.TrimSpace(v)
			event.MergePattern = mergePattern
		}
		onCreate = stringSliceAttr(attrs, "mergeOnCreate")
		if len(onCreate) > 0 {
			event.MergeOnCreate = append([]string(nil), onCreate...)
		}
		onMatch = stringSliceAttr(attrs, "mergeOnMatch")
		if len(onMatch) > 0 {
			event.MergeOnMatch = append([]string(nil), onMatch...)
		}
	}

	event.MutationType = classifyMutationType(kind, mergePattern, raw)
	event.Vertex, event.Edge = buildMutationPayloads(event.MutationType, mergePattern, raw)
	event.Bindings = captureBindings(state)
	event.ResolvedParams = resolveWriteParams(state, raw, mergePattern, onCreate, onMatch)
	event.ParamKeys = referencedParamKeys(raw, mergePattern, strings.Join(onCreate, " "), strings.Join(onMatch, " "))
	state.WriteEvents = append(state.WriteEvents, event)

	if len(state.Rows) == 0 {
		state.Rows = []map[string]any{{}}
	}
	for _, row := range state.Rows {
		if row == nil {
			continue
		}
		materializeWriteBindings(row, event)
		row["__write_event_count"] = len(state.WriteEvents)
	}

	return nil
}

func materializeWriteBindings(row map[string]any, event WriteEvent) {
	if row == nil {
		return
	}
	if event.Vertex != nil {
		vertexID := resolveWriteEntityID(event.Vertex.Var, event.Vertex.IDParam, event.Bindings, event.ResolvedParams)
		if vertexID != "" {
			bindWriteVariable(row, event.Vertex.Var, vertexID)
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
	}
}

func bindWriteVariable(row map[string]any, varName, entityID string) {
	varName = strings.TrimSpace(varName)
	entityID = strings.TrimSpace(entityID)
	if varName == "" || entityID == "" {
		return
	}
	row[varName] = entityID
	row[varName+".id"] = entityID
}

func resolveWriteEntityID(varName, idParam string, bindings map[string]any, resolvedParams map[string]any) string {
	varName = strings.TrimSpace(varName)
	if varName != "" {
		if value, ok := bindings[varName]; ok {
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

func scalarString(value any) string {
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
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]any, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]any{key, row[key]})
	}
	encoded, err := json.Marshal(pairs)
	if err != nil {
		return fmt.Sprint(pairs)
	}
	return string(encoded)
}

func classifyMutationType(kind, mergePattern, raw string) MutationType {
	text := strings.ToUpper(strings.TrimSpace(mergePattern))
	if text == "" {
		text = strings.ToUpper(strings.TrimSpace(raw))
	}
	if strings.Contains(text, "-[") && (strings.Contains(text, "]->") || strings.Contains(text, "<-[")) {
		return MutationTypeEdge
	}
	if strings.Contains(text, "(") && strings.Contains(text, ")") {
		return MutationTypeVertex
	}
	if strings.EqualFold(strings.TrimSpace(kind), "SET") {
		return MutationTypeProperty
	}
	return MutationTypeUnknown
}

func buildMutationPayloads(mutationType MutationType, mergePattern, raw string) (*VertexMutation, *EdgeMutation) {
	pattern := strings.TrimSpace(mergePattern)
	if pattern == "" {
		pattern = extractLeadingPattern(raw)
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

func parseEdgeMutation(pattern string) *EdgeMutation {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	reverse := strings.Contains(pattern, "<-[")
	matches := edgePatternRe.FindStringSubmatch(pattern)
	if len(matches) != 4 {
		return &EdgeMutation{Pattern: pattern, Reverse: reverse}
	}
	return &EdgeMutation{
		Pattern:      pattern,
		Reverse:      reverse,
		LeftVar:      variableFromPatternSegment(matches[1]),
		RightVar:     variableFromPatternSegment(matches[3]),
		LeftIDParam:  idParamFromPatternSegment(matches[1]),
		RightIDParam: idParamFromPatternSegment(matches[3]),
		Type:         strings.TrimSpace(matches[2]),
		LeftLabels:   labelsFromPatternSegment(matches[1]),
		RightLabels:  labelsFromPatternSegment(matches[3]),
	}
}

func parseVertexMutation(pattern string) *VertexMutation {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || strings.Contains(pattern, "-[") {
		return nil
	}
	return &VertexMutation{Pattern: pattern, Var: variableFromPatternSegment(pattern), IDParam: idParamFromPatternSegment(pattern), Labels: labelsFromPatternSegment(pattern)}
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
	labels := []string{}
	seen := map[string]struct{}{}
	runes := []rune(segment)
	for idx := 0; idx < len(runes); idx++ {
		if runes[idx] != ':' {
			continue
		}
		start := idx + 1
		if start >= len(runes) || !(unicode.IsLetter(runes[start]) || runes[start] == '_') {
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
		label := string(runes[start:end])
		if _, ok := seen[label]; ok {
			idx = end - 1
			continue
		}
		seen[label] = struct{}{}
		labels = append(labels, label)
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
	keys := referencedParamKeys(segment)
	if len(keys) == 0 {
		return ""
	}
	if strings.Contains(strings.ToLower(segment), "id:$") {
		for _, key := range keys {
			if strings.Contains(strings.ToLower(segment), "id:$"+strings.ToLower(key)) {
				return key
			}
		}
	}
	return keys[0]
}

func captureBindings(state *State) map[string]any {
	if state == nil || len(state.Rows) == 0 {
		return nil
	}
	row := state.Rows[0]
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
		return strings.EqualFold(strings.TrimSpace(s), "DESC")
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
	}

	return out
}

func valueFromRow(row map[string]any, key string) any {
	if row == nil {
		return nil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	if v, ok := row[key]; ok {
		return v
	}
	if idx := strings.LastIndex(key, "."); idx >= 0 && idx < len(key)-1 {
		if v, ok := row[key[idx+1:]]; ok {
			return v
		}
	}
	return nil
}

func compareScalarValues(left, right any) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return 1
	}
	if right == nil {
		return -1
	}

	if ln, lok := numericValue(left); lok {
		if rn, rok := numericValue(right); rok {
			switch {
			case ln < rn:
				return -1
			case ln > rn:
				return 1
			default:
				return 0
			}
		}
	}

	ls, lsok := left.(string)
	rs, rsok := right.(string)
	if lsok && rsok {
		switch {
		case ls < rs:
			return -1
		case ls > rs:
			return 1
		default:
			return 0
		}
	}

	lb, lbok := left.(bool)
	rb, rbok := right.(bool)
	if lbok && rbok {
		if lb == rb {
			return 0
		}
		if !lb && rb {
			return -1
		}
		return 1
	}

	ls = fmt.Sprint(left)
	rs = fmt.Sprint(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

func numericValue(v any) (float64, bool) {
	switch typed := v.(type) {
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
