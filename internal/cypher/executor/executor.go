package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
)

type Params map[string]any

const ProcedureDeclsParam = "__tck_procedures"

type ProcedureArg struct {
	Name     string
	Type     string
	Nullable bool
}

type ProcedureDecl struct {
	Name    string
	Inputs  []ProcedureArg
	Outputs []ProcedureArg
	Rows    []map[string]any
}

type Row map[string]any

type Result struct {
	Columns []string
	Rows    []Row
	Stats   Stats
}

type Stats struct {
	RowsReturned int
	Duration     time.Duration
}

type Metrics interface {
	ObserveStatement(kind ast.StatementKind, outcome string, duration time.Duration)
	ObserveRowsReturned(rows int)
	ObserveIndexCandidate(tenant, schema, property string, indexed bool)
	ObserveIndexLookup(strategy, outcome string, matches int)
}

type IndexCatalog interface {
	HasPropertyIndex(tenant, schema, property string) bool
}

type Options struct {
	Metrics      Metrics
	IndexCatalog IndexCatalog
}

type Executor struct {
	store        graph.GraphStore
	metrics      Metrics
	indexCatalog IndexCatalog
}

type noopMetrics struct{}

func (noopMetrics) ObserveStatement(_ ast.StatementKind, _ string, _ time.Duration) {}

func (noopMetrics) ObserveRowsReturned(_ int) {}

func (noopMetrics) ObserveIndexCandidate(_, _, _ string, _ bool) {}

func (noopMetrics) ObserveIndexLookup(_, _ string, _ int) {}

func New(store graph.GraphStore, opts Options) *Executor {
	metrics := opts.Metrics
	if metrics == nil {
		metrics = noopMetrics{}
	}
	return &Executor{store: store, metrics: metrics, indexCatalog: opts.IndexCatalog}
}

func (e *Executor) ExecuteStatement(ctx context.Context, stmt ast.Statement, params Params) (_ *Result, err error) {
	if e == nil || e.store == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "statement is required", nil)
	}
	if params == nil {
		params = Params{}
	}

	started := time.Now()
	defer func() {
		e.metrics.ObserveStatement(stmt.Kind(), outcomeFromError(err), time.Since(started))
	}()

	switch s := stmt.(type) {
	case *ast.MatchQueryStatement:
		res, execErr := e.executeMatchQuery(ctx, s, params)
		if execErr != nil {
			return nil, execErr
		}
		e.metrics.ObserveRowsReturned(len(res.Rows))
		return res, nil
	case *ast.QueryStatement:
		res, execErr := e.executeQueryStatement(ctx, s, params)
		if execErr != nil {
			return nil, execErr
		}
		e.metrics.ObserveRowsReturned(len(res.Rows))
		return res, nil
	case *ast.StandaloneCallStatement:
		res, execErr := e.executeStandaloneCallStatement(s, params)
		if execErr != nil {
			return nil, execErr
		}
		e.metrics.ObserveRowsReturned(len(res.Rows))
		return res, nil
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("statement kind %s not yet executable", stmt.Kind()), nil)
	}
}

func (e *Executor) executeMatchQuery(ctx context.Context, stmt *ast.MatchQueryStatement, params Params) (*Result, error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "match query statement is required", nil)
	}
	if len(stmt.MatchClauses) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "at least one MATCH clause is required", nil)
	}
	if stmt.Return.IncludeAll || len(stmt.Return.Items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "RETURN * is not yet supported", nil)
	}
	if stmt.Return.Distinct {
		return nil, graph.NewError(graph.ErrKindUnsupported, "RETURN DISTINCT is not yet supported", nil)
	}
	if len(stmt.Return.OrderBy) > 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "ORDER BY is not yet supported", nil)
	}

	skip, err := evalOptionalInt(stmt.Return.Skip, params)
	if err != nil {
		return nil, err
	}
	limit, err := evalOptionalInt(stmt.Return.Limit, params)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(stmt.Return.Items))
	for _, item := range stmt.Return.Items {
		if strings.TrimSpace(item.Alias) != "" {
			columns = append(columns, item.Alias)
			continue
		}
		columns = append(columns, item.Expression.Raw)
	}

	rows := make([]Row, 0)
	execErr := e.store.View(ctx, func(tx graph.Tx) error {
		matchRows := []Row{{}}
		for _, match := range stmt.MatchClauses {
			kind := ast.ClauseKindMatch
			clauseRaw := "MATCH " + match.Pattern
			if match.Optional {
				kind = ast.ClauseKindOptionalMatch
				clauseRaw = "OPTIONAL MATCH " + match.Pattern
			}
			if match.Where != nil && strings.TrimSpace(match.Where.Raw) != "" {
				clauseRaw += " WHERE " + strings.TrimSpace(match.Where.Raw)
			}

			nextRows, err := e.applyMatchClause(ctx, tx, matchRows, ast.Clause{Kind: kind, Raw: clauseRaw}, params)
			if err != nil {
				return err
			}
			matchRows = nextRows
		}
		projectedRows, err := evalReturnRows(stmt.Return.Items, matchRows)
		if err != nil {
			return err
		}
		rows = append(rows, projectedRows...)
		return nil
	})
	if execErr != nil {
		return nil, execErr
	}

	rows = applySkipLimit(rows, skip, limit)

	result := &Result{
		Columns: columns,
		Rows:    rows,
		Stats: Stats{
			RowsReturned: len(rows),
		},
	}
	result.Rows = normalizeResultRows(result.Rows)
	return result, nil
}

func evalReturnRow(items []ast.ProjectionItem, binding map[string]any) (Row, error) {
	out := Row{}
	for _, item := range items {
		val, err := evalExpression(item.Expression.Raw, binding)
		if err != nil {
			return nil, err
		}
		col := strings.TrimSpace(item.Alias)
		if col == "" {
			col = item.Expression.Raw
		}
		out[col] = val
	}
	return out, nil
}

func evalReturnRows(items []ast.ProjectionItem, matchRows []Row) ([]Row, error) {
	hasAggregate := false
	for _, item := range items {
		if _, ok := parseCountExpression(item.Expression.Raw); ok {
			hasAggregate = true
			break
		}
	}
	if !hasAggregate {
		out := make([]Row, 0, len(matchRows))
		for _, row := range matchRows {
			projected, err := evalReturnRow(items, row)
			if err != nil {
				return nil, err
			}
			out = append(out, projected)
		}
		return out, nil
	}

	type groupedReturn struct {
		projected Row
		counts    map[int]int
	}
	nonAggregateCount := 0
	for _, item := range items {
		if _, ok := parseCountExpression(item.Expression.Raw); !ok {
			nonAggregateCount++
		}
	}

	groups := map[string]*groupedReturn{}
	groupOrder := make([]string, 0)
	for _, row := range matchRows {
		projected := Row{}
		keyValues := make([]any, 0, nonAggregateCount)
		for _, item := range items {
			if _, ok := parseCountExpression(item.Expression.Raw); ok {
				continue
			}
			value, err := evalExpression(item.Expression.Raw, row)
			if err != nil {
				return nil, err
			}
			col := strings.TrimSpace(item.Alias)
			if col == "" {
				col = item.Expression.Raw
			}
			projected[col] = value
			keyValues = append(keyValues, normalizeResultValue(value))
		}

		keyBytes, err := json.Marshal(keyValues)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, "aggregation key is not serializable", err)
		}
		groupKey := string(keyBytes)
		group, ok := groups[groupKey]
		if !ok {
			group = &groupedReturn{projected: projected, counts: map[int]int{}}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}

		for idx, item := range items {
			countArg, ok := parseCountExpression(item.Expression.Raw)
			if !ok {
				continue
			}
			if countArg == "*" {
				group.counts[idx]++
				continue
			}
			value, err := evalExpression(countArg, row)
			if err != nil {
				return nil, err
			}
			if value != nil {
				group.counts[idx]++
			}
		}
	}

	if len(matchRows) == 0 && nonAggregateCount == 0 {
		empty := Row{}
		for _, item := range items {
			col := strings.TrimSpace(item.Alias)
			if col == "" {
				col = item.Expression.Raw
			}
			if _, ok := parseCountExpression(item.Expression.Raw); ok {
				empty[col] = 0
			} else {
				empty[col] = nil
			}
		}
		return []Row{empty}, nil
	}

	out := make([]Row, 0, len(groupOrder))
	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		projected := cloneRow(group.projected)
		for idx, item := range items {
			if _, ok := parseCountExpression(item.Expression.Raw); !ok {
				continue
			}
			col := strings.TrimSpace(item.Alias)
			if col == "" {
				col = item.Expression.Raw
			}
			projected[col] = group.counts[idx]
		}
		out = append(out, projected)
	}
	return out, nil
}

func evalExpression(raw string, binding map[string]any) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "empty return expression", nil)
	}
	if left, right, ok := splitTopLevelOperator(raw, ">="); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf >= rf, nil
		}
		return fmt.Sprint(lhs) >= fmt.Sprint(rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "<="); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf <= rf, nil
		}
		return fmt.Sprint(lhs) <= fmt.Sprint(rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "<>"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		return !reflect.DeepEqual(lhs, rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "="); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		return reflect.DeepEqual(lhs, rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, ">"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf > rf, nil
		}
		return fmt.Sprint(lhs) > fmt.Sprint(rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "<"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf < rf, nil
		}
		return fmt.Sprint(lhs) < fmt.Sprint(rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "+"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf + rf, nil
		}
		if value, ok := evalTemporalArithmetic(lhs, rhs, "+"); ok {
			return value, nil
		}
		return fmt.Sprint(lhs) + fmt.Sprint(rhs), nil
	}
	if left, right, ok := splitTopLevelOperator(raw, "-"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf - rf, nil
		}
		if value, ok := evalTemporalArithmetic(lhs, rhs, "-"); ok {
			return value, nil
		}
	}
	if left, right, ok := splitTopLevelOperator(raw, "*"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf * rf, nil
		}
		if value, ok := evalTemporalArithmetic(lhs, rhs, "*"); ok {
			return value, nil
		}
	}
	if left, right, ok := splitTopLevelOperator(raw, "/"); ok {
		lhs, err := evalExpression(left, binding)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpression(right, binding)
		if err != nil {
			return nil, err
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			if rf == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "division by zero", nil)
			}
			return lf / rf, nil
		}
		if value, ok := evalTemporalArithmetic(lhs, rhs, "/"); ok {
			return value, nil
		}
	}
	if raw == "true" || raw == "false" {
		return raw == "true", nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	if strings.HasPrefix(raw, "'") || strings.HasPrefix(raw, `"`) {
		unquoted, err := unquoteCypherString(raw)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("string literal %q is not supported", raw), err)
		}
		return unquoted, nil
	}
	if arg, ok := parseFunctionCall(raw, "labels"); ok {
		return evalLabelsFunction(arg, binding)
	}
	if arg, ok := parseFunctionCall(raw, "type"); ok {
		return evalTypeFunction(arg, binding)
	}
	if v, ok := binding[raw]; ok {
		switch typed := v.(type) {
		case *graph.Vertex:
			return vertexToMap(typed), nil
		case *graph.Edge:
			return edgeToMap(typed), nil
		default:
			return typed, nil
		}
	}

	parts := strings.Split(raw, ".")
	if len(parts) == 2 {
		base, ok := binding[parts[0]]
		if !ok {
			return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", parts[0]), nil)
		}
		if base == nil {
			return nil, nil
		}
		field := parts[1]
		switch typed := base.(type) {
		case *graph.Vertex:
			return evalVertexField(typed, field)
		case *graph.Edge:
			return evalEdgeField(typed, field)
		case map[string]any:
			if value, ok := typed[field]; ok {
				return value, nil
			}
			return nil, nil
		case string:
			if mapped, ok := parseStoredMapString(typed); ok {
				if value, ok := mapped[field]; ok {
					return value, nil
				}
				return nil, nil
			}
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("field access not supported on %T", base), nil)
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("field access not supported on %T", base), nil)
		}
	}

	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("return expression %q is not yet supported", raw), nil)
}

func evalVertexField(v *graph.Vertex, field string) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch field {
	case "id":
		return v.ID, nil
	case "tenant":
		return v.Tenant, nil
	case "labels":
		return append([]string(nil), v.Labels...), nil
	default:
		if v.Properties == nil {
			return nil, nil
		}
		val, ok := v.Properties[field]
		if !ok {
			return nil, nil
		}
		return string(val), nil
	}
}

func evalEdgeField(e *graph.Edge, field string) (any, error) {
	if e == nil {
		return nil, nil
	}
	switch field {
	case "id":
		return e.ID, nil
	case "tenant":
		return e.Tenant, nil
	case "type":
		return e.Type, nil
	case "src":
		return e.SrcID, nil
	case "dst":
		return e.DstID, nil
	default:
		if e.Properties == nil {
			return nil, nil
		}
		val, ok := e.Properties[field]
		if !ok {
			return nil, nil
		}
		return string(val), nil
	}
}

func evalOptionalInt(expr *ast.Expression, params Params) (int, error) {
	if expr == nil {
		return 0, nil
	}
	raw := strings.TrimSpace(expr.Raw)
	if raw == "" {
		return 0, graph.NewError(graph.ErrKindSemantic, "empty numeric expression", nil)
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		v, ok := params[name]
		if !ok {
			return 0, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		n, err := toInt(v)
		if err != nil {
			return 0, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("parameter %q is not an int", name), err)
		}
		if n < 0 {
			return 0, graph.NewError(graph.ErrKindInvalidInput, "numeric expression must be >= 0", nil)
		}
		return n, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("numeric expression %q is not supported", raw), err)
	}
	if n < 0 {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "numeric expression must be >= 0", nil)
	}
	return n, nil
}

func applySkipLimit(rows []Row, skip, limit int) []Row {
	if skip > len(rows) {
		return []Row{}
	}
	if skip > 0 {
		rows = rows[skip:]
	}
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

func requireStringParam(params Params, name string) (string, error) {
	v, ok := params[name]
	if !ok {
		return "", graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("parameter %q must be non-empty string", name), nil)
	}
	return s, nil
}

func toInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case int32:
		return int(n), nil
	case uint:
		return int(n), nil
	case uint64:
		return int(n), nil
	case uint32:
		return int(n), nil
	case float64:
		return int(n), nil
	case float32:
		return int(n), nil
	case string:
		return strconv.Atoi(strings.TrimSpace(n))
	default:
		return 0, fmt.Errorf("unsupported int conversion for %T", v)
	}
}

func vertexToMap(v *graph.Vertex) map[string]any {
	if v == nil {
		return nil
	}
	props := map[string]any{}
	for k, val := range v.Properties {
		props[k] = string(val)
	}
	return map[string]any{
		"tenant":     v.Tenant,
		"id":         v.ID,
		"labels":     append([]string(nil), v.Labels...),
		"properties": props,
	}
}

func edgeToMap(e *graph.Edge) map[string]any {
	if e == nil {
		return nil
	}
	props := map[string]any{}
	for k, val := range e.Properties {
		props[k] = string(val)
	}
	return map[string]any{
		"tenant":     e.Tenant,
		"id":         e.ID,
		"type":       e.Type,
		"src":        e.SrcID,
		"dst":        e.DstID,
		"properties": props,
	}
}

func normalizeResultRows(rows []Row) []Row {
	if len(rows) == 0 {
		return rows
	}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		normalized := Row{}
		for key, value := range row {
			normalized[key] = normalizeResultValue(value)
		}
		out = append(out, normalized)
	}
	return out
}

func normalizeResultValue(value any) any {
	switch typed := value.(type) {
	case *graph.Vertex:
		return vertexToMap(typed)
	case *graph.Edge:
		return edgeToMap(typed)
	case []byte:
		return string(typed)
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			if rendered, ok := renderTemporalValue(mapped); ok {
				return rendered
			}
		}
		return typed
	case map[string]any:
		if rendered, ok := renderTemporalValue(typed); ok {
			return rendered
		}
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeResultValue(item)
		}
		return out
	case Row:
		out := Row{}
		for key, item := range typed {
			out[key] = normalizeResultValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = normalizeResultValue(item)
		}
		return out
	default:
		return value
	}
}

func renderTemporalValue(value map[string]any) (string, bool) {
	typeNameRaw, ok := value["__temporal_type"]
	if !ok {
		return "", false
	}
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(typeNameRaw)))
	if typeName == "" {
		return "", false
	}

	if raw, ok := value["value"]; ok && strings.TrimSpace(fmt.Sprint(raw)) != "" {
		return fmt.Sprint(raw), true
	}

	if typeName == "duration" {
		return formatDurationComponents(durationComponentsFromMap(value)), true
	}

	normalized := normalizeTemporalMapForRendering(typeName, value)
	if normalized == nil {
		return "", false
	}

	if truncUnit := strings.TrimSpace(fmt.Sprint(value["truncated"])); truncUnit != "" {
		normalized = applyTemporalTruncation(typeName, normalized, truncUnit)
	}

	switch typeName {
	case "date":
		t, ok := temporalDateTime(normalized, false)
		if !ok {
			return "", false
		}
		return t.Format("2006-01-02"), true
	case "localtime":
		t, ok := temporalDateTime(normalized, false)
		if !ok {
			return "", false
		}
		return formatTimeString(t, false), true
	case "time":
		t, ok := temporalDateTime(normalized, true)
		if !ok {
			return "", false
		}
		return formatTimeWithNamedTimezone(t, normalized), true
	case "localdatetime":
		t, ok := temporalDateTime(normalized, false)
		if !ok {
			return "", false
		}
		return formatDateTimeString(t, false), true
	case "datetime":
		t, ok := temporalDateTime(normalized, true)
		if !ok {
			return "", false
		}
		return formatDateTimeWithNamedTimezone(t, normalized), true
	default:
		return "", false
	}
}

func normalizeTemporalMapForRendering(typeName string, src map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range src {
		out[k] = v
	}

	if rawValue, ok := src["value"]; ok {
		if literal := strings.TrimSpace(fmt.Sprint(rawValue)); literal != "" {
			if parsed, ok := parseTemporalLiteralToMap(typeName, literal); ok {
				for k, v := range parsed {
					if _, exists := out[k]; !exists {
						out[k] = v
					}
				}
			}
		}
	}

	baseDate := time.Time{}
	hasBaseDate := false

	if embedded, ok := src["date"]; ok {
		switch typed := embedded.(type) {
		case map[string]any:
			if dateText, ok := renderTemporalValue(typed); ok {
				if t, err := time.Parse("2006-01-02", dateText); err == nil {
					baseDate = t
					hasBaseDate = true
					if _, ok := out["year"]; !ok {
						out["year"] = t.Year()
					}
					if _, ok := out["month"]; !ok {
						out["month"] = int(t.Month())
					}
					if _, ok := out["day"]; !ok {
						out["day"] = t.Day()
					}
				}
			}
		case string:
			if t, err := time.Parse("2006-01-02", strings.TrimSpace(typed)); err == nil {
				baseDate = t
				hasBaseDate = true
				if _, ok := out["year"]; !ok {
					out["year"] = t.Year()
				}
				if _, ok := out["month"]; !ok {
					out["month"] = int(t.Month())
				}
				if _, ok := out["day"]; !ok {
					out["day"] = t.Day()
				}
			}
		}
	}

	_, hasYear := out["year"]
	_, hasMonth := out["month"]
	_, hasDay := out["day"]
	if week, ok := mapInt(out, "week"); ok && !(hasYear && hasMonth && hasDay) {
		_, explicitYear := src["year"]
		year, hasYear := mapInt(out, "year")
		if !explicitYear {
			if hasBaseDate {
				year, _ = baseDate.ISOWeek()
				hasYear = true
			} else if y, m, d, ok := dateParts(out); ok {
				t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
				year, _ = t.ISOWeek()
				hasYear = true
			}
		}
		if hasYear {
			dayOfWeek, hasDOW := mapInt(out, "dayOfWeek")
			if !hasDOW || dayOfWeek < 1 || dayOfWeek > 7 {
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
			if dt, ok := isoWeekDate(year, week, dayOfWeek); ok {
				out["year"] = dt.Year()
				out["month"] = int(dt.Month())
				out["day"] = dt.Day()
			}
		}
	}

	return out
}

func dateParts(src map[string]any) (int, int, int, bool) {
	year, yOK := mapInt(src, "year")
	month, mOK := mapInt(src, "month")
	day, dOK := mapInt(src, "day")
	if !(yOK && mOK && dOK) {
		return 0, 0, 0, false
	}
	return year, month, day, true
}

func temporalDateTime(src map[string]any, withTimezone bool) (time.Time, bool) {
	year, hasYear := mapInt(src, "year")
	month, hasMonth := mapInt(src, "month")
	day, hasDay := mapInt(src, "day")
	hour, hasHour := mapInt(src, "hour")
	minute, hasMinute := mapInt(src, "minute")
	second, hasSecond := mapInt(src, "second")
	nanosecond, hasNano := mapInt(src, "nanosecond")

	if !hasYear {
		year = 0
	}
	if !hasMonth {
		month = 1
	}
	if !hasDay {
		day = 1
	}
	if !hasHour {
		hour = 0
	}
	if !hasMinute {
		minute = 0
	}
	if !hasSecond {
		second = 0
	}
	if !hasNano {
		nanosecond = 0
	}

	loc := time.UTC
	if withTimezone {
		if tzRaw, ok := src["timezone"]; ok {
			tz := strings.TrimSpace(fmt.Sprint(tzRaw))
			if tz != "" {
				if offset, err := parseOffsetSeconds(tz); err == nil {
					loc = time.FixedZone("", offset)
				} else if l, err := time.LoadLocation(tz); err == nil {
					loc = l
				}
			}
		}
	}

	if year == 0 && !hasYear {
		year = 1970
	}
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, loc), true
}

func applyTemporalTruncation(typeName string, src map[string]any, unit string) map[string]any {
	out := map[string]any{}
	for k, v := range src {
		out[k] = v
	}
	unit = strings.ToLower(strings.TrimSpace(unit))

	truncateTimeFields := func(target map[string]any, unit string) {
		switch unit {
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
			n, _ := mapInt(target, "nanosecond")
			target["nanosecond"] = (n / 1_000_000) * 1_000_000
		case "microsecond":
			n, _ := mapInt(target, "nanosecond")
			target["nanosecond"] = (n / 1_000) * 1_000
		}
	}

	switch unit {
	case "millennium", "century", "decade", "year", "quarter", "month", "week", "day":
		t, ok := temporalDateTime(out, strings.Contains(typeName, "time") && typeName != "localtime" && typeName != "localdatetime")
		if !ok {
			return out
		}
		switch unit {
		case "millennium":
			y := t.Year()
			t = time.Date((y/1000)*1000, 1, 1, 0, 0, 0, 0, t.Location())
		case "century":
			y := t.Year()
			t = time.Date((y/100)*100, 1, 1, 0, 0, 0, 0, t.Location())
		case "decade":
			y := t.Year()
			t = time.Date((y/10)*10, 1, 1, 0, 0, 0, 0, t.Location())
		case "year":
			t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
		case "quarter":
			m := ((int(t.Month())-1)/3)*3 + 1
			t = time.Date(t.Year(), time.Month(m), 1, 0, 0, 0, 0, t.Location())
		case "month":
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		case "week":
			y, w := t.ISOWeek()
			dt, ok := isoWeekDate(y, w, 1)
			if ok {
				t = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, t.Location())
			}
		case "day":
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		}
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
	case "hour", "minute", "second", "millisecond", "microsecond":
		truncateTimeFields(out, unit)
	}

	if rawOverrides, ok := out["truncate_overrides"]; ok {
		if overrides, ok := rawOverrides.(map[string]any); ok {
			for k, v := range overrides {
				if k == "nanosecond" {
					baseNanos, _ := mapInt(out, "nanosecond")
					if addNanos, ok := toInt(v); ok == nil {
						out[k] = baseNanos + addNanos
						continue
					}
				}
				out[k] = v
			}
			if unit == "week" {
				if dow, ok := mapInt(overrides, "dayOfWeek"); ok {
					if y, m, d, ok := dateParts(out); ok {
						base := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
						if dow >= 1 && dow <= 7 {
							base = base.AddDate(0, 0, dow-1)
							out["year"] = base.Year()
							out["month"] = int(base.Month())
							out["day"] = base.Day()
						}
					}
				}
			}
		}
	}

	delete(out, "truncated")
	delete(out, "truncate_overrides")
	return out
}

func isoWeekDate(year, week, dayOfWeek int) (time.Time, bool) {
	if week < 1 || week > 53 || dayOfWeek < 1 || dayOfWeek > 7 {
		return time.Time{}, false
	}
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	wd := int(jan4.Weekday())
	if wd == 0 {
		wd = 7
	}
	week1Monday := jan4.AddDate(0, 0, -(wd - 1))
	return week1Monday.AddDate(0, 0, (week-1)*7+(dayOfWeek-1)), true
}

func formatTimeWithNamedTimezone(t time.Time, src map[string]any) string {
	base := formatTimeString(t, false)
	tz := strings.TrimSpace(fmt.Sprint(src["timezone"]))
	if tz == "" {
		_, off := t.Zone()
		return base + formatOffsetString(off)
	}
	if strings.HasPrefix(tz, "+") || strings.HasPrefix(tz, "-") || strings.EqualFold(tz, "z") {
		if off, err := parseOffsetSeconds(tz); err == nil {
			return base + formatOffsetString(off)
		}
		return base + tz
	}
	_, off := t.Zone()
	return base + formatOffsetString(off)
}

func formatDateTimeWithNamedTimezone(t time.Time, src map[string]any) string {
	base := formatDateTimeString(t, false)
	tz := strings.TrimSpace(fmt.Sprint(src["timezone"]))
	if tz == "" {
		_, off := t.Zone()
		return base + formatOffsetString(off)
	}
	if strings.HasPrefix(tz, "+") || strings.HasPrefix(tz, "-") || strings.EqualFold(tz, "z") {
		if off, err := parseOffsetSeconds(tz); err == nil {
			return base + formatOffsetString(off)
		}
		return base + tz
	}
	if strings.Contains(tz, "/") {
		_, off := t.Zone()
		return base + formatOffsetString(off) + "[" + tz + "]"
	}
	_, off := t.Zone()
	return base + formatOffsetString(off)
}

func outcomeFromError(err error) string {
	if err == nil {
		return "ok"
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return "not_found"
	}
	if graph.IsKind(err, graph.ErrKindConflict) {
		return "conflict"
	}
	return "error"
}
