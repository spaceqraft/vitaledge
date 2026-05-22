package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
)

type Params map[string]any

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
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("statement kind %s not yet executable", stmt.Kind()), nil)
	}
}

func (e *Executor) executeMatchQuery(ctx context.Context, stmt *ast.MatchQueryStatement, params Params) (*Result, error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "match query statement is required", nil)
	}
	if len(stmt.MatchClauses) != 1 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "only single MATCH clause is currently supported", nil)
	}

	match := stmt.MatchClauses[0]
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
		kind := ast.ClauseKindMatch
		clauseRaw := "MATCH " + match.Pattern
		if match.Optional {
			kind = ast.ClauseKindOptionalMatch
			clauseRaw = "OPTIONAL MATCH " + match.Pattern
		}
		if match.Where != nil && strings.TrimSpace(match.Where.Raw) != "" {
			clauseRaw += " WHERE " + strings.TrimSpace(match.Where.Raw)
		}

		matchRows, err := e.applyMatchClause(ctx, tx, []Row{{}}, ast.Clause{Kind: kind, Raw: clauseRaw}, params)
		if err != nil {
			return err
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
	if arg, ok := parseFunctionCall(raw, "labels"); ok {
		return evalLabelsFunction(arg, binding)
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
	case map[string]any:
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
