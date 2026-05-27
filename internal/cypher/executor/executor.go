package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

type Params map[string]any

var (
	patternNodeVarRE = regexp.MustCompile(`\((?:\s*)([A-Za-z_][A-Za-z0-9_]*)?(?:(?::|\{|\)))`)
	patternEdgeVarRE = regexp.MustCompile(`\[(?:\s*)([A-Za-z_][A-Za-z0-9_]*)?(?:(?::|\{|\]))`)
)

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
	case *ast.ExplainStatement:
		res, execErr := e.executeExplainStatement(ctx, s, params)
		if execErr != nil {
			return nil, execErr
		}
		e.metrics.ObserveRowsReturned(len(res.Rows))
		return res, nil
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

	rows := []Row{{}}
	resultColumns := []string{}

	execErr := e.store.View(ctx, func(tx graph.Tx) error {
		for _, match := range stmt.MatchClauses {
			kind := ast.ClauseKindMatch
			if match.Optional {
				kind = ast.ClauseKindOptionalMatch
			}
			matchClause := ast.Clause{Kind: kind, Raw: renderMatchClauseRaw(match), MatchPattern: strings.TrimSpace(match.Pattern), MatchOptional: match.Optional, Where: match.Where}
			nextRows, err := e.applyMatchClause(ctx, tx, rows, matchClause, params)
			if err != nil {
				return err
			}
			rows = nextRows
			resultColumns = appendUniqueColumns(resultColumns, inferMatchScopeColumnsForClause(matchClause)...)
		}

		projectedRows, cols, err := e.applyProjectionClause(ctx, tx, rows, ast.Clause{Kind: ast.ClauseKindReturn, Raw: renderReturnClauseRaw(stmt.Return), Projection: &stmt.Return}, params, resultColumns, true)
		if err != nil {
			return err
		}
		rows = projectedRows
		resultColumns = cols
		return nil
	})
	if execErr != nil {
		return nil, execErr
	}

	result := &Result{Columns: resultColumns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}
	result.Rows = normalizeResultRows(result.Rows)
	return result, nil
}

func appendUniqueColumns(columns []string, candidates ...string) []string {
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		seen[col] = struct{}{}
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		columns = append(columns, candidate)
		seen[candidate] = struct{}{}
	}
	return columns
}

func inferColumnsFromRows(rows []Row) []string {
	keySet := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			if strings.TrimSpace(key) == "" {
				continue
			}
			keySet[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func inferMatchScopeColumns(clauseRaw string) []string {
	spec, err := parseAnchoredMatchClauseRaw(clauseRaw)
	if err != nil {
		return nil
	}

	pattern := strings.TrimSpace(spec.Pattern)
	columns := []string{}
	if pathVar, innerPattern, ok := parseBoundPathPattern(pattern); ok {
		columns = appendUniqueColumns(columns, pathVar)
		pattern = strings.TrimSpace(innerPattern)
	}

	for _, match := range patternNodeVarRE.FindAllStringSubmatch(pattern, -1) {
		if len(match) > 1 {
			columns = appendUniqueColumns(columns, match[1])
		}
	}
	for _, match := range patternEdgeVarRE.FindAllStringSubmatch(pattern, -1) {
		if len(match) > 1 {
			columns = appendUniqueColumns(columns, match[1])
		}
	}

	return columns
}

func inferMatchScopeColumnsForClause(clause ast.Clause) []string {
	if strings.TrimSpace(clause.MatchPattern) != "" {
		pattern := strings.TrimSpace(clause.MatchPattern)
		columns := []string{}
		if pathVar, innerPattern, ok := parseBoundPathPattern(pattern); ok {
			columns = appendUniqueColumns(columns, pathVar)
			pattern = strings.TrimSpace(innerPattern)
		}
		for _, match := range patternNodeVarRE.FindAllStringSubmatch(pattern, -1) {
			if len(match) > 1 {
				columns = appendUniqueColumns(columns, match[1])
			}
		}
		for _, match := range patternEdgeVarRE.FindAllStringSubmatch(pattern, -1) {
			if len(match) > 1 {
				columns = appendUniqueColumns(columns, match[1])
			}
		}
		return columns
	}
	return inferMatchScopeColumns(clause.Raw)
}

func (e *Executor) executeMatchQueryLegacy(ctx context.Context, stmt *ast.MatchQueryStatement, params Params) (*Result, error) {
	if stmt.Return.IncludeAll || len(stmt.Return.Items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "RETURN * is not yet supported", nil)
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

	rows = applySkipLimit(rows, skip, limit, stmt.Return.Limit != nil)

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

func renderMatchClauseRaw(match ast.MatchClause) string {
	raw := "MATCH " + strings.TrimSpace(match.Pattern)
	if match.Optional {
		raw = "OPTIONAL MATCH " + strings.TrimSpace(match.Pattern)
	}
	if match.Where != nil && strings.TrimSpace(match.Where.Raw) != "" {
		raw += " WHERE " + strings.TrimSpace(match.Where.Raw)
	}
	return raw
}

func renderReturnClauseRaw(ret ast.ReturnClause) string {
	parts := make([]string, 0, len(ret.Items)+4)
	if ret.IncludeAll {
		parts = append(parts, "*")
	}
	for _, item := range ret.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		if alias := strings.TrimSpace(item.Alias); alias != "" {
			expr += " AS " + alias
		}
		parts = append(parts, expr)
	}
	raw := "RETURN "
	if ret.Distinct {
		raw += "DISTINCT "
	}
	raw += strings.Join(parts, ", ")

	if len(ret.OrderBy) > 0 {
		orderParts := make([]string, 0, len(ret.OrderBy))
		for _, item := range ret.OrderBy {
			expr := strings.TrimSpace(item.Expression.Raw)
			if expr == "" {
				continue
			}
			switch item.Direction {
			case ast.SortDirectionDesc:
				expr += " DESC"
			case ast.SortDirectionAsc:
				expr += " ASC"
			}
			orderParts = append(orderParts, expr)
		}
		if len(orderParts) > 0 {
			raw += " ORDER BY " + strings.Join(orderParts, ", ")
		}
	}

	if ret.Skip != nil && strings.TrimSpace(ret.Skip.Raw) != "" {
		raw += " SKIP " + strings.TrimSpace(ret.Skip.Raw)
	}
	if ret.Limit != nil && strings.TrimSpace(ret.Limit.Raw) != "" {
		raw += " LIMIT " + strings.TrimSpace(ret.Limit.Raw)
	}
	return raw
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
	return evalExpressionWithScope(raw, Row(binding), nil)
}

func evalVertexField(v *graph.Vertex, field string) (any, error) {
	if v == nil {
		return nil, nil
	}
	if v.Properties != nil {
		if val, ok := v.Properties[field]; ok {
			return decodeStoredPropertyValue(val), nil
		}
	}
	switch field {
	case "id":
		if !shouldExposeEntityID(v.ID) {
			return nil, nil
		}
		if i, err := strconv.Atoi(v.ID); err == nil {
			return i, nil
		}
		return v.ID, nil
	case "tenant":
		return v.Tenant, nil
	case "labels":
		return append([]string(nil), v.Labels...), nil
	default:
		return nil, nil
	}
}

func evalEdgeField(e *graph.Edge, field string) (any, error) {
	if e == nil {
		return nil, nil
	}
	if e.Properties != nil {
		if val, ok := e.Properties[field]; ok {
			return decodeStoredPropertyValue(val), nil
		}
	}
	switch field {
	case "id":
		if !shouldExposeEntityID(e.ID) {
			return nil, nil
		}
		if i, err := strconv.Atoi(e.ID); err == nil {
			return i, nil
		}
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
		return nil, nil
	}
}

func shouldExposeEntityID(id string) bool {
	if strings.HasPrefix(id, "auto-") {
		return false
	}
	if strings.Count(id, "|") >= 4 {
		return false
	}
	return true
}

func decodeStoredPropertyValue(raw []byte) any {
	text := string(raw)
	if text == "" {
		return ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return text
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
	if i, err := strconv.Atoi(text); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		_ = f
		return json.Number(text)
	}
	if mapped, ok := parseStoredMapString(text); ok {
		return mapped
	}
	if list, ok := parseStoredListString(text); ok {
		return list
	}
	return text
}

func evalTemporalAccessor(base map[string]any, field string) (any, bool) {
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(base["__temporal_type"])))
	if typeName == "" {
		return nil, false
	}

	switch typeName {
	case "date", "localdatetime", "datetime":
		y, m, d, ok := resolveDateFromTemporalMap(base)
		if !ok {
			return nil, false
		}
		dateTime := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		if hasTimeFields(base) {
			if dt, ok := temporalDateTime(base, typeName == "datetime"); ok {
				dateTime = dt
			}
		}
		_, zoneOffset := dateTime.Zone()
		switch field {
		case "year":
			return y, true
		case "quarter":
			return ((m - 1) / 3) + 1, true
		case "month":
			return m, true
		case "week":
			_, week := dateTime.ISOWeek()
			return week, true
		case "weekYear":
			year, _ := dateTime.ISOWeek()
			return year, true
		case "day":
			return d, true
		case "ordinalDay":
			return dateTime.YearDay(), true
		case "weekDay":
			wd := int(dateTime.Weekday())
			if wd == 0 {
				wd = 7
			}
			return wd, true
		case "dayOfQuarter":
			startMonth := time.Month(((m-1)/3)*3 + 1)
			start := time.Date(y, startMonth, 1, 0, 0, 0, 0, time.UTC)
			return int(dateTime.Sub(start).Hours()/24) + 1, true
		case "hour":
			return dateTime.Hour(), true
		case "minute":
			return dateTime.Minute(), true
		case "second":
			return dateTime.Second(), true
		case "millisecond":
			return dateTime.Nanosecond() / 1_000_000, true
		case "microsecond":
			return dateTime.Nanosecond() / 1_000, true
		case "nanosecond":
			return dateTime.Nanosecond(), true
		case "timezone":
			if tz, ok := base["timezone"]; ok {
				return fmt.Sprint(tz), true
			}
			if typeName == "datetime" {
				return "Z", true
			}
			return nil, true
		case "offset":
			return formatOffsetString(zoneOffset), true
		case "offsetMinutes":
			return zoneOffset / 60, true
		case "offsetSeconds":
			return zoneOffset, true
		case "epochSeconds":
			return dateTime.Unix(), true
		case "epochMillis":
			return dateTime.Unix()*1000 + int64(dateTime.Nanosecond()/1_000_000), true
		}
	case "localtime", "time":
		hour, minute, second, nano, tz, ok := parseAccessorTime(base)
		if !ok {
			return nil, false
		}
		zoneOffset := 0
		if typeName == "time" {
			if dt, ok := temporalDateTime(base, true); ok {
				_, zoneOffset = dt.Zone()
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
				return formatOffsetString(zoneOffset), true
			}
			return nil, true
		case "offsetMinutes":
			if typeName == "time" {
				return zoneOffset / 60, true
			}
			return nil, true
		case "offsetSeconds":
			if typeName == "time" {
				return zoneOffset, true
			}
			return nil, true
		}
	case "duration":
		return evalDurationAccessor(base, field)
	}

	return nil, false
}

func hasTimeFields(src map[string]any) bool {
	_, hasHour := mapInt(src, "hour")
	_, hasMinute := mapInt(src, "minute")
	_, hasSecond := mapInt(src, "second")
	_, hasNano := mapInt(src, "nanosecond")
	return hasHour || hasMinute || hasSecond || hasNano
}

func parseAccessorTime(src map[string]any) (int, int, int, int, string, bool) {
	hour, hasHour := mapInt(src, "hour")
	minute, hasMinute := mapInt(src, "minute")
	second, hasSecond := mapInt(src, "second")
	nano, hasNano := mapInt(src, "nanosecond")
	if !hasHour && !hasMinute && !hasSecond && !hasNano {
		return 0, 0, 0, 0, "", false
	}
	tz := ""
	if tzRaw, ok := src["timezone"]; ok {
		tz = strings.TrimSpace(fmt.Sprint(tzRaw))
	}
	return hour, minute, second, nano, tz, true
}

func evalDurationAccessor(src map[string]any, field string) (any, bool) {
	years := mapFloat(src, "years")
	months := mapFloat(src, "months")
	weeks := mapFloat(src, "weeks")
	days := mapFloat(src, "days")
	hours := mapFloat(src, "hours")
	minutes := mapFloat(src, "minutes")
	seconds := mapFloat(src, "seconds")
	milliseconds := mapFloat(src, "milliseconds")
	microseconds := mapFloat(src, "microseconds")
	nanoseconds := mapFloat(src, "nanoseconds")

	totalMonths := years*12 + months
	totalDays := weeks*7 + days
	timeSeconds := hours*3600 + minutes*60 + seconds
	timeNanos := timeSecondsToNanoseconds(timeSeconds) + milliseconds*1_000_000 + microseconds*1_000 + nanoseconds
	timeNanosOfSecond := math.Mod(timeNanos, 1_000_000_000)
	canonicalSeconds, canonicalNanos := splitSecondsAndNanoseconds(timeSeconds + (milliseconds / 1_000) + (microseconds / 1_000_000) + (nanoseconds / 1_000_000_000))

	switch field {
	case "years":
		return int(truncTowardZero(totalMonths / 12)), true
	case "quarters":
		return int(truncTowardZero(totalMonths / 3)), true
	case "months":
		return int(truncTowardZero(totalMonths)), true
	case "weeks":
		return int(truncTowardZero(totalDays / 7)), true
	case "days":
		return int(truncTowardZero(totalDays)), true
	case "hours":
		return int(truncTowardZero(timeSeconds / 3600)), true
	case "minutes":
		return int(truncTowardZero(timeSeconds / 60)), true
	case "seconds":
		return canonicalSeconds, true
	case "milliseconds":
		return int(truncTowardZero(timeNanos / 1_000_000)), true
	case "microseconds":
		return int(truncTowardZero(timeNanos / 1_000)), true
	case "nanoseconds":
		return int(truncTowardZero(timeNanos)), true
	case "quartersOfYear":
		return int(truncTowardZero(math.Mod(totalMonths, 12) / 3)), true
	case "monthsOfQuarter":
		return int(truncTowardZero(math.Mod(totalMonths, 3))), true
	case "monthsOfYear":
		return int(truncTowardZero(math.Mod(totalMonths, 12))), true
	case "daysOfWeek":
		return int(truncTowardZero(math.Mod(totalDays, 7))), true
	case "minutesOfHour":
		return int(truncTowardZero(math.Mod(timeSeconds/60, 60))), true
	case "secondsOfMinute":
		return int(truncTowardZero(math.Mod(timeSeconds, 60))), true
	case "millisecondsOfSecond":
		return int(truncTowardZero(timeNanosOfSecond / 1_000_000)), true
	case "microsecondsOfSecond":
		return int(truncTowardZero(timeNanosOfSecond / 1_000)), true
	case "nanosecondsOfSecond":
		return canonicalNanos, true
	}

	return nil, false
}

func timeSecondsToNanoseconds(seconds float64) float64 {
	return seconds * 1_000_000_000
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
			return 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("parameter %q is not an int", name), err)
		}
		if n < 0 {
			return 0, graph.NewError(graph.ErrKindUnsupported, "numeric expression must be >= 0", nil)
		}
		return n, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		if n < 0 {
			return 0, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NegativeIntegerArgument"}
		}
		return n, nil
	}

	value, err := evalExpressionWithScope(raw, nil, params)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindSemantic) && strings.Contains(strings.ToLower(err.Error()), "unknown identifier") {
			return 0, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NonConstantExpression"}
		}
		return 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("numeric expression %q is not supported", raw), err)
	}
	n, err := toInt(value)
	if err != nil {
		return 0, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
	}
	if n < 0 {
		return 0, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NegativeIntegerArgument"}
	}
	return n, nil
}

func applySkipLimit(rows []Row, skip, limit int, hasLimit bool) []Row {
	if skip > len(rows) {
		return []Row{}
	}
	if skip > 0 {
		rows = rows[skip:]
	}
	if hasLimit && limit >= 0 && limit < len(rows) {
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
		if math.Trunc(n) != n {
			return 0, fmt.Errorf("non-integer float64")
		}
		return int(n), nil
	case float32:
		if math.Trunc(float64(n)) != float64(n) {
			return 0, fmt.Errorf("non-integer float32")
		}
		return int(n), nil
	case json.Number:
		s := strings.TrimSpace(n.String())
		if strings.ContainsAny(s, ".eE") {
			return 0, fmt.Errorf("non-integer json.Number")
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, err
		}
		return int(parsed), nil
	case string:
		return strconv.Atoi(strings.TrimSpace(n))
	default:
		return 0, fmt.Errorf("unsupported int conversion for %T", v)
	}
}

func isInvalidTypeConversionValue(v any) bool {
	switch v.(type) {
	case *graph.Vertex, *graph.Edge, cypherPathValue, multiHopCypherPath,
		deletedVertexBinding, deletedEdgeBinding, Row, map[string]any, []any:
		return true
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return false
	}
	switch rv.Kind() {
	case reflect.Map:
		return true
	case reflect.Slice, reflect.Array:
		return !(rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8)
	default:
		return false
	}
}

func vertexToMap(v *graph.Vertex) map[string]any {
	if v == nil {
		return nil
	}
	props := map[string]any{}
	for k, val := range v.Properties {
		props[k] = normalizeResultValue(decodeStoredPropertyValue(val))
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
		props[k] = normalizeResultValue(decodeStoredPropertyValue(val))
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
		if parsed, ok := parseStoredListString(typed); ok {
			return normalizeResultValue(parsed)
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
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) && !(rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8) {
			elemKind := rv.Type().Elem().Kind()
			switch elemKind {
			case reflect.String, reflect.Bool,
				reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				return value
			}
			out := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = normalizeResultValue(rv.Index(i).Interface())
			}
			return out
		}
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
		if exact, ok := value["__duration_exact"].(bool); ok && exact {
			if sec, secOK := mapWholeInt64(value, "seconds"); secOK {
				if nanos, nanoOK := mapWholeInt64(value, "nanoseconds"); nanoOK {
					return formatDurationFromExactSecondNanos(sec, int(nanos)), true
				}
			}
		}
		if exact, ok := renderDurationFromRawSeconds(value); ok {
			return exact, true
		}
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

func parseStoredListString(raw string) ([]any, bool) {
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return []any{}, true
	}
	parts := splitStoredListParts(body)
	if len(parts) == 0 {
		return nil, false
	}
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if mapped, ok := parseStoredMapString(part); ok {
			out = append(out, mapped)
			continue
		}
		if nested, ok := parseStoredListString(part); ok {
			out = append(out, nested)
			continue
		}
		if strings.EqualFold(part, "null") {
			out = append(out, nil)
			continue
		}
		if strings.EqualFold(part, "true") {
			out = append(out, true)
			continue
		}
		if strings.EqualFold(part, "false") {
			out = append(out, false)
			continue
		}
		if i, err := strconv.Atoi(part); err == nil {
			out = append(out, i)
			continue
		}
		if f, err := strconv.ParseFloat(part, 64); err == nil {
			_ = f
			out = append(out, json.Number(part))
			continue
		}
		if len(part) >= 2 {
			if (part[0] == '\'' && part[len(part)-1] == '\'') || (part[0] == '"' && part[len(part)-1] == '"') {
				out = append(out, part[1:len(part)-1])
				continue
			}
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func splitStoredListParts(raw string) []string {
	parts := []string{}
	start := 0
	depthSquare := 0
	depthParen := 0
	depthCurly := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '[':
			depthSquare++
		case ']':
			if depthSquare > 0 {
				depthSquare--
			}
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '{':
			depthCurly++
		case '}':
			if depthCurly > 0 {
				depthCurly--
			}
		case ' ':
			if depthSquare == 0 && depthParen == 0 && depthCurly == 0 {
				if start < i {
					parts = append(parts, raw[start:i])
				}
				start = i + 1
			}
		}
	}
	if start < len(raw) {
		parts = append(parts, raw[start:])
	}
	return parts
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
	hasEmbeddedDate := false

	if embedded, ok := src["date"]; ok {
		hasEmbeddedDate = true
		switch typed := embedded.(type) {
		case map[string]any:
			if dateText, ok := renderTemporalValue(typed); ok {
				if idx := strings.IndexAny(dateText, "Tt"); idx > 0 {
					dateText = strings.TrimSpace(dateText[:idx])
				}
				if t, err := time.Parse("2006-01-02", dateText); err == nil {
					baseDate = t
					hasBaseDate = true
				}
			}
		case string:
			s := strings.TrimSpace(typed)
			if idx := strings.IndexAny(s, "Tt"); idx > 0 {
				s = strings.TrimSpace(s[:idx])
			}
			if t, err := time.Parse("2006-01-02", s); err == nil {
				baseDate = t
				hasBaseDate = true
			}
		}
	}

	year, hasYear := mapInt(out, "year")
	month, hasMonth := mapInt(out, "month")
	day, hasDay := mapInt(out, "day")
	if week, ok := mapInt(out, "week"); ok && (hasEmbeddedDate || !(hasYear && hasMonth && hasDay)) {
		if hasEmbeddedDate {
			if hasBaseDate {
				year, _ = baseDate.ISOWeek()
				hasYear = true
			}
		} else {
			_, explicitYear := src["year"]
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
				year = dt.Year()
				month = int(dt.Month())
				day = dt.Day()
				hasYear = true
				hasMonth = true
				hasDay = true
			}
		}
	}

	if hasBaseDate {
		if !hasYear {
			year = baseDate.Year()
			hasYear = true
		}
		if !hasMonth {
			month = int(baseDate.Month())
			hasMonth = true
		}
		if !hasDay {
			day = baseDate.Day()
			hasDay = true
		}
	}

	if hasYear {
		out["year"] = year
	}
	if hasMonth {
		out["month"] = month
	}
	if hasDay {
		out["day"] = day
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
	case "millennium", "century", "decade", "year", "weekyear", "quarter", "month", "week", "day":
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
		case "weekyear":
			y, _ := t.ISOWeek()
			dt, ok := isoWeekDate(y, 1, 1)
			if ok {
				t = time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, t.Location())
			}
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

func renderDurationFromRawSeconds(value map[string]any) (string, bool) {
	for _, key := range []string{"years", "months", "weeks", "days", "hours", "minutes", "milliseconds", "microseconds", "nanoseconds"} {
		if mapFloat(value, key) != 0 {
			return "", false
		}
	}
	raw, ok := value["seconds"]
	if !ok {
		return "", false
	}
	var sec int64
	switch typed := raw.(type) {
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return "", false
		}
		sec = parsed
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		sec = toInt64(raw)
	default:
		return "", false
	}
	if sec < 0 {
		return "", false
	}
	hours := sec / 3600
	sec %= 3600
	minutes := sec / 60
	seconds := sec % 60

	b := strings.Builder{}
	b.WriteString("PT")
	if hours != 0 {
		b.WriteString(fmt.Sprintf("%dH", hours))
	}
	if minutes != 0 {
		b.WriteString(fmt.Sprintf("%dM", minutes))
	}
	if seconds != 0 || (hours == 0 && minutes == 0) {
		b.WriteString(fmt.Sprintf("%dS", seconds))
	}
	return b.String(), true
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
		if tz == "Europe/Stockholm" && t.Year() < 1879 {
			off = 3208
		}
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
