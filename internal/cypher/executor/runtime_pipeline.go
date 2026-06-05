package executor

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/physical"
	cypherruntime "github.com/paegun/vitaledge/internal/cypher/runtime"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
	"github.com/paegun/vitaledge/internal/graph"
)

var runtimeSimpleWhereRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|<>|<=|>=|<|>)\s*(.+?)\s*$`)
var runtimeStringWhereRE = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(STARTS\s+WITH|ENDS\s+WITH|CONTAINS)\s*(.+?)\s*$`)
var runtimeNullWhereRE = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(IS\s+NULL|IS\s+NOT\s+NULL)\s*$`)

type runtimeWhereAtom struct {
	leftName       string
	op             string
	rightAny       any
	rightParamName string
}

type runtimeWhereExpr struct {
	atoms      []runtimeWhereAtom
	useOrLogic bool
	logicToken string
}

func (e *Executor) tryExecuteViaRuntimePipeline(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, bool, error) {
	if e == nil || e.store == nil {
		return nil, false, nil
	}
	routeDecision := decideQueryRoute(
		stmt,
		map[string]any(params),
		e.enableRuntimePipelineDefault,
		supportsRuntimePipelineQuery(stmt),
	)
	if routeDecision.route != queryRouteRuntimePipeline {
		return nil, false, nil
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		return nil, false, err
	}
	logicalPlan := logical.Build(sem)
	physicalPlan := physical.Build(logicalPlan)
	engine := cypherruntime.New()
	tenant := tenantFromParams(params)

	var runtimeRes cypherruntime.ExecutionResult
	err = e.store.Update(ctx, func(tx graph.Tx) error {
		var execErr error
		runtimeRes, execErr = engine.ExecuteWithTx(ctx, cypherruntime.ExecutionContext{Plan: physicalPlan, Tenant: tenant, Params: map[string]any(params)}, tx)
		return execErr
	})
	if err != nil {
		return nil, true, err
	}

	if !runtimePipelineHasReturnClause(stmt) {
		return &Result{Stats: Stats{RowsReturned: 0}}, true, nil
	}

	rows := runtimeRowsToExecutorRows(runtimeRes.Rows)
	if filtered, applied := applyRuntimeWithWhereFilter(stmt, rows, params); applied {
		rows = filtered
	}
	resultColumns := inferColumnsFromRows(rows)
	return &Result{Columns: resultColumns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}, true, nil

	// write-only shape
}

func supportsRuntimePipelineQuery(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Unions) != 0 || len(stmt.Parts) != 1 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 1 || len(clauses) > 3 {
		return false
	}
	writeClause := clauses[0]
	if writeClause.Kind != ast.ClauseKindCreate && writeClause.Kind != ast.ClauseKindMerge {
		return false
	}
	if strings.TrimSpace(writeClause.MergeOnCreate) != "" || strings.TrimSpace(writeClause.MergeOnMatch) != "" {
		return false
	}
	if writeClause.Projection != nil || writeClause.Where != nil || strings.TrimSpace(writeClause.MatchPattern) != "" {
		return false
	}
	if strings.TrimSpace(writeClause.Raw) == "" {
		return false
	}
	if len(clauses) == 1 {
		return true
	}

	start := 1
	if len(clauses) == 3 {
		withClause := clauses[1]
		if !isSimpleRuntimeProjectionClause(withClause, ast.ClauseKindWith) {
			return false
		}
		start = 2
	}

	returnClause := clauses[start]
	if !isSimpleRuntimeProjectionClause(returnClause, ast.ClauseKindReturn) {
		return false
	}
	return true
}

func isSimpleRuntimeProjectionClause(clause ast.Clause, expected ast.ClauseKind) bool {
	if clause.Kind != expected || clause.Projection == nil {
		return false
	}
	projection := clause.Projection
	for _, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if !isSimpleRuntimeProjectionExpr(expr, true) {
			return false
		}
	}
	for _, order := range projection.OrderBy {
		expr := strings.TrimSpace(order.Expression.Raw)
		if !isRuntimeOrderByExpr(expr) {
			return false
		}
	}
	if projection.Skip != nil && !isSimpleNonNegativeIntegerExpr(projection.Skip.Raw) {
		return false
	}
	if projection.Limit != nil && !isSimpleNonNegativeIntegerExpr(projection.Limit.Raw) {
		return false
	}
	if clause.Where != nil && strings.TrimSpace(clause.Where.Raw) != "" {
		if expected != ast.ClauseKindWith || !isSimpleRuntimeWhereExpression(clause.Where.Raw) {
			return false
		}
	}
	return true
}

func isSimpleRuntimeProjectionExpr(raw string, allowStar bool) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if raw == "*" {
		return allowStar
	}
	if isRuntimeIdentifierPath(raw) {
		return true
	}
	if strings.HasPrefix(raw, "$") {
		return isIdentifier(strings.TrimSpace(raw[1:]))
	}
	if strings.EqualFold(raw, "null") || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") {
		return true
	}
	if isSimpleNumericLiteral(raw) {
		return true
	}
	if !isQuotedLiteral(raw) {
		return false
	}
	_, err := unquoteCypherString(raw)
	return err == nil
}

func isRuntimeIdentifierPath(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if !isIdentifier(part) {
			return false
		}
	}
	return true
}

func isRuntimeOrderByExpr(raw string) bool {
	raw = strings.TrimSpace(raw)
	if isRuntimeOrderByConstantExpr(stripRuntimeWhereOuterParens(raw)) {
		return true
	}
	if inner, ok := trimRuntimeWhereOuterParensOnce(raw); ok {
		// Parenthesized ORDER BY expressions remain restricted to constants/params.
		return isRuntimeOrderByConstantExpr(stripRuntimeWhereOuterParens(inner))
	}

	if isRuntimeIdentifierPath(raw) {
		return true
	}
	return false
}

func isRuntimeOrderByConstantExpr(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.EqualFold(raw, "null") || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") {
		return true
	}
	if strings.HasPrefix(raw, "$") {
		return isIdentifier(strings.TrimSpace(raw[1:]))
	}
	if isSimpleNumericLiteral(raw) {
		return true
	}
	if isQuotedLiteral(raw) {
		_, err := unquoteCypherString(raw)
		return err == nil
	}
	return false
}

func isSimpleRuntimeWhereExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	expr, ok := parseRuntimeWhereExpr(raw)
	if !ok || len(expr.atoms) == 0 {
		return false
	}
	return true
}

func runtimePipelineHasReturnClause(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	return clauses[len(clauses)-1].Kind == ast.ClauseKindReturn
}

func runtimeRowsToExecutorRows(rows []map[string]any) []Row {
	if len(rows) == 0 {
		return nil
	}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		next := Row{}
		for key, value := range row {
			next[key] = value
		}
		out = append(out, next)
	}
	return out
}

func isSimpleNonNegativeIntegerExpr(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func applyRuntimeWithWhereFilter(stmt *ast.QueryStatement, rows []Row, params Params) ([]Row, bool) {
	withClause, ok := runtimeWithClause(stmt)
	if !ok || withClause.Where == nil || strings.TrimSpace(withClause.Where.Raw) == "" {
		return rows, false
	}
	whereRaw := strings.TrimSpace(withClause.Where.Raw)
	whereExpr, ok := parseRuntimeWhereExpr(whereRaw)
	if !ok || len(whereExpr.atoms) == 0 {
		return rows, false
	}

	returnClause, ok := runtimeReturnClause(stmt)
	if !ok || returnClause.Projection == nil {
		return rows, false
	}
	resolvedAtoms := make([]struct {
		returnKey string
		op        string
		right     any
	}, 0, len(whereExpr.atoms))
	for _, atom := range whereExpr.atoms {
		returnKey, ok := runtimeReturnKeyForName(*returnClause.Projection, atom.leftName)
		if !ok {
			return rows, false
		}
		rightValue, ok := runtimeWhereValue(atom, params)
		if !ok {
			return rows, false
		}
		resolvedAtoms = append(resolvedAtoms, struct {
			returnKey string
			op        string
			right     any
		}{returnKey: returnKey, op: atom.op, right: rightValue})
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		matches := !whereExpr.useOrLogic
		for _, atom := range resolvedAtoms {
			leftValue := row[atom.returnKey]
			atomMatches := runtimeCompareWhere(leftValue, atom.op, atom.right)
			if whereExpr.useOrLogic {
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
		if matches {
			out = append(out, row)
		}
	}
	return out, true
}

func parseRuntimeWhereAtoms(raw string) ([]runtimeWhereAtom, bool) {
	expr, ok := parseRuntimeWhereExpr(raw)
	if !ok {
		return nil, false
	}
	return expr.atoms, true
}

func parseRuntimeWhereExpr(raw string) (runtimeWhereExpr, bool) {
	raw = stripRuntimeWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return runtimeWhereExpr{}, false
	}

	hasAnd := containsStandaloneBooleanToken(raw, "AND")
	hasOr := containsStandaloneBooleanToken(raw, "OR")
	if hasAnd && hasOr {
		return runtimeWhereExpr{}, false
	}

	if hasOr {
		return parseRuntimeCompoundWhereExpr(raw, "OR")
	}
	if hasAnd {
		return parseRuntimeCompoundWhereExpr(raw, "AND")
	}

	m := runtimeSimpleWhereRE.FindStringSubmatch(raw)
	if len(m) != 4 {
		m = runtimeStringWhereRE.FindStringSubmatch(raw)
	}
	if len(m) != 4 {
		nullMatch := runtimeNullWhereRE.FindStringSubmatch(raw)
		if len(nullMatch) == 3 {
			lhs := strings.TrimSpace(nullMatch[1])
			op := normalizeRuntimeWhereOp(nullMatch[2])
			if lhs == "" || !isIdentifier(lhs) {
				return runtimeWhereExpr{}, false
			}
			return runtimeWhereExpr{atoms: []runtimeWhereAtom{{leftName: lhs, op: op}}}, true
		}
		return runtimeWhereExpr{}, false
	}
	lhs := strings.TrimSpace(m[1])
	op := normalizeRuntimeWhereOp(m[2])
	rhs := strings.TrimSpace(m[3])
	if lhs == "" || rhs == "" {
		return runtimeWhereExpr{}, false
	}
	if !isIdentifier(lhs) {
		return runtimeWhereExpr{}, false
	}
	if strings.HasPrefix(rhs, "$") {
		if !isIdentifier(rhs[1:]) {
			return runtimeWhereExpr{}, false
		}
		return runtimeWhereExpr{atoms: []runtimeWhereAtom{{leftName: lhs, op: op, rightParamName: rhs[1:]}}}, true
	}
	if isRuntimeStringWhereOp(op) {
		if !isQuotedLiteral(rhs) {
			return runtimeWhereExpr{}, false
		}
	}
	if isIdentifier(rhs) {
		return runtimeWhereExpr{}, false
	}
	rightValue, ok := parseRuntimeWhereLiteralValue(rhs)
	if !ok {
		return runtimeWhereExpr{}, false
	}
	return runtimeWhereExpr{atoms: []runtimeWhereAtom{{leftName: lhs, op: op, rightAny: rightValue}}}, true
}

func parseRuntimeCompoundWhereExpr(raw string, token string) (runtimeWhereExpr, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return runtimeWhereExpr{}, false
	}
	parts := splitRuntimeWhereBoolean(raw, token)
	if len(parts) == 0 {
		return runtimeWhereExpr{}, false
	}
	atoms := make([]runtimeWhereAtom, 0, len(parts))
	for _, part := range parts {
		subExpr, ok := parseRuntimeWhereExpr(part)
		if !ok || len(subExpr.atoms) == 0 {
			return runtimeWhereExpr{}, false
		}
		if subExpr.logicToken != "" && !strings.EqualFold(subExpr.logicToken, token) {
			return runtimeWhereExpr{}, false
		}
		atoms = append(atoms, subExpr.atoms...)
	}
	return runtimeWhereExpr{atoms: atoms, useOrLogic: strings.EqualFold(token, "OR"), logicToken: token}, true
}

func splitRuntimeWhereBoolean(raw string, token string) []string {
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
			if runtimeQuoteIsEscaped(raw, i) {
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
			if runtimeQuoteIsEscaped(raw, i) {
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

func containsStandaloneBooleanToken(raw string, token string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	inSingle := false
	inDouble := false
	parenDepth := 0
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if runtimeQuoteIsEscaped(raw, i) {
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
			if runtimeQuoteIsEscaped(raw, i) {
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
				return false
			}
			parenDepth--
			continue
		}
		if parenDepth != 0 {
			continue
		}
		if matchBooleanTokenAt(raw, i, token) {
			return true
		}
	}
	return !inSingle && !inDouble && parenDepth == 0 && false
}

func stripRuntimeWhereOuterParens(raw string) string {
	raw = strings.TrimSpace(raw)
	for {
		inner, ok := trimRuntimeWhereOuterParensOnce(raw)
		if !ok {
			return raw
		}
		raw = strings.TrimSpace(inner)
	}
}

func trimRuntimeWhereOuterParensOnce(raw string) (string, bool) {
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
			if runtimeQuoteIsEscaped(raw, i) {
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
			if runtimeQuoteIsEscaped(raw, i) {
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
		if isIdentifierRune(prev) {
			return false
		}
	}
	if idx+len(token) < len(raw) {
		next := rune(raw[idx+len(token)])
		if isIdentifierRune(next) {
			return false
		}
	}
	return true
}

func isIdentifierRune(r rune) bool {
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

func runtimeWithClause(stmt *ast.QueryStatement) (ast.Clause, bool) {
	if stmt == nil || len(stmt.Parts) != 1 {
		return ast.Clause{}, false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) != 3 || clauses[1].Kind != ast.ClauseKindWith {
		return ast.Clause{}, false
	}
	return clauses[1], true
}

func runtimeReturnClause(stmt *ast.QueryStatement) (ast.Clause, bool) {
	if stmt == nil || len(stmt.Parts) != 1 {
		return ast.Clause{}, false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 2 {
		return ast.Clause{}, false
	}
	last := clauses[len(clauses)-1]
	if last.Kind != ast.ClauseKindReturn {
		return ast.Clause{}, false
	}
	return last, true
}

func runtimeReturnKeyForName(ret ast.ReturnClause, name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	for _, item := range ret.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr != name {
			continue
		}
		alias := strings.TrimSpace(item.Alias)
		if alias != "" {
			return alias, true
		}
		return expr, true
	}
	if ret.IncludeAll {
		return name, true
	}
	return "", false
}

func parseRuntimeWhereLiteralValue(raw string) (any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if isQuotedLiteral(raw) {
		value, err := unquoteCypherString(raw)
		if err != nil {
			return nil, false
		}
		return value, true
	}
	if isSimpleBooleanLiteral(raw) {
		return strings.EqualFold(raw, "true"), true
	}
	if isSimpleNumericLiteral(raw) {
		if strings.Contains(raw, ".") {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, false
			}
			return v, true
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, false
		}
		return v, true
	}
	return raw, true
}

func runtimeWhereValue(atom runtimeWhereAtom, params Params) (any, bool) {
	if atom.rightParamName != "" {
		name := strings.TrimSpace(atom.rightParamName)
		if !isIdentifier(name) || params == nil {
			return nil, false
		}
		value, ok := params[name]
		return value, ok
	}
	return atom.rightAny, true
}

func runtimeCompareWhere(left any, op string, right any) bool {
	switch op {
	case "IS NULL":
		return left == nil
	case "IS NOT NULL":
		return left != nil
	}
	if isRuntimeStringWhereOp(op) {
		return runtimeCompareStringWhere(left, op, right)
	}
	cmp := runtimeCompareScalar(left, right)
	switch op {
	case "=":
		return cmp == 0
	case "!=", "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

func runtimeCompareStringWhere(left any, op string, right any) bool {
	leftString, ok := runtimeStringValue(left)
	if !ok {
		return false
	}
	rightString, ok := runtimeStringValue(right)
	if !ok {
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

func runtimeCompareScalar(left, right any) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if ln, lok := runtimeNumeric(left); lok {
		if rn, rok := runtimeNumeric(right); rok {
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
	ls := fmt.Sprint(left)
	rs := fmt.Sprint(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

func runtimeNumeric(value any) (float64, bool) {
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
		v, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return v, true
	default:
		return 0, false
	}
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

func isRuntimeStringWhereOp(op string) bool {
	switch normalizeRuntimeWhereOp(op) {
	case "STARTS WITH", "ENDS WITH", "CONTAINS":
		return true
	default:
		return false
	}
}

func normalizeRuntimeWhereOp(op string) string {
	fields := strings.Fields(strings.TrimSpace(op))
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(strings.Join(fields, " "))
}

func isIdentifier(value string) bool {
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

func isQuotedLiteral(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return false
	}
	return (value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')
}

func runtimeQuoteIsEscaped(raw string, idx int) bool {
	if idx <= 0 || idx > len(raw) {
		return false
	}
	backslashes := 0
	for i := idx - 1; i >= 0 && raw[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func isSimpleBooleanLiteral(value string) bool {
	value = strings.TrimSpace(value)
	return strings.EqualFold(value, "true") || strings.EqualFold(value, "false")
}

func isSimpleNumericLiteral(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return true
	}
	return false
}
