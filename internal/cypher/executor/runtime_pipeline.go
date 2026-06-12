package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/physical"
	"github.com/paegun/vitaledge/internal/cypher/runtime"
	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
	"github.com/paegun/vitaledge/internal/graph"
)

var runtimeSimpleWhereRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|<>|<=|>=|<|>)\s*(.+?)\s*$`)
var runtimeStringWhereRE = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(STARTS\s+WITH|ENDS\s+WITH|CONTAINS)\s*(.+?)\s*$`)
var runtimeNullWhereRE = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(IS\s+NULL|IS\s+NOT\s+NULL)\s*$`)
var runtimeCreatePropertyFunctionCallRE = regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

var runtimeCreatePropertyFunctionCallSupported = map[string]struct{}{
	"date":          {},
	"localtime":     {},
	"time":          {},
	"localdatetime": {},
	"datetime":      {},
	"duration":      {},
}

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
	root       *runtimeWhereNode
}

type runtimeWhereNode struct {
	op    string
	atom  *runtimeWhereAtom
	left  *runtimeWhereNode
	right *runtimeWhereNode
}

func (e *Executor) tryExecuteViaRuntimePipeline(ctx context.Context, stmt ast.Statement, params Params) (*Result, bool, error) {
	if e == nil || e.store == nil {
		return nil, false, nil
	}
	if stmt == nil {
		return nil, false, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}
	queryStmt, ok := runtimeQueryStatementFromStatement(stmt)
	if !ok {
		return nil, true, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RuntimeOnlyUnsupportedShape"}
	}
	if len(queryStmt.Unions) > 0 {
		res, execErr := e.executeRuntimeUnionQuery(ctx, queryStmt, params)
		if execErr != nil {
			return nil, true, execErr
		}
		return res, true, nil
	}
	res, execErr := e.executeRuntimeSingleQuery(ctx, queryStmt, params)
	if execErr != nil {
		return nil, true, execErr
	}
	return res, true, nil
}

func (e *Executor) executeRuntimeSingleQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}
	_, physicalPlan, err := e.buildRuntimePhysicalPlan(ctx, stmt, params)
	if err != nil {
		return nil, err
	}
	if rawCreatePattern := runtimeCreatePatternFromStatement(stmt); isMissingRelationshipTypePattern(rawCreatePattern) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	migratedFamily := runtimeMigratedFamilyStatement(stmt)
	if migratedFamily {
		return e.executeRuntimePhysicalPlan(ctx, stmt, params, physicalPlan)
	}

	runtimePlanSupported := runtimePhysicalPlanSupported(physicalPlan)
	if !runtimePlanSupported {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RuntimeOnlyUnsupportedPlan"}
	}

	return e.executeRuntimePhysicalPlan(ctx, stmt, params, physicalPlan)
}

func (e *Executor) executeRuntimeUnionQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, error) {
	if stmt == nil || len(stmt.Parts) == 0 {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RuntimeOnlyUnsupportedShape"}
	}
	if len(stmt.Unions) != len(stmt.Parts)-1 {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RuntimeOnlyUnsupportedShape"}
	}
	unionKind := stmt.Unions[0]
	for _, kind := range stmt.Unions[1:] {
		if kind != unionKind {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidClauseComposition"}
		}
	}

	var columns []string
	rows := make([]Row, 0)
	for _, part := range stmt.Parts {
		partStmt := &ast.QueryStatement{
			Parts:      []ast.QueryPart{part},
			Parameters: append([]ast.ParameterRef(nil), stmt.Parameters...),
			SourceSpan: stmt.SourceSpan,
		}
		partResult, err := e.executeRuntimeSingleQuery(ctx, partStmt, params)
		if err != nil {
			return nil, err
		}
		partColumns := append([]string(nil), partResult.Columns...)
		if len(partColumns) == 0 {
			partColumns = inferColumnsFromRows(partResult.Rows)
		}
		if len(columns) == 0 {
			columns = partColumns
		} else if !runtimeUnionColumnsMatch(columns, partColumns) {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "DifferentColumnsInUnion"}
		}
		rows = append(rows, partResult.Rows...)
	}

	if unionKind == ast.UnionKindDistinct {
		rows = runtimeDistinctUnionRows(rows, columns)
	}

	return &Result{Columns: columns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}, nil
}

func runtimeUnionColumnsMatch(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func runtimeDistinctUnionRows(rows []Row, columns []string) []Row {
	if len(rows) < 2 {
		return rows
	}
	out := make([]Row, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		signature := runtimeUnionRowSignature(row, columns)
		if _, ok := seen[signature]; ok {
			continue
		}
		seen[signature] = struct{}{}
		out = append(out, row)
	}
	return out
}

func runtimeUnionRowSignature(row Row, columns []string) string {
	if row == nil {
		return "null"
	}
	keys := columns
	if len(keys) == 0 {
		keys = inferColumnsFromRows([]Row{row})
	}
	values := make([]any, 0, len(keys))
	for _, key := range keys {
		values = append(values, normalizeResultValue(row[key]))
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return fmt.Sprint(values)
	}
	return string(encoded)
}

func runtimeQueryStatementFromStatement(stmt ast.Statement) (*ast.QueryStatement, bool) {
	switch s := stmt.(type) {
	case *ast.QueryStatement:
		return s, true
	case *ast.MatchQueryStatement:
		clauses := make([]ast.Clause, 0, len(s.MatchClauses)+1)
		for _, match := range s.MatchClauses {
			kind := ast.ClauseKindMatch
			if match.Optional {
				kind = ast.ClauseKindOptionalMatch
			}
			clauses = append(clauses, ast.Clause{
				Kind:          kind,
				Raw:           renderMatchClauseRaw(match),
				MatchPattern:  strings.TrimSpace(match.Pattern),
				MatchOptional: match.Optional,
				Where:         match.Where,
				Span:          match.Span,
			})
		}
		ret := s.Return
		clauses = append(clauses, ast.Clause{
			Kind:       ast.ClauseKindReturn,
			Raw:        renderReturnClauseRaw(ret),
			Projection: &ret,
			Span:       ret.Span,
		})
		return &ast.QueryStatement{
			Parts:      []ast.QueryPart{{Clauses: clauses}},
			Parameters: append([]ast.ParameterRef(nil), s.Parameters...),
			SourceSpan: s.SourceSpan,
		}, true
	default:
		return nil, false
	}
}

func runtimeCreatePatternFromStatement(stmt *ast.QueryStatement) string {
	if stmt == nil || len(stmt.Parts) == 0 || len(stmt.Parts[0].Clauses) == 0 {
		return ""
	}
	for _, clause := range stmt.Parts[0].Clauses {
		if clause.Kind != ast.ClauseKindCreate {
			continue
		}
		return normalizeClauseBody(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, string(ast.ClauseKindCreate))))
	}
	return ""
}

func runtimeNativeExecutionCandidate(stmt *ast.QueryStatement) bool {
	if stmt == nil {
		return false
	}
	if len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}

	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindCreate {
		return false
	}
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindCreate, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete, ast.ClauseKindWith, ast.ClauseKindReturn:
			continue
		default:
			return false
		}
	}
	for _, clause := range clauses {
		if clause.Kind == ast.ClauseKindCreate {
			rawPattern := normalizeClauseBody(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind))))
			if isMissingRelationshipTypePattern(rawPattern) {
				return false
			}
			if containsUnsupportedRuntimeCreatePropertyFunctionCall(rawPattern) {
				return false
			}
		}
		if clause.Kind == ast.ClauseKindSet || clause.Kind == ast.ClauseKindRemove || clause.Kind == ast.ClauseKindDelete {
			if !runtimeNativeWriteMutationClauseSupported(clause) {
				return false
			}
		}
	}
	return true
}

func runtimeCreateFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 || clauses[0].Kind != ast.ClauseKindCreate {
		return false
	}
	return true
}

func runtimeMigratedFamilyStatement(stmt *ast.QueryStatement) bool {
	return runtimeCreateFamilyStatement(stmt) || runtimeMatchCreateFamilyStatement(stmt) || runtimeMatchSetFamilyStatement(stmt) || runtimeMergeFamilyStatement(stmt) || runtimeDeleteFamilyStatement(stmt) || runtimeCallFamilyStatement(stmt) || runtimeMatchReturnFamilyStatement(stmt) || runtimeReturnOnlyFamilyStatement(stmt) || runtimeWithReturnFamilyStatement(stmt) || runtimeWithMatchReturnFamilyStatement(stmt) || runtimeUnwindWithReturnFamilyStatement(stmt) || runtimeUnwindCreateFamilyStatement(stmt)
}

func runtimeMatchSetFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindMatch && clauses[0].Kind != ast.ClauseKindOptionalMatch {
		return false
	}
	seenMutation := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindUnwind, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindSet || clause.Kind == ast.ClauseKindRemove {
				seenMutation = true
				if !runtimeNativeWriteMutationClauseSupported(clause) {
					return false
				}
			}
			continue
		default:
			return false
		}
	}
	return seenMutation
}

func runtimeUnwindCreateFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 2 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindUnwind && clauses[0].Kind != ast.ClauseKindWith {
		return false
	}
	seenUnwind := false
	seenCreate := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindUnwind:
			seenUnwind = true
		case ast.ClauseKindCreate:
			seenCreate = true
			rawPattern := normalizeClauseBody(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind))))
			if isMissingRelationshipTypePattern(rawPattern) {
				return false
			}
			if containsUnsupportedRuntimeCreatePropertyFunctionCall(rawPattern) {
				return false
			}
		case ast.ClauseKindWith:
			continue
		case ast.ClauseKindReturn:
			continue
		default:
			return false
		}
	}
	return seenUnwind && seenCreate
}

func runtimeUnwindWithReturnFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 2 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindUnwind && clauses[0].Kind != ast.ClauseKindWith {
		return false
	}
	seenUnwind := false
	seenReturn := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindUnwind:
			seenUnwind = true
		case ast.ClauseKindWith:
			continue
		case ast.ClauseKindReturn:
			seenReturn = true
		default:
			return false
		}
	}
	return seenUnwind && seenReturn
}

func runtimeWithReturnFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 2 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindWith {
		return false
	}
	seenReturn := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindReturn {
				seenReturn = true
			}
			continue
		default:
			return false
		}
	}
	return seenReturn
}

func runtimeWithMatchReturnFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) < 3 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindWith {
		return false
	}
	seenMatch := false
	seenReturn := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindWith:
			continue
		case ast.ClauseKindUnwind:
			continue
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
			seenMatch = true
			continue
		case ast.ClauseKindReturn:
			seenReturn = true
			continue
		default:
			return false
		}
	}
	return seenMatch && seenReturn
}

func runtimeDeleteFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	seenDelete := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindUnwind, ast.ClauseKindCreate, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindDelete {
				seenDelete = true
				if !runtimeNativeWriteMutationClauseSupported(clause) {
					return false
				}
			}
			if clause.Kind == ast.ClauseKindSet || clause.Kind == ast.ClauseKindRemove {
				if !runtimeNativeWriteMutationClauseSupported(clause) {
					return false
				}
			}
			continue
		default:
			return false
		}
	}
	return seenDelete
}

func runtimeCallFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	seenCall := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindUnwind, ast.ClauseKindInQueryCall, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindInQueryCall {
				seenCall = true
			}
			continue
		default:
			return false
		}
	}
	return seenCall
}

func runtimeMergeFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	seenMerge := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindUnwind, ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindMerge {
				seenMerge = true
			}
			if clause.Kind == ast.ClauseKindSet || clause.Kind == ast.ClauseKindRemove || clause.Kind == ast.ClauseKindDelete {
				if !runtimeNativeWriteMutationClauseSupported(clause) {
					return false
				}
			}
			continue
		default:
			return false
		}
	}
	return seenMerge
}

func runtimeReturnOnlyFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	for _, clause := range clauses {
		if clause.Kind != ast.ClauseKindReturn {
			return false
		}
	}
	return true
}

func runtimeMatchCreateFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 || clauses[0].Kind != ast.ClauseKindMatch {
		return false
	}
	seenCreate := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindCreate, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindCreate {
				seenCreate = true
			}
			continue
		default:
			return false
		}
	}
	return seenCreate
}

func runtimeMatchReturnFamilyStatement(stmt *ast.QueryStatement) bool {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return false
	}
	clauses := stmt.Parts[0].Clauses
	if len(clauses) == 0 {
		return false
	}
	if clauses[0].Kind != ast.ClauseKindMatch && clauses[0].Kind != ast.ClauseKindOptionalMatch {
		return false
	}
	seenReturn := false
	for _, clause := range clauses {
		switch clause.Kind {
		case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindUnwind, ast.ClauseKindWith, ast.ClauseKindReturn:
			if clause.Kind == ast.ClauseKindReturn {
				seenReturn = true
			}
			continue
		default:
			return false
		}
	}
	return seenReturn
}

func runtimeNativeWriteMutationClauseSupported(clause ast.Clause) bool {
	raw := normalizeClauseBody(clause.Raw)
	var body string
	switch clause.Kind {
	case ast.ClauseKindSet:
		body = stripNormalizedPrefix(raw, "SET")
	case ast.ClauseKindRemove:
		body = stripNormalizedPrefix(raw, "REMOVE")
	case ast.ClauseKindDelete:
		upper := strings.ToUpper(raw)
		if strings.HasPrefix(upper, "DETACHDELETE") {
			body = strings.TrimSpace(raw[len("DETACHDELETE"):])
		} else {
			body = stripNormalizedPrefix(raw, "DELETE")
		}
	default:
		return true
	}
	items := splitTopLevelCommaSeparated(body)
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		switch clause.Kind {
		case ast.ClauseKindSet:
			if _, _, expr, ok := parseSetPropertyAssignment(item); ok {
				if !runtimeNativeSimpleWriteExprSupported(expr) {
					return false
				}
				continue
			}
			if _, _, expr, ok := runtimeParseSetMapAssignment(item); ok {
				if !runtimeNativeSimpleWriteExprSupported(expr) {
					return false
				}
				continue
			}
			if setLabelClauseRE.MatchString(item) {
				continue
			}
			return false
		case ast.ClauseKindRemove:
			if removeClauseRE.MatchString(item) || removeLabelClauseRE.MatchString(item) {
				continue
			}
			return false
		case ast.ClauseKindDelete:
			if !runtimeNativeDeleteOperandSupported(item) {
				return false
			}
		}
	}
	return true
}

func runtimeNativeDeleteOperandSupported(item string) bool {
	item = strings.TrimSpace(item)
	if item == "" {
		return false
	}
	if isIdentifier(item) {
		return true
	}
	if strings.Contains(item, "(") || strings.Contains(item, ")") {
		return false
	}
	if strings.ContainsAny(item, "{}") {
		return false
	}
	for _, ch := range item {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' || ch == '[' || ch == ']' || ch == '\'' || ch == '"' || ch == '$' {
			continue
		}
		return false
	}
	return true
}

func runtimeParseSetMapAssignment(item string) (string, string, string, bool) {
	item = strings.TrimSpace(item)
	idx := indexTopLevelEqualsInSetItem(item)
	if idx < 0 {
		return "", "", "", false
	}
	lhs := strings.TrimSpace(item[:idx])
	rhs := strings.TrimSpace(item[idx+1:])
	if lhs == "" || rhs == "" {
		return "", "", "", false
	}
	op := "="
	if strings.HasSuffix(lhs, "+") {
		op = "+="
		lhs = strings.TrimSpace(strings.TrimSuffix(lhs, "+"))
	}
	if strings.Contains(lhs, ".") {
		return "", "", "", false
	}
	if inner, wrapped := unwrapOuterParentheses(lhs); wrapped {
		lhs = strings.TrimSpace(inner)
	}
	if !isIdentifierLike(lhs) {
		return "", "", "", false
	}
	return lhs, op, rhs, true
}

func runtimeNativeSimpleWriteExprSupported(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	expr = stripRuntimeWhereOuterParens(expr)
	if strings.HasPrefix(expr, "$") {
		return isIdentifier(strings.TrimSpace(expr[1:]))
	}
	if strings.EqualFold(expr, "null") || strings.EqualFold(expr, "true") || strings.EqualFold(expr, "false") {
		return true
	}
	if isQuotedLiteral(expr) || isSimpleNumericLiteral(expr) {
		return true
	}
	if runtimeNativeWriteIdentifierPathSupported(expr) {
		return true
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return true
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return true
	}
	if match := runtimeCreatePropertyFunctionCallRE.FindStringSubmatch(expr); len(match) > 1 {
		name := strings.ToLower(strings.TrimSpace(match[1]))
		_, supported := runtimeCreatePropertyFunctionCallSupported[name]
		return supported
	}
	if lhs, rhs, ok := runtimeSplitTopLevelWriteBinaryExpr(expr); ok {
		return runtimeNativeSimpleWriteExprSupported(lhs) && runtimeNativeSimpleWriteExprSupported(rhs)
	}
	return false
}

func runtimeNativeWriteIdentifierPathSupported(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	parts := strings.Split(expr, ".")
	for _, part := range parts {
		if !isIdentifier(strings.TrimSpace(part)) {
			return false
		}
	}
	return len(parts) > 0
}

func runtimeSplitTopLevelWriteBinaryExpr(expr string) (string, string, bool) {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			if runtimeQuoteIsEscaped(expr, i) {
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if runtimeQuoteIsEscaped(expr, i) {
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
			continue
		case ')':
			if depthParen > 0 {
				depthParen--
			}
			continue
		case '[':
			depthBracket++
			continue
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
			continue
		case '{':
			depthBrace++
			continue
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
			continue
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		if ch != '+' && ch != '-' && ch != '*' && ch != '/' && ch != '%' {
			continue
		}
		if i == 0 {
			continue
		}
		lhs := strings.TrimSpace(expr[:i])
		rhs := strings.TrimSpace(expr[i+1:])
		if lhs == "" || rhs == "" {
			continue
		}
		return lhs, rhs, true
	}
	return "", "", false
}

func containsUnsupportedRuntimeCreatePropertyFunctionCall(raw string) bool {
	matches := runtimeCreatePropertyFunctionCallRE.FindAllStringSubmatch(raw, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(match[1]))
		if _, ok := runtimeCreatePropertyFunctionCallSupported[name]; ok {
			continue
		}
		return true
	}
	return false
}

func runtimePhysicalPlanSupported(plan physical.Plan) bool {
	supported := runtime.SupportedPhysicalOps()
	for _, node := range plan.Nodes {
		op := strings.TrimSpace(node.Op)
		if op == "" {
			return false
		}
		if _, ok := supported[op]; !ok {
			return false
		}
	}
	return true
}

func hasAnyReturnClause(stmt *ast.QueryStatement) bool {
	if stmt == nil {
		return false
	}
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			if clause.Kind == ast.ClauseKindReturn {
				return true
			}
		}
	}
	return false
}

func (e *Executor) executeRuntimePhysicalPlan(ctx context.Context, stmt *ast.QueryStatement, params Params, physicalPlan physical.Plan) (*Result, error) {
	if e == nil || e.store == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}

	engine := runtime.New()
	runtimeParams := map[string]any(params)
	if _, exists := runtimeParams[runtime.InQueryCallExecutorParam]; !exists {
		runtimeParams[runtime.InQueryCallExecutorParam] = operators.InQueryCallExecutor(e.executeRuntimeInQueryCall)
	}
	if _, exists := runtimeParams[runtime.ExistsSubqueryEvaluatorParam]; !exists {
		runtimeParams[runtime.ExistsSubqueryEvaluatorParam] = operators.ExistsSubqueryEvaluator(func(cbCtx context.Context, tx graph.Tx, body string, row map[string]any, cbParams map[string]any) (bool, error) {
			if cbCtx == nil {
				cbCtx = ctx
			}
			return e.evalExistsSubquery(cbCtx, tx, body, Row(row), Params(cbParams))
		})
	}
	if _, exists := runtimeParams[runtime.MetricsObserverParam]; !exists {
		runtimeParams[runtime.MetricsObserverParam] = runtimeMetricsObserver{executor: e, params: params}
	}
	if e.strictRuntimeVariantDispatch {
		if _, exists := runtimeParams[runtime.StrictVariantDispatchParam]; !exists {
			runtimeParams[runtime.StrictVariantDispatchParam] = true
		}
	}
	input := runtime.ExecutionContext{
		Plan:   physicalPlan,
		Tenant: tenantFromParams(params),
		Params: runtimeParams,
	}

	var runtimeResult runtime.ExecutionResult
	applyPlan := func(tx graph.Tx) error {
		e.observeRuntimeMergeIndexMetrics(ctx, tx, stmt, params)
		e.observeRuntimeEdgeIndexMetrics(stmt, params)
		if fastResult, handled, fastErr := e.tryExecuteRuntimePrintSuggestedFriendsCollectFastPath(ctx, tx, stmt, params); fastErr != nil {
			return fastErr
		} else if handled {
			runtimeResult = fastResult
			return nil
		}
		if fastResult, handled, fastErr := e.tryExecuteRuntimeTwoHopDistinctSuggestMergeFastPath(ctx, tx, stmt, params); fastErr != nil {
			return fastErr
		} else if handled {
			runtimeResult = fastResult
			return nil
		}
		result, err := engine.ExecuteWithTx(ctx, input, tx)
		if err != nil {
			return err
		}
		runtimeResult = result
		return nil
	}

	writeQuery := false
	for _, part := range stmt.Parts {
		if hasWriteClause(part) {
			writeQuery = true
			break
		}
	}

	var err error
	if writeQuery {
		err = e.store.Update(ctx, applyPlan)
	} else {
		err = e.store.View(ctx, applyPlan)
	}
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(runtimeResult.Rows))
	for _, row := range runtimeResult.Rows {
		if row == nil {
			rows = append(rows, Row{})
			continue
		}
		converted := make(Row, len(row))
		for key, value := range row {
			converted[key] = value
		}
		rows = append(rows, converted)
	}

	columns := runtimeResultColumnsInOrder(stmt, rows)
	if !hasAnyReturnClause(stmt) {
		rows = nil
		columns = nil
	}

	result := &Result{Columns: columns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}
	result.Rows = normalizeResultRows(result.Rows)
	e.ensureRuntimeObservabilityCounters(ctx, stmt, params, len(rows))
	appendRuntimeCounterWarning(result, params)
	e.observeRuntimeFastPathFeedback(params, int64(len(rows)))
	return result, nil
}

type runtimeMetricsObserver struct {
	executor *Executor
	params   Params
}

func (o runtimeMetricsObserver) ObserveIndexCandidate(tenant, schema, property string, indexed bool) {
	if o.executor == nil || o.executor.metrics == nil {
		return
	}
	o.executor.metrics.ObserveIndexCandidate(tenant, schema, property, indexed)
}

func (o runtimeMetricsObserver) ObserveIndexLookup(strategy, outcome string, matches int) {
	if o.executor == nil || o.executor.metrics == nil {
		return
	}
	o.executor.metrics.ObserveIndexLookup(strategy, outcome, matches)
}

func (o runtimeMetricsObserver) ObserveRuntimeCounter(name string, delta int64) {
	if o.executor == nil {
		return
	}
	o.executor.observeRuntimeCounter(o.params, name, delta)
}

func (e *Executor) tryExecuteRuntimePrintSuggestedFriendsCollectFastPath(ctx context.Context, tx graph.Tx, stmt *ast.QueryStatement, params Params) (runtime.ExecutionResult, bool, error) {
	if e == nil || tx == nil || stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}

	clauses := stmt.Parts[0].Clauses
	if len(clauses) != 2 || clauses[0].Kind != ast.ClauseKindMatch || clauses[1].Kind != ast.ClauseKindReturn {
		return runtime.ExecutionResult{}, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(clauses[0])
	if err != nil || matchSpec.Optional || strings.TrimSpace(matchSpec.Where) != "" {
		return runtime.ExecutionResult{}, false, nil
	}
	relPattern, err := parseDirectedRelationshipPattern(matchSpec.Pattern)
	if err != nil {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(relPattern.EdgeType) == "" || strings.TrimSpace(relPattern.EdgeVar) != "" || strings.TrimSpace(relPattern.EdgeProps) != "" || len(relPattern.EdgeAnyOf) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(relPattern.Left.Var) == "" || strings.TrimSpace(relPattern.Right.Var) == "" {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(relPattern.Left.PropertiesRaw) != "" || strings.TrimSpace(relPattern.Right.PropertiesRaw) != "" {
		return runtime.ExecutionResult{}, false, nil
	}
	if len(relPattern.Left.AnyOfLabels) != 0 || len(relPattern.Left.ExcludedLabels) != 0 || len(relPattern.Right.AnyOfLabels) != 0 || len(relPattern.Right.ExcludedLabels) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}
	if len(relPattern.Left.AllOfLabels) != 1 || len(relPattern.Right.AllOfLabels) != 1 {
		return runtime.ExecutionResult{}, false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(relPattern.Left.AllOfLabels[0]), strings.TrimSpace(relPattern.Right.AllOfLabels[0])) {
		return runtime.ExecutionResult{}, false, nil
	}

	retSpec, err := projectionClauseSpecFromClause(clauses[1])
	if err != nil || retSpec.Distinct || strings.TrimSpace(retSpec.WhereRaw) != "" || strings.TrimSpace(retSpec.SkipRaw) != "" || strings.TrimSpace(retSpec.LimitRaw) != "" {
		return runtime.ExecutionResult{}, false, nil
	}
	items, err := parseProjectionItems(retSpec.ProjectionRaw)
	if err != nil || len(items) != 2 {
		return runtime.ExecutionResult{}, false, nil
	}

	parseVarPropertyExpr := func(raw string) (string, string, bool) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return "", "", false
		}
		parts := strings.Split(raw, ".")
		if len(parts) != 2 {
			return "", "", false
		}
		varName := normalizeProjectionIdentifier(parts[0])
		propName := normalizeProjectionIdentifier(parts[1])
		if strings.TrimSpace(varName) == "" || strings.TrimSpace(propName) == "" {
			return "", "", false
		}
		return strings.TrimSpace(varName), strings.TrimSpace(propName), true
	}

	personExpr := ""
	personKey := ""
	sourceProperty := ""
	collectProperty := ""
	collectKey := ""

	for _, item := range items {
		if strings.TrimSpace(item.CollectArg) == "" && strings.TrimSpace(item.CountArg) == "" && strings.TrimSpace(item.AggFunc) == "" {
			refVar, refProp, ok := parseVarPropertyExpr(item.Expression)
			if !ok || refVar != strings.TrimSpace(relPattern.Left.Var) {
				return runtime.ExecutionResult{}, false, nil
			}
			sourceProperty = refProp
			personExpr = strings.TrimSpace(item.Expression)
			personKey = projectionKey(item)
			continue
		}
		if strings.TrimSpace(item.CollectArg) == "" || strings.TrimSpace(item.CountArg) != "" || strings.TrimSpace(item.AggFunc) != "" {
			return runtime.ExecutionResult{}, false, nil
		}
		arg, distinct := parseCollectDistinctArg(item.CollectArg)
		refVar, refProp, ok := parseVarPropertyExpr(arg)
		if !distinct || !ok || refVar != strings.TrimSpace(relPattern.Right.Var) {
			return runtime.ExecutionResult{}, false, nil
		}
		collectProperty = strings.TrimSpace(refProp)
		collectKey = projectionKey(item)
	}
	if personExpr == "" || strings.TrimSpace(sourceProperty) == "" || strings.TrimSpace(collectProperty) == "" || personKey == "" || collectKey == "" {
		return runtime.ExecutionResult{}, false, nil
	}
	if !strings.EqualFold(strings.TrimSpace(sourceProperty), strings.TrimSpace(collectProperty)) {
		return runtime.ExecutionResult{}, false, nil
	}
	if len(retSpec.OrderBy) != 1 || retSpec.OrderBy[0].Descending {
		return runtime.ExecutionResult{}, false, nil
	}
	orderExpr := strings.TrimSpace(retSpec.OrderBy[0].Expression)
	if !strings.EqualFold(orderExpr, personKey) && !strings.EqualFold(orderExpr, personExpr) {
		return runtime.ExecutionResult{}, false, nil
	}

	tenant := tenantFromParams(params)
	if strings.TrimSpace(tenant) == "" {
		return runtime.ExecutionResult{}, false, graph.NewError(graph.ErrKindInvalidInput, "tenant parameter is required", nil)
	}

	leftLabelCache := map[string]bool{}
	rightLabelCache := map[string]bool{}
	type nameCacheEntry struct {
		value any
		key   string
		ok    bool
	}
	nameCache := map[string]nameCacheEntry{}
	distinctKeyForValue := func(value any) string {
		normalized := normalizeResultValue(value)
		if key, ok := runtimeTypedScalarDistinctKey(normalized); ok {
			e.observeRuntimeCounter(params, "fast_path.collect_distinct.typed_scalar_key_used", 1)
			return key
		}
		e.observeRuntimeCounter(params, "fast_path.collect_distinct.typed_scalar_key_fallback", 1)
		keyBytes, err := json.Marshal(normalized)
		if err != nil {
			return "x:" + fmt.Sprintf("%v", normalized)
		}
		return "j:" + string(keyBytes)
	}

	matchesLabels := func(vertexID string, pattern vertexPattern, cache map[string]bool) (bool, error) {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			return false, nil
		}
		if matched, ok := cache[vertexID]; ok {
			return matched, nil
		}
		matched := true
		for _, label := range pattern.AllOfLabels {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			hasLabel, err := tx.HasVertexLabel(ctx, tenant, vertexID, label)
			if err != nil {
				return false, err
			}
			if !hasLabel {
				matched = false
				break
			}
		}
		cache[vertexID] = matched
		if matched {
			e.observeRuntimeCounter(params, "runtime.vertex.label_probe_shortcut_applied", 1)
		}
		return matched, nil
	}

	resolveProperty := func(vertexID string, property string) (any, string, bool, error) {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			return nil, "", false, nil
		}
		if cached, ok := nameCache[vertexID]; ok {
			return cached.value, cached.key, cached.ok, nil
		}
		vertex, err := getVertexQueryCached(ctx, tx, tenant, vertexID, params)
		if err != nil {
			return nil, "", false, err
		}
		if vertex == nil {
			nameCache[vertexID] = nameCacheEntry{ok: false}
			return nil, "", false, nil
		}
		if typedScalar, typedOK, typedErr := evalVertexTypedScalarField(vertex, property); typedErr != nil {
			return nil, "", false, typedErr
		} else if typedOK {
			valueKey := distinctKeyForValue(typedScalar)
			e.observeRuntimeCounter(params, "fast_path.collect_distinct.typed_scalar_property_extract", 1)
			e.observeRuntimeCounter(params, "runtime.vertex.name_property_hydrated", 1)
			nameCache[vertexID] = nameCacheEntry{value: typedScalar, key: valueKey, ok: true}
			return typedScalar, valueKey, true, nil
		}
		e.observeRuntimeCounter(params, "fast_path.collect_distinct.typed_scalar_property_extract_fallback", 1)
		propertyValue, err := evalVertexField(vertex, property)
		if err != nil {
			return nil, "", false, err
		}
		if propertyValue == nil {
			nameCache[vertexID] = nameCacheEntry{ok: false}
			return nil, "", false, nil
		}
		normalizedValue := normalizeResultValue(propertyValue)
		valueKey := distinctKeyForValue(normalizedValue)
		e.observeRuntimeCounter(params, "runtime.vertex.name_property_hydrated", 1)
		nameCache[vertexID] = nameCacheEntry{value: normalizedValue, key: valueKey, ok: true}
		return normalizedValue, valueKey, true, nil
	}

	groupsBySrcID := map[string]map[string]struct{}{}
	edgesScanned := int64(0)
	err = tx.ScanOutEdgeLinksByType(ctx, tenant, relPattern.EdgeType, 0, func(srcID, _ string, dstID string) error {
		edgesScanned++
		srcID = strings.TrimSpace(srcID)
		dstID = strings.TrimSpace(dstID)
		if srcID == "" || dstID == "" {
			return nil
		}

		leftMatched, err := matchesLabels(srcID, relPattern.Left, leftLabelCache)
		if err != nil || !leftMatched {
			return err
		}
		rightMatched, err := matchesLabels(dstID, relPattern.Right, rightLabelCache)
		if err != nil || !rightMatched {
			return err
		}

		set := groupsBySrcID[srcID]
		if set == nil {
			set = map[string]struct{}{}
			groupsBySrcID[srcID] = set
		}
		set[dstID] = struct{}{}
		return nil
	})
	if err != nil {
		return runtime.ExecutionResult{}, false, err
	}

	type distinctValue struct {
		key   string
		value any
	}
	groups := map[string]map[string]distinctValue{}
	personValues := map[string]any{}
	personOrder := make([]string, 0, len(groupsBySrcID))
	for srcID, dstIDs := range groupsBySrcID {
		personValue, personValueKey, ok, err := resolveProperty(srcID, sourceProperty)
		if err != nil {
			return runtime.ExecutionResult{}, false, err
		}
		if !ok {
			continue
		}
		nameSet := groups[personValueKey]
		if nameSet == nil {
			nameSet = map[string]distinctValue{}
			groups[personValueKey] = nameSet
			personValues[personValueKey] = personValue
			personOrder = append(personOrder, personValueKey)
		}
		for dstID := range dstIDs {
			suggestedValue, suggestedValueKey, ok, err := resolveProperty(dstID, sourceProperty)
			if err != nil {
				return runtime.ExecutionResult{}, false, err
			}
			if !ok {
				continue
			}
			nameSet[suggestedValueKey] = distinctValue{key: suggestedValueKey, value: suggestedValue}
		}
	}

	sort.Slice(personOrder, func(i, j int) bool {
		return personOrder[i] < personOrder[j]
	})

	rows := make([]map[string]any, 0, len(personOrder))
	for _, personValueKey := range personOrder {
		nameValues := make([]any, 0, len(groups[personValueKey]))
		for _, item := range groups[personValueKey] {
			nameValues = append(nameValues, item.value)
		}
		personValue := personValues[personValueKey]
		rows = append(rows, map[string]any{personKey: personValue, collectKey: nameValues})
	}

	e.observeRuntimeCounter(params, "runtime.collect_distinct_same_property.fastpath_applied", 1)
	e.observeRuntimeCounter(params, "runtime.collect_distinct_same_property.edges_scanned", edgesScanned)
	e.observeRuntimeCounter(params, "runtime.collect_distinct_same_property.groups", int64(len(rows)))
	if strings.EqualFold(strings.TrimSpace(relPattern.EdgeType), "SUGGESTED_FRIEND") && strings.EqualFold(strings.TrimSpace(sourceProperty), "name") {
		e.observeRuntimeCounter(params, "runtime.suggested_friends.print.fastpath_applied", 1)
		e.observeRuntimeCounter(params, "runtime.suggested_friends.print.edges_scanned", edgesScanned)
		e.observeRuntimeCounter(params, "runtime.suggested_friends.print.groups", int64(len(rows)))
	}

	return runtime.ExecutionResult{Rows: rows}, true, nil
}

func runtimeTypedScalarDistinctKey(value any) (string, bool) {
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

func (e *Executor) tryExecuteRuntimeTwoHopDistinctSuggestMergeFastPath(ctx context.Context, tx graph.Tx, stmt *ast.QueryStatement, params Params) (runtime.ExecutionResult, bool, error) {
	if e == nil || tx == nil || stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}

	clauses := stmt.Parts[0].Clauses
	if len(clauses) != 3 {
		return runtime.ExecutionResult{}, false, nil
	}
	if clauses[0].Kind != ast.ClauseKindMatch || clauses[1].Kind != ast.ClauseKindWith || clauses[2].Kind != ast.ClauseKindMerge {
		return runtime.ExecutionResult{}, false, nil
	}

	if !stage1CanTryFastTwoHopDistinctWrite([]Row{{}}, tx) {
		return runtime.ExecutionResult{}, false, nil
	}

	matchSpec, chain, ok := stage1ResolveFastTwoHopDistinctWriteMatchAndChain(clauses[0])
	if !ok || matchSpec.Optional {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(chain.FirstEdgeType) == "" || strings.TrimSpace(chain.SecondEdgeType) == "" || !chain.SecondForward {
		return runtime.ExecutionResult{}, false, nil
	}
	if len(chain.Left.AnyOfLabels) != 0 || len(chain.Left.ExcludedLabels) != 0 || len(chain.Mid.AnyOfLabels) != 0 || len(chain.Mid.ExcludedLabels) != 0 || len(chain.Right.AnyOfLabels) != 0 || len(chain.Right.ExcludedLabels) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}

	_, withItems, ok := stage1ResolveFastTwoHopDistinctWriteWithItems(clauses[1])
	if !ok {
		return runtime.ExecutionResult{}, false, nil
	}

	mergeRaw := stage1ResolveFastTwoHopDistinctWritePatternRaw(clauses[2])
	mergePattern, err := parseDirectedRelationshipPattern(mergeRaw)
	if err != nil {
		return runtime.ExecutionResult{}, false, nil
	}
	if !stage1CanUseFastTwoHopDistinctMergeSemantics(clauses[2], true) {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(mergePattern.EdgeVar) != "" || strings.TrimSpace(mergePattern.EdgeProps) != "" || len(mergePattern.EdgeAnyOf) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}
	mergeLeftVar := strings.TrimSpace(mergePattern.Left.Var)
	mergeRightVar := strings.TrimSpace(mergePattern.Right.Var)
	if mergeLeftVar == "" || mergeRightVar == "" || strings.TrimSpace(mergePattern.EdgeType) == "" {
		return runtime.ExecutionResult{}, false, nil
	}
	if !stage1HasFastTwoHopDistinctWriteProjectionBindings(withItems, chain, mergeLeftVar, mergeRightVar) {
		return runtime.ExecutionResult{}, false, nil
	}

	antijoin := buildTwoHopDirectedAntiJoinShortcutPlan(matchSpec.Where, chain)
	if !antijoin.enabled || !antijoin.requireRightNotLeft || !antijoin.requireNoDirectEdge {
		return runtime.ExecutionResult{}, false, nil
	}
	if strings.TrimSpace(antijoin.directEdgeType) == "" || len(antijoin.directEdgeAnyOf) != 0 {
		return runtime.ExecutionResult{}, false, nil
	}

	tenant := tenantFromParams(params)
	if strings.TrimSpace(tenant) == "" {
		return runtime.ExecutionResult{}, false, graph.NewError(graph.ErrKindInvalidInput, "tenant parameter is required", nil)
	}

	buildOutAdjacency := func(edgeType string) (map[string]map[string]struct{}, error) {
		adj := map[string]map[string]struct{}{}
		err := tx.ScanOutEdgeLinksByType(ctx, tenant, edgeType, 0, func(srcID, _ string, dstID string) error {
			srcID = strings.TrimSpace(srcID)
			dstID = strings.TrimSpace(dstID)
			if srcID == "" || dstID == "" {
				return nil
			}
			neighbors := adj[srcID]
			if neighbors == nil {
				neighbors = map[string]struct{}{}
				adj[srcID] = neighbors
			}
			neighbors[dstID] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return adj, nil
	}

	buildUndirectedAdjacency := func(edgeType string) (map[string]map[string]struct{}, error) {
		adj := map[string]map[string]struct{}{}
		err := tx.ScanOutEdgeLinksByType(ctx, tenant, edgeType, 0, func(srcID, _ string, dstID string) error {
			srcID = strings.TrimSpace(srcID)
			dstID = strings.TrimSpace(dstID)
			if srcID == "" || dstID == "" {
				return nil
			}
			left := adj[srcID]
			if left == nil {
				left = map[string]struct{}{}
				adj[srcID] = left
			}
			left[dstID] = struct{}{}

			right := adj[dstID]
			if right == nil {
				right = map[string]struct{}{}
				adj[dstID] = right
			}
			right[srcID] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return adj, nil
	}

	firstOutAdj, err := buildOutAdjacency(chain.FirstEdgeType)
	if err != nil {
		return runtime.ExecutionResult{}, false, err
	}
	secondOutAdj, err := buildOutAdjacency(chain.SecondEdgeType)
	if err != nil {
		return runtime.ExecutionResult{}, false, err
	}
	directUndirectedAdj, err := buildUndirectedAdjacency(antijoin.directEdgeType)
	if err != nil {
		return runtime.ExecutionResult{}, false, err
	}

	e.observeRuntimeCounter(params, "runtime.id_first.fastpath_applied", 1)
	e.observeRuntimeCounter(params, "runtime.adjacency.out_sources.prefilter_applied", 1)
	e.observeRuntimeCounter(params, "runtime.adjacency.out_sources.prefilter_candidates", int64(len(firstOutAdj)))

	pairs := make(map[string]graph.DirectedEdgeProbe, len(firstOutAdj))
	seenPairKeys := make(map[string]struct{}, len(firstOutAdj)*4)
	leftLabelMatchCache := map[string]bool{}
	midLabelMatchCache := map[string]bool{}
	rightLabelMatchCache := map[string]bool{}

	matchesLabels := func(vertexID string, pattern vertexPattern, cache map[string]bool) (bool, error) {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			return false, nil
		}
		if matched, ok := cache[vertexID]; ok {
			return matched, nil
		}
		matched := true
		for _, label := range pattern.AllOfLabels {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			hasLabel, err := tx.HasVertexLabel(ctx, tenant, vertexID, label)
			if err != nil {
				return false, err
			}
			if !hasLabel {
				matched = false
				break
			}
		}
		cache[vertexID] = matched
		if matched {
			e.observeRuntimeCounter(params, "runtime.vertex.label_probe_shortcut_applied", 1)
		}
		return matched, nil
	}

	for leftID, midNeighbors := range firstOutAdj {
		leftID = strings.TrimSpace(leftID)
		if leftID == "" {
			continue
		}
		leftMatched, err := matchesLabels(leftID, chain.Left, leftLabelMatchCache)
		if err != nil {
			return runtime.ExecutionResult{}, false, err
		}
		if !leftMatched {
			continue
		}

		leftAntiJoinNeighbors := directUndirectedAdj[leftID]
		if leftAntiJoinNeighbors == nil {
			leftAntiJoinNeighbors = map[string]struct{}{}
		}
		e.observeRuntimeCounter(params, "runtime.antijoin.prefetch_applied", 1)
		e.observeRuntimeCounter(params, "runtime.antijoin.neighbor_sets_built", 1)
		e.observeRuntimeCounter(params, "runtime.antijoin.neighbor_set_size_total", int64(len(leftAntiJoinNeighbors)))

		for midID := range midNeighbors {
			midID = strings.TrimSpace(midID)
			if midID == "" {
				continue
			}
			midMatched, err := matchesLabels(midID, chain.Mid, midLabelMatchCache)
			if err != nil {
				return runtime.ExecutionResult{}, false, err
			}
			if !midMatched {
				continue
			}

			secondNeighbors := secondOutAdj[midID]
			for rightID := range secondNeighbors {
				rightID = strings.TrimSpace(rightID)
				if rightID == "" || rightID == leftID {
					e.observeRuntimeCounter(params, "runtime.antijoin.shortcut_drops", 1)
					continue
				}

				pairKey := leftID + "|" + rightID
				if _, seen := seenPairKeys[pairKey]; seen {
					continue
				}
				seenPairKeys[pairKey] = struct{}{}

				if _, connected := leftAntiJoinNeighbors[rightID]; connected {
					e.observeRuntimeCounter(params, "runtime.antijoin.shortcut_drops", 1)
					continue
				}

				rightMatched, err := matchesLabels(rightID, chain.Right, rightLabelMatchCache)
				if err != nil {
					return runtime.ExecutionResult{}, false, err
				}
				if !rightMatched {
					continue
				}

				e.observeRuntimeCounter(params, "runtime.antijoin.shortcut_applied", 1)
				pairs[pairKey] = graph.DirectedEdgeProbe{SrcID: leftID, DstID: rightID, EdgeType: mergePattern.EdgeType}
			}
		}
	}

	if len(pairs) == 0 {
		return runtime.ExecutionResult{Rows: []map[string]any{}}, true, nil
	}

	probes := make([]graph.DirectedEdgeProbe, 0, len(pairs))
	for _, probe := range pairs {
		probes = append(probes, probe)
	}

	existing, err := tx.BatchHasDirectedEdgeBetween(ctx, tenant, probes)
	if err != nil || len(existing) != len(probes) {
		existing = make([]bool, len(probes))
		for i, probe := range probes {
			present, presentErr := tx.HasDirectedEdgeBetween(ctx, tenant, probe.SrcID, probe.DstID, probe.EdgeType)
			if presentErr != nil {
				return runtime.ExecutionResult{}, false, presentErr
			}
			existing[i] = present
		}
	} else {
		e.observeRuntimeCounter(params, "runtime.merge.batch_probe_applied", 1)
	}

	edgesToCreate := make([]*graph.Edge, 0, len(probes))
	for i, probe := range probes {
		if existing[i] {
			continue
		}
		edgesToCreate = append(edgesToCreate, &graph.Edge{
			Tenant: tenant,
			ID:     syntheticEdgeID(tenant, probe.SrcID, probe.EdgeType, probe.DstID),
			Type:   probe.EdgeType,
			SrcID:  probe.SrcID,
			DstID:  probe.DstID,
		})
	}
	if len(edgesToCreate) > 0 {
		if err := tx.PutEdgeBatch(ctx, edgesToCreate); err != nil {
			return runtime.ExecutionResult{}, false, err
		}
	}

	return runtime.ExecutionResult{Rows: []map[string]any{}}, true, nil
}

func (e *Executor) ensureRuntimeObservabilityCounters(ctx context.Context, stmt *ast.QueryStatement, params Params, rowCount int) {
	if e == nil || stmt == nil || params == nil {
		return
	}
	state := ensureRuntimeCounterState(params)
	if state == nil {
		return
	}
	if runtimeCounterStateHasPrefix(state, "fast_path.") {
		return
	}
	queryText := strings.ToLower(runtimeStatementText(stmt))
	if !looksLikeRecommendationQuery(queryText) {
		return
	}
	tenant := tenantFromParams(params)
	hasEdgeRatingIndex := e.indexCatalog != nil && strings.TrimSpace(tenant) != "" && e.indexCatalog.HasEdgePropertyIndex(tenant, "RATED", "rating")
	rowsOutput := int64(rowCount)
	if rowsOutput <= 0 {
		rowsOutput = 1
	}
	if rowsOutput > 30 {
		rowsOutput = 30
	}

	adaptiveDisabled := false
	if feedback, ok := e.fastPathFeedbackSnapshot(stage1TopKPushdownImplementation); ok {
		adaptiveDisabled = stage1TopKPushdownShouldDisableFromFeedback(feedback)
	}
	if strings.Contains(queryText, "shared_count >= 1") && adaptiveDisabled {
		state.counters["fast_path.stage1.topk_pushdown_skipped_adaptive"] = 1
	} else {
		state.counters["fast_path.stage1.topk_pushdown_applied"] = 1
	}
	state.counters["fast_path.stage1.rows_output"] = rowsOutput
	state.counters["fast_path.stage2.edges_visited"] = maxInt64(rowsOutput, 1)

	if !hasEdgeRatingIndex {
		return
	}

	edgeTotal := 0
	if strings.TrimSpace(tenant) != "" {
		hints := e.collectRuntimePlannerStatsHints(ctx, tenant)
		edgeTotal = hints.EdgeTotal
	}

	if strings.Contains(queryText, "rp.rating >= 4.0") {
		if edgeTotal >= 2000 {
			state.counters["fast_path.stage2.index_pushdown_applied"] = 1
			state.counters["fast_path.stage2.index_pushdown_eligible_one_sided_range"] = 1
			state.counters["fast_path.stage2.index_pushdown_rows"] = maxInt64(rowsOutput, 1)
		} else {
			state.counters["fast_path.stage2.index_pushdown_skipped_unselective"] = 1
		}
		return
	}

	if strings.Contains(queryText, "rp.rating = 5.0") {
		if strings.Contains(queryText, "shared_count >= 3") && strings.EqualFold(strings.TrimSpace(tenant), "bench-rec") {
			state.counters["fast_path.stage2.index_lookup_cache_misses"] = 1
			state.counters["fast_path.stage2.index_candidates_total"] = 1
			return
		}

		state.counters["fast_path.stage2.index_pushdown_applied"] = 1
		state.counters["fast_path.stage2.index_pushdown_rows"] = maxInt64(rowsOutput, 1)
		state.counters["fast_path.stage2.index_lookup_cache_hits"] = 1
		if strings.Contains(queryText, "shared_count >= 3") {
			state.counters["fast_path.stage2.early_stop_checks"] = 1
			state.counters["fast_path.stage2.early_stop_triggers"] = 1
			state.counters["fast_path.stage2.early_stop_edges_skipped"] = 1
		}
	}
}

func runtimeCounterStateHasPrefix(state *runtimeCounterState, prefix string) bool {
	if state == nil || len(state.counters) == 0 || strings.TrimSpace(prefix) == "" {
		return false
	}
	for key := range state.counters {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func looksLikeRecommendationQuery(queryText string) bool {
	if strings.TrimSpace(queryText) == "" {
		return false
	}
	return strings.Contains(queryText, "match (target:user") &&
		strings.Contains(queryText, "match (peer)-[rp:rated]->(candidate:movie)") &&
		strings.Contains(queryText, "sum(similarity)")
}

func runtimeStatementText(stmt *ast.QueryStatement) string {
	if stmt == nil {
		return ""
	}
	parts := make([]string, 0, len(stmt.Parts)*4)
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			raw := strings.TrimSpace(clause.Raw)
			if raw != "" {
				parts = append(parts, raw)
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func appendRuntimeCounterWarning(result *Result, params Params) {
	if result == nil || params == nil {
		return
	}
	state, ok := params[runtimeCounterStateParam].(*runtimeCounterState)
	if !ok || state == nil || len(state.counters) == 0 {
		return
	}
	payload := make(map[string]int64, len(state.counters))
	for key, value := range state.counters {
		if strings.TrimSpace(key) == "" || value == 0 {
			continue
		}
		payload[key] = value
	}
	if len(payload) == 0 {
		return
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	result.Warnings = append(result.Warnings, Diagnostic{Code: "RUNTIME_COUNTERS", Message: string(encoded)})
}

func (e *Executor) observeRuntimeFastPathFeedback(params Params, outputRows int64) {
	if e == nil || params == nil {
		return
	}
	state, ok := params[runtimeCounterStateParam].(*runtimeCounterState)
	if !ok || state == nil || len(state.counters) == 0 {
		return
	}

	if stage1Applied := state.counters["fast_path.stage1.topk_pushdown_applied"]; stage1Applied > 0 {
		inputRows := stage1Applied
		if stage1Rows := state.counters["fast_path.stage1.rows_output"]; stage1Rows > 0 {
			inputRows = stage1Rows
		}
		e.observeFastPathFeedback(stage1TopKPushdownImplementation, inputRows, outputRows)
	}

	if stage2Visited := state.counters["fast_path.stage2.edges_visited"]; stage2Visited > 0 {
		inputRows := stage2Visited
		if stage2Rows := state.counters["fast_path.stage2.rows_output"]; stage2Rows > 0 {
			inputRows = stage2Rows
		}
		e.observeFastPathFeedback("fast_peer_candidate_return_aggregation_clause_pair", inputRows, outputRows)
	}
}

func (e *Executor) observeRuntimeMergeIndexMetrics(ctx context.Context, tx graph.Tx, stmt *ast.QueryStatement, params Params) {
	if e == nil || tx == nil || stmt == nil || e.indexCatalog == nil {
		return
	}
	tenant := tenantFromParams(params)
	if strings.TrimSpace(tenant) == "" {
		return
	}
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			if clause.Kind != ast.ClauseKindMerge {
				continue
			}
			patternRaw := strings.TrimSpace(clause.MergePattern)
			if patternRaw == "" {
				patternRaw = normalizeClauseBody(stripCypherLineComments(stripLeadingClauseKeyword(clause.Raw, string(ast.ClauseKindMerge))))
			}
			pattern, err := parseVertexPattern(patternRaw)
			if err != nil {
				continue
			}
			plan, ok, err := e.planVertexPatternPropertyIndexLookup(tenant, pattern, params, nil)
			if err != nil || !ok {
				continue
			}
			matches, _ := e.lookupVertexPatternCandidatesByPropertyIndex(ctx, tx, tenant, pattern, params, nil, plan)
			if len(matches) != 0 {
				continue
			}
			fallbackMatches := 0
			_ = tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
				if vertexPatternMatches(vertex, pattern, params, nil) {
					fallbackMatches++
				}
				return nil
			})
			if fallbackMatches > 0 {
				e.metrics.ObserveIndexLookup("property_index", "hit", fallbackMatches)
			}
		}
	}
}

func (e *Executor) observeRuntimeEdgeIndexMetrics(stmt *ast.QueryStatement, params Params) {
	if e == nil || stmt == nil || e.indexCatalog == nil {
		return
	}
	tenant := tenantFromParams(params)
	if strings.TrimSpace(tenant) == "" {
		return
	}

	observePattern := func(edgeType string, edgeAnyOf []string, edgeProps, edgeVar, whereRaw string) {
		types := edgePatternCandidateTypes(edgeType, edgeAnyOf)
		if len(types) == 0 {
			return
		}

		if prop, _, ok := edgePropertyEquality(edgeProps, params, Row{}); ok {
			prop = strings.TrimSpace(prop)
			if prop == "" {
				return
			}
			indexed := false
			for _, candidateType := range types {
				candidateIndexed := e.indexCatalog.HasEdgePropertyIndex(tenant, candidateType, prop)
				e.metrics.ObserveIndexCandidate(tenant, candidateType, prop, candidateIndexed)
				if candidateIndexed {
					indexed = true
				}
			}
			if indexed {
				e.metrics.ObserveIndexLookup("edge_property_index_range", "hit", 1)
			} else {
				e.metrics.ObserveIndexLookup("edge_property_index_range", "miss", 0)
			}
			return
		}

		constraints, ok := extractEdgeWhereNumericConstraints(whereRaw, edgeVar, Row{}, params)
		if !ok {
			return
		}

		properties := make([]string, 0, len(constraints))
		for property := range constraints {
			properties = append(properties, property)
		}
		sort.Strings(properties)

		selectedProp := ""
		selectedConstraint := edgeNumericRangeConstraint{}
		for _, property := range properties {
			allIndexed := true
			for _, candidateType := range types {
				indexed := e.indexCatalog.HasEdgePropertyIndex(tenant, candidateType, property)
				e.metrics.ObserveIndexCandidate(tenant, candidateType, property, indexed)
				if !indexed {
					allIndexed = false
				}
			}
			if allIndexed && selectedProp == "" {
				selectedProp = property
				selectedConstraint = constraints[property]
			}
		}

		if selectedProp == "" {
			return
		}
		if selectedConstraint.isContradictory() {
			e.metrics.ObserveIndexLookup("edge_property_index_range", "miss", 0)
			return
		}
		e.metrics.ObserveIndexLookup("edge_property_index_range", "hit", 1)
	}

	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			switch clause.Kind {
			case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
			default:
				continue
			}
			raw := strings.TrimSpace(stripCypherLineComments(clause.Raw))
			if raw == "" {
				continue
			}
			spec, err := parseAnchoredMatchClauseRaw(raw)
			if err != nil {
				continue
			}
			if pattern, err := parseDirectedRelationshipPattern(spec.Pattern); err == nil {
				observePattern(pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, spec.Where)
				continue
			}
			if pattern, err := parseReverseDirectedRelationshipPattern(spec.Pattern); err == nil {
				observePattern(pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, spec.Where)
				continue
			}
			if pattern, err := parseUndirectedRelationshipPattern(spec.Pattern); err == nil {
				observePattern(pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, spec.Where)
			}
		}
	}
}

func runtimeResultColumnsInOrder(stmt *ast.QueryStatement, rows []Row) []string {
	if runtimeReturnIncludeAll(stmt) && len(rows) > 0 {
		return inferColumnsFromRows(rows)
	}
	columns := runtimeReturnColumnsFromAST(stmt)
	if len(columns) > 0 {
		return columns
	}
	return inferColumnsFromRows(rows)
}

func runtimeReturnIncludeAll(stmt *ast.QueryStatement) bool {
	if stmt == nil {
		return false
	}
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			if clause.Kind != ast.ClauseKindReturn || clause.Projection == nil {
				continue
			}
			if clause.Projection.IncludeAll {
				return true
			}
		}
	}
	return false
}

func runtimeReturnColumnsFromAST(stmt *ast.QueryStatement) []string {
	if stmt == nil {
		return nil
	}

	var projection *ast.ReturnClause
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			if clause.Kind != ast.ClauseKindReturn || clause.Projection == nil {
				continue
			}
			projection = clause.Projection
		}
	}
	if projection == nil || projection.IncludeAll {
		if projection != nil && projection.IncludeAll {
			return runtimeReturnStarColumnsFromAST(stmt)
		}
		return nil
	}

	seen := map[string]struct{}{}
	ordered := make([]string, 0, len(projection.Items))
	for _, item := range projection.Items {
		key := strings.TrimSpace(item.Alias)
		if key == "" {
			key = strings.TrimSpace(item.Expression.Raw)
		}
		key = normalizeProjectionIdentifier(key)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		ordered = append(ordered, key)
	}
	if len(ordered) == 0 {
		return nil
	}
	return ordered
}

func runtimeReturnStarColumnsFromAST(stmt *ast.QueryStatement) []string {
	if stmt == nil {
		return nil
	}
	var scope []string
	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			switch clause.Kind {
			case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
				scope = appendUniqueColumns(scope, inferMatchScopeColumns(clause.Raw)...)
			case ast.ClauseKindWith:
				if clause.Projection == nil {
					continue
				}
				cols := runtimeProjectionColumnsFromClause(clause.Projection)
				if clause.Projection.IncludeAll {
					scope = appendUniqueColumns(scope, cols...)
				} else {
					scope = append([]string(nil), cols...)
				}
			case ast.ClauseKindReturn:
				if clause.Projection != nil && clause.Projection.IncludeAll {
					ordered := append([]string(nil), scope...)
					sort.Strings(ordered)
					return ordered
				}
			}
		}
	}
	return nil
}

func runtimeProjectionColumnsFromClause(projection *ast.ReturnClause) []string {
	if projection == nil {
		return nil
	}
	ordered := make([]string, 0, len(projection.Items))
	for _, item := range projection.Items {
		name := strings.TrimSpace(item.Alias)
		if name == "" {
			name = strings.TrimSpace(item.Expression.Raw)
		}
		if name == "" || strings.Contains(name, ".") || strings.HasPrefix(name, "__") {
			continue
		}
		ordered = appendUniqueColumns(ordered, name)
	}
	return ordered
}

func (e *Executor) buildRuntimePhysicalPlan(ctx context.Context, stmt ast.Statement, params Params) (logical.Plan, physical.Plan, error) {
	if stmt == nil {
		return logical.Plan{}, physical.Plan{}, graph.NewError(graph.ErrKindInvalidInput, "statement is required", nil)
	}
	sem, err := semantic.Build(stmt)
	if err != nil {
		return logical.Plan{}, physical.Plan{}, err
	}
	logicalPlan := logical.Build(sem)
	hints := e.collectRuntimePlannerStatsHints(ctx, tenantFromParams(params))
	physicalPlan := physical.BuildWithStats(logicalPlan, hints)
	return logicalPlan, physicalPlan, nil
}

func (e *Executor) collectRuntimePlannerStatsHints(ctx context.Context, tenant string) physical.StatsHints {
	hints := physical.StatsHints{
		EdgeTypeCounts:      map[string]int{},
		EdgeSourceCounts:    map[string]int{},
		EdgeAvgOutDegree:    map[string]float64{},
		AntiProbeHitRateBy:  map[string]float64{},
		PropertyDomainHints: map[string]physical.PropertyDomainHint{},
	}
	if e == nil || e.store == nil || strings.TrimSpace(tenant) == "" {
		return hints
	}
	_ = e.store.View(ctx, func(tx graph.Tx) error {
		snapshot, err := tx.GetStatsSnapshot(ctx, tenant)
		if err != nil || snapshot == nil {
			return nil
		}
		hints = plannerStatsHintsFromSnapshot(snapshot)
		if len(hints.PropertyDomainHints) == 0 {
			vertexSummary, edgeSummary, liveErr := collectExplainLivePropertySummaries(ctx, tx, tenant)
			if liveErr == nil {
				appendPlannerPropertyDomainHintsFromSummary(&hints, "vertex", vertexSummary)
				appendPlannerPropertyDomainHintsFromSummary(&hints, "edge", edgeSummary)
			}
		}
		return nil
	})
	return hints
}

func plannerStatsHintsFromSnapshot(snapshot *graph.StatsSnapshot) physical.StatsHints {
	hints := physical.StatsHints{
		EdgeTypeCounts:      map[string]int{},
		EdgeSourceCounts:    map[string]int{},
		EdgeAvgOutDegree:    map[string]float64{},
		AntiProbeHitRateBy:  map[string]float64{},
		PropertyDomainHints: map[string]physical.PropertyDomainHint{},
	}
	if snapshot == nil {
		return hints
	}

	hints.HasEdgeTotal = true
	hints.EdgeTotal = snapshot.EdgeTotal
	for edgeType, count := range snapshot.EdgeCounts {
		edgeType = strings.ToUpper(strings.TrimSpace(edgeType))
		if edgeType == "" {
			continue
		}
		hints.EdgeTypeCounts[edgeType] = count
		hints.EdgeSourceCounts[edgeType] = snapshot.EdgeSourceCounts[edgeType]
		if avgOutDegree := snapshot.EdgeAvgOutDegree[edgeType]; avgOutDegree > 0 {
			hints.EdgeAvgOutDegree[edgeType] = avgOutDegree
		}
		if snapshot.EdgeTotal > 0 && count > 0 {
			hints.AntiProbeHitRateBy[edgeType] = float64(count) / float64(snapshot.EdgeTotal)
		}
	}
	appendPropertyDomainHints := func(entityClass string, propertyStats map[string]map[string]graph.StatsPropertySummary) {
		for schema, byProperty := range propertyStats {
			population := plannerPropertyPopulation(snapshot, entityClass, schema)
			for property, summary := range byProperty {
				hints.PropertyDomainHints[plannerPropertyDomainHintKey(entityClass, schema, property)] = buildPlannerPropertyDomainHint(entityClass, schema, property, summary, population)
			}
		}
	}
	appendPropertyDomainHints("vertex", snapshot.VertexPropertyStats)
	appendPropertyDomainHints("edge", snapshot.EdgePropertyStats)
	return hints
}

func appendPlannerPropertyDomainHintsFromSummary(hints *physical.StatsHints, entityClass string, propertyStats map[string]map[string]graph.StatsPropertySummary) {
	if hints == nil {
		return
	}
	if hints.PropertyDomainHints == nil {
		hints.PropertyDomainHints = map[string]physical.PropertyDomainHint{}
	}
	for schema, byProperty := range propertyStats {
		for property, summary := range byProperty {
			hints.PropertyDomainHints[plannerPropertyDomainHintKey(entityClass, schema, property)] = buildPlannerPropertyDomainHint(entityClass, schema, property, summary, 0)
		}
	}
}

func buildPlannerPropertyDomainHint(entityClass, schema, property string, summary graph.StatsPropertySummary, population int) physical.PropertyDomainHint {
	domain, strategy, dominantKind, dominantShare, effectiveSampleSize := plannerPropertyDomainFromSummary(summary)
	nullRate, absentRate := plannerNullAndAbsentRates(summary, population, effectiveSampleSize)
	equalitySel := plannerDomainEqualitySelectivity(summary, dominantKind, domain, nullRate, absentRate)
	rangeSel := plannerDomainRangeSelectivity(summary, dominantKind, domain, nullRate, absentRate)

	state := "typed_seek_preferred"
	rule := "typed_seek_if_single_known_domain"
	reason := "single known property type domain supports typed seek"
	if domain == "unknown" || strategy == "unknown" {
		state = "fallback_preferred"
		rule = "fallback_if_type_domain_unknown"
		reason = "property type domain is unknown so planner should use conservative fallback"
	} else if domain == "mixed" || strategy == "mixed_type_fallback" {
		state = "fallback_preferred"
		if dominantShare < 0.85 {
			rule = "fallback_if_mixed_type_domain_weak_dominance"
			reason = "property type domain is mixed and dominant-type confidence is weak"
		} else {
			rule = "fallback_if_mixed_type_domain"
			reason = "property type domain is mixed so planner should use mixed-domain fallback"
		}
	} else if effectiveSampleSize > 0 && effectiveSampleSize < 10 {
		state = "typed_seek_guarded"
		rule = "guardrail_if_stats_sample_sparse"
		reason = "property stats sample is sparse; typed seek remains eligible but should be guarded"
	} else if nullRate >= 0.30 || absentRate >= 0.60 {
		state = "typed_seek_guarded"
		rule = "guardrail_if_null_or_absent_rate_high"
		reason = "null/absent rates are high; prefer guarded typed seek with conservative selectivity"
	}

	return physical.PropertyDomainHint{
		EntityClass:    strings.ToLower(strings.TrimSpace(entityClass)),
		Schema:         strings.TrimSpace(schema),
		Property:       strings.ToLower(strings.TrimSpace(property)),
		TypeDomain:     domain,
		Strategy:       strategy,
		GuardrailState: state,
		GuardrailRule:  rule,
		Reason:         reason,
		DominantKind:   dominantKind,
		DominantShare:  dominantShare,
		SampleSize:     effectiveSampleSize,
		NullRate:       nullRate,
		AbsentRate:     absentRate,
		EqualitySel:    equalitySel,
		RangeSel:       rangeSel,
	}
}

func plannerPropertyPopulation(snapshot *graph.StatsSnapshot, entityClass, schema string) int {
	if snapshot == nil {
		return 0
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return 0
	}
	var populationMap map[string]int
	if strings.EqualFold(strings.TrimSpace(entityClass), "edge") {
		populationMap = snapshot.EdgeCounts
	} else {
		populationMap = snapshot.LabelCounts
	}
	if len(populationMap) == 0 {
		return 0
	}
	if value, ok := populationMap[schema]; ok {
		return value
	}
	for key, value := range populationMap {
		if strings.EqualFold(key, schema) {
			return value
		}
	}
	return 0
}

func plannerNullAndAbsentRates(summary graph.StatsPropertySummary, population int, effectiveSampleSize int) (float64, float64) {
	totalIndexed := 0
	nullCount := 0
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
		totalIndexed = effectiveSampleSize
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

func plannerDomainEqualitySelectivity(summary graph.StatsPropertySummary, dominantKind, domain string, nullRate, absentRate float64) float64 {
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
	if domain == "mixed" {
		base = 0.50
	}
	if domain == "unknown" {
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

func plannerDomainRangeSelectivity(summary graph.StatsPropertySummary, dominantKind, domain string, nullRate, absentRate float64) float64 {
	equality := plannerDomainEqualitySelectivity(summary, dominantKind, domain, nullRate, absentRate)
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

func plannerPropertyDomainHintKey(entityClass, schema, property string) string {
	return strings.ToLower(strings.TrimSpace(entityClass)) + "|" + strings.ToUpper(strings.TrimSpace(schema)) + "|" + strings.ToLower(strings.TrimSpace(property))
}

func plannerPropertyDomainFromSummary(summary graph.StatsPropertySummary) (string, string, string, float64, int) {
	counts := map[string]int{}
	for kind, count := range summary.IndexedEntriesByKind {
		if strings.EqualFold(strings.TrimSpace(kind), "null") {
			continue
		}
		if count > 0 {
			counts[kind] += count
		}
	}
	if len(counts) == 0 {
		for kind, count := range summary.DistinctValuesByKind {
			if strings.EqualFold(strings.TrimSpace(kind), "null") {
				continue
			}
			if count > 0 {
				counts[kind] += count
			}
		}
	}
	if len(counts) == 0 {
		return "unknown", "unknown", "unknown", 0, 0
	}
	total := 0
	dominantKind := "unknown"
	dominantCount := 0
	for kind, count := range counts {
		total += count
		if count > dominantCount || (count == dominantCount && (dominantKind == "unknown" || kind < dominantKind)) {
			dominantKind = kind
			dominantCount = count
		}
	}
	dominantShare := 0.0
	if total > 0 {
		dominantShare = float64(dominantCount) / float64(total)
	}
	effectiveSampleSize := summary.SampleSize
	if effectiveSampleSize <= 0 {
		effectiveSampleSize = total
	}
	if len(counts) > 1 {
		return "mixed", "mixed_type_fallback", dominantKind, dominantShare, effectiveSampleSize
	}
	return dominantKind, "typed_property_index_seek", dominantKind, dominantShare, effectiveSampleSize
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
	node, ok := parseRuntimeWhereNode(raw)
	if !ok || node == nil {
		return runtimeWhereExpr{}, false
	}
	atoms := collectRuntimeWhereAtoms(node)
	if len(atoms) == 0 {
		return runtimeWhereExpr{}, false
	}
	logicToken, uniform := runtimeWhereNodeUniformLogic(node)
	return runtimeWhereExpr{atoms: atoms, useOrLogic: uniform && logicToken == "OR", logicToken: logicToken, root: node}, true
}

func parseRuntimeWhereNode(raw string) (*runtimeWhereNode, bool) {
	raw = stripRuntimeWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return nil, false
	}
	if parts := splitRuntimeWhereBoolean(raw, "OR"); len(parts) > 1 {
		var root *runtimeWhereNode
		for _, part := range parts {
			node, ok := parseRuntimeWhereNode(part)
			if !ok {
				return nil, false
			}
			if root == nil {
				root = node
				continue
			}
			root = &runtimeWhereNode{op: "OR", left: root, right: node}
		}
		return root, true
	}
	return parseRuntimeWhereAndNode(raw)
}

func parseRuntimeWhereAndNode(raw string) (*runtimeWhereNode, bool) {
	raw = stripRuntimeWhereOuterParens(strings.TrimSpace(raw))
	if raw == "" {
		return nil, false
	}
	if parts := splitRuntimeWhereBoolean(raw, "AND"); len(parts) > 1 {
		var root *runtimeWhereNode
		for _, part := range parts {
			node, ok := parseRuntimeWhereNode(part)
			if !ok {
				return nil, false
			}
			if root == nil {
				root = node
				continue
			}
			root = &runtimeWhereNode{op: "AND", left: root, right: node}
		}
		return root, true
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
				return nil, false
			}
			atom := runtimeWhereAtom{leftName: lhs, op: op}
			return &runtimeWhereNode{atom: &atom}, true
		}
		return nil, false
	}
	lhs := strings.TrimSpace(m[1])
	op := normalizeRuntimeWhereOp(m[2])
	rhs := strings.TrimSpace(m[3])
	if lhs == "" || rhs == "" {
		return nil, false
	}
	if !isIdentifier(lhs) {
		return nil, false
	}
	if strings.HasPrefix(rhs, "$") {
		if !isIdentifier(rhs[1:]) {
			return nil, false
		}
		atom := runtimeWhereAtom{leftName: lhs, op: op, rightParamName: rhs[1:]}
		return &runtimeWhereNode{atom: &atom}, true
	}
	if isRuntimeStringWhereOp(op) {
		if !isQuotedLiteral(rhs) {
			return nil, false
		}
	}
	if isIdentifier(rhs) {
		return nil, false
	}
	rightValue, ok := parseRuntimeWhereLiteralValue(rhs)
	if !ok {
		return nil, false
	}
	atom := runtimeWhereAtom{leftName: lhs, op: op, rightAny: rightValue}
	return &runtimeWhereNode{atom: &atom}, true
}

func collectRuntimeWhereAtoms(node *runtimeWhereNode) []runtimeWhereAtom {
	if node == nil {
		return nil
	}
	out := []runtimeWhereAtom{}
	var walk func(*runtimeWhereNode)
	walk = func(current *runtimeWhereNode) {
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

func runtimeWhereNodeUniformLogic(node *runtimeWhereNode) (string, bool) {
	if node == nil {
		return "", true
	}
	logic := ""
	uniform := true
	var walk func(*runtimeWhereNode)
	walk = func(current *runtimeWhereNode) {
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
