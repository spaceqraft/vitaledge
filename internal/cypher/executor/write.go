package executor

import (
	"bytes"
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
	"sync/atomic"
	"time"
	"unicode"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

var (
	createVertexPatternRE            = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternRE              = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternReverseRE       = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternUndirectedRE    = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternTwoDirectionsRE = regexp.MustCompile(`\)<-\[[^\]]*\]->\(`)
	createChainNodeTokenRE           = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)`)
	createChainRelForwardTokenRE     = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->`)
	createChainRelReverseTokenRE     = regexp.MustCompile(`^<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createChainRelUndirTokenRE       = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createMissingRelTypeForwardRE    = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--?>\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeReverseRE    = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)<--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeUndirRE      = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createVariableLengthRelRE        = regexp.MustCompile(`\[[^\]]*\*[^\]]*\]`)
	setClauseRE                      = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	setMapReplaceClauseRE            = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	setMapAppendClauseRE             = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\+=(.+)$`)
	setLabelClauseRE                 = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)$`)
	removeClauseRE                   = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)
	removeLabelClauseRE              = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)$`)
)

type createEdgeDirection string

const (
	createEdgeDirectionForward    createEdgeDirection = "forward"
	createEdgeDirectionReverse    createEdgeDirection = "reverse"
	createEdgeDirectionUndirected createEdgeDirection = "undirected"
)

type createChainNodePattern struct {
	Var      string
	Labels   []string
	PropsRaw string
}

type createVertexPatternSpec struct {
	Var      string
	Labels   []string
	PropsRaw string
}

type createChainRelPattern struct {
	Var       string
	Type      string
	PropsRaw  string
	Direction createEdgeDirection
}

type createChainPattern struct {
	Nodes []createChainNodePattern
	Rels  []createChainRelPattern
}

type deletedVertexBinding struct {
	Tenant string
	ID     string
	Labels []string
}

type deletedEdgeBinding struct {
	Tenant string
	ID     string
	Type   string
}

const mergeCreatedMarkerKey = "__ve_merge_created"
const projectionEvalExecutorParam = "__ve_projection_eval_executor"
const projectionEvalTxParam = "__ve_projection_eval_tx"
const projectionEvalCtxParam = "__ve_projection_eval_ctx"

func withProjectionEvalRuntime(ctx context.Context, tx graph.Tx, params Params, exec *Executor) Params {
	if params == nil {
		params = Params{}
	}
	runtime := make(Params, len(params)+3)
	for key, value := range params {
		runtime[key] = value
	}
	runtime[projectionEvalExecutorParam] = exec
	runtime[projectionEvalTxParam] = tx
	runtime[projectionEvalCtxParam] = ctx
	return runtime
}

var autoVertexIDSeq uint64
var autoEdgeIDSeq uint64

func (e *Executor) executeQueryStatement(ctx context.Context, stmt *ast.QueryStatement, params Params) (_ *Result, err error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}
	if len(stmt.Parts) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "at least one query part is required", nil)
	}
	if len(stmt.Unions) != 0 && len(stmt.Unions) != len(stmt.Parts)-1 {
		return nil, &parser.ParseError{Kind: parser.ParseErrorInternal, Message: "invalid union boundaries", Statement: 1}
	}
	if err := validateUnionKinds(stmt.Unions); err != nil {
		return nil, err
	}

	writeQuery := false
	for _, part := range stmt.Parts {
		if hasWriteClause(part) {
			writeQuery = true
			break
		}
	}

	resultRows := []Row{}
	resultColumns := []string{}
	hasAnyReturn := false

	withTx := func(tx graph.Tx) error {
		for idx, part := range stmt.Parts {
			partRows, partColumns, returnSeen, stepErr := e.executeQueryPart(ctx, tx, part, params)
			if stepErr != nil {
				return stepErr
			}
			if !returnSeen {
				if len(stmt.Parts) > 1 {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: 1}
				}
				continue
			}

			hasAnyReturn = true
			if idx == 0 {
				resultRows = append(resultRows, partRows...)
				resultColumns = append([]string(nil), partColumns...)
				continue
			}

			if !equalStringSlices(resultColumns, partColumns) {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "DifferentColumnsInUnion", Statement: 1}
			}

			op := stmt.Unions[idx-1]
			if op == ast.UnionKindAll {
				resultRows = append(resultRows, partRows...)
				continue
			}
			resultRows = append(resultRows, partRows...)
			resultRows = distinctProjectionRows(resultRows)
		}
		return nil
	}

	if writeQuery {
		err = e.store.Update(ctx, withTx)
	} else {
		err = e.store.View(ctx, withTx)
	}
	if err != nil {
		return nil, err
	}

	if !hasAnyReturn {
		resultColumns = nil
		resultRows = nil
	}

	result := &Result{Columns: resultColumns, Rows: resultRows, Stats: Stats{RowsReturned: len(resultRows)}}
	result.Rows = normalizeResultRows(result.Rows)
	return result, nil
}

func (e *Executor) executeQueryPart(ctx context.Context, tx graph.Tx, part ast.QueryPart, params Params) ([]Row, []string, bool, error) {
	rows := []Row{{}}
	resultColumns := []string{}
	returnSeen := false

	for _, clause := range part.Clauses {
		var stepErr error
		switch clause.Kind {
		case ast.ClauseKindMatch:
			rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferMatchScopeColumns(clause.Raw)...)
		case ast.ClauseKindOptionalMatch:
			rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferMatchScopeColumns(clause.Raw)...)
		case ast.ClauseKindUnwind:
			rows, stepErr = e.applyUnwindClause(rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferColumnsFromRows(rows)...)
		case ast.ClauseKindWith:
			rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, clause, params, resultColumns, false)
		case ast.ClauseKindCreate:
			rows, stepErr = e.applyCreateClause(ctx, tx, rows, clause, params, false)
		case ast.ClauseKindMerge:
			rows, stepErr = e.applyCreateClause(ctx, tx, rows, clause, params, true)
		case ast.ClauseKindSet:
			rows, stepErr = e.applySetClause(ctx, tx, rows, clause, params)
		case ast.ClauseKindRemove:
			rows, stepErr = e.applyRemoveClause(ctx, tx, rows, clause, params)
		case ast.ClauseKindDelete:
			rows, stepErr = e.applyDeleteClause(ctx, tx, rows, clause, params)
		case ast.ClauseKindReturn:
			rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, clause, params, resultColumns, true)
			returnSeen = true
			if stepErr != nil {
				return nil, nil, false, stepErr
			}
			return rows, resultColumns, true, nil
		case ast.ClauseKindInQueryCall:
			rows, stepErr = e.applyInQueryCallClause(rows, clause, params)
		default:
			return nil, nil, false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("clause %s is not yet supported", clause.Kind), nil)
		}
		if stepErr != nil {
			return nil, nil, false, stepErr
		}
	}

	return rows, resultColumns, returnSeen, nil
}

func validateUnionKinds(kinds []ast.UnionKind) error {
	if len(kinds) <= 1 {
		return nil
	}
	first := kinds[0]
	for _, kind := range kinds[1:] {
		if kind != first {
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: 1}
		}
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasWriteClause(part ast.QueryPart) bool {
	for _, clause := range part.Clauses {
		switch clause.Kind {
		case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete:
			return true
		}
	}
	return false
}

func rowsAreEmpty(rows []Row) bool {
	for _, row := range rows {
		if len(row) > 0 {
			return false
		}
	}
	return true
}

func (e *Executor) applyMatchClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	spec, err := parseAnchoredMatchClauseRaw(clause.Raw)
	if err != nil {
		return nil, err
	}
	patternRaw := spec.Pattern
	expansionSpec := spec
	pathVar := ""
	if boundVar, innerPattern, ok := parseBoundPathPattern(spec.Pattern); ok {
		pathVar = boundVar
		patternRaw = innerPattern
		expansionSpec.Pattern = innerPattern
	}
	if parts := splitTopLevelCommaSeparated(patternRaw); len(parts) > 1 {
		return e.expandCompositeMatch(ctx, tx, rows, spec, parts, params)
	}
	if multi, ok := parseMultiNodeMatchPattern(patternRaw); ok {
		return e.expandMultiNodeMatch(ctx, tx, rows, spec, multi, params)
	}
	if node, err := parseNodePattern(patternRaw); err == nil {
		return e.expandNodeMatch(ctx, tx, rows, spec, node, params, pathVar)
	}
	if anchored, err := parseAnchoredOutPattern(patternRaw); err == nil {
		if shouldUseAnchoredOutPath(rows, anchored) {
			return e.expandAnchoredMatch(ctx, tx, rows, expansionSpec, params, pathVar)
		}
	}
	if directed, err := parseDirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandDirectedAdjacentMatch(ctx, tx, rows, spec, directed, params, pathVar)
	}
	if reverseDirected, err := parseReverseDirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandReverseDirectedAdjacentMatch(ctx, tx, rows, spec, reverseDirected, params, pathVar)
	}
	if undirected, err := parseUndirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandUndirectedAdjacentMatch(ctx, tx, rows, spec, undirected, params, pathVar)
	}
	if rel, err := parseDirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandDirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachBoundPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "forward")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if rel, err := parseReverseDirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandReverseDirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachBoundPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "reverse")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if rel, err := parseUndirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandUndirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachBoundPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "undirected")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if chain, err := parseDirectedRelationshipThenAdjacentPattern(patternRaw); err == nil {
		return e.expandDirectedRelationshipThenAdjacentMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedThenUndirectedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandDirectedThenUndirectedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseReverseRelationshipThenUndirectedVariableLengthPattern(patternRaw); err == nil {
		return e.expandReverseRelationshipThenUndirectedVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedAdjacentThenVariableLengthPattern(patternRaw); err == nil {
		return e.expandDirectedAdjacentThenVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedVariableLengthThenDirectedVariableLengthPattern(patternRaw); err == nil {
		return e.expandDirectedVariableLengthThenDirectedVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseMixedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandMixedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if rewritten, ok := rewriteReverseVariableLengthPatternPredicate(patternRaw); ok {
		if rel, err := parseDirectedVariableLengthRelationshipPattern(rewritten); err == nil {
			return e.expandVariableLengthDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
		}
	}
	if rel, err := parseDirectedVariableLengthRelationshipPattern(patternRaw); err == nil {
		return e.expandVariableLengthDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
	}
	if rel, err := parseUndirectedVariableLengthRelationshipPattern(patternRaw); err == nil {
		return e.expandVariableLengthUndirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
	}
	if chain, err := parseTwoHopDirectedChainPattern(patternRaw); err == nil {
		return e.expandTwoHopDirectedChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseTwoHopUndirectedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandTwoHopUndirectedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseMultiHopAdjacentChainPattern(patternRaw); err == nil {
		matched, matchErr := e.expandMultiHopAdjacentChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
		if matchErr != nil {
			return nil, matchErr
		}
		ensureOptionalPathBinding(matched, pathVar)
		return matched, nil
	}
	return e.expandAnchoredMatch(ctx, tx, rows, expansionSpec, params, pathVar)
}

func (e *Executor) expandCompositeMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, parts []string, params Params) ([]Row, error) {
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	raw := strings.TrimSpace(spec.Pattern)
	if raw == "" {
		return rows, nil
	}
	matchVars := inferMatchScopeColumns("MATCH " + raw)

	out := make([]Row, 0)
	for _, row := range rows {
		partials := []Row{cloneRow(row)}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			next, err := e.applyMatchClause(ctx, tx, partials, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + part}, params)
			if err != nil {
				return nil, err
			}
			partials = next
			if len(partials) == 0 {
				break
			}
		}

		matched := false
		if len(partials) > 0 {
			for _, partial := range partials {
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, partial, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				matched = true
				out = append(out, partial)
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			for _, name := range matchVars {
				setOptionalNoMatchBinding(merged, row, name)
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func parseBoundPathPattern(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || raw[i-1] != '\\') && !inSingle {
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
			depthParen--
		case '[':
			depthBracket++
		case ']':
			depthBracket--
		case '{':
			depthBrace++
		case '}':
			depthBrace--
		case '=':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				left := strings.TrimSpace(raw[:i])
				right := strings.TrimSpace(raw[i+1:])
				if !identifierRE.MatchString(left) || !strings.HasPrefix(right, "(") {
					return "", "", false
				}
				return left, right, true
			}
		}
	}
	return "", "", false
}

func attachBoundPathValues(rows []Row, pathVar, leftVar, edgeVar, rightVar, direction string) {
	if pathVar == "" {
		return
	}
	for _, row := range rows {
		left := vertexFromRowBinding(row, leftVar)
		edge := edgeFromRowBinding(row, edgeVar)
		right := vertexFromRowBinding(row, rightVar)
		row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: direction}
	}
}

type cypherPathValue struct {
	Left      *graph.Vertex
	Edge      *graph.Edge
	Right     *graph.Vertex
	Direction string
}

func (p cypherPathValue) String() string {
	left := renderPathNode(p.Left)
	if p.Edge == nil && p.Right == nil {
		return "<" + left + ">"
	}
	right := renderPathNode(p.Right)
	edge := renderPathEdge(p.Edge)
	switch p.Direction {
	case "reverse":
		return "<" + left + "<-" + edge + "-" + right + ">"
	case "undirected":
		return "<" + left + "-" + edge + "-" + right + ">"
	default:
		return "<" + left + "-" + edge + "->" + right + ">"
	}
}

func renderPathNode(v *graph.Vertex) string {
	if v == nil {
		return "()"
	}
	labels := append([]string(nil), v.Labels...)
	b := strings.Builder{}
	b.WriteString("(")
	for _, label := range labels {
		b.WriteString(":" + label)
	}
	if len(v.Properties) > 0 {
		parts := make([]string, 0, len(v.Properties))
		for key, raw := range v.Properties {
			parts = append(parts, key+": "+renderPathLiteral(decodeStoredPropertyValue(raw)))
		}
		sort.Strings(parts)
		if len(labels) > 0 {
			b.WriteString(" ")
		}
		b.WriteString("{" + strings.Join(parts, ", ") + "}")
	}
	b.WriteString(")")
	return b.String()
}

func renderPathEdge(e *graph.Edge) string {
	if e == nil {
		return "[]"
	}
	b := strings.Builder{}
	b.WriteString("[")
	if strings.TrimSpace(e.Type) != "" {
		b.WriteString(":" + e.Type)
	}
	if len(e.Properties) > 0 {
		parts := make([]string, 0, len(e.Properties))
		for key, raw := range e.Properties {
			parts = append(parts, key+": "+renderPathLiteral(decodeStoredPropertyValue(raw)))
		}
		sort.Strings(parts)
		if strings.TrimSpace(e.Type) != "" {
			b.WriteString(" ")
		}
		b.WriteString("{" + strings.Join(parts, ", ") + "}")
	}
	b.WriteString("]")
	return b.String()
}

func renderPathLiteral(v any) string {
	switch typed := normalizeResultValue(v).(type) {
	case nil:
		return "null"
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "\\'") + "'"
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			parts = append(parts, renderPathLiteral(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+": "+renderPathLiteral(typed[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprint(typed)
	}
}

func vertexFromRowBinding(row Row, key string) *graph.Vertex {
	if strings.TrimSpace(key) == "" || row == nil {
		return nil
	}
	if value, ok := row[key]; ok {
		if vertex, ok := value.(*graph.Vertex); ok {
			return vertex
		}
	}
	return nil
}

func edgeFromRowBinding(row Row, key string) *graph.Edge {
	if row == nil {
		return nil
	}
	if strings.TrimSpace(key) != "" {
		if value, ok := row[key]; ok {
			if edge, ok := value.(*graph.Edge); ok {
				return edge
			}
		}
	}
	if value, ok := row["edge"]; ok {
		if edge, ok := value.(*graph.Edge); ok {
			return edge
		}
	}
	return nil
}

func parseMultiNodeMatchPattern(raw string) ([]nodePattern, bool) {
	parts := splitTopLevelCommaSeparated(raw)
	if len(parts) <= 1 {
		return nil, false
	}
	out := make([]nodePattern, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		node, err := parseNodePattern(part)
		if err != nil {
			return nil, false
		}
		out = append(out, node)
	}
	return out, true
}

func (e *Executor) expandMultiNodeMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, patterns []nodePattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		partials := []Row{cloneRow(row)}
		for _, pattern := range patterns {
			next := make([]Row, 0)
			for _, partial := range partials {
				candidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, partial, pattern, params)
				if err != nil {
					return nil, err
				}
				if len(candidates) == 0 {
					continue
				}
				for _, candidate := range candidates {
					merged := cloneRow(partial)
					if pattern.Var != "" {
						merged[pattern.Var] = candidate
					}
					next = append(next, merged)
				}
			}
			partials = next
			if len(partials) == 0 {
				break
			}
		}

		if len(partials) == 0 {
			if spec.Optional {
				merged := cloneRow(row)
				for _, pattern := range patterns {
					if pattern.Var != "" {
						merged[pattern.Var] = nil
					}
				}
				out = append(out, merged)
			}
			continue
		}

		for _, partial := range partials {
			if spec.Where != "" {
				ok, err := e.evalWhereExpression(ctx, tx, spec.Where, partial, params)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			out = append(out, partial)
		}
	}

	return out, nil
}

func shouldUseAnchoredOutPath(rows []Row, pattern anchoredOutPattern) bool {
	if strings.TrimSpace(pattern.SourcePropertiesRaw) != "" {
		return true
	}
	if strings.TrimSpace(pattern.SourceIDParam) != "" {
		return true
	}
	if strings.TrimSpace(pattern.SourceVar) == "" {
		return false
	}
	for _, row := range rows {
		if _, ok := row[pattern.SourceVar]; ok {
			return true
		}
	}
	return false
}

type anchoredMatchSpec struct {
	Optional      bool
	Pattern       string
	SourceVar     string
	SourceIDParam string
	EdgeType      string
	TargetVar     string
	Where         string
}

func parseAnchoredMatchClauseRaw(raw string) (anchoredMatchSpec, error) {
	raw = normalizeClauseBody(raw)
	spec := anchoredMatchSpec{}
	if strings.HasPrefix(raw, "OPTIONALMATCH") {
		spec.Optional = true
		raw = strings.TrimPrefix(raw, "OPTIONALMATCH")
	} else if strings.HasPrefix(raw, "MATCH") {
		raw = strings.TrimPrefix(raw, "MATCH")
	} else {
		return anchoredMatchSpec{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("match clause %q is not supported", raw), nil)
	}
	pattern, where, ok := splitTopLevelMatchWhere(raw)
	if !ok {
		spec.Pattern = pattern
		return spec, nil
	}
	spec.Pattern = pattern
	spec.Where = where
	return spec, nil
}

func splitTopLevelMatchWhere(raw string) (string, string, bool) {
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	keyword := "WHERE"

	for i := 0; i <= len(upper)-len(keyword); i++ {
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

		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		if i > 0 && isAlphaNumericOrUnderscore(raw[i-1]) {
			continue
		}

		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left == "" || right == "" {
			continue
		}
		return raw[:i], raw[i+len(keyword):], true
	}

	return raw, "", false
}

func isAlphaNumericOrUnderscore(ch byte) bool {
	if ch == '_' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func (e *Executor) expandAnchoredMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, params Params, pathVar string) ([]Row, error) {
	pattern, err := parseAnchoredOutPattern(spec.Pattern)
	if err != nil {
		return nil, err
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		sources, err := e.resolveAnchoredSources(ctx, tx, tenant, row, pattern, params)
		if err != nil {
			return nil, err
		}
		if len(sources) == 0 {
			if spec.Optional {
				merged := cloneRow(row)
				setOptionalNoMatchBinding(merged, row, pattern.SourceVar)
				setOptionalNoMatchBinding(merged, row, pattern.TargetVar)
				merged["edge"] = nil
				if pathVar != "" {
					merged[pathVar] = nil
				}
				out = append(out, merged)
			}
			continue
		}

		matched := false
		for _, src := range sources {
			if src == nil {
				continue
			}
			if !vertexBindingMatches(row, pattern.SourceVar, src) {
				continue
			}
			srcID := src.ID
			if err := tx.ScanOutEdges(ctx, tenant, srcID, pattern.EdgeType, 0, func(edge *graph.Edge) error {
				dst, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if spec.Optional && graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(row, pattern.TargetVar, dst) {
					return nil
				}

				merged := cloneRow(row)
				merged[pattern.SourceVar] = src
				merged[pattern.TargetVar] = dst
				merged["edge"] = edge
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: src, Edge: edge, Right: dst, Direction: "forward"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.SourceVar)
			setOptionalNoMatchBinding(merged, row, pattern.TargetVar)
			merged["edge"] = nil
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) resolveAnchoredSources(ctx context.Context, tx graph.Tx, tenant string, row Row, pattern anchoredOutPattern, params Params) ([]*graph.Vertex, error) {
	if prop, value, ok := anchoredSourcePropertyEquality(pattern, params, row); ok {
		indexed := e.indexCatalog != nil && pattern.SourceLabel != "" && e.indexCatalog.HasPropertyIndex(tenant, pattern.SourceLabel, prop)
		e.metrics.ObserveIndexCandidate(tenant, pattern.SourceLabel, prop, indexed)
		if !indexed {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("MATCH source property lookup requires configured index on %s.%s", pattern.SourceLabel, prop), nil)
		}

		encoded := valueToBytes(value)
		ids := map[string]struct{}{}
		err := tx.ScanPropertyIndex(ctx, tenant, pattern.SourceLabel, prop, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
			ids[entry.EntityID] = struct{}{}
			return nil
		})
		if err != nil {
			e.metrics.ObserveIndexLookup("property_index", "error", 0)
			return nil, err
		}
		if len(ids) == 0 {
			e.metrics.ObserveIndexLookup("property_index", "miss", 0)
			return nil, nil
		}
		out := make([]*graph.Vertex, 0, len(ids))
		for id := range ids {
			vertex, err := tx.GetVertex(ctx, tenant, id)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					continue
				}
				return nil, err
			}
			if !vertexMatchesProperty(vertex, prop, encoded, pattern.SourceLabel) {
				continue
			}
			out = append(out, vertex)
		}
		e.metrics.ObserveIndexLookup("property_index", "hit", len(out))
		return out, nil
	}

	srcID, err := resolvePatternSourceID(row, params, pattern.SourceVar, pattern.SourceIDParam)
	if err != nil {
		e.metrics.ObserveIndexLookup("id_lookup", "error", 0)
		return nil, err
	}
	vertex, err := tx.GetVertex(ctx, tenant, srcID)
	if err != nil {
		e.metrics.ObserveIndexLookup("id_lookup", "error", 0)
		return nil, err
	}
	e.metrics.ObserveIndexLookup("id_lookup", "hit", 1)
	return []*graph.Vertex{vertex}, nil
}

func (e *Executor) expandNodeMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern nodePattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		candidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, candidate := range candidates {
			if candidate == nil {
				continue
			}
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = cypherPathValue{Left: candidate}
			}
			if pattern.Var != "" {
				merged[pattern.Var] = candidate
			}

			if spec.Where != "" {
				ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}

			matched = true
			out = append(out, merged)
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandUndirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		if pathVar != "" {
			row = cloneRow(row)
			row[pathVar] = nil
		}
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			emitted := map[string]struct{}{}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			handleAdjacent := func(edge *graph.Edge, otherID string) error {
				if edge == nil {
					return nil
				}
				key := edge.ID + "|" + otherID
				if _, seen := emitted[key]; seen {
					return nil
				}
				emitted[key] = struct{}{}

				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pathVar != "" {
					direction := "undirected"
					if edge.SrcID == left.ID && edge.DstID == neighbor.ID {
						direction = "forward"
					} else if edge.DstID == left.ID && edge.SrcID == neighbor.ID {
						direction = "reverse"
					}
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: neighbor, Direction: direction}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				return handleAdjacent(edge, edge.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				return handleAdjacent(edge, edge.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		if pathVar != "" {
			row = cloneRow(row)
			row[pathVar] = nil
		}
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				neighbor, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: neighbor, Direction: "forward"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseDirectedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		rightCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Right, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, right := range rightCandidates {
			if right == nil {
				continue
			}
			rowWithRight := cloneRow(row)
			if pattern.Right.Var != "" {
				rowWithRight[pattern.Right.Var] = right
			}

			if err := tx.ScanOutEdges(ctx, tenant, right.ID, "", 0, func(edge *graph.Edge) error {
				left, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithRight, pattern.Left.Var, left) {
					return nil
				}
				if !nodePatternMatches(left, pattern.Left, params, rowWithRight) {
					return nil
				}

				merged := cloneRow(rowWithRight)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: "reverse"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := tx.ScanOutEdges(ctx, tenant, left.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}
				if !edgePatternMatches(edge, pattern.EdgeProps, params, row) {
					return nil
				}
				neighbor, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseDirectedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}
				if !edgePatternMatches(edge, pattern.EdgeProps, params, rowWithLeft) {
					return nil
				}
				right, err := tx.GetVertex(ctx, tenant, edge.SrcID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, right) {
					return nil
				}
				if !nodePatternMatches(right, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandUndirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			emitted := map[string]struct{}{}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			handle := func(edge *graph.Edge, otherID string) error {
				key := edge.ID + "|" + otherID
				if _, seen := emitted[key]; seen {
					return nil
				}
				emitted[key] = struct{}{}

				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}

				if !edgePatternMatches(edge, pattern.EdgeProps, params, rowWithLeft) {
					return nil
				}
				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := tx.ScanOutEdges(ctx, tenant, left.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				return handle(edge, edge.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				return handle(edge, edge.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func edgeSequenceBindingMatches(row Row, varName string, edges []*graph.Edge) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	if binding == nil {
		return false
	}

	sameIDs := func(bound []*graph.Edge) bool {
		if len(bound) != len(edges) {
			return false
		}
		for i := range bound {
			if bound[i] == nil || edges[i] == nil {
				return false
			}
			if bound[i].ID != edges[i].ID {
				return false
			}
		}
		return true
	}

	switch typed := binding.(type) {
	case []*graph.Edge:
		return sameIDs(typed)
	case []any:
		if len(typed) != len(edges) {
			return false
		}
		for i, item := range typed {
			edge, ok := item.(*graph.Edge)
			if !ok || edge == nil || edges[i] == nil || edge.ID != edges[i].ID {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func edgeSequenceToAny(edges []*graph.Edge) []any {
	out := make([]any, len(edges))
	for i, edge := range edges {
		out[i] = edge
	}
	return out
}

func (e *Executor) expandVariableLengthDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedVariableLengthRelationshipPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			emitMatch := func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string) error {
				depth := len(edges)
				if depth < pattern.MinHops {
					return nil
				}
				if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
					return nil
				}
				if !vertexBindingMatches(baseRow, pattern.Right.Var, current) {
					return nil
				}

				merged := cloneRow(baseRow)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = current
				}
				if !edgeSequenceBindingMatches(baseRow, pattern.EdgeVar, edges) {
					return nil
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edgeSequenceToAny(edges)
				}
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(nodes, edges, dirs)
				}
				if !nodePatternMatches(current, pattern.Right, params, merged) {
					return nil
				}
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := emitMatch(left, []*graph.Vertex{left}, []*graph.Edge{}, []string{}); err != nil {
				return nil, err
			}

			var walk func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
					return nil
				}
				scanType := pattern.EdgeType
				if len(pattern.EdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.EdgeProps, params, baseRow) {
						return nil
					}
					right, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}

					nextNodes := append(append([]*graph.Vertex{}, nodes...), right)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")

					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true

					if err := emitMatch(right, nextNodes, nextEdges, nextDirs); err != nil {
						return err
					}

					return walk(right, nextNodes, nextEdges, nextDirs, nextUsed)
				})
			}

			if err := walk(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedVariableLengthThenDirectedVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedVariableLengthThenDirectedVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			var walkSecond func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool, midRow Row, firstEdgeCount int) error
			walkSecond = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool, midRow Row, firstEdgeCount int) error {
				depthSecond := len(edges) - firstEdgeCount
				if depthSecond >= pattern.SecondMinHops && (pattern.SecondMaxHops < 0 || depthSecond <= pattern.SecondMaxHops) {
					if vertexBindingMatches(midRow, pattern.Right.Var, current) {
						merged := cloneRow(midRow)
						if pattern.Right.Var != "" {
							merged[pattern.Right.Var] = current
						}
						if pathVar != "" {
							merged[pathVar] = multiHopPathValue(nodes, edges, dirs)
						}
						if nodePatternMatches(current, pattern.Right, params, merged) {
							if spec.Where != "" {
								ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
								if err != nil {
									return err
								}
								if ok {
									matched = true
									out = append(out, merged)
								}
							} else {
								matched = true
								out = append(out, merged)
							}
						}
					}
				}
				if pattern.SecondMaxHops >= 0 && depthSecond >= pattern.SecondMaxHops {
					return nil
				}
				scanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.SecondEdgeProps, params, midRow) {
						return nil
					}
					next, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					nextNodes := append(append([]*graph.Vertex{}, nodes...), next)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")
					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true
					return walkSecond(next, nextNodes, nextEdges, nextDirs, nextUsed, midRow, firstEdgeCount)
				})
			}

			var walkFirst func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walkFirst = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				depthFirst := len(edges)
				if depthFirst >= pattern.FirstMinHops {
					if pattern.Mid.Var == "" || vertexBindingMatches(baseRow, pattern.Mid.Var, current) {
						midRow := cloneRow(baseRow)
						if pattern.Mid.Var != "" {
							midRow[pattern.Mid.Var] = current
						}
						if nodePatternMatches(current, pattern.Mid, params, midRow) {
							usedForSecond := make(map[string]bool, len(used))
							for key := range used {
								usedForSecond[key] = true
							}
							if err := walkSecond(current, nodes, edges, dirs, usedForSecond, midRow, depthFirst); err != nil {
								return err
							}
						}
					}
				}

				if pattern.FirstMaxHops >= 0 && depthFirst >= pattern.FirstMaxHops {
					return nil
				}
				scanType := pattern.FirstEdgeType
				if len(pattern.FirstEdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.FirstEdgeProps, params, baseRow) {
						return nil
					}
					next, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					nextNodes := append(append([]*graph.Vertex{}, nodes...), next)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")
					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true
					return walkFirst(next, nextNodes, nextEdges, nextDirs, nextUsed)
				})
			}

			if err := walkFirst(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandVariableLengthUndirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedVariableLengthRelationshipPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			emitMatch := func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string) error {
				depth := len(edges)
				if depth < pattern.MinHops {
					return nil
				}
				if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
					return nil
				}
				if !vertexBindingMatches(baseRow, pattern.Right.Var, current) {
					return nil
				}

				merged := cloneRow(baseRow)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = current
				}
				if !edgeSequenceBindingMatches(baseRow, pattern.EdgeVar, edges) {
					return nil
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edgeSequenceToAny(edges)
				}
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(nodes, edges, dirs)
				}
				if !nodePatternMatches(current, pattern.Right, params, merged) {
					return nil
				}
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := emitMatch(left, []*graph.Vertex{left}, []*graph.Edge{}, []string{}); err != nil {
				return nil, err
			}

			var walk func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
					return nil
				}
				type neighborEdge struct {
					edge     *graph.Edge
					neighbor *graph.Vertex
					dir      string
				}
				neighbors := make([]neighborEdge, 0)
				seen := map[string]struct{}{}
				collect := func(edge *graph.Edge, neighborID string, dir string) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.EdgeProps, params, baseRow) {
						return nil
					}
					key := edge.ID + "|" + neighborID
					if _, ok := seen[key]; ok {
						return nil
					}
					seen[key] = struct{}{}
					neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					neighbors = append(neighbors, neighborEdge{edge: edge, neighbor: neighbor, dir: dir})
					return nil
				}

				scanType := pattern.EdgeType
				if len(pattern.EdgeAnyOf) > 0 {
					scanType = ""
				}
				if err := tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					return collect(edge, edge.DstID, "forward")
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					return collect(edge, edge.SrcID, "reverse")
				}); err != nil {
					return err
				}

				for _, candidate := range neighbors {
					nextNodes := append(append([]*graph.Vertex{}, nodes...), candidate.neighbor)
					nextEdges := append(append([]*graph.Edge{}, edges...), candidate.edge)
					nextDirs := append(append([]string{}, dirs...), candidate.dir)

					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[candidate.edge.ID] = true

					if err := emitMatch(candidate.neighbor, nextNodes, nextEdges, nextDirs); err != nil {
						return err
					}

					if err := walk(candidate.neighbor, nextNodes, nextEdges, nextDirs, nextUsed); err != nil {
						return err
					}
				}
				return nil
			}

			if err := walk(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedAdjacentThenVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedAdjacentThenVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge1 *graph.Edge) error {
				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}

				midRow := cloneRow(row)
				if pattern.Left.Var != "" {
					midRow[pattern.Left.Var] = left
				}
				if !vertexBindingMatches(midRow, pattern.Mid.Var, mid) {
					return nil
				}
				if pattern.Mid.Var != "" {
					midRow[pattern.Mid.Var] = mid
				}
				if !nodePatternMatches(mid, pattern.Mid, params, midRow) {
					return nil
				}

				var walk func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
				walk = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
					return tx.ScanOutEdges(ctx, tenant, current.ID, "", 0, func(edge *graph.Edge) error {
						if edge == nil || used[edge.ID] {
							return nil
						}
						right, err := tx.GetVertex(ctx, tenant, edge.DstID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}

						nextNodes := append(append([]*graph.Vertex{}, nodes...), right)
						nextEdges := append(append([]*graph.Edge{}, edges...), edge)
						nextDirs := append(append([]string{}, dirs...), "forward")

						nextUsed := make(map[string]bool, len(used)+1)
						for key := range used {
							nextUsed[key] = true
						}
						nextUsed[edge.ID] = true

						if vertexBindingMatches(midRow, pattern.Right.Var, right) {
							merged := cloneRow(midRow)
							if pattern.Right.Var != "" {
								merged[pattern.Right.Var] = right
							}
							if edgeSequenceBindingMatches(midRow, pattern.EdgeVar, nextEdges[1:]) {
								if pattern.EdgeVar != "" {
									merged[pattern.EdgeVar] = edgeSequenceToAny(nextEdges[1:])
								}
								if pathVar != "" {
									merged[pathVar] = multiHopPathValue(nextNodes, nextEdges, nextDirs)
								}
								if nodePatternMatches(right, pattern.Right, params, merged) {
									if spec.Where != "" {
										ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
										if err != nil {
											return err
										}
										if ok {
											matched = true
											out = append(out, merged)
										}
									} else {
										matched = true
										out = append(out, merged)
									}
								}
							}
						}

						return walk(right, nextNodes, nextEdges, nextDirs, nextUsed)
					})
				}

				initialEdges := []*graph.Edge{edge1}
				initialDirs := []string{"forward"}
				used := map[string]bool{edge1.ID: true}
				return walk(mid, []*graph.Vertex{left, mid}, initialEdges, initialDirs, used)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandTwoHopDirectedChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern twoHopDirectedChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, row) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}

				mergedMid := cloneRow(row)
				if pattern.Left.Var != "" {
					mergedMid[pattern.Left.Var] = left
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !vertexBindingMatches(mergedMid, pattern.Mid.Var, mid) {
					return nil
				}
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if !nodePatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}

				collectRight := func(edge2 *graph.Edge, rightID string) error {
					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, rightID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						directions := []string{"forward"}
						if pattern.SecondForward {
							directions = append(directions, "forward")
						} else {
							directions = append(directions, "reverse")
						}
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, directions)
					}
					if !nodePatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if pattern.SecondForward {
					if err := tx.ScanOutEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
						return collectRight(edge2, edge2.DstID)
					}); err != nil {
						return err
					}
				} else {
					if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
						return collectRight(edge2, edge2.SrcID)
					}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedRelationshipThenAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedRelationshipThenAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				scanType = ""
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, scanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}

				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !nodePatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, "", 0, func(edge2 *graph.Edge) error {
					if edge2 == nil || edge2.ID == edge1.ID {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, edge2.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"forward", "forward"})
					}
					if !nodePatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}); err != nil {
					return err
				}

				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandTwoHopUndirectedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern twoHopUndirectedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			emittedFirst := map[string]struct{}{}
			collectFirst := func(edge1 *graph.Edge, midID string) error {
				if edge1 == nil {
					return nil
				}
				key := edge1.ID + "|" + midID
				if _, seen := emittedFirst[key]; seen {
					return nil
				}
				emittedFirst[key] = struct{}{}

				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, midID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}

				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}
				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !nodePatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}

				emittedSecond := map[string]struct{}{}
				collectSecond := func(edge2 *graph.Edge, rightID string) error {
					if edge2 == nil {
						return nil
					}
					if edge2.ID == edge1.ID {
						return nil
					}
					secondKey := edge2.ID + "|" + rightID
					if _, seen := emittedSecond[secondKey]; seen {
						return nil
					}
					emittedSecond[secondKey] = struct{}{}

					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, rightID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"undirected", "undirected"})
					}
					if !nodePatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.DstID)
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.SrcID)
				}); err != nil {
					return err
				}

				return nil
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				return collectFirst(edge1, edge1.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				return collectFirst(edge1, edge1.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedThenUndirectedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedThenUndirectedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}

				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !nodePatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}

				emitted := map[string]struct{}{}
				collectSecond := func(edge2 *graph.Edge, rightID string, dir string) error {
					if edge2 == nil || edge2.ID == edge1.ID {
						return nil
					}
					key := edge2.ID + "|" + rightID
					if _, seen := emitted[key]; seen {
						return nil
					}
					emitted[key] = struct{}{}

					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, rightID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"forward", dir})
					}
					if !nodePatternMatches(right, pattern.Right, params, merged) {
						return nil
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.DstID, "forward")
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.SrcID, "reverse")
				}); err != nil {
					return err
				}

				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseRelationshipThenUndirectedVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseRelationshipThenUndirectedVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := tx.ScanInEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, baseRow) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.SrcID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(baseRow, pattern.Mid.Var, mid) {
					return nil
				}

				midRow := cloneRow(baseRow)
				if pattern.Mid.Var != "" {
					midRow[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					midRow[pattern.FirstEdgeVar] = edge1
				}
				if !nodePatternMatches(mid, pattern.Mid, params, midRow) {
					return nil
				}

				emitMatch := func(current *graph.Vertex, varNodes []*graph.Vertex, varEdges []*graph.Edge, varDirs []string) error {
					depth := len(varEdges)
					if depth < pattern.MinHops {
						return nil
					}
					if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
						return nil
					}
					if !vertexBindingMatches(midRow, pattern.Right.Var, current) {
						return nil
					}
					if !edgeSequenceBindingMatches(midRow, pattern.SecondEdgeVar, varEdges) {
						return nil
					}

					merged := cloneRow(midRow)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = current
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edgeSequenceToAny(varEdges)
					}
					pathNodes := append([]*graph.Vertex{left, mid}, varNodes...)
					pathEdges := append([]*graph.Edge{edge1}, varEdges...)
					pathDirs := append([]string{"reverse"}, varDirs...)
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue(pathNodes, pathEdges, pathDirs)
					}
					if !nodePatternMatches(current, pattern.Right, params, merged) {
						return nil
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := emitMatch(mid, []*graph.Vertex{}, []*graph.Edge{}, []string{}); err != nil {
					return err
				}

				var walk func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
				walk = func(current *graph.Vertex, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
					if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
						return nil
					}

					type neighborEdge struct {
						edge     *graph.Edge
						neighbor *graph.Vertex
						dir      string
					}
					neighbors := make([]neighborEdge, 0)
					seen := map[string]struct{}{}
					collect := func(edge *graph.Edge, neighborID string, dir string) error {
						if edge == nil || used[edge.ID] {
							return nil
						}
						if !edgeTypeMatches(edge, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
							return nil
						}
						if !edgePatternMatches(edge, pattern.SecondEdgeProps, params, midRow) {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, ok := seen[key]; ok {
							return nil
						}
						seen[key] = struct{}{}
						neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}
						neighbors = append(neighbors, neighborEdge{edge: edge, neighbor: neighbor, dir: dir})
						return nil
					}

					scanType := pattern.SecondEdgeType
					if len(pattern.SecondEdgeAnyOf) > 0 {
						scanType = ""
					}
					if err := tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.DstID, "forward")
					}); err != nil {
						return err
					}
					if err := tx.ScanInEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.SrcID, "reverse")
					}); err != nil {
						return err
					}

					for _, candidate := range neighbors {
						nextNodes := append(append([]*graph.Vertex{}, nodes...), candidate.neighbor)
						nextEdges := append(append([]*graph.Edge{}, edges...), candidate.edge)
						nextDirs := append(append([]string{}, dirs...), candidate.dir)

						nextUsed := make(map[string]bool, len(used)+1)
						for key := range used {
							nextUsed[key] = true
						}
						nextUsed[candidate.edge.ID] = true

						if err := emitMatch(candidate.neighbor, nextNodes, nextEdges, nextDirs); err != nil {
							return err
						}
						if err := walk(candidate.neighbor, nextNodes, nextEdges, nextDirs, nextUsed); err != nil {
							return err
						}
					}

					return nil
				}

				return walk(mid, []*graph.Vertex{}, []*graph.Edge{}, []string{}, map[string]bool{edge1.ID: true})
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandMixedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern mixedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	cloneUsed := func(src map[string]bool) map[string]bool {
		dst := make(map[string]bool, len(src)+1)
		for key := range src {
			dst[key] = true
		}
		return dst
	}

	out := make([]Row, 0)
	for _, row := range rows {
		startCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Nodes[0], params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, start := range startCandidates {
			if start == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Nodes[0].Var != "" {
				baseRow[pattern.Nodes[0].Var] = start
			}
			if !nodePatternMatches(start, pattern.Nodes[0], params, baseRow) {
				continue
			}

			var walk func(index int, current *graph.Vertex, currentRow Row, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(index int, current *graph.Vertex, currentRow Row, nodes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if index == len(pattern.Segments) {
					merged := cloneRow(currentRow)
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue(nodes, edges, dirs)
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}
					matched = true
					out = append(out, merged)
					return nil
				}

				segment := pattern.Segments[index]
				nextPattern := pattern.Nodes[index+1]

				minHops := segment.MinHops
				maxHops := segment.MaxHops
				if !segment.IsVariableLength {
					minHops = 1
					maxHops = 1
				}
				baseEdgeCount := len(edges)
				var explore func(vertex *graph.Vertex, pathNodes []*graph.Vertex, pathEdges []*graph.Edge, pathDirs []string, pathUsed map[string]bool) error
				explore = func(vertex *graph.Vertex, pathNodes []*graph.Vertex, pathEdges []*graph.Edge, pathDirs []string, pathUsed map[string]bool) error {
					segmentEdges := pathEdges[baseEdgeCount:]
					depth := len(segmentEdges)
					if depth >= minHops {
						if vertexBindingMatches(currentRow, nextPattern.Var, vertex) {
							nextRow := cloneRow(currentRow)
							if nextPattern.Var != "" {
								nextRow[nextPattern.Var] = vertex
							}
							segmentBindingOK := true
							if segment.IsVariableLength {
								segmentBindingOK = edgeSequenceBindingMatches(currentRow, segment.EdgeVar, segmentEdges)
								if segmentBindingOK && segment.EdgeVar != "" {
									nextRow[segment.EdgeVar] = edgeSequenceToAny(segmentEdges)
								}
							} else {
								segmentBindingOK = len(segmentEdges) == 1 && edgeBindingMatches(currentRow, segment.EdgeVar, segmentEdges[0])
								if segmentBindingOK && segment.EdgeVar != "" {
									nextRow[segment.EdgeVar] = segmentEdges[0]
								}
							}
							if segmentBindingOK && nodePatternMatches(vertex, nextPattern, params, nextRow) {
								if err := walk(index+1, vertex, nextRow, pathNodes, pathEdges, pathDirs, pathUsed); err != nil {
									return err
								}
							}
						}
					}
					if maxHops >= 0 && depth >= maxHops {
						return nil
					}

					scanType := segment.EdgeType
					if len(segment.EdgeAnyOf) > 0 {
						scanType = ""
					}
					emitted := map[string]struct{}{}
					collect := func(edge *graph.Edge, neighborID string, direction string) error {
						if edge == nil || used[edge.ID] || pathUsed[edge.ID] {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, ok := emitted[key]; ok {
							return nil
						}
						emitted[key] = struct{}{}
						if !edgeTypeMatches(edge, segment.EdgeType, segment.EdgeAnyOf) {
							return nil
						}
						if !edgePatternMatches(edge, segment.EdgeProps, params, currentRow) {
							return nil
						}
						neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}
						nextNodes := append(append([]*graph.Vertex{}, pathNodes...), neighbor)
						nextEdges := append(append([]*graph.Edge{}, pathEdges...), edge)
						nextDirs := append(append([]string{}, pathDirs...), direction)
						nextUsed := cloneUsed(pathUsed)
						nextUsed[edge.ID] = true
						return explore(neighbor, nextNodes, nextEdges, nextDirs, nextUsed)
					}

					if segment.Direction == "reverse" {
						return tx.ScanInEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.SrcID, "reverse")
						})
					}
					if segment.Direction == "undirected" {
						if err := tx.ScanOutEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.DstID, "forward")
						}); err != nil {
							return err
						}
						return tx.ScanInEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.SrcID, "reverse")
						})
					}
					return tx.ScanOutEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.DstID, "forward")
					})
				}

				return explore(current, nodes, edges, dirs, used)
			}

			if err := walk(0, start, baseRow, []*graph.Vertex{start}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			for _, node := range pattern.Nodes {
				setOptionalNoMatchBinding(merged, row, node.Var)
			}
			for _, segment := range pattern.Segments {
				setOptionalNoMatchBinding(merged, row, segment.EdgeVar)
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func ensureOptionalPathBinding(rows []Row, pathVar string) {
	if pathVar == "" {
		return
	}
	for _, row := range rows {
		if _, ok := row[pathVar]; !ok {
			row[pathVar] = nil
		}
	}
}

func setOptionalNoMatchBinding(dst Row, src Row, varName string) {
	if varName == "" {
		return
	}
	if _, bound := src[varName]; bound {
		dst[varName] = src[varName]
	} else {
		dst[varName] = nil
	}
}

func multiHopPathValue(nodes []*graph.Vertex, edges []*graph.Edge, directions []string) any {
	if len(nodes) == 0 {
		return nil
	}
	if len(nodes) == 1 {
		return cypherPathValue{Left: nodes[0]}
	}
	// Build the path as a serialized string similar to cypherPathValue.
	// For multi-hop, return a multiHopCypherPath struct.
	return multiHopCypherPath{Nodes: nodes, Edges: edges, Directions: directions}
}

type multiHopCypherPath struct {
	Nodes      []*graph.Vertex
	Edges      []*graph.Edge
	Directions []string
}

type multiHopPartialPath struct {
	Nodes      []*graph.Vertex
	Edges      []*graph.Edge
	Directions []string
	AccRow     Row
	UsedEdges  map[string]bool
}

func (p multiHopCypherPath) String() string {
	if len(p.Nodes) == 0 {
		return "<>"
	}
	b := strings.Builder{}
	b.WriteString("<")
	b.WriteString(renderPathNode(p.Nodes[0]))
	for i, edge := range p.Edges {
		dir := "forward"
		if i < len(p.Directions) {
			dir = p.Directions[i]
		}
		edgeStr := renderPathEdge(edge)
		switch dir {
		case "reverse":
			b.WriteString("<-" + edgeStr + "-")
		case "undirected":
			b.WriteString("-" + edgeStr + "-")
		default:
			b.WriteString("-" + edgeStr + "->")
		}
		if i+1 < len(p.Nodes) {
			b.WriteString(renderPathNode(p.Nodes[i+1]))
		}
	}
	b.WriteString(">")
	return b.String()
}

func (e *Executor) expandMultiHopAdjacentChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, chain multiHopAdjacentChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		startCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, chain.Start, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, start := range startCandidates {
			if start == nil {
				continue
			}

			accRow := cloneRow(row)
			if chain.Start.Var != "" {
				accRow[chain.Start.Var] = start
			}

			current := []multiHopPartialPath{{
				Nodes:     []*graph.Vertex{start},
				AccRow:    accRow,
				UsedEdges: make(map[string]bool),
			}}

			var hopErr error
			for _, hop := range chain.Hops {
				var next []multiHopPartialPath
				for _, partial := range current {
					last := partial.Nodes[len(partial.Nodes)-1]

					type edgeNeighbor struct {
						edge      *graph.Edge
						neighbor  *graph.Vertex
						direction string
					}
					var candidates []edgeNeighbor
					seenCandidate := map[string]struct{}{}

					collectFn := func(edge *graph.Edge, neighborID, dir string) error {
						if edge == nil {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, seen := seenCandidate[key]; seen {
							return nil
						}
						neighbor, nerr := tx.GetVertex(ctx, tenant, neighborID)
						if nerr != nil {
							if graph.IsKind(nerr, graph.ErrKindNotFound) {
								return nil
							}
							return nerr
						}
						seenCandidate[key] = struct{}{}
						candidates = append(candidates, edgeNeighbor{edge, neighbor, dir})
						return nil
					}

					if hop.Direction == "forward" || hop.Direction == "undirected" {
						if scanErr := tx.ScanOutEdges(ctx, tenant, last.ID, "", 0, func(edge *graph.Edge) error {
							return collectFn(edge, edge.DstID, "forward")
						}); scanErr != nil {
							hopErr = scanErr
							break
						}
					}
					if hop.Direction == "reverse" || hop.Direction == "undirected" {
						if scanErr := tx.ScanInEdges(ctx, tenant, last.ID, "", 0, func(edge *graph.Edge) error {
							return collectFn(edge, edge.SrcID, "reverse")
						}); scanErr != nil {
							hopErr = scanErr
							break
						}
					}
					if hopErr != nil {
						break
					}

					for _, c := range candidates {
						// Cypher path-uniqueness: each edge may only appear once per path.
						if partial.UsedEdges[c.edge.ID] {
							continue
						}
						if !nodePatternMatches(c.neighbor, hop.Node, params, partial.AccRow) {
							continue
						}

						newNodes := make([]*graph.Vertex, len(partial.Nodes)+1)
						copy(newNodes, partial.Nodes)
						newNodes[len(partial.Nodes)] = c.neighbor

						newEdges := make([]*graph.Edge, len(partial.Edges)+1)
						copy(newEdges, partial.Edges)
						newEdges[len(partial.Edges)] = c.edge

						newDirs := make([]string, len(partial.Directions)+1)
						copy(newDirs, partial.Directions)
						newDirs[len(partial.Directions)] = c.direction

						newAccRow := cloneRow(partial.AccRow)
						if hop.Node.Var != "" {
							newAccRow[hop.Node.Var] = c.neighbor
						}

						newUsed := make(map[string]bool, len(partial.UsedEdges)+1)
						for k := range partial.UsedEdges {
							newUsed[k] = true
						}
						newUsed[c.edge.ID] = true

						next = append(next, multiHopPartialPath{
							Nodes:      newNodes,
							Edges:      newEdges,
							Directions: newDirs,
							AccRow:     newAccRow,
							UsedEdges:  newUsed,
						})
					}
				}
				if hopErr != nil {
					break
				}
				current = next
			}

			if hopErr != nil {
				return nil, hopErr
			}

			for _, path := range current {
				merged := cloneRow(path.AccRow)
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(path.Nodes, path.Edges, path.Directions)
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}

				matched = true
				out = append(out, merged)
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if chain.Start.Var != "" {
				setOptionalNoMatchBinding(merged, row, chain.Start.Var)
			}
			for _, hop := range chain.Hops {
				if hop.Node.Var != "" {
					setOptionalNoMatchBinding(merged, row, hop.Node.Var)
				}
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func vertexBindingMatches(row Row, varName string, candidate *graph.Vertex) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	switch typed := binding.(type) {
	case nil:
		return candidate == nil
	case *graph.Vertex:
		return candidate != nil && typed.ID == candidate.ID
	case string:
		return candidate != nil && typed == candidate.ID
	default:
		return false
	}
}

func edgeBindingMatches(row Row, varName string, candidate *graph.Edge) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	switch typed := binding.(type) {
	case nil:
		return candidate == nil
	case *graph.Edge:
		return candidate != nil && typed.ID == candidate.ID
	case string:
		return candidate != nil && typed == candidate.ID
	default:
		return false
	}
}

func edgePatternMatches(edge *graph.Edge, propsRaw string, params Params, row Row) bool {
	if edge == nil {
		return false
	}
	propsRaw = strings.TrimSpace(propsRaw)
	if propsRaw == "" {
		return true
	}
	parsed, err := parsePropertyMap(propsRaw, params, row)
	if err != nil {
		return false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			if edge.ID != stringFromProperty(map[string]any{"id": value}, "id") {
				return false
			}
			continue
		}
		if strings.EqualFold(key, "type") {
			if edge.Type != stringFromProperty(map[string]any{"type": value}, "type") {
				return false
			}
			continue
		}
		if edge.Properties == nil {
			return false
		}
		current, ok := edge.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(current, valueToBytes(value)) {
			return false
		}
	}
	return true
}

func edgeTypeMatches(edge *graph.Edge, edgeType string, edgeAnyOf []string) bool {
	if edge == nil {
		return false
	}
	if len(edgeAnyOf) == 0 {
		if edgeType == "" {
			return true
		}
		return edge.Type == edgeType
	}
	for _, candidate := range edgeAnyOf {
		if edge.Type == candidate {
			return true
		}
	}
	return false
}

func (e *Executor) resolveNodePatternCandidates(ctx context.Context, tx graph.Tx, tenant string, row Row, pattern nodePattern, params Params) ([]*graph.Vertex, error) {
	if binding, ok := row[pattern.Var]; ok {
		switch typed := binding.(type) {
		case *graph.Vertex:
			if nodePatternMatches(typed, pattern, params, row) {
				return []*graph.Vertex{typed}, nil
			}
			return nil, nil
		case string:
			vertex, err := tx.GetVertex(ctx, tenant, typed)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					return nil, nil
				}
				return nil, err
			}
			if nodePatternMatches(vertex, pattern, params, row) {
				return []*graph.Vertex{vertex}, nil
			}
			return nil, nil
		}
	}

	out := make([]*graph.Vertex, 0)
	if err := tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if nodePatternMatches(vertex, pattern, params, row) {
			out = append(out, vertex)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func nodePatternMatches(vertex *graph.Vertex, pattern nodePattern, params Params, row Row) bool {
	if vertex == nil {
		return false
	}
	if !vertexBindingMatches(row, pattern.Var, vertex) {
		return false
	}
	if len(pattern.AnyOfLabels) > 0 {
		matched := false
		for _, want := range pattern.AnyOfLabels {
			if vertexHasLabel(vertex, want) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(pattern.AllOfLabels) > 0 {
		for _, want := range pattern.AllOfLabels {
			if !vertexHasLabel(vertex, want) {
				return false
			}
		}
	}
	if len(pattern.ExcludedLabels) > 0 {
		for _, want := range pattern.ExcludedLabels {
			if vertexHasLabel(vertex, want) {
				return false
			}
		}
	}
	if len(pattern.AllOfLabels) > 0 && len(pattern.AnyOfLabels) == 0 && len(pattern.ExcludedLabels) == 0 {
		if pattern.Var == "" {
			vertex.Labels = reorderLabelsByPattern(vertex.Labels, pattern.AllOfLabels)
		} else if _, alreadyBound := row[pattern.Var]; !alreadyBound {
			vertex.Labels = reorderLabelsByPattern(vertex.Labels, pattern.AllOfLabels)
		}
	}

	props := strings.TrimSpace(pattern.PropertiesRaw)
	if props == "" {
		return true
	}

	parsed, err := parsePropertyMap(props, params, row)
	if err != nil {
		return false
	}
	for key, value := range parsed {
		if vertex.Properties == nil {
			return false
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

func vertexHasLabel(vertex *graph.Vertex, label string) bool {
	if vertex == nil || strings.TrimSpace(label) == "" {
		return false
	}
	for _, current := range vertex.Labels {
		if current == label {
			return true
		}
	}
	return false
}

func reorderLabelsByPattern(labels []string, ordered []string) []string {
	if len(labels) == 0 || len(ordered) == 0 {
		return labels
	}
	seen := make(map[string]struct{}, len(labels))
	out := make([]string, 0, len(labels))
	for _, want := range ordered {
		for _, label := range labels {
			if label != want {
				continue
			}
			if _, ok := seen[label]; ok {
				break
			}
			seen[label] = struct{}{}
			out = append(out, label)
			break
		}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			continue
		}
		out = append(out, label)
	}
	return out
}

func anchoredSourcePropertyEquality(pattern anchoredOutPattern, params Params, row Row) (string, any, bool) {
	props := strings.TrimSpace(pattern.SourcePropertiesRaw)
	if props == "" {
		return "", nil, false
	}
	parsed, err := parsePropertyMap(props, params, row)
	if err != nil || len(parsed) != 1 {
		return "", nil, false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			return "", nil, false
		}
		return key, value, true
	}
	return "", nil, false
}

func vertexMatchesProperty(vertex *graph.Vertex, prop string, encoded []byte, label string) bool {
	if vertex == nil {
		return false
	}
	if label != "" {
		matched := false
		for _, current := range vertex.Labels {
			if current == label {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if vertex.Properties == nil {
		return false
	}
	value, ok := vertex.Properties[prop]
	if !ok {
		return false
	}
	return bytes.Equal(value, encoded)
}

func (e *Executor) applyCreateClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params, merge bool) ([]Row, error) {
	rawClause := stripLeadingClauseKeyword(clause.Raw, string(clause.Kind))
	raw := normalizeClauseBody(stripCypherLineComments(rawClause))
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []Row{{}}
	}
	parts := splitTopLevelCommaSeparated(raw)
	if merge {
		parts = []string{raw}
	}
	out := rows
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		patternRaw := part
		onCreateSet := ""
		onMatchSet := ""
		if merge {
			patternRaw, onCreateSet, onMatchSet = splitMergePatternAndActions(part)
			if patternRaw == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", part), nil)
			}
		}

		out, err = e.applyCreatePattern(ctx, tx, out, patternRaw, params, tenant, merge)
		if err != nil {
			return nil, err
		}
		if merge {
			out, err = e.applyMergeActions(ctx, tx, out, onCreateSet, onMatchSet, params)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(out) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
	}
	out = clearMergeCreatedMarker(out)
	return out, nil
}

func splitMergePatternAndActions(raw string) (pattern string, onCreateSet string, onMatchSet string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", ""
	}

	createIdx := findTopLevelKeywordIndex(raw, "ONCREATESET")
	matchIdx := findTopLevelKeywordIndex(raw, "ONMATCHSET")
	firstIdx := minPositiveIndex(createIdx, matchIdx)
	if firstIdx < 0 {
		return raw, "", ""
	}

	pattern = strings.TrimSpace(raw[:firstIdx])
	if createIdx >= 0 {
		end := len(raw)
		if matchIdx > createIdx {
			end = matchIdx
		}
		onCreateSet = strings.TrimSpace(raw[createIdx+len("ONCREATESET") : end])
	}
	if matchIdx >= 0 {
		end := len(raw)
		if createIdx > matchIdx {
			end = createIdx
		}
		onMatchSet = strings.TrimSpace(raw[matchIdx+len("ONMATCHSET") : end])
	}
	return pattern, onCreateSet, onMatchSet
}

func (e *Executor) applyMergeActions(ctx context.Context, tx graph.Tx, rows []Row, onCreateSet string, onMatchSet string, params Params) ([]Row, error) {
	onCreateSet = strings.TrimSpace(onCreateSet)
	onMatchSet = strings.TrimSpace(onMatchSet)
	if onCreateSet == "" && onMatchSet == "" {
		return rows, nil
	}
	if err := validateMergeActionAssignmentTargets(rows, onCreateSet, onMatchSet); err != nil {
		return nil, err
	}

	updated := make([]Row, 0, len(rows))
	for _, row := range rows {
		created := false
		if marker, ok := row[mergeCreatedMarkerKey]; ok {
			if flagged, ok := marker.(bool); ok {
				created = flagged
			}
		}

		current := cloneRow(row)
		if created && onCreateSet != "" {
			setClause := ast.Clause{Kind: ast.ClauseKindSet, Raw: "SET" + onCreateSet}
			nextRows, err := e.applySetClause(ctx, tx, []Row{current}, setClause, params)
			if err != nil {
				return nil, err
			}
			if len(nextRows) > 0 {
				current = nextRows[0]
			}
		}
		if !created && onMatchSet != "" {
			setClause := ast.Clause{Kind: ast.ClauseKindSet, Raw: "SET" + onMatchSet}
			nextRows, err := e.applySetClause(ctx, tx, []Row{current}, setClause, params)
			if err != nil {
				return nil, err
			}
			if len(nextRows) > 0 {
				current = nextRows[0]
			}
		}
		updated = append(updated, current)
	}
	return updated, nil
}

func validateMergeActionAssignmentTargets(rows []Row, onCreateSet string, onMatchSet string) error {
	if len(rows) == 0 {
		return nil
	}

	bound := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			bound[key] = struct{}{}
		}
	}

	validateOne := func(raw string) error {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil
		}
		for _, item := range splitTopLevelCommaSeparated(raw) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			varName := ""
			if m := setClauseRE.FindStringSubmatch(item); len(m) == 4 {
				varName = m[1]
			} else if m := setMapAppendClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			} else if m := setMapReplaceClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			} else if m := setLabelClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			}
			if varName == "" {
				continue
			}
			if _, ok := bound[varName]; !ok {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "UndefinedVariable"}
			}
		}
		return nil
	}

	if err := validateOne(onCreateSet); err != nil {
		return err
	}
	if err := validateOne(onMatchSet); err != nil {
		return err
	}
	return nil
}

func clearMergeCreatedMarker(rows []Row) []Row {
	clean := make([]Row, 0, len(rows))
	for _, row := range rows {
		next := cloneRow(row)
		delete(next, mergeCreatedMarkerKey)
		clean = append(clean, next)
	}
	return clean
}

func (e *Executor) applyCreatePattern(ctx context.Context, tx graph.Tx, rows []Row, raw string, params Params, tenant string, merge bool) ([]Row, error) {
	if pathVar, innerPattern, ok := parseBoundPathPattern(raw); ok {
		createdRows, err := e.applyCreatePattern(ctx, tx, rows, innerPattern, params, tenant, merge)
		if err != nil {
			return nil, err
		}
		if err := bindCreatePatternPathValues(ctx, tx, createdRows, pathVar, innerPattern); err != nil {
			return nil, err
		}
		return createdRows, nil
	}

	if isMissingRelationshipTypePattern(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	if createVariableLengthRelRE.MatchString(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "CreatingVarLength"}
	}
	if createEdgePatternTwoDirectionsRE.MatchString(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RequiresDirectedRelationship"}
	}
	if m := createEdgePatternRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionForward)
	}
	if m := createEdgePatternReverseRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionReverse)
	}
	if m := createEdgePatternUndirectedRE.FindStringSubmatch(raw); len(m) == 10 {
		if !merge {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RequiresDirectedRelationship"}
		}
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionUndirected)
	}
	if chain, ok := parseCreateChainPattern(raw); ok {
		return e.applyCreateChainPattern(ctx, tx, rows, chain, params, tenant, merge)
	}
	if m := createVertexPatternRE.FindStringSubmatch(raw); len(m) == 4 {
		return e.applyCreateVertex(ctx, tx, rows, m, params, tenant, merge)
	}
	if spec, ok := parseCreateVertexPatternSpec(raw); ok {
		return e.applyCreateVertexSpec(ctx, tx, rows, spec, params, tenant, merge)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
}

func bindCreatePatternPathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar, patternRaw string) error {
	if pathVar == "" {
		return nil
	}

	if spec, ok := parseCreateVertexPatternSpec(patternRaw); ok {
		if spec.Var == "" {
			for _, row := range rows {
				row[pathVar] = nil
			}
			return nil
		}
		for _, row := range rows {
			row[pathVar] = cypherPathValue{Left: vertexFromRowBinding(row, spec.Var)}
		}
		return nil
	}

	if m := createEdgePatternRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionForward)
	}
	if m := createEdgePatternReverseRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionReverse)
	}
	if m := createEdgePatternUndirectedRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionUndirected)
	}

	if chain, ok := parseCreateChainPattern(patternRaw); ok {
		return bindCreateChainPathValues(ctx, tx, rows, pathVar, chain)
	}

	for _, row := range rows {
		row[pathVar] = nil
	}
	return nil
}

func bindCreateSingleEdgePathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar string, m []string, direction createEdgeDirection) error {
	leftVar := strings.TrimSpace(m[1])
	edgeVar := strings.TrimSpace(m[4])
	rightVar := strings.TrimSpace(m[7])
	edgeType, err := normalizeCreateRelationshipType(m[5])
	if err != nil {
		return err
	}

	for _, row := range rows {
		left := vertexFromRowBinding(row, leftVar)
		right := vertexFromRowBinding(row, rightVar)
		edge := edgeFromRowBinding(row, edgeVar)
		if edge == nil {
			edge, err = lookupBoundPathEdge(ctx, tx, left, right, edgeType, direction)
			if err != nil {
				return err
			}
		}
		row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: createEdgeDirectionName(direction)}
	}
	return nil
}

func bindCreateChainPathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar string, pattern createChainPattern) error {
	for _, row := range rows {
		nodes := make([]*graph.Vertex, 0, len(pattern.Nodes))
		edges := make([]*graph.Edge, 0, len(pattern.Rels))
		dirs := make([]string, 0, len(pattern.Rels))
		for _, node := range pattern.Nodes {
			nodes = append(nodes, vertexFromRowBinding(row, node.Var))
		}
		for i, rel := range pattern.Rels {
			edge := edgeFromRowBinding(row, rel.Var)
			if edge == nil && i+1 < len(nodes) {
				edgeType, err := normalizeCreateRelationshipType(rel.Type)
				if err != nil {
					return err
				}
				edge, err = lookupBoundPathEdge(ctx, tx, nodes[i], nodes[i+1], edgeType, rel.Direction)
				if err != nil {
					return err
				}
			}
			edges = append(edges, edge)
			dirs = append(dirs, createEdgeDirectionName(rel.Direction))
		}
		if len(edges) <= 1 {
			var edge *graph.Edge
			if len(edges) == 1 {
				edge = edges[0]
			}
			left := (*graph.Vertex)(nil)
			right := (*graph.Vertex)(nil)
			if len(nodes) > 0 {
				left = nodes[0]
			}
			if len(nodes) > 1 {
				right = nodes[1]
			}
			row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: firstOrDefault(dirs, "forward")}
			continue
		}
		row[pathVar] = multiHopPathValue(nodes, edges, dirs)
	}
	return nil
}

func lookupBoundPathEdge(ctx context.Context, tx graph.Tx, left, right *graph.Vertex, edgeType string, direction createEdgeDirection) (*graph.Edge, error) {
	if left == nil || right == nil || strings.TrimSpace(edgeType) == "" {
		return nil, nil
	}

	matches, err := findMergeEdges(ctx, tx, left, right, edgeType, map[string]any{}, direction)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil
	}
	return matches[0], nil
}

func createEdgeDirectionName(direction createEdgeDirection) string {
	switch direction {
	case createEdgeDirectionReverse:
		return "reverse"
	case createEdgeDirectionUndirected:
		return "undirected"
	default:
		return "forward"
	}
}

func firstOrDefault(values []string, fallback string) string {
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return fallback
	}
	return values[0]
}

func parseCreateChainPattern(raw string) (createChainPattern, bool) {
	remaining := strings.TrimSpace(raw)
	if remaining == "" {
		return createChainPattern{}, false
	}

	nodeMatch := createChainNodeTokenRE.FindStringSubmatch(remaining)
	if len(nodeMatch) != 4 {
		return createChainPattern{}, false
	}
	pattern := createChainPattern{
		Nodes: []createChainNodePattern{{Var: nodeMatch[1], Labels: splitLabels(nodeMatch[2]), PropsRaw: nodeMatch[3]}},
		Rels:  make([]createChainRelPattern, 0),
	}
	remaining = strings.TrimSpace(remaining[len(nodeMatch[0]):])

	for remaining != "" {
		rel, consumed, ok := parseCreateChainRelToken(remaining)
		if !ok {
			return createChainPattern{}, false
		}
		pattern.Rels = append(pattern.Rels, rel)
		remaining = strings.TrimSpace(remaining[consumed:])

		nextNode := createChainNodeTokenRE.FindStringSubmatch(remaining)
		if len(nextNode) != 4 {
			return createChainPattern{}, false
		}
		pattern.Nodes = append(pattern.Nodes, createChainNodePattern{Var: nextNode[1], Labels: splitLabels(nextNode[2]), PropsRaw: nextNode[3]})
		remaining = strings.TrimSpace(remaining[len(nextNode[0]):])
	}

	if len(pattern.Rels) == 0 || len(pattern.Nodes) != len(pattern.Rels)+1 {
		return createChainPattern{}, false
	}
	return pattern, true
}

func parseCreateChainRelToken(raw string) (createChainRelPattern, int, bool) {
	if m := createChainRelForwardTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionForward}, len(m[0]), true
	}
	if m := createChainRelReverseTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionReverse}, len(m[0]), true
	}
	if m := createChainRelUndirTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionUndirected}, len(m[0]), true
	}
	return createChainRelPattern{}, 0, false
}

func (e *Executor) applyCreateVertex(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool) ([]Row, error) {
	return e.applyCreateVertexSpec(ctx, tx, rows, createVertexPatternSpec{Var: m[1], Labels: splitLabels(m[2]), PropsRaw: m[3]}, params, tenant, merge)
}

func (e *Executor) applyCreateVertexSpec(ctx context.Context, tx graph.Tx, rows []Row, spec createVertexPatternSpec, params Params, tenant string, merge bool) ([]Row, error) {
	varName := spec.Var
	labels := spec.Labels

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		props, err := parsePropertyMap(spec.PropsRaw, params, row)
		if err != nil {
			return nil, err
		}
		if merge && hasNilPropertyValue(props) {
			return nil, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
		}
		normalizedProps := normalizeVertexProperties(props)
		vertexID := ""
		if vertexID == "" {
			if varName != "" {
				if existing, ok := row[varName].(*graph.Vertex); ok {
					vertexID = existing.ID
				}
			}
		}

		if merge && vertexID == "" {
			matches, err := findMergeVerticesByPattern(ctx, tx, tenant, labels, props)
			if err != nil {
				return nil, err
			}
			if len(matches) == 0 {
				vertex := &graph.Vertex{Tenant: tenant, ID: nextAutoVertexID(varName), Labels: labels, Properties: normalizedProps}
				if err := tx.PutVertex(ctx, vertex); err != nil {
					return nil, err
				}
				merged := cloneRow(row)
				merged[mergeCreatedMarkerKey] = true
				if varName != "" {
					merged[varName] = vertex
				}
				out = append(out, merged)
				continue
			}
			for _, match := range matches {
				merged := cloneRow(row)
				merged[mergeCreatedMarkerKey] = false
				if varName != "" {
					merged[varName] = match
				}
				out = append(out, merged)
			}
			continue
		}

		if vertexID == "" {
			vertexID = nextAutoVertexID(varName)
		}

		vertex := &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizedProps}
		created := true
		if merge {
			if existing, err := tx.GetVertex(ctx, vertex.Tenant, vertex.ID); err == nil {
				vertex = existing
				created = false
			}
		}
		if err := tx.PutVertex(ctx, vertex); err != nil {
			return nil, err
		}
		merged := cloneRow(row)
		if merge {
			merged[mergeCreatedMarkerKey] = created
		}
		if varName != "" {
			merged[varName] = vertex
		}
		out = append(out, merged)
	}
	return out, nil
}

func hasNilPropertyValue(props map[string]any) bool {
	for _, value := range props {
		if value == nil {
			return true
		}
	}
	return false
}

func findMergeVerticesByPattern(ctx context.Context, tx graph.Tx, tenant string, labels []string, props map[string]any) ([]*graph.Vertex, error) {
	matches := make([]*graph.Vertex, 0)
	err := tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if !vertexMatchesMergePattern(vertex, labels, props) {
			return nil
		}
		matches = append(matches, vertex)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func vertexMatchesMergePattern(vertex *graph.Vertex, labels []string, props map[string]any) bool {
	if vertex == nil {
		return false
	}
	if len(labels) > 0 {
		for _, want := range labels {
			found := false
			for _, current := range vertex.Labels {
				if current == want {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	for key, expected := range props {
		stored, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		actual := decodeStoredPropertyValue(stored)
		if !mergePropertyValueEqual(expected, actual) {
			return false
		}
	}
	return true
}

func mergePropertyValueEqual(expected, actual any) bool {
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	switch exp := expected.(type) {
	case int:
		switch typed := actual.(type) {
		case int64:
			return int64(exp) == typed
		case json.Number:
			if i, err := typed.Int64(); err == nil {
				return int64(exp) == i
			}
		}
	case int64:
		switch typed := actual.(type) {
		case int:
			return exp == int64(typed)
		case json.Number:
			if i, err := typed.Int64(); err == nil {
				return exp == i
			}
		}
	case float64:
		if num, ok := actual.(json.Number); ok {
			if f, err := num.Float64(); err == nil {
				return exp == f
			}
		}
	case json.Number:
		switch typed := actual.(type) {
		case int:
			if i, err := exp.Int64(); err == nil {
				return i == int64(typed)
			}
		case int64:
			if i, err := exp.Int64(); err == nil {
				return i == typed
			}
		case float64:
			if f, err := exp.Float64(); err == nil {
				return f == typed
			}
		}
	}

	return fmt.Sprint(expected) == fmt.Sprint(actual)
}

func parseCreateVertexPatternSpec(raw string) (createVertexPatternSpec, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "(") || !strings.HasSuffix(raw, ")") {
		return createVertexPatternSpec{}, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return createVertexPatternSpec{}, true
	}

	head := body
	propsRaw := ""
	if idx := strings.Index(body, "{"); idx >= 0 {
		end, ok := findMatchingDelimiter(body, idx, '{', '}')
		if !ok {
			return createVertexPatternSpec{}, false
		}
		if strings.TrimSpace(body[end+1:]) != "" {
			return createVertexPatternSpec{}, false
		}
		head = strings.TrimSpace(body[:idx])
		propsRaw = strings.TrimSpace(body[idx+1 : end])
	}

	varName := ""
	labelsRaw := ""
	if strings.HasPrefix(head, ":") {
		labelsRaw = strings.TrimPrefix(head, ":")
	} else if strings.Contains(head, ":") {
		parts := strings.SplitN(head, ":", 2)
		varName = strings.TrimSpace(parts[0])
		labelsRaw = strings.TrimSpace(parts[1])
	} else {
		varName = strings.TrimSpace(head)
	}

	if varName != "" && !identifierRE.MatchString(varName) {
		return createVertexPatternSpec{}, false
	}
	return createVertexPatternSpec{Var: varName, Labels: splitLabels(labelsRaw), PropsRaw: propsRaw}, true
}

func findMatchingDelimiter(raw string, start int, open byte, close byte) (int, bool) {
	if start < 0 || start >= len(raw) || raw[start] != open {
		return -1, false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		}
		if inSingle || inDouble {
			continue
		}
		if ch == open {
			depth++
			continue
		}
		if ch == close {
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return -1, false
}

func (e *Executor) applyCreateEdge(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool, direction createEdgeDirection) ([]Row, error) {
	leftVar := m[1]
	leftLabels := splitLabels(m[2])
	edgeVar := m[4]
	edgeType, err := normalizeCreateRelationshipType(m[5])
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(edgeType) == "" {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	rightVar := m[7]
	rightLabels := splitLabels(m[8])
	undirectedSeenEdges := map[string]struct{}{}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		workRow := cloneRow(row)

		leftProps, err := parsePropertyMap(m[3], params, workRow)
		if err != nil {
			return nil, err
		}
		leftVertex, leftCreated, err := resolveOrCreateVertex(ctx, tx, tenant, workRow, leftVar, leftLabels, leftProps, merge)
		if err != nil {
			return nil, err
		}
		if leftVar != "" {
			workRow[leftVar] = leftVertex
		}

		edgeProps, err := parsePropertyMap(m[6], params, workRow)
		if err != nil {
			return nil, err
		}
		rightProps, err := parsePropertyMap(m[9], params, workRow)
		if err != nil {
			return nil, err
		}
		rightVertex, rightCreated, err := resolveOrCreateVertex(ctx, tx, tenant, workRow, rightVar, rightLabels, rightProps, merge)
		if err != nil {
			return nil, err
		}
		if rightVar != "" {
			workRow[rightVar] = rightVertex
		}

		if merge {
			matchedEdges, err := findMergeEdges(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, direction)
			if err != nil {
				return nil, err
			}
			if len(matchedEdges) > 0 {
				seenEdges := map[string]struct{}{}
				for _, edge := range matchedEdges {
					if edge == nil {
						continue
					}
					if _, seen := seenEdges[edge.ID]; seen {
						continue
					}
					if direction == createEdgeDirectionUndirected {
						if _, seen := undirectedSeenEdges[edge.ID]; seen {
							continue
						}
						undirectedSeenEdges[edge.ID] = struct{}{}
					}
					seenEdges[edge.ID] = struct{}{}
					merged := cloneRow(workRow)
					merged[mergeCreatedMarkerKey] = leftCreated || rightCreated
					if leftVar != "" {
						merged[leftVar] = leftVertex
					}
					if rightVar != "" {
						merged[rightVar] = rightVertex
					}
					if edgeVar != "" {
						merged[edgeVar] = edge
					}
					out = append(out, merged)
				}
				continue
			}
		}

		edge, edgeCreated, err := createOrMergeEdge(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, merge, direction)
		if err != nil {
			return nil, err
		}

		merged := cloneRow(workRow)
		if merge {
			merged[mergeCreatedMarkerKey] = leftCreated || rightCreated || edgeCreated
		}
		if leftVar != "" {
			merged[leftVar] = leftVertex
		}
		if rightVar != "" {
			merged[rightVar] = rightVertex
		}
		if edgeVar != "" {
			merged[edgeVar] = edge
		}
		out = append(out, merged)
	}
	return out, nil
}

func (e *Executor) applyCreateChainPattern(ctx context.Context, tx graph.Tx, rows []Row, pattern createChainPattern, params Params, tenant string, merge bool) ([]Row, error) {
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		merged := cloneRow(row)
		vertices := make([]*graph.Vertex, len(pattern.Nodes))
		createdAny := false

		for i, node := range pattern.Nodes {
			props, err := parsePropertyMap(node.PropsRaw, params, merged)
			if err != nil {
				return nil, err
			}
			vertex, vertexCreated, err := resolveOrCreateVertex(ctx, tx, tenant, merged, node.Var, node.Labels, props, merge)
			if err != nil {
				return nil, err
			}
			if merge && vertexCreated {
				createdAny = true
			}
			vertices[i] = vertex
			if node.Var != "" {
				merged[node.Var] = vertex
			}
		}

		for i, rel := range pattern.Rels {
			relType, err := normalizeCreateRelationshipType(rel.Type)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(relType) == "" {
				return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
			}
			relProps, err := parsePropertyMap(rel.PropsRaw, params, merged)
			if err != nil {
				return nil, err
			}
			edge, edgeCreated, err := createOrMergeEdge(ctx, tx, vertices[i], vertices[i+1], relType, relProps, merge, rel.Direction)
			if err != nil {
				return nil, err
			}
			if merge && edgeCreated {
				createdAny = true
			}
			if rel.Var != "" {
				merged[rel.Var] = edge
			}
		}
		if merge {
			merged[mergeCreatedMarkerKey] = createdAny
		}

		out = append(out, merged)
	}
	return out, nil
}

func createOrMergeEdge(ctx context.Context, tx graph.Tx, leftVertex, rightVertex *graph.Vertex, edgeType string, edgeProps map[string]any, merge bool, direction createEdgeDirection) (*graph.Edge, bool, error) {
	if merge && hasNilPropertyValue(edgeProps) {
		return nil, false, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}

	srcVertex := leftVertex
	dstVertex := rightVertex
	switch direction {
	case createEdgeDirectionReverse:
		srcVertex = rightVertex
		dstVertex = leftVertex
	case createEdgeDirectionForward, createEdgeDirectionUndirected:
		// Keep CREATE default direction left-to-right.
	default:
		return nil, false, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown edge direction %q", direction), nil)
	}

	if merge {
		matches, err := findMergeEdges(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, direction)
		if err != nil {
			return nil, false, err
		}
		if len(matches) > 0 {
			return matches[0], false, nil
		}
	}

	edge := &graph.Edge{
		Tenant:     srcVertex.Tenant,
		ID:         nextAutoEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID),
		Type:       edgeType,
		SrcID:      srcVertex.ID,
		DstID:      dstVertex.ID,
		Properties: normalizeEdgeProperties(edgeProps),
	}
	if err := tx.PutEdge(ctx, edge); err != nil {
		return nil, false, err
	}
	return edge, true, nil
}

func findMergeEdges(ctx context.Context, tx graph.Tx, leftVertex, rightVertex *graph.Vertex, edgeType string, edgeProps map[string]any, direction createEdgeDirection) ([]*graph.Edge, error) {
	if leftVertex == nil || rightVertex == nil {
		return nil, nil
	}

	appendMatches := func(out []*graph.Edge, seen map[string]struct{}, srcID, dstID string) ([]*graph.Edge, error) {
		err := tx.ScanOutEdges(ctx, leftVertex.Tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
			if edge.DstID != dstID {
				return nil
			}
			if !edgeMatchesMergePattern(edge, edgeProps) {
				return nil
			}
			if _, ok := seen[edge.ID]; ok {
				return nil
			}
			seen[edge.ID] = struct{}{}
			out = append(out, edge)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	matches := make([]*graph.Edge, 0)
	seen := map[string]struct{}{}
	switch direction {
	case createEdgeDirectionForward:
		return appendMatches(matches, seen, leftVertex.ID, rightVertex.ID)
	case createEdgeDirectionReverse:
		return appendMatches(matches, seen, rightVertex.ID, leftVertex.ID)
	case createEdgeDirectionUndirected:
		out, err := appendMatches(matches, seen, leftVertex.ID, rightVertex.ID)
		if err != nil {
			return nil, err
		}
		return appendMatches(out, seen, rightVertex.ID, leftVertex.ID)
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown edge direction %q", direction), nil)
	}
}

func edgeMatchesMergePattern(edge *graph.Edge, props map[string]any) bool {
	if edge == nil {
		return false
	}
	for key, expected := range props {
		if strings.EqualFold(key, "id") {
			if edge.ID != stringFromProperty(map[string]any{"id": expected}, "id") {
				return false
			}
			continue
		}
		if strings.EqualFold(key, "type") {
			if edge.Type != stringFromProperty(map[string]any{"type": expected}, "type") {
				return false
			}
			continue
		}
		if edge.Properties == nil {
			return false
		}
		stored, ok := edge.Properties[key]
		if !ok {
			return false
		}
		actual := decodeStoredPropertyValue(stored)
		if !mergePropertyValueEqual(expected, actual) {
			return false
		}
	}
	return true
}

func normalizeCreateRelationshipType(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	edgeType, edgeAnyOf, err := parseEdgeTypeFilter(strings.ReplaceAll(raw, ":", ""))
	if err != nil {
		return "", err
	}
	if len(edgeAnyOf) > 0 {
		return "", &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	return edgeType, nil
}

func isMissingRelationshipTypePattern(raw string) bool {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "[") || strings.Contains(raw, "]") {
		return false
	}
	if createMissingRelTypeForwardRE.MatchString(raw) {
		return true
	}
	if createMissingRelTypeReverseRE.MatchString(raw) {
		return true
	}
	if createMissingRelTypeUndirRE.MatchString(raw) {
		return true
	}
	return false
}

func (e *Executor) applySetClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	raw = stripNormalizedPrefix(raw, "SET")
	items := splitTopLevelCommaSeparated(raw)
	if len(items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		working := row
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			if varName, field, exprRaw, ok := parseSetPropertyAssignment(item); ok {
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				value, err := evalExpressionWithScope(exprRaw, working, params)
				if err != nil {
					value, err = evalWriteValue(exprRaw, params, working)
				}
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					if field == "id" {
						return nil, graph.NewError(graph.ErrKindUnsupported, "setting vertex id is not supported", nil)
					}
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					ensureProperties(mutated)
					if value == nil {
						delete(mutated.Properties, field)
					} else {
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[field] = encoded
					}
					if err := tx.PutVertex(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					if field == "id" {
						return nil, graph.NewError(graph.ErrKindUnsupported, "setting edge id is not supported", nil)
					}
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					ensurePropertiesEdge(mutated)
					if value == nil {
						delete(mutated.Properties, field)
					} else {
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[field] = encoded
					}
					if err := tx.PutEdge(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setMapAppendClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, exprRaw := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				mapValue, err := evalSetMapValue(exprRaw, working, params)
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					ensureProperties(mutated)
					for key, value := range mapValue {
						if value == nil {
							delete(mutated.Properties, key)
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := tx.PutVertex(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					ensurePropertiesEdge(mutated)
					for key, value := range mapValue {
						if value == nil {
							delete(mutated.Properties, key)
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := tx.PutEdge(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setMapReplaceClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, exprRaw := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				mapValue, err := evalSetMapValue(exprRaw, working, params)
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					mutated.Properties = map[string][]byte{}
					for key, value := range mapValue {
						if value == nil {
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := tx.PutVertex(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					mutated.Properties = map[string][]byte{}
					for key, value := range mapValue {
						if value == nil {
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := tx.PutEdge(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setLabelClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName := match[1]
				labels := splitLabels(match[2])
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				vertex, ok := binding.(*graph.Vertex)
				if !ok {
					if binding == nil {
						continue
					}
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				current, err := loadCurrentVertexForWrite(ctx, tx, vertex)
				if err != nil {
					return nil, err
				}
				mutated := cloneVertex(current)
				for _, label := range labels {
					if !vertexHasLabel(mutated, label) {
						mutated.Labels = append(mutated.Labels, label)
					}
				}
				if err := tx.PutVertex(ctx, mutated); err != nil {
					return nil, err
				}
				working = cloneRow(working)
				working[varName] = mutated
				continue
			}

			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET clause %q is not yet supported", raw), nil)
		}
		out = append(out, working)
	}
	return out, nil
}

func parseSetPropertyAssignment(item string) (string, string, string, bool) {
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

	base, fields, ok := splitTopLevelFieldAccess(lhs)
	if !ok || len(fields) != 1 {
		return "", "", "", false
	}
	base = strings.TrimSpace(base)
	if inner, wrapped := unwrapOuterParentheses(base); wrapped {
		base = strings.TrimSpace(inner)
	}
	if !isIdentifierLike(base) || !isIdentifierLike(fields[0]) {
		return "", "", "", false
	}

	return base, fields[0], rhs, true
}

func indexTopLevelEqualsInSetItem(raw string) int {
	depthParen, depthBracket, depthBrace := 0, 0, 0
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
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i
			}
		}
	}

	return -1
}

func loadCurrentVertexForWrite(ctx context.Context, tx graph.Tx, vertex *graph.Vertex) (*graph.Vertex, error) {
	if vertex == nil {
		return nil, nil
	}
	current, err := tx.GetVertex(ctx, vertex.Tenant, vertex.ID)
	if err == nil {
		return current, nil
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return vertex, nil
	}
	return nil, err
}

func loadCurrentEdgeForWrite(ctx context.Context, tx graph.Tx, edge *graph.Edge) (*graph.Edge, error) {
	if edge == nil {
		return nil, nil
	}
	current, err := tx.GetEdge(ctx, edge.Tenant, edge.ID)
	if err == nil {
		return current, nil
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return edge, nil
	}
	return nil, err
}

func evalSetMapValue(exprRaw string, row Row, params Params) (map[string]any, error) {
	value, err := evalExpressionWithScope(exprRaw, row, params)
	if err != nil {
		value, err = evalWriteValue(exprRaw, params, row)
	}
	if err != nil {
		return nil, err
	}

	switch typed := value.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		return typed, nil
	case *graph.Vertex:
		return decodePropertyMap(typed.Properties), nil
	case *graph.Edge:
		return decodePropertyMap(typed.Properties), nil
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
}

func decodePropertyMap(raw map[string][]byte) map[string]any {
	decoded := make(map[string]any, len(raw))
	for key, value := range raw {
		decoded[key] = decodeStoredPropertyValue(value)
	}
	return decoded
}

func (e *Executor) applyRemoveClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	raw = stripNormalizedPrefix(raw, "REMOVE")
	items := splitTopLevelCommaSeparated(raw)
	if len(items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		working := row
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			if match := removeClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, field := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					mutated := cloneVertex(typed)
					ensureProperties(mutated)
					delete(mutated.Properties, field)
					if err := tx.PutVertex(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					mutated := cloneEdge(typed)
					ensurePropertiesEdge(mutated)
					delete(mutated.Properties, field)
					if err := tx.PutEdge(ctx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE on %T is not supported", binding), nil)
				}
				continue
			}

			if match := removeLabelClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName := match[1]
				labelsToRemove := splitLabels(match[2])
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				vertex, ok := binding.(*graph.Vertex)
				if !ok {
					if binding == nil {
						continue
					}
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE on %T is not supported", binding), nil)
				}
				removeSet := map[string]struct{}{}
				for _, label := range labelsToRemove {
					removeSet[label] = struct{}{}
				}
				mutated := cloneVertex(vertex)
				kept := make([]string, 0, len(mutated.Labels))
				for _, label := range mutated.Labels {
					if _, remove := removeSet[label]; remove {
						continue
					}
					kept = append(kept, label)
				}
				mutated.Labels = kept
				if err := tx.PutVertex(ctx, mutated); err != nil {
					return nil, err
				}
				working = cloneRow(working)
				working[varName] = mutated
				continue
			}

			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE clause %q is not yet supported", raw), nil)
		}
		out = append(out, working)
	}
	return out, nil
}

func (e *Executor) applyDeleteClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	detach := false
	switch {
	case strings.HasPrefix(strings.ToUpper(raw), "DETACHDELETE"):
		detach = true
		raw = strings.TrimSpace(raw[len("DETACHDELETE"):])
	case strings.HasPrefix(strings.ToUpper(raw), "DELETE"):
		raw = strings.TrimSpace(raw[len("DELETE"):])
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE clause %q is not yet supported", raw), nil)
	}
	targets := splitTopLevelCommaSeparated(raw)
	if len(targets) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		working := row
		for _, target := range targets {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}

			value, err := evalExpressionWithScope(target, working, params)
			if err != nil {
				value, err = evalWriteValue(target, params, working)
			}
			if err != nil {
				return nil, err
			}

			replacement, err := e.deleteValue(ctx, tx, value, detach)
			if err != nil {
				return nil, err
			}

			if isIdentifierLike(target) {
				if _, bound := working[target]; bound {
					working = cloneRow(working)
					working[target] = replacement
				}
			}
		}
		out = append(out, working)
	}
	return out, nil
}

func stripNormalizedPrefix(raw, prefix string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToUpper(raw), prefix) {
		return strings.TrimSpace(raw[len(prefix):])
	}
	return raw
}

func (e *Executor) deleteValue(ctx context.Context, tx graph.Tx, value any, detach bool) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case *graph.Vertex:
		if detach {
			if err := deleteVertexWithEdges(ctx, tx, typed.Tenant, typed.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, err
			}
		} else {
			hasEdges, err := vertexHasAnyEdges(ctx, tx, typed.Tenant, typed.ID)
			if err != nil {
				return nil, err
			}
			if hasEdges {
				return nil, graph.NewError(graph.ErrKindConflict, "DeleteConnectedNode", nil)
			}
			if err := tx.DeleteVertex(ctx, typed.Tenant, typed.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, err
			}
		}
		return deletedVertexBinding{Tenant: typed.Tenant, ID: typed.ID, Labels: append([]string(nil), typed.Labels...)}, nil
	case *graph.Edge:
		if err := tx.DeleteEdge(ctx, typed.Tenant, typed.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
		return deletedEdgeBinding{Tenant: typed.Tenant, ID: typed.ID, Type: typed.Type}, nil
	case deletedVertexBinding, deletedEdgeBinding:
		return typed, nil
	case cypherPathValue:
		return e.deletePathValue(ctx, tx, typed, detach)
	case multiHopCypherPath:
		return e.deletePathValue(ctx, tx, typed, detach)
	case []any:
		for _, item := range typed {
			if _, err := e.deleteValue(ctx, tx, item, detach); err != nil {
				return nil, err
			}
		}
		return nil, nil
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				if _, err := e.deleteValue(ctx, tx, rv.Index(i).Interface(), detach); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE on %T is not supported", value), nil)
	}
}

func (e *Executor) deletePathValue(ctx context.Context, tx graph.Tx, value any, detach bool) (any, error) {
	deleteEdge := func(edge *graph.Edge) error {
		if edge == nil {
			return nil
		}
		if err := tx.DeleteEdge(ctx, edge.Tenant, edge.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return nil
	}

	deleteVertex := func(vertex *graph.Vertex) error {
		if vertex == nil {
			return nil
		}
		// Deleting a path should remove entities reachable by that path.
		if err := deleteVertexWithEdges(ctx, tx, vertex.Tenant, vertex.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return nil
	}

	switch typed := value.(type) {
	case cypherPathValue:
		if err := deleteEdge(typed.Edge); err != nil {
			return nil, err
		}
		if err := deleteVertex(typed.Left); err != nil {
			return nil, err
		}
		if err := deleteVertex(typed.Right); err != nil {
			return nil, err
		}
	case multiHopCypherPath:
		for _, edge := range typed.Edges {
			if err := deleteEdge(edge); err != nil {
				return nil, err
			}
		}
		for _, node := range typed.Nodes {
			if err := deleteVertex(node); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (e *Executor) applyUnwindClause(rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, "UNWIND"))
	parts := strings.SplitN(raw, "AS", 2)
	if len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("UNWIND clause %q is not yet supported", raw), nil)
	}

	exprRaw := strings.TrimSpace(parts[0])
	varName := strings.TrimSpace(parts[1])
	if varName == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "UNWIND target variable is required", nil)
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		values, err := evalUnwindValues(exprRaw, params, row)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged := cloneRow(row)
			merged[varName] = value
			out = append(out, merged)
		}
	}
	return out, nil
}

func (e *Executor) applyProjectionClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params, priorColumns []string, final bool) ([]Row, []string, error) {
	params = withProjectionEvalRuntime(ctx, tx, params, e)
	raw := strings.TrimSpace(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind)))
	projection, err := parseProjectionClauseSpec(raw)
	if err != nil {
		return nil, nil, err
	}
	items, err := parseProjectionItems(projection.ProjectionRaw)
	if err != nil {
		return nil, nil, err
	}
	if len(items) == 0 {
		return nil, nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("%s clause requires at least one projection item", clause.Kind), nil)
	}

	if err := validateProjectionOrderBy(items, projection.OrderBy, rows, projection.Distinct); err != nil {
		return nil, nil, err
	}
	projection.OrderBy = rewriteOrderByAggregateReferences(projection.OrderBy, items)

	filterProjectedRows := func(in []Row) ([]Row, error) {
		if projection.WhereRaw == "" {
			return in, nil
		}
		filtered := make([]Row, 0, len(in))
		for _, row := range in {
			ok, err := e.evalWhereExpression(ctx, tx, projection.WhereRaw, row, params)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		return filtered, nil
	}

	out := make([]Row, 0, len(rows))
	columns := make([]string, 0, len(items))
	hasAggregate := false
	hasStar := false
	for _, item := range items {
		if item.Expression == "*" {
			hasStar = true
			continue
		}
		if item.Alias != "" {
			columns = append(columns, item.Alias)
		} else {
			columns = append(columns, item.Expression)
		}
		if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" || len(extractAggregateCalls(item.Expression)) > 0 {
			hasAggregate = true
		}
	}

	if !hasAggregate {
		for _, row := range rows {
			projected := Row{}
			if len(projection.OrderBy) > 0 && !hasStar {
				for key, value := range row {
					projected[key] = value
				}
			}
			for _, item := range items {
				if item.Expression == "*" {
					for key, value := range row {
						projected[key] = value
					}
					continue
				}
				value, ok, err := e.evalProjectionPatternComprehension(ctx, tx, item.Expression, row, params)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					value, err = evalExpressionWithScope(item.Expression, row, params)
				}
				if err != nil {
					return nil, nil, err
				}
				key := item.Expression
				if item.Alias != "" {
					key = item.Alias
				}
				projected[key] = value
			}
			if projection.WhereRaw != "" {
				scope := cloneRow(row)
				for key, value := range projected {
					scope[key] = value
				}
				ok, err := e.evalWhereExpression(ctx, tx, projection.WhereRaw, scope, params)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					continue
				}
			}
			out = append(out, projected)
		}
		if hasStar {
			columns = inferProjectionColumns(out)
			if len(columns) == 0 && len(priorColumns) > 0 {
				columns = append([]string(nil), priorColumns...)
			}
		}
		if projection.WhereRaw == "" {
			out, err = filterProjectedRows(out)
			if err != nil {
				return nil, nil, err
			}
		}
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		if len(projection.OrderBy) > 0 && !hasStar {
			out = trimProjectionRows(out, columns)
		}
		return out, columns, nil
	}

	type projectionAggregate struct {
		funcName string
		count    int
		sum      float64
		intSum   int64
		intOnly  bool
		min      any
		max      any
		values   []float64
		pValue   *float64
		hasValue bool
	}

	type projectionGroup struct {
		projected          Row
		source             Row
		counts             map[int]int
		countSeen          map[int]map[string]struct{}
		collects           map[int][]any
		collectSeen        map[int]map[string]struct{}
		aggs               map[int]*projectionAggregate
		aggExprCounts      map[string]int
		aggExprCountSeen   map[string]map[string]struct{}
		aggExprCollects    map[string][]any
		aggExprCollectSeen map[string]map[string]struct{}
	}

	nonAggregateCount := 0
	for _, item := range items {
		if item.CountArg == "" && item.CollectArg == "" && item.AggFunc == "" && len(extractAggregateCalls(item.Expression)) == 0 {
			nonAggregateCount++
		}
	}

	groups := map[string]*projectionGroup{}
	groupOrder := make([]string, 0)
	for _, row := range rows {
		projected := Row{}
		keyValues := make([]any, 0, nonAggregateCount)
		for _, item := range items {
			if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" || len(extractAggregateCalls(item.Expression)) > 0 {
				continue
			}
			value, ok, err := e.evalProjectionPatternComprehension(ctx, tx, item.Expression, row, params)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				value, err = evalExpressionWithScope(item.Expression, row, params)
			}
			if err != nil {
				return nil, nil, err
			}
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			projected[key] = value
			keyValues = append(keyValues, normalizeResultValue(value))
		}

		keyBytes, err := json.Marshal(keyValues)
		if err != nil {
			return nil, nil, graph.NewError(graph.ErrKindUnsupported, "aggregation key is not serializable", err)
		}
		groupKey := string(keyBytes)
		group, ok := groups[groupKey]
		if !ok {
			group = &projectionGroup{projected: projected, source: cloneRow(row), counts: map[int]int{}, countSeen: map[int]map[string]struct{}{}, collects: map[int][]any{}, collectSeen: map[int]map[string]struct{}{}, aggs: map[int]*projectionAggregate{}, aggExprCounts: map[string]int{}, aggExprCountSeen: map[string]map[string]struct{}{}, aggExprCollects: map[string][]any{}, aggExprCollectSeen: map[string]map[string]struct{}{}}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}

		for idx, item := range items {
			calls := extractAggregateCalls(item.Expression)
			if len(calls) > 0 && item.CountArg == "" && item.CollectArg == "" && item.AggFunc == "" {
				seenCalls := map[string]struct{}{}
				for _, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					if _, seen := seenCalls[normalized]; seen {
						continue
					}
					seenCalls[normalized] = struct{}{}
					fn := aggregateFuncNameFromCall(call)
					switch fn {
					case "count":
						arg, ok := parseFunctionCall(call, "count")
						if !ok {
							continue
						}
						arg = strings.TrimSpace(arg)
						if arg == "*" {
							group.aggExprCounts[normalized]++
							continue
						}
						countExpr, countDistinct := parseCountDistinctArg(arg)
						if countExpr == "" {
							countExpr = arg
						}
						value, err := evalExpressionWithScope(countExpr, row, params)
						if err != nil {
							return nil, nil, err
						}
						if value == nil {
							continue
						}
						if countDistinct {
							if group.aggExprCountSeen[normalized] == nil {
								group.aggExprCountSeen[normalized] = map[string]struct{}{}
							}
							keyBytes, err := json.Marshal(normalizeResultValue(value))
							if err != nil {
								keyBytes = []byte(fmt.Sprintf("%v", value))
							}
							key := string(keyBytes)
							if _, ok := group.aggExprCountSeen[normalized][key]; ok {
								continue
							}
							group.aggExprCountSeen[normalized][key] = struct{}{}
						}
						group.aggExprCounts[normalized]++
					case "collect":
						arg, ok := parseFunctionCall(call, "collect")
						if !ok {
							continue
						}
						collectExpr, collectDistinct := parseCollectDistinctArg(arg)
						value, err := evalExpressionWithScope(collectExpr, row, params)
						if err != nil {
							return nil, nil, err
						}
						if value == nil {
							continue
						}
						if collectDistinct {
							if group.aggExprCollectSeen[normalized] == nil {
								group.aggExprCollectSeen[normalized] = map[string]struct{}{}
							}
							keyBytes, err := json.Marshal(normalizeResultValue(value))
							if err != nil {
								keyBytes = []byte(fmt.Sprintf("%v", value))
							}
							key := string(keyBytes)
							if _, ok := group.aggExprCollectSeen[normalized][key]; ok {
								continue
							}
							group.aggExprCollectSeen[normalized][key] = struct{}{}
						}
						group.aggExprCollects[normalized] = append(group.aggExprCollects[normalized], value)
					}
				}
			}
			if item.CountArg != "" {
				if item.CountArg == "*" {
					group.counts[idx]++
					continue
				}
				countExpr, countDistinct := parseCountDistinctArg(item.CountArg)
				if countExpr == "" {
					countExpr = item.CountArg
				}
				value, err := evalExpressionWithScope(countExpr, row, params)
				if err != nil {
					return nil, nil, err
				}
				if value != nil {
					if countDistinct {
						if group.countSeen[idx] == nil {
							group.countSeen[idx] = map[string]struct{}{}
						}
						keyBytes, err := json.Marshal(normalizeResultValue(value))
						if err != nil {
							keyBytes = []byte(fmt.Sprintf("%v", value))
						}
						key := string(keyBytes)
						if _, ok := group.countSeen[idx][key]; ok {
							continue
						}
						group.countSeen[idx][key] = struct{}{}
					}
					group.counts[idx]++
				}
				continue
			}
			if item.CollectArg != "" {
				collectExpr, collectDistinct := parseCollectDistinctArg(item.CollectArg)
				value, err := evalExpressionWithScope(collectExpr, row, params)
				if err != nil {
					return nil, nil, err
				}
				if value == nil {
					continue
				}
				if collectDistinct {
					if group.collectSeen[idx] == nil {
						group.collectSeen[idx] = map[string]struct{}{}
					}
					keyBytes, err := json.Marshal(normalizeResultValue(value))
					if err != nil {
						keyBytes = []byte(fmt.Sprintf("%v", value))
					}
					key := string(keyBytes)
					if _, ok := group.collectSeen[idx][key]; ok {
						continue
					}
					group.collectSeen[idx][key] = struct{}{}
				}
				group.collects[idx] = append(group.collects[idx], value)
				continue
			}
			if item.AggFunc != "" {
				agg := group.aggs[idx]
				if agg == nil {
					agg = &projectionAggregate{funcName: item.AggFunc, intOnly: true}
					group.aggs[idx] = agg
				}
				switch item.AggFunc {
				case "sum", "avg":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					n, ok := numericValue(value)
					if !ok {
						continue
					}
					agg.sum += n
					if agg.intOnly {
						integer, ok := exactIntegerAggregateValue(value)
						if ok && !isFloatLikeNumeric(value) {
							agg.intSum += integer
						} else {
							agg.intOnly = false
						}
					}
					agg.count++
					agg.hasValue = true
				case "min":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					if !agg.hasValue {
						agg.min = value
						agg.hasValue = true
						continue
					}
					if cmp, ok := compareCypherValues(value, agg.min); ok && cmp < 0 {
						agg.min = value
					}
				case "max":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					if !agg.hasValue {
						agg.max = value
						agg.hasValue = true
						continue
					}
					if cmp, ok := compareCypherValues(value, agg.max); ok && cmp > 0 {
						agg.max = value
					}
				case "percentiledisc", "percentilecont":
					valueExpr, percentileExpr, ok := parsePercentileAggregateArgs(item.AggArg)
					if !ok {
						return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
					}
					percentileRaw, err := evalExpressionWithScope(percentileExpr, row, params)
					if err != nil {
						return nil, nil, err
					}
					p, ok := numericValue(percentileRaw)
					if !ok || p < 0 || p > 1 {
						return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "NumberOutOfRange", nil)
					}
					agg.pValue = &p

					valueRaw, err := evalExpressionWithScope(valueExpr, row, params)
					if err != nil {
						return nil, nil, err
					}
					if valueRaw == nil {
						continue
					}
					n, ok := numericValue(valueRaw)
					if !ok {
						continue
					}
					agg.values = append(agg.values, n)
					agg.hasValue = true
				}
			}
		}
	}

	if len(rows) == 0 && nonAggregateCount == 0 {
		projected := Row{}
		for _, item := range items {
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			calls := extractAggregateCalls(item.Expression)
			if item.CountArg != "" {
				projected[key] = 0
			} else if item.CollectArg != "" {
				projected[key] = []any{}
			} else if item.AggFunc != "" {
				projected[key] = nil
			} else if len(calls) > 0 {
				evalRow := Row{}
				rewritten := item.Expression
				seenCalls := map[string]string{}
				for idx, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					alias, ok := seenCalls[normalized]
					if !ok {
						alias = fmt.Sprintf("__agg_expr_%d", idx)
						seenCalls[normalized] = alias
						switch aggregateFuncNameFromCall(call) {
						case "count":
							evalRow[alias] = 0
						case "collect":
							evalRow[alias] = []any{}
						default:
							evalRow[alias] = nil
						}
					}
					rewritten = strings.ReplaceAll(rewritten, call, alias)
				}
				value, err := evalExpressionWithScope(rewritten, evalRow, params)
				if err != nil {
					return nil, nil, err
				}
				projected[key] = value
			} else {
				projected[key] = nil
			}
		}
		out = append(out, projected)
		out, err = filterProjectedRows(out)
		if err != nil {
			return nil, nil, err
		}
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		return out, columns, nil
	}

	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		projected := cloneRow(group.projected)
		if len(projection.OrderBy) > 0 && !hasStar && group.source != nil {
			for key, value := range group.source {
				if _, exists := projected[key]; !exists {
					projected[key] = value
				}
			}
		}
		for idx, item := range items {
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			calls := extractAggregateCalls(item.Expression)
			if item.CountArg != "" {
				projected[key] = group.counts[idx]
				continue
			}
			if item.CollectArg != "" {
				if values, ok := group.collects[idx]; ok {
					projected[key] = append([]any(nil), values...)
				} else {
					projected[key] = []any{}
				}
				continue
			}
			if item.AggFunc != "" {
				agg := group.aggs[idx]
				if agg == nil || !agg.hasValue {
					projected[key] = nil
					continue
				}
				switch item.AggFunc {
				case "sum":
					if agg.intOnly {
						projected[key] = agg.intSum
					} else {
						projected[key] = agg.sum
					}
				case "avg":
					if agg.count == 0 {
						projected[key] = nil
					} else {
						projected[key] = json.Number(formatFloatResult(agg.sum / float64(agg.count)))
					}
				case "min":
					projected[key] = agg.min
				case "max":
					projected[key] = agg.max
				case "percentiledisc":
					if agg.pValue == nil || len(agg.values) == 0 {
						projected[key] = nil
						continue
					}
					values := append([]float64(nil), agg.values...)
					sort.Float64s(values)
					idx := int(math.Ceil(*agg.pValue*float64(len(values)))) - 1
					if idx < 0 {
						idx = 0
					}
					if idx >= len(values) {
						idx = len(values) - 1
					}
					projected[key] = json.Number(formatFloatResult(values[idx]))
				case "percentilecont":
					if agg.pValue == nil || len(agg.values) == 0 {
						projected[key] = nil
						continue
					}
					values := append([]float64(nil), agg.values...)
					sort.Float64s(values)
					if len(values) == 1 {
						projected[key] = json.Number(formatFloatResult(values[0]))
						continue
					}
					pos := *agg.pValue * float64(len(values)-1)
					low := int(math.Floor(pos))
					high := int(math.Ceil(pos))
					if low == high {
						projected[key] = json.Number(formatFloatResult(values[low]))
						continue
					}
					frac := pos - float64(low)
					interpolated := values[low] + (values[high]-values[low])*frac
					projected[key] = json.Number(formatFloatResult(interpolated))
				}
				continue
			}
			if len(calls) > 0 {
				evalRow := Row{}
				for k, v := range projected {
					evalRow[k] = v
				}
				for k, v := range group.source {
					if _, exists := evalRow[k]; !exists {
						evalRow[k] = v
					}
				}
				rewritten := item.Expression
				seenCalls := map[string]string{}
				for idx, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					alias, ok := seenCalls[normalized]
					if !ok {
						alias = fmt.Sprintf("__agg_expr_%d", idx)
						seenCalls[normalized] = alias
						switch aggregateFuncNameFromCall(call) {
						case "count":
							evalRow[alias] = group.aggExprCounts[normalized]
						case "collect":
							if values, ok := group.aggExprCollects[normalized]; ok {
								evalRow[alias] = append([]any(nil), values...)
							} else {
								evalRow[alias] = []any{}
							}
						default:
							evalRow[alias] = nil
						}
					}
					rewritten = strings.ReplaceAll(rewritten, call, alias)
				}
				value, err := evalExpressionWithScope(rewritten, evalRow, params)
				if err != nil {
					return nil, nil, err
				}
				projected[key] = value
			}
		}
		out = append(out, projected)
	}
	if hasStar {
		columns = inferProjectionColumns(out)
		if len(columns) == 0 && len(priorColumns) > 0 {
			columns = append([]string(nil), priorColumns...)
		}
	}
	out, err = filterProjectedRows(out)
	if err != nil {
		return nil, nil, err
	}
	out, err = applyProjectionPostProcessing(out, projection, params)
	if err != nil {
		return nil, nil, err
	}
	if len(projection.OrderBy) > 0 && !hasStar {
		out = trimProjectionRows(out, columns)
	}
	return out, columns, nil
}

type projectionSpec struct {
	Expression string
	Alias      string
	CountArg   string
	CollectArg string
	AggFunc    string
	AggArg     string
}

type projectionClauseSpec struct {
	Distinct      bool
	ProjectionRaw string
	WhereRaw      string
	OrderBy       []projectionOrderBySpec
	SkipRaw       string
	LimitRaw      string
}

type projectionOrderBySpec struct {
	Expression string
	Descending bool
}

func parseProjectionClauseSpec(raw string) (projectionClauseSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return projectionClauseSpec{}, nil
	}

	orderByIdx := findTopLevelKeywordIndex(raw, "ORDERBY")
	skipIdx := findTopLevelKeywordIndex(raw, "SKIP")
	limitIdx := findTopLevelKeywordIndex(raw, "LIMIT")

	firstTail := minPositiveIndex(orderByIdx, skipIdx, limitIdx)
	projectionRaw := raw
	if firstTail >= 0 {
		projectionRaw = raw[:firstTail]
	}
	projectionDistinct := false
	if strings.HasPrefix(strings.ToUpper(projectionRaw), "DISTINCT") {
		projectionDistinct = true
		projectionRaw = strings.TrimSpace(projectionRaw[len("DISTINCT"):])
	}

	out := projectionClauseSpec{Distinct: projectionDistinct, ProjectionRaw: strings.TrimSpace(projectionRaw)}
	if whereIdx := findTopLevelKeywordIndex(out.ProjectionRaw, "WHERE"); whereIdx >= 0 {
		out.WhereRaw = strings.TrimSpace(out.ProjectionRaw[whereIdx+len("WHERE"):])
		out.ProjectionRaw = strings.TrimSpace(out.ProjectionRaw[:whereIdx])
	}

	if orderByIdx >= 0 {
		end := minPositiveIndex(greaterIndex(skipIdx, orderByIdx), greaterIndex(limitIdx, orderByIdx))
		if end < 0 {
			end = len(raw)
		}
		orderByWidth := len("ORDERBY")
		if strings.HasPrefix(strings.ToUpper(raw[orderByIdx:]), "ORDER BY") {
			orderByWidth = len("ORDER BY")
		}
		orderByRaw := strings.TrimSpace(raw[orderByIdx+orderByWidth : end])
		items, err := parseProjectionOrderBy(orderByRaw)
		if err != nil {
			return projectionClauseSpec{}, err
		}
		out.OrderBy = items
	}

	if skipIdx >= 0 {
		end := greaterIndex(limitIdx, skipIdx)
		if end < 0 {
			end = len(raw)
		}
		out.SkipRaw = strings.TrimSpace(raw[skipIdx+len("SKIP") : end])
	}

	if limitIdx >= 0 {
		out.LimitRaw = strings.TrimSpace(raw[limitIdx+len("LIMIT"):])
	}

	if out.ProjectionRaw == "" {
		return projectionClauseSpec{}, graph.NewError(graph.ErrKindInvalidInput, "projection clause requires at least one item", nil)
	}

	return out, nil
}

func parseProjectionOrderBy(raw string) ([]projectionOrderBySpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "ORDER BY requires at least one expression", nil)
	}

	parts := splitTopLevelCommaSeparated(raw)
	out := make([]projectionOrderBySpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		upper := strings.ToUpper(part)
		spec := projectionOrderBySpec{}
		switch {
		case strings.HasSuffix(upper, "DESCENDING"):
			spec.Descending = true
			spec.Expression = strings.TrimSpace(part[:len(part)-len("DESCENDING")])
		case strings.HasSuffix(upper, "ASCENDING"):
			spec.Expression = strings.TrimSpace(part[:len(part)-len("ASCENDING")])
		case strings.HasSuffix(upper, "DESC"):
			spec.Descending = true
			spec.Expression = strings.TrimSpace(part[:len(part)-len("DESC")])
		case strings.HasSuffix(upper, "ASC"):
			spec.Expression = strings.TrimSpace(part[:len(part)-len("ASC")])
		default:
			spec.Expression = strings.TrimSpace(part)
		}
		if spec.Expression == "" {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("ORDER BY expression %q is invalid", part), nil)
		}
		out = append(out, spec)
	}

	if len(out) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "ORDER BY requires at least one expression", nil)
	}

	return out, nil
}

func validateProjectionOrderBy(items []projectionSpec, orderBy []projectionOrderBySpec, rows []Row, distinct bool) error {
	if len(orderBy) == 0 {
		return nil
	}

	hasProjectionAggregate := false
	projectedAggFuncs := map[string]struct{}{}
	for _, item := range items {
		if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" {
			hasProjectionAggregate = true
			if item.CountArg != "" {
				projectedAggFuncs["count"] = struct{}{}
			}
			if item.CollectArg != "" {
				projectedAggFuncs["collect"] = struct{}{}
			}
			if item.AggFunc != "" {
				projectedAggFuncs[strings.ToLower(item.AggFunc)] = struct{}{}
			}
		}
	}

	inScope := map[string]struct{}{}
	distinctScope := map[string]struct{}{}
	distinctExpandableRoots := map[string]struct{}{}
	if !distinct && len(rows) > 0 {
		for key := range rows[0] {
			inScope[key] = struct{}{}
		}
	}
	for _, item := range items {
		rawExpr := strings.TrimSpace(item.Expression)
		distinctScope[normalizeProjectionExpr(item.Expression)] = struct{}{}
		if item.Alias != "" {
			inScope[item.Alias] = struct{}{}
			distinctScope[normalizeProjectionExpr(item.Alias)] = struct{}{}
		}
		if ident, ok := parseSimpleIdentifierRoot(item.Expression); ok {
			inScope[ident] = struct{}{}
			if rawExpr == ident {
				distinctExpandableRoots[ident] = struct{}{}
			}
		}
	}

	for _, spec := range orderBy {
		expr := strings.TrimSpace(spec.Expression)
		if expr == "" {
			continue
		}
		hasAgg := containsAggregationExpression(expr)
		if hasAgg && !hasProjectionAggregate {
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "invalid aggregation expression"}
		}
		if hasAgg {
			calls := extractAggregateCalls(expr)
			for _, call := range calls {
				fn := aggregateFuncNameFromCall(call)
				if _, ok := projectedAggFuncs[fn]; !ok {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
				}
			}
			stripped := stripAggregateCalls(expr)
			for _, ident := range extractIdentifierRoots(stripped) {
				if _, ok := inScope[ident]; !ok {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
				}
			}
			continue
		}
		if distinct {
			if _, ok := distinctScope[normalizeProjectionExpr(expr)]; ok {
				continue
			}
			if ident, ok := parseSimpleIdentifierRoot(expr); ok {
				if _, in := distinctExpandableRoots[ident]; in {
					continue
				}
			}
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
		}
		if ident, ok := parseSimpleIdentifierRoot(expr); ok {
			if _, ok := inScope[ident]; !ok {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
			}
		}
	}

	return nil
}

func normalizeProjectionExpr(raw string) string {
	return strings.ToUpper(normalizeClauseBody(strings.TrimSpace(raw)))
}

func containsAggregationExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	return len(extractAggregateCalls(raw)) > 0
}

func normalizeAggregateExprCall(call string) string {
	call = strings.TrimSpace(call)
	idx := strings.Index(call, "(")
	if idx < 0 || !strings.HasSuffix(call, ")") {
		return strings.ToLower(call)
	}
	fn := strings.ToLower(strings.TrimSpace(call[:idx]))
	arg := strings.ToLower(strings.TrimSpace(call[idx+1 : len(call)-1]))
	return fn + "(" + arg + ")"
}

func projectionKey(item projectionSpec) string {
	if item.Alias != "" {
		return item.Alias
	}
	return item.Expression
}

func rewriteOrderByAggregateReferences(orderBy []projectionOrderBySpec, items []projectionSpec) []projectionOrderBySpec {
	if len(orderBy) == 0 {
		return orderBy
	}
	aggMap := map[string]string{}
	for _, item := range items {
		key := projectionKey(item)
		if item.CountArg != "" {
			aggMap[normalizeAggregateExprCall("count("+item.CountArg+")")] = key
		}
		if item.CollectArg != "" {
			aggMap[normalizeAggregateExprCall("collect("+item.CollectArg+")")] = key
		}
		if item.AggFunc != "" {
			aggMap[normalizeAggregateExprCall(item.AggFunc+"("+item.AggArg+")")] = key
		}
	}
	if len(aggMap) == 0 {
		return orderBy
	}
	out := make([]projectionOrderBySpec, 0, len(orderBy))
	for _, spec := range orderBy {
		expr := spec.Expression
		for _, call := range extractAggregateCalls(expr) {
			if repl, ok := aggMap[normalizeAggregateExprCall(call)]; ok {
				expr = strings.ReplaceAll(expr, call, repl)
			}
		}
		out = append(out, projectionOrderBySpec{Expression: expr, Descending: spec.Descending})
	}
	return out
}

func aggregateFuncNameFromCall(call string) string {
	call = strings.TrimSpace(call)
	idx := strings.Index(call, "(")
	if idx < 0 || !strings.HasSuffix(call, ")") {
		return strings.ToLower(call)
	}
	return strings.ToLower(strings.TrimSpace(call[:idx]))
}

func extractAggregateCalls(raw string) []string {
	calls := []string{}
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			i++
			continue
		}
		if i > 0 && raw[i-1] == '$' {
			j := i + 1
			for j < len(raw) && isIdentifierPart(raw[j]) {
				j++
			}
			i = j
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := strings.ToLower(strings.TrimSpace(raw[i:j]))
		k := skipSpaces(raw, j)
		if k >= len(raw) || raw[k] != '(' || !isAggregateFunctionName(name) {
			i = j
			continue
		}
		end := findClosingParen(raw, k)
		if end < 0 {
			break
		}
		calls = append(calls, strings.TrimSpace(raw[i:end+1]))
		i = end + 1
	}
	return calls
}

func stripAggregateCalls(raw string) string {
	var out strings.Builder
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			out.WriteByte(raw[i])
			i++
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := strings.ToLower(strings.TrimSpace(raw[i:j]))
		k := skipSpaces(raw, j)
		if k >= len(raw) || raw[k] != '(' || !isAggregateFunctionName(name) {
			out.WriteString(raw[i:j])
			i = j
			continue
		}
		end := findClosingParen(raw, k)
		if end < 0 {
			out.WriteString(raw[i:])
			break
		}
		out.WriteString("0")
		i = end + 1
	}
	return out.String()
}

func isAggregateFunctionName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "count", "collect", "sum", "min", "max", "avg", "percentiledisc", "percentilecont":
		return true
	default:
		return false
	}
}

func parsePercentileAggregateArgs(raw string) (string, string, bool) {
	parts := splitTopLevelCommaSeparated(raw)
	if len(parts) != 2 {
		return "", "", false
	}
	valueExpr := strings.TrimSpace(parts[0])
	percentileExpr := strings.TrimSpace(parts[1])
	if valueExpr == "" || percentileExpr == "" {
		return "", "", false
	}
	return valueExpr, percentileExpr, true
}

func findClosingParen(raw string, openIdx int) int {
	depth := 0
	inSingle := false
	for i := openIdx; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if inSingle {
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
		}
	}
	return -1
}

func skipSpaces(raw string, i int) int {
	for i < len(raw) {
		if raw[i] != ' ' && raw[i] != '\t' && raw[i] != '\n' && raw[i] != '\r' {
			break
		}
		i++
	}
	return i
}

func extractIdentifierRoots(raw string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			i++
			continue
		}
		if i > 0 && raw[i-1] == '$' {
			j := i + 1
			for j < len(raw) && isIdentifierPart(raw[j]) {
				j++
			}
			i = j
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := raw[i:j]
		lower := strings.ToLower(name)
		if lower == "true" || lower == "false" || lower == "null" || isAggregateFunctionName(lower) {
			i = j
			continue
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			out = append(out, name)
		}
		i = j
	}
	return out
}

func parseSimpleIdentifierRoot(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if strings.ContainsAny(raw, "()+-*/%<>=,![]{}") {
		return "", false
	}
	root := raw
	if idx := strings.Index(root, "."); idx >= 0 {
		root = root[:idx]
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return "", false
	}
	if !isIdentifierLike(root) {
		return "", false
	}
	return root, true
}

func isIdentifierLike(raw string) bool {
	if raw == "" {
		return false
	}
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (i > 0 && ch >= '0' && ch <= '9') {
			continue
		}
		return false
	}
	return true
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func applyProjectionPostProcessing(rows []Row, clause projectionClauseSpec, params Params) ([]Row, error) {
	if clause.Distinct {
		rows = distinctProjectionRows(rows)
	}
	if len(clause.OrderBy) > 0 && len(rows) > 1 {
		sorted, err := sortProjectedRows(rows, clause.OrderBy, params)
		if err != nil {
			return nil, err
		}
		rows = sorted
	}

	skip, err := evalOptionalInt(rawExpression(clause.SkipRaw), params)
	if err != nil {
		return nil, err
	}
	limit, err := evalOptionalInt(rawExpression(clause.LimitRaw), params)
	if err != nil {
		return nil, err
	}

	hasLimit := strings.TrimSpace(clause.LimitRaw) != ""
	return applySkipLimit(rows, skip, limit, hasLimit), nil
}

func distinctProjectionRows(rows []Row) []Row {
	if len(rows) <= 1 {
		return rows
	}
	seen := map[string]struct{}{}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		canonical := map[string]any{}
		for key, value := range row {
			canonical[key] = normalizeResultValue(value)
		}
		keyBytes, err := json.Marshal(canonical)
		if err != nil {
			keyBytes = []byte(fmt.Sprintf("%v", canonical))
		}
		key := string(keyBytes)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func trimProjectionRows(rows []Row, columns []string) []Row {
	if len(rows) == 0 || len(columns) == 0 {
		return rows
	}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		trimmed := Row{}
		for _, col := range columns {
			trimmed[col] = row[col]
		}
		out = append(out, trimmed)
	}
	return out
}

func sortProjectedRows(rows []Row, orderBy []projectionOrderBySpec, params Params) ([]Row, error) {
	type sortRow struct {
		row  Row
		keys []any
	}

	indexed := make([]sortRow, 0, len(rows))
	for _, row := range rows {
		keys := make([]any, 0, len(orderBy))
		for _, item := range orderBy {
			value, err := evalExpressionWithScope(item.Expression, row, params)
			if err != nil {
				return nil, err
			}
			keys = append(keys, value)
		}
		indexed = append(indexed, sortRow{row: row, keys: keys})
	}

	sort.SliceStable(indexed, func(i, j int) bool {
		for idx, item := range orderBy {
			cmp := compareSortValues(indexed[i].keys[idx], indexed[j].keys[idx])
			if cmp == 0 {
				continue
			}
			if item.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	out := make([]Row, 0, len(rows))
	for _, item := range indexed {
		out = append(out, item.row)
	}

	return out, nil
}

func compareSortValues(left, right any) int {
	cmp, ok := compareCypherValues(left, right)
	if ok {
		return cmp
	}
	leftText := fmt.Sprint(left)
	rightText := fmt.Sprint(right)
	switch {
	case leftText < rightText:
		return -1
	case leftText > rightText:
		return 1
	default:
		return 0
	}
}

func compareCypherValues(lhs, rhs any) (int, bool) {
	if lhs == nil && rhs == nil {
		return 0, true
	}
	if lhs == nil {
		// Cypher ORDER BY places null values after non-null values.
		return 1, true
	}
	if rhs == nil {
		return -1, true
	}

	leftRank := cypherSortRank(lhs)
	rightRank := cypherSortRank(rhs)
	if leftRank != rightRank {
		if leftRank < rightRank {
			return -1, true
		}
		return 1, true
	}

	if leftMap, leftTemporal := temporalMapValue(lhs); leftTemporal {
		if rightMap, rightTemporal := temporalMapValue(rhs); rightTemporal {
			if equal, ok := compareTemporalMaps(leftMap, rightMap, "="); ok && equal {
				return 0, true
			}
			if less, ok := compareTemporalMaps(leftMap, rightMap, "<"); ok {
				if less {
					return -1, true
				}
				return 1, true
			}
		}
	}

	if lf, lok := comparableNumericValue(lhs); lok {
		if rf, rok := comparableNumericValue(rhs); rok {
			leftNaN := math.IsNaN(lf)
			rightNaN := math.IsNaN(rf)
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
			case lf < rf:
				return -1, true
			case lf > rf:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if lb, lok := lhs.(bool); lok {
		if rb, rok := rhs.(bool); rok {
			switch {
			case !lb && rb:
				return -1, true
			case lb && !rb:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if ls, lok := lhs.(string); lok {
		if rs, rok := rhs.(string); rok {
			switch {
			case ls < rs:
				return -1, true
			case ls > rs:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if _, lhsString := lhs.(string); lhsString {
		if _, rhsNumeric := comparableNumericValue(rhs); rhsNumeric {
			return -1, true
		}
	}
	if _, rhsString := rhs.(string); rhsString {
		if _, lhsNumeric := comparableNumericValue(lhs); lhsNumeric {
			return 1, true
		}
	}

	if ll, lok := asAnySlice(lhs); lok {
		if rl, rok := asAnySlice(rhs); rok {
			limit := len(ll)
			if len(rl) < limit {
				limit = len(rl)
			}
			for i := 0; i < limit; i++ {
				cmp, ok := compareCypherValues(ll[i], rl[i])
				if !ok {
					return 0, false
				}
				if cmp != 0 {
					return cmp, true
				}
			}
			switch {
			case len(ll) < len(rl):
				return -1, true
			case len(ll) > len(rl):
				return 1, true
			default:
				return 0, true
			}
		}
	}

	return 0, false
}

func cypherSortRank(value any) int {
	if value == nil {
		return 90
	}
	if f, ok := comparableNumericValue(value); ok {
		if math.IsNaN(f) {
			return 80
		}
		return 70
	}
	switch typed := value.(type) {
	case map[string]any:
		if isRelationshipMapShape(typed) {
			return 20
		}
		if isNodeMapShape(typed) {
			return 10
		}
		return 0
	case *graph.Vertex:
		return 10
	case *graph.Edge:
		return 20
	case []any, []string:
		return 30
	case cypherPathValue:
		return 40
	case string:
		return 50
	case bool:
		return 60
	default:
		if _, ok := asAnySlice(value); ok {
			return 30
		}
		if rv := reflect.ValueOf(value); rv.IsValid() {
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				return 30
			}
		}
		return 85
	}
}

func isNodeMapShape(value map[string]any) bool {
	if value == nil {
		return false
	}
	_, hasLabels := value["labels"]
	_, hasProps := value["properties"]
	_, hasType := value["type"]
	return hasLabels && hasProps && !hasType
}

func isRelationshipMapShape(value map[string]any) bool {
	if value == nil {
		return false
	}
	_, hasType := value["type"]
	_, hasProps := value["properties"]
	_, hasSrc := value["src"]
	_, hasDst := value["dst"]
	return hasType && hasProps && hasSrc && hasDst
}

func asAnySlice(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	default:
		return nil, false
	}
}

func rawExpression(raw string) *ast.Expression {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return &ast.Expression{Raw: raw}
}

func greaterIndex(value int, floor int) int {
	if value > floor {
		return value
	}
	return -1
}

func minPositiveIndex(values ...int) int {
	best := -1
	for _, value := range values {
		if value < 0 {
			continue
		}
		if best == -1 || value < best {
			best = value
		}
	}
	return best
}

func findTopLevelKeywordIndex(raw, keyword string) int {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" || len(upper) < len(keyword) {
		return -1
	}

	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 {
			if strings.HasPrefix(upper[i:], keyword) {
				return i
			}
			if keyword == "ORDERBY" && strings.HasPrefix(upper[i:], "ORDER BY") {
				return i
			}
		}
	}

	return -1
}

func parseProjectionItems(raw string) ([]projectionSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := splitTopLevelCommaSeparated(raw)
	items := make([]projectionSpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" {
			items = append(items, projectionSpec{Expression: "*"})
			continue
		}
		alias := ""
		if idx := findTopLevelAliasIndex(part); idx > 0 {
			expr := strings.TrimSpace(part[:idx])
			alias = normalizeProjectionIdentifier(strings.TrimSpace(part[idx+2:]))
			if expr == "" || alias == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection item %q is not supported", part), nil)
			}
			countArg, _ := parseCountExpression(expr)
			collectArg, _ := parseCollectExpression(expr)
			aggFunc, aggArg, _ := parseNamedAggregateExpression(expr)
			if err := validateAggregateArgumentConstant(countArg); err != nil {
				return nil, err
			}
			if err := validateAggregateArgumentConstant(collectArg); err != nil {
				return nil, err
			}
			if err := validateAggregateArgumentConstant(aggArg); err != nil {
				return nil, err
			}
			items = append(items, projectionSpec{Expression: expr, Alias: alias, CountArg: countArg, CollectArg: collectArg, AggFunc: aggFunc, AggArg: aggArg})
			continue
		}
		countArg, _ := parseCountExpression(part)
		collectArg, _ := parseCollectExpression(part)
		aggFunc, aggArg, _ := parseNamedAggregateExpression(part)
		if err := validateAggregateArgumentConstant(countArg); err != nil {
			return nil, err
		}
		if err := validateAggregateArgumentConstant(collectArg); err != nil {
			return nil, err
		}
		if err := validateAggregateArgumentConstant(aggArg); err != nil {
			return nil, err
		}
		items = append(items, projectionSpec{Expression: part, CountArg: countArg, CollectArg: collectArg, AggFunc: aggFunc, AggArg: aggArg})
	}
	return items, nil
}

func validateAggregateArgumentConstant(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.Contains(strings.ToLower(raw), "rand(") {
		return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NonConstantExpression"}
	}
	return nil
}

func findTopLevelAliasIndex(raw string) int {
	upper := strings.ToUpper(raw)
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	candidates := make([]int, 0, 2)

	for i := 0; i <= len(raw)-2; i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
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
		if upper[i:i+2] != "AS" {
			continue
		}
		candidates = append(candidates, i)
	}

	for i := len(candidates) - 1; i >= 0; i-- {
		idx := candidates[i]
		lhs := strings.TrimSpace(raw[:idx])
		rhs := strings.TrimSpace(raw[idx+2:])
		if lhs == "" || rhs == "" {
			continue
		}
		if strings.HasPrefix(rhs, "`") && strings.HasSuffix(rhs, "`") && len(rhs) >= 2 {
			return idx
		}
		if isIdentifierLike(rhs) {
			return idx
		}
	}

	return -1
}

func inferProjectionColumns(rows []Row) []string {
	keySet := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			keySet[key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(keySet))
	for key := range keySet {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	return columns
}

func parseFunctionCall(raw string, name string) (string, bool) {
	raw = strings.TrimSpace(raw)
	name = strings.TrimSpace(name)
	if raw == "" || name == "" {
		return "", false
	}
	prefix := name + "("
	if len(raw) <= len(prefix) || !strings.HasSuffix(raw, ")") {
		return "", false
	}
	if !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", false
	}
	arg := strings.TrimSpace(raw[len(prefix) : len(raw)-1])
	return arg, true
}

func normalizeProjectionIdentifier(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "`") && strings.HasSuffix(raw, "`") && len(raw) >= 2 {
		return raw[1 : len(raw)-1]
	}
	return raw
}

func parseCountExpression(raw string) (string, bool) {
	arg, ok := parseFunctionCall(raw, "count")
	if !ok || arg == "" {
		return "", false
	}
	return arg, true
}

func parseCountDistinctArg(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "DISTINCT") {
		return strings.TrimSpace(raw[len("DISTINCT"):]), true
	}
	return raw, false
}

func parseCollectExpression(raw string) (string, bool) {
	arg, ok := parseFunctionCall(raw, "collect")
	if !ok || arg == "" {
		return "", false
	}
	return arg, true
}

func parseCollectDistinctArg(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "DISTINCT") {
		return strings.TrimSpace(raw[len("DISTINCT"):]), true
	}
	return raw, false
}

func parseNamedAggregateExpression(raw string) (string, string, bool) {
	aggFuncs := []string{"sum", "min", "max", "avg", "percentileDisc", "percentileCont"}
	for _, fn := range aggFuncs {
		arg, ok := parseFunctionCall(raw, fn)
		if !ok || strings.TrimSpace(arg) == "" {
			continue
		}
		return strings.ToLower(fn), strings.TrimSpace(arg), true
	}
	return "", "", false
}

func splitTopLevelCommaSeparated(raw string) []string {
	parts := []string{}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	start := 0
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
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
				parts = append(parts, raw[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, raw[start:])
	return parts
}

func evalExpressionWithScope(raw string, row Row, params Params) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "empty expression", nil)
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	if value, ok := resolveBareIdentifier(raw, row, params); ok {
		return value, nil
	}
	if inner, ok := unwrapOuterParentheses(raw); ok {
		return evalExpressionWithScope(inner, row, params)
	}
	if value, ok, err := evalCaseExpression(raw, row, params); ok {
		return value, err
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "OR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("OR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "OR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("OR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "XOR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("XOR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "XOR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("XOR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "AND"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("AND", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("AND", lhs, rhs)
	}
	if hasLogicalNotPrefix(raw) {
		value, err := evalExpressionWithScope(strings.TrimSpace(raw[3:]), row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanNot(value)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "STARTS WITH"); ok {
		return evalStringPredicateExpression(left, right, "STARTS WITH", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "STARTSWITH"); ok {
		return evalStringPredicateExpression(left, right, "STARTS WITH", row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "ENDS WITH"); ok {
		return evalStringPredicateExpression(left, right, "ENDS WITH", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "ENDSWITH"); ok {
		return evalStringPredicateExpression(left, right, "ENDS WITH", row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "CONTAINS"); ok {
		return evalStringPredicateExpression(left, right, "CONTAINS", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "CONTAINS"); ok {
		return evalStringPredicateExpression(left, right, "CONTAINS", row, params)
	}
	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return evalExpressionWithScope(raw[1:len(raw)-1], row, params)
	}
	if left, labels, ok := splitTopLevelLabelPredicate(raw); ok {
		return evalLabelPredicateExpression(left, labels, row, params)
	}
	if strings.HasPrefix(raw, "-(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[2:len(raw)-1]) {
		value, err := evalExpressionWithScope(raw[2:len(raw)-1], row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		if integer, err := toInt(value); err == nil {
			return -integer, nil
		}
		if numeric, ok := numericValue(value); ok {
			return json.Number(formatFloatResult(-numeric)), nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, right, ok := splitTopLevelOperator(raw, ">="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, ">=")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, "<=")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<>"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValuesWithRaw(lhs, rhs, "<>", left, right)
	}
	if left, right, ok := splitTopLevelOperator(raw, "="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValuesWithRaw(lhs, rhs, "=", left, right)
	}
	if left, right, ok := splitTopLevelOperator(raw, ">"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, ">")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, "<")
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalInExpression(lhs, rhs)
	}
	if left, right, op, ok := splitTopLevelOperatorSetLast(raw, "+", "-"); ok {
		return evalAdditiveExpression(op, left, right, raw, row, params)
	}
	if left, right, op, ok := splitTopLevelOperatorSetLast(raw, "*", "/", "%"); ok {
		return evalMultiplicativeExpression(op, left, right, raw, row, params)
	}
	if left, right, ok := splitTopLevelOperatorLast(raw, "^"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		if lhs == nil || rhs == nil {
			return nil, nil
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return json.Number(formatFloatResult(math.Pow(lf, rf))), nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, isNull, ok := splitTopLevelNullPredicate(raw); ok {
		value, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			return value == nil, nil
		}
		return value != nil, nil
	}
	if arg, ok := parseFunctionCall(raw, "rand"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "rand() expects no arguments", nil)
		}
		return rand.Float64(), nil
	}
	if value, ok, err := evalTemporalNamespaceFunction(raw, row, params); ok {
		return value, err
	}
	if value, ok, err := evalListPredicateFunction(raw, row, params); ok {
		return value, err
	}
	if value, ok, err := evalListComprehension(raw, row, params); ok {
		return value, err
	}
	if arg, ok := parseFunctionCall(raw, "size"); ok {
		if patternValue, handled, err := evalPatternComprehensionFromRuntime(arg, row, params); handled {
			if err != nil {
				return nil, err
			}
			switch typed := patternValue.(type) {
			case nil:
				return nil, nil
			case []any:
				return len(typed), nil
			case []string:
				return len(typed), nil
			default:
				return nil, graph.NewError(graph.ErrKindSemantic, "size() requires a list, map, or string", nil)
			}
		}
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case nil:
			return nil, nil
		case []any:
			return len(typed), nil
		case []string:
			return len(typed), nil
		case string:
			return len([]rune(typed)), nil
		case map[string]any:
			return len(typed), nil
		default:
			return nil, graph.NewError(graph.ErrKindSemantic, "size() requires a list, map, or string", nil)
		}
	}
	if arg, ok := parseFunctionCall(raw, "range"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) < 2 || len(parts) > 3 {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() expects 2 or 3 arguments", nil)
		}
		startVal, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		endVal, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		start, err := toInt(startVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() start must be an integer", err)
		}
		end, err := toInt(endVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() end must be an integer", err)
		}
		step := 1
		if len(parts) == 3 {
			stepVal, err := evalExpressionWithScope(parts[2], row, params)
			if err != nil {
				return nil, err
			}
			step, err = toInt(stepVal)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "range() step must be an integer", err)
			}
			if step == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "range() step cannot be zero", nil)
			}
		}
		out := []any{}
		if step > 0 {
			for i := start; i <= end; i += step {
				out = append(out, i)
			}
		} else {
			for i := start; i >= end; i += step {
				out = append(out, i)
			}
		}
		return out, nil
	}
	if arg, ok := parseFunctionCall(raw, "toString"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToStringValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toInteger"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToIntegerValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toBoolean"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToBooleanValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toFloat"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToFloatValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "ceil"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Ceil(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "coalesce"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) == 0 {
			return nil, graph.NewError(graph.ErrKindSemantic, "coalesce() expects at least one argument", nil)
		}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, err := evalExpressionWithScope(part, row, params)
			if err != nil {
				return nil, err
			}
			if value != nil {
				return value, nil
			}
		}
		return nil, nil
	}
	if arg, ok := parseFunctionCall(raw, "reverse"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		if list, ok := normalizeListValue(value); ok {
			out := make([]any, len(list))
			for i := range list {
				out[i] = list[len(list)-1-i]
			}
			return out, nil
		}
		if str, ok := value.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes), nil
		}
		return nil, graph.NewError(graph.ErrKindSemantic, "reverse() requires a list or string", nil)
	}
	if arg, ok := parseFunctionCall(raw, "split"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "split() expects exactly two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		delim, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || delim == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		delimStr, ok := delim.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		split := strings.Split(inputStr, delimStr)
		out := make([]any, 0, len(split))
		for _, s := range split {
			out = append(out, s)
		}
		return out, nil
	}
	if arg, ok := parseFunctionCall(raw, "substring"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 && len(parts) != 3 {
			return nil, graph.NewError(graph.ErrKindSemantic, "substring() expects two or three arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		startVal, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || startVal == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		start, err := toInt(startVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
		}
		runes := []rune(inputStr)
		if start < 0 {
			start = len(runes) + start
		}
		if start < 0 {
			start = 0
		}
		if start > len(runes) {
			return "", nil
		}
		end := len(runes)
		if len(parts) == 3 {
			lengthVal, err := evalExpressionWithScope(parts[2], row, params)
			if err != nil {
				return nil, err
			}
			if lengthVal == nil {
				return nil, nil
			}
			length, err := toInt(lengthVal)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if length <= 0 {
				return "", nil
			}
			end = start + length
			if end > len(runes) {
				end = len(runes)
			}
		}
		if start > end {
			return "", nil
		}
		return string(runes[start:end]), nil
	}
	if arg, ok := parseFunctionCall(raw, "toLower"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToLower(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "toUpper"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToUpper(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "keys"); ok {
		return evalKeysFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "head"); ok {
		return evalHeadFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "tail"); ok {
		return evalTailFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "abs"); ok {
		return evalAbsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "nodes"); ok {
		return evalNodesFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "relationships"); ok {
		return evalRelationshipsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "length"); ok {
		return evalLengthFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "last"); ok {
		return evalLastFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "sign"); ok {
		return evalSignFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "startNode"); ok {
		return evalStartNodeFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "endNode"); ok {
		return evalEndNodeFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "date.truncate"); ok {
		return evalTemporalTruncateFunction("date", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "time.truncate"); ok {
		return evalTemporalTruncateFunction("time", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "datetime.truncate"); ok {
		return evalTemporalTruncateFunction("datetime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localtime.truncate"); ok {
		return evalTemporalTruncateFunction("localtime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localdatetime.truncate"); ok {
		return evalTemporalTruncateFunction("localdatetime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "labels"); ok {
		return evalLabelsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "type"); ok {
		return evalTypeFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "properties"); ok {
		return evalPropertiesFunction(arg, row, params)
	}
	if baseExpr, indexExpr, ok := splitTrailingSubscript(raw); ok {
		base, err := evalExpressionWithScope(baseExpr, row, params)
		if err != nil {
			base, err = evalWriteValue(baseExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		if startExpr, endExpr, ok := splitTopLevelSliceBounds(indexExpr); ok {
			start, hasStart, startIsNull, err := evalSliceBound(startExpr, row, params)
			if err != nil {
				return nil, err
			}
			end, hasEnd, endIsNull, err := evalSliceBound(endExpr, row, params)
			if err != nil {
				return nil, err
			}
			if startIsNull || endIsNull {
				return nil, nil
			}
			switch typed := base.(type) {
			case nil:
				return nil, nil
			case []any:
				return applySliceAny(typed, start, end, hasStart, hasEnd), nil
			case []string:
				return applySliceStringList(typed, start, end, hasStart, hasEnd), nil
			case string:
				return applySliceString(typed, start, end, hasStart, hasEnd), nil
			default:
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
			}
		}
		indexValue, err := evalExpressionWithScope(indexExpr, row, params)
		if err != nil {
			indexValue, err = evalWriteValue(indexExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		switch typed := base.(type) {
		case nil:
			return nil, nil
		case *graph.Vertex:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if key == "id" {
				if !shouldExposeEntityID(typed.ID) {
					return nil, nil
				}
				return typed.ID, nil
			}
			if typed.Properties == nil {
				return nil, nil
			}
			raw, ok := typed.Properties[key]
			if !ok {
				return nil, nil
			}
			return decodeStoredPropertyValue(raw), nil
		case *graph.Edge:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if key == "id" {
				if !shouldExposeEntityID(typed.ID) {
					return nil, nil
				}
				return typed.ID, nil
			}
			if typed.Properties == nil {
				return nil, nil
			}
			raw, ok := typed.Properties[key]
			if !ok {
				return nil, nil
			}
			return decodeStoredPropertyValue(raw), nil
		case []any:
			idx, err := listIndexToInt(indexValue)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if idx < 0 {
				idx = len(typed) + idx
			}
			if idx < 0 || idx >= len(typed) {
				return nil, nil
			}
			return typed[idx], nil
		case []string:
			idx, err := listIndexToInt(indexValue)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if idx < 0 {
				idx = len(typed) + idx
			}
			if idx < 0 || idx >= len(typed) {
				return nil, nil
			}
			return typed[idx], nil
		case map[string]any:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if value, ok := typed[key]; ok {
				return value, nil
			}
			return nil, nil
		default:
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
	}
	if value, err := evalWriteValue(raw, params, row); err == nil {
		return value, nil
	}
	if value, handled, err := evalPatternComprehensionFromRuntime(raw, row, params); handled {
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	baseExpr, fields, ok := splitTopLevelFieldAccess(raw)
	if ok && len(fields) >= 1 {
		var base any
		if isIdentifierLike(baseExpr) {
			if value, exists := row[baseExpr]; exists {
				base = value
			} else if value, exists := params[baseExpr]; exists {
				base = value
			} else if value, err := evalExpressionWithScope(baseExpr, row, params); err == nil {
				base = value
			} else {
				return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", baseExpr), nil)
			}
		} else {
			value, err := evalExpressionWithScope(baseExpr, row, params)
			if err != nil {
				return nil, err
			}
			base = value
		}
		for i := 0; i < len(fields); i++ {
			if base == nil {
				return nil, nil
			}
			next, err := evalFieldAccessValue(base, fields[i])
			if err != nil {
				return nil, err
			}
			base = next
		}
		return base, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func listIndexToInt(v any) (int, error) {
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
	default:
		return 0, fmt.Errorf("unsupported list index type %T", v)
	}
}

func evalPatternComprehensionFromRuntime(raw string, row Row, params Params) (any, bool, error) {
	execRaw, ok := params[projectionEvalExecutorParam]
	if !ok || execRaw == nil {
		return nil, false, nil
	}
	exec, ok := execRaw.(*Executor)
	if !ok || exec == nil {
		return nil, false, nil
	}
	txRaw, ok := params[projectionEvalTxParam]
	if !ok || txRaw == nil {
		return nil, false, nil
	}
	tx, ok := txRaw.(graph.Tx)
	if !ok || tx == nil {
		return nil, false, nil
	}
	ctxRaw, ok := params[projectionEvalCtxParam]
	if !ok || ctxRaw == nil {
		return nil, false, nil
	}
	ctx, ok := ctxRaw.(context.Context)
	if !ok || ctx == nil {
		return nil, false, nil
	}
	return exec.evalProjectionPatternComprehension(ctx, tx, raw, row, params)
}

func evalFieldAccessValue(base any, field string) (any, error) {
	field = normalizeFieldAccessPart(field)
	switch typed := base.(type) {
	case *graph.Vertex:
		return evalVertexField(typed, field)
	case *graph.Edge:
		return evalEdgeField(typed, field)
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		if value, ok := evalTemporalAccessor(typed, field); ok {
			return value, nil
		}
		if value, ok := typed[field]; ok {
			return value, nil
		}
		return nil, nil
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			if value, ok := evalTemporalAccessor(mapped, field); ok {
				return value, nil
			}
			if value, ok := mapped[field]; ok {
				return value, nil
			}
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentTypePropertyAccess", nil)
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentTypePropertyAccess", nil)
	}
}

func splitTopLevelFieldAccess(raw string) (string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}

	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	firstDot := -1
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
		case '.':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				firstDot = i
				i = len(raw)
			}
		}
	}
	if firstDot <= 0 {
		return "", nil, false
	}

	baseExpr := strings.TrimSpace(raw[:firstDot])
	tail := raw[firstDot+1:]
	if baseExpr == "" || strings.TrimSpace(tail) == "" {
		return "", nil, false
	}

	readIdentifier := func(text string, idx int) (string, int, bool) {
		if idx >= len(text) || !isIdentifierStart(text[idx]) {
			return "", idx, false
		}
		start := idx
		idx++
		for idx < len(text) && isIdentifierPart(text[idx]) {
			idx++
		}
		return text[start:idx], idx, true
	}

	readDelimited := func(text string, start int) (string, int, bool) {
		if start >= len(text) || text[start] != '`' {
			return "", start, false
		}
		var b strings.Builder
		for i := start + 1; i < len(text); i++ {
			if text[i] != '`' {
				b.WriteByte(text[i])
				continue
			}
			if i+1 < len(text) && text[i+1] == '`' {
				b.WriteByte('`')
				i++
				continue
			}
			return b.String(), i + 1, true
		}
		return "", start, false
	}

	parts := make([]string, 0, 3)
	idx := 0
	for {
		idx = skipInlineSpaces(tail, idx)
		if idx >= len(tail) || !isIdentifierStart(tail[idx]) {
			if idx >= len(tail) {
				break
			}
			if tail[idx] == '`' {
				field, next, ok := readDelimited(tail, idx)
				if !ok {
					return "", nil, false
				}
				parts = append(parts, field)
				idx = skipInlineSpaces(tail, next)
				if idx >= len(tail) {
					break
				}
				if tail[idx] != '.' {
					return "", nil, false
				}
				idx++
				continue
			}
			return "", nil, false
		}

		field, next, ok := readIdentifier(tail, idx)
		if !ok {
			return "", nil, false
		}
		parts = append(parts, field)
		idx = skipInlineSpaces(tail, next)
		if idx >= len(tail) {
			break
		}
		if tail[idx] != '.' {
			return "", nil, false
		}
		idx++
	}

	if len(parts) == 0 {
		return "", nil, false
	}
	return baseExpr, parts, true
}

func skipInlineSpaces(raw string, idx int) int {
	for idx < len(raw) {
		switch raw[idx] {
		case ' ', '\t', '\n', '\r':
			idx++
		default:
			return idx
		}
	}
	return idx
}

func normalizeFieldAccessPart(part string) string {
	part = strings.TrimSpace(part)
	if len(part) >= 2 && part[0] == '`' && part[len(part)-1] == '`' {
		return strings.ReplaceAll(part[1:len(part)-1], "``", "`")
	}
	return part
}

func resolveBareIdentifier(raw string, row Row, params Params) (any, bool) {
	if !isIdentifierLike(raw) {
		return nil, false
	}
	if row != nil {
		if value, ok := row[raw]; ok {
			return value, true
		}
	}
	if params != nil {
		if value, ok := params[raw]; ok {
			return value, true
		}
	}
	return nil, false
}

func evalCaseExpression(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, false, nil
	}
	body := strings.TrimSpace(raw[len("CASE") : len(raw)-len("END")])
	if body == "" {
		return nil, false, nil
	}
	comparisonExpr := ""
	remaining := body
	if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
		whenIdx := findTopLevelKeywordIndex(remaining, "WHEN")
		if whenIdx <= 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is missing WHEN", nil)
		}
		comparisonExpr = strings.TrimSpace(remaining[:whenIdx])
		remaining = strings.TrimSpace(remaining[whenIdx:])
	}

	testValue := any(nil)
	if comparisonExpr != "" {
		value, err := evalExpressionWithScope(comparisonExpr, row, params)
		if err != nil {
			return nil, true, err
		}
		testValue = value
	}

	for {
		if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
			break
		}
		remaining = strings.TrimSpace(remaining[len("WHEN"):])
		thenIdx := findTopLevelKeywordIndex(remaining, "THEN")
		if thenIdx < 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is missing THEN", nil)
		}
		whenExpr := strings.TrimSpace(remaining[:thenIdx])
		afterThen := strings.TrimSpace(remaining[thenIdx+len("THEN"):])
		if whenExpr == "" || afterThen == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is malformed", nil)
		}

		nextWhenIdx := findTopLevelKeywordIndex(afterThen, "WHEN")
		elseIdx := findTopLevelKeywordIndex(afterThen, "ELSE")
		resultExpr := afterThen
		remaining = ""
		if nextWhenIdx >= 0 && (elseIdx < 0 || nextWhenIdx < elseIdx) {
			resultExpr = strings.TrimSpace(afterThen[:nextWhenIdx])
			remaining = strings.TrimSpace(afterThen[nextWhenIdx:])
		} else if elseIdx >= 0 {
			resultExpr = strings.TrimSpace(afterThen[:elseIdx])
			remaining = strings.TrimSpace(afterThen[elseIdx:])
		}

		matched := false
		if comparisonExpr == "" {
			conditionValue, err := evalExpressionWithScope(whenExpr, row, params)
			if err != nil {
				return nil, true, err
			}
			condition, ok := conditionValue.(bool)
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE condition must evaluate to a boolean", nil)
			}
			matched = condition
		} else {
			whenValue, err := evalExpressionWithScope(whenExpr, row, params)
			if err != nil {
				return nil, true, err
			}
			matched = simpleCaseValuesMatch(testValue, whenValue)
		}

		if matched {
			value, err := evalExpressionWithScope(resultExpr, row, params)
			return value, true, err
		}
	}

	if strings.HasPrefix(strings.ToUpper(remaining), "ELSE") {
		elseExpr := strings.TrimSpace(remaining[len("ELSE"):])
		if elseExpr == "" {
			return nil, true, nil
		}
		value, err := evalExpressionWithScope(elseExpr, row, params)
		return value, true, err
	}
	return nil, true, nil
}

func simpleCaseValuesMatch(lhs, rhs any) bool {
	if lhs == nil || rhs == nil {
		return false
	}
	if ls, ok := lhs.(string); ok {
		rs, ok := rhs.(string)
		return ok && ls == rs
	}
	if _, ok := rhs.(string); ok {
		return false
	}
	if lb, ok := lhs.(bool); ok {
		rb, ok := rhs.(bool)
		return ok && lb == rb
	}
	if _, ok := rhs.(bool); ok {
		return false
	}
	if isStrictNumericType(lhs) && isStrictNumericType(rhs) {
		lf, _ := numericValue(lhs)
		rf, _ := numericValue(rhs)
		return lf == rf
	}
	equal, isNull := cypherNullableEqual(lhs, rhs)
	return equal && !isNull
}

func isStrictNumericType(v any) bool {
	switch v.(type) {
	case int, int64, float32, float64, json.Number:
		return true
	default:
		return false
	}
}

func splitTopLevelNullPredicate(raw string) (string, bool, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, false
	}
	if strings.ContainsAny(raw, " \t\n\r") {
		if left, right, ok := splitTopLevelKeyword(raw, "IS"); ok {
			rightUpper := strings.ToUpper(strings.TrimSpace(right))
			if rightUpper == "NULL" {
				return left, true, true
			}
			if rightUpper == "NOT NULL" {
				return left, false, true
			}
		}
	}
	if left, ok := splitTopLevelSuffixKeyword(raw, "ISNOTNULL"); ok {
		return left, false, true
	}
	if left, ok := splitTopLevelSuffixKeyword(raw, "ISNULL"); ok {
		return left, true, true
	}
	return "", false, false
}

func splitTopLevelInExpression(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len("IN"); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || !strings.HasPrefix(upper[i:], "IN") {
			continue
		}
		if raw[i:i+2] != "IN" {
			continue
		}
		if i >= len("CONTA") && i+2 < len(raw) {
			if strings.EqualFold(raw[i-len("CONTA"):i], "CONTA") {
				next := raw[i+2]
				if next == 's' || next == 'S' {
					continue
				}
			}
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+2:])
		if left == "" || right == "" {
			continue
		}
		beforeWhitespace := i > 0 && strings.ContainsAny(string(raw[i-1]), " \t\n\r")
		afterIdx := i + 2
		afterWhitespace := afterIdx < len(raw) && strings.ContainsAny(string(raw[afterIdx]), " \t\n\r")
		if beforeWhitespace || afterWhitespace {
			return left, right, true
		}
		if !strings.ContainsAny(raw, " \t\n\r") {
			if (len(left) == 1 && len(right) == 1) || strings.HasPrefix(left, "$") || strings.HasPrefix(right, "$") || strings.HasPrefix(left, "[") || strings.HasPrefix(right, "[") || strings.HasPrefix(left, "'") || strings.HasPrefix(left, `"`) || (strings.HasPrefix(left, "(") && strings.HasSuffix(left, ")")) || strings.HasPrefix(right, "(") || isSimpleNumericToken(left) {
				return left, right, true
			}
		}
	}
	return "", "", false
}

func splitTopLevelCompactKeyword(raw, keyword string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left != "" && right != "" {
			return left, right, true
		}
	}
	return "", "", false
}

func evalStringPredicateExpression(leftExpr, rightExpr, op string, row Row, params Params) (any, error) {
	left, err := evalExpressionWithScope(leftExpr, row, params)
	if err != nil {
		return nil, err
	}
	right, err := evalExpressionWithScope(rightExpr, row, params)
	if err != nil {
		return nil, err
	}
	if left == nil || right == nil {
		return nil, nil
	}
	ls, ok := left.(string)
	if !ok {
		return nil, nil
	}
	rs, ok := right.(string)
	if !ok {
		return nil, nil
	}
	switch op {
	case "STARTS WITH":
		return strings.HasPrefix(ls, rs), nil
	case "ENDS WITH":
		return strings.HasSuffix(ls, rs), nil
	default:
		return strings.Contains(ls, rs), nil
	}
}

func splitTopLevelSliceBounds(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw)-1; i++ {
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
		}
		if depth == 0 && raw[i] == '.' && raw[i+1] == '.' {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+2:])
			return left, right, true
		}
	}
	return "", "", false
}

func evalSliceBound(expr string, row Row, params Params) (int, bool, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return 0, false, false, nil
	}
	value, err := evalExpressionWithScope(expr, row, params)
	if err != nil {
		value, err = evalWriteValue(expr, params, row)
	}
	if err != nil {
		return 0, false, false, err
	}
	if value == nil {
		return 0, false, true, nil
	}
	bound, err := toInt(value)
	if err != nil {
		return 0, false, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
	}
	return bound, true, false, nil
}

func applySliceAny(values []any, start, end int, hasStart, hasEnd bool) []any {
	length := len(values)
	startIdx := 0
	endIdx := length
	if hasStart {
		startIdx = start
		if startIdx < 0 {
			startIdx = length + startIdx
		}
	}
	if hasEnd {
		endIdx = end
		if endIdx < 0 {
			endIdx = length + endIdx
		}
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx < 0 {
		endIdx = 0
	}
	if startIdx > length {
		startIdx = length
	}
	if endIdx > length {
		endIdx = length
	}
	if endIdx < startIdx {
		return []any{}
	}
	return append([]any(nil), values[startIdx:endIdx]...)
}

func applySliceStringList(values []string, start, end int, hasStart, hasEnd bool) []any {
	anyValues := make([]any, 0, len(values))
	for _, value := range values {
		anyValues = append(anyValues, value)
	}
	return applySliceAny(anyValues, start, end, hasStart, hasEnd)
}

func applySliceString(value string, start, end int, hasStart, hasEnd bool) string {
	runes := []rune(value)
	length := len(runes)
	startIdx := 0
	endIdx := length
	if hasStart {
		startIdx = start
		if startIdx < 0 {
			startIdx = length + startIdx
		}
	}
	if hasEnd {
		endIdx = end
		if endIdx < 0 {
			endIdx = length + endIdx
		}
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx < 0 {
		endIdx = 0
	}
	if startIdx > length {
		startIdx = length
	}
	if endIdx > length {
		endIdx = length
	}
	if endIdx < startIdx {
		return ""
	}
	return string(runes[startIdx:endIdx])
}

func isSimpleNumericToken(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if _, err := strconv.Atoi(raw); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return true
	}
	return false
}

func evalListPredicateFunction(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	for _, name := range []string{"all", "any", "none", "single"} {
		arg, ok := parseFunctionCall(raw, name)
		if !ok {
			continue
		}
		body := strings.TrimSpace(arg)
		if body == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires arguments", nil)
		}
		whereIdx := findTopLevelKeywordIndex(body, "WHERE")
		if whereIdx < 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires WHERE", nil)
		}
		head := strings.TrimSpace(body[:whereIdx])
		predicateExpr := strings.TrimSpace(body[whereIdx+len("WHERE"):])
		if head == "" || predicateExpr == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires a list and a predicate", nil)
		}
		varName, listExpr, ok := splitTopLevelListPredicateHeader(head)
		if !ok {
			return nil, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
		}
		if !isIdentifierLike(varName) {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list predicate variable must be an identifier", nil)
		}
		listValue, err := evalExpressionWithScope(listExpr, row, params)
		if err != nil {
			return nil, true, err
		}
		values, ok := normalizeListValue(listValue)
		if !ok {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list predicate requires a list source", nil)
		}
		anyNull := false
		anyTrue := false
		anyFalse := false
		trueCount := 0
		for _, value := range values {
			scope := cloneRow(row)
			scope[varName] = value
			predValue, err := evalExpressionWithScope(predicateExpr, scope, params)
			if err != nil {
				return nil, true, err
			}
			if predValue == nil {
				anyNull = true
				continue
			}
			boolValue := truthyWhereValue(predValue)
			if boolValue {
				anyTrue = true
				trueCount++
			} else {
				anyFalse = true
			}
		}
		switch name {
		case "all":
			if anyFalse {
				return false, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return true, true, nil
		case "any":
			if anyTrue {
				return true, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return false, true, nil
		case "none":
			if anyTrue {
				return false, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return true, true, nil
		case "single":
			if trueCount > 1 {
				return false, true, nil
			}
			if trueCount == 1 {
				if anyNull {
					return nil, true, nil
				}
				return true, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return false, true, nil
		}
	}
	return nil, false, nil
}

func splitTopLevelListPredicateHeader(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		return left, right, true
	}
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len("IN"); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(upper[i:], "IN") {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+2:])
			if left != "" && right != "" {
				return left, right, true
			}
		}
	}
	return "", "", false
}

func splitTopLevelCompressedBoolean(raw, keyword string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, " \t\n\r") {
		return "", "", false
	}
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		if keyword == "OR" && i > 0 && strings.EqualFold(raw[i-1:i+len(keyword)], "XOR") {
			continue
		}
		if raw[i:i+len(keyword)] != keyword {
			continue
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left == "" || right == "" {
			continue
		}
		return left, right, true
	}
	return "", "", false
}

func compressedKeywordHasBoundaries(raw string, idx, kwLen int) bool {
	if idx < 0 || kwLen <= 0 || idx+kwLen > len(raw) {
		return false
	}
	beforeIsIdent := idx > 0 && isIdentifierByte(raw[idx-1])
	afterPos := idx + kwLen
	afterIsIdent := afterPos < len(raw) && isIdentifierByte(raw[afterPos])
	return !beforeIsIdent && !afterIsIdent
}

func isIdentifierByte(ch byte) bool {
	if ch == '_' {
		return true
	}
	if ch >= 'a' && ch <= 'z' {
		return true
	}
	if ch >= 'A' && ch <= 'Z' {
		return true
	}
	return ch >= '0' && ch <= '9'
}

func isCompressedBooleanOperandShape(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	if strings.ContainsAny(left, " \t\n\r()[]{}.,:+-*/%<>=") || strings.ContainsAny(right, " \t\n\r()[]{}.,:+-*/%<>=") {
		return false
	}
	if len(left) == 1 && len(right) == 1 {
		return true
	}
	if len(left) == 1 && containsCompressedBooleanKeyword(right) {
		return true
	}
	if len(right) == 1 && containsCompressedBooleanKeyword(left) {
		return true
	}
	return false
}

func containsCompressedBooleanKeyword(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	upper := strings.ToUpper(raw)
	for _, keyword := range []string{"AND", "OR", "XOR"} {
		if strings.Contains(upper, keyword) {
			return true
		}
	}
	return false
}

func splitTopLevelSuffixKeyword(raw, suffix string) (string, bool) {
	raw = strings.TrimSpace(raw)
	suffix = strings.ToUpper(strings.TrimSpace(suffix))
	if raw == "" || suffix == "" || len(raw) <= len(suffix) {
		return "", false
	}
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(suffix); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && i+len(suffix) == len(upper) && strings.HasPrefix(upper[i:], suffix) {
			left := strings.TrimSpace(raw[:i])
			if left != "" {
				return left, true
			}
		}
	}
	return "", false
}

func evalInExpression(lhs, rhs any) (any, error) {
	values, ok := normalizeListValue(rhs)
	if !ok {
		if rhs == nil {
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindSemantic, "IN requires a list on the right-hand side", nil)
	}
	if lhs == nil {
		if len(values) == 0 {
			return false, nil
		}
		return nil, nil
	}
	matchedNull := false
	for _, candidate := range values {
		equal, isNull := cypherNullableEqualForIn(lhs, candidate)
		if isNull {
			matchedNull = true
			continue
		}
		if equal {
			return true, nil
		}
	}
	if matchedNull {
		return nil, nil
	}
	return false, nil
}

func cypherNullableEqualForIn(lhs, rhs any) (equal bool, isNull bool) {
	if lhs == nil || rhs == nil {
		return false, true
	}

	if lf, lok := lhs.(float64); lok && math.IsNaN(lf) {
		_, rok := rhs.(float64)
		if rok {
			return false, false
		}
	}

	if isStrictNumericType(lhs) && isStrictNumericType(rhs) {
		lf, _ := numericValue(lhs)
		rf, _ := numericValue(rhs)
		return lf == rf, false
	}

	if (isStrictNumericType(lhs) && isStringType(rhs)) || (isStrictNumericType(rhs) && isStringType(lhs)) {
		return false, false
	}

	if lb, lok := lhs.(bool); lok {
		rb, rok := rhs.(bool)
		if !rok {
			return false, false
		}
		return lb == rb, false
	}

	if ls, lok := lhs.(string); lok {
		rs, rok := rhs.(string)
		if !rok {
			return false, false
		}
		return ls == rs, false
	}

	if ll, lok := asAnySlice(lhs); lok {
		rl, rok := asAnySlice(rhs)
		if !rok {
			return false, false
		}
		if len(ll) != len(rl) {
			return false, false
		}
		unknown := false
		for i := range ll {
			eq, isNull := cypherNullableEqualForIn(ll[i], rl[i])
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	if lm, lok := lhs.(map[string]any); lok {
		rm, rok := rhs.(map[string]any)
		if !rok {
			return false, false
		}
		if len(lm) != len(rm) {
			return false, false
		}
		unknown := false
		for key, lv := range lm {
			rv, ok := rm[key]
			if !ok {
				return false, false
			}
			eq, isNull := cypherNullableEqualForIn(lv, rv)
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	return reflect.DeepEqual(lhs, rhs), false
}

func isStringType(value any) bool {
	_, ok := value.(string)
	return ok
}

func normalizeListValue(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case string:
		if parsed, ok := parseStoredListString(typed); ok {
			return parsed, true
		}
	}
	return nil, false
}

func isFloatLikeNumeric(v any) bool {
	switch typed := v.(type) {
	case float64, float32:
		return true
	case json.Number:
		s := strings.TrimSpace(typed.String())
		return strings.ContainsAny(s, ".eE")
	case string:
		s := strings.TrimSpace(typed)
		return strings.ContainsAny(s, ".eE")
	default:
		return false
	}
}

func splitTrailingSubscript(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasSuffix(raw, "]") {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := len(raw) - 1; i >= 0; i-- {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || raw[i-1] != '\\') && !inSingle {
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
			depthBracket--
			if depthBracket == 0 {
				base := strings.TrimSpace(raw[:i])
				index := strings.TrimSpace(raw[i+1 : len(raw)-1])
				if base == "" || index == "" {
					return "", "", false
				}
				return base, index, true
			}
		case ')':
			depthParen++
		case '(':
			depthParen--
		case '}':
			depthBrace++
		case '{':
			depthBrace--
		}
		if depthParen < 0 || depthBracket < 0 || depthBrace < 0 {
			return "", "", false
		}
	}
	return "", "", false
}

func evalListComprehension(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, false, nil
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	pipeIdx := findTopLevelPipeIndex(body)
	projectionExpr := ""
	if pipeIdx >= 0 {
		projectionExpr = strings.TrimSpace(body[pipeIdx+1:])
		body = strings.TrimSpace(body[:pipeIdx])
	}
	upper := strings.ToUpper(body)
	inIdx := strings.Index(upper, "IN")
	if inIdx <= 0 {
		return nil, false, nil
	}
	varName := strings.TrimSpace(body[:inIdx])
	if !isIdentifierLike(varName) {
		return nil, false, nil
	}
	rest := strings.TrimSpace(body[inIdx+2:])
	if rest == "" {
		return nil, true, graph.NewError(graph.ErrKindSemantic, "list comprehension source is required", nil)
	}

	whereIdx := findTopLevelKeywordIndex(rest, "WHERE")
	listExpr := rest
	predicate := ""
	if whereIdx >= 0 {
		listExpr = strings.TrimSpace(rest[:whereIdx])
		predicate = strings.TrimSpace(rest[whereIdx+len("WHERE"):])
	}

	listValue, err := evalExpressionWithScope(listExpr, row, params)
	if err != nil {
		return nil, true, err
	}
	values, ok := listValue.([]any)
	if !ok {
		if typed, ok := listValue.([]string); ok {
			values = make([]any, 0, len(typed))
			for _, v := range typed {
				values = append(values, v)
			}
		} else {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list comprehension requires a list source", nil)
		}
	}

	out := make([]any, 0, len(values))
	for _, v := range values {
		scope := cloneRow(row)
		scope[varName] = v
		include := true
		if predicate != "" {
			predValue, err := evalExpressionWithScope(predicate, scope, params)
			if err != nil {
				return nil, true, err
			}
			if predValue == nil {
				include = false
			} else {
				include = truthyWhereValue(predValue)
			}
		}
		if include {
			if projectionExpr == "" {
				out = append(out, v)
				continue
			}
			projected, err := evalExpressionWithScope(projectionExpr, scope, params)
			if err != nil {
				return nil, true, err
			}
			out = append(out, projected)
		}
	}

	return out, true, nil
}

func evalLabelsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "labels() requires one argument", nil)
	}
	base, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		base, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, nil
	}
	if _, _, ok := pathComponents(base); ok {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
	}
	switch typed := base.(type) {
	case *graph.Vertex:
		return append([]string(nil), typed.Labels...), nil
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		if labels, ok := typed["labels"]; ok {
			switch l := labels.(type) {
			case []string:
				return append([]string(nil), l...), nil
			case []any:
				out := make([]string, 0, len(l))
				for _, item := range l {
					out = append(out, fmt.Sprint(item))
				}
				return out, nil
			}
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalTypeFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "type() requires one argument", nil)
	}
	base, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		base, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, nil
	}
	switch typed := base.(type) {
	case *graph.Edge:
		return typed.Type, nil
	case *graph.Vertex:
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
	case deletedEdgeBinding:
		return typed.Type, nil
	case map[string]any:
		if _, hasLabels := typed["labels"]; hasLabels {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
		}
		if relType, ok := typed["type"]; ok {
			return fmt.Sprint(relType), nil
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalPropertiesFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "properties() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	switch typed := value.(type) {
	case *graph.Vertex:
		return clonePropertyMap(typed.Properties), nil
	case *graph.Edge:
		return clonePropertyMap(typed.Properties), nil
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out, nil
	default:
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
}

func evalKeysFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "keys() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	props := map[string]any{}
	switch typed := value.(type) {
	case *graph.Vertex:
		for key, raw := range typed.Properties {
			if isStoredNullProperty(raw) {
				continue
			}
			props[key] = true
		}
	case *graph.Edge:
		for key, raw := range typed.Properties {
			if isStoredNullProperty(raw) {
				continue
			}
			props[key] = true
		}
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		props = typed
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			props = mapped
		} else {
			return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
		}
	default:
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
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
	return out, nil
}

func isStoredNullProperty(raw []byte) bool {
	text := strings.TrimSpace(string(raw))
	if strings.EqualFold(text, "null") {
		return true
	}
	return text == "<nil>"
}

func evalHeadFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "head() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

func evalTailFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "tail() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) <= 1 {
		return []any{}, nil
	}

	out := make([]any, 0, len(list)-1)
	out = append(out, list[1:]...)
	return out, nil
}

func evalAbsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "abs() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	if n, err := toInt(value); err == nil {
		if n == math.MinInt {
			return json.Number(formatFloatResult(math.Abs(float64(n)))), nil
		}
		if n < 0 {
			return -n, nil
		}
		return n, nil
	}

	f, ok := numericValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return json.Number(formatFloatResult(math.Abs(f))), nil
}

func evalNodesFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "nodes() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	nodes, _, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	out := make([]any, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node)
	}
	return out, nil
}

func evalRelationshipsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "relationships() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	_, edges, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	out := make([]any, 0, len(edges))
	for _, edge := range edges {
		out = append(out, edge)
	}
	return out, nil
}

func evalLengthFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "length() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	_, edges, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return len(edges), nil
}

func evalLastFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "last() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[len(list)-1], nil
}

func evalSignFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "sign() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	n, ok := numericValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if n > 0 {
		return 1, nil
	}
	if n < 0 {
		return -1, nil
	}
	return 0, nil
}

func evalStartNodeFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "startNode() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	edge, ok := edgeValueFromAny(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if node, ok := findBoundVertexByID(row, edge.SrcID); ok {
		return node, nil
	}
	return map[string]any{"id": edge.SrcID}, nil
}

func evalEndNodeFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "endNode() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	edge, ok := edgeValueFromAny(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if node, ok := findBoundVertexByID(row, edge.DstID); ok {
		return node, nil
	}
	return map[string]any{"id": edge.DstID}, nil
}

func pathComponents(value any) ([]*graph.Vertex, []*graph.Edge, bool) {
	switch typed := value.(type) {
	case cypherPathValue:
		nodes := make([]*graph.Vertex, 0, 2)
		edges := make([]*graph.Edge, 0, 1)
		if typed.Left != nil {
			nodes = append(nodes, typed.Left)
		}
		if typed.Edge != nil {
			edges = append(edges, typed.Edge)
		}
		if typed.Right != nil {
			nodes = append(nodes, typed.Right)
		}
		return nodes, edges, true
	case multiHopCypherPath:
		nodes := append([]*graph.Vertex(nil), typed.Nodes...)
		edges := append([]*graph.Edge(nil), typed.Edges...)
		return nodes, edges, true
	default:
		return nil, nil, false
	}
}

func isNullPathValue(value any) bool {
	switch typed := value.(type) {
	case cypherPathValue:
		return typed.Left == nil && typed.Edge == nil && typed.Right == nil
	case multiHopCypherPath:
		return len(typed.Nodes) == 0 && len(typed.Edges) == 0
	default:
		return false
	}
}

func edgeValueFromAny(value any) (*graph.Edge, bool) {
	switch typed := value.(type) {
	case *graph.Edge:
		return typed, true
	case map[string]any:
		src, sok := typed["src"]
		dst, dok := typed["dst"]
		if !sok || !dok {
			return nil, false
		}
		return &graph.Edge{SrcID: fmt.Sprint(src), DstID: fmt.Sprint(dst)}, true
	default:
		return nil, false
	}
}

func findBoundVertexByID(row Row, vertexID string) (*graph.Vertex, bool) {
	if row == nil || vertexID == "" {
		return nil, false
	}
	for _, value := range row {
		vertex, ok := value.(*graph.Vertex)
		if !ok || vertex == nil {
			continue
		}
		if vertex.ID == vertexID {
			return vertex, true
		}
	}
	return nil, false
}

func evalToBooleanValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if b, ok := value.(bool); ok {
		return b, nil
	}
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		s = strings.ToLower(s)
		switch s {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, nil
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalToStringValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if temporal, ok := temporalMapValue(value); ok {
		if rendered, ok := formatTemporalToString(temporal); ok {
			return rendered, nil
		}
	}
	if isInvalidTypeConversionValue(value) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	return fmt.Sprint(normalizeResultValue(value)), nil
}

func evalToIntegerValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if isInvalidTypeConversionValue(value) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	normalized := normalizeResultValue(value)
	switch typed := normalized.(type) {
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return nil, nil
		}
		if f, ok := numericValue(s); ok {
			return int(truncTowardZero(f)), nil
		}
		return nil, nil
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return nil, nil
		}
		return int(truncTowardZero(f)), nil
	default:
		if f, ok := numericValue(normalized); ok {
			return int(truncTowardZero(f)), nil
		}
		return nil, nil
	}
}

func evalToFloatValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case bool, []any, []string, map[string]any, *graph.Vertex, *graph.Edge, deletedVertexBinding, deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, nil
		}
		return json.Number(formatFloatResult(f)), nil
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return nil, nil
		}
		return json.Number(formatFloatResult(f)), nil
	case int:
		return json.Number(formatFloatResult(float64(typed))), nil
	case int64:
		return json.Number(formatFloatResult(float64(typed))), nil
	case int32:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint64:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint32:
		return json.Number(formatFloatResult(float64(typed))), nil
	case float64:
		return json.Number(formatFloatResult(typed)), nil
	case float32:
		return json.Number(formatFloatResult(float64(typed))), nil
	default:
		if f, ok := numericValue(value); ok {
			return json.Number(formatFloatResult(f)), nil
		}
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func clonePropertyMap(props graph.PropertyMap) map[string]any {
	if len(props) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(props))
	for key, raw := range props {
		out[key] = decodeStoredPropertyValue(raw)
	}
	return out
}

func (e *Executor) evalWhereExpression(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, graph.NewError(graph.ErrKindSemantic, "empty WHERE expression", nil)
	}
	if body, ok := parseExistsSubqueryBody(raw); ok {
		return e.evalExistsSubquery(ctx, tx, body, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "OR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if lhs {
			return true, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "OR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if lhs {
			return true, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "AND"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if !lhs {
			return false, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if !lhs {
			return false, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "XOR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := e.evalWhereExpression(ctx, tx, right, row, params)
		if err != nil {
			return false, err
		}
		return lhs != rhs, nil
	}
	if left, right, ok := splitTopLevelKeyword(raw, "XOR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := e.evalWhereExpression(ctx, tx, right, row, params)
		if err != nil {
			return false, err
		}
		return lhs != rhs, nil
	}

	if hasLogicalNotPrefix(raw) {
		operand := strings.TrimSpace(raw[3:])
		if matched, handled, err := e.evalWhereRelationshipPatternPredicate(ctx, tx, operand, row, params); handled {
			if err != nil {
				return false, err
			}
			return !matched, nil
		}
		value, err := evalExpressionWithScope(operand, row, params)
		if err != nil {
			return false, err
		}
		b, isNull, err := asNullableBoolean(value)
		if err != nil {
			return false, err
		}
		if isNull {
			return false, nil
		}
		return !b, nil
	}

	if matched, handled, err := e.evalWhereRelationshipPatternPredicate(ctx, tx, raw, row, params); handled {
		if err != nil {
			return false, err
		}
		return matched, nil
	}

	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return e.evalWhereExpression(ctx, tx, raw[1:len(raw)-1], row, params)
	}
	if operands, operators, ok := splitTopLevelComparisonChain(raw); ok {
		var sawNull bool
		for i := 0; i < len(operators); i++ {
			lhs, err := evalExpressionWithScope(operands[i], row, params)
			if err != nil {
				return false, err
			}
			rhs, err := evalExpressionWithScope(operands[i+1], row, params)
			if err != nil {
				return false, err
			}
			result, err := compareExpressionValues(lhs, rhs, operators[i])
			if err != nil {
				return false, err
			}
			if result == nil {
				sawNull = true
				continue
			}
			truth, ok := result.(bool)
			if !ok {
				return false, nil
			}
			if !truth {
				return false, nil
			}
		}
		if sawNull {
			return false, nil
		}
		return true, nil
	}

	if left, right, op, ok := splitTopLevelComparison(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return false, err
		}
		return compareWhereValues(lhs, rhs, op)
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return false, err
		}
		value, err := evalInExpression(lhs, rhs)
		if err != nil {
			return false, err
		}
		return truthyWhereValue(value), nil
	}
	if left, isNull, ok := splitTopLevelNullPredicate(raw); ok {
		value, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		if isNull {
			return value == nil, nil
		}
		return value != nil, nil
	}

	value, err := evalExpressionWithScope(raw, row, params)
	if err != nil {
		return false, err
	}
	return truthyWhereValue(value), nil
}

func parseExistsSubqueryBody(raw string) (string, bool) {
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
	if !bracesAreBalanced(rest[1 : len(rest)-1]) {
		return "", false
	}
	body := strings.TrimSpace(rest[1 : len(rest)-1])
	if body == "" {
		return "", false
	}
	return body, true
}

func bracesAreBalanced(raw string) bool {
	depth := 0
	for _, r := range raw {
		switch r {
		case '{':
			depth++
		case '}':
			if depth == 0 {
				return false
			}
			depth--
		}
	}
	return depth == 0
}

func (e *Executor) evalExistsSubquery(ctx context.Context, tx graph.Tx, body string, row Row, params Params) (bool, error) {
	if tx == nil {
		return false, graph.NewError(graph.ErrKindUnsupported, "EXISTS subquery requires transactional context", nil)
	}
	body = strings.TrimSpace(body)
	if result, ok, err := e.evalExistsQueryBody(ctx, tx, body, row, params); ok {
		return result, err
	}
	if patternBody, whereBody, ok := splitExistsPatternBody(body); ok {
		matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternBody}, params)
		if err != nil {
			return false, err
		}
		if whereBody == "" {
			return len(matches) > 0, nil
		}
		for _, matched := range matches {
			ok, err := e.evalWhereExpression(ctx, tx, whereBody, matched, params)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	if !strings.HasPrefix(strings.ToUpper(body), "MATCH") && !strings.HasPrefix(strings.ToUpper(body), "OPTIONALMATCH") {
		return false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}

	rows, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: body}, params)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *Executor) evalExistsQueryBody(ctx context.Context, tx graph.Tx, body string, row Row, params Params) (bool, bool, error) {
	body = normalizeClauseBody(stripCypherLineComments(body))
	upper := strings.ToUpper(body)
	matchKeyword := ""
	if strings.HasPrefix(upper, "OPTIONALMATCH") {
		matchKeyword = "OPTIONALMATCH"
	} else if strings.HasPrefix(upper, "MATCH") {
		matchKeyword = "MATCH"
	} else {
		return false, false, nil
	}
	rest := strings.TrimSpace(body[len(matchKeyword):])
	nextClauseIdx := minPositiveIndex(
		findTopLevelKeywordIndex(rest, "WITH"),
		findTopLevelKeywordIndex(rest, "RETURN"),
	)
	matchExpr := rest
	remaining := ""
	if nextClauseIdx >= 0 {
		matchExpr = strings.TrimSpace(rest[:nextClauseIdx])
		remaining = strings.TrimSpace(rest[nextClauseIdx:])
	}
	if matchExpr == "" {
		return false, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}
	matchRaw := "MATCH " + matchExpr
	matchKind := ast.ClauseKindMatch
	if matchKeyword == "OPTIONALMATCH" {
		matchRaw = "OPTIONAL MATCH " + matchExpr
		matchKind = ast.ClauseKindOptionalMatch
	}
	rows := []Row{cloneRow(row)}
	resultColumns := []string{}
	rows, err := e.applyMatchClause(ctx, tx, rows, ast.Clause{Kind: matchKind, Raw: matchRaw}, params)
	if err != nil {
		return false, true, err
	}
	if remaining == "" {
		return len(rows) > 0, true, nil
	}
	upperRemaining := strings.ToUpper(remaining)
	if strings.HasPrefix(upperRemaining, "WITH") {
		returnIdx := findTopLevelKeywordIndex(remaining, "RETURN")
		withRaw := remaining
		next := ""
		if returnIdx >= 0 {
			withRaw = strings.TrimSpace(remaining[:returnIdx])
			next = strings.TrimSpace(remaining[returnIdx:])
		}
		var stepErr error
		rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, ast.Clause{Kind: ast.ClauseKindWith, Raw: withRaw}, params, resultColumns, false)
		if stepErr != nil {
			return false, true, stepErr
		}
		remaining = next
		upperRemaining = strings.ToUpper(remaining)
	}
	if strings.HasPrefix(upperRemaining, "RETURN") {
		var stepErr error
		rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, ast.Clause{Kind: ast.ClauseKindReturn, Raw: remaining}, params, resultColumns, true)
		if stepErr != nil {
			return false, true, stepErr
		}
		return len(rows) > 0, true, nil
	}
	if remaining != "" {
		return false, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}
	return len(rows) > 0, true, nil
}

func splitExistsPatternBody(body string) (patternRaw string, whereRaw string, ok bool) {
	body = strings.TrimSpace(body)
	if body == "" || !strings.HasPrefix(body, "(") {
		return "", "", false
	}
	if idx := findTopLevelExistsWhereIndex(body); idx >= 0 {
		patternRaw = strings.TrimSpace(body[:idx])
		whereRaw = strings.TrimSpace(body[idx+len("WHERE"):])
	} else {
		patternRaw = body
		whereRaw = ""
	}
	if patternRaw == "" {
		return "", "", false
	}
	return patternRaw, whereRaw, true
}

func findTopLevelExistsWhereIndex(raw string) int {
	upper := strings.ToUpper(raw)
	keyword := "WHERE"
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i <= len(raw)-len(keyword); i++ {
		ch := raw[i]
		if inSingle {
			if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' && (i == 0 || raw[i-1] != '\\') {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if strings.HasPrefix(upper[i:], keyword) {
			return i
		}
	}
	return -1
}

func (e *Executor) evalWhereRelationshipPatternPredicate(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (bool, bool, error) {
	if tx == nil {
		return false, false, nil
	}
	patternRaw := strings.TrimSpace(raw)
	if rewritten, ok := rewriteReverseVariableLengthPatternPredicate(patternRaw); ok {
		patternRaw = rewritten
	}
	if !isWhereRelationshipPatternPredicate(patternRaw) {
		return false, false, nil
	}

	matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternRaw}, params)
	if err != nil {
		return false, true, err
	}
	return len(matches) > 0, true, nil
}

func rewriteReverseVariableLengthPatternPredicate(raw string) (string, bool) {
	m := regexp.MustCompile(`^\(([^()]*)\)<-\[([^\]]*\*)\]-\(([^()]*)\)$`).FindStringSubmatch(raw)
	if len(m) != 4 {
		return "", false
	}
	left := strings.TrimSpace(m[1])
	edge := strings.TrimSpace(m[2])
	right := strings.TrimSpace(m[3])
	return "(" + right + ")-[" + edge + "]->(" + left + ")", true
}

func isWhereRelationshipPatternPredicate(raw string) bool {
	if raw == "" {
		return false
	}
	if _, err := parseDirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseReverseDirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseUndirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseDirectedVariableLengthRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseUndirectedVariableLengthRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseMixedRelationshipChainPattern(raw); err == nil {
		return true
	}
	if _, err := parseTwoHopDirectedChainPattern(raw); err == nil {
		return true
	}
	if _, err := parseTwoHopUndirectedRelationshipChainPattern(raw); err == nil {
		return true
	}
	return false
}

func splitTopLevelKeyword(raw, keyword string) (string, string, bool) {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(keyword)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		switch upper[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(upper[i:], keyword) {
			if keyword == "OR" && i > 0 && strings.EqualFold(raw[i-1:i+len(keyword)], "XOR") {
				continue
			}
			beforeIsWord := i > 0 && isAlphaOrUnderscore(raw[i-1])
			afterIdx := i + len(keyword)
			afterIsWord := afterIdx < len(raw) && isAlphaOrUnderscore(raw[afterIdx])
			if beforeIsWord || afterIsWord {
				if !shouldSplitCompressedKeyword(raw, i, len(keyword)) {
					continue
				}
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(keyword):])
			if left == "" || right == "" {
				continue
			}
			return raw[:i], raw[i+len(keyword):], true
		}
	}
	return raw, "", false
}

func hasLogicalNotPrefix(raw string) bool {
	return len(raw) >= 3 && strings.EqualFold(raw[:3], "NOT")
}

func shouldSplitCompressedKeyword(raw string, idx, kwLen int) bool {
	if idx <= 0 || idx+kwLen >= len(raw) {
		return false
	}
	left := raw[:idx]
	right := raw[idx+kwLen:]
	if left == "" || right == "" {
		return false
	}
	leftHasExprMarker := strings.ContainsAny(left, ".)]}")
	rightHasExprMarker := strings.ContainsAny(right, ".[({$")
	return leftHasExprMarker && rightHasExprMarker
}

func isAlphaOrUnderscore(ch byte) bool {
	if ch == '_' {
		return true
	}
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func splitTopLevelComparison(raw string) (string, string, string, bool) {
	op := []string{"<=", ">=", "<>", "=", "<", ">"}
	depth := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
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
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
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
		if depth == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depth != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		for _, candidate := range op {
			if strings.HasPrefix(raw[i:], candidate) {
				return strings.TrimSpace(raw[:i]), strings.TrimSpace(raw[i+len(candidate):]), candidate, true
			}
		}
	}
	return "", "", "", false
}

func splitTopLevelComparisonChain(raw string) ([]string, []string, bool) {
	operators := []string{"<=", ">=", "<>", "=", "<", ">"}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)

	parts := make([]string, 0, 4)
	ops := make([]string, 0, 3)
	start := 0

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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}

		for _, op := range operators {
			if strings.HasPrefix(raw[i:], op) {
				left := strings.TrimSpace(raw[start:i])
				if left == "" {
					return nil, nil, false
				}
				parts = append(parts, left)
				ops = append(ops, op)
				i += len(op) - 1
				start = i + 1
				break
			}
		}
	}

	if len(ops) < 2 {
		return nil, nil, false
	}
	last := strings.TrimSpace(raw[start:])
	if last == "" {
		return nil, nil, false
	}
	parts = append(parts, last)
	if len(parts) != len(ops)+1 {
		return nil, nil, false
	}
	return parts, ops, true
}

func splitTopLevelLabelPredicate(raw string) (string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
			if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+1:])
			if left == "" || right == "" {
				return "", nil, false
			}
			labels := splitLabels(right)
			if len(labels) == 0 {
				return "", nil, false
			}
			return left, labels, true
		}
	}
	return "", nil, false
}

func evalLabelPredicateExpression(left string, labels []string, row Row, params Params) (any, error) {
	value, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		for _, label := range labels {
			if !vertexHasLabel(typed, label) {
				return false, nil
			}
		}
		return true, nil
	case *graph.Edge:
		for _, label := range labels {
			if typed.Type != label {
				return false, nil
			}
		}
		return true, nil
	case map[string]any:
		if relType, ok := typed["type"]; ok {
			current := fmt.Sprint(relType)
			for _, label := range labels {
				if current != label {
					return false, nil
				}
			}
			return true, nil
		}
		labelValue, ok := typed["labels"]
		if !ok {
			return false, nil
		}
		labelSet := map[string]struct{}{}
		switch current := labelValue.(type) {
		case []string:
			for _, label := range current {
				labelSet[label] = struct{}{}
			}
		case []any:
			for _, rawLabel := range current {
				labelSet[fmt.Sprint(rawLabel)] = struct{}{}
			}
		default:
			return false, nil
		}
		for _, label := range labels {
			if _, ok := labelSet[label]; !ok {
				return false, nil
			}
		}
		return true, nil
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", left+":"+strings.Join(labels, ":")), nil)
	}
}

func compareWhereValues(lhs, rhs any, op string) (bool, error) {
	value, err := compareExpressionValues(lhs, rhs, op)
	if err != nil {
		return false, err
	}
	return truthyWhereValue(value), nil
}

func compareExpressionValues(lhs, rhs any, op string) (any, error) {
	op = strings.TrimSpace(op)
	if lhs == nil || rhs == nil {
		return nil, nil
	}

	switch op {
	case "=", "<>":
		equal, isNull := cypherNullableEqual(lhs, rhs)
		if isNull {
			return nil, nil
		}
		if op == "=" {
			return equal, nil
		}
		return !equal, nil
	case "<", "<=", ">", ">=":
		if ll, lok := asAnySlice(lhs); lok {
			if rl, rok := asAnySlice(rhs); rok {
				return compareOrderedLists(ll, rl, op), nil
			}
		}
		if lf, lok := comparableNumericValue(lhs); lok {
			if rf, rok := comparableNumericValue(rhs); rok {
				if math.IsNaN(lf) || math.IsNaN(rf) {
					return false, nil
				}
			}
		}
		cmp, ok := compareCypherValues(lhs, rhs)
		if !ok {
			return nil, nil
		}
		sameKind := cypherSortRank(lhs) == cypherSortRank(rhs)
		bothNumeric := isNumericType(lhs) && isNumericType(rhs)
		if !sameKind && !bothNumeric {
			return nil, nil
		}
		switch op {
		case "<":
			return cmp < 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">":
			return cmp > 0, nil
		case ">=":
			return cmp >= 0, nil
		}
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("WHERE operator %q is not supported", op), nil)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("WHERE operator %q is not supported", op), nil)
}

func compareOrderedLists(lhs, rhs []any, op string) any {
	limit := len(lhs)
	if len(rhs) < limit {
		limit = len(rhs)
	}
	for i := 0; i < limit; i++ {
		if lhs[i] == nil || rhs[i] == nil {
			if lhs[i] == nil && rhs[i] == nil {
				continue
			}
			return nil
		}
		cmp, ok := compareCypherValues(lhs[i], rhs[i])
		if !ok {
			return nil
		}
		if cmp != 0 {
			switch op {
			case "<":
				return cmp < 0
			case "<=":
				return cmp < 0
			case ">":
				return cmp > 0
			case ">=":
				return cmp > 0
			}
		}
	}

	cmp := 0
	switch {
	case len(lhs) < len(rhs):
		cmp = -1
	case len(lhs) > len(rhs):
		cmp = 1
	default:
		cmp = 0
	}

	switch op {
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return nil
	}
}

func compareExpressionValuesWithRaw(lhs, rhs any, op, leftRaw, rightRaw string) (any, error) {
	op = strings.TrimSpace(op)
	if lhs == nil && rhs == nil && (op == "=" || op == "<>") {
		if shouldTreatDoubleNullAsLogicalEquality(leftRaw, rightRaw) {
			if op == "=" {
				return true, nil
			}
			return false, nil
		}
	}
	return compareExpressionValues(lhs, rhs, op)
}

func shouldTreatDoubleNullAsLogicalEquality(leftRaw, rightRaw string) bool {
	left := strings.ToUpper(strings.TrimSpace(leftRaw))
	right := strings.ToUpper(strings.TrimSpace(rightRaw))
	if left == "NULL" && right == "NULL" {
		return false
	}
	return isCompositeTruthExpression(left) || isCompositeTruthExpression(right)
}

func isCompositeTruthExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for _, marker := range []string{" OR ", " AND ", " XOR ", "NOT ", " IS NULL", " IS NOT NULL", "ISNULL", "ISNOTNULL", " IN "} {
		if strings.Contains(raw, marker) {
			return true
		}
	}
	if strings.ContainsAny(raw, "<>=") {
		return true
	}
	return strings.Contains(raw, "(") || strings.Contains(raw, ")")
}

func cypherNullableEqual(lhs, rhs any) (equal bool, isNull bool) {
	if lhs == nil || rhs == nil {
		return false, true
	}

	if leftMap, leftTemporal := temporalMapValue(lhs); leftTemporal {
		if rightMap, rightTemporal := temporalMapValue(rhs); rightTemporal {
			if result, ok := compareTemporalMaps(leftMap, rightMap, "="); ok {
				return result, false
			}
		}
	}

	if lf, lok := comparableNumericValue(lhs); lok {
		if rf, rok := comparableNumericValue(rhs); rok {
			if li, lokInt := exactIntegerValue(lhs); lokInt {
				if ri, rokInt := exactIntegerValue(rhs); rokInt {
					return li == ri, false
				}
			}
			return lf == rf, false
		}
	}

	if lb, lok := lhs.(bool); lok {
		rb, rok := rhs.(bool)
		if !rok {
			return false, false
		}
		return lb == rb, false
	}

	if ls, lok := lhs.(string); lok {
		rs, rok := rhs.(string)
		if !rok {
			return false, false
		}
		return ls == rs, false
	}

	if ll, lok := asAnySlice(lhs); lok {
		rl, rok := asAnySlice(rhs)
		if !rok {
			return false, false
		}
		if len(ll) != len(rl) {
			return false, false
		}
		unknown := false
		for i := range ll {
			eq, isNull := cypherNullableEqual(ll[i], rl[i])
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	lm, lok := lhs.(map[string]any)
	rm, rok := rhs.(map[string]any)
	if lok || rok {
		if !lok || !rok {
			return false, false
		}
		if len(lm) != len(rm) {
			return false, false
		}
		unknown := false
		for k, lv := range lm {
			rv, ok := rm[k]
			if !ok {
				return false, false
			}
			eq, isNull := cypherNullableEqual(lv, rv)
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	if equal, handled := comparePathEquality(lhs, rhs); handled {
		return equal, false
	}

	return reflect.DeepEqual(lhs, rhs), false
}

func comparePathEquality(lhs, rhs any) (bool, bool) {
	leftNodes, leftEdges, ok := pathValueComponents(lhs)
	if !ok {
		return false, false
	}
	rightNodes, rightEdges, ok := pathValueComponents(rhs)
	if !ok {
		return false, false
	}
	if len(leftNodes) != len(rightNodes) || len(leftEdges) != len(rightEdges) {
		return false, true
	}
	for i := 0; i < len(leftNodes); i++ {
		if leftNodes[i] != rightNodes[i] {
			return false, true
		}
	}
	for i := 0; i < len(leftEdges); i++ {
		if leftEdges[i] != rightEdges[i] {
			return false, true
		}
	}
	return true, true
}

func pathValueComponents(value any) ([]string, []string, bool) {
	vertexID := func(v *graph.Vertex) string {
		if v == nil {
			return ""
		}
		return v.ID
	}
	edgeID := func(e *graph.Edge) string {
		if e == nil {
			return ""
		}
		return e.ID
	}

	switch typed := value.(type) {
	case cypherPathValue:
		nodes := []string{vertexID(typed.Left)}
		edges := []string{}
		if typed.Edge != nil || typed.Right != nil {
			edges = append(edges, edgeID(typed.Edge))
			nodes = append(nodes, vertexID(typed.Right))
		}
		return nodes, edges, true
	case multiHopCypherPath:
		nodes := make([]string, 0, len(typed.Nodes))
		for _, node := range typed.Nodes {
			nodes = append(nodes, vertexID(node))
		}
		edges := make([]string, 0, len(typed.Edges))
		for _, edge := range typed.Edges {
			edges = append(edges, edgeID(edge))
		}
		return nodes, edges, true
	default:
		return nil, nil, false
	}
}

func exactIntegerValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float32:
		f := float64(typed)
		if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
			return 0, false
		}
		return int64(f), true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return i, true
		}
		f, err := typed.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}

func isNumericType(value any) bool {
	_, ok := comparableNumericValue(value)
	return ok
}

func compareTemporalMaps(lhs, rhs map[string]any, op string) (bool, bool) {
	leftType := strings.ToLower(strings.TrimSpace(fmt.Sprint(lhs["__temporal_type"])))
	rightType := strings.ToLower(strings.TrimSpace(fmt.Sprint(rhs["__temporal_type"])))
	if leftType == "" || rightType == "" {
		return false, false
	}

	if leftType == "duration" && rightType == "duration" {
		leftDur := durationComponentsFromMap(lhs)
		rightDur := durationComponentsFromMap(rhs)
		switch op {
		case "=":
			return durationComponentsEqual(leftDur, rightDur), true
		case "<>":
			return !durationComponentsEqual(leftDur, rightDur), true
		case "<", "<=", ">", ">=":
			return compareDurationComponents(leftDur, rightDur, op), true
		}
		return false, false
	}

	leftInstant, ok1 := coerceDurationInstant(lhs)
	rightInstant, ok2 := coerceDurationInstant(rhs)
	if !ok1 || !ok2 {
		return false, false
	}
	lt, ok1 := durationInstantToTime(leftInstant)
	rt, ok2 := durationInstantToTime(rightInstant)
	if !ok1 || !ok2 {
		return false, false
	}

	switch op {
	case "=":
		return lt.Equal(rt), true
	case "<>":
		return !lt.Equal(rt), true
	case "<":
		return lt.Before(rt), true
	case "<=":
		return lt.Before(rt) || lt.Equal(rt), true
	case ">":
		return lt.After(rt), true
	case ">=":
		return lt.After(rt) || lt.Equal(rt), true
	default:
		return false, false
	}
}

func durationComponentsEqual(left, right durationComponents) bool {
	const epsilon = 1e-9
	return math.Abs(left.months-right.months) < epsilon && math.Abs(left.days-right.days) < epsilon && math.Abs(left.seconds-right.seconds) < epsilon
}

func compareDurationComponents(left, right durationComponents, op string) bool {
	if durationComponentsEqual(left, right) {
		switch op {
		case "<", ">":
			return false
		case "<=", ">=":
			return true
		}
	}
	if left.months != right.months {
		switch op {
		case "<":
			return left.months < right.months
		case "<=":
			return left.months < right.months
		case ">":
			return left.months > right.months
		case ">=":
			return left.months > right.months
		}
	}
	if left.days != right.days {
		switch op {
		case "<":
			return left.days < right.days
		case "<=":
			return left.days < right.days
		case ">":
			return left.days > right.days
		case ">=":
			return left.days > right.days
		}
	}
	switch op {
	case "<":
		return left.seconds < right.seconds
	case "<=":
		return left.seconds <= right.seconds
	case ">":
		return left.seconds > right.seconds
	case ">=":
		return left.seconds >= right.seconds
	default:
		return false
	}
}

func truthyWhereValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	default:
		return true
	}
}

func evalBooleanNot(value any) (any, error) {
	b, isNull, err := asNullableBoolean(value)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return !b, nil
}

func evalBooleanBinary(op string, lhs, rhs any) (any, error) {
	l, lNull, err := asNullableBoolean(lhs)
	if err != nil {
		return nil, err
	}
	r, rNull, err := asNullableBoolean(rhs)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "AND":
		if (!lNull && !l) || (!rNull && !r) {
			return false, nil
		}
		if lNull || rNull {
			return nil, nil
		}
		return true, nil
	case "OR":
		if (!lNull && l) || (!rNull && r) {
			return true, nil
		}
		if lNull || rNull {
			return nil, nil
		}
		return false, nil
	case "XOR":
		if lNull || rNull {
			return nil, nil
		}
		return l != r, nil
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("boolean operator %q is not supported", op), nil)
	}
}

func asNullableBoolean(value any) (bool, bool, error) {
	if value == nil {
		return false, true, nil
	}
	b, ok := value.(bool)
	if !ok {
		return false, false, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return b, false, nil
}

func parensAreBalanced(raw string) bool {
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

func evalUnwindValues(raw string, params Params, row Row) ([]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "empty UNWIND expression", nil)
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(raw[1 : len(raw)-1])
		if inner == "" {
			return []any{}, nil
		}
		parts := splitTopLevelCommaSeparated(inner)
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			value, err := evalExpressionWithScope(part, row, params)
			if err != nil {
				value, err = evalWriteValue(part, params, row)
			}
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		value, ok := params[name]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		if value == nil {
			return []any{}, nil
		}
		return flattenListValue(value)
	}
	if row != nil {
		if value, ok := row[raw]; ok {
			if value == nil {
				return []any{}, nil
			}
			return flattenListValue(value)
		}
	}
	if value, err := evalExpressionWithScope(raw, row, params); err == nil {
		if value == nil {
			return []any{}, nil
		}
		return flattenListValue(value)
	}
	value, err := evalWriteValue(raw, params, row)
	if err == nil {
		if value == nil {
			return []any{}, nil
		}
		return []any{value}, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("UNWIND expression %q is not yet supported", raw), nil)
}

func (e *Executor) evalProjectionPatternComprehension(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (any, bool, error) {
	if tx == nil {
		return nil, false, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}
	wrapSize := false
	if arg, ok := parseFunctionCall(raw, "size"); ok {
		raw = strings.TrimSpace(arg)
		wrapSize = true
	}

	patternExpr, projectionExpr, ok := parsePatternComprehension(raw)
	if !ok {
		return nil, false, nil
	}
	if strings.TrimSpace(patternExpr) == "" || strings.TrimSpace(projectionExpr) == "" {
		return nil, true, graph.NewError(graph.ErrKindSemantic, "pattern comprehension variables are required", nil)
	}

	matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternExpr}, params)
	if err != nil {
		return nil, true, err
	}
	out := make([]any, 0)
	for _, matchRow := range matches {
		projected, err := evalExpressionWithScope(projectionExpr, matchRow, params)
		if err != nil {
			if nested, nestedOK, nestedErr := e.evalProjectionPatternComprehension(ctx, tx, projectionExpr, matchRow, params); nestedOK {
				if nestedErr != nil {
					return nil, true, nestedErr
				}
				projected = nested
			} else {
				return nil, true, err
			}
		}
		out = append(out, projected)
	}
	if wrapSize {
		return len(out), true, nil
	}
	return out, true, nil
}

func parsePatternComprehension(raw string) (patternExpr string, projectionExpr string, ok bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return "", "", false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	pipeIdx := findTopLevelPipeIndex(body)
	if pipeIdx <= 0 {
		return "", "", false
	}
	left := strings.TrimSpace(body[:pipeIdx])
	projectionExpr = strings.TrimSpace(body[pipeIdx+1:])
	if left == "" || projectionExpr == "" {
		return "", "", false
	}

	eqIdx := findTopLevelEqualsIndex(left)
	if eqIdx >= 0 {
		pathVar := strings.TrimSpace(left[:eqIdx])
		if pathVar == "" || !isIdentifierLike(pathVar) {
			return "", "", false
		}
		patternExpr = strings.TrimSpace(left[eqIdx+1:])
		if !strings.HasPrefix(patternExpr, "(") {
			return "", "", false
		}
		return left, projectionExpr, true
	}

	if !strings.HasPrefix(left, "(") {
		return "", "", false
	}
	return left, projectionExpr, true
}

func findTopLevelEqualsIndex(raw string) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || raw[i-1] != '\\') && !inSingle {
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
		case '=':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i
			}
		}
	}
	return -1
}

func parseSimpleUndirectedPathComprehension(raw string) (pathVar string, sourceVar string, projectionExpr string, direction string, ok bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return "", "", "", "", false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	pipeIdx := findTopLevelPipeIndex(body)
	if pipeIdx <= 0 {
		return "", "", "", "", false
	}
	left := strings.TrimSpace(body[:pipeIdx])
	projectionExpr = strings.TrimSpace(body[pipeIdx+1:])
	eqIdx := strings.Index(left, "=")
	pattern := left
	if eqIdx > 0 {
		pathVar = strings.TrimSpace(left[:eqIdx])
		pattern = strings.TrimSpace(left[eqIdx+1:])
		if !isIdentifierLike(pathVar) {
			return "", "", "", "", false
		}
	}
	if projectionExpr == "" {
		return "", "", "", "", false
	}
	pattern = strings.ReplaceAll(pattern, " ", "")
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") {
		return "", "", "", "", false
	}
	if !strings.Contains(pattern, "--") || strings.Contains(pattern, "[") || strings.Contains(pattern, "]") {
		return "", "", "", "", false
	}
	if !strings.HasPrefix(pattern, "(") {
		return "", "", "", "", false
	}
	closeIdx := strings.Index(pattern, ")")
	if closeIdx <= 1 {
		return "", "", "", "", false
	}
	sourceVar = strings.TrimSpace(pattern[1:closeIdx])
	if !isIdentifierLike(sourceVar) {
		return "", "", "", "", false
	}
	remainder := pattern[closeIdx:]
	switch remainder {
	case ")--()":
		direction = "any"
	case ")-->()":
		direction = "out"
	case ")<--()":
		direction = "in"
	default:
		return "", "", "", "", false
	}
	return pathVar, sourceVar, projectionExpr, direction, true
}

func findTopLevelPipeIndex(raw string) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || raw[i-1] != '\\') && !inSingle {
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
		case '|':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i
			}
		}
	}
	return -1
}

func flattenListValue(value any) ([]any, error) {
	if value == nil {
		return []any{nil}, nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		return out, nil
	}
	return []any{value}, nil
}

func deleteVertexWithEdges(ctx context.Context, tx graph.Tx, tenant, vertexID string) error {
	edgeIDs := map[string]struct{}{}
	if err := tx.ScanOutEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
		edgeIDs[edge.ID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if err := tx.ScanInEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
		edgeIDs[edge.ID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	for edgeID := range edgeIDs {
		if err := tx.DeleteEdge(ctx, tenant, edgeID); err != nil {
			return err
		}
	}
	return tx.DeleteVertex(ctx, tenant, vertexID)
}

func resolvePatternSourceID(row Row, params Params, varName, paramName string) (string, error) {
	if binding, ok := row[varName]; ok {
		switch typed := binding.(type) {
		case *graph.Vertex:
			return typed.ID, nil
		case string:
			return typed, nil
		}
	}
	return requireStringParam(params, paramName)
}

func resolveOrCreateVertex(ctx context.Context, tx graph.Tx, tenant string, row Row, varName string, labels []string, props map[string]any, merge bool) (*graph.Vertex, bool, error) {
	if binding, ok := row[varName]; ok {
		if v, ok := binding.(*graph.Vertex); ok {
			return v, false, nil
		}
		if s, ok := binding.(string); ok && s != "" {
			v, err := tx.GetVertex(ctx, tenant, s)
			if err == nil {
				return v, false, nil
			}
		}
	}
	if merge && hasNilPropertyValue(props) {
		return nil, false, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}

	vertexID := ""
	if vertexID == "" {
		if merge {
			matches, err := findMergeVerticesByPattern(ctx, tx, tenant, labels, props)
			if err != nil {
				return nil, false, err
			}
			if len(matches) > 0 {
				return matches[0], false, nil
			}
		}
		vertexID = nextAutoVertexID(varName)
	}

	vertex, err := tx.GetVertex(ctx, tenant, vertexID)
	if err == nil {
		return vertex, false, nil
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		return nil, false, err
	}

	vertex = &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizeVertexProperties(props)}
	if err := tx.PutVertex(ctx, vertex); err != nil {
		return nil, false, err
	}
	return vertex, true, nil
}

func nextAutoVertexID(varName string) string {
	n := atomic.AddUint64(&autoVertexIDSeq, 1)
	if strings.TrimSpace(varName) == "" {
		return fmt.Sprintf("auto-v-%d", n)
	}
	return fmt.Sprintf("auto-%s-%d", varName, n)
}

func nextAutoEdgeID(tenant, srcID, edgeType, dstID string) string {
	n := atomic.AddUint64(&autoEdgeIDSeq, 1)
	return fmt.Sprintf("%s|%s|%s|%s|%d", tenant, srcID, edgeType, dstID, n)
}

func parsePropertyMap(raw string, params Params, row Row) (map[string]any, error) {
	out := map[string]any{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, pair := range splitTopLevelCommaSeparated(raw) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("property pair %q is not supported", pair), nil)
		}
		key := strings.TrimSpace(parts[0])
		valueExpr := strings.TrimSpace(parts[1])
		value, err := evalExpressionWithScope(valueExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(valueExpr, params, row)
		}
		if err != nil {
			if isIdentifierLike(valueExpr) {
				return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "UndefinedVariable"}
			}
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func vertexHasAnyEdges(ctx context.Context, tx graph.Tx, tenant, vertexID string) (bool, error) {
	hasEdges := false
	if err := tx.ScanOutEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
		hasEdges = true
		return nil
	}); err != nil {
		return false, err
	}
	if hasEdges {
		return true, nil
	}
	if err := tx.ScanInEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
		hasEdges = true
		return nil
	}); err != nil {
		return false, err
	}
	return hasEdges, nil
}

func evalWriteValue(raw string, params Params, row Row) (any, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if arg, ok := parseFunctionCall(raw, "date"); ok {
		return evalTemporalConstructor("date", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "time"); ok {
		return evalTemporalConstructor("time", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "datetime"); ok {
		return evalTemporalConstructor("datetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "localtime"); ok {
		return evalTemporalConstructor("localtime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "localdatetime"); ok {
		return evalTemporalConstructor("localdatetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "duration"); ok {
		return evalTemporalConstructor("duration", arg, params, row)
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		v, ok := params[name]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		return v, nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		return parseListLiteral(raw, params, row)
	}
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		return parseInlineMapLiteral(raw, params, row)
	}
	if row != nil {
		if v, ok := row[raw]; ok {
			return v, nil
		}
	}
	if strings.HasPrefix(raw, "'") || strings.HasPrefix(raw, `"`) {
		unquoted, err := unquoteCypherString(raw)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("string literal %q is not supported", raw), err)
		}
		return unquoted, nil
	}
	if raw == "true" || raw == "false" {
		return raw == "true", nil
	}
	if value, ok, err := parseHexOrOctalIntegerLiteral(raw); ok {
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), err)
		}
		return value, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return json.Number(formatFloatResult(f)), nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), nil)
}

func parseHexOrOctalIntegerLiteral(raw string) (int, bool, error) {
	if raw == "" {
		return 0, false, nil
	}
	negative := false
	unsigned := raw
	if strings.HasPrefix(unsigned, "+") {
		unsigned = unsigned[1:]
	} else if strings.HasPrefix(unsigned, "-") {
		negative = true
		unsigned = unsigned[1:]
	}
	if len(unsigned) < 3 || unsigned[0] != '0' {
		return 0, false, nil
	}
	base := 0
	switch unsigned[1] {
	case 'x', 'X':
		base = 16
	case 'o', 'O':
		base = 8
	default:
		return 0, false, nil
	}

	digits := unsigned[2:]
	if digits == "" {
		return 0, true, fmt.Errorf("missing integer literal digits")
	}

	parsed, err := strconv.ParseUint(digits, base, 64)
	if err != nil {
		return 0, true, err
	}

	if negative {
		const minIntAbs = uint64(1) << 63
		if parsed > minIntAbs {
			return 0, true, fmt.Errorf("integer overflow")
		}
		if parsed == minIntAbs {
			return int(math.MinInt64), true, nil
		}
		return int(-int64(parsed)), true, nil
	}

	if parsed > math.MaxInt64 {
		return 0, true, fmt.Errorf("integer overflow")
	}
	return int(parsed), true, nil
}

func unwrapOuterParentheses(raw string) (string, bool) {
	if len(raw) < 2 || raw[0] != '(' || raw[len(raw)-1] != ')' {
		return "", false
	}
	depth := 0
	inSingle := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if inSingle {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i < len(raw)-1 {
				return "", false
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	if depth != 0 {
		return "", false
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	if inner == "" {
		return "", false
	}
	return inner, true
}

func parseListLiteral(raw string, params Params, row Row) ([]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return []any{}, nil
	}
	parts := splitTopLevelCommaSeparated(body)
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		var (
			value any
			err   error
		)
		if isQuotedCypherString(part) {
			value, err = evalWriteValue(part, params, row)
		} else {
			value, err = evalExpressionWithScope(part, row, params)
			if err != nil {
				value, err = evalWriteValue(part, params, row)
			}
		}
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func isQuotedCypherString(raw string) bool {
	if len(raw) < 2 {
		return false
	}
	first := raw[0]
	last := raw[len(raw)-1]
	if first != last {
		return false
	}
	return first == '\'' || first == '"'
}

func parseInlineMapLiteral(raw string, params Params, row Row) (map[string]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	for _, pair := range splitTopLevelCommaSeparated(body) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("property pair %q is not supported", pair), nil)
		}
		key := strings.TrimSpace(parts[0])
		valueExpr := strings.TrimSpace(parts[1])
		value, err := evalExpressionWithScope(valueExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(valueExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func evalTemporalConstructor(name, arg string, params Params, row Row) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return map[string]any{"__temporal_type": name}, nil
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	out := map[string]any{"__temporal_type": name}
	switch typed := value.(type) {
	case map[string]any:
		normalized, normErr := normalizeTemporalConstructorMap(name, typed)
		if normErr != nil {
			return nil, normErr
		}
		for key, v := range normalized {
			out[key] = v
		}
		out["__temporal_type"] = name
	case string:
		if parsed, ok := parseTemporalLiteralToMap(name, typed); ok {
			for key, v := range parsed {
				out[key] = v
			}
		} else {
			out["value"] = typed
		}
	default:
		out["value"] = typed
	}
	return out, nil
}

func normalizeTemporalConstructorMap(name string, in map[string]any) (map[string]any, error) {
	typeName := strings.ToLower(strings.TrimSpace(name))
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}

	if typeName == "duration" {
		return out, nil
	}

	if _, hasDate := out["date"]; !hasDate {
		if embeddedDateTime, ok := out["datetime"]; ok {
			out["date"] = embeddedDateTime
		}
	}

	if embeddedDate, ok := parseEmbeddedDate(out["date"]); ok {
		if _, ok := out["year"]; !ok {
			out["year"] = embeddedDate.Year()
		}
		if _, ok := out["month"]; !ok {
			out["month"] = int(embeddedDate.Month())
		}
		if _, ok := out["day"]; !ok {
			out["day"] = embeddedDate.Day()
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		timeSource := out["time"]
		sourceTZ := ""
		if timeSource == nil {
			timeSource = out["datetime"]
		}
		if h, m, s, n, tz, ok := parseEmbeddedTime(timeSource); ok {
			sourceTZ = tz
			if sourceMap, ok := temporalMapValue(timeSource); ok {
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
			targetTZ := strings.TrimSpace(fmt.Sprint(out["timezone"]))
			if sourceTZ != "" && targetTZ != "" && sourceTZ != targetTZ {
				year, month, day := 1970, 1, 1
				if typeName == "datetime" {
					if y, mo, d, ok := resolveDateFromTemporalMap(out); ok {
						year, month, day = y, mo, d
					}
				}
				hour, _ := mapInt(out, "hour")
				minute, _ := mapInt(out, "minute")
				second, _ := mapInt(out, "second")
				nano := combineNanoseconds(out)
				if converted, ok := convertTemporalClockTimezone(year, month, day, hour, minute, second, nano, sourceTZ, targetTZ); ok {
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
		y, m, d, ok := resolveDateFromTemporalMap(out)
		if ok {
			out["year"] = y
			out["month"] = m
			out["day"] = d
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		hour, _ := mapInt(out, "hour")
		minute, _ := mapInt(out, "minute")
		second, _ := mapInt(out, "second")
		nano := combineNanoseconds(out)
		out["hour"] = hour
		out["minute"] = minute
		out["second"] = second
		out["nanosecond"] = nano
		delete(out, "microsecond")
		delete(out, "millisecond")
	}

	if typeName == "time" || typeName == "datetime" {
		tz := strings.TrimSpace(fmt.Sprint(out["timezone"]))
		if tz == "" {
			out["timezone"] = "Z"
		}
	}

	return out, nil
}

func resolveDateFromTemporalMap(in map[string]any) (int, int, int, bool) {
	if y, ord, ok := yearAndOrdinal(in); ok {
		base := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, ord-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if y, q, doq, ok := yearQuarterDayOfQuarter(in); ok {
		month := (q-1)*3 + 1
		base := time.Date(y, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, doq-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if week, ok := mapInt(in, "week"); ok {
		weekYear, hasWeekYear := mapInt(in, "year")
		baseDate, hasBaseDate := parseEmbeddedDate(in["date"])
		if !hasWeekYear {
			if hasBaseDate {
				isoYear, _ := baseDate.ISOWeek()
				weekYear = isoYear
				hasWeekYear = true
			}
		}
		if hasWeekYear {
			dayOfWeek, hasDOW := mapInt(in, "dayOfWeek")
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
			if resolved, ok := isoWeekDate(weekYear, week, dayOfWeek); ok {
				return resolved.Year(), int(resolved.Month()), resolved.Day(), true
			}
		}
	}
	if y, m, d, ok := directYMD(in); ok {
		return y, m, d, true
	}
	if y, ok := mapInt(in, "year"); ok {
		return y, 1, 1, true
	}
	if embedded, ok := parseEmbeddedDate(in["date"]); ok {
		return embedded.Year(), int(embedded.Month()), embedded.Day(), true
	}
	return 0, 0, 0, false
}

func directYMD(in map[string]any) (int, int, int, bool) {
	y, yOK := mapInt(in, "year")
	m, mOK := mapInt(in, "month")
	if yOK && mOK {
		d, dOK := mapInt(in, "day")
		if !dOK {
			d = 1
		}
		return y, m, d, true
	}
	return 0, 0, 0, false
}

func yearAndOrdinal(in map[string]any) (int, int, bool) {
	y, yOK := mapInt(in, "year")
	ord, ordOK := mapInt(in, "ordinalDay")
	if !yOK || !ordOK {
		return 0, 0, false
	}
	return y, ord, true
}

func yearQuarterDayOfQuarter(in map[string]any) (int, int, int, bool) {
	y, yOK := mapInt(in, "year")
	q, qOK := mapInt(in, "quarter")
	if !yOK || !qOK {
		return 0, 0, 0, false
	}
	doq, doqOK := mapInt(in, "dayOfQuarter")
	if !doqOK {
		if m, mOK := mapInt(in, "month"); mOK {
			if d, dOK := mapInt(in, "day"); dOK {
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

func parseEmbeddedDate(raw any) (time.Time, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		if y, m, d, ok := resolveDateFromTemporalMap(typed); ok {
			return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), true
		}
		if v, ok := typed["value"]; ok {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				if idx := strings.IndexAny(s, "Tt"); idx > 0 {
					s = strings.TrimSpace(s[:idx])
				}
				if t, err := time.Parse("2006-01-02", s); err == nil {
					return t, true
				}
			}
		}
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return time.Time{}, false
		}
		if idx := strings.IndexAny(s, "Tt"); idx > 0 {
			s = strings.TrimSpace(s[:idx])
		}
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseEmbeddedTime(raw any) (int, int, int, int, string, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		mapped := typed
		if _, hasTemporal := mapped["__temporal_type"]; !hasTemporal {
			if v, ok := mapped["value"]; ok {
				if parsed, ok := parseTemporalLiteralToMap("time", fmt.Sprint(v)); ok {
					mapped = parsed
				}
			}
		}
		hour, hasHour := mapInt(mapped, "hour")
		minute, hasMinute := mapInt(mapped, "minute")
		second, _ := mapInt(mapped, "second")
		nano := combineNanoseconds(mapped)
		tz := ""
		if tzRaw, ok := mapped["timezone"]; ok {
			tz = strings.TrimSpace(fmt.Sprint(tzRaw))
		}
		if hasHour || hasMinute || second != 0 || nano != 0 {
			return hour, minute, second, nano, tz, true
		}
		if valueRaw, ok := mapped["value"]; ok {
			if parsed, ok := parseTemporalLiteralToMap("time", fmt.Sprint(valueRaw)); ok {
				hour, _ = mapInt(parsed, "hour")
				minute, _ = mapInt(parsed, "minute")
				second, _ = mapInt(parsed, "second")
				nano = combineNanoseconds(parsed)
				tz = ""
				if tzRaw, ok := parsed["timezone"]; ok {
					tz = strings.TrimSpace(fmt.Sprint(tzRaw))
				}
				return hour, minute, second, nano, tz, true
			}
			if parsed, ok := parseTemporalLiteralToMap("localtime", fmt.Sprint(valueRaw)); ok {
				hour, _ = mapInt(parsed, "hour")
				minute, _ = mapInt(parsed, "minute")
				second, _ = mapInt(parsed, "second")
				nano = combineNanoseconds(parsed)
				return hour, minute, second, nano, "", true
			}
		}
	case string:
		if parsed, ok := parseTemporalLiteralToMap("time", typed); ok {
			h, _ := mapInt(parsed, "hour")
			m, _ := mapInt(parsed, "minute")
			s, _ := mapInt(parsed, "second")
			n := combineNanoseconds(parsed)
			tz := ""
			if tzRaw, ok := parsed["timezone"]; ok {
				tz = strings.TrimSpace(fmt.Sprint(tzRaw))
			}
			return h, m, s, n, tz, true
		}
		if parsed, ok := parseTemporalLiteralToMap("localtime", typed); ok {
			h, _ := mapInt(parsed, "hour")
			m, _ := mapInt(parsed, "minute")
			s, _ := mapInt(parsed, "second")
			n := combineNanoseconds(parsed)
			return h, m, s, n, "", true
		}
	}
	return 0, 0, 0, 0, "", false
}

func convertTemporalClockTimezone(year, month, day, hour, minute, second, nanosecond int, sourceTZ, targetTZ string) (time.Time, bool) {
	srcLoc := time.UTC
	if off, err := parseOffsetSeconds(sourceTZ); err == nil {
		srcLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(sourceTZ); err == nil {
		srcLoc = l
	}
	dstLoc := time.UTC
	if off, err := parseOffsetSeconds(targetTZ); err == nil {
		dstLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(targetTZ); err == nil {
		dstLoc = l
	}
	src := time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, srcLoc)
	return src.In(dstLoc), true
}

func combineNanoseconds(in map[string]any) int {
	nano, _ := mapInt(in, "nanosecond")
	micro, _ := mapInt(in, "microsecond")
	milli, _ := mapInt(in, "millisecond")
	total := nano + micro*1_000 + milli*1_000_000
	if total < 0 {
		return 0
	}
	if total >= 1_000_000_000 {
		total = total % 1_000_000_000
	}
	return total
}

func evalTemporalTruncateFunction(namespace string, argList string, row Row, params Params) (any, error) {
	args := splitTopLevelCommaSeparated(argList)
	if len(args) < 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "truncate() requires at least 2 arguments", nil)
	}
	unit, err := evalWriteValue(args[0], params, row)
	if err != nil {
		return nil, err
	}
	targetExpr := strings.TrimSpace(args[1])
	target, err := evalExpressionWithScope(targetExpr, row, params)
	if err != nil {
		target, err = evalWriteValue(targetExpr, params, row)
		if err != nil {
			return nil, err
		}
	}
	if target == nil {
		return nil, nil
	}
	if mapped, ok := target.(map[string]any); ok {
		out := map[string]any{}
		for key, value := range mapped {
			out[key] = value
		}
		if namespace != "" {
			out["__temporal_type"] = namespace
		}
		out["truncated"] = fmt.Sprint(unit)
		if len(args) >= 3 {
			overrideExpr := strings.TrimSpace(args[2])
			overrideValue, err := evalExpressionWithScope(overrideExpr, row, params)
			if err != nil {
				overrideValue, err = evalWriteValue(overrideExpr, params, row)
				if err != nil {
					return nil, err
				}
			}
			if overrideValue == nil {
				return nil, nil
			}
			if overrideMap, ok := overrideValue.(map[string]any); ok {
				out["truncate_overrides"] = overrideMap
			}
		}
		return out, nil
	}
	return target, nil
}

func evalTemporalNamespaceFunction(raw string, row Row, params Params) (any, bool, error) {
	idx := strings.Index(raw, "(")
	if idx <= 0 || !strings.HasSuffix(raw, ")") {
		return nil, false, nil
	}
	funcName := strings.TrimSpace(raw[:idx])
	if !strings.Contains(funcName, ".") {
		return nil, false, nil
	}
	parts := strings.SplitN(funcName, ".", 2)
	if len(parts) != 2 {
		return nil, false, nil
	}
	namespace := strings.ToLower(strings.TrimSpace(parts[0]))
	method := strings.TrimSpace(parts[1])
	switch namespace {
	case "date", "time", "datetime", "localtime", "localdatetime", "duration":
	default:
		return nil, false, nil
	}

	argsRaw := strings.TrimSpace(raw[idx+1 : len(raw)-1])
	if strings.EqualFold(method, "truncate") {
		value, err := evalTemporalTruncateFunction(namespace, argsRaw, row, params)
		return value, true, err
	}

	argExprs := []string{}
	if argsRaw != "" {
		argExprs = splitTopLevelCommaSeparated(argsRaw)
	}
	args := make([]any, 0, len(argExprs))
	for _, argExpr := range argExprs {
		argExpr = strings.TrimSpace(argExpr)
		value, err := evalExpressionWithScope(argExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(argExpr, params, row)
			if err != nil {
				return nil, true, err
			}
		}
		args = append(args, value)
	}

	for _, arg := range args {
		if arg == nil {
			return nil, true, nil
		}
	}

	if namespace != "duration" {
		if strings.EqualFold(method, "transaction") || strings.EqualFold(method, "statement") || strings.EqualFold(method, "realtime") {
			if len(args) == 0 {
				return map[string]any{"__temporal_type": namespace}, true, nil
			}
			if len(args) == 1 {
				value, err := temporalFromConstructedValue(namespace, args[0])
				return value, true, err
			}
		}
	}

	if namespace == "duration" && (strings.EqualFold(method, "indays") || strings.EqualFold(method, "inmonths") || strings.EqualFold(method, "inseconds") || strings.EqualFold(method, "between")) {
		value, err := evalDurationMethod(method, args)
		return value, true, err
	}

	if namespace == "datetime" {
		switch strings.ToLower(strings.TrimSpace(method)) {
		case "fromepoch":
			if len(args) < 1 || len(args) > 2 {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "datetime.fromepoch requires 1 or 2 arguments", nil)
			}
			seconds, ok := numericValue(args[0])
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepoch requires numeric seconds", nil)
			}
			nanos := 0.0
			if len(args) == 2 {
				if v, ok := numericValue(args[1]); ok {
					nanos = v
				} else {
					return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepoch requires numeric nanoseconds", nil)
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
			}, true, nil
		case "fromepochmillis":
			if len(args) != 1 {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "datetime.fromepochmillis requires 1 argument", nil)
			}
			millis, ok := numericValue(args[0])
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepochmillis requires numeric milliseconds", nil)
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
			}, true, nil
		}
	}

	return map[string]any{"__temporal_type": namespace, "method": method, "args": args}, true, nil
}

func temporalFromConstructedValue(name string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	out := map[string]any{"__temporal_type": name}
	switch typed := value.(type) {
	case map[string]any:
		normalized, normErr := normalizeTemporalConstructorMap(name, typed)
		if normErr != nil {
			return nil, normErr
		}
		for key, v := range normalized {
			out[key] = v
		}
	case string:
		if parsed, ok := parseTemporalLiteralToMap(name, typed); ok {
			for key, v := range parsed {
				out[key] = v
			}
		} else {
			out["value"] = typed
		}
	default:
		out["value"] = typed
	}
	return out, nil
}

type durationInstant struct {
	kind     string
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

type durationClock struct {
	secondOfDay float64
	hasZone     bool
	offset      int
}

func evalDurationMethod(method string, args []any) (any, error) {
	if len(args) < 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "duration method requires 2 arguments", nil)
	}
	left, ok := coerceDurationInstant(args[0])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, nil
	}
	right, ok := coerceDurationInstant(args[1])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	// Mixed zoned/local values are interpreted in the zoned operand's zone.
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

	if methodKey == "inseconds" && !left.hasDate && !right.hasDate {
		lClock := durationInstantClock(left)
		rClock := durationInstantClock(right)
		delta := rClock.secondOfDay - lClock.secondOfDay
		if lClock.hasZone && rClock.hasZone {
			delta += float64(lClock.offset - rClock.offset)
		}
		result := map[string]any{"__temporal_type": "duration"}
		setDurationFields(result, durationComponents{seconds: delta})
		return result, nil
	}

	if methodKey == "between" && !left.hasDate && !right.hasDate {
		lClock := durationInstantClock(left)
		rClock := durationInstantClock(right)
		delta := rClock.secondOfDay - lClock.secondOfDay
		if lClock.hasZone && rClock.hasZone {
			delta += float64(lClock.offset - rClock.offset)
		}
		result := map[string]any{"__temporal_type": "duration"}
		setDurationFields(result, durationComponents{seconds: delta})
		return result, nil
	}

	if methodKey == "inseconds" {
		if whole, nanos, ok := durationSecondsBetweenExact(left, right); ok {
			result := map[string]any{"__temporal_type": "duration"}
			result["years"] = 0
			result["months"] = 0
			result["days"] = 0
			result["seconds"] = whole
			result["nanoseconds"] = nanos
			result["nanosecondsOfSecond"] = nanos
			result["__duration_exact"] = true
			return result, nil
		}
		if secs, ok := durationSecondsBetweenWithoutTimeDateOverflow(left, right); ok {
			result := map[string]any{"__temporal_type": "duration"}
			setDurationFields(result, durationComponents{seconds: secs})
			return result, nil
		}
	}

	t1, ok1 := durationInstantToTime(left)
	t2, ok2 := durationInstantToTime(right)
	if !ok1 || !ok2 {
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	var dur durationComponents
	switch methodKey {
	case "inseconds":
		dur = durationComponents{seconds: t2.Sub(t1).Seconds()}
	case "indays":
		if !(left.hasDate && right.hasDate) {
			dur = durationComponents{}
			break
		}
		dur = durationComponents{days: truncTowardZero(t2.Sub(t1).Hours() / 24)}
	case "inmonths":
		if !(left.hasDate && right.hasDate) {
			dur = durationComponents{}
			break
		}
		months := (right.year-left.year)*12 + (right.month - left.month)
		anchor := t1.AddDate(0, months, 0)
		if months > 0 && anchor.After(t2) {
			months--
		}
		if months < 0 && anchor.Before(t2) {
			months++
		}
		dur = durationComponents{months: float64(months)}
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
			days := int(truncTowardZero(t2.Sub(anchor).Hours() / 24))
			anchor = anchor.AddDate(0, 0, days)
			dur = durationComponents{
				months:  float64(months),
				days:    float64(days),
				seconds: t2.Sub(anchor).Seconds(),
			}
		} else {
			dur = durationComponents{seconds: t2.Sub(t1).Seconds()}
		}
	default:
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	result := map[string]any{"__temporal_type": "duration"}
	setDurationFields(result, dur)
	return result, nil
}

func setDurationFields(out map[string]any, dur durationComponents) {
	totalMonths := int(truncTowardZero(dur.months))
	years := int(truncTowardZero(float64(totalMonths) / 12))
	months := totalMonths - years*12

	days := int(truncTowardZero(dur.days))
	secondsWhole, nanos := splitSecondsAndNanoseconds(dur.seconds)

	out["years"] = years
	out["months"] = months
	out["days"] = days
	out["seconds"] = secondsWhole
	out["nanoseconds"] = nanos
	out["nanosecondsOfSecond"] = nanos
}

func splitSecondsAndNanoseconds(seconds float64) (int, int) {
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

func durationInstantClock(v durationInstant) durationClock {
	sec := float64(v.hour*3600+v.minute*60+v.second) + float64(v.nano)/1_000_000_000
	off, _ := durationInstantOffsetSeconds(v)
	return durationClock{secondOfDay: sec, hasZone: v.hasZone, offset: off}
}

func durationInstantOffsetSeconds(v durationInstant) (int, bool) {
	if !v.hasZone {
		return 0, false
	}
	if parsed, err := parseOffsetSeconds(v.timezone); err == nil {
		return parsed, true
	}
	if v.hasDate {
		if loc, err := time.LoadLocation(v.timezone); err == nil {
			hour, minute, second, nano := 0, 0, 0, 0
			if v.hasTime {
				hour, minute, second, nano = v.hour, v.minute, v.second, v.nano
			}
			t := time.Date(v.year, time.Month(v.month), v.day, hour, minute, second, nano, loc)
			_, off := t.Zone()
			return off, true
		}
	}
	return 0, false
}

func durationSecondsBetweenWithoutTimeDateOverflow(left, right durationInstant) (float64, bool) {
	if !(left.hasDate && right.hasDate) {
		return 0, false
	}
	leftDays, ok := daysSinceEpoch(left.year, left.month, left.day)
	if !ok {
		return 0, false
	}
	rightDays, ok := daysSinceEpoch(right.year, right.month, right.day)
	if !ok {
		return 0, false
	}
	leftClock := durationInstantClock(left)
	rightClock := durationInstantClock(right)
	seconds := float64(rightDays-leftDays)*86400 + (rightClock.secondOfDay - leftClock.secondOfDay)
	if leftClock.hasZone && rightClock.hasZone {
		seconds += float64(leftClock.offset - rightClock.offset)
	}
	return seconds, true
}

func durationSecondsBetweenExact(left, right durationInstant) (int64, int, bool) {
	if !(left.hasDate && right.hasDate) {
		return 0, 0, false
	}
	leftDays, ok := daysSinceEpoch(left.year, left.month, left.day)
	if !ok {
		return 0, 0, false
	}
	rightDays, ok := daysSinceEpoch(right.year, right.month, right.day)
	if !ok {
		return 0, 0, false
	}

	leftSec := int64(left.hour*3600 + left.minute*60 + left.second)
	rightSec := int64(right.hour*3600 + right.minute*60 + right.second)
	leftNanos := left.nano
	rightNanos := right.nano

	whole := (rightDays-leftDays)*86400 + (rightSec - leftSec)
	if left.hasZone && right.hasZone {
		leftOffset, _ := durationInstantOffsetSeconds(left)
		rightOffset, _ := durationInstantOffsetSeconds(right)
		whole += int64(leftOffset - rightOffset)
	}

	nanos := rightNanos - leftNanos
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

func daysSinceEpoch(year, month, day int) (int64, bool) {
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

func durationInstantToTime(v durationInstant) (time.Time, bool) {
	year := 1970
	month := 1
	day := 1
	if v.hasDate {
		year = v.year
		month = v.month
		day = v.day
	}
	hour, minute, second, nano := 0, 0, 0, 0
	if v.hasTime {
		hour, minute, second, nano = v.hour, v.minute, v.second, v.nano
	}
	loc := time.UTC
	if v.hasZone {
		if off, err := parseOffsetSeconds(v.timezone); err == nil {
			loc = time.FixedZone("", off)
		} else if l, err := time.LoadLocation(v.timezone); err == nil {
			loc = l
		}
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nano, loc), true
}

func coerceDurationInstant(raw any) (durationInstant, bool) {
	mapped, ok := temporalMapValue(raw)
	if !ok {
		return durationInstant{}, false
	}
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(mapped["__temporal_type"])))
	if typeName == "" || typeName == "duration" {
		return durationInstant{}, false
	}
	if valueRaw, ok := mapped["value"]; ok {
		if parsed, ok := parseTemporalLiteralToMap(typeName, fmt.Sprint(valueRaw)); ok {
			parsed["__temporal_type"] = typeName
			mapped = parsed
		}
	}

	if y, m, d, ok := resolveDateFromTemporalMap(mapped); ok {
		mapped["year"] = y
		mapped["month"] = m
		mapped["day"] = d
	}

	inst := durationInstant{kind: typeName}
	if y, yOK := mapInt(mapped, "year"); yOK {
		if m, mOK := mapInt(mapped, "month"); mOK {
			if d, dOK := mapInt(mapped, "day"); dOK {
				inst.year = y
				inst.month = m
				inst.day = d
				inst.hasDate = true
			}
		}
	}
	if h, hOK := mapInt(mapped, "hour"); hOK {
		inst.hour = h
		inst.hasTime = true
	}
	if m, mOK := mapInt(mapped, "minute"); mOK {
		inst.minute = m
		inst.hasTime = true
	}
	if s, sOK := mapInt(mapped, "second"); sOK {
		inst.second = s
		inst.hasTime = true
	}
	inst.nano = combineNanoseconds(mapped)
	if inst.nano != 0 {
		inst.hasTime = true
	}
	if tzRaw, ok := mapped["timezone"]; ok {
		tz := strings.TrimSpace(fmt.Sprint(tzRaw))
		if tz != "" {
			inst.timezone = tz
			inst.hasZone = true
		}
	}

	if typeName == "date" {
		inst.hasTime = false
	}
	if typeName == "localtime" || typeName == "time" {
		inst.hasDate = false
	}
	if typeName == "time" {
		inst.hasZone = true
		if inst.timezone == "" {
			inst.timezone = "Z"
		}
	}
	if typeName == "datetime" && inst.timezone == "" {
		inst.timezone = "Z"
		inst.hasZone = true
	}
	return inst, true
}

func parseTemporalLiteralToMap(typeName, raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	switch typeName {
	case "date":
		y, m, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": m, "day": d}, true
	case "localtime":
		h, m, s, n, _, ok := parseClockAndZone(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n}, true
	case "time":
		h, m, s, n, tz, ok := parseClockAndZone(raw)
		if !ok {
			return nil, false
		}
		if tz == "" {
			tz = "Z"
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n, "timezone": tz}, true
	case "localdatetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, _, ok := parseClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n}, true
		}
		y, mo, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0}, true
	case "datetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, tz, ok := parseClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			if tz == "" {
				tz = "Z"
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n, "timezone": tz}, true
		}
		y, mo, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0, "timezone": "Z"}, true
	case "duration":
		return parseDurationLiteralToMap(raw)
	default:
		return nil, false
	}
}

func parseDateParts(raw string) (int, int, int, bool) {
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
		if strings.HasPrefix(rest, "-") {
			rest = rest[1:]
		}
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
		if resolved, ok := isoWeekDate(year, week, dayOfWeek); ok {
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
	return 0, 0, 0, false
}

func parseClockAndZone(raw string) (int, int, int, int, string, bool) {
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
		norm, ok := normalizeOffsetToken(offset)
		if ok {
			if !hasNamedZone {
				tz = norm
			}
			raw = raw[:offsetIdx]
		}
	}

	clock := strings.SplitN(raw, ".", 2)
	h := 0
	m := 0
	s := 0
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

func parseDurationLiteralToMap(raw string) (map[string]any, bool) {
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
		if strings.ContainsAny(datePart, "YyMmWwDd") {
			if parsed, ok := parseDurationUnitSection(datePart, false); ok {
				for k, v := range parsed {
					out[k] = v
					hasValue = true
				}
			}
		} else if y, m, d, ok := parseDateParts(datePart); ok {
			out["years"] = float64(y)
			out["months"] = float64(m)
			out["days"] = float64(d)
			hasValue = true
		}
	}
	if timePart != "" {
		if strings.Contains(timePart, ":") {
			h, m, s, n, _, ok := parseClockAndZone(timePart)
			if ok {
				out["hours"] = float64(h)
				out["minutes"] = float64(m)
				out["seconds"] = float64(s) + float64(n)/1_000_000_000
				hasValue = true
			}
		} else if strings.ContainsAny(timePart, "HhMmSs") {
			if parsed, ok := parseDurationUnitSection(timePart, true); ok {
				for k, v := range parsed {
					out[k] = v
					hasValue = true
				}
			}
		}
	}
	if !hasValue {
		return nil, false
	}
	return out, true
}

func parseDurationUnitSection(raw string, timeSection bool) (map[string]float64, bool) {
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
			} else if strings.ContainsAny(raw, "YyWwDd") {
				result["months"] += value
			} else {
				wholeMonths := truncTowardZero(value)
				result["months"] += wholeMonths
				fracMonths := value - wholeMonths
				if fracMonths != 0 {
					monthSeconds := fracMonths * 2629746.0
					wholeDays := truncTowardZero(monthSeconds / 86400)
					result["days"] += wholeDays
					result["seconds"] += monthSeconds - wholeDays*86400
				}
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

func normalizeOffsetToken(raw string) (string, bool) {
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
	body := raw[1:]
	if len(body) == 4 {
		return string(raw[0]) + body[:2] + ":" + body[2:], true
	}
	if len(body) == 2 {
		return string(raw[0]) + body + ":00", true
	}
	if len(body) == 6 {
		return string(raw[0]) + body[:2] + ":" + body[2:4] + ":" + body[4:], true
	}
	if len(body) == 5 && body[2] == ':' {
		return raw, true
	}
	if len(body) == 8 && body[2] == ':' && body[5] == ':' {
		return raw, true
	}
	return "", false
}

func splitTopLevelOperator(raw string, op string) (string, string, bool) {
	if op == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(raw[i:], op) {
			if (op == "+" || op == "-") && isUnarySignPosition(raw, i) {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(op):])
			if left == "" || right == "" {
				return "", "", false
			}
			return left, right, true
		}
	}
	return "", "", false
}

func splitTopLevelOperatorLast(raw string, op string) (string, string, bool) {
	if op == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	matchIdx := -1
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(raw[i:], op) {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(op):])
			if left != "" && right != "" {
				matchIdx = i
			}
		}
	}
	if matchIdx == -1 {
		return "", "", false
	}
	left := strings.TrimSpace(raw[:matchIdx])
	right := strings.TrimSpace(raw[matchIdx+len(op):])
	if left == "" || right == "" {
		return "", "", false
	}
	return left, right, true
}

func splitTopLevelOperatorSetLast(raw string, ops ...string) (string, string, string, bool) {
	if len(ops) == 0 {
		return "", "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	matchIdx := -1
	matchOp := ""
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		for _, op := range ops {
			if strings.HasPrefix(raw[i:], op) {
				if (op == "+" || op == "-") && isUnarySignPosition(raw, i) {
					continue
				}
				if (op == "+" || op == "-") && isExponentSignPosition(raw, i) {
					continue
				}
				left := strings.TrimSpace(raw[:i])
				right := strings.TrimSpace(raw[i+len(op):])
				if left != "" && right != "" {
					matchIdx = i
					matchOp = op
				}
				break
			}
		}
	}
	if matchIdx == -1 {
		return "", "", "", false
	}
	left := strings.TrimSpace(raw[:matchIdx])
	right := strings.TrimSpace(raw[matchIdx+len(matchOp):])
	if left == "" || right == "" {
		return "", "", "", false
	}
	return left, right, matchOp, true
}

func findCaseExpressionEnd(raw string, start int) (int, bool) {
	if start < 0 || start >= len(raw) {
		return -1, false
	}
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper[start:], "CASE") {
		return -1, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	caseDepth := 0
	for i := start; i < len(raw); i++ {
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
		if strings.HasPrefix(upper[i:], "CASE") {
			caseDepth++
			i += len("CASE") - 1
			continue
		}
		if caseDepth > 0 && strings.HasPrefix(upper[i:], "END") {
			caseDepth--
			if caseDepth == 0 {
				return i + len("END") - 1, true
			}
			i += len("END") - 1
		}
	}
	return -1, false
}

func isUnarySignPosition(raw string, idx int) bool {
	if idx == 0 {
		return true
	}
	for i := idx - 1; i >= 0; i-- {
		ch := raw[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			continue
		}
		switch ch {
		case '(', '[', '{', ',', '+', '-', '*', '/', '%', '=', '<', '>', '!':
			return true
		default:
			return false
		}
	}
	return true
}

func isExponentSignPosition(raw string, idx int) bool {
	if idx <= 0 || idx >= len(raw) {
		return false
	}
	sign := raw[idx]
	if sign != '+' && sign != '-' {
		return false
	}
	if idx+1 >= len(raw) || raw[idx+1] < '0' || raw[idx+1] > '9' {
		return false
	}
	prevIdx := idx - 1
	for prevIdx >= 0 && (raw[prevIdx] == ' ' || raw[prevIdx] == '\t' || raw[prevIdx] == '\n' || raw[prevIdx] == '\r') {
		prevIdx--
	}
	if prevIdx < 0 {
		return false
	}
	if raw[prevIdx] != 'e' && raw[prevIdx] != 'E' {
		return false
	}
	if prevIdx == 0 {
		return false
	}
	basePrev := raw[prevIdx-1]
	return (basePrev >= '0' && basePrev <= '9') || basePrev == '.'
}

func numericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		f, err := typed.Float64()
		if err == nil {
			return f, true
		}
		return 0, false
	case float32:
		return float64(typed), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func exactIntegerAggregateValue(v any) (int64, bool) {
	switch typed := v.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case uint:
		return int64(typed), true
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case json.Number:
		s := strings.TrimSpace(typed.String())
		if s == "" || strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		s := strings.TrimSpace(typed)
		if s == "" || strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func comparableNumericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		f, err := typed.Float64()
		if err == nil {
			return f, true
		}
		return 0, false
	case float32:
		return float64(typed), true
	default:
		return 0, false
	}
}

func evalAdditiveExpression(op, left, right, raw string, row Row, params Params) (any, error) {
	lhs, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	rhs, err := evalExpressionWithScope(right, row, params)
	if err != nil {
		return nil, err
	}
	if lhs == nil || rhs == nil {
		return nil, nil
	}
	lf, lok := numericValue(lhs)
	rf, rok := numericValue(rhs)
	if lok && rok {
		if isFloatLikeNumeric(lhs) || isFloatLikeNumeric(rhs) {
			switch op {
			case "+":
				return json.Number(formatFloatResult(lf + rf)), nil
			case "-":
				return json.Number(formatFloatResult(lf - rf)), nil
			}
		}
		if li, err := toInt(lhs); err == nil {
			if ri, err := toInt(rhs); err == nil {
				switch op {
				case "+":
					return li + ri, nil
				case "-":
					return li - ri, nil
				}
			}
		}
		switch op {
		case "+":
			return lf + rf, nil
		case "-":
			return lf - rf, nil
		}
	}
	if op == "+" {
		if list, ok := normalizeListValue(lhs); ok {
			out := append([]any{}, list...)
			if rhsList, ok := normalizeListValue(rhs); ok {
				out = append(out, rhsList...)
			} else {
				out = append(out, rhs)
			}
			return out, nil
		}
		if rhsList, ok := normalizeListValue(rhs); ok {
			out := make([]any, 0, len(rhsList)+1)
			out = append(out, lhs)
			out = append(out, rhsList...)
			return out, nil
		}
	}
	if value, ok := evalTemporalArithmetic(lhs, rhs, op); ok {
		return value, nil
	}
	if op == "+" {
		return fmt.Sprint(lhs) + fmt.Sprint(rhs), nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func evalMultiplicativeExpression(op, left, right, raw string, row Row, params Params) (any, error) {
	lhs, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	rhs, err := evalExpressionWithScope(right, row, params)
	if err != nil {
		return nil, err
	}
	if lhs == nil || rhs == nil {
		return nil, nil
	}
	lf, lok := numericValue(lhs)
	rf, rok := numericValue(rhs)
	if lok && rok {
		if isFloatLikeNumeric(lhs) || isFloatLikeNumeric(rhs) {
			if (op == "/" || op == "%") && rf == 0 {
				if op == "/" {
					return json.Number(formatFloatResult(lf / rf)), nil
				}
				return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
			}
			switch op {
			case "*":
				return json.Number(formatFloatResult(lf * rf)), nil
			case "/":
				return json.Number(formatFloatResult(lf / rf)), nil
			case "%":
				return json.Number(formatFloatResult(math.Mod(lf, rf))), nil
			}
		}
		li, lerr := toInt(lhs)
		ri, rerr := toInt(rhs)
		if lerr == nil && rerr == nil {
			switch op {
			case "*":
				return li * ri, nil
			case "/":
				if ri == 0 {
					return nil, graph.NewError(graph.ErrKindInvalidInput, "division by zero", nil)
				}
				return li / ri, nil
			case "%":
				if ri == 0 {
					return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
				}
				return li % ri, nil
			}
		}
		switch op {
		case "*":
			return lf * rf, nil
		case "/":
			if rf == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "division by zero", nil)
			}
			return json.Number(formatFloatResult(lf / rf)), nil
		case "%":
			if rf == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
			}
			return json.Number(formatFloatResult(math.Mod(lf, rf))), nil
		}
	}
	if value, ok := evalTemporalArithmetic(lhs, rhs, op); ok {
		return value, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", compactExpression(raw)), nil)
}

func compactExpression(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteByte(raw[i])
		}
	}
	return b.String()
}

func formatFloatResult(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "Inf"
	}
	if math.IsInf(value, -1) {
		return "-Inf"
	}
	if value == 0 {
		return "0.0"
	}
	abs := math.Abs(value)
	if abs >= 1e15 || abs < 1e-8 {
		formatted := strconv.FormatFloat(value, 'e', -1, 64)
		parts := strings.SplitN(formatted, "e", 2)
		if len(parts) != 2 {
			return formatted
		}
		exp := parts[1]
		expSign := ""
		if strings.HasPrefix(exp, "+") || strings.HasPrefix(exp, "-") {
			expSign = exp[:1]
			exp = exp[1:]
		}
		exp = strings.TrimLeft(exp, "0")
		if exp == "" {
			exp = "0"
		}
		if expSign == "+" {
			expSign = ""
		}
		return parts[0] + "e" + expSign + exp
	}
	formatted := strconv.FormatFloat(value, 'f', -1, 64)
	if strings.HasPrefix(formatted, ".") {
		formatted = "0" + formatted
	}
	if strings.HasPrefix(formatted, "-.") {
		formatted = "-0" + formatted[1:]
	}
	if !strings.ContainsAny(formatted, ".eE") {
		formatted += ".0"
	}
	return formatted
}

func parseStoredMapString(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "map[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := strings.TrimSpace(raw[len("map[") : len(raw)-1])
	if body == "" {
		return map[string]any{}, true
	}
	out := map[string]any{}
	for _, part := range strings.Fields(body) {
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			continue
		}
		out[pair[0]] = pair[1]
	}
	return out, true
}

func evalTemporalArithmetic(lhs, rhs any, op string) (any, bool) {
	leftMap, leftTemporal := temporalMapValue(lhs)
	rightMap, rightTemporal := temporalMapValue(rhs)

	if leftTemporal && rightTemporal {
		leftType := strings.ToLower(fmt.Sprint(leftMap["__temporal_type"]))
		rightType := strings.ToLower(fmt.Sprint(rightMap["__temporal_type"]))
		if leftType == "duration" && rightType == "duration" {
			leftDur := durationComponentsFromMap(leftMap)
			rightDur := durationComponentsFromMap(rightMap)
			switch op {
			case "+":
				return formatDurationComponents(leftDur.add(rightDur)), true
			case "-":
				return formatDurationComponents(leftDur.sub(rightDur)), true
			}
		}

		if rightType == "duration" {
			if value, ok := applyTemporalAndDuration(leftMap, durationComponentsFromMap(rightMap), op); ok {
				return value, true
			}
		}
	}

	if leftTemporal {
		leftType := strings.ToLower(fmt.Sprint(leftMap["__temporal_type"]))
		if leftType == "duration" {
			leftDur := durationComponentsFromMap(leftMap)
			if factor, ok := numericValue(rhs); ok {
				switch op {
				case "*":
					return formatDurationComponents(leftDur.scale(factor)), true
				case "/":
					if factor == 0 {
						return nil, false
					}
					return formatDurationComponents(leftDur.scale(1 / factor)), true
				}
			}
		}
	}

	if rightTemporal && op == "*" {
		rightType := strings.ToLower(fmt.Sprint(rightMap["__temporal_type"]))
		if rightType == "duration" {
			if factor, ok := numericValue(lhs); ok {
				return formatDurationComponents(durationComponentsFromMap(rightMap).scale(factor)), true
			}
		}
	}

	return nil, false
}

type durationComponents struct {
	months  float64
	days    float64
	seconds float64
}

func (d durationComponents) add(other durationComponents) durationComponents {
	return durationComponents{months: d.months + other.months, days: d.days + other.days, seconds: d.seconds + other.seconds}
}

func (d durationComponents) sub(other durationComponents) durationComponents {
	return durationComponents{months: d.months - other.months, days: d.days - other.days, seconds: d.seconds - other.seconds}
}

func (d durationComponents) scale(factor float64) durationComponents {
	return durationComponents{months: d.months * factor, days: d.days * factor, seconds: d.seconds * factor}
}

func temporalMapValue(v any) (map[string]any, bool) {
	switch typed := v.(type) {
	case map[string]any:
		if _, ok := typed["__temporal_type"]; ok {
			return typed, true
		}
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			if _, hasTemporal := mapped["__temporal_type"]; hasTemporal {
				return mapped, true
			}
		}
	}
	return nil, false
}

func durationComponentsFromMap(value map[string]any) durationComponents {
	seconds := 3600*mapFloat(value, "hours") + 60*mapFloat(value, "minutes") + mapFloat(value, "seconds")
	months := 12*mapFloat(value, "years") + mapFloat(value, "months")
	days := 7*mapFloat(value, "weeks") + mapFloat(value, "days")

	// Fractional month components are converted into day/second components before
	// arithmetic so operations across durations preserve openCypher expectations.
	const avgMonthSeconds = 2629746.0
	wholeMonths := truncTowardZero(months)
	fracMonths := months - wholeMonths
	if fracMonths != 0 {
		monthSeconds := fracMonths * avgMonthSeconds
		wholeDays := truncTowardZero(monthSeconds / 86400)
		days += wholeDays
		seconds += monthSeconds - wholeDays*86400
	}
	wholeDays := truncTowardZero(days)
	fracDays := days - wholeDays
	if fracDays != 0 {
		seconds += fracDays * 86400
		days = wholeDays
	}

	var nanosAcc int64
	if ms, ok := mapWholeInt64(value, "milliseconds"); ok {
		nanosAcc += ms * 1_000_000
	} else {
		seconds += mapFloat(value, "milliseconds") / 1_000
	}
	if us, ok := mapWholeInt64(value, "microseconds"); ok {
		nanosAcc += us * 1_000
	} else {
		seconds += mapFloat(value, "microseconds") / 1_000_000
	}
	if ns, ok := mapWholeInt64(value, "nanoseconds"); ok {
		nanosAcc += ns
	} else {
		seconds += mapFloat(value, "nanoseconds") / 1_000_000_000
	}
	seconds += float64(nanosAcc) / 1_000_000_000
	secWhole, secNanos := splitSecondsAndNanoseconds(seconds)
	seconds = float64(secWhole) + float64(secNanos)/1_000_000_000

	return durationComponents{
		months:  wholeMonths,
		days:    days,
		seconds: seconds,
	}
}

func canonicalizeDurationComponents(dur durationComponents) durationComponents {
	years, months, days, seconds := decomposeDuration(dur)
	return durationComponents{months: float64(years*12 + months), days: float64(days), seconds: seconds}
}

func mapFloat(value map[string]any, key string) float64 {
	raw, ok := value[key]
	if !ok {
		return 0
	}
	if f, ok := numericValue(raw); ok {
		return f
	}
	return 0
}

func mapWholeInt64(value map[string]any, key string) (int64, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	intVal, err := toInt(raw)
	if err != nil {
		return 0, false
	}
	if f, ok := numericValue(raw); ok {
		if math.Abs(f-float64(intVal)) > 1e-12 {
			return 0, false
		}
	}
	return int64(intVal), true
}

func applyTemporalAndDuration(temporal map[string]any, dur durationComponents, op string) (any, bool) {
	if op != "+" && op != "-" {
		return nil, false
	}
	if op == "-" {
		dur = dur.scale(-1)
	}

	temporalType := strings.ToLower(fmt.Sprint(temporal["__temporal_type"]))
	year, yOk := mapInt(temporal, "year")
	month, mOk := mapInt(temporal, "month")
	day, dOk := mapInt(temporal, "day")
	hour, _ := mapInt(temporal, "hour")
	minute, _ := mapInt(temporal, "minute")
	second, _ := mapInt(temporal, "second")
	nanosecond, _ := mapInt(temporal, "nanosecond")

	loc := time.UTC
	if tzRaw, ok := temporal["timezone"]; ok {
		tz := strings.TrimSpace(fmt.Sprint(tzRaw))
		if offset, err := parseOffsetSeconds(tz); err == nil {
			loc = time.FixedZone("", offset)
		}
	}

	baseYear, baseMonth, baseDay := 2000, 1, 1
	if yOk {
		baseYear = year
	}
	if mOk {
		baseMonth = month
	}
	if dOk {
		baseDay = day
	}

	base := time.Date(baseYear, time.Month(baseMonth), baseDay, hour, minute, second, nanosecond, loc)
	addY, addM, addD, addSeconds := decomposeDuration(dur)
	dateAdjusted := base.AddDate(addY, addM, addD)
	adjusted := dateAdjusted.Add(secondsToDuration(addSeconds))

	switch temporalType {
	case "date":
		dayCarry := int(truncTowardZero(addSeconds / 86400))
		dateAdjusted = base.AddDate(addY, addM, addD+dayCarry)
		return temporalResultFromTime("date", dateAdjusted, temporal), true
	case "localtime":
		return temporalResultFromTime("localtime", adjusted, temporal), true
	case "time":
		return temporalResultFromTime("time", adjusted, temporal), true
	case "localdatetime":
		return temporalResultFromTime("localdatetime", adjusted, temporal), true
	case "datetime":
		return temporalResultFromTime("datetime", adjusted, temporal), true
	default:
		return nil, false
	}
}

func temporalResultFromTime(typeName string, t time.Time, source map[string]any) map[string]any {
	out := map[string]any{"__temporal_type": typeName}
	switch typeName {
	case "date":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
	case "localtime":
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
	case "time":
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
		if tzRaw, ok := source["timezone"]; ok {
			tz := strings.TrimSpace(fmt.Sprint(tzRaw))
			if tz != "" {
				out["timezone"] = tz
			} else {
				_, off := t.Zone()
				out["timezone"] = formatOffsetString(off)
			}
		} else {
			_, off := t.Zone()
			out["timezone"] = formatOffsetString(off)
		}
	case "localdatetime":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
	case "datetime":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
		if tzRaw, ok := source["timezone"]; ok {
			tz := strings.TrimSpace(fmt.Sprint(tzRaw))
			if tz != "" {
				out["timezone"] = tz
			} else {
				_, off := t.Zone()
				out["timezone"] = formatOffsetString(off)
			}
		} else {
			_, off := t.Zone()
			out["timezone"] = formatOffsetString(off)
		}
	}
	return out
}

func decomposeDuration(dur durationComponents) (int, int, int, float64) {
	const avgMonthSeconds = 2629746.0
	totalMonths := dur.months
	years := int(truncTowardZero(totalMonths / 12))
	remainingMonths := totalMonths - float64(years*12)
	months := int(truncTowardZero(remainingMonths))
	fracMonths := remainingMonths - float64(months)

	totalDays := dur.days + (fracMonths*avgMonthSeconds)/86400
	days := int(truncTowardZero(totalDays))
	fracDays := totalDays - float64(days)

	seconds := dur.seconds + fracDays*86400
	return years, months, days, seconds
}

func formatDurationComponents(dur durationComponents) string {
	years, months, days, seconds := decomposeDuration(dur)
	wholeSeconds := int64(truncTowardZero(seconds))
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
	// Snap values that are very close to integral nanoseconds to avoid binary drift.
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

	b := strings.Builder{}
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
				absNanos := nanos
				if absNanos < 0 {
					absNanos = -absNanos
				}
				frac := strings.TrimRight(fmt.Sprintf("%09d", absNanos), "0")
				b.WriteString(fmt.Sprintf("%s%d.%sS", sign, absSec, frac))
			}
		}
	}
	return b.String()
}

func truncTowardZero(v float64) float64 {
	if v < 0 {
		return math.Ceil(v)
	}
	return math.Floor(v)
}

func mapInt(value map[string]any, key string) (int, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	if iv, err := toInt(raw); err == nil {
		return iv, true
	}
	if fv, ok := numericValue(raw); ok {
		return int(truncTowardZero(fv)), true
	}
	return 0, false
}

func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func formatTimeString(t time.Time, includeZone bool) string {
	hms := t.Format("15:04")
	sec := t.Second()
	nanos := t.Nanosecond()
	frac := ""
	if sec != 0 || nanos != 0 {
		hms += fmt.Sprintf(":%02d", sec)
	}
	if nanos != 0 {
		frac = "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if includeZone {
		_, off := t.Zone()
		return hms + frac + formatOffsetString(off)
	}
	return hms + frac
}

func formatDateTimeString(t time.Time, includeZone bool) string {
	base := t.Format("2006-01-02T15:04")
	sec := t.Second()
	nanos := t.Nanosecond()
	if sec != 0 || nanos != 0 {
		base += fmt.Sprintf(":%02d", sec)
	}
	if nanos != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if includeZone {
		_, off := t.Zone()
		base += formatOffsetString(off)
	}
	return base
}

func formatTemporalToString(temporal map[string]any) (string, bool) {
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	switch typeName {
	case "date":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), true
	case "localtime":
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		return formatClockParts(hour, minute, second, nano), true
	case "time":
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		tzName := strings.TrimSpace(fmt.Sprint(temporal["timezone"]))
		if tzName == "" {
			tzName = "Z"
		}
		offsetRendered := tzName
		if offset, err := parseOffsetSeconds(tzName); err == nil {
			offsetRendered = formatOffsetString(offset)
		}
		return formatClockParts(hour, minute, second, nano) + offsetRendered, true
	case "localdatetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		return fmt.Sprintf("%04d-%02d-%02dT%s", y, m, d, formatClockParts(hour, minute, second, nano)), true
	case "datetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		tzName := strings.TrimSpace(fmt.Sprint(temporal["timezone"]))
		if tzName == "" {
			tzName = "Z"
		}
		clock := formatClockParts(hour, minute, second, nano)
		if offset, err := parseOffsetSeconds(tzName); err == nil {
			return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, formatOffsetString(offset)), true
		}
		if loc, err := time.LoadLocation(tzName); err == nil {
			t := time.Date(y, time.Month(m), d, hour, minute, second, nano, loc)
			_, offset := t.Zone()
			return fmt.Sprintf("%04d-%02d-%02dT%s%s[%s]", y, m, d, clock, formatOffsetString(offset), tzName), true
		}
		return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, tzName), true
	case "duration":
		if exact, ok := temporal["__duration_exact"].(bool); ok && exact {
			if sec, secOK := mapWholeInt64(temporal, "seconds"); secOK {
				if nanos, nanoOK := mapWholeInt64(temporal, "nanoseconds"); nanoOK {
					return formatDurationFromExactSecondNanos(sec, int(nanos)), true
				}
			}
		}
		return formatDurationComponents(durationComponentsFromMap(temporal)), true
	default:
		return "", false
	}
}

func formatDurationFromExactSecondNanos(seconds int64, nanos int) string {
	if nanos < 0 {
		nanos = 0
	}
	if nanos >= 1_000_000_000 {
		seconds += int64(nanos / 1_000_000_000)
		nanos = nanos % 1_000_000_000
	}

	negative := seconds < 0
	absSeconds := seconds
	absNanos := nanos
	if negative {
		absSeconds = -seconds
		if absNanos > 0 {
			absSeconds--
			absNanos = 1_000_000_000 - absNanos
		}
	}

	hours := int(absSeconds / 3600)
	remainingSeconds := absSeconds - int64(hours*3600)
	minutes := int(remainingSeconds / 60)
	secInt := int(remainingSeconds - int64(minutes*60))

	b := strings.Builder{}
	b.WriteString("PT")
	if hours != 0 {
		if negative {
			b.WriteString(fmt.Sprintf("-%dH", hours))
		} else {
			b.WriteString(fmt.Sprintf("%dH", hours))
		}
	}
	if minutes != 0 {
		if negative {
			b.WriteString(fmt.Sprintf("-%dM", minutes))
		} else {
			b.WriteString(fmt.Sprintf("%dM", minutes))
		}
	}
	if secInt != 0 || absNanos != 0 || (hours == 0 && minutes == 0) {
		if absNanos == 0 {
			if negative {
				b.WriteString(fmt.Sprintf("-%dS", secInt))
			} else {
				b.WriteString(fmt.Sprintf("%dS", secInt))
			}
		} else {
			sign := ""
			if negative {
				sign = "-"
			}
			frac := strings.TrimRight(fmt.Sprintf("%09d", absNanos), "0")
			b.WriteString(fmt.Sprintf("%s%d.%sS", sign, secInt, frac))
		}
	}
	return b.String()
}

func formatClockParts(hour, minute, second, nano int) string {
	base := fmt.Sprintf("%02d:%02d", hour, minute)
	if second != 0 || nano != 0 {
		base += fmt.Sprintf(":%02d", second)
	}
	if nano != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nano), "0")
	}
	return base
}

func parseOffsetSeconds(raw string) (int, error) {
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

func formatOffsetString(totalSeconds int) string {
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

func unquoteCypherString(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid string literal")
	}
	quote := raw[0]
	if raw[len(raw)-1] != quote {
		return "", fmt.Errorf("mismatched string literal quotes")
	}
	inner := raw[1 : len(raw)-1]
	switch quote {
	case '\'':
		var b strings.Builder
		b.Grow(len(inner))
		for i := 0; i < len(inner); i++ {
			ch := inner[i]
			if ch == '\'' && i+1 < len(inner) && inner[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			if ch != '\\' {
				b.WriteByte(ch)
				continue
			}
			if i+1 >= len(inner) {
				return "", fmt.Errorf("invalid string escape")
			}
			next := inner[i+1]
			i++
			switch next {
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case '\\':
				if i+1 < len(inner) && inner[i+1] == '\'' {
					b.WriteByte('\'')
					i++
					break
				}
				b.WriteByte('\\')
			case '\'':
				b.WriteByte('\'')
			case '"':
				b.WriteByte('"')
			case 'u':
				if i+4 >= len(inner) {
					return "", fmt.Errorf("invalid unicode escape")
				}
				codePoint, err := strconv.ParseUint(inner[i+1:i+5], 16, 16)
				if err != nil {
					return "", err
				}
				b.WriteRune(rune(codePoint))
				i += 4
			default:
				return "", fmt.Errorf("unsupported string escape %q", next)
			}
		}
		return b.String(), nil
	case '"':
		unquoted, err := strconv.Unquote(raw)
		if err != nil {
			return "", err
		}
		return unquoted, nil
	default:
		return "", fmt.Errorf("unsupported quote character")
	}
}

func normalizeVertexProperties(props map[string]any) graph.PropertyMap {
	out := graph.PropertyMap{}
	for k, v := range props {
		if v == nil {
			continue
		}
		out[k] = valueToBytes(v)
	}
	return out
}

func normalizeEdgeProperties(props map[string]any) graph.PropertyMap {
	out := graph.PropertyMap{}
	for k, v := range props {
		if v == nil {
			continue
		}
		out[k] = valueToBytes(v)
	}
	return out
}

func stringFromProperty(props map[string]any, key string) string {
	v, ok := props[key]
	if !ok {
		return ""
	}
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
}

func valueToBytes(v any) []byte {
	switch typed := v.(type) {
	case nil:
		return []byte("null")
	case []byte:
		return append([]byte(nil), typed...)
	case string:
		return []byte(typed)
	case int:
		return []byte(strconv.Itoa(typed))
	case int64:
		return []byte(strconv.FormatInt(typed, 10))
	case bool:
		if typed {
			return []byte("true")
		}
		return []byte("false")
	default:
		return []byte(fmt.Sprint(v))
	}
}

func valueToPropertyBytes(v any) ([]byte, error) {
	if !isSupportedPropertyValue(v) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
	}
	return valueToBytes(v), nil
}

func isSupportedPropertyValue(v any) bool {
	switch typed := v.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		json.Number:
		return true
	case map[string]any:
		_, temporal := typed["__temporal_type"]
		return temporal
	case *graph.Vertex, *graph.Edge, cypherPathValue, multiHopCypherPath:
		return false
	case []any:
		for _, item := range typed {
			if !isSupportedPropertyValue(item) {
				return false
			}
		}
		return true
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				if !isSupportedPropertyValue(rv.Index(i).Interface()) {
					return false
				}
			}
			return true
		}
		return false
	}
}

func splitLabels(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ":")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		labels = append(labels, part)
	}
	return labels
}

func stripLeadingClauseKeyword(raw, keyword string) string {
	raw = strings.TrimSpace(raw)
	return strings.TrimSpace(strings.TrimPrefix(raw, keyword))
}

func stripCypherLineComments(raw string) string {
	if raw == "" {
		return raw
	}
	var b strings.Builder
	b.Grow(len(raw))
	inSingle := false
	inDouble := false

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inSingle {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					b.WriteByte(raw[i+1])
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			b.WriteByte(ch)
			if ch == '\\' {
				if i+1 < len(raw) {
					b.WriteByte(raw[i+1])
					i++
				}
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}

		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			continue
		}
		if ch == '/' && i+1 < len(raw) && raw[i+1] == '/' {
			for i < len(raw) && raw[i] != '\n' && raw[i] != '\r' {
				i++
			}
			if i < len(raw) {
				b.WriteByte(raw[i])
			}
			continue
		}
		b.WriteByte(ch)
	}

	return b.String()
}

func normalizeClauseBody(raw string) string {
	if raw == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(raw))
	inSingle := false
	inDouble := false

	for i := 0; i < len(raw); i++ {
		r := raw[i]
		if inSingle {
			b.WriteByte(r)
			if r == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					b.WriteByte(raw[i+1])
					i++
					continue
				}
				inSingle = false
			}
			continue
		}

		if inDouble {
			b.WriteByte(r)
			if r == '\\' {
				if i+1 < len(raw) {
					b.WriteByte(raw[i+1])
					i++
				}
				continue
			}
			if r == '"' {
				inDouble = false
			}
			continue
		}

		if unicode.IsSpace(rune(r)) {
			continue
		}

		b.WriteByte(r)
		if r == '\'' {
			inSingle = true
			continue
		}
		if r == '"' {
			inDouble = true
		}
	}

	return b.String()
}

func cloneRow(in Row) Row {
	out := make(Row, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneVertex(v *graph.Vertex) *graph.Vertex {
	if v == nil {
		return nil
	}
	out := &graph.Vertex{
		Tenant: v.Tenant,
		ID:     v.ID,
		Labels: append([]string(nil), v.Labels...),
	}
	if v.Properties != nil {
		out.Properties = make(graph.PropertyMap, len(v.Properties))
		for k, val := range v.Properties {
			out.Properties[k] = append([]byte(nil), val...)
		}
	}
	return out
}

func cloneEdge(e *graph.Edge) *graph.Edge {
	if e == nil {
		return nil
	}
	out := &graph.Edge{
		Tenant: e.Tenant,
		ID:     e.ID,
		Type:   e.Type,
		SrcID:  e.SrcID,
		DstID:  e.DstID,
	}
	if e.Properties != nil {
		out.Properties = make(graph.PropertyMap, len(e.Properties))
		for k, val := range e.Properties {
			out.Properties[k] = append([]byte(nil), val...)
		}
	}
	return out
}

func ensureProperties(v *graph.Vertex) {
	if v.Properties == nil {
		v.Properties = graph.PropertyMap{}
	}
}

func ensurePropertiesEdge(e *graph.Edge) {
	if e.Properties == nil {
		e.Properties = graph.PropertyMap{}
	}
}

func syntheticEdgeID(tenant, srcID, edgeType, dstID string) string {
	return strings.Join([]string{tenant, srcID, edgeType, dstID}, "|")
}
