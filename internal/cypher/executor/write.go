package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	createVertexPatternRE         = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternRE           = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternReverseRE    = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternUndirectedRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createChainNodeTokenRE        = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)`)
	createChainRelForwardTokenRE  = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->`)
	createChainRelReverseTokenRE  = regexp.MustCompile(`^<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createChainRelUndirTokenRE    = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createMissingRelTypeForwardRE = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--?>\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeReverseRE = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)<--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeUndirRE   = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	setClauseRE                   = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	removeClauseRE                = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)
	deleteClauseRE                = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)$`)
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

var autoVertexIDSeq uint64

func (e *Executor) executeQueryStatement(ctx context.Context, stmt *ast.QueryStatement, params Params) (_ *Result, err error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}
	if len(stmt.Parts) != 1 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "only single query parts are supported", nil)
	}
	if len(stmt.Unions) > 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, "UNION is not yet supported", nil)
	}
	part := stmt.Parts[0]

	writeQuery := hasWriteClause(part)
	rows := []Row{{}}
	resultColumns := []string{}
	returnSeen := false

	withTx := func(tx graph.Tx) error {
		for _, clause := range part.Clauses {
			var stepErr error
			switch clause.Kind {
			case ast.ClauseKindMatch:
				rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			case ast.ClauseKindOptionalMatch:
				rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			case ast.ClauseKindUnwind:
				rows, stepErr = e.applyUnwindClause(rows, clause, params)
			case ast.ClauseKindWith:
				rows, resultColumns, stepErr = e.applyProjectionClause(rows, clause, params, false)
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
				rows, resultColumns, stepErr = e.applyProjectionClause(rows, clause, params, true)
				returnSeen = true
				if stepErr != nil {
					return stepErr
				}
				return nil
			case ast.ClauseKindInQueryCall:
				rows, stepErr = e.applyInQueryCallClause(rows, clause, params)
			default:
				return graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("clause %s is not yet supported", clause.Kind), nil)
			}
			if stepErr != nil {
				return stepErr
			}
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

	if !returnSeen {
		resultColumns = nil
		if writeQuery && rowsAreEmpty(rows) {
			rows = nil
		}
	}

	result := &Result{Columns: resultColumns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}
	result.Rows = normalizeResultRows(result.Rows)
	return result, nil
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
	if multi, ok := parseMultiNodeMatchPattern(spec.Pattern); ok {
		return e.expandMultiNodeMatch(ctx, tx, rows, spec, multi, params)
	}
	if node, err := parseNodePattern(spec.Pattern); err == nil {
		return e.expandNodeMatch(ctx, tx, rows, spec, node, params)
	}
	if anchored, err := parseAnchoredOutPattern(spec.Pattern); err == nil {
		if shouldUseAnchoredOutPath(rows, anchored) {
			return e.expandAnchoredMatch(ctx, tx, rows, spec, params)
		}
	}
	if directed, err := parseDirectedAdjacentPattern(spec.Pattern); err == nil {
		return e.expandDirectedAdjacentMatch(ctx, tx, rows, spec, directed, params)
	}
	if reverseDirected, err := parseReverseDirectedAdjacentPattern(spec.Pattern); err == nil {
		return e.expandReverseDirectedAdjacentMatch(ctx, tx, rows, spec, reverseDirected, params)
	}
	if undirected, err := parseUndirectedAdjacentPattern(spec.Pattern); err == nil {
		return e.expandUndirectedAdjacentMatch(ctx, tx, rows, spec, undirected, params)
	}
	if rel, err := parseDirectedRelationshipPattern(spec.Pattern); err == nil {
		return e.expandDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params)
	}
	if rel, err := parseReverseDirectedRelationshipPattern(spec.Pattern); err == nil {
		return e.expandReverseDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params)
	}
	if rel, err := parseUndirectedRelationshipPattern(spec.Pattern); err == nil {
		return e.expandUndirectedRelationshipMatch(ctx, tx, rows, spec, rel, params)
	}
	if chain, err := parseTwoHopDirectedChainPattern(spec.Pattern); err == nil {
		return e.expandTwoHopDirectedChainMatch(ctx, tx, rows, spec, chain, params)
	}
	return e.expandAnchoredMatch(ctx, tx, rows, spec, params)
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
	pattern, where, ok := splitTopLevelKeyword(raw, "WHERE")
	if !ok {
		spec.Pattern = pattern
		return spec, nil
	}
	spec.Pattern = pattern
	spec.Where = where
	return spec, nil
}

func (e *Executor) expandAnchoredMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, params Params) ([]Row, error) {
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
				merged[pattern.SourceVar] = nil
				merged[pattern.TargetVar] = nil
				merged["edge"] = nil
				out = append(out, merged)
			}
			continue
		}

		matched := false
		for _, src := range sources {
			if src == nil {
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

				merged := cloneRow(row)
				merged[pattern.SourceVar] = src
				merged[pattern.TargetVar] = dst
				merged["edge"] = edge

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
			merged[pattern.SourceVar] = nil
			merged[pattern.TargetVar] = nil
			merged["edge"] = nil
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

func (e *Executor) expandNodeMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern nodePattern, params Params) ([]Row, error) {
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
			merged[pattern.Var] = candidate

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
			merged[pattern.Var] = nil
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandUndirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedAdjacentPattern, params Params) ([]Row, error) {
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

			handleAdjacent := func(otherID string) error {
				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, row) {
					return nil
				}

				merged := cloneRow(row)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
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
				return handleAdjacent(edge.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				return handleAdjacent(edge.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedAdjacentPattern, params Params) ([]Row, error) {
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

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				neighbor, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, row) {
					return nil
				}

				merged := cloneRow(row)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseDirectedAdjacentPattern, params Params) ([]Row, error) {
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

			if err := tx.ScanOutEdges(ctx, tenant, right.ID, "", 0, func(edge *graph.Edge) error {
				left, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !nodePatternMatches(left, pattern.Left, params, row) {
					return nil
				}

				merged := cloneRow(row)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
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

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := tx.ScanOutEdges(ctx, tenant, left.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
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
				if !nodePatternMatches(neighbor, pattern.Right, params, row) {
					return nil
				}

				merged := cloneRow(row)
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
			if pattern.EdgeVar != "" {
				merged[pattern.EdgeVar] = nil
			}
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
		rightCandidates, err := e.resolveNodePatternCandidates(ctx, tx, tenant, row, pattern.Right, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, right := range rightCandidates {
			if right == nil {
				continue
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := tx.ScanOutEdges(ctx, tenant, right.ID, scanType, 0, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge, pattern.EdgeProps, params, row) {
					return nil
				}
				left, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !nodePatternMatches(left, pattern.Left, params, row) {
					return nil
				}

				merged := cloneRow(row)
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
			if pattern.EdgeVar != "" {
				merged[pattern.EdgeVar] = nil
			}
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

			handle := func(edge *graph.Edge, otherID string) error {
				if !edgePatternMatches(edge, pattern.EdgeProps, params, row) {
					return nil
				}
				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !nodePatternMatches(neighbor, pattern.Right, params, row) {
					return nil
				}

				merged := cloneRow(row)
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
			}
			if pattern.EdgeVar != "" {
				merged[pattern.EdgeVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandTwoHopDirectedChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern twoHopDirectedChainPattern, params Params) ([]Row, error) {
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
				if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, edge2.SrcID)
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
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = nil
			}
			if pattern.Mid.Var != "" {
				merged[pattern.Mid.Var] = nil
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = nil
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

	props := strings.TrimSpace(pattern.PropertiesRaw)
	if props == "" {
		return true
	}

	parsed, err := parsePropertyMap(props, params, row)
	if err != nil {
		return false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			if vertex.ID != stringFromProperty(map[string]any{"id": value}, "id") {
				return false
			}
			continue
		}
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
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind)))
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	parts := splitTopLevelCommaSeparated(raw)
	out := rows
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out, err = e.applyCreatePattern(ctx, tx, out, part, params, tenant, merge)
		if err != nil {
			return nil, err
		}
	}
	if len(out) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
	}
	return out, nil
}

func (e *Executor) applyCreatePattern(ctx context.Context, tx graph.Tx, rows []Row, raw string, params Params, tenant string, merge bool) ([]Row, error) {
	if m := createVertexPatternRE.FindStringSubmatch(raw); len(m) == 4 {
		return e.applyCreateVertex(ctx, tx, rows, m, params, tenant, merge)
	}
	if spec, ok := parseCreateVertexPatternSpec(raw); ok {
		return e.applyCreateVertexSpec(ctx, tx, rows, spec, params, tenant, merge)
	}
	if isMissingRelationshipTypePattern(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	if m := createEdgePatternRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionForward)
	}
	if m := createEdgePatternReverseRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionReverse)
	}
	if m := createEdgePatternUndirectedRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionUndirected)
	}
	if chain, ok := parseCreateChainPattern(raw); ok {
		return e.applyCreateChainPattern(ctx, tx, rows, chain, params, tenant, merge)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
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
		normalizedProps := normalizeVertexProperties(props)
		vertexID := stringFromProperty(props, "id")
		if vertexID == "" {
			if existing, ok := row[varName].(*graph.Vertex); ok {
				vertexID = existing.ID
			}
		}
		if vertexID == "" {
			if merge {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MERGE vertex requires id", nil)
			}
			vertexID = nextAutoVertexID(varName)
		}

		vertex := &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizedProps}
		if merge {
			if existing, err := tx.GetVertex(ctx, vertex.Tenant, vertex.ID); err == nil {
				vertex = existing
			}
		}
		if err := tx.PutVertex(ctx, vertex); err != nil {
			return nil, err
		}
		merged := cloneRow(row)
		if varName != "" {
			merged[varName] = vertex
		}
		out = append(out, merged)
	}
	return out, nil
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

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		leftProps, err := parsePropertyMap(m[3], params, row)
		if err != nil {
			return nil, err
		}
		edgeProps, err := parsePropertyMap(m[6], params, row)
		if err != nil {
			return nil, err
		}
		rightProps, err := parsePropertyMap(m[9], params, row)
		if err != nil {
			return nil, err
		}
		leftVertex, err := resolveOrCreateVertex(ctx, tx, tenant, row, leftVar, leftLabels, leftProps, merge)
		if err != nil {
			return nil, err
		}
		rightVertex, err := resolveOrCreateVertex(ctx, tx, tenant, row, rightVar, rightLabels, rightProps, merge)
		if err != nil {
			return nil, err
		}

		edge, err := createOrMergeEdge(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, merge, direction)
		if err != nil {
			return nil, err
		}

		merged := cloneRow(row)
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

		for i, node := range pattern.Nodes {
			props, err := parsePropertyMap(node.PropsRaw, params, merged)
			if err != nil {
				return nil, err
			}
			vertex, err := resolveOrCreateVertex(ctx, tx, tenant, merged, node.Var, node.Labels, props, merge)
			if err != nil {
				return nil, err
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
			edge, err := createOrMergeEdge(ctx, tx, vertices[i], vertices[i+1], relType, relProps, merge, rel.Direction)
			if err != nil {
				return nil, err
			}
			if rel.Var != "" {
				merged[rel.Var] = edge
			}
		}

		out = append(out, merged)
	}
	return out, nil
}

func createOrMergeEdge(ctx context.Context, tx graph.Tx, leftVertex, rightVertex *graph.Vertex, edgeType string, edgeProps map[string]any, merge bool, direction createEdgeDirection) (*graph.Edge, error) {
	srcVertex := leftVertex
	dstVertex := rightVertex
	switch direction {
	case createEdgeDirectionReverse:
		srcVertex = rightVertex
		dstVertex = leftVertex
	case createEdgeDirectionForward, createEdgeDirectionUndirected:
		// Keep CREATE default direction left-to-right.
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown edge direction %q", direction), nil)
	}

	edgeID := syntheticEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID)
	edge := &graph.Edge{Tenant: srcVertex.Tenant, ID: edgeID, Type: edgeType, SrcID: srcVertex.ID, DstID: dstVertex.ID, Properties: normalizeEdgeProperties(edgeProps)}
	if merge {
		if direction == createEdgeDirectionUndirected {
			if existing, err := tx.GetEdge(ctx, edge.Tenant, edge.ID); err == nil {
				edge = existing
			} else if !graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, err
			} else {
				reverseID := syntheticEdgeID(rightVertex.Tenant, rightVertex.ID, edgeType, leftVertex.ID)
				if existing, err := tx.GetEdge(ctx, edge.Tenant, reverseID); err == nil {
					edge = existing
				} else if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
					return nil, err
				}
			}
		} else if existing, err := tx.GetEdge(ctx, edge.Tenant, edge.ID); err == nil {
			edge = existing
		} else if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
	}
	if err := tx.PutEdge(ctx, edge); err != nil {
		return nil, err
	}
	return edge, nil
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
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, "SET"))
	match := setClauseRE.FindStringSubmatch(raw)
	if len(match) != 4 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET clause %q is not yet supported", raw), nil)
	}

	varName, field, exprRaw := match[1], match[2], match[3]
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		binding, ok := row[varName]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
		}
		value, err := evalWriteValue(exprRaw, params, row)
		if err != nil {
			return nil, err
		}
		switch typed := binding.(type) {
		case *graph.Vertex:
			if field == "id" {
				return nil, graph.NewError(graph.ErrKindUnsupported, "setting vertex id is not supported", nil)
			}
			mutated := cloneVertex(typed)
			ensureProperties(mutated)
			mutated.Properties[field] = valueToBytes(value)
			if err := tx.PutVertex(ctx, mutated); err != nil {
				return nil, err
			}
			row = cloneRow(row)
			row[varName] = mutated
		case *graph.Edge:
			if field == "id" {
				return nil, graph.NewError(graph.ErrKindUnsupported, "setting edge id is not supported", nil)
			}
			mutated := cloneEdge(typed)
			ensurePropertiesEdge(mutated)
			mutated.Properties[field] = valueToBytes(value)
			if err := tx.PutEdge(ctx, mutated); err != nil {
				return nil, err
			}
			row = cloneRow(row)
			row[varName] = mutated
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
		}
		out = append(out, row)
	}
	return out, nil
}

func (e *Executor) applyRemoveClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, "REMOVE"))
	match := removeClauseRE.FindStringSubmatch(raw)
	if len(match) != 3 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE clause %q is not yet supported", raw), nil)
	}

	varName, field := match[1], match[2]
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		binding, ok := row[varName]
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
			row = cloneRow(row)
			row[varName] = mutated
		case *graph.Edge:
			mutated := cloneEdge(typed)
			ensurePropertiesEdge(mutated)
			delete(mutated.Properties, field)
			if err := tx.PutEdge(ctx, mutated); err != nil {
				return nil, err
			}
			row = cloneRow(row)
			row[varName] = mutated
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE on %T is not supported", binding), nil)
		}
		out = append(out, row)
	}
	return out, nil
}

func (e *Executor) applyDeleteClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, "DELETE"))
	match := deleteClauseRE.FindStringSubmatch(raw)
	if len(match) != 2 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE clause %q is not yet supported", raw), nil)
	}

	varName := match[1]
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		binding, ok := row[varName]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
		}
		switch typed := binding.(type) {
		case *graph.Vertex:
			if err := deleteVertexWithEdges(ctx, tx, typed.Tenant, typed.ID); err != nil {
				return nil, err
			}
			row = cloneRow(row)
			delete(row, varName)
		case *graph.Edge:
			if err := tx.DeleteEdge(ctx, typed.Tenant, typed.ID); err != nil {
				return nil, err
			}
			row = cloneRow(row)
			delete(row, varName)
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE on %T is not supported", binding), nil)
		}
		out = append(out, row)
	}
	return out, nil
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

func (e *Executor) applyProjectionClause(rows []Row, clause ast.Clause, params Params, final bool) ([]Row, []string, error) {
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind)))
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
		if item.CountArg != "" || item.CollectArg != "" {
			hasAggregate = true
		}
	}

	if !hasAggregate {
		for _, row := range rows {
			projected := Row{}
			for _, item := range items {
				if item.Expression == "*" {
					for key, value := range row {
						projected[key] = value
					}
					continue
				}
				value, err := evalExpressionWithScope(item.Expression, row, params)
				if err != nil {
					return nil, nil, err
				}
				key := item.Expression
				if item.Alias != "" {
					key = item.Alias
				}
				projected[key] = value
			}
			out = append(out, projected)
		}
		if hasStar {
			columns = inferProjectionColumns(out)
		}
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		return out, columns, nil
	}

	type projectionGroup struct {
		projected Row
		counts    map[int]int
		collects  map[int][]any
	}

	nonAggregateCount := 0
	for _, item := range items {
		if item.CountArg == "" && item.CollectArg == "" {
			nonAggregateCount++
		}
	}

	groups := map[string]*projectionGroup{}
	groupOrder := make([]string, 0)
	for _, row := range rows {
		projected := Row{}
		keyValues := make([]any, 0, nonAggregateCount)
		for _, item := range items {
			if item.CountArg != "" || item.CollectArg != "" {
				continue
			}
			value, err := evalExpressionWithScope(item.Expression, row, params)
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
			group = &projectionGroup{projected: projected, counts: map[int]int{}, collects: map[int][]any{}}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}

		for idx, item := range items {
			if item.CountArg != "" {
				if item.CountArg == "*" {
					group.counts[idx]++
					continue
				}
				value, err := evalExpressionWithScope(item.CountArg, row, params)
				if err != nil {
					return nil, nil, err
				}
				if value != nil {
					group.counts[idx]++
				}
				continue
			}
			if item.CollectArg != "" {
				value, err := evalExpressionWithScope(item.CollectArg, row, params)
				if err != nil {
					return nil, nil, err
				}
				group.collects[idx] = append(group.collects[idx], value)
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
			if item.CountArg != "" {
				projected[key] = 0
			} else if item.CollectArg != "" {
				projected[key] = []any{}
			} else {
				projected[key] = nil
			}
		}
		out = append(out, projected)
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		return out, columns, nil
	}

	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		projected := cloneRow(group.projected)
		for idx, item := range items {
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
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
			}
		}
		out = append(out, projected)
	}
	if hasStar {
		columns = inferProjectionColumns(out)
	}
	out, err = applyProjectionPostProcessing(out, projection, params)
	if err != nil {
		return nil, nil, err
	}
	return out, columns, nil
}

type projectionSpec struct {
	Expression string
	Alias      string
	CountArg   string
	CollectArg string
}

type projectionClauseSpec struct {
	ProjectionRaw string
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

	out := projectionClauseSpec{ProjectionRaw: strings.TrimSpace(projectionRaw)}

	if orderByIdx >= 0 {
		end := minPositiveIndex(greaterIndex(skipIdx, orderByIdx), greaterIndex(limitIdx, orderByIdx))
		if end < 0 {
			end = len(raw)
		}
		orderByRaw := strings.TrimSpace(raw[orderByIdx+len("ORDERBY") : end])
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

func applyProjectionPostProcessing(rows []Row, clause projectionClauseSpec, params Params) ([]Row, error) {
	if len(clause.OrderBy) > 0 {
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

	return applySkipLimit(rows, skip, limit), nil
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
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	leftInt, leftIntErr := toInt(left)
	rightInt, rightIntErr := toInt(right)
	if leftIntErr == nil && rightIntErr == nil {
		switch {
		case leftInt < rightInt:
			return -1
		case leftInt > rightInt:
			return 1
		default:
			return 0
		}
	}

	leftBool, leftBoolOK := left.(bool)
	rightBool, rightBoolOK := right.(bool)
	if leftBoolOK && rightBoolOK {
		switch {
		case !leftBool && rightBool:
			return -1
		case leftBool && !rightBool:
			return 1
		default:
			return 0
		}
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
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
			return i
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
		if idx := strings.LastIndex(strings.ToUpper(part), "AS"); idx > 0 {
			expr := strings.TrimSpace(part[:idx])
			alias = normalizeProjectionIdentifier(strings.TrimSpace(part[idx+2:]))
			if expr == "" || alias == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection item %q is not supported", part), nil)
			}
			countArg, _ := parseCountExpression(expr)
			collectArg, _ := parseCollectExpression(expr)
			items = append(items, projectionSpec{Expression: expr, Alias: alias, CountArg: countArg, CollectArg: collectArg})
			continue
		}
		countArg, _ := parseCountExpression(part)
		collectArg, _ := parseCollectExpression(part)
		items = append(items, projectionSpec{Expression: part, CountArg: countArg, CollectArg: collectArg})
	}
	return items, nil
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

func parseCollectExpression(raw string) (string, bool) {
	arg, ok := parseFunctionCall(raw, "collect")
	if !ok || arg == "" {
		return "", false
	}
	return arg, true
}

func splitTopLevelCommaSeparated(raw string) []string {
	parts := []string{}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	start := 0
	for i, r := range raw {
		switch r {
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
	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return evalExpressionWithScope(raw[1:len(raw)-1], row, params)
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
		return compareWhereValues(lhs, rhs, ">=")
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
		return compareWhereValues(lhs, rhs, "<=")
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
		if _, lok := temporalMapValue(lhs); lok {
			if _, rok := temporalMapValue(rhs); rok {
				return !reflect.DeepEqual(normalizeResultValue(lhs), normalizeResultValue(rhs)), nil
			}
		}
		return !reflect.DeepEqual(lhs, rhs), nil
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
		if _, lok := temporalMapValue(lhs); lok {
			if _, rok := temporalMapValue(rhs); rok {
				return reflect.DeepEqual(normalizeResultValue(lhs), normalizeResultValue(rhs)), nil
			}
		}
		return reflect.DeepEqual(lhs, rhs), nil
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
		return compareWhereValues(lhs, rhs, ">")
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
		return compareWhereValues(lhs, rhs, "<")
	}
	if left, right, ok := splitTopLevelOperator(raw, "+"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
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
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
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
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, right, ok := splitTopLevelOperator(raw, "*"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
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
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, right, ok := splitTopLevelOperator(raw, "/"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
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
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if value, ok, err := evalTemporalNamespaceFunction(raw, row, params); ok {
		return value, err
	}
	if arg, ok := parseFunctionCall(raw, "toString"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		normalized := normalizeResultValue(value)
		if rendered, ok := normalized.(string); ok {
			return rendered, nil
		}
		return fmt.Sprint(normalized), nil
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
		return evalLabelsFunction(arg, row)
	}
	if arg, ok := parseFunctionCall(raw, "type"); ok {
		return evalTypeFunction(arg, row)
	}
	if value, err := evalWriteValue(raw, params, row); err == nil {
		return value, nil
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 2 {
		base, ok := row[parts[0]]
		if !ok {
			if value, ok := params[parts[0]]; ok {
				base = value
			} else {
				return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", parts[0]), nil)
			}
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
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("field access not supported on %T", base), nil)
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("field access not supported on %T", base), nil)
		}
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func evalLabelsFunction(arg string, binding map[string]any) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "labels() requires one argument", nil)
	}
	base, ok := binding[arg]
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", arg), nil)
	}
	if base == nil {
		return nil, nil
	}
	switch typed := base.(type) {
	case *graph.Vertex:
		return append([]string(nil), typed.Labels...), nil
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
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("labels() is not supported on %T", base), nil)
}

func evalTypeFunction(arg string, binding map[string]any) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "type() requires one argument", nil)
	}
	base, ok := binding[arg]
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", arg), nil)
	}
	if base == nil {
		return nil, nil
	}
	switch typed := base.(type) {
	case *graph.Edge:
		return typed.Type, nil
	case map[string]any:
		if relType, ok := typed["type"]; ok {
			return fmt.Sprint(relType), nil
		}
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("type() is not supported on %T", base), nil)
}

func (e *Executor) evalWhereExpression(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, graph.NewError(graph.ErrKindSemantic, "empty WHERE expression", nil)
	}
	if body, ok := parseExistsSubqueryBody(raw); ok {
		return e.evalExistsSubquery(ctx, tx, body, row, params)
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
		value, err := e.evalWhereExpression(ctx, tx, strings.TrimSpace(raw[3:]), row, params)
		if err != nil {
			return false, err
		}
		return !value, nil
	}

	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return e.evalWhereExpression(ctx, tx, raw[1:len(raw)-1], row, params)
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
	if !strings.HasPrefix(strings.ToUpper(body), "MATCH") && !strings.HasPrefix(strings.ToUpper(body), "OPTIONALMATCH") {
		return false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}

	rows, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: body}, params)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func splitTopLevelKeyword(raw, keyword string) (string, string, bool) {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(keyword)
	depth := 0
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
			if beforeIsWord && afterIsWord {
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

func isAlphaOrUnderscore(ch byte) bool {
	if ch == '_' {
		return true
	}
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func splitTopLevelComparison(raw string) (string, string, string, bool) {
	op := []string{"<=", ">=", "<>", "=", "<", ">"}
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
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

func compareWhereValues(lhs, rhs any, op string) (bool, error) {
	if lhs == nil || rhs == nil {
		switch op {
		case "=":
			return lhs == rhs, nil
		case "<>":
			return lhs != rhs, nil
		default:
			return false, nil
		}
	}

	if leftMap, leftTemporal := temporalMapValue(lhs); leftTemporal {
		if rightMap, rightTemporal := temporalMapValue(rhs); rightTemporal {
			if value, ok := compareTemporalMaps(leftMap, rightMap, op); ok {
				return value, nil
			}
		}
	}

	leftInt, leftIntErr := toInt(lhs)
	rightInt, rightIntErr := toInt(rhs)
	if leftIntErr == nil && rightIntErr == nil {
		switch op {
		case "=":
			return leftInt == rightInt, nil
		case "<>":
			return leftInt != rightInt, nil
		case "<":
			return leftInt < rightInt, nil
		case "<=":
			return leftInt <= rightInt, nil
		case ">":
			return leftInt > rightInt, nil
		case ">=":
			return leftInt >= rightInt, nil
		}
	}

	leftStr := fmt.Sprint(lhs)
	rightStr := fmt.Sprint(rhs)
	switch op {
	case "=":
		return leftStr == rightStr, nil
	case "<>":
		return leftStr != rightStr, nil
	case "<":
		return leftStr < rightStr, nil
	case "<=":
		return leftStr <= rightStr, nil
	case ">":
		return leftStr > rightStr, nil
	case ">=":
		return leftStr >= rightStr, nil
	default:
		return false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("WHERE operator %q is not supported", op), nil)
	}
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
			value, err := evalWriteValue(part, params, row)
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
		return flattenListValue(value)
	}
	if row != nil {
		if value, ok := row[raw]; ok {
			return flattenListValue(value)
		}
	}
	value, err := evalWriteValue(raw, params, row)
	if err == nil {
		return []any{value}, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("UNWIND expression %q is not yet supported", raw), nil)
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

func resolveOrCreateVertex(ctx context.Context, tx graph.Tx, tenant string, row Row, varName string, labels []string, props map[string]any, merge bool) (*graph.Vertex, error) {
	if binding, ok := row[varName]; ok {
		if v, ok := binding.(*graph.Vertex); ok {
			return v, nil
		}
		if s, ok := binding.(string); ok && s != "" {
			v, err := tx.GetVertex(ctx, tenant, s)
			if err == nil {
				return v, nil
			}
		}
	}

	vertexID := stringFromProperty(props, "id")
	if vertexID == "" {
		if merge {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("MERGE vertex %q requires id", varName), nil)
		}
		vertexID = nextAutoVertexID(varName)
	}

	vertex, err := tx.GetVertex(ctx, tenant, vertexID)
	if err == nil {
		return vertex, nil
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		return nil, err
	}

	vertex = &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizeVertexProperties(props)}
	if err := tx.PutVertex(ctx, vertex); err != nil {
		return nil, err
	}
	return vertex, nil
}

func nextAutoVertexID(varName string) string {
	n := atomic.AddUint64(&autoVertexIDSeq, 1)
	if strings.TrimSpace(varName) == "" {
		return fmt.Sprintf("auto-v-%d", n)
	}
	return fmt.Sprintf("auto-%s-%d", varName, n)
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
		value, err := evalWriteValue(parts[1], params, row)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func evalWriteValue(raw string, params Params, row Row) (any, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "null") {
		return nil, nil
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
			switch typed := v.(type) {
			case *graph.Vertex:
				return typed.ID, nil
			case *graph.Edge:
				return typed.ID, nil
			default:
				return typed, nil
			}
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
	if n, err := strconv.Atoi(raw); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), nil)
}

func parseListLiteral(raw string, params Params, row Row) ([]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return []any{}, nil
	}
	parts := splitTopLevelCommaSeparated(body)
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		value, err := evalWriteValue(part, params, row)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func parseInlineMapLiteral(raw string, params Params, row Row) (map[string]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return map[string]any{}, nil
	}
	return parsePropertyMap(body, params, row)
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
	whole := int(truncTowardZero(seconds))
	frac := seconds - float64(whole)
	nanos := int(math.Round(frac * 1_000_000_000))
	if nanos >= 1_000_000_000 {
		whole++
		nanos -= 1_000_000_000
	}
	if nanos <= -1_000_000_000 {
		whole--
		nanos += 1_000_000_000
	}
	if nanos < 0 {
		nanos += 1_000_000_000
		whole--
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
	if v.hasDate && v.hasTime {
		if loc, err := time.LoadLocation(v.timezone); err == nil {
			t := time.Date(v.year, time.Month(v.month), v.day, v.hour, v.minute, v.second, v.nano, loc)
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(raw[i:], op) {
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

func numericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
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
	raw := durationComponents{
		months: 12*mapFloat(value, "years") + mapFloat(value, "months"),
		days:   7*mapFloat(value, "weeks") + mapFloat(value, "days"),
		seconds: 3600*mapFloat(value, "hours") + 60*mapFloat(value, "minutes") +
			mapFloat(value, "seconds") + mapFloat(value, "milliseconds")/1000 + mapFloat(value, "microseconds")/1_000_000 + mapFloat(value, "nanoseconds")/1_000_000_000,
	}
	return canonicalizeDurationComponents(raw)
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
		return dateAdjusted.Format("2006-01-02"), true
	case "localtime":
		return formatTimeString(adjusted, false), true
	case "time":
		return formatTimeString(adjusted, true), true
	case "localdatetime":
		return formatDateTimeString(adjusted, false), true
	case "datetime":
		return formatDateTimeString(adjusted, true), true
	default:
		return nil, false
	}
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
	hours := int(truncTowardZero(seconds / 3600))
	seconds -= float64(hours * 3600)
	minutes := int(truncTowardZero(seconds / 60))
	seconds -= float64(minutes * 60)
	secInt := int(truncTowardZero(seconds))
	frac := seconds - float64(secInt)
	nanos := int(math.Round(frac * 1_000_000_000))

	if nanos >= 1_000_000_000 {
		secInt++
		nanos -= 1_000_000_000
	}
	if nanos <= -1_000_000_000 {
		secInt--
		nanos += 1_000_000_000
	}
	if nanos != 0 && math.Abs(float64(nanos)) <= 1 {
		nanos = 0
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
		return strings.ReplaceAll(inner, "''", "'"), nil
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
		if strings.EqualFold(k, "id") {
			continue
		}
		out[k] = valueToBytes(v)
	}
	return out
}

func normalizeEdgeProperties(props map[string]any) graph.PropertyMap {
	out := graph.PropertyMap{}
	for k, v := range props {
		if strings.EqualFold(k, "id") {
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
