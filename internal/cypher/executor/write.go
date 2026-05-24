package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
	if left, right, ok := splitTopLevelOperator(raw, "="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
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
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf > rf, nil
		}
		ls := fmt.Sprint(lhs)
		rs := fmt.Sprint(rhs)
		return ls > rs, nil
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
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return lf < rf, nil
		}
		ls := fmt.Sprint(lhs)
		rs := fmt.Sprint(rhs)
		return ls < rs, nil
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
		return fmt.Sprint(value), nil
	}
	if arg, ok := parseFunctionCall(raw, "date.truncate"); ok {
		return evalTemporalTruncateFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "time.truncate"); ok {
		return evalTemporalTruncateFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "datetime.truncate"); ok {
		return evalTemporalTruncateFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localtime.truncate"); ok {
		return evalTemporalTruncateFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localdatetime.truncate"); ok {
		return evalTemporalTruncateFunction(arg, row, params)
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
			if value, ok := typed[field]; ok {
				return value, nil
			}
			return nil, nil
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

	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "NOT") {
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
			return raw[:i], raw[i+len(keyword):], true
		}
	}
	return raw, "", false
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
		for key, v := range typed {
			out[key] = v
		}
	case string:
		out["value"] = typed
	default:
		out["value"] = typed
	}
	return out, nil
}

func evalTemporalTruncateFunction(argList string, row Row, params Params) (any, error) {
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
		out["truncated"] = fmt.Sprint(unit)
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
		value, err := evalTemporalTruncateFunction(argsRaw, row, params)
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

	if namespace == "duration" && (strings.EqualFold(method, "indays") || strings.EqualFold(method, "inmonths") || strings.EqualFold(method, "inseconds") || strings.EqualFold(method, "between")) {
		return map[string]any{"__temporal_type": "duration", "method": method, "args": args}, true, nil
	}

	return map[string]any{"__temporal_type": namespace, "method": method, "args": args}, true, nil
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
