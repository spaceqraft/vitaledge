package executor

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
)

var (
	createVertexPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternRE   = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)(?:\{([^{}]*)\})?\]->\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	setClauseRE           = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	removeClauseRE        = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)
	deleteClauseRE        = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)$`)
)

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
				return graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("clause %s is not yet supported", clause.Kind), nil)
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
	if node, err := parseNodePattern(spec.Pattern); err == nil {
		return e.expandNodeMatch(ctx, tx, rows, spec, node, params)
	}
	return e.expandAnchoredMatch(ctx, tx, rows, spec, params)
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
					ok, err := evalWhereExpression(spec.Where, merged, params)
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
				ok, err := evalWhereExpression(spec.Where, merged, params)
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
	if m := createEdgePatternRE.FindStringSubmatch(raw); len(m) == 9 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
}

func (e *Executor) applyCreateVertex(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool) ([]Row, error) {
	varName := m[1]
	labels := splitLabels(m[2])

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		props, err := parsePropertyMap(m[3], params, row)
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
		merged[varName] = vertex
		out = append(out, merged)
	}
	return out, nil
}

func (e *Executor) applyCreateEdge(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool) ([]Row, error) {
	srcVar := m[1]
	srcLabels := splitLabels(m[2])
	edgeType := m[4]
	dstVar := m[6]
	dstLabels := splitLabels(m[7])

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		srcProps, err := parsePropertyMap(m[3], params, row)
		if err != nil {
			return nil, err
		}
		edgeProps, err := parsePropertyMap(m[5], params, row)
		if err != nil {
			return nil, err
		}
		dstProps, err := parsePropertyMap(m[8], params, row)
		if err != nil {
			return nil, err
		}
		srcVertex, err := resolveOrCreateVertex(ctx, tx, tenant, row, srcVar, srcLabels, srcProps, merge)
		if err != nil {
			return nil, err
		}
		dstVertex, err := resolveOrCreateVertex(ctx, tx, tenant, row, dstVar, dstLabels, dstProps, merge)
		if err != nil {
			return nil, err
		}

		edgeID := syntheticEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID)
		edge := &graph.Edge{Tenant: srcVertex.Tenant, ID: edgeID, Type: edgeType, SrcID: srcVertex.ID, DstID: dstVertex.ID, Properties: normalizeEdgeProperties(edgeProps)}
		if merge {
			if existing, err := tx.GetEdge(ctx, edge.Tenant, edge.ID); err == nil {
				edge = existing
			}
		}
		if err := tx.PutEdge(ctx, edge); err != nil {
			return nil, err
		}

		merged := cloneRow(row)
		merged[srcVar] = srcVertex
		merged[dstVar] = dstVertex
		merged["edge"] = edge
		out = append(out, merged)
	}
	return out, nil
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
	items, err := parseProjectionItems(raw)
	if err != nil {
		return nil, nil, err
	}
	if len(items) == 0 {
		return nil, nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("%s clause requires at least one projection item", clause.Kind), nil)
	}

	out := make([]Row, 0, len(rows))
	columns := make([]string, 0, len(items))
	for _, item := range items {
		if item.Alias != "" {
			columns = append(columns, item.Alias)
		} else {
			columns = append(columns, item.Expression)
		}
	}

	for _, row := range rows {
		projected := Row{}
		for _, item := range items {
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

	return out, columns, nil
}

type projectionSpec struct {
	Expression string
	Alias      string
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
			return nil, graph.NewError(graph.ErrKindUnsupported, "projection * is not yet supported", nil)
		}
		alias := ""
		if idx := strings.LastIndex(strings.ToUpper(part), "AS"); idx > 0 {
			expr := strings.TrimSpace(part[:idx])
			alias = strings.TrimSpace(part[idx+2:])
			if expr == "" || alias == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection item %q is not supported", part), nil)
			}
			items = append(items, projectionSpec{Expression: expr, Alias: alias})
			continue
		}
		items = append(items, projectionSpec{Expression: part})
	}
	return items, nil
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
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("field access not supported on %T", base), nil)
		}
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func evalWhereExpression(raw string, row Row, params Params) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, graph.NewError(graph.ErrKindSemantic, "empty WHERE expression", nil)
	}

	if left, right, ok := splitTopLevelKeyword(raw, "OR"); ok {
		lhs, err := evalWhereExpression(left, row, params)
		if err != nil {
			return false, err
		}
		if lhs {
			return true, nil
		}
		return evalWhereExpression(right, row, params)
	}

	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		lhs, err := evalWhereExpression(left, row, params)
		if err != nil {
			return false, err
		}
		if !lhs {
			return false, nil
		}
		return evalWhereExpression(right, row, params)
	}

	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "NOT") {
		value, err := evalWhereExpression(strings.TrimSpace(raw[3:]), row, params)
		if err != nil {
			return false, err
		}
		return !value, nil
	}

	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return evalWhereExpression(raw[1:len(raw)-1], row, params)
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
	for _, pair := range strings.Split(raw, ",") {
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
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		v, ok := params[name]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		return v, nil
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
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), nil)
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
