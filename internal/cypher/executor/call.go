package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/graph"
)

type callSpec struct {
	Name         string
	ArgExprs     []string
	ImplicitArgs bool
	YieldAll     bool
	YieldItems   []yieldItem
}

type yieldItem struct {
	Field string
	Alias string
}

type builtinProcedureHandler func(ctx context.Context, args []any, params Params) ([]Row, error)

type resolvedProcedure struct {
	decl    ProcedureDecl
	handler builtinProcedureHandler
}

func (e *Executor) executeStandaloneCallStatement(ctx context.Context, stmt *ast.StandaloneCallStatement, params Params) (*Result, error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "standalone call statement is required", nil)
	}
	spec, err := parseCallClauseRaw(stmt.Call.Raw)
	if err != nil {
		return nil, err
	}
	rows, columns, err := e.executeProcedureCall(ctx, []Row{{}}, spec, params, false)
	if err != nil {
		return nil, err
	}
	rows = normalizeResultRows(rows)
	return &Result{Columns: columns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}, nil
}

func (e *Executor) applyInQueryCallClause(ctx context.Context, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	spec, err := parseCallClauseRaw(clause.Raw)
	if err != nil {
		return nil, err
	}
	resultRows, _, err := e.executeProcedureCall(ctx, rows, spec, params, true)
	if err != nil {
		return nil, err
	}
	return resultRows, nil
}

func (e *Executor) executeProcedureCall(ctx context.Context, inputRows []Row, spec callSpec, params Params, inQuery bool) ([]Row, []string, error) {
	resolved, ok := e.resolveProcedure(spec.Name, params)
	if !ok {
		return nil, nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("procedure %q not found", spec.Name), nil)
	}
	if resolved.handler != nil {
		return e.executeBuiltinProcedureCall(ctx, inputRows, spec, resolved, params, inQuery)
	}
	decl := resolved.decl

	if err := validateCallSpec(spec, decl, inQuery); err != nil {
		return nil, nil, err
	}

	if inQuery && spec.ImplicitArgs && len(decl.Inputs) > 0 {
		return nil, nil, graph.NewError(graph.ErrKindSemantic, "invalid argument passing mode", nil)
	}

	if !inQuery && len(inputRows) == 0 {
		inputRows = []Row{{}}
	}
	if len(inputRows) == 0 {
		return []Row{}, selectedColumns(decl, spec), nil
	}

	selectedOutputs, outColumns, err := resolveYieldSelection(decl, spec, inQuery, inputRows)
	if err != nil {
		return nil, nil, err
	}

	resultRows := make([]Row, 0)
	for _, row := range inputRows {
		args, argErr := evaluateCallArgs(spec, decl, row, params, inQuery)
		if argErr != nil {
			return nil, nil, argErr
		}
		procRows, callErr := e.executeProcedureRows(ctx, resolved, args, params)
		if callErr != nil {
			return nil, nil, callErr
		}
		for _, procRow := range procRows {
			if inQuery {
				merged := cloneRow(row)
				for outName, alias := range selectedOutputs {
					merged[alias] = procRow[outName]
				}
				resultRows = append(resultRows, merged)
				continue
			}
			if len(selectedOutputs) == 0 {
				continue
			}
			out := Row{}
			for outName, alias := range selectedOutputs {
				out[alias] = procRow[outName]
			}
			resultRows = append(resultRows, out)
		}
	}

	return resultRows, outColumns, nil
}

func (e *Executor) executeBuiltinProcedureCall(ctx context.Context, inputRows []Row, spec callSpec, proc resolvedProcedure, params Params, inQuery bool) ([]Row, []string, error) {
	if inQuery && spec.ImplicitArgs {
		return nil, nil, graph.NewError(graph.ErrKindSemantic, "invalid argument passing mode", nil)
	}
	for _, arg := range spec.ArgExprs {
		if strings.Contains(strings.ToLower(arg), "count(") {
			return nil, nil, graph.NewError(graph.ErrKindSemantic, "invalid aggregation in procedure argument", nil)
		}
	}

	if !inQuery && len(inputRows) == 0 {
		inputRows = []Row{{}}
	}
	if len(inputRows) == 0 {
		return []Row{}, selectedColumns(proc.decl, spec), nil
	}

	selectedOutputs, outColumns, err := resolveYieldSelection(proc.decl, spec, inQuery, inputRows)
	if err != nil {
		return nil, nil, err
	}

	resultRows := make([]Row, 0)
	for _, row := range inputRows {
		args := make([]any, 0, len(spec.ArgExprs))
		for _, argExpr := range spec.ArgExprs {
			value, evalErr := evalExpressionWithScope(argExpr, row, params)
			if evalErr != nil {
				return nil, nil, evalErr
			}
			args = append(args, value)
		}

		procRows, callErr := e.executeProcedureRows(ctx, proc, args, params)
		if callErr != nil {
			return nil, nil, callErr
		}
		for _, procRow := range procRows {
			if inQuery {
				merged := cloneRow(row)
				for outName, alias := range selectedOutputs {
					merged[alias] = procRow[outName]
				}
				resultRows = append(resultRows, merged)
				continue
			}
			if len(selectedOutputs) == 0 {
				continue
			}
			out := Row{}
			for outName, alias := range selectedOutputs {
				out[alias] = procRow[outName]
			}
			resultRows = append(resultRows, out)
		}
	}

	return resultRows, outColumns, nil
}

func (e *Executor) executeProcedureRows(ctx context.Context, proc resolvedProcedure, args []any, params Params) ([]Row, error) {
	if proc.handler != nil {
		return proc.handler(ctx, args, params)
	}
	return executeProcedureRows(proc.decl, args)
}

func (e *Executor) resolveProcedure(name string, params Params) (resolvedProcedure, bool) {
	if builtin, ok := e.resolveBuiltinProcedure(name); ok {
		return builtin, true
	}
	decls := procedureDeclsFromParams(params)
	decl, ok := decls[name]
	if !ok {
		return resolvedProcedure{}, false
	}
	return resolvedProcedure{decl: decl}, true
}

func (e *Executor) resolveBuiltinProcedure(name string) (resolvedProcedure, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "db.stats.edgecount":
		return resolvedProcedure{
			decl: ProcedureDecl{
				Name:    "db.stats.edgeCount",
				Outputs: []ProcedureArg{{Name: "edgeCount", Type: "INTEGER", Nullable: false}},
			},
			handler: e.builtinEdgeCountProcedure,
		}, true
	case "db.stats.vertexcount":
		return resolvedProcedure{
			decl: ProcedureDecl{
				Name:    "db.stats.vertexCount",
				Outputs: []ProcedureArg{{Name: "vertexCount", Type: "INTEGER", Nullable: false}},
			},
			handler: e.builtinVertexCountProcedure,
		}, true
	default:
		return resolvedProcedure{}, false
	}
}

func (e *Executor) builtinEdgeCountProcedure(ctx context.Context, args []any, params Params) ([]Row, error) {
	if e == nil || e.store == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	label, err := parseOptionalLabelArg(args)
	if err != nil {
		return nil, err
	}
	if label == "" {
		var edgeTotal int
		err = e.store.View(ctx, func(tx graph.Tx) error {
			snapshot, snapshotErr := tx.GetStatsSnapshot(ctx, tenant)
			if snapshotErr != nil {
				return snapshotErr
			}
			edgeTotal = snapshot.EdgeTotal
			return nil
		})
		if err == nil {
			return []Row{{"edgeCount": edgeTotal}}, nil
		}
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
	}

	edgeIDs := map[string]struct{}{}
	err = e.store.View(ctx, func(tx graph.Tx) error {
		type edgeIDScannerTx interface {
			ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) error
			ScanInEdgeIDs(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(string) error) error
		}

		if label == "" {
			if scanner, ok := tx.(edgeIDScannerTx); ok {
				return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
					if vertex == nil {
						return nil
					}
					return scanner.ScanOutEdgeIDs(ctx, tenant, vertex.ID, "", 0, func(edgeID string) error {
						edgeIDs[edgeID] = struct{}{}
						return nil
					})
				})
			}
			return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
				if vertex == nil {
					return nil
				}
				return tx.ScanOutEdges(ctx, tenant, vertex.ID, "", 0, func(edge *graph.Edge) error {
					if edge != nil {
						edgeIDs[edge.ID] = struct{}{}
					}
					return nil
				})
			})
		}

		if scanner, ok := tx.(edgeIDScannerTx); ok {
			return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
				if !vertexHasLabel(vertex, label) {
					return nil
				}
				if err := scanner.ScanOutEdgeIDs(ctx, tenant, vertex.ID, "", 0, func(edgeID string) error {
					edgeIDs[edgeID] = struct{}{}
					return nil
				}); err != nil {
					return err
				}
				return scanner.ScanInEdgeIDs(ctx, tenant, vertex.ID, "", 0, func(edgeID string) error {
					edgeIDs[edgeID] = struct{}{}
					return nil
				})
			})
		}

		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if !vertexHasLabel(vertex, label) {
				return nil
			}
			if err := tx.ScanOutEdges(ctx, tenant, vertex.ID, "", 0, func(edge *graph.Edge) error {
				if edge != nil {
					edgeIDs[edge.ID] = struct{}{}
				}
				return nil
			}); err != nil {
				return err
			}
			return tx.ScanInEdges(ctx, tenant, vertex.ID, "", 0, func(edge *graph.Edge) error {
				if edge != nil {
					edgeIDs[edge.ID] = struct{}{}
				}
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}

	return []Row{{"edgeCount": len(edgeIDs)}}, nil
}

func (e *Executor) builtinVertexCountProcedure(ctx context.Context, args []any, params Params) ([]Row, error) {
	if e == nil || e.store == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	label, err := parseOptionalLabelArg(args)
	if err != nil {
		return nil, err
	}
	if label == "" {
		var vertexTotal int
		err = e.store.View(ctx, func(tx graph.Tx) error {
			snapshot, snapshotErr := tx.GetStatsSnapshot(ctx, tenant)
			if snapshotErr != nil {
				return snapshotErr
			}
			vertexTotal = snapshot.VertexTotal
			return nil
		})
		if err == nil {
			return []Row{{"vertexCount": vertexTotal}}, nil
		}
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
	} else {
		var labelTotal int
		err = e.store.View(ctx, func(tx graph.Tx) error {
			snapshot, snapshotErr := tx.GetStatsSnapshot(ctx, tenant)
			if snapshotErr != nil {
				return snapshotErr
			}
			labelTotal = snapshot.LabelCounts[label]
			return nil
		})
		if err == nil {
			return []Row{{"vertexCount": labelTotal}}, nil
		}
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
	}

	count := 0
	err = e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if vertex == nil {
				return nil
			}
			if label != "" && !vertexHasLabel(vertex, label) {
				return nil
			}
			count++
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return []Row{{"vertexCount": count}}, nil
}

func parseOptionalLabelArg(args []any) (string, error) {
	if len(args) > 1 {
		return "", graph.NewError(graph.ErrKindSemantic, "invalid number of arguments", nil)
	}
	if len(args) == 0 || args[0] == nil {
		return "", nil
	}
	s, ok := args[0].(string)
	if !ok {
		return "", graph.NewError(graph.ErrKindSemantic, "invalid argument type for \"label\"", nil)
	}
	return strings.TrimSpace(s), nil
}

func parseCallClauseRaw(raw string) (callSpec, error) {
	compact := normalizeClauseBody(raw)
	upper := strings.ToUpper(compact)
	if !strings.HasPrefix(upper, "CALL") {
		return callSpec{}, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("invalid CALL clause %q", raw), nil)
	}
	body := compact[len("CALL"):]
	yieldIdx := strings.Index(strings.ToUpper(body), "YIELD")
	callPart := body
	yieldPart := ""
	if yieldIdx >= 0 {
		callPart = body[:yieldIdx]
		yieldPart = body[yieldIdx+len("YIELD"):]
	}

	spec, err := parseCallInvocation(callPart)
	if err != nil {
		return callSpec{}, err
	}
	if yieldPart != "" {
		spec.YieldAll, spec.YieldItems, err = parseYieldItems(yieldPart)
		if err != nil {
			return callSpec{}, err
		}
	}
	return spec, nil
}

func parseCallInvocation(raw string) (callSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return callSpec{}, graph.NewError(graph.ErrKindSemantic, "missing procedure name", nil)
	}
	if strings.HasSuffix(raw, "()") {
		return callSpec{Name: strings.TrimSpace(raw[:len(raw)-2]), ArgExprs: []string{}}, nil
	}
	open := strings.Index(raw, "(")
	if open < 0 {
		return callSpec{Name: raw, ImplicitArgs: true}, nil
	}
	if !strings.HasSuffix(raw, ")") || open == len(raw)-1 {
		return callSpec{}, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("invalid procedure invocation %q", raw), nil)
	}
	name := strings.TrimSpace(raw[:open])
	argsRaw := strings.TrimSpace(raw[open+1 : len(raw)-1])
	args := []string{}
	if argsRaw != "" {
		parts := splitTopLevelCommaSeparated(argsRaw)
		args = make([]string, 0, len(parts))
		for _, part := range parts {
			args = append(args, strings.TrimSpace(part))
		}
	}
	return callSpec{Name: name, ArgExprs: args}, nil
}

func parseYieldItems(raw string) (bool, []yieldItem, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, nil, graph.NewError(graph.ErrKindSemantic, "empty YIELD clause", nil)
	}
	if raw == "*" {
		return true, nil, nil
	}
	parts := splitTopLevelCommaSeparated(raw)
	items := make([]yieldItem, 0, len(parts))
	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		item := yieldItem{}
		if idx := strings.Index(strings.ToUpper(entry), "AS"); idx >= 0 {
			item.Field = strings.TrimSpace(entry[:idx])
			item.Alias = strings.TrimSpace(entry[idx+2:])
		} else {
			item.Field = entry
			item.Alias = entry
		}
		if item.Field == "" || item.Alias == "" {
			return false, nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("invalid YIELD item %q", entry), nil)
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return false, nil, graph.NewError(graph.ErrKindSemantic, "empty YIELD clause", nil)
	}
	return false, items, nil
}

func resolveYieldSelection(decl ProcedureDecl, spec callSpec, inQuery bool, inputRows []Row) (map[string]string, []string, error) {
	declaredOut := map[string]ProcedureArg{}
	for _, out := range decl.Outputs {
		declaredOut[out.Name] = out
	}

	selected := map[string]string{}
	columns := []string{}

	if spec.YieldAll {
		if inQuery {
			return nil, nil, graph.NewError(graph.ErrKindSemantic, "YIELD * is not allowed in in-query CALL", nil)
		}
		for _, out := range decl.Outputs {
			selected[out.Name] = out.Name
			columns = append(columns, out.Name)
		}
		return selected, columns, nil
	}

	if len(spec.YieldItems) == 0 {
		if len(decl.Outputs) > 0 {
			if inQuery {
				return nil, nil, graph.NewError(graph.ErrKindSemantic, "procedure outputs must be yielded in in-query CALL", nil)
			}
			for _, out := range decl.Outputs {
				selected[out.Name] = out.Name
				columns = append(columns, out.Name)
			}
			return selected, columns, nil
		}
		return selected, columns, nil
	}

	seenAlias := map[string]struct{}{}
	bound := map[string]struct{}{}
	if inQuery {
		for _, row := range inputRows {
			for name := range row {
				bound[name] = struct{}{}
			}
		}
	}
	for _, item := range spec.YieldItems {
		if _, ok := declaredOut[item.Field]; !ok {
			return nil, nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown procedure output %q", item.Field), nil)
		}
		if _, ok := seenAlias[item.Alias]; ok {
			return nil, nil, graph.NewError(graph.ErrKindSemantic, "yield variable already bound", nil)
		}
		if _, ok := bound[item.Alias]; ok {
			return nil, nil, graph.NewError(graph.ErrKindSemantic, "yield variable already bound", nil)
		}
		seenAlias[item.Alias] = struct{}{}
		selected[item.Field] = item.Alias
		columns = append(columns, item.Alias)
	}

	return selected, columns, nil
}

func evaluateCallArgs(spec callSpec, decl ProcedureDecl, row Row, params Params, inQuery bool) ([]any, error) {
	if spec.ImplicitArgs {
		if inQuery {
			return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument passing mode", nil)
		}
		args := make([]any, 0, len(decl.Inputs))
		for _, input := range decl.Inputs {
			if value, ok := params[input.Name]; ok {
				if err := validateProcedureArg(input, value); err != nil {
					return nil, err
				}
				args = append(args, value)
				continue
			}
			keys := make([]string, 0, len(params))
			for key := range params {
				keys = append(keys, key)
			}
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q (available: %s)", input.Name, strings.Join(keys, ",")), nil)
		}
		return args, nil
	}

	if len(spec.ArgExprs) != len(decl.Inputs) {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid number of arguments", nil)
	}

	args := make([]any, 0, len(spec.ArgExprs))
	for idx, argExpr := range spec.ArgExprs {
		value, err := evalExpressionWithScope(argExpr, row, params)
		if err != nil {
			return nil, err
		}
		if err := validateProcedureArg(decl.Inputs[idx], value); err != nil {
			return nil, err
		}
		args = append(args, value)
	}
	return args, nil
}

func validateCallSpec(spec callSpec, decl ProcedureDecl, inQuery bool) error {
	if inQuery && spec.ImplicitArgs && len(decl.Inputs) > 0 {
		return graph.NewError(graph.ErrKindSemantic, "invalid argument passing mode", nil)
	}
	if !spec.ImplicitArgs && len(spec.ArgExprs) != len(decl.Inputs) {
		return graph.NewError(graph.ErrKindSemantic, "invalid number of arguments", nil)
	}
	for _, arg := range spec.ArgExprs {
		if strings.Contains(strings.ToLower(arg), "count(") {
			return graph.NewError(graph.ErrKindSemantic, "invalid aggregation in procedure argument", nil)
		}
	}
	return nil
}

func validateProcedureArg(arg ProcedureArg, value any) error {
	if value == nil {
		if arg.Nullable {
			return nil
		}
		return graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("procedure argument %q does not allow null", arg.Name), nil)
	}

	typ := strings.ToUpper(strings.TrimSpace(arg.Type))
	switch typ {
	case "", "ANY":
		return nil
	case "INTEGER":
		if isIntegerValue(value) {
			return nil
		}
	case "FLOAT":
		if isFloatValue(value) || isIntegerValue(value) {
			return nil
		}
	case "NUMBER":
		if isIntegerValue(value) || isFloatValue(value) {
			return nil
		}
	case "STRING":
		if _, ok := value.(string); ok {
			return nil
		}
	case "BOOLEAN":
		if _, ok := value.(bool); ok {
			return nil
		}
	default:
		return nil
	}
	return graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("invalid argument type for %q", arg.Name), nil)
}

func executeProcedureRows(decl ProcedureDecl, args []any) ([]Row, error) {
	if len(decl.Rows) == 0 {
		return []Row{{}}, nil
	}
	matched := make([]Row, 0)
	for _, entry := range decl.Rows {
		if !procedureEntryMatchesInputs(entry, decl.Inputs, args) {
			continue
		}
		row := Row{}
		for _, out := range decl.Outputs {
			row[out.Name] = entry[out.Name]
		}
		matched = append(matched, row)
	}
	return matched, nil
}

func procedureEntryMatchesInputs(entry map[string]any, inputs []ProcedureArg, args []any) bool {
	if len(inputs) != len(args) {
		return false
	}
	for i, input := range inputs {
		if !procedureValueEqual(entry[input.Name], args[i]) {
			return false
		}
	}
	return true
}

func procedureValueEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	if isIntegerValue(a) && isIntegerValue(b) {
		return toInt64(a) == toInt64(b)
	}
	if (isIntegerValue(a) || isFloatValue(a)) && (isIntegerValue(b) || isFloatValue(b)) {
		return toFloat64(a) == toFloat64(b)
	}
	return reflect.DeepEqual(normalizeResultValue(a), normalizeResultValue(b))
}

func isIntegerValue(v any) bool {
	switch v := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case json.Number:
		_, err := v.Int64()
		return err == nil
	default:
		return false
	}
}

func isFloatValue(v any) bool {
	switch v := v.(type) {
	case float32, float64:
		return true
	case json.Number:
		num := v
		if _, err := num.Int64(); err == nil {
			return false
		}
		_, err := num.Float64()
		return err == nil
	default:
		return false
	}
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	case json.Number:
		f, err := n.Float64()
		if err == nil {
			return f
		}
		return 0
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

func selectedColumns(decl ProcedureDecl, spec callSpec) []string {
	if len(spec.YieldItems) > 0 {
		cols := make([]string, 0, len(spec.YieldItems))
		for _, item := range spec.YieldItems {
			cols = append(cols, item.Alias)
		}
		return cols
	}
	if !spec.YieldAll {
		cols := make([]string, 0, len(decl.Outputs))
		for _, out := range decl.Outputs {
			cols = append(cols, out.Name)
		}
		return cols
	}
	return nil
}

func procedureDeclsFromParams(params Params) map[string]ProcedureDecl {
	if params == nil {
		return map[string]ProcedureDecl{}
	}
	raw, ok := params[ProcedureDeclsParam]
	if !ok || raw == nil {
		return map[string]ProcedureDecl{}
	}
	switch typed := raw.(type) {
	case map[string]ProcedureDecl:
		return typed
	case map[string]*ProcedureDecl:
		out := map[string]ProcedureDecl{}
		for name, decl := range typed {
			if decl == nil {
				continue
			}
			out[name] = *decl
		}
		return out
	default:
		return map[string]ProcedureDecl{}
	}
}
