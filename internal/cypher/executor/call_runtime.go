package executor

import "context"

func (e *Executor) executeRuntimeInQueryCall(ctx context.Context, inputRows []map[string]any, callRaw string, runtimeParams map[string]any, inQuery bool) ([]map[string]any, []string, error) {
	if e == nil {
		return nil, nil, nil
	}
	spec, err := parseCallClauseRaw(callRaw)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]Row, 0, len(inputRows))
	for _, row := range inputRows {
		next := Row{}
		for key, value := range row {
			next[key] = value
		}
		rows = append(rows, next)
	}
	params := Params{}
	for key, value := range runtimeParams {
		params[key] = value
	}
	outRows, columns, err := e.executeProcedureCall(ctx, rows, spec, params, inQuery)
	if err != nil {
		return nil, nil, err
	}
	converted := make([]map[string]any, 0, len(outRows))
	for _, row := range outRows {
		next := map[string]any{}
		for key, value := range row {
			next[key] = value
		}
		converted = append(converted, next)
	}
	return converted, columns, nil
}
