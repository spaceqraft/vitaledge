package runtime

import (
	"context"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	"github.com/paegun/vitaledge/internal/graph"
)

// Engine executes physical plans.
type Engine struct {
	handlers map[string]operators.Handler
}

// New creates a runtime engine instance.
func New() *Engine {
	e := &Engine{handlers: map[string]operators.Handler{}}
	for _, h := range []operators.Handler{
		operators.NewExpandMatchHandler(),
		operators.NewOptionalExpandHandler(),
		operators.NewProjectHandler(),
		operators.NewSortHandler(),
		operators.NewPaginationHandler(),
		operators.NewWriteHandler(),
	} {
		e.RegisterHandler(h)
	}
	return e
}

// RegisterHandler registers or replaces a runtime operator handler.
func (e *Engine) RegisterHandler(handler operators.Handler) {
	if e == nil || handler == nil {
		return
	}
	name := strings.TrimSpace(handler.OpName())
	if name == "" {
		return
	}
	if e.handlers == nil {
		e.handlers = map[string]operators.Handler{}
	}
	e.handlers[name] = handler
}

// Execute runs the physical plan in a deterministic post-order traversal of
// node dependencies, recording executed operators.
func (e *Engine) Execute(ctx ExecutionContext) (ExecutionResult, error) {
	if e == nil {
		e = New()
	}
	state := &OperatorState{ExecutedOps: []string{}, Rows: []map[string]any{}, Params: ctx.Params}
	result := ExecutionResult{
		ExecutedOps: []string{},
		Rows:        []map[string]any{},
		Stats:       ExecutionStats{},
	}
	if len(ctx.Plan.Nodes) == 0 {
		return result, nil
	}

	nodesByID := map[string]struct {
		op       string
		children []string
		attrs    map[string]any
	}{}
	for _, n := range ctx.Plan.Nodes {
		nodesByID[n.ID] = struct {
			op       string
			children []string
			attrs    map[string]any
		}{
			op:       strings.TrimSpace(n.Op),
			children: append([]string(nil), n.Children...),
			attrs:    n.Attrs,
		}
	}

	visited := map[string]bool{}
	var visit func(string)
	visit = func(id string) {
		if id == "" || visited[id] {
			return
		}
		node, ok := nodesByID[id]
		if !ok {
			return
		}
		for _, child := range node.children {
			visit(child)
		}
		visited[id] = true
		if node.op != "" {
			handler, ok := e.handlers[node.op]
			if ok {
				_ = handler.Execute(id, node.attrs, state)
			} else {
				state.ExecutedOps = append(state.ExecutedOps, node.op)
				state.OperatorExecCount++
			}
		}
	}

	visit(ctx.Plan.RootNodeID)
	if len(result.ExecutedOps) == 0 {
		for _, n := range ctx.Plan.Nodes {
			visit(n.ID)
		}
	}

	result.ExecutedOps = append(result.ExecutedOps, state.ExecutedOps...)
	result.Rows = append(result.Rows, state.Rows...)
	result.WriteEvents = append(result.WriteEvents, state.WriteEvents...)
	result.Stats.OperatorsExecuted = state.OperatorExecCount
	result.Stats.WritesRecorded = len(state.WriteEvents)

	return result, nil
}

// ExecuteWithTx runs the physical plan and applies surfaced write events
// through the provided transaction.
func (e *Engine) ExecuteWithTx(ctx context.Context, input ExecutionContext, tx graph.Tx) (ExecutionResult, error) {
	result, err := e.Execute(input)
	if err != nil {
		return result, err
	}
	if err := ApplyWriteEvents(ctx, tx, input.Tenant, result.WriteEvents); err != nil {
		return result, err
	}
	return result, nil
}
