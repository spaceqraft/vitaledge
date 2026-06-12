package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/spaceqraft/vitaledge/internal/cypher/runtime/operators"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

// Engine executes physical plans.
type Engine struct {
	handlers map[string]operators.Handler
}

// SupportedOps returns the normalized set of physical operator names currently
// registered on the engine.
func (e *Engine) SupportedOps() map[string]struct{} {
	if e == nil || len(e.handlers) == 0 {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(e.handlers))
	for name := range e.handlers {
		op := strings.TrimSpace(name)
		if op == "" {
			continue
		}
		out[op] = struct{}{}
	}
	return out
}

// SupportedPhysicalOps returns the runtime engine's supported physical
// operator set. This is the single source of truth for runtime plan support.
func SupportedPhysicalOps() map[string]struct{} {
	return New().SupportedOps()
}

// New creates a runtime engine instance.
func New() *Engine {
	e := &Engine{handlers: map[string]operators.Handler{}}
	for _, h := range []operators.Handler{
		operators.NewExpandMatchHandler(),
		operators.NewOptionalExpandHandler(),
		operators.NewEmptyHandler(),
		operators.NewAntiProbeHandler(),
		operators.NewCallHandler(),
		operators.NewFilterHandler(),
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
	return e.executeInternal(context.Background(), ctx, nil)
}

func (e *Engine) executeInternal(runCtx context.Context, ctx ExecutionContext, tx graph.Tx) (ExecutionResult, error) {
	if e == nil {
		e = New()
	}
	state := &OperatorState{
		ExecutedOps:              []string{},
		Rows:                     []map[string]any{},
		Params:                   ctx.Params,
		Tenant:                   ctx.Tenant,
		Tx:                       tx,
		Context:                  runCtx,
		InQueryCallExecutor:      inQueryCallExecutorParam(ctx.Params),
		ExistsSubqueryEvaluator:  existsSubqueryEvaluatorParam(ctx.Params),
		Metrics:                  metricsObserverParam(ctx.Params),
		MaterializeWriteBindings: boolParam(ctx.Params, MaterializeWriteBindingsParam, true),
	}
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
	strictVariantDispatch := boolParam(ctx.Params, StrictVariantDispatchParam, false)
	var visit func(string) error
	visit = func(id string) error {
		if id == "" || visited[id] {
			return nil
		}
		node, ok := nodesByID[id]
		if !ok {
			return nil
		}
		for _, child := range node.children {
			if err := visit(child); err != nil {
				return err
			}
		}
		visited[id] = true
		if node.op != "" {
			variant, _ := node.attrs["variant"].(string)
			variant = strings.TrimSpace(variant)
			if strictVariantDispatch && variant != "" && operators.IsVariantDispatchOp(node.op) && !operators.SupportsVariant(node.op, variant) {
				return fmt.Errorf("runtime strict variant dispatch rejected unsupported variant: op=%s variant=%s", node.op, variant)
			}
			handler, ok := e.handlers[node.op]
			if ok {
				writeEventsBefore := len(state.WriteEvents)
				if variantHandler, vok := handler.(operators.VariantHandler); vok {
					if variant != "" {
						handled, variantErr := variantHandler.ExecuteVariant(id, variant, node.attrs, state)
						if variantErr != nil {
							return variantErr
						}
						if handled {
							if tx != nil && node.op == "PHY_WRITE" && len(state.WriteEvents) > writeEventsBefore {
								if err := ApplyWriteEvents(runCtx, tx, state.Tenant, state.WriteEvents[writeEventsBefore:]); err != nil {
									return err
								}
							}
							return nil
						}
					}
				}
				if err := handler.Execute(id, node.attrs, state); err != nil {
					return err
				}
				if tx != nil && node.op == "PHY_WRITE" && len(state.WriteEvents) > writeEventsBefore {
					if err := ApplyWriteEvents(runCtx, tx, state.Tenant, state.WriteEvents[writeEventsBefore:]); err != nil {
						return err
					}
				}
			} else {
				state.ExecutedOps = append(state.ExecutedOps, node.op)
				state.OperatorExecCount++
			}
		}
		return nil
	}

	if err := visit(ctx.Plan.RootNodeID); err != nil {
		return result, err
	}
	if len(result.ExecutedOps) == 0 {
		for _, n := range ctx.Plan.Nodes {
			if err := visit(n.ID); err != nil {
				return result, err
			}
		}
	}

	result.ExecutedOps = append(result.ExecutedOps, state.ExecutedOps...)
	result.Rows = append(result.Rows, state.Rows...)
	result.WriteEvents = append(result.WriteEvents, state.WriteEvents...)
	result.Stats.OperatorsExecuted = state.OperatorExecCount
	result.Stats.WritesRecorded = len(state.WriteEvents)

	return result, nil
}

func boolParam(params map[string]any, key string, defaultValue bool) bool {
	if params == nil {
		return defaultValue
	}
	value, ok := params[key]
	if !ok || value == nil {
		return defaultValue
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		default:
			return defaultValue
		}
	default:
		return defaultValue
	}
}

func inQueryCallExecutorParam(params map[string]any) operators.InQueryCallExecutor {
	if params == nil {
		return nil
	}
	raw, ok := params[InQueryCallExecutorParam]
	if !ok || raw == nil {
		return nil
	}
	fn, _ := raw.(operators.InQueryCallExecutor)
	return fn
}

func metricsObserverParam(params map[string]any) operators.MetricsObserver {
	if params == nil {
		return nil
	}
	raw, ok := params[MetricsObserverParam]
	if !ok || raw == nil {
		return nil
	}
	observer, _ := raw.(operators.MetricsObserver)
	return observer
}

func existsSubqueryEvaluatorParam(params map[string]any) operators.ExistsSubqueryEvaluator {
	if params == nil {
		return nil
	}
	raw, ok := params[ExistsSubqueryEvaluatorParam]
	if !ok || raw == nil {
		return nil
	}
	fn, _ := raw.(operators.ExistsSubqueryEvaluator)
	return fn
}

// ExecuteWithTx runs the physical plan and applies surfaced write events
// through the provided transaction.
func (e *Engine) ExecuteWithTx(ctx context.Context, input ExecutionContext, tx graph.Tx) (ExecutionResult, error) {
	return e.executeInternal(ctx, input, tx)
}
