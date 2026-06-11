package runtime

import (
	"github.com/paegun/vitaledge/internal/cypher/pipeline"
	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
)

// ExecutionContext is the runtime input contract for physical plan execution.
type ExecutionContext = pipeline.PhysicalExecutionInput

// ExecutionResult is the runtime output contract used by runtime tests while
// the full executor integration is still being migrated.
type ExecutionResult struct {
	ExecutedOps []string
	Rows        []map[string]any
	WriteEvents []operators.WriteEvent
	Stats       ExecutionStats
}

// ExecutionStats captures lightweight runtime counters for the current
// scaffolded runtime engine.
type ExecutionStats struct {
	OperatorsExecuted int
	WritesRecorded    int
}

// MaterializeWriteBindingsParam toggles whether PHY_WRITE backfills row
// bindings for downstream projection operators.
const MaterializeWriteBindingsParam = "__ve_materialize_write_bindings"

// StrictVariantDispatchParam toggles fail-fast validation for variantized
// runtime operator families. When enabled, unsupported variant names for
// known variant-dispatch ops are rejected instead of falling back.
const StrictVariantDispatchParam = "__ve_strict_variant_dispatch"

// InQueryCallExecutorParam carries an injected in-query CALL executor callback.
const InQueryCallExecutorParam = "__ve_in_query_call_executor"

// ExistsSubqueryEvaluatorParam carries an injected WHERE EXISTS { ... }
// evaluator callback.
const ExistsSubqueryEvaluatorParam = "__ve_exists_subquery_evaluator"

// MetricsObserverParam carries an injected metrics sink for runtime operators.
const MetricsObserverParam = "__ve_metrics_observer"

// InQueryCallExecutor bridges runtime PHY_CALL execution back to executor-level
// procedure semantics without introducing package cycles.
type InQueryCallExecutor = operators.InQueryCallExecutor

// ExistsSubqueryEvaluator bridges runtime WHERE EXISTS evaluation back to
// executor-level semantics without introducing package cycles.
type ExistsSubqueryEvaluator = operators.ExistsSubqueryEvaluator

// OperatorState is mutable execution state shared across operator handlers.
type OperatorState = operators.State

// WriteEvent is the typed write-side runtime payload.
type WriteEvent = operators.WriteEvent
