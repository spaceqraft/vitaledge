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

// OperatorState is mutable execution state shared across operator handlers.
type OperatorState = operators.State

// WriteEvent is the typed write-side runtime payload.
type WriteEvent = operators.WriteEvent
