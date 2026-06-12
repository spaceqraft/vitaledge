package operators

import (
	"context"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

type MutationType string

const (
	MutationTypeUnknown  MutationType = "unknown"
	MutationTypeVertex   MutationType = "vertex"
	MutationTypeEdge     MutationType = "edge"
	MutationTypeProperty MutationType = "property"
)

// WriteEvent is the typed write-side payload emitted by PHY_WRITE.
type WriteEvent struct {
	NodeID         string
	Kind           string
	Raw            string
	Pattern        string
	MergePattern   string
	MergeOnCreate  []string
	MergeOnMatch   []string
	MutationType   MutationType
	Vertex         *VertexMutation
	Edge           *EdgeMutation
	Bindings       map[string]any
	ParamKeys      []string
	ResolvedParams map[string]any
}

type VertexMutation struct {
	Pattern string
	Var     string
	IDParam string
	Labels  []string
}

type EdgeMutation struct {
	Pattern      string
	Reverse      bool
	Var          string
	LeftVar      string
	RightVar     string
	LeftIDParam  string
	RightIDParam string
	Type         string
	LeftLabels   []string
	RightLabels  []string
}

// InQueryCallExecutor executes an in-query CALL clause over the current row set.
type InQueryCallExecutor func(ctx context.Context, inputRows []map[string]any, callRaw string, params map[string]any, inQuery bool) ([]map[string]any, []string, error)

// ExistsSubqueryEvaluator evaluates WHERE EXISTS { ... } subquery bodies against
// the current row scope.
type ExistsSubqueryEvaluator func(ctx context.Context, tx graph.Tx, body string, row map[string]any, params map[string]any) (bool, error)

// MetricsObserver is an optional executor-provided sink for runtime-side
// index observability.
type MetricsObserver interface {
	ObserveIndexCandidate(tenant, schema, property string, indexed bool)
	ObserveIndexLookup(strategy, outcome string, matches int)
	ObserveRuntimeCounter(name string, delta int64)
}

// State is mutable execution state shared across operator handlers.
type State struct {
	ExecutedOps              []string
	Rows                     []map[string]any
	SortSourceRows           []map[string]any
	WriteEvents              []WriteEvent
	EvalError                error
	Params                   map[string]any
	Tenant                   string
	Tx                       graph.Tx
	Context                  context.Context
	InQueryCallExecutor      InQueryCallExecutor
	ExistsSubqueryEvaluator  ExistsSubqueryEvaluator
	Metrics                  MetricsObserver
	MaterializeWriteBindings bool
	OperatorExecCount        int
}

// Handler executes a physical operator.
type Handler interface {
	OpName() string
	Execute(nodeID string, attrs map[string]any, state *State) error
}

// VariantHandler optionally executes named operator variants selected by the
// physical planner (for example, expand/sort/anti-probe variants).
type VariantHandler interface {
	Handler
	ExecuteVariant(nodeID string, variant string, attrs map[string]any, state *State) (handled bool, err error)
}
