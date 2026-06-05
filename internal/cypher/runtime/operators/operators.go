package operators

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
	LeftVar      string
	RightVar     string
	LeftIDParam  string
	RightIDParam string
	Type         string
	LeftLabels   []string
	RightLabels  []string
}

// State is mutable execution state shared across operator handlers.
type State struct {
	ExecutedOps       []string
	Rows              []map[string]any
	WriteEvents       []WriteEvent
	Params            map[string]any
	OperatorExecCount int
}

// Handler executes a physical operator.
type Handler interface {
	OpName() string
	Execute(nodeID string, attrs map[string]any, state *State) error
}
