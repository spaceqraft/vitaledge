package pipeline

import "github.com/paegun/vitaledge/internal/cypher/ast"

// ParseOutput is the explicit handoff contract from parse to semantic validation.
//
// QP-0 baseline: parse produces typed AST plus source/ordering metadata and must
// not perform execution-time rewrites.
type ParseOutput struct {
	Statement ast.Statement
}

// SemanticModel is the explicit handoff contract from semantic validation to
// logical planning.
//
// QP-0 baseline: semantic validation resolves scope and clause intent into
// structured forms that later stages can consume without raw-text recovery.
type SemanticModel struct {
	StatementKind ast.StatementKind
	Projections   []ProjectionIntent
	Ordering      []OrderingIntent
	Pagination    PaginationIntent
	Patterns      []PatternIntent
	Calls         []CallIntent
	WriteActions  []WriteActionIntent
}

// ProjectionItemIntent carries one projected expression and optional alias.
type ProjectionItemIntent struct {
	Expression string
	Alias      string
}

// ProjectionIntent carries semantic projection details for WITH/RETURN forms.
type ProjectionIntent struct {
	Ordinal    int
	Kind       ast.ClauseKind
	Distinct   bool
	IncludeAll bool
	Items      []ProjectionItemIntent
	WhereExpr  string
	OrderBy    []OrderingIntent
	Pagination PaginationIntent
}

// OrderingIntent carries semantic ORDER BY details.
type OrderingIntent struct {
	Expression string
	Direction  ast.SortDirection
}

// PaginationIntent carries semantic SKIP/LIMIT details.
type PaginationIntent struct {
	SkipExpr  string
	LimitExpr string
}

// PatternIntent carries semantic MATCH/OPTIONAL MATCH pattern details.
type PatternIntent struct {
	Ordinal  int
	Kind     ast.ClauseKind
	Optional bool
	Pattern  string
	Where    string
}

// CallIntent carries semantic in-query CALL sequencing details.
type CallIntent struct {
	Ordinal    int
	ClauseKind ast.ClauseKind
	Raw        string
}

// WriteActionIntent carries semantic write action sequencing details.
type WriteActionIntent struct {
	Ordinal       int
	ClauseKind    ast.ClauseKind
	Raw           string
	Pattern       string
	MergePattern  string
	MergeOnCreate string
	MergeOnMatch  string
}

// LogicalPlan is the explicit handoff contract from logical planning to
// physical execution.
//
// QP-0 baseline: logical planning emits deterministic operator graphs that are
// explainable and independent from raw clause parsing in execution.
type LogicalPlan struct {
	RootNodeID string
	Nodes      []LogicalNode
}

// LogicalNode is a normalized logical operator with child links.
type LogicalNode struct {
	ID       string
	Op       string
	Children []string
	Attrs    map[string]any
}

// PhysicalPlan is the explicit handoff contract from physical planning to
// runtime execution.
type PhysicalPlan struct {
	RootNodeID string
	Nodes      []PhysicalNode
}

// PhysicalNode is a normalized physical operator with child links.
type PhysicalNode struct {
	ID       string
	Op       string
	Children []string
	Attrs    map[string]any
}

// PhysicalExecutionInput is the execution-stage contract consumed by runtime
// operator execution.
//
// QP-0 baseline: execution consumes structured plans and runtime context and
// must not reinterpret raw clause text to recover core semantics.
type PhysicalExecutionInput struct {
	Plan   PhysicalPlan
	Tenant string
	Params map[string]any
}
