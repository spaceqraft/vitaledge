package ast

// StatementKind identifies the normalized statement category.
type StatementKind string

const (
	StatementKindMatchQuery StatementKind = "MATCH_QUERY"
	StatementKindQuery      StatementKind = "QUERY"
	StatementKindCall       StatementKind = "CALL"
	StatementKindExplain    StatementKind = "EXPLAIN"
	StatementKindProfile    StatementKind = "PROFILE"
)

// ClauseKind identifies a normalized top-level clause.
type ClauseKind string

const (
	ClauseKindMatch          ClauseKind = "MATCH"
	ClauseKindOptionalMatch  ClauseKind = "OPTIONAL_MATCH"
	ClauseKindUnwind         ClauseKind = "UNWIND"
	ClauseKindInQueryCall    ClauseKind = "IN_QUERY_CALL"
	ClauseKindCreate         ClauseKind = "CREATE"
	ClauseKindMerge          ClauseKind = "MERGE"
	ClauseKindDelete         ClauseKind = "DELETE"
	ClauseKindSet            ClauseKind = "SET"
	ClauseKindRemove         ClauseKind = "REMOVE"
	ClauseKindWith           ClauseKind = "WITH"
	ClauseKindReturn         ClauseKind = "RETURN"
	ClauseKindStandaloneCall ClauseKind = "STANDALONE_CALL"
)

// UnionKind identifies the normalized union operator.
type UnionKind string

const (
	UnionKindDistinct UnionKind = "UNION"
	UnionKindAll      UnionKind = "UNION_ALL"
)

// SortDirection normalizes ORDER BY direction keywords.
type SortDirection string

const (
	SortDirectionNone SortDirection = "NONE"
	SortDirectionAsc  SortDirection = "ASC"
	SortDirectionDesc SortDirection = "DESC"
)

// Span stores 1-based line/column coordinates.
type Span struct {
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
}

// Statement is a typed Cypher statement AST node.
type Statement interface {
	statementNode()
	Kind() StatementKind
	Span() Span
}

// Batch is a semicolon-separated set of parsed statements.
type Batch struct {
	Statements []Statement
}

// Clause is a generic top-level query clause.
type Clause struct {
	Kind          ClauseKind
	Raw           string
	MatchPattern  string
	MatchOptional bool
	MergePattern  string
	MergeOnCreate string
	MergeOnMatch  string
	Projection    *ReturnClause
	Where         *Expression
	Parameters    []ParameterRef
	Span          Span
}

// QueryPart is one single query between UNION boundaries.
type QueryPart struct {
	Clauses []Clause
}

// QueryStatement captures general regular-query structure, including UNION.
type QueryStatement struct {
	Parts      []QueryPart
	Unions     []UnionKind
	Parameters []ParameterRef
	SourceSpan Span
}

func (*QueryStatement) statementNode() {}

func (*QueryStatement) Kind() StatementKind {
	return StatementKindQuery
}

func (q *QueryStatement) Span() Span {
	return q.SourceSpan
}

// StandaloneCallStatement captures a CALL statement outside regular query syntax.
type StandaloneCallStatement struct {
	Call       Clause
	Parameters []ParameterRef
	SourceSpan Span
}

func (*StandaloneCallStatement) statementNode() {}

func (*StandaloneCallStatement) Kind() StatementKind {
	return StatementKindCall
}

func (c *StandaloneCallStatement) Span() Span {
	return c.SourceSpan
}

// ParameterRef captures a parameter usage like $name or $1.
type ParameterRef struct {
	Name string
	Span Span
}

// Expression is a parsed expression in source form with parameter references.
type Expression struct {
	Raw        string
	Parameters []ParameterRef
	Span       Span
}

// MatchClause represents MATCH or OPTIONAL MATCH.
type MatchClause struct {
	Optional bool
	Pattern  string
	Where    *Expression
	Span     Span
}

// ProjectionItem is one RETURN item.
type ProjectionItem struct {
	Expression Expression
	Alias      string
}

// SortItem is one ORDER BY element.
type SortItem struct {
	Expression Expression
	Direction  SortDirection
}

// ReturnClause captures RETURN projection details.
type ReturnClause struct {
	Distinct   bool
	IncludeAll bool
	Items      []ProjectionItem
	OrderBy    []SortItem
	Skip       *Expression
	Limit      *Expression
	Span       Span
}

// MatchQueryStatement is the initial supported executable shape.
type MatchQueryStatement struct {
	MatchClauses []MatchClause
	Return       ReturnClause
	Parameters   []ParameterRef
	SourceSpan   Span
}

func (*MatchQueryStatement) statementNode() {}

func (*MatchQueryStatement) Kind() StatementKind {
	return StatementKindMatchQuery
}

func (m *MatchQueryStatement) Span() Span {
	return m.SourceSpan
}

// ExplainStatement wraps another statement for dry-run planning output.
type ExplainStatement struct {
	Raw        string
	Query      string
	Statement  Statement
	SourceSpan Span
}

func (*ExplainStatement) statementNode() {}

func (*ExplainStatement) Kind() StatementKind {
	return StatementKindExplain
}

func (e *ExplainStatement) Span() Span {
	return e.SourceSpan
}

// ProfileStatement wraps another statement for plan-plus-execution output.
type ProfileStatement struct {
	Raw        string
	Query      string
	Statement  Statement
	SourceSpan Span
}

func (*ProfileStatement) statementNode() {}

func (*ProfileStatement) Kind() StatementKind {
	return StatementKindProfile
}

func (p *ProfileStatement) Span() Span {
	return p.SourceSpan
}
