package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	cyphergen "github.com/spaceqraft/vitaledge/internal/cypher/grammar/generated"
)

var (
	mergeOnCreateSetRE = regexp.MustCompile(`(?i)\bON\s*CREATE\s*SET\b`)
	mergeOnMatchSetRE  = regexp.MustCompile(`(?i)\bON\s*MATCH\s*SET\b`)
)

func buildStatement(root cyphergen.IOC_CypherContext, seg statementSegment, fullQuery string) (ast.Statement, error) {
	stmtCtx := root.OC_Statement()
	if stmtCtx == nil || stmtCtx.OC_Query() == nil {
		return nil, internalError(seg, "missing query root")
	}

	query := stmtCtx.OC_Query()
	if standalone := query.OC_StandaloneCall(); standalone != nil {
		clause := buildClause(ast.ClauseKindStandaloneCall, standalone, seg, fullQuery)
		return &ast.StandaloneCallStatement{
			Call:       clause,
			Parameters: clause.Parameters,
			SourceSpan: spanFromContext(seg, fullQuery, standalone),
		}, nil
	}

	rq := query.OC_RegularQuery()
	if rq == nil {
		return nil, internalError(seg, "missing regular query")
	}

	if legacy, ok, err := tryBuildLegacyMatchQuery(rq, seg, fullQuery); err != nil {
		return nil, err
	} else if ok {
		return legacy, nil
	}

	return buildGeneralQueryStatement(rq, seg, fullQuery)
}

func tryBuildLegacyMatchQuery(rq cyphergen.IOC_RegularQueryContext, seg statementSegment, fullQuery string) (*ast.MatchQueryStatement, bool, error) {
	if len(rq.AllOC_Union()) > 0 {
		return nil, false, nil
	}

	sq := rq.OC_SingleQuery()
	if sq == nil || sq.OC_MultiPartQuery() != nil {
		return nil, false, nil
	}

	sp := sq.OC_SinglePartQuery()
	if sp == nil {
		return nil, false, nil
	}
	if len(sp.AllOC_UpdatingClause()) > 0 {
		return nil, false, nil
	}
	if sp.OC_Return() == nil {
		return nil, false, nil
	}

	reading := sp.AllOC_ReadingClause()
	if len(reading) == 0 {
		return nil, false, nil
	}

	stmt := &ast.MatchQueryStatement{
		MatchClauses: make([]ast.MatchClause, 0, len(reading)),
		SourceSpan:   spanFromContext(seg, fullQuery, sp),
	}

	for _, clause := range reading {
		m := clause.OC_Match()
		if m == nil {
			return nil, false, nil
		}

		matchClause := ast.MatchClause{
			Optional: m.OPTIONAL() != nil,
			Pattern:  strings.TrimSpace(m.OC_Pattern().GetText()),
			Span:     spanFromContext(seg, fullQuery, m),
		}

		patternParams := collectParameters(seg, fullQuery, m.OC_Pattern())
		stmt.Parameters = appendUniqueParams(stmt.Parameters, patternParams)

		if w := m.OC_Where(); w != nil {
			expr := expressionFromContext(seg, fullQuery, w.OC_Expression())
			matchClause.Where = &expr
			stmt.Parameters = appendUniqueParams(stmt.Parameters, expr.Parameters)
		}

		stmt.MatchClauses = append(stmt.MatchClauses, matchClause)
	}

	ret, err := buildReturnClause(sp.OC_Return(), seg, fullQuery)
	if err != nil {
		return nil, false, err
	}
	stmt.Return = ret

	for _, item := range ret.Items {
		stmt.Parameters = appendUniqueParams(stmt.Parameters, item.Expression.Parameters)
	}
	for _, s := range ret.OrderBy {
		stmt.Parameters = appendUniqueParams(stmt.Parameters, s.Expression.Parameters)
	}
	if ret.Skip != nil {
		stmt.Parameters = appendUniqueParams(stmt.Parameters, ret.Skip.Parameters)
	}
	if ret.Limit != nil {
		stmt.Parameters = appendUniqueParams(stmt.Parameters, ret.Limit.Parameters)
	}

	return stmt, true, nil
}

func buildGeneralQueryStatement(rq cyphergen.IOC_RegularQueryContext, seg statementSegment, fullQuery string) (*ast.QueryStatement, error) {
	parts := make([]ast.QueryPart, 0, 1+len(rq.AllOC_Union()))
	unions := make([]ast.UnionKind, 0, len(rq.AllOC_Union()))

	first := rq.OC_SingleQuery()
	if first == nil {
		return nil, internalError(seg, "missing first single query")
	}
	part, err := buildQueryPartFromSingle(first, seg, fullQuery)
	if err != nil {
		return nil, err
	}
	parts = append(parts, part)

	for _, u := range rq.AllOC_Union() {
		if u.ALL() != nil {
			unions = append(unions, ast.UnionKindAll)
		} else {
			unions = append(unions, ast.UnionKindDistinct)
		}

		sq := u.OC_SingleQuery()
		if sq == nil {
			return nil, internalError(seg, "missing union single query")
		}
		unionPart, err := buildQueryPartFromSingle(sq, seg, fullQuery)
		if err != nil {
			return nil, err
		}
		parts = append(parts, unionPart)
	}

	stmt := &ast.QueryStatement{
		Parts:      parts,
		Unions:     unions,
		Parameters: []ast.ParameterRef{},
		SourceSpan: spanFromContext(seg, fullQuery, rq),
	}

	for _, part := range parts {
		for _, c := range part.Clauses {
			stmt.Parameters = appendUniqueParams(stmt.Parameters, c.Parameters)
		}
	}

	return stmt, nil
}

func buildQueryPartFromSingle(sq cyphergen.IOC_SingleQueryContext, seg statementSegment, fullQuery string) (ast.QueryPart, error) {
	if sp := sq.OC_SinglePartQuery(); sp != nil {
		return buildQueryPartFromChildren(sp.GetChildren(), seg, fullQuery)
	}

	mp := sq.OC_MultiPartQuery()
	if mp == nil {
		return ast.QueryPart{}, internalError(seg, "single query has no single/multipart body")
	}

	return buildQueryPartFromChildren(mp.GetChildren(), seg, fullQuery)
}

func buildQueryPartFromChildren(children []antlr.Tree, seg statementSegment, fullQuery string) (ast.QueryPart, error) {
	part := ast.QueryPart{Clauses: []ast.Clause{}}

	for _, child := range children {
		switch c := child.(type) {
		case cyphergen.IOC_ReadingClauseContext:
			clause, err := buildReadingClause(c, seg, fullQuery)
			if err != nil {
				return ast.QueryPart{}, err
			}
			part.Clauses = append(part.Clauses, clause)
		case cyphergen.IOC_UpdatingClauseContext:
			clause, err := buildUpdatingClause(c, seg, fullQuery)
			if err != nil {
				return ast.QueryPart{}, err
			}
			part.Clauses = append(part.Clauses, clause)
		case cyphergen.IOC_WithContext:
			projection, where, err := buildWithProjectionClause(c, seg, fullQuery)
			if err != nil {
				return ast.QueryPart{}, err
			}
			clause := buildClause(ast.ClauseKindWith, c, seg, fullQuery)
			clause.Projection = &projection
			clause.Where = where
			part.Clauses = append(part.Clauses, clause)
		case cyphergen.IOC_ReturnContext:
			projection, err := buildReturnClause(c, seg, fullQuery)
			if err != nil {
				return ast.QueryPart{}, err
			}
			clause := buildClause(ast.ClauseKindReturn, c, seg, fullQuery)
			clause.Projection = &projection
			part.Clauses = append(part.Clauses, clause)
		case cyphergen.IOC_SinglePartQueryContext:
			nested, err := buildQueryPartFromChildren(c.GetChildren(), seg, fullQuery)
			if err != nil {
				return ast.QueryPart{}, err
			}
			part.Clauses = append(part.Clauses, nested.Clauses...)
		}
	}

	if len(part.Clauses) == 0 {
		return ast.QueryPart{}, &ParseError{Kind: ParseErrorSemantic, Message: "query part has no clauses", Statement: seg.index}
	}

	return part, nil
}

func buildReadingClause(c cyphergen.IOC_ReadingClauseContext, seg statementSegment, fullQuery string) (ast.Clause, error) {
	if m := c.OC_Match(); m != nil {
		kind := ast.ClauseKindMatch
		optional := false
		if m.OPTIONAL() != nil {
			kind = ast.ClauseKindOptionalMatch
			optional = true
		}
		clause := buildClause(kind, m, seg, fullQuery)
		if m.OC_Pattern() != nil {
			clause.MatchPattern = strings.TrimSpace(m.OC_Pattern().GetText())
		}
		clause.MatchOptional = optional
		if where := m.OC_Where(); where != nil {
			expr := expressionFromContext(seg, fullQuery, where.OC_Expression())
			clause.Where = &expr
		}
		return clause, nil
	}
	if u := c.OC_Unwind(); u != nil {
		return buildClause(ast.ClauseKindUnwind, u, seg, fullQuery), nil
	}
	if call := c.OC_InQueryCall(); call != nil {
		return buildClause(ast.ClauseKindInQueryCall, call, seg, fullQuery), nil
	}

	return ast.Clause{}, internalError(seg, "unknown reading clause")
}

func buildUpdatingClause(c cyphergen.IOC_UpdatingClauseContext, seg statementSegment, fullQuery string) (ast.Clause, error) {
	if create := c.OC_Create(); create != nil {
		clause := buildClause(ast.ClauseKindCreate, create, seg, fullQuery)
		if pattern := create.OC_Pattern(); pattern != nil {
			clause.MatchPattern = strings.TrimSpace(pattern.GetText())
		}
		return clause, nil
	}
	if merge := c.OC_Merge(); merge != nil {
		clause := buildClause(ast.ClauseKindMerge, merge, seg, fullQuery)
		if pattern := merge.OC_PatternPart(); pattern != nil {
			clause.MatchPattern = strings.TrimSpace(pattern.GetText())
		}
		pattern, onCreateSet, onMatchSet := splitMergePatternAndActions(clause.Raw)
		clause.MergePattern = pattern
		clause.MergeOnCreate = onCreateSet
		clause.MergeOnMatch = onMatchSet
		return clause, nil
	}
	if del := c.OC_Delete(); del != nil {
		return buildClause(ast.ClauseKindDelete, del, seg, fullQuery), nil
	}
	if setClause := c.OC_Set(); setClause != nil {
		return buildClause(ast.ClauseKindSet, setClause, seg, fullQuery), nil
	}
	if remove := c.OC_Remove(); remove != nil {
		return buildClause(ast.ClauseKindRemove, remove, seg, fullQuery), nil
	}

	return ast.Clause{}, internalError(seg, "unknown updating clause")
}

func buildClause(kind ast.ClauseKind, ctx antlr.ParserRuleContext, seg statementSegment, fullQuery string) ast.Clause {
	return ast.Clause{
		Kind:       kind,
		Raw:        strings.TrimSpace(ctx.GetText()),
		Parameters: collectParameters(seg, fullQuery, ctx),
		Span:       spanFromContext(seg, fullQuery, ctx),
	}
}

func buildReturnClause(r cyphergen.IOC_ReturnContext, seg statementSegment, fullQuery string) (ast.ReturnClause, error) {
	if r == nil {
		return ast.ReturnClause{}, internalErrorValue("nil return clause")
	}

	return buildProjectionBody(r.OC_ProjectionBody(), seg, fullQuery, r)
}

func buildWithProjectionClause(w cyphergen.IOC_WithContext, seg statementSegment, fullQuery string) (ast.ReturnClause, *ast.Expression, error) {
	if w == nil {
		return ast.ReturnClause{}, nil, internalErrorValue("nil with clause")
	}

	projection, err := buildProjectionBody(w.OC_ProjectionBody(), seg, fullQuery, w)
	if err != nil {
		return ast.ReturnClause{}, nil, err
	}

	var whereExpr *ast.Expression
	if where := w.OC_Where(); where != nil {
		expr := expressionFromContext(seg, fullQuery, where.OC_Expression())
		whereExpr = &expr
	}

	return projection, whereExpr, nil
}

func buildProjectionBody(body cyphergen.IOC_ProjectionBodyContext, seg statementSegment, fullQuery string, spanCtx antlr.ParserRuleContext) (ast.ReturnClause, error) {
	if body == nil {
		return ast.ReturnClause{}, internalErrorValue("missing projection body")
	}

	itemsCtx := body.OC_ProjectionItems()
	if itemsCtx == nil {
		return ast.ReturnClause{}, internalErrorValue("missing projection items")
	}

	ret := ast.ReturnClause{
		Distinct:   body.DISTINCT() != nil,
		IncludeAll: itemsCtx.GetStart().GetTokenType() == cyphergen.CypherParserT__4,
		Items:      make([]ast.ProjectionItem, 0, len(itemsCtx.AllOC_ProjectionItem())),
		OrderBy:    []ast.SortItem{},
		Span:       spanFromContext(seg, fullQuery, spanCtx),
	}

	for _, itemCtx := range itemsCtx.AllOC_ProjectionItem() {
		expr := expressionFromContext(seg, fullQuery, itemCtx.OC_Expression())
		item := ast.ProjectionItem{Expression: expr}
		if itemCtx.AS() != nil && itemCtx.OC_Variable() != nil {
			item.Alias = strings.TrimSpace(itemCtx.OC_Variable().GetText())
		}
		ret.Items = append(ret.Items, item)
	}

	if order := body.OC_Order(); order != nil {
		for _, sortCtx := range order.AllOC_SortItem() {
			direction := ast.SortDirectionNone
			switch {
			case sortCtx.ASC() != nil || sortCtx.ASCENDING() != nil:
				direction = ast.SortDirectionAsc
			case sortCtx.DESC() != nil || sortCtx.DESCENDING() != nil:
				direction = ast.SortDirectionDesc
			}
			ret.OrderBy = append(ret.OrderBy, ast.SortItem{
				Expression: expressionFromContext(seg, fullQuery, sortCtx.OC_Expression()),
				Direction:  direction,
			})
		}
	}

	if skip := body.OC_Skip(); skip != nil {
		expr := expressionFromContext(seg, fullQuery, skip.OC_Expression())
		ret.Skip = &expr
	}
	if limit := body.OC_Limit(); limit != nil {
		expr := expressionFromContext(seg, fullQuery, limit.OC_Expression())
		ret.Limit = &expr
	}

	return ret, nil
}

func expressionFromContext(seg statementSegment, fullQuery string, ctx cyphergen.IOC_ExpressionContext) ast.Expression {
	raw := ""
	if ctx != nil {
		raw = strings.TrimSpace(ctx.GetText())
	}
	return ast.Expression{
		Raw:        raw,
		Parameters: collectParameters(seg, fullQuery, ctx),
		Span:       spanFromContext(seg, fullQuery, ctx),
	}
}

func splitMergePatternAndActions(raw string) (pattern string, onCreateSet string, onMatchSet string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", ""
	}
	raw = stripLeadingKeywordInsensitive(raw, "MERGE")
	if raw == "" {
		return "", "", ""
	}

	createMatch := mergeOnCreateSetRE.FindStringIndex(raw)
	matchMatch := mergeOnMatchSetRE.FindStringIndex(raw)
	createIdx := -1
	createLen := 0
	if len(createMatch) == 2 {
		createIdx = createMatch[0]
		createLen = createMatch[1] - createMatch[0]
	}
	matchIdx := -1
	matchLen := 0
	if len(matchMatch) == 2 {
		matchIdx = matchMatch[0]
		matchLen = matchMatch[1] - matchMatch[0]
	}
	firstIdx := minPositiveIndex(createIdx, matchIdx)
	if firstIdx < 0 {
		return raw, "", ""
	}

	pattern = strings.TrimSpace(raw[:firstIdx])
	if createIdx >= 0 {
		end := len(raw)
		if matchIdx > createIdx {
			end = matchIdx
		}
		onCreateSet = strings.TrimSpace(raw[createIdx+createLen : end])
	}
	if matchIdx >= 0 {
		end := len(raw)
		if createIdx > matchIdx {
			end = createIdx
		}
		onMatchSet = strings.TrimSpace(raw[matchIdx+matchLen : end])
	}
	return pattern, onCreateSet, onMatchSet
}

func stripLeadingKeywordInsensitive(raw string, keyword string) string {
	raw = strings.TrimSpace(raw)
	keyword = strings.TrimSpace(keyword)
	if raw == "" || keyword == "" {
		return raw
	}
	if len(raw) < len(keyword) {
		return raw
	}
	if !strings.EqualFold(raw[:len(keyword)], keyword) {
		return raw
	}
	if len(raw) == len(keyword) {
		return ""
	}
	next := raw[len(keyword)]
	if (next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || (next >= '0' && next <= '9') || next == '_' {
		return raw
	}
	return strings.TrimSpace(raw[len(keyword):])
}

func minPositiveIndex(values ...int) int {
	best := -1
	for _, value := range values {
		if value < 0 {
			continue
		}
		if best < 0 || value < best {
			best = value
		}
	}
	return best
}

func collectParameters(seg statementSegment, fullQuery string, tree antlr.Tree) []ast.ParameterRef {
	if tree == nil {
		return nil
	}

	collector := &parameterCollector{
		BaseCypherListener: &cyphergen.BaseCypherListener{},
		params:             []ast.ParameterRef{},
		seg:                seg,
		fullQuery:          fullQuery,
	}
	antlr.ParseTreeWalkerDefault.Walk(collector, tree)
	return collector.params
}

type parameterCollector struct {
	*cyphergen.BaseCypherListener
	params    []ast.ParameterRef
	seg       statementSegment
	fullQuery string
}

func (p *parameterCollector) EnterOC_Parameter(c *cyphergen.OC_ParameterContext) {
	name := ""
	if c.OC_SchemaName() != nil {
		name = c.OC_SchemaName().GetText()
	} else if c.DecimalInteger() != nil {
		name = c.DecimalInteger().GetText()
	}

	line, col := localToGlobal(p.seg, p.fullQuery, c.GetStart().GetLine(), c.GetStart().GetColumn())
	p.params = appendUniqueParams(p.params, []ast.ParameterRef{{
		Name: name,
		Span: ast.Span{StartLine: line, StartColumn: col, EndLine: line, EndColumn: col + len(name)},
	}})
}

func appendUniqueParams(dst []ast.ParameterRef, src []ast.ParameterRef) []ast.ParameterRef {
	seen := make(map[string]bool, len(dst))
	for _, p := range dst {
		seen[p.Name] = true
	}
	for _, p := range src {
		if p.Name == "" || seen[p.Name] {
			continue
		}
		seen[p.Name] = true
		dst = append(dst, p)
	}
	return dst
}

func spanFromContext(seg statementSegment, fullQuery string, ctx antlr.ParserRuleContext) ast.Span {
	if ctx == nil || ctx.GetStart() == nil || ctx.GetStop() == nil {
		return ast.Span{}
	}

	startLine, startCol := localToGlobal(seg, fullQuery, ctx.GetStart().GetLine(), ctx.GetStart().GetColumn())
	endLine, endCol := localToGlobal(seg, fullQuery, ctx.GetStop().GetLine(), ctx.GetStop().GetColumn())
	endCol += len(strings.TrimSpace(ctx.GetStop().GetText()))

	return ast.Span{
		StartLine:   startLine,
		StartColumn: startCol,
		EndLine:     endLine,
		EndColumn:   endCol,
	}
}

func internalError(seg statementSegment, message string) error {
	return &ParseError{Kind: ParseErrorInternal, Message: message, Statement: seg.index}
}

func internalErrorValue(message string) error {
	return fmt.Errorf("internal parser error: %s", message)
}
