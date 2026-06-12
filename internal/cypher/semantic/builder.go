package semantic

import (
	"strings"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/pipeline"
)

// Build constructs a semantic model from a parsed statement.
func Build(stmt ast.Statement) (Model, error) {
	if stmt == nil {
		return Model{}, pipeline.ErrUnsupportedStatement{}
	}

	switch typed := stmt.(type) {
	case *ast.ExplainStatement:
		if typed.Statement == nil {
			return Model{}, pipeline.ErrUnsupportedStatement{Kind: ast.StatementKindExplain}
		}
		return Build(typed.Statement)
	case *ast.MatchQueryStatement:
		return buildFromMatchQuery(typed), nil
	case *ast.QueryStatement:
		return buildFromQueryStatement(typed), nil
	case *ast.StandaloneCallStatement:
		return Model{StatementKind: ast.StatementKindCall}, nil
	default:
		return Model{}, pipeline.ErrUnsupportedStatement{Kind: stmt.Kind()}
	}
}

// BuildFromParseOutput constructs a semantic model from a pipeline parse output.
func BuildFromParseOutput(out pipeline.ParseOutput) (Model, error) {
	return Build(out.Statement)
}

func buildFromMatchQuery(stmt *ast.MatchQueryStatement) Model {
	model := Model{
		StatementKind: stmt.Kind(),
		Projections:   []ProjectionIntent{},
		Ordering:      []OrderingIntent{},
		Patterns:      []PatternIntent{},
		Calls:         []CallIntent{},
		WriteActions:  []WriteActionIntent{},
	}
	ordinal := 0

	for _, match := range stmt.MatchClauses {
		whereRaw := ""
		if match.Where != nil {
			whereRaw = strings.TrimSpace(match.Where.Raw)
		}
		model.Patterns = append(model.Patterns, PatternIntent{
			Ordinal:  ordinal,
			Kind:     ast.ClauseKindMatch,
			Optional: match.Optional,
			Pattern:  strings.TrimSpace(match.Pattern),
			Where:    whereRaw,
		})
		ordinal++
	}

	appendProjection(&model, ordinal, ast.ClauseKindReturn, stmt.Return, nil)
	return model
}

func buildFromQueryStatement(stmt *ast.QueryStatement) Model {
	model := Model{
		StatementKind: stmt.Kind(),
		Projections:   []ProjectionIntent{},
		Ordering:      []OrderingIntent{},
		Patterns:      []PatternIntent{},
		Calls:         []CallIntent{},
		WriteActions:  []WriteActionIntent{},
	}
	ordinal := 0

	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			switch clause.Kind {
			case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
				whereRaw := ""
				if clause.Where != nil {
					whereRaw = strings.TrimSpace(clause.Where.Raw)
				}
				model.Patterns = append(model.Patterns, PatternIntent{
					Ordinal:  ordinal,
					Kind:     clause.Kind,
					Optional: clause.Kind == ast.ClauseKindOptionalMatch || clause.MatchOptional,
					Pattern:  strings.TrimSpace(clause.MatchPattern),
					Where:    whereRaw,
				})
			case ast.ClauseKindWith, ast.ClauseKindReturn:
				if clause.Projection != nil {
					appendProjection(&model, ordinal, clause.Kind, *clause.Projection, clause.Where)
				}
			case ast.ClauseKindUnwind:
				if projection, ok := projectionFromUnwindClauseRaw(clause.Raw); ok {
					appendProjection(&model, ordinal, clause.Kind, projection, nil)
				}
			case ast.ClauseKindInQueryCall:
				model.Calls = append(model.Calls, CallIntent{
					Ordinal:    ordinal,
					ClauseKind: clause.Kind,
					Raw:        strings.TrimSpace(clause.Raw),
				})
			case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindDelete, ast.ClauseKindSet, ast.ClauseKindRemove:
				model.WriteActions = append(model.WriteActions, WriteActionIntent{
					Ordinal:       ordinal,
					ClauseKind:    clause.Kind,
					Raw:           strings.TrimSpace(clause.Raw),
					Pattern:       strings.TrimSpace(clause.MatchPattern),
					MergePattern:  strings.TrimSpace(clause.MergePattern),
					MergeOnCreate: strings.TrimSpace(clause.MergeOnCreate),
					MergeOnMatch:  strings.TrimSpace(clause.MergeOnMatch),
				})
			}
			ordinal++
		}
	}

	return model
}

func appendProjection(model *Model, ordinal int, kind ast.ClauseKind, projection ast.ReturnClause, where *ast.Expression) {
	if model == nil {
		return
	}
	items := make([]ProjectionItemIntent, 0, len(projection.Items)+1)
	if projection.IncludeAll {
		items = append(items, ProjectionItemIntent{Expression: "*"})
	}
	for _, item := range projection.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		items = append(items, ProjectionItemIntent{
			Expression: expr,
			Alias:      strings.TrimSpace(item.Alias),
		})
	}

	ordering := make([]OrderingIntent, 0, len(projection.OrderBy))
	for _, sortItem := range projection.OrderBy {
		expr := strings.TrimSpace(sortItem.Expression.Raw)
		if expr == "" {
			continue
		}
		ordering = append(ordering, OrderingIntent{Expression: expr, Direction: sortItem.Direction})
		model.Ordering = append(model.Ordering, OrderingIntent{Expression: expr, Direction: sortItem.Direction})
	}

	pagination := PaginationIntent{}
	if projection.Skip != nil {
		pagination.SkipExpr = strings.TrimSpace(projection.Skip.Raw)
	}
	if projection.Limit != nil {
		pagination.LimitExpr = strings.TrimSpace(projection.Limit.Raw)
	}

	model.Projections = append(model.Projections, ProjectionIntent{
		Ordinal:    ordinal,
		Kind:       kind,
		Distinct:   projection.Distinct,
		IncludeAll: projection.IncludeAll,
		Items:      items,
		OrderBy:    ordering,
		Pagination: pagination,
		WhereExpr:  expressionRaw(where),
	})
	model.Pagination = pagination
}

func expressionRaw(expr *ast.Expression) string {
	if expr == nil {
		return ""
	}
	return strings.TrimSpace(expr.Raw)
}

func projectionFromUnwindClauseRaw(raw string) (ast.ReturnClause, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ast.ReturnClause{}, false
	}
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper, "UNWIND ") {
		return ast.ReturnClause{}, false
	}
	body := strings.TrimSpace(raw[len("UNWIND"):])
	if body == "" {
		return ast.ReturnClause{}, false
	}
	asPos := topLevelAsKeywordIndex(body)
	if asPos <= 0 {
		return ast.ReturnClause{}, false
	}
	expr := strings.TrimSpace(body[:asPos])
	alias := strings.TrimSpace(body[asPos+2:])
	if expr == "" || alias == "" {
		return ast.ReturnClause{}, false
	}
	if !isUnwindAliasIdentifier(alias) {
		return ast.ReturnClause{}, false
	}
	return ast.ReturnClause{
		Items: []ast.ProjectionItem{{
			Expression: ast.Expression{Raw: expr},
			Alias:      alias,
		}},
	}, true
}

func topLevelAsKeywordIndex(raw string) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i+1 < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		if !strings.EqualFold(raw[i:i+2], "AS") {
			continue
		}
		leftBoundary := i == 0 || raw[i-1] == ' ' || raw[i-1] == '\t' || raw[i-1] == '\n' || raw[i-1] == '\r'
		right := i + 2
		rightBoundary := right >= len(raw) || raw[right] == ' ' || raw[right] == '\t' || raw[right] == '\n' || raw[right] == '\r'
		if leftBoundary && rightBoundary {
			return i
		}
	}
	return -1
}

func isUnwindAliasIdentifier(alias string) bool {
	if alias == "" {
		return false
	}
	for i, ch := range alias {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' {
			continue
		}
		if i > 0 && ch >= '0' && ch <= '9' {
			continue
		}
		return false
	}
	return true
}
