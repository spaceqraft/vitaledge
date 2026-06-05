package semantic

import (
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/pipeline"
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
		WriteActions:  []WriteActionIntent{},
	}

	for _, match := range stmt.MatchClauses {
		whereRaw := ""
		if match.Where != nil {
			whereRaw = strings.TrimSpace(match.Where.Raw)
		}
		model.Patterns = append(model.Patterns, PatternIntent{
			Kind:     ast.ClauseKindMatch,
			Optional: match.Optional,
			Pattern:  strings.TrimSpace(match.Pattern),
			Where:    whereRaw,
		})
	}

	appendProjection(&model, ast.ClauseKindReturn, stmt.Return, nil)
	return model
}

func buildFromQueryStatement(stmt *ast.QueryStatement) Model {
	model := Model{
		StatementKind: stmt.Kind(),
		Projections:   []ProjectionIntent{},
		Ordering:      []OrderingIntent{},
		Patterns:      []PatternIntent{},
		WriteActions:  []WriteActionIntent{},
	}

	for _, part := range stmt.Parts {
		for _, clause := range part.Clauses {
			switch clause.Kind {
			case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
				whereRaw := ""
				if clause.Where != nil {
					whereRaw = strings.TrimSpace(clause.Where.Raw)
				}
				model.Patterns = append(model.Patterns, PatternIntent{
					Kind:     clause.Kind,
					Optional: clause.Kind == ast.ClauseKindOptionalMatch || clause.MatchOptional,
					Pattern:  strings.TrimSpace(clause.MatchPattern),
					Where:    whereRaw,
				})
			case ast.ClauseKindWith, ast.ClauseKindReturn:
				if clause.Projection != nil {
					appendProjection(&model, clause.Kind, *clause.Projection, clause.Where)
				}
			case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindDelete, ast.ClauseKindSet, ast.ClauseKindRemove:
				model.WriteActions = append(model.WriteActions, WriteActionIntent{
					ClauseKind:    clause.Kind,
					Raw:           strings.TrimSpace(clause.Raw),
					MergePattern:  strings.TrimSpace(clause.MergePattern),
					MergeOnCreate: strings.TrimSpace(clause.MergeOnCreate),
					MergeOnMatch:  strings.TrimSpace(clause.MergeOnMatch),
				})
			}
		}
	}

	return model
}

func appendProjection(model *Model, kind ast.ClauseKind, projection ast.ReturnClause, where *ast.Expression) {
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
