package logical

import (
	"fmt"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/pipeline"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

// Build constructs a deterministic logical plan from semantic output.
func Build(model semantic.Model) Plan {
	plan := Plan{RootNodeID: "", Nodes: []Node{}}

	appendNode := func(op string, attrs map[string]any) {
		id := fmt.Sprintf("n%d", len(plan.Nodes)+1)
		children := []string{}
		if plan.RootNodeID != "" {
			children = append(children, plan.RootNodeID)
		}
		plan.Nodes = append(plan.Nodes, Node{ID: id, Op: op, Children: children, Attrs: attrs})
		plan.RootNodeID = id
	}

	for _, pattern := range model.Patterns {
		op := "MATCH"
		if pattern.Optional {
			op = "OPTIONAL_MATCH"
		}
		if pattern.Kind == ast.ClauseKindOptionalMatch {
			op = "OPTIONAL_MATCH"
		}
		appendNode(op, map[string]any{
			"pattern": pattern.Pattern,
			"where":   pattern.Where,
		})
	}

	for _, write := range model.WriteActions {
		appendNode("WRITE", map[string]any{
			"kind":          string(write.ClauseKind),
			"raw":           write.Raw,
			"mergePattern":  write.MergePattern,
			"mergeOnCreate": write.MergeOnCreate,
			"mergeOnMatch":  write.MergeOnMatch,
		})
	}

	for _, projection := range model.Projections {
		items := make([]string, 0, len(projection.Items))
		for _, item := range projection.Items {
			if item.Alias != "" {
				items = append(items, item.Expression+" AS "+item.Alias)
				continue
			}
			items = append(items, item.Expression)
		}

		appendNode("PROJECT", map[string]any{
			"kind":       string(projection.Kind),
			"distinct":   projection.Distinct,
			"includeAll": projection.IncludeAll,
			"items":      items,
			"where":      projection.WhereExpr,
		})

		if len(projection.OrderBy) > 0 {
			ordering := make([]map[string]any, 0, len(projection.OrderBy))
			for _, item := range projection.OrderBy {
				ordering = append(ordering, map[string]any{
					"expression": item.Expression,
					"direction":  string(item.Direction),
				})
			}
			appendNode("SORT", map[string]any{"ordering": ordering})
		}

		if projection.Pagination.SkipExpr != "" || projection.Pagination.LimitExpr != "" {
			appendNode("PAGINATION", map[string]any{
				"skip":  projection.Pagination.SkipExpr,
				"limit": projection.Pagination.LimitExpr,
			})
		}
	}

	if len(plan.Nodes) == 0 {
		appendNode("NOOP", map[string]any{"statementKind": string(model.StatementKind)})
	}

	return plan
}

// BuildFromSemanticModel constructs a plan directly from pipeline semantic model.
func BuildFromSemanticModel(model pipeline.SemanticModel) Plan {
	return Build(semantic.Model(model))
}
