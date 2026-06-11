package logical

import (
	"fmt"
	"sort"

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

	type planStep struct {
		ordinal int
		index   int
		kind    string
		pattern semantic.PatternIntent
		call    semantic.CallIntent
		write   semantic.WriteActionIntent
		proj    semantic.ProjectionIntent
	}

	steps := make([]planStep, 0, len(model.Patterns)+len(model.Calls)+len(model.WriteActions)+len(model.Projections))
	seq := 0
	for i := range model.Patterns {
		p := model.Patterns[i]
		steps = append(steps, planStep{ordinal: p.Ordinal, index: seq, kind: "pattern", pattern: p})
		seq++
	}
	for i := range model.WriteActions {
		w := model.WriteActions[i]
		steps = append(steps, planStep{ordinal: w.Ordinal, index: seq, kind: "write", write: w})
		seq++
	}
	for i := range model.Calls {
		c := model.Calls[i]
		steps = append(steps, planStep{ordinal: c.Ordinal, index: seq, kind: "call", call: c})
		seq++
	}
	for i := range model.Projections {
		p := model.Projections[i]
		steps = append(steps, planStep{ordinal: p.Ordinal, index: seq, kind: "projection", proj: p})
		seq++
	}

	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].ordinal != steps[j].ordinal {
			return steps[i].ordinal < steps[j].ordinal
		}
		return steps[i].index < steps[j].index
	})

	for _, step := range steps {
		switch step.kind {
		case "pattern":
			pattern := step.pattern
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
		case "write":
			write := step.write
			appendNode("WRITE", map[string]any{
				"kind":          string(write.ClauseKind),
				"raw":           write.Raw,
				"pattern":       write.Pattern,
				"mergePattern":  write.MergePattern,
				"mergeOnCreate": write.MergeOnCreate,
				"mergeOnMatch":  write.MergeOnMatch,
			})
		case "call":
			call := step.call
			appendNode("CALL", map[string]any{
				"kind": string(call.ClauseKind),
				"raw":  call.Raw,
			})
		case "projection":
			projection := step.proj
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
