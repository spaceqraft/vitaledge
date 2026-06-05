package explain

import (
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/logical"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/cypher/physical"
	"github.com/paegun/vitaledge/internal/cypher/semantic"
)

func TestRenderPipelineDeterministic(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name ORDER BY name ASC LIMIT 5")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.Build(lp)

	first := RenderPipeline(lp, pp)
	second := RenderPipeline(lp, pp)
	if first != second {
		t.Fatalf("expected deterministic explain output, got first=%q second=%q", first, second)
	}

	checks := []string{
		"LOGICAL root=",
		"PHYSICAL root=",
		"- n1 MATCH",
		"- n2 PROJECT",
		"- n3 SORT",
		"- n4 PAGINATION",
		"- p1 PHY_EXPAND_MATCH",
		"- p2 PHY_PROJECT",
		"- p3 PHY_SORT",
		"- p4 PHY_PAGINATION",
	}
	for _, want := range checks {
		if !strings.Contains(first, want) {
			t.Fatalf("expected explain output to contain %q, got %q", want, first)
		}
	}
}

func TestRenderPipelineEmptyPlans(t *testing.T) {
	got := RenderPipeline(logical.Plan{}, physical.Plan{})
	if !strings.Contains(got, "LOGICAL root=") {
		t.Fatalf("expected logical section header, got %q", got)
	}
	if !strings.Contains(got, "PHYSICAL root=") {
		t.Fatalf("expected physical section header, got %q", got)
	}
	if strings.Count(got, "- (none)") != 2 {
		t.Fatalf("expected both empty sections to report none, got %q", got)
	}
}
