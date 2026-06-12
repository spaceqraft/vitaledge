package explain

import (
	"strings"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/logical"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/cypher/semantic"
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

func TestRenderPipelineIncludesOptionalExpandStrategyAnnotations(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) OPTIONAL MATCH (v)-[:LIKES]-(m:Movie) RETURN m.id AS mid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.BuildWithStats(lp, physical.StatsHints{
		EdgeAvgOutDegree: map[string]float64{"LIKES": 1.0},
	})

	out := RenderPipeline(lp, pp)
	checks := []string{
		"- p2 PHY_EXPAND_OPTIONAL",
		"\"accessPath\":\"adjacency_expand_optional_out_first\"",
		"\"variant\":\"optional_expand_out_first_indexed\"",
		"\"joinStrategy\":\"indexed_bind_join\"",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected explain output to contain %q, got %q", want, out)
		}
	}
}

func TestRenderPipelineIncludesSortVariantAnnotations(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) RETURN v.id AS vid ORDER BY vid DESC SKIP 5 LIMIT 10")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.BuildWithStats(lp, physical.StatsHints{HasEdgeTotal: true, EdgeTotal: 1000})

	out := RenderPipeline(lp, pp)
	checks := []string{
		"- p3 PHY_SORT",
		"\"strategy\":\"topk_heap\"",
		"\"variant\":\"sort_topk_heap\"",
		"\"topK\":15",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected explain output to contain %q, got %q", want, out)
		}
	}
}

func TestRenderPipelineIncludesAntiProbeVariantAnnotations(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (u:User)-[:KNOWS]->(v:User) WHERE NOT ((u)-[:BLOCKED]-(v)) AND NOT ((u)-[:MUTED]-(v)) RETURN v.id AS vid")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	sem, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	lp := logical.Build(sem)
	pp := physical.BuildWithStats(lp, physical.StatsHints{AntiProbeHitRateBy: map[string]float64{"BLOCKED": 0.95, "MUTED": 0.2}})

	out := RenderPipeline(lp, pp)
	checks := []string{
		"PHY_ANTI_PROBE",
		"\"variant\":\"anti_probe_batch_high\"",
		"\"variant\":\"anti_probe_row_low\"",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("expected explain output to contain %q, got %q", want, out)
		}
	}
}
