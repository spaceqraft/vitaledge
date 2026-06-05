package executor

import (
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

func TestDecideQueryRouteNilStatement(t *testing.T) {
	decision := decideQueryRoute(nil, nil, true, true)
	if decision.route != queryRouteLegacyExecutor {
		t.Fatalf("expected nil statement to route to legacy executor, got %#v", decision)
	}
	if decision.reason != "nil_statement" {
		t.Fatalf("expected nil statement reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteDisabled(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: false}, false, true)
	if decision.route != queryRouteLegacyExecutor {
		t.Fatalf("expected disabled runtime pipeline to route to legacy executor, got %#v", decision)
	}
	if decision.reason != "runtime_pipeline_disabled" {
		t.Fatalf("expected disabled reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteUnsupportedShape(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: true}, false, false)
	if decision.route != queryRouteLegacyExecutor {
		t.Fatalf("expected unsupported runtime shape to route to legacy executor, got %#v", decision)
	}
	if decision.reason != "runtime_shape_unsupported" {
		t.Fatalf("expected unsupported-shape reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteRuntimePipeline(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: true}, false, true)
	if decision.route != queryRouteRuntimePipeline {
		t.Fatalf("expected supported runtime shape to route to runtime pipeline, got %#v", decision)
	}
	if decision.reason != "runtime_shape_supported" {
		t.Fatalf("expected supported-shape reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteDefaultRuntimeEnabledWithoutParam(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, map[string]any{"tenant": "acme"}, true, true)
	if decision.route != queryRouteRuntimePipeline {
		t.Fatalf("expected default runtime pipeline policy to route supported shape to runtime pipeline, got %#v", decision)
	}
}

func TestDecideQueryRouteDefaultRuntimeCanBeExplicitlyDisabled(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: false}, true, true)
	if decision.route != queryRouteLegacyExecutor {
		t.Fatalf("expected explicit false param to override default runtime policy, got %#v", decision)
	}
	if decision.reason != "runtime_pipeline_disabled" {
		t.Fatalf("expected runtime_pipeline_disabled reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteStringParamEnableDisable(t *testing.T) {
	enabled := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: "true"}, false, true)
	if enabled.route != queryRouteRuntimePipeline {
		t.Fatalf("expected string true to enable runtime pipeline, got %#v", enabled)
	}
	disabled := decideQueryRoute(&ast.QueryStatement{}, map[string]any{runtimePipelineParam: " false "}, true, true)
	if disabled.route != queryRouteLegacyExecutor {
		t.Fatalf("expected string false to disable runtime pipeline, got %#v", disabled)
	}
}

func TestDecideExplainRouteNilStatement(t *testing.T) {
	decision := decideExplainRoute(nil, nil, false)
	if decision.route != explainRouteLegacyPayload {
		t.Fatalf("expected nil explain statement to route to legacy payload, got %#v", decision)
	}
	if decision.reason != "nil_statement" {
		t.Fatalf("expected nil explain reason, got %#v", decision.reason)
	}
}

func TestDecideExplainRouteLegacyDefault(t *testing.T) {
	decision := decideExplainRoute(&ast.ExplainStatement{Statement: &ast.QueryStatement{}}, map[string]any{"tenant": "acme"}, false)
	if decision.route != explainRouteLegacyPayload {
		t.Fatalf("expected explain route to remain legacy payload by default, got %#v", decision)
	}
	if decision.reason != "legacy_payload_default" {
		t.Fatalf("expected legacy default explain reason, got %#v", decision.reason)
	}
}

func TestDecideExplainRoutePipelineOptIn(t *testing.T) {
	decision := decideExplainRoute(&ast.ExplainStatement{Statement: &ast.QueryStatement{}}, map[string]any{"tenant": "acme"}, true)
	if decision.route != explainRoutePipelinePayload {
		t.Fatalf("expected opt-in explain route to select pipeline payload, got %#v", decision)
	}
	if decision.reason != "pipeline_payload_opt_in" {
		t.Fatalf("expected pipeline opt-in reason, got %#v", decision.reason)
	}
}
