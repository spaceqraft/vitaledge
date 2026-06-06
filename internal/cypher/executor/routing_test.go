package executor

import (
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

func TestDecideQueryRouteNilStatement(t *testing.T) {
	decision := decideQueryRoute(nil, true)
	if decision.route != queryRouteUnsupported {
		t.Fatalf("expected nil statement to route to unsupported, got %#v", decision)
	}
	if decision.reason != "nil_statement" {
		t.Fatalf("expected nil statement reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteUnsupportedShape(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, false)
	if decision.route != queryRouteUnsupported {
		t.Fatalf("expected unsupported runtime shape to route to unsupported, got %#v", decision)
	}
	if decision.reason != "runtime_shape_unsupported" {
		t.Fatalf("expected unsupported-shape reason, got %#v", decision.reason)
	}
}

func TestDecideQueryRouteRuntimePipeline(t *testing.T) {
	decision := decideQueryRoute(&ast.QueryStatement{}, true)
	if decision.route != queryRouteRuntimePipeline {
		t.Fatalf("expected supported runtime shape to route to runtime pipeline, got %#v", decision)
	}
	if decision.reason != "runtime_shape_supported" {
		t.Fatalf("expected supported-shape reason, got %#v", decision.reason)
	}
}
