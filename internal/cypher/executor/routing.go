package executor

import "github.com/paegun/vitaledge/internal/cypher/ast"

type queryRoute string

const (
	queryRouteUnsupported     queryRoute = "unsupported"
	queryRouteRuntimePipeline queryRoute = "runtime_pipeline"
)

type queryRouteDecision struct {
	route  queryRoute
	reason string
}

func decideQueryRoute(stmt *ast.QueryStatement, runtimeSupported bool) queryRouteDecision {
	if stmt == nil {
		return queryRouteDecision{route: queryRouteUnsupported, reason: "nil_statement"}
	}
	if !runtimeSupported {
		return queryRouteDecision{route: queryRouteUnsupported, reason: "runtime_shape_unsupported"}
	}
	return queryRouteDecision{route: queryRouteRuntimePipeline, reason: "runtime_shape_supported"}
}
