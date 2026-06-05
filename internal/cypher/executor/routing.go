package executor

import (
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

type queryRoute string

const (
	queryRouteLegacyExecutor  queryRoute = "legacy_executor"
	queryRouteRuntimePipeline queryRoute = "runtime_pipeline"
)

type queryRouteDecision struct {
	route  queryRoute
	reason string
}

type explainRoute string

const (
	explainRouteLegacyPayload   explainRoute = "legacy_payload"
	explainRoutePipelinePayload explainRoute = "pipeline_payload"
)

type explainRouteDecision struct {
	route  explainRoute
	reason string
}

const runtimePipelineParam = "__ve_use_runtime_pipeline"

func decideQueryRoute(stmt *ast.QueryStatement, params map[string]any, defaultRuntimePipeline bool, runtimeSupported bool) queryRouteDecision {
	if stmt == nil {
		return queryRouteDecision{route: queryRouteLegacyExecutor, reason: "nil_statement"}
	}
	runtimeEnabled := isRuntimePipelineRequested(params, defaultRuntimePipeline)
	if !runtimeEnabled {
		return queryRouteDecision{route: queryRouteLegacyExecutor, reason: "runtime_pipeline_disabled"}
	}
	if !runtimeSupported {
		return queryRouteDecision{route: queryRouteLegacyExecutor, reason: "runtime_shape_unsupported"}
	}
	return queryRouteDecision{route: queryRouteRuntimePipeline, reason: "runtime_shape_supported"}
}

func decideExplainRoute(stmt *ast.ExplainStatement, params map[string]any, preferPipelinePayload bool) explainRouteDecision {
	_ = params
	if stmt == nil || stmt.Statement == nil {
		return explainRouteDecision{route: explainRouteLegacyPayload, reason: "nil_statement"}
	}
	if preferPipelinePayload {
		return explainRouteDecision{route: explainRoutePipelinePayload, reason: "pipeline_payload_opt_in"}
	}
	return explainRouteDecision{route: explainRouteLegacyPayload, reason: "legacy_payload_default"}
}

func isRuntimePipelineRequested(params map[string]any, defaultEnabled bool) bool {
	if !defaultEnabled {
		return isRuntimePipelineExplicitlyEnabled(params)
	}
	if params == nil {
		return true
	}
	value, ok := params[runtimePipelineParam]
	if !ok || value == nil {
		return true
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		trimmed := strings.TrimSpace(typed)
		if strings.EqualFold(trimmed, "true") {
			return true
		}
		if strings.EqualFold(trimmed, "false") {
			return false
		}
		return false
	default:
		return false
	}
}

func isRuntimePipelineExplicitlyEnabled(params map[string]any) bool {
	if params == nil {
		return false
	}
	value, ok := params[runtimePipelineParam]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}
