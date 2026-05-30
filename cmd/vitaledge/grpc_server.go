package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	v1 "github.com/paegun/vitaledge/api/proto/vitaledge/v1"
	"github.com/paegun/vitaledge/internal/cypher"
	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	grpcSupportedParserVersion = "cypher-m23"
	grpcSupportedIRVersion     = "query-pipeline-v1"
)

type grpcQueryHandler struct {
	executor           *executor.Executor
	defaultTenant      string
	maxWriteBatchBytes int64
}

func startGRPCServer(listenAddress string, handler v1.QueryServiceServer) (*grpc.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", strings.TrimSpace(listenAddress))
	if err != nil {
		return nil, nil, err
	}

	srv := grpc.NewServer()
	v1.RegisterQueryServiceServer(srv, handler)

	go func() {
		_ = srv.Serve(ln)
	}()

	return srv, ln, nil
}
func (h *grpcQueryHandler) Execute(ctx context.Context, req *v1.QueryRequest) (*v1.QueryResponse, error) {
	tenant, query, err := grpcExtractTenantAndQuery(req, h.defaultTenant)
	if err != nil {
		return nil, err
	}

	params, err := grpcProtoParamsToExecutorParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parameter value: %v", err)
	}

	result, err := h.executeStatement(ctx, tenant, query, params)
	if err != nil {
		return nil, err
	}

	rows := make([]*v1.Row, 0, len(result.Rows))
	for _, row := range result.Rows {
		rowValues := make(map[string]*v1.Value, len(row))
		for key, rawValue := range row {
			converted, err := grpcAnyToProtoValue(rawValue)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to encode row value for %q: %v", key, err)
			}
			rowValues[key] = converted
		}
		rows = append(rows, &v1.Row{Values: rowValues})
	}

	return &v1.QueryResponse{
		Columns: result.Columns,
		Rows:    rows,
		Stats: &v1.QueryStats{
			RowsReturned: int64(result.Stats.RowsReturned),
			DurationMs:   grpcDurationMs(result.Stats.Duration),
		},
	}, nil
}

func (h *grpcQueryHandler) Explain(ctx context.Context, req *v1.QueryRequest) (*v1.ExplainResponse, error) {
	tenant, query, err := grpcExtractTenantAndQuery(req, h.defaultTenant)
	if err != nil {
		return nil, err
	}
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "EXPLAIN") {
		query = "EXPLAIN " + strings.TrimSpace(query)
	}

	params, err := grpcProtoParamsToExecutorParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parameter value: %v", err)
	}

	result, err := h.executeStatement(ctx, tenant, query, params)
	if err != nil {
		return nil, err
	}

	explainPayload := map[string]any{}
	if len(result.Rows) > 0 {
		if rawExplain, ok := result.Rows[0]["explain"]; ok {
			if mapped, ok := rawExplain.(map[string]any); ok {
				explainPayload = mapped
			}
		}
	}

	explainJSON, err := json.Marshal(explainPayload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to encode explain payload: %v", err)
	}

	return &v1.ExplainResponse{
		ExplainJson: explainJSON,
		Stats: &v1.QueryStats{
			RowsReturned: int64(result.Stats.RowsReturned),
			DurationMs:   grpcDurationMs(result.Stats.Duration),
		},
	}, nil
}

func grpcDurationMs(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	ms := duration.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}

func (h *grpcQueryHandler) GetCapabilities(_ context.Context, _ *v1.CapabilitiesRequest) (*v1.CapabilitiesResponse, error) {
	maxWriteBatchBytes := h.maxWriteBatchBytes
	if maxWriteBatchBytes <= 0 {
		maxWriteBatchBytes = int64(pebblestore.DefaultMaxWriteBatchBytes)
	}
	return &v1.CapabilitiesResponse{
		ProtocolVersion:        "v1",
		ParserVersions:         []string{grpcSupportedParserVersion},
		IrVersions:             []string{grpcSupportedIRVersion},
		PreparedQuerySupported: true,
		ParameterBinding:       "server_side",
		IndexDdlSupported:      true,
		MaxWriteBatchBytes:     maxWriteBatchBytes,
	}, nil
}

func (h *grpcQueryHandler) CreatePropertyIndex(ctx context.Context, req *v1.CreatePropertyIndexRequest) (*v1.CreatePropertyIndexResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if h == nil || h.executor == nil {
		return nil, status.Error(codes.FailedPrecondition, "executor is not configured")
	}

	tenant := strings.TrimSpace(h.defaultTenant)
	if override := strings.TrimSpace(req.GetTenant()); override != "" {
		tenant = override
	}
	if tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}

	created, indexedEntities, err := h.executor.CreatePropertyIndex(
		ctx,
		tenant,
		strings.TrimSpace(req.GetSchema()),
		strings.TrimSpace(req.GetProperty()),
		req.GetIfNotExists(),
	)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindInvalidInput) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if graph.IsKind(err, graph.ErrKindConflict) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &v1.CreatePropertyIndexResponse{
		Created:         created,
		IndexedEntities: int64(indexedEntities),
	}, nil
}

func (h *grpcQueryHandler) executeStatement(ctx context.Context, tenant, query string, params executor.Params) (*executor.Result, error) {
	if h == nil || h.executor == nil {
		return nil, status.Error(codes.FailedPrecondition, "executor is not configured")
	}

	stmt, err := cypher.ParseStatement(strings.TrimSpace(query))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if params == nil {
		params = executor.Params{}
	}
	params["tenant"] = tenant

	result, err := h.executor.ExecuteStatement(ctx, stmt, params)
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}
	return result, nil
}

func grpcExtractTenantAndQuery(req *v1.QueryRequest, defaultTenant string) (string, string, error) {
	if req == nil {
		return "", "", status.Error(codes.InvalidArgument, "request is required")
	}
	tenant := strings.TrimSpace(defaultTenant)
	if strings.TrimSpace(req.GetTenant()) != "" {
		tenant = strings.TrimSpace(req.GetTenant())
	}
	if tenant == "" {
		return "", "", status.Error(codes.InvalidArgument, "tenant is required")
	}

	if req.GetInput() == nil {
		return "", "", status.Error(codes.InvalidArgument, "query input is required")
	}
	if prepared := req.GetInput().GetPrepared(); prepared != nil {
		if grpcPreparedVersionsCompatible(prepared) {
			query := strings.TrimSpace(string(prepared.GetPayload()))
			if query == "" {
				return "", "", status.Error(codes.InvalidArgument, "prepared payload is empty")
			}
			return tenant, query, nil
		}
		if req.GetOptions().GetAllowFallbackToCypher() {
			fallback := strings.TrimSpace(prepared.GetFallbackCypher())
			if fallback != "" {
				return tenant, fallback, nil
			}
		}
		return "", "", status.Error(codes.FailedPrecondition, "prepared query version mismatch; fallback not available")
	}
	query := strings.TrimSpace(req.GetInput().GetCypher())
	if query == "" {
		return "", "", status.Error(codes.InvalidArgument, "query input.cypher is required")
	}
	return tenant, query, nil
}

func grpcPreparedVersionsCompatible(prepared *v1.PreparedQuery) bool {
	if prepared == nil {
		return false
	}
	if parser := strings.TrimSpace(prepared.GetParserVersion()); parser != "" && parser != grpcSupportedParserVersion {
		return false
	}
	if ir := strings.TrimSpace(prepared.GetIrVersion()); ir != "" && ir != grpcSupportedIRVersion {
		return false
	}
	return true
}

func grpcAnyToProtoValue(raw any) (*v1.Value, error) {
	switch typed := raw.(type) {
	case nil:
		return &v1.Value{Kind: &v1.Value_NullValue{NullValue: &v1.NullValue{}}}, nil
	case bool:
		return &v1.Value{Kind: &v1.Value_BoolValue{BoolValue: typed}}, nil
	case int:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case int8:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case int16:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case int32:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case int64:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: typed}}, nil
	case uint:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case uint8:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case uint16:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case uint32:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case uint64:
		return &v1.Value{Kind: &v1.Value_IntValue{IntValue: int64(typed)}}, nil
	case float32:
		return &v1.Value{Kind: &v1.Value_DoubleValue{DoubleValue: float64(typed)}}, nil
	case float64:
		return &v1.Value{Kind: &v1.Value_DoubleValue{DoubleValue: typed}}, nil
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return &v1.Value{Kind: &v1.Value_IntValue{IntValue: integer}}, nil
		}
		if decimal, err := typed.Float64(); err == nil {
			return &v1.Value{Kind: &v1.Value_DoubleValue{DoubleValue: decimal}}, nil
		}
		return nil, fmt.Errorf("invalid json number: %q", typed.String())
	case string:
		return &v1.Value{Kind: &v1.Value_StringValue{StringValue: typed}}, nil
	case []byte:
		return &v1.Value{Kind: &v1.Value_BytesValue{BytesValue: typed}}, nil
	case []string:
		values := make([]*v1.Value, 0, len(typed))
		for _, item := range typed {
			values = append(values, &v1.Value{Kind: &v1.Value_StringValue{StringValue: item}})
		}
		return &v1.Value{Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: values}}}, nil
	case []any:
		values := make([]*v1.Value, 0, len(typed))
		for _, item := range typed {
			converted, err := grpcAnyToProtoValue(item)
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		return &v1.Value{Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: values}}}, nil
	case map[string]any:
		values := make(map[string]*v1.Value, len(typed))
		for key, item := range typed {
			converted, err := grpcAnyToProtoValue(item)
			if err != nil {
				return nil, err
			}
			values[key] = converted
		}
		return &v1.Value{Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: values}}}, nil
	case executor.Row:
		return grpcAnyToProtoValue(map[string]any(typed))
	case *graph.Vertex, *graph.Edge:
		return grpcAnyToProtoValue(grpcNormalizeToGenericMap(typed))
	default:
		return grpcAnyToProtoValue(grpcNormalizeToGenericMap(typed))
	}
}

func grpcProtoParamsToExecutorParams(protoParams map[string]*v1.Value) (executor.Params, error) {
	if len(protoParams) == 0 {
		return executor.Params{}, nil
	}
	params := make(executor.Params, len(protoParams))
	for k, v := range protoParams {
		converted, err := grpcProtoValueToAny(v)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", k, err)
		}
		params[k] = converted
	}
	return params, nil
}

func grpcProtoValueToAny(v *v1.Value) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch typed := v.GetKind().(type) {
	case *v1.Value_NullValue:
		return nil, nil
	case *v1.Value_BoolValue:
		return typed.BoolValue, nil
	case *v1.Value_IntValue:
		return typed.IntValue, nil
	case *v1.Value_DoubleValue:
		return typed.DoubleValue, nil
	case *v1.Value_StringValue:
		return typed.StringValue, nil
	case *v1.Value_BytesValue:
		return typed.BytesValue, nil
	case *v1.Value_ListValue:
		if typed.ListValue == nil {
			return []any{}, nil
		}
		list := make([]any, 0, len(typed.ListValue.GetValues()))
		for _, item := range typed.ListValue.GetValues() {
			converted, err := grpcProtoValueToAny(item)
			if err != nil {
				return nil, err
			}
			list = append(list, converted)
		}
		return list, nil
	case *v1.Value_MapValue:
		if typed.MapValue == nil {
			return map[string]any{}, nil
		}
		m := make(map[string]any, len(typed.MapValue.GetValues()))
		for k, item := range typed.MapValue.GetValues() {
			converted, err := grpcProtoValueToAny(item)
			if err != nil {
				return nil, err
			}
			m[k] = converted
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported value kind: %T", typed)
	}
}

func grpcNormalizeToGenericMap(raw any) map[string]any {
	if raw == nil {
		return map[string]any{}
	}
	buf, err := json.Marshal(raw)
	if err != nil {
		return map[string]any{"value": "<unserializable>"}
	}
	out := map[string]any{}
	if err := json.Unmarshal(buf, &out); err != nil {
		return map[string]any{"value": "<unserializable>"}
	}
	return out
}
