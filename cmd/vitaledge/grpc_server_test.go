package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/spaceqraft/vitaledge/api/proto/vitaledge/v1"
	"github.com/spaceqraft/vitaledge/internal/cypher/executor"
	cypherruntime "github.com/spaceqraft/vitaledge/internal/cypher/runtime"
	"github.com/spaceqraft/vitaledge/internal/graph"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type capturedGRPCExecution struct {
	query  string
	params executor.Params
}

func newGRPCTestClient(t *testing.T, ddlHandler v1.DdlServiceServer, dmlHandler v1.DmlServiceServer) (v1.DmlServiceClient, func()) {
	t.Helper()

	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0", ddlHandler, dmlHandler)
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcSrv.GracefulStop()
		_ = grpcLn.Close()
		t.Fatalf("grpc dial failed: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcSrv.GracefulStop()
		_ = grpcLn.Close()
	}

	return v1.NewDmlServiceClient(conn), cleanup
}

func cloneExecutionParams(params executor.Params) executor.Params {
	copied := make(executor.Params, len(params))
	for key, value := range params {
		copied[key] = value
	}
	return copied
}

func captureExecution(mu *sync.Mutex, captured *[]capturedGRPCExecution, query string, params executor.Params) {
	mu.Lock()
	*captured = append(*captured, capturedGRPCExecution{query: query, params: cloneExecutionParams(params)})
	mu.Unlock()
}

type grpcBoundaryEndpointCase struct {
	name          string
	expectedQuery string
	call          func(context.Context, v1.DmlServiceClient, map[string]*v1.Value, *v1.RequestOptions) error
}

type grpcBoundaryScenarioCase struct {
	name               string
	parameters         map[string]*v1.Value
	options            *v1.RequestOptions
	expectErrorCode    codes.Code
	expectErrorMessage string
	expectCaptureCount int
	expectStrictExists *bool
	expectStrictValue  *bool
}

func boolPtr(v bool) *bool {
	return &v
}

func grpcExpectedQueryForEndpoint(endpoint, cypher string) string {
	if endpoint == "explain" {
		return "EXPLAIN " + cypher
	}
	return cypher
}

func buildGRPCBoundaryEndpoints(
	cypher string,
	buildRequest func(endpoint string, params map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest,
) []grpcBoundaryEndpointCase {
	endpoints := []string{"execute", "explain"}
	result := make([]grpcBoundaryEndpointCase, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint := endpoint
		result = append(result, grpcBoundaryEndpointCase{
			name:          endpoint,
			expectedQuery: grpcExpectedQueryForEndpoint(endpoint, cypher),
			call: func(ctx context.Context, client v1.DmlServiceClient, params map[string]*v1.Value, options *v1.RequestOptions) error {
				req := buildRequest(endpoint, params, options)
				if req == nil {
					return status.Error(codes.Internal, "boundary request builder returned nil")
				}
				if strings.TrimSpace(req.GetTenant()) == "" {
					req.Tenant = "acme"
				}
				if endpoint == "execute" {
					_, err := client.Execute(ctx, req)
					return err
				}
				_, err := client.Explain(ctx, req)
				return err
			},
		})
	}
	return result
}

func runGRPCBoundaryMatrix(
	t *testing.T,
	endpoints []grpcBoundaryEndpointCase,
	scenarios []grpcBoundaryScenarioCase,
) {
	t.Helper()

	for _, endpoint := range endpoints {
		endpoint := endpoint
		for _, scenario := range scenarios {
			scenario := scenario
			t.Run(endpoint.name+"/"+scenario.name, func(t *testing.T) {
				var mu sync.Mutex
				captured := make([]capturedGRPCExecution, 0, 1)
				hook := func(_ context.Context, _ string, query string, params executor.Params) (*executor.Result, error) {
					captureExecution(&mu, &captured, query, params)

					if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "EXPLAIN") {
						return &executor.Result{
							Columns: []string{"explain"},
							Rows:    []executor.Row{{"explain": map[string]any{"ok": true}}},
						}, nil
					}
					return &executor.Result{
						Columns: []string{"id"},
						Rows:    []executor.Row{{"id": "seed"}},
					}, nil
				}

				client, cleanup := newGRPCTestClient(t,
					&grpcDdlHandler{
						defaultTenant: "acme",
					},
					&grpcDmlHandler{
						defaultTenant:        "acme",
						executeStatementHook: hook,
					})
				defer cleanup()

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				err := endpoint.call(ctx, client, scenario.parameters, scenario.options)
				if scenario.expectErrorCode == codes.OK {
					if err != nil {
						t.Fatalf("expected success, got %v", err)
					}
				} else {
					if err == nil {
						t.Fatalf("expected %v error", scenario.expectErrorCode)
					}
					st, ok := status.FromError(err)
					if !ok {
						t.Fatalf("expected grpc status error, got %v", err)
					}
					if st.Code() != scenario.expectErrorCode {
						t.Fatalf("expected %v, got %v (%v)", scenario.expectErrorCode, st.Code(), st.Message())
					}
					if scenario.expectErrorMessage != "" && !strings.Contains(st.Message(), scenario.expectErrorMessage) {
						t.Fatalf("expected error message to contain %q, got %q", scenario.expectErrorMessage, st.Message())
					}
				}

				mu.Lock()
				defer mu.Unlock()
				if len(captured) != scenario.expectCaptureCount {
					t.Fatalf("unexpected capture count: got %d want %d", len(captured), scenario.expectCaptureCount)
				}
				if scenario.expectCaptureCount == 0 {
					return
				}
				if endpoint.expectedQuery != "" && strings.TrimSpace(captured[0].query) != endpoint.expectedQuery {
					t.Fatalf("unexpected executed query: got %q want %q", captured[0].query, endpoint.expectedQuery)
				}

				value, exists := captured[0].params[cypherruntime.StrictVariantDispatchParam]
				if scenario.expectStrictExists != nil && exists != *scenario.expectStrictExists {
					t.Fatalf("strict param existence mismatch: got exists=%v, want %v (params=%#v)", exists, *scenario.expectStrictExists, captured[0].params)
				}
				if scenario.expectStrictValue == nil {
					return
				}
				if !exists {
					t.Fatalf("expected strict param value %v but parameter was absent", *scenario.expectStrictValue)
				}
				boolValue, ok := value.(bool)
				if !ok {
					t.Fatalf("strict param should be bool, got %#v", value)
				}
				if boolValue != *scenario.expectStrictValue {
					t.Fatalf("strict param value mismatch: got %v, want %v", boolValue, *scenario.expectStrictValue)
				}
			})
		}
	}
}

func TestGRPCApplyRequestOptionsToParamsStrictVariantDispatch(t *testing.T) {
	t.Run("nil options strips injected strict param", func(t *testing.T) {
		params := executor.Params{"tenant": "acme", cypherruntime.StrictVariantDispatchParam: true}
		out := grpcApplyRequestOptionsToParams(nil, params)
		if got := out["tenant"]; got != "acme" {
			t.Fatalf("expected tenant param preserved, got %#v", out)
		}
		if _, exists := out[cypherruntime.StrictVariantDispatchParam]; exists {
			t.Fatalf("expected strict variant dispatch param to be stripped when options are nil, got %#v", out)
		}
	})

	t.Run("nil options keeps unrelated params", func(t *testing.T) {
		params := executor.Params{"tenant": "acme"}
		out := grpcApplyRequestOptionsToParams(nil, params)
		if got := out["tenant"]; got != "acme" {
			t.Fatalf("expected tenant param preserved, got %#v", out)
		}
		if _, exists := out[cypherruntime.StrictVariantDispatchParam]; exists {
			t.Fatalf("did not expect strict variant dispatch param when options are nil, got %#v", out)
		}
	})

	t.Run("explicit true sets strict param true", func(t *testing.T) {
		params := executor.Params{"tenant": "acme"}
		out := grpcApplyRequestOptionsToParams(&v1.RequestOptions{StrictVariantDispatch: true}, params)
		if got, _ := out[cypherruntime.StrictVariantDispatchParam].(bool); !got {
			t.Fatalf("expected strict variant dispatch param true, got %#v", out)
		}
	})

	t.Run("explicit false sets strict param false", func(t *testing.T) {
		params := executor.Params{"tenant": "acme", cypherruntime.StrictVariantDispatchParam: true}
		out := grpcApplyRequestOptionsToParams(&v1.RequestOptions{StrictVariantDispatch: false}, params)
		if got, _ := out[cypherruntime.StrictVariantDispatchParam].(bool); got {
			t.Fatalf("expected strict variant dispatch param false override, got %#v", out)
		}
	})
}

func TestGRPCRequestOptionsStrictVariantDispatchRPCMatrix(t *testing.T) {
	cypher := "MATCH (n:Seed) RETURN n.id AS id"
	endpoints := buildGRPCBoundaryEndpoints(cypher, func(_ string, _ map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest {
		return &v1.QueryRequest{
			Tenant:  "acme",
			Input:   &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: cypher}},
			Options: options,
		}
	})

	scenarios := []grpcBoundaryScenarioCase{
		{name: "strict-unset", options: nil, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(false)},
		{name: "strict-true", options: &v1.RequestOptions{StrictVariantDispatch: true}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(true)},
		{name: "strict-false", options: &v1.RequestOptions{StrictVariantDispatch: false}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(false)},
	}

	runGRPCBoundaryMatrix(t, endpoints, scenarios)
}

func runGRPCReservedInternalParamsBoundaryMatrix(
	t *testing.T,
	endpoints []grpcBoundaryEndpointCase,
) {
	t.Helper()

	scenarios := []grpcBoundaryScenarioCase{
		{
			name:               "reserved-param-rejected",
			parameters:         map[string]*v1.Value{"__ve_strict_variant_dispatch": {Kind: &v1.Value_BoolValue{BoolValue: true}}},
			options:            &v1.RequestOptions{StrictVariantDispatch: true},
			expectErrorCode:    codes.InvalidArgument,
			expectErrorMessage: "reserved internal prefix __ve_",
			expectCaptureCount: 0,
		},
		{
			name:               "reserved-valid-strict-true",
			parameters:         map[string]*v1.Value{"limit": {Kind: &v1.Value_IntValue{IntValue: 1}}},
			options:            &v1.RequestOptions{StrictVariantDispatch: true},
			expectErrorCode:    codes.OK,
			expectCaptureCount: 1,
			expectStrictExists: boolPtr(true),
			expectStrictValue:  boolPtr(true),
		},
	}

	runGRPCBoundaryMatrix(t, endpoints, scenarios)
}

func TestGRPCReservedInternalParamsRejectedBeforeExecutionHook(t *testing.T) {
	cypher := "MATCH (n:Seed) RETURN n.id AS id"
	endpoints := buildGRPCBoundaryEndpoints(cypher, func(_ string, parameters map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest {
		return &v1.QueryRequest{
			Tenant:     "acme",
			Input:      &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: cypher}},
			Parameters: parameters,
			Options:    options,
		}
	})

	runGRPCReservedInternalParamsBoundaryMatrix(t, endpoints)
}

func TestGRPCExecutePreparedFallbackStrictVariantDispatchMatrix(t *testing.T) {
	cypher := "MATCH (n:Seed) RETURN n.id AS id"
	endpoints := buildGRPCBoundaryEndpoints(cypher, func(endpoint string, _ map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest {
		if endpoint != "execute" {
			return nil
		}
		prepared := &v1.PreparedQuery{ParserVersion: "cypher-m99", IrVersion: "query-pipeline-v99", Payload: []byte(cypher)}
		if options != nil && options.GetAllowFallbackToCypher() {
			prepared = &v1.PreparedQuery{
				ParserVersion:  "cypher-m99",
				IrVersion:      "query-pipeline-v99",
				Payload:        []byte("MATCH (n:Never) RETURN n.id AS id"),
				FallbackCypher: cypher,
			}
		}
		return &v1.QueryRequest{Tenant: "acme", Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: prepared}}, Options: options}
	})[:1]

	scenarios := []grpcBoundaryScenarioCase{
		{name: "prepared-mismatch-no-fallback", options: &v1.RequestOptions{StrictVariantDispatch: true}, expectErrorCode: codes.FailedPrecondition, expectCaptureCount: 0},
		{name: "prepared-fallback-strict-true", options: &v1.RequestOptions{AllowFallbackToCypher: true, StrictVariantDispatch: true}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(true)},
		{name: "prepared-fallback-strict-false", options: &v1.RequestOptions{AllowFallbackToCypher: true, StrictVariantDispatch: false}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(false)},
	}

	runGRPCBoundaryMatrix(t, endpoints, scenarios)
}

func TestGRPCExplainPreparedFallbackStrictVariantDispatchMatrix(t *testing.T) {
	cypher := "MATCH (n:Seed) RETURN n.id AS id"
	endpoints := buildGRPCBoundaryEndpoints(cypher, func(endpoint string, _ map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest {
		if endpoint != "explain" {
			return nil
		}
		prepared := &v1.PreparedQuery{ParserVersion: "cypher-m99", IrVersion: "query-pipeline-v99", Payload: []byte(cypher)}
		if options != nil && options.GetAllowFallbackToCypher() {
			prepared = &v1.PreparedQuery{
				ParserVersion:  "cypher-m99",
				IrVersion:      "query-pipeline-v99",
				Payload:        []byte("MATCH (n:Never) RETURN n.id AS id"),
				FallbackCypher: cypher,
			}
		}
		return &v1.QueryRequest{Tenant: "acme", Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: prepared}}, Options: options}
	})[1:]

	scenarios := []grpcBoundaryScenarioCase{
		{name: "prepared-mismatch-no-fallback", options: &v1.RequestOptions{StrictVariantDispatch: true}, expectErrorCode: codes.FailedPrecondition, expectCaptureCount: 0},
		{name: "prepared-fallback-strict-true", options: &v1.RequestOptions{AllowFallbackToCypher: true, StrictVariantDispatch: true}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(true)},
		{name: "prepared-fallback-strict-false", options: &v1.RequestOptions{AllowFallbackToCypher: true, StrictVariantDispatch: false}, expectErrorCode: codes.OK, expectCaptureCount: 1, expectStrictExists: boolPtr(true), expectStrictValue: boolPtr(false)},
	}

	runGRPCBoundaryMatrix(t, endpoints, scenarios)
}

func TestGRPCPreparedRequestsRejectReservedInternalParamsBeforeExecutionHookMatrix(t *testing.T) {
	cypher := "MATCH (n:Seed) RETURN n.id AS id"
	endpoints := buildGRPCBoundaryEndpoints(cypher, func(_ string, params map[string]*v1.Value, options *v1.RequestOptions) *v1.QueryRequest {
		return &v1.QueryRequest{
			Tenant: "acme",
			Input: &v1.QueryInput{Kind: &v1.QueryInput_Prepared{Prepared: &v1.PreparedQuery{
				ParserVersion: "cypher-m23",
				IrVersion:     "query-pipeline-v1",
				Payload:       []byte(cypher),
			}}},
			Parameters: params,
			Options:    options,
		}
	})

	runGRPCReservedInternalParamsBoundaryMatrix(t, endpoints)
}

func TestGRPCDurationMs(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected int64
	}{
		{name: "zero", input: 0, expected: 0},
		{name: "negative", input: -1 * time.Millisecond, expected: 0},
		{name: "sub-millisecond rounds up", input: 500 * time.Microsecond, expected: 1},
		{name: "exact millisecond", input: 1 * time.Millisecond, expected: 1},
		{name: "multi-millisecond", input: 123 * time.Millisecond, expected: 123},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := grpcDurationMs(tc.input); got != tc.expected {
				t.Fatalf("grpcDurationMs(%s)=%d, want %d", tc.input, got, tc.expected)
			}
		})
	}
}

func TestGRPCAnyToProtoValueJSONNumberInteger(t *testing.T) {
	converted, err := grpcAnyToProtoValue(json.Number("42"))
	if err != nil {
		t.Fatalf("grpcAnyToProtoValue returned error: %v", err)
	}
	if _, ok := converted.GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected int_value kind, got %T", converted.GetKind())
	}
	if got := converted.GetIntValue(); got != 42 {
		t.Fatalf("expected int_value=42, got %d", got)
	}
}

func TestGRPCAnyToProtoValueJSONNumberFloat(t *testing.T) {
	converted, err := grpcAnyToProtoValue(json.Number("3.5"))
	if err != nil {
		t.Fatalf("grpcAnyToProtoValue returned error: %v", err)
	}
	if _, ok := converted.GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected double_value kind, got %T", converted.GetKind())
	}
	if got := converted.GetDoubleValue(); got != 3.5 {
		t.Fatalf("expected double_value=3.5, got %v", got)
	}
}

func TestGRPCAnyToProtoValueJSONNumberNestedInMapAndList(t *testing.T) {
	input := map[string]any{
		"avg":  json.Number("2.75"),
		"max":  json.Number("9"),
		"vals": []any{json.Number("1"), json.Number("1.5")},
	}

	converted, err := grpcAnyToProtoValue(input)
	if err != nil {
		t.Fatalf("grpcAnyToProtoValue returned error: %v", err)
	}

	mapKind, ok := converted.GetKind().(*v1.Value_MapValue)
	if !ok || mapKind.MapValue == nil {
		t.Fatalf("expected map_value kind, got %T", converted.GetKind())
	}

	avg := mapKind.MapValue.Values["avg"]
	if _, ok := avg.GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected avg as double_value kind, got %T", avg.GetKind())
	}
	if got := avg.GetDoubleValue(); got != 2.75 {
		t.Fatalf("expected avg=2.75, got %v", got)
	}

	mx := mapKind.MapValue.Values["max"]
	if _, ok := mx.GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected max as int_value kind, got %T", mx.GetKind())
	}
	if got := mx.GetIntValue(); got != 9 {
		t.Fatalf("expected max=9, got %d", got)
	}

	vals := mapKind.MapValue.Values["vals"]
	listKind, ok := vals.GetKind().(*v1.Value_ListValue)
	if !ok || listKind.ListValue == nil {
		t.Fatalf("expected vals as list_value kind, got %T", vals.GetKind())
	}
	if len(listKind.ListValue.Values) != 2 {
		t.Fatalf("expected 2 list values, got %d", len(listKind.ListValue.Values))
	}
	if _, ok := listKind.ListValue.Values[0].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected first list value as int_value kind, got %T", listKind.ListValue.Values[0].GetKind())
	}
	if got := listKind.ListValue.Values[0].GetIntValue(); got != 1 {
		t.Fatalf("expected first list value=1, got %d", got)
	}
	if _, ok := listKind.ListValue.Values[1].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected second list value as double_value kind, got %T", listKind.ListValue.Values[1].GetKind())
	}
	if got := listKind.ListValue.Values[1].GetDoubleValue(); got != 1.5 {
		t.Fatalf("expected second list value=1.5, got %v", got)
	}
}

func TestGRPCAnyToProtoValueJSONNumberInvalid(t *testing.T) {
	_, err := grpcAnyToProtoValue(json.Number("not-a-number"))
	if err == nil {
		t.Fatalf("expected error for invalid json.Number")
	}
}

func TestGRPCProtoParamsToExecutorParamsBoundaryIDValidation(t *testing.T) {
	t.Run("rejects reserved internal key prefix", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"__ve_strict_variant_dispatch": {Kind: &v1.Value_BoolValue{BoolValue: true}},
		})
		if err == nil || !strings.Contains(err.Error(), "reserved internal prefix __ve_") {
			t.Fatalf("expected reserved internal prefix validation error, got %v", err)
		}
	})

	t.Run("rejects empty edgeId", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"edgeId": {Kind: &v1.Value_StringValue{StringValue: ""}},
		})
		if err == nil || !strings.Contains(err.Error(), "edgeId") {
			t.Fatalf("expected edgeId validation error, got %v", err)
		}
	})

	t.Run("rejects whitespace padded edgeId", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"edgeId": {Kind: &v1.Value_StringValue{StringValue: " e-1 "}},
		})
		if err == nil || !strings.Contains(err.Error(), "surrounding whitespace") {
			t.Fatalf("expected surrounding whitespace validation error, got %v", err)
		}
	})

	t.Run("rejects non-string edgeId", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"edgeId": {Kind: &v1.Value_DoubleValue{DoubleValue: 2.14}},
		})
		if err == nil || !strings.Contains(err.Error(), "string identifier") {
			t.Fatalf("expected string identifier validation error, got %v", err)
		}
	})

	t.Run("accepts nested id field under non-id top-level param", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"people": {
				Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
					{
						Kind: &v1.Value_MapValue{MapValue: &v1.MapValue{Values: map[string]*v1.Value{
							"id":   {Kind: &v1.Value_IntValue{IntValue: 1}},
							"name": {Kind: &v1.Value_StringValue{StringValue: "alice"}},
						}}},
					},
				}}},
			},
		})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("rejects top-level ids list containing non-string value", func(t *testing.T) {
		_, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"edgeIds": {
				Kind: &v1.Value_ListValue{ListValue: &v1.ListValue{Values: []*v1.Value{
					{Kind: &v1.Value_StringValue{StringValue: "e-1"}},
					{Kind: &v1.Value_DoubleValue{DoubleValue: 2.14}},
				}}},
			},
		})
		if err == nil || !strings.Contains(err.Error(), "edgeIds[1]") {
			t.Fatalf("expected edgeIds[1] string identifier validation error, got %v", err)
		}
	})

	t.Run("accepts valid edgeId", func(t *testing.T) {
		params, err := grpcProtoParamsToExecutorParams(map[string]*v1.Value{
			"edgeId": {Kind: &v1.Value_StringValue{StringValue: "e-1"}},
			"limit":  {Kind: &v1.Value_IntValue{IntValue: 10}},
		})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got := params["edgeId"]; got != "e-1" {
			t.Fatalf("expected edgeId=e-1, got %#v", got)
		}
	})
}

func TestGRPCExecuteIntegrationSerializesNumericAggregatesAndProperties(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(context.Background(), func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "n1", Labels: []string{"Metric"}, Properties: graph.PropertyMap{"value": []byte("1.5")}},
			{Tenant: "acme", ID: "n2", Labels: []string{"Metric"}, Properties: graph.PropertyMap{"value": []byte("2.5")}},
			{Tenant: "acme", ID: "n3", Labels: []string{"Metric"}, Properties: graph.PropertyMap{"value": []byte("4")}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(context.Background(), vertex); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed metrics vertices failed: %v", err)
	}

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0",
		&grpcDdlHandler{
			defaultTenant: "acme",
		},
		&grpcDmlHandler{
			executor:      exec,
			defaultTenant: "acme",
		})
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewDmlServiceClient(conn)

	aggResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (n:Metric) RETURN count(n) AS c, avg(n.value) AS a, max(n.value) AS mx, min(n.value) AS mn"}},
	})
	if err != nil {
		t.Fatalf("aggregate Execute failed: %v", err)
	}
	if len(aggResp.GetRows()) != 1 {
		t.Fatalf("expected one aggregate row, got %d", len(aggResp.GetRows()))
	}

	aggRow := aggResp.GetRows()[0].GetValues()
	if _, ok := aggRow["c"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected c as int_value kind, got %T", aggRow["c"].GetKind())
	}
	if got := aggRow["c"].GetIntValue(); got != 3 {
		t.Fatalf("expected c=3, got %d", got)
	}
	if _, ok := aggRow["a"].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected a as double_value kind, got %T", aggRow["a"].GetKind())
	}
	if got := aggRow["a"].GetDoubleValue(); got != 8.0/3.0 {
		t.Fatalf("expected a=%v, got %v", 8.0/3.0, got)
	}
	if _, ok := aggRow["mx"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected mx as int_value kind, got %T", aggRow["mx"].GetKind())
	}
	if got := aggRow["mx"].GetIntValue(); got != 4 {
		t.Fatalf("expected mx=4, got %d", got)
	}
	if _, ok := aggRow["mn"].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected mn as double_value kind, got %T", aggRow["mn"].GetKind())
	}
	if got := aggRow["mn"].GetDoubleValue(); got != 1.5 {
		t.Fatalf("expected mn=1.5, got %v", got)
	}

	propResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (n:Metric) RETURN n.value AS v ORDER BY n.id"}},
	})
	if err != nil {
		t.Fatalf("property Execute failed: %v", err)
	}
	if len(propResp.GetRows()) != 3 {
		t.Fatalf("expected three property rows, got %d", len(propResp.GetRows()))
	}

	for i, row := range propResp.GetRows() {
		value := row.GetValues()["v"]
		switch i {
		case 0, 1:
			if _, ok := value.GetKind().(*v1.Value_DoubleValue); !ok {
				t.Fatalf("expected row %d value as double_value kind, got %T", i, value.GetKind())
			}
			expected := []float64{1.5, 2.5}
			if got := value.GetDoubleValue(); got != expected[i] {
				t.Fatalf("expected row %d value=%v, got %v", i, expected[i], got)
			}
		case 2:
			if _, ok := value.GetKind().(*v1.Value_IntValue); !ok {
				t.Fatalf("expected row %d value as int_value kind, got %T", i, value.GetKind())
			}
			if got := value.GetIntValue(); got != 4 {
				t.Fatalf("expected row %d value=4, got %d", i, got)
			}
		default:
			t.Fatalf("unexpected row index %d", i)
		}
	}
}

func TestGRPCExecuteIntegrationSerializesIntegerOnlyAggregatesAndProperties(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(context.Background(), func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "i1", Labels: []string{"IntMetric"}, Properties: graph.PropertyMap{"value": []byte("1")}},
			{Tenant: "acme", ID: "i2", Labels: []string{"IntMetric"}, Properties: graph.PropertyMap{"value": []byte("2")}},
			{Tenant: "acme", ID: "i3", Labels: []string{"IntMetric"}, Properties: graph.PropertyMap{"value": []byte("4")}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(context.Background(), vertex); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed integer metrics vertices failed: %v", err)
	}

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0",
		&grpcDdlHandler{
			defaultTenant: "acme",
		},
		&grpcDmlHandler{
			executor:      exec,
			defaultTenant: "acme",
		})
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewDmlServiceClient(conn)

	aggResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (n:IntMetric) RETURN count(n) AS c, avg(n.value) AS a, max(n.value) AS mx, min(n.value) AS mn"}},
	})
	if err != nil {
		t.Fatalf("aggregate Execute failed: %v", err)
	}
	if len(aggResp.GetRows()) != 1 {
		t.Fatalf("expected one aggregate row, got %d", len(aggResp.GetRows()))
	}

	aggRow := aggResp.GetRows()[0].GetValues()
	if _, ok := aggRow["c"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected c as int_value kind, got %T", aggRow["c"].GetKind())
	}
	if got := aggRow["c"].GetIntValue(); got != 3 {
		t.Fatalf("expected c=3, got %d", got)
	}
	if _, ok := aggRow["a"].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected a as double_value kind, got %T", aggRow["a"].GetKind())
	}
	if got := aggRow["a"].GetDoubleValue(); got != 7.0/3.0 {
		t.Fatalf("expected a=%v, got %v", 7.0/3.0, got)
	}
	if _, ok := aggRow["mx"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected mx as int_value kind, got %T", aggRow["mx"].GetKind())
	}
	if got := aggRow["mx"].GetIntValue(); got != 4 {
		t.Fatalf("expected mx=4, got %d", got)
	}
	if _, ok := aggRow["mn"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected mn as int_value kind, got %T", aggRow["mn"].GetKind())
	}
	if got := aggRow["mn"].GetIntValue(); got != 1 {
		t.Fatalf("expected mn=1, got %d", got)
	}

	propResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (n:IntMetric) RETURN n.value AS v ORDER BY n.id"}},
	})
	if err != nil {
		t.Fatalf("property Execute failed: %v", err)
	}
	if len(propResp.GetRows()) != 3 {
		t.Fatalf("expected three property rows, got %d", len(propResp.GetRows()))
	}

	expected := []int64{1, 2, 4}
	for i, row := range propResp.GetRows() {
		value := row.GetValues()["v"]
		if _, ok := value.GetKind().(*v1.Value_IntValue); !ok {
			t.Fatalf("expected row %d value as int_value kind, got %T", i, value.GetKind())
		}
		if got := value.GetIntValue(); got != expected[i] {
			t.Fatalf("expected row %d value=%d, got %d", i, expected[i], got)
		}
	}
}

func TestGRPCExecuteIntegrationParameterizedThresholdAndLimit(t *testing.T) {
	store := openTestStore(t)
	defer func() { _ = store.Close() }()

	if err := store.Update(context.Background(), func(tx graph.Tx) error {
		vertices := []*graph.Vertex{
			{Tenant: "acme", ID: "host-1", Labels: []string{"Host"}, Properties: graph.PropertyMap{"ip": []byte("10.0.0.1")}},
			{Tenant: "acme", ID: "host-2", Labels: []string{"Host"}, Properties: graph.PropertyMap{"ip": []byte("10.0.0.2")}},
			{Tenant: "acme", ID: "flow-1", Labels: []string{"Flow"}, Properties: graph.PropertyMap{"threat_score": []byte("0.95"), "detected_malicious": []byte("true")}},
			{Tenant: "acme", ID: "flow-2", Labels: []string{"Flow"}, Properties: graph.PropertyMap{"threat_score": []byte("0.98"), "detected_malicious": []byte("true")}},
			{Tenant: "acme", ID: "flow-3", Labels: []string{"Flow"}, Properties: graph.PropertyMap{"threat_score": []byte("0.92"), "detected_malicious": []byte("true")}},
			{Tenant: "acme", ID: "flow-4", Labels: []string{"Flow"}, Properties: graph.PropertyMap{"threat_score": []byte("0.40"), "detected_malicious": []byte("false")}},
		}
		for _, vertex := range vertices {
			if err := tx.PutVertex(context.Background(), vertex); err != nil {
				return err
			}
		}

		edges := []*graph.Edge{
			{Tenant: "acme", ID: "sent-1", Type: "SENT", SrcID: "host-1", DstID: "flow-1"},
			{Tenant: "acme", ID: "sent-2", Type: "SENT", SrcID: "host-1", DstID: "flow-2"},
			{Tenant: "acme", ID: "sent-3", Type: "SENT", SrcID: "host-2", DstID: "flow-3"},
			{Tenant: "acme", ID: "sent-4", Type: "SENT", SrcID: "host-2", DstID: "flow-4"},
		}
		for _, edge := range edges {
			if err := tx.PutEdge(context.Background(), edge); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed host/flow graph failed: %v", err)
	}

	exec := executor.New(store, executor.Options{Metrics: executor.NewCollector()})
	grpcSrv, grpcLn, err := startGRPCServer("127.0.0.1:0",
		&grpcDdlHandler{
			defaultTenant: "acme",
		},
		&grpcDmlHandler{
			executor:      exec,
			defaultTenant: "acme",
		},
	)
	if err != nil {
		t.Fatalf("startGRPCServer failed: %v", err)
	}
	defer grpcSrv.GracefulStop()
	defer func() { _ = grpcLn.Close() }()

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := v1.NewDmlServiceClient(conn)

	thresholdResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: "MATCH (f:Flow) WHERE f.threat_score >= $threshold RETURN count(f) AS ct"}},
		Parameters: map[string]*v1.Value{
			"threshold": {Kind: &v1.Value_DoubleValue{DoubleValue: 0.93}},
		},
	})
	if err != nil {
		t.Fatalf("threshold Execute failed: %v", err)
	}
	if len(thresholdResp.GetRows()) != 1 {
		t.Fatalf("expected one threshold row, got %d", len(thresholdResp.GetRows()))
	}
	thresholdRow := thresholdResp.GetRows()[0].GetValues()
	if _, ok := thresholdRow["ct"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected ct as int_value kind, got %T", thresholdRow["ct"].GetKind())
	}
	if got := thresholdRow["ct"].GetIntValue(); got != 2 {
		t.Fatalf("expected threshold count=2, got %d", got)
	}

	limitQuery := "MATCH (src:Host)-[:SENT]->(f:Flow) WHERE f.detected_malicious = true RETURN src.ip AS source_ip, count(f) AS suspicious_flows, avg(f.threat_score) AS avg_score, max(f.threat_score) AS max_score ORDER BY suspicious_flows DESC, avg_score DESC LIMIT $limit_value"
	limitResp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: "acme",
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: limitQuery}},
		Parameters: map[string]*v1.Value{
			"limit_value": {Kind: &v1.Value_IntValue{IntValue: 1}},
		},
	})
	if err != nil {
		t.Fatalf("limit Execute failed: %v", err)
	}
	if len(limitResp.GetRows()) != 1 {
		t.Fatalf("expected one limited row, got %d", len(limitResp.GetRows()))
	}

	row := limitResp.GetRows()[0].GetValues()
	if got := row["source_ip"].GetStringValue(); got != "10.0.0.1" {
		t.Fatalf("expected source_ip=10.0.0.1, got %q", got)
	}
	if _, ok := row["suspicious_flows"].GetKind().(*v1.Value_IntValue); !ok {
		t.Fatalf("expected suspicious_flows as int_value kind, got %T", row["suspicious_flows"].GetKind())
	}
	if got := row["suspicious_flows"].GetIntValue(); got != 2 {
		t.Fatalf("expected suspicious_flows=2, got %d", got)
	}
	if _, ok := row["avg_score"].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected avg_score as double_value kind, got %T", row["avg_score"].GetKind())
	}
	if got := row["avg_score"].GetDoubleValue(); got != 0.965 {
		t.Fatalf("expected avg_score=0.965, got %v", got)
	}
	if _, ok := row["max_score"].GetKind().(*v1.Value_DoubleValue); !ok {
		t.Fatalf("expected max_score as double_value kind, got %T", row["max_score"].GetKind())
	}
	if got := row["max_score"].GetDoubleValue(); got != 0.98 {
		t.Fatalf("expected max_score=0.98, got %v", got)
	}
}
