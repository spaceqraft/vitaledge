package executor

import "testing"

func assertPipelineExplainPayloadEnvelope(t *testing.T, explainPayload map[string]any) {
	t.Helper()
	if version, _ := explainPayload["version"].(string); version != "v2-pipeline" {
		t.Fatalf("expected v2-pipeline payload, got %#v", explainPayload["version"])
	}
	metadata, ok := explainPayload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata map, got %T", explainPayload["metadata"])
	}
	if route, _ := metadata["explainRoute"].(string); route != "pipeline_payload" {
		t.Fatalf("expected pipeline route, got %#v", metadata["explainRoute"])
	}
	if reason, _ := metadata["explainRouteReason"].(string); reason != "pipeline_payload_opt_in" {
		t.Fatalf("expected pipeline route reason, got %#v", metadata["explainRouteReason"])
	}
}

func requirePipelineLogicalPlanNodes(t *testing.T, explainPayload map[string]any) []map[string]any {
	t.Helper()
	logicalPlan, ok := explainPayload["logicalPlan"].(map[string]any)
	if !ok {
		t.Fatalf("expected logicalPlan map, got %T", explainPayload["logicalPlan"])
	}
	nodes, ok := logicalPlan["nodes"].([]map[string]any)
	if !ok || len(nodes) == 0 {
		t.Fatalf("expected non-empty logicalPlan.nodes, got %#v", logicalPlan["nodes"])
	}
	return nodes
}

func assertExplainPayloadOmitsKeys(t *testing.T, explainPayload map[string]any, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, exists := explainPayload[key]; exists {
			t.Fatalf("expected pipeline payload to omit %s", key)
		}
	}
}
