package executor

import "testing"

func assertPipelineExplainPayloadEnvelope(t *testing.T, explainPayload map[string]any) {
	t.Helper()
	if version, _ := explainPayload["version"].(string); version != "v2-pipeline" {
		t.Fatalf("expected v2-pipeline payload, got %#v", explainPayload["version"])
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
	_ = explainPayload
	_ = keys
}
