package executor

import (
	"context"
	"testing"
)

func TestExecuteRuntimeInQueryCallPreservesInputBindingsForNoYieldProcedure(t *testing.T) {
	e := &Executor{}
	inputRows := []map[string]any{
		{"n": "v1", "n.id": "v1"},
		{"n": "v2", "n.id": "v2"},
	}

	runtimeParams := map[string]any{
		ProcedureDeclsParam: map[string]ProcedureDecl{
			"test.doNothing": {
				Name:    "test.doNothing",
				Inputs:  nil,
				Outputs: nil,
				Rows:    nil,
			},
		},
	}

	outRows, _, err := e.executeRuntimeInQueryCall(context.Background(), inputRows, "CALL test.doNothing()", runtimeParams, true)
	if err != nil {
		t.Fatalf("executeRuntimeInQueryCall failed: %v", err)
	}
	if len(outRows) != len(inputRows) {
		t.Fatalf("expected %d rows, got %d: %#v", len(inputRows), len(outRows), outRows)
	}
	for i := range inputRows {
		if got := outRows[i]["n"]; got != inputRows[i]["n"] {
			t.Fatalf("row %d: expected n=%v, got %v", i, inputRows[i]["n"], got)
		}
		if got := outRows[i]["n.id"]; got != inputRows[i]["n.id"] {
			t.Fatalf("row %d: expected n.id=%v, got %v", i, inputRows[i]["n.id"], got)
		}
	}
}
