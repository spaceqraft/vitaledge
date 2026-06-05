package operators

import "testing"

func TestWriteHandlerRecordsEventAndMarksRows(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Rows:   []map[string]any{{"u": "u1"}},
		Params: map[string]any{"peer": "u2", "id": "u1"},
	}

	err := h.Execute("p2", map[string]any{
		"kind":         "MERGE",
		"raw":          "MERGE (u)-[:KNOWS]->(:User {id:$peer})",
		"mergePattern": "(u)-[:KNOWS]->(:User {id:$peer})",
		"mergeOnCreate": []string{
			"SET r.createdAt = timestamp()",
		},
		"mergeOnMatch": []any{
			"SET r.touchedAt = timestamp()",
		},
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if len(state.ExecutedOps) != 1 || state.ExecutedOps[0] != "PHY_WRITE" {
		t.Fatalf("unexpected executed ops: %#v", state.ExecutedOps)
	}
	if state.OperatorExecCount != 1 {
		t.Fatalf("unexpected operator count: %d", state.OperatorExecCount)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}

	event := state.WriteEvents[0]
	if event.NodeID != "p2" {
		t.Fatalf("unexpected event node id: %#v", event)
	}
	if event.Kind != "MERGE" {
		t.Fatalf("unexpected event kind: %#v", event)
	}
	if event.MergePattern == "" {
		t.Fatalf("expected merge pattern in write event: %#v", event)
	}
	if event.MutationType != MutationTypeEdge {
		t.Fatalf("expected edge mutation classification, got %#v", event)
	}
	if event.Edge == nil || event.Edge.Type != "KNOWS" {
		t.Fatalf("expected edge mutation payload, got %#v", event)
	}
	if event.Edge.LeftVar != "u" || event.Edge.RightIDParam != "peer" {
		t.Fatalf("expected bridge-ready endpoint hints, got %#v", event.Edge)
	}
	if len(event.Edge.RightLabels) != 1 || event.Edge.RightLabels[0] != "User" {
		t.Fatalf("expected right endpoint label User, got %#v", event.Edge)
	}
	if event.Vertex != nil {
		t.Fatalf("did not expect vertex payload on edge mutation, got %#v", event)
	}
	if got := event.Bindings["u"]; got != "u1" {
		t.Fatalf("expected captured row binding for u, got %#v", event.Bindings)
	}
	if got := event.ResolvedParams["peer"]; got != "u2" {
		t.Fatalf("expected resolved peer param in payload, got %#v", event.ResolvedParams)
	}
	if _, ok := event.ResolvedParams["id"]; ok {
		t.Fatalf("did not expect unrelated id param in payload, got %#v", event.ResolvedParams)
	}
	if len(event.ParamKeys) != 1 || event.ParamKeys[0] != "peer" {
		t.Fatalf("expected extracted param keys [peer], got %#v", event.ParamKeys)
	}
	if got, ok := state.Rows[0]["__write_event_count"].(int); !ok || got != 1 {
		t.Fatalf("expected row write marker=1, got row=%#v", state.Rows[0])
	}
}

func TestWriteHandlerSeedsRowsWhenEmpty(t *testing.T) {
	h := NewWriteHandler()
	state := &State{}

	err := h.Execute("p3", map[string]any{"kind": "CREATE"}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	if len(state.Rows) != 1 {
		t.Fatalf("expected seeded row for write path, got %#v", state.Rows)
	}
	if got, ok := state.Rows[0]["__write_event_count"].(int); !ok || got != 1 {
		t.Fatalf("expected seeded row write marker=1, got %#v", state.Rows[0])
	}
	event := state.WriteEvents[0]
	if event.MutationType != MutationTypeUnknown {
		t.Fatalf("expected unknown mutation type for bare CREATE kind, got %#v", event)
	}
}

func TestWriteHandlerBuildsVertexMutationPayload(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"id": "u1"}}

	err := h.Execute("p4", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (:User:Admin {id:$id})",
		"mergePattern": "(:User:Admin {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}

	event := state.WriteEvents[0]
	if event.MutationType != MutationTypeVertex {
		t.Fatalf("expected vertex mutation type, got %#v", event)
	}
	if event.Vertex == nil {
		t.Fatalf("expected vertex mutation payload, got %#v", event)
	}
	if event.Vertex.IDParam != "id" {
		t.Fatalf("expected vertex id param hint, got %#v", event.Vertex)
	}
	if len(event.Vertex.Labels) != 2 || event.Vertex.Labels[0] != "User" || event.Vertex.Labels[1] != "Admin" {
		t.Fatalf("unexpected vertex labels: %#v", event.Vertex)
	}
	if event.Edge != nil {
		t.Fatalf("did not expect edge payload on vertex mutation, got %#v", event)
	}
	if got := event.ResolvedParams["id"]; got != "u1" {
		t.Fatalf("expected resolved id param, got %#v", event.ResolvedParams)
	}
}

func TestWriteHandlerMaterializesVertexVariableBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"id": "u7"}}

	err := h.Execute("p5", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$id})",
		"mergePattern": "(u:User {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u7" {
		t.Fatalf("expected variable binding u=u7, got %#v", row)
	}
	if got := row["u.id"]; got != "u7" {
		t.Fatalf("expected dotted id binding u.id=u7, got %#v", row)
	}
}

func TestWriteHandlerMaterializesEdgeEndpointBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u8", "dst": "u9"}}

	err := h.Execute("p6", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})-[:KNOWS]->(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})-[:KNOWS]->(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u8" {
		t.Fatalf("expected left endpoint variable binding u=u8, got %#v", row)
	}
	if got := row["u.id"]; got != "u8" {
		t.Fatalf("expected left dotted id binding u.id=u8, got %#v", row)
	}
	if got := row["v"]; got != "u9" {
		t.Fatalf("expected right endpoint variable binding v=u9, got %#v", row)
	}
	if got := row["v.id"]; got != "u9" {
		t.Fatalf("expected right dotted id binding v.id=u9, got %#v", row)
	}
}

func TestWriteHandlerMaterializesReverseEdgeEndpointBindings(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u14", "dst": "u15"}}

	err := h.Execute("p6r", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one materialized row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["u"]; got != "u14" {
		t.Fatalf("expected left endpoint variable binding u=u14, got %#v", row)
	}
	if got := row["u.id"]; got != "u14" {
		t.Fatalf("expected left dotted id binding u.id=u14, got %#v", row)
	}
	if got := row["v"]; got != "u15" {
		t.Fatalf("expected right endpoint variable binding v=u15, got %#v", row)
	}
	if got := row["v.id"]; got != "u15" {
		t.Fatalf("expected right dotted id binding v.id=u15, got %#v", row)
	}
}

func TestWriteHandlerSkipsMaterializationWhenDisabled(t *testing.T) {
	h := NewWriteHandler()
	state := &State{
		Params:                   map[string]any{"id": "u21"},
		MaterializeWriteBindings: false,
	}

	err := h.Execute("p9", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$id})",
		"mergePattern": "(u:User {id:$id})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.Rows) != 0 {
		t.Fatalf("expected no row materialization when disabled, got %#v", state.Rows)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}
}

func TestWriteHandlerBuildsReverseEdgeMutationPayload(t *testing.T) {
	h := NewWriteHandler()
	state := &State{Params: map[string]any{"src": "u10", "dst": "u11"}}

	err := h.Execute("p8", map[string]any{
		"kind":         "CREATE",
		"raw":          "CREATE (u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
		"mergePattern": "(u:User {id:$src})<-[:KNOWS]-(v:User {id:$dst})",
	}, state)
	if err != nil {
		t.Fatalf("write execute failed: %v", err)
	}
	if len(state.WriteEvents) != 1 {
		t.Fatalf("expected one write event, got %#v", state.WriteEvents)
	}
	event := state.WriteEvents[0]
	if event.Edge == nil {
		t.Fatalf("expected edge mutation payload, got %#v", event)
	}
	if !event.Edge.Reverse {
		t.Fatalf("expected reverse edge mutation, got %#v", event.Edge)
	}
	if event.Edge.LeftVar != "u" || event.Edge.RightVar != "v" {
		t.Fatalf("unexpected endpoint variables in edge payload: %#v", event.Edge)
	}
}

func TestProjectHandlerDistinct(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{
		{"uid": "u1", "vid": "u2"},
		{"uid": "u1", "vid": "u2"},
		{"uid": "u1", "vid": "u3"},
	}}

	err := h.Execute("p7", map[string]any{
		"items":    []string{"uid", "vid"},
		"distinct": true,
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 2 {
		t.Fatalf("expected deduplicated rows, got %#v", state.Rows)
	}
	if state.Rows[0]["uid"] != "u1" || state.Rows[0]["vid"] != "u2" {
		t.Fatalf("unexpected first distinct row: %#v", state.Rows[0])
	}
	if state.Rows[1]["uid"] != "u1" || state.Rows[1]["vid"] != "u3" {
		t.Fatalf("unexpected second distinct row: %#v", state.Rows[1])
	}
}

func TestProjectHandlerResolvesParamAndNullExpressions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{
		Rows:   []map[string]any{{"uid": "u1"}},
		Params: map[string]any{"src": "u1"},
	}

	err := h.Execute("p9", map[string]any{
		"items": []string{"null AS n", "$src AS s", "uid AS u"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["n"]; got != nil {
		t.Fatalf("expected null projection for n, got %#v", row)
	}
	if got := row["s"]; got != "u1" {
		t.Fatalf("expected parameter projection s=u1, got %#v", row)
	}
	if got := row["u"]; got != "u1" {
		t.Fatalf("expected row projection u=u1, got %#v", row)
	}
}

func TestProjectHandlerResolvesQuotedAndNumericLiteralExpressions(t *testing.T) {
	h := NewProjectHandler()
	state := &State{Rows: []map[string]any{{}}}

	err := h.Execute("p10", map[string]any{
		"items": []string{"'a''b' AS s", "42 AS i", "3.5 AS f"},
	}, state)
	if err != nil {
		t.Fatalf("project execute failed: %v", err)
	}
	if len(state.Rows) != 1 {
		t.Fatalf("expected one projected row, got %#v", state.Rows)
	}
	row := state.Rows[0]
	if got := row["s"]; got != "a'b" {
		t.Fatalf("expected escaped single-quote literal for s, got %#v", row)
	}
	if got := row["i"]; got != int64(42) {
		t.Fatalf("expected integer literal i=42, got %#v", row)
	}
	if got := row["f"]; got != float64(3.5) {
		t.Fatalf("expected float literal f=3.5, got %#v", row)
	}
}

func TestSortHandlerPreservesStableOrderForConstantExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "numeric literal", expression: "1"},
		{name: "quoted literal", expression: "'x'"},
		{name: "keyword null", expression: "null"},
		{name: "parameter", expression: "$sort"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewSortHandler()
			state := &State{Rows: []map[string]any{
				{"id": "u1", "rank": 2},
				{"id": "u2", "rank": 1},
				{"id": "u3", "rank": 3},
			}}

			err := h.Execute("s1", map[string]any{
				"ordering": []map[string]any{{"expression": tc.expression, "direction": "DESC"}},
			}, state)
			if err != nil {
				t.Fatalf("sort execute failed: %v", err)
			}
			if len(state.Rows) != 3 {
				t.Fatalf("expected 3 rows after sort, got %#v", state.Rows)
			}
			if got := state.Rows[0]["id"]; got != "u1" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
			if got := state.Rows[1]["id"]; got != "u2" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
			if got := state.Rows[2]["id"]; got != "u3" {
				t.Fatalf("expected stable row order for constant expression, got %#v", state.Rows)
			}
		})
	}
}

func TestSortHandlerSortsByBoundIdentifier(t *testing.T) {
	h := NewSortHandler()
	state := &State{Rows: []map[string]any{
		{"id": "u1", "rank": 2},
		{"id": "u2", "rank": 1},
		{"id": "u3", "rank": 3},
	}}

	err := h.Execute("s2", map[string]any{
		"ordering": []map[string]any{{"expression": "rank", "direction": "DESC"}},
	}, state)
	if err != nil {
		t.Fatalf("sort execute failed: %v", err)
	}
	if len(state.Rows) != 3 {
		t.Fatalf("expected 3 rows after sort, got %#v", state.Rows)
	}
	if got := state.Rows[0]["id"]; got != "u3" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
	if got := state.Rows[1]["id"]; got != "u1" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
	if got := state.Rows[2]["id"]; got != "u2" {
		t.Fatalf("expected DESC rank ordering, got %#v", state.Rows)
	}
}
