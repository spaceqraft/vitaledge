package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	runtimestorage "github.com/paegun/vitaledge/internal/cypher/runtime/storage"
	"github.com/paegun/vitaledge/internal/graph"
)

type recordingTx struct {
	vertexes []*graph.Vertex
	edges    []*graph.Edge

	putVertexErr error
	putEdgeErr   error
}

type testStringer struct{ value string }

func (s testStringer) String() string { return s.value }

type pointerStringer struct{ value string }

func (s *pointerStringer) String() string { return s.value }

func (t *recordingTx) GetVertex(context.Context, string, string) (*graph.Vertex, error) {
	return nil, nil
}
func (t *recordingTx) HasVertexLabel(context.Context, string, string, string) (bool, error) {
	return false, nil
}
func (t *recordingTx) ScanVertices(context.Context, string, int, func(*graph.Vertex) error) error {
	return nil
}
func (t *recordingTx) ScanVerticesFrom(context.Context, string, string, int, func(*graph.Vertex) error) error {
	return nil
}
func (t *recordingTx) PutVertex(_ context.Context, vertex *graph.Vertex) error {
	if t.putVertexErr != nil {
		return t.putVertexErr
	}
	copyVertex := *vertex
	t.vertexes = append(t.vertexes, &copyVertex)
	return nil
}
func (t *recordingTx) DeleteVertex(context.Context, string, string) error { return nil }
func (t *recordingTx) GetStatsSnapshot(context.Context, string) (*graph.StatsSnapshot, error) {
	return nil, nil
}
func (t *recordingTx) GetEdge(context.Context, string, string) (*graph.Edge, error) { return nil, nil }
func (t *recordingTx) PutEdge(_ context.Context, edge *graph.Edge) error {
	if t.putEdgeErr != nil {
		return t.putEdgeErr
	}
	copyEdge := *edge
	t.edges = append(t.edges, &copyEdge)
	return nil
}
func (t *recordingTx) DeleteEdge(context.Context, string, string) error { return nil }
func (t *recordingTx) ScanOutEdges(context.Context, string, string, string, int, func(*graph.Edge) error) error {
	return nil
}
func (t *recordingTx) ScanOutEdgeLinks(context.Context, string, string, string, int, func(string, string) error) error {
	return nil
}
func (t *recordingTx) ScanOutEdgeLinksByType(context.Context, string, string, int, func(string, string, string) error) error {
	return nil
}
func (t *recordingTx) ScanOutEdgeProperty(context.Context, string, string, string, string, []byte, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) ScanOutEdgePropertyNumericRange(context.Context, string, string, string, string, float64, bool, bool, float64, bool, bool, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) HasDirectedEdgeBetween(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}
func (t *recordingTx) HasUndirectedEdgeBetween(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}
func (t *recordingTx) ScanOutEdgeSourceIDs(context.Context, string, string, int, func(string) error) error {
	return nil
}
func (t *recordingTx) ScanInEdges(context.Context, string, string, string, int, func(*graph.Edge) error) error {
	return nil
}
func (t *recordingTx) ScanPropertyIndex(context.Context, string, string, string, []byte, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) ScanPropertyIndexAll(context.Context, string, string, string, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) ScanPropertyIndexNumericRange(context.Context, string, string, string, float64, bool, bool, float64, bool, bool, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) ScanPropertyIndexBooleanRange(context.Context, string, string, string, bool, bool, bool, bool, bool, bool, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) ScanPropertyIndexDateTimeRange(context.Context, string, string, string, time.Time, bool, bool, time.Time, bool, bool, int, func(*graph.PropertyIndexEntry) error) error {
	return nil
}
func (t *recordingTx) PutPropertyIndex(context.Context, *graph.PropertyIndexEntry) error { return nil }
func (t *recordingTx) DeletePropertyIndex(context.Context, *graph.PropertyIndexEntry) error {
	return nil
}
func (t *recordingTx) Commit() error   { return nil }
func (t *recordingTx) Rollback() error { return nil }

func TestApplyWriteEventsAppliesEdgeMutation(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType:   operators.MutationTypeEdge,
		Bindings:       map[string]any{"u": "u1"},
		ResolvedParams: map[string]any{"peer": "u2"},
		Edge: &operators.EdgeMutation{
			Type:         "  KNOWS  ",
			LeftVar:      "u",
			RightIDParam: "peer",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 2 {
		t.Fatalf("expected endpoint vertex upserts, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge upsert, got %#v", tx.edges)
	}
	if tx.edges[0].SrcID != "u1" || tx.edges[0].DstID != "u2" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied edge: %#v", tx.edges[0])
	}
	if tx.edges[0].ID != "u1|KNOWS|u2" || tx.edges[0].Tenant != "acme" {
		t.Fatalf("expected deterministic edge id and tenant, got %#v", tx.edges[0])
	}
}

func TestApplyWriteEventsAppliesReverseEdgeMutation(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType:   operators.MutationTypeEdge,
		ResolvedParams: map[string]any{"src": "u1", "dst": "u2"},
		Edge: &operators.EdgeMutation{
			Type:         "  KNOWS  ",
			LeftIDParam:  "src",
			RightIDParam: "dst",
			LeftLabels:   []string{"LeftUser"},
			RightLabels:  []string{"RightUser"},
			Reverse:      true,
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 2 {
		t.Fatalf("expected endpoint vertex upserts, got %#v", tx.vertexes)
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge upsert, got %#v", tx.edges)
	}
	labelsByID := map[string][]string{}
	for _, v := range tx.vertexes {
		labelsByID[v.ID] = append([]string(nil), v.Labels...)
	}
	if got := labelsByID["u1"]; len(got) != 1 || got[0] != "LeftUser" {
		t.Fatalf("expected left endpoint label on u1, got %#v", labelsByID)
	}
	if got := labelsByID["u2"]; len(got) != 1 || got[0] != "RightUser" {
		t.Fatalf("expected right endpoint label on u2, got %#v", labelsByID)
	}
	if tx.edges[0].SrcID != "u2" || tx.edges[0].DstID != "u1" || tx.edges[0].Type != "KNOWS" {
		t.Fatalf("unexpected applied reverse edge: %#v", tx.edges[0])
	}
	if tx.edges[0].ID != "u2|KNOWS|u1" || tx.edges[0].Tenant != "acme" {
		t.Fatalf("expected deterministic reverse edge id and tenant, got %#v", tx.edges[0])
	}
}

func TestApplyWriteEventsAppliesVertexMutation(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType:   operators.MutationTypeVertex,
		ResolvedParams: map[string]any{"id": "u9"},
		Vertex: &operators.VertexMutation{
			IDParam: "id",
			Labels:  []string{"User", "Admin"},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one vertex upsert, got %#v", tx.vertexes)
	}
	if tx.vertexes[0].ID != "u9" {
		t.Fatalf("unexpected vertex id: %#v", tx.vertexes[0])
	}
	if len(tx.edges) != 0 {
		t.Fatalf("did not expect edge writes, got %#v", tx.edges)
	}
}

func TestApplyWriteEventsNoOpForNilTxAndEmptyEvents(t *testing.T) {
	err := ApplyWriteEvents(context.Background(), nil, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex:       &operators.VertexMutation{IDParam: "id"},
	}})
	if err != nil {
		t.Fatalf("expected nil error for nil tx, got %v", err)
	}

	tx := &recordingTx{}
	err = ApplyWriteEvents(context.Background(), tx, "acme", nil)
	if err != nil {
		t.Fatalf("expected nil error for empty events, got %v", err)
	}
	err = ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{})
	if err != nil {
		t.Fatalf("expected nil error for empty event slice, got %v", err)
	}
	if len(tx.vertexes) != 0 || len(tx.edges) != 0 {
		t.Fatalf("expected no writes for empty events, got vertexes=%#v edges=%#v", tx.vertexes, tx.edges)
	}
}

func TestApplyWriteEventsIgnoresUnknownMutationType(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: "UNKNOWN_MUTATION",
		Vertex:       &operators.VertexMutation{IDParam: "id"},
		ResolvedParams: map[string]any{
			"id": "u1",
		},
	}})
	if err != nil {
		t.Fatalf("expected unknown mutation type to be ignored, got error: %v", err)
	}
	if len(tx.vertexes) != 0 || len(tx.edges) != 0 {
		t.Fatalf("expected no writes for unknown mutation type, got vertexes=%#v edges=%#v", tx.vertexes, tx.edges)
	}
}

func TestApplyWriteEventsSkipsEdgeWhenEndpointUnresolved(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"left": "u1"},
	}})
	if err != nil {
		t.Fatalf("expected unresolved edge endpoint to be skipped, got error: %v", err)
	}
	if len(tx.vertexes) != 0 || len(tx.edges) != 0 {
		t.Fatalf("expected no writes when edge endpoint unresolved, got vertexes=%#v edges=%#v", tx.vertexes, tx.edges)
	}
}

func TestApplyWriteEventsTrimsTenantAtEntrypoint(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "  acme  ", []operators.WriteEvent{ {
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			IDParam: "id",
			Labels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"id": "u1"},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", tx.vertexes)
	}
	if tx.vertexes[0].Tenant != "acme" {
		t.Fatalf("expected trimmed tenant at entrypoint, got %#v", tx.vertexes[0])
	}
}

func TestApplyWriteEventsTrimsTenantAtEntrypointForEdgeAndEndpointVertexes(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "  acme  ", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 2 {
		t.Fatalf("expected endpoint vertex upserts, got %#v", tx.vertexes)
	}
	for _, v := range tx.vertexes {
		if v.Tenant != "acme" {
			t.Fatalf("expected trimmed tenant on endpoint vertex write, got %#v", v)
		}
	}
	if len(tx.edges) != 1 {
		t.Fatalf("expected one edge upsert, got %#v", tx.edges)
	}
	if tx.edges[0].Tenant != "acme" {
		t.Fatalf("expected trimmed tenant on edge write, got %#v", tx.edges[0])
	}
}

func TestApplyWriteEventsSkipsWhitespaceOnlyEdgeType(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "   ",
			LeftIDParam:  "left",
			RightIDParam: "right",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("expected whitespace-only edge type to be skipped, got error: %v", err)
	}
	if len(tx.vertexes) != 0 || len(tx.edges) != 0 {
		t.Fatalf("expected no writes when edge type is whitespace-only, got vertexes=%#v edges=%#v", tx.vertexes, tx.edges)
	}
}

func TestApplyWriteEventsPropagatesTxPutVertexError(t *testing.T) {
	expectedErr := errors.New("tx put vertex failed")
	tx := &recordingTx{putVertexErr: expectedErr}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			IDParam: "id",
		},
		ResolvedParams: map[string]any{"id": "u1"},
	}})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected propagated tx vertex error %v, got %v", expectedErr, err)
	}
}

func TestApplyWriteEventsPropagatesTxPutEdgeError(t *testing.T) {
	expectedErr := errors.New("tx put edge failed")
	tx := &recordingTx{putEdgeErr: expectedErr}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected propagated tx edge error %v, got %v", expectedErr, err)
	}
}

type recordingSink struct {
	vertexes []*graph.Vertex
	edges    []*graph.Edge

	vertexErr error
	edgeErr   error
}

var _ runtimestorage.WriteSink = (*recordingSink)(nil)

func (s *recordingSink) PutVertex(_ context.Context, vertex *graph.Vertex) error {
	if s.vertexErr != nil {
		return s.vertexErr
	}
	copyVertex := *vertex
	s.vertexes = append(s.vertexes, &copyVertex)
	return nil
}

func (s *recordingSink) PutEdge(_ context.Context, edge *graph.Edge) error {
	if s.edgeErr != nil {
		return s.edgeErr
	}
	copyEdge := *edge
	s.edges = append(s.edges, &copyEdge)
	return nil
}

func TestApplyWriteEventsToSinkTrimsTenantAndFallsBackToParams(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "  acme  ", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			IDParam: "vertexID",
			Labels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"vertexID": 42},
	}})
	if err != nil {
		t.Fatalf("apply write events to sink failed: %v", err)
	}
	if len(sink.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", sink.vertexes)
	}
	if sink.vertexes[0].Tenant != "acme" {
		t.Fatalf("expected trimmed tenant, got %q", sink.vertexes[0].Tenant)
	}
	if sink.vertexes[0].ID != "42" {
		t.Fatalf("expected scalar fallback id conversion, got %q", sink.vertexes[0].ID)
	}
}

func TestApplyWriteEventsToSinkTrimsTenantForEdgeAndEndpointVertexes(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "  acme  ", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("apply write events to sink failed: %v", err)
	}
	if len(sink.vertexes) != 2 {
		t.Fatalf("expected two endpoint vertex writes, got %#v", sink.vertexes)
	}
	for _, v := range sink.vertexes {
		if v.Tenant != "acme" {
			t.Fatalf("expected trimmed tenant on endpoint vertex write, got %#v", v)
		}
	}
	if len(sink.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", sink.edges)
	}
	if sink.edges[0].Tenant != "acme" {
		t.Fatalf("expected trimmed tenant on edge write, got %#v", sink.edges[0])
	}
}

func TestApplyWriteEventsToSinkSkipsInvalidEdgeEvent(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "",
			LeftIDParam:  "left",
			RightIDParam: "right",
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("expected invalid edge event to be skipped, got error: %v", err)
	}
	if len(sink.vertexes) != 0 {
		t.Fatalf("expected no vertex writes for skipped event, got %#v", sink.vertexes)
	}
	if len(sink.edges) != 0 {
		t.Fatalf("expected no edge writes for skipped event, got %#v", sink.edges)
	}
}

func TestApplyWriteEventsToSinkPropagatesVertexError(t *testing.T) {
	expectedErr := errors.New("vertex sink failed")
	sink := &recordingSink{vertexErr: expectedErr}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			IDParam: "id",
		},
		ResolvedParams: map[string]any{"id": "u1"},
	}})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected propagated vertex error %v, got %v", expectedErr, err)
	}
}

func TestApplyWriteEventsToSinkPropagatesEdgeError(t *testing.T) {
	expectedErr := errors.New("edge sink failed")
	sink := &recordingSink{edgeErr: expectedErr}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected propagated edge error %v, got %v", expectedErr, err)
	}
}

func TestApplyWriteEventsToSinkMixedBatchOrderAndSkipBehavior(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "tenant-a", []operators.WriteEvent{
		{
			MutationType: operators.MutationTypeVertex,
			Vertex: &operators.VertexMutation{
				Var:    "u",
				Labels: []string{"User"},
			},
			Bindings: map[string]any{"u": "u1"},
		},
		{
			MutationType: operators.MutationTypeEdge,
			Edge: &operators.EdgeMutation{
				Type:         "FOLLOWS",
				LeftIDParam:  "left",
				RightIDParam: "right",
			},
			ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
		},
		{
			MutationType: operators.MutationTypeEdge,
			Edge: &operators.EdgeMutation{
				Type:         "",
				LeftIDParam:  "left",
				RightIDParam: "right",
			},
			ResolvedParams: map[string]any{"left": "u2", "right": "u3"},
		},
		{
			MutationType: operators.MutationTypeVertex,
			Vertex: &operators.VertexMutation{
				IDParam: "tail",
				Labels:  []string{"Tail"},
			},
			ResolvedParams: map[string]any{"tail": "u4"},
		},
	})
	if err != nil {
		t.Fatalf("apply write events to sink failed: %v", err)
	}

	if len(sink.vertexes) != 2 {
		t.Fatalf("expected two vertex writes, got %#v", sink.vertexes)
	}
	if len(sink.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", sink.edges)
	}

	if sink.vertexes[0].ID != "u1" || sink.vertexes[0].Tenant != "tenant-a" {
		t.Fatalf("unexpected first vertex write: %#v", sink.vertexes[0])
	}
	if sink.edges[0].ID != "u1|FOLLOWS|u2" || sink.edges[0].SrcID != "u1" || sink.edges[0].DstID != "u2" {
		t.Fatalf("unexpected edge write: %#v", sink.edges[0])
	}
	if sink.vertexes[1].ID != "u4" {
		t.Fatalf("unexpected second vertex write: %#v", sink.vertexes[1])
	}
	if sink.vertexes[1].Tenant != "tenant-a" {
		t.Fatalf("expected tenant propagation on second vertex, got %#v", sink.vertexes[1])
	}
	if sink.edges[0].Tenant != "tenant-a" {
		t.Fatalf("expected tenant propagation on edge, got %#v", sink.edges[0])
	}
}

func TestApplyWriteEventsToSinkReverseEdgeTrimsType(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "  KNOWS  ",
			LeftIDParam:  "left",
			RightIDParam: "right",
			Reverse:      true,
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("apply write events to sink failed: %v", err)
	}
	if len(sink.vertexes) != 0 {
		t.Fatalf("did not expect endpoint vertex upserts without labels, got %#v", sink.vertexes)
	}
	if len(sink.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", sink.edges)
	}
	if sink.edges[0].Type != "KNOWS" {
		t.Fatalf("expected trimmed edge type, got %#v", sink.edges[0])
	}
	if sink.edges[0].ID != "u2|KNOWS|u1" || sink.edges[0].SrcID != "u2" || sink.edges[0].DstID != "u1" {
		t.Fatalf("unexpected reversed edge identity, got %#v", sink.edges[0])
	}
}

func TestApplyWriteEventsToSinkStopsAfterFirstError(t *testing.T) {
	sink := &recordingSink{edgeErr: errors.New("stop-on-edge")}
	err := applyWriteEventsToSink(context.Background(), sink, "tenant-a", []operators.WriteEvent{
		{
			MutationType: operators.MutationTypeVertex,
			Vertex: &operators.VertexMutation{
				IDParam: "v1",
			},
			ResolvedParams: map[string]any{"v1": "u1"},
		},
		{
			MutationType: operators.MutationTypeEdge,
			Edge: &operators.EdgeMutation{
				Type:         "FOLLOWS",
				LeftIDParam:  "left",
				RightIDParam: "right",
			},
			ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
		},
		{
			MutationType: operators.MutationTypeVertex,
			Vertex: &operators.VertexMutation{
				IDParam: "v2",
			},
			ResolvedParams: map[string]any{"v2": "u3"},
		},
	})
	if err == nil {
		t.Fatalf("expected sink error but got nil")
	}
	if len(sink.vertexes) != 1 {
		t.Fatalf("expected only pre-error vertex write, got %#v", sink.vertexes)
	}
	if sink.vertexes[0].ID != "u1" {
		t.Fatalf("unexpected pre-error vertex write: %#v", sink.vertexes[0])
	}
	if len(sink.edges) != 0 {
		t.Fatalf("expected no persisted edge writes when edge sink fails, got %#v", sink.edges)
	}
}

func TestResolveEntityIDPrefersBindingsOverParams(t *testing.T) {
	id := resolveEntityID(
		"userVar",
		"idParam",
		map[string]any{"userVar": "from-binding"},
		map[string]any{"idParam": "from-param"},
	)
	if id != "from-binding" {
		t.Fatalf("expected binding value to win, got %q", id)
	}
}

func TestResolveEntityIDFallsBackToParamsAndEmptyWhenMissing(t *testing.T) {
	id := resolveEntityID(
		"missingVar",
		"idParam",
		map[string]any{"other": "x"},
		map[string]any{"idParam": testStringer{value: " from-stringer "}},
	)
	if id != "from-stringer" {
		t.Fatalf("expected param fallback with trimmed stringer value, got %q", id)
	}

	missing := resolveEntityID("missingVar", "missingParam", map[string]any{}, map[string]any{})
	if missing != "" {
		t.Fatalf("expected empty id when unresolved, got %q", missing)
	}
}

func TestResolveEntityIDTrimsInputNamesAndFallsBackWhenBindingEmpty(t *testing.T) {
	id := resolveEntityID(
		"  userVar  ",
		"  idParam  ",
		map[string]any{"userVar": "   "},
		map[string]any{"idParam": "p-42"},
	)
	if id != "p-42" {
		t.Fatalf("expected fallback to param when binding resolves empty, got %q", id)
	}
}

func TestResolveEntityIDFallsBackWhenBindingIsNilPointerStringer(t *testing.T) {
	var nilPtrStringer *pointerStringer
	id := resolveEntityID(
		"userVar",
		"idParam",
		map[string]any{"userVar": nilPtrStringer},
		map[string]any{"idParam": "p-99"},
	)
	if id != "p-99" {
		t.Fatalf("expected param fallback when binding is nil pointer stringer, got %q", id)
	}
}

func TestScalarStringConversions(t *testing.T) {
	if got := scalarString("  abc  "); got != "abc" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
	if got := scalarString(testStringer{value: "  s-val "}); got != "s-val" {
		t.Fatalf("expected trimmed stringer, got %q", got)
	}
	if got := scalarString(&pointerStringer{value: "  p-val "}); got != "p-val" {
		t.Fatalf("expected trimmed non-nil pointer stringer, got %q", got)
	}
	if got := scalarString(123); got != "123" {
		t.Fatalf("expected numeric fallback conversion, got %q", got)
	}
	if got := scalarString(nil); got != "" {
		t.Fatalf("expected empty for nil conversion, got %q", got)
	}
	var nilPtrStringer *pointerStringer
	if got := scalarString(nilPtrStringer); got != "" {
		t.Fatalf("expected empty for nil pointer stringer, got %q", got)
	}
}

func TestApplyWriteEventsToSinkNoOpForNilSink(t *testing.T) {
	err := applyWriteEventsToSink(context.Background(), nil, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex:       &operators.VertexMutation{IDParam: "id"},
		ResolvedParams: map[string]any{
			"id": "u1",
		},
	}})
	if err != nil {
		t.Fatalf("expected nil error for nil sink, got %v", err)
	}
}

func TestApplyWriteEventsToSinkIgnoresUnknownMutationType(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: "UNKNOWN_MUTATION",
		Vertex:       &operators.VertexMutation{IDParam: "id"},
		ResolvedParams: map[string]any{
			"id": "u1",
		},
	}})
	if err != nil {
		t.Fatalf("expected unknown mutation type to be ignored, got error: %v", err)
	}
	if len(sink.vertexes) != 0 || len(sink.edges) != 0 {
		t.Fatalf("expected no writes for unknown mutation type, got vertexes=%#v edges=%#v", sink.vertexes, sink.edges)
	}
}

func TestApplyWriteEventsToSinkSkipsVertexWhenIDUnresolved(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			Var:    "u",
			IDParam: "id",
			Labels: []string{"User"},
		},
		Bindings:       map[string]any{"other": "x"},
		ResolvedParams: map[string]any{"other": "y"},
	}})
	if err != nil {
		t.Fatalf("expected unresolved vertex id to be skipped, got error: %v", err)
	}
	if len(sink.vertexes) != 0 || len(sink.edges) != 0 {
		t.Fatalf("expected no writes when vertex id is unresolved, got vertexes=%#v edges=%#v", sink.vertexes, sink.edges)
	}
}

func TestApplyWriteEventsToSinkSkipsEdgeWhenEndpointMissing(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeEdge,
		Edge: &operators.EdgeMutation{
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
			LeftLabels:   []string{"User"},
			RightLabels:  []string{"User"},
		},
		ResolvedParams: map[string]any{"left": "u1"},
	}})
	if err != nil {
		t.Fatalf("expected missing edge endpoint to be skipped, got error: %v", err)
	}
	if len(sink.vertexes) != 0 || len(sink.edges) != 0 {
		t.Fatalf("expected no writes when edge endpoint is missing, got vertexes=%#v edges=%#v", sink.vertexes, sink.edges)
	}
}
