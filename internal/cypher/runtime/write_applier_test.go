package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
func (t *recordingTx) PutVertexBatch(ctx context.Context, vertexes []*graph.Vertex) error {
	for _, vertex := range vertexes {
		if vertex == nil {
			continue
		}
		if err := t.PutVertex(ctx, vertex); err != nil {
			return err
		}
	}
	return nil
}
func (t *recordingTx) PutEdgeBatch(ctx context.Context, edges []*graph.Edge) error {
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if err := t.PutEdge(ctx, edge); err != nil {
			return err
		}
	}
	return nil
}
func (t *recordingTx) DeleteVertexDetach(ctx context.Context, tenant, vertexID string) error {
	return t.DeleteVertex(ctx, tenant, vertexID)
}
func (t *recordingTx) PatchVertexProperties(context.Context, string, string, graph.PropertyMap, []string) error {
	return nil
}
func (t *recordingTx) PatchEdgeProperties(context.Context, string, string, graph.PropertyMap, []string) error {
	return nil
}
func (t *recordingTx) EnsureEdge(ctx context.Context, edge *graph.Edge) (bool, error) {
	if err := t.PutEdge(ctx, edge); err != nil {
		return false, err
	}
	return true, nil
}
func (t *recordingTx) ScanOutEdges(context.Context, string, string, string, int, func(*graph.Edge) error) error {
	return nil
}
func (t *recordingTx) ScanOutEdgeLinks(context.Context, string, string, string, int, func(string, string) error) error {
	return nil
}
func (t *recordingTx) ScanAdjacencyLinks(context.Context, string, string, graph.EdgeDirection, string, int, func(string, string) error) error {
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
func (t *recordingTx) BatchHasDirectedEdgeBetween(context.Context, string, []graph.DirectedEdgeProbe) ([]bool, error) {
	return nil, nil
}
func (t *recordingTx) BatchHasUndirectedEdgeBetween(context.Context, string, []graph.UndirectedEdgeProbe) ([]bool, error) {
	return nil, nil
}
func (t *recordingTx) DirectedEdgePairCount(context.Context, string, string, string, string) (int, error) {
	return 0, nil
}
func (t *recordingTx) UndirectedEdgePairCount(context.Context, string, string, string, string) (int, error) {
	return 0, nil
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
	if tx.edges[0].ID != "acme|u1|KNOWS|u2" || tx.edges[0].Tenant != "acme" {
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
	if tx.edges[0].ID != "acme|u2|KNOWS|u1" || tx.edges[0].Tenant != "acme" {
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

func TestApplyWriteEventsCreatePatternWithInlineComments(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		Kind: "CREATE",
		Raw: `
CREATE (:A {num: 1, num2: 4}), // num + num2 = 5
       (:A {num: 5, num2: 2}), // num + num2 = 7
       (:A {num: 9, num2: 0})  // num + num2 = 9
`,
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 3 {
		t.Fatalf("expected three vertex upserts from comma-separated CREATE, got %#v", tx.vertexes)
	}
	for _, v := range tx.vertexes {
		if v == nil {
			t.Fatalf("expected non-nil vertex in writes")
		}
		if len(v.Labels) != 1 || v.Labels[0] != "A" {
			t.Fatalf("expected label A on created vertices, got %#v", v.Labels)
		}
	}
}

func TestParseCreatePatternPropertyMapHandlesEmbeddedApostropheMovieFields(t *testing.T) {
	props := parseCreatePatternPropertyMap(
		context.Background(),
		nil,
		"acme",
		"(m:Movie {title: 'The Devil's Advocate', released: 1997, tagline: 'Evil has its winning ways'})",
		nil,
		nil,
	)
	if props == nil {
		t.Fatalf("expected parsed properties")
	}
	if got := string(props["title"]); got != "The Devil's Advocate" {
		t.Fatalf("expected title to parse cleanly, got %q", got)
	}
	if got := string(props["released"]); got != "1997" {
		t.Fatalf("expected released property, got %q", got)
	}
	if got := string(props["tagline"]); got != "Evil has its winning ways" {
		t.Fatalf("expected tagline property, got %q", got)
	}
}

func TestParseCreatePatternPropertyMapHandlesEmbeddedApostrophePersonBorn(t *testing.T) {
	props := parseCreatePatternPropertyMap(
		context.Background(),
		nil,
		"acme",
		"(p:Person {name: 'Jerry O\\'Connell', born: 1974})",
		nil,
		nil,
	)
	if props == nil {
		t.Fatalf("expected parsed properties")
	}
	if got := string(props["name"]); got != "Jerry O'Connell" {
		t.Fatalf("expected name to parse cleanly, got %q", got)
	}
	if got := string(props["born"]); got != "1974" {
		t.Fatalf("expected born property, got %q", got)
	}
}

func TestApplyWriteEventsNoOpForNilTxAndEmptyEvents(t *testing.T) {
	err := ApplyWriteEvents(context.Background(), nil, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Vertex:       &operators.VertexMutation{IDParam: "id"},
	}})
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected invalid input error for nil tx, got %v", err)
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
	if len(tx.vertexes) != 2 || len(tx.edges) != 1 {
		t.Fatalf("expected endpoint materialization write shape, got vertexes=%#v edges=%#v", tx.vertexes, tx.edges)
	}
	if tx.edges[0].Type != "KNOWS" || tx.edges[0].SrcID != "u1" || tx.edges[0].DstID != "" {
		t.Fatalf("unexpected unresolved-endpoint edge payload: %#v", tx.edges[0])
	}
}

func TestApplyWriteEventsTrimsTenantAtEntrypoint(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "  acme  ", []operators.WriteEvent{{
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

func TestApplyWriteEventsPersistsTemporalCreateProperties(t *testing.T) {
	tx := &recordingTx{}
	err := ApplyWriteEvents(context.Background(), tx, "acme", []operators.WriteEvent{{
		MutationType: operators.MutationTypeVertex,
		Kind:         "CREATE",
		Vertex: &operators.VertexMutation{
			Pattern: "(:Event {created: localtime({hour: 12}), span: duration({minutes: 2, seconds: 30})})",
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(tx.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", tx.vertexes)
	}
	vertex := tx.vertexes[0]
	if got := string(vertex.Properties["created"]); got != "12:00" {
		t.Fatalf("expected rendered localtime property, got %q", got)
	}
	if got := string(vertex.Properties["span"]); got != "PT2M30S" {
		t.Fatalf("expected rendered duration property, got %q", got)
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

	vertexByID map[string]*graph.Vertex
	edgeByID   map[string]*graph.Edge

	deletedVertexIDs       []string
	deletedEdgeIDs         []string
	deletedVertexDetachIDs []string

	vertexErr error
	edgeErr   error
}

var _ runtimestorage.WriteSink = (*recordingSink)(nil)
var _ writeLookupTx = (*recordingSink)(nil)

func (s *recordingSink) PutVertex(_ context.Context, vertex *graph.Vertex) error {
	if s.vertexErr != nil {
		return s.vertexErr
	}
	copyVertex := *vertex
	copyVertex.Properties = clonePropertyMap(vertex.Properties)
	s.vertexes = append(s.vertexes, &copyVertex)
	if s.vertexByID == nil {
		s.vertexByID = map[string]*graph.Vertex{}
	}
	s.vertexByID[copyVertex.ID] = &copyVertex
	return nil
}

func (s *recordingSink) PutEdge(_ context.Context, edge *graph.Edge) error {
	if s.edgeErr != nil {
		return s.edgeErr
	}
	copyEdge := *edge
	copyEdge.Properties = clonePropertyMap(edge.Properties)
	s.edges = append(s.edges, &copyEdge)
	if s.edgeByID == nil {
		s.edgeByID = map[string]*graph.Edge{}
	}
	s.edgeByID[copyEdge.ID] = &copyEdge
	return nil
}

func (s *recordingSink) PutVertexBatch(ctx context.Context, vertexes []*graph.Vertex) error {
	for _, vertex := range vertexes {
		if vertex == nil {
			continue
		}
		if err := s.PutVertex(ctx, vertex); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingSink) PutEdgeBatch(ctx context.Context, edges []*graph.Edge) error {
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if err := s.PutEdge(ctx, edge); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingSink) DeleteVertexDetach(_ context.Context, _ string, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if _, ok := s.vertexByID[id]; !ok {
		return graph.NewError(graph.ErrKindNotFound, "vertex not found", nil)
	}
	s.deletedVertexDetachIDs = append(s.deletedVertexDetachIDs, id)
	delete(s.vertexByID, id)
	return nil
}
func (s *recordingSink) DeleteVertex(_ context.Context, _ string, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if _, ok := s.vertexByID[id]; !ok {
		return graph.NewError(graph.ErrKindNotFound, "vertex not found", nil)
	}
	s.deletedVertexIDs = append(s.deletedVertexIDs, id)
	delete(s.vertexByID, id)
	return nil
}
func (s *recordingSink) DeleteEdge(_ context.Context, _ string, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if _, ok := s.edgeByID[id]; !ok {
		return graph.NewError(graph.ErrKindNotFound, "edge not found", nil)
	}
	s.deletedEdgeIDs = append(s.deletedEdgeIDs, id)
	delete(s.edgeByID, id)
	return nil
}
func (s *recordingSink) GetVertex(_ context.Context, _ string, id string) (*graph.Vertex, error) {
	if s.vertexByID == nil {
		return nil, nil
	}
	v := s.vertexByID[id]
	if v == nil {
		return nil, nil
	}
	copyVertex := *v
	copyVertex.Properties = clonePropertyMap(v.Properties)
	return &copyVertex, nil
}
func (s *recordingSink) GetEdge(_ context.Context, _ string, id string) (*graph.Edge, error) {
	if s.edgeByID == nil {
		return nil, nil
	}
	e := s.edgeByID[id]
	if e == nil {
		return nil, nil
	}
	copyEdge := *e
	copyEdge.Properties = clonePropertyMap(e.Properties)
	return &copyEdge, nil
}
func (s *recordingSink) ScanVertices(_ context.Context, _ string, limit int, fn func(*graph.Vertex) error) error {
	if fn == nil || len(s.vertexByID) == 0 {
		return nil
	}
	seen := 0
	for _, vertex := range s.vertexByID {
		if vertex == nil {
			continue
		}
		copyVertex := *vertex
		copyVertex.Properties = clonePropertyMap(vertex.Properties)
		if err := fn(&copyVertex); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	return nil
}
func (s *recordingSink) PatchVertexProperties(_ context.Context, _ string, id string, props graph.PropertyMap, remove []string) error {
	v, ok := s.vertexByID[id]
	if !ok || v == nil {
		return nil
	}
	if v.Properties == nil {
		v.Properties = graph.PropertyMap{}
	}
	for _, key := range remove {
		delete(v.Properties, key)
	}
	for key, value := range props {
		v.Properties[key] = append([]byte(nil), value...)
	}
	return nil
}
func (s *recordingSink) PatchEdgeProperties(_ context.Context, _ string, id string, props graph.PropertyMap, remove []string) error {
	e, ok := s.edgeByID[id]
	if !ok || e == nil {
		return nil
	}
	if e.Properties == nil {
		e.Properties = graph.PropertyMap{}
	}
	for _, key := range remove {
		delete(e.Properties, key)
	}
	for key, value := range props {
		e.Properties[key] = append([]byte(nil), value...)
	}
	return nil
}
func (s *recordingSink) EnsureEdge(ctx context.Context, edge *graph.Edge) (bool, error) {
	if err := s.PutEdge(ctx, edge); err != nil {
		return false, err
	}
	return true, nil
}
func (s *recordingSink) ScanAdjacencyLinks(_ context.Context, tenant string, vertexID string, direction graph.EdgeDirection, edgeType string, _ int, emit func(string, string) error) error {
	if s == nil || emit == nil {
		return nil
	}
	tenant = strings.TrimSpace(tenant)
	vertexID = strings.TrimSpace(vertexID)
	edgeType = strings.TrimSpace(edgeType)
	for _, edge := range s.edgeByID {
		if edge == nil {
			continue
		}
		if tenant != "" && strings.TrimSpace(edge.Tenant) != tenant {
			continue
		}
		if edgeType != "" && strings.TrimSpace(edge.Type) != edgeType {
			continue
		}
		srcID := strings.TrimSpace(edge.SrcID)
		dstID := strings.TrimSpace(edge.DstID)
		edgeID := strings.TrimSpace(edge.ID)
		switch direction {
		case graph.EdgeDirectionIn:
			if dstID == vertexID {
				if err := emit(edgeID, srcID); err != nil {
					return err
				}
			}
		case graph.EdgeDirectionOut:
			if srcID == vertexID {
				if err := emit(edgeID, dstID); err != nil {
					return err
				}
			}
		default:
			if srcID == vertexID {
				if err := emit(edgeID, dstID); err != nil {
					return err
				}
			}
			if dstID == vertexID {
				if err := emit(edgeID, srcID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (s *recordingSink) BatchHasDirectedEdgeBetween(context.Context, string, []graph.DirectedEdgeProbe) ([]bool, error) {
	return nil, nil
}
func (s *recordingSink) BatchHasUndirectedEdgeBetween(context.Context, string, []graph.UndirectedEdgeProbe) ([]bool, error) {
	return nil, nil
}
func (s *recordingSink) DirectedEdgePairCount(context.Context, string, string, string, string) (int, error) {
	return 0, nil
}
func (s *recordingSink) UndirectedEdgePairCount(context.Context, string, string, string, string) (int, error) {
	return 0, nil
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
	if sink.edges[0].ID != "tenant-a|u1|FOLLOWS|u2" || sink.edges[0].SrcID != "u1" || sink.edges[0].DstID != "u2" {
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

func TestApplyWriteEventsDeleteOperandsFromListAndMap(t *testing.T) {
	sink := &recordingSink{
		vertexByID: map[string]*graph.Vertex{
			"u1": {Tenant: "acme", ID: "u1"},
			"u2": {Tenant: "acme", ID: "u2"},
		},
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "DELETE",
		Raw:  "DELETE m.nodes",
		Bindings: map[string]any{
			"m": map[string]any{"nodes": []any{"u1", "u2"}},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if !reflect.DeepEqual(sink.deletedVertexIDs, []string{"u1", "u2"}) {
		t.Fatalf("expected list/map delete to delete both vertexes, got %#v", sink.deletedVertexIDs)
	}
}

func TestApplyWriteEventsDeletePathLikeOperandDeletesPathEntities(t *testing.T) {
	sink := &recordingSink{
		vertexByID: map[string]*graph.Vertex{
			"u1": {Tenant: "acme", ID: "u1"},
			"u2": {Tenant: "acme", ID: "u2"},
		},
		edgeByID: map[string]*graph.Edge{
			"e1": {Tenant: "acme", ID: "e1", Type: "T", SrcID: "u1", DstID: "u2"},
		},
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "DELETE",
		Raw:  "DELETE p",
		Bindings: map[string]any{
			"p": map[string]any{
				"nodes":         []any{"u1", "u2"},
				"relationships": []any{"e1"},
			},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if !reflect.DeepEqual(sink.deletedEdgeIDs, []string{"e1"}) {
		t.Fatalf("expected path-like delete to remove relationship id, got edges=%#v", sink.deletedEdgeIDs)
	}
	if !reflect.DeepEqual(sink.deletedVertexIDs, []string{"u1", "u2"}) {
		t.Fatalf("expected path-like delete to remove vertex ids, got vertexes=%#v", sink.deletedVertexIDs)
	}
}

func TestApplyWriteEventsDeleteOperandListIndexParam(t *testing.T) {
	sink := &recordingSink{
		vertexByID: map[string]*graph.Vertex{
			"u1": {Tenant: "acme", ID: "u1"},
			"u2": {Tenant: "acme", ID: "u2"},
		},
		edgeByID: map[string]*graph.Edge{
			"e1": {Tenant: "acme", ID: "e1", Type: "FRIEND", SrcID: "u0", DstID: "u1"},
			"e2": {Tenant: "acme", ID: "e2", Type: "FRIEND", SrcID: "u0", DstID: "u2"},
		},
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{
		{
			Kind: "DETACH DELETE",
			Raw:  "DETACH DELETE friends[$friendIndex]",
			Bindings: map[string]any{
				"friends": []any{"u1", "u2"},
			},
			ResolvedParams: map[string]any{"friendIndex": 1},
		},
		{
			Kind: "DETACH DELETE",
			Raw:  "DETACH DELETE friendships[$friendIndex]",
			Bindings: map[string]any{
				"friendships": []any{"e1", "e2"},
			},
			ResolvedParams: map[string]any{"friendIndex": 0},
		},
	})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if !reflect.DeepEqual(sink.deletedVertexDetachIDs, []string{"u2"}) {
		t.Fatalf("expected parameterized list index to detach-delete selected vertex, got %#v", sink.deletedVertexDetachIDs)
	}
	if !reflect.DeepEqual(sink.deletedEdgeIDs, []string{"e1"}) {
		t.Fatalf("expected parameterized list index to delete selected edge, got %#v", sink.deletedEdgeIDs)
	}
}

func TestResolveDeleteWriteBindingsListIndexWithVertexPointers(t *testing.T) {
	event := operators.WriteEvent{
		Kind: "DETACH DELETE",
		Raw:  "DETACH DELETE friends[$friendIndex]",
		Bindings: map[string]any{
			"friends": []any{
				&graph.Vertex{Tenant: "acme", ID: "auto-v-1"},
				&graph.Vertex{Tenant: "acme", ID: "auto-v-2"},
			},
		},
		ResolvedParams: map[string]any{"friendIndex": 1},
	}

	bindings := resolveDeleteWriteBindings(event, "friends[$friendIndex]")
	if len(bindings) != 1 {
		t.Fatalf("expected one resolved binding, got %#v", bindings)
	}
	v, ok := bindings[0].(*graph.Vertex)
	if !ok || v == nil {
		t.Fatalf("expected resolved binding to be *graph.Vertex, got %#v", bindings[0])
	}
	if v.ID != "auto-v-2" {
		t.Fatalf("expected selected vertex id auto-v-2, got %#v", v)
	}
}

func TestResolveDeleteWriteBindingsParenthesizedOperand(t *testing.T) {
	event := operators.WriteEvent{
		Kind: "DELETE",
		Raw:  "DELETE (n)",
		Bindings: map[string]any{
			"n": "u1",
		},
	}

	bindings := resolveDeleteWriteBindings(event, "(n)")
	if len(bindings) == 0 {
		t.Fatalf("expected resolved bindings, got %#v", bindings)
	}
	if got := fmt.Sprint(bindings[0]); got != "u1" {
		t.Fatalf("expected first binding id u1, got %#v", bindings[0])
	}
}

type testPathLikeValue struct {
	nodes         []any
	relationships []any
}

func TestApplyWriteEventsDeletePathLikeUnexportedRelationships(t *testing.T) {
	sink := &recordingSink{
		edgeByID: map[string]*graph.Edge{
			"e1": {Tenant: "acme", ID: "e1", Type: "T", SrcID: "u1", DstID: "u2"},
		},
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "DELETE",
		Raw:  "DELETE p",
		Bindings: map[string]any{
			"p": testPathLikeValue{nodes: []any{"u1", "u2"}, relationships: []any{"e1"}},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if !reflect.DeepEqual(sink.deletedEdgeIDs, []string{"e1"}) {
		t.Fatalf("expected path-like delete to remove relationship id, got %#v", sink.deletedEdgeIDs)
	}
	if len(sink.deletedVertexIDs) != 0 {
		t.Fatalf("expected non-detach path-like delete to avoid unresolved vertex-id deletion without lookup, got %#v", sink.deletedVertexIDs)
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
	if sink.edges[0].ID != "acme|u2|KNOWS|u1" || sink.edges[0].SrcID != "u2" || sink.edges[0].DstID != "u1" {
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
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected invalid input error for nil sink, got %v", err)
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
			Var:     "u",
			IDParam: "id",
			Labels:  []string{"User"},
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

func TestApplyWriteEventsToSinkAllocatesIDForAnonymousCreateVertex(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:         "CREATE",
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			Pattern: "(:User)",
			Labels:  []string{"User"},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(sink.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", sink.vertexes)
	}
	if sink.vertexes[0].Tenant != "acme" {
		t.Fatalf("expected tenant acme, got %#v", sink.vertexes[0])
	}
	if sink.vertexes[0].ID == "" || !strings.HasPrefix(sink.vertexes[0].ID, "auto-v-") {
		t.Fatalf("expected allocated anonymous vertex id, got %#v", sink.vertexes[0])
	}
	if !reflect.DeepEqual(sink.vertexes[0].Labels, []string{"User"}) {
		t.Fatalf("expected preserved labels, got %#v", sink.vertexes[0].Labels)
	}
}

func TestApplyWriteEventsToSinkPersistsVertexPropertiesFromPattern(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:         "CREATE",
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			Pattern: "(n:User {id: 42, name: 'Someone', active: true, score: 3.5, tags: [1, 2, 3], missing: null})",
			Var:     "n",
			Labels:  []string{"User"},
		},
		Bindings: map[string]any{"n": "u42"},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(sink.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", sink.vertexes)
	}
	props := sink.vertexes[0].Properties
	if string(props["id"]) != "42" {
		t.Fatalf("expected id property encoded as 42, got %q", string(props["id"]))
	}
	if string(props["name"]) != "Someone" {
		t.Fatalf("expected name property encoded as Someone, got %q", string(props["name"]))
	}
	if string(props["active"]) != "true" {
		t.Fatalf("expected active property encoded as true, got %q", string(props["active"]))
	}
	if string(props["score"]) != "3.5" {
		t.Fatalf("expected score property encoded as 3.5, got %q", string(props["score"]))
	}
	if string(props["tags"]) != "[1 2 3]" {
		t.Fatalf("expected tags property encoded as [1 2 3], got %q", string(props["tags"]))
	}
	if _, ok := props["missing"]; ok {
		t.Fatalf("expected null-valued property to be omitted, got %#v", props)
	}
}

func TestApplyWriteEventsToSinkPersistsLargeIntegerProperty(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:         "CREATE",
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			Pattern: "(:TheLabel {id: 4611686018427387905})",
			Labels:  []string{"TheLabel"},
		},
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(sink.vertexes) != 1 {
		t.Fatalf("expected one vertex write, got %#v", sink.vertexes)
	}
	if string(sink.vertexes[0].Properties["id"]) != "4611686018427387905" {
		t.Fatalf("expected large integer id to round-trip as text, got %q", string(sink.vertexes[0].Properties["id"]))
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
	if len(sink.vertexes) != 2 || len(sink.edges) != 1 {
		t.Fatalf("expected endpoint materialization write shape, got vertexes=%#v edges=%#v", sink.vertexes, sink.edges)
	}
	if sink.edges[0].Type != "KNOWS" || sink.edges[0].SrcID != "u1" || sink.edges[0].DstID != "" {
		t.Fatalf("unexpected unresolved-endpoint edge payload: %#v", sink.edges[0])
	}
}

func TestApplyWriteEventsToSinkSplitsCreateCommaPatterns(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "CREATE",
		Raw:  "CREATE (a:User {id:'u1'}), (b:Group {id:'g1'}), (a)-[:MEMBER_OF]->(b)",
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(sink.vertexes) != 2 {
		t.Fatalf("expected two vertex writes, got %#v", sink.vertexes)
	}
	if len(sink.edges) != 1 {
		t.Fatalf("expected one edge write, got %#v", sink.edges)
	}
	if sink.edges[0].Type != "MEMBER_OF" {
		t.Fatalf("unexpected edge payload: %#v", sink.edges[0])
	}
	if sink.edges[0].SrcID == "" || sink.edges[0].DstID == "" {
		t.Fatalf("expected non-empty create endpoints, got %#v", sink.edges[0])
	}
}

func TestApplyWriteEventsToSinkSplitsCreateChainAndPersistsEdgeProperties(t *testing.T) {
	sink := &recordingSink{}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "CREATE",
		Raw:  "CREATE (a:Person {id:'p1'})-[:KNOWS {since: 2020}]->(b:Person {id:'p2'})-[:KNOWS {since: 2021}]->(c:Person {id:'p3'})",
	}})
	if err != nil {
		t.Fatalf("apply write events failed: %v", err)
	}
	if len(sink.vertexes) != 3 {
		t.Fatalf("expected three vertex writes, got %#v", sink.vertexes)
	}
	if len(sink.edges) != 2 {
		t.Fatalf("expected two edge writes, got %#v", sink.edges)
	}
	if got := string(sink.edges[0].Properties["since"]); got != "2020" {
		t.Fatalf("expected first edge since=2020, got %q", got)
	}
	if got := string(sink.edges[1].Properties["since"]); got != "2021" {
		t.Fatalf("expected second edge since=2021, got %q", got)
	}
}

func TestApplyWriteEventsToSinkMergeOnCreateSetMapForms(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"User"},
		Properties: graph.PropertyMap{
			"name":  []byte("bar"),
			"extra": []byte("1"),
		},
	}); err != nil {
		t.Fatalf("seed source vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "MERGE",
		Edge: &operators.EdgeMutation{
			Var:          "r",
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
		},
		Bindings: map[string]any{"a": "u1"},
		ResolvedParams: map[string]any{
			"left":  "u1",
			"right": "u2",
		},
		MergeOnCreate: []string{"SET r = a", "SET r += {name2: 'baz'}"},
	}})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}
	if len(sink.edges) != 1 {
		t.Fatalf("expected one merged edge write, got %#v", sink.edges)
	}
	edgeID := "acme|u1|KNOWS|u2"
	merged := sink.edgeByID[edgeID]
	if merged == nil {
		t.Fatalf("expected persisted merge edge %q", edgeID)
	}
	if got := string(merged.Properties["name"]); got != "bar" {
		t.Fatalf("expected map replace to copy name=bar, got %q", got)
	}
	if got := string(merged.Properties["extra"]); got != "1" {
		t.Fatalf("expected map replace to copy extra=1, got %q", got)
	}
	if got := string(merged.Properties["name2"]); got != "baz" {
		t.Fatalf("expected map append to set name2=baz, got %q", got)
	}
}

func TestApplyWriteEventsToSinkSetMapReplaceLiteralRemovesMissingAndNullKeys(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"X"},
		Properties: graph.PropertyMap{
			"name":  []byte("A"),
			"name2": []byte("B"),
		},
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:     "SET",
		Raw:      "SET n = {name: 'B', name2: null, baz: 'C'}",
		Bindings: map[string]any{"n": "u1"},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	v := sink.vertexByID["u1"]
	if v == nil {
		t.Fatalf("expected vertex u1 to exist after SET")
	}
	if got := string(v.Properties["name"]); got != "B" {
		t.Fatalf("expected name=B, got %q", got)
	}
	if got := string(v.Properties["baz"]); got != "C" {
		t.Fatalf("expected baz=C, got %q", got)
	}
	if _, ok := v.Properties["name2"]; ok {
		t.Fatalf("expected name2 to be removed on SET map replace, got %#v", v.Properties)
	}
}

func TestApplyWriteEventsToSinkSetMapReplaceLiteralUsesPersistedEntityForRemoval(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"X"},
		Properties: graph.PropertyMap{
			"name":  []byte("A"),
			"name2": []byte("B"),
		},
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "SET",
		Raw:  "SET n = {name: 'B', baz: 'C'}",
		Bindings: map[string]any{
			// Simulate runtime row bindings that may carry only partial properties.
			"n": &graph.Vertex{ID: "u1", Properties: graph.PropertyMap{"name": []byte("A")}},
		},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	v := sink.vertexByID["u1"]
	if v == nil {
		t.Fatalf("expected vertex u1 to exist after SET")
	}
	if _, ok := v.Properties["name2"]; ok {
		t.Fatalf("expected name2 to be removed using persisted properties, got %#v", v.Properties)
	}
}

func TestApplyWriteEventsToSinkSetMapAppendLiteralNullRemovesProperty(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"X"},
		Properties: graph.PropertyMap{
			"name":  []byte("A"),
			"name2": []byte("B"),
		},
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:     "SET",
		Raw:      "SET n += {name: null, baz: 'C'}",
		Bindings: map[string]any{"n": "u1"},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	v := sink.vertexByID["u1"]
	if v == nil {
		t.Fatalf("expected vertex u1 to exist after SET")
	}
	if _, ok := v.Properties["name"]; ok {
		t.Fatalf("expected name to be removed on SET map append null, got %#v", v.Properties)
	}
	if got := string(v.Properties["name2"]); got != "B" {
		t.Fatalf("expected name2 to remain B, got %q", got)
	}
	if got := string(v.Properties["baz"]); got != "C" {
		t.Fatalf("expected baz=C, got %q", got)
	}
}

func TestResolveWritePropertyValueSupportsHexOctalAndUnicodeEscapes(t *testing.T) {
	if got, ok := resolveWritePropertyValue("0x1A", nil); !ok || got != int64(26) {
		t.Fatalf("expected 0x1A to parse as int64(26), got (%#v, %v)", got, ok)
	}
	if got, ok := resolveWritePropertyValue("-0o12", nil); !ok || got != int64(-10) {
		t.Fatalf("expected -0o12 to parse as int64(-10), got (%#v, %v)", got, ok)
	}
	if got, ok := resolveWritePropertyValue("'\\u01FF'", nil); !ok || got != "\u01FF" {
		t.Fatalf("expected unicode escape to decode to ǿ, got (%#v, %v)", got, ok)
	}
}

func TestApplyWriteEventsToSinkMergeOnMatchSkipsOnCreateActions(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutEdge(context.Background(), &graph.Edge{
		Tenant: "acme",
		ID:     "acme|u1|KNOWS|u2",
		Type:   "KNOWS",
		SrcID:  "u1",
		DstID:  "u2",
		Properties: graph.PropertyMap{
			"name": []byte("old"),
		},
	}); err != nil {
		t.Fatalf("seed edge failed: %v", err)
	}
	preWrites := len(sink.edges)

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "MERGE",
		Edge: &operators.EdgeMutation{
			Var:          "r",
			Type:         "KNOWS",
			LeftIDParam:  "left",
			RightIDParam: "right",
		},
		ResolvedParams: map[string]any{
			"left":  "u1",
			"right": "u2",
		},
		MergeOnCreate: []string{"SET r.name = 'created'"},
		MergeOnMatch:  []string{"SET r.name = 'matched'"},
	}})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}
	if len(sink.edges) != preWrites {
		t.Fatalf("expected no new edge write on MERGE match, got %#v", sink.edges)
	}
	matched := sink.edgeByID["acme|u1|KNOWS|u2"]
	if matched == nil {
		t.Fatalf("expected matched edge to remain present")
	}
	if got := string(matched.Properties["name"]); got != "matched" {
		t.Fatalf("expected ON MATCH property update, got %q", got)
	}
}

func TestApplyWriteEventsToSinkMergeOnCreateSetUsesBoundNodePropertyFromStore(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "person-1",
		Labels: []string{"Person"},
		Properties: graph.PropertyMap{
			"bornIn": []byte("New York"),
		},
	}); err != nil {
		t.Fatalf("seed person vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "MERGE",
		Vertex: &operators.VertexMutation{
			Var:     "city",
			Labels:  []string{"City"},
			Pattern: "(city:City)",
		},
		Bindings:      map[string]any{"person": "person-1"},
		MergeOnCreate: []string{"SET city.name = person.bornIn"},
	}})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}

	var createdCity *graph.Vertex
	for _, vertex := range sink.vertexes {
		if vertex != nil && vertexHasLabel(vertex, "City") {
			createdCity = vertex
			break
		}
	}
	if createdCity == nil {
		t.Fatalf("expected MERGE to create a City vertex")
	}
	if got := string(createdCity.Properties["name"]); got != "New York" {
		t.Fatalf("expected ON CREATE SET city.name from bound person.bornIn, got %q", got)
	}
}

func TestApplyWriteEventsToSinkMergeEdgeMatchesByRelationshipProperties(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutEdge(context.Background(), &graph.Edge{
		Tenant: "acme",
		ID:     "acme|u1|TYPE|u2",
		Type:   "TYPE",
		SrcID:  "u1",
		DstID:  "u2",
		Properties: graph.PropertyMap{
			"name": []byte("r1"),
		},
	}); err != nil {
		t.Fatalf("seed edge r1 failed: %v", err)
	}
	if err := sink.PutEdge(context.Background(), &graph.Edge{
		Tenant: "acme",
		ID:     "acme|u1|TYPE|u2|auto-e-1",
		Type:   "TYPE",
		SrcID:  "u1",
		DstID:  "u2",
		Properties: graph.PropertyMap{
			"name": []byte("r2"),
		},
	}); err != nil {
		t.Fatalf("seed edge r2 failed: %v", err)
	}
	preWrites := len(sink.edges)

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "MERGE",
		Edge: &operators.EdgeMutation{
			Var:          "r",
			Type:         "TYPE",
			LeftIDParam:  "left",
			RightIDParam: "right",
			Pattern:      "[r:TYPE {name: 'r2'}]",
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}
	if len(sink.edges) != preWrites {
		t.Fatalf("expected MERGE to match existing rel by properties, got writes %#v", sink.edges)
	}
}

func TestApplyWriteEventsToSinkMergeEdgeCreatesWhenRelationshipPropertiesDoNotMatch(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutEdge(context.Background(), &graph.Edge{
		Tenant: "acme",
		ID:     "acme|u1|TYPE|u2",
		Type:   "TYPE",
		SrcID:  "u1",
		DstID:  "u2",
		Properties: graph.PropertyMap{
			"name": []byte("r1"),
		},
	}); err != nil {
		t.Fatalf("seed edge r1 failed: %v", err)
	}
	preWrites := len(sink.edges)

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "MERGE",
		Edge: &operators.EdgeMutation{
			Var:          "r",
			Type:         "TYPE",
			LeftIDParam:  "left",
			RightIDParam: "right",
			Pattern:      "[r:TYPE {name: 'r2'}]",
		},
		ResolvedParams: map[string]any{"left": "u1", "right": "u2"},
	}})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}
	if len(sink.edges) != preWrites+1 {
		t.Fatalf("expected MERGE to create new rel when property filter misses, got writes %#v", sink.edges)
	}

	created := sink.edgeByID["acme|u1|TYPE|u2|auto-e-1"]
	if created == nil {
		t.Fatalf("expected new auto edge id for property-miss MERGE")
	}
	if got := string(created.Properties["name"]); got != "r2" {
		t.Fatalf("expected created merge edge property name=r2, got %q", got)
	}
}

func TestApplyWriteEventsToSinkMergeVertexOnCreateAndOnMatchLabels(t *testing.T) {
	t.Run("on create", func(t *testing.T) {
		sink := &recordingSink{}
		err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
			Kind: "MERGE",
			Vertex: &operators.VertexMutation{
				Var:     "a",
				IDParam: "id",
				Labels:  []string{"User"},
			},
			ResolvedParams: map[string]any{"id": "u1"},
			MergeOnCreate:  []string{"SET a:M2"},
			MergeOnMatch:   []string{"SET a:M1"},
		}})
		if err != nil {
			t.Fatalf("apply merge write events failed: %v", err)
		}
		vertex := sink.vertexByID["u1"]
		if vertex == nil {
			t.Fatalf("expected merged vertex u1")
		}
		if !reflect.DeepEqual(vertex.Labels, []string{"User", "M2"}) {
			t.Fatalf("expected ON CREATE labels, got %#v", vertex.Labels)
		}
	})

	t.Run("on match", func(t *testing.T) {
		sink := &recordingSink{}
		if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			t.Fatalf("seed vertex failed: %v", err)
		}
		err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
			Kind: "MERGE",
			Vertex: &operators.VertexMutation{
				Var:     "a",
				IDParam: "id",
				Labels:  []string{"User"},
			},
			ResolvedParams: map[string]any{"id": "u1"},
			MergeOnCreate:  []string{"SET a:M2"},
			MergeOnMatch:   []string{"SET a:M1"},
		}})
		if err != nil {
			t.Fatalf("apply merge write events failed: %v", err)
		}
		vertex := sink.vertexByID["u1"]
		if vertex == nil {
			t.Fatalf("expected merged vertex u1")
		}
		if !reflect.DeepEqual(vertex.Labels, []string{"User", "M1"}) {
			t.Fatalf("expected ON MATCH labels, got %#v", vertex.Labels)
		}
	})
}

func TestApplyWriteEventsToSinkRemoveVertexLabel(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"Foo", "Bar"}}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:     "REMOVE",
		Raw:      "REMOVE n:Foo",
		Bindings: map[string]any{"n": "u1"},
	}})
	if err != nil {
		t.Fatalf("apply remove write event failed: %v", err)
	}
	vertex := sink.vertexByID["u1"]
	if vertex == nil {
		t.Fatalf("expected vertex u1 to remain present")
	}
	if !reflect.DeepEqual(vertex.Labels, []string{"Bar"}) {
		t.Fatalf("expected labels [Bar] after REMOVE, got %#v", vertex.Labels)
	}
}

func TestApplyWriteEventsToSinkSetPropertyWithParenthesizedTarget(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"A"}}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:     "SET",
		Raw:      "SET (n).name = 'neo4j'",
		Bindings: map[string]any{"n": "u1"},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	vertex := sink.vertexByID["u1"]
	if vertex == nil {
		t.Fatalf("expected vertex u1 to remain present")
	}
	if got := string(vertex.Properties["name"]); got != "neo4j" {
		t.Fatalf("expected name=neo4j after SET, got %q", got)
	}
}

func TestApplyWriteEventsToSinkSetPropertyWithMapEntityBinding(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"A"}}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "SET",
		Raw:  "SET n.name = 'neo4j'",
		Bindings: map[string]any{
			"n":    map[string]any{"id": "u1", "labels": []string{"A"}},
			"n.id": "u1",
		},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	vertex := sink.vertexByID["u1"]
	if vertex == nil {
		t.Fatalf("expected vertex u1 to remain present")
	}
	if got := string(vertex.Properties["name"]); got != "neo4j" {
		t.Fatalf("expected name=neo4j after map binding SET, got %q", got)
	}
}

func TestApplyWriteEventsToSinkSetPropertyWithMapEntityBindingAndDottedID(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"A"}}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}
	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "SET",
		Raw:  "SET n.name = 'neo4j'",
		Bindings: map[string]any{
			"n":    map[string]any{"labels": []string{"A"}, "properties": map[string]any{}},
			"n.id": "u1",
		},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	vertex := sink.vertexByID["u1"]
	if vertex == nil {
		t.Fatalf("expected vertex u1 to remain present")
	}
	if got := string(vertex.Properties["name"]); got != "neo4j" {
		t.Fatalf("expected name=neo4j after dotted-id fallback SET, got %q", got)
	}
}

func TestApplyWriteEventsToSinkSetPropertyExpressionUsesBindings(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"A"},
		Properties: graph.PropertyMap{
			"name": []byte("Andres"),
		},
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind: "SET",
		Raw:  "SET n.name = n.name + ' was here'",
		Bindings: map[string]any{
			"n": map[string]any{"id": "u1", "name": "Andres"},
		},
	}})
	if err != nil {
		t.Fatalf("apply set write event failed: %v", err)
	}
	v := sink.vertexByID["u1"]
	if v == nil {
		t.Fatalf("expected vertex u1 to exist after SET")
	}
	if got := string(v.Properties["name"]); got != "Andres was here" {
		t.Fatalf("expected expression-based property update, got %q", got)
	}
}

func TestApplyWriteEventsToSinkSetPropertyRejectsListOfMaps(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"A"}}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}

	err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:     "SET",
		Raw:      "SET n.maplist = [{num: 1}]",
		Bindings: map[string]any{"n": "u1"},
	}})
	if err == nil {
		t.Fatalf("expected InvalidPropertyType error for list-of-map SET")
	}
	if !graph.IsKind(err, graph.ErrKindInvalidInput) {
		t.Fatalf("expected invalid input error, got %v", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "invalidpropertytype") {
		t.Fatalf("expected InvalidPropertyType error message, got %v", err)
	}
}

func TestApplyWriteEventsToSinkMergeAnonymousVertexMatchesExistingByLabelAndProps(t *testing.T) {
	sink := &recordingSink{}
	if err := sink.PutVertex(context.Background(), &graph.Vertex{
		Tenant: "acme",
		ID:     "u1",
		Labels: []string{"User"},
		Properties: graph.PropertyMap{
			"name": []byte("alice"),
		},
	}); err != nil {
		t.Fatalf("seed vertex failed: %v", err)
	}
	preWrites := len(sink.vertexes)
	event := operators.WriteEvent{
		Kind:         "MERGE",
		MutationType: operators.MutationTypeVertex,
		Vertex: &operators.VertexMutation{
			Labels:  []string{"User"},
			Pattern: "(:User {name:'alice'})",
		},
	}
	matchedID, err := findAnonymousMergeVertexID(context.Background(), sink, "acme", event.Vertex.Labels, resolveVertexMutationProperties(context.Background(), sink, "acme", event), nil)
	if err != nil {
		t.Fatalf("anonymous merge pre-match failed: %v", err)
	}
	if matchedID != "u1" {
		t.Fatalf("expected anonymous merge pre-match u1, got %q", matchedID)
	}

	err = applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{event})
	if err != nil {
		t.Fatalf("apply merge write events failed: %v", err)
	}
	if len(sink.vertexes) != preWrites {
		t.Fatalf("expected anonymous MERGE to match existing vertex, got writes %#v", sink.vertexes)
	}
}

func TestExtractCreateLikeClauseBodyPrefersExplicitPattern(t *testing.T) {
	event := operators.WriteEvent{
		Kind:    "CREATE",
		Raw:     "MATCH (a) CREATE (a)-[:KNOWS]->(b) RETURN b",
		Pattern: "(a)-[:KNOWS]->(b)",
	}
	body, ok := extractCreateLikeClauseBody(event)
	if !ok {
		t.Fatalf("expected clause body extraction to succeed")
	}
	if body != "(a)-[:KNOWS]->(b)" {
		t.Fatalf("expected explicit pattern body, got %q", body)
	}
}

func TestApplyWriteEventsToSinkAvoidsAnonymousVertexIDCollisionsAcrossStatements(t *testing.T) {
	sink := &recordingSink{}

	if err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:    "CREATE",
		Pattern: "(:Begin)",
		Raw:     "CREATE (:Begin)",
	}}); err != nil {
		t.Fatalf("seed create failed: %v", err)
	}

	if err := applyWriteEventsToSink(context.Background(), sink, "acme", []operators.WriteEvent{{
		Kind:    "CREATE",
		Pattern: "(x:Begin)-[:TYPE]->(:End)",
		Raw:     "CREATE (x:Begin)-[:TYPE]->(:End)",
		Bindings: map[string]any{
			"x": "auto-v-1",
		},
	}}); err != nil {
		t.Fatalf("create edge with anonymous end failed: %v", err)
	}

	if sink.vertexByID["auto-v-2"] == nil {
		t.Fatalf("expected distinct anonymous end vertex ID auto-v-2, got vertexes=%#v", sink.vertexes)
	}
}
