package storage

import (
	"context"

	"github.com/paegun/vitaledge/internal/graph"
)

// WriteSink captures the runtime write primitives needed by write-apply.
// graph.Tx implementations are expected to satisfy this contract directly.
type WriteSink interface {
	PutVertex(context.Context, *graph.Vertex) error
	PutEdge(context.Context, *graph.Edge) error
	PutVertexBatch(context.Context, []*graph.Vertex) error
	PutEdgeBatch(context.Context, []*graph.Edge) error
	DeleteVertexDetach(context.Context, string, string) error
	DeleteEdge(context.Context, string, string) error
	PatchVertexProperties(context.Context, string, string, graph.PropertyMap, []string) error
	PatchEdgeProperties(context.Context, string, string, graph.PropertyMap, []string) error
	EnsureEdge(context.Context, *graph.Edge) (bool, error)
	ScanAdjacencyLinks(context.Context, string, string, graph.EdgeDirection, string, int, func(edgeID, peerID string) error) error
	BatchHasDirectedEdgeBetween(context.Context, string, []graph.DirectedEdgeProbe) ([]bool, error)
	BatchHasUndirectedEdgeBetween(context.Context, string, []graph.UndirectedEdgeProbe) ([]bool, error)
	DirectedEdgePairCount(context.Context, string, string, string, string) (int, error)
	UndirectedEdgePairCount(context.Context, string, string, string, string) (int, error)
}

var _ WriteSink = (graph.Tx)(nil)