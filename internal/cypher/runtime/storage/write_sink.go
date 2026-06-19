package storage

import (
	"context"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

// WriteSink captures the runtime write primitives needed by write-apply.
// graph.Tx implementations are expected to satisfy this contract directly.
type WriteSink interface {
	PutVertexBatch(context.Context, []*graph.Vertex) error
	DeleteVertexDetachBatch(context.Context, string, []string) error

	PutEdgeBatch(context.Context, []*graph.Edge) error
	DeleteEdgeBatch(context.Context, string, []string) error
	ScanAdjacencyLinks(context.Context, string, string, graph.EdgeDirection, string, int, func(edgeID, peerID string) error) error
	BatchHasDirectedEdgeBetween(context.Context, string, []graph.DirectedEdgeProbe) ([]bool, error)
	BatchHasUndirectedEdgeBetween(context.Context, string, []graph.UndirectedEdgeProbe) ([]bool, error)
	UndirectedEdgePairCount(context.Context, string, string, string, string) (int, error)
	DirectedEdgePairCount(context.Context, string, string, string, string) (int, error)

	PatchVertexProperties(context.Context, string, string, graph.PropertyMap, []string) error
	PatchEdgeProperties(context.Context, string, string, graph.PropertyMap, []string) error
}

var _ WriteSink = (graph.Tx)(nil)
