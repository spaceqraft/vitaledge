package graph

import (
	"context"
	"time"
)

// TxMode controls transaction mutability.
type TxMode int

const (
	TxReadOnly TxMode = iota
	TxReadWrite
)

// TxOptions configures transaction behavior.
type TxOptions struct {
	Mode TxMode
}

// EdgeDirection controls adjacency scan direction from an anchor vertex.
type EdgeDirection int

const (
	EdgeDirectionOut EdgeDirection = iota
	EdgeDirectionIn
	EdgeDirectionAny
)

// DirectedEdgeProbe defines one directed endpoint existence probe.
type DirectedEdgeProbe struct {
	SrcID    string
	DstID    string
	EdgeType string
}

// UndirectedEdgeProbe defines one undirected endpoint existence probe.
type UndirectedEdgeProbe struct {
	LeftID   string
	RightID  string
	EdgeType string
}

// Tx defines graph operations within a transactional boundary.
type Tx interface {
	GetStatsSnapshot(ctx context.Context, tenant string) (*StatsSnapshot, error)

	Commit() error
	Rollback() error

	GetVertex(ctx context.Context, tenant, vertexID string) (*Vertex, error)
	PutVertexBatch(ctx context.Context, vertexes []*Vertex) error
	DeleteVertexBatch(ctx context.Context, tenant string, vertexIDs []string) error
	DeleteVertexDetachBatch(ctx context.Context, tenant string, vertexIDs []string) error
	// TODO: HasVertexLabel should more likely be FilterVerticesByLabel or similar, and support batch queries
	HasVertexLabel(ctx context.Context, tenant, vertexID, label string) (bool, error)
	ScanVertices(ctx context.Context, tenant string, limit int, fn func(*Vertex) error) error
	ScanVerticesFrom(ctx context.Context, tenant, startAfterVertexID string, limit int, fn func(*Vertex) error) error

	GetEdge(ctx context.Context, tenant, edgeID string) (*Edge, error)
	PutEdgeBatch(ctx context.Context, edges []*Edge) error
	DeleteEdgeBatch(ctx context.Context, tenant string, edgeIDs []string) error
	ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*Edge) error) error
	ScanOutEdgeLinks(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(edgeID, dstID string) error) error
	ScanAdjacencyLinks(ctx context.Context, tenant, vertexID string, direction EdgeDirection, edgeType string, limit int, fn func(edgeID, peerID string) error) error
	ScanOutEdgeLinksByType(ctx context.Context, tenant, edgeType string, limit int, fn func(srcID, edgeID, dstID string) error) error
	HasDirectedEdgeBetween(ctx context.Context, tenant, srcID, dstID, edgeType string) (bool, error)
	BatchHasDirectedEdgeBetween(ctx context.Context, tenant string, probes []DirectedEdgeProbe) ([]bool, error)
	BatchHasUndirectedEdgeBetween(ctx context.Context, tenant string, probes []UndirectedEdgeProbe) ([]bool, error)
	DirectedEdgePairCount(ctx context.Context, tenant, srcID, dstID, edgeType string) (int, error)
	UndirectedEdgePairCount(ctx context.Context, tenant, leftID, rightID, edgeType string) (int, error)
	ScanOutEdgeSourceIDs(ctx context.Context, tenant, edgeType string, limit int, fn func(string) error) error
	ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*Edge) error) error

	PutPropertyIndex(ctx context.Context, entry *PropertyIndexEntry) error
	DeletePropertyIndex(ctx context.Context, entry *PropertyIndexEntry) error
	PatchVertexProperties(ctx context.Context, tenant, vertexID string, set PropertyMap, removeKeys []string) error
	PatchEdgeProperties(ctx context.Context, tenant, edgeID string, set PropertyMap, removeKeys []string) error
	ScanOutEdgeProperty(ctx context.Context, tenant, srcID, edgeType, property string, encodedValue []byte, limit int, fn func(*PropertyIndexEntry) error) error
	ScanOutEdgePropertyNumericRange(ctx context.Context, tenant, srcID, edgeType, property string, lower float64, lowerSet bool, lowerInclusive bool, upper float64, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
	HasUndirectedEdgeBetween(ctx context.Context, tenant, leftID, rightID, edgeType string) (bool, error)
	ScanPropertyIndex(ctx context.Context, tenant, schema, property string, encodedValue []byte, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexAll(ctx context.Context, tenant, schema, property string, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexNumericRange(ctx context.Context, tenant, schema, property string, lower float64, lowerSet bool, lowerInclusive bool, upper float64, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexBooleanRange(ctx context.Context, tenant, schema, property string, lower bool, lowerSet bool, lowerInclusive bool, upper bool, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexDateTimeRange(ctx context.Context, tenant, schema, property string, lower time.Time, lowerSet bool, lowerInclusive bool, upper time.Time, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
}

// GraphStore is the storage contract for the property graph engine.
type GraphStore interface {
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
	View(ctx context.Context, fn func(Tx) error) error
	Update(ctx context.Context, fn func(Tx) error) error
	Close() error
}
