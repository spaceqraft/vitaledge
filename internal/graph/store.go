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

// Tx defines graph operations within a transactional boundary.
type Tx interface {
	GetVertex(ctx context.Context, tenant, vertexID string) (*Vertex, error)
	ScanVertices(ctx context.Context, tenant string, limit int, fn func(*Vertex) error) error
	ScanVerticesFrom(ctx context.Context, tenant, startAfterVertexID string, limit int, fn func(*Vertex) error) error
	PutVertex(ctx context.Context, vertex *Vertex) error
	DeleteVertex(ctx context.Context, tenant, vertexID string) error
	GetStatsSnapshot(ctx context.Context, tenant string) (*StatsSnapshot, error)

	GetEdge(ctx context.Context, tenant, edgeID string) (*Edge, error)
	PutEdge(ctx context.Context, edge *Edge) error
	DeleteEdge(ctx context.Context, tenant, edgeID string) error

	ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*Edge) error) error
	ScanOutEdgeLinks(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(edgeID, dstID string) error) error
	ScanOutEdgeLinksByType(ctx context.Context, tenant, edgeType string, limit int, fn func(srcID, edgeID, dstID string) error) error
	ScanOutEdgeSourceIDs(ctx context.Context, tenant, edgeType string, limit int, fn func(string) error) error
	ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*Edge) error) error
	ScanPropertyIndex(ctx context.Context, tenant, schema, property string, encodedValue []byte, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexAll(ctx context.Context, tenant, schema, property string, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexNumericRange(ctx context.Context, tenant, schema, property string, lower float64, lowerSet bool, lowerInclusive bool, upper float64, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexBooleanRange(ctx context.Context, tenant, schema, property string, lower bool, lowerSet bool, lowerInclusive bool, upper bool, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error
	ScanPropertyIndexDateTimeRange(ctx context.Context, tenant, schema, property string, lower time.Time, lowerSet bool, lowerInclusive bool, upper time.Time, upperSet bool, upperInclusive bool, limit int, fn func(*PropertyIndexEntry) error) error

	PutPropertyIndex(ctx context.Context, entry *PropertyIndexEntry) error
	DeletePropertyIndex(ctx context.Context, entry *PropertyIndexEntry) error

	Commit() error
	Rollback() error
}

// GraphStore is the storage contract for the property graph engine.
type GraphStore interface {
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
	View(ctx context.Context, fn func(Tx) error) error
	Update(ctx context.Context, fn func(Tx) error) error
	Close() error
}
