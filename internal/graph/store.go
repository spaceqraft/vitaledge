package graph

import "context"

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
	PutVertex(ctx context.Context, vertex *Vertex) error
	DeleteVertex(ctx context.Context, tenant, vertexID string) error

	GetEdge(ctx context.Context, tenant, edgeID string) (*Edge, error)
	PutEdge(ctx context.Context, edge *Edge) error
	DeleteEdge(ctx context.Context, tenant, edgeID string) error

	ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*Edge) error) error
	ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*Edge) error) error

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
