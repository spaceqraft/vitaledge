package storage

import (
	"context"

	"github.com/paegun/vitaledge/internal/graph"
)

// WriteSink captures the runtime write primitives needed by write-apply.
type WriteSink interface {
	PutVertex(context.Context, *graph.Vertex) error
	PutEdge(context.Context, *graph.Edge) error
}

// TxAdapter bridges runtime write operators to graph.Tx primitives.
type TxAdapter struct {
	tx graph.Tx
}

// NewTxAdapter creates a transaction-backed runtime write sink.
func NewTxAdapter(tx graph.Tx) *TxAdapter {
	return &TxAdapter{tx: tx}
}

// PutVertex forwards vertex writes to the underlying graph transaction.
func (a *TxAdapter) PutVertex(ctx context.Context, vertex *graph.Vertex) error {
	if a == nil || a.tx == nil {
		return nil
	}
	return a.tx.PutVertex(ctx, vertex)
}

// PutEdge forwards edge writes to the underlying graph transaction.
func (a *TxAdapter) PutEdge(ctx context.Context, edge *graph.Edge) error {
	if a == nil || a.tx == nil {
		return nil
	}
	return a.tx.PutEdge(ctx, edge)
}
