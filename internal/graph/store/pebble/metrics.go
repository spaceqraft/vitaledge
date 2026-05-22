package pebblestore

import (
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/paegun/vitaledge/internal/graph"
)

// Metrics captures store-level observability signals.
// Implementations own registration and lifecycle outside this package.
type Metrics interface {
	ObserveTx(mode graph.TxMode, outcome string, duration time.Duration)
	ObserveOperation(name, outcome string, duration time.Duration)
	IncTxConflict()
}

// StoreOptions configures OpenWithOptions behavior.
type StoreOptions struct {
	PebbleOptions *cpebble.Options
	Metrics       Metrics
}

type noopMetrics struct{}

var defaultMetrics Metrics = noopMetrics{}

func (noopMetrics) ObserveTx(_ graph.TxMode, _ string, _ time.Duration) {}

func (noopMetrics) ObserveOperation(_, _ string, _ time.Duration) {}

func (noopMetrics) IncTxConflict() {}
