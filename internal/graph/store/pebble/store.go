package pebblestore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/paegun/vitaledge/internal/graph"
	"github.com/paegun/vitaledge/internal/graph/keyspace"
)

type Store struct {
	db                 *cpebble.DB
	edgeLocks          sync.Map
	metrics            Metrics
	maxWriteBatchBytes int
}

const currentSchemaVersion = 1

type kvReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(o *cpebble.IterOptions) (*cpebble.Iterator, error)
}

type kvWriter interface {
	Set(key []byte, value []byte, opts *cpebble.WriteOptions) error
	Delete(key []byte, opts *cpebble.WriteOptions) error
}

type tx struct {
	store              *Store
	mode               graph.TxMode
	reader             kvReader
	writer             kvWriter
	snapshot           *cpebble.Snapshot
	batch              *cpebble.Batch
	locks              map[string]func()
	counterBase        map[string]int
	counterBasePresent map[string]bool
	counterDeltas      map[string]int
	closed             bool
	maxWriteBatchBytes int
}

var _ graph.GraphStore = (*Store)(nil)
var _ graph.Tx = (*tx)(nil)

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, StoreOptions{})
}

func OpenWithOptions(path string, opts StoreOptions) (*Store, error) {
	pebbleOpts := &cpebble.Options{}
	if opts.PebbleOptions != nil {
		pebbleOpts = opts.PebbleOptions
	}
	db, err := cpebble.Open(path, pebbleOpts)
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "open pebble db", err)
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = defaultMetrics
	}
	maxWriteBatchBytes := opts.MaxWriteBatchBytes
	if maxWriteBatchBytes <= 0 {
		maxWriteBatchBytes = DefaultMaxWriteBatchBytes
	}
	store := &Store{db: db, metrics: metrics, maxWriteBatchBytes: maxWriteBatchBytes}
	if err := store.runMigrations(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) runMigrations(ctx context.Context) error {
	if s == nil || s.db == nil {
		return graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}
	version, err := s.schemaVersion()
	if err != nil {
		return err
	}
	if version >= currentSchemaVersion {
		return nil
	}

	return s.Update(ctx, func(txn graph.Tx) error {
		t, ok := txn.(*tx)
		if !ok {
			return graph.NewError(graph.ErrKindStorage, "unexpected tx implementation for migration", nil)
		}
		tenants, err := t.collectStatsBackfillTenants()
		if err != nil {
			return err
		}
		for _, tenant := range tenants {
			if err := t.backfillTenantStats(ctx, tenant); err != nil {
				return err
			}
		}
		if err := t.setUnchecked(keyspace.SchemaVersionKey(), []byte(strconv.Itoa(currentSchemaVersion)), "write schema version"); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) schemaVersion() (int, error) {
	if s == nil || s.db == nil {
		return 0, graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}
	value, closer, err := s.db.Get(keyspace.SchemaVersionKey())
	if err != nil {
		if errors.Is(err, cpebble.ErrNotFound) {
			return 0, nil
		}
		return 0, graph.NewError(graph.ErrKindStorage, "read schema version", err)
	}
	defer closer.Close()

	parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(value)))
	if parseErr != nil {
		return 0, graph.NewError(graph.ErrKindStorage, "decode schema version", parseErr)
	}
	if parsed < 0 {
		return 0, nil
	}
	return parsed, nil
}

func (s *Store) BeginTx(ctx context.Context, opts graph.TxOptions) (graph.Tx, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}

	switch opts.Mode {
	case graph.TxReadOnly:
		snap := s.db.NewSnapshot()
		return &tx{store: s, mode: opts.Mode, reader: snap, snapshot: snap}, nil
	case graph.TxReadWrite:
		batch := s.db.NewIndexedBatch()
		return &tx{store: s, mode: opts.Mode, reader: batch, writer: batch, batch: batch, maxWriteBatchBytes: s.maxWriteBatchBytes}, nil
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "unsupported transaction mode", nil)
	}
}

func (s *Store) View(ctx context.Context, fn func(graph.Tx) error) error {
	started := time.Now()
	var txErr error
	defer func() {
		s.observeTx(graph.TxReadOnly, txErr, started)
	}()

	txn, err := s.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		txErr = err
		return txErr
	}
	defer func() {
		_ = txn.Rollback()
	}()

	if err := fn(txn); err != nil {
		txErr = err
		return txErr
	}
	txErr = txn.Commit()
	return txErr
}

func (s *Store) Update(ctx context.Context, fn func(graph.Tx) error) error {
	started := time.Now()
	var txErr error
	defer func() {
		s.observeTx(graph.TxReadWrite, txErr, started)
	}()

	txn, err := s.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadWrite})
	if err != nil {
		txErr = err
		return txErr
	}
	defer func() {
		_ = txn.Rollback()
	}()

	if err := fn(txn); err != nil {
		txErr = err
		return txErr
	}
	txErr = txn.Commit()
	return txErr
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "close pebble db", err)
	}
	s.db = nil
	return nil
}

func (t *tx) GetVertex(ctx context.Context, tenant, vertexID string) (vertex *graph.Vertex, err error) {
	started := time.Now()
	defer func() { t.observeOperation("get_vertex", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	if tenant == "" || vertexID == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "tenant and vertex id are required", nil)
	}
	buf, err := t.get(keyspace.VertexKey(tenant, vertexID))
	if err != nil {
		return nil, err
	}
	var v graph.Vertex
	if err := json.Unmarshal(buf, &v); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "decode vertex", err)
	}
	vertex = &v
	return vertex, nil
}

func (t *tx) ScanVertices(ctx context.Context, tenant string, limit int, fn func(*graph.Vertex) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_vertices", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant and callback are required", nil)
	}

	prefix := keyspace.VertexPrefix(tenant)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create vertex iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		var v graph.Vertex
		if err := json.Unmarshal(iter.Value(), &v); err != nil {
			return graph.NewError(graph.ErrKindStorage, "decode vertex", err)
		}
		if err := fn(&v); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan vertices", err)
	}
	return nil
}

func (t *tx) PutVertex(ctx context.Context, vertex *graph.Vertex) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_vertex", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if vertex == nil || vertex.Tenant == "" || vertex.ID == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "vertex tenant and id are required", nil)
	}
	previous, err := t.GetVertex(ctx, vertex.Tenant, vertex.ID)
	if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	if previous == nil {
		if err := t.addToCounter(keyspace.StatsVertexTotalKey(vertex.Tenant), 1); err != nil {
			return err
		}
	}
	prevLabels := normalizedLabelSet(nil)
	if previous != nil {
		prevLabels = normalizedLabelSet(previous.Labels)
	}
	nextLabels := normalizedLabelSet(vertex.Labels)
	for label := range prevLabels {
		if _, ok := nextLabels[label]; ok {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(vertex.Tenant, label), -1); err != nil {
			return err
		}
	}
	for label := range nextLabels {
		if _, ok := prevLabels[label]; ok {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(vertex.Tenant, label), 1); err != nil {
			return err
		}
	}
	buf, err := json.Marshal(vertex)
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "encode vertex", err)
	}
	if err := t.set(keyspace.VertexKey(vertex.Tenant, vertex.ID), buf, "write vertex"); err != nil {
		return err
	}
	return nil
}

func (t *tx) DeleteVertex(ctx context.Context, tenant, vertexID string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_vertex", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if tenant == "" || vertexID == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant and vertex id are required", nil)
	}
	vertex, err := t.GetVertex(ctx, tenant, vertexID)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		}
		return err
	}
	if vertex != nil {
		if err := t.addToCounter(keyspace.StatsVertexTotalKey(tenant), -1); err != nil {
			return err
		}
		for label := range normalizedLabelSet(vertex.Labels) {
			if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(tenant, label), -1); err != nil {
				return err
			}
		}
	}
	if err := t.delete(keyspace.VertexKey(tenant, vertexID), "delete vertex"); err != nil {
		return err
	}
	return nil
}

func (t *tx) GetStatsSnapshot(ctx context.Context, tenant string) (snapshot *graph.StatsSnapshot, err error) {
	started := time.Now()
	defer func() { t.observeOperation("get_stats_snapshot", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	if tenant == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "tenant is required", nil)
	}

	vertexTotal, hasVertex, err := t.readCounterMaybeMissing(keyspace.StatsVertexTotalKey(tenant))
	if err != nil {
		return nil, err
	}
	edgeTotal, hasEdge, err := t.readCounterMaybeMissing(keyspace.StatsEdgeTotalKey(tenant))
	if err != nil {
		return nil, err
	}
	if !hasVertex && !hasEdge {
		return nil, graph.NewError(graph.ErrKindNotFound, "stats snapshot not found", nil)
	}
	labelCounts, err := t.scanCounterMap(keyspace.StatsVertexLabelPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgeCounts, err := t.scanCounterMap(keyspace.StatsEdgeTypePrefix(tenant))
	if err != nil {
		return nil, err
	}

	return &graph.StatsSnapshot{
		Tenant:      tenant,
		VertexTotal: vertexTotal,
		EdgeTotal:   edgeTotal,
		LabelCounts: labelCounts,
		EdgeCounts:  edgeCounts,
	}, nil
}

func (t *tx) GetEdge(ctx context.Context, tenant, edgeID string) (edge *graph.Edge, err error) {
	started := time.Now()
	defer func() { t.observeOperation("get_edge", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	if tenant == "" || edgeID == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "tenant and edge id are required", nil)
	}
	buf, err := t.get(keyspace.EdgeKey(tenant, edgeID))
	if err != nil {
		return nil, err
	}
	var e graph.Edge
	if err := json.Unmarshal(buf, &e); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "decode edge", err)
	}
	edge = &e
	return edge, nil
}

func (t *tx) PutEdge(ctx context.Context, edge *graph.Edge) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_edge", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if edge == nil || edge.Tenant == "" || edge.ID == "" || edge.SrcID == "" || edge.DstID == "" || edge.Type == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "edge tenant/id/src/dst/type are required", nil)
	}
	t.lockEdgeForMutation(edge.Tenant, edge.ID)

	previous, err := t.GetEdge(ctx, edge.Tenant, edge.ID)
	if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	if previous == nil {
		if err := t.addToCounter(keyspace.StatsEdgeTotalKey(edge.Tenant), 1); err != nil {
			return err
		}
		if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(edge.Tenant, normalizedEdgeType(edge.Type)), 1); err != nil {
			return err
		}
	}
	if previous != nil {
		oldType := normalizedEdgeType(previous.Type)
		newType := normalizedEdgeType(edge.Type)
		if oldType != newType {
			if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(edge.Tenant, oldType), -1); err != nil {
				return err
			}
			if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(edge.Tenant, newType), 1); err != nil {
				return err
			}
		}
	}
	if previous != nil {
		if err := t.delete(keyspace.OutAdjacencyKey(previous.Tenant, previous.SrcID, previous.Type, previous.ID), "delete stale out adjacency"); err != nil {
			return err
		}
		if err := t.delete(keyspace.InAdjacencyKey(previous.Tenant, previous.DstID, previous.Type, previous.ID), "delete stale in adjacency"); err != nil {
			return err
		}
	}

	buf, err := json.Marshal(edge)
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "encode edge", err)
	}
	if err := t.set(keyspace.EdgeKey(edge.Tenant, edge.ID), buf, "write edge"); err != nil {
		return err
	}
	if err := t.set(keyspace.OutAdjacencyKey(edge.Tenant, edge.SrcID, edge.Type, edge.ID), []byte(edge.ID), "write out adjacency"); err != nil {
		return err
	}
	if err := t.set(keyspace.InAdjacencyKey(edge.Tenant, edge.DstID, edge.Type, edge.ID), []byte(edge.ID), "write in adjacency"); err != nil {
		return err
	}
	return nil
}

func (t *tx) DeleteEdge(ctx context.Context, tenant, edgeID string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_edge", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if tenant == "" || edgeID == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant and edge id are required", nil)
	}
	t.lockEdgeForMutation(tenant, edgeID)

	edge, err := t.GetEdge(ctx, tenant, edgeID)
	if err != nil {
		return err
	}
	if err := t.addToCounter(keyspace.StatsEdgeTotalKey(tenant), -1); err != nil {
		return err
	}
	if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(tenant, normalizedEdgeType(edge.Type)), -1); err != nil {
		return err
	}
	if err := t.delete(keyspace.EdgeKey(tenant, edgeID), "delete edge"); err != nil {
		return err
	}
	if err := t.delete(keyspace.OutAdjacencyKey(edge.Tenant, edge.SrcID, edge.Type, edge.ID), "delete out adjacency"); err != nil {
		return err
	}
	if err := t.delete(keyspace.InAdjacencyKey(edge.Tenant, edge.DstID, edge.Type, edge.ID), "delete in adjacency"); err != nil {
		return err
	}
	return nil
}

func (t *tx) ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*graph.Edge) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edges", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || srcID == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, src id, and callback are required", nil)
	}
	return t.scanAdjacency(ctx, keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType), limit, tenant, fn)
}

// ScanOutEdgeIDs scans outgoing adjacency keys and yields edge IDs without loading edge records.
func (t *tx) ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_ids", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || srcID == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, src id, and callback are required", nil)
	}
	return t.scanAdjacencyEdgeIDs(ctx, keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType), limit, fn)
}

func (t *tx) ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*graph.Edge) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_in_edges", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || dstID == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, dst id, and callback are required", nil)
	}
	return t.scanAdjacency(ctx, keyspace.InAdjacencyPrefix(tenant, dstID, edgeType), limit, tenant, fn)
}

// ScanInEdgeIDs scans incoming adjacency keys and yields edge IDs without loading edge records.
func (t *tx) ScanInEdgeIDs(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_in_edge_ids", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || dstID == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, dst id, and callback are required", nil)
	}
	return t.scanAdjacencyEdgeIDs(ctx, keyspace.InAdjacencyPrefix(tenant, dstID, edgeType), limit, fn)
}

func (t *tx) ScanPropertyIndex(ctx context.Context, tenant, schema, property string, encodedValue []byte, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_property_index", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || schema == "" || property == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, property, and callback are required", nil)
	}
	return t.scanPropertyIndex(ctx, keyspace.PropertyIndexValuePrefix(tenant, schema, property, encodedValue), tenant, schema, property, limit, fn)
}

func (t *tx) PutPropertyIndex(ctx context.Context, entry *graph.PropertyIndexEntry) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_property_index", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if err := validatePropertyEntry(entry); err != nil {
		return err
	}
	key := keyspace.PropertyIndexKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID)
	if err := t.set(key, []byte(entry.EntityClass), "write property index"); err != nil {
		return err
	}
	return nil
}

func (t *tx) DeletePropertyIndex(ctx context.Context, entry *graph.PropertyIndexEntry) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_property_index", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if err := validatePropertyEntry(entry); err != nil {
		return err
	}
	key := keyspace.PropertyIndexKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID)
	if err := t.delete(key, "delete property index"); err != nil {
		return err
	}
	return nil
}

func (t *tx) Commit() error {
	if t == nil || t.closed {
		return nil
	}
	defer t.closeResources()
	if t.mode == graph.TxReadOnly {
		return nil
	}
	if err := t.flushCounterDeltas(); err != nil {
		return err
	}
	if err := t.batch.Commit(cpebble.Sync); err != nil {
		return graph.NewError(graph.ErrKindStorage, "commit transaction", err)
	}
	return nil
}

func (t *tx) set(key, value []byte, action string) (err error) {
	if err := t.ensureBatchCapacity(len(key) + len(value) + 64); err != nil {
		return err
	}
	return t.setUnchecked(key, value, action)
}

func (t *tx) setUnchecked(key, value []byte, action string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = t.handleRecoveredWrite(action, recovered)
		}
	}()
	if err := t.writer.Set(key, value, nil); err != nil {
		return graph.NewError(graph.ErrKindStorage, action, err)
	}
	return nil
}

func (t *tx) delete(key []byte, action string) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = t.handleRecoveredWrite(action, recovered)
		}
	}()
	if err := t.writer.Delete(key, nil); err != nil {
		return graph.NewError(graph.ErrKindStorage, action, err)
	}
	return nil
}

func (t *tx) ensureBatchCapacity(estimatedDelta int) error {
	if t.mode != graph.TxReadWrite || t.batch == nil || t.maxWriteBatchBytes <= 0 {
		return nil
	}
	if t.batch.Len()+estimatedDelta > t.maxWriteBatchBytes {
		return graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("transaction batch exceeds max_write_batch_bytes capability (%d bytes)", t.maxWriteBatchBytes), nil)
	}
	return nil
}

func (t *tx) handleRecoveredWrite(action string, recovered any) error {
	panicText := fmt.Sprint(recovered)
	if strings.Contains(panicText, "pebble: batch too large") {
		return graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("transaction batch exceeds max_write_batch_bytes capability (%d bytes)", t.maxWriteBatchBytes), nil)
	}
	return graph.NewError(graph.ErrKindStorage, action, fmt.Errorf("panic: %v", recovered))
}

func (t *tx) Rollback() error {
	if t == nil || t.closed {
		return nil
	}
	t.closeResources()
	return nil
}

func (t *tx) scanAdjacency(ctx context.Context, prefix []byte, limit int, tenant string, fn func(*graph.Edge) error) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create adjacency iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			return graph.NewError(graph.ErrKindStorage, "malformed adjacency key", nil)
		}
		edge, err := t.GetEdge(ctx, tenant, edgeID)
		if err != nil {
			return err
		}
		if err := fn(edge); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan adjacency", err)
	}
	return nil
}

func (t *tx) scanAdjacencyEdgeIDs(ctx context.Context, prefix []byte, limit int, fn func(string) error) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create adjacency iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			return graph.NewError(graph.ErrKindStorage, "malformed adjacency key", nil)
		}
		if err := fn(edgeID); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan adjacency", err)
	}
	return nil
}

func (t *tx) scanPropertyIndex(ctx context.Context, prefix []byte, tenant, schema, property string, limit int, fn func(*graph.PropertyIndexEntry) error) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create property index iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		entry, err := propertyIndexEntryFromKey(iter.Key(), iter.Value(), tenant, schema, property)
		if err != nil {
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan property index", err)
	}
	return nil
}

func (t *tx) get(key []byte) ([]byte, error) {
	value, closer, err := t.reader.Get(key)
	if err != nil {
		if errors.Is(err, cpebble.ErrNotFound) {
			return nil, graph.NewError(graph.ErrKindNotFound, fmt.Sprintf("record not found for key %q", key), err)
		}
		return nil, graph.NewError(graph.ErrKindStorage, "read key", err)
	}
	defer closer.Close()

	out := make([]byte, len(value))
	copy(out, value)
	return out, nil
}

func (t *tx) readCounterMaybeMissing(key []byte) (value int, found bool, err error) {
	if value, found, ok, err := t.counterValueFromPending(key); ok || err != nil {
		return value, found, err
	}
	buf, err := t.get(key)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	parsed, parseErr := strconv.ParseInt(strings.TrimSpace(string(buf)), 10, 64)
	if parseErr != nil {
		return 0, false, graph.NewError(graph.ErrKindStorage, "decode counter", parseErr)
	}
	if parsed < 0 {
		parsed = 0
	}
	value = int(parsed)
	if t != nil && t.mode == graph.TxReadWrite {
		if t.counterBase == nil {
			t.counterBase = map[string]int{}
		}
		if t.counterBasePresent == nil {
			t.counterBasePresent = map[string]bool{}
		}
		t.counterBase[string(key)] = value
		t.counterBasePresent[string(key)] = true
	}
	return value, true, nil
}

func (t *tx) addToCounter(key []byte, delta int) error {
	if t == nil || t.mode != graph.TxReadWrite {
		return graph.NewError(graph.ErrKindUnsupported, "transaction is read only", nil)
	}
	if _, _, err := t.readCounterMaybeMissing(key); err != nil {
		return err
	}
	if t.counterDeltas == nil {
		t.counterDeltas = map[string]int{}
	}
	t.counterDeltas[string(key)] += delta
	return nil
}

func (t *tx) counterValueFromPending(key []byte) (value int, found bool, ok bool, err error) {
	if t == nil || t.mode != graph.TxReadWrite {
		return 0, false, false, nil
	}
	if t.counterBase == nil {
		t.counterBase = map[string]int{}
	}
	if t.counterBasePresent == nil {
		t.counterBasePresent = map[string]bool{}
	}
	keyStr := string(key)
	base, hasBase := t.counterBase[keyStr]
	basePresent := t.counterBasePresent[keyStr]
	if !hasBase && !basePresent {
		return 0, false, false, nil
	}
	delta := 0
	if t.counterDeltas != nil {
		delta = t.counterDeltas[keyStr]
	}
	next := base + delta
	if next < 0 {
		next = 0
	}
	found = basePresent || delta != 0
	return next, found, true, nil
}

func (t *tx) flushCounterDeltas() error {
	if t == nil || t.mode != graph.TxReadWrite || len(t.counterDeltas) == 0 {
		return nil
	}
	keys := make([]string, 0, len(t.counterDeltas))
	for key, delta := range t.counterDeltas {
		if delta != 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		delta := t.counterDeltas[key]
		if delta == 0 {
			continue
		}
		base, hasBase, err := t.loadCounterBaseMaybeMissing([]byte(key))
		if err != nil {
			return err
		}
		if !hasBase {
			base = 0
		}
		next := base + delta
		if next < 0 {
			next = 0
		}
		if err := t.setUnchecked([]byte(key), []byte(strconv.FormatInt(int64(next), 10)), "write counter"); err != nil {
			return err
		}
		t.counterBase[key] = next
		t.counterBasePresent[key] = true
		t.counterDeltas[key] = 0
	}
	return nil
}

func (t *tx) scanCounterMap(prefix []byte) (map[string]int, error) {
	out := map[string]int{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create counter iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := strings.TrimSpace(string(key[len(prefix):]))
		if suffix == "" {
			continue
		}
		value, parseErr := strconv.ParseInt(strings.TrimSpace(string(iter.Value())), 10, 64)
		if parseErr != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode counter", parseErr)
		}
		if value > 0 {
			out[suffix] = int(value)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan counters", err)
	}
	return out, nil
}

func (t *tx) collectStatsBackfillTenants() ([]string, error) {
	tenants := map[string]struct{}{}
	collect := func(prefix []byte) error {
		iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
		if err != nil {
			return graph.NewError(graph.ErrKindStorage, "create tenant iterator", err)
		}
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			parts := strings.SplitN(string(iter.Key()), "/", 3)
			if len(parts) < 2 {
				continue
			}
			tenant := strings.TrimSpace(parts[1])
			if tenant == "" {
				continue
			}
			tenants[tenant] = struct{}{}
		}
		if err := iter.Error(); err != nil {
			return graph.NewError(graph.ErrKindStorage, "scan tenants", err)
		}
		return nil
	}
	if err := collect([]byte("v/")); err != nil {
		return nil, err
	}
	if err := collect([]byte("e/")); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(tenants))
	for tenant := range tenants {
		out = append(out, tenant)
	}
	sort.Strings(out)
	return out, nil
}

func (t *tx) backfillTenantStats(ctx context.Context, tenant string) error {
	if strings.TrimSpace(tenant) == "" {
		return nil
	}
	vertexTotal := 0
	edgeTotal := 0
	labelCounts := map[string]int{}
	edgeCounts := map[string]int{}

	if err := t.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		vertexTotal++
		for label := range normalizedLabelSet(v.Labels) {
			labelCounts[label]++
		}
		return nil
	}); err != nil {
		return err
	}

	edgePrefix := keyspace.EdgePrefix(tenant)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: edgePrefix, UpperBound: prefixUpperBound(edgePrefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create edge iterator", err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		var edge graph.Edge
		if err := json.Unmarshal(iter.Value(), &edge); err != nil {
			return graph.NewError(graph.ErrKindStorage, "decode edge", err)
		}
		edgeTotal++
		edgeCounts[normalizedEdgeType(edge.Type)]++
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan edges", err)
	}

	if err := t.clearCounterPrefix(keyspace.StatsVertexLabelPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgeTypePrefix(tenant)); err != nil {
		return err
	}

	if err := t.setUnchecked(keyspace.StatsVertexTotalKey(tenant), []byte(strconv.Itoa(vertexTotal)), "write vertex total"); err != nil {
		return err
	}
	if err := t.setUnchecked(keyspace.StatsEdgeTotalKey(tenant), []byte(strconv.Itoa(edgeTotal)), "write edge total"); err != nil {
		return err
	}
	labelKeys := make([]string, 0, len(labelCounts))
	for label := range labelCounts {
		labelKeys = append(labelKeys, label)
	}
	sort.Strings(labelKeys)
	for _, label := range labelKeys {
		if err := t.setUnchecked(keyspace.StatsVertexLabelCountKey(tenant, label), []byte(strconv.Itoa(labelCounts[label])), "write label count"); err != nil {
			return err
		}
	}
	typeKeys := make([]string, 0, len(edgeCounts))
	for edgeType := range edgeCounts {
		typeKeys = append(typeKeys, edgeType)
	}
	sort.Strings(typeKeys)
	for _, edgeType := range typeKeys {
		if err := t.setUnchecked(keyspace.StatsEdgeTypeCountKey(tenant, edgeType), []byte(strconv.Itoa(edgeCounts[edgeType])), "write edge type count"); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) clearCounterPrefix(prefix []byte) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create counter clear iterator", err)
	}
	defer iter.Close()
	keys := make([][]byte, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		keys = append(keys, key)
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan counters for clear", err)
	}
	for _, key := range keys {
		if err := t.delete(key, "clear counter key"); err != nil {
			return err
		}
	}
	return nil
}

func normalizedLabelSet(labels []string) map[string]struct{} {
	set := map[string]struct{}{}
	if len(labels) == 0 {
		set["UNLABELED"] = struct{}{}
		return set
	}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		set[label] = struct{}{}
	}
	if len(set) == 0 {
		set["UNLABELED"] = struct{}{}
	}
	return set
}

func normalizedEdgeType(edgeType string) string {
	edgeType = strings.TrimSpace(edgeType)
	if edgeType == "" {
		return "UNTYPED"
	}
	return edgeType
}

func (t *tx) loadCounterBaseMaybeMissing(key []byte) (value int, found bool, err error) {
	if t == nil {
		return 0, false, graph.NewError(graph.ErrKindStorage, "transaction is closed", nil)
	}
	if t.counterBase == nil {
		t.counterBase = map[string]int{}
	}
	if t.counterBasePresent == nil {
		t.counterBasePresent = map[string]bool{}
	}
	keyStr := string(key)
	if basePresent, known := t.counterBasePresent[keyStr]; known {
		return t.counterBase[keyStr], basePresent, nil
	}

	buf, err := t.get(key)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			t.counterBase[keyStr] = 0
			t.counterBasePresent[keyStr] = false
			return 0, false, nil
		}
		return 0, false, err
	}
	parsed, parseErr := strconv.ParseInt(strings.TrimSpace(string(buf)), 10, 64)
	if parseErr != nil {
		return 0, false, graph.NewError(graph.ErrKindStorage, "decode counter", parseErr)
	}
	if parsed < 0 {
		parsed = 0
	}
	value = int(parsed)
	t.counterBase[keyStr] = value
	t.counterBasePresent[keyStr] = true
	return value, true, nil
}

func (t *tx) ensureActive(ctx context.Context) error {
	if t == nil || t.closed {
		return graph.NewError(graph.ErrKindStorage, "transaction is closed", nil)
	}
	return checkCtx(ctx)
}

func (t *tx) ensureWrite(ctx context.Context) error {
	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if t.mode != graph.TxReadWrite || t.writer == nil {
		return graph.NewError(graph.ErrKindUnsupported, "transaction is read only", nil)
	}
	return nil
}

func (t *tx) closeResources() {
	if t.closed {
		return
	}
	t.releaseEdgeLocks()
	if t.batch != nil {
		_ = t.batch.Close()
		t.batch = nil
	}
	if t.snapshot != nil {
		t.snapshot.Close()
		t.snapshot = nil
	}
	t.reader = nil
	t.writer = nil
	t.closed = true
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return graph.NewError(graph.ErrKindTimeout, "context canceled", err)
	}
	return nil
}

func validatePropertyEntry(entry *graph.PropertyIndexEntry) error {
	if entry == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "property index entry is required", nil)
	}
	if entry.Tenant == "" || entry.Schema == "" || entry.Property == "" || entry.EntityID == "" || entry.EntityClass == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "property index entry has missing required fields", nil)
	}
	return nil
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	b := append([]byte(nil), prefix...)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return b[:i+1]
		}
	}
	return nil
}

func edgeIDFromAdjKey(key []byte) string {
	i := bytes.LastIndexByte(key, '/')
	if i < 0 || i >= len(key)-1 {
		return ""
	}
	return string(key[i+1:])
}

func propertyIndexEntryFromKey(key, value []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return nil, graph.NewError(graph.ErrKindStorage, "malformed property index key", nil)
	}
	entityID := string(parts[len(parts)-1])
	encodedValue := parts[len(parts)-2]
	decodedValue, err := hex.DecodeString(string(encodedValue))
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "decode property index value", err)
	}
	return &graph.PropertyIndexEntry{
		Tenant:      tenant,
		Schema:      schema,
		Property:    property,
		Value:       decodedValue,
		EntityID:    entityID,
		EntityClass: string(value),
	}, nil
}

func (s *Store) lockEdge(tenant, edgeID string) func() {
	key := tenant + "\x00" + edgeID
	raw, _ := s.edgeLocks.LoadOrStore(key, &sync.Mutex{})
	mu := raw.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (t *tx) lockEdgeForMutation(tenant, edgeID string) {
	if t.store == nil {
		return
	}
	if t.locks == nil {
		t.locks = make(map[string]func())
	}
	key := tenant + "\x00" + edgeID
	if _, ok := t.locks[key]; ok {
		return
	}
	t.locks[key] = t.store.lockEdge(tenant, edgeID)
}

func (t *tx) releaseEdgeLocks() {
	if len(t.locks) == 0 {
		return
	}
	for key, unlock := range t.locks {
		unlock()
		delete(t.locks, key)
	}
}

func (s *Store) observeTx(mode graph.TxMode, err error, started time.Time) {
	if s == nil || s.metrics == nil {
		return
	}
	outcome := outcomeFromError(err)
	if outcome == "conflict" {
		s.metrics.IncTxConflict()
	}
	s.metrics.ObserveTx(mode, outcome, time.Since(started))
}

func (t *tx) observeOperation(name string, err error, started time.Time) {
	if t == nil || t.store == nil || t.store.metrics == nil {
		return
	}
	outcome := outcomeFromError(err)
	if outcome == "conflict" {
		t.store.metrics.IncTxConflict()
	}
	t.store.metrics.ObserveOperation(name, outcome, time.Since(started))
}

func outcomeFromError(err error) string {
	if err == nil {
		return "ok"
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return "not_found"
	}
	if graph.IsKind(err, graph.ErrKindConflict) {
		return "conflict"
	}
	return "error"
}
