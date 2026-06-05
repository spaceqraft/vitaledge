package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
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
	ownedCache         *cpebble.Cache
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
	vertexCache        map[string]*graph.Vertex
	counterBase        map[string]int
	counterBasePresent map[string]bool
	counterDeltas      map[string]int
	closed             bool
	maxWriteBatchBytes int
}

const outAdjDstValuePrefix = "d:"
const vertexPropertySchema = "__vertex__"

var _ graph.GraphStore = (*Store)(nil)
var _ graph.Tx = (*tx)(nil)

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, StoreOptions{})
}

func OpenWithOptions(path string, opts StoreOptions) (*Store, error) {
	pebbleOpts := opts.PebbleOptions
	if pebbleOpts == nil {
		pebbleOpts = &cpebble.Options{}
	}
	var ownedCache *cpebble.Cache
	if opts.PebbleBlockCacheBytes > 0 && pebbleOpts.Cache == nil {
		ownedCache = cpebble.NewCache(opts.PebbleBlockCacheBytes)
		pebbleOpts.Cache = ownedCache
	}
	if opts.PebbleMemTableSizeBytes > 0 {
		pebbleOpts.MemTableSize = uint64(opts.PebbleMemTableSizeBytes)
	}
	if opts.PebbleMemTableStopWritesThreshold > 0 {
		pebbleOpts.MemTableStopWritesThreshold = opts.PebbleMemTableStopWritesThreshold
	}
	db, err := cpebble.Open(path, pebbleOpts)
	if err != nil {
		if ownedCache != nil {
			ownedCache.Unref()
		}
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
	store := &Store{db: db, metrics: metrics, maxWriteBatchBytes: maxWriteBatchBytes, ownedCache: ownedCache}
	if err := store.runMigrations(context.Background()); err != nil {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
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

	parsed, parseErr := strconv.Atoi(string(value))
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
	if s == nil {
		return nil
	}
	var closeErr error
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			closeErr = graph.NewError(graph.ErrKindStorage, "close pebble db", err)
		}
		s.db = nil
	}
	if s.ownedCache != nil {
		s.ownedCache.Unref()
		s.ownedCache = nil
	}
	return closeErr
}

func (t *tx) GetVertex(ctx context.Context, tenant, vertexID string) (vertex *graph.Vertex, err error) {
	started := time.Now()
	defer func() { t.observeOperation("get_vertex", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	if cached := t.vertexFromCache(tenant, vertexID); cached != nil {
		return cached, nil
	}
	buf, err := t.get(keyspace.VertexKey(tenant, vertexID))
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, graph.NewError(graph.ErrKindStorage, "decode vertex phash", nil)
	}
	vertex, err = t.loadVertexByID(ctx, tenant, vertexID)
	if err != nil {
		return nil, err
	}
	t.cacheVertex(vertex)
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
		vertexID := vertexIDFromKey(iter.Key())
		v, err := t.loadVertexByID(ctx, tenant, vertexID)
		if err != nil {
			return err
		}
		if err := fn(v); err != nil {
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

func (t *tx) ScanVerticesFrom(ctx context.Context, tenant, startAfterVertexID string, limit int, fn func(*graph.Vertex) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_vertices_from", err, started) }()

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

	seekKey := prefix
	if startAfterVertexID != "" {
		seekKey = keyspace.VertexKey(tenant, startAfterVertexID)
	}
	seen := 0
	for ok := iter.SeekGE(seekKey); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		vertexID := vertexIDFromKey(iter.Key())
		if startAfterVertexID != "" && vertexID == startAfterVertexID {
			continue
		}
		v, err := t.loadVertexByID(ctx, tenant, vertexID)
		if err != nil {
			return err
		}
		if err := fn(v); err != nil {
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

func (t *tx) ScanVerticesByLabel(ctx context.Context, tenant, label string, limit int, fn func(*graph.Vertex) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_vertices_by_label", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || label == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, label, and callback are required", nil)
	}

	prefix := keyspace.LabelVertexPrefix(tenant, label)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create label vertex iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		vertexID := string(iter.Value())
		buf, err := t.get(keyspace.VertexKey(tenant, vertexID))
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				continue
			}
			return err
		}
		if len(buf) == 0 {
			return graph.NewError(graph.ErrKindStorage, "decode vertex phash", nil)
		}
		v, err := t.loadVertexByID(ctx, tenant, vertexID)
		if err != nil {
			return err
		}
		if err := fn(v); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan vertices by label", err)
	}
	return nil
}

func (t *tx) ScanVertexIDsByLabel(ctx context.Context, tenant, label string, limit int, fn func(string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_vertex_ids_by_label", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || label == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, label, and callback are required", nil)
	}

	prefix := keyspace.LabelVertexPrefix(tenant, label)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create label vertex id iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		vertexID := string(iter.Value())
		if err := fn(vertexID); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan vertex ids by label", err)
	}
	return nil
}

func (t *tx) PutVertex(ctx context.Context, vertex *graph.Vertex) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_vertex", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if vertex == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "vertex is required", nil)
	}
	existingHash, err := t.get(keyspace.VertexKey(vertex.Tenant, vertex.ID))
	previousExists := err == nil
	if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	nextHash := vertexPHash(vertex)
	if previousExists && bytes.Equal(existingHash, nextHash) {
		return nil
	}
	if !previousExists {
		if err := t.addToCounter(keyspace.StatsVertexTotalKey(vertex.Tenant), 1); err != nil {
			return err
		}
	}
	prevLabelList := normalizedLabelsOrdered(nil)
	if previousExists {
		prevLabelList, err = t.loadVertexLabels(ctx, vertex.Tenant, vertex.ID)
		if err != nil {
			return err
		}
		prevLabelList = normalizedLabelsOrdered(prevLabelList)
	}
	nextLabelList := normalizedLabelsOrdered(vertex.Labels)
	prevLabels := labelSliceSet(prevLabelList)
	nextLabels := labelSliceSet(nextLabelList)
	for label := range prevLabels {
		if _, ok := nextLabels[label]; ok {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(vertex.Tenant, label), -1); err != nil {
			return err
		}
		if err := t.delete(keyspace.VertexLabelKey(vertex.Tenant, vertex.ID, label), "delete vertex label forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.VertexLabelMembershipKey(vertex.Tenant, label, vertex.ID), "delete vertex label membership"); err != nil {
			return err
		}
	}
	for idx, label := range nextLabelList {
		if _, ok := prevLabels[label]; ok {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(vertex.Tenant, label), 1); err != nil {
			return err
		}
		if err := t.set(keyspace.VertexLabelKey(vertex.Tenant, vertex.ID, label), encodeVertexLabelOrder(idx, label), "write vertex label forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.VertexLabelMembershipKey(vertex.Tenant, label, vertex.ID), []byte(vertex.ID), "write vertex label membership"); err != nil {
			return err
		}
	}
	if previousExists {
		if err := t.deleteVertexPropertiesBySchema(ctx, vertex.Tenant, vertex.ID, vertexPropertySchema); err != nil {
			return err
		}
	}
	if err := t.writeVertexProperties(vertex); err != nil {
		return err
	}
	if err := t.set(keyspace.VertexKey(vertex.Tenant, vertex.ID), nextHash, "write vertex"); err != nil {
		return err
	}
	t.cacheVertex(vertex)
	return nil
}

func (t *tx) DeleteVertex(ctx context.Context, tenant, vertexID string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_vertex", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if _, err := t.get(keyspace.VertexKey(tenant, vertexID)); err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		}
		return err
	}
	labels, err := t.loadVertexLabels(ctx, tenant, vertexID)
	if err != nil {
		return err
	}
	if err := t.addToCounter(keyspace.StatsVertexTotalKey(tenant), -1); err != nil {
		return err
	}
	for _, label := range normalizedLabelsOrdered(labels) {
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(tenant, label), -1); err != nil {
			return err
		}
	}
	if err := t.delete(keyspace.VertexKey(tenant, vertexID), "delete vertex"); err != nil {
		return err
	}
	t.dropVertexCache(tenant, vertexID)
	if err := t.deleteVertexPropertiesBySchema(ctx, tenant, vertexID, ""); err != nil {
		return err
	}
	for _, label := range normalizedLabelsOrdered(labels) {
		if err := t.delete(keyspace.VertexLabelKey(tenant, vertexID, label), "delete vertex label forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.VertexLabelMembershipKey(tenant, label, vertexID), "delete vertex label membership"); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) vertexCacheKey(tenant, vertexID string) string {
	return tenant + "\x00" + vertexID
}

func (t *tx) vertexFromCache(tenant, vertexID string) *graph.Vertex {
	if t == nil || len(t.vertexCache) == 0 {
		return nil
	}
	vertex := t.vertexCache[t.vertexCacheKey(tenant, vertexID)]
	return cloneVertex(vertex)
}

func (t *tx) cacheVertex(vertex *graph.Vertex) {
	if t == nil || vertex == nil {
		return
	}
	if t.vertexCache == nil {
		t.vertexCache = map[string]*graph.Vertex{}
	}
	t.vertexCache[t.vertexCacheKey(vertex.Tenant, vertex.ID)] = cloneVertex(vertex)
}

func (t *tx) dropVertexCache(tenant, vertexID string) {
	if t == nil || t.vertexCache == nil {
		return
	}
	delete(t.vertexCache, t.vertexCacheKey(tenant, vertexID))
}

func cloneVertex(vertex *graph.Vertex) *graph.Vertex {
	if vertex == nil {
		return nil
	}
	clone := &graph.Vertex{
		Tenant: vertex.Tenant,
		ID:     vertex.ID,
	}
	if len(vertex.Labels) > 0 {
		clone.Labels = append([]string(nil), vertex.Labels...)
	}
	if len(vertex.Properties) > 0 {
		clone.Properties = make(graph.PropertyMap, len(vertex.Properties))
		for key, value := range vertex.Properties {
			clone.Properties[key] = append([]byte(nil), value...)
		}
	}
	return clone
}

func (t *tx) loadVertexByID(ctx context.Context, tenant, vertexID string) (*graph.Vertex, error) {
	labels, err := t.loadVertexLabels(ctx, tenant, vertexID)
	if err != nil {
		return nil, err
	}
	properties, err := t.loadVertexProperties(ctx, tenant, vertexID)
	if err != nil {
		return nil, err
	}
	return &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: properties}, nil
}

func (t *tx) loadEdgeByID(ctx context.Context, tenant, edgeID string) (*graph.Edge, error) {
	edgeType, srcID, dstID, err := t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
	if err != nil {
		return nil, err
	}
	properties, err := t.loadEdgeProperties(ctx, tenant, edgeID)
	if err != nil {
		return nil, err
	}
	return &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID, Properties: properties}, nil
}

func (t *tx) loadEdgeTypeAndEndpoints(ctx context.Context, tenant, edgeID string) (edgeType, srcID, dstID string, err error) {
	prefix := keyspace.EdgeTypePrefix(tenant, edgeID)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "create edge type iterator", err)
	}
	defer iter.Close()

	if ok := iter.First(); !ok {
		if err := iter.Error(); err != nil {
			return "", "", "", graph.NewError(graph.ErrKindStorage, "scan edge type", err)
		}
		return "", "", "", graph.NewError(graph.ErrKindNotFound, "edge type not found", nil)
	}
	if err := checkCtx(ctx); err != nil {
		return "", "", "", err
	}
	parts := bytes.Split(iter.Key(), []byte{'/'})
	if len(parts) < 4 {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "malformed edge type key", nil)
	}
	edgeType = string(parts[len(parts)-1])
	payload, err := t.get(keyspace.TypeEdgeKey(tenant, edgeType, edgeID))
	if err != nil {
		return "", "", "", err
	}
	srcID, dstID, ok := parseEdgeEndpointsPayload(payload)
	if !ok {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "decode type edge endpoints", nil)
	}
	return edgeType, srcID, dstID, nil
}

func (t *tx) loadVertexLabels(ctx context.Context, tenant, vertexID string) ([]string, error) {
	prefix := keyspace.VertexLabelPrefix(tenant, vertexID)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create vertex label iterator", err)
	}
	defer iter.Close()

	type orderedLabel struct {
		order int
		label string
	}
	labels := make([]orderedLabel, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		parts := bytes.Split(iter.Key(), []byte{'/'})
		if len(parts) < 4 {
			return nil, graph.NewError(graph.ErrKindStorage, "malformed vertex label key", nil)
		}
		label := string(parts[len(parts)-1])
		if label == "" || label == "UNLABELED" {
			continue
		}
		order := decodeVertexLabelOrder(iter.Value(), label)
		labels = append(labels, orderedLabel{order: order, label: label})
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan vertex labels", err)
	}
	sort.SliceStable(labels, func(i, j int) bool {
		if labels[i].order == labels[j].order {
			return labels[i].label < labels[j].label
		}
		return labels[i].order < labels[j].order
	})
	out := make([]string, 0, len(labels))
	for _, item := range labels {
		out = append(out, item.label)
	}
	return out, nil
}

func (t *tx) loadVertexProperties(ctx context.Context, tenant, vertexID string) (graph.PropertyMap, error) {
	prefix := []byte("vp/" + tenant + "/" + vertexID + "/" + vertexPropertySchema + "/")
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create vertex property iterator", err)
	}
	defer iter.Close()

	properties := graph.PropertyMap{}
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		schema, property, encodedValue, ok := vertexPropertyPartsFromKey(iter.Key())
		if !ok {
			continue
		}
		if property == "" {
			continue
		}
		if _, exists := properties[property]; exists {
			continue
		}
		if schema == vertexPropertySchema {
			stored := append([]byte(nil), iter.Value()...)
			if len(stored) > 0 {
				properties[property] = stored
				continue
			}
		}
		properties[property] = encodedValue
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan vertex properties", err)
	}
	if len(properties) == 0 {
		return nil, nil
	}
	return properties, nil
}

func (t *tx) deleteVertexPropertiesBySchema(ctx context.Context, tenant, vertexID, schema string) error {
	prefix := []byte("vp/" + tenant + "/" + vertexID + "/")
	if schema != "" {
		prefix = []byte("vp/" + tenant + "/" + vertexID + "/" + schema + "/")
	}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create vertex property delete iterator", err)
	}
	defer iter.Close()

	type keyValue struct {
		key   []byte
		value []byte
	}
	items := make([]keyValue, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		items = append(items, keyValue{key: append([]byte(nil), iter.Key()...), value: append([]byte(nil), iter.Value()...)})
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan vertex properties for delete", err)
	}

	for _, item := range items {
		schema, property, encodedValue, ok := vertexPropertyPartsFromKey(item.key)
		if !ok {
			continue
		}
		if schema == vertexPropertySchema && len(item.value) > 0 {
			encodedValue = append([]byte(nil), item.value...)
		}
		if err := t.delete(item.key, "delete vertex property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyVertexKey(tenant, schema, property, encodedValue, vertexID), "delete property vertex reverse index"); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) writeVertexProperties(vertex *graph.Vertex) error {
	if vertex == nil || len(vertex.Properties) == 0 {
		return nil
	}
	for property, encodedValue := range vertex.Properties {
		if property == "" {
			continue
		}
		valueCopy := append([]byte(nil), encodedValue...)
		if err := t.set(keyspace.VertexPropertyKey(vertex.Tenant, vertex.ID, vertexPropertySchema, property, valueCopy), valueCopy, "write vertex property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyVertexKey(vertex.Tenant, vertexPropertySchema, property, valueCopy, vertex.ID), []byte(vertex.ID), "write property vertex reverse index"); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) loadEdgeProperties(ctx context.Context, tenant, edgeID string) (graph.PropertyMap, error) {
	prefix := keyspace.EdgePropertyEntityPrefix(tenant, edgeID)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create edge property iterator", err)
	}
	defer iter.Close()

	properties := graph.PropertyMap{}
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		_, property, encodedValue, ok := edgePropertyPartsFromKey(iter.Key())
		if !ok || property == "" {
			continue
		}
		if _, exists := properties[property]; exists {
			continue
		}
		properties[property] = encodedValue
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan edge properties", err)
	}
	if len(properties) == 0 {
		return nil, nil
	}
	return properties, nil
}

func (t *tx) deleteEdgePropertiesBySchema(ctx context.Context, tenant, edgeID, schema string) error {
	prefix := keyspace.EdgePropertyEntityPrefix(tenant, edgeID)
	if schema != "" {
		prefix = []byte("ep/" + tenant + "/" + edgeID + "/" + schema + "/")
	}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create edge property delete iterator", err)
	}
	defer iter.Close()

	type keyValue struct {
		key []byte
	}
	items := make([]keyValue, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		items = append(items, keyValue{key: append([]byte(nil), iter.Key()...)})
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan edge properties for delete", err)
	}

	for _, item := range items {
		schema, property, encodedValue, ok := edgePropertyPartsFromKey(item.key)
		if !ok {
			continue
		}
		if err := t.delete(item.key, "delete edge property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyEdgeKey(tenant, schema, property, encodedValue, edgeID), "delete property edge reverse index"); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) writeEdgeProperties(edge *graph.Edge) error {
	if edge == nil || len(edge.Properties) == 0 {
		return nil
	}
	for property, encodedValue := range edge.Properties {
		if property == "" {
			continue
		}
		valueCopy := append([]byte(nil), encodedValue...)
		if err := t.set(keyspace.EdgePropertyKey(edge.Tenant, edge.ID, edge.Type, property, valueCopy), []byte(edge.ID), "write edge property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyEdgeKey(edge.Tenant, edge.Type, property, valueCopy, edge.ID), []byte(edge.ID), "write property edge reverse index"); err != nil {
			return err
		}
	}
	return nil
}

func vertexPropertyPartsFromKey(key []byte) (schema, property string, encodedValue []byte, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return "", "", nil, false
	}
	schema = string(parts[len(parts)-3])
	property = string(parts[len(parts)-2])
	valueHex := string(parts[len(parts)-1])
	if schema == "" || property == "" {
		return "", "", nil, false
	}
	decodedValue, err := hex.DecodeString(valueHex)
	if err != nil {
		return "", "", nil, false
	}
	return schema, property, decodedValue, true
}

func vertexPHash(vertex *graph.Vertex) []byte {
	h := fnv.New64a()
	if vertex != nil {
		_, _ = h.Write([]byte(vertex.Tenant))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(vertex.ID))
		_, _ = h.Write([]byte{0})
		labels := make([]string, 0, len(vertex.Labels))
		for _, label := range vertex.Labels {
			normalized := label
			if normalized == "" {
				continue
			}
			labels = append(labels, normalized)
		}
		sort.Strings(labels)
		for _, label := range labels {
			_, _ = h.Write([]byte(label))
			_, _ = h.Write([]byte{0})
		}
		if len(vertex.Properties) > 0 {
			keys := make([]string, 0, len(vertex.Properties))
			for key := range vertex.Properties {
				normalized := key
				if normalized == "" {
					continue
				}
				keys = append(keys, normalized)
			}
			sort.Strings(keys)
			for _, key := range keys {
				_, _ = h.Write([]byte(key))
				_, _ = h.Write([]byte{0})
				_, _ = h.Write(vertex.Properties[key])
				_, _ = h.Write([]byte{0})
			}
		}
	}
	return []byte(strconv.FormatUint(h.Sum64(), 16))
}

func edgePHash(edge *graph.Edge) []byte {
	h := fnv.New64a()
	if edge != nil {
		_, _ = h.Write([]byte(edge.Tenant))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.ID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.SrcID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.DstID))
		_, _ = h.Write([]byte{0})
		if len(edge.Properties) > 0 {
			keys := make([]string, 0, len(edge.Properties))
			for key := range edge.Properties {
				normalized := key
				if normalized == "" {
					continue
				}
				keys = append(keys, normalized)
			}
			sort.Strings(keys)
			for _, key := range keys {
				_, _ = h.Write([]byte(key))
				_, _ = h.Write([]byte{0})
				_, _ = h.Write(edge.Properties[key])
				_, _ = h.Write([]byte{0})
			}
		}
	}
	return []byte(strconv.FormatUint(h.Sum64(), 16))
}

func normalizedLabelsOrdered(labels []string) []string {
	out := make([]string, 0, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		normalized := label
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return []string{"UNLABELED"}
	}
	return out
}

func labelSliceSet(labels []string) map[string]struct{} {
	out := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		out[label] = struct{}{}
	}
	return out
}

func encodeVertexLabelOrder(order int, label string) []byte {
	return []byte(fmt.Sprintf("%09d:%s", order, label))
}

func decodeVertexLabelOrder(raw []byte, label string) int {
	text := string(raw)
	if text == "" {
		return 0
	}
	parts := strings.SplitN(text, ":", 2)
	if len(parts) != 2 {
		return 0
	}
	if parts[1] != label {
		return 0
	}
	parsed, err := strconv.Atoi(parts[0])
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func (t *tx) HasVertexLabel(ctx context.Context, tenant, vertexID, label string) (has bool, err error) {
	started := time.Now()
	defer func() { t.observeOperation("has_vertex_label", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return false, err
	}
	if _, err := t.get(keyspace.VertexLabelKey(tenant, vertexID, label)); err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
	buf, err := t.get(keyspace.EdgeKey(tenant, edgeID))
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, graph.NewError(graph.ErrKindStorage, "decode edge phash", nil)
	}
	edge, err = t.loadEdgeByID(ctx, tenant, edgeID)
	if err != nil {
		return nil, err
	}
	return edge, nil
}

func (t *tx) getEdgeLink(ctx context.Context, tenant, edgeID string) (dstID string, err error) {
	if err := t.ensureActive(ctx); err != nil {
		return "", err
	}
	_, _, dstID, err = t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
	if err != nil {
		return "", err
	}
	if dstID == "" {
		return "", graph.NewError(graph.ErrKindStorage, "edge missing dst id", nil)
	}
	return dstID, nil
}

func (t *tx) PutEdge(ctx context.Context, edge *graph.Edge) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_edge", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if edge == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "edge is required", nil)
	}
	t.lockEdgeForMutation(edge.Tenant, edge.ID)

	existingHash, err := t.get(keyspace.EdgeKey(edge.Tenant, edge.ID))
	previousExists := err == nil
	if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	nextHash := edgePHash(edge)
	if previousExists && bytes.Equal(existingHash, nextHash) {
		return nil
	}
	var previousType, previousSrcID, previousDstID string
	if previousExists {
		previousType, previousSrcID, previousDstID, err = t.loadEdgeTypeAndEndpoints(ctx, edge.Tenant, edge.ID)
		if err != nil {
			return err
		}
	}
	if !previousExists {
		if err := t.addToCounter(keyspace.StatsEdgeTotalKey(edge.Tenant), 1); err != nil {
			return err
		}
		if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(edge.Tenant, normalizedEdgeType(edge.Type)), 1); err != nil {
			return err
		}
	}
	if previousExists {
		oldType := normalizedEdgeType(previousType)
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
	if previousExists {
		if err := t.delete(keyspace.EdgeTypeKey(edge.Tenant, edge.ID, previousType), "delete stale edge type"); err != nil {
			return err
		}
		if err := t.delete(keyspace.TypeEdgeKey(edge.Tenant, previousType, edge.ID), "delete stale type edge"); err != nil {
			return err
		}
		if err := t.delete(keyspace.OutAdjacencyKey(edge.Tenant, previousSrcID, previousType, edge.ID), "delete stale out adjacency"); err != nil {
			return err
		}
		if err := t.delete(keyspace.InAdjacencyKey(edge.Tenant, previousDstID, previousType, edge.ID), "delete stale in adjacency"); err != nil {
			return err
		}
		if err := t.delete(keyspace.OutEndpointKey(edge.Tenant, previousSrcID, previousType, previousDstID, edge.ID), "delete stale out endpoint"); err != nil {
			return err
		}
		if err := t.adjustOutEndpointPairCount(edge.Tenant, previousSrcID, previousType, previousDstID, -1); err != nil {
			return err
		}
		if err := t.adjustUndirectedEndpointPairCount(edge.Tenant, previousSrcID, previousType, previousDstID, -1); err != nil {
			return err
		}
	}

	if previousExists {
		if err := t.deleteEdgePropertiesBySchema(ctx, edge.Tenant, edge.ID, ""); err != nil {
			return err
		}
	}
	if err := t.writeEdgeProperties(edge); err != nil {
		return err
	}
	if err := t.set(keyspace.EdgeKey(edge.Tenant, edge.ID), nextHash, "write edge"); err != nil {
		return err
	}
	if err := t.set(keyspace.EdgeTypeKey(edge.Tenant, edge.ID, edge.Type), []byte(edge.Type), "write edge type"); err != nil {
		return err
	}
	if err := t.set(keyspace.TypeEdgeKey(edge.Tenant, edge.Type, edge.ID), edgeEndpointsPayload(edge.SrcID, edge.DstID), "write type edge"); err != nil {
		return err
	}
	if err := t.set(keyspace.OutAdjacencyKey(edge.Tenant, edge.SrcID, edge.Type, edge.ID), []byte(outAdjDstValuePrefix+edge.DstID), "write out adjacency"); err != nil {
		return err
	}
	if err := t.set(keyspace.InAdjacencyKey(edge.Tenant, edge.DstID, edge.Type, edge.ID), []byte(edge.ID), "write in adjacency"); err != nil {
		return err
	}
	if err := t.set(keyspace.OutEndpointKey(edge.Tenant, edge.SrcID, edge.Type, edge.DstID, edge.ID), []byte(edge.ID), "write out endpoint"); err != nil {
		return err
	}
	if err := t.adjustOutEndpointPairCount(edge.Tenant, edge.SrcID, edge.Type, edge.DstID, 1); err != nil {
		return err
	}
	if err := t.adjustUndirectedEndpointPairCount(edge.Tenant, edge.SrcID, edge.Type, edge.DstID, 1); err != nil {
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
	t.lockEdgeForMutation(tenant, edgeID)

	if _, err := t.get(keyspace.EdgeKey(tenant, edgeID)); err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		}
		return err
	}
	edgeType, srcID, dstID, err := t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		}
		return err
	}
	if err := t.addToCounter(keyspace.StatsEdgeTotalKey(tenant), -1); err != nil {
		return err
	}
	if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(tenant, normalizedEdgeType(edgeType)), -1); err != nil {
		return err
	}
	if err := t.delete(keyspace.EdgeKey(tenant, edgeID), "delete edge"); err != nil {
		return err
	}
	if err := t.deleteEdgePropertiesBySchema(ctx, tenant, edgeID, ""); err != nil {
		return err
	}
	if err := t.delete(keyspace.EdgeTypeKey(tenant, edgeID, edgeType), "delete edge type"); err != nil {
		return err
	}
	if err := t.delete(keyspace.TypeEdgeKey(tenant, edgeType, edgeID), "delete type edge"); err != nil {
		return err
	}
	if err := t.delete(keyspace.OutAdjacencyKey(tenant, srcID, edgeType, edgeID), "delete out adjacency"); err != nil {
		return err
	}
	if err := t.delete(keyspace.InAdjacencyKey(tenant, dstID, edgeType, edgeID), "delete in adjacency"); err != nil {
		return err
	}
	if err := t.delete(keyspace.OutEndpointKey(tenant, srcID, edgeType, dstID, edgeID), "delete out endpoint"); err != nil {
		return err
	}
	if err := t.adjustOutEndpointPairCount(tenant, srcID, edgeType, dstID, -1); err != nil {
		return err
	}
	if err := t.adjustUndirectedEndpointPairCount(tenant, srcID, edgeType, dstID, -1); err != nil {
		return err
	}
	return nil
}

func (t *tx) adjustOutEndpointPairCount(tenant, srcID, edgeType, dstID string, delta int) error {
	if delta == 0 {
		return nil
	}
	key := keyspace.OutEndpointPairCountKey(tenant, srcID, edgeType, dstID)
	current, _, err := t.readCounterMaybeMissing(key)
	if err != nil {
		return err
	}
	next := current + delta
	if next <= 0 {
		if err := t.delete(key, "delete out endpoint pair count"); err != nil {
			return err
		}
		return nil
	}
	return t.set(key, []byte(strconv.Itoa(next)), "write out endpoint pair count")
}

func (t *tx) adjustUndirectedEndpointPairCount(tenant, leftID, edgeType, rightID string, delta int) error {
	if delta == 0 {
		return nil
	}
	leftID, rightID = canonicalEndpointPair(leftID, rightID)
	key := keyspace.UndirectedEndpointPairCountKey(tenant, leftID, edgeType, rightID)
	current, _, err := t.readCounterMaybeMissing(key)
	if err != nil {
		return err
	}
	next := current + delta
	if next <= 0 {
		if err := t.delete(key, "delete undirected endpoint pair count"); err != nil {
			return err
		}
		return nil
	}
	return t.set(key, []byte(strconv.Itoa(next)), "write undirected endpoint pair count")
}

func canonicalEndpointPair(leftID, rightID string) (string, string) {
	if strings.Compare(leftID, rightID) <= 0 {
		return leftID, rightID
	}
	return rightID, leftID
}

func (t *tx) ScanOutEdges(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(*graph.Edge) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edges", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}
	return t.scanAdjacency(ctx, keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType), limit, tenant, fn)
}

func (t *tx) ScanOutEdgeLinks(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(edgeID, dstID string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_links", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}

	prefix := keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create out edge link iterator", err)
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
		dstID := ""
		if value := string(iter.Value()); strings.HasPrefix(value, outAdjDstValuePrefix) {
			dstID = strings.TrimPrefix(value, outAdjDstValuePrefix)
		}
		if dstID == "" {
			var err error
			dstID, err = t.getEdgeLink(ctx, tenant, edgeID)
			if err != nil {
				return err
			}
		}
		if err := fn(edgeID, dstID); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan out edge links", err)
	}
	return nil
}

func (t *tx) ScanOutEdgeLinksByType(ctx context.Context, tenant, edgeType string, limit int, fn func(srcID, edgeID, dstID string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_links_by_type", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || edgeType == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, edge type, and callback are required", nil)
	}

	prefix := keyspace.TypeEdgePrefix(tenant, edgeType)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create type-edge iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			return graph.NewError(graph.ErrKindStorage, "malformed type-edge key", nil)
		}
		sourceID, dstID, ok := parseEdgeEndpointsPayload(iter.Value())
		if !ok {
			edge, err := t.GetEdge(ctx, tenant, edgeID)
			if err != nil {
				return err
			}
			sourceID = edge.SrcID
			dstID = edge.DstID
		}
		if err := fn(sourceID, edgeID, dstID); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan type-edge links", err)
	}
	return nil
}

func (t *tx) ScanOutEdgeProperty(ctx context.Context, tenant, srcID, edgeType, property string, encodedValue []byte, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_property", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}

	stop := errors.New("scan out edge property limit reached")
	seen := 0
	if err := t.ScanOutEdgeLinks(ctx, tenant, srcID, edgeType, 0, func(edgeID, dstID string) error {
		value, hasValue, err := t.edgePropertyValue(ctx, tenant, edgeID, edgeType, property)
		if err != nil {
			return err
		}
		if !hasValue || !bytes.Equal(value, encodedValue) {
			return nil
		}
		entry := &graph.PropertyIndexEntry{
			Tenant:      tenant,
			Schema:      edgeType,
			Property:    property,
			Value:       append([]byte(nil), value...),
			EntityID:    edgeID,
			EntityClass: "edge",
			EdgeSrcID:   srcID,
			EdgeDstID:   dstID,
		}
		if err := fn(entry); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			return stop
		}
		return nil
	}); err != nil {
		if errors.Is(err, stop) {
			return nil
		}
		return err
	}
	return nil
}

func (t *tx) ScanOutEdgePropertyNumericRange(ctx context.Context, tenant, srcID, edgeType, property string, lower float64, lowerSet bool, lowerInclusive bool, upper float64, upperSet bool, upperInclusive bool, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_property_numeric_range", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}

	stop := errors.New("scan out edge property numeric range limit reached")
	seen := 0
	if err := t.ScanOutEdgeLinks(ctx, tenant, srcID, edgeType, 0, func(edgeID, dstID string) error {
		value, hasValue, err := t.edgePropertyValue(ctx, tenant, edgeID, edgeType, property)
		if err != nil {
			return err
		}
		if !hasValue {
			return nil
		}
		numeric, ok := parseNumericPropertyValue(value)
		if !ok || !numericValueInRange(numeric, lower, lowerSet, lowerInclusive, upper, upperSet, upperInclusive) {
			return nil
		}
		entry := &graph.PropertyIndexEntry{
			Tenant:      tenant,
			Schema:      edgeType,
			Property:    property,
			Value:       append([]byte(nil), value...),
			EntityID:    edgeID,
			EntityClass: "edge",
			EdgeSrcID:   srcID,
			EdgeDstID:   dstID,
		}
		if err := fn(entry); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			return stop
		}
		return nil
	}); err != nil {
		if errors.Is(err, stop) {
			return nil
		}
		return err
	}
	return nil
}

func (t *tx) hasTypedOutEdgeBetweenLegacy(ctx context.Context, tenant, srcID, dstID, edgeType string) (bool, error) {
	prefix := keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "create typed out-edge iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return false, err
		}
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			return false, graph.NewError(graph.ErrKindStorage, "malformed adjacency key", nil)
		}
		edgeDstID := ""
		if value := string(iter.Value()); strings.HasPrefix(value, outAdjDstValuePrefix) {
			edgeDstID = strings.TrimPrefix(value, outAdjDstValuePrefix)
		}
		if edgeDstID == "" {
			var err error
			edgeDstID, err = t.getEdgeLink(ctx, tenant, edgeID)
			if err != nil {
				return false, err
			}
		}
		if edgeDstID == dstID {
			return true, nil
		}
	}
	if err := iter.Error(); err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "scan typed out-edge", err)
	}
	return false, nil
}

func (t *tx) hasTypedOutEdgeBetween(ctx context.Context, tenant, srcID, dstID, edgeType string) (bool, error) {
	prefix := keyspace.OutEndpointPrefix(tenant, srcID, edgeType, dstID)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "create typed out-endpoint iterator", err)
	}
	defer iter.Close()

	if ok := iter.First(); ok {
		if err := checkCtx(ctx); err != nil {
			return false, err
		}
		if err := iter.Error(); err != nil {
			return false, graph.NewError(graph.ErrKindStorage, "scan typed out-endpoint", err)
		}
		return true, nil
	}
	if err := iter.Error(); err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "scan typed out-endpoint", err)
	}

	// Fallback keeps directed existence checks correct for legacy edges written
	// before out-endpoint keys were introduced.
	return t.hasTypedOutEdgeBetweenLegacy(ctx, tenant, srcID, dstID, edgeType)
}

func (t *tx) HasDirectedEdgeBetween(ctx context.Context, tenant, srcID, dstID, edgeType string) (exists bool, err error) {
	started := time.Now()
	defer func() { t.observeOperation("has_directed_edge_between", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return false, err
	}
	return t.HasDirectedEdgeBetweenFast(ctx, tenant, srcID, dstID, edgeType)
}

func (t *tx) HasDirectedEdgeBetweenFast(ctx context.Context, tenant, srcID, dstID, edgeType string) (bool, error) {
	buf, err := t.get(keyspace.OutEndpointPairCountKey(tenant, srcID, edgeType, dstID))
	if err == nil {
		// Count keys are deleted at zero, so key presence implies edge existence.
		if len(buf) > 0 {
			return true, nil
		}
		// Empty value should not occur in normal operation, but treat it as absent.
		return false, nil
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		return false, err
	}
	return t.hasTypedOutEdgeBetween(ctx, tenant, srcID, dstID, edgeType)
}

func (t *tx) HasUndirectedEdgeBetween(ctx context.Context, tenant, leftID, rightID, edgeType string) (exists bool, err error) {
	started := time.Now()
	defer func() { t.observeOperation("has_undirected_edge_between", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return false, err
	}
	return t.HasUndirectedEdgeBetweenFast(ctx, tenant, leftID, rightID, edgeType)
}

func (t *tx) HasUndirectedEdgeBetweenFast(ctx context.Context, tenant, leftID, rightID, edgeType string) (bool, error) {
	exists, err := t.HasDirectedEdgeBetweenFast(ctx, tenant, leftID, rightID, edgeType)
	if err != nil || exists {
		return exists, err
	}
	if leftID == rightID {
		return false, nil
	}
	return t.HasDirectedEdgeBetweenFast(ctx, tenant, rightID, leftID, edgeType)
}

func (t *tx) ScanOutEdgeSourceIDs(ctx context.Context, tenant, edgeType string, limit int, fn func(string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_sources", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant and callback are required", nil)
	}

	if edgeType != "" {
		prefix := keyspace.TypeEdgePrefix(tenant, edgeType)
		iter, err := t.reader.NewIter(&cpebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return graph.NewError(graph.ErrKindStorage, "create type-edge source iterator", err)
		}
		defer iter.Close()

		seen := 0
		emitted := make(map[string]struct{})
		for ok := iter.First(); ok; ok = iter.Next() {
			if err := checkCtx(ctx); err != nil {
				return err
			}
			sourceID, _, ok := parseEdgeEndpointsPayload(iter.Value())
			if !ok {
				edgeID := edgeIDFromAdjKey(iter.Key())
				if edgeID == "" {
					return graph.NewError(graph.ErrKindStorage, "malformed type-edge key", nil)
				}
				edge, err := t.GetEdge(ctx, tenant, edgeID)
				if err != nil {
					return err
				}
				sourceID = edge.SrcID
			}
			if sourceID == "" {
				continue
			}
			if _, ok := emitted[sourceID]; ok {
				continue
			}
			emitted[sourceID] = struct{}{}
			if err := fn(sourceID); err != nil {
				return err
			}
			seen++
			if limit > 0 && seen >= limit {
				break
			}
		}
		if err := iter.Error(); err != nil {
			return graph.NewError(graph.ErrKindStorage, "scan type-edge sources", err)
		}
		return nil
	}

	prefix := keyspace.OutAdjacencyTenantPrefix(tenant)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create out source iterator", err)
	}
	defer iter.Close()

	seen := 0
	lastSource := ""
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		sourceID, keyEdgeType, ok := outAdjSourceAndTypeFromKey(iter.Key())
		if !ok {
			return graph.NewError(graph.ErrKindStorage, "malformed out adjacency key", nil)
		}
		if edgeType != "" && keyEdgeType != edgeType {
			continue
		}
		if sourceID == lastSource {
			continue
		}
		lastSource = sourceID
		if err := fn(sourceID); err != nil {
			return err
		}
		seen++
		if limit > 0 && seen >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan out edge sources", err)
	}
	return nil
}

// ScanOutEdgeIDs scans outgoing adjacency keys and yields edge IDs without loading edge records.
func (t *tx) ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_out_edge_ids", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}
	return t.scanAdjacencyEdgeIDs(ctx, keyspace.OutAdjacencyPrefix(tenant, srcID, edgeType), limit, fn)
}

func (t *tx) ScanInEdges(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(*graph.Edge) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_in_edges", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
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
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
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

func (t *tx) ScanPropertyIndexAll(ctx context.Context, tenant, schema, property string, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_property_index_all", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || schema == "" || property == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, property, and callback are required", nil)
	}
	return t.scanPropertyIndex(ctx, keyspace.PropertyIndexPrefix(tenant, schema, property), tenant, schema, property, limit, fn)
}

func (t *tx) ScanPropertyIndexNumericRange(ctx context.Context, tenant, schema, property string, lower float64, lowerSet bool, lowerInclusive bool, upper float64, upperSet bool, upperInclusive bool, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_property_index_numeric_range", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || schema == "" || property == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, property, and callback are required", nil)
	}

	prefix := keyspace.PropertyIndexNumericPrefix(tenant, schema, property)
	lowerBound := prefix
	upperBound := prefixUpperBound(prefix)

	if lowerSet {
		ordered := orderedFloat64Bytes(lower)
		if lowerInclusive {
			lowerBound = keyspace.PropertyIndexNumericValuePrefix(tenant, schema, property, ordered)
		} else {
			lowerBound = keyspace.PropertyIndexNumericValueUpperBound(tenant, schema, property, ordered)
		}
	}
	if upperSet {
		ordered := orderedFloat64Bytes(upper)
		if upperInclusive {
			upperBound = keyspace.PropertyIndexNumericValueUpperBound(tenant, schema, property, ordered)
		} else {
			upperBound = keyspace.PropertyIndexNumericValuePrefix(tenant, schema, property, ordered)
		}
	}
	if len(upperBound) > 0 && bytes.Compare(lowerBound, upperBound) >= 0 {
		return nil
	}

	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create numeric property index iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		entry, err := numericPropertyIndexEntryFromKey(iter.Key(), iter.Value(), tenant, schema, property)
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
		return graph.NewError(graph.ErrKindStorage, "scan numeric property index", err)
	}
	return nil
}

func (t *tx) ScanPropertyIndexBooleanRange(ctx context.Context, tenant, schema, property string, lower bool, lowerSet bool, lowerInclusive bool, upper bool, upperSet bool, upperInclusive bool, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_property_index_boolean_range", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || schema == "" || property == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, property, and callback are required", nil)
	}

	prefix := keyspace.PropertyIndexBooleanPrefix(tenant, schema, property)
	lowerBound := prefix
	upperBound := prefixUpperBound(prefix)

	if lowerSet {
		ordered := orderedBoolBytes(lower)
		if lowerInclusive {
			lowerBound = keyspace.PropertyIndexBooleanValuePrefix(tenant, schema, property, ordered)
		} else {
			lowerBound = keyspace.PropertyIndexBooleanValueUpperBound(tenant, schema, property, ordered)
		}
	}
	if upperSet {
		ordered := orderedBoolBytes(upper)
		if upperInclusive {
			upperBound = keyspace.PropertyIndexBooleanValueUpperBound(tenant, schema, property, ordered)
		} else {
			upperBound = keyspace.PropertyIndexBooleanValuePrefix(tenant, schema, property, ordered)
		}
	}
	if len(upperBound) > 0 && bytes.Compare(lowerBound, upperBound) >= 0 {
		return nil
	}

	return t.scanPropertyIndexOrderedRange(ctx, tenant, schema, property, lowerBound, upperBound, limit, fn, booleanPropertyIndexEntryFromKey)
}

func (t *tx) ScanPropertyIndexDateTimeRange(ctx context.Context, tenant, schema, property string, lower time.Time, lowerSet bool, lowerInclusive bool, upper time.Time, upperSet bool, upperInclusive bool, limit int, fn func(*graph.PropertyIndexEntry) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_property_index_datetime_range", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if tenant == "" || schema == "" || property == "" || fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, property, and callback are required", nil)
	}

	prefix := keyspace.PropertyIndexDateTimePrefix(tenant, schema, property)
	lowerBound := prefix
	upperBound := prefixUpperBound(prefix)

	if lowerSet {
		ordered := orderedTimeBytes(lower.UTC())
		if lowerInclusive {
			lowerBound = keyspace.PropertyIndexDateTimeValuePrefix(tenant, schema, property, ordered)
		} else {
			lowerBound = keyspace.PropertyIndexDateTimeValueUpperBound(tenant, schema, property, ordered)
		}
	}
	if upperSet {
		ordered := orderedTimeBytes(upper.UTC())
		if upperInclusive {
			upperBound = keyspace.PropertyIndexDateTimeValueUpperBound(tenant, schema, property, ordered)
		} else {
			upperBound = keyspace.PropertyIndexDateTimeValuePrefix(tenant, schema, property, ordered)
		}
	}
	if len(upperBound) > 0 && bytes.Compare(lowerBound, upperBound) >= 0 {
		return nil
	}

	return t.scanPropertyIndexOrderedRange(ctx, tenant, schema, property, lowerBound, upperBound, limit, fn, datetimePropertyIndexEntryFromKey)
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
	if err := t.set(key, propertyIndexPayload(entry), "write property index"); err != nil {
		return err
	}
	if strings.EqualFold(entry.EntityClass, "vertex") {
		if err := t.set(keyspace.VertexPropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, entry.Value), []byte(entry.EntityID), "write vertex property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyVertexKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID), []byte(entry.EntityID), "write property vertex reverse index"); err != nil {
			return err
		}
	}
	if strings.EqualFold(entry.EntityClass, "edge") {
		if err := t.set(keyspace.EdgePropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, entry.Value), []byte(entry.EntityID), "write edge property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyEdgeKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID), []byte(entry.EntityID), "write property edge reverse index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := numericOrderedValueFromEncoded(entry.Value); ok {
		numericKey := keyspace.PropertyIndexNumericKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(numericKey, numericPropertyIndexPayload(entry), "write numeric property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := booleanOrderedValueFromEncoded(entry.Value); ok {
		booleanKey := keyspace.PropertyIndexBooleanKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(booleanKey, numericPropertyIndexPayload(entry), "write boolean property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := datetimeOrderedValueFromEncoded(entry.Value); ok {
		datetimeKey := keyspace.PropertyIndexDateTimeKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(datetimeKey, numericPropertyIndexPayload(entry), "write datetime property index"); err != nil {
			return err
		}
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
	if strings.EqualFold(entry.EntityClass, "vertex") {
		if err := t.delete(keyspace.VertexPropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, entry.Value), "delete vertex property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyVertexKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID), "delete property vertex reverse index"); err != nil {
			return err
		}
	}
	if strings.EqualFold(entry.EntityClass, "edge") {
		if err := t.delete(keyspace.EdgePropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, entry.Value), "delete edge property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyEdgeKey(entry.Tenant, entry.Schema, entry.Property, entry.Value, entry.EntityID), "delete property edge reverse index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := numericOrderedValueFromEncoded(entry.Value); ok {
		numericKey := keyspace.PropertyIndexNumericKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(numericKey, "delete numeric property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := booleanOrderedValueFromEncoded(entry.Value); ok {
		booleanKey := keyspace.PropertyIndexBooleanKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(booleanKey, "delete boolean property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := datetimeOrderedValueFromEncoded(entry.Value); ok {
		datetimeKey := keyspace.PropertyIndexDateTimeKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(datetimeKey, "delete datetime property index"); err != nil {
			return err
		}
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
	parsed, parseErr := strconv.ParseInt(string(buf), 10, 64)
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
		suffix := string(key[len(prefix):])
		if suffix == "" {
			continue
		}
		value, parseErr := strconv.ParseInt(string(iter.Value()), 10, 64)
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
			tenant := parts[1]
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
	if tenant == "" {
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
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			return graph.NewError(graph.ErrKindStorage, "malformed edge key", nil)
		}
		edgeType, _, _, err := t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
		if err != nil {
			return err
		}
		edgeTotal++
		edgeCounts[normalizedEdgeType(edgeType)]++
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
	parsed, parseErr := strconv.ParseInt(string(buf), 10, 64)
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

func outAdjSourceAndTypeFromKey(key []byte) (sourceID string, edgeType string, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 5 {
		return "", "", false
	}
	source := parts[len(parts)-3]
	typ := parts[len(parts)-2]
	if len(source) == 0 || len(typ) == 0 {
		return "", "", false
	}
	return string(source), string(typ), true
}

func edgeEndpointsPayload(srcID, dstID string) []byte {
	payload := make([]byte, 0, len(srcID)+len(dstID)+1)
	payload = append(payload, srcID...)
	payload = append(payload, 0)
	payload = append(payload, dstID...)
	return payload
}

func parseEdgeEndpointsPayload(payload []byte) (srcID, dstID string, ok bool) {
	parts := bytes.Split(payload, []byte{0})
	if len(parts) != 2 {
		return "", "", false
	}
	if len(parts[0]) == 0 || len(parts[1]) == 0 {
		return "", "", false
	}
	return string(parts[0]), string(parts[1]), true
}

func vertexIDFromKey(key []byte) string {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 3 {
		return ""
	}
	return string(parts[len(parts)-1])
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
	entityClass, edgeSrcID, edgeDstID, err := parsePropertyIndexPayload(value)
	if err != nil {
		return nil, err
	}
	return &graph.PropertyIndexEntry{
		Tenant:      tenant,
		Schema:      schema,
		Property:    property,
		Value:       decodedValue,
		EntityID:    entityID,
		EntityClass: entityClass,
		EdgeSrcID:   edgeSrcID,
		EdgeDstID:   edgeDstID,
	}, nil
}

func numericPropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index key", nil)
	}
	entityID := string(parts[len(parts)-1])
	entityClass, edgeSrcID, edgeDstID, rawValue, err := parseNumericPropertyIndexPayload(payload)
	if err != nil {
		return nil, err
	}
	return &graph.PropertyIndexEntry{
		Tenant:      tenant,
		Schema:      schema,
		Property:    property,
		Value:       rawValue,
		EntityID:    entityID,
		EntityClass: entityClass,
		EdgeSrcID:   edgeSrcID,
		EdgeDstID:   edgeDstID,
	}, nil
}

func edgePropertyPartsFromKey(key []byte) (schema, property string, encodedValue []byte, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return "", "", nil, false
	}
	schema = string(parts[len(parts)-3])
	property = string(parts[len(parts)-2])
	valueHex := string(parts[len(parts)-1])
	if schema == "" || property == "" {
		return "", "", nil, false
	}
	decodedValue, err := hex.DecodeString(valueHex)
	if err != nil {
		return "", "", nil, false
	}
	return schema, property, decodedValue, true
}

func (t *tx) edgePropertyValue(ctx context.Context, tenant, edgeID, schema, property string) ([]byte, bool, error) {
	prefix := keyspace.EdgePropertyPrefix(tenant, edgeID, schema, property)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, false, graph.NewError(graph.ErrKindStorage, "create edge property iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, false, err
		}
		keySchema, keyProperty, encodedValue, ok := edgePropertyPartsFromKey(iter.Key())
		if !ok {
			return nil, false, graph.NewError(graph.ErrKindStorage, "malformed edge property key", nil)
		}
		if keySchema != schema || keyProperty != property {
			continue
		}
		return encodedValue, true, nil
	}
	if err := iter.Error(); err != nil {
		return nil, false, graph.NewError(graph.ErrKindStorage, "scan edge properties", err)
	}
	return nil, false, nil
}

func parseNumericPropertyValue(raw []byte) (float64, bool) {
	text := string(raw)
	if text == "" {
		return 0, false
	}
	numeric, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(numeric) {
		return 0, false
	}
	return numeric, true
}

func numericValueInRange(value, lower float64, lowerSet, lowerInclusive bool, upper float64, upperSet, upperInclusive bool) bool {
	if lowerSet {
		if lowerInclusive {
			if value < lower {
				return false
			}
		} else if value <= lower {
			return false
		}
	}
	if upperSet {
		if upperInclusive {
			if value > upper {
				return false
			}
		} else if value >= upper {
			return false
		}
	}
	return true
}

func propertyIndexPayload(entry *graph.PropertyIndexEntry) []byte {
	if entry == nil {
		return nil
	}
	if entry.EdgeSrcID == "" && entry.EdgeDstID == "" {
		return []byte(entry.EntityClass)
	}
	payload := make([]byte, 0, len(entry.EntityClass)+len(entry.EdgeSrcID)+len(entry.EdgeDstID)+3)
	payload = append(payload, []byte(entry.EntityClass)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(entry.EdgeSrcID)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(entry.EdgeDstID)...)
	return payload
}

func numericPropertyIndexPayload(entry *graph.PropertyIndexEntry) []byte {
	if entry == nil {
		return nil
	}
	prefix := propertyIndexPayload(entry)
	payload := make([]byte, 0, len(prefix)+1+len(entry.Value))
	payload = append(payload, prefix...)
	payload = append(payload, 0)
	payload = append(payload, entry.Value...)
	return payload
}

func parsePropertyIndexPayload(payload []byte) (string, string, string, error) {
	if len(payload) == 0 {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "malformed property index payload", nil)
	}
	parts := bytes.Split(payload, []byte{0})
	entityClass := ""
	edgeSrcID := ""
	edgeDstID := ""
	if len(parts) > 0 {
		entityClass = string(parts[0])
	}
	if len(parts) > 1 {
		edgeSrcID = string(parts[1])
	}
	if len(parts) > 2 {
		edgeDstID = string(parts[2])
	}
	if entityClass == "" {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "property index payload missing entity class", nil)
	}
	return entityClass, edgeSrcID, edgeDstID, nil
}

func parseNumericPropertyIndexPayload(payload []byte) (string, string, string, []byte, error) {
	if len(payload) == 0 {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index payload", nil)
	}

	parts := bytes.Split(payload, []byte{0})
	if len(parts) >= 4 {
		entityClass := string(parts[0])
		edgeSrcID := string(parts[1])
		edgeDstID := string(parts[2])
		rawValue := append([]byte(nil), parts[3]...)
		if entityClass == "" {
			return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "numeric property index payload missing entity class", nil)
		}
		return entityClass, edgeSrcID, edgeDstID, rawValue, nil
	}

	sep := bytes.IndexByte(payload, 0)
	if sep < 0 {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index payload", nil)
	}
	entityClass := string(payload[:sep])
	rawValue := append([]byte(nil), payload[sep+1:]...)
	if entityClass == "" {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "numeric property index payload missing entity class", nil)
	}
	return entityClass, "", "", rawValue, nil
}

func numericOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	text := string(raw)
	if text == "" {
		return nil, false
	}
	numeric, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(numeric) {
		return nil, false
	}
	return orderedFloat64Bytes(numeric), true
}

func booleanOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	text := string(raw)
	switch text {
	case "false":
		return orderedBoolBytes(false), true
	case "true":
		return orderedBoolBytes(true), true
	default:
		return nil, false
	}
}

func datetimeOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	temporal, ok := parseStoredMapString(string(raw))
	if !ok {
		return nil, false
	}
	if !strings.EqualFold(fmt.Sprint(temporal["__temporal_type"]), "datetime") {
		return nil, false
	}
	t, ok := storedTemporalDateTime(temporal)
	if !ok {
		return nil, false
	}
	return orderedTimeBytes(t.UTC()), true
}

func orderedBoolBytes(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func orderedInt64Bytes(v int64) []byte {
	bits := uint64(v) ^ (uint64(1) << 63)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, bits)
	return out
}

func orderedTimeBytes(t time.Time) []byte {
	return orderedInt64Bytes(t.UnixNano())
}

func (t *tx) scanPropertyIndexOrderedRange(ctx context.Context, tenant, schema, property string, lowerBound, upperBound []byte, limit int, fn func(*graph.PropertyIndexEntry) error, decode func([]byte, []byte, string, string, string) (*graph.PropertyIndexEntry, error)) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create property index iterator", err)
	}
	defer iter.Close()

	seen := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		entry, err := decode(iter.Key(), iter.Value(), tenant, schema, property)
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

func booleanPropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	return numericPropertyIndexEntryFromKey(key, payload, tenant, schema, property)
}

func datetimePropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	return numericPropertyIndexEntryFromKey(key, payload, tenant, schema, property)
}

func parseStoredMapString(raw string) (map[string]any, bool) {
	if !strings.HasPrefix(raw, "map[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := raw[len("map[") : len(raw)-1]
	if body == "" {
		return map[string]any{}, true
	}
	out := map[string]any{}
	for _, part := range strings.Fields(body) {
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			continue
		}
		out[pair[0]] = pair[1]
	}
	return out, true
}

func storedTemporalDateTime(src map[string]any) (time.Time, bool) {
	year, ok := storedMapInt(src, "year")
	if !ok {
		return time.Time{}, false
	}
	month, ok := storedMapInt(src, "month")
	if !ok {
		month = 1
	}
	day, ok := storedMapInt(src, "day")
	if !ok {
		day = 1
	}
	hour, _ := storedMapInt(src, "hour")
	minute, _ := storedMapInt(src, "minute")
	second, _ := storedMapInt(src, "second")
	nanosecond, _ := storedMapInt(src, "nanosecond")

	loc := time.UTC
	if tzRaw, ok := src["timezone"]; ok {
		tz := fmt.Sprint(tzRaw)
		if tz != "" {
			if offset, err := parseOffsetSeconds(tz); err == nil {
				loc = time.FixedZone("", offset)
			} else if loaded, err := time.LoadLocation(tz); err == nil {
				loc = loaded
			}
		}
	}

	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, loc), true
}

func storedMapInt(value map[string]any, key string) (int, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	switch typed := raw.(type) {
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func parseOffsetSeconds(raw string) (int, error) {
	if raw == "" || strings.EqualFold(raw, "Z") {
		return 0, nil
	}
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		sign := 1
		if raw[0] == '-' {
			sign = -1
		}
		parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(raw, "+"), "-"), ":")
		if len(parts) != 2 && len(parts) != 3 {
			return 0, fmt.Errorf("invalid offset %q", raw)
		}
		hours, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, err
		}
		minutes, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
		seconds := 0
		if len(parts) == 3 {
			seconds, err = strconv.Atoi(parts[2])
			if err != nil {
				return 0, err
			}
		}
		return sign * (hours*3600 + minutes*60 + seconds), nil
	}
	return 0, fmt.Errorf("invalid offset %q", raw)
}

func orderedFloat64Bytes(v float64) []byte {
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, bits)
	return out
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
