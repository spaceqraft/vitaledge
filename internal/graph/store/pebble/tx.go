package pebblestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/spaceqraft/vitaledge/internal/graph"
	"github.com/spaceqraft/vitaledge/internal/graph/keyspace"
)

const outAdjDstValuePrefix = "d:"
const vertexPropertySchema = "__vertex__"

var _ graph.Tx = (*tx)(nil)

type tx struct {
	store                *Store
	mode                 graph.TxMode
	reader               kvReader
	writer               kvWriter
	snapshot             *cpebble.Snapshot
	batch                *cpebble.Batch
	locks                map[string]func()
	edgeLocks            sync.Map
	vertexCache          map[string]*graph.Vertex
	edgeMetaCache        map[string]edgeMeta
	counterBase          map[string]int
	counterBasePresent   map[string]bool
	counterDeltas        map[string]int
	pendingPropertyStats map[string]propertyStatsTarget
	pendingTenantStats   map[string]struct{}
	closed               bool
	maxWriteBatchBytes   int
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

func (t *tx) PutVertexNew(ctx context.Context, vertex *graph.Vertex) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_vertex_new", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if vertex == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "vertex is required", nil)
	}
	if err := t.addToCounter(keyspace.StatsVertexTotalKey(vertex.Tenant), 1); err != nil {
		return err
	}
	nextLabelList := normalizedLabelsOrdered(vertex.Labels)
	for idx, label := range nextLabelList {
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
	if err := t.writeVertexProperties(vertex); err != nil {
		return err
	}
	nextHash := vertexPHash(vertex)
	if err := t.set(keyspace.VertexKey(vertex.Tenant, vertex.ID), nextHash, "write vertex"); err != nil {
		return err
	}
	t.cacheVertex(vertex)
	t.queueTenantStatsRefresh(vertex.Tenant)
	return nil
}

func (t *tx) PutVertexBatch(ctx context.Context, vertexes []*graph.Vertex) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_vertex_batch", err, started) }()
	return t.putVertexesByMeta(ctx, vertexes)
}

type vertexPutMeta struct {
	vertex         *graph.Vertex
	nextHash       []byte
	previousExists bool
	previousLabels []string
	nextLabels     []string
}

type vertexLabelCounterKey struct {
	tenant string
	label  string
}

func (t *tx) putVertexesByMeta(ctx context.Context, vertexes []*graph.Vertex) error {
	if err := t.ensureWrite(ctx); err != nil {
		return err
	}

	metas := make([]vertexPutMeta, 0, len(vertexes))
	tenantVertexDeltas := map[string]int{}
	labelDeltas := map[vertexLabelCounterKey]int{}

	for _, vertex := range vertexes {
		if vertex == nil {
			continue
		}

		existingHash, err := t.get(keyspace.VertexKey(vertex.Tenant, vertex.ID))
		previousExists := err == nil
		if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}

		nextHash := vertexPHash(vertex)
		if previousExists && bytes.Equal(existingHash, nextHash) {
			continue
		}

		previousLabels := normalizedLabelsOrdered(nil)
		if previousExists {
			previousLabels, err = t.loadVertexLabels(ctx, vertex.Tenant, vertex.ID)
			if err != nil {
				return err
			}
			previousLabels = normalizedLabelsOrdered(previousLabels)
		} else {
			tenantVertexDeltas[vertex.Tenant]++
		}

		nextLabels := normalizedLabelsOrdered(vertex.Labels)
		previousLabelSet := labelSliceSet(previousLabels)
		nextLabelSet := labelSliceSet(nextLabels)
		for label := range previousLabelSet {
			if _, ok := nextLabelSet[label]; ok {
				continue
			}
			labelDeltas[vertexLabelCounterKey{tenant: vertex.Tenant, label: label}]--
		}
		for label := range nextLabelSet {
			if _, ok := previousLabelSet[label]; ok {
				continue
			}
			labelDeltas[vertexLabelCounterKey{tenant: vertex.Tenant, label: label}]++
		}

		metas = append(metas, vertexPutMeta{
			vertex:         vertex,
			nextHash:       nextHash,
			previousExists: previousExists,
			previousLabels: previousLabels,
			nextLabels:     nextLabels,
		})
	}

	for tenant, delta := range tenantVertexDeltas {
		if delta == 0 {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexTotalKey(tenant), delta); err != nil {
			return err
		}
	}
	for key, delta := range labelDeltas {
		if delta == 0 {
			continue
		}
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(key.tenant, key.label), delta); err != nil {
			return err
		}
	}

	for _, meta := range metas {
		vertex := meta.vertex
		previousLabelSet := labelSliceSet(meta.previousLabels)
		nextLabelSet := labelSliceSet(meta.nextLabels)

		for label := range previousLabelSet {
			if _, ok := nextLabelSet[label]; ok {
				continue
			}
			if err := t.delete(keyspace.VertexLabelKey(vertex.Tenant, vertex.ID, label), "delete vertex label forward index"); err != nil {
				return err
			}
			if err := t.delete(keyspace.VertexLabelMembershipKey(vertex.Tenant, label, vertex.ID), "delete vertex label membership"); err != nil {
				return err
			}
		}

		for idx, label := range meta.nextLabels {
			if _, ok := previousLabelSet[label]; ok {
				continue
			}
			if err := t.set(keyspace.VertexLabelKey(vertex.Tenant, vertex.ID, label), encodeVertexLabelOrder(idx, label), "write vertex label forward index"); err != nil {
				return err
			}
			if err := t.set(keyspace.VertexLabelMembershipKey(vertex.Tenant, label, vertex.ID), []byte(vertex.ID), "write vertex label membership"); err != nil {
				return err
			}
		}

		if meta.previousExists {
			if err := t.deleteVertexPropertiesBySchema(ctx, vertex.Tenant, vertex.ID, vertexPropertySchema); err != nil {
				return err
			}
		}
		if err := t.writeVertexProperties(vertex); err != nil {
			return err
		}
		if err := t.set(keyspace.VertexKey(vertex.Tenant, vertex.ID), meta.nextHash, "write vertex"); err != nil {
			return err
		}
		t.cacheVertex(vertex)
		t.queueTenantStatsRefresh(vertex.Tenant)
	}
	return nil
}

func (t *tx) PutEdgeBatch(ctx context.Context, edges []*graph.Edge) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_edge_batch", err, started) }()
	return t.putEdgesByMeta(ctx, edges)
}

type edgePutMeta struct {
	edge           *graph.Edge
	nextHash       []byte
	previousExists bool
	previousType   string
	previousSrcID  string
	previousDstID  string
}

func (t *tx) putEdgesByMeta(ctx context.Context, edges []*graph.Edge) error {
	if err := t.ensureWrite(ctx); err != nil {
		return err
	}

	metas := make([]edgePutMeta, 0, len(edges))
	tenantEdgeTotals := map[string]int{}
	typeDeltas := map[string]int{}
	sourceDegreeDeltas := map[string]int{}
	outPairDeltas := map[string]int{}
	undirectedPairDeltas := map[string]int{}

	for _, edge := range edges {
		if edge == nil {
			continue
		}
		t.lockEdgeForMutation(edge.Tenant, edge.ID)

		_, err := t.get(keyspace.EdgeKey(edge.Tenant, edge.ID))
		previousExists := err == nil
		if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}

		nextHash := edgePHash(edge)
		meta := edgePutMeta{edge: edge, nextHash: nextHash, previousExists: previousExists}
		if previousExists {
			meta.previousType, meta.previousSrcID, meta.previousDstID, err = t.loadEdgeTypeAndEndpoints(ctx, edge.Tenant, edge.ID)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					if cleanupErr := t.deleteEdgeKeysByIDs(ctx, edge.Tenant, []string{edge.ID}); cleanupErr != nil {
						return cleanupErr
					}
					meta.previousExists = false
				} else {
					return err
				}
			}
		}

		if !meta.previousExists {
			tenantEdgeTotals[edge.Tenant]++
			typeDeltas[edge.Tenant+"\x00"+edge.Type]++
			sourceDegreeDeltas[edge.Tenant+"\x00"+edge.Type+"\x00"+strings.TrimSpace(edge.SrcID)]++
			outPairDeltas[edge.Tenant+"\x00"+strings.TrimSpace(edge.SrcID)+"\x00"+strings.TrimSpace(edge.Type)+"\x00"+strings.TrimSpace(edge.DstID)]++
			leftID, rightID := canonicalEndpointPair(strings.TrimSpace(edge.SrcID), strings.TrimSpace(edge.DstID))
			undirectedPairDeltas[edge.Tenant+"\x00"+leftID+"\x00"+strings.TrimSpace(edge.Type)+"\x00"+rightID]++
		} else {
			oldType := meta.previousType
			newType := edge.Type
			if oldType != newType {
				typeDeltas[edge.Tenant+"\x00"+oldType]--
				typeDeltas[edge.Tenant+"\x00"+newType]++
			}
			if oldType != newType || meta.previousSrcID != edge.SrcID {
				sourceDegreeDeltas[edge.Tenant+"\x00"+oldType+"\x00"+strings.TrimSpace(meta.previousSrcID)]--
				sourceDegreeDeltas[edge.Tenant+"\x00"+newType+"\x00"+strings.TrimSpace(edge.SrcID)]++
			}
			if oldType != newType || meta.previousSrcID != edge.SrcID || meta.previousDstID != edge.DstID {
				outPairDeltas[edge.Tenant+"\x00"+strings.TrimSpace(meta.previousSrcID)+"\x00"+strings.TrimSpace(oldType)+"\x00"+strings.TrimSpace(meta.previousDstID)]--
				outPairDeltas[edge.Tenant+"\x00"+strings.TrimSpace(edge.SrcID)+"\x00"+strings.TrimSpace(newType)+"\x00"+strings.TrimSpace(edge.DstID)]++
				oldLeftID, oldRightID := canonicalEndpointPair(strings.TrimSpace(meta.previousSrcID), strings.TrimSpace(meta.previousDstID))
				undirectedPairDeltas[edge.Tenant+"\x00"+oldLeftID+"\x00"+strings.TrimSpace(oldType)+"\x00"+oldRightID]--
				newLeftID, newRightID := canonicalEndpointPair(strings.TrimSpace(edge.SrcID), strings.TrimSpace(edge.DstID))
				undirectedPairDeltas[edge.Tenant+"\x00"+newLeftID+"\x00"+strings.TrimSpace(newType)+"\x00"+newRightID]++
			}
		}

		metas = append(metas, meta)
	}

	for tenant, delta := range tenantEdgeTotals {
		if delta == 0 {
			continue
		}
		if err := t.addToCounter(keyspace.StatsEdgeTotalKey(tenant), delta); err != nil {
			return err
		}
	}
	for key, delta := range typeDeltas {
		if delta == 0 {
			continue
		}
		parts := strings.Split(key, "\x00")
		if len(parts) != 2 {
			continue
		}
		if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(parts[0], parts[1]), delta); err != nil {
			return err
		}
	}
	for key, delta := range sourceDegreeDeltas {
		if delta == 0 {
			continue
		}
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 {
			continue
		}
		if err := t.adjustEdgeTypeSourceDegree(parts[0], parts[1], parts[2], delta); err != nil {
			return err
		}
	}
	for key, delta := range outPairDeltas {
		if delta == 0 {
			continue
		}
		parts := strings.Split(key, "\x00")
		if len(parts) != 4 {
			continue
		}
		if err := t.adjustOutEndpointPairCount(parts[0], parts[1], parts[2], parts[3], delta); err != nil {
			return err
		}
	}
	for key, delta := range undirectedPairDeltas {
		if delta == 0 {
			continue
		}
		parts := strings.Split(key, "\x00")
		if len(parts) != 4 {
			continue
		}
		if err := t.adjustUndirectedEndpointPairCount(parts[0], parts[1], parts[2], parts[3], delta); err != nil {
			return err
		}
	}

	for _, meta := range metas {
		edge := meta.edge
		if meta.previousExists {
			if err := t.delete(keyspace.EdgeTypeKey(edge.Tenant, edge.ID, meta.previousType), "delete stale edge type"); err != nil {
				return err
			}
			if err := t.delete(keyspace.TypeEdgeKey(edge.Tenant, meta.previousType, edge.ID), "delete stale type edge"); err != nil {
				return err
			}
			if err := t.delete(keyspace.OutAdjacencyKey(edge.Tenant, meta.previousSrcID, meta.previousType, edge.ID), "delete stale out adjacency"); err != nil {
				return err
			}
			if err := t.delete(keyspace.InAdjacencyKey(edge.Tenant, meta.previousDstID, meta.previousType, edge.ID), "delete stale in adjacency"); err != nil {
				return err
			}
			if err := t.delete(keyspace.OutEndpointKey(edge.Tenant, meta.previousSrcID, meta.previousType, meta.previousDstID, edge.ID), "delete stale out endpoint"); err != nil {
				return err
			}
			if err := t.deleteEdgeKeysByIDs(ctx, edge.Tenant, []string{edge.ID}); err != nil {
				return err
			}
			if err := t.deleteEdgePropertiesBySchema(ctx, edge.Tenant, edge.ID, ""); err != nil {
				return err
			}
		}

		if err := t.writeEdgeProperties(edge); err != nil {
			return err
		}
		if err := t.set(keyspace.EdgeKey(edge.Tenant, edge.ID), meta.nextHash, "write edge"); err != nil {
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
		t.cacheEdgeMeta(edge.Tenant, edge.ID, edge.Type, edge.SrcID, edge.DstID)
		t.queueTenantStatsRefresh(edge.Tenant)
	}
	return nil
}

type edgeDeleteMeta struct {
	edgeID   string
	edgeType string
	srcID    string
	dstID    string
}

func (t *tx) PutEdgeNewBatch(ctx context.Context, edges []*graph.Edge) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_edge_new_batch", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	tenantEdgeTotals := map[string]int{}
	typeTotals := map[string]int{}
	typeSourceDeltas := map[string]int{}
	outPairDeltas := map[string]int{}
	undirectedPairDeltas := map[string]int{}
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		t.lockEdgeForMutation(edge.Tenant, edge.ID)
		if err := t.writeEdgeProperties(edge); err != nil {
			return err
		}
		if err := t.set(keyspace.EdgeKey(edge.Tenant, edge.ID), edgePHash(edge), "write edge"); err != nil {
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
		tenantEdgeTotals[edge.Tenant]++
		typeTotals[edge.Tenant+"\x00"+edge.Type]++
		typeSourceDeltas[edge.Tenant+"\x00"+edge.Type+"\x00"+strings.TrimSpace(edge.SrcID)]++
		outPairDeltas[edge.Tenant+"\x00"+strings.TrimSpace(edge.SrcID)+"\x00"+strings.TrimSpace(edge.Type)+"\x00"+strings.TrimSpace(edge.DstID)]++
		leftID, rightID := canonicalEndpointPair(strings.TrimSpace(edge.SrcID), strings.TrimSpace(edge.DstID))
		undirectedPairDeltas[edge.Tenant+"\x00"+leftID+"\x00"+strings.TrimSpace(edge.Type)+"\x00"+rightID]++
		t.queueTenantStatsRefresh(edge.Tenant)
	}
	for tenant, delta := range tenantEdgeTotals {
		if err := t.addToCounter(keyspace.StatsEdgeTotalKey(tenant), delta); err != nil {
			return err
		}
	}
	for key, delta := range typeTotals {
		parts := strings.Split(key, "\x00")
		if len(parts) != 2 {
			continue
		}
		if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(parts[0], parts[1]), delta); err != nil {
			return err
		}
	}
	for key, delta := range typeSourceDeltas {
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 {
			continue
		}
		if err := t.adjustEdgeTypeSourceDegree(parts[0], parts[1], parts[2], delta); err != nil {
			return err
		}
	}
	for key, delta := range outPairDeltas {
		parts := strings.Split(key, "\x00")
		if len(parts) != 4 {
			continue
		}
		if err := t.adjustOutEndpointPairCount(parts[0], parts[1], parts[2], parts[3], delta); err != nil {
			return err
		}
	}
	for key, delta := range undirectedPairDeltas {
		parts := strings.Split(key, "\x00")
		if len(parts) != 4 {
			continue
		}
		if err := t.adjustUndirectedEndpointPairCount(parts[0], parts[1], parts[2], parts[3], delta); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) DeleteVertexBatch(ctx context.Context, tenant string, vertexIDs []string) (err error) {
	return t.deleteVertexBatch(ctx, false, tenant, vertexIDs)
}

func (t *tx) DeleteVertexDetachBatch(ctx context.Context, tenant string, vertexIDs []string) (err error) {
	return t.deleteVertexBatch(ctx, true, tenant, vertexIDs)
}

func (t *tx) deleteVertexBatch(ctx context.Context, detach bool, tenant string, vertexIDs []string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_vertex_detach_batch", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if tenant == "" || len(vertexIDs) == 0 {
		return nil
	}

	uniqueVertexIDs := make([]string, 0, len(vertexIDs))
	seenVertexIDs := make(map[string]struct{}, len(vertexIDs))
	for _, vertexID := range vertexIDs {
		vertexID = strings.TrimSpace(vertexID)
		if vertexID == "" {
			continue
		}
		if _, ok := seenVertexIDs[vertexID]; ok {
			continue
		}
		seenVertexIDs[vertexID] = struct{}{}
		uniqueVertexIDs = append(uniqueVertexIDs, vertexID)
	}
	if len(uniqueVertexIDs) == 0 {
		return nil
	}
	sort.Strings(uniqueVertexIDs)

	if !detach {
		// batching any edge existence is likely more expensive since edge existence will be either prevalent so fail fast
		// or extremeley rare so batching won't save much.
		for _, vertexID := range uniqueVertexIDs {
			hasIncident, err := t.vertexHasIncidentEdges(ctx, tenant, vertexID)
			if err != nil {
				return err
			}
			if hasIncident {
				return graph.NewError(graph.ErrKindConflict, "DeleteConnectedNode", fmt.Errorf("vertex %s has incident edges", vertexID))
			}
		}
	}

	deletions := make([]vertexDeleteMeta, 0, len(uniqueVertexIDs))
	edgeIDSet := make(map[string]struct{})
	existingVertexSet := make(map[string]struct{}, len(uniqueVertexIDs))
	for _, vertexID := range uniqueVertexIDs {
		if _, err := t.get(keyspace.VertexKey(tenant, vertexID)); err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				continue
			}
			return err
		}
		existingVertexSet[vertexID] = struct{}{}
	}

	if detach {
		if fullPurge, err := t.isFullTenantVertexSelection(ctx, tenant, existingVertexSet); err != nil {
			return err
		} else if fullPurge {
			return t.purgeTenantDataFastPath(ctx, tenant)
		}
	}

	labelsByVertexID, err := t.loadVertexLabelsForIDs(ctx, tenant, existingVertexSet)
	if err != nil {
		return err
	}
	for _, vertexID := range uniqueVertexIDs {
		if _, exists := existingVertexSet[vertexID]; !exists {
			continue
		}
		deletions = append(deletions, vertexDeleteMeta{vertexID: vertexID, labels: labelsByVertexID[vertexID]})
	}

	if detach && len(existingVertexSet) > 0 {
		if len(existingVertexSet) >= 32 {
			edgeIDSet, err = t.collectIncidentEdgeIDsForVertexSet(ctx, tenant, existingVertexSet)
			if err != nil {
				return err
			}
		} else {
			for vertexID := range existingVertexSet {
				incidentEdgeIDs, err := t.vertexIncidentEdgeIDs(ctx, tenant, vertexID)
				if err != nil {
					return err
				}
				for edgeID := range incidentEdgeIDs {
					edgeID = strings.TrimSpace(edgeID)
					if edgeID == "" {
						continue
					}
					edgeIDSet[edgeID] = struct{}{}
				}
			}
		}
	}

	if len(edgeIDSet) > 0 {
		edgeIDs := make([]string, 0, len(edgeIDSet))
		for edgeID := range edgeIDSet {
			edgeIDs = append(edgeIDs, edgeID)
		}
		sort.Strings(edgeIDs)
		if err := t.DeleteEdgeBatch(ctx, tenant, edgeIDs); err != nil {
			return err
		}
	}

	return t.deleteVerticesByMeta(ctx, tenant, deletions)
}

type vertexDeleteMeta struct {
	vertexID string
	labels   []string
}

func (t *tx) deleteVerticesByMeta(ctx context.Context, tenant string, deletions []vertexDeleteMeta) error {
	if len(deletions) == 0 {
		return nil
	}
	if err := t.addToCounter(keyspace.StatsVertexTotalKey(tenant), -len(deletions)); err != nil {
		return err
	}
	labelDeltas := map[string]int{}
	for _, deletion := range deletions {
		for _, label := range normalizedLabelsOrdered(deletion.labels) {
			labelDeltas[label]--
		}
	}
	for label, delta := range labelDeltas {
		if err := t.addToCounter(keyspace.StatsVertexLabelCountKey(tenant, label), delta); err != nil {
			return err
		}
	}
	for _, deletion := range deletions {
		if err := t.delete(keyspace.VertexKey(tenant, deletion.vertexID), "delete vertex"); err != nil {
			return err
		}
		t.dropVertexCache(tenant, deletion.vertexID)
		if err := t.deleteVertexPropertiesBySchema(ctx, tenant, deletion.vertexID, ""); err != nil {
			return err
		}
		for _, label := range normalizedLabelsOrdered(deletion.labels) {
			if err := t.delete(keyspace.VertexLabelKey(tenant, deletion.vertexID, label), "delete vertex label forward index"); err != nil {
				return err
			}
			if err := t.delete(keyspace.VertexLabelMembershipKey(tenant, label, deletion.vertexID), "delete vertex label membership"); err != nil {
				return err
			}
		}
	}
	t.queueTenantStatsRefresh(tenant)
	return nil
}

func (t *tx) tenantEdgeTypes(ctx context.Context, tenant string) []string {
	snapshot, err := t.GetStatsSnapshot(ctx, tenant)
	if err != nil || snapshot == nil || len(snapshot.EdgeCounts) == 0 {
		return nil
	}
	types := make([]string, 0, len(snapshot.EdgeCounts))
	for edgeType := range snapshot.EdgeCounts {
		edgeType = strings.TrimSpace(edgeType)
		if edgeType == "" {
			continue
		}
		types = append(types, edgeType)
	}
	sort.Strings(types)
	return types
}

func (t *tx) vertexHasIncidentEdges(ctx context.Context, tenant, vertexID string) (bool, error) {
	types := t.tenantEdgeTypes(ctx, tenant)
	if len(types) == 0 {
		found := false
		if err := t.ScanOutEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
			if edge != nil {
				found = true
			}
			return nil
		}); err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
		if err := t.ScanInEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
			if edge != nil {
				found = true
			}
			return nil
		}); err != nil {
			return false, err
		}
		return found, nil
	}
	for _, edgeType := range types {
		found := false
		if err := t.ScanOutEdges(ctx, tenant, vertexID, edgeType, 1, func(edge *graph.Edge) error {
			if edge != nil {
				found = true
			}
			return nil
		}); err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
		if err := t.ScanInEdges(ctx, tenant, vertexID, edgeType, 1, func(edge *graph.Edge) error {
			if edge != nil {
				found = true
			}
			return nil
		}); err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

func (t *tx) vertexIncidentEdgeIDs(ctx context.Context, tenant, vertexID string) (map[string]struct{}, error) {
	edgeIDs := map[string]struct{}{}
	collect := func(edgeType string) error {
		if err := t.ScanOutEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge != nil && edge.ID != "" {
				edgeIDs[edge.ID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
		if err := t.ScanInEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge != nil && edge.ID != "" {
				edgeIDs[edge.ID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}
	types := t.tenantEdgeTypes(ctx, tenant)
	if len(types) == 0 {
		if err := collect(""); err != nil {
			return nil, err
		}
		return edgeIDs, nil
	}
	for _, edgeType := range types {
		if err := collect(edgeType); err != nil {
			return nil, err
		}
	}
	return edgeIDs, nil
}

func (t *tx) collectIncidentEdgeIDsForVertexSet(ctx context.Context, tenant string, vertexIDs map[string]struct{}) (map[string]struct{}, error) {
	edgeIDs := map[string]struct{}{}
	if tenant == "" || len(vertexIDs) == 0 {
		return edgeIDs, nil
	}
	prefix := keyspace.OutAdjacencyTenantPrefix(tenant)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create out adjacency iterator for batch vertex detach", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		srcID, _, ok := outAdjSourceAndTypeFromKey(iter.Key())
		if !ok {
			continue
		}
		_, srcSelected := vertexIDs[srcID]
		dstSelected := false
		if value := string(iter.Value()); strings.HasPrefix(value, outAdjDstValuePrefix) {
			dstID := strings.TrimPrefix(value, outAdjDstValuePrefix)
			_, dstSelected = vertexIDs[dstID]
		}
		if !srcSelected && !dstSelected {
			continue
		}
		edgeID := edgeIDFromAdjKey(iter.Key())
		if edgeID == "" {
			continue
		}
		edgeIDs[edgeID] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan out adjacency for batch vertex detach", err)
	}
	return edgeIDs, nil
}

func (t *tx) loadVertexLabelsForIDs(ctx context.Context, tenant string, vertexIDs map[string]struct{}) (map[string][]string, error) {
	out := make(map[string][]string, len(vertexIDs))
	if tenant == "" || len(vertexIDs) == 0 {
		return out, nil
	}
	prefix := []byte("vl/" + tenant + "/")
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create vertex label iterator for batch delete", err)
	}
	defer iter.Close()

	labelSetByVertex := map[string]map[string]struct{}{}
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, err
		}
		parts := bytes.Split(iter.Key(), []byte{'/'})
		if len(parts) < 4 {
			continue
		}
		vertexID := string(parts[len(parts)-2])
		if _, selected := vertexIDs[vertexID]; !selected {
			continue
		}
		label := strings.TrimSpace(string(parts[len(parts)-1]))
		if label == "" {
			continue
		}
		if labelSetByVertex[vertexID] == nil {
			labelSetByVertex[vertexID] = map[string]struct{}{}
		}
		labelSetByVertex[vertexID][label] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan vertex labels for batch delete", err)
	}

	for vertexID := range vertexIDs {
		labelSet := labelSetByVertex[vertexID]
		if len(labelSet) == 0 {
			out[vertexID] = nil
			continue
		}
		labels := make([]string, 0, len(labelSet))
		for label := range labelSet {
			labels = append(labels, label)
		}
		out[vertexID] = normalizedLabelsOrdered(labels)
	}
	return out, nil
}

func (t *tx) isFullTenantVertexSelection(ctx context.Context, tenant string, selectedVertexIDs map[string]struct{}) (bool, error) {
	if tenant == "" || len(selectedVertexIDs) == 0 {
		return false, nil
	}
	vertexTotal, found, err := t.readCounterMaybeMissing(keyspace.StatsVertexTotalKey(tenant))
	if err != nil {
		return false, err
	}
	if found {
		return vertexTotal == len(selectedVertexIDs), nil
	}

	count := 0
	prefix := keyspace.VertexPrefix(tenant)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "create vertex iterator for full-tenant check", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return false, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return false, graph.NewError(graph.ErrKindStorage, "scan vertices for full-tenant check", err)
	}
	return count == len(selectedVertexIDs), nil
}

func (t *tx) purgeTenantDataFastPath(ctx context.Context, tenant string) error {
	if tenant == "" {
		return nil
	}
	prefixes := []struct {
		prefix []byte
		action string
	}{
		{prefix: keyspace.EdgePrefix(tenant), action: "purge tenant edges"},
		{prefix: []byte("et/" + tenant + "/"), action: "purge tenant edge types"},
		{prefix: []byte("te/" + tenant + "/"), action: "purge tenant type edges"},
		{prefix: keyspace.OutAdjacencyTenantPrefix(tenant), action: "purge tenant out adjacency"},
		{prefix: keyspace.InAdjacencyTenantPrefix(tenant), action: "purge tenant in adjacency"},
		{prefix: []byte("od/" + tenant + "/"), action: "purge tenant out endpoints"},
		{prefix: []byte("odc/" + tenant + "/"), action: "purge tenant out endpoint pair counts"},
		{prefix: []byte("udc/" + tenant + "/"), action: "purge tenant undirected endpoint pair counts"},
		{prefix: []byte("ep/" + tenant + "/"), action: "purge tenant edge properties"},
		{prefix: []byte("pe/" + tenant + "/"), action: "purge tenant edge property reverse indexes"},
		{prefix: keyspace.VertexPrefix(tenant), action: "purge tenant vertices"},
		{prefix: []byte("vl/" + tenant + "/"), action: "purge tenant vertex labels"},
		{prefix: []byte("lv/" + tenant + "/"), action: "purge tenant label memberships"},
		{prefix: []byte("vp/" + tenant + "/"), action: "purge tenant vertex properties"},
		{prefix: []byte("pv/" + tenant + "/"), action: "purge tenant vertex property reverse indexes"},
		{prefix: []byte("pi/" + tenant + "/"), action: "purge tenant property indexes"},
		{prefix: []byte("pn/" + tenant + "/"), action: "purge tenant numeric property indexes"},
		{prefix: []byte("pib/" + tenant + "/"), action: "purge tenant boolean property indexes"},
		{prefix: []byte("pit/" + tenant + "/"), action: "purge tenant datetime property indexes"},
		{prefix: []byte("s/" + tenant + "/"), action: "purge tenant stats"},
	}
	for _, target := range prefixes {
		if err := t.deleteKeysByPrefix(ctx, target.prefix, target.action); err != nil {
			return err
		}
	}

	if t.vertexCache != nil {
		for key := range t.vertexCache {
			if strings.HasPrefix(key, tenant+"\x00") {
				delete(t.vertexCache, key)
			}
		}
	}
	if t.edgeMetaCache != nil {
		for key := range t.edgeMetaCache {
			if strings.HasPrefix(key, tenant+"\x00") {
				delete(t.edgeMetaCache, key)
			}
		}
	}

	if err := t.setUnchecked(keyspace.StatsVertexTotalKey(tenant), []byte("0"), "write purged tenant vertex total"); err != nil {
		return err
	}
	if err := t.setUnchecked(keyspace.StatsEdgeTotalKey(tenant), []byte("0"), "write purged tenant edge total"); err != nil {
		return err
	}
	t.clearTenantCounterCaches(tenant)
	t.queueTenantStatsRefresh(tenant)
	return nil
}

func (t *tx) clearTenantCounterCaches(tenant string) {
	if t == nil {
		return
	}
	prefix := "s/" + tenant + "/"
	if t.counterBase != nil {
		for key := range t.counterBase {
			if strings.HasPrefix(key, prefix) {
				delete(t.counterBase, key)
			}
		}
	}
	if t.counterBasePresent != nil {
		for key := range t.counterBasePresent {
			if strings.HasPrefix(key, prefix) {
				delete(t.counterBasePresent, key)
			}
		}
	}
	if t.counterDeltas != nil {
		for key := range t.counterDeltas {
			if strings.HasPrefix(key, prefix) {
				delete(t.counterDeltas, key)
			}
		}
	}
}

func (t *tx) deleteKeysByPrefix(ctx context.Context, prefix []byte, action string) error {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create prefix purge iterator", err)
	}
	defer iter.Close()

	keys := make([][]byte, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan keys for prefix purge", err)
	}
	for _, key := range keys {
		if err := t.delete(key, action); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) PatchVertexProperties(ctx context.Context, tenant, vertexID string, set graph.PropertyMap, removeKeys []string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("patch_vertex_properties", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	vertex, err := t.GetVertex(ctx, tenant, vertexID)
	if err != nil {
		return err
	}
	if vertex.Properties == nil {
		vertex.Properties = graph.PropertyMap{}
	}
	for key, value := range set {
		if key == "" {
			continue
		}
		vertex.Properties[key] = append([]byte(nil), value...)
	}
	for _, key := range removeKeys {
		if key == "" {
			continue
		}
		delete(vertex.Properties, key)
	}
	return t.PutVertexBatch(ctx, []*graph.Vertex{vertex})
}

func (t *tx) PatchEdgeProperties(ctx context.Context, tenant, edgeID string, set graph.PropertyMap, removeKeys []string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("patch_edge_properties", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	edge, err := t.GetEdge(ctx, tenant, edgeID)
	if err != nil {
		return err
	}
	if edge.Properties == nil {
		edge.Properties = graph.PropertyMap{}
	}
	for key, value := range set {
		if key == "" {
			continue
		}
		edge.Properties[key] = append([]byte(nil), value...)
	}
	for _, key := range removeKeys {
		if key == "" {
			continue
		}
		delete(edge.Properties, key)
	}
	return t.PutEdgeBatch(ctx, []*graph.Edge{edge})
}

func (t *tx) vertexCacheKey(tenant, vertexID string) string {
	return tenant + "\x00" + vertexID
}

func (t *tx) edgeMetaCacheKey(tenant, edgeID string) string {
	return tenant + "\x00" + edgeID
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

func (t *tx) edgeMetaFromCache(tenant, edgeID string) (edgeMeta, bool) {
	if t == nil || len(t.edgeMetaCache) == 0 {
		return edgeMeta{}, false
	}
	meta, ok := t.edgeMetaCache[t.edgeMetaCacheKey(tenant, edgeID)]
	return meta, ok
}

func (t *tx) cacheEdgeMeta(tenant, edgeID, edgeType, srcID, dstID string) {
	if t == nil || tenant == "" || edgeID == "" {
		return
	}
	if t.edgeMetaCache == nil {
		t.edgeMetaCache = map[string]edgeMeta{}
	}
	t.edgeMetaCache[t.edgeMetaCacheKey(tenant, edgeID)] = edgeMeta{edgeType: edgeType, srcID: srcID, dstID: dstID}
}

func (t *tx) dropEdgeMetaCache(tenant, edgeID string) {
	if t == nil || t.edgeMetaCache == nil {
		return
	}
	delete(t.edgeMetaCache, t.edgeMetaCacheKey(tenant, edgeID))
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
	if cached, ok := t.edgeMetaFromCache(tenant, edgeID); ok {
		return cached.edgeType, cached.srcID, cached.dstID, nil
	}

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
	t.cacheEdgeMeta(tenant, edgeID, edgeType, srcID, dstID)
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
			stored := decodeStoredPropertyValueBytes(iter.Value())
			if len(stored) > 0 {
				properties[property] = stored
				continue
			}
		}
		properties[property] = decodeStoredPropertyValueBytes(encodedValue)
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
		if err := t.delete(keyspace.PropertyIndexKey(tenant, schema, property, storedPropertyIndexTypeSegment(encodedValue), encodedValue, vertexID), "delete property index for vertex"); err != nil {
			return err
		}
		if orderedValue, ok := numericOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexNumericKey(tenant, schema, property, orderedValue, vertexID), "delete numeric property index for vertex"); err != nil {
				return err
			}
		}
		if orderedValue, ok := booleanOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexBooleanKey(tenant, schema, property, orderedValue, vertexID), "delete boolean property index for vertex"); err != nil {
				return err
			}
		}
		if orderedValue, ok := datetimeOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexDateTimeKey(tenant, schema, property, orderedValue, vertexID), "delete datetime property index for vertex"); err != nil {
				return err
			}
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
		storedValue := encodeStoredPropertyValue(valueCopy)
		if err := t.set(keyspace.VertexPropertyKey(vertex.Tenant, vertex.ID, vertexPropertySchema, property, storedValue), storedValue, "write vertex property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyVertexKey(vertex.Tenant, vertexPropertySchema, property, storedValue, vertex.ID), []byte(vertex.ID), "write property vertex reverse index"); err != nil {
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
		properties[property] = decodeStoredPropertyValueBytes(encodedValue)
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
		if err := t.delete(keyspace.PropertyIndexKey(tenant, schema, property, storedPropertyIndexTypeSegment(encodedValue), encodedValue, edgeID), "delete property index for edge"); err != nil {
			return err
		}
		if orderedValue, ok := numericOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexNumericKey(tenant, schema, property, orderedValue, edgeID), "delete numeric property index for edge"); err != nil {
				return err
			}
		}
		if orderedValue, ok := booleanOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexBooleanKey(tenant, schema, property, orderedValue, edgeID), "delete boolean property index for edge"); err != nil {
				return err
			}
		}
		if orderedValue, ok := datetimeOrderedValueFromEncoded(encodedValue); ok {
			if err := t.delete(keyspace.PropertyIndexDateTimeKey(tenant, schema, property, orderedValue, edgeID), "delete datetime property index for edge"); err != nil {
				return err
			}
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

type edgeTypeSourceKey struct {
	edgeType string
	srcID    string
}

type edgePairKey struct {
	leftID   string
	edgeType string
	rightID  string
}

func (t *tx) deleteEdgesByMeta(ctx context.Context, tenant string, deletions []edgeDeleteMeta) error {
	if len(deletions) == 0 {
		return nil
	}
	totalDelta := -len(deletions)
	typeDeltas := map[string]int{}
	sourceDegreeDeltas := map[edgeTypeSourceKey]int{}
	outPairDeltas := map[edgePairKey]int{}
	undirectedPairDeltas := map[edgePairKey]int{}
	for _, deletion := range deletions {
		typeDeltas[deletion.edgeType]--
		sourceDegreeDeltas[edgeTypeSourceKey{edgeType: deletion.edgeType, srcID: deletion.srcID}]--
		outPairDeltas[edgePairKey{leftID: deletion.srcID, edgeType: deletion.edgeType, rightID: deletion.dstID}]--
		leftID, rightID := canonicalEndpointPair(deletion.srcID, deletion.dstID)
		undirectedPairDeltas[edgePairKey{leftID: leftID, edgeType: deletion.edgeType, rightID: rightID}]--
	}
	if totalDelta != 0 {
		if err := t.addToCounter(keyspace.StatsEdgeTotalKey(tenant), totalDelta); err != nil {
			return err
		}
	}
	for edgeType, delta := range typeDeltas {
		if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(tenant, edgeType), delta); err != nil {
			return err
		}
	}
	for key, delta := range sourceDegreeDeltas {
		if err := t.adjustEdgeTypeSourceDegree(tenant, key.edgeType, key.srcID, delta); err != nil {
			return err
		}
	}
	for key, delta := range outPairDeltas {
		if err := t.adjustOutEndpointPairCount(tenant, key.leftID, key.edgeType, key.rightID, delta); err != nil {
			return err
		}
	}
	for key, delta := range undirectedPairDeltas {
		if err := t.adjustUndirectedEndpointPairCount(tenant, key.leftID, key.edgeType, key.rightID, delta); err != nil {
			return err
		}
	}
	for _, deletion := range deletions {
		if err := t.delete(keyspace.EdgeKey(tenant, deletion.edgeID), "delete edge"); err != nil {
			return err
		}
		if err := t.deleteEdgePropertiesBySchema(ctx, tenant, deletion.edgeID, ""); err != nil {
			return err
		}
		if err := t.delete(keyspace.EdgeTypeKey(tenant, deletion.edgeID, deletion.edgeType), "delete edge type"); err != nil {
			return err
		}
		if err := t.delete(keyspace.TypeEdgeKey(tenant, deletion.edgeType, deletion.edgeID), "delete type edge"); err != nil {
			return err
		}
		if err := t.delete(keyspace.OutAdjacencyKey(tenant, deletion.srcID, deletion.edgeType, deletion.edgeID), "delete out adjacency"); err != nil {
			return err
		}
		if err := t.delete(keyspace.InAdjacencyKey(tenant, deletion.dstID, deletion.edgeType, deletion.edgeID), "delete in adjacency"); err != nil {
			return err
		}
		if err := t.delete(keyspace.OutEndpointKey(tenant, deletion.srcID, deletion.edgeType, deletion.dstID, deletion.edgeID), "delete out endpoint"); err != nil {
			return err
		}
		t.dropEdgeMetaCache(tenant, deletion.edgeID)
	}
	return nil
}

func (t *tx) deleteEdgeKeysByIDs(ctx context.Context, tenant string, edgeIDs []string) error {
	if tenant == "" || len(edgeIDs) == 0 {
		return nil
	}
	edgeIDSet := make(map[string]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		edgeID = strings.TrimSpace(edgeID)
		if edgeID == "" {
			continue
		}
		edgeIDSet[edgeID] = struct{}{}
	}
	if len(edgeIDSet) == 0 {
		return nil
	}
	deleteMatchingKeys := func(prefix []byte, extractEdgeID func([]byte) string) error {
		iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
		if err != nil {
			return graph.NewError(graph.ErrKindStorage, "create edge cleanup iterator", err)
		}
		defer iter.Close()

		keys := make([][]byte, 0)
		for ok := iter.First(); ok; ok = iter.Next() {
			if err := checkCtx(ctx); err != nil {
				return err
			}
			edgeID := extractEdgeID(iter.Key())
			if _, ok := edgeIDSet[edgeID]; !ok {
				continue
			}
			keys = append(keys, append([]byte(nil), iter.Key()...))
		}
		if err := iter.Error(); err != nil {
			return graph.NewError(graph.ErrKindStorage, "scan edge cleanup keys", err)
		}
		for _, key := range keys {
			if err := t.delete(key, "delete stale edge key"); err != nil {
				return err
			}
		}
		return nil
	}

	if err := deleteMatchingKeys([]byte("et/"+tenant+"/"), func(key []byte) string {
		parts := bytes.Split(key, []byte{'/'})
		if len(parts) < 4 {
			return ""
		}
		return string(parts[len(parts)-2])
	}); err != nil {
		return err
	}
	if err := deleteMatchingKeys([]byte("te/"+tenant+"/"), edgeIDFromAdjKey); err != nil {
		return err
	}
	if err := deleteMatchingKeys(keyspace.OutAdjacencyTenantPrefix(tenant), edgeIDFromAdjKey); err != nil {
		return err
	}
	if err := deleteMatchingKeys(keyspace.InAdjacencyTenantPrefix(tenant), edgeIDFromAdjKey); err != nil {
		return err
	}
	if err := deleteMatchingKeys([]byte("od/"+tenant+"/"), edgeIDFromAdjKey); err != nil {
		return err
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
		storedValue := encodeStoredPropertyValue(valueCopy)
		if err := t.set(keyspace.EdgePropertyKey(edge.Tenant, edge.ID, edge.Type, property, storedValue), storedValue, "write edge property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyEdgeKey(edge.Tenant, edge.Type, property, storedValue, edge.ID), []byte(edge.ID), "write property edge reverse index"); err != nil {
			return err
		}
	}
	return nil
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
	statsEpoch, _, err := t.readInt64MaybeMissing(keyspace.StatsEpochKey(tenant))
	if err != nil {
		return nil, err
	}
	sampleSize, _, err := t.readCounterMaybeMissing(keyspace.StatsSampleSizeKey(tenant))
	if err != nil {
		return nil, err
	}
	if sampleSize <= 0 && (vertexTotal > 0 || edgeTotal > 0) {
		sampleSize = vertexTotal + edgeTotal
	}
	lastRefreshTS, _, err := t.readTimeMaybeMissing(keyspace.StatsLastRefreshKey(tenant))
	if err != nil {
		return nil, err
	}
	labelCounts, err := t.scanCounterMap(keyspace.StatsVertexLabelPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgeCounts, err := t.scanCounterMap(keyspace.StatsEdgeTypePrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgeSourceCounts, err := t.scanCounterMap(keyspace.StatsEdgeTypeSourceCountPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyNDV, err := t.scanSchemaPropertyCounterMap(keyspace.StatsVertexPropertyDistinctCountPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyNDVByKind, err := t.scanSchemaPropertyKindCounterMap(keyspace.StatsVertexPropertyDistinctCountByKindPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyEntries, err := t.scanSchemaPropertyCounterMap(keyspace.StatsVertexPropertyEntryCountPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyEntriesByKind, err := t.scanSchemaPropertyKindCounterMap(keyspace.StatsVertexPropertyEntryCountByKindPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyNDV, err := t.scanSchemaPropertyCounterMap(keyspace.StatsEdgePropertyDistinctCountPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyNDVByKind, err := t.scanSchemaPropertyKindCounterMap(keyspace.StatsEdgePropertyDistinctCountByKindPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyEntries, err := t.scanSchemaPropertyCounterMap(keyspace.StatsEdgePropertyEntryCountPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyEntriesByKind, err := t.scanSchemaPropertyKindCounterMap(keyspace.StatsEdgePropertyEntryCountByKindPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyHistograms, err := t.scanPropertyHistogramMap(keyspace.StatsVertexPropertyHistogramPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyHistograms, err := t.scanPropertyHistogramMap(keyspace.StatsEdgePropertyHistogramPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyEpoch, err := t.scanSchemaPropertyInt64Map(keyspace.StatsVertexPropertyEpochPrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertySampleSize, err := t.scanSchemaPropertyCounterMap(keyspace.StatsVertexPropertySampleSizePrefix(tenant))
	if err != nil {
		return nil, err
	}
	vertexPropertyLastRefresh, err := t.scanSchemaPropertyTimeMap(keyspace.StatsVertexPropertyLastRefreshPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyEpoch, err := t.scanSchemaPropertyInt64Map(keyspace.StatsEdgePropertyEpochPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertySampleSize, err := t.scanSchemaPropertyCounterMap(keyspace.StatsEdgePropertySampleSizePrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgePropertyLastRefresh, err := t.scanSchemaPropertyTimeMap(keyspace.StatsEdgePropertyLastRefreshPrefix(tenant))
	if err != nil {
		return nil, err
	}
	edgeAvgOutDegree := map[string]float64{}
	for edgeType, edgeCount := range edgeCounts {
		sourceCount := edgeSourceCounts[edgeType]
		if sourceCount <= 0 || edgeCount <= 0 {
			continue
		}
		edgeAvgOutDegree[edgeType] = float64(edgeCount) / float64(sourceCount)
	}

	vertexPropertySummary := buildPropertyStatsSummary(
		vertexPropertyNDV,
		vertexPropertyEntries,
		vertexPropertyNDVByKind,
		vertexPropertyEntriesByKind,
		vertexPropertyHistograms,
		vertexPropertyEpoch,
		vertexPropertySampleSize,
		vertexPropertyLastRefresh,
	)
	edgePropertySummary := buildPropertyStatsSummary(
		edgePropertyNDV,
		edgePropertyEntries,
		edgePropertyNDVByKind,
		edgePropertyEntriesByKind,
		edgePropertyHistograms,
		edgePropertyEpoch,
		edgePropertySampleSize,
		edgePropertyLastRefresh,
	)
	if len(vertexPropertySummary) == 0 && len(edgePropertySummary) == 0 {
		fallbackVertexSummary, fallbackEdgeSummary, fallbackErr := t.collectPropertyStatsSnapshotFallback(ctx, tenant, statsEpoch, lastRefreshTS)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		vertexPropertySummary = fallbackVertexSummary
		edgePropertySummary = fallbackEdgeSummary
	}

	return &graph.StatsSnapshot{
		Tenant:              tenant,
		StatsEpoch:          statsEpoch,
		SampleSize:          sampleSize,
		LastRefreshTS:       lastRefreshTS,
		VertexTotal:         vertexTotal,
		EdgeTotal:           edgeTotal,
		LabelCounts:         labelCounts,
		EdgeCounts:          edgeCounts,
		EdgeSourceCounts:    edgeSourceCounts,
		EdgeAvgOutDegree:    edgeAvgOutDegree,
		VertexPropertyStats: vertexPropertySummary,
		EdgePropertyStats:   edgePropertySummary,
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

func (t *tx) PutEdgeNew(ctx context.Context, edge *graph.Edge) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("put_edge_new", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if edge == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "edge is required", nil)
	}
	t.lockEdgeForMutation(edge.Tenant, edge.ID)
	if err := t.addToCounter(keyspace.StatsEdgeTotalKey(edge.Tenant), 1); err != nil {
		return err
	}
	if err := t.addToCounter(keyspace.StatsEdgeTypeCountKey(edge.Tenant, edge.Type), 1); err != nil {
		return err
	}
	if err := t.adjustEdgeTypeSourceDegree(edge.Tenant, edge.Type, edge.SrcID, 1); err != nil {
		return err
	}
	if err := t.writeEdgeProperties(edge); err != nil {
		return err
	}
	if err := t.set(keyspace.EdgeKey(edge.Tenant, edge.ID), edgePHash(edge), "write edge"); err != nil {
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
	t.cacheEdgeMeta(edge.Tenant, edge.ID, edge.Type, edge.SrcID, edge.DstID)
	t.queueTenantStatsRefresh(edge.Tenant)
	return nil
}

func (t *tx) DeleteEdgeBatch(ctx context.Context, tenant string, edgeIDs []string) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("delete_edge_batch", err, started) }()

	if err := t.ensureWrite(ctx); err != nil {
		return err
	}
	if tenant == "" || len(edgeIDs) == 0 {
		return nil
	}

	uniqueEdgeIDs := make([]string, 0, len(edgeIDs))
	seen := make(map[string]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		edgeID = strings.TrimSpace(edgeID)
		if edgeID == "" {
			continue
		}
		if _, ok := seen[edgeID]; ok {
			continue
		}
		seen[edgeID] = struct{}{}
		uniqueEdgeIDs = append(uniqueEdgeIDs, edgeID)
	}
	if len(uniqueEdgeIDs) == 0 {
		return nil
	}
	sort.Strings(uniqueEdgeIDs)

	deletions := make([]edgeDeleteMeta, 0, len(uniqueEdgeIDs))
	cleanupIDs := make([]string, 0, len(uniqueEdgeIDs))
	for _, edgeID := range uniqueEdgeIDs {
		t.lockEdgeForMutation(tenant, edgeID)
		if _, err := t.get(keyspace.EdgeKey(tenant, edgeID)); err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				continue
			}
			return err
		}
		cleanupIDs = append(cleanupIDs, edgeID)
		edgeType, srcID, dstID, err := t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				if err := t.delete(keyspace.EdgeKey(tenant, edgeID), "delete edge"); err != nil {
					return err
				}
				if err := t.deleteEdgePropertiesBySchema(ctx, tenant, edgeID, ""); err != nil {
					return err
				}
				continue
			}
			return err
		}
		deletions = append(deletions, edgeDeleteMeta{edgeID: edgeID, edgeType: edgeType, srcID: srcID, dstID: dstID})
	}

	if err := t.deleteEdgesByMeta(ctx, tenant, deletions); err != nil {
		return err
	}
	if err := t.deleteEdgeKeysByIDs(ctx, tenant, cleanupIDs); err != nil {
		return err
	}
	t.queueTenantStatsRefresh(tenant)
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

func (t *tx) adjustEdgeTypeSourceDegree(tenant, edgeType, srcID string, delta int) error {
	if delta == 0 {
		return nil
	}
	tenant = strings.TrimSpace(tenant)
	edgeType = strings.TrimSpace(edgeType)
	srcID = strings.TrimSpace(srcID)
	if tenant == "" || edgeType == "" || srcID == "" {
		return nil
	}
	degreeKey := keyspace.StatsEdgeTypeSourceDegreeKey(tenant, edgeType, srcID)
	current, _, err := t.readCounterMaybeMissing(degreeKey)
	if err != nil {
		return err
	}
	next := current + delta
	if current <= 0 && next > 0 {
		if err := t.addToCounter(keyspace.StatsEdgeTypeSourceCountKey(tenant, edgeType), 1); err != nil {
			return err
		}
	}
	if current > 0 && next <= 0 {
		if err := t.addToCounter(keyspace.StatsEdgeTypeSourceCountKey(tenant, edgeType), -1); err != nil {
			return err
		}
		if err := t.delete(degreeKey, "delete edge type source degree"); err != nil {
			return err
		}
		return nil
	}
	if next <= 0 {
		return nil
	}
	return t.set(degreeKey, []byte(strconv.Itoa(next)), "write edge type source degree")
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

func (t *tx) ScanAdjacencyLinks(ctx context.Context, tenant, vertexID string, direction graph.EdgeDirection, edgeType string, limit int, fn func(edgeID, peerID string) error) (err error) {
	started := time.Now()
	defer func() { t.observeOperation("scan_adjacency_links", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return err
	}
	if fn == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "callback is required", nil)
	}

	switch direction {
	case graph.EdgeDirectionOut:
		return t.ScanOutEdgeLinks(ctx, tenant, vertexID, edgeType, limit, fn)
	case graph.EdgeDirectionIn:
		seen := 0
		return t.ScanInEdges(ctx, tenant, vertexID, edgeType, limit, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			if err := fn(edge.ID, edge.SrcID); err != nil {
				return err
			}
			seen++
			if limit > 0 && seen >= limit {
				return nil
			}
			return nil
		})
	case graph.EdgeDirectionAny:
		seenEdges := map[string]struct{}{}
		emitted := 0
		emit := func(edgeID, peerID string) error {
			if _, ok := seenEdges[edgeID]; ok {
				return nil
			}
			seenEdges[edgeID] = struct{}{}
			if err := fn(edgeID, peerID); err != nil {
				return err
			}
			emitted++
			return nil
		}
		if err := t.ScanOutEdgeLinks(ctx, tenant, vertexID, edgeType, 0, emit); err != nil {
			return err
		}
		if limit > 0 && emitted >= limit {
			return nil
		}
		if err := t.ScanInEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			if limit > 0 && emitted >= limit {
				return nil
			}
			return emit(edge.ID, edge.SrcID)
		}); err != nil {
			return err
		}
		return nil
	default:
		return graph.NewError(graph.ErrKindInvalidInput, "unsupported edge direction", nil)
	}
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
		if !hasValue || !bytes.Equal(value, encodeStoredPropertyValue(encodedValue)) {
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
		value = decodeStoredPropertyValueBytes(value)
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
	return false, nil
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

func (t *tx) BatchHasDirectedEdgeBetween(ctx context.Context, tenant string, probes []graph.DirectedEdgeProbe) (results []bool, err error) {
	started := time.Now()
	defer func() { t.observeOperation("batch_has_directed_edge_between", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	results = make([]bool, len(probes))
	for i, probe := range probes {
		exists, err := t.HasDirectedEdgeBetweenFast(ctx, tenant, probe.SrcID, probe.DstID, probe.EdgeType)
		if err != nil {
			return nil, err
		}
		results[i] = exists
	}
	return results, nil
}

func (t *tx) BatchHasUndirectedEdgeBetween(ctx context.Context, tenant string, probes []graph.UndirectedEdgeProbe) (results []bool, err error) {
	started := time.Now()
	defer func() { t.observeOperation("batch_has_undirected_edge_between", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return nil, err
	}
	results = make([]bool, len(probes))
	for i, probe := range probes {
		exists, err := t.HasUndirectedEdgeBetweenFast(ctx, tenant, probe.LeftID, probe.RightID, probe.EdgeType)
		if err != nil {
			return nil, err
		}
		results[i] = exists
	}
	return results, nil
}

func (t *tx) DirectedEdgePairCount(ctx context.Context, tenant, srcID, dstID, edgeType string) (count int, err error) {
	started := time.Now()
	defer func() { t.observeOperation("directed_edge_pair_count", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return 0, err
	}
	raw, err := t.get(keyspace.OutEndpointPairCountKey(tenant, srcID, edgeType, dstID))
	if err == nil {
		if len(raw) == 0 {
			return 0, nil
		}
		parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
		if parseErr != nil {
			return 0, graph.NewError(graph.ErrKindStorage, "decode directed edge pair count", parseErr)
		}
		if parsed < 0 {
			return 0, nil
		}
		return parsed, nil
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		return 0, err
	}
	return t.countTypedOutEdgesBetween(ctx, tenant, srcID, dstID, edgeType)
}

func (t *tx) UndirectedEdgePairCount(ctx context.Context, tenant, leftID, rightID, edgeType string) (count int, err error) {
	started := time.Now()
	defer func() { t.observeOperation("undirected_edge_pair_count", err, started) }()

	if err := t.ensureActive(ctx); err != nil {
		return 0, err
	}
	forward, err := t.DirectedEdgePairCount(ctx, tenant, leftID, rightID, edgeType)
	if err != nil {
		return 0, err
	}
	if leftID == rightID {
		return forward, nil
	}
	reverse, err := t.DirectedEdgePairCount(ctx, tenant, rightID, leftID, edgeType)
	if err != nil {
		return 0, err
	}
	return forward + reverse, nil
}

func (t *tx) countTypedOutEdgesBetween(ctx context.Context, tenant, srcID, dstID, edgeType string) (int, error) {
	prefix := keyspace.OutEndpointPrefix(tenant, srcID, edgeType, dstID)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return 0, graph.NewError(graph.ErrKindStorage, "create out-endpoint count iterator", err)
	}
	defer iter.Close()

	count := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return 0, err
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, graph.NewError(graph.ErrKindStorage, "scan out-endpoint count", err)
	}
	if count > 0 {
		return count, nil
	}

	legacyCount := 0
	err = t.ScanOutEdgeLinks(ctx, tenant, srcID, edgeType, 0, func(_ string, edgeDstID string) error {
		if edgeDstID == dstID {
			legacyCount++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return legacyCount, nil
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
	storedValue := encodeStoredPropertyValue(encodedValue)
	return t.scanPropertyIndex(ctx, keyspace.PropertyIndexValuePrefix(tenant, schema, property, storedPropertyIndexTypeSegment(storedValue), storedValue), tenant, schema, property, limit, fn)
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
	storedValue := encodeStoredPropertyValue(entry.Value)
	key := keyspace.PropertyIndexKey(entry.Tenant, entry.Schema, entry.Property, storedPropertyIndexTypeSegment(storedValue), storedValue, entry.EntityID)
	if err := t.set(key, propertyIndexPayload(entry), "write property index"); err != nil {
		return err
	}
	if strings.EqualFold(entry.EntityClass, "vertex") {
		if err := t.set(keyspace.VertexPropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, storedValue), storedValue, "write vertex property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyVertexKey(entry.Tenant, entry.Schema, entry.Property, storedValue, entry.EntityID), []byte(entry.EntityID), "write property vertex reverse index"); err != nil {
			return err
		}
	}
	if strings.EqualFold(entry.EntityClass, "edge") {
		if err := t.set(keyspace.EdgePropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, storedValue), storedValue, "write edge property forward index"); err != nil {
			return err
		}
		if err := t.set(keyspace.PropertyEdgeKey(entry.Tenant, entry.Schema, entry.Property, storedValue, entry.EntityID), []byte(entry.EntityID), "write property edge reverse index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := numericOrderedValueFromEncoded(storedValue); ok {
		numericKey := keyspace.PropertyIndexNumericKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(numericKey, numericPropertyIndexPayload(entry), "write numeric property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := booleanOrderedValueFromEncoded(storedValue); ok {
		booleanKey := keyspace.PropertyIndexBooleanKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(booleanKey, numericPropertyIndexPayload(entry), "write boolean property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := datetimeOrderedValueFromEncoded(storedValue); ok {
		datetimeKey := keyspace.PropertyIndexDateTimeKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.set(datetimeKey, numericPropertyIndexPayload(entry), "write datetime property index"); err != nil {
			return err
		}
	}
	t.queuePropertyStatsRefresh(entry.Tenant, entry.EntityClass, entry.Schema, entry.Property)
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
	storedValue := encodeStoredPropertyValue(entry.Value)
	key := keyspace.PropertyIndexKey(entry.Tenant, entry.Schema, entry.Property, storedPropertyIndexTypeSegment(storedValue), storedValue, entry.EntityID)
	if err := t.delete(key, "delete property index"); err != nil {
		return err
	}
	if strings.EqualFold(entry.EntityClass, "vertex") {
		if err := t.delete(keyspace.VertexPropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, storedValue), "delete vertex property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyVertexKey(entry.Tenant, entry.Schema, entry.Property, storedValue, entry.EntityID), "delete property vertex reverse index"); err != nil {
			return err
		}
	}
	if strings.EqualFold(entry.EntityClass, "edge") {
		if err := t.delete(keyspace.EdgePropertyKey(entry.Tenant, entry.EntityID, entry.Schema, entry.Property, storedValue), "delete edge property forward index"); err != nil {
			return err
		}
		if err := t.delete(keyspace.PropertyEdgeKey(entry.Tenant, entry.Schema, entry.Property, storedValue, entry.EntityID), "delete property edge reverse index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := numericOrderedValueFromEncoded(storedValue); ok {
		numericKey := keyspace.PropertyIndexNumericKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(numericKey, "delete numeric property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := booleanOrderedValueFromEncoded(storedValue); ok {
		booleanKey := keyspace.PropertyIndexBooleanKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(booleanKey, "delete boolean property index"); err != nil {
			return err
		}
	}
	if orderedValue, ok := datetimeOrderedValueFromEncoded(storedValue); ok {
		datetimeKey := keyspace.PropertyIndexDateTimeKey(entry.Tenant, entry.Schema, entry.Property, orderedValue, entry.EntityID)
		if err := t.delete(datetimeKey, "delete datetime property index"); err != nil {
			return err
		}
	}
	t.queuePropertyStatsRefresh(entry.Tenant, entry.EntityClass, entry.Schema, entry.Property)
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
	if err := t.flushPendingPropertyStats(context.Background()); err != nil {
		return err
	}
	if err := t.flushCounterDeltas(); err != nil {
		return err
	}
	if err := t.flushPendingTenantStatsMetadata(); err != nil {
		return err
	}
	if err := t.batch.Commit(cpebble.Sync); err != nil {
		return graph.NewError(graph.ErrKindStorage, "commit transaction", err)
	}
	return nil
}

func (t *tx) queueTenantStatsRefresh(tenant string) {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return
	}
	if t.pendingTenantStats == nil {
		t.pendingTenantStats = map[string]struct{}{}
	}
	t.pendingTenantStats[tenant] = struct{}{}
}

func (t *tx) flushPendingTenantStatsMetadata() error {
	if t == nil || t.mode != graph.TxReadWrite || len(t.pendingTenantStats) == 0 {
		return nil
	}
	tenants := make([]string, 0, len(t.pendingTenantStats))
	for tenant := range t.pendingTenantStats {
		tenants = append(tenants, tenant)
	}
	t.pendingTenantStats = nil
	sort.Strings(tenants)
	for _, tenant := range tenants {
		if err := t.refreshTenantStatsMetadata(tenant); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) queuePropertyStatsRefresh(tenant, entityClass, schema, property string) {
	tenant = strings.TrimSpace(tenant)
	entityClass = strings.ToLower(strings.TrimSpace(entityClass))
	schema = strings.TrimSpace(schema)
	property = strings.TrimSpace(property)
	if tenant == "" || entityClass == "" || schema == "" || property == "" {
		return
	}
	if t.pendingPropertyStats == nil {
		t.pendingPropertyStats = map[string]propertyStatsTarget{}
	}
	key := tenant + "\x00" + entityClass + "\x00" + schema + "\x00" + property
	t.pendingPropertyStats[key] = propertyStatsTarget{tenant: tenant, entityClass: entityClass, schema: schema, property: property}
}

func (t *tx) flushPendingPropertyStats(_ context.Context) error {
	if t == nil || t.mode != graph.TxReadWrite || len(t.pendingPropertyStats) == 0 {
		return nil
	}
	t.pendingPropertyStats = nil
	return nil
}

func (t *tx) collectPropertyStatsSnapshotFallback(ctx context.Context, tenant string, statsEpoch int64, refreshTS time.Time) (map[string]map[string]graph.StatsPropertySummary, map[string]map[string]graph.StatsPropertySummary, error) {
	vertexSummary := map[string]map[string]graph.StatsPropertySummary{}
	edgeSummary := map[string]map[string]graph.StatsPropertySummary{}
	vertexProperties := map[string]map[string]struct{}{}
	edgeProperties := map[string]map[string]struct{}{}

	prefix := []byte("pi/" + tenant + "/")
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, nil, graph.NewError(graph.ErrKindStorage, "create property snapshot fallback iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return nil, nil, err
		}
		parts := strings.Split(string(iter.Key()), "/")
		if len(parts) < 6 {
			continue
		}
		schema := parts[2]
		property := parts[3]
		entry, err := propertyIndexEntryFromKey(iter.Key(), iter.Value(), tenant, schema, property)
		if err != nil {
			return nil, nil, err
		}
		target := edgeProperties
		if strings.EqualFold(entry.EntityClass, "vertex") {
			target = vertexProperties
		}
		if target[schema] == nil {
			target[schema] = map[string]struct{}{}
		}
		target[schema][property] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return nil, nil, graph.NewError(graph.ErrKindStorage, "scan property snapshot fallback", err)
	}

	for schema, properties := range vertexProperties {
		for property := range properties {
			summary, err := t.collectPropertyStats(ctx, tenant, "vertex", schema, property)
			if err != nil {
				return nil, nil, err
			}
			summary.StatsEpoch = statsEpoch
			summary.SampleSize = summary.IndexedEntries
			summary.LastRefreshTS = refreshTS
			if vertexSummary[schema] == nil {
				vertexSummary[schema] = map[string]graph.StatsPropertySummary{}
			}
			vertexSummary[schema][property] = summary
		}
	}
	for schema, properties := range edgeProperties {
		for property := range properties {
			summary, err := t.collectPropertyStats(ctx, tenant, "edge", schema, property)
			if err != nil {
				return nil, nil, err
			}
			summary.StatsEpoch = statsEpoch
			summary.SampleSize = summary.IndexedEntries
			summary.LastRefreshTS = refreshTS
			if edgeSummary[schema] == nil {
				edgeSummary[schema] = map[string]graph.StatsPropertySummary{}
			}
			edgeSummary[schema][property] = summary
		}
	}

	return vertexSummary, edgeSummary, nil
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
			if graph.IsKind(err, graph.ErrKindNotFound) {
				continue
			}
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
	edgeTypeSourceDegrees := map[string]map[string]int{}
	edgeSourceCounts := map[string]int{}

	if err := t.ScanVertices(ctx, tenant, 0, func(v *graph.Vertex) error {
		if v == nil {
			return nil
		}
		vertexTotal++
		for _, label := range v.Labels {
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
		edgeType, srcID, _, err := t.loadEdgeTypeAndEndpoints(ctx, tenant, edgeID)
		if err != nil {
			return err
		}
		edgeTotal++
		edgeCounts[edgeType]++
		srcID = strings.TrimSpace(srcID)
		if srcID != "" {
			if edgeTypeSourceDegrees[edgeType] == nil {
				edgeTypeSourceDegrees[edgeType] = map[string]int{}
			}
			edgeTypeSourceDegrees[edgeType][srcID]++
		}
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
	if err := t.clearCounterPrefix(keyspace.StatsEdgeTypeSourceCountPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgeTypeSourceDegreePrefix(tenant)); err != nil {
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
		edgeSourceCounts[edgeType] = len(edgeTypeSourceDegrees[edgeType])
		if edgeSourceCounts[edgeType] > 0 {
			if err := t.setUnchecked(keyspace.StatsEdgeTypeSourceCountKey(tenant, edgeType), []byte(strconv.Itoa(edgeSourceCounts[edgeType])), "write edge type source count"); err != nil {
				return err
			}
		}
		sourceIDs := make([]string, 0, len(edgeTypeSourceDegrees[edgeType]))
		for srcID := range edgeTypeSourceDegrees[edgeType] {
			sourceIDs = append(sourceIDs, srcID)
		}
		sort.Strings(sourceIDs)
		for _, srcID := range sourceIDs {
			count := edgeTypeSourceDegrees[edgeType][srcID]
			if count <= 0 {
				continue
			}
			if err := t.setUnchecked(keyspace.StatsEdgeTypeSourceDegreeKey(tenant, edgeType, srcID), []byte(strconv.Itoa(count)), "write edge type source degree"); err != nil {
				return err
			}
		}
	}
	if err := t.backfillPropertyStats(ctx, tenant); err != nil {
		return err
	}
	return t.refreshTenantStatsMetadata(tenant)
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

func (t *tx) readInt64MaybeMissing(key []byte) (value int64, found bool, err error) {
	buf, err := t.get(key)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	parsed, parseErr := strconv.ParseInt(string(buf), 10, 64)
	if parseErr != nil {
		return 0, false, graph.NewError(graph.ErrKindStorage, "decode stats metadata", parseErr)
	}
	return parsed, true, nil
}

func (t *tx) readTimeMaybeMissing(key []byte) (time.Time, bool, error) {
	buf, err := t.get(key)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	parsed, parseErr := time.Parse(time.RFC3339Nano, string(buf))
	if parseErr != nil {
		return time.Time{}, false, graph.NewError(graph.ErrKindStorage, "decode stats time metadata", parseErr)
	}
	return parsed.UTC(), true, nil
}

func (t *tx) scanSchemaPropertyCounterMap(prefix []byte) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create schema property counter iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := string(key[len(prefix):])
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) != 2 {
			continue
		}
		schema := parts[0]
		property := parts[1]
		if schema == "" || property == "" {
			continue
		}
		value, parseErr := strconv.ParseInt(string(iter.Value()), 10, 64)
		if parseErr != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode schema property counter", parseErr)
		}
		if value <= 0 {
			continue
		}
		if out[schema] == nil {
			out[schema] = map[string]int{}
		}
		out[schema][property] = int(value)
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan schema property counters", err)
	}
	return out, nil
}

func (t *tx) scanSchemaPropertyKindCounterMap(prefix []byte) (map[string]map[string]map[string]int, error) {
	out := map[string]map[string]map[string]int{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create schema property kind counter iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := string(key[len(prefix):])
		parts := strings.SplitN(suffix, "/", 3)
		if len(parts) != 3 {
			continue
		}
		schema := parts[0]
		property := parts[1]
		kind := parts[2]
		if schema == "" || property == "" || kind == "" {
			continue
		}
		value, parseErr := strconv.ParseInt(string(iter.Value()), 10, 64)
		if parseErr != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode schema property kind counter", parseErr)
		}
		if value <= 0 {
			continue
		}
		if out[schema] == nil {
			out[schema] = map[string]map[string]int{}
		}
		if out[schema][property] == nil {
			out[schema][property] = map[string]int{}
		}
		out[schema][property][kind] = int(value)
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan schema property kind counters", err)
	}
	return out, nil
}

func (t *tx) scanSchemaPropertyInt64Map(prefix []byte) (map[string]map[string]int64, error) {
	out := map[string]map[string]int64{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create schema property int64 iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := string(key[len(prefix):])
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) != 2 {
			continue
		}
		schema := parts[0]
		property := parts[1]
		if schema == "" || property == "" {
			continue
		}
		value, parseErr := strconv.ParseInt(string(iter.Value()), 10, 64)
		if parseErr != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode schema property int64", parseErr)
		}
		if out[schema] == nil {
			out[schema] = map[string]int64{}
		}
		out[schema][property] = value
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan schema property int64", err)
	}
	return out, nil
}

func (t *tx) scanSchemaPropertyTimeMap(prefix []byte) (map[string]map[string]time.Time, error) {
	out := map[string]map[string]time.Time{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create schema property time iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := string(key[len(prefix):])
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) != 2 {
			continue
		}
		schema := parts[0]
		property := parts[1]
		if schema == "" || property == "" {
			continue
		}
		parsed, parseErr := time.Parse(time.RFC3339Nano, string(iter.Value()))
		if parseErr != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode schema property time", parseErr)
		}
		if out[schema] == nil {
			out[schema] = map[string]time.Time{}
		}
		out[schema][property] = parsed.UTC()
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan schema property time", err)
	}
	return out, nil
}

func (t *tx) scanPropertyHistogramMap(prefix []byte) (map[string]map[string]map[string]*graph.StatsHistogram, error) {
	out := map[string]map[string]map[string]*graph.StatsHistogram{}
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create property histogram iterator", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		suffix := string(key[len(prefix):])
		parts := strings.SplitN(suffix, "/", 4)
		if len(parts) != 4 {
			continue
		}
		schema := parts[0]
		property := parts[1]
		kind := parts[2]
		if schema == "" || property == "" || kind == "" {
			continue
		}
		var bucket graph.StatsHistogramBucket
		if err := json.Unmarshal(iter.Value(), &bucket); err != nil {
			return nil, graph.NewError(graph.ErrKindStorage, "decode property histogram bucket", err)
		}
		if out[schema] == nil {
			out[schema] = map[string]map[string]*graph.StatsHistogram{}
		}
		if out[schema][property] == nil {
			out[schema][property] = map[string]*graph.StatsHistogram{}
		}
		histogram := out[schema][property][kind]
		if histogram == nil {
			histogram = &graph.StatsHistogram{Kind: kind, Buckets: []graph.StatsHistogramBucket{}}
			out[schema][property][kind] = histogram
		}
		histogram.Buckets = append(histogram.Buckets, bucket)
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan property histograms", err)
	}
	return out, nil
}

func (t *tx) refreshTenantStatsMetadata(tenant string) error {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return nil
	}
	vertexTotal, _, err := t.readCounterMaybeMissing(keyspace.StatsVertexTotalKey(tenant))
	if err != nil {
		return err
	}
	edgeTotal, _, err := t.readCounterMaybeMissing(keyspace.StatsEdgeTotalKey(tenant))
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := t.setUnchecked(keyspace.StatsEpochKey(tenant), []byte(strconv.FormatInt(now.UnixNano(), 10)), "write stats epoch"); err != nil {
		return err
	}
	if err := t.setUnchecked(keyspace.StatsSampleSizeKey(tenant), []byte(strconv.Itoa(vertexTotal+edgeTotal)), "write stats sample size"); err != nil {
		return err
	}
	return t.setUnchecked(keyspace.StatsLastRefreshKey(tenant), []byte(now.Format(time.RFC3339Nano)), "write stats refresh ts")
}

func (t *tx) collectOrderedPropertyHistogram(tenant, schema, property, entityClass, kind string, prefix []byte, decode func([]byte, []byte, string, string, string) (*graph.PropertyIndexEntry, error)) (*graph.StatsHistogram, error) {
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "create ordered property histogram iterator", err)
	}
	defer iter.Close()

	valueCounts := make([]propertyValueCount, 0)
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := decode(iter.Key(), iter.Value(), tenant, schema, property)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(entry.EntityClass, entityClass) {
			continue
		}
		value := string(entry.Value)
		if len(valueCounts) == 0 || valueCounts[len(valueCounts)-1].value != value {
			valueCounts = append(valueCounts, propertyValueCount{value: value, count: 1})
			continue
		}
		valueCounts[len(valueCounts)-1].count++
	}
	if err := iter.Error(); err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "scan ordered property histogram", err)
	}
	return buildEquiDepthHistogram(kind, valueCounts, statsHistogramBucketCount), nil
}

func (t *tx) collectPropertyStats(ctx context.Context, tenant, entityClass, schema, property string) (graph.StatsPropertySummary, error) {
	summary := graph.StatsPropertySummary{}
	summary.DistinctValuesByKind = map[string]int{}
	summary.IndexedEntriesByKind = map[string]int{}
	summary.EstimatedSelectivityByKind = map[string]float64{}
	summary.Histograms = map[string]*graph.StatsHistogram{}
	prefix := keyspace.PropertyIndexPrefix(tenant, schema, property)
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return summary, graph.NewError(graph.ErrKindStorage, "create property stats iterator", err)
	}
	defer iter.Close()

	seenValues := map[string]struct{}{}
	valueCountsByKind := map[string]map[string]int{}
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return summary, err
		}
		entry, err := propertyIndexEntryFromKey(iter.Key(), iter.Value(), tenant, schema, property)
		if err != nil {
			return summary, err
		}
		if !strings.EqualFold(entry.EntityClass, entityClass) {
			continue
		}
		summary.IndexedEntries++
		decodedValue := decodeStoredPropertyValueBytes(entry.Value)
		value := string(decodedValue)
		seenValues[value] = struct{}{}
		kind := propertyValueKind(decodedValue)
		if valueCountsByKind[kind] == nil {
			valueCountsByKind[kind] = map[string]int{}
		}
		valueCountsByKind[kind][value]++
		summary.IndexedEntriesByKind[kind]++
	}
	if err := iter.Error(); err != nil {
		return summary, graph.NewError(graph.ErrKindStorage, "scan property stats", err)
	}
	summary.DistinctValues = len(seenValues)
	if summary.DistinctValues > 0 {
		summary.EstimatedSelectivity = 1 / float64(summary.DistinctValues)
	}
	for kind, counts := range valueCountsByKind {
		summary.DistinctValuesByKind[kind] = len(counts)
		if len(counts) > 0 {
			summary.EstimatedSelectivityByKind[kind] = 1 / float64(len(counts))
		}
	}
	numericHistogram, err := t.collectOrderedPropertyHistogram(tenant, schema, property, entityClass, "numeric", keyspace.PropertyIndexNumericPrefix(tenant, schema, property), numericPropertyIndexEntryFromKey)
	if err != nil {
		return summary, err
	}
	if numericHistogram != nil {
		summary.Histograms["numeric"] = numericHistogram
	}
	datetimeHistogram, err := t.collectOrderedPropertyHistogram(tenant, schema, property, entityClass, "datetime", keyspace.PropertyIndexDateTimePrefix(tenant, schema, property), datetimePropertyIndexEntryFromKey)
	if err != nil {
		return summary, err
	}
	if datetimeHistogram != nil {
		summary.Histograms["datetime"] = datetimeHistogram
	}
	booleanHistogram, err := t.collectOrderedPropertyHistogram(tenant, schema, property, entityClass, "boolean", keyspace.PropertyIndexBooleanPrefix(tenant, schema, property), booleanPropertyIndexEntryFromKey)
	if err != nil {
		return summary, err
	}
	if booleanHistogram != nil {
		summary.Histograms["boolean"] = booleanHistogram
	}
	if len(valueCountsByKind["categorical"]) > 0 {
		summary.Histograms["categorical"] = buildHistogramFromCounts("categorical", valueCountsByKind["categorical"], statsHistogramBucketCount)
	}
	summary.Histogram = primaryHistogram(summary.Histograms)
	return summary, nil
}

func (t *tx) refreshPropertyStats(ctx context.Context, tenant, entityClass, schema, property string) error {
	tenant = strings.TrimSpace(tenant)
	entityClass = strings.ToLower(strings.TrimSpace(entityClass))
	schema = strings.TrimSpace(schema)
	property = strings.TrimSpace(property)
	if tenant == "" || schema == "" || property == "" {
		return nil
	}
	summary, err := t.collectPropertyStats(ctx, tenant, entityClass, schema, property)
	if err != nil {
		return err
	}
	var ndvKey []byte
	var entriesKey []byte
	if entityClass == "vertex" {
		ndvKey = keyspace.StatsVertexPropertyDistinctCountKey(tenant, schema, property)
		entriesKey = keyspace.StatsVertexPropertyEntryCountKey(tenant, schema, property)
	} else {
		ndvKey = keyspace.StatsEdgePropertyDistinctCountKey(tenant, schema, property)
		entriesKey = keyspace.StatsEdgePropertyEntryCountKey(tenant, schema, property)
	}
	if err := t.delete(ndvKey, "delete property ndv stats"); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	if err := t.delete(entriesKey, "delete property entry count stats"); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	if err := t.clearCounterPrefix(statsPropertyHistogramPrefix(tenant, entityClass, schema, property)); err != nil {
		return err
	}
	var ndvByKindPrefix []byte
	var entriesByKindPrefix []byte
	var epochKey []byte
	var sampleSizeKey []byte
	var refreshKey []byte
	if entityClass == "vertex" {
		ndvByKindPrefix = statsPropertyCounterByKindPrefix(tenant, entityClass, schema, property)
		entriesByKindPrefix = statsPropertyEntriesByKindPrefix(tenant, entityClass, schema, property)
		epochKey = keyspace.StatsVertexPropertyEpochKey(tenant, schema, property)
		sampleSizeKey = keyspace.StatsVertexPropertySampleSizeKey(tenant, schema, property)
		refreshKey = keyspace.StatsVertexPropertyLastRefreshKey(tenant, schema, property)
	} else {
		ndvByKindPrefix = statsPropertyCounterByKindPrefix(tenant, entityClass, schema, property)
		entriesByKindPrefix = statsPropertyEntriesByKindPrefix(tenant, entityClass, schema, property)
		epochKey = keyspace.StatsEdgePropertyEpochKey(tenant, schema, property)
		sampleSizeKey = keyspace.StatsEdgePropertySampleSizeKey(tenant, schema, property)
		refreshKey = keyspace.StatsEdgePropertyLastRefreshKey(tenant, schema, property)
	}
	if err := t.clearCounterPrefix(ndvByKindPrefix); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(entriesByKindPrefix); err != nil {
		return err
	}
	if summary.DistinctValues > 0 {
		if err := t.setUnchecked(ndvKey, []byte(strconv.Itoa(summary.DistinctValues)), "write property ndv stats"); err != nil {
			return err
		}
	}
	if summary.IndexedEntries > 0 {
		if err := t.setUnchecked(entriesKey, []byte(strconv.Itoa(summary.IndexedEntries)), "write property entry count stats"); err != nil {
			return err
		}
	}
	kinds := make([]string, 0, len(summary.DistinctValuesByKind))
	for kind := range summary.DistinctValuesByKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		distinctCount := summary.DistinctValuesByKind[kind]
		if distinctCount <= 0 {
			continue
		}
		var key []byte
		if entityClass == "vertex" {
			key = keyspace.StatsVertexPropertyDistinctCountByKindKey(tenant, schema, property, kind)
		} else {
			key = keyspace.StatsEdgePropertyDistinctCountByKindKey(tenant, schema, property, kind)
		}
		if err := t.setUnchecked(key, []byte(strconv.Itoa(distinctCount)), "write property ndv-by-kind stats"); err != nil {
			return err
		}
	}
	kinds = make([]string, 0, len(summary.IndexedEntriesByKind))
	for kind := range summary.IndexedEntriesByKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		entryCount := summary.IndexedEntriesByKind[kind]
		if entryCount <= 0 {
			continue
		}
		var key []byte
		if entityClass == "vertex" {
			key = keyspace.StatsVertexPropertyEntryCountByKindKey(tenant, schema, property, kind)
		} else {
			key = keyspace.StatsEdgePropertyEntryCountByKindKey(tenant, schema, property, kind)
		}
		if err := t.setUnchecked(key, []byte(strconv.Itoa(entryCount)), "write property entry-by-kind stats"); err != nil {
			return err
		}
	}
	histKinds := make([]string, 0, len(summary.Histograms))
	for kind := range summary.Histograms {
		histKinds = append(histKinds, kind)
	}
	sort.Strings(histKinds)
	for _, kind := range histKinds {
		histogram := summary.Histograms[kind]
		if histogram == nil {
			continue
		}
		for idx, bucket := range histogram.Buckets {
			payload, err := json.Marshal(bucket)
			if err != nil {
				return graph.NewError(graph.ErrKindStorage, "encode property histogram bucket", err)
			}
			var histKey []byte
			if entityClass == "vertex" {
				histKey = keyspace.StatsVertexPropertyHistogramKey(tenant, schema, property, kind, idx)
			} else {
				histKey = keyspace.StatsEdgePropertyHistogramKey(tenant, schema, property, kind, idx)
			}
			if err := t.setUnchecked(histKey, payload, "write property histogram bucket"); err != nil {
				return err
			}
		}
	}
	now := time.Now().UTC()
	if err := t.setUnchecked(epochKey, []byte(strconv.FormatInt(now.UnixNano(), 10)), "write property stats epoch"); err != nil {
		return err
	}
	if err := t.setUnchecked(sampleSizeKey, []byte(strconv.Itoa(summary.IndexedEntries)), "write property stats sample size"); err != nil {
		return err
	}
	if err := t.setUnchecked(refreshKey, []byte(now.Format(time.RFC3339Nano)), "write property stats refresh ts"); err != nil {
		return err
	}
	return t.refreshTenantStatsMetadata(tenant)
}

func (t *tx) backfillPropertyStats(ctx context.Context, tenant string) error {
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyDistinctCountPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyDistinctCountByKindPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyEntryCountPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyEntryCountByKindPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyEpochPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertySampleSizePrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyLastRefreshPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyDistinctCountPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyDistinctCountByKindPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyEntryCountPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyEntryCountByKindPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyEpochPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertySampleSizePrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyLastRefreshPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsVertexPropertyHistogramPrefix(tenant)); err != nil {
		return err
	}
	if err := t.clearCounterPrefix(keyspace.StatsEdgePropertyHistogramPrefix(tenant)); err != nil {
		return err
	}

	vertexProperties := map[string]map[string]struct{}{}
	edgeProperties := map[string]map[string]struct{}{}
	prefix := []byte("pi/" + tenant + "/")
	iter, err := t.reader.NewIter(&cpebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return graph.NewError(graph.ErrKindStorage, "create property backfill iterator", err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		parts := strings.Split(string(iter.Key()), "/")
		if len(parts) < 6 {
			continue
		}
		schema := parts[2]
		property := parts[3]
		entry, err := propertyIndexEntryFromKey(iter.Key(), iter.Value(), tenant, schema, property)
		if err != nil {
			return err
		}
		target := edgeProperties
		if strings.EqualFold(entry.EntityClass, "vertex") {
			target = vertexProperties
		}
		if target[schema] == nil {
			target[schema] = map[string]struct{}{}
		}
		target[schema][property] = struct{}{}
	}
	if err := iter.Error(); err != nil {
		return graph.NewError(graph.ErrKindStorage, "scan property stats for backfill", err)
	}
	for schema, properties := range vertexProperties {
		propertyNames := make([]string, 0, len(properties))
		for property := range properties {
			propertyNames = append(propertyNames, property)
		}
		sort.Strings(propertyNames)
		for _, property := range propertyNames {
			if err := t.refreshPropertyStats(ctx, tenant, "vertex", schema, property); err != nil {
				return err
			}
		}
	}
	for schema, properties := range edgeProperties {
		propertyNames := make([]string, 0, len(properties))
		for property := range properties {
			propertyNames = append(propertyNames, property)
		}
		sort.Strings(propertyNames)
		for _, property := range propertyNames {
			if err := t.refreshPropertyStats(ctx, tenant, "edge", schema, property); err != nil {
				return err
			}
		}
	}
	return t.refreshTenantStatsMetadata(tenant)
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

func (t *tx) lockEdgeForMutation(tenant, edgeID string) {
	if t.locks == nil {
		t.locks = make(map[string]func())
	}
	key := tenant + "\x00" + edgeID
	if _, ok := t.locks[key]; ok {
		return
	}
	t.locks[key] = t.lockEdge(tenant, edgeID)
}

func (t *tx) lockEdge(tenant, edgeID string) func() {
	key := tenant + "\x00" + edgeID
	raw, _ := t.edgeLocks.LoadOrStore(key, &sync.Mutex{})
	mu := raw.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
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

func canonicalEndpointPair(leftID, rightID string) (string, string) {
	if strings.Compare(leftID, rightID) <= 0 {
		return leftID, rightID
	}
	return rightID, leftID
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
