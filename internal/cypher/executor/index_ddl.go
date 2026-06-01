package executor

import (
	"context"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

// CreatePropertyIndex registers a property index in the runtime catalog and backfills
// existing matching vertices into the persistent property-index keyspace.
func (e *Executor) CreatePropertyIndex(ctx context.Context, tenant, schema, property string, ifNotExists bool) (created bool, indexedEntities int, err error) {
	if e == nil || e.store == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	if e.indexCatalog == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "index catalog is not configured", nil)
	}

	tenant = strings.TrimSpace(tenant)
	schema = strings.TrimSpace(schema)
	property = strings.TrimSpace(property)
	if tenant == "" || schema == "" || property == "" {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, and property are required", nil)
	}

	if e.indexCatalog.HasPropertyIndex(tenant, schema, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "property index already exists", nil)
	}
	if !e.indexCatalog.AddPropertyIndex(tenant, schema, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "property index already exists", nil)
	}

	count, buildErr := e.BackfillPropertyIndex(ctx, tenant, schema, property)
	if buildErr != nil {
		e.indexCatalog.RemovePropertyIndex(tenant, schema, property)
		return false, 0, buildErr
	}

	return true, count, nil
}

// CreateEdgePropertyIndex registers an edge-property index in the runtime catalog and
// backfills matching edges into the persistent property-index keyspace.
func (e *Executor) CreateEdgePropertyIndex(ctx context.Context, tenant, edgeType, property string, ifNotExists bool) (created bool, indexedEntities int, err error) {
	if e == nil || e.store == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	if e.indexCatalog == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "index catalog is not configured", nil)
	}

	tenant = strings.TrimSpace(tenant)
	edgeType = strings.TrimSpace(edgeType)
	property = strings.TrimSpace(property)
	if tenant == "" || edgeType == "" || property == "" {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "tenant, edge type, and property are required", nil)
	}

	if e.indexCatalog.HasEdgePropertyIndex(tenant, edgeType, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "edge property index already exists", nil)
	}
	if !e.indexCatalog.AddEdgePropertyIndex(tenant, edgeType, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "edge property index already exists", nil)
	}

	count, buildErr := e.BackfillEdgePropertyIndex(ctx, tenant, edgeType, property)
	if buildErr != nil {
		e.indexCatalog.RemoveEdgePropertyIndex(tenant, edgeType, property)
		return false, 0, buildErr
	}

	return true, count, nil
}

// CreateEdgePropertyIndexAsync registers an edge-property index and enqueues a
// durable background backfill job so callers can return quickly on large
// datasets. Pending jobs are resumed across restarts by StartIndexBuildWorker.
func (e *Executor) CreateEdgePropertyIndexAsync(ctx context.Context, tenant, edgeType, property string, ifNotExists bool) (created bool, indexedEntities int, err error) {
	if e == nil || e.store == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	if e.indexCatalog == nil {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "index catalog is not configured", nil)
	}

	tenant = strings.TrimSpace(tenant)
	edgeType = strings.TrimSpace(edgeType)
	property = strings.TrimSpace(property)
	if tenant == "" || edgeType == "" || property == "" {
		return false, 0, graph.NewError(graph.ErrKindInvalidInput, "tenant, edge type, and property are required", nil)
	}

	if e.indexCatalog.HasEdgePropertyIndex(tenant, edgeType, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "edge property index already exists", nil)
	}
	if !e.indexCatalog.AddEdgePropertyIndex(tenant, edgeType, property) {
		if ifNotExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindConflict, "edge property index already exists", nil)
	}

	if enqueueErr := e.enqueueEdgeIndexBuildJob(ctx, tenant, edgeType, property); enqueueErr != nil {
		e.indexCatalog.RemoveEdgePropertyIndex(tenant, edgeType, property)
		return false, 0, enqueueErr
	}

	return true, 0, nil
}

// BackfillPropertyIndex writes index entries for existing vertices that match
// (tenant, schema, property). This is intended for startup migrations.
func (e *Executor) BackfillPropertyIndex(ctx context.Context, tenant, schema, property string) (int, error) {
	if e == nil || e.store == nil {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	tenant = strings.TrimSpace(tenant)
	schema = strings.TrimSpace(schema)
	property = strings.TrimSpace(property)
	if tenant == "" || schema == "" || property == "" {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "tenant, schema, and property are required", nil)
	}

	count := 0
	err := e.store.Update(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if !vertexHasLabel(vertex, schema) {
				return nil
			}
			if vertex == nil || strings.TrimSpace(vertex.ID) == "" || vertex.Properties == nil {
				return nil
			}
			stored, ok := vertex.Properties[property]
			if !ok {
				return nil
			}
			entry := &graph.PropertyIndexEntry{
				Tenant:      tenant,
				Schema:      schema,
				Property:    property,
				Value:       append([]byte(nil), stored...),
				EntityID:    vertex.ID,
				EntityClass: "vertex",
			}
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
			count++
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// BackfillEdgePropertyIndex writes index entries for existing edges that match
// (tenant, edgeType, property). This is intended for startup migrations.
func (e *Executor) BackfillEdgePropertyIndex(ctx context.Context, tenant, edgeType, property string) (int, error) {
	if e == nil || e.store == nil {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	tenant = strings.TrimSpace(tenant)
	edgeType = strings.TrimSpace(edgeType)
	property = strings.TrimSpace(property)
	if tenant == "" || edgeType == "" || property == "" {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "tenant, edge type, and property are required", nil)
	}

	vertexIDs := []string{}
	if err := e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if vertex == nil || strings.TrimSpace(vertex.ID) == "" {
				return nil
			}
			vertexIDs = append(vertexIDs, vertex.ID)
			return nil
		})
	}); err != nil {
		return 0, err
	}

	count := 0
	for _, vertexID := range vertexIDs {
		written, err := e.backfillEdgePropertyIndexForVertex(ctx, tenant, vertexID, edgeType, property)
		if err != nil {
			return count, err
		}
		count += written
	}
	return count, nil
}

func (e *Executor) backfillEdgePropertyIndexForVertex(ctx context.Context, tenant, vertexID, edgeType, property string) (int, error) {
	attemptCount := 0
	err := e.store.Update(ctx, func(tx graph.Tx) error {
		return tx.ScanOutEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge == nil || strings.TrimSpace(edge.ID) == "" || edge.Properties == nil {
				return nil
			}
			stored, ok := edge.Properties[property]
			if !ok {
				return nil
			}
			entry := &graph.PropertyIndexEntry{
				Tenant:      tenant,
				Schema:      edgeType,
				Property:    property,
				Value:       append([]byte(nil), stored...),
				EntityID:    edge.ID,
				EntityClass: "edge",
			}
			if err := tx.PutPropertyIndex(ctx, entry); err != nil {
				return err
			}
			attemptCount++
			return nil
		})
	})
	if err == nil {
		return attemptCount, nil
	}
	if !isMaxWriteBatchExceededError(err) {
		return 0, err
	}
	return e.backfillEdgePropertyIndexForVertexSingleEntryWrites(ctx, tenant, vertexID, edgeType, property)
}

func (e *Executor) backfillEdgePropertyIndexForVertexSingleEntryWrites(ctx context.Context, tenant, vertexID, edgeType, property string) (int, error) {
	type edgeIndexRecord struct {
		edgeID string
		value  []byte
	}
	records := []edgeIndexRecord{}
	err := e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanOutEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge == nil || strings.TrimSpace(edge.ID) == "" || edge.Properties == nil {
				return nil
			}
			stored, ok := edge.Properties[property]
			if !ok {
				return nil
			}
			records = append(records, edgeIndexRecord{edgeID: edge.ID, value: append([]byte(nil), stored...)})
			return nil
		})
	})
	if err != nil {
		return 0, err
	}

	count := 0
	for _, record := range records {
		entry := &graph.PropertyIndexEntry{
			Tenant:      tenant,
			Schema:      edgeType,
			Property:    property,
			Value:       record.value,
			EntityID:    record.edgeID,
			EntityClass: "edge",
		}
		if err := e.store.Update(ctx, func(tx graph.Tx) error {
			return tx.PutPropertyIndex(ctx, entry)
		}); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func isMaxWriteBatchExceededError(err error) bool {
	if err == nil || !graph.IsKind(err, graph.ErrKindInvalidInput) {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "max_write_batch_bytes")
}
