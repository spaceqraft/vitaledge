package executor

import (
	"context"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

type propertyIndexEntryRecord struct {
	tenant      string
	schema      string
	property    string
	value       []byte
	entityID    string
	entityClass string
}

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

// DropPropertyIndex removes a property index from the runtime catalog and deletes
// matching persisted property-index entries.
func (e *Executor) DropPropertyIndex(ctx context.Context, tenant, schema, property string, ifExists bool) (dropped bool, deletedEntities int, err error) {
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

	if !e.indexCatalog.HasPropertyIndex(tenant, schema, property) {
		if ifExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindNotFound, "property index does not exist", nil)
	}

	entries, collectErr := e.collectPropertyIndexEntries(ctx, tenant, schema, property)
	if collectErr != nil {
		return false, 0, collectErr
	}
	deleted, deleteErr := e.deletePropertyIndexEntries(ctx, entries)
	if deleteErr != nil {
		return false, deleted, deleteErr
	}

	if finalizeErr := e.completePropertyIndexBuildJobs(ctx, tenant, schema, property); finalizeErr != nil {
		return false, deleted, finalizeErr
	}

	e.indexCatalog.RemovePropertyIndex(tenant, schema, property)
	return true, deleted, nil
}

// CreatePropertyIndexAsync registers a property index and enqueues a durable
// background backfill job so callers can return quickly on large datasets.
// Pending jobs are resumed across restarts by StartIndexBuildWorker.
func (e *Executor) CreatePropertyIndexAsync(ctx context.Context, tenant, schema, property string, ifNotExists bool) (created bool, indexedEntities int, err error) {
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

	if enqueueErr := e.enqueuePropertyIndexBuildJob(ctx, tenant, schema, property); enqueueErr != nil {
		e.indexCatalog.RemovePropertyIndex(tenant, schema, property)
		return false, 0, enqueueErr
	}

	return true, 0, nil
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

// DropEdgePropertyIndex removes an edge-property index from the runtime catalog,
// deletes persisted entries, and marks pending build jobs completed.
func (e *Executor) DropEdgePropertyIndex(ctx context.Context, tenant, edgeType, property string, ifExists bool) (dropped bool, deletedEntities int, err error) {
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

	if !e.indexCatalog.HasEdgePropertyIndex(tenant, edgeType, property) {
		if ifExists {
			return false, 0, nil
		}
		return false, 0, graph.NewError(graph.ErrKindNotFound, "edge property index does not exist", nil)
	}

	entries, collectErr := e.collectPropertyIndexEntries(ctx, tenant, edgeType, property)
	if collectErr != nil {
		return false, 0, collectErr
	}
	deleted, deleteErr := e.deletePropertyIndexEntries(ctx, entries)
	if deleteErr != nil {
		return false, deleted, deleteErr
	}

	if finalizeErr := e.completeEdgeIndexBuildJobs(ctx, tenant, edgeType, property); finalizeErr != nil {
		return false, deleted, finalizeErr
	}

	e.indexCatalog.RemoveEdgePropertyIndex(tenant, edgeType, property)
	return true, deleted, nil
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
		type labelVertexScannerTx interface {
			ScanVerticesByLabel(ctx context.Context, tenant, label string, limit int, fn func(*graph.Vertex) error) error
		}
		if scanner, ok := tx.(labelVertexScannerTx); ok {
			return scanner.ScanVerticesByLabel(ctx, tenant, schema, 0, func(vertex *graph.Vertex) error {
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
		}
		e.warnScanFallbackOnce(
			"BackfillPropertyIndex:vertex_scan",
			"BackfillPropertyIndex using ScanVertices fallback tenant=%s schema=%s property=%s",
			tenant,
			schema,
			property,
		)
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
		return tx.ScanOutEdgeSourceIDs(ctx, tenant, edgeType, 0, func(sourceID string) error {
			if strings.TrimSpace(sourceID) == "" {
				return nil
			}
			vertexIDs = append(vertexIDs, sourceID)
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
				EdgeSrcID:   edge.SrcID,
				EdgeDstID:   edge.DstID,
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
		srcID  string
		dstID  string
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
			records = append(records, edgeIndexRecord{edgeID: edge.ID, srcID: edge.SrcID, dstID: edge.DstID, value: append([]byte(nil), stored...)})
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
			EdgeSrcID:   record.srcID,
			EdgeDstID:   record.dstID,
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

func (e *Executor) collectPropertyIndexEntries(ctx context.Context, tenant, schema, property string) ([]propertyIndexEntryRecord, error) {
	entries := []propertyIndexEntryRecord{}
	err := e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, tenant, schema, property, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry == nil || strings.TrimSpace(entry.EntityID) == "" {
				return nil
			}
			entries = append(entries, propertyIndexEntryRecord{
				tenant:      tenant,
				schema:      schema,
				property:    property,
				value:       append([]byte(nil), entry.Value...),
				entityID:    entry.EntityID,
				entityClass: entry.EntityClass,
			})
			return nil
		})
	})
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return entries, nil
}

func (e *Executor) deletePropertyIndexEntries(ctx context.Context, entries []propertyIndexEntryRecord) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}

	attemptCount := 0
	err := e.store.Update(ctx, func(tx graph.Tx) error {
		for _, entry := range entries {
			if err := tx.DeletePropertyIndex(ctx, &graph.PropertyIndexEntry{
				Tenant:      entry.tenant,
				Schema:      entry.schema,
				Property:    entry.property,
				Value:       append([]byte(nil), entry.value...),
				EntityID:    entry.entityID,
				EntityClass: entry.entityClass,
			}); err != nil {
				return err
			}
			attemptCount++
		}
		return nil
	})
	if err == nil {
		return attemptCount, nil
	}
	if !isMaxWriteBatchExceededError(err) {
		return 0, err
	}

	deleted := 0
	for _, entry := range entries {
		if err := e.store.Update(ctx, func(tx graph.Tx) error {
			return tx.DeletePropertyIndex(ctx, &graph.PropertyIndexEntry{
				Tenant:      entry.tenant,
				Schema:      entry.schema,
				Property:    entry.property,
				Value:       append([]byte(nil), entry.value...),
				EntityID:    entry.entityID,
				EntityClass: entry.entityClass,
			})
		}); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (e *Executor) completeEdgeIndexBuildJobs(ctx context.Context, tenant, edgeType, property string) error {
	records, err := e.listAllEdgeIndexBuildJobRecords(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if !strings.EqualFold(strings.TrimSpace(record.Job.Tenant), tenant) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(record.Job.EdgeType), edgeType) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(record.Job.Property), property) {
			continue
		}
		if record.State.Completed {
			continue
		}
		if err := e.dequeueEdgeIndexBuildJob(ctx, record.Job, record.State); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) completePropertyIndexBuildJobs(ctx context.Context, tenant, schema, property string) error {
	records, err := e.listAllPropertyIndexBuildJobRecords(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if !strings.EqualFold(strings.TrimSpace(record.Job.Tenant), tenant) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(record.Job.Schema), schema) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(record.Job.Property), property) {
			continue
		}
		if record.State.Completed {
			continue
		}
		if err := e.dequeuePropertyIndexBuildJob(ctx, record.Job, record.State); err != nil {
			return err
		}
	}
	return nil
}
