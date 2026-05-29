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

	count := 0
	buildErr := e.store.Update(ctx, func(tx graph.Tx) error {
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
	if buildErr != nil {
		e.indexCatalog.RemovePropertyIndex(tenant, schema, property)
		return false, 0, buildErr
	}

	return true, count, nil
}
