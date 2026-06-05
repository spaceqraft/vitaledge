package runtime

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	runtimestorage "github.com/paegun/vitaledge/internal/cypher/runtime/storage"
	"github.com/paegun/vitaledge/internal/graph"
)

// ApplyWriteEvents applies runtime write events through the graph transaction boundary.
func ApplyWriteEvents(ctx context.Context, tx graph.Tx, tenant string, events []operators.WriteEvent) error {
	if tx == nil || len(events) == 0 {
		return nil
	}
	return applyWriteEventsToSink(ctx, runtimestorage.NewTxAdapter(tx), tenant, events)
}

func applyWriteEventsToSink(ctx context.Context, sink runtimestorage.WriteSink, tenant string, events []operators.WriteEvent) error {
	if sink == nil || len(events) == 0 {
		return nil
	}
	tenant = strings.TrimSpace(tenant)
	for _, event := range events {
		switch event.MutationType {
		case operators.MutationTypeVertex:
			if err := applyVertexWriteEvent(ctx, sink, tenant, event); err != nil {
				return err
			}
		case operators.MutationTypeEdge:
			if err := applyEdgeWriteEvent(ctx, sink, tenant, event); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyVertexWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent) error {
	if event.Vertex == nil {
		return nil
	}
	vertexID := resolveEntityID(event.Vertex.Var, event.Vertex.IDParam, event.Bindings, event.ResolvedParams)
	if vertexID == "" {
		return nil
	}
	return sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: append([]string(nil), event.Vertex.Labels...)})
}

func applyEdgeWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent) error {
	if event.Edge == nil {
		return nil
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	if leftID == "" || rightID == "" || edgeType == "" {
		return nil
	}
	if len(event.Edge.LeftLabels) > 0 {
		if err := sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: leftID, Labels: append([]string(nil), event.Edge.LeftLabels...)}); err != nil {
			return err
		}
	}
	if len(event.Edge.RightLabels) > 0 {
		if err := sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: rightID, Labels: append([]string(nil), event.Edge.RightLabels...)}); err != nil {
			return err
		}
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	edgeID := fmt.Sprintf("%s|%s|%s", srcID, edgeType, dstID)
	return sink.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID})
}

func resolveEntityID(varName, idParam string, bindings map[string]any, resolvedParams map[string]any) string {
	varName = strings.TrimSpace(varName)
	if varName != "" {
		if value, ok := bindings[varName]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
	}
	idParam = strings.TrimSpace(idParam)
	if idParam != "" {
		if value, ok := resolvedParams[idParam]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
	}
	return ""
}

func scalarString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		rv := reflect.ValueOf(typed)
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			return ""
		}
		return strings.TrimSpace(typed.String())
	default:
		text := strings.TrimSpace(fmt.Sprint(value))
		if text == "<nil>" {
			return ""
		}
		return text
	}
}
