package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/runtime/operators"
	runtimestorage "github.com/paegun/vitaledge/internal/cypher/runtime/storage"
	"github.com/paegun/vitaledge/internal/graph"
)

// ApplyWriteEvents applies runtime write events through the graph transaction boundary.
func ApplyWriteEvents(ctx context.Context, tx graph.Tx, tenant string, events []operators.WriteEvent) error {
	if len(events) == 0 {
		return nil
	}
	if tx == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "write transaction required", nil)
	}
	return applyWriteEventsToSink(ctx, tx, tenant, events)
}

func applyWriteEventsToSink(ctx context.Context, sink runtimestorage.WriteSink, tenant string, events []operators.WriteEvent) error {
	if len(events) == 0 {
		return nil
	}
	if sink == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "write sink required", nil)
	}
	tenant = strings.TrimSpace(tenant)
	lookup, _ := sink.(writeLookupTx)
	nextAutoVertexID := 0
	createEdgeCounts := map[string]int{}
	mergeEdgeCounts := map[string]int{}
	createPatternItemsCache := map[string][]string{}
	simpleMergeEdgeExistsCache := map[string]bool{}
	deletedVertexIDs := map[string]struct{}{}
	deletedEdgeIDs := map[string]struct{}{}
	hasDeleteClause := false
	mergeVertexCache := map[string]string{}
	for _, event := range events {
		kind := strings.ToUpper(strings.TrimSpace(event.Kind))
		if kind == "DELETE" || kind == "DETACH DELETE" {
			hasDeleteClause = true
			deletedVertexIDs["*"] = struct{}{}
			collectDeletedEntityIDsFromEventWithLookup(ctx, lookup, tenant, event, deletedVertexIDs, deletedEdgeIDs)
		}
	}
	for i := 0; i < len(events); i++ {
		event := events[i]
		kind := strings.ToUpper(strings.TrimSpace(event.Kind))
		if kind == "CREATE" {
			if err := applyCreateLikeWriteEvent(ctx, sink, tenant, event, &nextAutoVertexID, createEdgeCounts, createPatternItemsCache); err != nil {
				return err
			}
			continue
		}
		if kind == "MERGE" {
			if consumed, err := tryBatchApplyZeroTypeSimpleMergeEdges(ctx, sink, tenant, events, i, hasDeleteClause); err != nil {
				return err
			} else if consumed > 0 {
				i += consumed - 1
				continue
			}
			if handled, err := tryApplySimpleDirectedMergeEdgeFastPath(ctx, sink, tenant, event, hasDeleteClause, simpleMergeEdgeExistsCache); err != nil {
				return err
			} else if handled {
				continue
			}
			if err := applyMergeWriteEvent(ctx, sink, tenant, event, &nextAutoVertexID, deletedVertexIDs, deletedEdgeIDs, mergeVertexCache, mergeEdgeCounts, hasDeleteClause); err != nil {
				return err
			}
			continue
		}
		if kind == "DELETE" || kind == "DETACH DELETE" {
			collectDeletedEntityIDsFromEventWithLookup(ctx, lookup, tenant, event, deletedVertexIDs, deletedEdgeIDs)
			if err := applyDeleteWriteEvent(ctx, sink, tenant, event); err != nil {
				return err
			}
			continue
		}
		switch event.MutationType {
		case operators.MutationTypeVertex:
			if err := applyVertexWriteEvent(ctx, sink, tenant, event, &nextAutoVertexID); err != nil {
				return err
			}
		case operators.MutationTypeEdge:
			if err := applyEdgeWriteEvent(ctx, sink, tenant, event, &nextAutoVertexID, createEdgeCounts); err != nil {
				return err
			}
		case operators.MutationTypeProperty:
			if err := applyPropertyWriteEvent(ctx, sink, tenant, event); err != nil {
				return err
			}
		default:
			switch kind {
			case "CREATE":
				if err := applyCreateLikeWriteEvent(ctx, sink, tenant, event, &nextAutoVertexID, createEdgeCounts, createPatternItemsCache); err != nil {
					return err
				}
			case "SET", "REMOVE":
				if err := applyPropertyWriteEvent(ctx, sink, tenant, event); err != nil {
					return err
				}
			case "DELETE", "DETACH DELETE":
				collectDeletedEntityIDsFromEventWithLookup(ctx, lookup, tenant, event, deletedVertexIDs, deletedEdgeIDs)
				if err := applyDeleteWriteEvent(ctx, sink, tenant, event); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func tryApplySimpleDirectedMergeEdgeFastPath(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, hasDeleteClause bool, existsCache map[string]bool) (bool, error) {
	if hasDeleteClause || event.Edge == nil {
		return false, nil
	}
	if len(event.MergeOnCreate) != 0 || len(event.MergeOnMatch) != 0 {
		return false, nil
	}
	if strings.TrimSpace(event.Edge.Var) != "" {
		return false, nil
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	if edgeType == "" || len(event.Edge.LeftLabels) != 0 || len(event.Edge.RightLabels) != 0 {
		return false, nil
	}
	if isUndirectedMergeEdgePattern(event.Edge.Pattern) {
		return false, nil
	}
	lookup, _ := sink.(writeLookupTx)
	props, hasNullProp := resolveEdgeMutationProperties(ctx, lookup, tenant, event)
	if hasNullProp || len(props) != 0 {
		return false, nil
	}
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	if leftID == "" || rightID == "" {
		return false, nil
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	edgeID := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	if existsCache == nil {
		existsCache = map[string]bool{}
	}
	if exists, ok := existsCache[edgeID]; ok {
		if exists {
			return true, nil
		}
	} else {
		exists, err := simpleDirectedMergeEdgeExists(ctx, sink, lookup, tenant, srcID, dstID, edgeType, edgeID)
		if err != nil {
			return true, err
		}
		existsCache[edgeID] = exists
		if exists {
			return true, nil
		}
	}
	if fast, ok := sink.(appendOnlyEdgeWriter); ok {
		if err := fast.PutEdgeNew(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID}); err != nil {
			return true, err
		}
	} else {
		if err := sink.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID}); err != nil {
			return true, err
		}
	}
	existsCache[edgeID] = true
	return true, nil
}

func simpleDirectedMergeEdgeExists(ctx context.Context, sink runtimestorage.WriteSink, lookup writeLookupTx, tenant, srcID, dstID, edgeType, edgeID string) (bool, error) {
	if existsTx, ok := sink.(writeEdgeExistenceTx); ok {
		exists, err := existsTx.HasDirectedEdgeBetween(ctx, tenant, srcID, dstID, edgeType)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}
	if lookup != nil {
		edge, err := lookup.GetEdge(ctx, tenant, edgeID)
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				return false, nil
			}
			return false, err
		}
		return edge != nil, nil
	}
	return false, nil
}

func applyCreateLikeWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int, createEdgeCounts map[string]int, createPatternItemsCache map[string][]string) error {
	if strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE") {
		if err := applyCreateWriteEventPatterns(ctx, sink, tenant, event, nextAutoVertexID, createEdgeCounts, createPatternItemsCache); err != nil {
			return err
		}
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(event.Kind), "MERGE") {
		return applyMergeWriteEvent(ctx, sink, tenant, event, nextAutoVertexID, nil, nil, nil, nil, false)
	}
	return applyCreateLikeWriteEventFallback(ctx, sink, tenant, event, nextAutoVertexID, createEdgeCounts)
}

func applyCreateLikeWriteEventFallback(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int, createEdgeCounts map[string]int) error {
	if event.Vertex != nil {
		return applyVertexWriteEvent(ctx, sink, tenant, event, nextAutoVertexID)
	}
	if event.Edge != nil {
		return applyEdgeWriteEvent(ctx, sink, tenant, event, nextAutoVertexID, createEdgeCounts)
	}
	return nil
}

func applyMergeWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int, deletedVertexIDs map[string]struct{}, deletedEdgeIDs map[string]struct{}, mergeVertexCache map[string]string, mergeEdgeCounts map[string]int, forceUniqueMergeEdgeID bool) error {
	lookup, _ := sink.(writeLookupTx)
	bindings := cloneWriteBindings(event.Bindings)
	deletedEdgeIDs = mergeDeletedEdgeIDHints(deletedEdgeIDs, bindings)
	matched := false

	switch {
	case event.Edge != nil:
		matchState, edgeID, err := applyMergeEdgeWriteEvent(ctx, sink, lookup, tenant, event, deletedEdgeIDs, mergeEdgeCounts, forceUniqueMergeEdgeID)
		if err != nil {
			return err
		}
		matched = matchState
		bindMergeEdgeID(bindings, tenant, event, edgeID)
	case event.Vertex != nil:
		matchState, vertexID, err := applyMergeVertexWriteEvent(ctx, sink, lookup, tenant, event, nextAutoVertexID, deletedVertexIDs, mergeVertexCache)
		if err != nil {
			return err
		}
		matched = matchState
		bindMergeVertexID(bindings, event, vertexID)
	default:
		if err := applyCreateLikeWriteEventFallback(ctx, sink, tenant, event, nextAutoVertexID, nil); err != nil {
			return err
		}
	}

	actions := event.MergeOnCreate
	if matched {
		actions = event.MergeOnMatch
	}
	for _, action := range actions {
		if err := applyMergeActionClause(ctx, sink, tenant, event, bindings, action); err != nil {
			return err
		}
	}
	return nil
}

func applyMergeEdgeWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, lookup writeLookupTx, tenant string, event operators.WriteEvent, deletedEdgeIDs map[string]struct{}, mergeEdgeCounts map[string]int, forceUniqueID bool) (bool, string, error) {
	if event.Edge == nil {
		return false, "", nil
	}
	props, hasNullProp := resolveEdgeMutationProperties(ctx, lookup, tenant, event)
	if hasNullProp {
		return false, "", graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	if leftID == "" || rightID == "" || edgeType == "" {
		return false, "", nil
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	edgeID := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	if lookup != nil {
		existing, err := lookup.GetEdge(ctx, tenant, edgeID)
		if err != nil {
			if !graph.IsKind(err, graph.ErrKindNotFound) {
				return false, "", err
			}
			existing = nil
		}
		if existing != nil {
			if _, deleted := deletedEdgeIDs[strings.TrimSpace(existing.ID)]; deleted {
				existing = nil
			} else if !mergeEdgeMatches(existing, props) {
				existing = nil
			}
		}
		if existing != nil {
			return true, existing.ID, nil
		}
	}
	undirected := isUndirectedMergeEdgePattern(event.Edge.Pattern)
	if existingEdgeID, err := findDirectedExistingEdgeID(ctx, sink, lookup, tenant, srcID, dstID, edgeType, props, deletedEdgeIDs); err != nil {
		return false, "", err
	} else if existingEdgeID != "" {
		return true, existingEdgeID, nil
	}
	if undirected {
		if existingEdgeID, err := findDirectedExistingEdgeID(ctx, sink, lookup, tenant, dstID, srcID, edgeType, props, deletedEdgeIDs); err != nil {
			return false, "", err
		} else if existingEdgeID != "" {
			return true, existingEdgeID, nil
		}
	}
	if len(event.Edge.LeftLabels) > 0 {
		if err := sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: leftID, Labels: append([]string(nil), event.Edge.LeftLabels...)}); err != nil {
			return false, "", err
		}
	}
	if len(event.Edge.RightLabels) > 0 {
		if err := sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: rightID, Labels: append([]string(nil), event.Edge.RightLabels...)}); err != nil {
			return false, "", err
		}
	}
	edgeID = allocateMergeEdgeID(ctx, lookup, tenant, srcID, edgeType, dstID, deletedEdgeIDs, mergeEdgeCounts, forceUniqueID)
	if fast, ok := sink.(appendOnlyEdgeWriter); ok {
		if err := fast.PutEdgeNew(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID, Properties: props}); err != nil {
			return false, "", err
		}
		return false, edgeID, nil
	}
	if err := sink.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID, Properties: props}); err != nil {
		return false, "", err
	}
	return false, edgeID, nil
}

func allocateMergeEdgeID(ctx context.Context, lookup writeLookupTx, tenant, srcID, edgeType, dstID string, deletedEdgeIDs map[string]struct{}, mergeEdgeCounts map[string]int, forceUnique bool) string {
	base := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	if mergeEdgeCounts == nil {
		mergeEdgeCounts = map[string]int{}
	}
	if deletedEdgeIDs == nil {
		deletedEdgeIDs = map[string]struct{}{}
	}
	candidate := base
	if forceUnique {
		mergeEdgeCounts[base] = mergeEdgeCounts[base] + 1
		candidate = fmt.Sprintf("%s|auto-merge-e-%d", base, mergeEdgeCounts[base])
	}
	for attempt := 0; attempt < 100000; attempt++ {
		if _, deleted := deletedEdgeIDs[candidate]; deleted {
			mergeEdgeCounts[base] = mergeEdgeCounts[base] + 1
			if forceUnique {
				candidate = fmt.Sprintf("%s|auto-merge-e-%d", base, mergeEdgeCounts[base])
			} else {
				candidate = fmt.Sprintf("%s|auto-e-%d", base, mergeEdgeCounts[base])
			}
			continue
		}
		if lookup != nil {
			if existing, err := lookup.GetEdge(ctx, tenant, candidate); err == nil && existing != nil {
				mergeEdgeCounts[base] = mergeEdgeCounts[base] + 1
				if forceUnique {
					candidate = fmt.Sprintf("%s|auto-merge-e-%d", base, mergeEdgeCounts[base])
				} else {
					candidate = fmt.Sprintf("%s|auto-e-%d", base, mergeEdgeCounts[base])
				}
				continue
			}
		}
		return candidate
	}
	return candidate
}

func applyMergeVertexWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, lookup writeLookupTx, tenant string, event operators.WriteEvent, nextAutoVertexID *int, deletedVertexIDs map[string]struct{}, mergeVertexCache map[string]string) (bool, string, error) {
	if event.Vertex == nil {
		return false, "", nil
	}
	props, hasNullProp := resolveMergeVertexMutationProperties(ctx, lookup, tenant, event)
	if hasNullProp {
		return false, "", graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}
	vertexID := resolveSyntheticWriteBindingVertexID(event.Vertex.Var, event.Bindings)
	if vertexID == "" {
		vertexID = resolveEntityID("", event.Vertex.IDParam, event.Bindings, event.ResolvedParams)
	}
	cacheKey := ""
	if vertexID == "" {
		cacheKey = mergeAnonymousVertexCacheKey(event.Vertex.Labels, props)
		if cacheKey != "" && mergeVertexCache != nil {
			if cachedID := strings.TrimSpace(mergeVertexCache[cacheKey]); cachedID != "" {
				return true, cachedID, nil
			}
		}
	}
	allowLookupMatch := true
	_, hasDeleteMarker := deletedVertexIDs["*"]
	if vertexID == "" && len(props) == 0 && hasDeleteMarker {
		allowLookupMatch = false
	}
	if lookup != nil {
		if allowLookupMatch && vertexID != "" {
			existing, err := lookup.GetVertex(ctx, tenant, vertexID)
			if err != nil {
				if !graph.IsKind(err, graph.ErrKindNotFound) {
					return false, "", err
				}
				existing = nil
			}
			if existing != nil {
				if _, deleted := deletedVertexIDs[strings.TrimSpace(existing.ID)]; deleted {
					existing = nil
				} else if !mergeVertexMatches(existing, event.Vertex.Labels, props) {
					existing = nil
					vertexID = ""
				}
			}
			if existing != nil {
				if cacheKey != "" && mergeVertexCache != nil {
					mergeVertexCache[cacheKey] = strings.TrimSpace(existing.ID)
				}
				return true, vertexID, nil
			}
		}
		if allowLookupMatch && vertexID == "" {
			matchedID, err := findAnonymousMergeVertexID(ctx, sink, tenant, event.Vertex.Labels, props, deletedVertexIDs)
			if err != nil {
				return false, "", err
			}
			if matchedID != "" {
				if cacheKey != "" && mergeVertexCache != nil {
					mergeVertexCache[cacheKey] = matchedID
				}
				return true, matchedID, nil
			}
		}
	}
	if vertexID == "" {
		vertexID, _ = allocateAnonymousVertexIDForTenantWithReserved(ctx, sink, tenant, nextAutoVertexID, deletedVertexIDs)
	}
	if err := sink.PutVertex(ctx, &graph.Vertex{
		Tenant:     tenant,
		ID:         vertexID,
		Labels:     append([]string(nil), event.Vertex.Labels...),
		Properties: props,
	}); err != nil {
		return false, "", err
	}
	if cacheKey != "" && mergeVertexCache != nil {
		mergeVertexCache[cacheKey] = vertexID
	}
	return false, vertexID, nil
}

func resolveSyntheticWriteBindingVertexID(varName string, bindings map[string]any) string {
	if len(bindings) == 0 {
		return ""
	}
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return ""
	}
	if value, ok := bindings[varName+".id"]; ok {
		if id := strings.TrimSpace(scalarString(value)); strings.HasPrefix(id, "__ve_write_v_") || strings.HasPrefix(id, "auto-mv-") {
			return id
		}
	}
	if value, ok := bindings[varName]; ok {
		if id := strings.TrimSpace(scalarString(value)); strings.HasPrefix(id, "__ve_write_v_") || strings.HasPrefix(id, "auto-mv-") {
			return id
		}
	}
	return ""
}

func mergeAnonymousVertexCacheKey(labels []string, props graph.PropertyMap) string {
	if len(labels) == 0 && len(props) == 0 {
		return ""
	}
	normLabels := append([]string(nil), labels...)
	for i := range normLabels {
		normLabels[i] = strings.TrimSpace(normLabels[i])
	}
	sort.Strings(normLabels)
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, strings.TrimSpace(key))
	}
	sort.Strings(keys)
	b := strings.Builder{}
	b.WriteString(strings.Join(normLabels, ":"))
	b.WriteString("|")
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(string(props[key]))
		b.WriteString(";")
	}
	return b.String()
}

func bindMergeVertexID(bindings map[string]any, event operators.WriteEvent, vertexID string) {
	if event.Vertex == nil || bindings == nil {
		return
	}
	varName := strings.TrimSpace(event.Vertex.Var)
	if varName == "" {
		return
	}
	if vertexID == "" {
		return
	}
	bindings[varName] = vertexID
	bindings[varName+".id"] = vertexID
}

func bindMergeEdgeID(bindings map[string]any, tenant string, event operators.WriteEvent, edgeID string) {
	if event.Edge == nil || bindings == nil {
		return
	}
	varName := strings.TrimSpace(event.Edge.Var)
	if varName == "" {
		return
	}
	if strings.TrimSpace(edgeID) != "" {
		bindings[varName] = strings.TrimSpace(edgeID)
		return
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	if leftID == "" || rightID == "" || edgeType == "" {
		return
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	edgeID = fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	bindings[varName] = edgeID
}

func applyMergeActionClause(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, bindings map[string]any, clause string) error {
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return nil
	}
	upper := strings.ToUpper(clause)
	if strings.HasPrefix(upper, "ON CREATE SET") {
		clause = strings.TrimSpace(clause[len("ON CREATE"):])
	} else if strings.HasPrefix(upper, "ON MATCH SET") {
		clause = strings.TrimSpace(clause[len("ON MATCH"):])
	}
	kind := "SET"
	if strings.HasPrefix(strings.ToUpper(clause), "SET") {
		clause = strings.TrimSpace(stripWritePrefix(clause, "SET"))
	}
	actionEvent := operators.WriteEvent{Kind: kind, Bindings: bindings, ResolvedParams: event.ResolvedParams}
	return applySetWriteBody(ctx, sink, tenant, actionEvent, clause)
}

func applyVertexWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int) error {
	if event.Vertex == nil {
		return nil
	}
	lookup, _ := sink.(writeLookupTx)
	props := resolveVertexMutationProperties(ctx, lookup, tenant, event)
	vertexID := resolveEntityID(event.Vertex.Var, event.Vertex.IDParam, event.Bindings, event.ResolvedParams)
	skipLookupMerge := false
	if vertexID == "" && isCreateLikeWriteKind(event.Kind) {
		vertexID, skipLookupMerge = allocateAnonymousVertexIDForTenantChecked(ctx, sink, tenant, nextAutoVertexID)
	}
	if vertexID == "" {
		return nil
	}
	mergeExisting := !strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE")
	return putCreateLikeVertex(ctx, sink, tenant, vertexID, event.Vertex.Labels, props, skipLookupMerge, mergeExisting)
}

func resolveVertexMutationProperties(ctx context.Context, lookup writeLookupTx, tenant string, event operators.WriteEvent) graph.PropertyMap {
	if event.Vertex == nil {
		return nil
	}
	body, ok := extractPropertyMapBody(event.Vertex.Pattern)
	if !ok || strings.TrimSpace(body) == "" {
		return nil
	}

	props := graph.PropertyMap{}
	for _, pair := range splitTopLevelByComma(body) {
		key, expr, ok := splitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		expr = strings.TrimSpace(expr)
		value, ok := resolveWritePropertyValueWithBindings(expr, event.ResolvedParams, event.Bindings)
		if ok {
			if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
				ok = false
			}
		}
		if !ok {
			value, ok = resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, expr, event.ResolvedParams, event.Bindings)
		}
		if !ok {
			continue
		}
		if value == nil {
			continue
		}
		props[key] = encodePropertyValue(value)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

func resolveMergeVertexMutationProperties(ctx context.Context, lookup writeLookupTx, tenant string, event operators.WriteEvent) (graph.PropertyMap, bool) {
	if event.Vertex == nil {
		return nil, false
	}
	body, ok := extractPropertyMapBody(event.Vertex.Pattern)
	if !ok || strings.TrimSpace(body) == "" {
		return nil, false
	}

	props := graph.PropertyMap{}
	hasNull := false
	for _, pair := range splitTopLevelByComma(body) {
		key, expr, ok := splitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value, ok := resolveWritePropertyValueWithBindings(strings.TrimSpace(expr), event.ResolvedParams, event.Bindings)
		expr = strings.TrimSpace(expr)
		if ok {
			if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
				ok = false
			}
		}
		if !ok {
			value, ok = resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, strings.TrimSpace(expr), event.ResolvedParams, event.Bindings)
		}
		if !ok {
			continue
		}
		if value == nil {
			hasNull = true
			continue
		}
		props[key] = encodePropertyValue(value)
	}
	if len(props) == 0 {
		return nil, hasNull
	}
	return props, hasNull
}

func resolveWritePropertyValueFromBindingEntity(ctx context.Context, lookup writeLookupTx, tenant, expr string, params map[string]any, bindings map[string]any) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if value, ok := resolveWritePropertyValueWithBindings(expr, params, bindings); ok {
		if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
			// Keep going: this is an unresolved path-like token.
		} else {
			return value, true
		}
	}
	if leftExpr, op, rightExpr, ok := splitWriteTopLevelBinary(expr, "+-"); ok {
		left, leftOK := resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, leftExpr, params, bindings)
		if !leftOK {
			return nil, false
		}
		right, rightOK := resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, rightExpr, params, bindings)
		if !rightOK {
			return nil, false
		}
		return applyWriteBinaryValue(op, left, right)
	}
	if leftExpr, op, rightExpr, ok := splitWriteTopLevelBinary(expr, "*/"); ok {
		left, leftOK := resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, leftExpr, params, bindings)
		if !leftOK {
			return nil, false
		}
		right, rightOK := resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, rightExpr, params, bindings)
		if !rightOK {
			return nil, false
		}
		return applyWriteBinaryValue(op, left, right)
	}
	if lookup == nil || len(bindings) == 0 {
		return nil, false
	}
	parts := splitDeleteOperandPath(expr)
	if len(parts) < 2 {
		return nil, false
	}
	binding, ok := bindings[parts[0]]
	if !ok {
		return nil, false
	}
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return nil, false
	}
	var current any
	switch {
	case vertex != nil:
		current = vertex
	case edge != nil:
		current = edge
	case id != "":
		current = map[string]any{"id": id}
	default:
		return nil, false
	}
	for _, part := range parts[1:] {
		part = resolveDeleteOperandPathToken(part, params)
		next, ok := lookupDeleteOperandPart(current, part)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

var writeSetPropertyAssignmentRE = regexp.MustCompile(`^\(?\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)?\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$`)
var writeSetLabelAssignmentRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[A-Za-z_][A-Za-z0-9_]*)*)$`)
var writeSetMapReplaceBindingRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)$`)
var writeSetMapReplaceLiteralRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\{.*\})$`)
var writeSetMapAppendRE = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*\+=\s*(\{.*\})$`)
var writeRemovePropertyAssignmentRE = regexp.MustCompile(`^\(?\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)?\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)$`)

type writeLookupTx interface {
	GetVertex(context.Context, string, string) (*graph.Vertex, error)
	GetEdge(context.Context, string, string) (*graph.Edge, error)
	DeleteVertex(context.Context, string, string) error
	DeleteVertexDetach(context.Context, string, string) error
	DeleteEdge(context.Context, string, string) error
	PatchVertexProperties(context.Context, string, string, graph.PropertyMap, []string) error
	PatchEdgeProperties(context.Context, string, string, graph.PropertyMap, []string) error
}

type writeVertexScanner interface {
	ScanVertices(context.Context, string, int, func(*graph.Vertex) error) error
}

type writePropertyIndexScanner interface {
	ScanPropertyIndex(context.Context, string, string, string, []byte, int, func(*graph.PropertyIndexEntry) error) error
}

type writeEdgeScanner interface {
	ScanVertices(context.Context, string, int, func(*graph.Vertex) error) error
	ScanOutEdges(context.Context, string, string, string, int, func(*graph.Edge) error) error
}

type writeEdgeExistenceTx interface {
	HasDirectedEdgeBetween(context.Context, string, string, string, string) (bool, error)
}

type appendOnlyVertexWriter interface {
	PutVertexNew(context.Context, *graph.Vertex) error
}

type appendOnlyEdgeWriter interface {
	PutEdgeNew(context.Context, *graph.Edge) error
}

type appendOnlyEdgeBatchWriter interface {
	PutEdgeNewBatch(context.Context, []*graph.Edge) error
}

type writeStatsSnapshotTx interface {
	GetStatsSnapshot(context.Context, string) (*graph.StatsSnapshot, error)
}

type simpleMergeEdgeSpec struct {
	edgeType string
	edge     *graph.Edge
}

func tryBatchApplyZeroTypeSimpleMergeEdges(ctx context.Context, sink runtimestorage.WriteSink, tenant string, events []operators.WriteEvent, start int, hasDeleteClause bool) (int, error) {
	if hasDeleteClause || start < 0 || start >= len(events) {
		return 0, nil
	}
	statsTx, ok := sink.(writeStatsSnapshotTx)
	if !ok {
		return 0, nil
	}
	firstSpec, ok := simpleAnonymousMergeEdgeSpecForBatch(ctx, sink, tenant, events[start])
	if !ok {
		return 0, nil
	}
	snapshot, err := statsTx.GetStatsSnapshot(ctx, tenant)
	if err != nil {
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return 0, err
		}
		snapshot = nil
	}
	normalizedType := strings.TrimSpace(firstSpec.edgeType)
	if normalizedType == "" {
		normalizedType = "UNTYPED"
	}
	if snapshot != nil && snapshot.EdgeCounts != nil && snapshot.EdgeCounts[normalizedType] > 0 {
		return 0, nil
	}
	batchWriter, hasBatchWriter := sink.(appendOnlyEdgeBatchWriter)
	fastWriter, hasFastWriter := sink.(appendOnlyEdgeWriter)
	if !hasBatchWriter && !hasFastWriter {
		return 0, nil
	}
	edges := make([]*graph.Edge, 0)
	seen := map[string]struct{}{}
	consumed := 0
	for i := start; i < len(events); i++ {
		event := events[i]
		if strings.ToUpper(strings.TrimSpace(event.Kind)) != "MERGE" {
			break
		}
		spec, ok := simpleAnonymousMergeEdgeSpecForBatch(ctx, sink, tenant, event)
		if !ok || !strings.EqualFold(strings.TrimSpace(spec.edgeType), firstSpec.edgeType) {
			break
		}
		consumed++
		if spec.edge == nil || strings.TrimSpace(spec.edge.ID) == "" {
			continue
		}
		if _, exists := seen[spec.edge.ID]; exists {
			continue
		}
		seen[spec.edge.ID] = struct{}{}
		edges = append(edges, spec.edge)
	}
	if consumed == 0 {
		return 0, nil
	}
	if len(edges) == 0 {
		return consumed, nil
	}
	if hasBatchWriter {
		if err := batchWriter.PutEdgeNewBatch(ctx, edges); err != nil {
			return 0, err
		}
		return consumed, nil
	}
	for _, edge := range edges {
		if err := fastWriter.PutEdgeNew(ctx, edge); err != nil {
			return 0, err
		}
	}
	return consumed, nil
}

func simpleAnonymousMergeEdgeSpecForBatch(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent) (simpleMergeEdgeSpec, bool) {
	if event.Edge == nil {
		return simpleMergeEdgeSpec{}, false
	}
	if len(event.MergeOnCreate) != 0 || len(event.MergeOnMatch) != 0 {
		return simpleMergeEdgeSpec{}, false
	}
	if strings.TrimSpace(event.Edge.Var) != "" {
		return simpleMergeEdgeSpec{}, false
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	if edgeType == "" || len(event.Edge.LeftLabels) != 0 || len(event.Edge.RightLabels) != 0 || isUndirectedMergeEdgePattern(event.Edge.Pattern) {
		return simpleMergeEdgeSpec{}, false
	}
	lookup, _ := sink.(writeLookupTx)
	props, hasNullProp := resolveEdgeMutationProperties(ctx, lookup, tenant, event)
	if hasNullProp || len(props) != 0 {
		return simpleMergeEdgeSpec{}, false
	}
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	if leftID == "" || rightID == "" {
		return simpleMergeEdgeSpec{}, false
	}
	srcID := leftID
	dstID := rightID
	if event.Edge.Reverse {
		srcID = rightID
		dstID = leftID
	}
	edgeID := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	return simpleMergeEdgeSpec{
		edgeType: edgeType,
		edge:     &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID},
	}, true
}

var errMergeVertexMatchFound = errors.New("merge vertex match found")

type createVertexPattern struct {
	varName string
	idParam string
	labels  []string
	props   graph.PropertyMap
}

type createEdgePattern struct {
	reverse  bool
	typeName string
	props    graph.PropertyMap
}

func applyCreateWriteEventPatterns(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int, createEdgeCounts map[string]int, createPatternItemsCache map[string][]string) error {
	cacheKey := strings.TrimSpace(event.Pattern)
	if cacheKey == "" {
		cacheKey = strings.TrimSpace(event.Raw)
	}
	items, cached := createPatternItemsCache[cacheKey]
	if !cached {
		body, ok := extractCreateLikeClauseBody(event)
		if !ok || strings.TrimSpace(body) == "" {
			return applyCreateLikeWriteEventFallback(ctx, sink, tenant, event, nextAutoVertexID, createEdgeCounts)
		}
		body = stripCypherLineComments(body)
		items = splitTopLevelByComma(body)
		if createPatternItemsCache != nil {
			createPatternItemsCache[cacheKey] = items
		}
	}

	bindings := cloneWriteBindings(event.Bindings)
	lookup, _ := sink.(writeLookupTx)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if err := applyCreatePatternItem(ctx, sink, tenant, event, item, bindings, lookup, nextAutoVertexID, createEdgeCounts); err != nil {
			return err
		}
	}
	return nil
}

func stripCypherLineComments(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i > 0 && raw[i-1] == '\\' {
				b.WriteByte(ch)
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				b.WriteByte(ch)
				i++
				b.WriteByte(raw[i])
				continue
			}
			inSingle = !inSingle
			b.WriteByte(ch)
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				b.WriteByte(ch)
				continue
			}
			inDouble = !inDouble
			b.WriteByte(ch)
			continue
		}
		if !inSingle && !inDouble && ch == '/' && i+1 < len(raw) && raw[i+1] == '/' {
			for i < len(raw) && raw[i] != '\n' {
				i++
			}
			if i < len(raw) && raw[i] == '\n' {
				b.WriteByte('\n')
			}
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func applyCreatePatternItem(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, item string, bindings map[string]any, lookup writeLookupTx, nextAutoVertexID *int, createEdgeCounts map[string]int) error {
	vertexes, edges, ok := parseCreatePatternItem(ctx, lookup, tenant, item, event.ResolvedParams, bindings)
	if !ok || len(vertexes) == 0 {
		return nil
	}

	currentID, err := ensureCreatePatternVertex(ctx, sink, tenant, event, vertexes[0], bindings, nextAutoVertexID)
	if err != nil {
		return err
	}
	if currentID == "" {
		return nil
	}

	for idx, edgePattern := range edges {
		nextID, err := ensureCreatePatternVertex(ctx, sink, tenant, event, vertexes[idx+1], bindings, nextAutoVertexID)
		if err != nil {
			return err
		}
		if nextID == "" {
			currentID = nextID
			continue
		}
		if err := putCreatePatternEdge(ctx, sink, tenant, currentID, nextID, edgePattern, createEdgeCounts); err != nil {
			return err
		}
		currentID = nextID
	}
	return nil
}

func ensureCreatePatternVertex(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, vertex createVertexPattern, bindings map[string]any, nextAutoVertexID *int) (string, error) {
	vertexID := resolveCreatePatternVertexID(vertex, event.ResolvedParams, bindings)
	skipLookupMerge := false
	if vertexID == "" && isCreateLikeWriteKind(event.Kind) {
		vertexID, skipLookupMerge = allocateAnonymousVertexIDForTenantChecked(ctx, sink, tenant, nextAutoVertexID)
	}
	if vertexID == "" {
		return "", nil
	}

	writeVertex := shouldWriteCreatePatternVertex(vertex, bindings)
	if !writeVertex {
		if lookup, ok := sink.(writeLookupTx); ok {
			existing, err := lookup.GetVertex(ctx, tenant, vertexID)
			if err != nil {
				if !graph.IsKind(err, graph.ErrKindNotFound) {
					return "", err
				}
				existing = nil
			}
			if existing == nil {
				writeVertex = true
			}
		}
	}

	if writeVertex {
		if err := putCreateLikeVertex(ctx, sink, tenant, vertexID, vertex.labels, vertex.props, skipLookupMerge, false); err != nil {
			return "", err
		}
	}

	bindCreatePatternVertex(bindings, vertex.varName, vertexID)
	if varName := strings.TrimSpace(vertex.varName); varName != "" && vertex.props != nil {
		if rawID, hasID := vertex.props["id"]; hasID {
			bindings[varName+".id"] = decodeWriteStoredPropertyValue(rawID)
		}
	}
	return vertexID, nil
}

func putCreatePatternEdge(ctx context.Context, sink runtimestorage.WriteSink, tenant, leftID, rightID string, edge createEdgePattern, createEdgeCounts map[string]int) error {
	edgeType := strings.TrimSpace(edge.typeName)
	if leftID == "" || rightID == "" || edgeType == "" {
		return nil
	}
	srcID := leftID
	dstID := rightID
	if edge.reverse {
		srcID = rightID
		dstID = leftID
	}
	lookup, _ := sink.(writeLookupTx)
	edgeID := allocateCreateEdgeIDWithLookup(ctx, lookup, tenant, srcID, edgeType, dstID, createEdgeCounts)
	if fast, ok := sink.(appendOnlyEdgeWriter); ok {
		return fast.PutEdgeNew(ctx, &graph.Edge{
			Tenant:     tenant,
			ID:         edgeID,
			Type:       edgeType,
			SrcID:      srcID,
			DstID:      dstID,
			Properties: clonePropertyMap(edge.props),
		})
	}
	return sink.PutEdge(ctx, &graph.Edge{
		Tenant:     tenant,
		ID:         edgeID,
		Type:       edgeType,
		SrcID:      srcID,
		DstID:      dstID,
		Properties: clonePropertyMap(edge.props),
	})
}

func extractCreateLikeClauseBody(event operators.WriteEvent) (string, bool) {
	kind := strings.ToUpper(strings.TrimSpace(event.Kind))
	pattern := strings.TrimSpace(event.Pattern)
	raw := strings.TrimSpace(event.Raw)
	if pattern != "" && (kind == "CREATE" || kind == "MERGE") {
		return pattern, true
	}
	if kind == "CREATE" {
		return extractClauseBodyFromRaw(raw, "CREATE")
	}
	if kind == "MERGE" {
		if pattern := strings.TrimSpace(event.MergePattern); pattern != "" {
			return pattern, true
		}
		return extractClauseBodyFromRaw(raw, "MERGE")
	}
	return "", false
}

func extractClauseBodyFromRaw(raw, kind string) (string, bool) {
	raw = strings.TrimSpace(raw)
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if raw == "" || kind == "" {
		return "", false
	}

	upper := strings.ToUpper(raw)
	idx := -1
	if strings.HasPrefix(upper, kind) {
		idx = 0
	} else {
		needle := "\n" + kind
		if lineIdx := strings.Index(upper, needle); lineIdx >= 0 {
			idx = lineIdx + 1
		} else {
			if spacedIdx := strings.Index(upper, " "+kind+" "); spacedIdx >= 0 {
				idx = spacedIdx + 1
			} else {
				body := strings.TrimSpace(raw)
				if body == "" {
					return "", false
				}
				if cut := nextClauseStart(body); cut >= 0 {
					body = strings.TrimSpace(body[:cut])
				}
				if body == "" {
					return "", false
				}
				return body, true
			}
		}
	}
	body := strings.TrimSpace(raw[idx+len(kind):])
	if body == "" {
		return "", false
	}
	if cut := nextClauseStart(body); cut >= 0 {
		body = strings.TrimSpace(body[:cut])
	}
	if body == "" {
		return "", false
	}
	return body, true
}

func nextClauseStart(body string) int {
	upper := strings.ToUpper(body)
	separators := []string{
		"\nWITH ", "\nRETURN ", "\nMATCH ", "\nOPTIONAL MATCH ", "\nUNWIND ", "\nCALL ",
		"\nCREATE ", "\nMERGE ", "\nSET ", "\nREMOVE ", "\nDELETE ", "\nDETACH DELETE ",
		" WITH ", " RETURN ", " MATCH ", " OPTIONAL MATCH ", " UNWIND ", " CALL ",
		" CREATE ", " MERGE ", " SET ", " REMOVE ", " DELETE ", " DETACH DELETE ",
	}
	best := -1
	for _, sep := range separators {
		if idx := strings.Index(upper, sep); idx >= 0 {
			if best < 0 || idx < best {
				best = idx
			}
		}
	}
	return best
}

func resolveEdgeMutationProperties(ctx context.Context, lookup writeLookupTx, tenant string, event operators.WriteEvent) (graph.PropertyMap, bool) {
	if event.Edge == nil {
		return nil, false
	}
	body, ok := extractPropertyMapBody(event.Edge.Pattern)
	if !ok || strings.TrimSpace(body) == "" {
		return nil, false
	}
	props := graph.PropertyMap{}
	hasNull := false
	for _, pair := range splitTopLevelByComma(body) {
		key, expr, ok := splitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		expr = strings.TrimSpace(expr)
		value, ok := resolveWritePropertyValueWithBindings(expr, event.ResolvedParams, event.Bindings)
		if ok {
			if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
				ok = false
			}
		}
		if !ok {
			value, ok = resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, expr, event.ResolvedParams, event.Bindings)
		}
		if !ok {
			continue
		}
		if value == nil {
			hasNull = true
			continue
		}
		props[key] = encodePropertyValue(value)
	}
	if len(props) == 0 {
		return nil, hasNull
	}
	return props, hasNull
}

func cloneWriteBindings(bindings map[string]any) map[string]any {
	if len(bindings) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(bindings))
	for key, value := range bindings {
		out[key] = value
	}
	return out
}

func mergeDeletedEdgeIDHints(base map[string]struct{}, bindings map[string]any) map[string]struct{} {
	if base == nil {
		base = map[string]struct{}{}
	}
	if len(bindings) == 0 {
		return base
	}
	raw, ok := bindings["ve_deleted_edge_ids"]
	if !ok || raw == nil {
		return base
	}
	appendID := func(value any) {
		if id := strings.TrimSpace(fmt.Sprint(value)); id != "" {
			base[id] = struct{}{}
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, id := range typed {
			appendID(id)
		}
	case []any:
		for _, id := range typed {
			appendID(id)
		}
	default:
		appendID(typed)
	}
	return base
}

func parseCreatePatternItem(ctx context.Context, lookup writeLookupTx, tenant, item string, params map[string]any, bindings map[string]any) ([]createVertexPattern, []createEdgePattern, bool) {
	item = strings.TrimSpace(item)
	if item == "" {
		return nil, nil, false
	}

	vertexes := []createVertexPattern{}
	edges := []createEdgePattern{}
	i := 0
	for {
		segment, next, ok := readBalancedSegment(item, i, '(', ')')
		if !ok {
			break
		}
		vertex, ok := parseCreateVertexPattern(ctx, lookup, tenant, segment, params, bindings)
		if !ok {
			return nil, nil, false
		}
		vertexes = append(vertexes, vertex)
		i = next

		nextVertexStart, ok := findNextCreateVertexStart(item, i)
		if !ok {
			break
		}
		edge, ok := parseCreateEdgePattern(ctx, lookup, tenant, item[i:nextVertexStart], params, bindings)
		if !ok {
			return nil, nil, false
		}
		edges = append(edges, edge)
		i = nextVertexStart
	}

	if len(vertexes) == 0 {
		return nil, nil, false
	}
	if len(edges) != len(vertexes)-1 {
		return nil, nil, false
	}
	return vertexes, edges, true
}

func readBalancedSegment(raw string, start int, open, close byte) (string, int, bool) {
	start = skipWriteWhitespace(raw, start)
	if start >= len(raw) || raw[start] != open {
		return "", start, false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return strings.TrimSpace(raw[start : i+1]), i + 1, true
			}
		}
	}
	return "", start, false
}

func findNextCreateVertexStart(raw string, start int) (int, bool) {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i, true
			}
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
	}
	return 0, false
}

func parseCreateVertexPattern(ctx context.Context, lookup writeLookupTx, tenant, segment string, params map[string]any, bindings map[string]any) (createVertexPattern, bool) {
	segment = strings.TrimSpace(segment)
	if len(segment) < 2 || segment[0] != '(' || segment[len(segment)-1] != ')' {
		return createVertexPattern{}, false
	}
	body := strings.TrimSpace(segment[1 : len(segment)-1])
	return createVertexPattern{
		varName: parseCreateVertexVarName(body),
		idParam: parseCreatePatternIDParam(segment),
		labels:  parseCreateVertexLabels(body),
		props:   parseCreatePatternPropertyMap(ctx, lookup, tenant, segment, params, bindings),
	}, true
}

func parseCreateEdgePattern(ctx context.Context, lookup writeLookupTx, tenant, connector string, params map[string]any, bindings map[string]any) (createEdgePattern, bool) {
	connector = strings.TrimSpace(connector)
	if connector == "" {
		return createEdgePattern{}, false
	}
	open := strings.IndexByte(connector, '[')
	close := strings.LastIndexByte(connector, ']')
	if open < 0 || close <= open {
		return createEdgePattern{}, false
	}
	inside := strings.TrimSpace(connector[open+1 : close])
	typeName := parseCreateEdgeTypeName(inside)
	if typeName == "" {
		return createEdgePattern{}, false
	}
	return createEdgePattern{
		reverse:  detectCreateEdgeReverse(connector),
		typeName: typeName,
		props:    parseCreatePatternPropertyMap(ctx, lookup, tenant, inside, params, bindings),
	}, true
}

func parseCreateVertexVarName(body string) string {
	body = strings.TrimSpace(body)
	if body == "" || body[0] == ':' || body[0] == '{' {
		return ""
	}
	end := 0
	for end < len(body) && isCreateIdentChar(body[end], end == 0) {
		end++
	}
	if end == 0 {
		return ""
	}
	return strings.TrimSpace(body[:end])
}

func parseCreateVertexLabels(body string) []string {
	head := createPatternHeadBeforeProps(body)
	if head == "" {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	for idx := 0; idx < len(head); idx++ {
		if head[idx] != ':' {
			continue
		}
		start := idx + 1
		end := start
		for end < len(head) && isCreateIdentChar(head[end], end == start) {
			end++
		}
		if end == start {
			continue
		}
		label := strings.TrimSpace(head[start:end])
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseCreatePatternIDParam(segment string) string {
	body, ok := extractPropertyMapBody(segment)
	if !ok || strings.TrimSpace(body) == "" {
		return ""
	}
	for _, pair := range splitTopLevelByComma(body) {
		key, expr, ok := splitTopLevelKeyValue(pair)
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "id") {
			continue
		}
		expr = strings.TrimSpace(expr)
		if !strings.HasPrefix(expr, "$") {
			return ""
		}
		return strings.TrimSpace(expr[1:])
	}
	return ""
}

func parseCreatePatternPropertyMap(ctx context.Context, lookup writeLookupTx, tenant, pattern string, params map[string]any, bindings map[string]any) graph.PropertyMap {
	body, ok := extractPropertyMapBody(pattern)
	if !ok || strings.TrimSpace(body) == "" {
		return nil
	}
	props := graph.PropertyMap{}
	for _, pair := range splitTopLevelByComma(body) {
		key, expr, ok := splitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		expr = strings.TrimSpace(expr)
		value, ok := resolveWritePropertyValueWithBindings(expr, params, bindings)
		if ok {
			if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
				ok = false
			}
		}
		if !ok {
			value, ok = resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, expr, params, bindings)
		}
		if !ok {
			continue
		}
		if value == nil {
			continue
		}
		props[key] = encodePropertyValue(value)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

func parseCreateEdgeTypeName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ':':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				start := i + 1
				for start < len(raw) && isWriteSpace(raw[start]) {
					start++
				}
				end := start
				for end < len(raw) && isCreateIdentChar(raw[end], end == start) {
					end++
				}
				if end > start {
					return strings.TrimSpace(raw[start:end])
				}
			}
		}
	}
	return ""
}

func detectCreateEdgeReverse(connector string) bool {
	compact := strings.ReplaceAll(strings.TrimSpace(connector), " ", "")
	hasIn := strings.Contains(compact, "<-")
	hasOut := strings.Contains(compact, "->")
	return hasIn && !hasOut
}

func resolveCreatePatternVertexID(vertex createVertexPattern, params, bindings map[string]any) string {
	if id := resolveEntityID(vertex.varName, vertex.idParam, bindings, params); id != "" {
		return id
	}
	if vertex.varName != "" {
		if value, ok := bindings[vertex.varName+".id"]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
	}
	return ""
}

func shouldWriteCreatePatternVertex(vertex createVertexPattern, bindings map[string]any) bool {
	if strings.TrimSpace(vertex.varName) == "" {
		return true
	}
	if value, ok := bindings[vertex.varName]; ok {
		id := strings.TrimSpace(scalarString(value))
		if strings.HasPrefix(id, "__ve_write_v_") {
			return true
		}
		return false
	}
	if value, ok := bindings[vertex.varName+".id"]; ok {
		id := strings.TrimSpace(scalarString(value))
		if strings.HasPrefix(id, "__ve_write_v_") {
			return true
		}
		return false
	}
	return true
}

func bindCreatePatternVertex(bindings map[string]any, varName, vertexID string) {
	if strings.TrimSpace(varName) == "" || vertexID == "" {
		return
	}
	bindings[varName] = vertexID
	bindings[varName+".id"] = vertexID
}

func clonePropertyMap(in graph.PropertyMap) graph.PropertyMap {
	if len(in) == 0 {
		return nil
	}
	out := make(graph.PropertyMap, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func putCreateLikeVertex(ctx context.Context, sink runtimestorage.WriteSink, tenant, vertexID string, labels []string, props graph.PropertyMap, skipLookupMerge bool, mergeExisting bool) error {
	if skipLookupMerge {
		if fast, ok := sink.(appendOnlyVertexWriter); ok {
			return fast.PutVertexNew(ctx, &graph.Vertex{
				Tenant:     tenant,
				ID:         vertexID,
				Labels:     append([]string(nil), labels...),
				Properties: clonePropertyMap(props),
			})
		}
	}
	lookup, _ := sink.(writeLookupTx)
	mergedLabels := append([]string(nil), labels...)
	mergedProps := clonePropertyMap(props)
	if lookup != nil && !skipLookupMerge && mergeExisting {
		existing, err := lookup.GetVertex(ctx, tenant, vertexID)
		if err != nil {
			if !graph.IsKind(err, graph.ErrKindNotFound) {
				return err
			}
			existing = nil
		}
		if existing != nil {
			mergedLabels = mergeLabelSets(existing.Labels, labels)
			mergedProps = mergePropertyMaps(existing.Properties, props)
		}
	}
	return sink.PutVertex(ctx, &graph.Vertex{
		Tenant:     tenant,
		ID:         vertexID,
		Labels:     mergedLabels,
		Properties: mergedProps,
	})
}

func mergeLabelSets(existing, incoming []string) []string {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}
	out := append([]string(nil), existing...)
	seen := map[string]struct{}{}
	for _, label := range out {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		seen[label] = struct{}{}
	}
	for _, label := range incoming {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func findAnonymousMergeVertexID(ctx context.Context, sink runtimestorage.WriteSink, tenant string, labels []string, props graph.PropertyMap, deletedVertexIDs map[string]struct{}) (string, error) {
	lookup, _ := sink.(writeLookupTx)
	if lookup != nil {
		if vertexID, ok := mergeVertexIDProperty(props); ok {
			vertex, err := lookup.GetVertex(ctx, tenant, vertexID)
			if err != nil {
				if !graph.IsKind(err, graph.ErrKindNotFound) {
					return "", err
				}
			} else if vertex != nil {
				if _, deleted := deletedVertexIDs[strings.TrimSpace(vertex.ID)]; !deleted && mergeVertexMatches(vertex, labels, props) {
					return strings.TrimSpace(vertex.ID), nil
				}
			}
		}
		if matchedID, err := findAnonymousMergeVertexIDByPropertyIndex(ctx, sink, lookup, tenant, labels, props, deletedVertexIDs); err != nil {
			return "", err
		} else if matchedID != "" {
			return matchedID, nil
		}
	}

	scanner, ok := sink.(writeVertexScanner)
	if !ok {
		return "", nil
	}

	matchedID := ""
	err := scanner.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil {
			return nil
		}
		if _, deleted := deletedVertexIDs[strings.TrimSpace(vertex.ID)]; deleted {
			return nil
		}
		if !mergeVertexMatches(vertex, labels, props) {
			return nil
		}
		matchedID = strings.TrimSpace(vertex.ID)
		return errMergeVertexMatchFound
	})
	if err != nil && !errors.Is(err, errMergeVertexMatchFound) {
		return "", err
	}
	return matchedID, nil
}

func mergeVertexIDProperty(props graph.PropertyMap) (string, bool) {
	for key, raw := range props {
		if !strings.EqualFold(strings.TrimSpace(key), "id") {
			continue
		}
		id := strings.TrimSpace(scalarString(decodeWriteStoredPropertyValue(raw)))
		if id == "" {
			return "", false
		}
		return id, true
	}
	return "", false
}

func findAnonymousMergeVertexIDByPropertyIndex(ctx context.Context, sink runtimestorage.WriteSink, lookup writeLookupTx, tenant string, labels []string, props graph.PropertyMap, deletedVertexIDs map[string]struct{}) (string, error) {
	indexScanner, ok := sink.(writePropertyIndexScanner)
	if !ok || lookup == nil || tenant == "" || len(labels) == 0 || len(props) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(props))
	for key := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	matchedID := ""
	for _, key := range keys {
		encoded, ok := props[key]
		if !ok {
			continue
		}
		for _, label := range labels {
			label = strings.TrimSpace(label)
			if label == "" {
				continue
			}
			err := indexScanner.ScanPropertyIndex(ctx, tenant, label, key, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
				if entry == nil || !strings.EqualFold(strings.TrimSpace(entry.EntityClass), "vertex") {
					return nil
				}
				candidateID := strings.TrimSpace(entry.EntityID)
				if candidateID == "" {
					return nil
				}
				if _, deleted := deletedVertexIDs[candidateID]; deleted {
					return nil
				}
				candidate, err := lookup.GetVertex(ctx, tenant, candidateID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if candidate == nil || !mergeVertexMatches(candidate, labels, props) {
					return nil
				}
				matchedID = candidateID
				return errMergeVertexMatchFound
			})
			if err != nil {
				if errors.Is(err, errMergeVertexMatchFound) {
					return matchedID, nil
				}
				return "", err
			}
		}
	}
	return "", nil
}

func collectDeletedEntityIDsFromBindings(bindings map[string]any, vertexOut map[string]struct{}, edgeOut map[string]struct{}) {
	if len(bindings) == 0 || (vertexOut == nil && edgeOut == nil) {
		return
	}
	for key, value := range bindings {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		isEdgeBinding := false
		baseKey := strings.TrimSuffix(key, ".id")
		if _, ok := bindings[baseKey+".type"]; ok {
			isEdgeBinding = true
		}
		if strings.HasSuffix(key, ".id") {
			if id := scalarString(value); id != "" {
				if isEdgeBinding {
					if edgeOut != nil {
						edgeOut[id] = struct{}{}
					}
				} else if vertexOut != nil {
					vertexOut[id] = struct{}{}
				}
			}
			continue
		}
		if strings.Contains(key, ".") {
			continue
		}
		if id := scalarString(value); id != "" {
			if _, ok := bindings[key+".type"]; ok {
				if edgeOut != nil {
					edgeOut[id] = struct{}{}
				}
			} else if vertexOut != nil {
				vertexOut[id] = struct{}{}
			}
		}
	}
}

func collectDeletedEntityIDsFromBindingsWithLookup(ctx context.Context, lookup writeLookupTx, tenant string, bindings map[string]any, vertexOut map[string]struct{}, edgeOut map[string]struct{}) {
	collectDeletedEntityIDsFromBindings(bindings, vertexOut, edgeOut)
	if lookup == nil || len(bindings) == 0 || edgeOut == nil {
		return
	}
	for key, value := range bindings {
		key = strings.TrimSpace(key)
		if key == "" || strings.Contains(key, ".") {
			continue
		}
		id := strings.TrimSpace(scalarString(value))
		if id == "" {
			continue
		}
		if _, hasType := bindings[key+".type"]; hasType {
			edgeOut[id] = struct{}{}
			if vertexOut != nil {
				delete(vertexOut, id)
			}
			continue
		}
		if found, err := lookup.GetEdge(ctx, tenant, id); err == nil && found != nil {
			edgeOut[id] = struct{}{}
			if vertexOut != nil {
				delete(vertexOut, id)
			}
		}
	}
}

func collectDeletedEntityIDsFromEventWithLookup(ctx context.Context, lookup writeLookupTx, tenant string, event operators.WriteEvent, vertexOut map[string]struct{}, edgeOut map[string]struct{}) {
	if len(event.Bindings) == 0 {
		return
	}
	roots := deleteOperandRootsForEvent(event)
	if len(roots) == 0 {
		return
	}
	filtered := map[string]any{}
	for key, value := range event.Bindings {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		base := key
		if dot := strings.IndexByte(base, '.'); dot >= 0 {
			base = strings.TrimSpace(base[:dot])
		}
		if _, ok := roots[base]; !ok {
			continue
		}
		filtered[key] = value
	}
	collectDeletedEntityIDsFromBindingsWithLookup(ctx, lookup, tenant, filtered, vertexOut, edgeOut)
}

func deleteOperandRootsForEvent(event operators.WriteEvent) map[string]struct{} {
	roots := map[string]struct{}{}
	kind := strings.ToUpper(strings.TrimSpace(event.Kind))
	if kind != "DELETE" && kind != "DETACH DELETE" {
		return roots
	}
	body, ok := extractWriteClauseBody(event.Raw, event.Kind)
	if !ok || strings.TrimSpace(body) == "" {
		body = strings.TrimSpace(event.Pattern)
	}
	for _, item := range splitTopLevelByComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		root := ""
		for i := 0; i < len(item); i++ {
			ch := item[i]
			if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				root += string(ch)
				continue
			}
			break
		}
		root = strings.TrimSpace(root)
		if root != "" {
			roots[root] = struct{}{}
		}
	}
	return roots
}

func mergeVertexMatches(vertex *graph.Vertex, labels []string, props graph.PropertyMap) bool {
	if vertex == nil {
		return false
	}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if !vertexHasLabel(vertex, label) {
			return false
		}
	}
	for key, value := range props {
		if key == "" {
			continue
		}
		existing, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(existing, value) {
			return false
		}
	}
	return true
}

func vertexHasLabel(vertex *graph.Vertex, label string) bool {
	if vertex == nil || label == "" {
		return false
	}
	for _, existing := range vertex.Labels {
		if strings.EqualFold(strings.TrimSpace(existing), label) {
			return true
		}
	}
	return false
}

func mergePropertyMaps(existing, incoming graph.PropertyMap) graph.PropertyMap {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}
	out := clonePropertyMap(existing)
	if out == nil {
		out = graph.PropertyMap{}
	}
	for key, value := range incoming {
		out[key] = append([]byte(nil), value...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func createPatternHeadBeforeProps(body string) string {
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if ch == '\'' && !inDouble {
			if i > 0 && body[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(body, i) {
				continue
			}
			if inSingle && i+1 < len(body) && body[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && body[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return strings.TrimSpace(body[:i])
			}
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
	}
	return strings.TrimSpace(body)
}

func skipWriteWhitespace(raw string, start int) int {
	for start < len(raw) && isWriteSpace(raw[start]) {
		start++
	}
	return start
}

func isWriteSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isCreateIdentChar(ch byte, first bool) bool {
	if first {
		return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
	}
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func applyPropertyWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent) error {
	body, ok := extractWriteClauseBody(event.Raw, event.Kind)
	if !ok || strings.TrimSpace(body) == "" {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(event.Kind), "SET") {
		return applySetWriteBody(ctx, sink, tenant, event, body)
	}
	if strings.EqualFold(strings.TrimSpace(event.Kind), "REMOVE") {
		lookup, _ := sink.(writeLookupTx)
		for _, item := range splitTopLevelByComma(body) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if match := writeRemovePropertyAssignmentRE.FindStringSubmatch(item); len(match) == 3 {
				varName, field := match[1], match[2]
				binding := resolveWriteBinding(event, varName)
				if binding == nil {
					continue
				}
				if err := patchWriteBindingProperty(ctx, lookup, sink, tenant, binding, field, nil); err != nil {
					return err
				}
				continue
			}
			if match := writeSetLabelAssignmentRE.FindStringSubmatch(item); len(match) == 3 {
				varName := match[1]
				binding := resolveWriteBinding(event, varName)
				if binding == nil {
					continue
				}
				if err := removeWriteBindingLabels(ctx, lookup, sink, tenant, binding, splitRuntimeLabels(match[2])); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func applySetWriteBody(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, body string) error {
	lookup, _ := sink.(writeLookupTx)
	for _, item := range splitTopLevelByComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if match := writeSetPropertyAssignmentRE.FindStringSubmatch(item); len(match) == 4 {
			varName, field, expr := match[1], match[2], strings.TrimSpace(match[3])
			binding := resolveWriteBinding(event, varName)
			if binding == nil {
				continue
			}
			value, ok := resolveWritePropertyValueWithBindings(expr, event.ResolvedParams, event.Bindings)
			if ok {
				if literal, isString := value.(string); isString && literal == expr && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
					ok = false
				}
			}
			if !ok {
				value, ok = resolveWritePropertyValueFromBindingEntity(ctx, lookup, tenant, expr, event.ResolvedParams, event.Bindings)
			}
			if !ok {
				continue
			}
			if err := patchWriteBindingProperty(ctx, lookup, sink, tenant, binding, field, value); err != nil {
				return err
			}
			continue
		}
		if match := writeSetLabelAssignmentRE.FindStringSubmatch(item); len(match) == 3 {
			varName := match[1]
			binding := resolveWriteBinding(event, varName)
			if binding == nil {
				continue
			}
			if err := addWriteBindingLabels(ctx, lookup, sink, tenant, binding, splitRuntimeLabels(match[2])); err != nil {
				return err
			}
			continue
		}
		if match := writeSetMapAppendRE.FindStringSubmatch(item); len(match) == 3 {
			targetBinding := resolveWriteBinding(event, match[1])
			if targetBinding == nil {
				continue
			}
			props, ok := parseWritePropertyMapLiteralValues(match[2], event.ResolvedParams)
			if !ok {
				continue
			}
			if err := patchWriteBindingMapAppendLiteral(ctx, lookup, sink, tenant, targetBinding, props); err != nil {
				return err
			}
			continue
		}
		if match := writeSetMapReplaceBindingRE.FindStringSubmatch(item); len(match) == 3 {
			targetBinding := resolveWriteBinding(event, match[1])
			sourceBinding := resolveWriteBinding(event, match[2])
			if targetBinding == nil || sourceBinding == nil {
				continue
			}
			props, ok := resolveWriteBindingPropertyMap(ctx, lookup, tenant, sourceBinding)
			if !ok {
				continue
			}
			if err := patchWriteBindingMapReplace(ctx, lookup, sink, tenant, targetBinding, props); err != nil {
				return err
			}
			continue
		}
		if match := writeSetMapReplaceLiteralRE.FindStringSubmatch(item); len(match) == 3 {
			targetBinding := resolveWriteBinding(event, match[1])
			if targetBinding == nil {
				continue
			}
			props, ok := parseWritePropertyMapLiteralValues(match[2], event.ResolvedParams)
			if !ok {
				continue
			}
			if err := patchWriteBindingMapReplaceLiteral(ctx, lookup, sink, tenant, targetBinding, props); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyDeleteWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent) error {
	body, ok := extractWriteClauseBody(event.Raw, event.Kind)
	if !ok || strings.TrimSpace(body) == "" {
		body = strings.TrimSpace(event.Pattern)
	}
	if strings.TrimSpace(body) == "" {
		if event.Vertex != nil {
			if name := strings.TrimSpace(event.Vertex.Var); name != "" {
				body = name
			}
		}
	}
	if strings.TrimSpace(body) == "" {
		return nil
	}
	detach := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(event.Raw)), "DETACHDELETE") || strings.HasPrefix(strings.ToUpper(strings.TrimSpace(event.Raw)), "DETACH DELETE")
	lookup, _ := sink.(writeLookupTx)
	seen := map[string]struct{}{}
	edgeBindings := make([]any, 0)
	vertexBindings := make([]any, 0)
	otherBindings := make([]any, 0)
	for _, item := range splitTopLevelByComma(body) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		bindings := resolveDeleteWriteBindings(event, item)
		for _, binding := range bindings {
			key, ok, err := deleteWriteBindingKey(ctx, lookup, tenant, binding)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			switch {
			case strings.HasPrefix(key, "edge|"):
				edgeBindings = append(edgeBindings, binding)
			case strings.HasPrefix(key, "vertex|"):
				vertexBindings = append(vertexBindings, binding)
			default:
				otherBindings = append(otherBindings, binding)
			}
		}
	}
	for _, binding := range edgeBindings {
		if err := deleteWriteBinding(ctx, lookup, sink, tenant, binding, detach); err != nil {
			return err
		}
	}
	for _, binding := range vertexBindings {
		if err := deleteWriteBinding(ctx, lookup, sink, tenant, binding, detach); err != nil {
			return err
		}
	}
	for _, binding := range otherBindings {
		if err := deleteWriteBinding(ctx, lookup, sink, tenant, binding, detach); err != nil {
			return err
		}
	}
	return nil
}

func resolveDeleteWriteBindings(event operators.WriteEvent, operand string) []any {
	operand = normalizeDeleteOperand(strings.TrimSpace(operand))
	if operand == "" {
		return nil
	}
	resolved := make([]any, 0)
	if binding := resolveWriteBinding(event, operand); binding != nil {
		appendDeleteBindingsFromOperand(binding, &resolved)
	}
	if len(event.Bindings) != 0 {
		if binding, ok := resolveWriteBindingPath(event.Bindings, event.ResolvedParams, operand); ok {
			appendDeleteBindingsFromOperand(binding, &resolved)
		}
	}
	return resolved
}

func normalizeDeleteOperand(operand string) string {
	operand = strings.TrimSpace(operand)
	for len(operand) >= 2 && strings.HasPrefix(operand, "(") && strings.HasSuffix(operand, ")") {
		inner := strings.TrimSpace(operand[1 : len(operand)-1])
		if inner == "" {
			break
		}
		operand = inner
	}
	return operand
}

func appendDeleteBindingsFromOperand(value any, out *[]any) {
	if out == nil || value == nil {
		return
	}
	switch typed := value.(type) {
	case string, *graph.Vertex, graph.Vertex, *graph.Edge, graph.Edge:
		*out = append(*out, typed)
		return
	case []any:
		for _, item := range typed {
			appendDeleteBindingsFromOperand(item, out)
		}
		return
	case map[string]any:
		if id := strings.TrimSpace(scalarString(typed["id"])); id != "" {
			*out = append(*out, typed)
			return
		}
		if id := strings.TrimSpace(scalarString(typed["ID"])); id != "" {
			*out = append(*out, typed)
			return
		}
		if isWriteBindingEntityLikeMap(typed) {
			*out = append(*out, typed)
			return
		}
		pathLike := false
		if rels, ok := typed["relationships"]; ok {
			pathLike = true
			appendDeleteBindingsFromOperand(rels, out)
		}
		if edges, ok := typed["edges"]; ok {
			pathLike = true
			appendDeleteBindingsFromOperand(edges, out)
		}
		if nodes, ok := typed["nodes"]; ok {
			pathLike = true
			appendDeleteBindingsFromOperand(nodes, out)
		}
		if pathLike {
			return
		}
		for _, item := range typed {
			appendDeleteBindingsFromOperand(item, out)
		}
		return
	}
	appendDeleteBindingsFromReflectValue(reflect.ValueOf(value), out)
}

func isWriteBindingEntityLikeMap(binding map[string]any) bool {
	if len(binding) == 0 {
		return false
	}
	if _, ok := binding["labels"]; ok {
		return true
	}
	if _, ok := binding["type"]; ok {
		return true
	}
	for _, key := range []string{"src", "dst", "source", "target", "start", "end"} {
		if _, ok := binding[key]; ok {
			return true
		}
	}
	return false
}

func appendDeleteBindingsFromReflectValue(v reflect.Value, out *[]any) {
	if out == nil || !v.IsValid() {
		return
	}
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.String:
		if id := strings.TrimSpace(v.String()); id != "" {
			*out = append(*out, id)
		}
		return
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			appendDeleteBindingsFromReflectValue(v.Index(i), out)
		}
		return
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			appendDeleteBindingsFromReflectValue(iter.Value(), out)
		}
		return
	case reflect.Struct:
		if idField := v.FieldByName("ID"); idField.IsValid() && idField.Kind() == reflect.String {
			if id := strings.TrimSpace(idField.String()); id != "" {
				*out = append(*out, id)
				return
			}
		}
		if idField := v.FieldByName("id"); idField.IsValid() && idField.Kind() == reflect.String {
			if id := strings.TrimSpace(idField.String()); id != "" {
				*out = append(*out, id)
				return
			}
		}
		pathLike := false
		if rels := v.FieldByName("Relationships"); rels.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(rels, out)
		}
		if rels := v.FieldByName("relationships"); rels.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(rels, out)
		}
		if edges := v.FieldByName("Edges"); edges.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(edges, out)
		}
		if edges := v.FieldByName("edges"); edges.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(edges, out)
		}
		if nodes := v.FieldByName("Nodes"); nodes.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(nodes, out)
		}
		if nodes := v.FieldByName("nodes"); nodes.IsValid() {
			pathLike = true
			appendDeleteBindingsFromReflectValue(nodes, out)
		}
		if pathLike {
			return
		}
		for i := 0; i < v.NumField(); i++ {
			appendDeleteBindingsFromReflectValue(v.Field(i), out)
		}
		return
	}
}

func resolveWriteBindingPath(bindings map[string]any, params map[string]any, operand string) (any, bool) {
	if len(bindings) == 0 {
		return nil, false
	}
	if value, ok := bindings[operand]; ok {
		return value, true
	}
	if value, ok := bindings[operand+".id"]; ok {
		return value, true
	}
	parts := splitDeleteOperandPath(operand)
	if len(parts) == 0 {
		return nil, false
	}
	current, ok := bindings[parts[0]]
	if !ok {
		return nil, false
	}
	for _, part := range parts[1:] {
		part = resolveDeleteOperandPathToken(part, params)
		next, ok := lookupDeleteOperandPart(current, part)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func resolveDeleteOperandPathToken(part string, params map[string]any) string {
	part = strings.TrimSpace(part)
	if !strings.HasPrefix(part, "$") {
		return part
	}
	name := strings.TrimSpace(part[1:])
	if name == "" || len(params) == 0 {
		return part
	}
	if raw, ok := params[name]; ok && raw != nil {
		return strings.TrimSpace(fmt.Sprint(raw))
	}
	if raw, ok := params[part]; ok && raw != nil {
		return strings.TrimSpace(fmt.Sprint(raw))
	}
	for key, raw := range params {
		if !strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		if raw != nil {
			return strings.TrimSpace(fmt.Sprint(raw))
		}
		break
	}
	return part
}

func splitDeleteOperandPath(operand string) []string {
	operand = strings.TrimSpace(operand)
	if operand == "" {
		return nil
	}
	parts := make([]string, 0)
	b := strings.Builder{}
	for i := 0; i < len(operand); i++ {
		ch := operand[i]
		switch ch {
		case '.':
			if b.Len() > 0 {
				parts = append(parts, b.String())
				b.Reset()
			}
		case '[':
			if b.Len() > 0 {
				parts = append(parts, b.String())
				b.Reset()
			}
			end := strings.IndexByte(operand[i+1:], ']')
			if end < 0 {
				return nil
			}
			token := strings.TrimSpace(operand[i+1 : i+1+end])
			token = strings.Trim(token, "\"'")
			if token != "" {
				parts = append(parts, token)
			}
			i += end + 1
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteByte(ch)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

func lookupDeleteOperandPart(current any, part string) (any, bool) {
	part = strings.TrimSpace(part)
	if current == nil || part == "" {
		return nil, false
	}
	switch typed := current.(type) {
	case map[string]any:
		value, ok := typed[part]
		return value, ok
	case *graph.Vertex:
		if typed == nil {
			return nil, false
		}
		return lookupGraphVertexOperandPart(typed, part)
	case graph.Vertex:
		return lookupGraphVertexOperandPart(&typed, part)
	case *graph.Edge:
		if typed == nil {
			return nil, false
		}
		return lookupGraphEdgeOperandPart(typed, part)
	case graph.Edge:
		return lookupGraphEdgeOperandPart(&typed, part)
	case []any:
		idx, err := strconv.Atoi(part)
		if err != nil || idx < 0 || idx >= len(typed) {
			return nil, false
		}
		return typed[idx], true
	}
	v := reflect.ValueOf(current)
	if !v.IsValid() {
		return nil, false
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		iter := v.MapRange()
		for iter.Next() {
			if strings.EqualFold(strings.TrimSpace(fmt.Sprint(iter.Key().Interface())), part) {
				return iter.Value().Interface(), true
			}
		}
		return nil, false
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		idx, err := strconv.Atoi(part)
		if err != nil || idx < 0 || idx >= v.Len() {
			return nil, false
		}
		return v.Index(idx).Interface(), true
	}
	if v.Kind() == reflect.Struct {
		typ := v.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			if strings.EqualFold(field.Name, part) {
				return v.Field(i).Interface(), true
			}
		}
	}
	return nil, false
}

func lookupGraphVertexOperandPart(vertex *graph.Vertex, part string) (any, bool) {
	if vertex == nil {
		return nil, false
	}
	if strings.EqualFold(part, "id") {
		return strings.TrimSpace(vertex.ID), true
	}
	if strings.EqualFold(part, "labels") {
		return append([]string(nil), vertex.Labels...), true
	}
	if vertex.Properties == nil {
		return nil, false
	}
	if raw, ok := vertex.Properties[part]; ok {
		return decodeWriteStoredPropertyValue(raw), true
	}
	for key, raw := range vertex.Properties {
		if strings.EqualFold(strings.TrimSpace(key), part) {
			return decodeWriteStoredPropertyValue(raw), true
		}
	}
	return nil, false
}

func lookupGraphEdgeOperandPart(edge *graph.Edge, part string) (any, bool) {
	if edge == nil {
		return nil, false
	}
	if strings.EqualFold(part, "id") {
		return strings.TrimSpace(edge.ID), true
	}
	if strings.EqualFold(part, "type") {
		return strings.TrimSpace(edge.Type), true
	}
	if edge.Properties == nil {
		return nil, false
	}
	if raw, ok := edge.Properties[part]; ok {
		return decodeWriteStoredPropertyValue(raw), true
	}
	for key, raw := range edge.Properties {
		if strings.EqualFold(strings.TrimSpace(key), part) {
			return decodeWriteStoredPropertyValue(raw), true
		}
	}
	return nil, false
}

func decodeWriteStoredPropertyValue(raw []byte) any {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return ""
	}
	if strings.EqualFold(text, "null") {
		return nil
	}
	if strings.EqualFold(text, "true") {
		return true
	}
	if strings.EqualFold(text, "false") {
		return false
	}
	if i, err := strconv.ParseInt(text, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		return f
	}
	return text
}

func deleteWriteBindingKey(ctx context.Context, lookup writeLookupTx, tenant string, binding any) (string, bool, error) {
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return "", false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false, nil
	}
	if vertex != nil {
		return "vertex|" + id, true, nil
	}
	if edge != nil {
		return "edge|" + id, true, nil
	}
	return "entity|" + id, true, nil
}

func extractWriteClauseBody(raw, kind string) (string, bool) {
	raw = strings.TrimSpace(raw)
	kind = strings.ToUpper(strings.TrimSpace(kind))
	if raw == "" || kind == "" {
		return "", false
	}
	upper := strings.ToUpper(raw)
	switch kind {
	case "SET":
		if strings.HasPrefix(upper, "SET") {
			return strings.TrimSpace(raw[len("SET"):]), true
		}
		return strings.TrimSpace(stripWritePrefix(raw, "SET")), true
	case "REMOVE":
		if strings.HasPrefix(upper, "REMOVE") {
			return strings.TrimSpace(raw[len("REMOVE"):]), true
		}
		return strings.TrimSpace(stripWritePrefix(raw, "REMOVE")), true
	case "DELETE":
		if strings.HasPrefix(upper, "DETACHDELETE") {
			return strings.TrimSpace(raw[len("DETACHDELETE"):]), true
		}
		if strings.HasPrefix(upper, "DETACH DELETE") {
			return strings.TrimSpace(raw[len("DETACH DELETE"):]), true
		}
		if strings.HasPrefix(upper, "DELETE") {
			return strings.TrimSpace(raw[len("DELETE"):]), true
		}
		return strings.TrimSpace(stripWritePrefix(raw, "DELETE")), true
	case "DETACH DELETE":
		if strings.HasPrefix(upper, "DETACHDELETE") {
			return strings.TrimSpace(raw[len("DETACHDELETE"):]), true
		}
		if strings.HasPrefix(upper, "DETACH DELETE") {
			return strings.TrimSpace(raw[len("DETACH DELETE"):]), true
		}
		return strings.TrimSpace(stripWritePrefix(raw, "DETACH DELETE")), true
	default:
		return "", false
	}
}

func stripWritePrefix(raw, prefix string) string {
	raw = strings.TrimSpace(raw)
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return raw
	}
	if strings.HasPrefix(strings.ToUpper(raw), strings.ToUpper(prefix)) {
		return strings.TrimSpace(raw[len(prefix):])
	}
	return raw
}

func resolveWriteBinding(event operators.WriteEvent, varName string) any {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return nil
	}
	if event.Bindings != nil {
		if value, ok := event.Bindings[varName]; ok {
			return value
		}
		if value, ok := event.Bindings[varName+".id"]; ok {
			return value
		}
	}
	return nil
}

func patchWriteBindingProperty(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, field string, value any) error {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil
	}
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return err
	}
	propMap := graph.PropertyMap{}
	remove := []string{}
	if value == nil {
		remove = append(remove, field)
	} else {
		if !isSupportedRuntimePropertyValue(value) {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
		}
		propMap[field] = encodePropertyValue(value)
	}
	if vertex == nil && edge == nil {
		if strings.TrimSpace(id) == "" {
			return nil
		}
		return patchUnresolvedBindingByID(ctx, sink, tenant, id, propMap, remove, writeBindingLikelyEdge(binding, id))
	}
	if vertex != nil {
		if err := sink.PatchVertexProperties(ctx, tenant, id, propMap, remove); err != nil {
			return err
		}
		return nil
	}
	if edge != nil {
		return sink.PatchEdgeProperties(ctx, tenant, id, propMap, remove)
	}
	return nil
}

func patchWriteBindingMapReplace(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, props graph.PropertyMap) error {
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return err
	}
	if lookup != nil && strings.TrimSpace(id) != "" {
		if loadedVertex, loadErr := lookup.GetVertex(ctx, tenant, id); loadErr == nil && loadedVertex != nil {
			vertex = loadedVertex
			edge = nil
		} else if loadedEdge, loadErr := lookup.GetEdge(ctx, tenant, id); loadErr == nil && loadedEdge != nil {
			edge = loadedEdge
			vertex = nil
		}
	}
	if vertex == nil && edge == nil {
		if strings.TrimSpace(id) == "" {
			return nil
		}
		return patchUnresolvedBindingByID(ctx, sink, tenant, id, clonePropertyMap(props), nil, writeBindingLikelyEdge(binding, id))
	}
	existing := graph.PropertyMap{}
	if vertex != nil {
		existing = vertex.Properties
	}
	if edge != nil {
		existing = edge.Properties
	}
	remove := []string{}
	for key := range existing {
		if _, ok := props[key]; !ok {
			remove = append(remove, key)
		}
	}
	if vertex != nil {
		return sink.PatchVertexProperties(ctx, tenant, id, clonePropertyMap(props), remove)
	}
	if edge != nil {
		return sink.PatchEdgeProperties(ctx, tenant, id, clonePropertyMap(props), remove)
	}
	return nil
}

func patchWriteBindingMapAppendLiteral(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, props map[string]any) error {
	if len(props) == 0 {
		return nil
	}
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return err
	}
	if vertex == nil && edge == nil {
		if strings.TrimSpace(id) == "" {
			return nil
		}
		upsert := graph.PropertyMap{}
		remove := []string{}
		for key, value := range props {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if value == nil {
				remove = append(remove, key)
				continue
			}
			if !isSupportedRuntimePropertyValue(value) {
				return graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
			}
			upsert[key] = encodePropertyValue(value)
		}
		return patchUnresolvedBindingByID(ctx, sink, tenant, id, upsert, remove, writeBindingLikelyEdge(binding, id))
	}
	upsert := graph.PropertyMap{}
	remove := []string{}
	for key, value := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value == nil {
			remove = append(remove, key)
			continue
		}
		if !isSupportedRuntimePropertyValue(value) {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
		}
		upsert[key] = encodePropertyValue(value)
	}
	if vertex != nil {
		return sink.PatchVertexProperties(ctx, tenant, id, upsert, remove)
	}
	if edge != nil {
		return sink.PatchEdgeProperties(ctx, tenant, id, upsert, remove)
	}
	return nil
}

func patchWriteBindingMapReplaceLiteral(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, props map[string]any) error {
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return err
	}
	if lookup != nil && strings.TrimSpace(id) != "" {
		if loadedVertex, loadErr := lookup.GetVertex(ctx, tenant, id); loadErr == nil && loadedVertex != nil {
			vertex = loadedVertex
			edge = nil
		} else if loadedEdge, loadErr := lookup.GetEdge(ctx, tenant, id); loadErr == nil && loadedEdge != nil {
			edge = loadedEdge
			vertex = nil
		}
	}
	if vertex == nil && edge == nil {
		if strings.TrimSpace(id) == "" {
			return nil
		}
		upsert := graph.PropertyMap{}
		remove := []string{}
		for key, value := range props {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if value == nil {
				remove = append(remove, key)
				continue
			}
			if !isSupportedRuntimePropertyValue(value) {
				return graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
			}
			upsert[key] = encodePropertyValue(value)
		}
		return patchUnresolvedBindingByID(ctx, sink, tenant, id, upsert, remove, writeBindingLikelyEdge(binding, id))
	}
	existing := graph.PropertyMap{}
	if vertex != nil {
		existing = vertex.Properties
	}
	if edge != nil {
		existing = edge.Properties
	}
	upsert := graph.PropertyMap{}
	removeSet := map[string]struct{}{}
	for key := range existing {
		if _, ok := props[key]; !ok {
			removeSet[key] = struct{}{}
		}
	}
	for key, value := range props {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value == nil {
			removeSet[key] = struct{}{}
			continue
		}
		if !isSupportedRuntimePropertyValue(value) {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
		}
		upsert[key] = encodePropertyValue(value)
	}
	remove := make([]string, 0, len(removeSet))
	for key := range removeSet {
		remove = append(remove, key)
	}
	if vertex != nil {
		return sink.PatchVertexProperties(ctx, tenant, id, upsert, remove)
	}
	if edge != nil {
		return sink.PatchEdgeProperties(ctx, tenant, id, upsert, remove)
	}
	return nil
}

func patchUnresolvedBindingByID(ctx context.Context, sink runtimestorage.WriteSink, tenant, id string, upsert graph.PropertyMap, remove []string, preferEdge bool) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	tryPatchVertex := func() error {
		err := sink.PatchVertexProperties(ctx, tenant, id, clonePropertyMap(upsert), append([]string(nil), remove...))
		if err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return err
	}
	tryPatchEdge := func() error {
		err := sink.PatchEdgeProperties(ctx, tenant, id, clonePropertyMap(upsert), append([]string(nil), remove...))
		if err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return err
	}

	if preferEdge {
		if err := tryPatchEdge(); err == nil {
			return nil
		} else if !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		if err := tryPatchVertex(); err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		} else {
			return err
		}
	}

	if err := tryPatchVertex(); err == nil {
		return nil
	} else if !graph.IsKind(err, graph.ErrKindNotFound) {
		return err
	}
	if err := tryPatchEdge(); err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
		return nil
	} else {
		return err
	}
}

func writeBindingLikelyEdge(binding any, id string) bool {
	id = strings.TrimSpace(id)
	if id != "" && strings.Count(id, "|") >= 3 {
		return true
	}
	if m, ok := binding.(map[string]any); ok {
		if _, hasType := m["type"]; hasType {
			return true
		}
		if _, hasSrc := m["src"]; hasSrc {
			return true
		}
		if _, hasDst := m["dst"]; hasDst {
			return true
		}
		if _, hasSrcID := m["srcid"]; hasSrcID {
			return true
		}
		if _, hasDstID := m["dstid"]; hasDstID {
			return true
		}
	}
	return false
}

func resolveWriteBindingPropertyMap(ctx context.Context, lookup writeLookupTx, tenant string, binding any) (graph.PropertyMap, bool) {
	vertex, edge, _, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return nil, false
	}
	if vertex != nil {
		return clonePropertyMap(vertex.Properties), true
	}
	if edge != nil {
		return clonePropertyMap(edge.Properties), true
	}
	return nil, false
}

func parseWritePropertyMapLiteralValues(expr string, params map[string]any) (map[string]any, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "{") || !strings.HasSuffix(expr, "}") {
		return nil, false
	}
	body := strings.TrimSpace(expr[1 : len(expr)-1])
	if body == "" {
		return map[string]any{}, true
	}
	props := map[string]any{}
	for _, pair := range splitTopLevelByComma(body) {
		key, valueExpr, ok := splitTopLevelKeyValue(pair)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value, ok := resolveWritePropertyValue(strings.TrimSpace(valueExpr), params)
		if !ok {
			continue
		}
		props[key] = value
	}
	return props, true
}

func addWriteBindingLabels(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, labels []string) error {
	vertex, _, _, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil || vertex == nil {
		return err
	}
	if len(labels) == 0 {
		return nil
	}
	next := append([]string(nil), vertex.Labels...)
	seen := map[string]struct{}{}
	for _, label := range next {
		seen[label] = struct{}{}
	}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		next = append(next, label)
	}
	updated := *vertex
	updated.Labels = next
	return sink.PutVertex(ctx, &updated)
}

func removeWriteBindingLabels(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, labels []string) error {
	vertex, _, _, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil || vertex == nil {
		return err
	}
	if len(labels) == 0 {
		return nil
	}
	removeSet := map[string]struct{}{}
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			continue
		}
		removeSet[label] = struct{}{}
	}
	if len(removeSet) == 0 {
		return nil
	}
	next := make([]string, 0, len(vertex.Labels))
	for _, label := range vertex.Labels {
		if _, remove := removeSet[strings.TrimSpace(label)]; remove {
			continue
		}
		next = append(next, label)
	}
	updated := *vertex
	updated.Labels = next
	return sink.PutVertex(ctx, &updated)
}

func deleteWriteBinding(ctx context.Context, lookup writeLookupTx, sink runtimestorage.WriteSink, tenant string, binding any, detach bool) error {
	vertex, edge, id, err := resolveWriteBindingEntity(ctx, lookup, tenant, binding)
	if err != nil {
		return err
	}
	if vertex != nil {
		if !detach && lookup != nil {
			return lookup.DeleteVertex(ctx, tenant, id)
		}
		return sink.DeleteVertexDetach(ctx, tenant, id)
	}
	if edge != nil {
		return sink.DeleteEdge(ctx, tenant, id)
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if detach {
		if err := sink.DeleteVertexDetach(ctx, tenant, id); err == nil {
			return nil
		} else if !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		err = sink.DeleteEdge(ctx, tenant, id)
		if err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
			return nil
		}
		return err
	}
	if lookup != nil {
		if err := lookup.DeleteVertex(ctx, tenant, id); err == nil {
			return nil
		} else if !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
	}
	err = sink.DeleteEdge(ctx, tenant, id)
	if err == nil || graph.IsKind(err, graph.ErrKindNotFound) {
		return nil
	}
	return err
}

func splitRuntimeLabels(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ":")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	return out
}

func resolveWriteBindingEntity(ctx context.Context, lookup writeLookupTx, tenant string, binding any) (*graph.Vertex, *graph.Edge, string, error) {
	switch typed := binding.(type) {
	case *graph.Vertex:
		return typed, nil, typed.ID, nil
	case *graph.Edge:
		return nil, typed, typed.ID, nil
	case map[string]any:
		id := strings.TrimSpace(scalarString(typed["id"]))
		if id == "" {
			id = strings.TrimSpace(scalarString(typed["ID"]))
		}
		if id == "" {
			if lookup != nil {
				if edge, ok := resolveWriteBindingEdgeByShape(ctx, lookup, tenant, typed); ok {
					return nil, edge, strings.TrimSpace(edge.ID), nil
				}
				if vertex, ok := resolveWriteBindingVertexByShape(ctx, lookup, tenant, typed); ok {
					return vertex, nil, strings.TrimSpace(vertex.ID), nil
				}
			}
			return nil, nil, "", nil
		}
		if lookup != nil {
			if vertex, err := lookup.GetVertex(ctx, tenant, id); err == nil && vertex != nil {
				return vertex, nil, id, nil
			}
			if edge, err := lookup.GetEdge(ctx, tenant, id); err == nil && edge != nil {
				return nil, edge, id, nil
			}
			if vertex, ok := resolveWriteBindingVertexBySemanticID(ctx, lookup, tenant, id, collectWriteBindingLabels(typed)); ok {
				return vertex, nil, vertex.ID, nil
			}
		}
		return nil, nil, id, nil
	case string:
		id := strings.TrimSpace(typed)
		if id == "" {
			return nil, nil, "", nil
		}
		if lookup != nil {
			if edge, ok := resolveCanonicalEdgeBinding(ctx, lookup, tenant, id); ok {
				return nil, edge, strings.TrimSpace(edge.ID), nil
			}
		}
		if lookup != nil {
			if vertex, err := lookup.GetVertex(ctx, tenant, id); err == nil && vertex != nil {
				return vertex, nil, id, nil
			}
			if edge, err := lookup.GetEdge(ctx, tenant, id); err == nil && edge != nil {
				return nil, edge, id, nil
			}
			if vertex, ok := resolveWriteBindingVertexBySemanticID(ctx, lookup, tenant, id, nil); ok {
				return vertex, nil, vertex.ID, nil
			}
		}
		return nil, nil, id, nil
	default:
		id := scalarString(binding)
		if id == "" {
			return nil, nil, "", nil
		}
		if lookup != nil {
			if edge, ok := resolveCanonicalEdgeBinding(ctx, lookup, tenant, id); ok {
				return nil, edge, strings.TrimSpace(edge.ID), nil
			}
		}
		if lookup != nil {
			if vertex, err := lookup.GetVertex(ctx, tenant, id); err == nil && vertex != nil {
				return vertex, nil, id, nil
			}
			if edge, err := lookup.GetEdge(ctx, tenant, id); err == nil && edge != nil {
				return nil, edge, id, nil
			}
			if vertex, ok := resolveWriteBindingVertexBySemanticID(ctx, lookup, tenant, id, nil); ok {
				return vertex, nil, vertex.ID, nil
			}
		}
		return nil, nil, id, nil
	}
}

func resolveCanonicalEdgeBinding(ctx context.Context, lookup writeLookupTx, tenant string, id string) (*graph.Edge, bool) {
	id = strings.TrimSpace(id)
	parts := strings.Split(id, "|")
	if len(parts) != 4 {
		return nil, false
	}
	srcID := strings.TrimSpace(parts[1])
	edgeType := strings.TrimSpace(parts[2])
	dstID := strings.TrimSpace(parts[3])
	if srcID == "" || edgeType == "" || dstID == "" {
		return nil, false
	}
	scanner, ok := lookup.(writeEdgeScanner)
	if !ok {
		return nil, false
	}
	var matched *graph.Edge
	_ = scanner.ScanOutEdges(ctx, tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
		if edge == nil {
			return nil
		}
		if strings.TrimSpace(edge.DstID) != dstID {
			return nil
		}
		matched = edge
		return errMergeVertexMatchFound
	})
	if matched == nil {
		return nil, false
	}
	return matched, true
}

func resolveWriteBindingVertexByShape(ctx context.Context, lookup writeLookupTx, tenant string, binding map[string]any) (*graph.Vertex, bool) {
	scanner, ok := lookup.(writeVertexScanner)
	if !ok {
		return nil, false
	}
	labels := collectWriteBindingLabels(binding)
	props := collectWriteBindingScalarProps(binding)
	var matched *graph.Vertex
	ambiguous := false
	_ = scanner.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil || ambiguous {
			return nil
		}
		if len(labels) != 0 && !writeBindingVertexHasAllLabels(vertex, labels) {
			return nil
		}
		if !writeBindingVertexPropertiesMatch(vertex, props) {
			return nil
		}
		if matched != nil && strings.TrimSpace(matched.ID) != strings.TrimSpace(vertex.ID) {
			ambiguous = true
			matched = nil
			return nil
		}
		matched = vertex
		return nil
	})
	if ambiguous || matched == nil {
		return nil, false
	}
	return matched, true
}

func resolveWriteBindingEdgeByShape(ctx context.Context, lookup writeLookupTx, tenant string, binding map[string]any) (*graph.Edge, bool) {
	scanner, ok := lookup.(writeEdgeScanner)
	if !ok {
		return nil, false
	}
	edgeType := strings.TrimSpace(scalarString(binding["type"]))
	props := collectWriteBindingScalarProps(binding)
	for _, key := range []string{"type", "src", "dst", "source", "target", "start", "end", "labels"} {
		delete(props, key)
	}
	srcHint := strings.TrimSpace(scalarString(binding["src"]))
	if srcHint == "" {
		srcHint = strings.TrimSpace(scalarString(binding["source"]))
	}
	if srcHint == "" {
		srcHint = strings.TrimSpace(scalarString(binding["start"]))
	}
	dstHint := strings.TrimSpace(scalarString(binding["dst"]))
	if dstHint == "" {
		dstHint = strings.TrimSpace(scalarString(binding["target"]))
	}
	if dstHint == "" {
		dstHint = strings.TrimSpace(scalarString(binding["end"]))
	}

	var matched *graph.Edge
	ambiguous := false
	_ = scanner.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil || ambiguous {
			return nil
		}
		srcID := strings.TrimSpace(vertex.ID)
		if srcHint != "" && srcID != srcHint {
			return nil
		}
		return scanner.ScanOutEdges(ctx, tenant, srcID, "", 0, func(edge *graph.Edge) error {
			if edge == nil || ambiguous {
				return nil
			}
			if edgeType != "" && !strings.EqualFold(strings.TrimSpace(edge.Type), edgeType) {
				return nil
			}
			if dstHint != "" && strings.TrimSpace(edge.DstID) != dstHint {
				return nil
			}
			if !writeBindingEdgePropertiesMatch(edge, props) {
				return nil
			}
			if matched != nil && strings.TrimSpace(matched.ID) != strings.TrimSpace(edge.ID) {
				ambiguous = true
				matched = nil
				return nil
			}
			matched = edge
			return nil
		})
	})
	if ambiguous || matched == nil {
		return nil, false
	}
	return matched, true
}

func collectWriteBindingScalarProps(binding map[string]any) map[string]any {
	if len(binding) == 0 {
		return nil
	}
	out := map[string]any{}
	for key, value := range binding {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if strings.EqualFold(key, "id") || strings.EqualFold(key, "labels") {
			continue
		}
		switch value.(type) {
		case map[string]any, []any, []string:
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writeBindingVertexPropertiesMatch(vertex *graph.Vertex, expected map[string]any) bool {
	if vertex == nil || len(expected) == 0 {
		return true
	}
	for key, value := range expected {
		raw, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		if !writeBindingValuesEqual(decodeWriteStoredPropertyValue(raw), value) {
			return false
		}
	}
	return true
}

func writeBindingEdgePropertiesMatch(edge *graph.Edge, expected map[string]any) bool {
	if edge == nil || len(expected) == 0 {
		return true
	}
	for key, value := range expected {
		raw, ok := edge.Properties[key]
		if !ok {
			return false
		}
		if !writeBindingValuesEqual(decodeWriteStoredPropertyValue(raw), value) {
			return false
		}
	}
	return true
}

func writeBindingValuesEqual(actual any, expected any) bool {
	if reflect.DeepEqual(actual, expected) {
		return true
	}
	return strings.TrimSpace(fmt.Sprint(actual)) == strings.TrimSpace(fmt.Sprint(expected))
}

func resolveWriteBindingVertexBySemanticID(ctx context.Context, lookup writeLookupTx, tenant, semanticID string, labels []string) (*graph.Vertex, bool) {
	scanner, ok := lookup.(writeVertexScanner)
	if !ok {
		return nil, false
	}
	semanticID = strings.TrimSpace(semanticID)
	if semanticID == "" {
		return nil, false
	}
	var matched *graph.Vertex
	_ = scanner.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil || matched != nil {
			return nil
		}
		if len(labels) != 0 && !writeBindingVertexHasAllLabels(vertex, labels) {
			return nil
		}
		rawID, hasID := vertex.Properties["id"]
		if !hasID || strings.TrimSpace(string(rawID)) != semanticID {
			return nil
		}
		matched = vertex
		return nil
	})
	if matched == nil {
		return nil, false
	}
	return matched, true
}

func collectWriteBindingLabels(binding map[string]any) []string {
	raw, ok := binding["labels"]
	if !ok || raw == nil {
		return nil
	}
	out := make([]string, 0)
	appendLabel := func(value any) {
		label := strings.TrimSpace(fmt.Sprint(value))
		if label != "" {
			out = append(out, label)
		}
	}
	switch typed := raw.(type) {
	case []string:
		for _, label := range typed {
			appendLabel(label)
		}
	case []any:
		for _, label := range typed {
			appendLabel(label)
		}
	default:
		appendLabel(typed)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func writeBindingVertexHasAllLabels(vertex *graph.Vertex, labels []string) bool {
	if vertex == nil {
		return false
	}
	if len(labels) == 0 {
		return true
	}
	if len(vertex.Labels) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(vertex.Labels))
	for _, label := range vertex.Labels {
		trimmed := strings.TrimSpace(label)
		if trimmed != "" {
			seen[trimmed] = struct{}{}
		}
	}
	for _, label := range labels {
		if _, ok := seen[strings.TrimSpace(label)]; !ok {
			return false
		}
	}
	return true
}

func extractPropertyMapBody(pattern string) (string, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return "", false
	}
	open := strings.Index(pattern, "{")
	if open < 0 {
		return "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := open; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\'' && !inDouble {
			if i > 0 && pattern[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(pattern, i) {
				continue
			}
			if inSingle && i+1 < len(pattern) && pattern[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && pattern[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return pattern[open+1 : i], true
			}
		}
	}
	return "", false
}

func splitTopLevelByComma(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := []string{}
	start := 0
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(raw, i) {
				continue
			}
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && raw[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ',':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				parts = append(parts, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	return parts
}

func splitTopLevelKeyValue(pair string) (string, string, bool) {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return "", "", false
	}
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(pair); i++ {
		ch := pair[i]
		if ch == '\'' && !inDouble {
			if i > 0 && pair[i-1] == '\\' {
				continue
			}
			if inSingle && isEmbeddedApostrophe(pair, i) {
				continue
			}
			if inSingle && i+1 < len(pair) && pair[i+1] == '\'' {
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && pair[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case ':':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return strings.TrimSpace(pair[:i]), strings.TrimSpace(pair[i+1:]), true
			}
		}
	}
	return "", "", false
}

func resolveWritePropertyValue(expr string, params map[string]any) (any, bool) {
	return resolveWritePropertyValueWithBindings(expr, params, nil)
}

func resolveWritePropertyValueWithBindings(expr string, params map[string]any, bindings map[string]any) (any, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}
	if value, ok := resolveWriteTemporalConstructor(expr, params); ok {
		return value, true
	}
	if strings.HasPrefix(expr, "$") {
		key := strings.TrimSpace(expr[1:])
		if key == "" || params == nil {
			return nil, false
		}
		value, ok := params[key]
		return value, ok
	}
	if strings.EqualFold(expr, "null") {
		return nil, true
	}
	if strings.EqualFold(expr, "true") {
		return true, true
	}
	if strings.EqualFold(expr, "false") {
		return false, true
	}
	if s, ok := parseQuotedPropertyString(expr); ok {
		return s, true
	}
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		body := strings.TrimSpace(expr[1 : len(expr)-1])
		if body == "" {
			return []any{}, true
		}
		parts := splitTopLevelByComma(body)
		list := make([]any, 0, len(parts))
		for _, part := range parts {
			v, ok := resolveWritePropertyValueWithBindings(part, params, bindings)
			if !ok {
				return nil, false
			}
			list = append(list, v)
		}
		return list, true
	}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		body := strings.TrimSpace(expr[1 : len(expr)-1])
		if body == "" {
			return map[string]any{}, true
		}
		props := map[string]any{}
		for _, pair := range splitTopLevelByComma(body) {
			key, valueExpr, ok := splitTopLevelKeyValue(pair)
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			value, ok := resolveWritePropertyValueWithBindings(strings.TrimSpace(valueExpr), params, bindings)
			if !ok {
				return nil, false
			}
			props[key] = value
		}
		return props, true
	}
	if value, ok := resolveWriteBinaryValueWithBindings(expr, params, bindings); ok {
		return value, true
	}
	if i, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return i, true
	}
	lower := strings.ToLower(expr)
	if strings.HasPrefix(lower, "0x") || strings.HasPrefix(lower, "-0x") || strings.HasPrefix(lower, "+0x") || strings.HasPrefix(lower, "0o") || strings.HasPrefix(lower, "-0o") || strings.HasPrefix(lower, "+0o") {
		if i, err := strconv.ParseInt(expr, 0, 64); err == nil {
			return i, true
		}
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, true
	}
	if bindings != nil && (strings.Contains(expr, ".") || strings.Contains(expr, "[")) {
		if value, ok := resolveWriteBindingPath(bindings, params, expr); ok {
			return value, true
		}
	}
	if isWriteBindingIdentifierExpr(expr) {
		if bindings != nil {
			if value, ok := bindings[expr]; ok {
				return value, true
			}
			if value, ok := bindings[expr+".id"]; ok {
				return value, true
			}
		}
		if params != nil {
			if value, ok := params[expr]; ok {
				return value, true
			}
		}
		return nil, false
	}
	if strings.Contains(expr, ".") || strings.Contains(expr, "[") {
		return nil, false
	}
	return expr, true
}

func resolveWriteBinaryValueWithBindings(expr string, params map[string]any, bindings map[string]any) (any, bool) {
	if leftExpr, op, rightExpr, ok := splitWriteTopLevelBinary(expr, "+-"); ok {
		left, leftOK := resolveWritePropertyValueWithBindings(leftExpr, params, bindings)
		if !leftOK {
			return nil, false
		}
		right, rightOK := resolveWritePropertyValueWithBindings(rightExpr, params, bindings)
		if !rightOK {
			return nil, false
		}
		return applyWriteBinaryValue(op, left, right)
	}
	if leftExpr, op, rightExpr, ok := splitWriteTopLevelBinary(expr, "*/"); ok {
		left, leftOK := resolveWritePropertyValueWithBindings(leftExpr, params, bindings)
		if !leftOK {
			return nil, false
		}
		right, rightOK := resolveWritePropertyValueWithBindings(rightExpr, params, bindings)
		if !rightOK {
			return nil, false
		}
		return applyWriteBinaryValue(op, left, right)
	}
	return nil, false
}

func splitWriteTopLevelBinary(expr string, ops string) (string, string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := len(expr) - 1; i >= 0; i-- {
		ch := expr[i]
		if ch == '\'' && !inDouble {
			if i > 0 && expr[i-1] == '\\' {
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			if i > 0 && expr[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case ')':
			depthParen++
		case '(':
			if depthParen > 0 {
				depthParen--
			}
		case ']':
			depthBracket++
		case '[':
			if depthBracket > 0 {
				depthBracket--
			}
		case '}':
			depthBrace++
		case '{':
			if depthBrace > 0 {
				depthBrace--
			}
		default:
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.ContainsRune(ops, rune(ch)) {
				if i == 0 {
					continue
				}
				if (ch == '+' || ch == '-') && writeUnarySignPosition(expr, i) {
					continue
				}
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+1:])
				if left == "" || right == "" {
					continue
				}
				return left, string(ch), right, true
			}
		}
	}
	return "", "", "", false
}

func writeUnarySignPosition(expr string, idx int) bool {
	if idx <= 0 || idx >= len(expr) {
		return false
	}
	for j := idx - 1; j >= 0; j-- {
		if expr[j] == ' ' || expr[j] == '\t' || expr[j] == '\n' || expr[j] == '\r' {
			continue
		}
		switch expr[j] {
		case '(', '[', '{', ',', ':', '+', '-', '*', '/', '%':
			return true
		default:
			return false
		}
	}
	return true
}

func applyWriteBinaryValue(op string, left any, right any) (any, bool) {
	if left == nil || right == nil {
		return nil, true
	}
	if op == "+" {
		if ls, ok := left.(string); ok {
			return ls + fmt.Sprint(right), true
		}
		if rs, ok := right.(string); ok {
			return fmt.Sprint(left) + rs, true
		}
	}
	lf, lok := writeToFloat64(left)
	rf, rok := writeToFloat64(right)
	if !lok || !rok {
		return nil, false
	}
	switch op {
	case "+":
		return lf + rf, true
	case "-":
		return lf - rf, true
	case "*":
		return lf * rf, true
	case "/":
		if rf == 0 {
			return nil, false
		}
		return lf / rf, true
	default:
		return nil, false
	}
}

func writeToFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func isWriteBindingIdentifierExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	for i := 0; i < len(expr); i++ {
		if !isCreateIdentChar(expr[i], i == 0) {
			return false
		}
	}
	return true
}

func resolveWriteTemporalConstructor(expr string, params map[string]any) (any, bool) {
	name, arg, ok := splitTopLevelFunctionCall(expr)
	if !ok {
		return nil, false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "date", "localtime", "time", "localdatetime", "datetime", "duration":
	default:
		return nil, false
	}
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, false
	}
	if strings.HasPrefix(arg, "{") && strings.HasSuffix(arg, "}") {
		body := strings.TrimSpace(arg[1 : len(arg)-1])
		if body == "" {
			return nil, false
		}
		values := map[string]any{}
		for _, pair := range splitTopLevelByComma(body) {
			key, valueExpr, ok := splitTopLevelKeyValue(pair)
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			value, ok := resolveWritePropertyValue(strings.TrimSpace(valueExpr), params)
			if !ok {
				return nil, false
			}
			values[key] = value
		}
		if len(values) == 0 {
			return nil, false
		}
		temporal := map[string]any{"__temporal_type": strings.ToLower(strings.TrimSpace(name))}
		for key, value := range values {
			temporal[key] = value
		}
		if rendered, ok := formatTemporalToString(temporal); ok {
			return rendered, true
		}
		return nil, false
	}
	return nil, false
}

func splitTopLevelFunctionCall(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}
	open := strings.IndexByte(expr, '(')
	if open <= 0 || !strings.HasSuffix(expr, ")") {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	for i := open; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && i == len(expr)-1 {
			name := strings.TrimSpace(expr[:open])
			if name == "" {
				return "", "", false
			}
			return name, expr[open+1 : len(expr)-1], true
		}
	}
	return "", "", false
}

func isEmbeddedApostrophe(raw string, idx int) bool {
	if idx <= 0 || idx+1 >= len(raw) {
		return false
	}
	prev := raw[idx-1]
	next := raw[idx+1]
	return isApostropheWordChar(prev) && isApostropheWordChar(next)
}

func isApostropheWordChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func formatTemporalToString(temporal map[string]any) (string, bool) {
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	switch typeName {
	case "date":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), true
	case "localtime":
		hour, _ := mapIntRuntime(temporal, "hour")
		minute, _ := mapIntRuntime(temporal, "minute")
		second, _ := mapIntRuntime(temporal, "second")
		nano := combineNanosecondsRuntime(temporal)
		return formatClockPartsRuntime(hour, minute, second, nano), true
	case "time":
		hour, _ := mapIntRuntime(temporal, "hour")
		minute, _ := mapIntRuntime(temporal, "minute")
		second, _ := mapIntRuntime(temporal, "second")
		nano := combineNanosecondsRuntime(temporal)
		tzName := runtimeTimezoneString(temporal)
		if tzName == "" {
			tzName = "Z"
		}
		offsetRendered := tzName
		if offset, err := parseOffsetSecondsRuntime(tzName); err == nil {
			offsetRendered = formatOffsetStringRuntime(offset)
		}
		return formatClockPartsRuntime(hour, minute, second, nano) + offsetRendered, true
	case "localdatetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapIntRuntime(temporal, "hour")
		minute, _ := mapIntRuntime(temporal, "minute")
		second, _ := mapIntRuntime(temporal, "second")
		nano := combineNanosecondsRuntime(temporal)
		return fmt.Sprintf("%04d-%02d-%02dT%s", y, m, d, formatClockPartsRuntime(hour, minute, second, nano)), true
	case "datetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapIntRuntime(temporal, "hour")
		minute, _ := mapIntRuntime(temporal, "minute")
		second, _ := mapIntRuntime(temporal, "second")
		nano := combineNanosecondsRuntime(temporal)
		tzName := runtimeTimezoneString(temporal)
		if tzName == "" {
			tzName = "Z"
		}
		clock := formatClockPartsRuntime(hour, minute, second, nano)
		if offset, err := parseOffsetSecondsRuntime(tzName); err == nil {
			return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, formatOffsetStringRuntime(offset)), true
		}
		if loc, err := time.LoadLocation(tzName); err == nil {
			t := time.Date(y, time.Month(m), d, hour, minute, second, nano, loc)
			_, offset := t.Zone()
			return fmt.Sprintf("%04d-%02d-%02dT%s%s[%s]", y, m, d, clock, formatOffsetStringRuntime(offset), tzName), true
		}
		return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, tzName), true
	case "duration":
		return formatDurationFromMapRuntime(temporal), true
	default:
		return "", false
	}
}

func runtimeTimezoneString(temporal map[string]any) string {
	raw, ok := temporal["timezone"]
	if !ok || raw == nil {
		return ""
	}
	tz := strings.TrimSpace(fmt.Sprint(raw))
	if strings.EqualFold(tz, "<nil>") {
		return ""
	}
	return tz
}

func formatDurationFromMapRuntime(temporal map[string]any) string {
	years := mapFloatRuntime(temporal, "years")
	months := mapFloatRuntime(temporal, "months")
	weeks := mapFloatRuntime(temporal, "weeks")
	days := mapFloatRuntime(temporal, "days") + 7*weeks
	hours := mapFloatRuntime(temporal, "hours")
	minutes := mapFloatRuntime(temporal, "minutes")
	seconds := mapFloatRuntime(temporal, "seconds")
	seconds += mapFloatRuntime(temporal, "milliseconds") / 1000
	seconds += mapFloatRuntime(temporal, "microseconds") / 1_000_000
	seconds += mapFloatRuntime(temporal, "nanoseconds") / 1_000_000_000

	var b strings.Builder
	b.WriteString("P")
	if years != 0 {
		b.WriteString(formatSignedDurationComponentRuntime(years, "Y"))
	}
	if months != 0 {
		b.WriteString(formatSignedDurationComponentRuntime(months, "M"))
	}
	if days != 0 {
		b.WriteString(formatSignedDurationComponentRuntime(days, "D"))
	}
	if hours != 0 || minutes != 0 || seconds != 0 || (years == 0 && months == 0 && days == 0) {
		b.WriteString("T")
		if hours != 0 {
			b.WriteString(formatSignedDurationComponentRuntime(hours, "H"))
		}
		if minutes != 0 {
			b.WriteString(formatSignedDurationComponentRuntime(minutes, "M"))
		}
		if seconds != 0 || (hours == 0 && minutes == 0) {
			b.WriteString(formatDurationSecondsRuntime(seconds))
		}
	}
	if b.Len() == 1 {
		b.WriteString("T0S")
	}
	return b.String()
}

func formatSignedDurationComponentRuntime(value float64, suffix string) string {
	if value == 0 {
		return ""
	}
	if math.Trunc(value) == value {
		return fmt.Sprintf("%d%s", int64(value), suffix)
	}
	return fmt.Sprintf("%s%s", strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", value), "0"), "."), suffix)
}

func formatDurationSecondsRuntime(seconds float64) string {
	if seconds == 0 {
		return "0S"
	}
	whole := math.Trunc(seconds)
	frac := seconds - whole
	if frac == 0 {
		return fmt.Sprintf("%dS", int64(whole))
	}
	fracStr := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.9f", math.Abs(frac)), "0"), ".")
	if whole == 0 {
		if seconds < 0 {
			return "-0." + strings.TrimPrefix(fracStr, "0.") + "S"
		}
		return "0." + strings.TrimPrefix(fracStr, "0.") + "S"
	}
	if seconds < 0 {
		return fmt.Sprintf("-%d.%sS", int64(math.Abs(whole)), strings.TrimPrefix(fracStr, "0."))
	}
	return fmt.Sprintf("%d.%sS", int64(whole), strings.TrimPrefix(fracStr, "0."))
}

func mapIntRuntime(value map[string]any, key string) (int, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	if f, ok := numericValueRuntime(raw); ok {
		return int(math.Trunc(f)), true
	}
	return 0, false
}

func mapFloatRuntime(value map[string]any, key string) float64 {
	raw, ok := value[key]
	if !ok {
		return 0
	}
	if f, ok := numericValueRuntime(raw); ok {
		return f
	}
	return 0
}

func combineNanosecondsRuntime(value map[string]any) int {
	if f, ok := numericValueRuntime(value["nanosecond"]); ok {
		return int(math.Trunc(f))
	}
	if f, ok := numericValueRuntime(value["nanoseconds"]); ok {
		return int(math.Trunc(f))
	}
	return 0
}

func numericValueRuntime(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func resolveDateFromTemporalMap(value map[string]any) (int, int, int, bool) {
	y, yOK := mapIntRuntime(value, "year")
	m, mOK := mapIntRuntime(value, "month")
	d, dOK := mapIntRuntime(value, "day")
	if !yOK || !mOK || !dOK {
		return 0, 0, 0, false
	}
	return y, m, d, true
}

func formatClockPartsRuntime(hour, minute, second, nano int) string {
	base := fmt.Sprintf("%02d:%02d", hour, minute)
	if second != 0 || nano != 0 {
		base += fmt.Sprintf(":%02d", second)
	}
	if nano != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nano), "0")
	}
	return base
}

func parseOffsetSecondsRuntime(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "Z" || raw == "z" {
		return 0, nil
	}
	if len(raw) != 6 && len(raw) != 9 {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[0] != '+' && raw[0] != '-' {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[3] != ':' {
		return 0, fmt.Errorf("invalid offset")
	}
	hours, err := strconv.Atoi(raw[1:3])
	if err != nil {
		return 0, err
	}
	minutes, err := strconv.Atoi(raw[4:6])
	if err != nil {
		return 0, err
	}
	seconds := 0
	if len(raw) == 9 {
		if raw[6] != ':' {
			return 0, fmt.Errorf("invalid offset")
		}
		seconds, err = strconv.Atoi(raw[7:9])
		if err != nil {
			return 0, err
		}
	}
	if hours > 18 || minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("invalid offset")
	}
	total := hours*3600 + minutes*60 + seconds
	if raw[0] == '-' {
		total = -total
	}
	return total, nil
}

func formatOffsetStringRuntime(totalSeconds int) string {
	if totalSeconds == 0 {
		return "Z"
	}
	sign := "+"
	if totalSeconds < 0 {
		sign = "-"
		totalSeconds = -totalSeconds
	}
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	if seconds == 0 {
		return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
}

func parseQuotedPropertyString(expr string) (string, bool) {
	if len(expr) < 2 {
		return "", false
	}
	quote := expr[0]
	if (quote != '\'' && quote != '"') || expr[len(expr)-1] != quote {
		return "", false
	}
	body := expr[1 : len(expr)-1]
	var b strings.Builder
	b.Grow(len(body))
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if quote == '\'' && ch == '\'' && i+1 < len(body) && body[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		if ch == '\\' && i+1 < len(body) {
			i++
			switch body[i] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u', 'U':
				hexLen := 4
				if body[i] == 'U' {
					hexLen = 8
				}
				if i+hexLen >= len(body) {
					return "", false
				}
				rawHex := body[i+1 : i+1+hexLen]
				codepoint, err := strconv.ParseInt(rawHex, 16, 32)
				if err != nil {
					return "", false
				}
				b.WriteRune(rune(codepoint))
				i += hexLen
			default:
				b.WriteByte(body[i])
			}
			continue
		}
		b.WriteByte(ch)
	}
	return b.String(), true
}

func encodePropertyValue(value any) []byte {
	switch typed := value.(type) {
	case nil:
		return []byte("null")
	case []byte:
		return append([]byte(nil), typed...)
	case string:
		return []byte(typed)
	case bool:
		if typed {
			return []byte("true")
		}
		return []byte("false")
	case int:
		return []byte(strconv.Itoa(typed))
	case int64:
		return []byte(strconv.FormatInt(typed, 10))
	case int32:
		return []byte(strconv.FormatInt(int64(typed), 10))
	case float64:
		return []byte(strconv.FormatFloat(typed, 'f', -1, 64))
	case float32:
		return []byte(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	default:
		return []byte(fmt.Sprint(value))
	}
}

func isSupportedRuntimePropertyValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case []byte, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	case []any:
		for _, item := range typed {
			if !isSupportedRuntimeListElement(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isSupportedRuntimeListElement(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case []byte, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	case []any:
		for _, item := range typed {
			if !isSupportedRuntimeListElement(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isCreateLikeWriteKind(kind string) bool {
	kind = strings.TrimSpace(kind)
	return strings.EqualFold(kind, "CREATE") || strings.EqualFold(kind, "MERGE")
}

func allocateAnonymousVertexID(nextAutoVertexID *int) string {
	if nextAutoVertexID == nil {
		return "auto-v-1"
	}
	*nextAutoVertexID = *nextAutoVertexID + 1
	return fmt.Sprintf("auto-v-%d", *nextAutoVertexID)
}

func allocateAnonymousVertexIDForTenant(ctx context.Context, sink runtimestorage.WriteSink, tenant string, nextAutoVertexID *int) string {
	id, _ := allocateAnonymousVertexIDForTenantWithReserved(ctx, sink, tenant, nextAutoVertexID, nil)
	return id
}

func allocateAnonymousVertexIDForTenantChecked(ctx context.Context, sink runtimestorage.WriteSink, tenant string, nextAutoVertexID *int) (string, bool) {
	return allocateAnonymousVertexIDForTenantWithReserved(ctx, sink, tenant, nextAutoVertexID, nil)
}

func allocateAnonymousVertexIDForTenantWithReserved(ctx context.Context, sink runtimestorage.WriteSink, tenant string, nextAutoVertexID *int, reserved map[string]struct{}) (string, bool) {
	lookup, _ := sink.(writeLookupTx)
	if lookup == nil {
		for attempts := 0; attempts < 100000; attempts++ {
			candidate := allocateAnonymousVertexID(nextAutoVertexID)
			if _, blocked := reserved[candidate]; blocked {
				continue
			}
			return candidate, false
		}
		return allocateAnonymousVertexID(nextAutoVertexID), false
	}
	for attempts := 0; attempts < 100000; attempts++ {
		candidate := allocateAnonymousVertexID(nextAutoVertexID)
		if _, blocked := reserved[candidate]; blocked {
			continue
		}
		existing, err := lookup.GetVertex(ctx, tenant, candidate)
		if err != nil {
			if !graph.IsKind(err, graph.ErrKindNotFound) {
				return candidate, false
			}
			existing = nil
		}
		if existing == nil {
			return candidate, true
		}
	}
	return allocateAnonymousVertexID(nextAutoVertexID), false
}

func allocateCreateEdgeIDWithLookup(ctx context.Context, lookup writeLookupTx, tenant, srcID, edgeType, dstID string, createEdgeCounts map[string]int) string {
	base := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	next := 1
	if createEdgeCounts != nil {
		next = createEdgeCounts[base] + 1
	}
	for {
		candidate := base
		if next > 1 {
			candidate = fmt.Sprintf("%s|auto-e-%d", base, next)
		}
		if lookup == nil {
			if createEdgeCounts != nil {
				createEdgeCounts[base] = next
			}
			return candidate
		}
		edge, err := lookup.GetEdge(ctx, tenant, candidate)
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				if createEdgeCounts != nil {
					createEdgeCounts[base] = next
				}
				return candidate
			}
			if createEdgeCounts != nil {
				createEdgeCounts[base] = next
			}
			return candidate
		}
		if edge == nil {
			if createEdgeCounts != nil {
				createEdgeCounts[base] = next
			}
			return candidate
		}
		next++
	}
}

func applyEdgeWriteEvent(ctx context.Context, sink runtimestorage.WriteSink, tenant string, event operators.WriteEvent, nextAutoVertexID *int, createEdgeCounts map[string]int) error {
	if event.Edge == nil {
		return nil
	}
	edgeType := strings.TrimSpace(event.Edge.Type)
	leftID := resolveEntityID(event.Edge.LeftVar, event.Edge.LeftIDParam, event.Bindings, event.ResolvedParams)
	rightID := resolveEntityID(event.Edge.RightVar, event.Edge.RightIDParam, event.Bindings, event.ResolvedParams)
	leftAllocated := false
	rightAllocated := false
	if edgeType == "" {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE") {
		if leftID == "" {
			leftID = allocateAnonymousVertexIDForTenant(ctx, sink, tenant, nextAutoVertexID)
			leftAllocated = leftID != ""
		}
		if rightID == "" {
			rightID = allocateAnonymousVertexIDForTenant(ctx, sink, tenant, nextAutoVertexID)
			rightAllocated = rightID != ""
		}
	}
	if len(event.Edge.LeftLabels) > 0 || leftAllocated {
		if err := sink.PutVertex(ctx, &graph.Vertex{Tenant: tenant, ID: leftID, Labels: append([]string(nil), event.Edge.LeftLabels...)}); err != nil {
			return err
		}
	}
	if len(event.Edge.RightLabels) > 0 || rightAllocated {
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
	edgeID := fmt.Sprintf("%s|%s|%s|%s", tenant, srcID, edgeType, dstID)
	if strings.EqualFold(strings.TrimSpace(event.Kind), "CREATE") {
		lookup, _ := sink.(writeLookupTx)
		edgeID = allocateCreateEdgeIDWithLookup(ctx, lookup, tenant, srcID, edgeType, dstID, createEdgeCounts)
	}
	return sink.PutEdge(ctx, &graph.Edge{Tenant: tenant, ID: edgeID, Type: edgeType, SrcID: srcID, DstID: dstID})
}

func findDirectedExistingEdgeID(ctx context.Context, sink runtimestorage.WriteSink, lookup writeLookupTx, tenant, srcID, dstID, edgeType string, props graph.PropertyMap, excluded map[string]struct{}) (string, error) {
	// Fast no-match short-circuit: for property-agnostic MERGE checks with no
	// excluded IDs, endpoint existence can avoid an adjacency scan entirely.
	if len(props) == 0 && len(excluded) == 0 {
		if probe, ok := sink.(writeEdgeExistenceTx); ok {
			exists, err := probe.HasDirectedEdgeBetween(ctx, tenant, srcID, dstID, edgeType)
			if err != nil {
				return "", err
			}
			if !exists {
				return "", nil
			}
		}
	}

	found := ""
	err := sink.ScanAdjacencyLinks(ctx, tenant, srcID, graph.EdgeDirectionOut, edgeType, 0, func(edgeID, peerID string) error {
		if strings.TrimSpace(peerID) != dstID {
			return nil
		}
		candidate := strings.TrimSpace(edgeID)
		if _, skip := excluded[candidate]; skip {
			return nil
		}
		if lookup != nil {
			existing, err := lookup.GetEdge(ctx, tenant, candidate)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					return nil
				}
				return err
			}
			if existing == nil {
				return nil
			}
			if len(props) > 0 && !mergeEdgeMatches(existing, props) {
				return nil
			}
			candidate = strings.TrimSpace(existing.ID)
			if candidate == "" {
				return nil
			}
			if _, skip := excluded[candidate]; skip {
				return nil
			}
		}
		found = candidate
		return errMergeVertexMatchFound
	})
	if err != nil && !errors.Is(err, errMergeVertexMatchFound) {
		return "", err
	}
	return found, nil
}

func mergeEdgeMatches(edge *graph.Edge, props graph.PropertyMap) bool {
	if edge == nil {
		return false
	}
	for key, value := range props {
		if key == "" {
			continue
		}
		existing, ok := edge.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(existing, value) {
			return false
		}
	}
	return true
}

func isUndirectedMergeEdgePattern(pattern string) bool {
	raw := strings.TrimSpace(pattern)
	if raw == "" {
		return false
	}
	return strings.Contains(raw, "]-(") && !strings.Contains(raw, "]->") && !strings.Contains(raw, "<-[")
}

func resolveEntityID(varName, idParam string, bindings map[string]any, resolvedParams map[string]any) string {
	varName = strings.TrimSpace(varName)
	if varName != "" {
		if value, ok := bindings[varName]; ok {
			if id := scalarString(value); id != "" {
				return id
			}
		}
		if value, ok := bindings[varName+".id"]; ok {
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
	case *graph.Vertex:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case *graph.Edge:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.ID)
	case map[string]any:
		if id, ok := typed["id"]; ok {
			return scalarString(id)
		}
		if id, ok := typed["ID"]; ok {
			return scalarString(id)
		}
		return ""
	case map[string]string:
		if id, ok := typed["id"]; ok {
			return strings.TrimSpace(id)
		}
		if id, ok := typed["ID"]; ok {
			return strings.TrimSpace(id)
		}
		return ""
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
