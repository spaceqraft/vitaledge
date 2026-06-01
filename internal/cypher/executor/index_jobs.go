package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/paegun/vitaledge/internal/graph"
)

const (
	indexJobTenant               = "__system"
	indexJobSchema               = "__index_jobs"
	edgeIndexBuildJobProperty    = "edge_property_build"
	edgeIndexBuildJobEntityClass = "edge_index_build_pending"
	indexBuildWorkerPollInterval = 2 * time.Second
)

type edgeIndexBuildJob struct {
	Tenant   string
	EdgeType string
	Property string
}

type edgeIndexBuildJobState struct {
	Revision           int64  `json:"revision"`
	CheckpointVertexID string `json:"checkpoint_vertex_id"`
	IndexedEdges       int    `json:"indexed_edges"`
	Completed          bool   `json:"completed"`
}

type edgeIndexBuildJobRecord struct {
	Job   edgeIndexBuildJob
	State edgeIndexBuildJobState
}

type edgeIndexBuildProgress struct {
	Tenant             string
	EdgeType           string
	Property           string
	Pending            bool
	CheckpointVertexID string
	IndexedEdges       int
}

func edgeIndexBuildJobID(tenant, edgeType, property string) string {
	return url.QueryEscape(strings.TrimSpace(tenant)) + "|" + url.QueryEscape(strings.TrimSpace(edgeType)) + "|" + url.QueryEscape(strings.TrimSpace(property))
}

func parseEdgeIndexBuildJobID(id string) (edgeIndexBuildJob, bool) {
	parts := strings.Split(id, "|")
	if len(parts) != 3 {
		return edgeIndexBuildJob{}, false
	}
	tenant, err := url.QueryUnescape(parts[0])
	if err != nil {
		return edgeIndexBuildJob{}, false
	}
	edgeType, err := url.QueryUnescape(parts[1])
	if err != nil {
		return edgeIndexBuildJob{}, false
	}
	property, err := url.QueryUnescape(parts[2])
	if err != nil {
		return edgeIndexBuildJob{}, false
	}
	tenant = strings.TrimSpace(tenant)
	edgeType = strings.TrimSpace(edgeType)
	property = strings.TrimSpace(property)
	if tenant == "" || edgeType == "" || property == "" {
		return edgeIndexBuildJob{}, false
	}
	return edgeIndexBuildJob{Tenant: tenant, EdgeType: edgeType, Property: property}, true
}

func (e *Executor) enqueueEdgeIndexBuildJob(ctx context.Context, tenant, edgeType, property string) error {
	if e == nil || e.store == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	jobID := edgeIndexBuildJobID(tenant, edgeType, property)
	revision := int64(1)
	if records, err := e.listAllEdgeIndexBuildJobRecords(ctx); err == nil {
		for _, record := range records {
			if edgeIndexBuildJobID(record.Job.Tenant, record.Job.EdgeType, record.Job.Property) != jobID {
				continue
			}
			if record.State.Revision >= revision {
				revision = record.State.Revision + 1
			}
		}
	}
	entry := &graph.PropertyIndexEntry{
		Tenant:      indexJobTenant,
		Schema:      indexJobSchema,
		Property:    edgeIndexBuildJobProperty,
		Value:       edgeIndexBuildJobStateBytes(edgeIndexBuildJobState{Revision: revision}),
		EntityID:    jobID,
		EntityClass: edgeIndexBuildJobEntityClass,
	}
	return e.store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutPropertyIndex(ctx, entry)
	})
}

func (e *Executor) dequeueEdgeIndexBuildJob(ctx context.Context, job edgeIndexBuildJob, state edgeIndexBuildJobState) error {
	if e == nil || e.store == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	entry := &graph.PropertyIndexEntry{
		Tenant:      indexJobTenant,
		Schema:      indexJobSchema,
		Property:    edgeIndexBuildJobProperty,
		Value:       edgeIndexBuildJobStateBytes(edgeIndexBuildJobState{Revision: state.Revision + 1, Completed: true, IndexedEdges: state.IndexedEdges, CheckpointVertexID: state.CheckpointVertexID}),
		EntityID:    edgeIndexBuildJobID(job.Tenant, job.EdgeType, job.Property),
		EntityClass: edgeIndexBuildJobEntityClass,
	}
	return e.store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutPropertyIndex(ctx, entry)
	})
}

func (e *Executor) listEdgeIndexBuildJobs(ctx context.Context) ([]edgeIndexBuildJob, error) {
	records, err := e.listEdgeIndexBuildJobRecords(ctx)
	if err != nil {
		return nil, err
	}
	jobs := make([]edgeIndexBuildJob, 0, len(records))
	for _, record := range records {
		jobs = append(jobs, record.Job)
	}
	return jobs, nil
}

func (e *Executor) listEdgeIndexBuildJobRecords(ctx context.Context) ([]edgeIndexBuildJobRecord, error) {
	records, err := e.listAllEdgeIndexBuildJobRecords(ctx)
	if err != nil {
		return nil, err
	}
	pending := make([]edgeIndexBuildJobRecord, 0, len(records))
	for _, record := range records {
		if record.State.Completed {
			continue
		}
		pending = append(pending, record)
	}
	return pending, nil
}

func (e *Executor) listAllEdgeIndexBuildJobRecords(ctx context.Context) ([]edgeIndexBuildJobRecord, error) {
	if e == nil || e.store == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	records := []edgeIndexBuildJobRecord{}
	byID := map[string]edgeIndexBuildJobRecord{}
	err := e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanPropertyIndexAll(ctx, indexJobTenant, indexJobSchema, edgeIndexBuildJobProperty, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry == nil {
				return nil
			}
			job, ok := parseEdgeIndexBuildJobID(strings.TrimSpace(entry.EntityID))
			if !ok {
				return nil
			}
			id := edgeIndexBuildJobID(job.Tenant, job.EdgeType, job.Property)
			state := edgeIndexBuildJobStateFromBytes(entry.Value)
			if existing, exists := byID[id]; exists && existing.State.Revision > state.Revision {
				return nil
			}
			byID[id] = edgeIndexBuildJobRecord{Job: job, State: state}
			return nil
		})
	})
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, nil
		}
		return nil, err
	}
	for _, record := range byID {
		records = append(records, record)
	}
	return records, nil
}

func (e *Executor) processPendingEdgeIndexBuildJobs(ctx context.Context) (int, error) {
	records, err := e.listEdgeIndexBuildJobRecords(ctx)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, record := range records {
		if e.indexCatalog != nil {
			e.indexCatalog.AddEdgePropertyIndex(record.Job.Tenant, record.Job.EdgeType, record.Job.Property)
		}
		completed, err := e.processPendingEdgeIndexBuildJob(ctx, record)
		if err != nil {
			return processed, err
		}
		if completed {
			processed++
		}
	}
	return processed, nil
}

func (e *Executor) processPendingEdgeIndexBuildJob(ctx context.Context, record edgeIndexBuildJobRecord) (bool, error) {
	started := time.Now()
	lastCheckpoint := strings.TrimSpace(record.State.CheckpointVertexID)
	indexedTotal := record.State.IndexedEdges
	verticesProcessed := 0
	const maxVerticesPerPass = 64
	for verticesProcessed < maxVerticesPerPass {
		nextVertexID, indexed, done, err := e.backfillEdgePropertyIndexNextVertex(ctx, record.Job.Tenant, record.Job.EdgeType, record.Job.Property, lastCheckpoint)
		if err != nil {
			return false, fmt.Errorf("backfill edge index job %s/%s/%s: %w", record.Job.Tenant, record.Job.EdgeType, record.Job.Property, err)
		}
		if done {
			if err := e.dequeueEdgeIndexBuildJob(ctx, record.Job, edgeIndexBuildJobState{Revision: record.State.Revision, CheckpointVertexID: lastCheckpoint, IndexedEdges: indexedTotal}); err != nil {
				return false, fmt.Errorf("finalize edge index job %s/%s/%s: %w", record.Job.Tenant, record.Job.EdgeType, record.Job.Property, err)
			}
			log.Printf("index build worker: completed edge index backfill tenant=%s edge_type=%s property=%s checkpoint=%q indexed_entries=%d duration=%s", record.Job.Tenant, record.Job.EdgeType, record.Job.Property, lastCheckpoint, indexedTotal, time.Since(started).Round(time.Millisecond))
			return true, nil
		}
		indexedTotal += indexed
		lastCheckpoint = nextVertexID
		nextState := edgeIndexBuildJobState{Revision: record.State.Revision + 1, CheckpointVertexID: lastCheckpoint, IndexedEdges: indexedTotal}
		if err := e.replaceEdgeIndexBuildJobState(ctx, record.Job, record.State, nextState); err != nil {
			return false, fmt.Errorf("update edge index job %s/%s/%s: %w", record.Job.Tenant, record.Job.EdgeType, record.Job.Property, err)
		}
		record.State = nextState
		verticesProcessed++
		if verticesProcessed == 1 || verticesProcessed%16 == 0 {
			log.Printf("index build worker: checkpointed edge index tenant=%s edge_type=%s property=%s checkpoint=%q indexed_edges=%d vertices_processed=%d duration=%s", record.Job.Tenant, record.Job.EdgeType, record.Job.Property, lastCheckpoint, indexedTotal, verticesProcessed, time.Since(started).Round(time.Millisecond))
		}
	}
	return false, nil
}

func (e *Executor) backfillEdgePropertyIndexNextVertex(ctx context.Context, tenant, edgeType, property, startAfterVertexID string) (string, int, bool, error) {
	var nextVertexID string
	err := e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVerticesFrom(ctx, tenant, startAfterVertexID, 1, func(vertex *graph.Vertex) error {
			if vertex == nil || strings.TrimSpace(vertex.ID) == "" {
				return nil
			}
			nextVertexID = vertex.ID
			return nil
		})
	})
	if err != nil {
		return "", 0, false, err
	}
	if strings.TrimSpace(nextVertexID) == "" {
		return "", 0, true, nil
	}
	indexed, err := e.backfillEdgePropertyIndexForVertex(ctx, tenant, nextVertexID, edgeType, property)
	if err != nil {
		return "", 0, false, err
	}
	return nextVertexID, indexed, false, nil
}

func (e *Executor) replaceEdgeIndexBuildJobState(ctx context.Context, job edgeIndexBuildJob, previousState, nextState edgeIndexBuildJobState) error {
	if e == nil || e.store == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "executor requires a graph store", nil)
	}
	return e.store.Update(ctx, func(tx graph.Tx) error {
		current := &graph.PropertyIndexEntry{
			Tenant:      indexJobTenant,
			Schema:      indexJobSchema,
			Property:    edgeIndexBuildJobProperty,
			Value:       edgeIndexBuildJobStateBytes(previousState),
			EntityID:    edgeIndexBuildJobID(job.Tenant, job.EdgeType, job.Property),
			EntityClass: edgeIndexBuildJobEntityClass,
		}
		if err := tx.DeletePropertyIndex(ctx, current); err != nil {
			return err
		}
		next := &graph.PropertyIndexEntry{
			Tenant:      indexJobTenant,
			Schema:      indexJobSchema,
			Property:    edgeIndexBuildJobProperty,
			Value:       edgeIndexBuildJobStateBytes(nextState),
			EntityID:    edgeIndexBuildJobID(job.Tenant, job.EdgeType, job.Property),
			EntityClass: edgeIndexBuildJobEntityClass,
		}
		return tx.PutPropertyIndex(ctx, next)
	})
}

func (e *Executor) estimateEdgeIndexBuildProgress(ctx context.Context, job edgeIndexBuildJob, pending bool) (edgeIndexBuildProgress, error) {
	progress := edgeIndexBuildProgress{
		Tenant:             job.Tenant,
		EdgeType:           job.EdgeType,
		Property:           job.Property,
		Pending:            pending,
		CheckpointVertexID: "",
	}
	records, err := e.listEdgeIndexBuildJobRecords(ctx)
	if err == nil {
		for _, record := range records {
			if record.Job == job {
				progress.CheckpointVertexID = record.State.CheckpointVertexID
				progress.IndexedEdges = record.State.IndexedEdges
				break
			}
		}
	}
	err = e.store.View(ctx, func(tx graph.Tx) error {
		indexed := 0
		if err := tx.ScanPropertyIndexAll(ctx, job.Tenant, job.EdgeType, job.Property, 0, func(entry *graph.PropertyIndexEntry) error {
			if entry != nil && entry.EntityClass == "edge" {
				indexed++
			}
			return nil
		}); err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				progress.IndexedEdges = 0
				return nil
			}
			return err
		}
		progress.IndexedEdges = indexed
		return nil
	})
	if err != nil {
		return edgeIndexBuildProgress{}, err
	}
	return progress, nil
}

func (e *Executor) listPendingEdgeIndexBuildProgress(ctx context.Context) ([]edgeIndexBuildProgress, error) {
	jobs, err := e.listEdgeIndexBuildJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]edgeIndexBuildProgress, 0, len(jobs))
	for _, job := range jobs {
		progress, err := e.estimateEdgeIndexBuildProgress(ctx, job, true)
		if err != nil {
			return nil, err
		}
		out = append(out, progress)
	}
	return out, nil
}

func edgeIndexBuildJobStateBytes(state edgeIndexBuildJobState) []byte {
	buf, err := json.Marshal(state)
	if err != nil {
		return []byte(`{"revision":0,"checkpoint_vertex_id":"","indexed_edges":0,"completed":false}`)
	}
	return buf
}

func edgeIndexBuildJobStateFromBytes(buf []byte) edgeIndexBuildJobState {
	if len(buf) == 0 {
		return edgeIndexBuildJobState{}
	}
	var state edgeIndexBuildJobState
	if err := json.Unmarshal(buf, &state); err != nil {
		return edgeIndexBuildJobState{}
	}
	if state.Revision < 0 {
		state.Revision = 0
	}
	if state.IndexedEdges < 0 {
		state.IndexedEdges = 0
	}
	state.CheckpointVertexID = strings.TrimSpace(state.CheckpointVertexID)
	return state
}

func (e *Executor) StartIndexBuildWorker(ctx context.Context) {
	if e == nil || e.store == nil {
		return
	}
	e.indexJobWorkerOnce.Do(func() {
		workerCtx := context.Background()
		if ctx != nil {
			workerCtx = ctx
		}
		workerCtx, cancel := context.WithCancel(workerCtx)
		e.indexJobWorkerMu.Lock()
		e.indexJobWorkerCancel = cancel
		e.indexJobWorkerMu.Unlock()
		go e.runIndexBuildWorker(workerCtx)
	})
}

func (e *Executor) runIndexBuildWorker(ctx context.Context) {
	if _, err := e.processPendingEdgeIndexBuildJobs(ctx); err != nil {
		log.Printf("index build worker initial pass error: %v", err)
	}

	ticker := time.NewTicker(indexBuildWorkerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := e.processPendingEdgeIndexBuildJobs(ctx); err != nil {
				log.Printf("index build worker pass error: %v", err)
			}
		}
	}
}

func (e *Executor) StopIndexBuildWorker() {
	if e == nil {
		return
	}
	e.indexJobWorkerMu.Lock()
	cancel := e.indexJobWorkerCancel
	e.indexJobWorkerCancel = nil
	e.indexJobWorkerMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
