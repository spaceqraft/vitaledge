package pebblestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/paegun/vitaledge/internal/graph"
	"github.com/paegun/vitaledge/internal/graph/keyspace"
)

func TestVertexEdgeCRUDAndAdjacency(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "g1", Labels: []string{"Group"}}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e1",
			Type:   "MEMBER_OF",
			SrcID:  "u1",
			DstID:  "g1",
		})
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		edge, err := tx.GetEdge(ctx, "acme", "e1")
		if err != nil {
			return err
		}
		if edge.Type != "MEMBER_OF" {
			t.Fatalf("unexpected edge type: %s", edge.Type)
		}

		outCount := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			outCount++
			return nil
		}); err != nil {
			return err
		}
		if outCount != 1 {
			t.Fatalf("expected 1 out edge, got %d", outCount)
		}

		inCount := 0
		if err := tx.ScanInEdges(ctx, "acme", "g1", "MEMBER_OF", 10, func(edge *graph.Edge) error {
			inCount++
			return nil
		}); err != nil {
			return err
		}
		if inCount != 1 {
			t.Fatalf("expected 1 in edge, got %d", inCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeleteEdge(ctx, "acme", "e1")
	})
	if err != nil {
		t.Fatalf("delete edge failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		if _, err := tx.GetEdge(ctx, "acme", "e1"); !graph.IsKind(err, graph.ErrKindNotFound) {
			return errors.New("expected edge to be absent")
		}
		count := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			count++
			return nil
		}); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("expected no out edges after delete")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("post-delete verification failed: %v", err)
	}
}

func TestScanVertices(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v1", Labels: []string{"User"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v2", Labels: []string{"Group"}}); err != nil {
			return err
		}
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "other", ID: "v3", Labels: []string{"User"}}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		seen := map[string]bool{}
		if err := tx.ScanVertices(ctx, "acme", 0, func(v *graph.Vertex) error {
			seen[v.ID] = true
			return nil
		}); err != nil {
			return err
		}
		if len(seen) != 2 || !seen["v1"] || !seen["v2"] {
			return fmt.Errorf("unexpected scanned vertices: %#v", seen)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan vertices failed: %v", err)
	}
}

func TestDurabilityAcrossRestart(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-durable"})
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	err = store.View(ctx, func(tx graph.Tx) error {
		v, err := tx.GetVertex(ctx, "acme", "v-durable")
		if err != nil {
			return err
		}
		if v.ID != "v-durable" {
			return errors.New("unexpected vertex id")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("durability verification failed: %v", err)
	}
}

func TestPropertyIndexRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	entry := &graph.PropertyIndexEntry{
		Tenant:      "acme",
		Schema:      "User",
		Property:    "email",
		Value:       []byte("alice@acme.io"),
		EntityID:    "u1",
		EntityClass: "vertex",
	}

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutPropertyIndex(ctx, entry)
	})
	if err != nil {
		t.Fatalf("put index failed: %v", err)
	}

	prefix := keyspace.PropertyIndexPrefix("acme", "User", "email")
	if got := countByPrefix(t, store, prefix); got != 1 {
		t.Fatalf("expected one index key, got %d", got)
	}

	err = store.Update(ctx, func(tx graph.Tx) error {
		return tx.DeletePropertyIndex(ctx, entry)
	})
	if err != nil {
		t.Fatalf("delete index failed: %v", err)
	}

	if got := countByPrefix(t, store, prefix); got != 0 {
		t.Fatalf("expected zero index keys, got %d", got)
	}
}

func TestReadOnlyTxRejectsWrites(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	tx, err := store.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		t.Fatalf("begin readonly tx failed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-ro"})
	if !graph.IsKind(err, graph.ErrKindUnsupported) {
		t.Fatalf("expected unsupported error kind, got %v", err)
	}
}

func TestUpdateRollsBackOnCallbackError(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	boom := errors.New("boom")
	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-rollback"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected callback error, got %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		_, err := tx.GetVertex(ctx, "acme", "v-rollback")
		if !graph.IsKind(err, graph.ErrKindNotFound) {
			return fmt.Errorf("expected not found after rollback, got %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("rollback verification failed: %v", err)
	}
}

func TestEdgeUpdateRewritesAdjacencyIndexes(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e-rewrite",
			Type:   "MEMBER_OF",
			SrcID:  "u1",
			DstID:  "g1",
		}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e-rewrite",
			Type:   "OWNS",
			SrcID:  "u2",
			DstID:  "g2",
		})
	})
	if err != nil {
		t.Fatalf("edge rewrite failed: %v", err)
	}

	err = store.View(ctx, func(tx graph.Tx) error {
		oldOut := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u1", "", 10, func(edge *graph.Edge) error {
			oldOut++
			return nil
		}); err != nil {
			return err
		}
		if oldOut != 0 {
			return fmt.Errorf("expected stale out adjacency removed, got %d", oldOut)
		}

		newOut := 0
		if err := tx.ScanOutEdges(ctx, "acme", "u2", "OWNS", 10, func(edge *graph.Edge) error {
			newOut++
			if edge.ID != "e-rewrite" {
				return fmt.Errorf("unexpected edge id %s", edge.ID)
			}
			return nil
		}); err != nil {
			return err
		}
		if newOut != 1 {
			return fmt.Errorf("expected one rewritten out adjacency, got %d", newOut)
		}

		oldIn := 0
		if err := tx.ScanInEdges(ctx, "acme", "g1", "", 10, func(edge *graph.Edge) error {
			oldIn++
			return nil
		}); err != nil {
			return err
		}
		if oldIn != 0 {
			return fmt.Errorf("expected stale in adjacency removed, got %d", oldIn)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("adjacency rewrite verification failed: %v", err)
	}
}

func TestCanceledContextReturnsTimeoutKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "v-canceled"})
	})
	if !graph.IsKind(err, graph.ErrKindTimeout) {
		t.Fatalf("expected timeout error kind, got %v", err)
	}
}

func TestConcurrentUpdateWritesDeterministicRecords(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const writers = 24
	var wg sync.WaitGroup
	errCh := make(chan error, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("v-%02d", i)
			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: id})
			})
			errCh <- err
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent update failed: %v", err)
		}
	}

	err := store.View(ctx, func(tx graph.Tx) error {
		for i := 0; i < writers; i++ {
			id := fmt.Sprintf("v-%02d", i)
			if _, err := tx.GetVertex(ctx, "acme", id); err != nil {
				return fmt.Errorf("vertex %s missing: %w", id, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("concurrent write verification failed: %v", err)
	}
}

func TestConcurrentEdgeMutationStressSameIDPool(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const (
		workers        = 12
		opsPerWorker   = 120
		edgeIDPool     = 16
		nodeIDPool     = 10
		relTypeVariety = 4
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				edgeID := fmt.Sprintf("e-%02d", (worker+i)%edgeIDPool)
				srcID := fmt.Sprintf("u-%02d", (worker+(i*3))%nodeIDPool)
				dstID := fmt.Sprintf("g-%02d", (worker+(i*5))%nodeIDPool)
				typeName := fmt.Sprintf("REL_%d", (worker+i)%relTypeVariety)

				if (worker+i)%3 == 0 {
					err := store.Update(ctx, func(tx graph.Tx) error {
						return tx.DeleteEdge(ctx, "acme", edgeID)
					})
					if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
						errCh <- err
						return
					}
					continue
				}

				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.PutEdge(ctx, &graph.Edge{
						Tenant: "acme",
						ID:     edgeID,
						Type:   typeName,
						SrcID:  srcID,
						DstID:  dstID,
					})
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent edge mutation failed: %v", err)
		}
	}

	assertAdjacencyConsistency(t, store, "acme")
}

func TestConcurrentEdgeMutationWithReadersStress(t *testing.T) {
	ctx := context.Background()
	store := openTempStore(t)
	defer func() { _ = store.Close() }()

	const (
		writerWorkers  = 8
		readerWorkers  = 6
		opsPerWriter   = 100
		edgeIDPool     = 20
		nodeIDPool     = 12
		relTypeVariety = 3
	)

	var writers sync.WaitGroup
	writeErrCh := make(chan error, writerWorkers)

	for w := 0; w < writerWorkers; w++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for i := 0; i < opsPerWriter; i++ {
				edgeID := fmt.Sprintf("e-rw-%02d", (worker+i)%edgeIDPool)
				srcID := fmt.Sprintf("ru-%02d", (worker+i)%nodeIDPool)
				dstID := fmt.Sprintf("rg-%02d", (worker+(i*7))%nodeIDPool)
				typeName := fmt.Sprintf("RW_%d", (worker+i)%relTypeVariety)

				if (worker+i)%4 == 0 {
					err := store.Update(ctx, func(tx graph.Tx) error {
						return tx.DeleteEdge(ctx, "acme", edgeID)
					})
					if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
						writeErrCh <- err
						return
					}
					continue
				}

				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.PutEdge(ctx, &graph.Edge{
						Tenant: "acme",
						ID:     edgeID,
						Type:   typeName,
						SrcID:  srcID,
						DstID:  dstID,
					})
				})
				if err != nil {
					writeErrCh <- err
					return
				}
			}
		}(w)
	}

	var readers sync.WaitGroup
	readErrCh := make(chan error, readerWorkers)
	for r := 0; r < readerWorkers; r++ {
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			for i := 0; i < 120; i++ {
				node := fmt.Sprintf("ru-%02d", (reader+i)%nodeIDPool)
				err := store.View(ctx, func(tx graph.Tx) error {
					return tx.ScanOutEdges(ctx, "acme", node, "", 25, func(edge *graph.Edge) error {
						if edge == nil {
							return errors.New("nil edge observed during scan")
						}
						if edge.ID == "" || edge.SrcID == "" || edge.DstID == "" || edge.Type == "" {
							return errors.New("incomplete edge observed during scan")
						}
						return nil
					})
				})
				if err != nil {
					readErrCh <- err
					return
				}
			}
		}(r)
	}

	writers.Wait()
	close(writeErrCh)
	for err := range writeErrCh {
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
	}

	readers.Wait()
	close(readErrCh)
	for err := range readErrCh {
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
	}

	assertAdjacencyConsistency(t, store, "acme")
}

func TestInjectedMetricsObserveTxAndOperations(t *testing.T) {
	ctx := context.Background()
	metrics := newRecordingMetrics()
	store := openTempStoreWithMetrics(t, metrics)
	defer func() { _ = store.Close() }()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "acme", ID: "u-m1"}); err != nil {
			return err
		}
		return tx.PutEdge(ctx, &graph.Edge{
			Tenant: "acme",
			ID:     "e-m1",
			Type:   "LINKS",
			SrcID:  "u-m1",
			DstID:  "u-m2",
		})
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	_ = store.View(ctx, func(tx graph.Tx) error {
		_, _ = tx.GetVertex(ctx, "acme", "missing")
		return tx.ScanOutEdges(ctx, "acme", "u-m1", "", 10, func(edge *graph.Edge) error {
			return nil
		})
	})

	if got := metrics.txCount(graph.TxReadWrite, "ok"); got == 0 {
		t.Fatalf("expected at least one successful read-write tx observation")
	}
	if got := metrics.txCount(graph.TxReadOnly, "ok"); got == 0 {
		t.Fatalf("expected at least one successful read-only tx observation")
	}
	if got := metrics.opCount("put_vertex", "ok"); got == 0 {
		t.Fatalf("expected put_vertex operation observation")
	}
	if got := metrics.opCount("put_edge", "ok"); got == 0 {
		t.Fatalf("expected put_edge operation observation")
	}
	if got := metrics.opCount("get_vertex", "not_found"); got == 0 {
		t.Fatalf("expected get_vertex not_found observation")
	}
	if got := metrics.opCount("scan_out_edges", "ok"); got == 0 {
		t.Fatalf("expected scan_out_edges observation")
	}
}

func BenchmarkEdgeMutationLowContentionParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			edgeID := fmt.Sprintf("e-low-%d", n)
			srcID := fmt.Sprintf("u-low-%d", n%1024)
			dstID := fmt.Sprintf("g-low-%d", (n*7)%1024)
			typeName := fmt.Sprintf("REL_%d", n%8)

			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdge(ctx, &graph.Edge{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark write failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func BenchmarkEdgeMutationHighContentionParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	const (
		edgeIDPool = 16
		nodePool   = 64
	)

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			edgeID := fmt.Sprintf("e-hot-%02d", n%edgeIDPool)
			srcID := fmt.Sprintf("u-hot-%02d", n%nodePool)
			dstID := fmt.Sprintf("g-hot-%02d", (n*5)%nodePool)
			typeName := fmt.Sprintf("HOT_%d", n%4)

			if n%4 == 0 {
				err := store.Update(ctx, func(tx graph.Tx) error {
					return tx.DeleteEdge(ctx, "acme", edgeID)
				})
				if err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				continue
			}

			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdge(ctx, &graph.Edge{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark hot mutation failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func BenchmarkEdgeMutationMixedReadWriteParallel(b *testing.B) {
	ctx := context.Background()
	store := openTempStoreB(b)
	defer func() { _ = store.Close() }()

	const (
		seedEdges  = 256
		edgeIDPool = 64
		nodePool   = 64
	)

	for i := 0; i < seedEdges; i++ {
		edgeID := fmt.Sprintf("e-seed-%03d", i)
		srcID := fmt.Sprintf("u-seed-%02d", i%nodePool)
		dstID := fmt.Sprintf("g-seed-%02d", (i*3)%nodePool)
		err := store.Update(ctx, func(tx graph.Tx) error {
			return tx.PutEdge(ctx, &graph.Edge{
				Tenant: "acme",
				ID:     edgeID,
				Type:   "SEEDED",
				SrcID:  srcID,
				DstID:  dstID,
			})
		})
		if err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}

	b.ReportAllocs()

	var seq atomic.Uint64
	var firstErr error
	var errMu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) - 1
			if n%5 == 0 {
				nodeID := fmt.Sprintf("u-seed-%02d", n%nodePool)
				err := store.View(ctx, func(tx graph.Tx) error {
					return tx.ScanOutEdges(ctx, "acme", nodeID, "", 20, func(edge *graph.Edge) error {
						if edge == nil {
							return errors.New("nil edge observed")
						}
						return nil
					})
				})
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				continue
			}

			edgeID := fmt.Sprintf("e-mix-%02d", n%edgeIDPool)
			srcID := fmt.Sprintf("u-seed-%02d", n%nodePool)
			dstID := fmt.Sprintf("g-seed-%02d", (n*11)%nodePool)
			typeName := fmt.Sprintf("MIX_%d", n%6)
			err := store.Update(ctx, func(tx graph.Tx) error {
				return tx.PutEdge(ctx, &graph.Edge{
					Tenant: "acme",
					ID:     edgeID,
					Type:   typeName,
					SrcID:  srcID,
					DstID:  dstID,
				})
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("benchmark mixed workload failed: %v", firstErr)
	}
	assertAdjacencyConsistencyB(b, store, "acme")
}

func openTempStore(t *testing.T) *Store {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}

func openTempStoreWithMetrics(t *testing.T, metrics Metrics) *Store {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	store, err := OpenWithOptions(dbPath, StoreOptions{Metrics: metrics})
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}

func openTempStoreB(b *testing.B) *Store {
	b.Helper()
	base := b.TempDir()
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		b.Fatalf("mkdir failed: %v", err)
	}
	store, err := Open(dbPath)
	if err != nil {
		b.Fatalf("open store failed: %v", err)
	}
	return store
}

func countByPrefix(t *testing.T, store *Store, prefix []byte) int {
	t.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		t.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	return count
}

func assertAdjacencyConsistency(t *testing.T, store *Store, tenant string) {
	t.Helper()

	edges := readAllEdgesByID(t, store, tenant)

	for edgeID, edge := range edges {
		outKey := keyspace.OutAdjacencyKey(tenant, edge.SrcID, edge.Type, edgeID)
		inKey := keyspace.InAdjacencyKey(tenant, edge.DstID, edge.Type, edgeID)
		if !dbHasKey(t, store, outKey) {
			t.Fatalf("missing out adjacency for edge %s", edgeID)
		}
		if !dbHasKey(t, store, inKey) {
			t.Fatalf("missing in adjacency for edge %s", edgeID)
		}
	}

	outAdjCount := 0
	iteratePrefix(t, store, []byte("a/out/"+tenant+"/"), func(key, _ []byte) {
		outAdjCount++
		kTenant, srcID, edgeType, edgeID, ok := parseOutAdjacencyKey(key)
		if !ok {
			t.Fatalf("malformed out adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			t.Fatalf("orphan out adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.SrcID != srcID || edge.Type != edgeType {
			t.Fatalf("stale out adjacency key for edge %s", edgeID)
		}
	})

	inAdjCount := 0
	iteratePrefix(t, store, []byte("a/in/"+tenant+"/"), func(key, _ []byte) {
		inAdjCount++
		kTenant, dstID, edgeType, edgeID, ok := parseInAdjacencyKey(key)
		if !ok {
			t.Fatalf("malformed in adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			t.Fatalf("orphan in adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.DstID != dstID || edge.Type != edgeType {
			t.Fatalf("stale in adjacency key for edge %s", edgeID)
		}
	})

	if outAdjCount != len(edges) {
		t.Fatalf("out adjacency count mismatch: got=%d expected=%d", outAdjCount, len(edges))
	}
	if inAdjCount != len(edges) {
		t.Fatalf("in adjacency count mismatch: got=%d expected=%d", inAdjCount, len(edges))
	}
}

func assertAdjacencyConsistencyB(b *testing.B, store *Store, tenant string) {
	b.Helper()

	edges := readAllEdgesByIDB(b, store, tenant)

	for edgeID, edge := range edges {
		outKey := keyspace.OutAdjacencyKey(tenant, edge.SrcID, edge.Type, edgeID)
		inKey := keyspace.InAdjacencyKey(tenant, edge.DstID, edge.Type, edgeID)
		if !dbHasKeyB(b, store, outKey) {
			b.Fatalf("missing out adjacency for edge %s", edgeID)
		}
		if !dbHasKeyB(b, store, inKey) {
			b.Fatalf("missing in adjacency for edge %s", edgeID)
		}
	}

	outAdjCount := 0
	iteratePrefixB(b, store, []byte("a/out/"+tenant+"/"), func(key, _ []byte) {
		outAdjCount++
		kTenant, srcID, edgeType, edgeID, ok := parseOutAdjacencyKey(key)
		if !ok {
			b.Fatalf("malformed out adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			b.Fatalf("orphan out adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.SrcID != srcID || edge.Type != edgeType {
			b.Fatalf("stale out adjacency key for edge %s", edgeID)
		}
	})

	inAdjCount := 0
	iteratePrefixB(b, store, []byte("a/in/"+tenant+"/"), func(key, _ []byte) {
		inAdjCount++
		kTenant, dstID, edgeType, edgeID, ok := parseInAdjacencyKey(key)
		if !ok {
			b.Fatalf("malformed in adjacency key %q", key)
		}
		edge, ok := edges[edgeID]
		if !ok {
			b.Fatalf("orphan in adjacency key for missing edge %s", edgeID)
		}
		if kTenant != tenant || edge.Tenant != tenant || edge.DstID != dstID || edge.Type != edgeType {
			b.Fatalf("stale in adjacency key for edge %s", edgeID)
		}
	})

	if outAdjCount != len(edges) {
		b.Fatalf("out adjacency count mismatch: got=%d expected=%d", outAdjCount, len(edges))
	}
	if inAdjCount != len(edges) {
		b.Fatalf("in adjacency count mismatch: got=%d expected=%d", inAdjCount, len(edges))
	}
}

func readAllEdgesByID(t *testing.T, store *Store, tenant string) map[string]*graph.Edge {
	t.Helper()

	out := make(map[string]*graph.Edge)
	iteratePrefix(t, store, keyspace.EdgePrefix(tenant), func(key, value []byte) {
		edgeID := edgeIDFromAdjKey(key)
		if edgeID == "" {
			t.Fatalf("malformed edge key %q", key)
		}
		var edge graph.Edge
		if err := json.Unmarshal(value, &edge); err != nil {
			t.Fatalf("decode edge failed for key %q: %v", key, err)
		}
		out[edgeID] = &edge
	})
	return out
}

func readAllEdgesByIDB(b *testing.B, store *Store, tenant string) map[string]*graph.Edge {
	b.Helper()

	out := make(map[string]*graph.Edge)
	iteratePrefixB(b, store, keyspace.EdgePrefix(tenant), func(key, value []byte) {
		edgeID := edgeIDFromAdjKey(key)
		if edgeID == "" {
			b.Fatalf("malformed edge key %q", key)
		}
		var edge graph.Edge
		if err := json.Unmarshal(value, &edge); err != nil {
			b.Fatalf("decode edge failed for key %q: %v", key, err)
		}
		out[edgeID] = &edge
	})
	return out
}

func dbHasKey(t *testing.T, store *Store, key []byte) bool {
	t.Helper()
	_, closer, err := store.db.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("db get failed for key %q: %v", key, err)
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true
}

func dbHasKeyB(b *testing.B, store *Store, key []byte) bool {
	b.Helper()
	_, closer, err := store.db.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return false
	}
	if err != nil {
		b.Fatalf("db get failed for key %q: %v", key, err)
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true
}

func iteratePrefix(t *testing.T, store *Store, prefix []byte, fn func(key, value []byte)) {
	t.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		t.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		fn(k, v)
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
}

func iteratePrefixB(b *testing.B, store *Store, prefix []byte, fn func(key, value []byte)) {
	b.Helper()
	iter, err := store.db.NewIter(&cpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		b.Fatalf("new iter failed: %v", err)
	}
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		fn(k, v)
	}
	if err := iter.Error(); err != nil {
		b.Fatalf("iter error: %v", err)
	}
}

func parseOutAdjacencyKey(key []byte) (tenant, srcID, edgeType, edgeID string, ok bool) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 6 || parts[0] != "a" || parts[1] != "out" {
		return "", "", "", "", false
	}
	return parts[2], parts[3], parts[4], parts[5], true
}

func parseInAdjacencyKey(key []byte) (tenant, dstID, edgeType, edgeID string, ok bool) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 6 || parts[0] != "a" || parts[1] != "in" {
		return "", "", "", "", false
	}
	return parts[2], parts[3], parts[4], parts[5], true
}

type recordingMetrics struct {
	mu           sync.Mutex
	txCounts     map[string]int
	opCounts     map[string]int
	conflictIncs int
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{
		txCounts: make(map[string]int),
		opCounts: make(map[string]int),
	}
}

func (m *recordingMetrics) ObserveTx(mode graph.TxMode, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txCounts[fmt.Sprintf("%d|%s", mode, outcome)]++
}

func (m *recordingMetrics) ObserveOperation(name, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opCounts[name+"|"+outcome]++
}

func (m *recordingMetrics) IncTxConflict() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conflictIncs++
}

func (m *recordingMetrics) txCount(mode graph.TxMode, outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.txCounts[fmt.Sprintf("%d|%s", mode, outcome)]
}

func (m *recordingMetrics) opCount(name, outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.opCounts[name+"|"+outcome]
}
