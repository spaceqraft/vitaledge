package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/spaceqraft/vitaledge/internal/cypher/executor"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
	pebblestore "github.com/spaceqraft/vitaledge/internal/graph/store/pebble"
)

// Config is benchmark run configuration.
type Config struct {
	DatasetPath string
	Iterations  int
	SeedSize    int
}

// Result captures benchmark execution output.
type Result struct {
	Scenario   string
	Operations int
	Duration   time.Duration
	Metrics    map[string]float64
}

// Scenario is a runnable benchmark scenario.
type Scenario interface {
	Name() string
	Run(ctx context.Context, cfg Config) (Result, error)
}

type scenarioImpl struct {
	name string
	run  func(context.Context, Config) (Result, error)
}

func (s scenarioImpl) Name() string { return s.name }

func (s scenarioImpl) Run(ctx context.Context, cfg Config) (Result, error) {
	return s.run(ctx, cfg)
}

var registry = map[string]Scenario{
	"smoke":    scenarioImpl{name: "smoke", run: runSmoke},
	"research": scenarioImpl{name: "research", run: runResearch},
	"threat":   scenarioImpl{name: "threat", run: runThreat},
	"rebac":    scenarioImpl{name: "rebac", run: runReBAC},
}

func ScenarioByName(name string) (Scenario, error) {
	s, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q", name)
	}
	return s, nil
}

func runSmoke(ctx context.Context, cfg Config) (Result, error) {
	iters := cfg.Iterations
	if iters <= 0 {
		iters = 1
	}

	start := time.Now()
	ops := 0
	for i := 0; i < iters; i++ {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		default:
			ops++
		}
	}
	dur := time.Since(start)

	rps := 0.0
	if dur > 0 {
		rps = float64(ops) / dur.Seconds()
	}

	return Result{
		Scenario:   "smoke",
		Operations: ops,
		Duration:   dur,
		Metrics: map[string]float64{
			"ops_per_sec": rps,
		},
	}, nil
}

func runThreat(ctx context.Context, cfg Config) (Result, error) {
	iters := cfg.Iterations
	if iters <= 0 {
		iters = 10_000
	}

	store, cleanup, err := openTempStore()
	if err != nil {
		return Result{}, err
	}
	defer cleanup()

	if err := store.Update(ctx, func(tx graph.Tx) error {
		return tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "bench", ID: "source", Labels: []string{"Service"}}})
	}); err != nil {
		return Result{}, err
	}

	started := time.Now()
	err = store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < iters; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			dstID := "event-" + strconv.Itoa(i)
			edgeID := "ingest-" + strconv.Itoa(i)
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "bench", ID: dstID, Labels: []string{"Event"}}}); err != nil {
				return err
			}
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "bench", ID: edgeID, Type: "EMITS", SrcID: "source", DstID: dstID}}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	dur := time.Since(started)

	opsPerSec := 0.0
	edgesPerMin := 0.0
	if dur > 0 {
		ops := float64(iters) / dur.Seconds()
		opsPerSec = ops
		edgesPerMin = ops * 60
	}

	return Result{
		Scenario:   "threat",
		Operations: iters,
		Duration:   dur,
		Metrics: map[string]float64{
			"ops_per_sec":        opsPerSec,
			"edges_per_min":      edgesPerMin,
			"seed_vertices":      float64(iters + 1),
			"seed_edges":         float64(iters),
			"seed_relationships": float64(iters),
		},
	}, nil
}

func runReBAC(ctx context.Context, cfg Config) (Result, error) {
	iters := cfg.Iterations
	if iters <= 0 {
		iters = 1_000
	}

	store, cleanup, err := openTempStore()
	if err != nil {
		return Result{}, err
	}
	defer cleanup()

	fanout := cfg.SeedSize
	if fanout <= 0 {
		fanout = scaleFromIterations(iters, 10, 100, 20_000)
	}

	if err := seedReBACGraph(ctx, store, fanout); err != nil {
		return Result{}, err
	}

	stmt, err := parser.ParseStatement("MATCH (u:User {id: $uid})-[:MEMBER_OF]->(g:Group) MATCH (g)-[:CAN_ACCESS]->(r:Resource) RETURN count(r) AS accessible")
	if err != nil {
		return Result{}, err
	}

	exec := executor.New(store, executor.Options{})
	latencies := make([]float64, 0, iters)
	started := time.Now()
	for i := 0; i < iters; i++ {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		default:
		}

		runStart := time.Now()
		if _, err := exec.ExecuteStatement(ctx, stmt, executor.Params{"tenant": "bench", "uid": "user-0"}); err != nil {
			return Result{}, err
		}
		latencies = append(latencies, float64(time.Since(runStart).Microseconds())/1000.0)
	}
	dur := time.Since(started)

	opsPerSec := 0.0
	if dur > 0 {
		opsPerSec = float64(iters) / dur.Seconds()
	}

	return Result{
		Scenario:   "rebac",
		Operations: iters,
		Duration:   dur,
		Metrics: map[string]float64{
			"ops_per_sec":        opsPerSec,
			"p95_ms":             percentile(latencies, 95),
			"avg_ms":             average(latencies),
			"seed_fanout":        float64(fanout),
			"seed_vertices":      float64(1 + (fanout * 2)),
			"seed_edges":         float64(fanout * 2),
			"seed_relationships": float64(fanout * 2),
		},
	}, nil
}

func runResearch(ctx context.Context, cfg Config) (Result, error) {
	iters := cfg.Iterations
	if iters <= 0 {
		iters = 1_000
	}

	store, cleanup, err := openTempStore()
	if err != nil {
		return Result{}, err
	}
	defer cleanup()

	papers := cfg.SeedSize
	if papers <= 0 {
		papers = scaleFromIterations(iters, 50, 500, 50_000)
	}

	if err := seedDenseResearchGraph(ctx, store, papers); err != nil {
		return Result{}, err
	}

	stmt, err := parser.ParseStatement("MATCH (:Paper {id: $paperID})<-[:CITES]-(peer:Paper) RETURN count(peer) AS citingPapers")
	if err != nil {
		return Result{}, err
	}

	exec := executor.New(store, executor.Options{})
	latencies := make([]float64, 0, iters)
	started := time.Now()
	for i := 0; i < iters; i++ {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		default:
		}

		runStart := time.Now()
		if _, err := exec.ExecuteStatement(ctx, stmt, executor.Params{"tenant": "bench", "paperID": "paper-0"}); err != nil {
			return Result{}, err
		}
		latencies = append(latencies, float64(time.Since(runStart).Microseconds())/1000.0)
	}
	dur := time.Since(started)

	opsPerSec := 0.0
	if dur > 0 {
		opsPerSec = float64(iters) / dur.Seconds()
	}

	return Result{
		Scenario:   "research",
		Operations: iters,
		Duration:   dur,
		Metrics: map[string]float64{
			"ops_per_sec":        opsPerSec,
			"p95_ms":             percentile(latencies, 95),
			"avg_ms":             average(latencies),
			"seed_papers":        float64(papers),
			"seed_vertices":      float64(papers),
			"seed_edges":         float64(papers - 1),
			"seed_relationships": float64(papers - 1),
		},
	}, nil
}

func scaleFromIterations(iterations, min, divisor, max int) int {
	if iterations <= 0 {
		iterations = min * divisor
	}
	if min <= 0 {
		min = 1
	}
	if divisor <= 0 {
		divisor = 1
	}
	if max < min {
		max = min
	}

	scaled := iterations / divisor
	if scaled < min {
		return min
	}
	if scaled > max {
		return max
	}
	return scaled
}

func openTempStore() (graph.GraphStore, func(), error) {
	base, err := os.MkdirTemp("", "vitaledge-bench")
	if err != nil {
		return nil, nil, err
	}
	dbPath := filepath.Join(base, "graph.db")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		_ = os.RemoveAll(base)
		return nil, nil, err
	}
	store, err := pebblestore.Open(dbPath)
	if err != nil {
		_ = os.RemoveAll(base)
		return nil, nil, err
	}

	cleanup := func() {
		_ = store.Close()
		_ = os.RemoveAll(base)
	}

	return store, cleanup, nil
}

func seedReBACGraph(ctx context.Context, store graph.GraphStore, fanout int) error {
	if fanout <= 0 {
		fanout = 10
	}
	return store.Update(ctx, func(tx graph.Tx) error {
		if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "bench", ID: "user-0", Labels: []string{"User"}, Properties: graph.PropertyMap{"id": []byte("user-0")}}}); err != nil {
			return err
		}
		for i := 0; i < fanout; i++ {
			groupID := "group-" + strconv.Itoa(i)
			resourceID := "resource-" + strconv.Itoa(i)
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{
				{Tenant: "bench", ID: groupID, Labels: []string{"Group"}},
				{Tenant: "bench", ID: resourceID, Labels: []string{"Resource"}},
			}); err != nil {
				return err
			}
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{
				{Tenant: "bench", ID: "m-" + strconv.Itoa(i), Type: "MEMBER_OF", SrcID: "user-0", DstID: groupID},
				{Tenant: "bench", ID: "a-" + strconv.Itoa(i), Type: "CAN_ACCESS", SrcID: groupID, DstID: resourceID},
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func seedDenseResearchGraph(ctx context.Context, store graph.GraphStore, papers int) error {
	if papers < 2 {
		papers = 2
	}
	return store.Update(ctx, func(tx graph.Tx) error {
		for i := 0; i < papers; i++ {
			paperID := "paper-" + strconv.Itoa(i)
			if err := tx.PutVertexBatch(ctx, []*graph.Vertex{{Tenant: "bench", ID: paperID, Labels: []string{"Paper"}, Properties: graph.PropertyMap{"id": []byte(paperID)}}}); err != nil {
				return err
			}
			if i == 0 {
				continue
			}
			edgeID := "cite-" + strconv.Itoa(i)
			if err := tx.PutEdgeBatch(ctx, []*graph.Edge{{Tenant: "bench", ID: edgeID, Type: "CITES", SrcID: paperID, DstID: "paper-0"}}); err != nil {
				return err
			}
		}
		return nil
	})
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	idx := int((p / 100.0) * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}
