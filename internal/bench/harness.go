package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/executor"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
	pebblestore "github.com/paegun/vitaledge/internal/graph/store/pebble"
)

// Config is benchmark run configuration.
type Config struct {
	DatasetPath string
	Iterations  int
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

func runSkeleton(ctx context.Context, cfg Config) (Result, error) {
	_ = cfg
	start := time.Now()
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}
	return Result{
		Scenario:   "skeleton",
		Operations: 0,
		Duration:   time.Since(start),
		Metrics: map[string]float64{
			"ops_per_sec": 0,
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
		return tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: "source", Labels: []string{"Service"}})
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
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: dstID, Labels: []string{"Event"}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "bench", ID: edgeID, Type: "EMITS", SrcID: "source", DstID: dstID}); err != nil {
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
		opс := float64(iters) / dur.Seconds()
		opsPerSec = opс
		edgesPerMin = opс * 60
	}

	return Result{
		Scenario:   "threat",
		Operations: iters,
		Duration:   dur,
		Metrics: map[string]float64{
			"ops_per_sec":   opsPerSec,
			"edges_per_min": edgesPerMin,
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

	if err := seedReBACGraph(ctx, store, 100); err != nil {
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
			"ops_per_sec": opsPerSec,
			"p95_ms":      percentile(latencies, 95),
			"avg_ms":      average(latencies),
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

	if err := seedDenseResearchGraph(ctx, store, 500); err != nil {
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
			"ops_per_sec": opsPerSec,
			"p95_ms":      percentile(latencies, 95),
			"avg_ms":      average(latencies),
		},
	}, nil
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
		if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: "user-0", Labels: []string{"User"}, Properties: graph.PropertyMap{"id": []byte("user-0")}}); err != nil {
			return err
		}
		for i := 0; i < fanout; i++ {
			groupID := "group-" + strconv.Itoa(i)
			resourceID := "resource-" + strconv.Itoa(i)
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: groupID, Labels: []string{"Group"}}); err != nil {
				return err
			}
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: resourceID, Labels: []string{"Resource"}}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "bench", ID: "m-" + strconv.Itoa(i), Type: "MEMBER_OF", SrcID: "user-0", DstID: groupID}); err != nil {
				return err
			}
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "bench", ID: "a-" + strconv.Itoa(i), Type: "CAN_ACCESS", SrcID: groupID, DstID: resourceID}); err != nil {
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
			if err := tx.PutVertex(ctx, &graph.Vertex{Tenant: "bench", ID: paperID, Labels: []string{"Paper"}, Properties: graph.PropertyMap{"id": []byte(paperID)}}); err != nil {
				return err
			}
			if i == 0 {
				continue
			}
			edgeID := "cite-" + strconv.Itoa(i)
			if err := tx.PutEdge(ctx, &graph.Edge{Tenant: "bench", ID: edgeID, Type: "CITES", SrcID: paperID, DstID: "paper-0"}); err != nil {
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
