package bench

import (
	"context"
	"fmt"
	"time"
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
	"research": scenarioImpl{name: "research", run: runSkeleton},
	"threat":   scenarioImpl{name: "threat", run: runSkeleton},
	"rebac":    scenarioImpl{name: "rebac", run: runSkeleton},
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
