package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/spaceqraft/vitaledge/internal/bench"
)

func main() {
	var scenario string
	var dataset string
	var iterations int
	var seedSize int
	var jsonOutput bool

	flag.StringVar(&scenario, "scenario", "smoke", "Benchmark scenario: smoke|research|threat|rebac")
	flag.StringVar(&dataset, "dataset", "", "Path to dataset directory/file")
	flag.IntVar(&iterations, "iterations", 1000, "Iteration count")
	flag.IntVar(&seedSize, "seed-size", 0, "Seed graph size override for seeded scenarios; 0 derives from iterations")
	flag.BoolVar(&jsonOutput, "json", false, "Emit JSON output for machine-readable benchmark baselines")
	flag.Parse()

	runner, err := bench.ScenarioByName(scenario)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	result, err := runner.Run(context.Background(), bench.Config{
		DatasetPath: dataset,
		Iterations:  iterations,
		SeedSize:    seedSize,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "encode failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("scenario=%s operations=%d duration=%s\n", scenario, result.Operations, result.Duration)
	keys := make([]string, 0, len(result.Metrics))
	for k := range result.Metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := result.Metrics[k]
		fmt.Printf("metric %s=%.3f\n", k, v)
	}
}
