package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/paegun/vitaledge/internal/bench"
)

func main() {
	var scenario string
	var dataset string
	var iterations int

	flag.StringVar(&scenario, "scenario", "smoke", "Benchmark scenario: smoke|research|threat|rebac")
	flag.StringVar(&dataset, "dataset", "", "Path to dataset directory/file")
	flag.IntVar(&iterations, "iterations", 1000, "Iteration count")
	flag.Parse()

	runner, err := bench.ScenarioByName(scenario)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	result, err := runner.Run(context.Background(), bench.Config{
		DatasetPath: dataset,
		Iterations:  iterations,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("scenario=%s operations=%d duration=%s\n", scenario, result.Operations, result.Duration)
	for k, v := range result.Metrics {
		fmt.Printf("metric %s=%.3f\n", k, v)
	}
}
