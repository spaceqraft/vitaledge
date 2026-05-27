package main

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/executor"
)

func startMetricsServer(listenAddress string, collector *executor.Collector) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writePrometheusMetrics(w, collector)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	ln, err := net.Listen("tcp", strings.TrimSpace(listenAddress))
	if err != nil {
		return nil, err
	}

	srv := &http.Server{Handler: mux}
	go func() {
		_ = srv.Serve(ln)
	}()
	return srv, nil
}

func writePrometheusMetrics(w http.ResponseWriter, collector *executor.Collector) {
	snapshot := executor.Snapshot{}
	if collector != nil {
		snapshot = collector.Snapshot()
	}

	writeHelpType(w, "vitaledge_executor_statements_total", "Count of executed statements by kind and outcome.", "counter")
	stmtKeys := make([]executor.StatementMetricKey, 0, len(snapshot.Statements))
	for key := range snapshot.Statements {
		stmtKeys = append(stmtKeys, key)
	}
	sort.Slice(stmtKeys, func(i, j int) bool {
		if stmtKeys[i].Kind != stmtKeys[j].Kind {
			return stmtKeys[i].Kind < stmtKeys[j].Kind
		}
		return stmtKeys[i].Outcome < stmtKeys[j].Outcome
	})
	for _, key := range stmtKeys {
		value := snapshot.Statements[key]
		fmt.Fprintf(w, "vitaledge_executor_statements_total{kind=%q,outcome=%q} %d\n", string(key.Kind), key.Outcome, value.Count)
	}

	writeHelpType(w, "vitaledge_executor_statement_duration_seconds_total", "Total statement execution duration in seconds by kind and outcome.", "counter")
	for _, key := range stmtKeys {
		value := snapshot.Statements[key]
		fmt.Fprintf(w, "vitaledge_executor_statement_duration_seconds_total{kind=%q,outcome=%q} %s\n", string(key.Kind), key.Outcome, strconv.FormatFloat(value.TotalDuration.Seconds(), 'f', 9, 64))
	}

	writeHelpType(w, "vitaledge_executor_rows_returned_total", "Total rows returned by executed statements.", "counter")
	fmt.Fprintf(w, "vitaledge_executor_rows_returned_total %d\n", snapshot.RowsReturned)

	writeHelpType(w, "vitaledge_executor_index_candidates_total", "Index candidate observations by tenant, schema, property, and indexed state.", "counter")
	candidateKeys := make([]executor.IndexCandidateKey, 0, len(snapshot.IndexCandidates))
	for key := range snapshot.IndexCandidates {
		candidateKeys = append(candidateKeys, key)
	}
	sort.Slice(candidateKeys, func(i, j int) bool {
		if candidateKeys[i].Tenant != candidateKeys[j].Tenant {
			return candidateKeys[i].Tenant < candidateKeys[j].Tenant
		}
		if candidateKeys[i].Schema != candidateKeys[j].Schema {
			return candidateKeys[i].Schema < candidateKeys[j].Schema
		}
		if candidateKeys[i].Property != candidateKeys[j].Property {
			return candidateKeys[i].Property < candidateKeys[j].Property
		}
		return fmt.Sprint(candidateKeys[i].Indexed) < fmt.Sprint(candidateKeys[j].Indexed)
	})
	for _, key := range candidateKeys {
		count := snapshot.IndexCandidates[key]
		fmt.Fprintf(w, "vitaledge_executor_index_candidates_total{tenant=%q,schema=%q,property=%q,indexed=%q} %d\n", key.Tenant, key.Schema, key.Property, strconv.FormatBool(key.Indexed), count)
	}

	writeHelpType(w, "vitaledge_executor_unindexed_candidate_observations", "Current unindexed candidate observation counts by tenant, schema, and property.", "gauge")
	if collector != nil {
		for _, candidate := range collector.TopUnindexedCandidates(1000) {
			fmt.Fprintf(w, "vitaledge_executor_unindexed_candidate_observations{tenant=%q,schema=%q,property=%q} %d\n", candidate.Tenant, candidate.Schema, candidate.Property, candidate.Count)
		}
	}

	writeHelpType(w, "vitaledge_executor_index_lookups_total", "Count of index lookup attempts by strategy and outcome.", "counter")
	writeHelpType(w, "vitaledge_executor_index_lookup_matches_total", "Total matches returned by index lookup strategy and outcome.", "counter")
	lookupKeys := make([]executor.IndexLookupKey, 0, len(snapshot.IndexLookups))
	for key := range snapshot.IndexLookups {
		lookupKeys = append(lookupKeys, key)
	}
	sort.Slice(lookupKeys, func(i, j int) bool {
		if lookupKeys[i].Strategy != lookupKeys[j].Strategy {
			return lookupKeys[i].Strategy < lookupKeys[j].Strategy
		}
		return lookupKeys[i].Outcome < lookupKeys[j].Outcome
	})
	for _, key := range lookupKeys {
		value := snapshot.IndexLookups[key]
		fmt.Fprintf(w, "vitaledge_executor_index_lookups_total{strategy=%q,outcome=%q} %d\n", key.Strategy, key.Outcome, value.Count)
		fmt.Fprintf(w, "vitaledge_executor_index_lookup_matches_total{strategy=%q,outcome=%q} %d\n", key.Strategy, key.Outcome, value.TotalMatches)
	}
}

func writeHelpType(w http.ResponseWriter, name, help, metricType string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
}
