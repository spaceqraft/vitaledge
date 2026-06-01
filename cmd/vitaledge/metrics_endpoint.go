package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

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

	writeGoRuntimeMetrics(w)
	writeHostMetrics(w)

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

	bucketBounds := executor.StatementDurationHistogramBuckets()
	writeHelpType(w, "vitaledge_executor_statement_duration_seconds", "Statement execution latency histogram in seconds by kind and outcome.", "histogram")
	for _, key := range stmtKeys {
		value := snapshot.Statements[key]
		cumulative := int64(0)
		for idx, upperBound := range bucketBounds {
			if idx < len(value.DurationBuckets) {
				cumulative += value.DurationBuckets[idx]
			}
			fmt.Fprintf(
				w,
				"vitaledge_executor_statement_duration_seconds_bucket{kind=%q,outcome=%q,le=%q} %d\n",
				string(key.Kind),
				key.Outcome,
				formatPrometheusDurationBucket(upperBound),
				cumulative,
			)
		}
		fmt.Fprintf(w, "vitaledge_executor_statement_duration_seconds_bucket{kind=%q,outcome=%q,le=%q} %d\n", string(key.Kind), key.Outcome, "+Inf", value.Count)
		fmt.Fprintf(w, "vitaledge_executor_statement_duration_seconds_sum{kind=%q,outcome=%q} %s\n", string(key.Kind), key.Outcome, strconv.FormatFloat(value.TotalDuration.Seconds(), 'f', 9, 64))
		fmt.Fprintf(w, "vitaledge_executor_statement_duration_seconds_count{kind=%q,outcome=%q} %d\n", string(key.Kind), key.Outcome, value.Count)
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

	writeHelpType(w, "vitaledge_executor_delete_events_total", "Count of delete-related executor events by event type.", "counter")
	deleteKeys := make([]string, 0, len(snapshot.DeleteCounters))
	for key := range snapshot.DeleteCounters {
		deleteKeys = append(deleteKeys, key)
	}
	sort.Strings(deleteKeys)
	for _, key := range deleteKeys {
		fmt.Fprintf(w, "vitaledge_executor_delete_events_total{event=%q} %d\n", key, snapshot.DeleteCounters[key])
	}

	writeHelpType(w, "vitaledge_executor_runtime_counters_total", "Query runtime counters emitted by executor fast paths.", "counter")
	runtimeCounterKeys := make([]string, 0, len(snapshot.RuntimeCounters))
	for key := range snapshot.RuntimeCounters {
		runtimeCounterKeys = append(runtimeCounterKeys, key)
	}
	sort.Strings(runtimeCounterKeys)
	for _, key := range runtimeCounterKeys {
		fmt.Fprintf(w, "vitaledge_executor_runtime_counters_total{counter=%q} %d\n", key, snapshot.RuntimeCounters[key])
	}
}

func writeGoRuntimeMetrics(w http.ResponseWriter) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	writeHelpType(w, "vitaledge_go_goroutines", "Number of live goroutines.", "gauge")
	fmt.Fprintf(w, "vitaledge_go_goroutines %d\n", runtime.NumGoroutine())

	writeHelpType(w, "vitaledge_go_memory_heap_alloc_bytes", "Bytes of allocated heap objects.", "gauge")
	fmt.Fprintf(w, "vitaledge_go_memory_heap_alloc_bytes %d\n", ms.HeapAlloc)

	writeHelpType(w, "vitaledge_go_memory_heap_inuse_bytes", "Bytes in in-use heap spans.", "gauge")
	fmt.Fprintf(w, "vitaledge_go_memory_heap_inuse_bytes %d\n", ms.HeapInuse)

	writeHelpType(w, "vitaledge_go_memory_sys_bytes", "Bytes obtained from the OS by Go runtime.", "gauge")
	fmt.Fprintf(w, "vitaledge_go_memory_sys_bytes %d\n", ms.Sys)

	writeHelpType(w, "vitaledge_go_gc_cycles_total", "Total completed GC cycles.", "counter")
	fmt.Fprintf(w, "vitaledge_go_gc_cycles_total %d\n", ms.NumGC)

	writeHelpType(w, "vitaledge_go_gc_pause_seconds_total", "Total GC pause time in seconds.", "counter")
	fmt.Fprintf(w, "vitaledge_go_gc_pause_seconds_total %s\n", strconv.FormatFloat(float64(ms.PauseTotalNs)/1e9, 'f', 9, 64))

	writeHelpType(w, "vitaledge_go_next_gc_heap_target_bytes", "Heap size target for the next GC cycle.", "gauge")
	fmt.Fprintf(w, "vitaledge_go_next_gc_heap_target_bytes %d\n", ms.NextGC)
}

func writeHostMetrics(w http.ResponseWriter) {
	writeHostCPUMetrics(w)
	writeHostMemoryMetrics(w)
	writeHostNetworkMetrics(w)
}

func writeHostCPUMetrics(w http.ResponseWriter) {
	buf, err := os.ReadFile("/proc/stat")
	if err != nil {
		return
	}
	lines := strings.Split(string(buf), "\n")
	if len(lines) == 0 {
		return
	}
	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return
	}

	modes := []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice"}

	writeHelpType(w, "vitaledge_host_cpu_seconds_total", "Host CPU time spent in each mode.", "counter")
	for i := 1; i < len(fields) && i <= len(modes); i++ {
		ticks, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		seconds := ticks / 100.0
		fmt.Fprintf(w, "vitaledge_host_cpu_seconds_total{mode=%q} %s\n", modes[i-1], strconv.FormatFloat(seconds, 'f', 6, 64))
	}
}

func writeHostMemoryMetrics(w http.ResponseWriter) {
	buf, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	memKB := map[string]uint64{}
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		memKB[key] = val
	}

	totalKB, hasTotal := memKB["MemTotal"]
	availKB, hasAvail := memKB["MemAvailable"]
	if !hasTotal {
		return
	}

	writeHelpType(w, "vitaledge_host_memory_total_bytes", "Total host memory in bytes.", "gauge")
	fmt.Fprintf(w, "vitaledge_host_memory_total_bytes %d\n", totalKB*1024)

	if hasAvail {
		writeHelpType(w, "vitaledge_host_memory_available_bytes", "Available host memory in bytes.", "gauge")
		fmt.Fprintf(w, "vitaledge_host_memory_available_bytes %d\n", availKB*1024)
	}
}

func writeHostNetworkMetrics(w http.ResponseWriter) {
	buf, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return
	}

	var rxTotal uint64
	var txTotal uint64
	lines := strings.Split(string(buf), "\n")
	for i, line := range lines {
		if i < 2 {
			continue // skip headers
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rx, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		tx, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			continue
		}
		rxTotal += rx
		txTotal += tx
	}

	writeHelpType(w, "vitaledge_host_network_receive_bytes_total", "Total bytes received across host network interfaces.", "counter")
	fmt.Fprintf(w, "vitaledge_host_network_receive_bytes_total %d\n", rxTotal)

	writeHelpType(w, "vitaledge_host_network_transmit_bytes_total", "Total bytes transmitted across host network interfaces.", "counter")
	fmt.Fprintf(w, "vitaledge_host_network_transmit_bytes_total %d\n", txTotal)
}

func writeHelpType(w http.ResponseWriter, name, help, metricType string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
}

func formatPrometheusDurationBucket(duration time.Duration) string {
	seconds := duration.Seconds()
	return strconv.FormatFloat(seconds, 'f', -1, 64)
}
