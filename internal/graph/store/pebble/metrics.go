package pebblestore

import (
	"math"
	"sort"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

const DefaultMaxWriteBatchBytes = 64 * 1024 * 1024
const statsHistogramBucketCount = 8

// Metrics captures store-level observability signals.
// Implementations own registration and lifecycle outside this package.
type Metrics interface {
	ObserveTx(mode graph.TxMode, outcome string, duration time.Duration)
	ObserveOperation(name, outcome string, duration time.Duration)
	IncTxConflict()
}

type propertyStatsTarget struct {
	tenant      string
	entityClass string
	schema      string
	property    string
}

// StoreOptions configures OpenWithOptions behavior.
type StoreOptions struct {
	PebbleOptions                     *cpebble.Options
	Metrics                           Metrics
	MaxWriteBatchBytes                int
	PebbleBlockCacheBytes             int64
	PebbleMemTableSizeBytes           int
	PebbleMemTableStopWritesThreshold int
}

type noopMetrics struct{}

var defaultMetrics Metrics = noopMetrics{}

func (noopMetrics) ObserveTx(_ graph.TxMode, _ string, _ time.Duration) {}

func (noopMetrics) ObserveOperation(_, _ string, _ time.Duration) {}

func (noopMetrics) IncTxConflict() {}

func (s *Store) observeTx(mode graph.TxMode, err error, started time.Time) {
	if s == nil || s.metrics == nil {
		return
	}
	outcome := outcomeFromError(err)
	if outcome == "conflict" {
		s.metrics.IncTxConflict()
	}
	s.metrics.ObserveTx(mode, outcome, time.Since(started))
}

func (t *tx) observeOperation(name string, err error, started time.Time) {
	if t == nil || t.store == nil || t.store.metrics == nil {
		return
	}
	outcome := outcomeFromError(err)
	if outcome == "conflict" {
		t.store.metrics.IncTxConflict()
	}
	t.store.metrics.ObserveOperation(name, outcome, time.Since(started))
}

func outcomeFromError(err error) string {
	if err == nil {
		return "ok"
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return "not_found"
	}
	if graph.IsKind(err, graph.ErrKindConflict) {
		return "conflict"
	}
	return "error"
}

func buildPropertyStatsSummary(
	ndv map[string]map[string]int,
	entries map[string]map[string]int,
	ndvByKind map[string]map[string]map[string]int,
	entriesByKind map[string]map[string]map[string]int,
	histograms map[string]map[string]map[string]*graph.StatsHistogram,
	epochByProperty map[string]map[string]int64,
	sampleByProperty map[string]map[string]int,
	refreshByProperty map[string]map[string]time.Time,
) map[string]map[string]graph.StatsPropertySummary {
	out := map[string]map[string]graph.StatsPropertySummary{}
	merge := func(schema, property string) {
		if schema == "" || property == "" {
			return
		}
		if out[schema] == nil {
			out[schema] = map[string]graph.StatsPropertySummary{}
		}
		summary := out[schema][property]
		if summary.DistinctValuesByKind == nil {
			summary.DistinctValuesByKind = map[string]int{}
		}
		if summary.IndexedEntriesByKind == nil {
			summary.IndexedEntriesByKind = map[string]int{}
		}
		if summary.EstimatedSelectivityByKind == nil {
			summary.EstimatedSelectivityByKind = map[string]float64{}
		}
		if summary.Histograms == nil {
			summary.Histograms = map[string]*graph.StatsHistogram{}
		}
		if ndv[schema] != nil {
			summary.DistinctValues = ndv[schema][property]
		}
		if entries[schema] != nil {
			summary.IndexedEntries = entries[schema][property]
		}
		if ndvByKind[schema] != nil && ndvByKind[schema][property] != nil {
			for kind, value := range ndvByKind[schema][property] {
				summary.DistinctValuesByKind[kind] = value
				if value > 0 {
					summary.EstimatedSelectivityByKind[kind] = 1 / float64(value)
				}
			}
		}
		if entriesByKind[schema] != nil && entriesByKind[schema][property] != nil {
			for kind, value := range entriesByKind[schema][property] {
				summary.IndexedEntriesByKind[kind] = value
			}
		}
		if summary.DistinctValues > 0 {
			summary.EstimatedSelectivity = 1 / float64(summary.DistinctValues)
		}
		if histograms[schema] != nil && histograms[schema][property] != nil {
			for kind, histogram := range histograms[schema][property] {
				summary.Histograms[kind] = histogram
			}
			summary.Histogram = primaryHistogram(summary.Histograms)
		}
		if epochByProperty[schema] != nil {
			summary.StatsEpoch = epochByProperty[schema][property]
		}
		if sampleByProperty[schema] != nil {
			summary.SampleSize = sampleByProperty[schema][property]
		}
		if refreshByProperty[schema] != nil {
			summary.LastRefreshTS = refreshByProperty[schema][property]
		}
		out[schema][property] = summary
	}
	for schema, properties := range ndv {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range entries {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range histograms {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range ndvByKind {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range entriesByKind {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range epochByProperty {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range sampleByProperty {
		for property := range properties {
			merge(schema, property)
		}
	}
	for schema, properties := range refreshByProperty {
		for property := range properties {
			merge(schema, property)
		}
	}
	return out
}

func primaryHistogram(histograms map[string]*graph.StatsHistogram) *graph.StatsHistogram {
	if len(histograms) == 0 {
		return nil
	}
	if histograms["numeric"] != nil {
		return histograms["numeric"]
	}
	if histograms["datetime"] != nil {
		return histograms["datetime"]
	}
	if histograms["boolean"] != nil {
		return histograms["boolean"]
	}
	if histograms["categorical"] != nil {
		return histograms["categorical"]
	}
	kinds := make([]string, 0, len(histograms))
	for kind := range histograms {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return histograms[kinds[0]]
}

func statsPropertyHistogramPrefix(tenant, entityClass, schema, property string) []byte {
	if entityClass == "vertex" {
		return []byte("s/" + tenant + "/vertex_property_hist/" + schema + "/" + property + "/")
	}
	return []byte("s/" + tenant + "/edge_property_hist/" + schema + "/" + property + "/")
}

func statsPropertyCounterByKindPrefix(tenant, entityClass, schema, property string) []byte {
	if entityClass == "vertex" {
		return []byte("s/" + tenant + "/vertex_property_ndv_kind/" + schema + "/" + property + "/")
	}
	return []byte("s/" + tenant + "/edge_property_ndv_kind/" + schema + "/" + property + "/")
}

func statsPropertyEntriesByKindPrefix(tenant, entityClass, schema, property string) []byte {
	if entityClass == "vertex" {
		return []byte("s/" + tenant + "/vertex_property_entries_kind/" + schema + "/" + property + "/")
	}
	return []byte("s/" + tenant + "/edge_property_entries_kind/" + schema + "/" + property + "/")
}

type propertyValueCount struct {
	value string
	count int
}

func buildEquiDepthHistogram(kind string, valueCounts []propertyValueCount, maxBuckets int) *graph.StatsHistogram {
	if len(valueCounts) == 0 {
		return nil
	}
	if maxBuckets <= 0 {
		maxBuckets = statsHistogramBucketCount
	}
	total := 0
	for _, item := range valueCounts {
		total += item.count
	}
	if total <= 0 {
		return nil
	}
	target := int(math.Ceil(float64(total) / float64(maxBuckets)))
	if target <= 0 {
		target = 1
	}
	buckets := make([]graph.StatsHistogramBucket, 0, maxBuckets)
	lower := valueCounts[0].value
	upper := valueCounts[0].value
	bucketCount := 0
	remainingBuckets := maxBuckets
	for idx, item := range valueCounts {
		if bucketCount == 0 {
			lower = item.value
		}
		upper = item.value
		bucketCount += item.count
		remainingValues := len(valueCounts) - idx - 1
		shouldFlush := bucketCount >= target
		if remainingBuckets <= 1 || remainingValues <= 0 {
			shouldFlush = true
		}
		if !shouldFlush {
			continue
		}
		buckets = append(buckets, graph.StatsHistogramBucket{LowerBound: lower, UpperBound: upper, Count: bucketCount})
		bucketCount = 0
		remainingBuckets--
	}
	return &graph.StatsHistogram{Kind: kind, Buckets: buckets}
}

func buildHistogramFromCounts(kind string, counts map[string]int, maxBuckets int) *graph.StatsHistogram {
	if len(counts) == 0 {
		return nil
	}
	valueCounts := make([]propertyValueCount, 0, len(counts))
	for value, count := range counts {
		if count <= 0 {
			continue
		}
		valueCounts = append(valueCounts, propertyValueCount{value: value, count: count})
	}
	if len(valueCounts) == 0 {
		return nil
	}
	sort.Slice(valueCounts, func(i, j int) bool {
		return valueCounts[i].value < valueCounts[j].value
	})
	return buildEquiDepthHistogram(kind, valueCounts, maxBuckets)
}
