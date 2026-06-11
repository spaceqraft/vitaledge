package graph

import "time"

// StatsHistogramBucket captures one persisted histogram bucket.
type StatsHistogramBucket struct {
	LowerBound string
	UpperBound string
	Count      int
}

// StatsHistogram captures one persisted histogram for a numeric or datetime property.
type StatsHistogram struct {
	Kind    string
	Buckets []StatsHistogramBucket
}

// StatsPropertySummary captures persisted property-level summary stats.
type StatsPropertySummary struct {
	DistinctValues       int
	IndexedEntries       int
	EstimatedSelectivity float64
	Histogram            *StatsHistogram
	// DistinctValuesByKind captures distinct value counts by value family.
	DistinctValuesByKind map[string]int
	// IndexedEntriesByKind captures indexed entry counts by value family.
	IndexedEntriesByKind map[string]int
	// EstimatedSelectivityByKind captures equality selectivity by value family.
	EstimatedSelectivityByKind map[string]float64
	// Histograms stores persisted histograms keyed by family kind.
	Histograms map[string]*StatsHistogram
	// StatsEpoch is the property-level refresh epoch for this summary.
	StatsEpoch int64
	// SampleSize is the property-level sample size used to compute this summary.
	SampleSize int
	// LastRefreshTS is the property-level refresh timestamp.
	LastRefreshTS time.Time
}

// StatsSnapshot captures persisted tenant-level graph totals and property summaries.
type StatsSnapshot struct {
	Tenant        string
	StatsEpoch    int64
	SampleSize    int
	LastRefreshTS time.Time
	VertexTotal   int
	EdgeTotal     int
	LabelCounts   map[string]int
	EdgeCounts    map[string]int
	// EdgeSourceCounts stores distinct source-vertex counts per edge type.
	EdgeSourceCounts map[string]int
	// EdgeAvgOutDegree stores derived average out-degree per edge type.
	EdgeAvgOutDegree map[string]float64
	// VertexPropertyStats stores per-schema per-property index summaries for vertex indexes.
	VertexPropertyStats map[string]map[string]StatsPropertySummary
	// EdgePropertyStats stores per-schema per-property index summaries for edge indexes.
	EdgePropertyStats map[string]map[string]StatsPropertySummary
}
