package graph

// StatsSnapshot captures persisted tenant-level graph totals.
type StatsSnapshot struct {
	Tenant      string
	VertexTotal int
	EdgeTotal   int
	LabelCounts map[string]int
	EdgeCounts  map[string]int
}
