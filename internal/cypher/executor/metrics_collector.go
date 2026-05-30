package executor

import (
	"sort"
	"sync"
	"time"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

type StatementMetricKey struct {
	Kind    ast.StatementKind
	Outcome string
}

type StatementMetricValue struct {
	Count         int64
	TotalDuration time.Duration
}

type IndexCandidateKey struct {
	Tenant   string
	Schema   string
	Property string
	Indexed  bool
}

type IndexLookupKey struct {
	Strategy string
	Outcome  string
}

type IndexLookupValue struct {
	Count        int64
	TotalMatches int64
}

type UnindexedCandidate struct {
	Tenant   string
	Schema   string
	Property string
	Count    int64
}

type Snapshot struct {
	Statements      map[StatementMetricKey]StatementMetricValue
	RowsReturned    int64
	IndexCandidates map[IndexCandidateKey]int64
	IndexLookups    map[IndexLookupKey]IndexLookupValue
	DeleteCounters  map[string]int64
}

type Collector struct {
	mu              sync.Mutex
	statements      map[StatementMetricKey]StatementMetricValue
	rowsReturned    int64
	indexCandidates map[IndexCandidateKey]int64
	indexLookups    map[IndexLookupKey]IndexLookupValue
	deleteCounters  map[string]int64
}

var _ Metrics = (*Collector)(nil)

func NewCollector() *Collector {
	return &Collector{
		statements:      map[StatementMetricKey]StatementMetricValue{},
		indexCandidates: map[IndexCandidateKey]int64{},
		indexLookups:    map[IndexLookupKey]IndexLookupValue{},
		deleteCounters:  map[string]int64{},
	}
}

func (c *Collector) ObserveStatement(kind ast.StatementKind, outcome string, duration time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := StatementMetricKey{Kind: kind, Outcome: outcome}
	value := c.statements[key]
	value.Count++
	value.TotalDuration += duration
	c.statements[key] = value
}

func (c *Collector) ObserveRowsReturned(rows int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowsReturned += int64(rows)
}

func (c *Collector) ObserveIndexCandidate(tenant, schema, property string, indexed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := IndexCandidateKey{Tenant: tenant, Schema: schema, Property: property, Indexed: indexed}
	c.indexCandidates[key]++
}

func (c *Collector) ObserveIndexLookup(strategy, outcome string, matches int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := IndexLookupKey{Strategy: strategy, Outcome: outcome}
	value := c.indexLookups[key]
	value.Count++
	value.TotalMatches += int64(matches)
	c.indexLookups[key] = value
}

func (c *Collector) ObserveDeleteCounter(event string, delta int64) {
	if c == nil || delta == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteCounters[event] += delta
}

func (c *Collector) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	statements := make(map[StatementMetricKey]StatementMetricValue, len(c.statements))
	for key, value := range c.statements {
		statements[key] = value
	}

	indexCandidates := make(map[IndexCandidateKey]int64, len(c.indexCandidates))
	for key, value := range c.indexCandidates {
		indexCandidates[key] = value
	}

	indexLookups := make(map[IndexLookupKey]IndexLookupValue, len(c.indexLookups))
	for key, value := range c.indexLookups {
		indexLookups[key] = value
	}

	deleteCounters := make(map[string]int64, len(c.deleteCounters))
	for key, value := range c.deleteCounters {
		deleteCounters[key] = value
	}

	return Snapshot{
		Statements:      statements,
		RowsReturned:    c.rowsReturned,
		IndexCandidates: indexCandidates,
		IndexLookups:    indexLookups,
		DeleteCounters:  deleteCounters,
	}
}

func (c *Collector) TopUnindexedCandidates(limit int) []UnindexedCandidate {
	if c == nil || limit <= 0 {
		return nil
	}

	snapshot := c.Snapshot()
	list := make([]UnindexedCandidate, 0)
	for key, count := range snapshot.IndexCandidates {
		if key.Indexed {
			continue
		}
		list = append(list, UnindexedCandidate{Tenant: key.Tenant, Schema: key.Schema, Property: key.Property, Count: count})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Count != list[j].Count {
			return list[i].Count > list[j].Count
		}
		if list[i].Tenant != list[j].Tenant {
			return list[i].Tenant < list[j].Tenant
		}
		if list[i].Schema != list[j].Schema {
			return list[i].Schema < list[j].Schema
		}
		return list[i].Property < list[j].Property
	})

	if limit >= len(list) {
		return list
	}
	return list[:limit]
}
