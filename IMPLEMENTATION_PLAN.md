# VitalEdge Property Graph Implementation Plan

## Status

Active plan with ongoing status refreshes, derived from [DESIGN.md](DESIGN.md).

## Planning Goals

1. Deliver an edge-first, production-credible single-node property graph.
2. Preserve a clean path to replicated and partitioned operation.
3. Prove readiness through explicit phase gates (correctness, performance, operability).

## Scope and Non-Goals

In scope:

- Storage and graph core (Pebble-backed).
- Query execution on top of parser AST.
- Benchmarks for research, log detection, and ReBAC workloads.
- Replication and partitioning phases.

Out of scope for this plan version:

- UI tooling and visual graph exploration.
- Full language-level optimization beyond required planner work.

## Workstream Overview

- WS1: Storage engine and data model.
- WS2: Query planning and execution.
- WS3: Indexing and performance.
- WS4: Reliability, observability, and operations.
- WS5: Distribution (replication, then partitioning).

## Phase Plan

## Phase 0: Foundations and Contracts

Objective: lock interfaces and invariants before heavy implementation.

Current implementation status:

- Implemented: GraphStore and transaction contracts.
- Implemented: keyspace encoding package and tests.
- Implemented: error taxonomy scaffolding.
- Implemented: benchmark harness skeleton and runnable smoke benchmark.

Milestones:

1. Define GraphStore interface and transaction boundaries.
2. Define keyspace schema and encoding conventions.
3. Define error model (parse, semantic, storage, execution).
4. Define benchmark harness and dataset contracts.

Deliverables:

- GraphStore interface spec.
- Keyspace specification doc section with examples.
- Error taxonomy and API mapping.
- Benchmark driver skeleton committed.

Exit Criteria:

- Interface and keyspace reviewed and frozen for Phase 1.
- At least one end-to-end smoke benchmark script runs.

## Phase 1: Single-Node Local Graph Core (Edge-first MVP)

Objective: deliver local graph correctness with acceptable performance.

Current implementation status:

- Implemented: Pebble-backed GraphStore with transactional CRUD, adjacency indexes, property-index write/delete, and property-index scan.
- Implemented: durability and concurrency coverage for store operations (including restart durability and concurrent mutation stress).
- Implemented: store metrics hooks for tx/operation outcomes and durations (registration/lifecycle owned externally).
- Implemented: executor core clause pipeline for MATCH, OPTIONAL MATCH, WHERE, RETURN, CREATE, MERGE, SET, REMOVE, DELETE, WITH, and UNWIND.
- Implemented: index-first anchored source lookup for configured property indexes, with fallback behavior and explicit missing-index errors for unsupported property lookup patterns.
- Implemented: configuration-based index DDL loading into runtime index catalog.
- Implemented: executor metrics for statement outcomes, rows returned, index candidates, and index lookup outcomes; concrete in-process collector plus top unindexed-candidate reporting.
- Implemented: startup wiring for graph path, tenant defaults, index config loading, and metrics reporting.
- Implemented: lightweight TCP integration tests for request parsing/execution/JSON response path.
- Implemented: expanded Cypher compatibility for relationship patterns (directed, reverse-directed, undirected, type alternation, relationship properties), two-hop chain matching, and chained MATCH clause execution with shared bindings.
- Implemented: projection and aggregation enhancements for `count(...)`, `collect(...)`, `labels(...)`, and `type(...)`, including WITH alias propagation and ORDER BY/LIMIT handling in projection flow.
- Implemented: EXISTS subquery support in WHERE for supported MATCH subquery bodies.

Phase 1 deliverable status:

- Working single-node graph engine package: complete.
- Integration tests covering mixed read/update query flows: complete for current supported clause surface.
- Basic index DDL and index utilization in planner: complete via configuration-based index declarations and runtime index catalog consumption.

Remaining Phase 1 gaps before close:

- Phase 1 performance evidence has now been captured in this document (see Phase 1 closure evidence below).
- Operability baseline for Phase 1 is satisfied via in-process metrics collectors and periodic metrics/recommendation reporting; production sink/export wiring remains a Phase 2+ hardening task.
- Cypher compatibility backlog (non-blocking for Phase 1 close): path-variable capture and return (example: `MATCH path = ()-[:ACTED_IN]->(movie:Movie) RETURN path`) remains deferred.
- Planner/explainability backlog (Phase 2): explain-plan output coverage is still listed as a Phase 2 deliverable and is not yet evidenced for all supported query forms.

Milestones:

1. Pebble-backed GraphStore implementation.
2. Vertex/edge CRUD with adjacency indexes (in/out).
3. Basic property indexes (explicitly declared).
4. Single-node transactional write path (durable batches).
5. Query executor for core clauses:
   - MATCH / OPTIONAL MATCH
   - WHERE
   - RETURN
   - CREATE / MERGE / SET / DELETE / REMOVE
   - WITH
   - UNWIND

Deliverables:

- Working single-node graph engine package.
- Integration tests covering mixed read/update query flows.
- Basic index DDL and index utilization in planner.

Exit Criteria (must all pass):

- Correctness:
  - Deterministic CRUD behavior under concurrency tests.
  - No data loss across restart in durability tests.
- Performance (target baseline, tune later):
  - ReBAC p95 query latency <= 10 ms on representative local edge dataset.
  - Structured log ingest sustained >= 50k edges/min on reference hardware.
- Operability:
  - Exposed metrics for reads/writes/compactions/txn conflicts.

Current gate assessment:

- Correctness: passing based on current automated test suite.
- Performance: passing on current local benchmark evidence.
- Operability: passing for Phase 1 baseline (metrics exposed through in-process collectors and runtime reporting).

### Phase 1 closure evidence (captured 2026-05-22)

Command and outputs:

- `go run ./cmd/vitaledge-bench -scenario threat -iterations 20000`
   - `edges_per_min=8056885.100`
   - `ops_per_sec=134281.418`
- `go run ./cmd/vitaledge-bench -scenario rebac -iterations 2000`
   - `p95_ms=2.241`
   - `avg_ms=1.602`
   - `ops_per_sec=623.931`
- `go run ./cmd/vitaledge-bench -scenario research -iterations 2000`
   - `p95_ms=5.942`
   - `avg_ms=5.289`
   - `ops_per_sec=189.029`

Target comparison:

- ReBAC p95 target (`<= 10 ms`): pass (`2.241 ms`).
- Structured ingest target (`>= 50k edges/min`): pass (`8,056,885 edges/min`).

## Phase 2: Hardening and Optimization (Single-node)

Objective: make Phase 1 robust and cost-efficient before cluster complexity.

Milestones:

1. Query planner improvements (predicate pushdown, index-first plans).
2. Compaction and write amplification tuning profiles.
3. Memory and cache strategy (block cache, iterator bounds).
4. Fault-injection and chaos-style local durability tests.

Deliverables:

- Planner explain output for observability.
- Tuning profiles for ingest-heavy vs read-heavy workloads.
- Long-run soak test suite.

Exit Criteria:

- 24h soak test with no correctness regressions.
- p95 latency and ingest throughput improve by agreed target over Phase 1 baseline.
- Explain output available for all supported query forms.

## Phase 3: Replicated Multi-node (Raft)

Objective: enable high availability with minimal change to query semantics.

Milestones:

1. Raft group abstraction for replicated key ranges.
2. Leaseholder reads and quorum writes.
3. Replica management (bootstrap, join, replace, recover).
4. Consistency-aware transaction coordinator for multi-range operations (initial scope).

Deliverables:

- Multi-node cluster mode with replication factor control.
- Cluster health and replication lag metrics.

Exit Criteria:

- Node loss tests: cluster remains available under expected quorum assumptions.
- Data consistency checks pass under failover scenarios.
- Documented SLO for failover recovery time.

## Phase 4: Partitioning and Rebalancing (Cockroach-like evolution)

Objective: scale horizontally with balanced data placement.

Milestones:

1. Range splitting and metadata catalog.
2. Rebalancing policies (size/load-based).
3. Tenant-aware placement controls.
4. Cross-range query planning and execution support.

Deliverables:

- Automatic range split/rebalance subsystem.
- Placement and movement observability dashboards/metrics.

Exit Criteria:

- Rebalance tests maintain service under skewed load.
- Throughput scales with added nodes on benchmark datasets.
- No correctness regression in cross-range query tests.

## Phase 5: Production Readiness

Objective: stabilize APIs, operations, and upgrade story.

Milestones:

1. On-disk format versioning and migration tooling.
2. Backups/snapshots and restore workflows.
3. Security controls (authn/authz boundaries for multi-tenant operation).
4. Release process and compatibility policy.

Deliverables:

- Upgrade/migration documentation and tooling.
- Backup/restore validation suite.
- Production runbook.

Exit Criteria:

- Successful upgrade + rollback rehearsal.
- Backup restore RTO/RPO targets met in tests.
- API compatibility policy published.

## Workstream Details

### WS1: Storage and Data Model

Primary outputs:

- Key encoding package.
- Transaction wrapper.
- Graph CRUD primitives.

Quality gates:

- Property index correctness under concurrent mutation.
- Restart consistency tests.

### WS2: Query Planning and Execution

Primary outputs:

- Logical plan from AST.
- Physical operators for pattern expansion, filtering, projection, updates.

Quality gates:

- Plan determinism tests.
- Explain-plan coverage in integration tests.

### WS3: Indexing and Performance

Reference: index management decision is documented in [DESIGN.md](DESIGN.md) under "Index Management Decision".

Primary outputs:

- Index selection strategy.
- Adaptive scan bounds.

Quality gates:

- Benchmark threshold checks in CI (non-blocking initially, then blocking).

### WS4: Reliability and Operations

Primary outputs:

- Metrics/tracing/logging standards.
- Health endpoints and diagnostics.

Quality gates:

- Alertable conditions documented and testable.

### WS5: Distribution

Primary outputs:

- Replication protocol integration.
- Partition metadata and balancing.

Quality gates:

- Failover and split/merge scenario suite.

## Benchmark and Validation Plan

Use-case aligned benchmark suites:

1. Research loading:
   - Bulk import time
   - index build time
   - traversal latency on dense subgraphs
2. Threat/anomaly detection:
   - sustained ingest throughput
   - sliding-window query latency
   - compaction behavior under burst writes
3. ReBAC at edge:
   - path-check latency (1-3 hops typical)
   - update/read contention behavior
   - cold-start and restart recovery time

## Milestone Tracking Template

For each milestone, track:

- Owner
- Start/target date
- Dependencies
- Risk level
- Current status
- Exit evidence (tests/benchmarks/docs)

## Top Risks to Track

1. Scope drift in query execution breadth before storage hardening.
2. Performance regressions from over-indexing.
3. Premature distributed complexity before single-node maturity.

## Recommended Immediate Next Sprint

1. Capture and record benchmark exit evidence against Phase 1 targets (ReBAC p95 and ingest throughput) on reference hardware.
2. Close operability evidence gap by wiring metrics export/sink integration and documenting alertable signals.
3. Advance Cypher compatibility on deferred items, prioritizing path-variable capture/return and any remaining projection edge cases.
4. Begin Phase 2 explainability by introducing EXPLAIN output for currently supported query shapes.

Back to [DESIGN.md](DESIGN.md) and [README.md](README.md).
