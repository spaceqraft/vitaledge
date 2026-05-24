# VitalEdge Graph Design

## Status

Proposed (decision-driving document for initial implementation)

## Context

VitalEdge needs a property graph engine that supports three primary use cases:

1. Research and dataset loading.
2. Threat and anomaly detection over structured logs.
3. ReBAC at the edge.

Constraints and priorities:

- Strong edge deployment value (single binary, low operational footprint, offline-tolerant).
- Path to local and distributed operation (CockroachDB-like progression).
- Avoid unnecessary translation layers that can hurt write/read latency.
- Keep Cypher parser and execution decoupled from storage internals.

## Decision Summary

1. Local/distributed model: local-first, distributed-ready architecture.
2. Storage model: direct property-graph storage on embedded LSM KV (Pebble) for v1.
3. Distribution strategy: introduce Raft-based replication and range partitioning after single-node correctness/perf are stable.

## Decision 1: Local/Distributed Architecture

### Decision

Adopt a phased architecture:

- Phase 1: single-node local engine (edge-first default).
- Phase 2: replicated multi-node clusters (Raft groups).
- Phase 3: range/shard partitioning with rebalancing (CockroachDB-like operational model).

### Why

- Edge and ReBAC workloads benefit from low-latency local reads/writes.
- Single-node first keeps execution semantics and graph indexing correct before distributed complexity.
- The same storage and keyspace design can be preserved when adding Raft and sharding.

### Implications

- API and key layout must be deterministic and shard-friendly from day one.
- Transaction semantics should start as single-node serializable (or strict snapshot + write conflict detection), then map to distributed txn later.

## Decision 2: Storage Engine

### Decision

Use Pebble (embedded LSM KV in Go) as the initial storage backend.

### Why

- Edge-friendly: embedded, no external service required.
- Go-native implementation avoids CGO and simplifies cross-platform deployment.
- Strong write throughput and predictable compaction behavior for log-heavy ingestion workloads.
- Range/key-oriented design aligns with future replication and partitioning.

## Storage Options Matrix

Scoring: 1 (poor) to 5 (excellent). Higher total is better for VitalEdge priorities.

| Option | Edge Footprint | Write Throughput | Read Latency | Ops Simplicity | Distributed Evolution Fit | Implementation Complexity | Total |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Pebble (embedded LSM) | 5 | 5 | 4 | 5 | 5 | 3 | 27 |
| Badger (embedded LSM+value log) | 4 | 4 | 4 | 4 | 4 | 3 | 23 |
| RocksDB (CGO) | 3 | 5 | 4 | 2 | 5 | 3 | 22 |
| SQLite (embedded relational) | 5 | 3 | 4 | 5 | 2 | 4 | 23 |
| PostgreSQL (external RDBMS) | 2 | 4 | 3 | 2 | 3 | 4 | 18 |
| CockroachDB (external distributed SQL) | 1 | 4 | 3 | 2 | 5 | 4 | 19 |

### Matrix Notes

- SQLite scores high on simplicity but lower on future partitioned/distributed graph evolution.
- External SQL engines are viable for some graph overlays but reduce edge value due to process/runtime overhead.
- RocksDB is strong technically but CGO and packaging friction are meaningful edge drawbacks.
- Pebble provides the best balance for edge-first plus distributed progression.

## Graph Storage Layout (Initial)

Store graph primitives directly in KV with prefix-structured keys:

- Vertex record: `v/{tenant}/{vertexId}` -> vertex payload.
- Edge record: `e/{tenant}/{edgeId}` -> edge payload.
- Out adjacency index: `a/out/{tenant}/{srcId}/{edgeType}/{edgeId}` -> dstId/meta.
- In adjacency index: `a/in/{tenant}/{dstId}/{edgeType}/{edgeId}` -> srcId/meta.
- Property index (optional, per indexed field): `i/{tenant}/{labelOrType}/{prop}/{encodedValue}/{entityId}`.

Design principles:

- Prefix locality for range scans.
- Tenant/namespace first in key path for isolation and future placement controls.
- No mandatory relational translation layer.

## Transaction and Consistency Direction

- Phase 1: single-node ACID-like behavior (write batch + WAL durability + conflict checks).
- Phase 2+: per-range Raft replication, leaseholder reads, distributed txn coordinator for multi-range operations.

## ReBAC and Detection Fit

- ReBAC: adjacency-first indexes favor low-hop authorization checks.
- Threat/anomaly: high write ingest path optimized via LSM and batched writes.
- Research workloads: flexible property indexing with scan-friendly key layout.

## Index Management Decision

### Question

How should property indexes be declared and managed so planner behavior is predictable, operationally simple, and safe for edge deployments?

### Options Considered

1. No secondary property indexes.
2. Extend Cypher with index DDL (for example `CREATE INDEX`).
3. Fully automatic indexing by observing workload.
4. Configuration-based index declarations (startup config/schema file).

### Additional Viable Options

5. External control-plane/API-managed catalog.
   - Index definitions managed through an admin API/CLI (non-Cypher), persisted in metadata.
   - Good fit when query language stability is prioritized over operational flexibility.
6. Hybrid model: explicit baseline + adaptive candidate indexes.
   - Operators define required indexes; system proposes or builds candidate indexes under policy guardrails.
   - Balances determinism with performance adaptation.
7. Materialized projection/index service.
   - Maintain specialized denormalized projections for known query patterns, separate from generic property indexes.
   - Useful for very hot ReBAC/detection paths where generic indexes are insufficient.
8. Offline/maintenance-window index build pipeline.
   - Indexes declared separately and built by a job process (bulk/backfill first, then enable planner usage).
   - Reduces write-path disruption for ingest-heavy deployments.

### Decision (Current Phase)

Use configuration-based index declarations as the primary mechanism in Phase 1, backed by a runtime index catalog consumed by the planner.

Rationale:

- Keeps planner behavior deterministic and explainable.
- Avoids immediate Cypher grammar/surface expansion while parser/executor are still maturing.
- Preserves edge simplicity (single binary, no mandatory external control plane).
- Creates a clean path to future Cypher DDL or API-based management without changing planner contracts.

### Deferred Evolution

- Add Cypher index DDL after core query surface stabilizes and migration semantics are defined.
- Add optional adaptive indexing only with explicit policy controls, observability, and bounded resource usage.
- Add planner explain output that reports whether a chosen index came from configured baseline or adaptive candidate path.

## Networking Port Planning Note

Keep a centralized port map as multi-node and production features are introduced.

| purpose | port | mTLS |
| --- | --- | --- |
| client TCP | 6379 | NO |
| gRPC API | 7443 | YES |
| Prometheus metrics | 9464 | NO |
| Otel OTLP | 4327 | NO |
| RAFT control plane | 2380 | YES |
| Cluster replication | 2381 | YES |
| Admin UI | 8080 | NO |
| Node health | 8081 | NO |


## Risks and Mitigations

1. Risk: index explosion from over-indexing properties.
   - Mitigation: explicit index DDL and usage tracking before auto-indexing.
2. Risk: compaction amplification under burst ingest.
   - Mitigation: ingestion profiles, rate-limited compaction tuning, and benchmark gates.
3. Risk: distributed complexity too early.
   - Mitigation: enforce phase gates (correctness and perf SLOs before cluster features).

## Out of Scope (For This Decision)

- Full distributed transaction protocol details.
- Final on-disk schema versioning mechanics.
- Query planner internals beyond storage assumptions.

## Next Steps

Execution detail is tracked in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).
Key encoding details are tracked in [GRAPH_KEYSPACE.md](GRAPH_KEYSPACE.md).
Benchmark dataset contracts are tracked in [benchmarks/DATASETS.md](benchmarks/DATASETS.md).

1. Build a storage abstraction with Pebble-backed implementation (`GraphStore`).
2. Implement vertex/edge CRUD plus adjacency indexes.
3. Add benchmark suite for three target workloads (research load, log ingest, ReBAC checks).
4. Define phase gates and success criteria for moving from local to replicated mode.
