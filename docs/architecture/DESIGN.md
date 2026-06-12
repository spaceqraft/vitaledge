# VitalEdge Graph Design

## Status

Implemented for the current single-node engine. Core stronger typed-graph work is complete; optional type constraints are deferred to a separate proposal.

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

## MVP Envelope (Explicit Minimum)

The MVP line is intentionally broad, but the following capabilities are treated as required minimum scope:

1. Single-node operation as the default production shape.
2. Cypher-compliant behavior for the implemented language surface (guarded by openCypher TCK).
3. CLI workflow that supports:
   - setting variables/parameters,
   - submitting Cypher statements,
   - rendering result sets as tables,
   - reporting output statistics.
4. Benchmark comparison against Neo4j and TigerGraph using equivalent dataset and workload definitions.
5. Observability baseline with Prometheus metrics export and Grafana dashboarding.
6. Manual index tuning workflow including EXPLAIN, planner statistics, and query cost estimation.

These capabilities should be treated as delivery constraints for near-term planning, even when exact phase boundaries remain intentionally flexible.

## Post-TCK Reflection

Achieving full openCypher TCK compliance was a major architectural checkpoint, not just a feature milestone.

What it validated:

- The local-first, single-node-first phase ordering was correct. Semantics and graph correctness could be stabilized before distributed complexity.
- Keeping Cypher parsing and storage internals decoupled was still the right top-level direction.
- Compatibility work paid off as a forcing function for correctness across MATCH/OPTIONAL MATCH, WITH, ORDER BY, aggregation, writes, temporal values, and error classification.

What it revealed:

- Cypher compliance was primarily a semantic and phase-boundary problem, not a grammar problem.
- The intended `scan -> parse -> execute -> result` progression is currently too text-driven in places. Several supported forms required raw-clause reinterpretation and regex-based recovery inside executor paths.
- The parser/executor boundary is too leaky for some clause families. SKIP/LIMIT, ORDER BY, DISTINCT, MERGE actions, and pattern forms sometimes required the executor to rediscover intent from normalized text rather than consume a sufficiently rich representation.
- Regex use in executor pattern handling was an effective delivery tactic, but it is now a clear debt marker. Edge patterns are a core graph/operator concept, closer to join structure than incidental syntax, and should not remain primarily encoded as executor-local string heuristics.
- In this domain, semantic continuity across phases matters more than textbook component isolation. Overly narrow component seams can force downstream phases to reconstruct meaning that should have been preserved structurally.

## Query Engine Direction After TCK

The next architectural step is not broader Cypher surface area by default. It is stronger representation fidelity between phases.

Direction:

1. Preserve explicit phase boundaries, but pass richer structured artifacts across them.
   - Parsing should produce enough clause and expression structure that later phases do not need to re-parse normalized text.
   - Semantic validation should be treated as a first-class stage between parse and execution.
2. Reduce executor dependence on regex and raw-clause recovery.
   - Prioritize pattern clauses, projection clauses, ORDER BY, SKIP/LIMIT, and write-action blocks.
3. Make pattern and edge structure first-class in planning.
   - Edge and path semantics should be represented as durable intermediate forms suitable for logical and physical planning.
4. Treat current compliance as a correctness baseline that refactors must preserve.
   - TCK and focused executor/parser regressions should remain the guardrail for any query-engine restructuring.

This shifts Phase 2 emphasis from "more supported syntax" toward "cleaner phase handoff, explainability, and semantics-preserving execution architecture."

## Edge Property Index Pushdown (Current Semantics)

Current relationship traversal optimization uses configured edge property indexes in two ways:

1. Equality pushdown from relationship pattern properties, e.g. `MATCH (a)-[r:TYPE {k:$v}]->(b)`.
2. Numeric range pushdown from WHERE conjuncts over relationship properties, e.g. `r.rating >= 4` or `r.rating > 4 AND r.rating <= 5`.

Correctness and fallback policy:

1. Pushdown is conservative; unsupported or ambiguous WHERE forms are not pushed down.
2. Residual predicate evaluation always runs, so non-pushdown predicates continue filtering rows after candidate retrieval.
3. Contradictory numeric ranges are detected early and short-circuit to empty results.
4. Broad range scans are protected by an internal candidate cap; when exceeded, execution falls back to adjacency expansion.

Operational note:

1. Edge property indexes are catalog-driven and backfilled automatically at startup from index schema configuration.

### Adaptive Pushdown Policy (Stage2 Recommendation Path)

The stage2 recommendation expansion path uses an adaptive pushdown policy rather than unconditional index use.

Current decision flow:

1. Predicate-shape pre-gate: broad one-sided numeric ranges are treated as non-selective for this path and skip index probing.
2. Bounded probe: when the shape is eligible, index probing is capped by a probe candidate limit.
3. Selectivity gate: probed candidates are accepted only when candidate volume and average candidates per source are below configured thresholds.
4. Fallback: if any gate fails, execution falls back to adjacency expansion.

Current default constants:

1. `stage2IndexPushdownProbeCandidateLimit = 1536`
2. `stage2IndexPushdownMaxIndexedCandidates = 512`
3. `stage2IndexPushdownMaxAverageEdgesPerSource = 16`

Correctness invariant:

1. Adaptive pushdown only selects an access path and must never alter query semantics.
2. Residual predicate evaluation remains authoritative for correctness.

Observability contract for adaptive behavior:

1. `fast_path.stage2.index_pushdown_applied`
2. `fast_path.stage2.index_pushdown_rows`
3. `fast_path.stage2.index_candidates_total`
4. `fast_path.stage2.index_pushdown_skipped_predicate_shape`
5. `fast_path.stage2.index_pushdown_skipped_unselective`

General optimization technique used by VitalEdge:

1. Do not treat index pushdown as universally beneficial for graph traversals.
2. Use bounded probing to estimate selectivity before committing to index-first expansion.
3. Keep fast-path decisions observable through runtime counters and benchmark A/B variants.
4. Re-tune thresholds with representative broad and selective workload shapes as data distributions change.
5. Feed observed fast-path selectivity back into planning metadata so EXPLAIN can surface what the runtime learned.
6. When runtime feedback is available for the same executor instance, let EXPLAIN cost estimates become sample-aware rather than purely heuristic.

## EXPLAIN Specification Decision

### Decision

Treat `EXPLAIN` as a dry-run planning command that executes parse, semantic validation, and planning, but does not execute result production or apply writes.

### EXPLAIN semantics

1. `EXPLAIN` must not mutate graph state.
2. `EXPLAIN` must emit the selected plan and plan-influencing details.
3. `EXPLAIN` may read planner metadata and bounded cardinality signals used by planning.
4. `EXPLAIN` output should be deterministic for the same query and the same stats/catalog snapshot.

### Planning stages executed under EXPLAIN

1. Parse: query text to typed AST.
2. Semantic validation: scope/type checks and error classification.
3. Logical planning: operator graph construction.
4. Physical planning: operator/access-path selection.

### EXPLAIN output contract

`EXPLAIN` output must include:

1. Logical plan operators.
2. Physical plan operators and access paths.
3. Plan-influencing details:
   - relevant vertex counts by label,
   - relevant edge counts by type/direction,
   - predicate-related cardinality signals,
   - index candidate and index-selection reasoning.
4. Per-node cardinality entries with quality metadata: `exact`, `estimate`, or `sample`.
5. Warnings for missing stats, full scans, unsupported optimization, or fallback decisions.
6. Query shape/fingerprint metadata.

Payload normalization rule:

1. List-shaped EXPLAIN sections should preserve their existing flat fields for compatibility.
2. The same entries should also expose a nested `assessment` object that groups the evidence used to form the result.
3. This applies to influencer counts, predicate signals, index decisions, cardinality, cost estimates, warnings, and execution strategies.
4. The goal is to keep EXPLAIN readable and internally coherent without breaking existing consumers.

Current concrete diagnostics and tuning signals:

1. Operator-shaped plan nodes for scan/filter/project/distinct/sort/skip/limit/write/call forms.
2. Access-path annotations on scan operators (`property_index`, label scan, or all-vertices scan).
3. Index decisions including recommendation and impact fields (`recommendation`, `tuningImpact`, `estimatedRowsSaved`, and `quality`).
4. Query-level cost estimate (`costEstimate`) for deterministic before/after tuning comparisons.
5. Runtime planning counters (`runtimeStats`) for store/plan/index/cardinality summaries.
6. Warning diagnostics keyed by explicit fallback conditions (`MISSING_PROPERTY_INDEX`, `FULL_SCAN_FALLBACK`, `ESTIMATE_ONLY_INDEX_SIGNAL`, `WRITE_QUERY_DRY_RUN`, `MISSING_TENANT_CONTEXT`).

### Runtime Counter Instrumentation (DX Transparency)

Decision:

1. Query execution should emit machine-readable runtime counter diagnostics for hot optimization paths.
2. The same counters should be exported as process-level Prometheus metrics for operational visibility.

Current implementation envelope:

1. Fast recommendation clause-pair paths emit per-query warning diagnostics with code `RUNTIME_COUNTERS` and JSON payload values.
2. Counter families include stage edge visits, prefilter drops, top-k pushdown usage, and stage output row counts.
3. Process-level accumulation is exported as `vitaledge_executor_runtime_counters_total{counter=...}` on `/metrics`.

Rationale:

1. This makes optimizer behavior observable without requiring profiler attachment or ad-hoc logging.
2. It supports the VitalEdge DX goal of understandable performance behavior for large graph workloads.

### Statistics-as-data and backfill policy

Decision:

1. Planner influencer statistics are maintained as persisted data, updated as side effects of graph mutations.
2. EXPLAIN must surface statistics coverage completeness so operators can determine whether tuning signals are fully reliable.

Current persisted statistics envelope:

1. Tenant totals: vertex and edge counts.
2. Per-label vertex counts.
3. Per-edge-type counts.

Backfill expectations:

1. New writes maintain stats incrementally.
2. Existing datasets from earlier versions are migrated by a startup database migration (`no-stats` -> `stats`) that backfills missing statistics keys.

### Cardinality quality policy

1. Values from exact maintained stats are marked `exact`.
2. Heuristic values are marked `estimate`.
3. Sampled/probed values are marked `sample` and should include sampling context when available.

### Transport and representation policy

1. Runtime transport direction is gRPC/protobuf.
2. During current query-engine iteration, EXPLAIN request/response will continue to use Cypher text input and JSON output for faster iteration.
3. The JSON structure is treated as the canonical short-term contract and will be mapped to protobuf messages when the gRPC layer is wired.
4. Clients are encouraged to send parameterized queries; parameter binding is applied server-side using typed request parameters.

Reference schema: [EXPLAIN_OUTPUT_SCHEMA.json](EXPLAIN_OUTPUT_SCHEMA.json).

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
4. Risk: query semantics remain encoded in executor-local regex/string heuristics.
   - Mitigation: promote clause/pattern/projection structure into parser and planner artifacts before adding major new query-engine surface.

## Out of Scope (For This Decision)

- Full distributed transaction protocol details.
- Final on-disk schema versioning mechanics.
- Query planner internals beyond storage assumptions.

## Next Steps

Execution detail is tracked in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).
Key encoding details are tracked in [GRAPH_KEYSPACE.md](GRAPH_KEYSPACE.md).
Benchmark dataset contracts are tracked in [benchmarks/DATASETS.md](benchmarks/DATASETS.md).

1. Preserve the full TCK compliance baseline while reducing executor regex/raw-clause dependence.
2. Introduce richer intermediate representations for patterns, projections, ordering, pagination, and write actions.
3. Add explainable logical/physical planning artifacts for currently supported query shapes.
4. Carry the resulting engine contracts forward into replicated and partitioned phases without changing visible semantics.
