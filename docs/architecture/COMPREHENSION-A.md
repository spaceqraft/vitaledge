# VitalEdge Comprehension Answers

Purpose: concise reference answers for COMPREHENSION-Q.md.

## Topic 1: System Context and Priorities

<a id="a-001"></a>
### A-001
The three primary workload categories are: (1) research and dataset loading, (2) threat/anomaly detection over structured logs, and (3) ReBAC at the edge.
Back link: [Q-001](COMPREHENSION-Q.md#q-001)

<a id="a-002"></a>
### A-002
A single-node, local-first, single-binary focus minimizes operational complexity, enables edge/offline deployments, and lets the team validate correctness and performance before adding distributed features.
Back link: [Q-002](COMPREHENSION-Q.md#q-002)

<a id="a-003"></a>
### A-003
It means storing and querying graph primitives directly in the KV store, avoiding forced mapping through unrelated relational or translation layers that add latency and complexity.
Back link: [Q-003](COMPREHENSION-Q.md#q-003)

## Topic 2: Architecture Phases and Evolution

<a id="a-004"></a>
### A-004
Phase 1: single-node local engine for correctness and baseline performance; Phase 2: hardening and optimization (explainability, phase boundaries, planner improvements); Phase 3: replicated multi-node clusters with partitioning and rebalancing.
Back link: [Q-004](COMPREHENSION-Q.md#q-004)

<a id="a-005"></a>
### A-005
Deterministic, shard-friendly key layout, tenant-first key prefixes, stable APIs, and clear transaction boundaries are required from the start to enable future sharding and replication.
Back link: [Q-005](COMPREHENSION-Q.md#q-005)

<a id="a-006"></a>
### A-006
The single-node model uses serializable transactions with write conflict checks and WAL durability, which later map to per-range Raft replication and a distributed transaction coordinator for multi-range operations.
Back link: [Q-006](COMPREHENSION-Q.md#q-006)

## Topic 3: Storage Engine and Keyspace

<a id="a-007"></a>
### A-007
Pebble was chosen for its embedded, Go-native design (no CGO), strong write throughput, predictable compaction, and key-range orientation that aligns with future distributed evolution.
Back link: [Q-007](COMPREHENSION-Q.md#q-007)

<a id="a-008"></a>
### A-008
Key families: vertex records (`v/`), edge records (`e/`), out adjacency (`a/out/`), in adjacency (`a/in/`), and optional property indexes (`i/`), each mapping to graph primitives or index structures.
Back link: [Q-008](COMPREHENSION-Q.md#q-008)

<a id="a-009"></a>
### A-009
Tenant/namespace is first to enable strong isolation, efficient multi-tenant scans, and future placement/partitioning controls.
Back link: [Q-009](COMPREHENSION-Q.md#q-009)

<a id="a-010"></a>
### A-010
Prefix locality, deterministic ordering, and tenant-first key structure make scans efficient and future sharding straightforward.
Back link: [Q-010](COMPREHENSION-Q.md#q-010)

## Topic 4: Query Engine and Cypher Support

<a id="a-011"></a>
### A-011
Supported clauses: MATCH, OPTIONAL MATCH, WHERE, RETURN, CREATE, MERGE, SET, REMOVE, DELETE, WITH, UNWIND; with relationship pattern matching, chained MATCH, EXISTS subqueries, and projection/aggregation enhancements.
Back link: [Q-011](COMPREHENSION-Q.md#q-011)

<a id="a-012"></a>
### A-012
Decoupling allows the Cypher parser and executor to evolve independently from storage, improving maintainability, testability, and future backend flexibility.
Back link: [Q-012](COMPREHENSION-Q.md#q-012)

<a id="a-013"></a>
### A-013
Recent improvements: richer relationship pattern support (directed, undirected, alternation, properties), chained MATCH with shared bindings, EXISTS subqueries in WHERE, and enhanced projection/aggregation (count, collect, labels, type).
Back link: [Q-013](COMPREHENSION-Q.md#q-013)

<a id="a-014"></a>
### A-014
Path-variable capture and return is deferred; its absence is not blocking for Phase 1 since core correctness, performance, and operability gates are prioritized over full Cypher completeness.
Back link: [Q-014](COMPREHENSION-Q.md#q-014)

<a id="a-015"></a>
### A-015
TCK compliance revealed that executor reliance on regex and raw-clause recovery is architectural debt; the plan is to promote richer clause and pattern structure into parser/planner artifacts, reducing text-driven execution.
Back link: [Q-015](COMPREHENSION-Q.md#q-015)

## Topic 5: Index Management and Planner Behavior

<a id="a-016"></a>
### A-016
Property indexes are declared via configuration and loaded into a runtime catalog, ensuring deterministic planner behavior and operational simplicity in Phase 1.
Back link: [Q-016](COMPREHENSION-Q.md#q-016)

<a id="a-017"></a>
### A-017
Automatic indexing risks index explosion, write amplification, and unpredictable planner behavior; explicit config-based indexes and usage tracking mitigate these risks.
Back link: [Q-017](COMPREHENSION-Q.md#q-017)

<a id="a-018"></a>
### A-018
Planned features: Cypher index DDL, policy-guarded adaptive indexing, explain output distinguishing baseline vs adaptive indexes, and offline index build pipelines.
Back link: [Q-018](COMPREHENSION-Q.md#q-018)

## Topic 6: Reliability, Operability, and Performance

<a id="a-019"></a>
### A-019
Phase 1 exit criteria: correctness (concurrent CRUD, restart durability), performance (ReBAC p95 ≤ 10ms, ingest ≥ 50k edges/min), and operability (metrics for reads/writes/compactions/txn conflicts).
Back link: [Q-019](COMPREHENSION-Q.md#q-019)

<a id="a-020"></a>
### A-020
Benchmarks show ReBAC p95 at 2.2ms (target ≤ 10ms) and ingest at ~8M edges/min (target ≥ 50k), both exceeding Phase 1 requirements.
Back link: [Q-020](COMPREHENSION-Q.md#q-020)

<a id="a-021"></a>
### A-021
Operability baseline means in-process metrics collection and reporting are present and usable for local operation; production-grade export/integration is deferred to later phases.
Back link: [Q-021](COMPREHENSION-Q.md#q-021)

## Topic 7: Networking and Productionization

<a id="a-022"></a>
### A-022
A centralized port map prevents drift, clarifies trust boundaries, and supports secure, predictable deployment as services and cluster features are added.
Back link: [Q-022](COMPREHENSION-Q.md#q-022)

<a id="a-023"></a>
### A-023
Externally exposed: client TCP (6379), gRPC API (7443), Admin UI (8080), Node health (8081), Prometheus (9464), Otel OTLP (4327); Internal: RAFT control plane (2380), Cluster replication (2381).
Back link: [Q-023](COMPREHENSION-Q.md#q-023)

<a id="a-024"></a>
### A-024
mTLS is required for internal control-plane and replication ports to secure node-to-node and cluster communication; public endpoints may have separate policies.
Back link: [Q-024](COMPREHENSION-Q.md#q-024)

## Topic 8: Multi-node Readiness and Next Steps

<a id="a-025"></a>
### A-025
Before Phase 2, the system must show stable semantics, repeatable benchmark evidence, and sufficient observability for diagnosing regressions.
Back link: [Q-025](COMPREHENSION-Q.md#q-025)

<a id="a-026"></a>
### A-026
Phase 3 prerequisites: range/replication abstraction, failover semantics, health and replication metrics, and test coverage for node loss and consistency.
Back link: [Q-026](COMPREHENSION-Q.md#q-026)

<a id="a-027"></a>
### A-027
Reviewers should check for: design intent alignment, keyspace and transaction impact, observability and security boundary changes, and migration/rollback strategy.
Back link: [Q-027](COMPREHENSION-Q.md#q-027)

## Topic 9: Query Pipeline and EXPLAIN

<a id="a-032"></a>
### A-032
Parse: input is query text/params, output is typed AST and clause metadata.  
Semantic validation: input is AST + catalog + params, output is semantic model (scopes, symbols, typed expressions, projection/pagination intent).  
Logical planning: input is semantic model + stats/indexes, output is logical operator graph (scan/match/expand/filter/project/aggregate/sort/limit/update).  
Physical execution: input is logical plan + runtime context, output is result stream/set and diagnostics; must not reinterpret raw text for semantics.
Back link: [Q-032](COMPREHENSION-Q.md#q-032)

<a id="a-033"></a>
### A-033
EXPLAIN runs parse, semantic validation, logical and physical planning, but does not execute or mutate state. Output includes logical/physical plan nodes, plan-influencing details (cardinality, index selection, cost), per-node cardinality quality, and warnings for missing stats or fallbacks.
Back link: [Q-033](COMPREHENSION-Q.md#q-033)

<a id="a-034"></a>
### A-034
EXPLAIN executes all planning stages but never applies writes or mutates graph state; integration tests and dry-run guards ensure this for all query shapes, including write-shaped statements.
Back link: [Q-034](COMPREHENSION-Q.md#q-034)

<a id="a-035"></a>
### A-035
EXPLAIN surfaces operator-shaped plan nodes, access-path annotations, index decisions (recommendation, impact, selectivity), query-level cost estimates, runtime planning counters, and warning diagnostics for fallback or missing stats.
Back link: [Q-035](COMPREHENSION-Q.md#q-035)

<a id="a-036"></a>
### A-036
Cardinality values in EXPLAIN are classified as `exact` (from maintained stats), `estimate` (heuristics), or `sample` (probed), with sampling context when available; EXPLAIN output marks each accordingly.
Back link: [Q-036](COMPREHENSION-Q.md#q-036)

## Topic 10: Edge Property Index Pushdown

<a id="a-037"></a>
### A-037
Edge property index pushdown applies equality or numeric range predicates to edge property indexes for relationship traversal; fallback occurs for unsupported predicates, and residual filtering always runs to ensure correctness.
Back link: [Q-037](COMPREHENSION-Q.md#q-037)

<a id="a-038"></a>
### A-038
Adaptive pushdown for stage2 recommendations uses predicate-shape gating, bounded probe caps, selectivity thresholds, and falls back to adjacency expansion if any guardrail is tripped; correctness is invariant.
Back link: [Q-038](COMPREHENSION-Q.md#q-038)

<a id="a-039"></a>
### A-039
Index pushdown observability is achieved via runtime counters (per-query diagnostics and Prometheus metrics) for index usage, candidate counts, fallback reasons, and selectivity, all surfaced in EXPLAIN and operational dashboards.
Back link: [Q-039](COMPREHENSION-Q.md#q-039)

## Topic 11: Programmatic Interface and CLI

<a id="a-040"></a>
### A-040
gRPC/protobuf interface principles: support raw Cypher and prepared queries, enable client-side parse/completeness checks while keeping parameter binding on the server, keep server as semantic/planning authority, and provide explicit version negotiation and fallback.
Back link: [Q-040](COMPREHENSION-Q.md#q-040)

<a id="a-041"></a>
### A-041
Clients send parameterized Cypher or prepared payloads with typed parameters, and the server applies/binds them during query handling; this preserves type correctness without requiring client-side string interpolation.
Back link: [Q-041](COMPREHENSION-Q.md#q-041)

<a id="a-042"></a>
### A-042
CLI uses gRPC/protobuf transport, determines statement completeness locally (parser checks), and only sends complete statements; incomplete statements remain buffered until ready.
Back link: [Q-042](COMPREHENSION-Q.md#q-042)

## Optional Add-on Prompts

<a id="a-028"></a>
### A-028
Re-validate key prefixing, scan/iterator behavior, transaction and durability semantics, and benchmark targets if the storage backend changes.
Back link: [Q-028](COMPREHENSION-Q.md#q-028)

<a id="a-029"></a>
### A-029
Require catalog migration semantics, index build lifecycle controls, safe rollout/rollback, planner explainability, and resource guardrails before enabling Cypher index DDL.
Back link: [Q-029](COMPREHENSION-Q.md#q-029)

<a id="a-030"></a>
### A-030
Test node loss, split-brain prevention, leader change, read consistency under failover, and recovery/catch-up correctness as first distributed failure scenarios.
Back link: [Q-030](COMPREHENSION-Q.md#q-030)

<a id="a-031"></a>
### A-031
Build an edge-first property graph engine with deterministic semantics and a clear path to distributed and partitioned operation.
Back link: [Q-031](COMPREHENSION-Q.md#q-031)

---
