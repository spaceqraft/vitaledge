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

MVP envelope requirements (explicit minimum):

- Single-node operation.
- Cypher-compliant behavior for the implemented surface (TCK-validated).
- CLI support for setting variables, submitting Cypher, rendering tabular result sets, and showing output statistics.
- Comparative benchmark evidence versus Neo4j and TigerGraph.
- Metrics pipeline via Prometheus with Grafana dashboards.
- Manual index tuning workflow: EXPLAIN, planner statistics, and query cost estimation.

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
- Implemented: broad built-in function coverage across string, math, list, predicate/scalar, temporal, spatial, and vector families.
- Implemented: EXISTS subquery support in WHERE for supported MATCH subquery bodies.
- Implemented: persisted statistics-as-data side effects for tenant totals, per-label vertex counts, and per-edge-type counts.
- Implemented: stats snapshot read API consumed by EXPLAIN and statistics procedures, including EXPLAIN diagnostics for snapshot coverage completeness and backfill-required status.
- Implemented: startup schema migration (`no-stats` -> `stats`) that backfills persisted statistics for legacy stores and records schema version metadata.

Phase 1 deliverable status:

- Working single-node graph engine package: complete.
- Integration tests covering mixed read/update query flows: complete for current supported clause surface.
- Basic index DDL and index utilization in planner: complete via configuration-based index declarations and runtime index catalog consumption.

Remaining Phase 1 gaps before close:

- Phase 1 performance evidence has now been captured in this document (see Phase 1 closure evidence below).
- Operability baseline for Phase 1 is satisfied via in-process metrics collectors and periodic metrics/recommendation reporting; production sink/export wiring remains a Phase 2+ hardening task.
- Planner/explainability backlog (Phase 2): explain-plan output coverage is still listed as a Phase 2 deliverable and is not yet evidenced for all supported query forms.

MVP-critical items not yet complete:

- External workstream: phased SDK client repositories (Python, Go, Java, C#) on the gRPC/protobuf contract.
- External workstream: comparative benchmark repositories plus published blog post (methodology + VitalEdge/Neo4j/TigerGraph results).

MVP-critical items completed in this repository:

- CLI polish for first-class variable management, table rendering ergonomics, graph/path rendering, and explicit output statistics UX.
- Prometheus scrape endpoint and baseline Grafana dashboard set wired as supported operational path, including host hardware and Go runtime/GC metrics.
- Manual index tuning loop completed end-to-end: EXPLAIN output, planner/runtime statistics, and query cost estimation surfaced for operator decisions.

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

Additional closure evidence captured after the benchmark checkpoint:

- Full openCypher TCK compliance achieved: `3897/3897 scenarios`, `16006/16006 steps`.
- This milestone should be interpreted as semantic-compatibility maturity for the currently supported Cypher surface, not just clause-count completion.

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

### Phase 1 architectural learnings from TCK completion (captured 2026-05-26)

1. Grammar coverage was necessary but not sufficient.
   - Most of the remaining work to reach compliance lived in semantic validation, error classification, null behavior, projection semantics, and scope propagation.
2. Parser/executor boundaries need stronger contracts.
   - Several fixes depended on normalized raw-clause recovery in executor paths for ORDER BY, SKIP/LIMIT, DISTINCT, MERGE actions, and pattern forms.
   - This was an effective tactical bridge, but it is now a clear architectural debt marker.
3. Pattern and edge handling should become more structurally represented.
   - Edge semantics are central graph execution structure and should not remain primarily encoded as executor-local regex heuristics.
4. The next maturity step is representation fidelity across phases.
   - The engine should preserve more intent through parse -> semantic validation -> logical plan -> physical execution -> result normalization, rather than reconstructing intent from text in later stages.
5. Phase 1 is therefore complete in a stronger sense than originally framed.
   - Single-node correctness now includes standards-credible Cypher behavior for the implemented language surface.

## Phase 2: Hardening and Optimization (Single-node)

Objective: make Phase 1 robust, explainable, and less text-driven before cluster complexity.

### Phase 2 Focus: Query Pipeline

Primary focus for this phase is a strict pipeline for all supported query shapes:

1. Parse
2. Semantic validation
3. Logical planning
4. Physical execution

The implementation intent is to preserve semantics and scope as structured artifacts between stages so the executor does not need to recover intent from normalized raw text.

#### Stage contracts

1. Parse stage contract
   - Input: query text + parameters.
   - Output: typed AST + source locations + normalized clause ordering metadata.
   - Must not perform execution-time rewrites.
2. Semantic validation stage contract
   - Input: AST + schema/index catalog view + parameter values.
   - Output: semantic query model (scopes, symbol table, typed expressions, clause constraints, projection/pagination intent).
   - Responsible for user-facing semantic errors and category mapping.
3. Logical planning stage contract
   - Input: semantic query model + planner statistics/index metadata.
   - Output: deterministic logical operator graph (scan/match/expand/filter/project/aggregate/sort/limit/update).
   - Responsible for index candidate selection and explain-visible operator shapes.
4. Physical execution stage contract
   - Input: logical plan + runtime context (tenant, transaction, params).
   - Output: result stream/set + execution stats + diagnostics hooks.
   - Must not reinterpret clauses from raw text to recover core semantics.

#### First refactor targets (in order)

1. ORDER BY, SKIP, LIMIT
2. DISTINCT and projection forms
3. Pattern/edge clause structure used by MATCH/OPTIONAL MATCH
4. MERGE action blocks and write-action sequencing

#### Query Pipeline exit evidence

1. Explain output parity: all supported query forms produce stable explain plans.
2. Regression guardrail: parser/executor tests proving no regex/raw-text reconstruction for core semantics in supported forms.
3. Error-path consistency: semantic errors are produced in semantic stage for representative invalid queries.
4. Performance non-regression: no material regressions against Phase 1 benchmark baselines.

#### EXPLAIN contract for Phase 2

Objective: deliver a stable dry-run planning interface for manual index tuning and planner diagnostics.

Execution behavior:

1. EXPLAIN runs parse, semantic validation, logical planning, and physical planning.
2. EXPLAIN does not execute the physical plan for result production.
3. EXPLAIN does not apply writes or mutate persisted graph state.

Required output:

1. Logical and physical plan nodes.
2. Plan-influencing details used by planning:
   - vertex/edge cardinality facts relevant to query predicates and expansions,
   - index candidates and chosen access path rationale,
   - predicate cardinality signals.
3. Cardinality entries with quality classification: `exact`, `estimate`, `sample`.
4. Warnings and fallback diagnostics.
5. Statistics snapshot diagnostics exposing data readiness:
   - `coverage` by stats family,
   - `completeness` classification,
   - `backfillStatus` and `backfillRequired` flags.

Schema and transport policy:

1. Canonical short-term EXPLAIN output schema is JSON, defined in [EXPLAIN_OUTPUT_SCHEMA.json](EXPLAIN_OUTPUT_SCHEMA.json).
2. gRPC/protobuf remains the target external transport; JSON output is retained in this phase to accelerate contract iteration.
3. Protobuf mapping must preserve field semantics and cardinality quality annotations.

Exit evidence additions for EXPLAIN:

1. Integration tests proving EXPLAIN is read-only for write-shaped queries.
2. Golden-output tests for representative query families (match/filter/projection/order/pagination/write-shaped dry run).
3. Regression tests proving stable emission of plan-influencing details.

Implementation status for current EXPLAIN slice set:

1. Query-options completeness: completed (`query.options` and per-clause projection options are emitted).
2. Operator-shape fidelity: completed (plan nodes emit explicit scan/filter/project/sort/skip/limit/write/call operators).
3. Index-tuning signals: completed (index decisions include recommendation, impact, selectivity, and access-path context).
4. Warning/fallback diagnostics: completed (specific warning codes emitted for full scans, missing indexes, estimate-only signals, missing tenant context, and write dry-run behavior).
5. Documentation: completed (README and DESIGN include concrete EXPLAIN interpretation guidance and warning semantics).
6. Statistics transparency: completed for current persisted envelope (tenant totals + per-label + per-edge-type) and exposed through EXPLAIN influencer totals/counts.

#### Query Pipeline slice-by-slice execution plan (next)

Execution style mirrors the completed EXPLAIN track: one narrow slice at a time, each ending with focused tests, docs/schema deltas (if any), and a green regression gate before moving on.

Current status:

1. QP-0 baseline and guardrails: completed.
2. QP-1 ORDER BY, SKIP, LIMIT migration: completed.
3. QP-2 DISTINCT and projection forms migration: completed.
4. QP-3 MATCH/OPTIONAL MATCH pattern and edge-structure migration: completed.
5. QP-4 MERGE action blocks and write-action sequencing migration: completed.
6. QP-5 cleanup, hardening, and parity closeout: completed.

Slice QP-0: baseline and guardrails

1. Define and pin the pipeline handoff contracts in code comments/types where needed (parse output, semantic model, logical plan, physical execution input).
2. Add/refresh focused regression tests for currently supported query shapes so behavior is frozen before refactors.
3. Add a guardrail checklist in tests/review notes for "no raw-text semantic recovery" in migrated paths.

Exit gate:

1. Focused parser/executor/TCP tests are green.
2. Existing EXPLAIN tests remain green.

Slice QP-1: ORDER BY, SKIP, LIMIT migration

1. Promote ordering/pagination intent into structured stage artifacts consumed by planner/executor.
2. Remove or bypass regex/raw-clause recovery for these forms in migrated paths.
3. Keep EXPLAIN operator output aligned (`SORT`, `SKIP`, `LIMIT`) with the new pipeline artifacts.

Exit gate:

1. Existing ORDER/SKIP/LIMIT tests pass with no behavior regressions.
2. New tests assert stage-artifact usage (not raw text) for these clauses.

Slice QP-2: DISTINCT and projection forms

1. Move projection item shaping, DISTINCT behavior, and alias handling fully through semantic model + logical plan.
2. Ensure planner/executor consume structured projection intent only.
3. Keep EXPLAIN projection/query-options output stable while sourcing from migrated artifacts.

Exit gate:

1. Projection/DISTINCT correctness tests pass for MATCH and WITH/RETURN forms.
2. EXPLAIN query-options and plan-node projections remain stable in tests.

Slice QP-3: MATCH and OPTIONAL MATCH pattern/edge structure

1. Promote pattern/edge structure used for scans/expands/filters into planner-facing artifacts.
2. Reduce executor-local pattern regex recovery in migrated query shapes.
3. Keep index candidate/access-path behavior stable or improve with explicit planner inputs.

Exit gate:

1. MATCH/OPTIONAL MATCH coverage passes for supported pattern forms.
2. EXPLAIN operator/access-path/index-decision outputs remain stable.

Slice QP-4: MERGE action blocks and write-action sequencing

1. Encode MERGE action blocks (`ON MATCH` / `ON CREATE`) and write sequencing as structured execution inputs.
2. Remove raw-text fallback for migrated MERGE sequencing paths.
3. Preserve dry-run behavior under EXPLAIN for write-shaped statements.

Exit gate:

1. MERGE sequencing and mutation correctness tests pass.
2. EXPLAIN write-dry-run and warning diagnostics remain correct.

Slice QP-5: cleanup, hardening, and parity closeout

1. Remove replaced regex/raw-text paths once migrated parity is proven.
2. Add any missing semantic-stage error-path assertions for representative invalid queries.
3. Run benchmark non-regression checks against Phase 1 baselines for migrated query families.

Exit gate:

1. Query Pipeline exit evidence list is satisfied.
2. No supported migrated form requires executor-side raw-text semantic recovery.

Per-slice execution checklist:

1. Implement smallest vertical change.
2. Add or tighten focused unit/integration tests.
3. Run focused parser/executor/TCP tests.
4. Update docs/contracts only where changed.
5. Merge only on green tests and stable EXPLAIN outputs.

Milestones:

1. Strengthen query-engine phase boundaries.
   - Promote normalized raw-clause behavior into explicit parser/planner representations for patterns, projections, ORDER BY, SKIP/LIMIT, and write actions.
2. Query planner improvements (predicate pushdown, index-first plans, explainable operator shapes).
3. Compaction and write amplification tuning profiles.
4. Memory and cache strategy (block cache, iterator bounds).
5. Fault-injection and chaos-style local durability tests.

Deliverables:

- Planner explain output for observability.
- Reduced executor dependence on regex/string recovery for currently supported Cypher forms.
- Clear semantic handoff between parser validation, planning, and execution.
- Tuning profiles for ingest-heavy vs read-heavy workloads.
- Long-run soak test suite.

Exit Criteria:

- 24h soak test with no correctness regressions.
- p95 latency and ingest throughput improve by agreed target over Phase 1 baseline.
- Explain output available for all supported query forms.
- Supported query forms no longer require executor-side reconstruction of core clause semantics from normalized raw text.

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

- Semantic-validation outputs that preserve scope, projection, ordering, pagination, and write-action intent.
- Logical plan from AST/semantic artifacts.
- Physical operators for pattern expansion, filtering, projection, updates.

Quality gates:

- Plan determinism tests.
- Explain-plan coverage in integration tests.
- Regression coverage proving supported query forms do not rely on executor regex/raw-text rediscovery for core semantics.

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
4. Architectural drift from tactical regex/raw-clause recovery becoming a permanent execution dependency.

## Recommended Immediate Next Sprint

Theme: MVP closeout sprint (post-Query-Pipeline).

1. Complete the manual index tuning loop end-to-end for supported query shapes:
   - stable EXPLAIN output,
   - planner/runtime statistics surfaced to operators,
   - actionable query cost estimation.
2. Stand up Prometheus/Grafana as the default metrics path and publish dashboard/query examples.
3. Establish gRPC/protobuf as the canonical programmatic interface and finalize CLI UX on top of that interface.
4. Deliver phased language SDK clients (Python, Go, Java, C#) on the same gRPC/protobuf contract.
5. Run and publish benchmark comparisons against Neo4j and TigerGraph using documented workload parity rules.

Sprint deliverables:

1. Manual index tuning evidence:
   - at least three representative query tuning examples (before/after index and observed plan/stat changes),
   - stable explain-plan emission for supported query families,
   - documented operator guidance for interpreting planner/runtime signals.
2. Metrics path readiness:
   - Prometheus scrape endpoint wired and documented,
   - baseline Grafana dashboard set committed,
   - dashboard coverage includes latency, throughput, index usage, and error signals.
3. gRPC/protobuf contract readiness:
   - stable protobuf service/messages for query execution and explain flow,
   - protocol compatibility/version negotiation fields included,
   - structured error model and diagnostics mapping documented.
   - status: complete (typed generated stubs in repo, server wiring in place, prepared-query + capability-gated fallback covered by tests).
4. CLI usability closure:
   - variable management flow (`SET`/list/update/unset) is consistent,
   - tabular rendering behaves predictably for wide/null-heavy results,
   - statement output includes clear execution statistics,
   - CLI sends requests only after local parse-completeness checks.
   - load-generation support exists for soak execution with deterministic modes (`write`, `noop-write`, `read`) and tunable operation/seed/hop/limit/report parameters.
   - status: complete (gRPC CLI implements variable commands, client-side binding, adaptive table width capping, graph/path rendering, stats output, completeness gating before RPC, and deterministic soak-load generation modes).
5. SDK phase-1 delivery:
   - Python and Go clients support raw Cypher and prepared-query request paths,
   - client-side parse-completeness validation for interactive/scripted usage,
   - compatibility fallback from prepared-query to raw Cypher when required by server capability.
   - delivery model: external repositories (not in this repo).
   - status: pending external delivery.
6. Comparative benchmark publication:
   - workload parity methodology documented,
   - reproducible benchmark invocation scripts captured,
   - result summary published in repo docs.
   - delivery model: external benchmark repositories plus external blog post publication.
   - status: pending external publication.

Suggested implementation order for this sprint:

1. Manual index tuning loop and EXPLAIN/runtime stats packaging for operator workflows.
2. Prometheus/Grafana wiring and baseline dashboard publication.
3. gRPC/protobuf service surface and server wiring.
4. CLI UX polish and output-statistics ergonomics on top of gRPC.
5. Phased client SDK rollout (Python, then Go, then Java, then C#).
6. Cross-engine benchmark runs and documentation publication.

Sprint exit criteria:

1. MVP-critical gaps listed in this plan are closed or explicitly tracked with owner/date as follow-on work.
2. Full test sweep remains green after each deliverable set.
3. Documentation is sufficient for an external contributor to reproduce dashboards and benchmark comparisons.

### gRPC / Protobuf Programmatic Interface Draft

Objective: make gRPC/protobuf the canonical programmatic surface for CLI and SDK clients while preserving semantic correctness on the server.

Design principles:

1. Support both raw Cypher requests and prepared-query requests.
2. Allow client-side parse/completeness and parameter binding work to reduce server CPU/RAM.
3. Keep server as semantic/planning source of truth (schema/index/runtime-aware checks).
4. Include explicit version negotiation and compatibility fallback behavior.

Proposed protobuf surface (draft):

```proto
syntax = "proto3";

package vitaledge.v1;

service QueryService {
   rpc Execute(QueryRequest) returns (QueryResponse);
   rpc Explain(QueryRequest) returns (ExplainResponse);
   rpc GetCapabilities(CapabilitiesRequest) returns (CapabilitiesResponse);
}

message QueryRequest {
   string tenant = 1;
   QueryInput input = 2;
   RequestOptions options = 3;
   ClientContext client = 4;
}

message QueryInput {
   oneof kind {
      string cypher = 1; // client-submitted fully bound statement
      PreparedQuery prepared = 2;
   }
}

message PreparedQuery {
   string parser_version = 1;
   string ir_version = 2;
   string fingerprint = 3;
   bytes payload = 4;
}

message RequestOptions {
   bool read_only = 1;
   bool include_stats = 2;
   bool include_warnings = 3;
}

message ClientContext {
   string sdk_language = 1;
   string sdk_version = 2;
   string protocol_version = 3;
}

message QueryResponse {
   repeated string columns = 1;
   repeated Row rows = 2;
   QueryStats stats = 3;
   repeated Diagnostic warnings = 4;
}

message ExplainResponse {
   bytes explain_json = 1;
   QueryStats stats = 2;
   repeated Diagnostic warnings = 3;
}

message QueryStats {
   int64 rows_returned = 1;
   int64 duration_ms = 2;
}

message Diagnostic {
   string code = 1;
   string message = 2;
}

message CapabilitiesRequest {}

message CapabilitiesResponse {
   string protocol_version = 1;
   repeated string parser_versions = 2;
   repeated string ir_versions = 3;
   bool prepared_query_supported = 4;
}

message Row {
   map<string, Value> values = 1;
}

message Value {
   oneof kind {
      bool bool_value = 1;
      int64 int_value = 2;
      double double_value = 3;
      string string_value = 4;
      bytes bytes_value = 5;
      ListValue list_value = 6;
      MapValue map_value = 7;
      NullValue null_value = 8;
   }
}

message ListValue {
   repeated Value values = 1;
}

message MapValue {
   map<string, Value> values = 1;
}

message NullValue {}
```

Server-side behavior rules:

1. Raw `cypher` input: parse -> semantic validation -> planning -> execution.
2. `prepared` input: validate version/fingerprint/shape -> semantic validation -> planning -> execution.
3. On incompatible prepared input, return capability mismatch and support fallback to raw Cypher mode.
4. Server may reject or downgrade prepared payloads if validation fails.

Client-side parameter binding contract:

1. Programmatic clients and CLI bind parameters client-side before RPC submission.
2. Requests sent to gRPC contain the fully bound statement text or prepared payload; there is no separate parameter map in the wire request.
3. Binding should be structured/typed (AST or token-aware literal insertion), not naive string replacement.
4. SDKs must preserve Cypher literal correctness when binding strings, numerics, booleans, nulls, lists, and maps.

### Phased Client SDK Plan (Priority Order)

Objective: deliver consistent programmatic interface across language clients with predictable capability/fallback behavior.

Phase SDK-1: Python client

1. gRPC channel/client scaffolding and auth/tenant request metadata.
2. Raw Cypher execute/explain methods.
3. Client-side parameter binding + parse-completeness check and prepared-query request mode.
4. Capability negotiation + fallback path from prepared-query to raw Cypher.

Exit evidence:

1. Integration tests for execute/explain success and error mapping.
2. Tests for prepared-query fallback on unsupported capability.
3. Example scripts for sync and interactive usage.

Phase SDK-2: Go client

1. Same contract as Python, idiomatic Go API surface.
2. Strong typed helpers for values/rows/stats/diagnostics.
3. Prepared-query, client-side binding, and fallback behavior parity with Python.

Exit evidence:

1. Cross-client parity tests against shared golden server scenarios.
2. Benchmark check for client-side parse overhead vs server parse overhead.

Phase SDK-3: Java client

1. Java API parity with execute/explain/capabilities endpoints.
2. Prepared-query path + fallback parity.
3. Build tooling/publishing baseline for JVM environments.

Exit evidence:

1. Integration tests mirroring Python/Go scenario coverage.
2. Example application snippet for service-side integration.

Phase SDK-4: C# client

1. .NET API parity for execute/explain/capabilities.
2. Prepared-query path + fallback parity.
3. Packaging and sample for standard .NET runtime targets.

Exit evidence:

1. Cross-language contract parity tests pass.
2. End-to-end capability negotiation and fallback scenarios pass.

### CLI dependency and behavior contract

1. CLI transport is gRPC/protobuf, not TCP line framing.
2. CLI determines statement completeness locally using parser checks before issuing RPC.
3. CLI sends only complete statements; incomplete statements remain in local input buffer.
4. CLI and server both show consistent banner/version identity, including: `(v:Vital)ﮩ٨ـﮩﮩ٨ـ[e:Edge]ﮩ٨ـﮩﮩ٨ـ()`.

Back to [DESIGN.md](DESIGN.md) and [README.md](README.md).
