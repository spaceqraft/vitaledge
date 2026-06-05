# VitalEdge Graph Pipeline Proposal

## Purpose

This document proposes a redesign of the VitalEdge query engine into a graph-native pipeline. The current executor has shown that targeted fast paths can rescue important workloads, but the benchmark comparison against Neo4j shows that the broader design still behaves like a row-oriented interpreter with specialized shortcuts layered on top. That is the wrong long-term shape for a property graph engine.

The goal of this redesign is not to preserve every current executor tactic. The goal is to preserve correctness, Cypher compliance, and the performance lessons that generalize, while replacing the execution model with one that is structurally aligned with graph traversal workloads.

VitalEdge is not public-facing at this stage. That changes the redesign posture materially:

1. We can be aggressive with internal changes.
2. We can use git branches to experiment without preserving short-term compatibility for external consumers.
3. We do not need to protect production datasets as a constraint on the redesign.
4. Development and testing can rely on scripted data seeding and repeatable local fixtures.
5. Internal APIs, package boundaries, and executor shapes may be changed freely when that improves the graph pipeline.

## Why Redesign Now

VitalEdge has reached a point where the remaining performance gap is primarily architectural.

The friend suggestion benchmark and the movie recommendation benchmark are useful because they expose the same underlying issue from different angles:

1. Traversal-heavy queries still expand through a general row pipeline.
2. Distinct, collect, order, and merge logic still execute in generic in-memory Go paths.
3. Existing optimizations are shape-specific rescue paths rather than first-class operators.
4. Readback queries can still scale with graph size and output cardinality in ways that are materially worse than Neo4j.

This redesign should therefore target the server itself, not the client, transport, or benchmark harness.

## Design Principles

1. Keep Cypher compliance as a hard constraint.
2. Preserve the current parser and semantic validator as the source of truth for language meaning.
3. Move execution toward a structured operator pipeline rather than raw-row interpretation.
4. Treat graph traversals, anti-joins, aggregations, and writes as first-class physical operators.
5. Keep storage and execution decoupled, but make their contract explicit and rich enough to avoid text recovery in later stages.
6. Reuse existing optimization ideas only when they generalize beyond a single benchmark shape.

## What Should Survive

Some current work should be carried forward, but only as reusable ideas rather than as the final engine shape.

Keep:

1. The Cypher parser and semantic validation pipeline.
2. The persisted statistics model and EXPLAIN visibility.
3. The edge-property index catalog and backfill mechanism.
4. The runtime counter and metrics instrumentation model.
5. The benchmark corpus, especially the recommendation, friend suggestion, and compliance workloads.

Do not keep as the primary execution architecture:

1. Raw-text recovery for projection, ordering, and write semantics.
2. Generic row materialization as the default mode for graph traversal.
3. Executor-local regex heuristics as a core representation for patterns.
4. Special-case benchmark fast paths that cannot be expressed as reusable operators.

## Target Architecture

The redesigned engine should follow a strict pipeline:

1. Parse.
2. Semantic validation.
3. Logical planning.
4. Physical planning.
5. Physical execution.
6. Result shaping.

Each stage should receive structured input and emit structured output. Later stages should not have to reinterpret Cypher text to rediscover meaning.

### Parse Stage

The parser should remain focused on syntax and typed AST construction.

Required output:

1. Typed AST.
2. Source locations.
3. Clause ordering metadata.
4. Projection and pattern structure.
5. Parameter references.

The parser should not perform execution-time rewrites.

### Semantic Stage

Semantic validation should produce a query model that is richer than the AST but still language-level, not storage-level.

Required semantic artifacts:

1. Scoped symbols.
2. Bound vertex, edge, path, and value roles.
3. Projection intent.
4. Ordering intent.
5. Pagination intent.
6. Write-action intent.
7. Pattern intent for MATCH, OPTIONAL MATCH, and MERGE.

This stage should be where unsupported constructs, invalid scope, and invalid aggregation or projection combinations are rejected.

### Logical Planning Stage

Logical planning should translate semantic intent into an operator graph.

Primary logical operators:

1. Vertex scan.
2. Edge scan.
3. Label or type filter.
4. Property filter.
5. Directed expand.
6. Undirected expand.
7. Variable-length expand.
8. Anti-join / existence check.
9. Projection.
10. Aggregate.
11. Distinct.
12. Sort.
13. Limit / skip.
14. Create / merge / set / remove / delete.
15. Call / procedure execution.

The key shift is that common graph shapes should become explicit operators rather than executor-internal control flow.

### Physical Planning Stage

Physical planning should select access paths and operator variants.

Examples:

1. Vertex scan vs label scan vs index-backed lookup.
2. Edge adjacency scan vs index-backed edge candidate retrieval.
3. Streaming aggregate vs hash aggregate.
4. Nested-loop anti-join vs prebuilt endpoint probe vs batch existence check.
5. Full sort vs top-k sort.
6. Eager materialization vs deferred hydration.

This is the right place to incorporate persisted stats, edge-property indexes, and bounded selectivity probes.

### Physical Execution Stage

Physical execution should consume operator trees, not raw Cypher text.

Properties of the executor:

1. Operators should stream rows or bindings where possible.
2. Vertex and edge hydration should happen only when needed by downstream operators.
3. Existence checks should use dedicated storage primitives.
4. Aggregation should be performed by group-aware operators rather than projection-side bookkeeping.
5. Merge should be a write operator with explicit existence-check and create phases.

## Architectural Smells To Remove

The redesign should explicitly eliminate these patterns as primary mechanisms:

1. Executor-local reconstruction of projection meaning from raw clause text.
2. Large generic row maps as the default interchange format between all stages.
3. Query-shape detection implemented as a long chain of pattern heuristics.
4. Query-specific rescue paths that are hard to reuse outside one benchmark.
5. Projection-side aggregation that performs all grouping, distinct handling, and sort state management in one monolithic function.

These patterns are useful as implementation evidence of what the engine currently needs, but they should not be the final architecture.

## Compatibility Constraints

Cypher compliance remains a release gate, not a side project.

The redesign must preserve:

1. The implemented Cypher surface.
2. TCK-compatible semantics for supported queries.
3. Existing error classification behavior where practical.
4. Stable EXPLAIN output for supported forms.
5. Metrics and runtime counter visibility.

Compliance should shape the design in two ways:

1. Supported Cypher forms should map to explicit logical operators.
2. The compliance corpus should become a design input, not just a regression suite.

That means the engine must support more than the recommendation workload. The broader Cypher corpus should constrain operator design, scope handling, projection semantics, aggregation, optional pattern behavior, and write semantics.

## Common Use Cases To Optimize

Some benchmark-derived optimizations should survive because they represent common graph workload families.

Prioritize reusable support for:

1. Two-hop recommendation-style traversals.
2. Friend suggestion / anti-join / not-connected queries.
3. Aggregation over neighborhood results.
4. Top-k ranked graph queries.
5. Selective edge-property lookups.
6. MERGE-heavy ingest and deduplication.

These are common use cases, but they should be expressed as general operator capabilities, not benchmark-specific code paths.

## Proposal: Pipeline Components

### 1. Query Model Layer

Introduce a query model that sits between semantic validation and logical planning.

This model should represent:

1. Bound symbols and scopes.
2. Pattern graphs.
3. Predicate placement.
4. Projection expressions.
5. Grouping keys.
6. Write actions.
7. Plan-affecting hints inferred from stats and indexes.

### 2. Logical Operator Graph

Represent every supported query as a graph of operators.

Examples:

1. MATCH + WHERE + RETURN becomes scan -> expand -> filter -> project -> aggregate -> sort -> limit.
2. Friend suggestion becomes scan/expand -> anti-join -> dedup -> merge.
3. Count queries become scan -> count or edge count operator, not full row materialization.
4. Label histograms become grouped scan -> aggregate.

### 3. Physical Operator Library

Build a reusable set of physical operators with narrow responsibilities.

Examples:

1. Adjacency expand.
2. Indexed candidate lookup.
3. Endpoint existence probe.
4. Hash distinct.
5. Hash aggregate.
6. Streaming top-k.
7. Deferred hydration.
8. Merge existence-check and create.

### 4. Storage Contract

Keep Pebble as the single-node storage backend for now, but make the storage contract more explicit.

Required primitives:

1. Vertex lookup by ID and label.
2. Edge lookup by endpoint and type.
3. Adjacency scans.
4. Typed existence probes.
5. Property-index scans.
6. Statistics reads.

The executor should not have to infer whether a storage primitive exists by walking raw keys or replaying query intent.

## Implementation Strategy

The redesign should happen in phases.

The phases are intended as an execution plan, not a compatibility promise. Because the project is still internal, each phase may make disruptive refactors if they clarify the pipeline, remove dead structure, or unlock a better physical operator model.

### Phase A: Model Extraction

1. Introduce the semantic query model.
2. Move projection, ordering, pagination, and pattern intent into structured handoff types.
3. Reduce executor dependence on raw clause parsing.

### Phase B: Logical Planning

1. Build a logical operator graph for supported Cypher forms.
2. Encode pattern traversal, aggregation, distinct, and write semantics as operators.
3. Use the compliance corpus as the primary validation set.

### Phase C: Physical Operators

1. Implement streaming traversal and join operators.
2. Split aggregation and distinct from projection bookkeeping.
3. Add reusable top-k, hydration, and existence-check operators.

### Phase D: Query Families

1. Re-express the recommendation and friend suggestion workloads using the new pipeline.
2. Retain only the optimizations that can be expressed as reusable operators.
3. Remove benchmark-specific executor branches once equivalent general operators exist.

### Phase E: EXPLAIN and Metrics

1. Map logical and physical operators into EXPLAIN.
2. Preserve runtime counter transparency.
3. Ensure cost and cardinality estimates remain visible and sample-aware.

## What Success Looks Like

The redesign is successful when the following are true:

1. Cypher compliance remains stable or improves.
2. Recommendation and friend suggestion benchmarks improve without relying on benchmark-specific code paths.
3. Readback queries no longer degrade through generic row materialization and in-memory sort/collect bottlenecks.
4. EXPLAIN can show the real operator shape and access path choices.
5. The engine can support both common benchmark workloads and the broader Cypher compliance corpus without separate ad hoc execution branches.

## Non-Goals

This proposal does not attempt to:

1. Introduce distributed execution yet.
2. Redesign the wire protocol.
3. Replace Pebble in the near term.
4. Optimize for a single benchmark at the expense of general Cypher support.
5. Preserve executor-local shape hacks that do not generalize.

## Transition Decisions

These are not open questions for the redesign. They are constraints on how we proceed.

1. The current executor does not need to be retained as a compatibility adapter.
2. The current logical planner can be scrapped and replaced with whatever structure best serves the new pipeline.
3. There is no requirement to preserve compatibility with the current execution internals.
4. Benchmark-specific fast paths are reference material only and should be discarded unless they reappear as general operators.

## Implementation Order

The redesign must cover both read and write paths, because Cypher compliance requires both. The order should be driven by operator dependency, not by a desire to preserve the current executor.

Recommended sequence:

1. Core pattern traversal and binding operators.
2. Existence checks and anti-join operators.
3. Projection, distinct, aggregate, sort, and limit operators.
4. Write operators for create, merge, set, remove, and delete.
5. Physical access-path selection and storage-primitives wiring.
6. EXPLAIN and runtime-counter mapping for the new operator graph.

Read and write work should be designed together, but traversal, projection, and existence checks are the first structural foundation because both compliance and benchmark workloads depend on them.

## First Pass Package And File Plan

This section defines an initial implementation cut for the redesign. It is intentionally concrete so work can begin in parallel branches.

### Package Layout

Use these packages under internal/cypher:

1. pipeline
	- Owns stage contracts and shared pipeline-level types.
2. semantic
	- Owns semantic model construction from parser AST.
3. logical
	- Owns logical operator graph construction.
4. physical
	- Owns physical planning and access-path selection.
5. runtime
	- Owns physical operator execution.
6. runtime/operators
	- Owns reusable physical operators.
7. runtime/storage
	- Owns adapter from runtime operators to graph.Tx primitives.
8. explain
	- Owns explain generation from logical and physical plans.

Keep existing parser, ast, indexschema, and graph packages. The goal is to replace execution flow, not parser ownership.

### File Skeletons

Initial files to create for the first cut:

1. internal/cypher/pipeline/contracts.go
	- Extend existing contracts, do not delete this file.
2. internal/cypher/pipeline/errors.go
3. internal/cypher/semantic/model.go
4. internal/cypher/semantic/builder.go
5. internal/cypher/logical/plan.go
6. internal/cypher/logical/builder.go
7. internal/cypher/physical/plan.go
8. internal/cypher/physical/builder.go
9. internal/cypher/runtime/engine.go
10. internal/cypher/runtime/context.go
11. internal/cypher/runtime/operators/traverse.go
12. internal/cypher/runtime/operators/filter.go
13. internal/cypher/runtime/operators/project.go
14. internal/cypher/runtime/operators/aggregate.go
15. internal/cypher/runtime/operators/distinct.go
16. internal/cypher/runtime/operators/sort_limit.go
17. internal/cypher/runtime/operators/antijoin.go
18. internal/cypher/runtime/operators/write.go
19. internal/cypher/runtime/storage/tx_adapter.go
20. internal/cypher/explain/pipeline_explain.go
21. (removed in Slice 6) temporary compatibility router scaffolding

### Execution Slices

Implement in these slices, each independently branchable and mergeable.

Slice 1: Pipeline Contracts And Semantic Model

1. Expand pipeline contracts to represent pattern, projection, pagination, and write intent without raw text recovery.
2. Build semantic model output from parser AST.
3. Add guardrail tests for semantic model coverage over representative compliance shapes.

Slice 2: Logical Plan Builder

1. Build logical operators for MATCH, OPTIONAL MATCH, WHERE, WITH, RETURN, and pagination.
2. Keep writes represented in plan nodes even before runtime wiring.
3. Add deterministic logical-plan snapshot tests.

Slice 3: Runtime Read Operators

1. Implement traverse, filter, project, aggregate, distinct, sort, and limit operators.
2. Route read query families through runtime engine.
3. Validate against compliance read scenarios and benchmark read paths.

Slice 4: Runtime Write Operators

1. Implement create, merge, set, remove, and delete operators.
2. Implement existence-check and anti-join operators used by merge and suggestion workloads.
3. Validate against compliance write scenarios.

Slice 5: Physical Planner And Explain

1. Add physical operator selection and access-path decisions.
2. Wire explain output to logical and physical plans.
3. Preserve runtime counter visibility in the new engine.

Slice 6: Migration Cleanup

1. Remove migrated paths from executor-centric flow.
2. Remove benchmark-specific fast paths that are now covered by general operators.
3. Reduce compatibility/router usage to zero for migrated query families.

Current status (2026-06-05):

1. Executor hot-path query and EXPLAIN route decisions are served by executor-local routing helpers.
2. Temporary compatibility router scaffolding has been removed.
3. Stage-1 shared-seed expansion baseline-disable scaffold has been removed; multi-target stage-1 recommendation execution now consistently uses shared-seed expansion.
4. Stage-2 late-materialization baseline-disable scaffold has been removed; stage-2 recommendation execution now consistently uses late materialization for non-aggregate projection fields.
5. Stage-2 top-k baseline-disable scaffold has been removed; stage-2 recommendation execution now consistently applies top-k pushdown when query shape qualifies.
6. Stage-1 top-k baseline-disable scaffold has been removed; stage-1 recommendation execution now applies top-k pushdown based on query shape and adaptive feedback only.
7. Stage-2 edge-index pushdown baseline-disable scaffold has been removed; stage-2 recommendation execution now applies index pushdown based on query shape and selectivity only.

Trust-and-verify confirmation snapshot (2026-06-05):

1. Routing and runtime pipeline contract checks passed:
	- go test ./internal/cypher/executor -count=1 -run 'TestDecideQueryRoute|TestDecideExplainRoute|TestRuntimePipeline'
2. Recommendation, EXPLAIN guardrail, and pipeline baseline checks passed:
	- go test ./internal/cypher/executor -count=1 -run 'TestRecommendation|TestExplainTestsDeclareExplicitPayloadRoute|TestQueryPipelineBaselineSupportedShapes'
3. Full executor package passed:
	- go test ./internal/cypher/executor -count=1
4. Full cypher package suite passed, including compliance:
	- go test ./internal/cypher/... -count=1

Exit condition evidence status:

1. Primary query execution routes through semantic -> logical -> physical -> runtime for supported runtime shapes: verified by runtime routing and runtime pipeline tests.
2. Legacy executor flow remains available for unsupported shapes, but migrated recommendation and EXPLAIN hot paths are pipeline-routed with executor-local route decisions: verified by routing, recommendation, and EXPLAIN guardrail tests.
3. Benchmark-specific scaffolding removals completed for Slice 6 cut list in this document (shared-seed, late-materialization, stage-2 top-k, stage-1 top-k, and stage-2 edge-index disable toggles): verified by green executor and cypher suites.
4. Explain and runtime counter visibility remains intact on pipeline payload path: verified by PipelinePayload EXPLAIN guardrail checks and recommendation pipeline payload tests.

### Branch Strategy

Use short-lived feature branches per slice:

1. graph-pipeline-s1-semantic
2. graph-pipeline-s2-logical
3. graph-pipeline-s3-runtime-read
4. graph-pipeline-s4-runtime-write
5. graph-pipeline-s5-physical-explain
6. graph-pipeline-s6-cleanup

Branch sequencing is strict by dependency, but branch development may overlap when interfaces are stable.

### Test Gates Per Slice

Each slice should pass all of:

1. go test ./internal/cypher/parser/...
2. go test ./internal/cypher/semantic/... when introduced
3. go test ./internal/cypher/logical/... when introduced
4. go test ./internal/cypher/runtime/... when introduced
5. go test ./internal/cypher/compliance/...
6. Focused recommendation and friend suggestion benchmark checks

No slice is complete until compliance and benchmark guardrails are both green.

### Exit Condition For Full Migration

The redesign migration is complete when:

1. Primary query execution routes through semantic -> logical -> physical -> runtime.
2. Legacy executor flow is no longer required for supported query families.
3. Benchmark-specific fast paths are removed or reduced to reusable operator implementations.
4. Explain and runtime counters are produced by the new pipeline path.

### Final Signoff (Trust And Verify)

Automated verification status (2026-06-05):

1. PASS: routing and runtime pipeline contract tests.
2. PASS: recommendation + EXPLAIN guardrail + pipeline baseline tests.
3. PASS: full executor package tests.
4. PASS: full cypher package suite, including compliance.

Manual verification checklist (operator signoff):

1. Confirm representative supported query families route and execute as expected in local interactive runs.
2. Confirm EXPLAIN output for migrated recommendation and friend-suggestion shapes reports expected pipeline payload metadata.
3. Confirm runtime counter visibility and naming remain stable in manual query sessions.
4. Run project Python benchmark scripts and compare outcomes against latest baseline snapshots before declaring final closure.

Closure rule:

1. This proposal is considered complete when all automated checks above are PASS and manual verification plus Python benchmark validation are marked complete.

## Recommendation

Proceed with the redesign.

Use the compliance corpus to define the language contract, use the recommendation and friend-suggestion workloads to define common graph-performance requirements, and replace the current row-engine-plus-shortcuts model with a graph-native query pipeline.