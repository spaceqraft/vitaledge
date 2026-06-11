# VitalEdge Query Pipeline: Full Operator-Native Execution (All Ops)

## Purpose

This proposal defines the next redesign step after GRAPH_PIPELINE: complete migration to a fully operator-native Cypher execution path where the runtime engine executes physical operators directly, without delegating query semantics back to shared executor backends.

This proposal explicitly expands scope to include storage-layer evolution in the Pebble-backed graph store. The query pipeline and storage contract are co-designed so operators can execute graph workloads natively with predictable performance and full Cypher semantics.

## Scope

In scope:

1. Full operator-native read and write execution for supported Cypher surface.
2. Physical operator library coverage for all major clause families.
3. Runtime/operator diagnostics, errors, and counters at operator granularity.
4. Cost/cardinality planning based on persisted statistics.
5. Pebble-store primitive additions needed for operator-native execution.
6. Removal or rewrite of "fast path" and pushdowns.

Out of scope:

1. Distributed execution.
2. Wire protocol redesign.
3. Replacing Pebble as storage engine.

## Problem Statement

Current architecture still has one major remnant: runtime entrypoint can delegate to shared executor logic. That preserves correctness for unsupported shapes but prevents true operator-native semantics, consistent diagnostics, and principled optimization.

To close this gap, we must:

1. Implement full operator surface area (not only traversal stubs).
2. Represent optimization behavior as physical operators/rules instead of executor-local fast paths.
3. Extend Pebble-store APIs to provide direct graph primitives used by those operators.

## Operator-Native Architecture

Pipeline stages:

1. Parse -> typed AST with locations.
2. Semantic model -> scoped symbols, bound roles, clause intent.
3. Logical planning -> algebraic graph operators.
4. Physical planning -> operator variants and access paths.
5. Runtime execution -> operator DAG/trees with streaming bindings.
6. Result shaping -> protocol payload, summary, counters.

Design constraints:

1. No semantic fallback to executor text recovery in migrated paths.
2. Every supported clause maps to explicit logical and physical operators.
3. Runtime only consumes typed plans and storage adapters.
4. EXPLAIN and runtime counters are emitted from the same operator graph that executes.

## Table Of Query Pipeline Operations

The following table defines minimum operator coverage for full operator-native execution.

| Family | Logical op | Physical variants | Inputs | Output bindings | Pebble/store primitives |
|---|---|---|---|---|---|
| Vertex access | VertexScan | FullScan, LabelScan, LabelIndexSeek, IdSeek | label, id, predicates | vertex symbol | ScanVerticesByLabel, GetVertexByID |
| Edge access | EdgeScan | TypeScan, EdgePropIndexSeek, EndpointRangeScan | type, predicates | edge symbol (+ optional endpoints) | ScanEdgesByType, SeekEdgesByProperty |
| Traversal | Expand | OutExpand, InExpand, UndirectedExpand, VarLengthExpand | start vertex + edge/type constraints | new edge/vertex/path bindings | ScanAdjacency, ScanAdjacencyTyped, ScanAdjacencyVarLen |
| Pattern join | PatternJoin | NLJoin, HashJoin, BindJoin | lhs/rhs pattern streams | merged bindings | EndpointProbe, BatchEndpointProbe |
| Anti pattern | AntiJoin/NotExists | NestedLoopAnti, IndexedAntiProbe | lhs stream + pattern | lhs rows that have no match | ExistsEdge, BatchExistsEdge |
| Optional pattern | OptionalExpand | OptionalOut, OptionalIn, OptionalUndirected | lhs stream | lhs with nullable rhs bindings | ScanAdjacencyOptional |
| Filter | Filter | PredicateEval, IndexResidualFilter | binding stream | filtered stream | (none) + optional index residual metadata |
| Unwind | Unwind | ListUnwind, ExprUnwind | list expression per row | expanded rows | (none) |
| Projection | Project | StreamingProject, MaterializedProject | expressions/aliases | projected row | DeferredHydrateVertex, DeferredHydrateEdge |
| Aggregation | Aggregate | HashAggregate, StreamingAggregate, CountStarFast | key + aggregate exprs | grouped rows | FastCountVertices, FastCountEdges |
| Distinct | Distinct | HashDistinct, OrderedDistinct | stream | deduped rows | (none) |
| Sort/TopK | SortLimit | FullSort, TopKHeap, PartialSortMerge | order keys + skip/limit | ordered rows | OptionalIndexOrderSeek |
| Set operations | Union | UnionAll, UnionDistinct | branch streams | merged stream | (none) |
| Write create | Create | CreateVertex, CreateEdge, CreatePath | write intents | updated bindings + write stats | PutVertex, PutEdge, ReserveIDs |
| Write merge | Merge | MergeMatchProbeThenCreate | pattern + on-create/on-match actions | updated bindings + write stats | ExistsPattern, UpsertWithCheck |
| Write update | Set/Remove | SetProperty, RemoveProperty, ReplaceMap, MutateMap | entity bindings + expressions | updated bindings + write stats | UpdateVertexProps, UpdateEdgeProps |
| Delete | Delete | DeleteVertexDetach, DeleteEdge, DeletePathRefs | entity bindings | write stats | DeleteVertex, DeleteEdge, DeleteAdjacencyRefs |
| Procedure | Call | ProcedureInvoke | args stream | procedure rows | (optional) |
| Explain/Profile | Explain/Profile | ExplainBuild, ProfileCollect | logical/physical plan | explain/profile rows | StatsRead |

## Pebble Store Contract Expansion

Operator-native execution requires store-level primitives beyond generic key walking.

### Required read primitives

1. Adjacency scans by direction and edge type:
   - ScanOutAdjacency(vertexID, edgeType?)
   - ScanInAdjacency(vertexID, edgeType?)
2. Endpoint existence probes:
   - ExistsEdge(fromID, toID, edgeType?)
   - BatchExistsEdges(batch)
3. Variable-length expansion support:
   - ExpandFrontier(frontierIDs, hop, edgeType?, direction)
4. Index-backed seeks:
   - SeekVerticesByLabelProperty(label, key, predicate)
   - SeekEdgesByTypeProperty(edgeType, key, predicate)
5. Ordered index iteration (for sort pushdown/top-k where valid):
   - SeekOrdered(index, direction, startKey, limit)

### Required write primitives

1. Atomic MERGE probe-and-create with conflict-safe semantics.
2. Property map mutation APIs (replace/patch/remove) for vertex and edge entities.
3. Detach-delete helpers that remove adjacency and secondary index entries atomically.
4. Batch write API with idempotent retry semantics for runtime write operators.

### Required statistics primitives

1. Cardinalities:
   - vertex_count_total, vertex_count_by_label
   - edge_count_total, edge_count_by_type
2. Degree distributions:
   - out_degree_p50/p95/max by edge type and label pair
3. Selectivity and NDV:
   - index selectivity estimates per (label or type, property)
   - ndv estimates for common grouping keys
4. Histograms:
   - equi-depth histograms for indexed numeric/time properties
5. Freshness metadata:
   - stats_epoch, sample_size, last_refresh_ts

## Query Pipeline Optimizations

All optimizations must be expressed as planner rules plus physical operator variants.

### 1. Empty-set pruning for relationship patterns

For pattern (lhs)-[e]-(rhs), planner can prove emptiness and short-circuit execution before expansion when any required side is empty.

Cases:

1. LHS empty:
   - Example: prior filter yields no bound lhs vertices.
   - Rewrite: Replace downstream expand/join subtree with EmptyRelation.
2. Edge candidate set empty:
   - Example: edge type or edge-property index seek has zero cardinality.
   - Rewrite: Expand -> EmptyRelation.
3. RHS constrained domain empty:
   - Example: rhs label/property seek returns no vertices.
   - Rewrite: join/anti-join subtree collapses to EmptyRelation or passthrough (for OPTIONAL semantics).

Operator behavior:

1. EmptyRelation operator emits zero rows and zero-cost downstream work.
2. Optional patterns convert to NullBindProject (preserve lhs, bind rhs/e/path as null).
3. Rule execution order ensures emptiness checks happen before expensive traversal.

### 2. Early existence/anti-join pruning

1. Use IndexedAntiProbe for NOT (lhs)-[:T]-(rhs) patterns.
2. Convert full expansion + filter-not-exists into direct existence probe operators.
3. Batch probes for common recommendation/friend-suggestion shapes.

### 3. Degree-aware traversal direction

1. For undirected patterns, choose direction with lower expected fanout first.
2. Use degree stats by edge type and label pair.
3. Reorder expand chain to minimize intermediate bindings.

### 4. Top-k pushdown

1. Replace Sort + Limit with TopKHeap when order key available pre-hydration.
2. Push ordered index scans to store when index ordering is compatible.
3. Apply deferred hydration so only top-k rows hydrate full entities.

### 5. Aggregate fast paths

1. CountStarFast for COUNT(*) without grouping and without row-dependent expressions.
2. Grouped aggregate chooses streaming variant when upstream is already ordered by group keys.

### 6. Write optimization and safety

1. MergeMatchProbeThenCreate uses atomic probe/create path.
2. Batch property updates by entity type with conflict-safe retry semantics.
3. Detach-delete uses bulk adjacency delete primitives to avoid N+1 scans.

## Sequence Of Query Pipeline Stages

### Generic stage sequence (read query)

1. Parse: build AST.
2. Semantic bind: resolve symbols and clause scopes.
3. Logical plan build: construct operator graph.
4. Rule rewrite pass:
   - predicate pushdown
   - empty-set pruning
   - join reordering
5. Physical planning:
   - choose scans/seeks
   - choose traversal variants
   - choose aggregate/distinct/sort variants
6. Runtime execute:
   - stream bindings
   - apply deferred hydration
   - collect operator counters
7. Result shaping and profile/explain output.

### Generic stage sequence (write query)

1. Parse + semantic bind.
2. Logical write plan:
   - read-before-write pattern parts
   - mutation actions
3. Physical planning:
   - existence probes
   - merge strategy selection
4. Runtime transactional execution:
   - apply writes via batch primitives
   - update indexes and adjacency structures
5. Return clause evaluation and write summary counters.

## Example: Non-Optimized vs Optimized Plan

Query:

MATCH (u:User {id: $uid})-[:FRIEND]-(f:User)
WHERE NOT (u)-[:BLOCKED]-(f)
RETURN f.id
ORDER BY f.score DESC
LIMIT 10

### Non-optimized plan shape

1. LabelScan(User) -> Filter(id=$uid)
2. UndirectedExpand(:FRIEND)
3. UndirectedExpand(:BLOCKED)
4. Filter(not exists blocked)
5. Project(f.id, f.score)
6. FullSort(score DESC)
7. Limit(10)

Problems:

1. Expands BLOCKED edges even when anti-probe would be cheaper.
2. Hydrates/projects all candidate rows before sort.
3. Performs full sort then limit.

### Optimized operator-native plan shape

1. IdSeek(User, $uid)
2. OutExpandTyped(:FRIEND) or InExpandTyped based on degree stats
3. IndexedAntiProbe(:BLOCKED, u.id, f.id)
4. Project(f.id, scoreRef=f.score) with deferred hydration
5. TopKHeap(order=scoreRef DESC, k=10)
6. Hydrate f.id only for top-k output rows

Additional pruning rule:

1. If edge_count_by_type(:FRIEND)=0 or adjacency(u,:FRIEND)=empty, planner emits EmptyRelation at step 2 and query terminates immediately.

Expected gains:

1. Lower intermediate cardinality from anti-probe.
2. O(n log k) top-k behavior instead of full sort.
3. Reduced hydration and allocation overhead.

## How Statistics Are Used In The Query Pipeline

Statistics are planner inputs, runtime feedback, and explain artifacts.

### Planner-time usage

1. Cardinality estimation:
   - estimate rows after each operator.
2. Access path selection:
   - choose index seek vs adjacency scan vs full scan.
3. Join/traversal ordering:
   - order expands and joins by estimated selectivity/fanout.
4. Operator variant selection:
   - hash vs streaming aggregate
   - full sort vs top-k
   - nested-loop anti-join vs indexed anti-probe
5. Empty-set proofs:
   - use zero cardinality stats to collapse subplans.

### Runtime usage

1. Adaptive safeguards:
   - switch from optimistic strategy when observed fanout diverges hard from estimates.
2. Counter emission:
   - per-operator input/output rows, filtered rows, probe hits/misses, spill events.
3. Stats feedback:
   - record observed selectivity and fanout samples for later refresh.

### Explain/profile usage

1. Show estimated rows vs actual rows per operator.
2. Show chosen access paths and rejected alternatives when available.
3. Show stats freshness metadata and confidence band.

## Migration Plan To Full Operator-Native

## Development Assumption

For this proposal branch, VitalEdge is pre-production and has no production datasets.

Allowed migration posture:

1. Database keyspace and index layout changes may be destructive.
2. Store migrations may require delete-and-recreate workflows.
3. Backward compatibility of on-disk formats is not required for this branch.
4. Test and benchmark fixtures should be reseeded from scripts after storage changes.

### Phase 1: Close operator gaps

1. Implement full MATCH/OPTIONAL/var-length traversal semantics in runtime operators.
2. Implement full projection/expression evaluator integration.
3. Add UNION/UNWIND/group operator coverage.

### Phase 2: Move write semantics fully native

1. Implement MERGE semantics including ON CREATE/ON MATCH actions.
2. Implement full SET/REMOVE/DELETE property-map semantics.
3. Achieve side-effect accounting parity at operator runtime layer.

### Phase 3: Pebble contract expansion

1. Add typed adjacency, existence probe, ordered seek, and batch mutation APIs.
2. Add statistics catalog persistence and refresh workers.
3. Wire runtime/storage adapter to new primitives.

### Phase 4: Planner and optimization hardening

1. Enable empty-set pruning, anti-probe rewrites, top-k pushdown, join reordering by default.
2. Add deterministic plan snapshots for rule coverage.
3. Add operator-level profile counters to explain output.

### Phase 5: Remove architectural remnant

1. Remove runtime delegation to shared executor backend for migrated clause families.
2. Keep temporary fallback only for explicitly unsupported clauses.
3. Promote full operator-native path to default/only path for supported Cypher surface.

## Companion Implementation Checklist

This checklist maps proposal requirements to current code anchors and defines completion criteria for immediate execution.

### A. Runtime entrypoint and delegation removal

1. Anchor: internal/cypher/executor/routing.go
   - Change: Replace shape-based delegation policy with operator-native default for supported clauses; isolate unsupported clauses behind explicit capability checks.
   - Done when: supported query families no longer route by runtimeSupported gate and no semantic delegation remains for those families.
   - Tests: internal/cypher/executor/routing_test.go, internal/cypher/executor/runtime_pipeline_test.go.

2. Anchor: internal/cypher/runtime/engine.go
   - Change: Add strict handler error propagation, operator failure classification, and per-operator counters.
   - Done when: Execute and ExecuteWithTx fail fast on operator errors and emit deterministic operator diagnostics.
   - Tests: internal/cypher/runtime/engine_test.go, internal/cypher/runtime/engine_dispatch_test.go.

### B. Operator completeness and clause coverage

1. Anchor: internal/cypher/runtime/operators/default_handlers.go
   - Change: Replace stub MATCH and OPTIONAL behavior with real adjacency traversal, directionality, variable-length traversal, and binding semantics.
   - Done when: runtime handlers produce real bound vertex, edge, and path outputs for MATCH, OPTIONAL MATCH, and var-length patterns.
   - Tests: internal/cypher/runtime/operators/default_handlers_test.go plus new traversal conformance tests.

2. Anchor: internal/cypher/physical/builder.go and internal/cypher/physical/plan.go
   - Change: Expand physical lowering for UNION, UNWIND, aggregate/group, anti-join, top-k, and write operators.
   - Done when: major clause families lower into explicit physical operators with no executor-only semantic branches.
   - Tests: internal/cypher/physical/builder_test.go.

3. Anchor: internal/cypher/logical/builder.go and internal/cypher/logical/plan.go
   - Change: Ensure logical plans represent full clause semantics needed by runtime-native execution.
   - Done when: logical plan snapshots cover MATCH, OPTIONAL, WHERE, WITH, RETURN, UNION, UNWIND, writes, and MERGE actions.
   - Tests: internal/cypher/logical/builder_test.go.

### C. Write semantics parity

1. Anchor: internal/cypher/runtime/write_applier.go
   - Change: Move beyond event-capture apply; implement full mutation semantics (MERGE phases, ON CREATE, ON MATCH, SET, REMOVE, DELETE, DETACH DELETE) with side-effect accounting.
   - Done when: runtime write execution matches existing Cypher behavior for supported forms and reports parity counters.
   - Tests: internal/cypher/runtime/write_applier_test.go and compliance write scenarios.

2. Anchor: internal/cypher/runtime/storage/tx_adapter.go
   - Change: Expand WriteSink to include merge probe/create, map mutation, detach-delete, and batch APIs.
   - Done when: runtime write operators no longer compose write semantics from minimal PutVertex/PutEdge only.
   - Tests: adapter unit tests and runtime write integration tests.

### D. Pebble and graph store contract expansion

1. Anchor: internal/graph/store.go
   - Change: Extend graph.Tx interface with typed adjacency scans, endpoint/batch existence probes, ordered index seek hooks, and richer mutation primitives.
   - Done when: runtime/storage adapter can invoke all required primitives directly through graph.Tx.
   - Tests: compile-time interface coverage and tx contract tests.

2. Anchor: internal/graph/store/pebble/store.go
   - Change: Implement new graph.Tx primitives with bounded iterators and batch write guarantees.
   - Done when: new methods pass correctness tests and are used by runtime operators for traversal, anti-probe, and write paths.
   - Tests: internal/graph/store/pebble/store_test.go.

3. Anchor: internal/graph/keyspace/keys.go
   - Change: Add keyspace support for any new adjacency, ordered index, and statistics records needed by operator-native plans.
   - Done when: key encoding/decoding tests cover new key families and scan bounds.
   - Tests: internal/graph/keyspace/keys_test.go.

4. Anchor: internal/graph/stats.go
   - Change: Extend stats structures to include cardinality, selectivity, ndv, histograms, and freshness metadata required by planning.
   - Done when: planner consumes these fields and explain/profile surfaces them.
   - Tests: planner/explain tests and store stats tests.

### E. Fast path and pushdown removal or rewrite

1. Anchor: internal/cypher/executor/executor.go
   - Change: Remove or rewrite remaining fast_path execution toggles into planner rules and physical operator variants.
   - Done when: fast_path strategy constants and adaptive control logic are no longer required for migrated query families.
   - Tests: executor and pipeline guardrail tests remain green.

2. Anchor: internal/cypher/executor/write.go
   - Change: Migrate stage1 and stage2 pushdown heuristics into physical planner/operator decisions; remove executor-local counters for migrated paths.
   - Done when: index pushdown and top-k are represented in physical plans and runtime operator counters, not executor-specific branches.
   - Tests: edge index tests, recommendation benchmark tests, runtime pipeline tests.

3. Anchor: internal/cypher/executor/explain.go
   - Change: Explain output should reflect operator-native decisions and stats-based choices without fast_path-only metadata dependencies.
   - Done when: explain payload reports physical operator choices and estimated vs actual rows for migrated queries.
   - Tests: explain pipeline contract tests and executor explain tests.

### F. Statistics-driven planning and feedback loop

1. Anchor: internal/cypher/physical/builder.go
   - Change: Use stats for join order, expand direction, empty-set proofs, sort vs top-k, and aggregate variant selection.
   - Done when: plan selection changes deterministically with stats inputs and empty-set cases collapse to EmptyRelation/NullBindProject as appropriate.
   - Tests: deterministic plan snapshot tests with synthetic stats fixtures.

2. Anchor: internal/cypher/explain/pipeline_explain.go
   - Change: Include estimated rows, actual rows, and stats freshness/confidence metadata per operator.
   - Done when: explain output includes consistent operator-level estimate/actual blocks.
   - Tests: internal/cypher/explain/pipeline_explain_test.go.

### G. Immediate execution order (current status)

1. Graph store and runtime storage adapter contracts:
   - Status: in progress.
   - Completed slices: typed endpoint probes, adjacency link scans, and additional tx adapter/runtime wiring used by migrated recommendation and anti-join paths.
   - Remaining: finish broad contract cleanup for all migrated operator families and remove legacy compatibility shims.

2. Pebble primitives and keyspace updates:
   - Status: in progress.
   - Completed slices: source-count and per-type average out-degree persisted stats, endpoint existence primitives, and related backfill paths.
   - Remaining: additional histogram/selectivity families and freshness metadata persistence.

3. Runtime operator coverage:
   - Status: in progress.
   - Completed slices: runtime planning/explain parity and expanded operator-path test coverage.
   - Remaining: eliminate residual executor-local semantic branches for migrated query families.

4. Logical/physical lowering and stats-aware choices:
   - Status: in progress.
   - Completed slices: stats-aware empty rewrites, anti-probe ordering hints, and hint plumbing for edge counts/source counts/avg degree.
   - Remaining: broaden deterministic stats-driven operator choice coverage (join/traversal/sort variants).

5. Executor fast-path and pushdown rewrite (stage1/stage2):
   - Status: completed for stage2 decision-surface consolidation.
   - Completed slices: stage2 hint-driven policy extraction for probe-limit, predicate-shape gating, one-sided-range gating, wide non-range skip, source probe mode, and pushdown acceptance decisions.
   - Remaining: no known residual stage2 decision-surface migration work; remaining G.2 write-path work is compliance parity outside this stage2 contract-lift stream.

6. Routing policy flip for migrated coverage:
   - Status: pending.
   - Trigger: once migrated families no longer rely on executor-local semantic fallback.

7. Verification gates:
   - Status: active and required per slice.
   - Current practice: focused stage2 tests + touched-package regressions + broad cypher package regressions before merge.

#### G.1 Next implementation slice (immediate)

1. Completed in latest slice: per-row predicate/range/wide-skip assessment and probe lookup-context decisions are now consolidated in stage2HintPolicy policy methods, removing another inline branch cluster from the stage2 collection loop.
2. Completed in latest slice: remaining stage2 index lookup decision/cache bookkeeping branch moved out of the collect loop into dedicated helper methods, preserving counter names and cache semantics.
3. Completed in latest slice: stage2 first-hit counter/state-toggle bookkeeping is now centralized in helper methods, removing the remaining one-time toggle branches from the collect loop.
4. Completed in latest slice: stage2 collect-loop early-stop toggle and remaining-potential accounting orchestration moved into a dedicated helper state with focused unit coverage.
5. Completed in latest slice: stage2 work-item edge-processing execution split (indexed-path vs adjacency-scan path) is now extracted behind a single helper closure, reducing inline collect-loop branching.
6. Completed in latest slice: early-stop frontier edge-filter branch migrated into collect-orchestration helper method with focused counter-aware unit coverage.
7. Completed in latest slice: candidate-group reuse-vs-create branch in stage2 candidate processing is now helperized (reuse gate, ensure/create, and aggregate accumulation), reducing inline mutation/counter logic.
8. Completed in latest slice: merged-row construction + right-vertex hydration + optional where-eval gate path in stage2 candidate processing is now extracted into dedicated helper methods.
9. Completed in latest slice: early-stop frontier activation condition/counter branch moved into collect-orchestration helper method with focused unit coverage.
10. Completed in latest slice: stage2 max-outside-score early-stop bound computation extracted into a dedicated collect-orchestration helper with focused unit coverage.
11. Completed in latest slice: local ready/boundary/frontier decision glue around stage2TopKFrontierBoundary is now centralized in a collect-orchestration helper method.
12. Completed in latest slice: post-work-item early-stop orchestration gate (enabled check + potential consume + checks counter + frontier resolution) extracted into a single collect-orchestration helper.
13. Completed in latest slice: stage2 final output assembly split (top-k path vs non-top-k projection/post-processing path) extracted into a dedicated helper with focused counter-aware tests.
14. Completed in latest slice: stage2 output-tail branch (projection columns selection, prior-column fallback, trim, and rows_output counter) extracted into a dedicated helper with focused tests.
15. Completed in latest slice: stage2 adjacency scan-type selection branch (single edge type vs any-of fanout) extracted into a dedicated helper with focused unit coverage.
16. Completed in latest slice: orchestration-only scan confirms stage2 collect-loop control-flow plumbing is now helperized end-to-end (remaining inline logic is primarily semantic filtering/evaluation, not orchestration glue).
17. Completed in latest slice: stage2 semantic prefilter drop gate for numeric constraints and anti-join right-side exclusions moved into a dedicated helper with focused counter-aware unit coverage.
18. Completed in latest slice: remaining stage2 edge-type + edge-props gate checks were extracted into a dedicated candidate-edge gate helper, preserving collect-loop gating semantics.
19. Completed in latest slice: stage2 group-id/frontier/visit gate sequence moved into a dedicated helper, preserving early-stop skip + edges-visited counter behavior with focused tests.
20. Completed in latest slice: stage2 inline reuse-vs-scope-eval candidate-processing branch extracted into a dedicated helper, preserving reuse hits, scope-eval behavior, and aggregate accumulation semantics.
21. Completed in latest slice: post-scope-eval matched-vs-not-matched aggregate-application decision extracted into a dedicated helper, preserving aggregate creation/accumulation and counter behavior.
22. Completed in latest slice: stage2 reuse-guard decision inside candidate aggregation flow extracted into a dedicated helper, preserving reuse-hit gating and counter behavior.
23. Completed in latest slice: right-variable binding guard in stage2 candidate-scope resolution extracted into a dedicated helper, preserving binding semantics for named/blank right vars.
24. Completed in latest slice: stage2 where-evaluation guard/drop branch in candidate-scope resolution extracted into a dedicated helper, preserving where_eval_drops counter semantics.
25. Completed in latest slice: candidate hydration-result normalization (resolve error vs unmatched-right vs matched-right) extracted into a dedicated helper, preserving scope-resolution semantics.
26. Completed in latest slice: where-gate result normalization (where error vs unmatched-vs-matched) extracted into a dedicated helper, preserving candidate-scope return semantics.
27. Completed in latest slice: reusable-aggregate eligibility guard (skipWhereEval + group lookup + nil-check) extracted into a dedicated helper, preserving reuse-hit behavior.
28. Completed in latest slice: candidate aggregate sample-seeding decision (late-materialize candidate vs cloned scope sample) extracted into a dedicated helper, preserving aggregate creation counters.
29. Completed in latest slice: similarity accumulation eligibility guard inside stage2 candidate aggregation extracted into a dedicated helper, preserving similarity-sum semantics.
30. Completed in latest slice: average-accumulation eligibility guard inside stage2 candidate aggregation extracted into a dedicated helper, preserving avgCount/avgSum semantics.
31. Completed in latest slice: existing-candidate-aggregate lookup guard (missing/nil vs reusable aggregate) extracted into a dedicated helper, preserving aggregate reuse/create behavior.
32. Completed in latest slice: candidate-aggregate creation counter branch (groups-created + late-materialization counter gate) extracted into a dedicated helper, preserving counter semantics.
33. Completed in latest slice: average-contribution resolution guard inside stage2 aggregation extracted into a dedicated helper, preserving numeric-property eligibility behavior.
34. Completed in latest slice: similarity-eligibility guard predicate inside stage2 aggregation extracted into a dedicated helper, preserving similarity accumulation behavior.
35. Completed in latest slice: reusable-aggregate skipWhereEval gate predicate extracted into a dedicated helper, with reuse path lookup delegated through existing aggregate-lookup helper.
36. Completed in latest slice: candidate-aggregate nil-guard predicate in stage2 accumulation extracted into a dedicated helper, preserving accumulation short-circuit behavior.
37. Completed in latest slice: late-materialization counter gate predicate in candidate-aggregate creation observation extracted into a dedicated helper, preserving counter behavior.
38. Completed in latest slice: existing-candidate-aggregate usability predicate (exists flag + non-nil aggregate) extracted into a dedicated helper, preserving lookup behavior.
39. Completed in latest slice: sample-candidate seeding predicate (late-materialize projection gate) extracted into a dedicated helper, preserving sample-capture behavior.
40. Completed in latest slice: existing-candidate-aggregate reuse predicate extracted from ensure logic into a dedicated helper, preserving reuse-vs-create behavior.
41. Completed in latest slice: tightened max-pushdown-candidates applicability predicate extracted from stage2 max-candidate cap logic into a dedicated helper, preserving cap-tightening behavior.
42. Completed in latest slice: low-coverage/low-degree shared-source probe preference predicate extracted into a dedicated helper and reused across shared/per-peer probe decisions, preserving probe-mode behavior.
43. Completed in latest slice: high-coverage/high-degree wide-non-range pushdown-skip predicate extracted into a dedicated helper, preserving skip behavior.
44. Completed in latest slice: hint-coverage/high-candidate-load pushdown rejection predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving rejection behavior.
45. Completed in latest slice: avg-out-degree overload pushdown rejection predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving rejection behavior.
46. Completed in latest slice: absolute candidate-cap pushdown rejection predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving rejection behavior.
47. Completed in latest slice: average-per-source hard-cap pushdown rejection predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving rejection behavior.
48. Completed in latest slice: no-candidates short-circuit predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving short-circuit behavior.
49. Completed in latest slice: empty-indexed-sources short-circuit predicate extracted from stage2 pushdown eligibility into a dedicated helper, preserving short-circuit behavior.
50. Completed in latest slice: empty-type-list guard for stage2 hint-degree selectivity extracted into a dedicated helper, preserving selectivity short-circuit behavior.
51. Completed in latest slice: source/edge aggregate-count availability guard for stage2 hint-degree selectivity extracted into a dedicated helper, preserving selectivity short-circuit behavior.
52. Completed in latest slice (larger chunk): two stage2 hint-degree-selectivity decisions were extracted together into dedicated helpers (duplicate-type skip and direct-average override gate), preserving selectivity behavior.
53. Completed in latest slice (larger chunk): stage2 hint-degree-selectivity type collection and direct-average resolution decisions were extracted into dedicated helpers together, preserving selectivity behavior.
54. Completed in latest slice (larger chunk): stage2 finalization top-k gate and aggregate-row inclusion gate were extracted together into dedicated helpers, preserving finalization behavior and counters.
55. Completed in latest slice (larger chunk): stage2 result-row scope construction, projection output-key resolution, and average-value resolution decisions were extracted together into dedicated helpers, preserving finalization behavior.
56. Completed in latest slice (larger chunk): stage2 top-k spec ORDER BY cardinality gate, LIMIT-presence gate, and similarity-order-expression compatibility gate were extracted together into dedicated helpers, preserving top-k planning behavior.
57. Completed in latest slice (larger chunk): stage2 top-k row-planning decisions (empty-limit gate, keep-size resolution, aggregate inclusion gate, skip-window empty gate, and window-end resolution) were extracted together into dedicated helpers, preserving top-k row behavior.
58. Completed in latest slice (larger chunk): stage2 top-k rank comparison decisions (score ordering by direction and input-index tie-break ordering) were extracted together into dedicated helpers, preserving ranking behavior.
59. Completed in latest slice (larger chunk): shared-peer top-k WITH-clause parser eligibility decisions (with-spec shape gate, projection-item eligibility gate, projection-binding completeness gate, and similarity-order-expression compatibility gate) were extracted together into dedicated helpers, preserving parser behavior.
60. Completed in latest slice (larger chunk): shared-peer top-k row-planning decisions (empty-limit gate, keep-size resolution, aggregate inclusion gate, skip-window empty gate, and window-end resolution) were extracted together into dedicated helpers, preserving top-k row behavior.
61. Completed in latest slice (larger chunk): shared-peer top-k rank comparison decisions (score ordering by direction and input-index tie-break ordering) were extracted together into dedicated helpers, preserving ranking behavior.
62. Completed in latest slice (larger chunk): shared-peer aggregate ordering decisions (vertex-ID normalization and target/peer lexicographic ordering) were extracted together into dedicated helpers, preserving deterministic ordering behavior.
63. Completed in latest slice (larger chunk): shared-peer top-k row-assembly decisions (where-evaluation gate, similarity-score numeric resolution, and trimmed output-row construction) were extracted together into dedicated helpers, preserving top-k row behavior.
64. Completed in latest slice (larger chunk): shared-peer top-k candidate-selection decisions (heap-capacity push gate and root-replacement gate) were extracted together into dedicated helpers, preserving top-k selection behavior.
65. Completed in latest slice (larger chunk): shared-peer top-k row materialization decisions (average-diff resolution, row-seed construction, and ranked-candidate construction) were extracted together into dedicated helpers, preserving top-k row behavior.
66. Completed in latest slice (larger chunk): shared-peer top-k WITH parser projection-item decisions (output-key resolution, expression normalization, and target/peer/similarity binding with duplicate-similarity rejection) were extracted together into dedicated helpers, preserving parser behavior.
67. Completed in latest slice (larger chunk): shared-peer top-k post-selection decisions (ranked-row sorting, skip/limit ranked-window resolution, and output-row materialization from ranked window) were extracted together into dedicated helpers, preserving top-k row behavior.
68. Completed in latest slice (larger chunk): stage2 top-k early-stop frontier-boundary decisions (candidate inclusion/heap replacement predicates, frontier index+group mapping, and max non-frontier score resolution) were extracted together into dedicated helpers, preserving early-stop frontier behavior.
69. Completed in latest slice (larger chunk): non-shared stage2 top-k row decisions (heap push/replacement predicates, ranked-row sorting, ranked-window resolution, and output-row materialization) were extracted together into dedicated helpers, preserving top-k row behavior.
70. Completed in latest slice (larger chunk): stage2 candidate-edge processing decisions in the work-item loop (edge-gate/prefilter/visit-gate processability resolution and aggregation-application handoff) were extracted together into dedicated helpers, preserving counter names and behavior.
71. Completed in latest slice (larger chunk): stage2 work-item edge-path routing decisions (indexed-path predicate, nil-indexed-edge skip predicate, indexed-edge processing helper, and indexed-vs-scan orchestration helper) were extracted together into dedicated helpers, preserving path behavior and counters.
72. Completed in latest slice (larger chunk): stage2 index-lookup pushdown resolution decisions (probe-cap observation gate, indexed-candidate evaluation gate, indexed-candidate grouping/peer selection, cache-write gate, and final lookup-decision shaping) were extracted together into dedicated helpers, preserving pushdown behavior and counters.
73. Completed in latest slice (larger chunk): stage2 collectPeerEdges index-lookup decisions (lookup-attempt gate, per-peer-scope note gate, index-decision acceptance gate, and indexed-result shaping) were extracted together into dedicated helpers, preserving lookup behavior and counters.
74. Completed in latest slice (larger chunk): stage2 index-lookup cache-key construction decisions (candidate-type gate, type/equality/range fragment assembly, cache-key-component gate, and final key-result shaping) were extracted together into dedicated helpers, preserving cacheability behavior.
75. Completed in latest slice (larger chunk): stage2 predicate-shape numeric-constraint decisions (constraint-enables gate, aggregate enabling-constraints gate, one-sided-constraint gate, aggregate one-sided gate, and edge-prop-equality decisiveness gate) were extracted together into dedicated helpers, preserving pushdown-shape behavior.
76. Completed in latest slice (larger chunk): stage2 hint-policy row-assessment decisions (row numeric-constraint resolution, numeric-range-shape gate, initial-assessment construction, early-return gate, and assessment-completion assembly) were extracted together into dedicated helpers, preserving pushdown-assessment behavior.
77. Completed in latest slice (larger chunk): stage2 hint-policy lookup-context routing decisions (default-context construction, per-peer-use gate, peer-ID normalization/scoping gate, and per-peer-context construction) were extracted together into dedicated helpers, preserving lookup-context behavior.
78. Completed in latest slice (larger chunk): stage2 indexed-edge source grouping decisions (source-ID resolution/include gate, per-source append, and candidate-total increment shaping) were extracted together into dedicated helpers, preserving lookup-candidate grouping behavior.
79. Completed in latest slice (larger chunk): stage2 reusable/create aggregate decisions (reusable-apply gate, reusable-accumulation handoff, new-aggregate build+sample decision, and aggregate registration into map/order) were extracted together into dedicated helpers, preserving candidate-aggregation behavior and counters.
80. Completed in latest slice (larger chunk): stage2 max-pushdown-cap decisions (base cap resolution, one-sided-range relaxation resolution+apply gate, high-degree-threshold/tightened-cap resolution, and hint-tightening apply gate) were extracted together into dedicated helpers, preserving cap-selection behavior.
81. Completed in latest slice (larger chunk): stage2 source-probe coverage decisions (source-peer presence gate, scoped-source threshold gate, observed-coverage resolution, and shared coverage+degree resolution) were extracted together into dedicated helpers and reused across shared/per-peer/wide-skip probe-mode decisions, preserving behavior.
82. Completed in latest slice (larger chunk): stage2 index-pushdown candidate-load decisions (indexed-candidate counting, average-per-indexed-source resolution, candidate-load tuple shaping, and hint-coverage rejection shaping from indexed-source counts) were extracted together into dedicated helpers and reused in pushdown eligibility flow, preserving behavior.
83. Completed in latest slice (larger chunk): stage2 index-pushdown rejection orchestration decisions (hint-policy rejection resolution combining coverage-from-indexed-sources and degree-overload checks, plus hard-cap rejection resolution combining candidate-count and average-per-source caps) were extracted together into dedicated helpers and reused in pushdown eligibility flow, preserving behavior.
84. Completed in latest slice (larger chunk): stage2 hint-degree-selectivity aggregation decisions (type normalization, aggregation include/dedup resolution, per-type count accumulation, and direct-average-or-aggregated-average fallback resolution) were extracted together into dedicated helpers and reused in selectivity computation, preserving behavior.
85. Completed in latest slice (larger chunk): stage2 top-k spec parsing decisions (order-expression resolution, skip/limit pagination evaluation, limit normalization, and final top-k spec shaping) were extracted together into dedicated helpers and reused in fast peer-candidate top-k spec resolution, preserving behavior.
86. Completed in latest slice (larger chunk): shared-peer stage2 top-k WITH parsing decisions (projection-item-count gate, order-expression resolution, skip/limit pagination evaluation, and final shared-peer top-k spec shaping) were extracted together into dedicated helpers and reused in clause parsing flow, preserving behavior.
87. Completed in latest slice (even bigger chunk): shared-peer stage2 top-k candidate-resolution decisions (row-seed construction handoff, optional WHERE gate/evaluation, similarity-expression evaluation, numeric-score resolution gate, trimmed-row shaping, ranked-candidate construction, and input-index progression shaping) were extracted together into dedicated helpers and reused in top-k row loop orchestration, preserving behavior.
88. Completed in latest slice (even bigger chunk): shared-peer stage2 top-k orchestration decisions (heap application path combining push-vs-root-replace behavior and finalization path combining ranked-row sorting, skip/limit window resolution, and output-row materialization) were extracted together into dedicated helpers and reused in top-k row loop final orchestration, preserving behavior.
89. Completed in latest slice (even bigger chunk): shared-peer stage2 aggregate-ordering decisions (ordering-inclusion gate for non-nil aggregates, aggregate collection shaping, deterministic aggregate sorting orchestration, and sorted-result resolution) were extracted together into dedicated helpers and reused in shared-peer top-k aggregate iteration flow, preserving behavior.
90. Completed in latest slice (even bigger chunk): shared-peer stage2 top-k WITH parsing orchestration decisions (projection-binding loop resolution with eligibility+binding-completeness gates and spec-resolution flow with order-expression compatibility + pagination evaluation + final spec shaping) were extracted together into dedicated helpers and reused in clause parsing flow, preserving behavior.
91. Completed in latest slice (even bigger chunk): shared-peer stage1 top-k entry orchestration decisions (empty-result gate resolution and both aggregate/top-k output-column shaping decisions) were extracted together into dedicated helpers and reused in tryFastTargetSharedPeerAggregationWithTopKClauses flow, preserving behavior.
92. Completed in latest slice (even bigger chunk): shared-peer stage1 aggregate-collection preflight decisions (seed-row/tx eligibility gate, match+chain resolution with optional/shape guards, with-spec eligibility gate, and with+projection resolution flow) were extracted together into dedicated helpers and reused in collectFastTargetSharedPeerAggregates entry flow, preserving behavior.
93. Completed in latest slice (even bigger chunk): shared-peer stage1 aggregate-collection runtime decisions (match-WHERE presence gate, first/second-hop scan-type resolution for single-type vs any-of expansion, and shared shortcut/eval/drop WHERE orchestration with runtime-counter updates) were extracted together into dedicated helpers and reused across both single-target and seeded multi-target expansion paths, preserving behavior.
94. Completed in latest slice (even bigger chunk): shared-peer stage1 post-collection clause-pair decisions (aggregate-row inclusion gate, with-row materialization shaping, with-filter application orchestration, and output-column shaping) were extracted together into dedicated helpers and reused in tryFastTargetSharedPeerAggregationClausePair flow, preserving behavior.
95. Completed in latest slice (even bigger chunk): stage1 two-hop distinct write preflight decisions (seed/tx eligibility gate, match+chain resolution with chain-eligibility gate, DISTINCT-with-clause eligibility + projection-item resolution flow, write-pattern raw resolution, merge-semantics admissibility gate, and projection-binding gate) were extracted together into dedicated helpers and reused in tryFastTwoHopDistinctWriteClauses entry flow, preserving behavior.
96. Completed in latest slice (even bigger chunk): stage1 two-hop distinct write anti-join policy decisions (zero-type-shortcut eligibility gate, endpoint-probe/prefetch policy resolution, and left-candidate-set shaping) were extracted together into dedicated helpers and reused in tryFastTwoHopDistinctWriteClauses anti-join setup flow, preserving behavior.
97. Completed in latest slice (even bigger chunk): stage2 fast peer-candidate semantic-branch work-item assembly/orchestration was helperized in a grouped extraction (top-k early-stop settings resolution, bound peer-input/source-set collection, row prefilter + similarity resolution, and staged work-item construction), preserving runtime counters and fallback behavior.
98. Completed in latest slice (even bigger chunk): stage2 fast peer-candidate per-work-item processing loop was helperized as a grouped orchestration extraction (single-work-item candidate-edge processing with hydration policy + edge-path dispatch + early-stop frontier advancement, plus outer work-item loop coordinator), preserving counters and behavior.
99. Completed in latest slice (even bigger chunk): stage2 candidate-edge aggregation decision path was helperized with a grouped eval-context extraction (context builder for edge evaluation inputs, eval-context processable-group resolver, and eval-context aggregation applier), and stage2ProcessCandidateEdgeAggregation now orchestrates through this helper cluster while preserving counters and behavior.
100. Completed in latest slice (even bigger chunk): stage2 candidate-scope resolution was helperized in a grouped extraction (candidate-scope eval-context builder, merged-row right-binding helper, and normalized where-match resolver helper), and resolveStage2CandidateScope now orchestrates via this helper cluster while preserving behavior/counters.
101. Completed in latest slice (even bigger chunk): stage2 aggregation-decision orchestration was helperized with a grouped decision-context extraction (aggregation-decision context builder, candidate-scope resolver wrapper, and resolved-aggregation applier wrapper), and stage2ApplyCandidateAggregationDecision now orchestrates through this helper cluster while preserving behavior/counters.
102. Completed in latest slice (even bigger chunk): stage2 index-lookup decision orchestration was helperized in a grouped extraction (index-lookup resolution context builder, cached-decision resolver, probe-cap observation helper, indexed-candidate decision helper, and final decision/cache write helper), and resolveStage2IndexLookupDecision now orchestrates via this helper cluster while preserving behavior/counters.
103. Completed in latest slice (even bigger chunk): stage2 single-work-item edge processing orchestration was helperized in a grouped extraction (work-item processing context builder, candidate-edge dispatch helper, and frontier-advance helper), and stage2ProcessSinglePeerWorkItem now orchestrates through this helper cluster while preserving behavior/counters.
104. Completed in latest slice (even bigger chunk): stage2 peer-work-item loop orchestration was helperized in a grouped extraction (peer-work-item loop context builder, loop-iteration helper wrapper, and frontier-update helper), and stage2ProcessPeerWorkItems now orchestrates through this helper cluster while preserving behavior/counters.
105. Completed in latest slice (even bigger chunk): stage2 candidate prefilter guard/decision branch was helperized in a grouped extraction (candidate-prefilter decision context builder, numeric-prefilter evaluation/drop helpers, anti-join prefilter drop helper, and unified prefilter-drop resolution/observation helper), and stage2ShouldDropCandidateEdgeByPrefilters now orchestrates through this helper cluster while preserving behavior/counters.
106. Completed in latest slice (even bigger chunk): stage2 candidate-group visit gating was helperized in a grouped extraction (candidate-group visit decision context builder, destination-group normalization/eligibility helper, frontier-skip decision helper, and unified visit-resolution/observation helpers), and stage2ResolveCandidateGroupVisitGate now orchestrates through this helper cluster while preserving behavior/counters.
107. Completed in latest slice (even bigger chunk): stage2 candidate edge-path routing/iteration decisions were helperized in a grouped extraction (indexed-edge iteration context builder, indexed-edge considered-observation + single-iteration processing helpers, and work-item edge-path decision context/strategy/resolved-path helpers), and stage2ProcessIndexedEdgePath + stage2ProcessWorkItemEdgePath now orchestrate through this helper cluster while preserving behavior/counters.
108. Completed in latest slice (even bigger chunk): stage2 processable-candidate gate-chain orchestration was helperized in a grouped extraction (processable-candidate decision context builder, edge-gate decision helper, prefilter-gate decision helper, visit-gate wrapper helper, and unified processable-candidate resolver), and stage2ResolveProcessableCandidateGroupID now orchestrates through this helper cluster while preserving behavior/counters.
109. Completed in latest slice (even bigger chunk): stage2 candidate-WHERE gate decisions were helperized in a grouped extraction (candidate-WHERE decision context builder, where-gate bypass decision helper, where evaluation wrapper helper, where-drop observation helper, and unified where-gate resolution helper), and stage2ApplyCandidateWhereGate now orchestrates through this helper cluster while preserving behavior/counters.
110. Completed in latest slice (even bigger chunk): stage2 candidate-scope hydration/outcome orchestration was helperized in a grouped extraction (candidate-scope hydration decision context builder, hydrated-match resolver wrapper, shared abort-decision helper, abort-result shaping helper, and merged+where-match resolver helper), and resolveStage2CandidateScope now orchestrates through this helper cluster while preserving behavior/counters.
111. Completed in latest slice (even bigger chunk): stage2 reusable-candidate-aggregate fast-path orchestration was helperized in a grouped extraction (reusable-aggregate decision context builder, reusable-resolution wrapper helper, reusable-hit observation helper, and unified reusable-decision resolver helper), and stage2ReuseExistingCandidateAggregateIfEligible now orchestrates through this helper cluster while preserving behavior/counters.
112. Completed in latest slice (even bigger chunk): stage2 ensure-candidate-aggregate orchestration was helperized in a grouped extraction (ensure-aggregate decision context builder, existing-aggregate resolution helper, new-aggregate creation+registration+observation helper, and unified ensure resolver helper), and stage2EnsureCandidateAggregate now orchestrates through this helper cluster while preserving behavior/counters.
113. Completed in latest slice (even bigger chunk): stage2 max-pushdown candidate-cap resolution orchestration was helperized in a grouped extraction (max-pushdown decision context builder, one-sided relaxed-cap apply helper, hint avg-out-degree resolver helper, hint-tightened-cap apply helper, and unified max-pushdown decision resolver helper), and stage2MaxPushdownCandidates now orchestrates through this helper cluster while preserving behavior/counters.
114. Completed in latest slice (even bigger chunk): stage2 source-probe strategy decisions were helperized in a grouped extraction (source-probe strategy decision context builder, coverage+degree resolver wrapper, shared-mode resolver helper, per-peer-mode resolver helper, and wide-non-range skip resolver helper), and stage2ShouldUseSharedSourceProbeFilter + stage2ShouldUsePerPeerSourceProbe + stage2ShouldSkipWideNonRangePushdown now orchestrate through this helper cluster while preserving behavior/counters.
115. Completed in latest slice (even bigger chunk): stage2 index-pushdown eligibility orchestration was helperized in a grouped extraction (index-pushdown eligibility decision context builder, candidate-load resolution helper, no-candidate decision helper, hint-policy rejection decision helper, hard-cap rejection decision helper, and unified eligibility resolver helper), and shouldApplyStage2IndexPushdown now orchestrates through this helper cluster while preserving behavior/counters.
116. Completed in latest slice (even bigger chunk): stage2 hint-degree selectivity aggregation orchestration was helperized in a grouped extraction (hint-degree selectivity decision context builder, included-type resolver helper, per-type accumulation helper, average-resolution helper, and unified hint-degree decision resolver helper), and stage2HintDegreeSelectivity now orchestrates through this helper cluster while preserving behavior/counters.
117. Completed in latest slice (even bigger chunk): stage2 final output row orchestration was helperized in a grouped extraction (final-output decision context builder, candidate-group-total counter observer helper, top-k final row resolver helper, non-top-k final row resolver helper, and unified final-output resolver helper), and finalizeStage2OutputRows now orchestrates through this helper cluster while preserving behavior/counters.
118. Completed in latest slice (even bigger chunk): stage2 top-k candidate accumulation orchestration was helperized in a grouped extraction (top-k rows decision context builder, candidate row builder helper, push-or-replace heap decision helper, candidate accumulation resolver helper, and unified top-k rows decision resolver helper), and fastPeerCandidateTopKRows now orchestrates through this helper cluster while preserving behavior/counters.
119. Completed in latest slice (even bigger chunk): stage2 top-k spec parsing orchestration was helperized in a grouped extraction (top-k spec decision context builder, eligibility gate helper, order-expression match helper, spec-build helper, and unified top-k spec decision resolver helper), and fastPeerCandidateTopKSpecFromProjection now orchestrates through this helper cluster while preserving behavior/counters.
120. Completed in latest slice (even bigger chunk): stage2 projection-tail finalization orchestration was helperized in a grouped extraction (projection-tail decision context builder, projection-tail columns resolver helper, projection-tail trimmed-rows resolver helper, rows-output counter observer helper, and unified projection-tail decision resolver helper), and finalizeStage2ProjectionTail now orchestrates through this helper cluster while preserving behavior/counters.
121. Completed in latest slice (even bigger chunk): stage2 fast peer candidate result-row assembly orchestration was helperized in a grouped extraction (result-row decision context builder, non-aggregate value resolver helper, non-aggregate row resolver helper, aggregate row resolver helper, and unified result-row decision resolver helper), and buildFastPeerCandidateResultRow now orchestrates through this helper cluster while preserving behavior/counters.
122. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k spec resolution was helperized in a grouped extraction (top-k spec decision context builder, order-match decision helper, pagination decision helper, and unified top-k spec decision builder helper), and stage2ResolveFastTargetSharedPeerTopKSpecFromWith now orchestrates through this helper cluster while preserving behavior/counters.
123. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer WITH-clause top-k parsing orchestration was helperized in a grouped extraction (WITH-clause decision context builder, projection-items decision helper, projection-binding decision helper, spec-resolution decision helper, and unified WITH-clause decision resolver helper), and parseFastTargetSharedPeerTopKWithClause now orchestrates through this helper cluster while preserving behavior/counters.
124. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k row selection orchestration was helperized in a grouped extraction (top-k rows decision context builder, candidate-accumulation resolver helper, and unified top-k rows decision resolver helper), and fastTargetSharedPeerTopKRows now orchestrates through this helper cluster while preserving behavior/counters.
125. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k candidate-row resolution orchestration was helperized in a grouped extraction (candidate-row decision context builder, where-gate decision helper, similarity-decision helper, and unified candidate-row decision resolver helper), and stage2ResolveFastTargetSharedPeerTopKCandidateRow now orchestrates through this helper cluster while preserving behavior/counters.
126. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k candidate selection orchestration was helperized in a grouped extraction (candidate decision context builder, candidate-row resolution helper, ranked-candidate shaping helper, and unified candidate decision resolver helper), and stage2ResolveFastTargetSharedPeerTopKCandidate now orchestrates through this helper cluster while preserving behavior/counters.
127. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k aggregate-iteration candidate orchestration was helperized in a grouped extraction (aggregate decision context builder, aggregate candidate resolution helper, aggregate candidate-application helper, and unified aggregate decision resolver helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidates now orchestrates through this helper cluster while preserving behavior/counters.
128. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k rows decision orchestration was helperized in a grouped extraction (rows-resolve decision context builder, empty-limit decision helper, candidate-resolution decision helper, finalization decision helper, and unified rows-resolve decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsDecision now orchestrates through this helper cluster while preserving behavior/counters.
129. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k projection resolution orchestration was helperized in a grouped extraction (projection decision context builder, projection-items decision helper, projection-bindings decision helper, and unified projection decision resolver helper), and stage2ResolveFastTargetSharedPeerTopKProjection now orchestrates through this helper cluster while preserving behavior/counters.
130. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k WITH-clause parse preflight orchestration was helperized in a grouped extraction (parse decision context builder, with-spec parse decision helper, with-spec eligibility decision helper, and unified parse decision resolver helper), and parseFastTargetSharedPeerTopKWithClause now orchestrates through this helper cluster while preserving behavior/counters.
131. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k spec resolution orchestration was helperized in a grouped extraction (spec-resolve decision context builder, order-match decision helper, pagination decision helper, spec-finalization decision helper, and unified spec-resolve decision helper), and stage2ResolveFastTargetSharedPeerTopKSpecFromWith now orchestrates through this helper cluster while preserving behavior/counters.
132. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k WITH-clause decision orchestration was helperized in a grouped extraction (with-clause resolve decision context builder, items resolve decision helper, projection resolve decision helper, spec resolve decision helper, finalization decision helper, and unified with-clause resolve decision helper), and stage2ResolveFastTargetSharedPeerTopKWithClauseDecision now orchestrates through this helper cluster while preserving behavior/counters.
133. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k projection-item binding semantic decisions were helperized in a grouped extraction (projection-item binding decision context builder, target-binding decision helper, peer-binding decision helper, similarity-binding decision helper, and unified projection-item binding decision helper), and stage2ApplyFastTargetSharedPeerTopKProjectionItemBinding now orchestrates through this helper cluster while preserving behavior/counters.
134. Completed in latest slice (even bigger chunk): stage2 shared-peer top-k candidate-application branch decisions (candidate-apply decision context builder, push-decision helper, replace-decision helper, push-apply helper, replace-apply helper, and unified candidate-apply decision helper) were extracted together into dedicated helpers, and stage2ApplySharedPeerTopKCandidate now orchestrates through this helper cluster while preserving behavior/counters.
135. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k aggregate-candidate and rows-candidate-iteration semantic decisions were helperized in grouped extractions (aggregate-candidate resolve decision context builder, aggregate-eligibility decision helper, aggregate-candidate-values decision helper, aggregate-candidate-finalize decision helper, unified aggregate-candidate resolve helper, rows-candidates resolve decision context builder, per-iteration rows-candidate decision helper, and unified rows-candidates resolve helper), and stage2ResolveFastTargetSharedPeerTopKAggregateCandidateDecision + stage2ResolveFastTargetSharedPeerTopKRowsCandidates now orchestrate through these helper clusters while preserving behavior/counters.
136. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k aggregate-candidate apply semantic decisions were helperized in a grouped extraction (aggregate-apply decision context builder, apply-gate decision helper, rows-context preparation decision helper, candidate-apply decision helper, finalize decision helper, and unified aggregate-apply decision helper), and stage2ApplyFastTargetSharedPeerTopKAggregateCandidateDecision now orchestrates through this helper cluster while preserving behavior/counters.
137. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k candidate-resolution semantic decisions were helperized in a grouped extraction (candidate-resolve decision context builder, candidate-row resolve decision helper, ranked-candidate resolve decision helper, candidate-finalize resolve decision helper, and unified candidate-resolve decision helper), and stage2ResolveFastTargetSharedPeerTopKCandidateDecision now orchestrates through this helper cluster while preserving behavior/counters.
138. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k finalization semantic decisions were helperized in a grouped extraction (top-k finalization decision context builder, ranked-sort decision helper, ranked-window decision helper, output-rows decision helper, and unified finalization decision helper), and stage2FinalizeFastTargetSharedPeerTopKRows now orchestrates through this helper cluster while preserving behavior/counters.
139. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k rows-resolve orchestration semantic decisions were helperized in a grouped extraction (rows-resolve flow decision context builder, empty-limit flow decision helper, candidate-resolution flow decision helper, finalization flow decision helper, and unified rows-resolve flow decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsResolveDecision now orchestrates through this helper cluster while preserving behavior/counters.
140. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k aggregate and rows-candidates loop/error orchestration semantic decisions were helperized in grouped extractions (aggregate-resolve flow decision context builder, aggregate-candidate flow decision helper, aggregate-apply flow decision helper, unified aggregate-resolve flow decision helper, rows-candidates-resolve flow decision context builder, per-iteration rows-candidates flow decision helper, and unified rows-candidates-resolve flow decision helper), and stage2ResolveFastTargetSharedPeerTopKAggregateDecision + stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveDecision now orchestrate through these helper clusters while preserving behavior/counters.
141. Completed in latest slice (even bigger chunk): stage2 fast-target-shared-peer top-k rows candidate-iteration and rows-candidate orchestration semantic decisions were helperized in grouped extractions (rows-candidate-iteration resolve flow decision context builder, iteration aggregate-flow decision helper, iteration apply-flow decision helper, unified rows-candidate-iteration resolve flow decision helper, rows-candidate resolve flow decision context builder, rows-candidate rows-flow decision helper, rows-candidate apply-flow decision helper, and unified rows-candidate resolve flow decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationDecision + stage2ResolveFastTargetSharedPeerTopKRowsCandidateDecision now orchestrate through these helper clusters while preserving behavior/counters.
142. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k flow-result shaping semantic decisions were helperized across the remaining shared-peer top-k flow stack (aggregate-resolve flow result decision helper, rows-candidate-iteration resolve flow result decision helper, rows-candidates-resolve flow result decision helper, rows-candidate resolve flow result decision helper, and rows-resolve flow result decision helper), and the corresponding unified flow resolvers now delegate final error/success result shaping through these helpers while preserving behavior/counters.
143. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k candidate-resolve and aggregate-candidate-resolve semantic branch orchestration were helperized in grouped flow extractions (candidate-resolve flow decision context builder, row-flow decision helper, ranked-flow decision helper, candidate-resolve flow result decision helper, unified candidate-resolve flow decision helper, aggregate-candidate-resolve flow decision context builder, eligibility-flow decision helper, values-flow decision helper, finalize-flow decision helper, aggregate-candidate-resolve flow result decision helper, and unified aggregate-candidate-resolve flow decision helper), and stage2ResolveFastTargetSharedPeerTopKCandidateResolveDecision + stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveDecision now orchestrate through these helper clusters while preserving behavior/counters.
144. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k shared-candidate apply execution/finalization and rows empty/finalization decision branches were helperized in grouped extractions (shared-candidate apply execution decision helper, shared-candidate apply finalize decision helper, rows empty-limit gate decision helper, rows empty-limit result decision helper, rows finalize gate decision helper, and rows finalize result decision helper), and stage2ResolveSharedPeerTopKCandidateApplyDecision + stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitDecision + stage2ResolveFastTargetSharedPeerTopKRowsFinalizeDecision now orchestrate through these helper clusters while preserving behavior/counters.
145. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k aggregate-apply semantic branch orchestration was helperized in grouped extractions (aggregate-apply rows-context gate decision helper, aggregate-apply rows-context result decision helper, aggregate-apply candidate gate decision helper, aggregate-apply candidate result decision helper, aggregate-apply finalize gate decision helper, and aggregate-apply finalize result decision helper), and stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextDecision + stage2ApplyFastTargetSharedPeerTopKAggregateApplyCandidateDecision + stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeDecision now orchestrate through these helper clusters while preserving behavior/counters.
146. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidates iteration semantic guard/result branch was helperized in grouped extractions (rows-candidates iteration gate decision helper and rows-candidates iteration result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationFlowDecision now orchestrates through this helper cluster while preserving behavior/counters.
147. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidate-iteration apply/result semantic branch orchestration was helperized in grouped extractions (rows-candidate-iteration apply gate decision helper, rows-candidate-iteration apply result decision helper, rows-candidate-iteration resolve-flow-result gate decision helper, and rows-candidate-iteration resolve-flow-result result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyFlowDecision + stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultDecision now orchestrate through these helper clusters while preserving behavior/counters.
148. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidates resolve-flow result semantic branch orchestration was helperized in grouped extractions (rows-candidates resolve-flow-result gate decision helper and rows-candidates resolve-flow-result result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
149. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidate resolve-flow result semantic branch orchestration was helperized in grouped extractions (rows-candidate resolve-flow-result gate decision helper and rows-candidate resolve-flow-result result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
150. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-resolve flow-result semantic branch orchestration was helperized in grouped extractions (rows-resolve flow-result gate decision helper and rows-resolve flow-result result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
151. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-resolve candidate-flow semantic branch orchestration was helperized in grouped extractions (rows-resolve candidate-flow gate decision helper and rows-resolve candidate-flow result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowDecision now orchestrates through this helper cluster while preserving behavior/counters.
152. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-resolve finalize-flow semantic branch orchestration was helperized in grouped extractions (rows-resolve finalize-flow gate decision helper and rows-resolve finalize-flow result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowDecision now orchestrates through this helper cluster while preserving behavior/counters.
153. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidate rows-flow semantic branch orchestration was helperized in grouped extractions (rows-candidate rows-flow gate decision helper and rows-candidate rows-flow result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowDecision now orchestrates through this helper cluster while preserving behavior/counters.
154. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidate apply-flow semantic branch orchestration was helperized in grouped extractions (rows-candidate apply-flow gate decision helper and rows-candidate apply-flow result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowDecision now orchestrates through this helper cluster while preserving behavior/counters.
155. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-candidate resolve-flow result semantic branch orchestration was helperized in grouped extractions (rows-candidate resolve-flow error result decision helper and rows-candidate resolve-flow success result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
156. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-resolve candidate-flow result semantic branch orchestration was helperized in grouped extractions (rows-resolve candidate-flow error result decision helper and rows-resolve candidate-flow success result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
157. Completed in latest slice (single full helperization chunk): stage2 fast-target-shared-peer top-k rows-resolve flow-result semantic branch orchestration was helperized in grouped extractions (rows-resolve flow error result decision helper and rows-resolve flow success result decision helper), and stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultResultDecision now orchestrates through this helper cluster while preserving behavior/counters.
158. Completed in latest slice (single large helperization pass): stage2 fast-target-shared-peer top-k rows resolve/candidates semantic branch orchestration was helperized across the remaining inline result/error-preserve/apply branches in one consolidated extraction (rows-candidate-iteration aggregate-flow result + error/success helpers, rows-candidate-iteration resolve-flow error/success result helpers, rows-candidates-iteration error/success result helpers, rows-candidates-resolve-flow error/success result helpers, rows-candidate-rows-flow error/success result helpers, rows-empty-limit apply/preserve result helpers, rows-finalize apply/preserve result helpers, and rows-finalize-flow apply/preserve result helpers), and the corresponding unified resolvers now delegate through these helper clusters while preserving behavior/counters.
159. TODO: QPO - non-helperization implementation backlog (helperization stream for this stage2 shared-peer top-k rows stack is closed; continue with remaining proposal tracks).
160. Completed in latest slice (G.2.2): Pebble stats persistence now covers multi-family histogram/selectivity materialization (numeric, datetime, boolean, and categorical), persists per-kind NDV/entry counters, and persists property-level freshness metadata (epoch, sample size, refresh timestamp) for both vertex and edge property stats.
161. Completed in latest slice (G.2.3 partial): runtime-native execution gate was expanded from CREATE+simple RETURN to a constrained CREATE+WITH+RETURN family (single-part CREATE with simple WITH projections and alias-forward RETURN, while retaining non-native fallback for WITH WHERE/distinct/order/pagination and richer write-pattern semantics) to remove executor-local delegation for this migrated family without regressing broader semantics.
162. Completed in latest slice (G.2.3 partial): CREATE+WITH WHERE+RETURN is now runtime-native for parser-safe/simple WHERE forms on WITH aliases (including comparator/string/null predicates and pure conjunction/disjunction forms), with escaped-quote WHERE shapes intentionally kept non-native to preserve existing unsupported behavior boundaries.
163. Completed in latest slice (G.2.3 partial): mixed/complex WITH WHERE boolean forms are now runtime-native for parser-safe expressions with AND/OR precedence and parenthesized grouping (including grouped string/null predicate combinations); parser-unsafe escaped-quote tokenization boundaries remain non-native by design.
164. Completed in latest slice (G.2.3 partial): escaped-quote WITH WHERE shapes are now runtime-native as well (single and double-quoted escaped-quote token text and compound predicates), and the temporary escaped-quote non-native gate guard has been removed.
165. Completed in latest slice (G.2.3 partial): residual executor-local WITH-WHERE row-filter/evaluator fallback branches that were no longer on the runtime execution path for migrated CREATE+WITH+RETURN shapes were removed from runtime_pipeline (including obsolete helper-only tests), leaving runtime-native behavior validated through branch-level runtime execution assertions and parser/gate coverage.
166. Completed in latest slice (G.2.4 partial): physical planner sort strategy selection is now deterministically stats-driven for ORDER BY + LIMIT shapes, choosing topk_heap when LIMIT is small relative to hinted edge cardinality and retaining in_memory_sort otherwise, with focused physical planner tests validating both branches.
167. Completed in latest slice (G.2.4 partial): physical planner undirected expand access-path selection is now deterministically stats-driven by edge-type degree hints, choosing inbound-first traversal when hinted out-degree is high and outbound-first otherwise (while keeping directed expand access paths unchanged), with focused physical planner tests validating both branches and directed-shape stability.
168. Completed in latest slice (G.2.4 partial): physical planner now emits deterministic joinStrategy annotations for chained MATCH expand nodes, selecting indexed_bind_join for lower-degree edge families and nested_loop_join for higher-degree families using stats hints, with focused physical planner tests covering both strategy branches.
169. Completed in latest slice (G.2.4 partial): physical planner now emits deterministic joinStrategy annotations for chained OPTIONAL expand nodes as well (including MATCH->OPTIONAL chains), using the same stats-driven low-degree/high-degree branch policy, and explain output coverage now asserts these optional access-path/strategy annotations are rendered.
170. Completed in latest slice (G.2.4 partial): expand planning choices are now promoted into an explicit physical variant field (including join-flavored variant suffixes for chained expands), and runtime expand execution now consumes this variant to drive undirected unbound binding orientation and chained indexed-join anchor selection behavior; focused runtime/physical/explain tests now cover both planner emission and runtime consumption paths.
171. Completed in latest slice (G.2.4 partial): sort planning choices are now promoted into explicit sort variants as well (sort_full vs sort_topk_heap), planner now computes an effective top-k window from pagination skip+limit when selecting topk_heap, and runtime sort execution now consumes the explicit variant/topK path to execute and trim the sorted window deterministically; focused planner/runtime/explain coverage now asserts variant emission, runtime consumption, and explain visibility.
172. Completed in latest slice (G.2.4 partial): anti-probe planning choices are now promoted into explicit anti-probe variants derived from stats-driven selectivity (batch-high vs row-low), and runtime anti-probe execution now consumes these variants to choose executable probe mode (batch probes vs per-row probes) deterministically; focused planner/runtime/explain coverage now asserts variant emission, runtime path selection, and explain visibility.
173. Completed in latest slice (G.2.4 partial): runtime engine now supports explicit variant-dispatchable operator handlers through a dedicated variant execution contract, and expand/optional-expand handlers now expose keyed variant executors that are invoked by engine dispatch when planner-emitted variant names are present (falling back to default handler execution otherwise), with focused runtime dispatch tests asserting variant-path invocation and default-path bypass semantics.
174. Completed in latest slice (G.2.4 partial): sort and anti-probe operators are now also promoted to explicit variant-dispatchable handlers with keyed variant executors (sort_full/sort_topk_heap and anti_probe_batch_high/anti_probe_row_low), so engine-level variant routing executes dedicated variant paths while preserving default fallback behavior; focused runtime dispatch tests now cover sort top-k variant dispatch and anti-probe row-probe variant dispatch (including row-vs-batch probe-path assertions).
175. Completed in latest slice (G.2.4 partial): runtime engine dispatch contract is now hardened to propagate operator failures deterministically (both variant-path and default-path errors now surface instead of being silently swallowed), while preserving explicit fallback semantics for unhandled variant names; focused runtime dispatch tests now assert unknown-variant default fallback plus error propagation behavior for both variant and default execution paths.
176. Completed in latest slice (G.2.4 partial): planner/runtime variant contract is now explicitly guarded by tests that validate planner-emitted variants against runtime operator-supported variant registries across expand, optional-expand, sort, and anti-probe families, reducing risk of silent planner/runtime drift and constraining fallback semantics to non-planner external plans.
177. Completed in latest slice (G.2.4 partial): runtime now supports strict variant-dispatch enforcement via execution param toggle, so unsupported variants for known variantized operator families can fail fast at execution time in strict mode while non-strict mode retains backward-compatible fallback behavior; focused runtime dispatch tests now assert strict reject semantics and non-strict fallback semantics for unsupported sort variants.
178. Completed in latest slice (G.2.4 partial): strict runtime variant-dispatch enforcement is now threaded through executor configuration (`executor.Options`) into runtime pipeline execution input, so callers can enable fail-fast contract checks without raw internal param injection; focused executor runtime-pipeline tests now assert strict-option rejection for unsupported variants and non-strict fallback compatibility.
179. Completed in latest slice (G.2.4 partial): strict variant-dispatch control is now exposed at the gRPC API boundary via `RequestOptions.strict_variant_dispatch` and propagated into executor/runtime params, with focused gRPC handler tests asserting request-option-to-param wiring and explicit false/true override behavior for strict dispatch mode.
180. Completed in latest slice (G.2.4 partial): strict variant-dispatch precedence semantics are now explicitly covered at executor runtime-pipeline level, verifying that per-request strict params override process-level executor defaults in both directions (strict-default can be disabled per request, non-strict default can be elevated per request), with focused tests using unsupported variant plans to assert deterministic reject/fallback outcomes.
181. Completed in latest slice (G.2.4 partial): gRPC request option mapping now sanitizes reserved strict-dispatch internal params by stripping any client-supplied `__ve_strict_variant_dispatch` value unless explicitly set via `RequestOptions.strict_variant_dispatch`, tightening API-boundary control and preventing raw internal-param injection from bypassing strict-mode policy wiring.
182. Completed in latest slice (G.2.4 partial): strict runtime variant-dispatch rejection is now scoped to known variantized operator families only (expand/optional-expand/sort/anti-probe), aligning enforcement behavior with the intended contract while preserving compatibility for non-variantized ops that may still carry a `variant` attr; focused runtime dispatch tests now assert both strict rejection for unsupported sort variants and non-rejection for non-variantized op families.
183. Completed in latest slice (G.2.4 partial): gRPC parameter boundary now rejects client-supplied reserved internal param keys with `__ve_` prefix during request decoding, ensuring strict variant-dispatch and other internal controls cannot be set through raw query parameters and must be expressed through explicit request options/configuration channels.
184. Completed in latest slice (G.2.4 partial): gRPC capabilities now explicitly advertise strict variant-dispatch support via a dedicated capability field, allowing clients to feature-detect strict contract enforcement availability before setting `RequestOptions.strict_variant_dispatch`; focused capabilities integration tests now assert this signal under both default and tuned server configurations.
185. Completed in latest slice (G.2.4 partial): end-to-end gRPC `Execute` integration coverage now explicitly validates reserved internal parameter-key rejection semantics (`__ve_*`) at the RPC boundary with `InvalidArgument` status verification, ensuring the reserved-key hardening is exercised through full transport decoding and handler execution flow instead of helper-only unit scope.
186. Completed in latest slice (G.2.4 partial): end-to-end gRPC `Explain` integration coverage now mirrors reserved internal parameter-key rejection semantics (`__ve_*`) with explicit `InvalidArgument` status assertions, ensuring boundary enforcement remains symmetric across both primary RPC entrypoints.
187. Completed in latest slice (G.2.4 partial): transport-level RPC matrix coverage now validates strict variant-dispatch request-option precedence across both `Execute` and `Explain` entrypoints (`nil` options pass through with no injected internal strict key, explicit `true`/`false` deterministically map to internal strict-dispatch param values), preserving executor-default control when unspecified while enforcing caller intent when specified.
188. Completed in latest slice (G.2.4 partial): gRPC transport tests now assert reserved internal `__ve_*` parameter keys are rejected before execution-hook invocation for both `Execute` and `Explain`, while valid requests in the same shape still propagate `RequestOptions.strict_variant_dispatch=true` into internal strict-dispatch params, proving rejection ordering and option propagation coexist without bypass paths.
189. Completed in latest slice (G.2.4 partial): gRPC `Execute` prepared-query transport matrix now validates version-mismatch short-circuit behavior and fallback-path strict-option propagation together (`AllowFallbackToCypher=false` keeps mismatch failures at `FailedPrecondition` without invoking execution, while `AllowFallbackToCypher=true` executes fallback and carries explicit `RequestOptions.strict_variant_dispatch` `true`/`false` values into internal strict-dispatch params), ensuring fallback compatibility and strict-dispatch precedence remain coherent on prepared inputs.
190. Completed in latest slice (G.2.4 partial): gRPC `Explain` prepared-query transport matrix now mirrors prepared version-mismatch and fallback strict-option semantics (`AllowFallbackToCypher=false` mismatch remains `FailedPrecondition` with no execution invocation, while `AllowFallbackToCypher=true` executes fallback under `EXPLAIN` and propagates explicit `RequestOptions.strict_variant_dispatch` `true`/`false` into internal strict-dispatch params), ensuring prepared-input boundary behavior is symmetric across both primary RPC entrypoints.
191. Completed in latest slice (G.2.4 partial): prepared-query fallback strict-dispatch transport checks for `Execute` and `Explain` are now consolidated through a shared matrix helper while preserving endpoint-specific assertions, reducing duplication and keeping the boundary contract easier to evolve without diverging test behavior across entrypoints.
192. Completed in latest slice (G.2.4 partial): prepared-query gRPC boundary tests now explicitly assert reserved internal `__ve_*` parameter-key rejection ordering across both `Execute` and `Explain` with compatible prepared payloads (rejected as `InvalidArgument` before execution hook invocation), while paired valid prepared requests continue to propagate `RequestOptions.strict_variant_dispatch=true` into internal strict-dispatch params, closing the prepared-input reserved-key enforcement surface symmetrically.
193. Completed in latest slice (G.2.4 partial): reserved internal-parameter rejection transport coverage for non-prepared and prepared request forms is now unified through a shared matrix helper, preserving endpoint-specific assertions (`Execute`/`Explain`, query shape expectations, and strict-option propagation checks) while reducing duplicated hook/server harness code to keep boundary hardening tests easier to maintain and extend.
194. Completed in latest slice (G.2.4 partial): matrix-style gRPC boundary tests now share a common server/client test harness helper for ephemeral listener startup, client connection creation, and cleanup sequencing, reducing repetitive setup code across reserved-key and prepared-fallback coverage while preserving all existing transport assertions and failure semantics.
195. Completed in latest slice (G.2.4 partial): gRPC matrix-test execution capture is now centralized through shared param-clone and capture helpers, eliminating repeated hook-local map-copy/append logic across strict-option, reserved-key, and prepared-fallback suites while preserving existing query-shape and strict-dispatch assertion semantics.
196. Completed in latest slice (G.2.4 partial): strict request-option precedence and reserved internal-parameter rejection transport assertions are now executed through a single canonical gRPC boundary matrix runner (shared endpoint/scenario tables and uniform capture/error checks), aligning these adjacent contract checks under one helper so future boundary cases can be added without splitting semantics across parallel test harnesses.
197. Completed in latest slice (G.2.4 partial): prepared-query fallback strict-dispatch transport coverage for both `Execute` and `Explain` now uses the same canonical boundary matrix runner shape (endpoint/scenario tables with uniform error and capture assertions), removing the last bespoke prepared-fallback harness while preserving mismatch short-circuit and fallback strict `true`/`false` propagation guarantees.
198. Completed in latest slice (G.2.4 partial): boundary-matrix wrapper tests now share a compact endpoint-table builder helper that centralizes `Execute`/`Explain` endpoint construction and expected-query derivation for cypher and prepared request forms, reducing repeated wrapper boilerplate while preserving existing scenario semantics and transport-level assertions.
199. Completed in latest slice (G.2.4 partial): boundary transport-matrix helper names and scenario type labels were normalized for readability and consistency (without behavioral changes), improving test intent clarity while keeping strict-option precedence, reserved-key rejection, and prepared fallback assertions on the same canonical execution path.
200. Completed in latest slice (G.2.4 partial): canonical boundary-matrix scenario labels were shortened and normalized into parallel naming (`strict-*`, `reserved-*`, `prepared-*`) for clearer CI/test output scanning, without changing any transport behavior or assertion coverage.
201. Completed in latest slice (G.2.4 partial): anti-probe planner selectivity now derives from persisted count stats as well as explicit hit-rate/avg-degree hints, reusing `EdgeTypeCounts` + `EdgeSourceCounts` to compute average out-degree when direct anti-probe hit-rate data is absent; this removes a remaining edge-type-name heuristic fallback in stats-backed plans and broadens deterministic stats-driven operator choice coverage for anti-probe ordering and variant selection.
202. Completed in latest slice (G.2.5 partial): stage2 hint-policy assembly now consumes an explicit planner-policy input that materializes query-shape classification (`predicateShapeEligible`, `predicateShapeDecisive`, and one-sided numeric range eligibility) before runtime policy derivation, removing another executor-local branch cluster from the live stage2 handoff while preserving row-dependent fallback for parameterized shapes and existing source-probe/pushdown policy behavior.
203. Completed in latest slice (G.2.5 partial): stage2 source-probe mode and wide non-range skip policy are now precomputed once via a shared source-probe policy input (coverage/degree resolution + shared/per-peer selection + wide-skip signal) and consumed by hint-policy assembly/row assessment, removing repeated executor-local decision recomputation across shared-probe, per-peer probe, and wide non-range gating paths while preserving existing behavior for scoped-source and missing-hint fallback cases.
204. Completed in latest slice (G.2.5 partial): stage2 index-pushdown eligibility now consumes a precomputed hint-policy input (resolved source-count/avg-degree selectivity) captured once during hint-policy assembly and threaded through eligibility decision contexts, eliminating repeated hint-selectivity recomputation across pushdown acceptance/rejection branches while preserving candidate-load guards, hard-cap guards, and unresolved-hint fallback behavior.
205. Completed in latest slice (G.2.5 partial): stage2 per-row pushdown gating and index-lookup cache preconditions now run through a single precomputed row input path (shared numeric-constraint resolution feeding predicate-shape eligibility, one-sided/numeric-range flags, wide non-range skip decision, and cache-key materialization), removing repeated per-row constraint/predicate/cache recomputation across collect-loop branches while preserving existing pushdown/caching semantics.
206. Completed in latest slice (G.2.5 partial): stage2 index-lookup cache-hit, candidate-decision, and finalize flow now consumes a single precomputed index-lookup flow input/context object (lookup key/scope, peer, probe limit, row/pattern/where, and policy), replacing scattered helper parameter threading across resolution steps while preserving cache semantics, candidate-load counters, and pushdown apply/finalize behavior.
207. Completed in latest slice (G.2.5 Slice 1): stage2 collect-loop index decision surface is now executed through one resolver path (`stage2ResolveCollectPeerEdgesDecision`) fed by a single collect input object, replacing inline predicate/range/wide-skip/caching/lookup/apply branches in the loop body and centralizing counter side-effects (predicate-shape skip, wide non-range skip, per-peer probe scope, and pushdown-applied rows) without changing index lookup semantics.
208. Completed in latest slice (G.2.5 Slice 2): stage2 policy construction now lifts remaining planner/operator contract signals into one precomputed operator-policy signal set (predicate-shape decisiveness/eligibility, one-sided range eligibility, source-probe mode, wide non-range skip, max pushdown candidates, and index-probe limit+tightening) that runtime policy methods consume directly, removing residual execution-time recomputation of these policy fields while preserving existing gating and counter semantics.
209. Completed in latest slice (G.2.5 Slice 3): legacy stage2 compatibility wrappers for source-probe/pushdown and row-assessment paths were removed from runtime policy execution in favor of contract-only inputs (`stage2BuildSourceProbePolicyInput`, `buildRowPushdownInput`, and precomputed operator-policy signals), and stage2 decision tests were rewired to assert the new contract surfaces directly; hard verification ran in one pass across focused stage2 executor tests and broad cypher package tests. Follow-up stabilization fixed the two executor unit regressions (`TestDeleteBindingSemanticsForReturn`, `TestExecutePercentileAggregatesRejectOutOfRangePercentile`), so focused stage2 and full executor package tests are now green; known remaining failures are in compliance/TCK parity and unsupported surface coverage under `internal/cypher/compliance`.
210. Completed in latest slice (G.3.2 partial): runtime-native CREATE now accepts supported temporal property constructors directly, so `localtime`, `time`, `localdatetime`, `datetime`, and `duration` property writes no longer force a semantic fallback for the migrated CREATE family; focused runtime and executor validation is green for this slice.
211. Completed in latest slice (G.3.2 partial): the vestigial executor-local query-route shim and its tests were removed, leaving runtime pipeline routing decisions to the existing execution-path gate instead of a separate dead policy helper.
212. Completed in latest slice (G.3.2 compliance hardening partial): CREATE/MERGE relationship-shape validation for directedness, single-type constraints, and CREATE variable-length prohibition is now enforced at parse-time (instead of runtime fallback/late execution paths), with focused parser regressions and targeted compliance subtests green for these syntax-surface cases.
213. Completed in latest slice (G.3.2 compliance hardening partial): ORDER BY undefined-variable validation now executes at parse-time for RETURN/WITH projection clauses (including out-of-scope and never-defined identifiers), so these cases fail as compile-time `UndefinedVariable` instead of succeeding or surfacing as late runtime unsupported errors; focused parser regressions and targeted compliance subtests are green.

#### G.2 Status Snapshot (pre-compliance triage)

Completed in G.2:

1. Graph store/runtime adapter cleanup is completed for migrated families, including broad contract cleanup and legacy shim removal in this stream.
2. Pebble primitive expansion is completed for the planned G.2 scope, including histogram/selectivity persistence and property freshness metadata.
3. Runtime operator coverage completion target for G.2 is met, including removal of residual executor-local semantic branches in migrated families.
4. Planner hardening for deterministic stats-driven choices is substantially advanced and includes deterministic sort, expand direction, anti-probe, and join strategy pathways now emitted/consumed through variant contracts.
5. Stage2 decision-surface consolidation is completed through slices 202-209, including contract lift, legacy path removal, and hard verification.

Known remaining in G.3:

1. Compliance/TCK parity and unsupported-surface hardening remain the primary open workstream.

#### G.3 Remaining implementation slices
1. Completed: Graph store and runtime storage adapter cleanup.
1.a. Completed: Finish broad contract cleanup and remove legacy compatibility shims, see line 435 of this proposal
2. Completed: Pebble primitives and stats persistence expansion.
2.a. Completed: histogram/selectivity family persistence and property freshness metadata persistence are now implemented.
3. Completed: Runtime operator coverage completion.
3.a. Completed: Remove residual executor-local semantic branches in migrated families, see line 445 of this proposal.
4. Completed: Planner hardening.
4.a. Completed: broaden deterministic stats-driven operator choices across join, traversal, and sort variants, and keep explain/profile parity hardening in lockstep.
5. Completed: Fast-path and pushdown write completion for stage2 decision-surface consolidation.
5.a. Completed: migrated residual stage2 executor-local decisions into planner/operator-native representations and removed redundant stage2 heuristic plumbing in slices 202-209.
6. Completed: Routing policy flip for migrated CREATE-family coverage.
6.a. Completed: runtime-native CREATE temporal property constructors now execute without executor fallback.
6.b. Completed: removed the dead query-route helper/test pair from the executor package.
6.c. Completed: removed the last executor-local semantic fallback dependencies for migrated families and flipped default routing safely to operator-native for this migrated family.

## Validation Gates

1. Compliance:
   - go test ./internal/cypher/compliance/... -count=1
2. Runtime operators:
   - go test ./internal/cypher/runtime/... -count=1
3. Logical/physical planner:
   - go test ./internal/cypher/logical/... ./internal/cypher/physical/... -count=1
4. End-to-end cypher:
   - go test ./internal/cypher/... -count=1
5. Benchmarks:
   - recommendation, friend suggestion, top-k, merge ingest, empty-set traversal scenarios
6. Explain/profile guardrails:
   - estimated vs actual rows visibility, operator counters, access-path annotations

Current status: validation gates are green; G.3.2 now includes native temporal CREATE handling, dead route-shim removal, and routing-policy cleanup for migrated CREATE-family coverage. Remaining work is compliance/TCK parity and broader unsupported-surface closure.

## Exit Criteria

This proposal is complete when all are true:

1. Supported query families execute through operator-native runtime with no shared-executor semantic delegation.
2. Empty-set relationship pruning is active and covered by deterministic tests.
3. Pebble primitives required by physical operators are implemented and used.
4. Statistics drive plan choices and appear in explain/profile outputs.
5. Compliance and benchmark gates pass with no benchmark-specific executor fast paths.

## Recommendation

Proceed with this proposal as the successor execution plan after GRAPH_PIPELINE closure. It is the minimal architecture needed to remove the remaining non-test remnant and make VitalEdge a fully operator-native graph engine end-to-end, including storage-aware planning and execution.