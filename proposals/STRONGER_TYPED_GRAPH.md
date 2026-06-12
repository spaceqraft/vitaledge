# VitalEdge Proposal: Stronger Typed Graph

## Status

Completed core proposal. Track 6 optional type constraints are deferred to a separate proposal.

### Track Completion Snapshot (2026-06-11)

- [x] **Track 1: Typed Value Codec Foundation**
  - [x] Scalar codecs (null, bool, int64, float64, string, bytes)
  - [x] Container codecs (list, map)
  - [x] Temporal scalar wrappers (Date, LocalTime, LocalDateTime, ZonedDateTime, Duration)
  - [x] Round-trip and deterministic encoding tests
  - [x] Fuzz tests for decode robustness

- [x] **Track 2: Vertex and Edge Property Persistence Migration**
  - [x] Typed property storage for vertex and edge records
  - [x] Type-tag and encoded-value persistence
  - [x] Typed decode in read APIs
  - [x] Property round-trip compatibility at API level

- [x] **Track 3: Typed Property Index Keys**
  - [x] Type-tag segment in property index key layout
  - [x] Equality probes across typed values
  - [x] Range probes for numeric/temporal domains
  - [x] Mixed-type property index tests

- [x] **Track 4: Stats and Planner Type Awareness**
  - [x] EXPLAIN diagnostic payload metadata (comprehensive runtimeStats enrichment)
    - [x] runtimeStats.operators section surfacing typed DISTINCT/GROUP/SORT opportunities
    - [x] Scalar-shape rationale for typed DISTINCT/SORT candidates
    - [x] Blocked-reason counters for non-typed DISTINCT/SORT/GROUP
    - [x] Representative blocked-expression samples per fallback reason
    - [x] Per-family operator summaries (typed_eligible, fallback_likely, mixed_domain_risk)
    - [x] Higher-level operatorAssessment block with query-shape classification
    - [x] Operator-assessment recommendations and focus-family hints
    - [x] Mixed-domain and fallback signals in EXPLAIN warning stream
    - [x] Warning severity and priority metadata
    - [x] Deterministic warning sort order
    - [x] warningSummary block with severity totals and highest-priority metadata
    - [x] Warning-summary category rollups (planner, index, operator, general)
    - [x] diagnosticPosture summary with classification and recommendations
    - [x] Diagnostic-posture confidence field (high/medium/low)
    - [x] Diagnostic-posture rationale string
    - [x] Diagnostic-posture score and scoreBand fields
    - [x] Category-weight hooks for score adjustments
    - [x] scoreBreakdown components (base, confidence, penalty, final)
    - [x] trendHint field (stable/watch/degrading) with trendScore (-1/0/+1)
    - [x] trendEvidence block for audit trail
    - [x] scoreComputationConfig with all policy metadata externalized
    - [x] ruleReasonCatalog for policy traceability
    - [x] evaluatedPolicy with decisionTrace timeline
    - [x] Reason catalog consistency validation
    - [x] Contract hash and component fingerprints
    - [x] Contract compatibility validation metadata
    - [x] Governance verdict metadata with remediation semantics
    - [x] Governance-remediation baseline drift tracking
  - [x] Runtime compliance fixes
    - [x] Runtime CREATE preserves labels and properties on newly created vertexes
    - [x] Multi-pattern CREATE materializes sequential write bindings per segment
    - [x] Same-clause CREATE property-id semantics remain stable
    - [x] Runtime ORDER BY handles NaN as distinct numeric sentinel
    - [x] Runtime comma-separated CREATE strips inline `//` comments
    - [x] Broader regression confirmation passed full openCypher compliance suite
  - [x] Executor regression verification
    - [x] Triadic selection blocker cleared (CREATE-seeded pattern matching)
    - [x] ORDER BY / WITH dependency compliance cluster cleared
    - [x] Executor-only recommendation pushdown restored (fast-path counters)
    - [x] EXPLAIN runtime operator payload parity restored (shape/reason maps normalized)
    - [x] Vertex-WHERE candidate extraction supports deterministic top-level OR
    - [x] EXPLAIN/PROFILE parity for disjunctive same-property WHERE alternatives
    - [x] Full test suite passes with zero regressions

- [x] **Track 5: Runtime Typed Scalar Path (High-Impact Operators)**
  - [x] Typed scalar DISTINCT/aggregate/sort fast lanes with counters
  - [x] Typed scalar extraction helpers on vertex and edge property read paths
  - [x] Multi-column scalar row keying (single and multi-column cases)
  - [x] Optional-expand survivor/dedupe optimizations
  - [x] Correctness coverage for count(DISTINCT *) on scalar multi-column rows
  - [x] Aggregate micro-benchmarks for count/collect DISTINCT paths
  - [x] Scalar multi-column ORDER BY correctness coverage and sort-path benchmarks
  - [x] Runtime counters exposing typed-vs-fallback usage
  - [x] Measurable allocation reduction and performance gains vs generic paths

- [ ] **Track 6: Optional Type Constraints** is deferred to a separate proposal.

### Completion Gate Status

- [x] Planner uses type-domain stats with deterministic behavior under test
- [x] Explain/profile fully attributes typed planner choices and fallbacks
- [x] No regressions in existing Cypher correctness/compliance and executor explain contract tests
- [x] Full codebase test suite passes end-to-end (compliance 111s, executor 46s, all packages green)
- [x] **Performance Hardening & Edge-Case Coverage (complete)**
  - [x] Baseline benchmarks collected for typed vs fallback operator paths
  - [x] Key findings: Typed DISTINCT row key ~26% faster, 82% less memory vs generic stable key
  - [x] Ingest with typed indexes 2.8x faster than non-indexed (56.58 vs 20 rows/s)
  - [x] Recommendation queries stable ~45-47ms with ~285-290k allocs (neutral performance)
  - [x] CPU profiling identified `loadEdgeTypeAndEndpoints` as 26% hotspot in recommendation queries
  - [x] DISTINCT operator refined measurements: 47.6% faster, 60% less memory vs stable path
  - [x] Edge-case validation: all DISTINCT/aggregate/ORDER BY tests pass (including null/empty/mixed-type, temporal ordering, and list/map heterogeneity plus deep-nesting scenarios) with no regressions
  - [x] Compliance suite validation completed (full suite passing)
  - [x] Typed codec decode performance validation vs legacy JSON (benchmark complete: typed decode 3x-15x faster for bool/int/float with 0-1 allocs vs JSON 2-4 allocs; string path is slower but 50 B/op vs 208 B/op)
  - [x] Collect-DISTINCT shape performance on larger graphs (benchmark complete: 10k vertexes 0.80s @ 12.4k rows/s, 104.8 MB alloc; 25k vertexes 2.08s @ 12.0k rows/s, 258.3 MB alloc)
  - [x] Optimization implementation: edge type/endpoint caching (implemented in Pebble tx read path; recommendation benchmark improved from 45.65 ms to 36.53 ms, allocations from 15.47 MB to 13.14 MB, allocs/op from 290k to 230k)
  - [x] Profiling-driven ORDER BY allocation reduction (simple reference fast path in runtime sort key extraction: sort benchmark improved from 506 us to 53 us, bytes/op from 74.2 KB to 14.9 KB, allocs/op from 4498 to 146)
  - [x] Profiling-driven DISTINCT/stable key allocation reduction (replaced JSON pair serialization fallback, removed per-column temporary typed key strings in row-key generation, switched key sorting to stack-backed small buffers, and added simple aggregate-arg direct lookup for DISTINCT aggregates: projectionDistinctRowKey improved from 359.7 ns to 241.7 ns, 160 B/op to 64 B/op, allocs/op 6 to 1; stableRowKey improved from 859.6 ns to 244.0 ns, 376 B/op to 96 B/op, allocs/op 9 to 1; collect(DISTINCT x) benchmark improved from 4463 ns to 493 ns, 968 B/op to 144 B/op, allocs/op 68 to 8; count(DISTINCT *) benchmark improved from 1592 ns to 1319 ns, 704 B/op to 584 B/op, allocs/op 16 to 11)

## Motivation

VitalEdge currently pays substantial execution overhead by repeatedly materializing and transforming dynamic values through generic containers. This is especially visible in traversal + aggregation workloads where values pass through map[string]any and []any at multiple stages.

The team has confirmed there are no production deployments yet. That allows a stronger storage and execution contract, including destructive migration if needed.

At the same time, we want to preserve the user experience of property graphs in systems like Memgraph and Neo4j:

1. Users should not be required to predeclare property schemas to write data.
2. Property type should be inferred at write time from the value being set.
3. Typed storage should then become authoritative for planning and execution.

## Goals

1. Make property values strongly typed at storage boundaries.
2. Keep property type inference automatic on write (no mandatory DDL for basic use).
3. Reduce runtime dynamic-type overhead in scans, expansion, filtering, ordering, grouping, and aggregation.
4. Enable typed operator fast paths that are shape-based, not query-specific.
5. Keep Cypher behavior compatible for supported surface area.

## Non-Goals

1. Introducing mandatory user-defined property schemas for all labels and edge types.
2. Solving distributed execution in this proposal.
3. Replacing Pebble.

## Problem Summary

Today, major costs come from:

1. Repeated decode-normalize-convert flows for properties.
2. Generic map/list row structures carrying mixed values between operators.
3. Distinct/group key generation via generic normalization and serialization paths.
4. Limited planner certainty about property domains and comparability.

These are architectural costs, not just micro-optimization opportunities.

## Proposal Overview

Adopt a stronger typed graph contract with three pillars:

1. Typed property encoding in storage records.
2. Runtime type inference and enforcement on write.
3. Optional schema constraints as an additive feature, not a prerequisite.

This combines flexible ingestion with predictable typed execution.

## Typed Property Model

### Type Set (Phase 1)

Start with a practical subset already exercised by current workloads:

1. Null (absence semantics)
2. Boolean
3. Int64
4. Float64
5. String
6. Bytes
7. List (homogeneous preferred, heterogeneous allowed in Phase 1 for compatibility)
8. Map (string key -> typed value)
9. Temporal scalar family represented by normalized internal forms:
   1. Date
   2. LocalTime
   3. LocalDateTime
   4. ZonedDateTime
   5. Duration

Notes:

1. Enum and Point can be added in a follow-up once typed index and comparator behavior is in place.
2. Temporal values should have canonical encoded forms for direct compare/sort where Cypher allows.

### Write-Time Type Inference

On CREATE/SET/MERGE property assignment:

1. Infer type from incoming value.
2. Encode value with a compact typed format.
3. Persist the typed tag with the encoded payload.
4. Record type observation metadata for planner/statistics.

No pre-registration is required for this step.

### Type Evolution Policy

For a given property key across entities, multiple observed types may exist. We handle this with a staged policy:

1. Phase 1 default: allow multi-typed properties; store exact per-value type tags.
2. Phase 2 optional guard: support strict type constraints via schema rules.
3. Planner can still use dominant-type stats with safe fallback when mixed types appear.

## Storage Design

### Record Encoding

Replace opaque JSON-oriented property payload handling with typed cell encoding:

1. Property entry = property key + type tag + encoded value bytes.
2. Encodings are canonical and stable for equality and deterministic hashing.
3. Numeric and temporal encodings are ordered where feasible for index range scans.

### Property Index Encoding

Index keys should include type domain explicitly:

1. index-prefix / tenant / schema / property / type-tag / encoded-value / entity-id

Benefits:

1. Fast equality and range operations without runtime decode for key ordering.
2. Type-aware probe behavior.
3. Cleaner mixed-type semantics.

### Statistics Extensions

Maintain per property:

1. observed type histogram
2. ndv by type
3. null/absent rates
4. optional min/max for ordered scalar domains

This feeds physical planning decisions.

## Runtime and Operator Impact

### Immediate Runtime Benefits

1. Lower decode overhead for hot properties used in joins/grouping/order.
2. Faster distinct/group keys for scalar typed values.
3. Reduced fallback serialization in aggregation.

### Operator Contract Upgrade

Introduce typed value carriers internally:

1. scalar slots for common primitive projections
2. typed vector-like group buffers for aggregation-heavy operators
3. fallback dynamic row only when required by query shape

This does not need a one-shot rewrite. It can be introduced operator by operator.

### Query Shape Optimization Alignment

This proposal directly supports shape-based optimization. For example:

1. MATCH with typed edge expansion
2. RETURN src.prop, collect(DISTINCT dst.prop)
3. ORDER BY src.prop

can run with type-specialized grouping and sort paths when src.prop/dst.prop are scalar and comparable.

## Cypher Semantics

### Compatibility Targets

1. Property assignment remains dynamic from user perspective.
2. Null handling remains equivalent to property absence semantics where applicable.
3. Comparisons across incompatible domains remain defined by existing Cypher semantics (or rejected where current behavior rejects).

### Mixed-Type Cases

When a property appears with mixed types:

1. Execution remains correct using type-aware compare/equality rules.
2. Planner may choose conservative physical paths.
3. Optional schema constraints can prevent future drift.

## Optional Schema Layer (Additive)

After typed storage is in place, add optional schema DDL:

1. Enforce property type for label/property or edgeType/property pairs.
2. Validate writes and reject violations.
3. Enable stronger planner assumptions and cheaper operator paths.

Important: this is opt-in and not required to benefit from typed storage.

## Migration Plan (Development Mode)

Given there are no production deployments, we can use a destructive migration strategy.

### Migration Strategy

1. Perform a destructive cutover to the typed graph format.
2. Delete and recreate local databases when switching to the typed implementation branch.
3. Reseed benchmark and compliance fixtures.

### Why Destructive Migration Is Appropriate Here

1. Simplifies implementation and avoids dual-format complexity.
2. Removes risk from partial backward-compatibility shims.
3. Speeds iteration on storage and operator contracts.

## Rollout Plan

### Phase A: Typed Storage Foundation

1. Add typed value encoder/decoder package.
2. Update vertex and edge property persistence to use typed cells.
3. Update property index writers/readers to include type tag.

### Phase B: Planner and Stats Awareness

1. Record per-property type histograms.
2. Extend selectivity and cardinality logic to use type domains.
3. Add explain/profile fields for typed index decisions.

### Phase C: Typed Runtime Paths

1. Add typed scalar extraction APIs in graph tx interfaces.
2. Convert high-impact operators first:
   1. projection
   2. distinct
   3. aggregate
   4. sort
3. Keep dynamic fallback for unsupported expression shapes.

### Phase D: Optional Type Constraints

1. Add DDL for property type constraints.
2. Add write-time validation.
3. Add migration tools for coercion/audit where useful.

## Risks and Mitigations

1. Risk: mixed-type properties produce surprising ordering/equality behavior.
   Mitigation: codify and test type-precedence rules; expose planner/runtime counters for mixed-type fallback.

2. Risk: temporal and complex types increase encoder complexity.
   Mitigation: phase them in after scalar foundation; keep canonical encode tests exhaustive.

3. Risk: broad code touch across store and executor.
   Mitigation: phase rollout; perform destructive cutover; keep old behavior out of new typed implementation path to avoid branching spread.

4. Risk: benchmark regressions during transition.
   Mitigation: keep slope and shape guardrails; add typed-path benchmarks and counters.

## Testing Strategy

1. Unit tests for typed encode/decode across all supported values.
2. Property index tests for equality/range semantics per type.
3. Query correctness tests for mixed-type and null behavior.
4. Performance guardrails:
   1. ingest slope
   2. suggest-friends slope
   3. collect-distinct shape benchmarks

## Success Criteria

1. Typed graph storage path is the default in the active dev branch after cutover.
2. Existing compliance and targeted runtime tests pass after reseed/reset.
3. Measurable reduction in CPU time for collect-distinct and grouped read workloads.
4. Reduced allocation pressure in runtime hot paths.
5. Fewer query-specific fast paths needed as typed operators become general.

## Concrete Implementation Task Breakdown

This section is the immediate execution plan to start implementation now.

### Track 1: Typed Value Codec Foundation

1. Add typed value package for encode/decode and canonical ordering bytes.
2. Implement scalar codecs first: null, bool, int64, float64, string, bytes.
3. Add container codecs for list and map using recursive typed encoding.
4. Add temporal scalar wrappers with canonical internal bytes.

Suggested package targets:

1. internal/graph/store/typedvalue (new)
2. internal/graph/types (extensions if needed)

Deliverables:

1. Encode(value) -> (typeTag, encodedBytes)
2. Decode(typeTag, encodedBytes) -> value
3. Compare(typeTagA, bytesA, typeTagB, bytesB) for ordered domains

Acceptance criteria:

1. Round-trip tests for all supported types.
2. Deterministic encoding tests (same input, same bytes).
3. Fuzz tests for decode robustness.

### Track 2: Vertex and Edge Property Persistence Migration

1. Replace JSON-style property payload persistence with typed cells.
2. Update PutVertex, PutEdge, and property mutation paths.
3. Keep external graph API behavior unchanged at call boundary.

Suggested touchpoints:

1. internal/graph/store package property read/write paths
2. internal/graph/keyspace key templates for typed property payloads

Deliverables:

1. Typed property storage for vertex and edge records.
2. Backed-by-typed decode in read APIs.

Acceptance criteria:

1. Existing graph store tests pass with new encoding.
2. Property round-trip compatibility at API level.

### Track 3: Typed Property Index Keys

1. Add type-tag segment to property index key layout.
2. Update index write, delete, and seek logic to use typed encoded value bytes.
3. Preserve existing planner lookup APIs while adding typed-aware internals.

Suggested touchpoints:

1. internal/graph/keyspace index key builders
2. internal/graph/store index maintenance paths
3. internal/cypher/executor index lookup helpers

Deliverables:

1. New index key format with explicit type domain.
2. Equality probes working across typed values.
3. Range probes for numeric/time domains where comparator is defined.

Acceptance criteria:

1. Existing property index tests updated and passing.
2. New tests for mixed-type property indexes and probe behavior.

### Track 4: Stats and Planner Type Awareness

1. Extend stats collection with per-property type histogram and NDV by type.
2. Add planner hooks to prefer typed index seeks when type domain is known.
3. Emit explain/profile annotations for typed decisions and mixed-type fallback.

Suggested touchpoints:

1. internal/graph/stats
2. internal/cypher/executor/explain and planning helpers

Deliverables:

1. Type-aware stats persisted and queryable by planner.
2. Explain output includes typed seek/fallback rationale.

Acceptance criteria:

1. Explain tests updated to cover typed seek path.
2. No regression in non-typed/fallback query plans.

### Track 5: Runtime Typed Scalar Path (High-Impact Operators)

1. Add typed scalar extraction APIs from tx/entity read paths.
2. Implement typed fast lanes for:
   1. projection
   2. distinct
   3. aggregate collect/count
   4. order by scalar
3. Keep dynamic fallback for complex expressions.

Suggested touchpoints:

1. internal/cypher/executor/runtime_pipeline.go
2. internal/cypher/runtime/operators/default_handlers.go

Deliverables:

1. Typed scalar key path without generic normalize+serialize in hot loop.
2. Runtime counters for typed path usage and fallback usage.

Acceptance criteria:

1. Existing correctness tests pass.
2. New typed collect-distinct shape tests pass.
3. Measurable allocation drop in micro-benchmarks for collect-distinct workloads.

### Track 6: Optional Type Constraints (After Core Typed Path)

1. Add DDL for property type constraints by label/property and edgeType/property.
2. Add write-time enforcement.
3. Add error messaging and explain diagnostics for constraint violations.

Deliverables:

1. Constraint metadata storage.
2. Enforcement hooks in create/set/merge paths.

Acceptance criteria:

1. Constraint lifecycle tests pass.
2. Existing writes without constraints remain unaffected.

## Execution Order and Gates

Recommended order:

1. Track 1
2. Track 2
3. Track 3
4. Track 4 and Track 5 in parallel
5. Track 6

Hard gates:

1. Do not start Track 3 before Track 1 codec contract is frozen.
2. Do not start Track 5 typed runtime paths before Track 2 read semantics are stable.
3. Do not switch the branch to typed-by-default storage until Tracks 2 and 3 pass full test suite on a freshly recreated DB.

## First PR Slice (Recommended)

To start immediately with low coordination overhead, make the first PR limited to:

1. Track 1 scalar codecs only (null, bool, int64, float64, string, bytes).
2. Unit tests and fuzz tests for scalar codec round-trip and deterministic encoding.

Definition of done for first PR:

1. Scalar codec package merged with tests.
2. No behavior changes yet to query execution path.

## Questions

1. Should lists/maps be constrained to homogeneous scalar types in Phase 1 for indexability, or only in optional schema mode?
  * Due to TCK (and real-world use cases) including heterogenous lists/maps, lists/maps should be heterogenous.
2. Which temporal encodings should support ordered index operations in the first iteration?
   1. Date - support ordered
   2. LocalTime - support ordered
   3. LocalDateTime - support ordered
   4. ZonedDateTime - support ordered
   5. Duration - need not support ordered
3. Should value coercion be allowed in optional schema constraints, or always reject mismatches?
  * Reject mismatches
4. Should we expose type statistics via CALL/SHOW APIs during development for debugging planner decisions?
  * not necessary