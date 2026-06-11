# VitalEdge Proposal: Stronger Typed Graph

## Status

Draft proposal.

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