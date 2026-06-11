# Cypher id Semantics in VitalEdge

## Why this document exists

Recent compliance work showed repeated fragility around id behavior across MATCH, WITH, comparison, and write-binding flows. This document establishes:

1. The intended Cypher-facing semantics for id-related behavior.
2. Where that behavior is currently implemented in code.
3. The known deltas versus TCK-aligned behavior.
4. A staged plan to reconcile implementation and compliance.

## Terminology

1. Stored property id: A normal property key named id on a vertex or edge, accessed through property lookup like a.id or r.id.
2. Entity identity: The runtime/storage identity of a bound vertex or edge (for example, the identity used for variable rebinding and entity equality).
3. Synthetic binding keys: Internal row keys such as var and var.id that are used by runtime binding and joins.

## Intended Cypher-facing semantics

1. Property access semantics:
   a.id and r.id are property lookups first.
   If a stored property named id exists, that value must be returned.

2. Entity equality semantics:
   a = b compares entity identity, not property maps.
   This should hold across hydrated entities and entity-shaped maps.

3. Internal binding visibility:
   Synthetic runtime keys are implementation details.
   They must not leak into property semantics in a way that changes query results.

4. Numeric comparison/equality semantics:
   Integer-vs-integer comparisons should remain exact and avoid float precision loss.

## Current implementation map

Primary runtime file:

1. [internal/cypher/runtime/operators/default_handlers.go](internal/cypher/runtime/operators/default_handlers.go)

id/property resolution and binding:

1. [resolveProjectionPropertyChain](internal/cypher/runtime/operators/default_handlers.go#L6259)
2. [resolveProjectionBoundEntityProperty](internal/cypher/runtime/operators/default_handlers.go#L6482)
3. [resolveBoundEntityID](internal/cypher/runtime/operators/default_handlers.go#L2035)
4. [resolveBoundEntityIDFromValue](internal/cypher/runtime/operators/default_handlers.go#L2059)
5. [bindPatternVar](internal/cypher/runtime/operators/default_handlers.go#L2187)
6. [bindWriteVariable](internal/cypher/runtime/operators/default_handlers.go#L14075)
7. [resolveWriteEntityID](internal/cypher/runtime/operators/default_handlers.go#L14121)
8. [vertexHasExpectedProperties](internal/cypher/runtime/operators/default_handlers.go#L1205)

comparison and identity/equality logic:

1. [applyProjectionComparisonOp](internal/cypher/runtime/operators/default_handlers.go#L5525)
2. [applyProjectionOrderingComparisonOp](internal/cypher/runtime/operators/default_handlers.go#L5548)
3. [projectionValuesEqual](internal/cypher/runtime/operators/default_handlers.go#L5671)
4. [projectionValuesEqualTernary](internal/cypher/runtime/operators/default_handlers.go#L5816)
5. [projectionCompareNumericValues](internal/cypher/runtime/operators/default_handlers.go#L5745)
6. [projectionValueEntityID](internal/cypher/runtime/operators/default_handlers.go#L5773)

local regression tests covering this slice:

1. [TestExecuteWithForwardedScalarJoinUsesStoredIDProperty](internal/cypher/executor/executor_test.go#L7834)
2. [TestExecuteComparisonLargeIntegerEqualityIsExact](internal/cypher/executor/executor_test.go#L7867)
3. [TestExecuteComparisonNodeIdentityAfterWith](internal/cypher/executor/executor_test.go#L7894)

## Known deltas versus compliant behavior

Observed failing scenarios during this slice:

1. WITH forwarded-property join:
   [With2 scenario 1](.cache/opencypher/tck/features/clauses/with/With2.feature#L34)
2. WITH SKIP/LIMIT dependency forwarding:
   [WithSkipLimit2 scenario 2](.cache/opencypher/tck/features/clauses/with-skip-limit/WithSkipLimit2.feature#L84)
3. Comparison semantics still open (non-id-adjacent but in same comparison tranche):
   [Comparison1 scenario 14](.cache/opencypher/tck/features/expressions/comparison/Comparison1.feature#L276)
   [Comparison2 scenario 5](.cache/opencypher/tck/features/expressions/comparison/Comparison2.feature#L121)

Root risk pattern:

1. The runtime currently uses var.id both as:
   a) synthetic identity binding, and
   b) potential property projection surface.
2. Depending on clause flow, this can cause property lookup and entity identity to shadow each other in unintended ways.

## Reconciliation strategy

### Phase 1: Make id source explicit in-row

Introduce a strict separation of sources in runtime rows:

1. Keep synthetic entity identity in reserved internal keys only.
2. Keep projected/stored properties in separate keys.
3. Remove ambiguity where property resolution reads synthetic identity by fallback.

Acceptance criteria:

1. With2 and WithSkipLimit2 scenarios pass consistently.
2. No regression in existing executor tests listed above.

### Phase 2: Centralize property lookup policy

Consolidate property semantics behind one resolver policy used by:

1. dot access,
2. map-style property access,
3. write-time property predicates.

Policy rule:

1. stored property id wins for property access;
2. entity identity is only consulted through explicit identity channels.

Acceptance criteria:

1. No duplicated id special-casing across unrelated helpers.
2. Property lookup behavior is invariant across WITH forwarding and hydration states.

### Phase 3: Lock equality/ordering contracts

1. Preserve entity identity equality behavior for entity operands.
2. Preserve exact integer comparisons.
3. Complete remaining comparison semantics (path equality direction-insensitive behavior and NaN ordering behavior) without reintroducing id shadowing.

Acceptance criteria:

1. Targeted comparison scenarios pass, including current open failures.

### Phase 4: Guardrail tests and documentation sync

1. Expand executor tests to explicitly cover:
   id property access from hydrated and non-hydrated rows,
   WITH alias forwarding and rebind,
   mixed entity-map and entity-pointer equality.
2. Keep this document and [CYPHER.md](CYPHER.md) synchronized when semantics change.

## Operational checklist for future id-related slices

Before merge:

1. Run targeted executor tests for this area.
2. Run TCK subsets:
   clauses/with,
   clauses/with-skip-limit,
   expressions/comparison.
3. Confirm no new overlap regressions in MERGE/CREATE write-binding behavior.

After merge:

1. Update this document if any id policy changed.
2. Record the key lesson in repo memory for the next slice.

## Semantic invariants (developer guardrails)

These invariants should be treated as non-negotiable unless the Cypher compatibility contract is intentionally changed.

1. Property access and entity identity are distinct channels.
   Dot access (for example `a.id`) is property semantics.
   Identity semantics should be expressed via explicit identity channels (for example `id(a)` and entity equality).

2. Synthetic bindings are internal, not language semantics.
   Internal keys (for example `var.id`) may exist for execution, joins, and rebinding.
   They must not silently redefine query-visible property behavior.

3. CREATE and MERGE require separate materialization discipline.
   CREATE often needs same-clause forwarding of newly specified property values.
   MERGE must preserve match-or-create semantics and avoid identity reassignment side effects.

4. OPTIONAL MATCH miss handling must preserve prior bindings.
   Optional expansion may null optional outputs, but must not erase already-bound variables unrelated to the miss.

5. DELETE vs DETACH DELETE behavior is part of the semantic contract.
   Tests and runtime behavior must agree on connected-entity deletion semantics, not rely on implicit store behavior.

6. Numeric comparison correctness must prioritize exact integer paths.
   Integer-vs-integer comparisons should avoid lossy float coercion when exact comparison is possible.

7. Expression precedence is evaluator-critical, not parser-only.
   Operator precedence and null/boolean precedence must be asserted in evaluator tests even when parsing succeeds.

8. Test names must follow semantic intent.
   When semantics change, rename tests to match the new meaning so regressions are diagnosed from names alone.

## TCK triage playbook (fast regression workflow)

Use this workflow for future compliance regressions in this area.

1. Reproduce narrowly first.
   Run only the failing scenario names to get fast iteration and stable diagnostics.

2. Classify before patching.
   Assign each failure to one semantic class:
   property-vs-identity,
   write materialization,
   optional null-row behavior,
   expression precedence,
   delete semantics,
   numeric comparison.

3. Minimize scope of change.
   Patch only the resolver/materializer path for that class.
   Avoid cross-cutting fallback changes until a class-specific fix is proven.

4. Verify with representative canaries.
   For id-related regressions, always run:
   WITH forwarding cases,
   CREATE/MERGE property-id projection cases,
   one large-integer comparison case.

5. Run package-wide compliance before broad suite runs.
   Confirm `TestCypherCompliance` passes end-to-end before running wider unit and integration suites.

6. Lock the fix with targeted regression tests.
   Add or adjust tests that encode the intended behavior and remove tests that enforced invalid historical assumptions.

7. Capture the learning immediately.
   Update this document and repository memory while context is fresh to reduce rediscovery cost.
