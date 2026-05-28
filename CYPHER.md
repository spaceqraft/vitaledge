# Cypher Coverage and Compliance

## Compliance Statement

Current VitalEdge Cypher support is best described as:

- Grammar compliance: High for parsing shape, using pinned openCypher M23 grammar.
- Typed AST depth: Partial.
- Semantic/execution compliance: Limited at this stage.

In practical terms, the parser accepts Cypher according to the imported grammar and produces typed statement/clause structures, but most clause internals are still represented as raw clause text instead of fully typed semantic nodes.

## High-Level Coverage Table

| Area | Coverage Level | Current Status | Notes |
| --- | --- | --- | --- |
| Grammar version | Full (pinned) | openCypher M23 pinned in repo | Source: [internal/cypher/grammar/Cypher.g4](internal/cypher/grammar/Cypher.g4) |
| Syntax parsing engine | High | ANTLR-generated parser in use | Generated package: [internal/cypher/grammar/generated](internal/cypher/grammar/generated) |
| Statement batching (`;`) | Full | Supported | Entry: [internal/cypher/parser/parser.go](internal/cypher/parser/parser.go) |
| Query statements | High | Supported | Includes regular queries and unions |
| Standalone CALL statements | High | Supported | Mapped to dedicated statement type |
| UNION / UNION ALL | High | Supported | Captured as union boundaries in AST |
| Reading clauses | High | MATCH, OPTIONAL MATCH, UNWIND, in-query CALL supported | Mapped to clause kinds |
| Updating clauses | High | CREATE, MERGE, DELETE, SET, REMOVE supported | Mapped to clause kinds |
| WITH and RETURN clauses | High | Supported | RETURN also has legacy detailed mapping path |
| Parameters (`$name`, `$1`) | High | Supported | Collected from parse tree |
| Keyword normalization | Full | Supported through normalized enum kinds | Identifier casing remains source-preserved |
| Detailed typed clause internals | Partial | Limited | Most clauses currently stored as raw clause text |
| Expression semantic typing | Partial | Limited | Legacy MATCH/WHERE/RETURN path has deeper structure |
| Semantic validation beyond grammar | Limited | Minimal | Primarily grammar-level validation today |
| Query execution/planning compliance | Not yet | Not implemented in parser layer | Execution is intentionally separate |

## Current Guarantees

- Fail-fast parse behavior for syntax errors with statement and location context.
- Deterministic parser behavior tied to a pinned grammar artifact.
- Typed statement-level and clause-kind-level AST output for broad query shapes.

## Implemented Built-in Functions

Current execution support includes the main Cypher built-in function families that are implemented in the executor layer:

- String and scalar helpers such as `lower`, `upper`, `trim`, `replace`, `toString`, `toInteger`, and `valueType`.
- Mathematical helpers such as `floor`, `round`, `exp`, `log`/`ln`, `log10`, `e`, `pi`, and the trigonometric family.
- List and predicate helpers such as `size`, `range`, `reduce`, `all`, `any`, `none`, `single`, `isEmpty`, and list conversion helpers.
- Temporal helpers such as `date`, `time`, `datetime`, `duration`, and the supported alias and namespace forms.
- Spatial helpers such as `point`, `distance`, `point.distance`, and `point.withinBBox`.
- Vector helpers such as `vector`, `vector.similarity.cosine`, `vector.similarity.euclidean`, `vector_distance`, `vector_dimension_count`, and `vector_norm`.

## Current Gaps

- Clause-specific semantic AST for all clauses is not complete yet.
- Cross-clause semantic validation is not complete yet.
- Execution semantics are intentionally outside parser scope.

## Near-Term Direction

- Expand from clause-kind and raw clause payloads into fully typed clause internals.
- Increase semantic validation coverage while preserving strict error reporting.
- Keep parser and execution layers cleanly separated.

## Team Compliance Workflow

The openCypher TCK integration is designed to be repeatable and team-friendly.

Primary commands:

- `make cypher-compliance`: fetch official TCK M23 and run full suite.
- `make cypher-compliance-report`: run full suite, save run log, and print grouped failure summary.
- `make cypher-compliance-summary`: summarize the most recent compliance log.

Generated artifacts:

- TCK cache and features: `.cache/opencypher/tck`
- Last compliance log: `.cache/opencypher/cypher-compliance.log`

Failure interpretation model:

- `step is undefined`: TCK step phrase is not yet implemented in VitalEdge godog bindings.
- `UNSUPPORTED: ...`: parser/executor explicitly rejects a language feature; this is a concrete implementation gap.
- semantic category mismatches (for example expected compile-time vs runtime): error taxonomy mapping needs refinement.

Team operating rhythm:

1. Run `make cypher-compliance-report` before merging Cypher execution changes.
2. Capture top categories from the summary output and map them to implementation work items.
3. Re-run the same command after the change to confirm net reduction in failure categories.
4. Keep unsupported behavior explicit; do not hide or skip failing TCK scenarios.

Back to [README.md](README.md).
