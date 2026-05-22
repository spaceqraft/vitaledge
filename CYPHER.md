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

## Current Gaps

- Clause-specific semantic AST for all clauses is not complete yet.
- Cross-clause semantic validation is not complete yet.
- Execution semantics are intentionally outside parser scope.

## Near-Term Direction

- Expand from clause-kind and raw clause payloads into fully typed clause internals.
- Increase semantic validation coverage while preserving strict error reporting.
- Keep parser and execution layers cleanly separated.

Back to [README.md](README.md).
