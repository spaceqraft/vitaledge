# VitalEdge

VitalEdge is a Go project that is building Cypher parsing support with a strict parser-first architecture.

## Cypher Status

Cypher support is documented in [CYPHER.md](CYPHER.md), including:

- compliance and coverage statement
- high-level feature table
- current guarantees and limitations

Quick link: [Cypher Coverage and Compliance](CYPHER.md)

## Architecture Decisions

- [Property Graph Design](DESIGN.md)
- [Property Graph Implementation Plan](IMPLEMENTATION_PLAN.md)
- [Graph Keyspace Specification](GRAPH_KEYSPACE.md)

## Repository Layout

- [cmd/vitaledge/main.go](cmd/vitaledge/main.go): TCP server entrypoint
- [internal/tcp/protocol.go](internal/tcp/protocol.go): protocol-to-parser bridge
- [internal/cypher/grammar/Cypher.g4](internal/cypher/grammar/Cypher.g4): pinned openCypher grammar source
- [internal/cypher/parser/parser.go](internal/cypher/parser/parser.go): parse entrypoints
- [internal/cypher/ast/ast.go](internal/cypher/ast/ast.go): typed AST models

## Development Notes

- Grammar is pinned to openCypher M23 in source control.
- Parser generation target: `make generate-cypher-parser`
- Test command: `go test ./...`
- Compliance suite (official openCypher TCK via godog): `make cypher-compliance`
- Compliance run + summarized gap report: `make cypher-compliance-report`
- Summarize last compliance run log: `make cypher-compliance-summary`
- Smoke benchmark: `make bench-smoke`
- Graph store benchmark baseline: `make bench-graph-store`
- Milestone benchmark baseline (local JSONL snapshot): `make bench-milestone`

`vitaledge-bench` supports repeatability-oriented flags:

- `-iterations N`: operation count for the scenario loop.
- `-seed-size N`: explicit seed graph size override for seeded scenarios (`research`, `rebac`).
- `-json`: machine-readable output for baseline capture.

## Startup Index Schema Config

The server can load configuration-based index DDL at startup:

- flag: `--index-schema-config /path/to/indexes.json`
- env: `VITALEDGE_INDEX_SCHEMA_CONFIG=/path/to/indexes.json`

If both are provided, the flag value is used.

Graph store and tenant defaults are configurable at startup:

- flag: `--graph-path data/graph.db`
- env: `VITALEDGE_GRAPH_PATH=data/graph.db`
- flag: `--tenant default`
- env: `VITALEDGE_DEFAULT_TENANT=default`

Index recommendation metrics logging is enabled by default and configurable:

- flag: `--metrics-report-interval 30s`
- env: `VITALEDGE_METRICS_REPORT_INTERVAL=30s`

If set to `0`, periodic recommendation logging is disabled.

## EXPLAIN For Index Tuning

Use `EXPLAIN` when you want to inspect how VitalEdge would plan a query without mutating data. The output is returned as JSON in the `explain` column, and it includes the query shape, `query.options`, logical and physical plans, influencer counts, cardinality estimates, and index decisions.

Example:

```cypher
EXPLAIN MATCH (n:Person {name: $name}) RETURN DISTINCT n.name AS name ORDER BY name ASC SKIP 1 LIMIT $maxLimit
```

What to look at:

- `query.options`: captures projection modifiers such as `distinct`, `orderBy`, `skip`, and `limit`.
- `influencers.nodeCounts`: shows label counts observed in the current graph snapshot.
- `influencers.edgeCounts`: shows edge-type counts that may affect traversal choices.
- `influencers.predicateSignals`: highlights predicate clauses and the number of matching rows or vertices.
- `indexDecisions`: reports candidate indexes, whether one was selected, chosen access path, estimated scan savings/selectivity, and recommendation (`keep-index`, `create-index`, or `consider-index`).
- `cardinality`: shows per-plan-node row estimates and their quality (`exact`, `estimate`, or `sample`).
- `warnings`: emits fallback diagnostics (for example missing index, full-scan fallback, and estimate-only tuning signals) to highlight when planning signals are partial.

Warning codes currently emitted:

- `WRITE_QUERY_DRY_RUN`: write clauses were detected but EXPLAIN performed no mutations.
- `MISSING_TENANT_CONTEXT`: tenant was not supplied, so influencer stats are from an empty snapshot.
- `FULL_SCAN_FALLBACK`: planner selected an all-nodes scan access path.
- `MISSING_PROPERTY_INDEX`: a property predicate has no selected property index.
- `ESTIMATE_ONLY_INDEX_SIGNAL`: index recommendation is based on estimate-quality signals (for example unbound parameters).
- `PLAN_ANALYSIS_PARTIAL`: catch-all fallback when no more specific diagnostics apply.

Index-decision interpretation:

- `recommendation=keep-index`: planner selected an existing property index.
- `recommendation=create-index`: high-impact missing-index candidate.
- `recommendation=consider-index`: medium-impact candidate or estimate-quality signal.
- `recommendation=optional-index`: low-impact candidate.

This is the recommended entry point when deciding whether a property should be indexed or when checking whether an existing index is actually being chosen by the planner.

## TCP Query Execution

TCP messages are treated as Cypher statements and executed by the server.
Each response is emitted as one JSON line:

```json
{"ok":true,"columns":["dstID"],"rows":[{"dstID":"g1"}],"stats":{"rowsReturned":1,"durationMs":0}}
```

On errors:

```json
{"ok":false,"error":"..."}
```

Example config:

```json
{
	"property_indexes": [
		{ "tenant": "acme", "schema": "User", "property": "email" },
		{ "tenant": "acme", "schema": "Device", "property": "serial" }
	]
}
```
