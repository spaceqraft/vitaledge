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

Prometheus metrics endpoint is optional and configurable:

- flag: `--metrics-listen :9100`
- env: `VITALEDGE_METRICS_LISTEN=:9100`

When enabled, the process serves:

- `GET /metrics` (Prometheus text exposition)
- `GET /healthz` (simple liveness check)

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

## Prometheus and Grafana Baseline

Baseline observability assets are published under `tools/observability`:

- Prometheus scrape config: `tools/observability/prometheus.yml`
- Docker Compose stack: `tools/observability/docker-compose.yml`
- Grafana dashboard: `tools/observability/grafana/vitaledge-overview.json`

One-command local stack (Prometheus + Grafana):

```bash
make observability-up
```

Then open:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (default login: `admin` / `admin`)

To stop the stack:

```bash
make observability-down
```

Quick start:

1. Start VitalEdge with metrics endpoint enabled:

```bash
go run ./cmd/vitaledge --metrics-listen :9100
```

2. Run Prometheus with the provided config:

```bash
prometheus --config.file=tools/observability/prometheus.yml
```

3. In Grafana, add your Prometheus datasource and import `tools/observability/grafana/vitaledge-overview.json`.

Dashboard coverage includes:

- statement throughput and average statement duration,
- rows-returned rate,
- index lookup outcomes,
- top unindexed index-candidate observations.

### Reproducible Manual Tuning Examples

Use this loop for each query family:

1. Run baseline `EXPLAIN`.
2. Capture `indexDecisions`, `costEstimate`, `runtimeStats`, and `warnings`.
3. Apply index change (or parameter-binding change for estimate-quality signals).
4. Re-run `EXPLAIN` and compare the same fields.

Suggested comparison view:

```json
{
	"indexDecisions": "selected/recommendation/quality/accessPath",
	"costEstimate": "value + components",
	"runtimeStats": "index(candidates/selected/missing), cardinality(rowsRead/rowsOutput)",
	"warnings": "fallback and missing-index signals"
}
```

Example 1: Point lookup on missing index -> create index

Before:

```cypher
EXPLAIN MATCH (n:Person {email: $email}) RETURN n.id AS id
```

Observed baseline signals:

- `indexDecisions[*].selected=false`
- `indexDecisions[*].recommendation=create-index` or `consider-index`
- warning includes `MISSING_PROPERTY_INDEX`
- `runtimeStats.index.missing > 0`

After adding property index (`Person.email`) and re-running EXPLAIN:

- `indexDecisions[*].selected=true`
- `indexDecisions[*].recommendation=keep-index`
- scan node `accessPath=property_index`
- `runtimeStats.index.selected` increases and `runtimeStats.index.missing` drops
- `costEstimate.value` drops sharply on the same graph snapshot

Example 2: Estimate-quality signal -> exact-quality signal via parameter binding

Before (parameter omitted in EXPLAIN invocation):

```cypher
EXPLAIN MATCH (n:Device {serial: $serial}) RETURN n
```

Observed baseline signals:

- index decision `quality=estimate`
- access path can remain non-indexed (`label(Device)`) when parameter is unbound
- selectivity and index quality are less actionable than the bound-parameter case

After re-running with bound parameter values:

- index decision `quality=exact`
- access path shifts to `property_index(Device.serial)`
- `matchedCount`, `estimatedSelectivity`, and cardinality fields become actionable for tuning

Example 3: Traversal entry-point tuning

Before:

```cypher
EXPLAIN MATCH (u:User {region: $region})-[:MEMBER_OF]->(g:Group) RETURN g.id AS gid
```

Observed baseline signals when `User.region` is not indexed:

- scan operator uses label access (`label(User)`)
- index decision is unselected and `runtimeStats.index.missing > 0`
- warning includes `MISSING_PROPERTY_INDEX`

After adding property index (`User.region`) and re-running EXPLAIN:

- index decision becomes selected
- `accessPath=property_index(User.region)` on index-decision entry
- `runtimeStats.index.missing` drops to zero
- `costEstimate.value` drops materially on the same data snapshot

Local evidence snapshot (deterministic fixture run):

| Example | Before (selected / access / quality / missing / cost) | After (selected / access / quality / missing / cost) |
| --- | --- | --- |
| 1. Person email lookup | `false / label(Person) / exact / 1 / 102` | `true / property_index(Person.email) / exact / 0 / 2` |
| 2. Device serial parameter binding | `true / label(Device) / estimate / 0 / 2` | `true / property_index(Device.serial) / exact / 0 / 2` |
| 3. User region traversal entry-point | `false / label(User) / exact / 1 / 22` | `true / property_index(User.region) / exact / 0 / 2` |

Notes for reproducibility:

- Keep the same dataset snapshot when comparing before/after plans.
- Compare one change at a time (single index or single parameter-binding change).
- For config-backed indexes, update the index schema config and restart the server before the after-run.

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
