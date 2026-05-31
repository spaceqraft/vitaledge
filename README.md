# VitalEdge

VitalEdge is a Go project that is building Cypher parsing support with a strict parser-first architecture.

## Cypher Status

Cypher support is documented in [CYPHER.md](CYPHER.md), including:

- compliance and coverage statement
- high-level feature table
- current guarantees and limitations

That status document also summarizes the implemented built-in function surface, including string, math, list, predicate/scalar, temporal, spatial, and vector families.

Quick link: [Cypher Coverage and Compliance](CYPHER.md)

## Architecture Decisions

- [Property Graph Design](DESIGN.md)
- [Property Graph Implementation Plan](IMPLEMENTATION_PLAN.md)
- [Graph Keyspace Specification](GRAPH_KEYSPACE.md)

## Repository Layout

- [cmd/vitaledge/main.go](cmd/vitaledge/main.go): gRPC server entrypoint
- [cmd/vitaledge-cli/main.go](cmd/vitaledge-cli/main.go): gRPC interactive CLI/client entrypoint
- [cmd/vitaledge/grpc_server.go](cmd/vitaledge/grpc_server.go): QueryService gRPC handler and transport wiring
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
- Cypher ingest benchmark (indexed vs non-indexed UNWIND..MERGE): `make bench-merge-index`
- Milestone benchmark baseline (local JSONL snapshot): `make bench-milestone`
- Mixed CLI soak profile (concurrent write/noop-write/read): `make soak-mixed`

`vitaledge-bench` supports repeatability-oriented flags:

- `-iterations N`: operation count for the scenario loop.
- `-seed-size N`: explicit seed graph size override for seeded scenarios (`research`, `rebac`).
- `-json`: machine-readable output for baseline capture.

## Startup Index Schema Config

The server can load configuration-based index DDL at startup:

- flag: `--index-schema-config /path/to/indexes.json`
- env: `VITALEDGE_INDEX_SCHEMA_CONFIG=/path/to/indexes.json`

If both are provided, the flag value is used.

Property indexes can also be created at runtime through gRPC index DDL:

- RPC: `CreatePropertyIndex(CreatePropertyIndexRequest)`
- Request fields: `tenant`, `schema`, `property`, `if_not_exists`
- Response fields: `created`, `indexed_entities`

Runtime index DDL backfills existing vertices for the requested `(tenant, schema, property)` tuple so index-backed lookups become available immediately.

Graph store and tenant defaults are configurable at startup:

- flag: `--graph-path data/graph.db`
- env: `VITALEDGE_GRAPH_PATH=data/graph.db`
- flag: `--tenant default`
- env: `VITALEDGE_DEFAULT_TENANT=default`

Maximum write transaction batch size is configurable at startup:

- flag: `--max-write-batch-bytes 67108864`
- env: `VITALEDGE_MAX_WRITE_BATCH_BYTES=67108864`

Go runtime memory ceiling is configurable at startup:

- flag: `--go-memory-limit-bytes 0`
- env: `VITALEDGE_GO_MEMORY_LIMIT_BYTES=0`

Set to a positive value to apply a Go soft memory limit (in bytes). `0` disables this override.

Pebble memory controls are configurable at startup:

- flag: `--pebble-block-cache-bytes 0`
- env: `VITALEDGE_PEBBLE_BLOCK_CACHE_BYTES=0`
- flag: `--pebble-memtable-size-bytes 0`
- env: `VITALEDGE_PEBBLE_MEMTABLE_SIZE_BYTES=0`
- flag: `--pebble-memtable-stop-writes-threshold 0`
- env: `VITALEDGE_PEBBLE_MEMTABLE_STOP_WRITES_THRESHOLD=0`

Each value uses Pebble defaults when set to `0`.

The value must be greater than `0`. Oversized write transactions are rejected with an invalid-input error instead of triggering a Pebble panic.

The configured value is also exposed through gRPC capabilities as `max_write_batch_bytes`, so SDK clients can chunk write-heavy workloads (for example, bulk `UNWIND ... MERGE` ingest) before execution.

Prometheus metrics endpoint is optional and configurable:

- flag: `--metrics-listen :9100`
- env: `VITALEDGE_METRICS_LISTEN=:9100`

gRPC query endpoint is enabled by default:

- flag: `--grpc-listen :7443`
- env: `VITALEDGE_GRPC_LISTEN=:7443`

When enabled, the process serves:

- `GET /metrics` (Prometheus text exposition)
- `GET /healthz` (simple liveness check)

## EXPLAIN For Index Tuning

Use `EXPLAIN` when you want to inspect how VitalEdge would plan a query without mutating data. The output is returned as JSON in the `explain` column, and it includes the query shape, `query.options`, logical and physical plans, influencer counts, cardinality estimates, and index decisions.

Example:

```cypher
EXPLAIN MATCH (n:Person {name: $name}) RETURN DISTINCT n.name AS name ORDER BY name ASC SKIP 1 LIMIT $maxLimit
```

What to look at:

- `query.options`: captures projection modifiers such as `distinct`, `orderBy`, `skip`, and `limit`.
- `influencers.vertexCounts`: shows label counts observed in the current graph snapshot.
- `influencers.edgeCounts`: shows edge-type counts that may affect traversal choices.
- `influencers.totals`: shows tenant-level vertex and edge totals used by planner heuristics.
- `influencers.predicateSignals`: highlights predicate clauses and the number of matching rows or vertices.
- `indexDecisions`: reports candidate indexes, whether one was selected, chosen access path, estimated scan savings/selectivity, and recommendation (`keep-index`, `create-index`, or `consider-index`).
- `cardinality`: shows per-plan-vertex row estimates and their quality (`exact`, `estimate`, or `sample`).
- `warnings`: emits fallback diagnostics (for example missing index, full-scan fallback, and estimate-only tuning signals) to highlight when planning signals are partial.

Warning codes currently emitted:

- `WRITE_QUERY_DRY_RUN`: write clauses were detected but EXPLAIN performed no mutations.
- `MISSING_TENANT_CONTEXT`: tenant was not supplied, so influencer stats are from an empty snapshot.
- `FULL_SCAN_FALLBACK`: planner selected an all-vertices scan access path.
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
- top unindexed index-candidate observations,
- host CPU, memory, and network I/O signals,
- Go runtime and GC behavior (goroutines, heap allocation, GC pause/cycles).

### Benchmark: UNWIND..MERGE Index Tuning

Use this benchmark to compare batch ingest behavior with and without property indexes on:

- `Movie.movie_id`
- `User.user_id`
- `Genre.genre`

Run:

```bash
make bench-merge-index
```

The benchmark executes a representative three-step ingest workload:

1. `UNWIND $movies ... MERGE (mov:Movie {movie_id: ...})`
2. `UNWIND $pairs ... MATCH (mov:Movie {movie_id: ...}) MERGE (g:Genre {genre: ...})`
3. `UNWIND $ratings ... MERGE (u:User {user_id: ...}) ... MATCH (mov:Movie {movie_id: ...})`

Interpretation guidance:

- `with_property_indexes` should show higher `rows/s` and lower `ns/op` than `without_property_indexes` as graph size grows.
- If the gap is small, verify index DDL and backfill for the target tenant/schema/property tuples.
- Pair this benchmark with Prometheus/Grafana panels for `vitaledge_executor_index_lookups_total` and `vitaledge_executor_unindexed_candidate_observations` to confirm runtime index usage.

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
- scan vertex `accessPath=property_index`
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

## gRPC CLI

Build and run the CLI against the gRPC endpoint:

```bash
make build
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme
```

Interactive commands:

- `SET name=<scalar>`: set or update a client-side variable (`null`, booleans, numbers, or quoted strings).
- `SET` (or `LIST`/`VARS`): list all variables.
- `UNSET name`: remove one variable.
- `:quit` / `:exit`: leave the CLI.

Execution behavior:

- Statements are sent only after local parse-completeness checks pass.
- Variable bindings are applied client-side from `$name` placeholders.
- Multiline statements are buffered until complete (for example `MATCH` + `WHERE` + `RETURN`).
- Result sets are rendered in a capped-width table:
	- column width is computed from the column header and scanned row values,
	- width is capped by `--max-column-width` (default `80`).
- Returned graph values use Cypher-like rendering:
	- vertexes: `(id:Label {"k":"v"})` (auto-generated ids suppressed, first label shown),
	- edges: `[id:TYPE {"k":"v"}]` (internal auto-generated composite ids suppressed),
	- paths: vertex/edge chains with directionality (`->`, `<-`).
- Each request prints execution stats (`rows`, `durationMs`).

Common CLI flags:

- `--grpc-target 127.0.0.1:7443`
- `--tenant acme`
- `--timeout 5s`
- `--max-column-width 80`
- `--execute "<cypher>"` for one-shot mode.

Soak/load modes (deterministic, configurable, and suitable for running multiple CLI processes):

- `--load-mode write`: alternates `CREATE` and `DETACH DELETE` with locally tracked ids (equal create/delete counts).
- `--load-mode noop-write`: repeatedly `CREATE`s the same vertex id.
- `--load-mode read`: repeatedly runs `MATCH p=(a)-[*N]-(b) RETURN p LIMIT <k>` with deterministic hop selection.

Load flags:

- `--load-ops 1000`
- `--load-seed 1`
- `--load-prefix soak`
- `--load-read-min-hop 1`
- `--load-read-max-hop 3`
- `--load-read-limit 25`
- `--load-report-each 100`

Example soak invocations:

```bash
# Balanced write churn (create/delete pairs).
./bin/vitaledge-cli --tenant acme --load-mode write --load-ops 20000 --load-prefix writer-a --load-seed 7

# No-op write pressure.
./bin/vitaledge-cli --tenant acme --load-mode noop-write --load-ops 50000 --load-prefix noop-a --load-seed 7

# Read pressure with variable path hop counts.
./bin/vitaledge-cli --tenant acme --load-mode read --load-ops 20000 --load-read-min-hop 1 --load-read-max-hop 4 --load-read-limit 50 --load-seed 11
```

One-shot mode:

```bash
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "MATCH (n:Seed) RETURN n.id AS id"
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
