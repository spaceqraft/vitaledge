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
