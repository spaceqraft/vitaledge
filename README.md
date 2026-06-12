# VitalEdge

VitalEdge is a property graph database focused on three primary use cases:

1. Research and dataset loading.
2. Threat and anomaly detection over structured logs.
3. ReBAC at the edge.

## Why VitalEdge

- Local-first runtime with low operational overhead (single binary + embedded storage).
- Cypher-oriented query workflow with explicit compliance tracking.
- Focus on practical graph workloads: research datasets, threat/anomaly analysis, and ReBAC-style relationship queries.
- Performance and explainability emphasis through benchmarks, EXPLAIN output, and metrics.

## Current Status

- Engine shape: single-node production profile with distributed-ready architecture direction.
- Query language: implemented Cypher surface documented in [docs/architecture/CYPHER.md](docs/architecture/CYPHER.md).
- Compliance: openCypher TCK-driven testing is part of normal development.
- Runtime interfaces: gRPC server and CLI are both available today.

## Quick Start

### Prerequisites

- Go 1.22+
- POSIX-compatible shell

### Build

```bash
make build
```

This produces:

- `bin/vitaledge` (server)
- `bin/vitaledge-cli` (interactive and one-shot CLI)
- `bin/vitaledge-bench` (benchmark harness)

### Run the server

```bash
./bin/vitaledge --metrics-listen :9100
```

Default gRPC listen address is `:7443`.

### Connect with the CLI

```bash
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant default
```

One-shot example:

```bash
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant default --execute "MATCH (v) RETURN labels(v), count(labels(v))"
```

## Common Development Commands

```bash
make run
make test
make cypher-compliance
make bench-smoke
make soak-mixed
```

## Index Configuration (Startup)

VitalEdge can load property-index definitions during startup.

- Flag: `--index-schema-config /path/to/indexes.json`
- Environment variable: `VITALEDGE_INDEX_SCHEMA_CONFIG=/path/to/indexes.json`

Example:

```json
{
  "property_indexes": [
    { "tenant": "acme", "schema": "User", "property": "email" }
  ],
  "edge_property_indexes": [
    { "tenant": "acme", "edge_type": "RATED", "property": "rating" }
  ]
}
```

For operational details and index tuning guidance, see [docs/architecture/DESIGN.md](docs/architecture/DESIGN.md).

## Index Configuration via vitaledge-cli DDL

In addition to startup configuration, you can create and drop indexes via Cypher `CALL` statements through `vitaledge-cli`.

```bash
# Vertex property index
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "CALL db.index.createProperty('User', 'email') YIELD created, indexedEntities RETURN created, indexedEntities"

# Idempotent create
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "CALL db.index.createProperty('User', 'email', true) YIELD created, indexedEntities RETURN created, indexedEntities"

# Edge property index
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "CALL db.index.createEdgeProperty('RATED', 'rating') YIELD created, indexedEntities RETURN created, indexedEntities"

# Drop indexes
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "CALL db.index.dropProperty('User', 'email', true) YIELD dropped, deletedEntities RETURN dropped, deletedEntities"
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme --execute "CALL db.index.dropEdgeProperty('RATED', 'rating', true) YIELD dropped, deletedEntities RETURN dropped, deletedEntities"
```

For asynchronous vertex/edge index lifecycle visibility:

```bash
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme
```

```text
vitaledge> CALL db.index.edgeBuildJobs() YIELD tenant, edgeType, property, pending, indexedEdges RETURN tenant, edgeType, property, pending, indexedEdges
vitaledge> CALL db.index.processEdgeBuildJobs() YIELD processed, pending RETURN processed, pending
vitaledge> CALL db.index.restartEdgePropertyBuild('RATED', 'rating') YIELD enqueued RETURN enqueued
vitaledge> CALL db.index.propertyBuildJobs() YIELD tenant, schema, property, pending, indexedEntities RETURN tenant, schema, property, pending, indexedEntities
vitaledge> CALL db.index.processPropertyBuildJobs() YIELD processed, pending RETURN processed, pending
vitaledge> CALL db.index.restartPropertyBuild('User', 'email') YIELD enqueued RETURN enqueued
```

Both `db.index.createProperty` and `db.index.createEdgeProperty` enqueue durable background build jobs and return quickly. A newly enqueued job reports `created=true` and `indexedEntities=0`.

## Index Configuration via gRPC

The `QueryService` gRPC API currently provides a dedicated index DDL RPC for vertex property indexes:

- `CreatePropertyIndex(CreatePropertyIndexRequest)`

Example with `grpcurl`:

```bash
grpcurl -plaintext \
  -import-path . \
  -proto api/proto/vitaledge/v1/query.proto \
  -d '{"tenant":"acme","schema":"User","property":"email","ifNotExists":true}' \
  127.0.0.1:7443 \
  vitaledge.v1.QueryService/CreatePropertyIndex
```

`CreatePropertyIndex` follows the same asynchronous behavior as CLI DDL and returns immediately after enqueueing a durable backfill job.

You can also invoke index DDL procedures through `Execute` (Cypher over gRPC):

```bash
grpcurl -plaintext \
  -import-path . \
  -proto api/proto/vitaledge/v1/query.proto \
  -d '{"tenant":"acme","input":{"cypher":"CALL db.index.createProperty(\"User\", \"email\", true) YIELD created, indexedEntities RETURN created, indexedEntities"}}' \
  127.0.0.1:7443 \
  vitaledge.v1.QueryService/Execute
```

## Minimal End-to-End Query Example (vitaledge-cli)

This flow creates a tiny graph, configures an index, runs a query, and then runs `EXPLAIN` on the same query shape.

```bash
./bin/vitaledge-cli --grpc-target 127.0.0.1:7443 --tenant acme
```

```text
vitaledge> CALL db.index.createProperty('User', 'region', true) YIELD created, indexedEntities RETURN created, indexedEntities
vitaledge> CREATE (u1:User {user_id:'u1', name:'Alice', region:'west'}), (u2:User {user_id:'u2', name:'Bob', region:'east'}), (g:Group {group_id:'g1', name:'Blue'}), (u1)-[:MEMBER_OF]->(g), (u2)-[:MEMBER_OF]->(g)
vitaledge> MATCH (u:User {region:'west'})-[:MEMBER_OF]->(g:Group) RETURN u.name AS user, g.name AS grp ORDER BY user
vitaledge> EXPLAIN MATCH (u:User {region:'west'})-[:MEMBER_OF]->(g:Group) RETURN u.name AS user, g.name AS grp ORDER BY user
```

`EXPLAIN` returns a JSON payload in the `explain` column; this is the fastest way to inspect access paths and index recommendations before and after index changes.

## Observability

VitalEdge exposes Prometheus metrics and ships a baseline Grafana/Prometheus setup.

- Stack files: [tools/observability](tools/observability)
- Bring up stack: `make observability-up`
- Tear down stack: `make observability-down`

## Documentation Map

- Graph design and architecture decisions: [docs/architecture/DESIGN.md](docs/architecture/DESIGN.md)
- Graph keyspace proposal: [proposals/GRAPH_KEYSPACE.md](proposals/GRAPH_KEYSPACE.md)
- Cypher support and guarantees: [docs/architecture/CYPHER.md](docs/architecture/CYPHER.md)
- Cypher ID semantics: [docs/architecture/CYPHER_ID_SEMANTICS.md](docs/architecture/CYPHER_ID_SEMANTICS.md)
- Implementation planning context: [docs/architecture/IMPLEMENTATION_PLAN.md](docs/architecture/IMPLEMENTATION_PLAN.md)

## Repository Layout

- `cmd/vitaledge`: gRPC server entrypoint
- `cmd/vitaledge-cli`: CLI entrypoint
- `cmd/vitaledge-bench`: benchmark binary
- `internal/cypher`: parser, planner, and execution pipeline
- `internal/graph`: storage abstractions and graph store implementations
- `api/proto`: protobuf API definitions

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) and sign the CLA before your first merged contribution.

## License

See [LICENSE](LICENSE).
