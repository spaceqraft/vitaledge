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
- Smoke benchmark: `make bench-smoke`
