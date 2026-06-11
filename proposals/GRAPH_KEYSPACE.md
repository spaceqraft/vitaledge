# Graph Keyspace Specification (Phase 0)

This document defines the initial key encoding contracts for the property graph storage layer.

## Prefixes

- Vertex records: v
- Edge records: e
- Out adjacency index: a/out
- In adjacency index: a/in
- Property index: i

## Key Templates

- Vertex: v/{tenant}/{vertexId}
- Edge: e/{tenant}/{edgeId}
- Out adjacency: a/out/{tenant}/{srcId}/{edgeType}/{edgeId}
- In adjacency: a/in/{tenant}/{dstId}/{edgeType}/{edgeId}
- Property index: i/{tenant}/{schema}/{property}/{hex(value)}/{entityId}

## Example Keys

- v/acme/user-123
- e/acme/edge-999
- a/out/acme/user-123/MEMBER_OF/edge-999
- a/in/acme/group-abc/MEMBER_OF/edge-999
- i/acme/User/email/616c6963654061636d652e696f/user-123

## Encoding Notes

- Tenant always appears first after prefix for isolation and future placement.
- Property index values are hex-encoded bytes to preserve lexical safety in keys.
- Prefix scans should be used for adjacency traversals and index probes.

## Code Reference

See [internal/graph/keyspace/keys.go](internal/graph/keyspace/keys.go).
