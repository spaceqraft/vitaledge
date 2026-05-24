# VitalEdge Comprehension Questions

Purpose: interactive comprehension prompts for important design and implementation decisions.

How to use:
- Read one topic at a time.
- Try to answer each question before opening the linked answer.
- Use links to move between this file and COMPREHENSION-A.md.

## Topic 1: System Context and Priorities

<a id="q-001"></a>
### Q-001: What are the three primary workload categories shaping VitalEdge's architecture?
Answer link: [A-001](COMPREHENSION-A.md#a-001)

<a id="q-002"></a>
### Q-002: Why is a local-first, single-binary approach prioritized before distributed features?
Answer link: [A-002](COMPREHENSION-A.md#a-002)

<a id="q-003"></a>
### Q-003: What does "avoid unnecessary translation layers" mean for graph storage and query?
Answer link: [A-003](COMPREHENSION-A.md#a-003)

## Topic 2: Phased Architecture and Evolution

<a id="q-004"></a>
### Q-004: What are the three main architecture phases and their core objectives?
Answer link: [A-004](COMPREHENSION-A.md#a-004)

<a id="q-005"></a>
### Q-005: Which design constraints must be satisfied from the start to enable future sharding and replication?
Answer link: [A-005](COMPREHENSION-A.md#a-005)

<a id="q-006"></a>
### Q-006: How does the single-node transaction model map to future distributed transaction semantics?
Answer link: [A-006](COMPREHENSION-A.md#a-006)

## Topic 3: Storage Engine and Keyspace

<a id="q-007"></a>
### Q-007: Why was Pebble chosen over alternatives like Badger, RocksDB, or SQLite?
Answer link: [A-007](COMPREHENSION-A.md#a-007)

<a id="q-008"></a>
### Q-008: What are the initial key families in the storage layout, and what does each represent?
Answer link: [A-008](COMPREHENSION-A.md#a-008)

<a id="q-009"></a>
### Q-009: Why is tenant/namespace the first component in key prefixes?
Answer link: [A-009](COMPREHENSION-A.md#a-009)

<a id="q-010"></a>
### Q-010: What principles make the keyspace design efficient for scans and future sharding?
Answer link: [A-010](COMPREHENSION-A.md#a-010)

## Topic 4: Query Engine and Cypher Support

<a id="q-011"></a>
### Q-011: Which Cypher clauses and features are supported in the current executor pipeline?
Answer link: [A-011](COMPREHENSION-A.md#a-011)

<a id="q-012"></a>
### Q-012: Why is it important to decouple the Cypher parser/executor from storage internals?
Answer link: [A-012](COMPREHENSION-A.md#a-012)

<a id="q-013"></a>
### Q-013: What recent improvements have been made to Cypher compatibility and projection?
Answer link: [A-013](COMPREHENSION-A.md#a-013)

<a id="q-014"></a>
### Q-014: Which Cypher feature is deferred, and why is its absence not blocking for Phase 1?
Answer link: [A-014](COMPREHENSION-A.md#a-014)

## Topic 5: Index Management and Planner Behavior

<a id="q-015"></a>
### Q-015: How are property indexes declared and managed in Phase 1, and why?
Answer link: [A-015](COMPREHENSION-A.md#a-015)

<a id="q-016"></a>
### Q-016: What risks are associated with automatic property indexing, and how does the current approach mitigate them?
Answer link: [A-016](COMPREHENSION-A.md#a-016)

<a id="q-017"></a>
### Q-017: What future index management features are planned after the query surface stabilizes?
Answer link: [A-017](COMPREHENSION-A.md#a-017)

## Topic 6: Reliability, Operability, and Performance

<a id="q-018"></a>
### Q-018: What were the Phase 1 exit criteria for correctness, performance, and operability?
Answer link: [A-018](COMPREHENSION-A.md#a-018)

<a id="q-019"></a>
### Q-019: What benchmark evidence demonstrates Phase 1 performance targets were met?
Answer link: [A-019](COMPREHENSION-A.md#a-019)

<a id="q-020"></a>
### Q-020: What does "operability baseline" mean for the current implementation?
Answer link: [A-020](COMPREHENSION-A.md#a-020)

## Topic 7: Networking and Productionization

<a id="q-021"></a>
### Q-021: Why is a centralized service port map maintained in the design?
Answer link: [A-021](COMPREHENSION-A.md#a-021)

<a id="q-022"></a>
### Q-022: Which documented ports are externally exposed, and which are internal control-plane by default?
Answer link: [A-022](COMPREHENSION-A.md#a-022)

<a id="q-023"></a>
### Q-023: How do mTLS requirements affect service boundary and port exposure decisions?
Answer link: [A-023](COMPREHENSION-A.md#a-023)

## Topic 8: Multi-node Readiness and Next Steps

<a id="q-024"></a>
### Q-024: What capabilities must be demonstrated before entering Phase 2 (hardening)?
Answer link: [A-024](COMPREHENSION-A.md#a-024)

<a id="q-025"></a>
### Q-025: What are the prerequisites for starting Phase 3 (replicated multi-node)?
Answer link: [A-025](COMPREHENSION-A.md#a-025)

<a id="q-026"></a>
### Q-026: What practical review questions should be answered before approving major architecture changes?
Answer link: [A-026](COMPREHENSION-A.md#a-026)

## Optional Add-on Prompts

<a id="q-027"></a>
### Q-027: If the storage backend changed, which query and keyspace assumptions would need immediate re-validation?
Answer link: [A-027](COMPREHENSION-A.md#a-027)

<a id="q-028"></a>
### Q-028: If Cypher index DDL was introduced, what migration and operability controls are required first?
Answer link: [A-028](COMPREHENSION-A.md#a-028)

<a id="q-029"></a>
### Q-029: When distributed replication is enabled, what are the first failure scenarios to test?
Answer link: [A-029](COMPREHENSION-A.md#a-029)

<a id="q-030"></a>
### Q-030: What is the one-sentence architecture intent that should remain stable as implementation details evolve?
Answer link: [A-030](COMPREHENSION-A.md#a-030)

---
