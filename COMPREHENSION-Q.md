# VitalEdge Comprehension Questions

Purpose: interactive comprehension prompts for important design and implementation decisions.

How to use:
- Read one topic at a time.
- Try to answer each question before opening the linked answer.
- Use links to move between this file and COMPREHENSION-A.md.

## Topic 1: System Context and Priorities

<a id="q-001"></a>
### Q-001: What are the three primary workload categories that drive VitalEdge's architecture?
Answer link: [A-001](COMPREHENSION-A.md#a-001)

<a id="q-002"></a>
### Q-002: Why is the initial focus on a single-node, local-first, single-binary deployment?
Answer link: [A-002](COMPREHENSION-A.md#a-002)

<a id="q-003"></a>
### Q-003: What is meant by "avoiding unnecessary translation layers" in the context of graph storage and query?
Answer link: [A-003](COMPREHENSION-A.md#a-003)

## Topic 2: Architecture Phases and Evolution

<a id="q-004"></a>
### Q-004: What are the main architectural phases and their objectives in the VitalEdge roadmap?
Answer link: [A-004](COMPREHENSION-A.md#a-004)

<a id="q-005"></a>
### Q-005: Which design constraints must be satisfied from the start to enable future sharding and replication?
Answer link: [A-005](COMPREHENSION-A.md#a-005)

<a id="q-006"></a>
### Q-006: How does the single-node transaction model map to future distributed transaction semantics?
Answer link: [A-006](COMPREHENSION-A.md#a-006)

## Topic 3: Storage Engine and Keyspace

<a id="q-007"></a>
### Q-007: Why was Pebble selected as the storage backend over alternatives like Badger, RocksDB, or SQLite?
Answer link: [A-007](COMPREHENSION-A.md#a-007)

<a id="q-008"></a>
### Q-008: What are the key families in the initial storage layout, and what does each represent?
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

<a id="q-015"></a>
### Q-015: What architectural debt was revealed by achieving openCypher TCK compliance, and how is it being addressed?
Answer link: [A-015](COMPREHENSION-A.md#a-015)

## Topic 5: Index Management and Planner Behavior

<a id="q-016"></a>
### Q-016: How are property indexes declared and managed in Phase 1, and why?
Answer link: [A-016](COMPREHENSION-A.md#a-016)

<a id="q-017"></a>
### Q-017: What risks are associated with automatic property indexing, and how does the current approach mitigate them?
Answer link: [A-017](COMPREHENSION-A.md#a-017)

<a id="q-018"></a>
### Q-018: What future index management features are planned after the query surface stabilizes?
Answer link: [A-018](COMPREHENSION-A.md#a-018)

## Topic 6: Reliability, Operability, and Performance

<a id="q-019"></a>
### Q-019: What were the Phase 1 exit criteria for correctness, performance, and operability?
Answer link: [A-019](COMPREHENSION-A.md#a-019)

<a id="q-020"></a>
### Q-020: What benchmark evidence demonstrates Phase 1 performance targets were met?
Answer link: [A-020](COMPREHENSION-A.md#a-020)

<a id="q-021"></a>
### Q-021: What does "operability baseline" mean for the current implementation?
Answer link: [A-021](COMPREHENSION-A.md#a-021)

## Topic 7: Networking and Productionization

<a id="q-022"></a>
### Q-022: Why is a centralized service port map maintained in the design?
Answer link: [A-022](COMPREHENSION-A.md#a-022)

<a id="q-023"></a>
### Q-023: Which documented ports are externally exposed, and which are internal control-plane by default?
Answer link: [A-023](COMPREHENSION-A.md#a-023)

<a id="q-024"></a>
### Q-024: How do mTLS requirements affect service boundary and port exposure decisions?
Answer link: [A-024](COMPREHENSION-A.md#a-024)

## Topic 8: Multi-node Readiness and Next Steps

<a id="q-025"></a>
### Q-025: What capabilities must be demonstrated before entering Phase 2 (hardening)?
Answer link: [A-025](COMPREHENSION-A.md#a-025)

<a id="q-026"></a>
### Q-026: What are the prerequisites for starting Phase 3 (replicated multi-node)?
Answer link: [A-026](COMPREHENSION-A.md#a-026)

<a id="q-027"></a>
### Q-027: What practical review questions should be answered before approving major architecture changes?
Answer link: [A-027](COMPREHENSION-A.md#a-027)

## Topic 9: Query Pipeline and EXPLAIN

<a id="q-032"></a>
### Q-032: What are the explicit contracts and responsibilities for each stage in the query pipeline (parse, semantic validation, logical planning, physical execution)?
Answer link: [A-032](COMPREHENSION-A.md#a-032)

<a id="q-033"></a>
### Q-033: How does the EXPLAIN command behave, and what must its output include?
Answer link: [A-033](COMPREHENSION-A.md#a-033)

<a id="q-034"></a>
### Q-034: How does the system ensure that EXPLAIN is read-only, even for write-shaped queries?
Answer link: [A-034](COMPREHENSION-A.md#a-034)

<a id="q-035"></a>
### Q-035: What diagnostics and statistics are surfaced by EXPLAIN for operator tuning?
Answer link: [A-035](COMPREHENSION-A.md#a-035)

<a id="q-036"></a>
### Q-036: What is the policy for cardinality quality in EXPLAIN output, and how are values classified?
Answer link: [A-036](COMPREHENSION-A.md#a-036)

## Topic 10: Edge Property Index Pushdown

<a id="q-037"></a>
### Q-037: How does edge property index pushdown work, and what are the fallback and correctness policies?
Answer link: [A-037](COMPREHENSION-A.md#a-037)

<a id="q-038"></a>
### Q-038: What is the adaptive pushdown policy for stage2 recommendation queries, and what are its guardrails?
Answer link: [A-038](COMPREHENSION-A.md#a-038)

<a id="q-039"></a>
### Q-039: How is index pushdown observability achieved in the system?
Answer link: [A-039](COMPREHENSION-A.md#a-039)

## Topic 11: Programmatic Interface and CLI

<a id="q-040"></a>
### Q-040: What are the key design principles for the gRPC/protobuf programmatic interface?
Answer link: [A-040](COMPREHENSION-A.md#a-040)

<a id="q-041"></a>
### Q-041: How do parameterized queries and server-side parameter binding work, and why are they required?
Answer link: [A-041](COMPREHENSION-A.md#a-041)

<a id="q-042"></a>
### Q-042: What is the CLI's contract for statement completeness and transport?
Answer link: [A-042](COMPREHENSION-A.md#a-042)

## Optional Add-on Prompts

<a id="q-028"></a>
### Q-028: If the storage backend changed, which query and keyspace assumptions would need immediate re-validation?
Answer link: [A-028](COMPREHENSION-A.md#a-028)

<a id="q-029"></a>
### Q-029: If Cypher index DDL was introduced, what migration and operability controls are required first?
Answer link: [A-029](COMPREHENSION-A.md#a-029)

<a id="q-030"></a>
### Q-030: When distributed replication is enabled, what are the first failure scenarios to test?
Answer link: [A-030](COMPREHENSION-A.md#a-030)

<a id="q-031"></a>
### Q-031: What is the one-sentence architecture intent that should remain stable as implementation details evolve?
Answer link: [A-031](COMPREHENSION-A.md#a-031)

---
