CONTRIBUTING.md — VitalEdge

Thank you for your interest in contributing to VitalEdge, the open‑source, AI‑native graph database.
We welcome contributions in code, documentation, benchmarks, tests, and research.

This document explains how to contribute effectively and safely.
1. Code of Conduct

By participating in this project, you agree to uphold the
Contributor Covenant Code of Conduct.

VitalEdge is a technical project, but we maintain a respectful, collaborative environment.
2. Licensing Model

VitalEdge uses a dual‑license structure:

    Database core: AGPLv3

    Client libraries: LGPLv3

    Documentation & examples: CC‑BY‑4.0 unless otherwise noted

All contributions must be compatible with these licenses.

Before contributing, please read the VitalEdge Contributor License Agreement (CLA):
VitalEdge CLA

You must sign the CLA before your first merged PR.
3. What You Can Contribute

We welcome contributions in the following areas:

    Core engine (storage, query planner, Cypher compliance, Pebble integration)

    Distributed system (consensus, sharding, replication, CRDB‑style patterns)

    Materialized views (ReBAC, threat‑detection pipelines, research workloads)

    Performance (LDBC‑aligned benchmarks, EXPLAIN‑driven optimizations)

    Client libraries (Go, Rust, Python, TypeScript)

    Documentation (architecture, APIs, examples, tutorials)

    Tooling (loaders, indexers, schema validators)

If you’re unsure whether something fits, open an issue first.
4. Development Workflow
4.1. Fork and Branch

    Fork the repository

    Create a feature branch:
    git checkout -b feature/my-improvement

4.2. Build Requirements

VitalEdge requires:

    Go ≥ 1.22

    Pebble (imported as a module)

    A POSIX‑compatible environment

Run the full build:
Code

make build
make test

5. Coding Standards
5.1. Language

VitalEdge is primarily written in Go.
Client libraries may use other languages.
5.2. Style

    Follow Go standard formatting (go fmt, go vet)

    Keep functions small and composable

    Prefer explicitness over cleverness

    Avoid panics except in impossible states

    Use table‑driven tests

5.3. Performance

VitalEdge is a database. Performance matters.

    Benchmark changes (go test -bench=.)

    Include microbenchmarks for hot paths

    Include macrobenchmarks for query‑level changes

    Use EXPLAIN output to justify planner changes

6. Testing Requirements

Every PR must include:

    Unit tests

    Integration tests (where applicable)

    Benchmarks for performance‑critical code

    Reproduction tests for bug fixes

We reject PRs that reduce test coverage or introduce nondeterminism.
7. Commit & PR Guidelines
7.1. Commit Messages

Use clear, descriptive commit messages:
Code

feat: add pushdown predicate support for typed edges
fix: fix Pebble iterator invalidation bug
chore: update dependencies

7.2. Pull Requests

A good PR includes:

    A clear description of the change

    Motivation and context

    Tests

    Benchmarks (if performance‑related)

    Documentation updates (if user‑visible)

Small, focused PRs are easier to review and merge.
8. Security & Responsible Disclosure

VitalEdge handles sensitive workloads (ReBAC, threat detection).
If you discover a security issue:

    Do open a public issue

9. Architecture Notes

VitalEdge has several architectural pillars:

    Pebble‑backed storage engine

    Cypher‑compatible query language

    Strong typing, constraints, and pushdown predicates

If you are modifying any of these areas, please read the relevant design docs in /docs/architecture.
10. Contributor Recognition

We maintain a public Contributors Hall in the repository.
Significant contributions (features, optimizations, research) are highlighted.
11. Getting Help

If you need help:

    Open a discussion

    File an issue with the question label

We’re happy to help you get started.
