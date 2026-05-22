# Benchmark Dataset Contracts

This document defines initial dataset contracts for Phase 0 benchmark scaffolding.

## Directory Convention

Recommended layout:

- benchmarks/datasets/research/
- benchmarks/datasets/threat/
- benchmarks/datasets/rebac/

## Research

Purpose: large-file and graph-structure ingestion experiments.

Suggested files:

- vertices.csv
- edges.csv

Required columns:

- vertices.csv: tenant,id,labels_json,properties_json
- edges.csv: tenant,id,type,src_id,dst_id,properties_json

## Threat/Anomaly

Purpose: structured log ingestion and relationship generation.

Suggested files:

- events.jsonl

Required fields per line:

- tenant
- ts
- subject
- action
- object
- attributes (object)

## ReBAC

Purpose: relationship graph for authorization checks.

Suggested files:

- principals.csv
- resources.csv
- relationships.csv

Required columns:

- principals.csv: tenant,id,properties_json
- resources.csv: tenant,id,type,properties_json
- relationships.csv: tenant,id,type,src_id,dst_id,properties_json

## Notes

- JSON payload columns are opaque in Phase 0 and validated only for presence.
- Dataset validation can be added in Phase 1 benchmark implementation.
