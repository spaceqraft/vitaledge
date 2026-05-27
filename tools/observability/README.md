# VitalEdge Observability Baseline

This directory contains baseline Prometheus and Grafana assets for single-node VitalEdge operation.

## Files

- `prometheus.yml`: scrape config targeting VitalEdge metrics endpoint on `localhost:9100`.
- `prometheus.docker.yml`: scrape config for containerized Prometheus (targets `host.docker.internal:9100`).
- `docker-compose.yml`: one-command local Prometheus + Grafana stack.
- `grafana/vitaledge-overview.json`: starter dashboard for executor throughput, latency, index lookup behavior, and unindexed-candidate signals.
- `grafana/provisioning/*`: Grafana datasource and dashboard provisioning.

## One-Command Stack (Recommended)

From repository root:

```bash
make observability-up
```

This starts:

- Prometheus at `http://localhost:9090`
- Grafana at `http://localhost:3000`

Grafana defaults:

- user: `admin`
- password: `admin`

Dashboard provisioning is automatic (`VitalEdge / VitalEdge Overview`).

To stop:

```bash
make observability-down
```

## Enable Metrics Endpoint

Run VitalEdge with metrics endpoint enabled:

```bash
go run ./cmd/vitaledge --metrics-listen :9100
```

Equivalent environment variable:

```bash
export VITALEDGE_METRICS_LISTEN=:9100
```

Exposed HTTP routes:

- `/metrics` for Prometheus scraping
- `/healthz` for liveness checks

## Start Prometheus

```bash
prometheus --config.file=tools/observability/prometheus.yml
```

## Import Grafana Dashboard

When using docker-compose provisioning, datasource and dashboard import are automatic.

For manual Grafana setups:

1. Configure a Prometheus datasource in Grafana.
2. Import `tools/observability/grafana/vitaledge-overview.json`.
3. Select your datasource in the dashboard variable dropdown.
