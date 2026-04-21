# Container Observability Plan

- Date: 2026-04-02
- Status: Proposed follow-on operational plan

## Summary

JobScout should use an internal scrape-based observability stack rather than trying to derive application metrics from logs. The recommended free self-hosted path is:

1. Prometheus for metrics scraping and retention
2. Grafana OSS for dashboards and alerts
3. Loki OSS plus Promtail or Grafana Alloy for container logs
4. Exporters for infrastructure containers that do not expose native Prometheus metrics

This keeps operational telemetry off the public HTTP surface. Metrics endpoints should be reachable on the Docker network, not exposed publicly unless there is a deliberate operator need.

## Why Not Logs For Metrics

Logs and metrics solve different problems.

- Metrics are numeric time series, intended to be scraped and aggregated.
- Logs are event streams, intended for debugging and forensic analysis.
- Prometheus is designed around a pull model over HTTP for time-series metrics.
- Grafana visualizes metrics and logs, but it is not itself a metrics collector.

So the recommended pattern is:

- application and exporter metrics -> Prometheus
- container and service logs -> Loki
- dashboards and alerts -> Grafana

Do not treat log scraping as the primary metrics pipeline.

## Licensing And Cost

The recommended stack is available as no-cost self-hosted open source software:

- Prometheus is open source and uses a pull-based metrics model.
- Grafana OSS is available as open-source self-hosted dashboarding software.
- Loki OSS is available as open-source self-hosted log aggregation software.

If a managed service is desired later, Grafana Cloud also has a free tier, but the self-hosted OSS path is enough for JobScout.

## JobScout Current State

Current service metrics endpoints already exist on the split-stack services:

- `extraction` -> `/metrics`
- `embeddings` -> `/metrics`
- `scorer-matcher` -> `/metrics`
- `orchestrator` -> `/metrics`
- `web-backend` -> `/metrics`

Current worker metrics exposure also exists:

- `notification-worker` -> Prometheus scrape endpoint on port `9464`

Current gaps:

- `web-frontend` is a static frontend container and does not expose Prometheus app metrics
- `postgres`, `redis`, and `jobspy` do not currently have dedicated exporters wired into compose
- fit diagnostics are persisted and logged, but not yet surfaced in a real observability dashboard

## Recommended Architecture

### Metrics Plane

- Prometheus scrapes all service `/metrics` endpoints on the Docker network.
- Prometheus also scrapes exporters for infrastructure containers:
  - `cadvisor` for per-container CPU, memory, filesystem, and network usage
  - `node-exporter` for host metrics if needed
  - `postgres-exporter` for PostgreSQL
  - `redis-exporter` for Redis
- Grafana reads Prometheus as a data source.

### Logs Plane

- Promtail or Grafana Alloy tails Docker container logs.
- Logs are shipped to Loki.
- Grafana reads Loki as a data source.

### Security Posture

- Keep metrics and log collection internal to the compose network.
- Do not expose `/metrics` publicly by default.
- Expose Grafana only behind explicit operator access controls.
- Keep capability management separate from observability; do not overload public product routes with admin telemetry.

## Container Mapping

### Already scrapeable

- `extraction:8081/metrics`
- `embeddings:8082/metrics`
- `scorer-matcher:8083/metrics`
- `orchestrator:8084/metrics`
- `web-backend:8080/metrics`
- `notification-worker:9464`

### Recommended next additions

### Infrastructure exporters

- `cadvisor`
  - all container runtime metrics
- `postgres-exporter`
  - DB connection counts, locks, cache hit ratio, slow query indicators
- `redis-exporter`
  - memory, ops/sec, connected clients, evictions

### Optional

- `blackbox-exporter`
  - probe health endpoints externally from Prometheus if needed

## Fit Metrics To Surface

Prometheus should eventually expose or derive panels for:

- semantic fit requests by mode:
  - `cross_encoder`
  - `llm`
  - `threshold`
- provider route counts:
  - `local`
  - `remote`
  - `local_heuristic`
  - `threshold`
- fallback rate and fallback reasons
- truncation rate by field
- average truncated characters per request
- pair counts per scoring run
- recall depth usage
- hybrid vs dense-only retrieval counts
- judged requirement counts
- fit scoring latency percentiles

Some of these currently exist only in persisted diagnostics. To show them in Grafana, we will need explicit Prometheus counters and histograms in the scoring path.

## Compose-Level Rollout Plan

### Phase 1

- Add `prometheus` and `grafana` services to a dedicated compose overlay
- Scrape the existing service `/metrics` endpoints plus the notification worker scrape target
- Add dashboards for service health and request volume

### Phase 2

- Add `cadvisor`, `postgres-exporter`, and `redis-exporter`
- Add infrastructure dashboards

### Phase 3

- Add Loki plus Promtail or Grafana Alloy
- Correlate fit failures and latency spikes with logs in Grafana

### Phase 4

- Add explicit fit-domain Prometheus metrics in scorer/matcher code
- Build fit-specific dashboards for truncation, fallback, retrieval mix, and mode usage

## Recommendation

Best practice for JobScout is:

- metrics via Prometheus scrape targets
- logs via Loki shipping
- dashboards via Grafana OSS

Do not rely on logs as the primary metrics source, and do not introduce a public-facing `/dashboard` or `/metrics` route for internal fit observability. Keep metrics internal and operator-facing.
