# System Overview

The Lightspeed OTel Collector is a custom OpenTelemetry Collector distribution tailored for the OLS fleet. It runs on both hub and spoke clusters, collecting metrics, traces, and logs from OLS components and forwarding them to configured backends (hub aggregation endpoint, Prometheus, Jaeger, or other OTLP-compatible backends).

## Behavioral Rules

### System Role

1. The collector is a custom OTel Collector distribution built with the OpenTelemetry Collector Builder (ocb).
2. It includes only the receivers, processors, and exporters needed by the OLS fleet — no unnecessary upstream components.
3. It runs as a deployment or sidecar on both hub and spoke clusters.

### Deployment Modes

4. **Spoke mode:** collects telemetry from local OLS components (service, agentic operator, sandbox, alerts adapter) and exports to the hub collector. `[PLANNED]`
5. **Hub mode:** receives telemetry from spoke collectors, aggregates fleet-wide data, and exports to the final backend (Prometheus, Jaeger, etc.). `[PLANNED]`
6. The deployment mode is determined by configuration, not by separate binaries. `[PLANNED]`

### Signal Support

7. The collector MUST support metrics (Prometheus scraping and OTLP ingestion).
8. The collector MUST support traces (OTLP ingestion).
9. The collector SHOULD support logs (OTLP ingestion) — initially optional, required when structured logging is adopted across OLS components.

### Agentic Data Collection

10. Collector-local trace filtering, atom projection, and atomic file publication are specified in `what/agentic-data-collection.md`; the parent [`Agentic Data Collection`](../../../../.ai/spec/what/agentic-data-collection.md) specification owns the end-to-end contract. `[PLANNED: OLS-3569]`

### Resilience

11. The collector MUST buffer data during transient export failures using a bounded in-memory or persistent queue.
12. Queue overflow MUST result in back-pressure or oldest-first eviction, never silent data loss.
13. The collector MUST expose its own health and performance metrics (queue depth, export success/failure rates, dropped spans/metrics).

## Configuration Surface

| Field/Flag | Type | Default | Description |
|---|---|---|---|
| Configuration follows standard OTel Collector YAML config — receivers, processors, exporters, pipelines. Configuration is documented per-component: see `what/collector.md` for Collector configuration, `what/postgres-exporter.md` for the PostgreSQL exporter, and `what/agentic-data-collection.md` for planned Agentic Collector mechanics. ||||

## Planned Changes

| Ticket | Summary |
|---|---|
| — | Initial implementation — all rules above are planned |
| OLS-3569 | Collector-local Agentic trace filtering and atomic JSONL publication |
