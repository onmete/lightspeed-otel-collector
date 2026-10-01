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

10. The Collector-local Agentic branch selects spans per the exact service and span-owned correlation rules, then serializes each eligible contextualized span and its nested events as one native OTLP JSON array element. It privately stages JSONL and atomically publishes top-level arrays under `traces/v1/`, preserves best-effort fan-out, and leaves downstream classification/reconstruction to the consumer; see `what/agentic-data-collection.md`. `[IMPLEMENTED: OLS-4248]` `[IMPLEMENTED: OLS-4249]` This does not implement an uploader or prove cluster/downstream integration.

### Resilience

11. The collector MAY buffer data during transient export failures using a bounded queue. The Agentic branch uses private JSONL staging and versioned array publication in its own bounded best-effort spool and does not back-pressure other configured trace destinations; queue and byte-budget semantics are defined in `what/agentic-data-collection.md`. `[IMPLEMENTED: OLS-4249]`
12. Queue overflow behavior MUST be explicit. When a selected span cannot be admitted at the 64-job count bound or shared encoded-byte budget, the Agentic exporter rejects one complete document and records the loss without back-pressure to other destinations; the bounds are distinct. `[IMPLEMENTED: OLS-4248]` `[IMPLEMENTED: OLS-4249]`
13. The collector MUST expose health and performance metrics, including bounded document-loss accounting for the Agentic branch. `[IMPLEMENTED: OLS-4248]`

## Configuration Surface

| Field/Flag | Type | Default | Description |
|---|---|---|---|
| Configuration follows standard OTel Collector YAML config — receivers, processors, exporters, pipelines. Configuration is documented per-component: see `what/collector.md` for Collector configuration, `what/postgres-exporter.md` for the PostgreSQL exporter, and `what/agentic-data-collection.md` for the Collector-local Agentic native OTLP array contract. ||||

## Collector-local Implementation Status

| Ticket | Summary |
|---|---|
| OLS-4248 | Per-span Agentic eligibility, native OTLP documents, telemetry, and exporter integration `[IMPLEMENTED: OLS-4248]` |
| OLS-4249 | Private bounded Agentic JSONL staging, versioned array publication, isolation, and recovery `[IMPLEMENTED: OLS-4249]` |
