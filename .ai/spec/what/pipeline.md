# Pipeline

The data pipeline: how telemetry flows from OLS components through the collector to backends.

## Behavioral Rules

### Receivers

1. The collector MUST include a Prometheus receiver for scraping OLS component metrics endpoints. `[PLANNED]`
2. The collector MUST include an OTLP receiver (gRPC and HTTP) for ingesting traces and metrics pushed by OLS components.
3. On the hub, the collector MUST include an OTLP receiver to accept forwarded telemetry from spoke collectors. `[PLANNED]`

### Processors

4. The collector MUST add cluster identity labels to all telemetry (cluster name, cluster ID) so fleet-wide data is attributable to its source spoke. `[PLANNED]`
5. The collector MUST support batch processing to reduce export overhead.
6. The collector SHOULD support filtering/sampling processors to control volume in large fleets.

### Exporters

7. In spoke mode, the collector MUST export to the hub collector's OTLP endpoint. `[PLANNED]`
8. In hub mode, the collector MUST export to at least one configurable backend (Prometheus remote-write, OTLP endpoint, or both).
9. The collector MUST support multiple exporters simultaneously (e.g., Prometheus for metrics + Jaeger for traces).

### Pipeline Composition

10. Pipelines (receiver → processor → exporter chains) MUST be defined per signal type (metrics, traces, logs).
11. A misconfigured pipeline MUST fail at startup with a clear error, not at runtime.

### Agentic Product-Data Branch

12. The planned trace-to-file branch is specified in `what/agentic-data-collection.md`; the parent [`Agentic Data Collection`](../../../../.ai/spec/what/agentic-data-collection.md) specification owns its accepted cross-repository interface and product semantics. `[PLANNED: OLS-3569]`

## Planned Changes

| Ticket | Summary |
|---|---|
| — | All rules are planned — initial design |
| OLS-3569 | Trace-only Agentic candidate classification and JSONL spool branch |
