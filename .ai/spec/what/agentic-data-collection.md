# Agentic Data Collection

Collector-local implementation contract for OLS-3569. The parent [`Agentic Data Collection`](../../../../.ai/spec/what/agentic-data-collection.md) specification owns the end-to-end architecture, correlation and candidate semantics, canonical record envelope, collection policy, handoff, and downstream ownership; [`Decision 0042`](../../../../.ai/spec/decisions/0042-agentic-data-collection-via-otel.md) records the architectural boundary. This file owns only trace filtering, atom-level classification and projection, safe JSONL publication, failure isolation, and Collector telemetry. `[PLANNED: OLS-3569]`

## Accepted Input and Mechanical Classification

1. The product-data branch MUST consume OTLP traces only. OTLP metrics produce no candidate. OTLP log records MUST NOT enter this branch or any product-data file, even when a log name or attributes resemble a named span event; the existing templog/PostgreSQL log path remains separate. `[PLANNED: OLS-3569]`
2. The branch MUST apply the parent specification's [Input Contract](../../../../.ai/spec/what/agentic-data-collection.md#input-contract) and [Candidate Streams](../../../../.ai/spec/what/agentic-data-collection.md#candidate-streams) predicates exactly. It MUST NOT introduce another allowed service, phase, event predicate, or content-based classification rule. `[PLANNED: OLS-3569]`
3. Eligibility MUST be evaluated per span. The Collector MUST read `agenticrun.uid` and `agenticrun.phase` from that span's attributes, require both values to satisfy the parent contract, and use them for the span and all of its attached events. It MUST NOT fall back to resource attributes or infer either value from IDs, names, payloads, topology, or another span. `[PLANNED: OLS-3569]`
4. For each eligible span, the Collector MUST visit one span atom followed by every attached span-event atom in original OTLP order. An event's `event_index` is its zero-based position among the containing span's attached events only and MUST NOT be renumbered after filtering or classification. `[PLANNED: OLS-3569]`
5. Classification MUST apply this ordered map. The first matching row is final, and one source atom MUST NOT produce more than one candidate. `[PLANNED: OLS-3569]`

| OTLP input | Collector predicate | Candidate | Collector directory |
|---|---|---|---|
| Metric | Any | None | None |
| Log record | Any | None | None |
| Span or attached span event | Resource `service.name` or required span correlation attributes fail the parent Input Contract | None | None |
| Eligible span | Any | `action` | `/var/lib/lightspeed-data-collection/actions` |
| Eligible span event | Matches a parent Transcript Candidate predicate | `transcript` | `/var/lib/lightspeed-data-collection/transcripts` |
| Eligible span event | Every other event | `action` | `/var/lib/lightspeed-data-collection/actions` |

6. Event classification MUST inspect only the source event name and the attribute-presence predicates defined by the parent contract. It MUST NOT inspect literal payload text, duplicate a Transcript candidate into Actions, join atoms, or reclassify an atom after projection. `[PLANNED: OLS-3569]`
7. The branch MUST NOT remove, mutate, or suppress traces delivered to any other configured trace destination. `[PLANNED: OLS-3569]`

## Exact OTLP Projection

8. Each accepted atom MUST become one compact JSON object conforming exactly to the parent's [Candidate Record Envelope](../../../../.ai/spec/what/agentic-data-collection.md#candidate-record-envelope). The Collector MUST populate that envelope from OTLP using this map. `[PLANNED: OLS-3569]`

| Envelope destination | OTLP source or Collector value |
|---|---|
| `schema_version` | Constant required by the parent envelope |
| `candidate_type` | Final result of rule 5 |
| `record_kind` | `span` for a span atom; `span_event` for an attached event atom |
| `timestamp` | Span start timestamp for `span`; original event timestamp for `span_event` |
| `agenticrun_uid` | Eligible span attribute `agenticrun.uid` |
| `phase` | Eligible span attribute `agenticrun.phase` |
| `trace_id` | Containing span trace ID |
| `span_id` | Containing span ID |
| `parent_span_id` | Containing span parent ID; JSON `null` for a root span |
| `event_index` | Original zero-based attached-event position for `span_event`; JSON `null` for `span` |
| `service_name` | Resource attribute `service.name` |
| `name` | Original span name for `span`; original event name for `span_event` |
| `otel.resource` | Resource schema URL and dropped-attributes count |
| `otel.scope` | Instrumentation scope name, version, schema URL, and dropped-attributes count |
| `otel.span` | Containing span start/end timestamps, kind, status code/message, trace state, flags, links with their attributes and dropped counts, and dropped attribute/event/link counts |
| `otel.event` | Event dropped-attributes count for `span_event`; absent for `span` |
| `attributes.resource` | Original resource attribute map |
| `attributes.scope` | Original instrumentation scope attribute map |
| `attributes.span` | Original containing-span attribute map |
| `attributes.event` | Original event attribute map for `span_event`; absent for `span` |

9. IDs, timestamps, enums, scalar values, arrays, key-value lists, byte values, links, counts, nullability, and attribute values MUST use the exact representation required by the parent envelope and standard OTLP JSON mapping. The Collector MUST NOT stringify, normalize, semantically rename, redact, or discard accepted values. `[PLANNED: OLS-3569]`
10. Projection MUST be atom-local and stateless except for bounded per-directory file batching. The Collector MUST NOT wait for another span, event, trace, phase, or run state and MUST NOT join, deduplicate, reconstruct, aggregate, derive outcomes, or perform downstream transformations. `[PLANNED: OLS-3569]`
11. An atom that cannot produce a valid canonical envelope MUST NOT appear in a ready file. The Collector MUST increment a bounded rejection counter and emit a content-free error containing only the failure reason; it MUST NOT log rejected attributes or record content. `[PLANNED: OLS-3569]`

## OCB Capability and Configuration

12. The OCB build manifest MUST include the minimum custom trace component capability needed to apply rules 1–11 and publish the two file streams. These mechanics MAY be implemented as one trace exporter; separate classifier, connector, processor, projector, and writer components are not required. `[PLANNED: OLS-3569]`
13. The product-data trace branch MUST use the existing OTLP trace receiver and pipeline configuration. The Collector component MUST define no producer-facing OTLP endpoint, collection-state propagation, credential source, network uploader, upload schedule, retention policy, or downstream logical model. Its external output ends when an immutable `.jsonl` file becomes ready. `[PLANNED: OLS-3569]`
14. Component configuration MUST supply the Actions and Transcripts directory paths defined in rule 16. Static validation MUST reject non-absolute or identical paths and incomplete pipeline wiring. Envelope schema `"1.0"` and the rule 20 size and age limits are implementation constants, not independent runtime settings. A validation failure MUST stop startup before receivers open and report a clear content-free error. `[PLANNED: OLS-3569]`
15. The product-data branch MUST have its own bounded sending queue and retry path. Queue or filesystem exhaustion MUST reject only product candidates, increment the applicable rejection metric, and MUST NOT apply back-pressure to compliance, templog/PostgreSQL, or configured trace-backend pipelines. `[PLANNED: OLS-3569]`

## Exact Directories and Atomic File Protocol

16. The Collector MUST write only these independent streams. The cross-container volume views and ready-file consumer contract remain defined by the parent specification's [Shared Volume and File Protocol](../../../../.ai/spec/what/agentic-data-collection.md#shared-volume-and-file-protocol). `[PLANNED: OLS-3569]`

| Candidate | Collector directory | Temporary filename | Ready filename |
|---|---|---|---|
| `action` | `/var/lib/lightspeed-data-collection/actions` | `.<uuid>.tmp` | `<uuid>.jsonl` |
| `transcript` | `/var/lib/lightspeed-data-collection/transcripts` | `.<uuid>.tmp` | `<uuid>.jsonl` |

17. The Collector MUST maintain at most one open batch per candidate directory. A batch MAY contain records from multiple AgenticRuns, MUST contain only its directory's candidate type, and MUST NOT be published when empty. `[PLANNED: OLS-3569]`
18. Each batch MUST use a newly generated random lowercase canonical UUIDv4. The Collector MUST exclusively create `.<uuid>.tmp` in the destination directory, append only complete records, close the file successfully, and atomically rename it within that directory to `<uuid>.jsonl`. A filename MUST contain no run, user, cluster, model, tool, or event identity. `[PLANNED: OLS-3569]`
19. Each record MUST be compact UTF-8 JSON followed by exactly one LF (`\n`). A line MUST never be split between files, and every ready file MUST end after a complete LF-terminated record. `[PLANNED: OLS-3569]`
20. A batch's age starts when its first record is written. After appending each complete line, the Collector MUST close and publish the batch when its encoded size is at least 1 MiB (1,048,576 bytes); one complete record MAY make the file larger than that threshold. Independently, every non-empty batch MUST be published no later than 30 seconds after its first record. `[PLANNED: OLS-3569]`
21. On graceful shutdown, the Collector MUST close and publish every non-empty valid batch before its component stops. `[PLANNED: OLS-3569]`
22. On process startup, the Collector MUST create or open both configured directories and, before receivers open, delete every abandoned `.<uuid>.tmp` in them. It MUST NOT finalize or count an abandoned temporary file as ready. Cleanup failure MUST be observable and MUST prevent the product branch from opening the affected directory while unrelated pipelines remain available. `[PLANNED: OLS-3569]`
23. A ready `.jsonl` file is immutable. After atomic rename, the Collector MUST NOT append, rewrite, move, or delete it. `[PLANNED: OLS-3569]`
24. A directory creation, open, write, close, rename, or temporary-file removal failure after valid configuration is an operational product-branch failure. The component MUST preserve every already-ready file, increment the applicable bounded failure metric, emit a content-free error, and keep unrelated Collector pipelines running. `[PLANNED: OLS-3569]`

## Collector Observability

25. Collector internal telemetry MUST expose at least these product-branch measurements. Metric names MAY follow repository and OCB conventions, but meanings and bounded dimensions are required. `[PLANNED: OLS-3569]`

| Measurement | Type | Required bounded dimensions |
|---|---|---|
| Candidate atoms classified and emitted | Counter | `candidate_type`, `record_kind`, allowed `service_name` |
| Trace atoms ignored or rejected | Counter | bounded `reason`: `unsupported_service`, `missing_uid`, `missing_or_invalid_phase`, `invalid_envelope`, `queue_full` |
| Ready files created | Counter | `candidate_type` |
| Ready records and bytes created | Counters | `candidate_type` |
| Current ready-file backlog | Gauges | `candidate_type`: files and bytes visible as `.jsonl` |
| File operation failures | Counter | `candidate_type`, bounded `operation`: `mkdir`, `open`, `write`, `close`, `rename`, `remove_tmp` |
| Current open batch | Gauges | `candidate_type`: records, bytes, and age seconds |

26. Metrics MUST NOT use `agenticrun.uid`, trace IDs, span IDs, user or cluster IDs, model names, tool names, event names, filenames, or record content as labels. Collector errors and logs MUST NOT include candidate attributes or literal content. `[PLANNED: OLS-3569]`
27. Health and readiness MUST distinguish static Collector misconfiguration, an operationally degraded product-data branch, and unrelated pipeline health. Product-branch degradation MUST remain observable without making the existing Collector health endpoint unavailable solely because of a queue or spool failure. `[PLANNED: OLS-3569]`

## Planned Changes

| Ticket | Summary |
|---|---|
| OLS-3569 | Trace-only Agentic filtering, canonical atom projection, and atomic JSONL publication |
