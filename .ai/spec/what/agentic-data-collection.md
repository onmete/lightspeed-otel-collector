# Agentic Data Collection

Collector-local implementation contract for OLS-4248 and OLS-4249. The parent [`Agentic Data Collection`](../../../../.ai/spec/what/agentic-data-collection.md) specification owns the end-to-end architecture, correlation and candidate semantics, canonical record envelope, collection policy, handoff, and downstream ownership; [`Decision 0043`](../../../../.ai/spec/decisions/0043-agentic-data-collection-via-otel.md) records the architectural boundary. This file owns only trace filtering, atom-level classification and projection, safe JSONL publication, failure isolation, and Collector telemetry. `[IMPLEMENTED: OLS-4248]` `[IMPLEMENTED: OLS-4249]`

## Accepted Input and Mechanical Classification

1. The product-data branch MUST consume OTLP traces only. OTLP metrics produce no candidate. OTLP log records MUST NOT enter this branch or any product-data file, even when a log name or attributes resemble a named span event; the existing templog/PostgreSQL log path remains separate. `[IMPLEMENTED: OLS-4248]`
2. The branch MUST apply the parent specification's [Input Contract](../../../../.ai/spec/what/agentic-data-collection.md#input-contract) and [Candidate Streams](../../../../.ai/spec/what/agentic-data-collection.md#candidate-streams) predicates exactly. It MUST NOT introduce another allowed service, phase, event predicate, or content-based classification rule. `[IMPLEMENTED: OLS-4248]`
3. Eligibility MUST be evaluated per span. The Collector MUST read `agenticrun.uid` and `agenticrun.phase` from that span's attributes, require both values to satisfy the parent contract, and use them for the span and all of its attached events. It MUST NOT fall back to resource attributes or infer either value from IDs, names, payloads, topology, or another span. `[IMPLEMENTED: OLS-4248]`
4. For each eligible span, the Collector MUST visit one span atom followed by every attached span-event atom in original OTLP order. An event's `event_index` is its zero-based position among the containing span's attached events only and MUST NOT be renumbered after filtering or classification. `[IMPLEMENTED: OLS-4248]`
5. Classification MUST apply this ordered map. The first matching row is final, and one source atom MUST NOT produce more than one candidate. `[IMPLEMENTED: OLS-4248]`

| OTLP input | Collector predicate | Candidate | Collector directory |
|---|---|---|---|
| Metric | Any | None | None |
| Log record | Any | None | None |
| Span or attached span event | Resource `service.name` or required span correlation attributes fail the parent Input Contract | None | None |
| Eligible span | Any | `action` | `/var/lib/lightspeed-data-collection/actions` |
| Eligible span event | Matches a parent Transcript Candidate predicate | `transcript` | `/var/lib/lightspeed-data-collection/transcripts` |
| Eligible span event | Every other event | `action` | `/var/lib/lightspeed-data-collection/actions` |

6. Event classification MUST inspect only the source event name and the attribute-presence predicates defined by the parent contract. It MUST NOT inspect literal payload text, duplicate a Transcript candidate into Actions, join atoms, or reclassify an atom after projection. `[IMPLEMENTED: OLS-4248]`
7. The branch MUST NOT remove, mutate, suppress, delay, or back-pressure traces delivered to any other configured trace destination. `[IMPLEMENTED: OLS-4248]`

## Exact OTLP Projection

8. Each accepted atom MUST become one compact JSON object conforming exactly to the parent's [Candidate Record Envelope](../../../../.ai/spec/what/agentic-data-collection.md#candidate-record-envelope). The Collector MUST populate that envelope from OTLP using this map. `[IMPLEMENTED: OLS-4248]`

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

9. Every resource, scope, span, event, and link attribute value MUST use a tagged OTLP `AnyValue` object with exactly one of `stringValue`, `boolValue`, `intValue`, `doubleValue`, `bytesValue`, `arrayValue`, or `kvlistValue`; an unset value is `{}`. `arrayValue` recursively contains tagged values in its `values` array. `kvlistValue` preserves source order as standard OTLP `{"key": ..., "value": <tagged AnyValue>}` entries in its `values` array. Bytes are base64, signed and unsigned 64-bit integers are decimal strings, and non-finite doubles are `"NaN"`, `"Infinity"`, or `"-Infinity"`. IDs, timestamps, enums, links, counts, nullability, and all remaining metadata MUST use standard OTLP JSON representation. The Collector MUST NOT stringify, normalize, semantically rename, redact, truncate, flatten, or discard accepted values. `[IMPLEMENTED: OLS-4248]`
10. Projection MUST be atom-local and stateless except for bounded per-directory file batching. The Collector MUST NOT wait for another span, event, trace, phase, or run state and MUST NOT join, deduplicate, reconstruct, aggregate, derive outcomes, or perform downstream transformations. `[IMPLEMENTED: OLS-4248]`
11. An atom that cannot produce a valid canonical envelope MUST NOT appear in a ready file. The Collector MUST increment a bounded rejection counter and emit a content-free error containing only the failure reason; it MUST NOT log rejected attributes or record content. `[IMPLEMENTED: OLS-4248]`

## OCB Capability and Configuration

12. The OCB build manifest MUST include one custom `agentic` trace exporter implementing rules 1–11 and the two file streams. Separate classifier, connector, processor, projector, and writer components are not required. `[IMPLEMENTED: OLS-4248]`
13. The product-data trace branch MUST use the existing OTLP trace receiver and pipeline configuration. The Collector component MUST define no producer-facing OTLP endpoint, collection-state propagation, credential source, network uploader, upload schedule, retention policy, or downstream logical model. Its external output ends when an immutable `.jsonl` file becomes ready. `[IMPLEMENTED: OLS-4248]`
14. Omitting the `agentic` exporter from the trace pipeline is the normal disabled state. When present, configuration defaults to Actions directory `/var/lib/lightspeed-data-collection/actions`, Transcripts directory `/var/lib/lightspeed-data-collection/transcripts`, and `max_backlog_bytes: 4194304`, applied independently to each stream. An empty or relative directory, identical directories, or a non-positive explicitly configured backlog disables only this exporter: `Validate` remains non-fatal, component construction and Collector startup succeed, and one content-free deployment error is emitted. Envelope schema `"1.0"`, the rule 20 size threshold, and maximum batch age remain implementation constants. `[IMPLEMENTED: OLS-4248]`
15. Actions and Transcripts MUST have independent queues, workers, active batches, filesystem/degradation state, and byte budgets. For each stream, unpublished bytes are exactly active-batch bytes plus queued bytes. When unpublished bytes are zero, the stream MUST accept one complete record even if it exceeds `max_backlog_bytes`; otherwise it accepts a record only when the resulting unpublished bytes are at most the budget. It MUST never truncate, split, or partially enqueue a record, MUST reject excess as `queue_full`, and MUST release a reservation only after atomic ready-file publication. The OTLP gRPC and HTTP receivers MUST each enforce a finite request-size limit, and the operator-owned Agentic `emptyDir` MUST have a finite `sizeLimit`. Candidate rejection MUST return success to shared OTLP intake and MUST NOT wait for spool recovery or back-pressure any compliance, templog/PostgreSQL, admin, log, metric, or other trace pipeline. OLS-4256 validates and tunes the provisional budget, transport limits, encoded expansion, ready backlog, and upload throughput against Collector memory and volume budgets. `[IMPLEMENTED: OLS-4249]`

## Exact Directories and Atomic File Protocol

16. The Collector MUST write only these independent streams. The cross-container volume views and ready-file consumer contract remain defined by the parent specification's [Shared Volume and File Protocol](../../../../.ai/spec/what/agentic-data-collection.md#shared-volume-and-file-protocol). `[IMPLEMENTED: OLS-4249]`

| Candidate | Collector directory | Temporary filename | Ready filename |
|---|---|---|---|
| `action` | `/var/lib/lightspeed-data-collection/actions` | `.<uuid>.tmp` | `<uuid>.jsonl` |
| `transcript` | `/var/lib/lightspeed-data-collection/transcripts` | `.<uuid>.tmp` | `<uuid>.jsonl` |

17. The Collector MUST maintain at most one open batch per candidate directory. A batch MAY contain records from multiple AgenticRuns, MUST contain only its directory's candidate type, and MUST NOT be published when empty. `[IMPLEMENTED: OLS-4249]`
18. Each batch MUST use a newly generated random lowercase canonical UUIDv4. The Collector MUST exclusively create `.<uuid>.tmp` in the destination directory, append only complete records, close the file successfully, and atomically rename it within that directory to `<uuid>.jsonl`. A filename MUST contain no run, user, cluster, model, tool, or event identity. `[IMPLEMENTED: OLS-4249]`
19. Each record MUST be compact UTF-8 JSON followed by exactly one LF (`\n`). A line MUST never be split between files, and every ready file MUST end after a complete LF-terminated record. `[IMPLEMENTED: OLS-4249]`
20. A batch's age starts when its first record is written. After appending each complete line, the Collector MUST close and publish the batch when its encoded size is at least 1 MiB (1,048,576 bytes); one complete record MAY make the file larger than that threshold. Independently, every non-empty batch MUST be published no later than 30 seconds after its first record. `[IMPLEMENTED: OLS-4249]`
21. On graceful shutdown, the Collector MUST attempt to close and publish every non-empty valid batch before the supplied shutdown context expires. `[IMPLEMENTED: OLS-4249]`
22. Before receivers open, each stream MUST synchronously create or open its configured directory and remove only abandoned files that exactly match the owned `.<uuid>.tmp` pattern. It MUST NOT finalize, count, alter, or remove ready `.jsonl` or unrelated files. A preflight failure degrades only the affected stream, starts capped exponential-backoff recovery probing, and leaves `Start` successful so the other stream and unrelated pipelines open normally. `[IMPLEMENTED: OLS-4249]`
23. A ready `.jsonl` file is immutable. After atomic rename, the Collector MUST NOT append, rewrite, move, overwrite, or delete it. `[IMPLEMENTED: OLS-4249]`
24. A runtime `mkdir`, `open`, `write`, `close`, `rename`, or `remove_tmp` failure MUST degrade only the affected stream, preserve all ready files, retain valid active records within that stream's byte reservation, stop normal write attempts, and begin capped exponential-backoff recovery probes. After a partial write or failed close, it MUST remove only the exact owned temporary file before creating another and rebuild from retained complete record bytes under a new UUID; after rename failure, it MUST retry the same complete closed temporary file. It MUST emit one content-free degradation transition, bounded operation counters without repetitive probe logs, and one recovery transition. The other stream, shared Collector paths, and Collector health MUST continue independently. `[IMPLEMENTED: OLS-4249]`

## Collector Observability

25. Collector internal telemetry MUST expose these product-branch measurements. Metric names MAY follow repository and OCB conventions, but meanings and labels are required and bounded. `[IMPLEMENTED: OLS-4248]`

| Measurement | Type | Required bounded labels |
|---|---|---|
| Candidates classified | Counter | `candidate_type`, `record_kind`, allowed `service_name` |
| Atoms ignored or rejected | Counter | fixed `reason`: `unsupported_service`, `missing_uid`, `missing_or_invalid_phase`, `invalid_envelope`, `queue_full` |
| Record size | Histogram | `candidate_type`, `record_kind` |
| Queue high-water bytes | Gauge | `candidate_type` |
| Current unpublished records and bytes | Gauges | `candidate_type` |
| Current open batch records, bytes, and age | Gauges | `candidate_type` |
| Ready files, records, and bytes created | Counters | `candidate_type` |
| Current visible ready-file backlog files and bytes | Gauges | `candidate_type` |
| File operation failures | Counter | `candidate_type`, fixed `operation`: `mkdir`, `open`, `write`, `close`, `rename`, `remove_tmp` |
| Stream state | Gauge | `candidate_type` |

For rule 25, an ineligible span increments its applicable rejection reason once for the span and once for every attached event ignored with it. The classified counter increments only after successful projection and queue admission; ready counters increment only after atomic publication; ready-backlog gauges measure `.jsonl` files currently visible in each stream directory, including subtraction after the upload sidecar removes a successfully uploaded file.

26. Metrics and logs MUST NOT use or contain run UIDs, trace or span IDs, user or cluster IDs, model or tool names, event names, filenames, attributes, prompts, outputs, or record content. `[IMPLEMENTED: OLS-4248]`
27. Invalid Agentic configuration, startup preflight failure, or runtime stream degradation MUST remain observable through bounded metrics and content-free logs without changing Collector liveness or readiness solely because of this exporter. The exporter MUST add no health endpoint. `[IMPLEMENTED: OLS-4248]`

## Planned Changes

| Ticket | Summary |
|---|---|
| OLS-4248 | Agentic trace classification, schema-1.0 projection, telemetry, and best-effort Collector exporter integration |
| OLS-4249 | Independent bounded Actions and Transcripts queues, atomic file spool, failure isolation, and recovery |
