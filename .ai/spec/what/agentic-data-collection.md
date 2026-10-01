# Agentic Data Collection — Collector-Local Contract

> **Status:** Implemented Collector-local replacement contract; fresh-binary smoke proof passed healthy-output, invalid-configuration, and same-process-recovery scenarios. This establishes the Collector-local output and failure boundary only; coordinated rollout prerequisites remain outstanding.

This contract replaces the Collector-owned split Action/Transcript candidate projection with one native contextualized OTLP span document per JSONL line. It supersedes the Collector-local behavior only. The workspace [parent specification](../../../../.ai/spec/what/agentic-data-collection.md) and [ADR 0043](../../../../.ai/spec/decisions/0043-agentic-data-collection-via-otel.md) still require the old split streams and custom envelope; those owners must coordinate revisions before integrated rollout. Operator-generated configuration and the ready-file consumer also require migration. This file owns only Collector-local span selection, native encoding, spooling, failure isolation, and telemetry; Dataverse owns flattening and product interpretation.

## Input and Per-Span Eligibility

1. The product-data branch MUST consume OTLP traces only. OTLP metrics produce no product document. OTLP log records MUST remain on the separate templog/PostgreSQL path. `[IMPLEMENTED: OLS-4248]`
2. Eligibility MUST be evaluated independently for each span. The Collector MUST accept only exact resource `service.name` values `lightspeed-agentic-operator` and `lightspeed-agentic-sandbox`. `[IMPLEMENTED: OLS-4248]`
3. The span itself MUST carry a non-empty string `agenticrun.uid` and a string `agenticrun.phase` whose value is one of `analysis`, `approval`, `execution`, `verification`, `escalation`, or `terminal`. The Collector MUST NOT fall back to resource attributes or infer either value from another span, IDs, names, payloads, topology, or other context. `[IMPLEMENTED: OLS-4248]`
4. A selected span MUST also have non-empty trace and span IDs. A span failing any service, correlation, phase, or identity requirement is rejected as one document; none of its attached events are emitted separately. `[IMPLEMENTED: OLS-4248]`
5. Once a span is eligible, the Collector MUST retain it regardless of operation name or GenAI attribute names/contents, and MUST retain every attached event regardless of event name or attributes. Unknown and future operations/events are valid evidence. The Collector MUST NOT classify spans or events as Actions versus Transcripts or inspect embedded content strings. `[IMPLEMENTED: OLS-4248]`
6. The branch MUST NOT remove, mutate, suppress, delay, or back-pressure traces delivered to any other configured trace destination. `[IMPLEMENTED: OLS-4248]`

## Native OTLP JSONL Record

7. Every published document MUST contain exactly one eligible span under the native hierarchy `resourceSpans[] -> scopeSpans[] -> spans[]`, with its original events nested under that span. The output unit is the whole contextualized span: the Collector MUST NOT split its events into separate documents or duplicate one admitted job across documents. Best-effort rejection, encoding, queue, filesystem, or shutdown failure MAY result in no ready document for a selected span. Each document ends in exactly one LF (`\n`); newlines in JSON strings are escaped within it. `[IMPLEMENTED: OLS-4248]`
8. The document MUST retain the selected span's original resource and instrumentation-scope context, including their attributes and schema URLs, plus the span's attributes, source identity, timing, kind, parent identity when present, status, trace state, flags, links and link attributes, and dropped counts. All recorded span events and their attributes remain nested in original array order; an event's original zero-based index is its position in that array, not a Collector field. `[IMPLEMENTED: OLS-4248]`
9. Attribute values MUST use native OTLP JSON typed-value representation, preserving strings, booleans, integers, doubles, bytes, arrays, and key-value lists. Values are not stringified, normalized, flattened, redacted, truncated, or decoded from content strings. This is semantic serialization of received pdata, not preservation of original HTTP/protobuf bytes or unknown wire fields already discarded by the receiver. `[IMPLEMENTED: OLS-4248]`
10. The Collector MUST NOT add `schema_version`, `candidate_type`, `record_kind`, `event_index`, flat correlation fields, or a custom `data` wrapper. Run UID and phase remain in the original span attributes; service identity remains in resource attributes. `[IMPLEMENTED: OLS-4248]`
11. A file MAY contain documents for different services, runs, phases, and traces. Filename, line, and upload order MUST NOT be treated as execution order. Dataverse owns per-line JSON parsing, OTLP flattening, classification, deduplication, joins, run/phase reconstruction, transcript assembly, and logical SQL models. `[IMPLEMENTED: OLS-4248]`

## One Spool and Bounds

12. The `agentic` exporter MUST use one directory and one writer. The configuration is: `[IMPLEMENTED: OLS-4248]`

```yaml
agentic:
  directory: /var/lib/lightspeed-data-collection/traces
  max_backlog_bytes: 8388608
```

The default directory is `/var/lib/lightspeed-data-collection/traces`; the default shared unpublished-byte budget is 8 MiB (`8388608`) applied once. `actions_directory` and `transcripts_directory` are removed keys, not aliases or fallback paths.
13. The asynchronous contextualized-span job queue has a separate maximum of 64 jobs. This is a job-count bound, not the encoded-byte budget. The single writer's byte budget measures all encoded bytes admitted but not yet published (queued plus active batch); it is not a total-process-memory or ready-file disk cap. `[IMPLEMENTED: OLS-4248]`
14. The writer MUST admit a complete document or reject it; it MUST NOT truncate, split, or partially enqueue one. Before writer admission, each complete encoded JSONL document, including its final LF, MUST be at most 120,000,000 bytes. This limit is fixed, not a configuration option, and counts all native context and events. A larger document MUST be rejected whole with one `record_too_large` rejection; equality passes the size guard. Cancellation MUST retain `shutdown_deadline` precedence over size rejection. When unpublished bytes are zero, the writer MAY accept one complete document larger than the configured byte budget, but this exception MUST NOT bypass the record-size cap. Otherwise admission MUST remain within the shared budget. Reservations remain until atomic publication succeeds. `[IMPLEMENTED: OLS-4249]`
15. Reference receivers MUST retain finite OTLP request limits: 20 MiB gRPC and 20,971,520 bytes HTTP; neither receiver limit nor in-memory budget is a ready-file disk quota. `[IMPLEMENTED: OLS-4249]`
16. Empty or relative directories and non-positive budgets disable only this exporter. Configuration validation remains non-fatal and Collector startup succeeds. `[IMPLEMENTED: OLS-4248]`

## Atomic Publication and Best-Effort Failure

17. The single writer MUST publish only in the configured `traces/` directory. It MUST exclusively create a random lowercase canonical UUIDv4 `.<uuid>.tmp`, append complete LF-terminated documents, close it successfully, and atomically rename it within the same directory to `<uuid>.jsonl`. Startup cleanup MUST remove only abandoned temporary files that match the writer-owned pattern. `[IMPLEMENTED: OLS-4249]`
18. A non-empty batch MUST publish when its encoded size reaches 1,048,576 bytes or no later than 30 seconds after its first document. One whole document MAY cross the 1 MiB publication threshold; that threshold is not a hard file or line limit. Combined with the record-size cap in rule 14, new ready files MUST be smaller than 121,048,576 bytes, safely below the downstream 128 MB raw-row ceiling. Graceful shutdown MUST attempt bounded publication before its context expires. `[IMPLEMENTED: OLS-4249]`
19. A ready `.jsonl` file is immutable: the Collector MUST NOT append, rewrite, move, overwrite, or delete it. Atomic rename provides complete-file visibility; this writer makes no fsync, power-loss, or crash-durability guarantee. On the pod-local spool volume, ready files are retry buffers, not durable retention; pod replacement or volume removal can lose unsent ready and in-progress data. Upload and ready-file deletion remain outside the Collector. `[IMPLEMENTED: OLS-4249]`
20. Directory preflight and runtime mkdir/open/write/close/rename/temporary-cleanup failures MUST degrade only this product-data writer and use capped exponential-backoff recovery. On a partial write or failed close, the writer MUST remove only its owned temporary file before rebuilding the active batch from retained complete documents under a new UUID; after a rename failure, it MUST retry the same complete closed temporary file. Ready files are preserved; active records remain reserved until publication or bounded shutdown loss. The writer MUST NOT block shared trace intake while waiting for queue capacity, filesystem recovery, or publication. A full queue, encoding failure, oversized record, invalid identity, or shutdown deadline can lose the affected document and is reported through bounded, content-free telemetry. `[IMPLEMENTED: OLS-4249]`
21. Exporter configuration or filesystem degradation MUST NOT make Collector startup, health, or another configured pipeline fail solely because of this branch. `[IMPLEMENTED: OLS-4249]`

## Telemetry

22. Existing Agentic metric names MUST be retained, but their measurements describe one document per selected span. Admission, record size, rejection, and ready-record accounting MUST count a span document once, regardless of its number of nested events. Ineligible spans and their ignored events are one rejected source document, not one count per event. `[IMPLEMENTED: OLS-4248]`
23. `candidate_type`, `record_kind`, and `stream` labels MUST be removed. Remaining labels and rejection reasons MUST be bounded; fixed document rejection reasons include `unsupported_service`, `missing_uid`, `missing_or_invalid_phase`, `invalid_trace_identity`, `queue_full`, `encoding_failed`, `record_too_large`, and `shutdown_deadline`. `[IMPLEMENTED: OLS-4248]`
24. Metrics and logs MUST NOT contain run UIDs, trace/span IDs, user or cluster IDs, event names, filenames, attribute values, prompts, outputs, or record content. `[IMPLEMENTED: OLS-4248]`

## Inspection and Rollout Coordination

If downstream storage receives a whole file as one raw value, it MUST first split on LF, parse each complete JSONL line as a separate OTLP document, and then flatten it. To inspect copied complete ready files locally:

```bash
jq -c '.resourceSpans[] | .scopeSpans[] | .spans[] | {traceId, spanId, parentSpanId, name, attributes, events}' /local/copied/traces/*.jsonl
```

This command reads existing fields from each document. External inspection scripts and dashboards are not migrated by this Collector-only change.

Before integrated rollout, owners MUST coordinate all of the following:

- Revise the workspace parent collection specification and ADR 0043, which still define the old Action/Transcript split and custom envelope.
- Update operator-generated configuration from the removed directory keys to `directory` and the single shared budget.
- Provide a finite spool-volume size limit; the shared in-memory budget is not a ready-file disk quota.
- Point the ready-file consumer at complete `.jsonl` files in `traces/`, excluding active `.tmp` files, with compatible shared-volume permissions.
- Decide how to preserve or drain existing `actions/` and `transcripts/` files; the Collector MUST NOT dispose of them.
- Update dashboards that depend on removed labels or event-atom counts, and migrate external inspection tooling separately.

| Ticket | Collector-local change |
|---|---|
| OLS-4248 | Per-span eligibility, one native contextualized OTLP document, and document-level telemetry `[IMPLEMENTED: OLS-4248]` |
| OLS-4249 | One shared bounded JSONL spool, atomic publication, isolation, and recovery `[IMPLEMENTED: OLS-4249]` |
