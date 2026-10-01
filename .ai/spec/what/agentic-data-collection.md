# Agentic Data Collection — Collector-Local Contract

> **Status:** Implemented Collector-local private-JSONL-to-array contract. On 2026-10-01 the fresh binary passed the 30-second, >1 MiB, and graceful-shutdown publication paths, native typed span/event fidelity, invalid-staging isolation, and same-process non-root failure/recovery. The generated production operator stanza decoded with the real Collector `validate` command. This evidence does not prove cluster deployment or downstream runtime ingestion.

This contract replaces the Collector-owned split Action/Transcript candidate projection with one native contextualized OTLP JSON object per eligible span, privately staged as JSONL and publicly packaged in top-level arrays. It owns Collector-local span selection, encoding, staging, bounded publication, failure isolation, and telemetry. A future consumer owns file transfer, array parsing, flattening, classification, deduplication, joins, and run/phase reconstruction; no uploader is implemented in the Collector.

## Input and Per-Span Eligibility

1. The product-data branch MUST consume OTLP traces only. OTLP metrics produce no product document. OTLP log records MUST remain on the separate templog/PostgreSQL path. `[IMPLEMENTED: OLS-4248]`
2. Eligibility MUST be evaluated independently for each span. The Collector MUST accept only exact resource `service.name` values `lightspeed-agentic-operator` and `lightspeed-agentic-sandbox`. `[IMPLEMENTED: OLS-4248]`
3. The span itself MUST carry a non-empty string `agenticrun.uid` and a string `agenticrun.phase` whose value is one of `analysis`, `approval`, `execution`, `verification`, `escalation`, or `terminal`. The Collector MUST NOT fall back to resource attributes or infer either value from another span, IDs, names, payloads, topology, or other context. `[IMPLEMENTED: OLS-4248]`
4. A selected span MUST also have non-empty trace and span IDs. A span failing any service, correlation, phase, or identity requirement is rejected as one document; none of its attached events are emitted separately. `[IMPLEMENTED: OLS-4248]`
5. Once a span is eligible, the Collector MUST retain it regardless of operation name or GenAI attribute names/contents, and MUST retain every attached event regardless of event name or attributes. Unknown and future operations/events are valid evidence. The Collector MUST NOT classify spans or events as Actions versus Transcripts or inspect embedded content strings. `[IMPLEMENTED: OLS-4248]`
6. The branch MUST NOT remove, mutate, suppress, delay, or back-pressure traces delivered to any other configured trace destination. `[IMPLEMENTED: OLS-4248]`

## Native OTLP Array Elements

7. Every public file MUST be a non-empty compact top-level JSON array at `<directory>/v1/<uuid>.json`. Each array element MUST be one unchanged native OTLP JSON object containing exactly one eligible span under `resourceSpans[] -> scopeSpans[] -> spans[]`, with all attached events nested under that span. The private source representation is one such object plus LF per source record; escaped newlines in JSON strings remain inside the object. Conversion MUST remove only source LFs, separate objects with commas, and add array brackets without a trailing comma, custom wrapper, or `schema_version`. A selected span MAY have no public element after best-effort admission, encoding, filesystem, or shutdown loss. `[IMPLEMENTED: OLS-4248]`
8. Each object MUST retain the selected span's original resource and instrumentation-scope context, including their attributes and schema URLs, plus the span's attributes, source identity, timing, kind, parent identity when present, status, trace state, flags, links and link attributes, and dropped counts. All span events and their attributes remain nested in original array order; an event's original zero-based index is its position in that array, not a Collector field. `[IMPLEMENTED: OLS-4248]`
9. Attribute values MUST use native OTLP JSON typed-value representation, preserving strings, booleans, integers, doubles, bytes, arrays, and key-value lists. Values are not stringified, normalized, flattened, redacted, truncated, or decoded from content strings. This is semantic serialization of received pdata, not preservation of original HTTP/protobuf bytes or unknown wire fields already discarded by the receiver. `[IMPLEMENTED: OLS-4248]`
10. The Collector MUST NOT add `schema_version`, `candidate_type`, `record_kind`, `event_index`, flat correlation fields, or a custom `data` wrapper. Run UID and phase remain in original span attributes; service identity remains in resource attributes. `v1` versions file packaging, not OTLP. `[IMPLEMENTED: OLS-4248]`
11. A file MAY contain array elements for different services, runs, phases, and traces. File and element order MUST NOT be treated as execution order. A downstream consumer owns array-element parsing, OTLP flattening, classification, deduplication, joins, run/phase reconstruction, transcript assembly, and logical SQL models; this Collector change does not implement or exercise that ingestion path. `[IMPLEMENTED: OLS-4248]`

## One Spool and Bounds

12. The `agentic` exporter MUST use one writer, one shared byte budget, a public export root, and a distinct private staging root. The configuration is: `[IMPLEMENTED: OLS-4248]`

```yaml
agentic:
  directory: /var/lib/lightspeed-data/export/traces
  staging_directory: /var/lib/lightspeed-data/staging
  max_backlog_bytes: 8388608
```

`directory` is the unversioned trace export root; the writer derives `directory/v1` for published files. The default directory is `/var/lib/lightspeed-data/export/traces`, the default staging directory is `/var/lib/lightspeed-data/staging`, and the default shared unpublished-byte budget is 8 MiB (`8388608`). `actions_directory` and `transcripts_directory` are removed keys, not aliases or fallback paths.

The configured roots MUST be absolute and MUST NOT overlap. For atomic rename, `staging_directory` and `directory/v1` MUST be descendants of the same mounted tree—not merely the same filesystem or backing volume. The Collector MUST mount the whole existing spool exactly once at `/var/lib/lightspeed-data`, with no `subPath`; separate Collector `subPath`/bind mounts can make rename fail with `EXDEV` even when they use the same volume. The exporter cannot validate mount identity or infer the future uploader's complete pickup tree. The `staging` directory MUST remain outside that tree. A future uploader MUST mount only the volume's `export` subpath at its pickup root; the Collector pod does not implement an uploader.

13. The asynchronous contextualized-span job queue has a separate maximum of 64 jobs. This is a job-count bound, not the encoded-byte budget. `max_backlog_bytes` measures admitted private JSONL source bytes (queued plus active) until publication; it is not a total-process-memory or disk quota, and it does not bound public arrays or backlog. When no bytes are unpublished, the writer MAY admit one whole source record larger than the byte budget, subject to the record-size cap. `[IMPLEMENTED: OLS-4248]`
14. The writer MUST admit a complete native OTLP JSON object or reject it; it MUST NOT truncate, split, or partially enqueue one. Before writer admission, each complete private JSONL record, including its final LF, MUST be at most 120,000,000 bytes. This limit is fixed, not a configuration option, and counts all native context and events. A larger record MUST be rejected whole with one `record_too_large` rejection; equality passes. Cancellation MUST retain `shutdown_deadline` precedence over size rejection. The final array has a separate exclusive limit of 128,000,000 bytes. `[IMPLEMENTED: OLS-4248]`
15. Reference receivers MUST retain finite OTLP request limits: 20 MiB gRPC and 20,971,520 bytes HTTP; neither receiver limit nor the in-memory admission budget is a ready-file disk quota. `[IMPLEMENTED: OLS-4249]`
16. Empty or relative export/staging directories, overlapping configured roots, and non-positive budgets disable only this exporter. Configuration validation remains non-fatal and Collector startup succeeds. The deployment MUST mount the whole spool exactly once at `/var/lib/lightspeed-data` without `subPath`, keep both paths beneath that mount, and keep staging outside the uploader's entire pickup tree; configuration validation cannot verify mount identity. `[IMPLEMENTED: OLS-4248]`

## Atomic Publication and Best-Effort Failure

17. The single writer MUST keep all source and array-candidate files in `staging_directory` and publish only complete arrays under `directory/v1/`:

```text
staging_directory/
  .<uuid>.jsonl.tmp    # active source; private
  <uuid>.jsonl         # sealed source; private
  .<uuid>.json.tmp     # array candidate; private
directory/v1/
  <uuid>.json          # completed top-level array
```

The writer MUST exclusively create canonical lowercase UUIDv4 names, append complete LF-terminated source records, close and seal the source privately, stream its objects into the private array candidate, then atomically rename the closed candidate to `<uuid>.json`. Startup cleanup MUST remove only files with the recognized private temporary-name patterns. It MUST preserve complete sealed JSONL sources for inspection, leave unrelated files untouched, and neither replay those sources nor inspect public-file inventory. `[IMPLEMENTED: OLS-4249]`
18. A non-empty source batch MUST publish when its source size reaches 1,048,576 bytes or no later than 30 seconds after its first successfully written complete record. One whole record MAY cross the 1 MiB threshold. For source size `B` bytes and `N` records, the final array is exactly `B + 1` bytes: remove `N` LFs, add `N-1` commas and two brackets. Every final file MUST be strictly smaller than 128,000,000 bytes, including brackets and commas; equality fails the exclusive guard. With the retained record cap and rotation threshold, a new file is at most 121,048,576 bytes. Graceful shutdown MUST attempt bounded publication before its context expires. `[IMPLEMENTED: OLS-4249]`
19. Successful final rename is the handoff and MUST end Collector ownership of that public file. The published array is immutable: the Collector MUST NOT append, rewrite, move, overwrite, list, read, stat, delete, or check upload status of it after rename. Publication accounting MUST commit once; source cleanup after commit is private cleanup only. Atomic rename provides complete-file visibility, not `fsync`, power-loss, or crash-durability guarantees. On a pod-local spool volume, pod replacement or volume removal can lose files; startup preserves complete sealed sources but does not replay them. `[IMPLEMENTED: OLS-4249]`
20. Directory preflight and runtime mkdir/open/read/write/close/rename/temporary-cleanup failures MUST degrade only this product-data writer and use capped exponential-backoff recovery (initially 1 second, at most 30 seconds). The writer MUST retain complete source records when rebuilding or retrying:

- A partial source write or failed close MUST remove only its broken active temporary file and rebuild from retained complete records.
- A failed source-sealing rename MUST retain the closed source temporary file and retry sealing; conversion MUST NOT start before sealing succeeds.
- Source read or conversion write/close failure MUST retain the sealed source, clean only a broken array candidate, and retry conversion.
- Final rename failure, including `EXDEV`, MUST retain the sealed source and closed array candidate and retry that same candidate; there is no copy fallback.
- Source removal failure after final rename MUST retry only private cleanup. It MUST NOT republish, inspect the public file, or account for the publication twice; `ErrNotExist` completes cleanup.

Active source records remain reserved until publication or bounded shutdown loss. If the shutdown deadline expires during conversion, the writer MUST close private handles, retain the sealed source for inspection, report unpublished loss with existing precedence, and MUST NOT later publish that batch. The writer MUST NOT block shared trace intake while waiting for queue capacity, filesystem recovery, or publication. A full queue, encoding failure, oversized record, invalid identity, or shutdown deadline can lose the affected document. `[IMPLEMENTED: OLS-4249]`

Shutdown MUST wait for the writer to finish before exporter telemetry teardown.
The deadline cancels conversion and prevents late publication; it is not an
absolute bound on synchronous filesystem calls or closes already in progress.
Waiting for those operations to resolve MUST NOT abandon a worker or allow
callbacks after teardown.

21. Exporter configuration or filesystem degradation MUST NOT make Collector startup, health, or another configured pipeline fail solely because of this branch. `[IMPLEMENTED: OLS-4249]`

## Telemetry

22. Existing Agentic metric names MUST be retained, but their measurements describe one document per selected span. Admission counters count admitted span documents once; `otelcol_agentic_exporter_record_size` records the size of each admitted source JSONL record, including its final LF. Unpublished/open-batch gauges describe only the private queue/batch state (source bytes for byte gauges). `otelcol_agentic_exporter_ready_files_created` and `otelcol_agentic_exporter_ready_records_created` are cumulative successful-publication counters; `otelcol_agentic_exporter_ready_bytes_created` counts final array bytes, not source bytes. There MUST be no live public-backlog file/byte gauges or inventory scans. Ineligible spans and ignored events are one rejected source document, not one count per event. `[IMPLEMENTED: OLS-4248]`
23. `candidate_type`, `record_kind`, and `stream` labels MUST be removed. Remaining labels and rejection reasons MUST be bounded; fixed document rejection reasons include `unsupported_service`, `missing_uid`, `missing_or_invalid_phase`, `invalid_trace_identity`, `queue_full`, `encoding_failed`, `record_too_large`, and `shutdown_deadline`. `[IMPLEMENTED: OLS-4248]`
24. Metrics and logs MUST NOT contain run UIDs, trace/span IDs, user or cluster IDs, event names, filenames, attribute values, prompts, outputs, or record content. `[IMPLEMENTED: OLS-4248]`

## Inspection and Rollout Coordination

Downstream consumers MUST parse each complete top-level JSON array first; they MUST NOT split public files on LF. To inspect copied output:

```bash
jq -c '.[] | .resourceSpans[] | .scopeSpans[] | .spans[] | {traceId, spanId, parentSpanId, name, attributes, events}' /local/copied/traces/v1/*.json
```

This command reads existing native fields from each array element. Dataverse owns parsing, flattening, classification, deduplication, joins, run/phase reconstruction, transcript assembly, and logical SQL models. No downstream uploader or ingestion runtime was exercised by the Collector smoke.

Before rollout, owners MUST:

- Mount the whole spool exactly once for the Collector at `/var/lib/lightspeed-data`, without `subPath`, so staging and export are beneath the same mount; keep all private staging outside the entire future uploader pickup tree.
- Ensure any future uploader mounts only the volume's `export` subpath at its pickup root, selects complete arrays under `traces/v1/`, and does not see staging; no uploader is implemented in the Collector pod.
- Size the finite spool volume for source and array co-existence during conversion (approximately twice one unpublished batch), plus published-array backlog. The 8 MiB unpublished-byte budget is not a disk quota.
- Drain or otherwise handle previously published `.jsonl` files and old `actions/` and `transcripts/` files. This exporter does not convert or delete them.
- Exercise the actual cluster deployment and downstream array consumer separately; local Collector proof does not establish either.

| Ticket | Collector-local change |
|---|---|
| OLS-4248 | Per-span eligibility, one native contextualized OTLP array element, and document-level telemetry `[IMPLEMENTED: OLS-4248]` |
| OLS-4249 | Private bounded JSONL staging, versioned array publication, isolation, and recovery `[IMPLEMENTED: OLS-4249]` |
