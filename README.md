# OpenTelemetry Collector — OpenShift Lightspeed

Custom OpenTelemetry Collector distribution for OpenShift Lightspeed.
Receives OTLP logs over TLS and writes them to PostgreSQL. The `agentic` trace
exporter fans out eligible spans to private JSONL staging and publishes complete
native OTLP JSON arrays under `traces/v1/`.

```
OTLP traces --> receiver --> existing trace exporter(s)
                       \--> agentic exporter --> private JSONL --> traces/v1/*.json
OTLP logs ----> receiver --> postgresexporter --> PostgreSQL (TLS)

App ---------- GET/DELETE /api/v1/logs (HTTPS) --> postgres_admin --> PostgreSQL (TLS)
```

## Project Structure

```
├── builder-config.yaml              # OCB manifest — defines included components
├── cmd/otelcol-lightspeed/          # Pre-generated Collector source (committed)
│   ├── main.go                      # Generated entry point
│   ├── components.go                # Generated component wiring
│   └── go.mod / go.sum              # Full dependency graph (used by cachi2)
├── Dockerfile                       # Multi-stage UBI9 container build
├── Makefile                         # Build, test, container targets
├── postgresexporter/
│   ├── go.mod                       # Go module (pgx/v5)
│   ├── doc.go                       # Package documentation
│   ├── metadata.go                  # Component type registration ("postgres")
│   ├── config.go                    # Configuration struct + validation
│   ├── factory.go                   # Factory — creates exporter instances
│   ├── exporter.go                  # Core logic — pgx batch inserts
│   ├── telemetry.go                 # Internal metrics (insert duration, pool stats)
│   ├── config_test.go               # Config validation tests
│   └── exporter_test.go             # Exporter logic tests (pgxmock)
├── agenticexporter/
│   ├── classifier.go              # Per-span service and correlation eligibility
│   ├── stream_writer.go           # Private JSONL staging, array conversion and recovery
│   ├── telemetry.go               # Document-level metrics and content-free state logs
│   ├── exporter.go / factory.go   # Native contextualized OTLP array elements
│   ├── *_test.go                  # Contract, lifecycle, and filesystem tests
└── extension/
    ├── postgresadmin/
    │   ├── go.mod                   # Go module (pgx/v5)
    │   ├── doc.go                   # Package documentation
    │   ├── metadata.go              # Component type registration ("postgres_admin")
    │   ├── config.go                # Extension configuration + validation
    │   ├── factory.go               # Factory — creates extension instances
    │   ├── extension.go             # HTTP server + GET/DELETE handlers
    │   ├── config_test.go           # Config validation tests
    │   └── extension_test.go        # HTTP handler tests (pgxmock)
    └── httpsmetrics/
        ├── go.mod                   # Go module
        ├── doc.go                   # Package documentation
        ├── metadata.go              # Component type registration ("https_metrics")
        ├── config.go                # Extension configuration + validation
        ├── factory.go               # Factory — creates extension instances
        ├── extension.go             # HTTPS reverse proxy for /metrics
        ├── config_test.go           # Config validation tests
        └── extension_test.go        # Proxy + TLS tests
```

## Quick Start

```bash
# Prerequisites: Go 1.26.0+ for the Collector build; Go 1.26.5+ for E2E; PostgreSQL

# Build the local collector binary with OCB using builder-config.yaml.
make build

# Run locally
make run

# Run tests
make test

# Regenerate source after changing builder-config.yaml
make generate
```

`make build` runs OCB. The Dockerfile instead compiles the checked-in,
OCB-generated Go source with `go build`; it does not run OCB.

## Agentic Native OTLP Array Export

> **Collector-local evidence (2026-10-01):** A freshly built binary published
> arrays through the 30-second time trigger, the >1 MiB size trigger, and
> graceful shutdown. Native span/event evidence and typed values were preserved.
> A non-root permission failure retained the sealed source and recovered a
> byte-exact array in the same process after permissions were restored; deleting
> a published file did not make the Collector recreate it. An invalid
> overlapping staging path disabled only this exporter while health and the
> debug destination remained available. The generated production operator
> `agentic` stanza decoded with the real Collector `validate` command. These
> checks do not prove cluster deployment, uploader behavior, or downstream
> Dataverse ingestion, and do not authorize rollout.

The Agentic branch consumes traces only. Eligibility is evaluated per span:
`service.name` must be exactly `lightspeed-agentic-operator` or
`lightspeed-agentic-sandbox`; the span itself must have a non-empty string
`agenticrun.uid`, a valid `agenticrun.phase` (`analysis`, `approval`,
`execution`, `verification`, `escalation`, or `terminal`), and non-empty trace
and span IDs. There is no resource-attribute fallback. Each eligible span is
one native contextualized OTLP JSON object under
`resourceSpans[] -> scopeSpans[] -> spans[]`, retaining original typed values,
resource/scope context, and all attached events. The Collector does not
classify Action/Transcript candidates or inspect embedded content strings.

The private staging source is JSONL: one unchanged object plus LF per span.
Publication streams those objects into a compact top-level JSON array, removing
the source LFs and inserting commas and brackets. Each array element is one
eligible span; there is no custom wrapper, `schema_version`, or trailing comma.
Files may contain different services, runs, phases, and traces; neither file
nor array-element order is execution order. `v1` versions this packaging
contract, not OTLP.

The configuration is:

```yaml
agentic:
  directory: /var/lib/lightspeed-data/export/traces
  staging_directory: /var/lib/lightspeed-data/staging
  max_backlog_bytes: 8388608
```

`directory` is the unversioned export root; completed files are
`<directory>/v1/<uuid>.json`. Private files remain under
`staging_directory`:

```text
.<uuid>.jsonl.tmp    # active private source
<uuid>.jsonl         # sealed source
.<uuid>.json.tmp     # private array candidate
```

Staging must be outside the entire future uploader pickup tree, not merely
outside `traces/` or hidden by a filename. The exporter checks that both
configured roots are absolute and do not overlap, but cannot infer the full
pickup tree or validate mount identity. The Collector must mount the whole
existing spool exactly once at `/var/lib/lightspeed-data`, with no `subPath`;
staging and export then remain beneath one mount so atomic `rename` can succeed.
Separate Collector `subPath` mounts can fail with `EXDEV` even when they refer
to the same volume. A future uploader is separate: it must mount only the
volume's `export` subpath at its pickup root, so it cannot see staging. No
uploader is implemented in the Collector pod.

The asynchronous queue is bounded to 64 jobs. The shared 8 MiB unpublished
byte budget counts complete JSONL source records (including LF) queued or in
the active batch until publication; when it is zero, one complete document
larger than the budget may be admitted. Neither bound is a total process-memory
or disk quota.

Each source record is limited to **120,000,000 bytes**, including its final LF;
equality passes, and a larger record is rejected whole. A non-empty batch
publishes at 1,048,576 source bytes or within 30 seconds of its first
successfully written complete record. One complete record may cross the 1 MiB
threshold. The final array is exactly one byte larger than its JSONL source:
the record LFs are replaced by commas and the array brackets. A separate
exclusive guard requires every final file to be smaller than **128,000,000
bytes**; with the existing record cap and rotation threshold, a new file is at
most 121,048,576 bytes.

During conversion, the sealed source and candidate array coexist in staging;
budget disk for roughly twice one unpublished batch, plus already published
array backlog. The 8 MiB unpublished-byte budget does not cap spool-disk usage,
so deployments need a finite volume sized for this overhead and backlog.

Filesystem failures degrade only this best-effort branch and use capped
exponential backoff (1-second initial, 30-second maximum). A partial source
write or failed close removes only the broken private temporary file and
rebuilds from retained complete records. A failed source-sealing rename keeps
the closed source for retry. Read/conversion failures retain the sealed source
and clean only a broken array candidate; a failed final rename retains the
sealed source and closed candidate array for retry, with no copy fallback.
After a successful final rename, publication is committed once. If private
source cleanup then fails, recovery retries only that cleanup and never
republishes the array. If the shutdown deadline expires during conversion, the
writer closes private handles, preserves the sealed source for inspection, and
does not publish that batch later. This branch does not back-pressure other
trace destinations.

Shutdown waits for the writer to finish before closing exporter telemetry.
The deadline cancels conversion and prevents late publication, but a synchronous
filesystem call or close already in progress can delay completion beyond it.

Atomic rename gives complete-file visibility, not crash durability: there is no
`fsync`, restart replay, journal, or exactly-once guarantee, and pod-local
volume removal can lose files. Startup removes only recognized private source
and array-candidate temp names; it preserves complete sealed JSONL sources for
inspection but does not replay them. The Collector does not enumerate, read, stat, or track
published arrays after rename, and does not recreate a published file removed
by a consumer.

Admission counters count span documents once. The record-size histogram and
unpublished/open-batch byte gauges describe private JSONL source bytes,
including LF. The `otelcol_agentic_exporter_ready_files_created`,
`otelcol_agentic_exporter_ready_records_created`, and
`otelcol_agentic_exporter_ready_bytes_created` instruments are cumulative
publication counters; ready bytes are the final array size. The Collector
exposes no public-file backlog gauges. Labels and logs remain bounded and
content-free.

Downstream consumers must parse each complete `.json` top-level array before
traversing native OTLP hierarchy; they must not split the public file on LF.
Dataverse owns parsing, flattening, classification, deduplication, joins, and
run/phase reconstruction. This Collector change does not implement an uploader
or prove downstream ingestion. Inspect copied output with:

```bash
jq -c '.[] | .resourceSpans[] | .scopeSpans[] | .spans[] | {traceId, spanId, parentSpanId, name, attributes, events}' /local/copied/traces/v1/*.json
```

Previously published `.jsonl` files and old `actions/` or `transcripts/` files
are not converted or deleted automatically; owners must drain or otherwise
handle them before rollout.

## Log Record Schema

The `postgresexporter` writes a 5-column schema optimised for agentic run audit log
storage. The `postgres_admin` extension creates the table automatically on
startup (idempotent `CREATE TABLE IF NOT EXISTS`).

```sql
CREATE TABLE templogs.logs (
    id              BIGSERIAL PRIMARY KEY,
    agentic_run_id  TEXT NOT NULL,
    phase           TEXT NOT NULL DEFAULT '',
    timestamp       TIMESTAMPTZ NOT NULL,
    event           TEXT NOT NULL,
    body            JSONB
);

CREATE INDEX idx_logs_agentic_run_id ON templogs.logs (agentic_run_id);
CREATE INDEX idx_logs_run_phase ON templogs.logs (agentic_run_id, phase);
CREATE INDEX idx_logs_timestamp ON templogs.logs (timestamp);
```

| Column         | Type        | Source                                                     |
|----------------|-------------|------------------------------------------------------------|
| agentic_run_id | TEXT        | Log attribute `"agenticrun.uid"` — standard UUID (with hyphens, e.g. `550e8400-e29b-41d4-a716-446655440000`) |
| phase          | TEXT        | Log attribute `"agenticrun.phase"` (e.g. `planning`, `execution`) |
| timestamp      | TIMESTAMPTZ | TimeUnixNano → ObservedTimestamp → now                     |
| event          | TEXT        | Log attribute `"event"`                                    |
| body           | JSONB       | Log record body (serialized)                               |

## Configuration Reference

- [`builder-config.yaml`](builder-config.yaml) — OCB build manifest (which components are compiled in)
- [`config.yaml`](config.yaml) — Runtime config: direct-to-PostgreSQL (simple pipeline)
- [`config-router.yaml`](config-router.yaml) — Runtime config: routing by service name and signal type

## Admin API

### GET /api/v1/logs

Retrieve log records for an agentic run with cursor-based pagination.

```bash
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&limit=50&after=100"

# Filter by phase:
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&phase=planning"
```

| Parameter       | Required | Default | Description                                  |
|-----------------|----------|---------|----------------------------------------------|
| `agentic_run_id`| yes      | —       | Agentic run ID (standard UUID with hyphens)  |
| `phase`         | no       | —       | Filter by phase within the run               |
| `limit`         | no       | 100     | Max records to return (capped at 1000)       |
| `after`         | no       | 0       | Cursor: return records with id > N           |
| `format`        | no       | json    | Set to `text` for plain-text output          |

#### JSON response (default)
```json
{
  "agentic_run_id": "550e8400-e29b-41d4-a716-446655440000",
  "phase": "planning",
  "records": [
    {"id": 1, "phase": "planning", "timestamp": "2026-07-09T12:00:00Z", "event": "audit.agent.started", "body": {"msg": "hello"}},
    {"id": 2, "phase": "planning", "timestamp": "2026-07-09T12:00:01Z", "event": "audit.agent.tool.call", "body": {"tool": "bash"}}
  ],
  "has_more": false
}
```

#### Plain-text response (`format=text`)

```bash
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&format=text"
```

Returns `text/plain` with a metadata header, blank line, then one `timestamp: body` per line:

```text
agentic_run_id: 550e8400-e29b-41d4-a716-446655440000
records: 3
has_more: false

2026-07-09T12:00:00Z: [agent] Starting query (model=gpt-5.4, provider=openai)
2026-07-09T12:00:01.404171Z: HTTP Request: POST https://api.openai.com/v1/responses "HTTP/1.1 200 OK"
2026-07-09T12:00:02.174204Z: [provider:run] thinking: **Investigating pods in namespace**...
```

### DELETE /api/v1/logs

Delete all log records for an agentic run (all phases).

```bash
curl -X DELETE "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000"
```

| Parameter       | Required | Description                                  |
|-----------------|----------|----------------------------------------------|
| `agentic_run_id`| yes      | Agentic run ID (standard UUID with hyphens)  |

Response:
```json
{"deleted": 42, "agentic_run_id": "550e8400-e29b-41d4-a716-446655440000"}
```

## Container Build

```bash
# Build image (runs tests first)
make docker-build

# Push to registry
make docker-push

# Custom image tag
make docker-build VERSION=0.1.0
```

## PostgreSQL Log Delivery Durability

This section applies only to OTLP logs sent to PostgreSQL. The Agentic exporter
uses private JSONL staging and publishes arrays; its atomic rename is not an
fsync or crash-durability guarantee.

| Failure scenario | What happens |
|---|---|
| **Transient PostgreSQL failure** | Retried automatically with backoff |
| **Pod restart** | Queue persisted to disk — data resumes on restart |
| **Node failure** | Queue volume lost — in-flight data is lost |

## Credentials

Use the collector's environment variable substitution to inject credentials
from a Kubernetes Secret:

```yaml
# In collector config:
connection_string: "${env:POSTGRES_CONNECTION_STRING}"
```

When managed by the **lightspeed-operator**, credential handling is automatic.

## TLS

All communication channels use TLS:

| Channel | Protocol | TLS mechanism |
|---|---|---|
| OTLP ingestion (gRPC :4317) | mTLS-capable | Serving cert via `tls.cert_file` / `tls.key_file` |
| OTLP ingestion (HTTP :4318) | HTTPS | Serving cert via `tls.cert_file` / `tls.key_file` |
| Admin API (:8080) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| Prometheus metrics (:8888) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| PostgreSQL connection | TLS | `sslmode=require` (or `verify-full`) in DSN |
| Trace export (OTLP gRPC) | TLS | Default TLS (system CA bundle) |

In OpenShift, the serving certificate is injected by `service-ca` into
`/var/run/secrets/serving-cert/tls.{crt,key}`. For local development, omit
the TLS fields from `postgres_admin` and `https_metrics` to fall back to plaintext HTTP.
