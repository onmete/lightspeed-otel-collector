// Package agenticexporter implements an OpenTelemetry Collector exporter that
// writes one native contextualized OTLP span document per JSONL line, retaining
// its original attributes and nested events in a bounded best-effort spool.
package agenticexporter
