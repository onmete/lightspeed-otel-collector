// Package agenticexporter implements an OpenTelemetry Collector exporter that
// stages native contextualized OTLP span documents as private JSONL, then
// streams them into atomically published top-level arrays under the configured
// trace root's v1 directory. It retains original attributes and nested events.
// Staging and export paths must be below one Collector mount; separate
// subPath mounts can make the final rename fail with EXDEV.
package agenticexporter
