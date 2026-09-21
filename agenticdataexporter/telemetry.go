package agenticdataexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterScope = "github.com/openshift/lightspeed-otel-collector/agenticdataexporter"

type telemetry struct {
	classified metric.Int64Counter
	ignored    metric.Int64Counter
}

func newTelemetry(settings component.TelemetrySettings) (*telemetry, error) {
	meter := settings.MeterProvider.Meter(meterScope)
	classified, err := meter.Int64Counter(
		"otelcol_agentic_candidate_atoms_total",
		metric.WithDescription("Eligible Agentic trace atoms classified and published."),
		metric.WithUnit("{atoms}"),
	)
	if err != nil {
		return nil, err
	}
	ignored, err := meter.Int64Counter(
		"otelcol_agentic_trace_atoms_ignored_total",
		metric.WithDescription("Agentic trace atoms ignored before candidate publication."),
		metric.WithUnit("{atoms}"),
	)
	if err != nil {
		return nil, err
	}
	return &telemetry{classified: classified, ignored: ignored}, nil
}

func (t *telemetry) recordClassified(ctx context.Context, candidateType, recordKind, serviceName string) {
	if t == nil {
		return
	}
	t.classified.Add(ctx, 1, metric.WithAttributes(
		attribute.String("candidate_type", candidateType),
		attribute.String("record_kind", recordKind),
		attribute.String("service_name", serviceName),
	))
}

func (t *telemetry) recordIgnored(ctx context.Context, reason string) {
	if t == nil {
		return
	}
	t.ignored.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
