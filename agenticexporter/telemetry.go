package agenticexporter

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func metricContextWithoutSpan(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

const meterScope = "github.com/openshift/lightspeed-otel-collector/agenticexporter"

type telemetry struct {
	meter metric.Meter

	candidates            metric.Int64Counter
	rejections            metric.Int64Counter
	recordSize            metric.Int64Histogram
	readyFilesCreated     metric.Int64Counter
	readyRecordsCreated   metric.Int64Counter
	readyBytesCreated     metric.Int64Counter
	fileOperationFailures metric.Int64Counter

	queueHighWaterBytes metric.Int64ObservableGauge
	unpublishedRecords  metric.Int64ObservableGauge
	unpublishedBytes    metric.Int64ObservableGauge
	openBatchRecords    metric.Int64ObservableGauge
	openBatchBytes      metric.Int64ObservableGauge
	openBatchAge        metric.Float64ObservableGauge
	streamState         metric.Int64ObservableGauge

	registration metric.Registration
	closeOnce    sync.Once
}

func newTelemetry(settings component.TelemetrySettings) (*telemetry, error) {
	if settings.MeterProvider == nil {
		return nil, errors.New("agentic exporter meter provider is nil")
	}
	meter := settings.MeterProvider.Meter(meterScope)
	t := &telemetry{meter: meter}
	var err error

	if t.candidates, err = meter.Int64Counter(
		"otelcol_agentic_exporter_candidates",
		metric.WithDescription("Native OTLP span documents successfully admitted to the shared spool."),
		metric.WithUnit("{records}"),
	); err != nil {
		return nil, err
	}
	if t.rejections, err = meter.Int64Counter(
		"otelcol_agentic_exporter_rejections",
		metric.WithDescription("Native OTLP span documents rejected before admission or lost at admission."),
		metric.WithUnit("{records}"),
	); err != nil {
		return nil, err
	}
	if t.recordSize, err = meter.Int64Histogram(
		"otelcol_agentic_exporter_record_size",
		metric.WithDescription("Size of an admitted native OTLP JSONL document, including its trailing line feed."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if t.readyFilesCreated, err = meter.Int64Counter(
		"otelcol_agentic_exporter_ready_files_created",
		metric.WithDescription("Ready files created by successful atomic rename."),
		metric.WithUnit("{files}"),
	); err != nil {
		return nil, err
	}
	if t.readyRecordsCreated, err = meter.Int64Counter(
		"otelcol_agentic_exporter_ready_records_created",
		metric.WithDescription("Span documents published in ready files."),
		metric.WithUnit("{records}"),
	); err != nil {
		return nil, err
	}
	if t.readyBytesCreated, err = meter.Int64Counter(
		"otelcol_agentic_exporter_ready_bytes_created",
		metric.WithDescription("Bytes published in ready files."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if t.fileOperationFailures, err = meter.Int64Counter(
		"otelcol_agentic_exporter_file_operation_failures",
		metric.WithDescription("Failed bounded file operations."),
		metric.WithUnit("{failures}"),
	); err != nil {
		return nil, err
	}

	if t.queueHighWaterBytes, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_queue_high_water_bytes",
		metric.WithDescription("Highest unpublished byte count observed by the stream."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if t.unpublishedRecords, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_unpublished_records",
		metric.WithDescription("Span documents admitted but not yet published."),
		metric.WithUnit("{records}"),
	); err != nil {
		return nil, err
	}
	if t.unpublishedBytes, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_unpublished_bytes",
		metric.WithDescription("Bytes admitted but not yet published."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if t.openBatchRecords, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_open_batch_records",
		metric.WithDescription("Span documents in the open batch."),
		metric.WithUnit("{records}"),
	); err != nil {
		return nil, err
	}
	if t.openBatchBytes, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_open_batch_bytes",
		metric.WithDescription("Bytes in the open batch."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if t.openBatchAge, err = meter.Float64ObservableGauge(
		"otelcol_agentic_exporter_open_batch_age",
		metric.WithDescription("Age of the open batch."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if t.streamState, err = meter.Int64ObservableGauge(
		"otelcol_agentic_exporter_stream_state",
		metric.WithDescription("Current stream state: unavailable=0, healthy=1, degraded=2."),
		metric.WithUnit("1"),
	); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *telemetry) registerWriter(writer *streamWriter) error {
	if writer == nil {
		return errors.New("agentic exporter stream writer is nil")
	}
	registration, err := t.meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			t.observeSnapshot(observer, writer.snapshot())
			return nil
		},
		t.queueHighWaterBytes,
		t.unpublishedRecords,
		t.unpublishedBytes,
		t.openBatchRecords,
		t.openBatchBytes,
		t.openBatchAge,
		t.streamState,
	)
	if err != nil {
		return err
	}
	t.registration = registration
	return nil
}

func (t *telemetry) observeSnapshot(observer metric.Observer, snapshot streamSnapshot) {
	observer.ObserveInt64(t.queueHighWaterBytes, snapshot.queueHighWaterBytes)
	observer.ObserveInt64(t.unpublishedRecords, snapshot.unpublishedRecords)
	observer.ObserveInt64(t.unpublishedBytes, snapshot.unpublishedBytes)
	observer.ObserveInt64(t.openBatchRecords, snapshot.openBatchRecords)
	observer.ObserveInt64(t.openBatchBytes, snapshot.openBatchBytes)
	observer.ObserveFloat64(t.openBatchAge, snapshot.openBatchAge.Seconds())
	observer.ObserveInt64(t.streamState, int64(snapshot.state))
}

func (t *telemetry) recordAdmission(ctx context.Context, serviceName string, size int) {
	ctx = metricContextWithoutSpan(ctx)
	var attrs []attribute.KeyValue
	if allowedServiceName(serviceName) {
		attrs = append(attrs, attribute.String("service_name", serviceName))
	}
	t.candidates.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.recordSize.Record(ctx, int64(size), metric.WithAttributes(attrs...))
}

func (t *telemetry) recordRejection(ctx context.Context, serviceName string, reason rejectionReason) {
	ctx = metricContextWithoutSpan(ctx)
	attrs := []attribute.KeyValue{attribute.String("reason", string(reason))}
	if allowedServiceName(serviceName) {
		attrs = append(attrs, attribute.String("service_name", serviceName))
	}
	t.rejections.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (t *telemetry) recordPublished(ctx context.Context, records, bytes int64) {
	ctx = metricContextWithoutSpan(ctx)
	t.readyFilesCreated.Add(ctx, 1)
	t.readyRecordsCreated.Add(ctx, records)
	t.readyBytesCreated.Add(ctx, bytes)
}

func (t *telemetry) recordFileOperationFailure(ctx context.Context, operation fileOperation) {
	ctx = metricContextWithoutSpan(ctx)
	t.fileOperationFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", string(operation))))
}

func (t *telemetry) close() {
	t.closeOnce.Do(func() {
		if t.registration != nil {
			_ = t.registration.Unregister()
		}
	})
}

func allowedServiceName(serviceName string) bool {
	return serviceName == "lightspeed-agentic-operator" || serviceName == "lightspeed-agentic-sandbox"
}
