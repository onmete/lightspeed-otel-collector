package agenticexporter

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func TestTelemetryContract(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	tel, err := newTelemetry(component.TelemetrySettings{MeterProvider: provider})
	if err != nil {
		t.Fatalf("newTelemetry() error = %v", err)
	}
	actions := newStreamWriter(candidateAction, t.TempDir(), 1024)
	transcripts := newStreamWriter(candidateTranscript, t.TempDir(), 1024)
	if err := tel.registerWriters(actions, transcripts); err != nil {
		t.Fatalf("registerWriters() error = %v", err)
	}
	t.Cleanup(tel.close)

	ctx := context.Background()
	tel.recordCandidate(ctx, candidateAction, recordSpan, "lightspeed-agentic-operator", 37)
	tel.recordRejection(ctx, candidateTranscript, recordSpanEvent, "secret-service", rejectQueueFull)
	tel.recordPublished(ctx, candidateAction, 2, 74)
	tel.recordFileOperationFailure(ctx, candidateTranscript, opRename)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	wantNames := map[string]bool{
		"otelcol_agentic_exporter_candidates":              false,
		"otelcol_agentic_exporter_rejections":              false,
		"otelcol_agentic_exporter_record_size":             false,
		"otelcol_agentic_exporter_queue_high_water_bytes":  false,
		"otelcol_agentic_exporter_unpublished_records":     false,
		"otelcol_agentic_exporter_unpublished_bytes":       false,
		"otelcol_agentic_exporter_open_batch_records":      false,
		"otelcol_agentic_exporter_open_batch_bytes":        false,
		"otelcol_agentic_exporter_open_batch_age":          false,
		"otelcol_agentic_exporter_ready_files_created":     false,
		"otelcol_agentic_exporter_ready_records_created":   false,
		"otelcol_agentic_exporter_ready_bytes_created":     false,
		"otelcol_agentic_exporter_ready_backlog_files":     false,
		"otelcol_agentic_exporter_ready_backlog_bytes":     false,
		"otelcol_agentic_exporter_file_operation_failures": false,
		"otelcol_agentic_exporter_stream_state":            false,
	}
	allowedKeys := map[attribute.Key]bool{
		"candidate_type": true,
		"record_kind":    true,
		"service_name":   true,
		"reason":         true,
		"operation":      true,
	}
	for _, scope := range collected.ScopeMetrics {
		if scope.Scope.Name != meterScope {
			continue
		}
		for _, measured := range scope.Metrics {
			if _, ok := wantNames[measured.Name]; !ok {
				t.Errorf("unexpected instrument %q", measured.Name)
				continue
			}
			wantNames[measured.Name] = true
			assertMetricKind(t, measured)
			for _, attrs := range metricAttributeSets(measured.Data) {
				for _, kv := range attrs.ToSlice() {
					if !allowedKeys[kv.Key] {
						t.Errorf("instrument %q has unbounded attribute key %q", measured.Name, kv.Key)
					}
					if kv.Key == "service_name" && !allowedServiceName(kv.Value.AsString()) {
						t.Errorf("instrument %q exported disallowed service_name %q", measured.Name, kv.Value.AsString())
					}
				}
			}
		}
	}
	for name, seen := range wantNames {
		if !seen {
			t.Errorf("instrument %q was not collected", name)
		}
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != 1 {
		t.Errorf("candidate counter = %d, want 1", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 1 {
		t.Errorf("rejection counter = %d, want 1", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_ready_files_created"); got != 1 {
		t.Errorf("ready files counter = %d, want 1", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_ready_records_created"); got != 2 {
		t.Errorf("ready records counter = %d, want 2", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_ready_bytes_created"); got != 74 {
		t.Errorf("ready bytes counter = %d, want 74", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_file_operation_failures"); got != 1 {
		t.Errorf("file failure counter = %d, want 1", got)
	}
	if got := int64HistogramSum(collected, "otelcol_agentic_exporter_record_size"); got != 37 {
		t.Errorf("record size histogram sum = %d, want 37", got)
	}
}

func assertMetricKind(t *testing.T, measured metricdata.Metrics) {
	t.Helper()
	switch measured.Name {
	case "otelcol_agentic_exporter_record_size":
		if _, ok := measured.Data.(metricdata.Histogram[int64]); !ok {
			t.Errorf("%s data type = %T, want int64 histogram", measured.Name, measured.Data)
		}
	case "otelcol_agentic_exporter_open_batch_age":
		if _, ok := measured.Data.(metricdata.Gauge[float64]); !ok {
			t.Errorf("%s data type = %T, want float64 gauge", measured.Name, measured.Data)
		}
	case "otelcol_agentic_exporter_queue_high_water_bytes",
		"otelcol_agentic_exporter_unpublished_records",
		"otelcol_agentic_exporter_unpublished_bytes",
		"otelcol_agentic_exporter_open_batch_records",
		"otelcol_agentic_exporter_open_batch_bytes",
		"otelcol_agentic_exporter_ready_backlog_files",
		"otelcol_agentic_exporter_ready_backlog_bytes",
		"otelcol_agentic_exporter_stream_state":
		if _, ok := measured.Data.(metricdata.Gauge[int64]); !ok {
			t.Errorf("%s data type = %T, want int64 gauge", measured.Name, measured.Data)
		}
	default:
		if _, ok := measured.Data.(metricdata.Sum[int64]); !ok {
			t.Errorf("%s data type = %T, want int64 sum", measured.Name, measured.Data)
		}
	}
}

func int64HistogramSum(collected metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name == name {
				if histogram, ok := measured.Data.(metricdata.Histogram[int64]); ok {
					for _, point := range histogram.DataPoints {
						total += point.Sum
					}
				}
			}
		}
	}
	return total
}

func metricAttributeSets(data metricdata.Aggregation) []attribute.Set {
	var sets []attribute.Set
	switch points := data.(type) {
	case metricdata.Sum[int64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Gauge[float64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	}
	return sets
}
func TestTelemetryRecordsWithoutTraceExemplars(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	tel, err := newTelemetry(component.TelemetrySettings{MeterProvider: provider})
	if err != nil {
		t.Fatalf("newTelemetry() error = %v", err)
	}
	t.Cleanup(tel.close)

	traceContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), traceContext)
	tel.recordCandidate(ctx, candidateAction, recordSpan, "lightspeed-agentic-operator", 37)
	tel.recordRejection(ctx, candidateTranscript, recordSpanEvent, "", rejectQueueFull)
	tel.recordPublished(ctx, candidateAction, 1, 37)
	tel.recordFileOperationFailure(ctx, candidateTranscript, opRename)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			switch points := measured.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range points.DataPoints {
					if len(point.Exemplars) != 0 {
						t.Fatalf("%s emitted trace exemplars: %+v", measured.Name, point.Exemplars)
					}
				}
			case metricdata.Histogram[int64]:
				for _, point := range points.DataPoints {
					if len(point.Exemplars) != 0 {
						t.Fatalf("%s emitted trace exemplars: %+v", measured.Name, point.Exemplars)
					}
				}
			}
		}
	}
}
