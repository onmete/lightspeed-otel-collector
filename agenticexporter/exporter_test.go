package agenticexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestConsumeTracesRoutesAtomsWithoutMutatingSource(t *testing.T) {
	exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
	traces := eligibleTestTraces("unique-source-secret")
	marshaler := ptrace.JSONMarshaler{}
	before, err := marshaler.MarshalTraces(traces)
	if err != nil {
		t.Fatalf("MarshalTraces(before) error = %v", err)
	}

	if err := exp.consumeTraces(context.Background(), traces); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	after, err := marshaler.MarshalTraces(traces)
	if err != nil {
		t.Fatalf("MarshalTraces(after) error = %v", err)
	}
	if string(before) != string(after) {
		t.Fatal("consumeTraces mutated its pdata input")
	}

	actionRecords := queuedRecords(exp.actions)
	transcriptRecords := queuedRecords(exp.transcripts)
	if len(actionRecords) != 2 {
		t.Fatalf("Action records = %d, want 2", len(actionRecords))
	}
	if len(transcriptRecords) != 1 {
		t.Fatalf("Transcript records = %d, want 1", len(transcriptRecords))
	}
	assertProjectedAtom(t, actionRecords[0], "action", "span", nil)
	fallbackIndex := 1
	assertProjectedAtom(t, actionRecords[1], "action", "span_event", &fallbackIndex)
	transcriptIndex := 0
	assertProjectedAtom(t, transcriptRecords[0], "transcript", "span_event", &transcriptIndex)
}

func TestConsumeTracesAcceptsBothServices(t *testing.T) {
	for _, serviceName := range []string{"lightspeed-agentic-operator", "lightspeed-agentic-sandbox"} {
		t.Run(serviceName, func(t *testing.T) {
			exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
			traces := eligibleTestTraces("secret")
			traces.ResourceSpans().At(0).Resource().Attributes().PutStr("service.name", serviceName)
			if err := exp.consumeTraces(context.Background(), traces); err != nil {
				t.Fatalf("consumeTraces() error = %v", err)
			}
			if got := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts)); got != 3 {
				t.Fatalf("queued records = %d, want 3", got)
			}
		})
	}
}

func TestConsumeTracesRejectsEachIneligibleSpanReason(t *testing.T) {
	tests := map[string]func(ptrace.Traces){
		"unsupported service": func(traces ptrace.Traces) {
			traces.ResourceSpans().At(0).Resource().Attributes().PutStr("service.name", "other-service")
		},
		"missing uid": func(traces ptrace.Traces) {
			traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Remove("agenticrun.uid")
		},
		"invalid phase": func(traces ptrace.Traces) {
			traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("agenticrun.phase", "other")
		},
	}
	for name, makeIneligible := range tests {
		t.Run(name, func(t *testing.T) {
			exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
			traces := eligibleTestTraces("secret")
			makeIneligible(traces)
			if err := exp.consumeTraces(context.Background(), traces); err != nil {
				t.Fatalf("consumeTraces() error = %v", err)
			}
			if got := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts)); got != 0 {
				t.Fatalf("queued records = %d, want 0", got)
			}
		})
	}
}

func TestConsumeTracesCountsEveryIneligibleAtom(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), provider)
	traces := eligibleTestTraces("secret")
	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	span.Attributes().Remove("agenticrun.uid")

	if err := exp.consumeTraces(context.Background(), traces); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	if got := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts)); got != 0 {
		t.Fatalf("queued records = %d, want 0", got)
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 3 {
		t.Fatalf("rejections = %d, want one span plus two events", got)
	}
}

func TestConsumeTracesContainsProjectionAndQueueLoss(t *testing.T) {
	t.Run("invalid envelope", func(t *testing.T) {
		exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
		traces := eligibleTestTraces("secret")
		traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetTraceID(pcommon.TraceID{})
		if err := exp.consumeTraces(context.Background(), traces); err != nil {
			t.Fatalf("consumeTraces() error = %v", err)
		}
		if got := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts)); got != 0 {
			t.Fatalf("queued records = %d, want 0", got)
		}
	})

	t.Run("queue full", func(t *testing.T) {
		cfg := validTestConfig(t)
		cfg.MaxBacklogBytes = 1
		exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
		if !exp.actions.tryEnqueue([]byte{'\n'}) || !exp.transcripts.tryEnqueue([]byte{'\n'}) {
			t.Fatal("failed to fill test stream queues")
		}
		before := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts))
		if err := exp.consumeTraces(context.Background(), eligibleTestTraces("secret")); err != nil {
			t.Fatalf("consumeTraces() error = %v", err)
		}
		if got := len(queuedRecords(exp.actions)) + len(queuedRecords(exp.transcripts)); got != before {
			t.Fatalf("queued records = %d, want unchanged %d", got, before)
		}
	})
}

func TestExporterLifecycleContainsFilesystemLossAndLogsNoContent(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("unique-filesystem-secret"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg := &Config{
		ActionsDirectory:     filepath.Join(blockingFile, "actions"),
		TranscriptsDirectory: filepath.Join(blockingFile, "transcripts"),
		MaxBacklogBytes:      1 << 20,
	}
	exp := newTestAgenticExporter(t, cfg, logger, noop.NewMeterProvider())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), eligibleTestTraces("unique-pdata-secret")); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}

	allowedFields := map[string]bool{
		"candidate_type":      true,
		"from_state":          true,
		"state":               true,
		"operation":           true,
		"unpublished_records": true,
		"unpublished_bytes":   true,
	}
	for _, entry := range observed.All() {
		serialized := entry.Message + fmt.Sprint(entry.Context)
		if strings.Contains(serialized, "unique-pdata-secret") || strings.Contains(serialized, "unique-filesystem-secret") {
			t.Fatalf("log contains source content: %s", serialized)
		}
		for _, field := range entry.Context {
			if !allowedFields[field.Key] {
				t.Errorf("log field %q is not bounded", field.Key)
			}
		}
	}
}

func TestDisabledExporterLogsOnceAndSucceeds(t *testing.T) {
	core, observed := observer.New(zap.ErrorLevel)
	logger := zap.New(core)
	directory := t.TempDir()
	cfg := &Config{ActionsDirectory: directory, TranscriptsDirectory: directory, MaxBacklogBytes: 1024}
	exp := newTestAgenticExporter(t, cfg, logger, noop.NewMeterProvider())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("first start() error = %v", err)
	}
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("second start() error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), eligibleTestTraces("unique-disabled-secret")); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	if got := observed.Len(); got != 1 {
		t.Fatalf("deployment error logs = %d, want 1", got)
	}
	if strings.Contains(fmt.Sprint(observed.All()[0].Context), "unique-disabled-secret") {
		t.Fatal("deployment error log contains pdata content")
	}
	if err := exp.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
}

func newTestAgenticExporter(
	t *testing.T,
	cfg *Config,
	logger *zap.Logger,
	provider metric.MeterProvider,
) *agenticExporter {
	t.Helper()
	exp, err := newAgenticExporter(exporter.Settings{TelemetrySettings: component.TelemetrySettings{
		Logger:        logger,
		MeterProvider: provider,
	}}, cfg)
	if err != nil {
		t.Fatalf("newAgenticExporter() error = %v", err)
	}
	t.Cleanup(exp.telemetry.close)
	return exp
}

func validTestConfig(t *testing.T) *Config {
	t.Helper()
	root := t.TempDir()
	return &Config{
		ActionsDirectory:     filepath.Join(root, "actions"),
		TranscriptsDirectory: filepath.Join(root, "transcripts"),
		MaxBacklogBytes:      1 << 20,
	}
}

func eligibleTestTraces(secret string) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	resourceSpan.Resource().Attributes().PutStr("service.name", "lightspeed-agentic-operator")
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{2})
	span.SetStartTimestamp(1)
	span.SetEndTimestamp(2)
	span.SetName(secret)
	span.Attributes().PutStr("agenticrun.uid", "uid-secret")
	span.Attributes().PutStr("agenticrun.phase", "execution")

	transcript := span.Events().AppendEmpty()
	transcript.SetName("gen_ai.choice")
	transcript.SetTimestamp(3)
	transcript.Attributes().PutStr("gen_ai.completion", secret)
	fallback := span.Events().AppendEmpty()
	fallback.SetName(secret)
	fallback.SetTimestamp(4)
	return traces
}

func queuedRecords(writer *streamWriter) [][]byte {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	records := make([][]byte, len(writer.queued))
	for index := range writer.queued {
		records[index] = append([]byte(nil), writer.queued[index].data...)
	}
	return records
}

func assertProjectedAtom(t *testing.T, data []byte, candidate, kind string, eventIndex *int) {
	t.Helper()
	var decoded struct {
		CandidateType string `json:"candidate_type"`
		RecordKind    string `json:"record_kind"`
		EventIndex    *int   `json:"event_index"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded.CandidateType != candidate || decoded.RecordKind != kind {
		t.Fatalf("projected atom = (%q, %q), want (%q, %q)", decoded.CandidateType, decoded.RecordKind, candidate, kind)
	}
	if eventIndex == nil {
		if decoded.EventIndex != nil {
			t.Fatalf("event_index = %v, want nil", *decoded.EventIndex)
		}
		return
	}
	if decoded.EventIndex == nil || *decoded.EventIndex != *eventIndex {
		t.Fatalf("event_index = %v, want %d", decoded.EventIndex, *eventIndex)
	}
}

func int64MetricSum(collected metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name != name {
				continue
			}
			if sum, ok := measured.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					total += point.Value
				}
			}
		}
	}
	return total
}
