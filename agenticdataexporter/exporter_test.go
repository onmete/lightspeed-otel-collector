package agenticdataexporter

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{
			name: "valid",
			cfg: Config{
				ActionsDir:     "/var/lib/lightspeed-data-collection/actions",
				TranscriptsDir: "/var/lib/lightspeed-data-collection/transcripts",
			},
			want: true,
		},
		{
			name: "relative actions directory",
			cfg: Config{
				ActionsDir:     "actions",
				TranscriptsDir: "/tmp/transcripts",
			},
		},
		{
			name: "same directories",
			cfg: Config{
				ActionsDir:     "/tmp/data",
				TranscriptsDir: "/tmp/./data",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.Validate() == nil; got != tt.want {
				t.Fatalf("Validate() success = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTranscriptEventPredicates(t *testing.T) {
	allInput := []string{
		"gen_ai.input.system_prompt",
		"gen_ai.input.prompt",
		"gen_ai.input.context",
		"gen_ai.input.output_schema",
	}
	allOutput := []string{
		"gen_ai.output.value",
		"gen_ai.request.model",
		"gen_ai.response.model",
		"gen_ai.usage.input_tokens",
		"gen_ai.usage.output_tokens",
		"gen_ai.usage.reasoning_tokens",
	}
	tests := []struct {
		name      string
		eventName string
		attrs     []string
		want      bool
	}{
		{name: "input", eventName: "gen_ai.input", attrs: allInput, want: true},
		{name: "input missing field", eventName: "gen_ai.input", attrs: allInput[:3]},
		{name: "choice completion", eventName: "gen_ai.choice", attrs: []string{"gen_ai.completion"}, want: true},
		{name: "choice reasoning", eventName: "gen_ai.choice", attrs: []string{"gen_ai.reasoning_content"}, want: true},
		{name: "choice missing content", eventName: "gen_ai.choice"},
		{name: "tool call", eventName: "gen_ai.tool.call", attrs: []string{"gen_ai.tool.name", "gen_ai.tool.call.id", "tool.input"}, want: true},
		{name: "tool result", eventName: "gen_ai.tool.result", attrs: []string{"gen_ai.tool.name", "gen_ai.tool.call.id", "tool.status", "tool.output"}, want: true},
		{name: "skill loaded", eventName: "gen_ai.skill.loaded", attrs: []string{"gen_ai.skill.name"}, want: true},
		{name: "skill used", eventName: "gen_ai.skill.used", attrs: []string{"gen_ai.skill.name"}, want: true},
		{name: "output", eventName: "gen_ai.output", attrs: allOutput, want: true},
		{name: "unknown event", eventName: "agenticrun.execution.completed", attrs: allOutput},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			span := ptrace.NewSpan()
			event := span.Events().AppendEmpty()
			event.SetName(tt.eventName)
			for _, key := range tt.attrs {
				event.Attributes().PutStr(key, "value")
			}
			if got := isTranscriptEvent(event); got != tt.want {
				t.Fatalf("isTranscriptEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumeTracesSeparatesActionsAndTranscripts(t *testing.T) {
	actionsDir := t.TempDir()
	transcriptsDir := t.TempDir()
	exporter := newTestExporter(actionsDir, transcriptsDir)
	if err := exporter.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", serviceSandbox)
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()
	span.SetName("execute_tool kubectl")
	span.SetStartTimestamp(pcommon.Timestamp(100))
	span.SetEndTimestamp(pcommon.Timestamp(200))
	span.SetTraceID(traceID(1))
	span.SetSpanID(spanID(1))
	span.Attributes().PutStr("agenticrun.uid", "run-123")
	span.Attributes().PutStr("agenticrun.phase", "execution")

	input := span.Events().AppendEmpty()
	input.SetName("gen_ai.input")
	for _, key := range []string{
		"gen_ai.input.system_prompt",
		"gen_ai.input.prompt",
		"gen_ai.input.context",
		"gen_ai.input.output_schema",
	} {
		input.Attributes().PutStr(key, "value")
	}
	input.SetTimestamp(pcommon.Timestamp(101))

	other := span.Events().AppendEmpty()
	other.SetName("agenticrun.execution.completed")
	other.SetTimestamp(pcommon.Timestamp(102))

	choice := span.Events().AppendEmpty()
	choice.SetName("gen_ai.choice")
	choice.Attributes().PutStr("gen_ai.reasoning_content", "thinking")
	choice.SetTimestamp(pcommon.Timestamp(103))

	if err := exporter.consumeTraces(context.Background(), traces); err != nil {
		t.Fatal(err)
	}
	if err := exporter.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	actionRecords := readRecords(t, actionsDir)
	if len(actionRecords) != 2 {
		t.Fatalf("got %d action records, want 2", len(actionRecords))
	}
	if actionRecords[0]["record_kind"] != "span" {
		t.Fatalf("first action record kind = %v, want span", actionRecords[0]["record_kind"])
	}
	if actionRecords[1]["event_index"] != float64(1) || actionRecords[1]["name"] != "agenticrun.execution.completed" {
		t.Fatalf("unexpected non-transcript event: %#v", actionRecords[1])
	}

	transcriptRecords := readRecords(t, transcriptsDir)
	if len(transcriptRecords) != 2 {
		t.Fatalf("got %d transcript records, want 2", len(transcriptRecords))
	}
	transcript := transcriptRecords[0]
	if transcript["candidate_type"] != transcriptCandidate || transcript["record_kind"] != "span_event" {
		t.Fatalf("unexpected transcript envelope: %#v", transcript)
	}
	if transcript["event_index"] != float64(0) || transcript["name"] != "gen_ai.input" {
		t.Fatalf("transcript event index/name = %v/%v, want 0/gen_ai.input", transcript["event_index"], transcript["name"])
	}
	if transcriptRecords[1]["event_index"] != float64(2) || transcriptRecords[1]["name"] != "gen_ai.choice" {
		t.Fatalf("unexpected choice transcript: %#v", transcriptRecords[1])
	}
}

func TestConsumeTracesFiltersByResourceAndSpanCorrelation(t *testing.T) {
	actionsDir := t.TempDir()
	transcriptsDir := t.TempDir()
	exporter := newTestExporter(actionsDir, transcriptsDir)
	if err := exporter.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	traces := ptrace.NewTraces()
	appendSpan := func(service, uid, phase string) {
		resourceSpans := traces.ResourceSpans().AppendEmpty()
		resourceSpans.Resource().Attributes().PutStr("service.name", service)
		span := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.Attributes().PutStr("agenticrun.uid", uid)
		span.Attributes().PutStr("agenticrun.phase", phase)
		span.Events().AppendEmpty().SetName("gen_ai.choice")
	}
	appendSpan("other-service", "run-1", "execution")
	appendSpan(serviceOperator, "", "execution")
	appendSpan(serviceOperator, "run-2", "unknown")
	appendSpan(serviceOperator, "run-3", "execution")

	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", serviceOperator)
	resourceSpans.Resource().Attributes().PutStr("agenticrun.uid", "resource-run")
	resourceSpans.Resource().Attributes().PutStr("agenticrun.phase", "execution")
	resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty().Events().AppendEmpty().SetName("gen_ai.choice")

	if err := exporter.consumeTraces(context.Background(), traces); err != nil {
		t.Fatal(err)
	}
	if err := exporter.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	actionRecords := readRecords(t, actionsDir)
	if len(actionRecords) != 2 {
		t.Fatalf("got %d action records, want eligible span and event", len(actionRecords))
	}
	if actionRecords[0]["agenticrun_uid"] != "run-3" || actionRecords[1]["agenticrun_uid"] != "run-3" {
		t.Fatalf("filtered records contain unexpected run IDs: %#v", actionRecords)
	}
	if got := len(readRecords(t, transcriptsDir)); got != 0 {
		t.Fatalf("got %d transcript records, want 0", got)
	}
}

func TestCandidateAttributesUseOTLPJSON(t *testing.T) {
	actionsDir := t.TempDir()
	transcriptsDir := t.TempDir()
	exporter := newTestExporter(actionsDir, transcriptsDir)
	if err := exporter.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", serviceSandbox)
	resourceSpans.Resource().Attributes().PutInt("resource.int", 42)
	resourceSpans.Resource().Attributes().PutEmptyBytes("resource.bytes").FromRaw([]byte{1, 2})
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scopeSpans.Scope().Attributes().PutBool("scope.bool", true)
	span := scopeSpans.Spans().AppendEmpty()
	span.Attributes().PutStr("agenticrun.uid", "run-123")
	span.Attributes().PutStr("agenticrun.phase", "execution")
	span.Attributes().PutInt("span.int", 7)
	event := span.Events().AppendEmpty()
	event.SetName("gen_ai.choice")
	event.Attributes().PutStr("gen_ai.completion", "done")
	event.Attributes().PutEmptyBytes("event.bytes").FromRaw([]byte{3, 4})

	if err := exporter.consumeTraces(context.Background(), traces); err != nil {
		t.Fatal(err)
	}
	if err := exporter.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	actionRecords := readRecords(t, actionsDir)
	resourceInt := candidateAttributeValue(t, actionRecords[0], "resource", "resource.int")
	if resourceInt["intValue"] != float64(42) {
		t.Fatalf("resource int was not encoded as intValue: %#v", resourceInt)
	}
	resourceBytes := candidateAttributeValue(t, actionRecords[0], "resource", "resource.bytes")
	if resourceBytes["bytesValue"] != "AQI=" {
		t.Fatalf("resource bytes were not encoded as bytesValue: %#v", resourceBytes)
	}
	scopeBool := candidateAttributeValue(t, actionRecords[0], "scope", "scope.bool")
	if scopeBool["boolValue"] != true {
		t.Fatalf("scope bool was not encoded as boolValue: %#v", scopeBool)
	}
	spanInt := candidateAttributeValue(t, actionRecords[0], "span", "span.int")
	if spanInt["intValue"] != float64(7) {
		t.Fatalf("span int was not encoded as intValue: %#v", spanInt)
	}

	transcriptRecords := readRecords(t, transcriptsDir)
	eventBytes := candidateAttributeValue(t, transcriptRecords[0], "event", "event.bytes")
	if eventBytes["bytesValue"] != "AwQ=" {
		t.Fatalf("event bytes were not encoded as bytesValue: %#v", eventBytes)
	}
}

func TestStartRemovesAbandonedTemporaryFiles(t *testing.T) {
	actionsDir := t.TempDir()
	transcriptsDir := t.TempDir()
	for _, dir := range []string{actionsDir, transcriptsDir} {
		if err := os.WriteFile(filepath.Join(dir, ".550e8400-e29b-41d4-a716-446655440000.tmp"), []byte("partial\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	exporter := newTestExporter(actionsDir, transcriptsDir)
	if err := exporter.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	defer exporter.shutdown(context.Background())
	for _, dir := range []string{actionsDir, transcriptsDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("temporary files remain in %s: %v", dir, entries)
		}
	}
}

func TestIsolatingTracesExporterDropsQueueFull(t *testing.T) {
	base, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error {
		return exporterhelper.ErrQueueIsFull
	})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &isolatingTracesExporter{Traces: &testTracesExporter{Traces: base}}
	if err := wrapped.ConsumeTraces(context.Background(), ptrace.NewTraces()); err != nil {
		t.Fatalf("queue-full error propagated: %v", err)
	}
}

type testTracesExporter struct {
	consumer.Traces
}

func (*testTracesExporter) Start(context.Context, component.Host) error { return nil }

func (*testTracesExporter) Shutdown(context.Context) error { return nil }
func newTestExporter(actionsDir, transcriptsDir string) *traceExporter {
	return &traceExporter{config: &Config{
		ActionsDir:     actionsDir,
		TranscriptsDir: transcriptsDir,
	}}
}

func candidateAttributeValue(t *testing.T, record map[string]any, namespace, key string) map[string]any {
	t.Helper()
	attributes, ok := record["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("record attributes have type %T, want map[string]any", record["attributes"])
	}
	return attributeValue(t, attributes[namespace], key)
}

func attributeValue(t *testing.T, attributes any, key string) map[string]any {
	t.Helper()
	list, ok := attributes.([]any)
	if !ok {
		t.Fatalf("attributes have type %T, want []any", attributes)
	}
	for _, raw := range list {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if entry["key"] != key {
			continue
		}
		value, ok := entry["value"].(map[string]any)
		if !ok {
			t.Fatalf("attribute %q has value type %T, want map[string]any", key, entry["value"])
		}
		return value
	}
	t.Fatalf("attribute %q not found in %#v", key, list)
	return nil
}

func readRecords(t *testing.T, dir string) []map[string]any {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var records []map[string]any
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		file, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return records
}

func traceID(value byte) pcommon.TraceID {
	var id pcommon.TraceID
	id[15] = value
	return id
}

func spanID(value byte) pcommon.SpanID {
	var id pcommon.SpanID
	id[7] = value
	return id
}
