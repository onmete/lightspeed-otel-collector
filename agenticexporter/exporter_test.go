package agenticexporter

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNativeSpoolKeepsUnknownGenAIEventsAndRawContext(t *testing.T) {
	cfg := validTestConfig(t)
	exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
	input := eligibleTestTraces("native-test")
	resource := input.ResourceSpans().At(0)
	resource.SetSchemaUrl("resource/schema")
	resource.Resource().Attributes().PutStr("same", "resource")
	scope := resource.ScopeSpans().At(0)
	scope.SetSchemaUrl("scope/schema")
	scope.Scope().SetName("raw-source")
	scope.Scope().Attributes().PutStr("same", "scope")
	span := scope.Spans().At(0)
	span.SetParentSpanID(pcommon.SpanID{3})
	span.SetEndTimestamp(10)
	span.Attributes().PutStr("same", "span")
	span.Attributes().PutStr("gen_ai.operation.name", "future_operation")
	span.Attributes().PutStr("gen_ai.input.messages", "first line\nsecond line")
	span.Attributes().PutInt("gen_ai.usage.input_tokens", 42)
	span.Events().At(0).SetName("gen_ai.future_input")
	span.Events().At(1).SetName("gen_ai.future_output")
	span.Events().At(1).SetTimestamp(span.Events().At(0).Timestamp())
	span.Events().At(0).Attributes().PutStr("same", "event")

	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := exp.consumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	documents := readNativeReady(t, cfg.Directory)
	if len(documents) != 1 {
		t.Fatalf("documents = %d; want one indivisible span", len(documents))
	}
	gotResource := documents[0].ResourceSpans().At(0)
	gotScope := gotResource.ScopeSpans().At(0)
	got := gotScope.Spans().At(0)
	if gotResource.SchemaUrl() != "resource/schema" || gotScope.SchemaUrl() != "scope/schema" ||
		gotScope.Scope().Name() != "raw-source" {
		t.Fatal("source context lost")
	}
	if got.TraceID() != (pcommon.TraceID{1}) || got.SpanID() != (pcommon.SpanID{2}) ||
		got.ParentSpanID() != (pcommon.SpanID{3}) || got.StartTimestamp() != 1 || got.EndTimestamp() != 10 {
		t.Fatal("graph/timing evidence lost")
	}
	assertString := func(attributes pcommon.Map, key, want string) {
		t.Helper()
		value, exists := attributes.Get(key)
		if !exists || value.Type() != pcommon.ValueTypeStr || value.Str() != want {
			t.Fatalf("attribute %q lost or changed", key)
		}
	}
	assertString(gotResource.Resource().Attributes(), "same", "resource")
	assertString(gotScope.Scope().Attributes(), "same", "scope")
	assertString(got.Attributes(), "same", "span")
	assertString(got.Attributes(), "agenticrun.uid", "uid-secret")
	assertString(got.Attributes(), "agenticrun.phase", "execution")
	assertString(got.Attributes(), "gen_ai.input.messages", "first line\nsecond line")
	tokens, exists := got.Attributes().Get("gen_ai.usage.input_tokens")
	if !exists || tokens.Type() != pcommon.ValueTypeInt || tokens.Int() != 42 {
		t.Fatal("typed evidence lost")
	}
	if got.Events().Len() != 2 || got.Events().At(0).Name() != "gen_ai.future_input" ||
		got.Events().At(1).Name() != "gen_ai.future_output" ||
		got.Events().At(0).Timestamp() != got.Events().At(1).Timestamp() {
		t.Fatal("unknown events or original equal-time order lost")
	}
	assertString(got.Events().At(0).Attributes(), "same", "event")
}

func TestNativeSpoolRetainsEvidenceAfterSourceReuse(t *testing.T) {
	cfg := validTestConfig(t)
	exp, err := newAgenticExporter(exporter.Settings{TelemetrySettings: component.TelemetrySettings{
		Logger: zap.NewNop(), MeterProvider: noop.NewMeterProvider(),
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exp.shutdown(context.Background()) })
	exp.encodingMu.Lock()
	exp.encodingAccepting = true
	exp.encodingMu.Unlock()
	input := eligibleTestTraces("at-admission")
	if err := exp.consumeTraces(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	sourceResource := input.ResourceSpans().At(0)
	sourceSpan := sourceResource.ScopeSpans().At(0).Spans().At(0)
	sourceResource.Resource().Attributes().PutStr("service.name", "source-reused")
	sourceSpan.SetName("source-reused")
	sourceSpan.Attributes().PutStr("agenticrun.uid", "source-reused")
	sourceSpan.Events().At(1).SetName("source-reused")
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	documents := readNativeReady(t, cfg.Directory)
	if len(documents) != 1 {
		t.Fatalf("documents = %d; want one admitted span", len(documents))
	}
	resource := documents[0].ResourceSpans().At(0)
	span := resource.ScopeSpans().At(0).Spans().At(0)
	service, serviceOK := resource.Resource().Attributes().Get("service.name")
	uid, uidOK := span.Attributes().Get("agenticrun.uid")
	if !serviceOK || service.Str() != "lightspeed-agentic-sandbox" ||
		!uidOK || uid.Str() != "uid-secret" || span.Name() != "at-admission" ||
		span.Events().At(1).Name() != "at-admission" {
		t.Fatal("source reuse corrupted admitted evidence")
	}
}

func TestNativeSpoolPreservesStatusLinksFlagsDroppedCountsAndTypedValues(t *testing.T) {
	cfg := validTestConfig(t)
	exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
	input := eligibleTestTraces("native-metadata")
	resource := input.ResourceSpans().At(0)
	resource.Resource().SetDroppedAttributesCount(1)
	scope := resource.ScopeSpans().At(0)
	scope.Scope().SetName("native-metadata-scope")
	scope.Scope().SetVersion("1.2.3")
	scope.Scope().SetDroppedAttributesCount(2)
	span := scope.Spans().At(0)
	span.SetKind(ptrace.SpanKindServer)
	span.SetFlags(1)
	span.SetDroppedAttributesCount(3)
	span.SetDroppedEventsCount(4)
	span.SetDroppedLinksCount(5)
	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("failed")
	span.TraceState().FromRaw("vendor=value")

	attributes := span.Attributes()
	attributes.PutStr("string", "text")
	attributes.PutBool("bool", true)
	attributes.PutInt("intPositive", 9223372036854775807)
	attributes.PutInt("intNegative", -9223372036854775807)
	attributes.PutDouble("double", 1.5)
	attributes.PutDouble("nan", math.NaN())
	attributes.PutDouble("posInfinity", math.Inf(1))
	attributes.PutDouble("negInfinity", math.Inf(-1))
	attributes.PutEmptyBytes("bytes").FromRaw([]byte{1, 2, 3})
	array := attributes.PutEmptySlice("array")
	array.AppendEmpty().SetStr("nested")
	nestedArray := array.AppendEmpty().SetEmptySlice()
	nestedArray.AppendEmpty().SetInt(8)
	kvlist := attributes.PutEmptyMap("kvlist")
	kvlist.PutStr("first", "one")
	kvlist.PutInt("second", -2)
	attributes.PutEmpty("empty")

	link := span.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30})
	link.SetSpanID(pcommon.SpanID{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38})
	link.SetFlags(2)
	link.SetDroppedAttributesCount(6)
	link.Attributes().PutStr("same", "link")
	link.TraceState().FromRaw("link=value")
	span.Events().At(0).SetDroppedAttributesCount(7)

	documents := publishTestTraces(t, cfg, exp, input)
	if len(documents) != 1 {
		t.Fatalf("documents = %d; want one span document", len(documents))
	}
	gotResource := documents[0].ResourceSpans().At(0)
	gotScope := gotResource.ScopeSpans().At(0)
	got := gotScope.Spans().At(0)
	if gotResource.Resource().DroppedAttributesCount() != 1 || gotScope.Scope().DroppedAttributesCount() != 2 ||
		got.DroppedAttributesCount() != 3 || got.DroppedEventsCount() != 4 || got.DroppedLinksCount() != 5 ||
		got.Events().At(0).DroppedAttributesCount() != 7 {
		t.Fatal("resource, scope, span, or event dropped counts lost")
	}
	if got.Kind() != ptrace.SpanKindServer || got.Status().Code() != ptrace.StatusCodeError ||
		got.Status().Message() != "failed" || got.TraceState().AsRaw() != "vendor=value" || uint32(got.Flags()) != 1 {
		t.Fatal("kind, status, trace state, or flags lost")
	}
	if got.Links().Len() != 1 {
		t.Fatalf("links = %d; want one", got.Links().Len())
	}
	gotLink := got.Links().At(0)
	if gotLink.TraceID() != link.TraceID() || gotLink.SpanID() != link.SpanID() ||
		gotLink.TraceState().AsRaw() != "link=value" || uint32(gotLink.Flags()) != 2 ||
		gotLink.DroppedAttributesCount() != 6 {
		t.Fatal("link identity or metadata lost")
	}
	linkAttr, linkAttrOK := gotLink.Attributes().Get("same")
	if !linkAttrOK || linkAttr.Str() != "link" {
		t.Fatal("link attributes lost")
	}

	assertType := func(key string, want pcommon.ValueType) pcommon.Value {
		t.Helper()
		value, ok := got.Attributes().Get(key)
		if !ok {
			t.Fatalf("attribute %q missing", key)
		}
		if value.Type() != want {
			t.Fatalf("attribute %q type = %v, want %v", key, value.Type(), want)
		}
		return value
	}
	if assertType("string", pcommon.ValueTypeStr).Str() != "text" ||
		!assertType("bool", pcommon.ValueTypeBool).Bool() ||
		assertType("intPositive", pcommon.ValueTypeInt).Int() != 9223372036854775807 ||
		assertType("intNegative", pcommon.ValueTypeInt).Int() != -9223372036854775807 ||
		assertType("double", pcommon.ValueTypeDouble).Double() != 1.5 ||
		!math.IsNaN(assertType("nan", pcommon.ValueTypeDouble).Double()) ||
		!math.IsInf(assertType("posInfinity", pcommon.ValueTypeDouble).Double(), 1) ||
		!math.IsInf(assertType("negInfinity", pcommon.ValueTypeDouble).Double(), -1) {
		t.Fatal("scalar attribute values lost")
	}
	if gotBytes := assertType("bytes", pcommon.ValueTypeBytes).Bytes().AsRaw(); !bytes.Equal(gotBytes, []byte{1, 2, 3}) {
		t.Fatalf("bytes attribute = %v", gotBytes)
	}
	gotArray := assertType("array", pcommon.ValueTypeSlice).Slice()
	if gotArray.Len() != 2 || gotArray.At(0).Type() != pcommon.ValueTypeStr || gotArray.At(0).Str() != "nested" ||
		gotArray.At(1).Type() != pcommon.ValueTypeSlice || gotArray.At(1).Slice().At(0).Int() != 8 {
		t.Fatal("nested array attribute lost")
	}
	gotMap := assertType("kvlist", pcommon.ValueTypeMap).Map()
	first, firstOK := gotMap.Get("first")
	second, secondOK := gotMap.Get("second")
	if !firstOK || first.Type() != pcommon.ValueTypeStr || first.Str() != "one" ||
		!secondOK || second.Type() != pcommon.ValueTypeInt || second.Int() != -2 {
		t.Fatal("map attribute lost")
	}
	assertType("empty", pcommon.ValueTypeEmpty)
}

func TestConsumeTracesDoesNotMutateSource(t *testing.T) {
	exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	input := eligibleTestTraces("unique-source-secret")
	marshaler := ptrace.JSONMarshaler{}
	before, err := marshaler.MarshalTraces(input)
	if err != nil {
		t.Fatalf("MarshalTraces(before) error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), input); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	waitUntil(t, exp.encodingIdle)
	after, err := marshaler.MarshalTraces(input)
	if err != nil {
		t.Fatalf("MarshalTraces(after) error = %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("consumeTraces mutated its pdata input")
	}
}

func TestConsumeTracesAcceptsBothServices(t *testing.T) {
	for _, serviceName := range []string{"lightspeed-agentic-operator", "lightspeed-agentic-sandbox"} {
		t.Run(serviceName, func(t *testing.T) {
			cfg := validTestConfig(t)
			exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
			input := eligibleTestTraces("accepted")
			input.ResourceSpans().At(0).Resource().Attributes().PutStr("service.name", serviceName)
			documents := publishTestTraces(t, cfg, exp, input)
			if len(documents) != 1 || documents[0].SpanCount() != 1 {
				t.Fatalf("native documents = %d; want one span document", len(documents))
			}
			resource := documents[0].ResourceSpans().At(0)
			got, ok := resource.Resource().Attributes().Get("service.name")
			if !ok || got.Type() != pcommon.ValueTypeStr || got.Str() != serviceName {
				t.Fatalf("service.name = %v, want %q", got, serviceName)
			}
		})
	}
}

func TestConsumeTracesAcceptsUnknownAndMetadataOnlyOperations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ptrace.Span)
	}{
		{
			name: "unknown operation without message content",
			mutate: func(span ptrace.Span) {
				span.Attributes().PutStr("gen_ai.operation.name", "future_operation")
				span.Attributes().Remove("gen_ai.input.messages")
				span.Attributes().Remove("gen_ai.output.messages")
			},
		},
		{
			name: "missing operation and content",
			mutate: func(span ptrace.Span) {
				span.Attributes().Remove("gen_ai.operation.name")
				span.Attributes().Remove("gen_ai.input.messages")
				span.Attributes().Remove("gen_ai.output.messages")
			},
		},
		{
			name: "metadata only",
			mutate: func(span ptrace.Span) {
				span.Attributes().PutStr("gen_ai.operation.name", "chat")
				span.Attributes().Remove("gen_ai.input.messages")
				span.Attributes().Remove("gen_ai.output.messages")
				span.Attributes().PutStr("gen_ai.system_instructions", "system")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig(t)
			exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
			input := eligibleTestTraces("unknown-operation")
			span := input.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
			tt.mutate(span)
			documents := publishTestTraces(t, cfg, exp, input)
			if len(documents) != 1 || documents[0].SpanCount() != 1 {
				t.Fatalf("native documents = %d; want one span document", len(documents))
			}
			if got := documents[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Events().Len(); got != 2 {
				t.Fatalf("nested events = %d; want both attached events", got)
			}
		})
	}
}

func TestConsumeTracesRejectsIneligibleSpanOnce(t *testing.T) {
	tests := []struct {
		name   string
		reason rejectionReason
		mutate func(ptrace.Traces)
	}{
		{
			name:   "unsupported service",
			reason: rejectUnsupportedService,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).Resource().Attributes().PutStr("service.name", "other-service")
			},
		},
		{
			name:   "missing uid",
			reason: rejectMissingUID,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Remove("agenticrun.uid")
			},
		},
		{
			name:   "empty uid",
			reason: rejectMissingUID,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("agenticrun.uid", "")
			},
		},
		{
			name:   "non-string uid",
			reason: rejectMissingUID,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutInt("agenticrun.uid", 1)
			},
		},
		{
			name:   "missing phase",
			reason: rejectInvalidPhase,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Remove("agenticrun.phase")
			},
		},
		{
			name:   "non-string phase",
			reason: rejectInvalidPhase,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutInt("agenticrun.phase", 1)
			},
		},
		{
			name:   "invalid phase",
			reason: rejectInvalidPhase,
			mutate: func(traces ptrace.Traces) {
				traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutStr("agenticrun.phase", "planning")
			},
		},
		{
			name:   "resource correlation is not a fallback",
			reason: rejectMissingUID,
			mutate: func(traces ptrace.Traces) {
				resource := traces.ResourceSpans().At(0).Resource()
				resource.Attributes().PutStr("agenticrun.uid", "resource-uid")
				resource.Attributes().PutStr("agenticrun.phase", "execution")
				span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
				span.Attributes().Remove("agenticrun.uid")
				span.Attributes().Remove("agenticrun.phase")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), provider)
			input := eligibleTestTraces("secret")
			tt.mutate(input)
			if err := exp.consumeTraces(context.Background(), input); err != nil {
				t.Fatalf("consumeTraces() error = %v", err)
			}
			waitUntil(t, exp.encodingIdle)
			if got := len(queuedRecords(exp.writer)); got != 0 {
				t.Fatalf("queued documents = %d, want none", got)
			}
			var collected metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &collected); err != nil {
				t.Fatalf("Collect() error = %v", err)
			}
			if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 1 {
				t.Fatalf("rejections = %d, want one span document, including its events", got)
			}
			if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections", map[string]string{"reason": string(tt.reason)}); got != 1 {
				t.Fatalf("rejections for %s = %d, want 1", tt.reason, got)
			}
		})
	}
}

func TestConsumeTracesRejectsEmptyTraceAndSpanIDsOnce(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ptrace.Span)
	}{
		{
			name: "empty trace id",
			mutate: func(span ptrace.Span) {
				span.SetTraceID(pcommon.TraceID{})
			},
		},
		{
			name: "empty span id",
			mutate: func(span ptrace.Span) {
				span.SetSpanID(pcommon.SpanID{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), provider)
			input := eligibleTestTraces("secret")
			span := input.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
			tt.mutate(span)
			if err := exp.consumeTraces(context.Background(), input); err != nil {
				t.Fatalf("consumeTraces() error = %v", err)
			}
			var collected metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &collected); err != nil {
				t.Fatalf("Collect() error = %v", err)
			}
			if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections", map[string]string{"reason": string(rejectInvalidTraceIdentity)}); got != 1 {
				t.Fatalf("invalid identity rejections = %d, want one document", got)
			}
			if got := len(queuedRecords(exp.writer)); got != 0 {
				t.Fatalf("queued documents = %d, want none", got)
			}
		})
	}
}

func TestConsumeTracesAccountsForOneNativeDocumentPerSpan(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), provider)
	if err := exp.consumeTraces(context.Background(), eligibleTestTraces("secret")); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	waitUntil(t, exp.encodingIdle)
	records := queuedRecords(exp.writer)
	if len(records) != 1 {
		t.Fatalf("queued documents = %d, want one span document", len(records))
	}
	decoder := ptrace.JSONUnmarshaler{}
	documents, err := decoder.UnmarshalTraces(records[0])
	if err != nil {
		t.Fatalf("UnmarshalTraces() error = %v", err)
	}
	if documents.SpanCount() != 1 || documents.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Events().Len() != 2 {
		t.Fatal("one admitted span did not retain its nested events")
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != 1 {
		t.Fatalf("admitted documents = %d, want one", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 0 {
		t.Fatalf("rejected documents = %d, want zero", got)
	}
	if got := int64HistogramSum(collected, "otelcol_agentic_exporter_record_size"); got != int64(len(records[0])) {
		t.Fatalf("document byte count = %d, want %d", got, len(records[0]))
	}
}
func TestEncodedRecordSizeLimitBoundary(t *testing.T) {
	const recordLimitBytes = 120_000_000
	const nulJSONBytes = 6

	emptyTraces := eligibleTestTraces("encoded-record-size-boundary")
	emptySpan := emptyTraces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	emptySpan.Attributes().PutStr("gen_ai.input.messages", "")
	marshaler := ptrace.JSONMarshaler{}
	emptyJSON, err := marshaler.MarshalTraces(emptyTraces)
	if err != nil {
		t.Fatalf("MarshalTraces(empty fixture) error = %v", err)
	}
	emptyLineBytes := len(emptyJSON) + 1

	tests := []struct {
		name              string
		lineBytes         int
		wantQueuedRecords int
		wantRejections    int64
	}{
		{name: "at limit", lineBytes: recordLimitBytes, wantQueuedRecords: 1},
		{name: "one byte above", lineBytes: recordLimitBytes + 1, wantRejections: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), provider)

			input := eligibleTestTraces("encoded-record-size-boundary")
			span := input.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
			payloadJSONBytes := tt.lineBytes - emptyLineBytes
			payload := strings.Repeat("\x00", payloadJSONBytes/nulJSONBytes) +
				strings.Repeat("x", payloadJSONBytes%nulJSONBytes)
			span.Attributes().PutStr("gen_ai.input.messages", payload)
			if err := exp.consumeTraces(context.Background(), input); err != nil {
				t.Fatalf("consumeTraces() error = %v", err)
			}

			drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			exp.stopEncoding(drainCtx)
			select {
			case <-exp.encodingDone:
			case <-drainCtx.Done():
				t.Fatalf("encoding did not drain: %v", drainCtx.Err())
			}

			var collected metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &collected); err != nil {
				t.Fatalf("Collect() error = %v", err)
			}

			exp.writer.mu.Lock()
			queuedRecords := len(exp.writer.queued)
			unpublishedRecords := exp.writer.unpublishedRecords
			unpublishedBytes := exp.writer.unpublishedBytes
			recordBytes := 0
			recordHasFinalLF := false
			if queuedRecords != 0 {
				record := exp.writer.queued[0].data
				recordBytes = len(record)
				recordHasFinalLF = recordBytes != 0 && record[recordBytes-1] == '\n'
			}
			exp.writer.mu.Unlock()

			if queuedRecords != tt.wantQueuedRecords ||
				unpublishedRecords != int64(tt.wantQueuedRecords) {
				t.Fatalf("queued records = %d/%d, want %d", queuedRecords, unpublishedRecords, tt.wantQueuedRecords)
			}
			wantAdmissionBytes := int64(0)
			if tt.wantQueuedRecords != 0 {
				wantAdmissionBytes = int64(tt.lineBytes)
				if recordBytes != tt.lineBytes || !recordHasFinalLF {
					t.Fatalf("accepted native line = %d bytes, final LF = %t; want %d bytes including LF",
						recordBytes, recordHasFinalLF, tt.lineBytes)
				}
			}
			if unpublishedBytes != wantAdmissionBytes {
				t.Fatalf("queued native bytes = %d, want %d", unpublishedBytes, wantAdmissionBytes)
			}
			if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != int64(tt.wantQueuedRecords) {
				t.Fatalf("admissions = %d, want %d", got, tt.wantQueuedRecords)
			}
			if got := int64HistogramSum(collected, "otelcol_agentic_exporter_record_size"); got != wantAdmissionBytes {
				t.Fatalf("admitted native bytes = %d, want %d", got, wantAdmissionBytes)
			}
			if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != tt.wantRejections {
				t.Fatalf("rejections = %d, want %d", got, tt.wantRejections)
			}
			if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections",
				map[string]string{"reason": "record_too_large"}); got != tt.wantRejections {
				t.Fatalf("record_too_large rejections = %d, want %d", got, tt.wantRejections)
			}
		})
	}
}

func TestConsumeTracesRejectsOneDocumentWhenWriterBudgetIsFull(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	cfg := validTestConfig(t)
	cfg.MaxBacklogBytes = 1
	exp := newTestAgenticExporter(t, cfg, zap.NewNop(), provider)
	if !exp.writer.tryEnqueue([]byte("\n")) {
		t.Fatal("failed to occupy the shared writer budget")
	}
	if err := exp.consumeTraces(context.Background(), eligibleTestTraces("secret")); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	waitUntil(t, exp.encodingIdle)
	if got := len(queuedRecords(exp.writer)); got != 1 {
		t.Fatalf("queued documents = %d, want only the existing reservation", got)
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections", map[string]string{"reason": string(rejectQueueFull)}); got != 1 {
		t.Fatalf("queue-full rejections = %d, want one span document", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != 0 {
		t.Fatalf("admitted documents = %d, want zero", got)
	}
}

func TestConsumeTracesEnforcesSpanEncodingQueueCapacity(t *testing.T) {
	const admittedSpanCount = 64
	const sourceSpanCount = admittedSpanCount + 1

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	cfg := validTestConfig(t)
	exp, err := newAgenticExporter(exporter.Settings{TelemetrySettings: component.TelemetrySettings{
		Logger: zap.NewNop(), MeterProvider: provider,
	}}, cfg)
	if err != nil {
		t.Fatalf("newAgenticExporter() error = %v", err)
	}
	t.Cleanup(func() { _ = exp.shutdown(context.Background()) })

	template := eligibleTestTraces("template")
	templateSpan := template.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	input := eligibleTestTraces("template")
	spans := input.ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	for range admittedSpanCount {
		templateSpan.CopyTo(spans.AppendEmpty())
	}
	for i := range sourceSpanCount {
		span := spans.At(i)
		marker := fmt.Sprintf("source-%02d", i)
		span.SetSpanID(pcommon.SpanID{byte(i + 1)})
		span.SetName(marker)
		span.Attributes().PutStr("source.marker", marker)
		span.Events().At(1).SetName(marker)
	}

	exp.encodingMu.Lock()
	exp.encodingAccepting = true
	exp.encodingMu.Unlock()
	if err := exp.consumeTraces(context.Background(), input); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	exp.encodingMu.Lock()
	queuedJobs := len(exp.encodingQueue)
	exp.encodingMu.Unlock()
	if queuedJobs != admittedSpanCount {
		t.Fatalf("queued encoding jobs = %d, want %d before worker start", queuedJobs, admittedSpanCount)
	}

	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}

	documents := readNativeReady(t, cfg.Directory)
	if len(documents) != admittedSpanCount {
		t.Fatalf("native span documents = %d, want %d admitted source spans", len(documents), admittedSpanCount)
	}
	seen := make(map[string]bool, admittedSpanCount)
	for _, document := range documents {
		span := document.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		marker := span.Name()
		markerAttr, ok := span.Attributes().Get("source.marker")
		if document.SpanCount() != 1 || span.Events().Len() != 2 ||
			span.Events().At(1).Name() != marker ||
			!ok || markerAttr.Type() != pcommon.ValueTypeStr || markerAttr.Str() != marker {
			t.Fatalf("native document lost marker or nested event evidence for %q", marker)
		}
		if seen[marker] {
			t.Fatalf("source marker %q was admitted more than once", marker)
		}
		seen[marker] = true
	}
	for i := range admittedSpanCount {
		marker := fmt.Sprintf("source-%02d", i)
		if !seen[marker] {
			t.Fatalf("admitted source marker %q was not published", marker)
		}
	}
	if seen[fmt.Sprintf("source-%02d", admittedSpanCount)] {
		t.Fatal("65th source span passed the bounded encoding queue")
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != admittedSpanCount {
		t.Fatalf("admitted span documents = %d, want %d", got, admittedSpanCount)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 1 {
		t.Fatalf("rejected span documents = %d, want one queue-full span with nested events", got)
	}
	if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections", map[string]string{"reason": string(rejectQueueFull)}); got != 1 {
		t.Fatalf("queue-full span rejections = %d, want one despite nested events", got)
	}
}

func TestEncodingCancellationAccountsForCanceledAndDiscardedDocuments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	cfg := validTestConfig(t)
	exp, err := newAgenticExporter(exporter.Settings{TelemetrySettings: component.TelemetrySettings{
		Logger: zap.NewNop(), MeterProvider: provider,
	}}, cfg)
	if err != nil {
		t.Fatalf("newAgenticExporter() error = %v", err)
	}
	t.Cleanup(func() { _ = exp.shutdown(context.Background()) })
	if err := exp.writer.start(context.Background()); err != nil {
		t.Fatalf("writer.start() error = %v", err)
	}

	exp.encodingMu.Lock()
	exp.encodingAccepting = true
	exp.encodingMu.Unlock()
	encodingInput := eligibleTestTraces("cancel-during-encoding")
	discardInput := eligibleTestTraces("cancel-in-queue")
	discardInput.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetSpanID(pcommon.SpanID{3})
	if err := exp.consumeTraces(context.Background(), encodingInput); err != nil {
		t.Fatalf("consumeTraces(encoding job) error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), discardInput); err != nil {
		t.Fatalf("consumeTraces(queued job) error = %v", err)
	}

	exp.encodingMu.Lock()
	queuedJobs := len(exp.encodingQueue)
	if queuedJobs != 2 {
		exp.encodingMu.Unlock()
		t.Fatalf("accepted encoding jobs = %d, want 2", queuedJobs)
	}
	encodingJob := <-exp.encodingQueue
	remainingJobs := len(exp.encodingQueue)
	exp.encodingMu.Unlock()
	if encodingJob.traces.SpanCount() != 1 ||
		encodingJob.traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Events().Len() != 2 {
		t.Fatal("accepted encoding job did not retain one span with its nested events")
	}
	if exp.encodingCancellationRequested() {
		t.Fatal("accepted job was already canceled before its processing check")
	}

	exp.encodingMu.Lock()
	exp.encodingAccepting = false
	exp.encodingCanceled = true
	close(exp.encodingCancel)
	exp.encodingMu.Unlock()
	exp.encodeAndAdmit(encodingJob)
	exp.discardEncodingQueue()
	if remainingJobs != 1 || len(exp.encodingQueue) != 0 {
		t.Fatalf("queued jobs after cancellation: before discard=%d, remaining=%d", remainingJobs, len(exp.encodingQueue))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
	if documents := readNativeReady(t, cfg.Directory); len(documents) != 0 {
		t.Fatalf("canceled encoder admitted %d late native documents", len(documents))
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_candidates"); got != 0 {
		t.Fatalf("admitted span documents after cancellation = %d, want 0", got)
	}
	if got := int64HistogramSum(collected, "otelcol_agentic_exporter_record_size"); got != 0 {
		t.Fatalf("admitted document bytes after cancellation = %d, want 0", got)
	}
	if got := int64MetricSum(collected, "otelcol_agentic_exporter_rejections"); got != 2 {
		t.Fatalf("shutdown-deadline rejections = %d, want one for each canceled span", got)
	}
	if got := metricCountWithLabels(collected, "otelcol_agentic_exporter_rejections", map[string]string{"reason": string(rejectShutdown)}); got != 2 {
		t.Fatalf("shutdown-deadline rejections = %d, want one per encoded/discarded span, not per event", got)
	}
}

func TestExporterLifecycleContainsFilesystemLossAndLogsNoContent(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("unique-filesystem-secret"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg := &Config{
		Directory:       filepath.Join(blockingFile, "traces"),
		MaxBacklogBytes: 1 << 20,
	}
	exp := newTestAgenticExporter(t, cfg, logger, noop.NewMeterProvider())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), eligibleTestTraces("unique-pdata-secret")); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	waitUntil(t, exp.encodingIdle)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}

	allowedFields := map[string]bool{
		"from_state":          true,
		"state":               true,
		"operation":           true,
		"reason":              true,
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

func TestExporterShutdownIsIdempotent(t *testing.T) {
	exp := newTestAgenticExporter(t, validTestConfig(t), zap.NewNop(), noop.NewMeterProvider())
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	firstCtx, firstCancel := context.WithTimeout(context.Background(), time.Second)
	defer firstCancel()
	if err := exp.shutdown(firstCtx); err != nil {
		t.Fatalf("first shutdown() error = %v", err)
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	if err := exp.shutdown(secondCtx); err != nil {
		t.Fatalf("second shutdown() error = %v", err)
	}
}

func TestDisabledExporterLogsOnceAndSucceeds(t *testing.T) {
	core, observed := observer.New(zap.ErrorLevel)
	logger := zap.New(core)
	cfg := &Config{Directory: "relative", MaxBacklogBytes: 1024}
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

func publishTestTraces(t *testing.T, cfg *Config, exp *agenticExporter, traces ptrace.Traces) []ptrace.Traces {
	t.Helper()
	if err := exp.start(context.Background(), nil); err != nil {
		t.Fatalf("start() error = %v", err)
	}
	if err := exp.consumeTraces(context.Background(), traces); err != nil {
		t.Fatalf("consumeTraces() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exp.shutdown(ctx); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
	return readNativeReady(t, cfg.Directory)
}

func readNativeReady(t *testing.T, directory string) []ptrace.Traces {
	t.Helper()
	var result []ptrace.Traces
	for _, name := range readyFiles(t, directory) {
		if !canonicalReadyName.MatchString(name) {
			t.Fatalf("unexpected ready filename %q", name)
		}
		file, err := os.Open(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadBytes('\n')
			if err == io.EOF {
				if len(line) != 0 {
					_ = file.Close()
					t.Fatal("incomplete final JSONL line")
				}
				break
			}
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if bytes.ContainsAny(line[:len(line)-1], "\n\r") {
				_ = file.Close()
				t.Fatal("noncompact record")
			}
			decoder := ptrace.JSONUnmarshaler{}
			traces, err := decoder.UnmarshalTraces(line)
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if traces.SpanCount() != 1 {
				_ = file.Close()
				t.Fatal("line is not one native span document")
			}
			result = append(result, traces)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return result
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
	if exp.enabled {
		exp.startEncoding()
	}
	t.Cleanup(func() { _ = exp.shutdown(context.Background()) })
	return exp
}

func validTestConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		Directory:       filepath.Join(t.TempDir(), "traces"),
		MaxBacklogBytes: 1 << 20,
	}
}

func eligibleTestTraces(secret string) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	resourceSpan.Resource().Attributes().PutStr("service.name", "lightspeed-agentic-sandbox")
	scopeSpan := resourceSpan.ScopeSpans().AppendEmpty()
	span := scopeSpan.Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1})
	span.SetSpanID(pcommon.SpanID{2})
	span.SetStartTimestamp(1)
	span.SetEndTimestamp(2)
	span.SetName(secret)
	span.Attributes().PutStr("agenticrun.uid", "uid-secret")
	span.Attributes().PutStr("agenticrun.phase", "execution")
	span.Attributes().PutStr("gen_ai.operation.name", "chat")
	span.Attributes().PutStr("gen_ai.input.messages", `[{"role":"user","parts":[{"type":"text","content":"request"}]}]`)

	first := span.Events().AppendEmpty()
	first.SetName("agenticrun.execution.started")
	first.SetTimestamp(3)
	first.Attributes().PutStr("input.kind", secret)
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

func metricCountWithLabels(collected metricdata.ResourceMetrics, name string, labels map[string]string) int64 {
	var count int64
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name != name {
				continue
			}
			sum, ok := measured.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				matches := true
				for key, want := range labels {
					got, present := point.Attributes.Value(attribute.Key(key))
					if !present || got.AsString() != want {
						matches = false
						break
					}
				}
				if matches {
					count += point.Value
				}
			}
		}
	}
	return count
}
