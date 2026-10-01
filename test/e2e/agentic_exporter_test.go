//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	fixtureAgenticRunUID           = "550e8400-e29b-41d4-a716-446655440000"
	fixtureTraceIDHex              = "00112233445566778899aabbccddeeff"
	fixtureSpanIDHex               = "0123456789abcdef"
	fixtureParentSpanIDHex         = "fedcba9876543210"
	fixtureSpanName                = "e2e-agentic-operation"
	fixtureResourceSchemaURL       = "https://example.com/ols/e2e/resource/v1"
	fixtureScopeSchemaURL          = "https://example.com/ols/e2e/scope/v1"
	fixtureSpanStartTimeUnixNano   = uint64(1_700_000_000_000_000_000)
	fixtureSpanEndTimeUnixNano     = uint64(1_700_000_000_000_000_010)
	fixtureFirstEventTimeUnixNano  = uint64(1_700_000_000_000_000_003)
	fixtureSecondEventTimeUnixNano = uint64(1_700_000_000_000_000_007)
	firstActionEventName           = "agenticrun.execution.started"
	fallbackEventName              = "agenticrun.execution.completed"
	privatePayloadPrefix           = "E2E_PRIVATE_PAYLOAD:"
)

type agenticJSONValue struct {
	StringValue *string `json:"stringValue,omitempty"`
	IntValue    *string `json:"intValue,omitempty"`
}

type agenticJSONAttribute struct {
	Key   string           `json:"key"`
	Value agenticJSONValue `json:"value"`
}

type agenticJSONResource struct {
	Attributes []agenticJSONAttribute `json:"attributes"`
}

type agenticJSONScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type agenticJSONEvent struct {
	TimeUnixNano string                 `json:"timeUnixNano"`
	Name         string                 `json:"name"`
	Attributes   []agenticJSONAttribute `json:"attributes"`
}

type agenticJSONSpan struct {
	TraceID           string                 `json:"traceId"`
	SpanID            string                 `json:"spanId"`
	ParentSpanID      string                 `json:"parentSpanId"`
	Name              string                 `json:"name"`
	Kind              int32                  `json:"kind"`
	StartTimeUnixNano string                 `json:"startTimeUnixNano"`
	EndTimeUnixNano   string                 `json:"endTimeUnixNano"`
	Attributes        []agenticJSONAttribute `json:"attributes"`
	Events            []agenticJSONEvent     `json:"events"`
}

type agenticJSONScopeSpans struct {
	SchemaURL string            `json:"schemaUrl"`
	Scope     agenticJSONScope  `json:"scope"`
	Spans     []agenticJSONSpan `json:"spans"`
}

type agenticJSONResourceSpans struct {
	SchemaURL  string                  `json:"schemaUrl"`
	Resource   agenticJSONResource     `json:"resource"`
	ScopeSpans []agenticJSONScopeSpans `json:"scopeSpans"`
}

type agenticJSONDocument struct {
	ResourceSpans []agenticJSONResourceSpans `json:"resourceSpans"`
}

type agenticTraceFixture struct {
	marker  string
	payload string
}

func TestAgenticExporterRuntime(t *testing.T) {
	podBefore := collectorPodName(t)

	happy := newAgenticTraceFixture("happy")
	happyLogWindowStart := time.Now().Add(-time.Minute)
	happyDebugEntriesBefore := debugTraceEntryCount(collectorLogsSince(t, happyLogWindowStart))
	sendAgenticTrace(t, happy)

	happyDocuments := waitForAgenticDocuments(t, happy.marker, 1, 15*time.Second)
	assertHappyAgenticDocument(t, happy, happyDocuments)
	waitForDebugTrace(t, happyLogWindowStart, happyDebugEntriesBefore, 10*time.Second)
	assertCollectorHealthy(t)

	setCollectorTraceDirectoryMode(t, "0550")
	traceDirectoryWritable := false
	defer func() {
		if !traceDirectoryWritable {
			setCollectorTraceDirectoryMode(t, "0770")
		}
	}()

	failure := newAgenticTraceFixture("failure")
	failureLogWindowStart := time.Now().Add(-time.Minute)
	failureDebugEntriesBefore := debugTraceEntryCount(collectorLogsSince(t, failureLogWindowStart))
	sendAgenticTrace(t, failure)

	waitForDebugTrace(t, failureLogWindowStart, failureDebugEntriesBefore, 10*time.Second)
	assertCollectorHealthy(t)
	assertAgenticMarkerAbsent(t, failure.marker)
	assertContentFreeCollectorLogs(t, failureLogWindowStart, failure)

	setCollectorTraceDirectoryMode(t, "0770")
	traceDirectoryWritable = true
	recoveredDocuments := waitForAgenticDocuments(t, failure.marker, 1, 45*time.Second)
	assertHappyAgenticDocument(t, failure, recoveredDocuments)
	if podAfter := collectorPodName(t); podAfter != podBefore {
		t.Fatalf("collector restarted during spool recovery: before=%s after=%s", podBefore, podAfter)
	}
	assertCollectorHealthy(t)
}

func newAgenticTraceFixture(label string) agenticTraceFixture {
	return agenticTraceFixture{
		marker:  fmt.Sprintf("e2e-agentic-%s-%d", label, time.Now().UnixNano()),
		payload: makeIncompressiblePayload((1 << 20) + 4096),
	}
}

func makeIncompressiblePayload(size int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	payload := make([]byte, size)
	copy(payload, privatePayloadPrefix)
	state := uint64(0x9e3779b97f4a7c15)
	for i := len(privatePayloadPrefix); i < len(payload); i++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = alphabet[state%uint64(len(alphabet))]
	}
	return string(payload)
}

func sendAgenticTrace(t *testing.T, fixture agenticTraceFixture) {
	t.Helper()
	traceID, err := hex.DecodeString(fixtureTraceIDHex)
	if err != nil {
		t.Fatalf("decode trace ID: %v", err)
	}
	spanID, err := hex.DecodeString(fixtureSpanIDHex)
	if err != nil {
		t.Fatalf("decode span ID: %v", err)
	}
	parentSpanID, err := hex.DecodeString(fixtureParentSpanIDHex)
	if err != nil {
		t.Fatalf("decode parent span ID: %v", err)
	}

	span := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              fixtureSpanName,
		Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
		StartTimeUnixNano: fixtureSpanStartTimeUnixNano,
		EndTimeUnixNano:   fixtureSpanEndTimeUnixNano,
		Attributes: []*commonpb.KeyValue{
			stringAttribute("agenticrun.uid", fixtureAgenticRunUID),
			stringAttribute("agenticrun.phase", "execution"),
			stringAttribute("e2e.payload", fixture.payload),
			intAttribute("gen_ai.usage.input_tokens", 42),
			stringAttribute("gen_ai.operation.name", "chat"),
			stringAttribute("gen_ai.input.messages", `[{"role":"user","parts":[{"type":"text","content":"request-original"}]}]`),
		},
		Events: []*tracepb.Span_Event{
			{
				TimeUnixNano: fixtureFirstEventTimeUnixNano,
				Name:         firstActionEventName,
				Attributes: []*commonpb.KeyValue{
					stringAttribute("input.kind", "original"),
				},
			},
			{
				TimeUnixNano: fixtureSecondEventTimeUnixNano,
				Name:         fallbackEventName,
				Attributes: []*commonpb.KeyValue{
					stringAttribute("result.uid", "result-original"),
				},
			},
		},
	}

	request := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				SchemaUrl: fixtureResourceSchemaURL,
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
					stringAttribute("service.name", "lightspeed-agentic-sandbox"),
					stringAttribute("e2e.source_marker", fixture.marker),
				}},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						SchemaUrl: fixtureScopeSchemaURL,
						Scope:     &commonpb.InstrumentationScope{Name: "agentic-e2e", Version: "1.0"},
						Spans:     []*tracepb.Span{span},
					},
				},
			},
		},
	}

	conn, err := grpc.NewClient(env.Endpoints.OTLPgRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial collector: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := collectortracepb.NewTraceServiceClient(conn).Export(ctx, request); err != nil {
		t.Fatalf("export agentic trace: %v", err)
	}
}

func stringAttribute(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}

func intAttribute(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: value},
		},
	}
}

func waitForAgenticDocuments(
	t *testing.T,
	marker string,
	want int,
	timeout time.Duration,
) []agenticJSONDocument {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var matching []agenticJSONDocument
		for _, name := range listCollectorJSONL(t) {
			for _, line := range readCollectorJSONLLines(t, name) {
				var document agenticJSONDocument
				if err := json.Unmarshal([]byte(line), &document); err != nil {
					t.Fatalf("decode native OTLP JSONL document in %q: %v", name, err)
				}
				if recordMarker(document) == marker {
					matching = append(matching, document)
				}
			}
		}
		if len(matching) >= want {
			return matching
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d matching native OTLP JSONL documents", want)
	return nil
}

func recordMarker(document agenticJSONDocument) string {
	for _, resourceSpans := range document.ResourceSpans {
		value, ok := agenticAttributeValue(resourceSpans.Resource.Attributes, "e2e.source_marker")
		if ok && value.StringValue != nil {
			return *value.StringValue
		}
	}
	return ""
}

func agenticAttributeValue(attributes []agenticJSONAttribute, key string) (agenticJSONValue, bool) {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return attribute.Value, true
		}
	}
	return agenticJSONValue{}, false
}

func assertStringAttribute(t *testing.T, attributes []agenticJSONAttribute, key, want string) {
	t.Helper()
	value, ok := agenticAttributeValue(attributes, key)
	if !ok || value.StringValue == nil || *value.StringValue != want {
		t.Fatalf("string attribute %q was not preserved", key)
	}
}

func assertIntAttribute(t *testing.T, attributes []agenticJSONAttribute, key, want string) {
	t.Helper()
	value, ok := agenticAttributeValue(attributes, key)
	if !ok || value.IntValue == nil || *value.IntValue != want {
		t.Fatalf("integer attribute %q was not preserved as an int", key)
	}
}

func assertHappyAgenticDocument(t *testing.T, fixture agenticTraceFixture, documents []agenticJSONDocument) {
	t.Helper()
	if len(documents) != 1 {
		t.Fatalf("native documents = %d, want one span document", len(documents))
	}
	document := documents[0]
	if len(document.ResourceSpans) != 1 {
		t.Fatalf("resourceSpans = %d, want one resource context", len(document.ResourceSpans))
	}
	resource := document.ResourceSpans[0]
	if len(resource.ScopeSpans) != 1 {
		t.Fatalf("scopeSpans = %d, want one instrumentation scope", len(resource.ScopeSpans))
	}
	scope := resource.ScopeSpans[0]
	if len(scope.Spans) != 1 {
		t.Fatalf("spans = %d, want one source span", len(scope.Spans))
	}
	span := scope.Spans[0]

	assertStringAttribute(t, resource.Resource.Attributes, "service.name", "lightspeed-agentic-sandbox")
	assertStringAttribute(t, resource.Resource.Attributes, "e2e.source_marker", fixture.marker)
	if resource.SchemaURL != fixtureResourceSchemaURL || scope.SchemaURL != fixtureScopeSchemaURL {
		t.Fatal("resource or scope schema URL was not preserved")
	}
	if span.TraceID != fixtureTraceIDHex || span.SpanID != fixtureSpanIDHex ||
		span.ParentSpanID != fixtureParentSpanIDHex || span.Name != fixtureSpanName ||
		span.Kind != int32(tracepb.Span_SPAN_KIND_INTERNAL) {
		t.Fatal("span identity, parent, name, or kind was not preserved")
	}
	if span.StartTimeUnixNano != strconv.FormatUint(fixtureSpanStartTimeUnixNano, 10) ||
		span.EndTimeUnixNano != strconv.FormatUint(fixtureSpanEndTimeUnixNano, 10) {
		t.Fatal("span timestamps were not preserved")
	}
	assertStringAttribute(t, span.Attributes, "agenticrun.uid", fixtureAgenticRunUID)
	assertStringAttribute(t, span.Attributes, "agenticrun.phase", "execution")
	assertStringAttribute(t, span.Attributes, "e2e.payload", fixture.payload)
	assertStringAttribute(t, span.Attributes, "gen_ai.operation.name", "chat")
	assertStringAttribute(t, span.Attributes, "gen_ai.input.messages",
		`[{"role":"user","parts":[{"type":"text","content":"request-original"}]}]`)
	assertIntAttribute(t, span.Attributes, "gen_ai.usage.input_tokens", "42")

	if len(span.Events) != 2 {
		t.Fatalf("span events = %d, want both source events", len(span.Events))
	}
	if span.Events[0].Name != firstActionEventName || span.Events[1].Name != fallbackEventName {
		t.Fatal("source event order or names were not preserved")
	}
	if span.Events[0].TimeUnixNano != strconv.FormatUint(fixtureFirstEventTimeUnixNano, 10) ||
		span.Events[1].TimeUnixNano != strconv.FormatUint(fixtureSecondEventTimeUnixNano, 10) {
		t.Fatal("source event timestamps were not preserved")
	}
	assertStringAttribute(t, span.Events[0].Attributes, "input.kind", "original")
	assertStringAttribute(t, span.Events[1].Attributes, "result.uid", "result-original")
}

func debugTraceEntryCount(logs string) int {
	count := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "Traces") {
			count++
		}
	}
	return count
}

func waitForDebugTrace(t *testing.T, since time.Time, entriesBefore int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if debugTraceEntryCount(collectorLogsSince(t, since)) > entriesBefore {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("debug exporter trace-entry count did not advance past %d", entriesBefore)
}

func assertCollectorHealthy(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + env.Endpoints.Health)
	if err != nil {
		t.Fatalf("collector health request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("collector health status = %d, want 200", resp.StatusCode)
	}
}

func assertAgenticMarkerAbsent(t *testing.T, marker string) {
	t.Helper()
	for _, name := range listCollectorJSONL(t) {
		for _, line := range readCollectorJSONLLines(t, name) {
			if strings.Contains(line, marker) {
				t.Fatalf("traces directory published marker %q while it was read-only", marker)
			}
		}
	}
}

func assertContentFreeCollectorLogs(t *testing.T, since time.Time, fixture agenticTraceFixture) {
	t.Helper()
	rawLogs := collectorLogsSince(t, since)
	var internalLogs []string
	for _, line := range strings.Split(rawLogs, "\n") {
		if strings.Contains(line, "agentic exporter") || strings.Contains(line, "agenticexporter@") {
			internalLogs = append(internalLogs, line)
		}
	}
	logs := strings.Join(internalLogs, "\n")
	for _, secret := range []string{
		fixture.marker,
		fixtureAgenticRunUID,
		fixtureTraceIDHex,
		fixtureSpanIDHex,
		fixtureParentSpanIDHex,
		fixtureSpanName,
		fallbackEventName,
		privatePayloadPrefix,
		"request-original",
		"result-original",
	} {
		if strings.Contains(logs, secret) {
			t.Fatalf("collector logs leaked source content or identity %q", secret)
		}
	}
}
