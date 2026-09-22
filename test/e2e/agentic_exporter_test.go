//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
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
	fixtureAgenticRunUID = "550e8400-e29b-41d4-a716-446655440000"
	fixtureTraceIDHex    = "00112233445566778899aabbccddeeff"
	fixtureSpanIDHex     = "0123456789abcdef"
	fixtureSpanName      = "e2e-agentic-operation"
	transcriptEventName  = "gen_ai.input"
	fallbackEventName    = "agenticrun.execution.completed"
	privatePayloadPrefix = "E2E_PRIVATE_PAYLOAD:"
)

type agenticJSONValue struct {
	StringValue *string `json:"stringValue,omitempty"`
}

type agenticJSONRecord struct {
	SchemaVersion string `json:"schema_version"`
	CandidateType string `json:"candidate_type"`
	RecordKind    string `json:"record_kind"`
	AgenticRunUID string `json:"agenticrun_uid"`
	Phase         string `json:"phase"`
	TraceID       string `json:"trace_id"`
	SpanID        string `json:"span_id"`
	EventIndex    *int   `json:"event_index"`
	ServiceName   string `json:"service_name"`
	Name          string `json:"name"`
	Attributes    struct {
		Resource map[string]agenticJSONValue `json:"resource"`
		Span     map[string]agenticJSONValue `json:"span"`
		Event    map[string]agenticJSONValue `json:"event"`
	} `json:"attributes"`
}

type agenticTraceFixture struct {
	marker  string
	payload string
}

func TestAgenticExporterRuntime(t *testing.T) {
	testStarted := time.Now().Add(-time.Second)
	podBefore := collectorPodName(t)

	happy := newAgenticTraceFixture("happy")
	sendAgenticTrace(t, happy)

	actionRecords := waitForAgenticRecords(t, "actions", happy.marker, 2, 15*time.Second)
	transcriptRecords := waitForAgenticRecords(t, "transcripts", happy.marker, 1, 15*time.Second)
	assertHappyAgenticRecords(t, happy, actionRecords, transcriptRecords)
	waitForDebugTrace(t, testStarted, 10*time.Second)
	assertCollectorHealthy(t)

	setCollectorStreamDirectoryMode(t, "actions", "0550")
	actionsWritable := false
	defer func() {
		if !actionsWritable {
			setCollectorStreamDirectoryMode(t, "actions", "0770")
		}
	}()

	failureStarted := time.Now().Add(-time.Second)
	failure := newAgenticTraceFixture("actions-failure")
	sendAgenticTrace(t, failure)

	failureTranscripts := waitForAgenticRecords(t, "transcripts", failure.marker, 1, 15*time.Second)
	if got := failureTranscripts[0].CandidateType; got != "transcript" {
		t.Fatalf("failure-isolation candidate_type = %q, want transcript", got)
	}
	waitForDebugTrace(t, failureStarted, 10*time.Second)
	assertCollectorHealthy(t)
	assertMarkerAbsent(t, "actions", failure.marker)
	assertContentFreeCollectorLogs(t, failureStarted, failure)

	setCollectorStreamDirectoryMode(t, "actions", "0770")
	actionsWritable = true
	recoveredActions := waitForAgenticRecords(t, "actions", failure.marker, 2, 45*time.Second)
	if len(recoveredActions) != 2 {
		t.Fatalf("recovered action records = %d, want 2", len(recoveredActions))
	}
	if podAfter := collectorPodName(t); podAfter != podBefore {
		t.Fatalf("collector restarted during stream recovery: before=%s after=%s", podBefore, podAfter)
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

	now := uint64(time.Now().UnixNano())
	span := &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		Name:              fixtureSpanName,
		Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
		StartTimeUnixNano: now,
		EndTimeUnixNano:   now + uint64(time.Millisecond),
		Attributes: []*commonpb.KeyValue{
			stringAttribute("agenticrun.uid", fixtureAgenticRunUID),
			stringAttribute("agenticrun.phase", "execution"),
			stringAttribute("e2e.payload", fixture.payload),
		},
		Events: []*tracepb.Span_Event{
			{
				TimeUnixNano: now + 1,
				Name:         transcriptEventName,
				Attributes: []*commonpb.KeyValue{
					stringAttribute("gen_ai.input.system_prompt", "system-original"),
					stringAttribute("gen_ai.input.prompt", "prompt-original"),
					stringAttribute("gen_ai.input.context", "context-original"),
					stringAttribute("gen_ai.input.output_schema", "schema-original"),
				},
			},
			{
				TimeUnixNano: now + 2,
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
				Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
					stringAttribute("service.name", "lightspeed-agentic-sandbox"),
					stringAttribute("e2e.source_marker", fixture.marker),
				}},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{Name: "agentic-e2e", Version: "1.0"},
						Spans: []*tracepb.Span{span},
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

func waitForAgenticRecords(
	t *testing.T,
	stream, marker string,
	want int,
	timeout time.Duration,
) []agenticJSONRecord {
	t.Helper()
	lines := waitForJSONLOutput(t, stream, func(line string) bool {
		var record agenticJSONRecord
		return json.Unmarshal([]byte(line), &record) == nil && recordMarker(record) == marker
	}, want, timeout)

	records := make([]agenticJSONRecord, 0, len(lines))
	for _, line := range lines {
		var record agenticJSONRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode %s JSONL: %v", stream, err)
		}
		records = append(records, record)
	}
	return records
}

func recordMarker(record agenticJSONRecord) string {
	value := record.Attributes.Resource["e2e.source_marker"].StringValue
	if value == nil {
		return ""
	}
	return *value
}

func assertHappyAgenticRecords(
	t *testing.T,
	fixture agenticTraceFixture,
	actions, transcripts []agenticJSONRecord,
) {
	t.Helper()
	all := append(append([]agenticJSONRecord(nil), actions...), transcripts...)
	for _, record := range all {
		if record.SchemaVersion != "1.0" {
			t.Fatalf("schema_version = %q, want 1.0", record.SchemaVersion)
		}
		if record.AgenticRunUID != fixtureAgenticRunUID || record.Phase != "execution" ||
			record.TraceID != fixtureTraceIDHex || record.SpanID != fixtureSpanIDHex ||
			record.ServiceName != "lightspeed-agentic-sandbox" {
			t.Fatalf("record identity/projection mismatch for %s/%s", record.RecordKind, record.Name)
		}
		if recordMarker(record) != fixture.marker {
			t.Fatalf("source marker = %q, want %q", recordMarker(record), fixture.marker)
		}
		payload := record.Attributes.Span["e2e.payload"].StringValue
		if payload == nil || *payload != fixture.payload {
			t.Fatal("span payload was not preserved exactly")
		}
	}

	var sawSpan, sawFallback bool
	for _, record := range actions {
		if record.CandidateType != "action" {
			t.Fatalf("Actions candidate_type = %q", record.CandidateType)
		}
		switch record.RecordKind {
		case "span":
			sawSpan = record.Name == fixtureSpanName && record.EventIndex == nil
		case "span_event":
			result := record.Attributes.Event["result.uid"].StringValue
			sawFallback = record.Name == fallbackEventName && record.EventIndex != nil &&
				*record.EventIndex == 1 && result != nil && *result == "result-original"
		}
	}
	if !sawSpan || !sawFallback {
		t.Fatalf("Actions missing projected span/fallback event: span=%t fallback=%t", sawSpan, sawFallback)
	}

	transcript := transcripts[0]
	if transcript.CandidateType != "transcript" || transcript.RecordKind != "span_event" ||
		transcript.Name != transcriptEventName || transcript.EventIndex == nil ||
		*transcript.EventIndex != 0 {
		t.Fatal("Transcript envelope or original event index mismatch")
	}
	for key, want := range map[string]string{
		"gen_ai.input.system_prompt": "system-original",
		"gen_ai.input.prompt":        "prompt-original",
		"gen_ai.input.context":       "context-original",
		"gen_ai.input.output_schema": "schema-original",
	} {
		got := transcript.Attributes.Event[key].StringValue
		if got == nil || *got != want {
			t.Fatalf("Transcript attribute %q was not preserved", key)
		}
	}
}

func waitForDebugTrace(t *testing.T, since time.Time, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(collectorLogsSince(t, since), "Traces") {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("debug exporter did not report the trace fan-out")
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

func assertMarkerAbsent(t *testing.T, stream, marker string) {
	t.Helper()
	for _, name := range listCollectorJSONL(t, stream) {
		if strings.Contains(readCollectorJSONL(t, stream, name), marker) {
			t.Fatalf("%s stream published marker %q while its directory was read-only", stream, marker)
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
		fixtureSpanName,
		transcriptEventName,
		fallbackEventName,
		privatePayloadPrefix,
		"prompt-original",
		"result-original",
	} {
		if strings.Contains(logs, secret) {
			t.Fatalf("collector logs leaked source content or identity %q", secret)
		}
	}
}
