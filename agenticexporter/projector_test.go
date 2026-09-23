package agenticexporter

import (
	"bytes"
	"math"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestProjectActionSpanExactFixture(t *testing.T) {
	input := baseProjectionInput(t)

	assertProjectFixture(t, input, "testdata/action-span.jsonl")
}

func TestProjectTranscriptEventExactFixture(t *testing.T) {
	input := baseProjectionInput(t)
	input.candidate = candidateTranscript
	input.kind = recordSpanEvent
	input.span.SetParentSpanID(pcommon.SpanID{0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48})
	event := ptrace.NewSpanEvent()
	event.SetTimestamp(150)
	event.SetName("gen_ai.input")
	event.SetDroppedAttributesCount(7)
	event.Attributes().PutStr("same", "event")
	index := 2
	input.event = &event
	input.eventIndex = &index

	assertProjectFixture(t, input, "testdata/transcript-event.jsonl")
}

func TestProjectAllAnyValuesExactFixture(t *testing.T) {
	input := baseProjectionInput(t)
	attributes := input.span.Attributes()
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

	assertProjectFixture(t, input, "testdata/all-any-values.jsonl")
}

func TestProjectEventIncludesEmptyEventNamespaces(t *testing.T) {
	input := baseProjectionInput(t)
	input.kind = recordSpanEvent
	event := ptrace.NewSpanEvent()
	event.SetName("empty-event")
	index := 0
	input.event = &event
	input.eventIndex = &index

	got, err := project(input)
	if err != nil {
		t.Fatalf("project() error = %v", err)
	}
	if !bytes.Contains(got, []byte(`"event":{"droppedAttributesCount":0}`)) {
		t.Fatalf("project() omitted zero-valued event metadata: %s", got)
	}
	if !bytes.Contains(got, []byte(`"event":{}}`)) {
		t.Fatalf("project() omitted empty event attributes: %s", got)
	}
}

func TestProjectRejectsInvalidEnvelope(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*projectionInput)
	}{
		{
			name: "candidate enum",
			mutate: func(input *projectionInput) {
				input.candidate = candidateType("secret-candidate")
			},
		},
		{
			name: "record enum",
			mutate: func(input *projectionInput) {
				input.kind = recordKind("secret-record")
			},
		},
		{
			name: "empty service",
			mutate: func(input *projectionInput) {
				input.context.serviceName = ""
			},
		},
		{
			name: "empty run uid",
			mutate: func(input *projectionInput) {
				input.context.agenticRunUID = ""
			},
		},
		{
			name: "empty phase",
			mutate: func(input *projectionInput) {
				input.context.phase = ""
			},
		},
		{
			name: "empty trace id",
			mutate: func(input *projectionInput) {
				input.span.SetTraceID(pcommon.TraceID{})
			},
		},
		{
			name: "span with event",
			mutate: func(input *projectionInput) {
				event := ptrace.NewSpanEvent()
				input.event = &event
			},
		},
		{
			name: "event without event",
			mutate: func(input *projectionInput) {
				input.kind = recordSpanEvent
				index := 0
				input.eventIndex = &index
			},
		},
		{
			name: "event without index",
			mutate: func(input *projectionInput) {
				input.kind = recordSpanEvent
				event := ptrace.NewSpanEvent()
				input.event = &event
			},
		},
		{
			name: "negative event index",
			mutate: func(input *projectionInput) {
				input.kind = recordSpanEvent
				event := ptrace.NewSpanEvent()
				index := -1
				input.event = &event
				input.eventIndex = &index
			},
		},
		{
			name: "failed pdata conversion",
			mutate: func(input *projectionInput) {
				input.resource = pcommon.Resource{}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := baseProjectionInput(t)
			tt.mutate(&input)

			got, err := project(input)
			if got != nil {
				t.Fatalf("project() bytes = %q, want nil", got)
			}
			if err == nil || err.Error() != string(rejectInvalidEnvelope) {
				t.Fatalf("project() error = %v, want %q", err, rejectInvalidEnvelope)
			}
			if strings.Contains(err.Error(), "secret") ||
				(input.context.agenticRunUID != "" && strings.Contains(err.Error(), input.context.agenticRunUID)) {
				t.Fatalf("project() error leaked source content: %q", err)
			}
		})
	}
}

func TestProjectReturnsIndependentBytes(t *testing.T) {
	input := baseProjectionInput(t)
	first, err := project(input)
	if err != nil {
		t.Fatalf("first project() error = %v", err)
	}
	first[0] = '!'

	second, err := project(input)
	if err != nil {
		t.Fatalf("second project() error = %v", err)
	}
	if second[0] != '{' {
		t.Fatalf("project() reused caller-mutable bytes: %q", second[:1])
	}
}

func assertProjectFixture(t *testing.T, input projectionInput, fixturePath string) {
	t.Helper()

	want, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	got, err := project(input)
	if err != nil {
		t.Fatalf("project() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("project() mismatch\ngot:  %s\nwant: %s", got, want)
	}
	if !bytes.HasSuffix(got, []byte{'\n'}) || bytes.HasSuffix(got, []byte{'\n', '\n'}) {
		t.Fatalf("project() must end in exactly one LF: %q", got)
	}
}

func baseProjectionInput(t *testing.T) projectionInput {
	t.Helper()

	resource := pcommon.NewResource()
	resource.SetDroppedAttributesCount(1)
	resource.Attributes().PutStr("service.name", "lightspeed-agentic-sandbox")
	resource.Attributes().PutStr("same", "resource")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("scope")
	scope.SetVersion("1.2.3")
	scope.SetDroppedAttributesCount(2)
	scope.Attributes().PutStr("same", "scope")

	span := ptrace.NewSpan()
	span.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID(pcommon.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	span.SetName("operation")
	span.SetStartTimestamp(100)
	span.SetEndTimestamp(200)
	span.SetKind(ptrace.SpanKindServer)
	span.SetFlags(1)
	span.SetDroppedAttributesCount(3)
	span.SetDroppedEventsCount(4)
	span.SetDroppedLinksCount(5)
	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("failed")
	span.TraceState().FromRaw("vendor=value")
	span.Attributes().PutStr("agenticrun.uid", "run-1")
	span.Attributes().PutStr("agenticrun.phase", "Running")
	span.Attributes().PutStr("same", "span")

	link := span.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30})
	link.SetSpanID(pcommon.SpanID{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38})
	link.SetFlags(2)
	link.SetDroppedAttributesCount(6)
	link.Attributes().PutStr("same", "link")
	link.TraceState().FromRaw("link=value")

	return projectionInput{
		candidate: candidateAction,
		kind:      recordSpan,
		context: spanContext{
			serviceName:   "lightspeed-agentic-sandbox",
			agenticRunUID: "run-1",
			phase:         "Running",
		},
		resource:          resource,
		resourceSchemaURL: "resource/schema",
		scope:             scope,
		scopeSchemaURL:    "scope/schema",
		span:              span,
	}
}
