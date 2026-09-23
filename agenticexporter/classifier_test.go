package agenticexporter

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type testAttribute struct {
	missing   bool
	nonString bool
	value     string
}

func TestClassifyEnumValues(t *testing.T) {
	candidateValues := map[candidateType]string{
		candidateAction:     "action",
		candidateTranscript: "transcript",
	}
	for value, want := range candidateValues {
		if string(value) != want {
			t.Errorf("candidate type %q = %q, want %q", want, value, want)
		}
	}

	recordValues := map[recordKind]string{
		recordSpan:      "span",
		recordSpanEvent: "span_event",
	}
	for value, want := range recordValues {
		if string(value) != want {
			t.Errorf("record kind %q = %q, want %q", want, value, want)
		}
	}

	rejectionValues := map[rejectionReason]string{
		rejectNone:               "",
		rejectUnsupportedService: "unsupported_service",
		rejectMissingUID:         "missing_uid",
		rejectInvalidPhase:       "missing_or_invalid_phase",
		rejectInvalidEnvelope:    "invalid_envelope",
		rejectQueueFull:          "queue_full",
		rejectShutdown:           "shutdown_deadline",
	}
	for value, want := range rejectionValues {
		if string(value) != want {
			t.Errorf("rejection reason %q = %q, want %q", want, value, want)
		}
	}
}

func TestClassifySpan(t *testing.T) {
	validService := testAttribute{value: "lightspeed-agentic-operator"}
	validUID := testAttribute{value: "550e8400-e29b-41d4-a716-446655440000"}
	validPhase := testAttribute{value: "analysis"}

	tests := []struct {
		name                     string
		service                  testAttribute
		uid                      testAttribute
		phase                    testAttribute
		resourceCorrelationAttrs bool
		spanServiceAttr          bool
		wantContext              spanContext
		wantReason               rejectionReason
	}{
		{
			name:        "operator service",
			service:     validService,
			uid:         validUID,
			phase:       validPhase,
			wantContext: spanContext{serviceName: validService.value, agenticRunUID: validUID.value, phase: validPhase.value},
			wantReason:  rejectNone,
		},
		{
			name:        "sandbox service",
			service:     testAttribute{value: "lightspeed-agentic-sandbox"},
			uid:         validUID,
			phase:       validPhase,
			wantContext: spanContext{serviceName: "lightspeed-agentic-sandbox", agenticRunUID: validUID.value, phase: validPhase.value},
			wantReason:  rejectNone,
		},
		{name: "service absent", service: testAttribute{missing: true}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service non-string", service: testAttribute{nonString: true}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service empty", service: testAttribute{}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service wrong case", service: testAttribute{value: "Lightspeed-Agentic-Operator"}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service unknown", service: testAttribute{value: "lightspeed-service"}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "span service is not a fallback", service: testAttribute{missing: true}, uid: validUID, phase: validPhase, spanServiceAttr: true, wantReason: rejectUnsupportedService},
		{name: "UID absent", service: validService, uid: testAttribute{missing: true}, phase: validPhase, wantReason: rejectMissingUID},
		{name: "UID non-string", service: validService, uid: testAttribute{nonString: true}, phase: validPhase, wantReason: rejectMissingUID},
		{name: "UID empty", service: validService, uid: testAttribute{}, phase: validPhase, wantReason: rejectMissingUID},
		{
			name:        "UID is literal and not trimmed",
			service:     validService,
			uid:         testAttribute{value: " "},
			phase:       validPhase,
			wantContext: spanContext{serviceName: validService.value, agenticRunUID: " ", phase: validPhase.value},
			wantReason:  rejectNone,
		},
		{name: "resource correlation is not a fallback", service: validService, uid: testAttribute{missing: true}, phase: testAttribute{missing: true}, resourceCorrelationAttrs: true, wantReason: rejectMissingUID},
		{name: "resource phase is not a fallback", service: validService, uid: validUID, phase: testAttribute{missing: true}, resourceCorrelationAttrs: true, wantReason: rejectInvalidPhase},
		{name: "phase absent", service: validService, uid: validUID, phase: testAttribute{missing: true}, wantReason: rejectInvalidPhase},
		{name: "phase non-string", service: validService, uid: validUID, phase: testAttribute{nonString: true}, wantReason: rejectInvalidPhase},
		{name: "phase empty", service: validService, uid: validUID, phase: testAttribute{}, wantReason: rejectInvalidPhase},
		{name: "phase unknown", service: validService, uid: validUID, phase: testAttribute{value: "planning"}, wantReason: rejectInvalidPhase},
		{name: "phase wrong case", service: validService, uid: validUID, phase: testAttribute{value: "Analysis"}, wantReason: rejectInvalidPhase},
		{name: "precedence service before UID and phase", service: testAttribute{value: "other"}, uid: testAttribute{missing: true}, phase: testAttribute{missing: true}, wantReason: rejectUnsupportedService},
		{name: "precedence UID before phase", service: validService, uid: testAttribute{missing: true}, phase: testAttribute{missing: true}, wantReason: rejectMissingUID},
	}

	for _, phase := range []string{"analysis", "approval", "execution", "verification", "escalation", "terminal"} {
		tests = append(tests, struct {
			name                     string
			service                  testAttribute
			uid                      testAttribute
			phase                    testAttribute
			resourceCorrelationAttrs bool
			spanServiceAttr          bool
			wantContext              spanContext
			wantReason               rejectionReason
		}{
			name:        "valid phase " + phase,
			service:     validService,
			uid:         validUID,
			phase:       testAttribute{value: phase},
			wantContext: spanContext{serviceName: validService.value, agenticRunUID: validUID.value, phase: phase},
			wantReason:  rejectNone,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceAttrs := pcommon.NewMap()
			putTestAttribute(resourceAttrs, "service.name", tt.service)
			if tt.resourceCorrelationAttrs {
				resourceAttrs.PutStr("agenticrun.uid", "resource-uid")
				resourceAttrs.PutStr("agenticrun.phase", "analysis")
			}

			span := ptrace.NewSpan()
			putTestAttribute(span.Attributes(), "agenticrun.uid", tt.uid)
			putTestAttribute(span.Attributes(), "agenticrun.phase", tt.phase)
			if tt.spanServiceAttr {
				span.Attributes().PutStr("service.name", "lightspeed-agentic-operator")
			}

			gotContext, gotReason := classifySpan(resourceAttrs, span)
			if gotContext != tt.wantContext || gotReason != tt.wantReason {
				t.Fatalf("classifySpan() = (%+v, %q), want (%+v, %q)", gotContext, gotReason, tt.wantContext, tt.wantReason)
			}
		})
	}
}

func TestClassifyEvent(t *testing.T) {
	type eventTest struct {
		name      string
		eventName string
		attrs     []string
		want      candidateType
	}

	inputAttrs := []string{"gen_ai.input.system_prompt", "gen_ai.input.prompt", "gen_ai.input.context", "gen_ai.input.output_schema"}
	toolCallAttrs := []string{"gen_ai.tool.name", "gen_ai.tool.call.id", "tool.input"}
	toolResultAttrs := []string{"gen_ai.tool.name", "gen_ai.tool.call.id", "tool.status", "tool.output"}
	outputAttrs := []string{"gen_ai.output.value", "gen_ai.request.model", "gen_ai.response.model", "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens", "gen_ai.usage.reasoning_tokens"}

	tests := []eventTest{
		{name: "input complete", eventName: "gen_ai.input", attrs: inputAttrs, want: candidateTranscript},
		{name: "choice completion only", eventName: "gen_ai.choice", attrs: []string{"gen_ai.completion"}, want: candidateTranscript},
		{name: "choice reasoning only", eventName: "gen_ai.choice", attrs: []string{"gen_ai.reasoning_content"}, want: candidateTranscript},
		{name: "choice completion and reasoning", eventName: "gen_ai.choice", attrs: []string{"gen_ai.completion", "gen_ai.reasoning_content"}, want: candidateTranscript},
		{name: "choice neither", eventName: "gen_ai.choice", want: candidateAction},
		{name: "tool call complete", eventName: "gen_ai.tool.call", attrs: toolCallAttrs, want: candidateTranscript},
		{name: "tool result complete", eventName: "gen_ai.tool.result", attrs: toolResultAttrs, want: candidateTranscript},
		{name: "skill loaded", eventName: "gen_ai.skill.loaded", attrs: []string{"gen_ai.skill.name"}, want: candidateTranscript},
		{name: "skill used", eventName: "gen_ai.skill.used", attrs: []string{"gen_ai.skill.name"}, want: candidateTranscript},
		{name: "skill loaded with optional attributes", eventName: "gen_ai.skill.loaded", attrs: []string{"gen_ai.skill.name", "gen_ai.skill.content", "gen_ai.skill.metadata"}, want: candidateTranscript},
		{name: "skill used with optional attributes", eventName: "gen_ai.skill.used", attrs: []string{"gen_ai.skill.name", "gen_ai.skill.content", "gen_ai.skill.metadata"}, want: candidateTranscript},
		{name: "skill loaded missing name", eventName: "gen_ai.skill.loaded", attrs: []string{"gen_ai.skill.content", "gen_ai.skill.metadata"}, want: candidateAction},
		{name: "skill used missing name", eventName: "gen_ai.skill.used", attrs: []string{"gen_ai.skill.content", "gen_ai.skill.metadata"}, want: candidateAction},
		{name: "output complete", eventName: "gen_ai.output", attrs: outputAttrs, want: candidateTranscript},
		{name: "wrong case", eventName: "Gen_AI.Input", attrs: inputAttrs, want: candidateAction},
		{name: "unknown name with transcript-shaped attributes", eventName: "gen_ai.unknown", attrs: outputAttrs, want: candidateAction},
		{name: "unnamed event", attrs: inputAttrs, want: candidateAction},
	}

	for _, required := range []struct {
		name      string
		eventName string
		attrs     []string
	}{
		{name: "input", eventName: "gen_ai.input", attrs: inputAttrs},
		{name: "tool call", eventName: "gen_ai.tool.call", attrs: toolCallAttrs},
		{name: "tool result", eventName: "gen_ai.tool.result", attrs: toolResultAttrs},
		{name: "output", eventName: "gen_ai.output", attrs: outputAttrs},
	} {
		for missing := range required.attrs {
			tests = append(tests, eventTest{
				name:      fmt.Sprintf("%s missing %s", required.name, required.attrs[missing]),
				eventName: required.eventName,
				attrs:     withoutIndex(required.attrs, missing),
				want:      candidateAction,
			})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := eventWithAttributes(tt.eventName, tt.attrs...)
			if got := classifyEvent(event); got != tt.want {
				t.Fatalf("classifyEvent() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyEventUsesPresenceNotValues(t *testing.T) {
	event := ptrace.NewSpanEvent()
	event.SetName("gen_ai.input")
	event.Attributes().PutStr("gen_ai.input.system_prompt", "")
	event.Attributes().PutInt("gen_ai.input.prompt", 0)
	event.Attributes().PutBool("gen_ai.input.context", false)
	event.Attributes().PutEmptyMap("gen_ai.input.output_schema")

	if got := classifyEvent(event); got != candidateTranscript {
		t.Fatalf("classifyEvent() = %q, want %q when every key is present with empty or non-string values", got, candidateTranscript)
	}
}

func TestTraversalOrderingAndIndexes(t *testing.T) {
	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("service.name", "lightspeed-agentic-sandbox")
	span := ptrace.NewSpan()
	span.Attributes().PutStr("agenticrun.uid", "run-1")
	span.Attributes().PutStr("agenticrun.phase", "execution")

	first := span.Events().AppendEmpty()
	eventWithAttributesInto(first, "gen_ai.choice", "gen_ai.completion")
	second := span.Events().AppendEmpty()
	eventWithAttributesInto(second, "agenticrun.execution.completed", "result.uid")
	third := span.Events().AppendEmpty()
	eventWithAttributesInto(third, "gen_ai.skill.used", "gen_ai.skill.name")

	if _, reason := classifySpan(resourceAttrs, span); reason != rejectNone {
		t.Fatalf("fixture span rejected: %q", reason)
	}

	type classifiedAtom struct {
		kind       recordKind
		eventIndex int
		candidate  candidateType
	}
	got := []classifiedAtom{{kind: recordSpan, eventIndex: -1, candidate: candidateAction}}
	for index := range span.Events().Len() {
		got = append(got, classifiedAtom{
			kind:       recordSpanEvent,
			eventIndex: index,
			candidate:  classifyEvent(span.Events().At(index)),
		})
	}
	want := []classifiedAtom{
		{kind: recordSpan, eventIndex: -1, candidate: candidateAction},
		{kind: recordSpanEvent, eventIndex: 0, candidate: candidateTranscript},
		{kind: recordSpanEvent, eventIndex: 1, candidate: candidateAction},
		{kind: recordSpanEvent, eventIndex: 2, candidate: candidateTranscript},
	}

	if len(got) != len(want) {
		t.Fatalf("traversal produced %d atoms, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("atom %d = %+v, want %+v", index, got[index], want[index])
		}
	}
}

func putTestAttribute(attrs pcommon.Map, key string, value testAttribute) {
	if value.missing {
		return
	}
	if value.nonString {
		attrs.PutInt(key, 1)
		return
	}
	attrs.PutStr(key, value.value)
}

func eventWithAttributes(name string, keys ...string) ptrace.SpanEvent {
	event := ptrace.NewSpanEvent()
	eventWithAttributesInto(event, name, keys...)
	return event
}

func eventWithAttributesInto(event ptrace.SpanEvent, name string, keys ...string) {
	event.SetName(name)
	for index, key := range keys {
		event.Attributes().PutInt(key, int64(index))
	}
}

func withoutIndex(values []string, removed int) []string {
	result := make([]string, 0, len(values)-1)
	result = append(result, values[:removed]...)
	result = append(result, values[removed+1:]...)
	return result
}
