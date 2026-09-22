package agenticexporter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type candidateType string

const (
	candidateAction     candidateType = "action"
	candidateTranscript candidateType = "transcript"
)

type recordKind string

const (
	recordSpan      recordKind = "span"
	recordSpanEvent recordKind = "span_event"
)

type rejectionReason string

const (
	rejectNone               rejectionReason = ""
	rejectUnsupportedService rejectionReason = "unsupported_service"
	rejectMissingUID         rejectionReason = "missing_uid"
	rejectInvalidPhase       rejectionReason = "missing_or_invalid_phase"
	rejectInvalidEnvelope    rejectionReason = "invalid_envelope"
	rejectQueueFull          rejectionReason = "queue_full"
)

type spanContext struct {
	serviceName   string
	agenticRunUID string
	phase         string
}

func classifySpan(resourceAttrs pcommon.Map, span ptrace.Span) (spanContext, rejectionReason) {
	serviceName, ok := stringAttribute(resourceAttrs, "service.name")
	if !ok || (serviceName != "lightspeed-agentic-operator" && serviceName != "lightspeed-agentic-sandbox") {
		return spanContext{}, rejectUnsupportedService
	}

	agenticRunUID, ok := stringAttribute(span.Attributes(), "agenticrun.uid")
	if !ok || agenticRunUID == "" {
		return spanContext{}, rejectMissingUID
	}

	phase, ok := stringAttribute(span.Attributes(), "agenticrun.phase")
	if !ok || !validPhase(phase) {
		return spanContext{}, rejectInvalidPhase
	}

	return spanContext{
		serviceName:   serviceName,
		agenticRunUID: agenticRunUID,
		phase:         phase,
	}, rejectNone
}

func classifyEvent(event ptrace.SpanEvent) candidateType {
	attrs := event.Attributes()
	transcript := false

	switch event.Name() {
	case "gen_ai.input":
		transcript = hasAll(attrs,
			"gen_ai.input.system_prompt",
			"gen_ai.input.prompt",
			"gen_ai.input.context",
			"gen_ai.input.output_schema",
		)
	case "gen_ai.choice":
		transcript = hasAny(attrs, "gen_ai.completion", "gen_ai.reasoning_content")
	case "gen_ai.tool.call":
		transcript = hasAll(attrs, "gen_ai.tool.name", "gen_ai.tool.call.id", "tool.input")
	case "gen_ai.tool.result":
		transcript = hasAll(attrs, "gen_ai.tool.name", "gen_ai.tool.call.id", "tool.status", "tool.output")
	case "gen_ai.skill.loaded", "gen_ai.skill.used":
		transcript = hasAll(attrs, "gen_ai.skill.name")
	case "gen_ai.output":
		transcript = hasAll(attrs,
			"gen_ai.output.value",
			"gen_ai.request.model",
			"gen_ai.response.model",
			"gen_ai.usage.input_tokens",
			"gen_ai.usage.output_tokens",
			"gen_ai.usage.reasoning_tokens",
		)
	}

	if transcript {
		return candidateTranscript
	}
	return candidateAction
}

func stringAttribute(attrs pcommon.Map, key string) (string, bool) {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	return value.Str(), true
}

func validPhase(phase string) bool {
	switch phase {
	case "analysis", "approval", "execution", "verification", "escalation", "terminal":
		return true
	default:
		return false
	}
}

func hasAll(attrs pcommon.Map, keys ...string) bool {
	for _, key := range keys {
		if _, ok := attrs.Get(key); !ok {
			return false
		}
	}
	return true
}

func hasAny(attrs pcommon.Map, keys ...string) bool {
	for _, key := range keys {
		if _, ok := attrs.Get(key); ok {
			return true
		}
	}
	return false
}
