package agenticexporter

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type rejectionReason string

const (
	rejectNone                 rejectionReason = ""
	rejectUnsupportedService   rejectionReason = "unsupported_service"
	rejectMissingUID           rejectionReason = "missing_uid"
	rejectInvalidPhase         rejectionReason = "missing_or_invalid_phase"
	rejectInvalidTraceIdentity rejectionReason = "invalid_trace_identity"
	rejectEncoding             rejectionReason = "encoding_failed"
	rejectRecordTooLarge       rejectionReason = "record_too_large"
	rejectQueueFull            rejectionReason = "queue_full"
	rejectShutdown             rejectionReason = "shutdown_deadline"
)

type spanContext struct {
	serviceName string
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

	return spanContext{serviceName: serviceName}, rejectNone
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
