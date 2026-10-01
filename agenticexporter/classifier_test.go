package agenticexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type testAttribute struct {
	missing   bool
	nonString bool
	value     string
}

func TestClassifySpan(t *testing.T) {
	const operator = "lightspeed-agentic-operator"
	const sandbox = "lightspeed-agentic-sandbox"
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
			service:     testAttribute{value: operator},
			uid:         validUID,
			phase:       validPhase,
			wantContext: spanContext{serviceName: operator},
		},
		{
			name:        "sandbox service",
			service:     testAttribute{value: sandbox},
			uid:         validUID,
			phase:       validPhase,
			wantContext: spanContext{serviceName: sandbox},
		},
		{name: "service absent", service: testAttribute{missing: true}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service non-string", service: testAttribute{nonString: true}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service empty", service: testAttribute{}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service wrong case", service: testAttribute{value: "Lightspeed-Agentic-Operator"}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "service unknown", service: testAttribute{value: "lightspeed-service"}, uid: validUID, phase: validPhase, wantReason: rejectUnsupportedService},
		{name: "span service is not a fallback", service: testAttribute{missing: true}, uid: validUID, phase: validPhase, spanServiceAttr: true, wantReason: rejectUnsupportedService},
		{name: "UID absent", service: testAttribute{value: operator}, uid: testAttribute{missing: true}, phase: validPhase, wantReason: rejectMissingUID},
		{name: "UID non-string", service: testAttribute{value: operator}, uid: testAttribute{nonString: true}, phase: validPhase, wantReason: rejectMissingUID},
		{name: "UID empty", service: testAttribute{value: operator}, uid: testAttribute{}, phase: validPhase, wantReason: rejectMissingUID},
		{
			name:        "UID is literal and not trimmed",
			service:     testAttribute{value: operator},
			uid:         testAttribute{value: " "},
			phase:       validPhase,
			wantContext: spanContext{serviceName: operator},
		},
		{
			name:                     "resource correlation is not a fallback",
			service:                  testAttribute{value: operator},
			uid:                      testAttribute{missing: true},
			phase:                    testAttribute{missing: true},
			resourceCorrelationAttrs: true,
			wantReason:               rejectMissingUID,
		},
		{
			name:                     "resource phase is not a fallback",
			service:                  testAttribute{value: operator},
			uid:                      validUID,
			phase:                    testAttribute{missing: true},
			resourceCorrelationAttrs: true,
			wantReason:               rejectInvalidPhase,
		},
		{name: "phase absent", service: testAttribute{value: operator}, uid: validUID, phase: testAttribute{missing: true}, wantReason: rejectInvalidPhase},
		{name: "phase non-string", service: testAttribute{value: operator}, uid: validUID, phase: testAttribute{nonString: true}, wantReason: rejectInvalidPhase},
		{name: "phase empty", service: testAttribute{value: operator}, uid: validUID, phase: testAttribute{}, wantReason: rejectInvalidPhase},
		{name: "phase unknown", service: testAttribute{value: operator}, uid: validUID, phase: testAttribute{value: "planning"}, wantReason: rejectInvalidPhase},
		{name: "phase wrong case", service: testAttribute{value: operator}, uid: validUID, phase: testAttribute{value: "Analysis"}, wantReason: rejectInvalidPhase},
		{name: "precedence service before UID and phase", service: testAttribute{value: "other"}, uid: testAttribute{missing: true}, phase: testAttribute{missing: true}, wantReason: rejectUnsupportedService},
		{name: "precedence UID before phase", service: testAttribute{value: operator}, uid: testAttribute{missing: true}, phase: testAttribute{missing: true}, wantReason: rejectMissingUID},
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
			service:     testAttribute{value: operator},
			uid:         validUID,
			phase:       testAttribute{value: phase},
			wantContext: spanContext{serviceName: operator},
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
				span.Attributes().PutStr("service.name", operator)
			}

			gotContext, gotReason := classifySpan(resourceAttrs, span)
			if gotContext != tt.wantContext || gotReason != tt.wantReason {
				t.Fatalf("classifySpan() = (%+v, %q), want (%+v, %q)", gotContext, gotReason, tt.wantContext, tt.wantReason)
			}
		})
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
