package agenticexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

const replayService = "lightspeed-agentic-sandbox"

func replaySpan(spans ptrace.SpanSlice, id byte, parent byte, name, operation string, start, end uint64) ptrace.Span {
	span := spans.AppendEmpty()
	span.SetTraceID(pcommon.TraceID{1, 2, 3})
	span.SetSpanID(pcommon.SpanID{id})
	if parent != 0 {
		span.SetParentSpanID(pcommon.SpanID{parent})
	}
	span.SetName(name)
	span.SetStartTimestamp(pcommon.Timestamp(start))
	span.SetEndTimestamp(pcommon.Timestamp(end))
	if operation == "chat" || operation == "generate_content" {
		span.SetKind(ptrace.SpanKindClient)
	} else {
		span.SetKind(ptrace.SpanKindInternal)
	}
	span.Attributes().PutStr("agenticrun.uid", "550e8400-e29b-41d4-a716-446655440000")
	span.Attributes().PutStr("agenticrun.phase", "execution")
	span.Attributes().PutStr("gen_ai.operation.name", operation)
	return span
}

func replayTraces() (ptrace.Traces, ptrace.SpanSlice) {
	traces := ptrace.NewTraces()
	resource := traces.ResourceSpans().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", replayService)
	resource.Resource().Attributes().PutStr("deployment.environment", "offline")
	scope := resource.ScopeSpans().AppendEmpty()
	scope.Scope().SetName("sandbox-genai")
	scope.Scope().SetVersion("1.2.3")
	return traces, scope.Spans()
}

func replaySpool(t *testing.T, traces ptrace.Traces) []ptrace.Traces {
	t.Helper()
	cfg := validTestConfig(t)
	exp := newTestAgenticExporter(t, cfg, zap.NewNop(), noop.NewMeterProvider())
	documents := publishTestTraces(t, cfg, exp, traces)
	if len(documents) == 0 {
		t.Fatal("native spool did not publish any span documents")
	}
	return documents
}

func replaySpansByID(t *testing.T, documents []ptrace.Traces) map[pcommon.SpanID]ptrace.Span {
	t.Helper()
	result := make(map[pcommon.SpanID]ptrace.Span, len(documents))
	for _, document := range documents {
		resourceSpans := document.ResourceSpans()
		if resourceSpans.Len() != 1 {
			t.Fatalf("resourceSpans = %d; want one contextual resource", resourceSpans.Len())
		}
		resource := resourceSpans.At(0)
		serviceName, ok := resource.Resource().Attributes().Get("service.name")
		if !ok || serviceName.Type() != pcommon.ValueTypeStr || serviceName.Str() != replayService {
			t.Fatal("native document lost its source service")
		}
		scopeSpans := resource.ScopeSpans()
		if scopeSpans.Len() != 1 || scopeSpans.At(0).Spans().Len() != 1 {
			t.Fatal("native document did not contain one scoped span")
		}
		span := scopeSpans.At(0).Spans().At(0)
		if _, exists := result[span.SpanID()]; exists {
			t.Fatalf("source span %s was published more than once", span.SpanID())
		}
		result[span.SpanID()] = span
	}
	return result
}

func replayStringAttribute(t *testing.T, span ptrace.Span, key, want string) {
	t.Helper()
	value, ok := span.Attributes().Get(key)
	if !ok {
		t.Fatalf("span %s attribute %q missing", span.SpanID(), key)
	}
	if value.Type() != pcommon.ValueTypeStr || value.Str() != want {
		t.Fatalf("span %s attribute %q = %v, want %q", span.SpanID(), key, value, want)
	}
}

func TestOfflineNativeSpoolPreservesSequentialRunEvidence(t *testing.T) {
	traces, spans := replayTraces()
	parent := replaySpan(spans, 1, 0, "invoke_agent lightspeed", "invoke_agent", 100, 900)
	parent.Attributes().PutStr("gen_ai.agent.name", "lightspeed")
	parentInput := `[{"role":"user","parts":[{"type":"text","content":"check"}]}]`
	parentOutput := `[{"role":"assistant","finish_reason":"stop","parts":[{"type":"text","content":"answer"}]}]`
	parent.Attributes().PutStr("gen_ai.input.messages", parentInput)
	parent.Attributes().PutStr("gen_ai.output.messages", parentOutput)

	chatA := replaySpan(spans, 2, 1, "chat model", "chat", 150, 250)
	chatA.Attributes().PutStr("gen_ai.system_instructions", `[{"type":"text","content":"verify"}]`)
	chatA.Attributes().PutStr("gen_ai.tool.definitions", `[{"name":"search"},{"name":"read_file"}]`)
	chatA.Attributes().PutStr("gen_ai.input.messages", parentInput)
	chatOutput := `[{"role":"assistant","finish_reason":"tool_call","parts":[{"type":"reasoning","content":"reasoning"},{"type":"tool_call","id":"call-A","name":"search","arguments":{"query":"check"}}]}]`
	chatA.Attributes().PutStr("gen_ai.output.messages", chatOutput)
	firstEvent := chatA.Events().AppendEmpty()
	firstEvent.SetName("model.observed")
	firstEvent.SetTimestamp(245)
	secondEvent := chatA.Events().AppendEmpty()
	secondEvent.SetName("gen_ai.future_event")
	secondEvent.SetTimestamp(245)

	toolA := replaySpan(spans, 3, 1, "execute_tool search", "execute_tool", 300, 390)
	toolA.Attributes().PutStr("gen_ai.tool.name", "search")
	toolA.Attributes().PutStr("gen_ai.tool.call.id", "call-A")
	toolA.Attributes().PutStr("gen_ai.tool.call.arguments", `{"query":"check"}`)
	toolA.Attributes().PutStr("gen_ai.tool.call.result", `"found full document"`)

	chatB := replaySpan(spans, 4, 1, "chat model", "chat", 430, 520)
	chatInput := `[{"role":"user","parts":[{"type":"text","content":"check"}]},{"role":"tool","parts":[{"type":"tool_call_response","id":"call-A","response":"found"}]}]`
	chatB.Attributes().PutStr("gen_ai.input.messages", chatInput)
	chatB.Attributes().PutStr("gen_ai.output.messages", `[{"role":"assistant","finish_reason":"stop","parts":[{"type":"text","content":"answer"}]}]`)

	toolB := replaySpan(spans, 5, 1, "execute_tool read_file", "execute_tool", 560, 650)
	toolB.Attributes().PutStr("gen_ai.tool.name", "read_file")
	toolB.Attributes().PutStr("gen_ai.tool.call.id", "call-B")
	toolB.Attributes().PutStr("gen_ai.tool.call.arguments", `{"path":"notes/check.txt"}`)
	toolB.Attributes().PutStr("gen_ai.tool.call.result", `"file contents"`)

	documents := replaySpool(t, traces)
	if len(documents) != 5 {
		t.Fatalf("span documents = %d; want five distinct source spans", len(documents))
	}
	got := replaySpansByID(t, documents)
	if len(got) != 5 {
		t.Fatalf("distinct spans = %d; want five", len(got))
	}
	gotParent := got[pcommon.SpanID{1}]
	if gotParent.TraceID() != (pcommon.TraceID{1, 2, 3}) || gotParent.ParentSpanID() != (pcommon.SpanID{}) ||
		gotParent.StartTimestamp() != 100 || gotParent.EndTimestamp() != 900 {
		t.Fatal("parent span identity or timing lost")
	}
	replayStringAttribute(t, gotParent, "gen_ai.input.messages", parentInput)
	replayStringAttribute(t, gotParent, "gen_ai.output.messages", parentOutput)
	replayStringAttribute(t, got[pcommon.SpanID{2}], "gen_ai.system_instructions", `[{"type":"text","content":"verify"}]`)
	replayStringAttribute(t, got[pcommon.SpanID{2}], "gen_ai.tool.definitions", `[{"name":"search"},{"name":"read_file"}]`)
	replayStringAttribute(t, got[pcommon.SpanID{2}], "gen_ai.output.messages", chatOutput)
	chat := got[pcommon.SpanID{2}]
	if chat.ParentSpanID() != (pcommon.SpanID{1}) || chat.Events().Len() != 2 ||
		chat.Events().At(0).Name() != "model.observed" || chat.Events().At(1).Name() != "gen_ai.future_event" ||
		chat.Events().At(0).Timestamp() != chat.Events().At(1).Timestamp() {
		t.Fatal("chat graph identity or nested event order lost")
	}
	replayStringAttribute(t, got[pcommon.SpanID{3}], "gen_ai.tool.call.arguments", `{"query":"check"}`)
	replayStringAttribute(t, got[pcommon.SpanID{3}], "gen_ai.tool.call.result", `"found full document"`)
	replayStringAttribute(t, got[pcommon.SpanID{4}], "gen_ai.input.messages", chatInput)
	replayStringAttribute(t, got[pcommon.SpanID{5}], "gen_ai.tool.call.arguments", `{"path":"notes/check.txt"}`)
	replayStringAttribute(t, got[pcommon.SpanID{5}], "gen_ai.tool.call.result", `"file contents"`)
	for _, id := range []pcommon.SpanID{{2}, {3}, {4}, {5}} {
		if got[id].ParentSpanID() != (pcommon.SpanID{1}) {
			t.Fatalf("span %s lost original parent", id)
		}
	}
}

func TestOfflineNativeSpoolPreservesOverlappingSourceSpans(t *testing.T) {
	traces, spans := replayTraces()
	replaySpan(spans, 1, 0, "invoke_agent lightspeed", "invoke_agent", 100, 800)
	first := replaySpan(spans, 2, 1, "execute_tool search", "execute_tool", 300, 500)
	first.Attributes().PutStr("gen_ai.tool.call.id", "parallel-A")
	first.Attributes().PutStr("gen_ai.tool.call.arguments", `{"query":"a"}`)
	first.Attributes().PutStr("gen_ai.tool.call.result", `"a"`)
	second := replaySpan(spans, 3, 1, "execute_tool search", "execute_tool", 350, 450)
	second.Attributes().PutStr("gen_ai.tool.call.id", "parallel-B")
	second.Attributes().PutStr("gen_ai.tool.call.arguments", `{"query":"b"}`)
	second.Attributes().PutStr("gen_ai.tool.call.result", `"b"`)
	chat := replaySpan(spans, 4, 1, "chat model", "chat", 360, 430)
	chatOutput := `[{"role":"assistant","finish_reason":"stop","parts":[{"type":"reasoning","content":"compare"},{"type":"text","content":"continue"}]}]`
	chat.Attributes().PutStr("gen_ai.output.messages", chatOutput)

	documents := replaySpool(t, traces)
	if len(documents) != 4 {
		t.Fatalf("span documents = %d; want four distinct source spans", len(documents))
	}
	got := replaySpansByID(t, documents)
	if len(got) != 4 {
		t.Fatalf("distinct spans = %d; want four", len(got))
	}
	for id, want := range map[pcommon.SpanID][2]pcommon.Timestamp{
		pcommon.SpanID{2}: {300, 500},
		pcommon.SpanID{3}: {350, 450},
		pcommon.SpanID{4}: {360, 430},
	} {
		span := got[id]
		if span.StartTimestamp() != want[0] || span.EndTimestamp() != want[1] || span.ParentSpanID() != (pcommon.SpanID{1}) {
			t.Fatalf("span %s lost its overlapping source interval or parent", id)
		}
	}
	replayStringAttribute(t, got[pcommon.SpanID{2}], "gen_ai.tool.call.arguments", `{"query":"a"}`)
	replayStringAttribute(t, got[pcommon.SpanID{3}], "gen_ai.tool.call.arguments", `{"query":"b"}`)
	replayStringAttribute(t, got[pcommon.SpanID{4}], "gen_ai.output.messages", chatOutput)
}

func TestOfflineNativeSpoolPreservesFailureEvidence(t *testing.T) {
	traces, spans := replayTraces()
	parent := replaySpan(spans, 1, 0, "invoke_agent lightspeed", "invoke_agent", 100, 700)
	parent.Attributes().PutStr("gen_ai.input.messages", `[{"role":"user","parts":[{"type":"text","content":"check"}]}]`)
	chat := replaySpan(spans, 2, 1, "chat model", "chat", 150, 240)
	chat.Attributes().PutStr("gen_ai.output.messages", `[{"role":"assistant","finish_reason":"tool_call","parts":[{"type":"tool_call","id":"failed-call","name":"search","arguments":{"query":"check"}}]}]`)
	tool := replaySpan(spans, 3, 1, "execute_tool search", "execute_tool", 300, 380)
	tool.Attributes().PutStr("gen_ai.tool.name", "search")
	tool.Attributes().PutStr("gen_ai.tool.call.id", "failed-call")
	tool.Attributes().PutStr("gen_ai.tool.call.arguments", `{"query":"check"}`)
	tool.Attributes().PutStr("error.type", "ToolTimeout")
	tool.Status().SetCode(ptrace.StatusCodeError)
	failedChat := replaySpan(spans, 4, 1, "chat model", "chat", 430, 510)
	failedChat.Attributes().PutStr("gen_ai.input.messages", `[{"role":"tool","parts":[{"type":"tool_call_response","id":"failed-call","response":"timed out"}]}]`)
	failedChat.Attributes().PutStr("error.type", "ModelUnavailable")
	failedChat.Status().SetCode(ptrace.StatusCodeError)

	documents := replaySpool(t, traces)
	if len(documents) != 4 {
		t.Fatalf("span documents = %d; want four distinct source spans", len(documents))
	}
	got := replaySpansByID(t, documents)
	if len(got) != 4 {
		t.Fatalf("distinct spans = %d; want four", len(got))
	}
	replayStringAttribute(t, got[pcommon.SpanID{3}], "error.type", "ToolTimeout")
	replayStringAttribute(t, got[pcommon.SpanID{3}], "gen_ai.tool.call.arguments", `{"query":"check"}`)
	if got[pcommon.SpanID{3}].Status().Code() != ptrace.StatusCodeError {
		t.Fatal("tool error status lost")
	}
	replayStringAttribute(t, got[pcommon.SpanID{4}], "error.type", "ModelUnavailable")
	replayStringAttribute(t, got[pcommon.SpanID{4}], "gen_ai.input.messages", `[{"role":"tool","parts":[{"type":"tool_call_response","id":"failed-call","response":"timed out"}]}]`)
	if got[pcommon.SpanID{4}].Status().Code() != ptrace.StatusCodeError {
		t.Fatal("model error status lost")
	}
	if _, exists := got[pcommon.SpanID{3}].Attributes().Get("gen_ai.tool.call.result"); exists {
		t.Fatal("failed tool acquired success-only result evidence")
	}
	if _, exists := got[pcommon.SpanID{4}].Attributes().Get("gen_ai.output.messages"); exists {
		t.Fatal("failed model acquired output evidence")
	}
}
