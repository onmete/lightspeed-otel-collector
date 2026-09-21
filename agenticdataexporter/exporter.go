package agenticdataexporter

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const (
	actionCandidate     = "action"
	transcriptCandidate = "transcript"
	schemaVersion       = "1.0"
	maxFileBytes        = 1 << 20
	maxFileAge          = 30 * time.Second

	serviceOperator = "lightspeed-agentic-operator"
	serviceSandbox  = "lightspeed-agentic-sandbox"
)

var (
	abandonedFile = regexp.MustCompile(`^\.[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.tmp$`)
	validPhases   = map[string]struct{}{
		"analysis":     {},
		"approval":     {},
		"execution":    {},
		"verification": {},
		"escalation":   {},
		"terminal":     {},
	}
)

type traceExporter struct {
	config      *Config
	logger      *zap.Logger
	telemetry   *telemetry
	mu          sync.Mutex
	actions     *candidateWriter
	transcripts *candidateWriter
	flushStop   chan struct{}
	flushDone   chan struct{}
}

type candidateWriter struct {
	dir           string
	candidateType string
	logger        *zap.Logger
	ready         bool
	file          *os.File
	tmpPath       string
	bytes         int
	firstRecordAt time.Time
}

func (e *traceExporter) start(_ context.Context, _ component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.actions = newCandidateWriter(e.config.ActionsDir, actionCandidate, e.logger)
	e.transcripts = newCandidateWriter(e.config.TranscriptsDir, transcriptCandidate, e.logger)
	e.prepareWriter(e.actions)
	e.prepareWriter(e.transcripts)

	e.flushStop = make(chan struct{})
	e.flushDone = make(chan struct{})
	go e.flushLoop()
	return nil
}

func newCandidateWriter(dir, candidateType string, logger *zap.Logger) *candidateWriter {
	return &candidateWriter{
		dir:           dir,
		candidateType: candidateType,
		logger:        logger,
	}
}

func (e *traceExporter) prepareWriter(writer *candidateWriter) {
	if err := os.MkdirAll(writer.dir, 0o755); err != nil {
		reportUnavailable(writer.logger, writer.candidateType, "mkdir")
		return
	}
	entries, err := os.ReadDir(writer.dir)
	if err != nil {
		reportUnavailable(writer.logger, writer.candidateType, "open")
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !abandonedFile.MatchString(entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(writer.dir, entry.Name())); err != nil {
			reportUnavailable(writer.logger, writer.candidateType, "remove_tmp")
			return
		}
	}
	writer.ready = true
}

func reportUnavailable(logger *zap.Logger, candidateType, operation string) {
	if logger != nil {
		logger.Error(
			"Agentic candidate stream unavailable",
			zap.String("candidate_type", candidateType),
			zap.String("operation", operation),
		)
	}
}

func (e *traceExporter) flushLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(e.flushDone)
	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			e.flushDue(time.Now())
			e.mu.Unlock()
		case <-e.flushStop:
			return
		}
	}
}

func (e *traceExporter) flushDue(now time.Time) {
	for _, writer := range []*candidateWriter{e.actions, e.transcripts} {
		if writer == nil || !writer.ready || writer.file == nil || now.Sub(writer.firstRecordAt) < maxFileAge {
			continue
		}
		if err := writer.publish(); err != nil {
			e.disableWriter(writer, "flush", err)
		}
	}
}

func (e *traceExporter) shutdown(context.Context) error {
	e.mu.Lock()
	stop := e.flushStop
	done := e.flushDone
	e.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	var firstErr error
	for _, writer := range []*candidateWriter{e.actions, e.transcripts} {
		if writer == nil || !writer.ready || writer.file == nil {
			continue
		}
		if err := writer.publish(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *traceExporter) consumeTraces(ctx context.Context, td ptrace.Traces) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	resourceSpans := td.ResourceSpans()
	for resourceIndex := range resourceSpans.Len() {
		resource := resourceSpans.At(resourceIndex)
		serviceName, ok := stringAttribute(resource.Resource().Attributes(), "service.name")
		if !ok || !isEligibleService(serviceName) {
			e.telemetry.recordIgnored(ctx, "unsupported_service")
			continue
		}

		scopeSpans := resource.ScopeSpans()
		for scopeIndex := range scopeSpans.Len() {
			scope := scopeSpans.At(scopeIndex)
			spans := scope.Spans()
			for spanIndex := range spans.Len() {
				span := spans.At(spanIndex)
				runID, phase, reason, ok := spanCorrelation(span)
				if !ok {
					e.telemetry.recordIgnored(ctx, reason)
					continue
				}

				e.emit(ctx, e.actions, newCandidate(resource, scope, span, serviceName, runID, phase, actionCandidate, nil))

				events := span.Events()
				for eventIndex := range events.Len() {
					event := events.At(eventIndex)
					candidateType := actionCandidate
					writer := e.actions
					if isTranscriptEvent(event) {
						candidateType = transcriptCandidate
						writer = e.transcripts
					}
					e.emit(ctx, writer, newCandidate(resource, scope, span, serviceName, runID, phase, candidateType, &eventRef{
						event: event,
						index: eventIndex,
					}))
				}
			}
		}
	}
	return nil
}

func (e *traceExporter) emit(ctx context.Context, writer *candidateWriter, record candidateRecord) {
	if writer == nil || !writer.ready {
		return
	}
	if err := writer.append(record); err != nil {
		e.disableWriter(writer, "write", err)
		return
	}
	e.telemetry.recordClassified(ctx, record.CandidateType, record.RecordKind, record.ServiceName)
}

func (e *traceExporter) disableWriter(writer *candidateWriter, operation string, err error) {
	writer.ready = false
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	if writer.logger != nil {
		writer.logger.Error(
			"Agentic candidate stream disabled",
			zap.String("candidate_type", writer.candidateType),
			zap.String("operation", operation),
			zap.Error(err),
		)
	}
}

type eventRef struct {
	event ptrace.SpanEvent
	index int
}

type candidateRecord struct {
	SchemaVersion string         `json:"schema_version"`
	CandidateType string         `json:"candidate_type"`
	RecordKind    string         `json:"record_kind"`
	Timestamp     string         `json:"timestamp"`
	AgenticRunUID string         `json:"agenticrun_uid"`
	Phase         string         `json:"phase"`
	TraceID       string         `json:"trace_id"`
	SpanID        string         `json:"span_id"`
	ParentSpanID  *string        `json:"parent_span_id"`
	EventIndex    *int           `json:"event_index"`
	ServiceName   string         `json:"service_name"`
	Name          string         `json:"name"`
	Otel          map[string]any `json:"otel"`
	Attributes    map[string]any `json:"attributes"`
}

func newCandidate(
	resourceSpans ptrace.ResourceSpans,
	scopeSpans ptrace.ScopeSpans,
	span ptrace.Span,
	serviceName string,
	runID string,
	phase string,
	candidateType string,
	event *eventRef,
) candidateRecord {
	parentSpanID := span.ParentSpanID().String()
	var parent *string
	if !span.ParentSpanID().IsEmpty() {
		parent = &parentSpanID
	}

	recordKind := "span"
	name := span.Name()
	timestamp := span.StartTimestamp()
	var eventIndex *int
	var eventAttributes []any
	var eventMetadata map[string]any
	if event != nil {
		recordKind = "span_event"
		name = event.event.Name()
		timestamp = event.event.Timestamp()
		index := event.index
		eventIndex = &index
		eventAttributes = attributeList(event.event.Attributes())
		eventMetadata = map[string]any{
			"droppedAttributesCount": event.event.DroppedAttributesCount(),
		}
	}

	links := make([]any, 0, span.Links().Len())
	for linkIndex := range span.Links().Len() {
		link := span.Links().At(linkIndex)
		links = append(links, map[string]any{
			"traceId":                link.TraceID().String(),
			"spanId":                 link.SpanID().String(),
			"traceState":             link.TraceState().AsRaw(),
			"flags":                  link.Flags(),
			"attributes":             attributeList(link.Attributes()),
			"droppedAttributesCount": link.DroppedAttributesCount(),
		})
	}

	status := span.Status()
	otel := map[string]any{
		"resource": map[string]any{
			"schemaUrl":              resourceSpans.SchemaUrl(),
			"droppedAttributesCount": resourceSpans.Resource().DroppedAttributesCount(),
		},
		"scope": map[string]any{
			"name":                   scopeSpans.Scope().Name(),
			"version":                scopeSpans.Scope().Version(),
			"schemaUrl":              scopeSpans.SchemaUrl(),
			"droppedAttributesCount": scopeSpans.Scope().DroppedAttributesCount(),
		},
		"span": map[string]any{
			"startTimeUnixNano":      strconv.FormatUint(uint64(span.StartTimestamp()), 10),
			"endTimeUnixNano":        strconv.FormatUint(uint64(span.EndTimestamp()), 10),
			"kind":                   spanKind(span.Kind()),
			"status":                 map[string]any{"code": statusCode(status.Code()), "message": status.Message()},
			"traceState":             span.TraceState().AsRaw(),
			"flags":                  span.Flags(),
			"links":                  links,
			"droppedAttributesCount": span.DroppedAttributesCount(),
			"droppedEventsCount":     span.DroppedEventsCount(),
			"droppedLinksCount":      span.DroppedLinksCount(),
		},
	}
	if event != nil {
		otel["event"] = eventMetadata
	}

	attributes := map[string]any{
		"resource": attributeList(resourceSpans.Resource().Attributes()),
		"scope":    attributeList(scopeSpans.Scope().Attributes()),
		"span":     attributeList(span.Attributes()),
	}
	if event != nil {
		attributes["event"] = eventAttributes
	}

	return candidateRecord{
		SchemaVersion: schemaVersion,
		CandidateType: candidateType,
		RecordKind:    recordKind,
		Timestamp:     strconv.FormatUint(uint64(timestamp), 10),
		AgenticRunUID: runID,
		Phase:         phase,
		TraceID:       span.TraceID().String(),
		SpanID:        span.SpanID().String(),
		ParentSpanID:  parent,
		EventIndex:    eventIndex,
		ServiceName:   serviceName,
		Name:          name,
		Otel:          otel,
		Attributes:    attributes,
	}
}

func stringAttribute(attributes pcommon.Map, key string) (string, bool) {
	value, ok := attributes.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	return value.AsString(), true
}

// Producer code owns the UID shape; the Collector only enforces the input
// contract's literal string and non-empty requirements.
func spanCorrelation(span ptrace.Span) (string, string, string, bool) {
	runID, ok := stringAttribute(span.Attributes(), "agenticrun.uid")
	if !ok || runID == "" {
		return "", "", "missing_uid", false
	}
	phase, ok := stringAttribute(span.Attributes(), "agenticrun.phase")
	if !ok {
		return "", "", "missing_or_invalid_phase", false
	}
	if _, ok := validPhases[phase]; !ok {
		return "", "", "missing_or_invalid_phase", false
	}
	return runID, phase, "", true
}

func isEligibleService(serviceName string) bool {
	return serviceName == serviceOperator || serviceName == serviceSandbox
}

func isTranscriptEvent(event ptrace.SpanEvent) bool {
	attributes := event.Attributes()
	switch event.Name() {
	case "gen_ai.input":
		return hasAttributes(attributes,
			"gen_ai.input.system_prompt",
			"gen_ai.input.prompt",
			"gen_ai.input.context",
			"gen_ai.input.output_schema",
		)
	case "gen_ai.choice":
		return hasAnyAttribute(attributes, "gen_ai.completion", "gen_ai.reasoning_content")
	case "gen_ai.tool.call":
		return hasAttributes(attributes, "gen_ai.tool.name", "gen_ai.tool.call.id", "tool.input")
	case "gen_ai.tool.result":
		return hasAttributes(attributes, "gen_ai.tool.name", "gen_ai.tool.call.id", "tool.status", "tool.output")
	case "gen_ai.skill.loaded", "gen_ai.skill.used":
		return hasAttributes(attributes, "gen_ai.skill.name")
	case "gen_ai.output":
		return hasAttributes(attributes,
			"gen_ai.output.value",
			"gen_ai.request.model",
			"gen_ai.response.model",
			"gen_ai.usage.input_tokens",
			"gen_ai.usage.output_tokens",
			"gen_ai.usage.reasoning_tokens",
		)
	default:
		return false
	}
}

func hasAttributes(attributes pcommon.Map, keys ...string) bool {
	for _, key := range keys {
		if _, ok := attributes.Get(key); !ok {
			return false
		}
	}
	return true
}

func hasAnyAttribute(attributes pcommon.Map, keys ...string) bool {
	for _, key := range keys {
		if _, ok := attributes.Get(key); ok {
			return true
		}
	}
	return false
}

func attributeList(attributes pcommon.Map) []any {
	keys := make([]string, 0, attributes.Len())
	attributes.Range(func(key string, _ pcommon.Value) bool {
		keys = append(keys, key)
		return true
	})
	sort.Strings(keys)

	values := make([]any, 0, len(keys))
	for _, key := range keys {
		value, _ := attributes.Get(key)
		values = append(values, map[string]any{
			"key":   key,
			"value": anyValue(value),
		})
	}
	return values
}

func anyValue(value pcommon.Value) map[string]any {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return map[string]any{"stringValue": value.Str()}
	case pcommon.ValueTypeBool:
		return map[string]any{"boolValue": value.Bool()}
	case pcommon.ValueTypeInt:
		return map[string]any{"intValue": value.Int()}
	case pcommon.ValueTypeDouble:
		return map[string]any{"doubleValue": otlpFloat(value.Double())}
	case pcommon.ValueTypeBytes:
		return map[string]any{"bytesValue": base64.StdEncoding.EncodeToString(value.Bytes().AsRaw())}
	case pcommon.ValueTypeMap:
		values := attributeList(value.Map())
		container := map[string]any{}
		if len(values) > 0 {
			container["values"] = values
		}
		return map[string]any{"kvlistValue": container}
	case pcommon.ValueTypeSlice:
		values := make([]any, 0, value.Slice().Len())
		for index := range value.Slice().Len() {
			values = append(values, anyValue(value.Slice().At(index)))
		}
		container := map[string]any{}
		if len(values) > 0 {
			container["values"] = values
		}
		return map[string]any{"arrayValue": container}
	default:
		return map[string]any{}
	}
}

func otlpFloat(value float64) any {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	default:
		return value
	}
}

func spanKind(kind ptrace.SpanKind) string {
	switch kind {
	case ptrace.SpanKindInternal:
		return "SPAN_KIND_INTERNAL"
	case ptrace.SpanKindServer:
		return "SPAN_KIND_SERVER"
	case ptrace.SpanKindClient:
		return "SPAN_KIND_CLIENT"
	case ptrace.SpanKindProducer:
		return "SPAN_KIND_PRODUCER"
	case ptrace.SpanKindConsumer:
		return "SPAN_KIND_CONSUMER"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}

func statusCode(code ptrace.StatusCode) string {
	switch code {
	case ptrace.StatusCodeOk:
		return "STATUS_CODE_OK"
	case ptrace.StatusCodeError:
		return "STATUS_CODE_ERROR"
	default:
		return "STATUS_CODE_UNSET"
	}
}

func (w *candidateWriter) append(record candidateRecord) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal candidate: %w", err)
	}
	line = append(line, '\n')
	if w.file != nil && w.bytes > 0 && w.bytes+len(line) > maxFileBytes {
		if err := w.publish(); err != nil {
			return err
		}
	}
	if w.file == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	if err := writeAll(w.file, line); err != nil {
		return fmt.Errorf("write candidate: %w", err)
	}
	w.bytes += len(line)
	if w.firstRecordAt.IsZero() {
		w.firstRecordAt = time.Now()
	}
	if w.bytes >= maxFileBytes {
		return w.publish()
	}
	return nil
}

func (w *candidateWriter) open() error {
	id, err := newUUID()
	if err != nil {
		return fmt.Errorf("generate candidate filename: %w", err)
	}
	w.tmpPath = filepath.Join(w.dir, "."+id+".tmp")
	file, err := os.OpenFile(w.tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open candidate file: %w", err)
	}
	w.file = file
	w.bytes = 0
	w.firstRecordAt = time.Time{}
	return nil
}

func (w *candidateWriter) publish() error {
	if w.file == nil {
		return nil
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close candidate file: %w", err)
	}
	w.file = nil
	base := filepath.Base(w.tmpPath)
	id := strings.TrimPrefix(strings.TrimSuffix(base, ".tmp"), ".")
	readyPath := filepath.Join(w.dir, id+".jsonl")
	if err := os.Rename(w.tmpPath, readyPath); err != nil {
		return fmt.Errorf("publish candidate file: %w", err)
	}
	w.tmpPath = ""
	w.bytes = 0
	w.firstRecordAt = time.Time{}
	return nil
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return fmt.Errorf("short write")
		}
		data = data[written:]
	}
	return nil
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	var encoded [36]byte
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])
	return string(encoded[:]), nil
}
