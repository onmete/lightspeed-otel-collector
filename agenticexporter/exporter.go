package agenticexporter

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type agenticExporter struct {
	enabled        bool
	disabledReason configReason
	actions        *streamWriter
	transcripts    *streamWriter
	telemetry      *telemetry
	logger         *zap.Logger
	disabledLog    sync.Once
}

func newAgenticExporter(settings exporter.Settings, cfg *Config) (*agenticExporter, error) {
	assessment := assessConfig(cfg)
	logger := settings.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	tel, err := newTelemetry(settings.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	actionsDirectory := ""
	transcriptsDirectory := ""
	maxBacklogBytes := int64(0)
	if cfg != nil {
		actionsDirectory = cfg.ActionsDirectory
		transcriptsDirectory = cfg.TranscriptsDirectory
		maxBacklogBytes = cfg.MaxBacklogBytes
	}
	e := &agenticExporter{
		enabled:        assessment.enabled,
		disabledReason: assessment.reason,
		actions:        newStreamWriter(candidateAction, actionsDirectory, maxBacklogBytes),
		transcripts:    newStreamWriter(candidateTranscript, transcriptsDirectory, maxBacklogBytes),
		telemetry:      tel,
		logger:         logger,
	}
	e.actions.callbacks = e.callbacks()
	e.transcripts.callbacks = e.callbacks()
	if err := tel.registerWriters(e.actions, e.transcripts); err != nil {
		tel.close()
		return nil, err
	}
	return e, nil
}

func (e *agenticExporter) callbacks() streamCallbacks {
	return streamCallbacks{
		stateChanged: func(candidate candidateType, _ streamState, to streamState, operation fileOperation) {
			e.logger.Warn(
				"agentic exporter stream state changed",
				zap.String("candidate_type", string(candidate)),
				zap.String("state", streamStateLabel(to)),
				zap.String("operation", fileOperationLabel(operation)),
			)
		},
		operationFailed: func(candidate candidateType, operation fileOperation) {
			e.telemetry.recordFileOperationFailure(context.Background(), candidate, operation)
		},
		published: func(candidate candidateType, records, bytes int64) {
			e.telemetry.recordPublished(context.Background(), candidate, records, bytes)
		},
		shutdownDeadline: func(candidate candidateType, records, bytes int64) {
			e.logger.Warn(
				"agentic exporter shutdown deadline reached",
				zap.String("candidate_type", string(candidate)),
				zap.Int64("unpublished_records", records),
				zap.Int64("unpublished_bytes", bytes),
			)
		},
	}
}

func (e *agenticExporter) start(ctx context.Context, _ component.Host) error {
	if !e.enabled {
		e.disabledLog.Do(func() {
			e.logger.Error(
				"agentic exporter disabled by invalid deployment configuration",
				zap.String("reason", string(e.disabledReason)),
			)
		})
		return nil
	}
	_ = e.actions.start(ctx)
	_ = e.transcripts.start(ctx)
	return nil
}

func (e *agenticExporter) consumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if !e.enabled {
		return nil
	}

	resourceSpans := traces.ResourceSpans()
	for resourceIndex := range resourceSpans.Len() {
		resourceSpan := resourceSpans.At(resourceIndex)
		resource := resourceSpan.Resource()
		scopeSpans := resourceSpan.ScopeSpans()
		for scopeIndex := range scopeSpans.Len() {
			scopeSpan := scopeSpans.At(scopeIndex)
			scope := scopeSpan.Scope()
			spans := scopeSpan.Spans()
			for spanIndex := range spans.Len() {
				span := spans.At(spanIndex)
				spanCtx, rejection := classifySpan(resource.Attributes(), span)
				if rejection != rejectNone {
					serviceName, _ := stringAttribute(resource.Attributes(), "service.name")
					e.telemetry.recordRejection(ctx, candidateAction, recordSpan, serviceName, rejection)
					events := span.Events()
					for eventIndex := range events.Len() {
						e.telemetry.recordRejection(
							ctx,
							classifyEvent(events.At(eventIndex)),
							recordSpanEvent,
							serviceName,
							rejection,
						)
					}
					continue
				}

				e.projectAndAdmit(ctx, projectionInput{
					candidate:         candidateAction,
					kind:              recordSpan,
					context:           spanCtx,
					resource:          resource,
					resourceSchemaURL: resourceSpan.SchemaUrl(),
					scope:             scope,
					scopeSchemaURL:    scopeSpan.SchemaUrl(),
					span:              span,
				})

				events := span.Events()
				for eventIndex := range events.Len() {
					event := events.At(eventIndex)
					index := eventIndex
					candidate := classifyEvent(event)
					e.projectAndAdmit(ctx, projectionInput{
						candidate:         candidate,
						kind:              recordSpanEvent,
						context:           spanCtx,
						resource:          resource,
						resourceSchemaURL: resourceSpan.SchemaUrl(),
						scope:             scope,
						scopeSchemaURL:    scopeSpan.SchemaUrl(),
						span:              span,
						event:             &event,
						eventIndex:        &index,
					})
				}
			}
		}
	}
	return nil
}

func (e *agenticExporter) projectAndAdmit(ctx context.Context, input projectionInput) {
	record, err := project(input)
	if err != nil {
		e.telemetry.recordRejection(ctx, input.candidate, input.kind, input.context.serviceName, rejectInvalidEnvelope)
		return
	}
	writer := e.actions
	if input.candidate == candidateTranscript {
		writer = e.transcripts
	}
	if !writer.tryEnqueue(record) {
		e.telemetry.recordRejection(ctx, input.candidate, input.kind, input.context.serviceName, rejectQueueFull)
		return
	}
	e.telemetry.recordCandidate(ctx, input.candidate, input.kind, input.context.serviceName, len(record))
}

func (e *agenticExporter) shutdown(ctx context.Context) error {
	if !e.enabled {
		e.telemetry.close()
		return nil
	}

	done := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		e.actions.shutdown(ctx)
	}()
	go func() {
		defer wait.Done()
		e.transcripts.shutdown(ctx)
	}()
	go func() {
		wait.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.telemetry.close()
	case <-ctx.Done():
		go func() {
			<-done
			e.telemetry.close()
		}()
	}
	return nil
}

func streamStateLabel(state streamState) string {
	switch state {
	case streamHealthy:
		return "healthy"
	case streamDegraded:
		return "degraded"
	default:
		return "unavailable"
	}
}

func fileOperationLabel(operation fileOperation) string {
	if operation == "" {
		return "none"
	}
	return string(operation)
}
