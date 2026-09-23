package agenticexporter

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const projectionQueueCapacity = 64

type agenticExporter struct {
	enabled        bool
	disabledReason configReason
	actions        *streamWriter
	transcripts    *streamWriter
	telemetry      *telemetry
	logger         *zap.Logger
	disabledLog    sync.Once

	projectionQueue      chan projectionJob
	projectionStop       chan struct{}
	projectionCancel     chan struct{}
	projectionDone       chan struct{}
	projectionMu         sync.Mutex
	projectionStarted    bool
	projectionStopped    bool
	projectionCanceled   bool
	projectionAccepting  bool
	projectionProcessing bool
	shutdownOnce         sync.Once
	shutdownDone         chan struct{}
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
		enabled:          assessment.enabled,
		disabledReason:   assessment.reason,
		actions:          newStreamWriter(candidateAction, actionsDirectory, maxBacklogBytes),
		transcripts:      newStreamWriter(candidateTranscript, transcriptsDirectory, maxBacklogBytes),
		telemetry:        tel,
		logger:           logger,
		projectionQueue:  make(chan projectionJob, projectionQueueCapacity),
		projectionStop:   make(chan struct{}),
		projectionCancel: make(chan struct{}),
		projectionDone:   make(chan struct{}),
		shutdownDone:     make(chan struct{}),
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
	e.startProjection()
	_ = e.actions.start(ctx)
	_ = e.transcripts.start(ctx)
	return nil
}

func (e *agenticExporter) consumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if !e.enabled {
		return nil
	}
	metricsCtx := metricContextWithoutSpan(ctx)

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
					e.telemetry.recordRejection(metricsCtx, candidateAction, recordSpan, serviceName, rejection)
					events := span.Events()
					for eventIndex := range events.Len() {
						e.telemetry.recordRejection(
							metricsCtx,
							classifyEvent(events.At(eventIndex)),
							recordSpanEvent,
							serviceName,
							rejection,
						)
					}
					continue
				}

				input := projectionInput{
					candidate:         candidateAction,
					kind:              recordSpan,
					context:           spanCtx,
					resource:          resource,
					resourceSchemaURL: resourceSpan.SchemaUrl(),
					scope:             scope,
					scopeSchemaURL:    scopeSpan.SchemaUrl(),
					span:              span,
				}
				if !e.tryEnqueueProjection(metricsCtx, input) {
					e.recordQueueRejections(metricsCtx, spanCtx, span)
				}
			}
		}
	}
	return nil
}

func (e *agenticExporter) recordQueueRejections(ctx context.Context, spanCtx spanContext, span ptrace.Span) {
	e.telemetry.recordRejection(ctx, candidateAction, recordSpan, spanCtx.serviceName, rejectQueueFull)
	events := span.Events()
	for eventIndex := range events.Len() {
		e.telemetry.recordRejection(
			ctx,
			classifyEvent(events.At(eventIndex)),
			recordSpanEvent,
			spanCtx.serviceName,
			rejectQueueFull,
		)
	}
}

func (e *agenticExporter) projectAndAdmit(ctx context.Context, input projectionInput) {
	record, err := project(input)
	if err != nil {
		e.telemetry.recordRejection(ctx, input.candidate, input.kind, input.context.serviceName, rejectInvalidEnvelope)
		e.logger.Error(
			"agentic exporter rejected candidate",
			zap.String("reason", string(rejectInvalidEnvelope)),
		)
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

func (e *agenticExporter) startProjection() {
	e.projectionMu.Lock()
	if e.projectionStarted {
		e.projectionMu.Unlock()
		return
	}
	e.projectionStarted = true
	e.projectionAccepting = true
	done := e.projectionDone
	e.projectionMu.Unlock()
	go func() {
		defer close(done)
		e.runProjection()
	}()
}

func (e *agenticExporter) runProjection() {
	defer func() {
		e.projectionMu.Lock()
		e.projectionProcessing = false
		e.projectionMu.Unlock()
	}()
	for {
		select {
		case <-e.projectionCancel:
			e.discardProjectionQueue()
			return
		default:
		}
		select {
		case <-e.projectionCancel:
			e.discardProjectionQueue()
			return
		case <-e.projectionStop:
			for {
				select {
				case <-e.projectionCancel:
					e.discardProjectionQueue()
					return
				default:
				}
				select {
				case <-e.projectionCancel:
					e.discardProjectionQueue()
					return
				case job := <-e.projectionQueue:
					e.processProjectionJob(job)
				default:
					return
				}
			}
		case job := <-e.projectionQueue:
			e.processProjectionJob(job)
		}
	}
}

func (e *agenticExporter) discardProjectionQueue() {
	for {
		select {
		case job := <-e.projectionQueue:
			e.recordProjectionRejections(job)
		default:
			return
		}
	}
}

func (e *agenticExporter) recordProjectionRejections(job projectionJob) {
	span := job.traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	e.telemetry.recordRejection(
		job.metricsContext,
		candidateAction,
		recordSpan,
		job.context.serviceName,
		rejectShutdown,
	)
	for eventIndex := range span.Events().Len() {
		e.telemetry.recordRejection(
			job.metricsContext,
			classifyEvent(span.Events().At(eventIndex)),
			recordSpanEvent,
			job.context.serviceName,
			rejectShutdown,
		)
	}
}

func (e *agenticExporter) recordProjectionEventRejection(job projectionJob, event ptrace.SpanEvent) {
	e.telemetry.recordRejection(
		job.metricsContext,
		classifyEvent(event),
		recordSpanEvent,
		job.context.serviceName,
		rejectShutdown,
	)
}

func (e *agenticExporter) projectionCancellationRequested() bool {
	e.projectionMu.Lock()
	defer e.projectionMu.Unlock()
	return e.projectionCanceled
}

func (e *agenticExporter) processProjectionJob(job projectionJob) {
	if e.projectionCancellationRequested() {
		e.recordProjectionRejections(job)
		return
	}
	defer func() {
		e.projectionMu.Lock()
		e.projectionProcessing = len(e.projectionQueue) != 0
		e.projectionMu.Unlock()
	}()

	resourceSpan := job.traces.ResourceSpans().At(0)
	scopeSpan := resourceSpan.ScopeSpans().At(0)
	span := scopeSpan.Spans().At(0)
	e.projectAndAdmit(job.metricsContext, projectionInput{
		candidate:         candidateAction,
		kind:              recordSpan,
		context:           job.context,
		resource:          resourceSpan.Resource(),
		resourceSchemaURL: job.resourceSchemaURL,
		scope:             scopeSpan.Scope(),
		scopeSchemaURL:    job.scopeSchemaURL,
		span:              span,
	})
	events := span.Events()
	for eventIndex := range events.Len() {
		if e.projectionCancellationRequested() {
			for remaining := eventIndex; remaining < events.Len(); remaining++ {
				e.recordProjectionEventRejection(job, events.At(remaining))
			}
			return
		}
		event := events.At(eventIndex)
		index := eventIndex
		e.projectAndAdmit(job.metricsContext, projectionInput{
			candidate:         classifyEvent(event),
			kind:              recordSpanEvent,
			context:           job.context,
			resource:          resourceSpan.Resource(),
			resourceSchemaURL: job.resourceSchemaURL,
			scope:             scopeSpan.Scope(),
			scopeSchemaURL:    job.scopeSchemaURL,
			span:              span,
			event:             &event,
			eventIndex:        &index,
		})
	}
}

func (e *agenticExporter) tryEnqueueProjection(ctx context.Context, input projectionInput) bool {
	e.projectionMu.Lock()
	defer e.projectionMu.Unlock()
	if !e.projectionAccepting || len(e.projectionQueue) == cap(e.projectionQueue) {
		return false
	}
	e.projectionProcessing = true
	e.projectionQueue <- newProjectionJob(ctx, input)
	return true
}

func (e *agenticExporter) projectionIdle() bool {
	e.projectionMu.Lock()
	defer e.projectionMu.Unlock()
	return len(e.projectionQueue) == 0 && !e.projectionProcessing
}

func (e *agenticExporter) waitProjectionDone() {
	e.projectionMu.Lock()
	started := e.projectionStarted
	done := e.projectionDone
	e.projectionMu.Unlock()
	if started {
		<-done
	}
}

func (e *agenticExporter) stopProjection(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.projectionMu.Lock()
	if !e.projectionStarted {
		e.projectionAccepting = false
		e.projectionMu.Unlock()
		return
	}
	e.projectionAccepting = false
	if !e.projectionStopped {
		e.projectionStopped = true
		close(e.projectionStop)
	}
	done := e.projectionDone
	e.projectionMu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
		e.projectionMu.Lock()
		if !e.projectionCanceled {
			e.projectionCanceled = true
			close(e.projectionCancel)
		}
		e.projectionMu.Unlock()
	}
}

func (e *agenticExporter) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.shutdownOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, maxShutdownFlush)
		go func() {
			defer cancel()
			e.shutdownOnceRun(shutdownCtx)
		}()
	})
	select {
	case <-e.shutdownDone:
	case <-ctx.Done():
	}
	return nil
}

func (e *agenticExporter) shutdownOnceRun(ctx context.Context) {
	defer close(e.shutdownDone)
	if e.enabled {
		e.stopProjection(ctx)
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
		wait.Wait()
		e.waitProjectionDone()
	}
	e.telemetry.close()
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
