package agenticexporter

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const encodingQueueCapacity = 64

type encodingJob struct {
	traces         ptrace.Traces
	serviceName    string
	metricsContext context.Context
}

type agenticExporter struct {
	enabled        bool
	disabledReason configReason
	writer         *streamWriter
	telemetry      *telemetry
	logger         *zap.Logger
	disabledLog    sync.Once

	encodingQueue      chan encodingJob
	encodingStop       chan struct{}
	encodingCancel     chan struct{}
	encodingDone       chan struct{}
	encodingMu         sync.Mutex
	encodingStarted    bool
	encodingStopped    bool
	encodingCanceled   bool
	encodingAccepting  bool
	encodingProcessing bool
	shutdownOnce       sync.Once
	shutdownDone       chan struct{}
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

	directory := ""
	maxBacklogBytes := int64(0)
	if cfg != nil {
		directory = cfg.Directory
		maxBacklogBytes = cfg.MaxBacklogBytes
	}
	writer := newStreamWriter(directory, maxBacklogBytes)
	e := &agenticExporter{
		enabled:        assessment.enabled,
		disabledReason: assessment.reason,
		writer:         writer,
		telemetry:      tel,
		logger:         logger,
		encodingQueue:  make(chan encodingJob, encodingQueueCapacity),
		encodingStop:   make(chan struct{}),
		encodingCancel: make(chan struct{}),
		encodingDone:   make(chan struct{}),
		shutdownDone:   make(chan struct{}),
	}
	writer.callbacks = e.callbacks()
	if err := tel.registerWriter(writer); err != nil {
		tel.close()
		return nil, err
	}
	return e, nil
}

func (e *agenticExporter) callbacks() streamCallbacks {
	return streamCallbacks{
		stateChanged: func(from, to streamState, operation fileOperation) {
			e.logger.Warn(
				"agentic exporter stream state changed",
				zap.String("from_state", streamStateLabel(from)),
				zap.String("state", streamStateLabel(to)),
				zap.String("operation", fileOperationLabel(operation)),
			)
		},
		operationFailed: func(operation fileOperation) {
			e.telemetry.recordFileOperationFailure(context.Background(), operation)
		},
		published: func(records, bytes int64) {
			e.telemetry.recordPublished(context.Background(), records, bytes)
		},
		shutdownDeadline: func(records, bytes int64) {
			e.logger.Warn(
				"agentic exporter shutdown deadline reached",
				zap.String("reason", string(rejectShutdown)),
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
	_ = e.writer.start(ctx)
	e.startEncoding()
	return nil
}

func (e *agenticExporter) consumeTraces(ctx context.Context, traces ptrace.Traces) error {
	if !e.enabled {
		return nil
	}
	metricsCtx := metricContextWithoutSpan(ctx)

	resourceSpans := traces.ResourceSpans()
	for resourceIndex := range resourceSpans.Len() {
		resource := resourceSpans.At(resourceIndex)
		serviceName, _ := stringAttribute(resource.Resource().Attributes(), "service.name")
		scopeSpans := resource.ScopeSpans()
		for scopeIndex := range scopeSpans.Len() {
			scope := scopeSpans.At(scopeIndex)
			spans := scope.Spans()
			for spanIndex := range spans.Len() {
				span := spans.At(spanIndex)
				spanCtx, rejection := classifySpan(resource.Resource().Attributes(), span)
				if rejection != rejectNone {
					e.telemetry.recordRejection(metricsCtx, serviceName, rejection)
					continue
				}
				if span.TraceID() == (pcommon.TraceID{}) || span.SpanID() == (pcommon.SpanID{}) {
					e.telemetry.recordRejection(metricsCtx, spanCtx.serviceName, rejectInvalidTraceIdentity)
					continue
				}
				if !e.tryEnqueueEncoding(metricsCtx, resource, scope, span, spanCtx.serviceName) {
					e.telemetry.recordRejection(metricsCtx, spanCtx.serviceName, rejectQueueFull)
				}
			}
		}
	}
	return nil
}

func newEncodingJob(
	ctx context.Context,
	resource ptrace.ResourceSpans,
	scope ptrace.ScopeSpans,
	span ptrace.Span,
	serviceName string,
) encodingJob {
	copied := ptrace.NewTraces()
	targetResource := copied.ResourceSpans().AppendEmpty()
	resource.Resource().CopyTo(targetResource.Resource())
	targetResource.SetSchemaUrl(resource.SchemaUrl())
	targetScope := targetResource.ScopeSpans().AppendEmpty()
	scope.Scope().CopyTo(targetScope.Scope())
	targetScope.SetSchemaUrl(scope.SchemaUrl())
	span.CopyTo(targetScope.Spans().AppendEmpty())
	return encodingJob{traces: copied, serviceName: serviceName, metricsContext: ctx}
}

func (e *agenticExporter) tryEnqueueEncoding(
	ctx context.Context,
	resource ptrace.ResourceSpans,
	scope ptrace.ScopeSpans,
	span ptrace.Span,
	serviceName string,
) bool {
	e.encodingMu.Lock()
	defer e.encodingMu.Unlock()
	if !e.encodingAccepting || len(e.encodingQueue) == cap(e.encodingQueue) {
		return false
	}
	e.encodingProcessing = true
	e.encodingQueue <- newEncodingJob(ctx, resource, scope, span, serviceName)
	return true
}

func (e *agenticExporter) encodeAndAdmit(job encodingJob) {
	marshaler := ptrace.JSONMarshaler{}
	encoded, err := marshaler.MarshalTraces(job.traces)
	if err != nil {
		e.telemetry.recordRejection(job.metricsContext, job.serviceName, rejectEncoding)
		e.logger.Error("agentic exporter encoding failed", zap.String("reason", string(rejectEncoding)))
		return
	}
	tooLarge := len(encoded) >= maxEncodedRecordBytes // encoded JSON plus LF exceeds the cap
	size := len(encoded) + 1
	if !tooLarge {
		encoded = append(encoded, '\n')
	}
	e.encodingMu.Lock()
	canceled := e.encodingCanceled
	accepted := false
	if !canceled && !tooLarge {
		accepted = e.writer.tryEnqueue(encoded)
	}
	e.encodingMu.Unlock()
	if canceled {
		e.recordEncodingRejection(job)
		return
	}
	if tooLarge {
		e.telemetry.recordRejection(job.metricsContext, job.serviceName, rejectRecordTooLarge)
		return
	}
	if !accepted {
		e.telemetry.recordRejection(job.metricsContext, job.serviceName, rejectQueueFull)
		return
	}
	e.telemetry.recordAdmission(job.metricsContext, job.serviceName, size)
}

func (e *agenticExporter) startEncoding() {
	e.encodingMu.Lock()
	if e.encodingStarted {
		e.encodingMu.Unlock()
		return
	}
	e.encodingStarted = true
	e.encodingAccepting = true
	done := e.encodingDone
	e.encodingMu.Unlock()
	go func() {
		defer close(done)
		e.runEncoding()
	}()
}

func (e *agenticExporter) runEncoding() {
	defer func() {
		e.encodingMu.Lock()
		e.encodingProcessing = false
		e.encodingMu.Unlock()
	}()
	for {
		select {
		case <-e.encodingCancel:
			e.discardEncodingQueue()
			return
		default:
		}
		select {
		case <-e.encodingCancel:
			e.discardEncodingQueue()
			return
		case <-e.encodingStop:
			for {
				select {
				case <-e.encodingCancel:
					e.discardEncodingQueue()
					return
				default:
				}
				select {
				case <-e.encodingCancel:
					e.discardEncodingQueue()
					return
				case job := <-e.encodingQueue:
					e.processEncodingJob(job)
				default:
					return
				}
			}
		case job := <-e.encodingQueue:
			e.processEncodingJob(job)
		}
	}
}

func (e *agenticExporter) discardEncodingQueue() {
	for {
		select {
		case job := <-e.encodingQueue:
			e.recordEncodingRejection(job)
		default:
			return
		}
	}
}

func (e *agenticExporter) recordEncodingRejection(job encodingJob) {
	e.telemetry.recordRejection(job.metricsContext, job.serviceName, rejectShutdown)
}

func (e *agenticExporter) encodingCancellationRequested() bool {
	e.encodingMu.Lock()
	defer e.encodingMu.Unlock()
	return e.encodingCanceled
}

func (e *agenticExporter) processEncodingJob(job encodingJob) {
	if e.encodingCancellationRequested() {
		e.recordEncodingRejection(job)
		return
	}
	defer func() {
		e.encodingMu.Lock()
		e.encodingProcessing = len(e.encodingQueue) != 0
		e.encodingMu.Unlock()
	}()
	e.encodeAndAdmit(job)
}

func (e *agenticExporter) encodingIdle() bool {
	e.encodingMu.Lock()
	defer e.encodingMu.Unlock()
	return len(e.encodingQueue) == 0 && !e.encodingProcessing
}

func (e *agenticExporter) waitEncodingDone() {
	e.encodingMu.Lock()
	started := e.encodingStarted
	done := e.encodingDone
	e.encodingMu.Unlock()
	if started {
		<-done
	}
}

func (e *agenticExporter) stopEncoding(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.encodingMu.Lock()
	if !e.encodingStarted {
		e.encodingAccepting = false
		e.encodingMu.Unlock()
		return
	}
	e.encodingAccepting = false
	if !e.encodingStopped {
		e.encodingStopped = true
		close(e.encodingStop)
	}
	done := e.encodingDone
	e.encodingMu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
		e.encodingMu.Lock()
		if !e.encodingCanceled {
			e.encodingCanceled = true
			close(e.encodingCancel)
		}
		e.encodingMu.Unlock()
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
		e.stopEncoding(ctx)
		e.writer.shutdown(ctx)
		e.waitEncodingDone()
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
