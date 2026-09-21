package agenticdataexporter

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
	)
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	ec := cfg.(*Config)
	telemetry, err := newTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	e := &traceExporter{
		config:    ec,
		logger:    set.Logger,
		telemetry: telemetry,
	}
	exp, err := exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		e.consumeTraces,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithRetry(ec.RetryConfig),
		exporterhelper.WithQueue(ec.QueueConfig),
	)
	if err != nil {
		return nil, err
	}
	return &isolatingTracesExporter{Traces: exp, logger: set.Logger}, nil
}

type isolatingTracesExporter struct {
	exporter.Traces
	logger *zap.Logger
}

func (e *isolatingTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	err := e.Traces.ConsumeTraces(ctx, td)
	if errors.Is(err, exporterhelper.ErrQueueIsFull) {
		if e.logger != nil {
			e.logger.Warn("Agentic candidate queue is full; dropping candidates")
		}
		return nil
	}
	return err
}
