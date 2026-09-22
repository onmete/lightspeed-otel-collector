package agenticexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates the Agentic trace exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
	)
}

func createTracesExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	agenticConfig, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid agentic exporter configuration type %T", cfg)
	}
	exp, err := newAgenticExporter(settings, agenticConfig)
	if err != nil {
		return nil, err
	}
	wrapped, err := exporterhelper.NewTraces(
		ctx,
		settings,
		cfg,
		exp.consumeTraces,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
	if err != nil {
		exp.telemetry.close()
		return nil, err
	}
	return wrapped, nil
}
