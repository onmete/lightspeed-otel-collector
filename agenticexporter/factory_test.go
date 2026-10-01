package agenticexporter

import (
	"context"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

func TestFactoryTypeAndDefaultConfig(t *testing.T) {
	factory := NewFactory()
	if factory.Type() != Type {
		t.Fatalf("factory type = %q, want %q", factory.Type(), Type)
	}
	cfg, ok := factory.CreateDefaultConfig().(*Config)
	if !ok {
		t.Fatalf("default config type = %T, want *Config", factory.CreateDefaultConfig())
	}
	if assessment := assessConfig(cfg); !assessment.enabled {
		t.Fatalf("default config is disabled: %s", assessment.reason)
	}
}

func TestFactoryCreatedExporterLifecycle(t *testing.T) {
	factory := NewFactory()
	root := t.TempDir()
	cfg := &Config{
		Directory:       filepath.Join(root, "traces"),
		MaxBacklogBytes: 1 << 20,
	}
	settings := exporter.Settings{
		ID: component.NewID(Type),
		TelemetrySettings: component.TelemetrySettings{
			Logger:         zap.NewNop(),
			MeterProvider:  noop.NewMeterProvider(),
			TracerProvider: tracenoop.NewTracerProvider(),
		},
	}

	exp, err := factory.CreateTraces(context.Background(), settings, cfg)
	if err != nil {
		t.Fatalf("CreateTraces() error = %v", err)
	}
	if err := exp.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := exp.ConsumeTraces(context.Background(), eligibleTestTraces("factory-secret")); err != nil {
		t.Fatalf("ConsumeTraces() error = %v", err)
	}
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	documents := readNativeReady(t, cfg.Directory)
	if len(documents) != 1 || documents[0].SpanCount() != 1 {
		t.Fatalf("factory spool documents = %d, want one native span", len(documents))
	}
	span := documents[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if span.Name() != "factory-secret" || span.Events().Len() != 2 {
		t.Fatal("factory exporter did not retain native span evidence")
	}
}
