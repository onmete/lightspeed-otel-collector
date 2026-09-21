package agenticdataexporter

import (
	"fmt"
	"path/filepath"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	ActionsDir     string `mapstructure:"actions_dir"`
	TranscriptsDir string `mapstructure:"transcripts_dir"`

	RetryConfig configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.ActionsDir == "" {
		return fmt.Errorf("actions_dir must not be empty")
	}
	if !filepath.IsAbs(c.ActionsDir) {
		return fmt.Errorf("actions_dir must be absolute")
	}
	if c.TranscriptsDir == "" {
		return fmt.Errorf("transcripts_dir must not be empty")
	}
	if !filepath.IsAbs(c.TranscriptsDir) {
		return fmt.Errorf("transcripts_dir must be absolute")
	}
	if filepath.Clean(c.ActionsDir) == filepath.Clean(c.TranscriptsDir) {
		return fmt.Errorf("actions_dir and transcripts_dir must differ")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		RetryConfig: configretry.NewDefaultBackOffConfig(),
		QueueConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
	}
}
