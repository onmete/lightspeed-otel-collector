package agenticexporter

import (
	"path/filepath"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultDirectory             = "/var/lib/lightspeed-data-collection/traces"
	defaultMaxBacklogBytes int64 = 8 * 1024 * 1024
	maxFileBytes           int64 = 1024 * 1024
	maxBatchAge                  = 30 * time.Second
	maxEncodedRecordBytes        = 120_000_000
)

// Config holds the user-facing configuration for the Agentic exporter.
type Config struct {
	Directory       string `mapstructure:"directory"`
	MaxBacklogBytes int64  `mapstructure:"max_backlog_bytes"`
}

var _ component.Config = (*Config)(nil)

// Validate deliberately leaves invalid product configuration non-fatal. The
// exporter assesses it during construction and disables only its own branch.
func (c *Config) Validate() error {
	return nil
}

type configReason string

const (
	configOK               configReason = "ok"
	configInvalidDirectory configReason = "invalid_directory"
	configInvalidBacklog   configReason = "invalid_backlog"
)

type configAssessment struct {
	enabled bool
	reason  configReason
}

func assessConfig(c *Config) configAssessment {
	if c == nil || c.Directory == "" || !filepath.IsAbs(c.Directory) {
		return configAssessment{reason: configInvalidDirectory}
	}
	if c.MaxBacklogBytes <= 0 {
		return configAssessment{reason: configInvalidBacklog}
	}
	return configAssessment{enabled: true, reason: configOK}
}

func createDefaultConfig() component.Config {
	return &Config{Directory: defaultDirectory, MaxBacklogBytes: defaultMaxBacklogBytes}
}
