package agenticexporter

import (
	"path/filepath"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultActionsDirectory           = "/var/lib/lightspeed-data-collection/actions"
	defaultTranscriptsDirectory       = "/var/lib/lightspeed-data-collection/transcripts"
	defaultMaxBacklogBytes      int64 = 4 * 1024 * 1024
	maxFileBytes                int64 = 1024 * 1024
	maxBatchAge                       = 30 * time.Second
)

// Config holds the user-facing configuration for the Agentic exporter.
type Config struct {
	ActionsDirectory     string `mapstructure:"actions_directory"`
	TranscriptsDirectory string `mapstructure:"transcripts_directory"`
	MaxBacklogBytes      int64  `mapstructure:"max_backlog_bytes"`
}

var _ component.Config = (*Config)(nil)

// Validate deliberately leaves invalid product configuration non-fatal. The
// exporter assesses it during construction and disables only its own branch.
func (c *Config) Validate() error {
	return nil
}

type configReason string

const (
	configOK                   configReason = "ok"
	configInvalidDirectory     configReason = "invalid_directory"
	configIdenticalDirectories configReason = "identical_directories"
	configInvalidBacklog       configReason = "invalid_backlog"
)

type configAssessment struct {
	enabled bool
	reason  configReason
}

func assessConfig(c *Config) configAssessment {
	if c == nil || c.ActionsDirectory == "" || c.TranscriptsDirectory == "" ||
		!filepath.IsAbs(c.ActionsDirectory) || !filepath.IsAbs(c.TranscriptsDirectory) {
		return configAssessment{reason: configInvalidDirectory}
	}
	if filepath.Clean(c.ActionsDirectory) == filepath.Clean(c.TranscriptsDirectory) {
		return configAssessment{reason: configIdenticalDirectories}
	}
	if c.MaxBacklogBytes <= 0 {
		return configAssessment{reason: configInvalidBacklog}
	}
	return configAssessment{enabled: true, reason: configOK}
}

func createDefaultConfig() component.Config {
	return &Config{
		ActionsDirectory:     defaultActionsDirectory,
		TranscriptsDirectory: defaultTranscriptsDirectory,
		MaxBacklogBytes:      defaultMaxBacklogBytes,
	}
}
