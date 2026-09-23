package agenticexporter

import (
	"testing"
	"time"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg, ok := createDefaultConfig().(*Config)
	if !ok {
		t.Fatalf("createDefaultConfig() returned %T, want *Config", createDefaultConfig())
	}

	if cfg.ActionsDirectory != "/var/lib/lightspeed-data-collection/actions" {
		t.Errorf("ActionsDirectory = %q, want canonical actions directory", cfg.ActionsDirectory)
	}
	if cfg.TranscriptsDirectory != "/var/lib/lightspeed-data-collection/transcripts" {
		t.Errorf("TranscriptsDirectory = %q, want canonical transcripts directory", cfg.TranscriptsDirectory)
	}
	if cfg.MaxBacklogBytes != 4*1024*1024 {
		t.Errorf("MaxBacklogBytes = %d, want %d", cfg.MaxBacklogBytes, 4*1024*1024)
	}
}

func TestFixedBatchLimits(t *testing.T) {
	if maxFileBytes != 1024*1024 {
		t.Errorf("maxFileBytes = %d, want %d", maxFileBytes, 1024*1024)
	}
	if maxBatchAge != 30*time.Second {
		t.Errorf("maxBatchAge = %s, want %s", maxBatchAge, 30*time.Second)
	}
}

func TestAssessConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want configAssessment
	}{
		{
			name: "defaults",
			cfg:  createDefaultConfig().(*Config),
			want: configAssessment{enabled: true, reason: configOK},
		},
		{
			name: "valid explicit config",
			cfg: &Config{
				ActionsDirectory:     "/srv/agentic/actions",
				TranscriptsDirectory: "/srv/agentic/transcripts",
				MaxBacklogBytes:      8192,
			},
			want: configAssessment{enabled: true, reason: configOK},
		},
		{
			name: "nil config",
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "empty actions directory",
			cfg: &Config{
				TranscriptsDirectory: "/tmp/transcripts",
				MaxBacklogBytes:      1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "empty transcripts directory",
			cfg: &Config{
				ActionsDirectory: "/tmp/actions",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "relative actions directory",
			cfg: &Config{
				ActionsDirectory:     "actions",
				TranscriptsDirectory: "/tmp/transcripts",
				MaxBacklogBytes:      1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "relative transcripts directory",
			cfg: &Config{
				ActionsDirectory:     "/tmp/actions",
				TranscriptsDirectory: "transcripts",
				MaxBacklogBytes:      1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "identical directories",
			cfg: &Config{
				ActionsDirectory:     "/tmp/agentic",
				TranscriptsDirectory: "/tmp/agentic",
				MaxBacklogBytes:      1,
			},
			want: configAssessment{reason: configIdenticalDirectories},
		},
		{
			name: "directories identical after cleaning",
			cfg: &Config{
				ActionsDirectory:     "/tmp/agentic/actions/..",
				TranscriptsDirectory: "/tmp/agentic",
				MaxBacklogBytes:      1,
			},
			want: configAssessment{reason: configIdenticalDirectories},
		},
		{
			name: "zero backlog",
			cfg: &Config{
				ActionsDirectory:     "/tmp/actions",
				TranscriptsDirectory: "/tmp/transcripts",
			},
			want: configAssessment{reason: configInvalidBacklog},
		},
		{
			name: "negative backlog",
			cfg: &Config{
				ActionsDirectory:     "/tmp/actions",
				TranscriptsDirectory: "/tmp/transcripts",
				MaxBacklogBytes:      -1,
			},
			want: configAssessment{reason: configInvalidBacklog},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := assessConfig(tt.cfg); got != tt.want {
				t.Fatalf("assessConfig() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestValidateIsNonFatal(t *testing.T) {
	invalid := []*Config{
		{},
		{ActionsDirectory: "relative", TranscriptsDirectory: "/tmp/transcripts", MaxBacklogBytes: 1},
		{ActionsDirectory: "/tmp/same", TranscriptsDirectory: "/tmp/same", MaxBacklogBytes: 1},
		{ActionsDirectory: "/tmp/actions", TranscriptsDirectory: "/tmp/transcripts", MaxBacklogBytes: 0},
	}

	for i, cfg := range invalid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("invalid config %d: Validate() returned startup-fatal error: %v", i, err)
		}
	}
}
