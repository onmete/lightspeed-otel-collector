package agenticexporter

import "testing"

func TestAssessConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want configAssessment
	}{
		{
			name: "valid explicit config",
			cfg: &Config{
				Directory:        "/srv/agentic/traces",
				StagingDirectory: "/srv/agentic/staging",
				MaxBacklogBytes:  8192,
			},
			want: configAssessment{enabled: true, reason: configOK},
		},
		{
			name: "valid sibling path with common prefix",
			cfg: &Config{
				Directory:        "/srv/agentic/traces",
				StagingDirectory: "/srv/agentic/traces-staging",
				MaxBacklogBytes:  8192,
			},
			want: configAssessment{enabled: true, reason: configOK},
		},
		{name: "nil config", want: configAssessment{reason: configInvalidDirectory}},
		{
			name: "empty directory",
			cfg: &Config{
				StagingDirectory: "/tmp/staging",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "relative directory",
			cfg: &Config{
				Directory:        "traces",
				StagingDirectory: "/tmp/staging",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "empty staging directory",
			cfg: &Config{
				Directory:       "/tmp/traces",
				MaxBacklogBytes: 1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "relative staging directory",
			cfg: &Config{
				Directory:        "/tmp/traces",
				StagingDirectory: "staging",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "same cleaned directory",
			cfg: &Config{
				Directory:        "/tmp/export",
				StagingDirectory: "/tmp/export/./",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "staging nested under export directory",
			cfg: &Config{
				Directory:        "/tmp/export",
				StagingDirectory: "/tmp/export/staging",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "export directory nested under staging",
			cfg: &Config{
				Directory:        "/tmp/export/traces",
				StagingDirectory: "/tmp/export",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "cleaned paths overlap",
			cfg: &Config{
				Directory:        "/tmp/root/../traces",
				StagingDirectory: "/tmp/traces",
				MaxBacklogBytes:  1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "zero backlog",
			cfg: &Config{
				Directory:        "/tmp/traces",
				StagingDirectory: "/tmp/staging",
			},
			want: configAssessment{reason: configInvalidBacklog},
		},
		{
			name: "negative backlog",
			cfg: &Config{
				Directory:        "/tmp/traces",
				StagingDirectory: "/tmp/staging",
				MaxBacklogBytes:  -1,
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
		{Directory: "relative", StagingDirectory: "/tmp/staging", MaxBacklogBytes: 1},
		{Directory: "/tmp/traces", StagingDirectory: "relative", MaxBacklogBytes: 1},
		{Directory: "/tmp/traces", StagingDirectory: "/tmp/traces/staging", MaxBacklogBytes: 1},
		{Directory: "/tmp/traces", StagingDirectory: "/tmp/staging", MaxBacklogBytes: 0},
	}

	for i, cfg := range invalid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("invalid config %d: Validate() returned startup-fatal error: %v", i, err)
		}
	}
}
