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
				Directory:       "/srv/agentic/traces",
				MaxBacklogBytes: 8192,
			},
			want: configAssessment{enabled: true, reason: configOK},
		},
		{name: "nil config", want: configAssessment{reason: configInvalidDirectory}},
		{
			name: "empty directory",
			cfg: &Config{
				MaxBacklogBytes: 1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "relative directory",
			cfg: &Config{
				Directory:       "traces",
				MaxBacklogBytes: 1,
			},
			want: configAssessment{reason: configInvalidDirectory},
		},
		{
			name: "zero backlog",
			cfg: &Config{
				Directory: "/tmp/traces",
			},
			want: configAssessment{reason: configInvalidBacklog},
		},
		{
			name: "negative backlog",
			cfg: &Config{
				Directory:       "/tmp/traces",
				MaxBacklogBytes: -1,
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
		{Directory: "relative", MaxBacklogBytes: 1},
		{Directory: "/tmp/traces", MaxBacklogBytes: 0},
	}

	for i, cfg := range invalid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("invalid config %d: Validate() returned startup-fatal error: %v", i, err)
		}
	}
}
