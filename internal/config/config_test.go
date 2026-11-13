package config

import (
	"log/slog"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{name: "defaults", want: Config{LogLevel: slog.LevelInfo, LogFormat: LogJSON}},
		{
			name: "debug text",
			env:  map[string]string{"SBR_LOG_LEVEL": "debug", "SBR_LOG_FORMAT": "text"},
			want: Config{LogLevel: slog.LevelDebug, LogFormat: LogText},
		},
		{
			name: "explicit defaults",
			env:  map[string]string{"SBR_LOG_LEVEL": "info", "SBR_LOG_FORMAT": "json"},
			want: Config{LogLevel: slog.LevelInfo, LogFormat: LogJSON},
		},
		{
			name: "warn",
			env:  map[string]string{"SBR_LOG_LEVEL": "warn"},
			want: Config{LogLevel: slog.LevelWarn, LogFormat: LogJSON},
		},
		{
			name: "error",
			env:  map[string]string{"SBR_LOG_LEVEL": "error"},
			want: Config{LogLevel: slog.LevelError, LogFormat: LogJSON},
		},
		{
			name:    "invalid level",
			env:     map[string]string{"SBR_LOG_LEVEL": "trace"},
			wantErr: `SBR_LOG_LEVEL "trace"`,
		},
		{
			name:    "empty level",
			env:     map[string]string{"SBR_LOG_LEVEL": ""},
			wantErr: `SBR_LOG_LEVEL ""`,
		},
		{
			name:    "invalid format",
			env:     map[string]string{"SBR_LOG_FORMAT": "xml"},
			wantErr: `SBR_LOG_FORMAT "xml"`,
		},
		{
			name:    "empty format",
			env:     map[string]string{"SBR_LOG_FORMAT": ""},
			wantErr: `SBR_LOG_FORMAT ""`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(func(key string) (string, bool) {
				value, ok := tt.env[key]
				return value, ok
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load() error = %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("Load() = %+v, %v, want %+v and no error", got, err, tt.want)
			}
		})
	}
}
