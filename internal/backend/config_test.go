package backend

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestLoadConfigDefaults(t *testing.T) {
	want := Config{
		Config:          config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:              "backend",
		ListenAddr:      ":8080",
		ShutdownTimeout: 15 * time.Second,
	}

	got, err := LoadConfig(func(string) (string, bool) { return "", false })
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", got, err, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	env := map[string]string{
		"SBR_BACKEND_ID":               "backend-a",
		"SBR_BACKEND_LISTEN_ADDR":      "[::1]:0",
		"SBR_BACKEND_SHUTDOWN_TIMEOUT": "250ms",
		"SBR_LOG_LEVEL":                "debug",
		"SBR_LOG_FORMAT":               "text",
	}

	want := Config{
		Config:          config.Config{LogLevel: slog.LevelDebug, LogFormat: config.LogText},
		ID:              "backend-a",
		ListenAddr:      "[::1]:0",
		ShutdownTimeout: 250 * time.Millisecond,
	}

	got, err := LoadConfig(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", got, err, want)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{"SBR_BACKEND_ID", ""},
		{"SBR_BACKEND_ID", "Backend"},
		{"SBR_BACKEND_ID", "1backend"},
		{"SBR_BACKEND_ID", "backend_a"},
		{"SBR_BACKEND_ID", strings.Repeat("a", 64)},
		{"SBR_BACKEND_ID", "-backend"},
		{"SBR_BACKEND_ID", "backend "},
		{"SBR_BACKEND_LISTEN_ADDR", ""},
		{"SBR_BACKEND_LISTEN_ADDR", "localhost"},
		{"SBR_BACKEND_LISTEN_ADDR", "::1:8080"},
		{"SBR_BACKEND_LISTEN_ADDR", "bad host:80"},
		{"SBR_BACKEND_LISTEN_ADDR", "localhost:"},
		{"SBR_BACKEND_LISTEN_ADDR", ":http"},
		{"SBR_BACKEND_LISTEN_ADDR", ":-1"},
		{"SBR_BACKEND_LISTEN_ADDR", ":65536"},
		{"SBR_BACKEND_SHUTDOWN_TIMEOUT", ""},
		{"SBR_BACKEND_SHUTDOWN_TIMEOUT", "15"},
		{"SBR_BACKEND_SHUTDOWN_TIMEOUT", "0s"},
		{"SBR_BACKEND_SHUTDOWN_TIMEOUT", "-1s"},
		{"SBR_LOG_LEVEL", "trace"},
		{"SBR_LOG_FORMAT", "xml"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"/"+tt.value, func(t *testing.T) {
			_, err := LoadConfig(func(key string) (string, bool) {
				return tt.value, key == tt.key
			})
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("LoadConfig() error = %v, want error naming %s", err, tt.key)
			}
		})
	}
}

func TestLoadConfigIgnoresProxySettings(t *testing.T) {
	cfg, err := LoadConfig(func(key string) (string, bool) {
		return "invalid", strings.HasPrefix(key, "SBR_PROXY_")
	})
	want := Config{
		Config:          config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:              "backend",
		ListenAddr:      ":8080",
		ShutdownTimeout: 15 * time.Second,
	}
	if err != nil || cfg != want {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", cfg, err, want)
	}
}

