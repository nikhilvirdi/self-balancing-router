package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadProxy(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want ProxyConfig
	}{
		{
			name: "defaults",
			want: ProxyConfig{
				Config:                Config{LogLevel: slog.LevelInfo, LogFormat: LogJSON},
				ListenAddr:            ":8080",
				ShutdownTimeout:       15 * time.Second,
				DialTimeout:           3 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
			},
		},
		{
			name: "custom settings",
			env: map[string]string{
				"SBR_PROXY_LISTEN_ADDR":             "[::1]:0",
				"SBR_PROXY_SHUTDOWN_TIMEOUT":        "250ms",
				"SBR_PROXY_DIAL_TIMEOUT":            "750ms",
				"SBR_PROXY_RESPONSE_HEADER_TIMEOUT": "0",
				"SBR_LOG_LEVEL":                     "debug",
				"SBR_LOG_FORMAT":                    "text",
			},
			want: ProxyConfig{
				Config:                Config{LogLevel: slog.LevelDebug, LogFormat: LogText},
				ListenAddr:            "[::1]:0",
				ShutdownTimeout:       250 * time.Millisecond,
				DialTimeout:           750 * time.Millisecond,
				ResponseHeaderTimeout: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadProxy(func(key string) (string, bool) {
				value, ok := tt.env[key]
				return value, ok
			})
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("LoadProxy() = %+v, %v, want %+v", got, err, tt.want)
			}
		})
	}
}

func TestLoadProxyInvalid(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{"SBR_PROXY_LISTEN_ADDR", ""},
		{"SBR_PROXY_LISTEN_ADDR", "localhost"},
		{"SBR_PROXY_LISTEN_ADDR", "::1:8080"},
		{"SBR_PROXY_LISTEN_ADDR", "bad host:80"},
		{"SBR_PROXY_LISTEN_ADDR", "localhost:"},
		{"SBR_PROXY_LISTEN_ADDR", ":http"},
		{"SBR_PROXY_LISTEN_ADDR", ":-1"},
		{"SBR_PROXY_LISTEN_ADDR", ":65536"},
		{"SBR_PROXY_SHUTDOWN_TIMEOUT", ""},
		{"SBR_PROXY_SHUTDOWN_TIMEOUT", "15"},
		{"SBR_PROXY_SHUTDOWN_TIMEOUT", "0s"},
		{"SBR_PROXY_SHUTDOWN_TIMEOUT", "-1s"},
		{"SBR_PROXY_DIAL_TIMEOUT", ""},
		{"SBR_PROXY_DIAL_TIMEOUT", "3"},
		{"SBR_PROXY_DIAL_TIMEOUT", "0"},
		{"SBR_PROXY_DIAL_TIMEOUT", "-1s"},
		{"SBR_PROXY_RESPONSE_HEADER_TIMEOUT", ""},
		{"SBR_PROXY_RESPONSE_HEADER_TIMEOUT", "30"},
		{"SBR_PROXY_RESPONSE_HEADER_TIMEOUT", "-1s"},
		{"SBR_LOG_LEVEL", "trace"},
		{"SBR_LOG_FORMAT", "xml"},
	}
	for _, tt := range tests {
		t.Run(tt.key+"/"+tt.value, func(t *testing.T) {
			_, err := LoadProxy(func(key string) (string, bool) {
				return tt.value, key == tt.key
			})
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("LoadProxy() error = %v, want error naming %s", err, tt.key)
			}
		})
	}
}

func TestLoadIgnoresProxySettings(t *testing.T) {
	cfg, err := Load(func(key string) (string, bool) {
		return "invalid", strings.HasPrefix(key, "SBR_PROXY_")
	})
	want := Config{LogLevel: slog.LevelInfo, LogFormat: LogJSON}
	if err != nil || cfg != want {
		t.Fatalf("Load() = %+v, %v, want %+v", cfg, err, want)
	}
}
