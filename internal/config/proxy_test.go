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
				Policy:                PolicyFirstEligible,
				P2CSeed:               1,
				PeakEWMADecay:         10 * time.Second,
				PeakEWMAInitialRTT:    10 * time.Millisecond,
				HealthCheck: HealthCheckConfig{
					Enabled: true, Interval: 5 * time.Second, Timeout: 2 * time.Second,
					FailureThreshold: 2, RecoveryThreshold: 2,
				},
			},
		},
		{
			name: "custom settings",
			env: map[string]string{
				"SBR_PROXY_LISTEN_ADDR":               "[::1]:0",
				"SBR_PROXY_SHUTDOWN_TIMEOUT":          "250ms",
				"SBR_PROXY_DIAL_TIMEOUT":              "750ms",
				"SBR_PROXY_RESPONSE_HEADER_TIMEOUT":   "0",
				"SBR_PROXY_POLICY":                    " p2c-peak-ewma ",
				"SBR_PROXY_P2C_SEED":                  " -42 ",
				"SBR_PROXY_PEAK_EWMA_DECAY":           "24h",
				"SBR_PROXY_PEAK_EWMA_INITIAL_RTT":     "1h",
				"SBR_LOG_LEVEL":                       "debug",
				"SBR_LOG_FORMAT":                      "text",
				"SBR_PROXY_HEALTH_CHECK_ENABLED":      "false",
				"SBR_PROXY_HEALTH_CHECK_INTERVAL":     "150ms",
				"SBR_PROXY_HEALTH_CHECK_TIMEOUT":      "40ms",
				"SBR_PROXY_HEALTH_FAILURE_THRESHOLD":  "3",
				"SBR_PROXY_HEALTH_RECOVERY_THRESHOLD": "4",
			},
			want: ProxyConfig{
				Config:                Config{LogLevel: slog.LevelDebug, LogFormat: LogText},
				ListenAddr:            "[::1]:0",
				ShutdownTimeout:       250 * time.Millisecond,
				DialTimeout:           750 * time.Millisecond,
				ResponseHeaderTimeout: 0,
				Policy:                PolicyP2CPeakEWMA,
				P2CSeed:               -42,
				PeakEWMADecay:         24 * time.Hour,
				PeakEWMAInitialRTT:    time.Hour,
				HealthCheck: HealthCheckConfig{
					Enabled: false, Interval: 150 * time.Millisecond, Timeout: 40 * time.Millisecond,
					FailureThreshold: 3, RecoveryThreshold: 4,
				},
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
		{"SBR_PROXY_POLICY", ""},
		{"SBR_PROXY_POLICY", "  "},
		{"SBR_PROXY_POLICY", "ROUND-ROBIN"},
		{"SBR_PROXY_POLICY", "round_robin"},
		{"SBR_PROXY_POLICY", "least-connections"},
		{"SBR_PROXY_POLICY", "lr"},
		{"SBR_PROXY_P2C_SEED", ""},
		{"SBR_PROXY_P2C_SEED", "1.5"},
		{"SBR_PROXY_P2C_SEED", "9223372036854775808"},
		{"SBR_PROXY_P2C_SEED", "-9223372036854775809"},
		{"SBR_PROXY_PEAK_EWMA_DECAY", "10"},
		{"SBR_PROXY_PEAK_EWMA_DECAY", "0s"},
		{"SBR_PROXY_PEAK_EWMA_DECAY", "-1s"},
		{"SBR_PROXY_PEAK_EWMA_DECAY", "24h1ns"},
		{"SBR_PROXY_PEAK_EWMA_INITIAL_RTT", "10"},
		{"SBR_PROXY_PEAK_EWMA_INITIAL_RTT", "0s"},
		{"SBR_PROXY_PEAK_EWMA_INITIAL_RTT", "-1ms"},
		{"SBR_PROXY_PEAK_EWMA_INITIAL_RTT", "1h1ns"},
		{"SBR_PROXY_HEALTH_CHECK_ENABLED", "maybe"},
		{"SBR_PROXY_HEALTH_CHECK_INTERVAL", "fast"},
		{"SBR_PROXY_HEALTH_CHECK_INTERVAL", "0"},
		{"SBR_PROXY_HEALTH_CHECK_INTERVAL", "-1s"},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", "fast"},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", "0"},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", "-1s"},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", "5s"},
		{"SBR_PROXY_HEALTH_CHECK_TIMEOUT", "6s"},
		{"SBR_PROXY_HEALTH_FAILURE_THRESHOLD", "1.5"},
		{"SBR_PROXY_HEALTH_FAILURE_THRESHOLD", "0"},
		{"SBR_PROXY_HEALTH_FAILURE_THRESHOLD", "-1"},
		{"SBR_PROXY_HEALTH_RECOVERY_THRESHOLD", "many"},
		{"SBR_PROXY_HEALTH_RECOVERY_THRESHOLD", "0"},
		{"SBR_PROXY_HEALTH_RECOVERY_THRESHOLD", "-1"},
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
