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
		Config:           config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:               "backend",
		ListenAddr:       ":8080",
		ShutdownTimeout:  15 * time.Second,
		Workers:          4,
		QueueCapacity:    32,
		ProcessingDelay:  0,
		ProcessingJitter: 0,
		DelaySeed:        1,
	}

	got, err := LoadConfig(func(string) (string, bool) { return "", false })
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", got, err, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	env := map[string]string{
		"SBR_BACKEND_ID":                "backend-a",
		"SBR_BACKEND_LISTEN_ADDR":       "[::1]:0",
		"SBR_BACKEND_SHUTDOWN_TIMEOUT":  "250ms",
		"SBR_BACKEND_WORKERS":           "8",
		"SBR_BACKEND_QUEUE_CAPACITY":    "64",
		"SBR_BACKEND_PROCESSING_DELAY":  "40ms",
		"SBR_BACKEND_PROCESSING_JITTER": "20ms",
		"SBR_BACKEND_DELAY_SEED":        "42",
		"SBR_LOG_LEVEL":                 "debug",
		"SBR_LOG_FORMAT":                "text",
	}

	want := Config{
		Config:           config.Config{LogLevel: slog.LevelDebug, LogFormat: config.LogText},
		ID:               "backend-a",
		ListenAddr:       "[::1]:0",
		ShutdownTimeout:  250 * time.Millisecond,
		Workers:          8,
		QueueCapacity:    64,
		ProcessingDelay:  40 * time.Millisecond,
		ProcessingJitter: 20 * time.Millisecond,
		DelaySeed:        42,
	}

	got, err := LoadConfig(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", got, err, want)
	}
}

func TestLoadConfigZeroQueueCapacity(t *testing.T) {
	got, err := LoadConfig(func(key string) (string, bool) {
		if key == "SBR_BACKEND_QUEUE_CAPACITY" {
			return "0", true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if got.QueueCapacity != 0 {
		t.Fatalf("QueueCapacity = %d, want 0", got.QueueCapacity)
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
		{"SBR_BACKEND_WORKERS", ""},
		{"SBR_BACKEND_WORKERS", "0"},
		{"SBR_BACKEND_WORKERS", "-1"},
		{"SBR_BACKEND_WORKERS", "1025"},
		{"SBR_BACKEND_WORKERS", "four"},
		{"SBR_BACKEND_WORKERS", " 4 "},
		{"SBR_BACKEND_QUEUE_CAPACITY", ""},
		{"SBR_BACKEND_QUEUE_CAPACITY", "-1"},
		{"SBR_BACKEND_QUEUE_CAPACITY", "65537"},
		{"SBR_BACKEND_QUEUE_CAPACITY", "thirty-two"},
		{"SBR_BACKEND_QUEUE_CAPACITY", " 0 "},
		{"SBR_LOG_LEVEL", "trace"},
		{"SBR_LOG_FORMAT", "xml"},
		{"SBR_BACKEND_PROCESSING_DELAY", ""},
		{"SBR_BACKEND_PROCESSING_DELAY", "-1ms"},
		{"SBR_BACKEND_PROCESSING_DELAY", "5m1s"},
		{"SBR_BACKEND_PROCESSING_DELAY", "invalid"},
		{"SBR_BACKEND_PROCESSING_JITTER", ""},
		{"SBR_BACKEND_PROCESSING_JITTER", "-1ms"},
		{"SBR_BACKEND_PROCESSING_JITTER", "5m1s"},
		{"SBR_BACKEND_PROCESSING_JITTER", "invalid"},
		{"SBR_BACKEND_DELAY_SEED", ""},
		{"SBR_BACKEND_DELAY_SEED", "invalid"},
		{"SBR_BACKEND_DELAY_SEED", "1.5"},
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

func TestLoadConfigDelayVariations(t *testing.T) {
	tests := []struct {
		name       string
		env        map[string]string
		wantDelay  time.Duration
		wantJitter time.Duration
		wantSeed   int64
	}{
		{
			name: "zero delay nonzero jitter",
			env: map[string]string{
				"SBR_BACKEND_PROCESSING_DELAY":  "0s",
				"SBR_BACKEND_PROCESSING_JITTER": "50ms",
			},
			wantDelay:  0,
			wantJitter: 50 * time.Millisecond,
			wantSeed:   1,
		},
		{
			name: "nonzero delay zero jitter",
			env: map[string]string{
				"SBR_BACKEND_PROCESSING_DELAY":  "100ms",
				"SBR_BACKEND_PROCESSING_JITTER": "0s",
			},
			wantDelay:  100 * time.Millisecond,
			wantJitter: 0,
			wantSeed:   1,
		},
		{
			name: "max individual durations",
			env: map[string]string{
				"SBR_BACKEND_PROCESSING_DELAY":  "5m",
				"SBR_BACKEND_PROCESSING_JITTER": "5m",
			},
			wantDelay:  5 * time.Minute,
			wantJitter: 5 * time.Minute,
			wantSeed:   1,
		},
		{
			name: "seed variations",
			env: map[string]string{
				"SBR_BACKEND_DELAY_SEED": "0",
			},
			wantDelay:  0,
			wantJitter: 0,
			wantSeed:   0,
		},
		{
			name: "negative seed",
			env: map[string]string{
				"SBR_BACKEND_DELAY_SEED": "-42",
			},
			wantDelay:  0,
			wantJitter: 0,
			wantSeed:   -42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(func(key string) (string, bool) {
				val, ok := tt.env[key]
				return val, ok
			})
			if err != nil {
				t.Fatalf("LoadConfig() unexpected error: %v", err)
			}
			if cfg.ProcessingDelay != tt.wantDelay {
				t.Errorf("ProcessingDelay = %v, want %v", cfg.ProcessingDelay, tt.wantDelay)
			}
			if cfg.ProcessingJitter != tt.wantJitter {
				t.Errorf("ProcessingJitter = %v, want %v", cfg.ProcessingJitter, tt.wantJitter)
			}
			if cfg.DelaySeed != tt.wantSeed {
				t.Errorf("DelaySeed = %v, want %v", cfg.DelaySeed, tt.wantSeed)
			}
		})
	}
}

func TestLoadConfigIgnoresProxySettings(t *testing.T) {
	cfg, err := LoadConfig(func(key string) (string, bool) {
		return "invalid", strings.HasPrefix(key, "SBR_PROXY_")
	})
	want := Config{
		Config:           config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:               "backend",
		ListenAddr:       ":8080",
		ShutdownTimeout:  15 * time.Second,
		Workers:          4,
		QueueCapacity:    32,
		ProcessingDelay:  0,
		ProcessingJitter: 0,
		DelaySeed:        1,
	}
	if err != nil || cfg != want {
		t.Fatalf("LoadConfig() = %+v, %v, want %+v", cfg, err, want)
	}
}
