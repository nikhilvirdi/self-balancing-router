package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestNewJSON(t *testing.T) {
	var output bytes.Buffer
	logger := New(&output, config.Config{LogLevel: slog.LevelWarn, LogFormat: config.LogJSON}, "proxy")
	logger.Info("filtered")
	if output.Len() != 0 {
		t.Fatalf("logger emitted a message below the configured level: %s", output.String())
	}
	logger.Warn("warning")
	var record struct {
		Level     string `json:"level"`
		Message   string `json:"msg"`
		Component string `json:"component"`
	}
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode log: %v", err)
	}
	if record.Level != "WARN" || record.Message != "warning" || record.Component != "proxy" {
		t.Fatalf("unexpected log record: %+v", record)
	}
}

func TestNewText(t *testing.T) {
	var output bytes.Buffer
	logger := New(&output, config.Config{LogLevel: slog.LevelDebug, LogFormat: config.LogText}, "backend")
	logger.Debug("ready")
	for _, field := range []string{"level=DEBUG", "msg=ready", "component=backend"} {
		if !strings.Contains(output.String(), field) {
			t.Errorf("text log %q missing %q", output.String(), field)
		}
	}
}
