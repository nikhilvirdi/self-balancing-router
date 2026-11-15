package config

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
)

type LogFormat string

const (
	LogJSON LogFormat = "json"
	LogText LogFormat = "text"
)

type Config struct {
	LogLevel              slog.Level
	LogFormat             LogFormat
	MetricsAddr           string
	DiagnosticSampleEvery uint64
}

func Load(lookupEnv func(string) (string, bool)) (Config, error) {
	cfg := Config{LogLevel: slog.LevelInfo, LogFormat: LogJSON}
	if value, ok := lookupEnv("SBR_LOG_LEVEL"); ok {
		switch value {
		case "debug":
			cfg.LogLevel = slog.LevelDebug
		case "info":
			cfg.LogLevel = slog.LevelInfo
		case "warn":
			cfg.LogLevel = slog.LevelWarn
		case "error":
			cfg.LogLevel = slog.LevelError
		default:
			return Config{}, fmt.Errorf("invalid SBR_LOG_LEVEL %q, expected debug, info, warn, or error", value)
		}
	}
	if value, ok := lookupEnv("SBR_LOG_FORMAT"); ok {
		switch LogFormat(value) {
		case LogJSON, LogText:
			cfg.LogFormat = LogFormat(value)
		default:
			return Config{}, fmt.Errorf("invalid SBR_LOG_FORMAT %q, expected json or text", value)
		}
	}
	if value, ok := lookupEnv("SBR_METRICS_ADDR"); ok && value != "" {
		_, port, err := net.SplitHostPort(value)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_METRICS_ADDR: expected TCP host:port")
		}
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return Config{}, fmt.Errorf("invalid SBR_METRICS_ADDR: expected numeric TCP port")
		}
		cfg.MetricsAddr = value
	}
	if value, ok := lookupEnv("SBR_DIAGNOSTIC_SAMPLE_EVERY"); ok {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SBR_DIAGNOSTIC_SAMPLE_EVERY: expected a nonnegative integer")
		}
		cfg.DiagnosticSampleEvery = n
	}
	return cfg, nil
}
