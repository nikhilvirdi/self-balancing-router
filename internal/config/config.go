package config

import (
	"fmt"
	"log/slog"
)

type LogFormat string

const (
	LogJSON LogFormat = "json"
	LogText LogFormat = "text"
)

type Config struct {
	LogLevel  slog.Level
	LogFormat LogFormat
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
	return cfg, nil
}
