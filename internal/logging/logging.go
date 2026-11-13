package logging

import (
	"io"
	"log/slog"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func New(output io.Writer, cfg config.Config, component string) *slog.Logger {
	options := &slog.HandlerOptions{Level: cfg.LogLevel}
	var handler slog.Handler
	if cfg.LogFormat == config.LogText {
		handler = slog.NewTextHandler(output, options)
	} else {
		handler = slog.NewJSONHandler(output, options)
	}
	return slog.New(handler).With("component", component)
}
