package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
	"github.com/nikhilvirdi/self-balancing-router/internal/logging"
	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

func Run(ctx context.Context, cfg config.ProxyConfig, logger *slog.Logger) error {
	registry, err := NewRegistry(cfg.Backends)
	if err != nil {
		return fmt.Errorf("initialize backend registry: %w", err)
	}
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(cfg, backends, tracker)
	if err != nil {
		return err
	}
	logger.Info("backend registry initialized", "backend_count", len(backends))
	transport := newTransport(cfg.DialTimeout, cfg.ResponseHeaderTimeout)
	defer transport.CloseIdleConnections()
	upstreams, err := newUpstreams(backends, transport, logger)
	if err != nil {
		return err
	}

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", cfg.ListenAddr, err)
	}
	defer func() { _ = listener.Close() }()
	var events *telemetry.Events
	if cfg.DiagnosticSampleEvery != 0 {
		events = telemetry.NewEvents(logging.New(os.Stdout, config.Config{LogFormat: config.LogJSON}, "proxy"), cfg.DiagnosticSampleEvery, "proxy")
		defer events.Close()
	}
	var metrics *proxyMetrics
	if cfg.MetricsAddr != "" {
		metrics = newProxyMetrics(backends)
		stop, err := telemetry.StartMetrics(ctx, cfg.MetricsAddr, logger, func(w io.Writer) { metrics.render(w, registry, tracker, events) })
		if err != nil {
			return err
		}
		defer stop()
	}

	server := &http.Server{
		Handler:                      instrumentedHandler(registry, policy, upstreams, tracker, nil, logger, metrics, events, cfg.BanditTrace),
		ReadHeaderTimeout:            5 * time.Second,
		IdleTimeout:                  60 * time.Second,
		MaxHeaderBytes:               32 << 10,
		DisableGeneralOptionsHandler: true,
		ErrorLog:                     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	logger.Info("routing policy initialized", "policy", cfg.Policy)
	stopHealthChecks := startHealthChecks(ctx, registry, cfg.HealthCheck, logger)
	defer stopHealthChecks()
	return serve(ctx, server, listener, cfg.ShutdownTimeout, logger)
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, timeout time.Duration, logger *slog.Logger) error {
	logger = logger.With("address", listener.Addr().String())
	logger.Info("HTTP server listening")
	served := make(chan error, 1)
	go func() {
		served <- server.Serve(listener)
	}()

	select {
	case err := <-served:
		return errors.Join(fmt.Errorf("serve HTTP: %w", err), server.Close())
	case <-ctx.Done():
	}

	logger.Info("HTTP server shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(fmt.Errorf("shut down HTTP server: %w", shutdownErr), server.Close())
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return errors.Join(shutdownErr, fmt.Errorf("serve HTTP: %w", err))
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("HTTP server stopped")
	return nil
}
