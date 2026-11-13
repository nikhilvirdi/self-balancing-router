package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func Run(ctx context.Context, cfg config.ProxyConfig, logger *slog.Logger) error {
	registry, err := NewRegistry(cfg.Backends)
	if err != nil {
		return fmt.Errorf("initialize backend registry: %w", err)
	}
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(cfg.Policy, tracker)
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

	server := &http.Server{
		Handler:                      handler(registry, policy, upstreams, tracker, nil, logger),
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
