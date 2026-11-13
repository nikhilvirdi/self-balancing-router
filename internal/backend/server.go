package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", cfg.ListenAddr, err)
	}

	delayModel := NewDelayModel(cfg.ProcessingDelay, cfg.ProcessingJitter, cfg.DelaySeed)
	pool := NewPool(cfg.Workers, cfg.QueueCapacity, makeProcessor(delayModel))
	server := &http.Server{
		Handler:                      Handler(cfg.ID, pool),
		ReadHeaderTimeout:            5 * time.Second,
		IdleTimeout:                  60 * time.Second,
		MaxHeaderBytes:               32 << 10,
		DisableGeneralOptionsHandler: true,
		ErrorLog:                     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	return serve(ctx, server, listener, pool, cfg.ShutdownTimeout, logger.With(
		"backend_id", cfg.ID,
		"processing_delay", cfg.ProcessingDelay,
		"processing_jitter", cfg.ProcessingJitter,
		"delay_seed", cfg.DelaySeed,
	))
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, pool *Pool, timeout time.Duration, logger *slog.Logger) error {
	logger = logger.With("address", listener.Addr().String(), "workers", pool.Workers(), "queue_capacity", pool.QueueCapacity())
	logger.Info("backend server listening")
	served := make(chan error, 1)
	go func() {
		served <- server.Serve(listener)
	}()

	select {
	case err := <-served:
		pool.Close()
		return errors.Join(fmt.Errorf("serve HTTP: %w", err), server.Close())
	case <-ctx.Done():
	}

	logger.Info("backend server shutdown started")
	pool.StopAccepting()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(fmt.Errorf("shut down HTTP server: %w", shutdownErr), server.Close())
		pool.CancelQueue(errors.New("shutdown deadline exceeded"))
	}
	pool.Close()

	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return errors.Join(shutdownErr, fmt.Errorf("serve HTTP: %w", err))
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("backend server stopped")
	return nil
}
