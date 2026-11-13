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

	var adminListener net.Listener
	if cfg.AdminEnabled {
		var adminErr error
		adminListener, adminErr = listenConfig.Listen(ctx, "tcp", cfg.AdminListenAddr)
		if adminErr != nil {
			_ = listener.Close()
			return fmt.Errorf("listen admin on %q: %w", cfg.AdminListenAddr, adminErr)
		}
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

	var adminServer *http.Server
	if cfg.AdminEnabled {
		adminServer = &http.Server{
			Handler:                      AdminHandler(delayModel, cfg.AdminToken),
			ReadHeaderTimeout:            5 * time.Second,
			IdleTimeout:                  60 * time.Second,
			MaxHeaderBytes:               32 << 10,
			DisableGeneralOptionsHandler: true,
			ErrorLog:                     slog.NewLogLogger(logger.Handler(), slog.LevelError),
		}
	}

	logAttrs := []any{
		"backend_id", cfg.ID,
		"processing_delay", cfg.ProcessingDelay,
		"processing_jitter", cfg.ProcessingJitter,
		"delay_seed", cfg.DelaySeed,
		"admin_enabled", cfg.AdminEnabled,
	}
	if cfg.AdminEnabled {
		logAttrs = append(logAttrs, "admin_address", adminListener.Addr().String())
	}

	return serveServers(ctx, server, listener, adminServer, adminListener, pool, cfg.ShutdownTimeout, logger.With(logAttrs...))
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, pool *Pool, timeout time.Duration, logger *slog.Logger) error {
	return serveServers(ctx, server, listener, nil, nil, pool, timeout, logger)
}

func serveServers(ctx context.Context, server *http.Server, listener net.Listener, adminServer *http.Server, adminListener net.Listener, pool *Pool, timeout time.Duration, logger *slog.Logger) error {
	logger = logger.With("address", listener.Addr().String(), "workers", pool.Workers(), "queue_capacity", pool.QueueCapacity())
	logger.Info("backend server listening")
	served := make(chan error, 1)
	go func() {
		served <- server.Serve(listener)
	}()

	var adminServed chan error
	if adminServer != nil && adminListener != nil {
		adminServed = make(chan error, 1)
		go func() {
			adminServed <- adminServer.Serve(adminListener)
		}()
	}

	select {
	case err := <-served:
		pool.Close()
		if adminServer != nil {
			_ = adminServer.Close()
		}
		return errors.Join(fmt.Errorf("serve HTTP: %w", err), server.Close())
	case err := <-adminServed:
		pool.Close()
		_ = server.Close()
		return errors.Join(fmt.Errorf("serve admin HTTP: %w", err), adminServer.Close())
	case <-ctx.Done():
	}

	logger.Info("backend server shutdown started")
	pool.StopAccepting()

	var adminShutdownErr error
	if adminServer != nil {
		adminShutdownCtx, cancelAdmin := context.WithTimeout(context.Background(), timeout)
		adminShutdownErr = adminServer.Shutdown(adminShutdownCtx)
		cancelAdmin()
		if adminShutdownErr != nil {
			adminShutdownErr = errors.Join(fmt.Errorf("shut down admin HTTP server: %w", adminShutdownErr), adminServer.Close())
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(fmt.Errorf("shut down HTTP server: %w", shutdownErr), server.Close())
		pool.CancelQueue(errors.New("shutdown deadline exceeded"))
	}
	pool.Close()

	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("serve HTTP: %w", err))
	}
	if adminServed != nil {
		if err := <-adminServed; !errors.Is(err, http.ErrServerClosed) {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("serve admin HTTP: %w", err))
		}
		if adminShutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, adminShutdownErr)
		}
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("backend server stopped")
	return nil
}
