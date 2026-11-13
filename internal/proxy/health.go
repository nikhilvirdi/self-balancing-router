package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func startHealthChecks(parent context.Context, registry *Registry, cfg config.HealthCheckConfig, logger *slog.Logger) func() {
	if !cfg.Enabled {
		return func() {}
	}
	backends := registry.Snapshot()
	if len(backends) == 0 {
		return func() {}
	}

	client := newHealthClient(cfg.Timeout)
	ctx, cancel := context.WithCancel(parent)
	var workers sync.WaitGroup
	for _, backend := range backends {
		workers.Go(func() {
			monitorBackend(ctx, client, registry, backend, cfg, logger)
		})
	}
	return func() {
		cancel()
		workers.Wait()
		client.CloseIdleConnections()
	}
}

func newHealthClient(timeout time.Duration) *http.Client {
	transport := newTransport(timeout, 0)
	transport.MaxIdleConnsPerHost = 2
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func monitorBackend(ctx context.Context, client *http.Client, registry *Registry, backend Backend, cfg config.HealthCheckConfig, logger *slog.Logger) {
	// Validated backend URLs have only an authority and an optional root slash.
	address := strings.TrimSuffix(backend.URL, "/") + "/healthz"
	logger = logger.With("backend_id", backend.ID)
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	var streak probeStreak
	for ctx.Err() == nil {
		status, err := probeHealth(ctx, client, address)
		if ctx.Err() != nil {
			return
		}
		next := streak.observe(err == nil && status >= http.StatusOK && status < http.StatusMultipleChoices, cfg)
		if next != Unknown {
			updateHealth(registry, backend.ID, next, status, err, logger)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func updateHealth(registry *Registry, id string, next Health, status int, probeErr error, logger *slog.Logger) {
	current, _ := registry.Lookup(id)
	if current.Health == next {
		return
	}
	if err := registry.SetHealth(id, next); err != nil {
		logger.Error("backend health update failed", "error", err)
		return
	}
	reason := "http_status"
	if probeErr != nil {
		reason = "transport_error"
		var networkErr net.Error
		if errors.As(probeErr, &networkErr) && networkErr.Timeout() {
			reason = "timeout"
		}
	}
	logger.Info("backend health changed", "previous", current.Health.String(), "health", next.String(),
		"reason", reason, "status", status)
}

func probeHealth(ctx context.Context, client *http.Client, address string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return 0, err
	}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	// Drain small, known-length bodies for reuse. The client deadline also covers this read.
	if res.ContentLength > 0 && res.ContentLength <= 4096 {
		_, err = io.CopyN(io.Discard, res.Body, res.ContentLength)
	}
	return res.StatusCode, errors.Join(err, res.Body.Close())
}

type probeStreak struct {
	target Health
	count  int
}

func (s *probeStreak) observe(success bool, cfg config.HealthCheckConfig) Health {
	target, threshold := Unhealthy, cfg.FailureThreshold
	if success {
		target, threshold = Healthy, cfg.RecoveryThreshold
	}
	if s.target != target {
		s.target, s.count = target, 0
	}
	if s.count < threshold {
		s.count++
	}
	if s.count == threshold {
		return target
	}
	return Unknown
}
