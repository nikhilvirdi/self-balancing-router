package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

func StartMetrics(ctx context.Context, addr string, logger *slog.Logger, render func(io.Writer)) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		render(w)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8 << 10}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics listener failed", "error", err)
		}
	}()
	logger.Info("metrics listener started", "address", listener.Addr().String())
	return func() { _ = server.Close(); <-done }, nil
}

var bounds = [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}

type Histogram struct {
	mu      sync.Mutex
	buckets [len(bounds) + 1]uint64
	seconds float64
}

func (h *Histogram) Observe(duration time.Duration) {
	index := 0
	for index < len(bounds) && duration > bounds[index] {
		index++
	}
	h.mu.Lock()
	h.buckets[index]++
	h.seconds += duration.Seconds()
	h.mu.Unlock()
}

func (h *Histogram) Write(w io.Writer, name string) {
	h.mu.Lock()
	buckets, sum := h.buckets, h.seconds
	h.mu.Unlock()
	_, _ = fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	var count uint64
	for i, bound := range bounds {
		count += buckets[i]
		_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, bound.Seconds(), count)
	}
	count += buckets[len(bounds)]
	_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", name, count, name, sum, name, count)
}

func WriteDrops(w io.Writer, events *Events) {
	if events != nil {
		_, _ = fmt.Fprintf(w, "# TYPE sbr_diagnostic_dropped_events_total counter\nsbr_diagnostic_dropped_events_total %d\n", events.Dropped())
	}
}
