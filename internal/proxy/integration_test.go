package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/backend"
)

func TestProxyBackendIntegration(t *testing.T) {
	const concurrent = 8
	entered := make(chan struct{}, concurrent)
	release, cancelEntered, cancelDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstDown, secondDown atomic.Bool
	firstPool := backend.NewPool(4, concurrent, nil)
	t.Cleanup(firstPool.Close)
	firstHandler := backend.Handler("backend-a", firstPool)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			if firstDown.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		case "/hold":
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		case "/cancel":
			close(cancelEntered)
			<-r.Context().Done()
			close(cancelDone)
			return
		}
		firstHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(first.Close)
	secondPool := backend.NewPool(1, 0, nil)
	t.Cleanup(secondPool.Close)
	secondHandler := backend.Handler("backend-b", secondPool)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && secondDown.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		secondHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(second.Close)

	observations := make(chan Observation, concurrent+8)
	registry, tracker, proxy := startObservedProxy(t, observations, testTimeout, first.URL, second.URL)
	cfg := healthTestConfig()
	cfg.Interval, cfg.Timeout = 100*time.Millisecond, 75*time.Millisecond
	cfg.FailureThreshold, cfg.RecoveryThreshold = 2, 2
	events, logger := healthLogs()
	stop := startHealthChecks(t.Context(), registry, cfg, logger)
	defer stop()
	waitFor(t, events)
	waitFor(t, events)
	assertHealth(t, registry, "backend-a", Healthy)
	assertHealth(t, registry, "backend-b", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	assertNoObservation(t, observations)

	cancelCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := startObservedRequest(t, cancelCtx, proxy, "/cancel")
	waitFor(t, cancelEntered)
	assertInFlight(t, tracker, "backend-a", 1)
	cancel()
	if got := waitFor(t, canceled); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled request = %+v", got)
	}
	waitFor(t, cancelDone)
	assertObservation(t, observations, "backend-a", 0, Canceled)
	assertInFlight(t, tracker, "backend-a", 0)
	assertHealth(t, registry, "backend-a", Healthy)

	results := make([]<-chan requestResult, concurrent)
	for i := range results {
		results[i] = startObservedRequest(t, t.Context(), proxy, "/hold")
	}
	for range concurrent {
		waitFor(t, entered)
	}
	assertInFlight(t, tracker, "backend-a", concurrent)
	assertInFlight(t, tracker, "backend-b", 0)
	firstDown.Store(true)
	assertHealthEvent(t, events, "backend-a", "unhealthy")
	assertResponse(t, proxy, http.MethodPost, "/api/items?limit=10", 200,
		`{"backend_id":"backend-b","method":"POST","path":"/api/items"}`+"\n")
	assertObservation(t, observations, "backend-b", 200, UpstreamResponse)
	assertInFlight(t, tracker, "backend-a", concurrent)

	secondPool.StopAccepting()
	assertResponse(t, proxy, http.MethodGet, "/api/items", 503, `{"error":"backend overloaded"}`+"\n")
	assertObservation(t, observations, "backend-b", 503, UpstreamResponse)
	assertInFlight(t, tracker, "backend-b", 0)
	assertHealth(t, registry, "backend-b", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")

	secondDown.Store(true)
	assertHealthEvent(t, events, "backend-b", "unhealthy")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/api/items", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	assertNoObservation(t, observations)

	close(release)
	for _, result := range results {
		if got := waitFor(t, result); got.err != nil || got.body != `{"backend_id":"backend-a","method":"GET","path":"/hold"}`+"\n" {
			t.Fatalf("assigned request = %+v, want response from backend-a", got)
		}
		assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	}
	assertInFlight(t, tracker, "backend-a", 0)
	assertInFlight(t, tracker, "backend-b", 0)

	firstDown.Store(false)
	assertHealthEvent(t, events, "backend-a", "healthy")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/api/items", 200,
		`{"backend_id":"backend-a","method":"GET","path":"/api/items"}`+"\n")
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	assertInFlight(t, tracker, "backend-a", 0)
	assertNoObservation(t, observations)
}

func assertHealthEvent(t *testing.T, events <-chan healthEvent, id, state string) {
	t.Helper()
	if got := waitFor(t, events); got.Backend != id || got.Health != state {
		t.Fatalf("health event = %+v, want %s %s", got, id, state)
	}
}
