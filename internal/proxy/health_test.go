package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestProbeStreak(t *testing.T) {
	cfg := config.HealthCheckConfig{FailureThreshold: 2, RecoveryThreshold: 3}
	for _, tt := range []struct {
		name     string
		outcomes string
		want     []Health
	}{
		{"initial success", "SSSS", []Health{Unknown, Unknown, Healthy, Healthy}},
		{"initial failure", "FFF", []Health{Unknown, Unhealthy, Unhealthy}},
		{"failure and recovery", "SSSFSFFSSS", []Health{Unknown, Unknown, Healthy, Healthy, Healthy, Healthy, Unhealthy, Unhealthy, Unhealthy, Healthy}},
		{"alternating", "SFSFSFSSFS", []Health{Unknown, Unknown, Unknown, Unknown, Unknown, Unknown, Unknown, Unknown, Unknown, Unknown}},
		{"recovery reset", "FFSSFSFSSS", []Health{Unknown, Unhealthy, Unhealthy, Unhealthy, Unhealthy, Unhealthy, Unhealthy, Unhealthy, Unhealthy, Healthy}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := Unknown
			var streak probeStreak
			for i, outcome := range tt.outcomes {
				if next := streak.observe(outcome == 'S', cfg); next != Unknown {
					state = next
				}
				if state != tt.want[i] {
					t.Fatalf("probe %d: health = %v, want %v", i+1, state, tt.want[i])
				}
			}
		})
	}
}

func TestHealthProbeHTTP(t *testing.T) {
	for _, tt := range []struct {
		status int
		want   Health
	}{
		{http.StatusOK, Healthy},
		{http.StatusAccepted, Healthy},
		{http.StatusNoContent, Healthy},
		{http.StatusFound, Unhealthy},
		{http.StatusNotFound, Unhealthy},
		{http.StatusServiceUnavailable, Unhealthy},
	} {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RequestURI() != "/healthz" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				if tt.status == http.StatusFound {
					w.Header().Set("Location", "/redirect-target")
				}
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(upstream.Close)
			registry := healthRegistry(t, upstream.URL+"/")
			events, logger := healthLogs()
			stop := startHealthChecks(t.Context(), registry, healthTestConfig(), logger)
			defer stop()
			event := waitFor(t, events)
			assertHealth(t, registry, "backend-a", tt.want)
			if event.Backend != "backend-a" || event.Previous != "unknown" || event.Health != tt.want.String() ||
				event.Reason != "http_status" || event.Status != tt.status {
				t.Fatalf("unexpected transition: %+v", event)
			}
		})
	}
}

func TestHealthProbeFailures(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		listener := listenLoopback(t)
		url := "http://" + listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		registry := healthRegistry(t, url)
		events, logger := healthLogs()
		stop := startHealthChecks(t.Context(), registry, healthTestConfig(), logger)
		defer stop()
		if event := waitFor(t, events); event.Reason != "transport_error" || event.Status != 0 {
			t.Fatalf("unexpected refusal: %+v", event)
		}
		assertHealth(t, registry, "backend-a", Unhealthy)
	})

	for _, flushHeaders := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeout after headers flushed=%t", flushHeaders), func(t *testing.T) {
			canceled := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if flushHeaders {
					w.Header().Set("Content-Length", "2")
					w.WriteHeader(http.StatusOK)
					if err := http.NewResponseController(w).Flush(); err != nil {
						t.Error(err)
						return
					}
				}
				<-r.Context().Done()
				close(canceled)
			}))
			t.Cleanup(upstream.Close)
			registry := healthRegistry(t, upstream.URL)
			cfg := healthTestConfig()
			cfg.Timeout = 300 * time.Millisecond
			events, logger := healthLogs()
			stop := startHealthChecks(t.Context(), registry, cfg, logger)
			defer stop()
			if event := waitFor(t, events); event.Reason != "timeout" {
				t.Fatalf("unexpected timeout: %+v", event)
			}
			assertHealth(t, registry, "backend-a", Unhealthy)
			waitFor(t, canceled)
		})
	}
}

func TestHealthWorkersIndependentAndCancelable(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	var slowCalls atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if slowCalls.Add(1) != 1 {
			t.Error("unexpected additional probe while first probe was pending")
			return
		}
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	t.Cleanup(slow.Close)
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(fast.Close)
	registry := healthRegistry(t, slow.URL, fast.URL)
	cfg := healthTestConfig()
	// Let the slow probe span multiple ticks to exercise the worker's single-flight behavior.
	cfg.Interval = 20 * time.Millisecond
	cfg.Timeout = time.Minute
	cfg.RecoveryThreshold = 3
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, logger := healthLogs()
	stop := startHealthChecks(ctx, registry, cfg, logger)
	defer stop()
	waitFor(t, entered)
	if event := waitFor(t, events); event.Backend != "backend-b" {
		t.Fatalf("unexpected transition while slow backend was still blocked: %+v", event)
	}
	assertHealth(t, registry, "backend-b", Healthy)
	cancel()
	waitFor(t, canceled)
	stop()
	if got := slowCalls.Load(); got != 1 {
		t.Fatalf("slow backend received %d probes, want 1", got)
	}
	assertHealth(t, registry, "backend-a", Unknown)
}

func TestHealthChecksDisabledAndEmpty(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			t.Error("disabled monitoring sent a probe")
		}
		http.Error(w, "application error", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	registry, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
	logger := slog.New(slog.DiscardHandler)
	stop := startHealthChecks(t.Context(), registry, config.HealthCheckConfig{}, logger)
	assertResponse(t, proxy, http.MethodGet, "/app", 500, "application error\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	stop()
	assertHealth(t, registry, "backend-a", Unknown)

	// No worker should need a ticker when there are no backends.
	empty := newTestRegistry(t)
	startHealthChecks(t.Context(), empty, config.HealthCheckConfig{Enabled: true}, logger)()
}

func TestHealthChecksChangeRoutingAndReadiness(t *testing.T) {
	appEntered, releaseApp := make(chan struct{}), make(chan struct{})
	var first, second atomic.Int32
	first.Store(http.StatusServiceUnavailable)
	second.Store(http.StatusNoContent)
	makeBackend := func(id string, status *atomic.Int32) config.BackendConfig {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				w.WriteHeader(int(status.Load()))
				return
			}
			if r.URL.Path == "/error" {
				http.Error(w, id, http.StatusInternalServerError)
				return
			}
			if r.URL.Path == "/hold" {
				close(appEntered)
				select {
				case <-releaseApp:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = io.WriteString(w, id)
		}))
		t.Cleanup(upstream.Close)
		return config.BackendConfig{ID: id, URL: upstream.URL}
	}
	registry, proxy := startTestProxy(t, makeBackend("backend-a", &first), makeBackend("backend-b", &second))
	assertResponse(t, proxy, http.MethodGet, "/app", 200, "backend-a")
	cfg := healthTestConfig()
	cfg.Interval, cfg.Timeout = 60*time.Millisecond, 40*time.Millisecond
	cfg.FailureThreshold, cfg.RecoveryThreshold = 2, 2
	events, logger := healthLogs()
	stop := startHealthChecks(t.Context(), registry, cfg, logger)
	defer stop()
	seen := map[string]string{}
	for range 2 {
		event := waitFor(t, events)
		seen[event.Backend] = event.Health
	}
	if seen["backend-a"] != "unhealthy" || seen["backend-b"] != "healthy" {
		t.Fatalf("initial transitions = %v", seen)
	}
	assertResponse(t, proxy, http.MethodGet, "/error", 500, "backend-b\n")
	assertHealth(t, registry, "backend-b", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/app", 200, "backend-b")

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxy.URL+"/hold", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan requestResult, 1)
	go func() { result <- readHealthTestResponse(proxy.Client(), request) }()
	waitFor(t, appEntered)
	second.Store(http.StatusServiceUnavailable)
	if event := waitFor(t, events); event.Backend != "backend-b" || event.Health != "unhealthy" {
		t.Fatalf("unexpected transition: %+v", event)
	}
	assertResponse(t, proxy, http.MethodGet, "/app", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	close(releaseApp)
	if got := waitFor(t, result); got.err != nil || got.body != "backend-b" {
		t.Fatalf("in-flight request = %+v, want response from originally selected backend", got)
	}

	first.Store(http.StatusOK)
	if event := waitFor(t, events); event.Backend != "backend-a" || event.Previous != "unhealthy" || event.Health != "healthy" {
		t.Fatalf("unexpected recovery: %+v", event)
	}
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/app", 200, "backend-a")
}

func TestRunStopsHealthChecksWhileDraining(t *testing.T) {
	probe, app := make(chan string, 1), make(chan string, 1)
	probeCanceled, releaseApp := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			probe <- r.RemoteAddr
			<-r.Context().Done()
			close(probeCanceled)
			return
		}
		app <- r.RemoteAddr
		select {
		case <-releaseApp:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)

	idleClosed := make(chan struct{}, 1)
	idle := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	idle.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			idleClosed <- struct{}{}
		}
	}
	idle.Start()
	t.Cleanup(idle.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := healthTestConfig()
	cfg.Timeout = time.Minute
	address := make(chan string, 1)
	idleHealth, logger := healthLogs()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, config.ProxyConfig{
			ListenAddr: "127.0.0.1:0", ShutdownTimeout: testTimeout,
			DialTimeout: time.Second, ResponseHeaderTimeout: testTimeout,
			HealthCheck: cfg,
			Backends: []config.BackendConfig{
				{ID: "backend-a", URL: upstream.URL},
				{ID: "backend-b", URL: idle.URL},
			},
		}, slog.New(&listenLogHandler{Handler: logger.Handler(), address: address}))
	}()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+waitFor(t, address)+"/app", nil)
	if err != nil {
		t.Fatal(err)
	}
	healthPeer := waitFor(t, probe)
	if event := waitFor(t, idleHealth); event.Backend != "backend-b" || event.Health != "healthy" {
		t.Fatalf("unexpected transition: %+v", event)
	}
	select {
	case <-idleClosed:
		t.Fatal("health connection closed before shutdown")
	default:
	}

	client := &http.Client{Timeout: testTimeout}
	t.Cleanup(client.CloseIdleConnections)
	result := make(chan requestResult, 1)
	go func() { result <- readHealthTestResponse(client, request) }()
	if appPeer := waitFor(t, app); appPeer == healthPeer {
		t.Fatal("application request shared the health probe connection")
	}
	cancel()
	waitFor(t, probeCanceled)
	select {
	case err := <-done:
		t.Fatalf("proxy exited while application request was active: %v", err)
	default:
	}
	close(releaseApp)
	if got := waitFor(t, result); got.err != nil || got.body != "finished" {
		t.Fatalf("draining request = %+v, want completed response", got)
	}
	if err := waitFor(t, done); err != nil {
		t.Fatal(err)
	}
	waitFor(t, idleClosed)
}

func readHealthTestResponse(client *http.Client, request *http.Request) requestResult {
	res, err := client.Do(request)
	if err != nil {
		return requestResult{err: err}
	}
	body, err := io.ReadAll(res.Body)
	err = errors.Join(err, res.Body.Close())
	if err == nil && res.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected response status %d", res.StatusCode)
	}
	return requestResult{body: string(body), err: err}
}

func healthTestConfig() config.HealthCheckConfig {
	return config.HealthCheckConfig{
		Enabled: true, Interval: time.Hour, Timeout: time.Second,
		FailureThreshold: 1, RecoveryThreshold: 1,
	}
}

func healthRegistry(t *testing.T, urls ...string) *Registry {
	t.Helper()
	var backends []config.BackendConfig
	for i, url := range urls {
		backends = append(backends, config.BackendConfig{ID: fmt.Sprintf("backend-%c", 'a'+i), URL: url})
	}
	registry, err := NewRegistry(backends)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func assertHealth(t *testing.T, registry *Registry, id string, want Health) {
	t.Helper()
	if backend, ok := registry.Lookup(id); !ok || backend.Health != want {
		t.Fatalf("backend %s health = %v, want %v", id, backend.Health, want)
	}
}

type healthEvent struct {
	Message  string `json:"msg"`
	Backend  string `json:"backend_id"`
	Previous string `json:"previous"`
	Health   string `json:"health"`
	Reason   string `json:"reason"`
	Status   int    `json:"status"`
}

type healthLogWriter chan healthEvent

func (w healthLogWriter) Write(data []byte) (int, error) {
	var event healthEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return 0, err
	}
	if event.Message == "backend health changed" {
		w <- event
	}
	return len(data), nil
}

func healthLogs() (<-chan healthEvent, *slog.Logger) {
	events := make(healthLogWriter, 16)
	return events, slog.New(slog.NewJSONHandler(events, nil))
}
