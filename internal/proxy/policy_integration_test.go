package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestConfiguredPolicies(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		want  []string
	}{
		{"default", "", []string{"backend-a", "backend-a", "backend-a"}},
		{"explicit first eligible", "first-eligible", []string{"backend-a", "backend-a", "backend-a"}},
		{"round robin", "round-robin", []string{"backend-a", "backend-b", "backend-a"}},
		{"least requests", "least-requests", []string{"backend-a", "backend-a", "backend-a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			urls := identityBackends(t, 2)
			values := map[string]string{
				"SBR_PROXY_LISTEN_ADDR":          "127.0.0.1:0",
				"SBR_PROXY_HEALTH_CHECK_ENABLED": "false",
				"SBR_PROXY_BACKENDS":             "backend-a=" + urls[0] + ",backend-b=" + urls[1],
			}
			if tt.value != "" {
				values["SBR_PROXY_POLICY"] = tt.value
			}
			cfg, err := config.LoadProxy(func(key string) (string, bool) {
				value, ok := values[key]
				return value, ok
			})
			if err != nil {
				t.Fatal(err)
			}
			start := func() string {
				address := make(chan string, 1)
				done := make(chan error, 1)
				ctx, stop := context.WithCancel(t.Context())
				t.Cleanup(func() {
					stop()
					if err := waitFor(t, done); err != nil {
						t.Error(err)
					}
				})
				go func() {
					logger := slog.New(&listenLogHandler{Handler: slog.NewTextHandler(io.Discard, nil), address: address})
					done <- Run(ctx, cfg, logger)
				}()
				return "http://" + waitFor(t, address)
			}
			first := start()
			client := &http.Client{Timeout: testTimeout}
			t.Cleanup(client.CloseIdleConnections)
			for i, want := range tt.want {
				if i == 1 {
					assertPolicyResponse(t, client, start(), "backend-a")
				}
				assertPolicyResponse(t, client, first, want)
			}
		})
	}
	if err := Run(t.Context(), config.ProxyConfig{Policy: "unknown"}, slog.New(slog.DiscardHandler)); err == nil ||
		!strings.Contains(err.Error(), "unsupported routing policy") {
		t.Fatalf("invalid policy startup error = %v", err)
	}
}

func assertPolicyResponse(t *testing.T, client *http.Client, url, want string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url+"/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, body := doRequest(t, client, req)
	if res.StatusCode != http.StatusOK || body != want {
		t.Fatalf("response = %d %q, want 200 %q", res.StatusCode, body, want)
	}
}

func TestRoundRobinForwarding(t *testing.T) {
	const count = 5
	registry, tracker, proxy, observations := startObservedPolicyProxy(t, config.PolicyRoundRobin, 16, identityBackends(t, count)...)
	events, logger := healthLogs()
	stop := startHealthChecks(t.Context(), registry, healthTestConfig(), logger)
	defer stop()
	for range count {
		if event := waitFor(t, events); event.Health != "healthy" {
			t.Fatalf("initial health check = %+v, want healthy", event)
		}
	}
	assertNoObservation(t, observations)
	all := registry.Snapshot()
	for i := range count * 2 {
		assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
		assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
		id := all[i%count].ID
		assertResponse(t, proxy, http.MethodGet, "/items", 200, id)
		assertObservation(t, observations, id, 200, UpstreamResponse)
		assertInFlight(t, tracker, id, 0)
	}

	for _, candidate := range all[1:] {
		setHealth(t, registry, candidate.ID, Unhealthy)
	}
	assertResponse(t, proxy, http.MethodGet, "/items", 200, all[0].ID)
	assertObservation(t, observations, all[0].ID, 200, UpstreamResponse)
	setHealth(t, registry, all[0].ID, Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertNoObservation(t, observations)

	setHealth(t, registry, all[2].ID, Healthy)
	setHealth(t, registry, all[4].ID, Healthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, all[4].ID)
	assertObservation(t, observations, all[4].ID, 200, UpstreamResponse)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, all[2].ID)
	assertObservation(t, observations, all[2].ID, 200, UpstreamResponse)
}

func TestRoundRobinConcurrentForwarding(t *testing.T) {
	testConcurrentPolicyForwarding(t, config.PolicyRoundRobin, 20)
}

func TestLeastRequestsConcurrentForwarding(t *testing.T) {
	testConcurrentPolicyForwarding(t, config.PolicyLeastRequests, 103)
}

func testConcurrentPolicyForwarding(t *testing.T, policy config.ProxyPolicy, total int) {
	t.Helper()
	const count = 5
	entered := make(chan string, total)
	release := make(chan struct{})
	urls := make([]string, count)
	for i := range urls {
		id := fmt.Sprintf("backend-%c", 'a'+i)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- id
			select {
			case <-release:
				_, _ = io.WriteString(w, id)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(server.Close)
		urls[i] = server.URL
	}
	registry, tracker, proxy, observations := startObservedPolicyProxy(t, policy, total, urls...)
	results := make([]<-chan requestResult, total)
	for i := range results {
		results[i] = startObservedRequest(t, t.Context(), proxy, "/items")
	}
	started := make(map[string]int)
	for range total {
		started[waitFor(t, entered)]++
	}
	for i, candidate := range registry.Snapshot() {
		want := total / count
		if i < total%count {
			want++
		}
		if got := started[candidate.ID]; got != want {
			t.Errorf("requests to %s = %d, want %d", candidate.ID, got, want)
		}
		assertInFlight(t, tracker, candidate.ID, want)
	}
	assertNoObservation(t, observations)
	close(release)
	completed, observed := make(map[string]int), make(map[string]int)
	for _, result := range results {
		got := waitFor(t, result)
		if got.err != nil {
			t.Fatal(got.err)
		}
		completed[got.body]++
		event := waitFor(t, observations)
		if event.Outcome != UpstreamResponse || event.UpstreamStatus != http.StatusOK {
			t.Fatalf("unexpected observation: %+v", event)
		}
		observed[event.BackendID]++
	}
	for _, candidate := range registry.Snapshot() {
		id := candidate.ID
		if completed[id] != started[id] || observed[id] != started[id] {
			t.Errorf("%s completed %d and observed %d, want %d each", id, completed[id], observed[id], started[id])
		}
		assertInFlight(t, tracker, id, 0)
	}
	assertNoObservation(t, observations)
}

func TestRoundRobinFailureAndCancellationFeedback(t *testing.T) {
	entered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cancel" {
			close(entered)
			<-r.Context().Done()
			return
		}
		http.Error(w, "upstream failure", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	listener := listenLoopback(t)
	closedURL := "http://" + listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, tracker, proxy, observations := startObservedPolicyProxy(t, config.PolicyRoundRobin, 4, upstream.URL, closedURL)
	assertResponse(t, proxy, http.MethodGet, "/items", 500, "upstream failure\n")
	assertObservation(t, observations, "backend-a", 500, UpstreamResponse)
	assertResponse(t, proxy, http.MethodGet, "/items", 502, "bad gateway\n")
	assertObservation(t, observations, "backend-b", 0, TransportFailure)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := startObservedRequest(t, ctx, proxy, "/cancel")
	waitFor(t, entered)
	assertInFlight(t, tracker, "backend-a", 1)
	cancel()
	if got := waitFor(t, result); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("client result = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-a", 0, Canceled)
	assertInFlight(t, tracker, "backend-a", 0)
	assertInFlight(t, tracker, "backend-b", 0)
	assertNoObservation(t, observations)
}

func TestLeastRequestsEligibilityAndFeedback(t *testing.T) {
	entered := make(chan struct{})
	secondEntered, finishSecond := make(chan struct{}), make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(entered)
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "backend-a")
	}))
	t.Cleanup(first.Close)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/finish-first" {
			close(secondEntered)
			select {
			case <-finishSecond:
				_, _ = io.WriteString(w, "backend-b")
			case <-r.Context().Done():
			}
			return
		}
		http.Error(w, "backend overloaded", http.StatusServiceUnavailable)
	}))
	t.Cleanup(second.Close)
	registry, tracker, proxy, observations := startObservedPolicyProxy(t, config.PolicyLeastRequests, 8, first.URL, second.URL)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := startObservedRequest(t, ctx, proxy, "/hold")
	waitFor(t, entered)
	assertInFlight(t, tracker, "backend-a", 1)

	_, independent, other, otherObservations := startObservedPolicyProxy(t, config.PolicyLeastRequests, 2, first.URL, second.URL)
	assertResponse(t, other, http.MethodGet, "/items", 200, "backend-a")
	assertObservation(t, otherObservations, "backend-a", 200, UpstreamResponse)
	assertInFlight(t, independent, "backend-a", 0)

	earlierCompletion := startObservedRequest(t, t.Context(), proxy, "/finish-first")
	waitFor(t, secondEntered)
	assertInFlight(t, tracker, "backend-b", 1)
	close(finishSecond)
	if got := waitFor(t, earlierCompletion); got.err != nil || got.body != "backend-b" {
		t.Fatalf("later assignment = %+v, want backend-b", got)
	}
	assertObservation(t, observations, "backend-b", 200, UpstreamResponse)

	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	assertNoObservation(t, observations)
	assertInFlight(t, tracker, "backend-a", 1)
	assertInFlight(t, tracker, "backend-b", 0)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "backend overloaded\n")
	assertObservation(t, observations, "backend-b", 503, UpstreamResponse)
	assertInFlight(t, tracker, "backend-b", 0)
	assertHealth(t, registry, "backend-b", Unknown)

	setHealth(t, registry, "backend-b", Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-a")
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	setHealth(t, registry, "backend-a", Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertNoObservation(t, observations)
	assertInFlight(t, tracker, "backend-a", 1)
	setHealth(t, registry, "backend-b", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "backend overloaded\n")
	assertObservation(t, observations, "backend-b", 503, UpstreamResponse)

	cancel()
	if got := waitFor(t, pending); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("client result = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-a", 0, Canceled)
	assertInFlight(t, tracker, "backend-a", 0)
	assertHealth(t, registry, "backend-a", Unhealthy)
	setHealth(t, registry, "backend-a", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-a")
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	assertInFlight(t, tracker, "backend-a", 0)
	assertInFlight(t, tracker, "backend-b", 0)
	assertNoObservation(t, observations)
}

func startObservedPolicyProxy(t *testing.T, policy config.ProxyPolicy, buffer int, urls ...string) (*Registry, *requestTracker, *httptest.Server, <-chan Observation) {
	t.Helper()
	observations := make(chan Observation, buffer)
	registry, tracker, server := startObservedProxyWithPolicy(t, policy, observations, testTimeout, urls...)
	return registry, tracker, server, observations
}

func identityBackends(t *testing.T, count int) []string {
	t.Helper()
	urls := make([]string, count)
	for i := range urls {
		id := fmt.Sprintf("backend-%c", 'a'+i)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, id)
		}))
		t.Cleanup(server.Close)
		urls[i] = server.URL
	}
	return urls
}
