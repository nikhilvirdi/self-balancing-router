package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestRequestTracker(t *testing.T) {
	tracker := newRequestTracker([]Backend{{ID: "b"}, {ID: "a"}})
	want := []BackendInFlight{{BackendID: "b"}, {BackendID: "a"}}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("initial snapshot = %v, want %v", got, want)
	}
	if _, ok := tracker.InFlight("missing"); ok {
		t.Fatal("unconfigured backend has tracking state")
	}
	const workers = 24
	assigned := make(chan struct{}, workers)
	release := make(chan struct{})
	releaseWorkers := sync.OnceFunc(func() { close(release) })
	var group sync.WaitGroup
	for i := range workers {
		group.Go(func() {
			id := want[i%2].BackendID
			index, ok := tracker.begin(id)
			if !ok {
				t.Errorf("missing tracker entry for %s", id)
			}
			assigned <- struct{}{}
			<-release
			if ok {
				tracker.finish(index)
			}
		})
	}
	defer func() {
		releaseWorkers()
		group.Wait()
	}()
	for range workers {
		waitFor(t, assigned)
		for _, backend := range tracker.Snapshot() {
			if backend.Count < 0 || backend.Count > workers/2 {
				t.Errorf("invalid count: %+v", backend)
			}
		}
	}
	snapshot := tracker.Snapshot()
	for _, backend := range snapshot {
		if backend.Count != workers/2 {
			t.Errorf("count = %d, want %d", backend.Count, workers/2)
		}
	}
	snapshot[0].Count = -1
	assertInFlight(t, tracker, "b", workers/2)
	releaseWorkers()
	group.Wait()
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("final snapshot = %v, want %v", got, want)
	}
}

func TestObservationUpstreamStatus(t *testing.T) {
	for _, status := range []int{200, 400, 404, 429, 500, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "upstream")
			}))
			t.Cleanup(backend.Close)
			observations := make(chan Observation, 4)
			registry, tracker, proxy := startObservedProxy(t, observations, time.Second, backend.URL)
			start := time.Now()
			assertResponse(t, proxy, http.MethodGet, "/app", status, "upstream")
			got := assertObservation(t, observations, "backend-a", status, UpstreamResponse)
			if got.StartedAt.Before(start) || got.FinishedAt.After(time.Now()) {
				t.Fatalf("observation timestamps outside request window: %+v", got)
			}
			assertInFlight(t, tracker, "backend-a", 0)
			assertHealth(t, registry, "backend-a", Unknown)
			assertNoObservation(t, observations)
		})
	}
}

func TestObservationGatewayFailures(t *testing.T) {
	for _, policy := range []config.ProxyPolicy{config.PolicyFirstEligible, config.PolicyLeastRequests, config.PolicyP2C} {
		t.Run(string(policy), func(t *testing.T) { testObservationGatewayFailures(t, policy) })
	}
}

func testObservationGatewayFailures(t *testing.T, policy config.ProxyPolicy) {
	t.Helper()
	for _, timeout := range []bool{false, true} {
		name := "refused"
		if timeout {
			name = "timeout"
		}
		t.Run(name, func(t *testing.T) {
			observations := make(chan Observation, 4)
			if timeout {
				backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					<-r.Context().Done()
				}))
				t.Cleanup(backend.Close)
				registry, tracker, proxy := startObservedProxyWithPolicy(t, policy, observations, 100*time.Millisecond, backend.URL)
				assertResponse(t, proxy, http.MethodGet, "/app", 504, "gateway timeout\n")
				assertObservation(t, observations, "backend-a", 0, UpstreamTimeout)
				assertInFlight(t, tracker, "backend-a", 0)
				assertHealth(t, registry, "backend-a", Unknown)
			} else {
				listener := listenLoopback(t)
				registry, tracker, proxy := startObservedProxyWithPolicy(t, policy, observations, time.Second, "http://"+listener.Addr().String())
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				assertResponse(t, proxy, http.MethodGet, "/app", 502, "bad gateway\n")
				assertObservation(t, observations, "backend-a", 0, TransportFailure)
				assertInFlight(t, tracker, "backend-a", 0)
				assertHealth(t, registry, "backend-a", Unknown)
			}
			assertNoObservation(t, observations)
		})
	}
}

func TestObservationCancellation(t *testing.T) {
	entered := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	t.Cleanup(backend.Close)
	observations := make(chan Observation, 4)
	registry, tracker, proxy := startObservedProxy(t, observations, testTimeout, backend.URL)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := startObservedRequest(t, ctx, proxy, "/app")
	waitFor(t, entered)
	assertInFlight(t, tracker, "backend-a", 1)
	cancel()
	if got := waitFor(t, result); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("client result = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-a", 0, Canceled)
	assertInFlight(t, tracker, "backend-a", 0)
	assertHealth(t, registry, "backend-a", Unknown)
	assertNoObservation(t, observations)
}

func TestObservationStreaming(t *testing.T) {
	for _, tt := range []struct {
		name    string
		outcome Outcome
	}{
		{"complete", UpstreamResponse},
		{"interrupted", Interrupted},
		{"canceled", Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "first")
				if err := http.NewResponseController(w).Flush(); err != nil {
					t.Error(err)
					return
				}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				if tt.outcome == Interrupted {
					panic(http.ErrAbortHandler)
				}
				_, _ = io.WriteString(w, "last")
			}))
			t.Cleanup(backend.Close)
			observations := make(chan Observation, 4)
			_, tracker, proxy := startObservedProxy(t, observations, testTimeout, backend.URL)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/app", nil)
			if err != nil {
				t.Fatal(err)
			}
			res, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = res.Body.Close() }()
			first := make([]byte, len("first"))
			if _, err := io.ReadFull(res.Body, first); err != nil || string(first) != "first" || res.StatusCode != 200 {
				t.Fatalf("initial response = %d %q, %v", res.StatusCode, first, err)
			}
			assertInFlight(t, tracker, "backend-a", 1)
			assertNoObservation(t, observations)
			if tt.outcome == Canceled {
				cancel()
			} else {
				close(release)
			}
			rest, err := io.ReadAll(res.Body)
			switch tt.outcome {
			case Interrupted:
				if !errors.Is(err, io.ErrUnexpectedEOF) || len(rest) != 0 {
					t.Fatalf("interrupted response = %q, %v", rest, err)
				}
			case Canceled:
				if !errors.Is(err, context.Canceled) || len(rest) != 0 {
					t.Fatalf("canceled response = %q, %v", rest, err)
				}
			default:
				if err != nil || string(rest) != "last" {
					t.Fatalf("stream completion = %q, %v", rest, err)
				}
			}
			assertObservation(t, observations, "backend-a", 200, tt.outcome)
			assertInFlight(t, tracker, "backend-a", 0)
			assertNoObservation(t, observations)
		})
	}
}

func TestObservationAssignmentSurvivesHealthChange(t *testing.T) {
	const total = 8
	entered := make(chan struct{}, total)
	release := make(chan struct{})
	failHealth := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			select {
			case <-failHealth:
				w.WriteHeader(http.StatusServiceUnavailable)
			case <-r.Context().Done():
			}
			return
		}
		entered <- struct{}{}
		select {
		case <-release:
			_, _ = io.WriteString(w, "first")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(first.Close)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "second")
	}))
	t.Cleanup(second.Close)
	observations := make(chan Observation, total+2)
	registry, tracker, proxy := startObservedProxy(t, observations, testTimeout, first.URL, second.URL)
	results := make([]<-chan requestResult, total)
	for i := range results {
		results[i] = startObservedRequest(t, t.Context(), proxy, "/app")
	}
	for range total {
		waitFor(t, entered)
	}
	assertInFlight(t, tracker, "backend-a", total)
	assertInFlight(t, tracker, "backend-b", 0)
	events, logger := healthLogs()
	close(failHealth)
	stop := startHealthChecks(t.Context(), registry, healthTestConfig(), logger)
	defer stop()
	waitFor(t, events)
	waitFor(t, events)
	assertHealth(t, registry, "backend-a", Unhealthy)
	assertNoObservation(t, observations)
	assertResponse(t, proxy, http.MethodGet, "/app", 200, "second")
	later := assertObservation(t, observations, "backend-b", 200, UpstreamResponse)
	assertInFlight(t, tracker, "backend-a", total)
	assertInFlight(t, tracker, "backend-b", 0)
	close(release)
	for _, result := range results {
		if got := waitFor(t, result); got.err != nil || got.body != "first" {
			t.Errorf("active request = %+v, want first backend", got)
		}
		got := assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
		if got.StartedAt.After(later.StartedAt) || got.FinishedAt.Before(later.FinishedAt) {
			t.Fatalf("first backend attempt did not span later attempt: %+v, %+v", got, later)
		}
	}
	assertInFlight(t, tracker, "backend-a", 0)
	assertNoObservation(t, observations)
}

func TestObservationExclusions(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("unexpected upstream request %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)
	observations := make(chan Observation, 4)
	registry, tracker, proxy := startObservedProxy(t, observations, time.Second, backend.URL)
	events, logger := healthLogs()
	stop := startHealthChecks(t.Context(), registry, healthTestConfig(), logger)
	defer stop()
	waitFor(t, events)
	assertHealth(t, registry, "backend-a", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	setHealth(t, registry, "backend-a", Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/app", 503, "request routing is unavailable\n")
	assertInFlight(t, tracker, "backend-a", 0)
	assertNoObservation(t, observations)
}

func TestMissingUpstreamHandler(t *testing.T) {
	registry := newTestRegistry(t, "backend-a", "backend-b")
	tracker := newRequestTracker(registry.Snapshot())
	for _, upstreams := range []map[string]*httputil.ReverseProxy{
		{},
		{"backend-a": nil},
	} {
		observations := make(chan Observation, 4)
		upstreams["backend-b"] = &httputil.ReverseProxy{Rewrite: func(*httputil.ProxyRequest) {
			t.Error("request fell back to the second backend")
		}}
		var logs strings.Builder
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		handle := handler(registry, firstEligiblePolicy{}, upstreams, tracker, func(o Observation) { observations <- o }, logger)
		res := httptest.NewRecorder()
		handle.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/app", nil))
		if res.Code != 500 || res.Body.String() != "internal server error\n" {
			t.Fatalf("missing upstream response = %d %q", res.Code, res.Body)
		}
		if !strings.Contains(logs.String(), `"backend_id":"backend-a"`) || !strings.Contains(logs.String(), `"level":"ERROR"`) {
			t.Fatalf("missing structured internal error: %s", logs.String())
		}
		assertInFlight(t, tracker, "backend-a", 0)
		assertHealth(t, registry, "backend-a", Unknown)
		assertNoObservation(t, observations)
	}
}

func TestObservationDraining(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backend.Close)
	registry := healthRegistry(t, backend.URL)
	observations := make(chan Observation, 4)
	tracker, handle := observedHandler(t, registry, config.PolicyFirstEligible, testTimeout, func(o Observation) { observations <- o })
	listener := &observedListener{Listener: listenLoopback(t), closed: make(chan struct{})}
	stop, done, request := startRequest(t, listener, &http.Server{Handler: handle}, testTimeout)
	waitFor(t, entered)
	stop()
	waitFor(t, listener.closed)
	assertInFlight(t, tracker, "backend-a", 1)
	assertNoObservation(t, observations)
	close(release)
	if got := waitFor(t, request); got.err != nil || got.body != "finished" {
		t.Fatalf("draining response = %+v", got)
	}
	if err := waitFor(t, done); err != nil {
		t.Fatal(err)
	}
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	assertInFlight(t, tracker, "backend-a", 0)
	assertNoObservation(t, observations)
}

func TestForwardTrackedObserver(t *testing.T) {
	for _, observerPanics := range []bool{false, true} {
		tracker := newRequestTracker([]Backend{{ID: "backend-a"}})
		res := httptest.NewRecorder()
		var calls int
		var observer Observer
		if observerPanics {
			observer = func(o Observation) {
				calls++
				assertInFlight(t, tracker, o.BackendID, 0)
				panic("observer failure")
			}
		}
		upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assertInFlight(t, tracker, "backend-a", 1)
			requestObservation(r).UpstreamStatus = http.StatusNoContent
			w.WriteHeader(http.StatusNoContent)
		})
		index, ok := tracker.begin("backend-a")
		if !ok {
			t.Fatal("missing tracker entry")
		}
		observation := Observation{BackendID: "backend-a", StartedAt: time.Now()}
		forwardTracked(res, httptest.NewRequest(http.MethodGet, "/", nil), upstream, tracker, index, observation, observer, slog.New(slog.DiscardHandler))
		assertInFlight(t, tracker, "backend-a", 0)
		if res.Code != 204 || (observerPanics && calls != 1) {
			t.Fatalf("response = %d, callback count = %d", res.Code, calls)
		}
	}
}

func startObservedProxy(t *testing.T, observations chan<- Observation, timeout time.Duration, urls ...string) (*Registry, *requestTracker, *httptest.Server) {
	t.Helper()
	return startObservedProxyWithPolicy(t, config.PolicyFirstEligible, observations, timeout, urls...)
}

func startObservedProxyWithPolicy(t *testing.T, policy config.ProxyPolicy, observations chan<- Observation, timeout time.Duration, urls ...string) (*Registry, *requestTracker, *httptest.Server) {
	t.Helper()
	registry := healthRegistry(t, urls...)
	tracker, handle := observedHandler(t, registry, policy, timeout, func(o Observation) { observations <- o })
	server := httptest.NewServer(handle)
	server.Client().Timeout = testTimeout
	t.Cleanup(server.Close)
	return registry, tracker, server
}

func observedHandler(t *testing.T, registry *Registry, policyName config.ProxyPolicy, timeout time.Duration, observer Observer) (*requestTracker, http.Handler) {
	t.Helper()
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(policyName, tracker, 1)
	if err != nil {
		t.Fatal(err)
	}
	transport := newTransport(time.Second, timeout)
	t.Cleanup(transport.CloseIdleConnections)
	logger := slog.New(slog.DiscardHandler)
	upstreams, err := newUpstreams(backends, transport, logger)
	if err != nil {
		t.Fatal(err)
	}
	return tracker, handler(registry, policy, upstreams, tracker, observer, logger)
}

func startObservedRequest(t *testing.T, ctx context.Context, proxy *httptest.Server, path string) <-chan requestResult {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan requestResult, 1)
	go func() { result <- readHealthTestResponse(proxy.Client(), req) }()
	return result
}

func assertObservation(t *testing.T, observations <-chan Observation, id string, status int, outcome Outcome) Observation {
	t.Helper()
	got := waitFor(t, observations)
	if got.BackendID != id || got.UpstreamStatus != status || got.Outcome != outcome {
		t.Fatalf("observation = %+v, want backend %s, status %d, outcome %v", got, id, status, outcome)
	}
	if got.StartedAt.IsZero() || got.FinishedAt.IsZero() || got.Duration < 0 ||
		got.Duration != got.FinishedAt.Sub(got.StartedAt) {
		t.Fatalf("invalid observation timing: %+v", got)
	}
	return got
}

func assertNoObservation(t *testing.T, observations <-chan Observation) {
	t.Helper()
	select {
	case got := <-observations:
		t.Fatalf("unexpected observation: %+v", got)
	default:
	}
}

func assertInFlight(t *testing.T, tracker *requestTracker, id string, want int) {
	t.Helper()
	if count, ok := tracker.InFlight(id); !ok || count != want {
		t.Fatalf("in-flight for %s = %d, present=%t, want %d", id, count, ok, want)
	}
}

func TestAllowedRequestTrailerIgnoresCase(t *testing.T) {
	for _, field := range []string{
		"x-forwarded-FoR", "X-FORWARDED-PROTO", "x-forwarded-host", "fOrWaRdEd",
		"AUTHORIZATION", "connection", "tRaNsFeR-EnCoDiNg", "proxy-authorization",
		"iF-MaTcH", "x-nOmInAtEd",
	} {
		if allowedRequestTrailer(field, []string{"keep-alive, X-NOMINATED"}) {
			t.Errorf("allowed blocked trailer %s", field)
		}
	}
	if !allowedRequestTrailer(strings.ToLower("X-Upload-Result"), []string{"X-Nominated"}) {
		t.Fatal("blocked a legitimate application trailer")
	}
}
