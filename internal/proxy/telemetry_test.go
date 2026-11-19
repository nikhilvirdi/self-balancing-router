package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

func TestBanditTracePreservesSelectionAndFeedback(t *testing.T) {
	for _, policy := range []config.ProxyPolicy{config.PolicyUCB1, config.PolicySlidingWindowUCB, config.PolicyDiscountedUCB, config.PolicySlidingWindowUCBPending} {
		t.Run(string(policy), func(t *testing.T) {
			cfg := config.ProxyConfig{Policy: policy, MABLatencyScale: 100 * time.Millisecond, MABSWWindow: 3, MABDiscountFactor: .99, MABPendingPenalty: .05}
			backends := []Backend{{ID: "a"}, {ID: "b"}}
			makePolicy := func() (routingPolicy, *requestTracker) {
				tracker := newRequestTracker(backends)
				p, err := newRoutingPolicy(cfg, backends, tracker)
				if err != nil {
					t.Fatal(err)
				}
				return p, tracker
			}
			plain, _ := makePolicy()
			traced, tracker := makePolicy()
			probe := traced.(interface {
				selectDiagnostic([]Backend, *telemetry.BanditDecision) (Backend, bool)
				applyFeedback(Observation) (float64, bool)
			})
			plainFeedback := plain.(observingPolicy)
			now := time.Unix(10, 0)
			for i := range 12 {
				want, _ := plain.Select(backends)
				var decision telemetry.BanditDecision
				got, ok := probe.selectDiagnostic(backends, &decision)
				if !ok || got != want {
					t.Fatalf("step %d selected %+v, want %+v", i, got, want)
				}
				reason := "maximum_ucb"
				if i < len(backends) {
					reason = "cold_no_pending"
				}
				if decision.Reason != reason || len(decision.Arms) == 0 {
					t.Fatalf("step %d decision = %+v", i, decision)
				}
				o := newTestObservation(got.ID, now, 100*time.Millisecond, UpstreamResponse, http.StatusOK)
				plainFeedback.observe(o)
				reward, applied := probe.applyFeedback(o)
				if reward != .5 || !applied {
					t.Fatalf("feedback = %v, %t", reward, applied)
				}
				o.Outcome = Canceled
				plainFeedback.observe(o)
				if _, applied := probe.applyFeedback(o); applied {
					t.Fatal("canceled request became feedback")
				}
			}
			logger := slog.New(slog.DiscardHandler)
			var log bytes.Buffer
			events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(&log, nil)), 1, "proxy")
			upstreams := telemetryUpstreams()
			upstreams["b"] = upstreams["a"]
			registry := &Registry{backends: backends}
			h := instrumentedHandler(registry, traced, upstreams, tracker, nil, logger, nil, events, true)
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			events.Close()
			decoder := json.NewDecoder(&log)
			var selection, feedback, completion struct{ Event telemetry.Event }
			for _, record := range []any{&selection, &feedback, &completion} {
				if err := decoder.Decode(record); err != nil {
					t.Fatal(err)
				}
			}
			s, f, c := selection.Event, feedback.Event, completion.Event
			if s.Bandit == nil || f.Feedback == nil || s.Bandit.Order != 1 || f.Feedback.Order != 2 ||
				s.RequestID != f.RequestID || s.RequestID != c.RequestID || !f.Feedback.Applied ||
				f.Feedback.AgeNS < c.DurationNS || f.Feedback.Reward <= 0 {
				t.Fatalf("uncorrelated decision/feedback: %+v %+v %+v", s, f, c)
			}
		})
	}
}

func TestBanditTraceOrdersOutOfOrderFeedback(t *testing.T) {
	backends := []Backend{{ID: "a"}, {ID: "b"}}
	tracker := newRequestTracker(backends)
	cfg := config.ProxyConfig{Policy: config.PolicySlidingWindowUCB, MABLatencyScale: time.Second, MABSWWindow: 2}
	policy, err := newRoutingPolicy(cfg, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	upstreams := telemetryUpstreams()
	upstream := upstreams["a"]
	transport := upstream.Transport
	started := make(chan struct{})
	release := make(chan struct{})
	upstream.Transport = telemetryRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/first" {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return transport.RoundTrip(r)
	})
	upstreams["b"] = upstream
	var log bytes.Buffer
	events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(&log, nil)), 1, "proxy")
	defer events.Close()
	h := instrumentedHandler(&Registry{backends: backends}, policy, upstreams, tracker, nil,
		slog.New(slog.DiscardHandler), nil, events, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/first", nil))
		done <- w.Code
	}()
	waitFor(t, started)
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/second", nil))
	close(release)
	if first := waitFor(t, done); first != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("response statuses = %d, %d", first, second.Code)
	}
	events.Close()
	decoder := json.NewDecoder(&log)
	for i, backend := range []string{"a", "b", "b", "b", "a", "a"} {
		var record struct{ Event telemetry.Event }
		if err := decoder.Decode(&record); err != nil || record.Event.BackendID != backend {
			t.Fatalf("record %d = %+v, error %v", i, record.Event, err)
		}
		if i == 1 && (record.Event.Bandit == nil || record.Event.Bandit.Order != 2) {
			t.Fatalf("incorrect second decision: %+v", record.Event.Bandit)
		}
		if want := map[int]uint64{2: 3, 4: 4}[i]; want != 0 &&
			(record.Event.Feedback == nil || record.Event.Feedback.Order != want || !record.Event.Feedback.Applied) {
			t.Fatalf("incorrect feedback at record %d: %+v", i, record.Event.Feedback)
		}
	}
}

func TestPendingWindowDiagnosticReplay(t *testing.T) {
	backends := []Backend{{ID: "a"}}
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(config.ProxyConfig{
		Policy: config.PolicySlidingWindowUCBPending, MABLatencyScale: time.Second, MABSWWindow: 2, MABPendingPenalty: 4,
	}, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	upstreams := telemetryUpstreams()
	upstream := upstreams["a"]
	upstream.Transport = telemetryRoundTripper(func(r *http.Request) (*http.Response, error) {
		var gate <-chan struct{}
		switch r.URL.Path {
		case "/first":
			gate = release[0]
		case "/second":
			gate = release[1]
		}
		if gate != nil {
			entered <- struct{}{}
			select {
			case <-gate:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		status := http.StatusOK
		switch r.URL.Path {
		case "/redirect":
			status = http.StatusFound
		case "/client-error":
			status = http.StatusBadRequest
		case "/overload":
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Body: http.NoBody, Request: r}, nil
	})
	originalResponse := upstream.ModifyResponse
	upstream.ModifyResponse = func(r *http.Response) error {
		if err := originalResponse(r); err != nil {
			return err
		}
		if r.Request.URL.Path == "/interrupted" {
			panic(http.ErrAbortHandler)
		}
		return nil
	}
	var output bytes.Buffer
	events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(&output, nil)), 1, "proxy")
	defer events.Close()
	h := instrumentedHandler(&Registry{backends: backends}, policy, upstreams, tracker, nil,
		slog.New(slog.DiscardHandler), nil, events, true)
	serve := func(path string) {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	serve("/initial")
	done := []chan struct{}{make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for i, path := range []string{"/first", "/second"} {
		go func() {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
			close(done[i])
		}()
		waitFor(t, entered)
	}
	assertInFlight(t, tracker, "a", 2)
	close(release[1])
	waitFor(t, done[1])
	close(release[0])
	waitFor(t, done[0])
	candidate := policy.(*banditPolicy[windowedArmState])
	window := candidate.algorithm.(*pendingSlidingWindowUCB)
	for _, path := range []string{"/redirect", "/client-error", "/overload"} {
		before := slices.Clone(window.window)
		serve(path)
		assertInFlight(t, tracker, "a", 0)
		if path != "/overload" && !slices.Equal(before, window.window) {
			t.Fatal("excluded response changed the window")
		}
	}
	before := slices.Clone(window.window)
	func() {
		defer func() {
			if got := recover(); got != http.ErrAbortHandler {
				t.Fatalf("interruption = %v", got)
			}
		}()
		serve("/interrupted")
	}()
	assertInFlight(t, tracker, "a", 0)
	if !slices.Equal(before, window.window) {
		t.Fatal("interrupted response became a reward")
	}
	events.Close()
	if events.Dropped() != 0 {
		t.Fatal("diagnostic export dropped events")
	}
	t.Log(output.String())
}

func BenchmarkBanditTrace(b *testing.B) {
	for _, mode := range []struct {
		name    string
		enabled bool
	}{{"standard_events", false}, {"bandit_detail", true}} {
		b.Run(mode.name, func(b *testing.B) {
			backends := []Backend{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}, {ID: "e"}}
			tracker := newRequestTracker(backends)
			cfg := config.ProxyConfig{Policy: config.PolicySlidingWindowUCB, MABLatencyScale: 100 * time.Millisecond, MABSWWindow: 1000}
			policy, err := newRoutingPolicy(cfg, backends, tracker)
			if err != nil {
				b.Fatal(err)
			}
			upstreams := telemetryUpstreams()
			for _, backend := range backends[1:] {
				upstreams[backend.ID] = upstreams["a"]
			}
			events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(io.Discard, nil)), 1, "proxy")
			h := instrumentedHandler(&Registry{backends: backends}, policy, upstreams, tracker, nil,
				slog.New(slog.DiscardHandler), nil, events, mode.enabled)
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			b.ReportAllocs()
			for b.Loop() {
				h.ServeHTTP(httptest.NewRecorder(), request)
			}
			events.Close()
			b.ReportMetric(float64(events.Dropped())/float64(b.N), "dropped-events/op")
		})
	}
}

func TestProxyTelemetry(t *testing.T) {
	ids := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids <- r.Header.Get(telemetry.RequestHeader)
		if r.URL.Path == "/overload" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(upstream.Close)
	registry := healthRegistry(t, upstream.URL)
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	logger := slog.New(slog.DiscardHandler)
	transport := newTransport(time.Second, time.Second)
	t.Cleanup(transport.CloseIdleConnections)
	upstreams, err := newUpstreams(backends, transport, logger)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(&log, nil)), 1, "proxy")
	defer events.Close()
	metrics := newProxyMetrics(backends)
	h := instrumentedHandler(registry, firstEligiblePolicy{}, upstreams, tracker, nil, logger, metrics, events, false)
	var propagated []string
	for i, path := range []string{"/ok?private=secret", "/overload", "/unavailable"} {
		if i == 2 {
			setHealth(t, registry, "backend-a", Unhealthy)
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set(telemetry.RequestHeader, "spoofed")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if want := []int{200, 503, 503}[i]; w.Code != want || w.Header().Get(telemetry.RequestHeader) != "" {
			t.Fatalf("response %d with headers %v, want %d", w.Code, w.Header(), want)
		}
		if i < 2 {
			propagated = append(propagated, waitFor(t, ids))
		}
	}
	events.Close()
	if strings.Contains(log.String(), "secret") || strings.Contains(log.String(), "spoofed") {
		t.Fatal("client data leaked to diagnostics")
	}
	decoder := json.NewDecoder(&log)
	for i, expected := range []string{"backend_selected", "request_completed", "backend_selected", "request_completed", "request_completed"} {
		var record struct{ Event telemetry.Event }
		if err := decoder.Decode(&record); err != nil || record.Event.Kind != expected || !telemetry.ValidRequestID(record.Event.RequestID) {
			t.Fatalf("event %d = %+v, error %v", i, record.Event, err)
		}
		if i < 4 && record.Event.RequestID != propagated[i/2] {
			t.Fatalf("event %d did not match the upstream ID", i)
		}
		if i == 4 && (record.Event.Status != 503 || record.Event.Outcome != "unavailable" || record.Event.BackendID != "") {
			t.Fatalf("incorrect unavailable outcome: %+v", record.Event)
		}
	}
	var scrape strings.Builder
	metrics.render(&scrape, registry, tracker, events)
	for _, want := range []string{
		`sbr_proxy_requests_total{status="200"} 1`, `sbr_proxy_requests_total{status="503"} 2`,
		`sbr_proxy_request_outcomes_total{outcome="unavailable"} 1`, `sbr_proxy_routed_requests_total{backend="backend-a"} 2`,
		`sbr_proxy_backend_health{backend="backend-a",state="unhealthy"} 1`, `sbr_proxy_backend_in_flight{backend="backend-a"} 0`,
		`sbr_proxy_request_duration_seconds_count 3`, `sbr_proxy_successful_request_duration_seconds_count 1`,
	} {
		if !strings.Contains(scrape.String(), want+"\n") {
			t.Errorf("missing metric %s", want)
		}
	}
}

type telemetryRoundTripper func(*http.Request) (*http.Response, error)

func (f telemetryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func telemetryUpstreams() map[string]*httputil.ReverseProxy {
	return map[string]*httputil.ReverseProxy{"a": {
		Rewrite: func(*httputil.ProxyRequest) {},
		Transport: telemetryRoundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
		}),
		ModifyResponse: func(res *http.Response) error {
			requestObservation(res.Request).UpstreamStatus = res.StatusCode
			return nil
		},
	}}
}

func TestProxyTelemetryConcurrentScrape(t *testing.T) {
	registry := &Registry{backends: []Backend{{ID: "a"}}}
	tracker := newRequestTracker(registry.Snapshot())
	metrics := newProxyMetrics(registry.Snapshot())
	logger := slog.New(slog.DiscardHandler)
	events := telemetry.NewEvents(logger, 2, "proxy")
	defer events.Close()
	h := instrumentedHandler(registry, firstEligiblePolicy{}, telemetryUpstreams(), tracker, nil, logger, metrics, events, false)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			metrics.render(io.Discard, registry, tracker, events)
		})
	}
	wg.Wait()
	var scrape strings.Builder
	metrics.render(&scrape, registry, tracker, events)
	for _, line := range []string{`sbr_proxy_requests_total{status="200"} 100`, `sbr_proxy_routed_requests_total{backend="a"} 100`, `sbr_proxy_backend_in_flight{backend="a"} 0`, `sbr_proxy_request_duration_seconds_count 100`} {
		if !strings.Contains(scrape.String(), line+"\n") {
			t.Errorf("missing metric %s", line)
		}
	}
}

func BenchmarkProxyTelemetry(b *testing.B) {
	upstreams := telemetryUpstreams()
	for _, mode := range []string{"disabled", "metrics", "metrics_and_events"} {
		b.Run(mode, func(b *testing.B) {
			registry := &Registry{backends: []Backend{{ID: "a"}}}
			tracker := newRequestTracker(registry.Snapshot())
			var m *proxyMetrics
			var e *telemetry.Events
			if mode != "disabled" {
				m = newProxyMetrics(registry.Snapshot())
			}
			if mode == "metrics_and_events" {
				e = telemetry.NewEvents(slog.New(slog.NewJSONHandler(io.Discard, nil)), 1, "proxy")
				defer e.Close()
			}
			h := instrumentedHandler(registry, firstEligiblePolicy{}, upstreams, tracker, nil, slog.New(slog.DiscardHandler), m, e, false)
			req := httptest.NewRequest(http.MethodGet, "/items", nil)
			b.ReportAllocs()
			for b.Loop() {
				h.ServeHTTP(httptest.NewRecorder(), req)
			}
			if e != nil {
				b.ReportMetric(float64(e.Dropped())/float64(b.N), "drops/op")
			}
		})
	}
}
