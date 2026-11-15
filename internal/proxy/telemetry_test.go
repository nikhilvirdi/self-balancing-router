package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

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
	h := instrumentedHandler(registry, firstEligiblePolicy{}, upstreams, tracker, nil, logger, metrics, events)
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
	h := instrumentedHandler(registry, firstEligiblePolicy{}, telemetryUpstreams(), tracker, nil, logger, metrics, events)
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
			h := instrumentedHandler(registry, firstEligiblePolicy{}, upstreams, tracker, nil, slog.New(slog.DiscardHandler), m, e)
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
