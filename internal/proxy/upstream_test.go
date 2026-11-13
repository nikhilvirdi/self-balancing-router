package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestProxyForwardsHTTP(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
		for key, value := range map[string]string{
			"X-Method":             r.Method,
			"X-Request-URI":        r.RequestURI,
			"X-Upstream-Host":      r.Host,
			"X-Request-Type":       r.Header.Get("Content-Type"),
			"X-Request-Header":     r.Header.Get("X-End-To-End"),
			"X-Seen-Forwarded":     r.Header.Get("Forwarded"),
			"X-Seen-For":           r.Header.Get("X-Forwarded-For"),
			"X-Seen-Host":          r.Header.Get("X-Forwarded-Host"),
			"X-Seen-Proto":         r.Header.Get("X-Forwarded-Proto"),
			"X-Seen-Prefix":        r.Header.Get("X-Forwarded-Prefix"),
			"X-Seen-Connection":    r.Header.Get("X-Request-Hop"),
			"X-Seen-Accept-Coding": r.Header.Get("Accept-Encoding"),
			"Content-Type":         "application/octet-stream",
			"Cache-Control":        "public, max-age=10",
			"Connection":           "X-Response-Hop",
			"X-Response-Hop":       "upstream-only",
		} {
			w.Header().Set(key, value)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, "upstream:%s", body)
	}))
	t.Cleanup(upstream.Close)
	_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL + "/"})
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/"},
		{method: http.MethodPost, path: "/api/items?limit=10&tag=a&tag=b", body: `{"name":"item"}`},
		{method: http.MethodPut, path: "/api/nested/a%2Fb?name=one+two&encoded=%2F"},
		{method: http.MethodPatch, path: "/api//items/"},
		{method: http.MethodDelete, path: "/api/../items?flag"},
		{method: http.MethodHead, path: "/api/items"},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(t.Context(), tt.method, proxy.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Host = "proxy.example"
			for key, value := range map[string]string{
				"Content-Type":       "application/json",
				"X-End-To-End":       "preserved",
				"Forwarded":          "for=spoofed",
				"X-Forwarded-For":    "192.0.2.1",
				"X-Forwarded-Host":   "spoofed.example",
				"X-Forwarded-Proto":  "https",
				"X-Forwarded-Prefix": "/spoofed",
				"Accept-Encoding":    "identity",
				"Connection":         "X-Request-Hop",
				"X-Request-Hop":      "client-only",
			} {
				req.Header.Set(key, value)
			}
			res, body := doRequest(t, proxy.Client(), req)
			wantBody := "upstream:" + tt.body
			if tt.method == http.MethodHead {
				wantBody = ""
			}
			if res.StatusCode != http.StatusAccepted || body != wantBody {
				t.Fatalf("response = %d %q, want 202 %q", res.StatusCode, body, wantBody)
			}
			for key, want := range map[string]string{
				"X-Method":             tt.method,
				"X-Request-URI":        tt.path,
				"X-Upstream-Host":      upstream.Listener.Addr().String(),
				"X-Request-Type":       "application/json",
				"X-Request-Header":     "preserved",
				"X-Seen-Forwarded":     "",
				"X-Seen-For":           "127.0.0.1",
				"X-Seen-Host":          "proxy.example",
				"X-Seen-Proto":         "http",
				"X-Seen-Prefix":        "",
				"X-Seen-Connection":    "",
				"X-Seen-Accept-Coding": "identity",
				"Content-Type":         "application/octet-stream",
				"Cache-Control":        "public, max-age=10",
				"X-Response-Hop":       "",
			} {
				if got := res.Header.Get(key); got != want {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
		})
	}
}

func TestProxySelectionAndReadiness(t *testing.T) {
	var requests atomic.Int32
	makeBackend := func(id string) config.BackendConfig {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			_, _ = io.WriteString(w, id)
		}))
		t.Cleanup(upstream.Close)
		return config.BackendConfig{ID: id, URL: upstream.URL}
	}
	registry, proxy := startTestProxy(t, makeBackend("backend-z"), makeBackend("backend-a"))
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	assertResponse(t, proxy, http.MethodHead, "/readyz", 200, "")
	assertResponse(t, proxy, http.MethodPost, "/readyz", 405, "Method Not Allowed\n")
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-z")
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-z")
	setHealth(t, registry, "backend-z", Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-a")
	setHealth(t, registry, "backend-z", Healthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 200, "backend-z")
	setHealth(t, registry, "backend-z", Unhealthy)
	setHealth(t, registry, "backend-a", Unhealthy)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
	if got := requests.Load(); got != 4 {
		t.Fatalf("upstream received %d requests, want 4 ordinary requests", got)
	}
}

func TestProxyEmptyRegistry(t *testing.T) {
	_, proxy := startTestProxy(t)
	assertResponse(t, proxy, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/readyz", 503, "request routing is unavailable\n")
	assertResponse(t, proxy, http.MethodGet, "/livez", 200, "ok\n")
}

func TestProxyUpstreamApplicationErrors(t *testing.T) {
	for _, status := range []int{400, 404, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "upstream error")
			}))
			t.Cleanup(upstream.Close)
			_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
			assertResponse(t, proxy, http.MethodGet, "/items", status, "upstream error")
		})
	}
}

func TestProxyUnreachableBackend(t *testing.T) {
	listener := listenLoopback(t)
	address := listener.Addr().String()
	var fallbackRequests atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		fallbackRequests.Add(1)
	}))
	t.Cleanup(fallback.Close)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	registry, handler := testProxyHandler(t, logger,
		config.BackendConfig{ID: "unreachable", URL: "http://" + address},
		config.BackendConfig{ID: "fallback", URL: fallback.URL},
	)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	for range 2 {
		assertResponse(t, proxy, http.MethodGet, "/items", 502, "bad gateway\n")
	}
	assertResponse(t, proxy, http.MethodGet, "/readyz", 200, "ok\n")
	if fallbackRequests.Load() != 0 {
		t.Fatal("failed request was rerouted to another backend")
	}
	if backend, _ := registry.Lookup("unreachable"); backend.Health != Unknown {
		t.Fatalf("health changed after a transport failure: %+v", backend)
	}
	proxy.Close()
	if !strings.Contains(logs.String(), `"backend_id":"unreachable"`) || !strings.Contains(logs.String(), `"error":`) {
		t.Fatalf("missing structured upstream error: %s", logs.String())
	}
}

func TestProxyClientCancellation(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-t.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)
	_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		res, err := proxy.Client().Do(req)
		if res != nil {
			err = errors.Join(err, res.Body.Close())
		}
		result <- err
	}()
	waitFor(t, entered)
	cancel()
	if err := waitFor(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("client error = %v, want cancellation", err)
	}
	waitFor(t, canceled)
}

func TestProxyReusesTransportAcrossBackends(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.RemoteAddr)
	}))
	t.Cleanup(upstream.Close)
	registry, proxy := startTestProxy(t,
		config.BackendConfig{ID: "backend-a", URL: upstream.URL},
		config.BackendConfig{ID: "backend-b", URL: upstream.URL},
	)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxy.URL+"/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, address := doRequest(t, proxy.Client(), req)
	setHealth(t, registry, "backend-a", Unhealthy)
	second, reused := doRequest(t, proxy.Client(), req)
	if first.StatusCode != 200 || second.StatusCode != 200 || address == "" || reused != address {
		t.Fatalf("upstream connections = %d %q and %d %q, want the same connection", first.StatusCode, address, second.StatusCode, reused)
	}
}

func TestProxyDrainsActiveUpstreamRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)
	registry, handle := testProxyHandler(t, slog.New(slog.DiscardHandler),
		config.BackendConfig{ID: "backend-a", URL: upstream.URL},
	)
	listener := &observedListener{Listener: listenLoopback(t), closed: make(chan struct{})}
	stop, done, request := startRequest(t, listener, &http.Server{Handler: handle}, testTimeout)
	waitFor(t, entered)
	setHealth(t, registry, "backend-a", Unhealthy)
	stop()
	waitFor(t, listener.closed)
	select {
	case err := <-done:
		t.Fatalf("serve returned before the active upstream request finished: %v", err)
	default:
	}
	close(release)
	if result := waitFor(t, request); result.err != nil || result.body != "finished" {
		t.Fatalf("active proxied request = %+v, want completed response", result)
	}
	if err := waitFor(t, done); err != nil {
		t.Fatalf("serve() error = %v, want clean shutdown", err)
	}
}

func startTestProxy(t *testing.T, backends ...config.BackendConfig) (*Registry, *httptest.Server) {
	t.Helper()
	registry, handler := testProxyHandler(t, slog.New(slog.DiscardHandler), backends...)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return registry, server
}

func testProxyHandler(t *testing.T, logger *slog.Logger, backends ...config.BackendConfig) (*Registry, http.Handler) {
	t.Helper()
	return testProxyWithTransport(t, newTransport(3*time.Second, 30*time.Second), logger, backends...)
}

func testProxyWithTransport(t *testing.T, transport *http.Transport, logger *slog.Logger, backends ...config.BackendConfig) (*Registry, http.Handler) {
	t.Helper()
	t.Cleanup(transport.CloseIdleConnections)
	registry, err := NewRegistry(backends)
	if err != nil {
		t.Fatal(err)
	}
	upstreams, err := newUpstreams(registry.Snapshot(), transport, logger)
	if err != nil {
		t.Fatal(err)
	}
	return registry, handler(registry, upstreams)
}

func assertResponse(t *testing.T, proxy *httptest.Server, method, path string, status int, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, proxy.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, got := doRequest(t, proxy.Client(), req)
	if res.StatusCode != status || got != body {
		t.Fatalf("%s %s = %d %q, want %d %q", method, path, res.StatusCode, got, status, body)
	}
}

func doRequest(t *testing.T, client *http.Client, req *http.Request) (*http.Response, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(req.Context(), testTimeout)
	defer cancel()
	res, err := client.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	if err := errors.Join(err, res.Body.Close()); err != nil {
		t.Fatal(err)
	}
	return res, string(body)
}
