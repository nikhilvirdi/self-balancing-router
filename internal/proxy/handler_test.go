package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"testing"
)

type selectorFunc func([]Backend) (Backend, bool)

func (f selectorFunc) Select(candidates []Backend) (Backend, bool) {
	return f(candidates)
}

func TestHandlerRejectsInvalidPolicySelection(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	setHealth(t, registry, "b", Unhealthy)
	all := registry.Snapshot()
	tracker := newRequestTracker(all)
	upstream := &httputil.ReverseProxy{Rewrite: func(*httputil.ProxyRequest) {
		t.Error("invalid selection was forwarded")
	}}
	upstreams := map[string]*httputil.ReverseProxy{"a": upstream, "b": upstream}
	for _, tt := range []struct {
		name     string
		backend  Backend
		selected bool
	}{
		{"unknown", Backend{ID: "unknown"}, true},
		{"unhealthy", all[1], true},
		{"modified snapshot", Backend{ID: "a", URL: "http://different:80"}, true},
		{"declined", Backend{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := selectorFunc(func([]Backend) (Backend, bool) { return tt.backend, tt.selected })
			observer := func(Observation) { t.Error("invalid selection produced feedback") }
			handle := handler(registry, policy, upstreams, tracker, observer, slog.New(slog.DiscardHandler))
			recorder := httptest.NewRecorder()
			handle.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/items", nil))
			if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != "internal server error\n" {
				t.Fatalf("response = %d %q", recorder.Code, recorder.Body)
			}
			assertInFlight(t, tracker, "a", 0)
			assertInFlight(t, tracker, "b", 0)
		})
	}
}

func TestHandler(t *testing.T) {
	tests := []struct {
		method string
		path   string
		status int
		body   string
		allow  string
	}{
		{http.MethodGet, "/livez", http.StatusOK, "ok\n", ""},
		{http.MethodGet, "/readyz", http.StatusServiceUnavailable, "request routing is unavailable\n", ""},
		{http.MethodPost, "/livez", http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodOptions, "/readyz", http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodGet, "/", http.StatusServiceUnavailable, "request routing is unavailable\n", ""},
		{http.MethodPost, "/api/items", http.StatusServiceUnavailable, "request routing is unavailable\n", ""},
		{http.MethodGet, "/livez/", http.StatusServiceUnavailable, "request routing is unavailable\n", ""},
		{http.MethodGet, "/api/../livez", http.StatusServiceUnavailable, "request routing is unavailable\n", ""},
	}
	handle := handler(newTestRegistry(t), firstEligiblePolicy{}, nil, newRequestTracker(nil), nil, slog.New(slog.DiscardHandler))
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handle.ServeHTTP(response, httptest.NewRequest(tt.method, tt.path, nil))
			if response.Code != tt.status || response.Body.String() != tt.body {
				t.Fatalf("response = %d %q, want %d %q", response.Code, response.Body.String(), tt.status, tt.body)
			}
			for key, want := range map[string]string{
				"Content-Type":  "text/plain; charset=utf-8",
				"Cache-Control": "no-store",
				"Allow":         tt.allow,
			} {
				if got := response.Header().Get(key); got != want {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
		})
	}
}
