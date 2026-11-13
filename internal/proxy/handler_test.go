package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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
	handle := handler(newTestRegistry(t), nil)
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
