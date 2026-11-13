package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestHealthEndpoint(t *testing.T) {
	tests := []struct {
		method string
		status int
		body   string
		allow  string
	}{
		{http.MethodGet, http.StatusOK, "ok\n", ""},
		{http.MethodHead, http.StatusOK, "", ""},
		{http.MethodPost, http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodPut, http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodDelete, http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodPatch, http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
		{http.MethodOptions, http.StatusMethodNotAllowed, "Method Not Allowed\n", "GET, HEAD"},
	}

	handler := Handler("backend-a")
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, "/healthz", nil)
			handler.ServeHTTP(recorder, req)

			if recorder.Code != tt.status {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.status)
			}
			if got := recorder.Body.String(); got != tt.body {
				t.Fatalf("body = %q, want %q", got, tt.body)
			}
			if got := recorder.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
				t.Errorf("Content-Type = %q, want text/plain; charset=utf-8", got)
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
			if got := recorder.Header().Get("Allow"); got != tt.allow {
				t.Errorf("Allow = %q, want %q", got, tt.allow)
			}
		})
	}
}

func TestApplicationHandler(t *testing.T) {
	const backendID = "backend-test"
	handler := Handler(backendID)

	tests := []struct {
		method   string
		url      string
		wantPath string
	}{
		{http.MethodGet, "/", "/"},
		{http.MethodPost, "/api/items", "/api/items"},
		{http.MethodPut, "/api/items/42", "/api/items/42"},
		{http.MethodPatch, "/api/items/42", "/api/items/42"},
		{http.MethodDelete, "/api/items/42", "/api/items/42"},
		{http.MethodHead, "/api/items", "/api/items"},
		{http.MethodGet, "/healthz/", "/healthz/"},
		{http.MethodGet, "/api/items?tag=go&limit=10", "/api/items"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.url, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.url, nil)
			handler.ServeHTTP(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", recorder.Code)
			}
			if got := recorder.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}

			if tt.method == http.MethodHead {
				if recorder.Body.Len() != 0 {
					t.Fatalf("HEAD response wrote body: %q", recorder.Body.String())
				}
				return
			}

			var payload response
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("decode JSON response: %v, raw: %q", err, recorder.Body.String())
			}

			if payload.BackendID != backendID {
				t.Errorf("BackendID = %q, want %q", payload.BackendID, backendID)
			}
			if payload.Method != tt.method {
				t.Errorf("Method = %q, want %q", payload.Method, tt.method)
			}
			if payload.Path != tt.wantPath {
				t.Errorf("Path = %q, want %q", payload.Path, tt.wantPath)
			}
		})
	}
}

func TestBackendIdentity(t *testing.T) {
	for _, id := range []string{"backend-a", "backend-b", "backend-c"} {
		handler := Handler(id)
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/identity", nil)
		handler.ServeHTTP(recorder, req)

		var payload response
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
		if payload.BackendID != id {
			t.Fatalf("handler(%q) returned backend_id = %q", id, payload.BackendID)
		}
	}
}

func TestConcurrentRequests(t *testing.T) {
	handler := Handler("backend-concurrent")
	const concurrency = 16
	const requestsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func(workerID int) {
			defer wg.Done()
			for j := range requestsPerWorker {
				if j%2 == 0 {
					rec := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
					handler.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
						t.Errorf("healthz failed: %d %q", rec.Code, rec.Body.String())
					}
				} else {
					rec := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodPost, "/api/data", io.NopCloser(stringsReader("payload")))
					handler.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("api request failed: %d", rec.Code)
					}
					var resp response
					if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
						t.Errorf("json unmarshal failed: %v", err)
					}
					if resp.BackendID != "backend-concurrent" || resp.Method != http.MethodPost || resp.Path != "/api/data" {
						t.Errorf("unexpected response: %+v", resp)
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

func stringsReader(s string) io.Reader {
	return io.NopCloser(io.LimitReader(newSlowReader(s), int64(len(s))))
}

type dummyReader struct {
	data string
	off  int
}

func newSlowReader(s string) *dummyReader {
	return &dummyReader{data: s}
}

func (r *dummyReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}
