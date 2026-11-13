package backend

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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

	pool := NewPool(2, 4, nil)
	t.Cleanup(pool.Close)
	handler := Handler("backend-a", pool)

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
	pool := NewPool(4, 32, nil)
	t.Cleanup(pool.Close)
	handler := Handler(backendID, pool)

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
		pool := NewPool(2, 4, nil)
		handler := Handler(id, pool)
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
		pool.Close()
	}
}

func TestHandlerOverloadResponse(t *testing.T) {
	block := make(chan struct{})
	proc := func(ctx context.Context, req requestInfo) (response, error) {
		<-block
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(1, 0, proc)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
		pool.Close()
	})

	handler := Handler("backend-ovl", pool)

	// Start 1 request to occupy the single worker
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/items", nil)
		handler.ServeHTTP(rec, req)
	}()

	// Wait until worker is active
	for {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// 1. Next GET request must receive 503 Overloaded
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/items", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}

	var errResp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal error JSON: %v, raw: %q", err, rec.Body.String())
	}
	if errResp.Error != "backend overloaded" {
		t.Fatalf("error = %q, want %q", errResp.Error, "backend overloaded")
	}

	// 2. HEAD request under overload must receive 503 with empty body
	headRec := httptest.NewRecorder()
	headReq := httptest.NewRequest(http.MethodHead, "/api/items", nil)
	handler.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("HEAD status = %d, want 503", headRec.Code)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("HEAD response wrote body: %q", headRec.Body.String())
	}
}

func TestHealthEndpointIsolationUnderOverload(t *testing.T) {
	block := make(chan struct{})
	proc := func(ctx context.Context, req requestInfo) (response, error) {
		<-block
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(1, 0, proc)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
		pool.Close()
	})

	handler := Handler("backend-iso", pool)

	// Block worker
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/items", nil)
		handler.ServeHTTP(rec, req)
	}()

	for {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Health endpoint must succeed despite saturated pool
	healthRec := httptest.NewRecorder()
	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(healthRec, healthReq)

	if healthRec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200 under overload", healthRec.Code)
	}
	if healthRec.Body.String() != "ok\n" {
		t.Fatalf("/healthz body = %q, want ok\\n", healthRec.Body.String())
	}

	// HEAD /healthz must also succeed
	headRec := httptest.NewRecorder()
	headReq := httptest.NewRequest(http.MethodHead, "/healthz", nil)
	handler.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK || headRec.Body.Len() != 0 {
		t.Fatalf("HEAD /healthz = %d %q, want 200 empty", headRec.Code, headRec.Body.String())
	}
}

func TestConcurrentRequests(t *testing.T) {
	pool := NewPool(8, 32, nil)
	t.Cleanup(pool.Close)
	handler := Handler("backend-concurrent", pool)

	const concurrency = 16
	const requestsPerWorker = 30

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
					if rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable {
						t.Errorf("unexpected status: %d", rec.Code)
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
