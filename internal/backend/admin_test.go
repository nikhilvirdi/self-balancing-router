package backend

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testAdminToken = "test-secret-token-123456"

func newTestAdminServer(model *DelayModel) (*httptest.Server, *http.Client) {
	handler := AdminHandler(model, testAdminToken)
	ts := httptest.NewServer(handler)
	return ts, ts.Client()
}

func TestAdminAuthentication(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	tests := []struct {
		name       string
		header     string
		query      string
		wantStatus int
	}{
		{
			name:       "missing authorization header",
			header:     "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "basic auth scheme instead of bearer",
			header:     "Basic dXNlcjpwYXNz",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid bearer token",
			header:     "Bearer wrong-token-12345678",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "query param token is rejected",
			header:     "",
			query:      "?token=" + testAdminToken,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "valid bearer token accepted",
			header:     "Bearer " + testAdminToken,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", ts.URL+"/admin/v1/workload"+tt.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()

			if res.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", res.StatusCode, tt.wantStatus)
			}
			if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", cc)
			}

			body, _ := io.ReadAll(res.Body)
			if strings.Contains(string(body), testAdminToken) {
				t.Errorf("response body leaks admin token: %q", string(body))
			}
		})
	}
}

func TestAdminGetWorkload(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest("GET", ts.URL+"/admin/v1/workload", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/v1/workload status = %d, want 200", res.StatusCode)
	}

	var data adminWorkloadResponse
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode workload response: %v", err)
	}

	if data.ProcessingDelay != "20ms" || data.ProcessingJitter != "5ms" {
		t.Errorf("current delay mismatch: got delay=%s jitter=%s, want 20ms and 5ms", data.ProcessingDelay, data.ProcessingJitter)
	}
	if data.BaselineProcessingDelay != "20ms" || data.BaselineProcessingJitter != "5ms" {
		t.Errorf("baseline mismatch: got delay=%s jitter=%s, want 20ms and 5ms", data.BaselineProcessingDelay, data.BaselineProcessingJitter)
	}
}

func TestAdminPutWorkload(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	// 1. Valid update
	payload := `{"processing_delay":"150ms","processing_jitter":"25ms"}`
	req, _ := http.NewRequest("PUT", ts.URL+"/admin/v1/workload", strings.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		t.Fatalf("PUT /admin/v1/workload status = %d, want 200. Body: %s", res.StatusCode, string(body))
	}

	var updated adminWorkloadResponse
	_ = json.NewDecoder(res.Body).Decode(&updated)
	_ = res.Body.Close()

	if updated.ProcessingDelay != "150ms" || updated.ProcessingJitter != "25ms" {
		t.Errorf("updated response mismatch: got %s, %s", updated.ProcessingDelay, updated.ProcessingJitter)
	}
	if updated.BaselineProcessingDelay != "20ms" || updated.BaselineProcessingJitter != "5ms" {
		t.Errorf("baseline altered: got %s, %s", updated.BaselineProcessingDelay, updated.BaselineProcessingJitter)
	}

	// Verify model itself was updated
	mCfg := model.Workload()
	if mCfg.ProcessingDelay != 150*time.Millisecond || mCfg.ProcessingJitter != 25*time.Millisecond {
		t.Fatalf("model state mismatch: got %+v", mCfg)
	}

	// 2. Invalid updates (all-or-nothing, must return 400 and preserve existing configuration)
	invalidPayloads := []struct {
		name string
		body string
	}{
		{"missing jitter", `{"processing_delay":"100ms"}`},
		{"missing delay", `{"processing_jitter":"10ms"}`},
		{"unknown field", `{"processing_delay":"100ms","processing_jitter":"10ms","extra":"foo"}`},
		{"invalid duration", `{"processing_delay":"not-valid","processing_jitter":"10ms"}`},
		{"negative delay", `{"processing_delay":"-10ms","processing_jitter":"10ms"}`},
		{"negative jitter", `{"processing_delay":"10ms","processing_jitter":"-10ms"}`},
		{"excessive delay (>5m)", `{"processing_delay":"5m1s","processing_jitter":"0s"}`},
		{"excessive jitter (>5m)", `{"processing_delay":"0s","processing_jitter":"5m1s"}`},
		{"combined overflow (>10m)", `{"processing_delay":"5m","processing_jitter":"5m1s"}`},
		{"malformed json", `{"processing_delay":"100ms",`},
		{"trailing json object", `{"processing_delay":"100ms","processing_jitter":"10ms"}{}`},
		{"trailing garbage", `{"processing_delay":"100ms","processing_jitter":"10ms"} extra`},
	}

	for _, tt := range invalidPayloads {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("PUT", ts.URL+"/admin/v1/workload", strings.NewReader(tt.body))
			r.Header.Set("Authorization", "Bearer "+testAdminToken)
			r.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 for payload %s", resp.StatusCode, tt.body)
			}

			// Ensure active config remains untouched (150ms / 25ms)
			active := model.Workload()
			if active.ProcessingDelay != 150*time.Millisecond || active.ProcessingJitter != 25*time.Millisecond {
				t.Fatalf("model was mutated by invalid payload %s: %+v", tt.body, active)
			}
		})
	}

	// 3. Oversized payload (> 4096 bytes) should return 413
	oversizedBody := `{"processing_delay":"10ms","processing_jitter":"5ms","padding":"` + strings.Repeat("x", 5000) + `"}`
	r, _ := http.NewRequest("PUT", ts.URL+"/admin/v1/workload", strings.NewReader(oversizedBody))
	r.Header.Set("Authorization", "Bearer "+testAdminToken)
	r.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, want 413", resp.StatusCode)
	}
}

func TestAdminResetWorkload(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	// Degrade model first
	if err := model.Update(200*time.Millisecond, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Call POST /admin/v1/workload/reset
	req, _ := http.NewRequest("POST", ts.URL+"/admin/v1/workload/reset", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("reset status = %d, want 200", res.StatusCode)
	}

	var data adminWorkloadResponse
	_ = json.NewDecoder(res.Body).Decode(&data)

	if data.ProcessingDelay != "20ms" || data.ProcessingJitter != "5ms" {
		t.Errorf("reset response mismatch: got %s, %s", data.ProcessingDelay, data.ProcessingJitter)
	}

	// Verify model itself was restored
	mCfg := model.Workload()
	if mCfg.ProcessingDelay != 20*time.Millisecond || mCfg.ProcessingJitter != 5*time.Millisecond {
		t.Fatalf("model state after reset mismatch: got %+v", mCfg)
	}

	// Repeat reset (idempotency)
	req2, _ := http.NewRequest("POST", ts.URL+"/admin/v1/workload/reset", nil)
	req2.Header.Set("Authorization", "Bearer "+testAdminToken)
	res2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("repeated reset status = %d, want 200", res2.StatusCode)
	}
}

func TestAdminMethodNotAllowed(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	// DELETE /admin/v1/workload -> 405 with Allow: GET, PUT
	req, _ := http.NewRequest("DELETE", ts.URL+"/admin/v1/workload", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("DELETE status = %d, want 405", res.StatusCode)
	}
	if allow := res.Header.Get("Allow"); allow != "GET, PUT" {
		t.Errorf("Allow header = %q, want 'GET, PUT'", allow)
	}

	// GET /admin/v1/workload/reset -> 405 with Allow: POST
	req2, _ := http.NewRequest("GET", ts.URL+"/admin/v1/workload/reset", nil)
	req2.Header.Set("Authorization", "Bearer "+testAdminToken)
	res2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET reset status = %d, want 405", res2.StatusCode)
	}
	if allow := res2.Header.Get("Allow"); allow != "POST" {
		t.Errorf("Allow header = %q, want 'POST'", allow)
	}
}

func TestAdminUnknownPath(t *testing.T) {
	model := NewDelayModel(20*time.Millisecond, 5*time.Millisecond, 1)
	ts, client := newTestAdminServer(model)
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest("GET", ts.URL+"/admin/v1/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown path status = %d, want 404", res.StatusCode)
	}
}

func TestApplicationListenerIsolation(t *testing.T) {
	pool := NewPool(2, 4, nil)
	t.Cleanup(pool.Close)

	appHandler := Handler("backend-isolation", pool)
	ts := httptest.NewServer(appHandler)
	t.Cleanup(ts.Close)

	client := ts.Client()

	paths := []string{
		"/admin",
		"/admin/",
		"/admin/v1/workload",
		"/admin/v1/workload/reset",
		"/admin/other",
	}

	methods := []string{"GET", "POST", "PUT", "DELETE"}

	for _, p := range paths {
		for _, m := range methods {
			req, _ := http.NewRequest(m, ts.URL+p, bytes.NewBufferString("{}"))
			req.Header.Set("Authorization", "Bearer "+testAdminToken)

			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("app listener %s %s returned status %d, want 404", m, p, res.StatusCode)
			}
		}
	}
}

func TestAdminResponsivenessUnderApplicationSaturation(t *testing.T) {
	delayModel := NewDelayModel(100*time.Millisecond, 0, 1)
	pool := NewPool(1, 0, makeProcessor(delayModel))
	t.Cleanup(pool.Close)

	appHandler := Handler("backend-sat", pool)
	appTs := httptest.NewServer(appHandler)
	t.Cleanup(appTs.Close)

	adminHandler := AdminHandler(delayModel, testAdminToken)
	adminTs := httptest.NewServer(adminHandler)
	t.Cleanup(adminTs.Close)

	client := appTs.Client()

	// Occupy the single worker with a delayed application request
	appDone := make(chan struct{})
	go func() {
		defer close(appDone)
		res, _ := client.Get(appTs.URL + "/api/sat-test")
		if res != nil {
			_ = res.Body.Close()
		}
	}()

	// Wait for worker to become active
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// While application worker is busy:
	// 1. GET /admin/v1/workload responds immediately (< 30ms)
	adminStart := time.Now()
	adminReq, _ := http.NewRequest("GET", adminTs.URL+"/admin/v1/workload", nil)
	adminReq.Header.Set("Authorization", "Bearer "+testAdminToken)
	adminRes, err := client.Do(adminReq)
	if err != nil {
		t.Fatal(err)
	}
	defer adminRes.Body.Close()
	if elapsed := time.Since(adminStart); elapsed > 50*time.Millisecond {
		t.Fatalf("GET /admin took %v under saturation, want prompt response", elapsed)
	}
	if adminRes.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin status = %d, want 200", adminRes.StatusCode)
	}

	// 2. PUT /admin/v1/workload responds immediately (< 30ms)
	putStart := time.Now()
	putReq, _ := http.NewRequest("PUT", adminTs.URL+"/admin/v1/workload", strings.NewReader(`{"processing_delay":"40ms","processing_jitter":"10ms"}`))
	putReq.Header.Set("Authorization", "Bearer "+testAdminToken)
	putReq.Header.Set("Content-Type", "application/json")
	putRes, err := client.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	defer putRes.Body.Close()
	if elapsed := time.Since(putStart); elapsed > 50*time.Millisecond {
		t.Fatalf("PUT /admin took %v under saturation, want prompt response", elapsed)
	}
	if putRes.StatusCode != http.StatusOK {
		t.Fatalf("PUT /admin status = %d, want 200", putRes.StatusCode)
	}

	// 3. /healthz responds immediately (< 30ms)
	hStart := time.Now()
	hRes, err := client.Get(appTs.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer hRes.Body.Close()
	if elapsed := time.Since(hStart); elapsed > 50*time.Millisecond {
		t.Fatalf("GET /healthz took %v under saturation, want prompt response", elapsed)
	}
	if hRes.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", hRes.StatusCode)
	}

	// 4. Another application request gets 503 overloaded
	overloadRes, err := client.Get(appTs.URL + "/api/sat-test-2")
	if err != nil {
		t.Fatal(err)
	}
	defer overloadRes.Body.Close()
	if overloadRes.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("app request under overload = %d, want 503", overloadRes.StatusCode)
	}

	<-appDone
}

