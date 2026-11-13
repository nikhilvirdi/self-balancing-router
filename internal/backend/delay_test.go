package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestDelayModelSampleBounds(t *testing.T) {
	fixed := 10 * time.Millisecond
	jitter := 20 * time.Millisecond
	model := NewDelayModel(fixed, jitter, 42)

	for i := 0; i < 100; i++ {
		sample := model.Sample()
		if sample < fixed || sample > fixed+jitter {
			t.Fatalf("Sample() = %v, want within [%v, %v]", sample, fixed, fixed+jitter)
		}
	}

	// Test zero jitter produces fixed delay exactly
	fixedOnly := NewDelayModel(50*time.Millisecond, 0, 1)
	for i := 0; i < 50; i++ {
		if sample := fixedOnly.Sample(); sample != 50*time.Millisecond {
			t.Fatalf("Sample() with zero jitter = %v, want %v", sample, 50*time.Millisecond)
		}
	}

	// Test zero fixed and zero jitter
	zeroModel := NewDelayModel(0, 0, 1)
	if sample := zeroModel.Sample(); sample != 0 {
		t.Fatalf("Sample() with zero delay = %v, want 0", sample)
	}

	// Test nil model returns 0
	var nilModel *DelayModel
	if sample := nilModel.Sample(); sample != 0 {
		t.Fatalf("nil DelayModel.Sample() = %v, want 0", sample)
	}
}

func TestDelayModelReproducibility(t *testing.T) {
	fixed := 5 * time.Millisecond
	jitter := 25 * time.Millisecond
	seed := int64(12345)

	m1 := NewDelayModel(fixed, jitter, seed)
	m2 := NewDelayModel(fixed, jitter, seed)

	for i := 0; i < 50; i++ {
		s1 := m1.Sample()
		s2 := m2.Sample()
		if s1 != s2 {
			t.Fatalf("iteration %d: m1 sample %v != m2 sample %v with identical seed %d", i, s1, s2, seed)
		}
	}

	// Different seeds should produce at least some different samples
	m3 := NewDelayModel(fixed, jitter, 99999)
	different := false
	for i := 0; i < 50; i++ {
		if m1.Sample() != m3.Sample() {
			different = true
			break
		}
	}
	if !different {
		t.Fatalf("expected different sample sequences with different seeds")
	}
}

func TestDelayModelConcurrentSampling(t *testing.T) {
	fixed := 10 * time.Millisecond
	jitter := 30 * time.Millisecond
	model := NewDelayModel(fixed, jitter, 7)

	const goroutines = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				s := model.Sample()
				if s < fixed || s > fixed+jitter {
					t.Errorf("concurrent sample %v outside [%v, %v]", s, fixed, fixed+jitter)
				}
			}
		}()
	}
	wg.Wait()
}

func TestWaitDelay(t *testing.T) {
	// Zero delay completes promptly
	ctx := t.Context()
	if err := waitDelay(ctx, 0); err != nil {
		t.Fatalf("waitDelay(0) err = %v, want nil", err)
	}
	if err := waitDelay(ctx, -5*time.Millisecond); err != nil {
		t.Fatalf("waitDelay(negative) err = %v, want nil", err)
	}

	// Canceled context before start returns immediately
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	start := time.Now()
	err := waitDelay(canceledCtx, 5*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitDelay() with canceled context = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("waitDelay() took %v on pre-canceled context, want immediate return", elapsed)
	}

	// Cancellation during wait terminates promptly
	activeCtx, cancelActive := context.WithCancel(ctx)
	errChan := make(chan error, 1)
	go func() {
		errChan <- waitDelay(activeCtx, 5*time.Second)
	}()
	time.Sleep(10 * time.Millisecond)
	cancelActive()

	select {
	case err := <-errChan:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waitDelay() cancelled during wait = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitDelay() did not terminate promptly upon cancellation")
	}
}

func TestWorkerPoolDelayOccupancyAndOverload(t *testing.T) {
	// Pool with 1 worker and 1 queue slot.
	// Processing delay: 60ms.
	delayModel := NewDelayModel(60*time.Millisecond, 0, 1)
	pool := NewPool(1, 1, makeProcessor(delayModel))
	t.Cleanup(pool.Close)

	ctx := t.Context()

	// Task 1 occupies the worker.
	done1 := make(chan workResult, 1)
	go func() {
		resp, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/1"})
		done1 <- workResult{resp: resp, err: err}
	}()

	// Wait until task 1 is active.
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	active, queued := pool.Stats()
	if active != 1 {
		t.Fatalf("active = %d, want 1", active)
	}

	// Task 2 fills the queue.
	done2 := make(chan workResult, 1)
	go func() {
		resp, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/2"})
		done2 <- workResult{resp: resp, err: err}
	}()

	// Wait until task 2 is queued.
	for i := 0; i < 50; i++ {
		_, queued = pool.Stats()
		if queued == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if queued != 1 {
		t.Fatalf("queued = %d, want 1", queued)
	}

	// Task 3 must be rejected immediately with ErrOverloaded.
	start := time.Now()
	_, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/3"})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("task 3 err = %v, want ErrOverloaded", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("overloaded rejection took %v, want prompt rejection", elapsed)
	}

	// Both task 1 and task 2 should complete successfully.
	res1 := <-done1
	if res1.err != nil || res1.resp.Path != "/1" {
		t.Fatalf("task 1 failed: %+v", res1)
	}
	res2 := <-done2
	if res2.err != nil || res2.resp.Path != "/2" {
		t.Fatalf("task 2 failed: %+v", res2)
	}
}

func TestOverloadDoesNotConsumePRNG(t *testing.T) {
	// Reference delay model to observe RNG sequence.
	refModel := NewDelayModel(10*time.Millisecond, 100*time.Millisecond, 42)
	refSample1 := refModel.Sample()
	refSample2 := refModel.Sample()

	// Pool model with identical seed.
	poolModel := NewDelayModel(10*time.Millisecond, 100*time.Millisecond, 42)
	pool := NewPool(1, 0, makeProcessor(poolModel))
	t.Cleanup(pool.Close)

	ctx := t.Context()

	// Task 1 occupies the single worker (consumes sample 1).
	task1Done := make(chan workResult, 1)
	go func() {
		resp, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/1"})
		task1Done <- workResult{resp: resp, err: err}
	}()

	// Wait for task 1 to become active.
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Task 2 is rejected because pool is full.
	_, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/2"})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("task 2 err = %v, want ErrOverloaded", err)
	}

	// Wait for task 1 to finish.
	res1 := <-task1Done
	if res1.err != nil {
		t.Fatalf("task 1 err: %v", res1.err)
	}

	// The next sample from poolModel must match refSample2 (not skipped).
	nextSample := poolModel.Sample()
	if nextSample != refSample2 {
		t.Fatalf("poolModel sample = %v, want %v (rejected request consumed a random sample)", nextSample, refSample2)
	}
	_ = refSample1
}

func TestCanceledActiveRequestTerminatesPromptly(t *testing.T) {
	// Worker pool with 1 worker and 2-second delay.
	delayModel := NewDelayModel(2*time.Second, 0, 1)
	pool := NewPool(1, 1, makeProcessor(delayModel))
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	resultChan := make(chan workResult, 1)
	start := time.Now()
	go func() {
		resp, err := pool.Submit(ctx, requestInfo{BackendID: "b1", Method: "GET", Path: "/cancel-test"})
		resultChan <- workResult{resp: resp, err: err}
	}()

	// Wait for worker to become active.
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Cancel the request context while the delay is in progress.
	cancel()

	select {
	case res := <-resultChan:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("result error = %v, want context.Canceled", res.err)
		}
		if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
			t.Fatalf("canceled delay took %v, want prompt return (< 300ms)", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for canceled request to terminate")
	}

	// Ensure the worker slot is released for a subsequent request.
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	active, _ := pool.Stats()
	if active != 0 {
		t.Fatalf("active workers = %d after cancellation, want 0", active)
	}
}

func TestHTTPDelayedHandlerAndHealthzIsolation(t *testing.T) {
	// Configure pool with 1 worker, 0 queue capacity, and 100ms processing delay.
	delayModel := NewDelayModel(100*time.Millisecond, 0, 1)
	pool := NewPool(1, 0, makeProcessor(delayModel))
	t.Cleanup(pool.Close)

	handler := Handler("backend-delay-test", pool)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := ts.Client()

	// 1. Submit an application request in background that takes 100ms.
	appDone := make(chan struct{})
	var appStatus int
	var appBody []byte
	go func() {
		defer close(appDone)
		res, err := client.Get(ts.URL + "/api/v1/work")
		if err != nil {
			t.Errorf("app request error: %v", err)
			return
		}
		appStatus = res.StatusCode
		appBody, _ = io.ReadAll(res.Body)
		_ = res.Body.Close()
	}()

	// Wait for worker to become active.
	for i := 0; i < 50; i++ {
		active, _ := pool.Stats()
		if active == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 2. While worker is delayed, /healthz GET must respond immediately (< 30ms).
	hStart := time.Now()
	hRes, err := client.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz error: %v", err)
	}
	hBody, _ := io.ReadAll(hRes.Body)
	_ = hRes.Body.Close()
	if hElapsed := time.Since(hStart); hElapsed > 50*time.Millisecond {
		t.Fatalf("GET /healthz took %v during active delay, want prompt response", hElapsed)
	}
	if hRes.StatusCode != http.StatusOK || string(hBody) != "ok\n" {
		t.Fatalf("GET /healthz = %d %q, want 200 ok\\n", hRes.StatusCode, string(hBody))
	}

	// 3. /healthz HEAD must also respond immediately.
	headRes, err := client.Head(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("HEAD /healthz error: %v", err)
	}
	_ = headRes.Body.Close()
	if headRes.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /healthz status = %d, want 200", headRes.StatusCode)
	}

	// 4. A second application request while worker is busy and queue is 0 must receive 503 promptly.
	overloadStart := time.Now()
	overloadRes, err := client.Get(ts.URL + "/api/v1/work")
	if err != nil {
		t.Fatalf("overload request error: %v", err)
	}
	overloadBody, _ := io.ReadAll(overloadRes.Body)
	_ = overloadRes.Body.Close()
	if oElapsed := time.Since(overloadStart); oElapsed > 50*time.Millisecond {
		t.Fatalf("overload response took %v, want prompt 503 rejection", oElapsed)
	}
	if overloadRes.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("overload status = %d, want 503", overloadRes.StatusCode)
	}
	var errResp errorResponse
	if err := json.Unmarshal(overloadBody, &errResp); err != nil || errResp.Error != "backend overloaded" {
		t.Fatalf("overload payload = %q, want backend overloaded", string(overloadBody))
	}

	// 5. Wait for first application request to finish and verify JSON payload.
	<-appDone
	if appStatus != http.StatusOK {
		t.Fatalf("first app request status = %d, want 200", appStatus)
	}
	var respPayload response
	if err := json.Unmarshal(appBody, &respPayload); err != nil {
		t.Fatalf("unmarshal app response %q: %v", string(appBody), err)
	}
	if respPayload.BackendID != "backend-delay-test" || respPayload.Method != "GET" || respPayload.Path != "/api/v1/work" {
		t.Fatalf("unexpected app response payload: %+v", respPayload)
	}
}

func TestMultipleBackendInstancesIndependentDelays(t *testing.T) {
	modelA := NewDelayModel(10*time.Millisecond, 5*time.Millisecond, 100)
	modelB := NewDelayModel(40*time.Millisecond, 10*time.Millisecond, 200)

	poolA := NewPool(2, 4, makeProcessor(modelA))
	t.Cleanup(poolA.Close)
	poolB := NewPool(2, 4, makeProcessor(modelB))
	t.Cleanup(poolB.Close)

	handlerA := Handler("backend-a", poolA)
	handlerB := Handler("backend-b", poolB)

	tsA := httptest.NewServer(handlerA)
	t.Cleanup(tsA.Close)
	tsB := httptest.NewServer(handlerB)
	t.Cleanup(tsB.Close)

	ctx := t.Context()
	client := tsA.Client()

	reqA, _ := http.NewRequestWithContext(ctx, "GET", tsA.URL+"/api/a", nil)
	resA, err := client.Do(reqA)
	if err != nil {
		t.Fatalf("backend A request failed: %v", err)
	}
	var dataA response
	_ = json.NewDecoder(resA.Body).Decode(&dataA)
	_ = resA.Body.Close()

	reqB, _ := http.NewRequestWithContext(ctx, "GET", tsB.URL+"/api/b", nil)
	resB, err := client.Do(reqB)
	if err != nil {
		t.Fatalf("backend B request failed: %v", err)
	}
	var dataB response
	_ = json.NewDecoder(resB.Body).Decode(&dataB)
	_ = resB.Body.Close()

	if dataA.BackendID != "backend-a" || dataA.Path != "/api/a" {
		t.Fatalf("backend A response mismatch: %+v", dataA)
	}
	if dataB.BackendID != "backend-b" || dataB.Path != "/api/b" {
		t.Fatalf("backend B response mismatch: %+v", dataB)
	}
}

func TestServerGracefulShutdownWithDelays(t *testing.T) {
	addressChan := make(chan string, 1)
	logger := slog.New(&listenLogHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		address: addressChan,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := Config{
		Config:           config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:               "backend-shutdown-delay",
		ListenAddr:       "127.0.0.1:0",
		ShutdownTimeout:  2 * time.Second,
		Workers:          2,
		QueueCapacity:    4,
		ProcessingDelay:  50 * time.Millisecond,
		ProcessingJitter: 0,
		DelaySeed:        1,
	}

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, logger)
	}()

	addr := waitFor(t, addressChan)
	client := &http.Client{Timeout: 5 * time.Second}

	// Start a delayed request
	reqDone := make(chan requestResult, 1)
	go func() {
		res, err := client.Get("http://" + addr + "/api/shutdown-test")
		if err != nil {
			reqDone <- requestResult{err: err}
			return
		}
		body, err := io.ReadAll(res.Body)
		reqDone <- requestResult{body: string(body), err: errors.Join(err, res.Body.Close())}
	}()

	// Allow request to be admitted and worker to begin delay
	time.Sleep(10 * time.Millisecond)

	// Initiate graceful shutdown
	cancel()

	// Delayed request should complete successfully within shutdown timeout
	result := waitFor(t, reqDone)
	if result.err != nil {
		t.Fatalf("delayed request failed during graceful shutdown: %v", result.err)
	}
	var payload response
	if err := json.Unmarshal([]byte(result.body), &payload); err != nil {
		t.Fatalf("failed to parse response: %v, body: %q", err, result.body)
	}
	if payload.BackendID != "backend-shutdown-delay" {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	// Server shutdown completes cleanly
	if err := waitFor(t, done); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	verifyListenerClosed(t, addr)
}

func TestServerShutdownTimeoutCancelsDelayedWork(t *testing.T) {
	addressChan := make(chan string, 1)
	logger := slog.New(&listenLogHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		address: addressChan,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Short shutdown timeout of 50ms, with 2s processing delay.
	cfg := Config{
		Config:           config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:               "backend-timeout-delay",
		ListenAddr:       "127.0.0.1:0",
		ShutdownTimeout:  50 * time.Millisecond,
		Workers:          1,
		QueueCapacity:    2,
		ProcessingDelay:  2 * time.Second,
		ProcessingJitter: 0,
		DelaySeed:        1,
	}

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, logger)
	}()

	addr := waitFor(t, addressChan)
	client := &http.Client{Timeout: 5 * time.Second}

	reqDone := make(chan requestResult, 1)
	go func() {
		res, err := client.Get("http://" + addr + "/api/timeout-test")
		if err != nil {
			reqDone <- requestResult{err: err}
			return
		}
		body, err := io.ReadAll(res.Body)
		reqDone <- requestResult{body: string(body), err: errors.Join(err, res.Body.Close())}
	}()

	time.Sleep(15 * time.Millisecond)

	// Initiate shutdown, which will exceed 50ms timeout because request delay is 2s.
	cancel()

	// Server returns deadline exceeded error
	err := waitFor(t, done)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}

	// Client request should have encountered closed connection or failure
	res := waitFor(t, reqDone)
	if res.err == nil {
		t.Fatalf("expected client request to fail due to server timeout, got body: %q", res.body)
	}

	verifyListenerClosed(t, addr)
}

