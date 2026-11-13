package backend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolConcurrencyLimit(t *testing.T) {
	const workers = 2
	const queueCap = 4

	entered := make(chan struct{}, workers)
	release := make(chan struct{})

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return response{BackendID: req.BackendID}, nil
		case <-ctx.Done():
			return response{}, ctx.Err()
		}
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(pool.Close)

	// Submit 2 tasks to occupy workers
	for range workers {
		go func() {
			_, _ = pool.Submit(context.Background(), requestInfo{BackendID: "test"})
		}()
	}

	// Wait for both to enter
	for range workers {
		select {
		case <-entered:
		case <-time.After(testTimeout):
			t.Fatal("timed out waiting for workers to enter")
		}
	}

	active, queued := pool.Stats()
	if active != workers || queued != 0 {
		t.Fatalf("Stats() = (%d, %d), want (%d, 0)", active, queued, workers)
	}

	close(release)
}

func TestPoolQueueCapacityAndOverload(t *testing.T) {
	const workers = 2
	const queueCap = 2

	entered := make(chan struct{}, workers)
	release := make(chan struct{})

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		entered <- struct{}{}
		<-release
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		pool.Close()
	})

	// Occupy 2 workers
	for range workers {
		go func() {
			_, _ = pool.Submit(context.Background(), requestInfo{BackendID: "test"})
		}()
	}
	for range workers {
		<-entered
	}

	// Queue 2 tasks
	for range queueCap {
		go func() {
			_, _ = pool.Submit(context.Background(), requestInfo{BackendID: "queued"})
		}()
	}

	// Wait briefly for queue to fill
	for {
		_, queued := pool.Stats()
		if queued == queueCap {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Next task must be rejected immediately
	_, err := pool.Submit(context.Background(), requestInfo{BackendID: "overflow"})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Submit() error = %v, want ErrOverloaded", err)
	}
}

func TestPoolZeroQueueCapacity(t *testing.T) {
	const workers = 2
	entered := make(chan struct{}, workers)
	release := make(chan struct{})

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		entered <- struct{}{}
		<-release
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(workers, 0, proc)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		pool.Close()
	})

	// Occupy both workers
	for range workers {
		go func() {
			_, _ = pool.Submit(context.Background(), requestInfo{BackendID: "test"})
		}()
	}
	for range workers {
		<-entered
	}

	// Queue capacity is 0, so next request must be rejected immediately
	_, err := pool.Submit(context.Background(), requestInfo{BackendID: "rejected"})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Submit() error = %v, want ErrOverloaded", err)
	}
}

func TestPoolFIFOOrder(t *testing.T) {
	const workers = 1
	const queueCap = 5

	block := make(chan struct{})
	enteredBlock := make(chan struct{})

	var orderMu sync.Mutex
	var order []string

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		if req.Path == "/block" {
			close(enteredBlock)
			<-block
			return response{}, nil
		}
		orderMu.Lock()
		order = append(order, req.Path)
		orderMu.Unlock()
		return response{Path: req.Path}, nil
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(pool.Close)

	// Block worker
	go func() {
		_, _ = pool.Submit(context.Background(), requestInfo{Path: "/block"})
	}()
	<-enteredBlock

	// Enqueue 3 tasks in order
	paths := []string{"/first", "/second", "/third"}
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			_, err := pool.Submit(context.Background(), requestInfo{Path: path})
			if err != nil {
				t.Errorf("Submit(%s) error = %v", path, err)
			}
		}(p)
		// Wait for this task to be enqueued before submitting the next
		for {
			_, queued := pool.Stats()
			if queued == i+1 {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Unblock worker
	close(block)
	wg.Wait()

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != len(paths) {
		t.Fatalf("order length = %d, want %d", len(order), len(paths))
	}
	for i, want := range paths {
		if order[i] != want {
			t.Errorf("order[%d] = %q, want %q", i, order[i], want)
		}
	}
}

func TestPoolCanceledQueuedRequest(t *testing.T) {
	const workers = 1
	const queueCap = 2

	block := make(chan struct{})
	enteredBlock := make(chan struct{})

	var executed atomic.Bool
	proc := func(ctx context.Context, req requestInfo) (response, error) {
		if req.Path == "/block" {
			close(enteredBlock)
			<-block
			return response{}, nil
		}
		if req.Path == "/cancel-me" {
			executed.Store(true)
		}
		return response{Path: req.Path}, nil
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(pool.Close)

	// Block worker
	go func() {
		_, _ = pool.Submit(context.Background(), requestInfo{Path: "/block"})
	}()
	<-enteredBlock

	// Enqueue cancellable task
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		_, err := pool.Submit(ctx, requestInfo{Path: "/cancel-me"})
		errChan <- err
	}()

	// Wait until it is in the queue
	for {
		_, queued := pool.Stats()
		if queued == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel while queued
	cancel()
	select {
	case err := <-errChan:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Submit() error = %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for cancelled task to return")
	}

	// Verify queue capacity was released promptly
	_, queued := pool.Stats()
	if queued != 0 {
		t.Fatalf("queued = %d, want 0 after cancellation", queued)
	}

	// Now another task can be queued and executed
	go func() {
		_, _ = pool.Submit(context.Background(), requestInfo{Path: "/replacement"})
	}()

	close(block)

	// Wait for queue to drain
	for {
		active, q := pool.Stats()
		if active == 0 && q == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if executed.Load() {
		t.Fatal("canceled queued task was executed by worker")
	}
}

func TestPoolCanceledActiveRequest(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		if req.BackendID == "test" {
			close(entered)
			<-ctx.Done()
			close(canceled)
			return response{}, ctx.Err()
		}
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(1, 1, proc)
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		_, err := pool.Submit(ctx, requestInfo{BackendID: "test"})
		errChan <- err
	}()

	<-entered
	cancel()
	<-canceled

	select {
	case err := <-errChan:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Submit to return")
	}

	// Verify worker recovers and processes next request
	res, err := pool.Submit(context.Background(), requestInfo{BackendID: "next"})
	if err != nil || res.BackendID != "next" {
		t.Fatalf("next task failed: res=%+v, err=%v", res, err)
	}
}

func TestPoolGracefulDraining(t *testing.T) {
	const workers = 2
	const queueCap = 4
	const queuedTasks = 2

	block := make(chan struct{})
	release := sync.OnceFunc(func() { close(block) })
	var completedCount atomic.Int32

	proc := func(ctx context.Context, req requestInfo) (response, error) {
		<-block
		completedCount.Add(1)
		return response{BackendID: req.BackendID}, nil
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(func() {
		release()
		pool.Close()
	})

	// Submit 2 active + 2 queued
	var wg sync.WaitGroup
	for range workers + queuedTasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pool.Submit(context.Background(), requestInfo{BackendID: "drain"})
			if err != nil {
				t.Errorf("Submit() error = %v", err)
			}
		}()
	}

	// Ensure all submissions have been admitted before stopping admission.
	deadline := time.NewTimer(testTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for active, queued := pool.Stats(); active != workers || queued != queuedTasks; active, queued = pool.Stats() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("Stats() = (%d, %d), want (%d, %d)", active, queued, workers, queuedTasks)
		}
	}

	// Stop accepting new tasks
	pool.StopAccepting()

	// Subsequent submissions must be rejected
	_, err := pool.Submit(context.Background(), requestInfo{BackendID: "too-late"})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Submit() after StopAccepting error = %v, want ErrOverloaded", err)
	}

	// Release blocked tasks
	release()
	wg.Wait()

	if completedCount.Load() != workers+queuedTasks {
		t.Fatalf("completed = %d, want %d", completedCount.Load(), workers+queuedTasks)
	}

	pool.Close()
}

func TestPoolCancelQueue(t *testing.T) {
	const workers = 1
	const queueCap = 2

	block := make(chan struct{})
	proc := func(ctx context.Context, req requestInfo) (response, error) {
		<-block
		return response{}, nil
	}

	pool := NewPool(workers, queueCap, proc)
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
		pool.Close()
	})

	// Block worker
	go func() {
		_, _ = pool.Submit(context.Background(), requestInfo{})
	}()

	time.Sleep(5 * time.Millisecond)

	// Enqueue 2 tasks
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := pool.Submit(context.Background(), requestInfo{})
			errs <- err
		}()
	}

	for {
		_, q := pool.Stats()
		if q == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	pool.CancelQueue(ErrShutdown)

	for range 2 {
		select {
		case err := <-errs:
			if !errors.Is(err, ErrShutdown) {
				t.Fatalf("err = %v, want ErrShutdown", err)
			}
		case <-time.After(testTimeout):
			t.Fatal("timed out waiting for cancelled queued tasks")
		}
	}
}

func TestPoolConcurrentSubmissions(t *testing.T) {
	const workers = 4
	const queueCap = 8
	const concurrency = 20
	const requestsPerWorker = 20

	pool := NewPool(workers, queueCap, defaultProcessor)
	t.Cleanup(pool.Close)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	var successes atomic.Int32
	var overloads atomic.Int32

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			for range requestsPerWorker {
				res, err := pool.Submit(context.Background(), requestInfo{BackendID: "test", Path: "/api/test"})
				if err == nil {
					if res.BackendID != "test" {
						t.Errorf("unexpected backend ID: %s", res.BackendID)
					}
					successes.Add(1)
				} else if errors.Is(err, ErrOverloaded) {
					overloads.Add(1)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	total := successes.Load() + overloads.Load()
	if total != concurrency*requestsPerWorker {
		t.Fatalf("total = %d, want %d", total, concurrency*requestsPerWorker)
	}
}
