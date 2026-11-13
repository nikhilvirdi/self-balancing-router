package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

const testTimeout = 5 * time.Second

func TestRunOccupiedPort(t *testing.T) {
	listener := listenLoopback(t)
	addr := listener.Addr().String()
	err := Run(t.Context(), Config{
		ID:              "backend-a",
		ListenAddr:      addr,
		ShutdownTimeout: time.Second,
		Workers:         2,
		QueueCapacity:   4,
	}, slog.New(slog.DiscardHandler))

	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "listen" || !strings.Contains(err.Error(), addr) {
		t.Fatalf("Run() error = %v, want wrapped listen error for %s", err, addr)
	}
}

func TestServeUnexpectedError(t *testing.T) {
	listener := listenLoopback(t)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	pool := NewPool(2, 4, nil)
	t.Cleanup(pool.Close)
	err := serve(t.Context(), &http.Server{}, listener, pool, time.Second, slog.New(slog.DiscardHandler))
	if !errors.Is(err, net.ErrClosed) || !strings.Contains(err.Error(), "serve HTTP") {
		t.Fatalf("serve() error = %v, want wrapped listener error", err)
	}
}

func TestServeCanceledBeforeStart(t *testing.T) {
	listener := listenLoopback(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	pool := NewPool(2, 4, nil)
	t.Cleanup(pool.Close)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, &http.Server{}, listener, pool, time.Second, slog.New(slog.DiscardHandler))
	}()
	if err := waitFor(t, done); err != nil {
		t.Fatalf("serve() error = %v, want clean shutdown", err)
	}
	if err := listener.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetDeadline() error = %v, want closed listener", err)
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	listener := &observedListener{Listener: listenLoopback(t), closed: make(chan struct{})}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	})}
	pool := NewPool(2, 4, nil)
	stop, done, request := startRequest(t, listener, server, pool, testTimeout)
	waitFor(t, entered)
	stop()
	waitFor(t, listener.closed)
	select {
	case err := <-done:
		t.Fatalf("serve returned before active handler finished: %v", err)
	default:
	}

	close(release)
	response := waitFor(t, request)
	if response.err != nil || response.body != "finished" {
		t.Fatalf("active request = %+v, want completed response", response)
	}
	if err := waitFor(t, done); err != nil {
		t.Fatalf("serve() error = %v, want clean shutdown", err)
	}
}

func TestServeShutdownTimeout(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(exited)
	})}
	pool := NewPool(2, 4, nil)
	stop, done, request := startRequest(t, listenLoopback(t), server, pool, 50*time.Millisecond)
	waitFor(t, entered)
	stop()

	if err := waitFor(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("serve() error = %v, want shutdown deadline exceeded", err)
	}
	waitFor(t, exited)
	if response := waitFor(t, request); response.err == nil {
		t.Fatalf("client received a response after forced shutdown: %+v", response)
	}
}

func TestRunLifecycle(t *testing.T) {
	addressChan := make(chan string, 1)
	logger := slog.New(&listenLogHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		address: addressChan,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := Config{
		Config:          config.Config{LogLevel: slog.LevelInfo, LogFormat: config.LogJSON},
		ID:              "backend-lifecycle",
		ListenAddr:      "127.0.0.1:0",
		ShutdownTimeout: testTimeout,
		Workers:         2,
		QueueCapacity:   4,
	}

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, logger)
	}()

	addr := waitFor(t, addressChan)
	client := &http.Client{Timeout: testTimeout}

	// 1. Check healthz endpoint
	healthRes, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	healthBody, err := io.ReadAll(healthRes.Body)
	_ = healthRes.Body.Close()
	if err != nil || healthRes.StatusCode != http.StatusOK || string(healthBody) != "ok\n" {
		t.Fatalf("healthz response = %d %q, want 200 ok\\n", healthRes.StatusCode, string(healthBody))
	}

	// 2. Check application endpoint
	appRes, err := client.Get("http://" + addr + "/api/v1/test")
	if err != nil {
		t.Fatalf("app request failed: %v", err)
	}
	appBody, err := io.ReadAll(appRes.Body)
	_ = appRes.Body.Close()
	if err != nil || appRes.StatusCode != http.StatusOK {
		t.Fatalf("app response error: status=%d err=%v", appRes.StatusCode, err)
	}

	var payload response
	if err := json.Unmarshal(appBody, &payload); err != nil {
		t.Fatalf("unmarshal app response %q: %v", string(appBody), err)
	}
	if payload.BackendID != "backend-lifecycle" || payload.Method != "GET" || payload.Path != "/api/v1/test" {
		t.Fatalf("unexpected app response: %+v", payload)
	}

	// 3. Initiate graceful shutdown
	cancel()
	if err := waitFor(t, done); err != nil {
		t.Fatalf("Run() returned unexpected error on shutdown: %v", err)
	}

	// 4. Verify listener is released
	verifyListenerClosed(t, addr)
}

func verifyListenerClosed(t *testing.T, addr string) {
	t.Helper()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to bind to previously closed address %s: %v", addr, err)
	}
	_ = l.Close()
}

type listenLogHandler struct {
	slog.Handler
	address chan<- string
	bound   string
}

func (h *listenLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.Handler = h.Handler.WithAttrs(attrs)
	for _, attr := range attrs {
		if attr.Key == "address" {
			clone.bound = attr.Value.String()
		}
	}
	return &clone
}

func (h *listenLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "backend server listening" {
		select {
		case h.address <- h.bound:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

type observedListener struct {
	net.Listener
	closed chan struct{}
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	close(l.closed)
	return err
}

func listenLoopback(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

type requestResult struct {
	body string
	err  error
}

func startRequest(t *testing.T, listener net.Listener, server *http.Server, pool *Pool, timeout time.Duration) (context.CancelFunc, <-chan error, <-chan requestResult) {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener, pool, timeout, slog.New(slog.DiscardHandler)) }()

	client := &http.Client{Timeout: testTimeout}
	request := make(chan requestResult, 1)
	go func() {
		response, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			request <- requestResult{err: err}
			return
		}
		body, err := io.ReadAll(response.Body)
		request <- requestResult{body: string(body), err: errors.Join(err, response.Body.Close())}
	}()
	return stop, done, request
}

func waitFor[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for server or request")
		var zero T
		return zero
	}
}
