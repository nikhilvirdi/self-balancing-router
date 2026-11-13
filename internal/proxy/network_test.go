package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestProxyDialTimeout(t *testing.T) {
	transport := newTransport(time.Second, 0)
	transport.DialContext = func(_ context.Context, network, _ string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: context.DeadlineExceeded}
	}
	proxy := startNetworkProxy(t, transport, "http://127.0.0.1:1")
	assertResponse(t, proxy, http.MethodGet, "/items", 504, "gateway timeout\n")
}

func TestProxyResponseHeaderTimeout(t *testing.T) {
	for _, tt := range []struct {
		name    string
		timeout time.Duration
		status  int
		body    string
	}{
		{"enabled", 100 * time.Millisecond, http.StatusGatewayTimeout, "gateway timeout\n"},
		{"disabled", 0, http.StatusOK, "delayed response"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			entered, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-r.Context().Done():
					close(canceled)
				case <-release:
					_, _ = io.WriteString(w, "delayed response")
				case <-t.Context().Done():
				}
			}))
			t.Cleanup(upstream.Close)
			proxy := startNetworkProxy(t, newTransport(time.Second, tt.timeout), upstream.URL)
			result := make(chan requestResult, 1)
			go func() {
				res, err := proxy.Client().Get(proxy.URL + "/items")
				if err != nil {
					result <- requestResult{err: err}
					return
				}
				body, err := io.ReadAll(res.Body)
				if res.StatusCode != tt.status {
					t.Errorf("unexpected status: %d", res.StatusCode)
				}
				result <- requestResult{body: string(body), err: errors.Join(err, res.Body.Close())}
			}()
			waitFor(t, entered)
			if tt.timeout == 0 {
				close(release)
			}
			if got := waitFor(t, result); got.err != nil || got.body != tt.body {
				t.Fatalf("response = %+v, want %q", got, tt.body)
			}
			if tt.timeout > 0 {
				waitFor(t, canceled)
			}
		})
	}
}

func TestProxyCanceledRequestDoesNotWrite(t *testing.T) {
	for _, tt := range []struct {
		name    string
		context func(context.Context) (context.Context, context.CancelFunc)
	}{
		{"canceled", context.WithCancel},
		{"expired", func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, -time.Second)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			_, handle := testProxyHandler(t, logger, config.BackendConfig{ID: "backend-a", URL: "http://127.0.0.1:1"})
			ctx, cancel := tt.context(t.Context())
			cancel()
			response := &writeRecorder{ResponseRecorder: httptest.NewRecorder()}
			handle.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/items", nil))
			if response.wrote || len(response.Header()) > 0 || logs.Len() > 0 {
				t.Fatalf("canceled request produced a response or error log: %v, %s", response.Header(), logs.String())
			}
		})
	}
}

type writeRecorder struct {
	*httptest.ResponseRecorder
	wrote bool
}

func (w *writeRecorder) WriteHeader(status int) {
	w.wrote = true
	w.ResponseRecorder.WriteHeader(status)
}

func (w *writeRecorder) Write(body []byte) (int, error) {
	w.wrote = true
	return w.ResponseRecorder.Write(body)
}

func TestProxyPreservesCompressionAndCookies(t *testing.T) {
	var payload bytes.Buffer
	gzipWriter := gzip.NewWriter(&payload)
	_, err := io.WriteString(gzipWriter, "compressed upstream payload")
	if err := errors.Join(err, gzipWriter.Close()); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "" {
			t.Error("proxy added an Accept-Encoding header")
		}
		if r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("Accept") != "application/octet-stream" {
			t.Errorf("missing end-to-end request headers: %v", r.Header)
		}
		for _, header := range []string{"Keep-Alive", "Proxy-Connection", "X-Hop"} {
			if r.Header.Get(header) != "" {
				t.Errorf("upstream received hop-by-hop header %s", header)
			}
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Set-Cookie", "a=1; Path=/")
		w.Header().Add("Set-Cookie", "b=2; Path=/")
		w.Header().Set("Connection", "X-Hop")
		w.Header().Set("X-Hop", "upstream only")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("Proxy-Connection", "close")
		_, _ = w.Write(payload.Bytes())
	}))
	t.Cleanup(upstream.Close)
	_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	t.Cleanup(transport.CloseIdleConnections)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxy.URL+"/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Connection", "close")
	req.Header.Set("Connection", "X-Hop")
	req.Header.Set("X-Hop", "client only")
	res, body := doRequest(t, &http.Client{Transport: transport}, req)
	if res.StatusCode != 200 || res.Header.Get("Content-Encoding") != "gzip" || body != payload.String() {
		t.Fatalf("compressed response was changed: status=%d encoding=%q bytes=%d", res.StatusCode, res.Header.Get("Content-Encoding"), len(body))
	}
	cookies := res.Header.Values("Set-Cookie")
	if len(cookies) != 2 || cookies[0] != "a=1; Path=/" || cookies[1] != "b=2; Path=/" {
		t.Fatalf("Set-Cookie = %q", cookies)
	}
	for _, header := range []string{"Keep-Alive", "Proxy-Connection", "X-Hop"} {
		if res.Header.Get(header) != "" {
			t.Errorf("client received hop-by-hop header %s", header)
		}
	}
}

func TestProxyNoBodyResponses(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("ETag", `"version-1"`)
				w.WriteHeader(status)
			}))
			t.Cleanup(upstream.Close)
			_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxy.URL+"/items", nil)
			if err != nil {
				t.Fatal(err)
			}
			res, body := doRequest(t, proxy.Client(), req)
			if res.StatusCode != status || body != "" || res.Header.Get("ETag") != `"version-1"` {
				t.Fatalf("unexpected empty response: %d %q %v", res.StatusCode, body, res.Header)
			}
		})
	}
}

func TestProxyStreamsResponseAndTrailers(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Result")
		_, _ = io.WriteString(w, "first\n")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-release:
			_, _ = io.WriteString(w, "last\n")
			w.Header().Set("X-Result", "complete")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)
	_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
	client := &http.Client{Timeout: testTimeout}
	t.Cleanup(client.CloseIdleConnections)
	res, err := client.Get(proxy.URL + "/items")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	first := make([]byte, len("first\n"))
	if _, err := io.ReadFull(res.Body, first); err != nil || string(first) != "first\n" {
		t.Fatalf("initial stream = %q, %v", first, err)
	}
	close(release)
	rest, err := io.ReadAll(res.Body)
	if err != nil || string(rest) != "last\n" || res.Trailer.Get("X-Result") != "complete" {
		t.Fatalf("stream completion = %q, %v, trailers %v", rest, err, res.Trailer)
	}
	if res.StatusCode != 200 || len(res.TransferEncoding) != 1 || res.TransferEncoding[0] != "chunked" {
		t.Fatalf("stream response = %d %v", res.StatusCode, res.TransferEncoding)
	}
}

func TestProxyStreamsRequestAndTrailers(t *testing.T) {
	const trailer = "X-Upload-Result"
	received := make(chan struct{})
	payload := strings.Repeat("request body\n", 32*1024)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, len("start\n"))
		if _, err := io.ReadFull(r.Body, first); err != nil || string(first) != "start\n" {
			t.Errorf("initial upload = %q, %v", first, err)
			return
		}
		close(received)
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != payload {
			t.Errorf("remaining upload = %d bytes, %v", len(body), err)
			return
		}
		for _, name := range []string{"X-Forwarded-For", "Forwarded", "X-Hop", "Proxy-Connection", "Authorization", "X-Unannounced"} {
			if _, exists := r.Trailer[name]; exists {
				t.Errorf("upstream received unexpected trailer %s", name)
			}
		}
		_, _ = io.WriteString(w, r.Trailer.Get(trailer))
	}))
	t.Cleanup(upstream.Close)
	_, proxy := startTestProxy(t, config.BackendConfig{ID: "backend-a", URL: upstream.URL})
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, proxy.URL+"/items", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "X-Hop")
	req.Trailer = http.Header{
		trailer: nil, "X-Forwarded-For": {"spoofed"}, "Forwarded": {"spoofed"},
		"X-Hop": {"hop"}, "Proxy-Connection": {"close"}, "Authorization": {"invalid"},
	}
	go func() {
		_, err := io.WriteString(writer, "start\n")
		if err == nil {
			select {
			case <-received:
				_, err = io.WriteString(writer, payload)
				req.Trailer.Set(trailer, "complete")
				req.Trailer.Set("X-Unannounced", "unannounced")
			case <-t.Context().Done():
				err = t.Context().Err()
			}
		}
		_ = writer.CloseWithError(err)
	}()
	res, body := doRequest(t, proxy.Client(), req)
	if res.StatusCode != 200 || body != "complete" {
		t.Fatalf("upload response = %d %q, want trailer value", res.StatusCode, body)
	}
}

func TestProxyForwardsContinue(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
	}))
	t.Cleanup(upstream.Close)
	proxy := startNetworkProxy(t, newTransport(time.Second, time.Second), upstream.URL)
	var continued atomic.Bool
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
		Got100Continue: func() { continued.Store(true) },
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+"/items", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Expect", "100-continue")
	res, body := doRequest(t, proxy.Client(), req)
	if res.StatusCode != http.StatusOK || body != "payload" || !continued.Load() {
		t.Fatalf("response = %d %q, 100 Continue received = %t", res.StatusCode, body, continued.Load())
	}
}

func TestProxyUpstreamDisconnect(t *testing.T) {
	for _, tt := range []struct {
		name    string
		partial bool
	}{
		{"before headers", false},
		{"after headers and body", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			received, release := make(chan struct{}), make(chan struct{})
			var requests atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) != 1 {
					_, _ = io.WriteString(w, "recovered")
					return
				}
				conn, buffer, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				if !tt.partial {
					return
				}
				_, err = buffer.WriteString("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
				if err != nil {
					t.Error(err)
					return
				}
				if err := buffer.Flush(); err != nil {
					t.Error(err)
					return
				}
				close(received)
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(upstream.Close)
			proxy := startNetworkProxy(t, newTransport(time.Second, time.Second), upstream.URL)
			if tt.partial {
				t.Cleanup(func() {
					select {
					case <-release:
					default:
						close(release)
					}
				})
				res, err := proxy.Client().Get(proxy.URL + "/items")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = res.Body.Close() }()
				waitFor(t, received)
				first := make([]byte, 5)
				if _, err := io.ReadFull(res.Body, first); err != nil || string(first) != "first" || res.StatusCode != 200 {
					t.Fatalf("committed response = %d %q, %v", res.StatusCode, first, err)
				}
				close(release)
				rest, err := io.ReadAll(res.Body)
				if len(rest) != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("after partial response read %q, %v, want truncated stream", rest, err)
				}
			} else {
				assertResponse(t, proxy, http.MethodGet, "/items", 502, "bad gateway\n")
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("failed request reached upstream %d times", got)
			}
			assertResponse(t, proxy, http.MethodGet, "/items", 200, "recovered")
		})
	}
}

func TestProxyConcurrentUpstreamConnections(t *testing.T) {
	const count = 8
	entered, release := make(chan string, count), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.RemoteAddr
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(upstream.Close)
	proxy := startNetworkProxy(t, newTransport(time.Second, testTimeout), upstream.URL)
	results := make(chan error, count)
	for range count {
		go func() {
			res, err := proxy.Client().Get(proxy.URL + "/items")
			if err == nil {
				if res.StatusCode != http.StatusNoContent {
					err = errors.New(res.Status)
				}
				err = errors.Join(err, res.Body.Close())
			}
			results <- err
		}()
	}
	addresses := make(map[string]bool, count)
	for range count {
		addresses[waitFor(t, entered)] = true
	}
	close(release)
	if len(addresses) != count {
		t.Fatalf("upstream used %d concurrent connections, want %d", len(addresses), count)
	}
	for range count {
		if err := waitFor(t, results); err != nil {
			t.Error(err)
		}
	}
}

func TestRunClosesIdleUpstreamConnections(t *testing.T) {
	closed := make(chan struct{}, 1)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "upstream")
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	upstream.Start()
	t.Cleanup(upstream.Close)
	address := make(chan string, 1)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, config.ProxyConfig{
			ListenAddr: "127.0.0.1:0", ShutdownTimeout: testTimeout,
			DialTimeout: time.Second, ResponseHeaderTimeout: time.Second,
			Backends: []config.BackendConfig{{ID: "backend-a", URL: upstream.URL}},
		}, slog.New(&listenLogHandler{Handler: slog.NewTextHandler(io.Discard, nil), address: address}))
	}()
	client := &http.Client{Timeout: testTimeout}
	t.Cleanup(client.CloseIdleConnections)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+waitFor(t, address)+"/items", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, body := doRequest(t, client, req)
	if res.StatusCode != 200 || body != "upstream" {
		t.Fatalf("upstream response = %d %q", res.StatusCode, body)
	}
	select {
	case <-closed:
		t.Fatal("upstream connection closed before shutdown")
	default:
	}
	stop()
	if err := waitFor(t, done); err != nil {
		t.Fatal(err)
	}
	waitFor(t, closed)
}

type listenLogHandler struct {
	slog.Handler
	address   chan<- string
	boundAddr string
}

func (h *listenLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.Handler = h.Handler.WithAttrs(attrs)
	for _, attr := range attrs {
		if attr.Key == "address" {
			clone.boundAddr = attr.Value.String()
		}
	}
	return &clone
}

func (h *listenLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "HTTP server listening" {
		h.address <- h.boundAddr
	}
	return h.Handler.Handle(ctx, record)
}

func startNetworkProxy(t *testing.T, transport *http.Transport, upstream string) *httptest.Server {
	t.Helper()
	_, handle := testProxyWithTransport(t, transport, slog.New(slog.DiscardHandler), config.BackendConfig{ID: "backend-a", URL: upstream})
	server := httptest.NewServer(handle)
	server.Client().Timeout = testTimeout
	t.Cleanup(server.Close)
	return server
}
