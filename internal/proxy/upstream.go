package proxy

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

func newTransport(dialTimeout, responseHeaderTimeout time.Duration) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Configured backends are contacted directly, independent of the host environment.
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = responseHeaderTimeout
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	return transport
}

func newUpstreams(backends []Backend, transport *http.Transport, logger *slog.Logger) (map[string]*httputil.ReverseProxy, error) {
	upstreams := make(map[string]*httputil.ReverseProxy, len(backends))
	for _, backend := range backends {
		target, err := url.Parse(backend.URL)
		if err != nil {
			return nil, fmt.Errorf("parse upstream URL for backend %q: %w", backend.ID, err)
		}
		backendLogger := logger.With("backend_id", backend.ID)
		upstreams[backend.ID] = &httputil.ReverseProxy{
			Transport: transport,
			ErrorLog:  slog.NewLogLogger(backendLogger.Handler(), slog.LevelError),
			Rewrite: func(req *httputil.ProxyRequest) {
				req.SetURL(target)
				for key := range req.Out.Header {
					if strings.HasPrefix(key, "X-Forwarded-") {
						delete(req.Out.Header, key)
					}
				}
				req.SetXForwarded()
				forwardRequestTrailers(req)
			},
			ModifyResponse: func(res *http.Response) error {
				if observation := requestObservation(res.Request); observation != nil {
					observation.UpstreamStatus = res.StatusCode
				}
				return nil
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				observation := requestObservation(r)
				if r.Context().Err() != nil {
					if observation != nil {
						observation.Outcome = Canceled
					}
					return
				}
				status := http.StatusBadGateway
				outcome := TransportFailure
				var networkErr net.Error
				if errors.As(err, &networkErr) && networkErr.Timeout() {
					status = http.StatusGatewayTimeout
					outcome = UpstreamTimeout
				}
				if observation != nil && observation.UpstreamStatus == 0 {
					observation.Outcome = outcome
				}
				backendLogger.Error("upstream request failed", "error", err, "status", status)
				w.Header().Set("Cache-Control", "no-store")
				http.Error(w, strings.ToLower(http.StatusText(status)), status)
			},
		}
	}
	return upstreams, nil
}

func forwardRequestTrailers(req *httputil.ProxyRequest) {
	if req.Out.Body == nil || len(req.Out.Trailer) == 0 {
		return
	}
	for name := range req.Out.Trailer {
		if !allowedRequestTrailer(name, req.In.Header.Values("Connection")) {
			delete(req.Out.Trailer, name)
		}
	}
	if len(req.Out.Trailer) != 0 {
		req.Out.Body = &requestTrailerBody{ReadCloser: req.Out.Body, source: req.In, trailers: req.Out.Trailer}
	}
}

func allowedRequestTrailer(name string, connection []string) bool {
	name = http.CanonicalHeaderKey(name)
	if strings.HasPrefix(name, "X-Forwarded-") || strings.HasPrefix(name, "If-") {
		return false
	}
	switch name {
	case "Forwarded", "Authorization", "Cache-Control", "Connection", "Content-Encoding",
		"Content-Length", "Content-Range", "Content-Type", "Expect", "Host", "Keep-Alive",
		"Max-Forwards", "Pragma", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
		"Range", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Www-Authenticate":
		return false
	}
	for _, value := range connection {
		for token := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(name, strings.TrimSpace(token)) {
				return false
			}
		}
	}
	return true
}

type requestTrailerBody struct {
	io.ReadCloser
	source   *http.Request
	trailers http.Header
}

func (b *requestTrailerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		// ReverseProxy clones the trailer map before the incoming body populates it.
		for name := range b.trailers {
			b.trailers[name] = b.source.Trailer[name]
		}
	}
	return n, err
}
