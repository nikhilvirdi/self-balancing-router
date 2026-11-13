package proxy

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

func newTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Configured backends are contacted directly, independent of the host environment.
	transport.Proxy = nil
	transport.DisableCompression = true
	return transport
}

func newUpstreams(backends []Backend, transport *http.Transport, logger *slog.Logger) (map[string]http.Handler, error) {
	upstreams := make(map[string]http.Handler, len(backends))
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
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				if r.Context().Err() == nil {
					backendLogger.Error("upstream request failed", "error", err)
				}
				w.Header().Set("Cache-Control", "no-store")
				http.Error(w, "bad gateway", http.StatusBadGateway)
			},
		}
	}
	return upstreams, nil
}
