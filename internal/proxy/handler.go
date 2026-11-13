package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
)

func handler(registry *Registry, upstreams map[string]*httputil.ReverseProxy, tracker *requestTracker, observer Observer, logger *slog.Logger) http.Handler {
	const (
		livePath  = "/livez"
		readyPath = "/readyz"
	)
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	}
	mux.HandleFunc("GET "+livePath, ok)
	mux.HandleFunc("GET "+readyPath, func(w http.ResponseWriter, r *http.Request) {
		if len(registry.Eligible()) == 0 {
			routingUnavailable(w)
			return
		}
		ok(w, r)
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case livePath, readyPath:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			mux.ServeHTTP(w, r)
			return
		}
		backend, eligible := firstEligible(registry.Eligible())
		if !eligible {
			routingUnavailable(w)
			return
		}
		upstream := upstreams[backend.ID]
		if upstream == nil {
			logger.Error("missing upstream handler", "backend_id", backend.ID)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		forwardTracked(w, r, upstream, backend.ID, tracker, observer, logger)
	})
}

func firstEligible(candidates []Backend) (Backend, bool) {
	if len(candidates) == 0 {
		return Backend{}, false
	}
	return candidates[0], true
}

func routingUnavailable(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, "request routing is unavailable", http.StatusServiceUnavailable)
}
