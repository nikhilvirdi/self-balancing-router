package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"slices"
	"sync"
	"time"
)

func handler(registry *Registry, policy routingPolicy, upstreams map[string]*httputil.ReverseProxy, tracker *requestTracker, observer Observer, logger *slog.Logger) http.Handler {
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

	// Publish each reservation before the next selection reads the tracker.
	var assignmentMu sync.Mutex
	assign := func(candidates []Backend) (*httputil.ReverseProxy, int, Observation, string) {
		assignmentMu.Lock()
		defer assignmentMu.Unlock()

		backend, selected := policy.Select(candidates)
		observation := Observation{BackendID: backend.ID}
		if !selected || !slices.Contains(candidates, backend) {
			return nil, 0, observation, "invalid routing policy selection"
		}
		upstream := upstreams[backend.ID]
		if upstream == nil {
			return nil, 0, observation, "missing upstream handler"
		}
		index, ok := tracker.begin(backend.ID)
		if !ok {
			return nil, 0, observation, "missing backend request tracking state"
		}
		observation.StartedAt = time.Now()
		return upstream, index, observation, ""
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case livePath, readyPath:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			mux.ServeHTTP(w, r)
			return
		}
		candidates := registry.Eligible()
		if len(candidates) == 0 {
			routingUnavailable(w)
			return
		}
		upstream, index, observation, failure := assign(candidates)
		if failure != "" {
			logger.Error(failure, "backend_id", observation.BackendID)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		forwardTracked(w, r, upstream, tracker, index, observation, observer, logger)
	})
}

func routingUnavailable(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, "request routing is unavailable", http.StatusServiceUnavailable)
}
