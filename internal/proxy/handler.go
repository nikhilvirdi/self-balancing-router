package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"slices"
	"sync"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type diagnosticKey struct{}

func handler(registry *Registry, policy routingPolicy, upstreams map[string]*httputil.ReverseProxy, tracker *requestTracker, observer Observer, logger *slog.Logger) http.Handler {
	return instrumentedHandler(registry, policy, upstreams, tracker, observer, logger, nil, nil)
}

func instrumentedHandler(registry *Registry, policy routingPolicy, upstreams map[string]*httputil.ReverseProxy, tracker *requestTracker, observer Observer, logger *slog.Logger, metrics *proxyMetrics, events *telemetry.Events) http.Handler {
	// Selection sees each reservation and, for observing policies, the release
	// and resulting policy update together.
	var assignmentMu sync.Mutex
	var sequence uint64
	complete := func(o Observation, id string) {
		status := diagnosticStatus(o)
		if metrics != nil {
			metrics.complete(o, status)
		}
		if id != "" {
			events.Emit(o.FinishedAt, telemetry.Event{Kind: "request_completed", RequestID: id, BackendID: o.BackendID,
				Status: status, Outcome: outcomeNames[o.Outcome], DurationNS: o.Duration.Nanoseconds()})
		}
	}
	observe := func(id string) Observer {
		return func(o Observation) {
			complete(o, id)
			if observer != nil {
				observer(o)
			}
		}
	}
	defaultObserver := observer
	if metrics != nil {
		defaultObserver = observe("")
	}
	finish := func(index int, _ Observation) { tracker.finish(index) }
	if policy, ok := policy.(observingPolicy); ok {
		finish = func(index int, o Observation) {
			assignmentMu.Lock()
			defer assignmentMu.Unlock()
			tracker.finish(index)
			deliverObservation(policy.observe, o, logger)
		}
	}
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

	assign := func() (*httputil.ReverseProxy, int, Observation, string, string) {
		assignmentMu.Lock()
		defer assignmentMu.Unlock()

		var id string
		if events != nil {
			sequence++
			id = events.RequestID(sequence)
		}
		var observation Observation
		if metrics != nil || id != "" {
			observation.StartedAt = time.Now()
		}
		candidates := registry.Eligible()
		if len(candidates) == 0 {
			return nil, 0, observation, id, ""
		}
		var load []BackendInFlight
		if id != "" {
			load = tracker.Snapshot()
		}
		backend, selected := policy.Select(candidates)
		observation.BackendID = backend.ID
		if !selected || !slices.Contains(candidates, backend) {
			return nil, 0, observation, id, "invalid routing policy selection"
		}
		upstream := upstreams[backend.ID]
		if upstream == nil {
			return nil, 0, observation, id, "missing upstream handler"
		}
		index, ok := tracker.begin(backend.ID)
		if !ok {
			return nil, 0, observation, id, "missing backend request tracking state"
		}
		observation.StartedAt = time.Now()
		if metrics != nil {
			metrics.routed[backend.ID].Add(1)
		}
		if id != "" {
			events.Emit(observation.StartedAt, telemetry.Event{Kind: "backend_selected", RequestID: id, BackendID: backend.ID, InFlight: load})
		}
		return upstream, index, observation, id, ""
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case livePath, readyPath:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			mux.ServeHTTP(w, r)
			return
		}
		upstream, index, observation, id, failure := assign()
		if upstream == nil {
			if !observation.StartedAt.IsZero() {
				observation.Outcome = unavailable
				if failure != "" {
					observation.Outcome = internalError
				}
				observation.FinishedAt = time.Now()
				observation.Duration = observation.FinishedAt.Sub(observation.StartedAt)
				complete(observation, id)
			}
			if failure != "" {
				logger.Error(failure, "backend_id", observation.BackendID)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			} else {
				routingUnavailable(w)
			}
			return
		}
		requestObserver := defaultObserver
		if id != "" {
			r = r.WithContext(context.WithValue(r.Context(), diagnosticKey{}, id))
			requestObserver = observe(id)
		}
		forwardTracked(w, r, upstream, index, observation, finish, requestObserver, logger)
	})
}

func routingUnavailable(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, "request routing is unavailable", http.StatusServiceUnavailable)
}
