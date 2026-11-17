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
	return instrumentedHandler(registry, policy, upstreams, tracker, observer, logger, nil, nil, false)
}

func instrumentedHandler(registry *Registry, policy routingPolicy, upstreams map[string]*httputil.ReverseProxy, tracker *requestTracker, observer Observer, logger *slog.Logger, metrics *proxyMetrics, events *telemetry.Events, banditTrace bool) http.Handler {
	// Selection sees each reservation and, for observing policies, the release
	// and resulting policy update together.
	var assignmentMu sync.Mutex
	var sequence uint64
	var traceOrder uint64
	bandit, _ := policy.(interface {
		selectDiagnostic([]Backend, *telemetry.BanditDecision) (Backend, bool)
		applyFeedback(Observation) (float64, bool)
	})
	if !banditTrace {
		bandit = nil
	}
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
	var finishObserved func(int, Observation, string)
	if policy, ok := policy.(observingPolicy); ok {
		finishObserved = func(index int, o Observation, id string) {
			assignmentMu.Lock()
			defer assignmentMu.Unlock()
			tracker.finish(index)
			if id == "" || bandit == nil {
				deliverObservation(policy.observe, o, logger)
				return
			}
			feedback := &telemetry.BanditFeedback{}
			deliverObservation(func(o Observation) {
				feedback.Reward, feedback.Applied = bandit.applyFeedback(o)
			}, o, logger)
			at := time.Now()
			traceOrder++
			feedback.Order = traceOrder
			feedback.AgeNS = at.Sub(o.StartedAt).Nanoseconds()
			events.Emit(at, telemetry.Event{Kind: "bandit_feedback", RequestID: id, BackendID: o.BackendID, Feedback: feedback})
		}
		finish = func(index int, o Observation) { finishObserved(index, o, "") }
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
		var backend Backend
		var selected bool
		var decision *telemetry.BanditDecision
		if bandit != nil && id != "" {
			decision = &telemetry.BanditDecision{Arms: make([]telemetry.BanditArm, 0, len(candidates))}
			backend, selected = bandit.selectDiagnostic(candidates, decision)
		} else {
			backend, selected = policy.Select(candidates)
		}
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
			if decision != nil {
				traceOrder++
				decision.Order = traceOrder
			}
			events.Emit(observation.StartedAt, telemetry.Event{Kind: "backend_selected", RequestID: id, BackendID: backend.ID, InFlight: load, Bandit: decision})
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
		requestFinish := finish
		if id != "" {
			r = r.WithContext(context.WithValue(r.Context(), diagnosticKey{}, id))
			requestObserver = observe(id)
			if bandit != nil {
				requestFinish = func(index int, o Observation) { finishObserved(index, o, id) }
			}
		}
		forwardTracked(w, r, upstream, index, observation, requestFinish, requestObserver, logger)
	})
}

func routingUnavailable(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, "request routing is unavailable", http.StatusServiceUnavailable)
}
