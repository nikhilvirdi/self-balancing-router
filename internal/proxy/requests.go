package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"
)

type Outcome uint8

const (
	Interrupted Outcome = iota
	UpstreamResponse
	TransportFailure
	UpstreamTimeout
	Canceled
)

type Observation struct {
	BackendID      string
	StartedAt      time.Time
	FinishedAt     time.Time
	Duration       time.Duration // Assignment through forwarding-handler termination.
	UpstreamStatus int           // Zero if no final upstream response was observed.
	Outcome        Outcome
}

// Observer runs synchronously after accounting. Implementations must be bounded and safe for concurrent calls.
type Observer func(Observation)

type BackendInFlight struct {
	BackendID string
	Count     int
}

type requestTracker struct {
	mu   sync.RWMutex
	byID map[string]int
	load []BackendInFlight
}

func newRequestTracker(backends []Backend) *requestTracker {
	t := &requestTracker{
		byID: make(map[string]int, len(backends)),
		load: make([]BackendInFlight, len(backends)),
	}
	for i, backend := range backends {
		t.byID[backend.ID] = i
		t.load[i].BackendID = backend.ID
	}
	return t
}

func (t *requestTracker) InFlight(id string) (int, bool) {
	index, ok := t.byID[id]
	if !ok {
		return 0, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.load[index].Count, true
}

// Snapshot copies all counts under one read lock, in configuration order.
func (t *requestTracker) Snapshot() []BackendInFlight {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return slices.Clone(t.load)
}

func (t *requestTracker) begin(id string) (int, bool) {
	index, ok := t.byID[id]
	if ok {
		t.mu.Lock()
		t.load[index].Count++
		t.mu.Unlock()
	}
	return index, ok
}

func (t *requestTracker) finish(index int) {
	t.mu.Lock()
	t.load[index].Count--
	t.mu.Unlock()
}

type observationKey struct{}

func requestObservation(r *http.Request) *Observation {
	observation, _ := r.Context().Value(observationKey{}).(*Observation)
	return observation
}

func forwardTracked(w http.ResponseWriter, r *http.Request, upstream http.Handler, tracker *requestTracker, index int, observation Observation, observer Observer, logger *slog.Logger) {
	defer func() {
		observation.FinishedAt = time.Now()
		observation.Duration = observation.FinishedAt.Sub(observation.StartedAt)
		if r.Context().Err() != nil {
			observation.Outcome = Canceled
		}
		tracker.finish(index)
		if observer != nil {
			deliverObservation(observer, observation, logger)
		}
	}()

	upstream.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), observationKey{}, &observation)))
	// net/http aborts the handler on a response-body copy error.
	if observation.Outcome == Interrupted && observation.UpstreamStatus >= http.StatusOK {
		observation.Outcome = UpstreamResponse
	}
}

func deliverObservation(observer Observer, observation Observation, logger *slog.Logger) {
	defer func() {
		if recover() != nil {
			logger.Error("request observation callback panicked", "backend_id", observation.BackendID)
		}
	}()
	observer(observation)
}
