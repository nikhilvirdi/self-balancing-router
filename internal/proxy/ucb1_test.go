package proxy

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestUCB1Score(t *testing.T) {
	for _, tt := range []struct {
		state ucb1State
		total uint64
		want  float64
	}{
		{ucb1State{1, 1}, 1, 1},
		{ucb1State{1.5, 3}, 10, 1.7389740629499462},
		{ucb1State{0, 1}, math.MaxUint64, math.Sqrt(2 * 64 * math.Ln2)},
		{ucb1State{float64(math.MaxUint64), math.MaxUint64}, math.MaxUint64, 1.0000000021931506},
	} {
		if got := tt.state.score(tt.total); math.IsNaN(got) || math.Abs(got-tt.want) > 1e-12 {
			t.Errorf("score for %+v with N=%d = %.16g, want %.16g", tt.state, tt.total, got, tt.want)
		}
	}
	a := ucb1Algorithm{total: math.MaxUint64}
	arms := map[string]ucb1State{"a": {observations: math.MaxUint64 - 1}}
	for range 2 {
		a.Update(arms, banditFeedback{BackendID: "a", Reward: 1})
	}
	if s := arms["a"]; s != (ucb1State{1, math.MaxUint64}) || a.total != math.MaxUint64 {
		t.Fatalf("large counters wrapped or lost consistency: state %+v, total %d", s, a.total)
	}
}

func TestUCB1ExplorationAndHealth(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c")
	p, tracker, algorithm := testUCB1(t, registry)
	backends := registry.Snapshot()
	now := time.Unix(100, 0)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	index, _ := tracker.begin("a")
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	tracker.finish(index)
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK))
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	index, _ = tracker.begin("b")
	assertSelection(t, p, registry.Eligible(), backends[2], true)
	other, _ := tracker.begin("c")
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	tracker.finish(index)
	tracker.finish(other)
	assertSelection(t, p, registry.Eligible(), backends[1], true)

	// Update with exact rewards to exercise the score independently of HTTP timing.
	p.arms["a"] = ucb1State{80, 100}
	p.arms["b"] = ucb1State{32.5, 50}
	p.arms["c"] = ucb1State{0, 1}
	algorithm.total = 151
	assertSelection(t, p, registry.Eligible(), backends[2], true)
	setHealth(t, registry, "c", Unhealthy)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	p.arms["c"] = ucb1State{0, 1_000_000}
	algorithm.total = 1_000_150
	// Including C's retained observations increases B's exploration bonus.
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	setHealth(t, registry, "c", Healthy)
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	if p.arms["c"].observations != 1_000_000 {
		t.Fatal("recovery lost arm history")
	}
	p.arms["b"] = p.arms["a"]
	algorithm.total = 1_000_200
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	independent, _, independentAlgorithm := testUCB1(t, registry)
	if independentAlgorithm.total != 0 || independent.arms["a"].observations != 0 {
		t.Fatal("separate proxy inherited learning state")
	}
}

func TestUCB1Feedback(t *testing.T) {
	registry := newTestRegistry(t, "a")
	p, _, algorithm := testUCB1(t, registry)
	now := time.Unix(100, 0)
	for _, o := range []Observation{
		newTestObservation("a", now, time.Second, UpstreamResponse, http.StatusOK),
		newTestObservation("a", now.Add(-time.Second), 0, UpstreamResponse, http.StatusOK),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusServiceUnavailable),
		newTestObservation("a", now, 0, TransportFailure, 0),
		newTestObservation("a", now, 0, UpstreamTimeout, 0),
		newTestObservation("a", now, 0, Canceled, http.StatusOK),
		newTestObservation("a", now, 0, Interrupted, http.StatusOK),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusNotFound),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusFound),
		newTestObservation("missing", now, 0, UpstreamResponse, http.StatusOK),
	} {
		p.observe(o)
	}
	if got := p.arms["a"]; got != (ucb1State{1.5, 5}) || algorithm.total != 5 {
		t.Fatalf("state = %+v, total = %d, want sum 1.5 and 5 observations", got, algorithm.total)
	}
}

func TestUCB1HTTPFeedback(t *testing.T) {
	entered := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cancel" {
			close(entered)
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "a")
	}))
	t.Cleanup(first.Close)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	t.Cleanup(second.Close)
	registry := healthRegistry(t, first.URL, second.URL)
	p, tracker, algorithm := testUCB1(t, registry)
	observations := make(chan Observation, 4)
	server := httptest.NewServer(observedPolicyHandler(t, registry, tracker, p, testTimeout, func(o Observation) { observations <- o }))
	t.Cleanup(server.Close)
	server.Client().Timeout = testTimeout

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pending := startObservedRequest(t, ctx, server, "/cancel")
	waitFor(t, entered)
	assertInFlight(t, tracker, "backend-a", 1)
	assertResponse(t, server, http.MethodGet, "/overload", 503, "overloaded\n")
	assertObservation(t, observations, "backend-b", 503, UpstreamResponse)
	cancel()
	if got := waitFor(t, pending); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("response = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-a", 0, Canceled)
	assertInFlight(t, tracker, "backend-a", 0)
	assertInFlight(t, tracker, "backend-b", 0)
	if p.arms["backend-a"] != (ucb1State{}) || p.arms["backend-b"] != (ucb1State{observations: 1}) || algorithm.total != 1 {
		t.Fatalf("unexpected state after cancellation and overload: %+v, total %d", p.arms, algorithm.total)
	}

	assertResponse(t, server, http.MethodGet, "/items", 200, "a")
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	for _, id := range []string{"backend-a", "backend-b"} {
		setHealth(t, registry, id, Unhealthy)
		assertInFlight(t, tracker, id, 0)
	}
	assertResponse(t, server, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertNoObservation(t, observations)
	if state := p.arms["backend-a"]; algorithm.total != 2 || state.observations != 1 || state.rewardSum <= 0 || state.rewardSum > 1 {
		t.Fatalf("unexpected successful state: %+v, total %d", state, algorithm.total)
	}
}

func testUCB1(t *testing.T, registry *Registry) (*banditPolicy[ucb1State], *requestTracker, *ucb1Algorithm) {
	t.Helper()
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicyUCB1, MABLatencyScale: time.Second}, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	p := policy.(*banditPolicy[ucb1State])
	return p, tracker, p.algorithm.(*ucb1Algorithm)
}
