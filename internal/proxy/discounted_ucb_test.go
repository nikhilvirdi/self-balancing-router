package proxy

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestDiscountedUCBFeedbackAndGlobalClock(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c")
	p, _, a := testDiscountedUCB(t, registry, 0.5)
	independent, _, independentAlgorithm := testDiscountedUCB(t, registry, 0.5)
	now := time.Unix(100, 0)
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK))
	setHealth(t, registry, "a", Unhealthy)
	p.observe(newTestObservation("b", now.Add(-time.Second), time.Second, UpstreamResponse, http.StatusOK))
	if got := p.arms["a"]; got != (discountedArmState{1, 1, 1}) {
		t.Fatalf("unrelated update rewrote arm a: %+v", got)
	}
	if got := p.arms["a"].at(a.step, a.gamma); got != (discountedArmState{0.5, 0.5, 2}) {
		t.Fatalf("inactive arm a did not decay: %+v", got)
	}
	if got := p.arms["b"]; got != (discountedArmState{0.5, 1, 2}) {
		t.Fatalf("delayed feedback did not enter with full weight: %+v", got)
	}
	for _, o := range []Observation{
		{},
		newTestObservation("a", now, 0, Canceled, http.StatusOK),
		newTestObservation("a", now, 0, Interrupted, http.StatusOK),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusBadRequest),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusFound),
		newTestObservation("missing", now, 0, UpstreamResponse, http.StatusOK),
	} {
		p.observe(o)
	}
	if a.step != 2 || a.total != 1.5 {
		t.Fatalf("excluded feedback advanced the clock: step %d, total %g", a.step, a.total)
	}
	p.observe(newTestObservation("a", now, 0, UpstreamTimeout, 0))
	if got := p.arms["a"]; got != (discountedArmState{0.25, 1.25, 3}) || a.total != 1.75 {
		t.Fatalf("unexpected timeout update for arm a: %+v, total %g", got, a.total)
	}
	p.observe(newTestObservation("c", now, 0, UpstreamResponse, http.StatusServiceUnavailable))
	p.observe(newTestObservation("c", now, 0, TransportFailure, 0))
	if got := p.arms["c"]; got != (discountedArmState{0, 1.5, 5}) || a.total != 1.9375 {
		t.Fatalf("unexpected zero-reward updates for arm c: %+v, total %g", got, a.total)
	}
	setHealth(t, registry, "a", Healthy)
	if got := p.arms["a"].at(a.step, a.gamma); got != (discountedArmState{0.0625, 0.3125, 5}) {
		t.Fatalf("arm a lost its decayed history after recovery: %+v", got)
	}
	if independentAlgorithm.step != 0 || independentAlgorithm.total != 0 || independent.arms["a"] != (discountedArmState{}) {
		t.Fatal("separate policy inherited discount state")
	}
}

func TestDiscountedUCBScoreAndBoundaries(t *testing.T) {
	for _, tt := range []struct {
		state discountedArmState
		total float64
		want  float64
		known bool
	}{
		{discountedArmState{rewardSum: 0.25, observations: 0.5}, 1.5, 1.773522843310106, true},
		{discountedArmState{rewardSum: 0.25, observations: 0.5}, 1, 0.5, true},
		{discountedArmState{rewardSum: 0.25, observations: 0.5}, math.Nextafter(1, 0), 0.5, true},
		{discountedArmState{}, 0, 0, false},
		{discountedArmState{rewardSum: 0.5, observations: 1}.at(4096, 0.5), 2, 0, false},
		{discountedArmState{observations: minDiscountedObservations}, 2, 0, false},
	} {
		if got, ok := tt.state.selectionScore(tt.total); ok != tt.known || math.IsNaN(got) || math.Abs(got-tt.want) > 1e-12 {
			t.Errorf("score for %+v with total %g = %.16g, %t, want %.16g, %t", tt.state, tt.total, got, ok, tt.want, tt.known)
		}
	}
	registry := newTestRegistry(t, "a")
	backends := registry.Snapshot()
	for _, gamma := range []float64{0, -0.1, 1, 1.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicyDiscountedUCB, MABLatencyScale: time.Second, MABDiscountFactor: gamma}, backends, newRequestTracker(backends)); err == nil {
			t.Errorf("accepted invalid gamma %v", gamma)
		}
	}
	for _, gamma := range []float64{math.SmallestNonzeroFloat64, math.Nextafter(1, 0)} {
		p, _, a := testDiscountedUCB(t, registry, gamma)
		for range 128 {
			p.observe(newTestObservation("a", time.Unix(100, 0), 0, UpstreamResponse, http.StatusOK))
		}
		if score, known := p.arms["a"].selectionScore(a.total); !known || math.IsNaN(score) || math.IsInf(score, 0) || score < 1 {
			t.Errorf("invalid score %v for gamma %g", score, gamma)
		}
	}
}

func TestDiscountedUCBAdaptationAndExploration(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	setHealth(t, registry, "a", Healthy)
	backends := registry.Snapshot()
	p, _, _ := testDiscountedUCB(t, registry, 0.99)
	now := time.Unix(100, 0)
	observe := func(id string, duration time.Duration) {
		p.observe(newTestObservation(id, now, duration, UpstreamResponse, http.StatusOK))
	}
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	for _, stage := range []struct {
		duration time.Duration
		winner   int
	}{{0, 0}, {3 * time.Second, 1}, {0, 0}} {
		for range 200 {
			observe("a", stage.duration)
			observe("b", time.Second)
		}
		assertSelection(t, p, registry.Eligible(), backends[stage.winner], true)
	}

	// Repeated feedback on B ages A even while the registry excludes it.
	p, tracker, a := testDiscountedUCB(t, registry, 0.5)
	observe("a", 0)
	setHealth(t, registry, "a", Unhealthy)
	for range 41 {
		observe("b", time.Second)
	}
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	if state := p.arms["a"].at(a.step, a.gamma); state.observations == 0 || state.observations > minDiscountedObservations {
		t.Fatalf("expected negligible positive history, got %+v", state)
	}
	setHealth(t, registry, "a", Healthy)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	index, ok := tracker.begin("a")
	if !ok {
		t.Fatal("missing reservation for arm a")
	}
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	tracker.finish(index)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
}

func TestDiscountedUCBConcurrentUpdates(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c")
	p, _, a := testDiscountedUCB(t, registry, 0.99)
	candidates := registry.Eligible()
	var workers sync.WaitGroup
	for i := range 96 {
		workers.Go(func() {
			id := candidates[i%len(candidates)].ID
			p.observe(newTestObservation(id, time.Unix(100, int64(i)), time.Second, UpstreamResponse, http.StatusOK))
			if _, ok := p.Select(candidates); !ok {
				t.Error("selection failed with eligible backends")
			}
		})
	}
	workers.Wait()
	var total float64
	for _, backend := range candidates {
		state := p.arms[backend.ID].at(a.step, a.gamma)
		if state.observations <= 0 || state.rewardSum != 0.5*state.observations {
			t.Fatalf("arm %s has inconsistent discounted state: %+v", backend.ID, state)
		}
		total += state.observations
	}
	if a.step != 96 || math.Abs(a.total-total) > 1e-12 {
		t.Fatalf("step %d, global total %g, sum of arms %g", a.step, a.total, total)
	}
}

func TestDiscountedUCBHTTPFeedback(t *testing.T) {
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
	p, tracker, a := testDiscountedUCB(t, registry, 0.5)
	observations := make(chan Observation, 4)
	server := httptest.NewServer(observedPolicyHandler(t, registry, tracker, p, testTimeout, func(o Observation) { observations <- o }))
	t.Cleanup(server.Close)
	server.Client().Timeout = testTimeout
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := startObservedRequest(t, ctx, server, "/cancel")
	waitFor(t, entered)
	assertInFlight(t, tracker, "backend-a", 1)
	assertResponse(t, server, http.MethodGet, "/overload", 503, "overloaded\n")
	assertObservation(t, observations, "backend-b", 503, UpstreamResponse)
	cancel()
	if got := waitFor(t, request); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("response = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-a", 0, Canceled)
	if a.step != 1 || p.arms["backend-a"] != (discountedArmState{}) {
		t.Fatal("cancellation changed discount state")
	}
	assertResponse(t, server, http.MethodGet, "/items", 200, "a")
	assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	for _, id := range []string{"backend-a", "backend-b"} {
		setHealth(t, registry, id, Unhealthy)
		assertInFlight(t, tracker, id, 0)
	}
	assertResponse(t, server, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertNoObservation(t, observations)
	if state := p.arms["backend-a"]; a.step != 2 || a.total != 1.5 || state.observations != 1 || state.rewardSum <= 0 || state.rewardSum > 1 {
		t.Fatalf("unexpected success feedback: %+v, step %d, total %g", state, a.step, a.total)
	}
}

func testDiscountedUCB(t *testing.T, registry *Registry, gamma float64) (*banditPolicy[discountedArmState], *requestTracker, *discountedUCB) {
	t.Helper()
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicyDiscountedUCB, MABLatencyScale: time.Second, MABDiscountFactor: gamma}, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	p := policy.(*banditPolicy[discountedArmState])
	return p, tracker, p.algorithm.(*discountedUCB)
}
