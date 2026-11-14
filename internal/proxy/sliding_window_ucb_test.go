package proxy

import (
	"math"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestSlidingWindowUCBEviction(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c")
	p, _, algorithm := testSlidingWindowUCB(t, registry, 3)
	independent, _, independentAlgorithm := testSlidingWindowUCB(t, registry, 3)
	now := time.Unix(100, 0)
	for _, o := range []Observation{
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK),
		newTestObservation("b", now, time.Second, UpstreamResponse, http.StatusOK),
		newTestObservation("a", now, 0, UpstreamResponse, http.StatusServiceUnavailable),
	} {
		p.observe(o)
	}
	assertWindowArm(t, p, "a", 2, 1)
	assertWindowArm(t, p, "b", 1, 0.5)
	want := slices.Clone(algorithm.window)
	for _, o := range []Observation{
		{},
		newTestObservation("missing", now, 0, UpstreamResponse, http.StatusOK),
		newTestObservation("c", now, 0, Canceled, 0),
		newTestObservation("c", now, 0, Interrupted, http.StatusOK),
		newTestObservation("c", now, 0, UpstreamResponse, http.StatusFound),
		newTestObservation("c", now, 0, UpstreamResponse, http.StatusBadRequest),
	} {
		p.observe(o)
	}
	if !slices.Equal(algorithm.window, want) || algorithm.next != 0 {
		t.Fatalf("excluded feedback changed the window: %+v", algorithm.window)
	}

	// Feedback application order determines eviction even when timestamps arrive out of order.
	p.observe(newTestObservation("c", now.Add(-time.Second), time.Second, UpstreamResponse, http.StatusOK))
	assertWindowArm(t, p, "a", 1, 0)
	assertWindowArm(t, p, "c", 1, 0.5)
	if algorithm.window[0] != (windowReward{"c", 0.5}) || algorithm.next != 1 {
		t.Fatalf("late completion was not retained at the tail: %+v", algorithm.window)
	}
	p.observe(newTestObservation("c", now, time.Second, UpstreamTimeout, 0))
	assertWindowArm(t, p, "b", 0, 0)
	assertWindowArm(t, p, "c", 2, 0.5)
	p.observe(newTestObservation("a", now, time.Second, TransportFailure, 0))
	assertWindowArm(t, p, "a", 1, 0)
	if len(algorithm.window) != 3 || algorithm.next != 0 || len(independentAlgorithm.window) != 0 {
		t.Fatalf("unexpected ring lengths or cursor: %d, %d, %d", len(algorithm.window), algorithm.next, len(independentAlgorithm.window))
	}
	assertWindowArm(t, independent, "a", 0, 0)
}

func TestSlidingWindowUCBBoundaries(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	backends := registry.Snapshot()
	now := time.Unix(100, 0)
	p, _, algorithm := testSlidingWindowUCB(t, registry, 1)
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK))
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusServiceUnavailable))
	assertWindowArm(t, p, "a", 1, 0)
	p.observe(newTestObservation("b", now, time.Second, UpstreamResponse, http.StatusOK))
	assertWindowArm(t, p, "a", 0, 0)
	assertWindowArm(t, p, "b", 1, 0.5)
	assertSelection(t, p, backends, backends[0], true)
	if len(algorithm.window) != 1 || algorithm.next != 0 {
		t.Fatalf("unexpected one-entry ring: %+v", algorithm)
	}

	p, _, algorithm = testSlidingWindowUCB(t, registry, 2)
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK))
	slow := newTestObservation("a", now, math.MaxInt64, UpstreamResponse, http.StatusOK)
	tiny, _ := banditReward(slow, time.Second)
	p.observe(slow)
	p.observe(newTestObservation("b", now, 0, UpstreamResponse, http.StatusServiceUnavailable))
	assertWindowArm(t, p, "a", 1, tiny)
	for range 20 {
		p.observe(newTestObservation("b", now, 0, UpstreamResponse, http.StatusOK))
	}
	assertWindowArm(t, p, "a", 0, 0)
	assertWindowArm(t, p, "b", 2, 2)
	if score := p.arms["b"].score(uint64(len(algorithm.window))); math.Abs(score-1.8325546111576977) > 1e-12 || math.IsNaN(score) {
		t.Fatalf("unexpected windowed score %v", score)
	}
	for _, size := range []int{0, -1} {
		if _, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicySlidingWindowUCB, MABLatencyScale: time.Second, MABSWWindow: size}, backends, newRequestTracker(backends)); err == nil {
			t.Fatalf("accepted window size %d", size)
		}
	}
}

func TestSlidingWindowUCBAdaptsAndReexplores(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	setHealth(t, registry, "a", Healthy)
	backends := registry.Snapshot()
	p, tracker, _ := testSlidingWindowUCB(t, registry, 4)
	now := time.Unix(100, 0)
	observe := func(id string, duration time.Duration) {
		p.observe(newTestObservation(id, now, duration, UpstreamResponse, http.StatusOK))
	}
	for range 2 {
		observe("a", 0)
		observe("b", time.Second)
	}
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	for range 2 {
		observe("a", 3*time.Second)
		observe("b", time.Second)
	}
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	setHealth(t, registry, "a", Unhealthy)
	assertWindowArm(t, p, "a", 2, 0.5)
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	setHealth(t, registry, "a", Healthy)
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	setHealth(t, registry, "a", Unhealthy)
	for range 4 {
		observe("b", time.Second)
	}
	assertWindowArm(t, p, "a", 0, 0)
	setHealth(t, registry, "a", Healthy)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	index, ok := tracker.begin("a")
	if !ok {
		t.Fatal("missing arm reservation")
	}
	assertSelection(t, p, registry.Eligible(), backends[1], true)
	tracker.finish(index)
	for range 2 {
		observe("a", 0)
		observe("b", time.Second)
	}
	assertSelection(t, p, registry.Eligible(), backends[0], true)
	observe("b", time.Second)
	observe("a", time.Second)
	observe("b", time.Second)
	observe("a", time.Second)
	assertSelection(t, p, registry.Eligible(), backends[0], true)
}

func TestSlidingWindowUCBConcurrentEviction(t *testing.T) {
	const size = 17
	registry := newTestRegistry(t, "a", "b", "c")
	p, _, algorithm := testSlidingWindowUCB(t, registry, size)
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
	if len(algorithm.window) != size || algorithm.next != (96-size)%size {
		t.Fatalf("unexpected ring after concurrent updates: size %d, next %d", len(algorithm.window), algorithm.next)
	}
	for _, backend := range candidates {
		var count uint64
		for _, reward := range algorithm.window {
			if reward.backendID == backend.ID {
				count++
			}
		}
		assertWindowArm(t, p, backend.ID, count, float64(count)*0.5)
	}
}

func testSlidingWindowUCB(t *testing.T, registry *Registry, size int) (*banditPolicy[windowedArmState], *requestTracker, *slidingWindowUCB) {
	t.Helper()
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicySlidingWindowUCB, MABLatencyScale: time.Second, MABSWWindow: size}, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	p := policy.(*banditPolicy[windowedArmState])
	return p, tracker, p.algorithm.(*slidingWindowUCB)
}

func assertWindowArm(t *testing.T, p *banditPolicy[windowedArmState], id string, count uint64, sum float64) {
	t.Helper()
	state := p.arms[id]
	if got := state.rewardSum + state.correction; state.observations != count || got != sum {
		t.Fatalf("arm %s has %d rewards summing to %g, want %d summing to %g", id, state.observations, got, count, sum)
	}
}
