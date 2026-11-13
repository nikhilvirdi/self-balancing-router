package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestPeakEWMAEstimator(t *testing.T) {
	const initial, decay = 10 * time.Millisecond, 10 * time.Second
	start := time.Unix(100, 0)
	e := latencyEstimate{assignedAt: start}
	assertLatency(t, e.score(start, 2, initial, decay), 30*time.Millisecond)
	e.observe(start, 4*time.Millisecond, decay)
	assertLatency(t, e.nanos, 4*time.Millisecond)
	e.observe(start.Add(time.Second), 100*time.Millisecond, decay)
	assertLatency(t, e.nanos, 100*time.Millisecond)
	last := start.Add(time.Second + decay)
	e.observe(last, 10*time.Millisecond, decay)
	want := float64(10*time.Millisecond) + math.Exp(-1)*float64(90*time.Millisecond)
	assertLatency(t, e.nanos, time.Duration(want))
	e.observe(last.Add(2*decay), 10*time.Millisecond, decay)
	want = float64(10*time.Millisecond) + math.Exp(-2)*(want-float64(10*time.Millisecond))
	assertLatency(t, e.nanos, time.Duration(want))
	last = e.observedAt
	idle := e.score(last.Add(2*decay), 0, initial, decay)
	assertLatency(t, idle, time.Duration(1+math.Exp(-1)*(want-1)))
	assertLatency(t, e.score(last.Add(2*decay), 1, initial, decay), time.Duration(2*want))
	before := e
	for _, input := range []struct {
		at       time.Time
		duration time.Duration
	}{{last.Add(-time.Nanosecond), time.Hour}, {last.Add(time.Second), -time.Millisecond}, {time.Time{}, time.Hour}} {
		e.observe(input.at, input.duration, decay)
	}
	if e != before {
		t.Fatalf("invalid or stale samples changed estimate: %+v", e)
	}
	zeroSampleTime := last.Add(time.Second)
	e.observe(zeroSampleTime, 0, decay)
	if e.observedAt != zeroSampleTime {
		t.Fatalf("zero-duration observation did not update observedAt: %+v", e)
	}
	want = math.Exp(-1/float64(decay/time.Second))*(want-1) + 1
	assertLatency(t, e.nanos, time.Duration(want))
	e.observe(zeroSampleTime, math.MaxInt64, decay)
	assertLatency(t, e.nanos, math.MaxInt64)
	if got := e.score(zeroSampleTime, math.MaxInt, initial, decay); math.IsInf(got, 0) || math.IsNaN(got) || got <= 0 {
		t.Fatalf("invalid extreme score %v", got)
	}
	assertLatency(t, e.score(last.Add(900*decay), 0, initial, decay), time.Nanosecond)
	e.observe(last.Add(900*decay), time.Nanosecond, decay)
	assertLatency(t, e.nanos, time.Nanosecond)
}

func TestPeakEWMASampledScores(t *testing.T) {
	ids := []string{"e", "d", "c", "b", "a", "f", "g"}
	now := time.Unix(100, 0)
	for _, n := range []int{0, 1, 2, 5, 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			candidates := newTestRegistry(t, ids[:n]...).Eligible()
			p := testPeakEWMAPolicy(candidates, now)
			if n < 2 {
				var want Backend
				if n == 1 {
					want = candidates[0]
				}
				if got, ok := p.selectAt(candidates, now); got != want || ok != (n == 1) {
					t.Fatalf("selection = %v, %t, want %v, %t", got, ok, want, n == 1)
				}
				return
			}
			ref := rand.New(rand.NewPCG(42, 0))
			for round := range 2 {
				if round == 1 {
					for k, backend := range candidates {
						p.observe(successfulSample(backend.ID, now, time.Duration(n-k)*time.Millisecond))
						for range k {
							p.tracker.begin(backend.ID)
						}
					}
				}
				for range 50 {
					i, j := referenceP2CPair(ref, n)
					// Round 0 has tied cold scores. Round 1 has scores of (n-k)*(k+1).
					if round == 1 && (n-j)*(j+1) < (n-i)*(i+1) {
						i = j
					}
					if got, ok := p.selectAt(candidates, now); !ok || got != candidates[i] {
						t.Fatalf("selection = %v, %t, want %s", got, ok, candidates[i].ID)
					}
				}
			}
		})
	}
}

func TestPeakEWMAColdBackendGetsTraffic(t *testing.T) {
	candidates := newTestRegistry(t, "fast", "cold").Eligible()
	now := time.Unix(100, 0)
	p := testPeakEWMAPolicy(candidates, now)
	p.observe(successfulSample("fast", now, 2*time.Millisecond))
	if got, _ := p.selectAt(candidates, now); got.ID != "fast" {
		t.Fatalf("cold start selected %s, want fast", got.ID)
	}
	later := now.Add(4 * p.decay)
	p.observe(successfulSample("fast", later, 2*time.Millisecond))
	if got, _ := p.selectAt(candidates, later); got.ID != "cold" {
		t.Fatalf("after idle decay selected %s, want cold", got.ID)
	}
	if got, _ := p.selectAt(candidates, later); got.ID != "fast" {
		t.Fatalf("after cold assignment selected %s, want fast", got.ID)
	}
}

func TestPeakEWMADegradationAndRecovery(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	now := time.Unix(100, 0)
	p := testPeakEWMAPolicy(registry.Snapshot(), now)
	check := func(at time.Time, id string) {
		t.Helper()
		if got, ok := p.selectAt(registry.Eligible(), at); !ok || got.ID != id {
			t.Fatalf("selected %q, %t, want %s", got.ID, ok, id)
		}
	}
	p.observe(successfulSample("a", now, 2*time.Millisecond))
	p.observe(successfulSample("b", now, 10*time.Millisecond))
	check(now, "a")
	now = now.Add(time.Second)
	p.observe(successfulSample("a", now, 50*time.Millisecond))
	check(now, "b")
	setHealth(t, registry, "b", Unhealthy)
	check(now, "a")
	setHealth(t, registry, "b", Healthy)
	check(now, "b")
	now = now.Add(p.decay)
	p.observe(successfulSample("a", now, 2*time.Millisecond))
	p.observe(successfulSample("b", now, 10*time.Millisecond))
	check(now, "b")
	now = now.Add(p.decay)
	p.observe(successfulSample("a", now, 2*time.Millisecond))
	p.observe(successfulSample("b", now, 10*time.Millisecond))
	check(now, "a")
}

func TestPeakEWMAFeedbackFilteringAndConcurrency(t *testing.T) {
	candidates := newTestRegistry(t, "a", "b").Eligible()
	now := time.Unix(100, 0)
	p := testPeakEWMAPolicy(candidates, now)
	latest := now.Add(time.Second)
	p.observe(successfulSample("a", latest, 50*time.Millisecond))
	for _, obs := range []Observation{
		{BackendID: "a", FinishedAt: latest, Duration: time.Hour, Outcome: UpstreamResponse, UpstreamStatus: http.StatusServiceUnavailable},
		{BackendID: "a", FinishedAt: latest, Duration: time.Hour, Outcome: Interrupted, UpstreamStatus: http.StatusOK},
		{BackendID: "a", FinishedAt: latest, Duration: time.Hour, Outcome: Canceled, UpstreamStatus: http.StatusOK},
		{BackendID: "a", FinishedAt: latest, Duration: time.Hour, Outcome: UpstreamTimeout},
		{BackendID: "a", FinishedAt: latest, Duration: time.Hour, Outcome: TransportFailure},
		successfulSample("missing", latest, time.Hour),
	} {
		p.observe(obs)
	}
	assertLatency(t, p.latency["a"].nanos, 50*time.Millisecond)
	if !p.latency["a"].observedAt.Equal(latest) || len(p.latency) != 2 {
		t.Fatalf("invalid feedback changed state: %+v", p.latency["a"])
	}
	var group sync.WaitGroup
	for i := range 16 {
		group.Go(func() {
			for range 64 {
				p.observe(successfulSample("a", now, time.Hour))
				p.observe(successfulSample("b", now.Add(time.Duration(i)*time.Nanosecond), time.Millisecond))
				if _, ok := p.selectAt(candidates, latest); !ok {
					t.Error("selection failed")
				}
			}
		})
	}
	group.Wait()
	assertLatency(t, p.latency["a"].nanos, 50*time.Millisecond)
	if want := now.Add(15 * time.Nanosecond); !p.latency["b"].observedAt.Equal(want) {
		t.Fatalf("latest completion = %s, want %s", p.latency["b"].observedAt, want)
	}
}

func TestPeakEWMAHTTPFeedback(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	urls := make([]string, 2)
	for i := range urls {
		id := fmt.Sprintf("backend-%c", 'a'+i)
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/hold":
				entered <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			case "/overload":
				http.Error(w, "backend overloaded", http.StatusServiceUnavailable)
				return
			case "/cancel":
				entered <- struct{}{}
				<-r.Context().Done()
				return
			}
			_, _ = io.WriteString(w, id)
		}))
		t.Cleanup(backend.Close)
		urls[i] = backend.URL
	}
	registry := healthRegistry(t, urls...)
	p := testPeakEWMAPolicy(registry.Snapshot(), time.Now())
	observations := make(chan Observation, 4)
	server := httptest.NewServer(observedPolicyHandler(t, registry, p.tracker, p, testTimeout, func(o Observation) { observations <- o }))
	t.Cleanup(server.Close)
	server.Client().Timeout = testTimeout

	request := func(id string) Observation {
		assertResponse(t, server, http.MethodGet, "/items", http.StatusOK, id)
		return assertObservation(t, observations, id, http.StatusOK, UpstreamResponse)
	}
	startHeld := func(path string) (<-chan requestResult, context.CancelFunc) {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		result := startObservedRequest(t, ctx, server, path)
		waitFor(t, entered)
		return result, cancel
	}

	setHealth(t, registry, "backend-a", Unhealthy)
	slow, _ := startHeld("/hold")
	setHealth(t, registry, "backend-b", Unhealthy)
	setHealth(t, registry, "backend-a", Healthy)
	fast := request("backend-a")
	// Guarantee that the held request outlasts the actual fast sample.
	timer := time.NewTimer(max(3*fast.Duration, 5*time.Millisecond))
	waitFor(t, timer.C)
	close(release)
	if got := waitFor(t, slow); got.err != nil || got.body != "backend-b" {
		t.Fatalf("held response = %+v, want backend-b", got)
	}
	peak := assertObservation(t, observations, "backend-b", http.StatusOK, UpstreamResponse)
	if peak.Duration < 3*fast.Duration {
		t.Fatalf("held duration %s, want at least %s", peak.Duration, 3*fast.Duration)
	}
	setHealth(t, registry, "backend-b", Healthy)
	request("backend-a")
	setHealth(t, registry, "backend-a", Unhealthy)
	p.mu.Lock()
	before := *p.latency["backend-b"]
	p.mu.Unlock()
	assertLatency(t, before.nanos, peak.Duration)
	assertResponse(t, server, http.MethodGet, "/overload", http.StatusServiceUnavailable, "backend overloaded\n")
	assertObservation(t, observations, "backend-b", http.StatusServiceUnavailable, UpstreamResponse)
	canceled, cancel := startHeld("/cancel")
	assertInFlight(t, p.tracker, "backend-b", 1)
	cancel()
	if got := waitFor(t, canceled); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled response = %+v, want client cancellation", got)
	}
	assertObservation(t, observations, "backend-b", 0, Canceled)
	assertInFlight(t, p.tracker, "backend-b", 0)
	setHealth(t, registry, "backend-b", Unhealthy)
	assertResponse(t, server, http.MethodGet, "/items", http.StatusServiceUnavailable, "request routing is unavailable\n")
	assertNoObservation(t, observations)

	setHealth(t, registry, "backend-b", Healthy)
	p.mu.Lock()
	after := *p.latency["backend-b"]
	p.mu.Unlock()
	if after.observedAt != before.observedAt || after.nanos != before.nanos {
		t.Fatalf("overload, cancellation, or eligibility change altered estimate: before %+v, after %+v", before, after)
	}
	recovered := request("backend-b")
	p.mu.Lock()
	after = *p.latency["backend-b"]
	p.mu.Unlock()
	want := float64(max(recovered.Duration, time.Nanosecond))
	if recovered.Duration <= peak.Duration {
		alpha := math.Exp(-float64(recovered.FinishedAt.Sub(peak.FinishedAt)) / float64(p.decay))
		want = alpha*float64(peak.Duration) + (1-alpha)*want
	}
	if math.Abs(after.nanos-want) > 1 {
		t.Fatalf("estimate = %g ns, want %g ns, peak %+v, recovery %+v, stored %+v", after.nanos, want, peak, recovered, after)
	}
	for _, id := range []string{"backend-a", "backend-b"} {
		assertInFlight(t, p.tracker, id, 0)
	}
	assertNoObservation(t, observations)
}

func testPeakEWMAPolicy(backends []Backend, now time.Time) *peakEWMAPolicy {
	return newPeakEWMAPolicy(config.ProxyConfig{
		P2CSeed: 42, PeakEWMADecay: 10 * time.Second, PeakEWMAInitialRTT: 10 * time.Millisecond,
	}, newRequestTracker(backends), now)
}

func successfulSample(id string, finished time.Time, duration time.Duration) Observation {
	return Observation{BackendID: id, FinishedAt: finished, Duration: duration, Outcome: UpstreamResponse, UpstreamStatus: http.StatusOK}
}

func assertLatency(t *testing.T, nanos float64, want time.Duration) {
	t.Helper()
	if math.IsNaN(nanos) || math.Abs(nanos-float64(want)) > 1 {
		t.Fatalf("latency = %g ns, want %s", nanos, want)
	}
}

func TestPeakEWMAZeroDurationObservation(t *testing.T) {
	const decay = 10 * time.Second
	now := time.Unix(100, 0)

	// Direct latencyEstimate observation of zero duration
	var e latencyEstimate
	e.observe(now, 0, decay)
	if e.observedAt != now {
		t.Fatalf("observedAt = %v, want %v", e.observedAt, now)
	}
	if e.nanos != float64(time.Nanosecond) {
		t.Fatalf("nanos = %g, want %g", e.nanos, float64(time.Nanosecond))
	}

	// Negative duration must be rejected
	later := now.Add(time.Second)
	e.observe(later, -time.Millisecond, decay)
	if e.observedAt != now || e.nanos != float64(time.Nanosecond) {
		t.Fatalf("negative duration was not rejected: %+v", e)
	}

	// Policy observe should clamp zero-duration observation to 1ns
	backends := []Backend{{ID: "backend-a"}, {ID: "backend-b"}}
	policy := testPeakEWMAPolicy(backends, now)
	policy.observe(successfulSample("backend-a", later, 0))

	policy.mu.Lock()
	est := *policy.latency["backend-a"]
	policy.mu.Unlock()

	if est.observedAt != later {
		t.Fatalf("policy observedAt = %v, want %v", est.observedAt, later)
	}
	if est.nanos != float64(time.Nanosecond) {
		t.Fatalf("policy nanos = %g, want %g", est.nanos, float64(time.Nanosecond))
	}
}

func TestPeakEWMAColdStartAndZeroDurationSelection(t *testing.T) {
	const initial = 10 * time.Millisecond
	now := time.Unix(100, 0)
	backends := []Backend{{ID: "backend-a"}, {ID: "backend-b"}}
	policy := testPeakEWMAPolicy(backends, now)

	// In cold start, both backends score initialRTT * (0 + 1) = 10ms
	policy.mu.Lock()
	scoreA := policy.score("backend-a", 0, now)
	scoreB := policy.score("backend-b", 0, now)
	policy.mu.Unlock()

	if scoreA != float64(initial) || scoreB != float64(initial) {
		t.Fatalf("cold-start scores = (%g, %g), want %g", scoreA, scoreB, float64(initial))
	}

	// Backend-a receives a zero-duration completion (clamped to 1ns)
	completionTime := now.Add(50 * time.Millisecond)
	policy.observe(successfulSample("backend-a", completionTime, 0))

	policy.mu.Lock()
	scoreA = policy.score("backend-a", 0, completionTime)
	scoreB = policy.score("backend-b", 0, completionTime)
	policy.mu.Unlock()

	// Backend-a should score 1ns, beating Backend-b's unobserved 10ms
	if scoreA >= scoreB {
		t.Fatalf("zero-duration backend-a score (%g) should be lower than cold backend-b score (%g)", scoreA, scoreB)
	}
	if scoreA != float64(time.Nanosecond) {
		t.Fatalf("backend-a score = %g, want 1ns (%g)", scoreA, float64(time.Nanosecond))
	}

	// Selection between candidate backends: backend-a must be chosen over backend-b
	selected, ok := policy.selectAt(backends, completionTime)
	if !ok || selected.ID != "backend-a" {
		t.Fatalf("selected = %v (ok=%v), want backend-a", selected, ok)
	}
}

func TestPeakEWMAMissingEstimatorDefensiveLookup(t *testing.T) {
	now := time.Unix(100, 0)
	known := []Backend{{ID: "backend-a"}}
	tracker := newRequestTracker(append(known, Backend{ID: "backend-b"}))
	policy := newPeakEWMAPolicy(config.ProxyConfig{
		P2CSeed: 42, PeakEWMADecay: 10 * time.Second, PeakEWMAInitialRTT: 10 * time.Millisecond,
	}, tracker, now)

	// backend-b is in tracker but intentionally deleted from policy.latency to simulate an unmapped candidate
	delete(policy.latency, "backend-b")
	candidates := []Backend{{ID: "backend-a"}, {ID: "backend-b"}}
	if got := policy.score("backend-b", math.MaxInt, now); math.IsInf(got, 0) || math.IsNaN(got) || got <= 0 {
		t.Fatalf("invalid fallback score %v", got)
	}

	// Must not panic, and should return a valid selection
	selected, ok := policy.selectAt(candidates, now)
	if !ok {
		t.Fatal("selectAt failed on unmapped backend candidate")
	}
	if selected.ID != "backend-a" && selected.ID != "backend-b" {
		t.Fatalf("selected = %v, want backend-a or backend-b", selected)
	}

	// Unmapped backend should be safe to observe without panic and should not corrupt latency map
	policy.observe(successfulSample("backend-b", now.Add(time.Second), 5*time.Millisecond))

	policy.mu.Lock()
	est := policy.latency["backend-b"]
	policy.mu.Unlock()

	if est != nil {
		t.Fatalf("unmapped backend unexpectedly created in latency map: %+v", est)
	}
}
