package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

type recordingBandit struct{}

func (recordingBandit) Select(arms []banditArm[[]banditFeedback]) (Backend, bool) {
	return arms[0].Backend, true
}

func (recordingBandit) Update(arms map[string][]banditFeedback, feedback banditFeedback) {
	id := feedback.BackendID
	arms[id] = append(arms[id], feedback)
}

func TestBanditArms(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	for _, n := range []int{0, 1, 3, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			registry := newTestRegistry(t, ids[:n]...)
			p := testBanditPolicy(t, registry)
			independent := testBanditPolicy(t, registry)
			if len(p.arms) != n {
				t.Fatalf("registered %d arms, want %d", len(p.arms), n)
			}
			now := time.Unix(100, 0)
			for i, id := range ids[:n] {
				assertSelection(t, p, registry.Eligible(), registry.Eligible()[0], true)
				setHealth(t, registry, id, Unhealthy)
				// A request assigned before the failure can still deliver feedback.
				p.observe(newTestObservation(id, now, time.Second, UpstreamResponse, http.StatusOK))
				p.observe(newTestObservation(id, now.Add(-time.Second), 0, TransportFailure, 0))
				want := []banditFeedback{{id, now, 0.5}, {id, now.Add(-time.Second), 0}}
				if !slices.Equal(p.arms[id], want) || len(independent.arms[id]) != 0 {
					t.Fatalf("arm %s feedback = %v, independent = %v, want %v and empty", id, p.arms[id], independent.arms[id], want)
				}
				if i+1 == n {
					assertSelection(t, p, registry.Snapshot(), Backend{}, false)
				}
			}
			p.observe(newTestObservation("missing", now, 0, UpstreamResponse, http.StatusOK))
			assertSelection(t, p, []Backend{{ID: "missing"}}, Backend{}, false)
			if len(p.arms) != n {
				t.Fatalf("unknown backend changed arm count to %d", len(p.arms))
			}
			for _, id := range ids[:n] {
				setHealth(t, registry, id, Healthy)
				candidate, _ := registry.Lookup(id)
				assertSelection(t, p, []Backend{candidate}, candidate, true)
				if len(p.arms[id]) != 2 {
					t.Fatalf("recovered arm %s lost history: %v", id, p.arms[id])
				}
			}
		})
	}
	if _, err := newBanditPolicy(nil, 0, recordingBandit{}); err == nil {
		t.Fatal("expected invalid scale error")
	}
}

func TestBanditConcurrentUpdates(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c")
	p := testBanditPolicy(t, registry)
	candidates := registry.Eligible()
	const perArm = 64
	now := time.Unix(100, 0)
	var wg sync.WaitGroup
	for _, backend := range candidates {
		for i := range perArm {
			wg.Go(func() {
				p.observe(newTestObservation(backend.ID, now.Add(time.Duration(i)), time.Second, UpstreamResponse, http.StatusOK))
				if _, ok := p.Select(candidates); !ok {
					t.Error("selection failed with eligible backends")
				}
			})
		}
	}
	wg.Wait()
	for _, backend := range candidates {
		seen := make(map[time.Time]bool, perArm)
		for _, feedback := range p.arms[backend.ID] {
			if feedback.BackendID != backend.ID || feedback.Reward != 0.5 || seen[feedback.FinishedAt] {
				t.Fatalf("unexpected or duplicate feedback: %+v", feedback)
			}
			seen[feedback.FinishedAt] = true
		}
		if len(seen) != perArm {
			t.Fatalf("arm %s has %d updates, want %d", backend.ID, len(seen), perArm)
		}
	}
}

func TestBanditHTTPFeedback(t *testing.T) {
	holding, release, cancelEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Keep the body pending without triggering the upstream header timeout.
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		close(holding)
		select {
		case <-release:
			_, _ = io.WriteString(w, "a")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(first.Close)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/overload":
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
		case "/timeout":
			<-r.Context().Done()
		case "/cancel":
			close(cancelEntered)
			<-r.Context().Done()
		default:
			_, _ = io.WriteString(w, "b")
		}
	}))
	t.Cleanup(second.Close)
	closed := listenLoopback(t)
	registry := healthRegistry(t, first.URL, second.URL, "http://"+closed.Addr().String())
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	setHealth(t, registry, "backend-c", Unhealthy)
	p := testBanditPolicy(t, registry)
	tracker := newRequestTracker(registry.Snapshot())
	observations := make(chan Observation, 8)
	server := httptest.NewServer(observedPolicyHandler(t, registry, tracker, p, 500*time.Millisecond, func(o Observation) { observations <- o }))
	t.Cleanup(server.Close)
	server.Client().Timeout = testTimeout
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	held := startObservedRequest(t, ctx, server, "/hold")
	waitFor(t, holding)
	setHealth(t, registry, "backend-a", Unhealthy)
	assertResponse(t, server, http.MethodGet, "/items", 200, "b")
	fast := assertObservation(t, observations, "backend-b", 200, UpstreamResponse)
	close(release)
	if got := waitFor(t, held); got.err != nil || got.body != "a" {
		t.Fatalf("held response = %+v, want a", got)
	}
	delayed := assertObservation(t, observations, "backend-a", 200, UpstreamResponse)
	if delayed.StartedAt.After(fast.StartedAt) || delayed.FinishedAt.Before(fast.FinishedAt) {
		t.Fatalf("held observation did not span the second request: %+v, %+v", delayed, fast)
	}

	assertResponse(t, server, http.MethodGet, "/overload", 503, "overloaded\n")
	overloaded := assertObservation(t, observations, "backend-b", 503, UpstreamResponse)
	assertResponse(t, server, http.MethodGet, "/timeout", 504, "gateway timeout\n")
	timedOut := assertObservation(t, observations, "backend-b", 0, UpstreamTimeout)
	canceled := startObservedRequest(t, ctx, server, "/cancel")
	waitFor(t, cancelEntered)
	cancel()
	if got := waitFor(t, canceled); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled response = %+v, want cancellation", got)
	}
	assertObservation(t, observations, "backend-b", 0, Canceled)
	setHealth(t, registry, "backend-b", Unhealthy)
	setHealth(t, registry, "backend-c", Healthy)
	assertResponse(t, server, http.MethodGet, "/items", 502, "bad gateway\n")
	refused := assertObservation(t, observations, "backend-c", 0, TransportFailure)
	setHealth(t, registry, "backend-c", Unhealthy)
	assertResponse(t, server, http.MethodGet, "/items", 503, "request routing is unavailable\n")
	assertNoObservation(t, observations)
	for id, samples := range map[string][]Observation{
		"backend-a": {delayed}, "backend-b": {fast, overloaded, timedOut}, "backend-c": {refused},
	} {
		assertInFlight(t, tracker, id, 0)
		if len(p.arms[id]) != len(samples) {
			t.Fatalf("arm %s has %d updates, want %d", id, len(p.arms[id]), len(samples))
		}
		for i, sample := range samples {
			want := 0.0
			if sample.UpstreamStatus == http.StatusOK {
				want = 1 / (1 + float64(sample.Duration)/float64(time.Second))
			}
			if got := p.arms[id][i]; got.BackendID != id || got.FinishedAt != sample.FinishedAt || got.Reward != want {
				t.Fatalf("arm %s update %d = %+v, want reward %v at %v", id, i, got, want, sample.FinishedAt)
			}
		}
	}
}

func testBanditPolicy(t *testing.T, registry *Registry) *banditPolicy[[]banditFeedback] {
	t.Helper()
	p, err := newBanditPolicy(registry.Snapshot(), time.Second, recordingBandit{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
