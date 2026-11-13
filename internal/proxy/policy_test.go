package proxy

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestFirstEligiblePolicy(t *testing.T) {
	policy := firstEligiblePolicy{}
	a, b := Backend{ID: "a"}, Backend{ID: "b"}
	assertSelection(t, policy, nil, Backend{}, false)
	assertSelection(t, policy, []Backend{a}, a, true)
	for range 3 {
		assertSelection(t, policy, []Backend{b, a}, b, true)
	}
	assertSelection(t, policy, []Backend{a}, a, true)
}

func TestLeastRequestsSelection(t *testing.T) {
	ids := []string{"e", "d", "c", "b", "a", "f", "g"}
	for _, count := range []int{1, 2, 3, 5, 7} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			registry := newTestRegistry(t, ids[:count]...)
			candidates := registry.Eligible()
			tracker := newRequestTracker(candidates)
			policy := leastRequestsPolicy{tracker: tracker}
			assertSelection(t, policy, nil, Backend{}, false)
			for range 3 {
				assertSelection(t, policy, candidates, candidates[0], true)
			}
			var active []int
			for _, candidate := range candidates[:count-1] {
				index, ok := tracker.begin(candidate.ID)
				if !ok {
					t.Fatal("missing tracker entry", candidate.ID)
				}
				active = append(active, index)
			}
			assertSelection(t, policy, candidates, candidates[count-1], true)
			for _, index := range active {
				tracker.finish(index)
			}
			assertSelection(t, policy, candidates, candidates[0], true)
		})
	}
}

func TestLeastRequestsHealthAndTies(t *testing.T) {
	registry := newTestRegistry(t, "c", "b", "a")
	candidates := registry.Eligible()
	tracker := newRequestTracker(candidates)
	policy := leastRequestsPolicy{tracker: tracker}
	var assigned []int
	for _, id := range []string{"c", "b", "a", "a"} {
		index, ok := tracker.begin(id)
		if !ok {
			t.Fatal("missing tracker entry", id)
		}
		assigned = append(assigned, index)
	}
	for range 3 {
		assertSelection(t, policy, registry.Eligible(), candidates[0], true)
	}
	setHealth(t, registry, "c", Unhealthy)
	assertSelection(t, policy, registry.Eligible(), candidates[1], true)
	assertInFlight(t, tracker, "c", 1)
	setHealth(t, registry, "c", Healthy)
	candidates[0].Health = Healthy
	assertSelection(t, policy, registry.Eligible(), candidates[0], true)
	for _, candidate := range candidates {
		setHealth(t, registry, candidate.ID, Unhealthy)
	}
	assertSelection(t, policy, registry.Eligible(), Backend{}, false)
	for _, index := range assigned {
		tracker.finish(index)
	}
	assertInFlight(t, tracker, "c", 0)
}

func TestRoundRobinCycles(t *testing.T) {
	for _, count := range []int{1, 2, 3, 5, 7} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			candidates := make([]Backend, count)
			for i := range candidates {
				candidates[i] = Backend{ID: fmt.Sprintf("backend-%d", count-i)}
			}
			policy := &roundRobinPolicy{}
			other := &roundRobinPolicy{}
			for i := range count*4 + 1 {
				assertSelection(t, policy, nil, Backend{}, false)
				assertSelection(t, policy, candidates, candidates[i%count], true)
			}
			assertSelection(t, other, candidates, candidates[0], true)
			assertSelection(t, policy, candidates, candidates[1%count], true)
		})
	}
}

func TestRoundRobinEligibilityChanges(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c", "d", "e")
	policy := &roundRobinPolicy{}
	all := registry.Eligible()
	assertSelection(t, policy, all, all[0], true)
	setHealth(t, registry, "b", Unhealthy)
	setHealth(t, registry, "d", Unhealthy)
	for _, i := range []int{2, 4, 0} {
		assertSelection(t, policy, registry.Eligible(), all[i], true)
	}
	for _, candidate := range all {
		setHealth(t, registry, candidate.ID, Unhealthy)
	}
	assertSelection(t, policy, registry.Eligible(), Backend{}, false)
	for _, candidate := range all {
		setHealth(t, registry, candidate.ID, Healthy)
	}
	want := all[4]
	want.Health = Healthy
	assertSelection(t, policy, registry.Eligible(), want, true)
}

func TestRoundRobinWraparound(t *testing.T) {
	policy := &roundRobinPolicy{}
	candidates := []Backend{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	policy.cursor.Store(math.MaxUint64 - 1)
	assertSelection(t, policy, candidates, candidates[(math.MaxUint64-1)%3], true)
	assertSelection(t, policy, candidates, candidates[math.MaxUint64%3], true)
	assertSelection(t, policy, candidates, candidates[0], true)
	assertSelection(t, policy, candidates, candidates[1], true)
}

func TestRoundRobinConcurrentSelections(t *testing.T) {
	const total = 1000
	candidates := newTestRegistry(t, "a", "b", "c", "d", "e").Eligible()
	policy := &roundRobinPolicy{}
	start := make(chan struct{})
	results := make(chan Backend, total)
	var workers sync.WaitGroup
	for range 20 {
		workers.Go(func() {
			<-start
			for range total / 20 {
				backend, _ := policy.Select(candidates)
				results <- backend
			}
		})
	}
	close(start)
	workers.Wait()
	close(results)
	counts := make(map[Backend]int)
	for backend := range results {
		counts[backend]++
	}
	for _, backend := range candidates {
		if got := counts[backend]; got != total/len(candidates) {
			t.Errorf("selections for %q = %d, want %d", backend.ID, got, total/len(candidates))
		}
	}
}

func assertSelection(t *testing.T, policy routingPolicy, candidates []Backend, want Backend, selected bool) {
	t.Helper()
	if got, ok := policy.Select(candidates); got != want || ok != selected {
		t.Fatalf("Select(%v) = %v, %t, want %v, %t", candidates, got, ok, want, selected)
	}
}
