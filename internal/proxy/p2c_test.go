package proxy

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestP2CSelectsFromDistinctSample(t *testing.T) {
	ids := []string{"e", "d", "c", "b", "a", "f", "g"}
	for _, n := range []int{2, 3, 5, 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			candidates := newTestRegistry(t, ids[:n]...).Eligible()
			tracker := newRequestTracker(candidates)
			const seed = 42
			policy := testP2CPolicy(t, tracker, seed)
			sampler := testP2CPolicy(t, tracker, seed)
			reference := rand.New(rand.NewPCG(seed, 0))
			pairCounts := make(map[[2]int]int)
			const trials = 14000
			for k := range trials {
				i, j := referenceP2CPair(reference, n)
				first, second := sampler.sample(n)
				if first != i || second != j || first == second {
					t.Fatalf("sample = (%d, %d), want distinct pair (%d, %d)", first, second, i, j)
				}
				pairCounts[[2]int{first, second}]++
				switch k % 3 {
				case 0:
					assertSelection(t, policy, candidates, candidates[i], true)
				default:
					busy, want := i, j
					if k%3 == 2 {
						busy, want = j, i
					}
					index, ok := tracker.begin(candidates[busy].ID)
					if !ok {
						t.Fatal("missing tracker entry", candidates[busy].ID)
					}
					assertSelection(t, policy, candidates, candidates[want], true)
					tracker.finish(index)
				}
			}
			mean := trials / (n * (n - 1))
			if len(pairCounts) != n*(n-1) {
				t.Fatalf("sampled %d distinct ordered pairs, want %d", len(pairCounts), n*(n-1))
			}
			for pair, count := range pairCounts {
				if count < mean*7/10 || count > mean*13/10 {
					t.Errorf("samples of pair %v = %d, expected near %d", pair, count, mean)
				}
			}
		})
	}
}

func TestP2CSeedAndEligibility(t *testing.T) {
	registry := newTestRegistry(t, "a", "b", "c", "d", "e")
	all := registry.Eligible()
	tracker := newRequestTracker(all)
	policy := testP2CPolicy(t, tracker, -42)
	reference := testP2CPolicy(t, tracker, -42)
	for _, ids := range [][]string{{}, {"a"}, {"a", "c", "e"}, {"a", "b", "c", "d", "e"}} {
		for _, candidate := range all {
			setHealth(t, registry, candidate.ID, Unhealthy)
		}
		for _, id := range ids {
			setHealth(t, registry, id, Healthy)
		}
		candidates := registry.Eligible()
		if len(candidates) == 0 {
			assertSelection(t, policy, candidates, Backend{}, false)
			continue
		}
		if len(candidates) == 1 {
			assertSelection(t, policy, candidates, candidates[0], true)
			continue
		}
		for range 40 {
			want, _ := reference.Select(candidates)
			assertSelection(t, policy, candidates, want, true)
		}
	}
}

func TestP2CConcurrentSampling(t *testing.T) {
	candidates := newTestRegistry(t, "a", "b", "c", "d", "e").Eligible()
	policy := testP2CPolicy(t, newRequestTracker(candidates), 1)
	const workers, draws = 16, 128
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for range draws {
				i, j := policy.sample(len(candidates))
				if i == j || i < 0 || j < 0 || i >= len(candidates) || j >= len(candidates) {
					t.Errorf("invalid sample (%d, %d)", i, j)
				}
			}
		})
	}
	group.Wait()
}

func testP2CPolicy(t *testing.T, tracker *requestTracker, seed int64) *p2cPolicy {
	t.Helper()
	policy, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicyP2C, P2CSeed: seed}, tracker)
	if err != nil {
		t.Fatal(err)
	}
	return policy.(*p2cPolicy)
}

func referenceP2CPair(rng *rand.Rand, n int) (int, int) {
	first := rng.IntN(n)
	offset := rng.IntN(n - 1)
	second := offset
	if offset >= first {
		second = offset + 1
	}
	return first, second
}
