package proxy

import (
	"fmt"
	"math"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

func testPendingWindow(t *testing.T, registry *Registry, size int, penalty float64) (*banditPolicy[windowedArmState], *requestTracker, *slidingWindowUCB) {
	t.Helper()
	backends := registry.Snapshot()
	tracker := newRequestTracker(backends)
	policy, err := newRoutingPolicy(config.ProxyConfig{
		Policy: config.PolicySlidingWindowUCBPending, MABLatencyScale: time.Second, MABSWWindow: size, MABPendingPenalty: penalty,
	}, backends, tracker)
	if err != nil {
		t.Fatal(err)
	}
	p := policy.(*banditPolicy[windowedArmState])
	return p, tracker, p.algorithm.(*pendingSlidingWindowUCB).slidingWindowUCB
}

func TestPendingWindowScores(t *testing.T) {
	for _, tt := range []struct {
		name    string
		penalty float64
		load    [2]int
		sums    [2]float64
		want    int
	}{
		{"zero penalty", 0, [2]int{5, 0}, [2]float64{85, 65}, 0},
		{"zero outstanding", .05, [2]int{}, [2]float64{85, 65}, 0},
		{"learning overrides load", .05, [2]int{3, 0}, [2]float64{85, 65}, 0},
		{"penalty changes winner", .05, [2]int{5, 0}, [2]float64{85, 65}, 1},
		{"negative maximum", 1, [2]int{5, 4}, [2]float64{85, 65}, 1},
		{"exact tie", .5, [2]int{1, 1}, [2]float64{50, 50}, 0},
		{"same as least requests", .05, [2]int{2, 1}, [2]float64{50, 50}, 1},
		{"overflow ordering", math.MaxFloat64, [2]int{3, 2}, [2]float64{85, 65}, 1},
		{"large equal load retains learning", math.MaxFloat64, [2]int{2, 2}, [2]float64{65, 85}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			registry := newTestRegistry(t, "a", "b")
			p, tracker, window := testPendingWindow(t, registry, 200, tt.penalty)
			for i, backend := range registry.Snapshot() {
				p.arms[backend.ID] = windowedArmState{ucb1State: ucb1State{rewardSum: tt.sums[i], observations: 100}}
				for range 100 {
					window.window = append(window.window, windowReward{backend.ID, tt.sums[i] / 100})
				}
				for range tt.load[i] {
					tracker.begin(backend.ID)
				}
			}
			candidates := registry.Eligible()
			assertSelection(t, p, candidates, candidates[tt.want], true)
			var trace telemetry.BanditDecision
			got, _ := p.selectDiagnostic(candidates, &trace)
			if got.ID != candidates[tt.want].ID || trace.Reason != "maximum_ucb" || !slices.Equal(trace.Eligible, []string{"a", "b"}) {
				t.Fatalf("selected %+v, decision %+v", got, trace)
			}
			for i, arm := range trace.Arms {
				ucb := tt.sums[i]/100 + math.Sqrt(2*math.Log(200)/100)
				wantPenalty := min(math.MaxFloat64, tt.penalty*float64(tt.load[i]))
				if math.Abs(arm.Score-ucb) > 1e-12 || arm.Pending == nil || arm.Pending.Count != tt.load[i] ||
					arm.Pending.Penalty != wantPenalty || arm.Pending.AdjustedScore != arm.Score-wantPenalty ||
					math.IsNaN(arm.Pending.AdjustedScore) || math.IsInf(arm.Pending.AdjustedScore, 0) {
					t.Fatalf("arm diagnostic = %+v, pending %+v", arm, arm.Pending)
				}
			}
		})
	}
}

func TestPendingWindowZeroEquivalence(t *testing.T) {
	for _, count := range []int{1, 3, 5} {
		for _, size := range []int{1, 3, 17, 1000} {
			t.Run(fmt.Sprintf("%d/%d", count, size), func(t *testing.T) {
				ids := []string{"a", "b", "c", "d", "e"}[:count]
				registry := newTestRegistry(t, ids...)
				parent, parentTracker, parentWindow := testSlidingWindowUCB(t, registry, size)
				candidate, tracker, window := testPendingWindow(t, registry, size, 0)
				type assigned struct {
					id    string
					index int
				}
				var pending []assigned
				now := time.Unix(100, 0)
				complete := func(index, step int) {
					task := pending[index]
					o := newTestObservation(task.id, now.Add(-time.Duration(step)), time.Duration(step%13)*time.Millisecond, UpstreamResponse, http.StatusOK)
					switch step % 7 {
					case 0:
						o.Outcome = Canceled
					case 1:
						o.Outcome = Interrupted
					case 2:
						o.UpstreamStatus = http.StatusFound
					case 3:
						o.UpstreamStatus = http.StatusBadRequest
					case 4:
						o.UpstreamStatus = http.StatusServiceUnavailable
					case 5:
						o.Outcome, o.UpstreamStatus = TransportFailure, 0
					case 6:
						o.Outcome, o.UpstreamStatus = UpstreamTimeout, 0
					}
					if step%11 == 0 {
						o.Outcome, o.UpstreamStatus = UpstreamResponse, http.StatusOK
					}
					tracker.finish(task.index)
					parentTracker.finish(task.index)
					candidate.observe(o)
					parent.observe(o)
					pending = slices.Delete(pending, index, index+1)
				}
				for step := 0; step < 240; step++ {
					if step%13 == 0 {
						id := ids[(step/13)%count]
						health := Healthy
						if step%26 == 0 {
							health = Unhealthy
						}
						setHealth(t, registry, id, health)
					}
					eligible := registry.Eligible()
					want, wantOK := parent.Select(eligible)
					got, ok := candidate.Select(eligible)
					var trace telemetry.BanditDecision
					traced, tracedOK := candidate.selectDiagnostic(eligible, &trace)
					if got != want || ok != wantOK || traced != want || tracedOK != wantOK {
						t.Fatalf("step %d: got %+v/%t, traced %+v/%t, want %+v/%t", step, got, ok, traced, tracedOK, want, wantOK)
					}
					if ok {
						index, _ := tracker.begin(got.ID)
						parentIndex, _ := parentTracker.begin(want.ID)
						if index != parentIndex {
							t.Fatal("reservation indices differ")
						}
						pending = append(pending, assigned{got.ID, index})
					}
					if len(pending) > 2 && step%3 != 0 {
						complete(len(pending)/2, step)
					}
					if !reflect.DeepEqual(candidate.arms, parent.arms) || !slices.Equal(window.window, parentWindow.window) ||
						window.next != parentWindow.next || !slices.Equal(tracker.Snapshot(), parentTracker.Snapshot()) {
						t.Fatalf("state diverged at step %d", step)
					}
				}
				for len(pending) > 0 {
					complete(len(pending)-1, 240+len(pending))
				}
				if !reflect.DeepEqual(candidate.arms, parent.arms) || !slices.Equal(window.window, parentWindow.window) || window.next != parentWindow.next {
					t.Fatal("terminal feedback changed equivalence")
				}
				for _, load := range tracker.Snapshot() {
					if load.Count != 0 {
						t.Fatalf("leaked reservation %+v", load)
					}
				}
			})
		}
	}
}

func TestPendingWindowColdHealthAndEviction(t *testing.T) {
	for _, count := range []int{1, 3, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ids := []string{"a", "b", "c", "d", "e"}[:count]
			registry := newTestRegistry(t, ids...)
			p, tracker, window := testPendingWindow(t, registry, 1, .05)
			other, _, otherWindow := testPendingWindow(t, registry, 1, .05)
			for _, backend := range registry.Snapshot() {
				assertSelection(t, p, registry.Eligible(), backend, true)
				tracker.begin(backend.ID)
			}
			all := registry.Snapshot()
			assertSelection(t, p, registry.Eligible(), all[0], true)
			p.observe(newTestObservation("a", time.Unix(100, 0), 0, UpstreamResponse, 200))
			assertSelection(t, p, registry.Eligible(), all[0], true)
			for _, backend := range all {
				setHealth(t, registry, backend.ID, Unhealthy)
			}
			assertSelection(t, p, registry.Eligible(), Backend{}, false)
			p.observe(newTestObservation("a", time.Unix(90, 0), 0, TransportFailure, 0))
			assertWindowArm(t, p, "a", 1, 0)
			if count > 1 {
				p.observe(newTestObservation(ids[1], time.Unix(80, 0), 0, UpstreamResponse, 200))
				assertWindowArm(t, p, "a", 0, 0)
			}
			setHealth(t, registry, "a", Healthy)
			tracker.finish(0)
			recovered, _ := registry.Lookup("a")
			assertSelection(t, p, registry.Eligible(), recovered, true)
			if len(window.window) != 1 || len(otherWindow.window) != 0 || other.arms["a"].observations != 0 {
				t.Fatal("window isolation failed")
			}
		})
	}
}

func TestPendingWindowConfigurationBoundaries(t *testing.T) {
	backends := newTestRegistry(t, "a").Snapshot()
	for _, penalty := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicySlidingWindowUCBPending, MABLatencyScale: time.Second, MABSWWindow: 1, MABPendingPenalty: penalty}, backends, newRequestTracker(backends))
		if err == nil {
			t.Fatalf("accepted coefficient %g", penalty)
		}
	}
	for _, size := range []int{0, -1} {
		_, err := newRoutingPolicy(config.ProxyConfig{Policy: config.PolicySlidingWindowUCBPending, MABLatencyScale: time.Second, MABSWWindow: size, MABPendingPenalty: .05}, backends, newRequestTracker(backends))
		if err == nil {
			t.Fatalf("accepted size %d", size)
		}
	}
}

func TestPendingWindowNumericalHistory(t *testing.T) {
	registry := newTestRegistry(t, "a", "b")
	p, _, _ := testPendingWindow(t, registry, 2, .05)
	now := time.Unix(100, 0)
	p.observe(newTestObservation("a", now, 0, UpstreamResponse, http.StatusOK))
	delayed := newTestObservation("a", now, math.MaxInt64, UpstreamResponse, http.StatusOK)
	tiny, _ := banditReward(delayed, time.Second)
	p.observe(delayed)
	p.observe(newTestObservation("b", now, 0, UpstreamResponse, http.StatusServiceUnavailable))
	assertWindowArm(t, p, "a", 1, tiny)
	assertSelection(t, p, registry.Eligible(), registry.Eligible()[0], true)
}

func TestPendingWindowConcurrentUpdates(t *testing.T) {
	const size = 17
	registry := newTestRegistry(t, "a", "b", "c")
	p, tracker, window := testPendingWindow(t, registry, size, .05)
	backends := registry.Eligible()
	var workers sync.WaitGroup
	for i := range 96 {
		workers.Go(func() {
			backend, ok := p.Select(backends)
			if !ok {
				t.Error("selection failed")
				return
			}
			index, _ := tracker.begin(backend.ID)
			p.observe(newTestObservation(backend.ID, time.Unix(100, int64(96-i)), time.Second, UpstreamResponse, http.StatusOK))
			tracker.finish(index)
		})
	}
	workers.Wait()
	if len(window.window) != size {
		t.Fatalf("window size = %d", len(window.window))
	}
	for _, backend := range backends {
		var count uint64
		for _, sample := range window.window {
			if sample.backendID == backend.ID {
				count++
			}
		}
		assertWindowArm(t, p, backend.ID, count, float64(count)*.5)
		assertInFlight(t, tracker, backend.ID, 0)
	}
}

func BenchmarkPendingWindowSelection(b *testing.B) {
	for _, count := range []int{1, 3, 5} {
		for _, mode := range []struct {
			name    string
			policy  config.ProxyPolicy
			penalty float64
		}{
			{"parent", config.PolicySlidingWindowUCB, 0},
			{"zero", config.PolicySlidingWindowUCBPending, 0},
			{"pending", config.PolicySlidingWindowUCBPending, .05},
		} {
			b.Run(fmt.Sprintf("%d/%s", count, mode.name), func(b *testing.B) {
				backends := make([]Backend, count)
				for i := range backends {
					backends[i].ID = fmt.Sprintf("backend-%d", i)
				}
				tracker := newRequestTracker(backends)
				policy, err := newRoutingPolicy(config.ProxyConfig{Policy: mode.policy, MABLatencyScale: time.Second, MABSWWindow: 1000, MABPendingPenalty: mode.penalty}, backends, tracker)
				if err != nil {
					b.Fatal(err)
				}
				p := policy.(*banditPolicy[windowedArmState])
				for i := range 1000 {
					p.observe(newTestObservation(backends[i%count].ID, time.Unix(100, 0), time.Duration(i%7)*time.Millisecond, UpstreamResponse, 200))
				}
				for i, backend := range backends {
					for range i * 3 {
						tracker.begin(backend.ID)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, ok := p.Select(backends); !ok {
						b.Fatal("selection failed")
					}
				}
			})
		}
	}
}
