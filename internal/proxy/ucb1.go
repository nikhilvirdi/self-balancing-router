package proxy

import "math"

type ucb1State struct {
	rewardSum    float64
	observations uint64
}

// The bandit policy lock protects both the arm states and this total, which
// includes rewards from arms that are temporarily ineligible.
type ucb1Algorithm struct {
	tracker *requestTracker
	total   uint64
}

func (a *ucb1Algorithm) Select(arms []banditArm[ucb1State]) (Backend, bool) {
	return selectUCB(arms, a.tracker, a.total, func(state ucb1State) ucb1State { return state })
}

func selectUCB[S any](arms []banditArm[S], tracker *requestTracker, total uint64, stats func(S) ucb1State) (Backend, bool) {
	var best, cold Backend
	var bestScore float64
	minPending := math.MaxInt
	// A zero-pending cold arm gets its first try. When all cold arms have
	// pending work, use the best observed arm or spread load among cold arms.
	for _, arm := range arms {
		state := stats(arm.State)
		if state.observations == 0 {
			pending, ok := tracker.InFlight(arm.Backend.ID)
			if !ok {
				continue
			}
			if pending == 0 {
				return arm.Backend, true
			}
			if cold.ID == "" || pending < minPending {
				cold, minPending = arm.Backend, pending
			}
			continue
		}
		score := state.score(total)
		if best.ID == "" || score > bestScore {
			best, bestScore = arm.Backend, score
		}
	}
	if best.ID != "" {
		return best, true
	}
	return cold, cold.ID != ""
}

func (a *ucb1Algorithm) Update(arms map[string]ucb1State, feedback banditFeedback) {
	state := arms[feedback.BackendID]
	if state.observations < math.MaxUint64 {
		state.observations++
		state.rewardSum += feedback.Reward
		arms[feedback.BackendID] = state
		if a.total < math.MaxUint64 {
			a.total++
		}
	}
}

// score is used only after an arm has at least one completed reward.
func (s ucb1State) score(total uint64) float64 {
	n := float64(s.observations)
	return s.rewardSum/n + math.Sqrt(2*math.Log(float64(total))/n)
}
