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
	return selectUCB(arms, a.tracker, func(state ucb1State) (float64, bool) {
		return state.selectionScore(a.total)
	})
}

func selectUCB[S any](arms []banditArm[S], tracker *requestTracker, score func(S) (float64, bool)) (Backend, bool) {
	var best, cold Backend
	var bestScore float64
	minPending := math.MaxInt
	// A zero-pending cold arm gets its first try. When all cold arms have
	// pending work, use the best observed arm or spread load among cold arms.
	for _, arm := range arms {
		value, observed := score(arm.State)
		if !observed {
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
		if best.ID == "" || value > bestScore {
			best, bestScore = arm.Backend, value
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

func (s ucb1State) selectionScore(total uint64) (float64, bool) {
	if s.observations == 0 {
		return 0, false
	}
	return s.score(total), true
}

// score is used only after an arm has at least one completed reward.
func (s ucb1State) score(total uint64) float64 {
	return ucbScore(s.rewardSum, float64(s.observations), float64(total))
}

func ucbScore(rewardSum, observations, total float64) float64 {
	return rewardSum/observations + math.Sqrt(2*math.Log(total)/observations)
}
