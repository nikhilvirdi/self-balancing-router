package proxy

import (
	"math"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type ucb1State struct {
	rewardSum    float64
	observations uint64
}

type ucbArmScore struct {
	telemetry.BanditArm
	pending int
}

// The bandit policy lock protects both the arm states and this total, which
// includes rewards from arms that are temporarily ineligible.
type ucb1Algorithm struct {
	tracker *requestTracker
	total   uint64
}

func (a *ucb1Algorithm) Select(arms []banditArm[ucb1State]) (Backend, bool) {
	return a.selectDiagnostic(arms, nil)
}

func (a *ucb1Algorithm) selectDiagnostic(arms []banditArm[ucb1State], trace *telemetry.BanditDecision) (Backend, bool) {
	return selectUCB(arms, a.tracker, trace, func(arm banditArm[ucb1State]) ucbArmScore {
		state := arm.State
		score, observed := state.selectionScore(a.total)
		return armDiagnostic(state.rewardSum, float64(state.observations), score, observed, trace != nil)
	}, nil)
}

func armDiagnostic(sum, count, score float64, observed, tracing bool) ucbArmScore {
	result := ucbArmScore{BanditArm: telemetry.BanditArm{Score: score, Count: count, Observed: observed}}
	if observed && tracing {
		result.Mean = sum / count
	}
	return result
}

func selectUCB[S any](arms []banditArm[S], tracker *requestTracker, trace *telemetry.BanditDecision, score func(banditArm[S]) ucbArmScore, better func(ucbArmScore, ucbArmScore) bool) (Backend, bool) {
	var best, cold Backend
	var bestScore ucbArmScore
	minPending := math.MaxInt
	// A zero-pending cold arm gets its first try. When all cold arms have
	// pending work, use the best observed arm or spread load among cold arms.
	for _, arm := range arms {
		result := score(arm)
		if trace != nil {
			result.BackendID = arm.Backend.ID
			trace.Arms = append(trace.Arms, result.BanditArm)
		}
		if !result.Observed {
			pending, ok := tracker.InFlight(arm.Backend.ID)
			if !ok {
				continue
			}
			if pending == 0 {
				if trace != nil {
					trace.Reason = "cold_no_pending"
				}
				return arm.Backend, true
			}
			if cold.ID == "" || pending < minPending {
				cold, minPending = arm.Backend, pending
			}
			continue
		}
		improves := result.Score > bestScore.Score
		if best.ID != "" && better != nil {
			improves = better(result, bestScore)
		}
		if best.ID == "" || improves {
			best, bestScore = arm.Backend, result
		}
	}
	if best.ID != "" {
		if trace != nil {
			trace.Reason = "maximum_ucb"
		}
		return best, true
	}
	if trace != nil {
		trace.Reason = "cold_least_pending"
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
