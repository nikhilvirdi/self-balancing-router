package proxy

import "math"

// Below this effective count, the confidence bonus is dominated by a nearly
// empty history. Use the shared pending-aware exploration rule instead.
const minDiscountedObservations = 1e-12

type discountedArmState struct {
	rewardSum    float64
	observations float64
	step         uint64
}

func (s discountedArmState) at(step uint64, gamma float64) discountedArmState {
	if elapsed := step - s.step; elapsed != 0 && s.observations != 0 {
		factor := math.Pow(gamma, float64(elapsed))
		s.rewardSum *= factor
		s.observations *= factor
	}
	s.step = step
	return s
}

func (s discountedArmState) selectionScore(total float64) (float64, bool) {
	if s.observations <= minDiscountedObservations {
		return 0, false
	}
	return ucbScore(s.rewardSum, s.observations, max(1, total)), true
}

// The bandit policy lock protects the clock, total, and arm states. The clock
// follows valid feedback application, including delayed completions, so each
// newly applied reward enters with weight one.
type discountedUCB struct {
	tracker *requestTracker
	gamma   float64
	step    uint64
	total   float64
}

func (a *discountedUCB) Select(arms []banditArm[discountedArmState]) (Backend, bool) {
	return selectUCB(arms, a.tracker, func(state discountedArmState) (float64, bool) {
		return state.at(a.step, a.gamma).selectionScore(a.total)
	})
}

func (a *discountedUCB) Update(arms map[string]discountedArmState, feedback banditFeedback) {
	a.step++
	// The total includes ineligible arms and ages at the same rate as each arm.
	a.total = a.gamma*a.total + 1
	state := arms[feedback.BackendID].at(a.step, a.gamma)
	state.observations++
	state.rewardSum += feedback.Reward
	arms[feedback.BackendID] = state
}
