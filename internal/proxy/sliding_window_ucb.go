package proxy

import "math"

type windowedArmState struct {
	ucb1State
	correction float64
}

func (s *windowedArmState) addReward(reward float64) {
	// Neumaier summation preserves small rewards when larger ones are evicted.
	sum := s.rewardSum + reward
	if math.Abs(s.rewardSum) >= math.Abs(reward) {
		s.correction += (s.rewardSum - sum) + reward
	} else {
		s.correction += (reward - sum) + s.rewardSum
	}
	s.rewardSum = sum
}

type windowReward struct {
	backendID string
	reward    float64
}

// The bandit policy lock protects the ring and rolling arm state. Samples are
// ordered by feedback application, so a delayed completion enters at the tail.
type slidingWindowUCB struct {
	tracker *requestTracker
	size    int
	window  []windowReward
	next    int
}

func (a *slidingWindowUCB) Select(arms []banditArm[windowedArmState]) (Backend, bool) {
	return selectUCB(arms, a.tracker, uint64(len(a.window)), func(state windowedArmState) ucb1State {
		return ucb1State{
			rewardSum:    min(float64(state.observations), max(0, state.rewardSum+state.correction)),
			observations: state.observations,
		}
	})
}

func (a *slidingWindowUCB) Update(arms map[string]windowedArmState, feedback banditFeedback) {
	sample := windowReward{backendID: feedback.BackendID, reward: feedback.Reward}
	if len(a.window) < a.size {
		a.window = append(a.window, sample)
	} else {
		expired := a.window[a.next]
		state := arms[expired.backendID]
		state.observations--
		if state.observations == 0 {
			state = windowedArmState{}
		} else {
			state.addReward(-expired.reward)
		}
		arms[expired.backendID] = state
		a.window[a.next] = sample
		a.next = (a.next + 1) % a.size
	}
	state := arms[sample.backendID]
	state.observations++
	state.addReward(sample.reward)
	arms[sample.backendID] = state
}
