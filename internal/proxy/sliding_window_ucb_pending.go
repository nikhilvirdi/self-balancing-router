package proxy

import (
	"math"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type pendingSlidingWindowUCB struct {
	*slidingWindowUCB
	penalty float64
}

func (a *pendingSlidingWindowUCB) Select(arms []banditArm[windowedArmState]) (Backend, bool) {
	return a.selectDiagnostic(arms, nil)
}

func (a *pendingSlidingWindowUCB) selectDiagnostic(arms []banditArm[windowedArmState], trace *telemetry.BanditDecision) (Backend, bool) {
	if a.penalty == 0 && trace == nil {
		return a.slidingWindowUCB.Select(arms)
	}
	if trace != nil {
		trace.Eligible = make([]string, len(arms))
		for i, arm := range arms {
			trace.Eligible[i] = arm.Backend.ID
		}
	}
	return selectUCB(arms, a.tracker, trace, func(arm banditArm[windowedArmState]) ucbArmScore {
		result := a.score(arm.State, trace != nil)
		count, _ := a.tracker.InFlight(arm.Backend.ID)
		result.pending = count
		if trace != nil {
			penalty := a.penalty * float64(count)
			saturated := math.IsInf(penalty, 0)
			// Preserve finite diagnostic values when a finite coefficient overflows.
			penalty = min(penalty, math.MaxFloat64)
			result.Pending = &telemetry.BanditPending{
				Count: count, Penalty: penalty, AdjustedScore: result.Score - penalty, Saturated: saturated,
			}
		}
		return result
	}, func(next, best ucbArmScore) bool {
		// Compare differences to retain the ordering when absolute scores
		// overflow or round away small UCB differences at equal pending counts.
		return next.Score-best.Score > a.penalty*float64(next.pending-best.pending)
	})
}
