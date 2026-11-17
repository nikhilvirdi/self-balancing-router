package proxy

import (
	"errors"
	"sync"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type banditFeedback struct {
	BackendID  string
	FinishedAt time.Time
	Reward     float64
}

type banditArm[S any] struct {
	Backend Backend
	State   S
}

// Callbacks run under the policy lock and must be bounded. Select borrows a
// read-only view of eligible arms. Update may receive completions out of order
// and may update state for any configured arm, including ineligible arms.
type banditAlgorithm[S any] interface {
	Select([]banditArm[S]) (Backend, bool)
	Update(map[string]S, banditFeedback)
}

type banditPolicy[S any] struct {
	mu        sync.Mutex
	arms      map[string]S
	algorithm banditAlgorithm[S]
	scale     time.Duration
}

// Each configured arm starts with zero-value algorithm state. The registry
// controls eligibility independently of the lifetime of that state.
func newBanditPolicy[S any](backends []Backend, scale time.Duration, algorithm banditAlgorithm[S]) (*banditPolicy[S], error) {
	if scale <= 0 {
		return nil, errors.New("bandit latency scale must be positive")
	}
	arms := make(map[string]S, len(backends))
	for _, backend := range backends {
		var state S
		arms[backend.ID] = state
	}
	return &banditPolicy[S]{arms: arms, algorithm: algorithm, scale: scale}, nil
}

func (p *banditPolicy[S]) Select(candidates []Backend) (Backend, bool) {
	return p.selectDiagnostic(candidates, nil)
}

func (p *banditPolicy[S]) selectDiagnostic(candidates []Backend, trace *telemetry.BanditDecision) (Backend, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	eligible := make([]banditArm[S], 0, len(candidates))
	for _, backend := range candidates {
		if state, ok := p.arms[backend.ID]; ok && backend.Health != Unhealthy {
			eligible = append(eligible, banditArm[S]{Backend: backend, State: state})
		}
	}
	if len(eligible) == 0 {
		return Backend{}, false
	}
	if trace != nil {
		if algorithm, ok := p.algorithm.(interface {
			selectDiagnostic([]banditArm[S], *telemetry.BanditDecision) (Backend, bool)
		}); ok {
			return algorithm.selectDiagnostic(eligible, trace)
		}
	}
	return p.algorithm.Select(eligible)
}

func (p *banditPolicy[S]) observe(o Observation) {
	p.applyFeedback(o)
}

func (p *banditPolicy[S]) applyFeedback(o Observation) (float64, bool) {
	reward, ok := banditReward(o, p.scale)
	if !ok {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.arms[o.BackendID]; ok {
		p.algorithm.Update(p.arms, banditFeedback{
			BackendID: o.BackendID, FinishedAt: o.FinishedAt, Reward: reward,
		})
		return reward, true
	}
	return 0, false
}
