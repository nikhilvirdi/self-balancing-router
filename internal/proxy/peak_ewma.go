package proxy

import (
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

type latencyEstimate struct {
	nanos      float64
	observedAt time.Time
	assignedAt time.Time
}

func (e *latencyEstimate) observe(at time.Time, duration, decay time.Duration) {
	// Concurrent callbacks can arrive after a later completion has already been applied.
	if duration <= 0 || at.IsZero() || at.Before(e.observedAt) {
		return
	}
	sample := float64(duration)
	if e.observedAt.IsZero() || sample > e.nanos {
		e.nanos = sample
	} else {
		e.nanos = decayLatency(e.nanos, sample, at.Sub(e.observedAt), decay)
	}
	e.observedAt = at
}

func (e latencyEstimate) score(now time.Time, inFlight int, initialRTT, decay time.Duration) float64 {
	estimate := e.nanos
	if e.observedAt.IsZero() {
		estimate = float64(initialRTT)
	}
	lastActivity := e.observedAt
	if e.assignedAt.After(lastActivity) {
		lastActivity = e.assignedAt
	}
	// After one idle time constant, age toward 1 ns so quiet backends can compete again.
	if idle := now.Sub(lastActivity); inFlight == 0 && idle > decay {
		estimate = decayLatency(estimate, float64(time.Nanosecond), idle-decay, decay)
	}
	return estimate * (float64(inFlight) + 1)
}

func decayLatency(estimate, target float64, elapsed, decay time.Duration) float64 {
	alpha := math.Exp(-float64(elapsed) / float64(decay))
	return alpha*estimate + (1-alpha)*target
}

type peakEWMAPolicy struct {
	*p2cSampler
	tracker    *requestTracker
	initialRTT time.Duration
	decay      time.Duration
	mu         sync.Mutex
	latency    map[string]*latencyEstimate
}

func newPeakEWMAPolicy(cfg config.ProxyConfig, tracker *requestTracker, now time.Time) *peakEWMAPolicy {
	backends := tracker.Snapshot()
	latency := make(map[string]*latencyEstimate, len(backends))
	for _, backend := range backends {
		latency[backend.BackendID] = &latencyEstimate{assignedAt: now}
	}
	return &peakEWMAPolicy{
		p2cSampler: newP2CSampler(cfg.P2CSeed), tracker: tracker,
		initialRTT: cfg.PeakEWMAInitialRTT, decay: cfg.PeakEWMADecay, latency: latency,
	}
}

func (p *peakEWMAPolicy) Select(candidates []Backend) (Backend, bool) {
	return p.selectAt(candidates, time.Now())
}

func (p *peakEWMAPolicy) selectAt(candidates []Backend, now time.Time) (Backend, bool) {
	if len(candidates) == 0 {
		return Backend{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	chosen := candidates[0]
	if len(candidates) > 1 {
		i, j := p.sample(len(candidates))
		first, second := candidates[i], candidates[j]
		firstLoad, firstOK := p.tracker.InFlight(first.ID)
		secondLoad, secondOK := p.tracker.InFlight(second.ID)
		if !firstOK || !secondOK {
			return Backend{}, false
		}
		chosen = first
		if p.latency[second.ID].score(now, secondLoad, p.initialRTT, p.decay) <
			p.latency[first.ID].score(now, firstLoad, p.initialRTT, p.decay) {
			chosen = second
		}
	}
	if estimate := p.latency[chosen.ID]; estimate != nil && now.After(estimate.assignedAt) {
		estimate.assignedAt = now
	}
	return chosen, true
}

func (p *peakEWMAPolicy) observe(o Observation) {
	// Forwarding time can include downstream transfer. Health probes filter availability.
	// Error-rate outlier detection requires a separate mechanism.
	if o.Outcome != UpstreamResponse || o.UpstreamStatus < http.StatusOK || o.UpstreamStatus >= http.StatusMultipleChoices {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if estimate := p.latency[o.BackendID]; estimate != nil {
		estimate.observe(o.FinishedAt, o.Duration, p.decay)
	}
}
