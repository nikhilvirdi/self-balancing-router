package proxy

import (
	"fmt"
	"sync/atomic"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

type routingPolicy interface {
	// Select receives a read-only snapshot in configuration order.
	Select(candidates []Backend) (Backend, bool)
}

func newRoutingPolicy(name config.ProxyPolicy, tracker *requestTracker) (routingPolicy, error) {
	switch name {
	case config.PolicyFirstEligible:
		return firstEligiblePolicy{}, nil
	case config.PolicyRoundRobin:
		return &roundRobinPolicy{}, nil
	case config.PolicyLeastRequests:
		return leastRequestsPolicy{tracker: tracker}, nil
	default:
		return nil, fmt.Errorf("unsupported routing policy %q", name)
	}
}

type firstEligiblePolicy struct{}

func (firstEligiblePolicy) Select(candidates []Backend) (Backend, bool) {
	if len(candidates) == 0 {
		return Backend{}, false
	}
	return candidates[0], true
}

type roundRobinPolicy struct {
	cursor atomic.Uint64
}

func (p *roundRobinPolicy) Select(candidates []Backend) (Backend, bool) {
	if len(candidates) == 0 {
		return Backend{}, false
	}
	// Unsigned wraparound keeps the modulo index valid across overflow.
	index := p.cursor.Add(1) - 1
	return candidates[index%uint64(len(candidates))], true
}

type leastRequestsPolicy struct {
	tracker *requestTracker
}

func (p leastRequestsPolicy) Select(candidates []Backend) (Backend, bool) {
	if len(candidates) == 0 {
		return Backend{}, false
	}
	best := candidates[0]
	minimum, ok := p.tracker.InFlight(best.ID)
	if !ok {
		return Backend{}, false
	}
	for _, candidate := range candidates[1:] {
		count, ok := p.tracker.InFlight(candidate.ID)
		if !ok {
			return Backend{}, false
		}
		if count < minimum {
			best, minimum = candidate, count
		}
	}
	return best, true
}
