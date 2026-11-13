package proxy

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

type routingPolicy interface {
	// Select receives a read-only snapshot in configuration order.
	Select(candidates []Backend) (Backend, bool)
}

func newRoutingPolicy(name config.ProxyPolicy, tracker *requestTracker, p2cSeed int64) (routingPolicy, error) {
	switch name {
	case config.PolicyFirstEligible:
		return firstEligiblePolicy{}, nil
	case config.PolicyRoundRobin:
		return &roundRobinPolicy{}, nil
	case config.PolicyLeastRequests:
		return leastRequestsPolicy{tracker: tracker}, nil
	case config.PolicyP2C:
		return &p2cPolicy{
			tracker: tracker,
			rng:     rand.New(rand.NewPCG(uint64(p2cSeed), 0)),
		}, nil
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

type p2cPolicy struct {
	tracker *requestTracker
	mu      sync.Mutex
	rng     *rand.Rand
}

func (p *p2cPolicy) Select(candidates []Backend) (Backend, bool) {
	switch len(candidates) {
	case 0:
		return Backend{}, false
	case 1:
		return candidates[0], true
	}
	i, j := p.sample(len(candidates))
	first, second := candidates[i], candidates[j]
	firstCount, firstOK := p.tracker.InFlight(first.ID)
	secondCount, secondOK := p.tracker.InFlight(second.ID)
	if !firstOK || !secondOK {
		return Backend{}, false
	}
	if secondCount < firstCount {
		return second, true
	}
	return first, true
}

func (p *p2cPolicy) sample(n int) (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	i, j := p.rng.IntN(n), p.rng.IntN(n-1)
	// Map the second draw onto every index except the first.
	if j >= i {
		j++
	}
	return i, j
}
