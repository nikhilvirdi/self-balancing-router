package backend

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type DelayModel struct {
	fixed  time.Duration
	jitter time.Duration
	mu     sync.Mutex
	rng    *rand.Rand
}

func NewDelayModel(fixed, jitter time.Duration, seed int64) *DelayModel {
	return &DelayModel{
		fixed:  fixed,
		jitter: jitter,
		rng:    rand.New(rand.NewSource(seed)),
	}
}

func (m *DelayModel) Sample() time.Duration {
	if m == nil {
		return 0
	}
	if m.jitter <= 0 {
		return m.fixed
	}
	m.mu.Lock()
	jitterSample := time.Duration(m.rng.Int63n(int64(m.jitter) + 1))
	m.mu.Unlock()
	return m.fixed + jitterSample
}

func waitDelay(ctx context.Context, d time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func makeProcessor(model *DelayModel) processorFunc {
	return func(ctx context.Context, req requestInfo) (response, error) {
		if ctx.Err() != nil {
			return response{}, ctx.Err()
		}
		if model != nil {
			delay := model.Sample()
			if err := waitDelay(ctx, delay); err != nil {
				return response{}, err
			}
		}
		return response(req), nil
	}
}

