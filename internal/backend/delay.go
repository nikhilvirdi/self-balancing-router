package backend

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type DelayModel struct {
	baselineFixed  time.Duration
	baselineJitter time.Duration
	mu             sync.Mutex
	fixed          time.Duration
	jitter         time.Duration
	rng            *rand.Rand
}

type WorkloadConfig struct {
	ProcessingDelay          time.Duration
	ProcessingJitter         time.Duration
	BaselineProcessingDelay  time.Duration
	BaselineProcessingJitter time.Duration
}

func NewDelayModel(fixed, jitter time.Duration, seed int64) *DelayModel {
	return &DelayModel{
		baselineFixed:  fixed,
		baselineJitter: jitter,
		fixed:          fixed,
		jitter:         jitter,
		rng:            rand.New(rand.NewSource(seed)),
	}
}

func (m *DelayModel) Sample() time.Duration {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	fixed := m.fixed
	jitter := m.jitter
	var jitterSample time.Duration
	if jitter > 0 {
		jitterSample = time.Duration(m.rng.Int63n(int64(jitter) + 1))
	}
	m.mu.Unlock()
	return fixed + jitterSample
}

func (m *DelayModel) Workload() WorkloadConfig {
	if m == nil {
		return WorkloadConfig{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return WorkloadConfig{
		ProcessingDelay:          m.fixed,
		ProcessingJitter:         m.jitter,
		BaselineProcessingDelay:  m.baselineFixed,
		BaselineProcessingJitter: m.baselineJitter,
	}
}

func (m *DelayModel) Update(delay, jitter time.Duration) error {
	if m == nil {
		return nil
	}
	if err := validateWorkload(delay, jitter); err != nil {
		return err
	}
	m.mu.Lock()
	m.fixed = delay
	m.jitter = jitter
	m.mu.Unlock()
	return nil
}

func (m *DelayModel) Reset() WorkloadConfig {
	if m == nil {
		return WorkloadConfig{}
	}
	m.mu.Lock()
	m.fixed = m.baselineFixed
	m.jitter = m.baselineJitter
	cfg := WorkloadConfig{
		ProcessingDelay:          m.fixed,
		ProcessingJitter:         m.jitter,
		BaselineProcessingDelay:  m.baselineFixed,
		BaselineProcessingJitter: m.baselineJitter,
	}
	m.mu.Unlock()
	return cfg
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
