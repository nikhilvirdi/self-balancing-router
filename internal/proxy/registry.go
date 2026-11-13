package proxy

import (
	"fmt"
	"slices"
	"sync"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

type Health uint8

const (
	Unknown Health = iota
	Healthy
	Unhealthy
)

// Backend is a value snapshot of a configured backend and its current health.
type Backend struct {
	ID     string
	URL    string
	Health Health
}

type Registry struct {
	mu       sync.RWMutex
	backends []Backend
	byID     map[string]int
}

func NewRegistry(definitions []config.BackendConfig) (*Registry, error) {
	registry := &Registry{
		backends: make([]Backend, 0, len(definitions)),
		byID:     make(map[string]int, len(definitions)),
	}
	for i, definition := range definitions {
		if err := definition.Validate(); err != nil {
			return nil, fmt.Errorf("backend %d: %w", i+1, err)
		}
		if _, exists := registry.byID[definition.ID]; exists {
			return nil, fmt.Errorf("duplicate backend identifier %q", definition.ID)
		}
		registry.byID[definition.ID] = i
		registry.backends = append(registry.backends, Backend{ID: definition.ID, URL: definition.URL, Health: Unknown})
	}
	return registry, nil
}

func (r *Registry) Lookup(id string) (Backend, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	index, ok := r.byID[id]
	if !ok {
		return Backend{}, false
	}
	return r.backends[index], true
}

func (r *Registry) Snapshot() []Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Clone(r.backends)
}

// Eligible includes unassessed backends and backends explicitly marked healthy.
func (r *Registry) Eligible() []Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	eligible := make([]Backend, 0, len(r.backends))
	for _, backend := range r.backends {
		if backend.Health != Unhealthy {
			eligible = append(eligible, backend)
		}
	}
	return eligible
}

func (r *Registry) SetHealth(id string, health Health) error {
	switch health {
	case Unknown, Healthy, Unhealthy:
	default:
		return fmt.Errorf("invalid backend health state %d", health)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	index, ok := r.byID[id]
	if !ok {
		return fmt.Errorf("unknown backend identifier %q", id)
	}
	r.backends[index].Health = health
	return nil
}
