package proxy

import (
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/nikhilvirdi/self-balancing-router/internal/config"
)

func TestRegistryEmpty(t *testing.T) {
	registry := newTestRegistry(t)
	if len(registry.Snapshot()) != 0 || len(registry.Eligible()) != 0 {
		t.Fatal("empty registry returned backends")
	}
	if _, ok := registry.Lookup("missing"); ok {
		t.Fatal("lookup found an unknown backend")
	}
	if err := registry.SetHealth("missing", Healthy); err == nil {
		t.Fatal("health update accepted an unknown backend")
	}
}

func TestRegistryInvalidDefinitions(t *testing.T) {
	first := config.BackendConfig{ID: "backend-a", URL: "http://first:8080"}
	tests := []struct {
		name        string
		definitions []config.BackendConfig
		wantErr     string
	}{
		{"duplicate", []config.BackendConfig{first, {ID: first.ID, URL: "http://second:8080"}}, "duplicate backend identifier"},
		{"invalid identifier", []config.BackendConfig{{ID: "1a", URL: first.URL}}, "backend identifier"},
		{"invalid URL", []config.BackendConfig{{ID: first.ID, URL: "https://first:8080"}}, "absolute HTTP backend URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry, err := NewRegistry(tt.definitions)
			if registry != nil || err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("NewRegistry() = %v, %v, want error containing %q", registry, err, tt.wantErr)
			}
		})
	}
}

func TestRegistryHealthAndEligibility(t *testing.T) {
	registry := newTestRegistry(t, "backend-c", "backend-a", "backend-b")
	c := Backend{ID: "backend-c", URL: "http://backend-c:8080", Health: Unknown}
	a := Backend{ID: "backend-a", URL: "http://backend-a:8080", Health: Unknown}
	b := Backend{ID: "backend-b", URL: "http://backend-b:8080", Health: Unknown}
	assertBackends(t, registry.Snapshot(), c, a, b)
	assertBackends(t, registry.Eligible(), c, a, b)

	setHealth(t, registry, a.ID, Unhealthy)
	a.Health = Unhealthy
	assertBackends(t, registry.Snapshot(), c, a, b)
	assertBackends(t, registry.Eligible(), c, b)

	setHealth(t, registry, a.ID, Healthy)
	a.Health = Healthy
	assertBackends(t, registry.Eligible(), c, a, b)
	if got, ok := registry.Lookup(a.ID); !ok || got != a {
		t.Fatalf("Lookup(%q) = %+v, %t, want %+v", a.ID, got, ok, a)
	}
	if err := registry.SetHealth(a.ID, Health(3)); err == nil {
		t.Fatal("health update accepted an invalid state")
	}
	assertBackends(t, registry.Snapshot(), c, a, b)

	for _, backend := range registry.Snapshot() {
		setHealth(t, registry, backend.ID, Unhealthy)
	}
	assertBackends(t, registry.Eligible())
	setHealth(t, registry, a.ID, Unknown)
	a.Health = Unknown
	assertBackends(t, registry.Eligible(), a)
}

func TestRegistrySnapshotsAreIndependent(t *testing.T) {
	definitions := []config.BackendConfig{{ID: "backend-a", URL: "http://backend-a:8080"}}
	registry, err := NewRegistry(definitions)
	if err != nil {
		t.Fatal(err)
	}
	want := Backend{ID: definitions[0].ID, URL: definitions[0].URL, Health: Unknown}
	all := registry.Snapshot()
	eligible := registry.Eligible()
	found, ok := registry.Lookup(want.ID)
	if !ok || found != want {
		t.Fatalf("Lookup(%q) = %+v, %t, want %+v", want.ID, found, ok, want)
	}
	setHealth(t, registry, want.ID, Healthy)
	assertBackends(t, all, want)
	assertBackends(t, eligible, want)
	if found != want {
		t.Fatalf("lookup snapshot changed to %+v", found)
	}

	definitions[0] = config.BackendConfig{}
	all[0] = Backend{}
	eligible[0] = Backend{}
	want.Health = Healthy
	assertBackends(t, registry.Snapshot(), want)
	assertBackends(t, registry.Eligible(), want)
}

func TestRegistryConcurrentAccess(t *testing.T) {
	const id = "backend-a"
	registry := newTestRegistry(t, id)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, health := range []Health{Healthy, Unhealthy} {
		workers.Go(func() {
			<-start
			for range 200 {
				if err := registry.SetHealth(id, health); err != nil {
					t.Error(err)
				}
			}
		})
	}
	for range 4 {
		workers.Go(func() {
			<-start
			for range 200 {
				if got, ok := registry.Lookup(id); !ok || got.ID != id || got.Health > Unhealthy {
					t.Errorf("invalid concurrent lookup: %+v, %t", got, ok)
				}
				if got := registry.Snapshot(); len(got) != 1 || got[0].ID != id || got[0].Health > Unhealthy {
					t.Errorf("invalid concurrent snapshot: %+v", got)
				}
				if got := registry.Eligible(); len(got) > 1 || (len(got) == 1 && (got[0].ID != id || got[0].Health == Unhealthy)) {
					t.Errorf("invalid concurrent eligibility snapshot: %+v", got)
				}
			}
		})
	}
	close(start)
	workers.Wait()
}

func newTestRegistry(t *testing.T, ids ...string) *Registry {
	t.Helper()
	var definitions []config.BackendConfig
	for _, id := range ids {
		definitions = append(definitions, config.BackendConfig{ID: id, URL: "http://" + id + ":8080"})
	}
	registry, err := NewRegistry(definitions)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func assertBackends(t *testing.T, got []Backend, want ...Backend) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("backends = %+v, want %+v", got, want)
	}
}

func setHealth(t *testing.T, registry *Registry, id string, health Health) {
	t.Helper()
	if err := registry.SetHealth(id, health); err != nil {
		t.Fatal(err)
	}
}
