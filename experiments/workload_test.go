package experiments

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogAndScenariosLoadingAndValidation(t *testing.T) {
	catalogPath := filepath.Join("..", "experiments", "scenarios.json")
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		catalogPath = "scenarios.json"
	}

	catalog, err := LoadCatalog(catalogPath)
	if err != nil {
		t.Fatalf("failed to load catalog from %s: %v", catalogPath, err)
	}

	if len(catalog.Scenarios) != 7 {
		t.Fatalf("expected 7 scenarios in catalog, got %d", len(catalog.Scenarios))
	}

	expectedIDs := []string{
		"scenario_1_homogeneous",
		"scenario_2_heterogeneous",
		"scenario_3_sudden_degradation",
		"scenario_4_gradual_degradation",
		"scenario_5_backend_recovery",
		"scenario_6_traffic_bursts",
		"scenario_7_repeated_performance_changes",
	}

	for i, expectedID := range expectedIDs {
		if catalog.Scenarios[i].ID != expectedID {
			t.Errorf("scenario %d ID mismatch: expected %s, got %s", i, expectedID, catalog.Scenarios[i].ID)
		}
	}

	// Validate individual files
	scenariosDir := filepath.Join("..", "experiments", "scenarios")
	if _, err := os.Stat(scenariosDir); os.IsNotExist(err) {
		scenariosDir = "scenarios"
	}

	entries, err := os.ReadDir(scenariosDir)
	if err != nil {
		t.Fatalf("failed to read scenarios directory %s: %v", scenariosDir, err)
	}

	jsonCount := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			jsonCount++
			fpath := filepath.Join(scenariosDir, entry.Name())
			sc, err := LoadScenario(fpath)
			if err != nil {
				t.Errorf("failed to load scenario file %s: %v", entry.Name(), err)
				continue
			}
			if err := sc.Validate(); err != nil {
				t.Errorf("scenario %s validation failed: %v", entry.Name(), err)
			}
		}
	}

	if jsonCount != 7 {
		t.Errorf("expected 7 scenario JSON files in directory, found %d", jsonCount)
	}
}

func TestTheoreticalCapacities(t *testing.T) {
	scenariosDir := filepath.Join("..", "experiments", "scenarios")
	if _, err := os.Stat(scenariosDir); os.IsNotExist(err) {
		scenariosDir = "scenarios"
	}

	// 1. Homogeneous scenario
	homoSc, err := LoadScenario(filepath.Join(scenariosDir, "01_stable_homogeneous.json"))
	if err != nil {
		t.Fatalf("failed to load homogeneous scenario: %v", err)
	}

	homoCap := homoSc.TheoreticalClusterCapacity()
	if math.Abs(homoCap-500.0) > 1e-2 {
		t.Errorf("expected homogeneous cluster capacity 500 rps, got %.2f", homoCap)
	}

	homoUniform := homoSc.TheoreticalUniformCapacity()
	if math.Abs(homoUniform-500.0) > 1e-2 {
		t.Errorf("expected homogeneous uniform capacity 500 rps, got %.2f", homoUniform)
	}

	// 2. Heterogeneous scenario
	heteroSc, err := LoadScenario(filepath.Join(scenariosDir, "02_stable_heterogeneous.json"))
	if err != nil {
		t.Fatalf("failed to load heterogeneous scenario: %v", err)
	}

	capA, _ := heteroSc.TheoreticalBackendCapacity("backend-a") // 4 / 0.02 = 200
	capB, _ := heteroSc.TheoreticalBackendCapacity("backend-b") // 4 / 0.04 = 100
	capC, _ := heteroSc.TheoreticalBackendCapacity("backend-c") // 4 / 0.08 = 50
	capD, _ := heteroSc.TheoreticalBackendCapacity("backend-d") // 4 / 0.12 = 33.33
	capE, _ := heteroSc.TheoreticalBackendCapacity("backend-e") // 4 / 0.18 = 22.22

	if math.Abs(capA-200.0) > 1e-2 || math.Abs(capB-100.0) > 1e-2 || math.Abs(capC-50.0) > 1e-2 ||
		math.Abs(capD-(4.0/0.12)) > 1e-2 || math.Abs(capE-(4.0/0.18)) > 1e-2 {
		t.Errorf("heterogeneous backend capacity calculation error: A=%.1f, B=%.1f, C=%.1f, D=%.1f, E=%.1f",
			capA, capB, capC, capD, capE)
	}

	heteroUniform := heteroSc.TheoreticalUniformCapacity()
	expectedUniform := 5.0 * (4.0 / 0.18) // ~111.11 rps
	if math.Abs(heteroUniform-expectedUniform) > 1e-2 {
		t.Errorf("expected heterogeneous uniform capacity %.2f, got %.2f", expectedUniform, heteroUniform)
	}

	// 3. Traffic bursts scenario
	burstSc, err := LoadScenario(filepath.Join(scenariosDir, "06_traffic_bursts.json"))
	if err != nil {
		t.Fatalf("failed to load burst scenario: %v", err)
	}

	// Max stage RPS should exceed uniform capacity to guarantee overload testing
	maxStageRPS := 0.0
	for _, stage := range burstSc.Workload.Stages {
		if stage.TargetRPS > maxStageRPS {
			maxStageRPS = stage.TargetRPS
		}
	}
	if maxStageRPS <= expectedUniform {
		t.Errorf("burst scenario max RPS (%.1f) should exceed uniform capacity (%.1f)", maxStageRPS, expectedUniform)
	}
}

func TestScenarioValidationErrors(t *testing.T) {
	baseScenario := func() Scenario {
		return Scenario{
			ID:              "valid_scenario",
			Name:            "Valid Scenario",
			Description:     "Valid description",
			DurationSeconds: 60.0,
			Timeline: Timeline{
				WarmupSeconds:      10.0,
				MeasurementSeconds: 45.0,
				CooldownSeconds:    5.0,
			},
			Workload: Workload{
				Pattern: "constant",
				Stages: []WorkloadStage{
					{StartSeconds: 0.0, DurationSeconds: 60.0, TargetRPS: 50.0},
				},
			},
			Backends: map[string]BackendConfig{
				"backend-a": {Workers: 4, QueueCapacity: 32, ProcessingDelay: "40ms", ProcessingJitter: "10ms", Seed: 101},
				"backend-b": {Workers: 4, QueueCapacity: 32, ProcessingDelay: "40ms", ProcessingJitter: "10ms", Seed: 102},
				"backend-c": {Workers: 4, QueueCapacity: 32, ProcessingDelay: "40ms", ProcessingJitter: "10ms", Seed: 103},
				"backend-d": {Workers: 4, QueueCapacity: 32, ProcessingDelay: "40ms", ProcessingJitter: "10ms", Seed: 104},
				"backend-e": {Workers: 4, QueueCapacity: 32, ProcessingDelay: "40ms", ProcessingJitter: "10ms", Seed: 105},
			},
			Events: []WorkloadEvent{},
			Evaluation: Evaluation{
				PrimaryHypothesis: "Test hypothesis",
				TargetMetrics:     []string{"latency"},
				PolicyExpectations: PolicyExpectations{
					RoundRobin:       "exp",
					LeastRequests:    "exp",
					P2C:              "exp",
					P2CPeakEWMA:      "exp",
					UCB1:             "exp",
					SlidingWindowUCB: "exp",
					DiscountedUCB:    "exp",
				},
			},
		}
	}

	tests := []struct {
		name        string
		modify      func(s *Scenario)
		errContains string
	}{
		{
			name: "invalid scenario ID",
			modify: func(s *Scenario) {
				s.ID = "invalid-id-with-dashes-and-CAPS!"
			},
			errContains: "invalid scenario ID",
		},
		{
			name: "timeline sum mismatch",
			modify: func(s *Scenario) {
				s.Timeline.MeasurementSeconds = 30.0 // 10 + 30 + 5 = 45 != 60
			},
			errContains: "timeline sum",
		},
		{
			name: "stages timing mismatch",
			modify: func(s *Scenario) {
				s.Workload.Stages = []WorkloadStage{
					{StartSeconds: 0.0, DurationSeconds: 30.0, TargetRPS: 50.0},
					{StartSeconds: 40.0, DurationSeconds: 20.0, TargetRPS: 50.0}, // gap between 30 and 40
				}
			},
			errContains: "start_seconds",
		},
		{
			name: "missing backend",
			modify: func(s *Scenario) {
				delete(s.Backends, "backend-e")
			},
			errContains: "missing required backend",
		},
		{
			name: "jitter exceeds delay",
			modify: func(s *Scenario) {
				b := s.Backends["backend-a"]
				b.ProcessingDelay = "10ms"
				b.ProcessingJitter = "20ms"
				s.Backends["backend-a"] = b
			},
			errContains: "exceeds processing_delay",
		},
		{
			name: "event timestamp out of range",
			modify: func(s *Scenario) {
				s.Events = []WorkloadEvent{
					{TimestampSeconds: 95.0, Target: "backend-a", Action: "reset"},
				}
			},
			errContains: "outside duration range",
		},
		{
			name: "invalid event action",
			modify: func(s *Scenario) {
				s.Events = []WorkloadEvent{
					{TimestampSeconds: 20.0, Target: "backend-a", Action: "invalid_action"},
				}
			},
			errContains: "unknown action",
		},
		{
			name: "missing policy expectation",
			modify: func(s *Scenario) {
				s.Evaluation.PolicyExpectations.DiscountedUCB = ""
			},
			errContains: "must define expectations for all required policies",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := baseScenario()
			tc.modify(&sc)
			err := sc.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("expected error to contain %q, got %q", tc.errContains, err.Error())
			}
		})
	}
}
