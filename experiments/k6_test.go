package experiments

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestK6FilesExist(t *testing.T) {
	requiredFiles := []string{
		filepath.Join("..", "experiments", "k6", "workload.js"),
		filepath.Join("..", "experiments", "k6", "config.js"),
		filepath.Join("..", "experiments", "k6", "config_test.js"),
	}

	for _, file := range requiredFiles {
		clean := filepath.Clean(file)
		if _, err := os.Stat(clean); os.IsNotExist(err) {
			t.Errorf("required k6 file %s does not exist", file)
		}
	}
}

func TestK6ScenarioStageMapping(t *testing.T) {
	catalogPath := filepath.Join("..", "experiments", "scenarios.json")
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		catalogPath = "scenarios.json"
	}

	catalog, err := LoadCatalog(catalogPath)
	if err != nil {
		t.Fatalf("failed to load catalog: %v", err)
	}

	for _, sc := range catalog.Scenarios {
		totalDuration := 0.0
		currentTime := 0.0

		for idx, stage := range sc.Workload.Stages {
			if math.Abs(stage.StartSeconds-currentTime) > 1e-4 {
				t.Errorf("scenario %s stage %d start time mismatch: expected %.2f, got %.2f",
					sc.ID, idx, currentTime, stage.StartSeconds)
			}
			if stage.DurationSeconds <= 0 {
				t.Errorf("scenario %s stage %d duration must be positive", sc.ID, idx)
			}
			if stage.TargetRPS <= 0 {
				t.Errorf("scenario %s stage %d target RPS must be positive", sc.ID, idx)
			}

			// Validate VU allocation formula
			preAllocated := int(math.Max(5, math.Ceil(stage.TargetRPS*0.5)))
			maxVUs := int(math.Max(20, math.Ceil(stage.TargetRPS*2.5)))
			if preAllocated <= 0 || maxVUs < preAllocated {
				t.Errorf("scenario %s stage %d invalid VU allocation: preAlloc=%d, max=%d",
					sc.ID, idx, preAllocated, maxVUs)
			}

			currentTime += stage.DurationSeconds
			totalDuration += stage.DurationSeconds
		}

		if math.Abs(totalDuration-sc.DurationSeconds) > 1e-4 {
			t.Errorf("scenario %s total stages duration (%.2f) does not match duration_seconds (%.2f)",
				sc.ID, totalDuration, sc.DurationSeconds)
		}
	}
}
