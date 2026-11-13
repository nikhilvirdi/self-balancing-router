package experiments

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Standard backend identifiers expected in the five-backend simulation fleet.
var ExpectedBackends = []string{
	"backend-a",
	"backend-b",
	"backend-c",
	"backend-d",
	"backend-e",
}

// Expected routing policies documented in evaluation criteria.
var ExpectedPolicies = []string{
	"round_robin",
	"least_requests",
	"p2c",
	"p2c_peak_ewma",
	"ucb1",
	"sliding_window_ucb",
	"discounted_ucb",
}

var idPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// BackendConfig specifies the operational capacity and delay parameters of a backend server.
type BackendConfig struct {
	Workers          int    `json:"workers"`
	QueueCapacity    int    `json:"queue_capacity"`
	ProcessingDelay  string `json:"processing_delay"`
	ProcessingJitter string `json:"processing_jitter"`
	Seed             int64  `json:"seed"`
}

// Timeline specifies experimental warmup, active measurement, and cooldown intervals.
type Timeline struct {
	WarmupSeconds      float64 `json:"warmup_seconds"`
	MeasurementSeconds float64 `json:"measurement_seconds"`
	CooldownSeconds    float64 `json:"cooldown_seconds"`
}

// WorkloadStage specifies an arrival-rate stage in an open-loop load schedule.
type WorkloadStage struct {
	StartSeconds    float64 `json:"start_seconds"`
	DurationSeconds float64 `json:"duration_seconds"`
	TargetRPS       float64 `json:"target_rps"`
}

// Workload specifies the arrival pattern and schedule stages.
type Workload struct {
	Pattern string          `json:"pattern"`
	Stages  []WorkloadStage `json:"stages"`
}

// WorkloadEvent specifies a runtime administrative workload reconfiguration.
type WorkloadEvent struct {
	TimestampSeconds float64 `json:"timestamp_seconds"`
	Target           string  `json:"target"`
	Action           string  `json:"action"`
	ProcessingDelay  string  `json:"processing_delay,omitempty"`
	ProcessingJitter string  `json:"processing_jitter,omitempty"`
	Description      string  `json:"description,omitempty"`
}

// PolicyExpectations captures the expected behavioral dynamics of each policy.
type PolicyExpectations struct {
	RoundRobin       string `json:"round_robin"`
	LeastRequests    string `json:"least_requests"`
	P2C              string `json:"p2c"`
	P2CPeakEWMA      string `json:"p2c_peak_ewma"`
	UCB1             string `json:"ucb1"`
	SlidingWindowUCB string `json:"sliding_window_ucb"`
	DiscountedUCB    string `json:"discounted_ucb"`
}

// Evaluation criteria and benchmark expectations for a scenario.
type Evaluation struct {
	PrimaryHypothesis  string             `json:"primary_hypothesis"`
	TargetMetrics      []string           `json:"target_metrics"`
	PolicyExpectations PolicyExpectations `json:"policy_expectations"`
}

// Scenario represents a complete, self-contained experimental benchmark specification.
type Scenario struct {
	ID              string                   `json:"id"`
	Name            string                   `json:"name"`
	Description     string                   `json:"description"`
	DurationSeconds float64                  `json:"duration_seconds"`
	Timeline        Timeline                 `json:"timeline"`
	Workload        Workload                 `json:"workload"`
	Backends        map[string]BackendConfig `json:"backends"`
	Events          []WorkloadEvent          `json:"events"`
	Evaluation      Evaluation               `json:"evaluation"`
}

// BackendFleet specifies fleet-wide metadata in the scenario catalog.
type BackendFleet struct {
	Instances            []string `json:"instances"`
	DefaultWorkers       int      `json:"default_workers"`
	DefaultQueueCapacity int      `json:"default_queue_capacity"`
}

// Catalog represents a collection of benchmark workload scenarios.
type Catalog struct {
	Version            string       `json:"version"`
	CatalogDescription string       `json:"catalog_description"`
	BackendFleet       BackendFleet `json:"backend_fleet"`
	Scenarios          []Scenario   `json:"scenarios"`
}

// LoadScenario reads and decodes a Scenario from a JSON file.
func LoadScenario(path string) (*Scenario, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("reading scenario file %s: %w", path, err)
	}
	return LoadScenarioBytes(data)
}

// LoadScenarioBytes parses JSON bytes into a Scenario and validates it.
func LoadScenarioBytes(data []byte) (*Scenario, error) {
	var s Scenario
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("decoding scenario JSON: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("validating scenario %q: %w", s.ID, err)
	}
	return &s, nil
}

// LoadCatalog reads and decodes a Catalog from a JSON file.
func LoadCatalog(path string) (*Catalog, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("reading catalog file %s: %w", path, err)
	}
	return LoadCatalogBytes(data)
}

// LoadCatalogBytes parses JSON bytes into a Catalog and validates it.
func LoadCatalogBytes(data []byte) (*Catalog, error) {
	var c Catalog
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decoding catalog JSON: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validating catalog: %w", err)
	}
	return &c, nil
}

// Validate verifies the structural and domain integrity of a Scenario.
func (s *Scenario) Validate() error {
	if !idPattern.MatchString(s.ID) {
		return fmt.Errorf("invalid scenario ID %q: must match %s", s.ID, idPattern.String())
	}
	if s.Name == "" {
		return fmt.Errorf("scenario name cannot be empty")
	}
	if s.Description == "" {
		return fmt.Errorf("scenario description cannot be empty")
	}
	if s.DurationSeconds <= 0 {
		return fmt.Errorf("duration_seconds must be positive, got %v", s.DurationSeconds)
	}

	// 1. Timeline checks
	totalTimeline := s.Timeline.WarmupSeconds + s.Timeline.MeasurementSeconds + s.Timeline.CooldownSeconds
	if math.Abs(totalTimeline-s.DurationSeconds) > 1e-4 {
		return fmt.Errorf("timeline sum (warmup=%.2f + measurement=%.2f + cooldown=%.2f = %.2f) does not match duration=%.2f",
			s.Timeline.WarmupSeconds, s.Timeline.MeasurementSeconds, s.Timeline.CooldownSeconds, totalTimeline, s.DurationSeconds)
	}
	if s.Timeline.WarmupSeconds < 0 || s.Timeline.MeasurementSeconds <= 0 || s.Timeline.CooldownSeconds < 0 {
		return fmt.Errorf("invalid timeline components: warmup=%.2f, measurement=%.2f, cooldown=%.2f",
			s.Timeline.WarmupSeconds, s.Timeline.MeasurementSeconds, s.Timeline.CooldownSeconds)
	}

	// 2. Workload stages checks
	if len(s.Workload.Stages) == 0 {
		return fmt.Errorf("workload.stages must contain at least one stage")
	}
	currentTime := 0.0
	for i, stage := range s.Workload.Stages {
		if math.Abs(stage.StartSeconds-currentTime) > 1e-4 {
			return fmt.Errorf("stage %d start_seconds (%.2f) does not match expected offset (%.2f)",
				i, stage.StartSeconds, currentTime)
		}
		if stage.DurationSeconds <= 0 {
			return fmt.Errorf("stage %d duration_seconds must be positive, got %.2f", i, stage.DurationSeconds)
		}
		if stage.TargetRPS <= 0 {
			return fmt.Errorf("stage %d target_rps must be positive, got %.2f", i, stage.TargetRPS)
		}
		currentTime += stage.DurationSeconds
	}
	if math.Abs(currentTime-s.DurationSeconds) > 1e-4 {
		return fmt.Errorf("total stages duration (%.2f) does not match scenario duration (%.2f)",
			currentTime, s.DurationSeconds)
	}

	// 3. Backends checks
	for _, expectedID := range ExpectedBackends {
		cfg, exists := s.Backends[expectedID]
		if !exists {
			return fmt.Errorf("missing required backend %q", expectedID)
		}
		if cfg.Workers < 1 {
			return fmt.Errorf("backend %q workers must be >= 1, got %d", expectedID, cfg.Workers)
		}
		if cfg.QueueCapacity < 1 {
			return fmt.Errorf("backend %q queue_capacity must be >= 1, got %d", expectedID, cfg.QueueCapacity)
		}
		delay, err := time.ParseDuration(cfg.ProcessingDelay)
		if err != nil {
			return fmt.Errorf("backend %q processing_delay %q is invalid duration: %w", expectedID, cfg.ProcessingDelay, err)
		}
		jitter, err := time.ParseDuration(cfg.ProcessingJitter)
		if err != nil {
			return fmt.Errorf("backend %q processing_jitter %q is invalid duration: %w", expectedID, cfg.ProcessingJitter, err)
		}
		if jitter > delay {
			return fmt.Errorf("backend %q processing_jitter (%v) exceeds processing_delay (%v)", expectedID, jitter, delay)
		}
	}
	if len(s.Backends) != len(ExpectedBackends) {
		return fmt.Errorf("backend count mismatch: expected %d, got %d", len(ExpectedBackends), len(s.Backends))
	}

	// 4. Events checks
	for i, ev := range s.Events {
		if ev.TimestampSeconds < 0 || ev.TimestampSeconds > s.DurationSeconds {
			return fmt.Errorf("event %d timestamp %.2f outside duration range [0, %.2f]",
				i, ev.TimestampSeconds, s.DurationSeconds)
		}
		if !isValidBackend(ev.Target) {
			return fmt.Errorf("event %d invalid target backend %q", i, ev.Target)
		}
		switch ev.Action {
		case "update":
			d, err := time.ParseDuration(ev.ProcessingDelay)
			if err != nil {
				return fmt.Errorf("event %d invalid processing_delay %q: %w", i, ev.ProcessingDelay, err)
			}
			j, err := time.ParseDuration(ev.ProcessingJitter)
			if err != nil {
				return fmt.Errorf("event %d invalid processing_jitter %q: %w", i, ev.ProcessingJitter, err)
			}
			if j > d {
				return fmt.Errorf("event %d processing_jitter (%v) exceeds processing_delay (%v)", i, j, d)
			}
		case "reset":
			// reset action does not require delay parameters
		default:
			return fmt.Errorf("event %d unknown action %q (expected 'update' or 'reset')", i, ev.Action)
		}
	}

	// 5. Evaluation checks
	if s.Evaluation.PrimaryHypothesis == "" {
		return fmt.Errorf("evaluation.primary_hypothesis cannot be empty")
	}
	if len(s.Evaluation.TargetMetrics) == 0 {
		return fmt.Errorf("evaluation.target_metrics must contain at least one metric")
	}
	if s.Evaluation.PolicyExpectations.RoundRobin == "" ||
		s.Evaluation.PolicyExpectations.LeastRequests == "" ||
		s.Evaluation.PolicyExpectations.P2C == "" ||
		s.Evaluation.PolicyExpectations.P2CPeakEWMA == "" ||
		s.Evaluation.PolicyExpectations.UCB1 == "" ||
		s.Evaluation.PolicyExpectations.SlidingWindowUCB == "" ||
		s.Evaluation.PolicyExpectations.DiscountedUCB == "" {
		return fmt.Errorf("evaluation.policy_expectations must define expectations for all required policies")
	}

	return nil
}

// Validate verifies the Catalog and all embedded Scenarios.
func (c *Catalog) Validate() error {
	if c.Version == "" {
		return fmt.Errorf("catalog version cannot be empty")
	}
	if len(c.BackendFleet.Instances) != len(ExpectedBackends) {
		return fmt.Errorf("catalog backend_fleet instances mismatch")
	}
	if len(c.Scenarios) == 0 {
		return fmt.Errorf("catalog scenarios cannot be empty")
	}

	seenIDs := make(map[string]bool)
	for i := range c.Scenarios {
		sc := &c.Scenarios[i]
		if seenIDs[sc.ID] {
			return fmt.Errorf("duplicate scenario ID %q in catalog", sc.ID)
		}
		seenIDs[sc.ID] = true
		if err := sc.Validate(); err != nil {
			return fmt.Errorf("scenario %d (%s) validation error: %w", i, sc.ID, err)
		}
	}
	return nil
}

// TheoreticalBackendCapacity calculates nominal service capacity (W / D) in requests per second.
func (s *Scenario) TheoreticalBackendCapacity(backendID string) (float64, error) {
	cfg, ok := s.Backends[backendID]
	if !ok {
		return 0, fmt.Errorf("backend %q not found in scenario", backendID)
	}
	d, err := time.ParseDuration(cfg.ProcessingDelay)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid delay for %q: %v", backendID, err)
	}
	return float64(cfg.Workers) / d.Seconds(), nil
}

// TheoreticalClusterCapacity returns the aggregate service capacity assuming optimal allocation.
func (s *Scenario) TheoreticalClusterCapacity() float64 {
	total := 0.0
	for _, bID := range ExpectedBackends {
		capVal, err := s.TheoreticalBackendCapacity(bID)
		if err == nil {
			total += capVal
		}
	}
	return total
}

// TheoreticalUniformCapacity returns the maximum stable arrival rate under uniform (Round Robin) routing,
// limited by 5 * min_capacity.
func (s *Scenario) TheoreticalUniformCapacity() float64 {
	minCap := math.MaxFloat64
	for _, bID := range ExpectedBackends {
		capVal, err := s.TheoreticalBackendCapacity(bID)
		if err == nil && capVal < minCap {
			minCap = capVal
		}
	}
	if minCap == math.MaxFloat64 {
		return 0
	}
	return minCap * float64(len(ExpectedBackends))
}

func isValidBackend(name string) bool {
	for _, b := range ExpectedBackends {
		if b == name {
			return true
		}
	}
	return false
}
