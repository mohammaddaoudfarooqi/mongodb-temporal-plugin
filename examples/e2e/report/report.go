// Package report holds the machine-readable result files the e2e runner
// writes and the qualification report renders.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Results is one runner invocation, i.e. one qualification phase.
type Results struct {
	Phase     string     `json:"phase"`
	Scenarios []Scenario `json:"scenarios,omitempty"`
	Load      *Load      `json:"load,omitempty"`
	Ladder    *Ladder    `json:"ladder,omitempty"`
	// SetupError is set when the runner could not start (namespace,
	// dial, search attributes).
	SetupError string `json:"setup_error,omitempty"`
	// FaultError is set when the fault this phase injects (an Atlas
	// failover, a node stop) did not happen, so its load result proves
	// nothing about surviving the fault.
	FaultError string `json:"fault_error,omitempty"`
	// Fault describes the fault injected during this phase and how the
	// cluster recovered, e.g. "cluster healthy after 312s".
	Fault string `json:"fault,omitempty"`
	// StartedAt and FinishedAt (Unix seconds) bound the phase, so host
	// samples can be attributed to it.
	StartedAt  int64 `json:"started_at,omitempty"`
	FinishedAt int64 `json:"finished_at,omitempty"`
}

// Scenario is one scenario's outcome.
type Scenario struct {
	Name       string `json:"name"`
	Pass       bool   `json:"pass"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// Load summarises a load run.
type Load struct {
	Workflows     int              `json:"workflows"`
	Concurrency   int              `json:"concurrency"`
	Completed     int64            `json:"completed"`
	Failed        int64            `json:"failed"`
	ErrorRate     float64          `json:"error_rate"`
	ErrorsByClass map[string]int64 `json:"errors_by_class,omitempty"`
	Elapsed       time.Duration    `json:"elapsed_ns"`
	PerSecond     float64          `json:"per_second"`
	P50           time.Duration    `json:"p50_ns"`
	P95           time.Duration    `json:"p95_ns"`
	P99           time.Duration    `json:"p99_ns"`
	Max           time.Duration    `json:"max_ns"`
}

// Ladder is a concurrency ladder. MaxSustainable is the
// highest concurrency that met the thresholds (0 if none did);
// StoppedAt is the step that breached them (0 if none did).
type Ladder struct {
	Steps          []Load        `json:"steps"`
	MaxSustainable int           `json:"max_sustainable"`
	StoppedAt      int           `json:"stopped_at,omitempty"`
	MaxP99         time.Duration `json:"max_p99_ns"`
	MaxErrorRate   float64       `json:"max_error_rate"`
}

// Pass reports whether every check in the phase passed. A ladder passes
// when at least one step was sustainable; the step that breached the
// thresholds is the measurement, not a failure.
func (r Results) Pass() bool {
	if r.SetupError != "" || r.FaultError != "" {
		return false
	}
	for _, s := range r.Scenarios {
		if !s.Pass {
			return false
		}
	}
	if r.Load != nil && r.Load.Failed > 0 {
		return false
	}
	if r.Ladder != nil && r.Ladder.MaxSustainable == 0 {
		return false
	}
	return true
}

// Write stores r as indented JSON at path.
func Write(path string, r Results) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("report: %w", err)
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// Read loads a results file.
func Read(path string) (Results, error) {
	var r Results
	b, err := os.ReadFile(path)
	if err != nil {
		return r, fmt.Errorf("report: %w", err)
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("report: %s: %w", path, err)
	}
	return r, nil
}
