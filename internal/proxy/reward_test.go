package proxy

import (
	"math"
	"net/http"
	"testing"
	"time"
)

func TestBanditReward(t *testing.T) {
	const scale = 100 * time.Millisecond
	for _, tt := range []struct {
		name     string
		outcome  Outcome
		status   int
		duration time.Duration
		want     float64
		learn    bool
	}{
		{"success", UpstreamResponse, http.StatusOK, scale, 0.5, true},
		{"slower success", UpstreamResponse, http.StatusAccepted, 3 * scale, 0.25, true},
		{"zero duration", UpstreamResponse, http.StatusNoContent, 0, 1, true},
		{"maximum duration", UpstreamResponse, http.StatusOK, math.MaxInt64, 1 / (1 + float64(math.MaxInt64)/float64(scale)), true},
		{"overloaded", UpstreamResponse, http.StatusServiceUnavailable, 0, 0, true},
		{"upstream failure", UpstreamResponse, http.StatusBadGateway, 0, 0, true},
		{"timeout", UpstreamTimeout, 0, scale, 0, true},
		{"transport failure", TransportFailure, 0, 0, 0, true},
		{"canceled", Canceled, http.StatusOK, scale, 0, false},
		{"interrupted", Interrupted, http.StatusOK, scale, 0, false},
		{"redirect", UpstreamResponse, http.StatusFound, scale, 0, false},
		{"client error", UpstreamResponse, http.StatusTooManyRequests, scale, 0, false},
		{"missing response status", UpstreamResponse, 0, scale, 0, false},
		{"invalid response status", UpstreamResponse, 600, scale, 0, false},
		{"inconsistent transport failure", TransportFailure, http.StatusOK, scale, 0, false},
		{"negative duration", UpstreamResponse, http.StatusOK, -scale, 0, false},
		{"unknown outcome", Outcome(255), http.StatusOK, scale, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := newTestObservation("a", time.Unix(100, 0), tt.duration, tt.outcome, tt.status)
			if got, ok := banditReward(o, scale); got != tt.want || ok != tt.learn {
				t.Fatalf("reward = %v, %t, want %v, %t", got, ok, tt.want, tt.learn)
			}
		})
	}

	o := newTestObservation("a", time.Unix(100, 0), math.MaxInt64, UpstreamResponse, http.StatusOK)
	if reward, ok := banditReward(o, time.Nanosecond); !ok || reward <= 0 || reward > 1 || math.IsNaN(reward) {
		t.Fatalf("extreme reward = %v, %t, want a positive bounded reward", reward, ok)
	}
	for _, invalid := range []Observation{
		{},
		newTestObservation("", o.FinishedAt, 0, UpstreamResponse, http.StatusOK),
		{BackendID: "a", StartedAt: o.StartedAt, Duration: o.Duration, Outcome: UpstreamResponse, UpstreamStatus: http.StatusOK},
		{BackendID: "a", FinishedAt: o.FinishedAt, Duration: o.Duration, Outcome: UpstreamResponse, UpstreamStatus: http.StatusOK},
		{BackendID: "a", StartedAt: o.StartedAt, FinishedAt: o.FinishedAt, Outcome: UpstreamResponse, UpstreamStatus: http.StatusOK},
	} {
		if _, ok := banditReward(invalid, scale); ok {
			t.Fatalf("invalid observation was accepted: %+v", invalid)
		}
	}
	for _, invalid := range []time.Duration{0, -time.Nanosecond} {
		if _, ok := banditReward(o, invalid); ok {
			t.Fatalf("invalid scale %s was accepted", invalid)
		}
	}
}

func newTestObservation(id string, finished time.Time, duration time.Duration, outcome Outcome, status int) Observation {
	return Observation{
		BackendID: id, StartedAt: finished.Add(-duration), FinishedAt: finished,
		Duration: duration, Outcome: outcome, UpstreamStatus: status,
	}
}
