package proxy

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type proxyMetrics struct {
	status          [1000]atomic.Uint64
	outcomes        [len(outcomeNames)]atomic.Uint64
	routed          map[string]*atomic.Uint64
	duration        telemetry.Histogram
	successDuration telemetry.Histogram
}

var outcomeNames = [...]string{"interrupted", "upstream_response", "transport_failure", "upstream_timeout", "canceled", "unavailable", "internal_error"}

const (
	unavailable   = Canceled + 1
	internalError = unavailable + 1
)

func newProxyMetrics(backends []Backend) *proxyMetrics {
	m := &proxyMetrics{routed: make(map[string]*atomic.Uint64, len(backends))}
	for _, b := range backends {
		m.routed[b.ID] = new(atomic.Uint64)
	}
	return m
}

func diagnosticStatus(o Observation) int {
	switch o.Outcome {
	case TransportFailure:
		return http.StatusBadGateway
	case UpstreamTimeout:
		return http.StatusGatewayTimeout
	case unavailable:
		return http.StatusServiceUnavailable
	case internalError:
		return http.StatusInternalServerError
	default:
		return o.UpstreamStatus
	}
}

func (m *proxyMetrics) complete(o Observation, status int) {
	if status < 0 || status >= len(m.status) {
		status = 0
	}
	m.status[status].Add(1)
	m.outcomes[o.Outcome].Add(1)
	m.duration.Observe(o.Duration)
	if o.Outcome == UpstreamResponse && status >= 200 && status < 300 {
		m.successDuration.Observe(o.Duration)
	}
}

func (m *proxyMetrics) render(w io.Writer, registry *Registry, tracker *requestTracker, events *telemetry.Events) {
	_, _ = io.WriteString(w, "# TYPE sbr_proxy_requests_total counter\n")
	for code := range m.status {
		if n := m.status[code].Load(); n != 0 {
			_, _ = fmt.Fprintf(w, "sbr_proxy_requests_total{status=\"%d\"} %d\n", code, n)
		}
	}
	_, _ = io.WriteString(w, "# TYPE sbr_proxy_request_outcomes_total counter\n")
	for i, name := range outcomeNames {
		_, _ = fmt.Fprintf(w, "sbr_proxy_request_outcomes_total{outcome=%q} %d\n", name, m.outcomes[i].Load())
	}
	backends := registry.Snapshot()
	_, _ = io.WriteString(w, "# TYPE sbr_proxy_routed_requests_total counter\n")
	for _, backend := range backends {
		_, _ = fmt.Fprintf(w, "sbr_proxy_routed_requests_total{backend=%q} %d\n", backend.ID, m.routed[backend.ID].Load())
	}
	_, _ = io.WriteString(w, "# TYPE sbr_proxy_backend_in_flight gauge\n")
	for _, load := range tracker.Snapshot() {
		_, _ = fmt.Fprintf(w, "sbr_proxy_backend_in_flight{backend=%q} %d\n", load.BackendID, load.Count)
	}
	_, _ = io.WriteString(w, "# TYPE sbr_proxy_backend_health gauge\n")
	for _, backend := range backends {
		for _, state := range []Health{Unknown, Healthy, Unhealthy} {
			value := 0
			if backend.Health == state {
				value = 1
			}
			_, _ = fmt.Fprintf(w, "sbr_proxy_backend_health{backend=%q,state=%q} %d\n", backend.ID, state.String(), value)
		}
	}
	m.duration.Write(w, "sbr_proxy_request_duration_seconds")
	m.successDuration.Write(w, "sbr_proxy_successful_request_duration_seconds")
	telemetry.WriteDrops(w, events)
}
