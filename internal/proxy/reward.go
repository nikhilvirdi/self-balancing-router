package proxy

import (
	"net/http"
	"time"
)

// banditReward reports whether a terminal observation is attributable to the
// backend. Forwarding duration can include downstream response transfer.
func banditReward(o Observation, scale time.Duration) (float64, bool) {
	if scale <= 0 || o.BackendID == "" || o.StartedAt.IsZero() || o.FinishedAt.IsZero() ||
		o.Duration < 0 || o.Duration != o.FinishedAt.Sub(o.StartedAt) {
		return 0, false
	}
	switch o.Outcome {
	case UpstreamResponse:
		switch {
		case o.UpstreamStatus >= http.StatusOK && o.UpstreamStatus < http.StatusMultipleChoices:
			return 1 / (1 + float64(o.Duration)/float64(scale)), true
		case o.UpstreamStatus >= http.StatusInternalServerError && o.UpstreamStatus < 600:
			return 0, true
		}
	case TransportFailure, UpstreamTimeout:
		return 0, o.UpstreamStatus == 0
	}
	// Redirects and client errors lack a backend attribution rule. Canceled and
	// interrupted requests lack a usable terminal result.
	return 0, false
}
