package backend

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type eventWriter chan telemetry.Event

func (w eventWriter) Write(data []byte) (int, error) {
	var record struct{ Event telemetry.Event }
	if err := json.Unmarshal(data, &record); err != nil {
		return 0, err
	}
	if record.Event.Kind != "" {
		w <- record.Event
	}
	return len(data), nil
}

func TestPoolTelemetry(t *testing.T) {
	records := make(eventWriter, 8)
	events := telemetry.NewEvents(slog.New(slog.NewJSONHandler(records, nil)), 1, "test")
	t.Cleanup(events.Close)
	ctx, cancel := context.WithCancel(t.Context())
	pool := NewPool(1, 1, func(ctx context.Context, _ requestInfo) (response, error) {
		<-ctx.Done()
		return response{}, ctx.Err()
	})
	pool.events = events
	t.Cleanup(func() { cancel(); pool.Close() })
	info := requestInfo{BackendID: "test"}
	assertEvent := func(kind, outcome string, seq uint64, active, queued int) {
		t.Helper()
		select {
		case got := <-records:
			if got.Kind != kind || got.Outcome != outcome || got.RequestID != events.RequestID(seq) || got.Active != active || got.Queued != queued || got.QueueWaitNS < 0 || got.DurationNS < 0 {
				t.Fatalf("event = %+v, want %s/%s for request %d at (%d, %d)", got, kind, outcome, seq, active, queued)
			}
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for %s/%s", kind, outcome)
		}
	}
	go func() { _, _ = pool.submit(ctx, info, events.RequestID(1)) }()
	assertEvent("queue_admitted", "direct", 1, 1, 0)
	go func() { _, _ = pool.submit(ctx, info, events.RequestID(2)) }()
	assertEvent("queue_admitted", "queued", 2, 1, 1)
	if _, err := pool.submit(ctx, info, events.RequestID(3)); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("full queue: %v", err)
	}
	assertEvent("queue_rejected", "queue_full", 3, 1, 1)
	pool.StopAccepting()
	if _, err := pool.submit(ctx, info, events.RequestID(4)); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("stopped pool: %v", err)
	}
	assertEvent("queue_rejected", "not_accepting", 4, 1, 1)
	var scrape strings.Builder
	pool.renderMetrics(&scrape)
	for _, want := range []string{"sbr_backend_workers_active 1", "sbr_backend_queue_occupancy 1", `sbr_backend_rejections_total{reason="queue_full"} 1`, `sbr_backend_rejections_total{reason="not_accepting"} 1`} {
		if !strings.Contains(scrape.String(), want+"\n") {
			t.Errorf("missing metric %s", want)
		}
	}
	pool.CancelQueue(ErrShutdown)
	assertEvent("work_completed", "shutdown", 2, 1, 0)
	cancel()
	assertEvent("work_completed", "canceled", 1, 0, 0)
}
