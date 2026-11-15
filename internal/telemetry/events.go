package telemetry

import (
	"context"
	"crypto/rand"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RequestHeader = "X-Sbr-Diagnostic-Id"
	SchemaVersion = 1
	queueSize     = 1024
)

type Event struct {
	Kind        string `json:"kind"`
	RequestID   string `json:"request_id"`
	BackendID   string `json:"backend_id,omitempty"`
	Status      int    `json:"status"`
	Outcome     string `json:"outcome,omitempty"`
	DurationNS  int64  `json:"duration_ns"`
	QueueWaitNS int64  `json:"queue_wait_ns"`
	Active      int    `json:"active_workers"`
	Queued      int    `json:"queued"`
	InFlight    any    `json:"in_flight,omitempty"`
}

type Events struct {
	handler slog.Handler
	every   uint64
	prefix  string
	mu      sync.RWMutex
	closed  bool
	queue   chan slog.Record
	done    chan struct{}
	written atomic.Uint64
	dropped atomic.Uint64
}

func NewEvents(logger *slog.Logger, every uint64, service string) *Events {
	if every == 0 {
		return nil
	}
	e := &Events{
		handler: logger.With("diagnostic_schema", SchemaVersion, "diagnostic_service", service).Handler(),
		every:   every, prefix: rand.Text(), queue: make(chan slog.Record, queueSize), done: make(chan struct{}),
	}
	go e.run()
	return e
}

func (e *Events) RequestID(sequence uint64) string {
	if sequence%e.every != 0 {
		return ""
	}
	return e.prefix + "-" + strconv.FormatUint(sequence, 10)
}

func ValidRequestID(id string) bool {
	prefix, seq, ok := strings.Cut(id, "-")
	if !ok || len(prefix) != 26 || len(seq) == 0 || len(seq) > 20 || seq[0] == '0' {
		return false
	}
	for _, c := range prefix {
		if (c < 'A' || c > 'Z') && (c < '2' || c > '7') {
			return false
		}
	}
	for _, c := range seq {
		if c < '0' || c > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(seq, 10, 64)
	return err == nil
}

func (e *Events) Emit(at time.Time, event Event) {
	if event.RequestID == "" {
		return
	}
	record := slog.NewRecord(at.UTC(), slog.LevelInfo, "sbr diagnostic event", 0)
	record.AddAttrs(slog.Any("event", event))
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.closed {
		select {
		case e.queue <- record:
			return
		default:
		}
	}
	e.dropped.Add(1)
}

func (e *Events) run() {
	defer close(e.done)
	ctx := context.Background()
	for record := range e.queue {
		if err := e.handler.Handle(ctx, record); err != nil {
			e.dropped.Add(1)
		} else {
			e.written.Add(1)
		}
	}
	record := slog.NewRecord(time.Now().UTC(), slog.LevelInfo, "sbr diagnostic summary", 0)
	record.AddAttrs(slog.Uint64("written", e.written.Load()), slog.Uint64("dropped", e.dropped.Load()), slog.Uint64("sample_every", e.every))
	// Export validates the presence of this record against the saved event count.
	_ = e.handler.Handle(ctx, record)
}

func (e *Events) Dropped() uint64 {
	return e.dropped.Load()
}

func (e *Events) Close() {
	if e == nil {
		return
	}
	e.mu.Lock()
	if !e.closed {
		e.closed = true
		close(e.queue)
	}
	e.mu.Unlock()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-e.done:
	case <-timer.C:
	}
}
