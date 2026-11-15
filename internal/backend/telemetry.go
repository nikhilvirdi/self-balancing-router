package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

type workTrace struct {
	id                                string
	admittedAt, startedAt, finishedAt time.Time
	result                            string
}

// Pool events take occupancy snapshots under the existing queue lock.
func (p *Pool) emit(task *workTask, at time.Time, kind, outcome string, wait, duration time.Duration) {
	p.events.Emit(at, telemetry.Event{Kind: kind, RequestID: task.trace.id, BackendID: task.req.BackendID,
		Outcome: outcome, Active: p.active, Queued: p.queue.Len(), QueueWaitNS: wait.Nanoseconds(), DurationNS: duration.Nanoseconds()})
}

func (p *Pool) admission(task *workTask, kind, outcome string) {
	if task.trace != nil {
		task.trace.admittedAt = time.Now()
		p.emit(task, task.trace.admittedAt, kind, outcome, 0, 0)
	}
}

func (task *workTask) recordStart() {
	if task.trace != nil {
		task.trace.startedAt = time.Now()
	}
}

func (task *workTask) recordResult(err error) {
	trace := task.trace
	if trace == nil {
		return
	}
	trace.finishedAt = time.Now()
	trace.result = "processed"
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		trace.result = "canceled"
	} else if err != nil {
		trace.result = "processing_error"
	}
}

func (p *Pool) completed(task *workTask) {
	if trace := task.trace; trace != nil {
		p.emit(task, trace.finishedAt, "work_completed", trace.result,
			trace.startedAt.Sub(trace.admittedAt), trace.finishedAt.Sub(trace.startedAt))
	}
}

func (p *Pool) queueRemoval(task *workTask, outcome string) {
	if task.trace != nil {
		now := time.Now()
		p.emit(task, now, "work_completed", outcome, now.Sub(task.trace.admittedAt), 0)
	}
}

func (p *Pool) renderMetrics(w io.Writer) {
	p.mu.Lock()
	active, queued, full, stopped := p.active, p.queue.Len(), p.full, p.stopped
	p.mu.Unlock()
	_, _ = fmt.Fprintf(w, `# TYPE sbr_backend_workers_active gauge
sbr_backend_workers_active %d
# TYPE sbr_backend_workers_capacity gauge
sbr_backend_workers_capacity %d
# TYPE sbr_backend_queue_occupancy gauge
sbr_backend_queue_occupancy %d
# TYPE sbr_backend_queue_capacity gauge
sbr_backend_queue_capacity %d
# TYPE sbr_backend_rejections_total counter
sbr_backend_rejections_total{reason="queue_full"} %d
sbr_backend_rejections_total{reason="not_accepting"} %d
`, active, p.workers, queued, p.queueCapacity, full, stopped)
	telemetry.WriteDrops(w, p.events)
}
