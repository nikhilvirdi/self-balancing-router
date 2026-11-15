package backend

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/nikhilvirdi/self-balancing-router/internal/telemetry"
)

var (
	ErrOverloaded = errors.New("backend overloaded")
	ErrShutdown   = errors.New("backend shutting down")
)

type processorFunc func(ctx context.Context, req requestInfo) (response, error)

type requestInfo struct {
	BackendID string
	Method    string
	Path      string
}

func defaultProcessor(ctx context.Context, req requestInfo) (response, error) {
	return response(req), ctx.Err()
}

type workTask struct {
	ctx      context.Context
	req      requestInfo
	done     chan workResult
	elem     *list.Element
	canceled bool
	trace    *workTrace
}

type workResult struct {
	resp response
	err  error
}

type worker struct {
	taskChan chan *workTask
}

type Pool struct {
	mu            sync.Mutex
	workers       int
	queueCapacity int
	active        int
	queue         *list.List
	idleWorkers   []*worker
	accepting     bool
	closed        bool
	processor     processorFunc
	wg            sync.WaitGroup
	events        *telemetry.Events
	full, stopped uint64
}

func NewPool(workers, queueCapacity int, proc processorFunc) *Pool {
	if proc == nil {
		proc = defaultProcessor
	}
	p := &Pool{
		workers:       workers,
		queueCapacity: queueCapacity,
		queue:         list.New(),
		idleWorkers:   make([]*worker, 0, workers),
		accepting:     true,
		processor:     proc,
	}
	p.start()
	return p
}

func (p *Pool) start() {
	p.wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		w := &worker{taskChan: make(chan *workTask, 1)}
		p.idleWorkers = append(p.idleWorkers, w)
		go p.runWorker(w)
	}
}

func (p *Pool) Workers() int {
	return p.workers
}

func (p *Pool) QueueCapacity() int {
	return p.queueCapacity
}

func (p *Pool) Stats() (active int, queued int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active, p.queue.Len()
}

func (p *Pool) Submit(ctx context.Context, req requestInfo) (response, error) {
	return p.submit(ctx, req, "")
}

func (p *Pool) submit(ctx context.Context, req requestInfo, diagnosticID string) (response, error) {
	if ctx.Err() != nil {
		if diagnosticID != "" {
			p.mu.Lock()
			p.admission(&workTask{req: req, trace: &workTrace{id: diagnosticID}}, "queue_rejected", "canceled")
			p.mu.Unlock()
		}
		return response{}, ctx.Err()
	}

	task := &workTask{
		ctx:  ctx,
		req:  req,
		done: make(chan workResult, 1),
	}
	if diagnosticID != "" {
		task.trace = &workTrace{id: diagnosticID}
	}

	p.mu.Lock()
	if !p.accepting {
		p.stopped++
		p.admission(task, "queue_rejected", "not_accepting")
		p.mu.Unlock()
		return response{}, ErrOverloaded
	}

	// Case 1: An idle worker is available.
	if len(p.idleWorkers) > 0 {
		w := p.idleWorkers[len(p.idleWorkers)-1]
		p.idleWorkers = p.idleWorkers[:len(p.idleWorkers)-1]
		p.active++
		p.admission(task, "queue_admitted", "direct")
		p.mu.Unlock()

		w.taskChan <- task
		return p.waitResult(ctx, task)
	}

	// Case 2: All workers are busy. Can we queue?
	if p.queue.Len() < p.queueCapacity {
		task.elem = p.queue.PushBack(task)
		p.admission(task, "queue_admitted", "queued")
		p.mu.Unlock()
		return p.waitResult(ctx, task)
	}

	// Case 3: Processing and waiting capacity are exhausted.
	p.full++
	p.admission(task, "queue_rejected", "queue_full")
	p.mu.Unlock()
	return response{}, ErrOverloaded
}

func (p *Pool) waitResult(ctx context.Context, task *workTask) (response, error) {
	select {
	case res := <-task.done:
		return res.resp, res.err
	case <-ctx.Done():
		p.cancelQueued(task)
		return response{}, ctx.Err()
	}
}

func (p *Pool) cancelQueued(task *workTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if task.elem != nil {
		p.queue.Remove(task.elem)
		task.elem = nil
		task.canceled = true
		p.queueRemoval(task, "canceled")
	}
}

func (p *Pool) executeTask(task *workTask) {
	task.recordStart()
	if task.ctx.Err() != nil {
		task.recordResult(task.ctx.Err())
		task.done <- workResult{err: task.ctx.Err()}
		return
	}
	resp, err := p.processor(task.ctx, task.req)
	task.recordResult(err)
	task.done <- workResult{resp: resp, err: err}
}

func (p *Pool) runWorker(w *worker) {
	defer p.wg.Done()
	for task := range w.taskChan {
		for task != nil {
			p.executeTask(task)

			p.mu.Lock()
			completed := task
			if p.queue.Len() > 0 && !p.closed {
				elem := p.queue.Front()
				p.queue.Remove(elem)
				task = elem.Value.(*workTask)
				task.elem = nil
				p.completed(completed)
				p.mu.Unlock()
			} else {
				task = nil
				p.active--
				p.completed(completed)
				if p.closed {
					p.mu.Unlock()
					return
				}
				p.idleWorkers = append(p.idleWorkers, w)
				p.mu.Unlock()
			}
		}
	}
}

func (p *Pool) StopAccepting() {
	p.mu.Lock()
	p.accepting = false
	p.mu.Unlock()
}

func (p *Pool) CancelQueue(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for elem := p.queue.Front(); elem != nil; elem = p.queue.Front() {
		task := elem.Value.(*workTask)
		p.queue.Remove(elem)
		task.elem = nil
		task.canceled = true
		p.queueRemoval(task, "shutdown")
		select {
		case task.done <- workResult{err: err}:
		default:
		}
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.wg.Wait()
		return
	}
	p.accepting = false
	p.closed = true
	for elem := p.queue.Front(); elem != nil; elem = p.queue.Front() {
		task := elem.Value.(*workTask)
		p.queue.Remove(elem)
		task.elem = nil
		task.canceled = true
		p.queueRemoval(task, "shutdown")
		select {
		case task.done <- workResult{err: ErrShutdown}:
		default:
		}
	}
	for _, w := range p.idleWorkers {
		close(w.taskChan)
	}
	p.idleWorkers = nil
	p.mu.Unlock()

	p.wg.Wait()
}
