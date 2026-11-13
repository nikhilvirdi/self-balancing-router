package backend

import (
	"container/list"
	"context"
	"errors"
	"sync"
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
	return response{
		BackendID: req.BackendID,
		Method:    req.Method,
		Path:      req.Path,
	}, ctx.Err()
}

type workTask struct {
	ctx      context.Context
	req      requestInfo
	done     chan workResult
	elem     *list.Element
	canceled bool
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
	if ctx.Err() != nil {
		return response{}, ctx.Err()
	}

	task := &workTask{
		ctx:  ctx,
		req:  req,
		done: make(chan workResult, 1),
	}

	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return response{}, ErrOverloaded
	}

	// Case 1: An idle worker is available.
	if len(p.idleWorkers) > 0 {
		w := p.idleWorkers[len(p.idleWorkers)-1]
		p.idleWorkers = p.idleWorkers[:len(p.idleWorkers)-1]
		p.active++
		p.mu.Unlock()

		w.taskChan <- task
		return p.waitResult(ctx, task)
	}

	// Case 2: All workers are busy. Can we queue?
	if p.queue.Len() < p.queueCapacity {
		task.elem = p.queue.PushBack(task)
		p.mu.Unlock()
		return p.waitResult(ctx, task)
	}

	// Case 3: Processing and waiting capacity are exhausted.
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
	}
}

func (p *Pool) executeTask(task *workTask) {
	if task.ctx.Err() != nil {
		task.done <- workResult{err: task.ctx.Err()}
		return
	}
	resp, err := p.processor(task.ctx, task.req)
	task.done <- workResult{resp: resp, err: err}
}

func (p *Pool) runWorker(w *worker) {
	defer p.wg.Done()
	for task := range w.taskChan {
		for task != nil {
			p.executeTask(task)

			p.mu.Lock()
			if p.queue.Len() > 0 && !p.closed {
				elem := p.queue.Front()
				p.queue.Remove(elem)
				task = elem.Value.(*workTask)
				task.elem = nil
				p.mu.Unlock()
			} else {
				task = nil
				p.active--
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
	for elem := p.queue.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(*workTask)
		task.elem = nil
		task.canceled = true
		select {
		case task.done <- workResult{err: err}:
		default:
		}
	}
	p.queue.Init()
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
	for elem := p.queue.Front(); elem != nil; elem = elem.Next() {
		task := elem.Value.(*workTask)
		task.elem = nil
		task.canceled = true
		select {
		case task.done <- workResult{err: ErrShutdown}:
		default:
		}
	}
	p.queue.Init()
	for _, w := range p.idleWorkers {
		close(w.taskChan)
	}
	p.idleWorkers = nil
	p.mu.Unlock()

	p.wg.Wait()
}

