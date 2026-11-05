package core

import (
	"context"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"
)

type scheduler struct {
	log *slog.Logger

	mu      sync.Mutex
	started bool
	ctx     context.Context
	pending []Task
	wg      sync.WaitGroup
}

// NewScheduler creates a scheduler. Tasks added before Run start immediately
// when Run is called; tasks added later start immediately when added.
func NewScheduler(log *slog.Logger) Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &scheduler{log: log}
}

func (s *scheduler) Add(t Task) {
	if t.Interval < time.Second {
		t.Interval = time.Second
	}
	if t.Timeout <= 0 || t.Timeout > t.Interval {
		t.Timeout = t.Interval
		if t.Timeout < 30*time.Second {
			t.Timeout = 30 * time.Second
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		s.pending = append(s.pending, t)
		return
	}
	s.startLocked(s.ctx, t)
}

func (s *scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.ctx = ctx
	pending := s.pending
	s.pending = nil
	for _, t := range pending {
		s.startLocked(ctx, t)
	}
	s.mu.Unlock()

	<-ctx.Done()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.log.Warn("scheduler tasks did not stop within 5s")
	}
	return nil
}

func (s *scheduler) startLocked(ctx context.Context, t Task) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop(ctx, t)
	}()
}

func (s *scheduler) loop(ctx context.Context, t Task) {
	run := func() {
		runCtx, cancel := context.WithTimeout(ctx, t.Timeout)
		defer cancel()
		start := time.Now()
		err := safeRun(runCtx, t)
		if err != nil && ctx.Err() == nil {
			s.log.Warn("scheduled task failed", "task", t.Name, "error", err, "duration", time.Since(start))
		}
	}
	run()
	for {
		timer := time.NewTimer(jitter(t.Interval))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			run()
		}
	}
}

func safeRun(ctx context.Context, t Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return t.Run(ctx)
}

// jitter returns interval with ±10% variance.
func jitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}
	factor := 0.9 + rand.Float64()*0.2
	return time.Duration(float64(interval) * factor)
}

func panicError(r any) error {
	return &PanicError{Value: r, Stack: debug.Stack()}
}

// PanicError is returned when a module or task panics.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return "panic: " + toString(e.Value)
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case error:
		return x.Error()
	default:
		return "non-string panic value"
	}
}
