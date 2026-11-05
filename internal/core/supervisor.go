package core

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	restartInitial = time.Second
	restartMax     = 60 * time.Second
	reprobe        = 60 * time.Second
)

type supervisedModule struct {
	module Module

	mu            sync.Mutex
	status        string
	reason        string
	lastError     string
	lastAttemptAt time.Time
	lastRunStart  time.Time
}

// Supervisor starts modules, isolates failures and restarts crashed modules
// with exponential backoff.
type Supervisor struct {
	deps    Deps
	log     *slog.Logger
	modules []*supervisedModule
	wg      sync.WaitGroup
}

// NewSupervisor builds a supervisor for the given modules.
func NewSupervisor(modules []Module, deps Deps, log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	sv := &Supervisor{deps: deps, log: log}
	for _, m := range modules {
		sv.modules = append(sv.modules, &supervisedModule{
			module: m,
			status: ModuleHealthy,
		})
	}
	return sv
}

// Start launches every module in its own goroutine.
func (s *Supervisor) Start(ctx context.Context) {
	for _, sm := range s.modules {
		s.wg.Add(1)
		go func(sm *supervisedModule) {
			defer s.wg.Done()
			s.run(ctx, sm)
		}(sm)
	}
}

// Wait blocks until all module goroutines have stopped.
func (s *Supervisor) Wait() { s.wg.Wait() }

func (s *Supervisor) run(ctx context.Context, sm *supervisedModule) {
	backoff := restartInitial
	for {
		if ctx.Err() != nil {
			s.setState(sm, ModuleDisabled, "shutdown", "")
			return
		}
		err := callInit(ctx, sm.module, s.deps)
		if err != nil {
			if ctx.Err() != nil {
				s.setState(sm, ModuleDisabled, "shutdown", "")
				return
			}
			if IsUnavailable(err) {
				reason := err.Error()
				s.setState(sm, ModuleUnavailable, reason, reason)
				s.log.Info("module unavailable, will re-probe", "module", sm.module.Name(), "reason", reason)
				if !sleep(ctx, reprobe) {
					s.setState(sm, ModuleDisabled, "shutdown", "")
					return
				}
				continue
			}
			s.setState(sm, ModuleFailed, "init_failed", err.Error())
			s.log.Error("module init failed", "module", sm.module.Name(), "error", err)
			if !sleep(ctx, backoff) {
				s.setState(sm, ModuleDisabled, "shutdown", "")
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		s.setState(sm, ModuleHealthy, "", "")
		sm.mu.Lock()
		sm.lastRunStart = time.Now()
		sm.mu.Unlock()

		err = callRun(ctx, sm.module)
		if ctx.Err() != nil {
			s.setState(sm, ModuleDisabled, "shutdown", "")
			return
		}
		if err == nil {
			err = errors.New("module stopped unexpectedly")
		}
		s.setState(sm, ModuleFailed, "run_failed", err.Error())
		s.log.Error("module stopped, restarting", "module", sm.module.Name(), "error", err, "backoff", backoff)

		sm.mu.Lock()
		ranFor := time.Since(sm.lastRunStart)
		sm.mu.Unlock()
		if ranFor > restartMax {
			backoff = restartInitial
		}
		if !sleep(ctx, backoff) {
			s.setState(sm, ModuleDisabled, "shutdown", "")
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func callInit(ctx context.Context, m Module, d Deps) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return m.Init(ctx, d)
}

func callRun(ctx context.Context, m Module) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return m.Run(ctx)
}

func (s *Supervisor) setState(sm *supervisedModule, status, reason, lastError string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.status = status
	sm.reason = reason
	if lastError != "" {
		sm.lastError = lastError
	}
	sm.lastAttemptAt = time.Now()
}

// Health merges supervisor state with each module's self-reported health.
func (s *Supervisor) Health() map[string]ModuleHealth {
	out := make(map[string]ModuleHealth, len(s.modules))
	for _, sm := range s.modules {
		sm.mu.Lock()
		status := sm.status
		reason := sm.reason
		lastErr := sm.lastError
		sm.mu.Unlock()

		h := sm.module.Health()
		if status == ModuleFailed || status == ModuleUnavailable || status == ModuleDisabled {
			h.Status = status
			if reason != "" {
				h.Reason = reason
			}
			if lastErr != "" {
				h.LastError = lastErr
			}
		} else if h.Status == "" {
			h.Status = ModuleHealthy
		}
		out[sm.module.Name()] = h
	}
	return out
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > restartMax {
		return restartMax
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
