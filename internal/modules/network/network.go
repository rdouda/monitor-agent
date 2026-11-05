package network

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"sync"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
)

// Module is the network module.
type Module struct {
	mu      sync.Mutex
	deps    core.Deps
	health  core.ModuleHealth
	backend backend

	interfaceMu       sync.Mutex
	routeMu           sync.Mutex
	socketMu          sync.Mutex
	interfaceStates   map[string]string
	interfaceBaseline bool
	socketCache       map[uint64]socketOwner

	taskErrs map[string]taskError

	lifecycleMu     sync.Mutex
	lifecycleWindow time.Time
	lifecycleCount  int
}

type taskError struct {
	reason string
	detail string
}

// New creates the network module.
func New() *Module {
	return &Module{interfaceStates: map[string]string{}, socketCache: map[uint64]socketOwner{}}
}

// Name implements core.Module.
func (m *Module) Name() string { return "network" }

// Init implements core.Module.
func (m *Module) Init(_ context.Context, d core.Deps) error {
	if d.Config == nil {
		return fmt.Errorf("network: config is required")
	}
	if d.Clock == nil {
		d.Clock = core.SystemClock{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deps = d
	m.backend = newBackend(d.Paths.ProcfsRoot)
	m.health = core.ModuleHealth{Status: core.ModuleHealthy}
	m.interfaceStates = map[string]string{}
	m.interfaceBaseline = false
	m.socketCache = map[uint64]socketOwner{}
	m.taskErrs = map[string]taskError{}
	if _, err := m.backend.Links(); err != nil {
		m.health = core.ModuleHealth{Status: core.ModuleUnavailable, Reason: "netlink_unavailable", LastError: err.Error()}
		return core.ErrUnavailable{Reason: "netlink_unavailable"}
	}
	return nil
}

// Run implements core.Module.
func (m *Module) Run(ctx context.Context) error {
	d := m.getDeps()
	n := d.Config.Modules.Network
	s := d.Scheduler
	s.Add(core.Task{Name: "network.interfaces", Interval: n.InventoryInterval, Timeout: 2 * n.InventoryInterval, Run: m.interfaceTask})
	s.Add(core.Task{Name: "network.routes", Interval: n.InventoryInterval, Timeout: 2 * n.InventoryInterval, Run: m.routeTask})
	if n.CollectListeningPorts {
		s.Add(core.Task{Name: "network.sockets", Interval: n.SocketsInterval, Timeout: 2 * n.SocketsInterval, Run: m.socketsTask})
	}
	s.Add(core.Task{Name: "network.metrics", Interval: n.MetricsInterval, Timeout: 30 * time.Second, Run: m.metricsTask})

	if m.backend != nil {
		updates, err := m.backend.Subscribe(ctx)
		if err != nil {
			m.log().Debug("netlink subscription unavailable", "error", err)
		} else {
			go m.eventLoop(ctx, updates)
		}
	}
	<-ctx.Done()
	return nil
}

// RunOnce implements core.OneShot.
func (m *Module) RunOnce(ctx context.Context) error {
	tasks := []func(context.Context) error{m.interfaceTask, m.routeTask}
	if m.getDeps().Config.Modules.Network.CollectListeningPorts {
		tasks = append(tasks, m.socketsTask)
	}
	tasks = append(tasks, m.metricsTask)
	var first error
	for _, task := range tasks {
		if err := task(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Resync implements core.Resyncer.
func (m *Module) Resync(ctx context.Context) error {
	tasks := []func(context.Context) error{m.interfaceTask, m.routeTask}
	if m.getDeps().Config.Modules.Network.CollectListeningPorts {
		tasks = append(tasks, m.socketsTask)
	}
	var first error
	for _, task := range tasks {
		if err := task(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Health implements core.Module.
func (m *Module) Health() core.ModuleHealth {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.health
}

func (m *Module) getDeps() core.Deps {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deps
}

func (m *Module) log() *slog.Logger {
	d := m.getDeps()
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

// setTaskOK clears the recorded error for one periodic task and refreshes
// module health.
func (m *Module) setTaskOK(task string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.taskErrs, task)
	m.health.LastSuccessAt = m.deps.Clock.Now()
	m.refreshHealthLocked()
}

// setTaskErr records a partial failure for one periodic task.
func (m *Module) setTaskErr(task, reason string, err error) {
	detail := ""
	if err != nil {
		detail = truncate(err.Error(), 512)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskErrs[task] = taskError{reason: reason, detail: detail}
	m.refreshHealthLocked()
}

func (m *Module) refreshHealthLocked() {
	if len(m.taskErrs) == 0 {
		m.health.Status = core.ModuleHealthy
		m.health.Reason = ""
		m.health.LastError = ""
		return
	}
	names := make([]string, 0, len(m.taskErrs))
	for name := range m.taskErrs {
		names = append(names, name)
	}
	sort.Strings(names)
	first := m.taskErrs[names[0]]
	m.health.Status = core.ModuleDegraded
	m.health.Reason = first.reason
	m.health.LastError = first.detail
}

func (m *Module) eventLoop(ctx context.Context, updates <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-updates:
			if !ok {
				return
			}
			// Debounce bursts of netlink updates.
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			_ = m.interfaceTask(ctx)
			_ = m.routeTask(ctx)
		}
	}
}

func (m *Module) emitLifecycle(d core.Deps, l events.Lifecycle) {
	now := d.Clock.Now()
	m.lifecycleMu.Lock()
	if m.lifecycleWindow.IsZero() || now.Sub(m.lifecycleWindow) >= time.Minute {
		m.lifecycleWindow = now
		m.lifecycleCount = 0
	}
	limit := d.Config.Limits.MaxLifecycleEventsPerMinute
	if limit > 0 && m.lifecycleCount >= limit {
		m.lifecycleMu.Unlock()
		if d.RateLimited != nil {
			d.RateLimited(1)
		}
		return
	}
	m.lifecycleCount++
	m.lifecycleMu.Unlock()
	d.Emitter.Lifecycle(l)
}

func (m *Module) hostEntityID() string {
	d := m.getDeps()
	return ids.HostID(d.Identity.HostID)
}

func (m *Module) excludeInterfaceMetrics(name string) bool {
	d := m.getDeps()
	for _, glob := range d.Config.Modules.Network.InterfaceMetricsExclude {
		if ok, _ := path.Match(glob, name); ok {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
