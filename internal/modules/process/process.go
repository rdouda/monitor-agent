// Package process implements process inventory, lifecycle and resource
// metrics.
package process

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"agent/internal/core"
	"agent/internal/ids"

	"github.com/google/uuid"
)

// Module is the process module.
type Module struct {
	mu     sync.Mutex
	deps   core.Deps
	health core.ModuleHealth

	inventoryMu sync.Mutex

	bootTime      time.Time
	bootID8       string
	logicalCores  int
	includeNames  []string
	excludeNames  []string
	redact        []*regexp.Regexp
	users         map[int]string
	commandLine   bool
	includeKernel bool

	lifecycleMu       sync.Mutex
	lifecycleBaseline bool
	lifecycleKnown    map[procKey]lifecycleProc
	lifecycleTimes    []time.Time

	metricsMu   sync.Mutex
	prevSamples map[procKey]cpuSample
}

// New creates the process module.
func New() *Module { return &Module{} }

// Name implements core.Module.
func (m *Module) Name() string { return "process" }

// Init implements core.Module.
func (m *Module) Init(_ context.Context, d core.Deps) error {
	if d.Config == nil {
		return fmt.Errorf("process: config is required")
	}
	p := d.Config.Modules.Process

	redact := make([]*regexp.Regexp, 0, len(p.RedactPatterns))
	for _, pat := range p.RedactPatterns {
		re, err := regexp.Compile(pat)
		if err != nil {
			return fmt.Errorf("process: invalid redact pattern %q: %w", pat, err)
		}
		redact = append(redact, re)
	}

	bootTime, bootID := readBootInfo(d.Paths.ProcfsRoot)
	bootID8 := ids.BootID8(bootID)
	if bootID8 == "" {
		return core.ErrUnavailable{Reason: "boot_id_unavailable"}
	}

	var users map[int]string
	if p.ResolveUserNames {
		users = readUserNames(d.Paths.HostfsRoot)
	}

	m.mu.Lock()
	m.deps = d
	m.bootTime = bootTime
	m.bootID8 = bootID8
	m.logicalCores = readLogicalCores(d.Paths.ProcfsRoot)
	m.includeNames = append([]string(nil), p.IncludeNames...)
	m.excludeNames = append([]string(nil), p.ExcludeNames...)
	m.redact = redact
	m.users = users
	m.commandLine = p.CollectCommandLine
	m.includeKernel = p.IncludeKernelThreads
	m.health = core.ModuleHealth{Status: core.ModuleHealthy}
	m.mu.Unlock()

	m.lifecycleMu.Lock()
	m.lifecycleBaseline = false
	m.lifecycleKnown = nil
	m.lifecycleTimes = nil
	m.lifecycleMu.Unlock()

	m.metricsMu.Lock()
	m.prevSamples = map[procKey]cpuSample{}
	m.metricsMu.Unlock()

	return nil
}

// Run implements core.Module.
func (m *Module) Run(ctx context.Context) error {
	d := m.getDeps()
	p := d.Config.Modules.Process
	s := d.Scheduler
	s.Add(core.Task{Name: "process.inventory", Interval: p.InventoryInterval, Timeout: p.InventoryInterval, Run: m.inventoryTask})
	s.Add(core.Task{Name: "process.lifecycle", Interval: p.LifecycleInterval, Timeout: p.LifecycleInterval, Run: m.lifecycleTask})
	s.Add(core.Task{Name: "process.metrics", Interval: p.MetricsInterval, Timeout: p.MetricsInterval, Run: m.metricsTask})
	<-ctx.Done()
	return nil
}

// RunOnce implements core.OneShot.
func (m *Module) RunOnce(ctx context.Context) error {
	if err := m.inventoryTask(ctx); err != nil {
		return err
	}
	return m.metricsTask(ctx)
}

// Resync implements core.Resyncer.
func (m *Module) Resync(ctx context.Context) error {
	return m.inventoryTask(ctx)
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

func (m *Module) markSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.Status = core.ModuleHealthy
	m.health.Reason = ""
	m.health.LastError = ""
	m.health.LastSuccessAt = m.deps.Clock.Now()
}

func (m *Module) markDegraded(reason string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.Status = core.ModuleDegraded
	m.health.Reason = reason
	if err != nil {
		m.health.LastError = err.Error()
	}
}

// touchSuccess records a successful partial task without clearing an
// existing degraded state. Only a complete inventory clears it.
func (m *Module) touchSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.LastSuccessAt = m.deps.Clock.Now()
}

func (m *Module) log() *slog.Logger {
	d := m.getDeps()
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

func (m *Module) hostEntityID() string {
	d := m.getDeps()
	return ids.HostID(d.Identity.HostID)
}

func snapshotID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("snapshot id: %w", err)
	}
	return id.String(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
