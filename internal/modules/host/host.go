// Package host implements the required host inventory and resource metrics
// module.
package host

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"

	"github.com/google/uuid"
)

// Module is the host module.
type Module struct {
	mu          sync.Mutex
	inventoryMu sync.Mutex
	deps        core.Deps
	health      core.ModuleHealth

	filesystems []fsEntry
	prevCPU     cpuSample
	hasPrevCPU  bool
}

// New creates the host module.
func New() *Module { return &Module{} }

// Name implements core.Module.
func (m *Module) Name() string { return "host" }

// Init implements core.Module.
func (m *Module) Init(_ context.Context, d core.Deps) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deps = d
	m.health = core.ModuleHealth{Status: core.ModuleHealthy}
	return nil
}

// Run implements core.Module. It schedules the module's periodic work and
// blocks until the context is canceled.
func (m *Module) Run(ctx context.Context) error {
	d := m.getDeps()
	s := d.Scheduler
	s.Add(core.Task{Name: "host.inventory", Interval: d.Config.Modules.Host.InventoryInterval, Timeout: 2 * d.Config.Modules.Host.InventoryInterval, Run: m.inventoryTask})
	s.Add(core.Task{Name: "host.metrics", Interval: d.Config.Modules.Host.MetricsInterval, Timeout: 30 * time.Second, Run: m.metricsTask})
	s.Add(core.Task{Name: "host.disk_usage", Interval: d.Config.Modules.Host.DiskUsageInterval, Timeout: time.Minute, Run: m.diskUsageTask})
	<-ctx.Done()
	return nil
}

// RunOnce implements core.OneShot.
func (m *Module) RunOnce(ctx context.Context) error {
	if err := m.inventoryTask(ctx); err != nil {
		return err
	}
	if err := m.metricsTask(ctx); err != nil {
		return err
	}
	if err := m.diskUsageTask(ctx); err != nil {
		return err
	}
	return nil
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

// inventoryTask performs the static host inventory and emits the host entity
// plus snapshot.completed.
func (m *Module) inventoryTask(ctx context.Context) error {
	m.inventoryMu.Lock()
	defer m.inventoryMu.Unlock()
	d := m.getDeps()
	started := d.Clock.Now()
	snapID, err := snapshotID()
	if err != nil {
		m.markDegraded("snapshot_id", err)
		return err
	}

	entity, scanErrs := m.buildEntity(d)
	snapshotErrs := make([]events.SnapshotError, 0, len(scanErrs))
	for _, e := range scanErrs {
		snapshotErrs = append(snapshotErrs, events.SnapshotError{
			Code:    events.ErrCodeParseError,
			Message: truncate(e.Error(), 500),
			Kind:    ids.KindHost,
		})
	}
	complete := len(snapshotErrs) == 0

	d.Emitter.Entities(snapID, []events.Entity{entity})
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module:             "host",
		SnapshotID:         snapID,
		ScopeEntityID:      entity.ID,
		CoversKinds:        []string{ids.KindHost},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      1,
		RelationshipsCount: 0,
		Errors:             snapshotErrs,
	})

	if complete {
		m.markSuccess()
	} else {
		m.markDegraded("inventory_incomplete", scanErrs[0])
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
