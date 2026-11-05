package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent/internal/config"
	"agent/internal/events"
	"agent/internal/ids"
)

// ModuleFactory creates the enabled modules once Deps is available.
type ModuleFactory func(Deps) ([]Module, error)

// Options configures the runtime.
type Options struct {
	Config         *config.Config
	Identity       Identity
	Seq            events.SeqAllocator
	Sink           Sink
	HeartbeatSink  Sink
	Version        string
	Commit         string
	Logger         *slog.Logger
	Clock          Clock
	ModuleFactory  ModuleFactory
	QueueStats     QueueStats
	ReportingStats ReportingStats
	StartedAt      time.Time
	// SampleWait is the pause between the priming sample and the first
	// emitted metric sample in RunOnce.
	SampleWait time.Duration
}

// Manager wires identity, scheduler, supervisor, emitter and modules.
type Manager struct {
	cfg           *config.Config
	identity      Identity
	log           *slog.Logger
	clock         Clock
	paths         Paths
	sink          Sink
	heartbeatSink Sink
	builder       *events.Builder
	emitter       *EventEmitter
	scheduler     Scheduler
	index         *Index
	modules       []Module
	super         *Supervisor
	queue         QueueStats
	reporting     ReportingStats
	version       string
	commit        string
	startedAt     time.Time
	sampleWait    time.Duration

	statsMu sync.Mutex

	resyncMu      sync.Mutex
	resyncRunning bool
	resyncWG      sync.WaitGroup
}

// NewManager constructs the runtime.
func NewManager(opts Options) (*Manager, error) {
	if opts.Config == nil {
		return nil, errors.New("manager: config is required")
	}
	if opts.Sink == nil {
		return nil, errors.New("manager: sink is required")
	}
	if opts.Seq == nil {
		return nil, errors.New("manager: sequence allocator is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.StartedAt.IsZero() {
		opts.StartedAt = opts.Clock.Now()
	}

	paths := Paths{
		ProcfsRoot: opts.Config.Paths.ProcfsRoot,
		SysfsRoot:  opts.Config.Paths.SysfsRoot,
		HostfsRoot: opts.Config.Paths.HostfsRoot,
	}
	builder := events.NewBuilder(
		opts.Identity.AgentID,
		opts.Identity.AgentRunID,
		opts.Config.Agent.Environment,
		opts.Seq,
		opts.Config.Reporting.MaxEventBytes,
	)
	builder.Clock = opts.Clock.Now
	emitter := NewEventEmitter(builder, opts.Sink, ids.HostID(opts.Identity.HostID), opts.Clock, opts.Logger)
	heartbeatSink := opts.HeartbeatSink
	if heartbeatSink == nil {
		heartbeatSink = opts.Sink
	}
	scheduler := NewScheduler(opts.Logger)
	index := NewIndex()

	// The rate-limit callback needs the manager itself. It is only invoked
	// once modules run, after the manager has been assigned.
	var mgr *Manager
	deps := Deps{
		Config:    opts.Config,
		Identity:  opts.Identity,
		Emitter:   emitter,
		Scheduler: scheduler,
		Index:     index,
		Paths:     paths,
		Clock:     opts.Clock,
		Log:       opts.Logger,
		Version:   opts.Version,
		Commit:    opts.Commit,
		RateLimited: func(n int64) {
			if mgr != nil {
				mgr.recordRateLimited(n)
			}
		},
	}

	var modules []Module
	if opts.ModuleFactory != nil {
		var err error
		modules, err = opts.ModuleFactory(deps)
		if err != nil {
			return nil, fmt.Errorf("create modules: %w", err)
		}
	}

	mgr = &Manager{
		cfg:           opts.Config,
		identity:      opts.Identity,
		log:           opts.Logger,
		clock:         opts.Clock,
		paths:         paths,
		sink:          opts.Sink,
		heartbeatSink: heartbeatSink,
		builder:       builder,
		emitter:       emitter,
		scheduler:     scheduler,
		index:         index,
		modules:       modules,
		super:         NewSupervisor(modules, deps, opts.Logger),
		queue:         opts.QueueStats,
		reporting:     opts.ReportingStats,
		version:       opts.Version,
		commit:        opts.Commit,
		startedAt:     opts.StartedAt,
		sampleWait:    opts.SampleWait,
	}
	return mgr, nil
}

// SetQueueStats wires the spool counters used by the heartbeat.
func (m *Manager) SetQueueStats(q QueueStats) {
	m.statsMu.Lock()
	m.queue = q
	m.statsMu.Unlock()
}

// SetReportingStats wires delivery counters used by the heartbeat.
func (m *Manager) SetReportingStats(r ReportingStats) {
	m.statsMu.Lock()
	m.reporting = r
	m.statsMu.Unlock()
}

// HostEntityID returns host:{host_id}.
func (m *Manager) HostEntityID() string { return ids.HostID(m.identity.HostID) }

// recordRateLimited forwards module-level lifecycle drops to the spool.
func (m *Manager) recordRateLimited(n int64) {
	if n <= 0 {
		return
	}
	m.statsMu.Lock()
	q := m.queue
	m.statsMu.Unlock()
	if r, ok := q.(RateLimitRecorder); ok {
		r.RecordRateLimited(n)
	}
}

// Modules returns the constructed modules (useful for tests).
func (m *Manager) Modules() []Module { return m.modules }

// Run starts periodic collection and blocks until ctx is canceled.
func (m *Manager) Run(ctx context.Context) error {
	m.log.Info("agent started",
		"agent_id", m.identity.AgentID,
		"host_id", m.identity.HostID,
		"run_id", m.identity.AgentRunID,
		"version", m.version,
		"environment", m.cfg.Agent.Environment,
		"modules", len(m.modules),
	)

	schedDone := make(chan struct{})
	go func() {
		_ = m.scheduler.Run(ctx)
		close(schedDone)
	}()
	m.super.Start(ctx)

	hb := time.NewTicker(30 * time.Second)
	defer hb.Stop()
	m.sendHeartbeat()

	for {
		select {
		case <-ctx.Done():
			m.log.Info("agent stopping")
			wait := make(chan struct{})
			go func() {
				m.super.Wait()
				close(wait)
			}()
			select {
			case <-wait:
			case <-time.After(5 * time.Second):
				m.log.Warn("modules did not stop within 5s")
			}
			select {
			case <-schedDone:
			case <-time.After(2 * time.Second):
			}
			resyncDone := make(chan struct{})
			go func() {
				m.resyncWG.Wait()
				close(resyncDone)
			}()
			select {
			case <-resyncDone:
			case <-time.After(5 * time.Second):
				m.log.Warn("resync did not stop within 5s")
			}
			_ = m.sink.Close()
			return nil
		case <-hb.C:
			m.sendHeartbeat()
		}
	}
}

// RunOnce performs one full scan of every module, then closes the sink.
// Rate-based metrics are primed first and sampled again after SampleWait.
func (m *Manager) RunOnce(ctx context.Context) error {
	var errs []error
	log := &snapshotLog{}
	deps := m.deps()
	deps.Emitter = &recordingEmitter{inner: m.emitter, log: log}

	var active []Module
	for _, mod := range m.modules {
		if err := mod.Init(ctx, deps); err != nil {
			if IsUnavailable(err) {
				m.log.Info("module unavailable", "module", mod.Name(), "reason", err)
				continue
			}
			errs = append(errs, fmt.Errorf("%s init: %w", mod.Name(), err))
			continue
		}
		active = append(active, mod)
	}

	primed := false
	for _, mod := range active {
		p, ok := mod.(MetricsPrimer)
		if !ok {
			continue
		}
		primed = true
		if err := p.PrimeMetrics(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s prime metrics: %w", mod.Name(), err))
		}
	}
	if primed && m.sampleWait > 0 {
		timer := time.NewTimer(m.sampleWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			errs = append(errs, ctx.Err())
		case <-timer.C:
		}
	}

	for _, mod := range active {
		one, ok := mod.(OneShot)
		if !ok {
			m.log.Warn("module does not support once mode", "module", mod.Name())
			continue
		}
		start := m.clock.Now()
		if err := one.RunOnce(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", mod.Name(), err))
			continue
		}
		m.log.Info("scan completed", "module", mod.Name(), "duration", m.clock.Now().Sub(start))
	}
	for _, snap := range log.snapshots() {
		if snap.Complete {
			continue
		}
		errs = append(errs, fmt.Errorf("%s snapshot incomplete", snap.Module))
	}
	if err := m.sink.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// snapshotLog records snapshots emitted during RunOnce so incomplete scans
// can be reported as a failure.
type snapshotLog struct {
	mu    sync.Mutex
	items []events.Snapshot
}

func (l *snapshotLog) add(s events.Snapshot) {
	l.mu.Lock()
	l.items = append(l.items, s)
	l.mu.Unlock()
}

func (l *snapshotLog) snapshots() []events.Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]events.Snapshot(nil), l.items...)
}

// recordingEmitter forwards events and records snapshots.
type recordingEmitter struct {
	inner Emitter
	log   *snapshotLog
}

func (r *recordingEmitter) Entities(snapshotID string, e []events.Entity) {
	r.inner.Entities(snapshotID, e)
}
func (r *recordingEmitter) Relationships(snapshotID string, rel []events.Relationship) {
	r.inner.Relationships(snapshotID, rel)
}
func (r *recordingEmitter) RelationshipsRemoved(ids []string, reason string) {
	r.inner.RelationshipsRemoved(ids, reason)
}
func (r *recordingEmitter) Metrics(observedAt time.Time, readings []events.Reading) {
	r.inner.Metrics(observedAt, readings)
}
func (r *recordingEmitter) Lifecycle(l events.Lifecycle) { r.inner.Lifecycle(l) }

func (r *recordingEmitter) LifecycleWithSubject(subject string, l events.Lifecycle) {
	if s, ok := r.inner.(interface {
		LifecycleWithSubject(string, events.Lifecycle)
	}); ok {
		s.LifecycleWithSubject(subject, l)
		return
	}
	r.inner.Lifecycle(l)
}
func (r *recordingEmitter) SnapshotCompleted(s events.Snapshot) {
	r.log.add(s)
	r.inner.SnapshotCompleted(s)
}

// WithSubject preserves the recording wrapper when the containers module
// scopes events to an engine.
func (r *recordingEmitter) WithSubject(subject string) Emitter {
	if w, ok := r.inner.(interface{ WithSubject(string) Emitter }); ok {
		return &recordingEmitter{inner: w.WithSubject(subject), log: r.log}
	}
	return r
}

func (m *Manager) deps() Deps {
	return Deps{
		Config:      m.cfg,
		Identity:    m.identity,
		Emitter:     m.emitter,
		Scheduler:   m.scheduler,
		Index:       m.index,
		Paths:       m.paths,
		Clock:       m.clock,
		Log:         m.log,
		Version:     m.version,
		Commit:      m.commit,
		RateLimited: m.recordRateLimited,
	}
}

// Health returns the current module health map.
func (m *Manager) Health() map[string]ModuleHealth {
	return m.super.Health()
}

func (m *Manager) sendHeartbeat() {
	modules := make(map[string]events.HeartbeatModule)
	overall := events.AgentHealthy
	for name, h := range m.super.Health() {
		hm := events.HeartbeatModule{
			Status:    h.Status,
			Reason:    truncate(h.Reason, 128),
			LastError: truncate(h.LastError, 512),
		}
		if !h.LastSuccessAt.IsZero() {
			t := events.Stamp(h.LastSuccessAt)
			hm.LastSuccessAt = &t
		}
		modules[name] = hm
		switch h.Status {
		case ModuleFailed:
			overall = events.AgentUnhealthy
		case ModuleUnavailable, ModuleDegraded:
			if overall == events.AgentHealthy {
				overall = events.AgentDegraded
			}
		}
	}

	m.statsMu.Lock()
	queueStats := m.queue
	reportingStats := m.reporting
	m.statsMu.Unlock()

	queue := events.HeartbeatQueue{
		DroppedTotal: events.HeartbeatDropped{},
	}
	if queueStats != nil {
		qs := queueStats.QueueSnapshot()
		queue.DepthEvents = qs.DepthEvents
		queue.SizeBytes = qs.SizeBytes
		queue.OldestEventAgeSeconds = qs.OldestEventAgeSeconds
		queue.DroppedTotal = events.HeartbeatDropped{
			P0: qs.Dropped[0],
			P1: qs.Dropped[1],
			P2: qs.Dropped[2],
		}
		queue.RejectedTotal = qs.RejectedTotal
		queue.LifecycleRateLimitedTotal = qs.RateLimitedTotal
	}
	reporting := events.HeartbeatReporting{}
	if reportingStats != nil {
		reporting.ConsecutiveFailures = reportingStats.ConsecutiveFailures()
		if t := reportingStats.LastSuccess(); !t.IsZero() {
			ts := events.Stamp(t)
			reporting.LastSuccessAt = &ts
		}
	}

	now := m.clock.Now()
	hb := events.Heartbeat{
		Agent: events.HeartbeatAgent{
			ID:            m.identity.AgentID,
			RunID:         m.identity.AgentRunID,
			Version:       m.version,
			Commit:        m.commit,
			Status:        overall,
			StartedAt:     events.Stamp(m.startedAt),
			UptimeSeconds: int64(now.Sub(m.startedAt).Seconds()),
			RSSBytes:      processRSS(m.paths.ProcfsRoot),
		},
		HostID:    m.identity.HostID,
		Modules:   modules,
		Queue:     queue,
		Reporting: reporting,
	}
	env, err := m.builder.HeartbeatEvent(m.HostEntityID(), hb)
	if err != nil {
		m.log.Error("build heartbeat", "error", err)
		return
	}
	if err := m.heartbeatSink.Send(env); err != nil {
		m.log.Warn("send heartbeat", "error", err)
	}
}

// Resync triggers an immediate full inventory scan of every module that
// supports it. Calls are coalesced; an already running resync is not
// duplicated.
func (m *Manager) Resync() {
	m.resyncMu.Lock()
	if m.resyncRunning {
		m.resyncMu.Unlock()
		return
	}
	m.resyncRunning = true
	m.resyncMu.Unlock()

	m.resyncWG.Add(1)
	go func() {
		defer func() {
			m.resyncWG.Done()
			m.resyncMu.Lock()
			m.resyncRunning = false
			m.resyncMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, mod := range m.modules {
			r, ok := mod.(Resyncer)
			if !ok {
				continue
			}
			start := m.clock.Now()
			if err := r.Resync(ctx); err != nil {
				m.log.Warn("resync failed", "module", mod.Name(), "error", err)
				continue
			}
			m.log.Info("resync completed", "module", mod.Name(), "duration", m.clock.Now().Sub(start))
		}
	}()
}

func processRSS(procfsRoot string) *int64 {
	b, err := os.ReadFile(filepath.Join(procfsRoot, "self/statm"))
	if err != nil {
		return nil
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return nil
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return nil
	}
	v := pages * int64(os.Getpagesize())
	return &v
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
