// Package containers implements the containers module: engine discovery,
// container/image/network/volume/pod inventory, metrics, lifecycle events and
// best-effort veth mapping.
package containers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/docker"
	"agent/internal/modules/containers/model"
	"agent/internal/modules/containers/podman"

	"github.com/vishvananda/netlink"
)

const (
	discoveryInterval = 60 * time.Second
	inventoryTimeout  = 2 * time.Minute
	imagesTimeout     = 2 * time.Minute
	metricsTimeout    = 45 * time.Second
	eventRetryDelay   = 2 * time.Second
)

// Module is the containers module.
type Module struct {
	mu     sync.Mutex
	deps   core.Deps
	health core.ModuleHealth

	labelPatterns []string

	states     map[string]*engineState // keyed by resolved socket path
	discoverMu sync.Mutex
	wg         sync.WaitGroup

	lifecycleMu     sync.Mutex
	lifecycleWindow time.Time
	lifecycleCount  int
}

// engineState is one discovered runtime engine and its per-engine caches.
type engineState struct {
	mu            sync.Mutex
	adapter       model.RuntimeAdapter
	engine        model.Engine
	engineID      string
	key           string
	endpoint      string
	emitter       core.Emitter
	available     bool
	started       bool
	baseline      bool
	imageBaseline bool
	lastErr       string

	// invMu serializes inventory/images/metrics for this engine.
	invMu sync.Mutex

	known      map[string]knownEntity
	rels       map[string]string
	containers map[string]model.Container
	indexRefs  map[string]core.ContainerRef
	stats      map[string]statSample
	recent     map[string]map[string]time.Time
	seenEvents map[string]time.Time

	lastEvent time.Time
}

type knownEntity struct {
	kind   string
	name   string
	status string
	health string
}

type statSample struct {
	wall time.Time
	cpu  uint64
}

// New creates the containers module.
func New() *Module {
	return &Module{states: map[string]*engineState{}}
}

// Name implements core.Module.
func (m *Module) Name() string { return "containers" }

// Init implements core.Module. It probes the runtime sockets and returns
// ErrUnavailable when none is present; the supervisor re-probes every 60 s.
func (m *Module) Init(ctx context.Context, d core.Deps) error {
	if d.Config == nil {
		return fmt.Errorf("containers: config is required")
	}
	if d.Clock == nil {
		d.Clock = core.SystemClock{}
	}
	m.mu.Lock()
	m.deps = d
	m.labelPatterns = append([]string(nil), d.Config.Modules.Containers.LabelAllowlist...)
	m.health = core.ModuleHealth{Status: core.ModuleHealthy}
	m.mu.Unlock()

	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	m.reconcileEngines(probeCtx, false)

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.states) == 0 {
		m.health.Status = core.ModuleUnavailable
		m.health.Reason = "no_container_runtime_socket"
		return core.ErrUnavailable{Reason: "no_container_runtime_socket"}
	}
	return nil
}

// Run implements core.Module.
func (m *Module) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	m.reconcileEngines(runCtx, true)

	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			m.wg.Wait()
			return nil
		case <-ticker.C:
			m.reconcileEngines(runCtx, true)
		}
	}
}

// RunOnce implements core.OneShot.
func (m *Module) RunOnce(ctx context.Context) error {
	m.mu.Lock()
	states := make([]*engineState, 0, len(m.states))
	for _, st := range m.states {
		states = append(states, st)
	}
	m.mu.Unlock()
	sort.Slice(states, func(i, j int) bool { return states[i].endpoint < states[j].endpoint })

	var errs []error
	any := false
	for _, st := range states {
		st.mu.Lock()
		available := st.available
		st.mu.Unlock()
		if !available {
			continue
		}
		any = true
		if err := m.inventoryTask(ctx, st); err != nil {
			errs = append(errs, fmt.Errorf("inventory %s: %w", st.endpoint, err))
			continue
		}
		if m.getDeps().Config.Modules.Containers.CollectImages {
			if err := m.imagesTask(ctx, st); err != nil {
				errs = append(errs, fmt.Errorf("images %s: %w", st.endpoint, err))
			}
		}
		if err := m.metricsTask(ctx, st); err != nil {
			errs = append(errs, fmt.Errorf("metrics %s: %w", st.endpoint, err))
		}
	}
	if !any {
		return core.ErrUnavailable{Reason: "no_container_engine_available"}
	}
	return errors.Join(errs...)
}

// Resync implements core.Resyncer.
func (m *Module) Resync(ctx context.Context) error {
	m.mu.Lock()
	states := make([]*engineState, 0, len(m.states))
	for _, st := range m.states {
		states = append(states, st)
	}
	m.mu.Unlock()

	var errs []error
	collectImages := m.getDeps().Config.Modules.Containers.CollectImages
	for _, st := range states {
		if err := m.inventoryTask(ctx, st); err != nil {
			errs = append(errs, err)
		}
		if collectImages {
			if err := m.imagesTask(ctx, st); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
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

// reconcileEngines applies one discovery round. When start is true, newly
// available engines get their periodic goroutine.
func (m *Module) reconcileEngines(ctx context.Context, start bool) {
	m.discoverMu.Lock()
	defer m.discoverMu.Unlock()

	d := m.getDeps()
	candidates := discoverCandidates(d)

	m.mu.Lock()
	states := m.states
	m.mu.Unlock()

	seen := make(map[string]bool, len(candidates))
	var toStart []*engineState
	for _, c := range candidates {
		seen[c.path] = true
		m.mu.Lock()
		st := states[c.path]
		m.mu.Unlock()

		if st == nil {
			adapter, engine, err := detectEngine(ctx, c, d.Config.Modules.Containers.StatsConcurrency)
			st = &engineState{
				endpoint:   c.path,
				known:      map[string]knownEntity{},
				rels:       map[string]string{},
				indexRefs:  map[string]core.ContainerRef{},
				stats:      map[string]statSample{},
				recent:     map[string]map[string]time.Time{},
				seenEvents: map[string]time.Time{},
			}
			if err != nil {
				// The socket exists but the daemon does not answer: still
				// report the engine, with engine_status unavailable.
				engine = model.Engine{
					Runtime:      c.hint,
					Endpoint:     c.path,
					EndpointType: "unix",
					OwnerUID:     c.ownerUID,
					Status:       "unavailable",
				}
				fallback := adapterFor(c.hint, c.path)
				setStatsConcurrency(fallback, d.Config.Modules.Containers.StatsConcurrency)
				st.adapter = fallback
				st.lastErr = err.Error()
			} else {
				engine.OwnerUID = c.ownerUID
				st.adapter = adapter
			}
			st.engine = engine
			st.key = ids.EngineKey(engine.Runtime, c.path)
			st.engineID = ids.EngineID(d.Identity.HostID, st.key)
			st.emitter = subjectEmitter(d.Emitter, st.engineID)
			st.available = err == nil
			m.mu.Lock()
			states[c.path] = st
			m.mu.Unlock()
			if err != nil {
				m.emitEngineEntity(st)
			}
		} else {
			// Re-probe every engine on every discovery round so a stopped
			// daemon is reported down and a returning one is re-adopted.
			adapter, engine, err := detectEngine(ctx, c, d.Config.Modules.Containers.StatsConcurrency)
			st.mu.Lock()
			wasAvailable := st.available
			st.mu.Unlock()
			if err == nil {
				engine.OwnerUID = c.ownerUID
				st.mu.Lock()
				st.adapter = adapter
				st.engine = engine
				st.available = true
				if !wasAvailable {
					st.lastErr = ""
				}
				// The engine key is pinned when the engine first starts
				// running. Re-identifying a live socket as a different
				// runtime is pathological and requires an agent restart; not
				// rekeying keeps entity IDs stable and avoids racing readers.
				newKey := ids.EngineKey(engine.Runtime, c.path)
				if newKey != st.key && !st.started {
					st.key = newKey
					st.engineID = ids.EngineID(d.Identity.HostID, newKey)
				}
				st.emitter = subjectEmitter(d.Emitter, st.engineID)
				st.mu.Unlock()
				if !wasAvailable {
					m.emitEngineAvailability(st, true)
				}
			} else {
				st.mu.Lock()
				st.lastErr = err.Error()
				was := st.available
				st.available = false
				st.indexRefs = map[string]core.ContainerRef{}
				st.emitter = subjectEmitter(d.Emitter, st.engineID)
				st.mu.Unlock()
				if was {
					m.emitEngineAvailability(st, false)
				}
			}
		}

		st.mu.Lock()
		available := st.available
		started := st.started
		st.mu.Unlock()
		if start && available && !started {
			st.mu.Lock()
			st.started = true
			st.mu.Unlock()
			toStart = append(toStart, st)
		}
	}

	// Engines whose socket disappeared are reported down but kept so that a
	// reappearing socket reuses the same identity.
	m.mu.Lock()
	for endpoint, st := range states {
		if seen[endpoint] {
			continue
		}
		st.mu.Lock()
		was := st.available
		st.available = false
		if was {
			st.lastErr = "socket_missing"
		}
		st.indexRefs = map[string]core.ContainerRef{}
		st.mu.Unlock()
		if was {
			m.emitEngineAvailability(st, false)
		}
	}
	m.mu.Unlock()

	for _, st := range toStart {
		m.wg.Add(1)
		go func(st *engineState) {
			defer m.wg.Done()
			m.runEngine(ctx, st)
		}(st)
	}
	m.publishIndex()
	m.refreshHealth()
}

func (m *Module) runEngine(ctx context.Context, st *engineState) {
	d := m.getDeps()
	c := d.Config.Modules.Containers

	go func() {
		m.runInventory(ctx, st)
		if c.CollectEvents {
			go m.eventsLoop(ctx, st)
		}
	}()

	inv := time.NewTicker(c.InventoryInterval)
	defer inv.Stop()
	img := time.NewTicker(c.ImagesInterval)
	defer img.Stop()
	met := time.NewTicker(c.MetricsInterval)
	defer met.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-inv.C:
			m.runInventory(ctx, st)
		case <-img.C:
			if c.CollectImages {
				taskCtx, cancel := context.WithTimeout(ctx, imagesTimeout)
				if err := m.imagesTask(taskCtx, st); err != nil {
					m.log().Warn("container images scan failed", "endpoint", st.endpoint, "error", err)
				}
				cancel()
			}
		case <-met.C:
			taskCtx, cancel := context.WithTimeout(ctx, metricsTimeout)
			if err := m.metricsTask(taskCtx, st); err != nil {
				m.log().Debug("container metrics failed", "endpoint", st.endpoint, "error", err)
			}
			cancel()
		}
	}
}

func (m *Module) runInventory(ctx context.Context, st *engineState) {
	taskCtx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()
	if err := m.inventoryTask(taskCtx, st); err != nil {
		m.log().Warn("container inventory failed", "endpoint", st.endpoint, "error", err)
	}
}

// subjectEmitter returns an emitter view whose CloudEvents subject is the
// engine entity, as required for every containers-module event.
func subjectEmitter(base core.Emitter, subject string) core.Emitter {
	if base == nil {
		return nil
	}
	if s, ok := base.(interface{ WithSubject(string) core.Emitter }); ok {
		return s.WithSubject(subject)
	}
	return base
}

func (m *Module) emitterFor(st *engineState) core.Emitter {
	if st == nil {
		return m.getDeps().Emitter
	}
	st.mu.Lock()
	em := st.emitter
	st.mu.Unlock()
	if em != nil {
		return em
	}
	return m.getDeps().Emitter
}

// emitEngineEntity emits the engine upsert without a lifecycle transition.
// It is used when a socket exists but its daemon does not answer.
func (m *Module) emitEngineEntity(st *engineState) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	if d.Emitter == nil {
		return
	}
	st.mu.Lock()
	engine := st.engine
	engineID := st.engineID
	st.mu.Unlock()
	if engineID == "" {
		return
	}
	d.Emitter.Entities("", []events.Entity{engineEntity(engine, engineID, events.Stamp(d.Clock.Now()))})
}

func (m *Module) emitEngineAvailability(st *engineState, up bool) {
	m.emitEngineTransition(st, up, true)
}

// emitEngineTransition emits an engine availability transition. The entity
// upsert is optional because inventory already emitted the engine entity.
func (m *Module) emitEngineTransition(st *engineState, up, emitEntity bool) {
	st.mu.Lock()
	if !st.baseline {
		st.mu.Unlock()
		return
	}
	engine := st.engine
	engineID := st.engineID
	st.mu.Unlock()
	if engineID == "" {
		return
	}
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	now := events.Stamp(d.Clock.Now())
	if up {
		engine.Status = "available"
	} else {
		engine.Status = "unavailable"
	}
	if emitEntity {
		d.Emitter.Entities("", []events.Entity{engineEntity(engine, engineID, now)})
	}
	status := events.StatusDown
	reason := events.ReasonEngineUnavailable
	if up {
		status = events.StatusUp
		reason = events.ReasonEngineAvailable
	}
	m.emitLifecycle(d, st, events.Lifecycle{
		EntityID:   engineID,
		Kind:       ids.KindContainerEngine,
		Name:       engineName(engine),
		Status:     status,
		Reason:     reason,
		ObservedAt: now,
	})
}

func (m *Module) refreshHealth() {
	now := m.getDeps().Clock.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.states) == 0 {
		m.health = core.ModuleHealth{Status: core.ModuleUnavailable, Reason: "no_container_runtime_socket"}
		return
	}
	var available, unavailable, degraded int
	var lastErr string
	for _, st := range m.states {
		st.mu.Lock()
		if st.available {
			available++
			if st.lastErr != "" {
				degraded++
			}
		} else {
			unavailable++
		}
		if st.lastErr != "" {
			lastErr = st.lastErr
		}
		st.mu.Unlock()
	}
	switch {
	case available == 0:
		m.health.Status = core.ModuleUnavailable
		m.health.Reason = "engine_unavailable"
		m.health.LastError = truncate(lastErr, 512)
	case unavailable > 0:
		m.health.Status = core.ModuleDegraded
		m.health.Reason = "partial_engine_failure"
		m.health.LastError = truncate(lastErr, 512)
	case degraded > 0:
		m.health.Status = core.ModuleDegraded
		m.health.Reason = "partial_scan_failure"
		m.health.LastError = truncate(lastErr, 512)
	default:
		m.health.Status = core.ModuleHealthy
		m.health.Reason = ""
		m.health.LastError = ""
	}
	if available > 0 {
		m.health.LastSuccessAt = now
	}
}

func (m *Module) markEngineDegraded(st *engineState, reason string, err error) {
	st.mu.Lock()
	st.lastErr = reason
	if err != nil {
		st.lastErr = err.Error()
	}
	st.mu.Unlock()
	m.refreshHealth()
}

func (m *Module) markEngineHealthy(st *engineState) {
	st.mu.Lock()
	st.available = true
	st.lastErr = ""
	st.mu.Unlock()
	m.refreshHealth()
}

// publishIndex republishes the shared container index from every engine.
func (m *Module) publishIndex() {
	d := m.getDeps()
	if d.Index == nil {
		return
	}
	publisher, ok := d.Index.(core.ContainerPublisher)
	if !ok {
		return
	}
	m.mu.Lock()
	states := make([]*engineState, 0, len(m.states))
	for _, st := range m.states {
		states = append(states, st)
	}
	m.mu.Unlock()

	var refs []core.ContainerRef
	for _, st := range states {
		st.mu.Lock()
		for _, ref := range st.indexRefs {
			refs = append(refs, ref)
		}
		st.mu.Unlock()
	}
	publisher.Publish(refs)
}

// labelAllowed applies the configured label allowlist globs.
func (m *Module) labelAllowed(key string) bool {
	m.mu.Lock()
	patterns := m.labelPatterns
	m.mu.Unlock()
	for _, p := range patterns {
		if ok, _ := path.Match(p, key); ok {
			return true
		}
	}
	return false
}

// hostInterfaceExists reports whether a host bridge interface currently
// exists. It is replaceable in tests.
var hostInterfaceExists = func(name string) bool {
	_, err := netlink.LinkByName(name)
	return err == nil
}

// sanitizeNetworkBridge follows the wire contract: rootless engines never
// have a host bridge, and rootful engines report one only while it exists.
func (m *Module) sanitizeNetworkBridge(st *engineState, n *model.Network) {
	st.mu.Lock()
	rootless := st.engine.Rootless
	st.mu.Unlock()
	if rootless {
		n.BridgeInterface = ""
		return
	}
	if n.BridgeInterface != "" && !hostInterfaceExists(n.BridgeInterface) {
		n.BridgeInterface = ""
	}
}

func (m *Module) filterLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if len(out) >= 64 {
			break
		}
		if m.labelAllowed(k) {
			out[k] = truncate(labels[k], 256)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (m *Module) hostID() string { return m.getDeps().Identity.HostID }

// emitLifecycle applies the per-minute lifecycle budget before emitting.
func (m *Module) emitLifecycle(d core.Deps, st *engineState, l events.Lifecycle) {
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
	if sle, ok := d.Emitter.(interface {
		LifecycleWithSubject(string, events.Lifecycle)
	}); ok && st != nil {
		st.mu.Lock()
		engineID := st.engineID
		st.mu.Unlock()
		if engineID != "" {
			sle.LifecycleWithSubject(engineID, l)
			return
		}
	}
	d.Emitter.Lifecycle(l)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func adapterFor(runtimeName, socketPath string) model.RuntimeAdapter {
	if strings.EqualFold(runtimeName, "podman") {
		return podman.New(socketPath)
	}
	return docker.New(socketPath)
}

// hostPath maps an absolute host path into the configured hostfs root.
func hostPath(hostfsRoot, p string) string {
	if hostfsRoot == "" || hostfsRoot == "/" {
		return p
	}
	if strings.HasPrefix(p, hostfsRoot) {
		return p
	}
	return filepath.Join(hostfsRoot, strings.TrimPrefix(p, "/"))
}
