package containers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"
)

type captureEmitter struct {
	mu         sync.Mutex
	entities   []events.Entity
	rels       []events.Relationship
	removed    []string
	metrics    []events.Reading
	snapshots  []events.Snapshot
	lifecycles []events.Lifecycle
}

func (c *captureEmitter) Entities(_ string, e []events.Entity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entities = append(c.entities, e...)
}
func (c *captureEmitter) Relationships(_ string, r []events.Relationship) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rels = append(c.rels, r...)
}
func (c *captureEmitter) RelationshipsRemoved(ids []string, _ string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removed = append(c.removed, ids...)
}
func (c *captureEmitter) Metrics(_ time.Time, r []events.Reading) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = append(c.metrics, r...)
}
func (c *captureEmitter) Lifecycle(l events.Lifecycle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lifecycles = append(c.lifecycles, l)
}
func (c *captureEmitter) SnapshotCompleted(s events.Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots = append(c.snapshots, s)
}

func (c *captureEmitter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entities = nil
	c.rels = nil
	c.removed = nil
	c.metrics = nil
	c.snapshots = nil
	c.lifecycles = nil
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type fakeContainer struct {
	ID       string
	Name     string
	Image    string
	ImageID  string
	State    string
	Health   string
	PID      int
	ExitCode int
	Restarts int
	Networks map[string]string
	Mounts   []string
}

type fakeImage struct {
	ID   string
	Tags []string
}

type fakeRuntime struct {
	mu          sync.Mutex
	containers  []fakeContainer
	images      []fakeImage
	failList    bool
	failInspect bool
	cpuNs       uint64
	eventLines  []string
}

func (f *fakeRuntime) set(fn func(*fakeRuntime)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

func (f *fakeRuntime) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		p := r.URL.Path
		switch {
		case p == "/version":
			fmt.Fprint(w, `{"Version":"26.1.0","ApiVersion":"1.45","Components":[{"Name":"Engine"}]}`)
		case p == "/v1.45/info":
			fmt.Fprint(w, `{"Driver":"overlay2","CgroupVersion":"2","SecurityOptions":[]}`)
		case p == "/v1.45/containers/json":
			if f.failList {
				http.Error(w, "engine down", http.StatusInternalServerError)
				return
			}
			type summary struct {
				ID      string `json:"Id"`
				Names   []string
				Image   string
				ImageID string
				State   string
			}
			out := make([]summary, 0, len(f.containers))
			for _, c := range f.containers {
				out = append(out, summary{ID: c.ID, Names: []string{"/" + c.Name}, Image: c.Image, ImageID: "sha256:" + c.ImageID, State: c.State})
			}
			_ = json.NewEncoder(w).Encode(out)
		case len(p) > len("/v1.45/containers/") && len(p) > 5 && p[len(p)-5:] == "/json":
			if f.failInspect {
				http.Error(w, "inspect failed", http.StatusInternalServerError)
				return
			}
			id := p[len("/v1.45/containers/") : len(p)-len("/json")]
			for _, c := range f.containers {
				if c.ID != id {
					continue
				}
				networks := map[string]any{}
				for name, nid := range c.Networks {
					networks[name] = map[string]any{"NetworkID": nid, "IPAddress": "172.17.0.2", "Gateway": "172.17.0.1"}
				}
				mounts := make([]map[string]any, 0, len(c.Mounts))
				for _, m := range c.Mounts {
					mounts = append(mounts, map[string]any{"Type": "volume", "Name": m, "Destination": "/data/" + m})
				}
				health := map[string]any(nil)
				if c.Health != "" && c.Health != "none" {
					health = map[string]any{"Status": c.Health}
				}
				body := map[string]any{
					"Id":      c.ID,
					"Name":    "/" + c.Name,
					"Created": "2026-11-04T18:00:00Z",
					"State": map[string]any{
						"Status": c.State, "Pid": c.PID, "ExitCode": c.ExitCode,
						"StartedAt": "2026-11-04T18:00:01Z", "FinishedAt": "0001-01-01T00:00:00Z",
						"Health": health,
					},
					"Image": "sha256:" + c.ImageID,
					"Config": map[string]any{"Image": c.Image, "Labels": map[string]string{
						"com.docker.compose.service": c.Name,
						"secret":                     "no",
					}},
					"HostConfig": map[string]any{"RestartPolicy": map[string]any{"Name": "always"}},
					"NetworkSettings": map[string]any{
						"Ports":    map[string]any{"8080/tcp": []map[string]any{{"HostIp": "0.0.0.0", "HostPort": "18080"}}},
						"Networks": networks,
					},
					"Mounts":       mounts,
					"RestartCount": c.Restarts,
				}
				_ = json.NewEncoder(w).Encode(body)
				return
			}
			http.NotFound(w, r)
		case len(p) > len("/v1.45/containers/") && len(p) > 6 && p[len(p)-6:] == "/stats":
			id := p[len("/v1.45/containers/") : len(p)-len("/stats")]
			for _, c := range f.containers {
				if c.ID != id {
					continue
				}
				body := map[string]any{
					"cpu_stats":    map[string]any{"cpu_usage": map[string]any{"total_usage": f.cpuNs}},
					"memory_stats": map[string]any{"usage": uint64(1000), "limit": uint64(2000), "stats": map[string]uint64{"total_inactive_file": 100}},
					"networks":     map[string]any{"eth0": map[string]uint64{"rx_bytes": 10, "tx_bytes": 20}},
					"blkio_stats": map[string]any{"io_service_bytes_recursive": []map[string]any{
						{"op": "read", "value": 30}, {"op": "write", "value": 40},
					}},
					"pids_stats": map[string]any{"current": 5},
				}
				_ = json.NewEncoder(w).Encode(body)
				return
			}
			http.NotFound(w, r)
		case p == "/v1.45/images/json":
			out := make([]map[string]any, 0, len(f.images))
			for _, im := range f.images {
				out = append(out, map[string]any{
					"Id": "sha256:" + im.ID, "RepoTags": im.Tags,
					"RepoDigests": []string{"acme/api@sha256:" + im.ID},
					"Created":     1762290000, "Size": 100,
				})
			}
			_ = json.NewEncoder(w).Encode(out)
		case p == "/v1.45/networks":
			fmt.Fprint(w, `[{"Id":"net1","Name":"bridge","Driver":"bridge","Scope":"local","IPAM":{"Config":[{"Subnet":"172.17.0.0/16","Gateway":"172.17.0.1"}]},"Options":{}}]`)
		case p == "/v1.45/volumes":
			fmt.Fprint(w, `{"Volumes":[{"Name":"www","Driver":"local","Scope":"local","Mountpoint":"/var/lib/docker/volumes/www/_data","CreatedAt":"2026-11-01T00:00:00Z"}]}`)
		case p == "/v1.45/events":
			w.Header().Set("Content-Type", "application/json")
			for _, line := range f.eventLines {
				fmt.Fprintln(w, line)
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	})
}

type containerFixture struct {
	mod   *Module
	state *engineState
	em    *captureEmitter
	clock *fakeClock
	fake  *fakeRuntime
	deps  core.Deps
}

func newContainerFixture(t *testing.T) *containerFixture {
	t.Helper()
	root := t.TempDir()
	procfs := filepath.Join(root, "procfs")
	sysfs := filepath.Join(root, "sysfs")
	hostfs := filepath.Join(root, "hostfs")
	for _, dir := range []string{procfs, sysfs, hostfs} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var cpuinfo string
	for i := 0; i < 4; i++ {
		cpuinfo += fmt.Sprintf("processor\t: %d\n", i)
	}
	if err := os.WriteFile(filepath.Join(procfs, "cpuinfo"), []byte(cpuinfo), 0o644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeRuntime{
		images: []fakeImage{{ID: "img1", Tags: []string{"acme/api:1.0"}}},
		containers: []fakeContainer{{
			ID: "web1", Name: "web", Image: "acme/api:1.0", ImageID: "img1",
			State: "running", Health: "healthy", PID: 4821, Restarts: 2,
			Networks: map[string]string{"bridge": "net1"},
			Mounts:   []string{"www"},
		}},
		cpuNs: 1_000_000_000,
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: fake.handler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	cfg := config.Default()
	cfg.Paths.ProcfsRoot = procfs
	cfg.Paths.SysfsRoot = sysfs
	cfg.Paths.HostfsRoot = hostfs
	cfg.Modules.Containers.Autodiscovery = false
	cfg.Modules.Containers.Runtimes = []config.RuntimeEndpoint{{Type: "docker", Endpoint: "unix://" + socket}}
	cfg.Modules.Containers.MapVethInterfaces = false
	cfg.Modules.Containers.CollectEvents = false

	em := &captureEmitter{}
	clock := &fakeClock{t: time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)}
	deps := core.Deps{
		Config: cfg,
		Identity: core.Identity{
			AgentID:    "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
			HostID:     "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
			AgentRunID: "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		},
		Emitter:   em,
		Scheduler: core.NewScheduler(slog.New(slog.NewTextHandler(io.Discard, nil))),
		Index:     core.NewIndex(),
		Paths:     core.Paths{ProcfsRoot: procfs, SysfsRoot: sysfs, HostfsRoot: hostfs},
		Clock:     clock,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	oldExists := hostInterfaceExists
	hostInterfaceExists = func(string) bool { return true }
	t.Cleanup(func() { hostInterfaceExists = oldExists })

	mod := New()
	if err := mod.Init(context.Background(), deps); err != nil {
		t.Fatalf("init: %v", err)
	}
	var st *engineState
	for _, s := range mod.states {
		st = s
	}
	if st == nil {
		t.Fatal("no engine discovered")
	}
	return &containerFixture{mod: mod, state: st, em: em, clock: clock, fake: fake, deps: deps}
}

func TestInventoryAndSnapshots(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	f.em.mu.Unlock()

	kinds := map[string]int{}
	for _, e := range entities {
		kinds[e.Kind]++
	}
	if kinds[ids.KindContainerEngine] != 1 || kinds[ids.KindContainer] != 1 || kinds[ids.KindContainerNetwork] != 1 || kinds[ids.KindContainerVolume] != 1 {
		t.Fatalf("entity kinds = %+v", kinds)
	}
	var container events.Entity
	for _, e := range entities {
		if e.Kind == ids.KindContainer {
			container = e
		}
	}
	if container.Name != "web" || container.Attributes["main_pid"] != 4821 {
		t.Fatalf("container entity = %+v", container)
	}
	if _, ok := container.Attributes["exit_code"]; ok {
		t.Fatal("running container must not carry exit_code")
	}
	if labels := container.Labels; len(labels) != 1 || labels["com.docker.compose.service"] != "web" {
		t.Fatalf("labels = %+v", labels)
	}
	relTypes := map[string]int{}
	for _, r := range rels {
		relTypes[r.From+"|"+r.Type]++
	}
	engineID := ids.EngineID(f.deps.Identity.HostID, f.state.key)
	if relTypes[engineID+"|runs_on"] != 1 {
		t.Fatalf("engine relationship missing: %+v", relTypes)
	}
	if len(snaps) != 1 || !snaps[0].Complete || snaps[0].CoversKinds[0] != ids.KindContainer {
		t.Fatalf("snapshot = %+v", snaps)
	}
	if snaps[0].EntitiesCount != len(entities) || snaps[0].RelationshipsCount != len(rels) {
		t.Fatalf("snapshot counts = %d/%d, emitted %d/%d", snaps[0].EntitiesCount, snaps[0].RelationshipsCount, len(entities), len(rels))
	}
	ref, ok := f.deps.Index.Lookup("web1")
	if !ok || ref.MainPID != 4821 || ref.EngineKey != f.state.key {
		t.Fatalf("index ref = %+v, %v", ref, ok)
	}
	if f.mod.Health().Status != core.ModuleHealthy {
		t.Fatalf("health = %+v", f.mod.Health())
	}

	// Images are a separate snapshot covering only container_image.
	f.em.reset()
	if err := f.mod.imagesTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	imgSnaps := append([]events.Snapshot(nil), f.em.snapshots...)
	imgEntities := append([]events.Entity(nil), f.em.entities...)
	imgLifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	if len(imgLifecycles) != 0 {
		t.Fatalf("first image scan must be baseline-only, got %+v", imgLifecycles)
	}
	if len(imgSnaps) != 1 || len(imgSnaps[0].CoversKinds) != 1 || imgSnaps[0].CoversKinds[0] != ids.KindContainerImage || !imgSnaps[0].Complete {
		t.Fatalf("image snapshot = %+v", imgSnaps)
	}
	if len(imgEntities) != 1 || imgEntities[0].Kind != ids.KindContainerImage {
		t.Fatalf("image entities = %+v", imgEntities)
	}
}

func TestLifecycleReconciliation(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()

	// web stops, and the volume disappears from the engine.
	f.fake.set(func(r *fakeRuntime) {
		r.containers[0].State = "exited"
		r.containers[0].PID = 0
		r.containers[0].ExitCode = 1
		r.containers[0].Mounts = nil
	})
	f.clock.Advance(time.Minute)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	removed := append([]string(nil), f.em.removed...)
	f.em.mu.Unlock()

	var sawExited, sawVolumeRemoved bool
	for _, l := range lifecycles {
		if l.Kind == ids.KindContainer && l.Status == "exited" && l.Reason == events.ReasonStateReconciled {
			sawExited = true
		}
		if l.Kind == ids.KindContainerVolume && l.Status == "removed" && l.Reason == events.ReasonEntityRemoved {
			sawVolumeRemoved = true
		}
	}
	if !sawExited {
		t.Fatalf("no exited lifecycle: %+v", lifecycles)
	}
	// The volume list still contains www; only the mount relationship is
	// stale, so the volume itself must not be removed.
	if sawVolumeRemoved {
		t.Fatalf("volume wrongly removed: %+v", lifecycles)
	}
	if len(removed) == 0 {
		t.Fatalf("expected stale mount relationship removal, lifecycles=%+v", lifecycles)
	}
}

func TestContainerRemoval(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.containers = nil })
	f.clock.Advance(time.Minute)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	removed := append([]string(nil), f.em.removed...)
	f.em.mu.Unlock()
	found := false
	for _, l := range lifecycles {
		if l.Kind == ids.KindContainer && l.Status == "removed" && l.Reason == events.ReasonEntityRemoved {
			found = true
		}
	}
	if !found {
		t.Fatalf("no removed lifecycle: %+v", lifecycles)
	}
	if len(removed) == 0 {
		t.Fatal("expected relationship removals for the removed container")
	}
	if _, ok := f.deps.Index.Lookup("web1"); ok {
		t.Fatal("container still in index")
	}
}

func TestMetricsFirstSampleAndUtilization(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	if err := f.mod.metricsTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	first := append([]events.Reading(nil), f.em.metrics...)
	f.em.mu.Unlock()
	reading, found := findReading(first, ids.ContainerID(f.deps.Identity.HostID, f.state.key, "web1"))
	if !found {
		t.Fatalf("no container reading: %+v", first)
	}
	if hasMetric(reading, "container.cpu.utilization") {
		t.Fatal("first CPU sample must be skipped")
	}
	if !hasMetric(reading, "container.cpu.time") || !hasMetric(reading, "container.memory.usage") {
		t.Fatalf("missing counters: %+v", reading)
	}

	f.fake.set(func(r *fakeRuntime) { r.cpuNs = 2_000_000_000 })
	f.clock.Advance(time.Second)
	f.em.reset()
	if err := f.mod.metricsTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	second := append([]events.Reading(nil), f.em.metrics...)
	f.em.mu.Unlock()
	reading, found = findReading(second, ids.ContainerID(f.deps.Identity.HostID, f.state.key, "web1"))
	if !found {
		t.Fatalf("no second reading: %+v", second)
	}
	util, ok := metricValue(reading, "container.cpu.utilization")
	if !ok {
		t.Fatalf("missing utilization: %+v", reading)
	}
	// 1 CPU-second over 1 wall second on 4 logical cores.
	if util < 0.24 || util > 0.26 {
		t.Fatalf("utilization = %v, want 0.25", util)
	}
	// Engine counts are always present.
	if _, found := findReading(second, ids.EngineID(f.deps.Identity.HostID, f.state.key)); !found {
		t.Fatalf("no engine readings: %+v", second)
	}
}

func findReading(readings []events.Reading, entityID string) (events.Reading, bool) {
	for _, r := range readings {
		if r.EntityID == entityID {
			return r, true
		}
	}
	return events.Reading{}, false
}

func hasMetric(r events.Reading, name string) bool {
	_, ok := metricValue(r, name)
	return ok
}

func metricValue(r events.Reading, name string) (float64, bool) {
	for _, m := range r.Metrics {
		if m.Name == name {
			return m.Value, true
		}
	}
	return 0, false
}

func TestEventLifecycleMapping(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()

	f.fake.set(func(r *fakeRuntime) {
		r.containers[0].State = "exited"
		r.containers[0].PID = 0
		r.containers[0].ExitCode = 137
	})
	f.clock.Advance(time.Second)
	f.mod.handleEvent(context.Background(), f.state, model.RuntimeEvent{
		Type: "container", Action: "stop", ActorID: "web1", Time: f.clock.Now(),
	})
	f.mod.handleEvent(context.Background(), f.state, model.RuntimeEvent{
		Type: "container", Action: "die", ActorID: "web1", Time: f.clock.Now(),
	})
	f.em.mu.Lock()
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	if len(lifecycles) != 1 || lifecycles[0].Status != "exited" || lifecycles[0].Reason != events.ReasonContainerStopped {
		t.Fatalf("lifecycles = %+v", lifecycles)
	}
	if v, ok := lifecycles[0].Details["exit_code"]; !ok || v != 137 {
		t.Fatalf("details = %+v", lifecycles[0].Details)
	}

	// OOM wins over stop.
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.containers[0].State = "running" })
	_ = f.mod.inventoryTask(context.Background(), f.state)
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.containers[0].State = "exited" })
	f.clock.Advance(time.Second)
	f.mod.handleEvent(context.Background(), f.state, model.RuntimeEvent{Type: "container", Action: "oom", ActorID: "web1", Time: f.clock.Now()})
	f.mod.handleEvent(context.Background(), f.state, model.RuntimeEvent{Type: "container", Action: "die", ActorID: "web1", Time: f.clock.Now()})
	f.em.mu.Lock()
	lifecycles = append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	if len(lifecycles) == 0 || lifecycles[len(lifecycles)-1].Reason != events.ReasonContainerOOMKilled {
		t.Fatalf("lifecycles = %+v", lifecycles)
	}

	// Destroy removes the entity and its relationships.
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.containers = nil })
	f.clock.Advance(time.Second)
	f.mod.handleEvent(context.Background(), f.state, model.RuntimeEvent{Type: "container", Action: "destroy", ActorID: "web1", Time: f.clock.Now()})
	f.em.mu.Lock()
	lifecycles = append([]events.Lifecycle(nil), f.em.lifecycles...)
	removed := append([]string(nil), f.em.removed...)
	f.em.mu.Unlock()
	if len(lifecycles) != 1 || lifecycles[0].Status != "removed" {
		t.Fatalf("lifecycles = %+v", lifecycles)
	}
	if len(removed) == 0 {
		t.Fatal("destroy must remove container relationships")
	}
}

func TestEngineFailureAndRecovery(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.failList = true })
	if err := f.mod.inventoryTask(context.Background(), f.state); err == nil {
		t.Fatal("expected inventory failure")
	}
	f.em.mu.Lock()
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	entities := append([]events.Entity(nil), f.em.entities...)
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	if len(snaps) != 1 || snaps[0].Complete {
		t.Fatalf("snapshot = %+v", snaps)
	}
	if len(entities) != 1 || entities[0].Kind != ids.KindContainerEngine || entities[0].Attributes["engine_status"] != "unavailable" {
		t.Fatalf("engine entity = %+v", entities)
	}
	if _, ok := f.deps.Index.Lookup("web1"); ok {
		t.Fatal("container index not cleared for an unavailable engine")
	}
	foundDown := false
	for _, l := range lifecycles {
		if l.Kind == ids.KindContainerEngine && l.Status == "down" && l.Reason == events.ReasonEngineUnavailable {
			foundDown = true
		}
	}
	if !foundDown {
		t.Fatalf("lifecycles = %+v", lifecycles)
	}

	f.fake.set(func(r *fakeRuntime) { r.failList = false })
	f.em.reset()
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	lifecycles = append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	foundUp := false
	for _, l := range lifecycles {
		if l.Kind == ids.KindContainerEngine && l.Status == "up" {
			foundUp = true
		}
	}
	if !foundUp {
		t.Fatalf("lifecycles = %+v", lifecycles)
	}
}

func TestDiscoveryDedupesSymlinks(t *testing.T) {
	f := newContainerFixture(t)
	d := f.deps
	link := filepath.Join(t.TempDir(), "docker-link.sock")
	if err := os.Symlink(f.state.endpoint, link); err != nil {
		t.Fatal(err)
	}
	d.Config.Modules.Containers.Autodiscovery = true
	d.Config.Modules.Containers.Runtimes = []config.RuntimeEndpoint{
		{Type: "docker", Endpoint: "unix://" + link},
		{Type: "docker", Endpoint: "unix://" + f.state.endpoint},
	}
	candidates := discoverCandidates(d)
	real := map[string]int{}
	for _, c := range candidates {
		real[c.path]++
	}
	if real[f.state.endpoint] != 1 {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestDiscoveryFindsRootlessSocketsUnderHostfs(t *testing.T) {
	f := newContainerFixture(t)
	hostfs := filepath.Join(t.TempDir(), "hostfs")
	dirs := []string{
		filepath.Join(hostfs, "var", "run"),
		filepath.Join(hostfs, "run", "user", "1000", "podman"),
		filepath.Join(hostfs, "run", "user", "1000"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	rootless := filepath.Join(hostfs, "run", "user", "1000", "podman", "podman.sock")
	docker := filepath.Join(hostfs, "var", "run", "docker.sock")
	for _, path := range []string{rootless, docker} {
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
	}
	d := f.deps
	d.Paths.HostfsRoot = hostfs
	d.Config.Modules.Containers.Autodiscovery = true
	d.Config.Modules.Containers.Runtimes = nil
	candidates := discoverCandidates(d)
	found := map[string]candidate{}
	for _, c := range candidates {
		found[c.path] = c
	}
	if c, ok := found[rootless]; !ok || c.hint != "podman" || c.configured {
		t.Fatalf("rootless candidate = %+v, present=%v", c, ok)
	}
	if c, ok := found[docker]; !ok || c.hint != "docker" {
		t.Fatalf("docker candidate = %+v, present=%v", c, ok)
	}
}

func TestRuntimeIdentifiedFromResponseNotPath(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			fmt.Fprint(w, `{"Version":"4.9.4","ApiVersion":"4.9.4","Components":[{"Name":"Podman Engine","Version":"4.9.4"}]}`)
		case "/libpod/info":
			fmt.Fprint(w, `{"host":{"security":{"rootless":false}},"store":{"graphDriverName":"overlay"}}`)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	adapter, engine, err := detectEngine(context.Background(), candidate{path: socket, hint: "docker"}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Runtime() != "podman" || engine.Runtime != "podman" {
		t.Fatalf("runtime = %s / %s, want podman despite the docker.sock path", adapter.Runtime(), engine.Runtime)
	}
}

func TestEventStreamLifecycleAndReconnect(t *testing.T) {
	f := newContainerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) {
		r.eventLines = []string{
			`{"Type":"container","Action":"start","Actor":{"ID":"web1","Attributes":{}},"time":1762290000,"timeNano":1762290000123456789}`,
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.mod.eventsLoop(ctx, f.state)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.em.mu.Lock()
		found := false
		for _, l := range f.em.lifecycles {
			if l.Kind == ids.KindContainer && l.Reason == events.ReasonContainerStarted {
				found = true
			}
		}
		f.em.mu.Unlock()
		if found {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("events loop did not stop")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("no container_started lifecycle from the event stream")
}

func TestLifecycleRateLimit(t *testing.T) {
	f := newContainerFixture(t)
	f.deps.Config.Limits.MaxLifecycleEventsPerMinute = 2
	var limited int64
	d := f.mod.getDeps()
	d.RateLimited = func(n int64) { limited += n }
	f.mod.mu.Lock()
	f.mod.deps = d
	f.mod.mu.Unlock()

	for i := 0; i < 3; i++ {
		f.mod.emitLifecycle(f.mod.getDeps(), f.state, events.Lifecycle{
			EntityID: "container:" + f.deps.Identity.HostID + ":" + f.state.key + ":web1",
			Kind:     ids.KindContainer, Name: "web", Status: "running",
			Reason: events.ReasonStateReconciled, ObservedAt: events.Stamp(f.clock.Now()),
		})
	}
	f.em.mu.Lock()
	n := len(f.em.lifecycles)
	f.em.mu.Unlock()
	if n != 2 || limited != 1 {
		t.Fatalf("emitted=%d limited=%d, want 2/1", n, limited)
	}
}

func TestPartialInspectDoesNotMarkEngineUnavailable(t *testing.T) {
	f := newContainerFixture(t)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	f.fake.set(func(r *fakeRuntime) { r.failInspect = true; r.containers[0].State = "exited" })
	f.clock.Advance(time.Minute)
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatalf("partial inspect should not fail the whole scan: %v", err)
	}
	f.em.mu.Lock()
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	if len(snaps) != 1 || snaps[0].Complete {
		t.Fatalf("snapshot = %+v", snaps)
	}
	for _, l := range lifecycles {
		if l.Kind == ids.KindContainerEngine && l.Status == "down" {
			t.Fatalf("engine wrongly marked down: %+v", lifecycles)
		}
	}
	st := f.state
	st.mu.Lock()
	available := st.available
	st.mu.Unlock()
	if !available {
		t.Fatal("engine marked unavailable for a partial inspect failure")
	}
}

func TestUnresponsiveSocketEmitsUnavailableEngine(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.SysfsRoot = filepath.Join(root, "sysfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	cfg.Modules.Containers.Autodiscovery = false
	cfg.Modules.Containers.Runtimes = []config.RuntimeEndpoint{{Type: "docker", Endpoint: "unix://" + socket}}
	em := &captureEmitter{}
	deps := core.Deps{
		Config: cfg,
		Identity: core.Identity{
			AgentID:    "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
			HostID:     "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
			AgentRunID: "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		},
		Emitter: em,
		Index:   core.NewIndex(),
		Paths:   core.Paths{ProcfsRoot: cfg.Paths.ProcfsRoot, SysfsRoot: cfg.Paths.SysfsRoot, HostfsRoot: cfg.Paths.HostfsRoot},
		Clock:   core.SystemClock{},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	mod := New()
	if err := mod.Init(context.Background(), deps); err != nil {
		t.Fatalf("an existing but unresponsive socket should not be ErrUnavailable: %v", err)
	}
	em.mu.Lock()
	entities := append([]events.Entity(nil), em.entities...)
	em.mu.Unlock()
	if len(entities) != 1 || entities[0].Kind != ids.KindContainerEngine || entities[0].Attributes["engine_status"] != "unavailable" {
		t.Fatalf("entities = %+v", entities)
	}
	if mod.Health().Status != core.ModuleUnavailable {
		t.Fatalf("health = %+v", mod.Health())
	}
}

func TestRootlessNetworkOmitsHostBridge(t *testing.T) {
	f := newContainerFixture(t)
	f.state.mu.Lock()
	f.state.engine.Rootless = true
	f.state.mu.Unlock()
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	f.em.mu.Unlock()
	for _, e := range entities {
		if e.Kind != ids.KindContainerNetwork {
			continue
		}
		if _, ok := e.Attributes["bridge_interface"]; ok {
			t.Fatalf("rootless network has bridge_interface: %+v", e.Attributes)
		}
		for _, r := range rels {
			if r.From == e.ID && r.Type == "attached_to" {
				t.Fatalf("rootless network has attached_to relationship: %+v", r)
			}
		}
		return
	}
	t.Fatal("network entity missing")
}

func TestMissingBridgeInterfaceOmitsRelationship(t *testing.T) {
	f := newContainerFixture(t)
	hostInterfaceExists = func(string) bool { return false }
	if err := f.mod.inventoryTask(context.Background(), f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	rels := append([]events.Relationship(nil), f.em.rels...)
	f.em.mu.Unlock()
	for _, r := range rels {
		if r.Type == "attached_to" {
			t.Fatalf("dangling attached_to relationship: %+v", r)
		}
	}
}

type envelopeSink struct {
	mu   sync.Mutex
	envs []*events.Envelope
}

func (s *envelopeSink) Send(env *events.Envelope) error {
	s.mu.Lock()
	s.envs = append(s.envs, env)
	s.mu.Unlock()
	return nil
}
func (s *envelopeSink) Close() error { return nil }

func TestAllContainerEventsUseEngineSubject(t *testing.T) {
	f := newContainerFixture(t)
	sink := &envelopeSink{}
	builder := events.NewBuilder(
		f.deps.Identity.AgentID,
		f.deps.Identity.AgentRunID,
		"test", &memSeq{}, 1<<20,
	)
	builder.Clock = f.clock.Now
	em := core.NewEventEmitter(builder, sink, ids.HostID(f.deps.Identity.HostID), f.clock,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	f.state.mu.Lock()
	engineID := f.state.engineID
	f.state.emitter = subjectEmitter(em, engineID)
	f.state.mu.Unlock()

	ctx := context.Background()
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.imagesTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.PrimeMetrics(ctx); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Second)
	if err := f.mod.metricsTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.fake.set(func(r *fakeRuntime) { r.containers[0].State = "exited" })
	f.clock.Advance(time.Minute)
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}

	sink.mu.Lock()
	envs := append([]*events.Envelope(nil), sink.envs...)
	sink.mu.Unlock()
	if len(envs) == 0 {
		t.Fatal("no events emitted")
	}
	seen := map[string]bool{}
	for _, env := range envs {
		seen[env.Type] = true
		if env.Subject != engineID {
			t.Fatalf("%s subject = %s, want engine %s", env.Type, env.Subject, engineID)
		}
	}
	for _, want := range []string{
		events.TypeEntityUpsert, events.TypeRelationshipUpsert,
		events.TypeMetricBatch, events.TypeSnapshotCompleted,
		events.TypeLifecycleChanged,
	} {
		if !seen[want] {
			t.Errorf("no %s event emitted", want)
		}
	}
}

func TestUntaggedImagesFilteredUnlessUsed(t *testing.T) {
	f := newContainerFixture(t)
	f.fake.set(func(r *fakeRuntime) {
		r.containers[0].ImageID = "img3"
		r.images = []fakeImage{
			{ID: "img1", Tags: []string{"acme/api:1.0"}},
			{ID: "img2"},
			{ID: "img3"},
		}
	})
	ctx := context.Background()
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	if err := f.mod.imagesTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	f.em.mu.Unlock()
	got := map[string]events.Entity{}
	for _, e := range entities {
		if e.Kind == ids.KindContainerImage {
			got[e.Attributes["image_id"].(string)] = e
		}
	}
	if len(got) != 2 || got["img1"].ID == "" || got["img3"].ID == "" {
		t.Fatalf("images = %+v, want img1 and used untagged img3", got)
	}
	if got["img3"].Attributes["dangling"] != true || got["img1"].Attributes["dangling"] != false {
		t.Fatalf("dangling attributes = %v / %v", got["img1"].Attributes["dangling"], got["img3"].Attributes["dangling"])
	}
	if _, ok := got["img2"]; ok {
		t.Fatal("unused untagged image was reported")
	}

	f.deps.Config.Modules.Containers.IncludeUntaggedImages = true
	f.em.reset()
	if err := f.mod.imagesTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	n := 0
	for _, e := range f.em.entities {
		if e.Kind == ids.KindContainerImage {
			n++
		}
	}
	f.em.mu.Unlock()
	if n != 3 {
		t.Fatalf("include_untagged_images reported %d images, want 3", n)
	}
}

func TestPrimeMetricsSeedsContainerCPU(t *testing.T) {
	f := newContainerFixture(t)
	ctx := context.Background()
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.PrimeMetrics(ctx); err != nil {
		t.Fatal(err)
	}
	f.fake.set(func(r *fakeRuntime) { r.cpuNs = 2_000_000_000 })
	f.clock.Advance(time.Second)
	f.em.reset()
	if err := f.mod.metricsTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	readings := append([]events.Reading(nil), f.em.metrics...)
	f.em.mu.Unlock()
	reading, ok := findReading(readings, ids.ContainerID(f.deps.Identity.HostID, f.state.key, "web1"))
	if !ok {
		t.Fatalf("no container reading: %+v", readings)
	}
	util, ok := metricValue(reading, "container.cpu.utilization")
	if !ok || util < 0.24 || util > 0.26 {
		t.Fatalf("utilization = %v, present=%v", util, ok)
	}
}
