package process

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
)

const (
	testHostID = "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03"
	testAgent  = "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55"
	testRun    = "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f"
)

type captureEmitter struct {
	mu            sync.Mutex
	entities      []events.Entity
	relationships []events.Relationship
	metrics       []events.Reading
	snapshots     []events.Snapshot
	lifecycles    []events.Lifecycle
}

func (c *captureEmitter) Entities(_ string, list []events.Entity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entities = append(c.entities, list...)
}

func (c *captureEmitter) Relationships(_ string, list []events.Relationship) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.relationships = append(c.relationships, list...)
}

func (c *captureEmitter) RelationshipsRemoved(_ []string, _ string) {}

func (c *captureEmitter) Metrics(at time.Time, list []events.Reading) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = append(c.metrics, list...)
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

func (c *captureEmitter) resetMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = nil
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type testFixture struct {
	root        string
	procfs      string
	hostfs      string
	cfg         *config.Config
	em          *captureEmitter
	mod         *Module
	clock       *testClock
	index       *core.Index
	rateLimited atomic.Int64
}

type mutateFunc func(*config.Config)

func newTestFixture(t *testing.T, mutate mutateFunc) *testFixture {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join("..", "..", "..", "testdata"), root)

	cfg := config.Default()
	cfg.Agent.Environment = "test"
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.SysfsRoot = filepath.Join(root, "sysfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	if mutate != nil {
		mutate(cfg)
	}

	clock := &testClock{t: time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)}
	em := &captureEmitter{}
	index := core.NewIndex()
	f := &testFixture{
		root:   root,
		procfs: cfg.Paths.ProcfsRoot,
		hostfs: cfg.Paths.HostfsRoot,
		cfg:    cfg,
		em:     em,
		clock:  clock,
		index:  index,
	}
	mod := New()
	deps := core.Deps{
		Config:   cfg,
		Identity: core.Identity{AgentID: testAgent, HostID: testHostID, AgentRunID: testRun},
		Emitter:  em,
		Index:    index,
		Paths:    core.Paths{ProcfsRoot: cfg.Paths.ProcfsRoot, SysfsRoot: cfg.Paths.SysfsRoot, HostfsRoot: cfg.Paths.HostfsRoot},
		Clock:    clock,
		Version:  "test",
		RateLimited: func(n int64) {
			f.rateLimited.Add(n)
		},
	}
	if err := mod.Init(context.Background(), deps); err != nil {
		t.Fatal(err)
	}
	f.mod = mod
	return f
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInventory(t *testing.T) {
	f := newTestFixture(t, nil)
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.entities) != 5 {
		t.Fatalf("entities = %d, want 5: %v", len(f.em.entities), entityPIDs(f.em.entities))
	}
	for _, pid := range []int{1, 100, 101, 200, 400} {
		if _, ok := entityByPID(f.em.entities, pid); !ok {
			t.Errorf("missing process %d", pid)
		}
	}
	if _, ok := entityByPID(f.em.entities, 300); ok {
		t.Error("kernel thread 300 must be skipped")
	}

	init := mustEntity(t, f.em.entities, 1)
	if init.Name != "systemd" || init.Kind != ids.KindProcess {
		t.Fatalf("pid 1 entity: %+v", init)
	}
	if init.ID != ids.ProcessID(testHostID, "6f1f3b0e", 1, 100) {
		t.Fatalf("pid 1 id = %s", init.ID)
	}
	if init.Attributes["user_name"] != "root" || init.Attributes["user_id"] != 0 {
		t.Fatalf("pid 1 user: %v", init.Attributes)
	}
	if init.Attributes["group_id"] != 0 {
		t.Fatalf("pid 1 group: %v", init.Attributes)
	}
	if init.Attributes["state"] != "sleeping" {
		t.Fatalf("pid 1 state: %v", init.Attributes["state"])
	}
	if init.Attributes["cgroup"] != "/init.scope" {
		t.Fatalf("pid 1 cgroup: %v", init.Attributes["cgroup"])
	}
	if init.Attributes["executable"] != "/sbin/init" {
		t.Fatalf("pid 1 executable: %v", init.Attributes["executable"])
	}
	if _, ok := init.Attributes["executable_deleted"]; ok {
		t.Fatal("pid 1 must not be marked deleted")
	}
	wantStart := time.Unix(1700000000, 0).UTC().Add(time.Second)
	if got := init.Attributes["started_at"]; got != wantStart {
		t.Fatalf("started_at = %v, want %v", got, wantStart)
	}

	nginx := mustEntity(t, f.em.entities, 100)
	if nginx.Attributes["user_name"] != "www-data" {
		t.Fatalf("pid 100 user: %v", nginx.Attributes["user_name"])
	}
	if _, ok := nginx.Attributes["command_line"]; ok {
		t.Fatal("command_line must be omitted by default")
	}

	deleted := mustEntity(t, f.em.entities, 400)
	if deleted.Attributes["executable"] != "/usr/bin/deleted-proc" || deleted.Attributes["executable_deleted"] != true {
		t.Fatalf("pid 400 executable: %v", deleted.Attributes)
	}

	if len(f.em.relationships) != 9 {
		t.Fatalf("relationships = %d, want 9: %+v", len(f.em.relationships), f.em.relationships)
	}
	hostID := ids.HostID(testHostID)
	runsOn := 0
	for _, rel := range f.em.relationships {
		if rel.Type == "runs_on" {
			runsOn++
			if rel.To != hostID {
				t.Errorf("runs_on target = %s, want host", rel.To)
			}
		}
	}
	if runsOn != 5 {
		t.Fatalf("runs_on relationships = %d, want 5", runsOn)
	}
	parent := mustEntity(t, f.em.entities, 100)
	child := mustEntity(t, f.em.entities, 101)
	wantChild := ids.RelationshipID(child.ID, "child_of", parent.ID)
	foundChild := false
	for _, rel := range f.em.relationships {
		if rel.ID == wantChild {
			foundChild = true
			if rel.Type != "child_of" || rel.From != child.ID || rel.To != parent.ID {
				t.Fatalf("child relationship: %+v", rel)
			}
		}
	}
	if !foundChild {
		t.Fatal("missing child_of relationship 101 -> 100")
	}

	if len(f.em.snapshots) != 1 {
		t.Fatalf("snapshots = %d", len(f.em.snapshots))
	}
	snap := f.em.snapshots[0]
	if snap.Module != "process" || !snap.Complete || snap.EntitiesCount != 5 || snap.RelationshipsCount != 9 {
		t.Fatalf("snapshot: %+v", snap)
	}
	if len(snap.CoversKinds) != 1 || snap.CoversKinds[0] != ids.KindProcess || snap.ScopeEntityID != hostID {
		t.Fatalf("snapshot scope: %+v", snap)
	}
}

func TestInventoryResolvesContainer(t *testing.T) {
	f := newTestFixture(t, nil)
	containerID := strings.Repeat("a", 64)
	entityID := ids.ContainerID(testHostID, "docker-3fa91c2e", containerID)
	f.index.Publish([]core.ContainerRef{{
		ContainerID: containerID,
		EntityID:    entityID,
		EngineKey:   "docker-3fa91c2e",
		MainPID:     200,
	}})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}

	proc := mustEntity(t, f.em.entities, 200)
	if proc.Attributes["container_id_hint"] != containerID {
		t.Fatalf("container hint: %v", proc.Attributes)
	}
	if proc.Attributes["container_runtime_hint"] != "docker" {
		t.Fatalf("runtime hint: %v", proc.Attributes)
	}
	want := ids.RelationshipID(proc.ID, "runs_on", entityID)
	found := false
	for _, rel := range f.em.relationships {
		if rel.ID == want {
			found = true
			if rel.To != entityID {
				t.Fatalf("runs_on target = %s, want %s", rel.To, entityID)
			}
		}
	}
	if !found {
		t.Fatal("missing process runs_on container relationship")
	}
}

func TestCommandLineRedaction(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.CollectCommandLine = true
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	deleted := mustEntity(t, f.em.entities, 400)
	got, _ := deleted.Attributes["command_line"].(string)
	if got != "deleted-proc --[REDACTED]" {
		t.Fatalf("command_line = %q", got)
	}
	if strings.Contains(got, "abcdef") {
		t.Fatal("secret leaked into command_line")
	}
	nginx := mustEntity(t, f.em.entities, 100)
	if nginx.Attributes["command_line"] != "nginx: master process /usr/sbin/nginx -g daemon off;" {
		t.Fatalf("nginx command_line = %q", nginx.Attributes["command_line"])
	}
}

func TestNameFiltersAndKernelThreads(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.IncludeNames = []string{"nginx"}
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := entityPIDs(f.em.entities); len(got) != 2 || got[0] != 100 || got[1] != 101 {
		t.Fatalf("include filter entities = %v", got)
	}
	if f.em.snapshots[0].EntitiesCount != 2 || f.em.snapshots[0].RelationshipsCount != 3 {
		t.Fatalf("snapshot counts: %+v", f.em.snapshots[0])
	}

	f = newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.ExcludeNames = []string{"nginx"}
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := entityPIDs(f.em.entities); len(got) != 3 || got[0] != 1 || got[1] != 200 || got[2] != 400 {
		t.Fatalf("exclude filter entities = %v", got)
	}

	f = newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.IncludeKernelThreads = true
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := entityByPID(f.em.entities, 300); !ok {
		t.Fatal("kernel thread must be included when configured")
	}
}

func TestInventoryEntityLimit(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Limits.MaxEntitiesPerKind = 2
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.entities) != 2 {
		t.Fatalf("entities = %d, want 2", len(f.em.entities))
	}
	snap := f.em.snapshots[0]
	if snap.Complete {
		t.Fatal("limit hit must make the snapshot incomplete")
	}
	if snap.EntitiesCount != len(f.em.entities) || snap.RelationshipsCount != len(f.em.relationships) {
		t.Fatalf("snapshot counts do not match: %+v", snap)
	}
	if len(snap.Errors) != 1 || snap.Errors[0].Code != events.ErrCodeLimitExceeded {
		t.Fatalf("snapshot errors: %+v", snap.Errors)
	}
	if got := f.mod.Health().Status; got != core.ModuleDegraded {
		t.Fatalf("health = %s, want degraded", got)
	}
}

func TestInventoryListFailure(t *testing.T) {
	f := newTestFixture(t, nil)
	if err := os.RemoveAll(f.procfs); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.entities) != 0 || len(f.em.relationships) != 0 {
		t.Fatalf("entities=%d relationships=%d", len(f.em.entities), len(f.em.relationships))
	}
	snap := f.em.snapshots[0]
	if snap.Complete || len(snap.Errors) != 1 {
		t.Fatalf("snapshot: %+v", snap)
	}
	if snap.Errors[0].Code != events.ErrCodeUnavailable {
		t.Fatalf("error code = %s", snap.Errors[0].Code)
	}
}

func TestLifecycleBaselineAndTransitions(t *testing.T) {
	f := newTestFixture(t, nil)
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The initial scan establishes the baseline; no lifecycle events.
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 0 {
		t.Fatalf("baseline emitted %d lifecycle events: %+v", len(f.em.lifecycles), f.em.lifecycles)
	}

	writeProcFixture(t, f.procfs, 500, "worker", 1, 1500, 10, 5)
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 1 {
		t.Fatalf("lifecycle events = %d, want 1", len(f.em.lifecycles))
	}
	started := f.em.lifecycles[0]
	if started.Reason != events.ReasonProcessStarted || started.Status != events.StatusRunning {
		t.Fatalf("started event: %+v", started)
	}
	if started.EntityID != ids.ProcessID(testHostID, "6f1f3b0e", 500, 1500) {
		t.Fatalf("started entity id = %s", started.EntityID)
	}
	if started.Name != "worker" || started.Kind != ids.KindProcess {
		t.Fatalf("started event: %+v", started)
	}

	if err := os.RemoveAll(filepath.Join(f.procfs, "500")); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 2 {
		t.Fatalf("lifecycle events = %d, want 2", len(f.em.lifecycles))
	}
	exited := f.em.lifecycles[1]
	if exited.Reason != events.ReasonProcessExited || exited.Status != events.StatusExited {
		t.Fatalf("exited event: %+v", exited)
	}
	if exited.PreviousStatus == nil || *exited.PreviousStatus != events.StatusRunning {
		t.Fatalf("previous status: %+v", exited.PreviousStatus)
	}
	if exited.EntityID != started.EntityID {
		t.Fatalf("exited id = %s, want %s", exited.EntityID, started.EntityID)
	}

	// Kernel threads are not part of the observed set.
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 2 {
		t.Fatalf("unexpected lifecycle events: %+v", f.em.lifecycles)
	}
}

func TestLifecycleRateLimit(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Limits.MaxLifecycleEventsPerMinute = 1
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	writeProcFixture(t, f.procfs, 500, "worker-a", 1, 1500, 10, 5)
	writeProcFixture(t, f.procfs, 501, "worker-b", 1, 1600, 10, 5)
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 1 {
		t.Fatalf("lifecycle events = %d, want 1", len(f.em.lifecycles))
	}
	if f.em.lifecycles[0].Name != "worker-a" {
		t.Fatalf("first event = %+v", f.em.lifecycles[0])
	}
	if got := f.rateLimited.Load(); got != 1 {
		t.Fatalf("rate limited = %d, want 1", got)
	}
}

func TestLifecycleIncludeFilter(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.IncludeNames = []string{"nginx", "worker"}
	})
	if err := f.mod.inventoryTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	writeProcFixture(t, f.procfs, 500, "other", 1, 1500, 10, 5)
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 0 {
		t.Fatalf("filtered process emitted lifecycle: %+v", f.em.lifecycles)
	}
	writeProcFixture(t, f.procfs, 501, "worker", 1, 1600, 10, 5)
	if err := f.mod.lifecycleTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.lifecycles) != 1 || f.em.lifecycles[0].Name != "worker" {
		t.Fatalf("lifecycle events: %+v", f.em.lifecycles)
	}
}

func TestMetricsFirstSampleAndRates(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.TopN = 50
	})
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	readings := readingsByPID(t, f.em.metrics)
	if len(readings) != 5 {
		t.Fatalf("readings = %d, want 5", len(readings))
	}
	nginx := readings[100]
	metrics := metricValues(nginx)
	if _, ok := metrics["process.cpu.utilization"]; ok {
		t.Fatal("first sample must skip process.cpu.utilization")
	}
	if metrics["process.cpu.time"] != 0.35 {
		t.Fatalf("cpu.time = %v, want 0.35", metrics["process.cpu.time"])
	}
	if metrics["process.memory.rss"] != 9000*1024 {
		t.Fatalf("rss = %v", metrics["process.memory.rss"])
	}
	if metrics["process.memory.virtual"] != 22345*1024 {
		t.Fatalf("virtual = %v", metrics["process.memory.virtual"])
	}
	if metrics["process.threads"] != 8 {
		t.Fatalf("threads = %v", metrics["process.threads"])
	}
	if metrics["process.open_fds"] != 4 {
		t.Fatalf("open_fds = %v", metrics["process.open_fds"])
	}
	if _, ok := metricValues(readings[1])["process.open_fds"]; ok {
		t.Fatal("pid 1 has no fd fixture; open_fds must be omitted")
	}

	f.clock.Advance(10 * time.Second)
	writeStatFixture(t, f.procfs, 100, "nginx", 75, 10, 500, 900)
	f.em.resetMetrics()
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	readings = readingsByPID(t, f.em.metrics)
	metrics = metricValues(readings[100])
	util, ok := metrics["process.cpu.utilization"]
	if !ok {
		t.Fatal("second sample must include process.cpu.utilization")
	}
	if util < 0.0249 || util > 0.0251 {
		t.Fatalf("utilization = %v, want 0.025", util)
	}
	if metrics["process.cpu.time"] != 0.85 {
		t.Fatalf("cpu.time = %v, want 0.85", metrics["process.cpu.time"])
	}
}

func TestMetricsSelection(t *testing.T) {
	f := newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.MetricsMode = "top_n"
		c.Modules.Process.TopN = 1
	})
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := readingsByPID(t, f.em.metrics)
	if len(got) != 2 || got[1].EntityID == "" || got[200].EntityID == "" {
		t.Fatalf("top_n first sample pids = %v", readingPIDs(got))
	}

	f.clock.Advance(10 * time.Second)
	writeStatFixture(t, f.procfs, 100, "nginx", 75, 10, 500, 900)
	f.em.resetMetrics()
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	got = readingsByPID(t, f.em.metrics)
	if len(got) != 2 || got[100].EntityID == "" || got[200].EntityID == "" {
		t.Fatalf("top_n second sample pids = %v", readingPIDs(got))
	}

	f = newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.MetricsMode = "allowlist"
		c.Modules.Process.IncludeNames = []string{"nginx"}
	})
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	got = readingsByPID(t, f.em.metrics)
	if len(got) != 2 || got[100].EntityID == "" || got[101].EntityID == "" {
		t.Fatalf("allowlist pids = %v", readingPIDs(got))
	}

	f = newTestFixture(t, func(c *config.Config) {
		c.Modules.Process.MetricsMode = "all"
	})
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.metrics) != 5 {
		t.Fatalf("all mode readings = %d", len(f.em.metrics))
	}
}

func TestMetricsPrunePreviousSamples(t *testing.T) {
	f := newTestFixture(t, nil)
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(f.mod.prevSamples); got != 5 {
		t.Fatalf("previous samples = %d, want 5", got)
	}
	if err := os.RemoveAll(filepath.Join(f.procfs, "400")); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Second)
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(f.mod.prevSamples); got != 4 {
		t.Fatalf("previous samples after exit = %d, want 4", got)
	}
}

func TestInitRequiresBootID(t *testing.T) {
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "procfs"), 0o755)
	cfg := config.Default()
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	deps := core.Deps{
		Config:   cfg,
		Identity: core.Identity{HostID: testHostID},
		Emitter:  &captureEmitter{},
		Paths:    core.Paths{ProcfsRoot: cfg.Paths.ProcfsRoot, HostfsRoot: cfg.Paths.HostfsRoot},
		Clock:    &testClock{t: time.Now()},
	}
	if err := New().Init(context.Background(), deps); err == nil {
		t.Fatal("Init must fail without a boot id")
	}
}

func entityByPID(entities []events.Entity, pid int) (events.Entity, bool) {
	for _, e := range entities {
		if e.Attributes["pid"] == pid {
			return e, true
		}
	}
	return events.Entity{}, false
}

func mustEntity(t *testing.T, entities []events.Entity, pid int) events.Entity {
	t.Helper()
	e, ok := entityByPID(entities, pid)
	if !ok {
		t.Fatalf("missing entity for pid %d", pid)
	}
	return e
}

func entityPIDs(entities []events.Entity) []int {
	out := make([]int, 0, len(entities))
	for _, e := range entities {
		if pid, ok := e.Attributes["pid"].(int); ok {
			out = append(out, pid)
		}
	}
	return out
}

func readingsByPID(t *testing.T, readings []events.Reading) map[int]events.Reading {
	t.Helper()
	out := map[int]events.Reading{}
	for _, r := range readings {
		parts := strings.Split(r.EntityID, ":")
		if len(parts) < 3 {
			t.Fatalf("unexpected entity id %q", r.EntityID)
		}
		pid, err := strconv.Atoi(parts[len(parts)-2])
		if err != nil {
			t.Fatalf("unexpected entity id %q", r.EntityID)
		}
		out[pid] = r
	}
	return out
}

func readingPIDs(readings map[int]events.Reading) []int {
	out := make([]int, 0, len(readings))
	for pid := range readings {
		out = append(out, pid)
	}
	return out
}

func metricValues(r events.Reading) map[string]float64 {
	out := map[string]float64{}
	for _, m := range r.Metrics {
		out[m.Name] = m.Value
	}
	return out
}

// writeProcFixture creates a minimal /proc entry for lifecycle tests.
func writeProcFixture(t *testing.T, procfsRoot string, pid int, comm string, ppid int, start, utime, stime uint64) {
	t.Helper()
	dir := filepath.Join(procfsRoot, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stat := statFixture(pid, comm, ppid, start, utime, stime)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	status := "Name:\t" + comm + "\nUid:\t0\t0\t0\t0\nGid:\t0\t0\t0\t0\nVmSize:\t4096 kB\nVmRSS:\t1024 kB\nThreads:\t1\n"
	if err := os.WriteFile(filepath.Join(dir, "status"), []byte(status), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup"), []byte("0::/system.slice/test.service\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeStatFixture(t *testing.T, procfsRoot string, pid int, comm string, utime, stime, start, rssPages uint64) {
	t.Helper()
	p := filepath.Join(procfsRoot, strconv.Itoa(pid), "stat")
	if err := os.WriteFile(p, []byte(statFixture(pid, comm, 1, start, utime, stime)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func statFixture(pid int, comm string, ppid int, start, utime, stime uint64) string {
	return strings.Join([]string{
		strconv.Itoa(pid), "(" + comm + ")", "S", strconv.Itoa(ppid), "1", "1", "0", "-1", "4194560",
		"0", "0", "0", "0", strconv.FormatUint(utime, 10), strconv.FormatUint(stime, 10),
		"0", "0", "20", "0", "1", "0", strconv.FormatUint(start, 10), "4096", "1",
	}, " ")
}

func TestPrimeMetricsIsSilentAndSeedsRates(t *testing.T) {
	f := newTestFixture(t, nil)
	if err := f.mod.PrimeMetrics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.metrics) != 0 {
		t.Fatalf("prime emitted metrics: %+v", f.em.metrics)
	}
	f.clock.Advance(10 * time.Second)
	writeStatFixture(t, f.procfs, 100, "nginx", 75, 10, 500, 900)
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	readings := readingsByPID(t, f.em.metrics)
	util, ok := metricValues(readings[100])["process.cpu.utilization"]
	if !ok {
		t.Fatal("primed second sample did not include process.cpu.utilization")
	}
	if util < 0.0249 || util > 0.0251 {
		t.Fatalf("utilization = %v, want 0.025", util)
	}
}
