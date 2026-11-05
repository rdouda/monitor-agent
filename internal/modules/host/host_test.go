package host

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/core"
	"agent/internal/events"
)

type captureEmitter struct {
	mu         sync.Mutex
	entities   []events.Entity
	rels       []events.Relationship
	removed    []string
	metrics    []events.Reading
	observedAt time.Time
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
func (c *captureEmitter) Metrics(at time.Time, r []events.Reading) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observedAt = at
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

type fixture struct {
	root   string
	procfs string
	sysfs  string
	hostfs string
	cfg    *config.Config
	em     *captureEmitter
	mod    *Module
	now    time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join("..", "..", "..", "testdata"), root)
	if err := os.MkdirAll(filepath.Join(root, "hostfs", "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Agent.Environment = "test"
	cfg.Agent.Tags = map[string]string{"team": "infra"}
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.SysfsRoot = filepath.Join(root, "sysfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	now := time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)
	em := &captureEmitter{}
	mod := New()
	deps := core.Deps{
		Config:   cfg,
		Identity: core.Identity{AgentID: "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55", HostID: "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03", AgentRunID: "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f"},
		Emitter:  em,
		Paths:    core.Paths{ProcfsRoot: cfg.Paths.ProcfsRoot, SysfsRoot: cfg.Paths.SysfsRoot, HostfsRoot: cfg.Paths.HostfsRoot},
		Clock:    fixedClock{now},
		Version:  "test",
	}
	if err := mod.Init(context.Background(), deps); err != nil {
		t.Fatal(err)
	}
	return &fixture{root: root, procfs: cfg.Paths.ProcfsRoot, sysfs: cfg.Paths.SysfsRoot, hostfs: cfg.Paths.HostfsRoot, cfg: cfg, em: em, mod: mod, now: now}
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestInventoryAndMetrics(t *testing.T) {
	f := newFixture(t)
	if err := f.mod.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.entities) != 1 {
		t.Fatalf("got %d entities", len(f.em.entities))
	}
	e := f.em.entities[0]
	if e.Kind != "host" || e.Name != mustHostname(t) {
		t.Fatalf("unexpected entity: %+v", e)
	}
	if e.Labels["team"] != "infra" {
		t.Fatalf("labels: %v", e.Labels)
	}
	attrs := e.Attributes
	if attrs["distribution"] != "ubuntu" || attrs["os_version"] != "24.04" {
		t.Fatalf("os attributes: %v", attrs)
	}
	if attrs["distribution_name"] != "Ubuntu 24.04.1 LTS" {
		t.Fatalf("pretty name: %v", attrs["distribution_name"])
	}
	if attrs["boot_id"] != "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55" {
		t.Fatalf("boot id: %v", attrs["boot_id"])
	}
	if attrs["memory_total_bytes"] != uint64(8_000_000*1024) {
		t.Fatalf("memory total: %v", attrs["memory_total_bytes"])
	}
	cpu, ok := attrs["cpu"].(map[string]any)
	if !ok {
		t.Fatalf("cpu attribute missing: %v", attrs["cpu"])
	}
	if cpu["logical_cores"] != 2 || cpu["physical_cores"] != 1 {
		t.Fatalf("cpu: %v", cpu)
	}
	virt, ok := attrs["virtualization"].(map[string]any)
	if !ok || virt["type"] != "vm" || virt["technology"] != "kvm" {
		t.Fatalf("virtualization: %v", attrs["virtualization"])
	}
	fsList, ok := attrs["filesystems"].([]map[string]string)
	if !ok || len(fsList) != 1 {
		t.Fatalf("filesystems: %#v", attrs["filesystems"])
	}
	if fsList[0]["mountpoint"] != "/data" || fsList[0]["device"] != "/dev/sda1" || fsList[0]["fstype"] != "ext4" {
		t.Fatalf("filesystem: %#v", fsList[0])
	}

	if len(f.em.snapshots) != 1 {
		t.Fatalf("snapshots: %d", len(f.em.snapshots))
	}
	s := f.em.snapshots[0]
	if s.Module != "host" || !s.Complete || s.EntitiesCount != 1 || len(s.Errors) != 0 {
		t.Fatalf("snapshot: %+v", s)
	}
	if s.CoversKinds[0] != "host" || s.ScopeEntityID != e.ID {
		t.Fatalf("snapshot scope: %+v", s)
	}

	got := metricNames(f.em.metrics)
	for _, want := range []string{
		"host.uptime", "host.load.1m", "host.memory.used", "host.swap.total",
		"host.cpu.frequency", "host.disk.io.read.bytes", "host.disk.utilization",
		"host.disk.inodes.utilization",
	} {
		if !got[want] {
			t.Errorf("missing metric %s (have %v)", want, sortedNames(got))
		}
	}
	if got["host.cpu.utilization"] {
		t.Error("first CPU sample must be skipped")
	}
	for _, m := range f.em.metrics {
		for _, metric := range m.Metrics {
			if device, ok := metric.Attributes["device"]; ok && device == "ram0" {
				t.Error("ram0 must be excluded")
			}
		}
	}
}

func TestCPUUtilizationSecondSample(t *testing.T) {
	f := newFixture(t)
	if err := f.mod.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Advance the counter: deltas total=960, idle=800, iowait=10, steal=0.
	stat := "cpu  200 10 100 1600 20 2 3 5 0 0\nbtime 1700000000\n"
	if err := os.WriteFile(filepath.Join(f.procfs, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	f.em.metrics = nil
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, r := range f.em.metrics {
		for _, m := range r.Metrics {
			got[m.Name] = m.Value
		}
	}
	util, ok := got["host.cpu.utilization"]
	if !ok {
		t.Fatal("second sample must include host.cpu.utilization")
	}
	if util < 0.15 || util > 0.17 {
		t.Fatalf("utilization = %v, want ~0.15625", util)
	}
	if got["host.cpu.iowait"] < 0.009 || got["host.cpu.iowait"] > 0.011 {
		t.Fatalf("iowait = %v", got["host.cpu.iowait"])
	}
}

func TestMountinfoUnescape(t *testing.T) {
	in := "36 35 8:1 / /mnt/with\\040space rw - ext4 /dev/sda1 rw\n"
	entries, err := parseMountInfo(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Mountpoint != "/mnt/with space" {
		t.Fatalf("entries: %+v", entries)
	}
}

func TestContainerVirtualization(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "run/systemd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run/systemd/container"), []byte("podman\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	v := detectVirtualization(root, "", "")
	if v["type"] != "container" || v["technology"] != "podman" {
		t.Fatalf("virtualization: %v", v)
	}
}

func metricNames(readings []events.Reading) map[string]bool {
	out := map[string]bool{}
	for _, r := range readings {
		for _, m := range r.Metrics {
			out[m.Name] = true
		}
	}
	return out
}

func sortedNames(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func mustHostname(t *testing.T) string {
	t.Helper()
	h, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	if len(h) > 256 {
		h = h[:256]
	}
	return h
}

func FuzzParseMountInfo(f *testing.F) {
	f.Add("36 25 0:30 / / rw,relatime - ext4 /dev/sda1 rw\n")
	f.Add("36 25 0:30 /foo /bar rw - overlay overlay rw\n")
	f.Add("malformed\n")
	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 1<<20 {
			return
		}
		_, _ = parseMountInfo(strings.NewReader(data))
	})
}

func TestPseudoFilesystemsAreOmitted(t *testing.T) {
	f := newFixture(t)
	old := filesystemStatfs
	filesystemStatfs = func(path string, out *statFS) error {
		if strings.HasSuffix(path, "/data") {
			out.Blocks = 0
			return nil
		}
		return statfs(path, out)
	}
	defer func() { filesystemStatfs = old }()

	files, errs := f.mod.collectFilesystems(f.mod.getDeps())
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if len(files) != 0 {
		t.Fatalf("zero-sized filesystem was reported: %+v", files)
	}
	if !excludedFS(mountEntry{FSType: "rpc_pipefs"}, f.cfg.Modules.Host.ExcludeFSTypes, nil) {
		t.Fatal("rpc_pipefs is not excluded by default")
	}
}

func TestPrimeMetricsSeedsCPUSample(t *testing.T) {
	f := newFixture(t)
	if err := f.mod.PrimeMetrics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.em.metrics) != 0 {
		t.Fatalf("prime emitted metrics: %+v", f.em.metrics)
	}
	stat := "cpu  200 10 100 1600 20 2 3 5 0 0\nbtime 1700000000\n"
	if err := os.WriteFile(filepath.Join(f.procfs, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := metricNames(f.em.metrics)
	if !got["host.cpu.utilization"] || !got["host.cpu.iowait"] || !got["host.cpu.steal"] {
		t.Fatalf("missing CPU rate metrics: %v", got)
	}
}
