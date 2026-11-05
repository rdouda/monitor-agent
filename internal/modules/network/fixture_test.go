package network

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
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
	c.entities, c.rels, c.removed, c.metrics, c.snapshots, c.lifecycles = nil, nil, nil, nil, nil, nil
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

type fakeBackend struct {
	mu     sync.Mutex
	links  []Link
	addrs  map[int][]Addr
	routes map[int][]Route
}

func (f *fakeBackend) Links() ([]Link, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Link(nil), f.links...), nil
}
func (f *fakeBackend) Addrs(index int) ([]Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Addr(nil), f.addrs[index]...), nil
}
func (f *fakeBackend) Routes(family int) ([]Route, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Route(nil), f.routes[family]...), nil
}
func (f *fakeBackend) Subscribe(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}
func (f *fakeBackend) set(fn func(*fakeBackend)) {
	f.mu.Lock()
	fn(f)
	f.mu.Unlock()
}

type netFixture struct {
	mod    *Module
	em     *captureEmitter
	clock  *fakeClock
	fake   *fakeBackend
	deps   core.Deps
	procfs string
	sysfs  string
	index  *core.Index
}

func newNetFixture(t *testing.T) *netFixture {
	t.Helper()
	root := t.TempDir()
	procfs := filepath.Join(root, "procfs")
	sysfs := filepath.Join(root, "sysfs")
	hostfs := filepath.Join(root, "hostfs")
	for _, dir := range []string{procfs, sysfs, hostfs, filepath.Join(sysfs, "class", "net", "eth0", "device")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sysfs, "class", "net", "eth0", "speed"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	fake := &fakeBackend{
		links: []Link{
			{Index: 1, Name: "lo", MTU: 65536, Flags: net.FlagUp | net.FlagLoopback, OperState: "unknown", Type: "device"},
			{Index: 2, Name: "eth0", MTU: 1500, Flags: net.FlagUp, OperState: "up", Type: "device", HardwareAddr: "aa:bb:cc:dd:ee:ff", Statistics: &LinkStats{RxBytes: 100, TxBytes: 200, RxPackets: 1, TxPackets: 2}},
			{Index: 3, Name: "br0", MTU: 1500, Flags: net.FlagUp, OperState: "up", Type: "bridge"},
			{Index: 4, Name: "veth123", MTU: 1500, Flags: net.FlagUp, OperState: "up", Type: "veth", MasterIndex: 3, ParentIndex: 2},
		},
		addrs: map[int][]Addr{
			2: {{IP: net.ParseIP("192.0.2.10"), PrefixLen: 24, Scope: 0}, {IP: net.ParseIP("fe80::1"), PrefixLen: 64, Scope: 253}},
		},
		routes: map[int][]Route{},
	}
	fixture := &netFixture{em: &captureEmitter{}, clock: &fakeClock{t: time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)}, fake: fake, procfs: procfs, sysfs: sysfs}
	old := newBackend
	newBackend = func(string) backend { return fake }
	t.Cleanup(func() { newBackend = old })

	cfg := config.Default()
	cfg.Paths.ProcfsRoot = procfs
	cfg.Paths.SysfsRoot = sysfs
	cfg.Paths.HostfsRoot = hostfs
	cfg.Modules.Network.CollectMACAddresses = true
	cfg.Modules.Network.CollectContainerNamespaces = false
	cfg.Modules.Network.InterfaceMetricsExclude = []string{"veth*"}
	index := core.NewIndex()
	deps := core.Deps{
		Config: cfg,
		Identity: core.Identity{
			AgentID:    "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
			HostID:     "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
			AgentRunID: "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		},
		Emitter:   fixture.em,
		Scheduler: core.NewScheduler(slog.New(slog.NewTextHandler(io.Discard, nil))),
		Index:     index,
		Paths:     core.Paths{ProcfsRoot: procfs, SysfsRoot: sysfs, HostfsRoot: hostfs},
		Clock:     fixture.clock,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	mod := New()
	if err := mod.Init(context.Background(), deps); err != nil {
		t.Fatal(err)
	}
	fixture.mod = mod
	fixture.deps = deps
	fixture.index = index
	return fixture
}
