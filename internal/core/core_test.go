package core

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/events"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestSchedulerRunsImmediatelyAndStops(t *testing.T) {
	s := NewScheduler(testLogger())
	var n atomic.Int64
	s.Add(Task{Name: "t", Interval: time.Hour, Timeout: time.Second, Run: func(context.Context) error {
		n.Add(1)
		return nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { return n.Load() == 1 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not stop")
	}
}

func TestSchedulerRecoversPanics(t *testing.T) {
	s := NewScheduler(testLogger())
	var n atomic.Int64
	s.Add(Task{Name: "panic", Interval: time.Hour, Timeout: time.Second, Run: func(context.Context) error {
		n.Add(1)
		panic("boom")
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	waitFor(t, func() bool { return n.Load() == 1 })
	// A panic must not take down the process or the task loop.
	time.Sleep(50 * time.Millisecond)
}

func TestSchedulerTimeout(t *testing.T) {
	s := NewScheduler(testLogger())
	got := make(chan struct{}, 1)
	s.Add(Task{Name: "slow", Interval: time.Hour, Timeout: 20 * time.Millisecond, Run: func(ctx context.Context) error {
		<-ctx.Done()
		got <- struct{}{}
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx) }()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("task context was not canceled by the timeout")
	}
}

type fakeModule struct {
	name      string
	initErr   error
	runPanic  bool
	runBlock  bool
	initDelay time.Duration
	health    ModuleHealth
	runCount  atomic.Int64
	initCount atomic.Int64
}

func (f *fakeModule) Name() string { return f.name }
func (f *fakeModule) Init(ctx context.Context, _ Deps) error {
	f.initCount.Add(1)
	if f.initDelay > 0 {
		time.Sleep(f.initDelay)
	}
	return f.initErr
}
func (f *fakeModule) Run(ctx context.Context) error {
	f.runCount.Add(1)
	if f.runPanic {
		panic("module boom")
	}
	if f.runBlock {
		<-ctx.Done()
	}
	return nil
}
func (f *fakeModule) Health() ModuleHealth {
	if f.health.Status == "" {
		return ModuleHealth{Status: ModuleHealthy}
	}
	return f.health
}

func TestSupervisorIsolationAndPanic(t *testing.T) {
	healthy := &fakeModule{name: "host", runBlock: true}
	bad := &fakeModule{name: "process", runPanic: true}
	log := testLogger()
	sup := NewSupervisor([]Module{healthy, bad}, Deps{}, log)
	ctx, cancel := context.WithCancel(context.Background())
	sup.Start(ctx)

	waitFor(t, func() bool { return healthy.runCount.Load() == 1 })
	waitFor(t, func() bool { return bad.runCount.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)

	h := sup.Health()
	if h["host"].Status != ModuleHealthy {
		t.Fatalf("healthy module status = %s", h["host"].Status)
	}
	if h["process"].Status != ModuleFailed {
		t.Fatalf("panicking module status = %s", h["process"].Status)
	}
	if healthy.runCount.Load() != 1 {
		t.Fatal("healthy module must not be restarted")
	}
	cancel()
	done := make(chan struct{})
	go func() { sup.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}

func TestSupervisorUnavailable(t *testing.T) {
	m := &fakeModule{name: "containers", initErr: ErrUnavailable{Reason: "no docker"}}
	sup := NewSupervisor([]Module{m}, Deps{}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	sup.Start(ctx)
	waitFor(t, func() bool { return sup.Health()["containers"].Status == ModuleUnavailable })
	if m.runCount.Load() != 0 {
		t.Fatal("unavailable module must not run")
	}
	cancel()
	sup.Wait()
}

func TestSupervisorInitFailureBackoff(t *testing.T) {
	m := &fakeModule{name: "network", initErr: errors.New("boom")}
	sup := NewSupervisor([]Module{m}, Deps{}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	sup.Start(ctx)
	waitFor(t, func() bool { return sup.Health()["network"].Status == ModuleFailed })
	cancel()
	sup.Wait()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

type testSink struct{}

func (testSink) Send(*events.Envelope) error { return nil }
func (testSink) Close() error                { return nil }

type testSeq struct{ n uint64 }

func (s *testSeq) Next() (uint64, error) { s.n++; return s.n, nil }

type onceModule struct {
	name       string
	deps       Deps
	incomplete bool
	prime      int
	runOnce    int
	runAt      time.Time
}

func (o *onceModule) Name() string { return o.name }
func (o *onceModule) Init(_ context.Context, d Deps) error {
	o.deps = d
	return nil
}
func (o *onceModule) Run(context.Context) error { return nil }
func (o *onceModule) Health() ModuleHealth      { return ModuleHealth{Status: ModuleHealthy} }
func (o *onceModule) PrimeMetrics(context.Context) error {
	o.prime++
	return nil
}
func (o *onceModule) RunOnce(context.Context) error {
	o.runOnce++
	o.runAt = time.Now()
	o.deps.Emitter.SnapshotCompleted(events.Snapshot{
		Module: o.name, SnapshotID: "0192f3a4-8000-7000-8000-000000000001",
		ScopeEntityID: "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
		CoversKinds:   []string{"host"}, Complete: !o.incomplete,
		StartedAt: events.Now(), CompletedAt: events.Now(),
	})
	return nil
}

func TestRunOncePrimesAndRejectsIncompleteSnapshots(t *testing.T) {
	mod := &onceModule{name: "host", incomplete: true}
	m, err := NewManager(Options{
		Config: config.Default(),
		Identity: Identity{
			AgentID:    "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
			HostID:     "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
			AgentRunID: "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		},
		Seq:           &testSeq{},
		Sink:          testSink{},
		Logger:        testLogger(),
		SampleWait:    40 * time.Millisecond,
		ModuleFactory: func(Deps) ([]Module, error) { return []Module{mod}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = m.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "snapshot incomplete") {
		t.Fatalf("err = %v", err)
	}
	if mod.prime != 1 || mod.runOnce != 1 {
		t.Fatalf("prime=%d runOnce=%d", mod.prime, mod.runOnce)
	}
	if elapsed := mod.runAt.Sub(start); elapsed < 40*time.Millisecond {
		t.Fatalf("second sample ran after %v, want >= 40ms", elapsed)
	}
}
