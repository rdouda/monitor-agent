package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/identity"
	"agent/internal/schema"
)

func fixtureConfig(t *testing.T, stateDir string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "agent.yaml")
	content := `
agent:
  environment: test
paths:
  state_dir: ` + stateDir + `
  procfs_root: ` + filepath.Join(root, "procfs") + `
  sysfs_root: ` + filepath.Join(root, "sysfs") + `
  hostfs_root: ` + filepath.Join(root, "hostfs") + `
server:
  base_url: "https://ingest.example.com"
reporting:
  sink: stdout
logging:
  level: error
  format: json
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

type onceResult struct {
	events []map[string]any
	raw    [][]byte
}

func runOnce(t *testing.T, configPath string, extra ...string) (int, onceResult) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	args := append([]string{"once", "--config", configPath}, extra...)
	code := run(args)
	os.Stdout = old
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()

	var out onceResult
	s := bufio.NewScanner(bytes.NewReader(raw))
	s.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("invalid JSON line %q: %v", line, err)
		}
		out.events = append(out.events, m)
		out.raw = append(out.raw, line)
	}
	return code, out
}

func TestOnceEndToEnd(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	cfgPath := fixtureConfig(t, state)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identity.LoadOrCreate(cfg.Paths.StateDir, cfg.Paths.HostfsRoot, cfg.Paths.SysfsRoot); err != nil {
		t.Fatal(err)
	}
	seqPath := filepath.Join(state, "seq")
	if err := os.WriteFile(seqPath, []byte("5000\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, first := runOnce(t, cfgPath, "--modules", "host,process", "--sample-wait", "100ms")
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if len(first.events) < 3 {
		t.Fatalf("only %d events", len(first.events))
	}
	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, raw := range first.raw {
		if err := set.ValidateEnvelope(raw); err != nil {
			t.Fatalf("event %d invalid: %v\n%s", i, err, raw)
		}
	}
	types := map[string]bool{}
	metrics := map[string]bool{}
	var lastSeq float64
	var agentID, hostID, runID string
	for _, e := range first.events {
		types[e["type"].(string)] = true
		seq := e["agentseq"].(float64)
		if seq <= lastSeq {
			t.Fatalf("agentseq not increasing: %v after %v", seq, lastSeq)
		}
		lastSeq = seq
		agentID = e["agentid"].(string)
		runID = e["agentrun"].(string)
		hostID = e["subject"].(string)
		if e["type"] == "agent.metric.batch.v1" {
			for _, r := range e["data"].(map[string]any)["readings"].([]any) {
				for _, m := range r.(map[string]any)["metrics"].([]any) {
					metrics[m.(map[string]any)["name"].(string)] = true
				}
			}
		}
	}
	if lastSeq != float64(len(first.events)) {
		t.Fatalf("agentseq = %v, want a fresh in-memory counter ending at %d", lastSeq, len(first.events))
	}
	if first.events[0]["agentseq"].(float64) != 1 {
		t.Fatalf("first agentseq = %v, want 1", first.events[0]["agentseq"])
	}
	for _, want := range []string{"agent.entity.upsert.v1", "agent.snapshot.completed.v1", "agent.metric.batch.v1"} {
		if !types[want] {
			t.Errorf("missing event type %s", want)
		}
	}
	for _, unwanted := range []string{"agent.heartbeat.v1", "agent.entity.lifecycle.changed.v1", "agent.relationship.removed.v1"} {
		if types[unwanted] {
			t.Errorf("once must not emit %s", unwanted)
		}
	}
	// --modules limits the scan.
	for _, e := range first.events {
		if e["type"] == "agent.snapshot.completed.v1" {
			module := e["data"].(map[string]any)["module"].(string)
			if module != "host" && module != "process" {
				t.Fatalf("unexpected module snapshot %q", module)
			}
		}
	}
	if raw, err := os.ReadFile(seqPath); err != nil || string(raw) != "5000\n" {
		t.Fatalf("once modified the seq file: %q, %v", raw, err)
	}

	code, second := runOnce(t, cfgPath, "--modules", "host,process", "--sample-wait", "100ms")
	if code != 0 {
		t.Fatalf("second run exit code %d", code)
	}
	for i, raw := range second.raw {
		if err := set.ValidateEnvelope(raw); err != nil {
			t.Fatalf("second run event %d invalid: %v", i, err)
		}
	}
	if len(second.events) == 0 {
		t.Fatal("second run emitted no events")
	}
	if second.events[0]["agentid"] != agentID {
		t.Fatal("agent_id changed across runs with a stored identity")
	}
	if second.events[0]["agentrun"] == runID {
		t.Fatal("agent_run_id must change across runs")
	}
	if second.events[0]["subject"] != hostID {
		t.Fatal("host_id changed across runs")
	}
	if second.events[0]["agentseq"].(float64) != 1 {
		t.Fatal("agentseq must restart at 1 in once mode")
	}
}

func TestOnceSampleWaitProducesHostCPUMetrics(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	cfgPath := filepath.Join(t.TempDir(), "agent.yaml")
	content := `
agent:
  environment: test
paths:
  state_dir: ` + state + `
  procfs_root: /proc
  sysfs_root: /sys
  hostfs_root: /
reporting:
  sink: stdout
logging:
  level: error
  format: json
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out := runOnce(t, cfgPath, "--modules", "host", "--sample-wait", "250ms")
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	metrics := map[string]bool{}
	for _, e := range out.events {
		if e["type"] != "agent.metric.batch.v1" {
			continue
		}
		for _, r := range e["data"].(map[string]any)["readings"].([]any) {
			for _, m := range r.(map[string]any)["metrics"].([]any) {
				metrics[m.(map[string]any)["name"].(string)] = true
			}
		}
	}
	for _, want := range []string{"host.cpu.utilization", "host.cpu.iowait", "host.cpu.steal"} {
		if !metrics[want] {
			t.Errorf("missing metric %s (have %v)", want, metrics)
		}
	}
}

func TestOnceUsesEphemeralIdentityAndWritesNoState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	cfgPath := fixtureConfig(t, state)
	code, first := runOnce(t, cfgPath, "--modules", "host", "--sample-wait", "50ms")
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	code, second := runOnce(t, cfgPath, "--modules", "host", "--sample-wait", "50ms")
	if code != 0 {
		t.Fatalf("second exit code %d", code)
	}
	if first.events[0]["agentid"] == second.events[0]["agentid"] {
		t.Fatal("ephemeral identity should differ between invocations")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("once created state directory: %v", err)
	}
}

func TestOnceRejectsUnknownModule(t *testing.T) {
	cfgPath := fixtureConfig(t, filepath.Join(t.TempDir(), "state"))
	if code := run([]string{"once", "--config", cfgPath, "--modules", "nope"}); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestValidateConfigCommand(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	cfgPath := fixtureConfig(t, state)
	if code := run([]string{"validate-config", "--config", cfgPath}); code != 0 {
		t.Fatalf("validate-config exit = %d", code)
	}
	if code := run([]string{"validate-config", "--config", filepath.Join(t.TempDir(), "missing.yaml")}); code != 2 {
		t.Fatalf("missing config exit = %d, want 2", code)
	}
}

func TestVersionCommand(t *testing.T) {
	if code := run([]string{"version"}); code != 0 {
		t.Fatalf("version exit = %d", code)
	}
}

func reloadFixtureConfig(t *testing.T, state, procfs, extra string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	if procfs == "" {
		procfs = filepath.Join(root, "procfs")
	}
	p := filepath.Join(t.TempDir(), "agent.yaml")
	content := `
agent:
  environment: test
paths:
  state_dir: ` + state + `
  procfs_root: ` + procfs + `
  sysfs_root: ` + filepath.Join(root, "sysfs") + `
  hostfs_root: ` + filepath.Join(root, "hostfs") + `
server:
  base_url: "https://ingest.example.com"
reporting:
  sink: stdout
logging:
  level: error
  format: json
` + extra
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWatchReloadsHotConfig(t *testing.T) {
	cfgPath := reloadFixtureConfig(t, t.TempDir(), "", "modules:\n  host:\n    metrics_interval: 3s\n")
	base, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hup := make(chan os.Signal, 1)
	canceled := make(chan struct{}, 1)
	reloads := watchReloads(ctx, cfgPath, base, hup, setupLogger(base, io.Discard), func() { canceled <- struct{}{} })
	hup <- syscall.SIGHUP

	select {
	case got := <-reloads:
		if got.Modules.Host.MetricsInterval != 3*time.Second {
			t.Fatalf("reloaded interval = %v", got.Modules.Host.MetricsInterval)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("reload did not cancel the old manager")
	}
}

func TestWatchReloadsRejectsRestartChanges(t *testing.T) {
	cfgPath := reloadFixtureConfig(t, t.TempDir(), "/host/proc", "")
	base := config.Default()
	base.Paths.ProcfsRoot = "/proc"
	ctx, cancel := context.WithCancel(context.Background())
	hup := make(chan os.Signal, 1)
	reloads := watchReloads(ctx, cfgPath, base, hup, setupLogger(base, io.Discard), func() {})
	hup <- syscall.SIGHUP

	select {
	case next := <-reloads:
		t.Fatalf("restart-required change was applied: %+v", next.Paths)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
}
