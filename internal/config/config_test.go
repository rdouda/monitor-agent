package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
}

func TestLoadFullFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "agent.yaml")
	content := `
agent:
  environment: staging
  tags:
    team: infrastructure
paths:
  state_dir: /var/lib/agent
  procfs_root: /proc
  sysfs_root: /sys
  hostfs_root: /
server:
  base_url: "https://ingest.example.com"
  events_endpoint: "/api/v1/agents/events"
  enroll_endpoint: "/api/v1/agents/enroll"
  enrollment_token_file: "/etc/agent/enrollment-token"
  credential_file: "/var/lib/agent/credential"
  timeout: 10s
  tls:
    ca_file: ""
    client_cert_file: ""
    client_key_file: ""
    min_version: "1.2"
modules:
  host:
    inventory_interval: 10m
    metrics_interval: 5s
    disk_usage_interval: 30s
    exclude_fs_types: [proc, sysfs, tmpfs]
    exclude_mount_prefixes: [/proc, /sys]
    exclude_devices: ['^(loop|ram)\d+']
  process:
    enabled: true
    inventory_interval: 60s
    lifecycle_interval: 5s
    metrics_interval: 10s
    metrics_mode: top_n
    top_n: 50
    include_names: []
    exclude_names: []
    include_kernel_threads: false
    resolve_user_names: true
    collect_command_line: false
    redact_patterns: ['(?i)password=\S+']
  containers:
    enabled: true
    autodiscovery: true
    runtimes: []
    inventory_interval: 60s
    metrics_interval: 15s
    images_interval: 5m
    stats_concurrency: 8
    collect_images: true
    collect_networks: true
    collect_volumes: true
    collect_events: true
    collect_mount_sources: false
    map_veth_interfaces: true
    label_allowlist: ["agent.*"]
  network:
    enabled: true
    inventory_interval: 60s
    metrics_interval: 10s
    sockets_interval: 30s
    collect_listening_ports: true
    collect_container_namespaces: true
    collect_mac_addresses: false
    include_local_routes: false
    interface_metrics_exclude: ["veth*"]
    fd_walk_budget: 2s
    max_netns: 256
reporting:
  sink: http
  flush_interval: 1s
  max_events_per_batch: 100
  max_batch_bytes: 262144
  max_event_bytes: 65536
  compress: gzip
  retry:
    initial_interval: 1s
    max_interval: 60s
  spool:
    directory: "/var/lib/agent/spool"
    max_size_mb: 100
    segment_size_mb: 4
  resync_after_outage: 60s
limits:
  max_entities_per_kind: 20000
  max_lifecycle_events_per_minute: 600
logging:
  level: info
  format: json
  file: ""
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Agent.Environment != "staging" {
		t.Errorf("environment = %q", cfg.Agent.Environment)
	}
	if cfg.Modules.Process.InventoryInterval != time.Minute {
		t.Errorf("inventory interval = %v", cfg.Modules.Process.InventoryInterval)
	}
	if cfg.Modules.Containers.LabelAllowlist[0] != "agent.*" {
		t.Errorf("label allowlist = %v", cfg.Modules.Containers.LabelAllowlist)
	}
}

func TestUnknownKeysRejected(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "agent.yaml")
	if err := os.WriteFile(p, []byte("agent:\n  environment: production\n  bogus: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(p)
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestHostCannotBeDisabled(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "agent.yaml")
	if err := os.WriteFile(p, []byte("modules:\n  host:\n    enabled: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(p)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "enabled") {
		t.Fatalf("expected enabled to be rejected, got %v", err)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"short interval", func(c *Config) { c.Modules.Host.MetricsInterval = 500 * time.Millisecond }, "at least 1s"},
		{"http base url", func(c *Config) { c.Server.BaseURL = "http://insecure.example.com" }, "https"},
		{"bad sink", func(c *Config) { c.Reporting.Sink = "kafka" }, "sink"},
		{"bad mode", func(c *Config) { c.Modules.Process.MetricsMode = "some" }, "metrics_mode"},
		{"bad regex", func(c *Config) { c.Modules.Host.ExcludeDevices = []string{"["} }, "invalid regex"},
		{"bad redact", func(c *Config) { c.Modules.Process.RedactPatterns = []string{"("} }, "invalid regex"},
		{"bad glob", func(c *Config) { c.Modules.Process.IncludeNames = []string{"["} }, "invalid glob"},
		{"bad runtime", func(c *Config) { c.Modules.Containers.Runtimes = []RuntimeEndpoint{{Type: "cri", Endpoint: "/x"}} }, "docker or podman"},
		{"segment too big", func(c *Config) { c.Reporting.Spool.SegmentSizeMB = 200 }, "segment_size_mb"},
		{"empty environment", func(c *Config) { c.Agent.Environment = "" }, "environment"},
		{"relative state", func(c *Config) { c.Paths.StateDir = "var/lib/agent" }, "absolute"},
		{"bad tls", func(c *Config) { c.Server.TLS.MinVersion = "1.1" }, "min_version"},
		{"event larger than batch", func(c *Config) {
			c.Reporting.MaxEventBytes = 4096
			c.Reporting.MaxBatchBytes = 2048
		}, "max_event_bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			tt.mut(c)
			err := c.Validate()
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.want)) {
				t.Fatalf("got error %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFileSecurity(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "credential")
	if err := os.WriteFile(p, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CheckFileSecurity(p); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("expected mode error, got %v", err)
	}
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckFileSecurity(p); err != nil {
		t.Fatalf("0600 file rejected: %v", err)
	}
	if err := CheckFileSecurity(filepath.Join(dir, "missing")); err != nil {
		t.Fatalf("missing file must be accepted: %v", err)
	}
}

func TestStartupSecretsRequiresCredentialOrToken(t *testing.T) {
	c := Default()
	c.Server.CredentialFile = filepath.Join(t.TempDir(), "credential")
	c.Server.EnrollmentTokenFile = filepath.Join(t.TempDir(), "token")
	if err := c.CheckStartupSecrets(); err == nil {
		t.Fatal("expected error when neither secret exists")
	}
	if err := os.WriteFile(c.Server.CredentialFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckStartupSecrets(); err != nil {
		t.Fatalf("credential should suffice: %v", err)
	}
}

func TestRestartReasons(t *testing.T) {
	a := Default()
	b := Default()
	if got := a.RestartReasons(b); len(got) != 0 {
		t.Fatalf("identical configs: %v", got)
	}
	b.Modules.Host.MetricsInterval = 3 * time.Second
	if got := a.RestartReasons(b); len(got) != 0 {
		t.Fatalf("interval change should be hot-reloadable: %v", got)
	}
	b.Paths.ProcfsRoot = "/host/proc"
	if got := a.RestartReasons(b); len(got) == 0 || got[0] != "paths" {
		t.Fatalf("path change should require restart: %v", got)
	}
}

func TestLoadOnceForcesStdoutSink(t *testing.T) {
	p := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(p, []byte(`
agent:
  environment: test
server:
  base_url: "http://127.0.0.1:8099"
reporting:
  sink: http
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("Load should reject an http sink with an http base URL")
	}
	cfg, err := LoadOnce(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Reporting.Sink != "stdout" {
		t.Fatalf("sink = %q, want stdout", cfg.Reporting.Sink)
	}
}
