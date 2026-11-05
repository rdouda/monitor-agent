// Package config loads and validates /etc/agent/agent.yaml.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the complete agent configuration.
type Config struct {
	Agent     AgentConfig     `yaml:"agent"`
	Paths     PathsConfig     `yaml:"paths"`
	Server    ServerConfig    `yaml:"server"`
	Modules   ModulesConfig   `yaml:"modules"`
	Reporting ReportingConfig `yaml:"reporting"`
	Limits    LimitsConfig    `yaml:"limits"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// AgentConfig holds identity metadata sent with every event.
type AgentConfig struct {
	Environment string            `yaml:"environment"`
	Tags        map[string]string `yaml:"tags"`
}

// PathsConfig redirects all host reads. Collectors must use these roots and
// never a hardcoded absolute path.
type PathsConfig struct {
	StateDir   string `yaml:"state_dir"`
	ProcfsRoot string `yaml:"procfs_root"`
	SysfsRoot  string `yaml:"sysfs_root"`
	HostfsRoot string `yaml:"hostfs_root"`
}

// ServerConfig describes the ingestion API and credentials.
type ServerConfig struct {
	BaseURL             string        `yaml:"base_url"`
	EventsEndpoint      string        `yaml:"events_endpoint"`
	EnrollEndpoint      string        `yaml:"enroll_endpoint"`
	EnrollmentTokenFile string        `yaml:"enrollment_token_file"`
	CredentialFile      string        `yaml:"credential_file"`
	Timeout             time.Duration `yaml:"timeout"`
	TLS                 TLSConfig     `yaml:"tls"`
}

// TLSConfig holds optional TLS material settings.
type TLSConfig struct {
	CAFile         string `yaml:"ca_file"`
	ClientCertFile string `yaml:"client_cert_file"`
	ClientKeyFile  string `yaml:"client_key_file"`
	MinVersion     string `yaml:"min_version"`
}

// ModulesConfig groups the four modules.
type ModulesConfig struct {
	Host       HostModuleConfig       `yaml:"host"`
	Process    ProcessModuleConfig    `yaml:"process"`
	Containers ContainersModuleConfig `yaml:"containers"`
	Network    NetworkModuleConfig    `yaml:"network"`
}

// HostModuleConfig configures host inventory and metrics.
type HostModuleConfig struct {
	InventoryInterval    time.Duration `yaml:"inventory_interval"`
	MetricsInterval      time.Duration `yaml:"metrics_interval"`
	DiskUsageInterval    time.Duration `yaml:"disk_usage_interval"`
	ExcludeFSTypes       []string      `yaml:"exclude_fs_types"`
	ExcludeMountPrefixes []string      `yaml:"exclude_mount_prefixes"`
	ExcludeDevices       []string      `yaml:"exclude_devices"`
}

// ProcessModuleConfig configures process inventory, lifecycle and metrics.
type ProcessModuleConfig struct {
	Enabled              bool          `yaml:"enabled"`
	InventoryInterval    time.Duration `yaml:"inventory_interval"`
	LifecycleInterval    time.Duration `yaml:"lifecycle_interval"`
	MetricsInterval      time.Duration `yaml:"metrics_interval"`
	MetricsMode          string        `yaml:"metrics_mode"`
	TopN                 int           `yaml:"top_n"`
	IncludeNames         []string      `yaml:"include_names"`
	ExcludeNames         []string      `yaml:"exclude_names"`
	IncludeKernelThreads bool          `yaml:"include_kernel_threads"`
	ResolveUserNames     bool          `yaml:"resolve_user_names"`
	CollectCommandLine   bool          `yaml:"collect_command_line"`
	RedactPatterns       []string      `yaml:"redact_patterns"`
}

// RuntimeEndpoint is an explicitly configured container runtime endpoint.
type RuntimeEndpoint struct {
	Type     string `yaml:"type"`
	Endpoint string `yaml:"endpoint"`
}

// ContainersModuleConfig configures runtime discovery and collection.
type ContainersModuleConfig struct {
	Enabled               bool              `yaml:"enabled"`
	Autodiscovery         bool              `yaml:"autodiscovery"`
	Runtimes              []RuntimeEndpoint `yaml:"runtimes"`
	InventoryInterval     time.Duration     `yaml:"inventory_interval"`
	MetricsInterval       time.Duration     `yaml:"metrics_interval"`
	ImagesInterval        time.Duration     `yaml:"images_interval"`
	StatsConcurrency      int               `yaml:"stats_concurrency"`
	CollectImages         bool              `yaml:"collect_images"`
	IncludeUntaggedImages bool              `yaml:"include_untagged_images"`
	CollectNetworks       bool              `yaml:"collect_networks"`
	CollectVolumes        bool              `yaml:"collect_volumes"`
	CollectEvents         bool              `yaml:"collect_events"`
	CollectMountSources   bool              `yaml:"collect_mount_sources"`
	MapVethInterfaces     bool              `yaml:"map_veth_interfaces"`
	LabelAllowlist        []string          `yaml:"label_allowlist"`
}

// NetworkModuleConfig configures network discovery.
type NetworkModuleConfig struct {
	Enabled                    bool          `yaml:"enabled"`
	InventoryInterval          time.Duration `yaml:"inventory_interval"`
	MetricsInterval            time.Duration `yaml:"metrics_interval"`
	SocketsInterval            time.Duration `yaml:"sockets_interval"`
	CollectListeningPorts      bool          `yaml:"collect_listening_ports"`
	CollectContainerNamespaces bool          `yaml:"collect_container_namespaces"`
	CollectMACAddresses        bool          `yaml:"collect_mac_addresses"`
	IncludeLocalRoutes         bool          `yaml:"include_local_routes"`
	InterfaceMetricsExclude    []string      `yaml:"interface_metrics_exclude"`
	FDWalkBudget               time.Duration `yaml:"fd_walk_budget"`
	MaxNetns                   int           `yaml:"max_netns"`
}

// ReportingConfig configures delivery and the disk spool.
type ReportingConfig struct {
	Sink              string        `yaml:"sink"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
	MaxEventsPerBatch int           `yaml:"max_events_per_batch"`
	MaxBatchBytes     int           `yaml:"max_batch_bytes"`
	MaxEventBytes     int           `yaml:"max_event_bytes"`
	Compress          string        `yaml:"compress"`
	Retry             RetryConfig   `yaml:"retry"`
	Spool             SpoolConfig   `yaml:"spool"`
	ResyncAfterOutage time.Duration `yaml:"resync_after_outage"`
}

// RetryConfig configures exponential backoff.
type RetryConfig struct {
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
}

// SpoolConfig configures the bounded disk queue.
type SpoolConfig struct {
	Directory     string `yaml:"directory"`
	MaxSizeMB     int    `yaml:"max_size_mb"`
	SegmentSizeMB int    `yaml:"segment_size_mb"`
}

// LimitsConfig configures per-scan and per-minute caps.
type LimitsConfig struct {
	MaxEntitiesPerKind          int `yaml:"max_entities_per_kind"`
	MaxLifecycleEventsPerMinute int `yaml:"max_lifecycle_events_per_minute"`
}

// LoggingConfig configures slog.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	File   string `yaml:"file"`
}

// Default returns the documented default configuration.
func Default() *Config {
	return &Config{
		Agent: AgentConfig{
			Environment: "production",
			Tags:        map[string]string{},
		},
		Paths: PathsConfig{
			StateDir:   "/var/lib/agent",
			ProcfsRoot: "/proc",
			SysfsRoot:  "/sys",
			HostfsRoot: "/",
		},
		Server: ServerConfig{
			BaseURL:             "https://ingest.example.com",
			EventsEndpoint:      "/api/v1/agents/events",
			EnrollEndpoint:      "/api/v1/agents/enroll",
			EnrollmentTokenFile: "/etc/agent/enrollment-token",
			CredentialFile:      "/var/lib/agent/credential",
			Timeout:             10 * time.Second,
			TLS: TLSConfig{
				MinVersion: "1.2",
			},
		},
		Modules: ModulesConfig{
			Host: HostModuleConfig{
				InventoryInterval: 10 * time.Minute,
				MetricsInterval:   5 * time.Second,
				DiskUsageInterval: 30 * time.Second,
				ExcludeFSTypes: []string{
					"proc", "sysfs", "devtmpfs", "devpts", "tmpfs", "cgroup", "cgroup2",
					"overlay", "squashfs", "autofs", "mqueue", "hugetlbfs", "debugfs",
					"tracefs", "securityfs", "pstore", "bpf", "configfs", "fusectl", "nsfs", "ramfs",
					"rpc_pipefs", "nfsd", "binfmt_misc", "selinuxfs", "efivarfs",
				},
				ExcludeMountPrefixes: []string{
					"/proc", "/sys", "/dev", "/run", "/var/lib/docker", "/var/lib/containers", "/snap",
				},
				ExcludeDevices: []string{`^(loop|ram|fd|sr|zram)\d+`},
			},
			Process: ProcessModuleConfig{
				Enabled:           true,
				InventoryInterval: time.Minute,
				LifecycleInterval: 5 * time.Second,
				MetricsInterval:   10 * time.Second,
				MetricsMode:       "top_n",
				TopN:              50,
				ResolveUserNames:  true,
				RedactPatterns: []string{
					`(?i)(password|passwd|secret|token|apikey|api-key|authorization)=\S+`,
					`(?i)--(password|token|secret)\s+\S+`,
				},
			},
			Containers: ContainersModuleConfig{
				Enabled:           true,
				Autodiscovery:     true,
				InventoryInterval: time.Minute,
				MetricsInterval:   15 * time.Second,
				ImagesInterval:    5 * time.Minute,
				StatsConcurrency:  8,
				CollectImages:     true,
				CollectNetworks:   true,
				CollectVolumes:    true,
				CollectEvents:     true,
				MapVethInterfaces: true,
				LabelAllowlist: []string{
					"agent.*",
					"com.docker.compose.project",
					"com.docker.compose.service",
					"io.podman.compose.project",
				},
			},
			Network: NetworkModuleConfig{
				Enabled:                    true,
				InventoryInterval:          time.Minute,
				MetricsInterval:            10 * time.Second,
				SocketsInterval:            30 * time.Second,
				CollectListeningPorts:      true,
				CollectContainerNamespaces: true,
				InterfaceMetricsExclude:    []string{"veth*"},
				FDWalkBudget:               2 * time.Second,
				MaxNetns:                   256,
			},
		},
		Reporting: ReportingConfig{
			Sink:              "http",
			FlushInterval:     time.Second,
			MaxEventsPerBatch: 100,
			MaxBatchBytes:     262144,
			MaxEventBytes:     65536,
			Compress:          "gzip",
			Retry: RetryConfig{
				InitialInterval: time.Second,
				MaxInterval:     time.Minute,
			},
			Spool: SpoolConfig{
				Directory:     "/var/lib/agent/spool",
				MaxSizeMB:     100,
				SegmentSizeMB: 4,
			},
			ResyncAfterOutage: time.Minute,
		},
		Limits: LimitsConfig{
			MaxEntitiesPerKind:          20000,
			MaxLifecycleEventsPerMinute: 600,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			File:   "",
		},
	}
}

// Load reads path on top of the documented defaults and validates the result.
func Load(path string) (*Config, error) {
	return load(path, false)
}

// LoadOnce loads a configuration for `agent once`. The reporting sink is
// forced to stdout before validation, so a development config that points at
// an HTTP ingestion URL is still usable for read-only local scans.
func LoadOnce(path string) (*Config, error) {
	return load(path, true)
}

func load(path string, forceStdout bool) (*Config, error) {
	cfg := Default()
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("parse %s: multiple YAML documents are not allowed", path)
		}
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if forceStdout {
		cfg.Reporting.Sink = "stdout"
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
