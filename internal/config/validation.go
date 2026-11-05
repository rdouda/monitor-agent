package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// Validate checks every invariant that can be checked without touching runtime
// state. The returned error joins all individual problems.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Agent.Environment == "" {
		add("agent.environment must not be empty")
	}
	if len(c.Agent.Environment) > 64 {
		add("agent.environment must be at most 64 characters")
	}
	if len(c.Agent.Tags) > 64 {
		add("agent.tags must contain at most 64 entries")
	}
	for k, v := range c.Agent.Tags {
		if len(k) > 128 || len(v) > 256 {
			add("agent.tags entry %q is too long", k)
		}
	}

	if !filepath.IsAbs(c.Paths.StateDir) {
		add("paths.state_dir must be an absolute path")
	}
	if !filepath.IsAbs(c.Paths.ProcfsRoot) {
		add("paths.procfs_root must be an absolute path")
	}
	if !filepath.IsAbs(c.Paths.SysfsRoot) {
		add("paths.sysfs_root must be an absolute path")
	}
	if !filepath.IsAbs(c.Paths.HostfsRoot) {
		add("paths.hostfs_root must be an absolute path")
	}

	if _, err := url.ParseRequestURI(c.Server.BaseURL); err != nil {
		add("server.base_url is not a valid URL: %v", err)
	}
	if c.Reporting.Sink == "http" && !strings.HasPrefix(c.Server.BaseURL, "https://") {
		add("server.base_url must use https:// (only sink stdout may use http)")
	}
	if !strings.HasPrefix(c.Server.EventsEndpoint, "/") {
		add("server.events_endpoint must start with /")
	}
	if !strings.HasPrefix(c.Server.EnrollEndpoint, "/") {
		add("server.enroll_endpoint must start with /")
	}
	if c.Server.EnrollmentTokenFile == "" {
		add("server.enrollment_token_file must not be empty")
	}
	if c.Server.CredentialFile == "" {
		add("server.credential_file must not be empty")
	}
	if c.Server.Timeout < time.Second {
		add("server.timeout must be at least 1s")
	}
	switch c.Server.TLS.MinVersion {
	case "1.2", "1.3":
	default:
		add("server.tls.min_version must be 1.2 or 1.3")
	}
	if (c.Server.TLS.ClientCertFile == "") != (c.Server.TLS.ClientKeyFile == "") {
		add("server.tls.client_cert_file and client_key_file must be set together")
	}

	// Host module (cannot be disabled).
	if err := atLeastOneSecond("modules.host.inventory_interval", c.Modules.Host.InventoryInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.host.metrics_interval", c.Modules.Host.MetricsInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.host.disk_usage_interval", c.Modules.Host.DiskUsageInterval); err != nil {
		errs = append(errs, err)
	}
	for _, p := range c.Modules.Host.ExcludeDevices {
		if _, err := regexp.Compile(p); err != nil {
			add("modules.host.exclude_devices: invalid regex %q: %v", p, err)
		}
	}
	for _, p := range c.Modules.Host.ExcludeMountPrefixes {
		if !strings.HasPrefix(p, "/") {
			add("modules.host.exclude_mount_prefixes: %q must be absolute", p)
		}
	}

	p := &c.Modules.Process
	if err := atLeastOneSecond("modules.process.inventory_interval", p.InventoryInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.process.lifecycle_interval", p.LifecycleInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.process.metrics_interval", p.MetricsInterval); err != nil {
		errs = append(errs, err)
	}
	switch p.MetricsMode {
	case "top_n":
		if p.TopN < 1 {
			add("modules.process.top_n must be at least 1 in top_n mode")
		}
	case "all", "allowlist":
	default:
		add("modules.process.metrics_mode must be top_n, all or allowlist")
	}
	if p.MetricsMode == "allowlist" && len(p.IncludeNames) == 0 {
		add("modules.process.include_names must not be empty in allowlist mode")
	}
	for _, name := range []string{"modules.process.include_names", "modules.process.exclude_names"} {
		list := p.IncludeNames
		if strings.Contains(name, "exclude") {
			list = p.ExcludeNames
		}
		for _, glob := range list {
			if _, err := path.Match(glob, ""); err != nil {
				add("%s: invalid glob %q", name, glob)
			}
		}
	}
	for _, pat := range p.RedactPatterns {
		if _, err := regexp.Compile(pat); err != nil {
			add("modules.process.redact_patterns: invalid regex %q: %v", pat, err)
		}
	}

	cm := &c.Modules.Containers
	if err := atLeastOneSecond("modules.containers.inventory_interval", cm.InventoryInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.containers.metrics_interval", cm.MetricsInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.containers.images_interval", cm.ImagesInterval); err != nil {
		errs = append(errs, err)
	}
	if cm.StatsConcurrency < 1 {
		add("modules.containers.stats_concurrency must be at least 1")
	}
	for i, rt := range cm.Runtimes {
		switch rt.Type {
		case "docker", "podman":
		default:
			add("modules.containers.runtimes[%d].type must be docker or podman", i)
		}
		if rt.Endpoint == "" {
			add("modules.containers.runtimes[%d].endpoint must not be empty", i)
		}
	}
	for _, glob := range cm.LabelAllowlist {
		if _, err := path.Match(glob, ""); err != nil {
			add("modules.containers.label_allowlist: invalid glob %q", glob)
		}
	}

	n := &c.Modules.Network
	if err := atLeastOneSecond("modules.network.inventory_interval", n.InventoryInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.network.metrics_interval", n.MetricsInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.network.sockets_interval", n.SocketsInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("modules.network.fd_walk_budget", n.FDWalkBudget); err != nil {
		errs = append(errs, err)
	}
	if n.MaxNetns < 1 {
		add("modules.network.max_netns must be at least 1")
	}
	for _, glob := range n.InterfaceMetricsExclude {
		if _, err := path.Match(glob, ""); err != nil {
			add("modules.network.interface_metrics_exclude: invalid glob %q", glob)
		}
	}

	r := &c.Reporting
	switch r.Sink {
	case "http", "stdout":
	default:
		add("reporting.sink must be http or stdout")
	}
	if err := atLeastOneSecond("reporting.flush_interval", r.FlushInterval); err != nil {
		errs = append(errs, err)
	}
	if r.MaxEventsPerBatch < 1 || r.MaxEventsPerBatch > 10000 {
		add("reporting.max_events_per_batch must be between 1 and 10000")
	}
	if r.MaxBatchBytes < 1024 {
		add("reporting.max_batch_bytes must be at least 1024")
	}
	if r.MaxEventBytes < 1024 {
		add("reporting.max_event_bytes must be at least 1024")
	}
	if r.MaxEventBytes > r.MaxBatchBytes {
		add("reporting.max_event_bytes must not exceed max_batch_bytes")
	}
	switch r.Compress {
	case "gzip", "none":
	default:
		add("reporting.compress must be gzip or none")
	}
	if err := atLeastOneSecond("reporting.retry.initial_interval", r.Retry.InitialInterval); err != nil {
		errs = append(errs, err)
	}
	if err := atLeastOneSecond("reporting.retry.max_interval", r.Retry.MaxInterval); err != nil {
		errs = append(errs, err)
	}
	if r.Retry.MaxInterval < r.Retry.InitialInterval {
		add("reporting.retry.max_interval must be >= initial_interval")
	}
	if !filepath.IsAbs(r.Spool.Directory) {
		add("reporting.spool.directory must be an absolute path")
	}
	if r.Spool.MaxSizeMB < 1 {
		add("reporting.spool.max_size_mb must be at least 1")
	}
	if r.Spool.SegmentSizeMB < 1 {
		add("reporting.spool.segment_size_mb must be at least 1")
	}
	if r.Spool.SegmentSizeMB > r.Spool.MaxSizeMB {
		add("reporting.spool.segment_size_mb must not exceed max_size_mb")
	}
	if err := atLeastOneSecond("reporting.resync_after_outage", r.ResyncAfterOutage); err != nil {
		errs = append(errs, err)
	}

	if c.Limits.MaxEntitiesPerKind < 1 {
		add("limits.max_entities_per_kind must be at least 1")
	}
	if c.Limits.MaxLifecycleEventsPerMinute < 1 {
		add("limits.max_lifecycle_events_per_minute must be at least 1")
	}

	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		add("logging.level must be debug, info, warn or error")
	}
	switch c.Logging.Format {
	case "json", "text":
	default:
		add("logging.format must be json or text")
	}
	if c.Logging.File != "" && !filepath.IsAbs(c.Logging.File) {
		add("logging.file must be absolute or empty")
	}

	return errors.Join(errs...)
}

func atLeastOneSecond(name string, d time.Duration) error {
	if d < time.Second {
		return fmt.Errorf("%s must be at least 1s", name)
	}
	return nil
}

// CheckFiles verifies the permissions of the enrollment token and credential
// if they exist. Both must be regular files, mode 0600, and owned by root when
// the agent runs as root (the owner may be the current user in development).
func (c *Config) CheckFiles() error {
	var errs []error
	for _, p := range []string{c.Server.EnrollmentTokenFile, c.Server.CredentialFile} {
		if err := CheckFileSecurity(p); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CheckFileSecurity validates one secret file. A missing file is not an
// error.
func CheckFileSecurity(p string) error {
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", p, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", p)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		return fmt.Errorf("%s must have mode 0600, got %04o", p, perm)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: cannot determine owner", p)
	}
	want := uint32(os.Geteuid())
	if os.Geteuid() == 0 {
		want = 0
	}
	if st.Uid != want {
		return fmt.Errorf("%s must be owned by uid %d, got %d", p, want, st.Uid)
	}
	return nil
}

// CheckStartupSecrets verifies that the agent can authenticate: either a
// credential exists, or an enrollment token exists.
func (c *Config) CheckStartupSecrets() error {
	hasCredential := fileExists(c.Server.CredentialFile)
	hasToken := fileExists(c.Server.EnrollmentTokenFile)
	if !hasCredential && !hasToken {
		return fmt.Errorf("neither credential %s nor enrollment token %s exists", c.Server.CredentialFile, c.Server.EnrollmentTokenFile)
	}
	return c.CheckFiles()
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// RestartReasons lists configuration differences that cannot be applied by a
// SIGHUP reload. Everything else is hot-reloadable (intervals, filters, label
// allowlist, log level).
func (c *Config) RestartReasons(next *Config) []string {
	var reasons []string
	if !reflect.DeepEqual(c.Paths, next.Paths) {
		reasons = append(reasons, "paths")
	}
	if !reflect.DeepEqual(c.Server, next.Server) {
		reasons = append(reasons, "server")
	}
	if c.Modules.Process.Enabled != next.Modules.Process.Enabled {
		reasons = append(reasons, "modules.process.enabled")
	}
	if c.Modules.Containers.Enabled != next.Modules.Containers.Enabled {
		reasons = append(reasons, "modules.containers.enabled")
	}
	if c.Modules.Network.Enabled != next.Modules.Network.Enabled {
		reasons = append(reasons, "modules.network.enabled")
	}
	if !reflect.DeepEqual(c.Modules.Containers.Runtimes, next.Modules.Containers.Runtimes) {
		reasons = append(reasons, "modules.containers.runtimes")
	}
	if c.Reporting.Sink != next.Reporting.Sink {
		reasons = append(reasons, "reporting.sink")
	}
	if !reflect.DeepEqual(c.Reporting.Spool, next.Reporting.Spool) {
		reasons = append(reasons, "reporting.spool")
	}
	if c.Reporting.Compress != next.Reporting.Compress {
		reasons = append(reasons, "reporting.compress")
	}
	if c.Reporting.FlushInterval != next.Reporting.FlushInterval {
		reasons = append(reasons, "reporting.flush_interval")
	}
	if c.Reporting.MaxEventsPerBatch != next.Reporting.MaxEventsPerBatch {
		reasons = append(reasons, "reporting.max_events_per_batch")
	}
	if c.Reporting.MaxBatchBytes != next.Reporting.MaxBatchBytes {
		reasons = append(reasons, "reporting.max_batch_bytes")
	}
	if c.Reporting.MaxEventBytes != next.Reporting.MaxEventBytes {
		reasons = append(reasons, "reporting.max_event_bytes")
	}
	if !reflect.DeepEqual(c.Reporting.Retry, next.Reporting.Retry) {
		reasons = append(reasons, "reporting.retry")
	}
	if c.Reporting.ResyncAfterOutage != next.Reporting.ResyncAfterOutage {
		reasons = append(reasons, "reporting.resync_after_outage")
	}
	if c.Logging.Format != next.Logging.Format {
		reasons = append(reasons, "logging.format")
	}
	if c.Logging.File != next.Logging.File {
		reasons = append(reasons, "logging.file")
	}
	return reasons
}
