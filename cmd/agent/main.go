// Command agent is the monitoring agent.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"agent/internal/config"
	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/identity"
	"agent/internal/modules"
	"agent/internal/queue"
	"agent/internal/reporting"
)

var (
	version = "dev"
	commit  = "none"
)

const defaultConfigPath = "/etc/agent/agent.yaml"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "run":
		return cmdRun(args[1:])
	case "validate-config":
		return cmdValidate(args[1:])
	case "once":
		return cmdOnce(args[1:])
	case "enroll":
		return cmdEnroll(args[1:])
	case "version":
		fmt.Printf("agent %s (commit %s, %s)\n", version, commit, runtime.Version())
		return 0
	case "help", "-h", "--help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  agent run --config PATH
  agent validate-config --config PATH
  agent once --config PATH [--sample-wait 2s] [--modules host,process]
  agent enroll --config PATH
  agent version
`)
}

func parseConfigFlag(name string, args []string) (string, error) {
	fs := flagSet(name)
	path := fs.String("config", defaultConfigPath, "path to agent.yaml")
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	if fs.NArg() != 0 {
		return "", fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	return *path, nil
}

func cmdValidate(args []string) int {
	path, err := parseConfigFlag("validate-config", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		return 2
	}
	fmt.Printf("config %s is valid (environment %s)\n", path, cfg.Agent.Environment)
	return 0
}

func cmdOnce(args []string) int {
	fs := flagSet("once")
	path := fs.String("config", defaultConfigPath, "path to agent.yaml")
	sampleWait := fs.Duration("sample-wait", 2*time.Second, "pause between rate-metric samples")
	moduleList := fs.String("modules", "", "comma-separated modules to scan (default: all enabled)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected arguments:", strings.Join(fs.Args(), " "))
		return 2
	}
	if *sampleWait < 0 {
		fmt.Fprintln(os.Stderr, "--sample-wait must not be negative")
		return 2
	}
	names, err := parseModuleList(*moduleList)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	cfg, err := config.LoadOnce(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		return 2
	}
	log := setupLogger(cfg, os.Stderr)
	limitProcs()

	stored, err := identity.Load(cfg.Paths.StateDir, cfg.Paths.HostfsRoot, cfg.Paths.SysfsRoot)
	if err != nil {
		log.Error("identity read failed", "error", err)
		return 1
	}
	id := core.Identity{AgentID: stored.AgentID, HostID: stored.HostID, AgentRunID: stored.AgentRunID}
	// once must never touch the persistent seq file: a running service may be
	// using the same agent_id.
	seq := &memorySeq{}

	mgr, err := buildManagerWith(cfg, log, id, seq, reporting.NewStdoutSink(os.Stdout), nil, time.Now(), *sampleWait, names)
	if err != nil {
		log.Error("startup failed", "error", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := mgr.RunOnce(ctx); err != nil {
		log.Error("scan failed", "error", err)
		return 1
	}
	return 0
}

type memorySeq struct {
	mu sync.Mutex
	n  uint64
}

func (s *memorySeq) Next() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return s.n, nil
}

func parseModuleList(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	valid := map[string]bool{"host": true, "process": true, "containers": true, "network": true}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("--modules contains an empty name")
		}
		if !valid[name] {
			return nil, fmt.Errorf("unknown module %q (want host, process, containers or network)", name)
		}
		out = append(out, name)
	}
	return out, nil
}

func cmdEnroll(args []string) int {
	path, err := parseConfigFlag("enroll", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		return 2
	}
	log := setupLogger(cfg, os.Stderr)
	if err := cfg.CheckFiles(); err != nil {
		log.Error("insecure credential files", "error", err)
		return 2
	}
	if _, err := os.Stat(cfg.Server.EnrollmentTokenFile); err != nil {
		log.Error("enrollment token not found", "path", cfg.Server.EnrollmentTokenFile, "error", err)
		return 2
	}
	id, _, err := loadIdentity(cfg, log)
	if err != nil {
		log.Error("identity failed", "error", err)
		return 1
	}
	client, err := newReportingClient(cfg, id)
	if err != nil {
		log.Error("client setup failed", "error", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Enroll(ctx); err != nil {
		log.Error("enrollment failed", "error", err)
		return 1
	}
	fmt.Printf("enrolled agent %s; credential stored in %s\n", id.AgentID, cfg.Server.CredentialFile)
	return 0
}

func cmdRun(args []string) int {
	path, err := parseConfigFlag("run", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		return 2
	}
	logDest := io.Writer(os.Stdout)
	if cfg.Reporting.Sink == "stdout" || cfg.Logging.File != "" {
		logDest = os.Stderr
	}
	if cfg.Logging.File != "" {
		f, err := os.OpenFile(cfg.Logging.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open log file:", err)
			return 2
		}
		defer f.Close()
		logDest = f
	}
	log := setupLogger(cfg, logDest)
	limitProcs()

	if cfg.Reporting.Sink != "stdout" && cfg.Reporting.Sink != "http" {
		log.Error("unsupported reporting sink", "sink", cfg.Reporting.Sink)
		return 2
	}

	id, seq, err := loadIdentity(cfg, log)
	if err != nil {
		log.Error("identity failed", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	if cfg.Reporting.Sink == "stdout" {
		return runStdout(ctx, path, cfg, id, seq, hup, log)
	}
	return runHTTP(ctx, path, cfg, id, seq, hup, log)
}

// runStdout runs the development sink and supports SIGHUP reload by rebuilding
// the manager with the new hot-reloadable configuration.
func runStdout(ctx context.Context, path string, cfg *config.Config, id core.Identity, seq events.SeqAllocator, hup <-chan os.Signal, log *slog.Logger) int {
	sink := reporting.NewStdoutSink(os.Stdout)
	current := cfg
	startedAt := time.Now()
	for {
		mgr, err := buildManager(current, log, id, seq, sink, nil, startedAt)
		if err != nil {
			log.Error("startup failed", "error", err)
			return 1
		}
		reloadCtx, cancel := context.WithCancel(ctx)
		reloads := watchReloads(reloadCtx, path, current, hup, log, cancel)
		runErr := mgr.Run(reloadCtx)
		cancel()
		select {
		case next := <-reloads:
			current = next
			continue
		default:
		}
		if ctx.Err() != nil {
			return 0
		}
		if runErr != nil {
			log.Error("agent failed", "error", runErr)
			return 1
		}
		return 0
	}
}

// watchReloads reloads the config on SIGHUP. Changes that require a process
// restart are rejected and the current config is kept.
func watchReloads(ctx context.Context, path string, current *config.Config, hup <-chan os.Signal, log *slog.Logger, cancel context.CancelFunc) <-chan *config.Config {
	out := make(chan *config.Config, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				next, err := config.Load(path)
				if err != nil {
					log.Error("config reload failed; keeping current config", "error", err)
					continue
				}
				if reasons := current.RestartReasons(next); len(reasons) > 0 {
					log.Warn("config change requires a restart; keeping current config", "fields", reasons)
					continue
				}
				applyLogLevel(next)
				log.Info("configuration reloaded")
				select {
				case out <- next:
				case <-ctx.Done():
					return
				}
				cancel()
				return
			}
		}
	}()
	return out
}

func runHTTP(ctx context.Context, path string, cfg *config.Config, id core.Identity, seq events.SeqAllocator, hup <-chan os.Signal, log *slog.Logger) int {
	if err := cfg.CheckStartupSecrets(); err != nil {
		log.Error("missing or insecure credentials", "error", err)
		return 2
	}
	spool, err := queue.Open(queue.Options{
		Directory:     cfg.Reporting.Spool.Directory,
		MaxBytes:      int64(cfg.Reporting.Spool.MaxSizeMB) << 20,
		SegmentBytes:  int64(cfg.Reporting.Spool.SegmentSizeMB) << 20,
		FlushInterval: cfg.Reporting.FlushInterval,
		Logger:        log,
	})
	if err != nil {
		log.Error("open spool", "error", err)
		return 1
	}

	client, err := newReportingClient(cfg, id)
	if err != nil {
		_ = spool.Close()
		log.Error("client setup failed", "error", err)
		return 1
	}

	// Enroll before the first scan when possible. If the server is down the
	// agent still starts and keeps events in the spool until delivery works.
	enrollCtx, cancelEnroll := context.WithTimeout(ctx, 30*time.Second)
	if err := client.EnsureCredential(enrollCtx); err != nil && ctx.Err() == nil {
		log.Warn("enrollment failed; events will be spooled", "error", err)
	}
	cancelEnroll()

	var mgrPtr atomic.Pointer[core.Manager]
	sender, err := reporting.NewSender(reporting.SenderConfig{
		Spool:             spool,
		Client:            client,
		Logger:            log,
		MaxEventsPerBatch: cfg.Reporting.MaxEventsPerBatch,
		MaxBatchBytes:     cfg.Reporting.MaxBatchBytes,
		FlushInterval:     cfg.Reporting.FlushInterval,
		Retry:             cfg.Reporting.Retry,
		ResyncAfterOutage: cfg.Reporting.ResyncAfterOutage,
		Resync: func() {
			if m := mgrPtr.Load(); m != nil {
				m.Resync()
			}
		},
	})
	if err != nil {
		_ = spool.Close()
		log.Error("sender setup failed", "error", err)
		return 1
	}

	sink := &reporting.SpoolSink{Spool: spool, OnSend: sender.Wake}
	heartbeat := &reporting.HeartbeatSink{Client: client, Timeout: cfg.Server.Timeout, Log: log}

	senderCtx, stopSender := context.WithCancel(ctx)
	defer stopSender()
	senderDone := make(chan error, 1)
	go func() { senderDone <- sender.Run(senderCtx) }()

	current := cfg
	startedAt := time.Now()
	var runErr error
	for {
		mgr, err := buildManager(current, log, id, seq, sink, heartbeat, startedAt)
		if err != nil {
			runErr = err
			break
		}
		mgrPtr.Store(mgr)
		mgr.SetQueueStats(reporting.QueueStats{Spool: spool})
		mgr.SetReportingStats(sender)

		reloadCtx, cancel := context.WithCancel(ctx)
		reloads := watchReloads(reloadCtx, path, current, hup, log, cancel)
		err = mgr.Run(reloadCtx)
		cancel()
		select {
		case next := <-reloads:
			current = next
			continue
		default:
		}
		if err != nil {
			runErr = err
		}
		break
	}
	mgrPtr.Store(nil)
	stopSender()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case err := <-senderDone:
		if err != nil && runErr == nil {
			runErr = err
		}
	case <-stopCtx.Done():
		log.Warn("sender did not stop within 5s")
	}
	if err := spool.Close(); err != nil && runErr == nil {
		runErr = err
	}
	if runErr != nil {
		log.Error("agent failed", "error", runErr)
		return 1
	}
	return 0
}

func loadIdentity(cfg *config.Config, log *slog.Logger) (core.Identity, events.SeqAllocator, error) {
	id, err := identity.LoadOrCreate(cfg.Paths.StateDir, cfg.Paths.HostfsRoot, cfg.Paths.SysfsRoot)
	if err != nil {
		return core.Identity{}, nil, err
	}
	if id.CloneDetected {
		log.Warn("host fingerprint changed; generated new agent_id and host_id",
			"agent_id", id.AgentID, "host_id", id.HostID)
	}
	seq, err := identity.OpenSeq(filepath.Join(cfg.Paths.StateDir, "seq"))
	if err != nil {
		return core.Identity{}, nil, err
	}
	return core.Identity{AgentID: id.AgentID, HostID: id.HostID, AgentRunID: id.AgentRunID}, seq, nil
}

func buildManager(cfg *config.Config, log *slog.Logger, id core.Identity, seq events.SeqAllocator, sink, heartbeatSink core.Sink, startedAt time.Time) (*core.Manager, error) {
	return buildManagerWith(cfg, log, id, seq, sink, heartbeatSink, startedAt, 0, nil)
}

func buildManagerWith(cfg *config.Config, log *slog.Logger, id core.Identity, seq events.SeqAllocator, sink, heartbeatSink core.Sink, startedAt time.Time, sampleWait time.Duration, moduleNames []string) (*core.Manager, error) {
	return core.NewManager(core.Options{
		Config:        cfg,
		Identity:      id,
		Seq:           seq,
		Sink:          sink,
		HeartbeatSink: heartbeatSink,
		Version:       version,
		Commit:        commit,
		Logger:        log,
		Clock:         core.SystemClock{},
		StartedAt:     startedAt,
		SampleWait:    sampleWait,
		ModuleFactory: func(d core.Deps) ([]core.Module, error) {
			return modules.BuildSelected(d, moduleNames)
		},
	})
}

func newReportingClient(cfg *config.Config, id core.Identity) (*reporting.Client, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return reporting.NewClient(reporting.ClientConfig{
		BaseURL:             cfg.Server.BaseURL,
		EventsEndpoint:      cfg.Server.EventsEndpoint,
		EnrollEndpoint:      cfg.Server.EnrollEndpoint,
		CredentialFile:      cfg.Server.CredentialFile,
		EnrollmentTokenFile: cfg.Server.EnrollmentTokenFile,
		Timeout:             cfg.Server.Timeout,
		TLS:                 cfg.Server.TLS,
		Compress:            cfg.Reporting.Compress == "gzip",
		UserAgent:           "agent/" + version,
		AgentID:             id.AgentID,
		HostID:              id.HostID,
		Hostname:            hostname,
		OS:                  "linux",
		Arch:                runtime.GOARCH,
		Environment:         cfg.Agent.Environment,
		AgentVersion:        version,
	})
}

var activeLogLevel = new(slog.LevelVar)

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func applyLogLevel(cfg *config.Config) {
	activeLogLevel.Set(parseLogLevel(cfg.Logging.Level))
}

func setupLogger(cfg *config.Config, w io.Writer) *slog.Logger {
	activeLogLevel.Set(parseLogLevel(cfg.Logging.Level))
	opts := &slog.HandlerOptions{Level: activeLogLevel}
	var h slog.Handler
	if cfg.Logging.Format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}

func limitProcs() {
	n := runtime.NumCPU()
	if n > 2 {
		n = 2
	}
	if n < 1 {
		n = 1
	}
	runtime.GOMAXPROCS(n)
}

func flagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}
