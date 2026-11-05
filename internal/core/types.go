// Package core contains the agent runtime: modules, scheduling, supervision,
// event emission and health reporting.
package core

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"agent/internal/config"
	"agent/internal/events"
)

// Identity is the runtime view of the persistent identity.
type Identity struct {
	AgentID    string
	HostID     string
	AgentRunID string
}

// Paths are the roots every collector must read through.
type Paths struct {
	ProcfsRoot string
	SysfsRoot  string
	HostfsRoot string
}

// Clock is injectable for tests.
type Clock interface {
	Now() time.Time
}

// SystemClock uses the wall clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Module status values, mirroring the heartbeat vocabulary.
const (
	ModuleHealthy     = events.ModuleHealthy
	ModuleDegraded    = events.ModuleDegraded
	ModuleUnavailable = events.ModuleUnavailable
	ModuleFailed      = events.ModuleFailed
	ModuleDisabled    = events.ModuleDisabled
)

// ModuleHealth is the health snapshot of one module.
type ModuleHealth struct {
	Status        string
	Reason        string
	LastSuccessAt time.Time
	LastError     string
}

// Module is implemented by host, process, containers and network.
type Module interface {
	Name() string
	Init(ctx context.Context, d Deps) error
	Run(ctx context.Context) error
	Health() ModuleHealth
}

// OneShot is implemented by modules that can perform a single full scan for
// `agent once`.
type OneShot interface {
	RunOnce(ctx context.Context) error
}

// Resyncer is implemented by modules that can run their full inventory scan
// on demand (used after a delivery outage or spool eviction).
type Resyncer interface {
	Resync(ctx context.Context) error
}

// MetricsPrimer seeds rate-based metric samples without emitting them. It is
// used by `agent once` so that a second sample can produce CPU utilization
// metrics.
type MetricsPrimer interface {
	PrimeMetrics(ctx context.Context) error
}

// ErrUnavailable signals that a module precondition is missing (for example
// no container runtime socket). The supervisor re-probes every 60 seconds.
type ErrUnavailable struct {
	Reason string
}

func (e ErrUnavailable) Error() string { return "unavailable: " + e.Reason }

// IsUnavailable reports whether err is an ErrUnavailable.
func IsUnavailable(err error) bool {
	var e ErrUnavailable
	return errors.As(err, &e)
}

// Deps is passed to every module.
type Deps struct {
	Config    *config.Config
	Identity  Identity
	Emitter   Emitter
	Scheduler Scheduler
	Index     ContainerIndex
	Paths     Paths
	Clock     Clock
	Log       *slog.Logger
	Version   string
	Commit    string
	// RateLimited reports lifecycle events dropped by a module's own rate
	// limit so the spool can include them in the heartbeat.
	RateLimited func(n int64)
}

// Emitter converts module output into CloudEvents and passes them to the sink.
type Emitter interface {
	Entities(snapshotID string, e []events.Entity)
	Relationships(snapshotID string, r []events.Relationship)
	RelationshipsRemoved(ids []string, reason string)
	Metrics(observedAt time.Time, readings []events.Reading)
	Lifecycle(l events.Lifecycle)
	SnapshotCompleted(s events.Snapshot)
}

// ContainerRef is one entry of the shared container index.
type ContainerRef struct {
	ContainerID string
	EntityID    string
	EngineKey   string
	MainPID     int
}

// ContainerIndex lets the process and network modules resolve containers
// published by the containers module. Implementations that also support
// enumeration or publication may implement ContainerEnumerator and
// ContainerPublisher; callers type-assert for those optional capabilities.
type ContainerIndex interface {
	Lookup(containerID string) (ContainerRef, bool)
	ByPID(pid int) (ContainerRef, bool)
}

// ContainerEnumerator is the optional enumeration capability used by the
// network module to discover container network namespaces.
type ContainerEnumerator interface {
	All() []ContainerRef
}

// ContainerPublisher is the optional publication capability used by the
// containers module.
type ContainerPublisher interface {
	Publish(refs []ContainerRef)
}

// Sink accepts fully built events. Implementations are stdout (development),
// or the spool (production).
type Sink interface {
	Send(env *events.Envelope) error
	Close() error
}

// Task is one periodic scheduler entry.
type Task struct {
	Name     string
	Interval time.Duration
	Timeout  time.Duration
	Run      func(context.Context) error
}

// Scheduler runs periodic tasks without overlapping a task with itself.
type Scheduler interface {
	Add(t Task)
	Run(ctx context.Context) error
}

// QueueStats is implemented by the spool to feed the heartbeat.
type QueueStats interface {
	QueueSnapshot() QueueSnapshot
}

// RateLimitRecorder is implemented by the spool to count lifecycle events
// dropped before they reach the queue.
type RateLimitRecorder interface {
	RecordRateLimited(n int64)
}

// QueueSnapshot mirrors the heartbeat queue object.
type QueueSnapshot struct {
	DepthEvents           int64
	SizeBytes             int64
	OldestEventAgeSeconds *int64
	Dropped               [3]int64
	RejectedTotal         int64
	RateLimitedTotal      int64
}

// ReportingStats is implemented by the delivery pipeline.
type ReportingStats interface {
	LastSuccess() time.Time
	ConsecutiveFailures() int64
}
