package events

// Agent status values used in heartbeats.
const (
	AgentHealthy   = "healthy"
	AgentDegraded  = "degraded"
	AgentUnhealthy = "unhealthy"
)

// Module health statuses, matching AGENTS.md §5.3.
const (
	ModuleHealthy     = "healthy"
	ModuleDegraded    = "degraded"
	ModuleUnavailable = "unavailable"
	ModuleFailed      = "failed"
	ModuleDisabled    = "disabled"
)

// Heartbeat is the data object of agent.heartbeat.v1.
type Heartbeat struct {
	Agent     HeartbeatAgent             `json:"agent"`
	HostID    string                     `json:"host_id"`
	Modules   map[string]HeartbeatModule `json:"modules"`
	Queue     HeartbeatQueue             `json:"queue"`
	Reporting HeartbeatReporting         `json:"reporting"`
}

// HeartbeatAgent identifies the running agent.
type HeartbeatAgent struct {
	ID             string    `json:"id"`
	RunID          string    `json:"run_id"`
	Version        string    `json:"version"`
	Commit         string    `json:"commit,omitempty"`
	Status         string    `json:"status"`
	StartedAt      Timestamp `json:"started_at"`
	UptimeSeconds  int64     `json:"uptime_seconds"`
	RSSBytes       *int64    `json:"rss_bytes,omitempty"`
	CPUUtilization *float64  `json:"cpu_utilization,omitempty"`
}

// HeartbeatModule reports one module's state. Details is free-form.
type HeartbeatModule struct {
	Status        string         `json:"status"`
	Reason        string         `json:"reason,omitempty"`
	LastSuccessAt *Timestamp     `json:"last_success_at,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

// HeartbeatQueue reports spool state.
type HeartbeatQueue struct {
	DepthEvents               int64            `json:"depth_events"`
	SizeBytes                 int64            `json:"size_bytes"`
	OldestEventAgeSeconds     *int64           `json:"oldest_event_age_seconds,omitempty"`
	DroppedTotal              HeartbeatDropped `json:"dropped_total"`
	RejectedTotal             int64            `json:"rejected_total,omitempty"`
	LifecycleRateLimitedTotal int64            `json:"lifecycle_rate_limited_total,omitempty"`
}

// HeartbeatDropped counts events discarded because the spool was full.
type HeartbeatDropped struct {
	P0 int64 `json:"p0"`
	P1 int64 `json:"p1"`
	P2 int64 `json:"p2"`
}

// HeartbeatReporting reports delivery health.
type HeartbeatReporting struct {
	LastSuccessAt       *Timestamp `json:"last_success_at,omitempty"`
	ConsecutiveFailures int64      `json:"consecutive_failures"`
}
