package events

// Lifecycle statuses from AGENTS.md §7.5.
const (
	StatusCreated    = "created"
	StatusRunning    = "running"
	StatusPaused     = "paused"
	StatusRestarting = "restarting"
	StatusExited     = "exited"
	StatusDead       = "dead"
	StatusUp         = "up"
	StatusDown       = "down"
	StatusRemoved    = "removed"
)

// Lifecycle reasons from AGENTS.md §7.5.
const (
	ReasonProcessStarted      = "process_started"
	ReasonProcessExited       = "process_exited"
	ReasonContainerCreated    = "container_created"
	ReasonContainerStarted    = "container_started"
	ReasonContainerStopped    = "container_stopped"
	ReasonContainerDied       = "container_died"
	ReasonContainerOOMKilled  = "container_oom_killed"
	ReasonContainerPaused     = "container_paused"
	ReasonContainerUnpaused   = "container_unpaused"
	ReasonContainerRestarting = "container_restarting"
	ReasonContainerHealth     = "container_health_changed"
	ReasonInterfaceUp         = "interface_up"
	ReasonInterfaceDown       = "interface_down"
	ReasonEngineAvailable     = "engine_available"
	ReasonEngineUnavailable   = "engine_unavailable"
	ReasonEntityCreated       = "entity_created"
	ReasonEntityRemoved       = "entity_removed"
	ReasonStateReconciled     = "state_reconciled"
)

// Lifecycle is the data object of agent.entity.lifecycle.changed.v1.
type Lifecycle struct {
	EntityID       string         `json:"entity_id"`
	Kind           string         `json:"kind"`
	Name           string         `json:"name"`
	PreviousStatus *string        `json:"previous_status,omitempty"`
	Status         string         `json:"status"`
	Health         string         `json:"health,omitempty"`
	Reason         string         `json:"reason"`
	Details        map[string]any `json:"details,omitempty"`
	ObservedAt     Timestamp      `json:"observed_at"`
}

// SnapshotError codes from the schema.
const (
	ErrCodePermissionDenied = "permission_denied"
	ErrCodeAPIError         = "api_error"
	ErrCodeTimeout          = "timeout"
	ErrCodeLimitExceeded    = "limit_exceeded"
	ErrCodeFDWalkTimeout    = "fd_walk_timeout"
	ErrCodeParseError       = "parse_error"
	ErrCodeUnavailable      = "unavailable"
	ErrCodeInternal         = "internal"
)

// SnapshotError describes one problem encountered during a scan.
type SnapshotError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Kind    string `json:"kind,omitempty"`
}

// Snapshot is the data object of agent.snapshot.completed.v1.
type Snapshot struct {
	Module             string          `json:"module"`
	SnapshotID         string          `json:"snapshot_id"`
	ScopeEntityID      string          `json:"scope_entity_id"`
	CoversKinds        []string        `json:"covers_kinds"`
	Complete           bool            `json:"complete"`
	StartedAt          Timestamp       `json:"started_at"`
	CompletedAt        Timestamp       `json:"completed_at"`
	EntitiesCount      int             `json:"entities_count"`
	RelationshipsCount int             `json:"relationships_count"`
	Errors             []SnapshotError `json:"errors"`
}
