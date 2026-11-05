package events

// EnrollRequest is the body of POST {enroll_endpoint}.
type EnrollRequest struct {
	AgentID      string `json:"agent_id"`
	HostID       string `json:"host_id"`
	Hostname     string `json:"hostname"`
	AgentVersion string `json:"agent_version"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Environment  string `json:"environment,omitempty"`
}

// EnrollResponse is returned by a successful enrollment.
type EnrollResponse struct {
	AgentID             string     `json:"agent_id"`
	Credential          string     `json:"credential"`
	CredentialExpiresAt *Timestamp `json:"credential_expires_at,omitempty"`
}

// IngestResponse is returned by the events endpoint.
type IngestResponse struct {
	Status        string          `json:"status"`
	AcceptedCount int             `json:"accepted_count"`
	RejectedCount int             `json:"rejected_count"`
	ServerTime    *Timestamp      `json:"server_time,omitempty"`
	Rejected      []RejectedEvent `json:"rejected,omitempty"`
}

// RejectedEvent describes one event the server refused.
type RejectedEvent struct {
	ID        string `json:"id"`
	Code      string `json:"code"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable"`
}
