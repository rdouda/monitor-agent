package events

// Envelope is the CloudEvents envelope shared by all event types.
type Envelope struct {
	SpecVersion     string    `json:"specversion"`
	ID              string    `json:"id"`
	Source          string    `json:"source"`
	Type            string    `json:"type"`
	Subject         string    `json:"subject"`
	Time            Timestamp `json:"time"`
	DataContentType string    `json:"datacontenttype"`
	AgentID         string    `json:"agentid"`
	AgentSeq        uint64    `json:"agentseq"`
	AgentRun        string    `json:"agentrun"`
	Environment     string    `json:"environment"`
	Data            any       `json:"data"`
}
