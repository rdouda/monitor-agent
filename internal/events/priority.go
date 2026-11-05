package events

// Priority is the spool priority class of an event.
type Priority int

// Priority classes. PriorityNone marks events that are never spooled
// (heartbeats).
const (
	PriorityP0 Priority = iota
	PriorityP1
	PriorityP2
	PriorityNone
)

// PriorityOf returns the spool class for an event type.
func PriorityOf(eventType string) Priority {
	switch eventType {
	case TypeLifecycleChanged, TypeRelationshipRemoved:
		return PriorityP0
	case TypeMetricBatch:
		return PriorityP2
	case TypeHeartbeat:
		return PriorityNone
	case TypeEntityUpsert, TypeRelationshipUpsert, TypeSnapshotCompleted:
		return PriorityP1
	default:
		return PriorityP1
	}
}

// String returns "p0", "p1", "p2" or "none".
func (p Priority) String() string {
	switch p {
	case PriorityP0:
		return "p0"
	case PriorityP1:
		return "p1"
	case PriorityP2:
		return "p2"
	default:
		return "none"
	}
}
