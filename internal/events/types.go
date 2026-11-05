package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event types accepted by the v1 ingestion API.
const (
	TypeEntityUpsert        = "agent.entity.upsert.v1"
	TypeRelationshipUpsert  = "agent.relationship.upsert.v1"
	TypeRelationshipRemoved = "agent.relationship.removed.v1"
	TypeMetricBatch         = "agent.metric.batch.v1"
	TypeLifecycleChanged    = "agent.entity.lifecycle.changed.v1"
	TypeHeartbeat           = "agent.heartbeat.v1"
	TypeSnapshotCompleted   = "agent.snapshot.completed.v1"
)

// Timestamp serializes as RFC 3339 in UTC with at most millisecond
// precision, as required by the wire format.
type Timestamp struct {
	time.Time
}

// Stamp normalizes t for the wire format.
func Stamp(t time.Time) Timestamp {
	return Timestamp{Time: t.UTC().Truncate(time.Millisecond)}
}

// Now returns the current time, normalized.
func Now() Timestamp {
	return Stamp(time.Now())
}

// MarshalJSON implements json.Marshaler.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	// "999" prints a fractional second only when non-zero, with at most
	// millisecond resolution.
	return []byte(`"` + t.UTC().Format("2006-01-02T15:04:05.999Z") + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Timestamp) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fmt.Errorf("invalid timestamp %q: %w", s, err)
	}
	t.Time = parsed.UTC()
	return nil
}

// IsZero reports whether the timestamp was never set.
func (t Timestamp) IsZero() bool {
	return t.Time.IsZero()
}

// Entity is one item of an entity upsert event.
type Entity struct {
	ID         string            `json:"id"`
	Kind       string            `json:"kind"`
	Name       string            `json:"name"`
	Attributes map[string]any    `json:"attributes"`
	Labels     map[string]string `json:"labels,omitempty"`
	ObservedAt Timestamp         `json:"observed_at"`
}

// NewEntity constructs an entity with a non-nil attributes map.
func NewEntity(id, kind, name string, attrs map[string]any, observedAt Timestamp) Entity {
	if attrs == nil {
		attrs = map[string]any{}
	}
	return Entity{ID: id, Kind: kind, Name: name, Attributes: attrs, ObservedAt: observedAt}
}

// Relationship is one item of a relationship upsert event.
type Relationship struct {
	ID         string         `json:"id"`
	From       string         `json:"from"`
	Type       string         `json:"type"`
	To         string         `json:"to"`
	Attributes map[string]any `json:"attributes,omitempty"`
	ObservedAt Timestamp      `json:"observed_at"`
}

// NewRelationship constructs a relationship with the given attributes.
func NewRelationship(id, from, relationshipType, to string, attrs map[string]any, observedAt Timestamp) Relationship {
	return Relationship{ID: id, From: from, Type: relationshipType, To: to, Attributes: attrs, ObservedAt: observedAt}
}

// Metric kinds.
const (
	KindGauge   = "gauge"
	KindCounter = "counter"
)

// Metric units.
const (
	UnitBytes   = "By"
	UnitRatio   = "ratio"
	UnitSeconds = "s"
	UnitHertz   = "Hz"
	UnitCount   = "count"
)

// Metric is a single reading. Temporality must be set for counters only.
type Metric struct {
	Name        string         `json:"name"`
	Kind        string         `json:"kind"`
	Value       float64        `json:"value"`
	Unit        string         `json:"unit"`
	Temporality string         `json:"temporality,omitempty"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

// Gauge constructs a gauge metric.
func Gauge(name string, value float64, unit string, attrs map[string]any) Metric {
	return Metric{Name: name, Kind: KindGauge, Value: value, Unit: unit, Attributes: attrs}
}

// Counter constructs a cumulative counter metric.
func Counter(name string, value float64, unit string, attrs map[string]any) Metric {
	return Metric{Name: name, Kind: KindCounter, Value: value, Unit: unit, Temporality: "cumulative", Attributes: attrs}
}

// Reading is the set of metrics for one entity in a metric batch.
type Reading struct {
	EntityID string   `json:"entity_id"`
	Metrics  []Metric `json:"metrics"`
}

// EntityUpsertData is the data object of agent.entity.upsert.v1.
type EntityUpsertData struct {
	SnapshotID string   `json:"snapshot_id,omitempty"`
	Entities   []Entity `json:"entities"`
}

// RelationshipUpsertData is the data object of
// agent.relationship.upsert.v1.
type RelationshipUpsertData struct {
	SnapshotID    string         `json:"snapshot_id,omitempty"`
	Relationships []Relationship `json:"relationships"`
}

// Relationship removal reasons.
const (
	RelReasonEntityRemoved       = "entity_removed"
	RelReasonNetworkDisconnected = "network_disconnected"
	RelReasonNoLongerObserved    = "no_longer_observed"
)

// RelationshipRemovedData is the data object of
// agent.relationship.removed.v1.
type RelationshipRemovedData struct {
	RelationshipIDs []string  `json:"relationship_ids"`
	Reason          string    `json:"reason"`
	ObservedAt      Timestamp `json:"observed_at"`
}

// MetricBatchData is the data object of agent.metric.batch.v1.
type MetricBatchData struct {
	ObservedAt Timestamp `json:"observed_at"`
	Readings   []Reading `json:"readings"`
}
