package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrEventTooLarge is returned when a single item cannot fit into one event
// even after all splittable arrays have been split.
var ErrEventTooLarge = errors.New("event exceeds max_event_bytes")

// SeqAllocator yields strictly increasing, durable agent sequence numbers.
type SeqAllocator interface {
	Next() (uint64, error)
}

// Builder creates CloudEvent envelopes. It is the only place that assigns
// event IDs, agent sequence numbers and envelope timestamps.
type Builder struct {
	AgentID       string
	AgentRunID    string
	Environment   string
	Seq           SeqAllocator
	MaxEventBytes int
	Clock         func() time.Time

	mu sync.Mutex
}

// NewBuilder constructs an event builder. MaxEventBytes must be positive.
func NewBuilder(agentID, agentRunID, environment string, seq SeqAllocator, maxEventBytes int) *Builder {
	if maxEventBytes <= 0 {
		maxEventBytes = 65536
	}
	return &Builder{
		AgentID:       agentID,
		AgentRunID:    agentRunID,
		Environment:   environment,
		Seq:           seq,
		MaxEventBytes: maxEventBytes,
		Clock:         time.Now,
	}
}

// Source returns the CloudEvents source URN for this agent.
func (b *Builder) Source() string {
	return "urn:agent:" + b.AgentID
}

func (b *Builder) now() time.Time {
	clock := b.Clock
	if clock == nil {
		clock = time.Now
	}
	return clock()
}

// New creates one envelope, allocating the next sequence number.
func (b *Builder) New(eventType, subject string, data any) (*Envelope, error) {
	return b.newEnvelope(eventType, subject, data, b.now())
}

func (b *Builder) newEnvelope(eventType, subject string, data any, at time.Time) (*Envelope, error) {
	if b.Seq == nil {
		return nil, errors.New("event builder has no sequence allocator")
	}
	b.mu.Lock()
	seq, err := b.Seq.Next()
	b.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("allocate agentseq: %w", err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate event id: %w", err)
	}
	return &Envelope{
		SpecVersion:     "1.0",
		ID:              id.String(),
		Source:          b.Source(),
		Type:            eventType,
		Subject:         subject,
		Time:            Stamp(at),
		DataContentType: "application/json",
		AgentID:         b.AgentID,
		AgentSeq:        seq,
		AgentRun:        b.AgentRunID,
		Environment:     b.Environment,
		Data:            data,
	}, nil
}

// EntityUpserts builds one or more entity upsert events.
func (b *Builder) EntityUpserts(subject, snapshotID string, entities []Entity) ([]*Envelope, error) {
	if len(entities) == 0 {
		return nil, nil
	}
	return buildChunked(b, TypeEntityUpsert, subject, entities, 200, b.now(), func(chunk []Entity) any {
		return EntityUpsertData{SnapshotID: snapshotID, Entities: chunk}
	})
}

// RelationshipUpserts builds one or more relationship upsert events.
func (b *Builder) RelationshipUpserts(subject, snapshotID string, rels []Relationship) ([]*Envelope, error) {
	if len(rels) == 0 {
		return nil, nil
	}
	return buildChunked(b, TypeRelationshipUpsert, subject, rels, 200, b.now(), func(chunk []Relationship) any {
		return RelationshipUpsertData{SnapshotID: snapshotID, Relationships: chunk}
	})
}

// RelationshipsRemoved builds one or more relationship removal events.
func (b *Builder) RelationshipsRemoved(subject string, ids []string, reason string, observedAt Timestamp) ([]*Envelope, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return buildChunked(b, TypeRelationshipRemoved, subject, ids, 500, observedAt.Time, func(chunk []string) any {
		return RelationshipRemovedData{RelationshipIDs: chunk, Reason: reason, ObservedAt: observedAt}
	})
}

// MetricBatches builds one or more metric batch events. Readings with more
// than 64 metrics are split into several readings for the same entity.
func (b *Builder) MetricBatches(subject string, observedAt Timestamp, readings []Reading) ([]*Envelope, error) {
	normalized, err := normalizeReadings(readings)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	return buildChunked(b, TypeMetricBatch, subject, normalized, 500, observedAt.Time, func(chunk []Reading) any {
		return MetricBatchData{ObservedAt: observedAt, Readings: chunk}
	})
}

// Lifecycle builds one lifecycle event with the entity as its subject.
func (b *Builder) Lifecycle(l Lifecycle) (*Envelope, error) {
	return b.checkSingle(TypeLifecycleChanged, l.EntityID, l)
}

// LifecycleWithSubject builds a lifecycle event with an explicit envelope
// subject. The containers module uses the engine entity as subject for every
// event it emits.
func (b *Builder) LifecycleWithSubject(subject string, l Lifecycle) (*Envelope, error) {
	return b.checkSingle(TypeLifecycleChanged, subject, l)
}

// SnapshotCompleted builds one snapshot.completed event.
func (b *Builder) SnapshotCompleted(s Snapshot) (*Envelope, error) {
	if s.Errors == nil {
		s.Errors = []SnapshotError{}
	}
	return b.checkSingle(TypeSnapshotCompleted, s.ScopeEntityID, s)
}

// HeartbeatEvent builds one heartbeat event. It assigns a sequence number but
// the caller must not spool it.
func (b *Builder) HeartbeatEvent(subject string, h Heartbeat) (*Envelope, error) {
	return b.checkSingle(TypeHeartbeat, subject, h)
}

func (b *Builder) checkSingle(eventType, subject string, data any) (*Envelope, error) {
	env, err := b.New(eventType, subject, data)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	if len(raw) > b.MaxEventBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes", ErrEventTooLarge, eventType, len(raw))
	}
	return env, nil
}

func normalizeReadings(readings []Reading) ([]Reading, error) {
	out := make([]Reading, 0, len(readings))
	for _, r := range readings {
		if r.EntityID == "" || len(r.Metrics) == 0 {
			continue
		}
		for _, m := range r.Metrics {
			if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
				return nil, fmt.Errorf("metric %s of %s is not finite", m.Name, r.EntityID)
			}
		}
		for start := 0; start < len(r.Metrics); start += 64 {
			end := start + 64
			if end > len(r.Metrics) {
				end = len(r.Metrics)
			}
			out = append(out, Reading{EntityID: r.EntityID, Metrics: r.Metrics[start:end]})
		}
	}
	return out, nil
}

// buildChunked splits items first by maxItems and then, if the serialized
// event is still too large, by halving the chunk.
func buildChunked[T any](
	b *Builder,
	eventType, subject string,
	items []T,
	maxItems int,
	at time.Time,
	dataOf func([]T) any,
) ([]*Envelope, error) {
	if len(items) == 0 {
		return nil, nil
	}
	var out []*Envelope

	var emit func(chunk []T) error
	emit = func(chunk []T) error {
		env, err := b.newEnvelope(eventType, subject, dataOf(chunk), at)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if len(raw) <= b.MaxEventBytes {
			out = append(out, env)
			return nil
		}
		if len(chunk) == 1 {
			return fmt.Errorf("%w: %s is %d bytes", ErrEventTooLarge, eventType, len(raw))
		}
		mid := len(chunk) / 2
		if err := emit(chunk[:mid]); err != nil {
			return err
		}
		return emit(chunk[mid:])
	}

	for start := 0; start < len(items); start += maxItems {
		end := start + maxItems
		if end > len(items) {
			end = len(items)
		}
		if err := emit(items[start:end]); err != nil {
			return nil, err
		}
	}
	return out, nil
}
