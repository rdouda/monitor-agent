package core

import (
	"log/slog"
	"time"

	"agent/internal/events"
)

// EventEmitter builds CloudEvents and writes them to the sink. It is safe for
// concurrent use.
type EventEmitter struct {
	builder *events.Builder
	sink    Sink
	subject string
	log     *slog.Logger
	clock   Clock
}

// NewEventEmitter creates an emitter whose default subject is hostEntityID.
func NewEventEmitter(builder *events.Builder, sink Sink, hostEntityID string, clock Clock, log *slog.Logger) *EventEmitter {
	if log == nil {
		log = slog.Default()
	}
	if clock == nil {
		clock = SystemClock{}
	}
	return &EventEmitter{builder: builder, sink: sink, subject: hostEntityID, log: log, clock: clock}
}

// WithSubject returns an emitter view that uses subject for multi-entity
// events (the containers module uses the engine entity ID).
func (e *EventEmitter) WithSubject(subject string) Emitter {
	clone := *e
	clone.subject = subject
	return &clone
}

// Builder exposes the underlying builder to the runtime.
func (e *EventEmitter) Builder() *events.Builder { return e.builder }

// Entities emits entity upserts.
func (e *EventEmitter) Entities(snapshotID string, list []events.Entity) {
	if len(list) == 0 {
		return
	}
	envs, err := e.builder.EntityUpserts(e.subject, snapshotID, list)
	e.sendAll("entity upsert", envs, err)
}

// Relationships emits relationship upserts.
func (e *EventEmitter) Relationships(snapshotID string, list []events.Relationship) {
	if len(list) == 0 {
		return
	}
	envs, err := e.builder.RelationshipUpserts(e.subject, snapshotID, list)
	e.sendAll("relationship upsert", envs, err)
}

// RelationshipsRemoved emits relationship removals.
func (e *EventEmitter) RelationshipsRemoved(ids []string, reason string) {
	if len(ids) == 0 {
		return
	}
	envs, err := e.builder.RelationshipsRemoved(e.subject, ids, reason, events.Stamp(e.clock.Now()))
	e.sendAll("relationship removed", envs, err)
}

// Metrics emits a metric batch.
func (e *EventEmitter) Metrics(observedAt time.Time, readings []events.Reading) {
	if len(readings) == 0 {
		return
	}
	envs, err := e.builder.MetricBatches(e.subject, events.Stamp(observedAt), readings)
	e.sendAll("metric batch", envs, err)
}

// Lifecycle emits one lifecycle event with the entity as subject.
func (e *EventEmitter) Lifecycle(l events.Lifecycle) {
	e.lifecycleWith(l, l.EntityID)
}

// LifecycleWithSubject emits one lifecycle event with an explicit subject.
func (e *EventEmitter) LifecycleWithSubject(subject string, l events.Lifecycle) {
	e.lifecycleWith(l, subject)
}

func (e *EventEmitter) lifecycleWith(l events.Lifecycle, subject string) {
	if l.ObservedAt.IsZero() {
		l.ObservedAt = events.Stamp(e.clock.Now())
	}
	env, err := e.builder.LifecycleWithSubject(subject, l)
	if err != nil {
		e.log.Error("build lifecycle event", "kind", l.Kind, "reason", l.Reason, "error", err)
		return
	}
	e.sendAll("lifecycle", []*events.Envelope{env}, nil)
}

// SnapshotCompleted emits a snapshot completion record.
func (e *EventEmitter) SnapshotCompleted(s events.Snapshot) {
	if s.CompletedAt.IsZero() {
		s.CompletedAt = events.Stamp(e.clock.Now())
	}
	env, err := e.builder.SnapshotCompleted(s)
	if err != nil {
		e.log.Error("build snapshot event", "module", s.Module, "error", err)
		return
	}
	e.sendAll("snapshot", []*events.Envelope{env}, nil)
}

func (e *EventEmitter) sendAll(what string, envs []*events.Envelope, buildErr error) {
	if buildErr != nil {
		e.log.Error("build events", "what", what, "error", buildErr)
		return
	}
	for _, env := range envs {
		if err := e.sink.Send(env); err != nil {
			e.log.Error("send events", "what", what, "type", env.Type, "error", err)
		}
	}
}
