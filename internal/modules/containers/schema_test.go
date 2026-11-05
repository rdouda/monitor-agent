package containers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"agent/internal/events"
	"agent/internal/schema"

	"github.com/google/uuid"
)

type memSeq struct {
	n uint64
}

func (s *memSeq) Next() (uint64, error) {
	s.n++
	return s.n, nil
}

func TestEmittedEventsValidateAgainstSchemas(t *testing.T) {
	f := newContainerFixture(t)
	ctx := context.Background()
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.imagesTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	if err := f.mod.metricsTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Minute)
	if err := f.mod.metricsTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}
	f.fake.set(func(r *fakeRuntime) { r.containers[0].State = "exited" })
	f.clock.Advance(time.Minute)
	if err := f.mod.inventoryTask(ctx, f.state); err != nil {
		t.Fatal(err)
	}

	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	readings := append([]events.Reading(nil), f.em.metrics...)
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()

	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}
	b := events.NewBuilder(
		"6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		"a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		"test", &memSeq{}, 1<<20,
	)
	hostID := "host:" + f.deps.Identity.HostID
	snapID := uuid.NewString()
	if len(snaps) > 0 {
		snapID = snaps[0].SnapshotID
	}
	validate := func(env *events.Envelope) {
		t.Helper()
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		if err := set.ValidateEnvelope(raw); err != nil {
			t.Fatalf("%s invalid: %v\n%s", env.Type, err, raw)
		}
	}
	for _, e := range entities {
		envs, err := b.EntityUpserts(hostID, snapID, []events.Entity{e})
		if err != nil {
			t.Fatal(err)
		}
		for _, env := range envs {
			validate(env)
		}
	}
	for _, r := range rels {
		envs, err := b.RelationshipUpserts(hostID, snapID, []events.Relationship{r})
		if err != nil {
			t.Fatal(err)
		}
		for _, env := range envs {
			validate(env)
		}
	}
	for _, s := range snaps {
		env, err := b.SnapshotCompleted(s)
		if err != nil {
			t.Fatal(err)
		}
		validate(env)
	}
	for _, l := range lifecycles {
		env, err := b.Lifecycle(l)
		if err != nil {
			t.Fatal(err)
		}
		validate(env)
	}
	if len(readings) > 0 {
		envs, err := b.MetricBatches(hostID, events.Stamp(f.clock.Now()), readings)
		if err != nil {
			t.Fatal(err)
		}
		for _, env := range envs {
			validate(env)
		}
	}
}
