package events

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/internal/schema"
)

const (
	testAgentID = "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55"
	testRunID   = "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f"
	testHostID  = "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03"
	testHostEID = "host:" + testHostID
)

type memSeq struct {
	mu sync.Mutex
	n  uint64
}

func (s *memSeq) Next() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return s.n, nil
}

func testBuilder(maxBytes int) (*Builder, *memSeq) {
	seq := &memSeq{}
	b := NewBuilder(testAgentID, testRunID, "production", seq, maxBytes)
	b.Clock = func() time.Time { return time.Date(2026, 11, 4, 20, 0, 0, 123456789, time.UTC) }
	return b, seq
}

func validate(t *testing.T, s *schema.Set, env *Envelope) {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.ValidateEnvelope(raw); err != nil {
		t.Fatalf("schema validation failed: %v\n%s", err, raw)
	}
}

func TestAllEventTypesValidate(t *testing.T) {
	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := testBuilder(65536)
	now := Stamp(time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC))

	ent := NewEntity(testHostEID, "host", "web-01", map[string]any{"os": "linux"}, now)
	envs, err := b.EntityUpserts(testHostEID, "0192f3a4-8000-7000-8000-000000000001", []Entity{ent})
	if err != nil || len(envs) != 1 {
		t.Fatalf("entity upsert: %v, %d envelopes", err, len(envs))
	}
	validate(t, set, envs[0])

	rel := NewRelationship("rel:0123456789abcdef0123456789abcdef", testHostEID, "runs_on", testHostEID, nil, now)
	envs, err = b.RelationshipUpserts(testHostEID, "0192f3a4-8000-7000-8000-000000000001", []Relationship{rel})
	if err != nil || len(envs) != 1 {
		t.Fatalf("relationship upsert: %v", err)
	}
	validate(t, set, envs[0])

	envs, err = b.RelationshipsRemoved(testHostEID, []string{rel.ID}, RelReasonEntityRemoved, now)
	if err != nil || len(envs) != 1 {
		t.Fatalf("relationship removed: %v", err)
	}
	validate(t, set, envs[0])

	readings := []Reading{{EntityID: testHostEID, Metrics: []Metric{
		Gauge("host.cpu.utilization", 0.42, UnitRatio, nil),
		Counter("host.disk.io.read.bytes", 91827364, UnitBytes, map[string]any{"device": "sda"}),
	}}}
	envs, err = b.MetricBatches(testHostEID, now, readings)
	if err != nil || len(envs) != 1 {
		t.Fatalf("metric batch: %v", err)
	}
	validate(t, set, envs[0])

	prev := "running"
	life, err := b.Lifecycle(Lifecycle{
		EntityID:       testHostEID,
		Kind:           "host",
		Name:           "web-01",
		PreviousStatus: &prev,
		Status:         StatusUp,
		Reason:         ReasonStateReconciled,
		Details:        map[string]any{"exit_code": 1},
		ObservedAt:     now,
	})
	if err != nil {
		t.Fatalf("lifecycle: %v", err)
	}
	validate(t, set, life)

	snap, err := b.SnapshotCompleted(Snapshot{
		Module:             "host",
		SnapshotID:         "0192f3a4-8000-7000-8000-000000000002",
		ScopeEntityID:      testHostEID,
		CoversKinds:        []string{"host"},
		Complete:           true,
		StartedAt:          now,
		CompletedAt:        now,
		EntitiesCount:      1,
		RelationshipsCount: 0,
		Errors:             []SnapshotError{},
	})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	validate(t, set, snap)

	hb, err := b.HeartbeatEvent(testHostEID, Heartbeat{
		Agent: HeartbeatAgent{
			ID: testAgentID, RunID: testRunID, Version: "1.0.0",
			Status: AgentHealthy, StartedAt: now, UptimeSeconds: 60,
		},
		HostID: testHostID,
		Modules: map[string]HeartbeatModule{
			"host": {Status: ModuleHealthy},
		},
		Queue: HeartbeatQueue{
			DepthEvents:  3,
			SizeBytes:    512,
			DroppedTotal: HeartbeatDropped{},
		},
		Reporting: HeartbeatReporting{ConsecutiveFailures: 0},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	validate(t, set, hb)
}

func TestSequenceIncreases(t *testing.T) {
	b, _ := testBuilder(65536)
	now := Now()
	for i := 0; i < 5; i++ {
		envs, err := b.EntityUpserts(testHostEID, "", []Entity{
			NewEntity(testHostEID, "host", "h", nil, now),
		})
		if err != nil {
			t.Fatal(err)
		}
		if envs[0].AgentSeq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", envs[0].AgentSeq, i+1)
		}
	}
}

func TestEntitySplittingByCount(t *testing.T) {
	b, _ := testBuilder(65536)
	now := Now()
	entities := make([]Entity, 0, 450)
	for i := 0; i < 450; i++ {
		entities = append(entities, NewEntity(testHostEID, "host", "h", map[string]any{"i": i}, now))
	}
	envs, err := b.EntityUpserts(testHostEID, "", entities)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 3 {
		t.Fatalf("got %d envelopes, want 3", len(envs))
	}
	total := 0
	for _, e := range envs {
		data := e.Data.(EntityUpsertData)
		if len(data.Entities) > 200 {
			t.Fatalf("chunk has %d entities", len(data.Entities))
		}
		total += len(data.Entities)
	}
	if total != 450 {
		t.Fatalf("lost entities: %d", total)
	}
}

func TestEntitySplittingByBytes(t *testing.T) {
	b, _ := testBuilder(2048)
	now := Now()
	entities := make([]Entity, 0, 40)
	for i := 0; i < 40; i++ {
		entities = append(entities, NewEntity(testHostEID, "host", "h", map[string]any{
			"payload": strings.Repeat("x", 200),
			"i":       i,
		}, now))
	}
	envs, err := b.EntityUpserts(testHostEID, "", entities)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) < 2 {
		t.Fatal("expected splitting")
	}
	total := 0
	for _, e := range envs {
		raw, _ := json.Marshal(e)
		if len(raw) > 2048 {
			t.Fatalf("event is %d bytes", len(raw))
		}
		total += len(e.Data.(EntityUpsertData).Entities)
	}
	if total != 40 {
		t.Fatalf("lost entities: %d", total)
	}
}

func TestOversizedSingleItem(t *testing.T) {
	b, _ := testBuilder(1024)
	ent := NewEntity(testHostEID, "host", "h", map[string]any{"payload": strings.Repeat("x", 4000)}, Now())
	_, err := b.EntityUpserts(testHostEID, "", []Entity{ent})
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("got %v, want ErrEventTooLarge", err)
	}
}

func TestMetricSplitByMetrics(t *testing.T) {
	b, _ := testBuilder(65536)
	metrics := make([]Metric, 0, 130)
	for i := 0; i < 130; i++ {
		metrics = append(metrics, Gauge("host.load.1m", float64(i), UnitCount, nil))
	}
	envs, err := b.MetricBatches(testHostEID, Now(), []Reading{{EntityID: testHostEID, Metrics: metrics}})
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 1 {
		t.Fatalf("got %d envelopes, want 1", len(envs))
	}
	readings := envs[0].Data.(MetricBatchData).Readings
	if len(readings) != 3 {
		t.Fatalf("got %d readings, want 3", len(readings))
	}
	for _, r := range readings {
		if len(r.Metrics) > 64 {
			t.Fatalf("reading has %d metrics", len(r.Metrics))
		}
	}
}

func TestTimestampFormat(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC), `"2026-11-04T20:00:00Z"`},
		{time.Date(2026, 11, 4, 20, 0, 0, 123000000, time.UTC), `"2026-11-04T20:00:00.123Z"`},
		{time.Date(2026, 11, 4, 22, 0, 0, 999999999, time.FixedZone("x", 7200)), `"2026-11-04T20:00:00.999Z"`},
	}
	for _, c := range cases {
		got, _ := json.Marshal(Stamp(c.in))
		if string(got) != c.want {
			t.Errorf("Stamp(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestPriorityOf(t *testing.T) {
	tests := map[string]Priority{
		TypeLifecycleChanged:    PriorityP0,
		TypeRelationshipRemoved: PriorityP0,
		TypeEntityUpsert:        PriorityP1,
		TypeRelationshipUpsert:  PriorityP1,
		TypeSnapshotCompleted:   PriorityP1,
		TypeMetricBatch:         PriorityP2,
		TypeHeartbeat:           PriorityNone,
	}
	for typ, want := range tests {
		if got := PriorityOf(typ); got != want {
			t.Errorf("PriorityOf(%s) = %v, want %v", typ, got, want)
		}
	}
}

func TestNonFiniteMetricRejected(t *testing.T) {
	b, _ := testBuilder(65536)
	_, err := b.MetricBatches(testHostEID, Now(), []Reading{{
		EntityID: testHostEID,
		Metrics:  []Metric{Gauge("host.load.1m", math.NaN(), UnitCount, nil)},
	}})
	if err == nil {
		t.Fatal("NaN metric must be rejected")
	}
}
