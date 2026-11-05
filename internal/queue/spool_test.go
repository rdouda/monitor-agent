package queue

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/events"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestSpool(t *testing.T, dir string, maxBytes, segmentBytes int64) *Spool {
	t.Helper()
	s, err := Open(Options{
		Directory:     dir,
		MaxBytes:      maxBytes,
		SegmentBytes:  segmentBytes,
		FlushInterval: time.Hour,
		Logger:        testLogger(),
	})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func makeEvent(t *testing.T, typ string, seq uint64, payloadSize int) *events.Envelope {
	t.Helper()
	data := map[string]any{
		"snapshot_id": "0192f3a4-8000-7000-8000-000000000001",
		"entities": []any{map[string]any{
			"id":          "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d0f03",
			"kind":        "host",
			"name":        "test",
			"attributes":  map[string]any{"padding": strings.Repeat("x", payloadSize)},
			"observed_at": "2026-11-04T20:00:00Z",
		}},
	}
	return &events.Envelope{
		SpecVersion:     "1.0",
		ID:              eventID(seq),
		Source:          "urn:agent:6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		Type:            typ,
		Subject:         "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d0f03",
		Time:            events.Stamp(time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)),
		DataContentType: "application/json",
		AgentID:         "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		AgentSeq:        seq,
		AgentRun:        "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		Environment:     "test",
		Data:            data,
	}
}

func eventID(seq uint64) string {
	// A valid-looking UUIDv7; uniqueness is all the spool cares about.
	return "0192f3a4-7c1e-7b52-9d3a-" + leftPad(seq, 12)
}

func leftPad(n uint64, width int) string {
	s := ""
	for i := 0; i < width; i++ {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func send(t *testing.T, s *Spool, env *events.Envelope) {
	t.Helper()
	if err := s.Send(env); err != nil {
		t.Fatalf("send seq %d: %v", env.AgentSeq, err)
	}
}

func seqs(qe []QueuedEvent) []uint64 {
	out := make([]uint64, len(qe))
	for i := range qe {
		out[i] = qe[i].Seq
	}
	return out
}

func equalSeqs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSpoolPriorityDrainOrder(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1<<20, 1<<20)

	send(t, s, makeEvent(t, events.TypeEntityUpsert, 1, 10))
	send(t, s, makeEvent(t, events.TypeMetricBatch, 2, 10))
	send(t, s, makeEvent(t, events.TypeEntityUpsert, 3, 10))
	send(t, s, makeEvent(t, events.TypeLifecycleChanged, 4, 10))
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batch, err := s.ReadBatch(10, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := []uint64{4, 1, 3, 2}
	if got := seqs(batch); !equalSeqs(got, want) {
		t.Fatalf("drain order = %v, want %v", got, want)
	}
	if batch[0].Priority != events.PriorityP0 || batch[1].Priority != events.PriorityP1 || batch[3].Priority != events.PriorityP2 {
		t.Fatalf("unexpected priorities: %+v", batch)
	}

	receipts := make([]Receipt, len(batch))
	for i, qe := range batch {
		receipts[i] = Receipt{Priority: qe.Priority, Seq: qe.Seq}
	}
	if err := s.Resolve(receipts); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if q := s.QueueSnapshot(); q.DepthEvents != 0 {
		t.Fatalf("depth after resolve = %d, want 0", q.DepthEvents)
	}
	if got := segmentFiles(t, dir); len(got) != 0 {
		t.Fatalf("segments remain after resolve: %v", got)
	}
}

func TestSpoolRejectsHeartbeats(t *testing.T) {
	s := newTestSpool(t, t.TempDir(), 1<<20, 1<<20)
	if err := s.Send(makeEvent(t, events.TypeHeartbeat, 1, 0)); err == nil {
		t.Fatal("expected heartbeat to be rejected by the spool")
	}
}

func TestSpoolCursorRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1<<20, 1<<20)
	for i := uint64(1); i <= 5; i++ {
		send(t, s, makeEvent(t, events.TypeEntityUpsert, i, 10))
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	first, err := s.ReadBatch(10, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, want := seqs(first), []uint64{1, 2, 3, 4, 5}; !equalSeqs(got, want) {
		t.Fatalf("initial read = %v, want %v", got, want)
	}
	if err := s.Resolve([]Receipt{
		{Priority: events.PriorityP1, Seq: 1},
		{Priority: events.PriorityP1, Seq: 2},
		{Priority: events.PriorityP1, Seq: 3},
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(Options{
		Directory:     dir,
		MaxBytes:      1 << 20,
		SegmentBytes:  1 << 20,
		FlushInterval: time.Hour,
		Logger:        testLogger(),
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if q := reopened.QueueSnapshot(); q.DepthEvents != 2 {
		t.Fatalf("depth after reopen = %d, want 2", q.DepthEvents)
	}
	batch, err := reopened.ReadBatch(10, 1<<20)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if got, want := seqs(batch), []uint64{4, 5}; !equalSeqs(got, want) {
		t.Fatalf("read after reopen = %v, want %v", got, want)
	}
	if err := reopened.Resolve([]Receipt{
		{Priority: events.PriorityP1, Seq: 4},
		{Priority: events.PriorityP1, Seq: 5},
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := segmentFiles(t, dir); len(got) != 0 {
		t.Fatalf("segments remain: %v", got)
	}
}

func TestSpoolSegmentRotationAndPartialCursor(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1<<20, 900)
	for i := uint64(1); i <= 8; i++ {
		send(t, s, makeEvent(t, events.TypeEntityUpsert, i, 100))
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if files := segmentFiles(t, dir); len(files) < 2 {
		t.Fatalf("expected rotation into multiple segments, got %v", files)
	}

	// Resolve through seq 4, which lies in the middle of the first segment.
	// The event-level cursor must skip entries within that segment.
	if err := s.Resolve([]Receipt{{Priority: events.PriorityP1, Seq: 4}}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	batch, err := s.ReadBatch(3, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, want := seqs(batch), []uint64{5, 6, 7}; !equalSeqs(got, want) {
		t.Fatalf("read after partial resolve = %v, want %v", got, want)
	}
}

func TestSpoolEvictionDropsLowestPriorityFirst(t *testing.T) {
	dir := t.TempDir()
	// Every event lands in its own segment so eviction is deterministic.
	s := newTestSpool(t, dir, 2600, 600)

	for i := uint64(1); i <= 3; i++ {
		send(t, s, makeEvent(t, events.TypeMetricBatch, i, 100))
	}
	for i := uint64(4); i <= 6; i++ {
		send(t, s, makeEvent(t, events.TypeEntityUpsert, i, 100))
	}
	for i := uint64(7); i <= 9; i++ {
		send(t, s, makeEvent(t, events.TypeLifecycleChanged, i, 100))
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	st := s.Stats()
	if st.Dropped[2] == 0 {
		t.Fatalf("expected P2 eviction, stats = %+v", st)
	}
	if st.Dropped[0] != 0 {
		t.Fatalf("P0 must never be evicted while P1/P2 have data, stats = %+v", st)
	}
	if st.Dropped[1] > 0 && st.Dropped[2] != 3 {
		t.Fatalf("P1 evicted before P2 was exhausted, stats = %+v", st)
	}

	// Keep adding P0 events until P0 itself is evicted. At that point P1
	// and P2 must already be gone.
	for i := uint64(10); i <= 60 && s.Stats().Dropped[0] == 0; i++ {
		send(t, s, makeEvent(t, events.TypeLifecycleChanged, i, 100))
		if err := s.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	st = s.Stats()
	if st.Dropped[0] == 0 {
		t.Fatalf("expected P0 eviction once lower priorities were exhausted, stats = %+v", st)
	}
	if st.Dropped[1] != 3 || st.Dropped[2] != 3 {
		t.Fatalf("P0 evicted before P1/P2 were exhausted, stats = %+v", st)
	}
}

func TestSpoolEvictionOfCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1500, 1<<20)

	// Three events do not fit into the byte budget. They are written to one
	// open segment and the whole segment is then evicted.
	for i := uint64(1); i <= 3; i++ {
		send(t, s, makeEvent(t, events.TypeEntityUpsert, i, 10))
	}
	if got := s.Stats().Dropped[1]; got != 3 {
		t.Fatalf("dropped[1] = %d, want 3", got)
	}
	if q := s.QueueSnapshot(); q.DepthEvents != 0 {
		t.Fatalf("depth = %d, want 0", q.DepthEvents)
	}
	if batch, err := s.ReadBatch(10, 1<<20); err != nil || len(batch) != 0 {
		t.Fatalf("read after eviction = %v, %v", seqs(batch), err)
	}

	// The spool must still be usable after dropping its open segment.
	send(t, s, makeEvent(t, events.TypeEntityUpsert, 4, 10))
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	batch, err := s.ReadBatch(10, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, want := seqs(batch), []uint64{4}; !equalSeqs(got, want) {
		t.Fatalf("read = %v, want %v", got, want)
	}
}

func TestSpoolQueueSnapshotCounters(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)
	s, err := Open(Options{
		Directory:     dir,
		MaxBytes:      1 << 20,
		SegmentBytes:  1 << 20,
		FlushInterval: time.Hour,
		Logger:        testLogger(),
		Clock:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	send(t, s, makeEvent(t, events.TypeEntityUpsert, 1, 10))
	send(t, s, makeEvent(t, events.TypeEntityUpsert, 2, 10))
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	s.RecordRateLimited(3)
	if err := s.Resolve([]Receipt{{Priority: events.PriorityP1, Seq: 1, Rejected: true}}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	now = now.Add(30 * time.Second)

	q := s.QueueSnapshot()
	if q.DepthEvents != 1 {
		t.Fatalf("depth = %d, want 1", q.DepthEvents)
	}
	if q.SizeBytes <= 0 {
		t.Fatalf("size = %d, want > 0", q.SizeBytes)
	}
	if q.RejectedTotal != 1 {
		t.Fatalf("rejected = %d, want 1", q.RejectedTotal)
	}
	if q.RateLimitedTotal != 3 {
		t.Fatalf("rate limited = %d, want 3", q.RateLimitedTotal)
	}
	if q.OldestEventAgeSeconds == nil || *q.OldestEventAgeSeconds < 29 {
		t.Fatalf("oldest age = %v, want >= 29", q.OldestEventAgeSeconds)
	}
}

func TestSpoolReadBatchLimits(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1<<20, 1<<20)
	for i := uint64(1); i <= 10; i++ {
		send(t, s, makeEvent(t, events.TypeEntityUpsert, i, 10))
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	batch, err := s.ReadBatch(3, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("got %d events, want 3", len(batch))
	}
	// A byte budget smaller than one event still yields one event so the
	// queue can make progress.
	batch, err = s.ReadBatch(10, 1024)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("got %d events with tight budget, want 1", len(batch))
	}
}

func TestSpoolPersistedEnvelopeIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	s := newTestSpool(t, dir, 1<<20, 1<<20)
	env := makeEvent(t, events.TypeLifecycleChanged, 7, 5)
	send(t, s, env)
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	batch, err := s.ReadBatch(1, 1<<20)
	if err != nil || len(batch) != 1 {
		t.Fatalf("read: %v, %v", batch, err)
	}
	var decoded events.Envelope
	if err := json.Unmarshal(batch[0].Raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ID != env.ID || decoded.AgentSeq != env.AgentSeq || decoded.Type != env.Type {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
	if !strings.Contains(string(batch[0].Raw), `"agentseq":7`) {
		t.Fatalf("raw event does not look like an envelope: %s", batch[0].Raw)
	}
}

func TestSpoolOptionsDefaultsAndConfigWiring(t *testing.T) {
	// Guard against accidentally dropping the config mapping in Phase 2.
	cfg := config.Default()
	if cfg.Reporting.Spool.Directory == "" || cfg.Reporting.Spool.MaxSizeMB <= 0 || cfg.Reporting.Spool.SegmentSizeMB <= 0 {
		t.Fatalf("unexpected spool defaults: %+v", cfg.Reporting.Spool)
	}
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for _, class := range []string{"p0", "p1", "p2"} {
		entries, err := os.ReadDir(filepath.Join(dir, class))
		if err != nil {
			t.Fatalf("read dir: %v", err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "seg-") {
				out = append(out, filepath.Join(class, e.Name()))
			}
		}
	}
	return out
}
