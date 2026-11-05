package reporting

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/queue"
)

// QueueStats adapts the spool's counters to the runtime heartbeat interface.
type QueueStats struct {
	Spool *queue.Spool
}

// RecordRateLimited forwards module-level lifecycle drops to the spool
// counter reported in heartbeats.
func (q QueueStats) RecordRateLimited(n int64) {
	q.Spool.RecordRateLimited(n)
}

// QueueSnapshot implements core.QueueStats.
func (q QueueStats) QueueSnapshot() core.QueueSnapshot {
	snap := q.Spool.QueueSnapshot()
	return core.QueueSnapshot{
		DepthEvents:           snap.DepthEvents,
		SizeBytes:             snap.SizeBytes,
		OldestEventAgeSeconds: snap.OldestEventAgeSeconds,
		Dropped:               snap.Dropped,
		RejectedTotal:         snap.RejectedTotal,
		RateLimitedTotal:      snap.RateLimitedTotal,
	}
}

// SpoolSink writes events to the disk spool and wakes the sender. It never
// accepts heartbeats; the spool rejects them and heartbeats use a separate
// sink.
type SpoolSink struct {
	Spool  *queue.Spool
	OnSend func()
}

// Send implements core.Sink.
func (s *SpoolSink) Send(env *events.Envelope) error {
	if err := s.Spool.Send(env); err != nil {
		return err
	}
	if s.OnSend != nil {
		s.OnSend()
	}
	return nil
}

// Close flushes buffered events. The spool itself is closed by the caller
// after the sender has stopped.
func (s *SpoolSink) Close() error {
	return s.Spool.Flush()
}

// HeartbeatSink posts heartbeats directly. Heartbeats are never spooled, so a
// failed delivery is logged (at debug level) and dropped.
type HeartbeatSink struct {
	Client  *Client
	Timeout time.Duration
	Log     *slog.Logger
}

// Send implements core.Sink.
func (h *HeartbeatSink) Send(env *events.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := h.Client.PostEvents(ctx, [][]byte{raw}); err != nil && h.Log != nil {
		h.Log.Debug("heartbeat delivery failed", "error", err)
	}
	return nil
}

// Close implements core.Sink.
func (h *HeartbeatSink) Close() error { return nil }
