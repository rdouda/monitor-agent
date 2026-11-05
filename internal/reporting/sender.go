package reporting

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"agent/internal/config"
	"agent/internal/events"
	"agent/internal/queue"
)

const (
	minBackoff             = 100 * time.Millisecond
	maxBackoff             = 60 * time.Second
	forbiddenRetryInterval = 5 * time.Minute
	reenrollInterval       = 5 * time.Minute
)

// SenderConfig configures the delivery loop.
type SenderConfig struct {
	Spool             *queue.Spool
	Client            *Client
	Logger            *slog.Logger
	MaxEventsPerBatch int
	MaxBatchBytes     int
	FlushInterval     time.Duration
	Retry             config.RetryConfig
	ResyncAfterOutage time.Duration
	Resync            func()
	Now               func() time.Time
	Rand              func() float64
}

// Sender drains the spool and delivers batches according to the retry
// policy. It is the only component that advances spool cursors.
type Sender struct {
	spool  *queue.Spool
	client *Client
	log    *slog.Logger
	cfg    SenderConfig
	wake   chan struct{}
	now    func() time.Time
	rand   func() float64

	limitMu   sync.Mutex
	maxEvents int
	maxBytes  int

	mu             sync.Mutex
	lastSuccess    time.Time
	failures       int64
	outageStart    time.Time
	dropsBaseline  [3]int64
	lastReenroll   time.Time
	forbiddenUntil time.Time
	rejectedTotal  int64
	unhealthy      bool

	resyncMu      sync.Mutex
	resyncRunning bool
}

// NewSender constructs a delivery loop.
func NewSender(cfg SenderConfig) (*Sender, error) {
	if cfg.Spool == nil {
		return nil, errors.New("sender: spool is required")
	}
	if cfg.Client == nil {
		return nil, errors.New("sender: client is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxEventsPerBatch <= 0 {
		cfg.MaxEventsPerBatch = 100
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 262144
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Retry.InitialInterval <= 0 {
		cfg.Retry.InitialInterval = time.Second
	}
	if cfg.Retry.MaxInterval <= 0 {
		cfg.Retry.MaxInterval = time.Minute
	}
	if cfg.ResyncAfterOutage <= 0 {
		cfg.ResyncAfterOutage = time.Minute
	}
	s := &Sender{
		spool:     cfg.Spool,
		client:    cfg.Client,
		log:       cfg.Logger,
		cfg:       cfg,
		wake:      make(chan struct{}, 1),
		now:       cfg.Now,
		rand:      cfg.Rand,
		maxEvents: cfg.MaxEventsPerBatch,
		maxBytes:  cfg.MaxBatchBytes,
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.rand == nil {
		s.rand = rand.Float64
	}
	snap := s.spool.QueueSnapshot()
	s.dropsBaseline = snap.Dropped
	return s, nil
}

// Wake asks the sender to attempt delivery immediately.
func (s *Sender) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// LastSuccess implements core.ReportingStats.
func (s *Sender) LastSuccess() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSuccess
}

// ConsecutiveFailures implements core.ReportingStats.
func (s *Sender) ConsecutiveFailures() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

// Unhealthy reports whether delivery has been failing for a while.
func (s *Sender) Unhealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unhealthy
}

// Run blocks until ctx is canceled.
func (s *Sender) Run(ctx context.Context) error {
	s.log.Info("reporting started",
		"events_endpoint", s.client.eventsEP,
		"max_events_per_batch", s.maxEvents,
	)

	// Try to obtain a credential before the first batch. Failure is not
	// fatal: events keep accumulating in the spool and enrollment is retried.
	if err := s.client.EnsureCredential(ctx); err != nil && ctx.Err() == nil {
		s.recordFailure(err)
		s.log.Warn("initial enrollment failed; events will be spooled", "error", err)
	}

	timer := time.NewTimer(s.cfg.FlushInterval)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}

		delivered, err := s.drain(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			s.recordFailure(err)
			delay := s.backoffDelay(err)
			s.log.Warn("delivery failed", "error", err, "retry_in", delay.Round(time.Millisecond), "consecutive_failures", s.ConsecutiveFailures())
			if !s.sleep(ctx, delay) {
				return nil
			}
			continue
		}
		if delivered > 0 {
			// More events may have arrived while the batch was in flight.
			continue
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.cfg.FlushInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-s.wake:
		case <-timer.C:
		}
	}
}

// drain sends batches until the spool is empty or an error occurs.
func (s *Sender) drain(ctx context.Context) (int, error) {
	delivered := 0
	for {
		batch, err := s.spool.ReadBatch(s.currentLimit(), s.currentBytes())
		if err != nil {
			return delivered, fmt.Errorf("read spool: %w", err)
		}
		if len(batch) == 0 {
			return delivered, nil
		}
		if s.client.Credential() == "" {
			if err := s.client.EnsureCredential(ctx); err != nil {
				return delivered, fmt.Errorf("enroll: %w", err)
			}
		}
		if err := s.deliver(ctx, batch); err != nil {
			return delivered, err
		}
		delivered += len(batch)
	}
}

// deliver sends one batch. Permanent batch-level errors are handled by
// splitting the batch or dropping a single poison event.
func (s *Sender) deliver(ctx context.Context, batch []queue.QueuedEvent) error {
	err := s.deliverOnce(ctx, batch)
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrTooLarge):
		s.halveLimit()
		s.log.Warn("batch too large; reducing batch size", "events", len(batch), "new_limit", s.currentLimit())
		if len(batch) > 1 {
			return s.bisect(ctx, batch)
		}
		s.dropEvent(batch[0], "too_large")
		return nil
	case errors.Is(err, ErrBadRequest):
		if len(batch) > 1 {
			return s.bisect(ctx, batch)
		}
		s.dropEvent(batch[0], "bad_request")
		return nil
	default:
		return err
	}
}

func (s *Sender) bisect(ctx context.Context, batch []queue.QueuedEvent) error {
	mid := len(batch) / 2
	if mid == 0 {
		mid = 1
	}
	if err := s.deliver(ctx, batch[:mid]); err != nil {
		return err
	}
	return s.deliver(ctx, batch[mid:])
}

func (s *Sender) deliverOnce(ctx context.Context, batch []queue.QueuedEvent) error {
	raws := make([][]byte, len(batch))
	for i := range batch {
		raws[i] = batch[i].Raw
	}
	resp, err := s.client.PostEvents(ctx, raws)
	if err != nil && errors.Is(err, ErrUnauthorized) {
		if reErr := s.tryReenroll(ctx); reErr != nil {
			return fmt.Errorf("re-enroll after 401: %w", reErr)
		}
		// The rejected batch was not delivered. Retry it once with the
		// fresh credential; the server deduplicates anything it already
		// accepted.
		resp, err = s.client.PostEvents(ctx, raws)
	}
	if err != nil {
		if errors.Is(err, ErrForbidden) {
			s.setForbidden()
		}
		return err
	}
	retryable, err := s.applyResponse(batch, resp)
	if err != nil {
		return err
	}
	s.markSuccess()
	s.restoreLimit()
	if retryable > 0 {
		return &APIError{
			Kind:      ErrTransient,
			Retryable: true,
			Message:   fmt.Sprintf("%d retryable rejected events", retryable),
		}
	}
	return nil
}

// applyResponse resolves the accepted prefix of every priority class. Events
// after the first retryable rejection in a class stay in the spool so the
// cursor never skips an undelivered event.
func (s *Sender) applyResponse(batch []queue.QueuedEvent, resp *events.IngestResponse) (int, error) {
	if resp.AcceptedCount+resp.RejectedCount != len(batch) {
		return 0, &APIError{
			Kind:      ErrInvalidResponse,
			Retryable: true,
			Message:   fmt.Sprintf("counts accepted=%d rejected=%d do not match batch=%d", resp.AcceptedCount, resp.RejectedCount, len(batch)),
		}
	}
	if resp.RejectedCount > 0 && len(resp.Rejected) == 0 {
		return 0, &APIError{
			Kind:      ErrInvalidResponse,
			Retryable: true,
			Message:   "rejected_count > 0 but no rejected events were returned",
		}
	}
	rejects := make(map[string]events.RejectedEvent, len(resp.Rejected))
	for _, r := range resp.Rejected {
		rejects[r.ID] = r
	}

	cut := make(map[events.Priority]int)
	retryable := 0
	for i, qe := range batch {
		if _, done := cut[qe.Priority]; done {
			continue
		}
		if r, ok := rejects[qe.Envelope.ID]; ok && r.Retryable {
			cut[qe.Priority] = i
			retryable++
		}
	}

	receipts := make([]queue.Receipt, 0, len(batch))
	for i, qe := range batch {
		if limit, ok := cut[qe.Priority]; ok && i >= limit {
			continue
		}
		r, rejected := rejects[qe.Envelope.ID]
		if rejected && r.Retryable {
			continue
		}
		receipt := queue.Receipt{Priority: qe.Priority, Seq: qe.Seq}
		if rejected {
			receipt.Rejected = true
			s.mu.Lock()
			s.rejectedTotal++
			s.mu.Unlock()
			s.log.Warn("event rejected permanently",
				"event_id", qe.Envelope.ID,
				"type", qe.Envelope.Type,
				"code", r.Code,
				"message", r.Message,
			)
		}
		receipts = append(receipts, receipt)
	}
	if len(receipts) > 0 {
		if err := s.spool.Resolve(receipts); err != nil {
			return retryable, fmt.Errorf("resolve spool: %w", err)
		}
	}
	return retryable, nil
}

func (s *Sender) dropEvent(qe queue.QueuedEvent, reason string) {
	s.mu.Lock()
	s.rejectedTotal++
	s.mu.Unlock()
	s.log.Error("dropping undeliverable event",
		"event_id", qe.Envelope.ID,
		"type", qe.Envelope.Type,
		"agentseq", qe.Seq,
		"reason", reason,
	)
	if err := s.spool.Resolve([]queue.Receipt{{
		Priority: qe.Priority,
		Seq:      qe.Seq,
		Rejected: true,
	}}); err != nil {
		s.log.Error("resolve dropped event", "error", err)
	}
}

func (s *Sender) tryReenroll(ctx context.Context) error {
	s.mu.Lock()
	last := s.lastReenroll
	s.mu.Unlock()
	if !last.IsZero() && s.now().Sub(last) < reenrollInterval {
		return errors.New("re-enrollment attempted recently")
	}
	s.mu.Lock()
	s.lastReenroll = s.now()
	s.mu.Unlock()
	s.log.Warn("credential rejected; attempting re-enrollment")
	if err := s.client.Enroll(ctx); err != nil {
		return err
	}
	s.log.Info("re-enrollment succeeded")
	return nil
}

func (s *Sender) setForbidden() {
	s.mu.Lock()
	s.forbiddenUntil = s.now().Add(forbiddenRetryInterval)
	s.mu.Unlock()
}

func (s *Sender) recordFailure(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures++
	if s.outageStart.IsZero() {
		s.outageStart = s.now()
	}
	// Treat missing credentials as unhealthy; ordinary transient outages are
	// reflected by consecutive_failures in the heartbeat instead.
	if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) {
		s.unhealthy = true
	}
}

func (s *Sender) markSuccess() {
	now := s.now()
	s.mu.Lock()
	outage := time.Duration(0)
	if !s.outageStart.IsZero() {
		outage = now.Sub(s.outageStart)
	}
	snap := s.spool.QueueSnapshot()
	dropped := snap.Dropped
	needResync := outage >= s.cfg.ResyncAfterOutage || dropped != s.dropsBaseline
	s.lastSuccess = now
	s.failures = 0
	s.outageStart = time.Time{}
	s.unhealthy = false
	if needResync {
		s.dropsBaseline = dropped
	}
	s.mu.Unlock()

	if needResync {
		s.log.Warn("resynchronizing after outage or dropped events", "outage", outage.Round(time.Second))
		s.requestResync()
	}
}

func (s *Sender) requestResync() {
	if s.cfg.Resync == nil {
		return
	}
	s.resyncMu.Lock()
	if s.resyncRunning {
		s.resyncMu.Unlock()
		return
	}
	s.resyncRunning = true
	s.resyncMu.Unlock()
	go func() {
		defer func() {
			s.resyncMu.Lock()
			s.resyncRunning = false
			s.resyncMu.Unlock()
		}()
		s.cfg.Resync()
	}()
}

func (s *Sender) currentLimit() int {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	return s.maxEvents
}

func (s *Sender) currentBytes() int {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	return s.maxBytes
}

func (s *Sender) halveLimit() {
	s.limitMu.Lock()
	if s.maxEvents > 1 {
		s.maxEvents /= 2
		if s.maxEvents < 1 {
			s.maxEvents = 1
		}
	}
	s.limitMu.Unlock()
}

func (s *Sender) restoreLimit() {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	if s.maxEvents >= s.cfg.MaxEventsPerBatch {
		return
	}
	step := s.cfg.MaxEventsPerBatch / 20
	if step < 1 {
		step = 1
	}
	s.maxEvents += step
	if s.maxEvents > s.cfg.MaxEventsPerBatch {
		s.maxEvents = s.cfg.MaxEventsPerBatch
	}
}

// backoffDelay computes full-jitter exponential backoff and honors
// Retry-After.
func (s *Sender) backoffDelay(err error) time.Duration {
	s.mu.Lock()
	until := s.forbiddenUntil
	s.mu.Unlock()
	if !until.IsZero() {
		if d := until.Sub(s.now()); d > 0 {
			return d
		}
	}
	if d := RetryAfter(err); d > 0 {
		if d > time.Hour {
			d = time.Hour
		}
		return d
	}
	s.mu.Lock()
	failures := s.failures
	s.mu.Unlock()
	if failures < 1 {
		failures = 1
	}
	exp := s.cfg.Retry.InitialInterval
	for i := int64(1); i < failures && exp < s.cfg.Retry.MaxInterval; i++ {
		exp *= 2
	}
	if exp > s.cfg.Retry.MaxInterval {
		exp = s.cfg.Retry.MaxInterval
	}
	if exp < minBackoff {
		exp = minBackoff
	}
	d := time.Duration(s.rand() * float64(exp))
	if d < 0 {
		d = 0
	}
	return d
}

func (s *Sender) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
