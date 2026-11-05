package reporting

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/events"
	"agent/internal/queue"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type senderHarness struct {
	srv    *httptest.Server
	spool  *queue.Spool
	client *Client
	sender *Sender
	dir    string
}

func newSenderHarness(t *testing.T, handler http.HandlerFunc, mutate func(*SenderConfig)) *senderHarness {
	t.Helper()
	dir := t.TempDir()
	credPath := filepath.Join(dir, "credential")
	if err := os.WriteFile(credPath, []byte("cred-initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	spool, err := queue.Open(queue.Options{
		Directory:     filepath.Join(dir, "spool"),
		MaxBytes:      1 << 20,
		SegmentBytes:  4096,
		FlushInterval: time.Hour,
		Logger:        testLogger(t),
	})
	if err != nil {
		srv.Close()
		t.Fatalf("open spool: %v", err)
	}
	client, err := NewClient(ClientConfig{
		BaseURL:             srv.URL,
		EventsEndpoint:      "/api/v1/agents/events",
		EnrollEndpoint:      "/api/v1/agents/enroll",
		CredentialFile:      credPath,
		EnrollmentTokenFile: filepath.Join(dir, "enrollment-token"),
		Timeout:             2 * time.Second,
		TLS:                 config.TLSConfig{MinVersion: "1.2"},
		Compress:            true,
		UserAgent:           "agent/test",
		AgentID:             "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		HostID:              "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d0f03",
		Hostname:            "testhost",
		OS:                  "linux",
		Arch:                "amd64",
		AgentVersion:        "test",
	})
	if err != nil {
		spool.Close()
		srv.Close()
		t.Fatalf("new client: %v", err)
	}
	cfg := SenderConfig{
		Spool:             spool,
		Client:            client,
		Logger:            testLogger(t),
		MaxEventsPerBatch: 8,
		MaxBatchBytes:     1 << 20,
		FlushInterval:     10 * time.Millisecond,
		Retry:             config.RetryConfig{InitialInterval: time.Millisecond, MaxInterval: 10 * time.Millisecond},
		ResyncAfterOutage: time.Hour,
		Rand:              func() float64 { return 0 },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	sender, err := NewSender(cfg)
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	h := &senderHarness{srv: srv, spool: spool, client: client, sender: sender, dir: dir}
	t.Cleanup(func() {
		srv.Close()
		_ = spool.Close()
	})
	return h
}

func (h *senderHarness) send(t *testing.T, seq uint64, typ string) {
	t.Helper()
	if err := h.spool.Send(testEnvelope(seq, typ)); err != nil {
		t.Fatalf("spool send: %v", err)
	}
	h.sender.Wake()
}

func (h *senderHarness) run(t *testing.T) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.sender.Run(ctx) }()
	return cancel, done
}

func stopSender(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sender: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not stop")
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitDepth(t *testing.T, spool *queue.Spool, want int64) {
	t.Helper()
	waitFor(t, "spool depth", 5*time.Second, func() bool {
		return spool.QueueSnapshot().DepthEvents == want
	})
}

func decodeBatch(t *testing.T, r *http.Request) []events.Envelope {
	t.Helper()
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip reader: %v", err)
			return nil
		}
		defer zr.Close()
		reader = zr
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("read body: %v", err)
		return nil
	}
	var envs []events.Envelope
	if err := json.Unmarshal(body, &envs); err != nil {
		t.Errorf("decode batch: %v (%s)", err, body)
		return nil
	}
	return envs
}

func accept(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(events.IngestResponse{Status: "accepted", AcceptedCount: n})
}

func rejectRetryable(w http.ResponseWriter, accepted int, id string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(events.IngestResponse{
		Status:        "partial",
		AcceptedCount: accepted,
		RejectedCount: 1,
		Rejected:      []events.RejectedEvent{{ID: id, Code: "internal", Retryable: true}},
	})
}

func TestSenderDeliversInPriorityOrder(t *testing.T) {
	var mu sync.Mutex
	var order []uint64
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/events" {
			http.NotFound(w, r)
			return
		}
		envs := decodeBatch(t, r)
		mu.Lock()
		for _, env := range envs {
			order = append(order, env.AgentSeq)
		}
		mu.Unlock()
		accept(w, len(envs))
	}, nil)
	h.send(t, 1, events.TypeMetricBatch)
	h.send(t, 2, events.TypeEntityUpsert)
	h.send(t, 3, events.TypeLifecycleChanged)

	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	want := []uint64{3, 2, 1}
	if len(order) != len(want) {
		t.Fatalf("delivered %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("delivered %v, want %v", order, want)
		}
	}
}

func TestSenderHonorsByteAndEventBatchLimits(t *testing.T) {
	var mu sync.Mutex
	var batches []int
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		envs := decodeBatch(t, r)
		mu.Lock()
		batches = append(batches, len(envs))
		mu.Unlock()
		accept(w, len(envs))
	}, func(cfg *SenderConfig) {
		cfg.MaxEventsPerBatch = 3
	})
	for i := uint64(1); i <= 7; i++ {
		h.send(t, i, events.TypeEntityUpsert)
	}
	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	for _, n := range batches {
		if n > 3 {
			t.Fatalf("batch of %d exceeds limit 3: %v", n, batches)
		}
	}
	if len(batches) < 3 {
		t.Fatalf("expected at least 3 batches, got %v", batches)
	}
}

func TestSenderSplitsOn413AndRestoresLimit(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	var delivered int
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		envs := decodeBatch(t, r)
		mu.Lock()
		sizes = append(sizes, len(envs))
		tooBig := len(envs) > 2
		if !tooBig {
			delivered += len(envs)
		}
		mu.Unlock()
		if tooBig {
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
			return
		}
		accept(w, len(envs))
	}, func(cfg *SenderConfig) {
		cfg.MaxEventsPerBatch = 8
	})
	for i := uint64(1); i <= 8; i++ {
		h.send(t, i, events.TypeEntityUpsert)
	}
	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	if delivered != 8 {
		t.Fatalf("delivered %d events, want 8", delivered)
	}
	if len(sizes) == 0 || sizes[0] != 8 {
		t.Fatalf("first batch sizes = %v, want first 8", sizes)
	}
	seenTwo := false
	for _, n := range sizes {
		if n == 2 {
			seenTwo = true
		}
	}
	if !seenTwo {
		t.Fatalf("expected bisected batches of 2, got %v", sizes)
	}
}

func TestSenderBisectsAndDropsPoisonEvent(t *testing.T) {
	var mu sync.Mutex
	delivered := map[uint64]int{}
	poison := eventUUID(5)
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		envs := decodeBatch(t, r)
		for _, env := range envs {
			if env.ID == poison {
				http.Error(w, "bad event", http.StatusUnprocessableEntity)
				return
			}
		}
		mu.Lock()
		for _, env := range envs {
			delivered[env.AgentSeq]++
		}
		mu.Unlock()
		accept(w, len(envs))
	}, nil)
	for i := uint64(1); i <= 8; i++ {
		h.send(t, i, events.TypeEntityUpsert)
	}
	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 7 {
		t.Fatalf("delivered %v, want 7 events", delivered)
	}
	if delivered[5] != 0 {
		t.Fatalf("poison event was delivered: %v", delivered)
	}
	if got := h.spool.QueueSnapshot().RejectedTotal; got != 1 {
		t.Fatalf("rejected total = %d, want 1", got)
	}
}

func TestSenderReenrollsOn401(t *testing.T) {
	var mu sync.Mutex
	enrollCalls := 0
	eventTokens := []string{}
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/agents/enroll":
			mu.Lock()
			enrollCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(events.EnrollResponse{
				AgentID:    "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
				Credential: "cred-new",
			})
		case "/api/v1/agents/events":
			token := r.Header.Get("Authorization")
			envs := decodeBatch(t, r)
			mu.Lock()
			eventTokens = append(eventTokens, token)
			mu.Unlock()
			if token != "Bearer cred-new" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			accept(w, len(envs))
		default:
			http.NotFound(w, r)
		}
	}, nil)
	// The harness client has cred-initial and no enrollment token file; the
	// enroll handler above does not check one.
	// enable re-enrollment by pointing at a token file that exists.
	tokenPath := filepath.Join(h.dir, "enrollment-token")
	if err := os.WriteFile(tokenPath, []byte("enroll-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.client.SetCredential("cred-initial")

	h.send(t, 1, events.TypeEntityUpsert)
	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	if enrollCalls != 1 {
		t.Fatalf("enroll calls = %d, want 1", enrollCalls)
	}
	foundNew := false
	for _, tok := range eventTokens {
		if tok == "Bearer cred-new" {
			foundNew = true
		}
	}
	if !foundNew {
		t.Fatalf("event requests never used the new credential: %v", eventTokens)
	}
	b, err := os.ReadFile(filepath.Join(h.dir, "credential"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "cred-new" {
		t.Fatalf("stored credential = %q", b)
	}
}

func TestSenderRetriesRetryableRejections(t *testing.T) {
	var mu sync.Mutex
	first := true
	delivered := map[uint64]int{}
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		envs := decodeBatch(t, r)
		mu.Lock()
		defer mu.Unlock()
		if first {
			first = false
			for _, env := range envs {
				if env.AgentSeq == 2 {
					for _, accepted := range envs {
						if accepted.AgentSeq != 2 {
							delivered[accepted.AgentSeq]++
						}
					}
					rejectRetryable(w, len(envs)-1, env.ID)
					return
				}
			}
			t.Fatal("expected seq 2 in first batch")
		}
		for _, env := range envs {
			delivered[env.AgentSeq]++
		}
		accept(w, len(envs))
	}, nil)
	for i := uint64(1); i <= 3; i++ {
		h.send(t, i, events.TypeEntityUpsert)
	}
	cancel, done := h.run(t)
	waitDepth(t, h.spool, 0)
	stopSender(t, cancel, done)

	mu.Lock()
	defer mu.Unlock()
	for i := uint64(1); i <= 3; i++ {
		if delivered[i] == 0 {
			t.Fatalf("event %d was never delivered successfully: %v", i, delivered)
		}
	}
	if got := h.spool.QueueSnapshot().RejectedTotal; got != 0 {
		t.Fatalf("retryable rejection was dropped: rejected=%d", got)
	}
}

func TestSenderResyncsAfterOutage(t *testing.T) {
	failUntil := time.Now().Add(120 * time.Millisecond)
	var mu sync.Mutex
	delivered := 0
	resynced := make(chan struct{}, 1)
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if time.Now().Before(failUntil) {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		envs := decodeBatch(t, r)
		mu.Lock()
		delivered += len(envs)
		mu.Unlock()
		accept(w, len(envs))
	}, func(cfg *SenderConfig) {
		cfg.Retry = config.RetryConfig{InitialInterval: 10 * time.Millisecond, MaxInterval: 50 * time.Millisecond}
		cfg.Rand = func() float64 { return 0.5 }
		cfg.ResyncAfterOutage = 30 * time.Millisecond
		cfg.Resync = func() {
			select {
			case resynced <- struct{}{}:
			default:
			}
		}
	})
	h.send(t, 1, events.TypeEntityUpsert)
	cancel, done := h.run(t)
	defer stopSender(t, cancel, done)

	waitDepth(t, h.spool, 0)
	select {
	case <-resynced:
	case <-time.After(3 * time.Second):
		t.Fatal("resync was not triggered after an outage")
	}
	mu.Lock()
	defer mu.Unlock()
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1", delivered)
	}
}

func TestSenderBackoffHonorsRetryAfterAndForbidden(t *testing.T) {
	now := time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)
	h := newSenderHarness(t, func(w http.ResponseWriter, r *http.Request) {
		accept(w, 0)
	}, func(cfg *SenderConfig) {
		cfg.Retry = config.RetryConfig{InitialInterval: time.Second, MaxInterval: time.Minute}
		cfg.Rand = func() float64 { return 0.5 }
		cfg.Now = func() time.Time { return now }
	})
	s := h.sender

	if got := s.backoffDelay(&APIError{Kind: ErrRateLimited, RetryAfter: 7 * time.Second, Retryable: true}); got != 7*time.Second {
		t.Fatalf("retry-after = %v, want 7s", got)
	}
	s.mu.Lock()
	s.failures = 1
	s.mu.Unlock()
	if got := s.backoffDelay(errors.New("x")); got != 500*time.Millisecond {
		t.Fatalf("first backoff = %v, want 500ms", got)
	}
	s.mu.Lock()
	s.failures = 10
	s.mu.Unlock()
	if got := s.backoffDelay(errors.New("x")); got != 30*time.Second {
		t.Fatalf("capped backoff = %v, want 30s", got)
	}

	s.setForbidden()
	if got := s.backoffDelay(errors.New("x")); got != 5*time.Minute {
		t.Fatalf("forbidden delay = %v, want 5m", got)
	}
}
