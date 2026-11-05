// Command mockserver is a development ingestion API. It validates every
// CloudEvent against schemas/v1, deduplicates by event ID and
// (agentid, agentseq), and can inject failures on demand.
package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"agent/internal/events"
	"agent/internal/schema"
)

const unknownID = "00000000-0000-0000-0000-000000000000"

type storedEvent struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	AgentID string          `json:"agentid"`
	Seq     uint64          `json:"agentseq"`
	Time    time.Time       `json:"time"`
	Raw     json.RawMessage `json:"-"`
}

type server struct {
	set         *schema.Set
	token       string
	requireAuth bool
	maxStore    int
	delay       time.Duration
	rejectHTTP  int

	mu            sync.Mutex
	seenIDs       map[string]struct{}
	seenSeq       map[string]struct{}
	accepted      int64
	rejected      int64
	duplicates    int64
	stored        []storedEvent
	credentials   map[string]struct{}
	failStatus    int
	failRemaining int
	rejectType    string
}

func newServer(set *schema.Set) *server {
	return &server{
		set:         set,
		seenIDs:     map[string]struct{}{},
		seenSeq:     map[string]struct{}{},
		credentials: map[string]struct{}{},
		maxStore:    10000,
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /api/v1/agents/enroll", s.handleEnroll)
	mux.HandleFunc("POST /api/v1/agents/events", s.handleEvents)
	mux.HandleFunc("GET /admin/stats", s.handleStats)
	mux.HandleFunc("POST /admin/reset", s.handleReset)
	mux.HandleFunc("POST /admin/inject", s.handleInject)
	return mux
}

func (s *server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if s.token != "" && !bearerOK(r, s.token) {
		http.Error(w, "invalid enrollment token", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if err := s.set.ValidateValue(body, "enroll-request.schema.json"); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	var req events.EnrollRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	cred, err := newCredential()
	if err != nil {
		http.Error(w, "credential generation failed", http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	s.credentials[cred] = struct{}{}
	s.mu.Unlock()
	resp := events.EnrollResponse{AgentID: req.AgentID, Credential: cred}
	slog.Info("agent enrolled", "agent_id", req.AgentID, "hostname", req.Hostname)
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	if s.failRemaining > 0 {
		s.failRemaining--
		status := s.failStatus
		s.mu.Unlock()
		http.Error(w, "injected failure", status)
		return
	}
	rejectType := s.rejectType
	s.mu.Unlock()

	if s.requireAuth {
		auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		s.mu.Lock()
		_, ok := s.credentials[auth]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	body, err := readMaybeGzip(w, r)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	items, err := splitEvents(body)
	if err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp := events.IngestResponse{}
	var acceptedRaw []json.RawMessage
	for _, raw := range items {
		rej := s.validateAndDedupe(raw, rejectType)
		if rej != nil {
			resp.Rejected = append(resp.Rejected, *rej)
			continue
		}
		resp.AcceptedCount++
		acceptedRaw = append(acceptedRaw, raw)
	}
	resp.RejectedCount = len(resp.Rejected)
	switch {
	case resp.RejectedCount == 0:
		resp.Status = "accepted"
	case resp.AcceptedCount == 0:
		resp.Status = "rejected"
	default:
		resp.Status = "partial"
	}

	s.mu.Lock()
	s.accepted += int64(resp.AcceptedCount)
	s.rejected += int64(resp.RejectedCount)
	for _, raw := range acceptedRaw {
		var ev storedEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		ev.Raw = append(json.RawMessage(nil), raw...)
		if s.maxStore <= 0 || len(s.stored) < s.maxStore {
			s.stored = append(s.stored, ev)
		}
	}
	s.mu.Unlock()

	code := http.StatusOK
	if resp.RejectedCount > 0 && s.rejectHTTP > 0 {
		code = s.rejectHTTP
	}
	writeJSON(w, code, resp)
}

func (s *server) validateAndDedupe(raw json.RawMessage, rejectType string) *events.RejectedEvent {
	if err := s.set.ValidateEnvelope(raw); err != nil {
		id := unknownID
		var head struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &head) == nil && head.ID != "" {
			id = head.ID
		}
		return &events.RejectedEvent{
			ID:        id,
			Code:      "schema_invalid",
			Message:   truncate(err.Error(), 500),
			Retryable: false,
		}
	}
	var ev storedEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return &events.RejectedEvent{ID: unknownID, Code: "schema_invalid", Message: err.Error()}
	}
	if rejectType != "" && ev.Type == rejectType {
		return &events.RejectedEvent{
			ID: ev.ID, Code: "unknown_type", Message: "injected rejection of " + ev.Type, Retryable: false,
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seqKey := fmt.Sprintf("%s|%d", ev.AgentID, ev.Seq)
	_, dupID := s.seenIDs[ev.ID]
	_, dupSeq := s.seenSeq[seqKey]
	if dupID || dupSeq {
		s.duplicates++
		return nil
	}
	s.seenIDs[ev.ID] = struct{}{}
	s.seenSeq[seqKey] = struct{}{}
	return nil
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":   s.accepted,
		"rejected":   s.rejected,
		"duplicates": s.duplicates,
		"stored":     len(s.stored),
		"events":     s.stored,
	})
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.seenIDs = map[string]struct{}{}
	s.seenSeq = map[string]struct{}{}
	s.accepted = 0
	s.rejected = 0
	s.duplicates = 0
	s.stored = nil
	s.failRemaining = 0
	s.rejectType = ""
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleInject(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Status int    `json:"status"`
		Count  int    `json:"count"`
		Type   string `json:"type"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&in); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if in.Status >= 400 && in.Count > 0 {
		s.failStatus = in.Status
		s.failRemaining = in.Count
	}
	if in.Type != "" {
		s.rejectType = in.Type
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func splitEvents(body []byte) ([]json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, fmt.Errorf("empty body")
	}
	if trimmed[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, err
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("empty batch")
		}
		return items, nil
	}
	if trimmed[0] == '{' {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(body, &obj); err != nil {
			return nil, err
		}
		return []json.RawMessage{json.RawMessage(body)}, nil
	}
	return nil, fmt.Errorf("expected a JSON object or array")
}

func readMaybeGzip(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	var reader io.Reader = http.MaxBytesReader(w, r.Body, 64<<20)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}
	return io.ReadAll(reader)
}

func bearerOK(r *http.Request, want string) bool {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == want
}

func newCredential() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func main() {
	var (
		addr        = flag.String("addr", ":8080", "listen address")
		schemasDir  = flag.String("schemas", "", "schema directory (default: embedded)")
		token       = flag.String("token", "", "required enrollment token (empty: accept any)")
		requireAuth = flag.Bool("require-auth", false, "require a credential issued by /enroll for event delivery")
		failStatus  = flag.Int("fail-status", 500, "status code used by -fail-count")
		failCount   = flag.Int("fail-count", 0, "fail the first N event requests")
		rejectType  = flag.String("reject-type", "", "reject events of this type with a non-retryable error")
		rejectHTTP  = flag.Int("reject-http", 0, "HTTP status for requests containing rejected events")
		delay       = flag.Duration("delay", 0, "artificial delay per event request")
		maxStore    = flag.Int("max-store", 10000, "maximum events kept for /admin/stats")
		tlsCert     = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS)")
		tlsKey      = flag.String("tls-key", "", "TLS private key file")
	)
	flag.Parse()

	var (
		set *schema.Set
		err error
	)
	if *schemasDir != "" {
		set, err = schema.LoadDir(*schemasDir)
	} else {
		set, err = schema.Load()
	}
	if err != nil {
		slog.Error("load schemas", "error", err)
		os.Exit(1)
	}

	s := newServer(set)
	s.token = *token
	s.requireAuth = *requireAuth
	s.failStatus = *failStatus
	s.failRemaining = *failCount
	s.rejectType = *rejectType
	s.rejectHTTP = *rejectHTTP
	s.delay = *delay
	s.maxStore = *maxStore

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		scheme := "http"
		if *tlsCert != "" {
			scheme = "https"
		}
		slog.Info("mock ingestion API listening", "addr", *addr, "scheme", scheme)
		var err error
		if *tlsCert != "" {
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	slog.Info("stopped")
}
