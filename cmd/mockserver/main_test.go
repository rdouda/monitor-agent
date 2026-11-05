package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"agent/internal/events"
	"agent/internal/schema"

	"github.com/google/uuid"
)

var (
	seqMu sync.Mutex
	seqN  uint64
)

func nextSeq() uint64 {
	seqMu.Lock()
	defer seqMu.Unlock()
	seqN++
	return seqN
}

func entityEvent(t *testing.T) []byte {
	t.Helper()
	b := events.NewBuilder("6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55", uuid.NewString(), "test", seqFunc{}, 65536)
	env, err := b.EntityUpserts("host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03", "", []events.Entity{
		events.NewEntity("host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03", "host", "web-01", map[string]any{"os": "linux"}, events.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env[0])
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type seqFunc struct{}

func (seqFunc) Next() (uint64, error) { return nextSeq(), nil }

func testServer(t *testing.T) *server {
	t.Helper()
	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}
	s := newServer(set)
	s.failStatus = http.StatusInternalServerError
	s.delay = 0
	// Make response serialization fast in tests.
	s.maxStore = 100
	return s
}

func do(t *testing.T, h http.Handler, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIngestDedupeAndValidation(t *testing.T) {
	s := testServer(t)
	h := s.routes()
	ev := entityEvent(t)

	rec := do(t, h, "POST", "/api/v1/agents/events", ev, map[string]string{"Content-Type": "application/cloudevents+json"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp events.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "accepted" || resp.AcceptedCount != 1 || resp.RejectedCount != 0 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// Same event again: accepted but deduplicated.
	rec = do(t, h, "POST", "/api/v1/agents/events", ev, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var stats map[string]any
	rec = do(t, h, "GET", "/admin/stats", nil, nil)
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats["duplicates"].(float64) != 1 {
		t.Fatalf("duplicates = %v", stats["duplicates"])
	}

	// Invalid envelope.
	rec = do(t, h, "POST", "/api/v1/agents/events", []byte(`{"specversion":"1.0","id":"nope"}`), nil)
	var bad events.IngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &bad); err != nil {
		t.Fatal(err)
	}
	if bad.Status != "rejected" || bad.RejectedCount != 1 || bad.Rejected[0].Code != "schema_invalid" {
		t.Fatalf("unexpected rejection response: %+v", bad)
	}
}

func TestBatchAndGzip(t *testing.T) {
	s := testServer(t)
	h := s.routes()
	var buf bytes.Buffer
	items := []json.RawMessage{entityEvent(t), entityEvent(t)}
	raw, _ := json.Marshal(items)
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(raw)
	_ = gz.Close()

	rec := do(t, h, "POST", "/api/v1/agents/events", buf.Bytes(), map[string]string{"Content-Encoding": "gzip"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp events.IngestResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.AcceptedCount != 2 {
		t.Fatalf("accepted = %d", resp.AcceptedCount)
	}
}

func TestFailureInjection(t *testing.T) {
	s := testServer(t)
	h := s.routes()
	body, _ := json.Marshal(map[string]any{"status": 503, "count": 1})
	rec := do(t, h, "POST", "/admin/inject", body, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("inject status = %d", rec.Code)
	}
	rec = do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	rec = do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestEnrollAndAuth(t *testing.T) {
	s := testServer(t)
	s.token = "enroll-me"
	s.requireAuth = true
	h := s.routes()

	reqBody, _ := json.Marshal(events.EnrollRequest{
		AgentID:  "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		HostID:   "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
		Hostname: "web-01", AgentVersion: "1.0.0", OS: "linux", Architecture: "amd64",
	})
	rec := do(t, h, "POST", "/api/v1/agents/enroll", reqBody, map[string]string{"Authorization": "Bearer wrong"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d", rec.Code)
	}
	rec = do(t, h, "POST", "/api/v1/agents/enroll", reqBody, map[string]string{"Authorization": "Bearer enroll-me"})
	if rec.Code != http.StatusOK {
		t.Fatalf("enroll status = %d: %s", rec.Code, rec.Body.String())
	}
	var enrolled events.EnrollResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &enrolled)
	if len(enrolled.Credential) < 32 {
		t.Fatalf("short credential %q", enrolled.Credential)
	}

	// No credential: 401.
	rec = do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	rec = do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), map[string]string{"Authorization": "Bearer " + enrolled.Credential})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRejectType(t *testing.T) {
	s := testServer(t)
	s.rejectType = events.TypeEntityUpsert
	h := s.routes()
	rec := do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), nil)
	var resp events.IngestResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.RejectedCount != 1 || resp.Rejected[0].Retryable {
		t.Fatalf("unexpected: %+v", resp)
	}
}

func TestReadOnlyNoPanic(t *testing.T) {
	s := testServer(t)
	rec := do(t, s.routes(), "GET", "/healthz", nil, nil)
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != 200 || len(body) == 0 {
		t.Fatalf("healthz = %d %q", rec.Code, body)
	}
}

func TestTimestampFromStoredEvent(t *testing.T) {
	s := testServer(t)
	h := s.routes()
	rec := do(t, h, "POST", "/api/v1/agents/events", entityEvent(t), nil)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	rec = do(t, h, "GET", "/admin/stats", nil, nil)
	var stats struct {
		Events []struct {
			Time time.Time `json:"time"`
		} `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if len(stats.Events) != 1 || stats.Events[0].Time.IsZero() {
		t.Fatalf("stored event time missing: %+v", stats)
	}
}
