package reporting

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent/internal/config"
	"agent/internal/events"
)

func testEnvelope(seq uint64, typ string) *events.Envelope {
	return &events.Envelope{
		SpecVersion:     "1.0",
		ID:              eventUUID(seq),
		Source:          "urn:agent:6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		Type:            typ,
		Subject:         "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d0f03",
		Time:            events.Stamp(time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)),
		DataContentType: "application/json",
		AgentID:         "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		AgentSeq:        seq,
		AgentRun:        "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
		Environment:     "test",
		Data:            map[string]any{"snapshot_id": "0192f3a4-8000-7000-8000-000000000001"},
	}
}

func eventUUID(seq uint64) string {
	// Not necessarily RFC-valid, but stable and unique within tests.
	const hex = "0123456789abcdef"
	b := []byte("0192f3a4-7c1e-7b52-9d3a-000000000000")
	for i := len(b) - 1; i >= len(b)-10; i-- {
		b[i] = hex[seq%16]
		seq /= 16
	}
	return string(b)
}

func rawEvents(t *testing.T, envs ...*events.Envelope) [][]byte {
	t.Helper()
	out := make([][]byte, len(envs))
	for i, env := range envs {
		b, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out[i] = b
	}
	return out
}

func testClient(t *testing.T, url string, mutate func(*ClientConfig)) *Client {
	t.Helper()
	cfg := ClientConfig{
		BaseURL:        url,
		EventsEndpoint: "/api/v1/agents/events",
		EnrollEndpoint: "/api/v1/agents/enroll",
		Timeout:        2 * time.Second,
		TLS:            config.TLSConfig{MinVersion: "1.2"},
		Compress:       true,
		UserAgent:      "agent/test",
		AgentID:        "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
		HostID:         "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d0f03",
		Hostname:       "testhost",
		OS:             "linux",
		Arch:           "amd64",
		AgentVersion:   "test",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

func TestClientPostEvents(t *testing.T) {
	var gotEvents []json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/cloudevents-batch+json" {
			t.Errorf("content-type = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer cred-123" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("User-Agent"); got != "agent/test" {
			t.Errorf("user-agent = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &gotEvents); err != nil {
			t.Fatalf("body is not an array: %v (%s)", err, body)
		}
		_ = json.NewEncoder(w).Encode(events.IngestResponse{Status: "accepted", AcceptedCount: len(gotEvents)})
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, nil)
	c.SetCredential("cred-123")
	envs := []*events.Envelope{
		testEnvelope(1, events.TypeEntityUpsert),
		testEnvelope(2, events.TypeMetricBatch),
	}
	resp, err := c.PostEvents(context.Background(), rawEvents(t, envs...))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.AcceptedCount != 2 {
		t.Fatalf("accepted = %d, want 2", resp.AcceptedCount)
	}
	if len(gotEvents) != 2 {
		t.Fatalf("server saw %d events", len(gotEvents))
	}
}

func TestClientGzipAboveThreshold(t *testing.T) {
	var encoding string
	var decoded []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		var reader io.Reader = r.Body
		if encoding == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Fatalf("gzip reader: %v", err)
			}
			defer zr.Close()
			reader = zr
		}
		decoded, _ = io.ReadAll(reader)
		_ = json.NewEncoder(w).Encode(events.IngestResponse{Status: "accepted", AcceptedCount: 1})
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, nil)
	env := testEnvelope(1, events.TypeEntityUpsert)
	env.Data = map[string]any{"padding": strings.Repeat("x", 4096)}
	if _, err := c.PostEvents(context.Background(), rawEvents(t, env)); err != nil {
		t.Fatalf("post: %v", err)
	}
	if encoding != "gzip" {
		t.Fatalf("content-encoding = %q, want gzip", encoding)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(decoded, &arr); err != nil || len(arr) != 1 {
		t.Fatalf("decoded body: %v, %v", err, decoded)
	}
}

func TestClientErrorClasses(t *testing.T) {
	tests := []struct {
		status     int
		retryAfter string
		want       error
		retryable  bool
	}{
		{http.StatusUnauthorized, "", ErrUnauthorized, false},
		{http.StatusForbidden, "", ErrForbidden, false},
		{http.StatusRequestEntityTooLarge, "", ErrTooLarge, false},
		{http.StatusBadRequest, "", ErrBadRequest, false},
		{http.StatusUnprocessableEntity, "", ErrBadRequest, false},
		{http.StatusTooManyRequests, "7", ErrRateLimited, true},
		{http.StatusInternalServerError, "", ErrTransient, true},
		{http.StatusBadGateway, "", ErrTransient, true},
	}
	for _, tc := range tests {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				http.Error(w, "nope", tc.status)
			}))
			defer srv.Close()
			c := testClient(t, srv.URL, nil)
			_, err := c.PostEvents(context.Background(), rawEvents(t, testEnvelope(1, events.TypeEntityUpsert)))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want class %v", err, tc.want)
			}
			if got := IsRetryable(err); got != tc.retryable {
				t.Fatalf("retryable = %v, want %v", got, tc.retryable)
			}
			if tc.retryAfter != "" {
				if got := RetryAfter(err); got != 7*time.Second {
					t.Fatalf("retry-after = %v, want 7s", got)
				}
			}
		})
	}
}

func TestClientTransportErrorIsRetryable(t *testing.T) {
	c := testClient(t, "http://127.0.0.1:1", nil)
	_, err := c.PostEvents(context.Background(), rawEvents(t, testEnvelope(1, events.TypeEntityUpsert)))
	if err == nil {
		t.Fatal("expected error")
	}
	if !IsRetryable(err) || !errors.Is(err, ErrTransport) {
		t.Fatalf("error = %v, want retryable transport error", err)
	}
}

func TestClientRejectsInconsistentResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(events.IngestResponse{Status: "accepted", AcceptedCount: 10})
	}))
	defer srv.Close()
	c := testClient(t, srv.URL, nil)
	_, err := c.PostEvents(context.Background(), rawEvents(t, testEnvelope(1, events.TypeEntityUpsert)))
	if !errors.Is(err, ErrInvalidResponse) || !IsRetryable(err) {
		t.Fatalf("error = %v, want retryable invalid response", err)
	}
}

func TestClientEnrollStoresCredential(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "credential")
	tokenPath := filepath.Join(dir, "enrollment-token")
	if err := os.WriteFile(tokenPath, []byte("enroll-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer enroll-token" {
			t.Errorf("authorization = %q", got)
		}
		var req events.EnrollRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.Hostname != "testhost" || req.AgentID == "" || req.HostID == "" {
			t.Errorf("unexpected enroll request: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(events.EnrollResponse{
			AgentID:    req.AgentID,
			Credential: "issued-credential-value-1234567890",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, func(cfg *ClientConfig) {
		cfg.CredentialFile = credPath
		cfg.EnrollmentTokenFile = tokenPath
	})
	if err := c.EnsureCredential(context.Background()); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if got := c.Credential(); got != "issued-credential-value-1234567890" {
		t.Fatalf("credential = %q", got)
	}
	b, err := os.ReadFile(credPath)
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if string(b) != "issued-credential-value-1234567890" {
		t.Fatalf("stored credential = %q", b)
	}
	info, err := os.Stat(credPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}
}

func TestClientEnsureCredentialLoadsExistingFile(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "credential")
	if err := os.WriteFile(credPath, []byte("preexisting\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := testClient(t, "http://127.0.0.1:1", func(cfg *ClientConfig) {
		cfg.CredentialFile = credPath
	})
	if err := c.EnsureCredential(context.Background()); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if got := c.Credential(); got != "preexisting" {
		t.Fatalf("credential = %q", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 4, 20, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("7", now); got != 7*time.Second {
		t.Fatalf("seconds = %v", got)
	}
	later := now.Add(90 * time.Second).Format(http.TimeFormat)
	if got := parseRetryAfter(later, now); got != 90*time.Second {
		t.Fatalf("http date = %v", got)
	}
	if got := parseRetryAfter("garbage", now); got != 0 {
		t.Fatalf("garbage = %v", got)
	}
}
