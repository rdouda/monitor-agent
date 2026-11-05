package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/pem"
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
	"agent/internal/schema"
)

func TestHTTPRunEndToEnd(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	seen := map[string]int{}
	heartbeat := make(chan struct{}, 1)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/events" {
			http.NotFound(w, r)
			return
		}
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("gzip: %v", err)
				http.Error(w, "bad gzip", http.StatusBadRequest)
				return
			}
			defer zr.Close()
			reader = zr
		}
		var raws []json.RawMessage
		if err := json.NewDecoder(reader).Decode(&raws); err != nil {
			t.Errorf("decode batch: %v", err)
			http.Error(w, "bad batch", http.StatusBadRequest)
			return
		}
		for _, raw := range raws {
			if err := set.ValidateEnvelope(raw); err != nil {
				t.Errorf("invalid event: %v\n%s", err, raw)
				continue
			}
			var head struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(raw, &head)
			mu.Lock()
			seen[head.Type]++
			mu.Unlock()
			if head.Type == "agent.heartbeat.v1" {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":         "accepted",
			"accepted_count": len(raws),
			"rejected_count": 0,
		})
	}))
	defer srv.Close()

	state := t.TempDir()
	credPath := filepath.Join(state, "credential")
	if err := os.WriteFile(credPath, []byte("test-credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(state, "ca.pem")
	cert := srv.Certificate()
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Agent.Environment = "test"
	cfg.Paths.StateDir = state
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.SysfsRoot = filepath.Join(root, "sysfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	cfg.Server.BaseURL = srv.URL
	cfg.Server.CredentialFile = credPath
	cfg.Server.EnrollmentTokenFile = filepath.Join(state, "enrollment-token")
	cfg.Server.Timeout = 3 * time.Second
	cfg.Server.TLS.CAFile = caPath
	cfg.Reporting.Sink = "http"
	cfg.Reporting.FlushInterval = 50 * time.Millisecond
	cfg.Reporting.Spool.Directory = filepath.Join(state, "spool")
	cfg.Reporting.ResyncAfterOutage = time.Minute
	cfg.Logging.Level = "error"

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	id, seq, err := loadIdentity(cfg, log)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- runHTTP(ctx, "", cfg, id, seq, nil, log) }()

	waitFor(t, "heartbeat", 10*time.Second, func() bool {
		select {
		case <-heartbeat:
			return true
		default:
			return false
		}
	})
	waitFor(t, "host inventory and metrics", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["agent.entity.upsert.v1"] > 0 &&
			seen["agent.snapshot.completed.v1"] > 0 &&
			seen["agent.metric.batch.v1"] > 0
	})

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runHTTP exit = %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runHTTP did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if seen["agent.heartbeat.v1"] == 0 {
		t.Fatal("no heartbeat was delivered")
	}
	if seen["agent.entity.upsert.v1"] == 0 {
		t.Fatal("no entity was delivered")
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHTTPRunEnrollsWhenCredentialMissing(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	set, err := schema.Load()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	enrolled := false
	eventsAccepted := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/agents/enroll":
			if got := r.Header.Get("Authorization"); got != "Bearer enroll-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			var req struct {
				AgentID  string `json:"agent_id"`
				Hostname string `json:"hostname"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			mu.Lock()
			enrolled = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"agent_id":   req.AgentID,
				"credential": "cred-from-enroll-1234567890",
			})
		case "/api/v1/agents/events":
			if got := r.Header.Get("Authorization"); got != "Bearer cred-from-enroll-1234567890" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			reader, closeReader := maybeGzip(t, r)
			defer closeReader()
			var raws []json.RawMessage
			if err := json.NewDecoder(reader).Decode(&raws); err != nil {
				http.Error(w, "bad batch", http.StatusBadRequest)
				return
			}
			for _, raw := range raws {
				if err := set.ValidateEnvelope(raw); err != nil {
					t.Errorf("invalid event: %v", err)
				}
			}
			mu.Lock()
			eventsAccepted += len(raws)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":         "accepted",
				"accepted_count": len(raws),
				"rejected_count": 0,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	state := t.TempDir()
	tokenPath := filepath.Join(state, "enrollment-token")
	if err := os.WriteFile(tokenPath, []byte("enroll-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	credPath := filepath.Join(state, "credential")
	caPath := filepath.Join(state, "ca.pem")
	cert := srv.Certificate()
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Agent.Environment = "test"
	cfg.Paths.StateDir = state
	cfg.Paths.ProcfsRoot = filepath.Join(root, "procfs")
	cfg.Paths.SysfsRoot = filepath.Join(root, "sysfs")
	cfg.Paths.HostfsRoot = filepath.Join(root, "hostfs")
	cfg.Server.BaseURL = srv.URL
	cfg.Server.CredentialFile = credPath
	cfg.Server.EnrollmentTokenFile = tokenPath
	cfg.Server.Timeout = 3 * time.Second
	cfg.Server.TLS.CAFile = caPath
	cfg.Reporting.Sink = "http"
	cfg.Reporting.FlushInterval = 50 * time.Millisecond
	cfg.Reporting.Spool.Directory = filepath.Join(state, "spool")
	cfg.Logging.Level = "error"

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	id, seq, err := loadIdentity(cfg, log)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- runHTTP(ctx, "", cfg, id, seq, nil, log) }()

	waitFor(t, "enrollment and event delivery", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return enrolled && eventsAccepted > 0
	})
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runHTTP exit = %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runHTTP did not stop")
	}
	b, err := os.ReadFile(credPath)
	if err != nil {
		t.Fatalf("read credential: %v", err)
	}
	if string(b) != "cred-from-enroll-1234567890" {
		t.Fatalf("credential = %q", b)
	}
}

func maybeGzip(t *testing.T, r *http.Request) (io.Reader, func()) {
	t.Helper()
	if r.Header.Get("Content-Encoding") != "gzip" {
		return r.Body, func() {}
	}
	zr, err := gzip.NewReader(r.Body)
	if err != nil {
		t.Errorf("gzip: %v", err)
		return r.Body, func() {}
	}
	return zr, func() { _ = zr.Close() }
}
