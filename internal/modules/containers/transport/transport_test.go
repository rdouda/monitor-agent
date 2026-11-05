package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func serveUnix(t *testing.T, handler http.Handler) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "api.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(path)
	})
	return path
}

func TestGetDecodesJSON(t *testing.T) {
	path := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Name":"Podman Engine"}`))
	}))
	c := New(path, 2*time.Second)
	var v VersionResponse
	if err := c.Get(context.Background(), "/version", &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Components) != 0 {
		_ = v
	}
}

func TestReadOnlyTransportRejectsMutations(t *testing.T) {
	requests := 0
	path := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{}`))
	}))
	c := New(path, 2*time.Second)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req, err := http.NewRequest(method, "http://unix/containers/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.HTTP.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if !errors.Is(err, ErrReadOnly) {
			t.Fatalf("%s: err = %v, want ErrReadOnly", method, err)
		}
	}
	if requests != 0 {
		t.Fatalf("server received %d mutating requests", requests)
	}
}

func TestHTTPError(t *testing.T) {
	path := serveUnix(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	c := New(path, 2*time.Second)
	err := c.Get(context.Background(), "/version", &struct{}{})
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusInternalServerError {
		t.Fatalf("err = %v, want HTTPError 500", err)
	}
}
