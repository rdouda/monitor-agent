// Package transport is a minimal, strictly read-only HTTP client for
// container runtime unix sockets.
//
// Docker and Podman both expose an HTTP API over a unix socket. The agent
// only ever needs GET requests, so the transport rejects every other method.
// This is the code-level enforcement of the read-only guarantee in AGENTS.md
// §10.
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// MaxResponseBytes bounds a single non-streaming API response.
const MaxResponseBytes = 64 << 20

// ErrReadOnly is returned when a caller attempts a mutating request.
var ErrReadOnly = errors.New("container runtime transport is read-only; only GET and HEAD are allowed")

// Client is an HTTP client bound to one unix socket.
type Client struct {
	SocketPath string
	HTTP       *http.Client
}

// New creates a client for socketPath. A zero timeout means no overall
// request deadline (used for streaming event endpoints).
func New(socketPath string, timeout time.Duration) *Client {
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	base := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
		MaxIdleConns:        4,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	return &Client{
		SocketPath: socketPath,
		HTTP: &http.Client{
			Transport: readOnlyTransport{base: base},
			Timeout:   timeout,
		},
	}
}

// readOnlyTransport rejects mutating methods before they reach the socket.
type readOnlyTransport struct {
	base http.RoundTripper
}

func (t readOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return nil, fmt.Errorf("%w: %s %s", ErrReadOnly, req.Method, req.URL.Path)
	}
	return t.base.RoundTrip(req)
}

// Get performs a GET request and decodes the JSON body into out.
func (c *Client) Get(ctx context.Context, path string, out any) error {
	body, err := c.Open(ctx, path)
	if err != nil {
		return err
	}
	defer body.Close()
	dec := json.NewDecoder(io.LimitReader(body, MaxResponseBytes))
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// Open performs a GET request and returns the response body. The caller must
// close it.
func (c *Client) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix"+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, &HTTPError{Path: path, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	return resp.Body, nil
}

// HTTPError is a non-2xx runtime API response.
type HTTPError struct {
	Path       string
	StatusCode int
	Status     string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("GET %s: %s", e.Path, e.Status)
}

// VersionResponse is the runtime's /version payload. Docker and Podman both
// answer this endpoint, but the component list identifies the runtime.
type VersionResponse struct {
	Version       string `json:"Version"`
	APIVersion    string `json:"ApiVersion"`
	MinAPIVersion string `json:"MinAPIVersion"`
	GoVersion     string `json:"GoVersion"`
	Components    []struct {
		Name    string            `json:"Name"`
		Version string            `json:"Version"`
		Details map[string]string `json:"Details"`
	} `json:"Components"`
	Platform struct {
		Name string `json:"Name"`
	} `json:"Platform"`
}

// Podman reports a component named "Podman Engine" (or, on some builds,
// includes "podman" in the component version). The socket path is never used
// to decide: podman-docker symlinks Docker sockets to Podman.
func (v VersionResponse) Podman() bool {
	for _, c := range v.Components {
		name := strings.ToLower(c.Name)
		version := strings.ToLower(c.Version)
		if strings.Contains(name, "podman") || strings.Contains(version, "podman") {
			return true
		}
	}
	return false
}

// GetVersion fetches /version from the socket.
func (c *Client) GetVersion(ctx context.Context) (VersionResponse, error) {
	var v VersionResponse
	if err := c.Get(ctx, "/version", &v); err != nil {
		return VersionResponse{}, err
	}
	return v, nil
}
