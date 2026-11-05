package reporting

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent/internal/config"
	"agent/internal/events"
)

// Delivery error classes. Callers branch on these with errors.Is.
var (
	ErrUnauthorized    = errors.New("unauthorized")
	ErrForbidden       = errors.New("forbidden")
	ErrTooLarge        = errors.New("payload too large")
	ErrBadRequest      = errors.New("bad request")
	ErrRateLimited     = errors.New("rate limited")
	ErrTransient       = errors.New("transient server error")
	ErrTransport       = errors.New("transport error")
	ErrInvalidResponse = errors.New("invalid server response")
)

// APIError is the typed error returned by the ingestion and enrollment
// endpoints.
type APIError struct {
	StatusCode int
	Kind       error
	Message    string
	RetryAfter time.Duration
	Retryable  bool
}

func (e *APIError) Error() string {
	msg := e.Kind.Error()
	if e.StatusCode > 0 {
		msg = fmt.Sprintf("%s (http %d)", msg, e.StatusCode)
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg
}

// Unwrap exposes the sentinel class.
func (e *APIError) Unwrap() error { return e.Kind }

// IsRetryable reports whether the same batch may be retried unchanged.
func IsRetryable(err error) bool {
	var api *APIError
	if errors.As(err, &api) {
		return api.Retryable
	}
	return false
}

// RetryAfter returns the server-provided delay, if any.
func RetryAfter(err error) time.Duration {
	var api *APIError
	if errors.As(err, &api) {
		return api.RetryAfter
	}
	return 0
}

// ClientConfig configures the HTTPS ingestion client.
type ClientConfig struct {
	BaseURL             string
	EventsEndpoint      string
	EnrollEndpoint      string
	CredentialFile      string
	EnrollmentTokenFile string
	Timeout             time.Duration
	TLS                 config.TLSConfig
	Compress            bool
	UserAgent           string

	// Identity used by enrollment requests.
	AgentID      string
	HostID       string
	Hostname     string
	OS           string
	Arch         string
	Environment  string
	AgentVersion string

	// HTTPClient overrides the default client. Tests use this to point at
	// an httptest server.
	HTTPClient *http.Client
}

// Client posts batches of CloudEvents and performs enrollment.
type Client struct {
	cfg      ClientConfig
	http     *http.Client
	base     string
	eventsEP string
	enrollEP string

	mu        sync.RWMutex
	cred      string
	credUntil time.Time
}

// NewClient constructs a Client and loads an existing credential file.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("reporting: base_url is required")
	}
	if cfg.EventsEndpoint == "" || cfg.EnrollEndpoint == "" {
		return nil, errors.New("reporting: endpoints are required")
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("reporting: invalid base_url %q", cfg.BaseURL)
	}
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawQuery = ""
	base.Fragment = ""

	c := &Client{
		cfg:      cfg,
		base:     strings.TrimRight(base.String(), "/"),
		eventsEP: cfg.EventsEndpoint,
		enrollEP: cfg.EnrollEndpoint,
	}
	if cfg.Timeout <= 0 {
		c.cfg.Timeout = 10 * time.Second
	}
	if cfg.HTTPClient != nil {
		c.http = cfg.HTTPClient
	} else {
		transport, err := newTransport(cfg)
		if err != nil {
			return nil, err
		}
		c.http = &http.Client{Timeout: c.cfg.Timeout, Transport: transport}
	}
	if cfg.CredentialFile != "" {
		if cred, err := readSecret(cfg.CredentialFile); err == nil {
			c.cred = cred
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read credential: %w", err)
		}
	}
	return c, nil
}

func newTransport(cfg ClientConfig) (*http.Transport, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	switch cfg.TLS.MinVersion {
	case "1.3":
		tlsCfg.MinVersion = tls.VersionTLS13
	case "", "1.2":
	default:
		return nil, fmt.Errorf("reporting: unsupported tls min_version %q", cfg.TLS.MinVersion)
	}
	if cfg.TLS.CAFile != "" {
		pem, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s contains no certificates", cfg.TLS.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.TLS.ClientCertFile != "" || cfg.TLS.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.ClientCertFile, cfg.TLS.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       tlsCfg,
	}, nil
}

// Credential returns the current bearer credential (may be empty).
func (c *Client) Credential() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cred
}

// SetCredential updates the in-memory credential.
func (c *Client) SetCredential(cred string) {
	c.mu.Lock()
	c.cred = strings.TrimSpace(cred)
	c.mu.Unlock()
}

// CredentialExpiresAt returns the enrollment expiry, if the server sent one.
func (c *Client) CredentialExpiresAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.credUntil
}

// EnsureCredential loads the credential file or enrolls when it is missing.
func (c *Client) EnsureCredential(ctx context.Context) error {
	if c.Credential() != "" {
		return nil
	}
	if c.cfg.CredentialFile != "" {
		if cred, err := readSecret(c.cfg.CredentialFile); err == nil && cred != "" {
			c.SetCredential(cred)
			return nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read credential: %w", err)
		}
	}
	return c.Enroll(ctx)
}

// Enroll performs one enrollment request and stores the returned credential.
func (c *Client) Enroll(ctx context.Context) error {
	if c.cfg.EnrollmentTokenFile == "" {
		return errors.New("reporting: no enrollment token file configured")
	}
	token, err := readSecret(c.cfg.EnrollmentTokenFile)
	if err != nil {
		return fmt.Errorf("read enrollment token: %w", err)
	}
	if token == "" {
		return errors.New("reporting: enrollment token is empty")
	}
	req := events.EnrollRequest{
		AgentID:      c.cfg.AgentID,
		HostID:       c.cfg.HostID,
		Hostname:     c.cfg.Hostname,
		AgentVersion: c.cfg.AgentVersion,
		OS:           c.cfg.OS,
		Architecture: c.cfg.Arch,
		Environment:  c.cfg.Environment,
	}
	if req.OS == "" {
		req.OS = "linux"
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+c.enrollEP, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", c.userAgent())

	resp, err := c.do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := readResponse(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return apiErrorFromResponse(resp.StatusCode, raw, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()))
	}
	var er events.EnrollResponse
	if err := json.Unmarshal(raw, &er); err != nil {
		return &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: err.Error()}
	}
	if er.Credential == "" {
		return &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: "empty credential"}
	}
	if er.AgentID != "" && c.cfg.AgentID != "" && er.AgentID != c.cfg.AgentID {
		return &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: "agent_id mismatch"}
	}
	cred := strings.TrimSpace(er.Credential)
	var expires time.Time
	if er.CredentialExpiresAt != nil {
		expires = er.CredentialExpiresAt.Time
	}
	if c.cfg.CredentialFile != "" {
		if err := writeSecret(c.cfg.CredentialFile, cred); err != nil {
			return fmt.Errorf("store credential: %w", err)
		}
	}
	c.mu.Lock()
	c.cred = cred
	c.credUntil = expires
	c.mu.Unlock()
	return nil
}

// PostEvents posts a batch of pre-serialized CloudEvents. It never mutates
// the input and reuses the exact bytes on retries.
func (c *Client) PostEvents(ctx context.Context, raws [][]byte) (*events.IngestResponse, error) {
	if len(raws) == 0 {
		return nil, errors.New("reporting: empty batch")
	}
	body := make([]byte, 0, 2+len(raws))
	body = append(body, '[')
	for i, raw := range raws {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, raw...)
	}
	body = append(body, ']')

	var wire []byte
	compressed := false
	if c.cfg.Compress && len(body) > 1024 {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		wire = buf.Bytes()
		compressed = true
	} else {
		wire = body
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+c.eventsEP, bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/cloudevents-batch+json")
	httpReq.Header.Set("User-Agent", c.userAgent())
	if compressed {
		httpReq.Header.Set("Content-Encoding", "gzip")
	}
	if cred := c.Credential(); cred != "" {
		httpReq.Header.Set("Authorization", "Bearer "+cred)
	}

	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := readResponse(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, apiErrorFromResponse(resp.StatusCode, raw, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()))
	}
	var ir events.IngestResponse
	if err := json.Unmarshal(raw, &ir); err != nil {
		return nil, &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: "decode ingest response: " + err.Error(), Retryable: true}
	}
	if ir.AcceptedCount < 0 || ir.RejectedCount < 0 || ir.AcceptedCount+ir.RejectedCount > len(raws) {
		return nil, &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: "inconsistent ingest counts", Retryable: true}
	}
	switch ir.Status {
	case "accepted", "partial", "rejected":
	default:
		return nil, &APIError{StatusCode: resp.StatusCode, Kind: ErrInvalidResponse, Message: "unknown status " + ir.Status, Retryable: true}
	}
	return &ir, nil
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		kind := ErrTransport
		if errors.Is(err, context.DeadlineExceeded) {
			kind = ErrTransient
		}
		return nil, &APIError{Kind: kind, Message: err.Error(), Retryable: true}
	}
	return resp, nil
}

func (c *Client) userAgent() string {
	if c.cfg.UserAgent != "" {
		return c.cfg.UserAgent
	}
	return "agent/dev"
}

func readResponse(resp *http.Response) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, &APIError{StatusCode: resp.StatusCode, Kind: ErrTransport, Message: "read response: " + err.Error(), Retryable: true}
	}
	return raw, nil
}

func apiErrorFromResponse(status int, body []byte, retryAfter time.Duration) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 512 {
		msg = msg[:512]
	}
	e := &APIError{StatusCode: status, Message: msg, RetryAfter: retryAfter}
	switch {
	case status == http.StatusUnauthorized:
		e.Kind = ErrUnauthorized
	case status == http.StatusForbidden:
		e.Kind = ErrForbidden
	case status == http.StatusRequestEntityTooLarge:
		e.Kind = ErrTooLarge
	case status == http.StatusTooManyRequests:
		e.Kind = ErrRateLimited
		e.Retryable = true
	case status == http.StatusRequestTimeout:
		e.Kind = ErrTransient
		e.Retryable = true
	case status >= 500:
		e.Kind = ErrTransient
		e.Retryable = true
	default:
		e.Kind = ErrBadRequest
	}
	return e
}

// parseRetryAfter accepts delta-seconds and HTTP-date formats.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d > 0 {
			return d
		}
	}
	return 0
}

// readSecret reads a credential or token file and trims surrounding space.
func readSecret(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// writeSecret atomically writes a 0600 file.
func writeSecret(path, value string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".secret-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(value); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
