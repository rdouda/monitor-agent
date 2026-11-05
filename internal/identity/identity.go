// Package identity owns the persistent agent and host identities.
package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// NamespaceHost is the fixed UUIDv5 namespace for host IDs. It is part of the
// wire contract: changing it changes every future host_id.
var NamespaceHost = uuid.MustParse("7d9f4d8a-3f6b-5c2e-9a10-2f8b6c4d1e70")

const (
	identityFile = "identity.json"
	stateDirMode = 0o700
	fileMode     = 0o600
)

// Identity is the persistent identity of one agent installation.
type Identity struct {
	AgentID     string    `json:"agent_id"`
	HostID      string    `json:"host_id"`
	Fingerprint string    `json:"fingerprint"`
	CreatedAt   time.Time `json:"created_at"`

	// AgentRunID is generated fresh for every process lifetime and is not
	// persisted.
	AgentRunID string `json:"-"`
	// CloneDetected is true when a changed fingerprint forced new IDs.
	CloneDetected bool `json:"-"`
}

// Load reads the stored identity without writing anything. It is used by
// `agent once`. When no identity exists (or the stored fingerprint no longer
// matches), an ephemeral in-memory identity is returned. Every call assigns a
// fresh AgentRunID.
func Load(stateDir, hostfsRoot, sysfsRoot string) (Identity, error) {
	parts := readFingerprintParts(hostfsRoot, sysfsRoot)
	fp := fingerprint(parts.machineID, parts.productUUID)
	stored, err := readIdentity(filepath.Join(stateDir, identityFile))
	switch {
	case err == nil:
		if stored.Fingerprint != fp {
			stored = newIdentity(parts.machineID, fp, true)
		}
	case errors.Is(err, os.ErrNotExist), os.IsPermission(err):
		// once may run as an unprivileged user against /var/lib/agent; use
		// an ephemeral identity rather than failing the read-only scan.
		stored = newIdentity(parts.machineID, fp, false)
	default:
		return Identity{}, err
	}
	stored.AgentRunID = uuid.NewString()
	return stored, nil
}

// Ephemeral creates an in-memory identity without touching the state
// directory.
func Ephemeral(hostfsRoot, sysfsRoot string) Identity {
	parts := readFingerprintParts(hostfsRoot, sysfsRoot)
	fp := fingerprint(parts.machineID, parts.productUUID)
	id := newIdentity(parts.machineID, fp, false)
	id.AgentRunID = uuid.NewString()
	return id
}

// LoadOrCreate loads the stored identity or creates a new one. hostfsRoot is
// the prefix for /etc/machine-id and DMI files; sysfsRoot is the prefix for
// /sys/class/dmi/id.
func LoadOrCreate(stateDir, hostfsRoot, sysfsRoot string) (Identity, error) {
	parts := readFingerprintParts(hostfsRoot, sysfsRoot)
	fp := fingerprint(parts.machineID, parts.productUUID)

	if err := os.MkdirAll(stateDir, stateDirMode); err != nil {
		return Identity{}, fmt.Errorf("create state dir: %w", err)
	}
	if err := os.Chmod(stateDir, stateDirMode); err != nil {
		return Identity{}, fmt.Errorf("chmod state dir: %w", err)
	}

	path := filepath.Join(stateDir, identityFile)
	stored, err := readIdentity(path)
	switch {
	case err == nil:
		if stored.Fingerprint != fp {
			stored = newIdentity(parts.machineID, fp, true)
			if err := writeIdentity(path, stored); err != nil {
				return Identity{}, err
			}
		}
	case errors.Is(err, os.ErrNotExist):
		stored = newIdentity(parts.machineID, fp, false)
		if err := writeIdentity(path, stored); err != nil {
			return Identity{}, err
		}
	default:
		return Identity{}, err
	}
	stored.AgentRunID = uuid.NewString()
	return stored, nil
}

type fingerprintParts struct {
	machineID   string
	productUUID string
}

func readFingerprintParts(hostfsRoot, sysfsRoot string) fingerprintParts {
	return fingerprintParts{
		machineID:   readTrimmed(filepath.Join(hostfsRoot, "etc/machine-id")),
		productUUID: normalizeDMIToken(readTrimmed(filepath.Join(sysfsRoot, "class/dmi/id/product_uuid"))),
	}
}

func readTrimmed(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func normalizeDMIToken(s string) string {
	switch strings.ToLower(s) {
	case "", "none", "not specified", "unknown", "to be filled by o.e.m.":
		return ""
	}
	return strings.ToLower(s)
}

func fingerprint(machineID, productUUID string) string {
	sum := sha256.Sum256([]byte(machineID + "|" + productUUID))
	return hex.EncodeToString(sum[:])
}

func newIdentity(machineID, fp string, cloned bool) Identity {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return Identity{
		AgentID:       uuid.NewString(),
		HostID:        newHostID(machineID, cloned),
		Fingerprint:   fp,
		CreatedAt:     now,
		CloneDetected: cloned,
	}
}

func newHostID(machineID string, cloned bool) string {
	if machineID != "" && !cloned {
		return uuid.NewSHA1(NamespaceHost, []byte(machineID)).String()
	}
	return uuid.NewString()
}

func readIdentity(path string) (Identity, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Identity{}, err
	}
	var id Identity
	if err := json.Unmarshal(b, &id); err != nil {
		return Identity{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if _, err := uuid.Parse(id.AgentID); err != nil {
		return Identity{}, fmt.Errorf("%s: invalid agent_id: %w", path, err)
	}
	if _, err := uuid.Parse(id.HostID); err != nil {
		return Identity{}, fmt.Errorf("%s: invalid host_id: %w", path, err)
	}
	if id.CreatedAt.IsZero() {
		return Identity{}, fmt.Errorf("%s: missing created_at", path)
	}
	return id, nil
}

func writeIdentity(path string, id Identity) error {
	b, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return AtomicWriteFile(path, b, fileMode)
}

// AtomicWriteFile writes temp + fsync + rename + directory fsync.
func AtomicWriteFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, stateDirMode); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
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
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return nil // the rename succeeded; directory fsync is best effort
	}
	defer dirFile.Close()
	return dirFile.Sync()
}

// SeqAllocator persists only a reserved upper bound, not every number.
type SeqAllocator struct {
	mu    sync.Mutex
	path  string
	bound uint64
	next  uint64
	step  uint64
}

// OpenSeq opens (or initializes) the sequence allocator at path.
func OpenSeq(path string) (*SeqAllocator, error) {
	a := &SeqAllocator{path: path, step: 1000}
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read seq: %w", err)
	}
	if err == nil {
		bound, perr := parseUint(strings.TrimSpace(string(b)))
		if perr != nil {
			return nil, fmt.Errorf("parse seq file %s: %w", path, perr)
		}
		a.bound = bound
		a.next = bound
	}
	return a, nil
}

// Next returns the next sequence number, reserving a new durable bound when
// the current reservation is exhausted.
func (a *SeqAllocator) Next() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next >= a.bound {
		newBound := a.bound + a.step
		if newBound <= a.bound {
			return 0, errors.New("agentseq overflow")
		}
		if err := AtomicWriteFile(a.path, []byte(fmt.Sprintf("%d\n", newBound)), fileMode); err != nil {
			return 0, fmt.Errorf("reserve agentseq: %w", err)
		}
		a.bound = newBound
	}
	a.next++
	return a.next, nil
}

// Bound returns the currently reserved upper bound (for tests and logs).
func (a *SeqAllocator) Bound() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bound
}

func parseUint(s string) (uint64, error) {
	var n uint64
	if s == "" {
		return 0, nil
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("not an unsigned integer: %q", s)
		}
		n = n*10 + uint64(s[i]-'0')
	}
	return n, nil
}
