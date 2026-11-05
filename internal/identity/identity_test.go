package identity

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func fakeHost(t *testing.T, machineID, productUUID string) (hostfs, sysfs string) {
	t.Helper()
	root := t.TempDir()
	hostfs = root
	sysfs = filepath.Join(root, "sys")
	if err := os.MkdirAll(filepath.Join(hostfs, "etc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sysfs, "class/dmi/id"), 0o755); err != nil {
		t.Fatal(err)
	}
	if machineID != "" {
		if err := os.WriteFile(filepath.Join(hostfs, "etc/machine-id"), []byte(machineID+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if productUUID != "" {
		if err := os.WriteFile(filepath.Join(sysfs, "class/dmi/id/product_uuid"), []byte(productUUID+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return hostfs, sysfs
}

func TestStableAcrossRestarts(t *testing.T) {
	hostfs, sysfs := fakeHost(t, "abc123", "4C4C4544-0032-3510-8051-B8C04F4E4E32")
	state := t.TempDir()

	a, err := LoadOrCreate(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if a.AgentID != b.AgentID || a.HostID != b.HostID {
		t.Fatalf("identity changed across restart: %+v vs %+v", a, b)
	}
	if a.AgentRunID == b.AgentRunID {
		t.Fatal("agent_run_id must change per process")
	}
	if a.CloneDetected || b.CloneDetected {
		t.Fatal("unexpected clone detection")
	}
	info, err := os.Stat(filepath.Join(state, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o", info.Mode().Perm())
	}
}

func TestReinstallKeepsHostID(t *testing.T) {
	hostfs, sysfs := fakeHost(t, "machine-1", "")
	a, err := LoadOrCreate(t.TempDir(), hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(t.TempDir(), hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if a.HostID != b.HostID {
		t.Fatalf("host ID must derive from machine-id: %s != %s", a.HostID, b.HostID)
	}
	if a.AgentID == b.AgentID {
		t.Fatal("a new installation must get a new agent ID")
	}
}

func TestCloneDetection(t *testing.T) {
	hostfs, sysfs := fakeHost(t, "machine-1", "uuid-1")
	state := t.TempDir()
	a, err := LoadOrCreate(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a cloned VM: both identifiers change.
	if err := os.WriteFile(filepath.Join(hostfs, "etc/machine-id"), []byte("machine-2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sysfs, "class/dmi/id/product_uuid"), []byte("uuid-2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if !b.CloneDetected {
		t.Fatal("clone should have been detected")
	}
	if a.AgentID == b.AgentID || a.HostID == b.HostID {
		t.Fatal("clone must rotate both IDs")
	}
	if b.HostID == newHostID("machine-2", false) {
		t.Fatal("clone must use a random host ID, not the machine-id derivation")
	}
}

func TestNoMachineID(t *testing.T) {
	hostfs, sysfs := fakeHost(t, "", "")
	a, err := LoadOrCreate(t.TempDir(), hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(t.TempDir(), hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if a.HostID == b.HostID {
		t.Fatal("without machine-id each install must be random")
	}
}

func TestSeqPersistenceAndReservation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "seq")
	a, err := OpenSeq(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		got, err := a.Next()
		if err != nil {
			t.Fatal(err)
		}
		if got != i {
			t.Fatalf("seq = %d, want %d", got, i)
		}
	}
	if b := a.Bound(); b != 1000 {
		t.Fatalf("bound = %d, want 1000", b)
	}

	b, err := OpenSeq(p)
	if err != nil {
		t.Fatal(err)
	}
	next, err := b.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next != 1001 {
		t.Fatalf("after restart next = %d, want 1001", next)
	}
}

func TestSeqConcurrent(t *testing.T) {
	p := filepath.Join(t.TempDir(), "seq")
	a, err := OpenSeq(p)
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	seen := make(map[uint64]bool, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := a.Next()
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[v] {
				t.Errorf("duplicate sequence %d", v)
			}
			seen[v] = true
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("got %d unique numbers, want %d", len(seen), n)
	}
}

func TestSeqCorruptFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "seq")
	if err := os.WriteFile(p, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSeq(p); err == nil {
		t.Fatal("corrupt seq file must fail")
	}
}

func TestLoadIsReadOnly(t *testing.T) {
	hostfs, sysfs := fakeHost(t, "machine-1", "uuid-1")
	state := filepath.Join(t.TempDir(), "missing-state")

	// No identity: ephemeral, and no state directory is created.
	a, err := Load(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("Load created the state directory: %v", err)
	}
	b, err := Load(state, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if a.AgentID == b.AgentID {
		t.Fatal("ephemeral identities should differ per call")
	}

	// Existing identity: same IDs, fresh run, no writes.
	created, err := LoadOrCreate(t.TempDir(), hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	otherState := t.TempDir()
	if _, err := LoadOrCreate(otherState, hostfs, sysfs); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(otherState, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := Load(otherState, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Load(otherState, hostfs, sysfs)
	if err != nil {
		t.Fatal(err)
	}
	if c.AgentID != d.AgentID || c.HostID != d.HostID {
		t.Fatal("stored identity changed between Load calls")
	}
	if c.AgentRunID == d.AgentRunID {
		t.Fatal("Load must assign a fresh agent_run_id")
	}
	if c.AgentID == created.AgentID {
		t.Fatal("unrelated state directories must not share agent IDs")
	}
	after, err := os.ReadFile(filepath.Join(otherState, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("Load modified identity.json")
	}
}
