package network

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
)

const socketHeader = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReadSocketFile(t *testing.T) {
	dir := t.TempDir()
	tcp := socketHeader +
		"   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 111 1\n" +
		"   1: 0100007F:1F90 0100007F:9999 01 00000000:00000000 00:00000000 00000000     0        0 112 1\n" +
		"   2: 00000000:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 113 1\n"
	path := filepath.Join(dir, "tcp")
	writeFile(t, path, tcp)
	records, err := readSocketFile(path, "tcp", "ipv4")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("tcp records = %+v", records)
	}
	if records[0].Address != "127.0.0.1" || records[0].Port != 8080 || records[0].Inode != 111 {
		t.Fatalf("record[0] = %+v", records[0])
	}
	if records[1].Address != "0.0.0.0" || records[1].Port != 80 {
		t.Fatalf("record[1] = %+v", records[1])
	}

	udp := socketHeader +
		"   0: 00000000:0035 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 333 1\n" +
		"   1: 00000000:0035 0100007F:1F90 07 00000000:00000000 00:00000000 00000000     0        0 334 1\n" +
		"   2: 00000000:0000 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 335 1\n"
	writeFile(t, path, udp)
	records, err = readSocketFile(path, "udp", "ipv4")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Address != "0.0.0.0" || records[0].Port != 53 || records[0].Inode != 333 {
		t.Fatalf("udp records = %+v", records)
	}
}

func TestParseHexAddresses(t *testing.T) {
	cases := []struct{ in, family, want string }{
		{"0100007F", "ipv4", "127.0.0.1"},
		{"00000000", "ipv4", "0.0.0.0"},
		{"00000000000000000000000001000000", "ipv6", "::1"},
		{"00000000000000000000000000000000", "ipv6", "::"},
	}
	for _, c := range cases {
		got, err := parseHexAddress(c.in, c.family)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("%s = %s, want %s", c.in, got, c.want)
		}
	}
	if _, err := parseHexAddress("zz", "ipv4"); err == nil {
		t.Fatal("expected an error for invalid hex")
	}
}

func writeProcStat(t *testing.T, procfs, pid, comm string, start uint64) {
	t.Helper()
	line := pid + " (" + comm + ") S 1 100 100 0 -1 4194560 100 0 0 0 0 0 0 0 20 0 1 0 " + itoa(start) + " 12345 0\n"
	writeFile(t, filepath.Join(procfs, pid, "stat"), line)
	writeFile(t, filepath.Join(procfs, pid, "comm"), comm+"\n")
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

func mustLink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(link)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

func TestSocketsTaskHostAndContainer(t *testing.T) {
	f := newNetFixture(t)
	procfs := f.procfs
	if err := os.MkdirAll(filepath.Join(procfs, "sys", "kernel", "random"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(procfs, "sys", "kernel", "random", "boot_id"), "abcdef01-2345-6789-abcd-ef0123456789\n")
	for _, name := range []string{"tcp", "tcp6", "udp", "udp6"} {
		writeFile(t, filepath.Join(procfs, "net", name), socketHeader)
	}
	writeFile(t, filepath.Join(procfs, "net", "tcp"), socketHeader+
		"   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 111 1\n")

	writeProcStat(t, procfs, "100", "nginx", 500)
	mustLink(t, "net:[1111]", filepath.Join(procfs, "100", "ns", "net"))
	mustLink(t, "socket:[111]", filepath.Join(procfs, "100", "fd", "3"))

	writeProcStat(t, procfs, "200", "api", 700)
	mustLink(t, "net:[2222]", filepath.Join(procfs, "200", "ns", "net"))
	mustLink(t, "net:[1]", filepath.Join(procfs, "1", "ns", "net"))
	for _, name := range []string{"tcp", "tcp6", "udp", "udp6"} {
		writeFile(t, filepath.Join(procfs, "200", "net", name), socketHeader)
	}
	writeFile(t, filepath.Join(procfs, "200", "net", "tcp"), socketHeader+
		"   0: 00000000:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 222 1\n")
	mustLink(t, "socket:[222]", filepath.Join(procfs, "200", "fd", "4"))

	containerID := strings.Repeat("a", 64)
	containerEntity := "container:" + f.deps.Identity.HostID + ":docker-test:" + containerID
	f.index.Publish([]core.ContainerRef{{
		ContainerID: containerID, EntityID: containerEntity, EngineKey: "docker-test", MainPID: 200,
	}})
	f.deps.Config.Modules.Network.CollectContainerNamespaces = true

	if err := f.mod.socketsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	f.em.mu.Unlock()

	if len(entities) != 2 {
		t.Fatalf("entities = %+v", entities)
	}
	var host, container events.Entity
	for _, e := range entities {
		if e.Attributes["scope"] == "host" {
			host = e
		} else {
			container = e
		}
	}
	if host.ID == "" || host.Attributes["address"] != "127.0.0.1" || host.Attributes["port"] != 8080 ||
		host.Attributes["process_name"] != "nginx" || host.Attributes["family"] != "ipv4" {
		t.Fatalf("host socket = %+v", host)
	}
	if container.ID == "" || container.Attributes["container_id"] != containerID || container.Attributes["process_name"] != "api" {
		t.Fatalf("container socket = %+v", container)
	}
	processID := ids.ProcessID(f.deps.Identity.HostID, "abcdef01", 100, 500)
	foundOwned := false
	foundContainerRun := false
	for _, r := range rels {
		if r.Type == "owned_by" && r.To == processID {
			foundOwned = true
		}
		if r.Type == "runs_on" && r.From == container.ID && r.To == containerEntity {
			foundContainerRun = true
		}
	}
	if !foundOwned {
		t.Fatalf("owned_by relationship missing: %+v", rels)
	}
	if !foundContainerRun {
		t.Fatalf("container runs_on relationship missing: %+v", rels)
	}
	if len(snaps) != 1 || !snaps[0].Complete || snaps[0].CoversKinds[0] != ids.KindListeningSocket {
		t.Fatalf("snapshot = %+v", snaps)
	}
}

func TestSocketsFDWalkTimeout(t *testing.T) {
	f := newNetFixture(t)
	procfs := f.procfs
	writeFile(t, filepath.Join(procfs, "net", "tcp"), socketHeader+
		"   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 111 1\n")
	for _, name := range []string{"tcp6", "udp", "udp6"} {
		writeFile(t, filepath.Join(procfs, "net", name), socketHeader)
	}
	writeProcStat(t, procfs, "100", "nginx", 500)
	f.deps.Config.Modules.Network.FDWalkBudget = 1
	if err := f.mod.socketsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	f.em.mu.Unlock()
	if len(snaps) != 1 || snaps[0].Complete {
		t.Fatalf("snapshot = %+v", snaps)
	}
	found := false
	for _, e := range snaps[0].Errors {
		if e.Code == events.ErrCodeFDWalkTimeout {
			found = true
		}
	}
	if !found {
		t.Fatalf("errors = %+v", snaps[0].Errors)
	}
}

func FuzzReadSocketFile(f *testing.F) {
	f.Add(socketHeader + "   0: 0100007F:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 111 1\n")
	f.Add(socketHeader + "   0: 00000000000000000000000001000000:0050 00000000:0000 0A 00:00 00:00 00 0 0 1\n")
	f.Add("not a socket file\x00\n")
	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 1<<20 {
			return
		}
		path := filepath.Join(t.TempDir(), "tcp")
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		_, _ = readSocketFile(path, "tcp", "ipv4")
		_, _ = readSocketFile(path, "udp", "ipv6")
	})
}
