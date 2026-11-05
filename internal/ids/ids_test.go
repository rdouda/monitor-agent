package ids

import (
	"strings"
	"testing"
)

func TestEntityIDs(t *testing.T) {
	const host = "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03"
	const engine = "docker-71329c4c"
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"host", HostID(host), "host:" + host},
		{"process", ProcessID(host, "a1b2c3d4", 4821, 123456789), "process:" + host + ":a1b2c3d4:4821:123456789"},
		{"engine", EngineID(host, engine), "engine:" + host + ":" + engine},
		{"container", ContainerID(host, engine, "9f2c7a1b"), "container:" + host + ":" + engine + ":9f2c7a1b"},
		{"pod", PodID(host, "podman-91da59c9", "abc123"), "pod:" + host + ":podman-91da59c9:abc123"},
		{"image", ImageID(host, engine, "sha256deadbeef"), "image:" + host + ":" + engine + ":sha256deadbeef"},
		{"network", NetworkID(host, engine, "a1b2c3"), "network:" + host + ":" + engine + ":a1b2c3"},
		{"volume", VolumeID(host, engine, "payments-data"), "volume:" + host + ":" + engine + ":payments-data"},
		{"iface", InterfaceID(host, "eth0"), "iface:" + host + ":eth0"},
		{"route-v4", RouteID(host, "ipv4", "main", "default", "", "eth0", 100), "route:" + host + ":ipv4:main:default:-:eth0:100"},
		{"route-v6-encoded", RouteID("h", "ipv6", "main", "2001:db8::/64", "fe80::1", "eth0", 100), "route:h:ipv6:main:2001%3Adb8%3A%3A%2F64:fe80%3A%3A1:eth0:100"},
		{"socket-v4", SocketID(host, "host", "tcp", "0.0.0.0", 22), "socket:" + host + ":host:tcp:0.0.0.0:22"},
		{"socket-v6-encoded", SocketID("h", "container-9f2c7a1b5d8e", "tcp", "::", 443), "socket:h:container-9f2c7a1b5d8e:tcp:%3A%3A:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("got %q, want %q", tt.got, tt.want)
			}
			if len(tt.got) > MaxLength {
				t.Fatalf("ID longer than %d bytes: %d", MaxLength, len(tt.got))
			}
		})
	}
}

func TestEngineKey(t *testing.T) {
	tests := []struct {
		runtime string
		path    string
		want    string
	}{
		{"docker", "/var/run/docker.sock", "docker-71329c4c"},
		{"podman", "/run/user/1000/podman/podman.sock", "podman-91da59c9"},
		{"docker", "/run/docker.sock", "docker-dd6a1a9c"},
	}
	for _, tt := range tests {
		if got := EngineKey(tt.runtime, tt.path); got != tt.want {
			t.Errorf("EngineKey(%q, %q) = %q, want %q", tt.runtime, tt.path, got, tt.want)
		}
	}
}

func TestRelationshipID(t *testing.T) {
	got := RelationshipID("host:aaa", "runs_on", "host:bbb")
	want := "rel:a3608d9679d3a4e9020d076698f7aad3"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got2 := RelationshipID("host:aaa", "runs_on", "host:bbb"); got2 != got {
		t.Fatal("relationship IDs must be deterministic")
	}
	other := RelationshipID("host:aaa", "managed_by", "host:bbb")
	if other == got {
		t.Fatal("type must participate in the hash")
	}
	if !strings.HasPrefix(other, "rel:") || len(other) != 36 {
		t.Fatalf("unexpected relationship ID format: %q", other)
	}
}

func TestBootID8(t *testing.T) {
	tests := map[string]string{
		"6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55": "6f1f3b0e",
		"6F1F3B0E2C1D4A7E9B520A1C8D3E4F55":     "6f1f3b0e",
		"a1b2c3d4":                             "a1b2c3d4",
		"zzzz":                                 "",
		"":                                     "",
	}
	for in, want := range tests {
		if got := BootID8(in); got != want {
			t.Errorf("BootID8(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPIDReuseProducesDistinctIDs(t *testing.T) {
	a := ProcessID("h", "deadbeef", 42, 1000)
	b := ProcessID("h", "deadbeef", 42, 1001)
	if a == b {
		t.Fatal("start ticks must distinguish a reused PID")
	}
	c := ProcessID("h", "deadbeee", 42, 1000)
	if a == c {
		t.Fatal("boot id must distinguish a reused PID across reboots")
	}
}

func TestLengthBound(t *testing.T) {
	long := strings.Repeat("x", 2000)
	got := VolumeID("host", "docker-abcdef12", long)
	if len(got) != MaxLength {
		t.Fatalf("length = %d, want %d", len(got), MaxLength)
	}
	other := VolumeID("host", "docker-abcdef12", long+"y")
	if got == other {
		t.Fatal("bounded IDs of distinct inputs must differ")
	}
}

func TestKindForID(t *testing.T) {
	tests := map[string]string{
		"host:x":      KindHost,
		"process:x":   KindProcess,
		"engine:x":    KindContainerEngine,
		"container:x": KindContainer,
		"pod:x":       KindPodmanPod,
		"image:x":     KindContainerImage,
		"network:x":   KindContainerNetwork,
		"volume:x":    KindContainerVolume,
		"iface:x":     KindNetworkInterface,
		"route:x":     KindNetworkRoute,
		"socket:x":    KindListeningSocket,
	}
	for id, want := range tests {
		got, ok := KindForID(id)
		if !ok || got != want {
			t.Errorf("KindForID(%q) = %q, %v; want %q, true", id, got, ok, want)
		}
	}
	if _, ok := KindForID("bogus:x"); ok {
		t.Error("unknown prefix must not be accepted")
	}
}
