package network

import (
	"context"
	"net"
	"testing"
	"time"

	"agent/internal/events"
	"agent/internal/ids"
)

func TestInterfaceInventory(t *testing.T) {
	f := newNetFixture(t)
	if err := f.mod.interfaceTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	f.em.mu.Unlock()

	if len(entities) != 4 {
		t.Fatalf("entities = %d", len(entities))
	}
	byName := map[string]events.Entity{}
	for _, e := range entities {
		byName[e.Name] = e
	}
	lo := byName["lo"]
	if lo.Kind != ids.KindNetworkInterface || lo.Attributes["interface_type"] != "loopback" || lo.Attributes["state"] != "up" {
		t.Fatalf("lo = %+v", lo)
	}
	eth := byName["eth0"]
	if eth.Attributes["interface_type"] != "ethernet" || eth.Attributes["mac_address"] != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("eth0 = %+v", eth)
	}
	if eth.Attributes["speed_mbps"] != 1000 {
		t.Fatalf("speed = %+v", eth.Attributes["speed_mbps"])
	}
	addrs, ok := eth.Attributes["addresses"].([]map[string]any)
	if !ok || len(addrs) != 2 {
		t.Fatalf("addresses = %+v", eth.Attributes["addresses"])
	}
	veth := byName["veth123"]
	if veth.Attributes["interface_type"] != "veth" || veth.Attributes["peer_ifindex"] != 2 || veth.Attributes["master"] != "br0" {
		t.Fatalf("veth = %+v", veth)
	}
	br := byName["br0"]
	if br.Attributes["interface_type"] != "bridge" {
		t.Fatalf("br0 = %+v", br)
	}
	if len(snaps) != 1 || !snaps[0].Complete || snaps[0].CoversKinds[0] != ids.KindNetworkInterface {
		t.Fatalf("snapshot = %+v", snaps)
	}
	if snaps[0].EntitiesCount != len(entities) || snaps[0].RelationshipsCount != len(rels) {
		t.Fatalf("counts = %+v, emitted %d/%d", snaps[0], len(entities), len(rels))
	}
	roles := map[string]int{}
	for _, r := range rels {
		if r.Type == "attached_to" {
			roles[r.Attributes["role"].(string)]++
		}
	}
	if roles["master"] != 1 {
		t.Fatalf("relationship roles = %+v", roles)
	}
	if f.mod.Health().Status != "healthy" {
		t.Fatalf("health = %+v", f.mod.Health())
	}
}

func TestInterfaceLifecycleChanges(t *testing.T) {
	f := newNetFixture(t)
	if err := f.mod.interfaceTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	f.fake.set(func(b *fakeBackend) {
		// eth0 goes down, br0 is removed and a tun device appears.
		b.links[1].Flags = 0
		b.links[1].OperState = "down"
		b.links = []Link{
			b.links[0], b.links[1], b.links[3],
			{Index: 5, Name: "tun0", MTU: 1500, Flags: net.FlagUp, OperState: "up", Type: "tun"},
		}
	})
	f.clock.Advance(time.Minute)
	if err := f.mod.interfaceTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	lifecycles := append([]events.Lifecycle(nil), f.em.lifecycles...)
	f.em.mu.Unlock()
	got := map[string]string{}
	for _, l := range lifecycles {
		got[l.Name] = l.Status + "/" + l.Reason
	}
	want := map[string]string{
		"eth0": "down/interface_down",
		"br0":  "removed/entity_removed",
		"tun0": "created/entity_created",
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("%s = %q, want %q (all: %+v)", name, got[name], value, lifecycles)
		}
	}
}

func TestInterfaceMetrics(t *testing.T) {
	f := newNetFixture(t)
	if err := f.mod.interfaceTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.reset()
	if err := f.mod.metricsTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	readings := append([]events.Reading(nil), f.em.metrics...)
	f.em.mu.Unlock()
	if len(readings) != 1 {
		t.Fatalf("readings = %d, want 1 for eth0", len(readings))
	}
	wantID := ids.InterfaceID(f.deps.Identity.HostID, "eth0")
	r := readings[0]
	if r.EntityID != wantID || len(r.Metrics) != 8 {
		t.Fatalf("reading = %+v", r)
	}
	for _, m := range r.Metrics {
		if m.Kind != events.KindCounter || m.Temporality != "cumulative" {
			t.Fatalf("metric = %+v", m)
		}
	}
}
