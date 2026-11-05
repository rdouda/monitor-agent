package network

import (
	"context"
	"net"
	"testing"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"agent/internal/events"
	"agent/internal/ids"
)

func cidr(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRouteInventory(t *testing.T) {
	f := newNetFixture(t)
	f.fake.set(func(b *fakeBackend) {
		b.routes = map[int][]Route{
			netlink.FAMILY_V4: {
				{Dst: nil, Gw: net.ParseIP("192.0.2.1"), LinkIndex: 2, Table: 254, Metric: 100, Protocol: unix.RTPROT_DHCP, Scope: unix.RT_SCOPE_UNIVERSE, Type: unix.RTN_UNICAST},
				{Dst: cidr(t, "10.0.0.0/8"), LinkIndex: 2, Table: 254, Protocol: unix.RTPROT_KERNEL, Scope: unix.RT_SCOPE_LINK, Type: unix.RTN_UNICAST},
				{Dst: cidr(t, "192.0.2.10/32"), LinkIndex: 2, Table: 255, Protocol: unix.RTPROT_KERNEL, Scope: unix.RT_SCOPE_HOST, Type: unix.RTN_LOCAL},
			},
			netlink.FAMILY_V6: {
				{Dst: cidr(t, "::/0"), LinkIndex: 2, Table: 254, Metric: 1024, Protocol: unix.RTPROT_RA, Scope: unix.RT_SCOPE_UNIVERSE, Type: unix.RTN_UNICAST},
				{Dst: cidr(t, "fe80::/64"), LinkIndex: 2, Table: 254, Protocol: unix.RTPROT_KERNEL, Scope: unix.RT_SCOPE_LINK, Type: unix.RTN_UNICAST},
				{Dst: cidr(t, "ff00::/8"), LinkIndex: 2, Table: 254, Protocol: unix.RTPROT_KERNEL, Scope: unix.RT_SCOPE_UNIVERSE, Type: unix.RTN_UNICAST},
			},
		}
	})
	if err := f.mod.routeTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities := append([]events.Entity(nil), f.em.entities...)
	rels := append([]events.Relationship(nil), f.em.rels...)
	snaps := append([]events.Snapshot(nil), f.em.snapshots...)
	f.em.mu.Unlock()

	if len(entities) != 3 {
		t.Fatalf("entities = %d: %+v", len(entities), entities)
	}
	var def4, ten, def6 events.Entity
	for _, e := range entities {
		if e.Attributes["family"] == "ipv4" && e.Attributes["destination"] == "default" {
			def4 = e
		}
		if e.Attributes["destination"] == "10.0.0.0/8" {
			ten = e
		}
		if e.Attributes["family"] == "ipv6" {
			def6 = e
		}
	}
	if def4.ID == "" || def4.Attributes["gateway"] != "192.0.2.1" || def4.Attributes["protocol"] != "dhcp" ||
		def4.Attributes["interface"] != "eth0" || def4.Attributes["table"] != 254 || def4.Attributes["metric"] != 100 {
		t.Fatalf("default route = %+v", def4)
	}
	if ten.Attributes["protocol"] != "kernel" || ten.Attributes["scope"] != "link" {
		t.Fatalf("10/8 route = %+v", ten)
	}
	if def6.ID == "" || def6.Attributes["protocol"] != "ra" {
		t.Fatalf("ipv6 default route = %+v", def6)
	}
	if len(rels) != 6 {
		t.Fatalf("relationships = %d", len(rels))
	}
	if len(snaps) != 1 || !snaps[0].Complete || snaps[0].CoversKinds[0] != ids.KindNetworkRoute {
		t.Fatalf("snapshot = %+v", snaps)
	}

	// Link-local and multicast IPv6 routes are included only when asked.
	f.em.reset()
	f.deps.Config.Modules.Network.IncludeLocalRoutes = true
	if err := f.mod.routeTask(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.em.mu.Lock()
	entities = append([]events.Entity(nil), f.em.entities...)
	f.em.mu.Unlock()
	if len(entities) != 5 {
		t.Fatalf("entities with local routes = %d", len(entities))
	}
}

func TestRouteIDIncludesFamilyAndMetric(t *testing.T) {
	f := newNetFixture(t)
	host := f.deps.Identity.HostID
	a := ids.RouteID(host, "ipv4", "254", "default", "192.0.2.1", "eth0", 100)
	b := ids.RouteID(host, "ipv6", "254", "default", "192.0.2.1", "eth0", 100)
	c := ids.RouteID(host, "ipv4", "254", "default", "192.0.2.1", "eth0", 101)
	if a == b || a == c || b == c {
		t.Fatalf("route IDs collided: %s %s %s", a, b, c)
	}
}
