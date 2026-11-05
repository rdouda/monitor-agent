package network

import (
	"context"
	"fmt"
	"net"
	"sort"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"agent/internal/events"
	"agent/internal/ids"
)

func (m *Module) routeTask(ctx context.Context) error {
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	d := m.getDeps()
	started := d.Clock.Now()
	now := events.Stamp(started)
	snapID, err := newSnapshotID()
	if err != nil {
		return err
	}
	hostID := ids.HostID(d.Identity.HostID)

	links, err := m.backend.Links()
	if err != nil {
		d.Emitter.SnapshotCompleted(events.Snapshot{
			Module: "network", SnapshotID: snapID, ScopeEntityID: hostID,
			CoversKinds:        []string{ids.KindNetworkRoute},
			Complete:           false,
			StartedAt:          events.Stamp(started),
			CompletedAt:        events.Stamp(d.Clock.Now()),
			EntitiesCount:      0,
			RelationshipsCount: 0,
			Errors:             []events.SnapshotError{{Code: events.ErrCodeAPIError, Message: truncate(err.Error(), 500), Kind: ids.KindNetworkRoute}},
		})
		m.setTaskErr("routes", "netlink_failed", err)
		return err
	}
	byIndex := make(map[int]string, len(links))
	for _, l := range links {
		byIndex[l.Index] = l.Name
	}

	var errs scanErrors
	complete := true
	var routes []Route
	for _, family := range []int{netlink.FAMILY_V4, netlink.FAMILY_V6} {
		list, err := m.backend.Routes(family)
		if err != nil {
			errs.add(events.ErrCodeAPIError, fmt.Sprintf("routes family %d: %v", family, err), ids.KindNetworkRoute)
			complete = false
			continue
		}
		routes = append(routes, list...)
	}

	includeLocal := d.Config.Modules.Network.IncludeLocalRoutes
	limit := d.Config.Limits.MaxEntitiesPerKind
	entities := make([]events.Entity, 0, len(routes))
	rels := make([]events.Relationship, 0, len(routes)*2)
	for _, r := range routes {
		if ctx.Err() != nil {
			errs.add(events.ErrCodeTimeout, "route scan timed out", ids.KindNetworkRoute)
			complete = false
			break
		}
		if len(entities) >= limit {
			errs.add(events.ErrCodeLimitExceeded, "route entity limit reached", ids.KindNetworkRoute)
			complete = false
			break
		}
		if r.Type != 0 && r.Type != unix.RTN_UNICAST {
			continue
		}
		family, dst := routeDestination(r)
		if family == "" {
			continue
		}
		if family == "ipv6" && !includeLocal && dst != "default" {
			if isIPv6LinkLocalOrMulticast(dst) {
				continue
			}
		}
		iface := byIndex[r.LinkIndex]
		gw := ""
		if r.Gw != nil && !r.Gw.IsUnspecified() {
			gw = r.Gw.String()
		}
		table := r.Table
		if table == 0 {
			table = unix.RT_TABLE_MAIN
		}
		routeID := ids.RouteID(d.Identity.HostID, family, fmt.Sprintf("%d", table), dst, gw, iface, r.Metric)
		attrs := map[string]any{
			"family":      family,
			"destination": dst,
			"metric":      r.Metric,
			"table":       table,
			"protocol":    routeProtocol(r.Protocol),
			"scope":       routeScope(r.Scope),
		}
		if gw != "" {
			attrs["gateway"] = gw
		}
		if iface != "" {
			attrs["interface"] = iface
		}
		if r.PreferredSource != nil && !r.PreferredSource.IsUnspecified() {
			attrs["preferred_source"] = r.PreferredSource.String()
		}
		name := dst
		if iface != "" {
			name = dst + " dev " + iface
		}
		entities = append(entities, events.NewEntity(routeID, ids.KindNetworkRoute, truncate(name, 256), attrs, now))
		rels = append(rels, events.NewRelationship(ids.RelationshipID(routeID, "runs_on", hostID), routeID, "runs_on", hostID, nil, now))
		if iface != "" {
			to := ids.InterfaceID(d.Identity.HostID, iface)
			rels = append(rels, events.NewRelationship(ids.RelationshipID(routeID, "attached_to", to), routeID, "attached_to", to, nil, now))
		}
	}
	sort.Slice(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })

	d.Emitter.Entities(snapID, entities)
	d.Emitter.Relationships(snapID, rels)
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module: "network", SnapshotID: snapID, ScopeEntityID: hostID,
		CoversKinds:        []string{ids.KindNetworkRoute},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entities),
		RelationshipsCount: len(rels),
		Errors:             errs.list,
	})
	if errs.first != nil {
		m.setTaskErr("routes", "route_incomplete", errs.first)
	} else {
		m.setTaskOK("routes")
	}
	return nil
}

// routeDestination returns the wire family and destination string. A zero
// mask is reported as "default".
func routeDestination(r Route) (string, string) {
	family := "ipv4"
	if r.Family == netlink.FAMILY_V6 {
		family = "ipv6"
	} else if r.Family == netlink.FAMILY_V4 {
		family = "ipv4"
	} else if r.Dst != nil && r.Dst.IP.To4() == nil {
		family = "ipv6"
	}
	if r.Dst == nil {
		return family, "default"
	}
	ones, bits := r.Dst.Mask.Size()
	if ones == 0 && bits > 0 {
		return family, "default"
	}
	return family, r.Dst.String()
}

func isIPv6LinkLocalOrMulticast(dst string) bool {
	_, ipnet, err := net.ParseCIDR(dst)
	if err != nil {
		return false
	}
	if ipnet.IP.IsMulticast() {
		return true
	}
	// fe80::/10 link-local unicast.
	return ipnet.IP.IsLinkLocalUnicast()
}

func routeProtocol(p int) string {
	switch p {
	case unix.RTPROT_KERNEL:
		return "kernel"
	case unix.RTPROT_BOOT:
		return "boot"
	case unix.RTPROT_STATIC:
		return "static"
	case unix.RTPROT_DHCP:
		return "dhcp"
	case unix.RTPROT_RA:
		return "ra"
	default:
		return "other"
	}
}

func routeScope(scope int) string {
	switch scope {
	case unix.RT_SCOPE_UNIVERSE:
		return "global"
	case unix.RT_SCOPE_LINK:
		return "link"
	case unix.RT_SCOPE_HOST:
		return "host"
	case unix.RT_SCOPE_NOWHERE:
		return "nowhere"
	default:
		return "other"
	}
}
