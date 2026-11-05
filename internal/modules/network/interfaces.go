package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"agent/internal/events"
	"agent/internal/ids"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const maxSnapshotErrors = 20

type scanErrors struct {
	list  []events.SnapshotError
	first error
}

func (s *scanErrors) add(code, message, kind string) {
	if len(s.list) < maxSnapshotErrors {
		s.list = append(s.list, events.SnapshotError{Code: code, Message: truncate(message, 500), Kind: kind})
	}
	if s.first == nil {
		s.first = errors.New(message)
	}
}

func newSnapshotID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("snapshot id: %w", err)
	}
	return id.String(), nil
}

// interfaceTask emits the interface inventory, interface lifecycle changes
// and a snapshot.
func (m *Module) interfaceTask(ctx context.Context) error {
	m.interfaceMu.Lock()
	defer m.interfaceMu.Unlock()

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
			CoversKinds:        []string{ids.KindNetworkInterface},
			Complete:           false,
			StartedAt:          events.Stamp(started),
			CompletedAt:        events.Stamp(d.Clock.Now()),
			EntitiesCount:      0,
			RelationshipsCount: 0,
			Errors:             []events.SnapshotError{{Code: events.ErrCodeAPIError, Message: truncate(err.Error(), 500), Kind: ids.KindNetworkInterface}},
		})
		m.setTaskErr("interfaces", "netlink_failed", err)
		return err
	}
	sort.Slice(links, func(i, j int) bool { return links[i].Name < links[j].Name })
	byIndex := make(map[int]Link, len(links))
	for _, l := range links {
		byIndex[l.Index] = l
	}

	var errs scanErrors
	limit := d.Config.Limits.MaxEntitiesPerKind
	complete := true
	entities := make([]events.Entity, 0, len(links))
	rels := make([]events.Relationship, 0, len(links)*2)
	newStates := make(map[string]string, len(links))

	for _, link := range links {
		if link.Name == "" {
			continue
		}
		if ctx.Err() != nil {
			errs.add(events.ErrCodeTimeout, "interface scan timed out", ids.KindNetworkInterface)
			complete = false
			break
		}
		if len(entities) >= limit {
			errs.add(events.ErrCodeLimitExceeded, "interface entity limit reached", ids.KindNetworkInterface)
			complete = false
			break
		}
		addrs, addrsErr := m.backend.Addrs(link.Index)
		if addrsErr != nil {
			errs.add(events.ErrCodeAPIError, fmt.Sprintf("addresses for %s: %v", link.Name, addrsErr), ids.KindNetworkInterface)
			complete = false
		}
		e, rs := m.interfaceEntity(link, addrs, byIndex, now)
		entities = append(entities, e)
		newStates[link.Name] = interfaceState(link)
		rels = append(rels, rs...)
	}

	d.Emitter.Entities(snapID, entities)
	d.Emitter.Relationships(snapID, rels)

	if m.interfaceBaseline {
		m.reconcileInterfaceLifecycle(oldInterfaceStates(m.interfaceStates), newStates, complete, now)
	}
	m.interfaceBaseline = true
	merged := newStates
	if !complete {
		for name, state := range m.interfaceStates {
			if _, ok := merged[name]; !ok {
				merged[name] = state
			}
		}
	}
	m.interfaceStates = merged

	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module: "network", SnapshotID: snapID, ScopeEntityID: hostID,
		CoversKinds:        []string{ids.KindNetworkInterface},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entities),
		RelationshipsCount: len(rels),
		Errors:             errs.list,
	})
	if errs.first != nil {
		m.setTaskErr("interfaces", "interface_incomplete", errs.first)
	} else {
		m.setTaskOK("interfaces")
	}
	return nil
}

func oldInterfaceStates(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (m *Module) reconcileInterfaceLifecycle(old, current map[string]string, complete bool, now events.Timestamp) {
	d := m.getDeps()
	if len(old) == 0 {
		return // baseline
	}
	var names []string
	for name := range current {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		state := current[name]
		oldState, ok := old[name]
		if !ok {
			m.emitLifecycle(d, events.Lifecycle{
				EntityID: ids.InterfaceID(d.Identity.HostID, name), Kind: ids.KindNetworkInterface,
				Name: name, Status: events.StatusCreated, Reason: events.ReasonEntityCreated, ObservedAt: now,
			})
			continue
		}
		if state == oldState {
			continue
		}
		status := events.StatusDown
		reason := events.ReasonInterfaceDown
		if state == "up" {
			status = events.StatusUp
			reason = events.ReasonInterfaceUp
		}
		prev := oldState
		m.emitLifecycle(d, events.Lifecycle{
			EntityID: ids.InterfaceID(d.Identity.HostID, name), Kind: ids.KindNetworkInterface,
			Name: name, PreviousStatus: &prev, Status: status, Reason: reason, ObservedAt: now,
		})
	}
	if !complete {
		return
	}
	var removed []string
	for name := range old {
		if _, ok := current[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)
	for _, name := range removed {
		prev := old[name]
		m.emitLifecycle(d, events.Lifecycle{
			EntityID: ids.InterfaceID(d.Identity.HostID, name), Kind: ids.KindNetworkInterface,
			Name: name, PreviousStatus: &prev, Status: events.StatusRemoved,
			Reason: events.ReasonEntityRemoved, ObservedAt: now,
		})
	}
}

func (m *Module) interfaceEntity(link Link, addrs []Addr, byIndex map[int]Link, now events.Timestamp) (events.Entity, []events.Relationship) {
	d := m.getDeps()
	hostID := ids.HostID(d.Identity.HostID)
	entityID := ids.InterfaceID(d.Identity.HostID, link.Name)
	typ := interfaceType(d.Paths.SysfsRoot, link)
	state := interfaceState(link)
	attrs := map[string]any{
		"name":           link.Name,
		"ifindex":        link.Index,
		"interface_type": typ,
		"state":          state,
		"mtu":            link.MTU,
	}
	if d.Config.Modules.Network.CollectMACAddresses && link.HardwareAddr != "" {
		attrs["mac_address"] = link.HardwareAddr
	}
	if list := addressAttrs(addrs); len(list) > 0 {
		attrs["addresses"] = list
	}
	if speed, ok := interfaceSpeed(d.Paths.SysfsRoot, link.Name); ok {
		attrs["speed_mbps"] = speed
	}
	var masterName, parentName string
	if link.MasterIndex > 0 {
		if master, ok := byIndex[link.MasterIndex]; ok {
			masterName = master.Name
			attrs["master"] = masterName
		}
	}
	if link.ParentIndex > 0 {
		if typ == "veth" {
			attrs["peer_ifindex"] = link.ParentIndex
		} else if parent, ok := byIndex[link.ParentIndex]; ok {
			parentName = parent.Name
			attrs["parent"] = parentName
		}
	}
	e := events.NewEntity(entityID, ids.KindNetworkInterface, truncate(link.Name, 256), attrs, now)

	rels := []events.Relationship{
		events.NewRelationship(ids.RelationshipID(entityID, "runs_on", hostID), entityID, "runs_on", hostID, nil, now),
	}
	if masterName != "" {
		to := ids.InterfaceID(d.Identity.HostID, masterName)
		rels = append(rels, events.NewRelationship(ids.RelationshipID(entityID, "attached_to", to), entityID, "attached_to", to, map[string]any{"role": "master"}, now))
	}
	if parentName != "" {
		to := ids.InterfaceID(d.Identity.HostID, parentName)
		rels = append(rels, events.NewRelationship(ids.RelationshipID(entityID, "attached_to", to), entityID, "attached_to", to, map[string]any{"role": "parent"}, now))
	}
	return e, rels
}

func addressAttrs(addrs []Addr) []map[string]any {
	if len(addrs) == 0 {
		return nil
	}
	sorted := append([]Addr(nil), addrs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].IP.Equal(sorted[j].IP) {
			return sorted[i].PrefixLen < sorted[j].PrefixLen
		}
		return sorted[i].IP.String() < sorted[j].IP.String()
	})
	out := make([]map[string]any, 0, len(sorted))
	for _, a := range sorted {
		if a.IP == nil {
			continue
		}
		family := "ipv6"
		if a.IP.To4() != nil {
			family = "ipv4"
		}
		out = append(out, map[string]any{
			"address":       a.IP.String(),
			"prefix_length": a.PrefixLen,
			"family":        family,
			"scope":         addressScope(a.Scope),
		})
	}
	return out
}

func addressScope(scope int) string {
	switch scope {
	case unix.RT_SCOPE_UNIVERSE:
		return "global"
	case unix.RT_SCOPE_LINK:
		return "link"
	case unix.RT_SCOPE_HOST:
		return "host"
	case unix.RT_SCOPE_NOWHERE:
		return "host"
	default:
		return "global"
	}
}

func interfaceState(link Link) string {
	oper := strings.ToLower(link.OperState)
	if link.Flags&net.FlagUp != 0 && (oper == "up" || oper == "unknown" || oper == "") {
		return "up"
	}
	return "down"
}

func interfaceType(sysfsRoot string, link Link) string {
	if link.Flags&net.FlagLoopback != 0 || link.Name == "lo" {
		return "loopback"
	}
	switch strings.ToLower(link.Type) {
	case "veth":
		return "veth"
	case "bridge":
		return "bridge"
	case "vlan":
		return "vlan"
	case "bond":
		return "bond"
	case "vxlan":
		return "vxlan"
	case "tun":
		return "tun"
	case "tap":
		return "tap"
	case "dummy":
		return "dummy"
	case "wireguard":
		return "wireguard"
	case "macvlan":
		return "macvlan"
	case "ipvlan":
		return "ipvlan"
	case "loopback":
		return "loopback"
	case "", "device":
		if _, err := os.Stat(filepath.Join(sysfsRoot, "class", "net", link.Name, "device")); err == nil {
			return "ethernet"
		}
		return "other"
	default:
		return "other"
	}
}

func interfaceSpeed(sysfsRoot, name string) (int, bool) {
	b, err := os.ReadFile(filepath.Join(sysfsRoot, "class", "net", name, "speed"))
	if err != nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// metricsTask emits interface counters.
func (m *Module) metricsTask(ctx context.Context) error {
	d := m.getDeps()
	now := d.Clock.Now()
	links, err := m.backend.Links()
	if err != nil {
		m.setTaskErr("metrics", "netlink_failed", err)
		return err
	}
	readings := make([]events.Reading, 0, len(links))
	for _, link := range links {
		if ctx.Err() != nil {
			break
		}
		if link.Statistics == nil || m.excludeInterfaceMetrics(link.Name) {
			continue
		}
		id := ids.InterfaceID(d.Identity.HostID, link.Name)
		readings = append(readings, events.Reading{
			EntityID: id,
			Metrics: []events.Metric{
				events.Counter("network.interface.rx.bytes", float64(link.Statistics.RxBytes), events.UnitBytes, nil),
				events.Counter("network.interface.tx.bytes", float64(link.Statistics.TxBytes), events.UnitBytes, nil),
				events.Counter("network.interface.rx.packets", float64(link.Statistics.RxPackets), events.UnitCount, nil),
				events.Counter("network.interface.tx.packets", float64(link.Statistics.TxPackets), events.UnitCount, nil),
				events.Counter("network.interface.rx.errors", float64(link.Statistics.RxErrors), events.UnitCount, nil),
				events.Counter("network.interface.tx.errors", float64(link.Statistics.TxErrors), events.UnitCount, nil),
				events.Counter("network.interface.rx.dropped", float64(link.Statistics.RxDropped), events.UnitCount, nil),
				events.Counter("network.interface.tx.dropped", float64(link.Statistics.TxDropped), events.UnitCount, nil),
			},
		})
	}
	if len(readings) > 0 {
		d.Emitter.Metrics(now, readings)
	}
	m.setTaskOK("metrics")
	return nil
}
