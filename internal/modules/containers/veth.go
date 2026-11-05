package containers

import (
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"

	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"
)

// vethRelationships maps running containers to their host-side veth peer on a
// best-effort basis (§8.3). Every failure is silent apart from a debug log.
func (m *Module) vethRelationships(ctx context.Context, st *engineState, containers []model.Container, now events.Timestamp) []events.Relationship {
	d := m.getDeps()
	procfs := d.Paths.ProcfsRoot
	hostNS, _ := os.Readlink(filepath.Join(procfs, "1", "ns", "net"))
	selfNS, _ := os.Readlink(filepath.Join(procfs, "self", "ns", "net"))

	// In a containerized deployment the host-side veth lives in the host
	// namespace, which is not the agent's own namespace.
	var hostHandle *netlink.Handle
	if hostNS != "" && selfNS != "" && hostNS != selfNS {
		if ns, err := netns.GetFromPath(filepath.Join(procfs, "1", "ns", "net")); err == nil {
			if h, err := netlink.NewHandleAt(ns); err == nil {
				hostHandle = h
				defer h.Close()
			}
			ns.Close()
		}
	}

	seen := map[string]bool{}
	var out []events.Relationship

	for _, c := range containers {
		if ctx.Err() != nil {
			break
		}
		if c.State != "running" || c.MainPID <= 0 {
			continue
		}
		nsPath := filepath.Join(procfs, strconv.Itoa(c.MainPID), "ns", "net")
		nsName, err := os.Readlink(nsPath)
		if err != nil {
			continue
		}
		if hostNS != "" && nsName == hostNS {
			continue // host network namespace
		}
		nsHandle, err := netns.GetFromPath(nsPath)
		if err != nil {
			m.log().Debug("veth: open netns", "container", c.ID, "error", err)
			continue
		}
		handle, err := netlink.NewHandleAt(nsHandle)
		nsHandle.Close()
		if err != nil {
			m.log().Debug("veth: netlink handle", "container", c.ID, "error", err)
			continue
		}
		links, err := handle.LinkList()
		handle.Close()
		if err != nil {
			m.log().Debug("veth: list links", "container", c.ID, "error", err)
			continue
		}
		from := ids.ContainerID(d.Identity.HostID, st.key, c.ID)
		for _, link := range links {
			if link.Type() != "veth" {
				continue
			}
			peer := link.Attrs().ParentIndex
			if peer <= 0 {
				continue
			}
			var hostLink netlink.Link
			if hostHandle != nil {
				hostLink, err = hostHandle.LinkByIndex(peer)
			} else {
				hostLink, err = netlink.LinkByIndex(peer)
			}
			if err != nil {
				continue
			}
			to := ids.InterfaceID(d.Identity.HostID, hostLink.Attrs().Name)
			id := ids.RelationshipID(from, "attached_to", to)
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, events.NewRelationship(id, from, "attached_to", to, nil, now))
		}
	}
	return out
}
