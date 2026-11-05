package containers

import (
	"sort"
	"strings"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"
)

func attrTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC().Truncate(time.Millisecond)
}

func engineName(e model.Engine) string {
	name := e.Runtime
	if e.Endpoint != "" {
		name += " @ " + e.Endpoint
	}
	return truncate(name, 256)
}

func engineEntity(e model.Engine, entityID string, observed events.Timestamp) events.Entity {
	attrs := map[string]any{
		"runtime":       e.Runtime,
		"endpoint":      e.Endpoint,
		"endpoint_type": e.EndpointType,
		"rootless":      e.Rootless,
		"owner_uid":     e.OwnerUID,
		"engine_status": e.Status,
	}
	if e.Version != "" {
		attrs["version"] = e.Version
	}
	if e.APIVersion != "" {
		attrs["api_version"] = e.APIVersion
	}
	if e.StorageDriver != "" {
		attrs["storage_driver"] = e.StorageDriver
	}
	if e.CgroupVersion > 0 {
		attrs["cgroup_version"] = e.CgroupVersion
	}
	return events.NewEntity(entityID, ids.KindContainerEngine, engineName(e), attrs, observed)
}

func (m *Module) containerEntity(d core.Deps, st *engineState, c model.Container, now events.Timestamp) events.Entity {
	entityID := ids.ContainerID(d.Identity.HostID, st.key, c.ID)
	attrs := map[string]any{
		"container_id": c.ID,
		"runtime":      c.Runtime,
		"engine_id":    st.engineID,
	}
	if c.Image != "" {
		attrs["image"] = truncate(c.Image, 512)
	}
	if c.ImageID != "" {
		attrs["image_id"] = c.ImageID
	}
	if c.ImageDigest != "" {
		attrs["image_digest"] = c.ImageDigest
	}
	if c.State != "" {
		attrs["state"] = c.State
	}
	if c.StatusText != "" {
		attrs["status_text"] = truncate(c.StatusText, 256)
	}
	if c.Health != "" {
		attrs["health"] = c.Health
	}
	if t := attrTime(c.CreatedAt); !t.IsZero() {
		attrs["created_at"] = t
	}
	if t := attrTime(c.StartedAt); !t.IsZero() {
		attrs["started_at"] = t
	}
	if t := attrTime(c.FinishedAt); !t.IsZero() {
		attrs["finished_at"] = t
	}
	if c.State == "exited" || c.State == "dead" {
		attrs["exit_code"] = c.ExitCode
	}
	if c.MainPID > 0 && c.State == "running" {
		attrs["main_pid"] = c.MainPID
	}
	if c.PodID != "" {
		attrs["pod_id"] = c.PodID
	}
	if len(c.Ports) > 0 {
		attrs["ports"] = portAttrs(c.Ports)
	}
	if len(c.Networks) > 0 {
		attrs["networks"] = networkAttrs(c.Networks)
	}
	if len(c.Mounts) > 0 {
		attrs["mounts"] = mountAttrs(c.Mounts, d.Config.Modules.Containers.CollectMountSources)
	}
	if c.RestartPolicy != "" {
		attrs["restart_policy"] = c.RestartPolicy
	}
	name := c.Name
	if name == "" {
		name = truncate(c.ID, 12)
	}
	e := events.NewEntity(entityID, ids.KindContainer, truncate(name, 256), attrs, now)
	e.Labels = m.filterLabels(c.Labels)
	return e
}

func portAttrs(ports []model.Port) []map[string]any {
	out := make([]map[string]any, 0, len(ports))
	for _, p := range ports {
		if p.ContainerPort <= 0 {
			continue
		}
		item := map[string]any{"container_port": p.ContainerPort}
		if p.Protocol != "" {
			item["protocol"] = p.Protocol
		}
		if p.HostIP != "" {
			item["host_ip"] = p.HostIP
		}
		if p.HostPort > 0 {
			item["host_port"] = p.HostPort
		}
		out = append(out, item)
	}
	return out
}

func networkAttrs(networks []model.NetworkAttachment) []map[string]any {
	out := make([]map[string]any, 0, len(networks))
	for _, n := range networks {
		item := map[string]any{"name": n.Name}
		if n.NetworkID != "" {
			item["network_id"] = n.NetworkID
		}
		if n.IPAddress != "" {
			item["ip_address"] = n.IPAddress
		}
		if n.Gateway != "" {
			item["gateway"] = n.Gateway
		}
		if len(n.Aliases) > 0 {
			item["aliases"] = append([]string(nil), n.Aliases...)
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i]["name"].(string) < out[j]["name"].(string)
	})
	return out
}

func mountAttrs(mounts []model.Mount, includeSource bool) []map[string]any {
	out := make([]map[string]any, 0, len(mounts))
	for _, mt := range mounts {
		typ := mt.Type
		if typ == "" {
			typ = "bind"
		}
		item := map[string]any{
			"type":        typ,
			"destination": mt.Destination,
			"read_only":   mt.ReadOnly,
		}
		if mt.VolumeName != "" {
			item["volume_name"] = mt.VolumeName
		}
		if includeSource && mt.Source != "" {
			item["source"] = mt.Source
		}
		out = append(out, item)
	}
	return out
}

func (m *Module) networkEntity(d core.Deps, st *engineState, n model.Network, now events.Timestamp) events.Entity {
	id := ids.NetworkID(d.Identity.HostID, st.key, n.ID)
	attrs := map[string]any{
		"network_id": n.ID,
		"driver":     n.Driver,
		"scope":      n.Scope,
		"internal":   n.Internal,
		"ipv6":       n.IPv6,
	}
	if len(n.IPAM) > 0 {
		ipam := make([]map[string]any, 0, len(n.IPAM))
		for _, c := range n.IPAM {
			item := map[string]any{}
			if c.Subnet != "" {
				item["subnet"] = c.Subnet
			}
			if c.Gateway != "" {
				item["gateway"] = c.Gateway
			}
			if len(item) > 0 {
				ipam = append(ipam, item)
			}
		}
		if len(ipam) > 0 {
			attrs["ipam"] = ipam
		}
	}
	if n.BridgeInterface != "" {
		attrs["bridge_interface"] = n.BridgeInterface
	}
	name := n.Name
	if name == "" {
		name = truncate(n.ID, 12)
	}
	e := events.NewEntity(id, ids.KindContainerNetwork, truncate(name, 256), attrs, now)
	e.Labels = m.filterLabels(n.Labels)
	return e
}

func (m *Module) volumeEntity(d core.Deps, st *engineState, v model.Volume, now events.Timestamp) events.Entity {
	id := ids.VolumeID(d.Identity.HostID, st.key, v.Name)
	attrs := map[string]any{
		"volume_name": v.Name,
		"driver":      v.Driver,
		"scope":       v.Scope,
	}
	if v.Mountpoint != "" {
		attrs["mountpoint"] = v.Mountpoint
	}
	if t := attrTime(v.CreatedAt); !t.IsZero() {
		attrs["created_at"] = t
	}
	e := events.NewEntity(id, ids.KindContainerVolume, truncate(v.Name, 256), attrs, now)
	e.Labels = m.filterLabels(v.Labels)
	return e
}

func (m *Module) podEntity(d core.Deps, st *engineState, p model.Pod, now events.Timestamp) events.Entity {
	id := ids.PodID(d.Identity.HostID, st.key, p.ID)
	attrs := map[string]any{
		"pod_id":          p.ID,
		"state":           p.State,
		"container_count": len(p.ContainerIDs),
	}
	if t := attrTime(p.CreatedAt); !t.IsZero() {
		attrs["created_at"] = t
	}
	if p.InfraID != "" {
		attrs["infra_container_id"] = p.InfraID
	}
	name := p.Name
	if name == "" {
		name = truncate(p.ID, 12)
	}
	e := events.NewEntity(id, ids.KindPodmanPod, truncate(name, 256), attrs, now)
	e.Labels = m.filterLabels(p.Labels)
	return e
}

func (m *Module) imageEntity(d core.Deps, st *engineState, im model.Image, now events.Timestamp) events.Entity {
	id := ids.ImageID(d.Identity.HostID, st.key, im.ID)
	attrs := map[string]any{
		"image_id": im.ID,
		"dangling": len(im.RepoTags) == 0,
	}
	if len(im.RepoTags) > 0 {
		attrs["repo_tags"] = append([]string(nil), im.RepoTags...)
	}
	if len(im.RepoDigests) > 0 {
		attrs["repo_digests"] = append([]string(nil), im.RepoDigests...)
	}
	if t := attrTime(im.CreatedAt); !t.IsZero() {
		attrs["created_at"] = t
	}
	if im.SizeBytes > 0 {
		attrs["size_bytes"] = im.SizeBytes
	}
	if im.Architecture != "" {
		attrs["architecture"] = im.Architecture
	}
	if im.OS != "" {
		attrs["os"] = im.OS
	}
	name := truncate(im.ID, 12)
	if len(im.RepoTags) > 0 && im.RepoTags[0] != "" {
		name = im.RepoTags[0]
	}
	e := events.NewEntity(id, ids.KindContainerImage, truncate(name, 256), attrs, now)
	e.Labels = m.filterLabels(im.Labels)
	return e
}

// containerRelationships returns the relationships owned by one container.
func (m *Module) containerRelationships(d core.Deps, st *engineState, c model.Container, now events.Timestamp) []events.Relationship {
	from := ids.ContainerID(d.Identity.HostID, st.key, c.ID)
	var out []events.Relationship
	seen := map[string]bool{}
	add := func(typ, to string, attrs map[string]any) {
		if to == "" {
			return
		}
		id := ids.RelationshipID(from, typ, to)
		if seen[id] {
			return
		}
		seen[id] = true
		out = append(out, events.NewRelationship(id, from, typ, to, attrs, now))
	}
	add("managed_by", st.engineID, nil)
	if c.ImageID != "" {
		add("uses_image", ids.ImageID(d.Identity.HostID, st.key, c.ImageID), nil)
	}
	for _, n := range c.Networks {
		if n.NetworkID == "" {
			continue
		}
		attrs := map[string]any{}
		if n.IPAddress != "" {
			attrs["ip_address"] = n.IPAddress
		}
		if len(n.Aliases) > 0 {
			attrs["aliases"] = append([]string(nil), n.Aliases...)
		}
		add("uses_network", ids.NetworkID(d.Identity.HostID, st.key, n.NetworkID), attrs)
	}
	if d.Config.Modules.Containers.CollectVolumes {
		for _, mt := range c.Mounts {
			if !strings.EqualFold(mt.Type, "volume") && mt.VolumeName == "" {
				continue
			}
			if mt.VolumeName == "" {
				continue
			}
			add("mounts", ids.VolumeID(d.Identity.HostID, st.key, mt.VolumeName), map[string]any{
				"destination": mt.Destination,
				"read_only":   mt.ReadOnly,
			})
		}
	}
	return out
}

func networkRelationships(d core.Deps, st *engineState, n model.Network, now events.Timestamp) []events.Relationship {
	from := ids.NetworkID(d.Identity.HostID, st.key, n.ID)
	out := []events.Relationship{
		events.NewRelationship(ids.RelationshipID(from, "managed_by", st.engineID), from, "managed_by", st.engineID, nil, now),
	}
	if n.BridgeInterface != "" {
		to := ids.InterfaceID(d.Identity.HostID, n.BridgeInterface)
		out = append(out, events.NewRelationship(ids.RelationshipID(from, "attached_to", to), from, "attached_to", to, nil, now))
	}
	return out
}

func volumeRelationships(d core.Deps, st *engineState, v model.Volume, now events.Timestamp) []events.Relationship {
	from := ids.VolumeID(d.Identity.HostID, st.key, v.Name)
	return []events.Relationship{
		events.NewRelationship(ids.RelationshipID(from, "managed_by", st.engineID), from, "managed_by", st.engineID, nil, now),
	}
}

func imageRelationships(d core.Deps, st *engineState, im model.Image, now events.Timestamp) []events.Relationship {
	from := ids.ImageID(d.Identity.HostID, st.key, im.ID)
	return []events.Relationship{
		events.NewRelationship(ids.RelationshipID(from, "managed_by", st.engineID), from, "managed_by", st.engineID, nil, now),
	}
}

func podRelationships(d core.Deps, st *engineState, p model.Pod, now events.Timestamp) []events.Relationship {
	from := ids.PodID(d.Identity.HostID, st.key, p.ID)
	out := []events.Relationship{
		events.NewRelationship(ids.RelationshipID(from, "managed_by", st.engineID), from, "managed_by", st.engineID, nil, now),
	}
	for _, cid := range p.ContainerIDs {
		to := ids.ContainerID(d.Identity.HostID, st.key, cid)
		out = append(out, events.NewRelationship(ids.RelationshipID(from, "contains", to), from, "contains", to, nil, now))
	}
	return out
}
