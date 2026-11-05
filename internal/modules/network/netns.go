package network

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"agent/internal/core"
)

// netnsGroup is one container network namespace discovered through the
// container index.
type netnsGroup struct {
	key         string
	containerID string
	entityID    string
	pid         int
}

// containerNetnsGroups groups running containers by network namespace. The
// host namespace is excluded. The first container (sorted by ID) represents a
// shared namespace such as a Podman pod.
func containerNetnsGroups(d core.Deps) ([]netnsGroup, string) {
	hostNS, _ := os.Readlink(filepath.Join(d.Paths.ProcfsRoot, "1", "ns", "net"))
	var refs []core.ContainerRef
	if d.Index != nil {
		if all, ok := d.Index.(core.ContainerEnumerator); ok {
			refs = all.All()
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ContainerID < refs[j].ContainerID })

	byNS := map[string]netnsGroup{}
	for _, ref := range refs {
		if ref.MainPID <= 0 || ref.EntityID == "" || ref.ContainerID == "" {
			continue
		}
		ns, err := os.Readlink(filepath.Join(d.Paths.ProcfsRoot, strconv.Itoa(ref.MainPID), "ns", "net"))
		if err != nil {
			continue
		}
		if hostNS != "" && ns == hostNS {
			continue
		}
		if _, ok := byNS[ns]; ok {
			continue
		}
		byNS[ns] = netnsGroup{
			key:         ns,
			containerID: ref.ContainerID,
			entityID:    ref.EntityID,
			pid:         ref.MainPID,
		}
	}
	out := make([]netnsGroup, 0, len(byNS))
	for _, g := range byNS {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].containerID < out[j].containerID })
	return out, hostNS
}
