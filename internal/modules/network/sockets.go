package network

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"agent/internal/events"
	"agent/internal/ids"
)

type socketOwner struct {
	pid int
	at  time.Time
}

type socketRecord struct {
	Protocol    string
	Family      string
	Address     string
	Port        int
	Inode       uint64
	Scope       string // "host" or "container"
	ScopeID     string // "host" or "container-{id12}"
	ContainerID string
	EntityID    string
}

type socketFile struct {
	file     string
	protocol string
	family   string
}

var socketFiles = []socketFile{
	{"tcp", "tcp", "ipv4"},
	{"tcp6", "tcp", "ipv6"},
	{"udp", "udp", "ipv4"},
	{"udp6", "udp", "ipv6"},
}

func (m *Module) socketsTask(ctx context.Context) error {
	m.socketMu.Lock()
	defer m.socketMu.Unlock()
	d := m.getDeps()
	started := d.Clock.Now()
	now := events.Stamp(started)
	snapID, err := newSnapshotID()
	if err != nil {
		return err
	}
	hostID := ids.HostID(d.Identity.HostID)

	var errs scanErrors
	complete := true
	var records []socketRecord

	// In a containerized deployment the agent's own /proc/net points at the
	// agent's network namespace, not the host's. /proc/1/net always describes
	// PID 1's namespace, which is the host when the host /proc is mounted.
	hostNetDir := filepath.Join(d.Paths.ProcfsRoot, "net")
	hostNS, _ := os.Readlink(filepath.Join(d.Paths.ProcfsRoot, "1", "ns", "net"))
	selfNS, _ := os.Readlink(filepath.Join(d.Paths.ProcfsRoot, "self", "ns", "net"))
	if hostNS != "" && selfNS != "" && hostNS != selfNS {
		hostNetDir = filepath.Join(d.Paths.ProcfsRoot, "1", "net")
	}

	for _, sf := range socketFiles {
		path := filepath.Join(hostNetDir, sf.file)
		list, err := readSocketFile(path, sf.protocol, sf.family)
		if err != nil {
			errs.add(events.ErrCodeParseError, fmt.Sprintf("%s: %v", sf.file, err), ids.KindListeningSocket)
			complete = false
			continue
		}
		for i := range list {
			list[i].Scope = "host"
			list[i].ScopeID = "host"
		}
		records = append(records, list...)
	}

	if d.Config.Modules.Network.CollectContainerNamespaces && d.Index != nil {
		groups, _ := containerNetnsGroups(d)
		max := d.Config.Modules.Network.MaxNetns
		if len(groups) > max {
			groups = groups[:max]
			errs.add(events.ErrCodeLimitExceeded, "container network namespace limit reached", ids.KindListeningSocket)
			complete = false
		}
		for _, g := range groups {
			if ctx.Err() != nil {
				errs.add(events.ErrCodeTimeout, "container namespace scan timed out", ids.KindListeningSocket)
				complete = false
				break
			}
			for _, sf := range socketFiles {
				path := filepath.Join(d.Paths.ProcfsRoot, strconv.Itoa(g.pid), "net", sf.file)
				list, err := readSocketFile(path, sf.protocol, sf.family)
				if err != nil {
					if os.IsNotExist(err) {
						continue // the container may have exited
					}
					errs.add(events.ErrCodeParseError, fmt.Sprintf("%s/%s: %v", g.containerID, sf.file, err), ids.KindListeningSocket)
					complete = false
					continue
				}
				scopeID := "container-" + first12(g.containerID)
				for i := range list {
					list[i].Scope = "container"
					list[i].ScopeID = scopeID
					list[i].ContainerID = g.containerID
					list[i].EntityID = g.entityID
				}
				records = append(records, list...)
			}
		}
	}

	wanted := make(map[uint64]bool, len(records))
	for _, r := range records {
		if r.Inode > 0 {
			wanted[r.Inode] = true
		}
	}
	owners, timedOut := m.resolveSocketOwners(ctx, d.Paths.ProcfsRoot, wanted, d.Config.Modules.Network.FDWalkBudget)
	if timedOut {
		errs.add(events.ErrCodeFDWalkTimeout, "socket file-descriptor walk timed out", ids.KindListeningSocket)
		complete = false
	}

	bootID8 := readBootID8(d.Paths.ProcfsRoot)
	limit := d.Config.Limits.MaxEntitiesPerKind
	entities := make([]events.Entity, 0, len(records))
	rels := make([]events.Relationship, 0, len(records)*2)
	seen := map[string]bool{}
	processCache := map[int]struct {
		name string
		id   string
	}{}

	for _, r := range records {
		if ctx.Err() != nil {
			errs.add(events.ErrCodeTimeout, "socket scan timed out", ids.KindListeningSocket)
			complete = false
			break
		}
		if len(entities) >= limit {
			errs.add(events.ErrCodeLimitExceeded, "listening socket entity limit reached", ids.KindListeningSocket)
			complete = false
			break
		}
		pid := owners[r.Inode]
		var processName, processID string
		if pid > 0 {
			if cached, ok := processCache[pid]; ok {
				processName, processID = cached.name, cached.id
			} else {
				processName = readComm(d.Paths.ProcfsRoot, pid)
				if bootID8 != "" {
					processID = processEntityID(d.Paths.ProcfsRoot, d.Identity.HostID, bootID8, pid)
				}
				processCache[pid] = struct {
					name string
					id   string
				}{processName, processID}
			}
		}
		id := ids.SocketID(d.Identity.HostID, r.ScopeID, r.Protocol, r.Address, r.Port)
		if seen[id] {
			continue
		}
		seen[id] = true

		attrs := map[string]any{
			"protocol": r.Protocol,
			"family":   r.Family,
			"address":  r.Address,
			"port":     r.Port,
			"scope":    r.Scope,
		}
		if r.ContainerID != "" {
			attrs["container_id"] = r.ContainerID
		}
		if processName != "" {
			attrs["process_name"] = truncate(processName, 256)
		}
		label := processName
		if label == "" {
			label = "unknown"
		}
		name := fmt.Sprintf("%s:%d/%s", label, r.Port, r.Protocol)
		entities = append(entities, events.NewEntity(id, ids.KindListeningSocket, truncate(name, 256), attrs, now))

		target := hostID
		if r.Scope == "container" && r.EntityID != "" {
			target = r.EntityID
		}
		rels = append(rels, events.NewRelationship(ids.RelationshipID(id, "runs_on", target), id, "runs_on", target, nil, now))
		if processID != "" {
			rels = append(rels, events.NewRelationship(ids.RelationshipID(id, "owned_by", processID), id, "owned_by", processID, nil, now))
		}
	}
	sort.Slice(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })

	d.Emitter.Entities(snapID, entities)
	d.Emitter.Relationships(snapID, rels)
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module: "network", SnapshotID: snapID, ScopeEntityID: hostID,
		CoversKinds:        []string{ids.KindListeningSocket},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entities),
		RelationshipsCount: len(rels),
		Errors:             errs.list,
	})
	if errs.first != nil {
		m.setTaskErr("sockets", "sockets_incomplete", errs.first)
	} else {
		m.setTaskOK("sockets")
	}
	return nil
}

func first12(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// readSocketFile parses /proc/net/{tcp,tcp6,udp,udp6}.
func readSocketFile(path, protocol, family string) ([]socketRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	first := true
	var out []socketRecord
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if first {
			first = false
			continue
		}
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		local := fields[1]
		remote := fields[2]
		state := fields[3]
		inodeText := fields[9]
		switch protocol {
		case "tcp":
			if !strings.EqualFold(state, "0A") {
				continue
			}
		case "udp":
			if !strings.EqualFold(state, "07") {
				continue
			}
			if !remoteZero(remote) {
				continue
			}
		default:
			continue
		}
		addr, port, err := parseLocalAddress(local, family)
		if err != nil || port == 0 {
			continue
		}
		inode, err := strconv.ParseUint(inodeText, 10, 64)
		if err != nil || inode == 0 {
			continue
		}
		out = append(out, socketRecord{
			Protocol: protocol,
			Family:   family,
			Address:  addr,
			Port:     port,
			Inode:    inode,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func remoteZero(remote string) bool {
	addr, portText, ok := strings.Cut(remote, ":")
	if !ok {
		return false
	}
	if strings.Trim(portText, "0") != "" {
		return false
	}
	return strings.Trim(addr, "0") == ""
}

func parseLocalAddress(local, family string) (string, int, error) {
	addrHex, portHex, ok := strings.Cut(local, ":")
	if !ok {
		return "", 0, errors.New("invalid local address")
	}
	port, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil {
		return "", 0, err
	}
	addr, err := parseHexAddress(addrHex, family)
	if err != nil {
		return "", 0, err
	}
	return addr, int(port), nil
}

func parseHexAddress(s, family string) (string, error) {
	if family == "ipv4" {
		b, err := hex.DecodeString(s)
		if err != nil || len(b) != 4 {
			return "", fmt.Errorf("invalid IPv4 address %q", s)
		}
		return net.IPv4(b[3], b[2], b[1], b[0]).String(), nil
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 {
		return "", fmt.Errorf("invalid IPv6 address %q", s)
	}
	// Each 32-bit group is stored little-endian.
	out := make([]byte, 16)
	for g := 0; g < 4; g++ {
		for j := 0; j < 4; j++ {
			out[g*4+j] = b[g*4+(3-j)]
		}
	}
	return net.IP(out).String(), nil
}

// resolveSocketOwners maps socket inodes to owning PIDs, bounded by budget.
// Cached owners from the previous scan are reused.
func (m *Module) resolveSocketOwners(ctx context.Context, procfs string, wanted map[uint64]bool, budget time.Duration) (map[uint64]int, bool) {
	now := time.Now()
	owners := make(map[uint64]int, len(wanted))
	remaining := make(map[uint64]bool, len(wanted))

	m.mu.Lock()
	for inode := range wanted {
		if o, ok := m.socketCache[inode]; ok && now.Sub(o.at) < 10*time.Minute {
			owners[inode] = o.pid
		} else {
			remaining[inode] = true
		}
	}
	m.mu.Unlock()

	timedOut := false
	if len(remaining) > 0 {
		walkCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		pids := listPIDs(procfs)
		for _, pid := range pids {
			if walkCtx.Err() != nil {
				timedOut = true
				break
			}
			dir := filepath.Join(procfs, strconv.Itoa(pid), "fd")
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if walkCtx.Err() != nil {
					timedOut = true
					break
				}
				link, err := os.Readlink(filepath.Join(dir, e.Name()))
				if err != nil {
					continue
				}
				inode, ok := parseSocketLink(link)
				if !ok || !remaining[inode] {
					continue
				}
				owners[inode] = pid
				delete(remaining, inode)
			}
			if len(remaining) == 0 {
				break
			}
		}
	}

	m.mu.Lock()
	for inode, o := range m.socketCache {
		if _, ok := wanted[inode]; !ok || now.Sub(o.at) > 10*time.Minute {
			delete(m.socketCache, inode)
		}
	}
	for inode, pid := range owners {
		m.socketCache[inode] = socketOwner{pid: pid, at: now}
	}
	m.mu.Unlock()
	return owners, timedOut
}

func parseSocketLink(link string) (uint64, bool) {
	const prefix = "socket:["
	if !strings.HasPrefix(link, prefix) || !strings.HasSuffix(link, "]") {
		return 0, false
	}
	inode, err := strconv.ParseUint(link[len(prefix):len(link)-1], 10, 64)
	if err != nil {
		return 0, false
	}
	return inode, true
}

func listPIDs(procfs string) []int {
	entries, err := os.ReadDir(procfs)
	if err != nil {
		return nil
	}
	out := make([]int, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		out = append(out, pid)
	}
	return out
}

func readComm(procfs string, pid int) string {
	b, err := os.ReadFile(filepath.Join(procfs, strconv.Itoa(pid), "comm"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func readBootID8(procfs string) string {
	b, err := os.ReadFile(filepath.Join(procfs, "sys", "kernel", "random", "boot_id"))
	if err != nil {
		return ""
	}
	return ids.BootID8(strings.TrimSpace(string(b)))
}

func processEntityID(procfs, hostID, bootID8 string, pid int) string {
	b, err := os.ReadFile(filepath.Join(procfs, strconv.Itoa(pid), "stat"))
	if err != nil {
		return ""
	}
	s := string(b)
	idx := strings.LastIndex(s, ")")
	if idx < 0 || idx+2 > len(s) {
		return ""
	}
	fields := strings.Fields(s[idx+1:])
	// fields[0] is state (field 3), so starttime (field 22) is fields[19].
	if len(fields) < 20 {
		return ""
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return ""
	}
	return ids.ProcessID(hostID, bootID8, pid, start)
}
