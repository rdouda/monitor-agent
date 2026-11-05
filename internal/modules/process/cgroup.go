package process

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// cgroupInfo is the subset of a cgroup file used by the process module.
type cgroupInfo struct {
	Path        string
	ContainerID string
	RuntimeHint string
}

type cgroupLine struct {
	hierarchy   string
	controllers string
	path        string
}

var cgroupPatterns = []struct {
	re      *regexp.Regexp
	runtime string
}{
	{regexp.MustCompile(`docker-([0-9a-f]{64})\.scope`), "docker"},
	{regexp.MustCompile(`/docker/([0-9a-f]{64})`), "docker"},
	{regexp.MustCompile(`libpod-([0-9a-f]{64})\.scope`), "podman"},
	{regexp.MustCompile(`/libpod/([0-9a-f]{64})`), "podman"},
	{regexp.MustCompile(`cri-containerd-([0-9a-f]{64})\.scope`), "containerd"},
	{regexp.MustCompile(`crio-([0-9a-f]{64})\.scope`), "cri-o"},
}

func readCgroup(procfsRoot string, pid int) (cgroupInfo, error) {
	p := filepath.Join(procfsRoot, strconv.Itoa(pid), "cgroup")
	data, err := os.ReadFile(p)
	if err != nil {
		return cgroupInfo{}, fmt.Errorf("pid %d cgroup: %w", pid, err)
	}
	return parseCgroup(string(data)), nil
}

// parseCgroup extracts the reported cgroup path and a container hint.
//
// cgroup v2 uses a single "0::/path" line. On v1 the name=systemd (or cpu)
// line is preferred for the reported path and for hint detection.
func parseCgroup(content string) cgroupInfo {
	var lines []cgroupLine
	for _, raw := range strings.Split(content, "\n") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.SplitN(raw, ":", 3)
		if len(parts) != 3 {
			continue
		}
		lines = append(lines, cgroupLine{
			hierarchy:   parts[0],
			controllers: parts[1],
			path:        parts[2],
		})
	}
	if len(lines) == 0 {
		return cgroupInfo{}
	}

	var info cgroupInfo
	info.Path = preferredPath(lines)
	for _, path := range preferredPaths(lines) {
		if id, runtime := containerHint(path); id != "" {
			info.ContainerID = id
			info.RuntimeHint = runtime
			break
		}
	}
	return info
}

func preferredPath(lines []cgroupLine) string {
	for _, l := range lines {
		if l.hierarchy == "0" && l.controllers == "" {
			return l.path
		}
	}
	for _, name := range []string{"systemd", "cpu"} {
		for _, l := range lines {
			if hasController(l.controllers, name) {
				return l.path
			}
		}
	}
	return lines[0].path
}

// preferredPaths orders paths so that the cgroup v1 systemd/cpu lines are
// inspected before unrelated hierarchies.
func preferredPaths(lines []cgroupLine) []string {
	var unified, systemd, cpu, rest []string
	for _, l := range lines {
		switch {
		case l.hierarchy == "0" && l.controllers == "":
			unified = append(unified, l.path)
		case hasController(l.controllers, "systemd"):
			systemd = append(systemd, l.path)
		case hasController(l.controllers, "cpu"):
			cpu = append(cpu, l.path)
		default:
			rest = append(rest, l.path)
		}
	}
	out := make([]string, 0, len(lines))
	out = append(out, unified...)
	out = append(out, systemd...)
	out = append(out, cpu...)
	out = append(out, rest...)
	return out
}

func hasController(controllers, name string) bool {
	for _, c := range strings.Split(controllers, ",") {
		c = strings.TrimSpace(c)
		if c == name || (name == "systemd" && c == "name=systemd") {
			return true
		}
	}
	return false
}

func containerHint(path string) (string, string) {
	for _, p := range cgroupPatterns {
		if m := p.re.FindStringSubmatch(path); len(m) == 2 {
			return m[1], p.runtime
		}
	}
	return "", ""
}
