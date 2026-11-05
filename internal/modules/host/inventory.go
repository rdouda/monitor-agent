package host

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"golang.org/x/sys/unix"
)

// buildEntity reads the slowly changing host facts.
func (m *Module) buildEntity(d core.Deps) (events.Entity, []error) {
	now := events.Stamp(d.Clock.Now())
	var errs []error

	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	hostname = truncate(hostname, 256)

	osr, err := readOSRelease(d.Paths.HostfsRoot)
	if err != nil {
		errs = append(errs, err)
	}

	kernel, arch, err := uname()
	if err != nil {
		errs = append(errs, err)
	}

	bootTime, bootID, err := readBootInfo(d.Paths.ProcfsRoot)
	if err != nil {
		errs = append(errs, err)
	}

	cpu, err := readCPUInfo(d.Paths.ProcfsRoot)
	if err != nil {
		errs = append(errs, err)
	}

	memTotal, err := readMemTotal(d.Paths.ProcfsRoot)
	if err != nil {
		errs = append(errs, err)
	}

	filesystems, fsErrs := m.collectFilesystems(d)
	errs = append(errs, fsErrs...)

	attrs := map[string]any{
		"hostname":      hostname,
		"os":            "linux",
		"agent_id":      d.Identity.AgentID,
		"agent_version": d.Version,
	}
	if osr.ID != "" {
		attrs["distribution"] = osr.ID
	}
	if osr.PrettyName != "" {
		attrs["distribution_name"] = osr.PrettyName
	}
	if osr.VersionID != "" {
		attrs["os_version"] = osr.VersionID
	}
	if kernel != "" {
		attrs["kernel_version"] = kernel
	}
	if arch == "" {
		arch = runtime.GOARCH
	}
	attrs["architecture"] = arch
	if !bootTime.IsZero() {
		attrs["boot_time"] = bootTime.UTC().Truncate(time.Millisecond)
	}
	if bootID != "" {
		attrs["boot_id"] = bootID
	}
	if tz := readTimezone(d.Paths.HostfsRoot); tz != "" {
		attrs["timezone"] = tz
	}
	if cpu != nil {
		attrs["cpu"] = cpu
	}
	if memTotal > 0 {
		attrs["memory_total_bytes"] = memTotal
	}
	if virt := detectVirtualization(d.Paths.HostfsRoot, d.Paths.SysfsRoot, d.Paths.ProcfsRoot); virt != nil {
		attrs["virtualization"] = virt
	}
	fsList := make([]map[string]string, 0, len(filesystems))
	for _, fs := range filesystems {
		fsList = append(fsList, map[string]string{
			"device": fs.Device, "mountpoint": fs.Mountpoint, "fstype": fs.FSType,
		})
	}
	attrs["filesystems"] = fsList

	labels := make(map[string]string, len(d.Config.Agent.Tags))
	for k, v := range d.Config.Agent.Tags {
		labels[k] = v
	}
	if len(labels) == 0 {
		labels = nil
	}

	return events.Entity{
		ID:         ids.HostID(d.Identity.HostID),
		Kind:       ids.KindHost,
		Name:       hostname,
		Attributes: attrs,
		Labels:     labels,
		ObservedAt: now,
	}, errs
}

type osRelease struct {
	ID         string
	PrettyName string
	VersionID  string
}

func readOSRelease(hostfsRoot string) (osRelease, error) {
	p := filepath.Join(hostfsRoot, "etc/os-release")
	f, err := os.Open(p)
	if err != nil {
		return osRelease{}, fmt.Errorf("os-release: %w", err)
	}
	defer f.Close()
	var out osRelease
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v = unquoteShell(v)
		switch k {
		case "ID":
			out.ID = v
		case "PRETTY_NAME":
			out.PrettyName = v
		case "VERSION_ID":
			out.VersionID = v
		}
	}
	if err := s.Err(); err != nil {
		return out, fmt.Errorf("os-release: %w", err)
	}
	return out, nil
}

func unquoteShell(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
	}
	v = strings.ReplaceAll(v, `\"`, `"`)
	v = strings.ReplaceAll(v, `\\`, `\`)
	return v
}

func uname() (kernel, arch string, err error) {
	var u unix.Utsname
	if err := unix.Uname(&u); err != nil {
		return "", "", fmt.Errorf("uname: %w", err)
	}
	return chars(u.Release[:]), chars(u.Machine[:]), nil
}

func chars(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

func readBootInfo(procfsRoot string) (time.Time, string, error) {
	var errs []error
	var bootTime time.Time
	f, err := os.Open(filepath.Join(procfsRoot, "stat"))
	if err == nil {
		s := bufio.NewScanner(f)
		for s.Scan() {
			fields := strings.Fields(s.Text())
			if len(fields) == 2 && fields[0] == "btime" {
				sec, perr := strconv.ParseInt(fields[1], 10, 64)
				if perr == nil {
					bootTime = time.Unix(sec, 0).UTC()
				}
				break
			}
		}
		if serr := s.Err(); serr != nil {
			errs = append(errs, fmt.Errorf("proc/stat: %w", serr))
		}
		_ = f.Close()
	} else {
		errs = append(errs, fmt.Errorf("proc/stat: %w", err))
	}
	bootID := ""
	b, err := os.ReadFile(filepath.Join(procfsRoot, "sys/kernel/random/boot_id"))
	if err != nil {
		errs = append(errs, fmt.Errorf("boot_id: %w", err))
	} else {
		bootID = strings.TrimSpace(string(b))
	}
	return bootTime, bootID, errors.Join(errs...)
}

func readCPUInfo(procfsRoot string) (map[string]any, error) {
	f, err := os.Open(filepath.Join(procfsRoot, "cpuinfo"))
	if err != nil {
		return nil, fmt.Errorf("cpuinfo: %w", err)
	}
	defer f.Close()

	logical := 0
	model, vendor := "", ""
	freqSum, freqCount := 0.0, 0
	physical := map[string]struct{}{}
	havePhysical := true

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		switch k {
		case "processor":
			logical++
		case "model name":
			if model == "" {
				model = v
			}
		case "vendor_id":
			if vendor == "" {
				vendor = v
			}
		case "physical id":
			physical["p="+v] = struct{}{}
			if v == "" {
				havePhysical = false
			}
		case "core id":
			if v == "" {
				havePhysical = false
			}
		case "cpu MHz":
			if hz, err := strconv.ParseFloat(v, 64); err == nil {
				freqSum += hz
				freqCount++
			}
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("cpuinfo: %w", err)
	}
	if logical == 0 {
		return nil, errors.New("cpuinfo: no processor entries")
	}
	cpu := map[string]any{"logical_cores": logical}
	if model != "" {
		cpu["model"] = model
	}
	if vendor != "" {
		cpu["vendor"] = vendor
	}
	if havePhysical && len(physical) > 0 {
		// Count distinct (physical id, core id) pairs. The map above only
		// tracked physical IDs, so parse again for the pairs.
		pairs := map[string]struct{}{}
		if f, err := os.Open(filepath.Join(procfsRoot, "cpuinfo")); err == nil {
			var phys, core string
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				k, v, ok := strings.Cut(sc.Text(), ":")
				if !ok {
					continue
				}
				switch strings.TrimSpace(k) {
				case "physical id":
					phys = strings.TrimSpace(v)
				case "core id":
					core = strings.TrimSpace(v)
					if phys != "" && core != "" {
						pairs[phys+"/"+core] = struct{}{}
					}
				}
			}
			_ = f.Close()
		}
		if len(pairs) > 0 {
			cpu["physical_cores"] = len(pairs)
		}
	}
	if freqCount > 0 {
		cpu["nominal_frequency_hz"] = int64(freqSum / float64(freqCount) * 1e6)
	}
	return cpu, nil
}

func readMemTotal(procfsRoot string) (uint64, error) {
	info, err := readMemInfo(procfsRoot)
	if err != nil {
		return 0, err
	}
	total, _ := info.get("MemTotal")
	return total, nil
}

func readTimezone(hostfsRoot string) string {
	p := filepath.Join(hostfsRoot, "etc/localtime")
	target, err := os.Readlink(p)
	if err == nil {
		if i := strings.Index(target, "zoneinfo/"); i >= 0 {
			return target[i+len("zoneinfo/"):]
		}
		if filepath.IsAbs(target) {
			if i := strings.Index(target, "/zoneinfo/"); i >= 0 {
				return target[i+len("/zoneinfo/"):]
			}
		}
	}
	if name, _ := time.Now().Zone(); name != "" {
		return name
	}
	return ""
}

func detectVirtualization(hostfsRoot, sysfsRoot, procfsRoot string) map[string]any {
	if b, err := os.ReadFile(filepath.Join(hostfsRoot, "run/systemd/container")); err == nil {
		runtime := strings.TrimSpace(string(b))
		v := map[string]any{"type": "container"}
		if runtime != "" {
			v["technology"] = runtime
		}
		return v
	}
	if _, err := os.Stat(filepath.Join(hostfsRoot, ".dockerenv")); err == nil {
		return map[string]any{"type": "container", "technology": "docker"}
	}
	if _, err := os.Stat(filepath.Join(hostfsRoot, "run/.containerenv")); err == nil {
		return map[string]any{"type": "container", "technology": "podman"}
	}

	vendor := strings.ToLower(readSmall(filepath.Join(sysfsRoot, "class/dmi/id/sys_vendor")))
	product := strings.ToLower(readSmall(filepath.Join(sysfsRoot, "class/dmi/id/product_name")))
	if tech := dmiTechnology(vendor, product); tech != "" {
		return map[string]any{"type": "vm", "technology": tech}
	}
	if vendor != "" || product != "" {
		return map[string]any{"type": "physical"}
	}
	if cpuinfoHasHypervisor(procfsRoot) {
		return map[string]any{"type": "vm"}
	}
	return map[string]any{"type": "unknown"}
}

func readSmall(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func dmiTechnology(vendor, product string) string {
	all := vendor + " " + product
	switch {
	case strings.Contains(all, "vmware"):
		return "vmware"
	case strings.Contains(all, "microsoft"), strings.Contains(all, "hyper-v"):
		return "hyperv"
	case strings.Contains(all, "xen"):
		return "xen"
	case strings.Contains(all, "qemu"), strings.Contains(all, "kvm"):
		return "kvm"
	case strings.Contains(all, "amazon"), strings.Contains(all, "google"), strings.Contains(all, "digitalocean"):
		return "kvm"
	default:
		return ""
	}
}

func cpuinfoHasHypervisor(procfsRoot string) bool {
	f, err := os.Open(filepath.Join(procfsRoot, "cpuinfo"))
	if err != nil {
		return false
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(strings.TrimSpace(line), "flags") || strings.HasPrefix(strings.TrimSpace(line), "Features") {
			return strings.Contains(line, "hypervisor")
		}
	}
	return false
}

func sortFilesystems(list []fsEntry) {
	sort.Slice(list, func(i, j int) bool { return list[i].Mountpoint < list[j].Mountpoint })
}
