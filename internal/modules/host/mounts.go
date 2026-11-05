package host

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"agent/internal/core"
	"agent/internal/events"
	"golang.org/x/sys/unix"
)

type fsEntry struct {
	Device     string
	Mountpoint string
	FSType     string
}

// filesystemStatfs is replaceable in tests.
var filesystemStatfs = statfs

type mountEntry struct {
	DevKey     string
	Root       string
	Mountpoint string
	FSType     string
	Source     string
}

func (m *Module) collectFilesystems(d core.Deps) ([]fsEntry, []error) {
	f, err := os.Open(filepath.Join(d.Paths.ProcfsRoot, "self/mountinfo"))
	if err != nil {
		return nil, []error{fmt.Errorf("mountinfo: %w", err)}
	}
	defer f.Close()

	entries, err := parseMountInfo(f)
	if err != nil {
		return nil, []error{err}
	}
	cfg := d.Config.Modules.Host
	seen := map[string]struct{}{}
	var out []fsEntry
	for _, e := range entries {
		if excludedFS(e, cfg.ExcludeFSTypes, cfg.ExcludeMountPrefixes) {
			continue
		}
		key := e.DevKey + "|" + e.Root
		if _, ok := seen[key]; ok {
			continue
		}
		mp := hostPath(d.Paths.HostfsRoot, e.Mountpoint)
		var st statFS
		if err := filesystemStatfs(mp, &st); err != nil || st.Blocks == 0 {
			// Unavailable or pseudo filesystems are omitted rather than
			// reported with zero capacity.
			continue
		}
		seen[key] = struct{}{}
		out = append(out, fsEntry{Device: e.Source, Mountpoint: e.Mountpoint, FSType: e.FSType})
	}
	sortFilesystems(out)

	m.mu.Lock()
	m.filesystems = out
	m.mu.Unlock()
	return out, nil
}

func parseMountInfo(r io.Reader) ([]mountEntry, error) {
	var out []mountEntry
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 1024*1024)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		sep := -1
		for i, f := range fields {
			if f == "-" {
				sep = i
				break
			}
		}
		if sep < 6 || len(fields) < sep+3 {
			continue
		}
		out = append(out, mountEntry{
			DevKey:     fields[2],
			Root:       unescapeMount(fields[3]),
			Mountpoint: unescapeMount(fields[4]),
			FSType:     unescapeMount(fields[sep+1]),
			Source:     unescapeMount(fields[sep+2]),
		})
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("parse mountinfo: %w", err)
	}
	return out, nil
}

func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			v := (s[i+1]-'0')*64 + (s[i+2]-'0')*8 + (s[i+3] - '0')
			b.WriteByte(v)
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isOctal(c byte) bool { return c >= '0' && c <= '7' }

func excludedFS(e mountEntry, fstypes, prefixes []string) bool {
	for _, t := range fstypes {
		if e.FSType == t {
			return true
		}
	}
	for _, p := range prefixes {
		p = strings.TrimSuffix(p, "/")
		if p == "" {
			continue
		}
		if e.Mountpoint == p || strings.HasPrefix(e.Mountpoint, p+"/") {
			return true
		}
	}
	return false
}

func hostPath(root, p string) string {
	if root == "" || root == "/" {
		return p
	}
	return filepath.Join(root, p)
}

func (m *Module) cachedFilesystems() []fsEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]fsEntry(nil), m.filesystems...)
}

func (m *Module) diskUsageTask(ctx context.Context) error {
	d := m.getDeps()
	files := m.cachedFilesystems()
	if len(files) == 0 {
		var errs []error
		files, errs = m.collectFilesystems(d)
		if len(errs) > 0 {
			m.log().Debug("filesystem inventory failed", "error", errs[0])
		}
	}
	now := d.Clock.Now()
	var metrics []events.Metric
	measured := 0
	for _, fs := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		mp := hostPath(d.Paths.HostfsRoot, fs.Mountpoint)
		var st statFS
		if err := filesystemStatfs(mp, &st); err != nil {
			m.log().Debug("statfs failed", "mountpoint", fs.Mountpoint, "error", err)
			continue
		}
		if st.Blocks == 0 {
			continue
		}
		measured++
		attrs := map[string]any{"mountpoint": fs.Mountpoint, "device": fs.Device, "fstype": fs.FSType}
		total := st.Blocks * uint64(st.Bsize)
		used := (st.Blocks - st.Bfree) * uint64(st.Bsize)
		available := st.Bavail * uint64(st.Bsize)
		metrics = append(metrics,
			events.Gauge("host.disk.total", float64(total), events.UnitBytes, attrs),
			events.Gauge("host.disk.used", float64(used), events.UnitBytes, attrs),
			events.Gauge("host.disk.available", float64(available), events.UnitBytes, attrs),
		)
		if denom := used + available; denom > 0 {
			metrics = append(metrics, events.Gauge("host.disk.utilization", clamp01(float64(used)/float64(denom)), events.UnitRatio, attrs))
		}
		if st.Files > 0 {
			usedInodes := st.Files - st.Ffree
			metrics = append(metrics, events.Gauge("host.disk.inodes.utilization", clamp01(float64(usedInodes)/float64(st.Files)), events.UnitRatio, attrs))
		}
	}
	if len(metrics) > 0 {
		d.Emitter.Metrics(now, chunkReadings(m.hostEntityID(), metrics))
	}
	if measured == 0 && len(files) > 0 {
		m.log().Debug("no filesystem could be measured", "candidates", len(files))
	}
	return nil
}

func chunkReadings(entityID string, metrics []events.Metric) []events.Reading {
	var out []events.Reading
	for start := 0; start < len(metrics); start += 64 {
		end := start + 64
		if end > len(metrics) {
			end = len(metrics)
		}
		out = append(out, events.Reading{EntityID: entityID, Metrics: metrics[start:end]})
	}
	return out
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// diskIOStats reads counters from /proc/diskstats for whole devices.
func (m *Module) diskIOStats(d core.Deps) ([]events.Metric, error) {
	f, err := os.Open(filepath.Join(d.Paths.ProcfsRoot, "diskstats"))
	if err != nil {
		return nil, fmt.Errorf("diskstats: %w", err)
	}
	defer f.Close()

	var excludes []*regexp.Regexp
	for _, pat := range d.Config.Modules.Host.ExcludeDevices {
		re, err := regexp.Compile(pat)
		if err != nil {
			continue
		}
		excludes = append(excludes, re)
	}

	var metrics []events.Metric
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if _, err := os.Stat(filepath.Join(d.Paths.SysfsRoot, "block", name)); err != nil {
			continue // partition or virtual device without a block entry
		}
		skip := false
		for _, re := range excludes {
			if re.MatchString(name) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		values := make([]uint64, 11)
		ok := true
		for i := 0; i < 11; i++ {
			v, err := strconv.ParseUint(fields[3+i], 10, 64)
			if err != nil {
				ok = false
				break
			}
			values[i] = v
		}
		if !ok {
			continue
		}
		attrs := map[string]any{"device": name}
		metrics = append(metrics,
			events.Counter("host.disk.io.read.bytes", float64(values[2]*512), events.UnitBytes, attrs),
			events.Counter("host.disk.io.write.bytes", float64(values[6]*512), events.UnitBytes, attrs),
			events.Counter("host.disk.io.read.ops", float64(values[0]), events.UnitCount, attrs),
			events.Counter("host.disk.io.write.ops", float64(values[4]), events.UnitCount, attrs),
			events.Counter("host.disk.io.time", float64(values[9])/1000.0, events.UnitSeconds, attrs),
		)
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("parse diskstats: %w", err)
	}
	return metrics, nil
}

type statFS struct {
	Bsize  int64
	Blocks uint64
	Bfree  uint64
	Bavail uint64
	Files  uint64
	Ffree  uint64
}

func statfs(path string, out *statFS) error {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return err
	}
	out.Bsize = st.Bsize
	out.Blocks = st.Blocks
	out.Bfree = st.Bfree
	out.Bavail = st.Bavail
	out.Files = st.Files
	out.Ffree = st.Ffree
	return nil
}
