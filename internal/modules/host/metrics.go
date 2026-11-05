package host

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"agent/internal/events"
)

type cpuSample struct {
	idle, iowait, steal, total uint64
}

type memInfo struct {
	kb map[string]uint64
}

func (m memInfo) get(key string) (uint64, bool) {
	v, ok := m.kb[key]
	return v, ok
}

func readMemInfo(procfsRoot string) (memInfo, error) {
	f, err := os.Open(filepath.Join(procfsRoot, "meminfo"))
	if err != nil {
		return memInfo{}, fmt.Errorf("meminfo: %w", err)
	}
	defer f.Close()
	out := memInfo{kb: map[string]uint64{}}
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) >= 3 && fields[2] == "kB" {
			v *= 1024
		}
		out.kb[key] = v
	}
	if err := s.Err(); err != nil {
		return out, fmt.Errorf("meminfo: %w", err)
	}
	if _, ok := out.kb["MemTotal"]; !ok {
		return out, errors.New("meminfo: MemTotal missing")
	}
	return out, nil
}

func readCPUSample(procfsRoot string) (cpuSample, error) {
	f, err := os.Open(filepath.Join(procfsRoot, "stat"))
	if err != nil {
		return cpuSample{}, fmt.Errorf("proc/stat: %w", err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 9 || fields[0] != "cpu" {
			continue
		}
		var v [8]uint64
		for i := 0; i < 8; i++ {
			n, err := strconv.ParseUint(fields[1+i], 10, 64)
			if err != nil {
				return cpuSample{}, fmt.Errorf("proc/stat cpu field %d: %w", i, err)
			}
			v[i] = n
		}
		// user nice system idle iowait irq softirq steal. Guest time is
		// already included in user/nice and must not be added.
		total := v[0] + v[1] + v[2] + v[3] + v[4] + v[5] + v[6] + v[7]
		return cpuSample{idle: v[3], iowait: v[4], steal: v[7], total: total}, nil
	}
	if err := s.Err(); err != nil {
		return cpuSample{}, fmt.Errorf("proc/stat: %w", err)
	}
	return cpuSample{}, errors.New("proc/stat: aggregate cpu line missing")
}

func readUptime(procfsRoot string) (float64, error) {
	b, err := os.ReadFile(filepath.Join(procfsRoot, "uptime"))
	if err != nil {
		return 0, fmt.Errorf("uptime: %w", err)
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return 0, errors.New("uptime: empty")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, fmt.Errorf("uptime: %w", err)
	}
	return v, nil
}

func readLoadAvg(procfsRoot string) ([3]float64, error) {
	var out [3]float64
	b, err := os.ReadFile(filepath.Join(procfsRoot, "loadavg"))
	if err != nil {
		return out, fmt.Errorf("loadavg: %w", err)
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return out, errors.New("loadavg: too few fields")
	}
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return out, fmt.Errorf("loadavg: %w", err)
		}
		out[i] = v
	}
	return out, nil
}

func readCPUFrequency(sysfsRoot string) (float64, bool) {
	pattern := filepath.Join(sysfsRoot, "devices/system/cpu/cpu*/cpufreq/scaling_cur_freq")
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		return 0, false
	}
	var sum float64
	var n int
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		khz, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
		if err != nil {
			continue
		}
		sum += khz * 1000
		n++
	}
	if n == 0 {
		return 0, false
	}
	return sum / float64(n), true
}

// PrimeMetrics seeds the CPU sample used for rate calculations without
// emitting any metric.
func (m *Module) PrimeMetrics(ctx context.Context) error {
	d := m.getDeps()
	cur, err := readCPUSample(d.Paths.ProcfsRoot)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.prevCPU = cur
	m.hasPrevCPU = true
	m.mu.Unlock()
	return nil
}

func (m *Module) metricsTask(ctx context.Context) error {
	d := m.getDeps()
	now := d.Clock.Now()
	var metrics []events.Metric
	var errs []error

	if up, err := readUptime(d.Paths.ProcfsRoot); err == nil {
		metrics = append(metrics, events.Gauge("host.uptime", up, events.UnitSeconds, nil))
	} else {
		errs = append(errs, err)
	}

	if load, err := readLoadAvg(d.Paths.ProcfsRoot); err == nil {
		metrics = append(metrics,
			events.Gauge("host.load.1m", load[0], events.UnitCount, nil),
			events.Gauge("host.load.5m", load[1], events.UnitCount, nil),
			events.Gauge("host.load.15m", load[2], events.UnitCount, nil),
		)
	} else {
		errs = append(errs, err)
	}

	cur, err := readCPUSample(d.Paths.ProcfsRoot)
	if err != nil {
		errs = append(errs, err)
	} else {
		m.mu.Lock()
		prev, hasPrev := m.prevCPU, m.hasPrevCPU
		m.prevCPU = cur
		m.hasPrevCPU = true
		m.mu.Unlock()

		if hasPrev && cur.total > prev.total {
			dt := float64(cur.total - prev.total)
			di := float64(cur.idle - prev.idle)
			diow := float64(cur.iowait - prev.iowait)
			dsteal := float64(cur.steal - prev.steal)
			if dt > 0 {
				metrics = append(metrics,
					events.Gauge("host.cpu.utilization", clamp01(1-(di+diow)/dt), events.UnitRatio, nil),
					events.Gauge("host.cpu.iowait", clamp01(diow/dt), events.UnitRatio, nil),
					events.Gauge("host.cpu.steal", clamp01(dsteal/dt), events.UnitRatio, nil),
				)
			}
		}
	}

	if hz, ok := readCPUFrequency(d.Paths.SysfsRoot); ok {
		metrics = append(metrics, events.Gauge("host.cpu.frequency", hz, events.UnitHertz, nil))
	}

	if mem, err := readMemInfo(d.Paths.ProcfsRoot); err == nil {
		total, _ := mem.get("MemTotal")
		avail, hasAvail := mem.get("MemAvailable")
		if hasAvail && total > 0 {
			used := total - avail
			metrics = append(metrics,
				events.Gauge("host.memory.used", float64(used), events.UnitBytes, nil),
				events.Gauge("host.memory.available", float64(avail), events.UnitBytes, nil),
				events.Gauge("host.memory.utilization", clamp01(float64(used)/float64(total)), events.UnitRatio, nil),
			)
		}
		if cached, ok := mem.get("Cached"); ok {
			metrics = append(metrics, events.Gauge("host.memory.cached", float64(cached), events.UnitBytes, nil))
		}
		if buffers, ok := mem.get("Buffers"); ok {
			metrics = append(metrics, events.Gauge("host.memory.buffers", float64(buffers), events.UnitBytes, nil))
		}
		swapTotal, hasSwapTotal := mem.get("SwapTotal")
		swapFree, hasSwapFree := mem.get("SwapFree")
		if hasSwapTotal {
			metrics = append(metrics, events.Gauge("host.swap.total", float64(swapTotal), events.UnitBytes, nil))
			if hasSwapFree {
				used := swapTotal - swapFree
				metrics = append(metrics, events.Gauge("host.swap.used", float64(used), events.UnitBytes, nil))
				if swapTotal > 0 {
					metrics = append(metrics, events.Gauge("host.swap.utilization", clamp01(float64(used)/float64(swapTotal)), events.UnitRatio, nil))
				}
			}
		}
	} else {
		errs = append(errs, err)
	}

	if ioMetrics, err := m.diskIOStats(d); err == nil {
		metrics = append(metrics, ioMetrics...)
	} else {
		errs = append(errs, err)
	}

	if len(metrics) > 0 {
		d.Emitter.Metrics(now, chunkReadings(m.hostEntityID(), metrics))
	}
	if len(errs) > 0 {
		m.markDegraded("metrics_incomplete", errors.Join(errs...))
		m.log().Debug("host metrics incomplete", "error", errors.Join(errs...))
	} else {
		m.touchSuccess()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// touchSuccess updates last-success without changing an existing degraded
// status (only a complete inventory clears that).
func (m *Module) touchSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.LastSuccessAt = m.deps.Clock.Now()
}
