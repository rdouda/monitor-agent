package process

import (
	"context"
	"path"
	"sort"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
)

type cpuSample struct {
	wall  time.Time
	ticks uint64
}

type procSample struct {
	key         procKey
	name        string
	ticks       uint64
	rss         uint64
	status      procStatus
	cpuDelta    uint64
	utilization float64
	hasUtil     bool
}

func (m *Module) metricsTask(ctx context.Context) error {
	return m.metricsRun(ctx, true)
}

// PrimeMetrics seeds per-process CPU samples without emitting metrics.
func (m *Module) PrimeMetrics(ctx context.Context) error {
	return m.metricsRun(ctx, false)
}

func (m *Module) metricsRun(ctx context.Context, emit bool) error {
	d := m.getDeps()
	now := d.Clock.Now()

	pids, err := listPIDs(d.Paths.ProcfsRoot)
	if err != nil {
		m.markDegraded("metrics_failed", err)
		return nil
	}

	samples := make([]*procSample, 0, len(pids))
	var scanErr error
	for i, pid := range pids {
		if i%128 == 0 && ctx.Err() != nil {
			break
		}
		st, err := readStat(d.Paths.ProcfsRoot, pid)
		if err != nil {
			if !skipProcessError(err) {
				scanErr = err
			}
			continue
		}
		if !m.includeKernel && st.Flags&pfKThread != 0 {
			continue
		}
		name := truncate(st.Comm, 256)
		if name == "" {
			name = "unknown"
		}
		if !nameAllowed(name, m.includeNames, m.excludeNames) {
			continue
		}
		status, statusErr := readStatus(d.Paths.ProcfsRoot, pid)
		if statusErr != nil {
			if skipProcessError(statusErr) {
				continue
			}
			scanErr = statusErr
		}
		s := &procSample{
			key:    procKey{pid: st.PID, start: st.StartTime},
			name:   name,
			ticks:  st.UTime + st.STime,
			status: status,
		}
		if status.HasVmRSS {
			s.rss = status.VmRSS
		}
		samples = append(samples, s)
	}
	if err := ctx.Err(); err != nil {
		m.markDegraded("metrics_incomplete", err)
		return nil
	}

	cores := m.logicalCores
	if cores < 1 {
		cores = 1
	}

	m.metricsMu.Lock()
	prev := m.prevSamples
	current := make(map[procKey]cpuSample, len(samples))
	for _, s := range samples {
		current[s.key] = cpuSample{wall: now, ticks: s.ticks}
		if p, ok := prev[s.key]; ok {
			elapsed := now.Sub(p.wall).Seconds()
			if elapsed > 0 && s.ticks >= p.ticks {
				s.cpuDelta = s.ticks - p.ticks
				s.utilization = clamp01((float64(s.cpuDelta) / userHZ) / elapsed / float64(cores))
				s.hasUtil = true
			}
		}
	}
	m.prevSamples = current
	m.metricsMu.Unlock()

	if emit {
		selected := selectMetrics(samples, d.Config.Modules.Process.MetricsMode, d.Config.Modules.Process.TopN, m.includeNames)
		readings := make([]events.Reading, 0, len(selected))
		for _, s := range selected {
			readings = append(readings, m.buildReading(d, s))
		}
		if len(readings) > 0 {
			d.Emitter.Metrics(now, readings)
		}
	}

	if scanErr != nil {
		m.markDegraded("metrics_incomplete", scanErr)
	} else {
		m.touchSuccess()
	}
	return nil
}

func selectMetrics(samples []*procSample, mode string, topN int, include []string) []*procSample {
	switch mode {
	case "all":
		return sortedSamples(samples)
	case "allowlist":
		if len(include) == 0 {
			return nil
		}
		return sortedSamples(samples)
	case "top_n":
		selected := make(map[procKey]bool, topN*2)
		byCPU := append([]*procSample(nil), samples...)
		sort.SliceStable(byCPU, func(i, j int) bool {
			if byCPU[i].cpuDelta != byCPU[j].cpuDelta {
				return byCPU[i].cpuDelta > byCPU[j].cpuDelta
			}
			return byCPU[i].key.pid < byCPU[j].key.pid
		})
		for i := 0; i < topN && i < len(byCPU); i++ {
			selected[byCPU[i].key] = true
		}
		byRSS := append([]*procSample(nil), samples...)
		sort.SliceStable(byRSS, func(i, j int) bool {
			if byRSS[i].rss != byRSS[j].rss {
				return byRSS[i].rss > byRSS[j].rss
			}
			return byRSS[i].key.pid < byRSS[j].key.pid
		})
		for i := 0; i < topN && i < len(byRSS); i++ {
			selected[byRSS[i].key] = true
		}
		for _, s := range samples {
			if matchesAny(s.name, include) {
				selected[s.key] = true
			}
		}
		out := make([]*procSample, 0, len(selected))
		for _, s := range samples {
			if selected[s.key] {
				out = append(out, s)
			}
		}
		return sortedSamples(out)
	default:
		return sortedSamples(samples)
	}
}

func sortedSamples(samples []*procSample) []*procSample {
	out := append([]*procSample(nil), samples...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].key.pid != out[j].key.pid {
			return out[i].key.pid < out[j].key.pid
		}
		return out[i].key.start < out[j].key.start
	})
	return out
}

func matchesAny(name string, globs []string) bool {
	for _, glob := range globs {
		if ok, _ := path.Match(glob, name); ok {
			return true
		}
	}
	return false
}

func (m *Module) buildReading(d core.Deps, s *procSample) events.Reading {
	metrics := make([]events.Metric, 0, 6)
	metrics = append(metrics, events.Counter("process.cpu.time", float64(s.ticks)/userHZ, events.UnitSeconds, nil))
	if s.hasUtil {
		metrics = append(metrics, events.Gauge("process.cpu.utilization", s.utilization, events.UnitRatio, nil))
	}
	if s.status.HasVmRSS {
		metrics = append(metrics, events.Gauge("process.memory.rss", float64(s.status.VmRSS), events.UnitBytes, nil))
	}
	if s.status.HasVmSize {
		metrics = append(metrics, events.Gauge("process.memory.virtual", float64(s.status.VmSize), events.UnitBytes, nil))
	}
	if s.status.HasThreads {
		metrics = append(metrics, events.Gauge("process.threads", float64(s.status.Threads), events.UnitCount, nil))
	}
	if fds, ok := readFDCount(d.Paths.ProcfsRoot, s.key.pid); ok {
		metrics = append(metrics, events.Gauge("process.open_fds", float64(fds), events.UnitCount, nil))
	}
	return events.Reading{
		EntityID: ids.ProcessID(d.Identity.HostID, m.bootID8, s.key.pid, s.key.start),
		Metrics:  metrics,
	}
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
