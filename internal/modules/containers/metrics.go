package containers

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"
)

const unlimitedMemory = uint64(1) << 62

// PrimeMetrics seeds container CPU samples without emitting metrics. It is
// used by `agent once` before the second sample.
func (m *Module) PrimeMetrics(ctx context.Context) error {
	m.mu.Lock()
	states := make([]*engineState, 0, len(m.states))
	for _, st := range m.states {
		states = append(states, st)
	}
	m.mu.Unlock()

	var errs []error
	for _, st := range states {
		st.invMu.Lock()
		st.mu.Lock()
		adapter := st.adapter
		available := st.available
		st.mu.Unlock()
		if adapter == nil || !available {
			st.invMu.Unlock()
			continue
		}
		containers, err := adapter.ListContainers(ctx)
		if err != nil && len(containers) == 0 && !errors.Is(err, model.ErrPartialInspect) {
			errs = append(errs, err)
			st.invMu.Unlock()
			continue
		}
		ids := make([]string, 0, len(containers))
		for _, c := range containers {
			if c.State == "running" {
				ids = append(ids, c.ID)
			}
		}
		samples := map[string]statSample{}
		if len(ids) > 0 {
			stats, statsErr := adapter.Stats(ctx, ids)
			if statsErr != nil {
				errs = append(errs, statsErr)
			}
			now := m.getDeps().Clock.Now()
			for id, s := range stats {
				samples[id] = statSample{wall: now, cpu: s.CPUUsageNs}
			}
		}
		st.mu.Lock()
		st.stats = samples
		st.mu.Unlock()
		st.invMu.Unlock()
	}
	return errors.Join(errs...)
}

func (m *Module) metricsTask(ctx context.Context, st *engineState) error {
	st.invMu.Lock()
	defer st.invMu.Unlock()

	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	now := d.Clock.Now()
	st.mu.Lock()
	adapter := st.adapter
	available := st.available
	engineID := st.engineID
	key := st.key
	containers := make(map[string]model.Container, len(st.containers))
	for id, c := range st.containers {
		containers[id] = c
	}
	prev := make(map[string]statSample, len(st.stats))
	for id, s := range st.stats {
		prev[id] = s
	}
	st.mu.Unlock()

	if adapter == nil || !available {
		return nil
	}

	total := len(containers)
	var running, paused int
	idsToQuery := make([]string, 0, total)
	for _, c := range containers {
		switch c.State {
		case "running":
			running++
			idsToQuery = append(idsToQuery, c.ID)
		case "paused":
			paused++
		}
	}
	readings := make([]events.Reading, 0, running+1)
	readings = append(readings, events.Reading{
		EntityID: engineID,
		Metrics: []events.Metric{
			events.Gauge("container_engine.containers.total", float64(total), events.UnitCount, nil),
			events.Gauge("container_engine.containers.running", float64(running), events.UnitCount, nil),
			events.Gauge("container_engine.containers.paused", float64(paused), events.UnitCount, nil),
			events.Gauge("container_engine.containers.stopped", float64(total-running-paused), events.UnitCount, nil),
		},
	})

	var stats map[string]model.Stats
	var statsErr error
	if len(idsToQuery) > 0 {
		stats, statsErr = adapter.Stats(ctx, idsToQuery)
	}

	cores := readLogicalCores(d.Paths.ProcfsRoot)
	next := make(map[string]statSample, len(prev))
	newUtil := map[string]float64{}
	haveUtil := map[string]bool{}
	for _, c := range containers {
		if c.State != "running" {
			continue
		}
		s, ok := stats[c.ID]
		if !ok {
			continue
		}
		eid := ids.ContainerID(d.Identity.HostID, key, c.ID)
		if p, ok := prev[c.ID]; ok && s.CPUUsageNs >= p.cpu {
			wall := now.Sub(p.wall).Seconds()
			if wall > 0 {
				util := (float64(s.CPUUsageNs-p.cpu) / 1e9) / wall / float64(cores)
				newUtil[c.ID] = clamp01(util)
				haveUtil[c.ID] = true
			}
		}
		next[c.ID] = statSample{wall: now, cpu: s.CPUUsageNs}
		readings = append(readings, containerReading(eid, c, s, newUtil[c.ID], haveUtil[c.ID]))
	}

	st.mu.Lock()
	st.stats = next
	st.mu.Unlock()

	if len(readings) > 0 {
		d.Emitter.Metrics(now, readings)
	}
	if statsErr != nil {
		m.markEngineDegraded(st, "stats_incomplete", statsErr)
	} else {
		m.touchEngineHealthy(st)
	}
	return statsErr
}

func containerReading(entityID string, c model.Container, s model.Stats, utilization float64, haveUtil bool) events.Reading {
	metrics := make([]events.Metric, 0, 10)
	if haveUtil {
		metrics = append(metrics, events.Gauge("container.cpu.utilization", clamp01(utilization), events.UnitRatio, nil))
	}
	metrics = append(metrics, events.Counter("container.cpu.time", float64(s.CPUUsageNs)/1e9, events.UnitSeconds, nil))
	metrics = append(metrics, events.Gauge("container.memory.usage", float64(s.MemoryUsage), events.UnitBytes, nil))
	if s.MemoryLimit > 0 && s.MemoryLimit < unlimitedMemory {
		metrics = append(metrics, events.Gauge("container.memory.limit", float64(s.MemoryLimit), events.UnitBytes, nil))
		metrics = append(metrics, events.Gauge("container.memory.utilization", clamp01(float64(s.MemoryUsage)/float64(s.MemoryLimit)), events.UnitRatio, nil))
	}
	metrics = append(metrics,
		events.Counter("container.network.rx.bytes", float64(s.NetworkRxBytes), events.UnitBytes, nil),
		events.Counter("container.network.tx.bytes", float64(s.NetworkTxBytes), events.UnitBytes, nil),
		events.Counter("container.blkio.read.bytes", float64(s.BlockReadBytes), events.UnitBytes, nil),
		events.Counter("container.blkio.write.bytes", float64(s.BlockWriteBytes), events.UnitBytes, nil),
		events.Gauge("container.pids", float64(s.Pids), events.UnitCount, nil),
		events.Gauge("container.restarts", float64(c.RestartCount), events.UnitCount, nil),
	)
	return events.Reading{EntityID: entityID, Metrics: metrics}
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

// readLogicalCores counts processor entries in /proc/cpuinfo (not
// runtime.NumCPU, which honours CPU affinity).
func readLogicalCores(procfsRoot string) int {
	f, err := os.Open(filepath.Join(procfsRoot, "cpuinfo"))
	if err == nil {
		defer f.Close()
		count := 0
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "processor") {
				if _, value, ok := strings.Cut(line, ":"); ok {
					if _, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
						count++
					}
				}
			}
		}
		if count > 0 {
			return count
		}
	}
	if n := runtime.NumCPU(); n > 0 {
		return n
	}
	return 1
}

func (m *Module) touchEngineHealthy(st *engineState) {
	now := m.getDeps().Clock.Now()
	m.mu.Lock()
	if m.health.Status != core.ModuleUnavailable {
		m.health.LastSuccessAt = now
	}
	m.mu.Unlock()
}
