package process

import (
	"context"
	"errors"
	"sort"
	"time"

	"agent/internal/events"
	"agent/internal/ids"
)

type procKey struct {
	pid   int
	start uint64
}

type lifecycleProc struct {
	pid   int
	start uint64
	name  string
}

// seedLifecycleBaseline records the processes that exist when the agent
// starts. They are reported by the initial snapshot only; no lifecycle events
// are emitted for them.
func (m *Module) seedLifecycleBaseline(entries []lifecycleProc) {
	known := make(map[procKey]lifecycleProc, len(entries))
	for _, e := range entries {
		known[procKey{pid: e.pid, start: e.start}] = e
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.lifecycleBaseline {
		return
	}
	m.lifecycleKnown = known
	m.lifecycleBaseline = true
}

func (m *Module) lifecycleTask(ctx context.Context) error {
	d := m.getDeps()
	scanAt := d.Clock.Now()

	current := map[procKey]lifecycleProc{}
	var scanErr error
	pids, err := listPIDs(d.Paths.ProcfsRoot)
	if err != nil {
		m.markDegraded("lifecycle_failed", err)
		return nil
	}
	for i, pid := range pids {
		if i%128 == 0 && ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				scanErr = ctx.Err()
			}
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
		key := procKey{pid: st.PID, start: st.StartTime}
		current[key] = lifecycleProc{pid: st.PID, start: st.StartTime, name: name}
	}
	if err := ctx.Err(); err != nil {
		m.markDegraded("lifecycle_incomplete", err)
		return nil
	}

	m.lifecycleMu.Lock()
	if !m.lifecycleBaseline {
		m.lifecycleKnown = current
		m.lifecycleBaseline = true
		m.lifecycleMu.Unlock()
		m.touchSuccess()
		return nil
	}
	known := m.lifecycleKnown

	var pending []events.Lifecycle
	for _, key := range sortedKeys(current) {
		if _, ok := known[key]; ok {
			continue
		}
		info := current[key]
		pending = append(pending, events.Lifecycle{
			EntityID:   ids.ProcessID(d.Identity.HostID, m.bootID8, key.pid, key.start),
			Kind:       ids.KindProcess,
			Name:       truncate(info.name, 256),
			Status:     events.StatusRunning,
			Reason:     events.ReasonProcessStarted,
			ObservedAt: events.Stamp(scanAt),
		})
	}
	for _, key := range sortedKeys(known) {
		if _, ok := current[key]; ok {
			continue
		}
		info := known[key]
		previous := events.StatusRunning
		pending = append(pending, events.Lifecycle{
			EntityID:       ids.ProcessID(d.Identity.HostID, m.bootID8, key.pid, key.start),
			Kind:           ids.KindProcess,
			Name:           truncate(info.name, 256),
			PreviousStatus: &previous,
			Status:         events.StatusExited,
			Reason:         events.ReasonProcessExited,
			ObservedAt:     events.Stamp(scanAt),
		})
	}

	// Enforce the lifecycle rate limit on a rolling one-minute window.
	maxEvents := d.Config.Limits.MaxLifecycleEventsPerMinute
	cutoff := scanAt.Add(-time.Minute)
	i := 0
	for i < len(m.lifecycleTimes) && !m.lifecycleTimes[i].After(cutoff) {
		i++
	}
	m.lifecycleTimes = m.lifecycleTimes[i:]

	var dropped int64
	toEmit := make([]events.Lifecycle, 0, len(pending))
	for _, ev := range pending {
		if len(m.lifecycleTimes) >= maxEvents {
			dropped++
			continue
		}
		m.lifecycleTimes = append(m.lifecycleTimes, scanAt)
		toEmit = append(toEmit, ev)
	}
	m.lifecycleKnown = current
	m.lifecycleMu.Unlock()

	if dropped > 0 && d.RateLimited != nil {
		d.RateLimited(dropped)
	}
	for _, ev := range toEmit {
		d.Emitter.Lifecycle(ev)
	}

	if scanErr != nil {
		m.markDegraded("lifecycle_incomplete", scanErr)
	} else {
		m.touchSuccess()
	}
	return nil
}

func sortedKeys[V any](m map[procKey]V) []procKey {
	keys := make([]procKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].pid != keys[j].pid {
			return keys[i].pid < keys[j].pid
		}
		return keys[i].start < keys[j].start
	})
	return keys
}
