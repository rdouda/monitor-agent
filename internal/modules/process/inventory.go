package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"agent/internal/events"
	"agent/internal/ids"
)

const maxSnapshotErrors = 20

type procEntry struct {
	stat          procStat
	entity        events.Entity
	containerHint string
}

type scanErrors struct {
	list  []events.SnapshotError
	first error
}

func (s *scanErrors) add(code, message string) {
	if len(s.list) < maxSnapshotErrors {
		s.list = append(s.list, events.SnapshotError{
			Code:    code,
			Message: truncate(message, 500),
			Kind:    ids.KindProcess,
		})
	}
	if s.first == nil {
		s.first = errors.New(message)
	}
}

// inventoryTask emits process entities, relationships and a snapshot record.
func (m *Module) inventoryTask(ctx context.Context) error {
	m.inventoryMu.Lock()
	defer m.inventoryMu.Unlock()

	d := m.getDeps()
	started := d.Clock.Now()
	now := events.Stamp(d.Clock.Now())
	snapID, err := snapshotID()
	if err != nil {
		m.markDegraded("snapshot_id", err)
		return err
	}

	var errs scanErrors
	hostID := ids.HostID(d.Identity.HostID)
	maxEntities := d.Config.Limits.MaxEntitiesPerKind
	entries := make([]procEntry, 0, 256)
	var baseline []lifecycleProc
	limitHit := false
	overLimit := 0

	pids, err := listPIDs(d.Paths.ProcfsRoot)
	if err != nil {
		errs.add(scanErrCode(err), err.Error())
	}

	for i, pid := range pids {
		if i%128 == 0 && ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				errs.add(events.ErrCodeTimeout, "process inventory timed out")
			} else {
				errs.add(events.ErrCodeInternal, "process inventory canceled")
			}
			break
		}

		st, err := readStat(d.Paths.ProcfsRoot, pid)
		if err != nil {
			if skipProcessError(err) {
				continue
			}
			errs.add(scanErrCode(err), err.Error())
			continue
		}
		if !m.includeKernel && st.Flags&pfKThread != 0 {
			continue
		}
		name := truncate(st.Comm, 256)
		if name == "" {
			name = "pid-" + strconv.Itoa(pid)
		}
		if !nameAllowed(name, m.includeNames, m.excludeNames) {
			continue
		}

		if len(entries) >= maxEntities {
			limitHit = true
			overLimit++
			baseline = append(baseline, lifecycleProc{pid: st.PID, start: st.StartTime, name: name})
			continue
		}

		status, statusErr := readStatus(d.Paths.ProcfsRoot, pid)
		if statusErr != nil {
			if skipProcessError(statusErr) {
				continue
			}
			errs.add(scanErrCode(statusErr), statusErr.Error())
		}

		cgroup, cgroupErr := readCgroup(d.Paths.ProcfsRoot, pid)
		if cgroupErr != nil && !skipProcessError(cgroupErr) {
			errs.add(scanErrCode(cgroupErr), cgroupErr.Error())
		}

		executable, executableDeleted := readExecutable(d.Paths.ProcfsRoot, pid)

		commandLine := ""
		if m.commandLine {
			value, err := readCommandLine(d.Paths.ProcfsRoot, pid)
			if err != nil {
				if !skipProcessError(err) {
					errs.add(scanErrCode(err), err.Error())
				}
			} else if value != "" {
				commandLine = redactCommandLine(value, m.redact)
			}
		}

		attrs := map[string]any{
			"pid":  st.PID,
			"ppid": st.PPID,
			"name": name,
		}
		if state, ok := stateName(st.State); ok {
			attrs["state"] = state
		}
		if executable != "" {
			attrs["executable"] = executable
			if executableDeleted {
				attrs["executable_deleted"] = true
			}
		}
		if status.HasUID {
			attrs["user_id"] = status.UID
			if userName, ok := m.users[status.UID]; ok && userName != "" {
				attrs["user_name"] = userName
			}
		}
		if status.HasGID {
			attrs["group_id"] = status.GID
		}
		if !m.bootTime.IsZero() {
			startedAt := m.bootTime.Add(time.Duration(st.StartTime) * (time.Second / userHZ))
			attrs["started_at"] = startedAt.UTC().Truncate(time.Millisecond)
		}
		if cgroup.Path != "" {
			attrs["cgroup"] = cgroup.Path
		}
		if cgroup.ContainerID != "" {
			attrs["container_id_hint"] = cgroup.ContainerID
			attrs["container_runtime_hint"] = cgroup.RuntimeHint
		}
		if commandLine != "" {
			attrs["command_line"] = commandLine
		}

		entityID := ids.ProcessID(d.Identity.HostID, m.bootID8, st.PID, st.StartTime)
		entry := procEntry{
			stat:          st,
			entity:        events.NewEntity(entityID, ids.KindProcess, name, attrs, now),
			containerHint: cgroup.ContainerID,
		}
		entries = append(entries, entry)
		baseline = append(baseline, lifecycleProc{pid: st.PID, start: st.StartTime, name: name})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].stat.PID < entries[j].stat.PID })

	entityList := make([]events.Entity, 0, len(entries))
	byPID := make(map[int]int, len(entries))
	for i, e := range entries {
		entityList = append(entityList, e.entity)
		byPID[e.stat.PID] = i
	}

	relationships := make([]events.Relationship, 0, len(entries)*2)
	for _, e := range entries {
		target := hostID
		if e.containerHint != "" && d.Index != nil {
			if ref, ok := d.Index.Lookup(e.containerHint); ok && ref.EntityID != "" {
				target = ref.EntityID
			}
		}
		relationships = append(relationships, events.NewRelationship(
			ids.RelationshipID(e.entity.ID, "runs_on", target),
			e.entity.ID, "runs_on", target, nil, now,
		))
		if parentIdx, ok := byPID[e.stat.PPID]; ok {
			parent := entries[parentIdx]
			if parent.stat.PID != e.stat.PID {
				relationships = append(relationships, events.NewRelationship(
					ids.RelationshipID(e.entity.ID, "child_of", parent.entity.ID),
					e.entity.ID, "child_of", parent.entity.ID, nil, now,
				))
			}
		}
	}

	if limitHit {
		errs.add(events.ErrCodeLimitExceeded,
			fmt.Sprintf("max_entities_per_kind (%d) reached; %d more process(es) not reported", maxEntities, overLimit))
	}

	complete := len(errs.list) == 0

	d.Emitter.Entities(snapID, entityList)
	d.Emitter.Relationships(snapID, relationships)
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module:             "process",
		SnapshotID:         snapID,
		ScopeEntityID:      hostID,
		CoversKinds:        []string{ids.KindProcess},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entityList),
		RelationshipsCount: len(relationships),
		Errors:             errs.list,
	})

	if complete {
		m.seedLifecycleBaseline(baseline)
		m.markSuccess()
	} else {
		reason := "inventory_incomplete"
		if limitHit && len(errs.list) == 1 {
			reason = "entity_limit"
		}
		m.markDegraded(reason, errs.first)
		m.log().Debug("process inventory incomplete", "error", errs.first)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func scanErrCode(err error) string {
	switch {
	case errors.Is(err, os.ErrPermission):
		return events.ErrCodePermissionDenied
	case errors.Is(err, context.DeadlineExceeded):
		return events.ErrCodeTimeout
	case errors.Is(err, os.ErrNotExist):
		return events.ErrCodeUnavailable
	default:
		return events.ErrCodeParseError
	}
}
