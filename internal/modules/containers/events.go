package containers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"
)

// eventsLoop keeps one runtime event stream per engine alive. It reconnects
// with the last observed event time and runs a full reconciliation after
// every break.
func (m *Module) eventsLoop(ctx context.Context, st *engineState) {
	for ctx.Err() == nil {
		st.mu.Lock()
		adapter := st.adapter
		since := st.lastEvent
		st.mu.Unlock()
		if adapter == nil {
			if !sleepCtx(ctx, eventRetryDelay) {
				return
			}
			continue
		}
		if since.IsZero() {
			since = time.Now().Add(-time.Second)
		}
		ch, errCh := adapter.Events(ctx, since)
		for ev := range ch {
			m.handleEvent(ctx, st, ev)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case err := <-errCh:
			if err != nil {
				m.log().Debug("container event stream broke", "endpoint", st.endpoint, "error", err)
			}
		default:
		}
		// Stream ended: reconcile immediately, then reconnect.
		m.runInventory(ctx, st)
		if !sleepCtx(ctx, eventRetryDelay) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (m *Module) handleEvent(ctx context.Context, st *engineState, ev model.RuntimeEvent) {
	action := strings.ToLower(strings.TrimSpace(ev.Action))
	if action == "" || ignoredAction(action) {
		return
	}
	st.mu.Lock()
	if ev.Time.After(st.lastEvent) {
		st.lastEvent = ev.Time
	}
	if !ev.Time.IsZero() {
		sig := fmt.Sprintf("%s|%s|%s|%d", ev.Type, action, ev.ActorID, ev.Time.UnixNano())
		if _, seen := st.seenEvents[sig]; seen {
			st.mu.Unlock()
			return
		}
		st.seenEvents[sig] = ev.Time
		if len(st.seenEvents) > 2048 {
			cutoff := ev.Time.Add(-10 * time.Minute)
			for key, at := range st.seenEvents {
				if at.Before(cutoff) {
					delete(st.seenEvents, key)
				}
			}
		}
	}
	st.mu.Unlock()

	switch strings.ToLower(ev.Type) {
	case model.EventContainer, "":
		m.handleContainerEvent(ctx, st, ev, action)
	case model.EventNetwork, model.EventVolume, model.EventImage, model.EventPod:
		m.handleAuxEvent(ctx, st, ev, action)
	}
}

func ignoredAction(action string) bool {
	switch {
	case strings.HasPrefix(action, "exec_"):
		return true
	}
	switch action {
	case "attach", "detach", "resize", "top", "update", "copy", "prune",
		"export", "import", "commit", "checkpoint", "restore",
		"mount", "unmount", "tag", "untag":
		return true
	}
	return false
}

func (m *Module) handleContainerEvent(ctx context.Context, st *engineState, ev model.RuntimeEvent, action string) {
	if ev.ActorID == "" {
		return
	}
	switch action {
	case "created":
		action = "create"
	case "started":
		action = "start"
	case "stopped":
		action = "stop"
	case "died":
		action = "die"
	case "removed":
		action = "destroy"
	case "paused":
		action = "pause"
	case "unpaused":
		action = "unpause"
	}
	id := ev.ActorID
	switch action {
	case "stop", "kill", "oom", "oom_kill":
		m.recordRecent(st, id, action, ev.Time)
	case "create":
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusCreated, events.ReasonContainerCreated, "", nil)
	case "start":
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusRunning, events.ReasonContainerStarted, "", nil)
	case "restart":
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusRestarting, events.ReasonContainerRestarting, "", nil)
	case "pause":
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusPaused, events.ReasonContainerPaused, "", nil)
	case "unpause":
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusRunning, events.ReasonContainerUnpaused, "", nil)
	case "die":
		reason := m.reasonForDie(st, id, ev.Time)
		details := map[string]any{}
		if ev.ExitCode != nil {
			details["exit_code"] = *ev.ExitCode
		}
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusExited, reason, "", details)
	case "health_status":
		health := ev.Health
		m.refreshContainerAndLifecycle(ctx, st, id, events.StatusRunning, events.ReasonContainerHealth, health, nil)
	case "rename":
		_, _, _ = m.refreshContainer(ctx, st, id)
	case "destroy", "remove":
		m.removeContainer(st, id, events.ReasonEntityRemoved)
	}
}

func (m *Module) refreshContainerAndLifecycle(ctx context.Context, st *engineState, id, status, reason, health string, details map[string]any) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	entityID := ids.ContainerID(d.Identity.HostID, st.key, id)
	st.mu.Lock()
	prev, hadPrev := st.known[entityID]
	st.mu.Unlock()

	c, found, err := m.refreshContainer(ctx, st, id)
	if err != nil {
		m.log().Warn("refresh container failed", "container", id, "error", err)
		return
	}
	if !found {
		if hadPrev {
			m.removeContainer(st, id, events.ReasonEntityRemoved)
		}
		return
	}
	if status == "" {
		status = c.State
	}
	if mapped, ok := lifecycleStatus(ids.KindContainer, status); ok {
		status = mapped
	}
	if health == "" && status == events.StatusRunning && c.Health != "" && c.Health != "none" {
		health = c.Health
	}
	if !validHealth(health) {
		health = ""
	}
	now := events.Stamp(d.Clock.Now())
	l := events.Lifecycle{
		EntityID:   entityID,
		Kind:       ids.KindContainer,
		Name:       truncate(c.Name, 256),
		Status:     status,
		Reason:     reason,
		Health:     health,
		Details:    details,
		ObservedAt: now,
	}
	if hadPrev && prev.status != "" {
		prevStatus := prev.status
		l.PreviousStatus = &prevStatus
	}
	if status == events.StatusExited && len(details) == 0 && (c.State == "exited" || c.State == "dead") {
		l.Details = map[string]any{"exit_code": c.ExitCode}
	}
	if !validHealth(l.Health) {
		l.Health = ""
	}
	m.emitLifecycle(d, st, l)
}

// refreshContainer re-reads one container and emits its entity and
// relationships. It returns false when the container no longer exists.
func (m *Module) refreshContainer(ctx context.Context, st *engineState, id string) (model.Container, bool, error) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	st.mu.Lock()
	adapter := st.adapter
	key := st.key
	st.mu.Unlock()
	if adapter == nil {
		return model.Container{}, false, nil
	}
	containers, err := adapter.ListContainers(ctx)
	if err != nil && len(containers) == 0 && !errors.Is(err, model.ErrPartialInspect) {
		return model.Container{}, false, err
	}
	var found *model.Container
	for i := range containers {
		if containers[i].ID == id {
			found = &containers[i]
			break
		}
	}
	now := events.Stamp(d.Clock.Now())
	entityID := ids.ContainerID(d.Identity.HostID, key, id)
	if found == nil {
		if err != nil {
			return model.Container{}, false, err
		}
		m.forgetContainer(st, id)
		return model.Container{}, false, nil
	}
	c := *found
	entity := m.containerEntity(d, st, c, now)
	rels := m.containerRelationships(d, st, c, now)
	d.Emitter.Entities("", []events.Entity{entity})
	d.Emitter.Relationships("", rels)

	st.mu.Lock()
	st.containers[c.ID] = c
	st.known[entityID] = knownEntity{kind: ids.KindContainer, name: entity.Name, status: c.State, health: c.Health}
	st.indexRefs[c.ID] = core.ContainerRef{
		ContainerID: c.ID,
		EntityID:    entityID,
		EngineKey:   key,
		MainPID:     c.MainPID,
	}
	st.mu.Unlock()
	m.publishIndex()
	return c, true, nil
}

func (m *Module) forgetContainer(st *engineState, id string) {
	st.mu.Lock()
	key := st.key
	st.mu.Unlock()
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	entityID := ids.ContainerID(d.Identity.HostID, key, id)
	st.mu.Lock()
	delete(st.known, entityID)
	delete(st.containers, id)
	delete(st.indexRefs, id)
	st.mu.Unlock()
}

func (m *Module) removeContainer(st *engineState, id, reason string) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	st.mu.Lock()
	key := st.key
	c, hasContainer := st.containers[id]
	entityID := ids.ContainerID(d.Identity.HostID, key, id)
	prev, hadPrev := st.known[entityID]
	relIDs := make([]string, 0)
	for rid, from := range st.rels {
		if from == entityID {
			relIDs = append(relIDs, rid)
			delete(st.rels, rid)
		}
	}
	delete(st.known, entityID)
	delete(st.containers, id)
	delete(st.indexRefs, id)
	st.mu.Unlock()
	if len(relIDs) > 0 {
		d.Emitter.RelationshipsRemoved(relIDs, events.RelReasonEntityRemoved)
	}
	if !hadPrev && !hasContainer {
		return
	}
	name := prev.name
	if name == "" && hasContainer {
		name = c.Name
	}
	if name == "" {
		name = truncate(id, 12)
	}
	status, _ := lifecycleStatus(ids.KindContainer, "removed")
	l := events.Lifecycle{
		EntityID:   entityID,
		Kind:       ids.KindContainer,
		Name:       truncate(name, 256),
		Status:     status,
		Reason:     reason,
		ObservedAt: events.Stamp(d.Clock.Now()),
	}
	if hadPrev && prev.status != "" {
		p := prev.status
		l.PreviousStatus = &p
	}
	m.emitLifecycle(d, st, l)
	m.publishIndex()
}

func validHealth(h string) bool {
	switch h {
	case "none", "starting", "healthy", "unhealthy":
		return true
	}
	return h == ""
}

func (m *Module) recordRecent(st *engineState, id, action string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.recent[id] == nil {
		st.recent[id] = map[string]time.Time{}
	}
	st.recent[id][action] = at
}

func (m *Module) reasonForDie(st *engineState, id string, at time.Time) string {
	if at.IsZero() {
		at = time.Now()
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	recent := st.recent[id]
	if recent == nil {
		return events.ReasonContainerDied
	}
	for _, action := range []string{"oom", "oom_kill"} {
		if t, ok := recent[action]; ok && at.Sub(t) <= 5*time.Second {
			return events.ReasonContainerOOMKilled
		}
	}
	for _, action := range []string{"stop", "kill"} {
		if t, ok := recent[action]; ok && at.Sub(t) <= 10*time.Second {
			return events.ReasonContainerStopped
		}
	}
	return events.ReasonContainerDied
}

// handleAuxEvent handles network, volume, image and pod events.
func (m *Module) handleAuxEvent(ctx context.Context, st *engineState, ev model.RuntimeEvent, action string) {
	if ev.ActorID == "" {
		return
	}
	kind := strings.ToLower(ev.Type)
	switch kind {
	case model.EventNetwork:
		if action == "connect" || action == "disconnect" {
			m.handleNetworkMemberEvent(ctx, st, ev, action)
			return
		}
		m.handleAuxLifecycle(ctx, st, ids.KindContainerNetwork, ev.ActorID, action)
	case model.EventVolume:
		m.handleAuxLifecycle(ctx, st, ids.KindContainerVolume, ev.ActorID, action)
	case model.EventImage:
		m.handleAuxLifecycle(ctx, st, ids.KindContainerImage, ev.ActorID, action)
	case model.EventPod:
		m.handleAuxLifecycle(ctx, st, ids.KindPodmanPod, ev.ActorID, action)
	}
}

func (m *Module) handleNetworkMemberEvent(ctx context.Context, st *engineState, ev model.RuntimeEvent, action string) {
	containerID := ev.Attributes["container"]
	if containerID == "" {
		containerID = ev.Attributes["container_id"]
	}
	if containerID == "" {
		return
	}
	if _, _, err := m.refreshContainer(ctx, st, containerID); err != nil {
		m.log().Debug("refresh container after network event", "container", containerID, "error", err)
		return
	}
	if action != "disconnect" {
		return
	}
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	st.mu.Lock()
	key := st.key
	st.mu.Unlock()
	from := ids.ContainerID(d.Identity.HostID, key, containerID)
	to := ids.NetworkID(d.Identity.HostID, key, ev.ActorID)
	d.Emitter.RelationshipsRemoved([]string{ids.RelationshipID(from, "uses_network", to)}, events.RelReasonNetworkDisconnected)
}

func (m *Module) handleAuxLifecycle(ctx context.Context, st *engineState, kind, id, action string) {
	removed := false
	switch action {
	case "destroy", "remove", "delete", "rm":
		removed = true
	case "create", "start", "rename", "tag", "untag", "import", "pull":
		removed = false
	default:
		return
	}
	if removed {
		m.removeAux(st, kind, id)
		return
	}
	m.refreshAuxAndLifecycle(ctx, st, kind, id)
}

func (m *Module) refreshAuxAndLifecycle(ctx context.Context, st *engineState, kind, id string) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	entityID := m.auxEntityID(d, st, kind, id)
	st.mu.Lock()
	prev, hadPrev := st.known[entityID]
	st.mu.Unlock()
	ent, rels, found, err := m.refreshAux(ctx, st, kind, id)
	if err != nil {
		m.log().Debug("refresh aux entity", "kind", kind, "id", id, "error", err)
		return
	}
	if !found {
		if hadPrev {
			m.removeAux(st, kind, id)
		}
		return
	}
	status := events.StatusCreated
	if kind == ids.KindPodmanPod {
		status = ent.known.status
	}
	if mapped, ok := lifecycleStatus(kind, status); ok {
		status = mapped
	}
	l := events.Lifecycle{
		EntityID:   ent.entity.ID,
		Kind:       kind,
		Name:       ent.entity.Name,
		Status:     status,
		Reason:     events.ReasonEntityCreated,
		ObservedAt: events.Stamp(d.Clock.Now()),
	}
	if kind == ids.KindPodmanPod {
		l.Reason = events.ReasonStateReconciled
	}
	if hadPrev && prev.status != "" {
		p := prev.status
		l.PreviousStatus = &p
	}
	m.emitLifecycle(d, st, l)
	_ = rels
}

type auxEntity struct {
	entity events.Entity
	known  knownEntity
}

func (m *Module) refreshAux(ctx context.Context, st *engineState, kind, id string) (auxEntity, []events.Relationship, bool, error) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	st.mu.Lock()
	adapter := st.adapter
	st.mu.Unlock()
	if adapter == nil {
		return auxEntity{}, nil, false, nil
	}
	now := events.Stamp(d.Clock.Now())
	var ent events.Entity
	var rels []events.Relationship
	found := false
	switch kind {
	case ids.KindContainerNetwork:
		list, err := adapter.ListNetworks(ctx)
		if err != nil {
			return auxEntity{}, nil, false, err
		}
		for _, item := range list {
			if item.ID != id && item.Name != id {
				continue
			}
			ent = m.networkEntity(d, st, item, now)
			rels = networkRelationships(d, st, item, now)
			found = true
			break
		}
	case ids.KindContainerVolume:
		list, err := adapter.ListVolumes(ctx)
		if err != nil {
			return auxEntity{}, nil, false, err
		}
		for _, item := range list {
			if item.Name != id {
				continue
			}
			ent = m.volumeEntity(d, st, item, now)
			rels = volumeRelationships(d, st, item, now)
			found = true
			break
		}
	case ids.KindContainerImage:
		list, err := adapter.ListImages(ctx)
		if err != nil {
			return auxEntity{}, nil, false, err
		}
		for _, item := range list {
			if item.ID != model.StripSHA256(id) {
				continue
			}
			ent = m.imageEntity(d, st, item, now)
			rels = imageRelationships(d, st, item, now)
			found = true
			break
		}
	case ids.KindPodmanPod:
		list, err := adapter.ListPods(ctx)
		if err != nil {
			return auxEntity{}, nil, false, err
		}
		for _, item := range list {
			if item.ID != id {
				continue
			}
			ent = m.podEntity(d, st, item, now)
			rels = podRelationships(d, st, item, now)
			found = true
			break
		}
	}
	if !found {
		return auxEntity{}, nil, false, nil
	}
	d.Emitter.Entities("", []events.Entity{ent})
	if len(rels) > 0 {
		d.Emitter.Relationships("", rels)
	}
	status := "created"
	if kind == ids.KindPodmanPod {
		status = ent.Attributes["state"].(string)
	}
	known := knownEntity{kind: kind, name: ent.Name, status: status}
	st.mu.Lock()
	st.known[ent.ID] = known
	for _, r := range rels {
		st.rels[r.ID] = r.From
	}
	st.mu.Unlock()
	return auxEntity{entity: ent, known: known}, rels, true, nil
}

func (m *Module) auxEntityID(d core.Deps, st *engineState, kind, id string) string {
	switch kind {
	case ids.KindContainerNetwork:
		return ids.NetworkID(d.Identity.HostID, st.key, id)
	case ids.KindContainerVolume:
		return ids.VolumeID(d.Identity.HostID, st.key, id)
	case ids.KindContainerImage:
		return ids.ImageID(d.Identity.HostID, st.key, model.StripSHA256(id))
	case ids.KindPodmanPod:
		return ids.PodID(d.Identity.HostID, st.key, id)
	}
	return ""
}

func (m *Module) removeAux(st *engineState, kind, id string) {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	entityID := m.auxEntityID(d, st, kind, id)
	st.mu.Lock()
	prev, hadPrev := st.known[entityID]
	relIDs := make([]string, 0)
	for rid, from := range st.rels {
		if from == entityID {
			relIDs = append(relIDs, rid)
			delete(st.rels, rid)
		}
	}
	delete(st.known, entityID)
	st.mu.Unlock()
	if len(relIDs) > 0 {
		d.Emitter.RelationshipsRemoved(relIDs, events.RelReasonEntityRemoved)
	}
	if !hadPrev {
		return
	}
	name := prev.name
	if name == "" {
		name = truncate(id, 12)
	}
	status, _ := lifecycleStatus(kind, "removed")
	l := events.Lifecycle{
		EntityID:   entityID,
		Kind:       kind,
		Name:       truncate(name, 256),
		Status:     status,
		Reason:     events.ReasonEntityRemoved,
		ObservedAt: events.Stamp(d.Clock.Now()),
	}
	if prev.status != "" {
		p := prev.status
		l.PreviousStatus = &p
	}
	m.emitLifecycle(d, st, l)
}
