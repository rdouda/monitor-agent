package containers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"agent/internal/core"
	"agent/internal/events"
	"agent/internal/ids"
	"agent/internal/modules/containers/model"

	"github.com/google/uuid"
)

const maxSnapshotErrors = 20

type scanErrors struct {
	list  []events.SnapshotError
	first error
}

func (s *scanErrors) add(code, message, kind string) {
	if len(s.list) < maxSnapshotErrors {
		s.list = append(s.list, events.SnapshotError{Code: code, Message: truncate(message, 500), Kind: kind})
	}
	if s.first == nil {
		s.first = fmt.Errorf("%s: %s", code, message)
	}
}

func snapshotID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("snapshot id: %w", err)
	}
	return id.String(), nil
}

func cloneKnown(in map[string]knownEntity) map[string]knownEntity {
	out := make(map[string]knownEntity, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneRels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// inventoryTask performs one full engine inventory and emits a snapshot.
func (m *Module) inventoryTask(ctx context.Context, st *engineState) error {
	st.invMu.Lock()
	defer st.invMu.Unlock()

	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	started := d.Clock.Now()
	now := events.Stamp(started)
	snapID, err := snapshotID()
	if err != nil {
		return err
	}

	st.mu.Lock()
	adapter := st.adapter
	engine := st.engine
	engineID := st.engineID
	key := st.key
	baseline := st.baseline
	wasAvailable := st.available
	oldKnown := cloneKnown(st.known)
	oldRels := cloneRels(st.rels)
	st.mu.Unlock()

	if adapter == nil {
		return fmt.Errorf("containers: engine %s has no adapter", engineID)
	}

	c := d.Config.Modules.Containers
	covers := []string{ids.KindContainer}
	if c.CollectNetworks {
		covers = append(covers, ids.KindContainerNetwork)
	}
	if c.CollectVolumes {
		covers = append(covers, ids.KindContainerVolume)
	}
	if engine.Runtime == "podman" {
		covers = append(covers, ids.KindPodmanPod)
	}

	var errs scanErrors
	limit := d.Config.Limits.MaxEntitiesPerKind

	containers, listErr := adapter.ListContainers(ctx)
	if listErr != nil && len(containers) == 0 && !errors.Is(listErr, model.ErrPartialInspect) {
		return m.engineScanFailed(ctx, st, snapID, started, engine, engineID, covers, listErr)
	}
	if listErr != nil {
		errs.add(events.ErrCodeAPIError, listErr.Error(), ids.KindContainer)
	}
	containerComplete := true
	if len(containers) > limit {
		containers = containers[:limit]
		containerComplete = false
		errs.add(events.ErrCodeLimitExceeded, "container entity limit reached", ids.KindContainer)
	}

	var networks []model.Network
	networkComplete := false
	if c.CollectNetworks {
		networkComplete = true
		networks, err = adapter.ListNetworks(ctx)
		if err != nil {
			networkComplete = false
			errs.add(events.ErrCodeAPIError, err.Error(), ids.KindContainerNetwork)
			networks = nil
		} else if len(networks) > limit {
			networks = networks[:limit]
			networkComplete = false
			errs.add(events.ErrCodeLimitExceeded, "network entity limit reached", ids.KindContainerNetwork)
		}
		for i := range networks {
			m.sanitizeNetworkBridge(st, &networks[i])
		}
	}

	var volumes []model.Volume
	volumeComplete := false
	if c.CollectVolumes {
		volumeComplete = true
		volumes, err = adapter.ListVolumes(ctx)
		if err != nil {
			volumeComplete = false
			errs.add(events.ErrCodeAPIError, err.Error(), ids.KindContainerVolume)
			volumes = nil
		} else if len(volumes) > limit {
			volumes = volumes[:limit]
			volumeComplete = false
			errs.add(events.ErrCodeLimitExceeded, "volume entity limit reached", ids.KindContainerVolume)
		}
	}

	var pods []model.Pod
	podComplete := false
	if engine.Runtime == "podman" {
		podComplete = true
		pods, err = adapter.ListPods(ctx)
		if err != nil && err != model.ErrNotSupported {
			podComplete = false
			errs.add(events.ErrCodeAPIError, err.Error(), ids.KindPodmanPod)
			pods = nil
		} else if len(pods) > limit {
			pods = pods[:limit]
			podComplete = false
			errs.add(events.ErrCodeLimitExceeded, "pod entity limit reached", ids.KindPodmanPod)
		}
	}

	engine.Status = "available"
	entities := []events.Entity{engineEntity(engine, engineID, now)}
	hostID := ids.HostID(d.Identity.HostID)
	rels := []events.Relationship{
		events.NewRelationship(ids.RelationshipID(engineID, "runs_on", hostID), engineID, "runs_on", hostID, nil, now),
	}
	newRels := map[string]string{rels[0].ID: engineID}
	newKnown := map[string]knownEntity{}
	containersByID := make(map[string]model.Container, len(containers))
	indexRefs := make(map[string]core.ContainerRef, len(containers))

	for _, ct := range containers {
		if ct.ID == "" {
			continue
		}
		e := m.containerEntity(d, st, ct, now)
		entities = append(entities, e)
		containersByID[ct.ID] = ct
		newKnown[e.ID] = knownEntity{kind: ids.KindContainer, name: e.Name, status: ct.State, health: ct.Health}
		indexRefs[ct.ID] = core.ContainerRef{
			ContainerID: ct.ID,
			EntityID:    e.ID,
			EngineKey:   key,
			MainPID:     ct.MainPID,
		}
		for _, r := range m.containerRelationships(d, st, ct, now) {
			rels = append(rels, r)
			if _, exists := newRels[r.ID]; !exists {
				newRels[r.ID] = r.From
			}
		}
	}

	for _, n := range networks {
		if n.ID == "" {
			continue
		}
		e := m.networkEntity(d, st, n, now)
		entities = append(entities, e)
		newKnown[e.ID] = knownEntity{kind: ids.KindContainerNetwork, name: e.Name, status: "created"}
		for _, r := range networkRelationships(d, st, n, now) {
			rels = append(rels, r)
			if _, exists := newRels[r.ID]; !exists {
				newRels[r.ID] = r.From
			}
		}
	}
	for _, v := range volumes {
		if v.Name == "" {
			continue
		}
		e := m.volumeEntity(d, st, v, now)
		entities = append(entities, e)
		newKnown[e.ID] = knownEntity{kind: ids.KindContainerVolume, name: e.Name, status: "created"}
		for _, r := range volumeRelationships(d, st, v, now) {
			rels = append(rels, r)
			if _, exists := newRels[r.ID]; !exists {
				newRels[r.ID] = r.From
			}
		}
	}
	for _, p := range pods {
		if p.ID == "" {
			continue
		}
		e := m.podEntity(d, st, p, now)
		entities = append(entities, e)
		newKnown[e.ID] = knownEntity{kind: ids.KindPodmanPod, name: e.Name, status: p.State}
		for _, r := range podRelationships(d, st, p, now) {
			rels = append(rels, r)
			if _, exists := newRels[r.ID]; !exists {
				newRels[r.ID] = r.From
			}
		}
	}

	if c.MapVethInterfaces {
		extra := m.vethRelationships(ctx, st, containers, now)
		for _, r := range extra {
			rels = append(rels, r)
			if _, exists := newRels[r.ID]; !exists {
				newRels[r.ID] = r.From
			}
		}
	}

	d.Emitter.Entities(snapID, entities)
	d.Emitter.Relationships(snapID, rels)

	completeKinds := map[string]bool{
		ids.KindContainer: containerComplete && listErr == nil,
	}
	if c.CollectNetworks {
		completeKinds[ids.KindContainerNetwork] = networkComplete
	}
	if c.CollectVolumes {
		completeKinds[ids.KindContainerVolume] = volumeComplete
	}
	if engine.Runtime == "podman" {
		completeKinds[ids.KindPodmanPod] = podComplete
	}
	complete := len(errs.list) == 0 && containerComplete && listErr == nil
	if c.CollectNetworks {
		complete = complete && networkComplete
	}
	if c.CollectVolumes {
		complete = complete && volumeComplete
	}
	if engine.Runtime == "podman" {
		complete = complete && podComplete
	}

	if baseline {
		m.reconcileLifecycle(d, st, oldKnown, newKnown, completeKinds, complete, now)
		if complete {
			m.reconcileRelationships(d, oldRels, newRels, oldKnown, newKnown, now)
		}
	}
	if !complete {
		// Incomplete scans never imply removal; keep old entries.
		for id, k := range oldKnown {
			if _, ok := newKnown[id]; !ok {
				newKnown[id] = k
			}
		}
	}

	// Image entries are managed by the separate images snapshot.
	for id, k := range oldKnown {
		if k.kind == ids.KindContainerImage {
			newKnown[id] = k
		}
	}
	mergedRels := newRels
	if !complete {
		for id, from := range oldRels {
			if _, ok := mergedRels[id]; !ok {
				mergedRels[id] = from
			}
		}
	}

	st.mu.Lock()
	st.known = newKnown
	st.rels = mergedRels
	st.containers = containersByID
	st.indexRefs = indexRefs
	st.baseline = true
	st.available = true
	st.lastErr = ""
	if st.lastEvent.IsZero() || started.After(st.lastEvent) {
		st.lastEvent = started
	}
	st.mu.Unlock()

	m.publishIndex()

	localCovers := covers
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module:             "containers",
		SnapshotID:         snapID,
		ScopeEntityID:      engineID,
		CoversKinds:        localCovers,
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entities),
		RelationshipsCount: len(rels),
		Errors:             errs.list,
	})

	if errs.first != nil {
		m.markEngineDegraded(st, "inventory_incomplete", errs.first)
	} else {
		m.markEngineHealthy(st)
	}
	if !wasAvailable && baseline {
		m.emitEngineTransition(st, true, false)
	}
	return nil
}

// engineScanFailed marks an engine unavailable and emits an incomplete
// snapshot. Entities already known are kept.
func (m *Module) engineScanFailed(ctx context.Context, st *engineState, snapID string, started time.Time, engine model.Engine, engineID string, covers []string, cause error) error {
	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	engine.Status = "unavailable"
	now := events.Stamp(d.Clock.Now())
	d.Emitter.Entities("", []events.Entity{engineEntity(engine, engineID, now)})
	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module:             "containers",
		SnapshotID:         snapID,
		ScopeEntityID:      engineID,
		CoversKinds:        covers,
		Complete:           false,
		StartedAt:          events.Stamp(started),
		CompletedAt:        now,
		EntitiesCount:      1,
		RelationshipsCount: 0,
		Errors: []events.SnapshotError{{
			Code:    snapshotErrorCode(cause),
			Message: truncate(cause.Error(), 500),
		}},
	})
	st.mu.Lock()
	was := st.available
	st.available = false
	st.lastErr = cause.Error()
	st.indexRefs = map[string]core.ContainerRef{}
	st.mu.Unlock()
	if was {
		m.emitEngineTransition(st, false, false)
	}
	m.publishIndex()
	m.refreshHealth()
	return fmt.Errorf("engine %s: %w", engineID, cause)
}

func snapshotErrorCode(err error) string {
	if err == nil {
		return events.ErrCodeInternal
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return events.ErrCodeTimeout
	case strings.Contains(msg, "permission") || strings.Contains(msg, "denied"):
		return events.ErrCodePermissionDenied
	default:
		return events.ErrCodeAPIError
	}
}

// reconcileLifecycle emits lifecycle events for state differences between two
// complete scans.
func (m *Module) reconcileLifecycle(d core.Deps, st *engineState, oldKnown, newKnown map[string]knownEntity, completeKinds map[string]bool, complete bool, now events.Timestamp) {
	var eventsOut []events.Lifecycle
	for id, old := range oldKnown {
		if !completeKinds[old.kind] {
			continue
		}
		newer, ok := newKnown[id]
		if !ok {
			if !complete {
				continue
			}
			status, valid := lifecycleStatus(old.kind, "removed")
			if !valid {
				continue
			}
			prev := old.status
			eventsOut = append(eventsOut, events.Lifecycle{
				EntityID: id, Kind: old.kind, Name: old.name,
				PreviousStatus: &prev, Status: status,
				Reason: events.ReasonEntityRemoved, ObservedAt: now,
			})
			continue
		}
		if old.status != newer.status || old.health != newer.health {
			status, valid := lifecycleStatus(newer.kind, newer.status)
			if !valid {
				continue
			}
			prev := old.status
			l := events.Lifecycle{
				EntityID: id, Kind: newer.kind, Name: newer.name,
				PreviousStatus: &prev, Status: status,
				Reason:     events.ReasonStateReconciled,
				ObservedAt: now,
			}
			if newer.kind == ids.KindContainer && status == "running" && validHealth(newer.health) && newer.health != "none" {
				l.Health = newer.health
			}
			eventsOut = append(eventsOut, l)
		}
	}
	for id, newer := range newKnown {
		if !completeKinds[newer.kind] {
			continue
		}
		if _, ok := oldKnown[id]; ok {
			continue
		}
		status, valid := lifecycleStatus(newer.kind, newer.status)
		if !valid {
			continue
		}
		reason := events.ReasonStateReconciled
		switch newer.kind {
		case ids.KindContainerNetwork, ids.KindContainerVolume, ids.KindContainerImage:
			status = events.StatusCreated
			reason = events.ReasonEntityCreated
		case ids.KindContainer:
			if status == events.StatusCreated {
				reason = events.ReasonContainerCreated
			}
		}
		l := events.Lifecycle{
			EntityID: id, Kind: newer.kind, Name: newer.name,
			Status: status, Reason: reason, ObservedAt: now,
		}
		if newer.kind == ids.KindContainer && status == "running" && validHealth(newer.health) && newer.health != "none" {
			l.Health = newer.health
		}
		eventsOut = append(eventsOut, l)
	}
	sort.Slice(eventsOut, func(i, j int) bool { return eventsOut[i].EntityID < eventsOut[j].EntityID })
	for _, l := range eventsOut {
		m.emitLifecycle(d, st, l)
	}
}

func (m *Module) reconcileRelationships(d core.Deps, oldRels, newRels map[string]string, oldKnown, newKnown map[string]knownEntity, now events.Timestamp) {
	var stale []string
	for id := range oldRels {
		if _, ok := newRels[id]; ok {
			continue
		}
		stale = append(stale, id)
	}
	if len(stale) == 0 {
		return
	}
	sort.Strings(stale)
	reason := events.RelReasonNoLongerObserved
	for _, id := range stale {
		from := oldRels[id]
		if _, ok := newKnown[from]; !ok {
			if _, wasKnown := oldKnown[from]; wasKnown {
				reason = events.RelReasonEntityRemoved
				break
			}
		}
	}
	d.Emitter.RelationshipsRemoved(stale, reason)
}

// imagesTask performs one image inventory and emits a snapshot covering only
// container_image.
func (m *Module) imagesTask(ctx context.Context, st *engineState) error {
	st.invMu.Lock()
	defer st.invMu.Unlock()

	d := m.getDeps()
	d.Emitter = m.emitterFor(st)
	started := d.Clock.Now()
	now := events.Stamp(started)
	snapID, err := snapshotID()
	if err != nil {
		return err
	}
	st.mu.Lock()
	adapter := st.adapter
	engineID := st.engineID
	baseline := st.imageBaseline
	oldKnown := cloneKnown(st.known)
	oldRels := cloneRels(st.rels)
	st.mu.Unlock()
	if adapter == nil {
		return fmt.Errorf("containers: engine %s has no adapter", engineID)
	}

	images, err := adapter.ListImages(ctx)
	if err != nil {
		d.Emitter.SnapshotCompleted(events.Snapshot{
			Module: "containers", SnapshotID: snapID, ScopeEntityID: engineID,
			CoversKinds:        []string{ids.KindContainerImage},
			Complete:           false,
			StartedAt:          events.Stamp(started),
			CompletedAt:        events.Stamp(d.Clock.Now()),
			EntitiesCount:      0,
			RelationshipsCount: 0,
			Errors:             []events.SnapshotError{{Code: snapshotErrorCode(err), Message: truncate(err.Error(), 500), Kind: ids.KindContainerImage}},
		})
		st.mu.Lock()
		st.imageBaseline = true
		st.mu.Unlock()
		m.markEngineDegraded(st, "images_failed", err)
		return err
	}
	st.mu.Lock()
	usedImages := make(map[string]bool, len(st.containers))
	for _, c := range st.containers {
		if c.ImageID != "" {
			usedImages[c.ImageID] = true
		}
	}
	st.mu.Unlock()
	if !d.Config.Modules.Containers.IncludeUntaggedImages {
		kept := images[:0]
		for _, im := range images {
			if len(im.RepoTags) == 0 && !usedImages[im.ID] {
				continue
			}
			kept = append(kept, im)
		}
		images = kept
	}

	limit := d.Config.Limits.MaxEntitiesPerKind
	var errs scanErrors
	if len(images) > limit {
		images = images[:limit]
		errs.add(events.ErrCodeLimitExceeded, "image entity limit reached", ids.KindContainerImage)
	}

	entities := make([]events.Entity, 0, len(images))
	rels := make([]events.Relationship, 0, len(images))
	newKnown := map[string]knownEntity{}
	newRels := map[string]string{}
	for _, im := range images {
		if im.ID == "" {
			continue
		}
		e := m.imageEntity(d, st, im, now)
		entities = append(entities, e)
		newKnown[e.ID] = knownEntity{kind: ids.KindContainerImage, name: e.Name, status: "created"}
		for _, r := range imageRelationships(d, st, im, now) {
			rels = append(rels, r)
			if _, ok := newRels[r.ID]; !ok {
				newRels[r.ID] = r.From
			}
		}
	}
	if len(entities) > 0 {
		d.Emitter.Entities(snapID, entities)
	}
	if len(rels) > 0 {
		d.Emitter.Relationships(snapID, rels)
	}
	complete := len(errs.list) == 0
	if baseline {
		m.reconcileLifecycle(d, st, oldKnown, newKnown, map[string]bool{ids.KindContainerImage: complete}, complete, now)
		if complete {
			// Remove only image-owned relationships.
			var stale []string
			for id, from := range oldRels {
				if !strings.HasPrefix(from, ids.PrefixImage+":") {
					continue
				}
				if _, ok := newRels[id]; !ok {
					stale = append(stale, id)
				}
			}
			if len(stale) > 0 {
				d.Emitter.RelationshipsRemoved(stale, events.RelReasonNoLongerObserved)
			}
		}
	}

	merged := cloneKnown(oldKnown)
	for id := range merged {
		if merged[id].kind == ids.KindContainerImage {
			delete(merged, id)
		}
	}
	for id, k := range newKnown {
		merged[id] = k
	}
	mergedRels := cloneRels(oldRels)
	for id, from := range mergedRels {
		if strings.HasPrefix(from, ids.PrefixImage+":") {
			delete(mergedRels, id)
		}
	}
	for id, from := range newRels {
		mergedRels[id] = from
	}

	st.mu.Lock()
	st.known = merged
	st.rels = mergedRels
	st.imageBaseline = true
	st.mu.Unlock()

	d.Emitter.SnapshotCompleted(events.Snapshot{
		Module: "containers", SnapshotID: snapID, ScopeEntityID: engineID,
		CoversKinds:        []string{ids.KindContainerImage},
		Complete:           complete,
		StartedAt:          events.Stamp(started),
		CompletedAt:        events.Stamp(d.Clock.Now()),
		EntitiesCount:      len(entities),
		RelationshipsCount: len(rels),
		Errors:             errs.list,
	})
	if errs.first != nil {
		m.markEngineDegraded(st, "images_incomplete", errs.first)
	}
	return nil
}

// lifecycleStatus maps an entity state to the lifecycle vocabulary.
func lifecycleStatus(kind, state string) (string, bool) {
	switch kind {
	case ids.KindContainer:
		switch state {
		case "created", "running", "paused", "restarting", "exited", "dead", "removed":
			return state, true
		}
		return "", false
	case ids.KindPodmanPod:
		switch state {
		case "created", "running", "paused", "exited", "removed":
			return state, true
		case "degraded":
			return "running", true
		}
		return "", false
	case ids.KindContainerEngine:
		switch state {
		case "up", "down", "removed":
			return state, true
		}
		return "", false
	case ids.KindContainerImage, ids.KindContainerNetwork, ids.KindContainerVolume:
		switch state {
		case "created", "removed":
			return state, true
		}
		return "", false
	}
	return "", false
}
