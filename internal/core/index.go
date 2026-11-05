package core

import "sync"

// Index is the in-memory container index published by the containers module
// and consumed by the process and network modules.
type Index struct {
	mu    sync.RWMutex
	byID  map[string]ContainerRef
	byPID map[int]ContainerRef
}

// NewIndex creates an empty container index.
func NewIndex() *Index {
	return &Index{byID: map[string]ContainerRef{}, byPID: map[int]ContainerRef{}}
}

// Publish replaces the index contents.
func (i *Index) Publish(refs []ContainerRef) {
	byID := make(map[string]ContainerRef, len(refs))
	byPID := make(map[int]ContainerRef, len(refs))
	for _, r := range refs {
		byID[r.ContainerID] = r
		if r.MainPID > 0 {
			byPID[r.MainPID] = r
		}
	}
	i.mu.Lock()
	i.byID = byID
	i.byPID = byPID
	i.mu.Unlock()
}

// Lookup resolves a full container ID.
func (i *Index) Lookup(containerID string) (ContainerRef, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	r, ok := i.byID[containerID]
	return r, ok
}

// ByPID resolves the container whose main process has the given host PID.
func (i *Index) ByPID(pid int) (ContainerRef, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	r, ok := i.byPID[pid]
	return r, ok
}

// All returns a snapshot of every published container reference.
func (i *Index) All() []ContainerRef {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]ContainerRef, 0, len(i.byID))
	for _, ref := range i.byID {
		out = append(out, ref)
	}
	return out
}
