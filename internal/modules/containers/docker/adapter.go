// Package docker adapts the Docker Engine HTTP API to the runtime-neutral
// container model. It only ever issues GET requests.
package docker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent/internal/modules/containers/model"
	"agent/internal/modules/containers/transport"
)

// inspectTTL is how long an inspect result may be reused when the container
// state has not changed (§8.3).
const inspectTTL = 5 * time.Minute

// Adapter implements model.RuntimeAdapter for Docker.
type Adapter struct {
	socketPath string
	client     *transport.Client
	stream     *transport.Client

	mu               sync.Mutex
	prefix           string
	version          string
	cache            map[string]cacheEntry
	statsConcurrency int
}

type cacheEntry struct {
	state     string
	at        time.Time
	container model.Container
}

// New creates a Docker adapter for a resolved unix socket path.
func New(socketPath string) *Adapter {
	return &Adapter{
		socketPath:       socketPath,
		client:           transport.New(socketPath, 30*time.Second),
		stream:           transport.New(socketPath, 0),
		cache:            map[string]cacheEntry{},
		statsConcurrency: 8,
	}
}

// Runtime implements model.RuntimeAdapter.
func (a *Adapter) Runtime() string { return "docker" }

func (a *Adapter) path(p string) string {
	a.mu.Lock()
	prefix := a.prefix
	a.mu.Unlock()
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return prefix + p
}

// Probe identifies the runtime and negotiates the API version.
func (a *Adapter) Probe(ctx context.Context) (model.Engine, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	v, err := a.client.GetVersion(probeCtx)
	if err != nil {
		return model.Engine{}, err
	}
	if v.Podman() {
		return model.Engine{}, model.ErrWrongRuntime
	}
	prefix := ""
	if v.APIVersion != "" {
		prefix = "/v" + v.APIVersion
	}
	a.mu.Lock()
	a.prefix = prefix
	a.version = v.Version
	a.mu.Unlock()

	e := model.Engine{
		Runtime:      "docker",
		Version:      v.Version,
		APIVersion:   v.APIVersion,
		Endpoint:     a.socketPath,
		EndpointType: "unix",
		Status:       "available",
	}
	var info struct {
		Driver          string   `json:"Driver"`
		CgroupVersion   string   `json:"CgroupVersion"`
		SecurityOptions []string `json:"SecurityOptions"`
	}
	if err := a.client.Get(probeCtx, a.path("/info"), &info); err == nil {
		e.StorageDriver = info.Driver
		e.Rootless = containsRootless(info.SecurityOptions)
		if n, err := strconv.Atoi(info.CgroupVersion); err == nil {
			e.CgroupVersion = n
		}
	}
	return e, nil
}

func containsRootless(opts []string) bool {
	for _, o := range opts {
		if strings.Contains(strings.ToLower(o), "rootless") {
			return true
		}
	}
	return false
}

type containerSummary struct {
	ID     string `json:"Id"`
	Names  []string
	State  string
	Status string
}

type networkEndpoint struct {
	NetworkID string
	IPAddress string
	Gateway   string
	Aliases   []string
}

type mountJSON struct {
	Type        string
	Destination string
	ReadOnly    bool
	Name        string
	Source      string
}

type inspectState struct {
	Status     string
	Running    bool
	Paused     bool
	Restarting bool
	OOMKilled  bool
	Dead       bool
	Pid        int
	ExitCode   int
	StartedAt  string
	FinishedAt string
	Health     *struct {
		Status string
	}
}

type containerInspect struct {
	ID      string `json:"Id"`
	Name    string
	Created string
	State   inspectState
	Image   string
	Config  struct {
		Image        string
		Labels       map[string]string
		ExposedPorts map[string]json.RawMessage
	}
	HostConfig struct {
		RestartPolicy struct {
			Name              string
			MaximumRetryCount int
		}
		NetworkMode  string
		PortBindings map[string][]struct {
			HostIP   string
			HostPort string
		}
	}
	NetworkSettings struct {
		Ports    map[string][]struct{ HostIP, HostPort string }
		Networks map[string]networkEndpoint
	}
	Mounts       []mountJSON
	RestartCount int
}

// ListContainers returns every container, using inspect caching.
func (a *Adapter) ListContainers(ctx context.Context) ([]model.Container, error) {
	var summaries []containerSummary
	if err := a.client.Get(ctx, a.path("/containers/json?all=1"), &summaries); err != nil {
		return nil, err
	}
	now := time.Now()
	seen := make(map[string]bool, len(summaries))
	out := make([]model.Container, 0, len(summaries))
	var firstErr error
	a.mu.Lock()
	cache := make(map[string]cacheEntry, len(a.cache))
	for id, entry := range a.cache {
		cache[id] = entry
	}
	a.mu.Unlock()

	for _, s := range summaries {
		if s.ID == "" {
			continue
		}
		seen[s.ID] = true
		if c, ok := cache[s.ID]; ok && c.state == s.State && now.Sub(c.at) < inspectTTL {
			container := c.container
			container.State = model.NormalizeState(s.State)
			if s.Status != "" {
				container.StatusText = s.Status
			}
			out = append(out, container)
			continue
		}
		var raw containerInspect
		if err := a.client.Get(ctx, a.path("/containers/"+s.ID+"/json"), &raw); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("inspect container %s: %w", s.ID, err)
			}
			continue
		}
		c := convertContainer(raw)
		if s.Status != "" {
			c.StatusText = s.Status
		}
		a.mu.Lock()
		a.cache[s.ID] = cacheEntry{state: s.State, at: now, container: c}
		a.mu.Unlock()
		out = append(out, c)
	}

	a.mu.Lock()
	for id := range a.cache {
		if !seen[id] {
			delete(a.cache, id)
		}
	}
	a.mu.Unlock()
	if firstErr != nil {
		return out, fmt.Errorf("%w: %v", model.ErrPartialInspect, firstErr)
	}
	return out, nil
}

func convertContainer(raw containerInspect) model.Container {
	name := strings.TrimPrefix(raw.Name, "/")
	if name == "" {
		name = raw.ID
	}
	c := model.Container{
		ID:            raw.ID,
		Name:          name,
		Runtime:       "docker",
		Image:         raw.Config.Image,
		ImageID:       model.StripSHA256(raw.Image),
		State:         model.NormalizeState(raw.State.Status),
		StatusText:    raw.State.Status,
		Health:        "none",
		CreatedAt:     model.ParseTimestamp(raw.Created),
		StartedAt:     model.ParseTimestamp(raw.State.StartedAt),
		FinishedAt:    model.ParseTimestamp(raw.State.FinishedAt),
		MainPID:       raw.State.Pid,
		RestartPolicy: raw.HostConfig.RestartPolicy.Name,
		RestartCount:  raw.RestartCount,
		Labels:        raw.Config.Labels,
		Mounts:        convertMounts(raw.Mounts),
		Networks:      convertNetworks(raw.NetworkSettings.Networks),
		Ports:         convertPorts(raw.NetworkSettings.Ports, raw.HostConfig.PortBindings, raw.Config.ExposedPorts),
	}
	if c.State != "running" {
		c.MainPID = 0
	}
	if raw.State.Health != nil && raw.State.Health.Status != "" {
		c.Health = raw.State.Health.Status
	}
	if c.State == "exited" || c.State == "dead" {
		c.ExitCode = raw.State.ExitCode
	}
	if strings.Contains(raw.Config.Image, "@sha256:") {
		if _, digest, ok := strings.Cut(raw.Config.Image, "@"); ok {
			c.ImageDigest = digest
		}
	}
	return c
}

func convertMounts(in []mountJSON) []model.Mount {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.Mount, 0, len(in))
	for _, m := range in {
		typ := strings.ToLower(m.Type)
		if typ == "" {
			if m.Name != "" {
				typ = "volume"
			} else {
				typ = "bind"
			}
		}
		out = append(out, model.Mount{
			Type:        typ,
			Destination: m.Destination,
			ReadOnly:    m.ReadOnly,
			VolumeName:  m.Name,
			Source:      m.Source,
		})
	}
	return out
}

func convertNetworks(in map[string]networkEndpoint) []model.NetworkAttachment {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.NetworkAttachment, 0, len(in))
	for name, n := range in {
		out = append(out, model.NetworkAttachment{
			Name:      name,
			NetworkID: n.NetworkID,
			IPAddress: n.IPAddress,
			Gateway:   n.Gateway,
			Aliases:   append([]string(nil), n.Aliases...),
		})
	}
	return out
}

func convertPorts(ports map[string][]struct{ HostIP, HostPort string }, bindings map[string][]struct{ HostIP, HostPort string }, exposed map[string]json.RawMessage) []model.Port {
	seen := map[string]bool{}
	keySeen := map[string]bool{}
	var out []model.Port
	add := func(key string, hostIP, hostPort string) {
		port, protocol, ok := parsePortKey(key)
		if !ok || port == 0 {
			return
		}
		hp, _ := strconv.Atoi(hostPort)
		id := fmt.Sprintf("%d/%s/%s/%d", port, protocol, hostIP, hp)
		if seen[id] {
			return
		}
		seen[id] = true
		keySeen[key] = true
		out = append(out, model.Port{ContainerPort: port, Protocol: strings.ToLower(protocol), HostIP: hostIP, HostPort: hp})
	}
	for key, list := range ports {
		if len(list) == 0 {
			add(key, "", "")
			continue
		}
		for _, b := range list {
			add(key, b.HostIP, b.HostPort)
		}
	}
	for key, list := range bindings {
		for _, b := range list {
			add(key, b.HostIP, b.HostPort)
		}
	}
	for key := range exposed {
		if keySeen[key] {
			continue
		}
		add(key, "", "")
	}
	return out
}

func parsePortKey(key string) (int, string, bool) {
	portText, protocol, ok := strings.Cut(key, "/")
	if !ok {
		protocol = "tcp"
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return 0, "", false
	}
	if protocol == "" {
		protocol = "tcp"
	}
	return port, strings.ToLower(protocol), true
}

type imageSummary struct {
	ID           string `json:"Id"`
	RepoTags     []string
	RepoDigests  []string
	Created      model.FlexTime
	Size         int64
	Architecture string
	OS           string
	Labels       map[string]string
}

// ListImages returns every image.
func (a *Adapter) ListImages(ctx context.Context) ([]model.Image, error) {
	var raw []imageSummary
	if err := a.client.Get(ctx, a.path("/images/json?all=1"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Image, 0, len(raw))
	for _, im := range raw {
		out = append(out, model.Image{
			ID:           model.StripSHA256(im.ID),
			RepoTags:     im.RepoTags,
			RepoDigests:  im.RepoDigests,
			CreatedAt:    im.Created.Time,
			SizeBytes:    im.Size,
			Architecture: im.Architecture,
			OS:           im.OS,
			Labels:       im.Labels,
		})
	}
	return out, nil
}

type networkJSON struct {
	ID         string `json:"Id"`
	Name       string
	Driver     string
	Scope      string
	Internal   bool
	EnableIPv6 bool
	IPAM       struct {
		Config []struct {
			Subnet  string
			Gateway string
		}
	}
	Options map[string]string
	Labels  map[string]string
}

// ListNetworks returns every network visible to the engine.
func (a *Adapter) ListNetworks(ctx context.Context) ([]model.Network, error) {
	var raw []networkJSON
	if err := a.client.Get(ctx, a.path("/networks"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Network, 0, len(raw))
	for _, n := range raw {
		nw := model.Network{
			ID:       n.ID,
			Name:     n.Name,
			Driver:   n.Driver,
			Scope:    n.Scope,
			Internal: n.Internal,
			IPv6:     n.EnableIPv6,
			Labels:   n.Labels,
		}
		for _, cfg := range n.IPAM.Config {
			nw.IPAM = append(nw.IPAM, model.IPAMConfig{Subnet: cfg.Subnet, Gateway: cfg.Gateway})
		}
		if nw.Scope == "" {
			nw.Scope = "local"
		}
		if n.Driver == "bridge" {
			switch {
			case n.Options["com.docker.network.bridge.name"] != "":
				nw.BridgeInterface = n.Options["com.docker.network.bridge.name"]
			case n.Name == "bridge":
				nw.BridgeInterface = "docker0"
			case len(n.ID) >= 12:
				nw.BridgeInterface = "br-" + n.ID[:12]
			}
		}
		out = append(out, nw)
	}
	return out, nil
}

// ListVolumes returns every volume.
func (a *Adapter) ListVolumes(ctx context.Context) ([]model.Volume, error) {
	var raw struct {
		Volumes []struct {
			Name       string
			Driver     string
			Scope      string
			Mountpoint string
			CreatedAt  string
			Labels     map[string]string
		}
	}
	if err := a.client.Get(ctx, a.path("/volumes"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Volume, 0, len(raw.Volumes))
	for _, v := range raw.Volumes {
		scope := v.Scope
		if scope == "" {
			scope = "local"
		}
		out = append(out, model.Volume{
			Name:       v.Name,
			Driver:     v.Driver,
			Scope:      scope,
			Mountpoint: v.Mountpoint,
			CreatedAt:  model.ParseTimestamp(v.CreatedAt),
			Labels:     v.Labels,
		})
	}
	return out, nil
}

// ListPods is not supported by Docker.
func (a *Adapter) ListPods(context.Context) ([]model.Pod, error) {
	return nil, model.ErrNotSupported
}

type statsJSON struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
	} `json:"cpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
	BlkioStats struct {
		IOServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`
	PidsStats struct {
		Current uint64 `json:"current"`
	} `json:"pids_stats"`
}

// SetStatsConcurrency configures how many stats requests may run in parallel.
func (a *Adapter) SetStatsConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	a.mu.Lock()
	a.statsConcurrency = n
	a.mu.Unlock()
}

// Stats collects one-shot statistics for the given containers.
func (a *Adapter) Stats(ctx context.Context, ids []string) (map[string]model.Stats, error) {
	out := make(map[string]model.Stats, len(ids))
	var firstErr error
	var mu sync.Mutex
	a.mu.Lock()
	concurrency := a.statsConcurrency
	a.mu.Unlock()
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			var raw statsJSON
			if err := a.client.Get(ctx, a.path("/containers/"+id+"/stats?stream=false&one-shot=true"), &raw); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			s := model.Stats{
				CPUUsageNs:     raw.CPUStats.CPUUsage.TotalUsage,
				MemoryUsage:    raw.MemoryStats.Usage,
				MemoryLimit:    raw.MemoryStats.Limit,
				Pids:           raw.PidsStats.Current,
				NetworkRxBytes: 0,
				NetworkTxBytes: 0,
			}
			// cgroup v1 and v2 use different keys for inactive file pages.
			inactive := raw.MemoryStats.Stats["total_inactive_file"]
			if inactive == 0 {
				inactive = raw.MemoryStats.Stats["inactive_file"]
			}
			if inactive > 0 && inactive <= s.MemoryUsage {
				s.MemoryUsage -= inactive
			} else {
				s.MemoryUsage = 0
			}
			for _, n := range raw.Networks {
				s.NetworkRxBytes += n.RxBytes
				s.NetworkTxBytes += n.TxBytes
			}
			for _, e := range raw.BlkioStats.IOServiceBytesRecursive {
				switch strings.ToLower(e.Op) {
				case "read":
					s.BlockReadBytes += e.Value
				case "write":
					s.BlockWriteBytes += e.Value
				}
			}
			mu.Lock()
			out[id] = s
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, firstErr
}

type dockerEvent struct {
	Type     string `json:"Type"`
	Status   string `json:"status"`
	Action   string `json:"Action"`
	ID       string `json:"id"`
	Time     int64  `json:"time"`
	TimeNano int64  `json:"timeNano"`
	Actor    struct {
		ID         string            `json:"ID"`
		Attributes map[string]string `json:"Attributes"`
	} `json:"Actor"`
}

// Events streams Docker events. The returned channels are closed when ctx is
// canceled or the stream breaks.
func (a *Adapter) Events(ctx context.Context, since time.Time) (<-chan model.RuntimeEvent, <-chan error) {
	out := make(chan model.RuntimeEvent)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		path := fmt.Sprintf("/events?since=%d", model.SinceUnix(since))
		body, err := a.stream.Open(ctx, a.path(path))
		if err != nil {
			if ctx.Err() == nil {
				errs <- err
			}
			return
		}
		defer body.Close()
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var raw dockerEvent
			if err := json.Unmarshal([]byte(line), &raw); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			ev := model.RuntimeEvent{
				Type:       strings.ToLower(raw.Type),
				Action:     raw.Action,
				ActorID:    raw.Actor.ID,
				Attributes: raw.Actor.Attributes,
				Time:       time.Unix(raw.Time, raw.TimeNano%int64(time.Second)).UTC(),
			}
			if ev.Type == "" {
				ev.Type = model.EventContainer
			}
			if ev.Action == "" {
				ev.Action = raw.Status
			}
			if ev.ActorID == "" {
				ev.ActorID = raw.ID
			}
			if raw.Time == 0 && raw.TimeNano == 0 {
				ev.Time = time.Now()
			}
			if strings.HasPrefix(ev.Action, "health_status:") {
				ev.Health = strings.TrimSpace(strings.TrimPrefix(ev.Action, "health_status:"))
				ev.Action = "health_status"
			}
			if code, ok := raw.Actor.Attributes["exitCode"]; ok {
				if n, err := strconv.Atoi(code); err == nil {
					ev.ExitCode = &n
				}
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
		if err := sc.Err(); err != nil && ctx.Err() == nil {
			errs <- err
		}
	}()
	return out, errs
}

var _ model.RuntimeAdapter = (*Adapter)(nil)
