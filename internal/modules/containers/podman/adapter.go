// Package podman adapts the Podman libpod HTTP API to the runtime-neutral
// container model. It only ever issues GET requests.
package podman

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent/internal/modules/containers/model"
	"agent/internal/modules/containers/transport"
)

const inspectTTL = 5 * time.Minute

// Adapter implements model.RuntimeAdapter for Podman.
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

// New creates a Podman adapter for a resolved unix socket path.
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
func (a *Adapter) Runtime() string { return "podman" }

func (a *Adapter) path(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	a.mu.Lock()
	prefix := a.prefix
	a.mu.Unlock()
	return prefix + p
}

// libpodVersion returns the libpod API version. Podman's compat
// /version reports ApiVersion (the Docker-compatible version) separately from
// the libpod version, which is in the Podman Engine component details.
func libpodVersion(v transport.VersionResponse) string {
	for _, c := range v.Components {
		if !strings.Contains(strings.ToLower(c.Name), "podman") {
			continue
		}
		if d := c.Details["APIVersion"]; d != "" {
			return d
		}
		return c.Version
	}
	return v.Version
}

func isNotFound(err error) bool {
	var he *transport.HTTPError
	return errors.As(err, &he) && he.StatusCode == 404
}

// Probe identifies the runtime. A Docker-only daemon is rejected.
func (a *Adapter) Probe(ctx context.Context) (model.Engine, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	v, err := a.client.GetVersion(probeCtx)
	if err != nil {
		return model.Engine{}, err
	}
	if !v.Podman() {
		return model.Engine{}, model.ErrWrongRuntime
	}
	prefix := ""
	if lv := libpodVersion(v); lv != "" {
		prefix = "/v" + lv
	}
	a.mu.Lock()
	a.prefix = prefix
	a.version = v.Version
	a.mu.Unlock()

	e := model.Engine{
		Runtime:      "podman",
		Version:      v.Version,
		APIVersion:   v.APIVersion,
		Endpoint:     a.socketPath,
		EndpointType: "unix",
		Status:       "available",
	}
	var info struct {
		Host struct {
			Security struct {
				Rootless bool
			}
			CgroupVersion string
		}
		Store struct {
			GraphDriverName string
		}
	}
	infoErr := a.client.Get(probeCtx, a.path("/libpod/info"), &info)
	// Older Podman releases (and some test doubles) expose the libpod API
	// without a version prefix. Fall back when the negotiated path is absent.
	if infoErr != nil && prefix != "" && isNotFound(infoErr) {
		if err := a.client.Get(probeCtx, "/libpod/info", &info); err == nil {
			a.mu.Lock()
			a.prefix = ""
			a.mu.Unlock()
			infoErr = nil
		}
	}
	if infoErr == nil {
		e.Rootless = info.Host.Security.Rootless
		e.StorageDriver = info.Store.GraphDriverName
		cg := strings.TrimPrefix(strings.ToLower(info.Host.CgroupVersion), "v")
		if n, err := strconv.Atoi(cg); err == nil {
			e.CgroupVersion = n
		}
	}
	return e, nil
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

type containerInspect struct {
	ID      string `json:"Id"`
	Name    string
	Created model.FlexTime
	State   struct {
		Status     string
		Running    bool
		Paused     bool
		Restarting bool
		Dead       bool
		Pid        int
		ExitCode   int
		StartedAt  model.FlexTime
		FinishedAt model.FlexTime
		Health     *struct{ Status string }
	}
	Image     string
	ImageName string
	Digest    string
	Config    struct {
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
		PortBindings map[string][]struct{ HostIP, HostPort string }
	}
	NetworkSettings struct {
		Ports    map[string][]struct{ HostIP, HostPort string }
		Networks map[string]networkEndpoint
	}
	Mounts       []mountJSON
	Pod          string
	RestartCount int
}

// ListContainers returns every container, using inspect caching.
func (a *Adapter) ListContainers(ctx context.Context) ([]model.Container, error) {
	var summaries []containerSummary
	if err := a.client.Get(ctx, a.path("/libpod/containers/json?all=true"), &summaries); err != nil {
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
			out = append(out, container)
			continue
		}
		var raw containerInspect
		if err := a.client.Get(ctx, a.path("/libpod/containers/"+s.ID+"/json"), &raw); err != nil {
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
	image := raw.Config.Image
	if image == "" {
		image = raw.ImageName
	}
	c := model.Container{
		ID:            raw.ID,
		Name:          name,
		Runtime:       "podman",
		Image:         image,
		ImageID:       model.StripSHA256(raw.Image),
		State:         model.NormalizeState(raw.State.Status),
		StatusText:    raw.State.Status,
		Health:        "none",
		CreatedAt:     raw.Created.Time,
		StartedAt:     raw.State.StartedAt.Time,
		FinishedAt:    raw.State.FinishedAt.Time,
		MainPID:       raw.State.Pid,
		PodID:         raw.Pod,
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
	if strings.Contains(image, "@sha256:") {
		if _, digest, ok := strings.Cut(image, "@"); ok {
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
		portText, protocol, ok := strings.Cut(key, "/")
		if !ok {
			protocol = "tcp"
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port <= 0 || port > 65535 {
			return
		}
		hp, _ := strconv.Atoi(hostPort)
		id := fmt.Sprintf("%d/%s/%s/%d", port, protocol, hostIP, hp)
		if seen[id] {
			return
		}
		seen[id] = true
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
	if err := a.client.Get(ctx, a.path("/libpod/images/json?all=true"), &raw); err != nil {
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
	ID               string `json:"id"`
	Name             string `json:"name"`
	Driver           string `json:"driver"`
	NetworkInterface string `json:"network_interface"`
	IPv6Enabled      bool   `json:"ipv6_enabled"`
	Internal         bool   `json:"internal"`
	Subnets          []struct {
		Subnet  string `json:"subnet"`
		Gateway string `json:"gateway"`
	} `json:"subnets"`
	Labels map[string]string `json:"labels"`
}

// ListNetworks returns every network visible to the engine.
func (a *Adapter) ListNetworks(ctx context.Context) ([]model.Network, error) {
	var raw []networkJSON
	if err := a.client.Get(ctx, a.path("/libpod/networks/json"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Network, 0, len(raw))
	for _, n := range raw {
		nw := model.Network{
			ID:              n.ID,
			Name:            n.Name,
			Driver:          n.Driver,
			Scope:           "local",
			Internal:        n.Internal,
			IPv6:            n.IPv6Enabled,
			BridgeInterface: n.NetworkInterface,
			Labels:          n.Labels,
		}
		if nw.Driver == "bridge" && nw.BridgeInterface == "" && len(n.ID) >= 12 {
			nw.BridgeInterface = "br-" + n.ID[:12]
		}
		for _, s := range n.Subnets {
			nw.IPAM = append(nw.IPAM, model.IPAMConfig{Subnet: s.Subnet, Gateway: s.Gateway})
		}
		out = append(out, nw)
	}
	return out, nil
}

// ListVolumes returns every volume.
func (a *Adapter) ListVolumes(ctx context.Context) ([]model.Volume, error) {
	var raw []struct {
		Name       string
		Driver     string
		Scope      string
		Mountpoint string
		CreatedAt  model.FlexTime
		Labels     map[string]string
	}
	if err := a.client.Get(ctx, a.path("/libpod/volumes/json"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Volume, 0, len(raw))
	for _, v := range raw {
		scope := v.Scope
		if scope == "" {
			scope = "local"
		}
		out = append(out, model.Volume{
			Name:       v.Name,
			Driver:     v.Driver,
			Scope:      scope,
			Mountpoint: v.Mountpoint,
			CreatedAt:  v.CreatedAt.Time,
			Labels:     v.Labels,
		})
	}
	return out, nil
}

// ListPods returns every pod. A Podman pod is not a Kubernetes pod.
func (a *Adapter) ListPods(ctx context.Context) ([]model.Pod, error) {
	var raw []struct {
		ID         string `json:"Id"`
		Name       string
		Status     string
		Created    model.FlexTime
		InfraID    string `json:"InfraId"`
		Labels     map[string]string
		Containers []struct {
			ID    string `json:"Id"`
			Names []string
		}
	}
	if err := a.client.Get(ctx, a.path("/libpod/pods/json"), &raw); err != nil {
		return nil, err
	}
	out := make([]model.Pod, 0, len(raw))
	for _, p := range raw {
		pod := model.Pod{
			ID:        p.ID,
			Name:      p.Name,
			State:     model.NormalizePodState(p.Status),
			CreatedAt: p.Created.Time,
			InfraID:   p.InfraID,
			Labels:    p.Labels,
		}
		for _, c := range p.Containers {
			pod.ContainerIDs = append(pod.ContainerIDs, c.ID)
		}
		out = append(out, pod)
	}
	return out, nil
}

type statsJSON struct {
	ContainerID string
	CPUNano     uint64
	MemUsage    uint64
	MemLimit    uint64
	NetInput    uint64
	NetOutput   uint64
	BlockInput  uint64
	BlockOutput uint64
	Pids        uint64

	// Docker-compatible fields, used by some Podman versions.
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
			if err := a.client.Get(ctx, a.path("/libpod/containers/"+id+"/stats?stream=false&oneShot=true"), &raw); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			s := model.Stats{
				CPUUsageNs:      raw.CPUNano,
				MemoryUsage:     raw.MemUsage,
				MemoryLimit:     raw.MemLimit,
				NetworkRxBytes:  raw.NetInput,
				NetworkTxBytes:  raw.NetOutput,
				BlockReadBytes:  raw.BlockInput,
				BlockWriteBytes: raw.BlockOutput,
				Pids:            raw.Pids,
			}
			if s.CPUUsageNs == 0 {
				s.CPUUsageNs = raw.CPUStats.CPUUsage.TotalUsage
			}
			if s.MemoryLimit == 0 {
				s.MemoryLimit = raw.MemoryStats.Limit
			}
			if s.MemoryUsage == 0 {
				s.MemoryUsage = raw.MemoryStats.Usage
				inactive := raw.MemoryStats.Stats["total_inactive_file"]
				if inactive == 0 {
					inactive = raw.MemoryStats.Stats["inactive_file"]
				}
				if inactive > 0 && inactive <= s.MemoryUsage {
					s.MemoryUsage -= inactive
				}
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

type podmanEvent struct {
	Type       string
	Status     string
	Action     string
	ID         string
	Name       string
	Time       model.FlexTime
	Attributes map[string]string
	Actor      struct {
		ID         string
		Attributes map[string]string
	}
}

// Events streams libpod events. The returned channels are closed when ctx is
// canceled or the stream breaks.
func (a *Adapter) Events(ctx context.Context, since time.Time) (<-chan model.RuntimeEvent, <-chan error) {
	out := make(chan model.RuntimeEvent)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		path := fmt.Sprintf("/libpod/events?stream=true&since=%d", model.SinceUnix(since))
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
			var raw podmanEvent
			if err := json.Unmarshal([]byte(line), &raw); err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			ev := model.RuntimeEvent{
				Type:       strings.ToLower(raw.Type),
				Action:     raw.Action,
				ActorID:    raw.ID,
				ActorName:  raw.Name,
				Attributes: raw.Attributes,
				Time:       raw.Time.Time,
			}
			if ev.Action == "" {
				ev.Action = raw.Status
			}
			if ev.ActorID == "" {
				ev.ActorID = raw.Actor.ID
			}
			if ev.Attributes == nil {
				ev.Attributes = raw.Actor.Attributes
			}
			if ev.Time.IsZero() {
				ev.Time = time.Now()
			}
			if strings.HasPrefix(ev.Action, "health_status") {
				health := ev.Attributes["health_status"]
				if health == "" {
					health = strings.TrimSpace(strings.TrimPrefix(ev.Action, "health_status:"))
				}
				ev.Health = health
				ev.Action = "health_status"
			}
			if code, ok := ev.Attributes["exitCode"]; ok {
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
