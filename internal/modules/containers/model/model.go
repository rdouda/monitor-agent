// Package model defines the runtime-neutral container model shared by the
// Docker and Podman adapters and the containers manager.
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrNotSupported is returned by adapters for optional collections that the
// runtime does not implement (for example Docker pods).
var ErrNotSupported = errors.New("not supported by runtime")

// ErrWrongRuntime is returned by an adapter Probe when the endpoint speaks a
// different runtime protocol than the adapter implements.
var ErrWrongRuntime = errors.New("endpoint serves a different runtime")

// ErrPartialInspect marks a container list that is usable but missing one or
// more containers because their inspect call failed.
var ErrPartialInspect = errors.New("partial container inspect failure")

// Engine is one container runtime daemon discovered by the agent.
type Engine struct {
	Runtime       string
	Version       string
	APIVersion    string
	Endpoint      string
	EndpointType  string
	Rootless      bool
	OwnerUID      int
	StorageDriver string
	CgroupVersion int
	Status        string
}

// Port is one published container port.
type Port struct {
	ContainerPort int
	Protocol      string
	HostIP        string
	HostPort      int
}

// NetworkAttachment is one container network membership.
type NetworkAttachment struct {
	Name      string
	NetworkID string
	IPAddress string
	Gateway   string
	Aliases   []string
}

// Mount is one container mount.
type Mount struct {
	Type        string
	Destination string
	ReadOnly    bool
	VolumeName  string
	Source      string
}

// Container is a runtime-neutral container, built from list plus inspect
// data.
type Container struct {
	ID            string
	Name          string
	Runtime       string
	Image         string
	ImageID       string
	ImageDigest   string
	State         string
	StatusText    string
	Health        string
	CreatedAt     time.Time
	StartedAt     time.Time
	FinishedAt    time.Time
	ExitCode      int
	MainPID       int
	PodID         string
	Ports         []Port
	Networks      []NetworkAttachment
	Mounts        []Mount
	RestartPolicy string
	RestartCount  int
	Labels        map[string]string
}

// Image is one container image.
type Image struct {
	ID           string
	RepoTags     []string
	RepoDigests  []string
	CreatedAt    time.Time
	SizeBytes    int64
	Architecture string
	OS           string
	Labels       map[string]string
}

// Network is one container network.
type Network struct {
	ID              string
	Name            string
	Driver          string
	Scope           string
	Internal        bool
	IPv6            bool
	IPAM            []IPAMConfig
	BridgeInterface string
	Labels          map[string]string
}

// IPAMConfig is one IPAM subnet.
type IPAMConfig struct {
	Subnet  string
	Gateway string
}

// Volume is one container volume.
type Volume struct {
	Name       string
	Driver     string
	Scope      string
	Mountpoint string
	CreatedAt  time.Time
	Labels     map[string]string
}

// Pod is one Podman pod. It is not a Kubernetes pod.
type Pod struct {
	ID           string
	Name         string
	State        string
	CreatedAt    time.Time
	InfraID      string
	ContainerIDs []string
	Labels       map[string]string
}

// Stats is one one-shot statistics sample for a container.
type Stats struct {
	CPUUsageNs      uint64
	MemoryUsage     uint64
	MemoryLimit     uint64
	NetworkRxBytes  uint64
	NetworkTxBytes  uint64
	BlockReadBytes  uint64
	BlockWriteBytes uint64
	Pids            uint64
}

// RuntimeEvent is one event from a runtime event stream.
type RuntimeEvent struct {
	Type       string
	Action     string
	ActorID    string
	ActorName  string
	Attributes map[string]string
	Time       time.Time
	ExitCode   *int
	Health     string
}

// Event kind names as reported by the runtimes.
const (
	EventContainer = "container"
	EventNetwork   = "network"
	EventVolume    = "volume"
	EventImage     = "image"
	EventPod       = "pod"
)

// RuntimeAdapter translates one container runtime API into the neutral model.
type RuntimeAdapter interface {
	Runtime() string
	Probe(ctx context.Context) (Engine, error)
	ListContainers(ctx context.Context) ([]Container, error)
	ListImages(ctx context.Context) ([]Image, error)
	ListNetworks(ctx context.Context) ([]Network, error)
	ListVolumes(ctx context.Context) ([]Volume, error)
	ListPods(ctx context.Context) ([]Pod, error)
	Stats(ctx context.Context, ids []string) (map[string]Stats, error)
	Events(ctx context.Context, since time.Time) (<-chan RuntimeEvent, <-chan error)
}

// FlexTime accepts a JSON timestamp expressed either as RFC 3339 text or as
// Unix seconds. Docker and Podman differ between endpoints (and versions) in
// which form they use.
type FlexTime struct {
	time.Time
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *FlexTime) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		t.Time = time.Time{}
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		t.Time = ParseTimestamp(s)
		return nil
	}
	var sec int64
	if err := json.Unmarshal(b, &sec); err != nil {
		var f float64
		if err2 := json.Unmarshal(b, &f); err2 != nil {
			return fmt.Errorf("invalid timestamp %s", string(b))
		}
		sec = int64(f)
	}
	if sec == 0 {
		t.Time = time.Time{}
		return nil
	}
	t.Time = time.Unix(sec, 0).UTC()
	return nil
}

// ParseTimestamp parses the timestamp forms used by Docker and Podman.
// Zero-valued timestamps ("0001-01-01T00:00:00Z") become the zero time.
func ParseTimestamp(s string) time.Time {
	if s == "" || strings.HasPrefix(s, "0001-01-01") {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05 -0700 MST"} {
		if v, err := time.Parse(layout, s); err == nil {
			if v.Year() <= 1 {
				return time.Time{}
			}
			return v.UTC()
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
		return time.Unix(n, 0).UTC()
	}
	return time.Time{}
}

// StripSHA256 removes a leading "sha256:" from an image or layer ID.
func StripSHA256(s string) string {
	return strings.TrimPrefix(s, "sha256:")
}

// NormalizeState maps runtime-specific states onto the vocabulary in §7.2.
func NormalizeState(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "created", "configured":
		return "created"
	case "running", "up":
		return "running"
	case "paused":
		return "paused"
	case "restarting":
		return "restarting"
	case "exited", "stopped", "dead":
		if strings.EqualFold(s, "dead") {
			return "dead"
		}
		return "exited"
	case "removing":
		return "removing"
	default:
		return "created"
	}
}

// NormalizePodState maps Podman pod states onto §7.2.
func NormalizePodState(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "created", "configured":
		return "created"
	case "running", "up":
		return "running"
	case "paused":
		return "paused"
	case "exited", "stopped":
		return "exited"
	case "degraded", "error":
		return "degraded"
	default:
		return "created"
	}
}

// SinceUnix converts a time to Unix seconds for "since" query parameters.
func SinceUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
