// Package ids is the single place where entity and relationship IDs are
// constructed. IDs are opaque to consumers; their internal grammar is
// documented in AGENTS.md §4.2 and must not be assembled anywhere else.
package ids

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// MaxLength is the maximum length of an entity or relationship ID in bytes.
const MaxLength = 512

// Entity kind names, exactly as used on the wire.
const (
	KindHost             = "host"
	KindProcess          = "process"
	KindContainerEngine  = "container_engine"
	KindContainer        = "container"
	KindPodmanPod        = "podman_pod"
	KindContainerImage   = "container_image"
	KindContainerNetwork = "container_network"
	KindContainerVolume  = "container_volume"
	KindNetworkInterface = "network_interface"
	KindNetworkRoute     = "network_route"
	KindListeningSocket  = "listening_socket"
)

// ID prefixes, exactly as used on the wire.
const (
	PrefixHost    = "host"
	PrefixProcess = "process"
	PrefixEngine  = "engine"
	PrefixPod     = "pod"
	PrefixImage   = "image"
	PrefixNetwork = "network"
	PrefixVolume  = "volume"
	PrefixIface   = "iface"
	PrefixRoute   = "route"
	PrefixSocket  = "socket"
)

// HostID returns host:{host_id}.
func HostID(hostID string) string {
	return join(PrefixHost, hostID)
}

// ProcessID returns process:{host_id}:{boot_id8}:{pid}:{start_ticks}.
func ProcessID(hostID, bootID8 string, pid int, startTicks uint64) string {
	return join(PrefixProcess, hostID, bootID8, strconv.Itoa(pid), strconv.FormatUint(startTicks, 10))
}

// EngineID returns engine:{host_id}:{engine_key}.
func EngineID(hostID, engineKey string) string {
	return join(PrefixEngine, hostID, engineKey)
}

// ContainerID returns container:{host_id}:{engine_key}:{container_id}.
func ContainerID(hostID, engineKey, containerID string) string {
	return join("container", hostID, engineKey, containerID)
}

// PodID returns pod:{host_id}:{engine_key}:{pod_id}.
func PodID(hostID, engineKey, podID string) string {
	return join(PrefixPod, hostID, engineKey, podID)
}

// ImageID returns image:{host_id}:{engine_key}:{image_id}.
func ImageID(hostID, engineKey, imageID string) string {
	return join(PrefixImage, hostID, engineKey, imageID)
}

// NetworkID returns network:{host_id}:{engine_key}:{network_id}.
func NetworkID(hostID, engineKey, networkID string) string {
	return join(PrefixNetwork, hostID, engineKey, networkID)
}

// VolumeID returns volume:{host_id}:{engine_key}:{volume_name}.
func VolumeID(hostID, engineKey, volumeName string) string {
	return join(PrefixVolume, hostID, engineKey, volumeName)
}

// InterfaceID returns iface:{host_id}:{name}.
func InterfaceID(hostID, name string) string {
	return join(PrefixIface, hostID, name)
}

// RouteID returns
// route:{host_id}:{family}:{table}:{destination}:{gateway_or_-}:{interface}:{metric}.
func RouteID(hostID, family, table, destination, gateway, iface string, metric int) string {
	if gateway == "" {
		gateway = "-"
	}
	return join(PrefixRoute, hostID, family, table, destination, gateway, iface, strconv.Itoa(metric))
}

// SocketID returns socket:{host_id}:{scope}:{protocol}:{bind_address}:{port}.
func SocketID(hostID, scope, protocol, bindAddress string, port int) string {
	return join(PrefixSocket, hostID, scope, protocol, bindAddress, strconv.Itoa(port))
}

// RelationshipID returns rel: followed by the first 32 hex characters of
// sha256(from + "|" + type + "|" + to).
func RelationshipID(from, relationshipType, to string) string {
	sum := sha256.Sum256([]byte(from + "|" + relationshipType + "|" + to))
	return "rel:" + hex.EncodeToString(sum[:])[:32]
}

// EngineKey returns {runtime}-{sha256(realpath(socket))[:8]}. The caller must
// pass the already-resolved real socket path.
func EngineKey(runtime, realSocketPath string) string {
	sum := sha256.Sum256([]byte(realSocketPath))
	return runtime + "-" + hex.EncodeToString(sum[:])[:8]
}

// BootID8 returns the first eight hex characters of a boot ID, with dashes
// removed. It returns "" when the input does not contain at least eight hex
// characters.
func BootID8(bootID string) string {
	clean := strings.ReplaceAll(strings.ToLower(bootID), "-", "")
	if len(clean) < 8 {
		return ""
	}
	for i := 0; i < 8; i++ {
		c := clean[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return ""
		}
	}
	return clean[:8]
}

// KindForID returns the entity kind implied by an entity ID prefix. The
// second return value is false when the prefix is not a known one.
func KindForID(id string) (string, bool) {
	prefix, _, ok := strings.Cut(id, ":")
	if !ok {
		return "", false
	}
	switch prefix {
	case PrefixHost:
		return KindHost, true
	case PrefixProcess:
		return KindProcess, true
	case PrefixEngine:
		return KindContainerEngine, true
	case "container":
		return KindContainer, true
	case PrefixPod:
		return KindPodmanPod, true
	case PrefixImage:
		return KindContainerImage, true
	case PrefixNetwork:
		return KindContainerNetwork, true
	case PrefixVolume:
		return KindContainerVolume, true
	case PrefixIface:
		return KindNetworkInterface, true
	case PrefixRoute:
		return KindNetworkRoute, true
	case PrefixSocket:
		return KindListeningSocket, true
	default:
		return "", false
	}
}

// EncodeComponent percent-encodes the characters that may not appear verbatim
// in an ID component: ':', '/' and '%'. RFC 3986.
func EncodeComponent(s string) string {
	if !strings.ContainsAny(s, ":/%") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '%':
			b.WriteString("%25")
		case ':':
			b.WriteString("%3A")
		case '/':
			b.WriteString("%2F")
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func join(prefix string, components ...string) string {
	var b strings.Builder
	b.WriteString(prefix)
	for _, c := range components {
		b.WriteByte(':')
		b.WriteString(EncodeComponent(c))
	}
	return bounded(b.String())
}

// bounded enforces MaxLength deterministically. Pathological names (for
// example a very long volume name) are shortened and tagged with a hash of the
// full ID so that distinct IDs stay distinct.
func bounded(s string) string {
	if len(s) <= MaxLength {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	suffix := "-" + hex.EncodeToString(sum[:4])
	return s[:MaxLength-len(suffix)] + suffix
}
