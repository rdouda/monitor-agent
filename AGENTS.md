# Agent — Technical Design v1.0

**Audience:** the AI coding agent that will implement this system.
**Status:** v1 specification. Linux only. Written in Go.

---

## 0. How to use this document

1. This document is the source of truth. The JSON Schemas in **Appendix A are normative** for the wire format. If prose and a schema disagree, the schema wins.
2. Do **not** invent event types, entity kinds, relationship types, metric names, or fields. If something is ambiguous, pick the simplest reading and record it in `docs/DECISIONS.md`.
3. Follow the build order in **§14**. Each phase has a Definition of Done. Do not start a phase before the previous one is done.
4. Keywords **MUST**, **MUST NOT**, **SHOULD** are used in the RFC 2119 sense.
5. Every collector **MUST** be testable without a real host. Read from injectable roots (`paths.procfs_root`, `paths.sysfs_root`, `paths.hostfs_root`) and use fixtures in `testdata/`.
6. Go 1.22+, `CGO_ENABLED=0`, targets `linux/amd64` and `linux/arm64`.
7. Every ID is produced by one package (`internal/ids`) with golden tests. Never build IDs with ad-hoc string concatenation elsewhere.

---

## 1. Overview

The **agent** is a lightweight background service installed **as root** on a Linux server. It discovers:

- the **host** (identity, OS, CPU, memory, disks),
- running **processes**,
- **Docker and Podman** runtimes and their containers, images, networks, volumes, pods,
- **network** interfaces, routes, and listening sockets,

and reports inventory, relationships, lifecycle events, and metrics to a central **Ingestion API** as **CloudEvents JSON over HTTPS**.

It needs no code changes in monitored applications. It is **outbound-only** and **read-only**: it never modifies the host or any container.

### Non-goals for v1

Kubernetes discovery, Windows/macOS, log collection, tracing, database-specific monitoring, remote commands, packet capture, port scanning, outbound connection tracking.

---

## 2. Architecture

```
┌─────────────────────────────── AGENT CORE ───────────────────────────────┐
│ Identity · Config · Scheduler · Supervisor · Event Builder · Container    │
│ Index · Spool (disk queue) · Batcher · HTTP Client · Heartbeat · Logging  │
└───────────────┬──────────────┬───────────────────┬────────────────┬──────┘
                │              │                   │                │
           ┌────▼───┐    ┌─────▼────┐      ┌───────▼───────┐   ┌────▼─────┐
           │  HOST  │    │ PROCESS  │      │  CONTAINERS   │   │ NETWORK  │
           │        │    │          │      │ docker|podman │   │          │
           └────────┘    └──────────┘      └───────────────┘   └──────────┘
                                   │
                         Spool ──► Batcher ──► HTTPS POST ──► Ingestion API
```

### 2.1 Technology choices

| Concern | Choice |
|---|---|
| Language | Go 1.22+, static binary |
| procfs parsing | `github.com/prometheus/procfs` (or hand-written parsers; keep behind an interface) |
| Netlink | `github.com/vishvananda/netlink` |
| Docker API | `github.com/docker/docker/client` with `WithAPIVersionNegotiation()` |
| Podman API | Plain HTTP over unix socket (Docker-compat + libpod endpoints). Do **not** import Podman bindings |
| YAML | `gopkg.in/yaml.v3` with `KnownFields(true)` |
| UUID | `github.com/google/uuid` (v4, v5, v7) |
| Logging | stdlib `log/slog`, JSON format |
| Schema tests | `github.com/santhosh-tekuri/jsonschema/v6` |
| CloudEvents | Serialize manually (envelope is small and fixed). SDK not required |

### 2.2 Resource budget (targets, verify in load tests)

| Resource | Target on a host with ~300 processes and ~50 containers |
|---|---|
| CPU | < 2% of one core averaged over 5 min |
| RSS | < 100 MB |
| Disk write | < 1 MB/min steady state |
| Network | < 50 KB/s steady state (gzip) |

Set `GOMAXPROCS=min(2, NumCPU)`. Every scan has a timeout and must never block other modules.

---

## 3. Privileges and runtime environment

- The agent runs as **root** under systemd. This is an accepted design decision and gives full access to `/proc/*/fd`, other users' processes, all container runtime sockets (including rootless Podman/Docker of every user), and foreign network namespaces.
- Because root is granted, the **read-only guarantee is enforced in code** (see §10), not by OS permissions.
- Supported: Linux kernel ≥ 4.15, cgroup v1 and v2, `amd64` and `arm64`.
- **Optional containerized deployment:** run with `--pid=host`, mount host `/proc`, `/sys`, `/etc/os-release`, and `/run` read-only, and set `paths.procfs_root`, `paths.sysfs_root`, `paths.hostfs_root` accordingly. All collectors MUST use these roots, never hardcoded absolute paths.

---

## 4. Identity and IDs

### 4.1 Persistent identities

State directory: `/var/lib/agent/` (mode `0700`). All writes are atomic: write temp file, `fsync`, rename.

| File | Content |
|---|---|
| `identity.json` | `agent_id`, `host_id`, `fingerprint`, `created_at` |
| `credential` | Bearer credential issued by the server (mode `0600`) |
| `seq` | Reserved upper bound of `agentseq` (see §9.5) |
| `spool/` | Disk queue (see §9.4) |

| ID | Meaning | Rule |
|---|---|---|
| `agent_id` | This installation of the agent | Random UUIDv4, created on first run, never changes |
| `host_id` | The monitored machine | First creation: `UUIDv5(NS_HOST, machine_id)` if `/etc/machine-id` exists, else random UUIDv4 |
| `agent_run_id` | One process lifetime of the agent | Random UUIDv4 on every start. Lets the backend detect counter resets |

`NS_HOST` is a constant UUID defined in code (`internal/identity`).

**Clone detection.** `fingerprint = sha256(machine_id + "|" + dmi_product_uuid)` (use whichever parts are readable). On startup, if the stored fingerprint differs from the current one (VM cloned or disk copied), generate a **new random** `host_id` and `agent_id`, store the new fingerprint, and log a warning.

**Reinstall without state.** Because the first `host_id` is derived from `machine_id`, reinstalling the agent on the same machine yields the same `host_id`. Restarts and upgrades never change identities.

### 4.2 Entity ID grammar

IDs are **opaque strings** to consumers. Only `internal/ids` builds them. Components are joined by `:`. Any component containing `:`, `/`, or `%` (IPv6 addresses, CIDRs, names) MUST be percent-encoded (RFC 3986). Maximum length 512.

| Kind | ID prefix | Format |
|---|---|---|
| `host` | `host` | `host:{host_id}` |
| `process` | `process` | `process:{host_id}:{boot_id8}:{pid}:{start_ticks}` |
| `container_engine` | `engine` | `engine:{host_id}:{engine_key}` |
| `container` | `container` | `container:{host_id}:{engine_key}:{container_id}` |
| `podman_pod` | `pod` | `pod:{host_id}:{engine_key}:{pod_id}` |
| `container_image` | `image` | `image:{host_id}:{engine_key}:{image_id}` |
| `container_network` | `network` | `network:{host_id}:{engine_key}:{network_id}` |
| `container_volume` | `volume` | `volume:{host_id}:{engine_key}:{volume_name}` |
| `network_interface` | `iface` | `iface:{host_id}:{name}` |
| `network_route` | `route` | `route:{host_id}:{family}:{table}:{destination}:{gateway_or_-}:{interface}:{metric}` |
| `listening_socket` | `socket` | `socket:{host_id}:{scope}:{protocol}:{bind_address}:{port}` |

Definitions:

- `boot_id8`: first 8 hex characters of `/proc/sys/kernel/random/boot_id` (dashes removed).
- `start_ticks`: field 22 (`starttime`) of `/proc/[pid]/stat`, in clock ticks since boot. **Use ticks, not a computed epoch time**: the `btime`-derived value can jitter by ±1 s, ticks are exact. `boot_id8` makes the pair unique across reboots, which also protects against PID reuse.
- `engine_key`: `{runtime}-{sha256(realpath(socket))[:8]}`, e.g. `docker-3fa91c2e`. Stable across restarts; distinct for each rootless user.
- `image_id`, `network_id`, `container_id`, `pod_id`: full ID, without any `sha256:` prefix.
- `scope` (sockets): `host` for the root network namespace, or `container-{container_id[:12]}` for a container namespace.

### 4.3 Relationship ID

`rel:` + first 32 hex chars of `sha256(from + "|" + type + "|" + to)`. Deterministic, so re-reporting never creates duplicates.

### 4.4 Event ID and ordering

- `id`: UUIDv7 (time-ordered), unique per event.
- `agentseq`: strictly increasing integer per `agent_id` (gaps allowed). The server deduplicates on `(agentid, agentseq)` and on `id`.

---

## 5. Agent core

### 5.1 Components

| Component | Responsibility |
|---|---|
| Identity | §4.1 |
| Config | Load, validate, reload (§6) |
| Supervisor | Starts modules, restarts a crashed module with exponential backoff (1 s → 60 s), recovers panics |
| Scheduler | Runs periodic tasks; adds ±10% startup jitter; never runs the same task concurrently with itself; per-task timeout |
| Container Index | In-memory map `container_id → {entity_id, engine_key, main_pid}`, published by the containers module, read by process and network modules |
| Event Builder | Converts module output to envelopes; assigns `id`, `agentseq`, `time`; splits oversized events |
| Spool / Batcher / HTTP Client | §9 |
| Heartbeat | Every 30 s (§7.7) |
| Logging | JSON logs; never log secrets |

### 5.2 Lifecycle

1. Parse CLI and config. Fail fast on invalid config (exit code 2).
2. Load or create identity (§4.1).
3. Open spool and replay unsent events.
4. Enroll if no credential exists (§9.2).
5. Start modules. Each runs an **initial full scan** immediately, emits entities, relationships, and a `snapshot.completed` event.
6. Start periodic metrics, lifecycle watchers, and runtime event streams.
7. Run until `SIGTERM`/`SIGINT`: stop schedulers, flush spool (deadline 5 s), exit 0.
8. `SIGHUP`: reload config (§6.2).

### 5.3 Module interface

```go
package core

type Module interface {
    Name() string                                  // "host" | "process" | "containers" | "network"
    Init(ctx context.Context, d Deps) error        // return ErrUnavailable{Reason} if unsupported on this host
    Run(ctx context.Context) error                 // blocks until ctx is done; schedules its own tasks via Deps.Scheduler
    Health() ModuleHealth
}

type Deps struct {
    Config    ModuleConfig
    Identity  Identity
    Emitter   Emitter
    Scheduler Scheduler
    Index     ContainerIndex
    Paths     Paths            // procfs_root, sysfs_root, hostfs_root
    Clock     Clock            // injectable for tests
    Log       *slog.Logger
}

type Emitter interface {
    Entities(snapshotID string, e []Entity)
    Relationships(snapshotID string, r []Relationship)
    RelationshipsRemoved(ids []string, reason string)
    Metrics(observedAt time.Time, readings []Reading)
    Lifecycle(l Lifecycle)
    SnapshotCompleted(s Snapshot)
}

type ContainerIndex interface {
    Lookup(containerID string) (ContainerRef, bool)
    ByPID(pid int) (ContainerRef, bool)
}

type ModuleHealth struct {
    Status        string    // healthy | degraded | unavailable | failed | disabled
    Reason        string    // short machine-readable reason
    LastSuccessAt time.Time
    LastError     string
}
```

### 5.4 Failure isolation rules

- A failing or unavailable module MUST NOT affect other modules.
- `unavailable`: precondition missing (e.g. no Docker socket). Re-probe every 60 s.
- `degraded`: running but with partial errors (e.g. some containers could not be inspected).
- `failed`: panic or repeated fatal errors. Supervisor restarts with backoff.
- The **host module cannot be disabled**. Process, containers, and network can be.

### 5.5 Data-quality rules

- **Unavailable data is omitted**, never reported as `0` or `""`.
- First sample of a rate-based gauge (CPU utilization) is skipped until a second sample exists.
- Ratios are clamped to `[0, 1]`.
- A scan that hit any list-level error or hit a limit (`limits.max_entities_per_kind`) emits `snapshot.completed` with `complete: false`. Entities already read are still emitted.
- **Entity attributes describe slow-changing facts. Anything that changes every sample goes into metrics.**

---

## 6. Configuration

File: `/etc/agent/agent.yaml`. Unknown keys MUST be rejected.

### 6.1 Full example (values shown are the defaults)

```yaml
agent:
  environment: production
  tags:                         # copied to labels of the host entity
    team: infrastructure
    location: lab

paths:
  state_dir: /var/lib/agent
  procfs_root: /proc
  sysfs_root: /sys
  hostfs_root: /                # prefix for /etc/os-release, /etc/machine-id, ...

server:
  base_url: "https://ingest.example.com"
  events_endpoint: "/api/v1/agents/events"
  enroll_endpoint: "/api/v1/agents/enroll"
  enrollment_token_file: "/etc/agent/enrollment-token"
  credential_file: "/var/lib/agent/credential"
  timeout: 10s
  tls:
    ca_file: ""                 # optional custom CA bundle
    client_cert_file: ""        # optional mTLS
    client_key_file: ""
    min_version: "1.2"

modules:
  host:                         # always enabled, cannot be disabled
    inventory_interval: 10m
    metrics_interval: 5s        # CPU, memory, disk I/O
    disk_usage_interval: 30s
    exclude_fs_types: [proc, sysfs, devtmpfs, devpts, tmpfs, cgroup, cgroup2, overlay, squashfs, autofs, mqueue, hugetlbfs, debugfs, tracefs, securityfs, pstore, bpf, configfs, fusectl, nsfs, ramfs, rpc_pipefs, nfsd, binfmt_misc, selinuxfs, efivarfs]
    exclude_mount_prefixes: [/proc, /sys, /dev, /run, /var/lib/docker, /var/lib/containers, /snap]
    exclude_devices: ['^(loop|ram|fd|sr|zram)\d+']

  process:
    enabled: true
    inventory_interval: 60s
    lifecycle_interval: 5s
    metrics_interval: 10s
    metrics_mode: top_n         # top_n | all | allowlist
    top_n: 50                   # top N by CPU and top N by RSS (union)
    include_names: []           # glob; if non-empty, only these are reported
    exclude_names: []           # glob
    include_kernel_threads: false
    resolve_user_names: true
    collect_command_line: false # if true, redaction (§10) is mandatory
    redact_patterns: ['(?i)(password|passwd|secret|token|apikey|api-key|authorization)=\S+', '(?i)--(password|token|secret)\s+\S+']

  containers:
    enabled: true
    autodiscovery: true         # probe well-known socket paths (§8.3)
    runtimes: []                # explicit endpoints, e.g. [{type: docker, endpoint: "unix:///var/run/docker.sock"}]
    inventory_interval: 60s     # also the reconciliation interval
    metrics_interval: 15s
    images_interval: 5m
    stats_concurrency: 8
    collect_images: true
    include_untagged_images: false   # skip untagged images that no container uses (build layers); always kept if a container uses them
    collect_networks: true
    collect_volumes: true
    collect_events: true
    collect_mount_sources: false   # host paths of bind mounts
    map_veth_interfaces: true      # best effort
    label_allowlist: ["agent.*", "com.docker.compose.project", "com.docker.compose.service", "io.podman.compose.project"]

  network:
    enabled: true
    inventory_interval: 60s
    metrics_interval: 10s
    sockets_interval: 30s
    collect_listening_ports: true
    collect_container_namespaces: true
    collect_mac_addresses: false
    include_local_routes: false
    interface_metrics_exclude: ["veth*"]
    fd_walk_budget: 2s
    max_netns: 256

reporting:
  sink: http                    # http | stdout (stdout prints events as JSON lines, for development)
  flush_interval: 1s
  max_events_per_batch: 100
  max_batch_bytes: 262144
  max_event_bytes: 65536
  compress: gzip
  retry:
    initial_interval: 1s
    max_interval: 60s           # exponential backoff, full jitter
  spool:
    directory: "/var/lib/agent/spool"
    max_size_mb: 100
    segment_size_mb: 4
  resync_after_outage: 60s

limits:
  max_entities_per_kind: 20000
  max_lifecycle_events_per_minute: 600

logging:
  level: info
  format: json
  file: ""                      # empty = stdout (journald) in `run`; always stderr in `once`
                                # and whenever reporting.sink is stdout, so stdout carries only events
```

### 6.2 Behavior

- **Validation:** durations ≥ 1 s; `modules.host` cannot be disabled; regexes must compile; token and credential files must be owned by root with mode `0600` (otherwise refuse to start).
- **Credentials MUST NOT appear in `agent.yaml`**, only as file paths.
- **Reload (`SIGHUP`)** re-reads the file. Hot-reloadable: intervals, filters, label allowlist, log level. Requires restart: `paths`, `server`, module enable/disable. On an invalid reload, keep the old config and log an error.
- **CLI:**

| Command | Purpose |
|---|---|
| `agent run --config PATH` | Run the service |
| `agent validate-config --config PATH` | Validate and exit |
| `agent enroll --config PATH` | Perform enrollment once and exit |
| `agent once --config PATH [--sample-wait 2s] [--modules host,process]` | Run one full scan of all enabled modules, print events as JSON lines to stdout, exit (dev/test) |
| `agent version` | Print version, commit, Go version |

**`agent once` requirements** (must work as `sudo ./agent once --config examples/agent.yaml | jq .`):

- Forces `reporting.sink: stdout` regardless of the config file. No network access, no enrollment, no spool, no state-directory writes except reading identity (an ephemeral identity is used if none exists). `agentseq` is an in-memory counter starting at 1 and `agentrun` is fresh on every invocation. `once` MUST NOT read or write the `seq` file: doing so would advance or collide with the sequence of a running service that uses the same `agent_id`.
- **stdout contains only events**, one CloudEvent JSON object per line, nothing else. All logs go to **stderr** in this mode.
- Rate-based metrics (CPU utilization, process CPU, container CPU) need two samples. `once` takes a first sample, waits `--sample-wait` (default 2 s), takes a second, then emits metrics, so these metrics are present in the output.
- Emits entities, relationships, metrics, and one `snapshot.completed` per module scan. No lifecycle events (there is no baseline to compare against) and no heartbeat.
- `--modules` limits the scan to the listed modules.
- Exit code 0 on success. Exit code 1 if a module failed or any snapshot was incomplete (events are still printed).
- Output must validate against `schemas/v1`, same as in `run` mode.

---

## 7. Data model

All data is sent as **seven event types**. All use the same envelope (§7.1).

| Event type | Purpose | Priority |
|---|---|---|
| `agent.entity.lifecycle.changed.v1` | An entity changed state | P0 |
| `agent.relationship.removed.v1` | Relationships that no longer exist | P0 |
| `agent.entity.upsert.v1` | Create or update entities | P1 |
| `agent.relationship.upsert.v1` | Create or update relationships | P1 |
| `agent.snapshot.completed.v1` | A full inventory scan finished | P1 |
| `agent.metric.batch.v1` | Metric readings | P2 |
| `agent.heartbeat.v1` | Agent health | not spooled |

### 7.1 Envelope

```json
{
  "specversion": "1.0",
  "id": "0192f3a4-7c1e-7b52-9d3a-5c8e1f2a4b60",
  "source": "urn:agent:6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
  "type": "agent.entity.upsert.v1",
  "subject": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
  "time": "2026-11-04T20:00:00Z",
  "datacontenttype": "application/json",
  "agentid": "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
  "agentseq": 1001,
  "agentrun": "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
  "environment": "production",
  "data": {}
}
```

- `time`: when the event was created. `observed_at` inside `data`: when the data was observed.
- `subject`: the entity the event is mostly about. For multi-entity events use the host entity ID (or engine entity ID for container-module events).
- Timestamps: RFC 3339, UTC, millisecond precision at most.
- Each event MUST serialize to ≤ `reporting.max_event_bytes`. The Event Builder splits arrays (`entities`, `relationships`, `readings`) into several events when needed.

### 7.2 Entity kinds and attributes

`attributes` is an open object; consumers ignore unknown keys. The agent MUST emit exactly the fields below (omit when unavailable). `labels` hold only allowlisted metadata.

#### `host`

| Field | Type | Source / note |
|---|---|---|
| `hostname` | string | `os.Hostname()` (also the entity `name`) |
| `os` | string | always `linux` |
| `distribution` | string | `ID` in os-release |
| `distribution_name` | string | `PRETTY_NAME` |
| `os_version` | string | `VERSION_ID` |
| `kernel_version` | string | `uname -r` |
| `architecture` | string | `uname -m` (`x86_64`, `aarch64`) |
| `virtualization` | object | `{type: physical\|vm\|container\|unknown, technology?: kvm\|vmware\|hyperv\|xen\|...}` |
| `boot_time` | timestamp | `btime` in `/proc/stat` |
| `boot_id` | string | `/proc/sys/kernel/random/boot_id` |
| `timezone` | string | IANA name from `/etc/localtime` symlink, else zone abbreviation |
| `cpu` | object | `{model?, vendor?, physical_cores?, logical_cores, nominal_frequency_hz?}` |
| `memory_total_bytes` | int | `MemTotal` |
| `filesystems` | array | `[{device, mountpoint, fstype}]` after exclusions |
| `agent_id`, `agent_version` | string | |

Labels: all `agent.tags` from config.

#### `process`

| Field | Type | Source / note |
|---|---|---|
| `pid`, `ppid` | int | `/proc/[pid]/stat` |
| `name` | string | `comm` (also entity `name`) |
| `executable` | string | `readlink /proc/[pid]/exe`; strip ` (deleted)` and set `executable_deleted: true` |
| `user_id`, `group_id` | int | `Uid`/`Gid` real UID in `/proc/[pid]/status` |
| `user_name` | string | if `resolve_user_names` (cached) |
| `state` | string | `running`, `sleeping`, `disk_sleep`, `stopped`, `zombie`, `idle` (point in time) |
| `started_at` | timestamp | `btime + start_ticks/100` (display only, never used in IDs) |
| `cgroup` | string | unified (`0::`) path, else the `name=systemd` or `cpu` line |
| `container_id_hint` | string | parsed from cgroup (§8.2) |
| `container_runtime_hint` | string | `docker`, `podman`, `containerd`, `cri-o` |
| `command_line` | string | only when `collect_command_line: true`, after redaction |

Volatile values (RSS, threads, FDs, CPU time) are **metrics**, not attributes.

#### `container_engine`

| Field | Type | Note |
|---|---|---|
| `runtime` | string | `docker` or `podman`, **determined from the API response**, not the socket path |
| `version`, `api_version` | string | |
| `endpoint` | string | real socket path |
| `endpoint_type` | string | `unix` |
| `rootless` | bool | |
| `owner_uid` | int | socket owner |
| `storage_driver` | string | if available |
| `cgroup_version` | int | 1 or 2, if available |
| `engine_status` | string | `available` or `unavailable` (point in time) |

If a socket exists but the daemon does not respond, still emit the engine entity with `engine_status: unavailable`.

#### `container`

| Field | Type | Note |
|---|---|---|
| `container_id` | string | full ID |
| `runtime`, `engine_id` | string | `engine_id` = engine entity ID |
| `image` | string | image reference as configured |
| `image_id` | string | |
| `image_digest` | string | first repo digest, if known |
| `state` | string | `created\|running\|paused\|restarting\|exited\|dead\|removing` (point in time) |
| `status_text` | string | runtime's human text |
| `health` | string | `none\|starting\|healthy\|unhealthy` |
| `created_at`, `started_at`, `finished_at` | timestamp | omit zero values (`0001-01-01...`) |
| `exit_code` | int | only when exited |
| `main_pid` | int | host PID of the container's init process (running only) |
| `pod_id` | string | Podman only |
| `ports` | array | `[{container_port, protocol, host_ip?, host_port?}]` |
| `networks` | array | `[{name, network_id, ip_address?, gateway?, aliases?}]` |
| `mounts` | array | `[{type: bind\|volume\|tmpfs, destination, read_only, volume_name?, source?}]`; `source` only if `collect_mount_sources` |
| `restart_policy` | string | |

Container **environment variables, command, and entrypoint are never collected.** `restart_count` is a metric.

#### `container_image`

`image_id`, `repo_tags[]`, `repo_digests[]`, `created_at`, `size_bytes`, `architecture?`, `os?`, `dangling` (bool, true when the image has no tags). Entity `name`: first tag, else first 12 chars of ID. When `include_untagged_images: false`, untagged images that no container uses are skipped (they are intermediate build layers); an untagged image used by a container is always reported.

#### `container_network`

`network_id`, `driver`, `scope`, `internal` (bool), `ipv6` (bool), `ipam: [{subnet, gateway?}]`, `bridge_interface?` (host interface name; for the default Docker bridge network `docker0`, for user-defined bridges the `com.docker.network.bridge.name` option or `br-{network_id[:12]}`). **Set it only for rootful engines, and only if that interface currently exists on the host** (check via netlink at scan time; re-evaluate when the network module reports interface changes). Rootless Podman/Docker networks do not use a host bridge, so omit it. The `container_network attached_to network_interface` relationship is emitted only when `bridge_interface` is set, so it never points to a non-existent interface.

#### `container_volume`

`volume_name`, `driver`, `scope`, `mountpoint`, `created_at?`. Labels: allowlisted only.

#### `podman_pod`

`pod_id`, `state` (`created\|running\|paused\|exited\|degraded`), `created_at`, `infra_container_id?`, `container_count`. A Podman pod is **not** a Kubernetes Pod.

#### `network_interface`

| Field | Type | Note |
|---|---|---|
| `name` | string | |
| `ifindex` | int | |
| `interface_type` | string | `loopback\|ethernet\|bridge\|veth\|vlan\|bond\|vxlan\|tun\|tap\|dummy\|wireguard\|macvlan\|ipvlan\|other` |
| `state` | string | `up` if `IFF_UP` and operstate is `up` or `unknown`; else `down` |
| `mtu` | int | |
| `mac_address` | string | only if `collect_mac_addresses` |
| `addresses` | array | `[{address, prefix_length, family: ipv4\|ipv6, scope: global\|link\|host}]` |
| `speed_mbps` | int | `/sys/class/net/<if>/speed`, omit if -1 |
| `master` | string | bridge/bond name |
| `parent` | string | VLAN/macvlan parent name |
| `peer_ifindex` | int | veth peer (`IFLA_LINK`) |

#### `network_route`

`family` (`ipv4\|ipv6`), `destination` (`default` or CIDR), `gateway?`, `interface`, `metric`, `table`, `protocol` (`kernel\|static\|dhcp\|boot\|ra\|other`), `scope`, `preferred_source?`.

#### `listening_socket`

`protocol` (`tcp\|udp`), `family` (`ipv4\|ipv6`), `address`, `port`, `scope` (`host\|container`), `container_id?`, `process_name?`. Entity `name`: `{process_name or unknown}:{port}/{protocol}`.

### 7.3 Relationships

Direction is always **from → to**. A relationship is emitted by the module that owns the `from` entity.

| From | Type | To | Attributes | Notes |
|---|---|---|---|---|
| `process` | `runs_on` | `host` | | process not in a known container |
| `process` | `runs_on` | `container` | | resolved via cgroup + Container Index |
| `process` | `child_of` | `process` | | only if parent is reported |
| `container_engine` | `runs_on` | `host` | | |
| `container` | `managed_by` | `container_engine` | | |
| `container_image` | `managed_by` | `container_engine` | | |
| `container_network` | `managed_by` | `container_engine` | | |
| `container_volume` | `managed_by` | `container_engine` | | |
| `podman_pod` | `managed_by` | `container_engine` | | |
| `podman_pod` | `contains` | `container` | | |
| `container` | `uses_image` | `container_image` | | |
| `container` | `uses_network` | `container_network` | `ip_address`, `aliases` | |
| `container` | `mounts` | `container_volume` | `destination`, `read_only` | named volumes only |
| `container_network` | `attached_to` | `network_interface` | | bridge networks only |
| `container` | `attached_to` | `network_interface` | | host-side veth, best effort |
| `network_interface` | `runs_on` | `host` | | |
| `network_interface` | `attached_to` | `network_interface` | `role: master\|parent` | bridge/bond/VLAN |
| `network_route` | `runs_on` | `host` | | |
| `network_route` | `attached_to` | `network_interface` | | egress interface |
| `listening_socket` | `runs_on` | `host` or `container` | | by `scope` |
| `listening_socket` | `owned_by` | `process` | | when PID resolved |

Reserved, not emitted in v1: `exposes`, `connected_to`.

The aggregator MUST tolerate relationships that reference entities it has not received yet.

### 7.4 Metric catalog

Rules: names are lowercase dot-separated; **a metric name never changes unit or meaning**; counters are cumulative; ratios are `0..1` of **total capacity**; units: `By`, `ratio`, `s`, `Hz`, `count`.

| Entity | Metric | Kind | Unit | Attributes | Source |
|---|---|---|---|---|---|
| host | `host.uptime` | gauge | s | | `/proc/uptime` |
| host | `host.cpu.utilization` | gauge | ratio | | `/proc/stat` deltas: `1 − (Δidle+Δiowait)/Δtotal` |
| host | `host.cpu.iowait` | gauge | ratio | | same |
| host | `host.cpu.steal` | gauge | ratio | | same |
| host | `host.cpu.frequency` | gauge | Hz | | avg of `scaling_cur_freq`, if available |
| host | `host.load.1m` / `.5m` / `.15m` | gauge | count | | `/proc/loadavg` |
| host | `host.memory.used` | gauge | By | | `MemTotal − MemAvailable` |
| host | `host.memory.available` | gauge | By | | `MemAvailable` |
| host | `host.memory.cached` | gauge | By | | `Cached` |
| host | `host.memory.buffers` | gauge | By | | `Buffers` |
| host | `host.memory.utilization` | gauge | ratio | | used / total |
| host | `host.swap.total` / `host.swap.used` | gauge | By | | `SwapTotal`, `SwapTotal − SwapFree` |
| host | `host.swap.utilization` | gauge | ratio | | omit if no swap |
| host | `host.disk.total` / `.used` / `.available` | gauge | By | `mountpoint`, `device`, `fstype` | `statfs` |
| host | `host.disk.utilization` | gauge | ratio | same | `used / (used + available)` (matches `df`) |
| host | `host.disk.inodes.utilization` | gauge | ratio | same | `statfs` files |
| host | `host.disk.io.read.bytes` / `.write.bytes` | counter | By | `device` | `/proc/diskstats` sectors × 512 |
| host | `host.disk.io.read.ops` / `.write.ops` | counter | count | `device` | `/proc/diskstats` |
| host | `host.disk.io.time` | counter | s | `device` | io_ticks ms / 1000 |
| process | `process.cpu.utilization` | gauge | ratio | | Δ(utime+stime)/100 / Δwall / host logical cores |
| process | `process.cpu.time` | counter | s | | (utime+stime)/100 |
| process | `process.memory.rss` | gauge | By | | `VmRSS` |
| process | `process.memory.virtual` | gauge | By | | `VmSize` |
| process | `process.threads` | gauge | count | | `Threads` |
| process | `process.open_fds` | gauge | count | | entries in `/proc/[pid]/fd` |
| container_engine | `container_engine.containers.total` / `.running` / `.paused` / `.stopped` | gauge | count | | list result |
| container | `container.cpu.utilization` | gauge | ratio | | Δcpu_ns / (Δwall_ns × host logical cores) |
| container | `container.cpu.time` | counter | s | | `total_usage` ns / 1e9 |
| container | `container.memory.usage` | gauge | By | | working set: cgroup v1 `usage − total_inactive_file`, v2 `usage − inactive_file`, floor 0 |
| container | `container.memory.limit` | gauge | By | | omit if unlimited |
| container | `container.memory.utilization` | gauge | ratio | | only with limit |
| container | `container.network.rx.bytes` / `.tx.bytes` | counter | By | | sum over container interfaces |
| container | `container.blkio.read.bytes` / `.write.bytes` | counter | By | | `io_service_bytes_recursive` (case-insensitive op) |
| container | `container.pids` | gauge | count | | `pids_stats.current` |
| container | `container.restarts` | gauge | count | | inspect `RestartCount` |
| network_interface | `network.interface.rx.bytes` / `.tx.bytes` | counter | By | | netlink `IFLA_STATS64` |
| network_interface | `network.interface.rx.packets` / `.tx.packets` | counter | count | | same |
| network_interface | `network.interface.rx.errors` / `.tx.errors` | counter | count | | same |
| network_interface | `network.interface.rx.dropped` / `.tx.dropped` | counter | count | | same |

Counters never reset on the agent side. The backend detects resets (value decreases or `agentrun` changes).

### 7.5 Lifecycle vocabulary

Lifecycle events carry `kind` and `name` so the server can create a placeholder entity if the matching upsert has not arrived yet.

| Kind | Allowed `status` |
|---|---|
| `process` | `running`, `exited` |
| `container` | `created`, `running`, `paused`, `restarting`, `exited`, `dead`, `removed` |
| `podman_pod` | `created`, `running`, `paused`, `exited`, `removed` |
| `container_engine` | `up`, `down` |
| `network_interface` | `created`, `up`, `down`, `removed` |
| `container_image`, `container_network`, `container_volume` | `created`, `removed` |

`reason` values: `process_started`, `process_exited`, `container_created`, `container_started`, `container_stopped`, `container_died`, `container_oom_killed`, `container_paused`, `container_unpaused`, `container_restarting`, `container_health_changed`, `interface_up`, `interface_down`, `engine_available`, `engine_unavailable`, `entity_created`, `entity_removed`, `state_reconciled`.

Rules:

- **Baseline:** entities that exist when the agent starts are reported through the initial snapshot only. Lifecycle events are emitted only for transitions observed afterwards.
- `status: removed` means the **agent observed** the removal. "Stale" or "offline" is decided by the aggregator from heartbeats and snapshots.
- Lifecycle events are sent immediately (P0) and are never merged into other events.
- `container_health_changed` uses `status: running` plus the `health` field.
- Processes shorter-lived than `lifecycle_interval` may be missed. This is accepted in v1.

### 7.6 Snapshot semantics (topology reconciliation)

1. A module starts a scan and creates `snapshot_id` (UUIDv7).
2. Every entity and relationship produced in that scan carries `snapshot_id`.
3. On finishing, the module emits `agent.snapshot.completed.v1` with:
   - `scope_entity_id`: the entity the scan describes (host entity for host/process/network; the **engine entity** for each container engine),
   - `covers_kinds`: entity kinds this scan is authoritative for (e.g. images scan covers only `container_image`),
   - `complete`, counts, `errors`.
4. The aggregator may mark as removed only entities that satisfy **all** of: kind ∈ `covers_kinds`, belong to `scope_entity_id`, and were **not** seen in a snapshot with `complete: true`. Relationships follow the same rule, based on the kind of their `from` entity.
5. An incomplete snapshot (permission error, API error, timeout, limit hit) MUST NOT cause removals.
6. `entities_count` and `relationships_count` let the aggregator verify it received everything. If they do not match, treat the snapshot as incomplete.
7. Within the P1 class events are sent in order, so `snapshot.completed` always follows its entities.

### 7.7 Heartbeat

Sent every 30 s as a single `agent.heartbeat.v1`. **Never spooled**: if the API is unreachable, the heartbeat is dropped. It lets the aggregator distinguish *server down* from *agent down*. See Appendix A for fields.

---

## 8. Modules

### 8.1 Module HOST (required)

**Purpose:** one `host` entity plus its resource metrics.

| Task | Interval | Output |
|---|---|---|
| Static inventory | startup, then `inventory_interval` (10 m) | `host` entity, `snapshot.completed` (`covers_kinds: [host]`) |
| CPU, memory, swap, load, uptime | `metrics_interval` (5 s) | metrics |
| Disk usage | `disk_usage_interval` (30 s) | metrics per mount |
| Disk I/O | `metrics_interval` | metrics per device |

**Implementation notes**

- OS identity: `{hostfs_root}/etc/os-release`. Kernel/arch: `unix.Uname`.
- CPU model/vendor: `/proc/cpuinfo` (`model name`, `vendor_id`). On ARM these fields differ; omit if absent. `logical_cores`: count of `processor` entries (not `runtime.NumCPU`, which honors affinity). `physical_cores`: distinct `(physical id, core id)` pairs; omit when not available.
- CPU utilization: first line `cpu` of `/proc/stat`. `total = user+nice+system+idle+iowait+irq+softirq+steal`. Do **not** add `guest`/`guest_nice` (already in user/nice).
- Memory: `/proc/meminfo`. `used = MemTotal − MemAvailable`.
- Filesystems: parse `/proc/self/mountinfo`; apply `exclude_fs_types`, `exclude_mount_prefixes`; deduplicate bind mounts of the same `(device, root)`. Use `unix.Statfs`. **Skip any filesystem whose `statfs` reports 0 total blocks** (pseudo filesystems): it MUST NOT appear in `filesystems` and MUST NOT produce disk metrics (zero-size values would violate the "omit, never report 0" rule). `used = (blocks − bfree) × bsize`, `available = bavail × bsize`.
- Disk I/O: `/proc/diskstats`; whole devices only (the name exists under `/sys/block/`); apply `exclude_devices`; sector size is always 512 in this file.
- Virtualization detection order (no external commands):
  1. `/run/systemd/container` or `/.dockerenv` or `/run/.containerenv` → `container`
  2. DMI (`/sys/class/dmi/id/sys_vendor`, `product_name`): QEMU/KVM, VMware, Microsoft (Hyper-V), Xen, Amazon EC2, Google, DigitalOcean → `vm` with `technology`
  3. `hypervisor` flag in `/proc/cpuinfo` → `vm` (no technology)
  4. DMI present, none of the above → `physical`; otherwise `unknown`

### 8.2 Module PROCESS

**Purpose:** report processes, hierarchy, container membership, and resource usage.

| Task | Interval | Output |
|---|---|---|
| Full inventory | 60 s | `process` entities, `runs_on`/`child_of`/`owned_by`-supporting relationships, `snapshot.completed` (`covers_kinds: [process]`) |
| Lifecycle scan | 5 s | `lifecycle` events (`process_started`, `process_exited`) |
| Metrics | 10 s | metrics for the selected set |

**Implementation notes**

- Enumerate numeric directories under `{procfs_root}`. Any `ENOENT`/`ESRCH` while reading a process is normal (it exited): skip silently.
- Parse `/proc/[pid]/stat` by locating the **last `)`** (the command name may contain spaces and parentheses).
- Kernel threads: skip if `flags & 0x00200000` (PF_KTHREAD) unless `include_kernel_threads`.
- `lifecycle scan` reads only `pid` and `starttime` per process (cheap) and diffs against the previous set. It follows `include_names`/`exclude_names` and is capped by `limits.max_lifecycle_events_per_minute` (excess is counted and reported in the heartbeat).
- `process.open_fds` is expensive: read only for processes in the metrics set.
- **Metrics set:** `top_n` mode = union of top N by CPU and top N by RSS from the latest CPU sample, plus anything matching `include_names`. `all` = every process. `allowlist` = only `include_names`.
- CPU utilization needs two samples; use the previous sample's ticks and wall time. Skip the first sample.
- `USER_HZ` is 100.
- **Container detection** (first match in the cgroup file; for cgroup v1 prefer the `name=systemd` or `cpu` line):

  | Pattern | Runtime hint |
  |---|---|
  | `docker-([0-9a-f]{64})\.scope` or `/docker/([0-9a-f]{64})` | `docker` |
  | `libpod-([0-9a-f]{64})\.scope` or `/libpod-([0-9a-f]{64})` | `podman` |
  | `cri-containerd-([0-9a-f]{64})\.scope` | `containerd` |
  | `crio-([0-9a-f]{64})\.scope` | `cri-o` |

  Set `container_id_hint` and `container_runtime_hint`. Emit `process runs_on container` **only** if `Index.Lookup(id)` succeeds; otherwise emit `runs_on host` and retry next scan. containerd and CRI-O containers are hint-only in v1.
- Optional optimization (phase 6, not required): netlink proc connector for exec/exit events with exit codes. Polling remains the baseline and reconciliation source.
- Redaction (only when `collect_command_line: true`): apply every `redact_patterns` regex replacing matches with `[REDACTED]`. Never read `/proc/[pid]/environ`.

### 8.3 Module CONTAINERS (Docker and Podman)

**Purpose:** engines, containers, images, networks, volumes, pods, metrics, lifecycle events.

#### Architecture

```
containers/manager.go      ← shared logic: model → entities/relationships/metrics/lifecycle, caching, reconciliation
containers/model.go        ← runtime-neutral structs (Engine, Container, Image, Network, Volume, Pod, Stats, RuntimeEvent)
containers/docker/adapter.go
containers/podman/adapter.go
```

Adapters only translate runtime JSON into model structs. All INFRAVERSE-style mapping logic lives in the manager.

```go
type RuntimeAdapter interface {
    Runtime() string                                         // "docker" | "podman"
    Probe(ctx context.Context) (Engine, error)
    ListContainers(ctx context.Context) ([]Container, error) // includes inspect data
    ListImages(ctx context.Context) ([]Image, error)
    ListNetworks(ctx context.Context) ([]Network, error)
    ListVolumes(ctx context.Context) ([]Volume, error)
    ListPods(ctx context.Context) ([]Pod, error)             // Docker returns ErrNotSupported
    Stats(ctx context.Context, ids []string) (map[string]Stats, error)
    Events(ctx context.Context, since time.Time) (<-chan RuntimeEvent, <-chan error)
}
```

#### Runtime discovery

Candidates: configured `runtimes[]` plus, when `autodiscovery: true`:

| Candidate path |
|---|
| `/var/run/docker.sock`, `/run/docker.sock` |
| `/run/podman/podman.sock` |
| `/run/user/*/podman/podman.sock` (enumerate directories) |
| `/run/user/*/docker.sock` (rootless Docker) |
| `$XDG_RUNTIME_DIR/podman/podman.sock` |

Rules:

- Resolve symlinks (`EvalSymlinks`) and deduplicate by real path. `podman-docker` often symlinks `/var/run/docker.sock` to Podman.
- Connect with a 2 s timeout and call `GET /version`. **The runtime is identified from the response** (a component named "Podman Engine" means Podman), not from the path.
- Probing a systemd-activated Podman socket starts the service; this is accepted.
- Re-probe every 60 s. A socket that stops responding → engine `down` lifecycle event and `engine_status: unavailable`; when it returns → `up`.
- One goroutine group per engine. One engine failing must not affect the others.

#### Reference endpoints

Verify against the negotiated API version; do not hardcode a version.

| Data | Docker | Podman |
|---|---|---|
| Version / info | `GET /version`, `GET /info` | `GET /version`, `GET /libpod/info` |
| Containers | `GET /containers/json?all=1`, `GET /containers/{id}/json` | `GET /libpod/containers/json?all=true`, `GET /libpod/containers/{id}/json` |
| Stats | `GET /containers/{id}/stats?stream=false&one-shot=true` | `GET /libpod/containers/{id}/stats?stream=false&oneShot=true` |
| Images | `GET /images/json` | `GET /libpod/images/json` |
| Networks | `GET /networks` | `GET /libpod/networks/json` |
| Volumes | `GET /volumes` | `GET /libpod/volumes/json` |
| Pods | n/a | `GET /libpod/pods/json` |
| Events | `GET /events` (streaming) | `GET /libpod/events` (streaming) |

#### Tasks

| Task | Interval | Notes |
|---|---|---|
| Inventory + reconciliation | 60 s | list containers, networks, volumes, pods; emit entities, relationships, one `snapshot.completed` **per engine** |
| Images inventory | 5 m | separate snapshot with `covers_kinds: [container_image]` |
| Metrics | 15 s | `Stats` for running containers, `stats_concurrency` parallel |
| Events | continuous | see below |

#### Inspect caching

Inspect only containers that are new, whose list `State` changed, or whose last inspect is older than 5 min. Reuse cached data otherwise. Publish every container to the Container Index (`container_id`, `entity_id`, `engine_key`, `main_pid`).

#### Events and lifecycle mapping

Subscribe to container, network, volume, image (and pod for Podman) events. Ignore `exec_*`, `exec_die`, `attach`, `resize`, `top`, `update`.

| Runtime event | Lifecycle `status` | `reason` | Extra |
|---|---|---|---|
| container `create` | `created` | `container_created` | also re-inspect and upsert |
| container `start` | `running` | `container_started` | re-inspect and upsert |
| container `die` | `exited` | `container_died`, or `container_stopped` if a `stop`/`kill` for it was seen in the last 10 s, or `container_oom_killed` if an `oom` was seen in the last 5 s | `details.exit_code` |
| container `pause` / `unpause` | `paused` / `running` | `container_paused` / `container_unpaused` | |
| container `health_status` | `running` | `container_health_changed` | `health` field |
| container `rename` | none | | re-emit entity upsert |
| container `destroy` | `removed` | `entity_removed` | also send `relationship.removed` for its relationships |
| network/volume/image create | `created` | `entity_created` | upsert |
| network/volume/image destroy/delete | `removed` | `entity_removed` | |
| network `connect`/`disconnect` | none | | re-inspect container, emit `relationship.upsert` / `relationship.removed` for `uses_network` |

Resilience:

- When the event stream breaks, reconnect with `since=<last event time>`, then run a full reconciliation immediately.
- Every reconciliation compares the new inventory against the previous one. Any state difference not already reported produces a lifecycle event with `reason: state_reconciled`. Entities that vanished produce `removed` events.

#### Metric computation

- Take **one-shot** stats. Compute rates from the agent's own previous sample (store `total_usage` and wall time per container). Do not rely on `precpu_stats`.
- Skip metrics for containers that are not running.
- Network counters: sum all interfaces in the `networks` map.

#### veth mapping (best effort, `map_veth_interfaces`)

For each running container with `main_pid`, open `/proc/{pid}/ns/net`, list links inside that namespace (netlink handle at namespace), take links of type `veth`, read their `peer_ifindex`, and look up the matching host interface. Emit `container attached_to network_interface`. Skip host-network containers (same netns as init). Failure is silent (debug log).

### 8.4 Module NETWORK

**Purpose:** interfaces, routes, listening sockets, interface counters. No scanning, no packet capture, no outbound connection tracking.

| Task | Interval | Output |
|---|---|---|
| Interface inventory | 60 s | `network_interface` entities and relationships |
| Interface metrics | 10 s | metrics (skip `interface_metrics_exclude`) |
| Route inventory | 60 s | `network_route` entities |
| Listening sockets | 30 s | `listening_socket` entities, `owned_by`, `runs_on` |
| Change detection | event-driven | netlink subscription |

**Implementation notes**

- Interfaces/addresses/routes via netlink (`LinkList`, `AddrList`, `RouteListFiltered` on the main table, both families). Interface counters from `Attrs().Statistics` (`IFLA_STATS64`).
- Interface type from the netlink link kind; `ethernet` if kind is empty and `/sys/class/net/<if>/device` exists.
- Routes: unicast only; skip `fe80::/64` and `ff00::/8` unless `include_local_routes`. Destination `0.0.0.0/0` and `::/0` become `default`.
- **Change detection:** subscribe to link, address, and route updates. On any event, debounce 500 ms, rescan the affected inventory, and emit lifecycle events for interface `created`/`up`/`down`/`removed`. Keep the periodic scan as the fallback and reconciliation.
- **Listening sockets:**
  - Parse `{procfs_root}/net/tcp`, `tcp6`, `udp`, `udp6`. TCP: state `0A` (LISTEN). UDP: state `07` with remote `0.0.0.0:0`/`::`, local port ≠ 0.
  - Map socket inode → PID by walking `/proc/*/fd` and `readlink` of `socket:[inode]`. The walk is bounded by `fd_walk_budget`; on timeout emit sockets without `owned_by` and mark the scan `complete: false` with error code `fd_walk_timeout`. Cache inode → PID between scans.
  - **Container namespaces** (`collect_container_namespaces`): group running containers' `main_pid`s by `readlink /proc/{pid}/ns/net`; skip the host namespace; for each distinct namespace (max `max_netns`) read `/proc/{pid}/net/tcp*` and `udp*` from a representative PID. These sockets have `scope: container-{id12}` and `runs_on container`. Resolve their owning processes by checking only the PIDs in that namespace.
  - IPv4 and IPv6 sockets on the same port are separate entities.
- Only **listening** sockets are collected. Established or remote connections are out of scope.

---

## 9. Reporting and reliability

### 9.1 Transport

`POST {base_url}{events_endpoint}`

| Header | Value |
|---|---|
| `Authorization` | `Bearer <credential>` |
| `Content-Type` | `application/cloudevents-batch+json` (array of events); a single event may use `application/cloudevents+json` |
| `Content-Encoding` | `gzip` when body > 1 KB and `compress: gzip` |
| `User-Agent` | `agent/<version>` |

TLS ≥ 1.2, certificate verification always on, HTTP/2 allowed, honors `HTTPS_PROXY`. Optional mTLS via `server.tls`.

**Response**

```json
{
  "status": "accepted",
  "accepted_count": 98,
  "rejected_count": 2,
  "rejected": [
    {"id": "0192f3a4-...", "code": "schema_invalid", "message": "...", "retryable": false}
  ]
}
```

`status`: `accepted` | `partial` | `rejected`. The server MUST acknowledge only after the events are **durably committed to PostgreSQL**.

### 9.2 Enrollment and authentication

1. If no `credential` file exists, the agent calls `POST {enroll_endpoint}` with `Authorization: Bearer <enrollment token>` (from `enrollment_token_file`) and the body in Appendix A (`enroll-request`).
2. The server returns an `enroll-response` containing the per-agent `credential` (and optional expiry). The agent stores it at `credential_file` (`0600`). The enrollment token is not read again.
3. On HTTP `401` for event delivery: if an enrollment token file exists, re-enroll once; otherwise set agent status `unhealthy` and keep spooling.
4. On HTTP `403`: the agent is disabled server-side. Keep spooling, retry every 5 min.

### 9.3 Priorities

| Class | Event types | Eviction order when spool is full |
|---|---|---|
| P0 | lifecycle, `relationship.removed` | last |
| P1 | `entity.upsert`, `relationship.upsert`, `snapshot.completed` | middle |
| P2 | `metric.batch` | first (oldest first) |

Each class is FIFO. Sending drains P0, then P1, then P2, filling each batch in that order. Cross-class ordering is **not guaranteed**; the server must tolerate it (this is why lifecycle events carry `kind` and `name`).

### 9.4 Spool (disk queue)

- All events (except heartbeats) are written to the spool first and sent from it.
- Layout: `spool/p0/`, `spool/p1/`, `spool/p2/`, each with segment files `seg-{first_seq:020d}.jsonl` (one CloudEvent per line, exactly as sent). A segment is sealed at `segment_size_mb`.
- Group commit: append and `fsync` once per `flush_interval`.
- Per class a `cursor` file stores the last acknowledged `agentseq` (atomic write). A segment is deleted when fully acknowledged.
- On startup, scan segments and resume from cursors (unacknowledged events are resent; the server deduplicates).
- When `max_size_mb` is exceeded, delete the oldest segment of the lowest-priority class that has data. Count drops per class (`dropped_events_total`) and report them in the heartbeat.

### 9.5 Sequence numbers

`agentseq` is assigned by a single goroutine in the Event Builder. Persistence: the `seq` file stores a **reserved upper bound**. When the in-memory counter reaches the bound, write `bound + 1000` before continuing. After a crash, restart from the stored bound + 1. Gaps are allowed and expected.

### 9.6 Retry policy

| Result | Action |
|---|---|
| 2xx | Mark accepted events acked. Handle `rejected` per below |
| network error, 408, 429, 5xx | Retry the same batch with exponential backoff (1 s → 60 s, full jitter). Honor `Retry-After` |
| 413 | Halve `max_events_per_batch` for this retry; restore gradually after successes |
| 400/422 | Bisect the batch to isolate the bad event; ack and drop non-retryable rejects; log with event id and code; count in heartbeat |
| 401 / 403 | §9.2 |

Rejected events with `retryable: false` are removed from the spool. Retried batches MUST reuse the same event IDs.

### 9.7 Resync after outage

If delivery failed continuously for longer than `resync_after_outage`, or any event was dropped from the spool, then after the next successful delivery every module MUST immediately run a full inventory scan and emit a fresh snapshot. This implements "reconnect → reconcile".

### 9.8 Server behavior the agent relies on

- Deduplicate by `(agentid, agentseq)` and `id`.
- Tolerate out-of-order delivery across priority classes.
- Decide stale/offline status for entities; the agent never does.

---

## 10. Security rules (MUST)

| Area | Rule |
|---|---|
| Execution | Dedicated systemd service as root (see §11). Outbound-only: **no listening sockets** |
| Read-only | Runtime socket clients are wrapped so that any HTTP method other than `GET` and `HEAD` returns an error. A unit test enforces this. The agent never creates, stops, removes, or executes anything in a container |
| Subprocesses | Do not shell out. All data comes from files, syscalls, netlink, and runtime APIs |
| Secrets | Never read `/proc/[pid]/environ`. Never collect container env, command, or entrypoint. Command lines off by default, redacted when on. Labels pass an allowlist. Never log credentials or tokens |
| Files | Token/credential files must be root-owned `0600`, else refuse to start |
| Transport | HTTPS only, certificate verification on. `http://` base URLs are rejected unless `sink: stdout` |
| Scope | No packet capture, no port scanning, no remote command execution |
| Self-protection | Per-scan timeouts, entity count limits, bounded channels, bounded spool, panic recovery per module |

---

## 11. Packaging and installation

Files: `/usr/local/bin/agent`, `/etc/agent/agent.yaml`, `/etc/agent/enrollment-token` (`0600`), state in `/var/lib/agent/`.

`deployments/systemd/agent.service`:

```ini
[Unit]
Description=Agent
After=network-online.target docker.service podman.socket
Wants=network-online.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/agent run --config /etc/agent/agent.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=5
TimeoutStopSec=15
StateDirectory=agent
StateDirectoryMode=0700
LogsDirectory=agent
ConfigurationDirectory=agent

# Resource protection for the host
MemoryMax=300M
CPUQuota=50%
OOMScoreAdjust=500
LimitNOFILE=8192

# Hardening that is compatible with full /proc and socket access
NoNewPrivileges=yes
ProtectSystem=strict
PrivateTmp=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
LockPersonality=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK

[Install]
WantedBy=multi-user.target
```

Do **not** add `ProtectProc=`, `PrivateUsers=`, or `PrivateNetwork=`: they would hide processes or network namespaces from the agent.

---

## 12. Project structure

```
agent/
├── cmd/
│   ├── agent/main.go                 # CLI: run, validate-config, enroll, once, version
│   └── mockserver/main.go            # mock ingestion API: validates every event against schemas/v1, dedupes, can inject failures
├── internal/
│   ├── core/            manager.go supervisor.go scheduler.go lifecycle.go index.go
│   ├── identity/        identity.go
│   ├── ids/             ids.go ids_test.go         # ALL ID builders + golden tests
│   ├── config/          config.go validation.go
│   ├── modules/
│   │   ├── host/        collector.go inventory.go metrics.go virt.go
│   │   ├── process/     collector.go inventory.go metrics.go lifecycle.go cgroup.go
│   │   ├── containers/
│   │   │   ├── manager.go model.go discovery.go events.go
│   │   │   ├── docker/adapter.go
│   │   │   └── podman/adapter.go
│   │   └── network/     interfaces.go routes.go sockets.go metrics.go netns.go
│   ├── events/          envelope.go entity.go relationship.go metrics.go lifecycle.go snapshot.go heartbeat.go
│   ├── reporting/       client.go batcher.go retry.go enroll.go
│   └── queue/           spool.go segment.go cursor.go
├── schemas/v1/          common envelope entity-upsert relationship-upsert relationship-removed
│                        metric-batch lifecycle-changed snapshot-completed heartbeat
│                        ingest-response enroll-request enroll-response   (*.schema.json)
├── deployments/systemd/agent.service
├── examples/agent.yaml
├── testdata/            procfs/ sysfs/ netlink/ docker/ podman/   # recorded fixtures
├── docs/DECISIONS.md
├── go.mod
└── README.md
```

The `schemas/v1/` files are exactly the schemas in Appendix A. A test serializes every emitted event and validates it against them.

---

## 13. Testing and acceptance

### 13.1 Required tests

| Area | Tests |
|---|---|
| IDs | Golden tests for every kind; percent-encoding of IPv6/CIDR; PID reuse yields different IDs |
| Parsers | Table tests on fixtures; fuzz `/proc/[pid]/stat` (names with spaces and `)`), `mountinfo`, `/proc/net/tcp*` |
| Rates | CPU, container CPU, interface throughput: first sample skipped, counter wrap handled |
| Schemas | Every emitted event validates; mock server rejects invalid events |
| Snapshots | Incomplete scan never implies removal; counts match |
| Spool | `kill -9` during write loses nothing acked-or-queued; eviction order P2→P1→P0; cursor recovery |
| Retry | 429 with `Retry-After`; 413 halving; poison event bisect; 401 re-enroll |
| Read-only | Docker/Podman client rejects non-GET methods |
| Containers | Fake Docker/Podman socket server with recorded JSON; event stream break and resume; rootless discovery; `podman-docker` symlink dedupe |
| Isolation | Module panic and missing Docker do not stop other modules |
| Identity | Restart keeps IDs; clone detection regenerates IDs |
| Load | 5,000 processes, 500 containers: stays within resource budget |

### 13.2 Acceptance criteria

1. Install on a Linux server → it appears in the backend within one flush interval of startup (host entity + snapshot).
2. Host CPU, memory, and disk metrics are available.
3. Processes appear with resource metrics and parent/child relationships; container processes are linked to their container.
4. Docker and Podman (rootful and rootless) are detected independently, each as its own engine.
5. Containers, images, networks, volumes, and pods are discovered with relationships to engine and host.
6. Starting, stopping, killing, or removing a container produces a lifecycle event within 2 s.
7. Network interfaces, routes, and listening ports (host and container scope) appear; sockets are linked to processes.
8. All events validate against `schemas/v1`.
9. Stopping the mock server for 10 minutes and restarting it loses no events within spool capacity; a full resync runs on reconnect.
10. Restarting or upgrading the agent creates no duplicate entities; `agentseq` keeps increasing.
11. Missing Docker, or any single module failing, does not affect other modules; heartbeat reports the state.
12. The agent opens no listening sockets and issues no mutating runtime API calls.

---

## 14. Build plan

| Phase | Scope | Definition of Done |
|---|---|---|
| **0 — Foundations** | Repo, `internal/ids`, `internal/config`, `internal/events`, all schemas, schema-validation test harness, `cmd/mockserver` | Golden ID tests pass; sample events validate; mock server runs |
| **1 — Core + Host** | Identity, scheduler, supervisor, event builder, `stdout` sink, Host module, `agent once` | `agent once` prints valid host entity, metrics, snapshot |
| **2 — Reporting** | Spool, batcher, HTTP client, retry, enrollment, heartbeat, `agentseq` persistence, systemd unit | Acceptance 1, 9, 10 against mock server (including `kill -9` test) |
| **3 — Process** | Process module, container detection (hint only), lifecycle scan | Acceptance 3 (without container linking) |
| **4 — Containers** | Docker adapter, then Podman adapter, manager, events, Container Index | Acceptance 4, 5, 6 and process→container linking |
| **5 — Network** | Interfaces, routes, sockets, netlink events, container namespaces, veth mapping | Acceptance 7 |
| **6 — Hardening** | Load tests, fuzzing, resource budget, optional proc-connector, packaging scripts, README | All of §13.2 |

---

# Appendix A — JSON Schemas (normative)

All files use JSON Schema draft 2020-12 and live in `schemas/v1/`. Cross-file references are relative.

### A.1 `common.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "common.schema.json",
  "$defs": {
    "uuid": {
      "type": "string",
      "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    },
    "timestamp": { "type": "string", "format": "date-time" },
    "entityKind": {
      "enum": [
        "host", "process", "container_engine", "container", "podman_pod",
        "container_image", "container_network", "container_volume",
        "network_interface", "network_route", "listening_socket"
      ]
    },
    "entityId": {
      "type": "string",
      "minLength": 6,
      "maxLength": 512,
      "pattern": "^(host|process|engine|container|pod|image|network|volume|iface|route|socket):.+$"
    },
    "relationshipId": { "type": "string", "pattern": "^rel:[0-9a-f]{32}$" },
    "relationshipType": {
      "enum": [
        "runs_on", "child_of", "managed_by", "contains", "attached_to",
        "uses_network", "uses_image", "mounts", "owned_by", "exposes", "connected_to"
      ]
    },
    "moduleName": { "enum": ["host", "process", "containers", "network"] },
    "scalar": { "type": ["string", "number", "boolean"] },
    "scalarMap": {
      "type": "object",
      "maxProperties": 32,
      "additionalProperties": { "$ref": "#/$defs/scalar" }
    },
    "labels": {
      "type": "object",
      "maxProperties": 64,
      "additionalProperties": { "type": "string", "maxLength": 256 }
    }
  }
}
```

### A.2 `envelope.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "envelope.schema.json",
  "type": "object",
  "required": [
    "specversion", "id", "source", "type", "subject", "time",
    "datacontenttype", "agentid", "agentseq", "agentrun", "environment", "data"
  ],
  "additionalProperties": false,
  "properties": {
    "specversion": { "const": "1.0" },
    "id": { "$ref": "common.schema.json#/$defs/uuid" },
    "source": { "type": "string", "pattern": "^urn:agent:[0-9a-f-]{36}$" },
    "type": {
      "enum": [
        "agent.entity.upsert.v1",
        "agent.relationship.upsert.v1",
        "agent.relationship.removed.v1",
        "agent.metric.batch.v1",
        "agent.entity.lifecycle.changed.v1",
        "agent.heartbeat.v1",
        "agent.snapshot.completed.v1"
      ]
    },
    "subject": { "$ref": "common.schema.json#/$defs/entityId" },
    "time": { "$ref": "common.schema.json#/$defs/timestamp" },
    "datacontenttype": { "const": "application/json" },
    "agentid": { "$ref": "common.schema.json#/$defs/uuid" },
    "agentseq": { "type": "integer", "minimum": 1 },
    "agentrun": { "$ref": "common.schema.json#/$defs/uuid" },
    "environment": { "type": "string", "minLength": 1, "maxLength": 64 },
    "data": { "type": "object" }
  },
  "allOf": [
    { "if": { "properties": { "type": { "const": "agent.entity.upsert.v1" } } },
      "then": { "properties": { "data": { "$ref": "entity-upsert.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.relationship.upsert.v1" } } },
      "then": { "properties": { "data": { "$ref": "relationship-upsert.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.relationship.removed.v1" } } },
      "then": { "properties": { "data": { "$ref": "relationship-removed.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.metric.batch.v1" } } },
      "then": { "properties": { "data": { "$ref": "metric-batch.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.entity.lifecycle.changed.v1" } } },
      "then": { "properties": { "data": { "$ref": "lifecycle-changed.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.heartbeat.v1" } } },
      "then": { "properties": { "data": { "$ref": "heartbeat.schema.json" } } } },
    { "if": { "properties": { "type": { "const": "agent.snapshot.completed.v1" } } },
      "then": { "properties": { "data": { "$ref": "snapshot-completed.schema.json" } } } }
  ]
}
```

A batch request body is a JSON array whose items each validate against this schema.

### A.3 `entity-upsert.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "entity-upsert.schema.json",
  "type": "object",
  "required": ["entities"],
  "additionalProperties": false,
  "properties": {
    "snapshot_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "entities": {
      "type": "array",
      "minItems": 1,
      "maxItems": 200,
      "items": { "$ref": "#/$defs/entity" }
    }
  },
  "$defs": {
    "entity": {
      "type": "object",
      "required": ["id", "kind", "name", "attributes", "observed_at"],
      "additionalProperties": false,
      "properties": {
        "id": { "$ref": "common.schema.json#/$defs/entityId" },
        "kind": { "$ref": "common.schema.json#/$defs/entityKind" },
        "name": { "type": "string", "minLength": 1, "maxLength": 256 },
        "attributes": { "type": "object" },
        "labels": { "$ref": "common.schema.json#/$defs/labels" },
        "observed_at": { "$ref": "common.schema.json#/$defs/timestamp" }
      }
    }
  }
}
```

Code-level rule (not expressible here): the ID prefix MUST match the kind (see §4.2).

### A.4 `relationship-upsert.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "relationship-upsert.schema.json",
  "type": "object",
  "required": ["relationships"],
  "additionalProperties": false,
  "properties": {
    "snapshot_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "relationships": {
      "type": "array",
      "minItems": 1,
      "maxItems": 200,
      "items": {
        "type": "object",
        "required": ["id", "from", "type", "to", "observed_at"],
        "additionalProperties": false,
        "properties": {
          "id": { "$ref": "common.schema.json#/$defs/relationshipId" },
          "from": { "$ref": "common.schema.json#/$defs/entityId" },
          "type": { "$ref": "common.schema.json#/$defs/relationshipType" },
          "to": { "$ref": "common.schema.json#/$defs/entityId" },
          "attributes": { "type": "object" },
          "observed_at": { "$ref": "common.schema.json#/$defs/timestamp" }
        }
      }
    }
  }
}
```

### A.5 `relationship-removed.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "relationship-removed.schema.json",
  "type": "object",
  "required": ["relationship_ids", "reason", "observed_at"],
  "additionalProperties": false,
  "properties": {
    "relationship_ids": {
      "type": "array",
      "minItems": 1,
      "maxItems": 500,
      "items": { "$ref": "common.schema.json#/$defs/relationshipId" }
    },
    "reason": { "enum": ["entity_removed", "network_disconnected", "no_longer_observed"] },
    "observed_at": { "$ref": "common.schema.json#/$defs/timestamp" }
  }
}
```

### A.6 `metric-batch.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "metric-batch.schema.json",
  "type": "object",
  "required": ["observed_at", "readings"],
  "additionalProperties": false,
  "properties": {
    "observed_at": { "$ref": "common.schema.json#/$defs/timestamp" },
    "readings": {
      "type": "array",
      "minItems": 1,
      "maxItems": 500,
      "items": {
        "type": "object",
        "required": ["entity_id", "metrics"],
        "additionalProperties": false,
        "properties": {
          "entity_id": { "$ref": "common.schema.json#/$defs/entityId" },
          "metrics": {
            "type": "array",
            "minItems": 1,
            "maxItems": 64,
            "items": { "$ref": "#/$defs/metric" }
          }
        }
      }
    }
  },
  "$defs": {
    "metric": {
      "type": "object",
      "required": ["name", "kind", "value", "unit"],
      "additionalProperties": false,
      "properties": {
        "name": {
          "type": "string",
          "maxLength": 128,
          "pattern": "^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)+$"
        },
        "kind": { "enum": ["gauge", "counter"] },
        "value": { "type": "number" },
        "unit": { "enum": ["By", "ratio", "s", "Hz", "count"] },
        "temporality": { "const": "cumulative" },
        "attributes": { "$ref": "common.schema.json#/$defs/scalarMap" }
      },
      "allOf": [
        { "if": { "properties": { "kind": { "const": "counter" } } },
          "then": { "required": ["temporality"] } },
        { "if": { "properties": { "kind": { "const": "gauge" } } },
          "then": { "not": { "required": ["temporality"] } } }
      ]
    }
  }
}
```

### A.7 `lifecycle-changed.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "lifecycle-changed.schema.json",
  "type": "object",
  "required": ["entity_id", "kind", "name", "status", "reason", "observed_at"],
  "additionalProperties": false,
  "properties": {
    "entity_id": { "$ref": "common.schema.json#/$defs/entityId" },
    "kind": { "$ref": "common.schema.json#/$defs/entityKind" },
    "name": { "type": "string", "minLength": 1, "maxLength": 256 },
    "previous_status": { "type": ["string", "null"] },
    "status": {
      "enum": ["created", "running", "paused", "restarting", "exited", "dead", "up", "down", "removed"]
    },
    "health": { "enum": ["none", "starting", "healthy", "unhealthy"] },
    "reason": {
      "enum": [
        "process_started", "process_exited",
        "container_created", "container_started", "container_stopped", "container_died",
        "container_oom_killed", "container_paused", "container_unpaused",
        "container_restarting", "container_health_changed",
        "interface_up", "interface_down",
        "engine_available", "engine_unavailable",
        "entity_created", "entity_removed", "state_reconciled"
      ]
    },
    "details": { "$ref": "common.schema.json#/$defs/scalarMap" },
    "observed_at": { "$ref": "common.schema.json#/$defs/timestamp" }
  }
}
```

### A.8 `snapshot-completed.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "snapshot-completed.schema.json",
  "type": "object",
  "required": [
    "module", "snapshot_id", "scope_entity_id", "covers_kinds", "complete",
    "started_at", "completed_at", "entities_count", "relationships_count", "errors"
  ],
  "additionalProperties": false,
  "properties": {
    "module": { "$ref": "common.schema.json#/$defs/moduleName" },
    "snapshot_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "scope_entity_id": { "$ref": "common.schema.json#/$defs/entityId" },
    "covers_kinds": {
      "type": "array",
      "minItems": 1,
      "uniqueItems": true,
      "items": { "$ref": "common.schema.json#/$defs/entityKind" }
    },
    "complete": { "type": "boolean" },
    "started_at": { "$ref": "common.schema.json#/$defs/timestamp" },
    "completed_at": { "$ref": "common.schema.json#/$defs/timestamp" },
    "entities_count": { "type": "integer", "minimum": 0 },
    "relationships_count": { "type": "integer", "minimum": 0 },
    "errors": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["code", "message"],
        "additionalProperties": false,
        "properties": {
          "code": {
            "enum": [
              "permission_denied", "api_error", "timeout", "limit_exceeded",
              "fd_walk_timeout", "parse_error", "unavailable", "internal"
            ]
          },
          "message": { "type": "string", "maxLength": 512 },
          "kind": { "$ref": "common.schema.json#/$defs/entityKind" }
        }
      }
    }
  },
  "allOf": [
    { "if": { "properties": { "complete": { "const": true } } },
      "then": { "properties": { "errors": { "maxItems": 0 } } } }
  ]
}
```

### A.9 `heartbeat.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "heartbeat.schema.json",
  "type": "object",
  "required": ["agent", "host_id", "modules", "queue", "reporting"],
  "additionalProperties": false,
  "properties": {
    "agent": {
      "type": "object",
      "required": ["id", "run_id", "version", "status", "started_at", "uptime_seconds"],
      "additionalProperties": false,
      "properties": {
        "id": { "$ref": "common.schema.json#/$defs/uuid" },
        "run_id": { "$ref": "common.schema.json#/$defs/uuid" },
        "version": { "type": "string" },
        "commit": { "type": "string" },
        "status": { "enum": ["healthy", "degraded", "unhealthy"] },
        "started_at": { "$ref": "common.schema.json#/$defs/timestamp" },
        "uptime_seconds": { "type": "integer", "minimum": 0 },
        "rss_bytes": { "type": "integer", "minimum": 0 },
        "cpu_utilization": { "type": "number", "minimum": 0 }
      }
    },
    "host_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "modules": {
      "type": "object",
      "propertyNames": { "$ref": "common.schema.json#/$defs/moduleName" },
      "additionalProperties": {
        "type": "object",
        "required": ["status"],
        "additionalProperties": false,
        "properties": {
          "status": { "enum": ["healthy", "degraded", "unavailable", "failed", "disabled"] },
          "reason": { "type": "string", "maxLength": 128 },
          "last_success_at": { "$ref": "common.schema.json#/$defs/timestamp" },
          "last_error": { "type": "string", "maxLength": 512 },
          "details": { "type": "object" }
        }
      }
    },
    "queue": {
      "type": "object",
      "required": ["depth_events", "size_bytes", "dropped_total"],
      "additionalProperties": false,
      "properties": {
        "depth_events": { "type": "integer", "minimum": 0 },
        "size_bytes": { "type": "integer", "minimum": 0 },
        "oldest_event_age_seconds": { "type": "integer", "minimum": 0 },
        "dropped_total": {
          "type": "object",
          "required": ["p0", "p1", "p2"],
          "additionalProperties": false,
          "properties": {
            "p0": { "type": "integer", "minimum": 0 },
            "p1": { "type": "integer", "minimum": 0 },
            "p2": { "type": "integer", "minimum": 0 }
          }
        },
        "rejected_total": { "type": "integer", "minimum": 0 },
        "lifecycle_rate_limited_total": { "type": "integer", "minimum": 0 }
      }
    },
    "reporting": {
      "type": "object",
      "required": ["consecutive_failures"],
      "additionalProperties": false,
      "properties": {
        "last_success_at": { "$ref": "common.schema.json#/$defs/timestamp" },
        "consecutive_failures": { "type": "integer", "minimum": 0 }
      }
    }
  }
}
```

For the `containers` module, `details` SHOULD contain `{ "engines": [ { "engine_id": "...", "runtime": "docker", "status": "healthy" } ] }`.

### A.10 `ingest-response.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "ingest-response.schema.json",
  "type": "object",
  "required": ["status", "accepted_count", "rejected_count"],
  "additionalProperties": false,
  "properties": {
    "status": { "enum": ["accepted", "partial", "rejected"] },
    "accepted_count": { "type": "integer", "minimum": 0 },
    "rejected_count": { "type": "integer", "minimum": 0 },
    "server_time": { "$ref": "common.schema.json#/$defs/timestamp" },
    "rejected": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "code", "retryable"],
        "additionalProperties": false,
        "properties": {
          "id": { "$ref": "common.schema.json#/$defs/uuid" },
          "code": { "enum": ["schema_invalid", "too_large", "unknown_type", "unauthorized", "internal"] },
          "message": { "type": "string", "maxLength": 512 },
          "retryable": { "type": "boolean" }
        }
      }
    }
  }
}
```

### A.11 `enroll-request.schema.json` and `enroll-response.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "enroll-request.schema.json",
  "type": "object",
  "required": ["agent_id", "host_id", "hostname", "agent_version", "os", "architecture"],
  "additionalProperties": false,
  "properties": {
    "agent_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "host_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "hostname": { "type": "string", "maxLength": 256 },
    "agent_version": { "type": "string" },
    "os": { "const": "linux" },
    "architecture": { "type": "string" },
    "environment": { "type": "string", "maxLength": 64 }
  }
}
```

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "enroll-response.schema.json",
  "type": "object",
  "required": ["agent_id", "credential"],
  "additionalProperties": false,
  "properties": {
    "agent_id": { "$ref": "common.schema.json#/$defs/uuid" },
    "credential": { "type": "string", "minLength": 32 },
    "credential_expires_at": { "$ref": "common.schema.json#/$defs/timestamp" }
  }
}
```

---

# Appendix B — Example payloads (`data` objects)

**Entity upsert (container)**

```json
{
  "snapshot_id": "0192f3a4-8000-7000-8000-000000000001",
  "entities": [
    {
      "id": "container:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e:9f2c7a1b5d8e0c4a6b3f1e2d7c9a8b5e4f3d2c1b0a9e8d7c6b5a4f3e2d1c0b9a",
      "kind": "container",
      "name": "payment-api",
      "attributes": {
        "container_id": "9f2c7a1b5d8e0c4a6b3f1e2d7c9a8b5e4f3d2c1b0a9e8d7c6b5a4f3e2d1c0b9a",
        "runtime": "docker",
        "engine_id": "engine:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e",
        "image": "registry.example.com/payment-api:1.8.2",
        "image_id": "4c1d...e9",
        "state": "running",
        "health": "healthy",
        "started_at": "2026-11-04T18:12:03Z",
        "main_pid": 4821,
        "ports": [{ "container_port": 3000, "protocol": "tcp", "host_ip": "0.0.0.0", "host_port": 3000 }],
        "networks": [{ "name": "app-network", "network_id": "a1b2...c3", "ip_address": "172.20.0.5" }],
        "mounts": [{ "type": "volume", "destination": "/data", "read_only": false, "volume_name": "payments-data" }]
      },
      "labels": { "com.docker.compose.service": "payment-api" },
      "observed_at": "2026-11-04T20:00:00Z"
    }
  ]
}
```

**Relationship upsert**

```json
{
  "snapshot_id": "0192f3a4-8000-7000-8000-000000000001",
  "relationships": [
    {
      "id": "rel:0f1e2d3c4b5a69788796a5b4c3d2e1f0",
      "from": "container:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e:9f2c...",
      "type": "uses_network",
      "to": "network:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e:a1b2...c3",
      "attributes": { "ip_address": "172.20.0.5" },
      "observed_at": "2026-11-04T20:00:00Z"
    }
  ]
}
```

**Metric batch**

```json
{
  "observed_at": "2026-11-04T20:00:05Z",
  "readings": [
    {
      "entity_id": "host:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
      "metrics": [
        { "name": "host.cpu.utilization", "kind": "gauge", "value": 0.42, "unit": "ratio" },
        { "name": "host.memory.used", "kind": "gauge", "value": 8589934592, "unit": "By" },
        { "name": "host.disk.utilization", "kind": "gauge", "value": 0.68, "unit": "ratio",
          "attributes": { "mountpoint": "/", "device": "/dev/sda1", "fstype": "ext4" } },
        { "name": "host.disk.io.read.bytes", "kind": "counter", "value": 91827364, "unit": "By",
          "temporality": "cumulative", "attributes": { "device": "sda" } }
      ]
    }
  ]
}
```

**Lifecycle**

```json
{
  "entity_id": "container:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e:9f2c...",
  "kind": "container",
  "name": "payment-api",
  "previous_status": "running",
  "status": "exited",
  "reason": "container_oom_killed",
  "details": { "exit_code": 137 },
  "observed_at": "2026-11-04T20:03:11Z"
}
```

**Snapshot completed**

```json
{
  "module": "containers",
  "snapshot_id": "0192f3a4-8000-7000-8000-000000000001",
  "scope_entity_id": "engine:3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03:docker-3fa91c2e",
  "covers_kinds": ["container", "container_network", "container_volume"],
  "complete": true,
  "started_at": "2026-11-04T20:00:00Z",
  "completed_at": "2026-11-04T20:00:01Z",
  "entities_count": 12,
  "relationships_count": 31,
  "errors": []
}
```

**Heartbeat**

```json
{
  "agent": {
    "id": "6f1f3b0e-2c1d-4a7e-9b52-0a1c8d3e4f55",
    "run_id": "a2d4e6f8-1b3c-4d5e-8f70-9a1b2c3d4e5f",
    "version": "1.0.0",
    "status": "degraded",
    "started_at": "2026-11-04T19:00:00Z",
    "uptime_seconds": 3600
  },
  "host_id": "3b9c1e52-8a47-5d0e-b6f1-2e7a4c9d1f03",
  "modules": {
    "host": { "status": "healthy" },
    "process": { "status": "healthy" },
    "containers": {
      "status": "degraded",
      "reason": "podman_socket_unreachable",
      "details": { "engines": [
        { "engine_id": "engine:3b9c...:docker-3fa91c2e", "runtime": "docker", "status": "healthy" },
        { "engine_id": "engine:3b9c...:podman-77ab01cd", "runtime": "podman", "status": "unavailable" }
      ] }
    },
    "network": { "status": "healthy" }
  },
  "queue": {
    "depth_events": 0, "size_bytes": 0,
    "dropped_total": { "p0": 0, "p1": 0, "p2": 0 }
  },
  "reporting": { "last_success_at": "2026-11-04T19:59:58Z", "consecutive_failures": 0 }
}
```
