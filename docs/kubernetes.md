# Plan: Kubernetes node-local discovery and CRI support

Phases 7-12. Status: designed, not implemented.

The agent stays outbound-only and read-only. Kubernetes support is a
**node-local inventory layer** (one agent per node, as a DaemonSet) plus a
**CRI adapter** so the existing containers module can describe containerd and
CRI-O containers. It extends the v1 architecture; it adds no parallel stack
and no new event types.

## 0. Review notes: what changed against the first draft

The first draft was checked against the code as it exists today. Corrections:

| # | Finding in the code | Change in this plan |
|---|---|---|
| 1 | `network` already supports an agent in a container with the host `/proc`: `sockets.go` uses `{procfs}/1/net` when `/proc/1/ns/net` differs from `/proc/self/ns/net`, and `backend.go` opens netlink at `/proc/1/ns/net` (DECISIONS, *Network module*). | `hostNetwork: true` is **not required**. The manifest defaults to `hostNetwork: false` (the pod keeps cluster DNS and the normal path to the API server) and Phase 8 verifies host-namespace network inventory. `hostNetwork: true` stays documented as the fallback. `hostPID: true` and `SYS_ADMIN`/`SYS_PTRACE` (or `privileged`) remain required for `setns` and reading `/proc/1/ns/net`. |
| 2 | `process/cgroup.go` only matches `cri-containerd-<id>.scope` and `crio-<id>.scope`. | Also needed: `crio-<id>` without `.scope` (cgroupfs driver) and the **bare 64-hex directory under `pod<uid>/`** that containerd uses with the cgroupfs driver (`/kubepods/<qos>/pod<uid>/<id>`). See §3.2. |
| 3 | `ids.KindForID` has an explicit prefix switch. | The new prefixes must be added there, with golden tests. |
| 4 | `core.ModuleHealth` has no `Details`, although `events.HeartbeatModule.Details` exists and `sendHeartbeat` does not copy it. | Add `Details` (shared with the node-health plan, whichever lands first owns it). |
| 5 | `internal/config/validation.go` accepts only `docker` and `podman` as runtime types. | Add `cri`. |
| 6 | `modules.Build` appends modules in a fixed order and `RunOnce` runs them in that order, so `kubernetes` must come **after** `containers`. | Build order is host, process, containers, network, kubernetes (and the node-health modules after that). |
| 7 | The services call in the draft used a cluster-wide list with a `metadata.namespace` field selector. | Use the namespaced path `GET /api/v1/namespaces/{ns}/services`. |
| 8 | RBAC listed `replicasets: get, list` and a blanket `namespaces: get`. | `replicasets: get` only; `namespaces: get` restricted with `resourceNames: [kube-system]`. |
| 9 | `agent.service` hardening (`ProtectKernelModules`, `ProtectClock`, ...) does not apply to the DaemonSet. | Only the container equivalents matter; see §5. |

Everything else in the first draft is kept.

## 1. Contract (additive v1)

### 1.1 New entity kinds and IDs

| Kind | ID prefix | Format |
|---|---|---|
| `kubernetes_node` | `k8snode` | `k8snode:{host_id}:{cluster_id}:{node_name}` |
| `kubernetes_pod` | `k8spod` | `k8spod:{host_id}:{cluster_id}:{pod_uid}` |

- `cluster_id`: `metadata.uid` of the `kube-system` namespace. If it cannot be
  read: `sha256(api_server_url)[:16]` with a warning. Stable across restarts;
  IDs do not depend on the API URL while the namespace UID is readable.
- The `host_id` component keeps IDs consistent with every other kind and
  preserves per-agent ownership. A pod UID never moves between nodes, so host
  scoping is safe.
- Built only in `internal/ids`: `KubernetesNodeID`, `KubernetesPodID`,
  `KindKubernetesNode`, `KindKubernetesPod`, `PrefixK8sNode`, `PrefixK8sPod`,
  plus `KindForID` cases and golden tests.

### 1.2 Entity attributes

`kubernetes_node`: `name`, `uid`, `cluster_id`, `ready` (bool),
`unschedulable` (bool), `created_at`, `kubelet_version`,
`container_runtime_version`, `os_image`, `kernel_version`, `operating_system`,
`architecture`, `capacity` and `allocatable` (raw quantity strings for `cpu`,
`memory`, `pods`, plus `ephemeral_storage` when present), allowlisted labels,
optional `topology: {region, zone}` from the well-known labels.

`kubernetes_pod`: `uid`, `name`, `namespace`, `node_name`, `phase`
(`Pending|Running|Succeeded|Failed|Unknown`), `ready` (bool), `qos_class`,
`created_at`, `started_at`, `pod_ip`, `pod_ips[]`, `host_ip`, `host_network`,
`host_pid`, `owner: {kind, name}` (a ReplicaSet owner is resolved to its
Deployment), `containers: [{name, container_id, image, kind: app|init|ephemeral}]`,
`ports`, `services`, `volumes: [{name, type, claim_name?}]`, allowlisted labels.

- `services`: `[{name, type, cluster_ip, ports: [{name, port, protocol, target_port}]}]`,
  matched by same-namespace selector subset, capped at 16.
- `containers[].container_id` is the bare CRI ID (scheme stripped).
- Never collected: annotations, env, command, secret or configmap names.
- Hard caps: 64 labels, 16 services, 64 containers, 64 ports per entity;
  values truncated to schema limits so one entity cannot exceed
  `reporting.max_event_bytes`.

`container` (CRI additions, optional fields only): `kubernetes_pod_uid`,
`kubernetes_pod_name`, `kubernetes_namespace` when the CRI labels provide them.
`runtime` on the engine widens to `containerd` and `cri-o`.

### 1.3 Relationships

No new relationship types.

| From | Type | To |
|---|---|---|
| `kubernetes_node` | `runs_on` | `host` |
| `kubernetes_pod` | `runs_on` | `kubernetes_node` |
| `kubernetes_pod` | `contains` | `container` |

The pod to container edge is emitted only when `Index.Lookup(podContainerID)`
resolves; unresolved links are retried shortly after startup. The Kubernetes
module owns, and removes, its own relationships.

### 1.4 Lifecycle

Existing statuses are reused; only kinds and reasons are new.

| Kind | Phase / state | Status | Reason |
|---|---|---|---|
| `kubernetes_pod` | Pending | `created` | `entity_created` / `state_reconciled` |
| `kubernetes_pod` | Running | `running` | `pod_started` |
| `kubernetes_pod` | Succeeded | `exited` | `pod_succeeded` |
| `kubernetes_pod` | Failed | `dead` | `pod_failed` |
| `kubernetes_pod` | deleted | `removed` | `entity_removed` |
| `kubernetes_node` | created | `created` | `entity_created` |
| `kubernetes_node` | Ready=True | `up` | `node_ready` |
| `kubernetes_node` | Ready=False | `down` | `node_not_ready` |
| `kubernetes_node` | deleted | `removed` | `entity_removed` |

New reason constants: `pod_started`, `pod_succeeded`, `pod_failed`,
`node_ready`, `node_not_ready`. Baseline rule is unchanged: pods and nodes that
exist at the first successful scan are reported through the snapshot only.

### 1.5 Metrics

| Entity | Metric | Kind | Unit | Source |
|---|---|---|---|---|
| `kubernetes_pod` | `kubernetes_pod.cpu.time` | counter | `s` | sum of CRI container CPU time |
| `kubernetes_pod` | `kubernetes_pod.cpu.utilization` | gauge | `ratio` | sum of per-container ratios (partial sums omitted unless complete) |
| `kubernetes_pod` | `kubernetes_pod.memory.used` | gauge | `By` | sum of CRI working sets |
| `kubernetes_pod` | `kubernetes_pod.containers.running` | gauge | `count` | pod status |
| `kubernetes_pod` | `kubernetes_pod.restarts` | gauge | `count` | sum of container `restartCount` |

- `kubernetes_node` has no metrics of its own; the existing `host.*` metrics
  describe the same machine.
- CRI containers emit `container.cpu.time`, `container.cpu.utilization`,
  `container.memory.usage`, and `container.memory.limit`/`utilization` when a
  limit is known. CRI does not expose network, blkio, pids or per-container
  restart counts: those metrics are **omitted** for CRI, never zero.
- `kubernetes_pod.cpu.time` can decrease when a container restarts (new
  counter); normal backend counter-reset handling applies. Record this in
  `DECISIONS.md`.
- `container.cpu.utilization` is defined against host logical cores; the pod
  sum is therefore also against host logical cores.

### 1.6 Schema diffs

- `common.schema.json`: add `kubernetes_node`, `kubernetes_pod` to
  `entityKind`; `k8snode`, `k8spod` to the `entityId` pattern; `kubernetes` to
  `moduleName`. `relationshipType` unchanged.
- `lifecycle-changed.schema.json`: add the five reasons.
- No change to snapshot, metric, heartbeat, enroll schemas beyond the widened
  enums. No new event types.
- The same enum widenings are made by the node-health plan; both are plain
  additions and merge trivially.

Rollout order: backend schema validators, then agent upgrade.

## 2. The `kubernetes` module

Location: `internal/modules/kubernetes/`.

### 2.1 API client and authentication

Hand-written, read-only REST client; no `client-go`, no informers. Five
endpoints are needed.

- **In-cluster:** `https://kubernetes.default.svc`; CA and token from the
  projected ServiceAccount volume. The token file is re-read per request so
  projected-token rotation works.
- **Kubeconfig:** minimal parser (clusters, contexts, users). Supports token,
  token file, client cert/key, CA file/data. **Rejects** `exec`,
  `auth-provider`, username/password and `insecure-skip-tls-verify`.
- TLS verification is always on. `http://` only for tests via an injected
  transport.
- A `RoundTripper` wrapper rejects every method except `GET`/`HEAD`; a unit
  test enforces it (same pattern as the container transport).
- HTTP/2 allowed; 10 s timeout for lists; 5-minute server-side watch timeouts;
  `User-Agent: agent/<version>`.

Endpoints:

```
GET /api/v1/nodes/{node_name}
GET /api/v1/pods?fieldSelector=spec.nodeName={node}&limit=500          (+ continue)
GET /api/v1/namespaces/{ns}/services?limit=500                         per involved namespace, cached 5 m
GET /apis/apps/v1/namespaces/{ns}/replicasets/{name}                   cached 10 m
GET /api/v1/namespaces/kube-system                                     once, for cluster_id
GET /api/v1/pods?fieldSelector=spec.nodeName={node}&watch=true&resourceVersion=...&allowWatchBookmarks=true
GET /api/v1/nodes?fieldSelector=metadata.name={node}&watch=true&resourceVersion=...
```

- 401/403 → module `unavailable` (`auth_failed`); other 4xx → `degraded`;
  410 on a watch → relist and reconnect.
- Node name: `modules.kubernetes.node_name`, else `NODE_NAME` (Downward API),
  else `ErrUnavailable{node_name_missing}`.
- `Init` probes the API and the node object and returns `ErrUnavailable` with a
  clear reason (the supervisor re-probes every 60 s). `Run` never exits on
  transient API errors; it keeps the scheduler alive and reports health.

### 2.2 Inventory and snapshots

- One snapshot per scan: `module: kubernetes`,
  `scope_entity_id: k8snode:...`, `covers_kinds: [kubernetes_node, kubernetes_pod]`.
- Pods filtered by `spec.nodeName`; pagination followed;
  `limits.max_entities_per_kind` applied to pods.
- The node entity is always emitted, one entity per pod, relationships as in
  §1.3.
- **Incomplete** (`complete: false`, no removals) on any list-level error
  (node, pods, services), pagination error, timeout, or limit hit. A partial
  pod list is still emitted.
- Owner resolution and service matching are enrichment: failures fall back
  (ReplicaSet name as owner, no services) and mark the module `degraded`, but
  do not make the snapshot incomplete unless the service **list** failed.
- Lifecycle diff against the previous **complete** scan; rate-limited via
  `limits.max_lifecycle_events_per_minute` and reported through
  `Deps.RateLimited`.
- Relationship removals are emitted for disappeared pods and nodes.

### 2.3 Watch and reconciliation

- Watch pods (node field selector) and the node. Any event posts to a
  coalescing trigger channel; a 500 ms debounce then runs a full pod
  reconcile.
- Reconcile uses `TryLock`; if a scan is running the trigger is dropped and the
  periodic scan catches up.
- 410 Gone → full relist with a new `resourceVersion`. Watch failures mark the
  module degraded but never stop the periodic inventory.
- `inventory_interval` (default 60 s; 30 s recommended in the Kubernetes
  example config) stays the source of truth. Services refresh every 5 minutes
  and on new namespaces; owner cache 10 minutes.
- Initial container linking can lag because modules start concurrently: if any
  pod container ID is unresolved after a scan, schedule up to 3 follow-up scans
  about 5 s apart.

### 2.4 Metrics

- `containers.running` and `restarts` come from the cached pod inventory every
  `metrics_interval`.
- CPU and memory come from a new read-only **PodUsageIndex** published by the
  containers module (§3.4), only for Running pods and only when the sample is
  at most 2 minutes old.
- `agent once` order is host, process, containers, network, kubernetes, so the
  containers metrics task fills the index before the Kubernetes `RunOnce`.

### 2.5 Health

- `healthy`: complete scan, watch connected (or watch disabled).
- `degraded`: partial enrichment errors, watch broken, incomplete scan.
- `unavailable`: API unreachable, auth failure, node missing.
- Add optional `Details map[string]any` to `core.ModuleHealth` and copy it into
  `HeartbeatModule.Details`. Kubernetes reports
  `{cluster_id, node_name, api_server, watch_connected, pods}`. This also makes
  the heartbeat `details` field usable by the containers module, as Appendix
  A.9 already suggests.

## 3. CRI support in the containers module

### 3.1 Adapter

New package `internal/modules/containers/cri/`, implementing the neutral model
over CRI v1 gRPC (`k8s.io/cri-api`, `google.golang.org/grpc`; both are pure
Go). Kubernetes 1.24 and newer only.

- Socket candidates: `/run/containerd/containerd.sock`,
  `/run/crio/crio.sock`, `/var/run/crio/crio.sock`,
  `/run/k3s/containerd/containerd.sock`, `/run/k0s/containerd.sock`, plus
  explicit `runtimes: [{type: cri, endpoint: unix://...}]`. All paths go
  through `hostfs_root`.
- Probe with `Version` (2 s); the runtime name comes from the response
  (`containerd`/`cri-o`), not from the path. Symlink dedupe as today.
- **Read-only unary interceptor** with an allowlist: `Version`, `Status`,
  `ListContainers`, `ContainerStatus`, `ListContainerStats`, `ListImages`,
  `ImageStatus`, `ListPodSandbox`, `PodSandboxStatus`. All streaming RPCs are
  rejected. A unit test calls `ExecSync` and `RunPodSandbox` and expects a
  permission error.
- `ListContainers`: state, timestamps, exit code, image and `imageRef`,
  labels. `ContainerStatus` is called for running containers (bounded by
  `stats_concurrency`) to get `info.pid`, mounts and the memory limit (cached
  for stats). `info.pid` is runtime-specific; when absent, network namespace and
  veth mapping are skipped for that container.
- `ListContainerStats`: one call per metrics tick; fills CPU, working set and
  limit only.
- `ListImages` is implemented (ID, tags, digests, size, dangling);
  `created_at`, architecture and OS are omitted.
- `ListNetworks`, `ListVolumes`, `ListPods` return `ErrNotSupported`.
- `Events` leaves the base adapter interface and becomes an optional
  `EventSource` capability. CRI does not implement it, so the manager starts no
  event loop for it. Docker and Podman keep it.
- Add `Capabilities()` (Events, Networks, Volumes, Pods, Images, PodUsage) so
  the manager skips unsupported collections without marking snapshots
  incomplete.
- Implement `io.Closer`: close the gRPC connection on re-probe, when a socket
  disappears, and on shutdown.

### 3.2 Containers manager and process module changes

- `covers_kinds` for a CRI engine is `[container]` only; images use the
  separate images snapshot. Known network, volume and pod entities are not
  falsely removed.
- Engine key `containerd-<hash8>` / `cri-o-<hash8>`.
- Container entities get the CRI fields from §1.2; `health` and
  `restart_policy` are omitted when unknown.
- Add `StatAvailability` bits (CPU, Memory, MemoryLimit, Network, BlockIO,
  Pids) to `model.Stats`: Docker and Podman set everything relevant, CRI sets
  CPU and Memory. Add `RestartCountKnown` to `model.Container`.
  `containerReading` emits only metrics whose bit is set.
- **`process/cgroup.go`** adds, after the existing patterns:

  | Pattern | Runtime hint | Case |
  |---|---|---|
  | `crio-([0-9a-f]{64})` (no `.scope`) | `cri-o` | cgroupfs driver |
  | `/pod[0-9a-f_-]+/([0-9a-f]{64})(?:/\|$)` | none (resolved through the index) | containerd, cgroupfs driver |

  The existing `.scope` patterns cover the systemd driver. A hint without a
  runtime still sets `container_id_hint`; `runs_on container` is emitted only
  when `Index.Lookup` succeeds, as before. Tests use real paths from a
  cgroup-v1 and a cgroup-v2 kind node.
- veth and namespace mapping works when `MainPID` is resolved from CRI;
  otherwise it is skipped with a debug log.

### 3.3 CRI IDs and labels

- Strip `containerd://`, `cri-o://`, `docker://` from pod-status container IDs
  before `Index.Lookup`.
- Map `io.kubernetes.pod.uid`, `io.kubernetes.pod.name`,
  `io.kubernetes.pod.namespace`, `io.kubernetes.container.name` into the
  optional container attributes and into `model.Container.PodUID`.
- Display name: `{pod_name}/{container_name}` when available, else the CRI
  metadata name, truncated.

### 3.4 Pod usage index

A small replace-all-per-engine index in `core`:

```go
type PodUsage struct {
    EngineKey         string
    PodUID            string
    ObservedAt        time.Time
    CPUTimeSeconds    float64
    HasCPUTime        bool
    CPUUtilization    float64
    HasCPUUtilization bool
    MemoryUsedBytes   float64
    HasMemory         bool
}
```

- The containers module publishes after each metrics tick, keyed by
  `(engineKey, PodUID)`.
- `Has*` is true only when **every** running container of the pod produced the
  sample, so partial sums are omitted rather than under-reported.
- The Kubernetes module sums samples for its pods. Publishing an empty slice
  clears stale data when an engine switches or stops.
- This avoids coupling the Kubernetes module to CRI and avoids a second CRI
  client.

## 4. Host and DaemonSet-mode fixes

Required for the agent to describe the node, not its own pod.

1. **Hostname helper** (`internal/hostname`), used by the host module and by
   enrollment in `cmd/agent/main.go` (both call `os.Hostname()` today):
   `AGENT_HOSTNAME` env, else `{hostfs_root}/etc/hostname` when
   `hostfs_root != "/"`, else `os.Hostname()`, else `{hostfs_root}/etc/hostname`.
2. **Host mountinfo:** `host/mounts.go` reads `{procfs_root}/self/mountinfo`
   (the pod's mount namespace). Add `modules.host.mountinfo_pid` (default `0` =
   self; the DaemonSet sets `1`). If PID 1's mountinfo is unreadable, mark the
   host module `degraded` with `mountinfo_unavailable` instead of reporting
   container filesystems.
3. **Network namespace:** already handled by the network module (see §0 row 1).
   Phase 8 verifies it; nothing to change unless the verification fails.
4. **Host PID namespace:** `hostPID: true` so `/proc/1` is the host init,
   foreign `/proc/*/fd` and namespaces are readable, and namespace switching
   works.
5. **Timezone:** `readTimezone` uses `readlink` under `hostfs_root`; works.
6. **Identity:** already reads `{hostfs_root}/etc/machine-id` and DMI from the
   host roots; no change.

## 5. Packaging

`deployments/kubernetes/`: `namespace.yaml`, `serviceaccount.yaml`,
`clusterrole.yaml`, `clusterrolebinding.yaml`, `configmap.yaml`,
`secret.example.yaml` (token placeholder only), `daemonset.yaml`, `README.md`.

DaemonSet essentials:

- `hostPID: true`; `hostNetwork: false` by default (see §0), documented
  fallback `hostNetwork: true` with `dnsPolicy: ClusterFirstWithHostNet`.
- `securityContext.privileged: true` for v1 (the systemd deployment is full
  root; the read-only guarantee is enforced in code). Granular alternative:
  `SYS_ADMIN` (setns), `SYS_PTRACE` (foreign `/proc/*/fd` and namespaces),
  `DAC_READ_SEARCH`.
- `hostPath` mounts, all read-only except state:
  `/` → `/host` (statfs, `/etc/os-release`, `/etc/machine-id`, `/etc/hostname`),
  `/proc` → `/host/proc`, `/sys` → `/host/sys`,
  `/var/lib/agent` read-write, and **individual** CRI sockets (for example
  `/run/containerd/containerd.sock`). Do not mount all of `/run`: it would
  shadow the projected ServiceAccount token.
- ConfigMap at `/etc/agent/agent.yaml`. Enrollment-token Secret at
  `/etc/agent/enrollment-token` with `defaultMode: 0600` (validation requires
  exactly `0600`, root-owned).
- Downward API env `NODE_NAME` and `AGENT_HOSTNAME` from `spec.nodeName`.
- `priorityClassName: system-node-critical`, tolerations `operator: Exists`,
  `nodeSelector: kubernetes.io/os: linux`.
- `updateStrategy.rollingUpdate: {maxSurge: 0, maxUnavailable: 1}` so two
  agents never share `/var/lib/agent` on one node.
- `terminationGracePeriodSeconds: 15`. No ports, no probes in v1 (the agent
  opens no sockets; process exit is the liveness signal).
- Image: static binary on `distroless/static` (includes CA certificates),
  UID 0, multi-arch `amd64`/`arm64`.

RBAC (read-only; no secrets, configmaps, logs, exec):

| Resource | Verbs | Note |
|---|---|---|
| `nodes` | `get`, `list`, `watch` | |
| `pods` | `get`, `list`, `watch` | |
| `services` | `get`, `list` | no watch (cached 5 m) |
| `namespaces` | `get` | `resourceNames: [kube-system]` |
| `replicasets` (apps) | `get` | |

Coexistence: the DaemonSet and the systemd agent must not both use
`/var/lib/agent` on one node. Choose one per node; document it. `maxSurge: 0`
only protects DaemonSet rollouts.

## 6. Configuration

New block; `enabled: false` by default so existing behavior is unchanged.

```yaml
modules:
  kubernetes:
    enabled: false
    node_name: ""                 # default: NODE_NAME env
    api:
      server: ""                  # default: in-cluster https://kubernetes.default.svc
      kubeconfig: ""              # alternative to in-cluster
      ca_file: ""                 # default: ServiceAccount ca.crt
      token_file: ""              # default: ServiceAccount token (re-read per request)
      timeout: 10s
    inventory_interval: 60s
    metrics_interval: 15s
    watch: true
    watch_debounce: 500ms
    services_interval: 5m
    owner_cache_ttl: 10m
    label_allowlist: ["app", "app.kubernetes.io/*", "component", "k8s-app", "name", "part-of", "version", "tier", "role", "helm.sh/chart", "topology.kubernetes.io/*", "node.kubernetes.io/*", "node-role.kubernetes.io/*", "kubernetes.io/hostname"]
    max_services_per_pod: 16
    max_containers_per_pod: 64
```

Validation: all intervals at least 1 s; `watch_debounce` at least 50 ms;
`insecure_skip_tls_verify` does not exist in the schema and kubeconfigs that set
it are rejected by the module; label globs compile with `path.Match`;
`modules.containers.runtimes[].type` accepts `cri`.

Reload: `RestartReasons` adds `modules.kubernetes` for `enabled`, `node_name`
and `api.*`. Intervals, watch, allowlist and caches are hot-reloadable.
`agent validate-config` does not need a reachable API; `once` and `run` report
`ErrUnavailable` at module init.

## 7. File-by-file change map

| Area | Files |
|---|---|
| Schemas | `schemas/v1/common.schema.json`, `schemas/v1/lifecycle-changed.schema.json`, `internal/schema/schema_test.go` |
| IDs, constants | `internal/ids/ids.go` (+`KindForID`), `internal/ids/ids_test.go`, `internal/events/lifecycle.go` |
| Config | `internal/config/config.go`, `internal/config/validation.go` (`cri`, new block, `RestartReasons`), tests |
| Core | `internal/core/types.go` (`ModuleHealth.Details`, pod-usage interfaces), `internal/core/index.go` (pod usage store), `internal/core/manager.go` (heartbeat details) |
| Module wiring | `internal/modules/modules.go` (+ test) |
| Kubernetes module (new) | `internal/modules/kubernetes/{module,client,auth,inventory,entities,lifecycle,metrics,services,owner,watch}.go` + tests |
| CRI adapter (new) | `internal/modules/containers/cri/{adapter,interceptor,stats}.go` + tests |
| Containers | `model/model.go` (capabilities, stat bits, pod fields), `discovery.go`, `containers.go`, `inventory.go`, `entities.go`, `metrics.go`, `docker/adapter.go`, `podman/adapter.go` |
| Process | `internal/modules/process/cgroup.go` (+ tests) |
| Host and CLI | `internal/modules/host/inventory.go`, `internal/modules/host/mounts.go`, new `internal/hostname`, `cmd/agent/main.go` |
| Packaging | `deployments/kubernetes/*`, `Dockerfile`, `examples/agent-kubernetes.yaml` |
| Docs | `docs/DECISIONS.md`, `README.md` |
| Fixtures | `testdata/kubernetes/{pods,node,services,replicasets}.json`, CRI gRPC fixtures, cgroup path fixtures |

## 8. Phases and Definition of Done

**Phase 7 - Contract foundations.** Schemas, ID builders and `KindForID`,
reason constants, config structs/defaults/validation, `ModuleHealth.Details`,
docs. *DoD:* golden ID tests; new-kind sample events validate against
`schemas/v1`; the mock server accepts them; all existing tests pass; no
collector behavior changes.

**Phase 8 - DaemonSet host mode and packaging.** Hostname helper,
`mountinfo_pid`, host module/CLI wiring, Dockerfile, manifests, example config.
*DoD:* `agent once` inside a kind node reports the node hostname (not the pod
name), host filesystems and disks (not the pod overlay), and the **host**
network namespace with `hostNetwork: false`; `kubectl apply --dry-run=server`
succeeds; no Kubernetes entities yet.

**Phase 9 - Kubernetes inventory.** Client/auth, module lifecycle,
node/pod/service/owner inventory, entities and relationships, snapshots,
lifecycle diff, watch triggers. *DoD:* `agent once --modules host,kubernetes`
emits node, pods and snapshot; pod create/delete and node Ready transitions
produce lifecycle events within about 2 s with watch on; incomplete scans never
imply removal; fixture tests cover pagination, 410, owner resolution, service
matching, dual-stack, init containers and limits.

**Phase 10 - CRI adapter and containers integration.** Adapter, interceptor,
capability and eventless refactor, stat availability, images, pod labels,
pod-usage publication, cgroup patterns. *DoD:* a kind node reports a
`containerd` engine, container entities with CPU/memory after the second
sample, images, `process runs_on container` links and
`kubernetes_pod contains container` links; Docker/Podman behavior and tests
unchanged; the interceptor rejects mutating and streaming RPCs; CRI
network/blkio/pids/restarts are omitted, not zero; cgroup patterns pass on both
the systemd and cgroupfs drivers.

**Phase 11 - Pod metrics, lifecycle, health hardening.** Pod aggregates,
lifecycle rate limiting, watch reconnect and resync, heartbeat details, resync
after outage. *DoD:* `kubernetes_pod.*` metrics for running pods; absent when
the containers module or CRI engine is unavailable; heartbeat reports the
Kubernetes status and details; spool outage plus resync produces a fresh
snapshot; a restart keeps IDs and creates no duplicates.

**Phase 12 - E2E, load, docs.** Kind-based e2e (DaemonSet plus mock server),
load fixtures, README and upgrade notes, decisions. *DoD:* all existing §13.2
criteria still pass; Kubernetes-specific acceptance: one node entity per node,
pods only from that node, lifecycle, services and owner attributes, CRI
linking, pod metrics, no mutating API calls, rollout with `maxSurge: 0`;
`go test -race ./...` and e2e green; resource budget verified with about 300
pods and 1,000 containers.

## 9. Testing matrix

| Area | Tests |
|---|---|
| IDs | Golden `k8snode`/`k8spod`; cluster/host/node variations; percent-encoding; `KindForID` |
| Client | GET-only transport; token rotation; pagination; watch 410/bookmark; kubeconfig token/cert parsing; reject exec/insecure; 401/403/timeout mapping |
| Inventory | Pod/node/service/ReplicaSet fixtures; owner chain; selector matching; dual-stack; init/ephemeral containers; label caps; limit and incomplete snapshots |
| Lifecycle | Baseline suppression; phase transitions; node Ready; removal; rate-limit accounting |
| Watch | ADDED/MODIFIED/DELETED/BOOKMARK/ERROR; debounce; reconcile trigger; reconnect with last resourceVersion |
| CRI | Fake gRPC server; `Version`/`ListContainers`/`ContainerStatus`/`ListContainerStats`/`ListImages`; pid parsing; limit cache; read-only interceptor; eventless manager; adapter close on re-probe |
| CRI metrics | First-sample CPU skip; utilization clamp; working set; unavailable network/blkio/pids/restarts omitted |
| Containers | CRI capabilities exclude networks/volumes/pods from `covers_kinds`; Docker/Podman regressions |
| Process | `cri-containerd-<id>.scope`, `crio-<id>`, bare id under `pod<uid>/`; PID reuse |
| Host | Hostname resolution order; mountinfo PID selection; timezone; degraded when mountinfo is unavailable |
| Schema | Every new event validates; mock server accepts; heartbeat with kubernetes details |
| E2E | Kind cluster, DaemonSet, enrollment, pod lifecycle, container metrics, process links, outage and resync |

## 10. Risks and limitations

- **Backend rollout:** new kinds and reasons must be accepted by the ingestion
  backend before agents are upgraded.
- **Broad node access:** the DaemonSet is privileged with `hostPID` and
  read-only host mounts. Guardrails: GET-only API client, read-only CRI
  interceptor, no shell, read-only RBAC.
- **CRI gaps:** no network, blkio, pids or restart counts per container; pod
  network metrics need a later kubelet/cAdvisor or CRI pod-sandbox-stats
  extension.
- **Eventless CRI:** container lifecycle latency equals `inventory_interval`
  (30 s recommended). Pod lifecycle is watch-driven (about 2 s).
- **CRI PID dependency:** `ContainerStatus.info.pid` is runtime-specific; when
  absent, namespace/veth mapping is skipped for that container.
- **Services are attributes:** no `kubernetes_service` entities in v1; cached up
  to 5 minutes.
- **No workloads or namespaces as entities:** cluster-scoped duplication and
  snapshot-removal semantics are deferred; a cluster mode can add them later.
- **Kubeconfig subset:** no exec/auth-provider plugins.
- **State directory exclusivity:** systemd agent and DaemonSet must not share
  `/var/lib/agent` on one node.
- **Cluster ID fallback:** if the `kube-system` UID is forbidden, IDs use an
  API-URL hash and change if the server URL changes; the RBAC rule avoids this.
- **Single-entity size:** pod attributes are capped so the event builder never
  has to split one entity.

## 11. Decisions taken in this plan

- No new event types; no new relationship types.
- `hostNetwork: false` by default; host namespaces are reached through
  `/proc/1`, as the network module already does.
- Hand-written read-only API client instead of `client-go`.
- Pod CPU/memory are derived from CRI container stats through a shared index,
  not from the kubelet.
- CRI engines are eventless; reconciliation is the lifecycle source for
  containers.
- `ModuleHealth.Details` is introduced once and shared with the node-health
  plan.
