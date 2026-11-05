# Implementation decisions

This file records choices where the specification leaves room for
interpretation, plus known deviations. It is updated as the build phases
progress.

## Repository and tooling

- The Go module is named `agent`. Internal imports are `agent/internal/...`.
- procfs parsing is hand-written rather than using
  `github.com/prometheus/procfs`. The specification explicitly allows this.
  Parsers are behind package-local helpers so the dependency can be replaced
  later.
- All timestamps use `events.Timestamp`, which normalizes to UTC and truncates
  to millisecond precision when serializing.

## IDs

- Entity IDs longer than 512 bytes are deterministically truncated and get a
  `-<8 hex chars of sha256(full ID)>` suffix. The suffix keeps truncated IDs
  collision-resistant while staying within the schema limit. IDs that already
  fit are unchanged.
- Percent-encoding follows RFC 3986 for every component containing `:`, `/`,
  or `%`. Percent-encoded uppercase hex is used.

## Schemas

- **Erratum:** the metric-name pattern in `schemas/v1/metric-batch.schema.json`
  is
  `^[a-z][a-z0-9_]*(\.[a-z0-9][a-z0-9_]*)+$`.
  Appendix A contains
  `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`, which rejects the catalog names
  `host.load.1m`, `host.load.5m`, and `host.load.15m` because a segment may
  begin with a digit. The catalog is normative and these metrics are required,
  so the pattern was relaxed in the schema. No other schema was changed.
- `agent.snapshot.completed.v1` with `complete: true` is enforced by the
  schema to carry an empty `errors` array.
- The schema set also validates enrollment request/response and ingest
  response documents, although those are not CloudEvents.

## Configuration

- Unknown YAML keys are rejected with `KnownFields(true)`.
- Secret files must be regular files with mode `0600`. The owner must be root
  when the agent runs as root. In development (non-root euid) the owner must be
  the current euid. This keeps fixtures usable without weakening the service
  deployment.
- Durations are validated against the documented minimums (1 second) even
  when tests would prefer faster values; tests construct config structs
  directly when they need sub-second intervals.
- `SIGHUP` reloads the configuration by rebuilding the collector manager
  around the existing spool and delivery pipeline. The old manager is stopped
  and a new one is constructed from the validated configuration, which avoids
  data races without a mutable shared config. Only hot-reloadable changes are
  accepted; `Config.RestartReasons` lists fields that require a process
  restart (paths, server, module enablement, runtime endpoints, reporting and
  spool parameters, logging format/file). The log level is applied in place
  through a `slog.LevelVar`.

## Runtime

- The scheduler runs a task immediately on start, then waits
  `interval * (0.9..1.1)`. Tests use explicit task invocations or construct
  schedulers with small intervals.
- A panic in a module or task is recovered and surfaced as a `PanicError`.
  The supervisor restarts failed modules with exponential backoff.
- Heartbeats use a separate `core.Sink` (`HeartbeatSink`). They are never
  written to the spool. If the heartbeat request fails it is dropped; the
  consecutive failure count is still reported.
- `agent once` always uses the stdout sink. It reads an existing identity but
  never writes identity or `seq` files; with no identity an ephemeral one is
  used. `agentseq` is an in-memory counter starting at 1 and `agentrun` is
  fresh per invocation, so `once` cannot collide with a running service using
  the same `agent_id`.
- `once` primes rate-based metrics (`PrimeMetrics` on host, process and
  containers), waits `--sample-wait` (default 2 s), then runs the full scans.
  It accepts `--modules host,process,...` and emits no lifecycle events or
  heartbeat. Exit code is 1 if a module failed or any emitted snapshot is
  incomplete; events are still printed. `config.LoadOnce` forces the stdout
  sink before validation, so development configs with an `http://` ingestion
  URL still work for read-only scans.
- `agent run` supports `sink: stdout` (development) and `sink: http`
  (production). HTTP requires `https://`; the only exception allowed by
  configuration validation is the stdout sink.

## Host module

- Filesystems are filtered twice: the configured type/prefix exclusions are
  applied first, then `statfs` must succeed and report a non-zero total block
  count. Pseudo filesystems such as `rpc_pipefs`/`sunrpc` are therefore absent
  from the `filesystems` attribute and never produce zero-size disk metrics.
  The default exclusion list includes `rpc_pipefs`, `nfsd`, `binfmt_misc`,
  `selinuxfs` and `efivarfs`.

## Process module

- The lifecycle baseline is seeded by the first **complete** inventory scan
  (or the first lifecycle scan when it runs first). This prevents every
  pre-existing process from being reported as `process_started` at startup.
  An incomplete inventory does not seed the baseline.
- `started_at` is `btime + starttime/USER_HZ`, with 10 ms resolution. The value
  is display-only; IDs use the raw start ticks.
- Command lines are joined from NUL-separated arguments with spaces and then
  every `redact_patterns` regex is applied. `/proc/[pid]/environ` is never read.
- In `top_n` mode the candidate set is the reportable set after
  `include_names`/`exclude_names`; the union of the top N by CPU and RSS is
  taken from it. This matches the configuration rule that non-empty
  `include_names` means only those processes are reported.
- Lifecycle events dropped by `limits.max_lifecycle_events_per_minute` are
  counted through `core.Deps.RateLimited`, which the manager forwards to the
  spool counter reported as `queue.lifecycle_rate_limited_total`.
- A missing boot ID makes the module `unavailable`; process IDs cannot be
  constructed safely without it.

## Spool and delivery

- Segment files are `spool/pN/seg-{first_seq:020d}.jsonl`; one compact
  CloudEvent per line. A segment is rotated before an event that would exceed
  `segment_size_mb`, except when the segment is still empty (a single event may
  exceed the configured segment size).
- Each priority class has a single cursor holding the last acknowledged
  `agentseq`. The delivery loop resolves events in spool order; a batch is
  never acknowledged past an unresolved earlier event. Retryable rejected
  events therefore stop the cursor at the preceding event and the rest of the
  class is retried (the server deduplicates).
- `Spool.Resolve` assumes receipts for a class are a contiguous prefix of the
  unresolved events in that class. This is guaranteed by the sender: batches
  are drained P0, P1, P2 and bisected in order.
- Eviction removes the oldest segment of the lowest-priority class that has
  data (P2, then P1, then P0). Events dropped this way are counted in
  `dropped_total` and are treated as resolved for queue-depth accounting.
- The spool writes are grouped by `flush_interval`; `Flush` and `ReadBatch`
  force a write before reading. The cursor file is written atomically and the
  directory is fsynced after the rename.
- Delivery treats 2xx with only non-retryable rejects as success. A 2xx
  containing retryable rejects is retried with backoff. 400/422 triggers
  sequential bisection; a single event that still fails is dropped and
  counted. 413 halves the event limit and splits the batch. 401 attempts one
  re-enrollment per five minutes. 403 waits five minutes before retrying.
- `Retry-After` accepts both delta-seconds and HTTP-date values. Backoff is
  full jitter between zero and the capped exponential interval.
- After a successful delivery that follows an outage longer than
  `resync_after_outage`, or after any spool drop, the sender calls the
  manager's `Resync`, which triggers a full inventory scan of each module that
  implements `core.Resyncer` (host, process, containers and network).

## Mock server

- `cmd/mockserver` validates every event against `schemas/v1`, deduplicates by
  event ID and `(agentid, agentseq)`, supports gzip request bodies, optional
  bearer authentication and enrollment, and injects failures through flags and
  `/admin/inject`.
- By default schema failures are reported per event with HTTP 200 and
  `retryable: false`; `-reject-http` makes the server return 400/422 instead so
  client bisection can be tested.
- The mock server issues random credentials on enrollment and stores them in
  memory; restarting it invalidates them, which is useful for exercising
  re-enrollment.

## Containers module

- Both Docker and Podman are accessed through a small shared HTTP client over
  the unix socket instead of importing `github.com/docker/docker/client` and
  the Podman bindings. This is a deliberate deviation from the suggested
  library: the wire API is tiny, the dependency trees are large, and the
  read-only guarantee is easier to enforce in one transport. The transport
  rejects every method except `GET` and `HEAD`, independently of the adapter,
  and a unit test covers it.
- Docker requests use the Docker-compatible API version prefix from
  `/version`. Current Podman releases require a **versioned libpod prefix**
  (`/v5.8.9/libpod/...`), while the §8.3 table shows the unversioned form.
  The adapter takes the libpod version from the Podman Engine component's
  `Details.APIVersion` (falling back to the component version), tries the
  versioned path first, and falls back to the unversioned path if the
  versioned `/libpod/info` returns 404. Both forms are covered by tests and
  the versioned form was verified against a live rootless Podman 5.8.9.
- The runtime-neutral model and adapter interface live in
  `internal/modules/containers/model` because the manager and the adapter
  subpackages would otherwise form an import cycle.
- Runtime identity is always taken from `/version` components
  ("Podman Engine"), never from the socket path. Discovery resolves symlinks
  and deduplicates real paths, so a `podman-docker` symlink is probed once.
- Autodiscovery paths are resolved under `paths.hostfs_root`; the `XDG_RUNTIME_DIR`
  candidates are mapped into the hostfs root too when it is not `/`.
- A socket that exists but does not answer is still reported as an engine with
  `engine_status: unavailable`; the configured/path-derived runtime hint is
  used to build its stable engine key until the daemon answers.
- Inspect caching lives inside each adapter (5 minute TTL, invalidated by a
  list-state change). This keeps `ListContainers` in the specification's shape
  while satisfying the inspect-caching rule.
- Docker memory working set uses `total_inactive_file` (cgroup v1) or
  `inactive_file` (cgroup v2). Podman's native stats do not expose the inactive
  file counters, so its `MemUsage` is reported as the working set; when a
  Docker-compatible Podman stats payload is returned, the same subtraction is
  applied. Podman stats support both native (`CPUNano`, `MemUsage`, ...) and
  Docker-compatible fields.
- Published container networks expose `bridge_interface` only for rootful
  engines and only while the named host interface exists (checked through
  netlink on every inventory scan). Rootless Podman/Docker networks never have
  a host bridge. The `container_network attached_to network_interface`
  relationship is emitted only when `bridge_interface` survives that check, so
  it cannot dangle. veth mapping remains best effort.
- Untagged images are reported with `dangling: true`. When
  `include_untagged_images` is false (the default), untagged images not used by
  any container are skipped as intermediate build layers; an untagged image
  used by a container is always kept.
- Every containers-module event (entity, relationship, metrics, lifecycle and
  snapshot) uses the engine entity as the CloudEvents `subject`. Event-driven
  and inventory emissions go through an engine-scoped emitter obtained with
  `WithSubject`; lifecycle events use `LifecycleWithSubject`.
- The two-method `core.ContainerIndex` from the specification is unchanged.
  Optional `core.ContainerEnumerator` (`All`) and `core.ContainerPublisher`
  (`Publish`) capabilities let the network module enumerate containers by
  network namespace and let the containers module publish the index without
  forcing every test fake to implement extra methods.
- Image digests for containers are taken from the Podman `Digest` field or an
  `@sha256:` image reference; Docker container inspect does not expose a
  repository digest, so the attribute is omitted there.
- Event stream breaks trigger an immediate full inventory reconciliation
  before reconnecting with the last observed event time. Reconciliations also
  compare the previous known-entity map and emit `state_reconciled`/`removed`
  lifecycle events; incomplete scans never imply removals. Replayed events are
  deduplicated by `(type, action, actor, timestamp)` because Docker `since`
  has one-second granularity.
- A failed inspect is a partial scan error, not an engine failure. The scan is
  reported with `complete: false`, entities already read are still emitted, and
  the engine is not marked down. A list-level API failure does mark the engine
  unavailable.
- Container and network lifecycle events share the
  `limits.max_lifecycle_events_per_minute` budget; dropped events are counted
  through the same spool/rate-limit counter as process lifecycle drops.

## Network module

- Netlink is behind a small `backend` interface. Production uses
  `github.com/vishvananda/netlink`; tests inject a fake backend and therefore
  never read or modify the host's interfaces. When the agent runs in a
  container with the host `/proc` mounted, the backend opens a netlink handle
  at `/proc/1/ns/net` so interface/route/address discovery and subscriptions
  describe the host namespace rather than the agent container's namespace.
  veth lookup uses the same host handle.
- Interface change detection subscribes to link, address and route updates,
  debounces for 500 ms, and reruns the interface and route inventories. The
  periodic scans remain the reconciliation source.
- Listening sockets are parsed from `{procfs_root}/net/*` for the host
  namespace and from `{procfs_root}/{main_pid}/net/*` for each distinct
  container network namespace. The host namespace is identified through
  `/proc/1/ns/net`; containers sharing a namespace (Podman pods) use the
  lexicographically first container ID for the `container-{id12}` scope.
- Socket inode → PID resolution walks `/proc/*/fd` under
  `modules.network.fd_walk_budget`. Findings are cached for 10 minutes. The
  cache can theoretically misattribute an inode that the kernel reuses after
  the owning socket was closed; this is bounded by the TTL and the next full
  walk.
- Process identity for `owned_by` is built from the boot-id prefix and the
  process start ticks, like the process module. If the boot ID is unavailable,
  sockets are still emitted without an owner edge.
- IPv6 link-local and multicast routes are skipped unless
  `include_local_routes` is set. Route `scope` is reported as
  `global|link|host|nowhere|other`.
- Socket entities are deduplicated by entity ID, so `SO_REUSEPORT` sockets
  owned by several PIDs produce one entity (the first owner found).

## Remaining limitations

- The optional netlink proc connector (exec/exit events with exit codes) is
  not implemented; polling remains the process lifecycle baseline, as allowed
  by §8.2.
- The load test in §13.1 (5,000 processes / 500 containers) is not part of the
  unit test suite; the collectors are bounded and time-limited but a dedicated
  load harness was not added.
- Docker/Podman adapter tests use inline fake unix-socket servers rather than
  recorded `testdata/docker` and `testdata/podman` files. The fake servers
  cover inspect caching, partial failures, stats, events, Podman pods and
  runtime identification.
