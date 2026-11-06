# Module `kernel` (Phase 20)

Part of the [node-health plan](README.md). Effort: M.

## 1. Summary and non-goals

Kernel-level events that matter operationally: OOM kills and classes of kernel
errors. It replaces the requested "journald events via `sd-journal`" (see the
README §1): `sd-journal` is libsystemd and needs cgo, while the requested
events are available from `/dev/kmsg` (the same ring buffer journald's kernel
messages come from) and `/proc/vmstat`, and service failures come from the
`systemd` module.

Non-goals: shipping log text, reading journal files, userspace logs, audit
records, writing to `/dev/kmsg`.

## 2. Contract

No new entity kinds. Metrics are attached to the existing `host` entity.

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `host.kernel.oom_kills` | counter | `count` | `oom_kill` in `/proc/vmstat` (cumulative since boot; kernel 4.13+, omitted if the field is absent) |
| `host.kernel.events` | counter | `count` | kmsg rule matches since the agent started; attribute `class` |

`class` values (fixed, low cardinality): `oom_kill`, `io_error`, `fs_error`,
`hung_task`, `soft_lockup`, `rcu_stall`, `mce`, `ecc_correctable`,
`ecc_uncorrectable`, `thermal_throttle`, `segfault`, `nf_conntrack_full`,
`nvme_timeout`, `kernel_oops`. All enabled classes are emitted on every tick
(value `0` is a real observation while the kmsg source is readable).

**Lifecycle:** one case, a process killed by the OOM killer: the matching
`process_exited` lifecycle event carries reason `process_oom_killed` and
`details` `{pid, constraint, memcg, anon_rss_kb, total_vm_kb}` (see §3.3). Other
classes produce counters only.

No snapshot is emitted (the module has no entities); it still reports health.

## 3. Design

### 3.1 kmsg reader

- Open `{hostfs_root}/dev/kmsg` `O_RDONLY|O_NONBLOCK`, `lseek` to the end so
  history before the agent started is not replayed (the vmstat counter covers
  earlier kills). Use the runtime poller; if registration fails, fall back to a
  1 s read loop.
- One `read` returns one record: `priority,sequence,timestamp_us,flags;message`
  followed by optional continuation lines starting with a space. Records longer
  than `max_message_bytes` are dropped. `observed_at` is the read time.
- `EPIPE` means the ring overran and records were lost: count it in
  `details.messages_lost` and continue.
- The reader is a single goroutine feeding a bounded channel; when the consumer
  is slow the oldest unprocessed records are dropped and counted.

### 3.2 Rule table

`internal/modules/kernel/rules.go` holds the fixed rule table: class, maximum
syslog level, and a compiled regular expression, applied only to records of that
level or more severe (warning and above, plus the OOM lines). Initial patterns
(finalized against recorded fixtures of the supported kernels):

| Class | Matches |
|---|---|
| `oom_kill` | `Out of memory: Killed process <pid> (<comm>) total-vm:..., anon-rss:...`, `Memory cgroup out of memory: Killed process ...`, and the preceding `oom-kill:constraint=...,oom_memcg=...,task=...,pid=...` line |
| `io_error` | `blk_update_request: ... error, dev <dev>, sector`, `Buffer I/O error on dev` |
| `fs_error` | `EXT4-fs error`, `EXT4-fs (...): remounting filesystem read-only`, `XFS (...): ... Corruption / metadata I/O error / Filesystem has been shut down`, `BTRFS error|critical` |
| `hung_task` | `INFO: task <comm>:<pid> blocked for more than <n> seconds` |
| `soft_lockup`, `rcu_stall` | `watchdog: BUG: soft lockup`, `rcu: INFO: rcu_.* detected stalls` |
| `mce` | `mce: [Hardware Error]`, `Machine check events logged` |
| `ecc_correctable`, `ecc_uncorrectable` | EDAC messages with `CE` / `UE` |
| `thermal_throttle` | `(Package|Core) temperature above threshold, cpu clock throttled` |
| `segfault` | `<comm>[<pid>]: segfault at` |
| `nf_conntrack_full` | `nf_conntrack: table full, dropping packet` |
| `nvme_timeout` | `nvme nvme<N>: ... timeout`, `controller is down`, `resetting controller` |
| `kernel_oops` | `BUG:`, `kernel BUG at`, `Oops:` |

`classes` in the config can restrict the set. The raw message text is used for
matching only; it is never emitted, and not logged above debug level (debug logs
carry the class only).

### 3.3 OOM victim handoff (`core.ExitHints`)

The process module's lifecycle scan sees the victim as a vanished PID and would
emit `process_exited`. To avoid a second, conflicting event:

1. On an `oom_kill` match the kernel module parses `pid`, `comm`, memory sizes,
   `constraint` and `oom_memcg`, and stores a hint keyed by `(pid, comm)` with a
   TTL of `hint_ttl` (60 s).
2. When the process module is about to emit `process_exited` for a PID it
   consults the store; a hint with the same `pid` and a `comm` equal to the
   process name is consumed, the reason becomes `process_oom_killed`, and the
   hint details are added.
3. A hint that is never consumed (a short-lived process that was never in the
   baseline, or a victim inside a container already reported by the runtime's
   own OOM event) expires silently; the counters still count it.

The kernel prints the PID of the init namespace, which is the PID the process
module sees (the agent runs with host PID visibility).

### 3.4 Tasks

- kmsg loop: continuous.
- Metrics tick (`metrics_interval`, 15 s): read `/proc/vmstat`, emit the vmstat
  counter and the class counters.
- `agent once`: emits only the `/proc/vmstat` counter (no baseline, no event
  stream), consistent with "once emits no lifecycle events".

## 4. Configuration

```yaml
modules:
  kernel:
    enabled: false
    metrics_interval: 15s
    collect_kmsg: true
    kmsg_path: ""                  # default {hostfs_root}/dev/kmsg
    classes: []                    # empty = all
    hint_ttl: 60s
    max_message_bytes: 1024
```

## 5. Health and failure modes

- Neither vmstat's `oom_kill` nor kmsg available: `NotApplicable`
  `unavailable`, reason `no_sources`.
- kmsg not readable (permission, device cgroup, `ProtectKernelLogs`): plain
  `unavailable` for the kmsg part, module `degraded` with reason
  `kmsg_unavailable`; the vmstat counter keeps working.
- Heartbeat details: `{kmsg_open, messages_lost, records_dropped, last_oom_at}`.

## 6. Security

kmsg may contain arbitrary kernel-logged strings (device names, occasionally
user-influenced data). They are matched in memory and discarded. Only the
fixed class names, numeric sizes, `pid`, `comm` and cgroup path of an OOM victim
are ever emitted.

## 7. Testing

`testdata/kmsg/` raw record streams from several kernel versions: global and
cgroup OOM, I/O error floods, hung task, MCE, EDAC, thermal throttle, an
oversized record, an overrun (`EPIPE`), continuation lines. Tests: class
counters, rate of drops under a flood, handoff (consumed, expired, `comm`
mismatch, PID reuse), `once` output, privacy (no message text in any event or
info-level log).

## 8. Phase 20 Definition of Done

Fixture streams give the expected counters; an OOM record turns the matching
`process_exited` into `process_oom_killed` with details and no duplicate event;
overrun is counted and survived; `agent once --modules kernel` is schema-valid.

## 9. Risks and limitations

kmsg wording varies across kernel versions, hence fixtures per version and
counters instead of free text. Container-internal OOM kills are only reported
through the runtime events for Docker and Podman; CRI engines are covered
later. `/dev/kmsg` content is bounded by the ring size, so a storm can lose
records (counted).
