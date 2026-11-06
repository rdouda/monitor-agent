# Plan: node health modules

Phases 13-24. Status: designed, not implemented.

Adds the "high value, low effort" host-level signals that need only files,
sysfs, procfs, D-Bus or a few read-only ioctls. Each signal group is its own
module, so each can be enabled, fail, and report health independently.

| Module (config key) | Document | Signals |
|---|---|---|
| `certificates` | [certificates.md](certificates.md) | TLS certificate expiry for local certificates |
| `sensors` | [sensors.md](sensors.md) | hwmon, thermal zones, fans, voltages, power and energy |
| `hardware` | [hardware.md](hardware.md) | DMI, PCI, USB, NUMA, hugepages, kernel modules, taint |
| `posture` | [posture.md](posture.md) | pending updates, reboot-required, kernel drift, unattended upgrades, SELinux/AppArmor |
| `storage` | [storage.md](storage.md) | block devices, SMART/NVMe health, mdraid, btrfs, ZFS, LVM, disk temperature |
| `systemd` | [systemd.md](systemd.md) | units and timers over D-Bus |
| `kernel` | [kernel.md](kernel.md) | OOM kills and kernel error classes |
| `timesync` | [timesync.md](timesync.md) | chrony / timesyncd / kernel clock state |
| `auth` | [auth.md](auth.md) | login sessions, failed logins |

## 1. Analysis of the original request

Mapping of the nine requested items, and where the analysis found that the
"low effort" label or the stated mechanism does not hold.

| Requested | Where | Effort | Finding |
|---|---|---|---|
| systemd units and timers via D-Bus | `systemd` | M | Feasible with a pure-Go D-Bus client. Needs a read-only call allowlist (the baseline forbids shelling out to `systemctl`). |
| journald events via `sd-journal` | `kernel` | M | **`sd-journal` is libsystemd (cgo)** and conflicts with `CGO_ENABLED=0` (`AGENTS.md` §0.6). The requested events are available without it: OOM kills and kernel errors from `/dev/kmsg` and `/proc/vmstat`; service failures from the systemd D-Bus `Result` property. The module is therefore named `kernel`. A native journal-file reader is deferred (decision D-3). |
| Disk health: SMART (ioctl), mdstat, LVM, btrfs/ZFS, temperature | `storage` | S for mdraid/btrfs/temperature, **L for SMART**, **L for LVM**, limited for ZFS | mdraid and btrfs are pure sysfs/procfs. SMART needs ATA pass-through (`SG_IO`) and NVMe admin ioctls with an opcode allowlist, plus quirks for vendor attributes. LVM capacity and thin-pool state are not in sysfs and need device-mapper ioctls. ZFS exposes only pool state and I/O through `/proc/spl`; capacity and vdev errors need an unstable ioctl ABI and are out of scope. |
| Sensors: hwmon, thermal, fans, voltages, power | `sensors` | S | Pure sysfs. Needs new metric units (`Cel`, `V`, `W`, `A`, `J`, `rpm`). |
| Package/update state | `posture` | **M, partial** | Without `apt`/`dnf` (no shelling out) the number of pending updates is only available where a distro writes it to a file (Ubuntu `updates-available`). Reboot-required (Debian/Ubuntu) and kernel drift are reliable. Native candidate computation is deferred (D-4). |
| Time sync: chrony/NTP | `timesync` | M | `adjtimex(2)` with `modes=0` gives sync state for any daemon with no protocol work. chrony needs its binary command protocol (read-only subset). timesyncd works over D-Bus. `ntpd` is not covered. |
| Hardware inventory | `hardware` (+ block devices in `storage`) | S/M | Pure sysfs. Block device model/serial live in `storage` so one module owns the `disk` entity. |
| Logins/security | `auth`, `posture` | M | `utmp`/`btmp` are binary files; `btmp` is absent on some modern distros. Failed-login **usernames are never collected** (users type passwords there). SELinux/AppArmor mode is trivial and lives in `posture`. |
| TLS certificate expiry | `certificates` | S | Not in the original README backlog; added. |

## 2. Shared contract (Phase 13)

All changes are additive. Both this plan and the Kubernetes plan widen the same
enums; the first to land creates the lines, the second appends.

### 2.1 Entity kinds and ID formats

| Kind | Prefix | Format | Module |
|---|---|---|---|
| `systemd_manager` | `systemd` | `systemd:{host_id}` | `systemd` |
| `systemd_unit` | `unit` | `unit:{host_id}:{unit_name}` | `systemd` |
| `disk` | `disk` | `disk:{host_id}:{disk_key}` | `storage` |
| `raid_array` | `raid` | `raid:{host_id}:{array_key}` | `storage` |
| `storage_pool` | `pool` | `pool:{host_id}:{pool_type}:{pool_key}` | `storage` |
| `sensor_chip` | `sensor` | `sensor:{host_id}:{chip_key}` | `sensors` |
| `hardware` | `hardware` | `hardware:{host_id}` | `hardware` |
| `pci_device` | `pci` | `pci:{host_id}:{address}` | `hardware` |
| `usb_device` | `usb` | `usb:{host_id}:{port_path}` | `hardware` |
| `numa_node` | `numa` | `numa:{host_id}:{node_id}` | `hardware` |
| `time_sync` | `timesync` | `timesync:{host_id}` | `timesync` |
| `host_posture` | `posture` | `posture:{host_id}` | `posture` |
| `login_session` | `session` | `session:{host_id}:{session_key}` | `auth` |
| `certificate` | `cert` | `cert:{host_id}:{sha256_fingerprint}` | `certificates` |

Existing prefixes are `host process engine container pod image network volume
iface route socket`; none collides with the table, and no new prefix is a
prefix of another (`pod` and `pool`/`posture` differ before the colon).

Builders live only in `internal/ids`: `SystemdManagerID`, `SystemdUnitID`,
`DiskID`, `RAIDArrayID`, `StoragePoolID`, `SensorChipID`, `HardwareID`,
`PCIDeviceID`, `USBDeviceID`, `NUMANodeID`, `TimeSyncID`, `HostPostureID`,
`LoginSessionID`, `CertificateID`, with matching `Kind*` and `Prefix*`
constants, `KindForID` cases and golden tests. Components containing `:`, `/`
or `%` (PCI addresses such as `0000:00:1f.2`, unit names with `/`) go through
the existing percent-encoder.

Key rules for the opaque components:

- `unit_name`: the systemd unit name as reported (for example
  `getty@tty1.service`; backslash escapes such as `\x2d` are kept as is).
- `disk_key`, `array_key`, `pool_key`, `chip_key`: defined in the module
  documents. Hardware serial numbers are **hashed**, never placed in an ID.
- `sha256_fingerprint`: lowercase hex of the SHA-256 of the certificate DER.

### 2.2 Schema diffs

| File | Change |
|---|---|
| `common.schema.json` | `entityKind` += the 14 kinds above. `entityId` pattern += `systemd\|unit\|disk\|raid\|pool\|sensor\|hardware\|pci\|usb\|numa\|timesync\|posture\|session\|cert`. `moduleName` += `systemd`, `kernel`, `storage`, `sensors`, `hardware`, `timesync`, `posture`, `auth`, `certificates`. |
| `metric-batch.schema.json` | `unit` += `Cel` (degrees Celsius), `V`, `W`, `A`, `J`, `rpm`. Units remain a closed enum. |
| `lifecycle-changed.schema.json` | `reason` += the 13 reasons in §2.3. `status` is **unchanged**. |
| `snapshot-completed.schema.json`, `heartbeat.schema.json` | none beyond the widened `moduleName`. |

Metric name rules are unchanged (`^[a-z][a-z0-9_]*(\.[a-z0-9][a-z0-9_]*)+$` as
fixed in `DECISIONS.md`). A single `Reading` holds at most 64 metrics per
entity; modules with more (a 64-core `coretemp` chip) emit several `Reading`
objects for the same entity in one batch.

### 2.3 Lifecycle vocabulary additions (extends §7.5)

| Kind | Allowed `status` |
|---|---|
| `systemd_unit` | `created`, `running`, `exited`, `dead`, `restarting`, `removed` |
| `disk`, `raid_array`, `storage_pool` | `created`, `up`, `down`, `removed` |
| `time_sync` | `up`, `down` |
| `login_session`, `usb_device`, `pci_device`, `numa_node`, `sensor_chip`, `certificate` | `created`, `removed` |
| `systemd_manager`, `hardware`, `host_posture` | none (no lifecycle events) |

New reasons: `unit_started`, `unit_stopped`, `unit_failed`, `unit_restarting`,
`disk_health_ok`, `disk_health_failing`, `raid_degraded`, `raid_recovered`,
`pool_degraded`, `pool_healthy`, `time_sync_acquired`, `time_sync_lost`,
`process_oom_killed`. `entity_created` / `entity_removed` are reused.

The baseline rule holds everywhere: entities that exist at startup are reported
through the snapshot only; lifecycle events are emitted only for transitions
observed afterwards. Lifecycle events of every new module count against
`limits.max_lifecycle_events_per_minute` and are reported through
`Deps.RateLimited`.

### 2.4 Relationships (no new types)

| From | Type | To | Module |
|---|---|---|---|
| `systemd_manager` | `runs_on` | `host` | systemd |
| `systemd_unit` | `managed_by` | `systemd_manager` | systemd |
| `disk`, `raid_array`, `storage_pool`, `sensor_chip`, `hardware`, `pci_device`, `usb_device`, `numa_node`, `time_sync`, `host_posture`, `login_session`, `certificate` | `runs_on` | `host` | each |
| `raid_array` | `contains` | `disk` | storage |
| `storage_pool` | `contains` | `disk` or `raid_array` | storage |
| `pci_device` | `attached_to` | `numa_node` | hardware |
| `sensor_chip` | `attached_to` | `pci_device` | sensors (only if `hardware` is enabled) |
| `disk` | `attached_to` | `pci_device` | storage (only if `hardware` is enabled) |
| `network_interface` | `attached_to` | `pci_device` | network (only if `hardware` is enabled; Phase 16 change) |
| `login_session` | `owned_by` | `process` | auth (leader PID, when resolvable) |

Rule: a relationship is emitted by the module that owns the `from` entity
(`AGENTS.md` §7.3), and a **cross-module edge is emitted only when the target
module is enabled**, so edges never point at entities that will not exist.

### 2.5 Framework changes (Phase 13)

1. **`core.ModuleHealth.Details`** (shared with the Kubernetes plan) copied into
   `HeartbeatModule.Details` by `sendHeartbeat`. Keep each module's details
   under 2 KB. Documented keys are listed per module.
2. **Not-applicable modules.** Today any `unavailable` module makes the whole
   agent `degraded` in the heartbeat. That is wrong for hardware-dependent
   modules (no hwmon on a VM, no `btmp` on a minimal host). Add
   `NotApplicable bool` to `core.ErrUnavailable` and `core.ModuleHealth`. A
   not-applicable module still reports `status: unavailable` with a `reason`
   and `details.not_applicable: true`, but is **ignored when the agent-level
   status is computed**. Existing modules keep their behavior (a missing Docker
   socket still degrades the agent; Acceptance 11 is unchanged). Rule for new
   modules: `NotApplicable` for "this machine does not have that thing";
   plain `unavailable` for "the thing exists but the operator must grant
   access" (for example `/dev/kmsg` not readable).
3. **`internal/dbusro`:** read-only D-Bus wrapper shared by `systemd`,
   `timesync` and `auth` (details in `systemd.md`). Built in Phase 19, but its
   interface is fixed here so `timesync` and `auth` can be developed against a
   fake.
4. **`core.ExitHints`:** a small TTL store the `kernel` module writes and the
   `process` module reads, so an OOM-killed process gets
   `process_oom_killed` without a second lifecycle event (see `kernel.md`).
5. **`internal/modbase`** (optional but recommended): a small helper that holds
   what `host.go`, `process.go` and `network.go` each duplicate (mutex, deps,
   health setters, `MarkNotApplicable`, snapshot ID and the
   `SnapshotCompleted` boilerplate). Existing modules are not refactored.
6. **`modules.Build`** appends in this order after `network`: `kubernetes`,
   `certificates`, `sensors`, `hardware`, `posture`, `storage`, `systemd`,
   `kernel`, `timesync`, `auth`. `agent once` runs them in that order. Rate
   metrics need two samples only where noted per module.
7. **Config:** one block per module under `modules:`, all `enabled: false` by
   default. `RestartReasons` gains `modules.<name>.enabled` and every path or
   socket key; intervals, filters and allowlists are hot-reloadable.
   `examples/agent.yaml` gets commented, disabled blocks.
8. **Paths:** no new root. Everything outside `/proc` and `/sys` is read under
   `{hostfs_root}` (`/dev`, `/run`, `/var/log`, `/etc`, `/boot`, `/lib/modules`).
   Collectors take their roots from `core.Paths` only.

### 2.6 Cross-cutting design rules

- **Absence is not an error.** A missing optional source (no btrfs, no md, no
  chrony) produces no entity and no snapshot error. Only a source that should
  exist and fails to read marks a snapshot incomplete.
- **Unknown is omitted.** A value that cannot be determined is left out; it is
  never `0`, `""` or `false` unless the source says so.
- **Slow facts are attributes, fast values are metrics.** Health *state* that
  changes rarely (SMART overall, RAID degraded) is both an attribute and a
  lifecycle event; numbers that change every sample are metrics.
- **Every scan has a timeout** and file reads that can block (hwmon, device
  files) run in a goroutine with a per-source deadline so a stuck driver cannot
  stall the scheduler.
- **Bounded output.** Each module has a `max_*` limit; hitting it emits
  `limit_exceeded` and an incomplete snapshot.
- **Personal and hardware identifiers** (serial numbers, usernames, remote
  hosts) have explicit `collect_*` switches; defaults are in each document.

## 3. Security

Read-only is enforced in code at each boundary, with a unit test per boundary
that proves a mutating call is refused, and an allowlist (default deny).

| Boundary | Enforcement | Module |
|---|---|---|
| D-Bus | `dbusro` allows only `Properties.Get/GetAll`, listed read methods, and match-rule subscription; everything else returns an error | systemd, timesync, auth |
| SMART / ATA | Opcode allowlist: identify, SMART read data, read thresholds, return status, read log, check power mode. No `SMART ENABLE/DISABLE`, no self-test start, no writes | storage |
| NVMe | Admin opcode allowlist: `Identify`, `Get Log Page`. No format, firmware or set-feature commands | storage |
| SCSI | `INQUIRY`, `LOG SENSE` only | storage |
| device-mapper | Control ioctl allowlist: version, list devices, device status, table status, table deps. No create, remove, suspend, load or clear | storage (Phase 24) |
| chrony | The client type exposes only tracking, sources, source data and activity requests; there is no generic send. A test asserts the request codes it can encode | timesync |
| `adjtimex` | Called with `modes = 0` only; a test asserts the constant | timesync |
| files and devices | Opened `O_RDONLY`; `/dev/kmsg` opened read-only and non-blocking | all |
| certificates | Only `CERTIFICATE` PEM blocks are decoded; private-key blocks are discarded and never logged; directories named `private` are not opened | certificates |

Other rules: no shelling out, no `environ`, no log shipping (raw kernel and auth
log text never leaves the host), IDs contain no raw serials, usernames from
failed logins are never read into memory beyond the record parse, nothing is
listened on.

### systemd unit hardening (`deployments/systemd/agent.service`)

The shipped hardening is compatible with this plan **except** for these notes.
Do not add the following to the unit; each breaks a module:

| Setting | Breaks |
|---|---|
| `ProtectClock=yes` | `adjtimex` (its syscall filter blocks the clock syscalls, including the read-only call) |
| `ProtectKernelLogs=yes` | `/dev/kmsg` |
| `PrivateDevices=yes`, restrictive `DeviceAllow=` | block-device ioctls, `/dev/kmsg`, device nodes in general |
| `PrivateNetwork=yes` | chrony UDP command port (also forbidden by the baseline) |

The existing `ProtectKernelModules=yes` makes `/usr/lib/modules` inaccessible.
`posture` therefore reads installed kernels from `/boot` first and falls back to
`/lib/modules`; on distributions whose kernels live only under `/usr/lib/modules`
(Arch) kernel-drift detection is unavailable under the shipped unit. Dropping
that one hardening line is the alternative (decision D-5). `RestrictAddressFamilies`
already includes `AF_UNIX` (D-Bus, chrony socket), `AF_INET`/`AF_INET6` (chrony
UDP) and `AF_NETLINK`; no change is needed.

For the DaemonSet, the extra hostPath mounts are listed per module (`/dev`,
`/run/dbus`, `/var/log`, `/etc/letsencrypt`, `/boot`); `privileged: true`
already satisfies device access and `CAP_SYS_RAWIO`.

## 4. Resource budget

All modules together stay within `AGENTS.md` §2.2 (under 2% of one core, under
100 MB RSS, under 1 MB/min disk, under 50 KB/s network) on the reference host.
Targets per module, to be verified by the Phase 13 load harness extension:

| Module | Steady-state pattern | Worst-case bound |
|---|---|---|
| `certificates` | stat-cache every 5 m, re-parse only changed files | 2,000 certificates, 1 MiB per file |
| `sensors` | about 100 file reads every 30 s | 256 chips, 128 channels each |
| `hardware` | sysfs walk every 10 m (USB every 60 s) | 512 PCI, 256 USB devices |
| `posture` | about 15 small file reads every 5 m | n/a |
| `storage` | sysfs/procfs every 60 s; SMART every 30 m, serial, one device at a time | 512 disks |
| `systemd` | D-Bus signals plus a 60 s reconcile | 2,000 reported units |
| `kernel` | one non-blocking read loop; regex rules on warning-level lines | rate-capped per class |
| `timesync` | one local query every 30 s | n/a |
| `auth` | utmp poll 15 s; btmp incremental read | 1,000 sessions |

## 5. Rollout and testing

- **Rollout:** backend validators first; modules default to disabled; enable one
  module at a time on a canary host; `agent once --modules <name>` is the
  acceptance tool for every phase.
- **Fixtures:** `testdata/sysfs/` gains hwmon, thermal, powercap, md, btrfs,
  pci, usb, node and dmi trees; `testdata/hostfs/` gains os-release variants,
  `boot/`, `etc/selinux`, `var/lib/update-notifier`, `run/reboot-required`;
  `testdata/dbus/` holds recorded property sets for the fake bus;
  `testdata/smart/` holds recorded ATA/NVMe/SCSI responses served by an
  injected ioctl function; `testdata/kmsg/` holds raw record streams;
  utmp/btmp fixtures and certificates are generated in the tests.
- **Fakes, never the host:** every kernel or bus boundary sits behind a small
  interface (as `network` does with its netlink `backend`) so tests never touch
  real devices, the real bus or real clocks.
- **Common tests per module:** snapshot completeness and no-removal on
  incomplete scans; baseline suppression of lifecycle events; schema validation
  of every emitted event; omitted-not-zero for each metric; `once` output;
  health transitions (`not applicable`, `unavailable`, `degraded`).

## 6. Phases

Ordered low risk first: file-only modules validate the contract and rollout
before the protocol- and ioctl-heavy ones. Effort is relative: S small, M
medium, L large.

| Phase | Scope | Effort | Definition of Done |
|---|---|---|---|
| **13 - Contract and framework** | §2 in full: schemas, IDs, reasons, units, `ModuleHealth.Details`, `NotApplicable`, `ExitHints`, `modbase`, config skeletons, `Build` order, example config | M | Golden ID tests; sample events of every new kind validate; mock server accepts them; heartbeat shows details and ignores not-applicable modules; all existing tests unchanged |
| **14 - Certificates** | `certificates.md` | S | `agent once --modules certificates` lists test certificates with `certificate.expires_in`; private keys never appear in output or logs (test with a combined PEM) |
| **15 - Sensors** | `sensors.md` | S | hwmon, thermal and powercap fixtures produce chips and metrics with the new units; stuck-chip isolation test; not-applicable on a host without sensors |
| **16 - Hardware** | `hardware.md` + `network_interface attached_to pci_device` | M | DMI, PCI, USB, NUMA, hugepages, kernel modules, taint from fixtures; USB hotplug lifecycle; serials absent by default |
| **17 - Posture** | `posture.md` | M | Debian, Ubuntu, RHEL and Arch fixtures give the documented attributes; unknown values omitted; kernel drift correct for each version format |
| **18 - Storage A** | disk inventory, mdraid, btrfs, ZFS state, hwmon disk temperature | M | md degrade/recover and btrfs error-stat fixtures produce lifecycle events; `disk` IDs stable across device renames; no SMART yet |
| **19 - systemd + `dbusro`** | `systemd.md` | M | Fake-bus tests for inventory, signals, flapping, reconnect; mutating D-Bus calls refused; failed unit gives `unit_failed` within 2 s |
| **20 - Kernel events** | `kernel.md` + process exit hints | M | kmsg fixture streams give class counters; an OOM line turns the matching `process_exited` into `process_oom_killed`; overrun handled |
| **21 - Time sync** | `timesync.md` | M | Kernel-only, chrony (recorded responses) and timesyncd (fake bus) paths; sync lost/acquired lifecycle with hysteresis |
| **22 - Auth** | `auth.md` | M | utmp and logind sessions; `btmp` incremental counting across rotation; no usernames in any event |
| **23 - Storage B (SMART)** | SMART/NVMe/SCSI health over ioctl | L | Recorded-response tests per protocol; opcode allowlist test; standby disks are not woken; SMART lifecycle events |
| **24 - Storage C (device-mapper)** | LVM volume groups, thin-pool and cache status | L | Fake DM backend tests; ioctl allowlist test; thin-pool utilization metrics |

Backlog after this plan (root README phases 25-27): network health, local
service probes, container log tailing.

Phases 13-24 are independent of the Kubernetes phases 7-12; only the shared
framework items (`ModuleHealth.Details`) must land once, in whichever phase
comes first.

## 7. Decisions taken in this plan

- **D-1 No new event type.** Discrete occurrences are expressed as lifecycle
  events on entities that have a state (unit, disk, array, pool, time sync,
  process) and as counters otherwise. If the backend later wants free-standing
  kernel/security events, the alternative is one additional P0 event type
  `agent.host.event.v1`; it needs its own schema and spool class and is
  deliberately not part of this plan.
- **D-2 Serial numbers are off by default** (`collect_serial_numbers: false`);
  IDs use a hash so identity stays stable without disclosing the serial.
- **D-3 No `sd-journal`.** The `kernel` module reads `/dev/kmsg` and
  `/proc/vmstat`; service failures come from systemd D-Bus. A pure-Go journal
  file reader is a possible later addition behind an interface and needs its own
  design (journal file format, compact/keyed hash, rotation) and a fuzz suite.
- **D-4 Pending updates are best effort.** Only distro-provided files are used.
  Missing means omitted, not zero.
- **D-5 `ProtectKernelModules=yes` stays.** `posture` reads `/boot` first.
  Revisit if kernel-drift coverage on Arch-like systems matters.
- **D-6 New modules are disabled by default** until the backend accepts the new
  kinds.
- **D-7 One module per signal group**, not one mega-module, so heartbeat
  health and enablement are per group.
- **D-8 Not-applicable modules do not degrade the agent**; existing modules
  keep today's behavior.
- **D-9 Failed-login usernames and remote hosts are never stored as metric
  attributes**; only counts and a distinct-source count are reported.
