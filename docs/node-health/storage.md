# Module `storage` (Phases 18, 23, 24)

Part of the [node-health plan](README.md). Effort: M for phase 18, L for phases
23 and 24.

## 1. Summary and non-goals

Physical disk inventory and health, software RAID, btrfs, ZFS pool state, LVM,
and disk temperature. Filesystem usage and per-device I/O stay in the `host`
module (`host.disk.*`).

Non-goals: hardware RAID controllers (MegaRAID, SmartArray: vendor ioctls),
starting or scheduling SMART self-tests, enabling SMART on a disk, ZFS capacity
and vdev error counters (the Linux ABI is `libzfs`/`/dev/zfs`, not stable),
partition tables, filesystems' internal health beyond what sysfs exposes.

The phases:

| Phase | Content |
|---|---|
| 18 | `disk` inventory (sysfs), mdraid, btrfs, ZFS pool state, disk temperature from hwmon |
| 23 | SMART / NVMe / SCSI health over read-only ioctls |
| 24 | device-mapper: LVM volume groups, thin-pool and cache status |

## 2. Contract

### 2.1 `disk`

ID `disk:{host_id}:{disk_key}` where `disk_key` is the first 16 hex characters
of `sha256(identity)` and `identity` is chosen in this order:

1. `wwid` (`/sys/block/<dev>/wwid` or `device/wwid`; for ATA disks it embeds
   model and serial),
2. a transport serial (`device/serial`, virtio `serial`),
3. `kernel:` plus the kernel name (unstable fallback; the attribute
   `id_stable` is then `false`).

The hash keeps the serial out of the ID. Replacing a disk creates a new entity
and, after a complete snapshot, removes the old one.

Which block devices: whole devices under `{sysfs_root}/block` that have a
`device` link, minus `exclude_devices` (default
`^(loop|ram|fd|sr|zram|dm-|md|nbd|zd|rbd)\d*`). Partitions are not entities.

Attributes: `device_name` (for example `sda`; not stable), `device_path`,
`model`, `vendor`, `firmware`, `size_bytes`, `logical_block_size`,
`physical_block_size`, `rotational`, `media_type` (`hdd|ssd|nvme|unknown`),
`transport` (`sata|sas|nvme|usb|virtio|scsi|unknown`, from the sysfs path),
`removable`, `read_only`, `id_source` (`wwid|serial|name`), `id_stable`;
`serial` and `wwid` **only with `collect_serial_numbers`**; and (phase 23)
`smart` object `{supported, enabled, protocol: ata|nvme|scsi, overall:
passed|failed|unknown, power_state?}`.

Lifecycle: `up`/`down` with reasons `disk_health_ok` / `disk_health_failing`
when `smart.overall` changes (phase 23); `created`/`removed` for hot-plug.
Baseline suppression applies.

Metrics (entity `disk`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `disk.temperature` | gauge | `Cel` | hwmon `drivetemp`/`nvme` chip matched to the disk (phase 18, `temperature_interval`), else SMART/NVMe log (phase 23) |
| `disk.power_on_time` | counter | `s` | ATA attribute 9 / NVMe `power_on_hours`; only for vendor formats known to be hours |
| `disk.power_cycles` | counter | `count` | ATA 12 / NVMe |
| `disk.reallocated_sectors` | gauge | `count` | ATA 5 raw |
| `disk.pending_sectors` | gauge | `count` | ATA 197 raw |
| `disk.uncorrectable_sectors` | gauge | `count` | ATA 198 raw |
| `disk.crc_errors` | counter | `count` | ATA 199 raw |
| `disk.smart.error_log_count` | gauge | `count` | ATA SMART error log summary |
| `disk.endurance.used` | gauge | `ratio` | NVMe percentage used (clamped to 1); ATA SSD wear attributes from a table of known IDs (for example 177, 231, 233): `(100 − normalized)/100` |
| `disk.written.bytes` / `disk.read.bytes` | counter | `By` | NVMe data units × 512,000; ATA total-LBAs attributes only where the unit is known |
| `disk.nvme.media_errors` | counter | `count` | NVMe log |
| `disk.nvme.unsafe_shutdowns` | counter | `count` | NVMe log |
| `disk.nvme.available_spare` | gauge | `ratio` | NVMe log |
| `disk.nvme.critical_warning` | gauge | `count` | NVMe critical-warning bitmask |

A metric whose vendor encoding is not known for that model is **omitted**.

### 2.2 `raid_array` (mdraid)

ID `raid:{host_id}:{array_key}`; `array_key` is the md UUID found by reverse
lookup of `{hostfs_root}/dev/disk/by-id/md-uuid-*`, else `name:{mdX}` (then
`id_stable: false`).

Attributes: `name`, `level`, `array_state`, `raid_disks`, `metadata_version`,
`chunk_size_bytes`, `size_bytes`, `degraded` (missing member count),
`sync_action`, `bitmap` (bool), `members` (at most 64) `[{device, slot, state}]`
with `state` from `md/dev-*/state` (`in_sync|faulty|spare|writemostly|...`).
Sources: `/proc/mdstat` for existence, `{sysfs_root}/block/md*/md/*` for values.

Relationships: `raid_array runs_on host`; `raid_array contains disk` for members
resolved from `md/dev-*/block` through the partition-to-disk walk (§3.4).

Lifecycle: `up` when `degraded == 0` and not inactive, `down` otherwise
(meaning "not fully redundant or not available"); reasons `raid_degraded` /
`raid_recovered`; `details: {failed_members, sync_action}`.

Metrics: `raid.devices.total` (`count`), `raid.devices.degraded` (`count`),
`raid.devices.failed` (`count`), `raid.sync.progress` (`ratio`, only while a sync
is running), `raid.mismatch.count` (`count`), `raid.member.errors` (counter,
`count`, attribute `member`).

### 2.3 `storage_pool`

ID `pool:{host_id}:{pool_type}:{pool_key}`.

| `pool_type` | `pool_key` | Sources |
|---|---|---|
| `btrfs` | filesystem UUID | `{sysfs_root}/fs/btrfs/<uuid>/` |
| `zfs` | pool name | `/proc/spl/kstat/zfs/<pool>/{state,io}` |
| `lvm_vg` (phase 24) | VG UUID (first 32 characters after `LVM-` in any LV's `dm/uuid`) | `block/dm-*/dm/{name,uuid}` plus device-mapper status |

Attributes: `pool_type`, `name`/`label`, `state` (zfs: `ONLINE|DEGRADED|FAULTED|OFFLINE|UNAVAIL|REMOVED|SUSPENDED`),
`devices` count, btrfs `exclusive_operation` when present.

Relationships: `storage_pool runs_on host`; `storage_pool contains disk` or
`raid_array` for btrfs devices (`devices/<dev>` links) and LVM physical volumes
(slave walk), resolved through §3.4.

Lifecycle: `up`/`down` with `pool_healthy`/`pool_degraded` (zfs state not
`ONLINE`; btrfs when `devinfo/*/missing` is set or error counters increased
since the previous scan; LVM when a PV or thin pool reports failure).

Metrics (entity `storage_pool`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `storage_pool.bytes.total` / `storage_pool.bytes.used` | gauge | `By` | btrfs `allocation/{data,metadata,system}/{total_bytes,bytes_used}`; attribute `class` |
| `storage_pool.devices.missing` | gauge | `count` | btrfs `devinfo/*/missing` |
| `storage_pool.device.errors` | counter | `count` | btrfs `devinfo/<id>/error_stats` (kernel 5.14+); attributes `device`, `kind` = `write|read|flush|corruption|generation` |
| `storage_pool.io.read.bytes` / `.write.bytes` | counter | `By` | zfs `io` (`nread`, `nwritten`) |
| `storage_pool.thin.data.utilization` / `storage_pool.thin.metadata.utilization` | gauge | `ratio` | phase 24, device-mapper thin-pool status; attribute `thin_pool` |

Snapshot: `module: storage`, scope `host`,
`covers_kinds: [disk, raid_array, storage_pool]`.

## 3. Design

### 3.1 Phase 18 (sysfs and procfs only)

Tasks: `inventory_interval` (5 m) for disks, arrays and pools;
`health_interval` (60 s) re-reads md states, btrfs counters and ZFS state and
emits the cheap metrics and lifecycle; `temperature_interval` (60 s) reads the
hwmon chip that belongs to each disk (match `hwmonN/device` to the block
device's sysfs path). Absent subsystems (no md, no btrfs, no ZFS) produce
nothing and no error.

### 3.2 Phase 23: SMART and NVMe over ioctl

- Pure Go, `CGO_ENABLED=0`: `golang.org/x/sys/unix` ioctls with locally defined
  structures. ioctl numbers and structure layouts must be taken from the kernel
  headers (`scsi/sg.h`, `linux/nvme_ioctl.h`) at implementation time; this
  document intentionally does not list numeric values.
- **ATA** through the SCSI generic layer (`SG_IO` with an ATA pass-through
  CDB) on `{hostfs_root}/dev/<name>`. **NVMe** through the NVMe admin ioctl on
  the controller or namespace device. **SCSI/SAS**: `INQUIRY` and `LOG SENSE`
  (temperature and informational-exceptions pages) only.
- **Opcode allowlist (default deny):** ATA: identify, SMART read data, SMART read
  thresholds, SMART return status, SMART read log, check power mode. NVMe:
  identify and get-log-page. SCSI: `INQUIRY`, `LOG SENSE`. The command builder
  cannot construct anything else, and a test enumerates dangerous opcodes
  (`SMART ENABLE/DISABLE OPERATIONS`, self-test start, format, firmware,
  sanitize, set features, writes) and asserts they are refused.
- **Never wake a sleeping disk:** before SMART commands to an ATA disk, send
  check-power-mode; if the disk is in standby or sleep and `skip_standby` is
  true, skip it and keep the previous values.
- **One device at a time**, per-command timeout (`smart_timeout`), whole task
  under a timeout; SMART runs every `smart_interval` (30 m).
- USB bridges and virtual disks: unsupported devices set
  `smart.supported: false` quietly (debug log), never an error.
- **Vendor attribute tables:** raw-value decoding (power-on time units, wear
  attributes, total-LBAs units, temperature packing) is table-driven in
  `testdata/smart/attributes.yaml`; unknown models fall back to omitting the
  metric.
- SMART overall result changes trigger a partial upsert of that `disk` and the
  lifecycle event; metrics are emitted on the SMART tick.

### 3.3 Phase 24: device-mapper

A small `internal/dmctl` package opens `{hostfs_root}/dev/mapper/control` and
supports only version, list devices, device status, table status and table
deps, behind an interface with a fake. It enables: LVM LV naming from
`dm/name` (VG and LV separated by a single `-`, with `--` as the escaped
dash), VG UUID from `dm/uuid`, thin-pool data/metadata usage and failure flags,
and cache and dm-raid health characters. Without it LVM capacity is not
available: sysfs has no extent data and the sum of LV sizes would overcount thin
volumes. Phase 24 is optional; phase 18 already inventories dm devices as
pool members when present, but reports no `lvm_vg` pool without it.

### 3.4 Partition-to-disk and stacking walk

`parentDisk(name)`: if `block/<name>/partition` exists, the parent is the
basename of the parent directory of the real path. For stacked devices (dm-crypt,
LVM, md) walk `slaves/` recursively up to depth 4 and collect the set of `disk`
entities. Relationships are emitted only to disks that were inventoried in the
same scan.

## 4. Configuration

```yaml
modules:
  storage:
    enabled: false
    inventory_interval: 5m
    health_interval: 60s
    temperature_interval: 60s
    collect_disks: true
    collect_raid: true
    collect_btrfs: true
    collect_zfs: true
    collect_lvm: true              # effective from phase 24
    collect_serial_numbers: false
    exclude_devices: ['^(loop|ram|fd|sr|zram|dm-|md|nbd|zd|rbd)\d*']
    max_disks: 512
    smart:                         # effective from phase 23
      enabled: true
      interval: 30m
      timeout: 5s
      skip_standby: true
```

## 5. Health and failure modes

- No block devices in sysfs (some containers): `NotApplicable` `unavailable`,
  reason `no_block_devices`.
- Unreadable `md`, `btrfs` or `spl` directory that exists: incomplete snapshot
  and `degraded`.
- `/dev` node missing for SMART (the agent cannot create nodes): that disk is
  skipped with a counted reason; the module stays healthy.
- Heartbeat details: `{disks, smart_supported, smart_failing, md_arrays, pools}`.

## 6. Security

Read-only boundaries and allowlists are in the node-health README §3. Serial
numbers and WWIDs only with `collect_serial_numbers`; IDs always hashed.
`SG_IO` and NVMe admin ioctls need `CAP_SYS_RAWIO`; the agent runs as root, and
the DaemonSet is privileged. Do not set `PrivateDevices=`/`DeviceAllow=` in the
unit.

## 7. Testing

Fixtures: SATA, NVMe, virtio, USB disk, renamed devices (ID stable via wwid),
md arrays healthy/degraded/rebuilding/with spare, btrfs with and without
`error_stats`, ZFS state files, dm stacks. SMART: recorded ATA, NVMe and SCSI
responses served by an injected ioctl function, including quirky vendor
attributes, standby disk, SMART disabled, and a failing-prediction disk. Tests:
opcode allowlist, serial redaction, standby never woken, one-device-at-a-time,
incomplete-snapshot semantics.

## 8. Definitions of Done

- **Phase 18:** md degrade/recover and btrfs error-stat fixtures give lifecycle
  events; `disk` IDs are stable across device renames; hwmon temperatures map to
  the correct disk; schema-valid `once` output; no SMART commands issued.
- **Phase 23:** recorded-response tests per protocol pass; opcode allowlist test
  passes; standby disks are skipped; SMART lifecycle events work.
- **Phase 24:** fake-DM tests pass; ioctl allowlist test passes; thin-pool
  utilization metrics and failure flags are emitted.

## 9. Risks and limitations

SMART semantics are vendor-specific and quirky; omitted metrics are preferred
over wrong ones. USB bridges and RAID controllers hide disks. ZFS reports state
and pool I/O only. LVM capacity needs phase 24. Disk temperature without hwmon
is only as fresh as `smart.interval`.
