# Module `hardware` (Phase 16)

Part of the [node-health plan](README.md). Effort: M.

## 1. Summary and non-goals

Static and slow-changing hardware facts: DMI/firmware identity, PCI and USB
devices, NUMA topology, hugepages, transparent hugepages, kernel modules, kernel
taint, Secure Boot. **Block devices are not here**: model, serial and size of
disks are reported by the `storage` module so exactly one module owns the `disk`
entity. Non-goals: SMBIOS memory-module (DIMM) detail (raw `dmi/entries`
parsing, possible later), PCI name databases, GPU detail, firmware updates.

## 2. Contract

Kinds `hardware`, `pci_device`, `usb_device`, `numa_node`.

### `hardware` (ID `hardware:{host_id}`, one per host)

| Attribute | Source |
|---|---|
| `dmi` object: `sys_vendor`, `product_name`, `product_version`, `product_family`, `bios_vendor`, `bios_version`, `bios_date`, `board_vendor`, `board_name`, `board_version`, `chassis_type` | `{sysfs_root}/class/dmi/id/*` |
| `dmi.product_serial`, `dmi.board_serial`, `dmi.chassis_serial` | same, **only if `collect_serial_numbers`** |
| `cpu_microcode` | `microcode` field of `/proc/cpuinfo` (x86), omitted elsewhere |
| `efi` (bool), `secure_boot` (bool) | `{sysfs_root}/firmware/efi`, `efivars/SecureBoot-*` (value byte); omitted without EFI |
| `thp` object `{enabled, defrag}` | bracketed selection in `kernel/mm/transparent_hugepage/{enabled,defrag}` |
| `hugepage_sizes_bytes` | directories `kernel/mm/hugepages/hugepages-<N>kB` |
| `numa_nodes` | count of `devices/system/node/node*` |
| `kernel_tainted` (int), `kernel_taint_flags` (list of letters) | `/proc/sys/kernel/tainted`, decoded per the kernel's `tainted-kernels` documentation |
| `kernel_modules` | names from `/proc/modules`, sorted, at most 1,024 (`collect_kernel_modules`) |

`product_uuid` is **never** reported (it feeds the clone-detection fingerprint).
Absent DMI (ARM, some VMs) means those fields are omitted.

### `pci_device` (ID `pci:{host_id}:{address}`, address like `0000:00:1f.2`)

Attributes: `address`, `vendor_id`, `device_id`, `subsystem_vendor_id`,
`subsystem_device_id`, `class_code`, `class_name` (from the base class byte via
a small embedded table), `revision`, `driver` (basename of the `driver` link),
`iommu_group`, `numa_node` (omitted for `-1`), `link` object
`{speed, width, max_speed, max_width}` plus `link_degraded` (bool, current below
maximum), optional `sriov_total_vfs`/`sriov_num_vfs`. Vendor and device **names**
are not embedded; ids are reported and the backend resolves them
(`resolve_names: true` reads `{hostfs_root}/usr/share/hwdata/pci.ids` if present).
PCI bridges (base class `0x06`) are skipped unless `include_pci_bridges`.

### `usb_device` (ID `usb:{host_id}:{port_path}`, for example `1-2.3`)

From `{sysfs_root}/bus/usb/devices/*` entries without a `:` (those are
interfaces): `busnum`, `devnum`, `port_path`, `vendor_id`, `product_id`,
`manufacturer`, `product`, `speed_mbps`, `usb_version`, `device_class`,
`max_power_ma`, `driver`, `removable`, and `serial` only with
`collect_serial_numbers`. Root hubs and hubs with vendor `1d6b` are skipped
unless `include_usb_hubs`.

### `numa_node` (ID `numa:{host_id}:{node_id}`)

Emitted only when the host has at least 2 nodes. Attributes: `node_id`,
`cpulist`, `cpu_count`, `memory_total_bytes`, `distance` (list).

### Relationships and lifecycle

`hardware`, `pci_device`, `usb_device`, `numa_node` `runs_on host`;
`pci_device attached_to numa_node`. Separately (Phase 16 change in the
`network` module) `network_interface attached_to pci_device` when
`modules.hardware.enabled`: the network module already checks
`/sys/class/net/<if>/device`.

Lifecycle: `created`/`removed` for `usb_device` and `pci_device` after the
baseline (hotplug), reasons `entity_created`/`entity_removed`.

### Metrics

| Entity | Metric | Kind | Unit | Source |
|---|---|---|---|---|
| `hardware` | `hardware.hugepages.total` | gauge | `count` | `nr_hugepages`, attribute `page_size_bytes` |
| `hardware` | `hardware.hugepages.free` | gauge | `count` | `free_hugepages` |
| `hardware` | `hardware.hugepages.reserved` | gauge | `count` | `resv_hugepages` |
| `hardware` | `hardware.hugepages.surplus` | gauge | `count` | `surplus_hugepages` |
| `numa_node` | `numa_node.memory.used` | gauge | `By` | `node*/meminfo` (`MemTotal − MemFree`) |
| `numa_node` | `numa_node.memory.free` | gauge | `By` | `MemFree` |
| `numa_node` | `numa_node.memory.accesses` | counter | `count` | `node*/numastat` (pages); attribute `result` = `hit|miss|foreign|interleave_hit|local|other` |

Snapshots: `module: hardware`, scope `host`,
`covers_kinds: [hardware, pci_device, usb_device, numa_node]`.

## 3. Design

- Tasks: `inventory_interval` (10 m) for DMI, PCI, NUMA, modules and taint;
  `usb_interval` (60 s) for USB; `metrics_interval` (30 s) for hugepages and
  NUMA metrics.
- All reads are sysfs/procfs; each file read is size-bounded; unreadable files
  omit the field. `link_degraded` is computed only when both speeds parse.
- USB hotplug is detected by polling at `usb_interval`. A later optimization
  may subscribe to kernel uevents (`AF_NETLINK`, read-only); polling stays the
  reconciliation source.
- Limits: `max_pci_devices`, `max_usb_devices`; exceeding gives `limit_exceeded`
  and an incomplete snapshot.

## 4. Configuration

```yaml
modules:
  hardware:
    enabled: false
    inventory_interval: 10m
    usb_interval: 60s
    metrics_interval: 30s
    collect_serial_numbers: false
    collect_kernel_modules: true
    include_pci_bridges: false
    include_usb_hubs: false
    resolve_names: false
    max_pci_devices: 512
    max_usb_devices: 256
```

## 5. Health and failure modes

Sysfs always exists on Linux, so the module is not `unavailable`. A missing DMI
or EFI tree only omits fields. A directory that exists but cannot be read marks
the snapshot incomplete. Heartbeat details: `{pci, usb, numa_nodes, modules}`.

## 6. Security

Read-only. Serial numbers and `product_uuid` handling as above. Kernel module
names are not sensitive but are capped.

## 7. Testing

Fixtures: Intel and AMD DMI, an ARM board without DMI, a VM, PCI with a
degraded link and SR-IOV, USB with hubs and interfaces, two-node NUMA with
`numastat`, hugepages of two sizes, `tainted` with several bits, EFI with and
without Secure Boot. Tests: serial absent by default, `product_uuid` never
present, hotplug lifecycle, ID percent-encoding of PCI addresses.

## 8. Phase 16 Definition of Done

All fixtures produce the documented entities and metrics, schema-valid;
`network_interface attached_to pci_device` appears only when the hardware module
is enabled; USB hotplug produces `created`/`removed` within one `usb_interval`.

## 9. Risks and limitations

`class_name` from a small table is coarse. PCI device names are not resolved by
default. Taint flags and module lists change on module load and cause entity
upserts at the inventory interval only.
