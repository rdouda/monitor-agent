# Module `sensors` (Phase 15)

Part of the [node-health plan](README.md). Effort: S.

## 1. Summary and non-goals

Temperatures, fan speeds, voltages, currents, power and energy from sysfs.
Non-goals: IPMI/BMC sensors (need `/dev/ipmi*` protocol), GPU vendor sensors,
fan control, cooling-device state, writing any `pwm`/threshold file.

## 2. Contract

Entity kind `sensor_chip`, one entity per chip (not per channel), ID
`sensor:{host_id}:{chip_key}`:

| Source | `chip_key` |
|---|---|
| hwmon | `hwmon-{name}-{sha256(realpath(device) relative to sysfs_root)[:8]}`; the `hwmonN` index is **not** used because it changes between boots |
| thermal zone | `thermal-{type}-{N}` |
| powercap (RAPL) | `powercap-{domain path}` (for example `intel-rapl:0:0`) |
| power supply (opt-in) | `power_supply-{name}` |

Attributes: `source` (`hwmon|thermal_zone|powercap|power_supply`), `chip`
(driver name or zone type), `bus` (basename of the `device/subsystem` link, for
example `i2c`, `platform`, `pci`), `address` (PCI address when the device is
PCI), and `channels`: at most 128 items
`{id, type: temp|fan|voltage|current|power|energy|humidity, label?, min?, max?, crit?, lcrit?}`
with thresholds converted to SI units. Channels are slow-changing facts;
readings are metrics.

Relationships: `sensor_chip runs_on host`; `sensor_chip attached_to pci_device`
for PCI-backed chips **only when `modules.hardware.enabled`**.

Lifecycle: `created`/`removed` (hot-unplug of a USB/PCI sensor chip).

Metrics (entity `sensor_chip`; attributes `channel`, optional `label`):

| Metric | Kind | Unit | Source / conversion |
|---|---|---|---|
| `sensor.temperature` | gauge | `Cel` | `tempN_input` / 1000; thermal zone `temp` / 1000 |
| `sensor.fan.speed` | gauge | `rpm` | `fanN_input` |
| `sensor.voltage` | gauge | `V` | `inN_input` / 1000 |
| `sensor.current` | gauge | `A` | `currN_input` / 1000 |
| `sensor.power` | gauge | `W` | `powerN_input` (or `_average`) / 1e6 |
| `sensor.energy` | counter | `J` | `energyN_input` / 1e6; RAPL `energy_uj` / 1e6 |
| `sensor.humidity` | gauge | `ratio` | `humidityN_input` / 100000 |
| `sensor.alarm` | gauge | `count` | `0` or `1` from `*_alarm` files; extra attribute `alarm` = `general|min|max|lcrit|crit` |

Power *draw* for RAPL domains is the rate of `sensor.energy`; the backend derives
it. RAPL counters wrap at `max_energy_range_uj`; the module accumulates wraps
into a monotonic in-memory total (the backend handles the `agentrun` reset on
restart).

Snapshot: `module: sensors`, scope `host`, `covers_kinds: [sensor_chip]`.

## 3. Design

- **Discovery** (`inventory_interval`): enumerate `{sysfs_root}/class/hwmon/*`,
  `class/thermal/thermal_zone*`, `class/powercap/*` (and `class/power_supply/*`
  when enabled). Read `name`, channel files and labels. Thermal-zone trips of
  type `critical` become `crit`, `hot` becomes `max`; other trips are ignored.
- **Readings** (`metrics_interval`): read only discovered channels.
- **Validity:** read errors (`EIO`, `ENODATA`, `ENXIO`) omit the value.
  Temperatures outside `[-100, 250]` °C are omitted (boards report sentinel
  values for unconnected sensors). A fan reading of `0` is reported as the
  kernel states it.
- **Disk temperatures belong to `storage`.** Chips named `drivetemp` and `nvme`
  are excluded by default so a disk temperature is reported once
  (`disk.temperature`).
- **Stuck drivers:** each chip is read in its own goroutine with
  `chip_read_timeout`. A chip that times out is skipped for the next 5 ticks and
  reported in health details; the scan still finishes.
- Channel limit per chip and chip limit overall produce `limit_exceeded`.

## 4. Configuration

```yaml
modules:
  sensors:
    enabled: false
    inventory_interval: 5m
    metrics_interval: 30s
    exclude_chips: ["drivetemp", "nvme"]
    include_chips: []              # if non-empty, only these chip names
    collect_thermal_zones: true
    collect_powercap: true
    collect_power_supply: false
    chip_read_timeout: 2s
    max_chips: 256
    max_channels_per_chip: 128
```

## 5. Health and failure modes

- No hwmon, thermal or powercap directories (typical for VMs): `NotApplicable`
  `unavailable` with reason `no_sensors`; re-probed every 60 s; does not degrade
  the agent.
- Permission denied on a chip: that chip is skipped, snapshot incomplete
  (`permission_denied`), module `degraded`.
- Heartbeat details: `{chips, channels, stalled_chips}`.

## 6. Security

Read-only; never opens `pwm*`, `*_enable`, or `*_crit`/`*_max` files for
writing. A test lists the files the module opens and asserts all are read-only
and none is a control file.

## 7. Testing

`testdata/sysfs` trees for: `coretemp` with labels, `k10temp`, `nct6775`-style
fan and voltage chip, thermal zones with trips, RAPL with a wrapping counter,
an unconnected sensor with sentinel values, a chip whose `temp1_input` blocks
(FIFO) to exercise the timeout, a 70-channel chip (reading split), renamed
`hwmonN` indices (ID stays stable), and the disk-chip exclusion.

## 8. Phase 15 Definition of Done

`agent once --modules sensors` over the fixtures prints chips and metrics with
the new units, schema-valid; the stuck-chip test finishes within the scan
timeout; a host without sensors reports not-applicable and an unchanged agent
status.

## 9. Risks and limitations

Channel labels and scaling are driver-defined; some drivers report plausible but
wrong values. BMC-only servers expose little through sysfs. Reading some drivers
can be slow, hence the timeout and the 30 s default.
