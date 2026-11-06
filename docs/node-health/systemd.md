# Module `systemd` (Phase 19)

Part of the [node-health plan](README.md). Effort: M. This phase also builds
`internal/dbusro`, used later by `timesync` and `auth`.

## 1. Summary and non-goals

State of systemd services and timers over D-Bus: failed units, restart counts,
services that should be running but are not, timers that did not fire. Never
`systemctl`.

Non-goals: starting, stopping, restarting, reloading, masking or resetting
units; unit file contents; journal text; per-service cgroup accounting
(memory/CPU, which depends on accounting being enabled); user (`--user`)
instances; non-systemd init systems.

## 2. `internal/dbusro`: read-only D-Bus

- Library: `github.com/godbus/dbus/v5` (pure Go). Added to the baseline
  technology table by this plan.
- Connects to `{hostfs_root}/run/dbus/system_bus_socket` (EXTERNAL
  authentication as root). Not `/run/systemd/private`.
- API shape: `Get(dest, path, iface, prop)`, `GetAll(dest, path, iface)`,
  `Call(dest, path, iface, method, args...)` for allowlisted methods only,
  `Subscribe(matchRule)` for signals, and `Close`. A small `Conn` interface
  with a fake implementation serves the tests.
- **Allowlist, default deny**, keyed by `interface.method`:
  `org.freedesktop.DBus.Properties.Get/GetAll`;
  `org.freedesktop.systemd1.Manager.ListUnits`, `ListUnitFiles`, `GetUnit`,
  `Subscribe`, `Unsubscribe`; `org.freedesktop.login1.Manager.ListSessions`
  (used by `auth`); signal match rules limited to `PropertiesChanged`,
  `UnitNew`, `UnitRemoved`, `SessionNew`, `SessionRemoved`. `LoadUnit` is
  excluded on purpose (it loads a unit into systemd). `Properties.Set` and every
  start/stop/restart/reload/kill/reset/`SetUnitProperties`/`Reload`/`Reexecute`/
  power method is refused.
- Test: a table of mutating method names (including those above) must be
  refused, and an unlisted method must be refused.

## 3. Contract

### `systemd_manager` (ID `systemd:{host_id}`)

Attributes: `version`, `system_state` (`initializing|starting|running|degraded|maintenance|stopping`),
`virtualization`. Relationship `runs_on host`. No lifecycle.

Metrics (entity `systemd_manager`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `systemd_manager.units` | gauge | `count` | `ListUnits`, counted over **all** loaded units; attribute `active_state` = `active|inactive|failed|activating|deactivating|reloading` |
| `systemd_manager.jobs.queued` | gauge | `count` | `NJobs` property |

### `systemd_unit` (ID `unit:{host_id}:{unit_name}`)

Attributes: `unit_type` (`service|timer|socket|...`), `description`,
`load_state`, `active_state`, `sub_state`, `unit_file_state`, `main_pid`,
`exec_main_status`, `result`, `restart_policy`, `control_group` (the cgroup path;
the join key to `process.cgroup`), `active_enter_at`, `inactive_enter_at`. For
timers: `triggers` (the unit it activates), `next_elapse_at`, `last_trigger_at`.

State attributes are point-in-time values, like `container.state`.

Relationships: `systemd_unit managed_by systemd_manager`. Processes are joined
to units by `process.cgroup == systemd_unit.control_group`; no new edge is
added to the `process` module in this plan.

Metrics (entity `systemd_unit`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `systemd.service.restarts` | gauge | `count` | service `NRestarts` (reset by a manual stop, hence a gauge) |
| `systemd.service.uptime` | gauge | `s` | now minus `ActiveEnterTimestampMonotonic`, active services only |
| `systemd.timer.overdue` | gauge | `s` | `max(0, now − next_elapse)` for active timers with a scheduled elapse (realtime against wall clock, monotonic against `CLOCK_MONOTONIC`) |
| `systemd.timer.last_trigger_age` | gauge | `s` | now minus `LastTriggerUSec`, when set |

"Timer drift" is therefore observable as `overdue` (a timer that should have
fired and has not) and `last_trigger_age`. The per-trigger delay is not
reported; it would need event history.

### Lifecycle (kind `systemd_unit`)

| `active_state` | `sub_state` | status | reason |
|---|---|---|---|
| `active` (from anything else) | any | `running` | `unit_started` |
| `inactive` (from active) | any | `exited` | `unit_stopped` |
| `failed` | any | `dead` | `unit_failed` |
| `activating` | `auto-restart` | `restarting` | `unit_restarting` |
| `activating`, `deactivating`, `reloading` | other | no event (transient) | |
| unit disappears from the scan | | `removed` | `entity_removed` |
| unit appears after the baseline | | `created` | `entity_created` |

`details`: `active_state`, `sub_state`, `result`, `exec_main_status`.
Baseline suppression and the rate limit apply. **Flap control:** at most one
lifecycle event per unit per `min_event_interval`; the latest state wins and the
suppressed count is added to `details.suppressed`.

Snapshot: `module: systemd`, scope `systemd_manager`,
`covers_kinds: [systemd_unit]`.

## 4. Design

- **Availability:** `{hostfs_root}/run/systemd/system` must exist (systemd is
  PID 1, the same check as `sd_booted`), and the bus socket must be reachable.
- **Reported set:** only `unit_types` (default `service`, `timer`). A unit is
  reported when it is active or in a transitional or failed state, **or** is
  inactive with `unit_file_state` of `enabled`/`enabled-runtime` (should be
  running) and is not a finished one-shot (`Type=oneshot` with
  `Result=success`). `inactive_policy` can widen (`all`) or narrow (`none`). The
  aggregate `systemd_manager.units` always counts every loaded unit.
- **Inventory** (`inventory_interval`, 60 s): `ListUnits`, `ListUnitFiles` for
  the enabled state, then `Properties.GetAll` for the reported units only.
- **Signals:** after `Subscribe`, `PropertiesChanged` for tracked unit paths,
  `UnitNew` and `UnitRemoved` trigger a per-unit refresh after
  `event_debounce` (500 ms) and a lifecycle diff. The periodic inventory is the
  reconciliation source; a failed connection reconnects with backoff and
  reconciles immediately.
- **Limits:** `max_units` reported units; exceeding it gives `limit_exceeded`
  and an incomplete snapshot.

## 5. Configuration

```yaml
modules:
  systemd:
    enabled: false
    bus_socket: ""                 # default {hostfs_root}/run/dbus/system_bus_socket
    inventory_interval: 60s
    metrics_interval: 30s
    unit_types: ["service", "timer"]
    inactive_policy: enabled_only  # enabled_only | all | none
    include_units: []              # globs
    exclude_units: []
    collect_events: true
    event_debounce: 500ms
    min_event_interval: 2s
    max_units: 2000
```

Restart required: `enabled`, `bus_socket`. Hot reload: the rest.

## 6. Health and failure modes

- Not systemd: `NotApplicable` `unavailable`, reason `not_systemd`.
- Socket missing or connection refused (typical in containers without the
  mount): plain `unavailable`, reason `dbus_unreachable`, re-probed every 60 s.
- Signal subscription failing: `degraded` (`signals_unavailable`); polling
  continues.
- Heartbeat details: `{systemd_version, units_loaded, units_reported, bus_connected}`.

## 7. Security

Allowlisted, read-only bus access as in §2. No unit file contents, environment
or command lines are read (`Environment`, `ExecStart` properties are not
requested).

## 8. Testing

Fake bus with recorded `ListUnits`/`GetAll` data (`testdata/dbus/*.json`):
healthy host, failed unit, crash loop, masked unit, templated and escaped unit
names, one-shot finished, timers (realtime and monotonic, overdue),
`PropertiesChanged` bursts (flap control), reconnect, limit hit, baseline
suppression, allowlist refusal.

## 9. Phase 19 Definition of Done

Fake-bus tests above pass; `agent once --modules systemd` is schema-valid; a
failed unit yields `unit_failed` within 2 s with signals on; every mutating
D-Bus method is refused by `dbusro`.

## 10. Risks and limitations

`NRestarts` needs systemd 235 or newer (omitted otherwise). Without per-service
accounting there is no per-service memory or CPU. Large hosts with thousands of
units depend on the reported-set filter. In containers the bus socket must be
mounted (`/run/dbus`).
