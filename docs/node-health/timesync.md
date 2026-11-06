# Module `timesync` (Phase 21)

Part of the [node-health plan](README.md). Effort: M.

## 1. Summary and non-goals

Whether the clock is synchronized, how far off it may be, and from whom. Layered
so that something useful exists on every host: the kernel's own clock state, then
daemon detail from chrony or systemd-timesyncd.

Non-goals: `ntpd` and other daemons (a read-only NTP mode-6 client is a possible
later addition), configuring or stepping the clock, PTP/hardware clocks, leap
second handling beyond reporting the state.

## 2. Contract

Entity kind `time_sync`, ID `timesync:{host_id}`, relationship `runs_on host`.

Attributes: `service` (`chrony|systemd-timesyncd|none|unknown`), `ntp_enabled`
(from `timedate1`, when available), `reference_id` (ASCII name or IP),
`leap_status` (`normal|insert|delete|unsynchronised`), `sources` (at most 16
`{address, stratum, state}` when `collect_sources`).

Lifecycle (kind `time_sync`): `up` when synchronized and the kernel maximum
error is at most `sync_max_error`, `down` otherwise; reasons
`time_sync_acquired` / `time_sync_lost`. A state flips only after `flip_samples`
consecutive samples (hysteresis). Baseline suppression applies.

Metrics (entity `time_sync`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `time_sync.offset` | gauge | `s` | chrony tracking: estimated local clock minus reference, **positive when the local clock is ahead**. The sign convention is verified against `chronyc tracking` output during implementation and documented in `DECISIONS.md` |
| `time_sync.root_delay` | gauge | `s` | chrony tracking, timesyncd `NTPMessage` |
| `time_sync.root_dispersion` | gauge | `s` | same |
| `time_sync.stratum` | gauge | `count` | chrony, timesyncd |
| `time_sync.last_update_age` | gauge | `s` | chrony: now minus reference time of the last measurement |
| `time_sync.sources.total` / `time_sync.sources.reachable` | gauge | `count` | chrony sources |
| `time_sync.kernel.maxerror` | gauge | `s` | `adjtimex` `maxerror` |
| `time_sync.kernel.esterror` | gauge | `s` | `adjtimex` `esterror` |
| `time_sync.rtc_offset` | gauge | `s` | `timedate1` `RTCTimeUSec` minus `TimeUSec` (RTC in local time is skipped) |

Metrics whose source is not present are omitted. Frequency error is not
reported (there is no suitable unit).

Snapshot: `module: timesync`, scope `host`, `covers_kinds: [time_sync]`.

## 3. Design

Layers, all optional except the first:

1. **Kernel (`adjtimex`).** `unix.Adjtimex` with `Modes = 0`, which only reads.
   `synchronized = status & STA_UNSYNC == 0`; `maxerror` and `esterror` are
   reported in seconds. This works for any NTP daemon. A test asserts the
   `Modes` constant is zero.
2. **chrony.** A native client of chrony's command protocol over the Unix socket
   `{hostfs_root}/run/chrony/chronyd.sock`, then UDP `127.0.0.1:323`. The client
   type exposes only: tracking, number of sources, source data, activity. There
   is no generic request sender, and a test asserts the request codes it can
   encode. Protocol details (packet header, padding of requests to the response
   size, the custom floating-point encoding, command codes) are taken from the
   `candm.h`/`util.c` of the supported chrony versions during implementation and
   pinned with recorded responses; this document intentionally does not restate
   numeric constants. Responses with an unknown protocol version are ignored and
   the module falls back to layer 1.
3. **systemd-timesyncd and `timedate1`** through `dbusro`: `org.freedesktop.timesync1`
   properties (`ServerName`, `NTPMessage`, `Frequency`, `PollIntervalUSec`) and
   `org.freedesktop.timedate1` (`NTP`, `NTPSynchronized`, `CanNTP`, `TimeUSec`,
   `RTCTimeUSec`, `LocalRTC`). The allowlist needs only `Properties.Get/GetAll`.

Detection (`source: auto`): chrony socket answers, else timesyncd owns its bus
name, else `service: none`. A host with no daemon is a finding, not a failure:
the entity says `service: none` and the kernel layer still reports.

Tasks: `inventory_interval` (5 m) for attributes and sources;
`metrics_interval` (30 s) for metrics and the lifecycle decision.

## 4. Configuration

```yaml
modules:
  timesync:
    enabled: false
    source: auto                   # auto | chrony | timesyncd | kernel
    inventory_interval: 5m
    metrics_interval: 30s
    chrony_socket: ""              # default {hostfs_root}/run/chrony/chronyd.sock
    chrony_address: "127.0.0.1:323"
    collect_sources: true
    sync_max_error: 1s
    flip_samples: 2
```

## 5. Health and failure modes

- `adjtimex` denied (a restrictive seccomp profile in a container, or
  `ProtectClock=yes`): `degraded`, reason `adjtimex_denied`; daemon layers still
  work. The node-health README §3 lists `ProtectClock=` as forbidden in the unit.
- Daemon unreachable although configured with an explicit `source`: `degraded`.
- No daemon in `auto`: healthy, `service: none`.
- Heartbeat details: `{service, kernel_synchronized, sources}`.

## 6. Security

Only read requests exist in the chrony client and only `Get` on D-Bus.
Source addresses are infrastructure information (NTP servers), not user data.

## 7. Testing

Recorded chrony tracking, sources and activity responses (several versions),
including truncated and wrong-version packets; fake bus for timesyncd and
`timedate1`; `adjtimex` through an injected function (synchronized,
unsynchronized, growing `maxerror`, `STA_NANO` offsets); hysteresis; sign
convention; no-daemon host.

## 8. Phase 21 Definition of Done

The three paths produce schema-valid output from fixtures; lifecycle only flips
after the configured number of samples; the request-code and `Modes` tests pass.

## 9. Risks and limitations

chrony's protocol is versioned and binary; hence recorded fixtures per version
and a clean fallback. `ntpd` hosts get kernel-level state only. Containers
usually cannot call `adjtimex` without privileges.
