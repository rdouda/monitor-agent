# Module `auth` (Phase 22)

Part of the [node-health plan](README.md). Effort: M.

## 1. Summary and non-goals

Who is logged in now, and how many failed logins happen. SELinux/AppArmor mode is
in `posture`, not here.

Non-goals: collecting **usernames or passwords of failed attempts**, full login
history (`wtmp`, `lastlog`), `sudo`/PAM audit trails, auditd records, shipping any
auth log text, geolocation or reputation of source addresses.

## 2. Contract

### `login_session` (ID `session:{host_id}:{session_key}`)

`session_key` is `{boot_id8}-{logind session id}` for logind and
`{boot_id8}-{line}-{pid}-{login_epoch}` for utmp, so keys cannot collide after a
reboot.

Attributes: `source` (`logind|utmp`), `user` (only with `collect_usernames`),
`uid`, `session_type` (`tty|x11|wayland|unspecified`), `class` (`user|greeter|lock-screen|background`),
`service` (`sshd|login|gdm|...`), `tty`, `remote` (bool), `remote_host` (only
with `collect_remote_hosts`), `started_at`, `leader_pid`, `state`
(`active|online|closing`).

Relationships: `login_session runs_on host`; `login_session owned_by process`
for the leader PID when the process module is enabled and the process
resolves (same ID construction as `network` uses for socket owners).

Lifecycle: `created` / `removed`, reasons `entity_created` / `entity_removed`,
after the baseline.

### Metrics (entity `host`)

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `host.auth.sessions` | gauge | `count` | current sessions; attribute `remote` = `true|false` |
| `host.auth.logins` | counter | `count` | sessions observed opening since the agent started |
| `host.auth.failed_logins` | counter | `count` | new `btmp` records (or auth-log failure lines) since the agent started; attribute `kind` = `ssh|console|other` |
| `host.auth.failed_login_sources` | gauge | `count` | number of **distinct** remote hosts among failures in the last `failed_login_window`; no address is emitted |

Snapshot: `module: auth`, scope `host`, `covers_kinds: [login_session]`.

## 3. Design

- **Sessions.** Prefer logind: `login1.Manager.ListSessions` plus `Properties.GetAll`
  per session through `dbusro`, with `SessionNew`/`SessionRemoved` signals for
  prompt lifecycle. Fall back to `{hostfs_root}/run/utmp`: fixed-size records
  (the layout of glibc's `struct utmp` for the platform; size and field offsets
  are taken from the C header at implementation time and covered by a generated
  fixture), keeping `USER_PROCESS` records whose PID is alive in `/proc`.
  `session_source: auto` picks logind when systemd and the bus are available.
- **Failed logins (`btmp`).** Open `{hostfs_root}/var/log/btmp` read-only, seek
  to the current end at start (baseline: only failures after the agent started
  are counted), then read new whole records on each tick. A shrink or an inode
  change (rotation) reopens from the start. Per record only `ut_line` class
  (`ssh:notty` becomes `ssh`, `tty*` becomes `console`) and the remote host are
  looked at. **`ut_user` is never read into a retained variable, logged or
  emitted**: users routinely type passwords into the username field. The remote
  host goes only into an in-memory set used for the distinct count.
- **Auth log fallback** (`auth_log_path`, default off; `auto` tries
  `/var/log/auth.log` then `/var/log/secure` when `btmp` is absent): follow with
  offset and inode handling and count lines that match a fixed set of sshd
  failure patterns using **non-capturing** regular expressions, so usernames and
  addresses are not extracted. Counted into the same metric with `kind: ssh`.
- **No source available** (modern minimal systems without `btmp` and without an
  auth log; journal-only): the failed-login metrics are omitted and health
  details say `failed_login_source: none`. Closing this gap needs a journal
  reader and is outside this plan (decision D-3 in the README).
- Tasks: sessions every `sessions_interval` (15 s) plus signals; failure counting
  and metrics every `metrics_interval` (15 s). `once`: sessions and gauges only.

## 4. Configuration

```yaml
modules:
  auth:
    enabled: false
    session_source: auto           # auto | logind | utmp
    sessions_interval: 15s
    metrics_interval: 15s
    collect_usernames: true
    collect_remote_hosts: true
    collect_failed_logins: true
    btmp_path: ""                  # default {hostfs_root}/var/log/btmp
    auth_log_path: ""              # "" off; "auto" tries auth.log then secure
    failed_login_window: 5m
    max_sessions: 1000
```

Usernames and remote hosts can be personal data in some jurisdictions; both have
off switches. Restart required: `enabled`, path keys. Hot reload: the rest.

## 5. Health and failure modes

- No sessions source (no logind, no utmp): `NotApplicable` `unavailable`,
  reason `no_session_source`.
- `btmp` or auth log unreadable although present: `degraded`, reason
  `failed_login_source_unreadable`; sessions keep working.
- `max_sessions` exceeded: `limit_exceeded`, incomplete snapshot.
- Heartbeat details: `{session_source, failed_login_source, sessions}`.

## 6. Security

Read-only. `wtmp` and `lastlog` are never opened. Record parsing validates sizes
and caps the number of records per tick. No username from failed attempts, no
remote address and no raw log line is ever stored in an attribute, a metric
attribute, an event, or an info-level log.

## 7. Testing

Generated utmp and btmp fixtures (sessions of several types, dead records,
brute-force bursts, truncation, rotation, oversize file), a fake bus for logind,
auth-log samples in several formats. Tests: no username or address in any emitted
byte (scan output for the fixture's usernames and addresses), distinct-source
counting, rotation, baseline suppression, logind vs utmp parity, privacy
switches.

## 8. Phase 22 Definition of Done

Sessions from logind and utmp fixtures produce entities and lifecycle events;
`btmp` increments are counted across rotation; the privacy scan passes; the
module is not-applicable (not degraded) on a host without any source.

## 9. Risks and limitations

`btmp` and `utmp` presence varies by distribution and version (some no longer
write `btmp`); failed SSH logins are then invisible without a journal reader.
Session lifecycle through utmp can lag by `sessions_interval`. Logind session IDs
reset on reboot (hence the boot prefix in the key).
