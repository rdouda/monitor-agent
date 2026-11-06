# Module `posture` (Phase 17)

Part of the [node-health plan](README.md). Effort: M, with a documented partial
scope for pending updates.

## 1. Summary and non-goals

One entity describing the machine's patch and hardening state: reboot-required,
kernel drift, pending-update counts where the distribution provides them,
unattended-upgrade status, SELinux/AppArmor mode, kernel lockdown and CPU
vulnerability mitigations.

Non-goals: running `apt`, `dnf`, `zypper`, `needrestart` or any package manager
(no shelling out); computing update candidates from package indexes (deferred,
decision D-4); vulnerability scanning; firewall state; sysctl audits.

## 2. Contract

Entity kind `host_posture`, ID `posture:{host_id}`, relationship
`runs_on host`. No lifecycle events: mode changes appear as attribute changes
in the next upsert.

Attribute groups (omit a group, or a field, when it cannot be determined):

| Group | Fields | Sources |
|---|---|---|
| `kernel` | `running`, `latest_installed`, `drift` (bool, installed is newer than running) | `uname -r`; installed kernels from `{hostfs_root}/boot/vmlinuz-*`, BLS entries `boot/loader/entries/*.conf`, fallback `lib/modules/*` |
| `reboot` | `required` (bool), `required_since`, `packages` (at most 32) | Debian/Ubuntu: `var/run/reboot-required` (+ mtime) and `reboot-required.pkgs`; any distro: `kernel.drift` |
| `updates` | `source`, `pending`, `security`, `last_refresh` | see §3 |
| `unattended_upgrades` | `enabled`, `last_run_at`, `last_result` (`success|failed|unknown`) | see §3 |
| `selinux` | `status` (`enforcing|permissive|disabled|unavailable`), `configured_mode`, `policy`, `mismatch` | `{sysfs_root}/fs/selinux/enforce`, `etc/selinux/config` |
| `apparmor` | `enabled`, `profiles: {enforce, complain, other}` | `{sysfs_root}/module/apparmor/parameters/enabled`, `kernel/security/apparmor/profiles` |
| `lsm` | `active` (list), `lockdown` (`none|integrity|confidentiality`) | `{sysfs_root}/kernel/security/lsm`, `.../lockdown` |
| `cpu_vulnerabilities` | map of name to status (first 128 characters), `vulnerable_count` | `{sysfs_root}/devices/system/cpu/vulnerabilities/*` |

Metrics (entity `host_posture`):

| Metric | Kind | Unit | Note |
|---|---|---|---|
| `posture.updates.pending` | gauge | `count` | omitted when unknown |
| `posture.updates.security` | gauge | `count` | omitted when unknown |
| `posture.updates.refresh_age` | gauge | `s` | now minus last package-list refresh |
| `posture.reboot.required_age` | gauge | `s` | only while a reboot is required |

Snapshot: `module: posture`, scope `host`, `covers_kinds: [host_posture]`.

## 3. Design

### Kernel drift

Installed kernel versions come from `/boot` first (the shipped unit hides
`/usr/lib/modules`, see the node-health README §3), then `lib/modules`. Only
kernels of the **same flavor** as the running one are compared (flavor = the
suffix after the numeric version and build number, for example `-generic`,
`.el9_3.x86_64`, `-arch1-1`, `-0-lts`). Versions compare with a version-aware
sort (split into numeric and alphabetic runs, numeric runs compared as numbers),
like `sort -V`. `drift` is true only when the newest installed version is
strictly greater than the running one. Fixtures cover Debian, Ubuntu, RHEL,
Fedora, Arch and Alpine version strings.

### Reboot required

On Debian and Ubuntu (`ID`/`ID_LIKE` in `os-release`) the flag file is
authoritative: present means required, absent means not required (published as
`false`). On other distributions `required` is `true` only when kernel drift is
true; otherwise it is **omitted** (an absent signal is not "no").

### Updates

- Ubuntu: `var/lib/update-notifier/updates-available` gives the pending and
  security counts. The file is localized; the module takes the first integer of
  the first and the second line and omits both if either does not parse.
  `source: update-notifier`.
- Debian family: `last_refresh` is the mtime of
  `var/lib/apt/periodic/update-success-stamp` when present. No counts.
- Other distributions: `source: unavailable`; no counts. This is deliberate:
  without package-manager libraries a correct count is not obtainable, and a
  wrong count is worse than none.

### Unattended upgrades

Debian family: `enabled` from `etc/apt/apt.conf.d/20auto-upgrades`
(`APT::Periodic::Unattended-Upgrade`), `last_run_at` from the mtime of
`var/lib/apt/periodic/unattended-upgrades-stamp`, `last_result` from the last
run summary in `var/log/unattended-upgrades/unattended-upgrades.log`
(success and failure marker lines only; no package names are kept). RPM family:
`enabled` is the presence of a `dnf-automatic`/`dnf5-automatic`/`yum-cron` timer
link under `etc/systemd/system/timers.target.wants/`; no result.

### Mandatory access control

SELinux `status` is derived from `fs/selinux/enforce` (`1` enforcing, `0`
permissive) when selinuxfs is present, `disabled` when `etc/selinux/config`
says so or the filesystem is absent on an SELinux-enabled distribution, and
`unavailable` when it cannot be determined (for example selinuxfs not visible
in a container). `mismatch` compares runtime and configured modes. AppArmor
profile counts come from the `profiles` file ("name (mode)" lines).

Task cadence: one task at `metrics_interval` (5 m) re-reads the cheap files and
emits metrics; the entity is upserted at `inventory_interval` (10 m) and
immediately when an attribute changes.

## 4. Configuration

```yaml
modules:
  posture:
    enabled: false
    inventory_interval: 10m
    metrics_interval: 5m
    collect_updates: true
    collect_reboot: true
    collect_mac: true
    collect_cpu_vulnerabilities: true
```

## 5. Health and failure modes

Kernel information always exists, so the module is never unavailable. Partial
sources (for example no `/boot` readable) omit the affected group; an
unreadable file that should exist marks the snapshot incomplete. Heartbeat
details: `{distribution_family, updates_source, kernel_drift}`.

## 6. Security

Read-only; logs are read only for the unattended-upgrades result lines and
nothing from them except the success/failure verdict is retained.

## 7. Testing

`testdata/hostfs` variants per distribution with os-release, `/boot` contents,
reboot flag files, update-notifier text in English and German, stamp files,
SELinux enforcing/permissive/disabled and a mismatch, AppArmor profile list,
lockdown and vulnerability files. Version-compare table tests. Check that
unknown values are omitted.

## 8. Phase 17 Definition of Done

Each distribution fixture yields the documented groups; unknown values are
absent; `agent once --modules posture` is schema-valid; the kernel-drift table
test passes for all listed version formats.

## 9. Risks and limitations

Pending-update counts exist only on Ubuntu (and only when update-notifier is
installed). Arch-like systems with kernels only under `/usr/lib/modules` lose
kernel drift under the shipped hardened unit. Localized update-notifier text is
parsed defensively and omitted when ambiguous.
