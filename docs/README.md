# Plans

`AGENTS.md` is the **baseline design** of the agent (phases 0-6). It is not
edited for every new feature. Work that extends the agent beyond phase 6 is
specified here, one plan per area. A plan is a *design*, not an
implementation: nothing in `docs/plans/` is built until its phases are picked
up in order.

## How plans relate to the baseline

1. A plan **extends** `AGENTS.md`; it never contradicts it silently. Where a
   plan changes a baseline rule, it says so in a section named
   *Deviations from the baseline* and the change is recorded in
   `docs/DECISIONS.md` when implemented.
2. Names introduced by a plan (entity kinds, ID prefixes, metric names,
   lifecycle reasons, config keys) are allowed once the plan is approved.
   Names that are in no plan and not in `AGENTS.md` still must not be invented
   (`AGENTS.md` §0.2).
3. The files in `schemas/v1/` are what is enforced on the wire. Appendix A of
   `AGENTS.md` is the baseline snapshot and is **not** updated for extensions.
   Every plan lists its schema diffs explicitly.
4. All baseline rules still apply to every plan: read-only, outbound-only, no
   shelling out, `CGO_ENABLED=0`, injectable roots, omitted-not-zero,
   snapshot semantics, per-scan timeouts, bounded output (`AGENTS.md` §5.5, §10).

## Index

| Plan | Phases | Scope | Status |
|---|---|---|---|
| [`kubernetes.md`](kubernetes.md) | 7-12 | Node-local Kubernetes discovery, CRI adapter, DaemonSet packaging | designed |
| [`node-health/`](node-health/README.md) | 13-24 | systemd, kernel events, storage health, sensors, hardware, time sync, posture, logins, TLS certificate expiry | designed |

Backlog (no design yet): network health (conntrack, TCP errors, nftables,
WireGuard/IPsec, NFS, DNS), local service probes (nginx, HAProxy, Envoy, Redis,
libvirt), container log tailing. They are listed in the root `README.md` as
phases 25-27.

## Conventions every plan follows

Each plan (or each module document inside a plan) has these sections, in this
order, so an implementer knows where to look:

1. **Summary and non-goals**
2. **Contract**: entity kinds, ID formats, attributes, relationships,
   lifecycle, metrics, schema diffs (additive only; new kinds are rejected by an
   old backend, see *Rollout* below)
3. **Design**: data sources (exact paths, syscalls, protocols), tasks and
   intervals, algorithms, caching, change detection
4. **Configuration**: YAML block with defaults, validation, reload behavior
5. **Health and failure modes**: which conditions are `unavailable`,
   *not applicable*, `degraded`, or an incomplete snapshot
6. **Security**: how read-only is enforced in code, what is never collected
7. **Testing**: fixtures, fakes, required tests
8. **Phases and Definition of Done**
9. **Risks and limitations**

### Rollout rule (all plans)

New entity kinds, module names, lifecycle reasons and units are **additive
schema changes**. An ingestion backend that has not been updated rejects them.
Therefore: update the backend validators first, ship the agent second, and keep
every new module `enabled: false` by default until the backend is known to
accept it. Old agents are never affected.

### Naming rules

- Module name = config key = heartbeat key = `module` in `snapshot.completed`.
- Metric names are `<entity-ish prefix>.<noun>...`, lowercase, dot separated,
  unit never changes (`AGENTS.md` §7.4).
- An ID prefix must not equal any existing prefix and must not be a prefix of
  another (the schema pattern matches `^(a|b|c):`).
- IDs never contain secrets or raw hardware serials; where a stable hardware
  identity is needed it is hashed (see the storage design).

### Recording decisions

Every plan ends with the decisions it has taken. When a phase is implemented,
copy the decisions that turned out to matter, plus any deviations, into
`docs/DECISIONS.md` under a heading named after the module.
