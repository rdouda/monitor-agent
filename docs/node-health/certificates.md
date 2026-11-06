# Module `certificates` (Phase 14)

Part of the [node-health plan](README.md). Effort: S.

## 1. Summary and non-goals

Reports TLS certificates found in local files and how long they remain valid.
Private keys are never read into the event pipeline.

Non-goals: probing remote endpoints or listening sockets (no port scanning,
`AGENTS.md` §10), revocation checks (OCSP/CRL), chain trust validation against
the system pool, PKCS#12 or Java keystores (need passwords), Kubernetes
secrets, the public CA trust store (excluded by default).

## 2. Contract

Entity kind `certificate`, ID `cert:{host_id}:{sha256_fingerprint}` (hex SHA-256
of the DER). The same certificate in several files is **one** entity.

Attributes (omit when unavailable):

| Field | Note |
|---|---|
| `subject`, `subject_cn` | RFC 2253 string (at most 256 characters), common name |
| `issuer`, `issuer_cn` | same |
| `serial_number` | hex |
| `not_before`, `not_after` | timestamps |
| `san_dns` | at most 32 names |
| `san_ip` | at most 16 addresses |
| `key_algorithm` | for example `RSA-2048`, `ECDSA-P256`, `Ed25519` |
| `signature_algorithm` | |
| `is_ca`, `self_signed` | `self_signed`: subject equals issuer and the signature verifies with its own key |
| `chain_position` | `0` for the first certificate in a file, increasing for the rest |
| `paths` | at most 8 file paths (relative to `hostfs_root`) |

Entity `name`: subject CN, else first SAN, else the first 12 characters of the
fingerprint.

Relationships: `certificate runs_on host`.

Lifecycle: `created` / `removed` with reasons `entity_created` /
`entity_removed`. A renewed certificate has a new fingerprint, so a renewal is
seen as one `removed` and one `created`.

Metrics (entity `certificate`):

| Metric | Kind | Unit | Source |
|---|---|---|---|
| `certificate.expires_in` | gauge | `s` | `not_after − now`, **negative** once expired; computed from the cache at every metrics tick |

Snapshot: `module: certificates`, scope `host`, `covers_kinds: [certificate]`.

## 3. Design

- **Scan set:** `paths` (files or directories) under `hostfs_root`. Directories
  are walked to `max_depth`. Files match `include_globs` and not
  `exclude_globs`, and are at most `max_file_bytes`.
- **Default `paths`:** `/etc/letsencrypt/live`, `/etc/ssl`, `/etc/pki/tls`.
  Other common locations (`/etc/nginx`, `/etc/apache2`, `/etc/httpd`,
  `/etc/haproxy`, `/etc/postfix`) are opt-in.
- **Default exclusions:** public trust stores (`/etc/ssl/certs`,
  `/usr/share/ca-certificates`, `/etc/pki/ca-trust`, `/etc/pki/tls/certs/ca-bundle*`,
  `/var/lib/ca-certificates`) unless `include_trust_store: true`, and any
  directory named `private`.
- **Parsing:** read the file (bounded), `pem.Decode` in a loop. Only blocks of
  type `CERTIFICATE` are parsed with `x509.ParseCertificate`. Every other block
  type, notably anything containing `PRIVATE KEY`, is dropped immediately: the
  bytes are not copied, stored, logged, or counted by type beyond a total
  `skipped_blocks`. Files with a `.der`/`.cer` extension are parsed as DER only
  when the content is not PEM.
- **Cache:** per path keyed by `(inode, size, mtime)`; unchanged files are not
  re-read. The metrics tick recomputes `expires_in` from cached `not_after`.
- **Dedup:** entities by fingerprint; symlinks followed with realpath dedupe and
  loop protection (`follow_symlinks`).
- **Removal:** a path that disappears removes the entity only when no other
  path still provides the fingerprint and the scan was complete.

## 4. Configuration

```yaml
modules:
  certificates:
    enabled: false
    paths: ["/etc/letsencrypt/live", "/etc/ssl", "/etc/pki/tls"]
    exclude_paths: []              # added to the built-in trust-store exclusions
    include_trust_store: false
    include_globs: ["*.pem", "*.crt", "*.cer"]
    exclude_globs: ["*.key", "*key*.pem"]
    max_depth: 4
    max_file_bytes: 1048576
    max_certificates: 2000
    follow_symlinks: true
    scan_interval: 5m
    metrics_interval: 5m
```

Validation: intervals at least 1 s; globs compile; `max_*` positive. Restart
required: `enabled`. Hot reload: everything else.

## 5. Health and failure modes

- `healthy`: all configured paths scanned.
- A configured path that does not exist is **not an error** (default paths are
  speculative). A path that exists but cannot be read, or an unparsable file
  that matched the globs, marks the snapshot incomplete (`permission_denied` /
  `parse_error`) and the module `degraded`.
- Hitting `max_certificates` gives `limit_exceeded` and an incomplete snapshot.
- Heartbeat details: `{certificates, paths_scanned, files_skipped, expiring_30d}`.

## 6. Security

Read-only, `O_RDONLY`. The key rule above is covered by tests: a combined
cert-plus-key PEM must yield one certificate and no key bytes in events, logs or
heap-visible strings (the test searches output for the key's base64 and the
`PRIVATE KEY` marker). Paths are reported relative to `hostfs_root`. In the
DaemonSet, `/etc/letsencrypt` and any other scanned directory must be mounted.

## 7. Testing

Certificates are generated in the test (`crypto/x509`): leaf plus chain,
expired, not yet valid, self-signed, ECDSA and Ed25519, many SANs, duplicate in
two files, symlink loop, oversize file, garbage file, combined cert/key PEM,
DER file. Fuzz the PEM loop. Verify `expires_in` sign at the boundary and
incomplete-snapshot behavior.

## 8. Phase 14 Definition of Done

`agent once --modules certificates` over a fixture tree prints the expected
entities, one `certificate.expires_in` per certificate, and one snapshot;
renewal produces `removed` plus `created`; the key-leak test passes.

## 9. Risks and limitations

Certificates held only in memory, in databases, in Kubernetes secrets or behind
application-specific paths are not seen. `expires_in` is the validity of the
file, not proof that a service has loaded it.
