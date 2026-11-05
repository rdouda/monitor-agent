# agent

A lightweight, outbound-only Linux monitoring agent. It discovers the host,
processes, Docker/Podman runtimes (containers, images, networks, volumes,
pods), and network interfaces/routes/listening sockets, then reports inventory,
relationships, lifecycle events, and metrics to an Ingestion API as
CloudEvents JSON over HTTPS.

The normative wire format and design are in [`AGENTS.md`](AGENTS.md). Choices
made where the specification leaves room are recorded in
[`docs/DECISIONS.md`](docs/DECISIONS.md).

## Status

All six build phases from `AGENTS.md` §14 are implemented:

| Phase | Status |
|---|---|
| 0 — Foundations (IDs, config, events, schemas, mock server) | done |
| 1 — Core + host module + `agent once` | done |
| 2 — Spool, batching, retry, enrollment, heartbeat, systemd unit | done |
| 3 — Process module and lifecycle scan | done |
| 4 — Docker/Podman discovery, containers, images, networks, volumes, pods, events, veth mapping | done |
| 5 — Network interfaces, routes, listening sockets, container namespaces, netlink change detection | done |
| 6 — Hardening, SIGHUP reload, fuzz/race tests, README | done |

Known limitations are listed at the end of `docs/DECISIONS.md`.

## Requirements

- Linux kernel ≥ 4.15 (cgroup v1 or v2), `amd64` or `arm64`
- Go 1.22+ to build
- Root to run the service (full `/proc`, runtime sockets, network namespaces)

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o agent ./cmd/agent
CGO_ENABLED=0 go build -trimpath -o mockserver ./cmd/mockserver
```

## Quick start

`examples/agent.yaml` is the production configuration: it writes identity and
spool state under `/var/lib/agent`, reports over HTTPS, and therefore normally
runs as root. `agent once` itself is read-only: it forces the stdout sink, uses
an ephemeral identity when no state exists, and does not touch the `seq` file.
For local testing, use the development script, which creates a temporary state
directory and prints CloudEvents to stdout:

```sh
./scripts/dev.sh once      # one full scan, then exit
./scripts/dev.sh run       # run until Ctrl-C
```

`once` prints only CloudEvents on stdout (logs go to stderr), waits two seconds
by default so CPU-rate metrics are populated, and never writes identity or
`seq` state. Its `agentseq` starts at 1 every time, so it cannot collide with a
running service. Useful variants:

```sh
./scripts/dev.sh once --modules host,process --sample-wait 500ms
./scripts/dev.sh once --modules host,process,containers,network
```

The script discovers rootful and rootless Podman/Docker sockets under the
current user's environment. To exercise persistent identity, start the service
once (it creates state under the chosen directory), then run `once` against the
same state:

```sh
AGENT_DEV_STATE=$PWD/.agent-state ./scripts/dev.sh run       # Ctrl-C after startup
AGENT_DEV_STATE=$PWD/.agent-state ./scripts/dev.sh once
```

If you do want to run the production example as root, build a binary first
(`sudo go` fails because sudo's `PATH` usually does not include Go):

```sh
CGO_ENABLED=0 go build -o /tmp/agent ./cmd/agent
sudo /tmp/agent once --config examples/agent.yaml
# or: sudo env "PATH=$PATH" go run ./cmd/agent once --config examples/agent.yaml
```

For a production-style run, point `paths.state_dir` and the spool at writable
directories, place the ingestion URL and enrollment token, and run:

```sh
sudo install -m 0755 agent /usr/local/bin/agent
sudo install -d -m 0700 /var/lib/agent
sudo install -m 0600 enrollment-token /etc/agent/enrollment-token
sudo install -m 0644 examples/agent.yaml /etc/agent/agent.yaml
sudo install -m 0644 deployments/systemd/agent.service /etc/systemd/system/agent.service
sudo systemctl daemon-reload
sudo systemctl enable --now agent
```

`SIGHUP` re-reads `/etc/agent/agent.yaml`. Interval, filter, label, limit, tag,
and log-level changes are applied by rebuilding the collector manager while the
spool and delivery pipeline stay up. Changes to `paths`, `server`, module
enablement, runtime endpoints, reporting/spool parameters, or the logging
format/file are rejected with a warning and require a restart.

## Commands

| Command | Purpose |
|---|---|
| `agent run --config PATH` | Run the service |
| `agent validate-config --config PATH` | Validate configuration and exit (exit 2 on error) |
| `agent enroll --config PATH` | Enroll once and exit |
| `agent once --config PATH [--sample-wait 2s] [--modules host,process]` | Run one read-only scan and print events as JSON lines |
| `agent version` | Print version, commit, Go version |

`reporting.sink: stdout` is for development; it emits one CloudEvent per line.
`reporting.sink: http` requires `https://` and a credential (or an enrollment
token) owned by root with mode `0600`.

## Mock ingestion API

`cmd/mockserver` validates every event against the embedded `schemas/v1`,
deduplicates by event ID and `(agentid, agentseq)`, supports gzip, optional
bearer authentication and enrollment, and can inject failures:

```sh
go run ./cmd/mockserver -addr 127.0.0.1:8099
curl -s http://127.0.0.1:8099/admin/stats | jq
```

See `go run ./cmd/mockserver -h` for flags and `/admin/inject` for fault
injection.

## Project layout

```
cmd/agent/                 CLI
cmd/mockserver/            mock ingestion API
internal/core/             manager, supervisor, scheduler, container index
internal/identity/         persistent agent/host identity, sequence file
internal/ids/              every entity/relationship ID builder
internal/config/           YAML loading and validation
internal/events/           CloudEvents envelope and data builders
internal/modules/host/     required host inventory and metrics
internal/modules/process/  process inventory, lifecycle, metrics
internal/modules/containers/
    model/                 runtime-neutral model
    transport/             read-only HTTP-over-unix client
    docker/                Docker adapter
    podman/                Podman adapter
    *.go                   discovery, inventory, metrics, events, veth
internal/modules/network/  interfaces, routes, sockets, netns, metrics
internal/queue/            segmented disk spool and cursors
internal/reporting/        HTTP client, sender, retry, sinks
internal/schema/           JSON Schema loader and validator
schemas/v1/                normative v1 schemas
deployments/systemd/       systemd unit
examples/agent.yaml        full default configuration
testdata/                  procfs/sysfs/hostfs fixtures
```

## Security properties

- Outbound only: the agent never listens on a socket.
- Runtime API clients only issue `GET`/`HEAD`; the transport rejects every
  other method, and a unit test enforces this.
- `/proc/[pid]/environ` is never read. Container environment, command, and
  entrypoint are never collected. Command lines are opt-in and redacted.
- Labels are filtered through the configured allowlist.
- Credentials are never logged and must be root-owned mode `0600`.
- TLS verification is always on; `http://` ingestion URLs are rejected unless
  the stdout sink is used.

## Testing

```sh
go test ./...
go test -race ./...
go vet ./...
```

The container adapters are tested against fake unix-socket HTTP servers that
serve recorded Docker/Podman JSON. The network module uses an injectable
netlink backend plus procfs fixtures, so tests never depend on the host's
interfaces. `agent once` is exercised end-to-end from `cmd/agent` and every
emitted event is validated against `schemas/v1`.
