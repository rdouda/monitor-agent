#!/usr/bin/env bash
# Run the agent locally without root, writing state to a temporary directory
# and printing CloudEvents to stdout. Usage: ./scripts/dev.sh [once|run]
set -euo pipefail

mode="${1:-once}"
case "$mode" in
  once|run) ;;
  *)
    echo "usage: $0 [once|run] [--sample-wait DURATION] [--modules LIST]" >&2
    exit 2
    ;;
esac
shift || true

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
keep_state=0
if [[ -n "${AGENT_DEV_STATE:-}" ]]; then
  state_dir="$AGENT_DEV_STATE"
  mkdir -p "$state_dir"
  state_dir="$(cd "$state_dir" && pwd)"
  keep_state=1
else
  state_dir="$(mktemp -d "${TMPDIR:-/tmp}/agent-dev-state.XXXXXX")"
fi
config_file="$(mktemp "${TMPDIR:-/tmp}/agent-dev-config.XXXXXX.yaml")"

cleanup() {
  if [[ "$keep_state" -eq 0 ]]; then
    rm -rf "$state_dir"
  fi
  rm -f "$config_file"
}
trap cleanup EXIT

cat >"$config_file" <<CONFIG
agent:
  environment: development
  tags:
    team: local
paths:
  state_dir: $state_dir
  procfs_root: /proc
  sysfs_root: /sys
  hostfs_root: /
server:
  base_url: "https://ingest.example.com"
reporting:
  sink: stdout
logging:
  level: info
  format: json
  file: ""
CONFIG

cd "$root"
echo "state: $state_dir" >&2
echo "config: $config_file" >&2
go run ./cmd/agent "$mode" --config "$config_file" "$@"
