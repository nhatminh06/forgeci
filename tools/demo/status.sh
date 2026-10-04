#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == -h || ${1:-} == --help ]]; then echo "usage: $0"; echo 'Report real health for the active ForgeCI demo environment.'; exit 0; fi
[[ $# -eq 0 ]] || { echo "usage: $0" >&2; exit 2; }
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
source "$repo/tools/demo/common.sh"
demo_load_state
pg_status=STOPPED; server_status=STOPPED; runner_a=OFFLINE; runner_b=OFFLINE
if [[ $(docker inspect -f '{{ index .Config.Labels "forgeci.self-hosting.root" }} {{ .State.Running }}' "$FORGECI_SELF_PG" 2>/dev/null || true) == "$FORGECI_SELF_ROOT true" ]] &&
  [[ $(docker exec "$FORGECI_SELF_PG" psql -U postgres -d forgeci -At -c 'SELECT 1' 2>/dev/null || true) == 1 ]]; then pg_status=READY; fi
if demo_process_matches "$FORGECI_SELF_SERVER_PID" forge-server && curl -fsS "$FORGECI_SELF_SERVER/healthz" >/dev/null 2>&1; then server_status=READY; fi
if [[ "$server_status" == READY ]]; then
  runners=$($FORGECI_SELF_BIN_DIR/forge runners --server "$FORGECI_SELF_SERVER" 2>/dev/null || true)
  awk '$1 == "runner-a" && $2 == "ONLINE" {found=1} END {exit !found}' <<<"$runners" && runner_a=ONLINE || true
  awk '$1 == "runner-b" && $2 == "ONLINE" {found=1} END {exit !found}' <<<"$runners" && runner_b=ONLINE || true
fi
printf 'ForgeCI demo environment\n\n%-16s %s\n%-16s %s\n%-16s %s\n%-16s %s\n\nServer\n%s\n' \
  PostgreSQL "$pg_status" forge-server "$server_status" runner-a "$runner_a" runner-b "$runner_b" "$FORGECI_SELF_SERVER"
[[ "$pg_status $server_status $runner_a $runner_b" == 'READY READY ONLINE ONLINE' ]]
