#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == -h || ${1:-} == --help ]]; then echo "usage: $0"; echo 'Safely stop and remove only the active ForgeCI demo environment.'; exit 0; fi
[[ $# -eq 0 ]] || { echo "usage: $0" >&2; exit 2; }
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
source "$repo/tools/demo/common.sh"
demo_load_state
source "$repo/tools/integration/self_hosting_common.sh"
label=$(docker inspect -f '{{ index .Config.Labels "forgeci.self-hosting.root" }}' "$FORGECI_SELF_PG" 2>/dev/null || true)
if [[ -n "$label" && "$label" != "$FORGECI_SELF_ROOT" ]]; then echo 'refusing to remove a container not owned by this demo state' >&2; exit 1; fi
for pair in "$FORGECI_SELF_RUNNER_A_PID:forge-runner" "$FORGECI_SELF_RUNNER_B_PID:forge-runner" "$FORGECI_SELF_SERVER_PID:forge-server"; do
  pid=${pair%%:*}; executable=${pair#*:}
  if kill -0 "$pid" 2>/dev/null && ! demo_process_matches "$pid" "$executable"; then echo "refusing to signal unrelated process $pid" >&2; exit 1; fi
done
forgeci_self_stop
rm -rf "$FORGECI_SELF_ROOT"
rm "$demo_state_file"
rmdir "$demo_state_dir" 2>/dev/null || true
echo 'ForgeCI demo environment stopped and isolated runtime state removed.'
