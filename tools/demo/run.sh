#!/usr/bin/env bash
set -euo pipefail

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
source "$repo/tools/integration/self_hosting_common.sh"
source "$repo/tools/demo/common.sh"
submit=0
case ${1:-} in
  '') ;;
  --submit) submit=1 ;;
  -h|--help) echo "usage: $0 [--submit]"; echo 'Start an isolated interactive ForgeCI environment; --submit also captures the canonical proof.'; exit 0 ;;
  *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
esac
[[ ! -e "$demo_state_file" ]] || { echo 'ForgeCI demo state already exists; run ./tools/demo/status.sh or ./tools/demo/stop.sh' >&2; exit 1; }

FORGECI_SELF_REPO=$repo
FORGECI_SELF_ROOT=$(mktemp -d /tmp/forgeci-demo.XXXXXX)
FORGECI_SELF_BIN_DIR="$repo/build"
FORGECI_SELF_REUSE_BINARIES=1
FORGECI_SELF_DETACH=1
persisted=0
cleanup_on_failure() {
  status=$?
  if [[ "$persisted" -ne 1 ]]; then
    if [[ "$status" -ne 0 ]]; then forgeci_self_diagnostics; fi
    forgeci_self_stop
    rm -rf "$FORGECI_SELF_ROOT"
    rm -f "$demo_state_file"
    rmdir "$demo_state_dir" 2>/dev/null || true
  fi
  if [[ "$status" -ne 0 ]]; then
    echo 'Demo startup failed; resources created by this attempt were cleaned up.' >&2
  fi
  exit "$status"
}
trap cleanup_on_failure EXIT INT TERM

echo '[1/4] Checking prerequisites'
forgeci_self_require
command -v jq >/dev/null 2>&1 || { echo 'missing required command: jq' >&2; exit 1; }
command -v setsid >/dev/null 2>&1 || { echo 'missing required command: setsid' >&2; exit 1; }
echo '[2/4] Preparing committed source and binaries'
forgeci_self_prepare
echo '[3/4] Starting PostgreSQL, server, and two runners'
forgeci_self_start
echo '[4/4] Recording restricted demo state'
mkdir -p "$demo_state_dir"; chmod 700 "$demo_state_dir"
umask 077
for variable in FORGECI_SELF_REPO FORGECI_SELF_ROOT FORGECI_SELF_BIN_DIR FORGECI_SELF_SERVER FORGECI_SELF_RUNNER_ENDPOINT FORGECI_SELF_PG FORGECI_SELF_TOKEN_FILE FORGECI_SELF_COMMIT FORGECI_SELF_DB_PORT FORGECI_SELF_SERVER_PID FORGECI_SELF_RUNNER_A_PID FORGECI_SELF_RUNNER_B_PID; do
  printf '%s=%s\n' "$variable" "${!variable}" >>"$demo_state_file"
done
chmod 600 "$demo_state_file"; persisted=1

printf '\nForgeCI demo environment is running\n\nServer\n  %s\n\n' "$FORGECI_SELF_SERVER"
"$FORGECI_SELF_BIN_DIR/forge" runners --server "$FORGECI_SELF_SERVER"
printf '\nCommands\n  ./tools/demo/status.sh\n  RUN_ID=$(./build/forge submit --quiet --server %s --file forge.yaml --jobs 4)\n  ./build/forge inspect "$RUN_ID" --server %s\n  ./tools/demo/capture.sh\n  ./tools/demo/stop.sh\n\n' "$FORGECI_SELF_SERVER" "$FORGECI_SELF_SERVER"
printf 'Logs\n  %s/server/server.log\n  %s/runner-a/runner.log\n  %s/runner-b/runner.log\n' "$FORGECI_SELF_ROOT" "$FORGECI_SELF_ROOT" "$FORGECI_SELF_ROOT"
echo 'The environment remains active until ./tools/demo/stop.sh is run.'
if [[ "$submit" -eq 1 ]]; then "$repo/tools/demo/capture.sh"; fi
