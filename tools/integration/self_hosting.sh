#!/usr/bin/env bash
set -euo pipefail

repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
source "$repo/tools/integration/self_hosting_common.sh"
FORGECI_SELF_REPO=$repo
FORGECI_SELF_ROOT=$(mktemp -d /tmp/forgeci-self-hosting.XXXXXX)
FORGECI_SELF_BIN_DIR="$FORGECI_SELF_ROOT/bin"
keep=${FORGECI_DIAGNOSTICS_DIR:-}
passed=0

cleanup() {
  if [[ "$passed" -ne 1 ]]; then forgeci_self_diagnostics; fi
  if [[ -n "$keep" ]]; then
    mkdir -p "$keep"
    cp "$FORGECI_SELF_ROOT/server/server.log" "$FORGECI_SELF_ROOT/runner-a/runner.log" "$FORGECI_SELF_ROOT/runner-b/runner.log" "$keep/" 2>/dev/null || true
  fi
  forgeci_self_stop
  rm -rf "$FORGECI_SELF_ROOT"
}
trap cleanup EXIT INT TERM

forgeci_self_require
forgeci_self_prepare
forgeci_self_start
export FORGECI_SELF_REPO FORGECI_SELF_ROOT FORGECI_SELF_BIN_DIR FORGECI_SELF_SERVER FORGECI_SELF_PG FORGECI_SELF_COMMIT
"$repo/tools/integration/self_hosting_proof.sh"
passed=1
