#!/usr/bin/env bash
set -euo pipefail
if [[ ${1:-} == -h || ${1:-} == --help ]]; then echo "usage: $0 [evidence-directory]"; echo 'Run and capture the canonical success, cache, artifact, log, and failure proofs.'; exit 0; fi
[[ $# -le 1 ]] || { echo "usage: $0 [evidence-directory]" >&2; exit 2; }
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
source "$repo/tools/demo/common.sh"
demo_load_state
output=${1:-"$demo_state_dir/evidence"}
case "$output" in /*) ;; *) output="$repo/$output" ;; esac
[[ "$output" != *'/../'* && "$output" != */.. && "$output" != *'/./'* ]] || { echo 'evidence output may not contain dot path segments' >&2; exit 2; }
[[ "$output" == "$demo_state_dir"/* || "$output" == "$repo"/demo-evidence/* ]] || { echo 'evidence output must be below .forgeci-demo/ or demo-evidence/' >&2; exit 2; }
[[ ! -e "$output" ]] || { echo "evidence output already exists: $output" >&2; exit 1; }
export FORGECI_SELF_REPO FORGECI_SELF_ROOT FORGECI_SELF_BIN_DIR FORGECI_SELF_SERVER FORGECI_SELF_PG FORGECI_SELF_COMMIT
export FORGECI_SELF_EVIDENCE_DIR=$output
"$repo/tools/integration/self_hosting_proof.sh"
