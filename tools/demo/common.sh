#!/usr/bin/env bash

demo_repo=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
demo_state_dir="$demo_repo/.forgeci-demo"
demo_state_file="$demo_state_dir/state"

demo_load_state() {
  [[ -f "$demo_state_file" && ! -L "$demo_state_file" ]] || { echo 'ForgeCI demo is not running' >&2; return 1; }
  [[ $(stat -c '%u' "$demo_state_file") == "$(id -u)" ]] || { echo 'demo state is owned by another user' >&2; return 1; }
  local mode
  mode=$(stat -c '%a' "$demo_state_file")
  [[ "$mode" == 600 ]] || { echo 'demo state must have mode 600' >&2; return 1; }
  local allowed='^(FORGECI_SELF_REPO|FORGECI_SELF_ROOT|FORGECI_SELF_BIN_DIR|FORGECI_SELF_SERVER|FORGECI_SELF_RUNNER_ENDPOINT|FORGECI_SELF_PG|FORGECI_SELF_TOKEN_FILE|FORGECI_SELF_COMMIT|FORGECI_SELF_DB_PORT|FORGECI_SELF_SERVER_PID|FORGECI_SELF_RUNNER_A_PID|FORGECI_SELF_RUNNER_B_PID)=[A-Za-z0-9_./:-]+$'
  if grep -Ev "$allowed" "$demo_state_file" | grep -q .; then echo 'demo state contains malformed data' >&2; return 1; fi
  # shellcheck disable=SC1090
  source "$demo_state_file"
  [[ "$FORGECI_SELF_REPO" == "$demo_repo" ]] || { echo 'demo state belongs to another repository' >&2; return 1; }
  [[ "$FORGECI_SELF_ROOT" =~ ^/tmp/forgeci-demo\.[A-Za-z0-9]+$ && -d "$FORGECI_SELF_ROOT" && ! -L "$FORGECI_SELF_ROOT" ]] || { echo 'demo state has an unsafe temporary root' >&2; return 1; }
  [[ "$FORGECI_SELF_BIN_DIR" == "$demo_repo/build" ]] || { echo 'demo state has an unexpected binary directory' >&2; return 1; }
  [[ "$FORGECI_SELF_SERVER" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] || { echo 'demo state has an unsafe server URL' >&2; return 1; }
  [[ "$FORGECI_SELF_PG" =~ ^forgeci-self-[0-9]+-[0-9]+-postgres$ ]] || { echo 'demo state has an unsafe container name' >&2; return 1; }
  local pid
  for pid in "$FORGECI_SELF_SERVER_PID" "$FORGECI_SELF_RUNNER_A_PID" "$FORGECI_SELF_RUNNER_B_PID"; do
    [[ "$pid" =~ ^[0-9]+$ ]] || { echo 'demo state has an invalid process ID' >&2; return 1; }
  done
}

demo_process_matches() {
  local pid=$1 executable=$2
  [[ -r "/proc/$pid/cmdline" ]] || return 1
  tr '\0' ' ' <"/proc/$pid/cmdline" | grep -Fq "$FORGECI_SELF_BIN_DIR/$executable"
}
