#!/bin/sh
# Milestone 10 log gate dispatcher. Existing suites own shared PostgreSQL,
# server, runner, Docker, lease-loss, and restart setup.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
scenario=all
while [ "$#" -gt 0 ]; do
  case "$1" in
    --scenario) [ "$#" -ge 2 ] || { echo "--scenario requires a value" >&2; exit 2; }; scenario=$2; shift 2 ;;
    -h|--help) echo "usage: $0 [--scenario all|local|remote-basic|runner-loss|follow]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
run_local() { "$root/tools/integration/job_logs_local.sh"; }
run_remote() { "$root/tools/integration/remote_runners.sh"; }
case "$scenario" in
  all) run_local; run_remote ;;
  local) run_local ;;
  remote-basic|runner-loss|follow) run_remote ;;
  *) echo "invalid scenario: $scenario" >&2; exit 2 ;;
esac
echo "job logs integration passed"
