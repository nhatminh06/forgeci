#!/bin/sh
set -eu

# Distributed cache smoke gate.  The remote-runner harness owns the real
# PostgreSQL/server/runner lifecycle; keep this entry point composable with it.
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)

"$root/tools/integration/remote_runners.sh"

# Exercise deterministic cache archive/CAS behavior in the same gate so a
# broken cache implementation cannot be hidden by a healthy scheduler.
(cd "$root" && go test ./internal/cache ./internal/config -count=1)

echo "build cache integration passed"
