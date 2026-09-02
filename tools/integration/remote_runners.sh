#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d /tmp/forgeci-remote.XXXXXX)
pg=forgeci-remote-postgres-$$
api_port=$((38000 + ($$ % 500)))
runner_port=$((39000 + ($$ % 500)))
server_pid=
runner_a_pid=
runner_b_pid=

cleanup() {
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do
    [ -z "$pid" ] || kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do
    [ -z "$pid" ] || wait "$pid" >/dev/null 2>&1 || true
  done
  docker rm -f "$pg" >/dev/null 2>&1 || true
  docker ps -aq --filter label=forgeci.managed=true | xargs -r docker rm -f >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$tmp/server" "$tmp/runner-a" "$tmp/runner-b" "$tmp/state-a" "$tmp/state-b" "$tmp/bin"
touch "$tmp/runner-a/RUNNER_ONLY_MARKER" "$tmp/runner-b/RUNNER_ONLY_MARKER"
cat >"$tmp/server/remote.yaml" <<'YAML'
version: 1
jobs:
  remote:
    steps:
      - run: test -f RUNNER_ONLY_MARKER
      - run: test ! -f SERVER_ONLY_MARKER
YAML
cat >"$tmp/server/slow.yaml" <<'YAML'
version: 1
jobs:
  slow:
    steps:
      - run: test -f RUNNER_ONLY_MARKER
      - run: sleep 2
YAML
cat >"$tmp/server/docker.yaml" <<'YAML'
version: 1
jobs:
  docker:
    image: alpine:3.20
    steps:
      - run: test -f RUNNER_ONLY_MARKER
      - run: test "$(pwd)" = /workspace
YAML
cat >"$tmp/server/long.yaml" <<'YAML'
version: 1
jobs:
  long:
    steps:
      - run: sleep 60
      - run: touch SHOULD_NOT_EXIST
YAML
touch "$tmp/server/SERVER_ONLY_MARKER"
printf '%s\n' integration-token >"$tmp/token"
chmod 600 "$tmp/token"

GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge" "$root/cmd/forge"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-server" "$root/cmd/forge-server"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-runner" "$root/cmd/forge-runner"

docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
db_port=$(docker port "$pg" 5432/tcp | sed 's/.*://')
i=0; until docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done

i=0
while :; do
  "$tmp/bin/forge-server" --execution-mode remote --listen "127.0.0.1:$api_port" --runner-listen "127.0.0.1:$runner_port" --runner-token-file "$tmp/token" --workspace "$tmp/server" --database-url "postgres://postgres:forgeci@127.0.0.1:$db_port/forgeci?sslmode=disable" >"$tmp/server.log" 2>&1 & server_pid=$!
  j=0; while kill -0 "$server_pid" >/dev/null 2>&1 && ! curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1; do j=$((j+1)); [ "$j" -lt 30 ] || break; sleep .1; done
  curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1 && break
  wait "$server_pid" >/dev/null 2>&1 || true; server_pid=
  i=$((i+1)); [ "$i" -lt 20 ] || { cat "$tmp/server.log"; exit 1; }
  sleep .2
done

submit() { "$tmp/bin/forge" submit --server "http://127.0.0.1:$api_port" --file "$1" --jobs "${2:-1}" | awk '{print $2}'; }
status() { "$tmp/bin/forge" inspect "$1" --server "http://127.0.0.1:$api_port" 2>/dev/null | sed -n 's/^Status: //p'; }
wait_status() { id=$1; wanted=$2; i=0; while [ "$(status "$id")" != "$wanted" ]; do i=$((i+1)); [ "$i" -lt 450 ] || { "$tmp/bin/forge" inspect "$id" --server "http://127.0.0.1:$api_port"; exit 1; }; sleep .1; done; }

queued=$(submit remote.yaml)
sleep 1
[ "$(status "$queued")" = QUEUED ]
[ ! -e "$tmp/server/RUNNER_ONLY_MARKER" ]

FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace "$tmp/runner-a" --state-dir "$tmp/state-a" --name runner-a --max-parallel 2 >"$tmp/runner-a.log" 2>&1 & runner_a_pid=$!
FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace "$tmp/runner-b" --state-dir "$tmp/state-b" --name runner-b --max-parallel 2 >"$tmp/runner-b.log" 2>&1 & runner_b_pid=$!
wait_status "$queued" PASSED

first=$(submit slow.yaml); second=$(submit slow.yaml)
i=0
while [ "$(status "$first")" != RUNNING ] || [ "$(status "$second")" != RUNNING ]; do
  i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1
done
listing=$("$tmp/bin/forge" runners --server "http://127.0.0.1:$api_port")
printf '%s\n' "$listing" | grep -q "$first"
printf '%s\n' "$listing" | grep -q "$second"
wait_status "$first" PASSED; wait_status "$second" PASSED

docker_run=$(submit docker.yaml)
wait_status "$docker_run" PASSED

canceled=$(submit long.yaml)
i=0; while [ "$(status "$canceled")" != RUNNING ]; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
"$tmp/bin/forge" cancel "$canceled" --server "http://127.0.0.1:$api_port" >/dev/null
wait_status "$canceled" CANCELED
[ ! -e "$tmp/runner-a/SHOULD_NOT_EXIST" ]
[ ! -e "$tmp/runner-b/SHOULD_NOT_EXIST" ]

identity=$(tr -d '\n' <"$tmp/state-a/runner-id")
kill "$runner_b_pid"; wait "$runner_b_pid" || true; runner_b_pid=
lost=$(submit long.yaml)
i=0; while [ "$(status "$lost")" != RUNNING ]; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
kill -9 "$runner_a_pid"; wait "$runner_a_pid" >/dev/null 2>&1 || true; runner_a_pid=
wait_status "$lost" ABORTED
FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace "$tmp/runner-a" --state-dir "$tmp/state-a" --name runner-a --max-parallel 2 >"$tmp/runner-a-2.log" 2>&1 & runner_a_pid=$!
[ "$(tr -d '\n' <"$tmp/state-a/runner-id")" = "$identity" ]

if grep -F integration-token "$tmp"/*.log >/dev/null 2>&1; then echo "runner token leaked" >&2; exit 1; fi
test -z "$(docker ps -q --filter label=forgeci.managed=true)"
echo "remote runner integration passed"
