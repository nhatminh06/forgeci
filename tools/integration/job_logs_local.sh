#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d /tmp/forgeci-joblogs.XXXXXX)
pg=forgeci-joblogs-$$
server_pid=
passed=0
port=$((40000 + ($$ % 500)))
cleanup() {
  [ -z "$server_pid" ] || kill "$server_pid" 2>/dev/null || true
  [ -z "$server_pid" ] || wait "$server_pid" 2>/dev/null || true
  docker rm -f "$pg" >/dev/null 2>&1 || true
  if [ "$passed" -ne 1 ]; then cat "$tmp/server.log" 2>/dev/null || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM
mkdir -p "$tmp/bin" "$tmp/work" "$tmp/snapshots" "$tmp/artifacts" "$tmp/cache"
cat >"$tmp/work/pipeline.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: printf 'alpha\n'
      - run: printf 'beta\n'
      - run: printf 'stderr-marker\n' >&2
      - run: printf 'gamma\n'
YAML
cat >"$tmp/work/failed.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: printf 'before-failure\n'
      - run: exit 7
YAML
cat >"$tmp/work/empty.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: true
YAML
GOCACHE=/tmp/forgeci-go-cache go build -o "$tmp/bin/forge" "$root/cmd/forge"
GOCACHE=/tmp/forgeci-go-cache go build -o "$tmp/bin/forge-server" "$root/cmd/forge-server"
docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
dbport=$(docker port "$pg" 5432/tcp | sed 's/.*://')
i=0
until docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1; do
  i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1
done
until docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1; do sleep .1; done
db() { docker exec "$pg" psql -U postgres -d forgeci -At -c "$1"; }
start() {
  "$tmp/bin/forge-server" --execution-mode local --listen "127.0.0.1:$port" --workspace "$tmp/work" --snapshot-dir "$tmp/snapshots" --artifact-dir "$tmp/artifacts" --cache-dir "$tmp/cache" --database-url "postgres://postgres:forgeci@127.0.0.1:$dbport/forgeci?sslmode=disable" >"$tmp/server.log" 2>&1 &
  server_pid=$!
  i=0
  until curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; do
    i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1
  done
}
start
submit() { (cd "$tmp/work" && "$tmp/bin/forge" submit --server "http://127.0.0.1:$port" --file "$1") | awk '{print $2}'; }
status() { "$tmp/bin/forge" inspect "$1" --server "http://127.0.0.1:$port" | sed -n 's/^Status: //p'; }
waitrun() {
  id=$1; i=0
  while :; do
    s=$(status "$id")
    case "$s" in PASSED|FAILED|ERROR|CANCELED|ABORTED) return;; esac
    i=$((i+1)); [ "$i" -lt 300 ] || exit 1; sleep .1
  done
}
run1=$(submit pipeline.yaml); waitrun "$run1"; [ "$(status "$run1")" = PASSED ]
rows=$(db "SELECT sequence,stream,convert_from(payload,'UTF8') FROM job_log_chunks WHERE run_id='$run1' AND job_name='build' ORDER BY sequence")
for m in alpha beta stderr-marker gamma; do echo "$rows" | grep -q "$m"; done
[ "$(db "SELECT count(*) FROM job_log_chunks WHERE run_id='$run1' AND job_name='build'")" -eq 4 ]
out=$("$tmp/bin/forge" logs "$run1" --job build --server "http://127.0.0.1:$port" 2>/dev/null)
for m in alpha beta stderr-marker gamma; do [ "$(printf '%s' "$out" | grep -o "$m" | wc -l | tr -d ' ')" -eq 1 ]; done
kill "$server_pid"; wait "$server_pid" 2>/dev/null || true; server_pid=
start
out2=$("$tmp/bin/forge" logs "$run1" --job build --server "http://127.0.0.1:$port" 2>/dev/null); [ "$out" = "$out2" ]
run2=$(submit failed.yaml); waitrun "$run2"; [ "$(status "$run2")" = FAILED ]
"$tmp/bin/forge" logs "$run2" --job build --server "http://127.0.0.1:$port" 2>/dev/null | grep -q before-failure
run3=$(submit empty.yaml); waitrun "$run3"; [ "$(status "$run3")" = PASSED ]
[ -z "$("$tmp/bin/forge" logs "$run3" --job build --server "http://127.0.0.1:$port" 2>/dev/null)" ]
passed=1
echo "local job logs passed run=$run1 failed=$run2 empty=$run3"
