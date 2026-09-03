#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); tmp=$(mktemp -d /tmp/forgeci-cache.XXXXXX)
pg=forgeci-cache-postgres-$$; api_port=$((38000 + ($$ % 500))); runner_port=$((39000 + ($$ % 500)))
server_pid=; runner_a_pid=; runner_b_pid=; passed=0; run1=; run2=; run3=; run4=; run5=; docker_run=
cleanup() {
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do [ -z "$pid" ] || kill "$pid" >/dev/null 2>&1 || true; done
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do [ -z "$pid" ] || wait "$pid" >/dev/null 2>&1 || true; done
  docker rm -f "$pg" >/dev/null 2>&1 || true
  if [ "$passed" -ne 1 ]; then
    echo '--- server log ---'; cat "$tmp/server.log" 2>/dev/null || true
    echo '--- runner A log ---'; cat "$tmp/runner-a.log" 2>/dev/null || true
    echo '--- runner B log ---'; cat "$tmp/runner-b.log" 2>/dev/null || true
    echo '--- forge inspect ---'; for id in "$run1" "$run2" "$run3" "$run4" "$run5" "$docker_run"; do [ -z "$id" ] || "$tmp/bin/forge" inspect "$id" --server "http://127.0.0.1:$api_port" 2>/dev/null || true; done
    echo '--- job_runs ---'; docker exec "$pg" psql -U postgres -d forgeci -c 'SELECT run_id,job_name,status,runner_id FROM job_runs' 2>/dev/null || true
    echo '--- cache rows ---'; docker exec "$pg" psql -U postgres -d forgeci -c 'SELECT workspace,cache_key,content_sha256,blob_sha256,archive_size_bytes,deleted_at FROM cache_entries' 2>/dev/null || true
    echo '--- docker ps -a ---'; docker ps -a 2>/dev/null || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM
mkdir -p "$tmp/server" "$tmp/runner-a" "$tmp/runner-b" "$tmp/state-a" "$tmp/state-b" "$tmp/bin" "$tmp/snapshots" "$tmp/artifacts" "$tmp/cache"
cat >"$tmp/server/seed.yaml" <<'YAML'
version: 1
jobs:
  build:
    cache:
      restore:
        - key: demo-cache-v1
          path: .cache/demo
      save:
        - key: demo-cache-v1
          path: .cache/demo
    steps:
      - run: mkdir -p .cache/demo
      - run: printf hello > .cache/demo/value
YAML
cat >"$tmp/server/hit.yaml" <<'YAML'
version: 1
jobs:
  build:
    cache:
      restore:
        - key: demo-cache-v1
          path: .cache/demo
    steps:
      - run: test -f .cache/demo/value
      - run: test "$(cat .cache/demo/value)" = hello
YAML
cat >"$tmp/server/v2.yaml" <<'YAML'
version: 1
jobs:
  build:
    cache:
      restore:
        - key: demo-cache-v2
          path: .cache/demo
    steps:
      - run: true
YAML
cat >"$tmp/server/seed-other.yaml" <<'YAML'
version: 1
jobs:
  build:
    cache:
      save:
        - key: demo-cache-other
          path: .cache/demo
    steps:
      - run: mkdir -p .cache/demo
      - run: printf hello > .cache/demo/value
YAML
cat >"$tmp/server/docker.yaml" <<'YAML'
version: 1
jobs:
  build:
    image: alpine:3.20
    cache:
      restore:
        - key: demo-cache-other
          path: .cache/demo
    steps:
      - run: test "$(pwd)" = /workspace
      - run: test -f /workspace/.cache/demo/value
      - run: test "$(cat /workspace/.cache/demo/value)" = hello
YAML
printf '%s\n' integration-token >"$tmp/token"; chmod 600 "$tmp/token"
GOCACHE=/tmp/forgeci-go-cache go build -o "$tmp/bin/forge" "$root/cmd/forge"; GOCACHE=/tmp/forgeci-go-cache go build -o "$tmp/bin/forge-server" "$root/cmd/forge-server"; GOCACHE=/tmp/forgeci-go-cache go build -o "$tmp/bin/forge-runner" "$root/cmd/forge-runner"
docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
db_port=$(docker port "$pg" 5432/tcp | sed 's/.*://'); i=0; until docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done; sleep 1
until docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1; do sleep .1; done
docker exec -i "$pg" psql -v ON_ERROR_STOP=1 -U postgres -d forgeci <"$root/internal/store/postgres/migrations/001_initial.sql" >/dev/null
docker exec -i "$pg" psql -v ON_ERROR_STOP=1 -U postgres -d forgeci <"$root/internal/store/postgres/migrations/002_runners_and_leases.sql" >/dev/null
docker exec "$pg" psql -U postgres -d forgeci -c "INSERT INTO schema_migrations(version) VALUES(1),(2) ON CONFLICT DO NOTHING" >/dev/null
db() { docker exec "$pg" psql -U postgres -d forgeci -At -c "$1"; }
while :; do "$tmp/bin/forge-server" --execution-mode remote --listen "127.0.0.1:$api_port" --runner-listen "127.0.0.1:$runner_port" --runner-token-file "$tmp/token" --workspace "$tmp/server" --snapshot-dir "$tmp/snapshots" --artifact-dir "$tmp/artifacts" --cache-dir "$tmp/cache" --database-url "postgres://postgres:forgeci@127.0.0.1:$db_port/forgeci?sslmode=disable" >"$tmp/server.log" 2>&1 & server_pid=$!; i=0; while kill -0 "$server_pid" 2>/dev/null && ! curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || break; sleep .1; done; curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1 && break; wait "$server_pid" 2>/dev/null || true; server_pid=; done
submit() { (cd "$tmp/server" && "$tmp/bin/forge" submit --server "http://127.0.0.1:$api_port" --file "$(basename "$1")") | awk '{print $2}'; }; status() { "$tmp/bin/forge" inspect "$1" --server "http://127.0.0.1:$api_port" 2>/dev/null | sed -n 's/^Status: //p'; }; wait_pass() { id=$1; i=0; while :; do s=$(status "$id"); case "$s" in PASSED) return 0;; FAILED|ERROR|CANCELED|ABORTED|BLOCKED|'') "$tmp/bin/forge" inspect "$id" --server "http://127.0.0.1:$api_port"; return 1;; esac; i=$((i+1)); [ "$i" -lt 500 ] || return 1; sleep .1; done; }
wait_terminal() { id=$1; i=0; while :; do s=$(status "$id"); case "$s" in PASSED|FAILED|ERROR|CANCELED|ABORTED|BLOCKED) return 0;; esac; i=$((i+1)); [ "$i" -lt 500 ] || return 1; sleep .1; done; }
start_a() { FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner-a" --state-dir "$tmp/state-a" --name runner-a --max-parallel 1 >"$tmp/runner-a.log" 2>&1 & runner_a_pid=$!; i=0; while [ ! -s "$tmp/state-a/runner-id" ]; do i=$((i+1)); [ "$i" -lt 100 ] || return 1; sleep .1; done; }; start_b() { FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner-b" --state-dir "$tmp/state-b" --name runner-b --max-parallel 1 >"$tmp/runner-b.log" 2>&1 & runner_b_pid=$!; i=0; while [ ! -s "$tmp/state-b/runner-id" ]; do i=$((i+1)); [ "$i" -lt 100 ] || return 1; sleep .1; done; }
start_a; runner_a_id=$(tr -d '\n' <"$tmp/state-a/runner-id"); run1=$(submit "$tmp/server/seed.yaml"); wait_pass "$run1"; grep -Fq '[cache] MISS demo-cache-v1' "$tmp/runner-a.log"; grep -Fq '[cache] SAVE demo-cache-v1' "$tmp/runner-a.log"; [ "$(db "SELECT runner_id::text FROM job_runs WHERE run_id='$run1' AND job_name='build'")" = "$runner_a_id" ]; meta=$(db "SELECT content_sha256||' '||blob_sha256||' '||archive_size_bytes FROM cache_entries WHERE workspace='$tmp/server' AND cache_key='demo-cache-v1' AND deleted_at IS NULL"); set -- $meta; content_sha=$1; blob_sha=$2; archive_size=$3; [ "$archive_size" -gt 0 ]; cas="$tmp/cache/blobs/sha256/$(printf '%s' "$blob_sha" | cut -c1-2)/$(printf '%s' "$blob_sha" | cut -c3-)"; [ "$(sha256sum "$cas" | awk '{print $1}')" = "$blob_sha" ]; echo "Run 1 ID=$run1 Runner A ID=$runner_a_id MISS/SAVE content_sha=$content_sha blob_sha=$blob_sha archive_size=$archive_size CAS=$cas"
kill "$runner_a_pid"; wait "$runner_a_pid" 2>/dev/null || true; runner_a_pid=; start_b; runner_b_id=$(tr -d '\n' <"$tmp/state-b/runner-id"); [ "$runner_a_id" != "$runner_b_id" ]; run2=$(submit "$tmp/server/hit.yaml"); wait_pass "$run2"; grep -Fq '[cache] HIT demo-cache-v1' "$tmp/runner-b.log"; [ "$run1" != "$run2" ]; [ "$(db "SELECT runner_id::text FROM job_runs WHERE run_id='$run2' AND job_name='build'")" = "$runner_b_id" ]; echo "Run 2 ID=$run2 Runner B ID=$runner_b_id HIT/cross-runner proof"
run3=$(submit "$tmp/server/v2.yaml"); wait_pass "$run3"; grep -Fq '[cache] MISS demo-cache-v2' "$tmp/runner-b.log"; echo 'exact-key MISS demo-cache-v2'; "$tmp/bin/forge" cache list --server "http://127.0.0.1:$api_port" | grep -q '^demo-cache-v1'; "$tmp/bin/forge" cache delete demo-cache-v1 --server "http://127.0.0.1:$api_port"; ! "$tmp/bin/forge" cache list --server "http://127.0.0.1:$api_port" | grep -q '^demo-cache-v1'; find "$tmp/runner-b" -type d -path '*/.cache/demo' -exec rm -rf {} +; run4=$(submit "$tmp/server/hit.yaml"); wait_terminal "$run4"; [ "$(status "$run4")" = FAILED ]; grep -Fq '[cache] MISS demo-cache-v1' "$tmp/runner-b.log"; echo "list/delete/post-delete MISS run=$run4"
run5=$(submit "$tmp/server/seed-other.yaml"); wait_pass "$run5"; grep -Fq '[cache] SAVE demo-cache-other' "$tmp/runner-b.log"; docker_run=$(submit "$tmp/server/docker.yaml"); wait_pass "$docker_run"; grep -Fq '[cache] HIT demo-cache-other' "$tmp/runner-b.log"; echo "Docker HIT run=$docker_run"; passed=1; echo 'build cache integration passed'
