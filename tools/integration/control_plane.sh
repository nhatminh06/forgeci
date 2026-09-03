#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
name="forgeci-m4-postgres-$$"
listen="127.0.0.1:38081"
server_url="http://$listen"
server_pid=""
snapshot_dir=$(mktemp -d /tmp/forgeci-control-snapshots.XXXXXX)
artifact_dir=$(mktemp -d /tmp/forgeci-control-artifacts.XXXXXX)
workspace=$(mktemp -d /tmp/forgeci-control-workspace.XXXXXX)
release="$snapshot_dir/release"
cat >"$workspace/forge.yaml" <<YAML
version: 1
jobs:
  frozen:
    steps:
      - run: while [ ! -f "$release" ]; do sleep .05; done
      - run: test "\$(cat version.txt)" = A
YAML
printf 'A\n' >"$workspace/version.txt"
cat >"$workspace/docker.yaml" <<'YAML'
version: 1
jobs:
  docker:
    image: alpine:3.20
    steps:
      - run: test "$(cat version.txt)" = B
      - run: test "$(pwd)" = /workspace
YAML
cat >"$workspace/artifacts.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: mkdir -p dist && printf durable > dist/app
    artifacts:
      upload:
        - name: app
          path: dist/app
  scrub:
    needs: [build]
    steps:
      - run: rm -f dist/app
  consume:
    needs: [build, scrub]
    artifacts:
      download:
        - from: build
          name: app
          into: restored
    steps:
      - run: test "$(cat restored/app)" = durable
YAML

cleanup() {
  if [[ -n "$server_pid" ]]; then kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  rm -rf "$snapshot_dir"
  rm -rf "$artifact_dir"
  rm -rf "$workspace"
  rm -f "$release"
}
trap cleanup EXIT INT TERM

docker run -d --name "$name" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 50); do docker exec "$name" pg_isready -U postgres -d forgeci >/dev/null 2>&1 && break; sleep 0.1; done
sleep 1
for _ in $(seq 1 50); do docker exec "$name" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 0.1; done
docker exec "$name" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null
port=$(docker port "$name" 5432/tcp | sed 's/.*://')
for _ in $(seq 1 50); do timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null && break; sleep 0.1; done
timeout 1 bash -c "</dev/tcp/127.0.0.1/$port"
database_url="postgres://postgres:forgeci@127.0.0.1:$port/forgeci?sslmode=disable"

start_server() {
  for _ in $(seq 1 20); do
    build/forge-server --listen "$listen" --workspace "$workspace" --snapshot-dir "$snapshot_dir" --artifact-dir "$artifact_dir" --database-url "$database_url" &
    server_pid=$!
    for _ in $(seq 1 10); do
      if curl -fsS "$server_url/healthz" >/dev/null 2>&1; then return 0; fi
      kill -0 "$server_pid" 2>/dev/null || break
      sleep 0.1
    done
    wait "$server_pid" 2>/dev/null || true
    server_pid=""
    sleep 0.1
  done
  return 1
}

cd "$root"
mkdir -p build
go build -o build/forge ./cmd/forge
go build -o build/forge-server ./cmd/forge-server
start_server

run_id=$(build/forge submit --server "$server_url" --file forge.yaml --jobs 3 | awk '{print $2}')
printf 'B\n' >"$workspace/version.txt"
touch "$release"
for _ in $(seq 1 100); do output=$(build/forge inspect "$run_id" --server "$server_url"); grep -q 'Status: PASSED' <<<"$output" && break; sleep 0.1; done
grep -q 'Status: PASSED' <<<"$output"
build/forge runs --server "$server_url" --limit 5 | grep -q "$run_id"

artifact_run=$(build/forge submit --server "$server_url" --file artifacts.yaml --jobs 2 | awk '{print $2}')
for _ in $(seq 1 100); do artifact_output=$(build/forge inspect "$artifact_run" --server "$server_url"); grep -q 'Status: PASSED' <<<"$artifact_output" && break; sleep 0.1; done
grep -q 'Status: PASSED' <<<"$artifact_output"
build/forge artifacts "$artifact_run" --server "$server_url" | grep -q 'build.*app.*available'
build/forge artifact download "$artifact_run" build app --output "$workspace/app.tar.gz" --server "$server_url" >/dev/null

kill -TERM "$server_pid"
wait "$server_pid"
server_pid=""
start_server
build/forge inspect "$run_id" --server "$server_url" | grep -q 'Status: PASSED'
build/forge artifacts "$artifact_run" --server "$server_url" | grep -q 'build.*app.*available'

if docker info >/dev/null 2>&1; then
  docker_id=$(build/forge submit --server "$server_url" --file docker.yaml --jobs 1 | awk '{print $2}')
  for _ in $(seq 1 100); do docker_output=$(build/forge inspect "$docker_id" --server "$server_url"); grep -q 'Status: PASSED' <<<"$docker_output" && break; sleep 0.1; done
  grep -q 'Status: PASSED' <<<"$docker_output"
fi
