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

mkdir -p "$tmp/server" "$tmp/runner-a" "$tmp/runner-b" "$tmp/state-a" "$tmp/state-b" "$tmp/bin" "$tmp/snapshots" "$tmp/artifacts"
cat >"$tmp/server/remote.yaml" <<'YAML'
version: 1
jobs:
  remote:
    steps:
      - run: test -f SERVER_ONLY_MARKER
      - run: test ! -f RUNNER_ONLY_MARKER
      - run: test "$(cat version.txt)" = A
      - run: test ! -e late.txt
YAML
cat >"$tmp/server/slow.yaml" <<'YAML'
version: 1
jobs:
  slow:
    steps:
      - run: test -f SERVER_ONLY_MARKER
      - run: sleep 2
YAML
cat >"$tmp/server/docker.yaml" <<'YAML'
version: 1
jobs:
  docker:
    image: alpine:3.20
    steps:
      - run: test -f SERVER_ONLY_MARKER
      - run: test "$(pwd)" = /workspace
YAML
cat >"$tmp/server/partition.yaml" <<'YAML'
version: 1
jobs:
  partition:
    image: alpine:3.20
    steps:
      - run: sleep 60
      - run: touch SHOULD_NOT_EXIST
YAML
cat >"$tmp/server/long.yaml" <<'YAML'
version: 1
jobs:
  long:
    steps:
      - run: sleep 60
      - run: touch SHOULD_NOT_EXIST
YAML
cat >"$tmp/server/leak.yaml" <<'YAML'
version: 1
jobs:
  leak:
    steps:
      - run: echo secret-from-run1 > leaked.txt
YAML
cat >"$tmp/server/no-leak.yaml" <<'YAML'
version: 1
jobs:
  isolated:
    steps:
      - run: test ! -e leaked.txt
YAML
cat >"$tmp/server/artifacts.yaml" <<'YAML'
version: 1
jobs:
  build:
    image: alpine:3.20
    steps:
      - run: mkdir -p dist && printf value > dist/app && chmod 755 dist/app
    artifacts:
      upload:
        - name: application
          path: dist/app
  scrub:
    needs: [build]
    steps:
      - run: rm -f dist/app
  consume:
    needs: [build, scrub]
    image: alpine:3.20
    artifacts:
      download:
        - from: build
          name: application
          into: restored
    steps:
      - run: test "$(cat restored/app)" = value
      - run: test -x restored/app
YAML
cat >"$tmp/server/artifact-access.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: mkdir -p out && printf scoped > out/value
    artifacts:
      upload:
        - name: scoped
          path: out/value
  consume:
    needs: [build]
    artifacts:
      download:
        - from: build
          name: scoped
          into: input
    steps:
      - run: sleep 10
YAML
cat >"$tmp/server/artifact-failed.yaml" <<'YAML'
version: 1
jobs:
  build:
    steps:
      - run: mkdir -p out && printf evidence > out/value
    artifacts:
      upload:
        - name: evidence
          path: out/value
  fail:
    needs: [build]
    steps:
      - run: exit 7
YAML
touch "$tmp/server/SERVER_ONLY_MARKER"
printf 'A\n' > "$tmp/server/version.txt"
printf '%s\n' integration-token >"$tmp/token"
chmod 600 "$tmp/token"

GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge" "$root/cmd/forge"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-server" "$root/cmd/forge-server"
GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$tmp/bin/forge-runner" "$root/cmd/forge-runner"

docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
db_port=$(docker port "$pg" 5432/tcp | sed 's/.*://')
i=0; until docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
sleep 1
i=0; until docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
docker exec -i "$pg" psql -v ON_ERROR_STOP=1 -U postgres -d forgeci <"$root/internal/store/postgres/migrations/001_initial.sql" >/dev/null
docker exec -i "$pg" psql -v ON_ERROR_STOP=1 -U postgres -d forgeci <"$root/internal/store/postgres/migrations/002_runners_and_leases.sql" >/dev/null
docker exec "$pg" psql -v ON_ERROR_STOP=1 -U postgres -d forgeci -c "INSERT INTO schema_migrations(version) VALUES(1),(2) ON CONFLICT DO NOTHING; INSERT INTO pipeline_runs(id,status,pipeline_file,pipeline_yaml,pipeline_sha256,workspace,max_parallel,effective_parallel) VALUES('00000000-0000-4000-8000-000000000099','PASSED','old.yaml',convert_to('version: 1','UTF8'),repeat('a',64),'$tmp/server',1,1);" >/dev/null

i=0
while :; do
  "$tmp/bin/forge-server" --execution-mode remote --listen "127.0.0.1:$api_port" --runner-listen "127.0.0.1:$runner_port" --runner-token-file "$tmp/token" --workspace "$tmp/server" --snapshot-dir "$tmp/snapshots" --artifact-dir "$tmp/artifacts" --artifact-retention 2s --artifact-gc-interval 200ms --artifact-orphan-grace 1s --database-url "postgres://postgres:forgeci@127.0.0.1:$db_port/forgeci?sslmode=disable" >"$tmp/server.log" 2>&1 & server_pid=$!
  j=0; while kill -0 "$server_pid" >/dev/null 2>&1 && ! curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1; do j=$((j+1)); [ "$j" -lt 30 ] || break; sleep .1; done
  curl -fsS "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1 && break
  wait "$server_pid" >/dev/null 2>&1 || true; server_pid=
  i=$((i+1)); [ "$i" -lt 20 ] || { cat "$tmp/server.log"; exit 1; }
  sleep .2
done
[ "$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(*) FROM schema_migrations WHERE version IN (3,4,5)")" = 3 ]
[ "$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT source_snapshot_sha256 IS NULL FROM pipeline_runs WHERE id='00000000-0000-4000-8000-000000000099'")" = t ]
"$tmp/bin/forge" inspect 00000000-0000-4000-8000-000000000099 --server "http://127.0.0.1:$api_port" >/dev/null

submit() { "$tmp/bin/forge" submit --server "http://127.0.0.1:$api_port" --file "$1" --jobs "${2:-1}" | awk '{print $2}'; }
status() { "$tmp/bin/forge" inspect "$1" --server "http://127.0.0.1:$api_port" 2>/dev/null | sed -n 's/^Status: //p'; }
wait_status() { id=$1; wanted=$2; i=0; while :; do current=$(status "$id"); [ "$current" = "$wanted" ] && break; case "$current" in PASSED|FAILED|CANCELED|ERROR|ABORTED) "$tmp/bin/forge" inspect "$id" --server "http://127.0.0.1:$api_port"; printf '%s\n' '-- server --'; cat "$tmp/server.log"; printf '%s\n' '-- runner a --'; cat "$tmp/runner-a.log"; printf '%s\n' '-- runner b --'; cat "$tmp/runner-b.log"; exit 1;; esac; i=$((i+1)); [ "$i" -lt 450 ] || { "$tmp/bin/forge" inspect "$id" --server "http://127.0.0.1:$api_port"; exit 1; }; sleep .1; done; }

queued=$(submit remote.yaml)
printf 'B\n' > "$tmp/server/version.txt"
touch "$tmp/server/late.txt"
sleep 1
[ "$(status "$queued")" = QUEUED ]
[ ! -e "$tmp/server/RUNNER_ONLY_MARKER" ]

FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner-a" --state-dir "$tmp/state-a" --name runner-a --max-parallel 2 >"$tmp/runner-a.log" 2>&1 & runner_a_pid=$!
FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner-b" --state-dir "$tmp/state-b" --name runner-b --max-parallel 2 >"$tmp/runner-b.log" 2>&1 & runner_b_pid=$!
wait_status "$queued" PASSED
test -z "$(find "$tmp/runner-a/runs" "$tmp/runner-b/runs" -mindepth 2 -type d 2>/dev/null || true)"

same_a=$(submit slow.yaml); same_b=$(submit slow.yaml)
digest_a=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$same_a'")
digest_b=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$same_b'")
[ "$digest_a" = "$digest_b" ]
[ "$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(*) FROM source_snapshots WHERE source_digest='$digest_a'")" = 1 ]
wait_status "$same_a" PASSED; wait_status "$same_b" PASSED

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

artifact_run=$(submit artifacts.yaml 2)
wait_status "$artifact_run" PASSED
artifact_listing=$("$tmp/bin/forge" artifacts "$artifact_run" --server "http://127.0.0.1:$api_port")
printf '%s\n' "$artifact_listing" | grep -q 'build.*application.*available'
"$tmp/bin/forge" artifact download "$artifact_run" build application --output "$tmp/application.tar.gz" --server "http://127.0.0.1:$api_port" >/dev/null
artifact_digest=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT blob_sha256 FROM artifacts WHERE run_id='$artifact_run' AND producer_job='build' AND name='application'")
[ "$(sha256sum "$tmp/application.tar.gz" | awk '{print $1}')" = "$artifact_digest" ]
[ "$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(*) FROM artifacts WHERE run_id='$artifact_run' AND expires_at IS NOT NULL AND deleted_at IS NULL")" = 1 ]

access_run=$(submit artifact-access.yaml)
i=0; until [ "$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT status FROM job_runs WHERE run_id='$access_run' AND job_name='consume'")" = RUNNING ]; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
access_row=$(docker exec "$pg" psql -U postgres -d forgeci -At -F ' ' -c "SELECT runner_id,lease_id,lease_generation FROM pipeline_runs WHERE id='$access_run'")
access_runner=$(printf '%s' "$access_row" | awk '{print $1}'); access_lease=$(printf '%s' "$access_row" | awk '{print $2}'); access_generation=$(printf '%s' "$access_row" | awk '{print $3}')
runner_a_id=$(tr -d '\n' <"$tmp/state-a/runner-id"); runner_b_id=$(tr -d '\n' <"$tmp/state-b/runner-id"); wrong_runner=$runner_a_id; [ "$wrong_runner" != "$access_runner" ] || wrong_runner=$runner_b_id
wrong_artifact_code=$(curl -sS -o "$tmp/wrong-artifact.json" -w '%{http_code}' -H 'Authorization: Bearer integration-token' "http://127.0.0.1:$runner_port/v1/runner/leases/$access_lease/artifacts/build/scoped?runner_id=$wrong_runner&run_id=$access_run&generation=$access_generation&consumer_job=consume")
[ "$wrong_artifact_code" = 409 ]
cross_run_code=$(curl -sS -o "$tmp/cross-artifact.json" -w '%{http_code}' -H 'Authorization: Bearer integration-token' "http://127.0.0.1:$runner_port/v1/runner/leases/$access_lease/artifacts/build/application?runner_id=$access_runner&run_id=$artifact_run&generation=$access_generation&consumer_job=consume")
[ "$cross_run_code" = 409 ]
"$tmp/bin/forge" cancel "$access_run" --server "http://127.0.0.1:$api_port" >/dev/null
wait_status "$access_run" CANCELED
"$tmp/bin/forge" artifacts "$access_run" --server "http://127.0.0.1:$api_port" | grep -q 'build.*scoped.*available'
empty_sha=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
stale_upload_code=$(curl -sS -o "$tmp/stale-upload.json" -w '%{http_code}' -X PUT -H 'Authorization: Bearer integration-token' -H 'Content-Length: 0' "http://127.0.0.1:$runner_port/v1/runner/leases/$access_lease/jobs/consume/artifact-blobs/$empty_sha?runner_id=$access_runner&run_id=$access_run&generation=$access_generation")
[ "$stale_upload_code" = 409 ]
stale_download_code=$(curl -sS -o "$tmp/stale-download.json" -w '%{http_code}' -H 'Authorization: Bearer integration-token' "http://127.0.0.1:$runner_port/v1/runner/leases/$access_lease/artifacts/build/scoped?runner_id=$access_runner&run_id=$access_run&generation=$access_generation&consumer_job=consume")
[ "$stale_download_code" = 409 ]
failed_artifact_run=$(submit artifact-failed.yaml)
wait_status "$failed_artifact_run" FAILED
"$tmp/bin/forge" artifacts "$failed_artifact_run" --server "http://127.0.0.1:$api_port" | grep -q 'build.*evidence.*available'
sleep 3
"$tmp/bin/forge" artifacts "$artifact_run" --server "http://127.0.0.1:$api_port" | grep -q 'build.*application.*expired'
expired_code=$(curl -sS -o "$tmp/expired.json" -w '%{http_code}' "http://127.0.0.1:$api_port/v1/runs/$artifact_run/artifacts/build/application")
[ "$expired_code" = 410 ]

leak_run=$(submit leak.yaml); wait_status "$leak_run" PASSED
isolated_run=$(submit no-leak.yaml); wait_status "$isolated_run" PASSED

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
i=0; until find "$tmp/runner-a/runs" -name .forgeci-workspace.json | grep -q .; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
kill -9 "$runner_a_pid"; wait "$runner_a_pid" >/dev/null 2>&1 || true; runner_a_pid=
find "$tmp/runner-a/runs" -name .forgeci-workspace.json | grep -q .
wait_status "$lost" ABORTED
FORGECI_RUNNER_TOKEN=integration-token "$tmp/bin/forge-runner" --server "http://127.0.0.1:$runner_port" --workspace-root "$tmp/runner-a" --state-dir "$tmp/state-a" --name runner-a --max-parallel 2 >"$tmp/runner-a-2.log" 2>&1 & runner_a_pid=$!
[ "$(tr -d '\n' <"$tmp/state-a/runner-id")" = "$identity" ]
i=0; while find "$tmp/runner-a/runs" -name .forgeci-workspace.json | grep -q .; do i=$((i+1)); [ "$i" -lt 50 ] || exit 1; sleep .1; done

partitioned=$(submit partition.yaml)
i=0; while [ "$(status "$partitioned")" != RUNNING ]; do i=$((i+1)); [ "$i" -lt 100 ] || exit 1; sleep .1; done
lease_row=$(docker exec "$pg" psql -U postgres -d forgeci -At -F ' ' -c "SELECT runner_id,lease_id,lease_generation FROM pipeline_runs WHERE id='$partitioned'")
partition_runner=$(printf '%s' "$lease_row" | awk '{print $1}')
partition_lease=$(printf '%s' "$lease_row" | awk '{print $2}')
partition_generation=$(printf '%s' "$lease_row" | awk '{print $3}')
wrong_runner=$(tr -d '\n' <"$tmp/state-b/runner-id")
wrong_source_code=$(curl -sS -o "$tmp/wrong-source.json" -w '%{http_code}' -H 'Authorization: Bearer integration-token' "http://127.0.0.1:$runner_port/v1/runner/leases/$partition_lease/source?runner_id=$wrong_runner&run_id=$partitioned&generation=$partition_generation")
[ "$wrong_source_code" = 409 ]
kill -STOP "$server_pid"
sleep 36
test -z "$(docker ps -q --filter label=forgeci.managed=true)"
[ ! -e "$tmp/runner-a/SHOULD_NOT_EXIST" ]
kill -CONT "$server_pid"
wait_status "$partitioned" ABORTED
stale_code=$(curl -sS -o "$tmp/stale.json" -w '%{http_code}' -X POST -H 'Authorization: Bearer integration-token' -H 'Content-Type: application/json' --data "{\"runner_id\":\"$partition_runner\",\"run_id\":\"$partitioned\",\"lease_id\":\"$partition_lease\",\"generation\":$partition_generation,\"status\":\"PASSED\"}" "http://127.0.0.1:$runner_port/v1/runner/leases/$partition_lease/complete")
[ "$stale_code" = 409 ]
[ "$(status "$partitioned")" = ABORTED ]

if grep -F integration-token "$tmp"/*.log >/dev/null 2>&1; then echo "runner token leaked" >&2; exit 1; fi
test -z "$(docker ps -q --filter label=forgeci.managed=true)"
echo "remote runner integration passed"
