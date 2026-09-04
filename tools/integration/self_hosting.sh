#!/usr/bin/env bash
set -euo pipefail
repo=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
root=$(mktemp -d /tmp/forgeci-self-hosting.XXXXXX)
suffix="m11-$$-$RANDOM"
pg="forgeci-$suffix-postgres"
server_pid= runner_a_pid= runner_b_pid=
keep=${FORGECI_DIAGNOSTICS_DIR:-}
cleanup() {
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true; done
  for pid in "$runner_a_pid" "$runner_b_pid" "$server_pid"; do [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true; done
  docker rm -f "$pg" >/dev/null 2>&1 || true
  [[ -n "$keep" ]] || rm -rf "$root"
}
trap cleanup EXIT INT TERM
fail() { echo "self-hosting failure: $*" >&2; cat "$root/server/server.log" "$root/runner-a/runner.log" "$root/runner-b/runner.log" 2>/dev/null || true; docker logs "$pg" 2>/dev/null || true; exit 1; }
mkdir -p "$root"/{bin,snapshots,artifacts,cache,server,runner-a,runner-b,downloads,source}
dogfood_commit=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" archive HEAD | tar -x -C "$root/source"
for required in forge.yaml go.mod cmd/forge cmd/forge-server cmd/forge-runner tools/integration/fixtures/self_host_failure.yaml; do
  [[ -e "$root/source/$required" ]] || fail "isolated source is missing $required"
done
for generated in .forgeci-cache dist binary-input docker-input; do
  [[ ! -e "$root/source/$generated" ]] || fail "isolated source contains generated $generated"
done
api_port=$((43000 + ($$ % 500))); runner_port=$((44000 + ($$ % 500)))
server="http://127.0.0.1:$api_port"; runner_endpoint="http://127.0.0.1:$runner_port"
token=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
printf '%s\n' "$token" >"$root/runner-token"; chmod 600 "$root/runner-token"
go build -o "$root/bin/forge" "$repo/cmd/forge"; go build -o "$root/bin/forge-server" "$repo/cmd/forge-server"; go build -o "$root/bin/forge-runner" "$repo/cmd/forge-runner"
docker run -d --name "$pg" -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
db_port=$(docker port "$pg" 5432/tcp | sed 's/.*://')
for n in $(seq 1 300); do docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null 2>&1 && break; sleep .2; done
docker exec "$pg" pg_isready -U postgres -d forgeci >/dev/null || fail "postgres not ready"
for n in $(seq 1 300); do docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null 2>&1 && break; sleep .2; done
docker exec "$pg" psql -U postgres -d forgeci -At -c 'SELECT 1' >/dev/null || fail "database not ready"
"$root/bin/forge-server" --execution-mode remote --listen "127.0.0.1:$api_port" --runner-listen "127.0.0.1:$runner_port" --runner-token-file "$root/runner-token" --workspace "$root/source" --snapshot-dir "$root/snapshots" --artifact-dir "$root/artifacts" --cache-dir "$root/cache" --database-url "postgres://postgres:forgeci@127.0.0.1:$db_port/forgeci?sslmode=disable" >"$root/server/server.log" 2>&1 & server_pid=$!
for n in $(seq 1 100); do curl -fsS "$server/healthz" >/dev/null 2>&1 && break; sleep .1; done
curl -fsS "$server/healthz" >/dev/null || fail "server not ready"
FORGECI_RUNNER_TOKEN="$token" "$root/bin/forge-runner" --server "$runner_endpoint" --workspace-root "$root/runner-a/work" --state-dir "$root/runner-a/state" --name runner-a --max-parallel 1 >"$root/runner-a/runner.log" 2>&1 & runner_a_pid=$!
FORGECI_RUNNER_TOKEN="$token" "$root/bin/forge-runner" --server "$runner_endpoint" --workspace-root "$root/runner-b/work" --state-dir "$root/runner-b/state" --name runner-b --max-parallel 1 >"$root/runner-b/runner.log" 2>&1 & runner_b_pid=$!
for n in $(seq 1 100); do [[ $("$root/bin/forge" runners --server "$server" 2>/dev/null | grep -c ONLINE || true) -ge 2 ]] && break; sleep .1; done
[[ $("$root/bin/forge" runners --server "$server" | grep -c ONLINE) -ge 2 ]] || fail "runners not online"
run=$("$root/bin/forge" submit --quiet --server "$server" --file forge.yaml --jobs 4)
[[ -n "$run" ]] || fail "empty run id"
"$root/bin/forge" wait "$run" --server "$server" --timeout 15m || fail "dogfood run failed"
inspect=$("$root/bin/forge" inspect "$run" --server "$server")
for job in format vet unit race build binary-smoke docker-smoke; do grep -q "^$job[[:space:]]*PASSED" <<<"$inspect" || fail "job $job did not pass"; done
[[ $(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(DISTINCT runner_id) FROM job_runs WHERE run_id='$run'") -ge 2 ]] || fail "fewer than two runners used"
logs=$("$root/bin/forge" logs "$run" --job docker-smoke --server "$server")
grep -q forgeci-docker-smoke-stdout <<<"$logs"; grep -q forgeci-docker-smoke-stderr <<<"$logs"
for job in unit build; do
  "$root/bin/forge" logs "$run" --job "$job" --server "$server" >/dev/null || fail "durable $job logs unavailable"
done
"$root/bin/forge" artifacts "$run" --server "$server" | grep -q self-binaries
"$root/bin/forge" artifact download "$run" build self-binaries --output "$root/downloads/self-binaries" --server "$server" >/dev/null
[[ -s "$root/downloads/self-binaries" ]]
mkdir "$root/downloads/extracted"
tar -xzf "$root/downloads/self-binaries" -C "$root/downloads/extracted"
for artifact_file in forge forge-server forge-runner build-marker.txt; do
  [[ -f "$root/downloads/extracted/dist/$artifact_file" ]] || fail "artifact missing $artifact_file"
done
[[ $(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(*) FROM job_log_chunks WHERE run_id='$run'") -gt 0 ]] || fail "durable logs missing"
source1=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$run'")
[[ -n "$source1" ]] || fail "missing first snapshot digest"
mapping=$(docker exec "$pg" psql -U postgres -d forgeci -At -F $'\t' -c "SELECT job_name, runner_id FROM job_runs WHERE run_id='$run' ORDER BY job_name")
for job in format vet unit race build binary-smoke docker-smoke; do grep -q "^$job"$'\t' <<<"$mapping" || fail "missing runner mapping for $job"; done
distinct_runners=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT count(DISTINCT runner_id) FROM job_runs WHERE run_id='$run'")
[[ "$distinct_runners" -ge 2 ]] || fail "fewer than two runners used"
"$root/bin/forge" cache list --server "$server" | grep -q forgeci-self-unit-gocache-v1
cache1=$(docker exec "$pg" psql -U postgres -d forgeci -At -F '|' -c "SELECT content_sha256,blob_sha256,last_accessed_at FROM cache_entries WHERE workspace='$root/source' AND cache_key='forgeci-self-unit-gocache-v1' AND deleted_at IS NULL")
[[ -n "$cache1" ]] || fail "missing cache metadata"
cache_access_1=${cache1##*|}
run2=$("$root/bin/forge" submit --quiet --server "$server" --file forge.yaml --jobs 4)
"$root/bin/forge" wait "$run2" --server "$server" --timeout 15m || fail "second dogfood run failed"
source2=$(docker exec "$pg" psql -U postgres -d forgeci -At -c "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$run2'")
[[ "$source1" = "$source2" ]] || fail "source snapshot changed"
cache2=$(docker exec "$pg" psql -U postgres -d forgeci -At -F '|' -c "SELECT content_sha256,blob_sha256,last_accessed_at FROM cache_entries WHERE workspace='$root/source' AND cache_key='forgeci-self-unit-gocache-v1' AND deleted_at IS NULL")
[[ "$cache1" != "$cache2" ]] || fail "cache access timestamp did not advance"
cache_access_2=${cache2##*|}
failure=$("$root/bin/forge" submit --quiet --server "$server" --file tools/integration/fixtures/self_host_failure.yaml --jobs 2)
set +e
"$root/bin/forge" wait "$failure" --server "$server" --timeout 5m
wait_status=$?
set -e
[[ "$wait_status" -eq 1 ]] || fail "failure wait returned $wait_status, want 1"
failure_inspect=$("$root/bin/forge" inspect "$failure" --server "$server")
grep -q '^independent-pass[[:space:]]*PASSED' <<<"$failure_inspect"
grep -q '^intentional-fail[[:space:]]*FAILED' <<<"$failure_inspect"
grep -q '^blocked-dependent[[:space:]]*BLOCKED' <<<"$failure_inspect"
"$root/bin/forge" logs "$failure" --job intentional-fail --server "$server" | grep -q forgeci-intentional-failure
printf 'Tested commit: %s\n\n' "$dogfood_commit"
printf 'RUN1: %s\nRUN2: %s\nFAIL_RUN: %s\n\n' "$run" "$run2" "$failure"
printf 'RUN1 source SHA: %s\nRUN2 source SHA: %s\n\n' "$source1" "$source2"
printf 'JOB\tRUNNER\n%s\n\n' "$mapping"
printf 'Distinct runners: %s\n\n' "$distinct_runners"
printf 'Cache key: forgeci-self-unit-gocache-v1\nCache access 1: %s\nCache access 2: %s\n\n' "$cache_access_1" "$cache_access_2"
printf 'Artifact: self-binaries\n'
echo "ForgeCI self-hosting integration passed"
