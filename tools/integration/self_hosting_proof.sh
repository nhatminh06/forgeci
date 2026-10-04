#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == -h || ${1:-} == --help ]]; then
  echo 'usage: self_hosting_proof.sh'
  echo 'Run the self-hosting checks in an environment prepared by self_hosting_common.sh.'
  exit 0
fi

required_variables=(
  FORGECI_SELF_REPO
  FORGECI_SELF_ROOT
  FORGECI_SELF_BIN_DIR
  FORGECI_SELF_SERVER
  FORGECI_SELF_PG
  FORGECI_SELF_COMMIT
)
for variable in "${required_variables[@]}"; do
  if [[ -z ${!variable:-} ]]; then
    printf '%s is required\n' "$variable" >&2
    exit 2
  fi
done
if ! command -v jq >/dev/null 2>&1; then
  echo 'missing required command: jq' >&2
  exit 2
fi

forge="$FORGECI_SELF_BIN_DIR/forge"
source_dir="$FORGECI_SELF_ROOT/source"
evidence_dir=${FORGECI_SELF_EVIDENCE_DIR:-}
output_dir=${evidence_dir:-$FORGECI_SELF_ROOT}

fail() {
  printf 'self-hosting proof failure: %s\n' "$*" >&2
  exit 1
}

db() {
  docker exec "$FORGECI_SELF_PG" psql -U postgres -d forgeci -At -F $'\t' -c "$1"
}

if [[ -n "$evidence_dir" ]]; then
  mkdir -p "$evidence_dir"
  chmod 700 "$evidence_dir"
fi

"$forge" runners --server "$FORGECI_SELF_SERVER" >"$output_dir/runners.txt"
if [[ $(grep -c ONLINE "$output_dir/runners.txt") -lt 2 ]]; then
  fail 'two online runners were not observed'
fi

run=$("$forge" submit --quiet --server "$FORGECI_SELF_SERVER" --file forge.yaml --jobs 4)
[[ -n "$run" ]] || fail 'empty run ID'
printf 'RUN_ID=%s\n' "$run"
"$forge" wait "$run" --server "$FORGECI_SELF_SERVER" --timeout 15m || fail 'self-hosting run failed'

inspect=$("$forge" inspect "$run" --server "$FORGECI_SELF_SERVER")
if [[ -n "$evidence_dir" ]]; then
  printf '%s\n' "$inspect" >"$evidence_dir/inspect.txt"
fi
for job in format vet unit race build binary-smoke docker-smoke; do
  grep -q "^$job[[:space:]]*PASSED" <<<"$inspect" || fail "job $job did not pass"
done

mapping=$(db "SELECT j.job_name,r.name FROM job_runs j JOIN runners r ON r.id=j.runner_id WHERE j.run_id='$run' ORDER BY j.job_name")
for job in format vet unit race build binary-smoke docker-smoke; do
  grep -q "^$job"$'\t' <<<"$mapping" || fail "missing runner mapping for $job"
done
distinct_runners=$(cut -f2 <<<"$mapping" | sort -u | wc -l | tr -d ' ')
[[ "$distinct_runners" -ge 2 ]] || fail 'self-hosting run used fewer than two runners'
if [[ -n "$evidence_dir" ]]; then
  printf 'JOB\tRUNNER\n%s\n' "$mapping" >"$evidence_dir/job-placement.txt"
fi

docker_log=$("$forge" logs "$run" --job docker-smoke --server "$FORGECI_SELF_SERVER")
grep -q forgeci-docker-smoke-stdout <<<"$docker_log" || fail 'stdout marker missing'
grep -q forgeci-docker-smoke-stderr <<<"$docker_log" || fail 'stderr marker missing'
if [[ -n "$evidence_dir" ]]; then
  printf '%s\n' "$docker_log" >"$evidence_dir/docker-smoke.log"
fi
for job in unit build; do
  "$forge" logs "$run" --job "$job" --server "$FORGECI_SELF_SERVER" >/dev/null || fail "durable $job logs unavailable"
done

artifacts=$("$forge" artifacts "$run" --server "$FORGECI_SELF_SERVER")
grep -q self-binaries <<<"$artifacts" || fail 'self-binaries artifact missing'
artifact_file="$FORGECI_SELF_ROOT/downloads/self-binaries-$run.tar.gz"
artifact_extract="$FORGECI_SELF_ROOT/downloads/extracted-$run"
"$forge" artifact download "$run" build self-binaries \
  --output "$artifact_file" \
  --server "$FORGECI_SELF_SERVER" >/dev/null
[[ -s "$artifact_file" ]] || fail 'downloaded artifact is empty'
mkdir "$artifact_extract"
tar -xzf "$artifact_file" -C "$artifact_extract"
for artifact_entry in forge forge-server forge-runner build-marker.txt; do
  [[ -f "$artifact_extract/dist/$artifact_entry" ]] || fail "artifact missing $artifact_entry"
done
artifact_verified=true
if [[ -n "$evidence_dir" ]]; then
  printf '%s\n' "$artifacts" >"$evidence_dir/artifacts.txt"
fi

[[ $(db "SELECT count(*) FROM job_log_chunks WHERE run_id='$run'") -gt 0 ]] || fail 'durable log chunks missing'
source1=$(db "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$run'")
[[ -n "$source1" ]] || fail 'source digest missing'
"$forge" cache list --server "$FORGECI_SELF_SERVER" | grep -q forgeci-self-unit-gocache-v1 || fail 'cache entry missing'
cache1=$(db "SELECT content_sha256,blob_sha256,last_accessed_at FROM cache_entries WHERE workspace='$source_dir' AND cache_key='forgeci-self-unit-gocache-v1' AND deleted_at IS NULL")
[[ -n "$cache1" ]] || fail 'cache metadata missing'

run2=$("$forge" submit --quiet --server "$FORGECI_SELF_SERVER" --file forge.yaml --jobs 4)
printf 'RUN_2_ID=%s\n' "$run2"
"$forge" wait "$run2" --server "$FORGECI_SELF_SERVER" --timeout 15m || fail 'second self-hosting run failed'
source2=$(db "SELECT source_snapshot_sha256 FROM pipeline_runs WHERE id='$run2'")
[[ "$source1" == "$source2" ]] || fail 'unchanged committed source produced a different digest'
cache2=$(db "SELECT content_sha256,blob_sha256,last_accessed_at FROM cache_entries WHERE workspace='$source_dir' AND cache_key='forgeci-self-unit-gocache-v1' AND deleted_at IS NULL")
[[ "$cache1" != "$cache2" ]] || fail 'cache access metadata did not advance'
cache_reused=true

failure=$("$forge" submit --quiet --server "$FORGECI_SELF_SERVER" \
  --file tools/integration/fixtures/self_host_failure.yaml \
  --jobs 2)
printf 'FAILURE_RUN_ID=%s\n' "$failure"
set +e
"$forge" wait "$failure" --server "$FORGECI_SELF_SERVER" --timeout 5m >/dev/null
wait_status=$?
set -e
[[ "$wait_status" -eq 1 ]] || fail "failure run wait returned $wait_status, want 1"

failure_inspect=$("$forge" inspect "$failure" --server "$FORGECI_SELF_SERVER")
grep -q '^independent-pass[[:space:]]*PASSED' <<<"$failure_inspect" || fail 'independent job did not pass'
grep -q '^intentional-fail[[:space:]]*FAILED' <<<"$failure_inspect" || fail 'intentional job did not fail'
grep -q '^blocked-dependent[[:space:]]*BLOCKED' <<<"$failure_inspect" || fail 'dependent job was not blocked'
failure_log=$("$forge" logs "$failure" --job intentional-fail --server "$FORGECI_SELF_SERVER")
grep -q forgeci-intentional-failure <<<"$failure_log" || fail 'failure log marker missing'

if [[ -n "$evidence_dir" ]]; then
  printf '%s\n' "$failure_inspect" >"$evidence_dir/failure-inspect.txt"
  printf '%s\n' "$failure_log" >"$evidence_dir/failure.log"

  jobs_json=$(
    while IFS=$'\t' read -r name runner; do
      status=$(awk -v job="$name" '$1 == job { print $2 }' <<<"$inspect")
      jq -cn \
        --arg name "$name" \
        --arg state "$status" \
        --arg runner "$runner" \
        '{name:$name,state:$state,runner:$runner}'
    done <<<"$mapping" | jq -s .
  )
  runners_json=$(cut -f2 <<<"$mapping" | sort -u | jq -R . | jq -s .)

  jq -n \
    --arg commit "$FORGECI_SELF_COMMIT" \
    --arg run_id "$run" \
    --arg second_run_id "$run2" \
    --arg failure_run_id "$failure" \
    --arg source_digest "$source1" \
    --argjson runners "$runners_json" \
    --argjson jobs "$jobs_json" \
    --argjson artifact_verified "$artifact_verified" \
    --argjson cache_reused "$cache_reused" \
    '{
      schema_version: 1,
      capture_kind: "self-hosting",
      forgeci_commit: $commit,
      run_id: $run_id,
      second_run_id: $second_run_id,
      failure_run_id: $failure_run_id,
      source_digest: $source_digest,
      status: "PASSED",
      runners: $runners,
      jobs: $jobs,
      artifact: {
        name: "self-binaries",
        verified: $artifact_verified,
        assertion: "downloaded archive contained forge, forge-server, forge-runner, and build-marker.txt"
      },
      cache: {
        key: "forgeci-self-unit-gocache-v1",
        reused: $cache_reused,
        assertion: "unchanged source digest and advancing cache access metadata across two runs"
      },
      logs: {
        job: "docker-smoke",
        stdout_marker: "forgeci-docker-smoke-stdout",
        stderr_marker: "forgeci-docker-smoke-stderr",
        verified: true
      },
      failure: {
        independent_pass: "PASSED",
        intentional_fail: "FAILED",
        blocked_dependent: "BLOCKED",
        log_marker: "forgeci-intentional-failure"
      }
    }' >"$evidence_dir/summary.json"

  (
    cd "$evidence_dir"
    sha256sum \
      artifacts.txt \
      docker-smoke.log \
      failure-inspect.txt \
      failure.log \
      inspect.txt \
      job-placement.txt \
      runners.txt \
      summary.json >manifest.txt
  )
fi

printf 'ForgeCI commit: %s\n' "$FORGECI_SELF_COMMIT"
printf 'Source SHA-256: %s\n' "$source1"
printf 'Distinct runners: %s\n' "$distinct_runners"
printf 'Artifact: self-binaries (verified)\n'
printf 'Cache: forgeci-self-unit-gocache-v1 (reused)\n'
printf 'Durable log: docker-smoke stdout/stderr markers verified\n'
printf 'Failure: independent-pass=PASSED intentional-fail=FAILED blocked-dependent=BLOCKED\n'
if [[ -n "$evidence_dir" ]]; then
  printf 'Evidence: %s\n' "$evidence_dir"
fi
echo 'ForgeCI self-hosting proof passed'
