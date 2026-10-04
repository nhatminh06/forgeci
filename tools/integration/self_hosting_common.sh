#!/usr/bin/env bash

# Shared lifecycle for the self-hosting integration gate and interactive demo.
# Callers own traps, state persistence, and presentation.

forgeci_self_require() {
  local command
  for command in git go docker curl tar od grep sed; do
    command -v "$command" >/dev/null 2>&1 || {
      printf 'missing required command: %s\n' "$command" >&2
      return 1
    }
  done
  docker info >/dev/null 2>&1 || {
    echo 'Docker is not available to the current user' >&2
    return 1
  }
}

forgeci_self_prepare() {
  : "${FORGECI_SELF_REPO:?FORGECI_SELF_REPO is required}"
  : "${FORGECI_SELF_ROOT:?FORGECI_SELF_ROOT is required}"
  : "${FORGECI_SELF_BIN_DIR:?FORGECI_SELF_BIN_DIR is required}"
  mkdir -p "$FORGECI_SELF_ROOT"/{snapshots,artifacts,cache,server,runner-a,runner-b,downloads,source}
  mkdir -p "$FORGECI_SELF_BIN_DIR"
  FORGECI_SELF_COMMIT=$(git -C "$FORGECI_SELF_REPO" rev-parse HEAD)
  git -C "$FORGECI_SELF_REPO" archive HEAD | tar -x -C "$FORGECI_SELF_ROOT/source"
  local required generated
  for required in forge.yaml go.mod cmd/forge cmd/forge-server cmd/forge-runner tools/integration/fixtures/self_host_failure.yaml; do
    [[ -e "$FORGECI_SELF_ROOT/source/$required" ]] || { printf 'isolated committed source is missing %s\n' "$required" >&2; return 1; }
  done
  for generated in .forgeci-cache dist binary-input docker-input build; do
    [[ ! -e "$FORGECI_SELF_ROOT/source/$generated" ]] || { printf 'isolated committed source contains generated path %s\n' "$generated" >&2; return 1; }
  done
  local marker="$FORGECI_SELF_BIN_DIR/.forgeci-demo-commit"
  if [[ ${FORGECI_SELF_REUSE_BINARIES:-0} == 1 ]] &&
    [[ -x "$FORGECI_SELF_BIN_DIR/forge" && -x "$FORGECI_SELF_BIN_DIR/forge-server" && -x "$FORGECI_SELF_BIN_DIR/forge-runner" ]] &&
    [[ -f "$marker" && $(<"$marker") == "$FORGECI_SELF_COMMIT" ]]; then
    FORGECI_SELF_BINARIES_REUSED=1
  else
    GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$FORGECI_SELF_BIN_DIR/forge" "$FORGECI_SELF_REPO/cmd/forge"
    GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$FORGECI_SELF_BIN_DIR/forge-server" "$FORGECI_SELF_REPO/cmd/forge-server"
    GOCACHE=${GOCACHE:-/tmp/forgeci-go-cache} go build -o "$FORGECI_SELF_BIN_DIR/forge-runner" "$FORGECI_SELF_REPO/cmd/forge-runner"
    printf '%s\n' "$FORGECI_SELF_COMMIT" >"$marker"
    FORGECI_SELF_BINARIES_REUSED=0
  fi
  FORGECI_SELF_API_PORT=${FORGECI_SELF_API_PORT:-$((18080 + (RANDOM % 1000)))}
  FORGECI_SELF_RUNNER_PORT=${FORGECI_SELF_RUNNER_PORT:-$((20080 + (RANDOM % 1000)))}
  FORGECI_SELF_SERVER="http://127.0.0.1:$FORGECI_SELF_API_PORT"
  FORGECI_SELF_RUNNER_ENDPOINT="http://127.0.0.1:$FORGECI_SELF_RUNNER_PORT"
  FORGECI_SELF_PG=${FORGECI_SELF_PG:-"forgeci-self-$$-${RANDOM}-postgres"}
  FORGECI_SELF_TOKEN_FILE="$FORGECI_SELF_ROOT/runner-token"
  od -An -N24 -tx1 /dev/urandom | tr -d ' \n' >"$FORGECI_SELF_TOKEN_FILE"
  printf '\n' >>"$FORGECI_SELF_TOKEN_FILE"
  chmod 600 "$FORGECI_SELF_TOKEN_FILE"
  export FORGECI_SELF_COMMIT FORGECI_SELF_BINARIES_REUSED FORGECI_SELF_API_PORT FORGECI_SELF_RUNNER_PORT
  export FORGECI_SELF_SERVER FORGECI_SELF_RUNNER_ENDPOINT FORGECI_SELF_PG FORGECI_SELF_TOKEN_FILE
}

forgeci_self_wait_postgres() {
  local attempt
  for attempt in $(seq 1 300); do
    if [[ $(docker inspect -f '{{.State.Running}}' "$FORGECI_SELF_PG" 2>/dev/null || true) == true ]] &&
      [[ $(docker exec "$FORGECI_SELF_PG" psql -U postgres -d forgeci -At -c 'SELECT 1' 2>/dev/null || true) == 1 ]]; then return 0; fi
    sleep .2
  done
  return 1
}

forgeci_self_spawn() {
  local log=$1
  shift
  if [[ ${FORGECI_SELF_DETACH:-0} == 1 ]]; then
    nohup setsid "$@" >"$log" 2>&1 </dev/null &
  else
    "$@" >"$log" 2>&1 &
  fi
  FORGECI_SELF_SPAWN_PID=$!
}

forgeci_self_start() {
  : "${FORGECI_SELF_SERVER:?environment is not prepared}"
  local token attempt
  token=$(tr -d '\n' <"$FORGECI_SELF_TOKEN_FILE")
  docker run -d --name "$FORGECI_SELF_PG" --label forgeci.self-hosting=true --label "forgeci.self-hosting.root=$FORGECI_SELF_ROOT" \
    -e POSTGRES_PASSWORD=forgeci -e POSTGRES_DB=forgeci -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
  FORGECI_SELF_DB_PORT=$(docker port "$FORGECI_SELF_PG" 5432/tcp | sed 's/.*://')
  forgeci_self_wait_postgres || return 1
  forgeci_self_spawn "$FORGECI_SELF_ROOT/server/server.log" \
    "$FORGECI_SELF_BIN_DIR/forge-server" --execution-mode remote --listen "127.0.0.1:$FORGECI_SELF_API_PORT" \
    --runner-listen "127.0.0.1:$FORGECI_SELF_RUNNER_PORT" --runner-token-file "$FORGECI_SELF_TOKEN_FILE" \
    --workspace "$FORGECI_SELF_ROOT/source" --snapshot-dir "$FORGECI_SELF_ROOT/snapshots" \
    --artifact-dir "$FORGECI_SELF_ROOT/artifacts" --cache-dir "$FORGECI_SELF_ROOT/cache" \
    --database-url "postgres://postgres:forgeci@127.0.0.1:$FORGECI_SELF_DB_PORT/forgeci?sslmode=disable"
  FORGECI_SELF_SERVER_PID=$FORGECI_SELF_SPAWN_PID
  for attempt in $(seq 1 100); do
    curl -fsS "$FORGECI_SELF_SERVER/healthz" >/dev/null 2>&1 && break
    kill -0 "$FORGECI_SELF_SERVER_PID" 2>/dev/null || return 1
    sleep .1
  done
  curl -fsS "$FORGECI_SELF_SERVER/healthz" >/dev/null || return 1
  forgeci_self_spawn "$FORGECI_SELF_ROOT/runner-a/runner.log" env FORGECI_RUNNER_TOKEN="$token" \
    "$FORGECI_SELF_BIN_DIR/forge-runner" --server "$FORGECI_SELF_RUNNER_ENDPOINT" \
    --workspace-root "$FORGECI_SELF_ROOT/runner-a/work" --state-dir "$FORGECI_SELF_ROOT/runner-a/state" \
    --name runner-a --max-parallel 1
  FORGECI_SELF_RUNNER_A_PID=$FORGECI_SELF_SPAWN_PID
  forgeci_self_spawn "$FORGECI_SELF_ROOT/runner-b/runner.log" env FORGECI_RUNNER_TOKEN="$token" \
    "$FORGECI_SELF_BIN_DIR/forge-runner" --server "$FORGECI_SELF_RUNNER_ENDPOINT" \
    --workspace-root "$FORGECI_SELF_ROOT/runner-b/work" --state-dir "$FORGECI_SELF_ROOT/runner-b/state" \
    --name runner-b --max-parallel 1
  FORGECI_SELF_RUNNER_B_PID=$FORGECI_SELF_SPAWN_PID
  for attempt in $(seq 1 100); do
    [[ $("$FORGECI_SELF_BIN_DIR/forge" runners --server "$FORGECI_SELF_SERVER" 2>/dev/null | grep -c ONLINE || true) -ge 2 ]] && break
    sleep .1
  done
  [[ $("$FORGECI_SELF_BIN_DIR/forge" runners --server "$FORGECI_SELF_SERVER" | grep -c ONLINE) -ge 2 ]] || return 1
  export FORGECI_SELF_DB_PORT FORGECI_SELF_SERVER_PID FORGECI_SELF_RUNNER_A_PID FORGECI_SELF_RUNNER_B_PID
}

forgeci_self_stop() {
  local pid attempt alive
  for pid in "${FORGECI_SELF_RUNNER_A_PID:-}" "${FORGECI_SELF_RUNNER_B_PID:-}" "${FORGECI_SELF_SERVER_PID:-}"; do [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true; done
  for attempt in $(seq 1 50); do
    alive=0
    for pid in "${FORGECI_SELF_RUNNER_A_PID:-}" "${FORGECI_SELF_RUNNER_B_PID:-}" "${FORGECI_SELF_SERVER_PID:-}"; do
      [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null || alive=1
    done
    [[ "$alive" -eq 0 ]] && break
    sleep .1
  done
  for pid in "${FORGECI_SELF_RUNNER_A_PID:-}" "${FORGECI_SELF_RUNNER_B_PID:-}" "${FORGECI_SELF_SERVER_PID:-}"; do
    [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  done
  for pid in "${FORGECI_SELF_RUNNER_A_PID:-}" "${FORGECI_SELF_RUNNER_B_PID:-}" "${FORGECI_SELF_SERVER_PID:-}"; do [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true; done
  [[ -z ${FORGECI_SELF_PG:-} ]] || docker rm -f "$FORGECI_SELF_PG" >/dev/null 2>&1 || true
}

forgeci_self_diagnostics() {
  printf '%s\n' '--- forge-server ---' >&2; sed -n '1,240p' "$FORGECI_SELF_ROOT/server/server.log" >&2 2>/dev/null || true
  printf '%s\n' '--- runner-a ---' >&2; sed -n '1,240p' "$FORGECI_SELF_ROOT/runner-a/runner.log" >&2 2>/dev/null || true
  printf '%s\n' '--- runner-b ---' >&2; sed -n '1,240p' "$FORGECI_SELF_ROOT/runner-b/runner.log" >&2 2>/dev/null || true
  [[ -z ${FORGECI_SELF_PG:-} ]] || docker inspect -f 'container={{.State.Status}} exit={{.State.ExitCode}}' "$FORGECI_SELF_PG" >&2 2>/dev/null || true
}
