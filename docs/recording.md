# ForgeCI recording plan

This run sheet produces one truthful 3 minute 45 second portfolio demo. It uses
the real public showcase, real ForgeCI processes, and committed evidence. Cuts
remove waiting; they do not replace command execution or results.

Do not add a video link to the README or showcase until the finished recording
has a stable URL.

## Setup

- Record at 1920 x 1080.
- Use a terminal font of at least 18 px.
- Keep browser zoom between 90 and 100 percent.
- Hide notifications, credentials, personal directories, and unrelated tabs.
- Pre-pull `postgres:17-alpine` and `alpine:3.20`.
- Build the three ForgeCI binaries before recording.
- Start from a clean demo environment.
- Prepare short captions in advance. Do not use generated narration, fake
  typing, or simulated terminal playback.

Run before recording:

```bash
docker pull postgres:17-alpine
docker pull alpine:3.20
mkdir -p build
go build -o build/forge ./cmd/forge
go build -o build/forge-server ./cmd/forge-server
go build -o build/forge-runner ./cmd/forge-runner
./tools/demo/stop.sh || true
```

## Shot list

### 0:00 to 0:15 | Project

Open the [public showcase](https://nhatminh06.github.io/forgeci/). Show the
title, pipeline blueprint, and captured-run heading without scrolling through
the entire page.

Caption: `A distributed CI engine built from scratch in Go.`

### 0:15 to 0:35 | Architecture

Show `architecture.png` at a readable scale. Keep the SCM boundary, PostgreSQL
control plane, remote runners, and storage boundaries visible.

Caption: `PostgreSQL control plane + fenced leases + remote runners + immutable source`

### 0:35 to 0:55 | Start the environment

Run:

```bash
./tools/demo/run.sh
./tools/demo/status.sh
```

Keep `PostgreSQL READY`, `forge-server READY`, `runner-a ONLINE`, and
`runner-b ONLINE` visible.

Caption: `Two real ForgeCI runner processes.`

### 0:55 to 1:15 | Show the DAG

Open `forge.yaml`. Briefly show `format`, `vet`, `unit`, `race`, `build`,
`binary-smoke`, and `docker-smoke`. Do not read the YAML line by line.

Caption: `ForgeCI is about to run its own repository pipeline.`

### 1:15 to 1:40 | Submit

Use the commands from the [demo guide](demo.md):

```bash
SERVER=$(sed -n 's/^FORGECI_SELF_SERVER=//p' .forgeci-demo/state)
RUN_ID=$(./build/forge submit --quiet --server "$SERVER" --file forge.yaml --jobs 4)
printf 'RUN_ID=%s\n' "$RUN_ID"
./build/forge wait "$RUN_ID" --server "$SERVER" --timeout 15m
```

Record the real submission and run ID. Cut the middle of the wait, then show
its real completion.

### 1:40 to 2:05 | Distributed execution

Run:

```bash
./build/forge inspect "$RUN_ID" --server "$SERVER"
```

Keep job names and both runner identities readable.

Caption: `One DAG. Two runners. PostgreSQL-backed job leases.`

### 2:05 to 2:20 | Durable logs

Run:

```bash
./build/forge logs "$RUN_ID" --job docker-smoke --server "$SERVER"
```

Show `forgeci-docker-smoke-stdout` and `forgeci-docker-smoke-stderr`.

Caption: `stdout and stderr persisted through the control plane.`

### 2:20 to 2:40 | Artifact and cache proof

Show the relevant output from a real `tools/demo/capture.sh` run. Keep these
assertions visible:

```text
self-binaries verified
cache reused
same source digest
```

Caption: `Artifacts cross runner boundaries. Unchanged source reuses cache state.`

Do not claim a measured performance improvement.

### 2:40 to 3:00 | Failure semantics

Show the captured failure result and persisted failure marker:

```text
independent-pass    PASSED
intentional-fail    FAILED
blocked-dependent   BLOCKED
forgeci-intentional-failure
```

Caption: `Independent work continues. Dependent work blocks after failure.`

### 3:00 to 3:20 | Correctness

Return to the showcase correctness ledger. Show exact revision execution,
fenced ownership, delivery idempotency, and durable GitHub Check state.

Caption: `These properties are backed by integration and mutation tests.`

Do not expose or simulate GitHub App credentials.

### 3:20 to 3:35 | Captured evidence

Show the public captured self-hosting section, including its runner placement.

Caption: `The public site shows captured evidence, not fake live status.`

### 3:35 to 3:45 | End

Finish on `ForgeCI` and `github.com/nhatminh06/forgeci`. Use no animated
outro.

## Review before publishing

Watch the complete export once and confirm:

- duration is no longer than 4 minutes;
- terminal and browser text remain readable at normal playback size;
- no credential, token, private URL, hostname, or personal path is visible;
- every command and result came from the recorded ForgeCI run;
- captions make no claim beyond the visible evidence;
- cuts remove waiting but do not imply commands that were not run; and
- the failure fixture and captured evidence match the repository.

Publish through a stable portfolio-appropriate host such as YouTube or a
GitHub release asset. Do not commit the video file to Git history. Once the URL
is final, add a `Demo video` link to the README link row and the showcase.
