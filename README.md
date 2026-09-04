# ForgeCI

ForgeCI is a self-hosted distributed CI engine built to explore scheduling,
remote execution, reproducible source transport, artifacts, caching, durable
logs, failure recovery, and self-hosting.

## Current capabilities

- YAML DAG pipelines with bounded dependency-aware concurrency
- local shell execution and one-container-per-job Docker execution
- PostgreSQL control plane, remote runner inventory, and job-level leases
- immutable source snapshots, durable artifacts, and reusable build cache
- durable and live job logs, including forge logs follow mode
- explicit downstream artifact restoration and cache restore/save
- failure propagation while independent DAG branches continue
- ForgeCI self-hosting with a real distributed control plane

## ForgeCI runs ForgeCI

    GitHub PR or main push
              |
    GitHub Actions bootstrap
              |
    self_hosting.sh
              |
    PostgreSQL + forge-server + two runners
              |
    ForgeCI executes forge.yaml

GitHub Actions supplies checkout and an execution host. ForgeCI owns the
project CI DAG and determines the pipeline result. The root pipeline runs
format, vet, unit, race, build, binary-smoke, and docker-smoke.

The self-hosting harness proves that two runners execute work, artifacts move
build output between jobs, cache is reused across runs, Docker logs are
durably retrieved, and an expected failure DAG behaves correctly. It submits
an isolated git archive of committed HEAD so developer build outputs and
untracked caches cannot affect the proof. Read the
[self-hosting guide](docs/self-hosting.md) for details.

## Quick start

Go 1.27 or newer is required. Docker jobs require a reachable Docker Engine.

    go build -o build/forge ./cmd/forge
    ./build/forge run --file forge.yaml

For durable server-backed execution, see [architecture](docs/architecture.md),
[control plane](docs/control-plane.md), [remote runners](docs/remote-runners.md),
and [pipeline format](docs/pipeline-format.md).

## Security and current boundary

ForgeCI assumes trusted repositories and execution environments. Docker daemon
access is privileged and does not provide hostile-code isolation. M11 does not
implement native GitHub webhooks, repository registration, Git clone/fetch,
GitHub Checks reporting, or GitHub App authentication.
