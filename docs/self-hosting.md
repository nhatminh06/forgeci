# ForgeCI runs ForgeCI

ForgeCI dogfoods its distributed execution path. Native GitHub SCM can ingest
the repository and report Checks. The GitHub Actions workflow remains as the
external bootstrap gate until a configured sandbox GitHub App has completed
the live push, pull-request, and failure smoke tests; it does not schedule the
project DAG.
The self-hosting harness builds bootstrap binaries, starts isolated PostgreSQL,
one server, and two remote runners, then submits the root forge.yaml.

The self-pipeline runs format, vet, unit, race, build, binary-smoke, and
docker-smoke. It proves two-runner placement, Docker execution, durable log
retrieval, artifact publication and restore, and cache reuse. RUN1 and RUN2
submit unchanged source and must have identical source digests; cache access
must advance. A separate fixture proves independent work passes, an intentional
failure fails, and its dependent is blocked.

## Isolated committed source

The harness uses git archive HEAD for its server workspace. ForgeCI intentionally
captures untracked files, while dogfooding must prove the exact committed
repository as a fresh CI checkout would. The archive contains no .git directory
and prevents local build outputs and caches from contaminating the source.

Run the same proof locally:

    ./tools/integration/self_hosting.sh

Direct forge run is useful for local execution but does not prove PostgreSQL,
source transport, remote runner leases, durable logs, or cross-runner
artifact/cache paths.

## Boundary and limitations

An execution host is still required. Native SCM now provides signed webhooks,
registration, exact Git fetch, GitHub App authentication, and Checks; ForgeCI
owns the CI DAG and determines its result. The bootstrap workflow is retained
only to avoid removing the existing external gate before live-app verification.
