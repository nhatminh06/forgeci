# ForgeCI runs ForgeCI

ForgeCI dogfoods its distributed execution path. GitHub Actions supplies a
fresh checkout and a bootstrap host; it does not schedule the project DAG.
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

A bootstrap machine is unavoidable: GitHub Actions provides checkout and an
execution host, while ForgeCI owns the CI DAG and determines its result. M11
does not add GitHub webhooks, repository registration, ForgeCI Git clone/fetch,
GitHub Checks reporting, or GitHub App authentication; those are M12 concerns.
