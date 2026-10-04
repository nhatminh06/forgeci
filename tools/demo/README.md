# Interactive self-hosting demo

The demo uses the same lifecycle library and proof driver as
`tools/integration/self_hosting.sh`. It does not implement a second ForgeCI
environment.

```bash
./tools/demo/run.sh
./tools/demo/status.sh
./tools/demo/capture.sh
./tools/demo/stop.sh
```

`run.sh` archives committed `HEAD` into a unique `/tmp/forgeci-demo.*`
directory, builds or reuses commit-matched binaries in `build/`, starts a
uniquely named PostgreSQL container, and binds the server and runner listener
to loopback-only ports. It does not submit a pipeline unless `--submit` is
given. Runtime coordinates are stored in `.forgeci-demo/state` with mode 600;
the runner credential remains in a separate mode-600 file and is never printed.

`capture.sh` submits the real `forge.yaml` twice and the committed failure
fixture once. It verifies two-runner placement, durable log markers, downloaded
artifact contents, stable source identity, advancing cache access metadata, and
PASS / FAIL / BLOCKED semantics. Its default output is ignored runtime state.
To retain a candidate capture for review, choose a new directory below
`demo-evidence/`:

```bash
./tools/demo/capture.sh demo-evidence/candidate
```

`stop.sh` validates repository identity, temporary-root shape, process command
lines, and the Docker ownership label before it signals or removes anything.
It deletes only the recorded demo processes, container, and temporary root.
