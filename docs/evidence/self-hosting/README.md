# Captured self-hosting evidence

This directory is one sanitized capture produced by:

```bash
./tools/demo/run.sh
./tools/demo/capture.sh demo-evidence/canonical
./tools/demo/stop.sh
```

[`summary.json`](summary.json) is the machine-readable source for the captured
run shown on GitHub Pages. It records observed run identities, source identity,
runner placement, and job states. Boolean verification fields are written only
after the shared proof driver performs the corresponding assertion:

- the artifact archive is downloaded, extracted, and checked for all expected
  files;
- the second run has the same source digest and advancing cache access metadata;
- persisted stdout and stderr markers are retrieved through `forge logs`;
- the committed failure fixture produces PASS / FAIL / BLOCKED and its log
  marker is retrieved.

The text files preserve compact CLI and assertion evidence. `manifest.txt`
contains their SHA-256 hashes. The capture excludes credentials, hostnames,
ports, personal paths, binaries, database state, source archives, and caches.

This is a static historical capture, not a live ForgeCI environment.
