# Source snapshots

Milestone 6 freezes the source of every server-backed run at submission. `forge run` remains a live-workspace operation.

## Canonical manifest

ForgeCI walks with `lstat`, excludes `.git` directories, sorts slash-separated relative paths lexicographically, and records:

- regular files: path, type, permission mode, byte size, and content SHA-256;
- directories: path, type, and permission mode;
- symlinks: path, type, permission mode, and link text.

Untracked files, `.env`, build output, and vendor trees are included. Sockets, FIFOs, devices, and other special nodes fail capture. Symlinks are never followed; only relative targets whose normalized destination stays inside the source root are accepted.

The canonical JSON manifest plus a newline produces the logical `source_snapshot_sha256`. Absolute paths, ownership, user/group names, and timestamps are excluded. Permission bits—including executable bits—are preserved. Ownership, timestamps, xattrs, and ACLs are normalized or omitted.

## Deterministic archive and CAS

Entries are written in manifest order to `tar-gzip-v1`. Tar ownership and names are empty/zero, times are the Unix epoch, and gzip metadata is normalized. SHA-256 over the exact compressed bytes is the blob digest. Logical and blob digests are separate because archive encoding can change without changing the logical source.

Capture writes under `<snapshot-dir>/tmp`, verifies the finished stream, and atomically publishes through a hard-link create-if-absent operation at:

```text
<snapshot-dir>/blobs/sha256/<first-two-source-digest-bytes>/<remaining-bytes>
```

Digest validation is mandatory before path construction. Identical trees reuse one immutable blob, including concurrent submissions. PostgreSQL stores metadata and run references only.

## Safety and guarantees

Runner downloads verify actual byte count and blob digest before safe extraction, then recompute the logical manifest before pipeline execution. Archive traversal, symlink redirection, hard links, unsupported types, excess entries, and excess extracted bytes are rejected. Per-run marker-owned workspaces eliminate state leakage between runs and are cleaned at terminal outcomes or conservatively on runner restart.

Capture is deterministic but not an atomic filesystem snapshot. ForgeCI detects a file whose archived bytes no longer match the size/hash observed during scanning and fails the submission. A filesystem-level atomic view, Git identity/checkout, custom ignore rules, xattrs/ACLs, artifacts, and cache remain outside this milestone.
