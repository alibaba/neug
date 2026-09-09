# How It Works

This page explains how NeuG implements transactions and checkpoints internally.
It is intended for advanced users and operators who need to understand the
mechanisms behind the guarantees. For everyday usage, see
[Transaction Management](transaction.mdx) and [Checkpoints](checkpoint.md).

## Transaction model internals

NeuG separates reads from writes using copy-on-write (COW) views:

- A **read-only** transaction pins one read view for its lifetime and rejects
  writes.
- A **read-write** transaction owns a private COW view, reads its own successful
  writes, and publishes its changes only when the commit succeeds. Failed or
  uncommitted COW changes are never published.

**Embedded (AP) admission.** A read-write transaction holds exclusive AP
admission for its whole lifetime: every other AP operation waits until it
commits, rolls back, or its connection closes. Embedded READ_WRITE mode also
allows only one public `Connection` at a time, so AP writes are effectively
serialized.

**Service (TP) update lease.** A read-write transaction holds the TP update
lease. While it holds the lease, no other read-write transaction can begin, and
ordinary inserts and updates wait for it to finish; reads stay concurrent on a
consistent MVCC snapshot. A successful commit appends and publishes one WAL
commit.

**WAL.** Committed ordinary writes append logical redo to the WAL and are
replayed on restart. The current WAL format is sufficient for normal replay, but
its final framing, corruption handling, and commit-unknown recovery rules are
still being hardened. Applications that require a durable materialized
checkpoint after bulk loading should use the checkpoint lifecycle described in
[Checkpoints](checkpoint.md).

## Checkpoint on-disk layout

A persistent database uses one manifest selector, immutable objects shared by
manifests, a WAL epoch per manifest ID, and a temporary workspace per database
open:

```text
data_dir/
├── checkpoint/
│   ├── CURRENT                 # decimal manifest ID + trailing newline
│   ├── manifests/<id>.manifest
│   └── objects/<object-id>
├── wal/<id>/                   # WAL epoch for manifest <id>
└── runtime/open-<id>/          # temporary files for one database open;
                                # <id> is an opaque unique suffix (a UUID)
```

`CURRENT` is the sole publication selector. Its content is the selected
manifest's decimal ID followed by a single trailing newline (for example `3\n`),
written via an atomic rename; operators may inspect or rewrite it manually with
a plain text editor. A published manifest has the required fields `v`,
`base_ts`, `schema`, and `modules`; it may also contain `scalars`. Module
descriptors persist object IDs, not absolute paths. The same ID names the
manifest and its WAL epoch. `base_ts` is the highest transaction timestamp
already represented by the manifest, so recovery replays the selected epoch from
`base_ts + 1`. Full checkpoints use `base_ts=0` and reset the transaction
timeline after reopening.

Checkpoint objects are immutable and may be referenced by several manifests.
Runtime files are not checkpoint data: each database open receives its own
`runtime/open-<id>/` directory (the suffix is an opaque unique ID, not a
timestamp or manifest ID), and closing that database removes only its own
unpinned workspace.

## Checkpoint protocol

A manual `CHECKPOINT` first takes exclusive checkpoint maintenance control and
waits for in-flight work to finish (see
[Concurrency](checkpoint.md#concurrency)). It preserves the existing
full-checkpoint behavior: compact the live graph, destructively dump it, publish
a complete manifest, and reopen the graph and allocators. Only dirty graph and
index modules need new immutable objects; clean module descriptors may continue
to reference existing objects. The manifest and its WAL epoch are made durable
before `CURRENT` is atomically replaced.

Persistent AP-direct `COPY` and batch inserts use a
narrower private-COW protocol. The statement
prepares all changes in a cloned graph, performs the existing consuming
dirty-module dump/reopen only on that clone, then publishes the staging manifest
and replaces the current graph atomically. It does not compact or reopen the
published graph or allocator. Every successful persistent bulk statement
advances the checkpoint ID and rotates active WAL writers to the new epoch.

After a manual checkpoint publishes, NeuG reopens the live graph and allocators
from the new checkpoint. In Service mode, each execution-slot WAL writer is then
rotated onto the new epoch. Finally, the transaction timeline is reset and new
transactions are admitted. These steps all run while the checkpoint barrier is
still held.

Recovery and shutdown checkpoints use the same compacting, destructive dump.
Recovery reopens the graph and allocators before the database starts serving;
shutdown persists without reopening.

Before the destructive dump begins, a shutdown checkpoint failure leaves the
open database usable and `Close()` reports the failure so the caller can correct
it and retry. Once the dump has consumed live graph state, failure is not
retryable for that open instance: `Close()` finishes resource and lock cleanup,
marks the database closed, and rethrows the failure. A later fresh `Open()` can
still use the previously published checkpoint.

"Full" describes the runtime lifecycle boundary; it does not require rewriting
every clean immutable object. Checkpoint disk growth is therefore driven by
rewritten modules plus objects retained by live references. Schedule checkpoints
based on the acceptable replay work after a crash and WAL growth, rather than a
fixed tight interval.

## Garbage collection and disk reclamation

Retired manifests, WAL epochs, immutable objects, and abandoned
`runtime/open-<id>/` workspaces are removed by garbage collection, which runs
only at three points: read-write database open, a successful manual
`CHECKPOINT`, and database close. Deleting rows or dropping tables therefore
does not shrink disk usage until one of those points is reached. Garbage
collection removes manifests, WAL epochs, and objects only when they are neither
current nor retained by a live checkpoint reference.

Read-only opens never run garbage collection. A pure read-only deployment
accumulates the stale `runtime/open-<id>/` workspaces left behind by crashed
read-only processes; the next read-write open reclaims them.

## Upgrading legacy checkpoint directories

When `CURRENT` is absent, the first read-write open automatically upgrades the
newest valid released v1 `checkpoint-N` generation. NeuG imports its immutable
snapshot files into `checkpoint/objects/`, preserves its generation as the new
manifest and WAL epoch ID, and publishes a v2 manifest with `base_ts=0`; normal
recovery then replays every legacy WAL record. Files are hardlinked when safe
and copied otherwise.

The old directories are not changed before the new `CURRENT` is durably
published. A crash before publication leaves the legacy checkpoint usable and
the next read-write open retries. After the database has opened and recovered
successfully, normal garbage collection removes the old `checkpoint-N` and
`checkpoint-N.next` directories, making the upgrade one-way. A legacy-only
database cannot be opened read-only: open it once in read-write mode to perform
the upgrade. Legacy `meta` versions other than v1 are rejected rather than
guessed.
