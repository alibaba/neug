# Transaction Model

NeuG provides the same user-visible transaction guarantees in both deployment
modes, but it does not use the same transaction model to provide them. This page
explains the different concurrency and persistence mechanisms used by Embedded
(AP) and Service (TP) mode. Applications should rely on the guarantees rather
than these implementation details. For normal usage, start with
[Transaction Management](transaction.mdx),
[Explicit Transactions](explicit_transactions.mdx), and
[Checkpoints](checkpoint.md).

## Shared transaction contract

Both Embedded (AP) and Service (TP) mode use private transaction views:

- A read-only transaction pins a consistent read view for its lifetime and
  rejects writes.
- A read-write transaction owns a private copy-on-write (COW) view. It reads its
  own successful writes, while other transactions continue to see committed
  state.
- Commit publishes all changes atomically. Rollback, an unsuccessful statement,
  or an uncommitted transaction never publishes partial COW state.
- Ordinary commits to a persistent database append redo information to the
  write-ahead log (WAL). On restart, NeuG restores the latest checkpoint and
  replays later committed WAL records.

The deployment modes differ in how they coordinate concurrent work and in
whether they support file and bulk statements.

## Embedded mode (AP)

Embedded mode favors predictable local and analytical execution. Read
operations may share access, while an operation that changes data, schema, or
indexes obtains exclusive write admission. An explicit read-write transaction
holds that admission until it commits, rolls back, or its connection closes.

### Ordinary writes

NeuG applies an ordinary write to the transaction's private COW view and records
the committed change in the WAL. Only after commit succeeds does NeuG publish
the view. A manual checkpoint is not required after each ordinary write.

### File and bulk operations

Embedded mode is the only mode that supports `LOAD FROM` and `COPY` statements:

- `LOAD FROM` reads external data for a query and does not modify persistent
  database state.
- `COPY ... TO` exports data and does not modify persistent database state.
- `COPY TEMP` builds temporary connection-scoped data and is not durable.
- A persistent `COPY ... FROM` changes database state and uses exclusive
  admission for the complete operation.

For a persistent `COPY ... FROM`, NeuG prepares and validates the imported data
in a private cloned view. If loading or validation fails, NeuG discards that
view and keeps the previously published database unchanged. On success, NeuG
creates and durably publishes a checkpoint containing the import before it
replaces the current view and reports success. This pessimistic admission plus
publish-after-checkpoint sequence preserves atomicity and durability even for a
large import.

## Service mode (TP)

Service mode is designed for concurrent application requests. It uses
multi-version concurrency control (MVCC) so reads can continue on a consistent
snapshot while other transactions commit.

Read and insert work can proceed concurrently. Operations that update existing
state, change schema, or change indexes are serialized through an update lease.
An explicit read-write transaction retains the required lease until commit or
rollback. Its changes remain private until commit publishes them.

Each successful ordinary write is recorded in the WAL. Service-mode checkpoint
maintenance waits for in-flight work, temporarily prevents new transactions
from starting, publishes the new checkpoint, and rotates execution-slot WAL
writers to the new epoch before admitting more work.

Service mode rejects `LOAD FROM` and every `COPY` statement. Long-running file
I/O and bulk database replacement therefore stay outside the online TP request
path. Prepare or import the persistent database in Embedded mode, then open it
in Service mode for concurrent workloads.

## Checkpoint publication

A manual `CHECKPOINT` takes exclusive maintenance control and waits for
in-flight work to finish. NeuG compacts live state, writes a complete manifest,
makes the manifest and its WAL epoch durable, and then atomically replaces the
`CURRENT` selector. Only after publication does normal transaction admission
resume.

Dirty graph and index modules receive new immutable objects. Clean modules may
continue to reference existing objects, so a full checkpoint marks a complete
runtime lifecycle boundary without necessarily rewriting every object.

Persistent Embedded-mode `COPY ... FROM` uses a narrower private-COW checkpoint
path. It publishes the prepared import atomically without destructively dumping
or reopening the live graph. `LOAD FROM`, `COPY ... TO`, and `COPY TEMP` do not
use this persistent publication path because they do not commit durable graph
state.

## On-disk layout

A persistent database uses one manifest selector, immutable objects shared by
manifests, one WAL epoch per manifest ID, and a temporary workspace for each
database open:

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

`CURRENT` is the publication selector. Its content is the selected manifest's
decimal ID followed by one newline (for example, `3\n`), and NeuG replaces it
atomically. A published manifest contains `v`, `base_ts`, `schema`, and
`modules`, and may also contain `scalars`. Module descriptors contain object IDs
rather than absolute paths. The manifest and its WAL epoch use the same ID.

`base_ts` is the highest transaction timestamp already represented by the
manifest. Recovery replays the selected WAL epoch beginning after that
timestamp. A full checkpoint resets the transaction timeline and uses
`base_ts=0` when the database reopens.

Checkpoint objects are immutable and may be referenced by more than one
manifest. Runtime workspaces are not checkpoint data. Each database open owns
one `runtime/open-<id>/` directory and removes only its own unpinned workspace
when it closes.

Do not edit `CURRENT` while a process is using the database. Selecting another
manifest is an offline recovery operation: stop every process, back up
`checkpoint/` and `wal/`, and select only a fully persisted, compatible
manifest whose objects and same-ID WAL epoch are complete. Selecting an
inconsistent or older epoch can make recovery fail or discard later committed
state.

## Recovery and checkpoint failures

Startup opens the manifest selected by `CURRENT` and replays committed WAL
records not already represented by it. Incomplete checkpoint work that was
never published through `CURRENT` is ignored.

Before a destructive checkpoint dump begins, a failure leaves the open
database usable. During database close, `Close()` reports that failure so the
caller can correct it and retry. If a shutdown checkpoint fails after consuming
live graph state, the current open instance cannot safely continue: NeuG
finishes cleanup, marks it closed, and reports the error. A later fresh open can
still recover from the previously published checkpoint and committed WAL.

## Garbage collection and disk reclamation

Garbage collection removes retired manifests, WAL epochs, immutable objects,
and abandoned runtime workspaces only when they are neither current nor
retained by a live checkpoint reference. It runs on a read-write database open,
after a successful manual `CHECKPOINT`, and when the database closes.

Deleting rows or dropping tables therefore does not immediately shrink disk
usage. Read-only opens never run garbage collection, so the next read-write
open reclaims stale workspaces left by crashed read-only processes.

## Upgrading legacy checkpoint directories

If `CURRENT` is absent, the first read-write open upgrades the newest valid
released v1 `checkpoint-N` generation. NeuG imports its immutable snapshot
files, preserves the generation as the new manifest and WAL epoch ID, publishes
a v2 manifest, and then performs normal recovery.

The legacy directories remain unchanged until the new `CURRENT` is durable. A
crash before publication leaves the legacy checkpoint usable and the next
read-write open retries. After a successful open and recovery, normal garbage
collection removes the legacy `checkpoint-N` and `checkpoint-N.next`
directories. A legacy-only database must be opened once in read-write mode
before it can be opened read-only. Unsupported legacy metadata versions are
rejected rather than guessed.
