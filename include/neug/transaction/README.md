# Transactions

> Note: This file documents internal transaction implementation details. For
> application-facing behavior, see
> [Transaction Management](../../../doc/source/transaction/transaction.md).

All transaction objects own their `VersionManager` timestamp and storage
resources through RAII. A successful `Commit()` or an explicit `Abort()`
releases those resources; destroying an active transaction aborts or releases
it. Transaction factories acquire the timestamp before pinning or cloning
graph state so that checkpoint maintenance cannot rotate storage, allocator,
or WAL state between those operations.

## Read Transaction

A `ReadTransaction` is assigned the current committed read timestamp and pins
the current `GraphSnapshotStore` slot with a `SnapshotGuard`. It provides
read-only access to the schema, topology, and properties through that pinned
snapshot. Record-level visibility is limited to timestamps less than or equal
to the transaction timestamp.

`Commit()` and `Abort()` have the same effect: they release the snapshot pin and
the reader lease. `Commit()` always returns `true`. The destructor performs the
same release if the transaction is still active; there is no public
`ReadTransaction::Release()` method.

References backed by the pinned graph, including graph-backed query results,
must not be used after the transaction is committed, aborted, or destroyed.

## Insert Transaction

An `InsertTransaction` receives a unique write timestamp and pins the current
snapshot slot. It buffers vertex and edge insert operations in a local WAL
archive; graph storage is not changed while the operations are being collected.

On a non-empty `Commit()`, the transaction:

1. appends the complete transaction record to the WAL;
2. replays the buffered operations into the live `PropertyGraph` behind the
   pinned slot, tagging every new record with the transaction timestamp; and
3. releases the snapshot pin and marks the write timestamp complete.

The live graph is shared with readers, so insert isolation does not come from a
copy-on-write snapshot. Every read path must filter records by timestamp. A
reader with an older timestamp can therefore share the slot while the insert is
being applied without observing the new records. The committed read watermark
advances only after the transaction has finished and all earlier write
timestamps are also complete.

An empty commit only releases the transaction. `Abort()` discards buffered
operations and completes the timestamp without applying graph changes. The
destructor calls `Abort()` if necessary. `InsertTransaction` does not expose a
general-purpose read interface; its schema and vertex lookup helpers exist to
validate inserts and resolve edge endpoints, including vertices added by the
same transaction.

## Update Transaction

Acquiring an `UpdateTransaction` enters the update-execution phase, blocks new
inserts and updates, and waits for active inserts to finish. Reads remain
allowed. Once the update lease has been acquired, the current `PropertyGraph`
is cloned using copy-on-write and all DML or DDL changes are made to that clone.
The changes are invisible to readers until commit publishes the clone through
`GraphSnapshotStore`.

For a non-empty update, `Commit()` verifies that a snapshot slot is available,
appends the finalized transaction to the WAL, and then enters the update-commit
phase. That phase blocks new reads and writers while the COW snapshot is
published. Existing readers are not drained by an ordinary update commit; they
continue on their pinned older snapshots. Publishing happens before the write
timestamp is marked complete, so a new reader cannot observe the advanced
timestamp with the old snapshot.

An empty commit releases the update lease without publishing a new snapshot.
`Abort()` and the destructor discard the COW clone and complete the timestamp
without making its changes visible.

## Compact Transaction

A `CompactTransaction` enters the exclusive compact phase. It blocks new
transactions and waits for both active inserts and active readers to finish
because compaction rewrites the live graph in place. On commit it writes a
compact WAL record, compacts and rebuilds the pinned graph view, and then
restores `update_state_` to `0`. `Abort()` or destruction also restores the
state without compacting.

## Checkpoint Maintenance

In service mode, a manual checkpoint uses an `UpdateTimestampGuard`, not a
`CompactTransaction`. It first enters update execution and waits for active
inserts, then enters update commit and explicitly drains active readers before
acquiring the checkpoint-maintenance handle. New transactions remain blocked
while the graph, allocator, and WAL state are replaced. A successful checkpoint
resets the timestamp timeline and restores `update_state_` to `0`.

# Version Management

## Visibility

`VersionManager::read_ts_` is the highest contiguous write timestamp that has
finished. Finishing includes commit, abort, and an empty transaction; only a
commit has graph effects. A `ReadTransaction` receives this watermark as its
timestamp, so every earlier committed insert is visible through record-level
timestamp filtering, while later inserts are hidden.

Updates use snapshot publication as well as timestamps. The updated snapshot
is published before its timestamp can advance the read watermark. Readers that
started earlier keep their pinned old snapshot; readers that start afterward
pin the published snapshot and receive a timestamp at least as new as the
update when all preceding writes have finished.

## Synchronization

`VersionManager` coordinates transactions with `update_state_` and active
reader/inserter counters. Its effective states are:

| State | New reads | New inserts | New updates/compact | Existing reads | Existing inserts |
|-------|-----------|-------------|---------------------|----------------|------------------|
| `0` — normal | allowed | allowed | one transition wins | continue | continue |
| `1` — update execution | allowed | blocked | blocked | continue | drained before acquisition completes |
| `2` — update commit | blocked | blocked | blocked | continue for an ordinary update | none |
| `2` — compact/checkpoint | blocked | blocked | blocked | drained before in-place maintenance | drained before maintenance |

Multiple reads and inserts can run concurrently in the normal state. Updates and
compact operations serialize by waiting until they can change `update_state_`
from `0`. New reads use a pre-check/increment/post-check sequence so they cannot
slip into state `2` during the transition.

An ordinary update keeps `update_state_` at `1` during execution and WAL append,
then changes it to `2` only for snapshot publication. A checkpoint additionally
calls `drain_readers()` because it mutates or replaces storage in place.
Compact acquisition enters state `2` directly and drains both readers and
inserters.

## Commit Ordering

Each insert, update, or compact transaction receives a unique write timestamp.
Completed timestamps are tracked in a window, and `read_ts_` advances only
across a contiguous sequence of completed writes. This prevents a later,
faster writer from becoming visible ahead of an earlier transaction.

Insert commit appends WAL before changing the live graph and marks its timestamp
complete only after replay finishes. Update commit appends WAL before publishing
its COW snapshot and marks its timestamp complete only after publication. These
orderings make each committed transaction visible as one logical unit even
though the underlying insert replay or snapshot publication consists of
multiple implementation steps.
