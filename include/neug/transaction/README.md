# Transactions

> This file documents internal implementation details. For application-facing
> behavior, see
> [Transaction Management](../../../doc/source/transaction/transaction.mdx).

For ordinary queries, the transactional `ExecutionSlot` strategy uses
`SnapshotReadTransaction`, `MvccInsertTransaction`, and
`SnapshotCowWriteTransaction`. The direct strategy uses `ReadSnapshotLease` for reads and
`CurrentCowWriteTransaction` with `CowGraphStorage` for ordinary writes.
Index DDL is supported by `CowGraphStorage` in both AP and TP and commits
through logical WAL. AP-direct COPY uses `BulkCowGraphStorage` over the same
private `CowGraphWorkspace` and commits through a statement-level checkpoint.
The ordinary storage type rejects batch insertion, keeping COPY-only
capabilities out of TP and explicit transactions. `COPY TEMP` uses the same
private bulk storage but commits through the `ExecutionSlot`-only transient
path: it atomically replaces the in-memory current graph without writing WAL
or publishing a checkpoint.
`InPlaceCompactionTransaction` and `CheckpointCoordinator` implement maintenance paths.
These names describe internal execution strategies; Connection and Session
continue to present logical read-only/read-write transaction semantics.

These objects use RAII: terminal operations disarm their resources, and
destruction releases any active transaction or lease. Acquisition is ordered
before graph access: read and insert timestamps are acquired before pinning a
snapshot, while an update lease is acquired before cloning the current graph.

## Embedded Explicit Connection Transactions

`Connection::BeginTransaction()` owns one `TransactionContext` across multiple
`Connection::Query()` calls. The context holds either a
`SnapshotReadTransaction` for `TransactionMode::kReadOnly` or a
`CurrentCowWriteTransaction` for
`TransactionMode::kReadWrite`; it does not introduce a public transaction
interface or a second execution pipeline. The read-only owner pins one view.
The read-write owner uses `OpenStorage()` for every supported statement, and
its private view supplies read-your-writes until one `Commit()` appends and
publishes the combined redo.

After a successful schema, bulk, or transient mutation changes private planning
inputs, queries compile against the private view without consulting the local or
global query cache. Ordinary DML continues to reuse plans compiled for the
unchanged schema. A regular statement failure aborts the concrete owner and
leaves the connection rollback-only; `Rollback()`, `Close()`, and destruction
clear it. Cypher transaction-control text, bulk,
checkpoint/maintenance, procedure calls, and temporary-schema operations are
rejected in this first embedded API before their side effects.

## Snapshot Read Strategy

With a `SnapshotReadTransaction`, a specific version of the graph can be read. Its
`ReadSnapshotLease` owns a visibility timestamp and a pinned graph snapshot as
one coherent read view.

Reader acquisition captures an atomically published
`{visibility timestamp, snapshot generation}` pair, pins the current snapshot,
and validates that the slot generation matches. If an update publishes between
the capture and the pin, the generation mismatch is detected and the complete
acquisition is retried. A transaction therefore cannot observe an old
timestamp with a newly published snapshot.

`Commit()` and `Abort()` both unpin the snapshot and release the active-reader
count; destruction does the same for an active transaction. References backed
by the pinned graph must not outlive the transaction.

Commit, abort, and destruction all release the snapshot pin before unregistering
the reader from `VersionManager`.

## MVCC Insert Strategy

`MvccInsertTransaction` is an insert-only optimization. It receives a unique write
timestamp, pins the current snapshot, and buffers vertex and edge operations in
a local WAL archive without modifying the graph.

A non-empty `Commit()`:

1. appends the complete transaction record to the WAL;
2. replays it into the pinned live graph with the transaction timestamp; and
3. unpins the snapshot and completes the timestamp.

Multiple inserts may run concurrently. They share the live graph with readers,
so visibility depends on every read path filtering records newer than its read
timestamp. Vertices added by the same transaction are tracked separately so
subsequent edge inserts can resolve them before WAL replay.

An empty commit only releases the transaction. `Abort()` or destruction
discards buffered operations and completes the timestamp without applying them.

## Snapshot COW Write Transaction

Acquiring an update timestamp changes admission from `kOpen` to
`kInsertsBlocked`, blocking new inserts and updates and waiting for active
inserts to finish. Reads remain allowed. `ExecutionSlot` then clones the
current `PropertyGraph`, and `CowGraphStorage` applies DML or DDL to
that COW clone.

A non-empty `Commit()` checks snapshot-slot capacity, appends the finalized WAL,
and calls `UpdateTimestampLease::BeginCommit()`. This changes admission to
`kAllBlocked`, preventing new readers and writers while the clone is published.
Readers that already hold a `SnapshotGuard` are not drained and continue using
their pinned slot. The snapshot is published before the update timestamp is
completed, so a reader cannot observe the new timestamp with the old snapshot.

Schema changes invalidate the shared query cache before publication. An empty
commit, `Abort()`, or destruction discards the clone, completes the timestamp,
and reopens admission without publishing a snapshot.

DML WAL records identify vertex and edge types by schema name rather than by
process-local numeric label ID. This keeps replay stable when temporary labels
occupied ID slots that are intentionally absent from the persisted schema.
Mutations whose target schema is temporary use `CommitTransient()` and produce
no WAL; a statement that mixes durable and transient mutations is rejected by
the commit validation instead of partially persisting either side.

## Bulk COW Write Mode

`CurrentCowWriteTransaction::OpenBulkStorage()` returns
`BulkCowGraphStorage`, a thin capability extension of `CowGraphStorage` for
COPY/batch insert. Both types mutate only a private shallow clone. Bulk
operations detach their target table, CSR, column, and affected indexes once
before consuming input; they continue to use the native batch loader instead
of per-row DML or per-row WAL. Index create/drop/activation is handled by
`CowGraphStorage` and commits through logical WAL in AP and TP.
When COPY infers a persistent schema, that schema creation belongs to the same
checkpoint-only bulk workspace and therefore does not conflict with the
empty-logical-redo requirement of `CommitCowWrite()`.

A successful persistent bulk statement calls
`CheckpointCoordinator::CommitCowWrite()`. It consumes and reopens dirty
modules only in the private clone, publishes the staging manifest as the
durable decision, replaces the current graph without changing snapshot
generation, and rotates every active WAL writer. Validation and staging failures
discard the private workspace, so no partial mutation becomes visible. Once the
consuming dump starts, failures are fail-stop: `VecColumn` payload buffers are
currently shared between the clone and its published base, so consumption may
invalidate the base even before manifest publication. Manifest publication is
still the durable decision point.

`COPY TEMP` is not a durable bulk statement. It calls `CommitTransient()` after
the private workspace has been fully prepared; failures discard the workspace,
and successful temporary objects disappear after database restart.

## In-Place Compaction Strategy

Compaction enters `kAllBlocked` directly and drains active inserts and readers
before pinning the live graph. Commit appends a compact WAL record, mutates the
live graph through the compact path, rebuilds its `GraphView`, completes the
timestamp, and reopens admission. Abort or destruction closes the timestamp gap
and reopens admission without modifying the graph.

## Checkpoint Maintenance

A manual checkpoint receives an active `UpdateTimestampLease` from
`ExecutionSlot`. `CheckpointCoordinator` promotes it to `kAllBlocked`, drains
readers, and runs maintenance only after `GraphSnapshotStore` verifies that no
ordinary or stale snapshot pins remain.

Manual maintenance preserves the full-checkpoint lifecycle: compact and
destructively dump the live graph, atomically publish the new manifest, reopen
the graph and allocators, rotate execution-slot WAL writers in service mode,
and reset the timestamp timeline. New transactions remain blocked until all
activation finishes. Recovery uses the same full path and reopens runtime state;
shutdown does not reopen the graph or run activation handlers.

Publication makes the new `checkpoint/CURRENT` selector durable only after its `manifests/<id>.manifest`, immutable objects, and `wal/<id>/` epoch are ready. Dirty modules write new objects while clean descriptors may be reused from the previous manifest; the manager reclaims unreferenced manifests, WAL epochs, and objects only after their checkpoint references are released. Each database open owns a separate `runtime/open-<epoch>/` workspace.

Recovery and shutdown checkpoints rely on database lifecycle quiescence rather
than a transaction lease.

## Version Management

`VersionManager` uses an atomic admission state plus active-reader and
active-inserter counters:

| Admission state | New reads | New inserts | New update/compact | Purpose |
|---|---|---|---|---|
| `kOpen` | allowed | allowed | one transition may enter | normal execution |
| `kInsertsBlocked` | allowed | blocked | blocked | update execution; active inserts are drained |
| `kAllBlocked` | blocked | blocked | blocked | update commit or exclusive maintenance |

An ordinary update does not drain readers already admitted before
`kAllBlocked`; compact and manual checkpoint explicitly drain them before
exclusive maintenance. Contended acquisition uses `AdaptiveBackoff` with the
runtime wait function configured for the current runtime.

`write_ts_` allocates unique write timestamps. `read_ts_` is the highest
contiguous completed timestamp and is returned to new readers. Completion
includes commit, abort, and empty transactions, but only commits modify graph
state. `TimestampWindow` records out-of-order completions so a later writer
cannot advance `read_ts_` past an earlier unfinished transaction.

Insert commit appends WAL before replaying into the live graph. Update commit
appends WAL before publishing its COW snapshot. Both complete their timestamps
only after the graph change is visible.

Update waiters directly contend the existing admission phase; acquisition order
is unspecified. The public manager API retains its no-deadline fast path.
The deadline overload of `UpdateTimestampLease` invokes a private lease-only
manager hook. If its absolute `steady_clock` deadline expires before timestamp
reservation, lease construction reports `ERR_TX_TIMEOUT` and restores any phase
acquired by that attempt. Admission contention and inserter draining use separate
backoff cursors. Existing production callers retain infinite-wait behavior and
do not read the clock; future explicit-transaction integration will pass its
write-wait deadline through this overload.

When `VersionManager::begin_update_commit` is called, the admission state changes from `kInsertsBlocked` to `kAllBlocked`. New reads and new inserts are blocked until the `SnapshotCowWriteTransaction` is committed or aborted. Already-acquired reads continue unaffected on their pinned snapshot.

Timestamp completion uses a fixed ring whose slots contain the exact completed
timestamp, not a boolean bit. Before assigning a new write timestamp,
`VersionManager` limits unresolved timestamps to the ring capacity. An insert
that encounters this intentional backpressure first releases its inserter
admission, so it cannot prevent an update or compact operation from draining
existing inserts.

## Serializability

For a `SnapshotReadTransaction`, it will be assigned a graph timestamp. All insert or update transactions with timestamp less than or equal to that timestamp have been committed and are visible through timestamp filtering and the pinned snapshot.

For each `MvccInsertTransaction` or `SnapshotCowWriteTransaction`, a unique timestamp will be assigned. When committing, a write-ahead log will be written to the disk and all modifications will be applied to the graph atomically.
