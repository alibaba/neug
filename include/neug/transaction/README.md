# Transactions

> This file documents internal implementation details. For application-facing
> behavior, see
> [Transaction Management](../../../doc/source/transaction/transaction.md).

For ordinary queries, the transactional `ExecutionSlot` strategy uses
`ReadTransaction`, `InsertTransaction`, and `UpdateTransaction`. The direct
strategy uses `APSharedGuard` for reads and `InPlaceWriteScope` with
`StorageAPInPlaceUpdateInterface` for automatic writes.
`CompactTransaction` and `CheckpointCoordinator` implement maintenance paths.

These objects use RAII: terminal operations disarm their resources, and
destruction releases any active transaction or lease. Acquisition is ordered
before graph access: read and insert timestamps are acquired before pinning a
snapshot, while an update lease is acquired before cloning the current graph.

## Read Transaction

With a `ReadTransaction`, a specific version of the graph can be read. Its
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

## Insert Transaction

`InsertTransaction` is an insert-only optimization. It receives a unique write
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

## Update Transaction

Acquiring an update timestamp changes admission from `kOpen` to
`kInsertsBlocked`, blocking new inserts and updates and waiting for active
inserts to finish. Reads remain allowed. `ExecutionSlot` then clones the
current `PropertyGraph`, and `StorageCOWUpdateInterface` applies DML or DDL to
that COW clone. `UpdateTransaction` owns the clone, CowState and timestamp
lease; each statement temporarily supplies its allocator, while commit
temporarily supplies the snapshot store and WAL writer.

A non-empty `Commit()` first reserves a snapshot slot, appends its finalized
redo, then calls `UpdateTimestampLease::BeginCommit()`, publishes the prepared
clone, and completes the timestamp with the published snapshot generation. New
readers and writers are briefly blocked during publication; already-pinned
readers continue using their old slot.

`PropertyGraphCowState::HasChanges()` decides whether a snapshot must be
published. Data-only commits retain the planning generation; schema or index
changes advance it once. An empty commit, `Abort()`, or destruction discards the
clone, closes the timestamp gap, and reopens admission without publishing.

## Compact Transaction

Compaction enters `kAllBlocked` directly and drains active inserts and readers
before pinning the live graph. Commit appends a compact WAL record, compacts the
graph in place, rebuilds its `GraphView`, completes the timestamp, and reopens
admission. Abort or destruction closes the timestamp gap and reopens admission
without modifying the graph.

## Checkpoint Maintenance

A manual checkpoint receives an active `UpdateTimestampLease` from
`ExecutionSlot`. `CheckpointCoordinator` promotes it to `kAllBlocked`, drains
readers, and runs maintenance only after `GraphSnapshotStore` verifies that no
ordinary or stale snapshot pins remain.

Maintenance compacts and dumps the live graph, publishes the checkpoint, and
reopens the graph. The database handler then reopens allocators and invalidates
the query cache; service mode additionally rotates execution-slot WAL writers.
New transactions remain blocked until these steps finish. Success resets the
timestamp timeline (`read_ts = 0`, next `write_ts = 1`) and reopens admission.

Recovery and shutdown checkpoints rely on database lifecycle quiescence rather
than a transaction lease. Shutdown does not reopen the graph or run activation
handlers.

## Version Management

`VersionManager` uses an atomic admission state plus active-reader and
active-inserter counters:

| Admission state | New reads | New inserts | New update/compact | Purpose |
|---|---|---|---|---|
| `kOpen` | allowed | allowed | one transition may enter | normal execution |
| `kInsertsBlocked` | allowed | blocked | blocked | update execution; active inserts are drained |
| `kAllBlocked` | blocked | blocked | blocked | update commit or in-place maintenance |

An ordinary update does not drain readers already admitted before
`kAllBlocked`; compact and manual checkpoint explicitly drain them before
in-place maintenance. Contended acquisition uses `AdaptiveBackoff` with the
runtime wait function configured for the current runtime.

`write_ts_` allocates unique write timestamps. `read_ts_` is the highest
contiguous completed timestamp and is returned to new readers. Completion
includes commit, abort, and empty transactions, but only commits modify graph
state. `TimestampWindow` records out-of-order completions so a later writer
cannot advance `read_ts_` past an earlier unfinished transaction.

Insert commit appends WAL before replaying into the live graph. An update
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

When `VersionManager::begin_update_commit` is called, the admission state changes from `kInsertsBlocked` to `kAllBlocked`. New reads and new inserts are blocked until the `UpdateTransaction` is committed or aborted. Already-acquired reads continue unaffected on their pinned snapshot.

Timestamp completion uses a fixed ring whose slots contain the exact completed
timestamp, not a boolean bit. Before assigning a new write timestamp,
`VersionManager` limits unresolved timestamps to the ring capacity. An insert
that encounters this intentional backpressure first releases its inserter
admission, so it cannot prevent an update or compact operation from draining
existing inserts.

## Serializability

For a `ReadTransaction`, it will be assigned a graph timestamp. All insert or update transactions with timestamp less than or equal to that timestamp have been committed and are visible through timestamp filtering and the pinned snapshot.

For each `InsertTransaction` or `UpdateTransaction`, a unique timestamp is assigned. Commit makes all modifications visible atomically. Transactional writes are logged before publication.
