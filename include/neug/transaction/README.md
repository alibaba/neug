# Transactions

> Note: This file documents internal transaction implementation details. For
> application-facing behavior, see
> [Transaction Management](../../../doc/source/transaction/transaction.md).

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

`ReadTransaction` provides a set of APIs to read the graph, including schema, topology, and properties.

Commit, abort, and destruction all release the snapshot pin before unregistering
the reader from `VersionManager`.

## Insert Transaction

With an `InsertTransaction`, a set of vertices and edges can be inserted into the graph with the timestamp of transaction.

After insertion, the transaction can be committed by calling `InsertTransaction::Commit()` or be aborted by calling `InsertTransaction::Abort()`.

`InsertTransaction` does not provide interfaces to read the graph.

## Update Transaction

With an `UpdateTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

Also, `UpdateTransaction` provides interfaces to insert and update vertices and edges.

After insertion and update, the transaction can be committed by calling `UpdateTransaction::Commit()` or be aborted by calling `UpdateTransaction::Abort()`.

`UpdateTransaction` mutates a copy-on-write `PropertyGraph` clone. Its changes are invisible until commit publishes the clone through `GraphSnapshotStore`.

# Version Management

## Visibility

Graph records that participate in MVCC visibility are associated with a timestamp, which is the timestamp of the transaction that creates or publishes the record version.

When reading graph data with a `ReadTransaction` or `UpdateTransaction`, only records with timestamp less than or equal to the transaction timestamp are visible.

## Synchronization

There is no synchronization between read and insert transactions in the normal state. All read and insert transactions can be executed concurrently.

The `VersionManager` state machine has three effective states for read, insert, and update transactions:

| State | Meaning | New Reads | New Inserts | New Updates | Existing Reads |
|-------|---------|-----------|-------------|-------------|----------------|
| `kOpen` | Normal | allowed | allowed | allowed | continue |
| `kInsertsBlocked` | Update execution | allowed | blocked | blocked | continue |
| `kAllBlocked` | Update commit or compaction | blocked | blocked | blocked | continue |

When an `UpdateTransaction` is created, the admission state changes from `kOpen` to `kInsertsBlocked`. It waits for all in-flight insert transactions to finish, but does not block or wait for read transactions. New insert transactions and new update transactions are blocked during this phase; existing and new reads continue.

When `VersionManager::begin_update_commit` is called, the admission state changes from `kInsertsBlocked` to `kAllBlocked`. New reads and new inserts are blocked until the `UpdateTransaction` is committed or aborted. Already-acquired reads continue unaffected on their pinned snapshot.

## Serializability

For a `ReadTransaction`, it will be assigned a graph timestamp. All insert or update transactions with timestamp less than or equal to that timestamp have been committed and are visible through timestamp filtering and the pinned snapshot.

For each `InsertTransaction` or `UpdateTransaction`, a unique timestamp will be assigned. When committing, a write-ahead log will be written to the disk and all modifications will be applied to the graph atomically.
