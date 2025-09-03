# Transactions

## Read Transaction

With an `ReadTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

`ReadTransaction` provides a set of APIs to read the graph, including schema, topology, and properties.

After query with the `ReadTransaction` object, the transaction should be released by calling `ReadTransaction::Release()`.

## Insert Transaction

With an `InsertTransaction`, a set of vertices and edges can be inserted into the graph with the timestamp of transaction.

After insertion, the transaction can be committed by calling `InsertTransaction::Commit()` or be aborted by calling `InsertTransaction::Abort()`.

`InsertTransaction` does not provide interfaces to read the graph.

## Update Transaction

With an `UpdateTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

Also, `UpdateTransaction` provides interfaces to insert and update vertices and edges.

After insertion and update, the transaction can be committed by calling `UpdateTransaction::Commit()` or be aborted by calling `UpdateTransaction::Abort()`.

# Version Management

## Visibility

Each edge is associated with a timestamp, which is the timestamp of the transaction that inserts the edge.

When read graph with a `ReadTransaction` or `UpdateTransaction`, only edges with timestamp less than or equal to the timestamp of the transaction will be returned.

## Synchronization

There is no synchronization between read and insert transactions. All read and insert transactions can be executed concurrently.

When an `UpdateTransaction` is created, it will wait for all read and insert transactions to finish. Then, the `UpdateTransaction` will read the graph with the latest timestamp, and all read and insert transactions will be blocked until the `UpdateTransaction` is committed or aborted.

## Serializability

For a `ReadTransaction`, it will be assigned with a timestamp of the graph, all insert or update transactions with timestamp less than or equal to the timestamp have been committed.

For each `InsertTransaction` or `UpdateTransaction`, a unique timestamp will be assigned. When committing, a write-ahead log will be written to the disk and all modifications will be applied to the graph atomically.