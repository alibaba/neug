# Checkpoints

A checkpoint writes a recoverable database snapshot to disk and limits how much
write-ahead log (WAL) NeuG must replay during startup.

Most applications do not need to create checkpoints manually:

- ordinary committed writes are already durable through the WAL in both
  Embedded and Service mode;
- a persistent `COPY ... FROM` in Embedded mode creates the checkpoint it needs
  before reporting success;
- Service mode does not support `LOAD FROM` or any `COPY` statement.

## When to create a checkpoint

Use a manual checkpoint when you want to:

- reduce WAL replay time after restart;
- consolidate persistent state before an operational milestone;
- explicitly verify that checkpoint maintenance succeeds.

A checkpoint is not required after each transaction. Creating checkpoints too
frequently adds unnecessary maintenance work.

## Run a checkpoint

```cypher
CHECKPOINT;
```

`CHECKPOINT` takes no arguments and must run as an auto-commit statement under
the `update` access mode.

If `access_mode` is omitted, NeuG infers `update`. If specified explicitly, use
`"update"` or `"u"`. Other modes are rejected. A read-only database cannot
create a checkpoint, and `CHECKPOINT` cannot run inside an explicit
transaction.

```python
conn.execute("CHECKPOINT")
conn.execute("CHECKPOINT", access_mode="update")
```

`CHECKPOINT` also supports
[`EXPLAIN` and `PROFILE`](../cypher_manual/explain_profile.md):

- `EXPLAIN CHECKPOINT` returns the execution plan without creating a
  checkpoint.
- `PROFILE CHECKPOINT` creates the checkpoint and reports its execution time.

## What applications observe

Checkpoint maintenance waits for work already in progress and temporarily
prevents new transactions from starting. A long-running query can therefore
delay a checkpoint, and the checkpoint can briefly delay new work.

After a successful checkpoint:

- existing Service-mode sessions remain valid;
- subsequent transactions continue normally;
- restart recovery begins from the new checkpoint and replays only later WAL
  records.

Schedule operational checkpoints during a quieter period when predictable
latency matters.

## Embedded mode

Ordinary writes do not require a manual checkpoint:

```python
import neug

db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

conn.execute("CREATE (p:Person {id: 42})")  # durable after commit
conn.close()
db.close()
```

Persistent bulk import is also automatic:

```python
conn.execute("COPY Person FROM 'people.csv'")
# Success means the import has been published in a checkpoint.
```

The import is atomic. If reading, validation, or checkpoint publication fails,
none of the imported data becomes visible and the previous database state
remains usable.

`COPY TEMP` is different: it updates only the connection's in-memory temporary
graph and is lost when the connection or database closes.

## Service mode

Service-mode writes are durable when they commit. A manual checkpoint is
optional maintenance:

```python
from neug import Session

session = Session("http://localhost:10000/")
session.execute(
    "CREATE (p:Person {name: 'Alice'})",
    access_mode="insert",
)
session.execute("CHECKPOINT")  # optional
session.close()
```

Closing a client `Session` only disconnects that client. It does not close or
checkpoint the server-side database.

Remember that Service mode rejects `LOAD FROM`, `COPY FROM`, `COPY TEMP`, and
`COPY TO`. Perform file I/O in Embedded mode before starting the service.

## Automatic checkpoint on database close

Persistent read-write databases default to `checkpoint_on_close=True`. Closing
the database therefore attempts one final checkpoint.

This setting applies when the database owner closes the database; closing a
remote client session does not trigger it.

If the automatic checkpoint fails, `close()` reports an error. Depending on when
the failure occurs, the database may remain open for another attempt or may
already be closed. Use an explicit `CHECKPOINT` when the application must know
the maintenance result before shutdown.

With `checkpoint_on_close=False`, ordinary committed writes are still
recoverable from the WAL.

## Recovery and failures

On startup, NeuG restores the latest published checkpoint and replays committed
WAL records created after it. Incomplete checkpoint work is ignored.

A manual checkpoint can fail in two ways:

- If NeuG cannot begin the checkpoint, the statement returns an error and the
  database remains usable.
- If failure occurs after replacement of the current state has begun, NeuG
  stops rather than continue from an unsafe in-memory state. Restarting recovers
  from the last published checkpoint and later committed WAL records.

`checkpoint_on_recovery` can request a new checkpoint after WAL recovery when a
read-write database opens. It is disabled by default. If it fails, the open
returns an error so the application can correct the cause and retry.

For the on-disk layout, AP/TP coordination, checkpoint publication, garbage
collection, and legacy-format migration, see
[Transaction Model](transaction_model.md).
