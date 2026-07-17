# Checkpoints

A checkpoint creates a durable snapshot of the current database state. The
two deployment modes persist writes differently: Embedded mode holds changes
in memory until a checkpoint writes them to disk, while Service mode appends
every committed change to a write-ahead log (WAL). A checkpoint therefore
plays a different role in each mode:

| Question | Embedded mode | Service mode |
|---|---|---|
| Is `CHECKPOINT` required for durability? | Yes; un-checkpointed changes are lost when the database closes | No; committed changes are already durable in the WAL |
| What is it for? | Persist changes and create a recovery point | Optional maintenance: fold WAL records into a fresh snapshot |
| What is recovered after a restart? | The last successful checkpoint | The last checkpoint plus WAL replay |

For transaction boundaries and concurrency outside checkpoint operations, see
[Transaction Management](transaction.md).

## Run a checkpoint

```cypher
CHECKPOINT;
```

`CHECKPOINT` takes no arguments and runs under the `update` access mode.
Omit `access_mode` and NeuG infers `update`, or set it explicitly:

```python
conn.execute("CHECKPOINT")
conn.execute("CHECKPOINT", access_mode="update")  # or "u"
```

Any other explicit mode (`read`/`r`, `insert`/`i`, or `schema`/`s`) is
rejected, and a database opened read-only cannot create a checkpoint.

- `EXPLAIN CHECKPOINT` returns the execution plan without creating a
  checkpoint.
- `PROFILE CHECKPOINT` creates the checkpoint and reports its execution time
  as a single `Checkpoint` operator.

### Embedded mode example

```python
import neug

# This application checkpoints explicitly and checks the result of each
# call, so it disables the automatic checkpoint on close.
db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

conn.execute("COPY Person FROM 'people.csv'")
conn.execute("CHECKPOINT")  # The loaded data is now durable.

conn.close()
db.close()
```

### Service mode example

```python
from neug import Session

session = Session("http://localhost:10000/")

# This insert is durable as soon as it commits; CHECKPOINT is not required.
session.execute(
    "CREATE (p:Person {name: 'Alice'})",
    access_mode="insert",
)

# Optional maintenance: fold committed WAL records into a fresh snapshot.
session.execute("CHECKPOINT")
session.close()
```

Closing a client `Session` only disconnects that client; it neither closes
nor checkpoints the server database.

## What a checkpoint does

Each successful checkpoint performs the following steps:

1. Take exclusive control of the database, waiting for in-flight work to
   finish (see [Concurrency](#concurrency)).
2. Write a complete new snapshot generation to disk, alongside the current
   one.
3. Publish the new generation atomically. From this point on it is the
   recovery point; the previous generation is no longer needed for recovery.
4. Reopen the live database on the new generation and resume normal
   operation. In Service mode this also starts a fresh, empty WAL.
5. Remove the retired generation (best effort).

Because step 2 writes a full copy while the current generation still exists,
peak disk usage during a checkpoint is roughly twice the database size, and
checkpoint time grows with database size. Disk usage drops back once the
retired generation is removed in step 5. Run checkpoints as often as your
recovery-point and WAL-size requirements justify, rather than on a fixed
tight interval.

### Concurrency

- **Embedded mode:** A checkpoint takes the exclusive query lock. It waits
  for running operations to finish and blocks new operations until it
  completes.
- **Service mode:** A checkpoint waits for in-flight reads and writes to
  finish without interrupting them, holds off new transactions while it
  runs, and then executes with no concurrent transactions. The wait is
  unbounded: a single long-running query can delay the entire checkpoint.
  After a successful checkpoint, existing sessions remain valid and NeuG
  starts a new empty WAL.

For Service mode, schedule checkpoints during a quiet period when possible.

## Automatic checkpoint on close

In the Python API, persistent read-write databases default to
`checkpoint_on_close=True`, so closing the database attempts a final
checkpoint. This automatic checkpoint is best effort: NeuG logs a failure
but does not raise it from `close()`.

Use an explicit `CHECKPOINT` when the application must know whether
persistence succeeded. If you set `checkpoint_on_close=False`:

- Embedded mode discards changes made after the last successful checkpoint
  when the database closes.
- Service mode can recover committed changes from the WAL, even if they were
  made after the last checkpoint.

Checkpoints do not make a temporary in-memory database survive after it is
closed.

## Failure and recovery

On startup, NeuG opens the newest completely published checkpoint; an
incomplete generation is never selected. In Service mode, NeuG then replays
WAL records created after that checkpoint.

A manual `CHECKPOINT` fails in one of two ways:

- **Before the snapshot build starts** — for example, if the database cannot
  begin maintenance — the statement returns an error and the database keeps
  running normally.
- **After the snapshot build has started**, a failure can leave the
  in-memory state undefined, so NeuG deliberately terminates the database
  process rather than continue. Restart the database to recover from the
  latest published checkpoint and, in Service mode, the WAL.

### Recovering from a failed Embedded write

Large Embedded-mode writes are not fully atomic. To make a failed batch
discardable, disable checkpoint on close, create a recovery point before the
batch, and checkpoint again only after the batch succeeds:

```python
import neug

path = "/path/to/database"
db = neug.Database(path, checkpoint_on_close=False)
conn = db.connect()

conn.execute("CHECKPOINT")  # Recovery point before the batch

try:
    conn.execute("COPY Person FROM 'large_batch.csv'")
    conn.execute("CHECKPOINT")  # Persist the completed batch
except Exception:
    # Do not checkpoint the possibly partial in-memory state.
    conn.close()
    db.close()

    db = neug.Database(path, checkpoint_on_close=False)
    conn = db.connect()  # Reopens the last successful checkpoint
```

For long imports, checkpoint after each batch whose completed work is worth
preserving. Always leave enough temporary disk space for a new full
generation.
