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

`CHECKPOINT` takes no arguments and must run under the `update` access
mode.

If `access_mode` is omitted, NeuG infers `update`. If it is specified
explicitly, it must be `"update"` or `"u"`. Any other access mode
(`"read"`/`"r"`, `"insert"`/`"i"`, or `"schema"`/`"s"`) is rejected, and
a database opened read-only cannot create a checkpoint.

Usage examples:

```python
# NeuG infers `update` access mode
conn.execute("CHECKPOINT")
```

```python
# or "u"; all other access modes are rejected
conn.execute("CHECKPOINT", access_mode="update")
```

`CHECKPOINT` can also be used with an
[`EXPLAIN`/`PROFILE` clause](../cypher_manual/explain_profile.md):

- `EXPLAIN CHECKPOINT` returns the execution plan without creating a
  checkpoint.
- `PROFILE CHECKPOINT` creates the checkpoint and reports its execution time
  as a single `CHECKPOINT` operator.

### Embedded mode example

```python
import neug

# This application checkpoints explicitly and checks the result of each
# call, so it disables the automatic checkpoint on close.
db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

conn.execute("COPY Person FROM 'people.csv'")  # Loaded data is in memory
conn.execute("CHECKPOINT")  # The loaded data has now been persisted to disk

conn.close()
# With the default checkpoint_on_close=True, db.close() would trigger an
# implicit checkpoint. This example disables it and checkpoints explicitly.
db.close()
```

### Service mode example

This example assumes a NeuG service is already running. To start one, see
[Service Mode](../getting_started/getting_started.md#service-mode)
(`db.serve()`).

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
nor checkpoints the server database. When the server-side database is later
closed with `checkpoint_on_close=True`, any outstanding WAL records are
folded into the final checkpoint; if checkpointing is disabled, the WAL
remains on disk for replay on the next startup.

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
retired generation is removed in step 5. Schedule checkpoints based on how
much work you can afford to redo after a crash (your recovery point) and
how large you want the WAL to grow, rather than on a fixed tight interval.

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

## Failure and recovery

On startup, NeuG opens the newest completely published checkpoint; an
incomplete generation is never selected. In Service mode, NeuG then replays
WAL records created after that checkpoint.

A **manual** `CHECKPOINT` fails in one of two ways:

- **Before the snapshot build starts** — for example, if the database cannot
  begin maintenance — the statement returns an error and the database keeps
  running normally.
- **After the snapshot build has started**, a failure can leave the
  in-memory state undefined. To avoid operating on a corrupt state, NeuG
  intentionally terminates the database process via `LOG(FATAL)`. This is
  by design, not a crash: restarting the database recovers cleanly from the
  latest published checkpoint and, in Service mode, the WAL.

A **recovery** checkpoint (run automatically on database open) takes a
different path: if it fails, NeuG does not terminate the process. Instead,
the open returns an error and the database remains unopened. Fix the
underlying cause (e.g., disk space, permissions) and retry.

### Recovering from a failed bulk load in Embedded mode

Bulk loads in Embedded mode are not fully atomic. To make a failed batch
discardable, disable checkpoint on close, create a recovery point before the
batch, and checkpoint again only after the batch succeeds. (Bulk load via
`COPY`/`LOAD FROM` is currently supported only in Embedded mode.)

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
