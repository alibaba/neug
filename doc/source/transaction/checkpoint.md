# Checkpoints

A checkpoint saves the current database state to disk. Since v0.2, committed
ordinary writes in both embedded and service mode are already durable through
WAL, so they do not require a manual checkpoint. Persistent `COPY ... FROM` and
batch inserts create a checkpoint before reporting success. `COPY TEMP` remains
in memory and is lost when the database closes.

| Question | Ordinary writes, including index changes | Persistent COPY/batch insert |
|---|---|---|
| Is a manual `CHECKPOINT` required for durability? | No; committed changes are already saved in WAL | No; a checkpoint is created before the statement reports success |
| Why create one? | To reduce the amount of WAL replayed during recovery | To make the imported data durable |
| What is restored after restart? | The latest checkpoint and later committed changes | The imported data from the latest checkpoint |

For transaction boundaries and concurrency outside checkpoint operations, see
[Transaction Management](transaction.mdx). For how checkpoints are stored and
applied internally, see [How It Works](how_it_works.md).

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
conn.execute("CHECKPOINT")  # NeuG infers `update`
conn.execute("CHECKPOINT", access_mode="update")  # or "u"; all other modes are rejected
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

# Ordinary writes remain recoverable from WAL when checkpoint-on-close is off.
db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

conn.execute("COPY Person FROM 'people.csv'")
# COPY returned only after publishing its private bulk checkpoint.
conn.execute("CREATE (p:Person {id: 42})")  # Durable through logical WAL.

conn.close()
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

# Optional maintenance: publish a checkpoint that bounds future WAL replay.
session.execute("CHECKPOINT")
session.close()
```

Closing a client `Session` only disconnects that client; it neither closes
nor checkpoints the server database. When the server-side database is later
closed with `checkpoint_on_close=True`, any outstanding WAL records are
folded into the final checkpoint; if checkpointing is disabled, the WAL
remains on disk for replay on the next startup.

## Concurrency

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
checkpoint. If it fails, `close()` raises an exception. Depending on when the
failure occurs, the database may remain open for another attempt or may already
be closed.

Use an explicit `CHECKPOINT` when the application must know whether maintenance
succeeded. If `checkpoint_on_close=False`, committed ordinary writes remain
recoverable through WAL in both embedded and service mode. Successful persistent
bulk writes have already created their own checkpoints.

## Failure and recovery

On startup, NeuG loads the checkpoint selected by `CURRENT`. When `CURRENT`
exists, NeuG does not fall back to an older database directory. Incomplete
checkpoints from interrupted operations are ignored. If `CURRENT` is absent, a
read-write open may perform a one-time v1 migration; see
[How It Works](how_it_works.md#upgrading-legacy-checkpoint-directories).
Persistent embedded and service databases then replay committed WAL records
created after the selected checkpoint.

A **manual** `CHECKPOINT` can fail in two ways:

- If NeuG cannot start the checkpoint, the statement returns an error and the
  database remains usable.
- If it fails after NeuG starts replacing the current state, NeuG terminates the
  process to avoid using an unsafe state. Restarting recovers from the checkpoint
  selected by `CURRENT` and subsequent committed writes.

`checkpoint_on_recovery` optionally creates a checkpoint after WAL recovery
when opening a read-write database. It is disabled by default. If it fails,
the open returns an error without terminating the process. Fix the underlying
cause (for example, disk space or permissions) and retry.

### Persistent bulk load failure

Persistent COPY and batch inserts are atomic. If the import or its checkpoint
fails, none of the new data becomes visible and the previous database state
remains usable.

```python
import neug

db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

try:
    conn.execute("COPY Person FROM 'large_batch.csv'")
except Exception:
    # The previous database state is still current and usable.
    pass
```

Each successful persistent COPY creates one checkpoint. `COPY TEMP` is the
exception: it updates only the in-memory database. Batch input at the application
level when possible, and leave enough temporary disk space for the checkpoint
and cleanup.
