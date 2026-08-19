# Checkpoints

A checkpoint creates a durable snapshot of the current database state. The
two deployment modes persist writes differently: Embedded mode holds changes
in memory until a checkpoint writes them to disk, while Service mode appends
every committed change to a write-ahead log (WAL). A checkpoint therefore
plays a different role in each mode:

| Question | Embedded mode | Service mode |
|---|---|---|
| Is `CHECKPOINT` required for durability? | Yes; un-checkpointed changes are lost when the database closes | No; committed changes are already durable in the WAL |
| What is it for? | Persist changes and create a recovery point | Optional maintenance: publish a checkpoint that bounds WAL replay |
| What is recovered after a restart? | The checkpoint selected by `CURRENT` | The `CURRENT` checkpoint plus WAL records after its `base_ts` |

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

# Optional maintenance: publish a checkpoint that bounds future WAL replay.
session.execute("CHECKPOINT")
session.close()
```

Closing a client `Session` only disconnects that client; it neither closes
nor checkpoints the server database. When the server-side database is later
closed with `checkpoint_on_close=True`, any outstanding WAL records are
folded into the final checkpoint; if checkpointing is disabled, the WAL
remains on disk for replay on the next startup.

## On-disk layout

A persistent database uses one manifest selector, immutable objects shared by manifests, a WAL epoch per manifest ID, and a temporary workspace per database open:

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

`CURRENT` is the sole publication selector. Its content is the selected manifest's decimal ID followed by a single trailing newline (for example `3\n`), written via an atomic rename; operators may inspect or rewrite it manually with a plain text editor. A published manifest has the required fields `v`, `base_ts`, `schema`, and `modules`; it may also contain `scalars`. Module descriptors persist object IDs, not absolute paths. The same ID names the manifest and its WAL epoch. `base_ts` is the highest transaction timestamp already represented by the manifest, so recovery replays the selected epoch from `base_ts + 1`. Full checkpoints use `base_ts=0` and reset the transaction timeline after reopening.

Checkpoint objects are immutable and may be referenced by several manifests. Runtime files are not checkpoint data: each database open receives its own `runtime/open-<id>/` directory (the suffix is an opaque unique ID, not a timestamp or manifest ID), and closing that database removes only its own unpinned workspace.

### Upgrading legacy checkpoint directories

When `CURRENT` is absent, the first read-write open automatically upgrades the newest valid released v1 `checkpoint-N` generation. NeuG imports its immutable snapshot files into `checkpoint/objects/`, preserves its generation as the new manifest and WAL epoch ID, and publishes a v2 manifest with `base_ts=0`; normal recovery then replays every legacy WAL record. Files are hardlinked when safe and copied otherwise.

The old directories are not changed before the new `CURRENT` is durably published. A crash before publication leaves the legacy checkpoint usable and the next read-write open retries. After the database has opened and recovered successfully, normal garbage collection removes the old `checkpoint-N` and `checkpoint-N.next` directories, making the upgrade one-way. A legacy-only database cannot be opened read-only: open it once in read-write mode to perform the upgrade. Legacy `meta` versions other than v1 are rejected rather than guessed.

## What a checkpoint does

A manual `CHECKPOINT` first takes exclusive checkpoint maintenance control and waits for in-flight work to finish (see [Concurrency](#concurrency)). It preserves the existing full-checkpoint behavior: compact the live graph, destructively dump it, publish a complete manifest, and reopen the graph and allocators. Only dirty graph and index modules need new immutable objects; clean module descriptors may continue to reference existing objects. The manifest and its WAL epoch are made durable before `CURRENT` is atomically replaced.

After publication, NeuG reopens the graph and allocators from the new checkpoint. In Service mode, each execution-slot WAL writer is then rotated onto the new epoch. Finally, the transaction timeline is reset and new transactions are admitted. These steps all run while the checkpoint barrier is still held.

Recovery and shutdown checkpoints use the same compacting, destructive dump. Recovery reopens the graph and allocators before the database starts serving; shutdown persists without reopening. Garbage collection removes manifests, WAL epochs, and objects only when they are neither current nor retained by a live checkpoint reference.

"Full" describes the runtime lifecycle boundary; it does not require rewriting every clean immutable object. Checkpoint disk growth is therefore driven by rewritten modules plus objects retained by live references. Schedule checkpoints based on the acceptable replay work after a crash and WAL growth, rather than a fixed tight interval.

### Disk space reclamation

Retired manifests, WAL epochs, immutable objects, and abandoned `runtime/open-<id>/` workspaces are removed by garbage collection, which runs only at three points: read-write database open, a successful manual `CHECKPOINT`, and database close. Deleting rows or dropping tables therefore does not shrink disk usage until one of those points is reached.

Read-only opens never run garbage collection. A pure read-only deployment accumulates the stale `runtime/open-<id>/` workspaces left behind by crashed read-only processes; the next read-write open reclaims them.

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

On startup, NeuG opens only the manifest named by `CURRENT`; it never falls back to a legacy directory when that selector exists. If `CURRENT` is absent, a read-write open may perform the one-time v1 migration described above. An incomplete staging manifest or object is unreachable until `CURRENT` changes and is therefore never selected. In Service mode, NeuG validates the selected WAL epoch and replays only records with timestamps greater than that manifest's `base_ts`.

A **manual** `CHECKPOINT` fails in one of two ways:

- **Before the snapshot build starts** — for example, if the database cannot
  begin maintenance — the statement returns an error and the database keeps
  running normally.
- **After the destructive publication boundary**, a failure can leave the
  in-memory state undefined. To avoid operating on a corrupt state, NeuG
  intentionally terminates the database process via `LOG(FATAL)`. This is
  by design, not a crash: restarting the database recovers cleanly from the
  manifest selected by `CURRENT` and, in Service mode, its WAL epoch.

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
preserving. Leave enough temporary disk space for rewritten objects and any
older objects that remain reachable during publication.
