# Checkpoints

A checkpoint creates a durable snapshot of the current database state. AP
direct and TP ordinary writes both append logical WAL; successful persistent
AP-direct COPY/batch insert instead publish a statement-level checkpoint
without row-level WAL. Index DDL follows ordinary AP/TP logical-WAL commit.
`COPY TEMP` is an in-memory private-COW
commit and produces neither WAL nor a checkpoint. A checkpoint therefore has
two roles:

| Question | Ordinary DML/DDL, including index DDL | AP-direct COPY/batch insert |
|---|---|---|
| Is an explicit `CHECKPOINT` required for durability? | No; a committed write is already durable in WAL | No; the statement returns success only after its checkpoint is published |
| What is it for? | Optional maintenance that bounds WAL replay | Statement commit and atomic publication |
| What is recovered after restart? | `CURRENT` plus WAL records after `base_ts` | The checkpoint selected by `CURRENT`; bulk rows are not repeated in WAL |

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

AP-direct COPY/batch insert use a narrower private-COW protocol.
The statement prepares all changes in a cloned graph, performs the existing
consuming dirty-module dump/reopen only on that clone, then publishes the
staging manifest and replaces the current graph atomically. It does not compact
or reopen the published graph or allocator. Every successful persistent bulk
statement advances the checkpoint ID and rotates active WAL writers to the new
epoch.

After a manual checkpoint publishes, NeuG reopens the live graph and allocators from the new checkpoint. In Service mode, each execution-slot WAL writer is then rotated onto the new epoch. Finally, the transaction timeline is reset and new transactions are admitted. These steps all run while the checkpoint barrier is still held.

Recovery and shutdown checkpoints use the same compacting, destructive dump. Recovery reopens the graph and allocators before the database starts serving; shutdown persists without reopening. Garbage collection removes manifests, WAL epochs, and objects only when they are neither current nor retained by a live checkpoint reference.

Before the destructive dump begins, a shutdown checkpoint failure leaves the
open database usable and `Close()` reports the failure so the caller can correct
it and retry. Once the dump has consumed live graph state, failure is not
retryable for that open instance: `Close()` finishes resource and lock cleanup,
marks the database closed, and rethrows the failure. A later fresh `Open()` can
still use the previously published checkpoint.

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
checkpoint. A failure before the destructive dump is raised from `close()` and
leaves the database open for a corrected retry. A failure after the destructive
dump completes teardown and is then raised.

Use an explicit `CHECKPOINT` when the application must know whether maintenance
succeeded. If `checkpoint_on_close=False`, committed ordinary writes remain
recoverable from WAL in AP direct and TP mode. Successful AP-direct bulk
statements have already published their own checkpoints.

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

### AP-direct bulk load failure

COPY/batch insert are atomic at statement scope. Their mutations
remain in a private COW graph until the staging checkpoint is ready. Supplier,
index maintenance, preflight, or staging failures discard that graph; the
published current graph, checkpoint ID, and WAL epoch remain unchanged. There
is no partial state or delayed checkpoint barrier to recover.

```python
import neug

db = neug.Database("/path/to/database", checkpoint_on_close=False)
conn = db.connect()

try:
    conn.execute("COPY Person FROM 'large_batch.csv'")
except Exception:
    # The previous published graph is still current and usable.
    pass
```

Each successful persistent COPY publishes one checkpoint. `COPY TEMP` is the
exception: it atomically updates only the current in-memory graph. Batch input
at the application level when possible, and leave enough temporary disk space
for rewritten objects and older objects retained until checkpoint garbage
collection.
