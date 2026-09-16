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

### Chunked property column storage

Fixed-length vertex and unbundled edge property columns use power-of-two logical chunks with a 256 KiB payload floor. Each logical chunk contains physical pages of at most 4 KiB. Reads use a flat page index and cached payload pointers; a covering update copies only the affected physical page. `get_span()` exposes the remaining contiguous range in a page segment for batch reads. Vertex property projection and top-N selection use a reader local to each evaluation loop to reuse this span; reference columns shared by readers do not contain mutable cursor state.

Page payloads from all columns in one checkpoint are packed into immutable objects with a 64 MiB target and 16-byte slice alignment. Each column persists a self-contained directory with a format version, row width, logical and physical row counts, object IDs, and page slices (object index, byte offset, length, and CRC32C). A boundary page can have an immutable prefix slice and a writable append suffix. The directory has its own checksum and is parsed by the same decoder during reopen and garbage collection. Immutable objects share one direct mapping within a checkpoint, including in disk mode; each page's payload checksum is verified on its first access. Moving the database directory preserves relative object references. The original unversioned whole-chunk directory is read and converted on the next checkpoint.

An incremental checkpoint writes dirty page segments and reuses clean slices. Directory rewriting still scales with allocated page count. Checkpoint-wide finalization owns the captured page versions until all packed objects are sealed; it does not borrow columns destroyed by table disassembly. Mutable pages are allocated from shared 2 MiB arenas to avoid one huge-page or runtime-file allocation per page.

Before publishing an insert view, storage prepares stable writable slots above the allocated VID/EID high-water mark. Concurrent insert transactions write distinct reserved rows without page allocation or pointer replacement during WAL application. Splitting a mutable boundary page in a private COW workspace copies its prefix instead of freezing the shared source, so aborting or discarding a no-op workspace leaves the published append area writable. The COW byte counter includes copies made while preparing the append area. Recreating a deleted vertex key requires covering an existing VID: autocommit detects this before WAL append, releases insert admission, and re-executes the statement through COW. Low-level insert callers receive an error and `RequiresCowRetry()` for that case.

Variable-length (`VARCHAR`) and composite (`ARRAY`, `LIST`) properties keep their existing column types, and a checkpoint written before this format reopens with its original `TypedColumn` modules; a legacy typed column is converted to the chunked layout through the `ChunkedColumn::FromLegacy` migration hook.

When an incremental checkpoint migrates an otherwise clean legacy table, it replaces and dumps only the fixed-length property columns. It reuses the previous primary-key, index, timestamp, and CSR modules without consuming buffers pinned by older snapshots. Dirty tables retain the normal detach/disassemble/reopen path. All retained property columns rebind their allocation context to the new checkpoint before the previous checkpoint can be released.
