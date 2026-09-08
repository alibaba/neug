# Operator batch streams

`IOperator::Eval` accepts and returns a `Stream<DataChunk>` (inside `result`
for setup errors). The stream is a move-only, single-consumer synchronous pull
interface. `Next()` returns a batch, EOF, or a status. Empty batches retain their
schema and do not mean EOF. EOF and failures are terminal. Destroying the stream
releases its reader without reading the rest of the input.

Each batch preserves the execution `head` alongside its `DataChunk`. Stream
metadata carries output `tag_ids`, including for zero-row results. This keeps
anonymous columns and head-only seeds working without adding execution state to
`DataChunk`.

`Pipeline::Execute` exchanges streams between every operator and materializes
only its public result. It drains or destroys all streams before returning, so
storage, operator and timer references captured by transforms remain valid.
PROFILE measures production when `Next()` executes, excluding nested upstream
pull time from the downstream operator's charge. Deferred read failures retain
producer names as they propagate through consumers.

## Operator implementations

- `DataSourceOpr` copies its cached read configuration per execution and creates
  its reader on the first pull. CSV, JSON and JSONL provide a `supplierFunc`.
  Readers without this callback use their existing materialized reader on the
  first pull. The execution pipeline does not inspect the file format.
- Every concrete operator implements `Eval` with stream input and output; there
  is no Context-returning Eval overload or adapter base class.
- Project and Select return lazy transforms that process one batch at a time,
  so COPY subqueries with these operators remain incremental.
- Operators retaining materialized algorithms explicitly call `materialize`
  inside their own Eval and return `stream_from_context` afterward. Context is
  an internal algorithm container. Sorting, grouping and joins still buffer
  their input; their global-input semantics need dedicated streaming algorithms.
- `BatchInsertVertexOpr` and `BatchInsertEdgeOpr` consume the input stream through
  the existing storage supplier API. They check its terminal status before
  reporting success, so late reader errors abort COPY's private workspace.
- Sink changes output tags without consuming or buffering rows.

There is one Source → transforms → BatchInsert chain. No CSV-specific fused
operators or adjacency-based optimizer rules are required.

## Remaining materialization

This changes the execution interface, not every underlying algorithm. CSV still
counts each file before parsing it; files are initialized one at a time. JSON
array decoding still builds a document, and JSONL retains its existing counting
pass. Edge insertion accumulates endpoint IDs and property batches in storage;
vertex insertion retains newly inserted IDs for index maintenance. These are
separate opportunities to reduce memory or repeated IO. This interface is lazy,
not asynchronous, and does not add parallel loading.
