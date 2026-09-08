# Operator batch streams

Every concrete `IOperator::Eval` accepts and returns `Stream<ContextChunk>`
(`result` reports setup errors). A stream is a move-only synchronous pull
interface. `Next()` produces one batch, EOF, or a terminal error. An empty
batch retains its schema and is not EOF. Destroying the stream releases its
cursor without reading the rest of its input.

`Next()` returns `result<std::optional<ContextChunk>>` directly. ContextChunk
owns its DataChunk and anonymous execution head; Stream has no additional Batch
wrapper. Readers and storage suppliers keep their DataChunk interface and adapt
once at the Source/BatchInsert boundary. Between operators, the same ContextChunk
is moved directly through the kernels. Stream tag_ids retain output aliases.

## Execution and state

- Source copies its read configuration per execution and opens its reader on
  first demand. CSV, JSON and JSONL provide the same supplier interface.
- Project, Select, Unfold, vertex/edge expansion, path operations, intersection
  and the triangle kernel use `map_chunks`. One pull invokes the kernel on one
  batch and returns that batch directly; no Context is constructed.
- Limit owns its cross-batch skip and remaining-row counters in the returned
  stream. Once satisfied, it releases upstream without pulling another batch.
- Union buffers its shared input for branch replay, then concatenates branch
  streams on demand. It does not collect or flatten branch outputs.
- The existing sorting, fused Project/OrderBy, grouping, deduplication and
  general join kernels require complete input. Their explicit collection
  boundaries operate on batches, without converting through Context. The
  primary-key join can process each right-side batch independently.
- Mutations retain read/write barriers so a downstream LIMIT cannot skip
  writes and writes cannot invalidate data that is still being read. Edge
  update/merge retains affected batches to refresh their property pointers.
- BatchInsert consumes the same stream through the storage supplier API and
  checks terminal reader errors before reporting success. Sink only sets tags.
- DDL and administration execute once and pass through their stream.

All mutable cursors and counters belong to the execution's stream, not the
cached operator. `Pipeline::ExecuteStream` connects nested plans without
materializing their results. Its caller must keep the pipeline, storage and
PROFILE timer alive until consumption or destruction. The external
`Pipeline::Execute` query boundary still materializes its public Context result.

PROFILE charges production during `Next()` and excludes nested upstream time
within a pipeline. Errors retain their producer's operator name while passing
through downstream consumers.

## Compatibility boundaries and remaining buffering

Index-scan and export extension callbacks still take Context in their existing
ABI; those operators convert only at that explicit callback boundary. Procedure
and GDS callbacks still return Context, which is exposed as a stream once.
Every registered reader must supply a supplier factory; there is no materialized
reader callback or fallback. Parquet uses RecordBatchReader directly without a
CountRows or ToTable pass. Reader classes expose only supplier creation and schema
inference: their Context-based read methods, ReadLocalState, and the obsolete
batch_read option have been removed. Reader tests consume suppliers directly.

This does not replace every underlying algorithm with an incremental one.
Graph scan kernels may still produce a large single batch. CSV counts a file
before parsing; JSON array decoding still builds a document. Global sorting,
grouping, deduplication and general join still buffer their inputs. Edge storage
still accumulates endpoints and property batches, and mutation barriers retain
input as described above. The interface is lazy, not asynchronous, and does not
add parallel loading.
