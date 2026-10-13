# Carquet Parquet Backend

NeuG's Parquet extension uses a Carquet-based reader and writer. Both implement
the existing input-stream, data-chunk and query-export interfaces, so the
migration does not change the public supplier or stream interfaces.

After `LOAD PARQUET`, `LOAD FROM` uses the Carquet reader for local files and
remote streams supplied by HTTPFS. `COPY TO` invokes the Carquet writer through
the existing query-export and output-stream interfaces for local and remote
destinations. Backend selection is internal and does not add a SQL option.
The previous Arrow implementation and dependencies remain during the migration.

The user-facing type mappings and read/write options are documented in the
[Parquet user documentation](../../doc/source/extensions/load_parquet.md).

## Reader

`CarquetSniffer` obtains the schema through an `InputStreamFactory`.
`CarquetChunkSupplier` implements `IDataChunkSupplier` and returns NeuG-owned
`DataChunk` columns. Returned values remain valid after subsequent reads or
supplier destruction. The `scanCarquet` entry point coordinates these
suppliers for one or more files with compatible schemas.

### Reading and filtering

The scan reads the union of requested output columns and all columns referenced
by the predicate, including nested expressions. It passes the complete predicate
and current query parameters to NeuG's shared chunk filter, then restores the
requested output column order and removes filter-only columns. Repeated output
columns are preserved. An empty projection means all columns.

Before decoding, the reader may skip row groups using scalar min/max and NULL
statistics. Comparisons, reversed comparisons, `IS NULL`, and Boolean combinations
are handled conservatively. Missing or incompatible statistics and unsupported
expressions keep the affected groups. A supported condition in an `AND` may still
prove that a group cannot match when another condition is unsupported. The
complete predicate is evaluated on decoded rows in all cases; statistics never
replace exact filtering. Parameters, casts and complex functions currently use
this exact-filtering path without native statistics evaluation.

Carquet's `ArrowSchema` and `ArrowArray` structures are the C Data interchange
ABI declared by Carquet; these adapters do not link against Arrow to convert
values. The previous Arrow implementation remains in the source tree during the
migration, but the registered Parquet read function uses Carquet.

### Batching, concurrency and memory

The scan accepts the existing `batch_read`, `parallel`, `BATCH_SIZE`,
`BUFFERED_STREAM`, `PRE_BUFFER` and `ENABLE_IO_COALESCING` options.
`batch_rows` controls the maximum rows returned in each supplier chunk
(default 65,536); `PARQUET_BATCH_ROWS` is still honored as a deprecated alias.
Full reads merge the resulting chunks with the shared helper.
Parallel scans use independent streams for row-group tasks and preserve file and
row-group order when collecting results.

Chunk size is **not a decoder memory limit**. The simple scalar path can use
Carquet's batch reader. Nested values, physical projection, selected row groups,
pruning and prebuffering use its row-group API, which decodes a complete selected
row group before splitting it into chunks. Parallel tasks can hold several row
groups simultaneously, and full reads retain the complete result.

Local and remote streams use the same interface. Projection and pruning can
reduce remote range reads, although buffering and footer inspection may fetch
additional bytes. The HTTPFS integration test measures actual HTTP response
bytes against the same projected read with pruning disabled.

## Writer

`CarquetExportWriter` is the Carquet implementation of
`QueryExportWriter`. Struct columns are written as an optional group with
`field_0`, `field_1`, … children, and boolean leaves always use plain encoding
regardless of the dictionary option.

### Integration and ownership

`CarquetExportWriter` receives the materialized `QueryResponse` produced by the
existing `QueryExportWriter`. It validates the response, options, column names,
date ranges and nested array shape before opening the output. Invalid offsets,
short validity bitmaps, mismatched child lengths and nesting deeper than 64
levels are rejected. Empty results follow the existing export contract and do
not open an output file.

Declared LIST/ARRAY columns must use a list response, and TUPLE columns must use
a struct response. Shape mismatches are rejected recursively, even for all-NULL
columns, before opening the output stream.

An injected stream opener connects the writer to an existing `OutputStream`;
the default opens a local file. The Carquet output adapter commits the stream
only after a successful footer write. Failures abort partial output and release
the writer and its temporary buffers.

Scalar columns use Carquet's batch API. Responses containing nested columns use
a private C Data bridge with owned buffers for one row group at a time. The
bridge uses the ABI declarations shipped by Carquet and requires no Arrow C++
headers, libraries or additional toolchain. The complete query result remains
materialized; this is not an end-to-end streaming export implementation.

The writer uses the Carquet submodule and adjacent patch already present in
NeuG. It does not depend on the Carquet reader implementation or alter public
IO interfaces or CSV/JSON writers. The previous Arrow writer remains in the
source tree during the migration, but the registered Parquet export function
constructs the Carquet writer.

## Building and testing

Include `parquet` in `BUILD_EXTENSIONS`. To build and run the focused tests,
also enable `BUILD_TEST=ON`, then build and run the reader tests:

```sh
cmake --build build --target parquet_carquet_test -j2
ctest --test-dir build -R '^parquet_carquet' --output-on-failure
```

For the writer, build `parquet_carquet_export_test` and run it with CTest:

```sh
cmake --build build --target parquet_carquet_export_test -j2
ctest --test-dir build -R '^parquet_carquet_export_test$' --output-on-failure
```

With `httpfs` also enabled, `httpfs_extension_test` includes a local HTTP Range
integration test. Tests reuse the repository's datasets and generate additional
files in memory; the C++ suite does not require PyArrow.

Writer tests generate Parquet in memory and use the existing Arrow reader as an
independent format oracle. They cover scalar and nested values, timestamp units,
NULL bitmaps, row group and page boundaries, compression and dictionary options,
malformed responses and output failures. No new binary fixtures are required.
Arrow is used only as an independent format oracle in this focused test; the
Carquet writer implementation itself does not use it.

## Carquet submodule and patch

Carquet remains a pinned submodule with its local changes in the adjacent
`third_party/carquet.patch`. Reader changes add recursive fixed-size-list
metadata restoration and selection of top-level fields before nested decoding.
The patch also retains the existing callback IO and page-encoding fixes.
Backend-specific code stays inside the Parquet extension, allowing a later
replacement without changing the public supplier or stream interfaces.
