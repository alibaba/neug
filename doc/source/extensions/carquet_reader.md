# Carquet reader

NeuG's Parquet extension includes a Carquet reader that implements the existing
input-stream and data-chunk interfaces. After `LOAD PARQUET`, `LOAD FROM` uses
this reader for local files and for remote streams supplied by the HTTPFS
extension. Backend selection is internal and does not add a SQL option.

## Reading and filtering

`CarquetSniffer` obtains the schema through an `InputStreamFactory`.
`CarquetChunkSupplier` implements `IDataChunkSupplier` and returns NeuG-owned
`DataChunk` columns. Returned values remain valid after subsequent reads or
supplier destruction. The `scanCarquet` entry point coordinates these
suppliers for one or more files with compatible schemas.

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

## Supported values

| Parquet values | NeuG representation |
| --- | --- |
| Boolean, signed/unsigned integers, float and double | Corresponding scalar type; narrow integers widen to INT32/UINT32 |
| UTF-8 strings, including large strings | VARCHAR |
| Dates and timestamps | DATE and millisecond TIMESTAMP, with unit and overflow checks |
| LIST and LARGE_LIST | LIST, preserving NULL lists, empty lists and NULL elements |
| Fixed-size lists with Arrow schema metadata | ARRAY with the recorded length |
| Supported lists and arrays nested inside one another | Nested LIST/ARRAY values |

MAP schemas can be inspected, but MAP and general STRUCT value columns are not
supported by this reader. INTERVAL values retain their textual storage for
downstream conversion. Unsupported schemas or invalid layouts report errors.

Carquet's `ArrowSchema` and `ArrowArray` structures are the C Data interchange
ABI declared by Carquet; these adapters do not link against Arrow to convert
values. The previous Arrow implementation remains in the source tree during the
migration, but the registered Parquet read function uses Carquet.

## Batching, concurrency and memory

The scan accepts the existing `batch_read`, `parallel`, `BATCH_SIZE`,
`BUFFERED_STREAM`, `PRE_BUFFER` and `ENABLE_IO_COALESCING` options.
`PARQUET_BATCH_ROWS` controls the maximum rows returned in each supplier chunk
(default 65,536). Full reads merge the resulting chunks with the shared helper.
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

## Building and testing

Include `parquet` in `BUILD_EXTENSIONS`. To build and run the focused tests,
also enable `BUILD_TEST=ON`, then run:

```sh
cmake --build build --target parquet_carquet_test -j2
ctest --test-dir build -R '^parquet_carquet' --output-on-failure
```

With `httpfs` also enabled, `httpfs_extension_test` includes a local HTTP Range
integration test. Tests reuse the repository's datasets and generate additional
files in memory; the C++ suite does not require PyArrow.

Carquet remains a pinned submodule with its local changes in the adjacent
`third_party/carquet.patch`. Reader changes add recursive fixed-size-list metadata
restoration and selection of top-level fields before nested decoding. The patch
also retains the existing callback IO and page-encoding fixes. Backend-specific
code stays inside the Parquet extension, allowing a later replacement without
changing the public supplier or stream interfaces.
