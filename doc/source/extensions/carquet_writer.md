# Carquet Parquet writer

NeuG's Parquet extension uses a Carquet implementation of `QueryExportWriter`.
`COPY TO` invokes it through the existing query export and
output stream interfaces for both local and remote destinations. Backend
selection is internal and does not add a query option.

## Supported values

| Query result | Parquet representation |
| --- | --- |
| Signed and unsigned 32/64-bit integers | INT32/INT64, with unsigned logical annotations where needed |
| FLOAT, DOUBLE, BOOL | FLOAT, DOUBLE, BOOLEAN |
| STRING | UTF-8 BYTE_ARRAY |
| DATE | DATE in days since the Unix epoch; millisecond payloads are normalized to their containing UTC day |
| TIMESTAMP | UTC TIMESTAMP in milliseconds, matching NeuG's internal unit |
| LIST and fixed ARRAY | Standard LIST, preserving empty lists, parent NULLs and child NULLs |
| STRUCT | Optional group with `field_0`, `field_1`, … children |
| INTERVAL, vertex, edge and path | Existing string values from `QueryResponse` |

Nested lists and structs can contain the same scalar and special values.
Fixed arrays retain the existing LIST export representation; when an entry
schema supplies fixed dimensions, the writer validates each dimension. It does
not add fixed-size-list metadata or reinterpret graph values as new structures.

## Options

The writer accepts the existing case-insensitive Parquet export options:

| Option | Default | Supported values |
| --- | --- | --- |
| `compression` | `snappy` | `none` / `uncompressed`, `snappy`, `gzip` / `zlib`, `zstd` / `zstandard` |
| `row_group_size` | `1048576` | Positive row count |
| `dictionary_encoding` | `true` | Boolean; boolean leaves use plain encoding |

## Integration and ownership

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

## Validation

Configure NeuG with `BUILD_TEST=ON` and `BUILD_EXTENSIONS=parquet`, then build
`parquet_carquet_export_test` and run it with CTest:

```bash
cmake --build build --target parquet_carquet_export_test -j2
ctest --test-dir build -R '^parquet_carquet_export_test$' --output-on-failure
```

Tests generate Parquet in memory and use the existing Arrow reader as an
independent format oracle. They cover scalar and nested values, timestamp units,
NULL bitmaps, row group and page boundaries, compression and dictionary options,
malformed responses and output failures. No new binary fixtures are required.
Arrow is used only as an independent format oracle in this focused test; the
Carquet writer implementation itself does not use it.
