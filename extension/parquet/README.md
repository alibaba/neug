# Parquet Extension

The Parquet extension provides Parquet file import and export for NeuG:

- **Import**: load external Parquet files with the `LOAD FROM` Cypher syntax
- **Export**: export query results to Parquet files with `COPY TO`

User documentation lives in
[doc/source/extensions/load_parquet.md](../../doc/source/extensions/load_parquet.md).

## Backend

The production backend uses **[Carquet](../../third_party/carquet)** for both
reading (`LOAD FROM`) and writing (`COPY TO`). Carquet is a C-level Parquet
library vendored as a submodule with NeuG-local patches in
[third_party/carquet.patch](../../third_party/carquet.patch). Backend selection
is internal and does not add a SQL option.

See [Carquet Parquet Backend](carquet_parquet_backend.md) for the design and
implementation notes.
