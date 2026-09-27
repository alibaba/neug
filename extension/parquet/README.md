# Parquet Extension

The Parquet extension provides Parquet file import and export for NeuG:

- **Import**: load external Parquet files with the `LOAD FROM` Cypher syntax
- **Export**: export query results to Parquet files with `COPY TO`

User documentation lives in
[doc/source/extensions/load_parquet.md](../../doc/source/extensions/load_parquet.md).

## Backend

The production backend is currently based on **Apache Arrow** for both reading
(`LOAD FROM`) and writing (`COPY TO`).

The plan is to unify the backend on [Carquet](../../third_party/carquet), a
C-level Parquet library vendored as a submodule with NeuG-local patches in
[third_party/carquet.patch](../../third_party/carquet.patch). A Carquet-based
reader and writer are already implemented and validated in the extension tests,
but they are not selectable in production yet — there is no SQL option to
switch backends.

See [Carquet Parquet Backend](carquet_parquet_backend.md) for the design and
implementation notes of the in-progress migration.
