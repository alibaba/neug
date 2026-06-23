# Overview

NeuG provides a set of tools for moving data in and out of your graph database.

## Architecture

The data ingestion pipeline in NeuG is built around a layered design:

```
External Files (CSV, JSON, Parquet, ...)
        │
        ▼
   ┌───────────┐    Schema inference, relational operations
   │ LOAD FROM │    (projection, filtering, type casting, aggregation, ...)
   └────┬──────┘
        │  Unified internal format
        ▼
   ┌───────────┐    Persist into graph storage
   │ COPY FROM │    (requires predefined schema)
   └───────────┘
```

**`LOAD FROM`** is the foundation of data ingestion. It reads external files, automatically infers the schema, and produces a temporary result set. You can apply relational operations — such as column projection, type casting, filtering, and aggregation — directly on the loaded data.

**`COPY FROM`** builds on top of `LOAD FROM`. It takes the result of a `LOAD FROM` operation and persists it into graph storage. Because `COPY FROM` uses `LOAD FROM` internally, **any file format supported by `LOAD FROM` is automatically available for `COPY FROM`** as well.

**`COPY TO`** works in the opposite direction — it exports query results to external file formats.

## Embedded Mode Only

> **Important:** `LOAD FROM`, `COPY FROM`, and `COPY TO` are supported **only in embedded mode**. They are not available when NeuG is running as a service (HTTP/TP mode). This is a current limitation; support for bulk loading in service mode is planned for a future release.

Bulk file I/O operations (`LOAD FROM`, `COPY FROM`, `COPY TO`) involve reading or writing large files, which are long-running, I/O-intensive operations that would block the transaction processing pipeline. For this reason, they are restricted to embedded mode.  Once the service is running, you can still insert individual records via `CREATE` statements, modify data with `MERGE`/`SET`/`DELETE`, and manage schema with `CREATE/DROP/ALTER TABLE`.

## Supported Formats

| Format | Supported | Availability |
|--------|-----------|--------------|
| CSV | ✅ | Built-in |
| JSON / JSONL | ✅ | Built-in (since v0.1.2) |
| Parquet | ✅  | Via [Parquet Extension](../extensions/load_parquet) |

> **Version Note:** In NeuG < 0.1.2, JSON/JSONL support was provided via the [JSON Extension](../extensions/load_json) and required `INSTALL json; LOAD json;` before use. Since NeuG >= 0.1.2, JSON/JSONL is a built-in feature — no extension installation or loading is needed.

> **Note:** As new format extensions are developed, both `LOAD FROM` and `COPY FROM` gain support automatically. See the [Extensions](../extensions/index) page for details.

## What's Next

- **[LOAD FROM / LOAD AS](load_data)** — Read external files for relational operations or create temporary graph tables
- **[COPY FROM](import_data)** — Persist external data into graph storage
- **[COPY TO](export_data)** — Export query results to external files
