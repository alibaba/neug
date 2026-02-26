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

## Supported Formats

| Format | Supported | Availability |
|--------|-----------|--------------|
| CSV | ✅ | Built-in |
| JSON / JSONL | ✅ | Via [JSON Extension](../extensions/load_json) |
| Parquet | — | Planned v0.2 |

> **Note:** As new format extensions are developed, both `LOAD FROM` and `COPY FROM` gain support automatically. See the [Extensions](../extensions/index) page for details.

## What's Next

- **[LOAD FROM](load_data)** — Read external files into temporary tables with relational operations
- **[COPY FROM](import_data)** — Persist external data into graph storage
- **[COPY TO](export_data)** — Export query results to external files
