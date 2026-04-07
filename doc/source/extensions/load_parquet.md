# Parquet Extension

Apache Parquet is a columnar storage format widely used in data engineering and analytics workloads. NeuG supports Parquet file import functionality through the Extension framework. After loading the Parquet Extension, users can directly load external Parquet files using the `LOAD FROM` syntax.

## Install Extension

```cypher
INSTALL PARQUET;
```

## Load Extension

```cypher
LOAD PARQUET;
```

## Using Parquet Extension

`LOAD FROM` reads Parquet files and exposes their columns for querying. Schema is automatically inferred from the Parquet file metadata by default.

### Parquet Format Options

The following options control how Parquet files are read:

| Option                   | Type  | Default | Description                                                                                                                                 |
| ------------------------ | ----- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `buffered_stream`        | bool  | `true`  | Enable buffered I/O stream for improved sequential read performance.                                                                         |
| `pre_buffer`             | bool  | `false` | Pre-buffer column data before decoding. Recommended for high-latency filesystems such as S3.                                                |
| `enable_io_coalescing`   | bool  | `true`  | Enable Arrow I/O read coalescing (hole-filling cache) to reduce I/O overhead when reading non-contiguous byte ranges. When `true`, uses lazy coalescing; when `false`, uses eager coalescing. |
| `parquet_batch_rows`     | int64 | `65536` | Number of rows per Arrow record batch when converting Parquet row groups into in-memory batches.                                            |

### Query Examples

#### Basic Parquet Loading

Load all columns from a Parquet file:

```cypher
LOAD FROM "person.parquet"
RETURN *;
```

#### Specifying Batch Size

Tune memory usage by adjusting the number of rows read per batch:

```cypher
LOAD FROM "person.parquet" (parquet_batch_rows=8192)
RETURN *;
```

#### Enabling I/O Coalescing

Enable eager I/O coalescing for workloads that benefit from pre-fetching contiguous data:

```cypher
LOAD FROM "person.parquet" (enable_io_coalescing=false)
RETURN *;
```

#### Column Projection

Return only specific columns from Parquet data:

```cypher
LOAD FROM "person.parquet"
RETURN fName, age;
```

#### Column Aliases

Use `AS` to assign aliases to columns:

```cypher
LOAD FROM "person.parquet"
RETURN fName AS name, age AS years;
```

> **Note:** All relational operations supported by `LOAD FROM` — including type conversion, WHERE filtering, aggregation, sorting, and limiting — work the same way with Parquet files. See the [LOAD FROM reference](../data_io/load_data) for the complete list of operations.
