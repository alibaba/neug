# Parquet Extension

Apache Parquet is a columnar storage format widely used in data engineering and analytics workloads. NeuG supports both Parquet file import and export functionality through the Extension framework.

- **Import**: Load external Parquet files using `LOAD FROM` syntax
- **Export**: Export query results to Parquet files using `COPY TO` syntax

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

## Export to Parquet

NeuG supports exporting query results to Parquet files using the `COPY TO` command. This is useful for:
- **Data archiving**: Store query results in efficient columnar format
- **Data sharing**: Exchange data with other analytics tools (Spark, Pandas, DuckDB, etc.)
- **Performance**: Parquet's columnar format provides excellent compression and query performance

### Basic Export Syntax

Export query results to a Parquet file:

```cypher
COPY (
    MATCH (p:person) 
    RETURN p.ID, p.fName, p.age
) TO 'output.parquet';
```

### Export Options

The following options control how Parquet files are written:

| Option                 | Type   | Default  | Description                                                                                                      |
| ---------------------- | ------ | -------- | ---------------------------------------------------------------------------------------------------------------- |
| `compression`          | string | `snappy` | Compression codec: `snappy`, `gzip`, `zstd`, `lz4`, `brotli`, or `none`                                         |
| `row_group_size`       | int64  | `1048576`| Number of rows per row group (1,048,576 = 1M rows). Larger values improve compression but use more memory.      |
| `dictionary_encoding`  | bool   | `true`   | Enable dictionary encoding for string columns. Reduces file size for columns with repeated values.              |

### Export Examples

#### Export with ZSTD Compression

```cypher
COPY (
    MATCH (p:person) 
    RETURN p.*
) TO 'person.parquet' (compression='zstd');
```

#### Export with Custom Row Group Size

```cypher
COPY (
    MATCH (v:node) 
    RETURN v.*
) TO 'nodes.parquet' (row_group_size=500000);
```

#### Export Without Compression

```cypher
COPY (
    MATCH (p:person)-[k:knows]->(p2:person)
    RETURN p.fName, p2.fName, k.since
) TO 'relationships.parquet' (compression='none');
```

#### Export with Dictionary Encoding Disabled

```cypher
COPY (
    MATCH (p:person)
    RETURN p.fName, p.email
) TO 'contacts.parquet' (dictionary_encoding=false);
```

### Supported Data Types

Parquet export supports all NeuG data types:

**Primitive Types:**
- INT32, INT64, UINT32, UINT64
- FLOAT, DOUBLE, BOOLEAN
- STRING, DATE, TIMESTAMP, INTERVAL

**Complex Types:**
- **List<T>**: Variable-length arrays (e.g., `list<string>`, `list<int64>`)
- **Struct**: Nested structures with named fields
- **Vertex**: Graph vertices exported as struct with `_ID`, `_LABEL`, and properties
- **Edge**: Graph edges exported as struct with `_ID`, `_LABEL`, `_SRC_ID`, `_DST_ID`, and properties
- **Path**: Graph paths exported as struct with `nodes`, `rels`, and `length` fields

### Export Vertex and Edge Data

Export complete vertex objects:

```cypher
COPY (
    MATCH (p:person) 
    RETURN p
) TO 'vertices.parquet';
```

This creates a Parquet file with a struct column containing:
```
p: struct<
  _ID: int64,
  _LABEL: string,
  ID: int64,
  fName: string,
  age: int64,
  ...
>
```

Export complete edge objects:

```cypher
COPY (
    MATCH (p:person)-[k:knows]->(p2:person) 
    RETURN k
) TO 'edges.parquet';
```

This creates a Parquet file with a struct column containing:
```
k: struct<
  _ID: int64,
  _LABEL: string,
  _SRC_ID: int64,
  _DST_ID: int64,
  since: date,
  ...
>
```

### Performance Tips

1. **Choose appropriate compression**: 
   - `snappy`: Good balance of speed and compression (default)
   - `zstd`: Best compression ratio, slightly slower
   - `none`: Fastest, but larger files

2. **Adjust row group size** based on your use case:
   - Large datasets (>10M rows): Use default 1M rows per group
   - Medium datasets (100K-10M rows): Use 500K rows per group
   - Small datasets (<100K rows): Use 100K rows per group

3. **Enable dictionary encoding** for string columns with repeated values (e.g., categories, status codes)

4. **Export only needed columns** to reduce file size:
   ```cypher
   COPY (
       MATCH (p:person) 
       RETURN p.ID, p.fName  -- Not p.*
   ) TO 'subset.parquet';
   ```

### Round-Trip Example

Export data and verify by loading it back:

```cypher
-- Step 1: Export to Parquet
COPY (
    MATCH (p:person) 
    RETURN p.ID, p.fName, p.age
) TO 'export.parquet';

-- Step 2: Load it back to verify
LOAD FROM "export.parquet"
RETURN *;
```
