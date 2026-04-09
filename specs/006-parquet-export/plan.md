# Implementation Plan: Parquet Export Support

**Branch**: `006-parquet-export` | **Date**: 2024-XX-XX | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/006-parquet-export/spec.md`

## Summary

Add Parquet file export support to NeuG's `COPY TO` command by implementing a new `ArrowParquetExportWriter` class in the `extension/parquet/` module. The implementation leverages Apache Arrow's `parquet::arrow::FileWriter` for streaming batch writes, supporting compression options (none, snappy, zlib, zstd) and configurable row group sizes. The feature follows the existing Export Function pattern established by CSV and JSON exporters.

## Technical Context

**Language/Version**: C++20  
**Primary Dependencies**: 
- Apache Arrow (parquet::arrow::FileWriter, WriterProperties)
- NeuG Export Writer framework (QueryExportWriter base class)
- NeuG Execution Engine (execution::Context, ArrowArrayContextColumn)

**Storage**: Local filesystem via Arrow FileSystem abstraction (supports local, S3, HDFS)  
**Testing**: Python pytest (via neug Python bindings), C++ unit tests

**Key Technical Decisions**:
1. **Streaming writes**: Use `parquet::arrow::FileWriter` instead of `WriteTable()` to support large datasets without OOM
2. **Single file output**: Initial implementation writes to single Parquet file (multi-file splitting deferred to P2)
3. **Arrow schema preservation**: Store Arrow schema in Parquet metadata for easier roundtrip reads
4. **Compression defaults**: SNAPPY compression by default (balance of speed and size)

## Project Structure

```text
extension/parquet/
├── CMakeLists.txt                    # Update: add export sources
├── include/
│   ├── parquet_options.h             # Existing: Parquet read options
│   ├── parquet_read_function.h       # Existing: Parquet read function
│   └── parquet_export_function.h     # NEW: Parquet export function header
└── src/
    ├── parquet_extension.cpp         # Existing: Extension registration
    ├── parquet_options.cc            # Existing: Options parsing
    ├── parquet_read_function.cc      # Existing: Parquet read implementation
    └── parquet_export_function.cc    # NEW: Parquet export implementation

tools/python_bind/tests/
└── test_export.py                    # Existing: Add Parquet export tests
```

**Structure Decision**: Add export functionality to existing `extension/parquet/` module (co-locate with Parquet read for consistency). This follows the pattern of JSON extension which has both read and export functions.

## Data Model

This feature has the following data models:

### 1. ArrowParquetExportWriter

**Data Structure**:

C++ class that inherits from `QueryExportWriter` and manages Parquet file writing state:

```cpp
class ArrowParquetExportWriter : public QueryExportWriter {
 private:
  // Parquet writer state
  std::shared_ptr<parquet::arrow::FileWriter> parquet_writer_;
  std::shared_ptr<arrow::io::FileOutputStream> outfile_;
  bool initialized_ = false;
  
  // Export options (parsed from COPY TO options)
  arrow::Compression::type compression_;      // e.g., SNAPPY, ZSTD
  int64_t row_group_size_;                     // e.g., 1048576 (1M rows)
  bool dictionary_encoding_;                   // true/false
};
```

**Data Access & Update**:

- **Initialization**: First call to `writeTable()` initializes FileWriter with schema and options
- **Batch writes**: Each subsequent `writeTable()` call appends a row group to the file
- **Cleanup**: Destructor closes FileWriter and writes Parquet footer
- **State management**: `initialized_` flag tracks whether FileWriter has been created

### 2. Export Options (WriterProperties)

**Data Structure**:

Arrow WriterProperties configuration mapped from NeuG COPY TO options:

```cpp
// User specifies in Cypher:
COPY (query) TO 'output.parquet' 
(compression='snappy', row_group_size=1048576, dictionary_encoding=true)

// Internally converted to:
auto props = parquet::WriterProperties::Builder()
    .compression(arrow::Compression::SNAPPY)
    .max_row_group_length(1048576)
    .enable_dictionary()
    .build();
```

**Data Access & Update**:

- **Parse**: Options parsed from `schema_.options` map during `initializeWriter()`
- **Validate**: Unknown compression codecs rejected with error
- **Apply**: Properties passed to `parquet::arrow::FileWriter::Open()`

### 3. Type Mapping Table

**Data Structure**:

Mapping between NeuG LogicalType and Arrow DataType:

```cpp
NeuG Type          → Arrow Type
─────────────────────────────────
INT64              → arrow::int64()
INT32              → arrow::int32()
UINT64             → arrow::uint64()
UINT32             → arrow::uint32()
DOUBLE             → arrow::float64()
FLOAT              → arrow::float32()
BOOLEAN            → arrow::boolean()
STRING             → arrow::utf8()
DATE               → arrow::date32()
TIMESTAMP          → arrow::timestamp(arrow::TimeUnit::MICROSECOND)
LIST<T>            → arrow::list(arrow_type<T>)
VERTEX             → arrow::struct(fields...)
EDGE               → arrow::struct(fields...)
```

**Data Access & Update**:

- **Lookup**: `logicalTypeToArrow()` function converts types during schema building
- **Extension**: New types added by adding case to switch statement
- **Error handling**: Unsupported types throw `INVALID_ARGUMENT_EXCEPTION`

## Algorithm Model

This feature has the following algorithm models:

### 1. Streaming Parquet Write Algorithm

**Algorithm Target**: Write query results to Parquet file in batches without loading entire dataset into memory

**Algorithm Details**:

```
Input: execution::Context (contains batches of ArrowArrayContextColumn)
Output: Parquet file at specified path

Steps:
1. For each batch from execution::Context:
   a. If first batch (initialized_ == false):
      - Build Arrow Schema from batch column types
      - Parse export options (compression, row_group_size, etc.)
      - Create WriterProperties with options
      - Open output file stream
      - Initialize parquet::arrow::FileWriter
      - Set initialized_ = true
   
   b. Convert QueryResponse to Arrow Table:
      - Extract Arrow arrays from ArrowArrayContextColumn
      - Create arrow::Table with schema and arrays
   
   c. Write Table to Parquet:
      - Call parquet_writer_->WriteTable(*table, table->num_rows())
      - Each WriteTable() creates one row group

2. On completion (Context exhausted):
   - Destructor closes FileWriter
   - Parquet footer written automatically
   - File stream flushed and closed

Example:
  Batch 1 (1000 rows) → Initialize → Write Row Group 1
  Batch 2 (1000 rows) → Write Row Group 2
  Batch 3 (500 rows)  → Write Row Group 3
  Close → Write Footer → File complete (2500 rows total)
```

**Key Properties**:
- **Memory efficiency**: Only one batch in memory at a time
- **Atomic writes**: File valid only after successful Close()
- **Error recovery**: Partial file deleted on failure (cleanup in destructor)

### 2. Compression Selection Algorithm

**Algorithm Target**: Map user-specified compression string to Arrow Compression enum

**Algorithm Details**:

```
Input: string compression_option (e.g., "snappy", "zstd")
Output: arrow::Compression::type enum value

Mapping:
  "none"   → arrow::Compression::UNCOMPRESSED
  "snappy" → arrow::Compression::SNAPPY
  "zlib"   → arrow::Compression::ZLIB
  "zstd"   → arrow::Compression::ZSTD

Default: If no compression option specified → SNAPPY

Error handling:
  - Unknown codec → THROW_INVALID_ARGUMENT_EXCEPTION
  - Empty string → Use default (SNAPPY)

Example:
  User specifies: (compression='zstd')
  Algorithm returns: arrow::Compression::ZSTD
  Result: Parquet file compressed with Zstandard
```

### 3. Schema Inference Algorithm

**Algorithm Target**: Build Arrow Schema from first batch of query results

**Algorithm Details**:

```
Input: QueryResponse (first batch)
Output: std::shared_ptr<arrow::Schema>

Steps:
1. For each column in QueryResponse:
   a. Get column name from context column metadata
   b. Get column type from ArrowArrayContextColumn
   c. Convert NeuG LogicalType to Arrow DataType
   d. Create arrow::field(name, arrow_type)

2. Combine fields into arrow::schema(fields)

3. Return schema

Example:
  Query: MATCH (n:person) RETURN n.ID, n.name, n.age
  
  Column 1: "n.ID"    → INT64    → arrow::int64()
  Column 2: "n.name"  → STRING   → arrow::utf8()
  Column 3: "n.age"   → INT32    → arrow::int32()
  
  Result Schema:
    schema([
      field("n.ID", int64),
      field("n.name", utf8),
      field("n.age", int32)
    ])
```

**Edge Cases**:
- Empty query result → Create schema with zero rows
- Complex types (VERTEX/EDGE) → Serialize to JSON string or struct
- NULL columns → Use arrow::null() type
