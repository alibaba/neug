# Module 1: Core Parquet Export

**Module ID**: M001  
**Priority**: P1  
**Goal**: Enable basic export of Cypher query results to Parquet file format with default settings (SNAPPY compression, 1M row groups)

**GitHub Settings**:
- **Assignee**: [TBD]
- **Label**: `feature`, `parquet`, `export`, `P1`
- **Milestone**: [TBD]
- **Project**: External Data Sources

**Independent Test**: Can be fully tested by executing `COPY (MATCH (n:person) RETURN n.ID, n.name) TO 'person.parquet'` and verifying the output file is readable by PyArrow

---

## Tasks

### [F006-T101] Create Parquet Export Function Header

**Description**: Create header file for Parquet export function declaration

**Details**:
- Create file: `extension/parquet/include/parquet_export_function.h`
- Define `ArrowParquetExportWriter` class inheriting from `QueryExportWriter`
- Include necessary headers:
  - `<parquet/arrow/writer.h>`
  - `<parquet/properties.h>`
  - `<arrow/io/file.h>`
  - `"neug/utils/writer/writer.h"`
- Declare class members:
  - `parquet_writer_` (std::shared_ptr<parquet::arrow::FileWriter>)
  - `outfile_` (std::shared_ptr<arrow::io::FileOutputStream>)
  - `initialized_` (bool)
- Declare methods:
  - Constructor
  - `writeTable()` override
  - Destructor

**Reference**: See `extension/json/include/json_export_function.h` for pattern

---

### [F006-T102] Implement Parquet Export Writer Class

**Description**: Implement core ArrowParquetExportWriter class with streaming write capability

**Details**:
- Create file: `extension/parquet/src/parquet_export_function.cc`
- Implement constructor:
  - Call parent QueryExportWriter constructor
  - Initialize member variables to null/false
- Implement `writeTable()` method:
  - Check if initialized, if not call `initializeWriter()`
  - Convert QueryResponse to Arrow Table
  - Call `parquet_writer_->WriteTable(*table, table->num_rows())`
- Implement destructor:
  - Check if parquet_writer_ exists and initialized_ is true
  - Call `parquet_writer_->Close()`
  - Log error if close fails

**Key Implementation Points**:
- Use ARROW_ASSIGN_OR_RAISE for Arrow operations
- Return neug::Status for error handling
- Follow JSON Export Writer pattern for structure

---

### [F006-T103] Implement Writer Initialization Logic

**Description**: Implement initializeWriter() method to setup FileWriter with schema and default options

**Details**:
- Add private method `initializeWriter(const QueryResponse* first_table)`
- Implementation steps:
  1. Build Arrow Schema from first batch:
     - Extract column types from QueryResponse
     - Convert to Arrow types using `logicalTypeToArrow()`
     - Create `arrow::schema(fields)`
  2. Create default WriterProperties:
     - Compression: SNAPPY
     - Max row group length: 1048576 (1M rows)
     - Enable dictionary encoding
  3. Create ArrowWriterProperties:
     - Enable `store_schema()` for metadata preservation
  4. Open output file:
     - Use `fileSystem_->OpenOutputStream(schema_.paths[0])`
  5. Create FileWriter:
     - Call `parquet::arrow::FileWriter::Open()`
  6. Set `initialized_ = true`

**Error Handling**:
- File open failure → Return error status
- Schema build failure → Return error status
- FileWriter creation failure → Return error status

---

### [F006-T104] Implement Type Conversion Function

**Description**: Implement logicalTypeToArrow() function to map NeuG types to Arrow types

**Details**:
- Add function: `std::shared_ptr<arrow::DataType> logicalTypeToArrow(const common::LogicalType& type)`
- Implement type mappings:
  ```cpp
  INT64    → arrow::int64()
  INT32    → arrow::int32()
  UINT64   → arrow::uint64()
  UINT32   → arrow::uint32()
  DOUBLE   → arrow::float64()
  FLOAT    → arrow::float32()
  BOOLEAN  → arrow::boolean()
  STRING   → arrow::utf8()
  DATE     → arrow::date32()
  TIMESTAMP → arrow::timestamp(arrow::TimeUnit::MICROSECOND)
  ```
- Default case:
  - Throw `INVALID_ARGUMENT_EXCEPTION` with type name

**Reference**: See existing type conversion in Arrow Reader code

---

### [F006-T105] Implement QueryResponse to Arrow Table Conversion

**Description**: Convert NeuG QueryResponse batches to Arrow Tables for Parquet writing

**Details**:
- Add method: `convertToArrowTable(const QueryResponse* table, const std::shared_ptr<arrow::Schema>& schema)`
- Implementation:
  1. Extract `context_columns()` from QueryResponse
  2. For each column:
     - Check if it's ArrowArrayContextColumn
     - Extract Arrow array using `get_arrow_array()`
     - Add to columns vector
  3. Create Arrow Table:
     - `arrow::Table::Make(schema, columns)`
  4. Return table

**Edge Cases**:
- Empty result → Create table with zero rows
- Mixed column types → Handle each type separately
- NULL values → Preserve null bitmap from Arrow arrays

---

### [F006-T106] Register COPY_PARQUET Export Function

**Description**: Register Parquet export function in extension system

**Details**:
- In `parquet_export_function.cc`, add:
  ```cpp
  namespace function {
  struct ExportParquetFunction : public ExportFunction {
    static constexpr const char* name = "COPY_PARQUET";
    static function_set getFunctionSet();
  };
  ```
- Implement `getFunctionSet()`:
  - Create ExportFunction with name "COPY_PARQUET"
  - Set `bind` function (reuse JSON export bind pattern)
  - Set `execFunc` to `parquetExecFunc`
  - Return function_set
- Implement `parquetExecFunc`:
  - Get FileSystem from MetadataRegistry
  - Create ArrowParquetExportWriter
  - Call `writer->write(ctx, graph)`
  - Handle errors and clear context

---

### [F006-T107] Update Parquet Extension CMakeLists

**Description**: Add export function source files to CMake build

**Details**:
- Edit file: `extension/parquet/CMakeLists.txt`
- Add export sources:
  ```cmake
  set(PARQUET_EXPORT_SOURCES
      src/parquet_export_function.cc
  )
  
  set(PARQUET_SOURCES
      ${PARQUET_READ_SOURCES}
      ${PARQUET_EXPORT_SOURCES}
  )
  ```
- Add export headers to include list
- Verify build completes without errors

**Testing**: Run `make parquet_extension` to verify compilation

---

### [F006-T108] Implement Extension Registration

**Description**: Register Parquet export function when extension loads

**Details**:
- Edit file: `extension/parquet/src/parquet_extension.cpp`
- In extension initialization function:
  - Call `ExportParquetFunction::getFunctionSet()`
  - Register functions with catalog:
    ```cpp
    auto parquetExportFunctions = ExportParquetFunction::getFunctionSet();
    for (auto& func : parquetExportFunctions) {
      catalog->registerFunction(std::move(func));
    }
    ```
- Verify function registered with name "COPY_PARQUET"

**Reference**: See how JSON extension registers export functions

---

**Module 1 Summary**: 8 tasks (T101-T108)  
**Estimated Effort**: 2-3 days  
**Dependencies**: None (can start immediately)  
**Deliverable**: Basic Parquet export with SNAPPY compression, 1M row groups, dictionary encoding enabled
