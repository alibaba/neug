# Module 2: Compression & Performance Options

**Module ID**: M002  
**Priority**: P2  
**Goal**: Allow users to configure compression codec and row group size for optimized storage and performance

**GitHub Settings**:
- **Assignee**: [TBD]
- **Label**: `feature`, `parquet`, `export`, `P2`, `compression`
- **Milestone**: [TBD]
- **Project**: External Data Sources

**Independent Test**: Can be fully tested by exporting the same data with different compression settings (none, snappy, zlib, zstd) and comparing file sizes

---

## Tasks

### [F006-T201] Implement Compression Option Parser

**Description**: Parse compression option from COPY TO options and map to Arrow Compression enum

**Details**:
- Add function: `parseCompressionOption(const options_map_t& options)`
- Parse options map for "compression" key
- Map string values to Arrow Compression enum:
  ```cpp
  "none"   → arrow::Compression::UNCOMPRESSED
  "snappy" → arrow::Compression::SNAPPY
  "zlib"   → arrow::Compression::ZLIB
  "zstd"   → arrow::Compression::ZSTD
  ```
- Default value: SNAPPY (if key not present)
- Error handling:
  - Unknown codec → THROW_INVALID_ARGUMENT_EXCEPTION
  - Case-insensitive matching (e.g., "SNAPPY" = "snappy")

**Reference**: See `parquet_options.cc` for option parsing patterns

---

### [F006-T202] Implement Row Group Size Option Parser

**Description**: Parse row_group_size option from COPY TO options

**Details**:
- Add function: `parseRowGroupSizeOption(const options_map_t& options)`
- Parse options map for "row_group_size" key
- Convert string to int64_t
- Default value: 1048576 (1M rows)
- Validation:
  - Minimum: 1024 rows
  - Maximum: 10,000,000 rows
  - Invalid value → THROW_INVALID_ARGUMENT_EXCEPTION with range info

**Example Usage**:
```cypher
COPY (MATCH (n) RETURN n) TO 'output.parquet' 
(row_group_size=65536);
```

---

### [F006-T203] Implement Dictionary Encoding Option Parser

**Description**: Parse dictionary_encoding option from COPY TO options

**Details**:
- Add function: `parseDictionaryEncodingOption(const options_map_t& options)`
- Parse options map for "dictionary_encoding" key
- Convert string to boolean:
  - "true", "1", "yes" → true
  - "false", "0", "no" → false
- Default value: true (enable dictionary encoding)
- Error handling:
  - Invalid boolean string → THROW_INVALID_ARGUMENT_EXCEPTION

**Impact**: Dictionary encoding reduces storage for string columns with repeated values

---

### [F006-T204] Update initializeWriter to Use Parsed Options

**Description**: Modify initializeWriter() to apply parsed compression, row group size, and dictionary encoding options

**Details**:
- Update `initializeWriter()` method signature to accept options
- Add option parsing calls:
  ```cpp
  auto compression = parseCompressionOption(schema_.options);
  auto row_group_size = parseRowGroupSizeOption(schema_.options);
  auto dict_encoding = parseDictionaryEncodingOption(schema_.options);
  ```
- Update WriterProperties builder:
  ```cpp
  auto props_builder = parquet::WriterProperties::Builder()
      .compression(compression)
      .max_row_group_length(row_group_size);
  
  if (dict_encoding) {
    props_builder.enable_dictionary();
  } else {
    props_builder.disable_dictionary();
  }
  ```
- Pass props to FileWriter::Open()

**Testing**: Verify each option is correctly applied by checking Parquet metadata

---

### [F006-T205] Add Compression Validation and Warnings

**Description**: Validate compression compatibility and warn users about trade-offs

**Details**:
- Add validation logic:
  - Check if compression codec is supported by linked Arrow library
  - Verify row_group_size is within valid range
- Add warnings (LOG(WARNING)):
  - Very small row groups (< 10K) → "May result in poor compression"
  - Very large row groups (> 5M) → "May increase memory usage"
  - No compression → "File size may be significantly larger"
- Error handling:
  - Unsupported codec → Clear error message listing supported codecs

**User Experience**: Help users make informed decisions about compression settings

---

### [F006-T206] Update Documentation with Options Examples

**Description**: Add comprehensive examples of all supported options to documentation

**Details**:
- Edit file: `doc/source/data_io/export_data.md`
- Add Parquet export section with examples:
  ```cypher
  -- Basic export (default SNAPPY)
  COPY (MATCH (n:person) RETURN n.*) TO 'person.parquet';
  
  -- Custom compression
  COPY (MATCH (n:person) RETURN n.*) 
  TO 'person.parquet' 
  (compression='zstd');
  
  -- Small row groups for streaming
  COPY (MATCH (n:person) RETURN n.*) 
  TO 'person.parquet' 
  (row_group_size=65536);
  
  -- Disable dictionary encoding
  COPY (MATCH (n:person) RETURN n.name) 
  TO 'person.parquet' 
  (dictionary_encoding=false);
  
  -- Combined options
  COPY (MATCH (n:person) RETURN n.*) 
  TO 'person.parquet' 
  (compression='zstd', row_group_size=1048576, dictionary_encoding=true);
  ```
- Add compression comparison table (from spec.md)

---

**Module 2 Summary**: 6 tasks (T201-T206)  
**Estimated Effort**: 1-2 days  
**Dependencies**: Module 1 (M001) must be completed first  
**Deliverable**: Configurable compression (none/snappy/zlib/zstd), row group size, and dictionary encoding
