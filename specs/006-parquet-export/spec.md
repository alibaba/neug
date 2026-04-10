# Feature Specification: Parquet Export Support

**Feature Branch**: `006-parquet-export`  
**Created**: 2024-XX-XX  
**Status**: Draft  
**Input**: User description: Add Parquet file export support to NeuG's COPY TO command

## User Scenarios & Testing *(mandatory)*

### Primary User: Data Engineer / Analyst

**Scenario 1: Export query results for external analytics**
- As a data engineer, I want to export Cypher query results to Parquet files so that I can analyze them using Spark, DuckDB, or other analytics tools
- **Test**: Execute `COPY (MATCH (n:person) RETURN n.*) TO 'output.parquet'` and verify the file can be read by external tools

**Scenario 2: Export with compression optimization**
- As a data engineer, I want to configure compression settings to balance file size and write performance
- **Test**: Export with different compression codecs (snappy, gzip, zstd) and verify file size differences

**Scenario 3: Export large datasets without memory issues**
- As a data engineer, I want to export millions of rows without running out of memory
- **Test**: Export a large dataset (1M+ rows) and verify no OOM errors occur

**Scenario 4: Export complex graph data**
- As a graph analyst, I want to export vertices and edges with all their properties preserved
- **Test**: Export node and edge data, then re-import and verify data integrity

## Functional Modules *(mandatory)*

### Module 1: Core Parquet Export (Priority: P1)

**Purpose**: Enable basic export of Cypher query results to Parquet file format with default settings

**Why this priority**: This is the minimum viable feature - users can export data immediately without any configuration

**Independent Test**: Can be fully tested by executing a simple COPY TO command and verifying the output Parquet file is readable

**Key Components**:

1. **Parquet Export Function**: Registers as COPY_PARQUET handler in the extension system, receives query results and writes to Parquet format
2. **Type Mapping**: Converts NeuG data types (INT64, STRING, DOUBLE, etc.) to corresponding Parquet/Arrow types
3. **File Output Manager**: Handles file creation, writing, and proper cleanup on completion or error

**Functional Requirements**:

1. **FR-001**: System MUST accept Cypher query wrapped in COPY (...) TO 'path.parquet' syntax
2. **FR-002**: System MUST create a valid Parquet file that can be read by Apache Arrow, Spark, DuckDB, and Presto
3. **FR-003**: System MUST preserve all column names and data types from the query result
4. **FR-005**: System MUST write Arrow schema metadata to enable easier reads back into Arrow

**Known Limitations**:
- **LM-001**: Current implementation loads entire query result into memory before writing (limited by `sink_results` design)
- **LM-002**: Streaming write support will be added in future iteration to handle very large datasets (100M+ rows) without OOM

**Future Improvements**:
- **FI-001**: Implement streaming write mode with `beginWrite()` / `writeTable()` / `endWrite()` lifecycle to support batch processing
- **FI-002**: Integrate with modified `sink_results` that yields results in batches instead of all-at-once

**Acceptance Scenarios**:

1. **Given** a NeuG database with person vertices, **When** user executes `COPY (MATCH (n:person) RETURN n.ID, n.name) TO 'person.parquet'`, **Then** a valid Parquet file is created with 2 columns and correct row count
2. **Given** an empty query result, **When** user executes COPY TO command, **Then** an empty Parquet file with correct schema is created
3. **Given** a query returning vertex objects, **When** user exports to Parquet, **Then** the vertex properties are serialized correctly

**Test Strategy**:

- **Unit Tests**: Test type conversion (NeuG types → Arrow types), batch writing, file creation
- **Integration Tests**: Export from various query types (vertex, edge, path, aggregation) and verify roundtrip with LOAD FROM

---

### Module 2: Compression & Performance Options (Priority: P2)

**Purpose**: Allow users to configure compression codec and row group size for optimized storage and performance

**Why this priority**: Compression is essential for production use cases where storage costs and I/O performance matter

**Independent Test**: Can be fully tested by exporting the same data with different compression settings and comparing file sizes

**Key Components**:

1. **Compression Configurator**: Parses and applies compression codec options (none, snappy, zlib, zstd)
2. **Row Group Manager**: Configures row group size to balance memory usage and query performance
3. **Dictionary Encoder**: Enables/disables dictionary encoding for string columns to optimize storage

**Functional Requirements**:

1. **FR-006**: System MUST support compression options: none, snappy, zlib, zstd
2. **FR-007**: System MUST default to SNAPPY compression when no compression option is specified
3. **FR-008**: System MUST allow configuration of row group size (number of rows per row group)
4. **FR-009**: System MUST enable dictionary encoding by default for string columns
5. **FR-010**: System MUST allow users to disable dictionary encoding via options

**Acceptance Scenarios**:

1. **Given** a query returning 1M rows, **When** user exports with `compression: 'zlib'`, **Then** file size is at least 40% smaller than uncompressed
2. **Given** export with default settings, **When** checking Parquet metadata, **Then** compression codec is SNAPPY
3. **Given** export with `dictionary_encoding: false`, **When** checking Parquet metadata, **Then** dictionary encoding is disabled for string columns

**Test Strategy**:

- **Unit Tests**: Test each compression codec, verify file size differences
- **Integration Tests**: Export large dataset with different settings and measure write performance

---

### Module 3: Complex Data Type Support (Priority: P3)

**Purpose**: Handle complex NeuG data types (nested objects, lists, dates) in Parquet export

**Why this priority**: Ensures all graph data can be exported without data loss, but basic scalar types cover most use cases

**Independent Test**: Can be fully tested by exporting vertices/edges with complex property types and verifying roundtrip

**Key Components**:

1. **Nested Object Serializer**: Converts vertex/edge objects to structured Parquet format
2. **List Array Handler**: Handles array/list properties in graph data
3. **DateTime Formatter**: Properly serializes date and timestamp types to Parquet format

**Functional Requirements**:

1. **FR-011**: System MUST export vertex objects with all properties preserved
2. **FR-012**: System MUST export edge objects including _SRC, _DST, _LABEL metadata
3. **FR-013**: System MUST handle list/array properties in vertex/edge attributes
4. **FR-014**: System MUST properly serialize DATE and TIMESTAMP types

**Acceptance Scenarios**:

1. **Given** a vertex with properties {name: "Alice", tags: ["engineer", "manager"]}, **When** exported to Parquet, **Then** tags column contains array values
2. **Given** an edge with timestamp property, **When** exported to Parquet, **Then** timestamp is preserved with correct precision

**Test Strategy**:

- **Unit Tests**: Test serialization of each complex type
- **Integration Tests**: Export comprehensive graph dataset and verify all types preserved

---

### Edge Cases

- What happens when the output file already exists? **System SHOULD overwrite the file by default**
- How does system handle export to non-existent directory? **System SHOULD create parent directories automatically**
- What if query returns 0 rows? **System SHOULD create empty Parquet file with correct schema**
- What if disk space runs out during export? **System SHOULD fail gracefully with clear error message and cleanup partial file**
- What if query contains unsupported data type? **System SHOULD fail with descriptive error indicating which column and type is unsupported**
- How does system handle very large exports (100M+ rows)? **System SHOULD use streaming writes (to be implemented in future iteration); current implementation loads all results into memory**

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can export query results to Parquet file that is readable by at least 3 external tools (Spark, DuckDB, Presto)
- **SC-002**: Export performance achieves at least 100 MB/s write throughput on standard hardware
- **SC-003**: System can export datasets up to 10M rows without memory errors (streaming writes to be implemented in future iteration)
- **SC-004**: Compressed file size with SNAPPY is at least 2x smaller than uncompressed CSV export
- **SC-005**: All NeuG scalar types (INT64, INT32, DOUBLE, STRING, BOOLEAN, DATE, TIMESTAMP) are correctly preserved in roundtrip test (export → import)
- **SC-006**: Export command completes with clear error message within 5 seconds for invalid inputs (bad path, unsupported type, etc.)
- **SC-007**: Test coverage for Parquet export module exceeds 80%
