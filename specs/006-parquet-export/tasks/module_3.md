# Module 3: Complex Data Type Support

**Module ID**: M003  
**Priority**: P3  
**Goal**: Handle complex NeuG data types (nested objects, lists, dates) in Parquet export

**GitHub Settings**:
- **Assignee**: [TBD]
- **Label**: `feature`, `parquet`, `export`, `P3`, `complex-types`
- **Milestone**: [TBD]
- **Project**: External Data Sources

**Independent Test**: Can be fully tested by exporting vertices/edges with complex property types (arrays, dates, nested objects) and verifying roundtrip integrity

---

## Tasks

### [F006-T301] Implement Vertex Object Serialization

**Description**: Export vertex objects with all properties preserved in Parquet format

**Details**:
- When query returns vertex (e.g., `RETURN v`), serialize as struct:
  ```cpp
  arrow::struct_({
    arrow::field("_ID", arrow::int64()),
    arrow::field("_LABEL", arrow::utf8()),
    arrow::field("prop1", arrow_type_1),
    arrow::field("prop2", arrow_type_2),
    // ... all vertex properties
  })
  ```
- Extract properties from vertex object:
  - Get vertex ID, label
  - Iterate over property map
  - Convert each property to Arrow array
- Handle NULL properties gracefully

**Example Output**:
```
Vertex {ID: 0, LABEL: "person", name: "Alice", age: 30}
↓
Struct column:
  _ID: [0]
  _LABEL: ["person"]
  name: ["Alice"]
  age: [30]
```

---

### [F006-T302] Implement Edge Object Serialization

**Description**: Export edge objects including _SRC, _DST, _LABEL metadata

**Details**:
- When query returns edge (e.g., `RETURN e`), serialize as struct:
  ```cpp
  arrow::struct_({
    arrow::field("_SRC", arrow::int64()),
    arrow::field("_DST", arrow::int64()),
    arrow::field("_SRC_LABEL", arrow::utf8()),
    arrow::field("_DST_LABEL", arrow::utf8()),
    arrow::field("_LABEL", arrow::utf8()),
    arrow::field("prop1", arrow_type_1),
    // ... all edge properties
  })
  ```
- Extract edge metadata:
  - Source ID, destination ID
  - Source label, destination label
  - Edge label
  - Edge properties

**Example Output**:
```
Edge {_SRC: 0, _DST: 1, _LABEL: "knows", weight: 0.5}
↓
Struct column:
  _SRC: [0]
  _DST: [1]
  _SRC_LABEL: ["person"]
  _DST_LABEL: ["person"]
  _LABEL: ["knows"]
  weight: [0.5]
```

---

### [F006-T303] Implement List/Array Property Handling

**Description**: Handle array/list properties in vertex/edge attributes

**Details**:
- Detect LIST<T> LogicalType in schema
- Convert to Arrow List type:
  ```cpp
  arrow::list(element_arrow_type)
  ```
- Example: `tags: ["engineer", "manager"]`
  ```cpp
  arrow::list(arrow::utf8())
  ```
- Handle nested lists if present (LIST<LIST<T>>)
- Preserve NULL elements within lists

**Edge Cases**:
- Empty list → Valid empty Arrow list
- List with NULLs → Preserve null bitmap
- Mixed types in list → Error (lists must be homogeneous)

---

### [F006-T304] Implement Date and Timestamp Serialization

**Description**: Properly serialize DATE and TIMESTAMP types to Parquet format

**Details**:
- DATE type:
  - Convert to `arrow::date32()` (days since Unix epoch)
  - Example: "2024-01-15" → 19738 days
- TIMESTAMP type:
  - Convert to `arrow::timestamp(arrow::TimeUnit::MICROSECOND)`
  - Preserve timezone if present
  - Example: "2024-01-15 10:30:00" → microseconds since epoch
- Handle NULL dates/timestamps

**Validation**:
- Export date → Import back → Verify same value
- Export timestamp → Import back → Verify same precision

---

### [F006-T305] Add Complex Type Unit Tests

**Description**: Create unit tests for all complex type serialization

**Details**:
- Create test file: `extension/parquet/tests/test_complex_types.cc` (or add to existing)
- Test cases:
  1. Vertex with all property types
  2. Edge with metadata and properties
  3. List property (strings, integers)
  4. Date property
  5. Timestamp property
  6. Nested struct (if applicable)
  7. NULL values in complex types
- Verify roundtrip: Export → Read back → Compare with original

**Test Pattern**:
```cpp
TEST(ParquetExportTest, ExportVertexWithComplexTypes) {
  // 1. Create vertex with complex properties
  // 2. Export to Parquet
  // 3. Read Parquet with PyArrow
  // 4. Verify all properties match
}
```

---

### [F006-T306] Create Python Integration Tests for Complex Types

**Description**: Add comprehensive Python tests for complex type export

**Details**:
- Add tests to existing file: `tools/python_bind/tests/test_export.py`
- Add new test class:
  ```python
  class TestParquetExport:
      """COPY TO Parquet tests"""
  ```
- Complex type tests:
  ```python
  def test_export_vertex_with_properties(self):
      """Export vertex with multiple property types"""
      self.conn.execute(
          "COPY (MATCH (v:person) RETURN v) TO 'vertex.parquet'"
      )
      table = pq.read_table('vertex.parquet')
      assert table.schema.field('_ID').type == pa.int64()
      assert table.schema.field('name').type == pa.string()
      assert table.schema.field('age').type == pa.int32()
  
  def test_export_edge_with_metadata(self):
      """Export edge with _SRC, _DST, _LABEL"""
      self.conn.execute(
          "COPY (MATCH (v)-[e:knows]->(v2) RETURN e) TO 'edge.parquet'"
      )
      table = pq.read_table('edge.parquet')
      assert '_SRC' in table.schema.names
      assert '_DST' in table.schema.names
      assert '_LABEL' in table.schema.names
  
  def test_export_with_array_property(self):
      """Export vertex with array property"""
      self.conn.execute(
          "COPY (MATCH (v:person) RETURN v.tags) TO 'tags.parquet'"
      )
      table = pq.read_table('tags.parquet')
      assert pa.types.is_list(table.schema.field('v.tags').type)
  ```

---

### [F006-T307] Add Roundtrip Verification Tests

**Description**: Test export → import cycle to ensure data integrity

**Details**:
- Create roundtrip test:
  1. Create test data with all supported types
  2. Export to Parquet: `COPY (query) TO 'test.parquet'`
  3. Import from Parquet: `LOAD FROM 'test.parquet' RETURN *`
  4. Compare original vs imported data
- Test scenarios:
  - All scalar types (INT64, STRING, DOUBLE, etc.)
  - Date and Timestamp
  - List properties
  - Vertex objects
  - Edge objects
  - NULL values
- Validate:
  - Row count matches
  - Column names match
  - Values match (within precision for floats)
  - NULL positions match

---

**Module 3 Summary**: 7 tasks (T301-T307)  
**Estimated Effort**: 2-3 days  
**Dependencies**: Module 1 (M001) must be completed first  
**Deliverable**: Full support for vertex/edge objects, list properties, date/timestamp types with comprehensive tests
