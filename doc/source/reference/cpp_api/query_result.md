# QueryResult

**Full name:** `neug::QueryResult`

Lightweight wrapper around protobuf `QueryResponse`.

``QueryResult`` stores a full query response and exposes utility methods for:
- constructing from serialized protobuf bytes (``From()``),
- obtaining row count (``length()``),
- accessing response schema (``result_schema()``),
- serializing/deserializing (``Serialize()`` / ``From()``),
- debugging output (``ToString()``),
- cursor-based row traversal via ``HasNext()`` / ``Next()``,
- typed cell access via ``GetInt32()``, ``GetString()``, etc.

### Cursor Traversal

#### `HasNext() const`

Check whether there are more rows to consume.

#### `Next()`

Advance the cursor to the next row. Throws if no more rows are available.

#### `Reset()`

Reset the internal cursor back to the first row.

#### `CurrentRowIndex() const`

Return the current cursor position (0-based row index).

### Typed Value Accessors

All getters read from the **current cursor row** by column index.

#### `IsNull(size_t column_index) const`

Check whether the cell at current row is NULL.

#### `GetInt32(size_t column_index) const`

#### `GetUInt32(size_t column_index) const`

#### `GetInt64(size_t column_index) const`

#### `GetUInt64(size_t column_index) const`

#### `GetFloat(size_t column_index) const`

#### `GetDouble(size_t column_index) const`

#### `GetString(size_t column_index) const`

#### `GetBool(size_t column_index) const`

### Metadata

#### `ColumnCount() const`

Get the number of columns.

#### `ColumnNames() const`

Get column names from schema.

### Other Methods

#### `ToString() const`

Convert entire result set to string.

#### `length() const`

Get total number of rows.

#### `result_schema() const`

Get result schema metadata.

#### `response() const`

Get underlying protobuf response (`const` reference).

#### `shared_response() const`

Get shared ownership of the underlying protobuf response.

Useful when callers need to extend the lifetime of the response beyond the `QueryResult` (e.g. zero-copy Arrow export).

#### `Serialize() const`

Serialize entire result set to string.

### Example

```cpp
auto result = QueryResult::From(serialized);

while (result.HasNext()) {
    if (!result.IsNull(0)) {
        int32_t id = result.GetInt32(0);
        std::string name = result.GetString(1);
    }
    result.Next();
}
```

