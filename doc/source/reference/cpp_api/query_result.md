# QueryResult

**Full name:** `neug::QueryResult`

Lightweight wrapper around protobuf `QueryResponse`.

``QueryResult`` stores a full query response and exposes utility methods for:
- constructing from serialized protobuf bytes (``From()``),
- obtaining row count (``length()``),
- accessing response schema (``result_schema()``),
- serializing/deserializing (``Serialize()`` / ``From()``),
- debugging output (``ToString()``),
- cursor-based row traversal via ``hasNext()`` / ``next()``,
- typed cell access via ``GetInt32()``, ``GetString()``, etc.

**Typed accessors:** Every getter reads from the current cursor row and has two overloads, by column index or by column name. Call `IsNull(...)` before reading a cell that may be NULL. Temporal columns (`date`, `timestamp`, `interval`) are not exposed as dedicated typed objects: use `GetString(...)` for their canonical string form (e.g. `"1970-01-01"`) and `GetInt64(...)` for the raw epoch value of `date` / `timestamp` columns.

**Example:** 
```cpp
auto result = QueryResult::From(serialized);
// Access by column index
while (result.hasNext()) {
    if (!result.IsNull(0)) {
        int32_t id = result.GetInt32(0);
        std::string name = result.GetString(1);
    }
    result.next();
}
// Access by column name
result.Reset();
while (result.hasNext()) {
    if (!result.IsNull("id")) {
        int32_t id = result.GetInt32("id");
        std::string name = result.GetString("name");
        double score = result.GetDouble("score");
    }
    result.next();
}
```

### Public Methods

#### `hasNext() const`

Check whether there are more rows to consume.

#### `next()`

Advance the cursor to the next row.

Throws if no more rows are available (check `hasNext()` first).

#### `Reset()`

Reset the internal cursor back to the first row.

#### `CurrentRowIndex() const`

Return the current cursor position (0-based row index).

#### `IsNull(size_t column_index) const`

Check whether the cell at current row is NULL.

- **Parameters:**
  - `column_index`

#### `GetInt32(size_t column_index) const`

Return the current cell as a signed 32-bit integer.

The source column must have type int32 or `bool`. Any other source type causes an exception. An int32 value is returned unchanged; a `bool` value is converted to 1 or 0.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetInt32(const std::string &column_name) const`

Return the named current cell as a signed 32-bit integer.

The source column must have type int32 or `bool`. Any other source type causes an exception. An int32 value is returned unchanged; a `bool` value is converted to 1 or 0.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetUInt32(size_t column_index) const`

Return the current cell as an unsigned 32-bit integer.

The source column must have type uint32 or `bool`. Any other source type causes an exception. A uint32 value is returned unchanged; a `bool` value is converted to 1 or 0.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetUInt32(const std::string &column_name) const`

Return the named current cell as an unsigned 32-bit integer.

The source column must have type uint32 or `bool`. Any other source type causes an exception. A uint32 value is returned unchanged; a `bool` value is converted to 1 or 0.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetInt64(size_t column_index) const`

Return the current cell as a signed 64-bit integer.

The source column must have type int64, int32, uint32, `bool`, date, or timestamp. Any other source type causes an exception. Smaller integers are widened, `bool` is converted to 1 or 0, and date and timestamp values are returned as their raw stored epoch values.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetInt64(const std::string &column_name) const`

Return the named current cell as a signed 64-bit integer.

The source column must have type int64, int32, uint32, `bool`, date, or timestamp. Any other source type causes an exception. Smaller integers are widened, `bool` is converted to 1 or 0, and date and timestamp values are returned as their raw stored epoch values.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetUInt64(size_t column_index) const`

Return the current cell as an unsigned 64-bit integer.

The source column must have type uint64, uint32, or `bool`. Any other source type causes an exception. A uint32 value is widened, and `bool` is converted to 1 or 0.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetUInt64(const std::string &column_name) const`

Return the named current cell as an unsigned 64-bit integer.

The source column must have type uint64, uint32, or `bool`. Any other source type causes an exception. A uint32 value is widened, and `bool` is converted to 1 or 0.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetFloat(size_t column_index) const`

Return the current cell as a single-precision floating-point value.

The source column must have type `float`, int32, uint32, or `bool`. Any other source type causes an exception. Integers are converted to `float`, and `bool` is converted to 1.0 or 0.0.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetFloat(const std::string &column_name) const`

Return the named current cell as a single-precision floating-point value.

The source column must have type `float`, int32, uint32, or `bool`. Any other source type causes an exception. Integers are converted to `float`, and `bool` is converted to 1.0 or 0.0.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetDouble(size_t column_index) const`

Return the current cell as a `double`-precision floating-point value.

The source column must have type `double`, `float`, int32, uint32, int64, uint64, or `bool`. Any other source type causes an exception. Numeric values are converted to `double`, and `bool` is converted to 1.0 or 0.0. Large 64-bit integers may lose precision.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetDouble(const std::string &column_name) const`

Return the named current cell as a `double`-precision floating-point value.

The source column must have type `double`, `float`, int32, uint32, int64, uint64, or `bool`. Any other source type causes an exception. Numeric values are converted to `double`, and `bool` is converted to 1.0 or 0.0. Large 64-bit integers may lose precision.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetString(size_t column_index) const`

Return the current cell as a string.

This getter supports every source column type. String values are returned directly; all other values use their human-readable representation.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetString(const std::string &column_name) const`

Return the named current cell as a string.

This getter supports every source column type. String values are returned directly; all other values use their human-readable representation.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `GetBool(size_t column_index) const`

Return the current cell as a boolean value.

The source column must have type `bool`. Any other source type causes an exception. The `bool` value is returned unchanged.

- **Parameters:**
  - `column_index`: Zero-based column index.

#### `GetBool(const std::string &column_name) const`

Return the named current cell as a boolean value.

The source column must have type `bool`. Any other source type causes an exception. The `bool` value is returned unchanged.

- **Parameters:**
  - `column_name`: Column name from the result schema.

#### `ColumnCount() const`

Get the number of columns.

#### `ColumnNames() const`

Get column names from schema.

#### `ToString() const`

Convert entire result set to string.

#### `GetCurrentRowAsString() const`

Convert the current cursor row to a human-readable string.

Produces a comma-separated list of the row's column values (NULL cells are rendered as "null"). Useful for printing rows while iterating with `hasNext()`/next(). Throws if the cursor is past the end of the result set.

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

#### `has_profile_result() const`

Check if profile result is available.

#### `profile_result_text() const`

Get human-readable PROFILE/EXPLAIN text output.

Returns empty string if no profile_result available. Suitable for CLI output and debugging.

