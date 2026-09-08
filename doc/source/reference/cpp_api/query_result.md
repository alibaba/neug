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

### Cursor Traversal

#### `hasNext() const`

Check whether there are more rows to consume.

#### `next()`

Advance the cursor to the next row. Throws if no more rows are available.

#### `Reset()`

Reset the internal cursor back to the first row.

#### `CurrentRowIndex() const`

Return the current cursor position (0-based row index).

### Typed Value Accessors

All getters read from the **current cursor row**. Each method has two overloads:
by column index or by column name. Use `IsNull(...)` before reading a nullable
cell. A getter throws when the cursor or column selector is invalid, or when the
column type cannot be converted to the requested return type.

#### `IsNull(size_t column_index)` / `IsNull(const std::string& column_name)`

Check whether the cell at current row is NULL.

#### `GetInt32(...)`

Return the current cell as a signed 32-bit integer. This accessor accepts
`int32` and `bool` columns; `true` is converted to `1` and `false` to `0`.

#### `GetUInt32(...)`

Return the current cell as an unsigned 32-bit integer. This accessor accepts
`uint32` and `bool` columns; `true` is converted to `1` and `false` to `0`.

#### `GetInt64(...)`

Return the current cell as a signed 64-bit integer. This accessor accepts
`int64`, `int32`, `uint32`, `bool`, `date`, and `timestamp` columns. Smaller
integers are widened, booleans become `1` or `0`, and `date` / `timestamp`
values are returned as the raw epoch value stored by NeuG.

#### `GetUInt64(...)`

Return the current cell as an unsigned 64-bit integer. This accessor accepts
`uint64`, `uint32`, and `bool` columns. A `uint32` value is widened, while
`true` and `false` are converted to `1` and `0` respectively.

#### `GetFloat(...)`

Return the current cell as a single-precision floating-point value. This
accessor accepts `float`, `int32`, `uint32`, and `bool` columns. Integer values
are converted to `float`; booleans become `1.0f` or `0.0f`.

#### `GetDouble(...)`

Return the current cell as a double-precision floating-point value. This
accessor accepts `double`, `float`, `int32`, `uint32`, `int64`, `uint64`, and
`bool` columns. Numeric values are converted to `double`, and booleans become
`1.0` or `0.0`. Large 64-bit integers may lose precision during conversion.

#### `GetString(...)`

Return the current cell as a string. This accessor accepts every column type:
string values are returned directly, while other values use NeuG's
human-readable string representation.

#### `GetBool(...)`

Return the current cell as a Boolean value. This accessor accepts only `bool`
columns; requesting a Boolean from any other column type throws an exception.

> Temporal columns (`date`, `timestamp`, `interval`) are not exposed as
> dedicated typed objects. Use `GetString(...)` for their canonical string form
> (e.g. `"1970-01-01"`), and `GetInt64(...)` to read the raw epoch value of
> `date` / `timestamp` columns.

### Metadata

#### `ColumnCount() const`

Get the number of columns.

#### `ColumnNames() const`

Get column names from schema.

### Other Methods

#### `ToString() const`

Convert entire result set to string.

#### `GetCurrentRowAsString() const`

Convert the **current cursor row** to a human-readable, comma-separated string
(NULL cells render as `null`). Handy for printing rows while iterating with
`hasNext()` / `next()`. Throws if the cursor is past the end of the result set.

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
