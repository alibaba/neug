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

All getters read from the **current cursor row**. Each method has two overloads: by column index or by column name.

#### `IsNull(size_t column_index)` / `IsNull(const std::string& column_name)`

Check whether the cell at current row is NULL.

#### `GetInt32(...)` — accepts `int32`, `bool`

#### `GetUInt32(...)` — accepts `uint32`, `bool`

#### `GetInt64(...)` — accepts `int64`, `int32`, `uint32`, `bool`, `date`, `timestamp` (date/timestamp return the raw int64 epoch value)

#### `GetUInt64(...)` — accepts `uint64`, `uint32`, `bool`

#### `GetFloat(...)` — accepts `float`, `int32`, `uint32`, `bool`

#### `GetDouble(...)` — accepts `double`, `float`, `int32`, `uint32`, `int64`, `uint64`, `bool`

#### `GetString(...)` — accepts **any type** (falls back to string representation)

#### `GetBool(...)` — accepts `bool` only

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

