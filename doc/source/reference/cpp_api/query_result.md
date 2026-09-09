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
cell. Choose a getter according to the column's underlying result type; these
methods do not parse or coerce arbitrary source types. A getter throws
`neug::exception::RuntimeError` when the cursor or column selector is invalid,
or when the source column type is not listed for that getter.

#### `IsNull(size_t column_index)` / `IsNull(const std::string& column_name)`

Check whether the cell at current row is NULL.

#### `GetInt32(...)`

Return the current cell as a signed 32-bit integer. Call this method only when
the source column type is `int32` or `bool`; any other source type causes a
`neug::exception::RuntimeError`. An `int32` value is returned unchanged, while
`true` is converted to `1` and `false` to `0`.

#### `GetUInt32(...)`

Return the current cell as an unsigned 32-bit integer. Call this method only
when the source column type is `uint32` or `bool`; any other source type causes
a `neug::exception::RuntimeError`. A `uint32` value is returned unchanged,
while `true` is converted to `1` and `false` to `0`.

#### `GetInt64(...)`

Return the current cell as a signed 64-bit integer. Call this method only when
the source column type is `int64`, `int32`, `uint32`, `bool`, `date`, or
`timestamp`; any other source type causes a `neug::exception::RuntimeError`.
An `int64` value is returned unchanged, smaller integers are widened, booleans
become `1` or `0`, and `date` / `timestamp` values are returned as the raw epoch
value stored by NeuG.

#### `GetUInt64(...)`

Return the current cell as an unsigned 64-bit integer. Call this method only
when the source column type is `uint64`, `uint32`, or `bool`; any other source
type causes a `neug::exception::RuntimeError`. A `uint64` value is returned
unchanged, a `uint32` value is widened, and booleans become `1` or `0`.

#### `GetFloat(...)`

Return the current cell as a single-precision floating-point value. Call this
method only when the source column type is `float`, `int32`, `uint32`, or
`bool`; any other source type causes a `neug::exception::RuntimeError`. A
`float` value is returned unchanged, integer values are converted to `float`,
and booleans become `1.0f` or `0.0f`.

#### `GetDouble(...)`

Return the current cell as a double-precision floating-point value. Call this
method only when the source column type is `double`, `float`, `int32`, `uint32`,
`int64`, `uint64`, or `bool`; any other source type causes a
`neug::exception::RuntimeError`. A `double` value is returned unchanged, other
numeric values are converted to `double`, and booleans become `1.0` or `0.0`.
Large 64-bit integers may lose precision during conversion.

#### `GetString(...)`

Return the current cell as a string. This is the only typed getter that can be
called for every source column type. String values are returned directly;
other values use NeuG's human-readable string representation.

#### `GetBool(...)`

Return the current cell as a Boolean value. Call this method only when the
source column type is `bool`; any other source type causes a
`neug::exception::RuntimeError`. The Boolean value is returned unchanged.

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
