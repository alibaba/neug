# QueryResult

**Full name:** `neug::QueryResult`

Lightweight wrapper around protobuf `QueryResponse`.

``QueryResult`` stores a full query response and exposes utility methods for:
- constructing from serialized protobuf bytes (``From()``),
- obtaining row count (``length()``),
- accessing response schema (``result_schema()``),
- serializing/deserializing (``Serialize()`` / ``From()``),
- debugging output (``ToString()``),
- read-only row traversal via C++ range-for (``begin()`/end()`).
Note: traversal currently provides row index + column access to raw protobuf arrays through ``RowView``, rather than materialized typed cell values.

### Public Methods

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

#### `begin() const`

Begin iterator for range-for traversal by row index.

#### `end() const`

End iterator for range-for traversal by row index.

