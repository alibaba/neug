# QueryResult

**Full name:** `neug::QueryResult`

Container for Cypher query execution results.

`QueryResult` provides convenient access to query results through both iterator-style (hasNext/next) and random access (operator[]) patterns. Results are returned as `RecordLine` objects containing Entry pointers.

**Usage Example:** 
```cpp
auto result = conn->Query("MATCH (n:Person) RETURN n.name, n.age LIMIT 100");
if (result.has_value()) {
  QueryResult& qr = result.value();
  // Method 1: Range-based for loop
  for (auto& record : qr) {
    std::cout << record.ToString() << std::endl;
  }
  // Method 2: Random access by index
  for (size_t i = 0; i < qr.length(); ++i) {
    RecordLine record = qr[i];
  }
  // Get result schema
  std::cout << "Schema: " << qr.get_result_schema() << std::endl;
}
```

**Memory Model:**
- `QueryResult` owns the underlying protobuf data
- `RecordLine` contains pointers to internal data (no copying)
- Do not access records after `QueryResult` is destroyed

### Constructors & Destructors

#### `QueryResult()=default`

Default constructor creating empty result.

- **Since:** v0.1.0

#### `QueryResult(results::CollectiveResults &&res)`

Construct `QueryResult` from CollectiveResults.

- **Parameters:**
  - `res`: CollectiveResults to be moved and stored

- **Since:** v0.1.0

### Public Methods

#### `From(results::CollectiveResults &&result)`

Create `QueryResult` from CollectiveResults (move semantics).

Factory method that creates a `QueryResult` by moving a CollectiveResults.
`QueryResult` constructor with `std::move`.

- **Parameters:**
  - `result`: CollectiveResults to be moved into the `QueryResult`

- **Returns:** `QueryResult` containing the moved results

- **Since:** v0.1.0

#### `From(const std::string &result_str)`

Create `QueryResult` by deserializing from a string.

Deserializes a CollectiveResults protobuf from string format.

- **Parameters:**
  - `result_str`: Serialized CollectiveResults string

- **Throws:**
  - `std::runtime_error`: if parsing fails

- **Returns:** `QueryResult` containing the deserialized results

- **Since:** v0.1.0

#### `hasNext() const`

Check if there are more records to iterate.

- **Returns:** `true` if `cur_index_` < `result_`.results_size(), `false` otherwise

- **Since:** v0.1.0

#### `next()`

Get the next result record and advance iterator.

Returns a `RecordLine` containing pointers to Entry objects from the current record. Advances `cur_index_` after retrieving the record.

- **Notes:**
  - Returns pointers to internal data - no memory allocation or copying
  - Returns empty `RecordLine` if no more records (logs error)
  - Caller should check `hasNext()` before calling this method

- **Returns:** `RecordLine` containing `const` Entry* pointers to record columns

- **Since:** v0.1.0

#### `operator[](int index) const`

Get a record by index (random access).

- **Parameters:**
  - `index`: Zero-based index of the record to retrieve

- **Notes:**
  - Does not affect iterator state (`cur_index_`)
  - Returns pointers to internal data - no copying

- **Returns:** `RecordLine` containing `const` Entry* pointers to record columns

- **Since:** v0.1.0

#### `length() const`

Get total number of records in the result set.

- **Returns:** Total number of result records

- **Since:** v0.1.0

#### `get_result_schema() const`

Get the result schema as a string.

- **Returns:** `const` `std::string`& Reference to the schema string from CollectiveResults

- **Since:** v0.1.0


---

## RecordLine

**Full name:** `neug::RecordLine`

A single row/record from query results.

`RecordLine` represents one row of query output, containing multiple column values (entries). Each entry corresponds to a RETURN clause expression in the Cypher query.

**Usage Example:** 
```cpp
auto result = conn->Query("MATCH (n:Person) RETURN n.name, n.age", "read");
for (auto& record : result.value()) {
  // Access entries by index
  const auto& entries = record.entries();
  // entries[0] = name, entries[1] = age
  std::cout << record.ToString() << std::endl;
}
```

### Constructors & Destructors

#### `RecordLine(std::vector< const results::Entry * > entries)`

Construct `RecordLine` from entry pointers.

- **Parameters:**
  - `entries`: Vector of pointers to result entries

### Public Methods

#### `ToString() const`

Convert record to string representation.

- **Returns:** String representation of all entries

#### `entries() const`

Get all entries (column values) in this record.

- **Returns:** Vector of `const` pointers to Entry objects

