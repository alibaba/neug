# Connection

**Full name:** `neug::Connection`

Database connection for executing Cypher queries.

`Connection` is the primary interface for interacting with a NeuG database. It provides methods to execute Cypher queries, retrieve schema information, and manage the connection lifecycle.

**Usage Example:**
```cpp
// Get connection from database
auto conn = db.Connect();
// Execute a read query
auto result = conn->Query("MATCH (n:Person) RETURN n.name LIMIT 10", "read");
auto& qr = result.value();
while (qr.hasNext()) {
  std::cout << qr.GetCurrentRowAsString() << std::endl;
  qr.next();
}
// Execute an insert query
conn->Query("CREATE (p:Person {name: 'Alice', age: 30})", "insert");
// Close connection when done
conn->Close();
```

**Access Modes:**
- `"read"` or `"r"`: Read-only queries (MATCH, RETURN)
- `"insert"` or `"i"`: Insert-only operations (CREATE)
- `"update"` or `"u"`: Update/delete operations (SET, DELETE, MERGE)
- `"schema"` or `"s"`: `Schema` modification operations (CREATE/DROP labels)

**Thread Safety:** This class is NOT thread-safe. Do not call `Query()`,
`GetSchema()`, or `Close()` concurrently on the same connection. Use a separate
connection per thread.

**Lifecycle:**
- Created via `NeugDB::Connect()`
- Execute queries via `Query()` method
- Close via `Close()`, which automatically unregisters the connection
- Automatically closed and unregistered in the destructor

### Public Methods

#### `Query(...)`

```cpp
Query(
    const std::string &query_string,
    const std::string &access_mode="",
    const execution::ParamsMap &parameters={}
)
```

Execute a Cypher query and return results.

Compiles and executes a Cypher query string against the database. The query is processed through the planner for optimization, then executed by the query processor.

**Usage Example:**
```cpp
// Simple read query
auto result = conn->Query("MATCH (n:Person) RETURN n.name", "read");
// Query with parameters
neug::execution::ParamsMap params;
params["min_age"] = neug::Value(18);
result = conn->Query("MATCH (p:Person) WHERE p.age > $min_age RETURN p",
"read", params);
// Process results
if (result.has_value()) {
  auto& qr = result.value();
  while (qr.hasNext()) {
    // Access columns via qr.GetString(0), qr.GetInt32(1), etc.
    qr.next();
  }
} else {
  std::cerr << "Query failed: " << result.error().message() << std::endl;
}
```

- **Parameters:**
  - `query_string`: The Cypher query to execute
  - `access_mode`: Query access mode:

- `"read"` or `"r"`: Read-only operations
- `"insert"` or `"i"`: Insert-only operations (CREATE)
- `"update"` or `"u"`: Update/delete operations
- `"schema"` or `"s"`: `Schema` modification operations
- Empty string: Infer the access mode from the query text
  - `parameters`: Named parameters for parameterized queries. Keys are parameter names (without `$`), values are parameter values.

- **Notes:**
  - Use parameterized queries for dynamic values to prevent injection.
  - Specifying correct access_mode ensures proper transaction handling.

- **Returns:** `result<QueryResult>` containing either:

- `QueryResult` with query results on success
- Error status with message on failure

- **Since:** v0.1.0

#### `Query(...)`

```cpp
Query(
    const std::string &query_string,
    const std::string &access_mode,
    const rapidjson::Value &parameters_json
)
```

Execute a Cypher query with JSON parameters.

The parameter values are provided as a JSON object.

- **Parameters:**
  - `query_string`
  - `access_mode`
  - `parameters_json`

#### `BeginTransaction(...)`

```cpp
Status BeginTransaction(
    TransactionMode mode = TransactionMode::kReadWrite
)
```

Begin an explicit embedded transaction owned by this connection. A read-only
transaction pins one published view. A read-write transaction uses a private
copy-on-write view and publishes all successful writes together on `Commit()`.
Nested transactions and read-to-write upgrades are not supported.

- **Parameters:**
  - `mode`: `TransactionMode::kReadWrite` or `TransactionMode::kReadOnly`
- **Returns:** `Status::OK` on success, otherwise a connection, state, argument,
  or unsupported-mode error

#### `Commit()`

```cpp
Status Commit()
```

Commit the active explicit transaction. A failed commit leaves the connection
rollback-only; call `Rollback()` before reusing it.

#### `Rollback()`

```cpp
Status Rollback()
```

Discard the active or rollback-only transaction and return the connection to
auto-commit mode.

#### `HasActiveTransaction() const`

```cpp
bool HasActiveTransaction() const noexcept
```

Return whether this connection has an active or rollback-only explicit
transaction.

#### `GetSchema() const`

Get the database schema as a `YAML` string.

Returns the complete graph schema definition in `YAML` format, including all vertex types, edge types, and their properties.

**Usage Example:**
```cpp
std::string schema_yaml = conn->GetSchema();
std::cout << "Schema:\n" << schema_yaml << std::endl;
```

- **Throws:**
  - `std::runtime_error`: if the connection is closed

- **Returns:** `std::string` YAML-formatted schema definition

- **Since:** v0.1.0

#### `Close()`

Close the connection and release resources.

Marks the connection as closed and releases any held resources. After closing, any `Query()` calls will fail.

**Usage Example:**
```cpp
conn->Close();
// conn->Query(...) will now return an error
```

- **Notes:**
  - Sequential repeated calls are idempotent; concurrent calls are not safe.
  - Closing automatically unregisters this connection from its database.
  - The connection is also automatically closed in the destructor.

- **Since:** v0.1.0

#### `IsClosed() const`

Check if the connection is closed.

- **Returns:** `true` if the connection has been closed, `false` if still active

- **Since:** v0.1.0
