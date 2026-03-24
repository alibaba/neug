# NeugDBService

**Full name:** `neug::NeugDBService`

NeuG database HTTP service for high-throughput scenarios.

`NeugDBService` provides an HTTP interface layer for the NeuG graph database, enabling remote query execution over HTTP. It manages the lifecycle of a BRPC-based HTTP server that handles Cypher queries, service status requests, and schema queries through RESTful endpoints.
This is the C++ equivalent of Python's `Database.serve()` functionality, designed for high-throughput Transaction Processing (TP) scenarios where multiple clients need concurrent access to the database.

**Usage Example:** 
```cpp
#include <neug/main/neug_db.h>
#include <neug/server/neug_db_service.h>
int main() {
  // 1. Open the database
  neug::NeugDB db;
  db.Open("/path/to/graph", 8);  // 8 threads
  // 2. Create and configure service
  neug::ServiceConfig config;
  config.query_port = 10000;
  config.host_str = "0.0.0.0";
  // 3. Start HTTP service
  neug::NeugDBService service(db, config);
  std::string url = service.Start();
  std::cout << "Service running at: " << url << std::endl;
  // 4. Block until shutdown signal (Ctrl+C)
  service.run_and_wait_for_exit();
  // 5. Cleanup
  db.Close();
  return 0;
}
```

**HTTP Endpoints:**
- `POST /cypher` - Execute Cypher queries
- `GET /schema` - Retrieve graph schema
- `GET /status` - Check service status

**Thread Safety:** All public methods are thread-safe. The service uses a `SessionPool` internally to handle concurrent requests efficiently.

### Constructors & Destructors

#### `NeugDBService(neug::NeugDB &db, const ServiceConfig &config=ServiceConfig())`

Constructs a service around an existing database instance.

- **Parameters:**
  - `db`: Reference to the NeuG database that will handle queries
  - `config`

- **Notes:**
  - The database should be opened and ready before creating the service

#### `~NeugDBService()`

Destructor that ensures proper cleanup.

Automatically stops the HTTP handler manager if it's running and releases all associated resources.

### Public Methods

#### `db()`

Gets direct access to the underlying graph database.

Direct database access bypasses the service layer

- **Returns:** Reference to the wrapped `NeugDB` instance

#### `Start()`

Starts the HTTP server.

Binds to the configured host and port and begins accepting HTTP requests. Returns the full URL where the service is accessible.

- **Throws:**
  - `std::runtime_error`: If service is not initialized
  - `std::runtime_error`: If service is already running
  - `std::runtime_error`: If unable to bind to configured address

- **Returns:** URL string in format "http://host:port" where service is running

#### `Stop()`

Stops the HTTP server gracefully.

Stops accepting new connections and shuts down the BRPC server. This method is thread-safe and can be called from signal handlers.

- **Notes:**
  - Prints status messages to stderr if service is not properly initialized
  - Protected by mutex to ensure thread-safe shutdown

#### `GetServiceConfig() const`

Retrieves the current service configuration.

- **Notes:**
  - Returns the configuration passed to init(), not runtime settings

- **Returns:** Const reference to the `ServiceConfig` used during initialization

#### `AcquireSession()`

Acquires a session from the internal session pool.

Returns a `SessionGuard` that automatically releases the session back to the pool when it goes out of scope. Use this for direct query execution when you need fine-grained control over session lifecycle.

**Usage Example:** 
```cpp
neug::NeugDBService service(db, config);
service.Start();
// Acquire session and execute query
auto guard = service.AcquireSession();
auto result = guard->Eval(R"({"query": "MATCH (n) RETURN count(n)"})");
// Session automatically released when guard goes out of scope
```

- **Notes:**
  - Blocks if no session is available in the pool

- **Returns:** `SessionGuard` managing the acquired session

#### `IsInitialized() const`

Checks if the service has been initialized.

- **Notes:**
  - This is a thread-safe atomic operation

- **Returns:** `true` if init() was successfully called, `false` otherwise

#### `IsRunning() const`

Checks if the HTTP server is currently running.

- **Notes:**
  - This delegates to the HTTP handler manager's `IsRunning()` method
  - Thread-safe query of server state

- **Returns:** `true` if the underlying BRPC server is accepting connections

#### `service_status()`

Gets current service status information.

Returns status messages indicating the current state:
- "NeugDB service has not been inited!" if not initialized
- "NeugDB service has not been started!" if initialized but not running
- "NeugDB service is running ..." if actively serving requests

- **Notes:**
  - Always returns OK status, actual state is in the message string

- **Returns:** Result containing status message with OK status code

#### `run_and_wait_for_exit()`

Starts service and blocks until shutdown signal.

Convenience method that starts the HTTP server and blocks the calling thread until the server is asked to quit (via `Stop()` or signal). Uses the underlying BRPC server's RunUntilAskedToQuit() mechanism.

- **Notes:**
  - This is the typical way to run the service in production

- **Throws:**
  - `std::runtime_error`: If service is not initialized
  - `std::runtime_error`: If service is already running
  - `std::runtime_error`: If HTTP handler manager is not available


---

## NeugDBSession

**Full name:** `neug::NeugDBSession`

Database session for executing queries in high-throughput scenarios.

`NeugDBSession` provides a session-based interface for interacting with the NeuG database. Each session maintains its own transaction context and application state, enabling concurrent access while ensuring data consistency.
Sessions are typically acquired from a `SessionPool` via `NeugDBService`, not created directly. This is the server-side equivalent of Python's Session class for client connections.

**Usage Example:** 
```cpp
// Acquire session from service
auto guard = service.AcquireSession();
// Execute read query
std::string query = R"({
  "query": "MATCH (n:Person) RETURN n.name LIMIT 10",
  "access_mode": "read"
})";
auto result = guard->Eval(query);
// Execute write query with parameters
std::string insert_query = R"({
  "query": "CREATE (n:Person {name: $name})",
  "access_mode": "insert",
  "parameters": {"name": "Alice"}
})";
auto write_result = guard->Eval(insert_query);
```

**Transaction Types:**
- ``ReadTransaction``: Read-only snapshot access
- ``InsertTransaction``: Add new vertices and edges
- ``UpdateTransaction``: Modify existing graph elements
- ``CompactTransaction``: Background compaction operations

**Thread Safety:** Each session is tied to a specific thread and should not be shared across threads. Sessions are managed by `SessionPool` to ensure thread-local access.

### Public Methods

#### `Eval(const std::string &query)`

Execute a Cypher query within the session.

Executes a query specified as a JSON string containing the Cypher query, access mode, and optional parameters. This is the primary method for query execution in high-throughput service scenarios.

**JSON Format:** 
```cpp
{
  "query": "MATCH (n:Person) RETURN n.name",
  "access_mode": "read",
  "parameters": {
    "param1": "value1",
    "list_param": [1, 2, 3],
    "map_param": {"key": "value"}
  }
}
```

**Access Modes:**
- `"read"` or `"r"`: Read-only query (MATCH without mutations)
- `"insert"` or `"i"`: Insert-only operations (CREATE)
- `"update"` or `"u"`: Update/delete operations (SET, DELETE, MERGE)
- `"schema"` or `"s"`: `Schema` modification operations (CREATE/DROP labels)

**Usage Example:** 
```cpp
auto guard = service.AcquireSession();
// Simple read query
auto result = guard->Eval(R"({"query": "MATCH (n) RETURN count(n)"})");
if (result.has_value()) {
  // Process result
}
// Parameterized query
std::string query = R"({
  "query": "MATCH (n:Person {age: $age}) RETURN n",
  "access_mode": "read",
  "parameters": {"age": 30}
})";
auto param_result = guard->Eval(query);
```

- **Parameters:**
  - `query`: JSON string containing query, access_mode, and parameters

- **Returns:** Result containing `QueryResult` on success, or error status


---

## SessionPool

**Full name:** `neug::SessionPool`

Pool of database sessions for concurrent query execution.

`SessionPool` manages a fixed-size pool of `NeugDBSession` instances, providing efficient session reuse for high-throughput scenarios. Sessions are pre-allocated during pool construction and recycled through acquire/release operations.
`SessionPool` is used internally by `NeugDBService`. For most use cases, access sessions through `NeugDBService::AcquireSession()` rather than directly through the pool.

**Key Features:**
- Pre-allocated sessions for zero-allocation query execution
- Thread-safe acquire/release with bthread synchronization
- Automatic WAL (Write-Ahead Log) management per session
- Memory-aligned session contexts for cache efficiency

**Pool Size:** Determined by `NeugDBConfig::thread_num`, typically matching the number of concurrent request handlers.

### Public Methods

#### `AcquireSession()`

Acquire a session from the pool.

Blocks if no session is available.

- **Returns:** `SessionGuard` managing the acquired session. The session is released back to the pool when the guard goes out of scope.

#### `getExecutedQueryNum() const`

Get the total number of executed queries across all sessions.

Expect lock held by caller.

- **Returns:** Total number of executed queries.


---

## SessionGuard

**Full name:** `neug::SessionGuard`

RAII guard for session lifecycle management.

`SessionGuard` provides automatic session release through RAII pattern. When the guard goes out of scope, the session is automatically returned to the `SessionPool` for reuse.

**Usage Example:** 
```cpp
{
  // Acquire session - blocks if none available
  auto guard = service.AcquireSession();
  // Use session for queries
  auto result = guard->Eval(query);
} // Session automatically released here
```

**Thread Safety:** `SessionGuard` is move-only (non-copyable) to ensure exclusive session ownership. Each guard should be used by a single thread.

