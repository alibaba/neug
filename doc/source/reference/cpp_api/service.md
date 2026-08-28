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
  config.thread_num = 0;  // Auto-select service threads from database max_thread_num.
  config.auto_compaction = true;
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

**Thread Safety:** All public methods are thread-safe. The service uses a `TpExecutionSlotPool` internally to handle concurrent requests efficiently.

**Service Threads:** `ServiceConfig::thread_num` controls the service thread
count. The default `0` auto-selects from the database `max_thread_num`. If set
explicitly, it must be less than or equal to the database `max_thread_num`. With
the default database thread setting, `max_thread_num` is resolved from hardware
concurrency and falls back to `1` if the runtime cannot detect it.
Service threads run TP queries concurrently, but each query uses one execution slot and one thread.

**Auto Compaction:** `ServiceConfig::auto_compaction` controls whether a
background auto-compaction thread runs while serving. Default is `true`.

### Constructors & Destructors

#### `NeugDBService(neug::NeugDB &db, const ServiceConfig &config=ServiceConfig())`

Constructs a service around an existing database instance.

- **Parameters:**
  - `db`: Reference to the NeuG database that will handle queries
  - `config`

- **Notes:**
  - The database should be opened and ready before creating the service
  - Construction closes existing embedded connections; none may be in use.

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

#### `AcquireExecutionSlot()`

Leases an execution slot from the internal TP execution-slot pool.

Returns an `ExecutionSlotLease` that automatically returns the execution slot
to the pool when it goes out of scope. Use this for direct query execution when
you need fine-grained control over the lease lifetime.

**Usage Example:** 
```cpp
neug::NeugDBService service(db, config);
service.Start();
// Lease an execution slot and execute a query.
auto lease = service.AcquireExecutionSlot();
auto result = lease->ExecuteTransactionalRequest(
    R"({"query": "MATCH (n) RETURN count(n)"})");
// The ExecutionSlot is automatically returned when lease leaves scope.
```

- **Notes:**
  - Blocks if no execution slot is available in the pool

- **Returns:** `ExecutionSlotLease` managing the leased execution slot

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

## ExecutionSlot

**Full name:** `neug::ExecutionSlot`

**Header:** `neug/main/execution_slot.h`

Reusable execution context for AP and TP query execution.

`ExecutionSlot` is a runtime-neutral execution context. It owns slot-local
query state and borrows the database snapshot store, version manager,
allocator, and optional WAL writer. Embedded connections own one slot each;
service mode owns a fixed set through `TpExecutionSlotPool`, which also binds
the TP-only WAL writers. The class itself has no brpc or bthread dependency.

An `ExecutionSlot` must not be used concurrently. It is not bound to a physical
pthread or bthread, so a request may keep the same execution slot, allocator,
and WAL writer across cooperative yields.

An `ExecutionSlot` borrows all constructor dependencies. Connections and the
service pool release their slots before `NeugDB` destroys those dependencies.

**Usage Example:** 
```cpp
// Acquire execution slot from service
auto lease = service.AcquireExecutionSlot();
// Execute read query
std::string query = R"({
  "query": "MATCH (n:Person) RETURN n.name LIMIT 10",
  "access_mode": "read"
})";
auto result = lease->ExecuteTransactionalRequest(query);
// Execute write query with parameters
std::string insert_query = R"({
  "query": "CREATE (n:Person {name: $name})",
  "access_mode": "insert",
  "parameters": {"name": "Alice"}
})";
auto write_result = lease->ExecuteTransactionalRequest(insert_query);
```

**Internal Transaction Strategies:**
- ``SnapshotReadTransaction``: Read-only snapshot access
- ``MvccInsertTransaction``: Add new vertices and edges
- ``SnapshotCowWriteTransaction``: Versioned COW updates used by transactional
  execution
- ``CurrentCowWriteTransaction``: Private COW updates of the AP current graph
- ``InPlaceCompactionTransaction``: Background compaction operations

These are `ExecutionSlot` implementation strategies. Connection and Session
continue to expose logical read-only/read-write transaction semantics; clients
do not select these internal types.

**Thread Safety:** An execution slot must not be used concurrently. Sequential
use may resume on a different physical worker because the slot is an execution
context, not thread-local state.

### Public Methods

#### `ExecuteTransactionalRequest(const std::string &query)`

Execute a Cypher query within the execution slot.

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
auto lease = service.AcquireExecutionSlot();
// Simple read query
auto result = lease->ExecuteTransactionalRequest(
    R"({"query": "MATCH (n) RETURN count(n)"})");
if (result.has_value()) {
  // Process result
}
// Parameterized query
std::string query = R"({
  "query": "MATCH (n:Person {age: $age}) RETURN n",
  "access_mode": "read",
  "parameters": {"age": 30}
})";
auto param_result = lease->ExecuteTransactionalRequest(query);
```

- **Parameters:**
  - `query`: JSON string containing query, access_mode, and parameters

- **Returns:** Result containing `QueryResult` on success, or error status


---

## TpExecutionSlotPool

**Full name:** `neug::TpExecutionSlotPool`

Pool of database execution slots for concurrent query execution.

`TpExecutionSlotPool` owns and schedules a fixed set of `ExecutionSlot`
instances for TP queries. Each aligned entry stores its execution slot inline,
keeps its allocator alive, and owns its WAL writer.
`TpExecutionSlotPool` is used internally by `NeugDBService`. For most use cases, access execution slots through `NeugDBService::AcquireExecutionSlot()` rather than directly through the pool.

**Key Features:**
- Service-owned execution slots with explicit lease/release
- Thread-safe slot scheduling with bthread synchronization
- Automatic WAL (Write-Ahead Log) management per slot
- Memory-aligned TP slot contexts for cache efficiency

**Pool Size:** `NeugDBConfig::max_thread_num` determines the pool size. Each query exclusively leases one slot and one thread for its duration.

### Public Methods

#### `AcquireExecutionSlot()`

Lease an execution slot from the pool.

Blocks if no execution slot is available.

- **Returns:** `ExecutionSlotLease` managing the leased slot. The slot is returned to the pool when the lease goes out of scope.

#### `getExecutedQueryNum() const`

Get the total number of executed queries across all execution slots.

Expect lock held by caller.

- **Returns:** Total number of executed queries.


---

## ExecutionSlotLease

**Full name:** `neug::ExecutionSlotLease`

Move-only RAII lease for exclusive use of an execution slot.

`ExecutionSlotLease` automatically returns its execution slot to the
`TpExecutionSlotPool` that issued it. Embedded connections do not lease their
slot.

A lease must not outlive the `NeugDBService` that issued it.

**Usage Example:** 
```cpp
{
  // Lease an execution slot; this blocks if none is available.
  auto lease = service.AcquireExecutionSlot();
  // Use execution slot for queries
  auto result = lease->ExecuteTransactionalRequest(query);
} // ExecutionSlot automatically released here
```

**Thread Safety:** `ExecutionSlotLease` is move-only (non-copyable) to preserve
exclusive slot use. Each lease should be used by a single logical request at a
time.
