# NeugDB

**Full name:** `neug::NeugDB`

Core database engine for NeuG graph database system.

`NeugDB` serves as the **primary entry point** for all NeuG graph database operations. It provides a complete lifecycle management API including database initialization, query execution, and graceful shutdown.

**Usage Example:** 
```cpp
// Create and open database
neug::NeugDB db;
db.Open("/path/to/data", 4);  // 4 threads
// Create connection and execute query
auto conn = db.Connect();
auto result = conn->Query("MATCH (n:Person) RETURN n LIMIT 10");
// Process results
auto& qr = result.value();
while (qr.hasNext()) {
  std::cout << qr.GetCurrentRowAsString() << std::endl;
  qr.next();
}
// Close database (persists data)
db.Close();
```

**Key Components:**
- `PropertyGraph`: Underlying graph data storage engine
- `ExecutionSlot`: Cypher query compilation and execution
- `ConnectionManager`: Client connection pool management
- `IGraphPlanner`: Query optimization (GOPT or Greedy planner)

**Database Modes:**
- `DBMode::READ_ONLY`: Read-only access for analytics workloads
- `DBMode::READ_WRITE`: Full transactional read/write access

**Thread Safety:** `Connection` creation and registration are synchronized, and separate connections can execute queries concurrently. Individual `Connection` instances are not thread-safe.

**Resource Management:**
- File locking serializes write access across processes: a database opened in read-write mode is exclusive, while multiple read-only processes (or multiple read-only instances within one process) can share the same database directory concurrently
- Automatic WAL (Write-Ahead Log) for crash recovery
- Configurable checkpoint on close

### Public Methods

#### `Open(...)`

```cpp
Open(
    const std::string &data_dir,
    int32_t max_thread_num=0,
    const DBMode mode=DBMode::READ_WRITE,
    const std::string &planner_kind="gopt",
    bool checkpoint_on_close=true
)
```

Open the database from persistent storage.

Initializes and opens the NeuG database from the specified data directory. This method loads the graph schema, vertex/edge data, and initializes the query processor and planner.

**Data Directory Structure:** Checkpointed data is organized as:
- `checkpoint/CURRENT`: atomically published manifest id
- `checkpoint/manifests/`: immutable manifest files
- `checkpoint/objects/`: immutable module objects
- `wal/<id>/`: WAL epoch for each manifest
- `runtime/open-<epoch>/`: mutable allocator workspace for an open process

**Usage Example:** 
```cpp
neug::NeugDB db;
// Simple open with defaults
db.Open("/path/to/graph");
// Open with custom settings (8 threads, read-write mode, GOPT planner)
db.Open("/path/to/graph", 8, neug::DBMode::READ_WRITE, "gopt");
```

- **Parameters:**
  - `data_dir`: `Path` to the graph data directory
  - `max_thread_num`: Database query capacity. 0 selects hardware concurrency (fallback 1); a positive value is honored as-is and a negative value is rejected. AP queries are single-threaded; intra-query parallelism is future work. In TP mode, it sizes the slot pool and caps service threads. Concurrent TP queries each use one slot and one thread.
  - `mode`: Database access mode (READ_ONLY or READ_WRITE)
  - `planner_kind`: Query planner type: "gopt" (Graph Optimizer) or "greedy"
  - `checkpoint_on_close`: Create checkpoint (persist data) when closing

- **Notes:**
  - This overload is primarily designed for Python bindings.
  - For C++ usage, prefer the config-based Open(NeugDBConfig&) overload.

- **Returns:** `true` if database opened successfully, `false` otherwise

- **Since:** v0.1.0

#### `Open(const NeugDBConfig &config)`

Open the database with a configuration object.

Opens the database using a NeugDBConfig structure that provides comprehensive configuration options.

**Usage Example:** 
```cpp
neug::NeugDBConfig config;
config.data_dir = "/path/to/graph";
config.max_thread_num = 8;
config.mode = neug::DBMode::READ_WRITE;
config.memory_level = 1;  // Use memory-mapped virtual memory
neug::NeugDB db;
db.Open(config);
```

- **Parameters:**
  - `config`: Configuration object with all database settings

- **Returns:** `true` if database opened successfully, `false` otherwise

- **Since:** v0.1.0

#### `Close()`

Close the database and release all resources.

Performs a graceful shutdown of the database. Depending on configuration:
- Creates a checkpoint if checkpoint_on_close is enabled
- Closes all open connections
- Releases file locks

**Important:** Always call `Close()` before destroying the `NeugDB` instance to ensure data integrity and proper resource cleanup.

**Usage Example:** 
```cpp
neug::NeugDB db;
db.Open("/path/to/data");
// ... perform operations ...
db.Close();  // Persist data and cleanup
```
The caller must ensure no `Connection` operation is in progress.

- **Notes:**
  - This method is idempotent after a successful close. If the optional shutdown checkpoint fails before consuming the live graph, `Close()` throws and leaves the database open so the caller can correct the failure and retry. A failure after consumption finishes teardown and is then rethrown; that instance cannot be reused.
  - After closing, the database cannot be reopened. Create a new `NeugDB` instance to open the database again.

- **Since:** v0.1.0

#### `IsClosed() const`

Check if the database is closed.

- **Returns:** `true` if the database is closed.

#### `HasActiveService() const`

Check if a `NeugDBService` is currently associated with this database.

At most one `NeugDBService` can be associated with a `NeugDB` instance at any given time. While a service is associated, local connections via `Connect()` are rejected and `Close()` fails.

- **Returns:** `true` if a `NeugDBService` is associated with this database.

#### `HasOpenConnections() const`

Check whether the database has an open local AP connection.

Used to prevent an AP-to-TP transition from invalidating a connection still held by a caller.

#### `Connect()`

Create a new connection to the database for query execution.

Creates and returns a `Connection` object that can be used to execute Cypher queries against the database. The connection shares the query planner and global cache with other connections from the same database, while exclusively owning its `ExecutionSlot`.

**Usage Example:** 
```cpp
auto conn = db.Connect();
auto result = conn->Query("MATCH (n) RETURN count(n)");
if (result.has_value()) {
    std::cout << "Query succeeded" << std::endl;
}
conn->Close();  // Optional: auto-closed on destruction
```

- **Notes:**
  - In READ_ONLY mode, multiple connections can be created.
  - In READ_WRITE mode, only one write connection is allowed.
  - Calling `Connection::Close` automatically unregisters the connection.
  - Connections share the planner instance for efficiency.
  - Each `Connection` must be used by only one thread at a time.

- **Throws:**
  - `std::runtime_error`: if database is not open or closed

- **Returns:** `std::shared_ptr`<Connection> A shared pointer to the new `Connection`

- **Since:** v0.1.0

#### `PrepareForServing()`

Prepare an opened database for TP service without rebuilding planner.

This requires local AP connections to be closed, persists and refreshes the live graph when needed, then rebuilds query runtime handles against the refreshed graph. The planner and its metadata registry are intentionally preserved so runtime extension registrations loaded in AP mode stay available in TP mode. The version manager is replaced only when a new durable checkpoint starts a fresh WAL timeline; otherwise the existing timeline is preserved.
New connections receive slots borrowing the refreshed resources.
The caller must close all local `Connection` objects first.

