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
for (auto& record : result.value()) {
  std::cout << record.ToString() << std::endl;
}
// Close database (persists data)
db.Close();
```

**Key Components:**
- `PropertyGraph`: Underlying graph data storage engine
- `QueryProcessor`: Cypher query compilation and execution
- `ConnectionManager`: Client connection pool management
- `IGraphPlanner`: Query optimization (GOPT or Greedy planner)

**Database Modes:**
- `DBMode::READ_ONLY`: Read-only access for analytics workloads
- `DBMode::READ_WRITE`: Full transactional read/write access

**Thread Safety:** This class is thread-safe. Multiple connections can execute queries concurrently. The `ConnectionManager` handles thread synchronization internally.

**Resource Management:**
- File locking prevents concurrent database access from multiple processes
- Automatic WAL (Write-Ahead Log) for crash recovery
- Configurable checkpoint and compaction on close

### Public Methods

#### `Open(...)`

```cpp
Open(
    const std::string &data_dir,
    int32_t max_num_threads=0,
    const DBMode mode=DBMode::READ_WRITE,
    const std::string &planner_kind="gopt",
    bool enable_auto_compaction=false,
    bool compact_csr=true,
    bool compact_on_close=true,
    bool checkpoint_on_close=true
)
```

Open the database from persistent storage.

Initializes and opens the NeuG database from the specified data directory. This method loads the graph schema, vertex/edge data, and initializes the query processor and planner.

**Data Directory Structure:** The data_dir should contain:
- `graph.yaml`: `Schema` definition file
- `snapshot/`: Vertex and edge data files
- `wal/`: Write-ahead log files (optional, for recovery)

**Usage Example:** 
```cpp
neug::NeugDB db;
// Simple open with defaults
db.Open("/path/to/graph");
// Open with custom settings (8 threads, read-write mode, GOPT planner)
db.Open("/path/to/graph", 8, neug::DBMode::READ_WRITE, "gopt");
```

- **Parameters:**
  - `data_dir`: Path to the graph data directory
  - `max_num_threads`: Maximum threads for concurrent operations. If 0, uses hardware concurrency (number of CPU cores)
  - `mode`: Database access mode (READ_ONLY or READ_WRITE)
  - `planner_kind`: Query planner type: "gopt" (Graph Optimizer) or "greedy"
  - `enable_auto_compaction`: Enable background auto-compaction thread
  - `compact_csr`: Compact CSR structures during auto-compaction
  - `compact_on_close`: Perform compaction when closing database
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
config.thread_num = 8;
config.mode = neug::DBMode::READ_WRITE;
config.memory_level = 2;  // Use hugepages
config.enable_auto_compaction = true;
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
- Creates checkpoint if checkpoint_on_close is enabled
- Performs compaction if compact_on_close is enabled
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

- **Notes:**
  - This method is idempotent - calling it multiple times is safe.
  - After closing, the database cannot be reopened. Create a new `NeugDB` instance to open the database again.

- **Since:** v0.1.0

#### `IsClosed() const`

Check if the database is closed.

- **Returns:** `true` if the database is closed.

#### `Connect()`

Create a new connection to the database for query execution.

Creates and returns a `Connection` object that can be used to execute Cypher queries against the database. The connection shares the query planner and processor with other connections from the same database.

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
  - Connections share the planner instance for efficiency.

- **Throws:**
  - `std::runtime_error`: if database is not open or closed

- **Returns:** `std::shared_ptr`<Connection> A shared pointer to the new `Connection`

- **Since:** v0.1.0

#### `RemoveConnection(std::shared_ptr< Connection > conn)`

Remove a connection from the database.

- **Parameters:**
  - `conn`: The connection to be removed.

- **Notes:**
  - This method is used to remove a connection when it is closed, to remove the handle from the database.
  - This method is not thread-safe, so it should be called only when the connection is closed. And should be only called internally.

