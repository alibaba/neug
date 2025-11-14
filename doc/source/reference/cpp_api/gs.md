# Namespace gs



## Classes and Structures

## Connection ⭐

**Full name:** `gs::Connection` **[Key API Class]**

Internal connection class for executing queries against the graph database.

`Connection` provides the core interface for query execution within a NeuG database. It maintains a reference to the property graph, query planner, and query processor to handle the complete query execution pipeline.
**Thread Safety:** This class is NOT thread-safe. Each thread should create and manage its own `Connection` instance.
**Lifecycle:**
- Created with references to graph, planner, and query processor
- Used to execute queries via `Query()` method
- Must be explicitly closed or will be closed in destructor

### Constructors & Destructors

#### `Connection(...)`

```cpp
Connection(
    PropertyGraph &graph,
    std::shared_ptr< IGraphPlanner > planner,
    std::shared_ptr< QueryProcessor > query_processor
)
```

- **Parameters:**
  - `graph`
  - `planner`
  - `query_processor`

#### `~Connection()`

### Public Methods

#### `Query(const std::string &query_string)`

Execute a query string and return the results.

Processes the query string through the planner and query processor, then converts the internal CollectiveResults to a `QueryResult` for user consumption.
`QueryResult` using ``QueryResult::From`()`.

- **Parameters:**
  - `query_string`: The query string to execute

- **Throws:**
  - Logs: error and returns error status if connection is closed

- **Returns:** `Result<QueryResult>` containing either the query results or an error status

- **Since:** v0.1.0

#### `GetSchema() const`

Get the database schema.

Returns a reference to the schema from the underlying property graph.

- **Throws:**
  - `std::runtime_error`: if the connection is closed

- **Returns:** `const` `Schema`& Reference to the database schema

- **Since:** v0.1.0

#### `Close()`

Close the connection and mark it as closed.

Sets the `is_closed_` atomic flag to `true`. This method is idempotent and safe to call multiple times.
Implementation: Uses atomic store operation to set `is_closed_` flag. Logs warning if already closed, otherwise logs info message.

- **Since:** v0.1.0

#### `IsClosed() const`

Check if the connection is closed.

- **Returns:** `true` if the connection has been closed, `false` otherwise

- **Since:** v0.1.0


---

## InsertTransaction ⭐

**Full name:** `gs::InsertTransaction` **[Key API Class]**

Transaction for inserting new vertices and edges into the graph.

`InsertTransaction` handles the insertion of new graph elements with ACID guarantees. It maintains a Write-Ahead Log (WAL) for durability and tracks added vertices to resolve edge insertions that reference newly added vertices.
**Key Features:**
- Write-Ahead Logging for durability
- Vertex insertion with property validation
- Edge insertion with vertex existence checking
- Automatic vertex resolution for new edges
- Transaction commit`/abort` semantics
**Implementation Details:**
- Uses grape::OutArchive for serializing operations to WAL
- Maintains `added_vertices_` map to track new vertices
- Destructor calls `Abort()` for cleanup
- Validates property types against schema

### Constructors & Destructors

#### `InsertTransaction(...)`

```cpp
InsertTransaction(
    const NeugDBSession &session,
    PropertyGraph &graph,
    Allocator &alloc,
    IWalWriter &logger,
    IVersionManager &vm,
    timestamp_t timestamp
)
```

Construct an `InsertTransaction`.

- **Parameters:**
  - `session`: Reference to the database session
  - `graph`: Reference to the property graph (mutable for insertions)
  - `alloc`: Reference to memory allocator
  - `logger`: Reference to WAL writer
  - `vm`: Reference to version manager
  - `timestamp`: Transaction timestamp

- **Since:** v0.1.0

#### `~InsertTransaction()`

Destructor that calls `Abort()`.

Implementation: Calls `Abort()` to ensure proper cleanup and release resources.

- **Since:** v0.1.0

### Public Methods

#### `AddVertex(label_t label, const Any &id, const std::vector< Any > &props)`

Add a new vertex to the transaction.

Validates properties against schema and serializes the vertex insertion to WAL. Tracks the added vertex for later edge resolution.

- **Parameters:**
  - `label`: Vertex label/type
  - `id`: Vertex primary key value
  - `props`: Vector of property values matching schema order

- **Returns:** `true` if vertex added successfully, `false` if validation fails

- **Since:** v0.1.0

#### `AddEdge(...)`

```cpp
AddEdge(
    label_t src_label,
    const Any &src,
    label_t dst_label,
    const Any &dst,
    label_t edge_label,
    const Any &prop
)
```

Add a new edge to the transaction.

Checks for existence of source and destination vertices (including newly added ones), then serializes the edge insertion to WAL.

- **Parameters:**
  - `src_label`: Source vertex label
  - `src`: Source vertex ID
  - `dst_label`: Destination vertex label
  - `dst`: Destination vertex ID
  - `edge_label`: Edge label/type
  - `prop`: Edge property value

- **Returns:** `true` if edge added successfully, `false` if vertices don't exist

- **Since:** v0.1.0

#### `Commit()`

Commit the transaction.

Writes the WAL data to persistent storage and releases the timestamp. Returns early if no operations were performed.

- **Returns:** `true` if commit successful

- **Since:** v0.1.0

#### `Abort()`

#### `timestamp() const`

#### `schema() const`

#### `GetSession() const`

#### `IngestWal(...)`

```cpp
IngestWal(
    PropertyGraph &graph,
    uint32_t timestamp,
    char *data,
    size_t length,
    Allocator &alloc
)
```

- **Parameters:**
  - `graph`
  - `timestamp`
  - `data`
  - `length`
  - `alloc`


---

## NeugDB ⭐

**Full name:** `gs::NeugDB` **[Key API Class]**

Core database engine for NeuG graph database system.

`NeugDB` serves as the main entry point and central management system for the NeuG graph database. It coordinates all major components including storage, transactions, query processing, and application management.
**Key Components:**
- `PropertyGraph` for graph data storage and schema management
- `TransactionManager` for ACID transaction control
- `QueryProcessor` for Cypher query execution
- VersionManager for multi-version concurrency control
- `ConnectionManager` for client connection handling
**Database Modes:**
- READ_ONLY: Read-only access for query operations
- READ_WRITE: Full read`/write` access with transaction support
**Thread Safety:** This class is thread-safe. Multiple sessions can access the database concurrently through the connection manager.
**Resource Management:**
- Handles file locking to prevent concurrent database access
- Manages background compaction and monitoring threads
- Provides graceful shutdown with optional data dumping

### Constructors & Destructors

#### `NeugDB()`

#### `~NeugDB()`

### Public Methods

#### `Open(...)`

```cpp
Open(
    const std::string &data_dir,
    int32_t max_num_threads=0,
    const DBMode mode=DBMode::READ_WRITE,
    const std::string &planner_kind="gopt",
    bool warmup=false,
    bool enable_auto_compaction=false,
    bool compact_csr=true,
    bool compact_on_close=false,
    bool checkpoint_on_close=true
)
```

Load the graph from data directory.

- **Parameters:**
  - `data_dir`: The directory of graph data.
  - `max_num_threads`: The maximum number of threads for graph db concurrency. If it is 0, it will be set to the number of hardware cores.
  - `mode`: The mode of opening graph db, could be "read_only" or "read_write".
  - `planner_kind`: The kind of graph planner, could be "gopt" or "greedy"
  - `warmup`: Whether to warm up the graph in memory.
  - `enable_auto_compaction`: Whether to enable auto compaction thread.
  - `compact_csr`: Whether to compact the csr when doing auto compaction.
  - `compact_on_close`: Whether to compact the graph when closing the graph db.
  - `checkpoint_on_close`: Whether to dump the graph when closing the graph db.

- **Notes:**
  - This function is mainly for python binding.

- **Returns:** `true` if successed.

#### `Open(const NeugDBConfig &config)`

Load the graph from data directory.

- **Parameters:**
  - `config`

- **Returns:** `true` if successed.

#### `Open(const Schema &schema, const NeugDBConfig &config)`

Load the graph from data directory with a given schema.

- **Parameters:**
  - `schema`: The schema of the graph. Could be empty if the graph already exists in data_dir.
  - `config`: The configuration for opening the graph db.

- **Returns:** `true` if successed.

#### `Close()`

Close the current opened graph.

#### `Connect()`

Open a connection to the database.

- **Notes:**
  - We the mode is read-only, this method could be called multiple times. But if the mode is read-write, this method should be called only once.
  - Each connection will hold a shared pointer, which means it will share the planner with other connections in the same database.

- **Returns:** A `Connection` object that can be used to interact with the database.

#### `RemoveConnection(std::shared_ptr< Connection > conn)`

Remove a connection from the database.

- **Parameters:**
  - `conn`: The connection to be removed.

- **Notes:**
  - This method is used to remove a connection when it is closed, to remove the handle from the database.
  - This method is not thread-safe, so it should be called only when the connection is closed. And should be only called internally.

#### `GetReadTransaction(int thread_id=0)`

Create a transaction to read vertices and edges.

- **Parameters:**
  - `thread_id`

- **Returns:** graph_dir The directory of graph data.

#### `GetInsertTransaction(int thread_id=0)`

Create a transaction to insert vertices and edges with a default allocator.

- **Parameters:**
  - `thread_id`

- **Returns:** `InsertTransaction`

#### `GetUpdateTransaction(int thread_id=0)`

Create a transaction to update vertices and edges.

- **Parameters:**
  - `thread_id`

- **Returns:** `UpdateTransaction`

#### `GetCompactTransaction(int thread_id=0)`

Create a transaction to compact the graph.

- **Parameters:**
  - `thread_id`

- **Returns:** `CompactTransaction`

#### `graph() const`

#### `graph()`

#### `schema() const`

#### `GetSession(int thread_id)`

- **Parameters:**
  - `thread_id`

#### `GetSession(int thread_id) const`

- **Parameters:**
  - `thread_id`

#### `SessionNum() const`

#### `work_dir() const`

#### `config() const`

#### `GetPlanner() const`

#### `Version() const`

#### `SwitchToTPMode()`

Switch the graph db to TP mode.

This method should be called before starting the server to ensure that the version manager is appropriate for transactional processing workloads.

#### `SwitchToAPMode()`

Switch the graph db to AP mode.

This method should be called before starting the server to ensure that the version manager is appropriate for analytical processing workloads.


---

## PropertyGraph ⭐

**Full name:** `gs::PropertyGraph` **[Key API Class]**

Core property graph storage engine managing vertices, edges, and schema.

`PropertyGraph` provides the fundamental storage layer for graph data in NeuG. It manages vertex and edge tables, schema information, and provides persistence through file-based storage with memory management optimizations.
**Key Components:**
- Vertex tables for storing vertex data and properties
- Edge tables using various CSR (Compressed Sparse Row) formats
- `Schema` management for vertex`/edge` types and properties
- Memory management with configurable memory levels
- Persistence with snapshot and compaction support
**Implementation Details:**
- `vertex_tables_` stores vertex data indexed by label
- `edge_tables_` stores edge data in CSR format
- `schema_` manages type definitions and property schemas
- `work_dir_` stores the working directory for persistence
- `memory_level_` controls memory usage vs performance tradeoff

### Constructors & Destructors

#### `PropertyGraph()`

Construct `PropertyGraph` with default settings.

Implementation: Initializes `vertex_label_num_`=0, `edge_label_num_`=0, `memory_level_`=1.

- **Since:** v0.1.0

#### `~PropertyGraph()`

Destructor that reserves space and cleans up resources.

Implementation: Calculates degree lists for vertices, reserves space in vertex and edge tables to optimize memory layout before destruction.

- **Since:** v0.1.0

### Public Methods


#### `UpdateEdge(...)`

```cpp
UpdateEdge(
    label_t src_label,
    vid_t src_lid,
    label_t dst_label,
    vid_t dst_lid,
    label_t edge_label,
    timestamp_t ts,
    const Any &arc,
    Allocator &alloc
)
```

Update an existing edge's property data.

This method updates the properties of an existing edge in the graph. The edge is identified by its source`/destination` vertices and edge label.
``EdgeTable::UpdateEdge`()` on the corresponding edge table.

- **Parameters:**
  - `src_label`: Source vertex label
  - `src_lid`: Source vertex local ID
  - `dst_label`: Destination vertex label
  - `dst_lid`: Destination vertex local ID
  - `edge_label`: Edge label/type
  - `ts`: Timestamp for the update
  - `arc`: New property data for the edge
  - `alloc`: Memory allocator for update operations

- **Since:** v0.1.0

#### `Open(const std::string &work_dir, int memory_level)`

Open the property graph from persistent storage.

- **Parameters:**
  - `work_dir`: Working directory containing graph data files
  - `memory_level`: Memory usage level (controls performance vs memory tradeoff)

- **Since:** v0.1.0

#### `Compact(bool reset_timestamp, bool compact_csr, float reserve_ratio, timestamp_t ts)`

- **Parameters:**
  - `reset_timestamp`
  - `compact_csr`
  - `reserve_ratio`
  - `ts`

#### `Dump()`

#### `DumpSchema()`

Dump schema information to a file.

- **Parameters:**
  - `filename`: Target file for schema dump

- **Since:** v0.1.0

#### `schema() const`

Get read-only access to the schema.

- **Returns:** `const` `Schema`& Reference to the graph schema

- **Since:** v0.1.0

#### `mutable_schema()`

Get mutable access to the schema.

- **Returns:** `Schema`& Mutable reference to the graph schema

- **Since:** v0.1.0

#### `Clear()`

Clear all graph data and reset to empty state.

Implementation: Clears `vertex_tables_`, `edge_tables_`, resets label counts to 0, and calls `schema_`.Clear().

- **Since:** v0.1.0

#### `CreateVertexType(...)`

```cpp
CreateVertexType(
    const std::string &vertex_type_name,
    const std::vector< std::tuple< PropertyType, std::string, Any > > &properties,
    const std::vector< std::string > &primary_key_names,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `vertex_type_name`
  - `properties`
  - `primary_key_names`
  - `error_on_conflict`

#### `CreteEdgeType(...)`

```cpp
CreteEdgeType(
    const std::string &src_vertex_type,
    const std::string &dst_vertex_type,
    const std::string &edge_type_name,
    const std::vector< std::tuple< PropertyType, std::string, Any > > &properties,
    bool error_on_conflict=true,
    EdgeStrategy oe_strategy=EdgeStrategy::kMultiple,
    EdgeStrategy ie_strategy=EdgeStrategy::kMultiple
)
```

- **Parameters:**
  - `src_vertex_type`
  - `dst_vertex_type`
  - `edge_type_name`
  - `properties`
  - `error_on_conflict`
  - `oe_strategy`
  - `ie_strategy`

#### `AddVertexProperties(...)`

```cpp
AddVertexProperties(
    const std::string &vertex_type_name,
    const std::vector< std::tuple< PropertyType, std::string, Any > > &add_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `vertex_type_name`
  - `add_properties`
  - `error_on_conflict`

#### `AddEdgeProperties(...)`

```cpp
AddEdgeProperties(
    const std::string &src_type_name,
    const std::string &dst_type_name,
    const std::string &edge_type_name,
    const std::vector< std::tuple< PropertyType, std::string, Any > > &add_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `src_type_name`
  - `dst_type_name`
  - `edge_type_name`
  - `add_properties`
  - `error_on_conflict`

#### `RenameVertexProperties(...)`

```cpp
RenameVertexProperties(
    const std::string &vertex_type_name,
    const std::vector< std::tuple< std::string, std::string > > &rename_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `vertex_type_name`
  - `rename_properties`
  - `error_on_conflict`

#### `RenameEdgeProperties(...)`

```cpp
RenameEdgeProperties(
    const std::string &src_type_name,
    const std::string &dst_type_name,
    const std::string &edge_type_name,
    const std::vector< std::tuple< std::string, std::string > > &rename_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `src_type_name`
  - `dst_type_name`
  - `edge_type_name`
  - `rename_properties`
  - `error_on_conflict`

#### `DeleteVertexProperties(...)`

```cpp
DeleteVertexProperties(
    const std::string &vertex_type_name,
    const std::vector< std::string > &delete_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `vertex_type_name`
  - `delete_properties`
  - `error_on_conflict`

#### `DeleteEdgeProperties(...)`

```cpp
DeleteEdgeProperties(
    const std::string &src_type_name,
    const std::string &dst_type_name,
    const std::string &edge_type_name,
    const std::vector< std::string > &delete_properties,
    bool error_on_conflict=true
)
```

- **Parameters:**
  - `src_type_name`
  - `dst_type_name`
  - `edge_type_name`
  - `delete_properties`
  - `error_on_conflict`

#### `DeleteVertexType(const std::string &vertex_type_name, bool is_detach, bool error_on_conflict)`

- **Parameters:**
  - `vertex_type_name`
  - `is_detach`
  - `error_on_conflict`

#### `DeleteEdgeType(...)`

```cpp
DeleteEdgeType(
    const std::string &src_vertex_type,
    const std::string &dst_vertex_type,
    const std::string &edge_type,
    bool error_on_conflict
)
```

- **Parameters:**
  - `src_vertex_type`
  - `dst_vertex_type`
  - `edge_type`
  - `error_on_conflict`

#### `batch_add_vertices(...)`

```cpp
batch_add_vertices(
    label_t v_label_id,
    std::vector< Any > &&ids,
    std::unique_ptr< Table > &&table,
    timestamp_t ts
)
```

- **Parameters:**
  - `v_label_id`
  - `ids`
  - `table`
  - `ts`

#### `batch_add_edges(...)`

```cpp
batch_add_edges(
    label_t src_label_id,
    label_t dst_label_id,
    label_t edge_label_id,
    std::vector< std::tuple< vid_t, vid_t, size_t > > &&edges_vec,
    std::unique_ptr< Table > &&table
)
```

- **Parameters:**
  - `src_label_id`
  - `dst_label_id`
  - `edge_label_id`
  - `edges_vec`
  - `table`

#### `batch_delete_vertices(const label_t &v_label_id, const std::vector< vid_t > &vids)`

- **Parameters:**
  - `v_label_id`
  - `vids`

#### `batch_delete_edges(...)`

```cpp
batch_delete_edges(
    const label_t &src_v_label,
    const label_t &dst_v_label,
    const label_t &edge_label,
    std::vector< std::tuple< vid_t, vid_t > > &edges_vec
)
```

- **Parameters:**
  - `src_v_label`
  - `dst_v_label`
  - `edge_label`
  - `edges_vec`

#### `get_vertex_table(label_t vertex_label)`

- **Parameters:**
  - `vertex_label`

#### `get_vertex_table(label_t vertex_label) const`

- **Parameters:**
  - `vertex_label`

#### `LidNum(label_t vertex_label) const`

- **Parameters:**
  - `vertex_label`

#### `VertexNumlabel_t vertex_label, timestamp_t ts=MAX_TIMESTAMP) const`

- **Parameters:**
  - `vertex_label`
  - `ts`

#### `IsValidLid(label_t vertex_label, vid_t lid, timestamp_t ts) const`

- **Parameters:**
  - `vertex_label`
  - `lid`
  - `ts`

#### `edge_num(label_t src_label, label_t edge_label, label_t dst_label) const`

- **Parameters:**
  - `src_label`
  - `edge_label`
  - `dst_label`

#### `get_lid(label_t label, const Any &oid, vid_t &lid, timestamp_t ts) const`

- **Parameters:**
  - `label`
  - `oid`
  - `lid`
  - `ts`

#### `GetOid(label_t label, vid_t lid, timestamp_t ts) const`

- **Parameters:**
  - `label`
  - `lid`
  - `ts`

#### `AddVertex(label_t label, const Any &id, timestamp_t ts)`

- **Parameters:**
  - `label`
  - `id`
  - `ts`

#### `AddVertexSafe(label_t label, const Any &id, timestamp_t ts)`

- **Parameters:**
  - `label`
  - `id`
  - `ts`

#### `get_outgoing_edges(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `get_incoming_edges(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `get_outgoing_edges_mut(label_t label, vid_t u, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `get_incoming_edges_mut(label_t label, vid_t u, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `get_oe_csr(label_t label, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `neighbor_label`
  - `edge_label`

#### `get_oe_csr(label_t label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `neighbor_label`
  - `edge_label`

#### `get_ie_csr(label_t label, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `neighbor_label`
  - `edge_label`

#### `get_ie_csr(label_t label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `neighbor_label`
  - `edge_label`

#### `vertex_table_modified(label_t label) const`

- **Parameters:**
  - `label`

#### `loadSchema(const std::string &filename)`

- **Parameters:**
  - `filename`

#### `get_vertex_timestamps(label_t label) const`

- **Parameters:**
  - `label`

#### `get_vertex_property_column(uint8_t label, const std::string &prop) const`

- **Parameters:**
  - `label`
  - `prop`

#### `get_vertex_id_column(uint8_t label) const`

- **Parameters:**
  - `label`

#### `statisticsFilePath() const`

#### `get_statistics_json() const`

#### `get_schema_yaml_path() const`

#### `generateStatistics() const`


---

## QueryResult ⭐

**Full name:** `gs::QueryResult` **[Key API Class]**

Container for query execution results with iterator-based access.

`QueryResult` wraps a CollectiveResults protobuf message and provides convenient C++ iterator-style access to the result records. It maintains an internal cursor for sequential access via `hasNext()`/next() pattern.
**Internal Structure:**
- Wraps `results::CollectiveResults` protobuf message
- Maintains `cur_index_` for iteration state
- Provides both sequential and random access to records
**Memory Model:** Returns pointers to internal data without copying.

### Constructors & Destructors

#### `QueryResult()=default`

Default constructor creating empty result.

- **Since:** v0.1.0

#### `QueryResult(results::CollectiveResults &&res)`

Construct `QueryResult` from CollectiveResults.

- **Parameters:**
  - `res`: CollectiveResults to be moved and stored

- **Since:** v0.1.0

#### `~QueryResult()`

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

#### `operator[](int index)`

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

#### `get_result() const`


---

## ReadTransaction ⭐

**Full name:** `gs::ReadTransaction` **[Key API Class]**

Read-only transaction for consistent snapshot access to graph data.

`ReadTransaction` provides read access to graph data at a specific timestamp, implementing snapshot isolation. It stores references to the session, graph, version manager, and the snapshot timestamp.
**Implementation Details:**
- Stores `const` reference to `PropertyGraph` for read-only access
- Maintains timestamp for consistent snapshot view
- Calls release() in destructor for cleanup
- `Commit()` simply calls release() and returns `true`
**Thread Safety:** Read operations are safe for concurrent access.

### Constructors & Destructors

#### `ReadTransaction(...)`

```cpp
ReadTransaction(
    const NeugDBSession &session,
    const PropertyGraph &graph,
    IVersionManager &vm,
    timestamp_t timestamp
)
```

Construct a `ReadTransaction`.

- **Parameters:**
  - `session`: Reference to the database session
  - `graph`: Const reference to the property graph
  - `vm`: Reference to version manager
  - `timestamp`: Snapshot timestamp for this transaction

- **Since:** v0.1.0

#### `~ReadTransaction()`

Destructor that calls release().

Implementation: Calls release() to clean up resources.

- **Since:** v0.1.0

### Public Methods

#### `timestamp() const`

#### `Commit()`

#### `Abort()`

#### `graph() const`

#### `get_vertex_property_column(uint8_t label, const std::string &col_name) const`

- **Parameters:**
  - `label`
  - `col_name`

#### `get_vertex_ref_property_column(uint8_t label, const std::string &col_name) const`

Get the handle of the vertex property column, including the primary key.

T
The type of the column.

- **Parameters:**
  - ``
  - `label`: The label of the vertex.
  - `col_name`: The name of the column.

#### `GetVertexIterator(label_t label) const`

- **Parameters:**
  - `label`

#### `FindVertex(label_t label, const Any &id) const`

- **Parameters:**
  - `label`
  - `id`

#### `GetVertexIndex(label_t label, const Any &id, vid_t &index) const`

- **Parameters:**
  - `label`
  - `id`
  - `index`

#### `GetVertexNum(label_t label) const`

- **Parameters:**
  - `label`

#### `GetVertexSet(label_t label) const`

- **Parameters:**
  - `label`

#### `IsValidVertex(label_t label, vid_t index) const`

- **Parameters:**
  - `label`
  - `index`

#### `GetVertexId(label_t label, vid_t index) const`

- **Parameters:**
  - `label`
  - `index`

#### `GetOutEdgeIterator(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetInEdgeIterator(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetOutDegree(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetInDegree(label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetOutgoingEdges(label_t v_label, vid_t v, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `v`
  - `neighbor_label`
  - `edge_label`

#### `GetIncomingEdges(label_t v_label, vid_t v, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `v`
  - `neighbor_label`
  - `edge_label`

#### `schema() const`

#### `GetOutgoingGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetIncomingGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetOutgoingSingleGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetIncomingSingleGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetOutgoingSingleImmutableGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetIncomingSingleImmutableGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetOutgoingImmutableGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetIncomingImmutableGraphView(label_t v_label, label_t neighbor_label, label_t edge_label) const`

- **Parameters:**
  - ``
  - `v_label`
  - `neighbor_label`
  - `edge_label`

#### `GetSession() const`


---

## UpdateTransaction ⭐

**Full name:** `gs::UpdateTransaction` **[Key API Class]**

Transaction for updating existing graph elements (vertices and edges).

`UpdateTransaction` handles modifications to existing graph data with ACID guarantees. It supports updating vertex properties, edge properties, and provides options for vertex column resizing during updates.
**Key Features:**
- Update vertex and edge properties
- Configurable vertex column resizing behavior
- Write-Ahead Logging for durability
- MVCC support with timestamp management
- Commit`/abort` transaction semantics
**Implementation Details:**
- `insert_vertex_with_resize_` controls whether to resize vertex columns
- Uses work_dir for temporary storage during updates
- Destructor calls release() for cleanup
- Integrates with version manager for timestamp coordination

### Constructors & Destructors

#### `UpdateTransaction(...)`

```cpp
UpdateTransaction(
    const NeugDBSession &session,
    PropertyGraph &graph,
    Allocator &alloc,
    const std::string &work_dir,
    IWalWriter &logger,
    IVersionManager &vm,
    timestamp_t timestamp
)
```

Construct an `UpdateTransaction`.

- **Parameters:**
  - `session`: Reference to the database session
  - `graph`: Reference to the property graph (mutable for updates)
  - `alloc`: Reference to memory allocator
  - `work_dir`: Working directory for temporary files
  - `logger`: Reference to WAL writer
  - `vm`: Reference to version manager
  - `timestamp`: Transaction timestamp

- **Since:** v0.1.0

#### `~UpdateTransaction()`

Destructor that calls release().

Implementation: Calls release() to clean up resources and release timestamp.

- **Since:** v0.1.0

### Public Methods

#### `set_insert_vertex_with_resize(bool insert_vertex_with_resize)`

Configure whether to resize vertex columns during property updates.

By default, update transactions will not resize vertex columns when updating properties. Setting this to `true` enables column resizing if needed.

- **Parameters:**
  - `insert_vertex_with_resize`: true to enable column resizing, false to disable

- **Since:** v0.1.0

#### `timestamp() const`

Get the transaction timestamp.

- **Returns:** `timestamp_t` The timestamp for this transaction

- **Since:** v0.1.0

#### `schema() const`

Get read-only access to the graph schema.

- **Returns:** `const` `Schema`& Reference to the graph schema

- **Since:** v0.1.0

#### `Commit()`

#### `Abort()`

#### `AddVertex(label_t label, const Any &oid, const std::vector< Any > &props)`

- **Parameters:**
  - `label`
  - `oid`
  - `props`

#### `AddVertex(label_t label, const Any &oid, const std::vector< Any > &props, vid_t &vid)`

- **Parameters:**
  - `label`
  - `oid`
  - `props`
  - `vid`

#### `AddEdge(...)`

```cpp
AddEdge(
    label_t src_label,
    const Any &src,
    label_t dst_label,
    const Any &dst,
    label_t edge_label,
    const Any &value
)
```

- **Parameters:**
  - `src_label`
  - `src`
  - `dst_label`
  - `dst`
  - `edge_label`
  - `value`

#### `AddEdge(...)`

```cpp
AddEdge(
    label_t src_label,
    vid_t src,
    label_t dst_label,
    vid_t dst,
    label_t edge_label,
    const Any &value
)
```

- **Parameters:**
  - `src_label`
  - `src`
  - `dst_label`
  - `dst`
  - `edge_label`
  - `value`

#### `GetVertexIterator(label_t label)`

- **Parameters:**
  - `label`

#### `GetOutEdgeIterator(label_t label, vid_t u, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetInEdgeIterator(label_t label, vid_t u, label_t neighbor_label, label_t edge_label)`

- **Parameters:**
  - `label`
  - `u`
  - `neighbor_label`
  - `edge_label`

#### `GetVertexField(label_t label, vid_t lid, int col_id) const`

- **Parameters:**
  - `label`
  - `lid`
  - `col_id`

#### `SetVertexField(label_t label, vid_t lid, int col_id, const Any &value)`

- **Parameters:**
  - `label`
  - `lid`
  - `col_id`
  - `value`

#### `SetEdgeData(...)`

```cpp
SetEdgeData(
    bool dir,
    label_t label,
    vid_t v,
    label_t neighbor_label,
    vid_t nbr,
    label_t edge_label,
    const Any &value,
    int32_t col_id=0
)
```

- **Parameters:**
  - `dir`
  - `label`
  - `v`
  - `neighbor_label`
  - `nbr`
  - `edge_label`
  - `value`
  - `col_id`

#### `GetUpdatedEdgeData(...)`

```cpp
GetUpdatedEdgeData(
    bool dir,
    label_t label,
    vid_t v,
    label_t neighbor_label,
    vid_t nbr,
    label_t edge_label,
    Any &ret) const
)
```

- **Parameters:**
  - `dir`
  - `label`
  - `v`
  - `neighbor_label`
  - `nbr`
  - `edge_label`
  - `ret`

#### `GetVertexId(label_t label, vid_t lid) const`

- **Parameters:**
  - `label`
  - `lid`

#### `GetSession() const`

#### `GetGraph() const`

#### `HasVertex(label_t label, const Any &oid) const`

Check if the vertex with the given label and oid exists.

- **Parameters:**
  - `label`: The label of the vertex.
  - `oid`: The oid of the vertex.

- **Returns:** `true` if the vertex exists, `false` otherwise.

#### `IngestWal(...)`

```cpp
IngestWal(
    PropertyGraph &graph,
    const std::string &work_dir,
    uint32_t timestamp,
    char *data,
    size_t length,
    Allocator &alloc
)
```

- **Parameters:**
  - `graph`
  - `work_dir`
  - `timestamp`
  - `data`
  - `length`
  - `alloc`


---

## APVersionManager

**Full name:** `gs::APVersionManager`

`APVersionManager` implements the version manager for Analytical Processing (AP) workloads.

It allows multiple concurrent read and insert transactions, but only one update transaction at a time. Update transactions will wait for all ongoing read and insert transactions to complete before pro...

**Public Methods:**

- `APVersionManager()`
- `~APVersionManager()`
- `init_ts(uint32_t ts, int thread_num) override`
- `clear() override`
- `acquire_read_timestamp() override`
- `release_read_timestamp() override`
- `acquire_insert_timestamp() override`
- `release_insert_timestamp(uint32_t ts) override`
- `acquire_update_timestamp() override`
- `release_update_timestamp(uint32_t ts) override`
- ... and 1 more methods


---

## AbstractArrowFragmentLoader

**Full name:** `gs::AbstractArrowFragmentLoader`

**Public Methods:**

- `AbstractArrowFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `~AbstractArrowFragmentLoader()`
- `AddVerticesRecordBatch(label_t v_label_id, const std::vector< std::strin...`
- `AddEdgesRecordBatch(label_t src_label_id, label_t dst_label_id, label...`
- `BatchConsumer(const std::vector< std::string > &files, size_t c...`
- `append_edges_utils(std::shared_ptr< arrow::Array > src_col, std::sha...`
- `batch_load_vertices(PropertyGraph &graph, const label_t &v_label_id, ...`
- `batch_load_edges(PropertyGraph &graph, const label_t &src_v_label,...`


---

## AdjListView

**Full name:** `gs::AdjListView`

**Public Methods:**

- `AdjListView(const slice_t &slice, timestamp_t timestamp)`
- `begin() const`
- `end() const`
- `estimated_degree() const`


---

## nbr_iterator

**Full name:** `gs::AdjListView::nbr_iterator`

**Public Methods:**

- `nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end, timesta...`
- `operator*() const`
- `operator->() const`
- `operator++()`
- `operator==(const nbr_iterator &rhs) const`
- `operator!=(const nbr_iterator &rhs) const`


---

## Any

**Full name:** `gs::Any`

**Public Methods:**

- `Any()`
- `Any(const Any &other)`
- `Any(Any &&other)`
- `Any(const std::initializer_list< Any > &list)`
- `Any(const std::vector< Any > &vec)`
- `Any(const std::string &str)`
- `Any(const T &val)`
- `operator=(const Any &other)`
- `~Any()`
- `get_long() const`
- ... and 43 more methods


---

## AnyConverter

**Full name:** `gs::AnyConverter`


---

## AnyConverter< Date >

**Full name:** `gs::AnyConverter< Date >`

**Public Methods:**

- `type()`
- `to_any(const Date &value)`
- `to_any(int32_t value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< DateTime >

**Full name:** `gs::AnyConverter< DateTime >`

**Public Methods:**

- `type()`
- `to_any(const DateTime &value)`
- `to_any(int64_t value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< GlobalId >

**Full name:** `gs::AnyConverter< GlobalId >`

**Public Methods:**

- `type()`
- `to_any(const GlobalId &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< Interval >

**Full name:** `gs::AnyConverter< Interval >`

**Public Methods:**

- `type()`
- `to_any(const Interval &value)`
- `to_any(uint64_t value)`
- `to_any(std::string_view value)` - Parse the interval from a string
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< LabelKey >

**Full name:** `gs::AnyConverter< LabelKey >`

**Public Methods:**

- `type()`
- `to_any(const LabelKey &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< Record >

**Full name:** `gs::AnyConverter< Record >`

**Public Methods:**

- `type()`
- `to_any(const Record &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`





---

## AnyConverter< TimeStamp >

**Full name:** `gs::AnyConverter< TimeStamp >`

**Public Methods:**

- `type()`
- `to_any(const TimeStamp &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< bool >

**Full name:** `gs::AnyConverter< bool >`

**Public Methods:**

- `type()`
- `to_any(const bool &value)`
- `from_any(const Any &value)`
- `type_name()`
- `from_any_value(const AnyValue &value)`


---

## AnyConverter< double >

**Full name:** `gs::AnyConverter< double >`

**Public Methods:**

- `type()`
- `to_any(const double &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< float >

**Full name:** `gs::AnyConverter< float >`

**Public Methods:**

- `type()`
- `to_any(const float &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## EmptyType >

**Full name:** `gs::AnyConverter< grape::EmptyType >`

**Public Methods:**

- `type()`
- `to_any(const grape::EmptyType &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< int32_t >

**Full name:** `gs::AnyConverter< int32_t >`

**Public Methods:**

- `type()`
- `to_any(const int32_t &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< int64_t >

**Full name:** `gs::AnyConverter< int64_t >`

**Public Methods:**

- `type()`
- `to_any(const int64_t &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## string >

**Full name:** `gs::AnyConverter< std::string >`

**Public Methods:**

- `type()`
- `to_any(const std::string &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## string_view >

**Full name:** `gs::AnyConverter< std::string_view >`

**Public Methods:**

- `type()`
- `to_any(const std::string_view &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< uint16_t >

**Full name:** `gs::AnyConverter< uint16_t >`

**Public Methods:**

- `type()`
- `to_any(const uint16_t &value)`
- `from_any(const Any &value)`
- `type_name()`


---

## AnyConverter< uint32_t >

**Full name:** `gs::AnyConverter< uint32_t >`

**Public Methods:**

- `type()`
- `to_any(const uint32_t &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< uint64_t >

**Full name:** `gs::AnyConverter< uint64_t >`

**Public Methods:**

- `type()`
- `to_any(const uint64_t &value)`
- `from_any(const Any &value)`
- `from_any_value(const AnyValue &value)`
- `type_name()`


---

## AnyConverter< uint8_t >

**Full name:** `gs::AnyConverter< uint8_t >`

**Public Methods:**

- `type()`
- `to_any(const uint8_t &value)`
- `from_any(const Any &value)`
- `type_name()`


---

## AppBase

**Full name:** `gs::AppBase`

**Public Methods:**

- `type() const =0`
- `mode() const =0`
- `run(NeugDBSession &db, Decoder &input, Encoder &output)=0`
- `~AppBase()`


---

## AppFactoryBase

**Full name:** `gs::AppFactoryBase`

**Public Methods:**

- `AppFactoryBase()`
- `~AppFactoryBase()`
- `CreateApp(const NeugDB &db)=0`


---

## AppManager

**Full name:** `gs::AppManager`

**Public Methods:**

- `AppManager(NeugDB &db)`
- `~AppManager()`
- `CreateApp(uint8_t app_type, int thread_id)`
- `initApps(const std::unordered_map< std::string, std::pair<...`
- `registerApp(const std::string &plugin_path, uint8_t index)`


---

## AppMetric

**Full name:** `gs::AppMetric`

**Public Methods:**

- `AppMetric()`
- `~AppMetric()`
- `add_record(int64_t val)`
- `empty() const`
- `operator+=(const AppMetric &rhs)`
- `output(const std::string &name) const`


---

## AppWrapper

**Full name:** `gs::AppWrapper`

**Public Methods:**

- `AppWrapper()`
- `AppWrapper(AppBase *app, void(*func_deletor)(void *))`
- `AppWrapper(AppWrapper &&rhs)`
- `~AppWrapper()`
- `operator=(AppWrapper &&rhs)`
- `app()`
- `app() const`


---

## ArenaAllocator

**Full name:** `gs::ArenaAllocator`

**Public Methods:**

- `ArenaAllocator(MemoryStrategy strategy, const std::string &prefix)`
- `~ArenaAllocator()`
- `reserve(size_t cap)`
- `allocate(size_t size)`
- `allocated_memory() const`


---

## ArrowRecordBatchArraySupplier

**Full name:** `gs::ArrowRecordBatchArraySupplier`

A record batch supplier that provides all record batches from a vector of arrays.

Already in memory.

**Public Methods:**

- `ArrowRecordBatchArraySupplier(const std::vector< std::vector< std::shared_ptr< ...`
- `GetNextBatch() override`


---

## ArrowRecordBatchStreamSupplier

**Full name:** `gs::ArrowRecordBatchStreamSupplier`

A record batch supplier that provides all record batches from a arrow reader, which is a streaming reader or a table reader, producing record batches from a CSV file.

**Public Methods:**

- `ArrowRecordBatchStreamSupplier(const std::shared_ptr< arrow::csv::StreamingReade...`
- `GetNextBatch() override`


---

## BasicFragmentLoader

**Full name:** `gs::BasicFragmentLoader`

**Public Methods:**

- `BasicFragmentLoader(const Schema &schema, const std::string &prefix)`
- `~BasicFragmentLoader()`
- `LoadFragment()`
- `FinishAddingVertex(label_t v_label, const IdIndexer< KEY_T, vid_t > &indexer)`
- `AddNoPropEdgeBatch(label_t src_label_id, label_t dst_label_id, label...`
- `PutEdges(label_t src_label_id, label_t dst_label_id, label...`
- `GetVertexTable(size_t ind)`
- `GetEdgePropertiesTable(label_t src_label_id, label_t dst_label_id, label...`
- `GetLFIndexer(label_t v_label) const`
- `GetLFIndexer(label_t v_label)`
- ... and 3 more methods


---

## CSVFragmentLoader

**Full name:** `gs::CSVFragmentLoader`

**Public Methods:**

- `CSVFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `~CSVFragmentLoader()`
- `LoadFragment() override`
- `Make(const std::string &work_dir, const Schema &schema...`


---

## CSVStreamRecordBatchSupplier

**Full name:** `gs::CSVStreamRecordBatchSupplier`

**Public Methods:**

- `CSVStreamRecordBatchSupplier(const std::string &file_path, arrow::csv::Convert...`
- `GetNextBatch() override`


---

## CSVTableRecordBatchSupplier

**Full name:** `gs::CSVTableRecordBatchSupplier`

**Public Methods:**

- `CSVTableRecordBatchSupplier(const std::string &file_path, arrow::csv::Convert...`
- `GetNextBatch() override`


---

## ColumnBase

**Full name:** `gs::ColumnBase`

**Public Methods:**

- `~ColumnBase()`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name)=0`
- `open_with_hugepages(const std::string &name, bool force)=0`
- `close()=0`
- `dump(const std::string &filename)=0`
- `size() const =0`
- `copy_to_tmp(const std::string &cur_path, const std::string &tmp_path)=0`
- `resize(size_t size)=0`
- `type() const =0`
- ... and 6 more methods


---

## ColumnsUtils

**Full name:** `gs::ColumnsUtils`

**Public Methods:**

- `generate_dedup_offset(const VEC_T &vec, size_t row_num, std::vector< si...`


---

## CompactTransaction

**Full name:** `gs::CompactTransaction`

**Public Methods:**

- `CompactTransaction(PropertyGraph &graph, IWalWriter &logger, IVersio...`
- `~CompactTransaction()`
- `timestamp() const`
- `Commit()`
- `Abort()`


---

## ConnectionManager

**Full name:** `gs::ConnectionManager`

**Public Methods:**

- `ConnectionManager(PropertyGraph &graph, std::shared_ptr< IGraphPlan...`
- `~ConnectionManager()`
- `CreateConnection()`
- `Close()` - Close all connections managed by the connection manager
- `RemoveConnection(std::shared_ptr< Connection > conn)` - Remove a connection from the database
- `ConnectionNum() const`


---

## Constants

**Full name:** `gs::Constants`

---

## CountVerticesFactory

**Full name:** `gs::CountVerticesFactory`

**Public Methods:**

- `CountVerticesFactory()=default`
- `~CountVerticesFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## CsrBase

**Full name:** `gs::CsrBase`

**Public Methods:**

- `CsrBase()=default`
- `~CsrBase()=default`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_sort_by_edge_data(timestamp_t ts)`
- `unsorted_since() const`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap)=0`
- `open_with_hugepages(const std::string &prefix, size_t v_cap=0)`
- `dump(const std::string &name, const std::string &new_s...`
- ... and 9 more methods


---

## CsrConstEdgeIterBase

**Full name:** `gs::CsrConstEdgeIterBase`

**Public Methods:**

- `CsrConstEdgeIterBase()=default`
- `~CsrConstEdgeIterBase()=default`
- `get_neighbor() const =0`
- `get_data() const =0`
- `get_timestamp() const =0`
- `size() const =0`
- `operator+=(size_t offset)=0`
- `next()=0`
- `is_valid() const =0`


---

## CsrEdgeIterBase

**Full name:** `gs::CsrEdgeIterBase`

**Public Methods:**

- `CsrEdgeIterBase()=default`
- `~CsrEdgeIterBase()=default`
- `get_neighbor() const =0`
- `get_data() const =0`
- `get_timestamp() const =0`
- `size() const =0`
- `operator+=(size_t offset)=0`
- `next()=0`
- `is_valid() const =0`
- `set_data(const Any &value, timestamp_t ts, int32_t col_id=0)=0`


---

## CypherReadApp

**Full name:** `gs::CypherReadApp`

**Public Methods:**

- `CypherReadApp(const NeugDB &db)`
- `type() const override`
- `Query(const NeugDBSession &graph, Decoder &input, Encod...`


---

## CypherReadAppFactory

**Full name:** `gs::CypherReadAppFactory`

**Public Methods:**

- `CypherReadAppFactory()=default`
- `~CypherReadAppFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## CypherReadProcAppBase

**Full name:** `gs::CypherReadProcAppBase`

**Public Methods:**

- `type() const override`
- `Query(const NeugDBSession &db, ARGS... args)=0`
- `Query(const NeugDBSession &db, Decoder &input, Encoder ...`
- `unpackedAndInvoke(const NeugDBSession &db, std::tuple< ARGS... > &tuple)`


---

## CypherUpdateApp

**Full name:** `gs::CypherUpdateApp`

**Public Methods:**

- `CypherUpdateApp(const NeugDB &db)`
- `type() const override`
- `Query(NeugDBSession &graph, Decoder &input, Encoder &ou...`
- `execute_ddl(NeugDBSession &graph, const physical::DDLPlan &ddl_plan)`
- `execute_update_query(NeugDBSession &graph, const physical::PhysicalPla...`
- `execute_add_vertex_property(NeugDBSession &graph, const physical::AddVertexPr...`
- `execute_add_edge_property(NeugDBSession &graph, const physical::AddEdgeProp...`
- `execute_drop_vertex_property(NeugDBSession &graph, const physical::DropVertexP...`
- `execute_drop_edge_property(NeugDBSession &graph, const physical::DropEdgePro...`
- `execute_rename_vertex_property(NeugDBSession &graph, const physical::RenameVerte...`
- ... and 3 more methods


---

## CypherUpdateAppFactory

**Full name:** `gs::CypherUpdateAppFactory`

**Public Methods:**

- `CypherUpdateAppFactory()=default`
- `~CypherUpdateAppFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## CypherWriteProcAppBase

**Full name:** `gs::CypherWriteProcAppBase`

**Public Methods:**

- `type() const override`
- `Query(NeugDBSession &db, ARGS... args)=0`
- `Query(NeugDBSession &db, Decoder &input, Encoder &outpu...`
- `unpackedAndInvoke(NeugDBSession &db, std::tuple< ARGS... > &tuple)`


---

## DayValue

**Full name:** `gs::DayValue`


---

## Decoder

**Full name:** `gs::Decoder`

**Public Methods:**

- `Decoder(const char *ptr, size_t size)`
- `~Decoder()`
- `get_int()`
- `get_uint()`
- `get_long()`
- `get_string()`
- `get_bytes()`
- `get_small_string()`
- `get_byte()`
- `get_float()`
- ... and 5 more methods


---

## DualCsr

**Full name:** `gs::DualCsr`

**Public Methods:**

- `DualCsr(EdgeStrategy oe_strategy, EdgeStrategy ie_strateg...`
- `~DualCsr()`
- `BatchInit(const std::string &oe_name, const std::string &ie...`
- `BatchInitInMemory(const std::string &edata_name, const std::string ...`
- `Open(const std::string &oe_name, const std::string &ie...`
- `OpenInMemory(const std::string &oe_name, const std::string &ie...`
- `OpenWithHugepages(const std::string &oe_name, const std::string &ie...`
- `Dump(const std::string &oe_name, const std::string &ie...`
- `GetInCsr() override`
- `GetOutCsr() override`
- ... and 12 more methods


---

## DummyWalWriter

**Full name:** `gs::DummyWalWriter`

`DummyWalWriter` is a no-op implementation of the `IWalWriter` interface.

It is used when write-ahead logging is disabled or not required.

**Public Methods:**

- `DummyWalWriter()`
- `~DummyWalWriter()`
- `type() const override`
- `open() override` - Open a wal file
- `close() override` - Close the wal writer
- `append(const char *data, size_t length) override` - Append data to the wal file


---

## EdgeTable

**Full name:** `gs::EdgeTable`

**Public Methods:**

- `EdgeTable(const std::string &src_label_name, const std::str...`
- `EdgeTable(EdgeTable &&edge_table)`
- `EdgeTable(const EdgeTable &)=delete`
- `BatchInit(const std::string &work_dir, const std::vector< i...`
- `Open(const std::string &work_dir, int memory_level, si...`
- `BatchAddEdges(std::vector< std::tuple< vid_t, vid_t, size_t > >...`
- `Dump(const std::string &checkpoint_dir_path)`
- `GetInCsr()`
- `GetOutCsr()`
- `GetInCsr() const`
- ... and 19 more methods


---

## EmptyCsr

**Full name:** `gs::EmptyCsr`

**Public Methods:**

- `EmptyCsr()=default`
- `~EmptyCsr()=default`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- `resize(vid_t vnum) override`
- `size() const override`
- ... and 12 more methods


---

## string_view >

**Full name:** `gs::EmptyCsr< std::string_view >`

**Public Methods:**

- `EmptyCsr(Table &table)`
- `~EmptyCsr()=default`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- `resize(vid_t vnum) override`
- `size() const override`
- `edge_num() const override`
- ... and 14 more methods


---

## Encoder

**Full name:** `gs::Encoder`

**Public Methods:**

- `Encoder(std::vector< char > &buf)`
- `put_long(int64_t v)`
- `skip_long()`
- `put_long_at(size_t pos, int64_t v)`
- `put_int(int v)`
- `put_uint(uint32_t v)`
- `skip_int()`
- `put_int_at(size_t pos, int v)`
- `put_byte(uint8_t v)`
- `put_bytes(const char *data, size_t size)`
- ... and 11 more methods


---

## FileLock

**Full name:** `gs::FileLock`

**Public Methods:**

- `FileLock(const std::string &data_dir)`
- `~FileLock()`
- `lock(std::string &error_msg, DBMode mode)`
- `unlock()`
- `CleanupAllLocks()`


---

## GHash

**Full name:** `gs::GHash`

**Public Methods:**

- `operator()(const T &val) const`


---

## GHash< Any >

**Full name:** `gs::GHash< Any >`

**Public Methods:**

- `operator()(const Any &val) const`


---

## GHash< int64_t >

**Full name:** `gs::GHash< int64_t >`

**Public Methods:**

- `operator()(const int64_t &val) const`


---

## GOptPlanner

**Full name:** `gs::GOptPlanner`

**Public Methods:**

- `GOptPlanner()`
- `type() const override`
- `compilePlan(const std::string &query) override` - Generate the executable plan
- `update_meta(const YAML::Node &schema_yaml_node) override` - Update the metadata of the graph
- `update_statistics(const std::string &graph_statistic_json) override` - Update the statistics of the graph


---

## GTypeUtils

**Full name:** `gs::GTypeUtils`

**Public Methods:**

- `createLogicalType(YAML::Node &node)`
- `toYAML(const gs::common::LogicalType &type)`


---

## GlobalId

**Full name:** `gs::GlobalId`

**Public Methods:**

- `get_label_id(gid_t gid)`
- `get_vid(gid_t gid)`
- `GlobalId()`
- `GlobalId(label_id_t label_id, vid_t vid)`
- `GlobalId(gid_t gid)`
- `label_id() const`
- `vid() const`
- `to_string() const`


---

## GraphView

**Full name:** `gs::GraphView`

**Public Methods:**

- `GraphView(const MutableCsr< EDATA_T > &csr, timestamp_t timestamp)`
- `get_edges(vid_t v) const`
- `foreach_edges_between(vid_t v, EDATA_T &min_value, EDATA_T &max_value, ...`
- `foreach_edges_gt(vid_t v, EDATA_T &min_value, const FUNC_T &func) const`
- `foreach_edges_lt(vid_t v, const EDATA_T &max_value, const FUNC_T &...`
- `foreach_edges_ge(vid_t v, EDATA_T &min_value, const FUNC_T &func) const`


---

## IFragmentLoader

**Full name:** `gs::IFragmentLoader`

**Public Methods:**

- `~IFragmentLoader()=default`
- `LoadFragment()=0`


---

## IGraphPlanner

**Full name:** `gs::IGraphPlanner`

Graph planner interface.

Receive the cypher query, and generate the executable plan.

**Public Methods:**

- `IGraphPlanner()`
- `type() const =0`
- `~IGraphPlanner()=default`
- `compilePlan(const std::string &query)=0` - Generate the executable plan
- `update_meta(const YAML::Node &schema_yaml_node)=0` - Update the metadata of the graph
- `update_statistics(const std::string &graph_statistic_json)=0` - Update the statistics of the graph


---

## IRecordBatchSupplier

**Full name:** `gs::IRecordBatchSupplier`

**Public Methods:**

- `~IRecordBatchSupplier()=default`
- `GetNextBatch()=0`


---

## IVersionManager

**Full name:** `gs::IVersionManager`

**Public Methods:**

- `init_ts(uint32_t ts, int thread_num)=0`
- `acquire_read_timestamp()=0`
- `release_read_timestamp()=0`
- `acquire_insert_timestamp()=0`
- `release_insert_timestamp(uint32_t ts)=0`
- `acquire_update_timestamp()=0`
- `release_update_timestamp(uint32_t ts)=0`
- `revert_update_timestamp(uint32_t ts)=0`
- `clear()=0`
- `~IVersionManager()`


---

## IWalParser

**Full name:** `gs::IWalParser`

The interface of wal parser.

**Public Methods:**

- `~IWalParser()`
- `open(const std::string &wal_uri)=0` - Open wals from a uri and parse the wal files
- `close()=0`
- `last_ts() const =0`
- `get_insert_wal(uint32_t ts) const =0`
- `get_update_wals() const =0` - Get all the update wal units


---

## IWalWriter

**Full name:** `gs::IWalWriter`

The interface of wal writer.

**Public Methods:**

- `~IWalWriter()`
- `type() const =0`
- `open()=0` - Open a wal file
- `close()=0` - Close the wal writer
- `append(const char *data, size_t length)=0` - Append data to the wal file


---

## IdIndexer

**Full name:** `gs::IdIndexer`

**Public Methods:**

- `IdIndexer()`
- `~IdIndexer()`
- `get_type() const override`
- `_add(const Any &oid) override`
- `add(const Any &oid, INDEX_T &lid) override`
- `get_key(const INDEX_T &lid, Any &oid) const override`
- `get_index(const Any &oid, INDEX_T &lid) const override`
- `Clear()`
- `entry_num() const`
- `add(const KEY_T &oid, INDEX_T &lid)`
- ... and 19 more methods


---

## IdIndexerBase

**Full name:** `gs::IdIndexerBase`

**Public Methods:**

- `IdIndexerBase()=default`
- `~IdIndexerBase()=default`
- `get_type() const =0`
- `_add(const Any &oid)=0`
- `add(const Any &oid, INDEX_T &lid)=0`
- `get_key(const INDEX_T &lid, Any &oid) const =0`
- `get_index(const Any &oid, INDEX_T &lid) const =0`
- `size() const =0`


---

## ImmutableAdjListView

**Full name:** `gs::ImmutableAdjListView`

**Public Methods:**

- `ImmutableAdjListView(const slice_t &slice)`
- `begin() const`
- `end() const`
- `estimated_degree() const`


---

## nbr_iterator

**Full name:** `gs::ImmutableAdjListView::nbr_iterator`

**Public Methods:**

- `nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end)`
- `operator*() const`
- `operator->() const`
- `operator++()`
- `operator==(const nbr_iterator &rhs) const`
- `operator!=(const nbr_iterator &rhs) const`


---

## ImmutableCsr

**Full name:** `gs::ImmutableCsr`

**Public Methods:**

- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `batch_sort_by_edge_data(timestamp_t ts) override`
- `unsorted_since() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- `resize(vid_t vnum) override`
- ... and 10 more methods

---

## string_view >

**Full name:** `gs::ImmutableCsr< std::string_view >`

**Public Methods:**

- `ImmutableCsr(Table &table)`
- `~ImmutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge_with_index(vid_t src, vid_t dst, size_t data, timestamp_t ts...`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- `resize(vid_t vnum) override`
- ... and 14 more methods


---

## ImmutableCsrConstEdgeIter

**Full name:** `gs::ImmutableCsrConstEdgeIter`

**Public Methods:**

- `ImmutableCsrConstEdgeIter(const ImmutableNbrSlice< EDATA_T > &slice)`
- `~ImmutableCsrConstEdgeIter()=default`
- `get_neighbor() const override`
- `get_data() const override`
- `get_timestamp() const override`
- `next() override`
- `operator+=(size_t offset) override`
- `is_valid() const override`
- `size() const override`


---

## ImmutableGraphView

**Full name:** `gs::ImmutableGraphView`

**Public Methods:**

- `ImmutableGraphView(const ImmutableCsr< EDATA_T > &csr)`
- `get_edges(vid_t v) const`


---

## ImmutableNbr

**Full name:** `gs::ImmutableNbr`

**Public Methods:**

- `ImmutableNbr()=default`
- `ImmutableNbr(const ImmutableNbr &rhs)`
- `~ImmutableNbr()=default`
- `operator=(const ImmutableNbr &rhs)`
- `get_data() const`
- `get_neighbor() const`
- `set_data(const EDATA_T &val)`
- `set_neighbor(vid_t neighbor)`
- `exists() const`


---

## EmptyType >

**Full name:** `gs::ImmutableNbr< grape::EmptyType >`

**Public Methods:**

- `ImmutableNbr()=default`
- `ImmutableNbr(const ImmutableNbr &rhs)`
- `~ImmutableNbr()=default`
- `operator=(const ImmutableNbr &rhs)`
- `set_data(const grape::EmptyType &)`
- `set_neighbor(vid_t neighbor)`
- `get_data() const`
- `get_neighbor() const`
- `exists() const`


---

## ImmutableNbrSlice

**Full name:** `gs::ImmutableNbrSlice`

**Public Methods:**

- `ImmutableNbrSlice()=default`
- `ImmutableNbrSlice(const ImmutableNbrSlice &rhs)`
- `~ImmutableNbrSlice()=default`
- `set_size(int size)`
- `size() const`
- `set_begin(const_nbr_ptr_t ptr)`
- `begin() const`
- `end() const`
- `empty()`


---

## ColumnNbr

**Full name:** `gs::ImmutableNbrSlice< std::string_view >::ColumnNbr`

**Public Methods:**

- `ColumnNbr(const_nbr_ptr_t ptr, const StringColumn &column)`
- `get_neighbor() const`
- `get_data() const`
- `operator*() const`
- `operator->() const`
- `operator=(const ColumnNbr &nbr) const`
- `operator==(const ColumnNbr &nbr) const`
- `operator!=(const ColumnNbr &nbr) const`
- `operator++() const`
- `operator+=(size_t n) const`
- ... and 2 more methods


---

## InplaceTopNGenerator

**Full name:** `gs::InplaceTopNGenerator`

**Public Methods:**

- `InplaceTopNGenerator(size_t n)`
- `generate_indices(const std::vector< T > &input, std::vector< size_...`


---

## KNeighbors

**Full name:** `gs::KNeighbors`

**Public Methods:**

- `KNeighbors()`
- `Query(const NeugDBSession &sess, std::string label_name...`


---

## KNeighborsFactory

**Full name:** `gs::KNeighborsFactory`

**Public Methods:**

- `KNeighborsFactory()=default`
- `~KNeighborsFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## LDBCLongDateParser

**Full name:** `gs::LDBCLongDateParser`

**Public Methods:**

- `LDBCLongDateParser()=default`
- `~LDBCLongDateParser() override`
- `operator()(const char *s, size_t length, arrow::TimeUnit::ty...`
- `kind() const override`
- `format() const override`


---

## LDBCTimeStampParser

**Full name:** `gs::LDBCTimeStampParser`

**Public Methods:**

- `LDBCTimeStampParser()=default`
- `~LDBCTimeStampParser() override`
- `operator()(const char *s, size_t length, arrow::TimeUnit::ty...`
- `kind() const override`
- `format() const override`


---

## LFIndexer

**Full name:** `gs::LFIndexer`

**Public Methods:**

- `LFIndexer()`
- `LFIndexer(LFIndexer &&rhs)`
- `~LFIndexer()`
- `init(const PropertyType &type)`
- `build_empty_LFIndexer(const std::string &filename, const std::string &s...`
- `reserve(size_t size)`
- `rehash(size_t size)`
- `capacity() const`
- `size() const`
- `get_type() const`
- ... and 20 more methods


---

## LabelKey

**Full name:** `gs::LabelKey`

**Public Methods:**

- `LabelKey()=default`
- `LabelKey(label_data_type id)`


---

## LoaderFactory

**Full name:** `gs::LoaderFactory`

`LoaderFactory` is a factory class to create `IFragmentLoader`.

Support Using dynamically built library as plugin.

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `Register(const std::string &scheme_type, const std::string...`


---

## LoadingConfig

**Full name:** `gs::LoadingConfig`

**Public Methods:**

- `ParseFromYamlFile(const Schema &schema, const std::string &yaml_file)`
- `ParseFromYamlNode(const Schema &schema, const YAML::Node &yaml_node)`
- `LoadingConfig(const Schema &schema)`
- `LoadingConfig(const Schema &schema, const std::string &data_sou...`
- `AddVertexSources(const std::string &label, const std::string &file_path)`
- `AddEdgeSources(const std::string &src_label, const std::string &...`
- `SetScheme(const std::string &data_source)`
- `SetDelimiter(const char &delimiter)`
- `SetMethod(const BulkLoadMethod &method)`
- `GetScheme() const`
- ... and 24 more methods


---

## LocalWalParser

**Full name:** `gs::LocalWalParser`

**Public Methods:**

- `Make(const std::string &wal_dir)`
- `LocalWalParser(const std::string &wal_uri)`
- `~LocalWalParser()`
- `open(const std::string &wal_uri) override` - Open wals from a uri and parse the wal files
- `close() override`
- `last_ts() const override`
- `get_insert_wal(uint32_t ts) const override`
- `get_update_wals() const override` - Get all the update wal units


---

## LocalWalWriter

**Full name:** `gs::LocalWalWriter`

**Public Methods:**

- `Make(const std::string &wal_uri, int thread_id)`
- `LocalWalWriter(const std::string &wal_uri, int thread_id)`
- `~LocalWalWriter()`
- `open() override` - Open a wal file
- `close() override` - Close the wal writer
- `append(const char *data, size_t length) override` - Append data to the wal file
- `type() const override`


---

## MutableAdjlist

**Full name:** `gs::MutableAdjlist`

**Public Methods:**

- `MutableAdjlist()`
- `MutableAdjlist(const MutableAdjlist &rhs)`
- `~MutableAdjlist()`
- `init(nbr_t *ptr, int cap, int size)`
- `batch_put_edge(vid_t neighbor, const EDATA_T &data, timestamp_t ts=0)`
- `put_edge(vid_t neighbor, const EDATA_T &data, timestamp_t ...`
- `remove_nbrs(const std::unordered_set< vid_t > &nbrs)`
- `clear()`
- `get_edges() const`
- `get_edges_mut()`
- ... and 4 more methods


---

## MutableCsr

**Full name:** `gs::MutableCsr`

**Public Methods:**

- `MutableCsr()`
- `MutableCsr(MutableCsr< EDATA_T > &&rhs)`
- `~MutableCsr()`
- `operator=(const MutableCsr &)=delete`
- `operator=(MutableCsr &&other) noexcept`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `batch_delete_vertices(bool is_src, const std::vector< vid_t > &vids)`
- `batch_delete_edges(bool is_out, const std::vector< std::tuple< vid_t...`
- ... and 18 more methods

---

## string_view >

**Full name:** `gs::MutableCsr< std::string_view >`

**Public Methods:**

- `MutableCsr(Table &table)`
- `~MutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve) override`
- `batch_put_edge_with_index(vid_t src, vid_t dst, size_t data, timestamp_t ts) override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- `resize(vid_t vnum) override`
- `size() const override`
- ... and 18 more methods


---

## MutableCsrConstEdgeIter

**Full name:** `gs::MutableCsrConstEdgeIter`

**Public Methods:**

- `MutableCsrConstEdgeIter(const MutableNbrSlice< EDATA_T > &slice)`
- `~MutableCsrConstEdgeIter()=default`
- `get_neighbor() const override`
- `get_data() const override`
- `get_timestamp() const override`
- `next() override`
- `operator+=(size_t offset) override`
- `is_valid() const override`
- `size() const override`


---

## MutableCsrEdgeIter

**Full name:** `gs::MutableCsrEdgeIter`

**Public Methods:**

- `MutableCsrEdgeIter(MutableNbrSliceMut< EDATA_T > slice)`
- `~MutableCsrEdgeIter()=default`
- `get_neighbor() const override`
- `get_data() const override`
- `get_timestamp() const override`
- `set_data(const Any &value, timestamp_t ts, int32_t col_id=...`
- `operator+=(size_t offset) override`
- `next() override`
- `is_valid() const override`
- `size() const override`


---

## string_view >

**Full name:** `gs::MutableCsrEdgeIter< std::string_view >`

**Public Methods:**

- `MutableCsrEdgeIter(MutableNbrSliceMut< std::string_view > slice)`
- `~MutableCsrEdgeIter()=default`
- `get_neighbor() const override`
- `get_data() const override`
- `get_timestamp() const override`
- `set_data(const Any &value, timestamp_t ts, int32_t col_id=...`
- `get_index() const`
- `set_timestamp(timestamp_t ts)`
- `operator+=(size_t offset) override`
- `next() override`
- ... and 2 more methods


---

## MutableNbr

**Full name:** `gs::MutableNbr`

**Public Methods:**

- `MutableNbr()=default`
- `MutableNbr(const MutableNbr &rhs)`
- `~MutableNbr()=default`
- `operator=(const MutableNbr &rhs)`
- `get_data() const`
- `get_neighbor() const`
- `get_timestamp() const`
- `set_data(const EDATA_T &val, timestamp_t ts)`
- `set_neighbor(vid_t neighbor)`
- `set_timestamp(timestamp_t ts)`


---

## EmptyType >

**Full name:** `gs::MutableNbr< grape::EmptyType >`

**Public Methods:**

- `MutableNbr()=delete`
- `MutableNbr(const MutableNbr &rhs)`
- `~MutableNbr()=default`
- `operator=(const MutableNbr &rhs)`
- `set_data(const grape::EmptyType &, timestamp_t ts)`
- `set_neighbor(vid_t neighbor)`
- `set_timestamp(timestamp_t ts)`
- `get_data() const`
- `get_neighbor() const`
- `get_timestamp() const`


---

## MutableNbrSlice

**Full name:** `gs::MutableNbrSlice`

**Public Methods:**

- `MutableNbrSlice()`
- `MutableNbrSlice(const MutableNbrSlice &rhs)`
- `~MutableNbrSlice()=default`
- `set_size(int size)`
- `size() const`
- `set_begin(const_nbr_ptr_t ptr)`
- `begin() const`
- `end() const`
- `empty()`


---

## MutableColumnNbr

**Full name:** `gs::MutableNbrSlice< std::string_view >::MutableColumnNbr`

**Public Methods:**

- `MutableColumnNbr(const_nbr_ptr_t ptr, const StringColumn &column)`
- `get_neighbor() const`
- `get_data() const`
- `get_timestamp() const`
- `operator*() const`
- `operator->() const`
- `operator=(const MutableColumnNbr &nbr) const`
- `operator==(const MutableColumnNbr &nbr) const`
- `operator!=(const MutableColumnNbr &nbr) const`
- `operator++() const`
- ... and 3 more methods


---

## MutableNbrSliceMut

**Full name:** `gs::MutableNbrSliceMut`

**Public Methods:**

- `MutableNbrSliceMut()`
- `~MutableNbrSliceMut()=default`
- `set_size(int size)`
- `size() const`
- `set_begin(nbr_t *ptr)`
- `begin()`
- `end()`
- `empty()`


---

## MutableColumnNbr

**Full name:** `gs::MutableNbrSliceMut< std::string_view >::MutableColumnNbr`

**Public Methods:**

- `MutableColumnNbr(nbr_t *ptr, StringColumn &column)`
- `neighbor() const`
- `data()`
- `get_neighbor() const`
- `get_data() const`
- `get_timestamp() const`
- `get_index() const`
- `set_data(const std::string_view &sw, timestamp_t ts)`
- `set_neighbor(vid_t neighbor)`
- `set_timestamp(timestamp_t ts)`
- ... and 8 more methods


---

## NeugDBSession

**Full name:** `gs::NeugDBSession`

Database session for executing queries and managing transactions.

`NeugDBSession` provides a session-based interface for interacting with the NeuG database. Each session maintains its own transaction context and application state, enabling concurrent access while en...

**Public Methods:**

- `NeugDBSession(PropertyGraph &graph, AppManager &app_manager, st...`
- `~NeugDBSession()`
- `GetReadTransaction() const`
- `GetInsertTransaction()`
- `GetUpdateTransaction()`
- `GetCompactTransaction()`
- `BatchUpdate(UpdateBatch &batch)`
- `graph() const`
- `graph()`
- `schema() const`
- ... and 11 more methods


---

## ODPSFragmentLoader

**Full name:** `gs::ODPSFragmentLoader`

**Public Methods:**

- `ODPSFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `~ODPSFragmentLoader()`
- `LoadFragment() override`
- `Make(const std::string &work_dir, const Schema &schema...`


---

## ODPSReadClient

**Full name:** `gs::ODPSReadClient`

**Public Methods:**

- `ODPSReadClient()`
- `~ODPSReadClient()`
- `init()`
- `CreateReadSession(std::string *session_id, int *split_count, const ...`
- `ReadTable(const std::string &session_id, int split_count, c...`
- `GetArrowClient() const`


---

## ODPSStreamRecordBatchSupplier

**Full name:** `gs::ODPSStreamRecordBatchSupplier`

**Public Methods:**

- `ODPSStreamRecordBatchSupplier(label_t label_id, const std::string &file_path, c...`
- `GetNextBatch() override`


---

## ODPSTableRecordBatchSupplier

**Full name:** `gs::ODPSTableRecordBatchSupplier`

**Public Methods:**

- `ODPSTableRecordBatchSupplier(label_t label_id, const std::string &file_path, c...`
- `GetNextBatch() override`


---

## OPPrintInfo

**Full name:** `gs::OPPrintInfo`

**Public Methods:**

- `OPPrintInfo()`
- `~OPPrintInfo()=default`
- `toString() const`
- `copy() const`


---

## PageRank

**Full name:** `gs::PageRank`

**Public Methods:**

- `PageRank()`
- `Query(const NeugDBSession &sess, std::string src_vertex...`


---

## PageRankFactory

**Full name:** `gs::PageRankFactory`

**Public Methods:**

- `PageRankFactory()=default`
- `~PageRankFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## PropertyType

**Full name:** `gs::PropertyType`

**Public Methods:**

- `GetStringDefaultMaxLength()`
- `Empty()`
- `Bool()`
- `UInt8()`
- `UInt16()`
- `Int32()`
- `UInt32()`
- `Int64()`
- `UInt64()`
- `Float()`
- ... and 20 more methods


---

## QueryProcessor

**Full name:** `gs::QueryProcessor`

**Public Methods:**

- `QueryProcessor(NeugDB &db, int32_t max_num_threads, bool is_read...`
- `execute(const physical::PhysicalPlan &plan, int32_t num_threads=0)`


---

## ReadAppBase

**Full name:** `gs::ReadAppBase`

**Public Methods:**

- `mode() const override`
- `type() const override`
- `run(NeugDBSession &db, Decoder &input, Encoder &outpu...`
- `Query(const NeugDBSession &db, Decoder &input, Encoder &output)=0`


---

## edge_iterator

**Full name:** `gs::ReadTransaction::edge_iterator`

**Public Methods:**

- `edge_iterator(label_t neighbor_label, label_t edge_label, std::...`
- `~edge_iterator()`
- `GetData() const`
- `IsValid() const`
- `Next()`
- `GetNeighbor() const`
- `GetNeighborLabel() const`
- `GetEdgeLabel() const`


---

## vertex_iterator

**Full name:** `gs::ReadTransaction::vertex_iterator`

**Public Methods:**

- `vertex_iterator(label_t label, vid_t cur, vid_t num, timestamp_t ...`
- `~vertex_iterator()`
- `IsValid() const`
- `Next()`
- `Goto(vid_t target)`
- `GetId() const`
- `GetIndex() const`
- `GetField(int col_id) const`
- `FieldNum() const`


---

## Record

**Full name:** `gs::Record`

**Public Methods:**

- `Record()`
- `Record(size_t len)`
- `Record(const Record &other)`
- `Record(Record &&other)`
- `operator=(const Record &other)`
- `Record(const std::vector< Any > &vec)`
- `Record(const std::initializer_list< Any > &list)`
- `~Record()`
- `size() const`
- `operator[](size_t idx) const`
- ... and 2 more methods


---

## RecordLine

**Full name:** `gs::RecordLine`

We use this class to represent the returned result of the query.

No extra memory should

**Public Methods:**

- `RecordLine()=default`
- `RecordLine(std::vector< const results::Entry * > entries)`
- `ToString() const`
- `entries() const`

---

## RefColumnBase

**Full name:** `gs::RefColumnBase`

Create RefColumn for ease of usage for hqps.

**Public Methods:**

- `~RefColumnBase()`
- `get(size_t index) const =0`


---

## RemoteStorageDownloader

**Full name:** `gs::RemoteStorageDownloader`

**Public Methods:**

- `~RemoteStorageDownloader()=default`
- `Open()=0` - Open a remote storage for reading
- `Get(const std::string &remote_path, const std::string...` - Get a file from the remote storage
- `List(const std::string &remote_path, std::vector< std:...` - List all files in the remote storage
- `Close()=0` - Close the remote storage


---

## RemoteStorageUploader

**Full name:** `gs::RemoteStorageUploader`

**Public Methods:**

- `~RemoteStorageUploader()=default`
- `Open()=0` - Open a remote storage for writing
- `Put(const std::string &local_path, const std::string ...` - Put a local file to the remote storage
- `Delete(const std::string &remote_path)=0` - Delete a file from the remote storage
- `Close()=0` - Close the remote storage


---

## Result

**Full name:** `gs::Result`

**Public Methods:**

- `Result()`
- `Result(const ValueType &value)`
- `Result(ValueType &&value)`
- `Result(const Status &status, ValueType &&value)`
- `Result(const Status &status)`
- `Result(const Status &status, const ValueType &value)`
- `Result(StatusCode code, const std::string &error_msg, co...`
- `Result(StatusCode code, std::string &&error_msg, const V...`
- `ok() const noexcept`
- `status() const noexcept`
- ... and 2 more methods


---

## Schema

**Full name:** `gs::Schema`

**Public Methods:**

- `IsBuiltinPlugin(const std::string &plugin_name)`
- `GetCompatibleVersions()`
- `LoadFromYaml(const std::string &schema_config)`
- `LoadFromYamlNode(const YAML::Node &schema_node)`
- `DumpToYaml(const Schema &schema)`
- `Schema()`
- `~Schema()`
- `Clear()`
- `Empty() const`
- `add_vertex_label(const std::string &label, const std::vector< Prop...`
- ... and 82 more methods


---

## SessionLocalContext

**Full name:** `gs::SessionLocalContext`

**Public Methods:**

- `SessionLocalContext(PropertyGraph &graph_, AppManager &app_manager, s...`
- `~SessionLocalContext()`


---

## SharedLibraryAppFactory

**Full name:** `gs::SharedLibraryAppFactory`

**Public Methods:**

- `SharedLibraryAppFactory(const std::string &path)`
- `~SharedLibraryAppFactory()`
- `CreateApp(const NeugDB &db) override`


---

## ShortestPathAmongThree

**Full name:** `gs::ShortestPathAmongThree`

**Public Methods:**

- `ShortestPathAmongThree()`
- `Query(const NeugDBSession &sess, std::string label_name...`


---

## ShortestPathAmongThreeFactory

**Full name:** `gs::ShortestPathAmongThreeFactory`

**Public Methods:**

- `ShortestPathAmongThreeFactory()=default`
- `~ShortestPathAmongThreeFactory()=default`
- `CreateApp(const NeugDB &db) override`


---

## SingleGraphView

**Full name:** `gs::SingleGraphView`

**Public Methods:**

- `SingleGraphView(const SingleMutableCsr< EDATA_T > &csr, timestamp...`
- `exist(vid_t v) const`
- `get_edge(vid_t v) const`


---

## string_view >

**Full name:** `gs::SingleGraphView< std::string_view >`

**Public Methods:**

- `SingleGraphView(const SingleMutableCsr< std::string_view > &csr, ...`
- `exist(vid_t v) const`
- `get_edge(vid_t v) const`


---

## SingleImmutableCsr

**Full name:** `gs::SingleImmutableCsr`

**Public Methods:**

- `SingleImmutableCsr()`
- `~SingleImmutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `batch_sort_by_edge_data(timestamp_t ts) override`
- `unsorted_since() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 13 more methods


---

## string_view >

**Full name:** `gs::SingleImmutableCsr< std::string_view >`

**Public Methods:**

- `SingleImmutableCsr(Table &table)`
- `~SingleImmutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge_with_index(vid_t src, vid_t dst, size_t data, timestamp_t ts) override`
- `batch_sort_by_edge_data(timestamp_t ts) override`
- `unsorted_since() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 15 more methods


---

## SingleImmutableGraphView

**Full name:** `gs::SingleImmutableGraphView`

**Public Methods:**

- `SingleImmutableGraphView(const SingleImmutableCsr< EDATA_T > &csr)`
- `exist(vid_t v) const`
- `get_edge(vid_t v) const`


---

## string_view >

**Full name:** `gs::SingleImmutableGraphView< std::string_view >`

**Public Methods:**

- `SingleImmutableGraphView(const SingleImmutableCsr< std::string_view > &csr)`
- `exist(vid_t v) const`
- `get_edge(vid_t v) const`


---

## SingleMutableCsr

**Full name:** `gs::SingleMutableCsr`

**Public Methods:**

- `SingleMutableCsr()`
- `~SingleMutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `batch_sort_by_edge_data(timestamp_t ts) override`
- `unsorted_since() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 14 more methods


---

## string_view >

**Full name:** `gs::SingleMutableCsr< std::string_view >`

**Public Methods:**

- `SingleMutableCsr(Table &table)`
- `~SingleMutableCsr()`
- `batch_init(const std::string &name, const std::string &work_...`
- `batch_init_in_memory(const std::vector< int > &degree, double reserve_...`
- `batch_put_edge_with_index(vid_t src, vid_t dst, size_t data, timestamp_t ts) override`
- `batch_sort_by_edge_data(timestamp_t ts) override`
- `unsorted_since() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `dump(const std::string &name, const std::string &new_s...`
- ... and 16 more methods


---

## Status

**Full name:** `gs::Status`

**Public Methods:**

- `Status() noexcept`
- `Status(StatusCode error_code) noexcept`
- `Status(StatusCode error_code, std::string &&error_msg) noexcept`
- `Status(StatusCode error_code, const std::string &error_m...`
- `ok() const`
- `error_message() const`
- `error_code() const`
- `ToString() const`
- `OK()`
- `RuntimeError(const std::string &error_msg)`
- ... and 2 more methods


---

## StringMapColumn

**Full name:** `gs::StringMapColumn`

**Public Methods:**

- `StringMapColumn(StorageStrategy strategy)`
- `~StringMapColumn()`
- `copy_to_tmp(const std::string &cur_path, const std::string &t...`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name) override`
- `open_with_hugepages(const std::string &name, bool force) override`
- `dump(const std::string &filename) override`
- `close() override`
- `size() const override`
- `resize(size_t size) override`
- ... and 12 more methods


---

## StringPtr

**Full name:** `gs::StringPtr`

**Public Methods:**

- `StringPtr()`
- `StringPtr(const std::string &str)`
- `StringPtr(const StringPtr &other)`
- `StringPtr(StringPtr &&other)`
- `operator=(const StringPtr &other)`
- `~StringPtr()`
- `operator*() const`


---

## StringViewVector

**Full name:** `gs::StringViewVector`

**Public Methods:**

- `StringViewVector()`
- `~StringViewVector()`
- `push_back(const std::string_view &val)`
- `emplace_back(const std::string_view &val)`
- `size() const`
- `operator[](size_t index) const`
- `content_buffer()`
- `content_buffer() const`
- `offset_buffer()`
- `offset_buffer() const`
- ... and 2 more methods


---

## SupplierWrapperWithFirstBatch

**Full name:** `gs::SupplierWrapperWithFirstBatch`

**Public Methods:**

- `SupplierWrapperWithFirstBatch(const std::vector< std::shared_ptr< IRecordBatchS...`
- `SupplierWrapperWithFirstBatch(const std::vector< std::shared_ptr< IRecordBatchS...`
- `GetNextBatch() override`


---

## TPVersionManager

**Full name:** `gs::TPVersionManager`

`TPVersionManager` implements the version manager for Transactional Processing (TP) workloads.

It supports multiple concurrent read and insert transactions, each receiving the same initial timestamp. Update transactions are exclusive and will wait for all ongoing read and insert transactions to...

**Public Methods:**

- `TPVersionManager()`
- `~TPVersionManager()`
- `init_ts(uint32_t ts, int thread_num) override`
- `clear() override`
- `acquire_read_timestamp() override`
- `release_read_timestamp() override`
- `acquire_insert_timestamp() override`
- `release_insert_timestamp(uint32_t ts) override`
- `acquire_update_timestamp() override`
- `release_update_timestamp(uint32_t ts) override`
- ... and 1 more methods


---

## Table

**Full name:** `gs::Table`

**Public Methods:**

- `Table()`
- `~Table()`
- `init(const std::string &name, const std::string &work_...`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name, const std::string &snaps...`
- `open_with_hugepages(const std::string &name, const std::string &snaps...`
- `copy_to_tmp(const std::string &name, const std::string &snaps...`
- `dump(const std::string &name, const std::string &snapshot_dir)`
- `reset_header(const std::vector< std::string > &col_name)`
- `add_column(const std::string &col_name, const PropertyType &...`
- ... and 29 more methods


---

## TopNAscCmp

**Full name:** `gs::TopNAscCmp`

**Public Methods:**

- `operator()(const elem_t &lhs, const elem_t &rhs) const`
- `operator()(const T &lhs, const T &rhs) const`


---

## TopNDescCmp

**Full name:** `gs::TopNDescCmp`

**Public Methods:**

- `operator()(const elem_t &lhs, const elem_t &rhs) const`
- `operator()(const T &lhs, const T &rhs) const`


---

## TopNGenerator

**Full name:** `gs::TopNGenerator`

**Public Methods:**

- `TopNGenerator(size_t n)`
- `push(const T &val, size_t idx)`
- `generate_indices(std::vector< size_t > &indices)`
- `generate_pairs(std::vector< T > &values, std::vector< size_t > &indices)`


---

## TopNUnit

**Full name:** `gs::TopNUnit`

**Public Methods:**

- `TopNUnit(const T &val_, size_t idx_)`


---

## TransactionManager

**Full name:** `gs::TransactionManager`

`TransactionManager` manages multiple NeugDBSessions, each associated with a thread.

It provides methods to obtain different types of transactions (read, insert, update, compact) for each session. It also handles the ingestion of write-ahead logs (WALs) during initialization.

**Public Methods:**

- `TransactionManager(std::shared_ptr< AppManager > app_manager, std::s...`
- `~TransactionManager()`
- `Clear()`
- `getExecutedQueryNum() const`
- `SessionNum() const`
- `GetAlloctedMemorySize(int thread_id) const`
- `GetEvalDuration(int thread_id) const`
- `GetQueryNum(int thread_id) const`
- `GetReadTransaction(int thread_id=0) const`
- `GetInsertTransaction(int thread_id=0)`
- ... and 7 more methods


---

## TypeConverter

**Full name:** `gs::TypeConverter`


---

## TypeConverter< Date >

**Full name:** `gs::TypeConverter< Date >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< DateTime >

**Full name:** `gs::TypeConverter< DateTime >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< Interval >

**Full name:** `gs::TypeConverter< Interval >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< TimeStamp >

**Full name:** `gs::TypeConverter< TimeStamp >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< bool >

**Full name:** `gs::TypeConverter< bool >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< double >

**Full name:** `gs::TypeConverter< double >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< float >

**Full name:** `gs::TypeConverter< float >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< int32_t >

**Full name:** `gs::TypeConverter< int32_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< int64_t >

**Full name:** `gs::TypeConverter< int64_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## string >

**Full name:** `gs::TypeConverter< std::string >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## string_view >

**Full name:** `gs::TypeConverter< std::string_view >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< uint32_t >

**Full name:** `gs::TypeConverter< uint32_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< uint64_t >

**Full name:** `gs::TypeConverter< uint64_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypedColumn

**Full name:** `gs::TypedColumn`

**Public Methods:**

- `TypedColumn(StorageStrategy strategy)`
- `~TypedColumn()`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name) override`
- `open_with_hugepages(const std::string &name, bool force) override`
- `close() override`
- `copy_to_tmp(const std::string &cur_path, const std::string &t...`
- `dump(const std::string &filename) override`
- `size() const override`
- `resize(size_t size) override`
- ... and 12 more methods


---

## EmptyType >

**Full name:** `gs::TypedColumn< grape::EmptyType >`

**Public Methods:**

- `TypedColumn(StorageStrategy strategy)`
- `~TypedColumn()`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name) override`
- `open_with_hugepages(const std::string &name, bool force) override`
- `dump(const std::string &filename) override`
- `copy_to_tmp(const std::string &cur_path, const std::string &t...`
- `close() override`
- `size() const override`
- `resize(size_t size) override`
- ... and 12 more methods


---

## TypedCsrBase

**Full name:** `gs::TypedCsrBase`

**Public Methods:**

- `batch_put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`


---

## string_view >

**Full name:** `gs::TypedCsrBase< std::string_view >`

**Public Methods:**

- `batch_put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t ts=0)=0`
- `put_edge_with_index(vid_t src, vid_t dst, size_t index, timestamp_t t...`
- `batch_put_edge(vid_t src, vid_t dst, const std::string_view &dat...`
- `put_edge(vid_t src, vid_t dst, const std::string_view &dat...`


---

## TypedImmutableCsrBase

**Full name:** `gs::TypedImmutableCsrBase`

**Public Methods:**

- `get_edges(vid_t v) const =0`


---

## TypedMutableCsrBase

**Full name:** `gs::TypedMutableCsrBase`

**Public Methods:**

- `get_edges(vid_t v) const =0`


---

## TypedRefColumn

**Full name:** `gs::TypedRefColumn`

**Public Methods:**

- `TypedRefColumn(const mmap_array< T > &buffer, StorageStrategy strategy)`
- `TypedRefColumn(const TypedColumn< T > &column)`
- `~TypedRefColumn()`
- `get_view(size_t index) const`
- `size() const`
- `get(size_t index) const override`


---

## TypedRefColumn< GlobalId >

**Full name:** `gs::TypedRefColumn< GlobalId >`

**Public Methods:**

- `TypedRefColumn(label_t label_key)`
- `~TypedRefColumn()`
- `get_view(size_t index) const`
- `get(size_t index) const override`
- `size() const`


---

## TypedRefColumn< LabelKey >

**Full name:** `gs::TypedRefColumn< LabelKey >`

**Public Methods:**

- `TypedRefColumn(LabelKey label_key)`
- `~TypedRefColumn()`
- `get_view(size_t index) const`
- `get(size_t index) const override`
- `size() const`


---

## UninitializedUtils

**Full name:** `gs::UninitializedUtils`

**Public Methods:**

- `copy(T *new_buffer, T *old_buffer, size_t len)`


---

## UpdateBatch

**Full name:** `gs::UpdateBatch`

**Public Methods:**

- `UpdateBatch()`
- `UpdateBatch(const UpdateBatch &other)=delete`
- `~UpdateBatch()`
- `clear()`
- `AddVertex(label_t label, Any &&oid, std::vector< Any > &&props)`
- `AddEdge(label_t src_label, Any &&src, label_t dst_label, ...`
- `GetUpdateVertices() const`
- `GetUpdateEdges() const`
- `GetArc()`


---

## edge_iterator

**Full name:** `gs::UpdateTransaction::edge_iterator`

**Public Methods:**

- `edge_iterator(bool dir, label_t label, vid_t v, label_t neighbo...`
- `~edge_iterator()`
- `GetData() const`
- `SetData(const Any &value)`
- `IsValid() const`
- `Next()`
- `Forward(size_t offset)`
- `GetNeighbor() const`
- `GetNeighborLabel() const`
- `GetEdgeLabel() const`


---

## vertex_iterator

**Full name:** `gs::UpdateTransaction::vertex_iterator`

**Public Methods:**

- `vertex_iterator(label_t label, vid_t cur, vid_t &num, timestamp_t...`
- `~vertex_iterator()`
- `IsValid() const`
- `Next()`
- `Goto(vid_t target)`
- `GetId() const`
- `GetIndex() const`
- `GetField(int col_id) const`
- `SetField(int col_id, const Any &value)`


---

## UpdateWalUnit

**Full name:** `gs::UpdateWalUnit`


---

## VarcharExtraInfo

**Full name:** `gs::VarcharExtraInfo`

**Public Methods:**

- `VarcharExtraInfo(uint64_t maxLength)`
- `~VarcharExtraInfo() override=default`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`


---

## VertexSet

**Full name:** `gs::VertexSet`

**Public Methods:**

- `VertexSet(vid_t size, const mmap_array< timestamp_t > &vert...`
- `~VertexSet()`
- `begin() const`
- `end() const`
- `size() const`


---

## iterator

**Full name:** `gs::VertexSet::iterator`

**Public Methods:**

- `iterator(vid_t v, const mmap_array< timestamp_t > &vertex_...`
- `~iterator()`
- `operator*() const`
- `operator++()`
- `operator==(const iterator &rhs) const`
- `operator!=(const iterator &rhs) const`


---

## VertexTable

**Full name:** `gs::VertexTable`

**Public Methods:**

- `VertexTable(std::string v_label_name, PropertyType pk_type, c...`
- `VertexTable(VertexTable &&other)`
- `VertexTable(const VertexTable &)=delete`
- `Open(const std::string &work_dir, int memory_level, bo...`
- `Dump(const std::string &target_dir)`
- `Close()`
- `Reserve(size_t cap)`
- `is_dropped() const`
- `get_index(const Any &oid, vid_t &lid, timestamp_t ts=MAX_TI...`
- `GetOid(vid_t lid, timestamp_t ts=MAX_TIMESTAMP) const`
- ... and 22 more methods


---

## WalContentUnit

**Full name:** `gs::WalContentUnit`


---

## WalHeader

**Full name:** `gs::WalHeader`


---

## WalParserFactory

**Full name:** `gs::WalParserFactory`

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateWalParser(const std::string &wal_uri)`
- `RegisterWalParser(const std::string &wal_parser_type, wal_parser_in...`


---

## WalWriterFactory

**Full name:** `gs::WalWriterFactory`

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateDummyWalWriter()`
- `CreateWalWriter(const std::string &wal_uri, int32_t thread_id)`
- `RegisterWalWriter(const std::string &wal_writer_type, wal_writer_in...`


---

## WriteAppBase

**Full name:** `gs::WriteAppBase`

**Public Methods:**

- `mode() const override`
- `type() const override`
- `run(NeugDBSession &db, Decoder &input, Encoder &outpu...`
- `Query(NeugDBSession &db, Decoder &input, Encoder &output)=0`


---

## string_view, INDEX_T >

**Full name:** `gs::_move_data< std::string_view, INDEX_T >`

**Public Methods:**

- `operator()(const key_buffer_t &input, ColumnBase &col, size_t size)`


---

## AdjQueryGraph

**Full name:** `gs::binder::AdjQueryGraph`

**Public Methods:**

- `AdjQueryGraph(const QueryGraphCollection *queryGraph)`
- `getAdjEdges(std::shared_ptr< NodeExpression > node) const`
- `getAdjNode(std::shared_ptr< NodeExpression > node, std::shar...`
- `getNode(std::shared_ptr< Expression > name) const`


---

## AggregateFunctionExpression

**Full name:** `gs::binder::AggregateFunctionExpression`

**Public Methods:**

- `AggregateFunctionExpression(function::AggregateFunction function, std::unique...`
- `getFunction() const`
- `getBindData() const`
- `isDistinct() const`
- `toStringInternal() const override`
- `getUniqueName(const std::string &functionName, const expression...`


---

## AttachInfo

**Full name:** `gs::binder::AttachInfo`

**Public Methods:**

- `AttachInfo(std::string dbPath, std::string dbAlias, std::str...`


---

## AttachOption

**Full name:** `gs::binder::AttachOption`


---

## Binder

**Full name:** `gs::binder::Binder`

**Public Methods:**

- `Binder(main::ClientContext *clientContext)`
- `bind(const parser::Statement &statement)`
- `setInputParameters(std::unordered_map< std::string, std::shared_ptr<...`
- `getParameterMap()`
- `bindExportTableData(ExportedTableData &tableData, const catalog::Tabl...`
- `createVariable(const std::string &name, const common::LogicalTyp...`
- `createInvisibleVariable(const std::string &name, const common::LogicalTyp...`
- `createVariables(const std::vector< std::string > &names, const st...`
- `createInvisibleVariables(const std::vector< std::string > &names, const st...`
- `bindWhereExpression(const parser::ParsedExpression &parsedExpression)`
- ... and 104 more methods


---

## BinderScope

**Full name:** `gs::binder::BinderScope`

**Public Methods:**

- `BinderScope()=default`
- `EXPLICIT_COPY_DEFAULT_MOVE(BinderScope)`
- `empty() const`
- `contains(const std::string &varName) const`
- `getExpression(const std::string &varName) const`
- `getExpressions() const`
- `addExpression(const std::string &varName, std::shared_ptr< Expr...`
- `replaceExpression(const std::string &oldName, const std::string &ne...`
- `memorizeTableEntries(const std::string &name, std::vector< catalog::Ta...`
- `hasMemorizedTableIDs(const std::string &name) const`
- ... and 6 more methods


---

## BoundAlter

**Full name:** `gs::binder::BoundAlter`

**Public Methods:**

- `BoundAlter(BoundAlterInfo info)`
- `getInfo() const`


---

## BoundAlterInfo

**Full name:** `gs::binder::BoundAlterInfo`

**Public Methods:**

- `BoundAlterInfo(common::AlterType alterType, std::string tableNam...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundAlterInfo)`
- `toString() const`


---

## BoundAttachDatabase

**Full name:** `gs::binder::BoundAttachDatabase`

**Public Methods:**

- `BoundAttachDatabase(binder::AttachInfo attachInfo)`
- `getAttachInfo() const`


---

## BoundBaseScanSource

**Full name:** `gs::binder::BoundBaseScanSource`

**Public Methods:**

- `BoundBaseScanSource(common::ScanSourceType type)`
- `~BoundBaseScanSource()=default`
- `getColumns()=0`
- `getWarningColumns() const`
- `getIgnoreErrorsOption() const`
- `getNumWarningDataColumns() const`
- `copy() const =0`
- `constCast() const`


---

## BoundCopyFrom

**Full name:** `gs::binder::BoundCopyFrom`

**Public Methods:**

- `BoundCopyFrom(BoundCopyFromInfo info)`
- `getInfo() const`


---

## BoundCopyFromInfo

**Full name:** `gs::binder::BoundCopyFromInfo`

**Public Methods:**

- `BoundCopyFromInfo(catalog::TableCatalogEntry *tableEntry, std::uniq...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo)`
- `getSourceColumns() const`
- `getNumWarningColumns() const`
- `getIgnoreErrorsOption() const`


---

## BoundCopyTo

**Full name:** `gs::binder::BoundCopyTo`

**Public Methods:**

- `BoundCopyTo(std::unique_ptr< function::ExportFuncBindData > b...`
- `getBindData() const`
- `getExportFunc() const`
- `getRegularQuery() const`


---

## BoundCreateMacro

**Full name:** `gs::binder::BoundCreateMacro`

**Public Methods:**

- `BoundCreateMacro(std::string macroName, std::unique_ptr< function:...`
- `getMacroName() const`
- `getMacro() const`


---

## BoundCreateSequence

**Full name:** `gs::binder::BoundCreateSequence`

**Public Methods:**

- `BoundCreateSequence(BoundCreateSequenceInfo info)`
- `getInfo() const`


---

## BoundCreateSequenceInfo

**Full name:** `gs::binder::BoundCreateSequenceInfo`

**Public Methods:**

- `BoundCreateSequenceInfo(std::string sequenceName, int64_t startWith, int6...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateSequenceInfo)`


---

## BoundCreateTable

**Full name:** `gs::binder::BoundCreateTable`

**Public Methods:**

- `BoundCreateTable(BoundCreateTableInfo info)`
- `getInfo() const`


---

## BoundCreateTableInfo

**Full name:** `gs::binder::BoundCreateTableInfo`

**Public Methods:**

- `BoundCreateTableInfo()=default`
- `BoundCreateTableInfo(catalog::CatalogEntryType type, std::string table...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo)`
- `toString() const`


---

## BoundCreateType

**Full name:** `gs::binder::BoundCreateType`

**Public Methods:**

- `BoundCreateType(std::string name, common::LogicalType type)`
- `getName() const`
- `getType() const`


---

## BoundDatabaseStatement

**Full name:** `gs::binder::BoundDatabaseStatement`

**Public Methods:**

- `BoundDatabaseStatement(common::StatementType statementType, std::string dbName)`
- `getDBName() const`


---

## BoundDeleteClause

**Full name:** `gs::binder::BoundDeleteClause`

**Public Methods:**

- `BoundDeleteClause()`
- `addInfo(BoundDeleteInfo info)`
- `hasNodeInfo() const`
- `getNodeInfos() const`
- `hasRelInfo() const`
- `getRelInfos() const`


---

## BoundDeleteInfo

**Full name:** `gs::binder::BoundDeleteInfo`

**Public Methods:**

- `BoundDeleteInfo(common::DeleteNodeType deleteType, common::TableT...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundDeleteInfo)`
- `toString() const`


---

## BoundDetachDatabase

**Full name:** `gs::binder::BoundDetachDatabase`

**Public Methods:**

- `BoundDetachDatabase(std::string dbName)`


---

## BoundDrop

**Full name:** `gs::binder::BoundDrop`

**Public Methods:**

- `BoundDrop(parser::DropInfo dropInfo)`
- `getDropInfo() const`


---

## BoundExplain

**Full name:** `gs::binder::BoundExplain`

**Public Methods:**

- `BoundExplain(std::unique_ptr< BoundStatement > statementToExpl...`
- `getStatementToExplain() const`
- `getExplainType() const`


---

## BoundExportDatabase

**Full name:** `gs::binder::BoundExportDatabase`

**Public Methods:**

- `BoundExportDatabase(std::string filePath, common::FileTypeInfo fileTy...`
- `getFilePath() const`
- `getFileType() const`
- `getExportOptions() const`
- `getBoundFileInfo() const`
- `getExportData() const`


---

## BoundExtensionStatement

**Full name:** `gs::binder::BoundExtensionStatement`

**Public Methods:**

- `BoundExtensionStatement(std::unique_ptr< ExtensionAuxInfo > info)`
- `getAuxInfo() const`


---

## BoundExtraAddPropertyInfo

**Full name:** `gs::binder::BoundExtraAddPropertyInfo`

**Public Methods:**

- `BoundExtraAddPropertyInfo(const PropertyDefinition &definition, std::shared...`
- `BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo &other)`
- `copy() const override`


---

## BoundExtraAlterInfo

**Full name:** `gs::binder::BoundExtraAlterInfo`

**Public Methods:**

- `~BoundExtraAlterInfo()=default`
- `constPtrCast() const`
- `constCast() const`
- `cast()`
- `copy() const =0`


---

## BoundExtraCommentInfo

**Full name:** `gs::binder::BoundExtraCommentInfo`

**Public Methods:**

- `BoundExtraCommentInfo(std::string comment)`
- `BoundExtraCommentInfo(const BoundExtraCommentInfo &other)`
- `copy() const override`


---

## BoundExtraCreateCatalogEntryInfo

**Full name:** `gs::binder::BoundExtraCreateCatalogEntryInfo`

**Public Methods:**

- `~BoundExtraCreateCatalogEntryInfo()=default`
- `constPtrCast() const`
- `ptrCast()`
- `copy() const =0`


---

## BoundExtraCreateNodeTableInfo

**Full name:** `gs::binder::BoundExtraCreateNodeTableInfo`

**Public Methods:**

- `BoundExtraCreateNodeTableInfo(std::string primaryKeyName, std::vector< Property...`
- `BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo &other)`
- `copy() const override`


---

## BoundExtraCreateRelTableGroupInfo

**Full name:** `gs::binder::BoundExtraCreateRelTableGroupInfo`

**Public Methods:**

- `BoundExtraCreateRelTableGroupInfo(std::vector< BoundCreateTableInfo > infos)`
- `BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo &other)`
- `copy() const override`


---

## BoundExtraCreateRelTableInfo

**Full name:** `gs::binder::BoundExtraCreateRelTableInfo`

**Public Methods:**

- `BoundExtraCreateRelTableInfo(common::table_id_t srcTableID, common::table_id_t...`
- `BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity, common::...`
- `BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity, common::...`
- `BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo &other)`
- `copy() const override`


---

## BoundExtraCreateTableInfo

**Full name:** `gs::binder::BoundExtraCreateTableInfo`

**Public Methods:**

- `BoundExtraCreateTableInfo(std::vector< PropertyDefinition > propertyDefinitions)`
- `BoundExtraCreateTableInfo(const BoundExtraCreateTableInfo &other)`
- `operator=(const BoundExtraCreateTableInfo &)=delete`
- `copy() const override`


---

## BoundExtraDropPropertyInfo

**Full name:** `gs::binder::BoundExtraDropPropertyInfo`

**Public Methods:**

- `BoundExtraDropPropertyInfo(std::string propertyName)`
- `BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo &other)`
- `copy() const override`


---

## BoundExtraRenamePropertyInfo

**Full name:** `gs::binder::BoundExtraRenamePropertyInfo`

**Public Methods:**

- `BoundExtraRenamePropertyInfo(std::string newName, std::string oldName)`
- `BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo &other)`
- `copy() const override`


---

## BoundExtraRenameTableInfo

**Full name:** `gs::binder::BoundExtraRenameTableInfo`

**Public Methods:**

- `BoundExtraRenameTableInfo(std::string newName)`
- `BoundExtraRenameTableInfo(const BoundExtraRenameTableInfo &other)`
- `copy() const override`


---

## BoundGraphPattern

**Full name:** `gs::binder::BoundGraphPattern`

**Public Methods:**

- `BoundGraphPattern()=default`
- `DELETE_COPY_DEFAULT_MOVE(BoundGraphPattern)`


---

## BoundImportDatabase

**Full name:** `gs::binder::BoundImportDatabase`

**Public Methods:**

- `BoundImportDatabase(std::string filePath, std::string query, std::str...`
- `getFilePath() const`
- `getQuery() const`
- `getIndexQuery() const`


---

## BoundInsertClause

**Full name:** `gs::binder::BoundInsertClause`

**Public Methods:**

- `BoundInsertClause(std::vector< BoundInsertInfo > infos)`
- `getInfos() const`
- `hasNodeInfo() const`
- `getNodeInfos() const`
- `hasRelInfo() const`
- `getRelInfos() const`


---

## BoundInsertInfo

**Full name:** `gs::binder::BoundInsertInfo`

**Public Methods:**

- `BoundInsertInfo(common::TableType tableType, std::shared_ptr< Exp...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundInsertInfo)`


---

## BoundJoinHintNode

**Full name:** `gs::binder::BoundJoinHintNode`

**Public Methods:**

- `BoundJoinHintNode()=default`
- `BoundJoinHintNode(std::shared_ptr< Expression > nodeOrRel)`
- `addChild(std::shared_ptr< BoundJoinHintNode > child)`
- `isLeaf() const`
- `isBinary() const`
- `isMultiWay() const`
- `isExpandE() const`
- `isGetV() const`


---

## BoundLoadFrom

**Full name:** `gs::binder::BoundLoadFrom`

**Public Methods:**

- `BoundLoadFrom(BoundTableScanInfo info)`
- `getInfo() const`


---

## BoundMatchClause

**Full name:** `gs::binder::BoundMatchClause`

**Public Methods:**

- `BoundMatchClause(QueryGraphCollection collection, common::MatchCla...`
- `getQueryGraphCollectionUnsafe()`
- `getQueryGraphCollection() const`
- `getMatchClauseType() const`
- `setHint(std::shared_ptr< BoundJoinHintNode > root)`
- `hasHint() const`
- `getHint() const`


---

## BoundMergeClause

**Full name:** `gs::binder::BoundMergeClause`

**Public Methods:**

- `BoundMergeClause(expression_vector columnDataExprs, std::shared_pt...`
- `getColumnDataExprs() const`
- `getExistenceMark() const`
- `getDistinctMark() const`
- `getQueryGraphCollection() const`
- `hasPredicate() const`
- `getPredicate() const`
- `getInsertInfosRef() const`
- `getOnMatchSetInfosRef() const`
- `getOnCreateSetInfosRef() const`
- ... and 14 more methods


---

## BoundProjectionBody

**Full name:** `gs::binder::BoundProjectionBody`

**Public Methods:**

- `BoundProjectionBody(bool distinct)`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundProjectionBody)`
- `isDistinct() const`
- `setProjectionExpressions(expression_vector expressions)`
- `getProjectionExpressions() const`
- `setGroupByExpressions(expression_vector expressions)`
- `getGroupByExpressions() const`
- `setAggregateExpressions(expression_vector expressions)`
- `hasAggregateExpressions() const`
- `getAggregateExpressions() const`
- ... and 11 more methods


---

## BoundQueryScanSource

**Full name:** `gs::binder::BoundQueryScanSource`

**Public Methods:**

- `BoundQueryScanSource(std::shared_ptr< BoundStatement > statement, Boun...`
- `BoundQueryScanSource(const BoundQueryScanSource &other)=default`
- `getIgnoreErrorsOption() const override`
- `getColumns() override`
- `copy() const override`


---

## BoundQueryScanSourceInfo

**Full name:** `gs::binder::BoundQueryScanSourceInfo`

**Public Methods:**

- `BoundQueryScanSourceInfo(common::case_insensitive_map_t< common::Value > options)`


---

## BoundReadingClause

**Full name:** `gs::binder::BoundReadingClause`

**Public Methods:**

- `BoundReadingClause(common::ClauseType clauseType)`
- `DELETE_COPY_DEFAULT_MOVE(BoundReadingClause)`
- `~BoundReadingClause()=default`
- `getClauseType() const`
- `setPredicate(std::shared_ptr< Expression > predicate_)`
- `hasPredicate() const`
- `getPredicate() const`
- `getConjunctivePredicates() const`
- `cast()`
- `constCast() const`
- ... and 2 more methods


---

## BoundRegularQuery

**Full name:** `gs::binder::BoundRegularQuery`

**Public Methods:**

- `BoundRegularQuery(std::vector< bool > isUnionAll, BoundStatementRes...`
- `addSingleQuery(NormalizedSingleQuery singleQuery)`
- `getNumSingleQueries() const`
- `getSingleQueryUnsafe(common::idx_t idx)`
- `getSingleQuery(common::idx_t idx) const`
- `getIsUnionAll(common::idx_t idx) const`
- `setPreQueryPart(std::vector< NormalizedQueryPart > preQueryParts)`
- `setPostSingleQuery(NormalizedSingleQuery postSingleQuery)`
- `setPreQueryExprs(binder::expression_vector preQueryExprs)`
- `getPreQueryExprs() const`
- ... and 5 more methods


---

## BoundReturnClause

**Full name:** `gs::binder::BoundReturnClause`

**Public Methods:**

- `BoundReturnClause(BoundProjectionBody projectionBody)`
- `BoundReturnClause(BoundProjectionBody projectionBody, BoundStatemen...`
- `DELETE_COPY_DEFAULT_MOVE(BoundReturnClause)`
- `~BoundReturnClause()=default`
- `getProjectionBody() const`
- `getStatementResult() const`


---

## BoundSetClause

**Full name:** `gs::binder::BoundSetClause`

**Public Methods:**

- `BoundSetClause()`
- `addInfo(BoundSetPropertyInfo info)`
- `getInfos() const`
- `hasNodeInfo() const`
- `getNodeInfos() const`
- `hasRelInfo() const`
- `getRelInfos() const`


---

## BoundSetPropertyInfo

**Full name:** `gs::binder::BoundSetPropertyInfo`

**Public Methods:**

- `BoundSetPropertyInfo(common::TableType tableType, std::shared_ptr< Exp...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundSetPropertyInfo)`


---

## BoundStandaloneCall

**Full name:** `gs::binder::BoundStandaloneCall`

**Public Methods:**

- `BoundStandaloneCall(const main::Option *option, std::shared_ptr< Expr...`
- `getOption() const`
- `getOptionValue() const`


---

## BoundStandaloneCallFunction

**Full name:** `gs::binder::BoundStandaloneCallFunction`

**Public Methods:**

- `BoundStandaloneCallFunction(BoundTableScanInfo info)`
- `getTableFunction() const`
- `getBindData() const`


---

## BoundStatement

**Full name:** `gs::binder::BoundStatement`

**Public Methods:**

- `BoundStatement(common::StatementType statementType, BoundStateme...`
- `DELETE_COPY_DEFAULT_MOVE(BoundStatement)`
- `~BoundStatement()=default`
- `getStatementType() const`
- `getStatementResult() const`
- `getStatementResultUnsafe()`
- `constCast() const`
- `cast()`


---

## BoundStatementResult

**Full name:** `gs::binder::BoundStatementResult`

**Public Methods:**

- `BoundStatementResult()=default`
- `BoundStatementResult(expression_vector columns, std::vector< std::stri...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundStatementResult)`
- `addColumn(const std::string &columnName, std::shared_ptr< E...`
- `getColumns() const`
- `getColumnNames() const`
- `getColumnTypes() const`
- `getSingleColumnExpr() const`
- `createEmptyResult()`
- `createSingleStringColumnResult(const std::string &columnName="result")`


---

## BoundStatementRewriter

**Full name:** `gs::binder::BoundStatementRewriter`

**Public Methods:**

- `rewrite(BoundStatement &boundStatement, main::ClientConte...`


---

## BoundStatementVisitor

**Full name:** `gs::binder::BoundStatementVisitor`

**Public Methods:**

- `BoundStatementVisitor()=default`
- `~BoundStatementVisitor()=default`
- `visit(const BoundStatement &statement)`
- `visitUnsafe(BoundStatement &statement)`
- `visitSingleQuery(const NormalizedSingleQuery &singleQuery)`
- `visitQueryPart(const NormalizedQueryPart &queryPart)`


---

## BoundTableFunctionCall

**Full name:** `gs::binder::BoundTableFunctionCall`

**Public Methods:**

- `BoundTableFunctionCall(BoundTableScanInfo info)`
- `getTableFunc() const`
- `getBindData() const`


---

## BoundTableScanInfo

**Full name:** `gs::binder::BoundTableScanInfo`

**Public Methods:**

- `BoundTableScanInfo(function::TableFunction func, std::unique_ptr< fu...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundTableScanInfo)`


---

## BoundTableScanSource

**Full name:** `gs::binder::BoundTableScanSource`

**Public Methods:**

- `BoundTableScanSource(common::ScanSourceType type, BoundTableScanInfo info)`
- `BoundTableScanSource(const BoundTableScanSource &other)`
- `getColumns() override`
- `getWarningColumns() const override`
- `getIgnoreErrorsOption() const override`
- `getNumWarningDataColumns() const override`
- `copy() const override`


---

## BoundTransactionStatement

**Full name:** `gs::binder::BoundTransactionStatement`

**Public Methods:**

- `BoundTransactionStatement(transaction::TransactionAction transactionAction)`
- `getTransactionAction() const`


---

## BoundUnwindClause

**Full name:** `gs::binder::BoundUnwindClause`

**Public Methods:**

- `BoundUnwindClause(std::shared_ptr< Expression > inExpr, std::shared...`
- `getInExpr() const`
- `getOutExpr() const`
- `getIDExpr() const`


---

## BoundUpdatingClause

**Full name:** `gs::binder::BoundUpdatingClause`

**Public Methods:**

- `BoundUpdatingClause(common::ClauseType clauseType)`
- `~BoundUpdatingClause()=default`
- `getClauseType() const`
- `cast() const`
- `constCast() const`


---

## BoundUseDatabase

**Full name:** `gs::binder::BoundUseDatabase`

**Public Methods:**

- `BoundUseDatabase(std::string dbName)`


---

## BoundWithClause

**Full name:** `gs::binder::BoundWithClause`

**Public Methods:**

- `BoundWithClause(BoundProjectionBody projectionBody)`
- `setWhereExpression(std::shared_ptr< Expression > expression)`
- `hasWhereExpression() const`
- `getWhereExpression() const`


---

## CaseAlternative

**Full name:** `gs::binder::CaseAlternative`

**Public Methods:**

- `CaseAlternative(std::shared_ptr< Expression > whenExpression, std...`


---

## CaseExpression

**Full name:** `gs::binder::CaseExpression`

**Public Methods:**

- `CaseExpression(common::LogicalType dataType, std::shared_ptr< Ex...`
- `addCaseAlternative(std::shared_ptr< Expression > when, std::shared_p...`
- `getNumCaseAlternatives() const`
- `getCaseAlternative(common::idx_t idx) const`
- `getElseExpression() const`
- `toStringInternal() const override`


---

## ColumnDefinition

**Full name:** `gs::binder::ColumnDefinition`

**Public Methods:**

- `ColumnDefinition()=default`
- `ColumnDefinition(std::string name, common::LogicalType type)`
- `EXPLICIT_COPY_DEFAULT_MOVE(ColumnDefinition)`


---

## CommonPatReuseRewriter

**Full name:** `gs::binder::CommonPatReuseRewriter`

This rewriter is used to set join order heuristically for each subquery in union.

For example, in the following query: MATCH (a:person) CALL (a) { subquery1 UNION ALL subquery2 }, it will generate join order for both subquery1 and subquery2. Each subquery is bound to the common exp...

**Public Methods:**

- `CommonPatReuseRewriter(main::ClientContext *clientContext)`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## ConfidentialStatementAnalyzer

**Full name:** `gs::binder::ConfidentialStatementAnalyzer`

**Public Methods:**

- `isConfidential() const`


---

## ConstantExpressionVisitor

**Full name:** `gs::binder::ConstantExpressionVisitor`

**Public Methods:**

- `needFold(const Expression &expr)`
- `isConstant(const Expression &expr)`


---

## DefaultTypeSolver

**Full name:** `gs::binder::DefaultTypeSolver`


---

## DependentVarNameCollector

**Full name:** `gs::binder::DependentVarNameCollector`

**Public Methods:**

- `getVarNames() const`


---

## ExportedTableData

**Full name:** `gs::binder::ExportedTableData`

**Public Methods:**

- `getColumnTypesRef() const`
- `getRegularQuery() const`


---

## Expression

**Full name:** `gs::binder::Expression`

**Public Methods:**

- `Expression(common::ExpressionType expressionType, common::Lo...`
- `Expression(common::ExpressionType expressionType, common::Lo...`
- `Expression(common::ExpressionType expressionType, common::Lo...`
- `Expression(common::ExpressionType expressionType, common::Lo...`
- `DELETE_COPY_DEFAULT_MOVE(Expression)`
- `~Expression()=default`
- `setUniqueName(const std::string &name)`
- `getUniqueName() const`
- `cast(const common::LogicalType &type)`
- `getDataType() const`
- ... and 15 more methods


---

## ExpressionBinder

**Full name:** `gs::binder::ExpressionBinder`

**Public Methods:**

- `ExpressionBinder(Binder *queryBinder, main::ClientContext *context)`
- `bindExpression(const parser::ParsedExpression &parsedExpression)`
- `foldExpression(const std::shared_ptr< Expression > &expression) const`
- `bindBooleanExpression(const parser::ParsedExpression &parsedExpression)`
- `bindBooleanExpression(common::ExpressionType expressionType, const expr...`
- `combineBooleanExpressions(common::ExpressionType expressionType, std::share...`
- `bindComparisonExpression(const parser::ParsedExpression &parsedExpression)`
- `bindComparisonExpression(common::ExpressionType expressionType, const expr...`
- `createEqualityComparisonExpression(std::shared_ptr< Expression > left, std::shared_p...`
- `bindNullOperatorExpression(const parser::ParsedExpression &parsedExpression)`
- ... and 32 more methods


---

## ExpressionBinderConfig

**Full name:** `gs::binder::ExpressionBinderConfig`


---

## ExpressionChildrenCollector

**Full name:** `gs::binder::ExpressionChildrenCollector`

**Public Methods:**

- `collectChildren(const Expression &expression)`


---

## ExpressionEquality

**Full name:** `gs::binder::ExpressionEquality`

**Public Methods:**

- `operator()(const std::shared_ptr< Expression > &left, const ...`


---

## ExpressionHasher

**Full name:** `gs::binder::ExpressionHasher`

**Public Methods:**

- `operator()(const std::shared_ptr< Expression > &expression) const`


---

## ExpressionUtil

**Full name:** `gs::binder::ExpressionUtil`

**Public Methods:**

- `getExpressionsWithDataType(const expression_vector &expressions, common::Log...`
- `find(const Expression *target, const expression_vector...`
- `toString(const expression_vector &expressions)`
- `toStringOrdered(const expression_vector &expressions)`
- `toString(const std::vector< expression_pair > &expressionPairs)`
- `toString(const expression_pair &expressionPair)`
- `getUniqueName(const expression_vector &expressions)`
- `excludeExpression(const expression_vector &exprs, const Expression ...`
- `excludeExpressions(const expression_vector &expressions, const expre...`
- `getDataTypes(const expression_vector &expressions)`
- ... and 20 more methods


---

## ExpressionVisitor

**Full name:** `gs::binder::ExpressionVisitor`

**Public Methods:**

- `~ExpressionVisitor()=default`
- `visit(std::shared_ptr< Expression > expr)`
- `isRandom(const Expression &expression)`


---

## ExtraBoundCopyFromInfo

**Full name:** `gs::binder::ExtraBoundCopyFromInfo`

**Public Methods:**

- `~ExtraBoundCopyFromInfo()=default`
- `copy() const =0`
- `constCast() const`


---

## ExtraBoundCopyRelInfo

**Full name:** `gs::binder::ExtraBoundCopyRelInfo`

**Public Methods:**

- `ExtraBoundCopyRelInfo(std::vector< common::idx_t > internalIDColumnIndi...`
- `ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo &other)=default`
- `copy() const override`


---

## IndexLookupInfo

**Full name:** `gs::binder::IndexLookupInfo`

**Public Methods:**

- `IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr< ...`
- `IndexLookupInfo(const IndexLookupInfo &other)=default`


---

## LambdaExpression

**Full name:** `gs::binder::LambdaExpression`

**Public Methods:**

- `LambdaExpression(std::unique_ptr< parser::ParsedExpression > parse...`
- `cast(const common::LogicalType &type_) override`
- `getParsedLambdaExpr() const`
- `setFunctionExpr(std::shared_ptr< Expression > expr)`
- `getFunctionExpr() const`
- `toStringInternal() const override`


---

## LiteralExpression

**Full name:** `gs::binder::LiteralExpression`

**Public Methods:**

- `LiteralExpression(common::Value value, const std::string &uniqueName)`
- `isNull() const`
- `cast(const common::LogicalType &type) override`
- `getValue() const`
- `toStringInternal() const override`
- `copy() const override`


---

## MatchClausePatternLabelRewriter

**Full name:** `gs::binder::MatchClausePatternLabelRewriter`

**Public Methods:**

- `MatchClausePatternLabelRewriter(const main::ClientContext &clientContext)`
- `visitMatchUnsafe(BoundReadingClause &readingClause) override`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## NodeExpression

**Full name:** `gs::binder::NodeExpression`

**Public Methods:**

- `NodeExpression(common::LogicalType dataType, std::string uniqueN...`
- `~NodeExpression() override`
- `setInternalID(std::unique_ptr< Expression > expression)`
- `getInternalID() const`
- `getPrimaryKey(common::table_id_t tableID) const`


---

## NodeOrRelExpression

**Full name:** `gs::binder::NodeOrRelExpression`

**Public Methods:**

- `NodeOrRelExpression(common::LogicalType dataType, std::string uniqueN...`
- `setExtraTypeInfo(std::unique_ptr< common::ExtraTypeInfo > info)`
- `getVariableName() const`
- `isEmpty() const`
- `isMultiLabeled() const`
- `getNumEntries() const`
- `getTableIDs() const`
- `getTableIDsSet() const`
- `getEntries() const`
- `setEntries(std::vector< catalog::TableCatalogEntry * > entries_)`
- ... and 14 more methods


---

## NormalizedQueryPart

**Full name:** `gs::binder::NormalizedQueryPart`

**Public Methods:**

- `NormalizedQueryPart()=default`
- `DELETE_COPY_DEFAULT_MOVE(NormalizedQueryPart)`
- `addReadingClause(std::unique_ptr< BoundReadingClause > boundReadingClause)`
- `hasReadingClause() const`
- `getNumReadingClause() const`
- `getReadingClause(uint32_t idx) const`
- `addUpdatingClause(std::unique_ptr< BoundUpdatingClause > boundUpdatingClause)`
- `hasUpdatingClause() const`
- `getNumUpdatingClause() const`
- `getUpdatingClause(uint32_t idx) const`
- ... and 7 more methods


---

## NormalizedQueryPartMatchRewriter

**Full name:** `gs::binder::NormalizedQueryPartMatchRewriter`

**Public Methods:**

- `NormalizedQueryPartMatchRewriter(main::ClientContext *clientContext)`


---

## NormalizedSingleQuery

**Full name:** `gs::binder::NormalizedSingleQuery`

**Public Methods:**

- `NormalizedSingleQuery()=default`
- `DELETE_COPY_DEFAULT_MOVE(NormalizedSingleQuery)`
- `appendQueryPart(NormalizedQueryPart queryPart)`
- `insertQueryPart(common::idx_t idx, NormalizedQueryPart queryPart)`
- `getNumQueryParts() const`
- `getQueryPartUnsafe(common::idx_t idx)`
- `getQueryPart(common::idx_t idx) const`
- `setStatementResult(BoundStatementResult result)`
- `getStatementResult() const`


---

## ParameterExpression

**Full name:** `gs::binder::ParameterExpression`

**Public Methods:**

- `ParameterExpression(const std::string &parameterName, common::Value value)`
- `cast(const common::LogicalType &type) override`
- `getValue() const`


---

## PathExpression

**Full name:** `gs::binder::PathExpression`

**Public Methods:**

- `PathExpression(common::LogicalType dataType, std::string uniqueN...`
- `getVariableName() const`
- `getNodeType() const`
- `getRelType() const`
- `toStringInternal() const override`


---

## PropertyCollector

**Full name:** `gs::binder::PropertyCollector`

**Public Methods:**

- `getProperties() const`
- `visitSingleQuerySkipNodeRel(const NormalizedSingleQuery &singleQuery)`


---

## PropertyDefinition

**Full name:** `gs::binder::PropertyDefinition`

**Public Methods:**

- `PropertyDefinition()=default`
- `PropertyDefinition(ColumnDefinition columnDefinition, std::unique_pt...`
- `EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinition)`
- `getName() const`
- `getType() const`
- `getDefaultExpressionName() const`
- `rename(const std::string &newName)`
- `serialize(common::Serializer &serializer) const`
- `PropertyDefinition(ColumnDefinition columnDefinition)`
- `deserialize(common::Deserializer &deserializer)`


---

## PropertyExprCollector

**Full name:** `gs::binder::PropertyExprCollector`

**Public Methods:**

- `getPropertyExprs() const`


---

## PropertyExpression

**Full name:** `gs::binder::PropertyExpression`

**Public Methods:**

- `PropertyExpression(common::LogicalType dataType, std::string propert...`
- `PropertyExpression(const PropertyExpression &other)`
- `isPrimaryKey() const`
- `isPrimaryKey(common::table_id_t tableID) const`
- `getPropertyName() const`
- `getVariableName() const`
- `getRawVariableName() const`
- `setUniqueVarName(const std::string &uniqueVarName)`
- `hasProperty(common::table_id_t tableID) const`
- `getColumnID(const catalog::TableCatalogEntry &entry) const`
- ... and 6 more methods


---

## QueryGraph

**Full name:** `gs::binder::QueryGraph`

**Public Methods:**

- `QueryGraph()=default`
- `QueryGraph(const QueryGraph &other)`
- `EXPLICIT_COPY_DEFAULT_MOVE(QueryGraph)`
- `isEmpty() const`
- `getAllPatterns() const`
- `getNumQueryNodes() const`
- `containsQueryNode(const std::string &queryNodeName) const`
- `getQueryNodes() const`
- `getQueryNode(const std::string &queryNodeName) const`
- `getQueryNodes(const std::vector< common::idx_t > &nodePoses) const`
- ... and 14 more methods


---

## QueryGraphCollection

**Full name:** `gs::binder::QueryGraphCollection`

**Public Methods:**

- `QueryGraphCollection()=default`
- `DELETE_COPY_DEFAULT_MOVE(QueryGraphCollection)`
- `merge(const QueryGraphCollection &other)`
- `addAndMergeQueryGraphIfConnected(QueryGraph queryGraphToAdd)`
- `finalize()`
- `getNumQueryGraphs() const`
- `getQueryGraphUnsafe(common::idx_t idx)`
- `getQueryGraph(common::idx_t idx) const`
- `contains(const std::string &name) const`
- `getQueryNodes() const`
- ... and 1 more methods


---

## QueryGraphLabelAnalyzer

**Full name:** `gs::binder::QueryGraphLabelAnalyzer`

**Public Methods:**

- `QueryGraphLabelAnalyzer(const main::ClientContext &clientContext, bool th...`
- `pruneLabel(QueryGraph &graph) const`


---

## RecursiveInfo

**Full name:** `gs::binder::RecursiveInfo`


---

## RelExpression

**Full name:** `gs::binder::RelExpression`

**Public Methods:**

- `RelExpression(common::LogicalType dataType, std::string uniqueN...`
- `isRecursive() const`
- `isBoundByMultiLabeledNode() const`
- `getSrcNode() const`
- `getSrcNodeName() const`
- `setDstNode(std::shared_ptr< NodeExpression > node)`
- `getDstNode() const`
- `getDstNodeName() const`
- `setLeftNode(std::shared_ptr< NodeExpression > node)`
- `getLeftNode() const`
- ... and 14 more methods


---

## RenameDependentVar

**Full name:** `gs::binder::RenameDependentVar`

**Public Methods:**

- `RenameDependentVar(const std::string &newVarName)`


---

## ScalarFunctionExpression

**Full name:** `gs::binder::ScalarFunctionExpression`

**Public Methods:**

- `ScalarFunctionExpression(common::ExpressionType expressionType, std::uniqu...`
- `getFunction() const`
- `getBindData() const`
- `toStringInternal() const override`
- `getUniqueName(const std::string &functionName, const expression...`


---

## SingleLabelPropertyInfo

**Full name:** `gs::binder::SingleLabelPropertyInfo`

**Public Methods:**

- `SingleLabelPropertyInfo(bool exists, bool isPrimaryKey)`
- `EXPLICIT_COPY_DEFAULT_MOVE(SingleLabelPropertyInfo)`


---

## SubqueryExprCollector

**Full name:** `gs::binder::SubqueryExprCollector`

**Public Methods:**

- `hasSubquery() const`
- `getSubqueryExprs() const`


---

## SubqueryExpression

**Full name:** `gs::binder::SubqueryExpression`

**Public Methods:**

- `SubqueryExpression(common::SubqueryType subqueryType, common::Logica...`
- `getSubqueryType() const`
- `getQueryGraphCollection() const`
- `setWhereExpression(std::shared_ptr< Expression > expression)`
- `hasWhereExpression() const`
- `getWhereExpression() const`
- `getPredicatesSplitOnAnd() const`
- `setCountStarExpr(std::shared_ptr< Expression > expr)`
- `getCountStarExpr() const`
- `setProjectionExpr(std::shared_ptr< Expression > expr)`
- ... and 4 more methods


---

## SubqueryGraph

**Full name:** `gs::binder::SubqueryGraph`

**Public Methods:**

- `SubqueryGraph(const QueryGraph &queryGraph)`
- `addQueryNode(common::idx_t nodePos)`
- `addQueryRel(common::idx_t relPos)`
- `addSubqueryGraph(const SubqueryGraph &other)`
- `getNumQueryRels() const`
- `getTotalNumVariables() const`
- `isSingleRel() const`
- `containAllVariables(const std::unordered_set< std::string > &variables) const`
- `getNodeNbrPositions() const`
- `getRelNbrPositions() const`
- ... and 5 more methods


---

## SubqueryGraphHasher

**Full name:** `gs::binder::SubqueryGraphHasher`

**Public Methods:**

- `operator()(const SubqueryGraph &key) const`


---

## UnionProjectionRewriter

**Full name:** `gs::binder::UnionProjectionRewriter`

This rewriter is used to project outer variables (defined by call) before each subquery in union.

For example, in the following query: MATCH (a:person) CALL (a) { subquery1 UNION ALL subquery2 }, it will project the outer variable `a` before subquery1 and subquery2.

**Public Methods:**

- `UnionProjectionRewriter(main::ClientContext *clientContext)`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## VariableExpression

**Full name:** `gs::binder::VariableExpression`

**Public Methods:**

- `VariableExpression(common::LogicalType dataType, std::string uniqueN...`
- `getVariableName() const`
- `cast(const common::LogicalType &type) override`
- `toStringInternal() const override`
- `copy() const override`


---

## WithClauseProjectionRewriter

**Full name:** `gs::binder::WithClauseProjectionRewriter`

**Public Methods:**

- `visitSingleQueryUnsafe(NormalizedSingleQuery &singleQuery) override`


---

## Catalog

**Full name:** `gs::catalog::Catalog`

**Public Methods:**

- `Catalog()`
- `Catalog(const std::string &directory, common::VirtualFile...`
- `~Catalog()=default`
- `containsTable(const transaction::Transaction *transaction, cons...`
- `containsTable(const transaction::Transaction *transaction, comm...`
- `getTableCatalogEntry(const transaction::Transaction *transaction, cons...`
- `getTableCatalogEntry(const transaction::Transaction *transaction, comm...`
- `getNodeTableEntries(const transaction::Transaction *transaction, bool...`
- `getRelTableEntries(const transaction::Transaction *transaction, bool...`
- `getTableEntries(const transaction::Transaction *transaction, bool...`
- ... and 43 more methods


---

## CatalogEntry

**Full name:** `gs::catalog::CatalogEntry`

**Public Methods:**

- `CatalogEntry()`
- `CatalogEntry(CatalogEntryType type, std::string name)`
- `DELETE_COPY_DEFAULT_MOVE(CatalogEntry)`
- `~CatalogEntry()=default`
- `getType() const`
- `rename(std::string name_)`
- `getName() const`
- `getTimestamp() const`
- `setTimestamp(common::transaction_t timestamp_)`
- `isDeleted() const`
- ... and 17 more methods


---

## CatalogEntryTypeUtils

**Full name:** `gs::catalog::CatalogEntryTypeUtils`

**Public Methods:**

- `toString(CatalogEntryType type)`


---

## CatalogSet

**Full name:** `gs::catalog::CatalogSet`

**Public Methods:**

- `CatalogSet()=default`
- `CatalogSet(bool isInternal)`
- `containsEntry(const transaction::Transaction *transaction, cons...`
- `getEntry(const transaction::Transaction *transaction, cons...`
- `createEntry(transaction::Transaction *transaction, std::uniqu...`
- `createEntryUnlocked(transaction::Transaction *transaction, std::uniqu...`
- `acquireExclusiveLock()`
- `dropEntry(transaction::Transaction *transaction, const std:...`
- `alterTableEntry(transaction::Transaction *transaction, const bind...`
- `alterRelGroupEntry(transaction::Transaction *transaction, const bind...`
- ... and 5 more methods


---

## DummyCatalogEntry

**Full name:** `gs::catalog::DummyCatalogEntry`

**Public Methods:**

- `DummyCatalogEntry(std::string name, common::oid_t oid)`
- `serialize(common::Serializer &) const override`
- `toCypher(const ToCypherInfo &) const override`


---

## FunctionCatalogEntry

**Full name:** `gs::catalog::FunctionCatalogEntry`

**Public Methods:**

- `FunctionCatalogEntry()=default`
- `FunctionCatalogEntry(CatalogEntryType entryType, std::string name, fun...`
- `getFunctionSet() const`
- `serialize(common::Serializer &) const override`


---

## FunctionEntryTypeUtils

**Full name:** `gs::catalog::FunctionEntryTypeUtils`

**Public Methods:**

- `toString(CatalogEntryType type)`


---

## GCatalog

**Full name:** `gs::catalog::GCatalog`

**Public Methods:**

- `GCatalog(const std::filesystem::path &schemaPath)`
- `GCatalog(const std::string &schemaData)`
- `GCatalog(const YAML::Node &schema)`
- `~GCatalog()=default`


---

## GRelTableCatalogEntry

**Full name:** `gs::catalog::GRelTableCatalogEntry`

**Public Methods:**

- `GRelTableCatalogEntry()=default`
- `GRelTableCatalogEntry(std::string name, common::RelMultiplicity srcMult...`
- `getLabelId() const`


---

## IndexAuxInfo

**Full name:** `gs::catalog::IndexAuxInfo`

**Public Methods:**

- `~IndexAuxInfo()=default`
- `serialize() const`
- `copy()=0`
- `cast()`
- `cast() const`
- `toCypher(const IndexCatalogEntry &indexEntry, const ToCyph...`
- `getTableEntryToExport(const main::ClientContext *) const`


---

## IndexCatalogEntry

**Full name:** `gs::catalog::IndexCatalogEntry`

**Public Methods:**

- `getInternalIndexName(common::table_id_t tableID, std::string indexName)`
- `deserialize(common::Deserializer &deserializer)`
- `IndexCatalogEntry(std::string type, common::table_id_t tableID, std...`
- `getIndexType() const`
- `getTableID() const`
- `getIndexName() const`
- `getPropertyIDs() const`
- `serialize(common::Serializer &serializer) const override`
- `toCypher(const ToCypherInfo &info) const override`
- `copy() const`
- ... and 6 more methods


---

## IndexToCypherInfo

**Full name:** `gs::catalog::IndexToCypherInfo`

**Public Methods:**

- `IndexToCypherInfo(const main::ClientContext *context, const common:...`


---

## NodeTableCatalogEntry

**Full name:** `gs::catalog::NodeTableCatalogEntry`

**Public Methods:**

- `NodeTableCatalogEntry()=default`
- `NodeTableCatalogEntry(std::string name, std::string primaryKeyName)`
- `isParent(common::table_id_t) override`
- `getTableType() const override`
- `getPrimaryKeyName() const`
- `getPrimaryKeyID() const`
- `getPrimaryKeyDefinition() const`
- `serialize(common::Serializer &serializer) const override`
- `copy() const override`
- `toCypher(const ToCypherInfo &info) const override`
- ... and 1 more methods


---

## PropertyDefinitionCollection

**Full name:** `gs::catalog::PropertyDefinitionCollection`

**Public Methods:**

- `PropertyDefinitionCollection()`
- `PropertyDefinitionCollection(common::column_id_t nextColumnID)`
- `EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinitionCollection)`
- `size() const`
- `contains(const std::string &name) const`
- `getDefinitions() const`
- `getDefinition(const std::string &name) const`
- `getDefinition(common::idx_t idx) const`
- `getMaxColumnID() const`
- `getColumnID(const std::string &name) const`
- ... and 9 more methods


---

## RelGroupCatalogEntry

**Full name:** `gs::catalog::RelGroupCatalogEntry`

**Public Methods:**

- `RelGroupCatalogEntry()=default`
- `RelGroupCatalogEntry(std::string tableName, std::vector< common::table...`
- `getNumRelTables() const`
- `getRelTableIDs() const`
- `alter(common::transaction_t timestamp, const binder::Bo...`
- `isParent(common::table_id_t tableID) const`
- `serialize(common::Serializer &serializer) const override`
- `toCypher(const ToCypherInfo &info) const override`
- `getBoundCreateTableInfo(transaction::Transaction *transaction, const Cata...`
- `setComment(std::string newComment)`
- ... and 4 more methods


---

## RelGroupToCypherInfo

**Full name:** `gs::catalog::RelGroupToCypherInfo`

**Public Methods:**

- `RelGroupToCypherInfo(const main::ClientContext *context)`


---

## RelTableCatalogEntry

**Full name:** `gs::catalog::RelTableCatalogEntry`

**Public Methods:**

- `RelTableCatalogEntry()`
- `RelTableCatalogEntry(std::string name, common::RelMultiplicity srcMult...`
- `isParent(common::table_id_t tableID) override`
- `hasParentRelGroup(const Catalog *catalog, const transaction::Transa...`
- `getParentRelGroup(const Catalog *catalog, const transaction::Transa...`
- `getTableType() const override`
- `getSrcTableID() const`
- `getDstTableID() const`
- `isSingleMultiplicity(common::RelDataDirection direction) const`
- `getMultiplicity(common::RelDataDirection direction) const`
- ... and 9 more methods


---

## RelTableToCypherInfo

**Full name:** `gs::catalog::RelTableToCypherInfo`

**Public Methods:**

- `RelTableToCypherInfo(const main::ClientContext *context)`


---

## ScalarMacroCatalogEntry

**Full name:** `gs::catalog::ScalarMacroCatalogEntry`

**Public Methods:**

- `ScalarMacroCatalogEntry()=default`
- `ScalarMacroCatalogEntry(std::string name, std::unique_ptr< function::Scal...`
- `getMacroFunction() const`
- `serialize(common::Serializer &serializer) const override`
- `toCypher(const ToCypherInfo &info) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## SequenceCatalogEntry

**Full name:** `gs::catalog::SequenceCatalogEntry`

**Public Methods:**

- `SequenceCatalogEntry()`
- `SequenceCatalogEntry(const binder::BoundCreateSequenceInfo &sequenceInfo)`
- `getSequenceData()`
- `currVal()`
- `nextKVal(transaction::Transaction *transaction, const uint...`
- `nextKVal(transaction::Transaction *transaction, const uint...`
- `rollbackVal(const uint64_t &usageCount, const int64_t &currVal)`
- `serialize(common::Serializer &serializer) const override`
- `toCypher(const ToCypherInfo &info) const override`
- `getBoundCreateSequenceInfo(bool isInternal) const`
- ... and 2 more methods


---

## SequenceData

**Full name:** `gs::catalog::SequenceData`

**Public Methods:**

- `SequenceData()=default`
- `SequenceData(const binder::BoundCreateSequenceInfo &info)`


---

## SequenceRollbackData

**Full name:** `gs::catalog::SequenceRollbackData`


---

## TableCatalogEntry

**Full name:** `gs::catalog::TableCatalogEntry`

**Public Methods:**

- `TableCatalogEntry()=default`
- `TableCatalogEntry(CatalogEntryType catalogType, std::string name)`
- `operator=(const TableCatalogEntry &)=delete`
- `getTableID() const`
- `alter(common::transaction_t timestamp, const binder::Bo...`
- `isParent(common::table_id_t)`
- `getTableType() const =0`
- `getComment() const`
- `setComment(std::string newComment)`
- `getScanFunction()`
- ... and 20 more methods


---

## TableCatalogEntryEquality

**Full name:** `gs::catalog::TableCatalogEntryEquality`

**Public Methods:**

- `operator()(TableCatalogEntry *left, TableCatalogEntry *right) const`


---

## TableCatalogEntryHasher

**Full name:** `gs::catalog::TableCatalogEntryHasher`

**Public Methods:**

- `operator()(TableCatalogEntry *entry) const`


---

## ToCypherInfo

**Full name:** `gs::catalog::ToCypherInfo`

**Public Methods:**

- `~ToCypherInfo()=default`
- `constCast() const`


---

## TypeCatalogEntry

**Full name:** `gs::catalog::TypeCatalogEntry`

**Public Methods:**

- `TypeCatalogEntry()=default`
- `TypeCatalogEntry(std::string name, common::LogicalType type)`
- `getLogicalType() const`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## AccumulateTypeUtil

**Full name:** `gs::common::AccumulateTypeUtil`

**Public Methods:**

- `toString(AccumulateType type)`


---

## ArrayType

**Full name:** `gs::common::ArrayType`

**Public Methods:**

- `getChildType(const LogicalType &type)`
- `getNumElements(const LogicalType &type)`


---

## ArrayTypeInfo

**Full name:** `gs::common::ArrayTypeInfo`

**Public Methods:**

- `ArrayTypeInfo()`
- `ArrayTypeInfo(LogicalType childType, uint64_t numElements)`
- `getNumElements() const`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## ArrowBuffer

**Full name:** `gs::common::ArrowBuffer`

**Public Methods:**

- `ArrowBuffer()`
- `~ArrowBuffer()`
- `ArrowBuffer(const ArrowBuffer &other)=delete`
- `operator=(const ArrowBuffer &)=delete`
- `ArrowBuffer(ArrowBuffer &&other) noexcept` - enable move constructors
- `operator=(ArrowBuffer &&other) noexcept`
- `reserve(uint64_t bytes)`
- `resize(uint64_t bytes)`
- `resize(uint64_t bytes, uint8_t value)`
- `size()`
- ... and 1 more methods


---

## ArrowColumnAuxiliaryBuffer

**Full name:** `gs::common::ArrowColumnAuxiliaryBuffer`


---

## ArrowConverter

**Full name:** `gs::common::ArrowConverter`

**Public Methods:**

- `toArrowSchema(const std::vector< LogicalType > &dataTypes, cons...`
- `fromArrowSchema(const ArrowSchema *schema)`
- `fromArrowArray(const ArrowSchema *schema, const ArrowArray *arra...`
- `fromArrowArray(const ArrowSchema *schema, const ArrowArray *arra...`


---

## ArrowNullMaskTree

**Full name:** `gs::common::ArrowNullMaskTree`

**Public Methods:**

- `ArrowNullMaskTree(const ArrowSchema *schema, const ArrowArray *arra...`
- `copyToValueVector(ValueVector *vec, uint64_t dstOffset, uint64_t count)`
- `isNull(int64_t idx)`
- `getChild(int idx)`
- `getDictionary()`
- `offsetBy(int64_t offset)`


---

## ArrowRowBatch

**Full name:** `gs::common::ArrowRowBatch`

**Public Methods:**

- `ArrowRowBatch(std::vector< LogicalType > types, std::int64_t capacity)`
- `append(main::QueryResult &queryResult, std::int64_t chunkSize)` - Append a data chunk to the underlying arrow array


---

## ArrowSchemaHolder

**Full name:** `gs::common::ArrowSchemaHolder`


---

## ArrowVector

**Full name:** `gs::common::ArrowVector`


---

## AuxiliaryBuffer

**Full name:** `gs::common::AuxiliaryBuffer`

**Public Methods:**

- `~AuxiliaryBuffer()=default`
- `cast()`
- `constCast() const`


---

## AuxiliaryBufferFactory

**Full name:** `gs::common::AuxiliaryBufferFactory`

**Public Methods:**

- `getAuxiliaryBuffer(LogicalType &type, storage::MemoryManager *memoryManager)`


---

## BinaryData

**Full name:** `gs::common::BinaryData`


---

## BitmaskUtils

**Full name:** `gs::common::BitmaskUtils`

**Public Methods:**

- `all1sMaskForLeastSignificantBits(uint32_t numBits)`
- `all1sMaskForLeastSignificantBits(uint32_t numBits)`


---

## Blob

**Full name:** `gs::common::Blob`

**Public Methods:**

- `toString(const uint8_t *value, uint64_t len)`
- `toString(const blob_t &blob)`
- `getBlobSize(const neug_string_t &blob)`
- `fromString(const char *str, uint64_t length, uint8_t *resultBuffer)`
- `getValue(const blob_t &data)`
- `getValue(char *data)`


---

## BlobVector

**Full name:** `gs::common::BlobVector`

**Public Methods:**

- `addBlob(ValueVector *vector, uint32_t pos, const char *da...`
- `addBlob(ValueVector *vector, uint32_t pos, const uint8_t ...`


---

## BufferBlock

**Full name:** `gs::common::BufferBlock`

**Public Methods:**

- `BufferBlock(std::unique_ptr< storage::MemoryBuffer > block)`
- `~BufferBlock()`
- `size() const`
- `data() const`
- `resetCurrentOffset()`


---

## BufferPoolConstants

**Full name:** `gs::common::BufferPoolConstants`


---

## BufferReader

**Full name:** `gs::common::BufferReader`

**Public Methods:**

- `BufferReader(uint8_t *data, size_t dataSize)`
- `read(uint8_t *outputData, uint64_t size) override`
- `finished() override`


---

## BufferedFileReader

**Full name:** `gs::common::BufferedFileReader`

**Public Methods:**

- `BufferedFileReader(std::unique_ptr< FileInfo > fileInfo)`
- `read(uint8_t *data, uint64_t size) override`
- `finished() override`


---

## BufferedFileWriter

**Full name:** `gs::common::BufferedFileWriter`

**Public Methods:**

- `BufferedFileWriter(FileInfo &fileInfo)`
- `~BufferedFileWriter() override`
- `write(const uint8_t *data, uint64_t size) override`
- `flush()`
- `sync()`
- `setFileOffset(uint64_t fileOffset)`
- `getFileOffset() const`
- `resetOffsets()`
- `getFileSize() const`
- `getFileInfo() const`


---

## BufferedSerializer

**Full name:** `gs::common::BufferedSerializer`

**Public Methods:**

- `BufferedSerializer(uint64_t maximumSize=SERIALIZER_DEFAULT_SIZE)`
- `BufferedSerializer(std::unique_ptr< uint8_t[]> data, uint64_t size)`
- `getData()`
- `getSize() const`
- `getBlobData() const`
- `reset()`
- `write(T element)`
- `write(const uint8_t *buffer, uint64_t len) final`
- `writeBufferData(const std::string &str)`
- `writeBufferData(const char &ch)`


---

## CSVOption

**Full name:** `gs::common::CSVOption`

**Public Methods:**

- `CSVOption()`
- `EXPLICIT_COPY_DEFAULT_MOVE(CSVOption)`
- `toCypher() const`
- `CSVOption(const CSVOption &other)`


---

## CSVReaderConfig

**Full name:** `gs::common::CSVReaderConfig`

**Public Methods:**

- `CSVReaderConfig()`
- `EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig)`
- `construct(const case_insensitive_map_t< common::Value > &options)`


---

## CaseInsensitiveStringEquality

**Full name:** `gs::common::CaseInsensitiveStringEquality`

**Public Methods:**

- `operator()(const std::string &lhs, const std::string &rhs) const`


---

## CaseInsensitiveStringHashFunction

**Full name:** `gs::common::CaseInsensitiveStringHashFunction`

**Public Methods:**

- `operator()(const std::string &str) const`


---

## CompressedFileInfo

**Full name:** `gs::common::CompressedFileInfo`

**Public Methods:**

- `CompressedFileInfo(CompressedFileSystem &compressedFS, std::unique_p...`
- `~CompressedFileInfo() override`
- `initialize()`
- `readData(void *buffer, size_t numBytes)`
- `close()`


---

## CompressedFileSystem

**Full name:** `gs::common::CompressedFileSystem`

**Public Methods:**

- `openCompressedFile(std::unique_ptr< FileInfo > fileInfo)=0`
- `createStream()=0`
- `getInputBufSize()=0`
- `getOutputBufSize()=0`
- `canPerformSeek() const override`


---

## ConcurrentVector

**Full name:** `gs::common::ConcurrentVector`

**Public Methods:**

- `ConcurrentVector(uint64_t initialNumElements, uint64_t initialBlockSize)`
- `resize(uint64_t newSize)`
- `push_back(T &&value)`
- `operator[](uint64_t elemPos)`
- `size()`


---

## Block

**Full name:** `gs::common::ConcurrentVector::Block`


---

## BlockIndex

**Full name:** `gs::common::ConcurrentVector::BlockIndex`

**Public Methods:**

- `BlockIndex()`


---

## ConflictActionUtil

**Full name:** `gs::common::ConflictActionUtil`

**Public Methods:**

- `toString(ConflictAction conflictAction)`


---

## CopyConstants

**Full name:** `gs::common::CopyConstants`


---

## CountZeros

**Full name:** `gs::common::CountZeros`

**Public Methods:**

- `Leading(T value_in)`
- `Trailing(T value_in)`


---

## CountZeros< int128_t >

**Full name:** `gs::common::CountZeros< int128_t >`

**Public Methods:**

- `Leading(int128_t value)`
- `Trailing(int128_t value)`


---

## DataChunk

**Full name:** `gs::common::DataChunk`

**Public Methods:**

- `DataChunk()`
- `DataChunk(uint32_t numValueVectors)`
- `DataChunk(uint32_t numValueVectors, const std::shared_ptr< ...`
- `DELETE_COPY_DEFAULT_MOVE(DataChunk)`
- `insert(uint32_t pos, std::shared_ptr< ValueVector > valueVector)`
- `resetAuxiliaryBuffer()`
- `getNumValueVectors() const`
- `getValueVector(uint64_t valueVectorPos) const`
- `getValueVectorMutable(uint64_t valueVectorPos) const`


---

## DataChunkCollection

**Full name:** `gs::common::DataChunkCollection`

**Public Methods:**

- `DataChunkCollection(storage::MemoryManager *mm)`
- `DELETE_COPY_DEFAULT_MOVE(DataChunkCollection)`
- `append(DataChunk &chunk)`
- `getChunks() const`
- `getChunksUnsafe()`
- `getNumChunks() const`
- `getChunk(uint64_t idx) const`
- `getChunkUnsafe(uint64_t idx)`
- `merge(DataChunkCollection *other)`
- `merge(DataChunk chunk)`


---

## DataChunkState

**Full name:** `gs::common::DataChunkState`

**Public Methods:**

- `DataChunkState()`
- `DataChunkState(sel_t capacity)`
- `initOriginalAndSelectedSize(uint64_t size)`
- `isFlat() const`
- `setToFlat()`
- `setToUnflat()`
- `getSelVector() const`
- `getSelSize() const`
- `getSelVectorUnsafe()`
- `getSelVectorShared()`
- ... and 3 more methods


---

## Date

**Full name:** `gs::common::Date`

**Public Methods:**

- `fromCString(const char *str, uint64_t len)`
- `toString(date_t date)`
- `tryConvertDate(const char *buf, uint64_t len, uint64_t &pos, dat...`
- `isLeapYear(int32_t year)`
- `isValid(int32_t year, int32_t month, int32_t day)`
- `convert(date_t date, int32_t &out_year, int32_t &out_mont...`
- `fromDate(int32_t year, int32_t month, int32_t day)`
- `parseDoubleDigit(const char *buf, uint64_t len, uint64_t &pos, int...`
- `monthDays(int32_t year, int32_t month)`
- `getDayName(date_t date)`
- ... and 6 more methods


---

## DateToStringCast

**Full name:** `gs::common::DateToStringCast`

**Public Methods:**

- `Length(int32_t date[], uint64_t &yearLength, bool &addBC)`
- `Format(char *data, int32_t date[], uint64_t yearLen, bool addBC)`


---

## DecimalType

**Full name:** `gs::common::DecimalType`

**Public Methods:**

- `getPrecision(const LogicalType &type)`
- `getScale(const LogicalType &type)`
- `insertDecimalPoint(const std::string &value, uint32_t posFromEnd)`


---

## DecimalTypeInfo

**Full name:** `gs::common::DecimalTypeInfo`

**Public Methods:**

- `DecimalTypeInfo(uint32_t precision=18, uint32_t scale=3)`
- `getPrecision() const`
- `getScale() const`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## Deserializer

**Full name:** `gs::common::Deserializer`

**Public Methods:**

- `Deserializer(std::unique_ptr< Reader > reader)`
- `finished() const`
- `deserializeValue(T &value)`
- `read(uint8_t *data, uint64_t size)`
- `validateDebuggingInfo(std::string &value, const std::string &expectedVal)`
- `deserializeOptionalValue(std::unique_ptr< T > &value)`
- `deserializeMap(std::map< T1, T2 > &values)`
- `deserializeUnorderedMap(std::unordered_map< T1, T2 > &values)`
- `deserializeUnorderedMapOfPtrs(std::unordered_map< T1, std::unique_ptr< T2 > > &values)`
- `deserializeVector(std::vector< T > &values)`
- ... and 6 more methods


---

## DropTypeUtils

**Full name:** `gs::common::DropTypeUtils`

**Public Methods:**

- `toString(DropType type)`


---

## ExceptionMessage

**Full name:** `gs::common::ExceptionMessage`

**Public Methods:**

- `duplicatePKException(const std::string &pkString)`
- `nonExistentPKException(const std::string &pkString)`
- `invalidPKType(const std::string &type)`
- `nullPKException()`
- `overLargeStringPKValueException(uint64_t length)`
- `overLargeStringValueException(uint64_t length)`
- `violateDeleteNodeWithConnectedEdgesConstraint(const std::string &tableName, const std::string &...`
- `violateRelMultiplicityConstraint(const std::string &tableName, const std::string &...`
- `variableNotInScope(const std::string &varName)`
- `listFunctionIncompatibleChildrenType(const std::string &functionName, const std::strin...`


---

## ExportCSVConstants

**Full name:** `gs::common::ExportCSVConstants`


---

## ExpressionTypeUtil

**Full name:** `gs::common::ExpressionTypeUtil`

**Public Methods:**

- `isUnary(ExpressionType type)`
- `isBinary(ExpressionType type)`
- `isBoolean(ExpressionType type)`
- `isComparison(ExpressionType type)`
- `isNullOperator(ExpressionType type)`
- `reverseComparisonDirection(ExpressionType type)`
- `toString(ExpressionType type)`
- `toParsableString(ExpressionType type)`


---

## ExtendDirectionUtil

**Full name:** `gs::common::ExtendDirectionUtil`

**Public Methods:**

- `getRelDataDirection(ExtendDirection direction)`
- `fromString(const std::string &str)`
- `toString(ExtendDirection direction)`


---

## ExtraTypeInfo

**Full name:** `gs::common::ExtraTypeInfo`

**Public Methods:**

- `~ExtraTypeInfo()=default`
- `serialize(Serializer &serializer) const`
- `containsAny() const =0`
- `operator==(const ExtraTypeInfo &other) const =0`
- `copy() const =0`
- `constPtrCast() const`


---

## FileFlags

**Full name:** `gs::common::FileFlags`


---

## FileInfo

**Full name:** `gs::common::FileInfo`

**Public Methods:**

- `FileInfo(std::string path, FileSystem *fileSystem)`
- `~FileInfo()=default`
- `getFileSize() const`
- `readFromFile(void *buffer, uint64_t numBytes, uint64_t position)`
- `readFile(void *buf, size_t nbyte)`
- `writeFile(const uint8_t *buffer, uint64_t numBytes, uint64_t offset)`
- `syncFile() const`
- `seek(uint64_t offset, int whence)`
- `reset()`
- `truncate(uint64_t size)`
- ... and 5 more methods


---

## FileOpenFlags

**Full name:** `gs::common::FileOpenFlags`

**Public Methods:**

- `FileOpenFlags(int flags)`


---

## FileScanInfo

**Full name:** `gs::common::FileScanInfo`

**Public Methods:**

- `FileScanInfo()`
- `FileScanInfo(FileTypeInfo fileTypeInfo, std::vector< std::stri...`
- `EXPLICIT_COPY_DEFAULT_MOVE(FileScanInfo)`
- `getNumFiles() const`
- `getFilePath(idx_t fileIdx) const`
- `getOption(std::string optionName, T defaultValue) const`


---

## FileSystem

**Full name:** `gs::common::FileSystem`

**Public Methods:**

- `FileSystem()=default`
- `FileSystem(std::string homeDir)`
- `~FileSystem()=default`
- `openFile(const std::string &, FileOpenFlags, main::ClientC...`
- `glob(main::ClientContext *, const std::string &) const`
- `overwriteFile(const std::string &from, const std::string &to)`
- `copyFile(const std::string &from, const std::string &to)`
- `createDir(const std::string &dir) const`
- `removeFileIfExists(const std::string &path)`
- `fileOrPathExists(const std::string &path, main::ClientContext *con...`
- ... and 11 more methods


---

## FileTypeInfo

**Full name:** `gs::common::FileTypeInfo`


---

## FileTypeUtils

**Full name:** `gs::common::FileTypeUtils`

**Public Methods:**

- `getFileTypeFromExtension(std::string_view extension)`
- `toString(FileType fileType)`
- `fromString(std::string fileType)`


---

## HashIndexConstants

**Full name:** `gs::common::HashIndexConstants`


---

## HexFormatConstants

**Full name:** `gs::common::HexFormatConstants`


---

## InMemOverflowBuffer

**Full name:** `gs::common::InMemOverflowBuffer`

**Public Methods:**

- `InMemOverflowBuffer(storage::MemoryManager *memoryManager)`
- `DEFAULT_BOTH_MOVE(InMemOverflowBuffer)`
- `allocateSpace(uint64_t size)`
- `merge(InMemOverflowBuffer &other)`
- `resetBuffer()`
- `getMemoryManager()`


---

## Int128_t

**Full name:** `gs::common::Int128_t`

**Public Methods:**

- `ToString(int128_t input)`
- `tryCast(int128_t input, T &result)`
- `Cast(int128_t input)`
- `tryCastTo(T value, int128_t &result)`
- `castTo(T value)`
- `negateInPlace(int128_t &input)`
- `negate(int128_t input)`
- `tryMultiply(int128_t lhs, int128_t rhs, int128_t &result)`
- `Add(int128_t lhs, int128_t rhs)`
- `Sub(int128_t lhs, int128_t rhs)`
- ... and 42 more methods


---

## InternalKeyword

**Full name:** `gs::common::InternalKeyword`


---

## Interval

**Full name:** `gs::common::Interval`

**Public Methods:**

- `addition(interval_t &result, uint64_t number, std::string ...`
- `fromCString(const char *str, uint64_t len)`
- `toString(interval_t interval)`
- `greaterThan(const interval_t &left, const interval_t &right)`
- `normalizeIntervalEntries(interval_t input, int64_t &months, int64_t &days,...`
- `tryGetDatePartSpecifier(std::string specifier, DatePartSpecifier &result)`
- `getIntervalPart(DatePartSpecifier specifier, interval_t timestamp)`
- `getMicro(const interval_t &val)`
- `getNanoseconds(const interval_t &val)`
- `regexPattern1()`
- ... and 1 more methods


---

## IntervalToStringCast

**Full name:** `gs::common::IntervalToStringCast`

**Public Methods:**

- `FormatSignedNumber(int64_t value, char buffer[], uint64_t &length)`
- `FormatTwoDigits(int64_t value, char buffer[], uint64_t &length)`
- `FormatIntervalValue(int32_t value, char buffer[], uint64_t &length, c...`
- `Format(interval_t interval, char buffer[])`


---

## LimitCounter

**Full name:** `gs::common::LimitCounter`

**Public Methods:**

- `LimitCounter(common::offset_t limitNumber)`
- `increase(common::offset_t number)`
- `exceedLimit() const`


---

## ListAuxiliaryBuffer

**Full name:** `gs::common::ListAuxiliaryBuffer`

**Public Methods:**

- `ListAuxiliaryBuffer(const LogicalType &dataVectorType, storage::Memor...`
- `setDataVector(std::shared_ptr< ValueVector > vector)`
- `getDataVector() const`
- `getSharedDataVector() const`
- `addList(list_size_t listSize)`
- `getSize() const`
- `resetSize()`
- `resize(uint64_t numValues)`


---

## ListType

**Full name:** `gs::common::ListType`

**Public Methods:**

- `getChildType(const LogicalType &type)`


---

## ListTypeInfo

**Full name:** `gs::common::ListTypeInfo`

**Public Methods:**

- `ListTypeInfo()=default`
- `ListTypeInfo(LogicalType childType)`
- `getChildType() const`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## ListVector

**Full name:** `gs::common::ListVector`

**Public Methods:**

- `getAuxBuffer(const ValueVector &vector)`
- `getAuxBufferUnsafe(const ValueVector &vector)`
- `setDataVector(const ValueVector *vector, std::shared_ptr< Value...`
- `copyListEntryAndBufferMetaData(ValueVector &vector, const SelectionVector &selVe...`
- `getDataVector(const ValueVector *vector)`
- `getSharedDataVector(const ValueVector *vector)`
- `getDataVectorSize(const ValueVector *vector)`
- `getListValues(const ValueVector *vector, const list_entry_t &listEntry)`
- `getListValuesWithOffset(const ValueVector *vector, const list_entry_t &li...`
- `addList(ValueVector *vector, uint64_t listSize)`
- ... and 6 more methods


---

## LocalFileInfo

**Full name:** `gs::common::LocalFileInfo`

**Public Methods:**

- `LocalFileInfo(std::string path, const int fd, FileSystem *fileSystem)`
- `~LocalFileInfo() override`


---

## LocalFileSystem

**Full name:** `gs::common::LocalFileSystem`

**Public Methods:**

- `LocalFileSystem(std::string homeDir)`
- `openFile(const std::string &path, FileOpenFlags flags, mai...`
- `glob(main::ClientContext *context, const std::string &...`
- `overwriteFile(const std::string &from, const std::string &to) override`
- `copyFile(const std::string &from, const std::string &to) override`
- `createDir(const std::string &dir) const override`
- `removeFileIfExists(const std::string &path) override`
- `fileOrPathExists(const std::string &path, main::ClientContext *con...`
- `expandPath(main::ClientContext *context, const std::string &...`
- `syncFile(const FileInfo &fileInfo) const override`
- ... and 2 more methods


---

## LogicalType

**Full name:** `gs::common::LogicalType`

**Public Methods:**

- `LogicalType()`
- `LogicalType(LogicalTypeID typeID, TypeCategory info=TypeCateg...`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalType)`
- `operator==(const LogicalType &other) const`
- `operator!=(const LogicalType &other) const`
- `toString() const`
- `getLogicalTypeID() const`
- `containsAny() const`
- `isInternalType() const`
- `getPhysicalType() const`
- ... and 49 more methods


---

## LogicalTypeRegistry

**Full name:** `gs::common::LogicalTypeRegistry`

**Public Methods:**

- `LogicalTypeRegistry()`
- `registerType(const YAML::Node &typeYaml, const common::Logical...`
- `getTypeID(const YAML::Node &typeYaml)`
- `containsTypeYaml(const YAML::Node &typeYaml)`


---

## LogicalTypeUtils

**Full name:** `gs::common::LogicalTypeUtils`

**Public Methods:**

- `toString(LogicalTypeID dataTypeID)`
- `toString(const std::vector< LogicalType > &dataTypes)`
- `toString(const std::vector< LogicalTypeID > &dataTypeIDs)`
- `getRowLayoutSize(const LogicalType &logicalType)`
- `isDate(const LogicalType &dataType)`
- `isDate(const LogicalTypeID &dataType)`
- `isTimestamp(const LogicalType &dataType)`
- `isTimestamp(const LogicalTypeID &dataType)`
- `isUnsigned(const LogicalType &dataType)`
- `isUnsigned(const LogicalTypeID &dataType)`
- ... and 17 more methods


---

## MD5

**Full name:** `gs::common::MD5`

**Public Methods:**

- `addToMD5(const char *z, uint32_t len)`
- `finishMD5()`


---

## Context

**Full name:** `gs::common::MD5::Context`


---

## MPSCQueue

**Full name:** `gs::common::MPSCQueue`

**Public Methods:**

- `MPSCQueue()`
- `DELETE_BOTH_COPY(MPSCQueue)`
- `MPSCQueue(MPSCQueue &&other)`
- `DELETE_MOVE_ASSN(MPSCQueue)`
- `push(T elem)`
- `pop(T &elem)`
- `approxSize() const`
- `~MPSCQueue()`


---

## Node

**Full name:** `gs::common::MPSCQueue::Node`

**Public Methods:**

- `Node(T data)`


---

## MapType

**Full name:** `gs::common::MapType`

**Public Methods:**

- `getKeyType(const LogicalType &type)`
- `getValueType(const LogicalType &type)`


---

## MapVector

**Full name:** `gs::common::MapVector`

**Public Methods:**

- `getKeyVector(const ValueVector *vector)`
- `getValueVector(const ValueVector *vector)`
- `getMapKeys(const ValueVector *vector, const list_entry_t &listEntry)`
- `getMapValues(const ValueVector *vector, const list_entry_t &listEntry)`


---

## MaybeUninit

**Full name:** `gs::common::MaybeUninit`

**Public Methods:**

- `assumeInit()`
- `assumeInit() const`
- `ptr()`
- `ptr() const`


---

## Metric

**Full name:** `gs::common::Metric`

Note that metrics are not thread safe.

**Public Methods:**

- `Metric(bool enabled)`
- `~Metric()=default`


---

## Mutex

**Full name:** `gs::common::Mutex`

**Public Methods:**

- `Mutex()`
- `Mutex(T data)`
- `DELETE_COPY_AND_MOVE(Mutex)`
- `lock()`
- `try_lock()`


---

## MutexGuard

**Full name:** `gs::common::MutexGuard`

**Public Methods:**

- `DELETE_COPY_DEFAULT_MOVE(MutexGuard)`
- `operator->() &`
- `operator*() &`
- `get() &`
- `operator->() &&=delete`
- `operator*() &&=delete`
- `get() &&=delete`


---

## NestedVal

**Full name:** `gs::common::NestedVal`

**Public Methods:**

- `getChildrenSize(const Value *val)`
- `getChildVal(const Value *val, uint32_t idx)`


---

## NodeOffsetMaskMap

**Full name:** `gs::common::NodeOffsetMaskMap`

**Public Methods:**

- `NodeOffsetMaskMap()=default`
- `getNumMaskedNode() const`
- `addMask(table_id_t tableID, std::unique_ptr< SemiMask > mask)`
- `getMasks() const`
- `containsTableID(table_id_t tableID) const`
- `getOffsetMask(table_id_t tableID) const`
- `pin(table_id_t tableID)`
- `hasPinnedMask() const`
- `getPinnedMask() const`
- `valid(offset_t offset) const`
- ... and 1 more methods


---

## NodeVal

**Full name:** `gs::common::NodeVal`

`NodeVal` represents a node in the graph and stores the nodeID, label and properties of that node.

**Public Methods:**

- `getProperties(const Value *val)`
- `getNumProperties(const Value *val)`
- `getPropertyName(const Value *val, uint64_t index)`
- `getPropertyVal(const Value *val, uint64_t index)`
- `getNodeIDVal(const Value *val)`
- `getLabelVal(const Value *val)`
- `toString(const Value *val)`


---

## NullBuffer

**Full name:** `gs::common::NullBuffer`

**Public Methods:**

- `isNull(const uint8_t *nullBytes, uint64_t valueIdx)`
- `setNull(uint8_t *nullBytes, uint64_t valueIdx)`
- `setNoNull(uint8_t *nullBytes, uint64_t valueIdx)`
- `getNumBytesForNullValues(uint64_t numValues)`
- `initNullBytes(uint8_t *nullBytes, uint64_t numValues)`


---

## NullMask

**Full name:** `gs::common::NullMask`

**Public Methods:**

- `NullMask(uint64_t capacity)`
- `NullMask(std::span< uint64_t > nullData, bool mayContainNulls)`
- `setAllNonNull()`
- `setAllNull()`
- `hasNoNullsGuarantee() const`
- `countNulls() const`
- `setNull(uint32_t pos, bool isNull)`
- `isNull(uint32_t pos) const`
- `getData() const`
- `copyFrom(const NullMask &nullMask, uint64_t srcOffset, uin...`
- ... and 11 more methods


---

## NumericHelper

**Full name:** `gs::common::NumericHelper`

`NumericHelper` is a `static` class that holds helper functions for integers`/doubles`

**Public Methods:**

- `FormatUnsigned(T value, char *ptr)`
- `getUnsignedInt64Length(uint64_t value)`


---

## NumericMetric

**Full name:** `gs::common::NumericMetric`

**Public Methods:**

- `NumericMetric(bool enable)`
- `increase(uint64_t value)`
- `incrementByOne()`


---

## OrderByConstants

**Full name:** `gs::common::OrderByConstants`


---

## ParquetConstants

**Full name:** `gs::common::ParquetConstants`


---

## PathSemanticUtils

**Full name:** `gs::common::PathSemanticUtils`

**Public Methods:**

- `fromString(const std::string &str)`
- `toString(PathSemantic semantic)`


---

## PhysicalTypeUtils

**Full name:** `gs::common::PhysicalTypeUtils`

**Public Methods:**

- `toString(PhysicalTypeID physicalType)`
- `getFixedTypeSize(PhysicalTypeID physicalType)`


---

## PlannerKnobs

**Full name:** `gs::common::PlannerKnobs`


---

## PortDBConstants

**Full name:** `gs::common::PortDBConstants`


---

## Profiler

**Full name:** `gs::common::Profiler`

**Public Methods:**

- `registerTimeMetric(const std::string &key)`
- `registerNumericMetric(const std::string &key)`
- `sumAllTimeMetricsWithKey(const std::string &key)`
- `sumAllNumericMetricsWithKey(const std::string &key)`


---

## ProgressBar

**Full name:** `gs::common::ProgressBar`

Progress bar for tracking the progress of a pipeline.

Prints the progress of each query pipeline and the overall progress.

**Public Methods:**

- `ProgressBar(bool enableProgressBar)`
- `addPipeline()`
- `finishPipeline(uint64_t queryID)`
- `endProgress(uint64_t queryID)`
- `startProgress(uint64_t queryID)`
- `toggleProgressBarPrinting(bool enable)`
- `updateProgress(uint64_t queryID, double curPipelineProgress)`
- `setDisplay(std::shared_ptr< ProgressBarDisplay > progressBarDipslay)`
- `getDisplay()`
- `getProgressBarPrinting() const`
- ... and 1 more methods


---

## ProgressBarDisplay

**Full name:** `gs::common::ProgressBarDisplay`

Interface for displaying progress of a pipeline and a query.

**Public Methods:**

- `ProgressBarDisplay()`
- `~ProgressBarDisplay()=default`
- `updateProgress(uint64_t queryID, double newPipelineProgress, uin...`
- `finishProgress(uint64_t queryID)=0`
- `setNumPipelines(uint32_t newNumPipelines)`


---

## QueryRelTypeUtils

**Full name:** `gs::common::QueryRelTypeUtils`

**Public Methods:**

- `isRecursive(QueryRelType type)`
- `isWeighted(QueryRelType type)`
- `getPathSemantic(QueryRelType queryRelType)`
- `getFunction(QueryRelType type)`


---

## RandomEngine

**Full name:** `gs::common::RandomEngine`

**Public Methods:**

- `RandomEngine()`
- `RandomEngine(uint64_t seed, uint64_t stream)`
- `nextRandomInteger()`
- `nextRandomInteger(uint32_t upper)`


---

## RandomState

**Full name:** `gs::common::RandomState`

**Public Methods:**

- `RandomState()`


---

## Reader

**Full name:** `gs::common::Reader`

**Public Methods:**

- `read(uint8_t *data, uint64_t size)=0`
- `~Reader()=default`
- `finished()=0`


---

## RecursiveRelVal

**Full name:** `gs::common::RecursiveRelVal`

`RecursiveRelVal` represents a path in the graph and stores the corresponding rels and nodes of that path.

**Public Methods:**

- `getNodes(const Value *val)`
- `getRels(const Value *val)`


---

## RelDirectionUtils

**Full name:** `gs::common::RelDirectionUtils`

**Public Methods:**

- `getOppositeDirection(RelDataDirection direction)`
- `relDirectionToString(RelDataDirection direction)`
- `relDirectionToKeyIdx(RelDataDirection direction)`


---

## RelMultiplicityUtils

**Full name:** `gs::common::RelMultiplicityUtils`

**Public Methods:**

- `getFwd(const std::string &str)`
- `getBwd(const std::string &str)`
- `toString(RelMultiplicity multiplicity)`


---

## RelVal

**Full name:** `gs::common::RelVal`

`RelVal` represents a rel in the graph and stores the relID, src`/dst` nodes and properties of that rel.

**Public Methods:**

- `getProperties(const Value *val)`
- `getNumProperties(const Value *val)`
- `getPropertyName(const Value *val, uint64_t index)`
- `getPropertyVal(const Value *val, uint64_t index)`
- `getSrcNodeIDVal(const Value *val)`
- `getDstNodeIDVal(const Value *val)`
- `getIDVal(const Value *val)`
- `getLabelVal(const Value *val)`
- `toString(const Value *val)`


---

## ScanSourceTypeUtils

**Full name:** `gs::common::ScanSourceTypeUtils`

**Public Methods:**

- `toString(ScanSourceType type)`


---

## ScheduledTask

**Full name:** `gs::common::ScheduledTask`

**Public Methods:**

- `ScheduledTask(std::shared_ptr< Task > task, uint64_t ID)`


---

## SelectionVector

**Full name:** `gs::common::SelectionVector`

**Public Methods:**

- `SelectionVector(sel_t capacity)`
- `slice(sel_t startIndex, sel_t selectedSize) const`
- `SelectionVector()`
- `setToUnfiltered()`
- `setToUnfiltered(sel_t size)`
- `setRange(sel_t startPos, sel_t size)`
- `setToFiltered()`
- `setToFiltered(sel_t size)`
- `makeDynamic()`
- `getMutableBuffer() const`
- ... and 5 more methods


---

## SelectionView

**Full name:** `gs::common::SelectionView`

**Public Methods:**

- `SelectionView(sel_t selectedSize)`
- `SelectionView(sel_t startPos, sel_t selectedSize)`
- `forEach(Func &&func) const`
- `forEachBreakWhenFalse(Func &&func) const`
- `getSelSize() const`
- `operator[](sel_t index) const`
- `isUnfiltered() const`
- `isStatic() const`
- `getSelectedPositions() const`


---

## SemiMask

**Full name:** `gs::common::SemiMask`

**Public Methods:**

- `SemiMask(offset_t maxOffset)`
- `~SemiMask()=default`
- `mask(offset_t nodeOffset)=0`
- `maskRange(offset_t startNodeOffset, offset_t endNodeOffset)=0`
- `isMasked(offset_t startNodeOffset)=0`
- `range(uint32_t start, uint32_t end)=0`
- `getNumMaskedNodes() const =0`
- `collectMaskedNodes(uint64_t size) const =0`
- `getMaxOffset() const`
- `isEnabled() const`
- ... and 1 more methods


---

## Serializer

**Full name:** `gs::common::Serializer`

**Public Methods:**

- `Serializer(std::shared_ptr< Writer > writer)`
- `serializeValue(const T &value)`
- `write(const T &value)`
- `writeDebuggingInfo(const std::string &value)`
- `write(const uint8_t *value, uint64_t len)`
- `serializeOptionalValue(const std::unique_ptr< T > &value)`
- `serializeMap(const std::map< T1, T2 > &values)`
- `serializeUnorderedMap(const std::unordered_map< T1, T2 > &values)`
- `serializeUnorderedMapOfPtrs(const std::unordered_map< T1, std::unique_ptr< T2...`
- `serializeVector(const std::vector< T > &values)`
- ... and 5 more methods


---

## StaticVector

**Full name:** `gs::common::StaticVector`

**Public Methods:**

- `StaticVector()`
- `StaticVector(StaticVector &&other)`
- `DELETE_COPY_ASSN(StaticVector)`
- `EXPLICIT_COPY_METHOD(StaticVector)`
- `operator=(StaticVector &&other)`
- `~StaticVector()`
- `operator[](size_t i)`
- `operator[](size_t i) const`
- `push_back(T elem)`
- `pop_back()`
- ... and 8 more methods


---

## StorageConstants

**Full name:** `gs::common::StorageConstants`


---

## StreamData

**Full name:** `gs::common::StreamData`


---

## StreamWrapper

**Full name:** `gs::common::StreamWrapper`

**Public Methods:**

- `~StreamWrapper()=default`
- `initialize(CompressedFileInfo &file)=0`
- `read(StreamData &stream_data)=0`
- `close()=0`


---

## StringAuxiliaryBuffer

**Full name:** `gs::common::StringAuxiliaryBuffer`

**Public Methods:**

- `StringAuxiliaryBuffer(storage::MemoryManager *memoryManager)`
- `getOverflowBuffer() const`
- `allocateOverflow(uint64_t size)`
- `resetOverflowBuffer() const`


---

## StringUtils

**Full name:** `gs::common::StringUtils`

**Public Methods:**

- `splitComma(const std::string &input)`
- `smartSplit(std::string_view input, char splitChar, uint64_t ...`
- `split(const std::string &input, const std::string &deli...`
- `splitBySpace(const std::string &input)`
- `getUpper(const std::string &input)`
- `getUpper(const std::string_view &input)`
- `getLower(const std::string &input)`
- `toLower(std::string &input)`
- `toUpper(std::string &input)`
- `isSpace(char c)`
- ... and 18 more methods


---

## string_hash

**Full name:** `gs::common::StringUtils::string_hash`

**Public Methods:**

- `operator()(const char *str) const`
- `operator()(std::string_view str) const`
- `operator()(std::string const &str) const`


---

## StringVector

**Full name:** `gs::common::StringVector`

**Public Methods:**

- `getInMemOverflowBuffer(ValueVector *vector)`
- `addString(ValueVector *vector, uint32_t vectorPos, neug_strin...`
- `addString(ValueVector *vector, uint32_t vectorPos, const ch...`
- `addString(ValueVector *vector, uint32_t vectorPos, const st...`
- `reserveString(ValueVector *vector, uint32_t vectorPos, uint64_t length)`
- `reserveString(ValueVector *vector, neug_string_t &dstStr, uint64_t length)`
- `addString(ValueVector *vector, neug_string_t &dstStr, neug_stri...`
- `addString(ValueVector *vector, neug_string_t &dstStr, const c...`
- `addString(gs::common::ValueVector *vector, neug_string_t &dst...`
- `copyToRowData(const ValueVector *vector, uint32_t pos, uint8_t ...`


---

## StructAuxiliaryBuffer

**Full name:** `gs::common::StructAuxiliaryBuffer`

**Public Methods:**

- `StructAuxiliaryBuffer(const LogicalType &type, storage::MemoryManager *...`
- `referenceChildVector(idx_t idx, std::shared_ptr< ValueVector > vectorT...`
- `getFieldVectors() const`
- `getFieldVectorShared(idx_t idx) const`
- `getFieldVectorPtr(idx_t idx) const`


---

## StructField

**Full name:** `gs::common::StructField`

**Public Methods:**

- `StructField()`
- `StructField(std::string name, LogicalType type)`
- `DELETE_COPY_DEFAULT_MOVE(StructField)`
- `getName() const`
- `getType() const`
- `containsAny() const`
- `operator==(const StructField &other) const`
- `operator!=(const StructField &other) const`
- `serialize(Serializer &serializer) const`
- `copy() const`
- ... and 1 more methods


---

## StructType

**Full name:** `gs::common::StructType`

**Public Methods:**

- `getFieldTypes(const LogicalType &type)`
- `getFieldType(const LogicalType &type, struct_field_idx_t idx)`
- `getFieldType(const LogicalType &type, const std::string &key)`
- `getFieldNames(const LogicalType &type)`
- `getNumFields(const LogicalType &type)`
- `getFields(const LogicalType &type)`
- `hasField(const LogicalType &type, const std::string &key)`
- `getField(const LogicalType &type, struct_field_idx_t idx)`
- `getField(const LogicalType &type, const std::string &key)`
- `getFieldIdx(const LogicalType &type, const std::string &key)`
- ... and 1 more methods


---

## StructTypeInfo

**Full name:** `gs::common::StructTypeInfo`

**Public Methods:**

- `StructTypeInfo()=default`
- `StructTypeInfo(std::vector< StructField > &&fields)`
- `StructTypeInfo(const std::vector< std::string > &fieldNames, con...`
- `hasField(const std::string &fieldName) const`
- `getStructFieldIdx(std::string fieldName) const`
- `getStructField(struct_field_idx_t idx) const`
- `getStructField(const std::string &fieldName) const`
- `getStructFields() const`
- `getChildType(struct_field_idx_t idx) const`
- `getChildrenTypes() const`
- ... and 5 more methods


---

## StructVector

**Full name:** `gs::common::StructVector`

**Public Methods:**

- `getFieldVectors(const ValueVector *vector)`
- `getFieldVector(const ValueVector *vector, struct_field_idx_t idx)`
- `getFieldVectorRaw(const ValueVector &vector, const std::string &fieldName)`
- `referenceVector(ValueVector *vector, struct_field_idx_t idx, std:...`
- `copyFromRowData(ValueVector *vector, uint32_t pos, const uint8_t *rowData)`
- `copyToRowData(const ValueVector *vector, uint32_t pos, uint8_t ...`
- `copyFromVectorData(ValueVector *dstVector, const uint8_t *dstData, c...`


---

## TableOptionConstants

**Full name:** `gs::common::TableOptionConstants`


---

## TableTypeUtils

**Full name:** `gs::common::TableTypeUtils`

**Public Methods:**

- `toString(TableType tableType)`


---

## Task

**Full name:** `gs::common::Task`

`Task` represents a task that can be executed by multiple threads in the `TaskScheduler`.

`Task` is a `virtual` class. Users of `TaskScheduler` need to extend the `Task` class and implement at least a `virtual` `run()` function. Users can assume that before T.run() is called, a worker thre...

**Public Methods:**

- `Task(uint64_t maxNumThreads)`
- `~Task()=default`
- `run()=0`
- `finalizeIfNecessary()`
- `addChildTask(std::unique_ptr< Task > child)`
- `isCompletedSuccessfully()`
- `isCompletedNoLock() const`
- `setSingleThreadedTask()`
- `registerThread()`
- `deRegisterThreadAndFinalizeTask()`
- ... and 3 more methods


---

## TaskScheduler

**Full name:** `gs::common::TaskScheduler`

`TaskScheduler` is a library that manages a set of worker threads that can execute tasks that are put into a task queue.

Each task accepts a maximum number of threads. Users of `TaskScheduler` schedule tasks to be executed by calling schedule functions, e.g., pushTaskIntoQueue or scheduleTaskAndWaitOrError. New tasks ar...

**Public Methods:**

- `TaskScheduler(uint64_t numWorkerThreads)`
- `~TaskScheduler()`
- `scheduleTaskAndWaitOrError(const std::shared_ptr< Task > &task, processor::E...`


---

## TerminalProgressBarDisplay

**Full name:** `gs::common::TerminalProgressBarDisplay`

A class that displays a progress bar in the terminal.

**Public Methods:**

- `updateProgress(uint64_t queryID, double newPipelineProgress, uin...`
- `finishProgress(uint64_t queryID) override`


---

## Time

**Full name:** `gs::common::Time`

**Public Methods:**

- `fromCString(const char *buf, uint64_t len)`
- `tryConvertInterval(const char *buf, uint64_t len, uint64_t &pos, dti...`
- `tryConvertTime(const char *buf, uint64_t len, uint64_t &pos, dti...`
- `toString(dtime_t time)`
- `fromTime(int32_t hour, int32_t minute, int32_t second, int...`
- `convert(dtime_t time, int32_t &out_hour, int32_t &out_min...`
- `isValid(int32_t hour, int32_t minute, int32_t second, int...`


---

## TimeMetric

**Full name:** `gs::common::TimeMetric`

**Public Methods:**

- `TimeMetric(bool enable)`
- `start()`
- `stop()`
- `getElapsedTimeMS() const`


---

## TimeToStringCast

**Full name:** `gs::common::TimeToStringCast`

**Public Methods:**

- `FormatMicros(uint32_t microseconds, char micro_buffer[])`
- `Length(int32_t time[], char micro_buffer[])`
- `FormatTwoDigits(char *ptr, int32_t value)`
- `Format(char *data, uint64_t length, int32_t time[], char...`


---

## Timer

**Full name:** `gs::common::Timer`

**Public Methods:**

- `start()`
- `stop()`
- `getDuration() const`
- `getElapsedTimeInMS() const`


---

## Timestamp

**Full name:** `gs::common::Timestamp`

**Public Methods:**

- `fromCString(const char *str, uint64_t len)`
- `toString(timestamp_t timestamp)`
- `getDate(timestamp_t timestamp)`
- `getTime(timestamp_t timestamp)`
- `fromDateTime(date_t date, dtime_t time)`
- `tryConvertTimestamp(const char *str, uint64_t len, timestamp_t &result)`
- `convert(timestamp_t timestamp, date_t &out_date, dtime_t &out_time)`
- `fromEpochMicroSeconds(int64_t epochMs)`
- `fromEpochMilliSeconds(int64_t ms)`
- `fromEpochSeconds(int64_t sec)`
- ... and 9 more methods


---

## TypeUtils

**Full name:** `gs::common::TypeUtils`

**Public Methods:**

- `paramPackForEachHelper(const Func &func, std::index_sequence< indices......`
- `paramPackForEach(const Func &func, Types &&... values)`
- `entryToString(const LogicalType &dataType, const uint8_t *value...`
- `toString(const T &val, void *=nullptr)`
- `nodeToString(const struct_entry_t &val, ValueVector *vector)`
- `relToString(const struct_entry_t &val, ValueVector *vector)`
- `encodeOverflowPtr(uint64_t &overflowPtr, page_idx_t pageIdx, uint32...`
- `decodeOverflowPtr(uint64_t overflowPtr, page_idx_t &pageIdx, uint32...`
- `getPhysicalTypeIDForType()`
- `visit(const LogicalType &dataType, Fs... funcs)`
- ... and 18 more methods


---

## UDTTypeInfo

**Full name:** `gs::common::UDTTypeInfo`

**Public Methods:**

- `UDTTypeInfo(std::string typeName)`
- `getTypeName() const`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## UUID

**Full name:** `gs::common::UUID`

**Public Methods:**

- `byteToHex(char byteVal, char *buf, uint64_t &pos)`
- `hex2Char(char ch)`
- `isHex(char ch)`
- `fromString(std::string str, int128_t &result)`
- `fromString(std::string str)`
- `fromCString(const char *str, uint64_t len)`
- `toString(int128_t input, char *buf)`
- `toString(int128_t input)`
- `toString(neug_uuid_t val)`
- `generateRandomUUID(RandomEngine *engine)`
- ... and 1 more methods


---

## UnionType

**Full name:** `gs::common::UnionType`

**Public Methods:**

- `getInternalFieldIdx(union_field_idx_t idx)`
- `getFieldName(const LogicalType &type, union_field_idx_t idx)`
- `getFieldType(const LogicalType &type, union_field_idx_t idx)`
- `getNumFields(const LogicalType &type)`


---

## UnionVector

**Full name:** `gs::common::UnionVector`

**Public Methods:**

- `getTagVector(const ValueVector *vector)`
- `getValVector(const ValueVector *vector, union_field_idx_t fieldIdx)`
- `referenceVector(ValueVector *vector, union_field_idx_t fieldIdx, ...`
- `setTagField(ValueVector &vector, SelectionVector &sel, union_...`


---

## UniqLock

**Full name:** `gs::common::UniqLock`

**Public Methods:**

- `UniqLock()`
- `UniqLock(std::mutex &mtx)`
- `UniqLock(const UniqLock &)=delete`
- `operator=(const UniqLock &)=delete`
- `UniqLock(UniqLock &&other) noexcept`
- `operator=(UniqLock &&other) noexcept`
- `isLocked() const`


---

## Value

**Full name:** `gs::common::Value`

**Public Methods:**

- `createNullValue()`
- `createNullValue(const LogicalType &dataType)`
- `createDefaultValue(const LogicalType &dataType)`
- `createValue(T)`
- `deserialize(Deserializer &deserializer)`
- `Value(bool val_)`
- `Value(int8_t val_)`
- `Value(int16_t val_)`
- `Value(int32_t val_)`
- `Value(int64_t val_)`
- ... and 105 more methods


---

## ValueVector

**Full name:** `gs::common::ValueVector`

A Vector represents values of the same data type. The capacity of a `ValueVector` is either 1 (sequence) or DEFAULT_VECTOR_CAPACITY.

**Public Methods:**

- `ValueVector(LogicalType dataType, storage::MemoryManager *mem...`
- `ValueVector(LogicalTypeID dataTypeID, storage::MemoryManager ...`
- `DELETE_COPY_AND_MOVE(ValueVector)`
- `~ValueVector()=default`
- `firstNonNull() const`
- `forEachNonNull(Func &&func) const`
- `countNonNull() const`
- `setState(const std::shared_ptr< DataChunkState > &state_)`
- `setAllNull()`
- `setAllNonNull()`
- ... and 24 more methods


---

## VirtualFileSystem

**Full name:** `gs::common::VirtualFileSystem`

**Public Methods:**

- `VirtualFileSystem()`
- `VirtualFileSystem(std::string homeDir)`
- `~VirtualFileSystem() override`
- `registerFileSystem(std::unique_ptr< FileSystem > fileSystem)`
- `openFile(const std::string &path, FileOpenFlags flags, mai...`
- `glob(main::ClientContext *context, const std::string &...`
- `overwriteFile(const std::string &from, const std::string &to) override`
- `createDir(const std::string &dir) const override`
- `removeFileIfExists(const std::string &path) override`
- `fileOrPathExists(const std::string &path, main::ClientContext *con...`
- ... and 3 more methods


---

## WarningConstants

**Full name:** `gs::common::WarningConstants`


---

## Writer

**Full name:** `gs::common::Writer`

**Public Methods:**

- `write(const uint8_t *data, uint64_t size)=0`
- `~Writer()=default`


---

## blob_t

**Full name:** `gs::common::blob_t`


---

## date_t

**Full name:** `gs::common::date_t`

**Public Methods:**

- `date_t()`
- `date_t(int32_t days_p)`
- `operator==(const date_t &rhs) const`
- `operator!=(const date_t &rhs) const`
- `operator<=(const date_t &rhs) const`
- `operator<(const date_t &rhs) const`
- `operator>(const date_t &rhs) const`
- `operator>=(const date_t &rhs) const`
- `operator==(const timestamp_t &rhs) const`
- `operator!=(const timestamp_t &rhs) const`
- ... and 9 more methods


---

## dtime_t

**Full name:** `gs::common::dtime_t`

**Public Methods:**

- `dtime_t()`
- `dtime_t(int64_t micros_p)`
- `operator=(int64_t micros_p)`
- `operator int64_t() const`
- `operator double() const`
- `operator==(const dtime_t &rhs) const`
- `operator!=(const dtime_t &rhs) const`
- `operator<=(const dtime_t &rhs) const`
- `operator<(const dtime_t &rhs) const`
- `operator>(const dtime_t &rhs) const`
- ... and 1 more methods


---

## int128_t

**Full name:** `gs::common::int128_t`

**Public Methods:**

- `int128_t()=default`
- `int128_t(int64_t value)`
- `int128_t(int32_t value)`
- `int128_t(int16_t value)`
- `int128_t(int8_t value)`
- `int128_t(uint64_t value)`
- `int128_t(uint32_t value)`
- `int128_t(uint16_t value)`
- `int128_t(uint8_t value)`
- `int128_t(double value)`
- ... and 21 more methods


---

## internalID_t

**Full name:** `gs::common::internalID_t`

**Public Methods:**

- `internalID_t()`
- `internalID_t(offset_t offset, table_id_t tableID)`
- `operator==(const internalID_t &rhs) const`
- `operator!=(const internalID_t &rhs) const`
- `operator>(const internalID_t &rhs) const`
- `operator>=(const internalID_t &rhs) const`
- `operator<(const internalID_t &rhs) const`
- `operator<=(const internalID_t &rhs) const`


---

## interval_t

**Full name:** `gs::common::interval_t`

**Public Methods:**

- `interval_t()`
- `interval_t(int32_t months_p, int32_t days_p, int64_t micros_p)`
- `operator==(const interval_t &rhs) const`
- `operator!=(const interval_t &rhs) const`
- `operator>(const interval_t &rhs) const`
- `operator<=(const interval_t &rhs) const`
- `operator<(const interval_t &rhs) const`
- `operator>=(const interval_t &rhs) const`
- `operator+(const interval_t &rhs) const`
- `operator+(const timestamp_t &rhs) const`
- ... and 3 more methods


---

## neug_list_t

**Full name:** `gs::common::neug_list_t`

**Public Methods:**

- `neug_list_t()`
- `neug_list_t(uint64_t size, uint64_t overflowPtr)`
- `set(const uint8_t *values, const LogicalType &dataType) const`


---

## neug_string_t

**Full name:** `gs::common::neug_string_t`

**Public Methods:**

- `neug_string_t()`
- `neug_string_t(const char *value, uint64_t length)`
- `getData() const`
- `getDataUnsafe()`
- `set(const std::string &value)`
- `set(const char *value, uint64_t length)`
- `set(const neug_string_t &value)`
- `setShortString(const char *value, uint64_t length)`
- `setLongString(const char *value, uint64_t length)`
- `setShortString(const neug_string_t &value)`
- ... and 12 more methods


---

## neug_uuid_t

**Full name:** `gs::common::neug_uuid_t`


---

## list_entry_t

**Full name:** `gs::common::list_entry_t`

**Public Methods:**

- `list_entry_t()`
- `list_entry_t(offset_t offset, list_size_t size)`


---

## map_entry_t

**Full name:** `gs::common::map_entry_t`


---

## MakeSigned

**Full name:** `gs::common::numeric_utils::MakeSigned`


---

## MakeSigned< int128_t >

**Full name:** `gs::common::numeric_utils::MakeSigned< int128_t >`


---

## MakeUnSigned

**Full name:** `gs::common::numeric_utils::MakeUnSigned`


---

## MakeUnSigned< int128_t >

**Full name:** `gs::common::numeric_utils::MakeUnSigned< int128_t >`


---

## overflow_value_t

**Full name:** `gs::common::overflow_value_t`


---

## overload

**Full name:** `gs::common::overload`

**Public Methods:**

- `overload(Funcs... funcs)`


---

## struct_entry_t

**Full name:** `gs::common::struct_entry_t`


---

## timestamp_ms_t

**Full name:** `gs::common::timestamp_ms_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_ns_t

**Full name:** `gs::common::timestamp_ns_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_sec_t

**Full name:** `gs::common::timestamp_sec_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_t

**Full name:** `gs::common::timestamp_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`
- `operator=(int64_t value_p)`
- `operator int64_t() const`
- `operator==(const timestamp_t &rhs) const`
- `operator!=(const timestamp_t &rhs) const`
- `operator<=(const timestamp_t &rhs) const`
- `operator<(const timestamp_t &rhs) const`
- `operator>(const timestamp_t &rhs) const`
- `operator>=(const timestamp_t &rhs) const`
- ... and 9 more methods


---

## timestamp_tz_t

**Full name:** `gs::common::timestamp_tz_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## union_entry_t

**Full name:** `gs::common::union_entry_t`


---

## EvaluatorLocalState

**Full name:** `gs::evaluator::EvaluatorLocalState`


---

## ExpressionEvaluator

**Full name:** `gs::evaluator::ExpressionEvaluator`

**Public Methods:**

- `ExpressionEvaluator(EvaluatorType type, std::shared_ptr< binder::Expr...`
- `ExpressionEvaluator(EvaluatorType type, std::shared_ptr< binder::Expr...`
- `ExpressionEvaluator(EvaluatorType type, std::shared_ptr< binder::Expr...`
- `ExpressionEvaluator(const ExpressionEvaluator &other)`
- `~ExpressionEvaluator()=default`
- `getEvaluatorType() const`
- `getExpression() const`
- `isResultFlat() const`
- `getChildren() const`
- `init(const processor::ResultSet &resultSet, main::Clie...`
- ... and 7 more methods


---

## ExpressionEvaluatorUtils

**Full name:** `gs::evaluator::ExpressionEvaluatorUtils`

**Public Methods:**

- `evaluateConstantExpression(std::shared_ptr< binder::Expression > expression,...`


---

## FunctionExpressionEvaluator

**Full name:** `gs::evaluator::FunctionExpressionEvaluator`

**Public Methods:**

- `FunctionExpressionEvaluator(std::shared_ptr< binder::Expression > expression,...`
- `evaluate() override`
- `evaluate(common::sel_t count) override`
- `selectInternal(common::SelectionVector &selVector) override`
- `copy() override`


---

## LiteralExpressionEvaluator

**Full name:** `gs::evaluator::LiteralExpressionEvaluator`

**Public Methods:**

- `LiteralExpressionEvaluator(std::shared_ptr< binder::Expression > expression,...`
- `evaluate() override`
- `evaluate(common::sel_t count) override`
- `selectInternal(common::SelectionVector &selVector) override`
- `copy() override`


---

## BinderException

**Full name:** `gs::exception::BinderException`

**Public Methods:**

- `BinderException(const std::string &msg)`
- `BinderException(const std::string &msg, const std::string &file_line)`


---

## CatalogException

**Full name:** `gs::exception::CatalogException`

**Public Methods:**

- `CatalogException(const std::string &msg)`
- `CatalogException(const std::string &msg, const std::string &file_line)`


---

## CheckpointException

**Full name:** `gs::exception::CheckpointException`

**Public Methods:**

- `CheckpointException(const std::exception &e)`
- `CheckpointException(const std::string &msg)`


---

## ConnectionException

**Full name:** `gs::exception::ConnectionException`

**Public Methods:**

- `ConnectionException(const std::string &msg)`
- `ConnectionException(const std::string &msg, const std::string &file_line)`


---

## ConversionException

**Full name:** `gs::exception::ConversionException`

**Public Methods:**

- `ConversionException(const std::string &msg)`
- `ConversionException(const std::string &msg, const std::string &file_line)`


---

## CopyException

**Full name:** `gs::exception::CopyException`

**Public Methods:**

- `CopyException(const std::string &msg)`
- `CopyException(const std::string &msg, const std::string &file_line)`


---

## DatabaseLockedException

**Full name:** `gs::exception::DatabaseLockedException`

**Public Methods:**

- `DatabaseLockedException(const std::string &msg)`
- `DatabaseLockedException(const std::string &msg, const std::string &file_line)`


---

## Exception

**Full name:** `gs::exception::Exception`

**Public Methods:**

- `Exception(std::string msg, gs::StatusCode error_code)`
- `Exception(std::string msg, std::string file_line)`
- `Exception(std::string msg, std::string file_line, gs::Statu...`
- `what() const noexcept override`


---

## ExtensionException

**Full name:** `gs::exception::ExtensionException`

**Public Methods:**

- `ExtensionException(const std::string &msg)`
- `ExtensionException(const std::string &msg, const std::string &file_line)`


---

## IOException

**Full name:** `gs::exception::IOException`

**Public Methods:**

- `IOException(const std::string &msg)`
- `IOException(const std::string &msg, const std::string &file_line)`


---

## IndexException

**Full name:** `gs::exception::IndexException`

**Public Methods:**

- `IndexException(const std::string &msg)`
- `IndexException(const std::string &msg, const std::string &file_line)`


---

## InternalException

**Full name:** `gs::exception::InternalException`

**Public Methods:**

- `InternalException(const std::string &msg)`
- `InternalException(const std::string &msg, const std::string &file_line)`


---

## InterruptException

**Full name:** `gs::exception::InterruptException`

**Public Methods:**

- `InterruptException()`
- `InterruptException(const std::string &msg, const std::string &file_line)`


---

## InvalidArgumentException

**Full name:** `gs::exception::InvalidArgumentException`

**Public Methods:**

- `InvalidArgumentException(const std::string &msg)`
- `InvalidArgumentException(const std::string &msg, const std::string &file_line)`


---

## NotFoundException

**Full name:** `gs::exception::NotFoundException`

**Public Methods:**

- `NotFoundException(const std::string &msg)`
- `NotFoundException(const std::string &msg, const std::string &file_line)`


---

## NotImplementedException

**Full name:** `gs::exception::NotImplementedException`

**Public Methods:**

- `NotImplementedException(const std::string &msg)`
- `NotImplementedException(const std::string &msg, const std::string &file_line)`


---

## NotSupportedException

**Full name:** `gs::exception::NotSupportedException`

**Public Methods:**

- `NotSupportedException(const std::string &msg)`
- `NotSupportedException(const std::string &msg, const std::string &file_line)`


---

## OverflowException

**Full name:** `gs::exception::OverflowException`

**Public Methods:**

- `OverflowException(const std::string &msg)`
- `OverflowException(const std::string &msg, const std::string &file_line)`


---

## ParserException

**Full name:** `gs::exception::ParserException`

**Public Methods:**

- `ParserException(const std::string &msg)`
- `ParserException(const std::string &msg, const std::string &file_line)`


---

## PermissionDeniedException

**Full name:** `gs::exception::PermissionDeniedException`

**Public Methods:**

- `PermissionDeniedException(const std::string &msg)`
- `PermissionDeniedException(const std::string &msg, const std::string &file_line)`


---

## QueryExecutionError

**Full name:** `gs::exception::QueryExecutionError`

**Public Methods:**

- `QueryExecutionError(const std::string &msg)`
- `QueryExecutionError(const std::string &msg, const std::string &file_line)`


---

## RuntimeError

**Full name:** `gs::exception::RuntimeError`

**Public Methods:**

- `RuntimeError(const std::string &msg)`
- `RuntimeError(const std::string &msg, const std::string &file_line)`


---

## SchemaMismatchException

**Full name:** `gs::exception::SchemaMismatchException`

**Public Methods:**

- `SchemaMismatchException(const std::string &msg)`
- `SchemaMismatchException(const std::string &msg, const std::string &file_line)`


---

## StorageException

**Full name:** `gs::exception::StorageException`

**Public Methods:**

- `StorageException(const std::string &msg)`
- `StorageException(const std::string &msg, const std::string &file_line)`


---

## TestException

**Full name:** `gs::exception::TestException`

**Public Methods:**

- `TestException(const std::string &msg)`
- `TestException(const std::string &msg, const std::string &file_line)`


---

## TransactionManagerException

**Full name:** `gs::exception::TransactionManagerException`

**Public Methods:**

- `TransactionManagerException(const std::string &msg)`
- `TransactionManagerException(const std::string &msg, const std::string &file_line)`


---

## TxStateConflictException

**Full name:** `gs::exception::TxStateConflictException`

**Public Methods:**

- `TxStateConflictException(const std::string &msg)`
- `TxStateConflictException(const std::string &msg, const std::string &file_line)`


---

## CatalogExtension

**Full name:** `gs::extension::CatalogExtension`

**Public Methods:**

- `CatalogExtension()`
- `init()=0`
- `invalidateCache()`


---

## Extension

**Full name:** `gs::extension::Extension`

**Public Methods:**

- `~Extension()=default`


---

## ExtensionAuxInfo

**Full name:** `gs::extension::ExtensionAuxInfo`

**Public Methods:**

- `ExtensionAuxInfo(ExtensionAction action, std::string path)`
- `~ExtensionAuxInfo()=default`
- `contCast() const`
- `copy()`


---

## ExtensionEntry

**Full name:** `gs::extension::ExtensionEntry`


---

## ExtensionInstaller

**Full name:** `gs::extension::ExtensionInstaller`

**Public Methods:**

- `ExtensionInstaller(const InstallExtensionInfo &info, main::ClientCon...`
- `~ExtensionInstaller()=default`
- `install()`


---

## ExtensionLibLoader

**Full name:** `gs::extension::ExtensionLibLoader`

**Public Methods:**

- `ExtensionLibLoader(const std::string &extensionName, const std::string &path)`
- `getLoadFunc()`
- `getInitFunc()`
- `getNameFunc()`
- `getInstallFunc()`


---

## ExtensionLoader

**Full name:** `gs::extension::ExtensionLoader`

**Public Methods:**

- `ExtensionLoader(std::string extensionName)`
- `~ExtensionLoader()=default`
- `loadDependency(main::ClientContext *context)=0`


---

## ExtensionManager

**Full name:** `gs::extension::ExtensionManager`

**Public Methods:**

- `loadExtension(const std::string &path, main::ClientContext *context)`
- `toCypher()`
- `addExtensionOption(std::string name, common::LogicalTypeID type, com...`
- `getExtensionOption(std::string name) const`
- `getLoadedExtensions() const`
- `autoLoadLinkedExtensions(main::ClientContext *context)`
- `lookupExtensionsByFunctionName(std::string_view functionName)`
- `lookupExtensionsByTypeName(std::string_view typeName)`


---

## ExtensionRepoInfo

**Full name:** `gs::extension::ExtensionRepoInfo`


---

## ExtensionSourceUtils

**Full name:** `gs::extension::ExtensionSourceUtils`

**Public Methods:**

- `toString(ExtensionSource source)`


---

## ExtensionUtils

**Full name:** `gs::extension::ExtensionUtils`

**Public Methods:**

- `isFullPath(const std::string &extension)`
- `getExtensionLibRepoInfo(const std::string &extensionName, const std::stri...`
- `getExtensionLoaderRepoInfo(const std::string &extensionName, const std::stri...`
- `getExtensionInstallerRepoInfo(const std::string &extensionName, const std::stri...`
- `getSharedLibRepoInfo(const std::string &fileName, const std::string &e...`
- `getExtensionFileName(const std::string &name)`
- `getLocalPathForExtensionLib(main::ClientContext *context, const std::string &...`
- `getLocalPathForExtensionLoader(main::ClientContext *context, const std::string &...`
- `getLocalPathForExtensionInstaller(main::ClientContext *context, const std::string &...`
- `getLocalExtensionDir(main::ClientContext *context, const std::string &...`
- ... and 9 more methods


---

## InstallExtensionAuxInfo

**Full name:** `gs::extension::InstallExtensionAuxInfo`

**Public Methods:**

- `InstallExtensionAuxInfo(std::string extensionRepo, std::string path)`
- `copy() override`


---

## InstallExtensionInfo

**Full name:** `gs::extension::InstallExtensionInfo`


---

## LoadedExtension

**Full name:** `gs::extension::LoadedExtension`

**Public Methods:**

- `LoadedExtension(std::string extensionName, std::string fullPath, ...`
- `getExtensionName() const`
- `getFullPath() const`
- `getSource() const`
- `toCypher()`


---

## Abs

**Full name:** `gs::function::Abs`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(int8_t &input, int8_t &result)`
- `operation(int16_t &input, int16_t &result)`
- `operation(int32_t &input, int32_t &result)`
- `operation(int64_t &input, int64_t &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## AbsFunction

**Full name:** `gs::function::AbsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Acos

**Full name:** `gs::function::Acos`

**Public Methods:**

- `operation(T &input, double &result)`


---

## AcosFunction

**Full name:** `gs::function::AcosFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Add

**Full name:** `gs::function::Add`

**Public Methods:**

- `operation(A &left, B &right, R &result)`
- `operation(uint8_t &left, uint8_t &right, uint8_t &result)`
- `operation(uint16_t &left, uint16_t &right, uint16_t &result)`
- `operation(uint32_t &left, uint32_t &right, uint32_t &result)`
- `operation(uint64_t &left, uint64_t &right, uint64_t &result)`
- `operation(int8_t &left, int8_t &right, int8_t &result)`
- `operation(int16_t &left, int16_t &right, int16_t &result)`
- `operation(int32_t &left, int32_t &right, int32_t &result)`
- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## AddFunction

**Full name:** `gs::function::AddFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateAvgFunction

**Full name:** `gs::function::AggregateAvgFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateFunction

**Full name:** `gs::function::AggregateFunction`

**Public Methods:**

- `AggregateFunction(std::string name, std::vector< common::LogicalTyp...`
- `EXPLICIT_COPY_DEFAULT_MOVE(AggregateFunction)`
- `getAggregateStateSize() const`
- `getInitialNullAggregateState()`
- `createInitialNullAggregateState() const`
- `updateAllState(uint8_t *state, common::ValueVector *input, uint6...`
- `updatePosState(uint8_t *state, common::ValueVector *input, uint6...`
- `combineState(uint8_t *state, uint8_t *otherState, common::InMe...`
- `finalizeState(uint8_t *state) const`
- `isFunctionDistinct() const`


---

## AggregateFunctionUtils

**Full name:** `gs::function::AggregateFunctionUtils`

**Public Methods:**

- `getAggFunc(std::string name, common::LogicalTypeID inputType...`
- `appendSumOrAvgFuncs(std::string name, common::LogicalTypeID inputType...`


---

## AggregateMaxFunction

**Full name:** `gs::function::AggregateMaxFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateMinFunction

**Full name:** `gs::function::AggregateMinFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateState

**Full name:** `gs::function::AggregateState`

**Public Methods:**

- `getStateSize() const =0`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos)=0`
- `~AggregateState()=default`


---

## AggregateSumFunction

**Full name:** `gs::function::AggregateSumFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AllSPDestinationsFunction

**Full name:** `gs::function::AllSPDestinationsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## AllSPPathsFunction

**Full name:** `gs::function::AllSPPathsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## AllWeightedSPPathsFunction

**Full name:** `gs::function::AllWeightedSPPathsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## And

**Full name:** `gs::function::And`

AND operator Truth table:

left isLeftNull right isRightNull result
T F T F 1 T F F F 0 F F T F 0 F F F F 0
- T T F 2
- T F F 0 T F - T 2 F F - T 0
- T - T 2

**Public Methods:**

- `operation(bool left, bool right, uint8_t &result, bool isLe...`


---

## ArrayAppendFunction

**Full name:** `gs::function::ArrayAppendFunction`


---

## ArrayCatFunction

**Full name:** `gs::function::ArrayCatFunction`


---

## ArrayConcatFunction

**Full name:** `gs::function::ArrayConcatFunction`


---

## ArrayContainsFunction

**Full name:** `gs::function::ArrayContainsFunction`


---

## ArrayCosineSimilarity

**Full name:** `gs::function::ArrayCosineSimilarity`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ArrayCosineSimilarityFunction

**Full name:** `gs::function::ArrayCosineSimilarityFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayCrossProduct

**Full name:** `gs::function::ArrayCrossProduct`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ArrayCrossProductFunction

**Full name:** `gs::function::ArrayCrossProductFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayDistance

**Full name:** `gs::function::ArrayDistance`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ArrayDistanceFunction

**Full name:** `gs::function::ArrayDistanceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayDotProductFunction

**Full name:** `gs::function::ArrayDotProductFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayExtract

**Full name:** `gs::function::ArrayExtract`

**Public Methods:**

- `operation(common::neug_string_t &str, int64_t &idx, common::k...`
- `copySubstr(common::neug_string_t &src, int64_t start, int64_t ...`


---

## ArrayExtractFunction

**Full name:** `gs::function::ArrayExtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayHasFunction

**Full name:** `gs::function::ArrayHasFunction`


---

## ArrayIndexOfFunction

**Full name:** `gs::function::ArrayIndexOfFunction`


---

## ArrayInnerProduct

**Full name:** `gs::function::ArrayInnerProduct`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ArrayInnerProductFunction

**Full name:** `gs::function::ArrayInnerProductFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayPositionFunction

**Full name:** `gs::function::ArrayPositionFunction`


---

## ArrayPrependFunction

**Full name:** `gs::function::ArrayPrependFunction`


---

## ArrayPushBackFunction

**Full name:** `gs::function::ArrayPushBackFunction`


---

## ArrayPushFrontFunction

**Full name:** `gs::function::ArrayPushFrontFunction`


---

## ArraySliceFunction

**Full name:** `gs::function::ArraySliceFunction`


---

## ArraySquaredDistance

**Full name:** `gs::function::ArraySquaredDistance`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ArraySquaredDistanceFunction

**Full name:** `gs::function::ArraySquaredDistanceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayValueFunction

**Full name:** `gs::function::ArrayValueFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Asin

**Full name:** `gs::function::Asin`

**Public Methods:**

- `operation(T &input, double &result)`


---

## AsinFunction

**Full name:** `gs::function::AsinFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Atan

**Full name:** `gs::function::Atan`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Atan2

**Full name:** `gs::function::Atan2`

**Public Methods:**

- `operation(A &left, B &right, double &result)`


---

## Atan2Function

**Full name:** `gs::function::Atan2Function`

**Public Methods:**

- `getFunctionSet()`


---

## AtanFunction

**Full name:** `gs::function::AtanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AtomicObjectArray

**Full name:** `gs::function::AtomicObjectArray`

**Public Methods:**

- `AtomicObjectArray(const common::offset_t size, storage::MemoryManag...`
- `setRelaxed(common::offset_t pos, const T &value)`
- `getRelaxed(const common::offset_t pos)`
- `compare_exchange_strong_max(const common::offset_t src, const common::offset_t dest)`


---

## AvgFunction

**Full name:** `gs::function::AvgFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(AvgState< RESULT_TYPE > *state, common::ValueVect...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *state_)`


---

## AvgState

**Full name:** `gs::function::AvgState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`
- `finalize()`
- `finalize()`


---

## BFSGraphManager

**Full name:** `gs::function::BFSGraphManager`

**Public Methods:**

- `BFSGraphManager(common::table_id_map_t< common::offset_t > maxOff...`
- `getCurrentGraph() const`
- `switchToDense(processor::ExecutionContext *context, graph::Graph *graph)`


---

## BMInfoFunction

**Full name:** `gs::function::BMInfoFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BaseBFSGraph

**Full name:** `gs::function::BaseBFSGraph`

**Public Methods:**

- `BaseBFSGraph(storage::MemoryManager *mm)`
- `~BaseBFSGraph()=default`
- `pinTableID(common::table_id_t tableID)=0`
- `addParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `addSingleParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `tryAddParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `tryAddSingleParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `getParentListHead(common::offset_t offset)=0`
- `getParentListHead(common::nodeID_t nodeID)=0`
- `setParentList(common::offset_t offset, ParentList *parentList)=0`
- ... and 1 more methods


---

## BaseCountFunction

**Full name:** `gs::function::BaseCountFunction`

**Public Methods:**

- `initialize()`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## CountState

**Full name:** `gs::function::BaseCountFunction::CountState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`


---

## BaseListSortOperation

**Full name:** `gs::function::BaseListSortOperation`

**Public Methods:**

- `isAscOrder(const std::string &sortOrder)`
- `isNullFirst(const std::string &nullOrder)`
- `sortValues(common::list_entry_t &input, common::list_entry_t...`
- `setVectorRangeToNull(common::ValueVector &vector, uint64_t offset, uin...`


---

## BaseLowerUpperFunction

**Full name:** `gs::function::BaseLowerUpperFunction`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`
- `convertCharCase(char *result, const char *input, int32_t charPos,...`
- `convertCase(char *result, uint32_t len, char *input, bool toUpper)`
- `getResultLen(char *inputStr, uint32_t inputLen, bool isUpper)`


---

## BaseMapExtract

**Full name:** `gs::function::BaseMapExtract`

**Public Methods:**

- `operation(common::list_entry_t &resultEntry, common::ValueV...`


---

## BasePadOperation

**Full name:** `gs::function::BasePadOperation`

**Public Methods:**

- `operation(common::neug_string_t &src, int64_t count, common::...`
- `padCountChars(const uint32_t count, const char *data, const uin...`
- `insertPadding(uint32_t charCount, common::neug_string_t pad, std:...`


---

## BaseRegexpOperation

**Full name:** `gs::function::BaseRegexpOperation`

**Public Methods:**

- `parseCypherPattern(const std::string &pattern)`
- `copyToKuzuString(const std::string &value, common::neug_string_t &ku...`


---

## BaseStrOperation

**Full name:** `gs::function::BaseStrOperation`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`


---

## BinaryBooleanFunctionExecutor

**Full name:** `gs::function::BinaryBooleanFunctionExecutor`

Binary boolean function requires special executor implementation because it's truth table handles null differently (e.g.

NULL OR TRUE = TRUE). Note that unary boolean operation (currently only NOT) does not require special implementation because NOT NULL = NULL.

**Public Methods:**

- `executeOnValueNoNull(common::ValueVector &left, common::ValueVector &r...`
- `executeOnValue(common::ValueVector &left, common::ValueVector &r...`
- `executeBothFlat(common::ValueVector &left, common::SelectionVecto...`
- `executeFlatUnFlat(common::ValueVector &left, common::SelectionVecto...`
- `executeUnFlatFlat(common::ValueVector &left, common::SelectionVecto...`
- `executeBothUnFlat(common::ValueVector &left, common::SelectionVecto...`
- `execute(common::ValueVector &left, common::SelectionVecto...`
- `selectOnValue(common::ValueVector &left, common::ValueVector &r...`
- `selectBothFlat(common::ValueVector &left, common::ValueVector &right)`
- `selectFlatUnFlat(common::ValueVector &left, common::ValueVector &r...`
- ... and 3 more methods


---

## BinaryComparisonFunctionWrapper

**Full name:** `gs::function::BinaryComparisonFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryFunctionExecutor

**Full name:** `gs::function::BinaryFunctionExecutor`

**Public Methods:**

- `executeOnValue(common::ValueVector &left, common::ValueVector &r...`
- `getSelectedPositions(common::SelectionVector *leftSelVector, common::S...`
- `executeOnSelectedValues(common::ValueVector &left, common::SelectionVecto...`
- `executeSwitch(common::ValueVector &left, common::SelectionVecto...`
- `execute(common::ValueVector &left, common::SelectionVecto...`
- `selectOnValue(common::ValueVector &left, common::ValueVector &r...`
- `selectBothFlat(common::ValueVector &left, common::ValueVector &r...`
- `selectFlatUnFlat(common::ValueVector &left, common::ValueVector &r...`
- `selectUnFlatFlat(common::ValueVector &left, common::ValueVector &r...`
- `selectBothUnFlat(common::ValueVector &left, common::ValueVector &r...`
- ... and 2 more methods


---

## BinaryComparisonSelectWrapper

**Full name:** `gs::function::BinaryFunctionExecutor::BinaryComparisonSelectWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinarySelectWrapper

**Full name:** `gs::function::BinaryFunctionExecutor::BinarySelectWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinaryFunctionWrapper

**Full name:** `gs::function::BinaryFunctionWrapper`

Binary operator assumes function with null returns null.

This does NOT applies to binary boolean operations (e.g. AND, OR, XOR).

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryHashFunctionExecutor

**Full name:** `gs::function::BinaryHashFunctionExecutor`

**Public Methods:**

- `execute(const common::ValueVector &left, const common::Se...`


---

## BinaryListExtractFunctionWrapper

**Full name:** `gs::function::BinaryListExtractFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryListStructFunctionWrapper

**Full name:** `gs::function::BinaryListStructFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryMapCreationFunctionWrapper

**Full name:** `gs::function::BinaryMapCreationFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinarySelectWithBindDataWrapper

**Full name:** `gs::function::BinarySelectWithBindDataWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinaryStringFunctionWrapper

**Full name:** `gs::function::BinaryStringFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryUDFExecutor

**Full name:** `gs::function::BinaryUDFExecutor`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryUDFFunctionWrapper

**Full name:** `gs::function::BinaryUDFFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BitShiftLeft

**Full name:** `gs::function::BitShiftLeft`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitShiftLeftFunction

**Full name:** `gs::function::BitShiftLeftFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitShiftRight

**Full name:** `gs::function::BitShiftRight`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitShiftRightFunction

**Full name:** `gs::function::BitShiftRightFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseAnd

**Full name:** `gs::function::BitwiseAnd`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitwiseAndFunction

**Full name:** `gs::function::BitwiseAndFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseOr

**Full name:** `gs::function::BitwiseOr`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitwiseOrFunction

**Full name:** `gs::function::BitwiseOrFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseXor

**Full name:** `gs::function::BitwiseXor`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitwiseXorFunction

**Full name:** `gs::function::BitwiseXorFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BlobFunction

**Full name:** `gs::function::BlobFunction`


---

## BuiltInFunctionsUtils

**Full name:** `gs::function::BuiltInFunctionsUtils`

**Public Methods:**

- `matchFunction(const std::string &name, const catalog::FunctionC...`
- `matchFunction(const std::string &name, const std::vector< commo...`
- `matchAggregateFunction(const std::string &name, const std::vector< commo...`
- `getCastCost(common::LogicalTypeID inputTypeID, common::Logica...`


---

## CardinalityFunction

**Full name:** `gs::function::CardinalityFunction`


---

## CastAnyFunction

**Full name:** `gs::function::CastAnyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastArrayHelper

**Full name:** `gs::function::CastArrayHelper`

**Public Methods:**

- `checkCompatibleNestedTypes(LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID)`
- `containsListToArray(const LogicalType &srcType, const LogicalType &dstType)`
- `validateListEntry(ValueVector *inputVector, const LogicalType &resu...`


---

## CastBetweenDecimal

**Full name:** `gs::function::CastBetweenDecimal`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &input...`


---

## CastBetweenTimestamp

**Full name:** `gs::function::CastBetweenTimestamp`

**Public Methods:**

- `operation(const SRC_TYPE &input, DST_TYPE &result)`
- `operation(const common::timestamp_t &input, common::timesta...`
- `operation(const common::timestamp_t &input, common::timesta...`
- `operation(const common::timestamp_t &input, common::timesta...`
- `operation(const common::timestamp_ms_t &input, common::time...`
- `operation(const common::timestamp_ms_t &input, common::time...`
- `operation(const common::timestamp_ms_t &input, common::time...`
- `operation(const common::timestamp_ns_t &input, common::time...`
- `operation(const common::timestamp_ns_t &input, common::time...`
- `operation(const common::timestamp_ns_t &input, common::time...`
- ... and 3 more methods


---

## CastDateToTimestamp

**Full name:** `gs::function::CastDateToTimestamp`

**Public Methods:**

- `operation(common::date_t &input, T &result)`
- `operation(common::date_t &input, common::timestamp_ns_t &result)`
- `operation(common::date_t &input, common::timestamp_ms_t &result)`
- `operation(common::date_t &input, common::timestamp_sec_t &result)`


---

## CastDecimalTo

**Full name:** `gs::function::CastDecimalTo`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &input...`
- `operation(int16_t &input, neug_string_t &output, const ValueV...`
- `operation(int32_t &input, neug_string_t &output, const ValueV...`
- `operation(int64_t &input, neug_string_t &output, const ValueV...`
- `operation(common::int128_t &input, neug_string_t &output, con...`


---

## CastFunction

**Full name:** `gs::function::CastFunction`

**Public Methods:**

- `hasImplicitCast(const common::LogicalType &srcType, const common:...`
- `bindCastFunction(const std::string &functionName, const common::Lo...`


---

## CastFunctionBindData

**Full name:** `gs::function::CastFunctionBindData`

**Public Methods:**

- `CastFunctionBindData(common::LogicalType dataType)`
- `copy() const override`


---

## CastNodeToString

**Full name:** `gs::function::CastNodeToString`

**Public Methods:**

- `operation(common::struct_entry_t &input, common::neug_string_...`


---

## CastRelToString

**Full name:** `gs::function::CastRelToString`

**Public Methods:**

- `operation(common::struct_entry_t &input, common::neug_string_...`


---

## CastString

**Full name:** `gs::function::CastString`

**Public Methods:**

- `copyStringToVector(ValueVector *vector, uint64_t vectorPos, std::str...`
- `tryCast(const neug_string_t &input, T &result)`
- `operation(const neug_string_t &input, T &result, ValueVector ...`
- `operation(const neug_string_t &input, int128_t &result, Value...`
- `operation(const neug_string_t &input, int32_t &result, ValueV...`
- `operation(const neug_string_t &input, int16_t &result, ValueV...`
- `operation(const neug_string_t &input, int8_t &result, ValueVe...`
- `operation(const neug_string_t &input, uint64_t &result, Value...`
- `operation(const neug_string_t &input, uint32_t &result, Value...`
- `operation(const neug_string_t &input, uint16_t &result, Value...`
- ... and 17 more methods


---

## CastToBlobFunction

**Full name:** `gs::function::CastToBlobFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToBoolFunction

**Full name:** `gs::function::CastToBoolFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToDate

**Full name:** `gs::function::CastToDate`

**Public Methods:**

- `operation(T &input, common::date_t &result)`
- `operation(common::timestamp_t &input, common::date_t &result)`
- `operation(common::timestamp_ns_t &input, common::date_t &result)`
- `operation(common::timestamp_ms_t &input, common::date_t &result)`
- `operation(common::timestamp_sec_t &input, common::date_t &result)`


---

## CastToDateFunction

**Full name:** `gs::function::CastToDateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToDecimal

**Full name:** `gs::function::CastToDecimal`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &, con...`
- `operation(neug_string_t &input, int16_t &output, const ValueV...`
- `operation(neug_string_t &input, int32_t &output, const ValueV...`
- `operation(neug_string_t &input, int64_t &output, const ValueV...`
- `operation(neug_string_t &input, common::int128_t &output, con...`


---

## CastToDouble

**Full name:** `gs::function::CastToDouble`

**Public Methods:**

- `operation(T &input, double &result)`
- `operation(common::int128_t &input, double &result)`


---

## CastToDoubleFunction

**Full name:** `gs::function::CastToDoubleFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToFloat

**Full name:** `gs::function::CastToFloat`

**Public Methods:**

- `operation(T &input, float &result)`
- `operation(common::int128_t &input, float &result)`


---

## CastToFloatFunction

**Full name:** `gs::function::CastToFloatFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToInt128

**Full name:** `gs::function::CastToInt128`

**Public Methods:**

- `operation(T &input, common::int128_t &result)`


---

## CastToInt128Function

**Full name:** `gs::function::CastToInt128Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToInt16

**Full name:** `gs::function::CastToInt16`

**Public Methods:**

- `operation(T &input, int16_t &result)`
- `operation(common::int128_t &input, int16_t &result)`


---

## CastToInt16Function

**Full name:** `gs::function::CastToInt16Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToInt32

**Full name:** `gs::function::CastToInt32`

**Public Methods:**

- `operation(T &input, int32_t &result)`
- `operation(common::int128_t &input, int32_t &result)`


---

## CastToInt32Function

**Full name:** `gs::function::CastToInt32Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToInt64

**Full name:** `gs::function::CastToInt64`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::int128_t &input, int64_t &result)`


---

## CastToInt64Function

**Full name:** `gs::function::CastToInt64Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToInt8

**Full name:** `gs::function::CastToInt8`

**Public Methods:**

- `operation(T &input, int8_t &result)`
- `operation(common::int128_t &input, int8_t &result)`


---

## CastToInt8Function

**Full name:** `gs::function::CastToInt8Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToIntervalFunction

**Full name:** `gs::function::CastToIntervalFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToSerial

**Full name:** `gs::function::CastToSerial`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::int128_t &input, int64_t &result)`


---

## CastToSerialFunction

**Full name:** `gs::function::CastToSerialFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToString

**Full name:** `gs::function::CastToString`

**Public Methods:**

- `operation(T &input, common::neug_string_t &result, common::Va...`


---

## CastToStringFunction

**Full name:** `gs::function::CastToStringFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToTimestampFunction

**Full name:** `gs::function::CastToTimestampFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUInt16

**Full name:** `gs::function::CastToUInt16`

**Public Methods:**

- `operation(T &input, uint16_t &result)`
- `operation(common::int128_t &input, uint16_t &result)`


---

## CastToUInt16Function

**Full name:** `gs::function::CastToUInt16Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUInt32

**Full name:** `gs::function::CastToUInt32`

**Public Methods:**

- `operation(T &input, uint32_t &result)`
- `operation(common::int128_t &input, uint32_t &result)`


---

## CastToUInt32Function

**Full name:** `gs::function::CastToUInt32Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUInt64

**Full name:** `gs::function::CastToUInt64`

**Public Methods:**

- `operation(T &input, uint64_t &result)`
- `operation(common::int128_t &input, uint64_t &result)`


---

## CastToUInt64Function

**Full name:** `gs::function::CastToUInt64Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUInt8

**Full name:** `gs::function::CastToUInt8`

**Public Methods:**

- `operation(T &input, uint8_t &result)`
- `operation(common::int128_t &input, uint8_t &result)`


---

## CastToUInt8Function

**Full name:** `gs::function::CastToUInt8Function`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUUIDFunction

**Full name:** `gs::function::CastToUUIDFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CatalogVersionFunction

**Full name:** `gs::function::CatalogVersionFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Cbrt

**Full name:** `gs::function::Cbrt`

**Public Methods:**

- `operation(T &input, double &result)`


---

## CbrtFunction

**Full name:** `gs::function::CbrtFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Ceil

**Full name:** `gs::function::Ceil`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## CeilFunction

**Full name:** `gs::function::CeilFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CeilingFunction

**Full name:** `gs::function::CeilingFunction`


---

## Century

**Full name:** `gs::function::Century`

**Public Methods:**

- `operation(common::timestamp_t &timestamp, int64_t &result)`


---

## CenturyFunction

**Full name:** `gs::function::CenturyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ClearWarningsFunction

**Full name:** `gs::function::ClearWarningsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CoalesceFunction

**Full name:** `gs::function::CoalesceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CollectFunction

**Full name:** `gs::function::CollectFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CombineHash

**Full name:** `gs::function::CombineHash`

**Public Methods:**

- `operation(const common::hash_t &left, const common::hash_t ...`


---

## ComparisonFunction

**Full name:** `gs::function::ComparisonFunction`

**Public Methods:**

- `getFunctionSet(const std::string &name)`


---

## ConcatFunction

**Full name:** `gs::function::ConcatFunction`

**Public Methods:**

- `execFunc(const std::vector< std::shared_ptr< common::Value...`
- `getFunctionSet()`


---

## ConstFunctionExecutor

**Full name:** `gs::function::ConstFunctionExecutor`

**Public Methods:**

- `execute(common::ValueVector &result, common::SelectionVector &sel)`


---

## ConstantOrNullFunction

**Full name:** `gs::function::ConstantOrNullFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Contains

**Full name:** `gs::function::Contains`

**Public Methods:**

- `operation(common::neug_string_t &left, common::neug_string_t &r...`


---

## ContainsFunction

**Full name:** `gs::function::ContainsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Cos

**Full name:** `gs::function::Cos`

**Public Methods:**

- `operation(T &input, double &result)`


---

## CosFunction

**Full name:** `gs::function::CosFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CostFunction

**Full name:** `gs::function::CostFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Cot

**Full name:** `gs::function::Cot`

**Public Methods:**

- `operation(T &input, double &result)`


---

## CotFunction

**Full name:** `gs::function::CotFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CountFunction

**Full name:** `gs::function::CountFunction`

**Public Methods:**

- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *, uint64_t ...`
- `paramRewriteFunc(binder::expression_vector &arguments)`
- `getFunctionSet()`


---

## CountIfFunction

**Full name:** `gs::function::CountIfFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CountStarFunction

**Full name:** `gs::function::CountStarFunction`

**Public Methods:**

- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `getFunctionSet()`


---

## CreateProjectedGraphFunction

**Full name:** `gs::function::CreateProjectedGraphFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CurrValFunction

**Full name:** `gs::function::CurrValFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CurrentDate

**Full name:** `gs::function::CurrentDate`

**Public Methods:**

- `operation(common::date_t &result, void *dataPtr)`


---

## CurrentDateFunction

**Full name:** `gs::function::CurrentDateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CurrentSettingFunction

**Full name:** `gs::function::CurrentSettingFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CurrentTimestamp

**Full name:** `gs::function::CurrentTimestamp`

**Public Methods:**

- `operation(common::timestamp_tz_t &result, void *dataPtr)`


---

## CurrentTimestampFunction

**Full name:** `gs::function::CurrentTimestampFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DBVersionFunction

**Full name:** `gs::function::DBVersionFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DateFunction

**Full name:** `gs::function::DateFunction`


---

## DatePart

**Full name:** `gs::function::DatePart`

**Public Methods:**

- `operation(LEFT_TYPE &, RIGHT_TYPE &, int64_t &)`
- `operation(common::neug_string_t &partSpecifier, common::date_...`
- `operation(common::neug_string_t &partSpecifier, common::times...`
- `operation(common::neug_string_t &partSpecifier, common::inter...`


---

## DatePartFunction

**Full name:** `gs::function::DatePartFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DatePartFunctionAlias

**Full name:** `gs::function::DatePartFunctionAlias`


---

## DateTrunc

**Full name:** `gs::function::DateTrunc`

**Public Methods:**

- `operation(LEFT_TYPE &, RIGHT_TYPE &, RIGHT_TYPE &)`
- `operation(common::neug_string_t &partSpecifier, common::date_...`
- `operation(common::neug_string_t &partSpecifier, common::times...`


---

## DateTruncFunction

**Full name:** `gs::function::DateTruncFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DateTruncFunctionAlias

**Full name:** `gs::function::DateTruncFunctionAlias`


---

## DayName

**Full name:** `gs::function::DayName`

**Public Methods:**

- `operation(T &, common::neug_string_t &)`
- `operation(common::date_t &input, common::neug_string_t &result)`
- `operation(common::timestamp_t &input, common::neug_string_t &result)`


---

## DayNameFunction

**Full name:** `gs::function::DayNameFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Decode

**Full name:** `gs::function::Decode`

**Public Methods:**

- `operation(common::blob_t &input, common::neug_string_t &resul...`


---

## DecodeFunctions

**Full name:** `gs::function::DecodeFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## DefaultRJAlgorithm

**Full name:** `gs::function::DefaultRJAlgorithm`

**Public Methods:**

- `getFunctionName() const override`
- `getResultColumns(const RJBindData &bindData) const override`
- `copy() const override`


---

## Degrees

**Full name:** `gs::function::Degrees`

**Public Methods:**

- `operation(T &input, double &result)`


---

## DegreesFunction

**Full name:** `gs::function::DegreesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DenseBFSGraph

**Full name:** `gs::function::DenseBFSGraph`

**Public Methods:**

- `DenseBFSGraph(storage::MemoryManager *mm, common::table_id_map_...`
- `init(processor::ExecutionContext *context, graph::Graph *graph)`
- `pinTableID(common::table_id_t tableID) override`
- `addParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `addSingleParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `tryAddParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `tryAddSingleParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `getParentListHead(common::offset_t offset) override`
- `getParentListHead(common::nodeID_t nodeID) override`
- `setParentList(common::offset_t offset, ParentList *parentList) override`


---

## DenseFrontier

**Full name:** `gs::function::DenseFrontier`

**Public Methods:**

- `DenseFrontier(const common::table_id_map_t< common::offset_t > ...`
- `DenseFrontier(const DenseFrontier &other)=delete`
- `DenseFrontier(const DenseFrontier &&other)=delete`
- `init(processor::ExecutionContext *context, graph::Grap...`
- `resetValue(processor::ExecutionContext *context, graph::Grap...`
- `pinTableID(common::table_id_t tableID) override`
- `addNode(common::nodeID_t nodeID, iteration_t iter) override`
- `addNode(common::offset_t offset, iteration_t iter) override`
- `addNodes(const std::vector< common::nodeID_t > &nodeIDs, i...`
- `getIteration(common::offset_t offset) const override`
- ... and 4 more methods


---

## DenseFrontierPair

**Full name:** `gs::function::DenseFrontierPair`

**Public Methods:**

- `DenseFrontierPair(std::unique_ptr< DenseFrontier > curDenseFrontier...`
- `beginNewIterationInternalNoLock() override`
- `getActiveNodesOnCurrentFrontier() override`
- `resetValue(processor::ExecutionContext *context, graph::Grap...`
- `getState() const override`
- `needSwitchToDense(uint64_t) const override`
- `switchToDense(processor::ExecutionContext *, graph::Graph *) override`


---

## DenseFrontierReference

**Full name:** `gs::function::DenseFrontierReference`

**Public Methods:**

- `DenseFrontierReference(const DenseFrontier &denseFrontier)`
- `pinTableID(common::table_id_t tableID) override`
- `addNode(common::nodeID_t nodeID, iteration_t iter) override`
- `addNode(common::offset_t offset, iteration_t iter) override`
- `addNodes(const std::vector< common::nodeID_t > &nodeIDs, i...`
- `getIteration(common::offset_t offset) const override`


---

## DenseSparseDynamicFrontierPair

**Full name:** `gs::function::DenseSparseDynamicFrontierPair`

**Public Methods:**

- `DenseSparseDynamicFrontierPair(std::unique_ptr< DenseFrontier > curDenseFrontier...`
- `beginNewIterationInternalNoLock() override`
- `getActiveNodesOnCurrentFrontier() override`
- `getState() const override`
- `needSwitchToDense(uint64_t threshold) const override`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`


---

## Divide

**Full name:** `gs::function::Divide`

**Public Methods:**

- `operation(A &left, B &right, R &result)`
- `operation(uint8_t &left, uint8_t &right, uint8_t &result)`
- `operation(uint16_t &left, uint16_t &right, uint16_t &result)`
- `operation(uint32_t &left, uint32_t &right, uint32_t &result)`
- `operation(uint64_t &left, uint64_t &right, uint64_t &result)`
- `operation(int8_t &left, int8_t &right, int8_t &result)`
- `operation(int16_t &left, int16_t &right, int16_t &result)`
- `operation(int32_t &left, int32_t &right, int32_t &result)`
- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## DivideFunction

**Full name:** `gs::function::DivideFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DropProjectedGraphFunction

**Full name:** `gs::function::DropProjectedGraphFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DurationFunction

**Full name:** `gs::function::DurationFunction`


---

## EdgeCompute

**Full name:** `gs::function::EdgeCompute`

Base interface for algorithms that can be implemented in Pregel-like vertex-centric manner or more specifically Ligra's edgeCompute (called edgeUpdate in Ligra paper) function.

Intended to be passed to the helper functions in `GDSUtils` that parallelize such Pregel-like computations.

**Public Methods:**

- `~EdgeCompute()=default`
- `edgeCompute(common::nodeID_t boundNodeID, graph::NbrScanState...`
- `resetSingleThreadState()`
- `terminate(common::NodeOffsetMaskMap &)`
- `copy()=0`


---

## ElementAtFunctions

**Full name:** `gs::function::ElementAtFunctions`


---

## EmptyGDSAuxiliaryState

**Full name:** `gs::function::EmptyGDSAuxiliaryState`

**Public Methods:**

- `EmptyGDSAuxiliaryState()=default`
- `beginFrontierCompute(common::table_id_t, common::table_id_t) override`
- `switchToDense(processor::ExecutionContext *, graph::Graph *) override`


---

## Encode

**Full name:** `gs::function::Encode`

**Public Methods:**

- `operation(common::neug_string_t &input, common::blob_t &resul...`


---

## EncodeFunctions

**Full name:** `gs::function::EncodeFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## EndNodeFunction

**Full name:** `gs::function::EndNodeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## EndsWith

**Full name:** `gs::function::EndsWith`

**Public Methods:**

- `operation(common::neug_string_t &left, common::neug_string_t &r...`


---

## EndsWithFunction

**Full name:** `gs::function::EndsWithFunction`

**Public Methods:**

- `getFunctionSet()`


---

## EpochMs

**Full name:** `gs::function::EpochMs`

**Public Methods:**

- `operation(int64_t &ms, common::timestamp_t &result)`


---

## EpochMsFunction

**Full name:** `gs::function::EpochMsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Equals

**Full name:** `gs::function::Equals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`
- `operation(const common::list_entry_t &left, const common::l...`
- `operation(const common::struct_entry_t &left, const common:...`


---

## EqualsFunction

**Full name:** `gs::function::EqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ErrorFunction

**Full name:** `gs::function::ErrorFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Even

**Full name:** `gs::function::Even`

**Public Methods:**

- `operation(T &input, double &result)`


---

## EvenFunction

**Full name:** `gs::function::EvenFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ExportCSVBindData

**Full name:** `gs::function::ExportCSVBindData`

**Public Methods:**

- `ExportCSVBindData(std::vector< std::string > names, std::string fil...`
- `copy() const override`


---

## ExportCSVFunction

**Full name:** `gs::function::ExportCSVFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ExportFuncBindData

**Full name:** `gs::function::ExportFuncBindData`

**Public Methods:**

- `ExportFuncBindData(std::vector< std::string > columnNames, std::stri...`
- `~ExportFuncBindData()=default`
- `setDataType(std::vector< common::LogicalType > types_)`
- `constCast() const`
- `ptrCast()`
- `copy() const =0`


---

## ExportFuncBindInput

**Full name:** `gs::function::ExportFuncBindInput`


---

## ExportFuncLocalState

**Full name:** `gs::function::ExportFuncLocalState`

**Public Methods:**

- `~ExportFuncLocalState()=default`
- `cast()`


---

## ExportFuncSharedState

**Full name:** `gs::function::ExportFuncSharedState`

**Public Methods:**

- `~ExportFuncSharedState()=default`
- `cast()`
- `init(main::ClientContext &context, const ExportFuncBin...`


---

## ExportFunction

**Full name:** `gs::function::ExportFunction`

**Public Methods:**

- `ExportFunction(std::string name)`
- `ExportFunction(std::string name, export_init_local_t initLocal, ...`


---

## ExportParquetFunction

**Full name:** `gs::function::ExportParquetFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ExtraScanTableFuncBindInput

**Full name:** `gs::function::ExtraScanTableFuncBindInput`


---

## ExtraTableFuncBindInput

**Full name:** `gs::function::ExtraTableFuncBindInput`

**Public Methods:**

- `~ExtraTableFuncBindInput()=default`
- `constPtrCast() const`


---

## Factorial

**Full name:** `gs::function::Factorial`

**Public Methods:**

- `operation(int64_t &input, int64_t &result)`


---

## FactorialFunction

**Full name:** `gs::function::FactorialFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Find

**Full name:** `gs::function::Find`

**Public Methods:**

- `operation(common::neug_string_t &left, common::neug_string_t &r...`


---

## Floor

**Full name:** `gs::function::Floor`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## FloorFunction

**Full name:** `gs::function::FloorFunction`

**Public Methods:**

- `getFunctionSet()`


---

## FreeSpaceInfoFunction

**Full name:** `gs::function::FreeSpaceInfoFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Frontier

**Full name:** `gs::function::Frontier`

**Public Methods:**

- `~Frontier()=default`
- `pinTableID(common::table_id_t tableID)=0`
- `addNode(common::nodeID_t nodeID, iteration_t iter)=0`
- `addNode(common::offset_t offset, iteration_t iter)=0`
- `addNodes(const std::vector< common::nodeID_t > &nodeIDs, i...`
- `getIteration(common::offset_t offset) const =0`
- `cast()`


---

## FrontierMorsel

**Full name:** `gs::function::FrontierMorsel`

**Public Methods:**

- `FrontierMorsel()=default`
- `getBeginOffset() const`
- `getEndOffset() const`
- `init(common::offset_t beginOffset_, common::offset_t endOffset_)`


---

## FrontierMorselDispatcher

**Full name:** `gs::function::FrontierMorselDispatcher`

**Public Methods:**

- `FrontierMorselDispatcher(uint64_t maxThreads)`
- `init(common::offset_t _maxOffset)`
- `getNextRangeMorsel(FrontierMorsel &frontierMorsel)`


---

## FrontierPair

**Full name:** `gs::function::FrontierPair`

**Public Methods:**

- `FrontierPair()`
- `~FrontierPair()=default`
- `resetCurrentIter()`
- `getCurrentIter() const`
- `setActiveNodesForNextIter()`
- `continueNextIter(uint16_t maxIter)`
- `beginNewIteration()`
- `pinCurrentFrontier(common::table_id_t tableID)`
- `pinNextFrontier(common::table_id_t tableID)`
- `beginFrontierComputeBetweenTables(common::table_id_t curTableID, common::table_id_t...`
- ... and 10 more methods


---

## FrontierTask

**Full name:** `gs::function::FrontierTask`

**Public Methods:**

- `FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo &i...`
- `run() override`
- `runSparse()`


---

## FrontierTaskInfo

**Full name:** `gs::function::FrontierTaskInfo`

**Public Methods:**

- `FrontierTaskInfo(catalog::TableCatalogEntry *boundEntry, catalog::...`
- `FrontierTaskInfo(const FrontierTaskInfo &other)`


---

## FrontierTaskSharedState

**Full name:** `gs::function::FrontierTaskSharedState`

**Public Methods:**

- `FrontierTaskSharedState(uint64_t maxNumThreads, FrontierPair &frontierPair)`
- `DELETE_COPY_AND_MOVE(FrontierTaskSharedState)`


---

## Function

**Full name:** `gs::function::Function`

**Public Methods:**

- `Function()`
- `Function(std::string name, std::vector< common::LogicalTyp...`
- `Function(const Function &)=default`
- `~Function()=default`
- `signatureToString() const`
- `constPtrCast() const`
- `ptrCast()`


---

## FunctionBindData

**Full name:** `gs::function::FunctionBindData`

**Public Methods:**

- `FunctionBindData(common::LogicalType dataType)`
- `FunctionBindData(std::vector< common::LogicalType > paramTypes, co...`
- `DELETE_COPY_AND_MOVE(FunctionBindData)`
- `~FunctionBindData()=default`
- `cast()`
- `copy() const`
- `getSimpleBindData(const binder::expression_vector &params, const co...`


---

## FunctionCollection

**Full name:** `gs::function::FunctionCollection`

**Public Methods:**

- `getFunctions()`


---

## FunctionSignatureRegistry

**Full name:** `gs::function::FunctionSignatureRegistry`

**Public Methods:**

- `registerScalar(const std::string &signature, const runtime::neug...`
- `lookup(const std::string &signature)`
- `printAllSignatures()`


---

## FunctionStringBindData

**Full name:** `gs::function::FunctionStringBindData`

**Public Methods:**

- `FunctionStringBindData(std::string str)`
- `copy() const override`


---

## GDSAuxiliaryState

**Full name:** `gs::function::GDSAuxiliaryState`

**Public Methods:**

- `GDSAuxiliaryState()=default`
- `~GDSAuxiliaryState()=default`
- `initSource(common::nodeID_t)`
- `beginFrontierCompute(common::table_id_t fromTableID, common::table_id_...`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`
- `ptrCast()`


---

## GDSBindData

**Full name:** `gs::function::GDSBindData`

**Public Methods:**

- `GDSBindData(binder::expression_vector columns, graph::GraphEn...`
- `GDSBindData(const GDSBindData &other)`
- `getConfig() const`
- `copy() const override`


---

## GDSComputeState

**Full name:** `gs::function::GDSComputeState`

**Public Methods:**

- `GDSComputeState(std::shared_ptr< FrontierPair > frontierPair, std...`
- `initSource(common::nodeID_t sourceNodeID) const`
- `beginFrontierCompute(common::table_id_t currTableID, common::table_id_...`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`


---

## GDSConfig

**Full name:** `gs::function::GDSConfig`

**Public Methods:**

- `~GDSConfig()=default`
- `constCast() const`


---

## GDSDenseObjectManager

**Full name:** `gs::function::GDSDenseObjectManager`

**Public Methods:**

- `allocate(common::table_id_t tableID, common::offset_t maxO...`
- `getData(common::table_id_t tableID) const`


---

## GDSFuncSharedState

**Full name:** `gs::function::GDSFuncSharedState`

**Public Methods:**

- `GDSFuncSharedState(std::unique_ptr< graph::Graph > graph)`
- `setGraphNodeMask(std::unique_ptr< common::NodeOffsetMaskMap > maskMap)`
- `getGraphNodeMaskMap() const`


---

## GDSFunction

**Full name:** `gs::function::GDSFunction`

**Public Methods:**

- `bindGraphEntry(main::ClientContext &context, const std::string &name)`
- `bindGraphEntry(main::ClientContext &context, const graph::Parsed...`
- `bindNodeOutput(const TableFuncBindInput &bindInput, const std::v...`
- `bindColumnName(const parser::YieldVariable &yieldVariable, std::...`
- `initSharedState(const TableFuncInitSharedStateInput &input)`
- `getLogicalPlan(planner::Planner *planner, const binder::BoundRea...`


---

## GDSOptionalParams

**Full name:** `gs::function::GDSOptionalParams`

**Public Methods:**

- `~GDSOptionalParams()=default`
- `getConfig() const =0`
- `copy() const =0`


---

## GDSResultVertexCompute

**Full name:** `gs::function::GDSResultVertexCompute`

**Public Methods:**

- `GDSResultVertexCompute(storage::MemoryManager *mm, GDSFuncSharedState *s...`
- `~GDSResultVertexCompute() override`


---

## GDSSpareObjectManager

**Full name:** `gs::function::GDSSpareObjectManager`

**Public Methods:**

- `GDSSpareObjectManager(const common::table_id_map_t< common::offset_t > ...`
- `allocate(common::table_id_t tableID)`
- `getData()`
- `getMap(common::table_id_t tableID)`
- `getData(common::table_id_t tableID)`
- `size() const`


---

## GDSUtils

**Full name:** `gs::function::GDSUtils`

**Public Methods:**

- `runAlgorithmEdgeCompute(processor::ExecutionContext *context, GDSComputeS...`
- `runFTSEdgeCompute(processor::ExecutionContext *context, GDSComputeS...`
- `runRecursiveJoinEdgeCompute(processor::ExecutionContext *context, GDSComputeS...`
- `runVertexCompute(processor::ExecutionContext *context, GDSDensityS...`
- `runVertexCompute(processor::ExecutionContext *context, GDSDensityS...`
- `runVertexCompute(processor::ExecutionContext *context, GDSDensityS...`


---

## GDSVertexCompute

**Full name:** `gs::function::GDSVertexCompute`

**Public Methods:**

- `GDSVertexCompute(common::NodeOffsetMaskMap *nodeMask)`
- `beginOnTable(common::table_id_t tableID) override`


---

## Gamma

**Full name:** `gs::function::Gamma`

**Public Methods:**

- `operation(T &input, T &result)`


---

## GammaFunction

**Full name:** `gs::function::GammaFunction`

**Public Methods:**

- `getFunctionSet()`


---

## GenRandomUUID

**Full name:** `gs::function::GenRandomUUID`

**Public Methods:**

- `operation(common::neug_uuid_t &input, void *dataPtr)`


---

## GenRandomUUIDFunction

**Full name:** `gs::function::GenRandomUUIDFunction`

**Public Methods:**

- `getFunctionSet()`


---

## GreaterThan

**Full name:** `gs::function::GreaterThan`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`
- `operation(const common::list_entry_t &left, const common::l...`
- `operation(const common::struct_entry_t &left, const common:...`


---

## GreaterThanEquals

**Full name:** `gs::function::GreaterThanEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## GreaterThanEqualsFunction

**Full name:** `gs::function::GreaterThanEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## GreaterThanFunction

**Full name:** `gs::function::GreaterThanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Greatest

**Full name:** `gs::function::Greatest`

**Public Methods:**

- `operation(T &left, T &right, T &result)`


---

## GreatestFunction

**Full name:** `gs::function::GreatestFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Hash

**Full name:** `gs::function::Hash`

**Public Methods:**

- `operation(const T &, common::hash_t &)`
- `operation(const T &key, bool isNull, common::hash_t &result)`
- `operation(const common::internalID_t &key, common::hash_t &result)`
- `operation(const bool &key, common::hash_t &result)`
- `operation(const uint8_t &key, common::hash_t &result)`
- `operation(const uint16_t &key, common::hash_t &result)`
- `operation(const uint32_t &key, common::hash_t &result)`
- `operation(const uint64_t &key, common::hash_t &result)`
- `operation(const int64_t &key, common::hash_t &result)`
- `operation(const int32_t &key, common::hash_t &result)`
- ... and 10 more methods


---

## HashFunction

**Full name:** `gs::function::HashFunction`

**Public Methods:**

- `getFunctionSet()`


---

## IDFunction

**Full name:** `gs::function::IDFunction`

**Public Methods:**

- `getFunctionSet()`


---

## IfNullFunction

**Full name:** `gs::function::IfNullFunction`

**Public Methods:**

- `getFunctionSet()`


---

## InitCapFunction

**Full name:** `gs::function::InitCapFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Int128CastData

**Full name:** `gs::function::Int128CastData`

**Public Methods:**

- `flush()`


---

## Int128CastOperation

**Full name:** `gs::function::Int128CastOperation`

**Public Methods:**

- `handleDigit(T &result, uint8_t digit)`
- `finalize(T &result)`


---

## IntegerCastData

**Full name:** `gs::function::IntegerCastData`


---

## IntegerCastOperation

**Full name:** `gs::function::IntegerCastOperation`

**Public Methods:**

- `handleDigit(T &state, uint8_t digit)`
- `finalize(T &)`


---

## InternalIDCreationFunction

**Full name:** `gs::function::InternalIDCreationFunction`

**Public Methods:**

- `getFunctionSet()`


---

## InternalIDHasher

**Full name:** `gs::function::InternalIDHasher`

**Public Methods:**

- `operator()(const common::internalID_t &internalID) const`


---

## IntervalFunction

**Full name:** `gs::function::IntervalFunction`

**Public Methods:**

- `getUnaryIntervalFunction(std::string funcName)`


---

## IntervalFunctionAlias

**Full name:** `gs::function::IntervalFunctionAlias`


---

## IsACyclicFunction

**Full name:** `gs::function::IsACyclicFunction`

**Public Methods:**

- `getFunctionSet()`


---

## IsNotNull

**Full name:** `gs::function::IsNotNull`

**Public Methods:**

- `operation(T, bool isNull, uint8_t &result)`


---

## IsNull

**Full name:** `gs::function::IsNull`

**Public Methods:**

- `operation(T, bool isNull, uint8_t &result)`


---

## IsTrailFunction

**Full name:** `gs::function::IsTrailFunction`

**Public Methods:**

- `getFunctionSet()`


---

## KeysFunctions

**Full name:** `gs::function::KeysFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## LabelFunction

**Full name:** `gs::function::LabelFunction`

**Public Methods:**

- `getFunctionSet()`
- `rewriteFunc(const RewriteFunctionBindInput &input)`


---

## LabelsFunction

**Full name:** `gs::function::LabelsFunction`


---

## LastDay

**Full name:** `gs::function::LastDay`

**Public Methods:**

- `operation(T &, common::date_t &)`
- `operation(common::date_t &input, common::date_t &result)`
- `operation(common::timestamp_t &input, common::date_t &result)`


---

## LastDayFunction

**Full name:** `gs::function::LastDayFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LcaseFunction

**Full name:** `gs::function::LcaseFunction`


---

## Least

**Full name:** `gs::function::Least`

**Public Methods:**

- `operation(T &left, T &right, T &result)`


---

## LeastFunction

**Full name:** `gs::function::LeastFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Left

**Full name:** `gs::function::Left`

**Public Methods:**

- `operation(common::neug_string_t &left, int64_t &right, common...`


---

## LeftFunction

**Full name:** `gs::function::LeftFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LengthFunction

**Full name:** `gs::function::LengthFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LessThan

**Full name:** `gs::function::LessThan`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## LessThanEquals

**Full name:** `gs::function::LessThanEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## LessThanEqualsFunction

**Full name:** `gs::function::LessThanEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LessThanFunction

**Full name:** `gs::function::LessThanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LevenshteinFunction

**Full name:** `gs::function::LevenshteinFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Lgamma

**Full name:** `gs::function::Lgamma`

**Public Methods:**

- `operation(T &input, double &result)`


---

## LgammaFunction

**Full name:** `gs::function::LgammaFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListAllFunction

**Full name:** `gs::function::ListAllFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListAnyFunction

**Full name:** `gs::function::ListAnyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListAnyValueFunction

**Full name:** `gs::function::ListAnyValueFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListAppendFunction

**Full name:** `gs::function::ListAppendFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListCatFunction

**Full name:** `gs::function::ListCatFunction`


---

## ListConcat

**Full name:** `gs::function::ListConcat`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ListConcatFunction

**Full name:** `gs::function::ListConcatFunction`

**Public Methods:**

- `getFunctionSet()`
- `bindFunc(const ScalarBindFuncInput &input)`


---

## ListContainsFunction

**Full name:** `gs::function::ListContainsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListCreationFunction

**Full name:** `gs::function::ListCreationFunction`

**Public Methods:**

- `getFunctionSet()`
- `execFunc(const std::vector< std::shared_ptr< common::Value...`


---

## ListDistinctFunction

**Full name:** `gs::function::ListDistinctFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListElementFunction

**Full name:** `gs::function::ListElementFunction`


---

## ListExtract

**Full name:** `gs::function::ListExtract`

**Public Methods:**

- `operation(common::list_entry_t &listEntry, int64_t pos, T &...`
- `operation(common::neug_string_t &str, int64_t &idx, common::k...`


---

## ListExtractFunction

**Full name:** `gs::function::ListExtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListFilterFunction

**Full name:** `gs::function::ListFilterFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListHasAllFunction

**Full name:** `gs::function::ListHasAllFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListHasFunction

**Full name:** `gs::function::ListHasFunction`


---

## ListIndexOfFunction

**Full name:** `gs::function::ListIndexOfFunction`


---

## ListLen

**Full name:** `gs::function::ListLen`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::neug_string_t &input, int64_t &result)`


---

## ListNoneFunction

**Full name:** `gs::function::ListNoneFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListPosition

**Full name:** `gs::function::ListPosition`

**Public Methods:**

- `operation(common::list_entry_t &list, T &element, int64_t &...`


---

## ListPositionFunction

**Full name:** `gs::function::ListPositionFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListPrependFunction

**Full name:** `gs::function::ListPrependFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListProductFunction

**Full name:** `gs::function::ListProductFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListRangeFunction

**Full name:** `gs::function::ListRangeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListReduceFunction

**Full name:** `gs::function::ListReduceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListReverseFunction

**Full name:** `gs::function::ListReverseFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListReverseSort

**Full name:** `gs::function::ListReverseSort`

**Public Methods:**

- `operation(common::list_entry_t &input, common::list_entry_t...`
- `operation(common::list_entry_t &input, common::neug_string_t ...`
- `operation(common::list_entry_t &, common::neug_string_t &, co...`


---

## ListReverseSortFunction

**Full name:** `gs::function::ListReverseSortFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListSingleFunction

**Full name:** `gs::function::ListSingleFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListSliceFunction

**Full name:** `gs::function::ListSliceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListSort

**Full name:** `gs::function::ListSort`

**Public Methods:**

- `operation(common::list_entry_t &input, common::list_entry_t...`
- `operation(common::list_entry_t &input, common::neug_string_t ...`
- `operation(common::list_entry_t &input, common::neug_string_t ...`


---

## ListSortFunction

**Full name:** `gs::function::ListSortFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListSumFunction

**Full name:** `gs::function::ListSumFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListToString

**Full name:** `gs::function::ListToString`

**Public Methods:**

- `operation(common::list_entry_t &input, common::neug_string_t ...`


---

## ListToStringFunction

**Full name:** `gs::function::ListToStringFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListTransformFunction

**Full name:** `gs::function::ListTransformFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListUnique

**Full name:** `gs::function::ListUnique`

**Public Methods:**

- `appendListElementsToValueSet(common::list_entry_t &input, common::ValueVector ...`
- `operation(common::list_entry_t &input, int64_t &result, com...`


---

## ListUniqueFunction

**Full name:** `gs::function::ListUniqueFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Ln

**Full name:** `gs::function::Ln`

**Public Methods:**

- `operation(T &input, double &result)`


---

## LnFunction

**Full name:** `gs::function::LnFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LocalCacheArrayColumnFunction

**Full name:** `gs::function::LocalCacheArrayColumnFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Log

**Full name:** `gs::function::Log`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Log10Function

**Full name:** `gs::function::Log10Function`


---

## Log2

**Full name:** `gs::function::Log2`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Log2Function

**Full name:** `gs::function::Log2Function`

**Public Methods:**

- `getFunctionSet()`


---

## LogFunction

**Full name:** `gs::function::LogFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Lower

**Full name:** `gs::function::Lower`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`


---

## LowerFunction

**Full name:** `gs::function::LowerFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(size_t idx, gs::runtime::Arena &arena, const std:...`


---

## Lpad

**Full name:** `gs::function::Lpad`

**Public Methods:**

- `operation(common::neug_string_t &src, int64_t count, common::...`
- `lpadOperation(common::neug_string_t &src, int64_t count, common::...`


---

## LpadFunction

**Full name:** `gs::function::LpadFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Ltrim

**Full name:** `gs::function::Ltrim`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`
- `ltrim(char *data, uint32_t len)`


---

## LtrimFunction

**Full name:** `gs::function::LtrimFunction`

**Public Methods:**

- `getFunctionSet()`


---

## MD5Function

**Full name:** `gs::function::MD5Function`

**Public Methods:**

- `getFunctionSet()`


---

## MakeDate

**Full name:** `gs::function::MakeDate`

**Public Methods:**

- `operation(int64_t &year, int64_t &month, int64_t &day, comm...`


---

## MakeDateFunction

**Full name:** `gs::function::MakeDateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## MapCreation

**Full name:** `gs::function::MapCreation`

**Public Methods:**

- `operation(common::list_entry_t &keyEntry, common::list_entr...`
- `copyListEntry(common::list_entry_t &resultEntry, common::ValueV...`


---

## MapCreationFunctions

**Full name:** `gs::function::MapCreationFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## MapExtract

**Full name:** `gs::function::MapExtract`

**Public Methods:**

- `operation(common::list_entry_t &listEntry, T &key, common::...`


---

## MapExtractFunctions

**Full name:** `gs::function::MapExtractFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## MapKeys

**Full name:** `gs::function::MapKeys`

**Public Methods:**

- `operation(common::list_entry_t &listEntry, common::list_ent...`


---

## MapKeysFunctions

**Full name:** `gs::function::MapKeysFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## MapValues

**Full name:** `gs::function::MapValues`

**Public Methods:**

- `operation(common::list_entry_t &listEntry, common::list_ent...`


---

## MapValuesFunctions

**Full name:** `gs::function::MapValuesFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## MinMaxFunction

**Full name:** `gs::function::MinMaxFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(MinMaxState *state, common::ValueVector *input, u...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## MinMaxState

**Full name:** `gs::function::MinMaxFunction::MinMaxState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`
- `setVal(const T &val_, common::InMemOverflowBuffer *)`
- `setVal(const common::neug_string_t &val_, common::InMemOve...`


---

## Modulo

**Full name:** `gs::function::Modulo`

**Public Methods:**

- `operation(A &left, B &right, R &result)`
- `operation(uint8_t &left, uint8_t &right, uint8_t &result)`
- `operation(uint16_t &left, uint16_t &right, uint16_t &result)`
- `operation(uint32_t &left, uint32_t &right, uint32_t &result)`
- `operation(uint64_t &left, uint64_t &right, uint64_t &result)`
- `operation(int8_t &left, int8_t &right, int8_t &result)`
- `operation(int16_t &left, int16_t &right, int16_t &result)`
- `operation(int32_t &left, int32_t &right, int32_t &result)`
- `operation(int64_t &left, int64_t &right, int64_t &result)`
- `operation(common::int128_t &left, common::int128_t &right, ...`


---

## ModuloFunction

**Full name:** `gs::function::ModuloFunction`

**Public Methods:**

- `getFunctionSet()`


---

## MonthName

**Full name:** `gs::function::MonthName`

**Public Methods:**

- `operation(T &, common::neug_string_t &)`
- `operation(common::date_t &input, common::neug_string_t &result)`
- `operation(common::timestamp_t &input, common::neug_string_t &result)`


---

## MonthNameFunction

**Full name:** `gs::function::MonthNameFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Multiply

**Full name:** `gs::function::Multiply`

**Public Methods:**

- `operation(A &left, B &right, R &result)`
- `operation(uint8_t &left, uint8_t &right, uint8_t &result)`
- `operation(uint16_t &left, uint16_t &right, uint16_t &result)`
- `operation(uint32_t &left, uint32_t &right, uint32_t &result)`
- `operation(uint64_t &left, uint64_t &right, uint64_t &result)`
- `operation(int8_t &left, int8_t &right, int8_t &result)`
- `operation(int16_t &left, int16_t &right, int16_t &result)`
- `operation(int32_t &left, int32_t &right, int32_t &result)`
- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## MultiplyFunction

**Full name:** `gs::function::MultiplyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Negate

**Full name:** `gs::function::Negate`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(int8_t &input, int8_t &result)`
- `operation(int16_t &input, int16_t &result)`
- `operation(int32_t &input, int32_t &result)`
- `operation(int64_t &input, int64_t &result)`


---

## NegateFunction

**Full name:** `gs::function::NegateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NextValFunction

**Full name:** `gs::function::NextValFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NodesFunction

**Full name:** `gs::function::NodesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Not

**Full name:** `gs::function::Not`

NOT operator Truth table:

operand isNull right
T F 0 F F 1
- T 2

**Public Methods:**

- `operation(bool operand, bool isNull, uint8_t &result)`


---

## NotEquals

**Full name:** `gs::function::NotEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## NotEqualsFunction

**Full name:** `gs::function::NotEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NullIfFunction

**Full name:** `gs::function::NullIfFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NullOperationExecutor

**Full name:** `gs::function::NullOperationExecutor`

**Public Methods:**

- `execute(common::ValueVector &operand, common::SelectionVe...`
- `select(common::ValueVector &operand, common::SelectionVe...`
- `selectOnValue(common::ValueVector &operand, uint64_t operandPos...`


---

## NumericLimits

**Full name:** `gs::function::NumericLimits`

**Public Methods:**

- `minimum()`
- `maximum()`
- `isSigned()`
- `isInBounds(V val)`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- ... and 5 more methods


---

## int128_t >

**Full name:** `gs::function::NumericLimits< common::int128_t >`

**Public Methods:**

- `minimum()`
- `maximum()`
- `isSigned()`
- `digits()`
- `isInBounds(V val)`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- ... and 5 more methods


---

## ObjectArray

**Full name:** `gs::function::ObjectArray`

**Public Methods:**

- `ObjectArray(const common::offset_t size, storage::MemoryManag...`
- `set(const common::offset_t pos, const T value)`
- `get(const common::offset_t pos)`


---

## ObjectBlock

**Full name:** `gs::function::ObjectBlock`

**Public Methods:**

- `ObjectBlock(std::unique_ptr< storage::MemoryBuffer > block, u...`
- `reserveNext()`
- `revertLast()`
- `hasSpace() const`


---

## OctetLength

**Full name:** `gs::function::OctetLength`

**Public Methods:**

- `operation(common::blob_t &input, int64_t &result)`


---

## OctetLengthFunctions

**Full name:** `gs::function::OctetLengthFunctions`

**Public Methods:**

- `getFunctionSet()`


---

## Offset

**Full name:** `gs::function::Offset`

**Public Methods:**

- `operation(common::internalID_t &input, int64_t &result)`


---

## OffsetFunction

**Full name:** `gs::function::OffsetFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Or

**Full name:** `gs::function::Or`

OR operator Truth table:

left isLeftNull right isRightNull result
T F T F 1 T F F F 1 F F T F 1 F F F F 0
- T T F 1
- T F F 2 T F - T 1 F F - T 2
- T - T 2

**Public Methods:**

- `operation(bool left, bool right, uint8_t &result, bool isLe...`


---

## PadOperation

**Full name:** `gs::function::PadOperation`

**Public Methods:**

- `operation(common::neug_string_t &src, int64_t count, common::...`


---

## ParentList

**Full name:** `gs::function::ParentList`

**Public Methods:**

- `setNbrInfo(common::nodeID_t nodeID_, common::relID_t edgeID_...`
- `getNodeID() const`
- `getEdgeID() const`
- `isFwdEdge() const`
- `setNextPtr(ParentList *ptr)`
- `getNextPtr()`
- `setIter(uint16_t iter_)`
- `getIter() const`
- `setCost(double cost_)`
- `getCost() const`


---

## PathAuxiliaryState

**Full name:** `gs::function::PathAuxiliaryState`

**Public Methods:**

- `PathAuxiliaryState(std::unique_ptr< BFSGraphManager > bfsGraphManager)`
- `getBFSGraphManager()`
- `beginFrontierCompute(common::table_id_t, common::table_id_t toTableID) override`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`


---

## PathsOutputWriter

**Full name:** `gs::function::PathsOutputWriter`

**Public Methods:**

- `PathsOutputWriter(main::ClientContext *context, common::NodeOffsetM...`
- `beginWritingInternal(common::table_id_t tableID) override`


---

## PathsOutputWriterInfo

**Full name:** `gs::function::PathsOutputWriterInfo`

**Public Methods:**

- `hasNodeMask() const`


---

## Pi

**Full name:** `gs::function::Pi`

**Public Methods:**

- `operation(double &result)`


---

## PiFunction

**Full name:** `gs::function::PiFunction`

**Public Methods:**

- `getFunctionSet()`


---

## PointerFunctionExecutor

**Full name:** `gs::function::PointerFunctionExecutor`

**Public Methods:**

- `execute(common::ValueVector &result, common::SelectionVec...`


---

## PowFunction

**Full name:** `gs::function::PowFunction`


---

## Power

**Full name:** `gs::function::Power`

**Public Methods:**

- `operation(A &left, B &right, R &result)`


---

## PowerFunction

**Full name:** `gs::function::PowerFunction`

**Public Methods:**

- `getFunctionSet()`


---

## PrefixFunction

**Full name:** `gs::function::PrefixFunction`


---

## PropertiesBindData

**Full name:** `gs::function::PropertiesBindData`

**Public Methods:**

- `PropertiesBindData(common::LogicalType dataType, common::idx_t childIdx)`
- `copy() const override`


---

## PropertiesFunction

**Full name:** `gs::function::PropertiesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RJAlgorithm

**Full name:** `gs::function::RJAlgorithm`

**Public Methods:**

- `~RJAlgorithm()=default`
- `getFunctionName() const =0`
- `getResultColumns(const RJBindData &bindData) const =0`
- `copy() const =0`


---

## RJBindData

**Full name:** `gs::function::RJBindData`

**Public Methods:**

- `RJBindData(graph::GraphEntry graphEntry)`
- `RJBindData(const RJBindData &other)`
- `getPathWriterInfo() const`
- `toString() const`


---

## RJOutputWriter

**Full name:** `gs::function::RJOutputWriter`

**Public Methods:**

- `RJOutputWriter(main::ClientContext *context, common::NodeOffsetM...`
- `~RJOutputWriter()=default`
- `beginWriting(common::table_id_t tableID)`
- `beginWritingInternal(common::table_id_t tableID)=0`
- `inOutputNodeMask(common::offset_t offset)`
- `copy()=0`


---

## Radians

**Full name:** `gs::function::Radians`

**Public Methods:**

- `operation(T &input, double &result)`


---

## RadiansFunction

**Full name:** `gs::function::RadiansFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpExtract

**Full name:** `gs::function::RegexpExtract`

**Public Methods:**

- `operation(common::neug_string_t &value, common::neug_string_t &...`
- `operation(common::neug_string_t &value, common::neug_string_t &...`
- `regexExtract(const std::string &input, const std::string &patt...`


---

## RegexpExtractAll

**Full name:** `gs::function::RegexpExtractAll`

**Public Methods:**

- `operation(common::neug_string_t &value, common::neug_string_t &...`
- `operation(common::neug_string_t &value, common::neug_string_t &...`
- `regexExtractAll(const std::string &value, const std::string &patt...`
- `IsCharacter(char c)`


---

## RegexpExtractAllFunction

**Full name:** `gs::function::RegexpExtractAllFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpExtractFunction

**Full name:** `gs::function::RegexpExtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpFullMatchFunction

**Full name:** `gs::function::RegexpFullMatchFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpMatches

**Full name:** `gs::function::RegexpMatches`

**Public Methods:**

- `operation(common::neug_string_t &left, common::neug_string_t &r...`


---

## RegexpMatchesFunction

**Full name:** `gs::function::RegexpMatchesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpReplaceFunction

**Full name:** `gs::function::RegexpReplaceFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RegexpSplitToArray

**Full name:** `gs::function::RegexpSplitToArray`

**Public Methods:**

- `operation(common::neug_string_t &value, common::neug_string_t &...`
- `regexExtractAll(const std::string &value, const std::string &pattern)`


---

## RegexpSplitToArrayFunction

**Full name:** `gs::function::RegexpSplitToArrayFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RelationshipsFunction

**Full name:** `gs::function::RelationshipsFunction`


---

## RelsFunction

**Full name:** `gs::function::RelsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Repeat

**Full name:** `gs::function::Repeat`

**Public Methods:**

- `operation(common::neug_string_t &left, int64_t &right, common...`


---

## RepeatFunction

**Full name:** `gs::function::RepeatFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Reverse

**Full name:** `gs::function::Reverse`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`


---

## ReverseFunction

**Full name:** `gs::function::ReverseFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(size_t idx, gs::runtime::Arena &arena, const std:...`


---

## RewriteFunction

**Full name:** `gs::function::RewriteFunction`

**Public Methods:**

- `RewriteFunction(std::string name, std::vector< common::LogicalTyp...`


---

## RewriteFunctionBindInput

**Full name:** `gs::function::RewriteFunctionBindInput`

**Public Methods:**

- `RewriteFunctionBindInput(main::ClientContext *context, binder::ExpressionB...`


---

## Right

**Full name:** `gs::function::Right`

**Public Methods:**

- `operation(common::neug_string_t &left, int64_t &right, common...`


---

## RightFunction

**Full name:** `gs::function::RightFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Round

**Full name:** `gs::function::Round`

**Public Methods:**

- `operation(A &left, B &right, double &result)`


---

## RoundFunction

**Full name:** `gs::function::RoundFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Rpad

**Full name:** `gs::function::Rpad`

**Public Methods:**

- `operation(common::neug_string_t &src, int64_t count, common::...`
- `rpadOperation(common::neug_string_t &src, int64_t count, common::...`


---

## RpadFunction

**Full name:** `gs::function::RpadFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Rtrim

**Full name:** `gs::function::Rtrim`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`
- `rtrim(char *data, uint32_t len)`


---

## RtrimFunction

**Full name:** `gs::function::RtrimFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SHA256Function

**Full name:** `gs::function::SHA256Function`

**Public Methods:**

- `getFunctionSet()`


---

## SPEdgeCompute

**Full name:** `gs::function::SPEdgeCompute`

**Public Methods:**

- `SPEdgeCompute(SPFrontierPair *frontierPair)`
- `resetSingleThreadState() override`
- `terminate(common::NodeOffsetMaskMap &maskMap) override`


---

## SPFrontierPair

**Full name:** `gs::function::SPFrontierPair`

**Public Methods:**

- `SPFrontierPair(std::unique_ptr< DenseFrontier > denseFrontier)`
- `getFrontier()`
- `beginNewIterationInternalNoLock() override`
- `getNumActiveNodesInCurrentFrontier(common::NodeOffsetMaskMap &mask)`
- `getActiveNodesOnCurrentFrontier() override`
- `getState() const override`
- `needSwitchToDense(uint64_t threshold) const override`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`


---

## SPPathsOutputWriter

**Full name:** `gs::function::SPPathsOutputWriter`

**Public Methods:**

- `SPPathsOutputWriter(main::ClientContext *context, common::NodeOffsetM...`
- `copy() override`


---

## ScalarBindFuncInput

**Full name:** `gs::function::ScalarBindFuncInput`

**Public Methods:**

- `ScalarBindFuncInput(const binder::expression_vector &arguments, Funct...`


---

## ScalarFunction

**Full name:** `gs::function::ScalarFunction`

**Public Methods:**

- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarFunction()=default`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `copy() const`
- `TernaryExecFunction(const std::vector< std::shared_ptr< common::Value...`
- `TernaryStringExecFunction(const std::vector< std::shared_ptr< common::Value...`
- `TernaryRegexExecFunction(const std::vector< std::shared_ptr< common::Value...`
- `TernaryExecListStructFunction(const std::vector< std::shared_ptr< common::Value...`
- ... and 15 more methods


---

## ScalarMacroFunction

**Full name:** `gs::function::ScalarMacroFunction`

**Public Methods:**

- `ScalarMacroFunction()=default`
- `ScalarMacroFunction(std::unique_ptr< parser::ParsedExpression > expre...`
- `getDefaultParameterName(uint64_t idx) const`
- `getNumArgs() const`
- `getPositionalArgs() const`
- `getDefaultParameterVals() const`
- `copy() const`
- `serialize(common::Serializer &serializer) const`
- `toCypher(const std::string &name) const`
- `deserialize(common::Deserializer &deserializer)`


---

## ScalarOrAggregateFunction

**Full name:** `gs::function::ScalarOrAggregateFunction`

**Public Methods:**

- `ScalarOrAggregateFunction()`
- `ScalarOrAggregateFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarOrAggregateFunction(std::string name, std::vector< common::LogicalTyp...`
- `signatureToString() const override`


---

## ScanFileBindData

**Full name:** `gs::function::ScanFileBindData`

**Public Methods:**

- `ScanFileBindData(binder::expression_vector columns, uint64_t numRo...`
- `ScanFileBindData(binder::expression_vector columns, uint64_t numRo...`
- `ScanFileBindData(const ScanFileBindData &other)`
- `getIgnoreErrorsOption() const override`
- `copy() const override`


---

## ScanFileSharedState

**Full name:** `gs::function::ScanFileSharedState`

**Public Methods:**

- `ScanFileSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)`
- `getNext()`


---

## ScanFileWithProgressSharedState

**Full name:** `gs::function::ScanFileWithProgressSharedState`

**Public Methods:**

- `ScanFileWithProgressSharedState(common::FileScanInfo fileScanInfo, uint64_t numRo...`


---

## ScanReplacement

**Full name:** `gs::function::ScanReplacement`

**Public Methods:**

- `ScanReplacement(scan_replace_func_t replaceFunc)`


---

## ScanReplacementData

**Full name:** `gs::function::ScanReplacementData`


---

## ShowAttachedDatabasesFunction

**Full name:** `gs::function::ShowAttachedDatabasesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowConnectionFunction

**Full name:** `gs::function::ShowConnectionFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowFunctionsFunction

**Full name:** `gs::function::ShowFunctionsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowIndexesFunction

**Full name:** `gs::function::ShowIndexesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowLoadedExtensionsFunction

**Full name:** `gs::function::ShowLoadedExtensionsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowOfficialExtensionsFunction

**Full name:** `gs::function::ShowOfficialExtensionsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowProjectedGraphsFunction

**Full name:** `gs::function::ShowProjectedGraphsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowSequencesFunction

**Full name:** `gs::function::ShowSequencesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowTablesFunction

**Full name:** `gs::function::ShowTablesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ShowWarningsFunction

**Full name:** `gs::function::ShowWarningsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Sign

**Full name:** `gs::function::Sign`

**Public Methods:**

- `operation(T &input, int64_t &result)`


---

## SignFunction

**Full name:** `gs::function::SignFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SimpleTableFunc

**Full name:** `gs::function::SimpleTableFunc`

**Public Methods:**

- `initSharedState(const TableFuncInitSharedStateInput &input)`
- `getTableFunc(simple_internal_table_func internalTableFunc)`


---

## SimpleTableFuncSharedState

**Full name:** `gs::function::SimpleTableFuncSharedState`

**Public Methods:**

- `SimpleTableFuncSharedState()=default`
- `SimpleTableFuncSharedState(common::row_idx_t numRows, common::offset_t maxMo...`
- `getMorsel()`


---

## Sin

**Full name:** `gs::function::Sin`

**Public Methods:**

- `operation(T &input, double &result)`


---

## SinFunction

**Full name:** `gs::function::SinFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SingleSPDestinationsFunction

**Full name:** `gs::function::SingleSPDestinationsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## SingleSPPathsFunction

**Full name:** `gs::function::SingleSPPathsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## SizeFunction

**Full name:** `gs::function::SizeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SparseBFSGraph

**Full name:** `gs::function::SparseBFSGraph`

**Public Methods:**

- `SparseBFSGraph(storage::MemoryManager *mm, common::table_id_map_...`
- `pinTableID(common::table_id_t tableID) override`
- `addParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `addSingleParent(uint16_t iter, common::nodeID_t boundNodeID, comm...`
- `tryAddParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `tryAddSingleParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edg...`
- `getParentListHead(common::offset_t offset) override`
- `getParentListHead(common::nodeID_t nodeID) override`
- `setParentList(common::offset_t offset, ParentList *parentList) override`
- `getCurrentData() const`


---

## SparseFrontier

**Full name:** `gs::function::SparseFrontier`

**Public Methods:**

- `SparseFrontier(const common::table_id_map_t< common::offset_t > ...`
- `pinTableID(common::table_id_t tableID) override`
- `addNode(common::nodeID_t nodeID, iteration_t iter) override`
- `addNode(common::offset_t offset, iteration_t iter) override`
- `addNodes(const std::vector< common::nodeID_t > &nodeIDs, i...`
- `getIteration(common::offset_t offset) const override`
- `size() const`
- `getCurrentData() const`


---

## SparseFrontierReference

**Full name:** `gs::function::SparseFrontierReference`

**Public Methods:**

- `SparseFrontierReference(SparseFrontier &frontier)`
- `pinTableID(common::table_id_t tableID) override`
- `addNode(common::nodeID_t nodeID, iteration_t iter) override`
- `addNode(common::offset_t offset, iteration_t iter) override`
- `addNodes(const std::vector< common::nodeID_t > &nodeIDs, i...`
- `getIteration(common::offset_t offset) const override`
- `getCurrentData() const`


---

## SplitPartFunction

**Full name:** `gs::function::SplitPartFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Sqrt

**Full name:** `gs::function::Sqrt`

**Public Methods:**

- `operation(T &input, double &result)`


---

## SqrtFunction

**Full name:** `gs::function::SqrtFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StartNodeFunction

**Full name:** `gs::function::StartNodeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StartsWith

**Full name:** `gs::function::StartsWith`

**Public Methods:**

- `operation(common::neug_string_t &left, common::neug_string_t &r...`


---

## StartsWithFunction

**Full name:** `gs::function::StartsWithFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StatsInfoFunction

**Full name:** `gs::function::StatsInfoFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StorageInfoFunction

**Full name:** `gs::function::StorageInfoFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StrSplitFunction

**Full name:** `gs::function::StrSplitFunction`


---

## StringFunction

**Full name:** `gs::function::StringFunction`


---

## StringSplitFunction

**Full name:** `gs::function::StringSplitFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StringToArrayFunction

**Full name:** `gs::function::StringToArrayFunction`


---

## StructExtractBindData

**Full name:** `gs::function::StructExtractBindData`

**Public Methods:**

- `StructExtractBindData(common::LogicalType dataType, common::idx_t childIdx)`
- `copy() const override`


---

## StructExtractFunctions

**Full name:** `gs::function::StructExtractFunctions`

**Public Methods:**

- `getFunctionSet()`
- `bindFunc(const ScalarBindFuncInput &input)`
- `compileFunc(FunctionBindData *bindData, const std::vector< st...`


---

## StructPackFunctions

**Full name:** `gs::function::StructPackFunctions`

**Public Methods:**

- `getFunctionSet()`
- `execFunc(const std::vector< std::shared_ptr< common::Value...`
- `undirectedRelPackExecFunc(const std::vector< std::shared_ptr< common::Value...`
- `compileFunc(FunctionBindData *bindData, const std::vector< st...`
- `undirectedRelCompileFunc(FunctionBindData *bindData, const std::vector< st...`


---

## SubStr

**Full name:** `gs::function::SubStr`

**Public Methods:**

- `operation(common::neug_string_t &src, int64_t start, int64_t ...`
- `copySubstr(common::neug_string_t &src, int64_t start, int64_t ...`


---

## SubStrFunction

**Full name:** `gs::function::SubStrFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SubstringFunction

**Full name:** `gs::function::SubstringFunction`


---

## Subtract

**Full name:** `gs::function::Subtract`

**Public Methods:**

- `operation(A &left, B &right, R &result)`
- `operation(uint8_t &left, uint8_t &right, uint8_t &result)`
- `operation(uint16_t &left, uint16_t &right, uint16_t &result)`
- `operation(uint32_t &left, uint32_t &right, uint32_t &result)`
- `operation(uint64_t &left, uint64_t &right, uint64_t &result)`
- `operation(int8_t &left, int8_t &right, int8_t &result)`
- `operation(int16_t &left, int16_t &right, int16_t &result)`
- `operation(int32_t &left, int32_t &right, int32_t &result)`
- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## SubtractFunction

**Full name:** `gs::function::SubtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SuffixFunction

**Full name:** `gs::function::SuffixFunction`


---

## SumFunction

**Full name:** `gs::function::SumFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(SumState< RESULT_TYPE > *state, common::ValueVect...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## SumState

**Full name:** `gs::function::SumState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`


---

## TableFuncBindData

**Full name:** `gs::function::TableFuncBindData`

**Public Methods:**

- `TableFuncBindData()`
- `TableFuncBindData(common::row_idx_t numRows)`
- `TableFuncBindData(binder::expression_vector columns)`
- `TableFuncBindData(binder::expression_vector columns, common::row_id...`
- `TableFuncBindData(const TableFuncBindData &other)`
- `operator=(const TableFuncBindData &other)=delete`
- `~TableFuncBindData()=default`
- `getNumColumns() const`
- `setColumnSkips(std::vector< bool > skips)`
- `getColumnSkips() const`
- ... and 5 more methods


---

## TableFuncBindInput

**Full name:** `gs::function::TableFuncBindInput`

**Public Methods:**

- `TableFuncBindInput()=default`
- `addLiteralParam(common::Value value)`
- `getParam(common::idx_t idx) const`
- `getValue(common::idx_t idx) const`
- `getLiteralVal(common::idx_t idx) const`


---

## TableFuncInitLocalStateInput

**Full name:** `gs::function::TableFuncInitLocalStateInput`

**Public Methods:**

- `TableFuncInitLocalStateInput(TableFuncSharedState &sharedState, TableFuncBindD...`


---

## TableFuncInitOutputInput

**Full name:** `gs::function::TableFuncInitOutputInput`

**Public Methods:**

- `TableFuncInitOutputInput(std::vector< processor::DataPos > outColumnPositi...`


---

## TableFuncInitSharedStateInput

**Full name:** `gs::function::TableFuncInitSharedStateInput`

**Public Methods:**

- `TableFuncInitSharedStateInput(TableFuncBindData *bindData, processor::Execution...`


---

## TableFuncInput

**Full name:** `gs::function::TableFuncInput`

**Public Methods:**

- `TableFuncInput()=default`
- `TableFuncInput(TableFuncBindData *bindData, TableFuncLocalState ...`
- `DELETE_COPY_DEFAULT_MOVE(TableFuncInput)`


---

## TableFuncLocalState

**Full name:** `gs::function::TableFuncLocalState`

**Public Methods:**

- `~TableFuncLocalState()=default`
- `ptrCast()`


---

## TableFuncMorsel

**Full name:** `gs::function::TableFuncMorsel`

**Public Methods:**

- `TableFuncMorsel(common::offset_t startOffset, common::offset_t endOffset)`
- `hasMoreToOutput() const`
- `getMorselSize() const`
- `isInvalid() const`
- `createInvalidMorsel()`


---

## TableFuncOutput

**Full name:** `gs::function::TableFuncOutput`

**Public Methods:**

- `TableFuncOutput(common::DataChunk dataChunk)`
- `~TableFuncOutput()=default`
- `resetState()`
- `setOutputSize(common::offset_t size) const`


---

## TableFuncSharedState

**Full name:** `gs::function::TableFuncSharedState`

**Public Methods:**

- `TableFuncSharedState()=default`
- `TableFuncSharedState(common::row_idx_t numRows)`
- `~TableFuncSharedState()=default`
- `getNumRows() const`
- `getSemiMasks() const`
- `ptrCast()`


---

## TableFunction

**Full name:** `gs::function::TableFunction`

**Public Methods:**

- `TableFunction()`
- `TableFunction(std::string name, std::vector< common::LogicalTyp...`
- `~TableFunction() override`
- `TableFunction(const TableFunction &)=default`
- `operator=(const TableFunction &other)=default`
- `DEFAULT_BOTH_MOVE(TableFunction)`
- `signatureToString() const override`
- `copy() const`
- `initEmptyLocalState(const TableFuncInitLocalStateInput &input)`
- `initEmptySharedState(const TableFuncInitSharedStateInput &input)`
- ... and 4 more methods


---

## TableInfoFunction

**Full name:** `gs::function::TableInfoFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Tan

**Full name:** `gs::function::Tan`

**Public Methods:**

- `operation(T &input, double &result)`


---

## TanFunction

**Full name:** `gs::function::TanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## TernaryFunctionExecutor

**Full name:** `gs::function::TernaryFunctionExecutor`

**Public Methods:**

- `executeOnValue(common::ValueVector &a, common::ValueVector &b, c...`
- `executeAllFlat(common::ValueVector &a, common::SelectionVector *...`
- `executeFlatFlatUnflat(common::ValueVector &a, common::SelectionVector *...`
- `executeFlatUnflatUnflat(common::ValueVector &a, common::SelectionVector *...`
- `executeFlatUnflatFlat(common::ValueVector &a, common::SelectionVector *...`
- `executeAllUnFlat(common::ValueVector &a, common::SelectionVector *...`
- `executeUnflatFlatFlat(common::ValueVector &a, common::SelectionVector *...`
- `executeUnflatFlatUnflat(common::ValueVector &a, common::SelectionVector *...`
- `executeUnflatUnFlatFlat(common::ValueVector &a, common::SelectionVector *...`
- `executeSwitch(common::ValueVector &a, common::SelectionVector *...`


---

## TernaryFunctionWrapper

**Full name:** `gs::function::TernaryFunctionWrapper`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## TernaryListFunctionWrapper

**Full name:** `gs::function::TernaryListFunctionWrapper`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## TernaryRegexFunctionWrapper

**Full name:** `gs::function::TernaryRegexFunctionWrapper`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## TernaryStringFunctionWrapper

**Full name:** `gs::function::TernaryStringFunctionWrapper`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## TernaryUDFExecutor

**Full name:** `gs::function::TernaryUDFExecutor`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## TernaryUDFFunctionWrapper

**Full name:** `gs::function::TernaryUDFFunctionWrapper`

**Public Methods:**

- `operation(A_TYPE &a, B_TYPE &b, C_TYPE &c, RESULT_TYPE &res...`


---

## ToDays

**Full name:** `gs::function::ToDays`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToDaysFunction

**Full name:** `gs::function::ToDaysFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToEpochMsFunction

**Full name:** `gs::function::ToEpochMsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToHours

**Full name:** `gs::function::ToHours`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToHoursFunction

**Full name:** `gs::function::ToHoursFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToLowerFunction

**Full name:** `gs::function::ToLowerFunction`


---

## ToMicroseconds

**Full name:** `gs::function::ToMicroseconds`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToMicrosecondsFunction

**Full name:** `gs::function::ToMicrosecondsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToMilliseconds

**Full name:** `gs::function::ToMilliseconds`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToMillisecondsFunction

**Full name:** `gs::function::ToMillisecondsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToMinutes

**Full name:** `gs::function::ToMinutes`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToMinutesFunction

**Full name:** `gs::function::ToMinutesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToMonths

**Full name:** `gs::function::ToMonths`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToMonthsFunction

**Full name:** `gs::function::ToMonthsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToSeconds

**Full name:** `gs::function::ToSeconds`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToSecondsFunction

**Full name:** `gs::function::ToSecondsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToTimestamp

**Full name:** `gs::function::ToTimestamp`

**Public Methods:**

- `operation(double &sec, common::timestamp_t &result)`


---

## ToTimestampFunction

**Full name:** `gs::function::ToTimestampFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ToUpperFunction

**Full name:** `gs::function::ToUpperFunction`


---

## ToYears

**Full name:** `gs::function::ToYears`

**Public Methods:**

- `operation(int64_t &input, common::interval_t &result)`


---

## ToYearsFunction

**Full name:** `gs::function::ToYearsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Trim

**Full name:** `gs::function::Trim`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`


---

## TrimFunction

**Full name:** `gs::function::TrimFunction`

**Public Methods:**

- `getFunctionSet()`


---

## TryCastStringToTimestamp

**Full name:** `gs::function::TryCastStringToTimestamp`

**Public Methods:**

- `tryCast(const char *input, uint64_t len, gs::common::time...`
- `cast(const char *input, uint64_t len, gs::common::time...`
- `tryCast(const char *input, uint64_t len, gs::common::time...`
- `tryCast(const char *input, uint64_t len, gs::common::time...`
- `tryCast(const char *input, uint64_t len, gs::common::time...`


---

## TypeOfFunction

**Full name:** `gs::function::TypeOfFunction`

**Public Methods:**

- `getFunctionSet()`


---

## UCaseFunction

**Full name:** `gs::function::UCaseFunction`


---

## UDF

**Full name:** `gs::function::UDF`

**Public Methods:**

- `templateValidateType(const common::LogicalTypeID &type)`
- `validateType(const common::LogicalTypeID &type)`
- `createEmptyParameterExecFunc(RESULT_TYPE(*)(Args...), const std::vector< commo...`
- `createEmptyParameterExecFunc(RESULT_TYPE(*udfFunc)(), const std::vector< commo...`
- `createUnaryExecFunc(RESULT_TYPE(*)(Args...), const std::vector< commo...`
- `createUnaryExecFunc(RESULT_TYPE(*udfFunc)(OPERAND_TYPE), const std::v...`
- `createBinaryExecFunc(RESULT_TYPE(*)(Args...), const std::vector< commo...`
- `createBinaryExecFunc(RESULT_TYPE(*udfFunc)(LEFT_TYPE, RIGHT_TYPE), con...`
- `createTernaryExecFunc(RESULT_TYPE(*)(Args...), const std::vector< commo...`
- `createTernaryExecFunc(RESULT_TYPE(*udfFunc)(A_TYPE, B_TYPE, C_TYPE), st...`
- ... and 9 more methods


---

## UUIDFunction

**Full name:** `gs::function::UUIDFunction`


---

## UnaryBooleanOperationExecutor

**Full name:** `gs::function::UnaryBooleanOperationExecutor`

**Public Methods:**

- `executeOnValue(common::ValueVector &operand, uint64_t operandPos...`
- `executeSwitch(common::ValueVector &operand, common::SelectionVe...`
- `execute(common::ValueVector &operand, common::SelectionVe...`
- `selectOnValue(common::ValueVector &operand, uint64_t operandPos...`
- `select(common::ValueVector &operand, common::SelectionVe...`


---

## UnaryCastFunctionWrapper

**Full name:** `gs::function::UnaryCastFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryCastStringFunctionWrapper

**Full name:** `gs::function::UnaryCastStringFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryFunctionExecutor

**Full name:** `gs::function::UnaryFunctionExecutor`

**Public Methods:**

- `executeOnValue(common::ValueVector &inputVector, uint64_t inputP...`
- `getSelectedPos(common::idx_t selIdx, common::SelectionVector *op...`
- `executeOnSelectedValues(common::ValueVector &operand, common::SelectionVe...`
- `executeSwitch(common::ValueVector &operand, common::SelectionVe...`
- `execute(common::ValueVector &operand, common::SelectionVe...`
- `executeSequence(common::ValueVector &operand, common::SelectionVe...`


---

## UnaryFunctionWrapper

**Full name:** `gs::function::UnaryFunctionWrapper`

Unary operator assumes operation with null returns null.

This does NOT applies to IS_NULL and IS_NOT_NULL operation.

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryHashFunctionExecutor

**Full name:** `gs::function::UnaryHashFunctionExecutor`

**Public Methods:**

- `execute(const common::ValueVector &operand, const common:...`


---

## UnaryNestedTypeFunctionWrapper

**Full name:** `gs::function::UnaryNestedTypeFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryPathExecutor

**Full name:** `gs::function::UnaryPathExecutor`

**Public Methods:**

- `executeNodeIDs(common::ValueVector &input, common::SelectionVect...`
- `executeRelIDs(common::ValueVector &input, common::SelectionVect...`
- `selectNodeIDs(common::ValueVector &input, common::SelectionVect...`
- `selectRelIDs(common::ValueVector &input, common::SelectionVect...`


---

## UnarySequenceFunctionWrapper

**Full name:** `gs::function::UnarySequenceFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryStringFunctionWrapper

**Full name:** `gs::function::UnaryStringFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryStructFunctionWrapper

**Full name:** `gs::function::UnaryStructFunctionWrapper`

**Public Methods:**

- `operation(void *, uint64_t, void *resultVector, uint64_t re...`


---

## UnaryUDFExecutor

**Full name:** `gs::function::UnaryUDFExecutor`

**Public Methods:**

- `operation(OPERAND_TYPE &input, RESULT_TYPE &result, void *udfFunc)`


---

## UnaryUDFFunctionWrapper

**Full name:** `gs::function::UnaryUDFFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnionExtractFunction

**Full name:** `gs::function::UnionExtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## UnionTag

**Full name:** `gs::function::UnionTag`

**Public Methods:**

- `operation(common::union_entry_t &unionValue, common::neug_str...`


---

## UnionTagFunction

**Full name:** `gs::function::UnionTagFunction`

**Public Methods:**

- `getFunctionSet()`


---

## UnionValueFunction

**Full name:** `gs::function::UnionValueFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Upper

**Full name:** `gs::function::Upper`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string_t &...`


---

## UpperFunction

**Full name:** `gs::function::UpperFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(size_t idx, gs::runtime::Arena &arena, const std:...`


---

## ValueEquality

**Full name:** `gs::function::ValueEquality`

**Public Methods:**

- `operator()(const common::Value &a, const common::Value &b) const`


---

## ValueHashFunction

**Full name:** `gs::function::ValueHashFunction`

**Public Methods:**

- `operator()(const common::Value &value) const`


---

## VarLenJoinsFunction

**Full name:** `gs::function::VarLenJoinsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## VectorBooleanFunction

**Full name:** `gs::function::VectorBooleanFunction`

**Public Methods:**

- `bindExecFunction(common::ExpressionType expressionType, const bind...`
- `bindSelectFunction(common::ExpressionType expressionType, const bind...`


---

## VectorHashFunction

**Full name:** `gs::function::VectorHashFunction`

**Public Methods:**

- `computeHash(const common::ValueVector &operand, const common:...`
- `combineHash(const common::ValueVector &left, const common::Se...`


---

## VectorNullFunction

**Full name:** `gs::function::VectorNullFunction`

**Public Methods:**

- `bindExecFunction(common::ExpressionType expressionType, const bind...`
- `bindSelectFunction(common::ExpressionType expressionType, const bind...`


---

## VectorStringFunction

**Full name:** `gs::function::VectorStringFunction`

**Public Methods:**

- `getUnaryStrFunction(std::string funcName)`


---

## VertexCompute

**Full name:** `gs::function::VertexCompute`

**Public Methods:**

- `~VertexCompute()=default`
- `beginOnTable(common::table_id_t)`
- `vertexCompute(const graph::VertexScanState::Chunk &)`
- `vertexCompute(common::offset_t, common::offset_t, common::table_id_t)`
- `vertexCompute(common::table_id_t)`
- `copy()=0`


---

## VertexComputeTask

**Full name:** `gs::function::VertexComputeTask`

**Public Methods:**

- `VertexComputeTask(uint64_t maxNumThreads, const VertexComputeTaskIn...`
- `getSharedState() const`
- `run() override`
- `runSparse()`


---

## VertexComputeTaskInfo

**Full name:** `gs::function::VertexComputeTaskInfo`

**Public Methods:**

- `VertexComputeTaskInfo(VertexCompute &vc, graph::Graph *graph, catalog::...`
- `VertexComputeTaskInfo(const VertexComputeTaskInfo &other)`
- `hasPropertiesToScan() const`


---

## VertexComputeTaskSharedState

**Full name:** `gs::function::VertexComputeTaskSharedState`

**Public Methods:**

- `VertexComputeTaskSharedState(uint64_t maxNumThreads)`


---

## WSPPathsAuxiliaryState

**Full name:** `gs::function::WSPPathsAuxiliaryState`

**Public Methods:**

- `WSPPathsAuxiliaryState(std::unique_ptr< BFSGraphManager > bfsGraphManager)`
- `getBFSGraphManager()`
- `initSource(common::nodeID_t sourceNodeID) override`
- `beginFrontierCompute(common::table_id_t, common::table_id_t toTableID) override`
- `switchToDense(processor::ExecutionContext *context, graph::Grap...`


---

## WeightedSPDestinationsFunction

**Full name:** `gs::function::WeightedSPDestinationsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## WeightedSPPathsFunction

**Full name:** `gs::function::WeightedSPPathsFunction`

**Public Methods:**

- `getAlgorithm()`


---

## Xor

**Full name:** `gs::function::Xor`

XOR operator Truth table:

left isLeftNull right isRightNull result
T F T F 0 T F F F 1 F F T F 1 F F F F 0
- T T F 2
- T F F 2 T F - T 2 F F - T 2
- T - T 2

**Public Methods:**

- `operation(bool left, bool right, uint8_t &result, bool isLe...`


---

## pickDecimalPhysicalType

**Full name:** `gs::function::pickDecimalPhysicalType`


---

## EdgeLabel

**Full name:** `gs::gopt::EdgeLabel`

**Public Methods:**

- `EdgeLabel(const std::string &type, const std::string &src, ...`


---

## EdgeLabelId

**Full name:** `gs::gopt::EdgeLabelId`

**Public Methods:**

- `EdgeLabelId(common::table_id_t eid, common::table_id_t sid, c...`


---

## GAliasManager

**Full name:** `gs::gopt::GAliasManager`

**Public Methods:**

- `GAliasManager(const planner::LogicalPlan &plan)`
- `getAliasId(const std::string &uniqueName)`
- `getGAliasName(common::alias_id_t aliasId)`
- `extractGAliasNames(const planner::LogicalOperator &op, std::vector< ...`
- `extractAliasIds(const planner::LogicalOperator &op, std::vector< ...`
- `printForDebug()`


---

## GAliasName

**Full name:** `gs::gopt::GAliasName`

**Public Methods:**

- `GAliasName()=default`
- `GAliasName(const std::string &uniqueName, std::optional< std...`
- `GAliasName(const std::string &uniqueName, const std::string ...`


---

## GDDLConverter

**Full name:** `gs::gopt::GDDLConverter`

**Public Methods:**

- `GDDLConverter(gs::catalog::Catalog *catalog)`
- `~GDDLConverter()=default`
- `convertCreateTable(const planner::LogicalCreateTable &op)`
- `convertDropTable(const planner::LogicalDrop &op)`
- `convertAlterTable(const planner::LogicalAlter &op)`
- `convert(const planner::LogicalPlan &plan)`


---

## GExprConverter

**Full name:** `gs::gopt::GExprConverter`

**Public Methods:**

- `GExprConverter(const std::shared_ptr< gopt::GAliasManager > aliasManager)`
- `convert(const binder::Expression &expr, const std::vector...`
- `convert(const binder::Expression &expr, const planner::Lo...`
- `convertPrimaryKey(const std::string &key, const binder::Expression &expr)`
- `convertVar(common::alias_id_t columnId)`
- `convertAlias(common::alias_id_t aliasId)`
- `convertAggFunc(const binder::AggregateFunctionExpression &expr, ...`
- `convertDefaultVar()`


---

## GNodeType

**Full name:** `gs::gopt::GNodeType`

**Public Methods:**

- `GNodeType(std::vector< catalog::NodeTableCatalogEntry * > n...`
- `GNodeType(const binder::NodeExpression &nodeExpr)`
- `getLabelIds() const`
- `toYAML() const`


---

## GPhysicalAnalyzer

**Full name:** `gs::gopt::GPhysicalAnalyzer`

**Public Methods:**

- `GPhysicalAnalyzer()`
- `analyze(const planner::LogicalPlan &plan)`


---

## GPhysicalConvertor

**Full name:** `gs::gopt::GPhysicalConvertor`

**Public Methods:**

- `GPhysicalConvertor(std::shared_ptr< GAliasManager > aliasManager, gs...`
- `convert(const planner::LogicalPlan &plan, bool skipSink=false)`


---

## GPrecedence

**Full name:** `gs::gopt::GPrecedence`

**Public Methods:**

- `getPrecedence(const binder::Expression &expr)`
- `isLeftAssociative(const binder::Expression &expr)`
- `needBrace(const binder::Expression &parent, const binder::E...`


---

## GQueryConvertor

**Full name:** `gs::gopt::GQueryConvertor`

**Public Methods:**

- `GQueryConvertor(std::shared_ptr< GAliasManager > aliasManager, gs...`
- `convert(const planner::LogicalPlan &plan, bool skipSink)`
- `skipColumn(const std::string &columnName)`


---

## GRelType

**Full name:** `gs::gopt::GRelType`

**Public Methods:**

- `GRelType(std::vector< catalog::GRelTableCatalogEntry * > relTables_)`
- `GRelType(const binder::RelExpression &relExpr)`
- `getLabelIds() const`
- `toYAML(catalog::Catalog *catalog) const`


---

## GResultSchema

**Full name:** `gs::gopt::GResultSchema`

**Public Methods:**

- `infer(const planner::LogicalPlan &plan, std::shared_ptr...`
- `convertType(const binder::Expression &expr, catalog::Catalog *catalog)`
- `inferFromExpr(const planner::LogicalPlan &plan)`


---

## GScalarType

**Full name:** `gs::gopt::GScalarType`

**Public Methods:**

- `GScalarType(const binder::Expression &expr)`
- `getType() const`
- `isArithmetic() const`
- `isTemporal() const`
- `isString() const`


---

## GTypeConverter

**Full name:** `gs::gopt::GTypeConverter`

**Public Methods:**

- `GTypeConverter()=default`
- `convertNodeType(const GNodeType &nodeType)`
- `convertRelType(const GRelType &relType)`
- `convertPathType(const GRelType &relType)`
- `convertArrayType(const common::LogicalType &type, const binder::Ex...`
- `convertLogicalType(const common::LogicalType &type, const binder::Ex...`
- `convertSimpleLogicalType(const common::LogicalType &type)`


---

## BoundGraphEntryTableInfo

**Full name:** `gs::graph::BoundGraphEntryTableInfo`

**Public Methods:**

- `BoundGraphEntryTableInfo(catalog::TableCatalogEntry *entry)`
- `BoundGraphEntryTableInfo(catalog::TableCatalogEntry *entry, std::shared_pt...`


---

## Graph

**Full name:** `gs::graph::Graph`

`Graph` interface to be use by GDS algorithms to get neighbors of nodes.

Instances of `Graph` are not expected to be thread-safe. Therefore, if `Graph` is intended to be used in a parallel manner, the user should first copy() an instance and give each thread a separate cop...

**Public Methods:**

- `Graph()=default`
- `~Graph()=default`
- `getGraphEntry()=0`
- `getNodeTableIDs() const =0`
- `getRelTableIDs() const =0`
- `getMaxOffsetMap(transaction::Transaction *transaction) const =0`
- `getMaxOffset(transaction::Transaction *transaction, common::ta...`
- `getNumNodes(transaction::Transaction *transaction) const =0`
- `getForwardNbrTableInfos(common::table_id_t srcNodeTableID)=0`
- `prepareRelScan(catalog::TableCatalogEntry *relEntry, catalog::Ta...`
- ... and 5 more methods


---

## EdgeIterator

**Full name:** `gs::graph::Graph::EdgeIterator`

**Public Methods:**

- `EdgeIterator(NbrScanState *scanState)`
- `DEFAULT_BOTH_MOVE(EdgeIterator)`
- `EdgeIterator(const EdgeIterator &other)=default`
- `EdgeIterator()`
- `operator*() const`
- `operator++()`
- `operator++(int)`
- `operator==(const EdgeIterator &other) const`
- `count() const`
- `collectNbrNodes()`
- ... and 2 more methods


---

## VertexIterator

**Full name:** `gs::graph::Graph::VertexIterator`

**Public Methods:**

- `VertexIterator(VertexScanState *scanState)`
- `DEFAULT_BOTH_MOVE(VertexIterator)`
- `VertexIterator(const VertexIterator &other)=default`
- `VertexIterator()`
- `operator*() const`
- `operator++()`
- `operator++(int)`
- `operator==(const VertexIterator &other) const`
- `begin() noexcept`
- `end() noexcept`


---

## GraphEntry

**Full name:** `gs::graph::GraphEntry`

**Public Methods:**

- `GraphEntry()=default`
- `GraphEntry(std::vector< catalog::TableCatalogEntry * > nodeE...`
- `EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry)`
- `isEmpty() const`
- `getNodeTableIDs() const`
- `getRelTableIDs() const`
- `getNodeEntries() const`
- `getRelEntries() const`
- `getRelInfo(common::table_id_t tableID) const`
- `setRelPredicate(std::shared_ptr< binder::Expression > predicate)`
- ... and 1 more methods


---

## GraphEntrySet

**Full name:** `gs::graph::GraphEntrySet`

**Public Methods:**

- `hasGraph(const std::string &name) const`
- `getEntry(const std::string &name) const`
- `addGraph(const std::string &name, const ParsedGraphEntry &entry)`
- `dropGraph(const std::string &name)`
- `begin()`
- `end()`
- `begin() const`
- `end() const`
- `cbegin() const`
- `cend() const`


---

## GraphEntryTableInfo

**Full name:** `gs::graph::GraphEntryTableInfo`

**Public Methods:**

- `GraphEntryTableInfo(std::string tableName, std::string predicate)`
- `toString() const`


---

## NbrScanState

**Full name:** `gs::graph::NbrScanState`

**Public Methods:**

- `~NbrScanState()=default`
- `getChunk()=0`
- `next()=0`


---

## Chunk

**Full name:** `gs::graph::NbrScanState::Chunk`

**Public Methods:**

- `EXPLICIT_COPY_METHOD(Chunk)`
- `forEach(Func &&func) const`
- `forEachBreakWhenFalse(Func &&func) const`
- `size() const`


---

## NbrTableInfo

**Full name:** `gs::graph::NbrTableInfo`

**Public Methods:**

- `NbrTableInfo(catalog::TableCatalogEntry *nodeEntry, catalog::T...`


---

## OnDiskGraph

**Full name:** `gs::graph::OnDiskGraph`

**Public Methods:**

- `OnDiskGraph(main::ClientContext *context, GraphEntry entry)`
- `getGraphEntry() override`
- `setNodeOffsetMask(common::NodeOffsetMaskMap *maskMap)`
- `getNodeTableIDs() const override`
- `getRelTableIDs() const override`
- `getMaxOffsetMap(transaction::Transaction *transaction) const override`
- `getMaxOffset(transaction::Transaction *transaction, common::ta...`
- `getNumNodes(transaction::Transaction *transaction) const override`
- `getForwardNbrTableInfos(common::table_id_t srcNodeTableID) override`
- `prepareRelScan(catalog::TableCatalogEntry *tableEntry, catalog::...`
- ... and 5 more methods


---

## OnDiskGraphNbrScanState

**Full name:** `gs::graph::OnDiskGraphNbrScanState`

**Public Methods:**

- `OnDiskGraphNbrScanState(main::ClientContext *context, catalog::TableCatal...`
- `OnDiskGraphNbrScanState(main::ClientContext *context, catalog::TableCatal...`
- `getChunk() override`
- `next() override`
- `startScan(common::RelDataDirection direction)`


---

## OnDiskGraphVertexScanState

**Full name:** `gs::graph::OnDiskGraphVertexScanState`

**Public Methods:**

- `OnDiskGraphVertexScanState(main::ClientContext &context, const catalog::Tabl...`
- `startScan(common::offset_t beginOffset, common::offset_t en...`
- `next() override`
- `getChunk() override`


---

## ParsedGraphEntry

**Full name:** `gs::graph::ParsedGraphEntry`


---

## VertexScanState

**Full name:** `gs::graph::VertexScanState`

**Public Methods:**

- `getChunk()=0`
- `next()=0`
- `~VertexScanState()=default`


---

## Chunk

**Full name:** `gs::graph::VertexScanState::Chunk`

**Public Methods:**

- `size() const`
- `getNodeIDs() const`
- `getProperties(size_t propertyIndex) const`


---

## TupleHash

**Full name:** `gs::hash_tuple::TupleHash`

**Public Methods:**

- `apply(std::size_t &seed, const Tuple &tuple)`


---

## TupleHash< Tuple, 0 >

**Full name:** `gs::hash_tuple::TupleHash< Tuple, 0 >`

**Public Methods:**

- `apply(std::size_t &seed, const Tuple &tuple)`


---

## hash

**Full name:** `gs::hash_tuple::hash`

**Public Methods:**

- `operator()(const std::tuple< Args... > &tuple) const`


---

## hash_combine

**Full name:** `gs::hash_tuple::hash_combine`


---

## value > >

**Full name:** `gs::hash_tuple::hash_combine< T, std::enable_if_t< is_tuple< T >::value > >`

**Public Methods:**

- `hash_combine(const T &val)`
- `operator()(std::size_t &seed) const`


---

## value > >

**Full name:** `gs::hash_tuple::hash_combine< T, std::enable_if_t<!is_tuple< T >::value > >`

**Public Methods:**

- `hash_combine(const T &val)`
- `operator()(std::size_t &seed) const`


---

## is_tuple

**Full name:** `gs::hash_tuple::is_tuple`


---

## tuple< Args... > >

**Full name:** `gs::hash_tuple::is_tuple< std::tuple< Args... > >`


---

## KeyBuffer

**Full name:** `gs::id_indexer_impl::KeyBuffer`

**Public Methods:**

- `serialize(std::unique_ptr< IOADAPTOR_T > &writer, const type &buffer)`
- `deserialize(std::unique_ptr< IOADAPTOR_T > &reader, type &buffer)`


---

## string >

**Full name:** `gs::id_indexer_impl::KeyBuffer< std::string >`

**Public Methods:**

- `serialize(std::unique_ptr< IOADAPTOR_T > &writer, const type &buffer)`
- `deserialize(std::unique_ptr< IOADAPTOR_T > &reader, type &buffer)`


---

## string_view >

**Full name:** `gs::id_indexer_impl::KeyBuffer< std::string_view >`

**Public Methods:**

- `serialize(std::unique_ptr< IOADAPTOR_T > &writer, const type &buffer)`
- `deserialize(std::unique_ptr< IOADAPTOR_T > &reader, type &buffer)`


---

## is_gs_result_type

**Full name:** `gs::is_gs_result_type`


---

## is_gs_result_type< Result< T > >

**Full name:** `gs::is_gs_result_type< Result< T > >`


---

## is_gs_status_type

**Full name:** `gs::is_gs_status_type`


---

## is_gs_status_type< Status >

**Full name:** `gs::is_gs_status_type< Status >`


---

## ActiveQuery

**Full name:** `gs::main::ActiveQuery`

**Public Methods:**

- `ActiveQuery()`
- `reset()`


---

## AutoCheckpointSetting

**Full name:** `gs::main::AutoCheckpointSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## CheckpointThresholdSetting

**Full name:** `gs::main::CheckpointThresholdSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ClientConfig

**Full name:** `gs::main::ClientConfig`


---

## ClientConfigDefault

**Full name:** `gs::main::ClientConfigDefault`


---

## ClientContext

**Full name:** `gs::main::ClientContext`

Contain client side configuration.

We make profiler associated per query, so profiler is not maintained in client context.

**Public Methods:**

- `ClientContext(MetadataManager *database)`
- `~ClientContext()`
- `getClientConfig() const`
- `getClientConfigUnsafe()`
- `getCurrentSetting(const std::string &optionName) const`
- `getTransaction() const`
- `tryReplace(const std::string &objectName) const`
- `setExtensionOption(std::string name, common::Value value)`
- `getExtensionOption(std::string optionName) const`
- `getExtensionDir() const`
- ... and 14 more methods


---

## ConfigurationOption

**Full name:** `gs::main::ConfigurationOption`

**Public Methods:**

- `ConfigurationOption(std::string name, common::LogicalTypeID parameter...`


---

## DisableMapKeyCheck

**Full name:** `gs::main::DisableMapKeyCheck`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableInternalCatalogSetting

**Full name:** `gs::main::EnableInternalCatalogSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableMVCCSetting

**Full name:** `gs::main::EnableMVCCSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableOptimizerSetting

**Full name:** `gs::main::EnableOptimizerSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableSemiMaskSetting

**Full name:** `gs::main::EnableSemiMaskSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableZoneMapSetting

**Full name:** `gs::main::EnableZoneMapSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ExtensionOption

**Full name:** `gs::main::ExtensionOption`

**Public Methods:**

- `ExtensionOption(std::string name, common::LogicalTypeID parameter...`


---

## FileSearchPathSetting

**Full name:** `gs::main::FileSearchPathSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ForceCheckpointClosingDBSetting

**Full name:** `gs::main::ForceCheckpointClosingDBSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## HomeDirectorySetting

**Full name:** `gs::main::HomeDirectorySetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## MetadataManager

**Full name:** `gs::main::MetadataManager`

Database class is the main class of Kuzu.

It manages all database components.

**Public Methods:**

- `MetadataManager()`
- `~MetadataManager()` - Destructs the database object
- `getCatalog()`
- `updateSchema(const std::filesystem::path &schemaPath)`
- `updateSchema(const std::string &schema)`
- `updateSchema(const YAML::Node &schema)`
- `updateStats(const std::filesystem::path &statsPath)`
- `updateStats(const std::string &stats)`


---

## OpProfileBox

**Full name:** `gs::main::OpProfileBox`

**Public Methods:**

- `OpProfileBox(std::string opName, const std::string &paramsName...`
- `getOpName() const`
- `getNumParams() const`
- `getParamsName(uint32_t idx) const`
- `getAttribute(uint32_t idx) const`
- `getNumAttributes() const`
- `getAttributeMaxLen() const`


---

## Option

**Full name:** `gs::main::Option`

**Public Methods:**

- `Option(std::string name, common::LogicalTypeID parameter...`
- `~Option()=default`


---

## PreparedStatement

**Full name:** `gs::main::PreparedStatement`

A prepared statement is a parameterized query which can avoid planning the same query for repeated execution.

**Public Methods:**

- `isTransactionStatement() const`
- `isSuccess() const`
- `getErrorMessage() const`
- `isReadOnly() const`
- `getParameterMap()`
- `getStatementType() const`
- `~PreparedStatement()`


---

## PreparedSummary

**Full name:** `gs::main::PreparedSummary`

`PreparedSummary` stores the compiling time and query options of a query.


---

## ProgressBarSetting

**Full name:** `gs::main::ProgressBarSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## QuerySummary

**Full name:** `gs::main::QuerySummary`

`QuerySummary` stores the execution time, plan, compiling time and query options of a query.

**Public Methods:**

- `getCompilingTime() const`
- `getExecutionTime() const`
- `incrementCompilingTime(double increment)`
- `incrementExecutionTime(double increment)`
- `setPreparedSummary(PreparedSummary preparedSummary_)`
- `isExplain() const`
- `getStatementType() const`


---

## RecursivePatternFactorSetting

**Full name:** `gs::main::RecursivePatternFactorSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## RecursivePatternSemanticSetting

**Full name:** `gs::main::RecursivePatternSemanticSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## SparseFrontierThresholdSetting

**Full name:** `gs::main::SparseFrontierThresholdSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## SpillToDiskSetting

**Full name:** `gs::main::SpillToDiskSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ThreadsSetting

**Full name:** `gs::main::ThreadsSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## TimeoutSetting

**Full name:** `gs::main::TimeoutSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## VarLengthExtendMaxDepthSetting

**Full name:** `gs::main::VarLengthExtendMaxDepthSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## WarningLimitSetting

**Full name:** `gs::main::WarningLimitSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## mmap_array

**Full name:** `gs::mmap_array`

**Public Methods:**

- `mmap_array()`
- `mmap_array(const mmap_array< T > &rhs)`
- `mmap_array(mmap_array &&rhs)`
- `~mmap_array()`
- `reset()`
- `unlink()`
- `set_hugepage_prefered(bool val)`
- `open(const std::string &filename, bool sync_to_file=fa...`
- `open_with_hugepages(const std::string &filename, size_t capacity=0)`
- `dump_without_close(const std::string &filename)`
- ... and 14 more methods


---

## string_view >

**Full name:** `gs::mmap_array< std::string_view >`

**Public Methods:**

- `mmap_array()`
- `mmap_array(mmap_array &&rhs)`
- `~mmap_array()`
- `reset()`
- `set_hugepage_prefered(bool val)`
- `open(const std::string &filename, bool sync_to_file=fa...`
- `open_with_hugepages(const std::string &filename)`
- `touch(const std::string &filename)`
- `dump(const std::string &filename)`
- `dump_without_close(const std::string &filename)`
- ... and 12 more methods


---

## mmap_vector

**Full name:** `gs::mmap_vector`

**Public Methods:**

- `mmap_vector()`
- `open(const std::string &filename, bool sync_to_file=true)`
- `reserve(size_t size)`
- `unlink()`
- `~mmap_vector()`
- `mmap_vector(mmap_vector &&other)`
- `push_back(const T &val)`
- `emplace_back(T &&val)`
- `resize(size_t size)`
- `size() const`
- ... and 5 more methods


---

## AggKeyDependencyOptimizer

**Full name:** `gs::optimizer::AggKeyDependencyOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## CardinalityUpdater

**Full name:** `gs::optimizer::CardinalityUpdater`

**Public Methods:**

- `CardinalityUpdater(const planner::CardinalityEstimator &cardinalityE...`
- `rewrite(planner::LogicalPlan *plan)`


---

## CorrelatedSubqueryUnnestSolver

**Full name:** `gs::optimizer::CorrelatedSubqueryUnnestSolver`

**Public Methods:**

- `CorrelatedSubqueryUnnestSolver(planner::LogicalOperator *accumulateOp)`
- `solve(planner::LogicalOperator *root_)`


---

## ExpandGetVFusion

**Full name:** `gs::optimizer::ExpandGetVFusion`

**Public Methods:**

- `ExpandGetVFusion(catalog::Catalog *catalog)`
- `~ExpandGetVFusion()=default`
- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitGetVReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `visitRecursiveExtendReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## FactorizationRewriter

**Full name:** `gs::optimizer::FactorizationRewriter`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(planner::LogicalOperator *op)`


---

## FilterPushDownOptimizer

**Full name:** `gs::optimizer::FilterPushDownOptimizer`

**Public Methods:**

- `FilterPushDownOptimizer(main::ClientContext *context)`
- `FilterPushDownOptimizer(main::ClientContext *context, PredicateSet predicateSet)`
- `rewrite(planner::LogicalPlan *plan)`


---

## FilterPushDownPattern

**Full name:** `gs::optimizer::FilterPushDownPattern`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `visitNodeLabelFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## FlatJoinToExpandOptimizer

**Full name:** `gs::optimizer::FlatJoinToExpandOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitHashJoinReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## HashJoinSIPOptimizer

**Full name:** `gs::optimizer::HashJoinSIPOptimizer`

**Public Methods:**

- `rewrite(const planner::LogicalPlan *plan)`


---

## LimitPushDownOptimizer

**Full name:** `gs::optimizer::LimitPushDownOptimizer`

**Public Methods:**

- `LimitPushDownOptimizer()`
- `rewrite(planner::LogicalPlan *plan)`


---

## LogicalFilterCollector

**Full name:** `gs::optimizer::LogicalFilterCollector`


---

## LogicalFlattenCollector

**Full name:** `gs::optimizer::LogicalFlattenCollector`


---

## LogicalIndexScanNodeCollector

**Full name:** `gs::optimizer::LogicalIndexScanNodeCollector`


---

## LogicalOperatorCollector

**Full name:** `gs::optimizer::LogicalOperatorCollector`

**Public Methods:**

- `~LogicalOperatorCollector() override=default`
- `collect(planner::LogicalOperator *op)`
- `hasOperators() const`
- `getOperators() const`


---

## LogicalOperatorVisitor

**Full name:** `gs::optimizer::LogicalOperatorVisitor`

**Public Methods:**

- `LogicalOperatorVisitor()=default`
- `~LogicalOperatorVisitor()=default`


---

## LogicalRecursiveExtendCollector

**Full name:** `gs::optimizer::LogicalRecursiveExtendCollector`


---

## LogicalScanNodeTableCollector

**Full name:** `gs::optimizer::LogicalScanNodeTableCollector`


---

## Optimizer

**Full name:** `gs::optimizer::Optimizer`

**Public Methods:**

- `optimize(planner::LogicalPlan *plan, main::ClientContext *...`


---

## PredicateSet

**Full name:** `gs::optimizer::PredicateSet`

**Public Methods:**

- `PredicateSet()=default`
- `EXPLICIT_COPY_DEFAULT_MOVE(PredicateSet)`
- `isEmpty() const`
- `clear()`
- `addPredicate(std::shared_ptr< binder::Expression > predicate)`
- `popNodePKEqualityComparison(const binder::Expression &nodeID)`
- `getAllPredicates()`


---

## ProjectionPushDownOptimizer

**Full name:** `gs::optimizer::ProjectionPushDownOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `ProjectionPushDownOptimizer(common::PathSemantic semantic)`


---

## RemoveFactorizationRewriter

**Full name:** `gs::optimizer::RemoveFactorizationRewriter`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`


---

## RemoveSubqueryAsJoin

**Full name:** `gs::optimizer::RemoveSubqueryAsJoin`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## RemoveUnnecessaryJoinOptimizer

**Full name:** `gs::optimizer::RemoveUnnecessaryJoinOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## SchemaPopulator

**Full name:** `gs::optimizer::SchemaPopulator`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## TopKOptimizer

**Full name:** `gs::optimizer::TopKOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`


---

## UnionAliasMapOptimizer

**Full name:** `gs::optimizer::UnionAliasMapOptimizer`

To guarantee all subqueries have the same ouput schema by adding LogicalAliasMap at the tail.

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitUnionReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## Alter

**Full name:** `gs::parser::Alter`

**Public Methods:**

- `Alter(AlterInfo info)`
- `getInfo() const`


---

## AlterInfo

**Full name:** `gs::parser::AlterInfo`

**Public Methods:**

- `AlterInfo(common::AlterType type, std::string tableName, st...`
- `DELETE_COPY_DEFAULT_MOVE(AlterInfo)`


---

## AttachDatabase

**Full name:** `gs::parser::AttachDatabase`

**Public Methods:**

- `AttachDatabase(AttachInfo attachInfo)`
- `getAttachInfo() const`


---

## AttachInfo

**Full name:** `gs::parser::AttachInfo`

**Public Methods:**

- `AttachInfo(std::string dbPath, std::string dbAlias, std::str...`


---

## BaseScanSource

**Full name:** `gs::parser::BaseScanSource`

**Public Methods:**

- `BaseScanSource(common::ScanSourceType type)`
- `~BaseScanSource()=default`
- `DELETE_COPY_AND_MOVE(BaseScanSource)`
- `ptrCast()`
- `constPtrCast() const`


---

## Copy

**Full name:** `gs::parser::Copy`

**Public Methods:**

- `Copy(common::StatementType type)`
- `setParsingOption(options_t options)`
- `getParsingOptions() const`


---

## CopyFrom

**Full name:** `gs::parser::CopyFrom`

**Public Methods:**

- `CopyFrom(std::unique_ptr< BaseScanSource > source, std::st...`
- `setByColumn()`
- `byColumn() const`
- `getSource() const`
- `getTableName() const`
- `setColumnInfo(CopyFromColumnInfo columnInfo_)`
- `getCopyColumnInfo() const`


---

## CopyFromColumnInfo

**Full name:** `gs::parser::CopyFromColumnInfo`

**Public Methods:**

- `CopyFromColumnInfo()=default`
- `CopyFromColumnInfo(bool inputColumnOrder, std::vector< std::string >...`


---

## CopyTo

**Full name:** `gs::parser::CopyTo`

**Public Methods:**

- `CopyTo(std::string filePath, std::unique_ptr< Statement ...`
- `getFilePath() const`
- `getStatement() const`


---

## CreateMacro

**Full name:** `gs::parser::CreateMacro`

**Public Methods:**

- `CreateMacro(std::string macroName, std::unique_ptr< ParsedExp...`
- `getMacroName() const`
- `getMacroExpression() const`
- `getPositionalArgs() const`
- `getDefaultArgs() const`


---

## CreateSequence

**Full name:** `gs::parser::CreateSequence`

**Public Methods:**

- `CreateSequence(CreateSequenceInfo info)`
- `getInfo() const`


---

## CreateSequenceInfo

**Full name:** `gs::parser::CreateSequenceInfo`

**Public Methods:**

- `CreateSequenceInfo(std::string sequenceName, common::ConflictAction ...`
- `EXPLICIT_COPY_DEFAULT_MOVE(CreateSequenceInfo)`


---

## CreateTable

**Full name:** `gs::parser::CreateTable`

**Public Methods:**

- `CreateTable(CreateTableInfo info)`
- `getInfo() const`


---

## CreateTableInfo

**Full name:** `gs::parser::CreateTableInfo`

**Public Methods:**

- `CreateTableInfo(catalog::CatalogEntryType type, std::string table...`
- `DELETE_COPY_DEFAULT_MOVE(CreateTableInfo)`


---

## CreateType

**Full name:** `gs::parser::CreateType`

**Public Methods:**

- `CreateType(std::string name, std::string dataType)`
- `getName() const`
- `getDataType() const`


---

## DatabaseStatement

**Full name:** `gs::parser::DatabaseStatement`

**Public Methods:**

- `DatabaseStatement(common::StatementType type, std::string dbName)`
- `getDBName() const`


---

## DeleteClause

**Full name:** `gs::parser::DeleteClause`

**Public Methods:**

- `DeleteClause(common::DeleteNodeType deleteType)`
- `addExpression(std::unique_ptr< ParsedExpression > expression)`
- `getDeleteClauseType() const`
- `getNumExpressions() const`
- `getExpression(uint32_t idx) const`


---

## DetachDatabase

**Full name:** `gs::parser::DetachDatabase`

**Public Methods:**

- `DetachDatabase(std::string dbName)`


---

## Drop

**Full name:** `gs::parser::Drop`

**Public Methods:**

- `Drop(DropInfo dropInfo)`
- `getDropInfo() const`


---

## DropInfo

**Full name:** `gs::parser::DropInfo`


---

## ExplainStatement

**Full name:** `gs::parser::ExplainStatement`

**Public Methods:**

- `ExplainStatement(std::unique_ptr< Statement > statementToExplain, ...`
- `getStatementToExplain() const`
- `getExplainType() const`


---

## ExportDB

**Full name:** `gs::parser::ExportDB`

**Public Methods:**

- `ExportDB(std::string filePath)`
- `setParsingOption(options_t options)`
- `getParsingOptionsRef() const`
- `getFilePath() const`


---

## ExtensionStatement

**Full name:** `gs::parser::ExtensionStatement`

**Public Methods:**

- `ExtensionStatement(std::unique_ptr< ExtensionAuxInfo > info)`
- `getAuxInfo() const`


---

## ExtraAddPropertyInfo

**Full name:** `gs::parser::ExtraAddPropertyInfo`

**Public Methods:**

- `ExtraAddPropertyInfo(std::string propertyName, std::string dataType, s...`


---

## ExtraAlterInfo

**Full name:** `gs::parser::ExtraAlterInfo`

**Public Methods:**

- `~ExtraAlterInfo()=default`
- `constPtrCast() const`
- `ptrCast()`


---

## ExtraCommentInfo

**Full name:** `gs::parser::ExtraCommentInfo`

**Public Methods:**

- `ExtraCommentInfo(std::string comment)`


---

## ExtraCreateNodeTableInfo

**Full name:** `gs::parser::ExtraCreateNodeTableInfo`

**Public Methods:**

- `ExtraCreateNodeTableInfo(std::string pKName)`


---

## ExtraCreateRelTableGroupInfo

**Full name:** `gs::parser::ExtraCreateRelTableGroupInfo`

**Public Methods:**

- `ExtraCreateRelTableGroupInfo(std::string relMultiplicity, std::vector< std::pa...`


---

## ExtraCreateRelTableInfo

**Full name:** `gs::parser::ExtraCreateRelTableInfo`

**Public Methods:**

- `ExtraCreateRelTableInfo(std::string relMultiplicity, std::string srcTable...`


---

## ExtraCreateTableInfo

**Full name:** `gs::parser::ExtraCreateTableInfo`

**Public Methods:**

- `~ExtraCreateTableInfo()=default`
- `constCast() const`


---

## ExtraDropPropertyInfo

**Full name:** `gs::parser::ExtraDropPropertyInfo`

**Public Methods:**

- `ExtraDropPropertyInfo(std::string propertyName)`


---

## ExtraRenamePropertyInfo

**Full name:** `gs::parser::ExtraRenamePropertyInfo`

**Public Methods:**

- `ExtraRenamePropertyInfo(std::string propertyName, std::string newName)`


---

## ExtraRenameTableInfo

**Full name:** `gs::parser::ExtraRenameTableInfo`

**Public Methods:**

- `ExtraRenameTableInfo(std::string newName)`


---

## FileScanSource

**Full name:** `gs::parser::FileScanSource`

**Public Methods:**

- `FileScanSource(std::vector< std::string > paths)`


---

## ImportDB

**Full name:** `gs::parser::ImportDB`

**Public Methods:**

- `ImportDB(std::string filePath)`
- `getFilePath() const`


---

## InQueryCallClause

**Full name:** `gs::parser::InQueryCallClause`

**Public Methods:**

- `InQueryCallClause(std::unique_ptr< ParsedExpression > functionExpre...`
- `getFunctionExpression() const`
- `getYieldVariables() const`


---

## InsertClause

**Full name:** `gs::parser::InsertClause`

**Public Methods:**

- `InsertClause(std::vector< PatternElement > patternElements)`
- `getPatternElementsRef() const`


---

## JoinHintNode

**Full name:** `gs::parser::JoinHintNode`

**Public Methods:**

- `JoinHintNode()=default`
- `JoinHintNode(std::string name)`
- `addChild(std::shared_ptr< JoinHintNode > child)`
- `isLeaf() const`


---

## KuzuCypherParser

**Full name:** `gs::parser::KuzuCypherParser`

**Public Methods:**

- `KuzuCypherParser(antlr4::TokenStream *input)`
- `notifyQueryNotConcludeWithReturn(antlr4::Token *startToken) override`
- `notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token *startToken) override`
- `notifyInvalidNotEqualOperator(antlr4::Token *startToken) override`
- `notifyEmptyToken(antlr4::Token *startToken) override`
- `notifyReturnNotAtEnd(antlr4::Token *startToken) override`
- `notifyNonBinaryComparison(antlr4::Token *startToken) override`


---

## LoadFrom

**Full name:** `gs::parser::LoadFrom`

**Public Methods:**

- `LoadFrom(std::unique_ptr< BaseScanSource > source)`
- `getSource() const`
- `setParingOptions(options_t options)`
- `getParsingOptions() const`
- `setPropertyDefinitions(std::vector< ParsedColumnDefinition > definitions)`
- `getColumnDefinitions() const`


---

## MacroParameterReplacer

**Full name:** `gs::parser::MacroParameterReplacer`

**Public Methods:**

- `MacroParameterReplacer(const std::unordered_map< std::string, ParsedExpr...`
- `replace(std::unique_ptr< ParsedExpression > input)`


---

## MatchClause

**Full name:** `gs::parser::MatchClause`

**Public Methods:**

- `MatchClause(std::vector< PatternElement > patternElements, co...`
- `getPatternElementsRef() const`
- `getMatchClauseType() const`
- `setHint(std::shared_ptr< JoinHintNode > root)`
- `hasHint() const`
- `getHint() const`


---

## MergeClause

**Full name:** `gs::parser::MergeClause`

**Public Methods:**

- `MergeClause(std::vector< PatternElement > patternElements)`
- `getPatternElementsRef() const`
- `addOnMatchSetItems(parsed_expr_pair setItem)`
- `hasOnMatchSetItems() const`
- `getOnMatchSetItemsRef() const`
- `addOnCreateSetItems(parsed_expr_pair setItem)`
- `hasOnCreateSetItems() const`
- `getOnCreateSetItemsRef() const`


---

## NodePattern

**Full name:** `gs::parser::NodePattern`

**Public Methods:**

- `NodePattern(std::string name, std::vector< std::string > tabl...`
- `DELETE_COPY_DEFAULT_MOVE(NodePattern)`
- `~NodePattern()=default`
- `getVariableName() const`
- `getTableNames() const`
- `getPropertyKeyVals() const`


---

## ObjectScanSource

**Full name:** `gs::parser::ObjectScanSource`

**Public Methods:**

- `ObjectScanSource(std::vector< std::string > objectNames)`


---

## ParsedCaseAlternative

**Full name:** `gs::parser::ParsedCaseAlternative`

**Public Methods:**

- `ParsedCaseAlternative()=default`
- `ParsedCaseAlternative(std::unique_ptr< ParsedExpression > whenExpressio...`
- `ParsedCaseAlternative(const ParsedCaseAlternative &other)`
- `DEFAULT_BOTH_MOVE(ParsedCaseAlternative)`
- `serialize(common::Serializer &serializer) const`
- `deserialize(common::Deserializer &deserializer)`


---

## ParsedCaseExpression

**Full name:** `gs::parser::ParsedCaseExpression`

**Public Methods:**

- `ParsedCaseExpression(std::string raw)`
- `ParsedCaseExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedCaseExpression(std::unique_ptr< ParsedExpression > caseExpressio...`
- `setCaseExpression(std::unique_ptr< ParsedExpression > expression)`
- `hasCaseExpression() const`
- `getCaseExpression() const`
- `addCaseAlternative(ParsedCaseAlternative caseAlternative)`
- `getNumCaseAlternative() const`
- `getCaseAlternativeUnsafe(uint32_t idx)`
- `getCaseAlternative(uint32_t idx) const`
- ... and 5 more methods


---

## ParsedColumnDefinition

**Full name:** `gs::parser::ParsedColumnDefinition`

**Public Methods:**

- `ParsedColumnDefinition(std::string name, std::string type)`
- `EXPLICIT_COPY_DEFAULT_MOVE(ParsedColumnDefinition)`


---

## ParsedExpression

**Full name:** `gs::parser::ParsedExpression`

**Public Methods:**

- `ParsedExpression(common::ExpressionType type, std::unique_ptr< Par...`
- `ParsedExpression(common::ExpressionType type, std::unique_ptr< Par...`
- `ParsedExpression(common::ExpressionType type, std::string rawName)`
- `ParsedExpression(common::ExpressionType type)`
- `ParsedExpression(common::ExpressionType type, std::string alias, s...`
- `DELETE_COPY_DEFAULT_MOVE(ParsedExpression)`
- `~ParsedExpression()=default`
- `getExpressionType() const`
- `setAlias(std::string name)`
- `hasAlias() const`
- ... and 12 more methods


---

## ParsedExpressionVisitor

**Full name:** `gs::parser::ParsedExpressionVisitor`

**Public Methods:**

- `~ParsedExpressionVisitor()=default`
- `visit(const ParsedExpression *expr)`
- `visitUnsafe(ParsedExpression *expr)`
- `visitSwitch(const ParsedExpression *expr)`
- `visitFunctionExpr(const ParsedExpression *)`
- `visitAggFunctionExpr(const ParsedExpression *)`
- `visitPropertyExpr(const ParsedExpression *)`
- `visitLiteralExpr(const ParsedExpression *)`
- `visitVariableExpr(const ParsedExpression *)`
- `visitPathExpr(const ParsedExpression *)`
- ... and 12 more methods


---

## ParsedFunctionExpression

**Full name:** `gs::parser::ParsedFunctionExpression`

**Public Methods:**

- `ParsedFunctionExpression(std::string functionName, std::string rawName, bo...`
- `ParsedFunctionExpression(std::string functionName, std::unique_ptr< Parsed...`
- `ParsedFunctionExpression(std::string functionName, std::unique_ptr< Parsed...`
- `ParsedFunctionExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedFunctionExpression(std::string functionName, bool isDistinct)`
- `getIsDistinct() const`
- `getFunctionName() const`
- `getNormalizedFunctionName() const`
- `addChild(std::unique_ptr< ParsedExpression > child)`
- `addOptionalParams(std::string name, std::unique_ptr< ParsedExpressi...`
- ... and 3 more methods


---

## ParsedLambdaExpression

**Full name:** `gs::parser::ParsedLambdaExpression`

**Public Methods:**

- `ParsedLambdaExpression(std::vector< std::string > varNames, std::unique_...`
- `getVarNames() const`
- `getFunctionExpr() const`
- `copy() const override`


---

## ParsedLiteralExpression

**Full name:** `gs::parser::ParsedLiteralExpression`

**Public Methods:**

- `ParsedLiteralExpression(common::Value value, std::string raw)`
- `ParsedLiteralExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedLiteralExpression(common::Value value)`
- `getValue() const`
- `copy() const override`
- `deserialize(common::Deserializer &deserializer)`


---

## ParsedParamExprCollector

**Full name:** `gs::parser::ParsedParamExprCollector`

**Public Methods:**

- `getParamExprs() const`
- `hasParamExprs() const`
- `visitParamExpr(const ParsedExpression *expr) override`


---

## ParsedParameterExpression

**Full name:** `gs::parser::ParsedParameterExpression`

**Public Methods:**

- `ParsedParameterExpression(std::string parameterName, std::string raw)`
- `getParameterName() const`
- `copy() const override`
- `deserialize(common::Deserializer &)`


---

## ParsedPropertyDefinition

**Full name:** `gs::parser::ParsedPropertyDefinition`

**Public Methods:**

- `ParsedPropertyDefinition(ParsedColumnDefinition columnDefinition, std::uni...`
- `EXPLICIT_COPY_DEFAULT_MOVE(ParsedPropertyDefinition)`
- `getName() const`
- `getType() const`


---

## ParsedPropertyExpression

**Full name:** `gs::parser::ParsedPropertyExpression`

**Public Methods:**

- `ParsedPropertyExpression(std::string propertyName, std::unique_ptr< Parsed...`
- `ParsedPropertyExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedPropertyExpression(std::string propertyName)`
- `getPropertyName() const`
- `isStar() const`
- `copy() const override`
- `deserialize(common::Deserializer &deserializer)`


---

## ParsedSubqueryExpression

**Full name:** `gs::parser::ParsedSubqueryExpression`

**Public Methods:**

- `ParsedSubqueryExpression(common::SubqueryType subqueryType, std::string rawName)`
- `getSubqueryType() const`
- `addPatternElement(PatternElement element)`
- `setPatternElements(std::vector< PatternElement > elements)`
- `getPatternElements() const`
- `setWhereClause(std::unique_ptr< ParsedExpression > expression)`
- `hasWhereClause() const`
- `getWhereClause() const`
- `setHint(std::shared_ptr< JoinHintNode > root)`
- `hasHint() const`
- ... and 3 more methods


---

## ParsedVariableExpression

**Full name:** `gs::parser::ParsedVariableExpression`

**Public Methods:**

- `ParsedVariableExpression(std::string variableName, std::string raw)`
- `ParsedVariableExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedVariableExpression(std::string variableName)`
- `getVariableName() const`
- `copy() const override`
- `deserialize(common::Deserializer &deserializer)`


---

## Parser

**Full name:** `gs::parser::Parser`

**Public Methods:**

- `parseQuery(std::string_view query)`


---

## ParserErrorListener

**Full name:** `gs::parser::ParserErrorListener`

**Public Methods:**

- `syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *of...`


---

## ParserErrorStrategy

**Full name:** `gs::parser::ParserErrorStrategy`


---

## PatternElement

**Full name:** `gs::parser::PatternElement`

**Public Methods:**

- `PatternElement(NodePattern nodePattern)`
- `DELETE_COPY_DEFAULT_MOVE(PatternElement)`
- `setPathName(std::string name)`
- `hasPathName() const`
- `getPathName() const`
- `getFirstNodePattern() const`
- `addPatternElementChain(PatternElementChain chain)`
- `getNumPatternElementChains() const`
- `getPatternElementChain(uint32_t idx) const`


---

## PatternElementChain

**Full name:** `gs::parser::PatternElementChain`

**Public Methods:**

- `PatternElementChain(RelPattern relPattern, NodePattern nodePattern)`
- `DELETE_COPY_DEFAULT_MOVE(PatternElementChain)`
- `getRelPattern() const`
- `getNodePattern() const`


---

## ProjectionBody

**Full name:** `gs::parser::ProjectionBody`

**Public Methods:**

- `ProjectionBody(bool isDistinct, std::vector< std::unique_ptr< Pa...`
- `DELETE_COPY_DEFAULT_MOVE(ProjectionBody)`
- `getIsDistinct() const`
- `getProjectionExpressions() const`
- `setOrderByExpressions(std::vector< std::unique_ptr< ParsedExpression > ...`
- `hasOrderByExpressions() const`
- `getOrderByExpressions() const`
- `getSortOrders() const`
- `setSkipExpression(std::unique_ptr< ParsedExpression > expression)`
- `hasSkipExpression() const`
- ... and 4 more methods


---

## QueryPart

**Full name:** `gs::parser::QueryPart`

**Public Methods:**

- `QueryPart(WithClause withClause)`
- `getNumUpdatingClauses() const`
- `getUpdatingClause(uint32_t idx) const`
- `addUpdatingClause(std::unique_ptr< UpdatingClause > updatingClause)`
- `getNumReadingClauses() const`
- `getReadingClause(uint32_t idx) const`
- `addReadingClause(std::unique_ptr< ReadingClause > readingClause)`
- `getWithClause() const`


---

## QueryScanSource

**Full name:** `gs::parser::QueryScanSource`

**Public Methods:**

- `QueryScanSource(std::unique_ptr< Statement > statement)`


---

## ReadWriteExprAnalyzer

**Full name:** `gs::parser::ReadWriteExprAnalyzer`

**Public Methods:**

- `ReadWriteExprAnalyzer(main::ClientContext *context)`
- `isReadOnly() const`
- `visitFunctionExpr(const ParsedExpression *expr) override`


---

## ReadingClause

**Full name:** `gs::parser::ReadingClause`

**Public Methods:**

- `ReadingClause(common::ClauseType clauseType)`
- `~ReadingClause()=default`
- `getClauseType() const`
- `setWherePredicate(std::unique_ptr< ParsedExpression > expression)`
- `hasWherePredicate() const`
- `getWherePredicate() const`
- `constCast() const`


---

## RecursiveRelPatternInfo

**Full name:** `gs::parser::RecursiveRelPatternInfo`

**Public Methods:**

- `RecursiveRelPatternInfo()=default`
- `DELETE_COPY_DEFAULT_MOVE(RecursiveRelPatternInfo)`


---

## RegularQuery

**Full name:** `gs::parser::RegularQuery`

**Public Methods:**

- `RegularQuery(SingleQuery singleQuery)`
- `addSingleQuery(SingleQuery singleQuery, bool isUnionAllQuery)`
- `getNumSingleQueries() const`
- `getSingleQuery(common::idx_t singleQueryIdx) const`
- `getIsUnionAll() const`
- `setPreQueryPart(std::vector< QueryPart > preQueryPart)`
- `setPostSingleQuery(SingleQuery postSingleQuery)`
- `setPreQueryExpressions(std::vector< std::unique_ptr< ParsedExpression > ...`
- `getPreQueryPart() const`
- `getPostSingleQuery() const`
- ... and 1 more methods


---

## RelPattern

**Full name:** `gs::parser::RelPattern`

**Public Methods:**

- `RelPattern(std::string name, std::vector< std::string > tabl...`
- `DELETE_COPY_DEFAULT_MOVE(RelPattern)`
- `getRelType() const`
- `getDirection() const`
- `getRecursiveInfo() const`


---

## ReturnClause

**Full name:** `gs::parser::ReturnClause`

**Public Methods:**

- `ReturnClause(ProjectionBody projectionBody)`
- `DELETE_COPY_DEFAULT_MOVE(ReturnClause)`
- `~ReturnClause()=default`
- `getProjectionBody() const`


---

## SetClause

**Full name:** `gs::parser::SetClause`

**Public Methods:**

- `SetClause()`
- `addSetItem(parsed_expr_pair setItem)`
- `getSetItemsRef() const`


---

## SingleQuery

**Full name:** `gs::parser::SingleQuery`

**Public Methods:**

- `SingleQuery()=default`
- `DELETE_COPY_DEFAULT_MOVE(SingleQuery)`
- `addQueryPart(QueryPart queryPart)`
- `getNumQueryParts() const`
- `getQueryPart(uint32_t idx) const`
- `getNumUpdatingClauses() const`
- `getUpdatingClause(uint32_t idx) const`
- `addUpdatingClause(std::unique_ptr< UpdatingClause > updatingClause)`
- `getNumReadingClauses() const`
- `getReadingClause(uint32_t idx) const`
- ... and 4 more methods


---

## StandaloneCall

**Full name:** `gs::parser::StandaloneCall`

**Public Methods:**

- `StandaloneCall(std::string optionName, std::unique_ptr< ParsedEx...`
- `getOptionName() const`
- `getOptionValue() const`


---

## StandaloneCallFunction

**Full name:** `gs::parser::StandaloneCallFunction`

**Public Methods:**

- `StandaloneCallFunction(std::unique_ptr< ParsedExpression > functionExpression)`
- `getFunctionExpression() const`


---

## StandaloneCallRewriter

**Full name:** `gs::parser::StandaloneCallRewriter`

**Public Methods:**

- `StandaloneCallRewriter(main::ClientContext *context, bool allowRewrite)`
- `getRewriteQuery(const Statement &statement)`


---

## Statement

**Full name:** `gs::parser::Statement`

**Public Methods:**

- `Statement(common::StatementType statementType)`
- `~Statement()=default`
- `getStatementType() const`
- `setToInternal()`
- `isInternal() const`
- `setParsingTime(double time)`
- `getParsingTime() const`
- `requireTransaction() const`
- `cast()`
- `constCast() const`
- ... and 1 more methods


---

## StatementReadWriteAnalyzer

**Full name:** `gs::parser::StatementReadWriteAnalyzer`

**Public Methods:**

- `StatementReadWriteAnalyzer(main::ClientContext *context)`
- `isReadOnly() const`


---

## StatementVisitor

**Full name:** `gs::parser::StatementVisitor`

**Public Methods:**

- `StatementVisitor()=default`
- `~StatementVisitor()=default`
- `visit(const Statement &statement)`


---

## TableFuncScanSource

**Full name:** `gs::parser::TableFuncScanSource`

**Public Methods:**

- `TableFuncScanSource(std::unique_ptr< ParsedExpression > functionExpression)`


---

## TransactionStatement

**Full name:** `gs::parser::TransactionStatement`

**Public Methods:**

- `TransactionStatement(transaction::TransactionAction transactionAction)`
- `getTransactionAction() const`


---

## Transformer

**Full name:** `gs::parser::Transformer`

**Public Methods:**

- `Transformer(CypherParser::Neug_StatementsContext &root)`
- `transform()`


---

## UnwindClause

**Full name:** `gs::parser::UnwindClause`

**Public Methods:**

- `UnwindClause(std::unique_ptr< ParsedExpression > expression, s...`
- `getExpression() const`
- `getAlias() const`


---

## UpdatingClause

**Full name:** `gs::parser::UpdatingClause`

**Public Methods:**

- `UpdatingClause(common::ClauseType clauseType)`
- `~UpdatingClause()=default`
- `getClauseType() const`
- `constCast() const`


---

## UseDatabase

**Full name:** `gs::parser::UseDatabase`

**Public Methods:**

- `UseDatabase(std::string dbName)`


---

## WithClause

**Full name:** `gs::parser::WithClause`

**Public Methods:**

- `WithClause(ProjectionBody projectionBody)`
- `DELETE_COPY_DEFAULT_MOVE(WithClause)`
- `setWhereExpression(std::unique_ptr< ParsedExpression > expression)`
- `hasWhereExpression() const`
- `getWhereExpression() const`


---

## YieldVariable

**Full name:** `gs::parser::YieldVariable`

**Public Methods:**

- `YieldVariable(std::string name, std::string alias)`
- `hasAlias() const`


---

## BaseLogicalExtend

**Full name:** `gs::planner::BaseLogicalExtend`

**Public Methods:**

- `BaseLogicalExtend(LogicalOperatorType operatorType, std::shared_ptr...`
- `getBoundNode() const`
- `getNbrNode() const`
- `getRel() const`
- `isRecursive() const`
- `getDirection() const`
- `extendFromSourceNode() const`
- `getGroupsPosToFlatten()=0`
- `getExpressionsForPrinting() const override`
- `getPrintInfo() const override`


---

## BaseLogicalExtendPrintInfo

**Full name:** `gs::planner::BaseLogicalExtendPrintInfo`

**Public Methods:**

- `BaseLogicalExtendPrintInfo(std::shared_ptr< binder::NodeExpression > boundNo...`
- `toString() const override`


---

## CardinalityEstimator

**Full name:** `gs::planner::CardinalityEstimator`

**Public Methods:**

- `CardinalityEstimator()`
- `CardinalityEstimator(main::ClientContext *context)`
- `DELETE_COPY_DEFAULT_MOVE(CardinalityEstimator)`
- `initNodeIDDom(const transaction::Transaction *transaction, cons...`
- `addNodeIDDomAndStats(const transaction::Transaction *transaction, cons...`
- `addPerQueryGraphNodeIDDom(const binder::Expression &nodeID, cardinality_t numNodes)`
- `clearPerQueryGraphStats()`
- `estimateScanNode(const LogicalOperator &op) const`
- `estimateHashJoin(const std::vector< binder::expression_pair > &joi...`
- `estimateCrossProduct(const LogicalOperator &probeOp, const LogicalOper...`
- ... and 7 more methods


---

## CostModel

**Full name:** `gs::planner::CostModel`

**Public Methods:**

- `computeExtendCost(const LogicalPlan &childPlan)`
- `computeHashJoinCost(const std::vector< binder::expression_pair > &joi...`
- `computeHashJoinCost(const binder::expression_vector &joinNodeIDs, con...`
- `computeMarkJoinCost(const std::vector< binder::expression_pair > &joi...`
- `computeMarkJoinCost(const binder::expression_vector &joinNodeIDs, con...`
- `computeIntersectCost(const LogicalPlan &probePlan, const std::vector< ...`
- `estimateIntersectCostByCard(const gs::planner::LogicalPlan &probePlan, const ...`


---

## DPLevel

**Full name:** `gs::planner::DPLevel`

**Public Methods:**

- `contains(const binder::SubqueryGraph &subqueryGraph)`
- `getSubgraphPlans(const binder::SubqueryGraph &subqueryGraph)`
- `getSubqueryGraphs()`
- `addPlan(const binder::SubqueryGraph &subqueryGraph, std::...`
- `clear()`


---

## ExtraJoinTreeNodeInfo

**Full name:** `gs::planner::ExtraJoinTreeNodeInfo`

**Public Methods:**

- `ExtraJoinTreeNodeInfo(std::shared_ptr< binder::NodeExpression > joinNode)`
- `ExtraJoinTreeNodeInfo(std::vector< std::shared_ptr< binder::NodeExpress...`
- `ExtraJoinTreeNodeInfo(const ExtraJoinTreeNodeInfo &other)`
- `copy() const override`


---

## ExtraKeyInfo

**Full name:** `gs::planner::ExtraKeyInfo`

**Public Methods:**

- `~ExtraKeyInfo()=default`
- `constCast() const`
- `copy() const =0`


---

## ExtraNodeIDListKeyInfo

**Full name:** `gs::planner::ExtraNodeIDListKeyInfo`

**Public Methods:**

- `ExtraNodeIDListKeyInfo(std::shared_ptr< binder::Expression > srcNodeID, ...`
- `copy() const override`


---

## ExtraPathKeyInfo

**Full name:** `gs::planner::ExtraPathKeyInfo`

**Public Methods:**

- `ExtraPathKeyInfo(common::ExtendDirection direction)`
- `copy() const override`


---

## ExtraScanNodeTableInfo

**Full name:** `gs::planner::ExtraScanNodeTableInfo`

**Public Methods:**

- `~ExtraScanNodeTableInfo()=default`
- `copy() const =0`
- `constCast() const`


---

## ExtraScanTreeNodeInfo

**Full name:** `gs::planner::ExtraScanTreeNodeInfo`

**Public Methods:**

- `ExtraScanTreeNodeInfo()=default`
- `ExtraScanTreeNodeInfo(const ExtraScanTreeNodeInfo &other)`
- `merge(const ExtraScanTreeNodeInfo &other)`
- `copy() const override`


---

## ExtraTreeNodeInfo

**Full name:** `gs::planner::ExtraTreeNodeInfo`

**Public Methods:**

- `~ExtraTreeNodeInfo()=default`
- `copy() const =0`
- `constCast() const`
- `cast()`


---

## FactorizationGroup

**Full name:** `gs::planner::FactorizationGroup`

**Public Methods:**

- `FactorizationGroup()`
- `FactorizationGroup(const FactorizationGroup &other)`
- `setFlat()`
- `isFlat() const`
- `setSingleState()`
- `isSingleState() const`
- `setMultiplier(double multiplier)`
- `getMultiplier() const`
- `insertExpression(const std::shared_ptr< binder::Expression > &expression)`
- `getExpressions() const`
- ... and 1 more methods


---

## FlattenAll

**Full name:** `gs::planner::FlattenAll`

**Public Methods:**

- `getGroupsPosToFlatten(const binder::expression_vector &exprs, const Sch...`
- `getGroupsPosToFlatten(std::shared_ptr< binder::Expression > expr, const...`
- `getGroupsPosToFlatten(const std::unordered_set< f_group_pos > &dependen...`


---

## FlattenAllButOne

**Full name:** `gs::planner::FlattenAllButOne`

**Public Methods:**

- `getGroupsPosToFlatten(const binder::expression_vector &exprs, const Sch...`
- `getGroupsPosToFlatten(std::shared_ptr< binder::Expression > expr, const...`
- `getGroupsPosToFlatten(const std::unordered_set< f_group_pos > &dependen...`


---

## GroupDependencyAnalyzer

**Full name:** `gs::planner::GroupDependencyAnalyzer`

**Public Methods:**

- `GroupDependencyAnalyzer(bool collectDependentExpr, const Schema &schema)`
- `getDependentExprs() const`
- `getDependentGroups() const`
- `getRequiredFlatGroups() const`
- `visit(std::shared_ptr< binder::Expression > expr)`


---

## JoinOrderEnumeratorContext

**Full name:** `gs::planner::JoinOrderEnumeratorContext`

**Public Methods:**

- `JoinOrderEnumeratorContext()`
- `DELETE_COPY_DEFAULT_MOVE(JoinOrderEnumeratorContext)`
- `init(const binder::QueryGraph *queryGraph, const binde...`
- `getWhereExpressions()`
- `containPlans(const binder::SubqueryGraph &subqueryGraph) const`
- `getPlans(const binder::SubqueryGraph &subqueryGraph) const`
- `addPlan(const binder::SubqueryGraph &subqueryGraph, std::...`
- `getEmptySubqueryGraph() const`
- `getFullyMatchedSubqueryGraph() const`
- `getQueryGraph()`
- ... and 1 more methods


---

## JoinOrderUtil

**Full name:** `gs::planner::JoinOrderUtil`

**Public Methods:**

- `getJoinKeysFlatCardinality(const binder::expression_vector &joinNodeIDs, con...`


---

## JoinPlanSolver

**Full name:** `gs::planner::JoinPlanSolver`

**Public Methods:**

- `JoinPlanSolver(Planner *planner)`
- `solve(const JoinTree &joinTree)`


---

## JoinTree

**Full name:** `gs::planner::JoinTree`

**Public Methods:**

- `JoinTree(std::shared_ptr< JoinTreeNode > root)`
- `JoinTree(const JoinTree &other)`


---

## JoinTreeConstructor

**Full name:** `gs::planner::JoinTreeConstructor`

**Public Methods:**

- `JoinTreeConstructor(const binder::QueryGraph &queryGraph, const Prope...`
- `construct(std::shared_ptr< binder::BoundJoinHintNode > root)`


---

## IntermediateResult

**Full name:** `gs::planner::JoinTreeConstructor::IntermediateResult`


---

## JoinTreeNode

**Full name:** `gs::planner::JoinTreeNode`

**Public Methods:**

- `JoinTreeNode(TreeNodeType type, std::unique_ptr< ExtraTreeNode...`
- `DELETE_COPY_DEFAULT_MOVE(JoinTreeNode)`
- `toString() const`
- `addChild(std::shared_ptr< JoinTreeNode > child)`


---

## LogicalAccumulate

**Full name:** `gs::planner::LogicalAccumulate`

**Public Methods:**

- `LogicalAccumulate(common::AccumulateType accumulateType, binder::ex...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getGroupPositionsToFlatten() const`
- `getExpressionsForPrinting() const override`
- `getAccumulateType() const`
- `getPayloads() const`
- `hasMark() const`
- `getMark() const`
- `copy() override`


---

## LogicalAggregate

**Full name:** `gs::planner::LogicalAggregate`

**Public Methods:**

- `LogicalAggregate(binder::expression_vector keys, binder::expressio...`
- `LogicalAggregate(binder::expression_vector keys, binder::expressio...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getGroupsPosToFlatten()`
- `getExpressionsForPrinting() const override`
- `hasKeys() const`
- `getKeys() const`
- `setKeys(binder::expression_vector expressions)`
- `getDependentKeys() const`
- ... and 4 more methods


---

## LogicalAggregatePrintInfo

**Full name:** `gs::planner::LogicalAggregatePrintInfo`

**Public Methods:**

- `LogicalAggregatePrintInfo(binder::expression_vector keys, binder::expressio...`
- `toString() const override`
- `copy() const override`


---

## LogicalAliasMap

**Full name:** `gs::planner::LogicalAliasMap`

Generated by UnionAliasMapOptimizer to guarantee all subqueries have the same output schema.

**Public Methods:**

- `LogicalAliasMap(const binder::expression_vector &srcExprs, const ...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`
- `getSrcExprs() const`
- `getDstExprs() const`


---

## LogicalAlter

**Full name:** `gs::planner::LogicalAlter`

**Public Methods:**

- `LogicalAlter(binder::BoundAlterInfo info, std::shared_ptr< bin...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalAlterPrintInfo

**Full name:** `gs::planner::LogicalAlterPrintInfo`

**Public Methods:**

- `LogicalAlterPrintInfo(binder::BoundAlterInfo info)`
- `toString() const override`
- `copy() const override`
- `LogicalAlterPrintInfo(const LogicalAlterPrintInfo &other)`


---

## LogicalAttachDatabase

**Full name:** `gs::planner::LogicalAttachDatabase`

**Public Methods:**

- `LogicalAttachDatabase(binder::AttachInfo attachInfo, std::shared_ptr< b...`
- `getAttachInfo() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalAttachDatabasePrintInfo

**Full name:** `gs::planner::LogicalAttachDatabasePrintInfo`

**Public Methods:**

- `LogicalAttachDatabasePrintInfo(std::string dbName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCopyFrom

**Full name:** `gs::planner::LogicalCopyFrom`

**Public Methods:**

- `LogicalCopyFrom(binder::BoundCopyFromInfo info, binder::expressio...`
- `LogicalCopyFrom(binder::BoundCopyFromInfo info, binder::expressio...`
- `getExpressionsForPrinting() const override`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getInfo() const`
- `getInfo()`
- `getOutExprs() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalCopyFromPrintInfo

**Full name:** `gs::planner::LogicalCopyFromPrintInfo`

**Public Methods:**

- `LogicalCopyFromPrintInfo(std::string tableName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCopyTo

**Full name:** `gs::planner::LogicalCopyTo`

**Public Methods:**

- `LogicalCopyTo(std::unique_ptr< function::ExportFuncBindData > b...`
- `getGroupsPosToFlatten()`
- `getExpressionsForPrinting() const override`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getBindData() const`
- `getExportFunc() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalCopyToPrintInfo

**Full name:** `gs::planner::LogicalCopyToPrintInfo`

**Public Methods:**

- `LogicalCopyToPrintInfo(std::vector< std::string > columnNames, std::stri...`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateMacro

**Full name:** `gs::planner::LogicalCreateMacro`

**Public Methods:**

- `LogicalCreateMacro(std::shared_ptr< binder::Expression > outputExpre...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getOutputExpression() const`
- `getMacroName() const`
- `getMacro() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalCreateMacroPrintInfo

**Full name:** `gs::planner::LogicalCreateMacroPrintInfo`

**Public Methods:**

- `LogicalCreateMacroPrintInfo(std::string macroName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateSequence

**Full name:** `gs::planner::LogicalCreateSequence`

**Public Methods:**

- `LogicalCreateSequence(binder::BoundCreateSequenceInfo info, std::shared...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() final`


---

## LogicalCreateSequencePrintInfo

**Full name:** `gs::planner::LogicalCreateSequencePrintInfo`

**Public Methods:**

- `LogicalCreateSequencePrintInfo(std::string sequenceName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateTable

**Full name:** `gs::planner::LogicalCreateTable`

**Public Methods:**

- `LogicalCreateTable(binder::BoundCreateTableInfo info, std::shared_pt...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalCreateTablePrintInfo

**Full name:** `gs::planner::LogicalCreateTablePrintInfo`

**Public Methods:**

- `LogicalCreateTablePrintInfo(binder::BoundCreateTableInfo info)`
- `toString() const override`
- `copy() const override`
- `LogicalCreateTablePrintInfo(const LogicalCreateTablePrintInfo &other)`


---

## LogicalCreateType

**Full name:** `gs::planner::LogicalCreateType`

**Public Methods:**

- `LogicalCreateType(std::string typeName, common::LogicalType type, s...`
- `getExpressionsForPrinting() const override`
- `getType() const`
- `getPrintInfo() const override`
- `copy() final`


---

## LogicalCreateTypePrintInfo

**Full name:** `gs::planner::LogicalCreateTypePrintInfo`

**Public Methods:**

- `LogicalCreateTypePrintInfo(std::string typeName, std::string type)`
- `toString() const override`
- `copy() const override`


---

## LogicalCrossProduct

**Full name:** `gs::planner::LogicalCrossProduct`

**Public Methods:**

- `LogicalCrossProduct(common::AccumulateType accumulateType, std::share...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getAccumulateType() const`
- `hasMark() const`
- `getMark() const`
- `getSIPInfoUnsafe()`
- `getSIPInfo() const`
- `copy() override`


---

## LogicalDatabase

**Full name:** `gs::planner::LogicalDatabase`

**Public Methods:**

- `LogicalDatabase(LogicalOperatorType operatorType, std::shared_ptr...`
- `getDBName() const`
- `getExpressionsForPrinting() const override`


---

## LogicalDelete

**Full name:** `gs::planner::LogicalDelete`

**Public Methods:**

- `LogicalDelete(std::vector< binder::BoundDeleteInfo > infos, std...`
- `getTableType() const`
- `getInfos() const`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getGroupsPosToFlatten() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalDeletePrintInfo

**Full name:** `gs::planner::LogicalDeletePrintInfo`

**Public Methods:**

- `LogicalDeletePrintInfo(std::vector< binder::BoundDeleteInfo > infos)`
- `toString() const override`


---

## LogicalDetachDatabase

**Full name:** `gs::planner::LogicalDetachDatabase`

**Public Methods:**

- `LogicalDetachDatabase(std::string dbName, std::shared_ptr< binder::Expr...`
- `copy() override`


---

## LogicalDistinct

**Full name:** `gs::planner::LogicalDistinct`

**Public Methods:**

- `LogicalDistinct(binder::expression_vector keys, std::shared_ptr< ...`
- `LogicalDistinct(LogicalOperatorType type, binder::expression_vect...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getGroupsPosToFlatten()`
- `getExpressionsForPrinting() const override`
- `getKeys() const`
- `setKeys(binder::expression_vector expressions)`
- `getPayloads() const`
- `setPayloads(binder::expression_vector expressions)`
- ... and 7 more methods


---

## LogicalDrop

**Full name:** `gs::planner::LogicalDrop`

**Public Methods:**

- `LogicalDrop(parser::DropInfo dropInfo, std::shared_ptr< binde...`
- `getExpressionsForPrinting() const override`
- `getDropInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalDropPrintInfo

**Full name:** `gs::planner::LogicalDropPrintInfo`

**Public Methods:**

- `LogicalDropPrintInfo(std::string name)`
- `toString() const override`


---

## LogicalDummyScan

**Full name:** `gs::planner::LogicalDummyScan`

**Public Methods:**

- `LogicalDummyScan(bool updateClause=false)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `isUpdateClause() const`
- `copy() override`
- `getDummyExpression()`


---

## LogicalDummySink

**Full name:** `gs::planner::LogicalDummySink`

**Public Methods:**

- `LogicalDummySink(std::shared_ptr< LogicalOperator > child)`
- `LogicalDummySink(logical_op_vector_t children)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalEmptyResult

**Full name:** `gs::planner::LogicalEmptyResult`

**Public Methods:**

- `LogicalEmptyResult(const Schema &schema)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalExplain

**Full name:** `gs::planner::LogicalExplain`

**Public Methods:**

- `LogicalExplain(std::shared_ptr< LogicalOperator > child, std::sh...`
- `computeSchema()`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getOutputExpression() const`
- `getExpressionsForPrinting() const override`
- `getExplainType() const`
- `getOutputExpressionsToExplain() const`
- `copy() override`


---

## LogicalExportDatabase

**Full name:** `gs::planner::LogicalExportDatabase`

**Public Methods:**

- `LogicalExportDatabase(common::FileScanInfo boundFileInfo, std::shared_p...`
- `getFilePath() const`
- `getFileType() const`
- `getCopyOption() const`
- `getBoundFileInfo() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalExpressionsScan

**Full name:** `gs::planner::LogicalExpressionsScan`

**Public Methods:**

- `LogicalExpressionsScan(binder::expression_vector expressions)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressions() const`
- `setOuterAccumulate(LogicalOperator *op)`
- `getOuterAccumulate() const`
- `copy() override`


---

## LogicalExtend

**Full name:** `gs::planner::LogicalExtend`

**Public Methods:**

- `LogicalExtend(std::shared_ptr< binder::NodeExpression > boundNo...`
- `getGroupsPosToFlatten() override`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getProperties() const`
- `setPropertyPredicates(std::vector< storage::ColumnPredicateSet > predicates)`
- `getPropertyPredicates() const`
- `setScanNbrID(bool scanNbrID_)`
- `shouldScanNbrID() const`
- `copy() override`
- ... and 13 more methods


---

## LogicalExtension

**Full name:** `gs::planner::LogicalExtension`

**Public Methods:**

- `LogicalExtension(std::unique_ptr< ExtensionAuxInfo > auxInfo, std:...`
- `getExpressionsForPrinting() const override`
- `getAuxInfo() const`
- `copy() override`


---

## LogicalFilter

**Full name:** `gs::planner::LogicalFilter`

**Public Methods:**

- `LogicalFilter(std::shared_ptr< binder::Expression > expression,...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getGroupsPosToFlatten()`
- `getExpressionsForPrinting() const override`
- `getPredicate() const`
- `getGroupPosToSelect() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalFilterPrintInfo

**Full name:** `gs::planner::LogicalFilterPrintInfo`

**Public Methods:**

- `LogicalFilterPrintInfo(std::shared_ptr< binder::Expression > expression)`
- `toString() const override`


---

## LogicalFlatten

**Full name:** `gs::planner::LogicalFlatten`

**Public Methods:**

- `LogicalFlatten(f_group_pos groupPos, std::shared_ptr< LogicalOpe...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getGroupPos() const`
- `copy() override`


---

## LogicalGetV

**Full name:** `gs::planner::LogicalGetV`

**Public Methods:**

- `LogicalGetV(std::shared_ptr< binder::Expression > nodeID, std...`
- `getExpressionsForPrinting() const override`
- `computeFlatSchema() override`
- `computeFactorizedSchema() override`
- `copy() override`
- `getNodeID() const`
- `getProperties() const`
- `getTableIDs() const`
- `getStartAliasName() const`
- `getGetVOpt() const`
- ... and 6 more methods


---

## LogicalHashJoin

**Full name:** `gs::planner::LogicalHashJoin`

**Public Methods:**

- `LogicalHashJoin(std::vector< join_condition_t > joinConditions, c...`
- `getGroupsPosToFlattenOnProbeSide()`
- `getGroupsPosToFlattenOnBuildSide()`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressionsToMaterialize() const`
- `getJoinNodeIDs() const`
- `getJoinConditions() const`
- `getJoinType() const`
- ... and 9 more methods


---

## LogicalImportDatabase

**Full name:** `gs::planner::LogicalImportDatabase`

**Public Methods:**

- `LogicalImportDatabase(std::string query, std::string indexQuery, std::s...`
- `getQuery() const`
- `getIndexQuery() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalInsert

**Full name:** `gs::planner::LogicalInsert`

**Public Methods:**

- `LogicalInsert(std::vector< LogicalInsertInfo > infos, std::shar...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const final`
- `getGroupsPosToFlatten()`
- `getInfos() const`
- `copy() override`
- `getGAliasNames() const`


---

## LogicalInsertInfo

**Full name:** `gs::planner::LogicalInsertInfo`

**Public Methods:**

- `LogicalInsertInfo(common::TableType tableType, std::shared_ptr< bin...`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalInsertInfo)`


---

## LogicalIntersect

**Full name:** `gs::planner::LogicalIntersect`

**Public Methods:**

- `LogicalIntersect(std::shared_ptr< binder::Expression > intersectNo...`
- `getGroupsPosToFlattenOnProbeSide()`
- `getGroupsPosToFlattenOnBuildSide(uint32_t buildIdx)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getIntersectNodeID() const`
- `getNumBuilds() const`
- `getKeyNodeIDs() const`
- `getKeyNodeID(uint32_t idx) const`
- ... and 5 more methods


---

## LogicalLimit

**Full name:** `gs::planner::LogicalLimit`

**Public Methods:**

- `LogicalLimit(std::shared_ptr< binder::Expression > skipNum, st...`
- `getGroupsPosToFlatten()`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `hasSkipNum() const`
- `canEvaluateSkipNum() const`
- `getSkipNum() const`
- `evaluateSkipNum() const`
- `hasLimitNum() const`
- ... and 6 more methods


---

## LogicalMerge

**Full name:** `gs::planner::LogicalMerge`

**Public Methods:**

- `LogicalMerge(std::shared_ptr< binder::Expression > existenceMa...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getGroupsPosToFlatten()`
- `getExistenceMark() const`
- `addInsertNodeInfo(LogicalInsertInfo info)`
- `getInsertNodeInfos() const`
- `addInsertRelInfo(LogicalInsertInfo info)`
- `getInsertRelInfos() const`
- ... and 10 more methods


---

## LogicalMultiplicityReducer

**Full name:** `gs::planner::LogicalMultiplicityReducer`

**Public Methods:**

- `LogicalMultiplicityReducer(std::shared_ptr< LogicalOperator > child)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalNodeLabelFilter

**Full name:** `gs::planner::LogicalNodeLabelFilter`

**Public Methods:**

- `LogicalNodeLabelFilter(std::shared_ptr< binder::Expression > nodeID, std...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getNodeID() const`
- `getTableIDSet() const`
- `copy() override`


---

## LogicalOperator

**Full name:** `gs::planner::LogicalOperator`

**Public Methods:**

- `LogicalOperator(LogicalOperatorType operatorType)`
- `LogicalOperator(LogicalOperatorType operatorType, std::shared_ptr...`
- `LogicalOperator(LogicalOperatorType operatorType, std::shared_ptr...`
- `LogicalOperator(LogicalOperatorType operatorType, const logical_o...`
- `~LogicalOperator()=default`
- `getNumChildren() const`
- `getChild(uint64_t idx) const`
- `getChildren() const`
- `insertChild(uint64_t idx, std::shared_ptr< LogicalOperator > child)`
- `setChild(uint64_t idx, std::shared_ptr< LogicalOperator > child)`
- ... and 19 more methods


---

## LogicalOperatorUtils

**Full name:** `gs::planner::LogicalOperatorUtils`

**Public Methods:**

- `logicalOperatorTypeToString(LogicalOperatorType type)`
- `isUpdate(LogicalOperatorType type)`
- `isAccHashJoin(const LogicalOperator &op)`


---

## LogicalOrderBy

**Full name:** `gs::planner::LogicalOrderBy`

**Public Methods:**

- `LogicalOrderBy(binder::expression_vector expressionsToOrderBy, s...`
- `getGroupsPosToFlatten()`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressionsToOrderBy() const`
- `getIsAscOrders() const`
- `isTopK() const`
- `setSkipNum(uint64_t num)`
- `getSkipNum() const`
- ... and 4 more methods


---

## LogicalPartitioner

**Full name:** `gs::planner::LogicalPartitioner`

**Public Methods:**

- `LogicalPartitioner(LogicalPartitionerInfo info, binder::BoundCopyFro...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getInfo()`
- `getInfo() const`
- `copy() override`


---

## LogicalPartitionerInfo

**Full name:** `gs::planner::LogicalPartitionerInfo`

**Public Methods:**

- `LogicalPartitionerInfo(catalog::TableCatalogEntry *tableEntry, std::shar...`
- `LogicalPartitionerInfo(const LogicalPartitionerInfo &other)`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalPartitionerInfo)`
- `getNumInfos() const`
- `getInfo(common::idx_t idx)`
- `getInfo(common::idx_t idx) const`


---

## LogicalPartitioningInfo

**Full name:** `gs::planner::LogicalPartitioningInfo`

**Public Methods:**

- `LogicalPartitioningInfo(common::idx_t keyIdx)`
- `LogicalPartitioningInfo(const LogicalPartitioningInfo &other)`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalPartitioningInfo)`


---

## LogicalPathPropertyProbe

**Full name:** `gs::planner::LogicalPathPropertyProbe`

**Public Methods:**

- `LogicalPathPropertyProbe(std::shared_ptr< binder::RelExpression > rel, std...`
- `computeFactorizedSchema() final`
- `computeFlatSchema() final`
- `getExpressionsForPrinting() const override`
- `getRel() const`
- `getPathNodeIDs() const`
- `getPathEdgeIDs() const`
- `setJoinType(RecursiveJoinType joinType_)`
- `getJoinType() const`
- `getNodeChild() const`
- ... and 4 more methods


---

## LogicalPlan

**Full name:** `gs::planner::LogicalPlan`

**Public Methods:**

- `LogicalPlan()`
- `setLastOperator(std::shared_ptr< LogicalOperator > op)`
- `isEmpty() const`
- `emptyResult(std::shared_ptr< LogicalOperator > op) const`
- `getLastOperator() const`
- `getLastOperatorRef() const`
- `getSchema() const`
- `getCardinality() const`
- `setCost(uint64_t cost_)`
- `getCost() const`
- ... and 5 more methods


---

## LogicalPlanUtil

**Full name:** `gs::planner::LogicalPlanUtil`

**Public Methods:**

- `encodeJoin(LogicalPlan &logicalPlan)`


---

## LogicalPrimaryKeyLookup

**Full name:** `gs::planner::LogicalPrimaryKeyLookup`

**Public Methods:**

- `LogicalPrimaryKeyLookup(std::vector< binder::IndexLookupInfo > infos, std...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getNumInfos() const`
- `getInfo(uint32_t idx) const`
- `copy() override`


---

## LogicalProjection

**Full name:** `gs::planner::LogicalProjection`

**Public Methods:**

- `LogicalProjection(binder::expression_vector expressions, std::share...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressionsToProject() const`
- `getDiscardedGroupsPos() const`
- `copy() override`
- `resetExprUniqueNames()`


---

## LogicalRecursiveExtend

**Full name:** `gs::planner::LogicalRecursiveExtend`

**Public Methods:**

- `LogicalRecursiveExtend(std::unique_ptr< function::RJAlgorithm > function...`
- `LogicalRecursiveExtend(std::unique_ptr< function::RJAlgorithm > function...`
- `computeFlatSchema() override`
- `computeFactorizedSchema() override`
- `getDirection() const`
- `setFusionType(gs::optimizer::FusionType fusionType_)`
- `getFusionType() const`
- `setFunction(std::unique_ptr< function::RJAlgorithm > func)`
- `getFunction() const`
- `getBindData() const`
- ... and 23 more methods


---

## LogicalScanNodeTable

**Full name:** `gs::planner::LogicalScanNodeTable`

**Public Methods:**

- `LogicalScanNodeTable(std::shared_ptr< binder::Expression > nodeID, std...`
- `LogicalScanNodeTable(const LogicalScanNodeTable &other)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getAliasName() const`
- `getGAliasName() const`
- `getPrimaryKey(catalog::Catalog *catalog) const`
- `getNodeType(catalog::Catalog *catalog) const`
- `getExpressionsForPrinting() const override`
- `getScanType() const`
- ... and 14 more methods


---

## LogicalScanNodeTablePrintInfo

**Full name:** `gs::planner::LogicalScanNodeTablePrintInfo`

**Public Methods:**

- `LogicalScanNodeTablePrintInfo(std::shared_ptr< binder::Expression > nodeID, bin...`
- `toString() const override`


---

## LogicalSemiMasker

**Full name:** `gs::planner::LogicalSemiMasker`

**Public Methods:**

- `LogicalSemiMasker(SemiMaskKeyType keyType, SemiMaskTargetType targe...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getKeyType() const`
- `getTargetType() const`
- `getKey() const`
- `setExtraKeyInfo(std::unique_ptr< ExtraKeyInfo > extraInfo)`
- `getExtraKeyInfo() const`
- `getNodeTableIDs() const`
- ... and 3 more methods


---

## LogicalSetProperty

**Full name:** `gs::planner::LogicalSetProperty`

**Public Methods:**

- `LogicalSetProperty(std::vector< binder::BoundSetPropertyInfo > infos...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getGroupsPosToFlatten(uint32_t idx) const`
- `getExpressionsForPrinting() const override`
- `getTableType() const`
- `getInfos() const`
- `getInfo(uint32_t idx) const`
- `copy() override`


---

## LogicalSimple

**Full name:** `gs::planner::LogicalSimple`

**Public Methods:**

- `LogicalSimple(LogicalOperatorType operatorType, std::shared_ptr...`
- `LogicalSimple(LogicalOperatorType operatorType, const std::vect...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getOutputExpression() const`


---

## LogicalStandaloneCall

**Full name:** `gs::planner::LogicalStandaloneCall`

**Public Methods:**

- `LogicalStandaloneCall(const main::Option *option, std::shared_ptr< bind...`
- `getOption() const`
- `getOptionValue() const`
- `getExpressionsForPrinting() const override`
- `computeFlatSchema() override`
- `computeFactorizedSchema() override`
- `copy() override`


---

## LogicalTableFunctionCall

**Full name:** `gs::planner::LogicalTableFunctionCall`

**Public Methods:**

- `LogicalTableFunctionCall(function::TableFunction tableFunc, std::unique_pt...`
- `getTableFunc() const`
- `getBindData() const`
- `setColumnSkips(std::vector< bool > columnSkips)`
- `setNodeMaskRoots(std::vector< std::shared_ptr< LogicalOperator > > roots)`
- `getNodeMaskRoots() const`
- `computeFlatSchema() override`
- `computeFactorizedSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalTransaction

**Full name:** `gs::planner::LogicalTransaction`

**Public Methods:**

- `LogicalTransaction(transaction::TransactionAction transactionAction)`
- `getExpressionsForPrinting() const final`
- `computeFlatSchema() final`
- `computeFactorizedSchema() final`
- `getTransactionAction() const`
- `copy() final`


---

## LogicalUnion

**Full name:** `gs::planner::LogicalUnion`

**Public Methods:**

- `LogicalUnion(binder::expression_vector expressions, const std:...`
- `getGroupsPosToFlatten(uint32_t childIdx)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressionsToUnion() const`
- `getSchemaBeforeUnion(uint32_t idx) const`
- `copy() override`
- `copyWithSchema()`
- `setPreQuery(bool preQuery)`
- ... and 1 more methods


---

## LogicalUnwind

**Full name:** `gs::planner::LogicalUnwind`

**Public Methods:**

- `LogicalUnwind(std::shared_ptr< binder::Expression > inExpr, std...`
- `getGroupsPosToFlatten()`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getInExpr() const`
- `getOutExpr() const`
- `hasIDExpr() const`
- `getIDExpr() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalUseDatabase

**Full name:** `gs::planner::LogicalUseDatabase`

**Public Methods:**

- `LogicalUseDatabase(std::string dbName, std::shared_ptr< binder::Expr...`
- `copy() override`


---

## NodeRelScanInfo

**Full name:** `gs::planner::NodeRelScanInfo`

**Public Methods:**

- `NodeRelScanInfo(std::shared_ptr< binder::Expression > nodeOrRel, ...`


---

## Planner

**Full name:** `gs::planner::Planner`

**Public Methods:**

- `Planner(main::ClientContext *clientContext)`
- `DELETE_COPY_AND_MOVE(Planner)`
- `getBestPlan(const binder::BoundStatement &statement)`
- `getAllPlans(const binder::BoundStatement &statement)`
- `appendCreateTable(const binder::BoundStatement &statement, LogicalPlan &plan)`
- `appendCreateType(const binder::BoundStatement &statement, LogicalPlan &plan)`
- `appendCreateSequence(const binder::BoundStatement &statement, LogicalPlan &plan)`
- `appendDrop(const binder::BoundStatement &statement, LogicalPlan &plan)`
- `appendAlter(const binder::BoundStatement &statement, LogicalPlan &plan)`
- `appendStandaloneCall(const binder::BoundStatement &statement, LogicalPlan &plan)`
- ... and 120 more methods


---

## PrimaryKey

**Full name:** `gs::planner::PrimaryKey`

**Public Methods:**

- `PrimaryKey(const std::string &key, PrimaryKeyScanInfo *value)`


---

## PrimaryKeyScanInfo

**Full name:** `gs::planner::PrimaryKeyScanInfo`

**Public Methods:**

- `PrimaryKeyScanInfo(std::shared_ptr< binder::Expression > key)`
- `copy() const override`


---

## PropertyExprCollection

**Full name:** `gs::planner::PropertyExprCollection`

**Public Methods:**

- `addProperties(const std::string &patternName, std::shared_ptr< ...`
- `getProperties(const binder::Expression &pattern) const`
- `getProperties() const`
- `clear()`


---

## QueryGraphPlanningInfo

**Full name:** `gs::planner::QueryGraphPlanningInfo`

**Public Methods:**

- `containsCorrExpr(const binder::Expression &expr) const`


---

## SIPInfo

**Full name:** `gs::planner::SIPInfo`


---

## Schema

**Full name:** `gs::planner::Schema`

**Public Methods:**

- `getNumGroups() const`
- `getGroup(const std::shared_ptr< binder::Expression > &expr...`
- `getGroup(const std::string &expressionName) const`
- `getGroup(uint32_t pos) const`
- `createGroup()`
- `insertToScope(const std::shared_ptr< binder::Expression > &expr...`
- `insertToGroupAndScope(const std::shared_ptr< binder::Expression > &expr...`
- `insertToScopeMayRepeat(const std::shared_ptr< binder::Expression > &expr...`
- `insertToGroupAndScopeMayRepeat(const std::shared_ptr< binder::Expression > &expr...`
- `insertToGroupAndScope(const binder::expression_vector &expressions, uin...`
- ... and 13 more methods


---

## SchemaUtils

**Full name:** `gs::planner::SchemaUtils`

**Public Methods:**

- `getLeadingGroupPos(const std::unordered_set< f_group_pos > &groupPos...`
- `validateAtMostOneUnFlatGroup(const std::unordered_set< f_group_pos > &groupPos...`
- `validateNoUnFlatGroup(const std::unordered_set< f_group_pos > &groupPos...`


---

## SinkOperatorUtil

**Full name:** `gs::planner::SinkOperatorUtil`

**Public Methods:**

- `mergeSchema(const Schema &inputSchema, const binder::expressi...`
- `recomputeSchema(const Schema &inputSchema, const binder::expressi...`


---

## SubPlansTable

**Full name:** `gs::planner::SubPlansTable`

**Public Methods:**

- `resize(uint32_t newSize)`
- `getMaxCost(const binder::SubqueryGraph &subqueryGraph) const`
- `containSubgraphPlans(const binder::SubqueryGraph &subqueryGraph) const`
- `getSubgraphPlans(const binder::SubqueryGraph &subqueryGraph)`
- `getSubqueryGraphs(uint32_t level)`
- `addPlan(const binder::SubqueryGraph &subqueryGraph, std::...`
- `clear()`


---

## SubgraphPlans

**Full name:** `gs::planner::SubgraphPlans`

**Public Methods:**

- `SubgraphPlans(const binder::SubqueryGraph &subqueryGraph)`
- `getMaxCost() const`
- `addPlan(std::unique_ptr< LogicalPlan > plan)`
- `getPlans()`


---

## TreeNodeTypeUtils

**Full name:** `gs::planner::TreeNodeTypeUtils`

**Public Methods:**

- `toString(TreeNodeType type)`


---

## CopyFromFileError

**Full name:** `gs::processor::CopyFromFileError`

**Public Methods:**

- `CopyFromFileError(std::string message, WarningSourceData warningDat...`
- `operator<(const CopyFromFileError &o) const`


---

## DataChunkDescriptor

**Full name:** `gs::processor::DataChunkDescriptor`

**Public Methods:**

- `DataChunkDescriptor(bool isSingleState)`
- `DataChunkDescriptor(const DataChunkDescriptor &other)`
- `copy() const`


---

## DataPos

**Full name:** `gs::processor::DataPos`

**Public Methods:**

- `DataPos()`
- `DataPos(data_chunk_pos_t dataChunkPos, value_vector_pos_t...`
- `DataPos(std::pair< data_chunk_pos_t, value_vector_pos_t > pos)`
- `isValid() const`
- `operator==(const DataPos &rhs) const`
- `getInvalidPos()`


---

## ExecutionContext

**Full name:** `gs::processor::ExecutionContext`

**Public Methods:**

- `ExecutionContext(common::Profiler *profiler, main::ClientContext *...`


---

## ExpressionMapper

**Full name:** `gs::processor::ExpressionMapper`

**Public Methods:**

- `ExpressionMapper()=default`
- `ExpressionMapper(const planner::Schema *schema)`
- `ExpressionMapper(const planner::Schema *schema, evaluator::Express...`
- `getEvaluator(std::shared_ptr< binder::Expression > expression)`
- `getConstantEvaluator(std::shared_ptr< binder::Expression > expression)`


---

## LineContext

**Full name:** `gs::processor::LineContext`

**Public Methods:**

- `setNewLine(uint64_t start)`
- `setEndOfLine(uint64_t end)`


---

## OperatorMetrics

**Full name:** `gs::processor::OperatorMetrics`

**Public Methods:**

- `OperatorMetrics(common::TimeMetric &executionTime, common::Numeri...`


---

## PhysicalOperator

**Full name:** `gs::processor::PhysicalOperator`

**Public Methods:**

- `PhysicalOperator(PhysicalOperatorType operatorType, physical_op_id...`
- `PhysicalOperator(PhysicalOperatorType operatorType, std::unique_pt...`
- `PhysicalOperator(PhysicalOperatorType operatorType, std::unique_pt...`
- `PhysicalOperator(PhysicalOperatorType operatorType, physical_op_ve...`
- `~PhysicalOperator()=default`
- `getOperatorID() const`
- `getOperatorType() const`
- `isSource() const`
- `isSink() const`
- `isParallel() const`
- ... and 15 more methods


---

## PhysicalOperatorUtils

**Full name:** `gs::processor::PhysicalOperatorUtils`

**Public Methods:**

- `operatorToString(const PhysicalOperator *physicalOp)`


---

## PopulatedCopyFromError

**Full name:** `gs::processor::PopulatedCopyFromError`


---

## ResultSet

**Full name:** `gs::processor::ResultSet`

**Public Methods:**

- `ResultSet()`
- `ResultSet(common::idx_t numDataChunks)`
- `ResultSet(ResultSetDescriptor *resultSetDescriptor, storage...`
- `insert(common::idx_t pos, std::shared_ptr< common::DataC...`
- `getDataChunk(data_chunk_pos_t dataChunkPos)`
- `getValueVector(const DataPos &dataPos) const`
- `getNumTuples(const std::unordered_set< uint32_t > &dataChunksPosInScope)`
- `getNumTuplesWithoutMultiplicity(const std::unordered_set< uint32_t > &dataChunksPosInScope)`


---

## ResultSetDescriptor

**Full name:** `gs::processor::ResultSetDescriptor`

**Public Methods:**

- `ResultSetDescriptor()=default`
- `ResultSetDescriptor(std::vector< std::unique_ptr< DataChunkDescriptor...`
- `ResultSetDescriptor(planner::Schema *schema)`
- `DELETE_BOTH_COPY(ResultSetDescriptor)`
- `copy() const`


---

## WarningContext

**Full name:** `gs::processor::WarningContext`

**Public Methods:**

- `WarningContext(main::ClientConfig *clientConfig)`
- `appendWarningMessages(const std::vector< CopyFromFileError > &messages)`
- `populateWarnings(uint64_t queryID, populate_func_t populateFunc={}...`
- `defaultPopulateAllWarnings(uint64_t queryID)`
- `getPopulatedWarnings() const`
- `getWarningCount(uint64_t queryID)`
- `clearPopulatedWarnings()`
- `setIgnoreErrorsForCurrentQuery(bool ignoreErrors)`
- `getIgnoreErrorsOption() const`


---

## WarningInfo

**Full name:** `gs::processor::WarningInfo`

**Public Methods:**

- `WarningInfo(PopulatedCopyFromError warning, uint64_t queryID)`


---

## WarningSourceData

**Full name:** `gs::processor::WarningSourceData`

**Public Methods:**

- `WarningSourceData()`
- `WarningSourceData(uint64_t numSourceSpecificValues)`
- `dumpTo(uint64_t &blockIdx, uint32_t &offsetInBlock, Type...`
- `getBlockIdx() const`
- `getOffsetInBlock() const`
- `constructFrom(uint64_t blockIdx, uint32_t offsetInBlock, Types....`
- `constructFromData(const std::vector< T * > &chunks, common::idx_t pos)`


---

## ArenaRef

**Full name:** `gs::runtime::ArenaRef`

**Public Methods:**

- `ArenaRef(const std::shared_ptr< Arena > &arena)`


---

## ArithExpr

**Full name:** `gs::runtime::ArithExpr`

**Public Methods:**

- `ArithExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## ArrowArrayContextColumn

**Full name:** `gs::runtime::ArrowArrayContextColumn`

`ArrowArrayContextColumn` is a context column that holds multiple arrow::Array objects.

**Public Methods:**

- `ArrowArrayContextColumn(const std::vector< std::shared_ptr< arrow::Array ...`
- `~ArrowArrayContextColumn()=default`
- `column_info() const override`
- `size() const override`
- `elem_type() const override`
- `column_type() const override`
- `is_optional() const override`
- `GetColumns() const`
- `GetArrowType() const`


---

## ArrowArrayContextColumnBuilder

**Full name:** `gs::runtime::ArrowArrayContextColumnBuilder`

**Public Methods:**

- `ArrowArrayContextColumnBuilder()=default`
- `~ArrowArrayContextColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `finish() override`
- `push_back(const std::shared_ptr< arrow::Array > &column)`


---

## ArrowStreamContextColumn

**Full name:** `gs::runtime::ArrowStreamContextColumn`

There are num_cols `ArrowStreamContextColumn` objects for a record batch.

Currently it is basiclly not well-implemented, and only workable with BatchInsertVertex/Edge operator followed by.

**Public Methods:**

- `ArrowStreamContextColumn(const std::vector< std::shared_ptr< IRecordBatchS...`
- `~ArrowStreamContextColumn()=default`
- `column_info() const override`
- `size() const override`
- `elem_type() const override`
- `column_type() const override`
- `GetSuppliers() const`


---

## ArrowStreamContextColumnBuilder

**Full name:** `gs::runtime::ArrowStreamContextColumnBuilder`

Each column take data from the streamReader's one column.

**Public Methods:**

- `ArrowStreamContextColumnBuilder(const std::vector< std::shared_ptr< IRecordBatchS...`
- `~ArrowStreamContextColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `finish() override`


---

## BDMLEdgeColumn

**Full name:** `gs::runtime::BDMLEdgeColumn`

**Public Methods:**

- `BDMLEdgeColumn(const std::vector< std::pair< LabelTriplet, Prope...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `get_labels() const override`
- `edge_column_type() const override`


---

## BDMLEdgeColumnBuilder

**Full name:** `gs::runtime::BDMLEdgeColumnBuilder`

**Public Methods:**

- `builder(const std::vector< std::pair< LabelTriplet, Prope...`
- `optional_builder(const std::vector< std::pair< LabelTriplet, Prope...`
- `builder()`
- `optional_builder()`
- `~BDMLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(label_t index, vid_t src, vid_t dst, const EdgeDa...`
- `push_back_opt(EdgeRecord e)`
- `push_back_opt(LabelTriplet label, vid_t src, vid_t dst, const E...`
- ... and 4 more methods


---

## BDSLEdgeColumn

**Full name:** `gs::runtime::BDSLEdgeColumn`

**Public Methods:**

- `BDSLEdgeColumn(const LabelTriplet &label, PropertyType prop_type)`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `get_labels() const override`
- `edge_column_type() const override`


---

## BDSLEdgeColumnBuilder

**Full name:** `gs::runtime::BDSLEdgeColumnBuilder`

**Public Methods:**

- `builder(const LabelTriplet &label, PropertyType prop_type)`
- `optional_builder(const LabelTriplet &label, PropertyType prop_type)`
- `~BDSLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(vid_t src, vid_t dst, const EdgeData &data, Direction dir)`
- `push_back_endpoints(vid_t src, vid_t dst, Direction dir)`
- `push_back_endpoints(vid_t src, vid_t dst, bool dir)`
- `push_back_null()`
- `finish() override`


---

## CObject

**Full name:** `gs::runtime::CObject`

**Public Methods:**

- `~CObject()=default`


---

## CaseWhenExpr

**Full name:** `gs::runtime::CaseWhenExpr`

**Public Methods:**

- `CaseWhenExpr(std::vector< std::pair< std::unique_ptr< ExprBase...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## ConstAccessor

**Full name:** `gs::runtime::ConstAccessor`

**Public Methods:**

- `ConstAccessor(const T &val)`
- `typed_eval_path(size_t) const`
- `typed_eval_vertex(label_t, vid_t, size_t) const`
- `typed_eval_edge(const LabelTriplet &, vid_t, vid_t, const Any &, ...`
- `eval_path(size_t) const override`
- `eval_vertex(label_t, vid_t, size_t) const override`
- `eval_edge(const LabelTriplet &, vid_t, vid_t, const Any &, ...`


---

## ConstExpr

**Full name:** `gs::runtime::ConstExpr`

**Public Methods:**

- `ConstExpr(const RTAny &val, bool take_ownership=false)`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Context

**Full name:** `gs::runtime::Context`

**Public Methods:**

- `Context()`
- `~Context()=default`
- `clear()`
- `set(int alias, std::shared_ptr< IContextColumn > col)`
- `set_with_reshuffle(int alias, std::shared_ptr< IContextColumn > col,...`
- `reshuffle(const std::vector< size_t > &offsets)`
- `optional_reshuffle(const std::vector< size_t > &offsets)`
- `get(int alias)`
- `get(int alias) const`
- `remove(int alias)`
- ... and 6 more methods


---

## ContextMeta

**Full name:** `gs::runtime::ContextMeta`

**Public Methods:**

- `ContextMeta()=default`
- `~ContextMeta()=default`
- `exist(int alias) const`
- `set(int alias)`
- `columns() const`
- `desc() const`


---

## ContextValueAccessor

**Full name:** `gs::runtime::ContextValueAccessor`

**Public Methods:**

- `ContextValueAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## CsvExportWriter

**Full name:** `gs::runtime::CsvExportWriter`

**Public Methods:**

- `CsvExportWriter(const std::string &file_path, const std::vector< ...`
- `~CsvExportWriter()`
- `Write(const std::vector< std::shared_ptr< IContextColum...`
- `Make(const std::string &file_path, const std::vector< ...`


---

## DateMinusExpr

**Full name:** `gs::runtime::DateMinusExpr`

**Public Methods:**

- `DateMinusExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## Dedup

**Full name:** `gs::runtime::Dedup`

**Public Methods:**

- `dedup(Context &&ctx, const std::vector< size_t > &cols)`
- `dedup(Context &&ctx, const std::vector< std::function< ...`
- `dedup(WriteContext &&ctx, const std::vector< size_t > &cols)`


---

## DummyEdgePredicate

**Full name:** `gs::runtime::DummyEdgePredicate`

**Public Methods:**

- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## DummyGetter

**Full name:** `gs::runtime::DummyGetter`

**Public Methods:**

- `DummyGetter(int from, int to)`
- `evaluate(const Context &ctx, Context &&ret) override`
- `alias() const override`


---

## DummyVertexPredicate

**Full name:** `gs::runtime::DummyVertexPredicate`

**Public Methods:**

- `operator()(label_t label, vid_t v, size_t path_idx) const`


---

## DummyWGetter

**Full name:** `gs::runtime::DummyWGetter`

**Public Methods:**

- `DummyWGetter(int from, int to)`
- `evaluate(WriteContext &ctx, WriteContext &&ret) override`


---

## EdgeData

**Full name:** `gs::runtime::EdgeData`

**Public Methods:**

- `as() const`
- `EdgeData(T val)`
- `to_string() const`
- `EdgeData()=default`
- `EdgeData(const Any &any)`
- `operator<(const EdgeData &e) const`
- `operator==(const EdgeData &e) const`


---

## EdgeExpand

**Full name:** `gs::runtime::EdgeExpand`

**Public Methods:**

- `expand_edge(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_edge_with_special_edge_predicate(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_edge_without_predicate(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_vertex(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_vertex_ep_lt(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_vertex_ep_gt(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_vertex_with_special_vertex_predicate(const GraphReadInterface &graph, Context &&ctx, c...`
- `expand_vertex_without_predicate(const GraphReadInterface &graph, Context &&ctx, c...`
- `tc(const GraphReadInterface &graph, Context &&ctx, c...`


---

## SPVPWrapper

**Full name:** `gs::runtime::EdgeExpand::SPVPWrapper`

**Public Methods:**

- `SPVPWrapper(const PRED_T &pred)`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgeExpandParams

**Full name:** `gs::runtime::EdgeExpandParams`


---

## EdgeGIdPathAccessor

**Full name:** `gs::runtime::EdgeGIdPathAccessor`

**Public Methods:**

- `EdgeGIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## EdgeIdPathAccessor

**Full name:** `gs::runtime::EdgeIdPathAccessor`

**Public Methods:**

- `EdgeIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## EdgeLabelPathAccessor

**Full name:** `gs::runtime::EdgeLabelPathAccessor`

**Public Methods:**

- `EdgeLabelPathAccessor(const Schema &schema, const Context &ctx, int tag)`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`


---

## EdgePropVec

**Full name:** `gs::runtime::EdgePropVec`

**Public Methods:**

- `~EdgePropVec()`
- `push_back(const T &val)`
- `emplace_back(T &&val)`
- `size() const override`
- `get(size_t idx) const override`
- `get_view(size_t idx) const`
- `resize(size_t size) override`
- `clear() override`
- `reserve(size_t size) override`
- `operator[](size_t idx) const`
- ... and 3 more methods


---

## EmptyType >

**Full name:** `gs::runtime::EdgePropVec< grape::EmptyType >`

**Public Methods:**

- `EdgePropVec()`
- `~EdgePropVec()`
- `push_back(const grape::EmptyType &val)`
- `emplace_back(grape::EmptyType &&val)`
- `size() const override`
- `get(size_t idx) const override`
- `get_view(size_t idx) const`
- `resize(size_t size) override`
- `clear() override`
- `reserve(size_t size) override`
- ... and 4 more methods


---

## EdgePropVecBase

**Full name:** `gs::runtime::EdgePropVecBase`

**Public Methods:**

- `make_edge_prop_vec(PropertyType type)`
- `~EdgePropVecBase()=default`
- `size() const =0`
- `resize(size_t size)=0`
- `reserve(size_t size)=0`
- `clear()=0`
- `get(size_t idx) const =0`
- `type() const =0`
- `set_any(size_t idx, EdgePropVecBase *other, size_t other_idx)=0`


---

## EdgePropertyEQPredicate

**Full name:** `gs::runtime::EdgePropertyEQPredicate`

**Public Methods:**

- `EdgePropertyEQPredicate(const std::string &target_str)`
- `~EdgePropertyEQPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyEdgeAccessor

**Full name:** `gs::runtime::EdgePropertyEdgeAccessor`

**Public Methods:**

- `EdgePropertyEdgeAccessor(const GraphInterface &graph, const std::string &name)`
- `typed_eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_path(size_t idx) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyGEPredicate

**Full name:** `gs::runtime::EdgePropertyGEPredicate`

**Public Methods:**

- `EdgePropertyGEPredicate(const std::string &target_str)`
- `~EdgePropertyGEPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyGTPredicate

**Full name:** `gs::runtime::EdgePropertyGTPredicate`

**Public Methods:**

- `EdgePropertyGTPredicate(const std::string &target_str)`
- `~EdgePropertyGTPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyLEPredicate

**Full name:** `gs::runtime::EdgePropertyLEPredicate`

**Public Methods:**

- `EdgePropertyLEPredicate(const std::string &target_str)`
- `~EdgePropertyLEPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyLTPredicate

**Full name:** `gs::runtime::EdgePropertyLTPredicate`

**Public Methods:**

- `EdgePropertyLTPredicate(const std::string &target_str)`
- `~EdgePropertyLTPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyNEPredicate

**Full name:** `gs::runtime::EdgePropertyNEPredicate`

**Public Methods:**

- `EdgePropertyNEPredicate(const std::string &target_str)`
- `~EdgePropertyNEPredicate()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyPathAccessor

**Full name:** `gs::runtime::EdgePropertyPathAccessor`

**Public Methods:**

- `EdgePropertyPathAccessor(const GraphInterface &graph, const std::string &p...`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`
- `is_optional() const override`


---

## EdgeRecord

**Full name:** `gs::runtime::EdgeRecord`

**Public Methods:**

- `operator<(const EdgeRecord &e) const`
- `operator==(const EdgeRecord &e) const`
- `src() const`
- `dst() const`
- `label_triplet() const`
- `prop() const`
- `dir() const`
- `to_string() const`


---

## EndNodeExpr

**Full name:** `gs::runtime::EndNodeExpr`

**Public Methods:**

- `EndNodeExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## ExactVertexPredicate

**Full name:** `gs::runtime::ExactVertexPredicate`

**Public Methods:**

- `ExactVertexPredicate(label_t label, vid_t vid)`
- `operator()(label_t label, vid_t vid, size_t path_idx) const`


---

## ExportWriterFactory

**Full name:** `gs::runtime::ExportWriterFactory`

**Public Methods:**

- `CreateExportWriter(const std::string &name, const std::string &file_...`
- `Register(const std::string &name, writer_initializer_t initializer)`


---

## Expr

**Full name:** `gs::runtime::Expr`

**Public Methods:**

- `Expr(const GraphInterface &graph, const Context &ctx, ...`
- `eval_path(size_t idx, Arena &) const`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const`
- `elem_type() const`
- `is_optional() const`


---

## ExprBase

**Full name:** `gs::runtime::ExprBase`

**Public Methods:**

- `eval_path(size_t idx, Arena &) const =0`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const =0`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const =0`
- `is_optional() const`
- `elem_type() const`
- `~ExprBase()=default`


---

## ExtractExpr

**Full name:** `gs::runtime::ExtractExpr`

**Public Methods:**

- `ExtractExpr(std::unique_ptr< ExprBase > &&expr, const ::commo...`
- `eval_impl(const RTAny &val) const`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## GKey

**Full name:** `gs::runtime::GKey`

**Public Methods:**

- `GKey(std::vector< EXPR > &&exprs, const std::vector< s...`
- `group(const Context &ctx) override`
- `tag_alias() const override`


---

## GPredWrapper

**Full name:** `gs::runtime::GPredWrapper`

**Public Methods:**

- `GPredWrapper(const GPRED_T &gpred)`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`


---

## GPredWrapper< GPRED_T, Any >

**Full name:** `gs::runtime::GPredWrapper< GPRED_T, Any >`

**Public Methods:**

- `GPredWrapper(const GPRED_T &gpred)`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`


---

## GeneralEdgePredicate

**Full name:** `gs::runtime::GeneralEdgePredicate`

**Public Methods:**

- `GeneralEdgePredicate(const GraphReadInterface &graph, const Context &c...`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## GeneralPathColumn

**Full name:** `gs::runtime::GeneralPathColumn`

**Public Methods:**

- `GeneralPathColumn()=default`
- `~GeneralPathColumn()`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_path(size_t idx) const override`
- ... and 5 more methods


---

## GeneralPathColumnBuilder

**Full name:** `gs::runtime::GeneralPathColumnBuilder`

**Public Methods:**

- `GeneralPathColumnBuilder()=default`
- `~GeneralPathColumnBuilder()=default`
- `push_back_opt(const Path &p)`
- `push_back_elem(const RTAny &val) override`
- `reserve(size_t size) override`
- `set_arena(const std::shared_ptr< Arena > &arena) override`
- `finish() override`


---

## GeneralPathPredicate

**Full name:** `gs::runtime::GeneralPathPredicate`

**Public Methods:**

- `GeneralPathPredicate(const GraphReadInterface &graph, const Context &c...`
- `operator()(size_t idx, Arena &arena) const`


---

## GeneralVertexPredicate

**Full name:** `gs::runtime::GeneralVertexPredicate`

**Public Methods:**

- `GeneralVertexPredicate(const GraphInterface &graph, const Context &ctx, ...`
- `operator()(label_t label, vid_t v, size_t path_idx, Arena &a...`


---

## GeneralVertexPredicateWrapper

**Full name:** `gs::runtime::GeneralVertexPredicateWrapper`

**Public Methods:**

- `GeneralVertexPredicateWrapper(const GeneralVertexPredicate &pred)`
- `operator()(label_t label, vid_t v, size_t path_idx) const`


---

## GetV

**Full name:** `gs::runtime::GetV`

**Public Methods:**

- `get_vertex_from_edges_optional_impl(const GraphInterface &graph, Context &&ctx, const...`
- `_get_vertex_from_path(const GraphInterface &graph, Context &&ctx, const...`
- `get_vertex_from_edges(const GraphInterface &graph, Context &&ctx, const...`
- `get_vertex_from_vertices(const GraphReadInterface &graph, Context &&ctx, c...`


---

## GetVParams

**Full name:** `gs::runtime::GetVParams`


---

## GraphInsertInterface

**Full name:** `gs::runtime::GraphInsertInterface`

**Public Methods:**

- `GraphInsertInterface(gs::InsertTransaction &txn)`
- `~GraphInsertInterface()`
- `AddVertex(label_t label, const Any &id, const std::vector< ...`
- `AddEdge(label_t src_label, const Any &src, label_t dst_la...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `Commit()`
- `Abort()`
- `schema() const`


---

## GraphReadInterface

**Full name:** `gs::runtime::GraphReadInterface`

**Public Methods:**

- `GraphReadInterface(const gs::ReadTransaction &txn)`
- `~GraphReadInterface()`
- `GetVertexColumn(label_t label, const std::string &prop_name) const`
- `GetVertexSet(label_t label) const`
- `GetVertexIndex(label_t label, const Any &id, vid_t &index) const`
- `IsValidVertex(label_t label, vid_t index) const`
- `GetVertexId(label_t label, vid_t index) const`
- `GetVertexProperty(label_t label, vid_t index, int prop_id) const`
- `GetOutEdgeIterator(label_t label, vid_t v, label_t neighbor_label, l...`
- `GetInEdgeIterator(label_t label, vid_t v, label_t neighbor_label, l...`
- ... and 4 more methods


---

## GraphUpdateInterface

**Full name:** `gs::runtime::GraphUpdateInterface`

**Public Methods:**

- `GetVertexColumn(label_t label, const std::string &prop_name) const`
- `GraphUpdateInterface(gs::UpdateTransaction &txn)`
- `~GraphUpdateInterface()`
- `SetVertexField(label_t label, vid_t lid, int col_id, const Any &value)`
- `GetVertexProperty(label_t label, vid_t index, int prop_id) const`
- `SetEdgeData(bool dir, label_t label, vid_t v, label_t neighbo...`
- `GetUpdatedEdgeData(bool dir, label_t label, vid_t v, label_t neighbo...`
- `AddVertex(label_t label, const Any &id, const std::vector< ...`
- `AddVertex(label_t label, const Any &id, const std::vector< ...`
- `AddEdge(label_t src_label, const Any &src, label_t dst_la...`
- ... and 10 more methods


---

## GroupBy

**Full name:** `gs::runtime::GroupBy`

**Public Methods:**

- `group_by(Context &&ctx, std::unique_ptr< KeyBase > &&key, ...`


---

## IAccessor

**Full name:** `gs::runtime::IAccessor`

**Public Methods:**

- `~IAccessor()=default`
- `eval_path(size_t idx) const =0`
- `eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const`
- `name() const`
- `builder() const`


---

## IContextColumn

**Full name:** `gs::runtime::IContextColumn`

**Public Methods:**

- `IContextColumn()=default`
- `~IContextColumn()=default`
- `size() const`
- `column_info() const =0`
- `column_type() const =0`
- `elem_type() const =0`
- `shuffle(const std::vector< size_t > &offsets) const`
- `optional_shuffle(const std::vector< size_t > &offsets) const`
- `union_col(std::shared_ptr< IContextColumn > other) const`
- `get_elem(size_t idx) const`
- ... and 8 more methods


---

## IContextColumnBuilder

**Full name:** `gs::runtime::IContextColumnBuilder`

**Public Methods:**

- `IContextColumnBuilder()=default`
- `~IContextColumnBuilder()=default`
- `reserve(size_t size)=0`
- `push_back_elem(const RTAny &val)=0`
- `finish()=0`
- `set_arena(const std::shared_ptr< Arena > &arena)`


---

## IEdgeColumn

**Full name:** `gs::runtime::IEdgeColumn`

**Public Methods:**

- `IEdgeColumn()=default`
- `~IEdgeColumn()=default`
- `column_type() const override`
- `get_edge(size_t idx) const =0`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val)=0`
- `get_elem(size_t idx) const override`
- `elem_type() const override`
- `get_labels() const =0`
- `edge_column_type() const =0`


---

## IExportWriter

**Full name:** `gs::runtime::IExportWriter`

**Public Methods:**

- `~IExportWriter()=default`
- `Write(const std::vector< std::shared_ptr< IContextColum...`


---

## IInsertOperator

**Full name:** `gs::runtime::IInsertOperator`

**Public Methods:**

- `~IInsertOperator()=default`
- `get_operator_name() const =0`
- `Eval(GraphInsertInterface &graph, const std::map< std:...`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## IInsertOperatorBuilder

**Full name:** `gs::runtime::IInsertOperatorBuilder`

**Public Methods:**

- `~IInsertOperatorBuilder()=default`
- `stepping(int i)`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const =0`


---

## IOptionalContextColumnBuilder

**Full name:** `gs::runtime::IOptionalContextColumnBuilder`

**Public Methods:**

- `IOptionalContextColumnBuilder()=default`
- `~IOptionalContextColumnBuilder()=default`
- `push_back_null()=0`


---

## IPathColumn

**Full name:** `gs::runtime::IPathColumn`

**Public Methods:**

- `IPathColumn()=default`
- `~IPathColumn()=default`
- `size() const =0`
- `column_info() const =0`
- `column_type() const =0`
- `shuffle(const std::vector< size_t > &offsets) const =0`
- `elem_type() const =0`
- `get_elem(size_t idx) const =0`
- `get_path(size_t idx) const =0`
- `get_path_length(size_t idx) const`
- ... and 2 more methods


---

## IProject

**Full name:** `gs::runtime::IProject`

**Public Methods:**

- `project(WriteContext &&ctx, const std::vector< std::uniqu...`


---

## IReadOperator

**Full name:** `gs::runtime::IReadOperator`

**Public Methods:**

- `~IReadOperator()=default`
- `get_operator_name() const =0`
- `Eval(const GraphReadInterface &graph, const std::map< ...`


---

## IReadOperatorBuilder

**Full name:** `gs::runtime::IReadOperatorBuilder`

**Public Methods:**

- `~IReadOperatorBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `stepping(int i)`
- `GetOpKinds() const =0`


---

## ISigColumn

**Full name:** `gs::runtime::ISigColumn`

**Public Methods:**

- `ISigColumn()=default`
- `~ISigColumn()=default`
- `get_sig(size_t idx) const =0`


---

## IUpdateOperator

**Full name:** `gs::runtime::IUpdateOperator`

**Public Methods:**

- `~IUpdateOperator()=default`
- `get_operator_name() const =0`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## IUpdateOperatorBuilder

**Full name:** `gs::runtime::IUpdateOperatorBuilder`

**Public Methods:**

- `~IUpdateOperatorBuilder()=default`
- `stepping(int i)`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const =0`


---

## IValueColumn

**Full name:** `gs::runtime::IValueColumn`

**Public Methods:**

- `IValueColumn()=default`
- `~IValueColumn()=default`
- `get_value(size_t idx) const =0`


---

## IVertexColumn

**Full name:** `gs::runtime::IVertexColumn`

**Public Methods:**

- `IVertexColumn()=default`
- `~IVertexColumn()=default`
- `column_type() const override`
- `vertex_column_type() const =0`
- `get_vertex(size_t idx) const =0`
- `get_elem(size_t idx) const override`
- `elem_type() const override`
- `get_labels_set() const =0`


---

## IVertexColumnBuilder

**Full name:** `gs::runtime::IVertexColumnBuilder`

**Public Methods:**

- `IVertexColumnBuilder()=default`
- `~IVertexColumnBuilder()=default`
- `push_back_vertex(VertexRecord v)=0`
- `push_back_elem(const RTAny &val) override`
- `push_back_null()=0`


---

## InsertPipeline

**Full name:** `gs::runtime::InsertPipeline`

**Public Methods:**

- `InsertPipeline()=default`
- `InsertPipeline(InsertPipeline &&rhs)`
- `InsertPipeline(std::vector< std::unique_ptr< IInsertOperator > >...`
- `~InsertPipeline()=default`
- `Execute(GraphInterface &graph, WriteContext &&ctx, const ...`


---

## Intersect

**Full name:** `gs::runtime::Intersect`

**Public Methods:**

- `Binary_Intersect_SL_Impl(const gs::runtime::GraphReadInterface &graph, con...`
- `Binary_Intersect_ML_Impl(const gs::runtime::GraphReadInterface &graph, con...`
- `Binary_Intersect(const gs::runtime::GraphReadInterface &graph, con...`
- `Multiple_Intersect(const gs::runtime::GraphReadInterface &graph, con...`


---

## Join

**Full name:** `gs::runtime::Join`

**Public Methods:**

- `join(Context &&ctx, Context &&ctx2, const JoinParams &params)`


---

## JoinParams

**Full name:** `gs::runtime::JoinParams`


---

## Key

**Full name:** `gs::runtime::Key`

**Public Methods:**

- `Key(EXPR &&expr, const std::vector< std::pair< int, i...`
- `group(const Context &ctx) override`
- `tag_alias() const override`


---

## KeyBase

**Full name:** `gs::runtime::KeyBase`

**Public Methods:**

- `~KeyBase()=default`
- `group(const Context &ctx)=0`
- `tag_alias() const =0`


---

## LabelTriplet

**Full name:** `gs::runtime::LabelTriplet`

**Public Methods:**

- `LabelTriplet()=default`
- `LabelTriplet(label_t src, label_t dst, label_t edge)`
- `to_string() const`
- `operator==(const LabelTriplet &rhs) const`
- `operator<(const LabelTriplet &rhs) const`


---

## Limit

**Full name:** `gs::runtime::Limit`

**Public Methods:**

- `limit(Context &&ctxs, size_t lower, size_t upper)`


---

## List

**Full name:** `gs::runtime::List`

**Public Methods:**

- `List()=default`
- `List(ListImplBase *impl)`
- `operator<(const List &p) const`
- `operator==(const List &p) const`
- `size() const`
- `get(size_t idx) const`
- `elem_type() const`
- `to_string() const`


---

## ListExprBase

**Full name:** `gs::runtime::ListExprBase`

**Public Methods:**

- `ListExprBase()=default`
- `type() const override`


---

## ListImpl

**Full name:** `gs::runtime::ListImpl`

**Public Methods:**

- `ListImpl()=default`
- `operator<(const ListImplBase &p) const override`
- `operator==(const ListImplBase &p) const override`
- `size() const override`
- `type() const override`
- `get(size_t idx) const override`
- `make_list_impl(std::vector< T > &&vals)`


---

## ListImplBase

**Full name:** `gs::runtime::ListImplBase`

**Public Methods:**

- `~ListImplBase()=default`
- `operator<(const ListImplBase &p) const =0`
- `operator==(const ListImplBase &p) const =0`
- `size() const =0`
- `type() const =0`
- `get(size_t idx) const =0`


---

## ListValueColumn

**Full name:** `gs::runtime::ListValueColumn`

**Public Methods:**

- `ListValueColumn(RTAnyType type)`
- `~ListValueColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_value(size_t idx) const override`
- `generate_signature() const override`
- ... and 5 more methods


---

## ListValueColumnBase

**Full name:** `gs::runtime::ListValueColumnBase`

**Public Methods:**

- `unfold() const =0`


---

## ListValueColumnBuilder

**Full name:** `gs::runtime::ListValueColumnBuilder`

**Public Methods:**

- `ListValueColumnBuilder(RTAnyType type)`
- `~ListValueColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(const List &val)`
- `set_arena(const std::shared_ptr< Arena > &ptr) override`
- `finish() override`


---

## Load

**Full name:** `gs::runtime::Load`

**Public Methods:**

- `load_single_edge(InsertInterface &graph, WriteContext &&ctxs, labe...`
- `load_single_vertex(InsertInterface &graph, WriteContext &&ctxs, labe...`
- `load(InsertInterface &graph, WriteContext &&ctxs, cons...`


---

## LogicalExpr

**Full name:** `gs::runtime::LogicalExpr`

**Public Methods:**

- `LogicalExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_impl(const RTAny &lhs_val, const RTAny &rhs_val) const`
- `type() const override`
- `is_optional() const override`


---

## MLVertexColumn

**Full name:** `gs::runtime::MLVertexColumn`

**Public Methods:**

- `MLVertexColumn()=default`
- `~MLVertexColumn()=default`
- `size() const override`
- `column_info() const override`
- `vertex_column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `get_vertex(size_t idx) const override`
- `is_optional() const override`
- `has_value(size_t idx) const override`
- ... and 4 more methods


---

## MLVertexColumnBuilder

**Full name:** `gs::runtime::MLVertexColumnBuilder`

**Public Methods:**

- `MLVertexColumnBuilder()`
- `MLVertexColumnBuilder(const std::set< label_t > &labels)`
- `~MLVertexColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_opt(VertexRecord v)`
- `push_back_vertex(VertexRecord v) override`
- `push_back_null() override`
- `finish() override`


---

## MSVertexColumn

**Full name:** `gs::runtime::MSVertexColumn`

**Public Methods:**

- `MSVertexColumn()=default`
- `~MSVertexColumn()=default`
- `size() const override`
- `column_info() const override`
- `vertex_column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `get_vertex(size_t idx) const override`
- `is_optional() const override`
- `has_value(size_t idx) const override`
- ... and 6 more methods


---

## MSVertexColumnBuilder

**Full name:** `gs::runtime::MSVertexColumnBuilder`

**Public Methods:**

- `MSVertexColumnBuilder(label_t label=std::numeric_limits< label_t >::max())`
- `~MSVertexColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_vertex(VertexRecord v) override`
- `start_label(label_t label)`
- `push_back_opt(vid_t v)`
- `push_back_null() override`
- `finish() override`


---

## Map

**Full name:** `gs::runtime::Map`

**Public Methods:**

- `make_map(MapImpl *impl)`
- `key_vals() const`
- `operator<(const Map &p) const`
- `operator==(const Map &p) const`
- `size() const`
- `to_string() const`


---

## MapExpr

**Full name:** `gs::runtime::MapExpr`

**Public Methods:**

- `MapExpr(const std::vector< RTAny > &keys, std::vector< st...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## MapImpl

**Full name:** `gs::runtime::MapImpl`

**Public Methods:**

- `MapImpl()`
- `~MapImpl()`
- `size() const`
- `operator<(const MapImpl &p) const`
- `operator==(const MapImpl &p) const`
- `make_map_impl(const std::vector< RTAny > &keys, const std::vect...`


---

## MultiPropsEdgePropertyEdgeAccessor

**Full name:** `gs::runtime::MultiPropsEdgePropertyEdgeAccessor`

**Public Methods:**

- `MultiPropsEdgePropertyEdgeAccessor(const GraphInterface &graph, const std::string &name)`
- `typed_eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_path(size_t idx) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `get_index(const LabelTriplet &label) const`


---

## MultiPropsEdgePropertyPathAccessor

**Full name:** `gs::runtime::MultiPropsEdgePropertyPathAccessor`

**Public Methods:**

- `MultiPropsEdgePropertyPathAccessor(const GraphInterface &graph, const std::string &p...`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`
- `is_optional() const override`
- `get_index(const LabelTriplet &label) const`


---

## NodesExpr

**Full name:** `gs::runtime::NodesExpr`

**Public Methods:**

- `NodesExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const override`
- `elem_type() const override`


---

## OprTimer

**Full name:** `gs::runtime::OprTimer`

**Public Methods:**

- `OprTimer()`
- `set_name(const std::string &name)`
- `name() const`
- `id() const`
- `add_child(std::unique_ptr< OprTimer > &&child)`
- `set_next(std::unique_ptr< OprTimer > &&next)`
- `record(const TimerUnit &tu)`
- `add_num_tuples(uint64_t num)`
- `~OprTimer()=default`
- `output(const std::string &path) const`
- ... and 4 more methods


---

## OptionalBDMLEdgeColumn

**Full name:** `gs::runtime::OptionalBDMLEdgeColumn`

**Public Methods:**

- `OptionalBDMLEdgeColumn(const std::vector< std::pair< LabelTriplet, Prope...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `get_labels() const override`
- `edge_column_type() const override`
- `is_optional() const override`
- ... and 1 more methods


---

## OptionalBDSLEdgeColumn

**Full name:** `gs::runtime::OptionalBDSLEdgeColumn`

**Public Methods:**

- `OptionalBDSLEdgeColumn(const LabelTriplet &label, PropertyType prop_type)`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `is_optional() const override`
- `has_value(size_t idx) const override`
- ... and 2 more methods


---

## OptionalGeneralPathColumn

**Full name:** `gs::runtime::OptionalGeneralPathColumn`

**Public Methods:**

- `OptionalGeneralPathColumn()=default`
- `~OptionalGeneralPathColumn()`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `is_optional() const override`
- `elem_type() const override`
- `has_value(size_t idx) const override`
- `get_elem(size_t idx) const override`
- ... and 6 more methods


---

## OptionalGeneralPathColumnBuilder

**Full name:** `gs::runtime::OptionalGeneralPathColumnBuilder`

**Public Methods:**

- `OptionalGeneralPathColumnBuilder()=default`
- `~OptionalGeneralPathColumnBuilder()=default`
- `push_back_opt(const Path &p, bool valid)`
- `push_back_elem(const RTAny &val) override`
- `push_back_null()`
- `reserve(size_t size) override`
- `set_arena(const std::shared_ptr< Arena > &arena) override`
- `finish() override`


---

## OptionalSDMLEdgeColumn

**Full name:** `gs::runtime::OptionalSDMLEdgeColumn`

**Public Methods:**

- `OptionalSDMLEdgeColumn(Direction dir, const std::vector< std::pair< Labe...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `get_labels() const override`
- `dir() const`
- `edge_column_type() const override`
- ... and 2 more methods


---

## OptionalSDSLEdgeColumn

**Full name:** `gs::runtime::OptionalSDSLEdgeColumn`

**Public Methods:**

- `OptionalSDSLEdgeColumn(Direction dir, const LabelTriplet &label, Propert...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `dir() const`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- ... and 4 more methods


---

## OptionalValueColumn

**Full name:** `gs::runtime::OptionalValueColumn`

**Public Methods:**

- `OptionalValueColumn()=default`
- `~OptionalValueColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_value(size_t idx) const override`
- `generate_signature() const override`
- ... and 5 more methods


---

## OptionalValueColumnBuilder

**Full name:** `gs::runtime::OptionalValueColumnBuilder`

**Public Methods:**

- `OptionalValueColumnBuilder()=default`
- `~OptionalValueColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(const T &val, bool valid)`
- `push_back_null() override`
- `set_arena(const std::shared_ptr< Arena > &arena) override`
- `finish() override`


---

## OrderBy

**Full name:** `gs::runtime::OrderBy`

**Public Methods:**

- `order_by_with_limit(const GraphReadInterface &graph, Context &&ctx, c...`
- `staged_order_by_with_limit(const GraphReadInterface &graph, Context &&ctx, c...`
- `order_by_with_limit_with_indices(const GraphReadInterface &graph, Context &&ctx, s...`


---

## PairsFstGetter

**Full name:** `gs::runtime::PairsFstGetter`

**Public Methods:**

- `PairsFstGetter(int from, int to)`
- `evaluate(WriteContext &ctx, WriteContext &&ret) override`


---

## PairsGetter

**Full name:** `gs::runtime::PairsGetter`

**Public Methods:**

- `PairsGetter(int from, int first, int second)`
- `evaluate(WriteContext &ctx, WriteContext &&ret) override`


---

## PairsSndGetter

**Full name:** `gs::runtime::PairsSndGetter`

**Public Methods:**

- `PairsSndGetter(int from, int to)`
- `evaluate(WriteContext &ctx, WriteContext &&ret) override`


---

## ParamAccessor

**Full name:** `gs::runtime::ParamAccessor`

**Public Methods:**

- `ParamAccessor(const std::map< std::string, std::string > &param...`
- `typed_eval_path(size_t) const`
- `typed_eval_vertex(label_t, vid_t, size_t) const`
- `typed_eval_edge(const LabelTriplet &, vid_t, vid_t, const Any &, ...`
- `eval_path(size_t) const override`
- `eval_vertex(label_t, vid_t, size_t) const override`
- `eval_edge(const LabelTriplet &, vid_t, vid_t, const Any &, ...`


---

## ParamsGetter

**Full name:** `gs::runtime::ParamsGetter`

**Public Methods:**

- `ParamsGetter(const std::string &val, int alias)`
- `evaluate(WriteContext &ctx, WriteContext &&ret) override`


---

## Path

**Full name:** `gs::runtime::Path`

**Public Methods:**

- `Path()=default`
- `Path(PathImpl *impl)`
- `to_string() const`
- `len() const`
- `get_end() const`
- `relationships() const`
- `nodes()`
- `edge_labels() const`
- `get_start() const`
- `operator<(const Path &p) const`
- ... and 1 more methods


---

## PathExpand

**Full name:** `gs::runtime::PathExpand`

**Public Methods:**

- `edge_expand_v(const GraphReadInterface &graph, Context &&ctx, c...`
- `edge_expand_p(const GraphReadInterface &graph, Context &&ctx, c...`
- `all_shortest_paths_with_given_source_and_dest(const GraphReadInterface &graph, Context &&ctx, c...`
- `single_source_single_dest_shortest_path(const GraphReadInterface &graph, Context &&ctx, c...`
- `single_source_shortest_path_with_order_by_length_limit(const GraphReadInterface &graph, Context &&ctx, c...`
- `single_source_shortest_path(const GraphReadInterface &graph, Context &&ctx, c...`
- `single_source_shortest_path_with_special_vertex_predicate(const GraphReadInterface &graph, Context &&ctx, c...`


---

## PathExpandParams

**Full name:** `gs::runtime::PathExpandParams`


---

## PathIdPathAccessor

**Full name:** `gs::runtime::PathIdPathAccessor`

**Public Methods:**

- `PathIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## PathImpl

**Full name:** `gs::runtime::PathImpl`

**Public Methods:**

- `make_path_impl(label_t label, vid_t v)`
- `make_path_impl(label_t label, label_t edge_label, std::vector< v...`
- `make_path_impl(const std::vector< label_t > &edge_labels, const ...`
- `expand(label_t edge_label, label_t label, vid_t v) const`
- `to_string() const`
- `get_end() const`
- `get_start() const`
- `operator<(const PathImpl &p) const`
- `operator==(const PathImpl &p) const`


---

## PathLenPathAccessor

**Full name:** `gs::runtime::PathLenPathAccessor`

**Public Methods:**

- `PathLenPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## PlanParser

**Full name:** `gs::runtime::PlanParser`

**Public Methods:**

- `PlanParser(const PlanParser &)=delete`
- `PlanParser(PlanParser &&)=delete`
- `operator=(const PlanParser &)=delete`
- `operator=(PlanParser &&)=delete`
- `~PlanParser()=default`
- `init()`
- `register_read_operator_builder(std::unique_ptr< IReadOperatorBuilder > &&builder)`
- `register_write_operator_builder(std::unique_ptr< IInsertOperatorBuilder > &&builder)`
- `register_update_operator_builder(std::unique_ptr< IUpdateOperatorBuilder > &&builder)`
- `parse_read_pipeline_with_meta(const gs::Schema &schema, const ContextMeta &ctx_...`
- ... and 4 more methods


---

## Project

**Full name:** `gs::runtime::Project`

**Public Methods:**

- `project(Context &&ctx, const std::vector< std::unique_ptr...`
- `project_order_by_fuse(const GraphReadInterface &graph, const std::map< ...`


---

## ProjectExpr

**Full name:** `gs::runtime::ProjectExpr`

**Public Methods:**

- `ProjectExpr(EXPR &&expr, const COLLECTOR_T &collector, int alias)`
- `evaluate(const Context &ctx, Context &&ret) override`
- `order_by_limit(const Context &ctx, bool asc, size_t limit, std::...`
- `alias() const override`


---

## ProjectExprBase

**Full name:** `gs::runtime::ProjectExprBase`

**Public Methods:**

- `~ProjectExprBase()=default`
- `evaluate(const Context &ctx, Context &&ret)=0`
- `alias() const =0`
- `order_by_limit(const Context &ctx, bool asc, size_t limit, std::...`


---

## RTAny

**Full name:** `gs::runtime::RTAny`

**Public Methods:**

- `RTAny()`
- `RTAny(RTAnyType type)`
- `RTAny(const Any &val)`
- `to_any() const`
- `RTAny(const EdgeData &val)`
- `RTAny(const RTAny &rhs)`
- `RTAny(const Path &p)`
- `~RTAny()=default`
- `is_null() const`
- `numerical_cmp(const RTAny &other) const`
- ... and 56 more methods


---

## ReadPipeline

**Full name:** `gs::runtime::ReadPipeline`

**Public Methods:**

- `ReadPipeline()`
- `ReadPipeline(ReadPipeline &&rhs)`
- `ReadPipeline(std::vector< std::unique_ptr< IReadOperator > > &...`
- `~ReadPipeline()=default`
- `Execute(const GraphReadInterface &graph, Context &&ctx, c...`


---

## Reducer

**Full name:** `gs::runtime::Reducer`

**Public Methods:**

- `Reducer(REDUCER_T &&reducer, COLLECTOR_T &&collector, int alias)`
- `reduce(const Context &ctx, Context &&ret, const std::vec...`


---

## ReducerBase

**Full name:** `gs::runtime::ReducerBase`

**Public Methods:**

- `~ReducerBase()=default`
- `reduce(const Context &ctx, Context &&ret, const std::vec...`


---

## Relation

**Full name:** `gs::runtime::Relation`

**Public Methods:**

- `operator<(const Relation &r) const`
- `operator==(const Relation &r) const`
- `start_node() const`
- `end_node() const`


---

## RelationshipsExpr

**Full name:** `gs::runtime::RelationshipsExpr`

**Public Methods:**

- `RelationshipsExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const override`
- `elem_type() const override`


---

## SDMLEdgeColumn

**Full name:** `gs::runtime::SDMLEdgeColumn`

**Public Methods:**

- `SDMLEdgeColumn(Direction dir, const std::vector< std::pair< Labe...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge(const FUNC_T &func) const`
- `get_labels() const override`
- `dir() const`
- ... and 1 more methods


---

## SDMLEdgeColumnBuilder

**Full name:** `gs::runtime::SDMLEdgeColumnBuilder`

**Public Methods:**

- `builder(Direction dir, const std::vector< std::pair< Labe...`
- `optional_builder(Direction dir, const std::vector< std::pair< Labe...`
- `~SDMLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(label_t index, vid_t src, vid_t dst, const EdgeData &data)`
- `push_back_opt(LabelTriplet label, vid_t src, vid_t dst, const E...`
- `push_back_null()`
- `push_back_endpoints(label_t index, vid_t src, vid_t dst)`
- `finish() override`


---

## SDSLEdgeColumn

**Full name:** `gs::runtime::SDSLEdgeColumn`

**Public Methods:**

- `SDSLEdgeColumn(Direction dir, const LabelTriplet &label, Propert...`
- `get_edge(size_t idx) const override`
- `set_edge_data(size_t idx, int32_t col_id, const Any &new_val) override`
- `size() const override`
- `dir() const`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- ... and 3 more methods


---

## SDSLEdgeColumnBuilder

**Full name:** `gs::runtime::SDSLEdgeColumnBuilder`

**Public Methods:**

- `builder(Direction dir, const LabelTriplet &label, Propert...`
- `optional_builder(Direction dir, const LabelTriplet &label, Propert...`
- `~SDSLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(vid_t src, vid_t dst, const EdgeData &data)`
- `push_back_endpoints(vid_t src, vid_t dst)`
- `push_back_null()`
- `finish() override`


---

## SDSLEdgeColumnBuilderBeta

**Full name:** `gs::runtime::SDSLEdgeColumnBuilderBeta`

**Public Methods:**

- `SDSLEdgeColumnBuilderBeta(Direction dir, const LabelTriplet &label, Propert...`
- `~SDSLEdgeColumnBuilderBeta()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(vid_t src, vid_t dst, const T &data)`
- `finish() override`


---

## SLVertexColumn

**Full name:** `gs::runtime::SLVertexColumn`

**Public Methods:**

- `SLVertexColumn(label_t label)`
- `~SLVertexColumn()=default`
- `size() const override`
- `column_info() const override`
- `vertex_column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offset) const override`
- `get_vertex(size_t idx) const override`
- `is_optional() const override`
- `has_value(size_t idx) const override`
- ... and 8 more methods


---

## SPEdgePredicate

**Full name:** `gs::runtime::SPEdgePredicate`

**Public Methods:**

- `~SPEdgePredicate()`
- `type() const =0`
- `data_type() const =0`


---

## SPVertexPredicate

**Full name:** `gs::runtime::SPVertexPredicate`

**Public Methods:**

- `~SPVertexPredicate()`
- `type() const =0`
- `data_type() const =0`


---

## ScalarFunctionExpr

**Full name:** `gs::runtime::ScalarFunctionExpr`

**Public Methods:**

- `ScalarFunctionExpr(neug_func_exec_t fn, RTAnyType ret_type, std::vec...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Scan

**Full name:** `gs::runtime::Scan`

**Public Methods:**

- `scan_vertex(Context &&ctx, const GraphReadInterface &graph, c...`
- `scan_vertex_with_limit(Context &&ctx, const GraphReadInterface &graph, c...`
- `scan_vertex_with_special_vertex_predicate(Context &&ctx, const GraphReadInterface &graph, c...`
- `filter_gids(Context &&ctx, const GraphReadInterface &graph, c...`
- `filter_gids_with_special_vertex_predicate(Context &&ctx, const GraphReadInterface &graph, c...`
- `filter_oids(Context &&ctx, const GraphReadInterface &graph, c...`
- `filter_oids_with_special_vertex_predicate(Context &&ctx, const GraphReadInterface &graph, c...`
- `find_vertex_with_oid(Context &&ctx, const GraphReadInterface &graph, l...`
- `find_vertex_with_gid(Context &&ctx, const GraphReadInterface &graph, l...`


---

## ScanParams

**Full name:** `gs::runtime::ScanParams`

**Public Methods:**

- `ScanParams()`


---

## Select

**Full name:** `gs::runtime::Select`

**Public Methods:**

- `select(Context &&ctx, const PRED_T &pred)`


---

## Set

**Full name:** `gs::runtime::Set`

**Public Methods:**

- `Set()=default`
- `Set(SetImplBase *impl)`
- `insert(const RTAny &val)`
- `operator<(const Set &p) const`
- `operator==(const Set &p) const`
- `exists(const RTAny &val) const`
- `values() const`
- `to_string() const`
- `elem_type() const`
- `size() const`


---

## SetImpl

**Full name:** `gs::runtime::SetImpl`

**Public Methods:**

- `SetImpl()=default`
- `~SetImpl()`
- `type() const override`
- `exists(const RTAny &val) const override`
- `exists(const T &val) const`
- `operator<(const SetImplBase &p) const override`
- `operator==(const SetImplBase &p) const override`
- `insert(const RTAny &val) override`
- `insert(const T &val)`
- `size() const override`
- ... and 2 more methods


---

## SetImpl< VertexRecord >

**Full name:** `gs::runtime::SetImpl< VertexRecord >`

**Public Methods:**

- `SetImpl()=default`
- `~SetImpl()`
- `values() const override`
- `type() const override`
- `exists(const RTAny &val) const override`
- `exists(VertexRecord val) const`
- `operator<(const SetImplBase &p) const override`
- `operator==(const SetImplBase &p) const override`
- `insert(const RTAny &val) override`
- `insert(VertexRecord val)`
- ... and 2 more methods


---

## SetImplBase

**Full name:** `gs::runtime::SetImplBase`

**Public Methods:**

- `~SetImplBase()=default`
- `operator<(const SetImplBase &p) const =0`
- `operator==(const SetImplBase &p) const =0`
- `size() const =0`
- `values() const =0`
- `insert(const RTAny &val)=0`
- `exists(const RTAny &val) const =0`
- `type() const =0`


---

## ShortestPathParams

**Full name:** `gs::runtime::ShortestPathParams`


---

## SigColumn

**Full name:** `gs::runtime::SigColumn`

**Public Methods:**

- `SigColumn(const std::vector< T > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< Date >

**Full name:** `gs::runtime::SigColumn< Date >`

**Public Methods:**

- `SigColumn(const std::vector< Date > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< DateTime >

**Full name:** `gs::runtime::SigColumn< DateTime >`

**Public Methods:**

- `SigColumn(const std::vector< DateTime > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< Interval >

**Full name:** `gs::runtime::SigColumn< Interval >`

**Public Methods:**

- `SigColumn(const std::vector< Interval > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< TimeStamp >

**Full name:** `gs::runtime::SigColumn< TimeStamp >`

**Public Methods:**

- `SigColumn(const std::vector< TimeStamp > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< VertexRecord >

**Full name:** `gs::runtime::SigColumn< VertexRecord >`

**Public Methods:**

- `SigColumn(const std::vector< VertexRecord > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## string_view >

**Full name:** `gs::runtime::SigColumn< std::string_view >`

**Public Methods:**

- `SigColumn(const std::vector< std::string_view > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## Sink

**Full name:** `gs::runtime::Sink`

**Public Methods:**

- `sink(const Context &ctx, const GraphInterface &graph)`
- `sink(const Context &ctx, const GraphInterface &graph, ...`
- `sink_encoder(const Context &ctx, const GraphInterface &graph, ...`
- `sink_beta(const Context &ctx, const GraphInterface &graph, ...`


---

## StartNodeExpr

**Full name:** `gs::runtime::StartNodeExpr`

**Public Methods:**

- `StartNodeExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## StrConcatExpr

**Full name:** `gs::runtime::StrConcatExpr`

**Public Methods:**

- `StrConcatExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## StrListSizeExpr

**Full name:** `gs::runtime::StrListSizeExpr`

**Public Methods:**

- `StrListSizeExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## StringImpl

**Full name:** `gs::runtime::StringImpl`

**Public Methods:**

- `str_view() const`
- `make_string_impl(const std::string &str)`
- `make_string_impl(const std::string_view &str)`


---

## TimerUnit

**Full name:** `gs::runtime::TimerUnit`

**Public Methods:**

- `TimerUnit()`
- `~TimerUnit()=default`
- `start()`
- `elapsed() const`


---

## ToFloatExpr

**Full name:** `gs::runtime::ToFloatExpr`

**Public Methods:**

- `to_double(const RTAny &val)`
- `ToFloatExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Tuple

**Full name:** `gs::runtime::Tuple`

**Public Methods:**

- `make_tuple_impl(std::tuple< Args... > &&args)`
- `make_generic_tuple_impl(std::vector< RTAny > &&args)`
- `Tuple()=default`
- `Tuple(TupleImplBase *impl)`
- `operator<(const Tuple &p) const`
- `operator==(const Tuple &p) const`
- `size() const`
- `get(size_t idx) const`
- `to_string() const`


---

## TupleExpr

**Full name:** `gs::runtime::TupleExpr`

**Public Methods:**

- `TupleExpr(std::vector< std::unique_ptr< ExprBase > > &&exprs)`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## TupleImpl

**Full name:** `gs::runtime::TupleImpl`

**Public Methods:**

- `TupleImpl()=default`
- `~TupleImpl()=default`
- `TupleImpl(Args &&... args)`
- `TupleImpl(std::tuple< Args... > &&args)`
- `operator<(const TupleImplBase &p) const override`
- `operator==(const TupleImplBase &p) const override`
- `get(size_t idx) const override`
- `size() const override`
- `to_string() const override`


---

## TupleImpl< RTAny >

**Full name:** `gs::runtime::TupleImpl< RTAny >`

**Public Methods:**

- `TupleImpl()=default`
- `~TupleImpl()`
- `TupleImpl(std::vector< RTAny > &&val)`
- `operator<(const TupleImplBase &p) const override`
- `operator==(const TupleImplBase &p) const override`
- `size() const override`
- `get(size_t idx) const override`
- `to_string() const override`


---

## TupleImplBase

**Full name:** `gs::runtime::TupleImplBase`

**Public Methods:**

- `~TupleImplBase()=default`
- `operator<(const TupleImplBase &p) const =0`
- `operator==(const TupleImplBase &p) const =0`
- `size() const =0`
- `get(size_t idx) const =0`
- `to_string() const =0`


---

## TypedConverter

**Full name:** `gs::runtime::TypedConverter`


---

## TypedConverter< Date >

**Full name:** `gs::runtime::TypedConverter< Date >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(Date val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< DateTime >

**Full name:** `gs::runtime::TypedConverter< DateTime >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(DateTime val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< Interval >

**Full name:** `gs::runtime::TypedConverter< Interval >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(Interval val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< Map >

**Full name:** `gs::runtime::TypedConverter< Map >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(Map val)`
- `name()`


---

## TypedConverter< Relation >

**Full name:** `gs::runtime::TypedConverter< Relation >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(const Relation &val)`
- `name()`


---

## TypedConverter< Set >

**Full name:** `gs::runtime::TypedConverter< Set >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(Set val)`
- `name()`


---

## TypedConverter< TimeStamp >

**Full name:** `gs::runtime::TypedConverter< TimeStamp >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(TimeStamp val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< Tuple >

**Full name:** `gs::runtime::TypedConverter< Tuple >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(Tuple val)`
- `name()`


---

## TypedConverter< VertexRecord >

**Full name:** `gs::runtime::TypedConverter< VertexRecord >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(VertexRecord val)`
- `name()`


---

## TypedConverter< bool >

**Full name:** `gs::runtime::TypedConverter< bool >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(bool val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< double >

**Full name:** `gs::runtime::TypedConverter< double >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(double val)`
- `typed_from_string(const std::string &str)`
- `name()`


---

## TypedConverter< float >

**Full name:** `gs::runtime::TypedConverter< float >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(float val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< int32_t >

**Full name:** `gs::runtime::TypedConverter< int32_t >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(int val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< int64_t >

**Full name:** `gs::runtime::TypedConverter< int64_t >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(int64_t val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## string_view >

**Full name:** `gs::runtime::TypedConverter< std::string_view >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(const std::string_view &val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< uint32_t >

**Full name:** `gs::runtime::TypedConverter< uint32_t >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(uint32_t val)`
- `name()`
- `typed_from_string(const std::string &str)`


---

## TypedConverter< uint64_t >

**Full name:** `gs::runtime::TypedConverter< uint64_t >`

**Public Methods:**

- `type()`
- `to_typed(const RTAny &val)`
- `from_typed(uint64_t val)`
- `name()`


---

## TypedTupleExpr

**Full name:** `gs::runtime::TypedTupleExpr`

**Public Methods:**

- `TypedTupleExpr(std::array< std::unique_ptr< ExprBase >, sizeof.....`
- `eval_path_impl(std::index_sequence< Is... >, size_t idx, Arena &...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex_impl(std::index_sequence< Is... >, label_t label, vid_...`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge_impl(std::index_sequence< Is... >, const LabelTriplet ...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## UEdgeExpand

**Full name:** `gs::runtime::UEdgeExpand`

**Public Methods:**

- `edge_expand_v_without_pred(const GraphUpdateInterface &graph, Context &&ctx,...`
- `edge_expand_e_without_pred(const GraphUpdateInterface &graph, Context &&ctx,...`


---

## UGetV

**Full name:** `gs::runtime::UGetV`

**Public Methods:**

- `get_vertex_from_vertices(const GraphUpdateInterface &graph, Context &&ctx,...`


---

## UScan

**Full name:** `gs::runtime::UScan`

**Public Methods:**

- `scan(const GraphUpdateInterface &graph, Context &&ctx,...`


---

## UnaryLogicalExpr

**Full name:** `gs::runtime::UnaryLogicalExpr`

**Public Methods:**

- `UnaryLogicalExpr(std::unique_ptr< ExprBase > &&expr, ::common::Log...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Unfold

**Full name:** `gs::runtime::Unfold`

**Public Methods:**

- `unfold(Context &&ctxs, int key, int alias)`
- `unfold(WriteContext &&ctxs, int key, int alias)`


---

## Union

**Full name:** `gs::runtime::Union`

**Public Methods:**

- `union_op(std::vector< Context > &&ctxs)`


---

## UpdatePipeline

**Full name:** `gs::runtime::UpdatePipeline`

**Public Methods:**

- `UpdatePipeline(UpdatePipeline &&rhs)`
- `UpdatePipeline(std::vector< std::unique_ptr< IUpdateOperator > >...`
- `UpdatePipeline(InsertPipeline &&insert)`
- `~UpdatePipeline()=default`
- `Execute(GraphUpdateInterface &graph, Context &&ctx, const...`
- `Execute(GraphUpdateInterface &graph, WriteContext &&ctx, ...`
- `is_insert() const`


---

## ValueColumn

**Full name:** `gs::runtime::ValueColumn`

**Public Methods:**

- `ValueColumn()`
- `~ValueColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_value(size_t idx) const override`
- ... and 7 more methods


---

## ValueColumnBuilder

**Full name:** `gs::runtime::ValueColumnBuilder`

**Public Methods:**

- `ValueColumnBuilder()=default`
- `~ValueColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const RTAny &val) override`
- `push_back_opt(const T &val)`
- `finish() override`
- `set_arena(const std::shared_ptr< Arena > &arena) override`


---

## Var

**Full name:** `gs::runtime::Var`

**Public Methods:**

- `Var(const GraphInterface &graph, const Context &ctx, ...`
- `~Var()`
- `get(size_t path_idx) const`
- `get_vertex(label_t label, vid_t v, size_t idx) const`
- `get_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const`
- `is_optional() const`
- `builder() const`


---

## VarGetterBase

**Full name:** `gs::runtime::VarGetterBase`

**Public Methods:**

- `~VarGetterBase()=default`
- `eval_path(size_t idx) const =0`
- `eval_vertex(label_t label, vid_t v, size_t idx) const =0`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `name() const =0`


---

## VariableExpr

**Full name:** `gs::runtime::VariableExpr`

**Public Methods:**

- `VariableExpr(const GraphInterface &graph, const Context &ctx, ...`
- `eval_path(size_t idx, Arena &) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## VertexGIdPathAccessor

**Full name:** `gs::runtime::VertexGIdPathAccessor`

**Public Methods:**

- `VertexGIdPathAccessor(const Context &ctx, int tag)`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexGIdVertexAccessor

**Full name:** `gs::runtime::VertexGIdVertexAccessor`

**Public Methods:**

- `VertexGIdVertexAccessor()`
- `typed_eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx) const override`


---

## VertexIdVertexAccessor

**Full name:** `gs::runtime::VertexIdVertexAccessor`

**Public Methods:**

- `VertexIdVertexAccessor()`
- `typed_eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx) const override`


---

## VertexLabelPathAccessor

**Full name:** `gs::runtime::VertexLabelPathAccessor`

**Public Methods:**

- `VertexLabelPathAccessor(const Schema &schema, const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexLabelVertexAccessor

**Full name:** `gs::runtime::VertexLabelVertexAccessor`

**Public Methods:**

- `VertexLabelVertexAccessor(const Schema &schema)`
- `typed_eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx) const override`


---

## VertexPathAccessor

**Full name:** `gs::runtime::VertexPathAccessor`

**Public Methods:**

- `VertexPathAccessor(const Context &ctx, int tag)`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexPropertyBetweenPredicateBeta

**Full name:** `gs::runtime::VertexPropertyBetweenPredicateBeta`

**Public Methods:**

- `VertexPropertyBetweenPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyBetweenPredicateBeta(VertexPropertyBetweenPredicateBeta &&other)`
- `~VertexPropertyBetweenPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyEQPredicateBeta

**Full name:** `gs::runtime::VertexPropertyEQPredicateBeta`

**Public Methods:**

- `VertexPropertyEQPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyEQPredicateBeta(VertexPropertyEQPredicateBeta &&other)`
- `~VertexPropertyEQPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyGEPredicateBeta

**Full name:** `gs::runtime::VertexPropertyGEPredicateBeta`

**Public Methods:**

- `VertexPropertyGEPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyGEPredicateBeta(VertexPropertyGEPredicateBeta &&other)`
- `~VertexPropertyGEPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyGTPredicateBeta

**Full name:** `gs::runtime::VertexPropertyGTPredicateBeta`

**Public Methods:**

- `VertexPropertyGTPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyGTPredicateBeta(VertexPropertyGTPredicateBeta &&other)`
- `~VertexPropertyGTPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyLEPredicateBeta

**Full name:** `gs::runtime::VertexPropertyLEPredicateBeta`

**Public Methods:**

- `VertexPropertyLEPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyLEPredicateBeta(VertexPropertyLEPredicateBeta &&other)`
- `~VertexPropertyLEPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyLTPredicateBeta

**Full name:** `gs::runtime::VertexPropertyLTPredicateBeta`

**Public Methods:**

- `VertexPropertyLTPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyLTPredicateBeta(VertexPropertyLTPredicateBeta &&other)`
- `~VertexPropertyLTPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyNEPredicateBeta

**Full name:** `gs::runtime::VertexPropertyNEPredicateBeta`

**Public Methods:**

- `VertexPropertyNEPredicateBeta(const GraphReadInterface &graph, const std::strin...`
- `VertexPropertyNEPredicateBeta(VertexPropertyNEPredicateBeta &&other)`
- `~VertexPropertyNEPredicateBeta()=default`
- `type() const override`
- `data_type() const override`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyPathAccessor

**Full name:** `gs::runtime::VertexPropertyPathAccessor`

**Public Methods:**

- `VertexPropertyPathAccessor(const GraphInterface &graph, const Context &ctx, ...`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexPropertyVertexAccessor

**Full name:** `gs::runtime::VertexPropertyVertexAccessor`

**Public Methods:**

- `VertexPropertyVertexAccessor(const GraphInterface &graph, const std::string &prop_name)`
- `typed_eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx) const override`
- `is_optional() const override`


---

## VertexRecord

**Full name:** `gs::runtime::VertexRecord`

**Public Methods:**

- `operator<(const VertexRecord &v) const`
- `operator==(const VertexRecord &v) const`
- `to_string() const`
- `label() const`
- `vid() const`


---

## VertexRecordHash

**Full name:** `gs::runtime::VertexRecordHash`

**Public Methods:**

- `operator()(const VertexRecord &v) const`
- `operator()(const std::pair< VertexRecord, VertexRecord > &v) const`


---

## VertexWithInListExpr

**Full name:** `gs::runtime::VertexWithInListExpr`

**Public Methods:**

- `VertexWithInListExpr(const Context &ctx, std::unique_ptr< ExprBase > &...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## VertexWithInSetExpr

**Full name:** `gs::runtime::VertexWithInSetExpr`

**Public Methods:**

- `VertexWithInSetExpr(const Context &ctx, std::unique_ptr< ExprBase > &...`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## WithInExpr

**Full name:** `gs::runtime::WithInExpr`

**Public Methods:**

- `WithInExpr(const Context &ctx, std::unique_ptr< ExprBase > &...`
- `eval_impl(const RTAny &val) const`
- `eval_path(size_t idx, Arena &arena) const override`
- `eval_vertex(label_t label, vid_t v, size_t idx, Arena &arena)...`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## WriteContext

**Full name:** `gs::runtime::WriteContext`

**Public Methods:**

- `WriteContext()=default`
- `~WriteContext()=default`
- `col_num() const`
- `set(int alias, WriteParamsColumn &&col)`
- `reshuffle(const std::vector< size_t > &offsets)`
- `set_with_reshuffle(int alias, WriteParamsColumn &&col, const std::ve...`
- `get(int alias) const`
- `get(int alias)`
- `clear()`
- `row_num() const`


---

## WriteParams

**Full name:** `gs::runtime::WriteContext::WriteParams`

**Public Methods:**

- `WriteParams()=default`
- `WriteParams(const std::string &val)`
- `WriteParams(std::string_view val)`
- `operator<(const WriteParams &rhs) const`
- `operator==(const WriteParams &rhs) const`
- `unfold() const`
- `pairs() const`
- `to_any(PropertyType type) const`


---

## WriteParamsColumn

**Full name:** `gs::runtime::WriteContext::WriteParamsColumn`

**Public Methods:**

- `WriteParamsColumn()`
- `WriteParamsColumn(std::vector< WriteParams > &&col)`
- `size() const`
- `get(int idx) const`
- `unfold()`
- `pairs()`
- `push_back(WriteParams &&val)`
- `push_back(const WriteParams &val)`
- `clear()`


---

## WriteProjectExprBase

**Full name:** `gs::runtime::WriteProjectExprBase`

**Public Methods:**

- `~WriteProjectExprBase()=default`
- `evaluate(WriteContext &ctx, WriteContext &&ret)=0`


---

## AdjListView

**Full name:** `gs::runtime::graph_interface_impl::AdjListView`

**Public Methods:**

- `AdjListView(const slice_t &slice, timestamp_t timestamp)`
- `begin() const`
- `end() const`


---

## nbr_iterator

**Full name:** `gs::runtime::graph_interface_impl::AdjListView::nbr_iterator`

**Public Methods:**

- `nbr_iterator(const_nbr_ptr_t ptr, const_nbr_ptr_t end, timesta...`
- `operator*() const`
- `operator->() const`
- `operator++()`
- `operator==(const nbr_iterator &rhs) const`
- `operator!=(const nbr_iterator &rhs) const`


---

## EdgeIterator

**Full name:** `gs::runtime::graph_interface_impl::EdgeIterator`

**Public Methods:**

- `EdgeIterator(gs::ReadTransaction::edge_iterator &&iter)`
- `~EdgeIterator()`
- `GetData() const`
- `IsValid() const`
- `Next()`
- `GetNeighbor() const`
- `GetNeighborLabel() const`
- `GetEdgeLabel() const`


---

## GraphView

**Full name:** `gs::runtime::graph_interface_impl::GraphView`

**Public Methods:**

- `GraphView()`
- `GraphView(const gs::MutableCsr< EDATA_T > *csr, timestamp_t...`
- `is_null() const`
- `get_edges(vid_t v) const`
- `foreach_edges_gt(vid_t v, const EDATA_T &min_value, const FUNC_T &...`
- `foreach_edges_lt(vid_t v, const EDATA_T &max_value, const FUNC_T &...`


---

## VertexArray

**Full name:** `gs::runtime::graph_interface_impl::VertexArray`

**Public Methods:**

- `VertexArray()`
- `VertexArray(const VertexSet &keys, const T &val)`
- `~VertexArray()`
- `Init(const VertexSet &keys, const T &val)`
- `operator[](vid_t v)`
- `operator[](vid_t v) const`


---

## VertexColumn

**Full name:** `gs::runtime::graph_interface_impl::VertexColumn`

**Public Methods:**

- `VertexColumn(std::shared_ptr< TypedRefColumn< PROP_T > > column)`
- `VertexColumn()`
- `get_view(vid_t v) const`
- `is_null() const`


---

## VertexColumn

**Full name:** `gs::runtime::graph_update_interface_impl::VertexColumn`

**Public Methods:**

- `VertexColumn()`
- `VertexColumn(UpdateTransaction *txn, label_t label, int col_id)`
- `get_view(vid_t v) const`
- `is_null() const`


---

## AvgReducer

**Full name:** `gs::runtime::ops::AvgReducer`


---

## value > >

**Full name:** `gs::runtime::ops::AvgReducer< EXPR, IS_OPTIONAL, std::enable_if_t< std::is_arithmetic< typename EXPR::V >::value > >`

**Public Methods:**

- `AvgReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, double &avg) const`


---

## BatchDeleteEdgeOpr

**Full name:** `gs::runtime::ops::BatchDeleteEdgeOpr`

**Public Methods:**

- `BatchDeleteEdgeOpr(const std::vector< std::vector< std::tuple< label...`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## BatchDeleteEdgeOprBuilder

**Full name:** `gs::runtime::ops::BatchDeleteEdgeOprBuilder`

**Public Methods:**

- `BatchDeleteEdgeOprBuilder()=default`
- `~BatchDeleteEdgeOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## BatchDeleteVertexOpr

**Full name:** `gs::runtime::ops::BatchDeleteVertexOpr`

**Public Methods:**

- `BatchDeleteVertexOpr(const std::vector< std::vector< label_t > > &vert...`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## BatchDeleteVertexOprBuilder

**Full name:** `gs::runtime::ops::BatchDeleteVertexOprBuilder`

**Public Methods:**

- `BatchDeleteVertexOprBuilder()=default`
- `~BatchDeleteVertexOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## BatchInsertEdgeOpr

**Full name:** `gs::runtime::ops::BatchInsertEdgeOpr`

**Public Methods:**

- `BatchInsertEdgeOpr(const label_t &edge_label_id, const label_t &src_...`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## BatchInsertEdgeOprBuilder

**Full name:** `gs::runtime::ops::BatchInsertEdgeOprBuilder`

**Public Methods:**

- `BatchInsertEdgeOprBuilder()=default`
- `~BatchInsertEdgeOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## BatchInsertVertexOpr

**Full name:** `gs::runtime::ops::BatchInsertVertexOpr`

**Public Methods:**

- `BatchInsertVertexOpr(const label_t &vertex_label_id, const PropertyTyp...`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## BatchInsertVertexOprBuilder

**Full name:** `gs::runtime::ops::BatchInsertVertexOprBuilder`

**Public Methods:**

- `BatchInsertVertexOprBuilder()=default`
- `~BatchInsertVertexOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## CSVDataSourceOpr

**Full name:** `gs::runtime::ops::CSVDataSourceOpr`

DataSourceOpr is used to load data from a CSV file.

**Public Methods:**

- `CSVDataSourceOpr(const std::vector< std::shared_ptr< IRecordBatchS...`
- `~CSVDataSourceOpr()=default`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## CaseWhenCollector

**Full name:** `gs::runtime::ops::CaseWhenCollector`

**Public Methods:**

- `CaseWhenCollector(const Context &ctx)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## ColumnWrapper

**Full name:** `gs::runtime::ops::ColumnWrapper`

**Public Methods:**

- `ColumnWrapper(const IContextColumn &column)`
- `operator()(size_t idx) const`


---

## CountDistinctReducer

**Full name:** `gs::runtime::ops::CountDistinctReducer`

**Public Methods:**

- `CountDistinctReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## CountReducer

**Full name:** `gs::runtime::ops::CountReducer`

**Public Methods:**

- `CountReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## CountStartReducer

**Full name:** `gs::runtime::ops::CountStartReducer`

**Public Methods:**

- `CountStartReducer()`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## DataExportOpr

**Full name:** `gs::runtime::ops::DataExportOpr`

**Public Methods:**

- `DataExportOpr(const std::string &extension_name, const std::str...`
- `get_operator_name() const override`
- `Eval(const gs::runtime::GraphReadInterface &graph, con...`


---

## DataExportOprBuilder

**Full name:** `gs::runtime::ops::DataExportOprBuilder`

**Public Methods:**

- `DataExportOprBuilder()=default`
- `~DataExportOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## DataSourceOprBuilder

**Full name:** `gs::runtime::ops::DataSourceOprBuilder`

**Public Methods:**

- `DataSourceOprBuilder()=default`
- `~DataSourceOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## DedupInsertOprBuilder

**Full name:** `gs::runtime::ops::DedupInsertOprBuilder`

**Public Methods:**

- `DedupInsertOprBuilder()=default`
- `~DedupInsertOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## DedupOprBuilder

**Full name:** `gs::runtime::ops::DedupOprBuilder`

**Public Methods:**

- `DedupOprBuilder()=default`
- `~DedupOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## DummySourceOprBuilder

**Full name:** `gs::runtime::ops::DummySourceOprBuilder`

**Public Methods:**

- `DummySourceOprBuilder()=default`
- `~DummySourceOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`
- `stepping(int i) override`


---

## EdgeCollector

**Full name:** `gs::runtime::ops::EdgeCollector`

**Public Methods:**

- `EdgeCollector()`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## EdgeExprWrapper

**Full name:** `gs::runtime::ops::EdgeCollector::EdgeExprWrapper`

**Public Methods:**

- `EdgeExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## EdgeExpandGetVOprBuilder

**Full name:** `gs::runtime::ops::EdgeExpandGetVOprBuilder`

**Public Methods:**

- `EdgeExpandGetVOprBuilder()=default`
- `~EdgeExpandGetVOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## EdgeExpandOprBuilder

**Full name:** `gs::runtime::ops::EdgeExpandOprBuilder`

**Public Methods:**

- `EdgeExpandOprBuilder()=default`
- `~EdgeExpandOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## FirstReducer

**Full name:** `gs::runtime::ops::FirstReducer`

**Public Methods:**

- `FirstReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## GeneralComparer

**Full name:** `gs::runtime::ops::GeneralComparer`

**Public Methods:**

- `GeneralComparer()`
- `~GeneralComparer()`
- `add_keys(Var &&key, bool asc)`
- `operator()(size_t lhs, size_t rhs) const`


---

## GroupByOprBuilder

**Full name:** `gs::runtime::ops::GroupByOprBuilder`

**Public Methods:**

- `GroupByOprBuilder()=default`
- `~GroupByOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## InsertEdgeOpr

**Full name:** `gs::runtime::ops::InsertEdgeOpr`

**Public Methods:**

- `InsertEdgeOpr(std::vector< edge_data_t > &&edge_data)`
- `get_operator_name() const override`
- `eval_impl(GraphInterface &graph, const std::map< std::strin...`
- `Eval(gs::runtime::GraphUpdateInterface &graph, const s...`


---

## InsertEdgeOprBuilder

**Full name:** `gs::runtime::ops::InsertEdgeOprBuilder`

**Public Methods:**

- `InsertEdgeOprBuilder()=default`
- `~InsertEdgeOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## InsertVertexOpr

**Full name:** `gs::runtime::ops::InsertVertexOpr`

**Public Methods:**

- `InsertVertexOpr(std::vector< std::tuple< label_t, vertex_prop_vec...`
- `get_operator_name() const override`
- `eval_impl(GraphInterface &graph, const std::map< std::strin...`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## InsertVertexOprBuilder

**Full name:** `gs::runtime::ops::InsertVertexOprBuilder`

**Public Methods:**

- `InsertVertexOprBuilder()=default`
- `~InsertVertexOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## IntersectOprBuilder

**Full name:** `gs::runtime::ops::IntersectOprBuilder`

**Public Methods:**

- `IntersectOprBuilder()=default`
- `~IntersectOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## IsCountReducer

**Full name:** `gs::runtime::ops::IsCountReducer`


---

## IsCountReducer< CountReducer< EXPR, is_optional > >

**Full name:** `gs::runtime::ops::IsCountReducer< CountReducer< EXPR, is_optional > >`


---

## IsCountReducer< CountStartReducer >

**Full name:** `gs::runtime::ops::IsCountReducer< CountStartReducer >`


---

## JoinOprBuilder

**Full name:** `gs::runtime::ops::JoinOprBuilder`

**Public Methods:**

- `JoinOprBuilder()=default`
- `~JoinOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## KeyBuilder

**Full name:** `gs::runtime::ops::KeyBuilder`

**Public Methods:**

- `make_sp_key(const Context &ctx, const std::vector< std::pair<...`


---

## KeyExpr

**Full name:** `gs::runtime::ops::KeyExpr`

**Public Methods:**

- `KeyExpr(std::tuple< EXPR... > &&exprs)`
- `operator()(size_t idx) const`


---

## LimitOprBuilder

**Full name:** `gs::runtime::ops::LimitOprBuilder`

**Public Methods:**

- `LimitOprBuilder()=default`
- `~LimitOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## ListCollector

**Full name:** `gs::runtime::ops::ListCollector`

**Public Methods:**

- `ListCollector()`
- `init(size_t size)`
- `collect(std::vector< T > &&val)`
- `get()`
- `ListCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## ListExprWrapper

**Full name:** `gs::runtime::ops::ListCollector::ListExprWrapper`

**Public Methods:**

- `ListExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## LoadOprBuilder

**Full name:** `gs::runtime::ops::LoadOprBuilder`

**Public Methods:**

- `LoadOprBuilder()=default`
- `~LoadOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## MLVertexCollector

**Full name:** `gs::runtime::ops::MLVertexCollector`

**Public Methods:**

- `MLVertexCollector()`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## MLVertexWrapper

**Full name:** `gs::runtime::ops::MLVertexWrapper`

**Public Methods:**

- `MLVertexWrapper(const VERTEX_COL &vertex)`
- `operator()(size_t idx) const`


---

## MaxReducer

**Full name:** `gs::runtime::ops::MaxReducer`

**Public Methods:**

- `MaxReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## MinReducer

**Full name:** `gs::runtime::ops::MinReducer`

**Public Methods:**

- `MinReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## OptionalTypedVarWrapper

**Full name:** `gs::runtime::ops::OptionalTypedVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `OptionalTypedVarWrapper(Var &&vars)`


---

## OptionalValueCollector

**Full name:** `gs::runtime::ops::OptionalValueCollector`

**Public Methods:**

- `OptionalValueCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## OptionalExprWrapper

**Full name:** `gs::runtime::ops::OptionalValueCollector::OptionalExprWrapper`

**Public Methods:**

- `OptionalExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## OptionalVarWrapper

**Full name:** `gs::runtime::ops::OptionalVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `OptionalVarWrapper(Var &&vars)`


---

## OrderByOprBuilder

**Full name:** `gs::runtime::ops::OrderByOprBuilder`

**Public Methods:**

- `OrderByOprBuilder()=default`
- `~OrderByOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## PathExpandOprBuilder

**Full name:** `gs::runtime::ops::PathExpandOprBuilder`

**Public Methods:**

- `PathExpandOprBuilder()=default`
- `~PathExpandOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## PathExpandVOprBuilder

**Full name:** `gs::runtime::ops::PathExpandVOprBuilder`

**Public Methods:**

- `PathExpandVOprBuilder()=default`
- `~PathExpandVOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## ProjectInsertOprBuilder

**Full name:** `gs::runtime::ops::ProjectInsertOprBuilder`

**Public Methods:**

- `ProjectInsertOprBuilder()=default`
- `~ProjectInsertOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## ProjectOprBuilder

**Full name:** `gs::runtime::ops::ProjectOprBuilder`

**Public Methods:**

- `ProjectOprBuilder()=default`
- `~ProjectOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## ProjectOrderByOprBuilder

**Full name:** `gs::runtime::ops::ProjectOrderByOprBuilder`

**Public Methods:**

- `ProjectOrderByOprBuilder()=default`
- `~ProjectOrderByOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## PropertyValueCollector

**Full name:** `gs::runtime::ops::PropertyValueCollector`

**Public Methods:**

- `PropertyValueCollector(const Context &ctx)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## SLVertexCollector

**Full name:** `gs::runtime::ops::SLVertexCollector`

**Public Methods:**

- `SLVertexCollector(label_t v_label)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## SLVertexWrapper

**Full name:** `gs::runtime::ops::SLVertexWrapper`

**Public Methods:**

- `SLVertexWrapper(const SLVertexColumn &column)`
- `operator()(size_t idx) const`


---

## SLVertexWrapperBeta

**Full name:** `gs::runtime::ops::SLVertexWrapperBeta`

**Public Methods:**

- `SLVertexWrapperBeta(const SLVertexColumn &column)`
- `operator()(size_t idx) const`


---

## SPOprBuilder

**Full name:** `gs::runtime::ops::SPOprBuilder`

**Public Methods:**

- `SPOprBuilder()=default`
- `~SPOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## SPOrderByLimitOprBuilder

**Full name:** `gs::runtime::ops::SPOrderByLimitOprBuilder`

**Public Methods:**

- `SPOrderByLimitOprBuilder()=default`
- `~SPOrderByLimitOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `stepping(int i) override`
- `GetOpKinds() const override`


---

## ScanOprBuilder

**Full name:** `gs::runtime::ops::ScanOprBuilder`

**Public Methods:**

- `ScanOprBuilder()=default`
- `~ScanOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## ScanUtils

**Full name:** `gs::runtime::ops::ScanUtils`

**Public Methods:**

- `parse_ids_with_type(PropertyType type, const algebra::IndexPredicate &triplet)`
- `check_idx_predicate(const physical::Scan &scan_opr, bool &scan_oid)`


---

## SelectOprBuilder

**Full name:** `gs::runtime::ops::SelectOprBuilder`

**Public Methods:**

- `SelectOprBuilder()=default`
- `~SelectOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## SetCollector

**Full name:** `gs::runtime::ops::SetCollector`

**Public Methods:**

- `SetCollector()`
- `init(size_t size)`
- `collect(std::set< T > &&val)`
- `get()`


---

## SingleValueCollector

**Full name:** `gs::runtime::ops::SingleValueCollector`

**Public Methods:**

- `SingleValueCollector()=default`
- `init(size_t size)`
- `collect(T &&val)`
- `get()`


---

## SinkInsertOprBuilder

**Full name:** `gs::runtime::ops::SinkInsertOprBuilder`

**Public Methods:**

- `SinkInsertOprBuilder()=default`
- `~SinkInsertOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## SinkOprBuilder

**Full name:** `gs::runtime::ops::SinkOprBuilder`

**Public Methods:**

- `SinkOprBuilder()=default`
- `~SinkOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## SumReducer

**Full name:** `gs::runtime::ops::SumReducer`


---

## value > >

**Full name:** `gs::runtime::ops::SumReducer< EXPR, IS_OPTIONAL, std::enable_if_t< std::is_arithmetic< typename EXPR::V >::value > >`

**Public Methods:**

- `SumReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &sum) const`


---

## TCOprBuilder

**Full name:** `gs::runtime::ops::TCOprBuilder`

**Public Methods:**

- `TCOprBuilder()=default`
- `~TCOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## ToListReducer

**Full name:** `gs::runtime::ops::ToListReducer`

**Public Methods:**

- `ToListReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &list) const`


---

## ToSetReducer

**Full name:** `gs::runtime::ops::ToSetReducer`

**Public Methods:**

- `ToSetReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## TypedKeyCollector

**Full name:** `gs::runtime::ops::TypedKeyCollector`

**Public Methods:**

- `TypedKeyCollector()`
- `init(size_t size)`
- `collect(const TypedKeyWrapper &expr, size_t idx)`
- `get()`


---

## TypedKeyWrapper

**Full name:** `gs::runtime::ops::TypedKeyCollector::TypedKeyWrapper`

**Public Methods:**

- `TypedKeyWrapper(Var &&expr)`
- `operator()(size_t idx) const`


---

## TypedVarWrapper

**Full name:** `gs::runtime::ops::TypedVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `TypedVarWrapper(Var &&vars)`


---

## UEdgeExpandOprBuilder

**Full name:** `gs::runtime::ops::UEdgeExpandOprBuilder`

**Public Methods:**

- `UEdgeExpandOprBuilder()=default`
- `~UEdgeExpandOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UGroupByOprBuilder

**Full name:** `gs::runtime::ops::UGroupByOprBuilder`

**Public Methods:**

- `UGroupByOprBuilder()=default`
- `~UGroupByOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UJoinOprBuilder

**Full name:** `gs::runtime::ops::UJoinOprBuilder`

**Public Methods:**

- `UJoinOprBuilder()=default`
- `~UJoinOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UProjectOprBuilder

**Full name:** `gs::runtime::ops::UProjectOprBuilder`

**Public Methods:**

- `UProjectOprBuilder()=default`
- `~UProjectOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UScanOprBuilder

**Full name:** `gs::runtime::ops::UScanOprBuilder`

**Public Methods:**

- `UScanOprBuilder()=default`
- `~UScanOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## USelectOprBuilder

**Full name:** `gs::runtime::ops::USelectOprBuilder`

**Public Methods:**

- `USelectOprBuilder()=default`
- `~USelectOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## USetOprBuilder

**Full name:** `gs::runtime::ops::USetOprBuilder`

**Public Methods:**

- `USetOprBuilder()=default`
- `~USetOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## USinkOprBuilder

**Full name:** `gs::runtime::ops::USinkOprBuilder`

**Public Methods:**

- `USinkOprBuilder()=default`
- `~USinkOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UVertexOprBuilder

**Full name:** `gs::runtime::ops::UVertexOprBuilder`

**Public Methods:**

- `UVertexOprBuilder()=default`
- `~UVertexOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UnfoldInsertOprBuilder

**Full name:** `gs::runtime::ops::UnfoldInsertOprBuilder`

**Public Methods:**

- `UnfoldInsertOprBuilder()=default`
- `~UnfoldInsertOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UnfoldOprBuilder

**Full name:** `gs::runtime::ops::UnfoldOprBuilder`

**Public Methods:**

- `UnfoldOprBuilder()=default`
- `~UnfoldOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## UnionOprBuilder

**Full name:** `gs::runtime::ops::UnionOprBuilder`

**Public Methods:**

- `UnionOprBuilder()=default`
- `~UnionOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## UpdateEdgeOpr

**Full name:** `gs::runtime::ops::UpdateEdgeOpr`

`UpdateEdgeOpr` is used to update edge properties in batch.

**Public Methods:**

- `UpdateEdgeOpr(edge_data_vec_t &&edge_data)`
- `get_operator_name() const override`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## UpdateEdgeOprBuilder

**Full name:** `gs::runtime::ops::UpdateEdgeOprBuilder`

**Public Methods:**

- `UpdateEdgeOprBuilder()=default`
- `~UpdateEdgeOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## UpdateVertexOpr

**Full name:** `gs::runtime::ops::UpdateVertexOpr`

`UpdateVertexOpr` is used to update vertex properties in batch.

**Public Methods:**

- `UpdateVertexOpr(vertex_prop_vec_t &&vertex_data)`
- `get_operator_name() const override`
- `eval_impl(GraphInterface &graph, const std::map< std::strin...`
- `Eval(GraphUpdateInterface &graph, const std::map< std:...`


---

## UpdateVertexOprBuilder

**Full name:** `gs::runtime::ops::UpdateVertexOprBuilder`

**Public Methods:**

- `UpdateVertexOprBuilder()=default`
- `~UpdateVertexOprBuilder()=default`
- `Build(const Schema &schema, const physical::PhysicalPla...`
- `GetOpKind() const override`


---

## ValueCollector

**Full name:** `gs::runtime::ops::ValueCollector`

**Public Methods:**

- `ValueCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## ExprWrapper

**Full name:** `gs::runtime::ops::ValueCollector::ExprWrapper`

**Public Methods:**

- `ExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## ValueWrapper

**Full name:** `gs::runtime::ops::ValueWrapper`

**Public Methods:**

- `ValueWrapper(const ValueColumn< T > &column)`
- `operator()(size_t idx) const`


---

## VarPairWrapper

**Full name:** `gs::runtime::ops::VarPairWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `VarPairWrapper(Var &&fst, Var &&snd)`


---

## VarWrapper

**Full name:** `gs::runtime::ops::VarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `VarWrapper(Var &&vars)`


---

## VertexCollector

**Full name:** `gs::runtime::ops::VertexCollector`

**Public Methods:**

- `VertexCollector()`
- `init(size_t size)`
- `collect(VertexRecord &&val)`
- `get()`


---

## VertexExprWrapper

**Full name:** `gs::runtime::ops::VertexExprWrapper`

**Public Methods:**

- `VertexExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## VertexOprBuilder

**Full name:** `gs::runtime::ops::VertexOprBuilder`

**Public Methods:**

- `VertexOprBuilder()=default`
- `~VertexOprBuilder()=default`
- `Build(const gs::Schema &schema, const ContextMeta &ctx_...`
- `GetOpKinds() const override`


---

## is_hashable

**Full name:** `gs::runtime::ops::is_hashable`


---

## declval< T >()))> >

**Full name:** `gs::runtime::ops::is_hashable< T, std::void_t< decltype(std::declval< std::hash< T > >()(std::declval< T >()))> >`


---

## pod_string_view

**Full name:** `gs::runtime::pod_string_view`

**Public Methods:**

- `pod_string_view()=default`
- `pod_string_view(const pod_string_view &other)=default`
- `pod_string_view(const char *data)`
- `pod_string_view(const char *data, size_t size)`
- `pod_string_view(const std::string &str)`
- `pod_string_view(const std::string_view &str)`
- `data() const`
- `size() const`
- `to_string() const`


---

## AlterTableEntryRecord

**Full name:** `gs::storage::AlterTableEntryRecord`

**Public Methods:**

- `AlterTableEntryRecord()`
- `AlterTableEntryRecord(const binder::BoundAlterInfo *alterInfo)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## BeginTransactionRecord

**Full name:** `gs::storage::BeginTransactionRecord`

**Public Methods:**

- `BeginTransactionRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CheckpointRecord

**Full name:** `gs::storage::CheckpointRecord`

**Public Methods:**

- `CheckpointRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## ColumnPredicate

**Full name:** `gs::storage::ColumnPredicate`

**Public Methods:**

- `ColumnPredicate(std::string columnName, common::ExpressionType ex...`
- `~ColumnPredicate()=default`
- `checkZoneMap(const MergedColumnChunkStats &stats) const =0`
- `toString()`
- `copy() const =0`
- `constCast() const`


---

## ColumnPredicateSet

**Full name:** `gs::storage::ColumnPredicateSet`

**Public Methods:**

- `ColumnPredicateSet()=default`
- `EXPLICIT_COPY_DEFAULT_MOVE(ColumnPredicateSet)`
- `addPredicate(std::unique_ptr< ColumnPredicate > predicate)`
- `tryAddPredicate(const binder::Expression &column, const binder::E...`
- `isEmpty() const`
- `checkZoneMap(const MergedColumnChunkStats &stats) const`
- `toString() const`


---

## ColumnPredicateUtil

**Full name:** `gs::storage::ColumnPredicateUtil`

**Public Methods:**

- `tryConvert(const binder::Expression &column, const binder::E...`


---

## ColumnStats

**Full name:** `gs::storage::ColumnStats`

**Public Methods:**

- `ColumnStats()=default`
- `ColumnStats(const common::LogicalType &dataType)`
- `EXPLICIT_COPY_DEFAULT_MOVE(ColumnStats)`
- `getNumDistinctValues() const`
- `update(const common::ValueVector *vector)`
- `merge(const ColumnStats &other)`
- `serialize(common::Serializer &serializer) const`
- `deserialize(common::Deserializer &deserializer)`


---

## CommitRecord

**Full name:** `gs::storage::CommitRecord`

**Public Methods:**

- `CommitRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CopyTableRecord

**Full name:** `gs::storage::CopyTableRecord`

**Public Methods:**

- `CopyTableRecord()`
- `CopyTableRecord(common::table_id_t tableID)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CreateCatalogEntryRecord

**Full name:** `gs::storage::CreateCatalogEntryRecord`

**Public Methods:**

- `CreateCatalogEntryRecord()`
- `CreateCatalogEntryRecord(catalog::CatalogEntry *catalogEntry, bool isInternal)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## DropCatalogEntryRecord

**Full name:** `gs::storage::DropCatalogEntryRecord`

**Public Methods:**

- `DropCatalogEntryRecord()`
- `DropCatalogEntryRecord(common::table_id_t entryID, catalog::CatalogEntry...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## GNodeTable

**Full name:** `gs::storage::GNodeTable`

**Public Methods:**

- `GNodeTable(const catalog::NodeTableCatalogEntry *tableEntry,...`
- `~GNodeTable() override=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override`
- `getStats(const transaction::Transaction *transaction) const override`


---

## GRelTable

**Full name:** `gs::storage::GRelTable`

**Public Methods:**

- `GRelTable(common::row_idx_t numRows, catalog::RelTableCatal...`
- `~GRelTable() override=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override`
- `getSrcTableId() const`
- `getDstTableId() const`


---

## HyperLogLog

**Full name:** `gs::storage::HyperLogLog`

**Public Methods:**

- `HyperLogLog()`
- `insertElement(common::hash_t h)`
- `update(const common::idx_t &i, const uint8_t &z)`
- `getRegister(const common::idx_t &i) const`
- `count() const`
- `merge(const HyperLogLog &other)`
- `serialize(common::Serializer &serializer) const`
- `extractCounts(uint32_t *c) const`
- `deserialize(common::Deserializer &deserializer)`
- `estimateCardinality(const uint32_t *c)`


---

## MemoryBuffer

**Full name:** `gs::storage::MemoryBuffer`

**Public Methods:**

- `MemoryBuffer(MemoryManager *mm, common::page_idx_t blockIdx, u...`
- `~MemoryBuffer()=default`
- `DELETE_COPY_AND_MOVE(MemoryBuffer)`
- `getBuffer() const`
- `getData() const`
- `getMemoryManager() const`


---

## MemoryManager

**Full name:** `gs::storage::MemoryManager`

**Public Methods:**

- `MemoryManager()=default`
- `MemoryManager(BufferManager *bm, common::VirtualFileSystem *vfs)`
- `~MemoryManager()=default`
- `allocateBuffer(bool initializeToZero=false, uint64_t size=common...`
- `getPageSize() const`
- `getBufferManager() const`


---

## NodeDeletionRecord

**Full name:** `gs::storage::NodeDeletionRecord`

**Public Methods:**

- `NodeDeletionRecord()`
- `NodeDeletionRecord(common::table_id_t tableID, common::offset_t node...`
- `NodeDeletionRecord(common::table_id_t tableID, common::offset_t node...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## NodeTable

**Full name:** `gs::storage::NodeTable`

**Public Methods:**

- `NodeTable()=default`
- `NodeTable(const StatsManager *storageManager, const catalog...`
- `NodeTable(const StatsManager *storageManager, const catalog...`
- `~NodeTable()=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override=0`
- `getStats(const transaction::Transaction *transaction) const =0`


---

## NodeUpdateRecord

**Full name:** `gs::storage::NodeUpdateRecord`

**Public Methods:**

- `NodeUpdateRecord()`
- `NodeUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `NodeUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelDeletionRecord

**Full name:** `gs::storage::RelDeletionRecord`

**Public Methods:**

- `RelDeletionRecord()`
- `RelDeletionRecord(common::table_id_t tableID, common::ValueVector *...`
- `RelDeletionRecord(common::table_id_t tableID, std::unique_ptr< comm...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelDetachDeleteRecord

**Full name:** `gs::storage::RelDetachDeleteRecord`

**Public Methods:**

- `RelDetachDeleteRecord()`
- `RelDetachDeleteRecord(common::table_id_t tableID, common::RelDataDirect...`
- `RelDetachDeleteRecord(common::table_id_t tableID, common::RelDataDirect...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelTable

**Full name:** `gs::storage::RelTable`

**Public Methods:**

- `RelTable(catalog::RelTableCatalogEntry *relTableEntry, con...`
- `RelTable(catalog::RelTableCatalogEntry *relTableEntry, con...`
- `getNumTotalRows(const transaction::Transaction *transaction) override=0`


---

## RelUpdateRecord

**Full name:** `gs::storage::RelUpdateRecord`

**Public Methods:**

- `RelUpdateRecord()`
- `RelUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `RelUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RollbackRecord

**Full name:** `gs::storage::RollbackRecord`

**Public Methods:**

- `RollbackRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## StatsManager

**Full name:** `gs::storage::StatsManager`

**Public Methods:**

- `StatsManager(MemoryManager &memoryManager)`
- `StatsManager(const std::filesystem::path &statsPath, main::Met...`
- `StatsManager(const std::string &statsData, main::MetadataManag...`
- `~StatsManager()=default`
- `getTable(common::table_id_t tableID)`
- `loadTables(const catalog::Catalog &catalog, common::VirtualF...`


---

## StorageVersionInfo

**Full name:** `gs::storage::StorageVersionInfo`

**Public Methods:**

- `getStorageVersionInfo()`
- `getStorageVersion()`


---

## Table

**Full name:** `gs::storage::Table`

**Public Methods:**

- `Table(const catalog::TableCatalogEntry *tableEntry, con...`
- `Table(const catalog::TableCatalogEntry *tableEntry, con...`
- `~Table()=default`
- `getTableType() const`
- `getTableID() const`
- `getTableName() const`
- `getNumTotalRows(const transaction::Transaction *transaction)=0`
- `cast()`
- `cast() const`
- `ptrCast()`
- ... and 1 more methods


---

## TableInsertionRecord

**Full name:** `gs::storage::TableInsertionRecord`

**Public Methods:**

- `TableInsertionRecord()`
- `TableInsertionRecord(common::table_id_t tableID, common::TableType tab...`
- `TableInsertionRecord(common::table_id_t tableID, common::TableType tab...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## TableStats

**Full name:** `gs::storage::TableStats`

**Public Methods:**

- `TableStats(std::span< const common::LogicalType > dataTypes)`
- `EXPLICIT_COPY_DEFAULT_MOVE(TableStats)`
- `incrementCardinality(common::cardinality_t increment)`
- `merge(const TableStats &other)`
- `merge(const std::vector< common::column_id_t > &columnI...`
- `getTableCard() const`
- `getNumDistinctValues(common::column_id_t columnID) const`
- `update(const std::vector< common::ValueVector * > &vecto...`
- `update(const std::vector< common::column_id_t > &columnI...`
- `addNewColumn(const common::LogicalType &dataType)`
- ... and 2 more methods


---

## UpdateSequenceRecord

**Full name:** `gs::storage::UpdateSequenceRecord`

**Public Methods:**

- `UpdateSequenceRecord()`
- `UpdateSequenceRecord(common::sequence_id_t sequenceID, uint64_t kCount)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## WAL

**Full name:** `gs::storage::WAL`

**Public Methods:**

- `WAL()`
- `WAL(const std::string &directory, bool readOnly, comm...`
- `~WAL()=default`
- `logCreateCatalogEntryRecord(catalog::CatalogEntry *catalogEntry, bool isInternal)`
- `logCreateCatalogEntryRecord(catalog::CatalogEntry *catalogEntry, std::vector<...`
- `logDropCatalogEntryRecord(uint64_t tableID, catalog::CatalogEntryType type)`
- `logAlterCatalogEntryRecord(const binder::BoundAlterInfo *alterInfo)`
- `logUpdateSequenceRecord(common::sequence_id_t sequenceID, uint64_t kCount)`
- `logTableInsertion(common::table_id_t tableID, common::TableType tab...`
- `logNodeDeletion(common::table_id_t tableID, common::offset_t node...`
- ... and 14 more methods


---

## WALRecord

**Full name:** `gs::storage::WALRecord`

**Public Methods:**

- `WALRecord()=default`
- `WALRecord(WALRecordType type)`
- `~WALRecord()=default`
- `DELETE_COPY_DEFAULT_MOVE(WALRecord)`
- `serialize(common::Serializer &serializer) const`
- `constCast() const`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## string_item

**Full name:** `gs::string_item`


---

## EmptyType >

**Full name:** `gs::to_string_impl< grape::EmptyType >`

**Public Methods:**

- `to_string(const grape::EmptyType &empty)`


---

## array< T, N > >

**Full name:** `gs::to_string_impl< std::array< T, N > >`

**Public Methods:**

- `to_string(const std::array< T, N > &empty)`


---

## array< T, N >, M > >

**Full name:** `gs::to_string_impl< std::array< std::array< T, N >, M > >`

**Public Methods:**

- `to_string(const std::array< std::array< T, N >, M > &empty)`


---

## pair< A, B > >

**Full name:** `gs::to_string_impl< std::pair< A, B > >`

**Public Methods:**

- `to_string(const std::pair< A, B > &t)`


---

## string >

**Full name:** `gs::to_string_impl< std::string >`

**Public Methods:**

- `to_string(const std::string &empty)`


---

## string_view >

**Full name:** `gs::to_string_impl< std::string_view >`

**Public Methods:**

- `to_string(const std::string_view &empty)`


---

## tuple< Args... > >

**Full name:** `gs::to_string_impl< std::tuple< Args... > >`

**Public Methods:**

- `to_string(const std::tuple< Args... > &t)`


---

## unordered_map< K, V > >

**Full name:** `gs::to_string_impl< std::unordered_map< K, V > >`

**Public Methods:**

- `to_string(const std::unordered_map< K, V > &vec)`


---

## vector< T > >

**Full name:** `gs::to_string_impl< std::vector< T > >`

**Public Methods:**

- `to_string(const std::vector< T > &vec)`


---

## LocalCacheManager

**Full name:** `gs::transaction::LocalCacheManager`

**Public Methods:**

- `contains(const std::string &key)`
- `at(const std::string &key)`
- `put(std::unique_ptr< LocalCacheObject > object)`
- `remove(const std::string &key)`


---

## LocalCacheObject

**Full name:** `gs::transaction::LocalCacheObject`

**Public Methods:**

- `LocalCacheObject(std::string key)`
- `~LocalCacheObject()=default`
- `getKey() const`
- `cast()`


---

## Transaction

**Full name:** `gs::transaction::Transaction`

**Public Methods:**

- `Transaction(main::ClientContext &clientContext, TransactionTy...`
- `Transaction(TransactionType transactionType) noexcept`
- `Transaction(TransactionType transactionType, common::transact...`
- `~Transaction()`
- `getType() const`
- `isReadOnly() const`
- `isWriteTransaction() const`
- `isDummy() const`
- `isRecovery() const`
- `getID() const`
- ... and 23 more methods


---

## TransactionActionUtils

**Full name:** `gs::transaction::TransactionActionUtils`

**Public Methods:**

- `toString(TransactionAction action)`


---

