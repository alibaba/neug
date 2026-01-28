# Namespace neug



## Classes and Structures

## APVersionManager

**Full name:** `neug::APVersionManager`

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

## AbstractPropertyGraphLoader

**Full name:** `neug::AbstractPropertyGraphLoader`

**Public Methods:**

- `AbstractPropertyGraphLoader(const std::string &work_dir, const Schema &schema...`
- `~AbstractPropertyGraphLoader()=default`
- `LoadFragment() override`


---

## AddEdgePropUndo

**Full name:** `neug::AddEdgePropUndo`

**Public Methods:**

- `AddEdgePropUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## AddEdgePropertiesRedo

**Full name:** `neug::AddEdgePropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &src_type, cons...`
- `Deserialize(OutArchive &arc, AddEdgePropertiesRedo &redo)`


---

## AddVertexPropUndo

**Full name:** `neug::AddVertexPropUndo`

**Public Methods:**

- `AddVertexPropUndo(label_t label, const std::vector< std::string > &...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## AddVertexPropertiesRedo

**Full name:** `neug::AddVertexPropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &vertex_type, c...`
- `Deserialize(OutArchive &arc, AddVertexPropertiesRedo &redo)`


---

## ArenaAllocator

**Full name:** `neug::ArenaAllocator`

**Public Methods:**

- `ArenaAllocator(MemoryStrategy strategy, const std::string &prefix)`
- `~ArenaAllocator()`
- `reserve(size_t cap)`
- `allocate(size_t size)`
- `allocated_memory() const`


---

## ArrowRecordBatchArraySupplier

**Full name:** `neug::ArrowRecordBatchArraySupplier`

A record batch supplier that provides all record batches from a vector of arrays.

Already in memory.

**Public Methods:**

- `ArrowRecordBatchArraySupplier(const std::vector< std::vector< std::shared_ptr< ...`
- `GetNextBatch() override`


---

## ArrowRecordBatchStreamSupplier

**Full name:** `neug::ArrowRecordBatchStreamSupplier`

A record batch supplier that provides all record batches from a arrow reader, which is a streaming reader or a table reader, producing record batches from a CSV file.

**Public Methods:**

- `ArrowRecordBatchStreamSupplier(const std::shared_ptr< arrow::RecordBatchReader > &reader)`
- `GetNextBatch() override`


---

## Bitset

**Full name:** `neug::Bitset`

**Public Methods:**

- `Bitset()`
- `Bitset(const Bitset &other)`
- `Bitset(Bitset &&other)`
- `~Bitset()`
- `operator=(const Bitset &other)`
- `operator=(Bitset &&other)`
- `reserve(size_t cap)`
- `clear()`
- `reset_all()`
- `resize(size_t new_size)`
- ... and 11 more methods


---

## BrpcServiceManager

**Full name:** `neug::BrpcServiceManager`

**Public Methods:**

- `BrpcServiceManager(neug::NeugDB &neug_db, SessionPool &session_pool)`
- `~BrpcServiceManager()`
- `Init(const ServiceConfig &config) override`
- `Start() override`
- `Stop() override`
- `RunAndWaitForExit() override`
- `IsRunning() const override`


---

## BrpcServiceProtocol

**Full name:** `neug::BrpcServiceProtocol`

The protocol entry for BRPC service manager.

Holding function pointers for parsing query requests and sending query responses.

**Public Methods:**

- `BrpcServiceProtocol()`
- `BrpcServiceProtocol(const BrpcServiceProtocol &other)`


---

## BrpcServiceProtocolEntry

**Full name:** `neug::BrpcServiceProtocolEntry`


---

## BrpcServiceProtocolManager

**Full name:** `neug::BrpcServiceProtocolManager`

The singleton manager for service protocols.

**Public Methods:**

- `Get()`
- `RegisterProtocol(brpc::ProtocolType type, const BrpcServiceProtoco...`
- `GetProtocol(brpc::ProtocolType type)`


---

## CSVPropertyGraphLoader

**Full name:** `neug::CSVPropertyGraphLoader`

**Public Methods:**

- `CSVPropertyGraphLoader(const std::string &work_dir, const Schema &schema...`
- `Make(const std::string &work_dir, const Schema &schema...`


---

## CSVStreamRecordBatchSupplier

**Full name:** `neug::CSVStreamRecordBatchSupplier`

**Public Methods:**

- `CSVStreamRecordBatchSupplier(const std::string &file_path, arrow::csv::Convert...`
- `GetNextBatch() override`


---

## CSVTableRecordBatchSupplier

**Full name:** `neug::CSVTableRecordBatchSupplier`

**Public Methods:**

- `CSVTableRecordBatchSupplier(const std::string &file_path, arrow::csv::Convert...`
- `GetNextBatch() override`


---

## ColumnBase

**Full name:** `neug::ColumnBase`

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

## CompactTransaction

**Full name:** `neug::CompactTransaction`

**Public Methods:**

- `CompactTransaction(PropertyGraph &graph, IWalWriter &logger, IVersio...`
- `~CompactTransaction()`
- `timestamp() const`
- `Commit()`
- `Abort()`


---

## Connection

**Full name:** `neug::Connection`

Internal connection class for executing queries against the graph database.

`Connection` provides the core interface for query execution within a NeuG database. It maintains a reference to the property graph, query planner, and query processor to handle the complete query exe...

**Public Methods:**

- `Connection(PropertyGraph &graph, std::shared_ptr< QueryProce...`
- `~Connection()`
- `Query(const std::string &query_string, const std::strin...` - Execute a query string and return the results
- `GetSchema() const` - Get the database schema
- `Close()` - Close the connection and mark it as closed
- `IsClosed() const` - Check if the connection is closed


---

## ConnectionManager

**Full name:** `neug::ConnectionManager`

**Public Methods:**

- `ConnectionManager(PropertyGraph &graph, std::shared_ptr< IGraphPlan...`
- `~ConnectionManager()`
- `CreateConnection()`
- `Close()` - Close all connections managed by the connection manager
- `RemoveConnection(std::shared_ptr< Connection > conn)` - Remove a connection from the database
- `ConnectionNum() const`


---

## Constants

**Full name:** `neug::Constants`


---

## CreateEdgeTypeRedo

**Full name:** `neug::CreateEdgeTypeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &src_type, cons...`
- `Deserialize(OutArchive &arc, CreateEdgeTypeRedo &redo)`


---

## CreateEdgeTypeUndo

**Full name:** `neug::CreateEdgeTypeUndo`

**Public Methods:**

- `CreateEdgeTypeUndo(const std::string &s, const std::string &d, const...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## CreateVertexTypeRedo

**Full name:** `neug::CreateVertexTypeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &vertex_type, c...`
- `Deserialize(OutArchive &arc, CreateVertexTypeRedo &redo)`


---

## CreateVertexTypeUndo

**Full name:** `neug::CreateVertexTypeUndo`

**Public Methods:**

- `CreateVertexTypeUndo(const std::string &vt)`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## CsrBase

**Full name:** `neug::CsrBase`

**Public Methods:**

- `CsrBase()=default`
- `~CsrBase()=default`
- `csr_type() const =0`
- `get_generic_view(timestamp_t ts) const =0`
- `unsorted_since() const`
- `size() const =0`
- `edge_num() const =0`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap)=0`
- `open_with_hugepages(const std::string &prefix, size_t v_cap=0)=0`
- ... and 13 more methods


---

## DataType

**Full name:** `neug::DataType`

**Public Methods:**

- `DataType()`
- `DataType(DataTypeId id)`
- `DataType(DataTypeId id, std::shared_ptr< ExtraTypeInfo > type_info)`
- `DataType(const DataType &other)`
- `DataType(DataType &&other) noexcept`
- `~DataType()`
- `id() const`
- `AuxInfo() const`
- `EqualTypeInfo(const DataType &rhs) const`
- `operator=(const DataType &other)`
- ... and 7 more methods


---

## Date

**Full name:** `neug::Date`

**Public Methods:**

- `Date()=default`
- `~Date()=default`
- `Date(int64_t ts)`
- `Date(int32_t num_days)`
- `Date(const std::string &date_str)`
- `to_string() const`
- `to_u32() const`
- `to_num_days() const`
- `from_num_days(int32_t num_days)`
- `from_u32(uint32_t val)`
- ... and 16 more methods


---

## DateTime

**Full name:** `neug::DateTime`

**Public Methods:**

- `DateTime()=default`
- `~DateTime()=default`
- `DateTime(int64_t x)`
- `DateTime(const std::string &date_time_str)`
- `to_string() const`
- `operator>(const DateTime &rhs) const`
- `operator<(const DateTime &rhs) const`
- `operator==(const DateTime &rhs) const`
- `operator+=(const Interval &interval)`
- `operator+(const Interval &interval) const`
- ... and 3 more methods


---

## DayValue

**Full name:** `neug::DayValue`


---

## Decoder

**Full name:** `neug::Decoder`

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

## DeleteEdgePropUndo

**Full name:** `neug::DeleteEdgePropUndo`

**Public Methods:**

- `DeleteEdgePropUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## DeleteEdgePropertiesRedo

**Full name:** `neug::DeleteEdgePropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &src_type, cons...`
- `Deserialize(OutArchive &arc, DeleteEdgePropertiesRedo &redo)`


---

## DeleteEdgeTypeRedo

**Full name:** `neug::DeleteEdgeTypeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &src_type, cons...`
- `Deserialize(OutArchive &arc, DeleteEdgeTypeRedo &redo)`


---

## DeleteEdgeTypeUndo

**Full name:** `neug::DeleteEdgeTypeUndo`

**Public Methods:**

- `DeleteEdgeTypeUndo(const std::string &s, const std::string &d, const...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## DeleteVertexPropUndo

**Full name:** `neug::DeleteVertexPropUndo`

**Public Methods:**

- `DeleteVertexPropUndo(label_t label, const std::vector< std::string > &...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## DeleteVertexPropertiesRedo

**Full name:** `neug::DeleteVertexPropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &vertex_type, c...`
- `Deserialize(OutArchive &arc, DeleteVertexPropertiesRedo &redo)`


---

## DeleteVertexTypeRedo

**Full name:** `neug::DeleteVertexTypeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &vertex_type)`
- `Deserialize(OutArchive &arc, DeleteVertexTypeRedo &redo)`


---

## DeleteVertexTypeUndo

**Full name:** `neug::DeleteVertexTypeUndo`

**Public Methods:**

- `DeleteVertexTypeUndo(const std::string &v_label)`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## DummyWalWriter

**Full name:** `neug::DummyWalWriter`

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

## EdgeDataAccessor

**Full name:** `neug::EdgeDataAccessor`

**Public Methods:**

- `EdgeDataAccessor()`
- `EdgeDataAccessor(DataTypeId data_type, ColumnBase *data_column)`
- `EdgeDataAccessor(const EdgeDataAccessor &other)`
- `is_bundled() const`
- `get_data(const NbrIterator &it) const`
- `get_typed_data(const NbrIterator &it) const`
- `get_typed_data_from_ptr(const void *data_ptr) const`
- `get_data_from_ptr(const void *data_ptr) const`
- `set_data(const NbrIterator &it, const Property &prop, time...`


---

## EdgeSchema

**Full name:** `neug::EdgeSchema`

**Public Methods:**

- `EdgeSchema()=default`
- `EdgeSchema(const std::string &src_label_name_, const std::st...`
- `is_bundled() const`
- `empty() const`
- `has_property(const std::string &prop) const`
- `add_properties(const std::vector< std::string > &names, const st...`
- `rename_properties(const std::vector< std::string > &names, const st...`
- `delete_properties(const std::vector< std::string > &names, bool is_...`
- `is_property_soft_deleted(const std::string &prop) const`
- `revert_delete_properties(const std::vector< std::string > &names)`
- ... and 3 more methods


---

## EdgeTable

**Full name:** `neug::EdgeTable`

**Public Methods:**

- `EdgeTable(std::shared_ptr< const EdgeSchema > meta)`
- `EdgeTable(EdgeTable &&edge_table)`
- `EdgeTable(const EdgeTable &)=delete`
- `~EdgeTable()=default`
- `Swap(EdgeTable &other)`
- `SetEdgeSchema(std::shared_ptr< const EdgeSchema > meta)`
- `Open(const std::string &work_dir)`
- `OpenInMemory(const std::string &work_dir, size_t src_v_cap, si...`
- `OpenWithHugepages(const std::string &work_dir, size_t src_v_cap, si...`
- `Dump(const std::string &checkpoint_dir_path)`
- ... and 22 more methods


---

## EmptyCsr

**Full name:** `neug::EmptyCsr`

**Public Methods:**

- `EmptyCsr()=default`
- `~EmptyCsr()=default`
- `csr_type() const override`
- `get_generic_view(timestamp_t ts) const override`
- `unsorted_since() const override`
- `size() const override`
- `edge_num() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 14 more methods


---

## EmptyType

**Full name:** `neug::EmptyType`

**Public Methods:**

- `operator==(const EmptyType &other) const`
- `operator!=(const EmptyType &other) const`
- `operator<(const EmptyType &other) const`


---

## Encoder

**Full name:** `neug::Encoder`

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

## ExtraTypeInfo

**Full name:** `neug::ExtraTypeInfo`

**Public Methods:**

- `ExtraTypeInfo(ExtraTypeInfoType type)`
- `~ExtraTypeInfo()`
- `Equals(ExtraTypeInfo *other_p) const`
- `Cast()`
- `Cast() const`


---

## FileLock

**Full name:** `neug::FileLock`

**Public Methods:**

- `FileLock(const std::string &data_dir)`
- `~FileLock()`
- `lock(std::string &error_msg, DBMode mode)`
- `unlock()`
- `CleanupAllLocks()`


---

## GHash

**Full name:** `neug::GHash`

**Public Methods:**

- `operator()(const T &val) const`


---

## GHash< Property >

**Full name:** `neug::GHash< Property >`

**Public Methods:**

- `operator()(const Property &val) const`


---

## GHash< int64_t >

**Full name:** `neug::GHash< int64_t >`

**Public Methods:**

- `operator()(const int64_t &val) const`


---

## GOptPlanner

**Full name:** `neug::GOptPlanner`

`GOptPlanner` is an implementation of `IGraphPlanner` that uses the GOpt optimization framework to compile Cypher queries into executable physical plans.

**Public Methods:**

- `GOptPlanner()`
- `type() const override`
- `compilePlan(const std::string &query) override` - Generate the executable plan
- `update_meta(const YAML::Node &schema_yaml_node) override` - Update the metadata of the graph
- `update_statistics(const std::string &graph_statistic_json) override` - Update the statistics of the graph
- `analyzeMode(const std::string &query) const override`


---

## GTypeUtils

**Full name:** `neug::GTypeUtils`

**Public Methods:**

- `createLogicalType(YAML::Node &node)`
- `toYAML(const neug::common::LogicalType &type)`


---

## GenericView

**Full name:** `neug::GenericView`

**Public Methods:**

- `GenericView()`
- `GenericView(const char *adjlists, const int *degrees, NbrIter...`
- `GenericView(const char *adjlists, NbrIterConfig cfg, timestam...`
- `type() const`
- `__attribute__((always_inline)) NbrList get_edges(vid_t v) const`
- `get_typed_view() const`


---

## GlobalId

**Full name:** `neug::GlobalId`

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

## HttpServiceImpl

**Full name:** `neug::HttpServiceImpl`

**Public Methods:**

- `HttpServiceImpl(neug::NeugDB &neug_db, SessionPool &session_pool)`
- `~HttpServiceImpl()`
- `PostCypherQuery(google::protobuf::RpcController *cntl_base, const...`
- `GetSchema(google::protobuf::RpcController *cntl_base, const...`
- `GetServiceStatus(google::protobuf::RpcController *cntl_base, const...`


---

## IFragmentLoader

**Full name:** `neug::IFragmentLoader`

**Public Methods:**

- `~IFragmentLoader()=default`
- `LoadFragment()=0`


---

## IGraphPlanner

**Full name:** `neug::IGraphPlanner`

Graph planner interface.

Receive the cypher query, and generate the executable plan.

**Public Methods:**

- `IGraphPlanner()`
- `type() const =0`
- `~IGraphPlanner()=default`
- `compilePlan(const std::string &query)=0` - Generate the executable plan
- `update_meta(const YAML::Node &schema_yaml_node)=0` - Update the metadata of the graph
- `update_statistics(const std::string &graph_statistic_json)=0` - Update the statistics of the graph
- `analyzeMode(const std::string &query) const =0`


---

## IRecordBatchSupplier

**Full name:** `neug::IRecordBatchSupplier`

**Public Methods:**

- `~IRecordBatchSupplier()=default`
- `GetNextBatch()=0`


---

## IServiceManager

**Full name:** `neug::IServiceManager`

**Public Methods:**

- `~IServiceManager()=default`
- `Init(const ServiceConfig &config)=0`
- `Start()=0`
- `Stop()=0`
- `RunAndWaitForExit()=0`
- `IsRunning() const =0`


---

## IStorageInterface

**Full name:** `neug::IStorageInterface`

**Public Methods:**

- `~IStorageInterface()`
- `readable() const =0`
- `writable() const =0`
- `schema() const =0`
- `GetVertexIndex(label_t label, const Property &id, vid_t &index) const =0`


---

## IUndoLog

**Full name:** `neug::IUndoLog`

**Public Methods:**

- `~IUndoLog()=default`
- `GetType() const =0`
- `Undo(PropertyGraph &graph, timestamp_t ts) const =0`


---

## IVersionManager

**Full name:** `neug::IVersionManager`

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

**Full name:** `neug::IWalParser`

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

**Full name:** `neug::IWalWriter`

The interface of wal writer.

**Public Methods:**

- `~IWalWriter()`
- `type() const =0`
- `open()=0` - Open a wal file
- `close()=0` - Close the wal writer
- `append(const char *data, size_t length)=0` - Append data to the wal file


---

## IdIndexer

**Full name:** `neug::IdIndexer`

**Public Methods:**

- `IdIndexer()`
- `~IdIndexer()`
- `get_type() const override`
- `_add(const Property &oid) override`
- `add(const Property &oid, INDEX_T &lid) override`
- `get_key(const INDEX_T &lid, Property &oid) const override`
- `get_index(const Property &oid, INDEX_T &lid) const override`
- `Clear()`
- `entry_num() const`
- `add(const KEY_T &oid, INDEX_T &lid)`
- ... and 19 more methods


---

## IdIndexerBase

**Full name:** `neug::IdIndexerBase`

**Public Methods:**

- `IdIndexerBase()=default`
- `~IdIndexerBase()=default`
- `get_type() const =0`
- `_add(const Property &oid)=0`
- `add(const Property &oid, INDEX_T &lid)=0`
- `get_key(const INDEX_T &lid, Property &oid) const =0`
- `get_index(const Property &oid, INDEX_T &lid) const =0`
- `size() const =0`


---

## ImmutableCsr

**Full name:** `neug::ImmutableCsr`

**Public Methods:**

- `ImmutableCsr()`
- `~ImmutableCsr()`
- `csr_type() const override`
- `get_generic_view(timestamp_t ts) const override`
- `unsorted_since() const override`
- `size() const override`
- `edge_num() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 13 more methods


---

## ImmutableNbr

**Full name:** `neug::ImmutableNbr`

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

## ImmutableNbr< EmptyType >

**Full name:** `neug::ImmutableNbr< EmptyType >`

**Public Methods:**

- `ImmutableNbr()=default`
- `ImmutableNbr(const ImmutableNbr &rhs)`
- `~ImmutableNbr()=default`
- `operator=(const ImmutableNbr &rhs)`
- `set_data(const EmptyType &)`
- `set_neighbor(vid_t neighbor)`
- `get_data() const`
- `get_neighbor() const`
- `exists() const`


---

## InArchive

**Full name:** `neug::InArchive`

**Public Methods:**

- `InArchive()=default`
- `InArchive(InArchive &&rhs)`
- `~InArchive()=default`
- `operator=(InArchive &&rhs)`
- `Reset()`
- `GetBuffer()`
- `GetBuffer() const`
- `GetSize() const`
- `AddBytes(const void *data, size_t size)`
- `Resize(size_t size)`
- ... and 3 more methods


---

## InplaceTopNGenerator

**Full name:** `neug::InplaceTopNGenerator`

**Public Methods:**

- `InplaceTopNGenerator(size_t n)`
- `generate_indices(const std::vector< T > &input, std::vector< size_...`


---

## InsertEdgeRedo

**Full name:** `neug::InsertEdgeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t src_label, const Property...`
- `Deserialize(OutArchive &arc, InsertEdgeRedo &redo)`


---

## InsertEdgeUndo

**Full name:** `neug::InsertEdgeUndo`

**Public Methods:**

- `InsertEdgeUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## InsertTransaction

**Full name:** `neug::InsertTransaction`

Transaction for inserting new vertices and edges into the graph.

`InsertTransaction` handles the insertion of new graph elements with ACID guarantees. It maintains a Write-Ahead Log (WAL) for durability and tracks added vertices to resolve edge insertions that refe...

**Public Methods:**

- `InsertTransaction(PropertyGraph &graph, Allocator &alloc, IWalWrite...` - Construct an `InsertTransaction`
- `~InsertTransaction()` - Destructor that calls `Abort()`
- `AddVertex(label_t label, const Property &id, const std::vec...` - Add a new vertex to the transaction
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...` - Add a new edge to the transaction
- `Commit()` - Commit the transaction
- `Abort()`
- `timestamp() const`
- `schema() const`
- `GetVertexIndex(label_t label, const Property &oid, vid_t &lid) const`
- `GetVertexId(label_t label, vid_t lid) const`
- ... and 2 more methods


---

## InsertVertexRedo

**Full name:** `neug::InsertVertexRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t label, const Property &oi...`
- `Deserialize(OutArchive &arc, InsertVertexRedo &redo)`


---

## InsertVertexUndo

**Full name:** `neug::InsertVertexUndo`

**Public Methods:**

- `InsertVertexUndo(label_t v_label, vid_t vid)`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## Interval

**Full name:** `neug::Interval`

**Public Methods:**

- `Interval()=default`
- `~Interval()=default`
- `Interval(std::string str)`
- `Interval(std::string_view str_view)`
- `normalize(int64_t &months, int64_t &days, int64_t &micros) const`
- `normalize() const`
- `operator=(const Interval &rhs)=default`
- `from_mill_seconds(int64_t mill_seconds)`
- `invert()`
- `to_mill_seconds() const`
- ... and 14 more methods


---

## LDBCLongDateParser

**Full name:** `neug::LDBCLongDateParser`

**Public Methods:**

- `LDBCLongDateParser()=default`
- `~LDBCLongDateParser() override`
- `operator()(const char *s, size_t length, arrow::TimeUnit::ty...`
- `kind() const override`
- `format() const override`


---

## LDBCTimeStampParser

**Full name:** `neug::LDBCTimeStampParser`

**Public Methods:**

- `LDBCTimeStampParser()=default`
- `~LDBCTimeStampParser() override`
- `operator()(const char *s, size_t length, arrow::TimeUnit::ty...`
- `kind() const override`
- `format() const override`


---

## LFIndexer

**Full name:** `neug::LFIndexer`

**Public Methods:**

- `LFIndexer()`
- `LFIndexer(LFIndexer &&rhs)`
- `~LFIndexer()`
- `swap(LFIndexer &other)`
- `init(const DataTypeId &type, std::shared_ptr< ExtraTyp...`
- `build_empty_LFIndexer(const std::string &filename, const std::string &s...`
- `reserve(size_t size)`
- `rehash(size_t size)`
- `capacity() const`
- `size() const`
- ... and 20 more methods


---

## ListType

**Full name:** `neug::ListType`

**Public Methods:**

- `GetChildType(const DataType &type)`


---

## ListTypeInfo

**Full name:** `neug::ListTypeInfo`

**Public Methods:**

- `ListTypeInfo(DataType child_type_p)`


---

## LoaderFactory

**Full name:** `neug::LoaderFactory`

`LoaderFactory` is a factory class to create `IFragmentLoader`.

Support Using dynamically built library as plugin.

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `Register(const std::string &scheme_type, const std::string...`


---

## LoadingConfig

**Full name:** `neug::LoadingConfig`

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

**Full name:** `neug::LocalWalParser`

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

**Full name:** `neug::LocalWalWriter`

**Public Methods:**

- `Make(const std::string &wal_uri, int thread_id)`
- `LocalWalWriter(const std::string &wal_uri, int thread_id)`
- `~LocalWalWriter()`
- `open() override` - Open a wal file
- `close() override` - Close the wal writer
- `append(const char *data, size_t length) override` - Append data to the wal file
- `type() const override`


---

## MutableCsr

**Full name:** `neug::MutableCsr`

**Public Methods:**

- `MutableCsr()`
- `~MutableCsr()`
- `csr_type() const override`
- `get_generic_view(timestamp_t ts) const override`
- `unsorted_since() const override`
- `size() const override`
- `edge_num() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 14 more methods


---

## MutableNbr

**Full name:** `neug::MutableNbr`

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

## MutableNbr< EmptyType >

**Full name:** `neug::MutableNbr< EmptyType >`

**Public Methods:**

- `MutableNbr()`
- `MutableNbr(const MutableNbr &rhs)`
- `~MutableNbr()=default`
- `operator=(const MutableNbr &rhs)`
- `set_data(const EmptyType &, timestamp_t ts)`
- `set_neighbor(vid_t neighbor)`
- `set_timestamp(timestamp_t ts)`
- `get_data() const`
- `get_neighbor() const`
- `get_timestamp() const`


---

## NbrIterConfig

**Full name:** `neug::NbrIterConfig`


---

## NbrIterator

**Full name:** `neug::NbrIterator`

**Public Methods:**

- `NbrIterator()=default`
- `~NbrIterator()=default`
- `init(const void *ptr, const void *end, NbrIterConfig c...`
- `operator*() const`
- `__attribute__((always_inline)) NbrIterator &operator++()`
- `__attribute__((always_inline)) NbrIterator &operator+`


---

## NbrList

**Full name:** `neug::NbrList`

**Public Methods:**

- `NbrList()=default`
- `~NbrList()=default`
- `__attribute__((always_inline)) NbrIterator begin() const`
- `__attribute__((always_inline)) NbrIterator end() const`
- `empty() const`


---

## NeugDB

**Full name:** `neug::NeugDB`

Core database engine for NeuG graph database system.

`NeugDB` serves as the main entry point and central management system for the NeuG graph database. It coordinates all major components including storage, transactions, query processing, and applicatio...

**Public Methods:**

- `NeugDB()`
- `~NeugDB()`
- `Open(const std::string &data_dir, int32_t max_num_thre...` - Load the graph from data directory
- `Open(const NeugDBConfig &config)` - Load the graph from data directory
- `Close()` - Close the current opened graph
- `IsClosed() const` - Check if the database is closed
- `Connect()` - Open a connection to the database
- `RemoveConnection(std::shared_ptr< Connection > conn)` - Remove a connection from the database
- `graph() const`
- `graph()`
- ... and 5 more methods


---

## NeugDBService

**Full name:** `neug::NeugDBService`

NeuG database HTTP service wrapper.

`NeugDBService` provides an HTTP interface layer for the NeuG graph database. It manages the lifecycle of a BRPC-based HTTP server that handles Cypher queries, service status requests, and schema quer...

**Public Methods:**

- `NeugDBService(neug::NeugDB &db, const ServiceConfig &config=Ser...` - Constructs a service around an existing database instance
- `db()` - Gets direct access to the underlying graph database
- `~NeugDBService()` - Destructor that ensures proper cleanup
- `Start()` - Starts the HTTP server
- `Stop()` - Stops the HTTP server gracefully
- `GetServiceConfig() const` - Retrieves the current service configuration
- `AcquireSession()`
- `IsInitialized() const` - Checks if the service has been initialized
- `IsRunning() const` - Checks if the HTTP server is currently running
- `service_status()` - Gets current service status information
- ... and 3 more methods


---

## NeugDBSession

**Full name:** `neug::NeugDBSession`

Database session for executing queries and managing transactions.

`NeugDBSession` provides a session-based interface for interacting with the NeuG database. Each session maintains its own transaction context and application state, enabling concurrent access while en...

**Public Methods:**

- `NeugDBSession(neug::PropertyGraph &graph, std::shared_ptr< neug...`
- `~NeugDBSession()`
- `GetReadTransaction() const`
- `GetInsertTransaction()`
- `GetUpdateTransaction()`
- `GetCompactTransaction()`
- `graph() const`
- `graph()`
- `schema() const`
- `Eval(const std::string &query)` - Execute a Cypher query within the session
- ... and 3 more methods


---

## ODPSFragmentLoader

**Full name:** `neug::ODPSFragmentLoader`

**Public Methods:**

- `ODPSFragmentLoader(const std::string &work_dir, const Schema &schema...`
- `~ODPSFragmentLoader()`
- `Make(const std::string &work_dir, const Schema &schema...`


---

## ODPSReadClient

**Full name:** `neug::ODPSReadClient`

**Public Methods:**

- `ODPSReadClient()`
- `~ODPSReadClient()`
- `init()`
- `CreateReadSession(std::string *session_id, int *split_count, const ...`
- `ReadTable(const std::string &session_id, int split_count, c...`
- `GetArrowClient() const`


---

## ODPSStreamRecordBatchSupplier

**Full name:** `neug::ODPSStreamRecordBatchSupplier`

**Public Methods:**

- `ODPSStreamRecordBatchSupplier(label_t label_id, const std::string &file_path, c...`
- `GetNextBatch() override`


---

## ODPSTableRecordBatchSupplier

**Full name:** `neug::ODPSTableRecordBatchSupplier`

**Public Methods:**

- `ODPSTableRecordBatchSupplier(label_t label_id, const std::string &file_path, c...`
- `GetNextBatch() override`


---

## OPPrintInfo

**Full name:** `neug::OPPrintInfo`

**Public Methods:**

- `OPPrintInfo()`
- `~OPPrintInfo()=default`
- `toString() const`
- `copy() const`


---

## OutArchive

**Full name:** `neug::OutArchive`

**Public Methods:**

- `OutArchive()`
- `OutArchive(size_t size)`
- `OutArchive(OutArchive &&oa)`
- `~OutArchive()`
- `operator=(OutArchive &&oa)`
- `Clear()`
- `Allocate(size_t size)`
- `Rewind()`
- `SetSlice(char *buffer, size_t size)`
- `GetBuffer()`
- ... and 5 more methods


---

## PropUtils

**Full name:** `neug::PropUtils`

**Public Methods:**

- `to_prop(const T &v)`
- `to_typed(const Property &prop)`


---

## PropUtils< Date >

**Full name:** `neug::PropUtils< Date >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const Date &v)`
- `to_prop(int32_t num_days)`


---

## PropUtils< DateTime >

**Full name:** `neug::PropUtils< DateTime >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const DateTime &v)`
- `to_prop(int64_t mill_seconds)`


---

## PropUtils< EmptyType >

**Full name:** `neug::PropUtils< EmptyType >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const EmptyType &v)`


---

## PropUtils< Interval >

**Full name:** `neug::PropUtils< Interval >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const Interval &v)`
- `to_prop(const std::string_view &str)`


---

## PropUtils< bool >

**Full name:** `neug::PropUtils< bool >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(bool v)`


---

## PropUtils< double >

**Full name:** `neug::PropUtils< double >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(double v)`


---

## PropUtils< float >

**Full name:** `neug::PropUtils< float >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(float v)`


---

## PropUtils< int32_t >

**Full name:** `neug::PropUtils< int32_t >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(int32_t v)`


---

## PropUtils< int64_t >

**Full name:** `neug::PropUtils< int64_t >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(int64_t v)`


---

## string >

**Full name:** `neug::PropUtils< std::string >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const std::string &v)`


---

## string_view >

**Full name:** `neug::PropUtils< std::string_view >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(const std::string_view &v)`


---

## PropUtils< uint32_t >

**Full name:** `neug::PropUtils< uint32_t >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(uint32_t v)`


---

## PropUtils< uint64_t >

**Full name:** `neug::PropUtils< uint64_t >`

**Public Methods:**

- `prop_type()`
- `to_typed(const Property &prop)`
- `to_prop(uint64_t v)`


---

## Property

**Full name:** `neug::Property`

**Public Methods:**

- `Property()`
- `~Property()=default`
- `set_bool(bool v)`
- `set_int32(int32_t v)`
- `set_uint32(uint32_t v)`
- `set_int64(int64_t v)`
- `set_uint64(uint64_t v)`
- `set_string_view(const std::string_view &v)`
- `set_float(float v)`
- `set_double(double v)`
- ... and 34 more methods


---

## PropertyGraph

**Full name:** `neug::PropertyGraph`

Core property graph storage engine managing vertices, edges, and schema.

`PropertyGraph` provides the fundamental storage layer for graph data in NeuG. It manages vertex and edge tables, schema information, and provides persistence through file-based storage with memory ma...

**Public Methods:**

- `PropertyGraph()` - Construct `PropertyGraph` with default settings
- `~PropertyGraph()` - Destructor that reserves space and cleans up resources
- `Open(const std::string &work_dir, int memory_level)` - Open the property graph from persistent storage
- `Open(const Schema &schema, const std::string &work_dir...`
- `Compact(bool compact_csr, float reserve_ratio, timestamp_t ts)`
- `Dump(bool reopen=true)`
- `DumpSchema()` - Dump schema information to a file
- `schema() const` - Get read-only access to the schema
- `mutable_schema()` - Get mutable access to the schema
- `Clear()` - Clear all graph data and reset to empty state
- ... and 48 more methods


---

## QueryProcessor

**Full name:** `neug::QueryProcessor`

**Public Methods:**

- `QueryProcessor(PropertyGraph &graph, std::shared_ptr< IGraphPlan...`
- `execute(const std::string &query_string, const std::strin...`


---

## QueryResult

**Full name:** `neug::QueryResult`

Container for query execution results with iterator-based access.

`QueryResult` wraps a CollectiveResults protobuf message and provides convenient C++ iterator-style access to the result records. It maintains an internal cursor for sequential access via `hasNext()`/...

**Public Methods:**

- `From(results::CollectiveResults &&result)` - Create `QueryResult` from CollectiveResults (move semantics)
- `From(const std::string &result_str)` - Create `QueryResult` by deserializing from a string
- `QueryResult()=default` - Default constructor creating empty result
- `QueryResult(results::CollectiveResults &&res)` - Construct `QueryResult` from CollectiveResults
- `~QueryResult()`
- `hasNext() const` - Check if there are more records to iterate
- `next()` - Get the next result record and advance iterator
- `operator[](int index) const` - Get a record by index (random access)
- `length() const` - Get total number of records in the result set
- `ToString() const`
- ... and 2 more methods


---

## ReadTransaction

**Full name:** `neug::ReadTransaction`

Read-only transaction for consistent snapshot access to graph data.

`ReadTransaction` provides read access to graph data at a specific timestamp, implementing snapshot isolation. It stores references to the session, graph, version manager, and the snapshot timestamp.
...

**Public Methods:**

- `ReadTransaction(const PropertyGraph &graph, IVersionManager &vm, ...` - Construct a `ReadTransaction`
- `~ReadTransaction()` - Destructor that calls release()
- `timestamp() const`
- `Commit()`
- `Abort()`
- `graph() const`


---

## RecordLine

**Full name:** `neug::RecordLine`

We use this class to represent the returned result of the query.

No extra memory should

**Public Methods:**

- `RecordLine()=default`
- `RecordLine(std::vector< const results::Entry * > entries)`
- `ToString() const`
- `entries() const`


---

## RefColumnBase

**Full name:** `neug::RefColumnBase`

Create RefColumn for ease of usage for hqps.

**Public Methods:**

- `~RefColumnBase()`
- `get(size_t index) const =0`
- `type() const =0`
- `col_type() const =0`


---

## RemoteStorageDownloader

**Full name:** `neug::RemoteStorageDownloader`

**Public Methods:**

- `~RemoteStorageDownloader()=default`
- `Open()=0` - Open a remote storage for reading
- `Get(const std::string &remote_path, const std::string...` - Get a file from the remote storage
- `List(const std::string &remote_path, std::vector< std:...` - List all files in the remote storage
- `Close()=0` - Close the remote storage


---

## RemoteStorageUploader

**Full name:** `neug::RemoteStorageUploader`

**Public Methods:**

- `~RemoteStorageUploader()=default`
- `Open()=0` - Open a remote storage for writing
- `Put(const std::string &local_path, const std::string ...` - Put a local file to the remote storage
- `Delete(const std::string &remote_path)=0` - Delete a file from the remote storage
- `Close()=0` - Close the remote storage


---

## RemoveEdgeRedo

**Full name:** `neug::RemoveEdgeRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t src_label, const Property...`
- `Deserialize(OutArchive &arc, RemoveEdgeRedo &redo)`


---

## RemoveEdgeUndo

**Full name:** `neug::RemoveEdgeUndo`

**Public Methods:**

- `RemoveEdgeUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## RemoveVertexRedo

**Full name:** `neug::RemoveVertexRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t label, const Property &oid)`
- `Deserialize(OutArchive &arc, RemoveVertexRedo &redo)`


---

## RemoveVertexUndo

**Full name:** `neug::RemoveVertexUndo`

**Public Methods:**

- `RemoveVertexUndo(label_t v_label, vid_t lid, const std::unordered_...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## RenameEdgePropUndo

**Full name:** `neug::RenameEdgePropUndo`

**Public Methods:**

- `RenameEdgePropUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## RenameEdgePropertiesRedo

**Full name:** `neug::RenameEdgePropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &src_type, cons...`
- `Deserialize(OutArchive &arc, RenameEdgePropertiesRedo &redo)`


---

## RenameVertexPropUndo

**Full name:** `neug::RenameVertexPropUndo`

**Public Methods:**

- `RenameVertexPropUndo(label_t label, const std::vector< std::pair< std:...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## RenameVertexPropertiesRedo

**Full name:** `neug::RenameVertexPropertiesRedo`

**Public Methods:**

- `Serialize(InArchive &arc, const std::string &vertex_type, c...`
- `Deserialize(OutArchive &arc, RenameVertexPropertiesRedo &redo)`


---

## Schema

**Full name:** `neug::Schema`

**Public Methods:**

- `Schema()`
- `~Schema()`
- `Clear()`
- `Empty() const`
- `get_vertex_schema(label_t label) const`
- `get_vertex_schema(label_t label)`
- `get_edge_schema(label_t src_label, label_t dst_label, label_t edg...`
- `get_edge_schema(label_t src_label, label_t dst_label, label_t edge_label)`
- `AddVertexLabel(const std::string &label, const std::vector< Data...`
- `AddEdgeLabel(const std::string &src_label, const std::string &...`
- ... and 95 more methods


---

## ServiceConfig

**Full name:** `neug::ServiceConfig`

**Public Methods:**

- `ServiceConfig()`


---

## SessionGuard

**Full name:** `neug::SessionGuard`

**Public Methods:**

- `SessionGuard()`
- `SessionGuard(NeugDBSession *session, SessionPool *pool, size_t...`
- `~SessionGuard()`
- `SessionGuard(SessionGuard &&other) noexcept`
- `SessionGuard(const SessionGuard &)=delete`
- `operator=(const SessionGuard &)=delete`
- `get() const`
- `operator->() const`
- `operator bool() const`


---

## SessionLocalContext

**Full name:** `neug::SessionLocalContext`

**Public Methods:**

- `SessionLocalContext(neug::PropertyGraph &graph_, std::shared_ptr< neu...`
- `~SessionLocalContext()`


---

## SessionPool

**Full name:** `neug::SessionPool`

**Public Methods:**

- `SessionPool(neug::PropertyGraph &graph, std::shared_ptr< neug...`
- `~SessionPool()`
- `AcquireSession()` - Acquire a session from the pool
- `SessionNum() const`
- `getExecutedQueryNum() const` - Get the total number of executed queries across all sessions


---

## SingleImmutableCsr

**Full name:** `neug::SingleImmutableCsr`

**Public Methods:**

- `SingleImmutableCsr()`
- `~SingleImmutableCsr()`
- `csr_type() const override`
- `get_generic_view(timestamp_t ts) const override`
- `unsorted_since() const override`
- `size() const override`
- `edge_num() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 13 more methods


---

## SingleMutableCsr

**Full name:** `neug::SingleMutableCsr`

**Public Methods:**

- `SingleMutableCsr()`
- `~SingleMutableCsr()`
- `csr_type() const override`
- `get_generic_view(timestamp_t ts) const override`
- `unsorted_since() const override`
- `size() const override`
- `edge_num() const override`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &prefix, size_t v_cap) override`
- `open_with_hugepages(const std::string &prefix, size_t v_cap) override`
- ... and 14 more methods


---

## SpinLock

**Full name:** `neug::SpinLock`

A simple implementation of spinlock based on `std::atomic`.

**Public Methods:**

- `lock()`
- `unlock()`


---

## Status

**Full name:** `neug::Status`

**Public Methods:**

- `Status() noexcept`
- `Status(StatusCode error_code) noexcept`
- `Status(StatusCode error_code, std::string &&error_msg) noexcept`
- `Status(StatusCode error_code, const std::string &error_m...`
- `ok() const`
- `error_message() const`
- `error_code() const`
- `operator bool() const`
- `ToString() const`
- `OK()`
- ... and 3 more methods


---

## StorageAPUpdateInterface

**Full name:** `neug::StorageAPUpdateInterface`

**Public Methods:**

- `StorageAPUpdateInterface(PropertyGraph &graph, timestamp_t timestamp, neug...`
- `~StorageAPUpdateInterface()`
- `UpdateVertexProperty(label_t label, vid_t lid, int col_id, const Prope...`
- `UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label, ...`
- `AddVertex(label_t label, const Property &id, const std::vec...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `CreateCheckpoint() override`
- `BatchAddVertices(label_t v_label_id, std::shared_ptr< IRecordBatch...`
- `BatchAddEdges(label_t src_label, label_t dst_label, label_t edg...`
- `BatchDeleteVertices(label_t v_label_id, const std::vector< vid_t > &v...`
- ... and 12 more methods


---

## StorageInsertInterface

**Full name:** `neug::StorageInsertInterface`

**Public Methods:**

- `StorageInsertInterface()`
- `~StorageInsertInterface()`
- `readable() const override`
- `writable() const override`
- `AddVertex(label_t label, const Property &id, const std::vec...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `BatchAddVertices(label_t v_label_id, std::shared_ptr< IRecordBatch...`
- `BatchAddEdges(label_t src_label, label_t dst_label, label_t edg...`


---

## StorageReadInterface

**Full name:** `neug::StorageReadInterface`

**Public Methods:**

- `StorageReadInterface(const PropertyGraph &graph, timestamp_t read_ts)`
- `~StorageReadInterface()`
- `readable() const override`
- `writable() const override`
- `GetVertexPropColumn(label_t label, const std::string &prop_name) const`
- `GetVertexSet(label_t label) const`
- `GetVertexIndex(label_t label, const Property &id, vid_t &index) ...`
- `IsValidVertex(label_t label, vid_t index) const`
- `GetVertexId(label_t label, vid_t index) const`
- `GetVertexProperty(label_t label, vid_t index, int prop_id) const`
- ... and 5 more methods


---

## StorageTPInsertInterface

**Full name:** `neug::StorageTPInsertInterface`

**Public Methods:**

- `StorageTPInsertInterface(InsertTransaction &txn)`
- `~StorageTPInsertInterface()`
- `AddVertex(label_t label, const Property &id, const std::vec...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `schema() const override`
- `GetVertexIndex(label_t label, const Property &id, vid_t &index) ...`
- `BatchAddVertices(label_t v_label_id, std::shared_ptr< IRecordBatch...`
- `BatchAddEdges(label_t src_label, label_t dst_label, label_t edg...`


---

## StorageTPUpdateInterface

**Full name:** `neug::StorageTPUpdateInterface`

**Public Methods:**

- `StorageTPUpdateInterface(UpdateTransaction &txn)`
- `~StorageTPUpdateInterface()`
- `UpdateVertexProperty(label_t label, vid_t lid, int col_id, const Prope...`
- `UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label, ...`
- `AddVertex(label_t label, const Property &id, const std::vec...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `CreateCheckpoint() override`
- `GetTransaction()`
- `BatchAddVertices(label_t v_label_id, std::shared_ptr< IRecordBatch...`
- `BatchAddEdges(label_t src_label, label_t dst_label, label_t edg...`
- ... and 13 more methods


---

## StorageUpdateInterface

**Full name:** `neug::StorageUpdateInterface`

**Public Methods:**

- `StorageUpdateInterface(const neug::PropertyGraph &graph, timestamp_t read_ts)`
- `~StorageUpdateInterface()`
- `readable() const override`
- `writable() const override`
- `UpdateVertexProperty(label_t label, vid_t lid, int col_id, const Prope...`
- `UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label, ...`
- `AddVertex(label_t label, const Property &id, const std::vec...`
- `AddEdge(label_t src_label, vid_t src, label_t dst_label, ...`
- `BatchDeleteVertices(label_t v_label_id, const std::vector< vid_t > &vids)=0`
- `BatchDeleteEdges(label_t src_v_label_id, label_t dst_v_label_id, l...`
- ... and 12 more methods


---

## StringTypeInfo

**Full name:** `neug::StringTypeInfo`

**Public Methods:**

- `StringTypeInfo(size_t length)`


---

## StringViewVector

**Full name:** `neug::StringViewVector`

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

## StructType

**Full name:** `neug::StructType`

**Public Methods:**

- `GetChildTypes(const DataType &type)`
- `GetChildType(const DataType &type, size_t index)`


---

## StructTypeInfo

**Full name:** `neug::StructTypeInfo`

**Public Methods:**

- `StructTypeInfo(std::vector< DataType > child_types_p)`


---

## SupplierWrapperWithFirstBatch

**Full name:** `neug::SupplierWrapperWithFirstBatch`

**Public Methods:**

- `SupplierWrapperWithFirstBatch(const std::vector< std::shared_ptr< IRecordBatchS...`
- `SupplierWrapperWithFirstBatch(const std::vector< std::shared_ptr< IRecordBatchS...`
- `GetNextBatch() override`


---

## TPVersionManager

**Full name:** `neug::TPVersionManager`

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

**Full name:** `neug::Table`

**Public Methods:**

- `Table()`
- `~Table()`
- `init(const std::string &name, const std::string &work_...`
- `open(const std::string &name, const std::string &work_...`
- `open_in_memory(const std::string &name, const std::string &work_...`
- `open_with_hugepages(const std::string &name, const std::string &work_...`
- `copy_to_tmp(const std::string &name, const std::string &snaps...`
- `dump(const std::string &name, const std::string &snapshot_dir)`
- `reset_header(const std::vector< std::string > &col_name)`
- `add_column(const std::string &col_name, const DataTypeId &co...`
- ... and 26 more methods


---

## TopNAscCmp

**Full name:** `neug::TopNAscCmp`

**Public Methods:**

- `operator()(const elem_t &lhs, const elem_t &rhs) const`
- `operator()(const T &lhs, const T &rhs) const`


---

## TopNDescCmp

**Full name:** `neug::TopNDescCmp`

**Public Methods:**

- `operator()(const elem_t &lhs, const elem_t &rhs) const`
- `operator()(const T &lhs, const T &rhs) const`


---

## TopNGenerator

**Full name:** `neug::TopNGenerator`

**Public Methods:**

- `TopNGenerator(size_t n)`
- `push(const T &val, size_t idx)`
- `generate_indices(std::vector< size_t > &indices)`
- `generate_pairs(std::vector< T > &values, std::vector< size_t > &indices)`


---

## TopNUnit

**Full name:** `neug::TopNUnit`

**Public Methods:**

- `TopNUnit(const T &val_, size_t idx_)`


---

## TypeConverter

**Full name:** `neug::TypeConverter`


---

## TypeConverter< Date >

**Full name:** `neug::TypeConverter< Date >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< DateTime >

**Full name:** `neug::TypeConverter< DateTime >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< Interval >

**Full name:** `neug::TypeConverter< Interval >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< bool >

**Full name:** `neug::TypeConverter< bool >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< double >

**Full name:** `neug::TypeConverter< double >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< float >

**Full name:** `neug::TypeConverter< float >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< int32_t >

**Full name:** `neug::TypeConverter< int32_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< int64_t >

**Full name:** `neug::TypeConverter< int64_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## string >

**Full name:** `neug::TypeConverter< std::string >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## string_view >

**Full name:** `neug::TypeConverter< std::string_view >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< uint32_t >

**Full name:** `neug::TypeConverter< uint32_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypeConverter< uint64_t >

**Full name:** `neug::TypeConverter< uint64_t >`

**Public Methods:**

- `property_type()`
- `ArrowTypeValue()`


---

## TypedColumn

**Full name:** `neug::TypedColumn`

**Public Methods:**

- `TypedColumn(const T &default_value, StorageStrategy strategy)`
- `~TypedColumn()`
- `open(const std::string &name, const std::string &snaps...`
- `open_in_memory(const std::string &name) override`
- `open_with_hugepages(const std::string &name, bool force) override`
- `close() override`
- `copy_to_tmp(const std::string &cur_path, const std::string &t...`
- `dump(const std::string &filename) override`
- `size() const override`
- `resize(size_t size) override`
- ... and 11 more methods


---

## TypedColumn< EmptyType >

**Full name:** `neug::TypedColumn< EmptyType >`

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
- ... and 11 more methods


---

## TypedCsrBase

**Full name:** `neug::TypedCsrBase`

**Public Methods:**

- `batch_put_edges(const std::vector< vid_t > &src_list, const std::...`
- `put_edge(vid_t src, vid_t dst, const EDATA_T &data, timest...`
- `put_generic_edge(vid_t src, vid_t dst, const Property &data, times...`


---

## TypedMutableCsrBase

**Full name:** `neug::TypedMutableCsrBase`


---

## TypedRefColumn

**Full name:** `neug::TypedRefColumn`

**Public Methods:**

- `TypedRefColumn(const TypedColumn< T > &column)`
- `~TypedRefColumn()`
- `get_view(size_t index) const`
- `get(size_t index) const override`
- `type() const override`
- `col_type() const override`


---

## TypedView

**Full name:** `neug::TypedView`

**Public Methods:**

- `TypedView()=default`
- `~TypedView()=default`


---

## kMultipleMutable >

**Full name:** `neug::TypedView< T, CsrViewType::kMultipleMutable >`

**Public Methods:**

- `TypedView(const MutableNbr< T > **adjlists, const int *degr...`
- `~TypedView()=default`
- `foreach_nbr_gt(vid_t v, const T &threshold, const FUNC_T &func) const`
- `foreach_nbr_lt(vid_t v, const T &threshold, const FUNC_T &func) const`


---

## UnifiedServiceImpl

**Full name:** `neug::UnifiedServiceImpl`

The unified service implementation for BRPC server.

**Public Methods:**

- `UnifiedServiceImpl(neug::NeugDB &neug_db, SessionPool &session_pool)`
- `~UnifiedServiceImpl()`
- `GetSchemaImpl(brpc::Controller *cntl_base)`
- `GetServiceStatusImpl(brpc::Controller *cntl_base)`


---

## UpdateEdgePropRedo

**Full name:** `neug::UpdateEdgePropRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t src_label, const Property...`
- `Deserialize(OutArchive &arc, UpdateEdgePropRedo &redo)`


---

## UpdateEdgePropUndo

**Full name:** `neug::UpdateEdgePropUndo`

**Public Methods:**

- `UpdateEdgePropUndo(label_t src_label, label_t dst_label, label_t edg...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## UpdateTransaction

**Full name:** `neug::UpdateTransaction`

Transaction for updating existing graph elements (vertices and edges).

`UpdateTransaction` handles modifications to existing graph data with ACID guarantees. It supports updating vertex properties, edge properties, and provides options for vertex column resizing during u...

**Public Methods:**

- `UpdateTransaction(PropertyGraph &graph, Allocator &alloc, IWalWrite...` - Construct an `UpdateTransaction`
- `~UpdateTransaction()` - Destructor that calls release()
- `timestamp() const` - Get the transaction timestamp
- `schema() const` - Get read-only access to the graph schema
- `Commit()`
- `Abort()`
- `CreateVertexType(const std::string &name, const std::vector< std::...`
- `CreateEdgeType(const std::string &src_type, const std::string &d...`
- `AddVertexProperties(const std::string &vertex_type_name, const std::v...`
- `AddEdgeProperties(const std::string &src_type, const std::string &d...`
- ... and 29 more methods


---

## UpdateVertexPropRedo

**Full name:** `neug::UpdateVertexPropRedo`

**Public Methods:**

- `Serialize(InArchive &arc, label_t label, const Property &oi...`
- `Deserialize(OutArchive &arc, UpdateVertexPropRedo &redo)`


---

## UpdateVertexPropUndo

**Full name:** `neug::UpdateVertexPropUndo`

**Public Methods:**

- `UpdateVertexPropUndo(label_t v_label, vid_t vid, int32_t col_id, const...`
- `GetType() const override`
- `Undo(PropertyGraph &graph, timestamp_t ts) const override`


---

## UpdateWalUnit

**Full name:** `neug::UpdateWalUnit`


---

## VarcharExtraInfo

**Full name:** `neug::VarcharExtraInfo`

**Public Methods:**

- `VarcharExtraInfo(uint64_t maxLength)`
- `~VarcharExtraInfo() override=default`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`


---

## VertexSchema

**Full name:** `neug::VertexSchema`

**Public Methods:**

- `VertexSchema()=default`
- `VertexSchema(const std::string &label_name_, const std::vector...`
- `clear()`
- `empty() const`
- `add_properties(const std::vector< std::string > &names, const st...`
- `set_properties(const std::vector< DataTypeId > &types, const std...`
- `rename_properties(const std::vector< std::string > &names, const st...`
- `delete_properties(const std::vector< std::string > &names, bool is_...`
- `revert_delete_properties(const std::vector< std::string > &names)`
- `is_property_soft_deleted(const std::string &prop) const`
- ... and 5 more methods


---

## VertexSet

**Full name:** `neug::VertexSet`

**Public Methods:**

- `VertexSet(vid_t size, const VertexTimestamp &v_ts_, timestamp_t ts)`
- `~VertexSet()`
- `foreach_vertex(const FUNC_T &func) const`
- `begin() const`
- `end() const`
- `size() const`


---

## iterator

**Full name:** `neug::VertexSet::iterator`

**Public Methods:**

- `iterator(vid_t v, vid_t limit, const VertexTimestamp &v_tr...`
- `~iterator()`
- `operator*() const`
- `operator++()`
- `operator==(const iterator &rhs) const`
- `operator!=(const iterator &rhs) const`


---

## VertexTable

**Full name:** `neug::VertexTable`

**Public Methods:**

- `VertexTable(std::shared_ptr< const VertexSchema > vertex_schema)`
- `VertexTable(VertexTable &&other)`
- `VertexTable(const VertexTable &)=delete`
- `Swap(VertexTable &other)`
- `Open(const std::string &work_dir, int memory_level, bo...`
- `Dump(const std::string &target_dir)`
- `Close()`
- `SetVertexSchema(std::shared_ptr< const VertexSchema > vertex_schema)`
- `Reserve(size_t cap)`
- `is_dropped() const`
- ... and 26 more methods


---

## VertexTimestamp

**Full name:** `neug::VertexTimestamp`

**Public Methods:**

- `VertexTimestamp()`
- `~VertexTimestamp()`
- `VertexTimestamp(VertexTimestamp &&other)`
- `Open(const std::string &tracker_file_prefix)`
- `Dump(const std::string &tracker_file_prefix)`
- `Init(vid_t init_vertex_num, vid_t max_vertex_num)`
- `Reserve(size_t new_size)`
- `Swap(VertexTimestamp &other)`
- `Reset()`
- `Clear()`
- ... and 11 more methods


---

## WalContentUnit

**Full name:** `neug::WalContentUnit`


---

## WalHeader

**Full name:** `neug::WalHeader`


---

## WalParserFactory

**Full name:** `neug::WalParserFactory`

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateWalParser(const std::string &wal_uri)`
- `RegisterWalParser(const std::string &wal_parser_type, wal_parser_in...`


---

## WalWriterFactory

**Full name:** `neug::WalWriterFactory`

**Public Methods:**

- `Init()`
- `Finalize()`
- `CreateDummyWalWriter()`
- `CreateWalWriter(const std::string &wal_uri, int32_t thread_id)`
- `RegisterWalWriter(const std::string &wal_writer_type, wal_writer_in...`


---

## string_view, INDEX_T >

**Full name:** `neug::_move_data< std::string_view, INDEX_T >`

**Public Methods:**

- `operator()(const key_buffer_t &input, ColumnBase &col, size_t size)`


---

## AdjQueryGraph

**Full name:** `neug::binder::AdjQueryGraph`

**Public Methods:**

- `AdjQueryGraph(const QueryGraphCollection *queryGraph)`
- `getAdjEdges(std::shared_ptr< NodeExpression > node) const`
- `getAdjNode(std::shared_ptr< NodeExpression > node, std::shar...`
- `getNode(std::shared_ptr< Expression > name) const`


---

## AggregateFunctionExpression

**Full name:** `neug::binder::AggregateFunctionExpression`

**Public Methods:**

- `AggregateFunctionExpression(function::AggregateFunction function, std::unique...`
- `getFunction() const`
- `getBindData() const`
- `isDistinct() const`
- `toStringInternal() const override`
- `getUniqueName(const std::string &functionName, const expression...`


---

## AttachInfo

**Full name:** `neug::binder::AttachInfo`

**Public Methods:**

- `AttachInfo(std::string dbPath, std::string dbAlias, std::str...`


---

## AttachOption

**Full name:** `neug::binder::AttachOption`


---

## Binder

**Full name:** `neug::binder::Binder`

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
- ... and 109 more methods


---

## BinderScope

**Full name:** `neug::binder::BinderScope`

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

**Full name:** `neug::binder::BoundAlter`

**Public Methods:**

- `BoundAlter(BoundAlterInfo info)`
- `getInfo() const`


---

## BoundAlterInfo

**Full name:** `neug::binder::BoundAlterInfo`

**Public Methods:**

- `BoundAlterInfo(common::AlterType alterType, std::string tableNam...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundAlterInfo)`
- `toString() const`


---

## BoundAttachDatabase

**Full name:** `neug::binder::BoundAttachDatabase`

**Public Methods:**

- `BoundAttachDatabase(binder::AttachInfo attachInfo)`
- `getAttachInfo() const`


---

## BoundBaseScanSource

**Full name:** `neug::binder::BoundBaseScanSource`

**Public Methods:**

- `BoundBaseScanSource(common::ScanSourceType type)`
- `~BoundBaseScanSource()=default`
- `getColumns()=0`
- `getWarningColumns() const`
- `getIgnoreErrorsOption() const`
- `getNumWarningDataColumns() const`
- `copy() const =0`
- `cast()`
- `ptrCast()`
- `constCast() const`
- ... and 1 more methods


---

## BoundCopyFrom

**Full name:** `neug::binder::BoundCopyFrom`

**Public Methods:**

- `BoundCopyFrom(BoundCopyFromInfo info)`
- `getInfo() const`


---

## BoundCopyFromInfo

**Full name:** `neug::binder::BoundCopyFromInfo`

**Public Methods:**

- `BoundCopyFromInfo(catalog::TableCatalogEntry *tableEntry, std::uniq...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo)`
- `getSourceColumns() const`
- `getNumWarningColumns() const`
- `getIgnoreErrorsOption() const`


---

## BoundCopyTo

**Full name:** `neug::binder::BoundCopyTo`

**Public Methods:**

- `BoundCopyTo(std::unique_ptr< function::ExportFuncBindData > b...`
- `getBindData() const`
- `getExportFunc() const`
- `getRegularQuery() const`


---

## BoundCreateMacro

**Full name:** `neug::binder::BoundCreateMacro`

**Public Methods:**

- `BoundCreateMacro(std::string macroName, std::unique_ptr< function:...`
- `getMacroName() const`
- `getMacro() const`


---

## BoundCreateSequence

**Full name:** `neug::binder::BoundCreateSequence`

**Public Methods:**

- `BoundCreateSequence(BoundCreateSequenceInfo info)`
- `getInfo() const`


---

## BoundCreateSequenceInfo

**Full name:** `neug::binder::BoundCreateSequenceInfo`

**Public Methods:**

- `BoundCreateSequenceInfo(std::string sequenceName, int64_t startWith, int6...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateSequenceInfo)`


---

## BoundCreateTable

**Full name:** `neug::binder::BoundCreateTable`

**Public Methods:**

- `BoundCreateTable(BoundCreateTableInfo info)`
- `getInfo() const`


---

## BoundCreateTableInfo

**Full name:** `neug::binder::BoundCreateTableInfo`

**Public Methods:**

- `BoundCreateTableInfo()=default`
- `BoundCreateTableInfo(catalog::CatalogEntryType type, std::string table...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundCreateTableInfo)`
- `toString() const`


---

## BoundCreateType

**Full name:** `neug::binder::BoundCreateType`

**Public Methods:**

- `BoundCreateType(std::string name, common::LogicalType type)`
- `getName() const`
- `getType() const`


---

## BoundDatabaseStatement

**Full name:** `neug::binder::BoundDatabaseStatement`

**Public Methods:**

- `BoundDatabaseStatement(common::StatementType statementType, std::string dbName)`
- `getDBName() const`


---

## BoundDeleteClause

**Full name:** `neug::binder::BoundDeleteClause`

**Public Methods:**

- `BoundDeleteClause()`
- `addInfo(BoundDeleteInfo info)`
- `hasNodeInfo() const`
- `getNodeInfos() const`
- `hasRelInfo() const`
- `getRelInfos() const`


---

## BoundDeleteInfo

**Full name:** `neug::binder::BoundDeleteInfo`

**Public Methods:**

- `BoundDeleteInfo(common::DeleteNodeType deleteType, common::TableT...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundDeleteInfo)`
- `toString() const`


---

## BoundDetachDatabase

**Full name:** `neug::binder::BoundDetachDatabase`

**Public Methods:**

- `BoundDetachDatabase(std::string dbName)`


---

## BoundDrop

**Full name:** `neug::binder::BoundDrop`

**Public Methods:**

- `BoundDrop(parser::DropInfo dropInfo)`
- `getDropInfo() const`


---

## BoundExplain

**Full name:** `neug::binder::BoundExplain`

**Public Methods:**

- `BoundExplain(std::unique_ptr< BoundStatement > statementToExpl...`
- `getStatementToExplain() const`
- `getExplainType() const`


---

## BoundExportDatabase

**Full name:** `neug::binder::BoundExportDatabase`

**Public Methods:**

- `BoundExportDatabase(std::string filePath, common::FileTypeInfo fileTy...`
- `getFilePath() const`
- `getFileType() const`
- `getExportOptions() const`
- `getBoundFileInfo() const`
- `getExportData() const`


---

## BoundExtensionStatement

**Full name:** `neug::binder::BoundExtensionStatement`

**Public Methods:**

- `BoundExtensionStatement(std::unique_ptr< ExtensionAuxInfo > info)`
- `getAuxInfo() const`


---

## BoundExtraAddPropertyInfo

**Full name:** `neug::binder::BoundExtraAddPropertyInfo`

**Public Methods:**

- `BoundExtraAddPropertyInfo(const PropertyDefinition &definition, std::shared...`
- `BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo &other)`
- `copy() const override`


---

## BoundExtraAlterInfo

**Full name:** `neug::binder::BoundExtraAlterInfo`

**Public Methods:**

- `~BoundExtraAlterInfo()=default`
- `constPtrCast() const`
- `constCast() const`
- `cast()`
- `copy() const =0`


---

## BoundExtraCommentInfo

**Full name:** `neug::binder::BoundExtraCommentInfo`

**Public Methods:**

- `BoundExtraCommentInfo(std::string comment)`
- `BoundExtraCommentInfo(const BoundExtraCommentInfo &other)`
- `copy() const override`


---

## BoundExtraCreateCatalogEntryInfo

**Full name:** `neug::binder::BoundExtraCreateCatalogEntryInfo`

**Public Methods:**

- `~BoundExtraCreateCatalogEntryInfo()=default`
- `constPtrCast() const`
- `ptrCast()`
- `copy() const =0`


---

## BoundExtraCreateNodeTableInfo

**Full name:** `neug::binder::BoundExtraCreateNodeTableInfo`

**Public Methods:**

- `BoundExtraCreateNodeTableInfo(std::string primaryKeyName, std::vector< Property...`
- `BoundExtraCreateNodeTableInfo(const BoundExtraCreateNodeTableInfo &other)`
- `copy() const override`


---

## BoundExtraCreateRelTableGroupInfo

**Full name:** `neug::binder::BoundExtraCreateRelTableGroupInfo`

**Public Methods:**

- `BoundExtraCreateRelTableGroupInfo(std::vector< BoundCreateTableInfo > infos)`
- `BoundExtraCreateRelTableGroupInfo(const BoundExtraCreateRelTableGroupInfo &other)`
- `copy() const override`


---

## BoundExtraCreateRelTableInfo

**Full name:** `neug::binder::BoundExtraCreateRelTableInfo`

**Public Methods:**

- `BoundExtraCreateRelTableInfo(common::table_id_t srcTableID, common::table_id_t...`
- `BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity, common::...`
- `BoundExtraCreateRelTableInfo(common::RelMultiplicity srcMultiplicity, common::...`
- `BoundExtraCreateRelTableInfo(const BoundExtraCreateRelTableInfo &other)`
- `copy() const override`


---

## BoundExtraCreateTableInfo

**Full name:** `neug::binder::BoundExtraCreateTableInfo`

**Public Methods:**

- `BoundExtraCreateTableInfo(std::vector< PropertyDefinition > propertyDefinitions)`
- `BoundExtraCreateTableInfo(const BoundExtraCreateTableInfo &other)`
- `operator=(const BoundExtraCreateTableInfo &)=delete`
- `copy() const override`


---

## BoundExtraDropPropertyInfo

**Full name:** `neug::binder::BoundExtraDropPropertyInfo`

**Public Methods:**

- `BoundExtraDropPropertyInfo(std::string propertyName)`
- `BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo &other)`
- `copy() const override`


---

## BoundExtraRenamePropertyInfo

**Full name:** `neug::binder::BoundExtraRenamePropertyInfo`

**Public Methods:**

- `BoundExtraRenamePropertyInfo(std::string newName, std::string oldName)`
- `BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo &other)`
- `copy() const override`


---

## BoundExtraRenameTableInfo

**Full name:** `neug::binder::BoundExtraRenameTableInfo`

**Public Methods:**

- `BoundExtraRenameTableInfo(std::string newName)`
- `BoundExtraRenameTableInfo(const BoundExtraRenameTableInfo &other)`
- `copy() const override`


---

## BoundGraphPattern

**Full name:** `neug::binder::BoundGraphPattern`

**Public Methods:**

- `BoundGraphPattern()=default`
- `DELETE_COPY_DEFAULT_MOVE(BoundGraphPattern)`


---

## BoundImportDatabase

**Full name:** `neug::binder::BoundImportDatabase`

**Public Methods:**

- `BoundImportDatabase(std::string filePath, std::string query, std::str...`
- `getFilePath() const`
- `getQuery() const`
- `getIndexQuery() const`


---

## BoundInsertClause

**Full name:** `neug::binder::BoundInsertClause`

**Public Methods:**

- `BoundInsertClause(std::vector< BoundInsertInfo > infos)`
- `getInfos() const`
- `hasNodeInfo() const`
- `getNodeInfos() const`
- `hasRelInfo() const`
- `getRelInfos() const`


---

## BoundInsertInfo

**Full name:** `neug::binder::BoundInsertInfo`

**Public Methods:**

- `BoundInsertInfo(common::TableType tableType, std::shared_ptr< Exp...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundInsertInfo)`


---

## BoundJoinHintNode

**Full name:** `neug::binder::BoundJoinHintNode`

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

**Full name:** `neug::binder::BoundLoadFrom`

**Public Methods:**

- `BoundLoadFrom(BoundTableScanInfo info)`
- `getInfo() const`


---

## BoundMatchClause

**Full name:** `neug::binder::BoundMatchClause`

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

**Full name:** `neug::binder::BoundMergeClause`

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

**Full name:** `neug::binder::BoundProjectionBody`

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

**Full name:** `neug::binder::BoundQueryScanSource`

**Public Methods:**

- `BoundQueryScanSource(std::shared_ptr< BoundStatement > statement, Boun...`
- `BoundQueryScanSource(const BoundQueryScanSource &other)=default`
- `getIgnoreErrorsOption() const override`
- `getColumns() override`
- `copy() const override`


---

## BoundQueryScanSourceInfo

**Full name:** `neug::binder::BoundQueryScanSourceInfo`

**Public Methods:**

- `BoundQueryScanSourceInfo(common::case_insensitive_map_t< common::Value > options)`


---

## BoundReadingClause

**Full name:** `neug::binder::BoundReadingClause`

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

**Full name:** `neug::binder::BoundRegularQuery`

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

**Full name:** `neug::binder::BoundReturnClause`

**Public Methods:**

- `BoundReturnClause(BoundProjectionBody projectionBody)`
- `BoundReturnClause(BoundProjectionBody projectionBody, BoundStatemen...`
- `DELETE_COPY_DEFAULT_MOVE(BoundReturnClause)`
- `~BoundReturnClause()=default`
- `getProjectionBody() const`
- `getStatementResult() const`


---

## BoundSetClause

**Full name:** `neug::binder::BoundSetClause`

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

**Full name:** `neug::binder::BoundSetPropertyInfo`

**Public Methods:**

- `BoundSetPropertyInfo(common::TableType tableType, std::shared_ptr< Exp...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundSetPropertyInfo)`


---

## BoundStandaloneCall

**Full name:** `neug::binder::BoundStandaloneCall`

**Public Methods:**

- `BoundStandaloneCall(const main::Option *option, std::shared_ptr< Expr...`
- `getOption() const`
- `getOptionValue() const`


---

## BoundStandaloneCallFunction

**Full name:** `neug::binder::BoundStandaloneCallFunction`

**Public Methods:**

- `BoundStandaloneCallFunction(BoundTableScanInfo info)`
- `BoundStandaloneCallFunction(BoundTableScanInfo info, expression_vector outputColumns)`
- `getTableScanInfo() const`
- `getTableFunction() const`
- `getOutputColumns() const`
- `getBindData() const`


---

## BoundStatement

**Full name:** `neug::binder::BoundStatement`

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

**Full name:** `neug::binder::BoundStatementResult`

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

**Full name:** `neug::binder::BoundStatementRewriter`

**Public Methods:**

- `rewrite(BoundStatement &boundStatement, main::ClientConte...`


---

## BoundStatementVisitor

**Full name:** `neug::binder::BoundStatementVisitor`

**Public Methods:**

- `BoundStatementVisitor()=default`
- `~BoundStatementVisitor()=default`
- `visit(const BoundStatement &statement)`
- `visitUnsafe(BoundStatement &statement)`
- `visitSingleQuery(const NormalizedSingleQuery &singleQuery)`
- `visitQueryPart(const NormalizedQueryPart &queryPart)`


---

## BoundTableFunctionCall

**Full name:** `neug::binder::BoundTableFunctionCall`

**Public Methods:**

- `BoundTableFunctionCall(BoundTableScanInfo info)`
- `BoundTableFunctionCall(BoundTableScanInfo info, expression_vector outputColumns)`
- `getTableScanInfo() const`
- `getOutputColumns() const`
- `getTableFunc() const`
- `getBindData() const`


---

## BoundTableScanInfo

**Full name:** `neug::binder::BoundTableScanInfo`

**Public Methods:**

- `BoundTableScanInfo(function::TableFunction func, std::unique_ptr< fu...`
- `EXPLICIT_COPY_DEFAULT_MOVE(BoundTableScanInfo)`


---

## BoundTableScanSource

**Full name:** `neug::binder::BoundTableScanSource`

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

**Full name:** `neug::binder::BoundTransactionStatement`

**Public Methods:**

- `BoundTransactionStatement(transaction::TransactionAction transactionAction)`
- `getTransactionAction() const`


---

## BoundUnwindClause

**Full name:** `neug::binder::BoundUnwindClause`

**Public Methods:**

- `BoundUnwindClause(std::shared_ptr< Expression > inExpr, std::shared...`
- `getInExpr() const`
- `getOutExpr() const`
- `getIDExpr() const`


---

## BoundUpdatingClause

**Full name:** `neug::binder::BoundUpdatingClause`

**Public Methods:**

- `BoundUpdatingClause(common::ClauseType clauseType)`
- `~BoundUpdatingClause()=default`
- `getClauseType() const`
- `cast() const`
- `constCast() const`


---

## BoundUseDatabase

**Full name:** `neug::binder::BoundUseDatabase`

**Public Methods:**

- `BoundUseDatabase(std::string dbName)`


---

## BoundWithClause

**Full name:** `neug::binder::BoundWithClause`

**Public Methods:**

- `BoundWithClause(BoundProjectionBody projectionBody)`
- `setWhereExpression(std::shared_ptr< Expression > expression)`
- `hasWhereExpression() const`
- `getWhereExpression() const`


---

## CaseAlternative

**Full name:** `neug::binder::CaseAlternative`

**Public Methods:**

- `CaseAlternative(std::shared_ptr< Expression > whenExpression, std...`


---

## CaseExpression

**Full name:** `neug::binder::CaseExpression`

**Public Methods:**

- `CaseExpression(common::LogicalType dataType, std::shared_ptr< Ex...`
- `addCaseAlternative(std::shared_ptr< Expression > when, std::shared_p...`
- `getNumCaseAlternatives() const`
- `getCaseAlternative(common::idx_t idx) const`
- `getElseExpression() const`
- `toStringInternal() const override`


---

## ColumnDefinition

**Full name:** `neug::binder::ColumnDefinition`

**Public Methods:**

- `ColumnDefinition()=default`
- `ColumnDefinition(std::string name, common::LogicalType type)`
- `EXPLICIT_COPY_DEFAULT_MOVE(ColumnDefinition)`


---

## CommonPatReuseRewriter

**Full name:** `neug::binder::CommonPatReuseRewriter`

This rewriter is used to set join order heuristically for each subquery in union.

For example, in the following query: MATCH (a:person) CALL (a) { subquery1 UNION ALL subquery2 }, it will generate join order for both subquery1 and subquery2. Each subquery is bound to the common exp...

**Public Methods:**

- `CommonPatReuseRewriter(main::ClientContext *clientContext)`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## ConfidentialStatementAnalyzer

**Full name:** `neug::binder::ConfidentialStatementAnalyzer`

**Public Methods:**

- `isConfidential() const`


---

## ConstantExpressionVisitor

**Full name:** `neug::binder::ConstantExpressionVisitor`

**Public Methods:**

- `needFold(const Expression &expr)`
- `isConstant(const Expression &expr)`


---

## DefaultTypeSolver

**Full name:** `neug::binder::DefaultTypeSolver`


---

## DependentVarNameCollector

**Full name:** `neug::binder::DependentVarNameCollector`

**Public Methods:**

- `getVarNames() const`


---

## EqualFilteringVisitor

**Full name:** `neug::binder::EqualFilteringVisitor`

**Public Methods:**

- `visitFunctionExpr(std::shared_ptr< Expression > expr) override`
- `containsEqualFiltering()`


---

## ExportedTableData

**Full name:** `neug::binder::ExportedTableData`

**Public Methods:**

- `getColumnTypesRef() const`
- `getRegularQuery() const`


---

## Expression

**Full name:** `neug::binder::Expression`

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

**Full name:** `neug::binder::ExpressionBinder`

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

**Full name:** `neug::binder::ExpressionBinderConfig`


---

## ExpressionChildrenCollector

**Full name:** `neug::binder::ExpressionChildrenCollector`

**Public Methods:**

- `collectChildren(const Expression &expression)`


---

## ExpressionEquality

**Full name:** `neug::binder::ExpressionEquality`

**Public Methods:**

- `operator()(const std::shared_ptr< Expression > &left, const ...`


---

## ExpressionHasher

**Full name:** `neug::binder::ExpressionHasher`

**Public Methods:**

- `operator()(const std::shared_ptr< Expression > &expression) const`


---

## ExpressionUtil

**Full name:** `neug::binder::ExpressionUtil`

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

**Full name:** `neug::binder::ExpressionVisitor`

**Public Methods:**

- `~ExpressionVisitor()=default`
- `visit(std::shared_ptr< Expression > expr)`
- `isRandom(const Expression &expression)`


---

## ExtraBoundCopyFromInfo

**Full name:** `neug::binder::ExtraBoundCopyFromInfo`

**Public Methods:**

- `~ExtraBoundCopyFromInfo()=default`
- `copy() const =0`
- `constCast() const`


---

## ExtraBoundCopyRelInfo

**Full name:** `neug::binder::ExtraBoundCopyRelInfo`

**Public Methods:**

- `ExtraBoundCopyRelInfo(std::vector< common::idx_t > internalIDColumnIndi...`
- `ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo &other)=default`
- `copy() const override`


---

## IndexLookupInfo

**Full name:** `neug::binder::IndexLookupInfo`

**Public Methods:**

- `IndexLookupInfo(common::table_id_t nodeTableID, std::shared_ptr< ...`
- `IndexLookupInfo(const IndexLookupInfo &other)=default`


---

## LambdaExpression

**Full name:** `neug::binder::LambdaExpression`

**Public Methods:**

- `LambdaExpression(std::unique_ptr< parser::ParsedExpression > parse...`
- `cast(const common::LogicalType &type_) override`
- `getParsedLambdaExpr() const`
- `setFunctionExpr(std::shared_ptr< Expression > expr)`
- `getFunctionExpr() const`
- `toStringInternal() const override`


---

## LiteralExpression

**Full name:** `neug::binder::LiteralExpression`

**Public Methods:**

- `LiteralExpression(common::Value value, const std::string &uniqueName)`
- `isNull() const`
- `cast(const common::LogicalType &type) override`
- `getValue() const`
- `toStringInternal() const override`
- `copy() const override`


---

## MatchClausePatternLabelRewriter

**Full name:** `neug::binder::MatchClausePatternLabelRewriter`

**Public Methods:**

- `MatchClausePatternLabelRewriter(const main::ClientContext &clientContext)`
- `visitMatchUnsafe(BoundReadingClause &readingClause) override`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## NodeExpression

**Full name:** `neug::binder::NodeExpression`

**Public Methods:**

- `NodeExpression(common::LogicalType dataType, std::string uniqueN...`
- `~NodeExpression() override`
- `setInternalID(std::unique_ptr< Expression > expression)`
- `getInternalID() const`
- `getInternalIDRef() const`
- `setNodeUniqueName(const std::string &uniqueName)`
- `getPrimaryKey(common::table_id_t tableID) const`


---

## NodeOrRelExpression

**Full name:** `neug::binder::NodeOrRelExpression`

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

**Full name:** `neug::binder::NormalizedQueryPart`

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

**Full name:** `neug::binder::NormalizedQueryPartMatchRewriter`

**Public Methods:**

- `NormalizedQueryPartMatchRewriter(main::ClientContext *clientContext)`


---

## NormalizedSingleQuery

**Full name:** `neug::binder::NormalizedSingleQuery`

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

**Full name:** `neug::binder::ParameterExpression`

**Public Methods:**

- `ParameterExpression(const std::string &parameterName, common::Value value)`
- `cast(const common::LogicalType &type) override`
- `getValue() const`
- `getName() const`


---

## PathExpression

**Full name:** `neug::binder::PathExpression`

**Public Methods:**

- `PathExpression(common::LogicalType dataType, std::string uniqueN...`
- `getVariableName() const`
- `getNodeType() const`
- `getRelType() const`
- `toStringInternal() const override`


---

## PropertyCollector

**Full name:** `neug::binder::PropertyCollector`

**Public Methods:**

- `getProperties() const`
- `visitSingleQuerySkipNodeRel(const NormalizedSingleQuery &singleQuery)`


---

## PropertyDefinition

**Full name:** `neug::binder::PropertyDefinition`

**Public Methods:**

- `PropertyDefinition()=default`
- `PropertyDefinition(ColumnDefinition columnDefinition, std::unique_pt...`
- `PropertyDefinition(ColumnDefinition columnDefinition, std::unique_pt...`
- `EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinition)`
- `getName() const`
- `getType() const`
- `getDefaultExpressionName() const`
- `rename(const std::string &newName)`
- `serialize(common::Serializer &serializer) const`
- `PropertyDefinition(ColumnDefinition columnDefinition)`
- ... and 1 more methods


---

## PropertyExprCollector

**Full name:** `neug::binder::PropertyExprCollector`

**Public Methods:**

- `getPropertyExprs() const`


---

## PropertyExpression

**Full name:** `neug::binder::PropertyExpression`

**Public Methods:**

- `PropertyExpression(common::LogicalType dataType, std::string propert...`
- `PropertyExpression(common::LogicalType dataType, std::string propert...`
- `PropertyExpression(const PropertyExpression &other)`
- `isPrimaryKey() const`
- `isPrimaryKey(common::table_id_t tableID) const`
- `getPropertyName() const`
- `getVariableName() const`
- `getRawVariableName() const`
- `setUniqueVarName(const std::string &uniqueVarName)`
- `hasProperty(common::table_id_t tableID) const`
- ... and 7 more methods


---

## QueryGraph

**Full name:** `neug::binder::QueryGraph`

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

**Full name:** `neug::binder::QueryGraphCollection`

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

**Full name:** `neug::binder::QueryGraphLabelAnalyzer`

**Public Methods:**

- `QueryGraphLabelAnalyzer(const main::ClientContext &clientContext, bool th...`
- `pruneLabel(QueryGraph &graph) const`


---

## RecursiveInfo

**Full name:** `neug::binder::RecursiveInfo`


---

## RelExpression

**Full name:** `neug::binder::RelExpression`

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

**Full name:** `neug::binder::RenameDependentVar`

**Public Methods:**

- `RenameDependentVar(const std::string &newVarName)`


---

## ResetVarUseNameVisitor

**Full name:** `neug::binder::ResetVarUseNameVisitor`

**Public Methods:**

- `ResetVarUseNameVisitor(bool useName)`


---

## ScalarFunctionExpression

**Full name:** `neug::binder::ScalarFunctionExpression`

**Public Methods:**

- `ScalarFunctionExpression(common::ExpressionType expressionType, std::uniqu...`
- `getFunction() const`
- `getBindData() const`
- `toStringInternal() const override`
- `getUniqueName(const std::string &functionName, const expression...`


---

## SingleLabelPropertyInfo

**Full name:** `neug::binder::SingleLabelPropertyInfo`

**Public Methods:**

- `SingleLabelPropertyInfo(bool exists, bool isPrimaryKey)`
- `EXPLICIT_COPY_DEFAULT_MOVE(SingleLabelPropertyInfo)`


---

## SubqueryExprCollector

**Full name:** `neug::binder::SubqueryExprCollector`

**Public Methods:**

- `hasSubquery() const`
- `getSubqueryExprs() const`


---

## SubqueryExpression

**Full name:** `neug::binder::SubqueryExpression`

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

**Full name:** `neug::binder::SubqueryGraph`

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

**Full name:** `neug::binder::SubqueryGraphHasher`

**Public Methods:**

- `operator()(const SubqueryGraph &key) const`


---

## UnionProjectionRewriter

**Full name:** `neug::binder::UnionProjectionRewriter`

This rewriter is used to project outer variables (defined by call) before each subquery in union.

For example, in the following query: MATCH (a:person) CALL (a) { subquery1 UNION ALL subquery2 }, it will project the outer variable `a` before subquery1 and subquery2.

**Public Methods:**

- `UnionProjectionRewriter(main::ClientContext *clientContext)`
- `visitRegularQueryUnsafe(BoundStatement &statement) override`


---

## VariableCastTypeCollector

**Full name:** `neug::binder::VariableCastTypeCollector`

**Public Methods:**

- `VariableCastTypeCollector(std::unordered_map< std::string, common::LogicalT...`


---

## VariableExpression

**Full name:** `neug::binder::VariableExpression`

**Public Methods:**

- `VariableExpression(common::LogicalType dataType, std::string uniqueN...`
- `getVariableName() const`
- `cast(const common::LogicalType &type) override`
- `toStringInternal() const override`
- `copy() const override`
- `setUseName(bool useName)`
- `getUseName() const`


---

## WithClauseProjectionRewriter

**Full name:** `neug::binder::WithClauseProjectionRewriter`

**Public Methods:**

- `visitSingleQueryUnsafe(NormalizedSingleQuery &singleQuery) override`


---

## Catalog

**Full name:** `neug::catalog::Catalog`

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
- ... and 42 more methods


---

## CatalogEntry

**Full name:** `neug::catalog::CatalogEntry`

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

**Full name:** `neug::catalog::CatalogEntryTypeUtils`

**Public Methods:**

- `toString(CatalogEntryType type)`


---

## CatalogSet

**Full name:** `neug::catalog::CatalogSet`

**Public Methods:**

- `CatalogSet()=default`
- `CatalogSet(bool isInternal)`
- `containsEntry(const transaction::Transaction *transaction, cons...`
- `getEntry(const transaction::Transaction *transaction, cons...`
- `createEntry(transaction::Transaction *transaction, std::uniqu...`
- `dropEntry(transaction::Transaction *transaction, const std:...`
- `alterTableEntry(transaction::Transaction *transaction, const bind...`
- `alterRelGroupEntry(transaction::Transaction *transaction, const bind...`
- `getEntries(const transaction::Transaction *transaction)`
- `getEntryOfOID(const transaction::Transaction *transaction, comm...`
- ... and 3 more methods


---

## DummyCatalogEntry

**Full name:** `neug::catalog::DummyCatalogEntry`

**Public Methods:**

- `DummyCatalogEntry(std::string name, common::oid_t oid)`
- `serialize(common::Serializer &) const override`
- `toCypher(const ToCypherInfo &) const override`


---

## FunctionCatalogEntry

**Full name:** `neug::catalog::FunctionCatalogEntry`

**Public Methods:**

- `FunctionCatalogEntry()=default`
- `FunctionCatalogEntry(CatalogEntryType entryType, std::string name, fun...`
- `getFunctionSet() const`
- `serialize(common::Serializer &) const override`


---

## FunctionEntryTypeUtils

**Full name:** `neug::catalog::FunctionEntryTypeUtils`

**Public Methods:**

- `toString(CatalogEntryType type)`


---

## GCatalog

**Full name:** `neug::catalog::GCatalog`

**Public Methods:**

- `GCatalog()`
- `GCatalog(const std::filesystem::path &schemaPath)`
- `GCatalog(const std::string &schemaData)`
- `GCatalog(const YAML::Node &schema)`
- `~GCatalog()=default`
- `addFunctionWithSignature(transaction::Transaction *transaction, CatalogEnt...`
- `getFunctionWithSignature(transaction::Transaction *transaction, const std:...`
- `getFunctionWithSignature(const std::string &signatureName)`
- `updateSchema(const std::filesystem::path &schemaPath)`
- `updateSchema(const std::string &schema)`
- ... and 1 more methods


---

## GCatalogHolder

**Full name:** `neug::catalog::GCatalogHolder`

**Public Methods:**

- `GCatalogHolder()=delete`
- `setGCatalog(GCatalog *catalog)`
- `getGCatalog()`


---

## GRelTableCatalogEntry

**Full name:** `neug::catalog::GRelTableCatalogEntry`

**Public Methods:**

- `GRelTableCatalogEntry()=default`
- `GRelTableCatalogEntry(std::string name, common::RelMultiplicity srcMult...`
- `getLabelId() const`


---

## IndexAuxInfo

**Full name:** `neug::catalog::IndexAuxInfo`

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

**Full name:** `neug::catalog::IndexCatalogEntry`

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

**Full name:** `neug::catalog::IndexToCypherInfo`

**Public Methods:**

- `IndexToCypherInfo(const main::ClientContext *context, const common:...`


---

## NodeTableCatalogEntry

**Full name:** `neug::catalog::NodeTableCatalogEntry`

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

**Full name:** `neug::catalog::PropertyDefinitionCollection`

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

**Full name:** `neug::catalog::RelGroupCatalogEntry`

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

**Full name:** `neug::catalog::RelGroupToCypherInfo`

**Public Methods:**

- `RelGroupToCypherInfo(const main::ClientContext *context)`


---

## RelTableCatalogEntry

**Full name:** `neug::catalog::RelTableCatalogEntry`

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

**Full name:** `neug::catalog::RelTableToCypherInfo`

**Public Methods:**

- `RelTableToCypherInfo(const main::ClientContext *context)`


---

## ScalarMacroCatalogEntry

**Full name:** `neug::catalog::ScalarMacroCatalogEntry`

**Public Methods:**

- `ScalarMacroCatalogEntry()=default`
- `ScalarMacroCatalogEntry(std::string name, std::unique_ptr< function::Scal...`
- `getMacroFunction() const`
- `serialize(common::Serializer &serializer) const override`
- `toCypher(const ToCypherInfo &info) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## SequenceCatalogEntry

**Full name:** `neug::catalog::SequenceCatalogEntry`

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

**Full name:** `neug::catalog::SequenceData`

**Public Methods:**

- `SequenceData()=default`
- `SequenceData(const binder::BoundCreateSequenceInfo &info)`


---

## SequenceRollbackData

**Full name:** `neug::catalog::SequenceRollbackData`


---

## TableCatalogEntry

**Full name:** `neug::catalog::TableCatalogEntry`

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

**Full name:** `neug::catalog::TableCatalogEntryEquality`

**Public Methods:**

- `operator()(TableCatalogEntry *left, TableCatalogEntry *right) const`


---

## TableCatalogEntryHasher

**Full name:** `neug::catalog::TableCatalogEntryHasher`

**Public Methods:**

- `operator()(TableCatalogEntry *entry) const`


---

## ToCypherInfo

**Full name:** `neug::catalog::ToCypherInfo`

**Public Methods:**

- `~ToCypherInfo()=default`
- `constCast() const`


---

## TypeCatalogEntry

**Full name:** `neug::catalog::TypeCatalogEntry`

**Public Methods:**

- `TypeCatalogEntry()=default`
- `TypeCatalogEntry(std::string name, common::LogicalType type)`
- `getLogicalType() const`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## AccumulateTypeUtil

**Full name:** `neug::common::AccumulateTypeUtil`

**Public Methods:**

- `toString(AccumulateType type)`


---

## ArrayType

**Full name:** `neug::common::ArrayType`

**Public Methods:**

- `getChildType(const LogicalType &type)`
- `getNumElements(const LogicalType &type)`


---

## ArrayTypeInfo

**Full name:** `neug::common::ArrayTypeInfo`

**Public Methods:**

- `ArrayTypeInfo()`
- `ArrayTypeInfo(LogicalType childType, uint64_t numElements)`
- `getNumElements() const`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## ArrowBuffer

**Full name:** `neug::common::ArrowBuffer`

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

**Full name:** `neug::common::ArrowColumnAuxiliaryBuffer`


---

## ArrowConverter

**Full name:** `neug::common::ArrowConverter`

**Public Methods:**

- `toArrowSchema(const std::vector< LogicalType > &dataTypes, cons...`
- `fromArrowSchema(const ArrowSchema *schema)`
- `fromArrowArray(const ArrowSchema *schema, const ArrowArray *arra...`
- `fromArrowArray(const ArrowSchema *schema, const ArrowArray *arra...`


---

## ArrowNullMaskTree

**Full name:** `neug::common::ArrowNullMaskTree`

**Public Methods:**

- `ArrowNullMaskTree(const ArrowSchema *schema, const ArrowArray *arra...`
- `copyToValueVector(ValueVector *vec, uint64_t dstOffset, uint64_t count)`
- `isNull(int64_t idx)`
- `getChild(int idx)`
- `getDictionary()`
- `offsetBy(int64_t offset)`


---

## ArrowRowBatch

**Full name:** `neug::common::ArrowRowBatch`

**Public Methods:**

- `ArrowRowBatch(std::vector< LogicalType > types, std::int64_t capacity)`
- `append(main::QueryResult &queryResult, std::int64_t chunkSize)` - Append a data chunk to the underlying arrow array


---

## ArrowSchemaHolder

**Full name:** `neug::common::ArrowSchemaHolder`


---

## ArrowVector

**Full name:** `neug::common::ArrowVector`


---

## AuxiliaryBuffer

**Full name:** `neug::common::AuxiliaryBuffer`

**Public Methods:**

- `~AuxiliaryBuffer()=default`
- `cast()`
- `constCast() const`


---

## AuxiliaryBufferFactory

**Full name:** `neug::common::AuxiliaryBufferFactory`

**Public Methods:**

- `getAuxiliaryBuffer(LogicalType &type, storage::MemoryManager *memoryManager)`


---

## BinaryData

**Full name:** `neug::common::BinaryData`


---

## BitmaskUtils

**Full name:** `neug::common::BitmaskUtils`

**Public Methods:**

- `all1sMaskForLeastSignificantBits(uint32_t numBits)`
- `all1sMaskForLeastSignificantBits(uint32_t numBits)`


---

## Blob

**Full name:** `neug::common::Blob`

**Public Methods:**

- `toString(const uint8_t *value, uint64_t len)`
- `toString(const blob_t &blob)`
- `getBlobSize(const neug_string_t &blob)`
- `fromString(const char *str, uint64_t length, uint8_t *resultBuffer)`
- `getValue(const blob_t &data)`
- `getValue(char *data)`


---

## BlobVector

**Full name:** `neug::common::BlobVector`

**Public Methods:**

- `addBlob(ValueVector *vector, uint32_t pos, const char *da...`
- `addBlob(ValueVector *vector, uint32_t pos, const uint8_t ...`


---

## BufferBlock

**Full name:** `neug::common::BufferBlock`

**Public Methods:**

- `BufferBlock(std::unique_ptr< storage::MemoryBuffer > block)`
- `~BufferBlock()`
- `size() const`
- `data() const`
- `resetCurrentOffset()`


---

## BufferPoolConstants

**Full name:** `neug::common::BufferPoolConstants`


---

## BufferReader

**Full name:** `neug::common::BufferReader`

**Public Methods:**

- `BufferReader(uint8_t *data, size_t dataSize)`
- `read(uint8_t *outputData, uint64_t size) override`
- `finished() override`


---

## BufferedFileReader

**Full name:** `neug::common::BufferedFileReader`

**Public Methods:**

- `BufferedFileReader(std::unique_ptr< FileInfo > fileInfo)`
- `read(uint8_t *data, uint64_t size) override`
- `finished() override`


---

## BufferedFileWriter

**Full name:** `neug::common::BufferedFileWriter`

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

**Full name:** `neug::common::BufferedSerializer`

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

**Full name:** `neug::common::CSVOption`

**Public Methods:**

- `CSVOption()`
- `EXPLICIT_COPY_DEFAULT_MOVE(CSVOption)`
- `toCypher() const`
- `CSVOption(const CSVOption &other)`


---

## CSVReaderConfig

**Full name:** `neug::common::CSVReaderConfig`

**Public Methods:**

- `CSVReaderConfig()`
- `EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig)`
- `construct(const case_insensitive_map_t< common::Value > &options)`


---

## CaseInsensitiveStringEquality

**Full name:** `neug::common::CaseInsensitiveStringEquality`

**Public Methods:**

- `operator()(const std::string &lhs, const std::string &rhs) const`


---

## CaseInsensitiveStringHashFunction

**Full name:** `neug::common::CaseInsensitiveStringHashFunction`

**Public Methods:**

- `operator()(const std::string &str) const`


---

## CompressedFileInfo

**Full name:** `neug::common::CompressedFileInfo`

**Public Methods:**

- `CompressedFileInfo(CompressedFileSystem &compressedFS, std::unique_p...`
- `~CompressedFileInfo() override`
- `initialize()`
- `readData(void *buffer, size_t numBytes)`
- `close()`


---

## CompressedFileSystem

**Full name:** `neug::common::CompressedFileSystem`

**Public Methods:**

- `openCompressedFile(std::unique_ptr< FileInfo > fileInfo)=0`
- `createStream()=0`
- `getInputBufSize()=0`
- `getOutputBufSize()=0`
- `canPerformSeek() const override`


---

## ConcurrentVector

**Full name:** `neug::common::ConcurrentVector`

**Public Methods:**

- `ConcurrentVector(uint64_t initialNumElements, uint64_t initialBlockSize)`
- `resize(uint64_t newSize)`
- `push_back(T &&value)`
- `operator[](uint64_t elemPos)`
- `size()`


---

## Block

**Full name:** `neug::common::ConcurrentVector::Block`


---

## BlockIndex

**Full name:** `neug::common::ConcurrentVector::BlockIndex`

**Public Methods:**

- `BlockIndex()`


---

## ConflictActionUtil

**Full name:** `neug::common::ConflictActionUtil`

**Public Methods:**

- `toString(ConflictAction conflictAction)`


---

## CopyConstants

**Full name:** `neug::common::CopyConstants`


---

## CountZeros

**Full name:** `neug::common::CountZeros`

**Public Methods:**

- `Leading(T value_in)`
- `Trailing(T value_in)`


---

## CountZeros< int128_t >

**Full name:** `neug::common::CountZeros< int128_t >`

**Public Methods:**

- `Leading(int128_t value)`
- `Trailing(int128_t value)`


---

## DataChunk

**Full name:** `neug::common::DataChunk`

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

**Full name:** `neug::common::DataChunkCollection`

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

**Full name:** `neug::common::DataChunkState`

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

**Full name:** `neug::common::Date`

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

**Full name:** `neug::common::DateToStringCast`

**Public Methods:**

- `Length(int32_t date[], uint64_t &yearLength, bool &addBC)`
- `Format(char *data, int32_t date[], uint64_t yearLen, bool addBC)`


---

## DecimalType

**Full name:** `neug::common::DecimalType`

**Public Methods:**

- `getPrecision(const LogicalType &type)`
- `getScale(const LogicalType &type)`
- `insertDecimalPoint(const std::string &value, uint32_t posFromEnd)`


---

## DecimalTypeInfo

**Full name:** `neug::common::DecimalTypeInfo`

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

**Full name:** `neug::common::Deserializer`

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

**Full name:** `neug::common::DropTypeUtils`

**Public Methods:**

- `toString(DropType type)`


---

## ExceptionMessage

**Full name:** `neug::common::ExceptionMessage`

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

**Full name:** `neug::common::ExportCSVConstants`


---

## ExpressionTypeUtil

**Full name:** `neug::common::ExpressionTypeUtil`

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

**Full name:** `neug::common::ExtendDirectionUtil`

**Public Methods:**

- `getRelDataDirection(ExtendDirection direction)`
- `fromString(const std::string &str)`
- `toString(ExtendDirection direction)`


---

## ExtraTypeInfo

**Full name:** `neug::common::ExtraTypeInfo`

**Public Methods:**

- `~ExtraTypeInfo()=default`
- `serialize(Serializer &serializer) const`
- `containsAny() const =0`
- `operator==(const ExtraTypeInfo &other) const =0`
- `copy() const =0`
- `constPtrCast() const`


---

## FileFlags

**Full name:** `neug::common::FileFlags`


---

## FileInfo

**Full name:** `neug::common::FileInfo`

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

**Full name:** `neug::common::FileOpenFlags`

**Public Methods:**

- `FileOpenFlags(int flags)`


---

## FileScanInfo

**Full name:** `neug::common::FileScanInfo`

**Public Methods:**

- `FileScanInfo()`
- `FileScanInfo(FileTypeInfo fileTypeInfo, std::vector< std::stri...`
- `EXPLICIT_COPY_DEFAULT_MOVE(FileScanInfo)`
- `getNumFiles() const`
- `getFilePath(idx_t fileIdx) const`
- `getOption(std::string optionName, T defaultValue) const`


---

## FileSystem

**Full name:** `neug::common::FileSystem`

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

**Full name:** `neug::common::FileTypeInfo`


---

## FileTypeUtils

**Full name:** `neug::common::FileTypeUtils`

**Public Methods:**

- `getFileTypeFromExtension(std::string_view extension)`
- `toString(FileType fileType)`
- `fromString(std::string fileType)`


---

## HashIndexConstants

**Full name:** `neug::common::HashIndexConstants`


---

## HexFormatConstants

**Full name:** `neug::common::HexFormatConstants`


---

## InMemOverflowBuffer

**Full name:** `neug::common::InMemOverflowBuffer`

**Public Methods:**

- `InMemOverflowBuffer(storage::MemoryManager *memoryManager)`
- `DEFAULT_BOTH_MOVE(InMemOverflowBuffer)`
- `allocateSpace(uint64_t size)`
- `merge(InMemOverflowBuffer &other)`
- `resetBuffer()`
- `getMemoryManager()`


---

## Int128_t

**Full name:** `neug::common::Int128_t`

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

**Full name:** `neug::common::InternalKeyword`


---

## Interval

**Full name:** `neug::common::Interval`

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

**Full name:** `neug::common::IntervalToStringCast`

**Public Methods:**

- `FormatSignedNumber(int64_t value, char buffer[], uint64_t &length)`
- `FormatTwoDigits(int64_t value, char buffer[], uint64_t &length)`
- `FormatIntervalValue(int32_t value, char buffer[], uint64_t &length, c...`
- `Format(interval_t interval, char buffer[])`


---

## LimitCounter

**Full name:** `neug::common::LimitCounter`

**Public Methods:**

- `LimitCounter(common::offset_t limitNumber)`
- `increase(common::offset_t number)`
- `exceedLimit() const`


---

## ListAuxiliaryBuffer

**Full name:** `neug::common::ListAuxiliaryBuffer`

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

**Full name:** `neug::common::ListType`

**Public Methods:**

- `getChildType(const LogicalType &type)`


---

## ListTypeInfo

**Full name:** `neug::common::ListTypeInfo`

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

**Full name:** `neug::common::ListVector`

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

**Full name:** `neug::common::LocalFileInfo`

**Public Methods:**

- `LocalFileInfo(std::string path, const int fd, FileSystem *fileSystem)`
- `~LocalFileInfo() override`


---

## LocalFileSystem

**Full name:** `neug::common::LocalFileSystem`

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

**Full name:** `neug::common::LogicalType`

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

**Full name:** `neug::common::LogicalTypeRegistry`

**Public Methods:**

- `LogicalTypeRegistry()`
- `registerType(const YAML::Node &typeYaml, const common::Logical...`
- `getTypeID(const YAML::Node &typeYaml)`
- `containsTypeYaml(const YAML::Node &typeYaml)`


---

## LogicalTypeUtils

**Full name:** `neug::common::LogicalTypeUtils`

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

**Full name:** `neug::common::MD5`

**Public Methods:**

- `addToMD5(const char *z, uint32_t len)`
- `finishMD5()`


---

## Context

**Full name:** `neug::common::MD5::Context`


---

## MPSCQueue

**Full name:** `neug::common::MPSCQueue`

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

**Full name:** `neug::common::MPSCQueue::Node`

**Public Methods:**

- `Node(T data)`


---

## MapType

**Full name:** `neug::common::MapType`

**Public Methods:**

- `getKeyType(const LogicalType &type)`
- `getValueType(const LogicalType &type)`


---

## MapVector

**Full name:** `neug::common::MapVector`

**Public Methods:**

- `getKeyVector(const ValueVector *vector)`
- `getValueVector(const ValueVector *vector)`
- `getMapKeys(const ValueVector *vector, const list_entry_t &listEntry)`
- `getMapValues(const ValueVector *vector, const list_entry_t &listEntry)`


---

## MaybeUninit

**Full name:** `neug::common::MaybeUninit`

**Public Methods:**

- `assumeInit()`
- `assumeInit() const`
- `ptr()`
- `ptr() const`


---

## Metric

**Full name:** `neug::common::Metric`

Note that metrics are not thread safe.

**Public Methods:**

- `Metric(bool enabled)`
- `~Metric()=default`


---

## Mutex

**Full name:** `neug::common::Mutex`

**Public Methods:**

- `Mutex()`
- `Mutex(T data)`
- `DELETE_COPY_AND_MOVE(Mutex)`
- `lock()`
- `try_lock()`


---

## MutexGuard

**Full name:** `neug::common::MutexGuard`

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

**Full name:** `neug::common::NestedVal`

**Public Methods:**

- `getChildrenSize(const Value *val)`
- `getChildVal(const Value *val, uint32_t idx)`


---

## NodeOffsetMaskMap

**Full name:** `neug::common::NodeOffsetMaskMap`

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

**Full name:** `neug::common::NodeVal`

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

**Full name:** `neug::common::NullBuffer`

**Public Methods:**

- `isNull(const uint8_t *nullBytes, uint64_t valueIdx)`
- `setNull(uint8_t *nullBytes, uint64_t valueIdx)`
- `setNoNull(uint8_t *nullBytes, uint64_t valueIdx)`
- `getNumBytesForNullValues(uint64_t numValues)`
- `initNullBytes(uint8_t *nullBytes, uint64_t numValues)`


---

## NullMask

**Full name:** `neug::common::NullMask`

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

**Full name:** `neug::common::NumericHelper`

`NumericHelper` is a `static` class that holds helper functions for integers`/doubles`

**Public Methods:**

- `FormatUnsigned(T value, char *ptr)`
- `getUnsignedInt64Length(uint64_t value)`


---

## NumericMetric

**Full name:** `neug::common::NumericMetric`

**Public Methods:**

- `NumericMetric(bool enable)`
- `increase(uint64_t value)`
- `incrementByOne()`


---

## OrderByConstants

**Full name:** `neug::common::OrderByConstants`


---

## ParquetConstants

**Full name:** `neug::common::ParquetConstants`


---

## PathSemanticUtils

**Full name:** `neug::common::PathSemanticUtils`

**Public Methods:**

- `fromString(const std::string &str)`
- `toString(PathSemantic semantic)`


---

## PhysicalTypeUtils

**Full name:** `neug::common::PhysicalTypeUtils`

**Public Methods:**

- `toString(PhysicalTypeID physicalType)`
- `getFixedTypeSize(PhysicalTypeID physicalType)`


---

## PlannerKnobs

**Full name:** `neug::common::PlannerKnobs`


---

## PortDBConstants

**Full name:** `neug::common::PortDBConstants`


---

## Profiler

**Full name:** `neug::common::Profiler`

**Public Methods:**

- `registerTimeMetric(const std::string &key)`
- `registerNumericMetric(const std::string &key)`
- `sumAllTimeMetricsWithKey(const std::string &key)`
- `sumAllNumericMetricsWithKey(const std::string &key)`


---

## ProgressBar

**Full name:** `neug::common::ProgressBar`

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

**Full name:** `neug::common::ProgressBarDisplay`

Interface for displaying progress of a pipeline and a query.

**Public Methods:**

- `ProgressBarDisplay()`
- `~ProgressBarDisplay()=default`
- `updateProgress(uint64_t queryID, double newPipelineProgress, uin...`
- `finishProgress(uint64_t queryID)=0`
- `setNumPipelines(uint32_t newNumPipelines)`


---

## QueryRelTypeUtils

**Full name:** `neug::common::QueryRelTypeUtils`

**Public Methods:**

- `isRecursive(QueryRelType type)`
- `isWeighted(QueryRelType type)`
- `getPathSemantic(QueryRelType queryRelType)`
- `getFunction(QueryRelType type)`


---

## RandomEngine

**Full name:** `neug::common::RandomEngine`

**Public Methods:**

- `RandomEngine()`
- `RandomEngine(uint64_t seed, uint64_t stream)`
- `nextRandomInteger()`
- `nextRandomInteger(uint32_t upper)`


---

## RandomState

**Full name:** `neug::common::RandomState`

**Public Methods:**

- `RandomState()`


---

## Reader

**Full name:** `neug::common::Reader`

**Public Methods:**

- `read(uint8_t *data, uint64_t size)=0`
- `~Reader()=default`
- `finished()=0`


---

## RecursiveRelVal

**Full name:** `neug::common::RecursiveRelVal`

`RecursiveRelVal` represents a path in the graph and stores the corresponding rels and nodes of that path.

**Public Methods:**

- `getNodes(const Value *val)`
- `getRels(const Value *val)`


---

## RelDirectionUtils

**Full name:** `neug::common::RelDirectionUtils`

**Public Methods:**

- `getOppositeDirection(RelDataDirection direction)`
- `relDirectionToString(RelDataDirection direction)`
- `relDirectionToKeyIdx(RelDataDirection direction)`


---

## RelMultiplicityUtils

**Full name:** `neug::common::RelMultiplicityUtils`

**Public Methods:**

- `getFwd(const std::string &str)`
- `getBwd(const std::string &str)`
- `toString(RelMultiplicity multiplicity)`


---

## RelVal

**Full name:** `neug::common::RelVal`

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

**Full name:** `neug::common::ScanSourceTypeUtils`

**Public Methods:**

- `toString(ScanSourceType type)`


---

## ScheduledTask

**Full name:** `neug::common::ScheduledTask`

**Public Methods:**

- `ScheduledTask(std::shared_ptr< Task > task, uint64_t ID)`


---

## SelectionVector

**Full name:** `neug::common::SelectionVector`

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

**Full name:** `neug::common::SelectionView`

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

**Full name:** `neug::common::SemiMask`

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

**Full name:** `neug::common::Serializer`

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

**Full name:** `neug::common::StaticVector`

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

**Full name:** `neug::common::StorageConstants`


---

## StreamData

**Full name:** `neug::common::StreamData`


---

## StreamWrapper

**Full name:** `neug::common::StreamWrapper`

**Public Methods:**

- `~StreamWrapper()=default`
- `initialize(CompressedFileInfo &file)=0`
- `read(StreamData &stream_data)=0`
- `close()=0`


---

## StringAuxiliaryBuffer

**Full name:** `neug::common::StringAuxiliaryBuffer`

**Public Methods:**

- `StringAuxiliaryBuffer(storage::MemoryManager *memoryManager)`
- `getOverflowBuffer() const`
- `allocateOverflow(uint64_t size)`
- `resetOverflowBuffer() const`


---

## StringUtils

**Full name:** `neug::common::StringUtils`

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

**Full name:** `neug::common::StringUtils::string_hash`

**Public Methods:**

- `operator()(const char *str) const`
- `operator()(std::string_view str) const`
- `operator()(std::string const &str) const`


---

## StringVector

**Full name:** `neug::common::StringVector`

**Public Methods:**

- `getInMemOverflowBuffer(ValueVector *vector)`
- `addString(ValueVector *vector, uint32_t vectorPos, neug_str...`
- `addString(ValueVector *vector, uint32_t vectorPos, const ch...`
- `addString(ValueVector *vector, uint32_t vectorPos, const st...`
- `reserveString(ValueVector *vector, uint32_t vectorPos, uint64_t length)`
- `reserveString(ValueVector *vector, neug_string_t &dstStr, uint6...`
- `addString(ValueVector *vector, neug_string_t &dstStr, neug_...`
- `addString(ValueVector *vector, neug_string_t &dstStr, const...`
- `addString(neug::common::ValueVector *vector, neug_string_t ...`
- `copyToRowData(const ValueVector *vector, uint32_t pos, uint8_t ...`


---

## StructAuxiliaryBuffer

**Full name:** `neug::common::StructAuxiliaryBuffer`

**Public Methods:**

- `StructAuxiliaryBuffer(const LogicalType &type, storage::MemoryManager *...`
- `referenceChildVector(idx_t idx, std::shared_ptr< ValueVector > vectorT...`
- `getFieldVectors() const`
- `getFieldVectorShared(idx_t idx) const`
- `getFieldVectorPtr(idx_t idx) const`


---

## StructField

**Full name:** `neug::common::StructField`

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

**Full name:** `neug::common::StructType`

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

**Full name:** `neug::common::StructTypeInfo`

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

**Full name:** `neug::common::StructVector`

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

**Full name:** `neug::common::TableOptionConstants`


---

## TableTypeUtils

**Full name:** `neug::common::TableTypeUtils`

**Public Methods:**

- `toString(TableType tableType)`


---

## Task

**Full name:** `neug::common::Task`

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

**Full name:** `neug::common::TaskScheduler`

`TaskScheduler` is a library that manages a set of worker threads that can execute tasks that are put into a task queue.

Each task accepts a maximum number of threads. Users of `TaskScheduler` schedule tasks to be executed by calling schedule functions, e.g., pushTaskIntoQueue or scheduleTaskAndWaitOrError. New tasks ar...

**Public Methods:**

- `TaskScheduler(uint64_t numWorkerThreads)`
- `~TaskScheduler()`
- `scheduleTaskAndWaitOrError(const std::shared_ptr< Task > &task, processor::E...`


---

## TerminalProgressBarDisplay

**Full name:** `neug::common::TerminalProgressBarDisplay`

A class that displays a progress bar in the terminal.

**Public Methods:**

- `updateProgress(uint64_t queryID, double newPipelineProgress, uin...`
- `finishProgress(uint64_t queryID) override`


---

## Time

**Full name:** `neug::common::Time`

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

**Full name:** `neug::common::TimeMetric`

**Public Methods:**

- `TimeMetric(bool enable)`
- `start()`
- `stop()`
- `getElapsedTimeMS() const`


---

## TimeToStringCast

**Full name:** `neug::common::TimeToStringCast`

**Public Methods:**

- `FormatMicros(uint32_t microseconds, char micro_buffer[])`
- `Length(int32_t time[], char micro_buffer[])`
- `FormatTwoDigits(char *ptr, int32_t value)`
- `Format(char *data, uint64_t length, int32_t time[], char...`


---

## Timer

**Full name:** `neug::common::Timer`

**Public Methods:**

- `start()`
- `stop()`
- `getDuration() const`
- `getElapsedTimeInMS() const`


---

## Timestamp

**Full name:** `neug::common::Timestamp`

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

**Full name:** `neug::common::TypeUtils`

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

**Full name:** `neug::common::UDTTypeInfo`

**Public Methods:**

- `UDTTypeInfo(std::string typeName)`
- `getTypeName() const`
- `containsAny() const override`
- `operator==(const ExtraTypeInfo &other) const override`
- `copy() const override`
- `deserialize(Deserializer &deserializer)`


---

## UUID

**Full name:** `neug::common::UUID`

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

**Full name:** `neug::common::UnionType`

**Public Methods:**

- `getInternalFieldIdx(union_field_idx_t idx)`
- `getFieldName(const LogicalType &type, union_field_idx_t idx)`
- `getFieldType(const LogicalType &type, union_field_idx_t idx)`
- `getNumFields(const LogicalType &type)`


---

## UnionVector

**Full name:** `neug::common::UnionVector`

**Public Methods:**

- `getTagVector(const ValueVector *vector)`
- `getValVector(const ValueVector *vector, union_field_idx_t fieldIdx)`
- `referenceVector(ValueVector *vector, union_field_idx_t fieldIdx, ...`
- `setTagField(ValueVector &vector, SelectionVector &sel, union_...`


---

## UniqLock

**Full name:** `neug::common::UniqLock`

**Public Methods:**

- `UniqLock()`
- `UniqLock(std::mutex &mtx)`
- `UniqLock(const UniqLock &)=delete`
- `operator=(const UniqLock &)=delete`
- `UniqLock(UniqLock &&other) noexcept`
- `operator=(UniqLock &&other) noexcept`
- `isLocked() const`


---

## VFSHolder

**Full name:** `neug::common::VFSHolder`

**Public Methods:**

- `VFSHolder()=delete`
- `setVFS(VirtualFileSystem *fileSystem)`
- `getVFS()`


---

## Value

**Full name:** `neug::common::Value`

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

**Full name:** `neug::common::ValueVector`

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

**Full name:** `neug::common::VirtualFileSystem`

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

**Full name:** `neug::common::WarningConstants`


---

## Writer

**Full name:** `neug::common::Writer`

**Public Methods:**

- `write(const uint8_t *data, uint64_t size)=0`
- `~Writer()=default`


---

## blob_t

**Full name:** `neug::common::blob_t`


---

## date_t

**Full name:** `neug::common::date_t`

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

**Full name:** `neug::common::dtime_t`

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

**Full name:** `neug::common::int128_t`

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

**Full name:** `neug::common::internalID_t`

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

**Full name:** `neug::common::interval_t`

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

## list_entry_t

**Full name:** `neug::common::list_entry_t`

**Public Methods:**

- `list_entry_t()`
- `list_entry_t(offset_t offset, list_size_t size)`


---

## map_entry_t

**Full name:** `neug::common::map_entry_t`


---

## neug_list_t

**Full name:** `neug::common::neug_list_t`

**Public Methods:**

- `neug_list_t()`
- `neug_list_t(uint64_t size, uint64_t overflowPtr)`
- `set(const uint8_t *values, const LogicalType &dataType) const`


---

## neug_string_t

**Full name:** `neug::common::neug_string_t`

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

**Full name:** `neug::common::neug_uuid_t`


---

## MakeSigned

**Full name:** `neug::common::numeric_utils::MakeSigned`


---

## MakeSigned< int128_t >

**Full name:** `neug::common::numeric_utils::MakeSigned< int128_t >`


---

## MakeUnSigned

**Full name:** `neug::common::numeric_utils::MakeUnSigned`


---

## MakeUnSigned< int128_t >

**Full name:** `neug::common::numeric_utils::MakeUnSigned< int128_t >`


---

## overflow_value_t

**Full name:** `neug::common::overflow_value_t`


---

## overload

**Full name:** `neug::common::overload`

**Public Methods:**

- `overload(Funcs... funcs)`


---

## struct_entry_t

**Full name:** `neug::common::struct_entry_t`


---

## timestamp_ms_t

**Full name:** `neug::common::timestamp_ms_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_ns_t

**Full name:** `neug::common::timestamp_ns_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_sec_t

**Full name:** `neug::common::timestamp_sec_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## timestamp_t

**Full name:** `neug::common::timestamp_t`

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

**Full name:** `neug::common::timestamp_tz_t`

**Public Methods:**

- `timestamp_t()`
- `timestamp_t(int64_t value_p)`


---

## union_entry_t

**Full name:** `neug::common::union_entry_t`


---

## EvaluatorLocalState

**Full name:** `neug::evaluator::EvaluatorLocalState`


---

## ExpressionEvaluator

**Full name:** `neug::evaluator::ExpressionEvaluator`

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

**Full name:** `neug::evaluator::ExpressionEvaluatorUtils`

**Public Methods:**

- `evaluateConstantExpression(std::shared_ptr< binder::Expression > expression,...`


---

## FunctionExpressionEvaluator

**Full name:** `neug::evaluator::FunctionExpressionEvaluator`

**Public Methods:**

- `FunctionExpressionEvaluator(std::shared_ptr< binder::Expression > expression,...`
- `evaluate() override`
- `evaluate(common::sel_t count) override`
- `selectInternal(common::SelectionVector &selVector) override`
- `copy() override`


---

## LiteralExpressionEvaluator

**Full name:** `neug::evaluator::LiteralExpressionEvaluator`

**Public Methods:**

- `LiteralExpressionEvaluator(std::shared_ptr< binder::Expression > expression,...`
- `evaluate() override`
- `evaluate(common::sel_t count) override`
- `selectInternal(common::SelectionVector &selVector) override`
- `copy() override`


---

## BinderException

**Full name:** `neug::exception::BinderException`

**Public Methods:**

- `BinderException(const std::string &msg)`
- `BinderException(const std::string &msg, const std::string &file_line)`


---

## CatalogException

**Full name:** `neug::exception::CatalogException`

**Public Methods:**

- `CatalogException(const std::string &msg)`
- `CatalogException(const std::string &msg, const std::string &file_line)`


---

## CheckpointException

**Full name:** `neug::exception::CheckpointException`

**Public Methods:**

- `CheckpointException(const std::exception &e)`
- `CheckpointException(const std::string &msg)`


---

## ConnectionException

**Full name:** `neug::exception::ConnectionException`

**Public Methods:**

- `ConnectionException(const std::string &msg)`
- `ConnectionException(const std::string &msg, const std::string &file_line)`


---

## ConversionException

**Full name:** `neug::exception::ConversionException`

**Public Methods:**

- `ConversionException(const std::string &msg)`
- `ConversionException(const std::string &msg, const std::string &file_line)`


---

## CopyException

**Full name:** `neug::exception::CopyException`

**Public Methods:**

- `CopyException(const std::string &msg)`
- `CopyException(const std::string &msg, const std::string &file_line)`


---

## DatabaseLockedException

**Full name:** `neug::exception::DatabaseLockedException`

**Public Methods:**

- `DatabaseLockedException(const std::string &msg)`
- `DatabaseLockedException(const std::string &msg, const std::string &file_line)`


---

## Exception

**Full name:** `neug::exception::Exception`

**Public Methods:**

- `Exception(std::string msg, neug::StatusCode error_code)`
- `Exception(std::string msg, std::string file_line)`
- `Exception(std::string msg, std::string file_line, neug::Sta...`
- `what() const noexcept override`


---

## ExtensionException

**Full name:** `neug::exception::ExtensionException`

**Public Methods:**

- `ExtensionException(const std::string &msg)`
- `ExtensionException(const std::string &msg, const std::string &file_line)`


---

## IOException

**Full name:** `neug::exception::IOException`

**Public Methods:**

- `IOException(const std::string &msg)`
- `IOException(const std::string &msg, const std::string &file_line)`


---

## IndexException

**Full name:** `neug::exception::IndexException`

**Public Methods:**

- `IndexException(const std::string &msg)`
- `IndexException(const std::string &msg, const std::string &file_line)`


---

## InternalException

**Full name:** `neug::exception::InternalException`

**Public Methods:**

- `InternalException(const std::string &msg)`
- `InternalException(const std::string &msg, const std::string &file_line)`


---

## InterruptException

**Full name:** `neug::exception::InterruptException`

**Public Methods:**

- `InterruptException()`
- `InterruptException(const std::string &msg, const std::string &file_line)`


---

## InvalidArgumentException

**Full name:** `neug::exception::InvalidArgumentException`

**Public Methods:**

- `InvalidArgumentException(const std::string &msg)`
- `InvalidArgumentException(const std::string &msg, const std::string &file_line)`


---

## NotFoundException

**Full name:** `neug::exception::NotFoundException`

**Public Methods:**

- `NotFoundException(const std::string &msg)`
- `NotFoundException(const std::string &msg, const std::string &file_line)`


---

## NotImplementedException

**Full name:** `neug::exception::NotImplementedException`

**Public Methods:**

- `NotImplementedException(const std::string &msg)`
- `NotImplementedException(const std::string &msg, const std::string &file_line)`


---

## NotSupportedException

**Full name:** `neug::exception::NotSupportedException`

**Public Methods:**

- `NotSupportedException(const std::string &msg)`
- `NotSupportedException(const std::string &msg, const std::string &file_line)`


---

## OverflowException

**Full name:** `neug::exception::OverflowException`

**Public Methods:**

- `OverflowException(const std::string &msg)`
- `OverflowException(const std::string &msg, const std::string &file_line)`


---

## ParserException

**Full name:** `neug::exception::ParserException`

**Public Methods:**

- `ParserException(const std::string &msg)`
- `ParserException(const std::string &msg, const std::string &file_line)`


---

## PermissionDeniedException

**Full name:** `neug::exception::PermissionDeniedException`

**Public Methods:**

- `PermissionDeniedException(const std::string &msg)`
- `PermissionDeniedException(const std::string &msg, const std::string &file_line)`


---

## PropertyNotFoundException

**Full name:** `neug::exception::PropertyNotFoundException`

**Public Methods:**

- `PropertyNotFoundException(const std::string &msg)`
- `PropertyNotFoundException(const std::string &msg, const std::string &file_line)`


---

## QueryExecutionError

**Full name:** `neug::exception::QueryExecutionError`

**Public Methods:**

- `QueryExecutionError(const std::string &msg)`
- `QueryExecutionError(const std::string &msg, const std::string &file_line)`


---

## RuntimeError

**Full name:** `neug::exception::RuntimeError`

**Public Methods:**

- `RuntimeError(const std::string &msg)`
- `RuntimeError(const std::string &msg, const std::string &file_line)`


---

## SchemaMismatchException

**Full name:** `neug::exception::SchemaMismatchException`

**Public Methods:**

- `SchemaMismatchException(const std::string &msg)`
- `SchemaMismatchException(const std::string &msg, const std::string &file_line)`


---

## StorageException

**Full name:** `neug::exception::StorageException`

**Public Methods:**

- `StorageException(const std::string &msg)`
- `StorageException(const std::string &msg, const std::string &file_line)`


---

## TestException

**Full name:** `neug::exception::TestException`

**Public Methods:**

- `TestException(const std::string &msg)`
- `TestException(const std::string &msg, const std::string &file_line)`


---

## TransactionManagerException

**Full name:** `neug::exception::TransactionManagerException`

**Public Methods:**

- `TransactionManagerException(const std::string &msg)`
- `TransactionManagerException(const std::string &msg, const std::string &file_line)`


---

## TxStateConflictException

**Full name:** `neug::exception::TxStateConflictException`

**Public Methods:**

- `TxStateConflictException(const std::string &msg)`
- `TxStateConflictException(const std::string &msg, const std::string &file_line)`


---

## CatalogExtension

**Full name:** `neug::extension::CatalogExtension`

**Public Methods:**

- `CatalogExtension()`
- `init()=0`
- `invalidateCache()`


---

## Extension

**Full name:** `neug::extension::Extension`

**Public Methods:**

- `~Extension()=default`


---

## ExtensionAPI

**Full name:** `neug::extension::ExtensionAPI`

**Public Methods:**

- `setCatalog(neug::catalog::Catalog *catalog)`
- `registerFunction(catalog::CatalogEntryType entryType)`
- `registerExtension(const ExtensionInfo &info)`
- `getLoadedExtensions()`


---

## ExtensionAuxInfo

**Full name:** `neug::extension::ExtensionAuxInfo`

**Public Methods:**

- `ExtensionAuxInfo(ExtensionAction action, std::string path)`
- `~ExtensionAuxInfo()=default`
- `contCast() const`
- `copy()`


---

## ExtensionEntry

**Full name:** `neug::extension::ExtensionEntry`


---

## ExtensionInfo

**Full name:** `neug::extension::ExtensionInfo`

Basic information for an extension.

`ExtensionInfo` describes the metadata of an extension. @field name Unique name of the extension. @field description Brief description of the extension's functionality.


---

## ExtensionInstaller

**Full name:** `neug::extension::ExtensionInstaller`

**Public Methods:**

- `ExtensionInstaller(const InstallExtensionInfo &info, main::ClientCon...`
- `~ExtensionInstaller()=default`
- `install()`


---

## ExtensionLibLoader

**Full name:** `neug::extension::ExtensionLibLoader`

**Public Methods:**

- `ExtensionLibLoader(const std::string &extensionName, const std::string &path)`
- `getLoadFunc()`
- `getInitFunc()`
- `getNameFunc()`
- `getInstallFunc()`


---

## ExtensionLoader

**Full name:** `neug::extension::ExtensionLoader`

**Public Methods:**

- `ExtensionLoader(std::string extensionName)`
- `~ExtensionLoader()=default`
- `loadDependency(main::ClientContext *context)=0`


---

## ExtensionManager

**Full name:** `neug::extension::ExtensionManager`

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

**Full name:** `neug::extension::ExtensionRepoInfo`


---

## ExtensionSourceUtils

**Full name:** `neug::extension::ExtensionSourceUtils`

**Public Methods:**

- `toString(ExtensionSource source)`


---

## ExtensionUtils

**Full name:** `neug::extension::ExtensionUtils`

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

**Full name:** `neug::extension::InstallExtensionAuxInfo`

**Public Methods:**

- `InstallExtensionAuxInfo(std::string extensionRepo, std::string path)`
- `copy() override`


---

## InstallExtensionInfo

**Full name:** `neug::extension::InstallExtensionInfo`


---

## LoadedExtension

**Full name:** `neug::extension::LoadedExtension`

**Public Methods:**

- `LoadedExtension(std::string extensionName, std::string fullPath, ...`
- `getExtensionName() const`
- `getFullPath() const`
- `getSource() const`
- `toCypher()`


---

## Abs

**Full name:** `neug::function::Abs`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(int8_t &input, int8_t &result)`
- `operation(int16_t &input, int16_t &result)`
- `operation(int32_t &input, int32_t &result)`
- `operation(int64_t &input, int64_t &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## AbsFunction

**Full name:** `neug::function::AbsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Acos

**Full name:** `neug::function::Acos`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Add

**Full name:** `neug::function::Add`

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

**Full name:** `neug::function::AddFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateAvgFunction

**Full name:** `neug::function::AggregateAvgFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateFunction

**Full name:** `neug::function::AggregateFunction`

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

**Full name:** `neug::function::AggregateFunctionUtils`

**Public Methods:**

- `getAggFunc(std::string name, common::LogicalTypeID inputType...`
- `appendSumOrAvgFuncs(std::string name, common::LogicalTypeID inputType...`


---

## AggregateMaxFunction

**Full name:** `neug::function::AggregateMaxFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateMinFunction

**Full name:** `neug::function::AggregateMinFunction`

**Public Methods:**

- `getFunctionSet()`


---

## AggregateState

**Full name:** `neug::function::AggregateState`

**Public Methods:**

- `getStateSize() const =0`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos)=0`
- `~AggregateState()=default`


---

## AggregateSumFunction

**Full name:** `neug::function::AggregateSumFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ArrayExtract

**Full name:** `neug::function::ArrayExtract`

**Public Methods:**

- `operation(common::neug_string_t &str, int64_t &idx, common:...`
- `copySubstr(common::neug_string_t &src, int64_t start, int64_...`


---

## Asin

**Full name:** `neug::function::Asin`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Atan

**Full name:** `neug::function::Atan`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Atan2

**Full name:** `neug::function::Atan2`

**Public Methods:**

- `operation(A &left, B &right, double &result)`


---

## AvgFunction

**Full name:** `neug::function::AvgFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(AvgState< RESULT_TYPE > *state, common::ValueVect...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *state_)`


---

## AvgState

**Full name:** `neug::function::AvgState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`
- `finalize()`
- `finalize()`


---

## BaseCountFunction

**Full name:** `neug::function::BaseCountFunction`

**Public Methods:**

- `initialize()`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## CountState

**Full name:** `neug::function::BaseCountFunction::CountState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`


---

## BaseListSortOperation

**Full name:** `neug::function::BaseListSortOperation`

**Public Methods:**

- `isAscOrder(const std::string &sortOrder)`
- `isNullFirst(const std::string &nullOrder)`
- `sortValues(common::list_entry_t &input, common::list_entry_t...`
- `setVectorRangeToNull(common::ValueVector &vector, uint64_t offset, uin...`


---

## BaseLowerUpperFunction

**Full name:** `neug::function::BaseLowerUpperFunction`

**Public Methods:**

- `operation(common::neug_string_t &input, common::neug_string...`
- `convertCharCase(char *result, const char *input, int32_t charPos,...`
- `convertCase(char *result, uint32_t len, char *input, bool toUpper)`
- `getResultLen(char *inputStr, uint32_t inputLen, bool isUpper)`


---

## BinaryComparisonFunctionWrapper

**Full name:** `neug::function::BinaryComparisonFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryFunctionExecutor

**Full name:** `neug::function::BinaryFunctionExecutor`

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

**Full name:** `neug::function::BinaryFunctionExecutor::BinaryComparisonSelectWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinarySelectWrapper

**Full name:** `neug::function::BinaryFunctionExecutor::BinarySelectWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinaryFunctionWrapper

**Full name:** `neug::function::BinaryFunctionWrapper`

Binary operator assumes function with null returns null.

This does NOT applies to binary boolean operations (e.g. AND, OR, XOR).

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryListExtractFunctionWrapper

**Full name:** `neug::function::BinaryListExtractFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryListStructFunctionWrapper

**Full name:** `neug::function::BinaryListStructFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryMapCreationFunctionWrapper

**Full name:** `neug::function::BinaryMapCreationFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinarySelectWithBindDataWrapper

**Full name:** `neug::function::BinarySelectWithBindDataWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, uint8_t &resu...`


---

## BinaryStringFunctionWrapper

**Full name:** `neug::function::BinaryStringFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BinaryUDFFunctionWrapper

**Full name:** `neug::function::BinaryUDFFunctionWrapper`

**Public Methods:**

- `operation(LEFT_TYPE &left, RIGHT_TYPE &right, RESULT_TYPE &...`


---

## BitShiftLeft

**Full name:** `neug::function::BitShiftLeft`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitShiftLeftFunction

**Full name:** `neug::function::BitShiftLeftFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitShiftRight

**Full name:** `neug::function::BitShiftRight`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitShiftRightFunction

**Full name:** `neug::function::BitShiftRightFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseAnd

**Full name:** `neug::function::BitwiseAnd`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitwiseAndFunction

**Full name:** `neug::function::BitwiseAndFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseOr

**Full name:** `neug::function::BitwiseOr`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BitwiseOrFunction

**Full name:** `neug::function::BitwiseOrFunction`

**Public Methods:**

- `getFunctionSet()`


---

## BitwiseXor

**Full name:** `neug::function::BitwiseXor`

**Public Methods:**

- `operation(int64_t &left, int64_t &right, int64_t &result)`


---

## BuiltInFunctionsUtils

**Full name:** `neug::function::BuiltInFunctionsUtils`

**Public Methods:**

- `matchFunction(const std::string &name, const catalog::FunctionC...`
- `matchFunction(const std::string &name, const std::vector< commo...`
- `matchAggregateFunction(const std::string &name, const std::vector< commo...`
- `getCastCost(common::LogicalTypeID inputTypeID, common::Logica...`


---

## CSVReadFunction

**Full name:** `neug::function::CSVReadFunction`

**Public Methods:**

- `getFunctionSet()`
- `validateAndConvertExecOptions(std::shared_ptr< reader::ReadSharedState > state)`
- `validateAndConvertSniffOptions(reader::FileSchema &schema)`
- `execFunc(std::shared_ptr< reader::ReadSharedState > state)`
- `sniffFunc(const reader::FileSchema &schema)`


---

## CallFuncInputBase

**Full name:** `neug::function::CallFuncInputBase`

**Public Methods:**

- `~CallFuncInputBase()=default`


---

## CastAnyFunction

**Full name:** `neug::function::CastAnyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastArrayHelper

**Full name:** `neug::function::CastArrayHelper`

**Public Methods:**

- `checkCompatibleNestedTypes(LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID)`
- `containsListToArray(const LogicalType &srcType, const LogicalType &dstType)`
- `validateListEntry(ValueVector *inputVector, const LogicalType &resu...`


---

## CastBetweenDecimal

**Full name:** `neug::function::CastBetweenDecimal`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &input...`


---

## CastBetweenTimestamp

**Full name:** `neug::function::CastBetweenTimestamp`

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

**Full name:** `neug::function::CastDateToTimestamp`

**Public Methods:**

- `operation(common::date_t &input, T &result)`
- `operation(common::date_t &input, common::timestamp_ns_t &result)`
- `operation(common::date_t &input, common::timestamp_ms_t &result)`
- `operation(common::date_t &input, common::timestamp_sec_t &result)`


---

## CastDecimalTo

**Full name:** `neug::function::CastDecimalTo`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &input...`
- `operation(int16_t &input, neug_string_t &output, const Valu...`
- `operation(int32_t &input, neug_string_t &output, const Valu...`
- `operation(int64_t &input, neug_string_t &output, const Valu...`
- `operation(common::int128_t &input, neug_string_t &output, c...`


---

## CastFunction

**Full name:** `neug::function::CastFunction`

**Public Methods:**

- `hasImplicitCast(const common::LogicalType &srcType, const common:...`
- `bindCastFunction(const std::string &functionName, const common::Lo...`


---

## CastFunctionBindData

**Full name:** `neug::function::CastFunctionBindData`

**Public Methods:**

- `CastFunctionBindData(common::LogicalType dataType)`
- `copy() const override`


---

## CastNodeToString

**Full name:** `neug::function::CastNodeToString`

**Public Methods:**

- `operation(common::struct_entry_t &input, common::neug_strin...`


---

## CastRelToString

**Full name:** `neug::function::CastRelToString`

**Public Methods:**

- `operation(common::struct_entry_t &input, common::neug_strin...`


---

## CastString

**Full name:** `neug::function::CastString`

**Public Methods:**

- `copyStringToVector(ValueVector *vector, uint64_t vectorPos, std::str...`
- `tryCast(const neug_string_t &input, T &result)`
- `operation(const neug_string_t &input, T &result, ValueVecto...`
- `operation(const neug_string_t &input, int128_t &result, Val...`
- `operation(const neug_string_t &input, int32_t &result, Valu...`
- `operation(const neug_string_t &input, int16_t &result, Valu...`
- `operation(const neug_string_t &input, int8_t &result, Value...`
- `operation(const neug_string_t &input, uint64_t &result, Val...`
- `operation(const neug_string_t &input, uint32_t &result, Val...`
- `operation(const neug_string_t &input, uint16_t &result, Val...`
- ... and 17 more methods


---

## CastToDate

**Full name:** `neug::function::CastToDate`

**Public Methods:**

- `operation(T &input, common::date_t &result)`
- `operation(common::timestamp_t &input, common::date_t &result)`
- `operation(common::timestamp_ns_t &input, common::date_t &result)`
- `operation(common::timestamp_ms_t &input, common::date_t &result)`
- `operation(common::timestamp_sec_t &input, common::date_t &result)`


---

## CastToDateFunction

**Full name:** `neug::function::CastToDateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToDecimal

**Full name:** `neug::function::CastToDecimal`

**Public Methods:**

- `operation(SRC &input, DST &output, const ValueVector &, con...`
- `operation(neug_string_t &input, int16_t &output, const Valu...`
- `operation(neug_string_t &input, int32_t &output, const Valu...`
- `operation(neug_string_t &input, int64_t &output, const Valu...`
- `operation(neug_string_t &input, common::int128_t &output, c...`


---

## CastToDouble

**Full name:** `neug::function::CastToDouble`

**Public Methods:**

- `operation(T &input, double &result)`
- `operation(common::int128_t &input, double &result)`


---

## CastToFloat

**Full name:** `neug::function::CastToFloat`

**Public Methods:**

- `operation(T &input, float &result)`
- `operation(common::int128_t &input, float &result)`


---

## CastToInt128

**Full name:** `neug::function::CastToInt128`

**Public Methods:**

- `operation(T &input, common::int128_t &result)`


---

## CastToInt16

**Full name:** `neug::function::CastToInt16`

**Public Methods:**

- `operation(T &input, int16_t &result)`
- `operation(common::int128_t &input, int16_t &result)`


---

## CastToInt32

**Full name:** `neug::function::CastToInt32`

**Public Methods:**

- `operation(T &input, int32_t &result)`
- `operation(common::int128_t &input, int32_t &result)`


---

## CastToInt64

**Full name:** `neug::function::CastToInt64`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::int128_t &input, int64_t &result)`


---

## CastToInt8

**Full name:** `neug::function::CastToInt8`

**Public Methods:**

- `operation(T &input, int8_t &result)`
- `operation(common::int128_t &input, int8_t &result)`


---

## CastToIntervalFunction

**Full name:** `neug::function::CastToIntervalFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToSerial

**Full name:** `neug::function::CastToSerial`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::int128_t &input, int64_t &result)`


---

## CastToString

**Full name:** `neug::function::CastToString`

**Public Methods:**

- `operation(T &input, common::neug_string_t &result, common::...`


---

## CastToTimestampFunction

**Full name:** `neug::function::CastToTimestampFunction`

**Public Methods:**

- `getFunctionSet()`


---

## CastToUInt16

**Full name:** `neug::function::CastToUInt16`

**Public Methods:**

- `operation(T &input, uint16_t &result)`
- `operation(common::int128_t &input, uint16_t &result)`


---

## CastToUInt32

**Full name:** `neug::function::CastToUInt32`

**Public Methods:**

- `operation(T &input, uint32_t &result)`
- `operation(common::int128_t &input, uint32_t &result)`


---

## CastToUInt64

**Full name:** `neug::function::CastToUInt64`

**Public Methods:**

- `operation(T &input, uint64_t &result)`
- `operation(common::int128_t &input, uint64_t &result)`


---

## CastToUInt8

**Full name:** `neug::function::CastToUInt8`

**Public Methods:**

- `operation(T &input, uint8_t &result)`
- `operation(common::int128_t &input, uint8_t &result)`


---

## Cbrt

**Full name:** `neug::function::Cbrt`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Ceil

**Full name:** `neug::function::Ceil`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## Century

**Full name:** `neug::function::Century`

**Public Methods:**

- `operation(common::timestamp_t &timestamp, int64_t &result)`


---

## CollectFunction

**Full name:** `neug::function::CollectFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ComparisonFunction

**Full name:** `neug::function::ComparisonFunction`

**Public Methods:**

- `getFunctionSet(const std::string &name)`


---

## ConstFunctionExecutor

**Full name:** `neug::function::ConstFunctionExecutor`

**Public Methods:**

- `execute(common::ValueVector &result, common::SelectionVector &sel)`


---

## ContainsFunction

**Full name:** `neug::function::ContainsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Cos

**Full name:** `neug::function::Cos`

**Public Methods:**

- `operation(T &input, double &result)`


---

## CostFunction

**Full name:** `neug::function::CostFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Cot

**Full name:** `neug::function::Cot`

**Public Methods:**

- `operation(T &input, double &result)`


---

## CountFunction

**Full name:** `neug::function::CountFunction`

**Public Methods:**

- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *, uint64_t ...`
- `paramRewriteFunc(binder::expression_vector &arguments)`
- `getFunctionSet()`


---

## CountStarFunction

**Full name:** `neug::function::CountStarFunction`

**Public Methods:**

- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `getFunctionSet()`


---

## CurrValFunction

**Full name:** `neug::function::CurrValFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DateFunction

**Full name:** `neug::function::DateFunction`


---

## DatePart

**Full name:** `neug::function::DatePart`

**Public Methods:**

- `operation(LEFT_TYPE &, RIGHT_TYPE &, int64_t &)`


---

## DatePartFunction

**Full name:** `neug::function::DatePartFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DatePartFunctionAlias

**Full name:** `neug::function::DatePartFunctionAlias`


---

## DefaultRJAlgorithm

**Full name:** `neug::function::DefaultRJAlgorithm`

**Public Methods:**

- `getFunctionName() const override`
- `getResultColumns(const RJBindData &bindData) const override`
- `copy() const override`


---

## Degrees

**Full name:** `neug::function::Degrees`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Divide

**Full name:** `neug::function::Divide`

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

**Full name:** `neug::function::DivideFunction`

**Public Methods:**

- `getFunctionSet()`


---

## DurationFunction

**Full name:** `neug::function::DurationFunction`


---

## EndNodeFunction

**Full name:** `neug::function::EndNodeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## EndsWithFunction

**Full name:** `neug::function::EndsWithFunction`

**Public Methods:**

- `getFunctionSet()`


---

## EpochMs

**Full name:** `neug::function::EpochMs`

**Public Methods:**

- `operation(int64_t &ms, common::timestamp_t &result)`


---

## Equals

**Full name:** `neug::function::Equals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`
- `operation(const common::list_entry_t &left, const common::l...`
- `operation(const common::struct_entry_t &left, const common:...`


---

## EqualsFunction

**Full name:** `neug::function::EqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Even

**Full name:** `neug::function::Even`

**Public Methods:**

- `operation(T &input, double &result)`


---

## ExportCSVBindData

**Full name:** `neug::function::ExportCSVBindData`

**Public Methods:**

- `ExportCSVBindData(std::vector< std::string > names, std::string fil...`
- `copy() const override`


---

## ExportCSVFunction

**Full name:** `neug::function::ExportCSVFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ExportFuncBindData

**Full name:** `neug::function::ExportFuncBindData`

**Public Methods:**

- `ExportFuncBindData(std::vector< std::string > columnNames, std::stri...`
- `~ExportFuncBindData()=default`
- `setDataType(std::vector< common::LogicalType > types_)`
- `constCast() const`
- `ptrCast()`
- `copy() const`


---

## ExportFuncBindInput

**Full name:** `neug::function::ExportFuncBindInput`


---

## ExportFuncLocalState

**Full name:** `neug::function::ExportFuncLocalState`

**Public Methods:**

- `~ExportFuncLocalState()=default`
- `cast()`


---

## ExportFuncSharedState

**Full name:** `neug::function::ExportFuncSharedState`

**Public Methods:**

- `~ExportFuncSharedState()=default`
- `cast()`
- `init(main::ClientContext &context, const ExportFuncBin...`


---

## ExportFunction

**Full name:** `neug::function::ExportFunction`

**Public Methods:**

- `ExportFunction(std::string name)`
- `ExportFunction(std::string name, export_init_local_t initLocal, ...`


---

## ExportParquetFunction

**Full name:** `neug::function::ExportParquetFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ExtraScanTableFuncBindInput

**Full name:** `neug::function::ExtraScanTableFuncBindInput`


---

## ExtraTableFuncBindInput

**Full name:** `neug::function::ExtraTableFuncBindInput`

**Public Methods:**

- `~ExtraTableFuncBindInput()=default`
- `constPtrCast() const`


---

## Factorial

**Full name:** `neug::function::Factorial`

**Public Methods:**

- `operation(int64_t &input, int64_t &result)`


---

## FactorialFunction

**Full name:** `neug::function::FactorialFunction`

**Public Methods:**

- `getFunctionSet()`


---

## FileInfo

**Full name:** `neug::function::FileInfo`


---

## FileSystemProvider

**Full name:** `neug::function::FileSystemProvider`

**Public Methods:**

- `provide(const reader::FileSchema &schema)=0`


---

## Floor

**Full name:** `neug::function::Floor`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(common::int128_t &input, common::int128_t &result)`


---

## Function

**Full name:** `neug::function::Function`

**Public Methods:**

- `Function()`
- `Function(std::string name, std::vector< common::LogicalTyp...`
- `Function(const Function &)=default`
- `~Function()=default`
- `signatureToString() const`
- `computeSignature()`
- `constPtrCast() const`
- `ptrCast()`


---

## FunctionBindData

**Full name:** `neug::function::FunctionBindData`

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

**Full name:** `neug::function::FunctionCollection`

**Public Methods:**

- `getFunctions()`


---

## FunctionSignatureUtil

**Full name:** `neug::function::FunctionSignatureUtil`

**Public Methods:**

- `getSignatureName(const std::string &funcName, const std::vector< n...`
- `getFunctionName(const std::string &signatureName)`


---

## Gamma

**Full name:** `neug::function::Gamma`

**Public Methods:**

- `operation(T &input, T &result)`


---

## GenRandomUUIDFunction

**Full name:** `neug::function::GenRandomUUIDFunction`

**Public Methods:**

- `getFunctionSet()`


---

## GreaterThan

**Full name:** `neug::function::GreaterThan`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`
- `operation(const common::list_entry_t &left, const common::l...`
- `operation(const common::struct_entry_t &left, const common:...`


---

## GreaterThanEquals

**Full name:** `neug::function::GreaterThanEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## GreaterThanEqualsFunction

**Full name:** `neug::function::GreaterThanEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## GreaterThanFunction

**Full name:** `neug::function::GreaterThanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## IDFunction

**Full name:** `neug::function::IDFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Int128CastData

**Full name:** `neug::function::Int128CastData`

**Public Methods:**

- `flush()`


---

## Int128CastOperation

**Full name:** `neug::function::Int128CastOperation`

**Public Methods:**

- `handleDigit(T &result, uint8_t digit)`
- `finalize(T &result)`


---

## IntegerCastData

**Full name:** `neug::function::IntegerCastData`


---

## IntegerCastOperation

**Full name:** `neug::function::IntegerCastOperation`

**Public Methods:**

- `handleDigit(T &state, uint8_t digit)`
- `finalize(T &)`


---

## IntervalFunctionAlias

**Full name:** `neug::function::IntervalFunctionAlias`


---

## IsACyclicFunction

**Full name:** `neug::function::IsACyclicFunction`

**Public Methods:**

- `getFunctionSet()`


---

## IsTrailFunction

**Full name:** `neug::function::IsTrailFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LabelFunction

**Full name:** `neug::function::LabelFunction`

**Public Methods:**

- `getFunctionSet()`
- `rewriteFunc(const RewriteFunctionBindInput &input)`


---

## LabelsFunction

**Full name:** `neug::function::LabelsFunction`


---

## LcaseFunction

**Full name:** `neug::function::LcaseFunction`


---

## LengthFunction

**Full name:** `neug::function::LengthFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LessThan

**Full name:** `neug::function::LessThan`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## LessThanEquals

**Full name:** `neug::function::LessThanEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## LessThanEqualsFunction

**Full name:** `neug::function::LessThanEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## LessThanFunction

**Full name:** `neug::function::LessThanFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Lgamma

**Full name:** `neug::function::Lgamma`

**Public Methods:**

- `operation(T &input, double &result)`


---

## ListConcat

**Full name:** `neug::function::ListConcat`

**Public Methods:**

- `operation(common::list_entry_t &left, common::list_entry_t ...`


---

## ListContainsFunction

**Full name:** `neug::function::ListContainsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListCreationFunction

**Full name:** `neug::function::ListCreationFunction`

**Public Methods:**

- `getFunctionSet()`
- `execFunc(const std::vector< std::shared_ptr< common::Value...`


---

## ListElementFunction

**Full name:** `neug::function::ListElementFunction`


---

## ListExtract

**Full name:** `neug::function::ListExtract`

**Public Methods:**

- `operation(common::list_entry_t &listEntry, int64_t pos, T &...`
- `operation(common::neug_string_t &str, int64_t &idx, common:...`


---

## ListExtractFunction

**Full name:** `neug::function::ListExtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ListHasFunction

**Full name:** `neug::function::ListHasFunction`


---

## ListLen

**Full name:** `neug::function::ListLen`

**Public Methods:**

- `operation(T &input, int64_t &result)`
- `operation(common::neug_string_t &input, int64_t &result)`


---

## ListPosition

**Full name:** `neug::function::ListPosition`

**Public Methods:**

- `operation(common::list_entry_t &list, T &element, int64_t &...`


---

## ListReverseSort

**Full name:** `neug::function::ListReverseSort`

**Public Methods:**

- `operation(common::list_entry_t &input, common::list_entry_t...`
- `operation(common::list_entry_t &input, common::neug_string_...`
- `operation(common::list_entry_t &, common::neug_string_t &, ...`


---

## ListSort

**Full name:** `neug::function::ListSort`

**Public Methods:**

- `operation(common::list_entry_t &input, common::list_entry_t...`
- `operation(common::list_entry_t &input, common::neug_string_...`
- `operation(common::list_entry_t &input, common::neug_string_...`


---

## ListToString

**Full name:** `neug::function::ListToString`

**Public Methods:**

- `operation(common::list_entry_t &input, common::neug_string_...`


---

## ListUnique

**Full name:** `neug::function::ListUnique`

**Public Methods:**

- `appendListElementsToValueSet(common::list_entry_t &input, common::ValueVector ...`
- `operation(common::list_entry_t &input, int64_t &result, com...`


---

## Ln

**Full name:** `neug::function::Ln`

**Public Methods:**

- `operation(T &input, double &result)`


---

## LocalFileSystemProvider

**Full name:** `neug::function::LocalFileSystemProvider`

**Public Methods:**

- `provide(const reader::FileSchema &schema) override`


---

## Log

**Full name:** `neug::function::Log`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Log2

**Full name:** `neug::function::Log2`

**Public Methods:**

- `operation(T &input, double &result)`


---

## LowerFunction

**Full name:** `neug::function::LowerFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(const std::vector< runtime::Value > &args)`


---

## MinMaxFunction

**Full name:** `neug::function::MinMaxFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(MinMaxState *state, common::ValueVector *input, u...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## MinMaxState

**Full name:** `neug::function::MinMaxFunction::MinMaxState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`
- `setVal(const T &val_, common::InMemOverflowBuffer *)`
- `setVal(const common::neug_string_t &val_, common::InMemO...`


---

## Modulo

**Full name:** `neug::function::Modulo`

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

**Full name:** `neug::function::ModuloFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Multiply

**Full name:** `neug::function::Multiply`

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

**Full name:** `neug::function::MultiplyFunction`

**Public Methods:**

- `getFunctionSet()`


---

## Negate

**Full name:** `neug::function::Negate`

**Public Methods:**

- `operation(T &input, T &result)`
- `operation(int8_t &input, int8_t &result)`
- `operation(int16_t &input, int16_t &result)`
- `operation(int32_t &input, int32_t &result)`
- `operation(int64_t &input, int64_t &result)`


---

## NegateFunction

**Full name:** `neug::function::NegateFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NeugCallFunction

**Full name:** `neug::function::NeugCallFunction`

**Public Methods:**

- `NeugCallFunction()=default`
- `NeugCallFunction(std::string name, std::vector< common::LogicalTyp...`
- `NeugCallFunction(std::string name, std::vector< common::LogicalTyp...`


---

## NeugScalarFunction

**Full name:** `neug::function::NeugScalarFunction`

**Public Methods:**

- `NeugScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `copy() const override`


---

## NextValFunction

**Full name:** `neug::function::NextValFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NodesFunction

**Full name:** `neug::function::NodesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NotEquals

**Full name:** `neug::function::NotEquals`

**Public Methods:**

- `operation(const A &left, const B &right, uint8_t &result, c...`
- `operation(const T &left, const T &right)`


---

## NotEqualsFunction

**Full name:** `neug::function::NotEqualsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## NumericLimits

**Full name:** `neug::function::NumericLimits`

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

**Full name:** `neug::function::NumericLimits< common::int128_t >`

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

## Offset

**Full name:** `neug::function::Offset`

**Public Methods:**

- `operation(common::internalID_t &input, int64_t &result)`


---

## Pi

**Full name:** `neug::function::Pi`

**Public Methods:**

- `operation(double &result)`


---

## PowFunction

**Full name:** `neug::function::PowFunction`


---

## Power

**Full name:** `neug::function::Power`

**Public Methods:**

- `operation(A &left, B &right, R &result)`


---

## PowerFunction

**Full name:** `neug::function::PowerFunction`

**Public Methods:**

- `getFunctionSet()`


---

## PropertiesBindData

**Full name:** `neug::function::PropertiesBindData`

**Public Methods:**

- `PropertiesBindData(common::LogicalType dataType, common::idx_t childIdx)`
- `copy() const override`


---

## PropertiesFunction

**Full name:** `neug::function::PropertiesFunction`

**Public Methods:**

- `getFunctionSet()`


---

## RJAlgorithm

**Full name:** `neug::function::RJAlgorithm`

**Public Methods:**

- `~RJAlgorithm()=default`
- `getFunctionName() const =0`
- `getResultColumns(const RJBindData &bindData) const =0`
- `copy() const =0`


---

## RJBindData

**Full name:** `neug::function::RJBindData`

**Public Methods:**

- `RJBindData(graph::GraphEntry graphEntry)`
- `RJBindData(const RJBindData &other)`
- `toString() const`


---

## Radians

**Full name:** `neug::function::Radians`

**Public Methods:**

- `operation(T &input, double &result)`


---

## ReadFunction

**Full name:** `neug::function::ReadFunction`

**Public Methods:**

- `ReadFunction(std::string name, std::vector< common::LogicalTyp...`


---

## RelationshipsFunction

**Full name:** `neug::function::RelationshipsFunction`


---

## RelsFunction

**Full name:** `neug::function::RelsFunction`

**Public Methods:**

- `getFunctionSet()`


---

## ReverseFunction

**Full name:** `neug::function::ReverseFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(const std::vector< neug::runtime::Value > &args)`


---

## RewriteFunction

**Full name:** `neug::function::RewriteFunction`

**Public Methods:**

- `RewriteFunction(std::string name, std::vector< common::LogicalTyp...`


---

## RewriteFunctionBindInput

**Full name:** `neug::function::RewriteFunctionBindInput`

**Public Methods:**

- `RewriteFunctionBindInput(main::ClientContext *context, binder::ExpressionB...`


---

## Round

**Full name:** `neug::function::Round`

**Public Methods:**

- `operation(A &left, B &right, double &result)`


---

## ScalarBindFuncInput

**Full name:** `neug::function::ScalarBindFuncInput`

**Public Methods:**

- `ScalarBindFuncInput(const binder::expression_vector &arguments, Funct...`


---

## ScalarFunction

**Full name:** `neug::function::ScalarFunction`

**Public Methods:**

- `ScalarFunction()=default`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarFunction(std::string name, std::vector< common::LogicalTyp...`
- `copy() const`
- `BinaryExecFunction(const std::vector< std::shared_ptr< common::Value...`
- `BinaryStringExecFunction(const std::vector< std::shared_ptr< common::Value...`
- `BinaryExecListStructFunction(const std::vector< std::shared_ptr< common::Value...`
- `BinaryExecWithBindData(const std::vector< std::shared_ptr< common::Value...`
- `BinarySelectFunction(const std::vector< std::shared_ptr< common::Value...`
- ... and 9 more methods


---

## ScalarMacroFunction

**Full name:** `neug::function::ScalarMacroFunction`

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

**Full name:** `neug::function::ScalarOrAggregateFunction`

**Public Methods:**

- `ScalarOrAggregateFunction()`
- `ScalarOrAggregateFunction(std::string name, std::vector< common::LogicalTyp...`
- `ScalarOrAggregateFunction(std::string name, std::vector< common::LogicalTyp...`
- `signatureToString() const override`


---

## ScanFileBindData

**Full name:** `neug::function::ScanFileBindData`

**Public Methods:**

- `ScanFileBindData(binder::expression_vector columns, uint64_t numRo...`
- `ScanFileBindData(binder::expression_vector columns, uint64_t numRo...`
- `ScanFileBindData(const ScanFileBindData &other)`
- `getIgnoreErrorsOption() const override`
- `copy() const override`


---

## ScanFileSharedState

**Full name:** `neug::function::ScanFileSharedState`

**Public Methods:**

- `ScanFileSharedState(common::FileScanInfo fileScanInfo, uint64_t numRows)`
- `getNext()`


---

## ScanFileWithProgressSharedState

**Full name:** `neug::function::ScanFileWithProgressSharedState`

**Public Methods:**

- `ScanFileWithProgressSharedState(common::FileScanInfo fileScanInfo, uint64_t numRo...`


---

## ScanReplacement

**Full name:** `neug::function::ScanReplacement`

**Public Methods:**

- `ScanReplacement(scan_replace_func_t replaceFunc)`


---

## ScanReplacementData

**Full name:** `neug::function::ScanReplacementData`


---

## ShowLoadedExtensionsFuncInput

**Full name:** `neug::function::ShowLoadedExtensionsFuncInput`

**Public Methods:**

- `ShowLoadedExtensionsFuncInput()=default`


---

## ShowLoadedExtensionsFunction

**Full name:** `neug::function::ShowLoadedExtensionsFunction`

**Public Methods:**

- `getFunctionSet()`
- `bindFunc(const neug::Schema &schema, const neug::runtime::...`
- `execFunc(const ShowLoadedExtensionsFuncInput &input)`


---

## Sign

**Full name:** `neug::function::Sign`

**Public Methods:**

- `operation(T &input, int64_t &result)`


---

## Sin

**Full name:** `neug::function::Sin`

**Public Methods:**

- `operation(T &input, double &result)`


---

## Sqrt

**Full name:** `neug::function::Sqrt`

**Public Methods:**

- `operation(T &input, double &result)`


---

## StartNodeFunction

**Full name:** `neug::function::StartNodeFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StartsWithFunction

**Full name:** `neug::function::StartsWithFunction`

**Public Methods:**

- `getFunctionSet()`


---

## StructExtractBindData

**Full name:** `neug::function::StructExtractBindData`

**Public Methods:**

- `StructExtractBindData(common::LogicalType dataType, common::idx_t childIdx)`
- `copy() const override`


---

## StructExtractFunctions

**Full name:** `neug::function::StructExtractFunctions`

**Public Methods:**

- `getFunctionSet()`
- `bindFunc(const ScalarBindFuncInput &input)`
- `compileFunc(FunctionBindData *bindData, const std::vector< st...`


---

## Subtract

**Full name:** `neug::function::Subtract`

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

**Full name:** `neug::function::SubtractFunction`

**Public Methods:**

- `getFunctionSet()`


---

## SuffixFunction

**Full name:** `neug::function::SuffixFunction`


---

## SumFunction

**Full name:** `neug::function::SumFunction`

**Public Methods:**

- `initialize()`
- `updateAll(uint8_t *state_, common::ValueVector *input, uint...`
- `updatePos(uint8_t *state_, common::ValueVector *input, uint...`
- `updateSingleValue(SumState< RESULT_TYPE > *state, common::ValueVect...`
- `combine(uint8_t *state_, uint8_t *otherState_, common::In...`
- `finalize(uint8_t *)`


---

## SumState

**Full name:** `neug::function::SumState`

**Public Methods:**

- `getStateSize() const override`
- `moveResultToVector(common::ValueVector *outputVector, uint64_t pos) override`


---

## TableFuncBindData

**Full name:** `neug::function::TableFuncBindData`

**Public Methods:**

- `TableFuncBindData()`
- `TableFuncBindData(common::row_idx_t numRows)`
- `TableFuncBindData(binder::expression_vector columns)`
- `TableFuncBindData(binder::expression_vector columns, common::row_id...`
- `TableFuncBindData(binder::expression_vector columns, common::row_id...`
- `TableFuncBindData(const TableFuncBindData &other)`
- `operator=(const TableFuncBindData &other)=delete`
- `~TableFuncBindData()=default`
- `getNumColumns() const`
- `setColumnSkips(std::vector< bool > skips)`
- ... and 8 more methods


---

## TableFuncBindInput

**Full name:** `neug::function::TableFuncBindInput`

**Public Methods:**

- `TableFuncBindInput()=default`
- `addLiteralParam(common::Value value)`
- `getParam(common::idx_t idx) const`
- `getValue(common::idx_t idx) const`
- `getLiteralVal(common::idx_t idx) const`


---

## TableFuncInitLocalStateInput

**Full name:** `neug::function::TableFuncInitLocalStateInput`

**Public Methods:**

- `TableFuncInitLocalStateInput(TableFuncSharedState &sharedState, TableFuncBindD...`


---

## TableFuncInitOutputInput

**Full name:** `neug::function::TableFuncInitOutputInput`

**Public Methods:**

- `TableFuncInitOutputInput(std::vector< processor::DataPos > outColumnPositi...`


---

## TableFuncInitSharedStateInput

**Full name:** `neug::function::TableFuncInitSharedStateInput`

**Public Methods:**

- `TableFuncInitSharedStateInput(TableFuncBindData *bindData, processor::Execution...`


---

## TableFuncInput

**Full name:** `neug::function::TableFuncInput`

**Public Methods:**

- `TableFuncInput()=default`
- `TableFuncInput(TableFuncBindData *bindData, TableFuncLocalState ...`
- `DELETE_COPY_DEFAULT_MOVE(TableFuncInput)`


---

## TableFuncLocalState

**Full name:** `neug::function::TableFuncLocalState`

**Public Methods:**

- `~TableFuncLocalState()=default`
- `ptrCast()`


---

## TableFuncOutput

**Full name:** `neug::function::TableFuncOutput`

**Public Methods:**

- `TableFuncOutput(common::DataChunk dataChunk)`
- `~TableFuncOutput()=default`
- `resetState()`
- `setOutputSize(common::offset_t size) const`


---

## TableFuncSharedState

**Full name:** `neug::function::TableFuncSharedState`

**Public Methods:**

- `TableFuncSharedState()=default`
- `TableFuncSharedState(common::row_idx_t numRows)`
- `~TableFuncSharedState()=default`
- `getNumRows() const`
- `getSemiMasks() const`
- `ptrCast()`


---

## TableFunction

**Full name:** `neug::function::TableFunction`

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

## Tan

**Full name:** `neug::function::Tan`

**Public Methods:**

- `operation(T &input, double &result)`


---

## ToLowerFunction

**Full name:** `neug::function::ToLowerFunction`


---

## ToTimestamp

**Full name:** `neug::function::ToTimestamp`

**Public Methods:**

- `operation(double &sec, common::timestamp_t &result)`


---

## ToUpperFunction

**Full name:** `neug::function::ToUpperFunction`


---

## TryCastStringToTimestamp

**Full name:** `neug::function::TryCastStringToTimestamp`

**Public Methods:**

- `tryCast(const char *input, uint64_t len, neug::common::ti...`
- `cast(const char *input, uint64_t len, neug::common::ti...`
- `tryCast(const char *input, uint64_t len, neug::common::ti...`
- `tryCast(const char *input, uint64_t len, neug::common::ti...`
- `tryCast(const char *input, uint64_t len, neug::common::ti...`


---

## UCaseFunction

**Full name:** `neug::function::UCaseFunction`


---

## UnaryCastFunctionWrapper

**Full name:** `neug::function::UnaryCastFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryCastStringFunctionWrapper

**Full name:** `neug::function::UnaryCastStringFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryFunctionExecutor

**Full name:** `neug::function::UnaryFunctionExecutor`

**Public Methods:**

- `executeOnValue(common::ValueVector &inputVector, uint64_t inputP...`
- `getSelectedPos(common::idx_t selIdx, common::SelectionVector *op...`
- `executeOnSelectedValues(common::ValueVector &operand, common::SelectionVe...`
- `executeSwitch(common::ValueVector &operand, common::SelectionVe...`
- `execute(common::ValueVector &operand, common::SelectionVe...`
- `executeSequence(common::ValueVector &operand, common::SelectionVe...`


---

## UnaryFunctionWrapper

**Full name:** `neug::function::UnaryFunctionWrapper`

Unary operator assumes operation with null returns null.

This does NOT applies to IS_NULL and IS_NOT_NULL operation.

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryNestedTypeFunctionWrapper

**Full name:** `neug::function::UnaryNestedTypeFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnarySequenceFunctionWrapper

**Full name:** `neug::function::UnarySequenceFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryStringFunctionWrapper

**Full name:** `neug::function::UnaryStringFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UnaryStructFunctionWrapper

**Full name:** `neug::function::UnaryStructFunctionWrapper`

**Public Methods:**

- `operation(void *, uint64_t, void *resultVector, uint64_t re...`


---

## UnaryUDFFunctionWrapper

**Full name:** `neug::function::UnaryUDFFunctionWrapper`

**Public Methods:**

- `operation(void *inputVector, uint64_t inputPos, void *resul...`


---

## UpperFunction

**Full name:** `neug::function::UpperFunction`

**Public Methods:**

- `getFunctionSet()`
- `Exec(const std::vector< neug::runtime::Value > &args)`


---

## ValueEquality

**Full name:** `neug::function::ValueEquality`

**Public Methods:**

- `operator()(const common::Value &a, const common::Value &b) const`


---

## ValueHashFunction

**Full name:** `neug::function::ValueHashFunction`

**Public Methods:**

- `operator()(const common::Value &value) const`


---

## VectorStringFunction

**Full name:** `neug::function::VectorStringFunction`

**Public Methods:**

- `getUnaryStrFunction(std::string funcName)`


---

## pickDecimalPhysicalType

**Full name:** `neug::function::pickDecimalPhysicalType`


---

## EdgeLabel

**Full name:** `neug::gopt::EdgeLabel`

**Public Methods:**

- `EdgeLabel(const std::string &type, const std::string &src, ...`


---

## EdgeLabelId

**Full name:** `neug::gopt::EdgeLabelId`

**Public Methods:**

- `EdgeLabelId(common::table_id_t eid, common::table_id_t sid, c...`


---

## ExecutionFlag

**Full name:** `neug::gopt::ExecutionFlag`


---

## GAliasManager

**Full name:** `neug::gopt::GAliasManager`

**Public Methods:**

- `GAliasManager(const planner::LogicalPlan &plan)`
- `getAliasId(const std::string &uniqueName)`
- `getGAliasName(common::alias_id_t aliasId)`
- `extractGAliasNames(const planner::LogicalOperator &op, std::vector< ...`
- `extractAliasIds(const planner::LogicalOperator &op, std::vector< ...`
- `printForDebug()`


---

## GAliasName

**Full name:** `neug::gopt::GAliasName`

**Public Methods:**

- `GAliasName()=default`
- `GAliasName(const std::string &uniqueName, std::optional< std...`
- `GAliasName(const std::string &uniqueName, const std::string ...`


---

## GDDLConverter

**Full name:** `neug::gopt::GDDLConverter`

**Public Methods:**

- `GDDLConverter(std::shared_ptr< GAliasManager > aliasManager, ne...`
- `~GDDLConverter()=default`
- `convertCreateTable(const planner::LogicalCreateTable &op, ::physical...`
- `convertDropTable(const planner::LogicalDrop &op, ::physical::Physi...`
- `convertAlterTable(const planner::LogicalAlter &op, ::physical::Phys...`


---

## GExprConverter

**Full name:** `neug::gopt::GExprConverter`

**Public Methods:**

- `GExprConverter(const std::shared_ptr< gopt::GAliasManager > aliasManager)`
- `convert(const binder::Expression &expr, const std::vector...`
- `convert(const binder::Expression &expr, const planner::Lo...`
- `convertPrimaryKey(const std::string &key, const binder::Expression &expr)`
- `convertVar(common::alias_id_t columnId)`
- `convertAlias(common::alias_id_t aliasId)`
- `convertAggFunc(const binder::AggregateFunctionExpression &expr, ...`
- `convertDefaultVar()`
- `convertDefaultValue(const binder::PropertyDefinition &propertyDef)`
- `convertPropertyExpr(const std::string &propName)`


---

## GLogicalTypeConverter

**Full name:** `neug::gopt::GLogicalTypeConverter`

**Public Methods:**

- `GLogicalTypeConverter()=default`
- `convertDataType(const ::common::DataType &type)`


---

## GNodeType

**Full name:** `neug::gopt::GNodeType`

**Public Methods:**

- `GNodeType(std::vector< catalog::NodeTableCatalogEntry * > n...`
- `GNodeType(const binder::NodeExpression &nodeExpr)`
- `getLabelIds() const`
- `toYAML() const`


---

## GPhysicalAnalyzer

**Full name:** `neug::gopt::GPhysicalAnalyzer`

**Public Methods:**

- `GPhysicalAnalyzer()=default`
- `analyze(const planner::LogicalPlan &plan)`


---

## GPhysicalConvertor

**Full name:** `neug::gopt::GPhysicalConvertor`

**Public Methods:**

- `GPhysicalConvertor(std::shared_ptr< GAliasManager > aliasManager, ne...`
- `createEmptyPlan()`
- `convert(const planner::LogicalPlan &plan, bool skipSink=false)`


---

## GPhysicalTypeConverter

**Full name:** `neug::gopt::GPhysicalTypeConverter`

**Public Methods:**

- `GPhysicalTypeConverter()=default`
- `convertNodeType(const GNodeType &nodeType)`
- `convertRelType(const GRelType &relType)`
- `convertPathType(const GRelType &relType)`
- `convertArrayType(const common::LogicalType &type, const binder::Ex...`
- `convertStructType(const common::LogicalType &type, const binder::Ex...`
- `convertLogicalType(const common::LogicalType &type, const binder::Ex...`
- `convertSimpleLogicalType(const common::LogicalType &type)`


---

## GPrecedence

**Full name:** `neug::gopt::GPrecedence`

**Public Methods:**

- `getPrecedence(const binder::Expression &expr)`
- `isLeftAssociative(const binder::Expression &expr)`
- `needBrace(const binder::Expression &parent, const binder::E...`


---

## GQueryConvertor

**Full name:** `neug::gopt::GQueryConvertor`

**Public Methods:**

- `GQueryConvertor(std::shared_ptr< GAliasManager > aliasManager, ne...`
- `convert(const planner::LogicalPlan &plan, bool skipSink)`
- `skipColumn(const std::string &columnName)`


---

## GRelType

**Full name:** `neug::gopt::GRelType`

**Public Methods:**

- `GRelType(std::vector< catalog::GRelTableCatalogEntry * > relTables_)`
- `GRelType(const binder::RelExpression &relExpr)`
- `getLabelIds() const`
- `toYAML(catalog::Catalog *catalog) const`


---

## GResultSchema

**Full name:** `neug::gopt::GResultSchema`

**Public Methods:**

- `infer(const planner::LogicalPlan &plan, std::shared_ptr...`
- `convertType(const binder::Expression &expr, catalog::Catalog *catalog)`
- `inferFromExpr(const planner::LogicalPlan &plan)`


---

## GScalarType

**Full name:** `neug::gopt::GScalarType`

**Public Methods:**

- `GScalarType(const binder::Expression &expr)`
- `getType() const`
- `isArithmetic() const`
- `isTemporal() const`


---

## BoundGraphEntryTableInfo

**Full name:** `neug::graph::BoundGraphEntryTableInfo`

**Public Methods:**

- `BoundGraphEntryTableInfo(catalog::TableCatalogEntry *entry)`
- `BoundGraphEntryTableInfo(catalog::TableCatalogEntry *entry, std::shared_pt...`


---

## Graph

**Full name:** `neug::graph::Graph`

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

**Full name:** `neug::graph::Graph::EdgeIterator`

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

**Full name:** `neug::graph::Graph::VertexIterator`

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

**Full name:** `neug::graph::GraphEntry`

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

**Full name:** `neug::graph::GraphEntrySet`

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

**Full name:** `neug::graph::GraphEntryTableInfo`

**Public Methods:**

- `GraphEntryTableInfo(std::string tableName, std::string predicate)`
- `toString() const`


---

## NbrScanState

**Full name:** `neug::graph::NbrScanState`

**Public Methods:**

- `~NbrScanState()=default`
- `getChunk()=0`
- `next()=0`


---

## Chunk

**Full name:** `neug::graph::NbrScanState::Chunk`

**Public Methods:**

- `EXPLICIT_COPY_METHOD(Chunk)`
- `forEach(Func &&func) const`
- `forEachBreakWhenFalse(Func &&func) const`
- `size() const`


---

## NbrTableInfo

**Full name:** `neug::graph::NbrTableInfo`

**Public Methods:**

- `NbrTableInfo(catalog::TableCatalogEntry *nodeEntry, catalog::T...`


---

## OnDiskGraph

**Full name:** `neug::graph::OnDiskGraph`

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

**Full name:** `neug::graph::OnDiskGraphNbrScanState`

**Public Methods:**

- `OnDiskGraphNbrScanState(main::ClientContext *context, catalog::TableCatal...`
- `OnDiskGraphNbrScanState(main::ClientContext *context, catalog::TableCatal...`
- `getChunk() override`
- `next() override`
- `startScan(common::RelDataDirection direction)`


---

## OnDiskGraphVertexScanState

**Full name:** `neug::graph::OnDiskGraphVertexScanState`

**Public Methods:**

- `OnDiskGraphVertexScanState(main::ClientContext &context, const catalog::Tabl...`
- `startScan(common::offset_t beginOffset, common::offset_t en...`
- `next() override`
- `getChunk() override`


---

## ParsedGraphEntry

**Full name:** `neug::graph::ParsedGraphEntry`


---

## VertexScanState

**Full name:** `neug::graph::VertexScanState`

**Public Methods:**

- `getChunk()=0`
- `next()=0`
- `~VertexScanState()=default`


---

## Chunk

**Full name:** `neug::graph::VertexScanState::Chunk`

**Public Methods:**

- `size() const`
- `getNodeIDs() const`
- `getProperties(size_t propertyIndex) const`


---

## VertexArray

**Full name:** `neug::graph_interface_impl::VertexArray`

**Public Methods:**

- `VertexArray()`
- `VertexArray(const VertexSet &keys, const T &val)`
- `~VertexArray()`
- `Init(const VertexSet &keys, const T &val)`
- `operator[](vid_t v)`
- `operator[](vid_t v) const`


---

## KeyBuffer

**Full name:** `neug::id_indexer_impl::KeyBuffer`

**Public Methods:**

- `serialize(std::ostream &os, const type &buffer)`
- `deserialize(std::istream &is, type &buffer)`


---

## string >

**Full name:** `neug::id_indexer_impl::KeyBuffer< std::string >`

**Public Methods:**

- `serialize(std::ostream &os, const type &buffer)`
- `deserialize(std::istream &is, type &buffer)`


---

## string_view >

**Full name:** `neug::id_indexer_impl::KeyBuffer< std::string_view >`

**Public Methods:**

- `serialize(std::ostream &os, const type &buffer)`
- `deserialize(std::istream &is, type &buffer)`


---

## ActiveQuery

**Full name:** `neug::main::ActiveQuery`

**Public Methods:**

- `ActiveQuery()`
- `reset()`


---

## AutoCheckpointSetting

**Full name:** `neug::main::AutoCheckpointSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## CheckpointThresholdSetting

**Full name:** `neug::main::CheckpointThresholdSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ClientConfig

**Full name:** `neug::main::ClientConfig`


---

## ClientConfigDefault

**Full name:** `neug::main::ClientConfigDefault`


---

## ClientContext

**Full name:** `neug::main::ClientContext`

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

**Full name:** `neug::main::ConfigurationOption`

**Public Methods:**

- `ConfigurationOption(std::string name, common::LogicalTypeID parameter...`


---

## DisableMapKeyCheck

**Full name:** `neug::main::DisableMapKeyCheck`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableInternalCatalogSetting

**Full name:** `neug::main::EnableInternalCatalogSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableMVCCSetting

**Full name:** `neug::main::EnableMVCCSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableOptimizerSetting

**Full name:** `neug::main::EnableOptimizerSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableSemiMaskSetting

**Full name:** `neug::main::EnableSemiMaskSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## EnableZoneMapSetting

**Full name:** `neug::main::EnableZoneMapSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ExtensionOption

**Full name:** `neug::main::ExtensionOption`

**Public Methods:**

- `ExtensionOption(std::string name, common::LogicalTypeID parameter...`


---

## FileSearchPathSetting

**Full name:** `neug::main::FileSearchPathSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ForceCheckpointClosingDBSetting

**Full name:** `neug::main::ForceCheckpointClosingDBSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## HomeDirectorySetting

**Full name:** `neug::main::HomeDirectorySetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## MetadataManager

**Full name:** `neug::main::MetadataManager`

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

**Full name:** `neug::main::OpProfileBox`

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

**Full name:** `neug::main::Option`

**Public Methods:**

- `Option(std::string name, common::LogicalTypeID parameter...`
- `~Option()=default`


---

## PreparedStatement

**Full name:** `neug::main::PreparedStatement`

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

**Full name:** `neug::main::PreparedSummary`

`PreparedSummary` stores the compiling time and query options of a query.


---

## ProgressBarSetting

**Full name:** `neug::main::ProgressBarSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## QuerySummary

**Full name:** `neug::main::QuerySummary`

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

**Full name:** `neug::main::RecursivePatternFactorSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## RecursivePatternSemanticSetting

**Full name:** `neug::main::RecursivePatternSemanticSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## SparseFrontierThresholdSetting

**Full name:** `neug::main::SparseFrontierThresholdSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## SpillToDiskSetting

**Full name:** `neug::main::SpillToDiskSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## ThreadsSetting

**Full name:** `neug::main::ThreadsSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## TimeoutSetting

**Full name:** `neug::main::TimeoutSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## VarLengthExtendMaxDepthSetting

**Full name:** `neug::main::VarLengthExtendMaxDepthSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## WarningLimitSetting

**Full name:** `neug::main::WarningLimitSetting`

**Public Methods:**

- `setContext(ClientContext *context, const common::Value &parameter)`
- `getSetting(const ClientContext *context)`


---

## mmap_array

**Full name:** `neug::mmap_array`

**Public Methods:**

- `mmap_array()`
- `mmap_array(const mmap_array< T > &rhs)`
- `mmap_array(mmap_array &&rhs)`
- `~mmap_array()`
- `reset(bool remove_file=true)`
- `set_hugepage_prefered(bool val)`
- `open(const std::string &filename, bool sync_to_file=fa...`
- `open_with_hugepages(const std::string &filename, size_t capacity=0)`
- `dump(const std::string &filename)`
- `resize(size_t size)`
- ... and 12 more methods


---

## string_view >

**Full name:** `neug::mmap_array< std::string_view >`

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
- `resize(size_t size, size_t data_size)`
- ... and 11 more methods


---

## AggKeyDependencyOptimizer

**Full name:** `neug::optimizer::AggKeyDependencyOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## CardinalityUpdater

**Full name:** `neug::optimizer::CardinalityUpdater`

**Public Methods:**

- `CardinalityUpdater(const planner::CardinalityEstimator &cardinalityE...`
- `rewrite(planner::LogicalPlan *plan)`


---

## CommonPatternReuseOptimizer

**Full name:** `neug::optimizer::CommonPatternReuseOptimizer`

**Public Methods:**

- `CommonPatternReuseOptimizer(main::ClientContext *ctx)`
- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitHashJoinReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `getScanParent(std::shared_ptr< planner::LogicalOperator > parent)`


---

## CorrelatedSubqueryUnnestSolver

**Full name:** `neug::optimizer::CorrelatedSubqueryUnnestSolver`

**Public Methods:**

- `CorrelatedSubqueryUnnestSolver(planner::LogicalOperator *accumulateOp)`
- `solve(planner::LogicalOperator *root_)`


---

## ExpandGetVFusion

**Full name:** `neug::optimizer::ExpandGetVFusion`

**Public Methods:**

- `ExpandGetVFusion(catalog::Catalog *catalog)`
- `~ExpandGetVFusion()=default`
- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitGetVReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `visitRecursiveExtendReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `hasLabelFiltering(const gopt::GNodeType &getVType, const gopt::GRel...`
- `transformExpandType(const gopt::GRelType &expandType, catalog::Catalo...`


---

## FactorizationRewriter

**Full name:** `neug::optimizer::FactorizationRewriter`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(planner::LogicalOperator *op)`


---

## FilterPushDownOptimizer

**Full name:** `neug::optimizer::FilterPushDownOptimizer`

**Public Methods:**

- `FilterPushDownOptimizer(main::ClientContext *context)`
- `FilterPushDownOptimizer(main::ClientContext *context, PredicateSet predicateSet)`
- `rewrite(planner::LogicalPlan *plan)`


---

## FilterPushDownPattern

**Full name:** `neug::optimizer::FilterPushDownPattern`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `visitNodeLabelFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## FlatJoinToExpandOptimizer

**Full name:** `neug::optimizer::FlatJoinToExpandOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitHashJoinReplace(std::shared_ptr< planner::LogicalOperator > op) override`
- `checkOperatorType(std::shared_ptr< planner::LogicalOperator > op, c...`
- `getScanParent(std::shared_ptr< planner::LogicalOperator > parent)`


---

## HashJoinSIPOptimizer

**Full name:** `neug::optimizer::HashJoinSIPOptimizer`

**Public Methods:**

- `rewrite(const planner::LogicalPlan *plan)`


---

## LimitPushDownOptimizer

**Full name:** `neug::optimizer::LimitPushDownOptimizer`

**Public Methods:**

- `LimitPushDownOptimizer()`
- `rewrite(planner::LogicalPlan *plan)`


---

## LogicalFilterCollector

**Full name:** `neug::optimizer::LogicalFilterCollector`


---

## LogicalFlattenCollector

**Full name:** `neug::optimizer::LogicalFlattenCollector`


---

## LogicalIndexScanNodeCollector

**Full name:** `neug::optimizer::LogicalIndexScanNodeCollector`


---

## LogicalOperatorCollector

**Full name:** `neug::optimizer::LogicalOperatorCollector`

**Public Methods:**

- `~LogicalOperatorCollector() override=default`
- `collect(planner::LogicalOperator *op)`
- `hasOperators() const`
- `getOperators() const`


---

## LogicalOperatorVisitor

**Full name:** `neug::optimizer::LogicalOperatorVisitor`

**Public Methods:**

- `LogicalOperatorVisitor()=default`
- `~LogicalOperatorVisitor()=default`


---

## LogicalRecursiveExtendCollector

**Full name:** `neug::optimizer::LogicalRecursiveExtendCollector`


---

## LogicalScanNodeTableCollector

**Full name:** `neug::optimizer::LogicalScanNodeTableCollector`


---

## Optimizer

**Full name:** `neug::optimizer::Optimizer`

**Public Methods:**

- `optimize(planner::LogicalPlan *plan, main::ClientContext *...`


---

## PredicateSet

**Full name:** `neug::optimizer::PredicateSet`

**Public Methods:**

- `PredicateSet()=default`
- `EXPLICIT_COPY_DEFAULT_MOVE(PredicateSet)`
- `isEmpty() const`
- `clear()`
- `addPredicate(std::shared_ptr< binder::Expression > predicate)`
- `popNodePKEqualityComparison(const binder::Expression &nodeID, const std::vect...`
- `getAllPredicates()`


---

## ProjectionPushDownOptimizer

**Full name:** `neug::optimizer::ProjectionPushDownOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `ProjectionPushDownOptimizer(common::PathSemantic semantic, main::ClientContext *ctx)`


---

## RemoveFactorizationRewriter

**Full name:** `neug::optimizer::RemoveFactorizationRewriter`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`


---

## RemoveSubqueryAsJoin

**Full name:** `neug::optimizer::RemoveSubqueryAsJoin`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitFilterReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## RemoveUnnecessaryJoinOptimizer

**Full name:** `neug::optimizer::RemoveUnnecessaryJoinOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## RenameDependentVarOpt

**Full name:** `neug::optimizer::RenameDependentVarOpt`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(planner::LogicalOperator *op)`
- `visitScanNodeTable(planner::LogicalOperator *op) override`
- `visitExtend(planner::LogicalOperator *op) override`
- `visitGetV(planner::LogicalOperator *op) override`


---

## SchemaPopulator

**Full name:** `neug::optimizer::SchemaPopulator`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`


---

## TopKOptimizer

**Full name:** `neug::optimizer::TopKOptimizer`

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`


---

## UnionAliasMapOptimizer

**Full name:** `neug::optimizer::UnionAliasMapOptimizer`

To guarantee all subqueries have the same ouput schema by adding LogicalAliasMap at the tail.

**Public Methods:**

- `rewrite(planner::LogicalPlan *plan)`
- `visitOperator(const std::shared_ptr< planner::LogicalOperator > &op)`
- `visitUnionReplace(std::shared_ptr< planner::LogicalOperator > op) override`


---

## Alter

**Full name:** `neug::parser::Alter`

**Public Methods:**

- `Alter(AlterInfo info)`
- `getInfo() const`


---

## AlterInfo

**Full name:** `neug::parser::AlterInfo`

**Public Methods:**

- `AlterInfo(common::AlterType type, std::string tableName, st...`
- `DELETE_COPY_DEFAULT_MOVE(AlterInfo)`


---

## AttachDatabase

**Full name:** `neug::parser::AttachDatabase`

**Public Methods:**

- `AttachDatabase(AttachInfo attachInfo)`
- `getAttachInfo() const`


---

## AttachInfo

**Full name:** `neug::parser::AttachInfo`

**Public Methods:**

- `AttachInfo(std::string dbPath, std::string dbAlias, std::str...`


---

## BaseScanSource

**Full name:** `neug::parser::BaseScanSource`

**Public Methods:**

- `BaseScanSource(common::ScanSourceType type)`
- `~BaseScanSource()=default`
- `DELETE_COPY_AND_MOVE(BaseScanSource)`
- `ptrCast()`
- `constPtrCast() const`


---

## Copy

**Full name:** `neug::parser::Copy`

**Public Methods:**

- `Copy(common::StatementType type)`
- `setParsingOption(options_t options)`
- `getParsingOptions() const`


---

## CopyFrom

**Full name:** `neug::parser::CopyFrom`

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

**Full name:** `neug::parser::CopyFromColumnInfo`

**Public Methods:**

- `CopyFromColumnInfo()=default`
- `CopyFromColumnInfo(bool inputColumnOrder, std::vector< std::string >...`


---

## CopyTo

**Full name:** `neug::parser::CopyTo`

**Public Methods:**

- `CopyTo(std::string filePath, std::unique_ptr< Statement ...`
- `getFilePath() const`
- `getStatement() const`


---

## CreateMacro

**Full name:** `neug::parser::CreateMacro`

**Public Methods:**

- `CreateMacro(std::string macroName, std::unique_ptr< ParsedExp...`
- `getMacroName() const`
- `getMacroExpression() const`
- `getPositionalArgs() const`
- `getDefaultArgs() const`


---

## CreateSequence

**Full name:** `neug::parser::CreateSequence`

**Public Methods:**

- `CreateSequence(CreateSequenceInfo info)`
- `getInfo() const`


---

## CreateSequenceInfo

**Full name:** `neug::parser::CreateSequenceInfo`

**Public Methods:**

- `CreateSequenceInfo(std::string sequenceName, common::ConflictAction ...`
- `EXPLICIT_COPY_DEFAULT_MOVE(CreateSequenceInfo)`


---

## CreateTable

**Full name:** `neug::parser::CreateTable`

**Public Methods:**

- `CreateTable(CreateTableInfo info)`
- `getInfo() const`


---

## CreateTableInfo

**Full name:** `neug::parser::CreateTableInfo`

**Public Methods:**

- `CreateTableInfo(catalog::CatalogEntryType type, std::string table...`
- `DELETE_COPY_DEFAULT_MOVE(CreateTableInfo)`


---

## CreateType

**Full name:** `neug::parser::CreateType`

**Public Methods:**

- `CreateType(std::string name, std::string dataType)`
- `getName() const`
- `getDataType() const`


---

## DatabaseStatement

**Full name:** `neug::parser::DatabaseStatement`

**Public Methods:**

- `DatabaseStatement(common::StatementType type, std::string dbName)`
- `getDBName() const`


---

## DeleteClause

**Full name:** `neug::parser::DeleteClause`

**Public Methods:**

- `DeleteClause(common::DeleteNodeType deleteType)`
- `addExpression(std::unique_ptr< ParsedExpression > expression)`
- `getDeleteClauseType() const`
- `getNumExpressions() const`
- `getExpression(uint32_t idx) const`


---

## DetachDatabase

**Full name:** `neug::parser::DetachDatabase`

**Public Methods:**

- `DetachDatabase(std::string dbName)`


---

## Drop

**Full name:** `neug::parser::Drop`

**Public Methods:**

- `Drop(DropInfo dropInfo)`
- `getDropInfo() const`


---

## DropInfo

**Full name:** `neug::parser::DropInfo`


---

## ExplainStatement

**Full name:** `neug::parser::ExplainStatement`

**Public Methods:**

- `ExplainStatement(std::unique_ptr< Statement > statementToExplain, ...`
- `getStatementToExplain() const`
- `getExplainType() const`


---

## ExportDB

**Full name:** `neug::parser::ExportDB`

**Public Methods:**

- `ExportDB(std::string filePath)`
- `setParsingOption(options_t options)`
- `getParsingOptionsRef() const`
- `getFilePath() const`


---

## ExtensionStatement

**Full name:** `neug::parser::ExtensionStatement`

**Public Methods:**

- `ExtensionStatement(std::unique_ptr< ExtensionAuxInfo > info)`
- `getAuxInfo() const`


---

## ExtraAddPropertyInfo

**Full name:** `neug::parser::ExtraAddPropertyInfo`

**Public Methods:**

- `ExtraAddPropertyInfo(std::string propertyName, std::string dataType, s...`


---

## ExtraAlterInfo

**Full name:** `neug::parser::ExtraAlterInfo`

**Public Methods:**

- `~ExtraAlterInfo()=default`
- `constPtrCast() const`
- `ptrCast()`


---

## ExtraCommentInfo

**Full name:** `neug::parser::ExtraCommentInfo`

**Public Methods:**

- `ExtraCommentInfo(std::string comment)`


---

## ExtraCreateNodeTableInfo

**Full name:** `neug::parser::ExtraCreateNodeTableInfo`

**Public Methods:**

- `ExtraCreateNodeTableInfo(std::string pKName)`


---

## ExtraCreateRelTableGroupInfo

**Full name:** `neug::parser::ExtraCreateRelTableGroupInfo`

**Public Methods:**

- `ExtraCreateRelTableGroupInfo(std::string relMultiplicity, std::vector< std::pa...`


---

## ExtraCreateRelTableInfo

**Full name:** `neug::parser::ExtraCreateRelTableInfo`

**Public Methods:**

- `ExtraCreateRelTableInfo(std::string relMultiplicity, std::string srcTable...`


---

## ExtraCreateTableInfo

**Full name:** `neug::parser::ExtraCreateTableInfo`

**Public Methods:**

- `~ExtraCreateTableInfo()=default`
- `constCast() const`


---

## ExtraDropPropertyInfo

**Full name:** `neug::parser::ExtraDropPropertyInfo`

**Public Methods:**

- `ExtraDropPropertyInfo(std::string propertyName)`


---

## ExtraRenamePropertyInfo

**Full name:** `neug::parser::ExtraRenamePropertyInfo`

**Public Methods:**

- `ExtraRenamePropertyInfo(std::string propertyName, std::string newName)`


---

## ExtraRenameTableInfo

**Full name:** `neug::parser::ExtraRenameTableInfo`

**Public Methods:**

- `ExtraRenameTableInfo(std::string newName)`


---

## FileScanSource

**Full name:** `neug::parser::FileScanSource`

**Public Methods:**

- `FileScanSource(std::vector< std::string > paths)`


---

## ImportDB

**Full name:** `neug::parser::ImportDB`

**Public Methods:**

- `ImportDB(std::string filePath)`
- `getFilePath() const`


---

## InQueryCallClause

**Full name:** `neug::parser::InQueryCallClause`

**Public Methods:**

- `InQueryCallClause(std::unique_ptr< ParsedExpression > functionExpre...`
- `getFunctionExpression() const`
- `getYieldVariables() const`


---

## InsertClause

**Full name:** `neug::parser::InsertClause`

**Public Methods:**

- `InsertClause(std::vector< PatternElement > patternElements)`
- `getPatternElementsRef() const`


---

## JoinHintNode

**Full name:** `neug::parser::JoinHintNode`

**Public Methods:**

- `JoinHintNode()=default`
- `JoinHintNode(std::string name)`
- `addChild(std::shared_ptr< JoinHintNode > child)`
- `isLeaf() const`


---

## KuzuCypherParser

**Full name:** `neug::parser::KuzuCypherParser`

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

**Full name:** `neug::parser::LoadFrom`

**Public Methods:**

- `LoadFrom(std::unique_ptr< BaseScanSource > source)`
- `getSource() const`
- `setParingOptions(options_t options)`
- `getParsingOptions() const`
- `setPropertyDefinitions(std::vector< ParsedColumnDefinition > definitions)`
- `getColumnDefinitions() const`


---

## MacroParameterReplacer

**Full name:** `neug::parser::MacroParameterReplacer`

**Public Methods:**

- `MacroParameterReplacer(const std::unordered_map< std::string, ParsedExpr...`
- `replace(std::unique_ptr< ParsedExpression > input)`


---

## MatchClause

**Full name:** `neug::parser::MatchClause`

**Public Methods:**

- `MatchClause(std::vector< PatternElement > patternElements, co...`
- `getPatternElementsRef() const`
- `getMatchClauseType() const`
- `setHint(std::shared_ptr< JoinHintNode > root)`
- `hasHint() const`
- `getHint() const`


---

## MergeClause

**Full name:** `neug::parser::MergeClause`

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

**Full name:** `neug::parser::NodePattern`

**Public Methods:**

- `NodePattern(std::string name, std::vector< std::string > tabl...`
- `DELETE_COPY_DEFAULT_MOVE(NodePattern)`
- `~NodePattern()=default`
- `getVariableName() const`
- `getTableNames() const`
- `getPropertyKeyVals() const`


---

## ObjectScanSource

**Full name:** `neug::parser::ObjectScanSource`

**Public Methods:**

- `ObjectScanSource(std::vector< std::string > objectNames)`


---

## ParsedCaseAlternative

**Full name:** `neug::parser::ParsedCaseAlternative`

**Public Methods:**

- `ParsedCaseAlternative()=default`
- `ParsedCaseAlternative(std::unique_ptr< ParsedExpression > whenExpressio...`
- `ParsedCaseAlternative(const ParsedCaseAlternative &other)`
- `DEFAULT_BOTH_MOVE(ParsedCaseAlternative)`
- `serialize(common::Serializer &serializer) const`
- `deserialize(common::Deserializer &deserializer)`


---

## ParsedCaseExpression

**Full name:** `neug::parser::ParsedCaseExpression`

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

**Full name:** `neug::parser::ParsedColumnDefinition`

**Public Methods:**

- `ParsedColumnDefinition(std::string name, std::string type)`
- `EXPLICIT_COPY_DEFAULT_MOVE(ParsedColumnDefinition)`


---

## ParsedExpression

**Full name:** `neug::parser::ParsedExpression`

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

**Full name:** `neug::parser::ParsedExpressionVisitor`

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

**Full name:** `neug::parser::ParsedFunctionExpression`

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

**Full name:** `neug::parser::ParsedLambdaExpression`

**Public Methods:**

- `ParsedLambdaExpression(std::vector< std::string > varNames, std::unique_...`
- `getVarNames() const`
- `getFunctionExpr() const`
- `copy() const override`


---

## ParsedLiteralExpression

**Full name:** `neug::parser::ParsedLiteralExpression`

**Public Methods:**

- `ParsedLiteralExpression(common::Value value, std::string raw)`
- `ParsedLiteralExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedLiteralExpression(common::Value value)`
- `getValue() const`
- `copy() const override`
- `deserialize(common::Deserializer &deserializer)`


---

## ParsedParamExprCollector

**Full name:** `neug::parser::ParsedParamExprCollector`

**Public Methods:**

- `getParamExprs() const`
- `hasParamExprs() const`
- `visitParamExpr(const ParsedExpression *expr) override`


---

## ParsedParameterExpression

**Full name:** `neug::parser::ParsedParameterExpression`

**Public Methods:**

- `ParsedParameterExpression(std::string parameterName, std::string raw)`
- `getParameterName() const`
- `copy() const override`
- `deserialize(common::Deserializer &)`


---

## ParsedPropertyDefinition

**Full name:** `neug::parser::ParsedPropertyDefinition`

**Public Methods:**

- `ParsedPropertyDefinition(ParsedColumnDefinition columnDefinition, std::uni...`
- `EXPLICIT_COPY_DEFAULT_MOVE(ParsedPropertyDefinition)`
- `getName() const`
- `getType() const`


---

## ParsedPropertyExpression

**Full name:** `neug::parser::ParsedPropertyExpression`

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

**Full name:** `neug::parser::ParsedSubqueryExpression`

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

**Full name:** `neug::parser::ParsedVariableExpression`

**Public Methods:**

- `ParsedVariableExpression(std::string variableName, std::string raw)`
- `ParsedVariableExpression(std::string alias, std::string rawName, parsed_ex...`
- `ParsedVariableExpression(std::string variableName)`
- `getVariableName() const`
- `copy() const override`
- `deserialize(common::Deserializer &deserializer)`


---

## Parser

**Full name:** `neug::parser::Parser`

**Public Methods:**

- `parseQuery(std::string_view query)`


---

## ParserErrorListener

**Full name:** `neug::parser::ParserErrorListener`

**Public Methods:**

- `syntaxError(antlr4::Recognizer *recognizer, antlr4::Token *of...`


---

## ParserErrorStrategy

**Full name:** `neug::parser::ParserErrorStrategy`


---

## PatternElement

**Full name:** `neug::parser::PatternElement`

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

**Full name:** `neug::parser::PatternElementChain`

**Public Methods:**

- `PatternElementChain(RelPattern relPattern, NodePattern nodePattern)`
- `DELETE_COPY_DEFAULT_MOVE(PatternElementChain)`
- `getRelPattern() const`
- `getNodePattern() const`


---

## ProjectionBody

**Full name:** `neug::parser::ProjectionBody`

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

**Full name:** `neug::parser::QueryPart`

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

**Full name:** `neug::parser::QueryScanSource`

**Public Methods:**

- `QueryScanSource(std::unique_ptr< Statement > statement)`


---

## ReadWriteExprAnalyzer

**Full name:** `neug::parser::ReadWriteExprAnalyzer`

**Public Methods:**

- `ReadWriteExprAnalyzer(main::ClientContext *context)`
- `isReadOnly() const`
- `visitFunctionExpr(const ParsedExpression *expr) override`


---

## ReadingClause

**Full name:** `neug::parser::ReadingClause`

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

**Full name:** `neug::parser::RecursiveRelPatternInfo`

**Public Methods:**

- `RecursiveRelPatternInfo()=default`
- `DELETE_COPY_DEFAULT_MOVE(RecursiveRelPatternInfo)`


---

## RegularQuery

**Full name:** `neug::parser::RegularQuery`

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

**Full name:** `neug::parser::RelPattern`

**Public Methods:**

- `RelPattern(std::string name, std::vector< std::string > tabl...`
- `DELETE_COPY_DEFAULT_MOVE(RelPattern)`
- `getRelType() const`
- `getDirection() const`
- `getRecursiveInfo() const`


---

## ReturnClause

**Full name:** `neug::parser::ReturnClause`

**Public Methods:**

- `ReturnClause(ProjectionBody projectionBody)`
- `DELETE_COPY_DEFAULT_MOVE(ReturnClause)`
- `~ReturnClause()=default`
- `getProjectionBody() const`


---

## SetClause

**Full name:** `neug::parser::SetClause`

**Public Methods:**

- `SetClause()`
- `addSetItem(parsed_expr_pair setItem)`
- `getSetItemsRef() const`


---

## SingleQuery

**Full name:** `neug::parser::SingleQuery`

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

**Full name:** `neug::parser::StandaloneCall`

**Public Methods:**

- `StandaloneCall(std::string optionName, std::unique_ptr< ParsedEx...`
- `getOptionName() const`
- `getOptionValue() const`


---

## StandaloneCallFunction

**Full name:** `neug::parser::StandaloneCallFunction`

**Public Methods:**

- `StandaloneCallFunction(std::unique_ptr< ParsedExpression > functionExpression)`
- `getFunctionExpression() const`


---

## StandaloneCallRewriter

**Full name:** `neug::parser::StandaloneCallRewriter`

**Public Methods:**

- `StandaloneCallRewriter(main::ClientContext *context, bool allowRewrite)`
- `getRewriteQuery(const Statement &statement)`


---

## Statement

**Full name:** `neug::parser::Statement`

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

**Full name:** `neug::parser::StatementReadWriteAnalyzer`

**Public Methods:**

- `StatementReadWriteAnalyzer(main::ClientContext *context)`
- `isReadOnly() const`


---

## StatementVisitor

**Full name:** `neug::parser::StatementVisitor`

**Public Methods:**

- `StatementVisitor()=default`
- `~StatementVisitor()=default`
- `visit(const Statement &statement)`


---

## TableFuncScanSource

**Full name:** `neug::parser::TableFuncScanSource`

**Public Methods:**

- `TableFuncScanSource(std::unique_ptr< ParsedExpression > functionExpression)`


---

## TransactionStatement

**Full name:** `neug::parser::TransactionStatement`

**Public Methods:**

- `TransactionStatement(transaction::TransactionAction transactionAction)`
- `getTransactionAction() const`


---

## Transformer

**Full name:** `neug::parser::Transformer`

**Public Methods:**

- `Transformer(CypherParser::Neug_StatementsContext &root)`
- `transform()`


---

## UnwindClause

**Full name:** `neug::parser::UnwindClause`

**Public Methods:**

- `UnwindClause(std::unique_ptr< ParsedExpression > expression, s...`
- `getExpression() const`
- `getAlias() const`


---

## UpdatingClause

**Full name:** `neug::parser::UpdatingClause`

**Public Methods:**

- `UpdatingClause(common::ClauseType clauseType)`
- `~UpdatingClause()=default`
- `getClauseType() const`
- `constCast() const`


---

## UseDatabase

**Full name:** `neug::parser::UseDatabase`

**Public Methods:**

- `UseDatabase(std::string dbName)`


---

## WithClause

**Full name:** `neug::parser::WithClause`

**Public Methods:**

- `WithClause(ProjectionBody projectionBody)`
- `DELETE_COPY_DEFAULT_MOVE(WithClause)`
- `setWhereExpression(std::unique_ptr< ParsedExpression > expression)`
- `hasWhereExpression() const`
- `getWhereExpression() const`


---

## YieldVariable

**Full name:** `neug::parser::YieldVariable`

**Public Methods:**

- `YieldVariable(std::string name, std::string alias)`
- `hasAlias() const`


---

## BaseLogicalExtend

**Full name:** `neug::planner::BaseLogicalExtend`

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

**Full name:** `neug::planner::BaseLogicalExtendPrintInfo`

**Public Methods:**

- `BaseLogicalExtendPrintInfo(std::shared_ptr< binder::NodeExpression > boundNo...`
- `toString() const override`


---

## CardinalityEstimator

**Full name:** `neug::planner::CardinalityEstimator`

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
- ... and 10 more methods


---

## CostModel

**Full name:** `neug::planner::CostModel`

**Public Methods:**

- `computeExtendCost(const LogicalPlan &childPlan)`
- `computeHashJoinCost(const std::vector< binder::expression_pair > &joi...`
- `computeHashJoinCost(const binder::expression_vector &joinNodeIDs, con...`
- `computeMarkJoinCost(const std::vector< binder::expression_pair > &joi...`
- `computeMarkJoinCost(const binder::expression_vector &joinNodeIDs, con...`
- `computeIntersectCost(const LogicalPlan &probePlan, const std::vector< ...`
- `estimateIntersectCostByCard(const neug::planner::LogicalPlan &probePlan, cons...`
- `computeGetVCost(const planner::LogicalRecursiveExtend &extendOp)`
- `computeGetVCost(const planner::LogicalExtend &extendOp)`


---

## DPLevel

**Full name:** `neug::planner::DPLevel`

**Public Methods:**

- `contains(const binder::SubqueryGraph &subqueryGraph)`
- `getSubgraphPlans(const binder::SubqueryGraph &subqueryGraph)`
- `getSubqueryGraphs()`
- `addPlan(const binder::SubqueryGraph &subqueryGraph, std::...`
- `clear()`


---

## ExtraJoinTreeNodeInfo

**Full name:** `neug::planner::ExtraJoinTreeNodeInfo`

**Public Methods:**

- `ExtraJoinTreeNodeInfo(std::shared_ptr< binder::NodeExpression > joinNode)`
- `ExtraJoinTreeNodeInfo(std::vector< std::shared_ptr< binder::NodeExpress...`
- `ExtraJoinTreeNodeInfo(const ExtraJoinTreeNodeInfo &other)`
- `copy() const override`


---

## ExtraKeyInfo

**Full name:** `neug::planner::ExtraKeyInfo`

**Public Methods:**

- `~ExtraKeyInfo()=default`
- `constCast() const`
- `copy() const =0`


---

## ExtraNodeIDListKeyInfo

**Full name:** `neug::planner::ExtraNodeIDListKeyInfo`

**Public Methods:**

- `ExtraNodeIDListKeyInfo(std::shared_ptr< binder::Expression > srcNodeID, ...`
- `copy() const override`


---

## ExtraPathKeyInfo

**Full name:** `neug::planner::ExtraPathKeyInfo`

**Public Methods:**

- `ExtraPathKeyInfo(common::ExtendDirection direction)`
- `copy() const override`


---

## ExtraScanNodeTableInfo

**Full name:** `neug::planner::ExtraScanNodeTableInfo`

**Public Methods:**

- `~ExtraScanNodeTableInfo()=default`
- `copy() const =0`
- `constCast() const`


---

## ExtraScanTreeNodeInfo

**Full name:** `neug::planner::ExtraScanTreeNodeInfo`

**Public Methods:**

- `ExtraScanTreeNodeInfo()=default`
- `ExtraScanTreeNodeInfo(const ExtraScanTreeNodeInfo &other)`
- `merge(const ExtraScanTreeNodeInfo &other)`
- `copy() const override`


---

## ExtraTreeNodeInfo

**Full name:** `neug::planner::ExtraTreeNodeInfo`

**Public Methods:**

- `~ExtraTreeNodeInfo()=default`
- `copy() const =0`
- `constCast() const`
- `cast()`


---

## FactorizationGroup

**Full name:** `neug::planner::FactorizationGroup`

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

**Full name:** `neug::planner::FlattenAll`

**Public Methods:**

- `getGroupsPosToFlatten(const binder::expression_vector &exprs, const Sch...`
- `getGroupsPosToFlatten(std::shared_ptr< binder::Expression > expr, const...`
- `getGroupsPosToFlatten(const std::unordered_set< f_group_pos > &dependen...`


---

## FlattenAllButOne

**Full name:** `neug::planner::FlattenAllButOne`

**Public Methods:**

- `getGroupsPosToFlatten(const binder::expression_vector &exprs, const Sch...`
- `getGroupsPosToFlatten(std::shared_ptr< binder::Expression > expr, const...`
- `getGroupsPosToFlatten(const std::unordered_set< f_group_pos > &dependen...`


---

## GroupDependencyAnalyzer

**Full name:** `neug::planner::GroupDependencyAnalyzer`

**Public Methods:**

- `GroupDependencyAnalyzer(bool collectDependentExpr, const Schema &schema)`
- `getDependentExprs() const`
- `getDependentGroups() const`
- `getRequiredFlatGroups() const`
- `visit(std::shared_ptr< binder::Expression > expr)`


---

## JoinOrderEnumeratorContext

**Full name:** `neug::planner::JoinOrderEnumeratorContext`

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

**Full name:** `neug::planner::JoinOrderUtil`

**Public Methods:**

- `getJoinKeysFlatCardinality(const binder::expression_vector &joinNodeIDs, con...`


---

## JoinPlanSolver

**Full name:** `neug::planner::JoinPlanSolver`

**Public Methods:**

- `JoinPlanSolver(Planner *planner)`
- `solve(const JoinTree &joinTree)`


---

## JoinTree

**Full name:** `neug::planner::JoinTree`

**Public Methods:**

- `JoinTree(std::shared_ptr< JoinTreeNode > root)`
- `JoinTree(const JoinTree &other)`


---

## JoinTreeConstructor

**Full name:** `neug::planner::JoinTreeConstructor`

**Public Methods:**

- `JoinTreeConstructor(const binder::QueryGraph &queryGraph, const Prope...`
- `construct(std::shared_ptr< binder::BoundJoinHintNode > root)`


---

## IntermediateResult

**Full name:** `neug::planner::JoinTreeConstructor::IntermediateResult`


---

## JoinTreeNode

**Full name:** `neug::planner::JoinTreeNode`

**Public Methods:**

- `JoinTreeNode(TreeNodeType type, std::unique_ptr< ExtraTreeNode...`
- `DELETE_COPY_DEFAULT_MOVE(JoinTreeNode)`
- `toString() const`
- `addChild(std::shared_ptr< JoinTreeNode > child)`


---

## LogicalAccumulate

**Full name:** `neug::planner::LogicalAccumulate`

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

**Full name:** `neug::planner::LogicalAggregate`

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

**Full name:** `neug::planner::LogicalAggregatePrintInfo`

**Public Methods:**

- `LogicalAggregatePrintInfo(binder::expression_vector keys, binder::expressio...`
- `toString() const override`
- `copy() const override`


---

## LogicalAliasMap

**Full name:** `neug::planner::LogicalAliasMap`

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

**Full name:** `neug::planner::LogicalAlter`

**Public Methods:**

- `LogicalAlter(binder::BoundAlterInfo info, std::shared_ptr< bin...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalAlterPrintInfo

**Full name:** `neug::planner::LogicalAlterPrintInfo`

**Public Methods:**

- `LogicalAlterPrintInfo(binder::BoundAlterInfo info)`
- `toString() const override`
- `copy() const override`
- `LogicalAlterPrintInfo(const LogicalAlterPrintInfo &other)`


---

## LogicalAttachDatabase

**Full name:** `neug::planner::LogicalAttachDatabase`

**Public Methods:**

- `LogicalAttachDatabase(binder::AttachInfo attachInfo, std::shared_ptr< b...`
- `getAttachInfo() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalAttachDatabasePrintInfo

**Full name:** `neug::planner::LogicalAttachDatabasePrintInfo`

**Public Methods:**

- `LogicalAttachDatabasePrintInfo(std::string dbName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCopyFrom

**Full name:** `neug::planner::LogicalCopyFrom`

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

**Full name:** `neug::planner::LogicalCopyFromPrintInfo`

**Public Methods:**

- `LogicalCopyFromPrintInfo(std::string tableName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCopyTo

**Full name:** `neug::planner::LogicalCopyTo`

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

**Full name:** `neug::planner::LogicalCopyToPrintInfo`

**Public Methods:**

- `LogicalCopyToPrintInfo(std::vector< std::string > columnNames, std::stri...`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateMacro

**Full name:** `neug::planner::LogicalCreateMacro`

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

**Full name:** `neug::planner::LogicalCreateMacroPrintInfo`

**Public Methods:**

- `LogicalCreateMacroPrintInfo(std::string macroName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateSequence

**Full name:** `neug::planner::LogicalCreateSequence`

**Public Methods:**

- `LogicalCreateSequence(binder::BoundCreateSequenceInfo info, std::shared...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() final`


---

## LogicalCreateSequencePrintInfo

**Full name:** `neug::planner::LogicalCreateSequencePrintInfo`

**Public Methods:**

- `LogicalCreateSequencePrintInfo(std::string sequenceName)`
- `toString() const override`
- `copy() const override`


---

## LogicalCreateTable

**Full name:** `neug::planner::LogicalCreateTable`

**Public Methods:**

- `LogicalCreateTable(binder::BoundCreateTableInfo info, std::shared_pt...`
- `getExpressionsForPrinting() const override`
- `getInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalCreateTablePrintInfo

**Full name:** `neug::planner::LogicalCreateTablePrintInfo`

**Public Methods:**

- `LogicalCreateTablePrintInfo(binder::BoundCreateTableInfo info)`
- `toString() const override`
- `copy() const override`
- `LogicalCreateTablePrintInfo(const LogicalCreateTablePrintInfo &other)`


---

## LogicalCreateType

**Full name:** `neug::planner::LogicalCreateType`

**Public Methods:**

- `LogicalCreateType(std::string typeName, common::LogicalType type, s...`
- `getExpressionsForPrinting() const override`
- `getType() const`
- `getPrintInfo() const override`
- `copy() final`


---

## LogicalCreateTypePrintInfo

**Full name:** `neug::planner::LogicalCreateTypePrintInfo`

**Public Methods:**

- `LogicalCreateTypePrintInfo(std::string typeName, std::string type)`
- `toString() const override`
- `copy() const override`


---

## LogicalCrossProduct

**Full name:** `neug::planner::LogicalCrossProduct`

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

**Full name:** `neug::planner::LogicalDatabase`

**Public Methods:**

- `LogicalDatabase(LogicalOperatorType operatorType, std::shared_ptr...`
- `getDBName() const`
- `getExpressionsForPrinting() const override`


---

## LogicalDelete

**Full name:** `neug::planner::LogicalDelete`

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

**Full name:** `neug::planner::LogicalDeletePrintInfo`

**Public Methods:**

- `LogicalDeletePrintInfo(std::vector< binder::BoundDeleteInfo > infos)`
- `toString() const override`


---

## LogicalDetachDatabase

**Full name:** `neug::planner::LogicalDetachDatabase`

**Public Methods:**

- `LogicalDetachDatabase(std::string dbName, std::shared_ptr< binder::Expr...`
- `copy() override`


---

## LogicalDistinct

**Full name:** `neug::planner::LogicalDistinct`

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

**Full name:** `neug::planner::LogicalDrop`

**Public Methods:**

- `LogicalDrop(parser::DropInfo dropInfo, std::shared_ptr< binde...`
- `getExpressionsForPrinting() const override`
- `getDropInfo() const`
- `getPrintInfo() const override`
- `copy() override`


---

## LogicalDropPrintInfo

**Full name:** `neug::planner::LogicalDropPrintInfo`

**Public Methods:**

- `LogicalDropPrintInfo(std::string name)`
- `toString() const override`


---

## LogicalDummyScan

**Full name:** `neug::planner::LogicalDummyScan`

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

**Full name:** `neug::planner::LogicalDummySink`

**Public Methods:**

- `LogicalDummySink(std::shared_ptr< LogicalOperator > child)`
- `LogicalDummySink(logical_op_vector_t children)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalEmptyResult

**Full name:** `neug::planner::LogicalEmptyResult`

**Public Methods:**

- `LogicalEmptyResult(const Schema &schema)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalExplain

**Full name:** `neug::planner::LogicalExplain`

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

**Full name:** `neug::planner::LogicalExportDatabase`

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

**Full name:** `neug::planner::LogicalExpressionsScan`

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

**Full name:** `neug::planner::LogicalExtend`

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

**Full name:** `neug::planner::LogicalExtension`

**Public Methods:**

- `LogicalExtension(std::unique_ptr< ExtensionAuxInfo > auxInfo, std:...`
- `getActionForPrinting() const`
- `getExpressionsForPrinting() const override`
- `getAuxInfo() const`
- `copy() override`


---

## LogicalFilter

**Full name:** `neug::planner::LogicalFilter`

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

**Full name:** `neug::planner::LogicalFilterPrintInfo`

**Public Methods:**

- `LogicalFilterPrintInfo(std::shared_ptr< binder::Expression > expression)`
- `toString() const override`


---

## LogicalFlatten

**Full name:** `neug::planner::LogicalFlatten`

**Public Methods:**

- `LogicalFlatten(f_group_pos groupPos, std::shared_ptr< LogicalOpe...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getGroupPos() const`
- `copy() override`


---

## LogicalGetV

**Full name:** `neug::planner::LogicalGetV`

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

**Full name:** `neug::planner::LogicalHashJoin`

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
- ... and 11 more methods


---

## LogicalImportDatabase

**Full name:** `neug::planner::LogicalImportDatabase`

**Public Methods:**

- `LogicalImportDatabase(std::string query, std::string indexQuery, std::s...`
- `getQuery() const`
- `getIndexQuery() const`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalInsert

**Full name:** `neug::planner::LogicalInsert`

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

**Full name:** `neug::planner::LogicalInsertInfo`

**Public Methods:**

- `LogicalInsertInfo(common::TableType tableType, std::shared_ptr< bin...`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalInsertInfo)`


---

## LogicalIntersect

**Full name:** `neug::planner::LogicalIntersect`

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

**Full name:** `neug::planner::LogicalLimit`

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

**Full name:** `neug::planner::LogicalMerge`

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

**Full name:** `neug::planner::LogicalMultiplicityReducer`

**Public Methods:**

- `LogicalMultiplicityReducer(std::shared_ptr< LogicalOperator > child)`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `copy() override`


---

## LogicalNodeLabelFilter

**Full name:** `neug::planner::LogicalNodeLabelFilter`

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

**Full name:** `neug::planner::LogicalOperator`

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

**Full name:** `neug::planner::LogicalOperatorUtils`

**Public Methods:**

- `logicalOperatorTypeToString(LogicalOperatorType type)`
- `isUpdate(LogicalOperatorType type)`
- `isAccHashJoin(const LogicalOperator &op)`


---

## LogicalOrderBy

**Full name:** `neug::planner::LogicalOrderBy`

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

**Full name:** `neug::planner::LogicalPartitioner`

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

**Full name:** `neug::planner::LogicalPartitionerInfo`

**Public Methods:**

- `LogicalPartitionerInfo(catalog::TableCatalogEntry *tableEntry, std::shar...`
- `LogicalPartitionerInfo(const LogicalPartitionerInfo &other)`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalPartitionerInfo)`
- `getNumInfos() const`
- `getInfo(common::idx_t idx)`
- `getInfo(common::idx_t idx) const`


---

## LogicalPartitioningInfo

**Full name:** `neug::planner::LogicalPartitioningInfo`

**Public Methods:**

- `LogicalPartitioningInfo(common::idx_t keyIdx)`
- `LogicalPartitioningInfo(const LogicalPartitioningInfo &other)`
- `EXPLICIT_COPY_DEFAULT_MOVE(LogicalPartitioningInfo)`


---

## LogicalPathPropertyProbe

**Full name:** `neug::planner::LogicalPathPropertyProbe`

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

**Full name:** `neug::planner::LogicalPlan`

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

**Full name:** `neug::planner::LogicalPlanUtil`

**Public Methods:**

- `encodeJoin(LogicalPlan &logicalPlan)`


---

## LogicalPrimaryKeyLookup

**Full name:** `neug::planner::LogicalPrimaryKeyLookup`

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

**Full name:** `neug::planner::LogicalProjection`

**Public Methods:**

- `LogicalProjection(binder::expression_vector expressions, std::share...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getExpressionsForPrinting() const override`
- `getExpressionsToProject() const`
- `getExpressionsToProjectRef()`
- `getDiscardedGroupsPos() const`
- `copy() override`
- `resetExprUniqueNames()`


---

## LogicalRecursiveExtend

**Full name:** `neug::planner::LogicalRecursiveExtend`

**Public Methods:**

- `LogicalRecursiveExtend(std::unique_ptr< function::RJAlgorithm > function...`
- `LogicalRecursiveExtend(std::unique_ptr< function::RJAlgorithm > function...`
- `computeFlatSchema() override`
- `computeFactorizedSchema() override`
- `getDirection() const`
- `setFusionType(neug::optimizer::FusionType fusionType_)`
- `getFusionType() const`
- `setFunction(std::unique_ptr< function::RJAlgorithm > func)`
- `getFunction() const`
- `getBindData() const`
- ... and 23 more methods


---

## LogicalScanNodeTable

**Full name:** `neug::planner::LogicalScanNodeTable`

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
- ... and 16 more methods


---

## LogicalScanNodeTablePrintInfo

**Full name:** `neug::planner::LogicalScanNodeTablePrintInfo`

**Public Methods:**

- `LogicalScanNodeTablePrintInfo(std::shared_ptr< binder::Expression > nodeID, bin...`
- `toString() const override`


---

## LogicalSemiMasker

**Full name:** `neug::planner::LogicalSemiMasker`

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

**Full name:** `neug::planner::LogicalSetProperty`

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

**Full name:** `neug::planner::LogicalSimple`

**Public Methods:**

- `LogicalSimple(LogicalOperatorType operatorType, std::shared_ptr...`
- `LogicalSimple(LogicalOperatorType operatorType, const std::vect...`
- `computeFactorizedSchema() override`
- `computeFlatSchema() override`
- `getOutputExpression() const`


---

## LogicalStandaloneCall

**Full name:** `neug::planner::LogicalStandaloneCall`

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

**Full name:** `neug::planner::LogicalTableFunctionCall`

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

**Full name:** `neug::planner::LogicalTransaction`

**Public Methods:**

- `LogicalTransaction(transaction::TransactionAction transactionAction)`
- `getExpressionsForPrinting() const final`
- `computeFlatSchema() final`
- `computeFactorizedSchema() final`
- `getTransactionAction() const`
- `copy() final`


---

## LogicalUnion

**Full name:** `neug::planner::LogicalUnion`

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

**Full name:** `neug::planner::LogicalUnwind`

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

**Full name:** `neug::planner::LogicalUseDatabase`

**Public Methods:**

- `LogicalUseDatabase(std::string dbName, std::shared_ptr< binder::Expr...`
- `copy() override`


---

## NodeRelScanInfo

**Full name:** `neug::planner::NodeRelScanInfo`

**Public Methods:**

- `NodeRelScanInfo(std::shared_ptr< binder::Expression > nodeOrRel, ...`


---

## Planner

**Full name:** `neug::planner::Planner`

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
- ... and 122 more methods


---

## PrimaryKey

**Full name:** `neug::planner::PrimaryKey`

**Public Methods:**

- `PrimaryKey(const std::string &key, PrimaryKeyScanInfo *value)`


---

## PrimaryKeyScanInfo

**Full name:** `neug::planner::PrimaryKeyScanInfo`

**Public Methods:**

- `PrimaryKeyScanInfo(std::shared_ptr< binder::Expression > key)`
- `copy() const override`


---

## PropertyExprCollection

**Full name:** `neug::planner::PropertyExprCollection`

**Public Methods:**

- `addProperties(const std::string &patternName, std::shared_ptr< ...`
- `getProperties(const binder::Expression &pattern) const`
- `getProperties() const`
- `clear()`


---

## QueryGraphPlanningInfo

**Full name:** `neug::planner::QueryGraphPlanningInfo`

**Public Methods:**

- `containsCorrExpr(const binder::Expression &expr) const`


---

## SIPInfo

**Full name:** `neug::planner::SIPInfo`


---

## Schema

**Full name:** `neug::planner::Schema`

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

**Full name:** `neug::planner::SchemaUtils`

**Public Methods:**

- `getLeadingGroupPos(const std::unordered_set< f_group_pos > &groupPos...`
- `validateAtMostOneUnFlatGroup(const std::unordered_set< f_group_pos > &groupPos...`
- `validateNoUnFlatGroup(const std::unordered_set< f_group_pos > &groupPos...`


---

## SinkOperatorUtil

**Full name:** `neug::planner::SinkOperatorUtil`

**Public Methods:**

- `mergeSchema(const Schema &inputSchema, const binder::expressi...`
- `recomputeSchema(const Schema &inputSchema, const binder::expressi...`


---

## SubPlansTable

**Full name:** `neug::planner::SubPlansTable`

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

**Full name:** `neug::planner::SubgraphPlans`

**Public Methods:**

- `SubgraphPlans(const binder::SubqueryGraph &subqueryGraph)`
- `getMaxCost() const`
- `addPlan(std::unique_ptr< LogicalPlan > plan)`
- `getPlans()`


---

## TreeNodeTypeUtils

**Full name:** `neug::planner::TreeNodeTypeUtils`

**Public Methods:**

- `toString(TreeNodeType type)`


---

## CopyFromFileError

**Full name:** `neug::processor::CopyFromFileError`

**Public Methods:**

- `CopyFromFileError(std::string message, WarningSourceData warningDat...`
- `operator<(const CopyFromFileError &o) const`


---

## DataChunkDescriptor

**Full name:** `neug::processor::DataChunkDescriptor`

**Public Methods:**

- `DataChunkDescriptor(bool isSingleState)`
- `DataChunkDescriptor(const DataChunkDescriptor &other)`
- `copy() const`


---

## DataPos

**Full name:** `neug::processor::DataPos`

**Public Methods:**

- `DataPos()`
- `DataPos(data_chunk_pos_t dataChunkPos, value_vector_pos_t...`
- `DataPos(std::pair< data_chunk_pos_t, value_vector_pos_t > pos)`
- `isValid() const`
- `operator==(const DataPos &rhs) const`
- `getInvalidPos()`


---

## ExecutionContext

**Full name:** `neug::processor::ExecutionContext`

**Public Methods:**

- `ExecutionContext(common::Profiler *profiler, main::ClientContext *...`


---

## ExpressionMapper

**Full name:** `neug::processor::ExpressionMapper`

**Public Methods:**

- `ExpressionMapper()=default`
- `ExpressionMapper(const planner::Schema *schema)`
- `ExpressionMapper(const planner::Schema *schema, evaluator::Express...`
- `getEvaluator(std::shared_ptr< binder::Expression > expression)`
- `getConstantEvaluator(std::shared_ptr< binder::Expression > expression)`


---

## LineContext

**Full name:** `neug::processor::LineContext`

**Public Methods:**

- `setNewLine(uint64_t start)`
- `setEndOfLine(uint64_t end)`


---

## OperatorMetrics

**Full name:** `neug::processor::OperatorMetrics`

**Public Methods:**

- `OperatorMetrics(common::TimeMetric &executionTime, common::Numeri...`


---

## PhysicalOperator

**Full name:** `neug::processor::PhysicalOperator`

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

**Full name:** `neug::processor::PhysicalOperatorUtils`

**Public Methods:**

- `operatorToString(const PhysicalOperator *physicalOp)`


---

## PopulatedCopyFromError

**Full name:** `neug::processor::PopulatedCopyFromError`


---

## ResultSet

**Full name:** `neug::processor::ResultSet`

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

**Full name:** `neug::processor::ResultSetDescriptor`

**Public Methods:**

- `ResultSetDescriptor()=default`
- `ResultSetDescriptor(std::vector< std::unique_ptr< DataChunkDescriptor...`
- `ResultSetDescriptor(planner::Schema *schema)`
- `DELETE_BOTH_COPY(ResultSetDescriptor)`
- `copy() const`


---

## WarningContext

**Full name:** `neug::processor::WarningContext`

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

**Full name:** `neug::processor::WarningInfo`

**Public Methods:**

- `WarningInfo(PopulatedCopyFromError warning, uint64_t queryID)`


---

## WarningSourceData

**Full name:** `neug::processor::WarningSourceData`

**Public Methods:**

- `WarningSourceData()`
- `WarningSourceData(uint64_t numSourceSpecificValues)`
- `dumpTo(uint64_t &blockIdx, uint32_t &offsetInBlock, Type...`
- `getBlockIdx() const`
- `getOffsetInBlock() const`
- `constructFrom(uint64_t blockIdx, uint32_t offsetInBlock, Types....`
- `constructFromData(const std::vector< T * > &chunks, common::idx_t pos)`


---

## ArrowCsvOptionsBuilder

**Full name:** `neug::reader::ArrowCsvOptionsBuilder`

CSV-specific implementation of Arrow scan options builder.

This class extends `ArrowOptionsBuilder` to provide CSV-specific functionality:
- `buildFragmentOptions()`: Builds CsvFragmentScanOptions with parse_options (delimiter, quoting, escaping, etc.) from C...

**Public Methods:**

- `ArrowCsvOptionsBuilder(std::shared_ptr< ReadSharedState > state)` - Constructs an ArrowCsvScanOptionsBuilder with the given shared state
- `build() const override` - Builds `ArrowOptions` from the shared state


---

## ArrowExpressionConverter

**Full name:** `neug::reader::ArrowExpressionConverter`

Converts common::Expression to Arrow compute expressions.

This class implements expression conversion using a shunting-yard-like algorithm to handle infix expressions with operator precedence. The input expression is in natural order (left-to-right), where o...

**Public Methods:**

- `convert(const ::common::Expression &expr) override`


---

## ArrowOperatorPrecedence

**Full name:** `neug::reader::ArrowOperatorPrecedence`

Helper class to get operator precedence for Arrow expression conversion IMPORTANT: Lower numbers = higher precedence (e.g., 3 > 4 means * has higher precedence than +) Priority order (from highest to lowest): NOT: 2 MUL/DIV/MOD: 3 ADD/SUB: 4 Comparison (GT, LT, etc.): 6 EQ/NE: 7 AND: 11 OR: 12 LEFT_BRACE: 17 (special value to prevent premature operator application)

**Public Methods:**

- `getPrecedence(const ::common::ExprOpr &opr)`


---

## ArrowOptions

**Full name:** `neug::reader::ArrowOptions`

Structure containing Arrow dataset scan options and file format.

This structure encapsulates both the scan options (schema, projection, filtering, etc.) and the file format (CSV, Parquet, etc.) needed to read Arrow datasets. The scanOptions and fileFormat are built...


---

## ArrowOptionsBuilder

**Full name:** `neug::reader::ArrowOptionsBuilder`

Base class for building Arrow dataset scan options.

This class builds `ArrowOptions` from `ReadSharedState`, which includes both arrow::dataset::ScanOptions and arrow::dataset::FileFormat. The `build()` method performs the following operations:
- Datas...

**Public Methods:**

- `ArrowOptionsBuilder(std::shared_ptr< ReadSharedState > state)` - Constructs an `ArrowOptionsBuilder` with the given shared state
- `~ArrowOptionsBuilder() override=default`
- `build() const override=0` - Builds `ArrowOptions` from the shared state
- `skipColumns(ArrowOptions &options) override` - Applies column projection to exclude skipped columns
- `skipRows(ArrowOptions &options) override` - Applies row filtering based on filter expressions


---

## ArrowReader

**Full name:** `neug::reader::ArrowReader`

Arrow-based reader implementation for reading data from external files.

- Full read: loads entire dataset into memory as a `Table`
- Batch read: streams data in batches using RecordBatchReader
The reader uses an `ArrowOptionsBuilder` to configure scan options including sc...

**Public Methods:**

- `ArrowReader(std::shared_ptr< ReadSharedState > sharedState, s...`
- `~ArrowReader() override=default`
- `read(std::shared_ptr< ReadLocalState > localState, run...` - Read data from local state and populate context
- `inferSchema()` - Infer schema from external files using Arrow Dataset API


---

## ArrowSniffer

**Full name:** `neug::reader::ArrowSniffer`

Arrow-based schema inference implementation.

The sniffing process:
1. Uses `ArrowReader` to create a FileSystemDatasetFactory
2. Calls factory->Inspect() to infer schema from sample data`/metadata`
3. Converts Arrow `Schema` to `EntrySchema` (co...

**Public Methods:**

- `ArrowSniffer(std::shared_ptr< ArrowReader > reader)` - Constructs an `ArrowSniffer` with an `ArrowReader` shared pointer
- `sniff() override` - Infer schema from external files using Arrow Dataset API


---

## ArrowTypeConverter

**Full name:** `neug::reader::ArrowTypeConverter`

Converts common::DataType (protobuf) to Arrow `DataType`.

This class converts protobuf `DataType` definitions to Arrow `DataType` instances. Supported types include:
- Primitive types: BOOL, INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE
- String types: all str...

**Public Methods:**

- `convert(const ::common::DataType &type) override` - Convert common::DataType to Arrow `DataType`
- `convert(const arrow::DataType &type) override` - Convert Arrow `DataType` to common::DataType


---

## CSVParseOptions

**Full name:** `neug::reader::CSVParseOptions`


---

## EdgeEntrySchema

**Full name:** `neug::reader::EdgeEntrySchema`

Edge entry schema.

Extends `EntrySchema` with edge-specific metadata including edge label, source`/destination` vertex labels, and source`/destination` column names. Used for representing edge table schemas in graph dat...

**Public Methods:**

- `type() const override`


---

## EntrySchema

**Full name:** `neug::reader::EntrySchema`

Base entry schema containing column information.

This struct represents the schema of an external table entry, containing column names and their corresponding logical types. It is used to store inferred or declared types for external data sources.

**Public Methods:**

- `~EntrySchema()=default`
- `type() const =0`
- `cast()`
- `ptrCast()`
- `constCast() const`
- `constPtrCast() const`


---

## ExpressionConverter

**Full name:** `neug::reader::ExpressionConverter`

Template base class for converting expressions to target format.

This template class provides a generic interface for converting internal common::Expression to target expression types (e.g., Arrow compute expressions). Derived classes implement the conversion logic...

**Public Methods:**

- `convert(const ::common::Expression &expr)=0`


---

## ExternalSchema

**Full name:** `neug::reader::ExternalSchema`

External schema combining entry and file information.

This struct combines `EntrySchema` (column information) and `FileSchema` (file access information) to provide complete metadata for external tables. It is used for maintaining external table metadata ...


---

## FileSchema

**Full name:** `neug::reader::FileSchema`

File schema containing file information.

This class contains all file-related metadata including:
- File paths: supports multiple files for batch reading
- Format type: identifies the file format (csv, parquet, json)
- URL options: configura...


---

## Option

**Full name:** `neug::reader::Option`

**Public Methods:**

- `Option(std::string key, std::string default_val, ParseFu...`
- `get(const options_t &options) const`
- `getKey() const`
- `Int32Option(const std::string &key, int32_t default_val)`
- `Int64Option(const std::string &key, int64_t default_val)`
- `StringOption(const std::string &key, const std::string &default_val)`
- `CharOption(const std::string &key, char default_val)`
- `BoolOption(const std::string &key, bool default_val)`


---

## OptionsBuilder

**Full name:** `neug::reader::OptionsBuilder`

Template base class for building format-specific scan options.

This template class provides a generic interface for building scan options for different data formats. It handles column projection (skipColumns) and row filtering (skipRows) operations. Derived class...

**Public Methods:**

- `OptionsBuilder(std::shared_ptr< ReadSharedState > state)` - Constructs an `OptionsBuilder` with the given shared state
- `~OptionsBuilder()=default`
- `build() const =0` - Builds the options structure from the shared state
- `skipColumns(T &options)` - Applies column projection to exclude skipped columns
- `skipRows(T &options)` - Applies row filtering based on filter expressions


---

## ReadLocalState

**Full name:** `neug::reader::ReadLocalState`

Base class for local read state.

This struct represents format-specific local state for reading operations. Format-specific readers can extend this struct to add their own local state (e.g., file handles, current position, buffer sta...

**Public Methods:**

- `~ReadLocalState()=default`
- `cast()`
- `ptrCast()`
- `constCast() const`
- `constPtrCast() const`


---

## ReadOptions

**Full name:** `neug::reader::ReadOptions`


---

## ReadSharedState

**Full name:** `neug::reader::ReadSharedState`

Shared state for reading operations.

This struct contains shared state information used across multiple read operations. It includes:
- `Schema` information: external table metadata (column names, types, file info)
- Column pruning: list...

**Public Methods:**

- `columnNum()` - Get the number of columns after column pruning


---

## Reader

**Full name:** `neug::reader::Reader`

Base `Reader` class template.

This template class provides a unified interface for reading data from external files. It is parameterized by FileSystem type to support different filesystem implementations (e.g., local filesystem, S...

**Public Methods:**

- `Reader(std::shared_ptr< ReadSharedState > sharedState, s...`
- `~Reader()=default`
- `read(std::shared_ptr< ReadLocalState > localState, run...` - Read data from local state and populate context


---

## Sniffer

**Full name:** `neug::reader::Sniffer`

Base class for schema inference from external files.

`Sniffer` is responsible for inferring column types and names from external files by reading a sample of the data (first block for CSV/JSON) or metadata (for Parquet). Derived classes implement format...

**Public Methods:**

- `~Sniffer()=default`
- `sniff()=0` - Infer schema from external file


---

## TableEntrySchema

**Full name:** `neug::reader::TableEntrySchema`

**Public Methods:**

- `type() const override`


---

## TypeConverter

**Full name:** `neug::reader::TypeConverter`

Template base class for converting types to target format.

This template class provides a generic interface for converting internal common::DataType (protobuf) to target type systems (e.g., Arrow `DataType`). Derived classes implement the conversion logic for...

**Public Methods:**

- `convert(const ::common::DataType &type)=0`
- `convert(const TargetType &type)=0`


---

## VertexEntrySchema

**Full name:** `neug::reader::VertexEntrySchema`

Vertex entry schema.

Extends `EntrySchema` with vertex-specific metadata including vertex label and primary key column name. Used for representing vertex table schemas in graph databases.

**Public Methods:**

- `type() const override`


---

## AbsExpr

**Full name:** `neug::runtime::AbsExpr`

**Public Methods:**

- `AbsExpr(std::unique_ptr< ExprBase > &&args)`
- `abs_impl(const Value &val) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## AddOp

**Full name:** `neug::runtime::AddOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## AndOpExpr

**Full name:** `neug::runtime::AndOpExpr`

**Public Methods:**

- `AndOpExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `is_optional() const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`


---

## ArithExpr

**Full name:** `neug::runtime::ArithExpr`

**Public Methods:**

- `ArithExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## ArrowArrayAccessor

**Full name:** `neug::runtime::ArrowArrayAccessor`

**Public Methods:**

- `ArrowArrayAccessor(const Context &ctx, int tag)`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## ArrowArrayContextColumn

**Full name:** `neug::runtime::ArrowArrayContextColumn`

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
- `get_elem(size_t idx) const override`
- ... and 2 more methods


---

## ArrowArrayContextColumnBuilder

**Full name:** `neug::runtime::ArrowArrayContextColumnBuilder`

**Public Methods:**

- `ArrowArrayContextColumnBuilder()=default`
- `~ArrowArrayContextColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `finish() override`
- `push_back(const std::shared_ptr< arrow::Array > &column)`


---

## ArrowStreamContextColumn

**Full name:** `neug::runtime::ArrowStreamContextColumn`

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

**Full name:** `neug::runtime::ArrowStreamContextColumnBuilder`

Each column take data from the streamReader's one column.

**Public Methods:**

- `ArrowStreamContextColumnBuilder(const std::vector< std::shared_ptr< IRecordBatchS...`
- `~ArrowStreamContextColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const runtime::Value &val) override`
- `finish() override`


---

## BDMLEdgeColumn

**Full name:** `neug::runtime::BDMLEdgeColumn`

**Public Methods:**

- `BDMLEdgeColumn()`
- `~BDMLEdgeColumn()=default`
- `get_edge(size_t idx) const override`
- `size() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `edge_column_type() const override`
- ... and 2 more methods


---

## BDMLEdgeColumnBuilder

**Full name:** `neug::runtime::BDMLEdgeColumnBuilder`

**Public Methods:**

- `BDMLEdgeColumnBuilder(const std::vector< LabelTriplet > &labels)`
- `~BDMLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_opt(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `push_back_opt(int label_idx, vid_t src, vid_t dst, const void *...`
- `push_back_null() override`
- `finish() override`
- `get_label_index(const LabelTriplet &label) const`
- `insert_label(const LabelTriplet &label)`


---

## BDSLEdgeColumn

**Full name:** `neug::runtime::BDSLEdgeColumn`

**Public Methods:**

- `BDSLEdgeColumn(const LabelTriplet &label)`
- `get_edge(size_t idx) const override`
- `size() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `edge_column_type() const override`
- `is_optional() const override`
- ... and 2 more methods


---

## BDSLEdgeColumnBuilder

**Full name:** `neug::runtime::BDSLEdgeColumnBuilder`

**Public Methods:**

- `BDSLEdgeColumnBuilder(const LabelTriplet &label)`
- `~BDSLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_opt(vid_t src, vid_t dst, const void *prop, Direction dir)`
- `push_back_null() override`
- `finish() override`


---

## BetweenCmp

**Full name:** `neug::runtime::BetweenCmp`

**Public Methods:**

- `BetweenCmp()=default`
- `BetweenCmp(const T &from, const T &to)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## CaseWhenExpr

**Full name:** `neug::runtime::CaseWhenExpr`

**Public Methods:**

- `CaseWhenExpr(std::vector< std::pair< std::unique_ptr< ExprBase...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## ColumnsUtils

**Full name:** `neug::runtime::ColumnsUtils`

**Public Methods:**

- `generate_dedup_offset(const VEC_T &vec, size_t row_num, std::vector< si...`
- `generate_optional_dedup_offset(const VEC_T &vec, const std::vector< bool > &vali...`
- `create_builder(const DataType &type)`


---

## ConstAccessor

**Full name:** `neug::runtime::ConstAccessor`

**Public Methods:**

- `ConstAccessor(const T &val)`
- `typed_eval_path(size_t) const`
- `typed_eval_vertex(label_t, vid_t, size_t) const`
- `typed_eval_edge(const LabelTriplet &, vid_t, vid_t, const Propert...`
- `eval_path(size_t) const override`
- `eval_vertex(label_t, vid_t) const override`
- `eval_edge(const LabelTriplet &, vid_t, vid_t, const void *d...`


---

## ConstExpr

**Full name:** `neug::runtime::ConstExpr`

**Public Methods:**

- `ConstExpr(const Value &val)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Context

**Full name:** `neug::runtime::Context`

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

## ContextListAccessor

**Full name:** `neug::runtime::ContextListAccessor`

**Public Methods:**

- `ContextListAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## ContextMeta

**Full name:** `neug::runtime::ContextMeta`

**Public Methods:**

- `ContextMeta()=default`
- `~ContextMeta()=default`
- `exist(int alias) const`
- `set(int alias)`
- `columns() const`
- `desc() const`


---

## ContextPathAccessor

**Full name:** `neug::runtime::ContextPathAccessor`

**Public Methods:**

- `ContextPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## ContextStructAccessor

**Full name:** `neug::runtime::ContextStructAccessor`

**Public Methods:**

- `ContextStructAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## ContextValueAccessor

**Full name:** `neug::runtime::ContextValueAccessor`

**Public Methods:**

- `ContextValueAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## CsvExportWriter

**Full name:** `neug::runtime::CsvExportWriter`

**Public Methods:**

- `CsvExportWriter(const std::string &file_path, const std::vector< ...`
- `~CsvExportWriter()`
- `Write(const std::vector< std::shared_ptr< IContextColum...`
- `Make(const std::string &file_path, const std::vector< ...`


---

## Dedup

**Full name:** `neug::runtime::Dedup`

**Public Methods:**

- `dedup(Context &&ctx, const std::vector< size_t > &cols)`


---

## DivOp

**Full name:** `neug::runtime::DivOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## DummyGetter

**Full name:** `neug::runtime::DummyGetter`

**Public Methods:**

- `DummyGetter(int from, int to)`
- `evaluate(const Context &ctx, Context &&ret) override`
- `alias() const override`


---

## DummyVertexPredicate

**Full name:** `neug::runtime::DummyVertexPredicate`

**Public Methods:**

- `operator()(label_t label, vid_t v) const`


---

## EQCmp

**Full name:** `neug::runtime::EQCmp`

**Public Methods:**

- `EQCmp()=default`
- `EQCmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## EdgeAndNbrPredicate

**Full name:** `neug::runtime::EdgeAndNbrPredicate`

**Public Methods:**

- `EdgeAndNbrPredicate(const VERTEX_PRED_T &vertex_pred_, const EDGE_PRE...`
- `operator()(label_t label, vid_t src, label_t nbr_label, vid_...`


---

## EdgeExpand

**Full name:** `neug::runtime::EdgeExpand`

**Public Methods:**

- `expand_degree(const StorageReadInterface &graph, Context &&ctx,...`
- `expand_edge(const StorageReadInterface &graph, Context &&ctx,...`
- `expand_edge_with_special_edge_predicate(const StorageReadInterface &graph, Context &&ctx,...`
- `expand_vertex(const StorageReadInterface &graph, Context &&ctx,...`
- `expand_vertex_ep_cmp(const StorageReadInterface &graph, Context &&ctx,...`
- `expand_vertex_with_special_vertex_predicate(const StorageReadInterface &graph, Context &&ctx,...`
- `tc(const StorageReadInterface &graph, Context &&ctx,...`
- `remove_null_from_ctx(Context &&ctx, int tag_id)`


---

## EdgeExpandParams

**Full name:** `neug::runtime::EdgeExpandParams`


---

## EdgeGIdPathAccessor

**Full name:** `neug::runtime::EdgeGIdPathAccessor`

**Public Methods:**

- `EdgeGIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## EdgeIdPathAccessor

**Full name:** `neug::runtime::EdgeIdPathAccessor`

**Public Methods:**

- `EdgeIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`
- `is_optional() const override`


---

## EdgeLabelPathAccessor

**Full name:** `neug::runtime::EdgeLabelPathAccessor`

**Public Methods:**

- `EdgeLabelPathAccessor(const Schema &schema, const Context &ctx, int tag)`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`


---

## EdgeNbrPredicate

**Full name:** `neug::runtime::EdgeNbrPredicate`

**Public Methods:**

- `EdgeNbrPredicate(const VERTEX_PRED_T &vertex_pred_)`
- `operator()(label_t label, vid_t src, label_t nbr_label, vid_...`


---

## EdgePropertyCmpPredicate

**Full name:** `neug::runtime::EdgePropertyCmpPredicate`

**Public Methods:**

- `EdgePropertyCmpPredicate(const GETTER_T &getter, const CMP_T &cmp)`
- `~EdgePropertyCmpPredicate()=default`
- `operator()(label_t v_label, vid_t v, label_t nbr_label, vid_...`


---

## EdgePropertyEdgeAccessor

**Full name:** `neug::runtime::EdgePropertyEdgeAccessor`

**Public Methods:**

- `EdgePropertyEdgeAccessor(const StorageReadInterface &graph, const std::string &name)`
- `typed_eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_path(size_t idx) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## EdgePropertyPathAccessor

**Full name:** `neug::runtime::EdgePropertyPathAccessor`

**Public Methods:**

- `EdgePropertyPathAccessor(const StorageReadInterface &graph, const std::str...`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`
- `is_optional() const override`


---

## EdgeRecord

**Full name:** `neug::runtime::EdgeRecord`

**Public Methods:**

- `operator<(const EdgeRecord &e) const`
- `operator==(const EdgeRecord &e) const`
- `start_node() const`
- `end_node() const`
- `to_string() const`


---

## EndNodeExpr

**Full name:** `neug::runtime::EndNodeExpr`

**Public Methods:**

- `EndNodeExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## EqOp

**Full name:** `neug::runtime::EqOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## ExportWriterFactory

**Full name:** `neug::runtime::ExportWriterFactory`

**Public Methods:**

- `CreateExportWriter(const std::string &name, const std::string &file_...`
- `Register(const std::string &name, writer_initializer_t initializer)`


---

## Expr

**Full name:** `neug::runtime::Expr`

**Public Methods:**

- `Expr(const StorageReadInterface *graph, const Context ...`
- `eval_path(size_t idx) const`
- `eval_vertex(label_t label, vid_t v) const`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const`
- `is_optional() const`


---

## ExprBase

**Full name:** `neug::runtime::ExprBase`

**Public Methods:**

- `eval_path(size_t idx) const =0`
- `eval_vertex(label_t label, vid_t v) const =0`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const =0`
- `is_optional() const`
- `~ExprBase()=default`


---

## ExtractExpr

**Full name:** `neug::runtime::ExtractExpr`

**Public Methods:**

- `ExtractExpr(std::unique_ptr< ExprBase > &&expr, const ::commo...`
- `eval_impl(const Value &val) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## GECmp

**Full name:** `neug::runtime::GECmp`

**Public Methods:**

- `GECmp()=default`
- `GECmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## GKey

**Full name:** `neug::runtime::GKey`

**Public Methods:**

- `GKey(std::vector< EXPR > &&exprs, const std::vector< s...`
- `group(const Context &ctx) override`
- `tag_alias() const override`


---

## GTCmp

**Full name:** `neug::runtime::GTCmp`

**Public Methods:**

- `GTCmp()=default`
- `GTCmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## GeOp

**Full name:** `neug::runtime::GeOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## GeneralEdgePredicate

**Full name:** `neug::runtime::GeneralEdgePredicate`

**Public Methods:**

- `GeneralEdgePredicate(const StorageReadInterface &graph, const Context ...`
- `operator()(label_t label, vid_t src, label_t nbr_label, vid_...`


---

## GeneralVertexPredicate

**Full name:** `neug::runtime::GeneralVertexPredicate`

**Public Methods:**

- `GeneralVertexPredicate(const StorageReadInterface &graph, const Context ...`
- `operator()(label_t label, vid_t v) const`


---

## GetV

**Full name:** `neug::runtime::GetV`

**Public Methods:**

- `get_vertex_from_edges_impl(const IEdgeColumn &input_edge_list, const GetVPar...`
- `_get_vertex_from_path(const StorageReadInterface &graph, Context &&ctx,...`
- `get_vertex_from_edges(const StorageReadInterface &graph, Context &&ctx,...`
- `get_vertex_from_vertices(const StorageReadInterface &graph, Context &&ctx,...`


---

## GetVParams

**Full name:** `neug::runtime::GetVParams`


---

## GroupBy

**Full name:** `neug::runtime::GroupBy`

**Public Methods:**

- `group_by(Context &&ctx, std::unique_ptr< KeyBase > &&key, ...`


---

## GtOp

**Full name:** `neug::runtime::GtOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## IAccessor

**Full name:** `neug::runtime::IAccessor`

**Public Methods:**

- `~IAccessor()=default`
- `eval_path(size_t idx) const`
- `eval_vertex(label_t label, vid_t v) const`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const`
- `name() const`


---

## IContextColumn

**Full name:** `neug::runtime::IContextColumn`

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
- ... and 6 more methods


---

## IContextColumnBuilder

**Full name:** `neug::runtime::IContextColumnBuilder`

**Public Methods:**

- `IContextColumnBuilder()=default`
- `~IContextColumnBuilder()=default`
- `reserve(size_t size)=0`
- `push_back_elem(const Value &val)=0`
- `push_back_null()`
- `finish()=0`


---

## IEdgeColumn

**Full name:** `neug::runtime::IEdgeColumn`

**Public Methods:**

- `IEdgeColumn()`
- `~IEdgeColumn()=default`
- `column_type() const override`
- `get_edge(size_t idx) const =0`
- `get_elem(size_t idx) const override`
- `dir() const`
- `elem_type() const override`
- `get_labels() const =0`
- `edge_column_type() const =0`


---

## IExportWriter

**Full name:** `neug::runtime::IExportWriter`

**Public Methods:**

- `~IExportWriter()=default`
- `Write(const std::vector< std::shared_ptr< IContextColum...`


---

## IOperator

**Full name:** `neug::runtime::IOperator`

**Public Methods:**

- `~IOperator()=default`
- `get_operator_name() const =0`
- `Eval(IStorageInterface &graph, const ParamsMap &params...`


---

## IOperatorBuilder

**Full name:** `neug::runtime::IOperatorBuilder`

**Public Methods:**

- `~IOperatorBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `stepping(int i)`
- `GetOpKinds() const =0`


---

## IPathColumn

**Full name:** `neug::runtime::IPathColumn`

**Public Methods:**

- `IPathColumn()=default`
- `~IPathColumn()=default`
- `get_path(size_t idx) const =0`
- `path_length(size_t idx) const`


---

## ISigColumn

**Full name:** `neug::runtime::ISigColumn`

**Public Methods:**

- `ISigColumn()=default`
- `~ISigColumn()=default`
- `get_sig(size_t idx) const =0`


---

## IValueColumn

**Full name:** `neug::runtime::IValueColumn`

**Public Methods:**

- `IValueColumn()=default`
- `~IValueColumn()=default`
- `get_value(size_t idx) const =0`


---

## IVertexColumn

**Full name:** `neug::runtime::IVertexColumn`

**Public Methods:**

- `IVertexColumn()`
- `~IVertexColumn()=default`
- `__attribute__((always_inline)) ContextColumnType column_type() ...`
- `vertex_column_type() const =0`
- `get_vertex(size_t idx) const =0`
- `get_elem(size_t idx) const override`
- `__attribute__((always_inline)) const DataType &elem_type() const override`
- `get_labels_set() const =0`


---

## IVertexColumnBuilder

**Full name:** `neug::runtime::IVertexColumnBuilder`

**Public Methods:**

- `IVertexColumnBuilder()=default`
- `~IVertexColumnBuilder()=default`
- `push_back_vertex(VertexRecord v)=0`
- `push_back_elem(const Value &val) override`


---

## Intersect

**Full name:** `neug::runtime::Intersect`

**Public Methods:**

- `Binary_Intersect_SL_Impl(const StorageReadInterface &graph, const ParamsMa...`
- `Binary_Intersect_ML_Impl(const StorageReadInterface &graph, const ParamsMa...`
- `Binary_Intersect(const StorageReadInterface &graph, const ParamsMa...`
- `Binary_Intersect_With_Edge(const StorageReadInterface &graph, const ParamsMa...`
- `Multiple_Intersect(const StorageReadInterface &graph, const ParamsMa...`


---

## Join

**Full name:** `neug::runtime::Join`

**Public Methods:**

- `join(Context &&ctx, Context &&ctx2, const JoinParams &params)`
- `pk_join(IStorageInterface &, Context &&ctx, const std::ve...`


---

## JoinParams

**Full name:** `neug::runtime::JoinParams`


---

## Key

**Full name:** `neug::runtime::Key`

**Public Methods:**

- `Key(EXPR &&expr, const std::vector< std::pair< int, i...`
- `group(const Context &ctx) override`
- `tag_alias() const override`


---

## KeyBase

**Full name:** `neug::runtime::KeyBase`

**Public Methods:**

- `~KeyBase()=default`
- `group(const Context &ctx)=0`
- `tag_alias() const =0`


---

## LECmp

**Full name:** `neug::runtime::LECmp`

**Public Methods:**

- `LECmp()=default`
- `LECmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## LTCmp

**Full name:** `neug::runtime::LTCmp`

**Public Methods:**

- `LTCmp()=default`
- `LTCmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## LabelTriplet

**Full name:** `neug::runtime::LabelTriplet`

**Public Methods:**

- `LabelTriplet()=default`
- `LabelTriplet(label_t src, label_t dst, label_t edge)`
- `to_string() const`
- `operator==(const LabelTriplet &rhs) const`
- `operator<(const LabelTriplet &rhs) const`


---

## LeOp

**Full name:** `neug::runtime::LeOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## Limit

**Full name:** `neug::runtime::Limit`

**Public Methods:**

- `limit(Context &&ctxs, size_t lower, size_t upper)`


---

## ListColumn

**Full name:** `neug::runtime::ListColumn`

**Public Methods:**

- `ListColumn(DataType type)`
- `~ListColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `generate_signature() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- ... and 1 more methods


---

## ListColumnBase

**Full name:** `neug::runtime::ListColumnBase`

**Public Methods:**

- `unfold() const =0`


---

## ListColumnBuilder

**Full name:** `neug::runtime::ListColumnBuilder`

**Public Methods:**

- `ListColumnBuilder(DataType type)`
- `~ListColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `finish() override`


---

## ListValue

**Full name:** `neug::runtime::ListValue`

**Public Methods:**

- `GetChildren(const Value &value)`


---

## LogicalExpr

**Full name:** `neug::runtime::LogicalExpr`

**Public Methods:**

- `LogicalExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_impl(const Value &lhs_val, const Value &rhs_val) const`
- `type() const override`
- `is_optional() const override`


---

## LtOp

**Full name:** `neug::runtime::LtOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## MLEdgePropertyGetter

**Full name:** `neug::runtime::MLEdgePropertyGetter`

**Public Methods:**

- `MLEdgePropertyGetter(const IStorageInterface &gi, const std::vector< L...`
- `~MLEdgePropertyGetter()=default`
- `get(label_t v_label, vid_t v, label_t nbr_label, vid_...`


---

## MLVertexColumn

**Full name:** `neug::runtime::MLVertexColumn`

**Public Methods:**

- `MLVertexColumn()=default`
- `~MLVertexColumn()=default`
- `__attribute__((always_inline)) size_t size() const override`
- `column_info() const override`
- `__attribute__((always_inline)) VertexColumnType vertex_column_t...`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `__attribute__((always_inline)) VertexRecord get_vertex(size_t i...`
- `__attribute__((always_inline)) bool is_optional() const override`
- `__attribute__((always_inline)) bool has_value(size_t idx) const override`
- ... and 4 more methods


---

## MLVertexColumnBuilder

**Full name:** `neug::runtime::MLVertexColumnBuilder`

**Public Methods:**

- `MLVertexColumnBuilder()`
- `MLVertexColumnBuilder(const std::set< label_t > &labels)`
- `~MLVertexColumnBuilder()=default`
- `reserve(size_t size) override`
- `__attribute__((always_inline)) void push_back_opt(VertexRecord v)`
- `push_back_vertex(VertexRecord v) override`
- `push_back_null() override`
- `finish() override`


---

## MLVertexColumnBuilderOpt

**Full name:** `neug::runtime::MLVertexColumnBuilderOpt`

**Public Methods:**

- `MLVertexColumnBuilderOpt(const std::set< label_t > &labels)`
- `~MLVertexColumnBuilderOpt()=default`
- `reserve(size_t size) override`
- `__attribute__((always_inline)) void push_back_opt(VertexRecord v)`
- `__attribute__((always_inline)) void push_back_vertex(VertexReco...`
- `__attribute__((always_inline)) void push_back_null() override`
- `__attribute__((always_inline)) std`
- `__attribute__((always_inline)) size_t cur_size()`


---

## MLVertexPropertyGetter

**Full name:** `neug::runtime::MLVertexPropertyGetter`

**Public Methods:**

- `MLVertexPropertyGetter(const IStorageInterface &gi, const std::string &p...`
- `~MLVertexPropertyGetter()=default`
- `get(label_t label, vid_t v) const`


---

## MSEdgeColumn

**Full name:** `neug::runtime::MSEdgeColumn`

**Public Methods:**

- `MSEdgeColumn()`
- `~MSEdgeColumn()=default`
- `get_edge(size_t idx) const override`
- `size() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `edge_column_type() const override`
- ... and 7 more methods


---

## MSEdgeColumnBuilder

**Full name:** `neug::runtime::MSEdgeColumnBuilder`

**Public Methods:**

- `MSEdgeColumnBuilder()`
- `~MSEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `start_label_dir(const LabelTriplet &label, Direction dir)`
- `push_back_opt(vid_t src, vid_t dst, const void *prop)`
- `push_back_null() override`
- `finish() override`


---

## MSVertexColumn

**Full name:** `neug::runtime::MSVertexColumn`

**Public Methods:**

- `MSVertexColumn()=default`
- `~MSVertexColumn()=default`
- `__attribute__((always_inline)) size_t size() const override`
- `column_info() const override`
- `__attribute__((always_inline)) VertexColumnType vertex_column_t...`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `__attribute__((always_inline)) VertexRecord get_vertex(size_t i...`
- `__attribute__((always_inline)) bool is_optional() const override`
- `__attribute__((always_inline)) bool has_value(size_t idx) const override`
- ... and 5 more methods


---

## MSVertexColumnBuilder

**Full name:** `neug::runtime::MSVertexColumnBuilder`

**Public Methods:**

- `MSVertexColumnBuilder(label_t label)`
- `~MSVertexColumnBuilder()=default`
- `reserve(size_t size) override`
- `__attribute__((always_inline)) void push_back_vertex(VertexReco...`
- `__attribute__((always_inline)) void start_label(label_t label)`
- `__attribute__((always_inline)) void push_back_opt(vid_t v)`
- `push_back_null() override`
- `finish() override`
- `__attribute__((always_inline)) size_t cur_size() const`


---

## ModOp

**Full name:** `neug::runtime::ModOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## MulOp

**Full name:** `neug::runtime::MulOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## NECmp

**Full name:** `neug::runtime::NECmp`

**Public Methods:**

- `NECmp()=default`
- `NECmp(const T &target)`
- `operator()(const T &v) const`
- `reset(const std::vector< T > &targets)`


---

## NeqOp

**Full name:** `neug::runtime::NeqOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## NodesExpr

**Full name:** `neug::runtime::NodesExpr`

**Public Methods:**

- `NodesExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const override`
- `type() const override`


---

## NumericCast

**Full name:** `neug::runtime::NumericCast`

**Public Methods:**

- `Operation(SRC input)`


---

## NumericLimits

**Full name:** `neug::runtime::NumericLimits`

Note: this should not be included directly, when these methods are required They should be used through the TryCast:: methods defined in 'cast_operators.hpp' This file produces 'unused `static` method...

**Public Methods:**

- `Minimum()`
- `Maximum()`
- `IsSigned()`
- `IsIntegral()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- `digits()`
- ... and 5 more methods


---

## NumericTryCast

**Full name:** `neug::runtime::NumericTryCast`

**Public Methods:**

- `Operation(SRC input, DST &result, bool strict=false)`


---

## OprTimer

**Full name:** `neug::runtime::OprTimer`

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
- `next()`
- ... and 3 more methods


---

## OptionalPathColumn

**Full name:** `neug::runtime::OptionalPathColumn`

**Public Methods:**

- `OptionalPathColumn()`
- `~OptionalPathColumn()`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `is_optional() const override`
- `elem_type() const override`
- `has_value(size_t idx) const override`
- `get_elem(size_t idx) const override`
- ... and 4 more methods


---

## OptionalValueColumn

**Full name:** `neug::runtime::OptionalValueColumn`

**Public Methods:**

- `OptionalValueColumn()`
- `~OptionalValueColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_value(size_t idx) const override`
- `generate_signature() const override`
- ... and 3 more methods


---

## OrOpExpr

**Full name:** `neug::runtime::OrOpExpr`

**Public Methods:**

- `OrOpExpr(std::unique_ptr< ExprBase > &&lhs, std::unique_pt...`
- `is_optional() const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`


---

## OrderBy

**Full name:** `neug::runtime::OrderBy`

**Public Methods:**

- `order_by_limit_impl(const StorageReadInterface &graph, const Context ...`
- `order_by_with_limit(const StorageReadInterface &graph, Context &&ctx,...`
- `staged_order_by_with_limit(const StorageReadInterface &graph, Context &&ctx,...`
- `order_by_with_limit_with_indices(const StorageReadInterface &graph, Context &&ctx,...`


---

## ParamAccessor

**Full name:** `neug::runtime::ParamAccessor`

**Public Methods:**

- `ParamAccessor(const ParamsMap &params, const std::string &key)`
- `typed_eval_path(size_t) const`
- `typed_eval_vertex(label_t, vid_t) const`
- `typed_eval_edge(const LabelTriplet &, vid_t, vid_t, const Property &) const`
- `eval_path(size_t) const override`
- `eval_vertex(label_t, vid_t) const override`
- `eval_edge(const LabelTriplet &, vid_t, vid_t, const void *)...`


---

## Path

**Full name:** `neug::runtime::Path`

**Public Methods:**

- `Path()=default`
- `Path(std::shared_ptr< PathImpl > impl)`
- `Path(label_t v_label, vid_t vid)`
- `Path(label_t label, label_t e_label, const std::vector...`
- `Path(const std::vector< std::tuple< label_t, Direction...`
- `expand(label_t edge_label, label_t label, vid_t v, Direc...`
- `length() const`
- `nodes() const`
- `relationships() const`
- `operator<(const Path &p) const`
- ... and 4 more methods


---

## PathColumn

**Full name:** `neug::runtime::PathColumn`

**Public Methods:**

- `PathColumn()`
- `~PathColumn()`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `get_path(size_t idx) const override`
- ... and 3 more methods


---

## PathColumnBuilder

**Full name:** `neug::runtime::PathColumnBuilder`

**Public Methods:**

- `PathColumnBuilder(bool is_optional=false)`
- `~PathColumnBuilder()=default`
- `push_back_opt(const Path &p)`
- `push_back_elem(const Value &val) override`
- `push_back_null() override`
- `reserve(size_t size) override`
- `finish() override`


---

## PathEdgePropsExpr

**Full name:** `neug::runtime::PathEdgePropsExpr`

**Public Methods:**

- `PathEdgePropsExpr(const StorageReadInterface &graph, const Context ...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## PathExpand

**Full name:** `neug::runtime::PathExpand`

**Public Methods:**

- `edge_expand_v(const StorageReadInterface &graph, Context &&ctx,...`
- `edge_expand_p(const StorageReadInterface &graph, Context &&ctx,...`
- `all_shortest_paths_with_given_source_and_dest(const StorageReadInterface &graph, Context &&ctx,...`
- `single_source_single_dest_shortest_path(const StorageReadInterface &graph, Context &&ctx,...`
- `single_source_shortest_path_with_order_by_length_limit(const StorageReadInterface &graph, Context &&ctx,...`
- `single_source_shortest_path(const StorageReadInterface &graph, Context &&ctx,...`
- `single_source_shortest_path_with_special_vertex_predicate(const StorageReadInterface &graph, Context &&ctx,...`
- `edge_expand_p_with_pred(const StorageReadInterface &graph, Context &&ctx,...`
- `any_weighted_shortest_path(const StorageReadInterface &graph, Context &&ctx,...`


---

## PathExpandParams

**Full name:** `neug::runtime::PathExpandParams`


---

## PathIdPathAccessor

**Full name:** `neug::runtime::PathIdPathAccessor`

**Public Methods:**

- `PathIdPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## PathLenPathAccessor

**Full name:** `neug::runtime::PathLenPathAccessor`

**Public Methods:**

- `PathLenPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## PathValue

**Full name:** `neug::runtime::PathValue`

**Public Methods:**

- `Get(const Value &value)`


---

## PathVertexPropsExpr

**Full name:** `neug::runtime::PathVertexPropsExpr`

**Public Methods:**

- `PathVertexPropsExpr(const StorageReadInterface &graph, const Context ...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## PathWeightPathAccessor

**Full name:** `neug::runtime::PathWeightPathAccessor`

**Public Methods:**

- `PathWeightPathAccessor(const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## Pipeline

**Full name:** `neug::runtime::Pipeline`

**Public Methods:**

- `Pipeline()`
- `Pipeline(Pipeline &&rhs)`
- `Pipeline(std::vector< std::unique_ptr< IOperator > > &&operators)`
- `~Pipeline()=default`
- `Execute(IStorageInterface &graph, Context &&ctx, const Pa...`


---

## PlanParser

**Full name:** `neug::runtime::PlanParser`

**Public Methods:**

- `PlanParser(const PlanParser &)=delete`
- `PlanParser(PlanParser &&)=delete`
- `operator=(const PlanParser &)=delete`
- `operator=(PlanParser &&)=delete`
- `~PlanParser()=default`
- `init()`
- `register_operator_builder(std::unique_ptr< IOperatorBuilder > &&builder)`
- `parse_execute_pipeline_with_meta(const neug::Schema &schema, const ContextMeta &ct...`
- `parse_execute_pipeline(const neug::Schema &schema, const ContextMeta &ct...`
- `get()`
- ... and 1 more methods


---

## PredWrapper

**Full name:** `neug::runtime::PredWrapper`

**Public Methods:**

- `PredWrapper(Expr &&pred)`
- `operator()(size_t idx) const`
- `operator()(label_t label, vid_t v) const`
- `operator()(const LabelTriplet &label, vid_t src, vid_t dst, ...`


---

## Project

**Full name:** `neug::runtime::Project`

**Public Methods:**

- `project(Context &&ctx, const std::vector< std::unique_ptr...`
- `project_order_by_fuse(const StorageReadInterface &graph, const ParamsMa...`


---

## ProjectExpr

**Full name:** `neug::runtime::ProjectExpr`

**Public Methods:**

- `ProjectExpr(EXPR &&expr, const COLLECTOR_T &collector, int alias)`
- `evaluate(const Context &ctx, Context &&ret) override`
- `order_by_limit(const Context &ctx, bool asc, size_t limit, std::...`
- `alias() const override`


---

## ProjectExprBase

**Full name:** `neug::runtime::ProjectExprBase`

**Public Methods:**

- `~ProjectExprBase()=default`
- `evaluate(const Context &ctx, Context &&ret)=0`
- `alias() const =0`
- `order_by_limit(const Context &ctx, bool asc, size_t limit, std::...`


---

## Reducer

**Full name:** `neug::runtime::Reducer`

**Public Methods:**

- `Reducer(REDUCER_T &&reducer, COLLECTOR_T &&collector, int alias)`
- `reduce(const Context &ctx, Context &&ret, const std::vec...`


---

## ReducerBase

**Full name:** `neug::runtime::ReducerBase`

**Public Methods:**

- `~ReducerBase()=default`
- `reduce(const Context &ctx, Context &&ret, const std::vec...`


---

## RelationshipsExpr

**Full name:** `neug::runtime::RelationshipsExpr`

**Public Methods:**

- `RelationshipsExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `is_optional() const override`
- `type() const override`


---

## SDMLEdgeColumn

**Full name:** `neug::runtime::SDMLEdgeColumn`

**Public Methods:**

- `SDMLEdgeColumn(Direction dir)`
- `~SDMLEdgeColumn()=default`
- `get_edge(size_t idx) const override`
- `size() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `edge_column_type() const override`
- ... and 3 more methods


---

## SDMLEdgeColumnBuilder

**Full name:** `neug::runtime::SDMLEdgeColumnBuilder`

**Public Methods:**

- `SDMLEdgeColumnBuilder(Direction dir, const std::vector< LabelTriplet > &labels)`
- `~SDMLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_opt(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `push_back_opt(int label_idx, vid_t src, vid_t dst, const void *prop)`
- `push_back_null() override`
- `finish() override`


---

## SDSLEdgeColumn

**Full name:** `neug::runtime::SDSLEdgeColumn`

**Public Methods:**

- `SDSLEdgeColumn(Direction dir, const LabelTriplet &label)`
- `get_edge(size_t idx) const override`
- `size() const override`
- `dir() const override`
- `generate_dedup_offset(std::vector< size_t > &offsets) const override`
- `generate_signature() const override`
- `column_info() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `foreach_edge_opt(const FUNC_T &func) const`
- ... and 3 more methods


---

## SDSLEdgeColumnBuilder

**Full name:** `neug::runtime::SDSLEdgeColumnBuilder`

**Public Methods:**

- `SDSLEdgeColumnBuilder(Direction dir, const LabelTriplet &label)`
- `~SDSLEdgeColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_opt(vid_t src, vid_t dst, const void *prop)`
- `push_back_null() override`
- `finish() override`


---

## SLEdgePropertyGetter

**Full name:** `neug::runtime::SLEdgePropertyGetter`

**Public Methods:**

- `SLEdgePropertyGetter(const StorageReadInterface &graph, const std::vec...`
- `~SLEdgePropertyGetter()=default`
- `get(label_t v_label, vid_t v, label_t nbr_label, vid_...`


---

## SLEdgePropertyPathAccessor

**Full name:** `neug::runtime::SLEdgePropertyPathAccessor`

**Public Methods:**

- `SLEdgePropertyPathAccessor(const StorageReadInterface &graph, const std::str...`
- `eval_path(size_t idx) const override`
- `typed_eval_path(size_t idx) const`
- `is_optional() const override`


---

## SLVertexColumn

**Full name:** `neug::runtime::SLVertexColumn`

**Public Methods:**

- `SLVertexColumn(label_t label)`
- `~SLVertexColumn()=default`
- `__attribute__((always_inline)) inline size_t size() const override`
- `column_info() const override`
- `__attribute__((always_inline)) VertexColumnType vertex_column_t...`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offset) const override`
- `__attribute__((always_inline)) VertexRecord get_vertex(size_t i...`
- `__attribute__((always_inline)) bool is_optional() const override`
- `__attribute__((always_inline)) bool has_value(size_t idx) const override`
- ... and 7 more methods


---

## SLVertexPropertyGetter

**Full name:** `neug::runtime::SLVertexPropertyGetter`

**Public Methods:**

- `SLVertexPropertyGetter(const IStorageInterface &graph, label_t label, co...`
- `~SLVertexPropertyGetter()=default`
- `get(label_t label, vid_t v) const`


---

## ScalarFunctionExpr

**Full name:** `neug::runtime::ScalarFunctionExpr`

**Public Methods:**

- `ScalarFunctionExpr(neug_func_exec_t fn, const DataType &ret_type, st...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Scan

**Full name:** `neug::runtime::Scan`

**Public Methods:**

- `scan_vertex(Context &&ctx, const IStorageInterface &gi, const...`
- `scan_vertex_with_limit(Context &&ctx, const IStorageInterface &gi, const...`
- `scan_vertex_with_special_vertex_predicate(Context &&ctx, const IStorageInterface &graph, co...`
- `filter_oids(Context &&ctx, const IStorageInterface &graph, co...`
- `find_vertex_with_oid(Context &&ctx, const IStorageInterface &graph, la...`


---

## ScanParams

**Full name:** `neug::runtime::ScanParams`

**Public Methods:**

- `ScanParams()`


---

## Select

**Full name:** `neug::runtime::Select`

**Public Methods:**

- `select(Context &&ctx, const PRED_T &pred)`


---

## ShortestPathParams

**Full name:** `neug::runtime::ShortestPathParams`


---

## SigColumn

**Full name:** `neug::runtime::SigColumn`

**Public Methods:**

- `SigColumn(const std::vector< T > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< Date >

**Full name:** `neug::runtime::SigColumn< Date >`

**Public Methods:**

- `SigColumn(const std::vector< Date > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< DateTime >

**Full name:** `neug::runtime::SigColumn< DateTime >`

**Public Methods:**

- `SigColumn(const std::vector< DateTime > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< Interval >

**Full name:** `neug::runtime::SigColumn< Interval >`

**Public Methods:**

- `SigColumn(const std::vector< Interval > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## string >

**Full name:** `neug::runtime::SigColumn< std::string >`

**Public Methods:**

- `SigColumn(const std::vector< std::string > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## SigColumn< vertex_t >

**Full name:** `neug::runtime::SigColumn< vertex_t >`

**Public Methods:**

- `SigColumn(const std::vector< vertex_t > &data)`
- `~SigColumn()=default`
- `get_sig(size_t idx) const override`


---

## Sink

**Full name:** `neug::runtime::Sink`

**Public Methods:**

- `sink(const Context &ctx, const StorageReadInterface &graph)`
- `sink(const Context &ctx, const StorageReadInterface &g...`
- `sink_encoder(const Context &ctx, const StorageReadInterface &g...`
- `sink_beta(const Context &ctx, const StorageReadInterface &g...`


---

## SpecialEdgePredicateConfig

**Full name:** `neug::runtime::SpecialEdgePredicateConfig`


---

## SpecialVertexPredicateConfig

**Full name:** `neug::runtime::SpecialVertexPredicateConfig`


---

## StartNodeExpr

**Full name:** `neug::runtime::StartNodeExpr`

**Public Methods:**

- `StartNodeExpr(std::unique_ptr< ExprBase > &&args)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## StringValue

**Full name:** `neug::runtime::StringValue`

**Public Methods:**

- `Get(const Value &value)`


---

## StructColumn

**Full name:** `neug::runtime::StructColumn`

**Public Methods:**

- `StructColumn()=default`
- `~StructColumn()=default`
- `size() const override`
- `column_info() const override`
- `column_type() const override`
- `shuffle(const std::vector< size_t > &offsets) const override`
- `optional_shuffle(const std::vector< size_t > &offsets) const override`
- `elem_type() const override`
- `get_elem(size_t idx) const override`
- `generate_signature() const override`
- ... and 3 more methods


---

## StructColumnBuilder

**Full name:** `neug::runtime::StructColumnBuilder`

**Public Methods:**

- `StructColumnBuilder(DataType type)`
- `~StructColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_null() override`
- `finish() override`


---

## StructValue

**Full name:** `neug::runtime::StructValue`

**Public Methods:**

- `GetChildren(const Value &value)`


---

## SubOp

**Full name:** `neug::runtime::SubOp`

**Public Methods:**

- `operation(const T &left, const T &right)`


---

## TimerUnit

**Full name:** `neug::runtime::TimerUnit`

**Public Methods:**

- `TimerUnit()`
- `~TimerUnit()=default`
- `start()`
- `elapsed() const`


---

## TupleExpr

**Full name:** `neug::runtime::TupleExpr`

**Public Methods:**

- `TupleExpr(std::vector< std::unique_ptr< ExprBase > > &&exprs)`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`


---

## UnaryLogicalExpr

**Full name:** `neug::runtime::UnaryLogicalExpr`

**Public Methods:**

- `UnaryLogicalExpr(std::unique_ptr< ExprBase > &&expr, ::common::Log...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## Unfold

**Full name:** `neug::runtime::Unfold`

**Public Methods:**

- `unfold(Context &&ctxs, int key, int alias)`
- `unfold(Context &&ctxs, const Expr &key, int alias)`


---

## Union

**Full name:** `neug::runtime::Union`

**Public Methods:**

- `union_op(std::vector< Context > &&ctxs)`


---

## Value

**Full name:** `neug::runtime::Value`

**Public Methods:**

- `Value(DataType type)`
- `Value(const Value &other)`
- `Value(Value &&other) noexcept`
- `operator=(const Value &other)`
- `operator=(Value &&other) noexcept`
- `type() const`
- `operator==(const Value &other) const`
- `operator<(const Value &other) const`
- `IsNull() const`
- `IsTrue() const`
- ... and 57 more methods


---

## ValueColumn

**Full name:** `neug::runtime::ValueColumn`

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
- ... and 5 more methods


---

## ValueColumnBuilder

**Full name:** `neug::runtime::ValueColumnBuilder`

**Public Methods:**

- `ValueColumnBuilder(bool is_optional=false)`
- `~ValueColumnBuilder()=default`
- `reserve(size_t size) override`
- `push_back_elem(const Value &val) override`
- `push_back_opt(const T &val)`
- `push_back_null() override`
- `finish() override`


---

## ValueConverter

**Full name:** `neug::runtime::ValueConverter`

**Public Methods:**

- `type()`


---

## ValueConverter< bool >

**Full name:** `neug::runtime::ValueConverter< bool >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`


---

## ValueConverter< date_t >

**Full name:** `neug::runtime::ValueConverter< date_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, Date &output)`


---

## ValueConverter< double >

**Full name:** `neug::runtime::ValueConverter< double >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, double &output)`


---

## ValueConverter< edge_t >

**Full name:** `neug::runtime::ValueConverter< edge_t >`

**Public Methods:**

- `type()`


---

## ValueConverter< float >

**Full name:** `neug::runtime::ValueConverter< float >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, float &output)`


---

## ValueConverter< int32_t >

**Full name:** `neug::runtime::ValueConverter< int32_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, int32_t &output)`


---

## ValueConverter< int64_t >

**Full name:** `neug::runtime::ValueConverter< int64_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, int64_t &output)`


---

## ValueConverter< interval_t >

**Full name:** `neug::runtime::ValueConverter< interval_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, Interval &output)`


---

## string >

**Full name:** `neug::runtime::ValueConverter< std::string >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`


---

## ValueConverter< timestamp_ms_t >

**Full name:** `neug::runtime::ValueConverter< timestamp_ms_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, DateTime &output)`


---

## ValueConverter< uint32_t >

**Full name:** `neug::runtime::ValueConverter< uint32_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, uint32_t &output)`


---

## ValueConverter< uint64_t >

**Full name:** `neug::runtime::ValueConverter< uint64_t >`

**Public Methods:**

- `type()`
- `name()`
- `typed_from_string(const std::string &str)`
- `cast(const T &input, uint64_t &output)`


---

## ValueConverter< vertex_t >

**Full name:** `neug::runtime::ValueConverter< vertex_t >`

**Public Methods:**

- `type()`


---

## Var

**Full name:** `neug::runtime::Var`

**Public Methods:**

- `graph_related_var(const common::Variable &pb)`
- `Var(const StorageReadInterface *graph, const Context ...`
- `~Var()`
- `get(size_t path_idx) const`
- `get_vertex(label_t label, vid_t v) const`
- `get_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const`
- `is_optional() const`


---

## VarGetterBase

**Full name:** `neug::runtime::VarGetterBase`

**Public Methods:**

- `~VarGetterBase()=default`
- `eval_path(size_t idx) const =0`
- `eval_vertex(label_t label, vid_t v, size_t idx) const =0`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `name() const =0`


---

## VariableExpr

**Full name:** `neug::runtime::VariableExpr`

**Public Methods:**

- `VariableExpr(const StorageReadInterface *graph, const Context ...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## VertexGIdPathAccessor

**Full name:** `neug::runtime::VertexGIdPathAccessor`

**Public Methods:**

- `VertexGIdPathAccessor(const Context &ctx, int tag)`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexGIdVertexAccessor

**Full name:** `neug::runtime::VertexGIdVertexAccessor`

**Public Methods:**

- `VertexGIdVertexAccessor()`
- `typed_eval_vertex(label_t label, vid_t v) const`
- `eval_vertex(label_t label, vid_t v) const override`


---

## VertexIdVertexAccessor

**Full name:** `neug::runtime::VertexIdVertexAccessor`

**Public Methods:**

- `VertexIdVertexAccessor()`
- `typed_eval_vertex(label_t label, vid_t v) const`
- `eval_vertex(label_t label, vid_t v) const override`


---

## VertexLabelPathAccessor

**Full name:** `neug::runtime::VertexLabelPathAccessor`

**Public Methods:**

- `VertexLabelPathAccessor(const Schema &schema, const Context &ctx, int tag)`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexLabelVertexAccessor

**Full name:** `neug::runtime::VertexLabelVertexAccessor`

**Public Methods:**

- `VertexLabelVertexAccessor(const Schema &schema)`
- `typed_eval_vertex(label_t label, vid_t v, size_t idx) const`
- `eval_vertex(label_t label, vid_t v) const override`


---

## VertexPathAccessor

**Full name:** `neug::runtime::VertexPathAccessor`

**Public Methods:**

- `VertexPathAccessor(const Context &ctx, int tag)`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexPropertyCmpPredicate

**Full name:** `neug::runtime::VertexPropertyCmpPredicate`

**Public Methods:**

- `VertexPropertyCmpPredicate(const GETTER_T &getter, const CMP_T &cmp)`
- `operator()(label_t label, vid_t v) const`


---

## VertexPropertyPathAccessor

**Full name:** `neug::runtime::VertexPropertyPathAccessor`

**Public Methods:**

- `VertexPropertyPathAccessor(const StorageReadInterface &graph, const Context ...`
- `is_optional() const override`
- `typed_eval_path(size_t idx) const`
- `eval_path(size_t idx) const override`


---

## VertexPropertyVertexAccessor

**Full name:** `neug::runtime::VertexPropertyVertexAccessor`

**Public Methods:**

- `VertexPropertyVertexAccessor(const StorageReadInterface &graph, const std::str...`
- `typed_eval_vertex(label_t label, vid_t v) const`
- `eval_vertex(label_t label, vid_t v) const override`
- `is_optional() const override`


---

## VertexRecord

**Full name:** `neug::runtime::VertexRecord`

**Public Methods:**

- `VertexRecord()=default`
- `VertexRecord(label_t label, vid_t vid)`
- `operator<(const VertexRecord &v) const`
- `operator==(const VertexRecord &v) const`
- `to_string() const`
- `label() const`
- `vid() const`


---

## WithInExpr

**Full name:** `neug::runtime::WithInExpr`

**Public Methods:**

- `WithInExpr(const Context &ctx, std::unique_ptr< ExprBase > &...`
- `eval_impl(const Value &val) const`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## WithInListExpr

**Full name:** `neug::runtime::WithInListExpr`

**Public Methods:**

- `WithInListExpr(const Context &ctx, std::unique_ptr< ExprBase > &...`
- `eval_path(size_t idx) const override`
- `eval_vertex(label_t label, vid_t v) const override`
- `eval_edge(const LabelTriplet &label, vid_t src, vid_t dst, ...`
- `type() const override`
- `is_optional() const override`


---

## list_item

**Full name:** `neug::runtime::list_item`


---

## AddEdgePropertySchemaOprBuilder

**Full name:** `neug::runtime::ops::AddEdgePropertySchemaOprBuilder`

**Public Methods:**

- `AddEdgePropertySchemaOprBuilder()=default`
- `~AddEdgePropertySchemaOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## AddVertexPropertySchemaOprBuilder

**Full name:** `neug::runtime::ops::AddVertexPropertySchemaOprBuilder`

**Public Methods:**

- `AddVertexPropertySchemaOprBuilder()=default`
- `~AddVertexPropertySchemaOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## AvgReducer

**Full name:** `neug::runtime::ops::AvgReducer`


---

## value > >

**Full name:** `neug::runtime::ops::AvgReducer< EXPR, IS_OPTIONAL, std::enable_if_t< std::is_arithmetic< typename EXPR::V >::value > >`

**Public Methods:**

- `AvgReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, double &avg) const`


---

## BatchDeleteEdgeOprBuilder

**Full name:** `neug::runtime::ops::BatchDeleteEdgeOprBuilder`

**Public Methods:**

- `BatchDeleteEdgeOprBuilder()=default`
- `~BatchDeleteEdgeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## BatchDeleteVertexOprBuilder

**Full name:** `neug::runtime::ops::BatchDeleteVertexOprBuilder`

**Public Methods:**

- `BatchDeleteVertexOprBuilder()=default`
- `~BatchDeleteVertexOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## BatchInsertEdgeOprBuilder

**Full name:** `neug::runtime::ops::BatchInsertEdgeOprBuilder`

**Public Methods:**

- `BatchInsertEdgeOprBuilder()=default`
- `~BatchInsertEdgeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## BatchInsertVertexOprBuilder

**Full name:** `neug::runtime::ops::BatchInsertVertexOprBuilder`

**Public Methods:**

- `BatchInsertVertexOprBuilder()=default`
- `~BatchInsertVertexOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## CaseWhenCollector

**Full name:** `neug::runtime::ops::CaseWhenCollector`

**Public Methods:**

- `CaseWhenCollector(const Context &ctx)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## CaseWhenExprBuilder

**Full name:** `neug::runtime::ops::CaseWhenExprBuilder`

**Public Methods:**

- `CaseWhenExprBuilder(const std::vector< std::string > &param_names, co...`
- `build(const IStorageInterface &igraph, const Context &c...`


---

## CheckpointOprBuilder

**Full name:** `neug::runtime::ops::CheckpointOprBuilder`

**Public Methods:**

- `CheckpointOprBuilder()=default`
- `~CheckpointOprBuilder() override=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `stepping(int i)`
- `GetOpKinds() const override`


---

## ColumnWrapper

**Full name:** `neug::runtime::ops::ColumnWrapper`

**Public Methods:**

- `ColumnWrapper(const IContextColumn &column)`
- `operator()(size_t idx) const`


---

## CountDistinctReducer

**Full name:** `neug::runtime::ops::CountDistinctReducer`

**Public Methods:**

- `CountDistinctReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## CountReducer

**Full name:** `neug::runtime::ops::CountReducer`

**Public Methods:**

- `CountReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## CountStartReducer

**Full name:** `neug::runtime::ops::CountStartReducer`

**Public Methods:**

- `CountStartReducer()`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## CreateEdge

**Full name:** `neug::runtime::ops::CreateEdge`

**Public Methods:**

- `insert_edge(StorageInsertInterface &graph, Context &&ctx, std...`


---

## CreateEdgeOprBuilder

**Full name:** `neug::runtime::ops::CreateEdgeOprBuilder`

**Public Methods:**

- `CreateEdgeOprBuilder()=default`
- `~CreateEdgeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## CreateEdgeTypeOprBuilder

**Full name:** `neug::runtime::ops::CreateEdgeTypeOprBuilder`

**Public Methods:**

- `CreateEdgeTypeOprBuilder()=default`
- `~CreateEdgeTypeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## CreateVertex

**Full name:** `neug::runtime::ops::CreateVertex`

**Public Methods:**

- `insert_vertex(StorageInsertInterface &graph, Context &&ctx, con...`


---

## CreateVertexOprBuilder

**Full name:** `neug::runtime::ops::CreateVertexOprBuilder`

**Public Methods:**

- `CreateVertexOprBuilder()=default`
- `~CreateVertexOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## CreateVertexTypeOprBuilder

**Full name:** `neug::runtime::ops::CreateVertexTypeOprBuilder`

**Public Methods:**

- `CreateVertexTypeOprBuilder()=default`
- `~CreateVertexTypeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DataExportOprBuilder

**Full name:** `neug::runtime::ops::DataExportOprBuilder`

**Public Methods:**

- `DataExportOprBuilder()=default`
- `~DataExportOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## DataSourceOprBuilder

**Full name:** `neug::runtime::ops::DataSourceOprBuilder`

**Public Methods:**

- `DataSourceOprBuilder()=default`
- `~DataSourceOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DedupOprBuilder

**Full name:** `neug::runtime::ops::DedupOprBuilder`

**Public Methods:**

- `DedupOprBuilder()=default`
- `~DedupOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## DropEdgePropertySchemaOprBuilder

**Full name:** `neug::runtime::ops::DropEdgePropertySchemaOprBuilder`

**Public Methods:**

- `DropEdgePropertySchemaOprBuilder()=default`
- `~DropEdgePropertySchemaOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DropEdgeTypeOprBuilder

**Full name:** `neug::runtime::ops::DropEdgeTypeOprBuilder`

**Public Methods:**

- `DropEdgeTypeOprBuilder()=default`
- `~DropEdgeTypeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DropVertexPropertySchemaOprBuilder

**Full name:** `neug::runtime::ops::DropVertexPropertySchemaOprBuilder`

**Public Methods:**

- `DropVertexPropertySchemaOprBuilder()=default`
- `~DropVertexPropertySchemaOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DropVertexTypeOprBuilder

**Full name:** `neug::runtime::ops::DropVertexTypeOprBuilder`

**Public Methods:**

- `DropVertexTypeOprBuilder()=default`
- `~DropVertexTypeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## DummyGetterBuilder

**Full name:** `neug::runtime::ops::DummyGetterBuilder`

**Public Methods:**

- `DummyGetterBuilder(int from, int to)`
- `build(const IStorageInterface &graph, const Context &ct...`


---

## DummySourceOprBuilder

**Full name:** `neug::runtime::ops::DummySourceOprBuilder`

**Public Methods:**

- `DummySourceOprBuilder()=default`
- `~DummySourceOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`
- `stepping(int i) override`


---

## EdgeCollector

**Full name:** `neug::runtime::ops::EdgeCollector`

**Public Methods:**

- `EdgeCollector()`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## EdgeExprWrapper

**Full name:** `neug::runtime::ops::EdgeCollector::EdgeExprWrapper`

**Public Methods:**

- `EdgeExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## EdgeExpandGetVOprBuilder

**Full name:** `neug::runtime::ops::EdgeExpandGetVOprBuilder`

**Public Methods:**

- `EdgeExpandGetVOprBuilder()=default`
- `~EdgeExpandGetVOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## EdgeExpandOprBuilder

**Full name:** `neug::runtime::ops::EdgeExpandOprBuilder`

**Public Methods:**

- `EdgeExpandOprBuilder()=default`
- `~EdgeExpandOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ExtensionInstallOprBuilder

**Full name:** `neug::runtime::ops::ExtensionInstallOprBuilder`

**Public Methods:**

- `ExtensionInstallOprBuilder()=default`
- `~ExtensionInstallOprBuilder() override=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## ExtensionLoadOprBuilder

**Full name:** `neug::runtime::ops::ExtensionLoadOprBuilder`

**Public Methods:**

- `ExtensionLoadOprBuilder()=default`
- `~ExtensionLoadOprBuilder() override=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## ExtensionUninstallOprBuilder

**Full name:** `neug::runtime::ops::ExtensionUninstallOprBuilder`

**Public Methods:**

- `ExtensionUninstallOprBuilder()=default`
- `~ExtensionUninstallOprBuilder() override=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## FirstReducer

**Full name:** `neug::runtime::ops::FirstReducer`

**Public Methods:**

- `FirstReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## GeneralComparer

**Full name:** `neug::runtime::ops::GeneralComparer`

**Public Methods:**

- `GeneralComparer()`
- `~GeneralComparer()`
- `add_keys(Var &&key, bool asc)`
- `operator()(size_t lhs, size_t rhs) const`


---

## GeneralListCollector

**Full name:** `neug::runtime::ops::GeneralListCollector`

**Public Methods:**

- `GeneralListCollector(const DataType &type)`
- `init(size_t size)`
- `collect(std::vector< Value > &&val)`
- `get()`


---

## GeneralProjectExprBuilder

**Full name:** `neug::runtime::ops::GeneralProjectExprBuilder`

**Public Methods:**

- `GeneralProjectExprBuilder(const common::Expression &expr, const std::option...`
- `build(const IStorageInterface &graph, const Context &ct...`
- `is_general() const override`


---

## GroupByOprBuilder

**Full name:** `neug::runtime::ops::GroupByOprBuilder`

**Public Methods:**

- `GroupByOprBuilder()=default`
- `~GroupByOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## IntersectOprBuilder

**Full name:** `neug::runtime::ops::IntersectOprBuilder`

**Public Methods:**

- `IntersectOprBuilder()=default`
- `~IntersectOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## IsCountReducer

**Full name:** `neug::runtime::ops::IsCountReducer`


---

## IsCountReducer< CountReducer< EXPR, is_optional > >

**Full name:** `neug::runtime::ops::IsCountReducer< CountReducer< EXPR, is_optional > >`


---

## IsCountReducer< CountStartReducer >

**Full name:** `neug::runtime::ops::IsCountReducer< CountStartReducer >`


---

## JoinOprBuilder

**Full name:** `neug::runtime::ops::JoinOprBuilder`

**Public Methods:**

- `JoinOprBuilder()=default`
- `~JoinOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## KeyBuilder

**Full name:** `neug::runtime::ops::KeyBuilder`

**Public Methods:**

- `make_sp_key(const Context &ctx, const std::vector< std::pair<...`


---

## KeyExpr

**Full name:** `neug::runtime::ops::KeyExpr`

**Public Methods:**

- `KeyExpr(std::tuple< EXPR... > &&exprs)`
- `operator()(size_t idx) const`


---

## LimitOprBuilder

**Full name:** `neug::runtime::ops::LimitOprBuilder`

**Public Methods:**

- `LimitOprBuilder()=default`
- `~LimitOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ListCollector

**Full name:** `neug::runtime::ops::ListCollector`

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

**Full name:** `neug::runtime::ops::ListCollector::ListExprWrapper`

**Public Methods:**

- `ListExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## MLPropertyExpr

**Full name:** `neug::runtime::ops::MLPropertyExpr`

**Public Methods:**

- `MLPropertyExpr(const IStorageInterface &igraph, const VertexColu...`
- `is_optional() const`
- `operator()(size_t idx) const`


---

## MLVertexCollector

**Full name:** `neug::runtime::ops::MLVertexCollector`

**Public Methods:**

- `MLVertexCollector()`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## MLVertexWrapper

**Full name:** `neug::runtime::ops::MLVertexWrapper`

**Public Methods:**

- `MLVertexWrapper(const VERTEX_COL &vertex)`
- `operator()(size_t idx) const`


---

## MaxReducer

**Full name:** `neug::runtime::ops::MaxReducer`

**Public Methods:**

- `MaxReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## MinReducer

**Full name:** `neug::runtime::ops::MinReducer`

**Public Methods:**

- `MinReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## OptionalTypedVarWrapper

**Full name:** `neug::runtime::ops::OptionalTypedVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `OptionalTypedVarWrapper(Var &&vars)`


---

## OptionalValueCollector

**Full name:** `neug::runtime::ops::OptionalValueCollector`

**Public Methods:**

- `OptionalValueCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## OptionalExprWrapper

**Full name:** `neug::runtime::ops::OptionalValueCollector::OptionalExprWrapper`

**Public Methods:**

- `OptionalExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## OptionalVarWrapper

**Full name:** `neug::runtime::ops::OptionalVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `OptionalVarWrapper(Var &&vars)`


---

## OrderByOprBuilder

**Full name:** `neug::runtime::ops::OrderByOprBuilder`

**Public Methods:**

- `OrderByOprBuilder()=default`
- `~OrderByOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## PathExpandOprBuilder

**Full name:** `neug::runtime::ops::PathExpandOprBuilder`

**Public Methods:**

- `PathExpandOprBuilder()=default`
- `~PathExpandOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## PathExpandVOprBuilder

**Full name:** `neug::runtime::ops::PathExpandVOprBuilder`

**Public Methods:**

- `PathExpandVOprBuilder()=default`
- `~PathExpandVOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## PrimaryKeyJoinOprBuilder

**Full name:** `neug::runtime::ops::PrimaryKeyJoinOprBuilder`

**Public Methods:**

- `PrimaryKeyJoinOprBuilder()=default`
- `~PrimaryKeyJoinOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## ProcedureCallOprBuilder

**Full name:** `neug::runtime::ops::ProcedureCallOprBuilder`

**Public Methods:**

- `ProcedureCallOprBuilder()=default`
- `~ProcedureCallOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ProjectExprBuilderBase

**Full name:** `neug::runtime::ops::ProjectExprBuilderBase`

**Public Methods:**

- `~ProjectExprBuilderBase()=default`
- `build(const IStorageInterface &graph, const Context &ct...`
- `is_general() const`


---

## ProjectOprBuilder

**Full name:** `neug::runtime::ops::ProjectOprBuilder`

**Public Methods:**

- `ProjectOprBuilder()=default`
- `~ProjectOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ProjectOrderByOprBuilder

**Full name:** `neug::runtime::ops::ProjectOrderByOprBuilder`

**Public Methods:**

- `ProjectOrderByOprBuilder()=default`
- `~ProjectOrderByOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## PropertyValueCollector

**Full name:** `neug::runtime::ops::PropertyValueCollector`

**Public Methods:**

- `PropertyValueCollector(const Context &ctx)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## ReadStateBuilder

**Full name:** `neug::runtime::ops::ReadStateBuilder`

**Public Methods:**

- `build(const ::physical::DataSource &data_source)`


---

## RenameEdgePropertyOprBuilder

**Full name:** `neug::runtime::ops::RenameEdgePropertyOprBuilder`

**Public Methods:**

- `RenameEdgePropertyOprBuilder()=default`
- `~RenameEdgePropertyOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## RenameVertexPropertyOprBuilder

**Full name:** `neug::runtime::ops::RenameVertexPropertyOprBuilder`

**Public Methods:**

- `RenameVertexPropertyOprBuilder()=default`
- `~RenameVertexPropertyOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## SLPropertyExpr

**Full name:** `neug::runtime::ops::SLPropertyExpr`

**Public Methods:**

- `SLPropertyExpr(const IStorageInterface &igraph, const VertexColu...`
- `operator()(size_t idx) const`
- `is_optional() const`


---

## SLVertexCollector

**Full name:** `neug::runtime::ops::SLVertexCollector`

**Public Methods:**

- `SLVertexCollector(label_t v_label)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## SLVertexWrapper

**Full name:** `neug::runtime::ops::SLVertexWrapper`

**Public Methods:**

- `SLVertexWrapper(const SLVertexColumn &column)`
- `operator()(size_t idx) const`


---

## SLVertexWrapperBeta

**Full name:** `neug::runtime::ops::SLVertexWrapperBeta`

**Public Methods:**

- `SLVertexWrapperBeta(const SLVertexColumn &column)`
- `operator()(size_t idx) const`


---

## SPOpr

**Full name:** `neug::runtime::ops::SPOpr`

**Public Methods:**

- `SPOpr(const VERTEX_COL_PTR &vertex_col, SP_PRED_T &&pre...`
- `operator()(size_t idx) const`


---

## SPOprBuilder

**Full name:** `neug::runtime::ops::SPOprBuilder`

**Public Methods:**

- `SPOprBuilder()=default`
- `~SPOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## SPOrderByLimitOprBuilder

**Full name:** `neug::runtime::ops::SPOrderByLimitOprBuilder`

**Public Methods:**

- `SPOrderByLimitOprBuilder()=default`
- `~SPOrderByLimitOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `stepping(int i) override`
- `GetOpKinds() const override`


---

## ScanOprBuilder

**Full name:** `neug::runtime::ops::ScanOprBuilder`

**Public Methods:**

- `ScanOprBuilder()=default`
- `~ScanOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ScanUtils

**Full name:** `neug::runtime::ops::ScanUtils`

**Public Methods:**

- `parse_ids_with_type(DataTypeId type, const algebra::IndexPredicate &triplet)`
- `check_idx_predicate(const physical::Scan &scan_opr)`


---

## SelectOprBuilder

**Full name:** `neug::runtime::ops::SelectOprBuilder`

**Public Methods:**

- `SelectOprBuilder()=default`
- `~SelectOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## SetCollector

**Full name:** `neug::runtime::ops::SetCollector`

**Public Methods:**

- `SetCollector()`
- `init(size_t size)`
- `collect(std::set< T > &&val)`
- `get()`


---

## SingleValueCollector

**Full name:** `neug::runtime::ops::SingleValueCollector`

**Public Methods:**

- `SingleValueCollector()=default`
- `init(size_t size)`
- `collect(T &&val)`
- `get()`


---

## SinkOprBuilder

**Full name:** `neug::runtime::ops::SinkOprBuilder`

**Public Methods:**

- `SinkOprBuilder()=default`
- `~SinkOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## StructCollector

**Full name:** `neug::runtime::ops::StructCollector`

**Public Methods:**

- `StructCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## StructExprWrapper

**Full name:** `neug::runtime::ops::StructCollector::StructExprWrapper`

**Public Methods:**

- `StructExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## SumReducer

**Full name:** `neug::runtime::ops::SumReducer`


---

## value > >

**Full name:** `neug::runtime::ops::SumReducer< EXPR, IS_OPTIONAL, std::enable_if_t< std::is_arithmetic< typename EXPR::V >::value > >`

**Public Methods:**

- `SumReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &sum) const`


---

## TCOprBuilder

**Full name:** `neug::runtime::ops::TCOprBuilder`

**Public Methods:**

- `TCOprBuilder()=default`
- `~TCOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## ToListReducer

**Full name:** `neug::runtime::ops::ToListReducer`

**Public Methods:**

- `ToListReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &list) const`


---

## ToSetReducer

**Full name:** `neug::runtime::ops::ToSetReducer`

**Public Methods:**

- `ToSetReducer(EXPR &&expr)`
- `operator()(const std::vector< size_t > &group, V &val) const`


---

## TypedVarWrapper

**Full name:** `neug::runtime::ops::TypedVarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `TypedVarWrapper(Var &&vars)`


---

## UnfoldOprBuilder

**Full name:** `neug::runtime::ops::UnfoldOprBuilder`

**Public Methods:**

- `UnfoldOprBuilder()=default`
- `~UnfoldOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## UnionOprBuilder

**Full name:** `neug::runtime::ops::UnionOprBuilder`

**Public Methods:**

- `UnionOprBuilder()=default`
- `~UnionOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## UpdateEdgeOprBuilder

**Full name:** `neug::runtime::ops::UpdateEdgeOprBuilder`

**Public Methods:**

- `UpdateEdgeOprBuilder()=default`
- `~UpdateEdgeOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## UpdateVertexOprBuilder

**Full name:** `neug::runtime::ops::UpdateVertexOprBuilder`

**Public Methods:**

- `UpdateVertexOprBuilder()=default`
- `~UpdateVertexOprBuilder()=default`
- `Build(const Schema &schema, const ContextMeta &ctx_meta...`
- `GetOpKinds() const override`


---

## ValueCollector

**Full name:** `neug::runtime::ops::ValueCollector`

**Public Methods:**

- `ValueCollector(const Context &ctx, const EXPR &expr)`
- `collect(const EXPR &expr, size_t idx)`
- `get()`


---

## ExprWrapper

**Full name:** `neug::runtime::ops::ValueCollector::ExprWrapper`

**Public Methods:**

- `ExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## ValueWrapper

**Full name:** `neug::runtime::ops::ValueWrapper`

**Public Methods:**

- `ValueWrapper(const ValueColumn< T > &column)`
- `operator()(size_t idx) const`


---

## VarPairWrapper

**Full name:** `neug::runtime::ops::VarPairWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `VarPairWrapper(Var &&fst, Var &&snd)`


---

## VarWrapper

**Full name:** `neug::runtime::ops::VarWrapper`

**Public Methods:**

- `operator()(size_t idx) const`
- `VarWrapper(Var &&vars)`


---

## VertexCollector

**Full name:** `neug::runtime::ops::VertexCollector`

**Public Methods:**

- `VertexCollector()`
- `init(size_t size)`
- `collect(VertexRecord &&val)`
- `get()`


---

## VertexExprWrapper

**Full name:** `neug::runtime::ops::VertexExprWrapper`

**Public Methods:**

- `VertexExprWrapper(Expr &&expr)`
- `operator()(size_t idx) const`


---

## VertexOprBuilder

**Full name:** `neug::runtime::ops::VertexOprBuilder`

**Public Methods:**

- `VertexOprBuilder()=default`
- `~VertexOprBuilder()=default`
- `Build(const neug::Schema &schema, const ContextMeta &ct...`
- `GetOpKinds() const override`


---

## VertexPropertyExprBuilder

**Full name:** `neug::runtime::ops::VertexPropertyExprBuilder`

**Public Methods:**

- `VertexPropertyExprBuilder(int tag, const std::string &property_name, int alias)`
- `build(const IStorageInterface &graph, const Context &ct...`


---

## is_hashable

**Full name:** `neug::runtime::ops::is_hashable`


---

## declval< T >()))> >

**Full name:** `neug::runtime::ops::is_hashable< T, std::void_t< decltype(std::declval< std::hash< T > >()(std::declval< T >()))> >`


---

## AlterTableEntryRecord

**Full name:** `neug::storage::AlterTableEntryRecord`

**Public Methods:**

- `AlterTableEntryRecord()`
- `AlterTableEntryRecord(const binder::BoundAlterInfo *alterInfo)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## BeginTransactionRecord

**Full name:** `neug::storage::BeginTransactionRecord`

**Public Methods:**

- `BeginTransactionRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CheckpointRecord

**Full name:** `neug::storage::CheckpointRecord`

**Public Methods:**

- `CheckpointRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## ColumnPredicate

**Full name:** `neug::storage::ColumnPredicate`

**Public Methods:**

- `ColumnPredicate(std::string columnName, common::ExpressionType ex...`
- `~ColumnPredicate()=default`
- `checkZoneMap(const MergedColumnChunkStats &stats) const =0`
- `toString()`
- `copy() const =0`
- `constCast() const`


---

## ColumnPredicateSet

**Full name:** `neug::storage::ColumnPredicateSet`

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

**Full name:** `neug::storage::ColumnPredicateUtil`

**Public Methods:**

- `tryConvert(const binder::Expression &column, const binder::E...`


---

## ColumnStats

**Full name:** `neug::storage::ColumnStats`

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

**Full name:** `neug::storage::CommitRecord`

**Public Methods:**

- `CommitRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CopyTableRecord

**Full name:** `neug::storage::CopyTableRecord`

**Public Methods:**

- `CopyTableRecord()`
- `CopyTableRecord(common::table_id_t tableID)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## CreateCatalogEntryRecord

**Full name:** `neug::storage::CreateCatalogEntryRecord`

**Public Methods:**

- `CreateCatalogEntryRecord()`
- `CreateCatalogEntryRecord(catalog::CatalogEntry *catalogEntry, bool isInternal)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## DropCatalogEntryRecord

**Full name:** `neug::storage::DropCatalogEntryRecord`

**Public Methods:**

- `DropCatalogEntryRecord()`
- `DropCatalogEntryRecord(common::table_id_t entryID, catalog::CatalogEntry...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## GNodeTable

**Full name:** `neug::storage::GNodeTable`

**Public Methods:**

- `GNodeTable(const catalog::NodeTableCatalogEntry *tableEntry,...`
- `~GNodeTable() override=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override`
- `getStats(const transaction::Transaction *transaction) const override`


---

## GRelTable

**Full name:** `neug::storage::GRelTable`

**Public Methods:**

- `GRelTable(common::row_idx_t numRows, catalog::RelTableCatal...`
- `~GRelTable() override=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override`
- `getSrcTableId() const`
- `getDstTableId() const`


---

## HyperLogLog

**Full name:** `neug::storage::HyperLogLog`

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

**Full name:** `neug::storage::MemoryBuffer`

**Public Methods:**

- `MemoryBuffer(MemoryManager *mm, common::page_idx_t blockIdx, u...`
- `~MemoryBuffer()=default`
- `DELETE_COPY_AND_MOVE(MemoryBuffer)`
- `getBuffer() const`
- `getData() const`
- `getMemoryManager() const`


---

## MemoryManager

**Full name:** `neug::storage::MemoryManager`

**Public Methods:**

- `MemoryManager()=default`
- `MemoryManager(BufferManager *bm, common::VirtualFileSystem *vfs)`
- `~MemoryManager()=default`
- `allocateBuffer(bool initializeToZero=false, uint64_t size=common...`
- `getPageSize() const`
- `getBufferManager() const`


---

## NodeDeletionRecord

**Full name:** `neug::storage::NodeDeletionRecord`

**Public Methods:**

- `NodeDeletionRecord()`
- `NodeDeletionRecord(common::table_id_t tableID, common::offset_t node...`
- `NodeDeletionRecord(common::table_id_t tableID, common::offset_t node...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## NodeTable

**Full name:** `neug::storage::NodeTable`

**Public Methods:**

- `NodeTable()=default`
- `NodeTable(const StatsManager *storageManager, const catalog...`
- `NodeTable(const StatsManager *storageManager, const catalog...`
- `~NodeTable()=default`
- `getNumTotalRows(const transaction::Transaction *transaction) override=0`
- `getStats(const transaction::Transaction *transaction) const =0`


---

## NodeUpdateRecord

**Full name:** `neug::storage::NodeUpdateRecord`

**Public Methods:**

- `NodeUpdateRecord()`
- `NodeUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `NodeUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelDeletionRecord

**Full name:** `neug::storage::RelDeletionRecord`

**Public Methods:**

- `RelDeletionRecord()`
- `RelDeletionRecord(common::table_id_t tableID, common::ValueVector *...`
- `RelDeletionRecord(common::table_id_t tableID, std::unique_ptr< comm...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelDetachDeleteRecord

**Full name:** `neug::storage::RelDetachDeleteRecord`

**Public Methods:**

- `RelDetachDeleteRecord()`
- `RelDetachDeleteRecord(common::table_id_t tableID, common::RelDataDirect...`
- `RelDetachDeleteRecord(common::table_id_t tableID, common::RelDataDirect...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RelTable

**Full name:** `neug::storage::RelTable`

**Public Methods:**

- `RelTable(catalog::RelTableCatalogEntry *relTableEntry, con...`
- `RelTable(catalog::RelTableCatalogEntry *relTableEntry, con...`
- `getNumTotalRows(const transaction::Transaction *transaction) override=0`


---

## RelUpdateRecord

**Full name:** `neug::storage::RelUpdateRecord`

**Public Methods:**

- `RelUpdateRecord()`
- `RelUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `RelUpdateRecord(common::table_id_t tableID, common::column_id_t c...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## RollbackRecord

**Full name:** `neug::storage::RollbackRecord`

**Public Methods:**

- `RollbackRecord()`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## StatsManager

**Full name:** `neug::storage::StatsManager`

**Public Methods:**

- `StatsManager(MemoryManager &memoryManager)`
- `StatsManager(const std::filesystem::path &statsPath, main::Met...`
- `StatsManager(const std::string &statsData, main::MetadataManag...`
- `~StatsManager()=default`
- `getTable(common::table_id_t tableID)`
- `loadTables(const catalog::Catalog &catalog, common::VirtualF...`


---

## StorageVersionInfo

**Full name:** `neug::storage::StorageVersionInfo`

**Public Methods:**

- `getStorageVersionInfo()`
- `getStorageVersion()`


---

## Table

**Full name:** `neug::storage::Table`

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

**Full name:** `neug::storage::TableInsertionRecord`

**Public Methods:**

- `TableInsertionRecord()`
- `TableInsertionRecord(common::table_id_t tableID, common::TableType tab...`
- `TableInsertionRecord(common::table_id_t tableID, common::TableType tab...`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer, const main::C...`


---

## TableStats

**Full name:** `neug::storage::TableStats`

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

**Full name:** `neug::storage::UpdateSequenceRecord`

**Public Methods:**

- `UpdateSequenceRecord()`
- `UpdateSequenceRecord(common::sequence_id_t sequenceID, uint64_t kCount)`
- `serialize(common::Serializer &serializer) const override`
- `deserialize(common::Deserializer &deserializer)`


---

## WAL

**Full name:** `neug::storage::WAL`

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

**Full name:** `neug::storage::WALRecord`

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

**Full name:** `neug::string_item`


---

## array< T, N > >

**Full name:** `neug::to_string_impl< std::array< T, N > >`

**Public Methods:**

- `to_string(const std::array< T, N > &empty)`


---

## array< T, N >, M > >

**Full name:** `neug::to_string_impl< std::array< std::array< T, N >, M > >`

**Public Methods:**

- `to_string(const std::array< std::array< T, N >, M > &empty)`


---

## pair< A, B > >

**Full name:** `neug::to_string_impl< std::pair< A, B > >`

**Public Methods:**

- `to_string(const std::pair< A, B > &t)`


---

## string >

**Full name:** `neug::to_string_impl< std::string >`

**Public Methods:**

- `to_string(const std::string &empty)`


---

## string_view >

**Full name:** `neug::to_string_impl< std::string_view >`

**Public Methods:**

- `to_string(const std::string_view &empty)`


---

## tuple< Args... > >

**Full name:** `neug::to_string_impl< std::tuple< Args... > >`

**Public Methods:**

- `to_string(const std::tuple< Args... > &t)`


---

## unordered_map< K, V > >

**Full name:** `neug::to_string_impl< std::unordered_map< K, V > >`

**Public Methods:**

- `to_string(const std::unordered_map< K, V > &vec)`


---

## vector< T > >

**Full name:** `neug::to_string_impl< std::vector< T > >`

**Public Methods:**

- `to_string(const std::vector< T > &vec)`


---

## LocalCacheManager

**Full name:** `neug::transaction::LocalCacheManager`

**Public Methods:**

- `contains(const std::string &key)`
- `at(const std::string &key)`
- `put(std::unique_ptr< LocalCacheObject > object)`
- `remove(const std::string &key)`


---

## LocalCacheObject

**Full name:** `neug::transaction::LocalCacheObject`

**Public Methods:**

- `LocalCacheObject(std::string key)`
- `~LocalCacheObject()=default`
- `getKey() const`
- `cast()`


---

## Transaction

**Full name:** `neug::transaction::Transaction`

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

**Full name:** `neug::transaction::TransactionActionUtils`

**Public Methods:**

- `toString(TransactionAction action)`


---

