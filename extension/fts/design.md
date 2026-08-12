## Cypher接口
### 创建索引
1. 支持在多列字符串属性上创建索引（主库暂不支持）
2. 支持自定义权重，通过map传入权重（主库暂不支持）
3. 其他参数均直接转发至SQLite，如tokenizer/prefix/detail/rank，参数名必须使用小写。
    1. tokenizer：目前支持unicode61 (default)、ascii、porter、trigram。后续可支持自定义tokenizer。
    2. prefix：前缀，表示额外增加对前缀的索引，例如插入abcd，那么也会插入到ab\*和abc\*。默认为空。
    3. detail：表示fts保存的信息。detail=none时什么都不记录；detail=column时会记录column name和row id；detail=full时还会记录term在字符串中的偏移量。默认为full。
    4. rank：排序函数，默认且只有bm25。（可以去掉？）

```cypher
CREATE INDEX entity_fts_index
ON Entity
USING FTS(title, description)
WITH (
    weight = {'title': 8.0, 'description': 2.0},
    tokenizer = 'unicode61',
    prefix = '2 3',
    detail = 'full',
    rank = 'bm25'
);
```

目前SQLite FTS支持的分词器主要适用于英文，对于中文只有一个通用的分词器unicode61，它只会根据特殊字符和标点符号进行划分。目前对于中文最合适的分词器是Jieba，后续可以做接入：[https://github.com/fxsjy/jieba](https://github.com/fxsjy/jieba)

### 删除索引
1. 计划通过索引名删除索引，不需要重复指定点类型、属性列表或索引类型
2. 计划支持 `IF EXISTS`，索引不存在时不报错

```cypher
DROP INDEX IF EXISTS entity_fts_index;
```

### 查询计划改写
demo 中通过 `bm25` 表达全文检索及相关性分数：

```cypher
MATCH (n:Entity)
RETURN n, bm25(n.description, 'graph database') AS score
ORDER BY score ASC
LIMIT 10;
```

Compiler会提供统一的算子识别优化规则，将上述查询统一改写为`IndexScan`算子，并调用执行索引中的Search接口；FTS索引只负责实现具体的实现接口 `SearchImpl`。

## Statement
SQLite支持提前预编译查询，提前将查询固化为执行算子，实际请求查询时只需要绑定参数即可直接执行。

### Insert Statement
AppendImpl每次插入前先reset statement并清除旧参数，再绑定index ID和属性值。

```sql
INSERT INTO fts_table(rowid, text) VALUES (?1, ?2);
```

### Query Statement
SearchImpl每次查询前先reset statement并清除旧参数，再绑定FTS查询字符串，通过Step逐条读取按rank排序的候选。

```sql
SELECT rowid, rank FROM fts_table
WHERE fts_table MATCH ?1 ORDER BY rank ASC;
```

## 多线程支持
目前所有的请求（Append、Search）都会以并发的形式发送到StorageIndex中，目前采用SQLite的标准查询接口执行，对应的链路如下：

```plain
NeuG Transactions -> Storage Index -> Append/Search -> Statement -> Connection
```

### SQLite的并行逻辑
SQLite的每个connection只能串行执行，即使同时发送多个请求，在内部也会加锁确保串行执行。

SQLite的一个connection可以生成多个statement。每个statement都有独立的状态，因此一个statement同时只能处理一个请求，如果statement不独占就必须加锁。

一个connection的多个statement可以并发执行，但内部还是受到connection的串行限制，实际表现为交替执行。



此外，SQLite每次创建connection的代价较大，而且在单个索引中频繁创建connection也非常不合理，因此提前创建connection并准备statement是一个更好的方案。

### 单读单写方案
单读单写方案是一个最简单的实现，即只有一个读连接和写连接，各自负责一个读查询和写查询的statement。

```cpp
std::shared_ptr<SQLiteConnection> database_;        // 共享数据库连接
std::shared_ptr<SQLiteStatement> write_statement_;  // 共享写连接
std::shared_ptr<SQLiteStatement> read_statement_;   // 共享读连接
```

statement内部保存了当前执行的状态，无法并发，因此在Append/Search中需要对statement进行加锁，这和sqlite的execute逻辑一致。单读方案可以看作是大小为1的读连接池，优点在于不需要实现连接池的调度和管理。

此外经过分析，可以直接在初始的storage index中直接创建并长期持有读写连接，没有必要在Detach的时候创建写连接。

### 多读单写方案
多读单写方案就是将读连接进一步扩展为一个读连接池。实现的难度主要在于：

1. 读连接的资源调度，需要实现轮询调度或者round robin调度
2. 读连接的释放，需要RAII包装传递给Search函数，确保查询出错时能够释放锁

在demo测试中，多读单写方案相比于单读单写方案，设置2个读线程的性能提升约10%，因此初版暂不考虑。

```cpp
std::shared_ptr<SQLiteConnection> database_;           // 共享数据库连接
std::shared_ptr<SQLiteStatement> write_statement_;     // 共享写连接
std::shared_ptr<QueryConnectionPool> read_statements_; // 共享读连接池

// 每个读连接都会和一个statement一一对应
struct QueryConnection {
SQLiteConnection connection;
SQLiteStatement statement;
bool in_use{false};
};

class QueryConnectionPool {
public:
// Lease类用于RAII包装QueryConnection，确保即使查询报错也能正常释放锁
class Lease {
 public:
  Lease(const Lease&) = delete;
  Lease& operator=(const Lease&) = delete;
  Lease(Lease&& other) noexcept;
  Lease& operator=(Lease&& other) = delete;
  ~Lease();

  SQLiteStatement& statement() const;

 private:
  friend class QueryConnectionPool;
  Lease(QueryConnectionPool* pool, QueryConnection* connection)
      : pool_(pool), connection_(connection) {}
  void Release();

  // pool用于释放锁
  QueryConnectionPool* pool_{nullptr};
  QueryConnection* connection_{nullptr};
};

void Open(const std::string& path, const std::string& sql, size_t size);
void Close();
Lease Acquire();

private:
void Release(QueryConnection* connection);

std::mutex mutex_;
std::condition_variable available_;
std::vector<std::unique_ptr<QueryConnection>> connections_;
bool open_{false};
};
```

## 索引行为与NeuG的对齐
NeuG的索引更新/查询均由索引框架负责，将cypher语句映射为原子的Append/Search操作，索引实例不负责管理查询事务的隔离，而是通过ID Accesor统一过滤可见性。

索引的Open/Clone行为需要对齐NeuG的事务执行逻辑，下面分成AP和TP分别分析。

### AP模式
AP模式下不会执行Clone，而是在每次写之前调用一次Detach。全局只有一个唯一的Storage Index实例，所有操作直接应用在ID Accessor和sqlite主库上，这一行为和当前NeuG的AP模式一致。

TODO：后续磊哥重构的事务管理会对AP模式也执行COW，因此索引的逻辑也会相应地改为Clone，但这些操作都是在StorageIndex层面的改动，具体的Index实例操作是兼容的。

| **NeuG行为** | **索引行为** | **具体操作** |
| :---: | :---: | :---: |
| 数据库初始化 | StorageIndex实例化 + 调用Open | 创建读连接池 |
| 并发读 | 并发调用Search | 在连接池中选择空闲的连接执行 |
| 独占写 | Detach + Append | 创建写连接 + 写连接串行执行 |
| Checkpoint | Dump | 写操作独占，SQLite断开连接后持久化，框架负责reopen |


### TP模式
TP模式下，NeuG行为可以抽象为多个Read Transaction和最多一个Update Transaction并发。

Update Transaction全局唯一，索引层面和COW的逻辑类似，执行一次Clone， 创建一个新的StorageIndex实例，拷贝metadata信息并创建写连接，但注意sqlite本身不会被拷贝，而是共享读连接。

此外TP模式下允许读写并发，即读事务依然能够持续地读取旧数据，所以sqlite要设置为WAL模式，即写入的数据临时存放在WAL中，旧副本依然可访问。

执行Dump时，NeuG的checkpoint会等待读事务执行完毕并阻塞新的读事务，此时索引部分可以直接关闭sqlite、复制文件，不会产生并发冲突。

| **NeuG行为** | **索引行为** | **具体操作** |
| :---: | :---: | :---: |
| 数据库初始化 | StorageIndex实例化 + 调用Open | 创建读连接池 |
| 创建Read Transaction | 无 | 无 |
| 并发执行Read Transaction | 并发调用Search | 在连接池中选择空闲的连接执行 |
| 创建Update Transaction | 调用Clone | 拷贝metadata，共享读连接池和整个底层sqlite数据库 |
| 执行Update Transaction | Detach + Append | 创建写连接 + 拷贝id accessor + 写连接串行执行 |
| Checkpoint | Dump | 写事务独占，SQLite断开连接后持久化，框架负责reopen |


整体来看，TP模式的行为能够兼容AP模式，因此后续均采用这个逻辑实现。

## Module 接口
全文索引 FTSIndex 继承统一的 StorageIndex 接口，只需要实现特定的函数接口，具体的调用由索引框架统一调用，遵循统一的创建/删除/数据更新机制。

```cpp
class FTSIndex final : public StorageIndex {
 public:
  ~FTSIndex() override;

  // Module 基类的接口，负责索引的基本操作（Open/Dump；TODO：Clone/Detach的支持）
  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& descriptor, MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& manifest,
            const std::string& key) override;
  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  // StorageIndex 的接口，负责绑定及批量导入数据接口
  Status Rebind(const IndexBindContext& context) override;
  Status BulkBuild(const VertexSet& vertices) override;

 protected:
  // StorageIndex 的接口，由具体索引实现的数据查询和写入接口
  result<std::vector<SearchCandidate>> SearchImpl(
      const IndexQueryParams& params) override;

  Status AppendImpl(index_id_t index_id,
                    const Value& value) override;
};
```

### Open
1. 恢复IndexIDAccessor。由于这是一个单独的Module，所以需要从manifest中恢复。
2. 恢复索引数据。将snapshot文件复制到runtime目录并打开。
3. FTS表创建或校验完成后，prepare insert和query两个statement。



```cpp
void FTSIndex::OpenInternal(
    Checkpoint& ckp, const CheckpointManifest* manifest,
    const ModuleDescriptor& descriptor, MemoryLevel level) {

  // 恢复索引元数据和IndexIDAccessor。
  StorageIndex::Open(ckp, descriptor, level);
  auto accessor_descriptor = ResolveAccessorDescriptor(manifest, descriptor);
  index_id_accessor_->Open(ckp, accessor_descriptor, level);

  // 将持久化文件复制到runtime目录，并在runtime文件上打开SQLite。
  auto runtime_file = ckp.CreateRuntimeFile();
  auto index_path = descriptor.get_path(kIndexFilePath);
  if (index_path) {
    file_utils::copy_file(*index_path, runtime_file.path(), true);
  }
  database_.Open(runtime_file.path());
  if (index_path) {
    ValidateExistingTable();
  } else {
    CreateTable();
  }

  // 生成 prepared statement
  append_statements_ = database_.Prepare(
      "INSERT INTO " + table_name_ + "(rowid, text) VALUES (?1, ?2)");
  search_statements_ = database_.Prepare(
      "SELECT rowid, rank FROM " + table_name_ +
      " WHERE " + table_name_ + " MATCH ?1 ORDER BY rank ASC");
  runtime_file_ = std::move(runtime_file);
}
```

### Dump
1. 保存索引的metadata信息和IndexIDAccessor。
2. 释放两个prepared statement，再关闭SQLite connection并完成snapshot创建。
3. 保存snapshot路径并结束Dump，后续由外层框架执行reopen。

```cpp
void FTSIndex::Dump(
    Checkpoint& ckp, CheckpointManifest& manifest,
    const std::string& key) {
  // 保存索引类型、IndexMeta及IndexIDAccessor引用。
  StorageIndex::Dump(ckp, manifest, key);
  const auto accessor_key = "sqlite_fts_accessor_" + meta_->name;
  index_id_accessor_->Dump(ckp, manifest, accessor_key);
  manifest.mutable_modules().at(accessor_key).mark_as_referenced_module();
  manifest.mutable_modules().at(key).set_ref(kAccessorRef, accessor_key);

  // 释放两个prepared statement
  append_statements_ = SQLiteStatement{};
  search_statements_ = SQLiteStatement{};

  // 刷新并关闭SQLite，将runtime文件提交到snapshot目录。
  FTSDumpContainer container(&database_, runtime_file_->path());
  auto persisted_path = ckp.Commit(container);
  runtime_file_.reset();
  manifest.mutable_modules().at(key).set_path(kIndexFilePath, persisted_path);
}
```

### Clone
1. 复制索引的metadata信息，并Clone IndexIDAccessor。
2. SQLite索引数据全局只有一份，因此共享SQLite connection、runtime文件及对应的锁。属性列由PropertyGraph在Clone完成后重新Rebind。

```cpp
std::unique_ptr<Module> FTSIndex::Clone() const {
  auto cloned = std::make_unique<FTSIndex>();
  cloned->meta_ = std::make_unique<IndexMeta>(*meta_);
  auto accessor = index_id_accessor_->Clone();
  cloned->index_id_accessor_.reset(
      static_cast<IndexIDAccessor*>(accessor.release()));

  // 共享SQLite connection、runtime文件及对应的锁。
  cloned->sqlite_state_ = sqlite_state_;
  cloned->table_name_ = table_name_;
  cloned->tokenizer_ = tokenizer_;
  cloned->prefix_ = prefix_;
  cloned->detail_ = detail_;
  return cloned;
}
```

### Detach
SQLite的connection全程共享，数据实时写入sqlite，只在ID Accessor层面做过滤。

因此Detach只需要分离ID Accessor，Update Transaction修改这个Accessor副本即可。

```cpp
void FTSIndex::Detach(Checkpoint& ckp, MemoryLevel level) {
  index_id_accessor_->Detach(ckp, level);
}
```

## StorageIndex 接口
StorageIndex提供四个接口（Rebind/BulkBuild/AppendImpl/SearchImpl），供特定的索引类型重写，并在框架层面统一调用。

### Rebind
Rebind 操作可以保存当前PropertyGraph所拥有的属性列指针，用于直接访问列属性。引入 Rebind 操作是因为，bound_column_是一个固定值，在 Open/Clone 之后，物理存储位置会发生变化，需要重新更新。

Rebind 之后，索引可以直接访问原始的列数据，在 FTS 索引中主要用于 BulkBuild。

TODO：多列情况需要扩展，demo已实现，待合入主库

```cpp
Status FTSIndex::Rebind(const IndexBindContext& context) {
  if (!context.column) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "FTSIndex requires a property column");
  }
  bound_column_ = context.column;
  return Status::OK();
}
```

### BulkBuild
在 demo 实验中，我们发现索引初始化是一个性能瓶颈：初版会遍历所有已有的 value 并调用 Upsert，而每个 Upsert 又会被视作一个独立的 sqlite transaction，导致退化为逐行提交。而 BulkBuild 则将所有 Upsert 包在一个显式事务中，优化为批量提交，减少事务管理带来的开销。

Rebind 中保存的 bound_column_ 就是用在这里，直接从原始列获取数据。

```cpp
Status FTSIndex::BulkBuild(const VertexSet& vertices) {
  CHECK_BOUND_AND_OPEN();
  database_.Execute("BEGIN TRANSACTION;");
  try {
    for (vid_t vid : vertices) {
      auto status = Upsert(vid, bound_column_->get_any(vid));
      if (!status.ok()) {
        database_.Execute("ROLLBACK;");
        return status;
      }
    }
    database_.Execute("COMMIT;");
    return Status::OK();
  } catch (...) {
    database_.Execute("ROLLBACK;");
    return Status::RuntimeError("FTSIndex bulk build failed");
  }
}
```

### AppendImpl
index框架先通过IndexIDAccessor为点分配新的index ID，再调用AppendImpl追加FTS记录。

SQLite的rowid直接使用index ID；更新不覆盖旧记录，而是直接追加新的版本，旧版本由IndexIDAccessor映射失效，后续查询时过滤。

```cpp
Status FTSIndex::AppendImpl(index_id_t index_id, const Value& value) {
  if (value.IsNull() || value.type().id() != DataTypeId::kVarchar) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "FTS value must be STRING");
  }
  append_statements_.Reset();
  append_statements_.ClearBindings();
  append_statements_.BindInt64(1, index_id);
  append_statements_.BindText(2, value.GetValue<std::string>());
  append_statements_.Step();
  return Status::OK();
}
```

### SearchImpl
查询参数包含FTS5查询字符串、topk和scalar filter（可选）。

SQLite按rank升序返回候选，避免额外的排序，生成结果通过 Step 逐条返回，接着通过mvcc过滤掉旧版本数据，最后收集到topk中。不能在SQLite查询中直接LIMIT topk，否则无效旧记录可能导致结果不足。

对于包含 `WHERE` 的查询，标量条件先执行并生成候选VID集合，作为scalar filter传入SearchImpl，FTS只需要在过滤阶段同时执行mvcc过滤和标量过滤即可。过滤发生在topk截断之前，因此可以得到严格正确的topk。

SearchImpl只返回内部index ID和SQLite rank；StorageIndex::Search负责最终转换为VID并再次过滤当前快照不可见的映射（冗余操作）。

```cpp
result<std::vector<SearchCandidate>> FTSIndex::SearchImpl(
    const IndexQueryParams& params) {
  auto* query = dynamic_cast<const FTSQueryParams*>(&params);
  if (!query || query->topk == 0) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "invalid FTS query parameters");
  }

  search_statements_.Reset();
  search_statements_.ClearBindings();
  auto allowed = MakeVIDSet(query->scalar_filter);
  search_statements_.BindText(1, query->query_string);

  std::vector<SearchCandidate> results;
  while (search_statements_.Step() == SQLITE_ROW && results.size() < query->topk) {
    auto index_id = search_statements_.ColumnInt64(0);
    auto vid = index_id_accessor_->GetVIDByIndexID(index_id);
    if (vid == INVALID_VID ||
        (query->use_scalar_filter && !allowed.contains(vid))) {
      continue;
    }
    results.push_back({index_id, search_statements_.ColumnDouble(1)});
  }
  return results;
}
```

## 错误处理
由于目前索引架构采用Append-Only模式，普通的Append/Search操作不负责删除已经写入SQLite的历史数据。BulkBuild是例外：BulkBuild会显式开启SQLite transaction，因此必须在成功时COMMIT，在任意失败路径执行ROLLBACK，确保该transaction正常结束。

这里的“不回滚”特指不清理由普通Append或已abort的TP事务写入SQLite的无效历史记录，不表示可以遗留一个未结束的SQLite transaction。

具体来说：

1. AP模式下，写操作是独占的，任何写入操作都会直接体现在id accessor和sqlite主库中。即使一个cypher查询写入了多个值，也会被拆分为多个Append原子执行，导致中途出错时不会回滚，后续可能读到中间状态。这与NeuG目前行为保持一致。
2. TP模式下，写入操作会在Clone出来的StorageIndex副本上执行，其中connection共享，数据会直接插入，但是id accessor会生成副本，可见性只会在副本上修改。任意时刻发生异常，都会让整个transaction直接abort，此时这个StorageIndex副本会被析构，id accessor不会被发布，自然也不会影响其他正在执行或者未来的查询，但会导致无效数据持久化驻留在sqlite中。
3. BulkBuild只用于创建尚未发布的新索引。BEGIN成功后，COMMIT之前的任意Status错误或异常都直接执行ROLLBACK；BulkBuild失败后该索引实例不会发布。ROLLBACK只负责关闭BulkBuild创建的SQLite transaction，不需要逐条恢复尚未发布的IndexIDAccessor。

总结：允许无效的append-only记录停留在SQLite中；AP模式允许出现与NeuG写入语义一致的中间状态；TP模式通过不发布IndexIDAccessor副本实现abort；BulkBuild必须负责结束自己创建的SQLite transaction。

### 错误传播边界
错误的传播方式由StorageIndex接口的返回类型决定：

1. `Open/Dump/Clone/Detach`没有Status返回值，失败时抛出NeuG异常。
2. `Rebind/BulkBuild/AppendImpl`返回`Status`，不得把SQLite异常直接泄漏给调用方；在接口边界捕获底层异常并转换为Status。
3. `SearchImpl`返回`result<std::vector<SearchCandidate>>`，参数或执行失败通过`result`中的Status返回。
4. `SQLiteStatement`和`SQLiteConnection`本身会处理大量与SQLite交互的错误情况，并通过NeuG异常报告错误。它们抛出的异常在`Open/Dump/Clone/Detach`中直接向上传播；如需清理资源，可以捕获后通过`throw;`原样重抛。对于返回`Status/result`的接口，只在接口边界将异常转换一次。
5. 最外层pipeline按照仓库`TRY_HANDLE_ALL_WITH_EXCEPTION`的规则将NeuG异常转换为StatusCode。

### 各接口的错误约定

- `Open`（Exception）
    - 索引名称、property类型或option名称错误：`InvalidArgumentException`
    - manifest中的IndexIDAccessor引用无法解析：`RuntimeError`
    - SQLite打开、FTS参数校验、PRAGMA或statement prepare失败：`RuntimeError`
    - 已有FTS表缺失：`RuntimeError`
- `Dump`（Exception）
    - SQLite connection不可用：`RuntimeError`
    - SQLite WAL checkpoint/flush失败：`RuntimeError`
    - snapshot提交、rename失败：保留文件系统原始异常
- `Clone/Detach`（Exception）
    - IndexIDAccessor自身抛出的异常保持原类型向上传播
- `Rebind`（Status）
    - property column为空：`ERR_INVALID_ARGUMENT`
- `BulkBuild`（Status）
    - 未Rebind或未Open：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
    - 列值不满足Append要求：透传`AppendImpl`的`ERR_QUERY_EXECUTION`
    - SQLite BEGIN/Append/COMMIT/ROLLBACK失败：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
    - BEGIN成功后的所有失败路径都尝试ROLLBACK
- `AppendImpl`（Status）
    - NULL或非STRING值：`ERR_INVALID_ARGUMENT`
    - 未Open或statement未准备：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
    - SQLite reset/bind/step失败：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
- `SearchImpl`（result/Status）
    - 参数类型错误、topk为0：`ERR_INVALID_ARGUMENT`
    - FTS5查询字符串语法错误：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
    - 未Open或statement未准备：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）
    - SQLite reset/bind/step失败：`ERR_QUERY_EXECUTION`（`Status::RuntimeError`）

错误信息必须保留操作名称、索引名、SQLite error code和原始SQLite message；涉及文件时还必须包含源路径或目标路径。包装错误时保留原始信息，不重复添加同一层上下文。

## 基于SQLite潜在的问题&后续优化改进
### 多版本数据问题/in-filtering支持
NeuG的索引框架定义为append-only + mvcc，在一个索引实例中会存储来自不同版本的数据，实现高效的版本隔离和读写并发。为此，索引也必须支持in-filtering，来过滤掉当前版本不可见的内容。此外，用户本身也可能在查询中带入where过滤条件，并要求在索引中实时应用。

由于sqlite fts原生不支持in-filtering，目前的方案会全量计算并排序所有文档，经过mvcc过滤和scalar过滤后，再执行topk截断。看似是post-filtering，实际上由于会对全量文档排序，所以保证最后结果一定会补满topk（如果可以的话）。

sqlite fts支持fetch_limit参数，即内部计算的候选集大小。在当前的方案中，fetch_limit必须设置为max，表示需要对所有文档计算排序，即使最后可能只需要一个较小的topk。fetch_limit对性能的影响较大，在demo上的实验表明，如果fetch_limit设置为128，**相比于设置为max能够提升25%**。

问题在于，如果设置了fetch_limit，那么会导致潜在的正确性问题；如果暴力对所有文档排序，性能就会有一定损失。如果想要完美解决这个问题，就需要实现真正的in-filtering，即在fts索引计算的内部实时执行过滤。

目前，sqlite的fts不支持也没有开放相关接口，后续可以考虑深入sqlite内部修改相关逻辑，或是自行实现。

### SQLite connection
目前neug索引以connection的方式与sqlite交互，且框架层面无法感知neug的connection，无法做到neug connection和sqlite connection的一一对应，必须在初始化阶段全部开启。

目前的设计采用单读单写方案，即创建一个读连接和一个写连接，分别负责Search和Append操作。一方面，后续可以将读连接扩展为读连接池，在不需要修改索引原有逻辑的情况下实现更高的并发度；另一方面，在demo上的测试表明，将读连接的数量翻倍（提升为两个读连接）**对qps的提升仅有10%**，说明主要的瓶颈并不在sqlite的连接数量上。

### 写放大问题
NeuG的索引设计为append-only，所有历史数据都会持久化在索引中，主要包括：

1. update/delete之后的旧版本数据，在没有读连接依赖后依然存在
2. abort之后的旧版本数据，在回滚后依然存在

目前的解决方案，相比于修改/压缩，直接重构的效率更高，主要原因有两个：

1. 在倒排列表中删除元素开销较大，而且sqlite fts本身也不会物理删除，而是加入一个删除标记
2. 重新构建的额外开销主要来自于tokenizer，可以忽略

### FTS存在大量无效优化 & 自行实现fts的可行性分析
SQLite FTS本身的实现是标准的BM25算法，并且做了非常多的工程优化，包括分段、分块、压缩等等，主要是为了提升以下场景的性能：

1. 磁盘存储，存在大量IO
2. 以id作为主键，任意插入/删除/修改数据

但是，我们的索引是纯内存的、id从0开始递增的、append-only架构，这些优化都没有意义。而且如果将这些无关的优化剥离之后，其实fts5的内容非常薄，只有内部b-tree存储和一些最基本的剪枝。理论上改用NeuG的存储或内存数据结构应该能够复现，且达到和sqlite fts相当的效率。

路线建议：先深入sqlite fts5源码确认执行路径，然后评估工作量，最后实现mvp版本demo进行基准测试。
