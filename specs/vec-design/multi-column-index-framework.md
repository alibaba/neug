# 多列索引框架改造设计

## 1. 背景与目标

当前 NeuG 的通用索引框架以“一个索引绑定一个顶点属性”为基本假设。该假设不仅存在于 `CREATE INDEX` 语法，还贯穿编译 IR、索引元数据、列绑定、索引写接口、增量维护、checkpoint 恢复和索引发现接口。

目标是让一个索引能够有序地绑定同一顶点标签下的一列或多列属性，例如：

```cypher
CREATE INDEX entity_text_fts
ON Entity USING FTS (name, description)
WITH (
    name_weight = 8.0,
    description_weight = 2.0
);
```

单列形式必须继续可用：

```cypher
CREATE INDEX item_embedding_hnsw
ON Item USING HNSW (embedding)
WITH (metric = 'cosine');
```

本设计只扩展通用索引框架的绑定能力，不要求所有索引实现都接受任意列数。索引扩展需要声明和校验自己的列数、列类型及选项约束。例如，FTS 可以支持多列 `VARCHAR`，HNSW 第一阶段仍只允许一列向量属性。

## 2. 当前单列假设

| 层次 | 当前表示或接口 | 单列限制 |
| --- | --- | --- |
| Parser | `ParsedCreateIndexInfo::propertyName` | AST 只保存一个属性名 |
| Binder | `BoundCreateIndexInfo::propertyName/propertyType` | 只解析和校验一个属性 |
| Physical IR | `physical::CreateIndex.property/property_type` | protobuf 字段为 singular |
| Metadata | `IndexBindSchema::property_name/property_type` | checkpoint 中只持久化一列 |
| Bind | `IndexBindContext::column`、`Rebind(...)` | 扩展只能得到一个 `ColumnBase*` |
| Write | `StorageIndex::Upsert(vid, Value)`、`AppendImpl(..., Value)` | 一次只能写入一个属性值 |
| Manager | `CreateIndex(..., const ColumnBase*)` | 创建与 bulk build 只绑定一列 |
| Lookup | `GetIndex(label, property)` | 只能传一个属性，不能按完整列集合精确匹配多列索引 |
| DML | 插入/更新逐属性调用 `Upsert` | 更新复合索引时拿不到同一行的其他列值 |
| Recovery | `IndexColumns` 和 `ActivateIndexes` | pending index 恢复时只重新绑定一列 |
| Schema DDL | drop/rename property | 只修改 `property_name` |
| Introspection | `SHOW_INDEXES().property` | 只展示一个属性名 |

相关代码入口包括：

- `src/compiler/scripts/antlr4/Cypher.g4` 和 `src/compiler/antlr4/Cypher.g4`
- `include/neug/compiler/parser/ddl/create_index.h`
- `include/neug/compiler/binder/ddl/bound_create_index.h`
- `proto/cypher_ddl.proto`
- `include/neug/storages/index/storage_index.h`
- `include/neug/storages/index/storage_index_manager.h`
- `src/storages/index/storage_index_manager.cc`
- `src/storages/graph/graph_interface.cc`
- `src/storages/graph/property_graph.cc`
- `src/transaction/update_transaction.cc`
- `src/compiler/function/show_indexes_function.cpp`
- `extension/fts`、`extension/vector_search` 以及 storage index 测试实现

## 3. 统一数据模型

### 3.1 有序列定义

把单个属性改为有序列定义。列顺序是索引身份和扩展语义的一部分，不能使用无序 map 表示。

```cpp
struct IndexBindColumn {
  std::string property_name;
  DataType property_type;
};

struct IndexBindSchema {
  label_t label_id = 0;
  std::vector<IndexBindColumn> columns;
};
```

建议不要继续维护可写的 `property_name`/`property_type` 镜像字段，否则两套状态容易不一致。可以在过渡期提供只读便利方法：

```cpp
bool IsSingleColumn() const;
const IndexBindColumn& SingleColumn() const;
bool ContainsProperty(std::string_view name) const;
std::optional<size_t> FindProperty(std::string_view name) const;
```

// 如果不维护可写接口，rename 要怎么实现呢？

框架层应保证：

- 至少包含一列；
- 同一索引内属性名不重复，比较规则与 catalog 一致；
- 所有列属于同一个顶点标签；
- 列顺序在 parser、IR、metadata、bind、write 和 restore 全链路保持不变；
- 类型来自 catalog，而不是由扩展或用户输入推断。

### 3.2 行值、单列更新值与绑定上下文

Graph interface 有两种写入形态：

- insert 一次提供一个顶点的全部属性；
- update/upsert 一次只提供一个顶点的单个属性。即使一条语句修改多个属性，graph interface 也会将其拆成多个单属性 update 操作。

索引接口需要同时迎合这两种形态。绑定上下文保存有序列指针；完整值数组与 metadata 同序；单列更新值则同时携带该列在索引绑定列中的位置和新值：

```cpp
struct IndexBindContext {
  std::vector<const ColumnBase*> columns;
};

using IndexValues = std::vector<Value>;

struct IndexValue {
  // Position in IndexBindSchema::columns, not the property-table column id.
  size_t column_id;
  Value value;
};

Status Upsert(vid_t vid, const IndexValue& value);
Status Upsert(vid_t vid, const IndexValues& values);
virtual Status AppendImpl(index_id_t index_id,
                          const IndexValues& values) = 0;
```

`IndexValue::column_id` 表示该属性在当前索引绑定列中的位置，不是 vertex table 中的 property column id。调用 `GetIndexesContainingProperty` 时，manager 应同时返回或能够快速解析该属性在每个索引内的 `column_id`，避免调用方重复按名称扫描 metadata。

`values[i]` 必须对应 `meta.schema.columns[i]`。框架检查数组长度、`column_id` 范围以及值类型，扩展检查自身的类型和 `NULL` 语义。

两个入口的职责如下：

```cpp
Status StorageIndex::Upsert(vid_t vid, const IndexValue& update) {
  if (update.column_id >= bind_context_.columns.size()) {
    return Status::InvalidArgument("Index column id is out of range");
  }

  IndexValues values;
  values.reserve(bind_context_.columns.size());
  for (const auto* column : bind_context_.columns) {
    values.emplace_back(ReadValue(*column, vid));
  }

  // Use the value carried by this update even if graph storage visibility or
  // call ordering means the bound column still exposes the previous value.
  values[update.column_id] = update.value;
  return Upsert(vid, values);
}
```

- `Upsert(vid, IndexValues)` 是统一实现：处理 index id、整行 `NULL` 策略，并最终调用 `AppendImpl(index_id, values)`；
- `Upsert(vid, IndexValue)` 是单属性适配入口：从 bound columns 读取该 vid 的其他属性，用本次更新值覆盖 `values[column_id]`，然后委托给完整值入口；
- insert 路径直接构造完整的 `IndexValues`，不需要先写入 graph 再从 bound columns 回读；
- 具体索引扩展只实现接收完整 `IndexValues` 的 `AppendImpl`，不负责补齐其他列。

`StorageIndex` 因此需要保存或能够访问最近一次 `Rebind` 的 ordered bind context。列读取应通过统一 helper 完成，正确处理主键列、普通属性列、soft-deleted 列和 `NULL`。

## 4. 编译和 DDL 接口修改

### 4.1 Grammar 与 Parser

把 `CREATE INDEX` 中的单个 `oC_PropertyKeyName` 改为非空属性列表：

```antlr
nEUG_CreateIndexPropertyList
    : oC_PropertyKeyName (SP? ',' SP? oC_PropertyKeyName)*
    ;
```

并在两份 grammar 源中保持一致，随后按项目流程重新生成 ANTLR 产物。

将：

```cpp
std::string propertyName;
```

改为：

```cpp
std::vector<std::string> propertyNames;
```

`Transformer::transformCreateIndex` 需要按出现顺序提取全部属性，拒绝空列表，并保留原有 options 与 `IF NOT EXISTS` 行为。

### 4.2 Binder

将 `BoundCreateIndexInfo` 改为：

```cpp
std::vector<std::string> propertyNames;
std::vector<common::DataType> propertyTypes;
```

Binder 对每一列执行存在性和类型解析，并增加：

- 重复属性校验；
- 列数上限校验（如框架需要设置防御性上限）；
- 调用索引类型约束校验。建议让索引模块暴露 schema validation hook，或由注册表提供 `min_columns`、`max_columns` 和允许类型，避免在 binder 中硬编码 `fts`/`hnsw`；
- options 与列的关联校验应由具体扩展完成，例如 FTS 的 `<column>_weight` 必须引用已索引列。

如果扩展在 bind 阶段尚未加载，至少完成通用校验，具体约束可延迟到执行 `CREATE INDEX` 时检查并返回明确错误。

### 4.3 Protobuf/Physical IR

不要直接把已有 protobuf 字段从 singular 改成 repeated 并复用字段号。为兼容旧 plan/序列化数据，建议新增消息：

```proto
message IndexColumn {
  string property = 1;
  common.DataType property_type = 2;
}

message CreateIndex {
  string name = 1;
  common.NameOrId vertex_type = 2;
  string create_index_type = 3;
  string property = 4 [deprecated = true];
  common.DataType property_type = 5 [deprecated = true];
  map<string, string> options = 6;
  ConflictAction conflict_action = 7;
  repeated IndexColumn columns = 8;
}
```

// 我理解目前的索引功能应该不需要考虑兼容性，可以将单列 property/property_type 直接去除

新 planner 只写 `columns`。执行器读取时优先使用 `columns`，为空时回退到旧的 `property/property_type`，以兼容旧 physical plan。`GDDLConverter::convertCreateIndex` 和 `CreateIndexMeta` 都需要同步修改。

## 5. 存储索引框架修改

### 5.1 元数据与 checkpoint 兼容

新的 JSON 建议写入：

```json
{
  "label_id": 1,
  "columns": [
    {"property_name": "name", "property_type_detail": "..."},
    {"property_name": "description", "property_type_detail": "..."}
  ]
}
```

`IndexBindSchema::FromJson` 应：

1. 优先读取 `columns`；
2. 若不存在，则读取旧 `property_name/property_type_detail` 并转换为单元素数组；
3. 拒绝空列、重复列、缺失类型或格式错误的数据。

新 checkpoint 可以只写 `columns`；若需要新版本产生的 checkpoint 可被旧二进制读取，则需要双写旧字段，但双写只能覆盖单列索引，多列索引必须明确标记最低兼容版本。建议采用“新读旧、旧不读新”的单向兼容策略，并在 manifest/metadata 中增加 schema version。

`IndexMeta::RenameProperty` 应遍历命中的列并保持原位置不变。建议改名为 `RenameBoundProperty(old_name, new_name)`，以免调用者错误地覆盖其他列。

### 5.2 创建、绑定和恢复

修改 `StorageIndexManager::CreateIndex`：

```cpp
result<StorageIndex*> CreateIndex(
    std::unique_ptr<IndexMeta> meta,
    std::unique_ptr<IndexIDAccessor> index_id_accessor,
    std::vector<const ColumnBase*> columns,
    const VertexSet& vertex_set);
```

创建前校验 metadata、columns 和类型逐项对应，再通过 `IndexBindContext` 传给扩展。

以下路径都必须按 metadata 顺序收集所有列：

- `StorageAPUpdateInterface::CreateIndex`；
- `PropertyGraph::rebind_indexes`；
- `StorageIndexManager::ActivateIndexes`；
- graph clone/COW 后的 rebind；
- pending extension 加载后的激活。

`IndexColumns` 目前可继续作为 `label -> property -> ColumnBase*` 的查找表，但激活时需要循环 `meta.schema.columns` 构建 ordered vector，不能把 map 的迭代顺序当作索引列顺序。

HNSW 的 `VecColumn` 转换、`VecColumnBackedIndexIDAccessor` 和 drop 时转回 `ArrayColumn` 都是单列特化逻辑，不修改原有语义。`StorageAPUpdateInterface::CreateIndex/DropIndex` 在 HNSW column 转换分支中使用 `GetIndex(label, {property})`，因此只匹配绑定列集合严格为 `{property}` 的索引。HNSW 仍校验恰好绑定一列，不为多列框架泛化这部分逻辑。

### 5.3 Bulk build

`BulkBuild(const VertexSet&)` 可以保持签名不变，因为扩展已通过 `Rebind` 获得所有列；但契约需要更新为“按相同 vid 读取所有绑定列，生成一条完整索引记录”。

如果希望减少扩展直接访问 `ColumnBase` 的复杂度，可以后续引入框架提供的 row iterator。第一阶段保持 `Rebind + BulkBuild` 可降低改造面。

### 5.4 索引发现接口

`GetIndex` 改为按完整属性集合严格匹配，同时为单属性 DML 提供独立的 contains 接口：

```cpp
// 索引绑定的完整属性集合必须与 property_names 相同，不做 contains 匹配。
result<std::vector<StorageIndex*>> GetIndex(
    label_t label_id,
    const std::vector<std::string>& property_names) const;

struct BoundIndexRef {
  StorageIndex* index;
  size_t column_id;
};

// 用于单属性 update 以及 DROP/RENAME PROPERTY。
result<std::vector<BoundIndexRef>> GetIndexesContainingProperty(
    label_t label_id, std::string_view property_name) const;
```

这里的“严格匹配”是完整集合相同而不是 contains：`GetIndex(label, {a})` 不会命中 `(a, b)`，`GetIndex(label, {a, b})` 也不会命中 `(a, b, c)`。属性参数顺序不影响索引身份，因此 `{a, b}` 和 `{b, a}` 应找到同一个索引；返回索引后，bind、values 和权重仍使用 metadata 中持久化的列顺序。

HNSW create/drop 调用 `GetIndex(label, {property})`，自然只作用于单列索引。单属性 update 需要维护包含该属性的多列索引，因此使用单独命名的 `GetIndexesContainingProperty`，不能把 contains 语义混入 `GetIndex`。

## 6. DML 和事务增量维护

这是改造中最容易遗漏的部分。框架必须保持 graph interface 现有的两种粒度：insert 传完整属性，update 每次只传一个属性；完整索引记录的补齐工作由 `StorageIndex` 的单值 `Upsert` 完成。

### 6.1 Insert

插入一个顶点时，graph interface 已经持有该顶点的全部属性：

1. 收集该标签上的索引，并按 index name 或 pointer 去重；
2. 对每个索引按 metadata 顺序从 insert 输入中提取完整 `IndexValues`；
3. 每个索引调用一次 `Upsert(vid, values)`。

不能对每个插入属性查到索引后立即 upsert。例如 `index_a_b` 同时绑定 `a_column` 和 `b_column` 时，`insert_vertex(a_column, b_column)` 只能产生：

```cpp
index_a_b->Upsert(vid, IndexValues{a_value, b_value});
```

而不能分别因 `a_column`、`b_column` 命中该索引而更新两次。实现上可以按 label 枚举全部索引一次；如果逐属性调用 `GetIndexesContainingProperty` 收集，则必须先放入以 index name/pointer 为 key 的 map 去重，之后再统一构造 values 和 upsert。

主键与普通属性分开存储的现状需要由统一的 value collector 屏蔽，collector 必须能从 insert 输入或 bound columns 中读取主键和普通属性。缺省属性按 vertex schema 的默认值或 `NULL` 规则填入对应位置。

### 6.2 Update/SET/REMOVE

单属性更新时：

1. 通过 `GetIndexesContainingProperty` 找出所有受影响索引；
2. 对每个索引把属性名映射为该索引内的 `column_id`；
3. 调用 `Upsert(vid, IndexValue{column_id, new_value})`；
4. 单值入口从 bound columns 读取其余列，用 `new_value` 覆盖目标位置，再调用完整值入口。

同一语句修改多个属性时无需在 graph interface 层按 `(index, vid)` 合并。现有执行方式可以保持为多个顺序 update：

```text
第一次：Upsert(vid, [col0_new, col1_old, ...])
第二次：Upsert(vid, [col0_new, col1_new, col2_old, ...])
...
```

每次都会产生一条完整且自洽的索引记录，最后一次写入对应最终图状态。代价是同一索引可能被重复更新；后续如果性能测试证明有必要，可以在 batch operator 内做可选合并，但这不是正确性的前置条件。

`REMOVE`/设为 `NULL` 使用单值入口传递目标列的 `NULL`，再由完整值入口统一处理。具体扩展决定部分列为 `NULL` 时如何索引；只有根据该索引的整体 `NULL` 策略判断整条记录不应存在时才调用 `Delete(vid)`，不能沿用“任一输入值为 `NULL` 就删除整条索引记录”的规则。

### 6.3 Delete

顶点删除仍是 row-level 操作，每个索引调用一次 `Delete(vid)`。当前按每个属性枚举索引会让多列索引被重复删除；需要按索引名或指针去重。

### 6.4 Pending mutation、WAL 与 COW

- `PendingIndexMutation` 已能携带多个 `(property, value)`。replay insert 必须以 index 为单位，按 metadata 顺序构造一次完整 `IndexValues`；同一个 `(index, vid)` 只能调用一次 `Upsert(vid, values)`，不能按 mutation 中的每个 property 分别触发同一索引；
- replay update 保持 graph interface 的单属性事件粒度：每个 update 映射为 `IndexValue{column_id, value}` 并顺序调用单值入口；如果一个 mutation 对象包含多个 update property，必须依次处理，不能命中第一个属性后 `break`；
- pending index 激活并完成 `Rebind` 后，单值入口可从当前 graph snapshot 补齐其他列。传入的更新值仍覆盖对应位置，因此不依赖该列在 bound column 中的可见时序；
- COW detach 应以索引为单位，每个 `(index, vid)` 一次；
- WAL redo 后触发索引维护的 `HasPendingIndex(label, property)` 必须使用 contains 语义；
- AP 和 TP 两套更新路径（`graph_interface.cc` 与 `update_transaction.cc`）都要同步修改，避免只修一种事务模式。

## 7. Schema DDL 行为

### DROP PROPERTY

当前行为会删除绑定该属性的索引。多列后建议继续采用原子规则：只要索引引用了被删除属性，就删除整个索引，不自动缩减列集合。自动把 `(name, description)` 改成 `(name)` 会悄悄改变索引语义、权重和查询匹配关系。

### RENAME PROPERTY

更新所有包含旧属性名的索引 metadata，在原位置替换为新名称，并保持类型与 options 一致。若 options 使用列名作为 key（例如 `name_weight`），扩展还必须提供 metadata option rename hook，或将权重改为与列位置对齐的结构化配置，避免改名后遗留无效 option。

### DROP LABEL

仍删除该 label 上的所有索引，不受列数影响，但遍历和 COW bookkeeping 需保证每个索引只处理一次。

## 8. 扩展接口与能力声明

建议为 `StorageIndex` 增加创建期 schema 校验接口：

```cpp
virtual Status ValidateSchema(const IndexBindSchema& schema,
                              const IndexOptions& options) const = 0;
```

也可以在 ModuleFactory 的索引注册描述中声明能力。至少需要表达：

- 最小/最大列数；
- 每个位置允许的数据类型；
- 是否允许主键；
- `NULL` 处理策略；
- 支持的 options 及校验规则。

扩展改造要求：

- FTS：`Rebind` 保存多列，bulk build/upsert 将各列写入同一文档，列顺序与 SQLite FTS5 列顺序一致；BM25 权重转换为相同顺序的位置权重；
- HNSW：适配 vector 形式的新接口，但校验 `columns.size() == 1`，行为保持不变；
- 测试索引和第三方扩展：至少完成单列适配，否则纯虚接口变化会导致编译失败。

## 9. 查询规划与执行接口

框架支持多列后，查询侧不能只用任意一列命中索引。对于 FTS 示例：

```cypher
bm25([n.name, n.description], 'MaxCompute')
```

Binder/optimizer 应提取：

- 同一个变量/标签；
- 有序属性列表 `name, description`；
- 查询参数；
- 可选的显式索引名（若未来支持）。

随后调用 `GetIndex(label, property_names)` 做完整属性集合匹配，并在多个同 schema、同类型索引并存时执行确定性的选择或报歧义错误。物理查询参数只携带搜索词等运行时输入，列权重应来自创建时持久化的 `IndexMeta.options`，不能由查询临时覆盖。

规划缓存失效策略无需改变触发点，但创建、删除、激活、属性重命名导致 metadata/schema 变化时都必须递增 planning generation。

## 10. SHOW_INDEXES 与外部可观测接口

`SHOW_INDEXES` 目前返回单个 `property VARCHAR`。推荐增加 `properties` 列表，同时保留旧列一段兼容期：

| 输出列 | 新语义 |
| --- | --- |
| `property` | 单列索引返回列名；多列索引返回 `NULL`，标记 deprecated |
| `properties` | 有序的 `LIST<VARCHAR>`，所有索引均返回 |
| `options` | 保持 JSON；列权重等配置可见 |

如果 CALL 框架暂不方便返回 list，可先将 `properties` 输出为 JSON array 字符串，但应在接口文档中明确格式，不能使用逗号拼接字符串，因为属性名转义和机器解析不可靠。

Python API 当前主要透传 Cypher，无需新增专用建索引方法；但依赖 `SHOW_INDEXES` 固定六列的测试和用户代码需要迁移说明。

## 11. 错误处理约定

至少提供以下明确错误：

- 属性列表为空；
- 同一属性重复出现；
- 属性不存在或已 soft delete；
- 多列来自不同 label/变量；
- 索引类型不支持该列数；
- 某列类型不受支持；
- metadata 列数与 bind context/value 数量不一致；
- 查询属性集合或顺序与索引不匹配；
- 列权重引用未知列、缺失、非数值或非正数；
- checkpoint 中新 metadata 版本不受当前二进制支持。

错误应在尽可能早的层次返回：语法问题在 parser，catalog 问题在 binder，扩展能力与 options 在创建期，损坏的持久化数据在 open/activate 阶段。

## 12. 测试清单

### Parser/Binder/IR

- 单列和多列 `CREATE INDEX`；
- 空列表、尾逗号、重复列、不存在列；
- 列顺序从 AST 到 physical protobuf 不变；
- 旧 protobuf 字段的读取兼容。

### Metadata/Lifecycle

- 单列旧 JSON 迁移为单元素 `columns`；
- 多列 metadata JSON round trip；
- create、bulk build、dump、close、reopen、clone、rebind；
- 扩展未加载形成 pending index，加载后按正确顺序激活；
- rename/drop property 和 drop label。

### DML/Transaction

- 插入包含全部、部分或全 `NULL` 索引列的顶点；
- 分别更新第一列、最后一列和同一语句更新多列；
- `SET NULL`、`REMOVE`、顶点删除；
- 一个属性同时属于多个单列/多列索引；
- insert 和 delete 对每个多列索引只执行一次；多个单属性 update 顺序执行后，最终索引记录与图的最终状态一致；
- AP update、TP update、WAL replay 和 COW 隔离结果一致；
- checkpoint 前后的增量结果一致。

### 扩展与回归

- FTS 多列建索引、查询和权重排序；
- HNSW 单列行为完全兼容，多列创建返回清晰错误；
- `SHOW_INDEXES` 新旧输出约定；
- 现有单列 FTS/HNSW 测试全部通过。

## 13. 推荐实施顺序

1. 引入 `IndexBindColumn` 和 `IndexBindSchema::columns`，完成旧 metadata 的只读迁移兼容。
2. 修改 grammar、parser、binder 和 protobuf，使多列定义能够到达执行层。
3. 修改 `IndexBindContext`、`StorageIndex::Upsert/AppendImpl`、manager create/rebind/activate 接口；先让所有现有扩展以单元素 vector 编译和通过回归。
4. 重构索引发现接口，明确 contains-property 与 exact-schema 两种语义。
5. 引入统一 value collector 和两种 `Upsert` 入口，改造 AP/TP 插入、单属性更新、删除、pending replay 与 COW 处理。
6. 改造 schema DDL、checkpoint、`SHOW_INDEXES` 和规划缓存失效。
7. 在 FTS 中实现真正的多列存储、查询与权重；HNSW 保持单列能力声明。
8. 补齐端到端、恢复和兼容性测试。

这个顺序将“框架能够表达多列”和“某个扩展真正使用多列”拆开。每一步都能保持单列索引可构建、可恢复、可增量维护，降低一次性修改整个索引链路的风险。

## 14. 完成标准

- 通用 metadata、bind、write、lookup 和 lifecycle 接口不再包含隐式单列假设；
- 单列索引的 Cypher、checkpoint 和运行行为保持兼容；
- 每个索引扩展能显式拒绝或支持多列，而不是依赖框架中的类型名称分支；
- insert 使用完整值入口；任一单属性 update/remove 都能通过 bound columns 补齐其他值，并委托给统一的完整值实现；
- checkpoint/reopen/pending activation 后列顺序、类型、options 和查询结果不变；
- `SHOW_INDEXES` 能无歧义展示索引绑定的全部有序属性。
