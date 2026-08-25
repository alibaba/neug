# TP 模式支持索引 DDL 与索引激活

## 1. 背景与目标

当前 `StorageIndexDDLInterface` 只由 `StorageAPUpdateInterface` 实现。因此，执行层虽然已经通过统一的 `StorageIndexDDLInterface` 调用 `CREATE INDEX`、`DROP INDEX` 和扩展加载后的索引激活，但 TP 查询拿到的 `StorageTPUpdateInterface` 无法完成这些操作：

- `CREATE INDEX`、`DROP INDEX` 在 TP 模式下会被执行算子拒绝；
- TP 模式执行 `LOAD <extension>` 后会跳过 pending index 激活；
- Update Transaction 的 WAL 中没有索引 DDL 记录，崩溃恢复无法重放这三类状态变化。

本需求的目标是让以下操作在 AP 和 TP 两条链路中具有一致的能力和用户可见语义：

1. `CREATE INDEX`；
2. `DROP INDEX`；
3. `LOAD <extension>` 后调用 `ActivateIndexes()` 激活可用的 pending indexes；
4. 上述 TP 操作通过 WAL 在异常退出后恢复。

本方案不修改 Cypher 语法、Binder、Planner 和物理计划格式；现有执行算子已经面向 `StorageIndexDDLInterface`，TP storage 实现该接口后即可复用现有计划。

## 2. 现状与关键约束

### 2.1 当前调用链

`CREATE/DROP INDEX` 的主要调用链如下：

```text
Cypher
  -> Binder / Planner / physical plan
  -> CreateIndexOpr / DropIndexOpr
  -> dynamic_cast<StorageIndexDDLInterface*>
  -> StorageAPUpdateInterface
  -> StorageIndexManager
```

TP 写事务的调用链为：

```text
ExecutionSlot::GetUpdateTransaction()
  -> PropertyGraph::Clone()
  -> UpdateTransaction
  -> StorageTPUpdateInterface
  -> 修改 COW graph + WalBuilder
  -> UpdateTransaction::Commit()
  -> PrepareSnapshot -> append WAL -> Publish snapshot
```

`LOAD <extension>` 是 `ExecutionSlot` 中单独处理的 admin request。AP 模式目前使用 `InPlaceWriteScope + StorageAPUpdateInterface::ActivateIndexes()`；TP 模式仅打印 warning 并跳过激活。

### 2.2 COW 与索引对象

`PropertyGraph::Clone()` 已经 clone `StorageIndexManager` 及其中的索引，并将 clone 后的索引重新绑定到 COW graph 的列。因此索引 DDL 可以只修改 COW graph：事务 abort 时直接丢弃 clone，commit 时随 snapshot 一起发布。

列转换也满足该隔离条件：`VertexTable::Clone()` 创建独立的 `Table`，`Table::Clone()` 为每列创建独立的 Column wrapper，`StorageIndexManager::Clone()` 创建独立的 manager 和 index wrapper；`Table::SetColumn()` 只替换 COW Table 中的 `unique_ptr`。Create HNSW 时生成的 VecColumn 可以复用原 ArrayColumn 的底层向量 buffer，但转换和 bulk build 只读取该共享 buffer，不修改旧 snapshot 的列结构；Drop HNSW 时 `FromVecColumn()` 创建并填充新的 ArrayColumn buffer。因此，即使 helper 中途已经完成部分转换或索引构造，只要事务未发布，变化仍只存在于 `cow_graph_`。

`UpdateTransaction::Abort()` 会清空 transaction-local WAL buffer、释放 timestamp lease，并释放 `view_`、`cow_graph_` 和 checkpoint 引用；它不会调用 `PrepareSnapshot/Publish`。其他读事务始终持有旧 snapshot，abort 后的新查询也继续从 SnapshotStore 取得旧 snapshot，所以当前事务的全部索引、column wrapper、列替换和 dirty 标记都会随 COW graph 丢弃，不影响后续查询。该结论需要用 Create/Drop 列转换后的 abort 测试固定下来。

本需求中的三个操作不调用 `StorageTPUpdateInterface::detachIndex()`：

- Create 时目标索引尚不存在；
- Drop 时索引会从 clone 的 manager 中直接删除；
- Activate 时 pending metadata 被转换为新的 index 实例，不更新已有 active index 的内部数据。

这里的“不 detach”仅指不调用索引数据的 `StorageIndex::Detach()`。HNSW Create/Drop 伴随的 `ArrayColumn <-> VecColumn` 转换、vertex table dirty 标记和 `GraphView::Rebuild()` 仍然必须执行。

### 2.3 提交和恢复顺序

Update Transaction 当前采用：先在 COW graph 上执行操作，commit 时先准备 snapshot，再写 WAL，最后以不可失败的步骤发布 snapshot。新增索引操作沿用这一原子性边界：

- 操作失败：上层使事务 abort，COW graph 和未提交 WAL buffer 一并丢弃；
- WAL append 失败：不发布 snapshot；
- WAL append 成功：发布已经准备好的、包含索引变化的 snapshot；
- 进程在 WAL append 后、snapshot 发布或 checkpoint 前退出：reopen 通过 WAL 重建索引变化。

## 3. 总体设计

### 3.1 接口层

修改 `StorageTPUpdateInterface`，同时继承：

```cpp
class StorageTPUpdateInterface : public StorageUpdateInterface,
                                 public StorageIndexDDLInterface
```

并实现：

```cpp
result<StorageIndex*> CreateIndex(std::unique_ptr<IndexMeta> meta) override;
Status DropIndex(const std::string& name) override;
Status ActivateIndexes() override;
```

同时更新 `StorageIndexDDLInterface` 的注释，删除“仅 AP 支持”的描述。Create/Drop 执行算子仍保留 dynamic cast 作为能力检查，但错误信息改为与模式无关的 “current storage interface does not support index DDL”，避免继续向用户暴露已经不存在的 AP 限制。

### 3.2 复用 AP/TP 的索引 DDL 逻辑

AP 现有实现不只是简单调用 `StorageIndexManager`，还包含 HNSW 列转换和 plan invalidation。为防止两套逻辑后续漂移，把与事务机制无关的现有 AP 实现原样抽成 storage graph 层的共享 helper，由 AP 和 TP wrapper 分别负责各自的 WAL、dirty/COW 和 planning callback。

共享 helper 的实现以当前 `StorageAPUpdateInterface::{CreateIndex,DropIndex,ActivateIndexes}` 为唯一行为基线：采用移动代码、补充必要上下文参数的方式提取，不重新设计或改写判断顺序，不改变校验条件、错误码、HNSW 转换时机、manager 调用顺序、GraphView 重建条件和 callback 条件。提取前后的 AP 测试结果必须完全一致，并增加针对错误码和列转换的 characterization tests，防止 helper 与原实现产生偏差。

共享逻辑应覆盖：

- Create 参数、label、property、主键和列类型校验；
- 普通索引与 HNSW 索引共存约束；
- HNSW 创建前 `ArrayColumn -> VecColumn`；
- `IndexIDAccessor` 创建、index bind 和 bulk build；
- Drop pending/active index 元数据解析；
- 删除最后一个 HNSW 后 `VecColumn -> ArrayColumn`；
- `StorageIndexManager::{CreateIndex,DropIndex}`；
- Activate pending index、重放 pending mutations；
- 必要时重建 `GraphView`。

helper 不负责写 WAL，也不改变当前 planning-change 机制。它接受现有实现所需的 graph、view 和 callback/context，使 AP 继续按当前方式通知 planning change，TP 则沿用 Update Transaction 当前已有的 WAL DDL 标志行为。

### 3.3 TP CreateIndex

执行顺序：

1. 校验 `meta` 是否为空、redo 是否可序列化，并只读检查 active/pending index 中是否已有同名索引；重复名称直接返回 `ERR_ILLEGAL_OPERATION`，不写 WAL；
2. 在调用任何可能修改 column、index manager 或 dirty 状态的 helper 之前，将完整 `IndexMeta` 写入 transaction-local WAL buffer；
3. 在 COW graph 上执行共享 CreateIndex 逻辑，包括其现有的全部校验、HNSW 列转换和 bulk build；
4. 若发生列替换，标记对应 vertex table dirty 并重建 transaction 的 `mut_view_`；
5. 标记本事务 planning state changed；
6. 返回新索引指针。该指针只在当前事务/COW snapshot 生命周期内有效，执行算子不应跨事务保存。

WAL buffer 写入必须发生在实际 mutation 之前，与现有 Update Transaction DDL 的模式一致。它只写内存 buffer，不是在此处将 WAL 持久化；持久化仍由 `Commit()` 在 snapshot prepare 之后完成。helper 内原有 duplicate 校验必须保留，TP wrapper 的只读 preflight 是额外保护，不改变 helper 行为。

该 preflight 是必需的：`IF NOT EXISTS` 会在执行算子中把 `ERR_ILLEGAL_OPERATION` 转成成功。如果先 Log、再由 helper 报 duplicate，事务可能正常 commit 一条无效 Create redo，replay 时反而失败。preflight 确保 no-op 分支没有 WAL；真正进入 helper 后若发生其他错误，查询必须 abort 整个 update transaction，buffer 中的 redo 随之丢弃。

### 3.4 TP DropIndex

执行顺序：

1. 只读检查 pending 和 active indexes 中是否存在目标；不存在时直接返回 `ERR_NOT_FOUND`，不写 WAL；
2. 在 WAL buffer 写入 index name；
3. 在此之后才调用共享 DropIndex helper；helper 仍按原 AP 顺序重新查找目标并取得 `IndexMeta`，原校验不得删除；
4. 在 COW graph 上继续执行原有 DropIndex 逻辑；
5. 如果删除最后一个 HNSW index，完成 `VecColumn -> ArrayColumn`、dirty 标记和 `mut_view_` 重建；
6. 标记 planning state changed。

`IF EXISTS` 继续由执行算子按 `ERR_NOT_FOUND` 处理。与 Create 相同，preflight 防止 no-op 的 `IF EXISTS` 请求提交无效 Drop redo。Drop 真正执行前已经写入 WAL buffer，并直接删除 COW manager 中的索引，不调用 `detachIndex()`。

### 3.5 TP ActivateIndexes

执行顺序：

1. 向 WAL buffer 写入无 payload 的 ActivateIndexes redo；
2. 调用 COW graph 的 `ActivateIndexes()`；
3. 直接返回该调用的错误状态；
4. 返回成功且 activated count 大于 0 时，按现有实现重建 `mut_view_`；planning-change 标志由 `LogActivateIndexes()` 按现有 Update Transaction DDL 机制无条件设置。

即使本次没有可激活的索引，也保留 ActivateIndexes WAL。原因是 WAL 表达的是 “extension 已加载后尝试激活” 这一有顺序的恢复事件；恢复时扩展可能尚不可用，而 `StorageIndexManager::ActivateIndexes()` 本身会跳过 factory 中仍不可创建的 pending module，并成功返回实际激活数量。恢复层不应把“仍有 pending index”视为 replay 失败。

如果 `ActivateIndexes()` 返回真实错误（例如 module 类型错误、索引绑定列非法或 pending mutation replay 失败），本次 update transaction abort，错误返回给 `LOAD` 调用方。

## 4. WAL 设计

### 4.1 OpType

在现有 `OpType` 尾部追加，禁止插入或重排已有编号，以保持旧 WAL 的二进制兼容：

```cpp
kCreateIndex = 16,
kDropIndex = 17,
kActivateIndexes = 18,
```

建议统一使用复数 `ActivateIndexes`，与现有 public API 一致。若实现代码沿用需求中的 `ActivateIndex` 命名，也必须保证 WalBuilder、redo struct 和 replay switch 命名一致。

### 4.2 Redo payload

| OpType | Payload | 说明 |
| --- | --- | --- |
| `kCreateIndex` | `IndexMeta` 的稳定序列化结果 | 包含 name、type、label id、property name/type 和 options |
| `kDropIndex` | `std::string name` | replay 按唯一名称删除 active 或 pending index |
| `kActivateIndexes` | 无 | 调用一次 graph `ActivateIndexes()` |

Create redo 建议复用 `IndexMeta::ToJsonString()/FromJsonString()`，WAL 中保存一个 length-prefixed string，避免在 WAL 层复制 `IndexMeta` 每个字段的编码规则。恢复所基于的 checkpoint 与 WAL 前序 schema redo 保留相同 label-id 演进顺序，因此可使用 metadata 中的 label id。解析失败应作为损坏 WAL/恢复失败处理，不能静默跳过。

在 `wal.h/.cc` 中增加：

```cpp
struct CreateIndexRedo { ... };
struct DropIndexRedo { ... };
struct ActivateIndexesRedo { ... };
```

在 `WalBuilder` 中增加 `LogCreateIndex`、`LogDropIndex`、`LogActivateIndexes`。三者都增加 `op_num_`，并与当前其他 DDL Log API 一样设置现有的 `schema_changed_` 标志。这里不重命名、不拆分也不修改当前 planning-change 功能。

### 4.3 Planning generation

不修改目前的 planning-change 功能。`WalBuilder::schema_changed_` 继续作为 Update Transaction 提交时增加 planning generation 的既有标志，不重命名、不新增 `MarkPlanningChanged()`，也不改变 `UpdateTransaction::Commit()` 的计算方式。

`LogCreateIndex()`、`LogDropIndex()`、`LogActivateIndexes()` 均无条件设置 `schema_changed_ = true`，行为与当前 DDL Log API 保持一致。这样即使 Activate 最终激活数量为 0，本次提交也会增加一次 planning generation 并使 PipelineCache 失效。

无条件设置在正确性上没有问题，代价仅是无 pending index 或扩展仍不可用时会发生一次不必要的 generation 增长和计划重新编译。采用无条件设置的理由是：

- WAL 必须在操作前写 buffer，此时还不知道实际 activated count；
- 不引入新的状态接口，不修改现有 planning-change 机制；
- 保守失效不会产生旧计划引用错误，比漏掉索引状态变化更安全；
- `LOAD` 属于低频 admin 操作，该额外开销可接受。

### 4.4 WAL replay

在 `UpdateTransaction::IngestWal()` 的 switch 中增加三个分支，并复用与正常执行相同的 graph-level 索引 DDL helper：

- Create：反序列化 `IndexMeta`，执行 CreateIndex；
- Drop：反序列化 name，执行 DropIndex；
- Activate：执行 ActivateIndexes，接受成功返回的任意 activated count（包括 0）。

Replay 必须严格按 WAL 中的原始顺序执行。例如：

```text
CREATE INDEX -> data mutation -> DROP INDEX
```

不能先集中恢复 schema/index，再恢复 DML，否则会改变 index bulk build 和增量维护的结果。

replay 中 Create/Drop 的非 OK 状态视为恢复失败，沿用现有 redo 的 `THROW_STORAGE_EXCEPTION_STATUS` 风格。Activate 只有返回 error 才使恢复失败；返回 0 表示依赖 extension 当前不可用，pending indexes 保持 pending，后续再次 `LOAD` 时可继续激活。

恢复过程中不需要写二次 WAL，也不调用 `detachIndex()`。Replay 针对尚未发布给读者的恢复 graph 直接执行，并正确设置 index catalog/vertex table dirty 状态，以便可选的 recovery checkpoint 能完整持久化结果。

## 5. ExecutionSlot 中的 LOAD EXTENSION

保留现有 `ExtensionManager::LoadExtension()` 在事务外先加载动态库的顺序；只有成功注册 module 后才进入索引激活。

AP 分支保持现状。TP 分支替换 warning/skip：

```text
LoadExtension(name)
  -> GetUpdateTransaction()
  -> StorageTPUpdateInterface storage(txn)
  -> storage.ActivateIndexes()
  -> txn.Commit()
```

具体错误处理：

- `LoadExtension` 失败：不创建 UpdateTransaction；
- `ActivateIndexes` 失败：abort transaction，返回原始 status；
- commit/WAL append 失败：返回 query execution/internal error，不能报告 LOAD 成功；
- 成功但激活数为 0：仍提交 ActivateIndexes redo，LOAD 成功；
- 成功激活索引：发布新 snapshot 和新 planning generation，后续查询可规划出 IndexScan。

这里应使用当前 execution slot 已绑定的 WAL writer、allocator、VersionManager 和 SnapshotStore，不创建临时 writer，也不走 AP 的 `InPlaceWriteScope`。

执行管线中的 `ExtensionLoadOpr` 已经通过 `StorageIndexDDLInterface` 调用激活；TP interface 完成后该路径自然生效。需要同步移除其中“current storage 不支持时跳过”的 TP 特例语义：对确实不实现接口的 storage 可以继续保留通用 warning，AP/TP 都不应进入该分支。

## 6. 一致性、错误和边界语义

### 6.1 AP/TP 一致性

两种模式必须保持以下行为一致：

- 相同输入得到相同的校验错误码；
- `IF NOT EXISTS` 和 `IF EXISTS` 行为一致；
- HNSW 与普通索引的共存限制一致；
- HNSW 列转换一致；
- active/pending index 均可 Drop；
- extension 加载后尝试激活全部当前可激活的 pending indexes；
- Create/Drop/Activate redo 提交后查询计划缓存失效，包括实际激活数为 0 的情况。

差异仅在持久化机制：AP 使用原地写作用域和 checkpoint 管理，TP 使用 COW snapshot + WAL commit。

### 6.2 事务可见性

- 未 commit 的索引变化只存在于 COW graph，其他 reader 继续读取旧 snapshot；
- commit 后 snapshot、planning generation 和 read visibility 一起发布；
- abort 不改变当前 graph；
- update admission 规则保证同一时间只有允许的 update writer，不额外增加 index manager 锁。

### 6.3 扩展依赖

Activate replay 不要求所有 pending index 都成功激活。未注册 module 的索引继续保留 pending，且 pending mutations 不能丢失。后续显式 `LOAD vector_search` 会产生新的 Activate redo 并再次尝试。

CreateIndex replay 仍执行与正常 Create 相同的 module 创建规则；如果创建该 index 所需模块在恢复环境中不可用，恢复应明确失败，而不是构造一个缺少 bulk-build 语义的伪 pending index。测试环境必须保证创建 HNSW 所需的 `vector_search` module 在创建 redo 被 replay 前已经注册，或使用项目正式的扩展恢复/预加载机制。

### 6.4 checkpoint

索引 catalog 或 HNSW backing column 变化必须使 graph 的 dirty 判断为 true。正常 checkpoint 应持久化：

- index manager catalog；
- active index data 或 pending metadata；
- Array/Vec 列转换结果；
- pending mutations（受现有“缺依赖时不可 checkpoint”约束）。

本需求不改变 checkpoint 文件格式。

## 7. 修改范围

预计涉及以下文件（最终可根据 helper 落点微调）：

| 文件 | 修改内容 |
| --- | --- |
| `include/neug/storages/graph/graph_interface.h` | TP 实现 index DDL interface；更新 AP-only 注释；声明共享 helper（若采用） |
| `src/storages/graph/graph_interface.cc` | 抽取/复用 Create、Drop、Activate 的 graph-level 逻辑 |
| `include/neug/transaction/update_transaction.h` | `StorageTPUpdateInterface` 继承接口并声明三个实现 |
| `src/transaction/update_transaction.cc` | TP 三个操作；新增 replay 分支 |
| `include/neug/transaction/transaction_utils.h` | 追加三个 `OpType` |
| `include/neug/transaction/wal/wal.h`、`src/transaction/wal/wal.cc` | redo 定义与序列化 |
| `include/neug/transaction/wal/wal_builder.h`、`src/transaction/wal/wal_builder.cc` | 三个 Log API；planning changed 状态 |
| `src/main/execution_slot.cc` | TP LOAD EXTENSION 使用 UpdateTransaction 激活并提交 |
| `src/execution/execute/ops/ddl/create_index.cc`、`drop_index.cc` | 去除 AP-only 错误文案 |
| `tests/storage/test_tp_index.cc` | interface、Create/Drop、COW 和错误语义单测 |
| `tools/python_bind/tests/test_hnsw_index_tp.py` 或同层新测试 | TP 服务端 HNSW DDL 与 LOAD 激活 E2E |
| `tests/transaction/test_update_transaction.cc`、`test_wal_replay.cc` | redo 编解码、事务 replay、异常退出 reopen 恢复 |
| `doc/source/extensions/vector_search.md` 等用户文档 | 统一 AP/TP 支持说明，移除限制 |

## 8. 测试方案

### 8.1 Update Transaction 单元测试

在 `tests/storage/test_tp_index.cc` 增加或修改：

1. `StorageTPUpdateInterface` 可以 dynamic cast 为 `StorageIndexDDLInterface`；
2. TP 空表创建普通测试索引成功，commit 后可查到；
3. TP 对已有数据创建索引，验证 bulk build 后查询结果；
4. TP 创建 HNSW，验证 ArrayColumn 转为 VecColumn、索引可搜索；
5. TP 删除 HNSW，验证最后一个 HNSW 删除后转回 ArrayColumn；
6. 同列仍有另一个 HNSW 时 Drop 不转换列；
7. pending index Drop；
8. duplicate Create、missing Drop 及 IF EXISTS/IF NOT EXISTS 对应错误语义；
9. Create/Drop 后 abort，当前 snapshot、列类型和 index catalog 均不变化；
10. Create/Drop/Activate commit 后 planning generation 增加，包括无索引可激活的情况；
11. 确认 Create/Drop/Activate 路径没有调用 `StorageIndex::Detach()`。

### 8.2 TP 服务 E2E

扩展现有 `test_hnsw_index_tp.py`，不要再在 AP 阶段预建索引：

1. 建库并写入带向量的数据，启动 TP service；
2. 在 TP session 执行 `LOAD vector_search`；
3. 在 TP session 执行 `CREATE INDEX ... USING HNSW`；
4. PROFILE 查询确认使用 `IndexScanOpr` 且结果正确；
5. 执行 TP insert/update/delete，验证索引增量维护；
6. 在 TP session 执行 `DROP INDEX`，确认 catalog 和查询规划更新；
7. 覆盖 IF EXISTS/IF NOT EXISTS。

Activate 场景单独覆盖：先持久化一个 extension 不可用时加载为 pending 的 HNSW index 和索引数据，再以 TP service 打开，在 TP 模式执行 `LOAD vector_search`，验证：

- pending index 转为 active；
- pending mutations 被重放；
- PROFILE 使用 IndexScan；
- 返回结果包含激活前已有的数据。

### 8.3 WAL builder/replay 单元测试

在 `test_update_transaction.cc` 验证：

- 三种 redo 的 op type 和 payload 可往返序列化；
- 一个 WAL 中混合 schema、CreateIndex、DML、DropIndex 时按顺序 replay；
- Activate 在扩展可用时成功激活；
- Activate 在扩展不可用时返回成功、索引仍为 pending；
- 非法 Create/Drop redo 返回明确恢复错误；
- 旧 OpType 数值未改变。

### 8.4 异常退出后的 db reopen

该测试必须构造“WAL 已落盘，但最新 graph 未 checkpoint”的真实场景：

1. 先正常创建并关闭基础 checkpoint；
2. 在子进程打开 DB，设置 `checkpoint_on_close=false`；
3. 通过 TP service 依次执行 CreateIndex/DropIndex/ActivateIndexes 所需场景并确保事务 commit 返回成功，即 WAL append 已完成；
4. 使用 `_exit()`/`std::_Exit()` 或现有 death-test 子进程模式直接结束进程，不调用 `db.Close()`，不运行析构清理；
5. 父进程重新打开同一 DB，设置 `checkpoint_on_recovery=false` 以确保验证的是 WAL replay 结果，而不是新 checkpoint 的副作用；
6. 验证 active/pending catalog、列类型、索引查询结果和 planning generation；
7. 再执行一次正常 checkpoint/reopen，验证恢复结果可继续持久化。

需要分别保留并验证三类记录，至少包含：

- Create-only WAL：reopen 后索引存在且数据完整；
- Create + Drop WAL：reopen 后索引不存在，HNSW backing column 状态正确；
- pending index + Activate WAL：依赖可用时激活；依赖仍不可用时 replay 成功且保持 pending。

不能通过正常 `db.Close()` 构造该测试，因为 close 可能按配置生成 checkpoint，或者关闭/轮转 WAL，使测试不再覆盖“仅依赖 WAL 恢复”的目标路径。

## 9. 文档更新

实现完成后检查全部用户文档，删除 Create/Drop Index 或 extension index activation “仅 AP 支持”的说明。当前代码内明确存在 AP-only 文案；用户文档中未检索到同样的明确限制，但仍需在索引主文档和 vector search 文档中统一补充：

> `CREATE INDEX`、`DROP INDEX` 以及 `LOAD` 扩展后对 pending index 的激活，在 Embedded/AP 与 Service/TP 模式下均受支持，语法和行为没有区别。

同时更新 TP HNSW 测试注释中“Build in AP mode”的旧工作流，示例优先展示索引可以直接在 TP service 中创建和删除。

## 10. 验收标准

- AP 现有索引测试全部通过且行为无变化；
- TP storage 实现 `StorageIndexDDLInterface`；
- TP `CREATE INDEX`、`DROP INDEX`、`LOAD vector_search` 激活均成功；
- HNSW 列转换、增量维护、pending mutation 和查询结果正确；
- Create/Drop/Activate 会使旧查询计划失效，包括 Activate 实际激活数为 0 的情况；
- abort 不泄漏索引或列转换变化；
- 三种 WAL 可在未 checkpoint 的异常退出后正确 replay；
- Activate replay 在扩展依赖不可用时不会误报恢复失败；
- 旧 WAL 仍可读取；
- 文档和错误信息不再声称索引 DDL 仅支持 AP 模式。
