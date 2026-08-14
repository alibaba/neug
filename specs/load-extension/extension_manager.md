重构 Load/Install/Uninstall Extension 的实现

目前在 `src/execution/execute/ops/admin/extension.cc` 中通过 pipeline 算子的方式支持了 Extension 相关操作，有下面几个问题：
- 已经加载的 extension 维护在进程中，通过静态变量保存，这个不太合理，我们现在需要维护在 NeuG Database 中，比如创建 ExtensionManager 中维护 loaded_extension
- 通过 pipeline 方式无法拿到 NeuGDB 中的 ExtensionManager，这需要我们参考 Checkpoint 实现方式，在 ExecutionSlot 中单独为 extension/checkpoint 实现

现在大概方案如下：
- 在 NeuGDB 中维护 ExtensionManager 对象
- ExtensionManager 提供 LoadExtension/InstallExtension/UninstallExtension 接口
- 内部维护 loaded_extensions 状态，保存当前 database 中已经加载的 extension
- 在执行 LoadExtension 过程中，对于重复加载的 extension 记录 warning、跳过动态库初始化并正常返回，
  让调用链继续重试 ActivateIndexes
- 目前通过 CAS 方式保证 LoadExtension 原子性，但这个方案比较难理解，先用直接加锁的方式

在 ExecutionSlot 中，Extension/Checkpoint 统一识别为 Admin 算子，对于这类操作，统一抽象一个 executeAdmin 接口执行：
- 在 QueryAnalysis 中识别 Load/Install/Uninstall Extension 操作
- Checkpoint/Load/Install/Uninstall 统一识别的 admin，并且通过统一的 executeAdmin 接口执行
- 对于 admin 类型，统一走 executeAdmin 执行接口
- 在 executeAdmin 内部分叉，分别执行 checkpoint/load extension/install extension/uninstall extension
- 特别需要注意的是 load extension 需要访问图接口，在执行完成 ExtensionManager::LoadExtension 之后，还需要调用 ActivateIndexes，因为会涉及到图数据修改，
需要 transaction 的保护，需要在 AP/TP 模式下创建具体的 UpdateInterface，保证数据修改的一致性 (commit/abort 等)

## 设计目标

1. Extension 的加载状态属于一个 `NeugDB` 实例，不再由进程级静态变量维护。
2. `CHECKPOINT`、`INSTALL EXTENSION`、`LOAD EXTENSION`、`UNINSTALL EXTENSION`
   都作为 Admin statement 由 `ExecutionSlot` 旁路执行，不进入普通 pipeline。
3. 同一个 database 上的 Extension 管理操作串行化；重复 `LOAD` 记录 warning、跳过重复初始化并正常返回。
4. `LOAD` 注册 extension 后，必须在同一个 admin 调用中激活 pending indexes；图数据修改遵循
   AP/TP 各自的 update transaction 提交/回滚语义。
5. 动态库一旦成功加载，在 database 关闭前不执行 `dlclose`。这是因为 extension 的函数指针、
   catalog/VFS/module registry 等对象可能仍引用该动态库代码。

## 核心对象

### AdminRequest

`QueryAnalysis` 除了判断语句类别，还应携带执行 Admin 所需的完整参数，避免
`ExecutionSlot` 再次解析 SQL，也避免为了取 extension name 先编译 physical plan。

```cpp
enum class AdminType : uint8_t {
  kCheckpoint,
  kInstallExtension,
  kLoadExtension,
  kUninstallExtension,
};

struct ExtensionAdminInfo {
  extension::ExtensionAction action;
  std::string name;
  std::string repository; // 仅 INSTALL 使用；未指定时为空
};

struct AdminRequest {
  AdminType type;
  std::optional<ExtensionAdminInfo> extension;
};

enum class QueryKind { kRegular, kAdmin };

struct QueryAnalysis {
  AccessMode access_mode = AccessMode::kRead;
  ExplainMode explain_mode = ExplainMode::kNone;
  QueryKind kind = QueryKind::kRegular;
  std::optional<AdminRequest> admin;

  bool isAdmin() const { return kind == QueryKind::kAdmin; }
};
```

`analyzeQuery()` 必须只从输入 string 静态解析 `AdminRequest`，不能调用 parser/binder，这是当前执行链的
约束。实现上扩展现有 `analyzeQueryPrefix()` 的轻量扫描器，至少正确处理：

- 可选的 `EXPLAIN`、`EXPLAIN LOGICAL`、`PROFILE` 前缀；
- `CHECKPOINT`；
- `INSTALL EXTENSION <name> [FROM <repository>]`；
- `LOAD EXTENSION <name>`；
- `UNINSTALL EXTENSION <name>`；
- 大小写、空白、末尾分号，以及语法允许的 quoted identifier/string；
- 不完整、存在多余 token 或引号未闭合时不构造 AdminRequest，交给后续正常编译路径报告语法错误。

该扫描器只提取 Admin 执行所需字段，不承担完整 Cypher 语法校验。为避免 SQL 文本生命周期问题，
`AdminRequest` 必须拥有解析出的 `name/repository` 字符串，不能保存 `string_view`。

### ExtensionManager

```cpp
class ExtensionManager {
 public:
  using InitFunc = void (*)();

  struct LoadedExtension {
    std::string name;       // normalize 后的名称
    std::string library_path;
    void* handle = nullptr;
    InitFunc init = nullptr;
  };

  ExtensionManager() = default;
  ~ExtensionManager(); // database 关闭时最后释放 handle；见析构顺序要求

  Status InstallExtension(const InstallExtensionAuxInfo& info);
  Status LoadExtension(const std::string& name);
  Status UninstallExtension(const std::string& name);

  bool IsLoaded(const std::string& name) const;
  std::vector<std::string> ListLoadedExtensions() const;
 private:
  static std::string NormalizeExtensionName(std::string name);

  // 对 install/load/uninstall 以及 loaded_extensions_ 的读写统一加锁。
  // 初版刻意采用粗粒度锁，让 dlopen + Init 的状态变化保持简单、可审计。
  mutable std::mutex mutex_;
  std::unordered_map<std::string, LoadedExtension> loaded_extensions_;
};
```

实现规则：

- `LoadExtension` 持有 `mutex_` 完成 normalize、重复检查、路径解析、`dlopen`、`dlsym`、`Init` 和
  map 插入。只有全部成功才写入 `loaded_extensions_`。
- `dlopen` 或 `Init` 失败时关闭本次打开的 handle，map 保持不变，后续允许重试。
- 名称比较统一大小写不敏感；map key、安装目录和错误消息中的 canonical name 使用 normalize 后结果。
- 已加载时记录 warning（例如 `Extension 'parquet' is already loaded; skip library initialization`），
  跳过 `dlopen/dlsym/Init` 并正常返回。ExecutionSlot 随后仍然执行 `ActivateIndexes()`，使上一次
  activation 失败后可以通过再次 `LOAD` 重试。
- `UninstallExtension` 保持当前行为，不增加 loaded 状态校验：只校验安装目录是否存在并执行
  `remove_all`。即使当前 database 已加载该 extension 也允许删除磁盘文件；已打开的动态库 handle
  和 loaded 状态保持到 database 生命周期结束。
- `InstallExtension` 和 `UninstallExtension` 的下载/文件操作也放进 manager，使所有状态检查只有一个入口；
  HTTP、checksum、路径等细节仍可留在无状态的 `ExtensionUtils` 中。
- 删除 `AcquireLoad/CompleteLoad/FailLoad/ReplayLoadedExtensions` 以及静态
  `loaded_extensions_`。`ReplayLoadedExtensions` 的需求应由“每 DB 独立初始化”取代，不能把一个 DB
  加载的 extension 隐式注册到另一个 DB。

> 注意：当前 extension 的 `Init()` 是无参数进程级注册接口。仅把 loaded map 移入 DB 并不能让
> catalog/VFS/module 注册天然隔离。建议后续把 ABI 演进为 `Init(ExtensionContext&)`，context 显式持有
> 当前 DB 的 catalog、VFS 和 module registry。本次重构至少先消除加载判重状态的进程级静态变量。

### 所有权与注入

`NeugDB` 新增：

```cpp
std::unique_ptr<extension::ExtensionManager> extension_manager_;
```

在 `Open()` 初始化 query runtime 之前创建，在所有 `ExecutionSlot`、planner metadata 和 graph snapshot
销毁之后再销毁。`ExecutionSlot` 构造函数新增
`extension::ExtensionManager& extension_manager`，仅保存引用。

代码核对结果：`MetadataManager::extensionManager` 并非零引用。目前
`ClientContext::getExtensionOption()`、`ClientContext::getExtensionManager()` 会访问它，`clone()` 也会共享它；
其中 `getExtensionOption()` 被 standalone call binder 使用。不过当前仓库没有发现
`ExtensionManager::extensionOptions` 的注册写入口，`ClientContext::getExtensionManager()` 也没有调用方，
这部分属于历史耦合，不能继续作为 DB runtime ExtensionManager 的所有权依据。

改造方式：

1. 删除无调用方的 `ClientContext::getExtensionManager()`。
2. 确认 extension option 功能确实未使用后，删除 `ClientContext::getExtensionOption()`、相关 option fallback、
   `ExtensionManager::getExtensionOption()` 和 `extensionOptions`；若仍需兼容，则把 option registry 独立为
   MetadataManager 的普通 metadata 字段，不能因此保留 runtime ExtensionManager。
3. 从 `MetadataManager` 的构造函数、clone 和成员中删除 `extensionManager`。
4. Runtime `ExtensionManager` 仅由 `NeugDB` 持有并直接注入 `ExecutionSlot`，不注入 planner metadata。

## ExecutionSlot Admin 执行模型

```cpp
Status ExecutionSlot::executeAdmin(const AdminRequest& request,
                                   ExplainMode explain_mode,
                                   QueryResponse& response);

Status ExecutionSlot::executeLoadExtension(
    const ExtensionAdminInfo& info, QueryResponse& response);
```

`executeCore()` 的主分支调整为：

```text
analyzeQuery
  -> validateAdminRequest / infer access mode
  -> EXPLAIN: 编译并返回说明，不产生副作用
  -> Admin: executeAdmin
  -> Regular: 现有 prepareQuery + pipeline + transaction
```

Admin 不需要 `prepareQuery()`，也不进入 query cache；否则 Extension 注册后 catalog 变化容易与缓存计划
产生不一致。Admin 成功后清理/失效 global 和 slot-local plan cache（至少 `LOAD` 必须失效）。

`executeAdmin` 分派：

```cpp
switch (request.type) {
case AdminType::kCheckpoint:
  return executeCheckpoint(...);
case AdminType::kInstallExtension:
  return extension_manager_.InstallExtension(...);
case AdminType::kLoadExtension:
  return executeLoadExtension(...);
case AdminType::kUninstallExtension:
  return extension_manager_.UninstallExtension(...);
}
```

访问模式建议：

| 操作 | access mode | 是否需要图事务 |
|---|---|---|
| CHECKPOINT | update | 由 CheckpointCoordinator 管理 |
| INSTALL | update/admin | 否，但只读 DB 禁止磁盘修改 |
| LOAD | update | 是，激活 pending indexes |
| UNINSTALL | update/admin | 否；不检查 loaded 状态 |

## LOAD 与 ActivateIndexes 的事务边界

调用链：

```text
Connection / Service request
  -> ExecutionSlot::executeCore
    -> planner->analyzeQuery: Admin(kLoadExtension, name)
    -> ExecutionSlot::executeAdmin
  -> ExtensionManager::LoadExtension(name)
         -> check duplicate
              -> absent: dlopen -> dlsym(Init) -> Init -> record loaded
              -> present: warning -> skip Init
      -> create AP/TP update scope
      -> Storage{AP,TP}UpdateInterface::ActivateIndexes()
      -> commit / publish graph update
      -> invalidate query caches
      -> QueryResponse(row_count = 0)
```

AP 模式沿用 `InPlaceWriteScope`，创建 `StorageAPUpdateInterface` 后调用 `ActivateIndexes()`；成功退出时发布
planning generation，失败不发布修改。

TP 模式使用 `GetUpdateTransaction()` 和 `StorageTPUpdateInterface`，调用 `ActivateIndexes()` 后再
`transaction.Commit()`；失败时 transaction 析构/显式 abort。为此需要让 `StorageTPUpdateInterface` 也实现
`StorageIndexDDLInterface::ActivateIndexes()`，并对被激活的 index/module 做 COW detach 和 WAL/dirty 标记。
不能在 TP 服务中临时构造 `StorageAPUpdateInterface`，否则会绕过 MVCC、WAL 和 commit/abort。

这里存在一个不可完全回滚的边界：动态库 `Init()` 对函数/模块 registry 的注册通常不可逆，而
`ActivateIndexes()` 可能失败。因此定义如下语义：

- `LoadExtension` 成功、`ActivateIndexes` 失败：extension 仍保持 **loaded**，图事务回滚，语句返回
  activation 错误；用户修复数据问题后可以再次执行 `LOAD`。manager 对重复加载记录 warning、跳过
  `Init()` 并正常返回，ExecutionSlot 会重新执行 `ActivateIndexes()`。
- 不应把这种情况从 `loaded_extensions_` 删除并允许再次调用 `Init()`，否则可能重复注册函数/模块。
- `ActivateIndexes()` 必须幂等：已激活项跳过，pending 项可安全重试。

为表达这一点，推荐把结果拆开：

```cpp
struct LoadExtensionResult {
  std::string canonical_name;
  bool newly_loaded;
};
```

重复 LOAD 正常返回 `newly_loaded=false`；该结果用于日志、决定是否失效 extension registry 相关缓存，
但无论其值如何都必须继续执行 pending index activation。

## 并发与锁顺序

- `ExtensionManager::mutex_` 只保护 extension 管理状态及 `Init()`；不要在持锁时获取图 update transaction。
- 固定顺序为：先完成 `LoadExtension` 并释放 manager mutex，再申请 AP write scope / TP update transaction。
  避免 extension mutex 与 version/snapshot/checkpoint 锁形成环。
- 同 DB 的重复并发 LOAD：先获得锁者执行；后获得锁者看到 loaded 后记录 warning、跳过 `Init()`，
  然后继续尝试 activation。
- 不同 DB 可以并发管理自己的状态，但受当前进程级 extension registry/ABI 限制，`Init()` 仍可能需要一个
  很小的进程级 ABI 注册锁。该锁只解决第三方库线程安全问题，不能再承担 DB loaded 状态。
- CHECKPOINT 与 LOAD 的图激活依赖现有 `VersionManager`/`CheckpointCoordinator` 协调；LOAD 获取 update
  scope 后自然与 destructive checkpoint 互斥。

## 编译与 physical plan 调整

最终形态下，Admin 真正执行不依赖 `ExtensionInstallOpr/ExtensionLoadOpr/ExtensionUninstallOpr`。
建议分两阶段迁移：

1. `QueryAnalysis` 识别 Admin 并让 `ExecutionSlot::executeAdmin` 旁路；保留 physical protobuf 和 builder，
   仅用于 `EXPLAIN` 展示及兼容旧缓存。
2. 确认无旧 plan 兼容要求后，删除 pipeline extension operator；Admin explain 由统一的 admin formatter 输出。

`validateQueryAnalysis()` 也应从仅比较 `checkpoint()` 改为比较完整 Admin 类型；Extension 名称和 repository
由 analysis 自己承载，不应和 compiled plan 做两套来源校验。

## 错误处理与返回值

- 所有 manager 接口返回 `Status/result<T>`，不得在 execution 层用异常拼接二次错误。
- 保留具体阶段：`resolve path`、`dlopen`、`dlsym Init`、`Init`、`activate indexes`、`commit`。
- Admin 成功统一设置 `row_count = 0`；PROFILE 由 `executeAdmin` 按 action 名称记录 timer。
- `EXPLAIN LOAD/INSTALL/UNINSTALL` 不下载、不加载、不删除文件，也不获取 update transaction。
- deprecated extension 检查从 pipeline operator 移入 `ExtensionManager` 或公共 validator，保证旁路后仍生效。

## 建议改造顺序

1. 将静态 CAS loaded map 改为 `ExtensionManager` 实例字段和 mutex，并迁移三个操作入口。
2. 清理 MetadataManager/ClientContext 的历史 ExtensionManager 耦合；在 `NeugDB` 创建唯一 runtime
   manager，只注入所有 ExecutionSlot。
3. 扩展静态 string scanner 和 `QueryAnalysis/AdminRequest`，不依赖 parser/binder 完成 Admin 参数提取。
4. 实现 `ExecutionSlot::executeAdmin`，先迁移 CHECKPOINT，再迁移 extension 三类操作。
5. 为 TP update interface 实现事务化 `ActivateIndexes()`，补齐 COW、dirty、WAL 和 commit/abort。
6. Admin 成功后失效 query cache，移除 extension pipeline 执行路径。
7. 增加并发、失败注入及 AP/TP 集成测试。

## 必要测试

- 同一 DB 顺序/并发重复 LOAD 均正常返回并记录 warning，`Init()` 只调用一次，每次都尝试 activation。
- 两个 DB 的 loaded map 相互隔离；关闭一个 DB 不影响另一个 DB 的查询执行。
- `dlopen`、`dlsym`、`Init` 失败后状态未记录且允许重试。
- 已加载 extension 仍可 UNINSTALL 磁盘文件，已打开 handle/loaded 状态不变；未安装 extension 的
  LOAD/UNINSTALL 返回稳定错误码。
- AP、TP 下 LOAD 后 pending index 均被激活并可查询。
- AP、TP 下 activation 失败时图修改回滚、extension 仍 loaded，重试 activation 不重复执行 `Init()`。
- LOAD 与 CHECKPOINT、两个 LOAD 并发时无死锁，状态和 graph generation 一致。
- EXPLAIN Admin 无副作用；PROFILE Admin 只执行一次并返回对应 profile 节点。
- LOAD 后旧 query cache 不再命中，使用新注册函数/reader/index module 的查询可重新编译。
