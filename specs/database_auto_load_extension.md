# Database open 自动加载 extension 设计方案

状态：已实现并完成下述验证，验证范围与构建限制见第 11 节。基于当前 `persist_extension` 分支，日期为 2026 年 10 月 28 日。

在 `NeugDBConfig` 中新增 `auto_load_extension`，默认 `false`。显式开启后，由 `ExtensionManager::Open(const NeugDBConfig& config)` 在 graph/index 打开之前扫描当前扩展加载目录，逐个加载已存在的扩展；发现、加载或初始化失败均记录 ERROR 日志，并继续打开数据库。C++、Python、Node.js 和独立服务启动入口统一透传至核心配置。

## 1 需求与行为约定

| 场景 | 约定行为 |
| --- | --- |
| 未传配置或传入 false | 不扫描、不自动加载，保持现有行为 |
| 显式传入 true | 每次 Open 同步扫描一次，并尝试加载所有符合现有目录布局的扩展 |
| 单个扩展加载失败 | ERROR 日志，继续处理其他扩展，Open 不因此失败 |
| 目录不存在 | 记录 ERROR 后正常打开，不抛异常 |
| 目录为空 | 不加载扩展，正常打开 |
| 目录无权限、路径不是目录或遍历失败 | ERROR 日志，处理已经发现的候选，正常打开 |
| 只读、读写、纯内存数据库 | 都支持；开关属于数据库启动配置，与数据目录是否持久化无关 |
| 自动加载结束后新增扩展 | 本次 Open 不再扫描，可显式 LOAD，或者下次 Open 自动加载 |
| 数据库本身打开失败 | 保留原有错误语义，不吞掉文件锁、数据损坏、非法 planner 等错误 |

配置不写入 checkpoint，不记录“上次加载过哪些扩展”，不自动 INSTALL、不联网、不增加目录监控或扩展依赖排序。所有符合布局的本地扩展均会执行初始化，不以此前是否手动 LOAD 过作为筛选条件。

“不中断 Open”覆盖 Status/result 失败和可捕获的 C++ 异常；进程内动态库导致的段错误、abort、LOG(FATAL)、永久阻塞不属于 catch 能隔离的范围。本方案不引入进程隔离。

## 2 原有实现与设计依据

以下路径均相对于仓库根目录。

| 位置 | 当前行为及影响 |
| --- | --- |
| `include/neug/config.h.in` | 定义 `NeugDBConfig`，有两个构造函数；应修改模板而不是生成后的 config.h |
| `src/main/neug_db.cc` 的 `NeugDB::Open` | 恢复 graph/WAL 后创建 version manager、extension manager、planner 和 query runtime；最后将 closed 置为 false |
| `include/neug/compiler/extension/extension_manager.h` | `LoadExtension` 返回 `result<LoadResult>`，包含规范名和 newly_loaded；同一 manager 内按扩展名去重 |
| `src/execution/extension/extension.cc` | 实际实现路径解析、dlopen、Init、加载失败返回值；NEUG_LITE 下 LOAD 返回不支持 |
| `src/main/execution_slot.cc` 的扩展管理路径 | 手动 LOAD 不仅加载库，还在 COW 事务内调用 ActivateIndexes |
| `src/storages/index/storage_index_manager.cc` | 扩展类型尚未注册时保留 pending indexes；加载后可恢复索引并重放其 pending mutations |
| `tools/python_bind/neug/database.py` 与 `src/py_database.*` | Python 参数经 kwargs、PyDatabase 构造函数写入 NeugDBConfig |
| `tools/nodejs_bind/lib/database.js` 与 `src/node_database.cc` | JS options 经原生 NodeDatabase 写入 NeugDBConfig |
| `bin/rt_server.cc` | 独立服务通过命令行构造 NeugDBConfig 后 Open |

仓库当前使用 glog 的 `LOG(ERROR)`，没有检索到 `LOG_ERROR` 宏。本需求中的 LOG_ERROR 采用现有 `LOG(ERROR)` 实现，不另建日志宏。

## 3 配置接口

### 3.1 C++

在 `NeugDBConfig` 尾部新增 `bool auto_load_extension = false;`，使用成员默认初始化保证两个现有构造函数均覆盖。注释注明扫描范围、默认值和失败不阻断 Open。

```cpp
neug::NeugDBConfig config(data_dir, max_thread_num);
config.auto_load_extension = false;
db.Open(config);
```

保留现有字符串参数版 `Open(...)` 签名；需要此选项的 C++ 调用方使用已有 config 重载。这样不继续扩张位置参数接口。新增结构字段会改变 C++ ABI，核心库和绑定需一起重新编译。

### 3.2 Python

在 `Database.__init__` 现有参数末尾增加 `auto_load_extension: bool = False`，保留所有已有位置参数含义。完整传递链为：

```text
Database(auto_load_extension=...) → PyDatabase kwargs → PyDatabase 构造函数
→ config.auto_load_extension → NeugDB::Open(config)
```

公开 Python 层和可直接调用的 pybind 入口均检查布尔类型；拒绝 `"true"`、`1`、`None` 等值，避免隐式真值转换。非法配置属于调用错误，在 Open 前抛出 TypeError，不属于应吞掉的扩展加载错误。

```python
from neug import Database

db = Database(db_path="/data/graph", mode="read-write", auto_load_extension=False)
```

同步修改参数文档、pybind 构造说明和 Python API 文档。Python 包已经在初始化时设置或保留 `NEUG_EXTENSION_HOME_PYENV`，不需要再增加 Python 专用扫描逻辑。

### 3.3 Node.js

遵循现有 camelCase 风格，公开参数命名为 `autoLoadExtension`，默认 `false`，映射至 C++ 的 `auto_load_extension`。不同时新增 snake_case 别名，避免冲突规则。

```javascript
const { Database } = require('neug');
const db = new Database({
  databasePath: '/data/graph',
  mode: 'read-write',
  autoLoadExtension: false,
});
```

`lib/database.js` 增加解构默认值、boolean 校验和原生参数透传；`src/node_database.cc` 再次校验，拒绝非 Boolean，而非静默忽略。省略或 JS 层 undefined 使用默认 false，null 和字符串应报 TypeError。同步更新 JSDoc 与 Node.js API 文档；当前目录未发现独立 TypeScript 声明文件，不为本功能新建整套声明。

### 3.4 服务和其他客户端

`rt_server` 增加 `--auto-load-extension` 布尔选项，默认 false，支持显式 `--auto-load-extension=true/false`，写入数据库配置。Python `serve()` 复用已经打开的数据库，不重复加载、不增加同名 ServiceConfig 字段。

远程 Session、Java/HTTP 客户端不在客户端打开本地数据库，不能通过连接选项改变服务端启动配置；由服务启动入口设置。后续新增嵌入式绑定遵循相同的核心配置透传方式。

## 4 扩展目录发现

### 4.1 唯一目录来源

目录解析由 ExtensionManager 的实现直接调用当前实现文件内的 `extension::getExtensionRootDir()`，不增加仅做转发的 ExtensionDir 成员函数。`getUserExtensionDir(name)` 复用同一根目录规则，保证自动发现与手动 LOAD 使用同一目录。底层路径 helper 保持实现文件内部可见，不向 NeugDB 暴露目录、候选列表或遍历接口，也不为此扩展公共 extension API。根目录解析顺序保持不变：

1. 非空 `NEUG_EXTENSION_HOME_PYENV` 作为 base。
2. 否则使用编译时 `NEUG_EXTENSION_HOME_MACRO`。
3. base 仍为空时使用 `/tmp`。
4. 当前扩展根目录为 `<base>/extension`。

这是单一有效目录，不是多个目录合并搜索；环境变量覆盖编译默认值后，不再扫描编译目录。不要扫描 database data_dir、当前工作目录或整个动态库搜索路径。

```text
<base>/extension/
  vector_search/libvector_search.neug_extension
  parquet/libparquet.neug_extension
  fts/libfts.neug_extension
```

扩展库文件名必须通过现有 `ExtensionUtils::getExtensionFileName(name)` 生成，不复制平台命名规则。一次扫描固定根目录；调用者不得在 Open 过程中并发修改进程环境变量。

### 4.2 候选规则

仅遍历根目录的直接子目录，不递归加载第三方依赖。每个子目录视为扩展安装槽位，使用与手动 LOAD 一致的小写规范名和目标路径；按规范名去重、排序，确保日志及尝试顺序稳定。

目录名为小写且预期库存在的槽位加入候选。槽位缺少预期库、大小写导致路径不匹配或检查失败时记录 ERROR 并继续；根目录普通文件、sha256、README 等不是扩展槽位，直接忽略。保留现有 LOAD 对符号链接路径的解析能力，只处理一层，不递归追随链接。损坏链接记录 ERROR。

不依靠后缀递归查找任意文件，也不从根目录直接加载裸库。这样每个自动加载候选都能用相同名字执行手动 LOAD。库在扫描后被删除或替换，由实际 LOAD 再检查并报告失败。

使用 `std::filesystem` 的 error_code 重载逐项检查，显式处理迭代器构造及 increment 失败。遍历中途失败时停止不可继续的迭代，但继续加载已收集的候选；不同候选的检查失败不终止整次扫描。

## 5 ExtensionManager 接口与 Open 集成

### 5.1 接口职责

新增公开接口 `void ExtensionManager::Open(const NeugDBConfig& config)`。该接口直接完成自动加载，不返回候选列表供 NeugDB 遍历；不需要额外的公开 Init 接口。

```cpp
struct NeugDBConfig;

class ExtensionManager {
 public:
  void Open(const NeugDBConfig& config);
  // 保留现有 LoadExtension、InstallExtension 等接口。

 private:
  // 如需拆分发现或批量加载函数，也保持为私有实现。
};
```

`config.auto_load_extension == false` 时直接返回，不解析目录、不扫描、不加载。`config.auto_load_extension == true` 时内部负责目录解析、候选发现、排序去重、逐个 LoadExtension、错误日志和异常隔离。NeugDB 只负责创建 manager 和传入配置，不新增 autoLoadExtensions、ExtensionDir 或 Activate 相关方法。

同一 manager 在配置开启时重复调用 Open(config) 可重新扫描，但已加载扩展由现有 LoadExtension 去重；配置关闭时调用 Open(config) 不卸载已经加载的扩展。正常数据库生命周期每次 Open 只调用一次该接口。

接口接受数据库启动配置，当前仅读取 auto_load_extension。参数使用 const 引用，避免复制及修改调用方配置；manager 不保存 config 的引用或指针，不引入对调用方配置生命周期的依赖。未来确有扩展相关启动选项时，由该接口读取对应字段，无需持续追加位置参数。头文件前置声明 NeugDBConfig，实现在需要读取字段处包含 neug/config.h；目录路径和候选发现继续作为 manager 内部细节，不通过 NeugDB 传递。

### 5.2 调用时点

将当前位于 `openGraphAndIngestWals()` 之后的 ExtensionManager 创建操作前移，在 `checkpoint_mgr_.Open(...)` 和 `PlanParser::get().init()` 之后、`openGraphAndIngestWals()` 之前，先初始化 planner，再创建并调用：

```cpp
initPlanner();  // 建立扩展注册函数所需的 MetadataRegistry
extension_manager_ = std::make_unique<ExtensionManager>();
extension_manager_->Open(config_);
timestamp_t initial_visibility_ts = openGraphAndIngestWals();
```

同时前移原有 initPlanner 调用，删除原位置的重复创建和 planner 初始化，保证后续执行槽使用的就是提前初始化的 manager。保留现有 Open 异常清理中的 `extension_manager_.reset()` 和 Close 中的释放逻辑。

```text
配置与数据目录锁 → checkpoint manager → PlanParser 初始化
→ planner 与 MetadataRegistry → 创建 ExtensionManager → ExtensionManager::Open(config)
→ openGraphAndIngestWals
    → PropertyGraph::Open → 原有模块和索引 Open
    → 原有 WAL 恢复
→ checkpoint coordinator / version manager / query runtime
→ 标记数据库已打开 → Open 返回
```

当前 `openGraphAndIngestWals()` 内先调用 `PropertyGraph::Open`，然后重放 WAL。选择在整个函数之前加载，可以保证索引模块反序列化及 index::Open 所需的扩展注册已经完成，无需向 storage/index 层插入新逻辑。扩展 Init 依赖 GOptPlanner 构造时注册的 MetadataRegistry，因此 initPlanner 必须先于 manager.Open；它不依赖 graph，可安全前移。扩展 Init 仍不能依赖尚未初始化的连接或 query runtime，沿用现有无参数 Init 的注册职责。

### 5.3 异常边界

自动加载异常全部在 ExtensionManager::Open 内处理。保留 LoadExtension 的现有返回值与手动调用错误语义，不把手动 LOAD 一并改为吞错。

```text
ExtensionManager::Open(config):
  if not config.auto_load_extension: return
  try:
    root = extension::getExtensionRootDir()
    names = discover(root)  // 私有实现，逐项记录错误，允许部分结果
    sort and deduplicate names
    for name in names:
      try:
        result = LoadExtension(name)
        if result failed: log ERROR with name, path, status
      catch std::exception: log ERROR with name and what()
      catch (...): log ERROR with name and unknown error
  catch std::exception: log ERROR with discovery/startup stage
  catch (...): log ERROR with unknown startup error
  return
```

不在整个 Open 期间持有 manager mutex；每次 LoadExtension 使用现有锁，避免嵌套获取同一把锁。目录解析、枚举、排序和加载均纳入保护范围。不要仅给方法加 noexcept 就认为已经容错，也不改变 NeugDB::Open 外层 catch 对数据库自身错误的清理和传播行为。

## 6 沿用现有索引打开与延迟初始化

扩展自动加载成功时，其模块已在 graph/index Open 之前注册。索引直接走现有打开及 WAL 恢复流程；NeugDB 不新增 Activate 功能，不创建启动专用 COW 工作区，不增加索引激活事务、redo 或快照发布接口。

扩展未安装、加载失败或 auto_load_extension=false 时，保持当前对不可用模块的处理：具备现有恢复条件的索引保留为 pending，等待用户在读写模式显式执行 LOAD EXTENSION 后，通过现有执行路径完成初始化。只读模式保留原有禁止 LOAD 管理操作的规则，需要使用扩展索引时应在 Open 时开启自动加载。缺少必要恢复元数据等情况也保持原处理方式，不修改 StorageIndexManager、事务或 WAL 逻辑。

错误边界需要区分两个阶段：ExtensionManager::Open 中的扫描、动态库加载和 Init 失败只记录 ERROR 并返回；后续 index::Open 或 WAL 恢复遇到数据损坏等错误，继续遵循原有数据库打开语义，不增加吞错或回滚机制。提前加载使某些索引直接打开，这些索引自身的打开失败不属于自动加载异常。

## 7 日志与生命周期

ERROR 日志至少包含固定前缀 `[AutoLoadExtension]`、阶段、根目录或库路径、扩展名（适用时）、Status 或异常原因。例如：

```text
[AutoLoadExtension] stage=load extension=fts path=... error=...; continuing database open
[AutoLoadExtension] stage=discover root=... error=Permission denied; continuing database open
```

可增加一条 INFO 汇总：发现数、成功数、已加载跳过数和失败数；不把每个扩展失败升级为数据库打开失败。目录不存在记录 ERROR 后正常返回，目录为空不刷 ERROR。NEUG_LITE 且显式开启时，记录一次 ERROR 说明当前构建不支持自动加载，然后继续 Open；默认关闭不增加日志。

同一 manager 内自动加载后再手动 LOAD，继续使用原有 newly_loaded=false 行为。Close 后重新 Open 创建新的 manager，重新扫描。全局 `ExtensionAPI` 和 `ModuleFactory` 是进程级注册表，`auto_load_extension=false` 只保证当前数据库不主动加载，不保证隔离进程中其他数据库已经注册的扩展。

本次只复用现有 LoadExtension，不修改其锁、Init 异常处理或句柄释放策略，也不修改 ModuleFactory 和 ExtensionAPI 的公共接口。多库并发注册及 Init 部分注册后失败的异常安全属于现有加载机制的通用问题，留待独立修改，不作为自动加载功能的一部分扩展范围。

## 8 改动文件清单

| 文件或范围 | 计划修改 |
| --- | --- |
| `include/neug/config.h.in` | 新配置默认值及文档 |
| `src/main/neug_db.cc` | 将 manager 创建前移，在 graph/index Open 前调用 manager.Open(config_)；不新增 NeugDB 方法 |
| `include/neug/compiler/extension/extension_manager.h` | 公开 Open(const NeugDBConfig&) 声明 |
| `src/execution/extension/extension.cc` | manager.Open 实现、内部目录解析和枚举、异常隔离、lite 分支 |
| `tools/python_bind/neug/database.py`、`tools/python_bind/src/py_database.h`、`.cc` | Python 配置完整透传和类型检查 |
| `tools/nodejs_bind/lib/database.js`、`tools/nodejs_bind/src/node_database.cc` | JS/N-API 配置透传和类型检查 |
| `bin/rt_server.cc` | 服务启动参数 |
| `tools/python_bind/tests/test_extension_auto_load.py` 和 `test_hnsw_index.py` | 发现、异常隔离、打开生命周期和跨进程索引恢复测试 |
| `tools/python_bind/src/neug_binding.cc` | 保留原生参数校验 TypeError，避免统一异常转换器将其改成 RuntimeError |
| Python 和 Node.js tests | 各端默认值、显式启用与原生端透传测试 |
| `doc/source/reference/{cpp_api,python_api,nodejs_api}`、服务启动说明 | 公开配置示例与失败语义 |

不修改生成文件、打包产物或用户本地 extension_packages 副本。

## 9 验证与验收

测试使用临时 extension home 和本地构建的测试动态库，不依赖远程 INSTALL。环境变量在加载 Python 包前设置；进程级注册无法靠关闭数据库清理，应以独立子进程验证默认关闭、重复打开和失败隔离。

| 测试组 | 核心断言 |
| --- | --- |
| 配置默认值 | 两种 C++ 构造函数、Python、Node.js、省略 CLI 参数均为 false |
| 完整透传 | 各端 true 能在第一条用户查询前调用测试扩展函数；false 不调用 Init |
| 类型校验 | Python/JS 非布尔值被拒绝；直接 pybind/N-API 调用也不能绕过 |
| 路径优先级 | 环境变量覆盖、编译默认、空值回退与手动 LOAD 一致，不合并其他根目录 |
| 发现过滤 | 多扩展、空目录、缺失库、大小写、符号链接、杂项文件、非递归、稳定排序 |
| 返回失败 | 无效动态库、缺失依赖、缺失 Init、扫描后文件消失；Open 成功且其他有效扩展加载 |
| 抛异常 | Init 抛 std::exception 和非标准异常，均有 ERROR，后续扩展仍被尝试 |
| 枚举失败 | 权限不足、根路径为文件、迭代中断；已收集候选继续加载 |
| 核心 Open 失败 | 文件锁冲突、无只读 checkpoint、非法 planner 等仍按原行为失败 |
| 生命周期 | 自动后手动 LOAD 不重复 Init；Close/Open 重新扫描 |
| 只读和内存 | 模式均可启用；只读启动不追加 WAL、不修改 checkpoint/索引文件 |
| 索引恢复 | 新进程加载带索引 checkpoint，含 pending WAL 变更；Open 后索引查询结果正确 |
| 索引延迟初始化 | 扩展缺失或加载失败时保持原 pending 行为；修复后显式 LOAD 按原流程初始化索引 |
| 索引打开错误 | 已注册模块的索引数据损坏等错误保持原 Open 失败语义，不被自动加载的 catch 吞掉 |
| 接入顺序 | 验证 Init 先于 graph/index Open；同一 manager 传给后续执行槽，没有重复创建 |
| lite 构建 | true 仅记录不支持错误并正常打开，false 没有额外错误 |
| 服务 | CLI 配置生效，HTTP 开始接受请求前自动加载完成，Python serve 不重复执行 |

日志断言捕获 C++ stderr，不只检查 Python logging。不要仅依赖 SHOW_LOADED_EXTENSIONS：当前测试已有 httpfs 不出现在该列表的例外；应结合测试扩展的实际功能和 Init 计数。

构建和测试遵循仓库 AGENTS.md，所有 NeuG Python 命令使用指定的 Python 解释器，构建限制并行度。优先跑扩展与数据库打开的定向 C++、Python、Node.js 测试，再执行相关索引恢复回归。

## 10 实施顺序

1. 新增核心配置并打通 C++、Python、Node.js、rt_server，测试默认值及类型约束。
2. 实现 ExtensionManager::Open(const NeugDBConfig&)，内部封装目录解析、发现和逐项加载，覆盖 filesystem 错误及 NEUG_LITE。
3. 前移 manager 创建与 Open 调用，保证在 graph/index 打开前完成加载，验证逐扩展失败隔离。
4. 回归原有索引打开、WAL 恢复和显式 LOAD 后延迟初始化行为；不新增索引激活路径或通用注册机制修改。
5. 补齐跨进程恢复及绑定端集成测试，更新公开 API 文档。

完成标准：默认行为不变；所有本地合法扩展均被尝试；ExtensionManager::Open 内可捕获的自动加载错误只写 ERROR、不向 NeugDB::Open 传播；Python 和 Node.js 配置确实到达核心；有效扩展在 Open 返回时可用；数据库自身错误依旧可见。

## 11 实现与验证记录

已按配置接口实现 ExtensionManager::Open(config)，目录发现和失败处理封装在 manager 内部，未新增 NeugDB Activate 方法，也未修改索引恢复、事务或 WAL 逻辑。

落地补充两个由现有代码决定的依赖：initPlanner 必须先于 manager.Open 建立 MetadataRegistry；原生 Python 异常转换器需要保留 pybind11::type_error 才能让直接绑定调用也返回 TypeError。只读模式继续禁止显式 LOAD，测试和用法说明已保留该规则。

| 验证 | 结果 |
| --- | --- |
| Python 自动加载端到端测试 `tests/test_extension_auto_load.py` | 29 通过，使用独立进程和本地动态库，无网络安装 |
| Python 数据库初始化回归 `tests/test_db_init.py -k 'not local_ldbc'` | 24 通过、5 按现有条件跳过、1 外部数据集测试排除 |
| Python 现有 checkpoint 重开及进程重启索引持久化回归 | 2 通过，显式开启 NEUG_RUN_EXTENSION_TESTS |
| Node.js `tests/test_extension_auto_load.js` | 10 通过，包括原生类型检查及坏库日志验证配置透传 |
| CTest `ExtensionAutoLoadConfig` 和 `ExtensionConfigTest`（删除冗余测试前） | 4 通过；后续已删除新增的 `ExtensionAutoLoadConfig` 测试 |
| rt_server 启动检查 | false 不加载；true 记录坏库错误后仍接受连接 |
| 格式与静态检查 | clang-format、Black、isort、项目配置下 flake8 和 git diff --check 通过 |

各端示例显式保持默认关闭，开启时将对应布尔配置设为 true。面向用户的完整说明统一维护在 [Python Database 的 auto_load_extension 配置参数](../doc/source/reference/python_api/database.md)，Node.js、C++ 和服务启动文档简述参数并引用该处。

构建使用指定 Python 3.9 和受限并行度，核心、Python/Node.js 绑定、服务程序及 C++ 测试构建成功。当前工作区原有 third_party/zvec 改动导致 zvec.patch 检查失败，未改动该依赖以绕过检查；本次核心构建将 BUILD_EXTENSIONS 显式置空，索引端到端及回归测试使用本地已有 vector_search 动态库。没有完成 vector_search 从源码重新构建，也未运行独立 lite 构建或全量测试套件。

## 12 评审修订

- 删除 ExtensionDir 转发成员，Open 直接调用实现文件内部的根目录 helper。
- 自动加载目录不存在时输出 LOG(ERROR) 并返回，空目录仍然正常返回。
- 保持原有 LoadExtension、ModuleFactory 和 ExtensionAPI 实现，撤销通用并发及异常安全加固。
- 初始化顺序保持 planner → ExtensionManager::Open → openGraphAndIngestWals → PropertyGraph::Open；lite 继续沿用原有不支持动态 LOAD 的行为。
- API 说明合并到现有配置参数部分；撤销重复的 Node.js README 新增章节。README 虽列入 npm 发布文件，但此次无需重复 API reference 内容。

本轮修订后重新编译核心、Python/Node.js 绑定、rt_server 和扩展单测成功；Python 自动加载端到端 29 项、Node.js 自动加载测试 10 项、C++ 扩展配置测试 4 项全部通过。缺失目录测试同时验证 ERROR 文本、目录路径以及数据库仍可查询。

后续测试修订：删除仅检查默认配置及关闭自动加载的 C++ `ExtensionAutoLoadConfig.DefaultsAndDisabledManager` 测试，`test_extension.cc` 恢复原状。在 `test_hnsw_index.py` 新增 `test_auto_load_extension_restores_index_in_new_process`：父进程关闭自动加载，显式 LOAD vector_search、创建 HNSW 索引并从 CSV 导入数据，关闭数据库后启动独立 Python 解释器。子进程断言 PID 与父进程不同，使用 `auto_load_extension=True` 只读打开数据库，不执行 LOAD，验证最近邻结果及 PROFILE 中的 `IndexScanOpr`。该用例实测 1 通过，Black、isort、flake8 检查通过。
