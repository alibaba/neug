**Version**: 0.2.0  
**Created**: 2026-01-23  
**Status**: Draft

本文档定义 GDS 扩展的产品需求、用户接口、技术实现与实现优先级。核心设计为 **Project Subgraph**：先通过 `project_graph` 投影命名子图，再基于子图名调用图算法，实现子图与算法解耦。

**文档结构**：§1 产品需求（算法列表与 Project Subgraph 语法）→ §2 用户接口（Extension 机制与 Cypher）→ §3 技术实现（Project SubGraph 方案与 GDSGraph）→ §4 开发者接口 → §5 内置 GDS 扩展 → §6 实现优先级 → §7 附录。

---

## 1. 产品需求：MVP 算法与语法
### 1.1 选择原则
+ **最常用经典算法**：图数据库的基础能力，覆盖大部分业务场景
+ **AI 时代刚需**：主要是 GraphRAG 场景的社区发现算法

### 1.2 V1 算法列表
第一版支持 **8 个核心算法**，分为两类（BFS、LCC 等别名在详细说明中列出）：

#### 经典图算法（6 个）
| 算法 | 图语义 | 描述 | 输出 | 并行化 |
| --- | --- | --- | --- | --- |
| **K-Core** | 无向 | 找出所有核心数 ≥ k 的子图 | `(node, core_number)` | 支持 |
| **PageRank** | 有向 | 计算节点的重要性分数 | `(node, rank)` | 支持 |
| **Shortest Path (Dijkstra)** | 有向 | 单源最短路径 | `(node, distance, path)` | 不支持 |
| **Connected Components** | 无向 | 弱连通分量检测（别名 WCC） | `(node, component_id)` | 支持 |
| **Breadth-First Search (BFS)** | 有向 | 从源点出发的广度优先遍历，按层扩展 | `(node, distance)` | 不支持 |
| **Local Clustering Coefficient (LCC)** | 无向 | 顶点局部聚类系数 | `(node, coefficient)` | 支持 |


#### AI/GraphRAG 刚需算法（2 个）
| 算法 | 图语义 | 描述 | 输出 | 并行化 |
| --- | --- | --- | --- | --- |
| **Leiden** | 无向 | 高质量社区发现（优于 Louvain） | `(node, community_id)` | 支持 |
| **Label Propagation** | 无向 | 基于标签传播的快速社区发现 | `(node, label)` | 支持 |


### 1.3 图语义说明
NeuG 底层存储为**有向图**（CSR for outgoing, CSC for incoming）。算法层根据算法需求封装不同的语义：

| 图语义 | 实现方式 | 适用算法 |
| --- | --- | --- |
| **有向** | 只使用 `out_neighbors()` | PageRank, Shortest Path, BFS |
| **无向** | 使用 `out_neighbors() ∪ in_neighbors()` | K-Core, Connected Components, Leiden, Label Propagation, LCC |


**注意**：如果用户数据已经在 `out_neighbors` 中存储了双向边（模拟无向图），使用"无向"语义的算法可能会导致边重复计算。用户应确保数据存储方式与算法语义匹配。

### 1.4 算法详细说明
这里我需要对算法的表示提出修改。我认为还是修改为Project Subgraph，再基于SubGraph进一步执行图算法更好。

+ Project 子图语法：通过 Project 操作记录子图中点，边 Label 信息，并且可以基于点和边执行过滤条件。

```cypher
CALL project_graph(
    <GRAPH_NAME>,
    {
        <NODE_TABLE_0> :  <NODE_PREDICATE_0>,
        <NODE_TABLE_1> :  <NODE_PREDICATE_1>,
        ...
    },
    {
        <REL_TABLE_0> :  <REL_PREDICATE_0>,
        <REL_TABLE_1> :  <REL_PREDICATE_1>,
        ...
    }
);
```

**示例**：

```cypher
CALL project_graph(
    'filtered_graph',
    {'Person': 'n.name <> "Ira"'},
    {'KNOWS': 'r.id < 3'}
);
```

上述例子表示：

从全图中筛选出Label为'person'点，且需满足 'n.name <> "Ira"'  
从全图中筛选出Label为'KNOWS'边，端点 (src和dst) 需满足 `'Person': 'n.name <> "Ira"'` 限制，且边上的属性需满足 `'r.id < 3'`

+ 执行算法语法：

基于子图进一步执行图算法，而不是将Label信息作为参数传递给算法。

```cypher
CALL algorithm_name(<graph_name>, {param1: value1, ...})
YIELD column1, column2, ...
```

这样可以将子图信息和图算法本身解藕，通过两种组合支持更多功能。

#### 1.4.1 K-Core Decomposition
**图语义**：**无向图** — 使用 `out_neighbors() ∪ in_neighbors()` 计算顶点度数

**描述**：K-Core 是一个子图，其中每个顶点至少有 k 个邻居。K-Core 分解计算每个顶点所属的最大 k 值。基于已 Project 的子图执行，不接收 Label 信息。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `min_k` | Integer | 否 | 1 | 返回 core_number ≥ min_k 的顶点 |
| `concurrency` | Integer | 否 | 0 | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 顶点标识符 |
| `core_number` | Integer | 顶点的核心数 |


**Cypher 示例**：

```cypher
CALL project_graph('my_graph', {'Person': 'n.name <> "Ira"'}, {'KNOWS': 'r.id < 3'});

CALL k_core('my_graph', {min_k: 3, concurrency: 4})
YIELD node, core_number
RETURN node, core_number
ORDER BY core_number DESC
```

#### 1.4.2 PageRank
**图语义**：**有向图** — 只使用 `out_neighbors()` 进行 rank 传播

**描述**：PageRank 通过迭代计算节点的重要性分数，基于链接分析。边的方向表示"投票"关系：A→B 表示 A 将部分 rank 传递给 B。基于已 Project 的子图执行。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `damping` | Float | 否 | 0.85 | 阻尼系数 |
| `max_iterations` | Integer | 否 | 20 | 最大迭代次数 |
| `tolerance` | Float | 否 | 1e-6 | 收敛阈值 |
| `concurrency` | Integer | 否 | 0 | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 顶点标识符 |
| `rank` | Float | PageRank 分数 |


**Cypher 示例**：

```cypher
CALL project_graph('page_graph', {'Page': 'true'}, {'LINKS_TO': 'true'});

CALL pagerank('page_graph', {damping: 0.85, max_iterations: 30, concurrency: 8})
YIELD node, rank
RETURN node, rank
ORDER BY rank DESC
LIMIT 10
```

#### 1.4.3 Shortest Path (Dijkstra)
**图语义**：**有向图** — 只使用 `out_neighbors()` 沿边方向搜索路径

**描述**：计算从源节点到所有其他节点的最短路径（加权）。路径只沿着边的方向行进。基于已 Project 的子图执行。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `source` | Any | 是 | - | 源节点标识符 |
| `target` | Any | 否 | null | 目标节点（null 表示计算到所有节点） |
| `weight_property` | String | 否 | null | 边权重属性名（null 表示无权重，等价于 BFS） |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 目标顶点标识符 |
| `distance` | Float | 从源到该节点的最短距离 |
| `path` | List | 最短路径经过的节点列表（可选） |


**Cypher 示例**：

```cypher
CALL project_graph('station_graph', {'Station': 'true'}, {'CONNECTED': 'true'});

-- 无权重最短路径 (BFS)
CALL shortest_path('station_graph', {source: 'StationA'})
YIELD node, distance
RETURN node, distance

-- 加权最短路径
CALL project_graph('road_graph', {'City': 'true'}, {'ROAD': 'true'});
CALL shortest_path('road_graph', {source: 'Beijing', target: 'Shanghai', weight_property: 'distance'})
YIELD node, distance, path
RETURN distance, path
```

#### 1.4.4 Connected Components (Weakly)
**图语义**：**无向图** — 使用 `out_neighbors() ∪ in_neighbors()` 进行连通性判断

**描述**：检测图中的弱连通分量。两个顶点如果可以通过边（忽略方向）相连，则属于同一分量。基于已 Project 的子图执行。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `concurrency` | Integer | 否 | 0 | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 顶点标识符 |
| `component_id` | Integer | 所属连通分量 ID |


**Cypher 示例**：

```cypher
CALL project_graph('social_graph', {'Person': 'true'}, {'KNOWS': 'true'});

CALL connected_components('social_graph', {concurrency: 4})
YIELD node, component_id
WITH component_id, count(*) AS size
RETURN component_id, size
ORDER BY size DESC
```

#### 1.4.5 Breadth-First Search (BFS)
**图语义**：**有向图** — 从源点出发，仅沿 `out_neighbors()` 按层扩展

**描述**：从给定源节点出发的广度优先遍历，按边数（跳数）分层。每层内的节点与源点的最短跳数相同。适用于层级发现、可达性分析、无权最短跳数。基于已 Project 的子图执行。

**输入参数**：

| 参数        | 类型    | 必选 | 默认值 | 描述                          |
| ----------- | ------- | ---- | ------ | ----------------------------- |
| `source`    | Any     | 是   | -      | 源节点标识符                  |
| `max_depth` | Integer | 否   | -1     | 最大遍历深度（-1 表示不限制） |


**输出列**：

| 列名       | 类型    | 描述                       |
| ---------- | ------- | -------------------------- |
| `node`     | Any     | 顶点标识符                 |
| `distance` | Integer | 从源到该节点的跳数（边数） |


**Cypher 示例**：

```cypher
CALL project_graph('social_graph', {'Person': 'true'}, {'KNOWS': 'true'});

CALL bfs('social_graph', {source: 'Alice', max_depth: 3})
YIELD node, distance
RETURN node, distance
ORDER BY distance
```

#### 1.4.6 Local Clustering Coefficient (LCC)
**图语义**：**无向图** — 使用 `out_neighbors() ∪ in_neighbors()` 计算邻居间连边

**描述**：顶点的局部聚类系数 = 其邻居间实际边数 / 邻居对理论最大边数。取值 [0, 1]，刻画局部稠密程度。基于已 Project 的子图执行。

**输入参数**：

| 参数          | 类型    | 必选 | 默认值 | 描述                       |
| ------------- | ------- | ---- | ------ | -------------------------- |
| `concurrency` | Integer | 否   | 0      | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名          | 类型  | 描述                 |
| ------------- | ----- | -------------------- |
| `node`        | Any   | 顶点标识符           |
| `coefficient` | Float | 局部聚类系数，[0, 1] |


**Cypher 示例**：

```cypher
CALL project_graph('coauthor_graph', {'Author': 'true'}, {'CO_AUTHOR': 'true'});

CALL lcc('coauthor_graph', {concurrency: 4})
YIELD node, coefficient
RETURN node, coefficient
ORDER BY coefficient DESC
LIMIT 20
```

#### 1.4.7 Leiden Community Detection
**图语义**：**无向图** — 使用 `out_neighbors() ∪ in_neighbors()` 计算模块度

**描述**：Leiden 算法是 Louvain 的改进版本，用于高质量社区发现。这是 GraphRAG 场景的核心算法。社区发现基于无向图的模块度优化。基于已 Project 的子图执行。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `resolution` | Float | 否 | 1.0 | 分辨率参数，值越大社区越小 |
| `max_iterations` | Integer | 否 | 10 | 最大迭代次数 |
| `weight_property` | String | 否 | null | 边权重属性名 |
| `concurrency` | Integer | 否 | 0 | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 顶点标识符 |
| `community_id` | Integer | 所属社区 ID |


**Cypher 示例**：

```cypher
-- GraphRAG 场景：对文档实体进行社区划分
CALL project_graph('entity_graph', {'Entity': 'true'}, {'RELATED': 'true'});

CALL leiden('entity_graph', {resolution: 1.0, max_iterations: 10, concurrency: 8})
YIELD node, community_id
RETURN community_id, collect(node) AS entities
ORDER BY size(entities) DESC
```

#### 1.4.8 Label Propagation
**图语义**：**无向图** — 使用 `out_neighbors() ∪ in_neighbors()` 进行标签传播

**描述**：基于标签传播的快速社区发现算法，适用于大规模图。每个节点选择其邻居中出现最多的标签作为自己的标签。基于已 Project 的子图执行。

**输入参数**：

| 参数 | 类型 | 必选 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `max_iterations` | Integer | 否 | 10 | 最大迭代次数 |
| `concurrency` | Integer | 否 | 0 | 并发线程数，0 表示自动检测 |


**输出列**：

| 列名 | 类型 | 描述 |
| --- | --- | --- |
| `node` | Any | 顶点标识符 |
| `label` | Integer | 所属标签/社区 ID |


**Cypher 示例**：

```cypher
CALL project_graph('user_graph', {'User': 'true'}, {'FOLLOWS': 'true'});

CALL label_propagation('user_graph', {max_iterations: 20, concurrency: 4})
YIELD node, label
RETURN label, count(*) AS community_size
ORDER BY community_size DESC
```

---

## 2. 用户接口：Extension 机制
### 2.1 设计目标
+ **简洁易用**：最小化用户操作步骤
+ **安全可控**：仅加载受信任的扩展
+ **平台覆盖**：支持 Linux 和 macOS（Windows通过WSL）

### 2.2 支持的平台
| 平台 | 架构 |   
|------|------|--------|  
| Linux | x86_64 |   
| Linux | aarch64 (ARM64) |   
| macOS | arm64 (Apple Silicon) |   
| macOS | x86_64 | 

> **注意**：不支持 Windows 平台。
>

### 2.3 Extension 生命周期
```plain
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  开发/编译   │ -> │   安装      │ -> │   加载      │ -> │   使用      │
│  (内部开发者) │    │ INSTALL     │    │ LOAD        │    │ CALL xxx()  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### 2.4 Cypher 接口定义
#### 2.4.1 INSTALL EXTENSION
**语法**：

```cypher
INSTALL extension_name;
```

**说明**：

+ 从 NeuG 官方仓库下载当前平台对应的 **.so** or **.dylib** 包并安装扩展
+ 扩展会被安装到 `$NEUG_HOME/extensions/` 目录
+ 安装后需要 `LOAD` 才能使用

**示例**：

```cypher
-- 安装图算法扩展
INSTALL gds;
```

#### 2.4.2 LOAD EXTENSION
**语法**：

```cypher
LOAD extension_name;
```

**说明**：

+ 加载已安装的扩展到当前会话
+ 加载后扩展中的函数可用

**示例**：

```cypher
-- 加载图算法扩展
LOAD gds;
```

#### 2.4.3 SHOW EXTENSIONS
**语法**：

```cypher
CALL SHOW_LOADED_EXTENSIONS() Return *;
```

**输出**：

| name | version | loaded | description |
| --- | --- | --- | --- |
| gds | 0.2.0 | true | Graph Data Science algorithms |


#### 2.4.4 project_graph（投影子图）
**语法**：

```cypher
CALL project_graph(
    <GRAPH_NAME>,
    { <NODE_TABLE_0> : <NODE_PREDICATE_0>, ... },
    { <REL_TABLE_0> : <REL_PREDICATE_0>, ... }
);
```

**说明**：通过 project_graph 定义命名子图（仅维护元信息，不拷贝数据）。子图名供后续算法调用使用。

#### 2.4.5 调用算法函数
**语法**：

```cypher
CALL algorithm_name(<graph_name>, {param1: value1, ...})
YIELD column1, column2, ...
```

**关键设计**：

+ **图名**：为已通过 project_graph 投影的子图名称，子图与算法解耦
+ **参数格式**：使用字典传递命名参数，便于扩展和解析
+ **YIELD 必选**：必须指定需要返回的列

**示例**：

```cypher
-- 先投影子图，再执行算法
CALL project_graph('my_graph', {'Person': 'n.name <> "Ira"'}, {'KNOWS': 'r.id < 3'});

CALL k_core('my_graph', {min_k: 3, concurrency: 4})
YIELD node, core_number;

CALL pagerank('page_graph', {damping: 0.85, max_iterations: 20})
YIELD node, rank;

CALL leiden('entity_graph', {resolution: 1.0})
YIELD node, community_id;
```

### 2.5 并发配置
#### 2.5.1 参数说明
| 参数 | 类型 | 范围 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `concurrency` | Integer | 0-1024 | 0 | 并发线程数 |


**行为**：

+ `0`: 自动检测，使用 `std::thread::hardware_concurrency()` 的值
+ `1`: 单线程执行（用于调试或小图以及默认配置）
+ `N`: 使用 N 个线程

---

## 3. 技术实现
本节约定子图元信息结构、子图算法的物理计划与执行接口，便于实现时对齐。

**接口一览**：

| 层次 | 接口/结构 | 作用 |
| --- | --- | --- |
| 子图元信息 (§3.1) | `VertexEntry` / `EdgeEntry` / `ProjectedSubgraph` | 描述 project_graph 定义的子图（点/边 label + predicate），不存数据；与 connection/session 绑定，仅 Compiler 使用 |
| 物理计划 (§3.2.1) | `GDSAlgo`（含 proto `Subgraph`：vertex_entries / edge_entries）、options | 表示 CALL 的算法名、子图（Schema 绑定后的 label id + Expression）与配置参数 |
| 执行接口 (§3.2.2) | `Subgraph`（C++）/ `GDSAlgoFunction` / `GDSAlgoOpr` / `algo_exec_func_t` | 运行时：GDSAlgoOpr 持 Subgraph + options + algoFunc，Eval 取图并调用算法；算法通过 algo_exec_func_t 接收 Context、Subgraph、options、StorageReadInterface |
| 访图抽象 (§3.2.3) | `GDSGraph`（含 `EdgeTriplet`、顶点/边迭代器） | 基于 StorageReadInterface + Subgraph 提供子图逻辑视图（按 predicate 过滤，不物化）；算法只依赖此接口，与存储、与具体子图定义解耦 |


### 3.1 Project SubGraph 方案
这章节讨论如何保存子图？

技术实现上需重点解决 **project subgraph** 的表示与访问方式。若将子图数据拷贝一份单独存储，会带来数据冗余与一致性问题，因此采用与 KUZU 类似的思路：**仅维护子图元信息**（点/边 label + predicate），运行时 Compiler 将这些元信息与当前 Schema 绑定，转换为全图访问接口中的 label/predicate 过滤条件，不物化子图数据。

元信息保存在 connection/session 对象中（保证一致的生命周期），并仅被 Compiler 使用，Engine 完全不感知。在真正执行图算法时，Engine 通过当前查询最新可读 Transaction 来访问子图，不接受指定版本的子图。

元信息包括：
**点元信息**：描述子图中「一类点」，即 label + 过滤条件（如 `age > 20`）。  
**边元信息**：描述子图中「一类边」，即三元组 `(src_label, edge_label, dst_label)` + 边上的过滤条件（如 `weight > 1.0`）；边的端点需落在子图点集合内（由点 label+predicate 隐式约束）。

我们目前支持的 predicate 范围是基于存储原始属性的过滤，不支持基于计算过程中某个中间值的过滤，并可以支持多个属性过滤的组合条件。

```cpp
// 子图中「一类点」的元信息：仅记录 label 与过滤表达式，不拷贝数据
struct VertexEntry {
    std::string label;      // 点 label 名称
    std::string predicate;  // 点上的过滤条件，如 "n.age > 20"
};

// 子图中「一类边」的元信息：三元组 (src, edge, dst) + 边上的过滤条件
struct EdgeEntry {
    std::string srcLabel;   // 源点 label 名称
    std::string edgeLabel;  // 边 label 名称
    std::string dstLabel;   // 目标点 label 名称
    std::string predicate;  // 边属性过滤，如 "r.weight > 1.0"
};

// 投影子图：由多类点、多类边的元信息组成，与 project_graph 语法一一对应
// graphName 用于 CALL algo(graph_name, ...) 时查找已注册的子图
class ProjectedSubgraph {
public:
    std::string graphName;
    std::vector<VertexEntry> vertexEntries;
    std::vector<EdgeEntry> edgeEntries;
};
```

**生命周期与使用流程**：

+ **生命周期**：`ProjectedSubgraph` 与 connection/session 绑定，仅在当前连接/会话内的查询中有效。
+ **编译期**：`ProjectedSubgraph` 仅被 compiler 使用。当解析到 `CALL algorithm_name(<graph_name>, {param1: value1, ...})` 时，compiler 根据 `<graph_name>` 查找对应的 vertex/edge entries，绑定 schema：将 string label 转为 label id，将 string predicate 转为 `Expression` 结构，供执行层使用。


### 3.2 Graph Algo 方案
如何支持图算法调用？例如：

```cypher
CALL k_core('my_graph', {min_k: 3, concurrency: 4})
YIELD node, core_number;
```

下面从 **Physical Plan 表示**（§3.2.1），**Engine 执行接口**（§3.2.2）和 **统一访图接口 GDSGraph** (§3.2.3) 展开。

#### 3.2.1 Physical Plan
我们将 `CALL procedure_name(args) YIELD ...` 统一翻译为 **GDSAlgo** 算子结构，对应 proto 定义如下：

```protobuf
message Subgraph {
  message VertexEntry {
    int32 label_id = 1; // 经过 Schema 绑定后的 label id
    common::Expression predicate = 2; // 经过Schema绑定后的Expression结构，确保子图中的属性在当前版本的schema中存在
  }

  message EdgeEntry {
    int32 src_label_id = 1;
    int32 edge_label_id = 2;
    int32 dst_label_id = 3;
    common::Expression predicate = 4;
  }

  repeated VertexEntry vertex_entries = 1;
  repeated EdgeEntry edge_entries = 2;
}

message GDSAlgo {
    // 算法名称
    string algo_name = 1;
    // 子图信息
    Subgraph sub_graph = 2;
    // 其他配置参数：concurrency, min_k ...
    map<string, string> options = 3;
}
```

上述 `CALL project_graph('my_graph', {'Person': 'n.name <> "Ira"'}, {'KNOWS': 'r.id < 3'}); CALL k_core('my_graph', {min_k: 3, concurrency: 4}) YIELD node, core_number;` 翻译成 PhysicalPlan 如下（`meta_data` 描述输出列类型与别名，此处为 node 与 core_number）：

```json
{
 "plan_id": 0,
 "query_plan": {
  "mode": "READ_WRITE",
  "plan": [
   {
    "opr": {
     "gds_algo": {
      "algo_name": "k_core",
      "sub_graph": {
       "vertex_entries": [
        {
         "label_id": 0,
         "predicate": {
          "func_name": "ne",
          "args": [
           {
            "var_ref": {
             "name": "n.name"
            }
           },
           {
            "literal": {
             "str_val": "Ira"
            }
           }
          ]
         }
        }
       ],
       "edge_entries": [
        {
         "src_label_id": 0,
         "edge_label_id": 0,
         "dst_label_id": 0,
         "predicate": {
          "func_name": "lt",
          "args": [
           {
            "var_ref": {
             "name": "r.id"
            }
           },
           {
            "literal": {
             "int_val": 3
            }
           }
          ]
         }
        }
       ]
      },
      "options": {
       "min_k": "3",
       "concurrency": "4"
      }
     }
    },
    "meta_data": [
     {
      "type": {
       "graph_type": {
        "element_opt": "VERTEX",
        "graph_data_type": [
         {
          "label": {
           "label": 0
          },
          "props": []
         }
        ]
       }
      },
      "alias": 0
     },
     {
      "type": {
       "data_type": {
        "primitive_type": "DT_SIGNED_INT64"
       }
      },
      "alias": 1
     }
    ]
   },
   {
    "opr": {
     "sink": {
      "tags": []
     }
    },
    "meta_data": []
   }
  ]
 }
}
```

#### 3.2.2 Engine 执行接口

```c++
// 将 protobuf Subgraph（§3.2.1）转为 C++ 运行时结构，与 proto 一一对应
class Subgraph {
 public:
  struct VertexEntry {
    int32_t label_id;               // Schema 绑定后的点 label id
    common::Expression predicate;    // Schema 绑定后的过滤表达式
  };

  struct EdgeEntry {
    int32_t src_label_id;
    int32_t edge_label_id;
    int32_t dst_label_id;
    common::Expression predicate;
  };

  std::vector<VertexEntry> vertex_entries;
  std::vector<EdgeEntry> edge_entries;

  Subgraph() = default;
  // 从 physical plan 的 proto physical::Subgraph 反序列化得到
  explicit Subgraph(const ::physical::Subgraph& proto);
};

using options_t = std::unordered_map<std::string, std::string>;

using algo_exec_func_t = std::function<execution::Context(
    execution::Context& ctx, const Subgraph &subgraph,
    const options_t &options,
    const StorageReadInterface& graph)>;

struct NEUG_API GDSAlgoFunction : public Function {
  explicit GDSAlgoFunction(std::string name) : Function{std::move(name), {}} {}
  // 每个算子需要实现自己的 execFunc 函数
  algo_exec_func_t execFunc;
};

class GDSAlgoOpr : public IOperator {
 public:
  GDSAlgoOpr(const Subgraph &subgraph,
             const options_t &options,
             function::GDSAlgoFunction* algoFunc)
      : subgraph_(subgraph),
        options_(options),
        algoFunc_(algoFunc) {}

  std::string get_operator_name() const override { return "GDSAlgoOpr"; }

  neug::result<neug::execution::Context> Eval(
      IStorageInterface& graph, const ParamsMap& params,
      neug::execution::Context&& ctx,
      neug::execution::OprTimer* timer) override;

 private:
  const Subgraph &subgraph_;
  const options_t &options_;
  function::GDSAlgoFunction* algoFunc_;
};
```

#### 3.2.3 统一访图接口 GDSGraph

我们可以进一步优化 `algo_exec_func_t` 接口：

优化后定义为：

```c++
using algo_exec_func_t = std::function<execution::Context(
    execution::Context& ctx,
    const options_t &options,
    const GDSGraph& graph)>;
```

优化前定义为：

```c++
using algo_exec_func_t = std::function<execution::Context(
    execution::Context& ctx, const Subgraph &subgraph,
    const options_t &options,
    const StorageReadInterface& graph)>;
```

我们基于 `StorageReadInterface graph` 和 `Subgraph subgraph` 提供当前 Transaction 的子图视图 `GDSGraph`。算法层不直接访问存储，而是通过 GDSGraph 从全图中按子图元信息「投影」出逻辑视图（按 predicate 过滤，不物化），使得算法与存储、与具体子图定义解耦。

```c++
class GDSGraph {
public:
    virtual ~GDSGraph() = default;

    /// 子图中包含的所有点 label（用于算法遍历所有点集）
    virtual std::vector<label_t> getVertexLabels() const = 0;

    /// 子图中包含的所有边类型（三元组列表），用于区分有向/多边类型
    virtual std::vector<EdgeTriplet> getEdgeTriplets() const = 0;

    /// 获取指定 label 下的顶点迭代器，仅包含满足该 VertexEntry.predicate 的顶点
    virtual VertexIterator getVertices(label_t vertexLabel) const = 0;

    /// 从 startVertex 出发、边类型为 edgeLabel 的所有出边；边与邻接点均需满足子图 predicate
    virtual EdgeIterator getOutgoingEdges(vid_t startVertex, label_t edgeLabel) const = 0;

    /// 以 startVertex 为终点、边类型为 edgeLabel 的所有入边；边与邻接点均需满足子图 predicate
    virtual EdgeIterator getIncomingEdges(vid_t startVertex, label_t edgeLabel) const = 0;

protected:
    const Subgraph &subgraph_;  // 子图元信息，由 project_graph 填充
    const StorageReadInterface& read_graph_; // 基于当前 Transaction 访问全图接口
};
```

**实现说明**：

+ GDSGraph 可在引擎层基于现有 `StorageReadInterface` 实现：用 `GetVertexSet(label)`、`GetGenericOutgoingGraphView` / `GetGenericIncomingGraphView` 获取全图数据，再按 `ProjectedSubgraph` 中的 `VertexEntry.predicate` 与 `EdgeEntry.predicate` 做过滤，不物化子图。
+ 若子图中同一边 label 对应多种 (src, edge, dst) 组合，可在 `getEdgeTriplets()` 中返回多个 `EdgeTriplet`；算法按需对每种 triplet 调用 `getOutgoingEdges` / `getIncomingEdges`（传入对应 edgeLabel 或扩展接口传 triplet）。

---

## 4. 开发者接口（编译扩展）

开发者应该参考 Extension 开发指南开发相应的图算法，具体可参见语雀文档：https://aliyuque.antfin.com/7br/acpom7/vaelciw1gexlsktq

---

## 5. 实现优先级
### P0 - 第一版 (MVP)
| 功能 | 描述 |
| --- | --- |
| Project Subgraph (project_graph) | 子图元信息与 GDSGraph 访图接口 |
| K-Core | 基础实现（单线程） |
| PageRank | 基础实现（单线程） |
| Connected Components | 基础实现（单线程） |
| Shortest Path | Dijkstra 实现 |
| Leiden | 社区发现（GraphRAG 核心） |
| Label Propagation | 快速社区发现 |
| Local Clustering Coefficient (LCC) | 顶点局部聚类系数 |
| INSTALL/LOAD/CALL | 基础扩展机制 |
| Linux x86_64 | 主要开发平台 |
| Linux aarch64 | ARM 服务器支持 |
| macOS arm64 | Apple Silicon 支持 |
| macOS x86_64 | Intel Mac 支持 |


### P1 - 第二版
| 功能 | 描述 |
| --- | --- |
| 并行化框架 | Morsel-Driven 并行执行 |
| concurrency 参数 | 实际生效 |


### P2 - 第三版
| 功能 | 描述 |
| --- | --- |
| 更多算法 | Betweenness Centrality, Triangle Count 等 |
| 性能优化 | 算法级别优化 |

---