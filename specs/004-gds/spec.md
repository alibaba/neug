**Version**: 2.0  
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


#### AI/GraphRAG 刚需算法（3 个）
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
INSTALL EXTENSION 'extension_name';
```

**说明**：

+ 从 NeuG 官方仓库下载当前平台对应的 **.so** 包并安装扩展
+ 扩展会被安装到 `$NEUG_HOME/extensions/` 目录
+ 安装后需要 `LOAD` 才能使用

**示例**：

```cypher
-- 安装图算法扩展
INSTALL EXTENSION 'gds';
```

#### 2.4.2 LOAD EXTENSION
**语法**：

```cypher
LOAD EXTENSION 'extension_name';
```

**说明**：

+ 加载已安装的扩展到当前会话
+ 加载后扩展中的函数可用
+ 可以在配置文件中设置自动加载

**示例**：

```cypher
-- 加载图算法扩展
LOAD EXTENSION 'gds';
```

#### 2.4.3 UNLOAD EXTENSION
**语法**：

```cypher
UNLOAD EXTENSION 'extension_name';
```

**说明**：

+ 从当前会话卸载扩展
+ 扩展中的函数不再可用

#### 2.4.4 SHOW EXTENSIONS
**语法**：

```cypher
SHOW EXTENSIONS;
```

**输出**：

| name | version | loaded | description |
| --- | --- | --- | --- |
| gds | 1.0.0 | true | Graph Data Science algorithms |


#### 2.4.5 project_graph（投影子图）
**语法**：

```cypher
CALL project_graph(
    <GRAPH_NAME>,
    { <NODE_TABLE_0> : <NODE_PREDICATE_0>, ... },
    { <REL_TABLE_0> : <REL_PREDICATE_0>, ... }
);
```

**说明**：在扩展加载后，通过 project_graph 定义命名子图（仅维护元信息，不拷贝数据）。子图名供后续算法调用使用。

#### 2.4.6 调用算法函数
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

#### 2.5.2 实现评估
**P0 (第一版实现)**：

+ 单线程串行执行
+ `concurrency` 参数接受但忽略（打印 warning）

**P1 (第二版实现)**：

+ 基于 Morsel-Driven 的并行执行
+ 支持 `concurrency` 参数

**评估依据**：

+ 第一版优先保证功能正确性和 API 稳定性
+ 并行化需要额外的测试和验证
+ 大规模图测试需要时间

---

## 3. 技术实现
本节约定子图元信息结构、GDS 访图接口、以及 CALL 的物理计划与执行接口，便于实现时对齐。

**接口一览**：

| 层次 | 接口/结构 | 作用 |
| --- | --- | --- |
| 子图元信息 | `VertexEntry` / `EdgeEntry` / `ProjectedSubgraph` | 描述 project_graph 定义的子图（点/边 label + predicate），不存数据 |
| 访图抽象 | `GDSGraph`（含 `EdgeTriplet`、顶点/边迭代器） | 算法只依赖此接口从全图逻辑投影子图，与存储解耦 |
| 物理计划 | `procedure_call.query`（Query + Argument） | 表示 CALL 的算法名与参数列表（含 graph_name） |
| 执行扩展 | `CallFuncInputBase` / `NeugCallFunction` / `ProcedureCallOpr` | bind 解析参数 → exec 取 GDSGraph 并跑算法，结果写 Context |


### 3.1 Project SubGraph 方案

技术实现上需重点解决 **project subgraph** 的表示与访问方式。若将子图数据拷贝一份单独存储，会带来数据冗余与一致性问题，因此采用与 KUZU 类似的思路：**仅维护子图元信息**（点/边 label + predicate），运行时从全图按需扫描并应用过滤，不物化子图数据。

**点元信息**：描述子图中「一类点」，即 label + 过滤条件（如 `age > 20`）。  
**边元信息**：描述子图中「一类边」，即三元组 `(src_label, edge_label, dst_label)` + 边上的过滤条件（如 `weight > 1.0`）；边的端点需落在子图点集合内（由点 predicate 隐式约束）。

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

下面从 **Physical Plan 表示**（§3.2.1）展开。

#### 3.2.1 Physical Plan
我们将 `CALL procedure_name(args) YIELD ...` 统一翻译为 **GDSAlgo** 算子，对应 proto 定义如下：

```protobuf
message SubGraph {
  message VertexEntry {
    int32 label_id = 1;
    common::Expression predicate = 2;
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
    string algo_name = 1;
    SubGraph sub_graph = 2;
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
       "graph_name": "my_graph",
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

---

## 4. 开发者接口（编译扩展）
### 4.1 扩展项目结构
推荐按以下方式组织单个 extension：

```plain
extension/
├── CMakeLists.txt                    # 扩展总入口，通过 add_extension_if_enabled 按需加入子目录
└── <extension_name>/                 # 例如 json
    ├── CMakeLists.txt                # 收集源文件、调用 build_extension_lib、设置 include/link
    ├── include/                      # 头文件（可选，按需）
    │   ├── xxx.h
    │   └── ...
    └── src/                          # 实现与入口
        ├── <extension_name>_extension.cpp   # 必须：实现 Init / Name 等 C 接口
        └── ...
```

**json extension 示例：**

```plain
extension/json/
├── CMakeLists.txt
├── include/
│   ├── json_dataset_builder.h
│   ├── json_options.h
│   └── json_read_function.h
└── src/
    ├── json_extension.cpp      # 入口：Init()、Name()、注册函数与 ExtensionInfo
    ├── json_dataset_builder.cc
    └── json_options.cc
```

### 4.2 添加 CMake 配置
#### extension/CMakeLists.txt（根）
+ `add_extension_if_enabled(extension)`  
当 `extension` 在 `BUILD_EXTENSIONS` 列表中时，才 `add_subdirectory(extension)`，避免未选中的扩展参与构建。  
例如：`add_extension_if_enabled("json")`。
+ `build_extension_lib(ext_name)`  
在子目录 `extension/<ext_name>/CMakeLists.txt` 中调用。要求该子目录已把要编译的源文件列表写入变量 `<EXT_NAME>_EXTENSION_OBJECT_FILES`（`<EXT_NAME>` 为 `ext_name` 的大写，如 `json` → `JSON_EXTENSION_OBJECT_FILES`）。  
该函数会：  
    - 用这些源文件创建 SHARED 库 `neug_<ext_name>_extension`  
    - 通过 `set_extension_properties` 设置输出名为 `lib<ext_name>.neug_extension`、输出目录等
+ **输出目录规则（set_extension_properties）**  
    - 若已设置 `CMAKE_LIBRARY_OUTPUT_DIRECTORY`（如 Python 构建）：扩展库输出到该目录下的 `extension/<ext_name>/`。  
    - 否则（独立 CMake 构建）：输出到 `CMAKE_BINARY_DIR` 下对应位置。  
最终产物：`lib<ext_name>.neug_extension`。

#### extension/<extension_name>/CMakeLists.txt
1. **收集源文件**  
将本扩展需要参与编译的源文件放入变量 `<EXTENSION_NAME>_EXTENSION_OBJECT_FILES`（`<EXTENSION_NAME>` 为扩展名大写，如 `JSON_EXTENSION_OBJECT_FILES`）。
2. **调用 **`build_extension_lib(<extension_name>)`  
必须在此子目录中调用，且应在设置好源文件变量之后、`add_subdirectory(test)` 之前。
3. **配置 include 与 link**  
对 target `neug_<extension_name>_extension` 使用 `target_include_directories`、`target_link_libraries` 等，引入头文件路径和依赖（如 `neug_libraries`、Arrow 相关 target 等）。

**json 示例（extension/json/CMakeLists.txt）：**

```cmake
# 1. 收集源文件
file(GLOB JSON_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
set(JSON_EXTENSION_OBJECT_FILES ${JSON_EXTENSION_SOURCES})

# 2. 构建扩展动态库（生成 libjson.neug_extension）
build_extension_lib("json")

# 3. 头文件与依赖
target_include_directories(neug_json_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR})
if(ARROW_INCLUDE_DIRS)
    target_include_directories(neug_json_extension PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
endif()
target_include_directories(neug_json_extension PRIVATE SYSTEM ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

target_link_libraries(neug_json_extension PRIVATE
    arrow_json
    arrow_dataset_objlib
    neug_libraries
)
```

### 4.3 算法注册
动态库被加载时，会通过 `dlopen` 查找并调用以下 **C 链接、extern "C"** 的符号（具体名称以 `include/neug/compiler/extension/extension.h` 中 `ExtensionLibLoader` 常量为准，一般为小写 `init` / `name`；部分加载路径可能使用 `Init` / `Name`，实现时需与当前加载方式一致）：

+ `void init(main::ClientContext* context)`（或兼容的 `Init()`）  
扩展初始化入口。在实现中通常：
    - 使用 `neug::extension::ExtensionAPI::registerFunction<T>()` 注册表函数（如 `JSON_SCAN`）。
    - 使用 `ExtensionAPI::registerFunctionAlias<T>()` 注册别名（如 `JSONL_SCAN`）。
    - 调用 `ExtensionAPI::registerExtension(ExtensionInfo{...})` 注册扩展元信息（名称与描述）。
+ `const char* name()`（或兼容的 `Name()`）  
返回扩展的显示名称，用于展示在“已加载扩展”等列表中。

头文件与 API 定义：

+ `neug/compiler/extension/extension_api.h`：`ExtensionAPI::registerFunction`、`registerFunctionAlias`、`registerExtension`、`ExtensionInfo`。
+ `neug/compiler/extension/extension.h`：`ExtensionLibLoader` 所需符号名、`ext_init_func_t` / `ext_name_func_t` 等。

**json 入口示例（json_extension.cpp 节选）：**

```cpp
#include "neug/compiler/extension/extension_api.h"
#include "json_read_function.h"

extern "C" {

void Init() {
    neug::extension::ExtensionAPI::registerFunction<
        neug::function::JsonReadFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    neug::extension::ExtensionAPI::registerFunctionAlias<
        neug::function::JsonLReadFunction>(
        neug::catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY);
    neug::extension::ExtensionAPI::registerExtension(
        neug::extension::ExtensionInfo{
            "json", "Provides functions to read and write JSON files."});
}

const char* Name() { return "JSON"; }

}
```

### 4.4 编译与本地测试
#### 使用脚本一键构建与上传
使用 `scripts/build_and_upload_extensions.sh` 可完成：构建指定扩展 → 打包 → 计算校验和 → 可选上传 OSS。

| 选项 | 说明 | 默认值 | 示例 |
| --- | --- | --- | --- |
| `--extensions` | 要构建的扩展列表，分号分隔 | `json` | `--extensions "json"` 或 `--extensions "json;parquet"` |
| `--version` | 版本标签，用于 OSS 路径等 | `0.1.0` | `--version "0.1.0"` |
| `--platform` | 平台标识 | `linux_x86_64` | 可选：`linux_arm64`, `osx_x86_64`, `osx_arm64` 等 |
| `--workspace` | 仓库根目录 | 当前目录 | `--workspace /path/to/neug` |
| `--skip-build` | 跳过构建，仅打包/上传 | false | `--skip-build` |
| `--skip-upload` | 跳过 OSS 上传 | false | `--skip-upload` |
| `--help` | 打印用法 | — | `--help` |


#### 上传 OSS 所需环境变量
上传到 OSS 前需配置：

```bash
export OSS_ACCESS_KEY_ID=<access_id>
export OSS_ACCESS_KEY_SECRET=<access_key>
export OSS_ENDPOINT=<endpoint>
export OSS_BUCKET_NAME=<bucket_name>
```

未配置时脚本会跳过上传并提示。

#### 本地仅构建（不打包上传）
在仓库根目录：

```bash
export BUILD_EXTENSIONS=json
cd tools/python_bind
make build
```

或使用 CMake 直接配置：

```bash
mkdir -p build && cd build
cmake .. -DBUILD_EXTENSIONS=json
make -j
```

扩展库输出路径由 `set_extension_properties` 决定（见上文），一般为 `build/extension/json/libjson.neug_extension` 或 Python 构建目录下对应位置。

---

## 5. 内置 GDS 扩展
### 5.1 使用流程
```cypher
-- Step 1: 安装（首次）
INSTALL EXTENSION 'gds';

-- Step 2: 加载（每次启动或需要时）
LOAD EXTENSION 'gds';

-- Step 3: 投影子图（按需）
CALL project_graph('my_graph', {'Person': 'true'}, {'KNOWS': 'true'});

-- Step 4: 调用算法
CALL k_core('my_graph', {min_k: 3})
YIELD node, core_number;

CALL leiden('entity_graph', {})
YIELD node, community_id;
```

---

## 6. 实现优先级
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