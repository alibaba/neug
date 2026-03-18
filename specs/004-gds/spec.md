**Version**: 0.2.0  
**Created**: 2026-01-23  
**Status**: Draft

本文档定义 GDS 扩展的产品需求、用户接口、技术实现与实现优先级。核心设计为 **Project Subgraph**：先通过 `project_graph` 投影命名子图，再基于子图名调用图算法，实现子图与算法解耦。

**文档结构**：§1 产品需求（算法列表与 Project Subgraph 语法）→ §2 用户接口（Extension 机制与 Cypher）→ §3 技术实现（Project SubGraph 方案与 GDSGraph）→ §4 开发者接口 → §5 内置 GDS 扩展 → §6 实现优先级 → §7 附录。

---

## 1. 产品需求：MVP 算法与语法
### 1.1 选择原则

- **对齐 LDBC Graphalytics 基准**：覆盖其标准算法与数据集，便于与 Kuzu、Neo4j 等图系统做可复现的性能对比。
- **补充社区与图结构能力**：在基准六算法外，增加社区发现（如 Leiden）与稠密子图（如 K-Core），支撑 GraphRAG、知识图谱等场景。

### 1.2 V1 算法列表
第一版支持 **8 个核心算法**，分为下面两类。

#### LDBC Graphalytics Benchmark （6个）

第一类为 [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/algorithms/) 官方的6个算法。我们希望可以在这个标准测试集上，和 Kuzu, LadybugDB, Neo4j 等数据库进行性能对比。具体的算法描述，数据集，性能指标请见 [Bench](#5-benchmark) 章节。 

| 算法 | 图语义 | 描述 | 输出 | 并行化 |
| **Breadth-First Search (BFS)** | 有向 | 从源点出发的广度优先遍历，按层扩展 | `(node, distance)` | 支持 |
| **PageRank** | 有向 | 计算节点的重要性分数 | `(node, rank)` | 支持 |
| **Weakly Connected Components (WCC)** | 无向 | 弱连通分量检测 | `(node, component_id)` | 支持 |
| **Label Propagation** | 有向 | 基于标签传播的快速社区发现 | `(node, label)` | 支持 |
| **Local Clustering Coefficient (LCC)** | 有向 | 顶点局部聚类系数 | `(node, coefficient)` | 支持 |
| **Single-source shortest paths (SSSP)** | 有向 | 单源最短路径 | `(node, distance, path)` | 支持 |

#### 社区发现与图结构算法（2 个）

在 Graphalytics 六算法之外，V1 增加 K-Core 与 Leiden：Leiden 为高质量社区发现，常用于 GraphRAG 等场景的分层摘要；K-Core 用于稠密子图与核心节点分析。

| 算法 | 图语义 | 描述 | 输出 | 并行化 |
| --- | --- | --- | --- | --- |
| **K-Core** | 无向 | 找出所有核心数 ≥ k 的子图 | `(node, core_number)` | 支持 |
| **Leiden** | 无向 | 高质量社区发现（优于 Louvain） | `(node, community_id)` | 支持 |

**注意**：所有算法暂时都支持并行化，后续可以结合 benchmark 需求（采用算法，数据规模）决定是否需要支持并行。

### 1.3 图语义说明
NeuG 底层存储为**有向图**（CSR for outgoing, CSC for incoming）。算法层根据算法需求封装不同的语义：

| 图语义 | 实现方式 | 适用算法 |
| --- | --- | --- |
| **有向** | 只使用 `out_neighbors()` | BFS, PageRank, LP, LCC, SSSP |
| **无向** | 使用 `out_neighbors() ∪ in_neighbors()` | WCC, K-Core, Leiden |

**注意**：如果用户数据已经在 `out_neighbors` 中存储了双向边（模拟无向图），使用"无向"语义的算法可能会导致边重复计算。用户应确保数据存储方式与算法语义匹配。

### 1.4 算法详细说明
接下来对图算法的一些公共特性做详细说明。

#### 基于子图
所有图论算法都执行在特定的一种或多种点边类型上，我们通过 `project_graph` 将这些类型提前 project 成子图，并给这个子图一个别名，后续的图算法通过指定子图别名，仅作用在子图范围内的点边类型上。
`project_graph` 语法表示为：
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

>上述例子表示：
>从全图中筛选出Label为'person'点，且需满足 'n.name <> "Ira"'  
>从全图中筛选出Label为'KNOWS'边，端点 (src和dst) 需满足 `'Person': 'n.name <> "Ira"'` 限制，且边上的属性需满足 `'r.id < 3'`

基于子图别名进一步执行图算法，而不是将Label信息作为参数传递给算法，这样可以将子图信息和图算法本身解藕，通过两种组合支持更多功能。
```cypher
CALL algorithm_name(<graph_name>, {param1: value1, ...})
YIELD column1, column2, ...
```

#### 公共配置
图算法除了有自己语义相关的特定配置外，还有一些性能相关的公共配置。

| 配置名         | 描述     |
| -------------- | -------- |
| `concurrency`  | 并发度控制 |
| `timeout`      | 超时限制   |
| `memory_limit` | 内存限制   |

理想情况下，这些配置应作为 `execute` 的公共参数，作用于所有查询语句：

```python
conn.execute(
    "CALL pagerank('page_graph', {damping: 0.85, max_iterations: 30}) YIELD node, rank",
    options={
        "concurrency": 8,
        "timeout": 10000,
        "memory_limit": "8G"
    }
)
```

目前 `execute` 尚不支持上述参数，可暂时将公共配置作为算法 options 传入：

```cypher
CALL pagerank('page_graph', {
  damping: 0.85,
  max_iterations: 30,
  concurrency: 8,
  timeout: 10000,
  memory_limit: "8G"
})
YIELD node, rank
```

#### 与其他算子组合情况
图算法还可以与其他 relational 算子组合，满足 AP + TP 混合执行的需求。为了保证数据一致性，我们限制图算法对图存储的访问一定是只读的，也就是图算法的返回结果无法进一步写入到图存储中，但可以基于图算法结果进一步执行 Read-Only relational 操作。例如：

**基于图算法结果排序：**
```
CALL pagerank('page_graph', {damping: 0.85, max_iterations: 30, concurrency: 8})
YIELD node, rank
RETURN node, rank
ORDER BY rank DESC
LIMIT 10
```

**基于图算法结果过滤：**
```
CALL pagerank('page_graph', {damping: 0.85, max_iterations: 30, concurrency: 8})
YIELD node, rank
WHERE rank > 10.0
ORDER BY rank DESC
LIMIT 10
```

**基于图算法结果聚合：**
```
CALL pagerank('page_graph', {damping: 0.85, max_iterations: 30, concurrency: 8})
YIELD node, rank
RETURN rank, count(node) as cnt
ORDER BY cnt DESC, rank ASC
LIMIT 10
```

### 1.5 图算法具体示例

#### 1.5.1 K-Core Decomposition
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

#### 1.5.2 PageRank
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

#### 1.5.3 Shortest Path (Dijkstra)
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

#### 1.5.4 Connected Components (Weakly)
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

#### 1.5.5 Breadth-First Search (BFS)
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

#### 1.5.6 Local Clustering Coefficient (LCC)
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

#### 1.5.7 Leiden Community Detection
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

#### 1.5.8 Label Propagation
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

## 5. Benchmark

### 算法描述

以下描述与 Graphalytics 等基准规范对齐，用于 Benchmark 的可复现与对比。

#### 5.1 广度优先搜索 (BFS)

广度优先搜索是一种遍历算法，为图中每个顶点标注从给定源点（根）到该顶点的最短路径长度（或称深度）。根的深度为 0，其出边邻居深度为 1，再下一层邻居深度为 2，以此类推。不可达顶点应赋值为无穷大（表示为 9223372036854775807）。

#### 5.2 PageRank (PR)

PageRank 是一种迭代算法，为每个顶点赋予一个排序值（重要性分数），最初由 Google 搜索用于网页排序。记 PR_i(v) 为第 i 轮迭代后顶点 v 的 PageRank 值。初始时，每个顶点 v 被赋予相同的值，且所有顶点值之和为 1：

```
PR_0(v) = 1 / |V|
```

每轮迭代中，各顶点沿出边将自己的 PageRank 传递给邻居。顶点 v 的 PageRank 按以下规则更新：

```
PR_i(v) = (1-d)/|V| + d · ( Σ_{u∈N_in(v)} PR_{i-1}(u)/|N_out(u)| + (1/|V|) Σ_{w∈D} PR_{i-1}(w) )
```

其中 D = { w ∈ V | N_out(w) = ∅ } 为 **sink 顶点**集合（无出边的顶点）。前一项为来自入边邻居的贡献；后一项为 sink 顶点上一轮 PageRank 之和均匀分配给所有顶点（teleport from sinks）。计算前一项时，对 sink 顶点 u 将 PR_{i-1}(u)/|N_out(u)| 视为 0。

**Notation (English):** d ∈ [0,1] is the damping factor; D is the set of sink vertices. The term (1/|V|) Σ_{w∈D} PR_{i-1}(w) is the total PageRank of sinks redistributed uniformly to every vertex.

算法按固定迭代轮数执行。浮点数须按 64 位双精度 IEEE 754 处理。

#### 5.3 弱连通分量 (WCC)

本算法求解图的弱连通分量，并为每个顶点分配一个唯一标签，表示其所属分量。若两顶点在沿图边可互相到达，则属于同一分量并具有相同标签。对有向图允许沿边的反方向行走，即按无向图理解。

#### 5.4 基于标签传播的社区发现 (CDLP)

该社区发现算法采用标签传播 (CDLP)，基于 Raghavan 等人提出的方法。算法为每个顶点分配一个表示社区的标签，并迭代更新：每个顶点根据其邻居标签的出现频率选择新标签。本规范采用确定性、可并行的变体。

记 L_i(v) 为第 i 轮迭代后顶点 v 的标签。初始时每个顶点 v 的标签等于其标识：L_0(v) = v。

在第 i 轮中，顶点 v 统计其入边与出边邻居的标签频率，选择出现次数最多的标签。若有向图中某邻居既通过入边又通过出边可达，其标签计两次。若存在多个标签同为最大频率，则选择其中最小的标签。若顶点无邻居，则保留当前标签。

**说明：** 与原始算法相比，Graphalytics 中的 CDLP 有两点主要区别：(1) 确定性——当多个标签频率同为最大时，选择最小标签；(2) 同步更新——每轮迭代基于上一轮的标签结果计算，在二分或近二分子图中可能导致标签振荡。

#### 5.5 局部聚类系数 (LCC)

局部聚类系数算法为每个顶点计算其局部聚类系数，即该顶点邻居之间实际存在的边数与其可能存在的最大边数之比。若顶点邻居数少于 2，则定义其系数为 0：

- 当 |N(v)| ≤ 1 时：LCC(v) = 0。
- 否则：LCC(v) = |{(u,w) | u,w ∈ N(v) ∧ (u,w) ∈ E}| / |{(u,w) | u,w ∈ N(v)}|。

对有向图，邻居集合 N(v) 不考虑边的方向（每个邻居只计一次），但在统计邻居之间的边时需考虑方向（例如使用 N_out(u)）。

#### 5.6 单源最短路径 (SSSP)

单源最短路径算法为每个顶点标注从给定根顶点到该顶点的最短路径长度。路径长度为路径上各边权之和。边权为浮点数，须按 64 位双精度 IEEE 754 处理；边权非负、非无穷、非 NaN，但可为 0。不可达顶点应赋值为无穷大。

---

### 数据集合

Graphalytics 同时使用真实场景图与由数据生成器产生的合成图，覆盖多种规模与密度。数据集存放于 [SURF/CWI 数据仓库](https://repository.surfsara.nl/datasets/cwi/graphalytics) 。

#### 真实世界数据集

来自不同领域（知识图谱、游戏、社交网络等）的真实图。下表为 Graphalytics 基准使用的真实世界数据集。

| ID | Name | n | m | Scale | Domain |
| --- | --- | --- | --- | --- | --- |
| R1 (2XS) | wiki-talk | 2.39M | 5.02M | 6.9 | Knowledge |
| R2 (XS) | kgs | 0.83M | 17.9M | 7.3 | Gaming |
| R3 (XS) | cit-patents | 3.77M | 16.5M | 7.3 | Knowledge |
| R4 (S) | dota-league | 0.06M | 50.9M | 7.7 | Gaming |
| R5 (XL) | com-friendster | 65.6M | 1.81B | 9.3 | Social |
| R6 (XL) | twitter_mpi | 52.6M | 1.97B | 9.3 | Social |


#### 合成数据集（Graph500）

除真实数据集外，Graphalytics 采用 Graph500 生成器生成幂律图。下表为 Graphalytics 使用的 Graph500 合成数据集。

| ID | Name | n | m | Scale |
| --- | --- | --- | --- | --- |
| G22 (S) | Graph500-22 | 2.4M | 64.2M | 7.8 |
| G23 (M) | Graph500-23 | 4.6M | 129.3M | 8.1 |
| G24 (M) | Graph500-24 | 8.9M | 260.4M | 8.4 |
| G25 (L) | Graph500-25 | 17.0M | 523.6M | 8.7 |
| G26 (XL) | Graph500-26 | 32.8M | 1.1B | 9.0 |
| G27 (XL) | Graph500-27 | 65.6M | 2.1B | 9.3 |
| G28 (2XL) | Graph500-28 | 121M | 4.2B | 9.6 |
| G29 (2XL) | Graph500-29 | 233M | 8.5B | 9.9 |
| G30 (3XL) | Graph500-30 | 448M | 17.0B | 10.2 |


#### 合成数据集（LDBC Datagen）

Graphalytics 采用 LDBC Datagen 生成社交网络类图。下表为 Graphalytics 使用的 Datagen 合成数据集。

| ID | Name | n | m | Scale |
| --- | --- | --- | --- | --- |
| D7.5 (S) | Datagen-7.5-fb | 0.6M | 34.2M | 7.5 |
| D7.6 (S) | Datagen-7.6-fb | 0.8M | 42.2M | 7.6 |
| D7.7 (S) | Datagen-7.7-zf | 13.2M | 32.8M | 7.6 |
| D7.8 (S) | Datagen-7.8-zf | 16.5M | 41.0M | 7.7 |
| D7.9 (S) | Datagen-7.9-fb | 1.4M | 85.7M | 7.9 |
| D8.0 (M) | Datagen-8.0-fb | 1.7M | 107.5M | 8.0 |
| D8.1 (M) | Datagen-8.1-fb | 2.1M | 134.3M | 8.1 |
| D8.2 (M) | Datagen-8.2-zf | 43.7M | 106.4M | 8.1 |
| D8.3 (M) | Datagen-8.3-zf | 53.5M | 130.6M | 8.2 |
| D8.4 (M) | Datagen-8.4-fb | 3.8M | 269.5M | 8.4 |
| D8.5 (L) | Datagen-8.5-fb | 4.6M | 332.0M | 8.5 |
| D8.6 (L) | Datagen-8.6-fb | 5.7M | 422.0M | 8.6 |
| D8.7 (L) | Datagen-8.7-zf | 145.1M | 340.2M | 8.6 |
| D8.8 (L) | Datagen-8.8-zf | 168.3M | 413.4M | 8.7 |
| D8.9 (L) | Datagen-8.9-fb | 10.6M | 848.7M | 8.9 |
| D9.0 (XL) | Datagen-9.0-fb | 12.9M | 1.0B | 9.0 |
| D9.1 (XL) | Datagen-9.1-fb | 16.1M | 1.3B | 9.1 |
| D9.2 (XL) | Datagen-9.2-zf | 434.9M | 1.0B | 9.1 |
| D9.3 (XL) | Datagen-9.3-zf | 555.3M | 13.1B | 9.2 |
| D9.4 (XL) | Datagen-9.4-fb | 29.3M | 2.6B | 9.4 |
| D-3k (XL) | Datagen-sf3k-fb | 33.5M | 2.9B | 9.4 |
| D-10k (2XL) | Datagen-sf10k-fb | 100.2M | 9.4B | 9.9 |


每个 Graphalytics 工作负载由「在单个数据集上执行单个算法」组成。算法实现无强制约束，只要其正确性可通过与参考输出对比进行验证即可。

### 性能指标

本节描述 Graphalytics 使用的度量指标。基准通过多种指标量化被测系统的性能及其他特性，其中性能主要由基准执行过程中各阶段所耗时间衡量。Graphalytics 报告性能指标、吞吐指标、成本指标与比率指标等，此处重点说明**性能指标**。

性能指标用于报告各平台操作的执行时间：

- **加载时间 (T_L)**，单位秒：将指定图加载到被测系统所花费的时间，包括将输入图转换为系统内部格式所需的预处理。该阶段在每个图上的所有算法执行前执行一次。

- **完成时间 / Makespan (T_M)**，单位秒：从 Graphalytics 驱动发出「在已加载的图上执行某算法」的命令，到算法输出可供驱动使用之间的时间。Makespan 可进一步拆分为处理时间与平台开销。该指标对应**冷启动**场景：系统启动后，对单数据集执行单算法，然后关闭。

- **处理时间 (T_D)**，单位秒：实际执行算法所需的时间，不包含平台相关开销（如资源分配、从文件系统加载图、图划分等）。该指标对应**已就绪、生产态**图处理系统：从文件系统加载图与图划分通常只做一次且与算法无关，故不计入处理时间。

执行时间以各基准配置的超时时长为上限。一旦达到超时，图处理任务将被终止，并上报该超时时长作为对应性能指标。

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