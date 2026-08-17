支持子图隔离功能

## 什么是子图隔离功能？

用户希望在产品界面中支持用户圈选部分节点形成一个独立子图（Namespace），后续所有查询、遍历、分析操作都限定在该子图范围内。

例如，用户圈选一批节点：

```
Node A
Node B
Node C
```

系统自动形成：
```
Namespace: user1_subgraph

Nodes:
A
B
C

Edges:
A -> B
B -> C
```

独立子图中具体包含以下数据：
- 一组节点集合
- 节点之间形成的内部边集合，例如：

  ```
  Node A ∈ Namespace
  Node B ∈ Namespace

  Edge(A -> B) ∈ Namespace
  Edge(B <- A) ∈ Namespace
  ```

经过前面讨论，我们目前倾向于使用 **逻辑子图投影** 方案支持该功能：
- 不真正物化子图数据，物理只存一份原图数据
- 保存逻辑子图视图
- 在查询阶段，通过逻辑子图中的点边类型 + 属性过滤条件来筛选子图数据
- 用户层需要配合做一些数据预处理，将不同 domain / user group 中的数据通过统一的属性列标注出来，例如增加统一属性 `domain STRING` 用于区分不同用户空间。

基于 Call project_graph + GraphEntrySet 持久化 + Compiler dot 语法支持子图隔离功能。

## Call Project Graph

目前我们已经支持该功能：

```cypher
CALL project_graph(
    'user1_subgraph',
    {
	 'Entity': 'n.domain = "user1"',
	 'Product': 'n.domain = "user1"'
    },
    [
	 '[Entity, rel_ee, Entity]',
	 '[Entity, rel_ep, Product]'
    ]
);
```

分成三个部分的定义：
- 指定子图名称为 'user1_subgraph'
- 指定多个点类型以及各自的属性过滤条件：`'Enitity': 'n.domain = "user1"'`, `'Product': 'n.domain = "user1"'`
- 指定多个三元组边类型：`'[Entity, rel_ee, Entity]'`, `'[Entity, rel_ep, Product]'`

### 实现

目前该功能主要通过 Compiler 侧 bind 接口完成，我们需要真正在 engine 层实现该功能，这样才可以将 GraphEntry 结果进一步持久化到 database 中。

需要实现 ProjectGraphFunction 中的 execFunc 接口，在 engine 侧调用并且在 graph_interface 中注册 GraphEntry

## GraphEntrySet 持久化

GraphEntrySet 持久化在 Schema 中，并通过 Catalog 向 Compiler 提供当前 snapshot 的查询接口；MetadataManager 不保存独立副本。

### 基本结构

#### GraphEntry

存储单个子图的基本结构，基本结构存储的内容都是用户指定的 string 字段，不是经过 schema 绑定之后的元数据。

```c++
struct GraphEntry {
  std::vector<VertexEntryInfo> vertexInfos;
  std::vector<EdgeEntryInfo> edgeInfos;
};

struct VertexEntryInfo {
  std::string labelName;
  std::string predicate;
};

struct EdgeEntryInfo {
  std::string srcLabelName;
  std::string edgeLabelName;
  std::string dstLabelName;
  std::string predicate;
};
```

例如当用户执行：

```cypher
CALL project_graph(
    'user1_subgraph',
    {
	 'Entity': 'n.domain = "user1"',
	 'Product': 'n.domain = "user1"'
    },
    [
	 '[Entity, rel_ee, Entity]',
	 '[Entity, rel_ep, Product]'
    ]
);
```

会构造保存这些别名的 GraphEntry 结构：

```c++
GraphEntry {

 vertexInfos:
 [
   {
     labelName: "Entity",
     predicate: "domain=\"user1\""
   },

   {
     labelName: "Product",
     predicate: "domain=\"user1\""
   }
 ]

 edgeInfos:
 [
   {
     srcLabelName: "Entity",
     edgeLabelName: "rel_ee",
     dstLabelName: "Entity"
   },
   {
     srcLabelName: "Entity",
     edgeLabelName: "rel_ep",
     dstLabelName: "Product"
   }
 ]

};
```

#### GraphEntrySet

GraphEntrySet 用于统一维护所有已 project 子图，提供查找/新增/删除接口。另外提供 ToYaml/FromYaml 用于序列化和反序列化。

```c++
class GraphEntrySet : Module {
 public:
  result<ProjectedGraphEntry*> GetEntry(const std::string& name);
  result<const ProjectedGraphEntry*> GetEntry(const std::string& name) const;
  Status AddEntry(const std::string& name,
                  const ProjectedGraphEntry& entry);
  Status DropEntry(const std::string& name);

  result<YAML::Node> ToYaml();

  static void FromYaml(const YAML::Node &node);

 private:
  std::unordered_map<std::string, GraphEntry> nameToEntry;
};
```

### Schema

我们在 Schema 结构内保存 `GraphEntrySet`，后续提供给 Compiler 访问。并且提供 AddEntry/DropEntry/GetEntry 的访问接口，以及在 Schema 序列和反序列化阶段增加对 GraphEntrySet 处理。

```c++
class Schema {
public:
  result<ProjectedGraphEntry*> GetGraphEntry(const std::string& name);
  result<const ProjectedGraphEntry*> GetGraphEntry(
      const std::string& name) const;
  Status AddGraphEntry(const std::string& name,
                       const ProjectedGraphEntry& entry);
  Status DropGraphEntry(const std::string& name);
  // 在 LoadFromYaml 和 DumpToYaml 中提供对 GraphEntrySet 序列化和反序列化支持
private:
	GraphEntrySet entry_set;
};
```

### GraphInterface

我们在 GraphInterface 中提供新增和删除子图的相关接口。

```c++
class StorageUpdateInterface {
public:
  Status AddGraphEntry(const std::string& name,
                       const ProjectedGraphEntry& entry);
  Status DropGraphEntry(const std::string& name);
};
```

## Compiler 通过 dot 语法调用

### GOptPlanner

通过 Schema 将 GraphEntrySet 进一步传递给 Compiler，用于在查询编译阶段绑定子图 Schema。

```c++
class GOptPlanner {
public:
  virtual result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query,
	  const Schema* schema,
      const GraphStats& stats) override;
}
```

### 查询指定 Namespace 下的数据

通过 `<namespace>.<label>` 表示特定子图内的特定 label，`<namespace>` 限定了只能访问当前子图内的所有点边，否则报错。例如 `user1_subgraph.Person` 则会报错，因为 `Person` 是不属于当前子图内的点类型。

```cypher
MATCH (n:user1_subgraph.Entity)
WITH n,
     vector_distance_ip(n.embedding, <query_embedding>) AS score
ORDER BY score DESC
LIMIT 20
MATCH (n)-[r:user1_subgraph.rel_ep]->(p:user1_subgraph.Product)
RETURN n.uid,
       n.name,
       score,
       p.name AS related_product,
       r.rel_type,
       r.content
ORDER BY score DESC;
```

如果不指定 `<namespace>` 则默认代表原图作用域，比如 `MATCH (n:Entity)` 会匹配原先点表中的所有数据。

### 查询 Namespace 下全部类型

我们通过 `user1_subgraph.*` 语法表示当前 namespace `user1_subgraph` 下的所有点类型或者边类型，例如：

```cypher
MATCH (n:user1_subgraph.*)-[r:user1_subgraph.*]->(p:user1_subgraph.*)
RETURN count(n);
```

## 实现方案

### 1. 目标与边界

本方案实现的是逻辑子图（Namespace），不复制点边数据。Namespace 只保存：

- Namespace 名称；
- 允许访问的点类型及各点类型的过滤谓词；
- 允许访问的边三元组及各边三元组的过滤谓词。

查询 Namespace 时必须同时满足“类型属于 Namespace”和“数据满足谓词”两个条件。边是否属于 Namespace 由完整三元组 `(src label, edge label, dst label)` 判断，不能只按 edge label 判断，因为同名边类型可能连接不同的点类型。

第一阶段只允许在 `MATCH` clause（包括 `OPTIONAL MATCH`）的 graph pattern 中使用 Namespace 限定，包括普通点边模式、匿名变量和 `namespace.*`。`CREATE`、`MERGE` 以及 pattern expression 等其他包含 graph pattern 的语法位置暂不支持 `<namespace>.<label>`。`SET`、`DELETE` 本身不包含 label pattern，因此不在 dot 语法的判断入口；如果同一语句前面有合法的 `MATCH`，Namespace 限定仍然属于 `MATCH` clause。是否进一步禁止通过 Namespace 匹配结果执行写操作属于独立的产品语义，不在本期 dot 语法限制中隐式扩大范围。

GDS 函数继续复用同一份 Namespace 定义。现有 `project_graph` Cypher 接口已经支持点谓词 map 和边谓词 map，本期不新增、不改变参数形态，只调整其元数据落库和执行时机。

### 2. 分层与依赖方向

持久化结构放在 storage 层，建议新增：

```text
include/neug/storages/graph/graph_entry.h
src/storages/graph/graph_entry.cc
```

`Schema` 可以依赖该结构；compiler 层读取并绑定它。不能把持久化结构继续放在 `compiler/graph/graph_entry.h`，否则 storage 会反向依赖 compiler，破坏当前依赖方向。

compiler 中现有的 `GraphEntry` 保留为“绑定后的运行时结构”，其中包含 `SchemaEntry*` 和已绑定的表达式。为避免概念混淆，持久化结构命名如下：

```c++
struct VertexEntryInfo {
  std::string labelName;
  std::string predicate;
};

struct EdgeEntryInfo {
  std::string srcLabelName;
  std::string edgeLabelName;
  std::string dstLabelName;
  std::string predicate;
};

struct ProjectedGraphEntry {
  std::vector<VertexEntryInfo> vertexInfos;
  std::vector<EdgeEntryInfo> edgeInfos;
};

class GraphEntrySet {
 public:
  bool HasEntry(const std::string& name) const;
  result<ProjectedGraphEntry*> GetEntry(const std::string& name);
  result<const ProjectedGraphEntry*> GetEntry(const std::string& name) const;
  Status AddEntry(const std::string& name,
                  const ProjectedGraphEntry& entry);
  Status DropEntry(const std::string& name);
  result<YAML::Node> ToYaml() const;
  static result<GraphEntrySet> FromYaml(const YAML::Node& node);
};
```

这里不持久化 label id、`SchemaEntry*` 或绑定表达式。它们依赖当前 Schema，数据库重开或 DDL 后可能变化，必须在每次编译查询时重新绑定。

### 3. Schema 集成与持久化格式

`Schema` 增加 `GraphEntrySet graph_entry_set_`，并提供只读查询和受控变更接口：

```c++
result<ProjectedGraphEntry*> GetGraphEntry(const std::string& name);
result<const ProjectedGraphEntry*> GetGraphEntry(
    const std::string& name) const;
Status AddGraphEntry(const std::string& name,
                     const ProjectedGraphEntry& entry);
Status DropGraphEntry(const std::string& name);
```

上述接口不直接抛异常。不存在、重复名称和非法 entry 都通过
`result`/`Status` 返回给 Schema、Catalog、compiler 或执行层处理。

`Clear`、`Clone`、`Compact`、`StripTemporary`、`Equals` 都必须明确处理 `GraphEntrySet`。其中：

- `Clone`、`Compact` 保留 Namespace；
- `Clear` 清空 Namespace；
- `StripTemporary` 删除引用 temporary label 的 Namespace，不能把无法恢复的 Namespace 写入 checkpoint；
- `Equals` 比较 Namespace，保证 schema round-trip 测试能发现元数据丢失。

YAML 顶层新增可选字段 `projected_graphs`，旧 checkpoint 没有该字段时按空集合加载，以保证向后兼容：

```yaml
projected_graphs:
  user1_subgraph:
    vertices:
      - label: Entity
        predicate: 'n.domain = "user1"'
      - label: Product
        predicate: 'n.domain = "user1"'
    edges:
      - src: Entity
        label: rel_ee
        dst: Entity
        predicate: ''
      - src: Entity
        label: rel_ep
        dst: Product
        predicate: ''
```

序列化时 Namespace 名称按字典序输出，点类型按 `label`、边类型按 `(src, label, dst)` 排序，使 checkpoint 和测试结果稳定。反序列化必须拒绝字段类型错误、重复 Namespace、重复点定义和重复边三元组。

### 4. `project_graph` 的执行语义

当前 `bindProjectGraph` 直接修改 `MetadataManager::GraphEntrySet`，而 `execFunc` 是空实现。这会导致：

- `EXPLAIN CALL project_graph(...)` 也可能产生副作用；
- 编译成功但执行失败时元数据已被修改；
- 数据只存在于 compiler 内存，无法进入事务和 checkpoint。

修改后 bind 和 exec 的职责如下：

1. bind 阶段解析三个参数并做只读校验；
2. 将 Namespace 名称和 `ProjectedGraphEntry` 放入 `ProjectGraphCallInput`；
3. exec 阶段通过 `StorageUpdateInterface::AddGraphEntry` 修改事务内 Schema；
4. 成功后标记 schema/planning generation 已变化，使后续查询重新编译；
5. 事务提交后 Namespace 与 Schema 一同发布，事务回滚则不产生可见变化。

`drop_projected_graph` 使用相同流程，通过 `StorageUpdateInterface::DropGraphEntry` 删除。

接口建议为：

```c++
class StorageUpdateInterface {
 public:
  Status AddGraphEntry(const std::string& name,
                       const ProjectedGraphEntry& entry);
  Status DropGraphEntry(const std::string& name);
};
```

TP 的实现修改 COW Schema 并记录 WAL；AP 的实现修改 AP 图的 mutable Schema。两条路径都必须调用 `MarkSchemaDirty()`，不能只更新全局 `MetadataRegistry`。

#### 保持现有 Cypher 接口

保留现有 GDS 调用形式：

```cypher
CALL project_graph('g', ['Person'], {'[Person, KNOWS, Person]': ''});
```

现有接口已经能够接收点谓词 map：

```cypher
CALL project_graph(
  'g',
  {'Person': 'n.domain = "user1"'},
  {'[Person, KNOWS, Person]': 'r.weight > 0'}
);
```

这里不需要增加新的 overload 或参数兼容逻辑。实现只需沿用当前 `extractGraphEntryTableInfos` 已支持的 list/map/struct 解析结果，将三元组字符串在入口处解析一次，存储时拆成三个字段。名称前后空格要去除，空名称、非三个元素和非法类型应返回清晰的 binder 错误。

#### 创建时校验

创建 Namespace 时必须校验：

- Namespace 名称非空且不存在；
- 点 label 存在且确实是点类型；
- 边三元组存在，且 src/dst 与 Schema 一致；
- 边的两端点都在当前 Namespace 的点集合中；
- 点、边谓词可以绑定且结果可隐式转换为 BOOLEAN；
- 谓词只能引用当前点或边的属性，不允许聚合、子查询、参数和非确定性函数；
- 同一点 label 或同一边三元组不能重复定义。

谓词以用户原始字符串持久化，但创建时必须完成一次完整绑定校验。后续查询编译仍需重新绑定，以适配数据库重开及 Schema 版本变化。

### 5. WAL、checkpoint 与恢复

仅把 Namespace 写入 checkpoint 不足以保证事务语义；checkpoint 之前发生的提交还必须可通过 WAL 恢复。新增两种 schema redo：

```text
AddGraphEntryRedo(name, ProjectedGraphEntry)
DropGraphEntryRedo(name)
```

执行顺序为：修改事务 COW Schema、写入 redo、提交时发布新 snapshot。WAL replay 调用 `PropertyGraph::mutable_schema()` 执行相同的 Add/Drop，并标记 Schema dirty。

checkpoint 使用已有 `Schema::DumpToYaml`，重开时由 `Schema::LoadFromYamlNode` 恢复。compiler 通过当前 snapshot 的 `Catalog` 透传 `Schema::HasGraphEntry/GetGraphEntry/GetGraphEntryNames`，`MetadataManager` 不再持有或复制 `GraphEntrySet`。

### 6. dot 语法

ANTLR grammar 中为点和边 label 增加限定名：

```antlr
nEUG_NamespacedLabel
    : oC_SchemaName (SP? '.' SP? (oC_SchemaName | '*'))?
    ;

oC_LabelName
    : nEUG_NamespacedLabel
    ;

oC_RelTypeName
    : nEUG_NamespacedLabel
    ;
```

Transformer 不把限定名压平成普通 table name，而是传递结构化信息：

```c++
struct QualifiedLabelName {
  std::optional<std::string> graphName;
  std::string labelName;
  bool wildcard = false;
};
```

这可以避免依赖字符串切分，并正确支持反引号名称。例如 `` `user.1`.`Person.Type` `` 中的点属于标识符内容，不能被当作 Namespace 分隔符。

语义如下：

- `:Entity`：原图上的 `Entity`，行为与现在一致；
- `:user1_subgraph.Entity`：Namespace 内指定点/边类型；
- `:user1_subgraph.*`：Namespace 内所有点类型或所有边三元组；
- Namespace 不存在：报 `Projected graph '...' does not exist`；
- label 不属于 Namespace：报 `Label '...' is not part of projected graph '...'`；
- 同一个模式元素指定多个 label 时，所有限定 label 必须属于同一作用域，不允许混用原图和 Namespace，也不允许混用两个 Namespace。

#### 仅在 MATCH 中启用

grammar 只能负责识别限定名，不能仅靠 grammar 限制 clause，因为 `MATCH`、`CREATE`、`MERGE` 和 pattern expression 会复用相同的 node/relationship pattern 规则。是否允许 Namespace 必须由 Binder 根据调用上下文决定。

当前 `bindGraphPattern` 的调用点至少包括：

```text
bindMatchClause             -> MATCH / OPTIONAL MATCH
bindInsertClause            -> CREATE
bindMergeClause             -> MERGE
bindSubqueryExpression      -> pattern expression / 子查询表达式
```

同时，`bindQueryNode`、`createQueryNode` 最终统一调用 `bindNodeTableEntries`，`bindQueryRel` 统一调用 `bindRelTableEntries`。因此不能直接在这两个通用函数中无条件启用 Namespace，否则 CREATE、MERGE 等路径也会自动获得支持。

建议显式传递绑定策略，而不是依赖 Binder 的隐式全局状态：

```c++
enum class NamespaceBindingMode {
  DISALLOW,
  ALLOW_FOR_MATCH,
};

BoundGraphPattern bindGraphPattern(
    const std::vector<PatternElement>& pattern,
    NamespaceBindingMode namespaceMode = NamespaceBindingMode::DISALLOW);
```

调用规则为：

- `bindMatchClause` 传 `ALLOW_FOR_MATCH`；
- `bindInsertClause`、`bindMergeClause`、`bindSubqueryExpression` 使用默认的 `DISALLOW`；
- 只有明确支持 Namespace 的 graph-pattern 调用点才传 `ALLOW_FOR_MATCH`。

该 mode 继续向下传给 `bindPatternElement`、`bindQueryNode`、`bindQueryRel`、`bindNodeTableEntries` 和 `bindRelTableEntries`。在 `DISALLOW` 模式下遇到 qualified label 时，返回明确错误：

```text
Namespace-qualified labels are only supported in MATCH clauses.
```

普通未限定 label 在所有模式下保持现有行为。这样既保留 table-entry 绑定函数的通用性，也在最靠近语义入口的位置明确控制能力边界。

### 7. 编译绑定与谓词注入

`Binder::bindNodeTableEntries` 和 `bindRelTableEntries` 扩展为接收 `QualifiedLabelName`。绑定过程分两步：

1. 根据 Namespace 定义缩小候选 `SchemaEntry` 集合；
2. 将 Namespace 中对应的谓词绑定到当前模式变量，并与用户 `WHERE` 条件做 AND 合并。

例如：

```cypher
MATCH (n:user1_subgraph.Entity)
WHERE n.status = 'active'
RETURN n
```

逻辑上重写为：

```cypher
MATCH (n:Entity)
WHERE (n.domain = 'user1') AND (n.status = 'active')
RETURN n
```

谓词以原始字符串保存，并在当前 Binder scope 中重新绑定，因为创建 Namespace 时绑定出的 expression 包含当时的 variable unique name，不能跨查询直接复用。点谓词沿用现有 `project_graph` 接口，使用 `n.<property>`；边谓词使用 `r.<property>`，不额外改写谓词文本。

边模式需要同时注入三类约束：

- 边三元组必须属于 Namespace；
- 边自身谓词；
- 左右端点各自的点谓词。

端点谓词即使端点使用原图 label 或没有显式 label，也必须注入，否则下面的查询会从 Namespace 内的边穿越到 Namespace 外的点：

```cypher
MATCH (n:user1_subgraph.Entity)-[r:user1_subgraph.rel_ep]->(p)
RETURN p;
```

绑定器应根据 `rel_ep` 的 Namespace 三元组反推 `p` 的候选点类型，并给 `p` 注入 `Product` 的谓词。若 wildcard 边包含多个三元组，则候选端点和谓词按三元组构造成 OR 分支，不能简单把所有点谓词 AND 在一起。例如：

```text
(type(r)=rel_ee AND src.domain=user1 AND dst.domain=user1)
OR
(type(r)=rel_ep AND src.domain=user1 AND dst.domain=user1)
```

为了避免遗漏，建议在 `BoundGraphPattern` 中增加 `namespacePredicates`，由 pattern binder 收集，最后在 `rewriteMatchPattern` 中统一与用户 `where`、属性 map 谓词和 self-loop 谓词合并。

### 8. show/info 的执行层改造

`show_projected_graphs()` 当前已经注册并可以调用，但它的执行逻辑仍然通过全局 `MetadataRegistry::getMetadata()` 读取 compiler 侧 `GraphEntrySet`。本期需要保留现有 Cypher 接口和输出 schema，把实际数据访问改到 engine 执行期：

```c++
function->execFunc = [](const CallFuncInputBase&,
                        IStorageInterface& graph) {
  auto names = graph.schema().GetGraphEntryNames();
  // 构造并返回 execution::Context
};
```

因此“放到执行层”具体包含：

- bind 阶段只生成空的 `CallFuncInputBase` 和固定输出列，不读取 Namespace 数据；
- exec 阶段通过传入的 `graph_interface`，即 `IStorageInterface& graph`，调用 `graph.schema().GetGraphEntryNames()`；
- 不访问 `MetadataRegistry`，不读取 compiler 进程级缓存；
- 输出 Namespace 名称按字典序排序，保证结果稳定；
- 读取当前执行 snapshot 对应的 Schema，从而自然遵守事务可见性。

`projected_graph_info(name)` 使用相同原则。graph name 常量可在 physical bind 阶段写入 `ProjectedGraphInfoCallInput`，但 Namespace 是否存在以及详情读取必须在 exec 阶段通过 `graph.schema().GetGraphEntry(name)` 完成，避免编译与执行之间 Schema 变化导致错误结果。

### 9. 查询缓存与元数据可见性

Namespace 会改变同一条 Cypher 的逻辑计划，因此 Add/Drop 成功后必须提升 planning generation 并使旧 query cache 失效。编译查询时使用与读 snapshot 对应的 Schema，不能从进程级 `MetadataRegistry` 读取可能更新得更早或更晚的 Namespace 集合。

`show_projected_graphs()` 和 `projected_graph_info(name)` 按上一节所述，从传入的 `IStorageInterface::schema()` 读取数据，从而遵守事务和 snapshot 可见性；不能继续读取全局 MetadataManager。

输出顺序应固定：Namespace 按名称排序，详情中点在前、边在后，内部均按名称排序。

### 10. DDL 一致性

Namespace 是低优先级的逻辑元数据，不阻塞其他 Schema DDL。若 label 或谓词引用的属性在 Namespace 创建后被修改或删除，后续真正使用该 Namespace 编译查询时重新绑定，并在绑定失败处返回 Schema 或 Binder 错误。

### 11. 错误与并发语义

Add/Drop 使用 Schema transaction 的冲突规则。同名 Namespace 的并发创建只能有一个提交成功；对已删除 Namespace 的重复删除返回 not found。错误分类建议为：

- 参数形态或谓词错误：BinderException；
- Namespace/label 不存在：SchemaMismatch 或明确的 not found；
- 当前执行接口不可写：IllegalOperation；
- WAL/checkpoint 失败：StorageException，并回滚事务。

Namespace 名称和 label 匹配沿用现有 Schema 名称的大小写规则，不单独引入另一套比较规则。

### 12. 测试方案

#### GraphEntrySet 单元测试

- Add/Get/Drop 正常路径；
- 重复 Add、删除不存在项；
- YAML round-trip；
- 空谓词、特殊字符和引号；
- 非法 YAML、重复点和重复边；
- 稳定输出顺序。

#### Schema 与恢复测试

- `Schema::Clone/Compact/Equals` 保留 Namespace；
- `DumpToYaml -> LoadFromYamlNode` 保留完整定义；
- checkpoint 后关闭并重开数据库仍能查询 Namespace；
- checkpoint 前仅依赖 WAL 的恢复；
- 未提交创建、回滚创建、回滚删除均不可见；
- 老版本 YAML 不含 `projected_graphs` 时正常加载。

#### Compiler 测试

- `namespace.label` 点查询；
- `namespace.label` 边查询；
- `namespace.*`；
- Namespace 不存在；
- label 不属于 Namespace；
- 原图与 Namespace 查询结果对比；
- Namespace 谓词与用户 WHERE 正确 AND；
- 匿名端点仍被隔离；
- wildcard 多三元组使用正确的 OR 语义；
- 反引号限定名；
- `CREATE` pattern 使用 Namespace 时被拒绝；
- `MERGE` pattern 使用 Namespace 时被拒绝；
- pattern expression 使用 Namespace 时被拒绝；
- 普通 label 在上述 clause 中保持原有行为；
- `OPTIONAL MATCH` 使用 Namespace 正常绑定。

#### Python 端到端测试

准备两个 domain 的点和跨 domain 边，创建 `user1_subgraph` 后验证：

1. 只返回 `domain = user1` 的点；
2. 不返回通往 user2 点的边；
3. 原图查询仍返回两个 domain；
4. 新连接能立即看到已提交 Namespace；
5. drop 后已有缓存查询不会继续执行旧计划；
6. 数据库重开后结果不变。

另外验证 `show_projected_graphs()` 和 `projected_graph_info(name)`：

- 能看到当前 snapshot 中已提交的 Namespace；
- 看不到其他事务尚未提交或已经回滚的 Namespace；
- drop 后结果立即更新；
- checkpoint/WAL 恢复后输出不变；
- 多个 Namespace 和多条详情的输出顺序稳定；
- 执行路径不依赖 `MetadataRegistry` 中的 compiler 侧集合。

### 13. 推荐实施顺序

1. 抽取 storage 层 `ProjectedGraphEntry/GraphEntrySet`，完成纯数据结构测试；
2. 接入 Schema clone/YAML/checkpoint，并完成恢复测试；
3. 为 Add/Drop 增加 update interface、COW、WAL 和 cache invalidation；
4. 把 `project_graph`/`drop_projected_graph` 的副作用从 bind 移到 exec，并将 show/info 改为 exec 阶段通过 graph interface 读取；
5. 修改 grammar 和 Transformer，先完成 Namespace 类型白名单绑定；
6. 实现点谓词、边谓词及匿名端点的谓词注入；
7. 增加完整端到端测试。

前四步完成后可以保证 Namespace 元数据具备正确的事务和持久化语义；第五、六步完成后才真正具备查询隔离能力，不能只实现类型白名单就宣称子图隔离完成。
