# SchemaView 设计

## 什么是 SchemaView？

`SchemaView` 提供了一层 Schema 逻辑视图，用于在同一个底层 Schema 存储中隔离不同逻辑 Schema 下的点边类型。

底层 Schema 负责统一管理所有点边类型，而 `SchemaView` 基于 namespace 对底层 Schema 进行过滤和访问控制，使不同逻辑 Schema 之间的点边类型相互隔离。

简单来说：

- **Schema：** 管理所有实际存在的点边类型；
- **SchemaView：** 提供某个逻辑 Schema namespace 下的访问视图，只暴露属于该 namespace 的点边类型。

---

## 为什么需要 SchemaView？

引入 `SchemaView` 的主要目的是在不改变现有 Schema 管理模型的情况下，逐步支持 NeuG 中不同场景下的 Schema 隔离需求。

目前 SchemaView 主要服务于以下两个场景：

### 1. 支持内部功能创建隐藏点边类型

一些 NeuG 内部功能需要在数据库中创建辅助点边表，但这些点边类型不应该暴露给普通用户。

例如全文索引：

- 全文索引可能需要在内部创建额外的点表、边表，用于存储索引相关的数据；
- 这些内部表需要复用 NeuG 的 Schema 和存储能力；
- 但用户不应该在正常图查询中看到或访问这些内部点边类型。

通过为内部功能分配独立的 namespace，并通过对应的 `SchemaView` 访问，可以实现：

- 内部功能可以正常创建和访问自己的点边类型；
- 用户侧 SchemaView 不包含这些内部点边类型；
- 不同功能模块之间的 Schema 互相隔离。

---

### 2. 支持用户侧多图（Multi-Graph）功能

在内部功能场景验证 SchemaView 后，可以进一步开放给用户，用于支持多图能力。

用户可以显式创建多个 Schema，并在指定 Schema 下创建点边类型。不同 Schema 之间的点边类型相互隔离，用户可以基于指定 Schema 执行图查询。

例如：

```cypher
CREATE SCHEMA s1;
CREATE SCHEMA s2;

CREATE NODE TABLE s1.Person (
    id INT64,
    name STRING,
	PRIMARY KEY(id)
);

CREATE NODE TABLE s2.Book (
    id INT64,
    title STRING,
	PRIMARY KEY(id)
);

MATCH (n: s1.Person)
WHERE n.name = 'XX'
RETURN count(n);

MATCH(n: s2.Book)
WHERE n.title <> 'XX'
RETURN count(n);
```

## SchemaView 设计

当前方案倾向于在 Schema 层之上提供一层 `SchemaView` 抽象，通过 namespace 对 Schema 进行逻辑隔离。`SchemaView` 表示某个特定 namespace 下的 Schema 视图，所有 Schema 查询操作均通过该 View 完成。通过在 View 层过滤非当前 namespace 的点边类型，实现不同 namespace 之间的 Schema 隔离。


`SchemaView` 内部维护：

- 底层 `Schema` 对象的只读引用；
- 当前 View 所属的 namespace。

底层 `Schema` 仍然统一存储所有 namespace 下的点边类型，并按照 label 提供统一的 Schema 管理能力。namespace 隔离逻辑由 `SchemaView` 负责，而不是由底层 `Schema` 实现。


当前设计**暂不支持不同 namespace 使用相同的 label name**：

- 主要原因是底层 Schema 当前仍然基于 label name 作为点边类型的唯一标识进行存储和管理。因此，不同 namespace 下的 label name 需要由用户保证全局唯一。

- 如果未来需要支持 namespace 内 label 重名，需要调整底层 Schema 存储结构，将 `(namespace, label)` 作为点边类型的唯一标识。

## 实现

### 接口设计

`SchemaView` 对外提供与 `Schema` 类似的查询接口，但所有查询结果均限定在当前 namespace 范围内，例如：

- `GetVertexSchemas` / `GetEdgeSchemas`: 返回当前 namespace 下的点边 Schema；
- `GetVertexSchema` / `GetEdgeSchema`: 仅返回属于当前 namespace 的指定 label Schema；
- `ContainsXXX`: 判断指定 label 是否属于当前 namespace。

所有 Get 类接口内部通过 `EnsureSchema` 或 namespace 校验逻辑，保证不会返回其他 namespace 下的 Schema。

```c++
  class SchemaView {
   public:
    SchemaView(const Schema* schema, std::string namespace_name);
    SchemaView(const Schema& schema, std::string namespace_name);

    const Schema& GetSchema() const;
    const std::string& GetNamespace() const;

	// Get 接口内部调用 EnsureSchema 保证只返回当前 namespace 内的点边
    std::vector<std::shared_ptr<const VertexSchema>> GetVertexSchemas() const;
    std::vector<std::shared_ptr<const EdgeSchema>> GetEdgeSchemas() const;

    result<std::shared_ptr<const VertexSchema>> GetVertexSchema(label_t label) const;
    result<std::shared_ptr<const VertexSchema>> GetVertexSchema(
        const std::string& label) const;

    result<std::shared_ptr<const EdgeSchema>> GetEdgeSchema(
        label_t src_label, label_t dst_label, label_t edge_label) const;
    result<std::shared_ptr<const EdgeSchema>> GetEdgeSchema(
        const std::string& src_label,
        const std::string& dst_label,
        const std::string& edge_label) const;

	// 内部调用 IsVertexInNamespace，确保当前 label 在 namespace 内
    bool ContainsVertexLabel(label_t label) const;
    bool ContainsVertexLabel(const std::string& label) const;

    bool ContainsEdgeLabel(label_t label) const;
    bool ContainsEdgeLabel(const std::string& label) const;

    bool ContainsEdgeTriplet(
        label_t src_label, label_t dst_label, label_t edge_label) const;
    bool ContainsEdgeTriplet(
        const std::string& src_label,
        const std::string& dst_label,
        const std::string& edge_label) const;

    std::vector<label_t> GetVertexLabelIds() const;
    std::vector<label_t> GetEdgeLabelIds() const;

   private:
    void EnsureSchema() const;

    bool IsVertexInNamespace(label_t label) const;
    bool IsVertexInNamespace(const VertexSchema& schema) const;
    bool IsEdgeInNamespace(const EdgeSchema& schema) const;

    const Schema* schema_;
    std::string namespace_;
  };
```

### 其他接口改动

我们进一步修改 Schema `AddVertexLabel/AddEdgeLabel` 接口：
- 用于在创建点边类型时指定所在 namespace；
- 不显示指定 namespace 默认为 `"default"`，代表默认 namespace 类型；

```c++
class Schema {
public:
  void AddVertexLabel(
      const std::string& label, const std::vector<DataType>& property_types,
      const std::vector<std::string>& property_names,
      const std::vector<std::tuple<DataType, std::string, size_t>>& primary_key,
      size_t max_vnum = static_cast<size_t>(1) << 32,
      const std::string& description = "",
      const std::vector<Value>& default_property_values = {},
      bool temporary = false,
      string namespace = "default");

  void AddEdgeLabel(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label,
      const std::vector<DataType>& properties,
      const std::vector<std::string>& prop_names,
      EdgeStrategy oe = EdgeStrategy::kMultiple,
      EdgeStrategy ie = EdgeStrategy::kMultiple,
      bool oe_mutable = true, bool ie_mutable = true,
      std::optional<std::string> sort_key_for_nbr = std::nullopt,
      const std::string& description = "",
      const std::vector<Value>& default_property_values = {},
      bool temporary = false,
      string namespace = "default");
};
```


`GraphInterface` 也需要透传 namespace 参数：
- 我们直接在 `CreateVertexTypeParam/CreateEdgeTypeParam` 增加 namespace 参数；
- 在全文索引调用 GraphInterface 创建点边类型时，需要显示设置 `namespace = fts`；

```c++
class StorageUpdateInterface{
public:
  virtual Status CreateVertexType(const CreateVertexTypeParam& config) = 0;
  virtual Status CreateEdgeType(const CreateEdgeTypeParam& config) = 0;
}
```

```c++
class CreateVertexTypeParam {
 private:
  std::string vertex_label_name;
  std::vector<std::pair<std::string, Value>> properties;
  std::vector<std::string> primary_key_names;
  bool temporary = false;
  std::string namespace = "default";
};
```

```c++
class CreateEdgeTypeParam {
 private:
  std::string src_label_name;
  std::string dst_label_name;
  std::string edge_label_name;
  std::vector<std::pair<std::string, Value>> properties;
  EdgeStrategy oe_edge_strategy;
  EdgeStrategy ie_edge_strategy;
  std::optional<std::string> sort_key_for_nbr;
  bool temporary = false;
  std::string namespace = "default";
};
```
