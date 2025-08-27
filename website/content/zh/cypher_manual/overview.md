# 语法概述

## 什么是 Cypher？

Cypher 是一种声明式的图查询语言，专门为图数据库设计。它提供了一种直观且富有表现力的方式来查询、操作和管理图数据。我们的实现基于 [OpenCypher](https://opencypher.org/) 规范，这是一个图查询语言的开放标准。

## 与 SQL 的主要区别

虽然 SQL 是为包含表和行的关系型数据库设计的，但 Cypher 针对包含节点、关系和属性的图数据库进行了优化：

- **结构**：SQL 使用表和连接（join）；Cypher 使用节点、关系和模式
- **模式匹配**：SQL 需要显式连接；Cypher 使用模式匹配语法
- **遍历**：SQL 需要复杂的连接来实现多跳查询；Cypher 天然支持路径遍历
- **可读性**：Cypher 的 ASCII 艺术语法使图模式在视觉上更加直观

## 在 Neug 中你可以用 Cypher 做什么？

在 Neug 中，我们将 Cypher 查询称为 **Statement**。一个 Statement 由多个 **Clauses** 组成。例如，在以下查询中：

```cypher
MATCH (p:person)
WHERE p.age = '29'
RETURN p.name as name;
```

`MATCH`、`WHERE` 和 `RETURN` 这些组件被称为 Clauses，它们是图数据库操作的基本逻辑单元。

基于 OpenCypher，我们定义了一系列 Statement 语法来管理 Neug 的图数据库，包括：

### Schema 管理 (DDL)

Neug 主要面向 Schema-Strict 的图数据场景，其中每一条数据都必须符合预定义的 schema 规范。这与传统的 SQL 类似；但图数据涉及更复杂的节点和关系结构，这些也必须遵守预定义的 schema 要求。

例如，考虑以下 schema 图：

<img src="figures/modern_schema.png" alt="Modern Schema Graph" style="display: block; margin: 2em auto; max-width: 500px;">

上述 schema 图可以通过以下语句创建：

```cypher
// 示例 schema 定义
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);

CREATE NODE TABLE software (
    name STRING,
    lang STRING,
    PRIMARY KEY (name)
);

CREATE REL TABLE knows (
    FROM person TO person,
    weight DOUBLE
);

CREATE REL TABLE created (
    FROM person TO software,
    weight DOUBLE
);
```

**符合 schema 的查询：**  
在以下查询中，顶点标签 `person` 和边标签 `(person-knows->person)` 都符合上面定义的 schema 约束。`person` 节点包含 `age` 和 `name` 属性，且 `age` 属性是 INT32 类型，可以与常量 18 进行比较。因此，该查询满足所有 schema 约束，是有效的：

```cypher
MATCH (p:person)-[:knows]->(f:person)
WHERE p.age > 18
RETURN p.name, f.name;
```

**不符合 schema 的查询（会失败）：**  
此查询中指定的边标签 `(person-follows->person)` 在 schema 中并不存在，因此该查询无效，并会返回 "Table `follows` does not exist" 错误。

```cypher
MATCH (p:person)-[:follows]->(m:person)
RETURN p.name;
```

我们定义了一组用于创建 schema 图的语法，如上所示，我们称之为 DDL（Data Definition Language）。所有后续的数据更新和查询操作都必须符合当前 DDL 所定义的 schema 规范。我们将在 [DDL 章节](ddl_clause.md) 中详细介绍相关内容。

### 数据查询 (DQL)

我们还定义了一套查询语法，可以同时满足事务处理 (TP) 和分析处理 (AP) 的查询需求。

例如，你可以使用以下查询语句在图数据库中查找所有的三角形模式：

```cypher
MATCH (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
WHERE a.name < c.name
RETURN a.name, b.name, c.name;
```

我们将每个 `MATCH`、`WHERE` 和 `RETURN` 称为一个 Clause（子句），它们是图数据操作的基本单元。在这里，`MATCH` 操作主要用于匹配构成三角形模式的所有数据，`WHERE` 进一步过滤模式数据以保证去重，而 `RETURN` 操作则对名称进行投影并输出最终结果。`MATCH` 操作主要完成图模式匹配，而 `WHERE`/`RETURN` 操作主要执行类似于 SQL 的关系操作。这些子句将在 [DQL 章节](query_clauses.md) 中详细介绍。

为了进一步确保 Clause 操作在数据上的合法性，我们定义了 Neug 所支持的数据类型边界，以及基于这些数据类型的表达式操作。这些内容将在 [数据类型](data_types.md) 和 [表达式章节](expression.md) 中详细介绍。

### 数据管理 (DML)

除了 DQL 和 DDL，Neug 还支持数据更新功能，我们称之为 DML (Data Manipulation Language)。DML 操作可以通过批量导入或增量更新来执行。

**批量导入示例：**
```cypher
COPY person FROM `person.csv`;
COPY knows FROM `knows.csv`;
```

以上两个 Statements 首先从 person.csv 批量加载标签为 `person` 的节点数据，然后从 knows.csv 批量加载标签为 `person-[knows]->person` 的边数据。

**增量更新示例：**

我们还提供了增量写入语法，用于增量更新图数据。

**节点创建示例：**
```cypher
CREATE (p:person {name: 'Bob', age: 30});
```

**关系创建示例：**
```cypher
MATCH (a:person {name: 'Bob'}), (b:person {name: 'marko'})
CREATE (a)-[:knows {weight: 3.0}]->(b);
```

**节点删除示例：**
```cypher
MATCH (p:person {name: 'Bob'})
DELETE p;
```

我们将在 [DML 章节](dml_clause.md) 中详细介绍这些 DML 操作。