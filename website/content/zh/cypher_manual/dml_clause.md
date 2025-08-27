# DML Clause

DML (Data Manipulation Language) 为图数据库中的数据插入、删除和修改操作提供支持。Neug 支持批量数据操作（如 COPY FROM）和单条数据操作（如 CREATE、SET 和 DELETE）。本文档为每种操作类型提供示例和说明。

## COPY FROM

COPY FROM 命令允许你从外部数据源批量加载数据，并在图存储中构建顶点和边。目前支持的数据源包括 CSV 格式。

### 加载顶点数据

从 CSV 文件加载 person 顶点数据。CSV 中的每一行对应一个顶点，列对应 person schema 中定义的顶点属性。

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**Command:**
```cypher
COPY person FROM "person.csv"
```

### 加载 Edge 数据

Load 从 CSV 文件中读取 edge 数据。前两列指定源 vertex 和目标 vertex 的主键，其余列定义 edge 的属性。

**knows.csv:**
```
src_name,dst_name,weight
marko,josh,1.0
marko,vadas,0.5
josh,peter,0.8
```

**Command:**
```cypher
COPY knows FROM "knows.csv"
```

## CREATE

CREATE 子句用于向图中插入新的 vertex 和 relationship。

### 创建 Vertex

创建具有指定属性的新 vertex。如果具有相同主键的 vertex 已存在，则会报错。

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### 创建 Vertex 和 Relationship

在单个语句中创建 vertex 和 relationship。当你需要同时创建 vertex 以及它们之间的 relationship 时非常有用。

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### 在现有顶点之间创建关系

首先匹配现有的顶点，然后在它们之间创建关系。

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

SET 子句用于更新现有顶点和关系的属性。

### 更新顶点属性

更新特定顶点的属性。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### 更新关系属性

更新特定关系的属性。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

DELETE 子句用于从图中删除顶点和关系。

### 删除顶点

从图中删除一个顶点。默认情况下，你只能删除没有关系的顶点，以避免产生悬空的边。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### 删除带有关系的顶点 (DETACH DELETE)

使用 DETACH DELETE 强制删除一个顶点及其所有关系。这可以避免在删除存在关系的顶点时出现错误。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### 删除关系

删除顶点之间的特定关系，同时保留顶点本身。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```