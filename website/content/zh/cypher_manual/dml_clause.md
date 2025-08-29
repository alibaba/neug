# DML 子句

DML (Data Manipulation Language) 提供了在图数据库中进行数据插入、删除和修改的操作。NeuG 支持批量数据操作（如 COPY FROM）和单条数据操作（如 CREATE、SET 和 DELETE）。本文档为每种操作类型提供了示例和说明。

## COPY FROM

COPY FROM 命令允许你从外部数据源批量加载数据，并在图存储中构建节点和边。
更多详情请参考 [导入数据](../user_guide/import_data.md)。

### 加载节点数据

从 CSV 文件加载 person 节点数据。CSV 中的每一行对应一个节点，列对应 person schema 中定义的节点属性。

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**命令:**
```cypher
COPY person FROM "person.csv"
```

### 加载 Edge Data

Load 从 CSV 文件中读取 edge 数据。前两列指定源节点和目标节点的主键，其余列定义 edge 的属性。

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

CREATE 子句用于向图中插入新的节点和边。

### 创建节点

创建具有指定属性的新节点。如果具有相同主键的节点已存在，则会报错。

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### 创建节点和边

在单个语句中创建节点和边。当你需要同时创建节点以及它们之间的边时非常有用。

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### 在现有节点之间创建边

首先匹配现有节点，然后在它们之间创建一条边。

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

SET 子句用于更新现有节点和边的属性。

### 更新节点属性

更新特定节点的属性。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### 更新边属性

更新特定边的属性。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

DELETE 子句用于从图中删除节点和边。

### 删除节点

从图中删除一个节点。默认情况下，你只能删除没有关联边的节点，以避免产生悬空边。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### 删除节点及其关联边 (DETACH DELETE)

使用 DETACH DELETE 可以强制删除一个节点及其所有关联的边。这样可以避免在删除仍有边连接的节点时出现错误。

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### 删除边

删除节点之间的特定边，同时保留节点。

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```