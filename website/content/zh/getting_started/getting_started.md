# NeuG 入门指南

本指南将带你创建第一个图数据库，执行基本操作，并探索嵌入式和服务模式。

## 前置要求

开始之前，请确保你已经安装了 NeuG。如果还没有安装，请参考 [安装指南](../installation/installation.md)。

## 理解 NeuG 架构

NeuG 有两个独立工作的架构概念：

### 数据库存储模式
数据存储方式：
- **In-Memory Database**: 数据仅存储在 RAM 中（最快，临时存储）
- **Persistent Database**: 数据存储在磁盘上（持久化，重启后数据不丢失）

### 连接模式
访问数据库的方式：
- **Embedded Mode**：直接进程内访问（分析、单用户）
- **Service Mode**：基于网络的访问（多用户、并发）

这些模式是独立的——你可以根据使用场景自由组合：

**常见组合：**
- **Persistent + Embedded**：大规模数据集的数据科学分析、ETL 处理、图研究
- **Persistent + Service**：生产级 Web 应用、多用户系统、微服务架构
- **In-Memory + Embedded**：单元测试、原型开发、临时计算、算法开发
- **In-Memory + Service**：高速缓存层、基于会话的分析、临时多用户负载

## 数据库存储模式

### 持久化数据库
- **使用场景**：生产应用、数据分析、长期存储
- **持久性**：数据在应用重启后仍然保留

```python

# 持久化模式示例

# 确保数据库目录存在且用户可写。
db_persistent = neug.Database(db_path="/path/to/database")
```

### 内存数据库
- **使用场景**: 临时计算、测试、原型开发
- **持久性**: 进程结束时数据丢失

```python

# 内存模式示例
db_memory = neug.Database(db_path="")
```

```{note}
目前，NeuG 的内存模式会创建一个临时数据库目录，进程退出时会自动清理。
```

## 连接模式

NeuG 提供两种模式来访问你的数据库：

### 嵌入模式
直接进程内访问 - 适合单用户场景：

```python
import neug

# 创建数据库并直接连接
db = neug.Database(db_path="./neug/db")  # 或者 db_path="" 用于内存模式
conn = db.connect()

print("Connect to NeuG in embedded mode")

conn.close()
db.close()
```

### 服务模式
基于网络的访问 - 适合多用户应用：

**启动服务：**
```python
import neug

# 以服务方式启动 NeuG
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print("NeuG service started on localhost:10000")
```

**客户端连接:**
```python
from neug import Session

# 连接到服务
session = Session("http://localhost:10000/")

session.close()
```

## 基本操作

以下操作无论你选择哪种数据库模式（in-memory 或 persistent）和连接模式（embedded 或 service）都是一样的。我们假设这个例子中使用的是 persistent 数据库的 embedded 模式。

```python
import neug

# 创建数据库并建立连接
db = neug.Database(db_path="./neug/db")
conn = db.connect()
```

### 创建节点和边

在插入数据之前，你需要先定义图谱 schema，包括节点和边的类型：

```python

# 创建节点表
conn.execute("""
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT64,
        email STRING
    )
""")

conn.execute("""
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT64
    )
""")

# 创建边表
conn.execute("""
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
""")

conn.execute("""
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT64,
        relationship_type STRING
    )
""")

print("Graph schema created successfully!")
```

### 插入数据

现在让我们向图中添加一些数据：

```python

# 插入节点
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# 插入关系
conn.execute("""
    MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
    CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
""")

conn.execute("""
    MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
    CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
""")

print("Data inserted successfully!")
```

### 查询数据

让我们通过一些查询来探索你的图数据：

```python

# 简单节点查询
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# 关系查询
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson works as Software Engineer at TechCorp

# 复杂模式查询
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith knows Alice Johnson who works at TechCorp
```

### 关闭连接和数据库

```python
conn.close()
db.close()
```

### 使用内置数据集

NeuG 提供了几个内置数据集，你可以用来快速开始图分析、学习或测试。这些数据集开箱即用，无需任何设置。

#### 可用数据集

你可以列出所有可用的内置数据集：

```python
from neug.datasets import get_available_datasets

# 列出所有可用数据集
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

当前可用的内置数据集包括：
- modern_graph

#### 加载内置数据集

有几种方式可以使用内置数据集：

**方法 1: 从数据集创建新数据库**

```python
from neug import Database

# 直接从内置数据集创建数据库
db = Database.from_builtin_dataset(dataset_name="modern_graph")
conn = db.connect()
```

# 探索已加载的数据集
result = conn.execute("MATCH (n) RETURN count(n) as node_count")
print(f"Total nodes: {result.__next__()[0]}") # 应该是 6
```

**方法 2: 将数据集加载到现有数据库中**

```python
import neug

# 创建一个空数据库
db = neug.Database("./my_analysis.db")

# 将内置数据集加载到其中
db.load_builtin_dataset(dataset_name="modern_graph")
```

注意，当将内置数据集导入到现有数据库时，请确保没有 schema 冲突，即不能有标签名为 `person` 和 `software` 的顶点，也不能有标签名为 `knows` 和 `created` 的边。

**方法 3: 使用便捷函数**

```python
from neug.datasets.loader import load_dataset

# 或者加载到指定路径
db = load_dataset("modern_graph", "/tmp/modern_graph.db")

conn = db.connect()

# ... 处理你的数据集
conn.close()
db.close()
```

## 下一步

恭喜！你已经掌握了 NeuG 的基础知识。接下来可以探索以下内容：

1. **[数据导入/导出](import_graph.md)**：学习如何导入大型数据集
2. **[高级 Cypher 查询](cypher_query.md)**：掌握复杂的图模式查询
3. **[数据建模](data_model.md)**：设计高效的图结构 schema
4. **[性能优化](../performance/index.md)**：调整数据库以获得最佳性能