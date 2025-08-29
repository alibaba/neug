### 核心概念
Database、Connection 和 Session 是 NeuG 中的基础概念。

## Database

NeuG 中的 **Database** 代表一个由顶点和边组成的图。默认情况下，数据会持久化存储在指定的目录中。NeuG 还提供了 **in-memory** 模式，数据仅存在于内存中，不会保存到磁盘。要创建一个内存数据库，可以将 `db_path` 设置为空字符串：

```python
from neug import Database

# 创建一个内存数据库
db = Database(db_path="")
```

### 访问模式

NeuG 数据库支持两种访问模式：`read_write` 和 `read_only`。

- **read_write**：以 **read_write** 模式打开目录时，会通过锁定该目录来授予当前进程独占访问权限，从而防止其他进程并发访问。
- **read_only**：以 **read_only** 模式打开时，多个进程可以同时以 **read_only** 模式访问该目录。

需要注意的是，内存数据库无法以 `read_only` 模式打开。

## 连接

**Connection** 是与嵌入式数据库交互的主要接口，用于执行查询和管理数据。

### 从数据库建立连接

**Connection** 对象作为访问数据库的通道，其行为受数据库访问模式的影响。

- 在 **read_write** 模式下，Connection 可以执行读取（MATCH）和写入查询（CREATE、COPY FROM）。为了保持数据一致性，read_write 数据库只能创建一个连接对象。
  
- 在 **read_only** 模式下，Connection 仅限于执行读取查询。

## 服务模式和会话

NeuG 也可以在 **service mode** 下运行，此时数据库作为服务器运行，接受远程连接。此模式适用于需要频繁、并发或分布式访问的应用程序。

要以服务模式启动 NeuG，请指定 host 和 port：

```python
import neug

# 以服务形式启动 NeuG

```python
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print(f"NeuG service started on {service}")
```

服务器启动后，客户端可以使用 `Session` 对象进行连接：

```python
from neug import Session

# 建立到 NeuG 服务的连接
session = Session("http://localhost:10000/")

# 执行查询
session.execute('MATCH(n) RETURN count(n)')

session.close()
```

可以在不同进程中创建多个 session 来提交并发查询。数据库服务器会确保在整个过程中维护数据库的 ACID 特性。