# NeuG Subgraph Matching 完整测试指南

本指南描述如何测试 SAMPLED_MATCH 函数。

## 测试方式

由于 neug 的 TableFunction 架构限制，目前有两种测试方式：

### 方式 1：独立测试程序（推荐）

使用 `test_sample_match` 程序直接访问数据库进行测试。

### 方式 2：HTTP 服务 + CALL 语句

通过 HTTP 服务执行 Cypher 查询（需要额外配置）。

---

## 方式 1：独立测试程序

### 步骤 1：编译项目

```bash
cd /path/to/neug
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_TEST=ON \
         -DBUILD_EXTENSIONS="sampled_match"
make -j$(nproc)
```

### 步骤 2：准备数据

数据已经在 `sampling/dataset/small/` 目录中准备好了。

### 步骤 3：运行测试

```bash
./extension/sample/test/test_sample_match \
    ../sampling/dataset/small \
    ../sampling/pattern/pattern_with_constraints.json
```

### 预期输出

```
============================================
  Subgraph Matching Test (FaSTest)
============================================
Data path: ../sampling/dataset/small
Pattern file: ../sampling/pattern/pattern_with_constraints.json

Opening database...
Database loaded in 0.5 seconds

[0] Building label mappings...
  Edge triplet: person -[person_knows_person]-> person
  Edge triplet: comment -[comment_hasCreator_person]-> person
  Edge triplet: post -[post_hasCreator_person]-> person

[1] Preprocessing data graph...
  Vertices: 6
  Edges: 6

...

=== Results ===
  Estimated embedding count: XXX
  Number of sampled embeddings: XX
```

---

## 方式 2：HTTP 服务 + CALL 语句

### 前置条件

需要修改 neug 服务器以支持在启动时注册图到 `GraphRegistry`。

### 步骤 1：修改服务器（可选）

在 `bin/rt_server.cc` 中添加：

```cpp
#include "sample_functions.h"

// 在 db.Open(config) 之后添加：
neug::timestamp_t read_ts = 0;
neug::StorageReadInterface* storage_interface = 
    new neug::StorageReadInterface(db.graph(), read_ts);
neug::function::GraphRegistry::SetGraph(storage_interface);
```

### 步骤 2：启动服务器

```bash
# 终端 1：启动服务器
cd build
./bin/rt_server --data-path ../sampling/dataset/small --http-port 10000
```

### 步骤 3：发送查询

```bash
# 终端 2：发送查询
curl -X POST http://localhost:10000/cypher \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CALL SAMPLED_MATCH(\"/absolute/path/to/pattern.json\")",
    "access_mode": "read"
  }'
```

或者使用 Python 脚本：

```python
import requests
import json

url = "http://localhost:10000/cypher"
data = {
    "query": 'CALL SAMPLED_MATCH("/path/to/neug/sampling/pattern/pattern_with_constraints.json")',
    "access_mode": "read"
}

response = requests.post(url, json=data)
print(json.dumps(response.json(), indent=2))
```

---

## Pattern 文件格式

```json
{
  "vertices": [
    {"id": 0, "label": "person"},
    {"id": 1, "label": "person"},
    {"id": 2, "label": "comment"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "comment_hasCreator_person"}
  ]
}
```

---

## 故障排查

### 问题：Graph not registered

**原因**：`GraphRegistry::SetGraph()` 没有被调用。

**解决方案**：
- 方式 1：使用独立测试程序
- 方式 2：修改服务器代码注册图

### 问题：Cannot open pattern file

**原因**：pattern 文件路径错误或不存在。

**解决方案**：
- 确保使用绝对路径
- 确保文件存在且可读

### 问题：Label not found

**原因**：pattern 中的 label 名称与图 schema 不匹配。

**解决方案**：
- 检查 `graph.yaml` 中定义的 label 名称
- 确保 pattern 中的 label 与 schema 完全匹配
