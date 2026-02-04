# Sampling Extension 编译与使用指南

本目录包含 NeuG 采样扩展（SAMPLED_MATCH）的测试数据和模式文件。

## 目录结构

```
sampling/
├── README.md                    # 本文档
├── dataset/
│   ├── small/                   # 小规模测试数据集
│   ├── small_db/                # 已导入的小规模数据库
│   └── sf01/                    # LDBC SF01 数据集
├── pattern/
│   ├── simple_triangle.json     # 简单三角形模式
│   └── pattern_with_constraints.json  # 带属性约束的模式
└── FaSTest/                     # FaSTest 子图匹配库
```

## 编译指南

### 1. 配置 CMake

```bash
cd /mnt/lyk/neug/build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug -DBUILD_EXTENSIONS="sample" -DBUILD_TEST=ON
```

### 2. 编译扩展库

```bash
# 编译 libsample.neug_extension
make -j8 neug_sample_extension
```

编译后的文件位置：
```
/mnt/lyk/neug/build/extension/sample/libsample.neug_extension
```

### 3. 编译测试程序

```bash
# 编译测试程序
make -j8 test_sample_match test_sample_match_ldbc
```

编译后的文件位置：
```
/mnt/lyk/neug/build/extension/sample/test/test_sample_match
/mnt/lyk/neug/build/extension/sample/test/test_sample_match_ldbc
```

## 环境配置

### EXTENSION_HOME 设置

NeuG 通过 `EXTENSION_HOME` 环境变量查找扩展库。设置方法：

```bash
# 方法1: 临时设置
export EXTENSION_HOME=/tmp

# 方法2: 添加到 ~/.bashrc
echo 'export EXTENSION_HOME=/tmp' >> ~/.bashrc
source ~/.bashrc
```

### 安装扩展库

将编译好的扩展库复制到 `$EXTENSION_HOME/extension/sample/` 目录：

```bash
# 创建目录
mkdir -p $EXTENSION_HOME/extension/sample/

# 复制扩展库
cp /mnt/lyk/neug/build/extension/sample/libsample.neug_extension $EXTENSION_HOME/extension/sample/
```

完成后，扩展库路径应为：
```
$EXTENSION_HOME/extension/sample/libsample.neug_extension
```

例如，如果 `EXTENSION_HOME=/tmp`，则路径为：
```
/tmp/extension/sample/libsample.neug_extension
```

## 运行测试

### 运行小规模测试

```bash
./build/extension/sample/test/test_sample_match
```

### 运行 LDBC SF01 测试

```bash
./build/extension/sample/test/test_sample_match_ldbc
```

## SAMPLED_MATCH 函数说明

### 输入

`SAMPLED_MATCH` 接受一个参数：模式文件的路径（JSON 格式）。

**Cypher 语法：**
```cypher
CALL SAMPLED_MATCH('/path/to/pattern.json')
```

### 模式文件格式

```json
{
  "vertices": [
    {
      "id": 0,
      "label": "person",
      "constraints": [
        {"property": "age", "operator": ">=", "value": 18}
      ]
    },
    {
      "id": 1,
      "label": "person",
      "constraints": []
    }
  ],
  "edges": [
    {
      "source": 0,
      "target": 1,
      "label": "person_knows_person",
      "constraints": []
    }
  ]
}
```

**字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `vertices` | 数组 | 模式中的顶点列表 |
| `vertices[].id` | 整数 | 顶点在模式中的唯一标识符 |
| `vertices[].label` | 字符串 | 顶点标签（需与数据库中的 Node Table 名称匹配，小写） |
| `vertices[].constraints` | 数组 | 顶点属性约束（可选） |
| `edges` | 数组 | 模式中的边列表 |
| `edges[].source` | 整数 | 边的源顶点 id |
| `edges[].target` | 整数 | 边的目标顶点 id |
| `edges[].label` | 字符串 | 边标签（需与数据库中的 Rel Table 名称匹配，小写） |
| `edges[].constraints` | 数组 | 边属性约束（可选） |

**约束格式：**
```json
{"property": "属性名", "operator": "比较运算符", "value": "比较值"}
```

支持的运算符：`=`, `!=`, `<`, `<=`, `>`, `>=`

### 输出

返回所有匹配的子图结果，每个结果包含模式顶点到数据图顶点的映射。

**输出列：**
- 每个模式顶点对应一列，列名为 `v0`, `v1`, `v2`, ...
- 每列的值为匹配的数据图顶点 ID

**示例输出：**
```
| v0 | v1 | v2 |
|----|----|----|
| 0  | 1  | 2  |
| 1  | 0  | 2  |
| 2  | 1  | 0  |
```

## 使用示例

### 1. 在 Cypher 中使用

```cypher
-- 加载扩展
LOAD sample;

-- 执行子图匹配
CALL SAMPLED_MATCH('/mnt/lyk/neug/sampling/pattern/simple_triangle.json');
```

### 2. 使用全路径加载

如果 `EXTENSION_HOME` 未设置或扩展未安装到指定位置，可使用全路径：

```cypher
LOAD '/mnt/lyk/neug/build/extension/sample/libsample.neug_extension';

CALL SAMPLED_MATCH('/mnt/lyk/neug/sampling/pattern/simple_triangle.json');
```

### 3. Python API 使用

```python
import neug

# 创建数据库连接
db = neug.Database("/path/to/database")
conn = db.connect()

# 加载扩展
conn.execute("LOAD sample;")

# 执行子图匹配
result = conn.execute("CALL SAMPLED_MATCH('/mnt/lyk/neug/sampling/pattern/simple_triangle.json')")
for row in result:
    print(row)

conn.close()
db.close()
```

## 模式示例

### 简单三角形模式 (simple_triangle.json)

查找三个互相认识的人：

```json
{
  "vertices": [
    {"id": 0, "label": "person"},
    {"id": 1, "label": "person"},
    {"id": 2, "label": "person"}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"}
  ]
}
```

### 带约束的复杂模式 (pattern_with_constraints.json)

查找三个互相认识的人，其中一人发表了评论，另一人发表了帖子（创建时间 >= 某值）：

```json
{
  "vertices": [
    {"id": 0, "label": "person", "constraints": []},
    {"id": 1, "label": "person", "constraints": []},
    {"id": 2, "label": "person", "constraints": []},
    {"id": 3, "label": "comment", "constraints": []},
    {"id": 4, "label": "post", "constraints": [
      {"property": "creationDate", "operator": ">=", "value": "2266179421451"}
    ]}
  ],
  "edges": [
    {"source": 0, "target": 1, "label": "person_knows_person"},
    {"source": 1, "target": 2, "label": "person_knows_person"},
    {"source": 2, "target": 0, "label": "person_knows_person"},
    {"source": 3, "target": 0, "label": "comment_hasCreator_person"},
    {"source": 4, "target": 1, "label": "post_hasCreator_person"}
  ]
}
```

## 故障排除

### 1. 扩展加载失败

**错误信息：**
```
Extension sample not found in user install or wheel package
```

**解决方法：**
1. 确认 `EXTENSION_HOME` 环境变量已设置
2. 确认扩展库已复制到正确位置
3. 使用全路径加载扩展

### 2. 模式文件未找到

**错误信息：**
```
Error: Cannot open pattern file: /path/to/pattern.json
```

**解决方法：**
1. 检查文件路径是否正确
2. 检查文件是否存在
3. 检查文件权限

### 3. gflags 链接错误

**错误信息：**
```
undefined reference to `google::FlagRegisterer::FlagRegisterer<int>...
```

**解决方法：**
确保 `CMakeLists.txt` 中的 `target_link_libraries` 包含 `gflags`：
```cmake
target_link_libraries(test_sample_match PRIVATE
    neug_libraries
    ${GLOG_LIBRARIES}
    gflags
)
```

## 相关文件

- 扩展源代码：`/mnt/lyk/neug/extension/sample/`
- 扩展文档：`/mnt/lyk/neug/extension/sample/README.md`
- Python Demo：`/mnt/lyk/neug/extension/sample/demo_sampled_match.py`
- C++ 测试：`/mnt/lyk/neug/extension/sample/test/test_sample_match.cc`
- LDBC 测试：`/mnt/lyk/neug/extension/sample/test/test_sample_match_ldbc.cc`
