# Neug 数据类型

本文档提供了 Neug 支持的所有数据类型的全面概述。

## 数据类型汇总表

下表展示了 Neug 支持的所有数据类型及其与 Neo4j 的差异。

| 类别 | 类型 | Neug 示例 | Neo4j 示例 |
|----------|------|--------------|---------------|
| Primitive | INT32 | `Return CAST(42, 'INT32')` | `Return 42` |
| Primitive | UINT32 | `Return CAST(42, 'UINT32')` | 不支持 |
| Primitive | INT64 | `Return 9223372036854775807` | `Return 9223372036854775807` |
| Primitive | UINT64 | `Return CAST(9223372036854775807, 'UINT64')` | 不支持 |
| Primitive | FLOAT | `Return CAST(3.14, 'FLOAT')` | `Return 3.14f` |
| Primitive | DOUBLE | `Return 3.14159265359` | `Return 3.14159265359d` |
| Primitive | BOOL | `Return true` | `Return true` |
| Primitive | NULL | `Return null` | `Return null` |
| String | VARCHAR | `Return 'Hello World'` | `Return 'Hello World'` |
| Temporal | DATE | `Return date('2022-06-06')` | `Return date('2022-06-06')` |
| Temporal | DATETIME | `Return timestamp('2022-06-06 12:00:00')` | `Return datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `Return interval('1 year 2 days')` | `Return duration('P1Y2D')` |
| Composite | LIST | `Return [1, 2, 3]` | `Return [1, 2, 3]` |
| Pattern | NODE | `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}` | `(:person {name: 'Alice', age: 30})` |
| Pattern | REL | `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:knows {weight: 1.0}]` |
| Pattern | REPEATED PATH | `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## 详细介绍

### 原始类型

#### INT32
- **描述**: 32 位有符号整数类型
- **范围**: [2,147,483,648, 2,147,483,647]
- **查询示例**: `Return CAST(42, 'INT32') as int32_value;`

#### UINT32
- **描述**: 32 位无符号整数类型
- **范围**: [0, 4,294,967,295]
- **查询示例**: `RETURN CAST(42, 'UINT32') AS uint32_value;`

#### INT64
- **描述**: 64 位有符号整数类型，整数值的默认类型
- **范围**: [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
- **查询示例**: `RETURN 9223372036854775807 AS int64_value;`

#### UINT64
- **描述**: 64 位无符号整数类型
- **范围**: [0, 18,446,744,073,709,551,615]
- **查询示例**: `RETURN CAST(18446744073709551615, 'UINT64') AS uint64_value;`

#### FLOAT
- **描述**: 单精度浮点数
- **精度**: ~7 位十进制数字
- **查询示例**: `RETURN CAST(3.14, 'FLOAT') AS float_value;`

#### DOUBLE
- **Description**: 双精度浮点数，float 值的默认类型
- **Precision**: ~15-17 位十进制数字
- **Query Example**: `RETURN 3.14159265359 AS double_value;`

#### BOOL
- **Description**: 布尔类型，表示 true 或 false 值
- **Values**: `true`, `false`
- **Query Example**: `RETURN true AS bool_value;`

#### NULL
- **Description**: 表示缺失或未定义的值
- **Query Example**: `RETURN null AS null_value;`

### String Types

#### VARCHAR
- **Description**: 可变长度字符串，使用 UTF-8 编码
- **Query Example**: `RETURN 'Hello World' AS string_value;`
- **Length**: 可变长度，受系统限制，默认为 `65536`

### Temporal Types

#### DATE
- **Description**: 日期类型，用于存储日历日期
- **Format**: YYYY-MM-DD
- **Query Example**: `RETURN date('2022-06-06') AS date_value;`

#### DATETIME
- **Description**: 日期和时间组合类型
- **Format**: YYYY-MM-DD HH:MM:SS
- **Query Example**: `RETURN timestamp('2022-06-06 12:00:00') AS datetime_value;`

#### INTERVAL
- **Description**: 持续时间或时间间隔类型，遵循 [kuzu 的规范]()
- **Query Example**: `RETURN interval('1 year 2 days') AS interval_value;`

### 复合类型

#### LIST
- **Description**: 有序的异构类型值集合
- **Query Example**: `RETURN [1, 2, 3] AS list_value;`

下表展示了 LIST 支持的所有组件类型：

| 类别 | 类型 | 示例 |
|----------|------|---------|
| 数值型 | INT32, INT64, UINT32, UINT64, DOUBLE, FLOAT | `RETURN [1, 2, 3.0];` |
| 字符串型 | VARCHAR | `RETURN ['marko', 'josh'];` |
| 日期型 | DATE, DATETIME | `RETURN [date('2011-01-25'), timestamp('2011-01-25 11:20:33')];` |
| 布尔型 | BOOL | `RETURN [true, false];` |
| 复合型 | LIST | `RETURN [[1, 2], [4, 5]];` |

**LIST 组件类型重要说明**：

Neug 通过 tuple 数据类型支持 list，这意味着复合类型可以是异构的。以下是一些示例：

在单个 list 中混合不同的 primitive 类型：
```cypher
RETURN ['marko', 2];
```

将节点中不同属性类型组合到 list 中：
```cypher
MATCH (n:person) RETURN [n.name, n.age];
```

支持嵌套 list 结构：
```cypher
MATCH (n:person) RETURN [["name", n.name], ["age", n.age]];
```

**关键技术细节**：
- Neug 中的 list 可以包含不同数据类型的元素（异构 list）
- 这是通过内部 tuple 数据类型支持实现的
- 在可能的情况下，系统会自动处理类型转换
- 完全支持嵌套 list 以构建复杂数据结构
- 系统在允许 list 组成灵活性的同时保持类型安全性

### Pattern Types

#### NODE
- **Description**: 表示图中的一个顶点
- **Internal Structure**: 包含 `_ID` (内部标识符)、`_LABEL` (节点标签) 和属性字段
- **Query Example**: `MATCH (n:person) RETURN n AS node_value;`
- **Neug Format**: `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}`

#### REL (Relationship)
- **Description**: 表示图中的一条边
- **Internal Structure**: 包含 `_SRC` (源节点 ID 和 LABEL)、`_DST` (目标节点 ID 和 LABEL)、`_ID` (关系内部标识符)、`_LABEL` (关系类型) 和属性字段
- **Query Example**: `MATCH ()-[r:knows]->() RETURN r AS rel_value;`
- **Neug Format**: `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}`

#### 重复路径 (REPEATED PATH)
- **描述**: 表示图中由重复边组成的路径
- **内部结构**: 包含路径中的每个节点和关系，包括起始节点和目标节点
- **查询示例**: `MATCH (a:person)-[p*1..2]->(c) RETURN p AS path_value;`
- **Neug 格式**: `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}`