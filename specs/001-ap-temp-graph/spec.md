# Feature Specification: AP Temporary Graph (临时点边)

**Feature Branch**: `001-ap-temp-graph`  
**Created**: 2025-12-09  
**Status**: Draft  
**Input**: User description: "NeuG临时点边功能：在AP场景下支持从外部数据源（CSV/JSON/Parquet）载入临时点和边，支持本地文件和网络资源（HTTP/S3/OSS），与持久图统一查询"

---

## Priority Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  P1: 本地文件 + 关系型查询 (可并行开发)                                       │
│  ┌─────────────┬─────────────┬─────────────┬─────────────────────────────┐  │
│  │  Load CSV   │  Load JSON  │ Load Parquet│  关系型查询 (LOAD...RETURN) │  │
│  │  [M1]       │  [M2]       │  [M3]       │  [M4]                       │  │
│  └──────┬──────┴──────┬──────┴──────┬──────┴──────────────┬──────────────┘  │
│         │             │             │                     │                 │
│         └─────────────┴─────────────┴─────────────────────┘                 │
│                                     │                                       │
│                              RecordBatch (统一中间抽象)                      │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
┌─────────────────────────────────────┼───────────────────────────────────────┐
│  P2: 临时图载入 (依赖 P1)            │                                       │
│  ┌──────────────────────────────────┴────────────────────────────────────┐  │
│  │                                                                       │  │
│  │  ┌────────────────────┐  ┌────────────────────┐  ┌─────────────────┐  │  │
│  │  │ BatchInsertTemp    │  │ BatchInsertTemp    │  │ 生命周期管理     │  │  │
│  │  │ Vertex [M5]        │  │ Edge [M6]          │  │ [M7]            │  │  │
│  │  └────────────────────┘  └────────────────────┘  └─────────────────┘  │  │
│  │                                                                       │  │
│  │  ┌────────────────────┐  ┌────────────────────┐                       │  │
│  │  │ 统一查询接口 [M8]   │  │ 场景边界控制 [M9]  │                       │  │
│  │  └────────────────────┘  └────────────────────┘                       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  P3: 网络资源支持 (可并行开发，扩展 P1 的数据源)                              │
│  ┌─────────────────────┬─────────────────────┬─────────────────────┐        │
│  │  HTTP 资源 [M10]    │  S3 资源 [M11]      │  OSS 资源 [M12]     │        │
│  └─────────────────────┴─────────────────────┴─────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

**并行开发说明**:
- **P1 内部**: M1(CSV), M2(JSON), M3(Parquet), M4(关系型查询) 可并行开发
- **P2 内部**: M5(临时点), M6(临时边) 需顺序执行；M7-M9 可与 M5/M6 部分并行
- **P3 内部**: M10(HTTP), M11(S3), M12(OSS) 可并行开发

---

## Functional Modules *(mandatory)*

---

## P1: 本地文件 + 关系型查询

> **目标**: 建立 LOAD 功能的基础能力，支持从本地文件读取数据并执行关系型查询。
> **可并行开发**: M1, M2, M3, M4 之间无强依赖，可同时开发。

---

### Module 1: Load CSV (Priority: P1)

**Purpose**: 支持从本地 CSV 文件读取数据，输出统一的中间数据格式 (RecordBatch) 供后续模块使用。新增功能：利用 CSV Reader 重新实现 COPY FROM 功能，替换旧链路实现。

**Why this priority**: CSV 是最常见的数据交换格式，是整个 LOAD 功能的基础能力之一。此外，重新实现 COPY FROM 功能可以提高性能和统一数据流。

**Parallel**: 可与 M2, M3, M4 并行开发

**Independent Test**: 执行 `LOAD FROM "file.csv" RETURN *` 验证 CSV 解析正确性。同时执行 `COPY FROM` 验证新实现。

**Key Components**:

1. **CSV Sniffer (CSV 类型推导器)**: 从 CSV 文件自动推导列名（首行）和列类型（采样推导）。输出 EntrySchema。
2. **CSV Reader (CSV 读取器)**: 解析 CSV 文件内容，支持自定义分隔符、引号、转义字符。输出统一的 RecordBatch 格式。
3. **Predicate Pushdown (谓词下推)**: 支持在读取时应用 WHERE 条件，减少数据传输量。
4. **COPY FROM Reimplementation (COPY FROM 重实现)**: 利用 CSV Reader 功能重新实现原有的 COPY FROM 操作，替换老链路的实现。
5. **Physical Plan Generator (物理计划生成器)**: Compiler 模块生成新的 EntrySchema 对应的物理执行计划。
6. **Reader Function Provider (读取器函数提供者)**: Engine 模块提供新的 Reader Function 支持。

**Functional Requirements**:

1. **FR-101**: 系统 MUST 支持从本地文件系统读取 CSV 文件
2. **FR-102**: 系统 MUST 自动推导 CSV 文件的列名（首行为 header）和列类型
3. **FR-103**: 系统 MUST 支持常见数据类型的自动推导：整数、浮点数、字符串、布尔值、日期时间
4. **FR-104**: 系统 MUST 支持在 LOAD 语句中使用 WHERE 子句进行谓词过滤
5. **FR-105**: 系统 MUST 支持自定义 CSV 格式选项（delimiter, quote, escape, header）
6. **FR-106**: 系统 MUST 重新实现 COPY FROM 操作，使用新的 CSV Reader 功能替代老链路。Compiler 模块需生成对应的 EntrySchema 物理执行计划，Engine 模块需提供新的 Reader Function 支持

**Acceptance Scenarios**:

1. **Given** 标准 CSV 文件（逗号分隔，首行为列名），**When** 执行 `LOAD FROM "file.csv" RETURN *`，**Then** 系统正确解析所有列和行
2. **Given** 包含 `id(int), name(string), value(float)` 的 CSV 文件，**When** 执行 LOAD，**Then** 系统推导出正确的列类型
3. **Given** 管道符分隔的 CSV 文件，**When** 执行 `LOAD FROM "file.csv" (delimiter="|") RETURN *`，**Then** 系统正确解析
4. **Given** CSV 文件用于 COPY FROM 操作，**When** 执行 `COPY FROM` 语句，**Then** 系统使用新的 CSV Reader 链路完成操作，Compiler 模块生成对应的物理执行计划，Engine 模块提供新的 Reader Function 支持

**Test Strategy**:

- **Unit Tests**: 类型推导准确性；分隔符和转义字符处理；边界值（空文件、单行、大文件）；COPY FROM 新旧链路对比
- **Integration Tests**: LOAD → RETURN 完整流程；COPY FROM 端到端功能验证及 Compiler 和 Engine 模块集成验证

---

### Module 2: Load JSON/JSONL (Priority: P1)

**Purpose**: 支持从本地 JSON 和 JSONL（JSON Lines）文件读取数据，输出统一的中间数据格式。

**Why this priority**: JSON 是 Web 和 API 数据的标准格式，JSONL 适合大规模数据处理。与 CSV 同为基础格式能力。

**Parallel**: 可与 M1, M3, M4 并行开发

**Independent Test**: 执行 `LOAD FROM "file.json" RETURN *` 或 `LOAD FROM "file.jsonl" RETURN *` 验证 JSON 解析正确性。

**Key Components**:

1. **JSON Sniffer (JSON 类型推导器)**: 从 JSON/JSONL 文件推导 Schema，处理嵌套结构的扁平化。
2. **JSON Reader (JSON 读取器)**: 解析 JSON 数组或 JSONL 格式，输出 RecordBatch。
3. **Nested Field Handler (嵌套字段处理器)**: 支持点号语法访问嵌套字段（如 `user.address.city`）。

**Functional Requirements**:

1. **FR-201**: 系统 MUST 支持从本地文件系统读取 JSON 文件（数组格式）
2. **FR-202**: 系统 MUST 支持从本地文件系统读取 JSONL 文件（每行一个 JSON 对象）
3. **FR-203**: 系统 MUST 自动推导 JSON 对象的字段名和类型
4. **FR-204**: 系统 MUST 支持嵌套字段的访问（通过点号语法）
5. **FR-205**: 系统 MUST 在遇到 Schema 不一致的行时提供合理的处理策略（填充 NULL 或报错）

**Acceptance Scenarios**:

1. **Given** JSON 数组文件 `[{"id":1,"name":"A"},{"id":2,"name":"B"}]`，**When** 执行 `LOAD FROM "file.json" RETURN *`，**Then** 系统返回 2 行数据
2. **Given** JSONL 文件（每行一个 JSON 对象），**When** 执行 `LOAD FROM "file.jsonl" RETURN *`，**Then** 系统正确解析每行
3. **Given** 嵌套 JSON `{"user":{"name":"A"}}`，**When** 执行 `LOAD FROM "file.json" RETURN user.name`，**Then** 系统返回 "A"

**Test Strategy**:

- **Unit Tests**: JSON/JSONL 格式识别；嵌套字段解析；Schema 不一致处理
- **Integration Tests**: 大文件 JSONL 流式处理

---

### Module 3: Load Parquet (Priority: P1)

**Purpose**: 支持从本地 Parquet 文件读取数据，利用其自带的 Schema 和列式存储优势。

**Why this priority**: Parquet 是大数据分析的标准格式，支持高效的列裁剪和谓词下推。与 CSV/JSON 同为基础格式能力。

**Parallel**: 可与 M1, M2, M4 并行开发

**Independent Test**: 执行 `LOAD FROM "file.parquet" RETURN col1, col2` 验证 Parquet 解析和列裁剪正确性。

**Key Components**:

1. **Parquet Schema Reader (Parquet Schema 读取器)**: 直接从 Parquet 文件元数据读取 Schema，无需推导。
2. **Parquet Reader (Parquet 读取器)**: 利用 Arrow 读取 Parquet 数据，支持列裁剪和行组过滤。
3. **Column Pruning (列裁剪)**: 根据 RETURN 子句只读取需要的列，提升性能。

**Functional Requirements**:

1. **FR-301**: 系统 MUST 支持从本地文件系统读取 Parquet 文件
2. **FR-302**: 系统 MUST 直接使用 Parquet 文件自带的 Schema（无需推导）
3. **FR-303**: 系统 MUST 支持列裁剪（Column Pruning），只读取 RETURN 子句指定的列
4. **FR-304**: 系统 MUST 支持谓词下推到 Parquet 行组级别过滤
5. **FR-305**: 系统 MUST 支持 Parquet 的常见压缩格式（snappy, gzip, zstd）

**Acceptance Scenarios**:

1. **Given** 包含 10 列的 Parquet 文件，**When** 执行 `LOAD FROM "file.parquet" RETURN col1, col2`，**Then** 系统只读取 col1 和 col2 两列数据
2. **Given** snappy 压缩的 Parquet 文件，**When** 执行 LOAD，**Then** 系统自动解压并读取
3. **Given** Parquet 文件和 WHERE 条件，**When** 执行带 WHERE 的 LOAD，**Then** 系统利用行组统计信息跳过不符合条件的行组

**Test Strategy**:

- **Unit Tests**: Schema 读取正确性；列裁剪有效性；压缩格式支持
- **Integration Tests**: 大文件读取性能；与 Arrow 的集成

---

### Module 4: 关系型查询 (LOAD ... RETURN) (Priority: P1)

**Purpose**: 支持对外部数据直接执行关系型查询（Project、Filter、GroupBy、OrderBy、Limit），无需载入为临时图。数据不进入图存储，不能与内部点边混合运算。

**Why this priority**: 这是轻量级使用模式，允许用户快速分析外部数据，无需创建临时图结构。与文件格式模块同为 P1 基础能力。

**Parallel**: 可与 M1, M2, M3 并行开发（需要至少一个格式模块完成后才能端到端测试）

**Independent Test**: 执行 `LOAD FROM "file.csv" WHERE x > 10 RETURN col1, SUM(col2) GROUP BY col1 ORDER BY col1 LIMIT 10` 验证关系操作正确性。

**Key Components**:

1. **Query Parser (查询解析器)**: 解析 LOAD ... RETURN 语法，识别 Project、Filter、GroupBy、OrderBy、Limit 子句。
2. **Relational Operators (关系算子)**: 实现标准关系操作，处理 RecordBatch 数据。
3. **Expression Evaluator (表达式求值器)**: 支持算术运算、比较运算、聚合函数（SUM、COUNT、AVG、MIN、MAX）。

**Functional Requirements**:

1. **FR-401**: 系统 MUST 支持 `LOAD FROM <source> RETURN <columns>` 语法进行列裁剪（Project）
2. **FR-402**: 系统 MUST 支持 WHERE 子句进行行过滤（Filter）
3. **FR-403**: 系统 MUST 支持 GROUP BY 子句进行分组聚合
4. **FR-404**: 系统 MUST 支持常见聚合函数：SUM、COUNT、AVG、MIN、MAX
5. **FR-405**: 系统 MUST 支持 ORDER BY 子句进行排序
6. **FR-406**: 系统 MUST 支持 LIMIT 子句限制返回行数
7. **FR-407**: 关系型查询的数据 MUST NOT 进入图存储，仅在执行上下文中处理
8. **FR-408**: 关系型查询 MUST NOT 支持与内部点边的混合运算（MATCH 语句）

**Acceptance Scenarios**:

1. **Given** CSV 文件，**When** 执行 `LOAD FROM "file.csv" RETURN name, value`，**Then** 系统只返回指定的两列
2. **Given** CSV 文件，**When** 执行 `LOAD FROM "file.csv" WHERE value > 100 RETURN *`，**Then** 系统只返回 value > 100 的行
3. **Given** CSV 文件，**When** 执行 `LOAD FROM "file.csv" RETURN category, SUM(amount) GROUP BY category`，**Then** 系统返回按 category 分组的聚合结果
4. **Given** 关系型查询执行完成，**When** 用户尝试 `MATCH (n) RETURN n`，**Then** 查询不会包含刚才 LOAD 的数据

**Test Strategy**:

- **Unit Tests**: 各关系算子的正确性；聚合函数实现；表达式求值
- **Integration Tests**: 复杂查询组合（Filter + GroupBy + OrderBy + Limit）

---

## P2: 临时图载入

> **目标**: 将外部数据载入为临时点/边，支持与持久图混合查询。
> **依赖**: 需要 P1 的文件格式模块完成后才能开始。
> **顺序约束**: M5(临时点) → M6(临时边)，M7-M9 可与 M5/M6 部分并行。

---

### Module 5: 临时点载入 (BatchInsertTempVertex) (Priority: P2)

**Purpose**: 将外部数据载入为临时点，写入 Connection 级别的临时图存储。支持与持久图进行混合查询。

**Why this priority**: 临时点是临时图功能的核心，依赖 P1 的数据读取能力。

**Depends on**: P1 (至少一个文件格式模块)

**Independent Test**: 执行 `LOAD FROM "file.csv" (primary_key="id") AS TempNode` 后，通过 `MATCH (n:TempNode)` 验证数据已写入临时图。

**Key Components**:

1. **Temp Vertex Schema Builder (临时点 Schema 构建器)**: 基于 EntrySchema 构建临时点类型定义，包含 label、primary_key、属性列表。
2. **BatchInsertTempVertex Operator (批量临时点插入算子)**: 将 RecordBatch 数据批量写入临时图存储，注册 Schema 并分配临时 label_id。
3. **Temp Vertex Storage (临时点存储)**: Connection 级别的点数据存储，与基图隔离。

**Functional Requirements**:

1. **FR-501**: 系统 MUST 支持 `LOAD FROM <source> (primary_key=<col>) AS <label>` 语法载入临时点
2. **FR-502**: 系统 MUST 在 Connection 本地存储中注册临时点的 Schema
3. **FR-503**: 临时点的 label_id MUST 与持久图的 label_id 不冲突（从 max-1 倒序编码）
4. **FR-504**: 系统 MUST 支持在 LOAD AS 之前使用 WHERE 子句过滤要载入的数据
5. **FR-505**: 载入的临时点 MUST 可通过 MATCH 语句查询，与持久点语法一致

**Acceptance Scenarios**:

1. **Given** CSV 文件，**When** 执行 `LOAD FROM "file.csv" (primary_key="id") AS TempUser`，**Then** 创建临时点类型 `TempUser`，可通过 `MATCH (n:TempUser)` 查询
2. **Given** CSV 文件和 WHERE 条件，**When** 执行 `LOAD FROM "file.csv" (primary_key="id") WHERE value > 100 AS TempNode`，**Then** 只有符合条件的数据被载入为临时点
3. **Given** 已存在持久点类型 `Person`，**When** 创建同名临时点 `Person`，**Then** 系统返回冲突错误

**Test Strategy**:

- **Unit Tests**: Schema 构建正确性；label_id 编码逻辑；主键唯一性检查
- **Integration Tests**: LOAD AS → MATCH 完整流程

---

### Module 6: 临时边载入 (BatchInsertTempEdge) (Priority: P2)

**Purpose**: 将外部数据载入为临时边，建立临时点之间以及临时点与持久点之间的关联关系。

**Why this priority**: 临时边依赖 M5 临时点功能，是构建完整临时图结构的关键。

**Depends on**: M5 (临时点载入)

**Independent Test**: 在临时点存在的前提下，执行 LOAD EDGE AS 后，通过路径查询验证边已创建。

**Key Components**:

1. **Temp Edge Schema Builder (临时边 Schema 构建器)**: 构建边类型定义，包含边标签三元组、源/目标点类型、关联列映射。
2. **Bridging Edge Resolver (桥接边解析器)**: 在写入桥接边时，根据关联列在持久图或临时图中查询对应的点 ID。
3. **BatchInsertTempEdge Operator (批量临时边插入算子)**: 将边数据批量写入临时图存储，处理三种边类型。

**Functional Requirements**:

1. **FR-601**: 系统 MUST 支持 `LOAD EDGE FROM <source> (from=<src_label>, to=<dst_label>, from_col=<col>, to_col=<col>) AS <label>` 语法载入临时边
2. **FR-602**: 系统 MUST 支持三种临时边类型：
   - 临时点 ↔ 临时点（纯临时边）
   - 临时点 → 持久点（出向桥接边）
   - 持久点 → 临时点（入向桥接边）
3. **FR-603**: 系统 MUST 在写入桥接边时，根据关联列查询对应的点 ID 进行关联
4. **FR-604**: 当桥接边的目标/源点不存在时，系统 MUST 跳过该边并记录警告
5. **FR-605**: 载入的临时边 MUST 可通过路径模式查询，支持与持久边混合遍历

**Acceptance Scenarios**:

1. **Given** 临时点 `TempUser` 和持久点 `Product`，**When** 执行边载入语句，**Then** 创建桥接边，可通过 `MATCH (u:TempUser)-[r:TempPurchased]->(p:Product)` 查询
2. **Given** 两个临时点类型 `TempA` 和 `TempB`，**When** 载入连接它们的边，**Then** 系统创建纯临时边
3. **Given** 边文件中存在引用不存在的点 ID，**When** 执行载入，**Then** 系统跳过这些边并输出警告

**Test Strategy**:

- **Unit Tests**: 三种边类型的正确创建；桥接边 ID 解析逻辑
- **Integration Tests**: 临时点→桥接边→持久点 的完整路径查询

---

### Module 7: 生命周期管理 (Priority: P2)

**Purpose**: 管理临时点边数据的生命周期，确保资源正确绑定到 Connection 并在适当时机自动清理。

**Why this priority**: 生命周期管理是系统稳定性的关键。

**Parallel**: 可与 M5/M6 部分并行开发（基础框架可先行，与存储集成需等 M5/M6）

**Independent Test**: 创建 Connection → 载入临时数据 → 关闭 Connection → 验证数据已清理。

**Key Components**:

1. **ConnectionGraph (连接级图存储)**: 封装基图引用和本地临时存储，维护 Connection 级别的 Schema 和数据表。
2. **Temporary Schema Registry (临时 Schema 注册表)**: 维护当前 Connection 的所有临时点/边类型定义。
3. **Resource Cleaner (资源清理器)**: 在 Connection 关闭时自动释放所有临时图资源。

**Functional Requirements**:

1. **FR-701**: 临时点边的生命周期 MUST 绑定到当前 Connection
2. **FR-702**: Connection 关闭时 MUST 自动清理该连接的所有临时点边数据
3. **FR-703**: 用户 MUST 能够通过 `DROP TABLE <label>` 显式删除临时点或边类型，删除临时点的时候，如果其有对应临时边，也一并删除
4. **FR-704**: 多个 Connection 的临时存储 MUST 相互隔离
5. **FR-705**: 系统 MUST 在异常退出时尽可能清理临时文件

**Acceptance Scenarios**:

1. **Given** 已载入临时点的 Connection，**When** 关闭该 Connection，**Then** 临时点数据被自动清理
2. **Given** 已载入临时边，**When** 执行 `DROP TABLE TempEdge`，**Then** 临时边被删除

**Test Strategy**:

- **Unit Tests**: DROP TABLE 执行；Connection 关闭时的资源释放
- **Integration Tests**: 多 Connection 隔离性测试

---

### Module 8: 统一查询接口 (Priority: P2)

**Purpose**: 提供统一的图查询接口，使查询层无需区分临时图还是持久图，实现透明的混合查询。

**Why this priority**: 统一查询是用户体验的核心。

**Parallel**: 可与 M5/M6 部分并行开发（接口设计可先行，实现需等 M5/M6）

**Independent Test**: 执行涉及临时和持久元素的混合 MATCH 查询，验证结果正确性。

**Key Components**:

1. **Unified Schema View (统一 Schema 视图)**: 合并基图 Schema 和临时 Schema，对外提供单一视图。
2. **Transparent Scan/Expand (透明扫描/展开)**: 支持对临时和持久数据的统一操作。

**Functional Requirements**:

1. **FR-801**: 系统 MUST 提供统一的 Schema 视图
2. **FR-802**: MATCH 语句 MUST 能够透明地引用临时点/边标签
3. **FR-803**: Scan、Expand 算子 MUST 能够统一操作临时图和持久图数据
4. **FR-804**: 跨临时图和持久图的路径查询 MUST 正确返回完整结果

**Acceptance Scenarios**:

1. **Given** 临时点、临时边、持久点、持久边，**When** 执行跨越它们的路径查询，**Then** 系统正确返回结果
2. **Given** 临时点和持久点有同名属性，**When** 查询该属性，**Then** 根据变量绑定返回正确值

**Test Strategy**:

- **Unit Tests**: Schema 合并逻辑；label_id 路由
- **Integration Tests**: 复杂混合查询

---

### Module 9: 场景边界控制 (Priority: P2)

**Purpose**: 确保临时图功能只在允许的场景下可用，在不支持的场景下返回清晰的错误信息。

**Why this priority**: 边界控制是系统健壮性的保障。

**Parallel**: 可与 M5-M8 并行开发

**Independent Test**: 在 read-only 模式或 TP 模式下执行 LOAD AS，验证返回正确的错误信息。

**Key Components**:

1. **Mode Validator (模式验证器)**: 验证当前运行模式和数据库打开模式。
2. **Error Message Provider (错误信息提供器)**: 提供清晰的错误信息。

**Functional Requirements**:

1. **FR-901**: 系统 MUST 仅在 AP（Embedded）模式下支持临时点边功能（LOAD AS）
2. **FR-902**: 系统 MUST 仅在 read-write 模式下支持临时点边功能（LOAD AS）
3. **FR-903**: 关系型查询（LOAD ... RETURN 不带 AS）在 read-only 模式下 MAY 支持
4. **FR-904**: 系统 MUST 在不支持的场景下返回明确的错误信息

**Acceptance Scenarios**:

1. **Given** read-only 模式，**When** 执行 LOAD AS，**Then** 返回错误 "Temporary graph requires read-write mode"
2. **Given** TP 服务模式，**When** 执行 LOAD AS，**Then** 返回错误 "Temporary graph is only supported in AP mode"
3. **Given** read-only 模式，**When** 执行 `LOAD FROM "file.csv" RETURN *`（不带 AS），**Then** 查询正常执行

**Test Strategy**:

- **Unit Tests**: 各种模式组合的验证结果
- **Integration Tests**: E2E 错误信息验证

---

## P3: 网络资源支持

> **目标**: 扩展数据源范围，支持从网络资源（HTTP/S3/OSS）读取数据。
> **依赖**: 可独立于 P2 开发，但需要 P1 的格式模块作为基础。
> **可并行开发**: M10, M11, M12 之间无强依赖，可同时开发。

---

### Module 10: HTTP 资源访问 (Priority: P3)

**Purpose**: 支持通过 HTTP/HTTPS 协议访问远程文件资源，扩展数据源范围。

**Why this priority**: HTTP 是最通用的网络协议，支持大量公开数据源。

**Parallel**: 可与 M11, M12 并行开发

**Independent Test**: 执行 `LOAD FROM "https://example.com/data.csv" RETURN *` 验证远程文件下载和解析。

**Key Components**:

1. **HTTP Client (HTTP 客户端)**: 支持 HTTP/HTTPS GET 请求，处理重定向、超时、重试。
2. **Stream Adapter (流适配器)**: 将 HTTP 响应流转换为文件格式 Reader 可处理的输入流。
3. **URL Parser (URL 解析器)**: 识别 HTTP/HTTPS URL，提取主机、路径、参数。

**Functional Requirements**:

1. **FR-1001**: 系统 MUST 支持从 HTTP/HTTPS URL 读取文件
2. **FR-1002**: 系统 MUST 根据 URL 扩展名或 Content-Type 自动识别文件格式（CSV/JSON/Parquet）
3. **FR-1003**: 系统 MUST 支持 HTTP 重定向（301/302/307）
4. **FR-1004**: 系统 MUST 支持配置超时时间和重试次数
5. **FR-1005**: 系统 MUST 支持 HTTPS 证书验证（可配置跳过）

**Acceptance Scenarios**:

1. **Given** 公开的 CSV 文件 URL，**When** 执行 `LOAD FROM "https://example.com/data.csv" RETURN *`，**Then** 系统下载并解析文件
2. **Given** 返回 JSON 的 API URL，**When** 执行 `LOAD FROM "https://api.example.com/users" RETURN *`，**Then** 系统识别 JSON 格式并解析
3. **Given** 需要重定向的 URL，**When** 执行 LOAD，**Then** 系统自动跟随重定向获取最终资源

**Test Strategy**:

- **Unit Tests**: URL 解析；HTTP 状态码处理；超时和重试逻辑
- **Integration Tests**: Mock HTTP 服务器集成测试

---

### Module 11: S3 资源访问 (Priority: P3)

**Purpose**: 支持从 AWS S3 存储桶读取文件，满足云端数据分析场景。

**Why this priority**: S3 是最流行的云存储服务，是企业数据湖的常见选择。

**Parallel**: 可与 M10, M12 并行开发

**Independent Test**: 配置 S3 凭证后，执行 `LOAD FROM "s3://bucket/path/data.csv" RETURN *` 验证 S3 访问。

**Key Components**:

1. **S3 Client (S3 客户端)**: 封装 AWS SDK，支持 GetObject 操作和分段下载。
2. **Credential Manager (凭证管理器)**: 支持多种凭证来源（环境变量、配置文件、IAM Role）。
3. **S3 URL Parser (S3 URL 解析器)**: 解析 `s3://bucket/key` 格式的 URL。

**Functional Requirements**:

1. **FR-1101**: 系统 MUST 支持从 S3 存储桶读取文件（`s3://bucket/path/file`）
2. **FR-1102**: 系统 MUST 支持多种 S3 凭证配置方式（环境变量、配置文件、显式传入）
3. **FR-1103**: 系统 MUST 支持配置 S3 区域（Region）和端点（Endpoint）
4. **FR-1104**: 系统 MUST 支持大文件的分段下载（避免内存溢出）
5. **FR-1105**: 系统 MUST 在凭证无效或权限不足时返回清晰的错误信息

**Acceptance Scenarios**:

1. **Given** 有效的 S3 凭证和存储桶权限，**When** 执行 `LOAD FROM "s3://mybucket/data/file.csv" RETURN *`，**Then** 系统成功读取文件
2. **Given** 无效的 S3 凭证，**When** 执行 LOAD，**Then** 系统返回 "Invalid AWS credentials" 错误
3. **Given** 大型 Parquet 文件（>1GB），**When** 执行 LOAD，**Then** 系统流式下载，不会内存溢出

**Test Strategy**:

- **Unit Tests**: URL 解析；凭证加载逻辑；错误处理
- **Integration Tests**: LocalStack 或真实 S3 集成测试

---

### Module 12: OSS 资源访问 (Priority: P3)

**Purpose**: 支持从阿里云 OSS 存储桶读取文件，满足国内云环境的数据分析场景。

**Why this priority**: OSS 是国内最流行的云存储服务。

**Parallel**: 可与 M10, M11 并行开发

**Independent Test**: 配置 OSS 凭证后，执行 `LOAD FROM "oss://bucket/path/data.csv" RETURN *` 验证 OSS 访问。

**Key Components**:

1. **OSS Client (OSS 客户端)**: 封装阿里云 OSS SDK，支持 GetObject 操作。
2. **OSS Credential Manager (OSS 凭证管理器)**: 支持 AccessKey、STS Token、RAM Role 等认证方式。
3. **OSS URL Parser (OSS URL 解析器)**: 解析 `oss://bucket/key` 格式的 URL。

**Functional Requirements**:

1. **FR-1201**: 系统 MUST 支持从 OSS 存储桶读取文件（`oss://bucket/path/file`）
2. **FR-1202**: 系统 MUST 支持 AccessKeyId/AccessKeySecret 认证
3. **FR-1203**: 系统 MUST 支持配置 OSS Endpoint（区分内网/外网）
4. **FR-1204**: 系统 MUST 支持 STS 临时凭证
5. **FR-1205**: 系统 MUST 在凭证无效或权限不足时返回清晰的错误信息

**Acceptance Scenarios**:

1. **Given** 有效的 OSS 凭证和存储桶权限，**When** 执行 `LOAD FROM "oss://mybucket/data/file.csv" RETURN *`，**Then** 系统成功读取文件
2. **Given** 内网环境的 OSS Endpoint 配置，**When** 执行 LOAD，**Then** 系统使用内网地址访问 OSS
3. **Given** STS 临时凭证，**When** 执行 LOAD，**Then** 系统正确使用临时凭证访问

**Test Strategy**:

- **Unit Tests**: URL 解析；凭证加载逻辑；Endpoint 配置
- **Integration Tests**: 真实 OSS 或 Mock 服务集成测试

---

### Module 13: Copy From With No Schema (Priority: P2)

**设计细化（Binder / TableInfo）**: [`table-info-copy-from-design.md`](./table-info-copy-from-design.md)

**用户说明（英文）**: [`doc/source/data_io/import_data.md`](../../doc/source/data_io/import_data.md) 中「COPY FROM without a predefined schema」一节。

#### 目标与范围

在目标点类型或边类型**尚未存在于 catalog** 时，仍可通过 `COPY ... FROM` 完成「推断列结构 → 可选自动建表 → 批量写入」；用户无需先手写 `CREATE NODE/REL TABLE`。本模块描述该路径下的**用户面语法、流水线阶段与物理计划形态**；与「已有 Schema 的 COPY」共用同一套 DML 读取与 `BatchInsert*` 写入语义，差异主要在 Binder 是否产出补充 DDL 与元数据。

#### 端到端流水线

1. **收集（Sniff / Scan）**：对外部数据源执行 sniff 或绑定扫描，得到列名与逻辑类型（`EntrySchema` / 等价结构）。
2. **组装（Infer）**：将列映射为点或边的属性列表；推断主键列（默认第一列或与选项一致）；边需同时确定 **边标签 + 源/宿点标签** 三元组。
3. **自动 DDL（可选）**：当 `auto_detect`（或等价策略）为真且类型不存在时，在物理计划中**前置** `CreateVertexSchema` / `CreateEdgeSchema`，在执行期先于 DML 应用。
4. **数据导入（DML）**：`DataSource`（及可能的 `Project` 等关系算子）读出行批，**`BatchInsertVertex` / `BatchInsertEdge`** 将列绑定到属性并调用存储批量写入。

#### Cypher 接口（当前聚焦形态）

**从文件载入点**

```cypher
COPY person FROM 'person.csv';
```

**从文件载入边**（需显式给出端点类型；边类型为单一三元组，暂不支持一次 COPY 声明多种 `(src,dst)` 组合）

```cypher
COPY knows FROM 'knows.csv' (from='person', to='person');
```

**从子查询载入点**

```cypher
COPY person FROM (LOAD FROM 'person.csv' RETURN id, name, age);
```

**从子查询载入边**

```cypher
COPY knows FROM (LOAD FROM 'knows.csv' RETURN src_id, dst_id, weight) (from='person', to='person');
```

上述四种用法均可配合 **schema 自动推断**：当目标类型尚不存在时，由编译与执行路径插入 DDL 再导入。用户可通过选项显式控制：

| 名称 | 含义 | 默认值 |
|------|------|--------|
| `auto_detect` | 当目标类型在 catalog 中不存在时，是否根据 sniff/扫描结果自动创建点/边类型 | `true` |

示例：

```cypher
COPY person FROM 'person.csv' (auto_detect=true);
COPY knows FROM 'knows.csv' (from='person', to='person') (auto_detect=true);
COPY person FROM (LOAD FROM 'person.csv' RETURN id, name, age) (auto_detect=true);
COPY knows FROM (LOAD FROM 'knows.csv' RETURN src_id, dst_id, weight) (from='person', to='person', auto_detect=true);
```

#### 技术实现要点

引擎与存储已提供 DDL、DML 及文件 sniff/read 能力；本功能主要在**查询编译链路**中组合算子，生成可下发的 Physical Plan。

**Sniff / Read 表函数（概念接口）**

```c++
using read_exec_func_t = std::function<execution::Context(
    std::shared_ptr<reader::ReadSharedState> state)>;

// 从外部数据源推断列名与列类型
using read_sniff_func_t = std::function<std::shared_ptr<reader::EntrySchema>(
    const reader::FileSchema& schema)>;

struct ReadFunction : public TableFunction {
  read_exec_func_t execFunc = nullptr;
  read_sniff_func_t sniffFunc = nullptr;

  ReadFunction(std::string name, std::vector<common::LogicalTypeID> inputTypes)
      : TableFunction{std::move(name), std::move(inputTypes)} {}
};
```

**Physical Plan 中的 DDL 算子（节选）**

```proto
message CreateVertexSchema {
    common.NameOrId vertex_type = 1;
    repeated PropertyDef properties = 2;
    repeated string primary_key = 3;
    ConflictAction conflict_action = 4;
}

message CreateEdgeSchema {
    enum Multiplicity { ONE_TO_ONE = 0; ONE_TO_MANY = 1; MANY_TO_ONE = 2; MANY_TO_MANY = 3; }
    message TypeInfo {
        EdgeType edge_type = 1;
        Multiplicity multiplicity = 2;
    }
    repeated TypeInfo type_info = 1;
    repeated PropertyDef properties = 2;
    repeated string primary_key = 3;
    ConflictAction conflict_action = 4;
}
```

**Physical Plan 中的 COPY DML 算子（节选）**

```proto
// COPY User FROM 'user.csv'
message BatchInsertVertex {
    common.NameOrId vertex_type = 1;
    repeated PropertyMapping property_mappings = 2;
}

// COPY Knows FROM 'knows_user_user.csv' (from='User', to='User');
message BatchInsertEdge {
    EdgeType edge_type = 1;
    repeated PropertyMapping source_vertex_binding = 2;
    repeated PropertyMapping destination_vertex_binding = 3;
    repeated PropertyMapping property_mappings = 4;
}
```

**执行期解析标签（与无 Schema COPY 强相关）**

`BatchInsertVertex` 携带 `common.NameOrId`，`BatchInsertEdge` 携带完整 `EdgeType`（边名 + 源/宿 `NameOrId`）。**Build 阶段不把名称解析为数值 label_id**；在 **`Eval` 阶段**根据**当前图上的 `Schema`** 解析 id 或 name。这样在同一物理计划中「先 `Create*Schema`、后 `BatchInsert*`」时，插入算子总能看到已注册的类型，避免计划构建时刻 catalog 尚未刷新的问题。

#### 编译侧：Binder 与 Planner

- **Binder**：`bindCopyFrom` 在「需要从无推断建表」时，构造 `BoundCopyFromInfo`，并填充 **`ddlTableInfo`**（如 `DDLVertexInfo` / `DDLEdgeInfo`），内含可下发为 `CreateVertexSchema` / `CreateEdgeSchema` 的 `BoundCreateTableInfo` 及临时 `TableCatalogEntry`；扫描源绑定与列表达式与常规 COPY 一致。
- **Planner / GOPT / `plan_copy`**：若存在 `ddlTableInfo`，在 COPY 子计划前插入对应 **Create*Schema** 物理算子，再衔接 **DataSource →（可选 Project）→ BatchInsert***。

（具体字段与分支以 `bound_copy_from.h`、`bind_copy_from.cpp`、`plan_copy.cpp`、`g_query_converter.cpp` 为准。）

#### 示例：点 COPY + 自动建表

数据文件 `/data/legacy_users.csv`：

```csv
user_id,user_name,total_spent
1,user1,20.0
2,user2,21.2
3,user3,21.4
```

```cypher
COPY ExtLegacyUser FROM '/data/legacy_users.csv' (auto_detect=true);
```

典型 Physical Plan 形态（示意；谓词下推等到 Reader 的细节以实现为准）：

```yaml
CreateVertexSchema:
  entry_schema:
    label: "ExtLegacyUser"
    primary_col: "user_id"
    column_names: ["user_id", "user_name", "total_spent"]
    column_types: ["uint64_t", "string", "double"]

DataSource:
  entry_schema:
    column_names: ["user_id", "user_name", "total_spent"]
    column_types: ["uint64_t", "string", "double"]
  file_schema:
    file_path:
      - "/data/legacy_users.csv"
    format_type: "csv"
    format_opts:
      header: true
      delimiter: "|"

BatchInsertVertex:
  vertex_type: { name: "ExtLegacyUser" }   # 执行期解析为 label_id
  property_mappings: # 列下标 → 属性名，以实现为准
    ...
```

#### 示例：列顺序与主键列 — 经子查询重排

同一数据若文件中 **主键不是第一列**，可通过 `LOAD ... RETURN` 重排后再 COPY，使推断的主键与属性顺序一致：

```csv
user_name,user_id,total_spent
user1,1,20.0
user2,2,21.2
user3,3,21.4
```

```cypher
COPY ExtLegacyUser FROM (
  LOAD FROM '/data/legacy_users.csv' RETURN user_id, user_name, total_spent
) (auto_detect=true);
```

典型 Physical Plan 在 `DataSource` 与 `BatchInsertVertex` 之间增加 **Project**，将扫描列投影为 `user_id, user_name, total_spent`；`CreateVertexSchema` 与 `BatchInsertVertex` 所见的逻辑列序一致，`BatchInsertVertex` 仍通过 **name 或 id** 在执行期绑定到已创建的 `ExtLegacyUser`。

```yaml
CreateVertexSchema:
  entry_schema:
    label: "ExtLegacyUser"
    primary_col: "user_id"
    column_names: ["user_id", "user_name", "total_spent"]
    column_types: ["uint64_t", "string", "double"]

DataSource:
  entry_schema:
    column_names: ["user_name", "user_id", "total_spent"]
    column_types: ["string", "uint64_t", "double"]
  file_schema:
    file_path:
      - "/data/legacy_users.csv"
    format_type: "csv"
    format_opts:
      header: true
      delimiter: "|"

Project:
  column_names: ["user_id", "user_name", "total_spent"]
  column_types: ["uint64_t", "string", "double"]

BatchInsertVertex:
  vertex_type: { name: "ExtLegacyUser" }
  property_mappings: ...
```

---

## Edge Cases

- **文件不存在**：返回 "File not found: <path>" 错误
- **网络超时**：返回 "Connection timeout accessing <url>" 错误，建议重试
- **格式错误**：返回具体的解析错误位置和原因
- **主键列不存在**：返回 "Column '<col>' not found in schema" 错误
- **类型推导失败**：返回 "Cannot infer type for column '<col>'" 错误
- **重复的临时表名**：返回 "Temporary table '<name>' already exists" 错误
- **凭证无效**：返回 "Invalid credentials for <service>" 错误
- **内存不足**：返回 "Out of memory" 错误，建议使用 LIMIT 或分批处理
- **桥接边目标点不存在**：跳过该边，记录警告日志

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 本地 CSV 文件（百万行）的 LOAD ... RETURN 查询在 10 秒内完成
- **SC-002**: 本地 Parquet 文件的列裁剪查询性能提升 50% 以上（相比读取全部列）
- **SC-003**: 临时点/边的查询性能与等量持久图数据差异不超过 20%
- **SC-004**: Connection 关闭后，临时数据内存在 1 秒内完全释放
- **SC-005**: HTTP/S3/OSS 资源访问在网络正常时成功率达到 99%
- **SC-006**: 所有图查询语法能够无差别应用于临时图和持久图
- **SC-007**: 在不支持的模式下使用临时图功能时，100% 返回清晰错误信息
- **SC-008**: 多 Connection 并发使用时，各自的临时数据完全隔离
- **SC-009**: 所有模块的单元测试覆盖率达到 80% 以上
- **SC-010**: 端到端流程的集成测试全部通过

---

## Assumptions

- 外部数据文件格式规范（CSV 有 header，JSON 格式正确，Parquet 包含 Schema）
- 网络资源（HTTP/S3/OSS）在访问时可用，凭证有效
- 临时数据量在单机内存可承载范围内（建议不超过可用内存的 50%）
- 桥接边的目标持久点已存在于图中
- read-write 模式下只有一个 Connection 可以写入

---

## Out of Scope

- TP（Service）模式下的临时点边支持（LOAD AS）
- 临时点边的 Transaction 控制（WAL、ACID）
- 临时数据的跨 Connection 共享
- 临时数据的磁盘持久化与恢复
- ORC、Avro 等其他文件格式的支持
- HDFS 文件系统的支持
- 临时点边的 Schema 修改（ALTER TABLE）
- 临时边的方向反转查询优化
- 临时图的索引创建
