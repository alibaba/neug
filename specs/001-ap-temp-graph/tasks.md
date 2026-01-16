# Tasks: AP Temporary Graph (临时点边)

**Input**: Design documents from `/specs/001-ap-temp-graph/`
**Prerequisites**: plan.md, spec.md

**Tests**: 每个模块都包含测试任务，以确保独立的可测试性。

**Organization**: Tasks are grouped by module, with each module corresponding to a component from the specification. Each task represents a specific user functionality.

## Initialize: Setup & Shared Infrastructure

**Purpose**: 项目初始化和通用基础组件。

- [ ] F001-T001 定义读取器的通用接口，包括 `EntrySchema` 和 `Reader` 接口
- [ ] F001-T002 [P] 配置 CMake 以包含新的源目录并链接 Apache Arrow 和 RapidJSON
- [ ] F001-T003 为临时图模块设置基础单元测试框架

---

## Module 1: Load CSV (Priority: P1)

**Goal**: 支持从本地 CSV 文件导入和查询数据。新增功能：利用 CSV Reader 重新实现 COPY FROM 功能，替换旧链路实现。

**Independent Test**: 执行 `LOAD FROM "test.csv" RETURN *` 并验证输出是否符合预期的 RecordBatch。同时执行 `COPY FROM` 验证新实现。

- [ ] F001-T101 [P] [M1] 实现从本地文件系统读取 CSV 文件
- [ ] F001-T102 [P] [M1] 实现 CSV 列名 (header) 的自动推导
- [ ] F001-T103 [P] [M1] 实现 CSV 列类型的自动推导（支持整数、浮点数、字符串、布尔值、日期时间）
- [ ] F001-T104 [P] [M1] 实现对自定义 CSV 分隔符 (delimiter) 的支持
- [ ] F001-T105 [P] [M1] 实现对自定义 CSV 引号 (quote) 和转义字符 (escape) 的支持
- [ ] F001-T106 [M1] 实现 CSV 读取时的谓词过滤 (WHERE 子句) 以减少内存占用
- [ ] F001-T107 [M1] 利用 CSV Reader 重新实现 COPY FROM 功能，替换老链路实现。Compiler 模块需生成对应的 EntrySchema 物理执行计划，Engine 模块需提供新的 Reader Function 支持
- [ ] F001-T108 [M1] 验证 COPY FROM 新旧链路的功能等价性

---

## Module 2: Load JSON/JSONL (Priority: P1)

**Goal**: 支持从本地 JSON 和 JSONL 文件导入和查询数据。

**Independent Test**: 执行 `LOAD FROM "test.jsonl" RETURN *` 并验证数据一致性。

- [ ] F001-T201 [P] [M2] 实现从本地文件系统读取 JSON 数组文件
- [ ] F001-T202 [P] [M2] 实现从本地文件系统读取 JSONL (JSON Lines) 文件
- [ ] F001-T203 [P] [M2] 实现 JSON 字段名和类型的自动推断
- [ ] F001-T204 [M2] 实现使用点号语法访问嵌套 JSON 字段
- [ ] F001-T205 [M2] 实现 Schema 不一致时的处理策略（填充 NULL 或报错）


---

## Module 3: Load Parquet (Priority: P1)

**Goal**: 支持从具有列裁剪功能的本地 Parquet 文件导入和查询数据。

**Independent Test**: 执行 `LOAD FROM "test.parquet" RETURN col1, col2` 并验证是否仅读取了指定的列。

- [ ] F001-T301 [P] [M3] 实现从本地文件系统读取 Parquet 文件
- [ ] F001-T302 [P] [M3] 实现从 Parquet 元数据中直接读取 Schema
- [ ] F001-T303 [M3] 实现 Parquet 列裁剪 (Column Pruning)，仅读取 RETURN 指定的列
- [ ] F001-T304 [M3] 实现谓词下推到 Parquet 行组 (Row Group) 级别过滤
- [ ] F001-T305 [P] [M3] 实现对 Parquet 常见压缩格式 (snappy, gzip, zstd) 的支持

---

## Module 4: 关系型查询 (LOAD ... RETURN) (Priority: P1)

**Goal**: 支持对加载的外部数据进行关系操作（投影、过滤、分组聚合等）。

**Independent Test**: 执行复杂查询 `LOAD FROM "f.csv" WHERE x > 10 RETURN x, SUM(y) GROUP BY x` 并验证结果。

- [ ] F001-T401 [M4] 实现 `LOAD FROM <source> RETURN <columns>` 进行列投影
- [ ] F001-T402 [M4] 实现 WHERE 子句进行行过滤
- [ ] F001-T403 [M4] 实现 GROUP BY 子句和常用聚合函数 (SUM, COUNT, AVG, MIN, MAX)
- [ ] F001-T404 [M4] 实现 ORDER BY 子句进行排序输出
- [ ] F001-T405 [M4] 实现 LIMIT 子句限制返回行数
- [ ] F001-T406 [M4] 强制执行关系型查询数据不进入图存储的限制
- [ ] F001-T407 [M4] 通过LOAD RETURN支持COPY FROM的Column Mapping功能
- [ ] F001-T408 [M4] 实现 RETURN 子句中 CAST 函数支持，允许在数据导入时显式指定列的数据类型转换（如 `RETURN CAST(col1, 'INT32'), CAST(col2, 'STRING')`）

---

## Module 5: 临时点载入 (BatchInsertTempVertex) (Priority: P2)

**Goal**: 支持将外部数据作为临时点加载到连接本地（Connection-local）存储中。

**Independent Test**: 执行 `LOAD FROM "v.csv" (primary_key="id") AS TempNode` 后通过 `MATCH (n:TempNode)` 验证其存在。

- [ ] F001-T501 [M5] 实现 `LOAD FROM ... (primary_key=<col>) AS <label>` 语法支持
- [ ] F001-T502 [M5] 实现 Connection 级别的临时点 Schema 注册
- [ ] F001-T503 [M5] 实现临时点标签 ID 的倒序编码逻辑
- [ ] F001-T504 [M5] 实现载入前的 WHERE 谓词过滤支持
- [ ] F001-T505 [M5] 实现临时点可通过标准 MATCH 语句进行查询

---

## Module 6: 临时边载入 (BatchInsertTempEdge) (Priority: P2)

**Goal**: 支持将外部数据作为临时边加载，包括到持久点的桥接边。

**Independent Test**: 执行 `LOAD EDGE FROM "e.csv" (from=TempV, to=PersistV) AS TempE` 并验证路径遍历。

- [ ] F001-T601 [M6] 实现 `LOAD EDGE FROM ... AS <label>` 语法，支持指定源/目标标签和列
- [ ] F001-T602 [M6] 实现三种临时边类型支持：T-T, T-P, P-T
- [ ] F001-T603 [M6] 实现桥接边写入时的持久图/临时图点 ID 关联解析
- [ ] F001-T604 [M6] 实现桥接边关联点不存在时的跳过与警告逻辑
- [ ] F001-T605 [M6] 实现临时边支持与持久边混合的路径模式查询与遍历

---

## Module 7: 生命周期管理 (Priority: P2)

**Goal**: 确保临时数据隔离到连接并能被正确清理。

**Independent Test**: 在连接中加载数据，关闭连接，并验证内存/资源是否释放。

- [ ] F001-T701 [M7] 实现临时点边生命周期与当前 Connection 的强制绑定
- [ ] F001-T702 [M7] 实现 Connection 关闭时的自动数据与资源清理
- [ ] F001-T703 [M7] 实现 `DROP TABLE <label>` 语法及其对关联边的级联删除
- [ ] F001-T704 [M7] 实现多 Connection 间临时存储的完全隔离
- [ ] F001-T705 [M7] 实现系统异常退出时对临时文件的兜底清理机制

---

## Module 8: 统一查询接口 (Priority: P2)

**Goal**: 提供透明的接口来查询混合的持久和临时图数据。

**Independent Test**: 执行 `MATCH (n:TempV)-[r:PersistE]->(m:PersistV)` 并验证路径发现是否正确。

- [ ] F001-T801 [M8] 实现合并基图与临时图的统一 Schema 视图
- [ ] F001-T802 [M8] 实现 MATCH 语句对临时标签的透明引用
- [ ] F001-T803 [M8] 实现 Scan 和 Expand 算子对临时/持久数据的统一路由与操作
- [ ] F001-T804 [M8] 实现跨临时图与持久图的复杂路径查询支持

---

## Module 9: 场景边界控制 (Priority: P2)

**Goal**: 强制执行临时图功能的限制（AP 模式、读写会话）。

**Independent Test**: 在 TP 模式下尝试执行 `LOAD AS` 并验证错误信息。

- [ ] F001-T901 [M9] 限制 `LOAD AS` 仅在 AP (Embedded) 模式下可用
- [ ] F001-T902 [M9] 限制 `LOAD AS` 仅在读写 (read-write) 模式下可用
- [ ] F001-T903 [M9] 实现 read-only 模式下对 `LOAD ... RETURN` 的支持
- [ ] F001-T904 [M9] 实现不支持场景下的清晰错误提示信息

---

## Module 10: HTTP 资源访问 (Priority: P3)

**Goal**: 将数据源扩展到包括 HTTP 资源。

**Independent Test**: 执行 `LOAD FROM "https://example.com/data.csv" RETURN *` 并验证远程数据检索。

- [ ] F001-T1001 [P] [M10] 实现从 HTTP/HTTPS URL 读取远程文件资源
- [ ] F001-T1002 [P] [M10] 实现基于 URL 扩展名或 Content-Type 的格式自动识别
- [ ] F001-T1003 [P] [M10] 实现对 HTTP 重定向 (301/302/307) 的处理
- [ ] F001-T1004 [P] [M10] 实现 HTTP 超时与自动重试机制
- [ ] F001-T1005 [P] [M10] 实现 HTTPS 证书验证及其配置项

---

## Module 11: S3 资源访问 (Priority: P3)

**Goal**: 将数据源扩展到包括 S3 资源。

**Independent Test**: 执行 `LOAD FROM "s3://bucket/data.csv" RETURN *` 并验证远程数据检索。

- [ ] F001-T1101 [P] [M11] 实现从 S3 存储桶读取文件 (`s3://bucket/key`)
- [ ] F001-T1102 [P] [M11] 实现多种 S3 凭证加载方式（环境变量、配置、显式传入）
- [ ] F001-T1103 [P] [M11] 支持配置 S3 Region 和 Endpoint
- [ ] F001-T1104 [P] [M11] 实现大文件的分段流式下载以优化内存
- [ ] F001-T1105 [P] [M11] 实现 S3 凭证无效或权限不足的错误处理

---

## Module 12: OSS 资源访问 (Priority: P3)

**Goal**: 将数据源扩展到包括 OSS 资源。

**Independent Test**: 执行 `LOAD FROM "oss://bucket/data.csv" RETURN *` 并验证远程数据检索。

- [ ] F001-T1201 [P] [M12] 实现从 OSS 存储桶读取文件 (`oss://bucket/key`)
- [ ] F001-T1202 [P] [M12] 支持 AccessKey/Secret 和 STS 临时凭证认证
- [ ] F001-T1203 [P] [M12] 支持配置 OSS Endpoint（内网/外网路由）
- [ ] F001-T1204 [P] [M12] 实现 OSS 访问异常与权限错误的清晰提示

---

## Finalize: Polish & Integration

**Purpose**: Documentation, final testing, and performance optimization.

- [ ] F001-T1301 [P] 更新用户文档和 Cypher 参考手册中的 LOAD 语法
- [ ] F001-T1302 [P] 实现针对百万行级 CSV/Parquet 的端到端集成测试
- [ ] F001-T1303 验证 CSV 自动推导及关系查询在百万行下的性能达标
- [ ] F001-T1304 验证 Parquet 列裁剪带来的性能提升
- [ ] F001-T1305 验证临时点边查询性能与持久数据的差异在 20% 以内
- [ ] F001-T1306 验证多连接并发隔离与资源释放速度
- [ ] F001-T1307 [P] 最终代码审查和重构，确保符合 NeuG 编码标准

---

## Dependencies & Execution Order

### 模块依赖

- **Initialize**: 无依赖 - 必须从这里开始。
- **Module 1-3 (Format Modules)**: Initialize 完成后即可开始。M1, M2, M3 相互独立。
- **Module 4 (关系型查询)**: 依赖至少一个格式模块（M1, M2 或 M3）以进行集成测试。
- **Module 5 (临时点)**: 依赖 Initialize 和至少一个格式模块。
- **Module 6 (临时边)**: 依赖 Module 5 (临时点)。
- **Module 7-9 (生命周期与查询)**: Module 5 完成后即可开始，但完整集成依赖于 Module 6。
- **Module 10-12 (网络资源)**: 依赖 Initialize 和格式模块。独立于 Module 5-9。
- **Module 13 (Finalize)**: 依赖之前的所有模块。

### 并行机会

- 所有标记为 [P] 的任务只要不共享相同的逻辑即可并行运行。
- M1, M2, M3 (CSV, JSON, Parquet) 可以并行开发。
- M10-M12 (网络资源) 可以与 M5-M9 并行开发。

---

## Implementation Strategy

### MVP 优先 (Initialize, Module 1, 4)

1. 完成初始化 (Initialize)。
2. 完成 CSV 支持 (Module 1)。
3. 完成关系型查询 (Module 4)。
4. **验证**: 用户现在可以直接查询本地 CSV 文件，而无需将其加载到图中。

### 增量交付

1. 添加 JSON/Parquet (Module 2, 3) -> 支持更多格式。
2. 添加临时点/边 (Module 5, 6) -> 开启临时图功能。
3. 添加统一查询 (Module 8) -> 开启混合查询。
4. 添加网络支持 (Module 10-12) -> 开启远程数据加载。
