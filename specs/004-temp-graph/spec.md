# Feature Specification: AP 链路临时图

**Feature Branch**: `008-temp-graph`
**Created**: 2026-06-10
**Status**: Draft
**Input**: User description: "在 AP 链路的 read-write Connection 上支持临时图计算：LOAD AS 语法从外部文件创建临时节点/边表，与持久化图统一 Cypher 查询，会话结束自动释放。"

## Functional Modules *(mandatory)*

### Module 1: LOAD AS 语法与临时数据接入 (Priority: P1)

**Purpose**: 提供 `LOAD NODE TABLE` / `LOAD EDGE TABLE` 语句，把外部文件物化为会话内的临时节点/边表，并允许临时边的端点引用持久化图中的节点。

**Why this priority**: 整个功能的入口。没有 LOAD AS，临时图既无 schema 也无数据，后续模块都无法运行。

**Independent Test**: 启动一条 AP read-write 连接，执行 `LOAD NODE TABLE FROM 'persons.csv' (primary_key = 'id') AS TempPerson`，通过 schema 查询接口看到 TempPerson 及其字段和行数，且持久化目录下不出现新增数据文件。

**Key Components**:

**LoadAs Parser**: 在 Cypher 语法中新增 `LOAD NODE TABLE FROM <source> (<options>) AS <label>` 和 `LOAD EDGE TABLE FROM <source> (<options>) AS <label>` 规则。独立于现有 LoadFrom ReadingClause（LoadFrom 返回行，LoadAs 写入图，语义不同）。

**LoadAs Binder/Planner**: 校验 label 冲突、类型绑定、边约束，生成 PhysicalPlan（`CreateVertexSchema(temporary=true)` + `DataSource` + `BatchInsertVertex`）。模式门禁（AP READ_WRITE 检查）由 `QueryProcessor` 层通过 `is_read_only_` + `ExecutionFlag.create_temp_table` 统一处理。

**Storage 层 temporary 标记**: `VertexSchema`/`EdgeSchema` 添加 `bool temporary` 字段，区分临时与持久化类型。`CreateVertexTypeParam`/`CreateEdgeTypeParam` 扩展 temporary 字段，复用整条现有 DDL + 数据加载链路。

**Proto 扩展**: `CreateVertexSchema`/`CreateEdgeSchema` message 添加 `bool temporary` 字段。`ExecutionFlag.create_temp_table` 已有，直接复用。

**Dump/Checkpoint 防护**: `Dump()`/`Serialize()`/`DumpToYaml()` 跳过 temporary labels，确保临时数据不写入磁盘。`to_yaml()` 包含 temporary labels（compiler 需要）。`Open()` 检测残留 temporary labels 作为兜底防护。

**Functional Requirements**:

1. **FR-001**: 系统 MUST 支持 `LOAD NODE TABLE FROM <source> (<options>) [WHERE <predicate>] [RETURN <columns>] AS <label>` 创建临时节点表。`<source>` 可以是本地文件路径或受支持的远程 URI。options 包括 `primary_key`、`header`、`auto_detect` 等。可选的 WHERE 子句对源数据做行过滤（filter pushdown），可选的 RETURN 子句指定投影列（projection pushdown）——仅 RETURN 中列出的列成为 vertex properties。若指定了 RETURN，MUST 显式包含 primary_key 列，否则报错。WHERE 中引用但不在 RETURN 中的列仅用于过滤，不成为 property。
2. **FR-002**: 系统 MUST 支持 `LOAD EDGE TABLE FROM <source> (<options>) [WHERE <predicate>] [RETURN <columns>] AS <label>` 创建临时边表。options 中通过 `from`/`to` 指定端点类型，`from_col`/`to_col` 指定外键列。`from`/`to` 可以是临时节点类型或持久化节点类型的任意组合。可选的 WHERE/RETURN 语义同 FR-001；若指定了 RETURN，MUST 显式包含 from_col 和 to_col 列。
3. **FR-003**: LOAD AS 执行时校验：源 URI 可访问、字段类型可解析、端点类型存在；任意一项失败 MUST 整体失败，不得只导入部分数据。已创建的 schema entry 和 table MUST 被回滚。
4. **FR-004**: 临时类型名 MUST 在本会话内唯一；如与已有持久化类型同名，MUST 拒绝创建并提示冲突。
5. **FR-005**: 系统 MUST 拒绝在非 AP read-write 连接上执行 LOAD AS，并返回明确错误。TP service 路径同样拒绝。
6. **FR-006**: 涉及临时点的边 MUST 也是临时边。持久化边不能引用临时点（session 结束后临时点被清理，持久化边会悬空）。
7. **FR-007**: 临时数据 MUST 不写入磁盘。`Dump()`/`Serialize()`/`DumpToYaml()` MUST 跳过 temporary labels。数据库 `Open()` 时如检测到残留 temporary labels MUST 告警并清理。

**Acceptance Scenarios**:

1. **Given** 一条 AP read-write 连接和一个本地 CSV 文件，**When** 执行 `LOAD NODE TABLE FROM 'persons.csv' (primary_key = 'id') AS TempPerson`，**Then** 该连接内可查询到 TempPerson schema 及数据。
2. **Given** 持久化图已有 `Product` 节点，**When** 用户先 LOAD `TempUser`，再 LOAD `TempPurchased` (from = "TempUser", to = "Product")，**Then** 临时边与持久化点形成可查询的关联。
3. **Given** 持久化图已有 `Person` 类型，**When** 用户执行 `LOAD NODE TABLE FROM ... AS Person`，**Then** 语句失败并提示类型名冲突。
4. **Given** LOAD 的源文件不存在，**When** 执行 LOAD AS，**Then** 语句整体失败，无残留 schema entry 和 table。
5. **Given** LOAD 临时数据后执行 Checkpoint，**Then** 临时数据不在磁盘文件中。重启数据库后临时数据不会被恢复。

**Test Strategy**:

- **Unit Tests**: LOAD AS 解析、label 冲突检查、模式门禁、边约束校验、LOAD 失败回滚。
- **Integration Tests**: 端到端 LOAD AS + schema 查询；Checkpoint 后验证磁盘无 temp 数据；重启后验证 temp 不存在。

---

### Module 2: 统一 Cypher 查询接口（含混合查询） (Priority: P2)

**Purpose**: 让用户通过与持久化图完全相同的 Cypher 查询接口，对临时图发起查询，包括纯临时图查询、纯持久化查询、以及混合查询。

**Why this priority**: Module 1 完成后临时数据已可见，但只有 Module 2 才让数据真正"可用"。

**Independent Test**: 准备好临时节点/边表，分别执行纯临时 MATCH、纯持久化 MATCH、混合 MATCH，三种情况都在同一连接内返回正确结果。

**Key Components**:

1. **统一查询入口**: 引擎根据语句中引用到的标签自行判定该标签来自临时图还是持久化图。共享存储方案下 compiler 通过 `to_yaml()` 获取包含 temp labels 的完整 schema，查询路径零改动。
2. **临时图 DML**: 临时 label 上支持 INSERT/UPDATE/DELETE，写入立即对同一连接内的后续语句可见，不提供 ACID 保证。
3. **graph_interface 不变**: `StorageUpdateInterface`/`StorageAPUpdateInterface` 所有接口签名不变，临时图对图接口层完全透明。

**Functional Requirements**:

1. **FR-008**: 系统 MUST 允许同一 Cypher 查询中同时引用临时标签与持久化标签，以一次执行返回完整结果。
2. **FR-009**: 临时图上的 MATCH/WHERE/RETURN/WITH/ORDER BY/LIMIT 等只读 Cypher 子句的语义 MUST 与现有持久化 AP 查询一致。
3. **FR-010**: 系统 MUST 支持在临时标签上执行 INSERT/UPDATE/DELETE 等 DML，写入立即可见，但不提供 ACID 保证。
4. **FR-011**: 当临时类型不存在或已被释放时，引用该标签的查询 MUST 以"未知 label"错误失败，而非返回空结果。
5. **FR-012**: 当临时边引用的持久化端点类型被 DROP 时，现有 `DeleteVertexType()` 会级联删除所有引用该类型的边（包括临时边），后续查询引用该临时边 MUST 报"未知 label"错误。此行为由共享存储天然保证。

**Acceptance Scenarios**:

1. **Given** 已 LOAD `TempUser` 和 `TempPurchased`，**When** 执行 `MATCH (a:TempUser)-[:TempPurchased]->(b:Product) RETURN a.name, b.name`，**Then** 返回正确的混合查询结果。
2. **Given** 已 LOAD `TempUser`，**When** 先 `SET u.name = 'updated'` 再查询，**Then** 第二次查询返回更新后的值。
3. **Given** 已 DROP `TempUser`，**When** 执行引用 `TempUser` 的查询，**Then** 报"未知 label"错误。
4. **Given** 临时边 `TempPurchased` 的 TO 端点指向持久化类型 `Product`，**When** DROP 持久化 `Product`，**Then** `TempPurchased` 被级联删除，后续查询报"未知 label"错误。

**Test Strategy**:

- **Unit Tests**: 标签解析对临时/持久化/不存在三种情况的判定；临时图 DML 后续可见性。
- **Integration Tests**: 端到端纯临时、纯持久化、混合三类查询；持久化 DDL 后跨侧引用的级联删除。

---

### Module 3: 会话级生命周期与 schema 管理 (Priority: P3)

**Purpose**: 管理临时图从"被 LOAD 创建"到"会话结束被释放"整个生命周期，包括显式释放入口、批量清空、连接关闭时的自动清理与缓存刷新。

**Why this priority**: Module 1+2 已提供"能用"的能力，Module 3 保证"用得久不漏"。

**Independent Test**: 启动 AP read-write 连接 → LOAD 多个临时类型 → 关闭连接 → 确认持久化目录无残留、内存释放、重新连接看不到上次的临时类型。

**Key Components**:

1. **显式释放**: 复用现有 `DROP TABLE <label>` 语法删除临时类型，不需要专用 `DROP TEMP` 语法。引擎不区分删除的是临时还是持久化类型——统一处理。
2. **Connection::Close() 自动清理**: 遍历清理所有 temporary labels（先边后点，per-label 错误隔离）+ 刷新查询缓存。

**Functional Requirements**:

1. **FR-013**: `DROP TABLE <label>` MUST 能释放临时类型（统一处理，不区分 temp/persistent）。
2. **FR-014**: Connection 以任何方式结束时（正常关闭、进程退出），系统 MUST 回收所有临时数据，确保不残留到磁盘或后续会话。
3. **FR-015**: Close/DROP 清理完临时 labels 后，MUST 刷新查询缓存（`global_query_cache_->clear()`），确保后续新 Connection 不会命中包含已删除 temp labels 的缓存计划。
4. **FR-016**: 当临时类型被释放后，针对该类型的后续查询 MUST 以"未知 label"错误失败，而不是返回旧缓存结果。

**Acceptance Scenarios**:

1. **Given** 已 LOAD 三个临时类型，**When** 执行 `DROP TABLE TempA`，**Then** 仅 TempA 被释放，其他仍可查询。
2. **Given** 已 LOAD 多个临时类型，**When** Close Connection 后重新 Connect，**Then** 看不到任何上次的临时类型，持久化数据不受影响。
3. **Given** LOAD 临时点 → 执行查询（生成缓存计划）→ Close → 重新 Connect → 执行引用同名 label 的查询，**Then** 不会命中旧缓存计划。

**Test Strategy**:

- **Unit Tests**: 会话 schema 的注册/释放路径；释放后再查询的失败语义；缓存刷新验证。
- **Integration Tests**: 全流程模拟（正常关闭 + 进程退出）下的资源回收与后续会话隔离。

---

### Edge Cases

- **临时类型命名与持久化类型冲突**: LOAD AS 阶段直接拒绝（FR-004），不做静默改名或名称遮蔽。
- **LOAD 中途失败（文件不存在、解析出错）**: 整体失败，回滚已部分创建的 schema entry 和 table（FR-003）。
- **大临时图导致内存压力**: 本特性不提供溢写到磁盘的能力。内存不足时 LOAD 应整体失败而不是部分加载。
- **临时图存活期间发生持久化 DDL**: 共享存储下 `DeleteVertexType()` 级联删除引用该类型的边（含临时边），由 PropertyGraph 现有机制天然保证（FR-012）。
- **Connection Close 时清理部分失败**: per-label 错误隔离，单个 label 清理失败不阻断其他 label，最大限度释放内存。
- **Close 时的查询竞态**: 单 Connection + 同步阻塞 Query() 模型下，Close 被调用时不可能有正在执行的查询，不需要 abort in-flight query。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 对常见本地 CSV/Parquet 数据源，千万行级别的临时节点表能在 LOAD AS 单条语句中一次性加载完成并立即可查询。
- **SC-002**: 混合 Cypher 查询（1 个临时节点类型 + 1 个临时边类型 + 1 个持久化节点类型）端到端完成率 ≥ 99%（仅排除外部数据源不可达类故障）。
- **SC-003**: Connection 以正常或异常方式结束后，临时图占用的进程内存在该会话生命周期结束时被完全回收，持久化目录无残留。
- **SC-004**: 临时图查询结果与基于持久化图（用同样数据导入持久化）执行同样 Cypher 的结果完全一致（行级一致）。
- **SC-005**: 在非 AP read-write 连接上执行 LOAD NODE/EDGE TABLE 时，100% 返回明确错误，不会出现段错误、挂起或静默失败。

## Assumptions

- 本特性只面向 **AP 链路**；TP 链路上的临时图不在本 spec 范围。
- AP 模式下每个数据库实例最多打开**一条 read-write 连接**，不处理跨连接共享/并发写冲突。
- 临时图数据**全量驻留内存**，溢写到磁盘不在范围内。
- 临时图支持 DML 但**不提供 ACID 保证**。

## Design References

- 设计方案详解: [design-proposal.md](./design-proposal.md)
- 存储方案对比: [storage-design-comparison.md](./storage-design-comparison.md)
