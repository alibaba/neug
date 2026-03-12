# Feature Specification: 新增针对insert的elle测试

**Feature Branch**: `003-elle-insert(#1416)`  
**Feature Issue**: [#1416](https://github.com/alibaba/neug/issues/1416)  
**Created**: 2026-01-19  
**Status**: Draft  
**Input**: User description: "新增针对insert的elle测试，主要包含以下两部分：1. read/insert并发的依赖关系。2. read/insert/delete并发的依赖关系。"

## Functional Modules *(mandatory)*

### Module 1: Read/Insert并发依赖关系检测 (Priority: P1)

**Purpose**: 检测read和insert并发操作之间的依赖关系，验证NeuG数据库在允许read和insert并发执行时，是否能够正确维护数据一致性和隔离性。该模块专门针对insert操作作为append操作的特殊性，通过推导偏序关系来构建依赖图，避免因无序结果导致的依赖边数量爆炸问题。

**Why this priority**: Read/Insert并发是NeuG数据库的核心特性之一。与大多数数据库使用全局锁确保insert隔离性不同，NeuG允许read和insert并发执行，这需要专门的测试来验证是否存在并发冲突。该模块是基础测试，为后续的read/insert/delete并发测试提供基础。

**Independent Test**: 可以通过执行并发的read和insert事务序列，记录每个read操作返回的结果集合，通过分析结果集合之间的偏序关系构建依赖图，使用Elle算法的环检测机制检测是否存在并发冲突。测试可以独立运行，不依赖其他模块。

**Key Components**:

1. **Insert操作记录器（Insert Operation Recorder）**: 记录每个insert事务的操作历史，包括插入的节点或边的标识符、插入时间戳、事务提交状态等信息。将insert操作视作对点表/边表的append操作。

2. **Read操作结果收集器（Read Operation Result Collector）**: 收集每个read事务的查询结果，例如执行`MATCH (p: Person) RETURN p.id`时返回的所有person id集合。记录每个read操作的时间戳和返回的结果集合。

3. **偏序关系推导器（Partial Order Deriver）**: 基于read操作返回的结果集合推导偏序关系。不同的read操作结果应该是最终点表的子集（对应于list的前缀），通过read操作的顺序判断元素插入的偏序。例如插入四个点[1,2,3,4]，如果两次read的结果分别是[1]和[1,2,3]，就可以知道1先插入，2/3接着插入，4最后插入。

4. **依赖图构建器（Dependency Graph Builder）**: 基于偏序关系构建依赖图，每个insert事务只会依赖于它前后的两次read事务，大大减少了依赖边的数量。构建包含wr（写读）、rw（读写）和ww（写写）依赖边的依赖图。

5. **Elle环检测器（Elle Cycle Detector）**: 使用Elle算法的环检测机制，在依赖图中检测是否存在环，识别并发冲突和隔离异常。

**Functional Requirements**:

1. **FR-001**: 系统必须能够记录所有insert事务的操作历史，包括插入的节点/边标识符、时间戳和事务状态
2. **FR-002**: 系统必须能够收集所有read事务的查询结果，包括返回的节点/边标识符集合和时间戳
3. **FR-003**: 系统必须能够基于read操作结果集合推导出insert操作的偏序关系
4. **FR-004**: 系统必须能够基于偏序关系构建依赖图，正确识别insert事务之间的依赖关系
5. **FR-005**: 系统必须能够使用Elle算法检测依赖图中的环，识别并发冲突
6. **FR-006**: 系统必须能够生成测试报告，说明检测到的并发冲突和隔离异常

**Acceptance Scenarios**:

1. **Given** 一个空的Person节点表，**When** 并发执行多个insert事务插入节点[1,2,3,4]，同时执行多个read事务查询所有Person节点，**Then** 系统能够基于read结果推导出insert的偏序关系，构建正确的依赖图
2. **Given** 多个并发的insert和read事务，**When** 执行依赖图构建和环检测，**Then** 如果存在并发冲突，系统能够检测到并报告异常
3. **Given** 一个包含偏序关系的依赖图，**When** 执行Elle环检测，**Then** 系统能够正确识别所有环，每个insert事务的依赖边数量应该显著少于全序情况下的依赖边数量

**Test Strategy**:

- **Unit Tests**: 测试偏序关系推导算法，验证给定read结果集合能够正确推导出insert操作的偏序关系
- **Integration Tests**: 测试完整的read/insert并发场景，验证依赖图构建和环检测的正确性
- **Performance Tests**: 验证偏序关系推导能够有效减少依赖边数量，避免依赖边数量爆炸

---

### Module 2: Read/Insert/Delete并发依赖关系检测 (Priority: P2)

**Purpose**: 检测read、insert和delete并发操作之间的依赖关系，验证NeuG数据库在三种操作并发执行时，是否能够正确维护严格可串行化（Strict Serializability）。该模块利用NeuG的全局写唯一机制，将delete事务作为barrier划分时间块，在每个时间块内仅包含insert和read操作，将问题退化为Module 1的情况。

**Why this priority**: Read/Insert/Delete并发是最复杂的并发场景，需要验证NeuG的严格可串行化保证。该模块依赖Module 1的偏序关系推导能力，但引入了delete操作带来的额外复杂性。由于NeuG的写操作独占机制，每个delete事务作为barrier可以简化依赖关系的构建。

**Independent Test**: 可以通过执行并发的read、insert和delete事务序列，利用NeuG的全局写唯一机制，将delete事务作为时间barrier划分操作块，在每个块内应用Module 1的偏序关系推导方法，构建依赖图并检测并发冲突。测试可以独立运行，但需要Module 1的基础能力。

**Key Components**:

1. **Delete操作记录器（Delete Operation Recorder）**: 记录每个delete事务的操作历史，包括删除的节点或边的标识符、删除时间戳、事务提交状态等信息。

2. **时间块划分器（Time Block Divider）**: 利用NeuG的全局写唯一机制，根据delete事务的时间戳将整个操作序列划分为多个时间块。每个delete事务作为一个barrier，将操作序列分为delete之前和delete之后两个部分。

3. **线性一致性依赖检测器（Linearizability Dependency Detector）**: 根据线性一致性的定义，如果事务a的结束时间早于事务b的开始时间，那么存在事务a到事务b的线性一致性依赖。由于NeuG的全局写唯一机制，每个写事务（包括delete）必定是独占发生的，可以作为barrier将所有的insert和read操作根据时间进行划分。

4. **分块依赖图构建器（Blocked Dependency Graph Builder）**: 在每个时间块内，仅包含insert和read操作，应用Module 1的偏序关系推导方法构建依赖图。read之间构成全序关系，因为每个时间块内的read操作结果应该是一致的（不考虑块内的insert操作）。

5. **跨块依赖连接器（Cross-Block Dependency Connector）**: 连接不同时间块之间的依赖关系，包括delete事务与前后时间块中操作之间的依赖关系。

6. **严格可串行化验证器（Strict Serializability Verifier）**: 基于完整的依赖图，验证NeuG是否满足严格可串行化要求，检测是否存在违反线性一致性的情况。

**Functional Requirements**:

1. **FR-007**: 系统必须能够记录所有delete事务的操作历史，包括删除的节点/边标识符、时间戳和事务状态
2. **FR-008**: 系统必须能够利用NeuG的全局写唯一机制，将delete事务作为barrier划分时间块
3. **FR-009**: 系统必须能够检测事务之间的线性一致性依赖关系，基于事务的开始和结束时间
4. **FR-010**: 系统必须能够在每个时间块内应用偏序关系推导，构建块内的依赖图
5. **FR-011**: 系统必须能够连接不同时间块之间的依赖关系，构建完整的依赖图
6. **FR-012**: 系统必须能够验证严格可串行化，检测是否存在违反线性一致性的情况
7. **FR-013**: 系统必须能够生成测试报告，说明检测到的严格可串行化违反

**Acceptance Scenarios**:

1. **Given** 一个包含Person节点的图，**When** 并发执行insert、read和delete事务，**Then** 系统能够正确识别delete事务作为barrier，将操作序列划分为多个时间块
2. **Given** 多个时间块，**When** 在每个块内应用偏序关系推导，**Then** 系统能够正确构建每个块内的依赖图，read之间构成全序关系
3. **Given** 完整的依赖图包含跨块依赖，**When** 执行严格可串行化验证，**Then** 系统能够正确检测是否存在违反线性一致性的情况
4. **Given** 最终结果为[1,2,4,5]，两次read结果分别是[1,2]和[1,2,3]，**When** 分析依赖关系，**Then** 系统能够正确处理read之间的偏序关系，考虑delete操作的影响

**Test Strategy**:

- **Unit Tests**: 测试时间块划分算法，验证delete事务能够正确作为barrier划分操作序列
- **Integration Tests**: 测试完整的read/insert/delete并发场景，验证依赖图构建和严格可串行化验证的正确性
- **Edge Case Tests**: 测试边界情况，如连续多个delete事务、delete和insert同时操作同一节点等场景

---

### Edge Cases

- 当read操作返回空结果时，如何推导偏序关系？
- 当多个insert事务同时插入相同标识符的节点时，如何处理冲突？
- 当delete事务删除的节点在后续read中仍然出现时，如何检测异常？
- 当read操作的结果集合不是最终结果的严格前缀时，如何处理偏序关系推导？
- 当delete事务和insert事务操作同一节点时，如何确定依赖关系？
- 当时间块划分导致某些操作无法确定所属块时，如何处理？
- 当read操作在delete前后返回的结果集合存在重叠但不完全一致时，如何判断依赖关系？

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 系统能够正确检测read/insert并发场景中的依赖关系，依赖边数量相比全序方法减少至少50%
- **SC-002**: 系统能够正确识别read/insert/delete并发场景中的时间块划分，准确率达到100%
- **SC-003**: 系统能够检测出所有已知的并发冲突模式，检测准确率达到95%以上
- **SC-004**: 测试执行时间在合理范围内，单个测试用例执行时间不超过5分钟
- **SC-005**: 测试报告能够清晰说明检测到的并发冲突和隔离异常，包含完整的依赖图可视化
- **SC-006**: 系统能够处理至少1000个并发事务的测试场景，不出现内存溢出或性能严重退化

## Assumptions

- NeuG数据库支持read和insert并发执行，不需要全局锁
- NeuG数据库的写操作（包括delete）是独占的，同时只有一个写事务可以执行
- Read操作返回的结果集合可能无序，但可以通过集合包含关系推导偏序
- Elle算法的环检测机制已经实现并可用
- 测试环境能够模拟并发事务执行，包括时间戳记录和操作历史收集

## Dependencies

- Module 1依赖现有的Elle测试框架（spec 002）的基础能力，包括操作历史记录和依赖图构建
- Module 2依赖Module 1的偏序关系推导能力
- 两个模块都需要NeuG数据库的并发事务执行能力

## Out of Scope

- 不涉及update操作的并发测试（update操作可以通过现有的Elle测试覆盖）
- 不涉及事务的原子性和持久性测试（由spec 002的其他模块覆盖）
- 不涉及性能基准测试和优化建议
- 不涉及数据库配置和调优
