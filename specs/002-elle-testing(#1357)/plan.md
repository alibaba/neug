# Implementation Plan: NeuG数据库事务ACID能力全面测试

**Branch**: `002-elle-testing-spec(#1357)` | **Date**: 2025-12-31 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-elle-testing(#1357)/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

本功能旨在为NeuG数据库实现全面的ACID事务能力测试框架，包括三个核心模块：
1. **原子性（Atomicity）测试模块**：验证事务的原子性保证，包括Atomicity-C测试器（验证提交事务的可见性）和Atomicity-RB测试器（验证回滚事务的不可见性）
2. **隔离性（Isolation）测试模块**：检测7种隔离异常（Dirty Write、Dirty Read、Cut Anomalies、Observed Transaction Vanishes、Fractured Read、Lost Update、Write Skew），使用Elle算法的环检测机制和自定义查询语言
3. **一致性和持久性（Consistency and Durability）测试模块**：验证线性一致性和持久性保证，包括Durability测试器（基于LDBC SNB Interactive工作负载）和Consistency测试器（验证线性一致性，通过RT边和环检测）

技术方法：通过Cypher查询语句执行测试，记录操作历史为三元组格式，构建依赖图并使用Elle算法进行环检测，同时使用自定义查询语言检测无法通过环检测发现的异常。对于线性一致性测试，通过引入实时边（RT边）捕获物理时间顺序约束，使用优化的前沿集合算法避免冗余边，检测包含RT边的环来验证线性一致性违反。

## Technical Context

**Language/Version**: C++17+ / Python 3.8+  
**Primary Dependencies**: 
- NeuG数据库核心库（C++）
- Python绑定（pybind11）
- Elle算法实现（用于环检测）
- LDBC工作负载驱动（用于持久性测试）
- NetworkX（Python，用于依赖图构建和分析）

**Storage**: 
- 测试数据存储在NeuG数据库中
- 操作历史记录使用内存数据结构（列表/字典）或持久化到文件
- 依赖图使用NetworkX的DiGraph数据结构

**Testing**: 
- pytest（Python测试框架）
- C++单元测试框架（gtest）
- 端到端测试框架（基于现有的e2e测试框架）

## Project Structure

测试框架将主要位于以下位置：

```
tests/
├── transaction/
│   ├── acid/                          # ACID测试模块
│   │   ├── atomicity/                 # 原子性测试
│   │   │   ├── atomicity_c_tester.py  # Atomicity-C测试器
│   │   │   └── atomicity_rb_tester.py # Atomicity-RB测试器
│   │   ├── isolation/                 # 隔离性测试
│   │   │   ├── dirty_write_tester.py  # Dirty Write测试器
│   │   │   ├── dirty_read_tester.py   # Dirty Read测试器
│   │   │   ├── cut_anomalies_tester.py # Cut Anomalies测试器
│   │   │   ├── observed_vanishes_tester.py # Observed Transaction Vanishes测试器
│   │   │   ├── fractured_read_tester.py # Fractured Read测试器
│   │   │   ├── lost_update_tester.py   # Lost Update测试器
│   │   │   ├── write_skew_tester.py   # Write Skew测试器
│   │   │   └── elle_cycle_detector.py  # Elle环检测器
│   │   └── consistency_durability/    # 一致性和持久性测试
│   │       ├── durability_tester.py   # Durability测试器
│   │       └── consistency_tester.py  # Consistency测试器
│   ├── utils/                         # 测试工具
│   │   ├── operation_recorder.py      # 操作历史记录器
│   │   ├── dependency_graph_builder.py # 依赖图构建器
│   │   └── history_parser.py         # 历史解析器
│   └── test_acid_comprehensive.py    # 综合测试入口
```

## Data Model

This feature has the following data models:
1. Schema信息（测试图结构）
2. 操作历史记录（三元组格式）
3. 依赖图（DSG）
4. 测试结果报告

### Schema信息（测试图结构）

**Data Structure**:

测试图包含以下节点和边的定义：

- **Person节点**：`Person(id INT64, numFriends INT64, version INT64, versionHistory INT64, name STRING, emails STRING[])`
- **Post节点**：`Post(id INT64)`
- **Knows边**：`Person -[:Knows {since: INT64}]-> Person`
- **Likes边**：`Person -[:Likes]-> Post`

**Data Access & Update**:

- **创建Schema**: 通过Cypher DDL语句创建节点表和边表
- **查询Schema**: 通过`MATCH`和`RETURN`语句查询节点和边的数据
- **修改数据**: 通过`CREATE`、`SET`、`DELETE`等Cypher DML语句修改节点和边的属性
- **查询数据**: 通过Cypher查询语句读取节点、边及其属性值

### 操作历史记录（三元组格式）

**Data Structure**:

所有事务的操作历史被记录为三元组格式，使用原生列表（LIST）存储。每条记录包含操作对象、操作类型和操作值。

```python
[target, type, value]
```

其中：
- `target`: 操作对象（字符串，如"Person_1.emails"或"Person_1->Person_2.Knows"）
- `type`: 操作类型（"read"或"append"）
- `value`: 操作值（列表格式）
  - 如果是read：读取到的值列表
  - 如果是append：列表仅有一个元素，为需要修改的目标值

**Data Access & Update**:

- **追加新记录**: 使用`append`在列表尾部追加新的三元组记录，保持时间顺序
- **读取记录**: 直接按照列表方式顺序读取或随机读取
- **查询特定事务**: 通过事务ID查询该事务的所有操作记录
- **导出历史**: 将完整的历史记录导出为JSON或CSV格式

**实现说明**: 
- 实际实现可以使用Python原生LIST操作
- 或通过STRING的concat模拟LIST操作（如果需要持久化到不支持列表的存储系统）

### 依赖图（DSG - Direct Serialization Graph）

**Data Structure**:

依赖图使用NetworkX的DiGraph表示，节点为事务，边为依赖关系。

**节点属性**:
- `transaction_id`: 事务ID（字符串）
- `commit_status`: 提交状态（"committed"或"aborted"）

**边属性**:
- `dep_type`: 依赖类型（"wr"读依赖、"rw"反依赖、"ww"写依赖、"SO"会话顺序）
- `key`: 依赖的数据对象（target值）

**Data Access & Update**:

- **添加节点**: 添加事务节点，设置事务ID和提交状态
- **添加边**: 添加依赖边，包含依赖类型和数据对象信息
- **环检测**: 通过图遍历算法检测依赖图中的环，使用NetworkX的`simple_cycles()`或强连通分量算法
- **获取边属性**: 获取环上的边的属性（如依赖类型dep_type），用于后续异常分类
- **清空图**: 清空所有节点和边，用于新的测试

### 测试结果报告

**Data Structure**:

测试结果报告包含检测到的异常信息和测试统计。

```json
{
    "test_type": "isolation",
    "test_name": "Dirty Write",
    "anomalies": [
        {
            "anomaly_type": "G0",
            "cycle_id": "cycle_1",
            "cycle_path": ["T1", "T2", "T1"],
            "edges": [
                {"from": "T1", "to": "T2", "type": "ww", "key": "Person_1.version"},
                {"from": "T2", "to": "T1", "type": "ww", "key": "Person_1.version"}
            ],
            "description": "Dirty write detected: WW edge in cycle",
            "severity": "high"
        }
    ],
    "statistics": {
        "total_transactions": 100,
        "committed_transactions": 95,
        "aborted_transactions": 5,
        "anomalies_detected": 1
    }
}
```

**Data Access & Update**:

- **生成报告**: 由测试器自动生成，包含异常类型、涉及的事务、依赖路径等信息
- **导出报告**: 将报告导出为JSON、HTML或文本格式
- **查询异常**: 按异常类型、严重程度等条件筛选报告

## Algorithm Model

This feature has the following algorithm models:
1. Atomicity-C测试算法
2. Atomicity-RB测试算法
3. Isolation通用检测算法
4. Dirty Write检测算法
5. Dirty Read - Aborted Reads检测算法
6. Dirty Read - Intermediate Reads检测算法
7. Circular Information Flow检测算法
8. Cut Anomalies - Item-Many-Preceders检测算法
9. Cut Anomalies - Predicate-Many-Preceders检测算法
10. Observed Transaction Vanishes检测算法
11. Fractured Read检测算法
12. Lost Update检测算法
13. Write Skew检测算法
14. Durability测试算法
15. Consistency测试算法（线性一致性验证）

### Atomicity-C测试算法

**Algorithm Target**: 检查每个成功提交的事务，确保客户端收到的每个成功提交消息对应的所有插入或修改的数据项在提交后都可见。

**Algorithm Details**

1. **初始化数据**: 创建初始测试图，包含两个Person节点：
   ```cypher
   CREATE (:Person {id: 1, name: 'Alice', emails: ['alice@aol.com']}),
          (:Person {id: 2, name: 'Bob', emails: ['bob@hotmail.com', 'bobby@yahoo.com']})
   ```

2. **初始化计数器**: 执行统计查询，获取初始状态：
   ```cypher
   MATCH (p:Person)
   RETURN count(p) AS numPersons, count(p.name) AS numNames, sum(size(p.emails)) AS numEmails
   ```
   使用统计结果初始化计数器`committed`（初始值为0）

3. **执行事务测试**: 执行N个事务实例，每个事务使用以下模式：
   ```cypher
   «BEGIN»
   MATCH (p1:Person {id: $person1Id})
   CREATE (p1)-[k:KNOWS]->(p2:Person)
   SET
     p1.emails = p1.emails + [$newEmail],
     p2.id = $person2Id,
     k.creationDate = $creationDate
   «COMMIT»
   ```
   对于每个成功提交的事务，将`committed`计数器递增，并记录该事务预期的修改：
   - 新增1个Person节点（p2）
   - 新增1个KNOWS边
   - 新增1个email到p1.emails列表
   - 设置p2.id属性
   - 设置k.creationDate属性

4. **最终状态扫描**: 重复执行统计查询，获取最终状态：
   ```cypher
   MATCH (p:Person)
   RETURN count(p) AS numPersons, count(p.name) AS numNames, sum(size(p.emails)) AS numEmails
   ```
   将结果存储在`finalState`中

5. **异常检查**: 验证`committed=finalState`是否成立：
   - 计算`finalState`与初始状态的差值
   - 验证差值与`committed`事务的预期修改总和是否一致
   - 如果一致，说明所有成功提交的事务的修改都正确可见；如果不一致，则检测到原子性违反

### Atomicity-RB测试算法

**Algorithm Target**: 检查每个中止的事务，确保其所有修改都不可见。

**Algorithm Details**

1. **初始化数据**: 创建初始测试图，包含两个Person节点：
   ```cypher
   CREATE (:Person {id: 1, name: 'Alice', emails: ['alice@aol.com']}),
          (:Person {id: 2, name: 'Bob', emails: ['bob@hotmail.com', 'bobby@yahoo.com']})
   ```

2. **初始化计数器**: 执行统计查询，获取初始状态：
   ```cypher
   MATCH (p:Person)
   RETURN count(p) AS numPersons, count(p.name) AS numNames, sum(size(p.emails)) AS numEmails
   ```
   使用统计结果初始化计数器`aborted`（初始值为0）

3. **执行事务测试**: 执行N个事务实例，每个事务使用以下模式：
   ```cypher
   «BEGIN»
   MATCH (p1:Person {id: $person1Id})
   SET p1.emails = p1.emails + [$newEmail]
   «IF» MATCH (p2:Person {id: $person2Id}) exists
   «THEN» «ABORT» «ELSE»
   CREATE (p2:Person {id: $person2Id, emails: []})
   «END»
   «COMMIT»
   ```
   对于每个中止的事务，将`aborted`计数器递增，并记录该事务预期的修改（但这些修改不应出现在最终状态中）：
   - 如果p2已存在，事务将中止，不应有任何修改
   - 如果p2不存在，事务将创建p2节点并提交，但需要验证如果事务中止，p2节点不应存在

4. **最终状态扫描**: 重复执行统计查询，获取最终状态：
   ```cypher
   MATCH (p:Person)
   RETURN count(p) AS numPersons, count(p.name) AS numNames, sum(size(p.emails)) AS numEmails
   ```
   将结果存储在`finalState`中

5. **异常检查**: 验证中止事务的所有修改都不出现在最终状态中：
   - 计算`finalState`与初始状态的差值
   - 验证差值与成功提交事务的预期修改总和是否一致
   - 验证中止事务的所有修改（如p1.emails的更新、p2节点的创建）都不出现在最终状态中
   - 如果中止事务的任何修改出现在最终状态中，则检测到原子性违反

### Isolation通用检测算法

**Algorithm Target**: 提供基于Elle算法的隔离异常检测框架，通过记录操作历史、构建依赖图和检测异常来发现各种隔离异常。

**Algorithm Details**

1. **执行事务查询，并通过LIST记录相关历史操作**:
   - 执行并发事务查询，每个事务包含BEGIN、读写操作序列和COMMIT/ABORT
   - 使用LIST数据结构记录每个事务的操作历史，每条记录为三元组格式`[target, type, value]`
   - 记录操作类型（"read"或"append"）、操作对象（节点、边、属性）和操作值
   - 记录事务的提交/中止状态和时间戳信息
   - 所有操作历史按执行顺序存储在LIST中，保持时间顺序

2. **依赖图构建，解析操作历史并添加四种边**:
   - 解析操作历史LIST，遍历每个事务的操作记录
   - 构建依赖图（DSG），节点为已提交的事务，边为依赖关系
   - 识别并添加四种类型的依赖边：
     - **读依赖（wr）**: 如果事务Ti写入版本xk且Tj读取xk，则建立Ti→Tj的wr边
     - **反依赖（rw）**: 如果事务Ti读取版本xk且Tj写入xk的下一个版本，则建立Ti→Tj的rw边
     - **写依赖（ww）**: 如果事务Ti写入版本xk且Tj写入xk的下一个版本，则建立Ti→Tj的ww边
     - **会话顺序（SO）**: 如果操作R1和R2在列表中连续出现且属于同一会话，则建立R1→R2的SO边
   - 每条边包含依赖类型（dep_type）和数据对象（key）信息

3. **异常检测，在依赖图上执行特定算法来检测异常**:
   - 使用强连通分量检测算法快速判断依赖图中是否存在环
   - 当检测到存在环时，使用强连通分量算法（如NetworkX的`strongly_connected_components()`）获取所有强连通分量
   - 在每个强连通分量中，使用BFS算法提取具体的环路径
   - 根据环中边的类型组合（ww、rw、wr、SO）识别异常类型：
     - 包含ww边的环：G0异常（脏写）
     - 包含rw边的反依赖环：G2异常（写偏斜）
     - 其他边类型组合：G1系列异常或其他隔离异常
   - 生成异常报告，包含异常类型、环路径、涉及的边和事务信息

### Dirty Write检测算法

**Algorithm Target**: 检测脏写异常（G0异常），验证两个并发事务不能同时修改同一数据对象。

**Algorithm Details**

1. **加载测试图**: 加载包含通过KNOWS边连接的Person节点对的测试图：
   - 每个Person节点具有唯一的id属性
   - 每个Person节点具有versionHistory属性（列表类型，初始为空）
   - 每个KNOWS边具有versionHistory属性（列表类型，初始为空）

2. **执行写事务**: 在执行期间，写客户端执行一系列G0 TW事务实例，每个事务使用以下模式：
   ```cypher
   MATCH
     (p1:Person {id: $person1Id})
     -[k:KNOWS]->(p2:Person {id: $person2Id})
   SET p1.versionHistory = p1.versionHistory + [$tId]
   SET p2.versionHistory = p2.versionHistory + [$tId]
   SET k.versionHistory = k.versionHistory + [$tId]
   ```
   每个G0 TW事务将其唯一的事务ID（$tId）追加到Person对中每个实体（2个Person节点和1个KNOWS边）的versionHistory属性中。事务ID假设全局唯一。

3. **执行读事务**: 执行期结束后，读客户端为测试图中的每个Person对发出G0 TR查询：
   ```cypher
   MATCH (p1:Person {id: $person1Id})
     -[k:KNOWS]->(p2:Person {id: $person2Id})
   RETURN
     p1.versionHistory AS p1VersionHistory,
     k.versionHistory AS kVersionHistory,
     p2.versionHistory AS p2VersionHistory
   ```
   检索每个Person对中每个实体（2个Person节点和1个KNOWS边）的versionHistory属性。

4. **异常检查**: 对每个Person对执行以下检查：
   - **修剪versionHistory列表**: 移除任何不在所有三个列表（p1.versionHistory、p2.versionHistory、k.versionHistory）中出现的版本号。此步骤用于处理Lost Update异常（Section 10.3.8）的干扰，因为丢失更新会覆盖版本，从而从数据项的历史中擦除它
   - **逐元素比较**: 对修剪后的versionHistory列表进行逐元素比较
   - **异常检测**: 如果三个列表不一致（即相同位置的元素不同），则检测到G0异常（脏写）

**工作原理**: 每个G0 TW事务有效地创建Person对的新版本。将事务ID追加到versionHistory属性中，保留了Person对中每个实体的版本历史。在防止G0的系统中，Person对的每个实体应该经历相同的更新，且顺序相同。因此，versionHistory列表中每个位置的元素应该相等。额外的修剪步骤是必要的，因为丢失更新会覆盖版本，从而从数据项的历史中有效擦除它。

### Dirty Read - Aborted Reads检测算法

**Algorithm Target**: 检测脏读异常（G1a异常），验证事务不能读取其他已中止事务写入的数据。

**Algorithm Details**

1. **加载测试图**: 加载仅包含Person节点的测试图到数据库：
   - 每个Person节点具有唯一的id属性
   - 每个Person节点具有version属性，初始化为1（任何奇数都可以）

2. **执行写事务**: 在执行期间，写客户端执行一系列G1a TW事务实例，每个事务使用以下模式：
   ```cypher
   MATCH (p:Person {id: $personId})
   SET p.version = 2
   «SLEEP($sleepTime)»
   «ABORT»
   ```
   每个G1a TW事务尝试将version设置为2（任何偶数都可以），但总是中止。为每个实例随机选择一个Person id。

3. **执行读事务**: 并发地，读客户端执行一系列G1a TR事务实例，每个事务使用以下模式：
   ```cypher
   MATCH (p:Person {id: $personId})
   RETURN p.version
   ```
   该事务检索Person的version属性。读客户端存储结果，执行结束后汇总所有结果。

4. **异常检查**: 每个读操作应该返回version=1（或任何奇数）。否则，检测到G1a异常（中止读）。

**工作原理**: 每个尝试将version设置为偶数的事务总是中止。因此，如果事务读取到version为偶数，它必须读取了已中止事务的写入。

### Dirty Read - Intermediate Reads检测算法

**Algorithm Target**: 检测脏读异常（G1b异常），验证事务不能读取其他未提交事务执行过程中的中间状态。

**Algorithm Details**

1. **加载测试图**: 加载仅包含Person节点的测试图到数据库。为每个Person分配唯一的id和version属性，version初始化为1（任何奇数都可以）。

2. **执行写事务**: 在执行期间，写客户端执行一系列G1b TW事务实例。该事务先将version设置为偶数，然后设置为奇数，然后提交：
   ```cypher
   MATCH (p:Person {id: $personId})
   SET p.version = $even
   «SLEEP($sleepTime)»
   SET p.version = $odd
   ```

3. **执行读事务**: 并发地，读客户端执行一系列G1b TR事务实例。通过id选择Person并检索其version属性：
   ```cypher
   MATCH (p:Person {id: $personId})
   RETURN p.version
   ```
   读客户端存储结果，执行结束后收集所有结果。

4. **异常检查**: 每个version的读操作应该返回奇数。否则，检测到G1b异常。

**工作原理**: G1b TW事务实例安装的最终版本永远不会是偶数。因此，如果事务读取到version为偶数，它必须读取了中间版本。

### Circular Information Flow检测算法

**Algorithm Target**: 检测循环信息流异常（G1c异常），验证事务之间是否存在循环的信息依赖关系。

**Algorithm Details**

1. **加载测试图**: 加载仅包含Person节点的测试图到数据库。为每个Person分配唯一的id和version属性，version初始化为0。

2. **执行读写事务**: 此测试需要读写客户端，执行一系列G1c TRW事务实例。该事务选择两个不同的Person节点，将其中一个Person的version设置为事务ID，并检索另一个Person的version：
   ```cypher
   MATCH (p1:Person {id: $person1Id}) SET p1.version = $transactionId
   MATCH (p2:Person {id: $person2Id}) RETURN p2.version
   ```
   注意，事务ID假设全局唯一。事务结果以格式(txn.id, versionRead)存储，执行结束后收集所有结果。

3. **异常检查**: 对于每个结果，检查versionRead对应的事务结果，该事务是否读取了该结果的事务。如果是，则检测到G1c异常。

**工作原理**: 考虑结果集：{(T1, T2), (T2, T3), (T3, T2)}。T1读取了T2写入的version，T2读取了T3写入的version。这里信息流从T1到T2是单向的。然而，T2读取了T3写入的version，T3读取了T2写入的version。这里信息流从T2到T3和T3到T2是循环的。因此检测到G1c异常。

### Cut Anomalies - Item-Many-Preceders检测算法

**Algorithm Target**: 检测切割异常（IMP异常），验证事务对同一数据项的多次读取是否一致。

**Algorithm Details**

1. **加载测试图**: 加载包含Person节点的测试图。为每个Person分配唯一的id和version属性，version初始化为1。

2. **执行写事务**: 在执行期间，写客户端执行一系列IMP TW事务实例。随机选择一个id并安装Person的新版本：
   ```cypher
   MATCH (p:Person {id: $personId})
   SET p.version = p.version + 1
   ```

3. **执行读事务**: 并发地，读客户端执行一系列IMP TR事务实例。对同一个Person执行多次读取；连续的读取可以通过人工注入的等待时间分隔，以使条件更有利于检测异常：
   ```cypher
   MATCH (p1:Person {id: $personId})
   WITH p1.version AS firstRead
   «SLEEP($sleepTime)»
   MATCH (p2:Person {id: $personId})
   RETURN firstRead,
     p2.version AS secondRead
   ```
   IMP TR事务中的两次读取都被返回、存储，执行结束后收集所有结果。

4. **异常检查**: 每个IMP TR结果集(firstRead, secondRead)应该包含相同的Person版本。否则，检测到IMP异常。

**工作原理**: 通过在同一事务内执行连续读取，此测试检查系统是否确保对同一数据项的一致性读取。如果版本发生变化，则并发事务已修改了数据项，读取事务未受到此更改的保护。

### Cut Anomalies - Predicate-Many-Preceders检测算法

**Algorithm Target**: 检测切割异常（PMP异常），验证事务对同一谓词的多次读取是否一致。

**Algorithm Details**

1. **加载测试图**: 加载包含Person和Post节点的测试图。在每个节点类型内分配唯一的ID。

2. **执行写事务**: 在执行期间，写客户端执行一系列PMP TW事务实例，在随机选择的Person和Post之间插入LIKES边：
   ```cypher
   MATCH (pe:Person {id: $personId}), (po:Post {id: $postId})
   CREATE (pe)-[:LIKES]->(po)
   ```

3. **执行读事务**: 并发地，读客户端执行一系列PMP TR事务实例。对模式(po:Post)<-[:LIKES]-(p:Person)执行多次读取并统计LIKES边的数量；连续的读取可以通过人工注入的等待时间分隔，以使条件更有利于检测异常：
   ```cypher
   MATCH (po1:Post {id: $postId})<-[:LIKES]-(pe1:Person)
   WITH count(pe1) AS firstRead
   «SLEEP($sleepTime)»
   MATCH (po2:Post {id: $postId})<-[:LIKES]-(pe2:Person)
   RETURN firstRead,
     count(pe2) AS secondRead
   ```
   PMP TR事务中的两次谓词读取都被返回、存储，测试执行结束后收集所有结果。

4. **异常检查**: 对于每个PMP TR事务结果集(firstRead, secondRead)，firstRead应该等于secondRead。否则，检测到PMP异常。

**工作原理**: 通过在同一事务内执行连续的谓词读取并统计LIKES边的数量，此测试检查系统是否确保对同一谓词的一致性读取。如果LIKES边的数量发生变化，则并发事务已插入了新的LIKES边，读取事务未受到此更改的保护。

### Observed Transaction Vanishes检测算法

**Algorithm Target**: 检测观察事务消失异常（OTV异常），验证已观察到的数据是否会在后续读取中消失。

**Algorithm Details**

1. **加载测试图**: 加载包含一组长度为4的Person循环的测试图，这些Person通过Knows边连接，具有相同的name。为每个Person分配id、name和version属性（初始化为1）。注意，id必须在所有节点中唯一，name必须在所有循环中唯一。

2. **执行写事务**: 在执行期间，写客户端选择一个name和id，执行一系列OTV TW事务实例。该事务有效地创建给定循环的新版本：
   ```cypher
   MATCH path =
     (n:Person {id: $personId})
     -[:KNOWS*..4]->(n)
   UNWIND nodes(path)[0..4] AS p
   SET p.version = p.version + 1
   ```

3. **执行读事务**: 并发地，读客户端执行一系列OTV TR事务实例。匹配给定循环并执行多次读取：
   ```cypher
   MATCH p1=(n1:Person {id: $personId})-[:KNOWS*..4]->(n1)
   RETURN extract(p IN nodes(p1) | p.version) AS firstRead
   «SLEEP($sleepTime)»
   MATCH p2=(n2:Person {id: $personId})-[:KNOWS*..4]->(n2)
   RETURN extract(p IN nodes(p2) | p.version) AS secondRead
   ```
   OTV TR事务中的两次读取都被返回、存储，执行结束后收集所有结果。

4. **异常检查**: 对于每个OTV TR结果集(firstRead, secondRead)，firstRead中的最大version应该小于或等于secondRead中的最小version。否则，检测到OTV异常。

**工作原理**: OTV TW通过更新每个Person的version属性来安装循环的新版本。因此，当匹配循环时，一旦事务观察到某个version，它应该至少为循环中的每个剩余实体观察到这个version。不幸的是，这不能从循环的单一读取中推断出来，因为匹配循环的结果通常不保留读取图实体的顺序。这通过多次读取循环来解决。firstRead的最大version决定了secondRead的最小version。如果违反此条件，则事务在firstRead中观察到了某个事务的效果，然后在secondRead中未能观察到它——观察到的事务已经消失！

### Fractured Read检测算法

**Algorithm Target**: 检测断裂读异常（FR异常），验证事务读取的数据是否来自多个不同版本的不一致快照。

**Algorithm Details**

1. **加载测试图**: 同OTV测试，加载包含一组长度为4的Person循环的测试图，这些Person通过Knows边连接，具有相同的name。为每个Person分配id、name和version属性（初始化为1）。注意，id必须在所有节点中唯一，name必须在所有循环中唯一。

2. **执行写事务**: 在执行期间，写客户端选择一个name和id，执行一系列FR TW事务实例（Listing 10.16）。该事务通过更新每个Person的version属性来安装循环的新版本：
   ```cypher
   MATCH path =
     (n:Person {id: $personId})
     -[:KNOWS*..4]->(n)
   UNWIND nodes(path)[0..4] AS p
   SET p.version = p.version + 1
   ```

3. **执行读事务**: 并发地，读客户端执行一系列FR TR事务实例（Listing 10.17）。匹配给定循环并执行多次读取：
   ```cypher
   MATCH p1=(n1:Person {id: $personId})-[:KNOWS*..4]->(n1)
   RETURN extract(p IN nodes(p1) | p.version) AS firstRead
   «SLEEP($sleepTime)»
   MATCH p2=(n2:Person {id: $personId})-[:KNOWS*..4]->(n2)
   RETURN extract(p IN nodes(p2) | p.version) AS secondRead
   ```
   FR TR事务中的两次读取都被返回、存储，执行结束后收集所有结果。

4. **异常检查**: 对于每个FR TR结果集(firstRead, secondRead)，两个版本集中的所有version应该相等。否则，检测到FR异常。

**工作原理**: FR TW通过更新循环中每个Person的version属性来安装循环的新版本。当FR TR观察到某个version时，该循环中的每个后续读取都应该读取相同的version，因为FR TW（Listing 10.16）为循环中的所有Person节点安装相同的version。因此，如果它观察到不同的version，它已经观察到了不同事务的效果，并且已经跨越了事务边界进行读取。

### Lost Update检测算法

**Algorithm Target**: 检测丢失更新异常（LU异常），验证并发事务的更新操作是否被正确序列化。

**Algorithm Details**

1. **加载测试图**: 加载包含Person节点的测试图。为每个Person分配唯一的id和numFriends属性（初始化为0）。

2. **执行写事务**: 在执行期间，写客户端执行一系列LU TW事务实例（Listing 10.18）。随机选择一个Person并递增其numFriends属性：
   ```cypher
   MATCH (p:Person {id: $personId})
   SET p.numFriends = p.numFriends + 1
   ```
   客户端为每个Person存储本地计数器(expNumFriends)，每次选择Person且LU TW实例成功提交时递增该计数器。

3. **执行读事务**: 执行期结束后，使用LU TR（Listing 10.19）检索每个Person的numFriends属性：
   ```cypher
   MATCH (p:Person {id: $personId})
   RETURN p.numFriends AS numFriends
   ```
   从写客户端汇总每个Person的expNumFriends。

4. **异常检查**: 对于每个Person，其numFriends属性应该等于该Person的（全局）expNumFriends。否则，检测到LU异常。

**工作原理**: 客户端知道为给定Person发出了多少成功的LU TW实例。可观察的numFriends应该反映这个真实情况，否则必须发生了LU异常。

### Write Skew检测算法

**Algorithm Target**: 检测写偏斜异常（WS异常，也称为G2-Item异常），验证事务基于读取值进行写入时是否出现不一致。

**Algorithm Details**

1. **加载测试图**: 加载包含n对Person节点(p1, p2)的测试图，对于k = 0, ..., n-1，第k对节点具有ID p1.id = 2*k+1和p2.id = 2*k+2，值分别为p1.value = 70和p2.value = 80。存在一个约束：p1.value + p2.value > 0。

2. **执行写事务**: 在执行期间，写客户端执行一系列WS TW事务实例（Listing 10.20）。随机选择一个Person对，在不会违反约束的情况下递减其中一个Person的value属性：
   ```cypher
   MATCH (p1:Person {id: $person1Id}),
     (p2:Person {id: $person2Id})
   «IF (p1.value+p2.value < 100)» «THEN» «ABORT» «END»
   «SLEEP($sleepTime)»
   pId = «pick randomly between personId1, personId2»
   MATCH (p:Person {id: $pId})
   SET p.value = p.value - 100
   «COMMIT»
   ```

3. **执行读事务**: 执行期结束后，使用WS TR（Listing 10.21）扫描数据库：
   ```cypher
   MATCH (p1:Person),
     (p2:Person {id: p1.id+1})
   WHERE p1.value + p2.value <= 0
   RETURN
     p1.id AS p1id,
     p1.value AS p1value,
     p2.id AS p2id,
     p2.value AS p2value
   ```

4. **异常检查**: 对于每个Person对，约束应该成立。否则，检测到WS异常。

**工作原理**: 在可序列化执行下，WS TW实例的执行不会违反约束p1.value + p2.value > 0。因此，如果WS TR返回违反此约束的结果，则明显发生了WS异常。

### Durability测试算法

**Algorithm Target**: 验证已提交事务的更改在系统崩溃、重启等故障场景下能够正确恢复。

**Algorithm Details**

1. **配置工作负载**: 使用LDBC工作负载驱动执行SNB Interactive工作负载，配置与性能运行相同
2. **预热阶段**: 数据库经历预热期，确保系统达到稳定状态
3. **执行工作负载**: 执行2小时的模拟执行，期间数据库处理各种事务操作
4. **终止数据库进程**: 通过断开机器连接或硬进程终止（kill -9）终止数据库进程
5. **重启数据库系统**: 重启数据库系统，使其从持久化存储中恢复
6. **验证数据恢复**: 每个客户端发出读取请求，查询它最后收到成功提交消息的实体（节点或边）的值，验证是否正确恢复

### Consistency测试算法（线性一致性验证）

**Algorithm Target**: 验证数据库是否满足线性一致性（Linearizability）保证。在线性一致性模型下，一旦某个更新操作（写操作）完成并通知了用户，那么任何用户在任何时间、任何节点上发起的读操作，都必须返回这个最新的值。

**Algorithm Details**

1. **复用Elle测试框架，建立事务依赖关系**:
   - 复用Elle测试框架（参见Isolation通用检测算法部分）
   - 构建基础依赖图（DSG），添加三种类型的依赖边：读依赖（wr）、反依赖（rw）、写依赖（ww）
   - 额外记录事务操作历史和时间戳信息（包括事务开始时间`start_time`和提交/中止时间`end_time`）

2. **加入实时边（Real-Time Edge, RT边）**:
   - 对于任意两个已提交的事务Ti和Tj，如果Ti的结束时间（`end_time`）早于Tj的开始时间（`start_time`），则建立从Ti到Tj的RT边（Ti→Tj）
   - RT边表示物理时间上的先后顺序：如果事务T1在T2开始之前完成，那么T1的更新必须在T2中可见
   - RT边的添加规则：对于事务Ti和Tj，如果`Ti.end_time < Tj.start_time`，则添加RT边`Ti --[RT]--> Tj`
   - RT边与数据依赖边（wr、rw、ww）共同构成线性一致性依赖图

3. **优化连边逻辑，避免冗余边插入**:
   - **事务排序**: 将所有事务按照`start_time`从小到大排序，得到有序事务序列T[1..n]
   - **依次处理事务**: 对于每个事务Ti（i从1到n），枚举所有剩余的事务Tj（j=i+1, i+2, ..., n），查找满足`Ti.end_time < Tj.start_time`的事务并连RT边
   - **二分查找优化**: 由于所有事务已按`start_time`排序，对于事务Ti，可以使用二分查找算法快速定位第一个满足`Ti.end_time < Tj.start_time`的事务Tk（即第一个`start_time > Ti.end_time`的事务），从Tk开始连边
   - **冗余边剪枝优化**: 
     - 在连边过程中，维护已连边事务的`end_time`的最小值，对应的事务为Tp
     - 当处理事务Tj时，如果`Tp.end_time < Tj.start_time`，则Tj本身以及所有后续的事务（Tj, Tj+1, Tj+2, ..., Tn）都不需要从Ti连RT边
     - **优化原理**: 如果`Tp.end_time < Tj.start_time`，那么对于Tj本身以及任意后续事务Tq（q >= j），均满足`Tp.end_time < Tq.start_time`，因此`Ti -> Tq`的依赖可以通过`Ti -> Tp -> Tq`的路径推导得出，无需直接连边

4. **构建依赖图并验证线性一致性**:
   - 将RT边与数据依赖边（wr、rw、ww）合并，构建完整的线性一致性依赖图。依赖图节点为已提交的事务，依赖图边包含四种类型：wr（读依赖）、rw（反依赖）、ww（写依赖）、RT（实时边）
   - 复用Elle模块的环检测算法（参见Isolation通用检测算法部分），使用强连通分量检测算法识别依赖图中的环
   - **线性一致性违反检测**: 对于每个检测到的环，检查环中是否包含RT边。如果环中包含RT边，则检测到线性一致性违反
   - **原理**: RT边表示物理时间上的先后顺序，如果RT边出现在环中，说明存在事务T1在T2之前完成，但T2的更新在T1中可见，这与线性一致性的要求矛盾

**工作原理**: 线性一致性要求系统对外呈现单一、一致的数据视图。通过引入RT边，我们捕获了物理时间上的先后顺序约束。如果依赖图中存在包含RT边的环，说明系统无法找到一个全局一致的顺序来满足所有的时间约束和数据依赖，从而违反了线性一致性保证。
