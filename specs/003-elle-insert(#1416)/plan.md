# Implementation Plan: 新增针对insert的elle测试

**Branch**: `003-elle-insert(#1416)` | **Date**: 2026-01-19 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/003-elle-insert(#1416)/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

本功能旨在为NeuG数据库实现针对insert操作的Elle测试，主要包含两个核心模块：
1. **Read/Insert并发依赖关系检测模块**：检测read和insert并发操作之间的依赖关系，通过推导偏序关系构建依赖图，避免因无序结果导致的依赖边数量爆炸问题
2. **Read/Insert/Delete并发依赖关系检测模块**：检测read、insert和delete并发操作之间的依赖关系，利用NeuG的全局写唯一机制，将delete事务作为barrier划分时间块，验证严格可串行化

技术方法：通过记录read操作返回的结果集合，基于集合包含关系推导insert操作的偏序关系，构建包含rr边、ir边、ri边和线性边的依赖图。对于read/insert/delete并发场景，利用delete事务的时间顺序构造dd边，并根据delete操作分块后构造id、rd、di、dr边，在每个块内应用read/insert并发的构图方法。使用Elle算法的环检测机制检测并发冲突和隔离异常。

## Technical Context

**Language/Version**: C++17+ / Python 3.8+  
**Primary Dependencies**: 
- NeuG数据库核心库（C++）
- Python绑定（pybind11）
- Elle算法实现（用于环检测，继承自spec 002）
- NetworkX（Python，用于依赖图构建和分析）

**Storage**: 
- 测试数据存储在NeuG数据库中
- Read操作结果集合使用内存数据结构（集合/列表）存储
- Insert/Delete操作历史记录使用内存数据结构（列表/字典）或持久化到文件
- 依赖图使用NetworkX的DiGraph数据结构

**Testing**: 
- pytest（Python测试框架）
- C++单元测试框架（gtest）
- 端到端测试框架（基于现有的e2e测试框架）

## Project Structure

测试框架将主要位于以下位置：

```
tools/python_bind/tests/transaction/elle/
├── elle_tester.py          # Elle测试器（复用spec 002）
├── elle_insert_tester.py   # Read/Insert并发测试器（Module 1，新增）
└── elle_delete_tester.py   # Read/Insert/Delete并发测试器（Module 2，新增）
```

**Structure Decision**: 在`tools/python_bind/tests/transaction/elle/`目录下新增两个测试文件。`elle_tester.py`复用spec 002中已实现的Elle测试框架（包括环检测、操作历史记录等功能）。`elle_insert_tester.py`实现Module 1的read/insert并发依赖关系检测，包含偏序关系推导和依赖图构建算法。`elle_delete_tester.py`实现Module 2的read/insert/delete并发依赖关系检测，包含时间块划分和跨块依赖构建算法。两个新文件可以复用`elle_tester.py`中的基础功能。

## Data Model

This feature has the following data models (这些数据结构在`elle_insert_tester.py`和`elle_delete_tester.py`中内部使用):
1. 操作历史记录（统一的三元组格式）

### 操作历史记录（统一三元组格式）

**Data Structure**:

所有操作（Read、Insert、Delete）统一使用三元组格式 `[op, target, list]` 记录：

- `op`: 操作类型，取值为 `'Read'`、`'Insert'` 或 `'Delete'`
- `target`: 操作对象，具体指点表名称（如 `'Person'`）
- `list`: 操作结果列表
  - 如果是 `Read` 操作：存储读取到的节点ID列表（如 `[1, 2, 3, 4]`）
  - 如果是 `Insert` 操作：存储需要插入的节点ID（只允许单点插入，如 `[1]`）
  - 如果是 `Delete` 操作：存储需要删除的节点ID列表（如 `[3]`）

每个操作记录还包含事务ID和时间戳信息：

```python
{
    "transaction_id": "T1",
    "timestamp": 1234567890,
    "operation": ["Read", "Person", [1, 2, 3, 4]]  # [op, target, list]
}
```

示例：
- Read操作：`["Read", "Person", [1, 2, 3, 4]]` - 读取Person表中的节点1,2,3,4
- Insert操作：`["Insert", "Person", [5]]` - 向Person表插入节点5（单点插入）
- Delete操作：`["Delete", "Person", [3]]` - 从Person表删除节点3

**Data Access & Update**:

- **记录操作**: 执行任何操作（read/insert/delete）后，将操作记录为三元组格式 `[op, target, list]`，同时记录事务ID和时间戳
- **查询操作历史**: 通过事务ID、操作类型或时间戳范围查询操作历史
- **过滤操作类型**: 根据op字段过滤特定类型的操作（如只查询所有Read操作）
- **获取操作结果**: 
  - 对于Read操作：获取读取到的节点ID列表
  - 对于Insert操作：获取插入的节点ID列表
  - 对于Delete操作：获取删除的节点ID列表
- **集合包含关系判断**: 对于Read操作，判断两个read操作的list是否存在子集关系（用于偏序关系推导）
- **按时间排序**: 根据时间戳对所有操作进行排序，用于时间块划分和依赖关系构建
- **导出历史**: 将操作历史导出为JSON或CSV格式用于分析

## Algorithm Model

This feature has the following algorithm models:
1. Read/Insert并发依赖图构建算法
2. Read/Insert/Delete并发依赖图构建算法

### Read/Insert并发依赖图构建算法

**Algorithm Target**: 基于read操作结果集合和insert操作历史，构建包含rr边、ir边、ri边和线性边的依赖图，用于检测read/insert并发场景中的隔离异常。

**Algorithm Details**

#### 输入
- 操作历史列表（统一三元组格式）：`operations = [
    (T1, ["Read", "Person", [1]], timestamp=[10, 11]),
    (T_insert_1, ["Insert", "Person", [1]], timestamp=[12, 13]),
    (T_insert_2, ["Insert", "Person", [2]], timestamp=[14, 15]),
    (T_insert_3, ["Insert", "Person", [3]], timestamp=[15, 16]),
    (T2, ["Read", "Person", [1, 2, 3]], timestamp=[16, 17]),
    (T_insert_4, ["Insert", "Person", [4]], timestamp=[18, 19]),
    (T3, ["Read", "Person", [1, 2, 3, 4]], timestamp=[20, 21])
]`
- 事务时间戳信息：每个事务的开始和结束时间

#### 算法步骤

1. **构建rr边（读读依赖边）**:
   - 根据Read操作结果列表的长度（数量）对所有Read操作进行排序，结果数量少的在前，数量多的在后
   - 对于排序后的相邻Read操作对 (Ta, Tb)，如果Ta的结果列表是Tb结果列表的子集（转换为集合后判断），则添加rr边 Ta → Tb
   - 其中Ta和Tb的操作三元组为 `["Read", target, list_a]` 和 `["Read", target, list_b]`
   - 示例：如果T1的操作为 `["Read", "Person", [1]]`（数量1），T2的操作为 `["Read", "Person", [1,2,3]]`（数量3），排序后T1在T2之前，且 [1] ⊆ [1,2,3]，则添加边 T1 → T2
   - 数学表示：如果 `len(list_a) < len(list_b)` 且 `set(list_a) ⊆ set(list_b)`，则 `Ta → Tb (rr)`

2. **构建ir边和ri边（插入读依赖边和读插入依赖边）**:
   - 遍历排序后的Read操作（已按结果数量排序），对于相邻的两个Read操作 (Ta, Tb)，其操作三元组分别为 `["Read", target, list_a]` 和 `["Read", target, list_b]`
   - 计算两个Read操作结果列表的差集：`diff = set(list_b) - set(list_a)`，表示Tb比Ta多读到的节点
   - 对于差集中的每个节点x，找到在Ta和Tb之间执行的Insert事务 T_insert，其操作三元组为 `["Insert", target, [x]]`（单点插入）
   - 如果找到这样的T_insert，则：
     - 添加ri边：Ta → T_insert
     - 添加ir边：T_insert → Tb
   - 示例：如果T1的操作为 `["Read", "Person", [1]]`，T_insert_2的操作为 `["Insert", "Person", [2]]`，T2的操作为 `["Read", "Person", [1,2,3]]`，排序后T1和T2相邻，差集为 {2, 3}，找到T_insert_2插入2，则添加边 T1 → T_insert_2 和 T_insert_2 → T2
   - 数学表示：如果 `diff = set(list_b) - set(list_a)` 且存在Insert事务 T_insert 插入节点 x ∈ diff，则 `Ta → T_insert (ri)` 和 `T_insert → Tb (ir)`

3. **构建线性边（线性一致性依赖边）**:
   - 遍历所有事务对 (Ta, Tb)
   - 如果事务a的结束时间早于事务b的开始时间，则添加线性边 Ta → Tb
   - 数学表示：如果 `end_time(Ta) < start_time(Tb)`，则 `Ta → Tb (linear)`

4. **构建完整依赖图**:
   - 将所有边添加到NetworkX的DiGraph中
   - 节点为所有事务（read和insert）
   - 边包含dep_type属性标识依赖类型

#### 输出
- 完整的依赖图（NetworkX DiGraph），包含所有rr边、ir边、ri边和线性边

#### 示例

假设有以下操作序列（使用三元组格式，Insert只允许单点插入）：
- T1: `["Read", "Person", [1]]`，时间戳 [10, 11]
- T_insert_2: `["Insert", "Person", [2]]`，时间戳 [12, 13]
- T_insert_3: `["Insert", "Person", [3]]`，时间戳 [13, 14]
- T2: `["Read", "Person", [1, 2, 3]]`，时间戳 [14, 15]
- T_insert_4: `["Insert", "Person", [4]]`，时间戳 [16, 17]
- T3: `["Read", "Person", [1, 2, 3, 4]]`，时间戳 [18, 19]

构建的依赖图包含：
- rr边: T1 → T2 (因为 [1] ⊆ [1,2,3])，T2 → T3 (因为 [1,2,3] ⊆ [1,2,3,4])
- ri边: T1 → T_insert_2 (因为T1的list [1]中不包含2)，T1 → T_insert_3 (因为T1的list [1]中不包含3)
- ir边: T_insert_2 → T2 (因为T2的list [1,2,3]中包含2)，T_insert_3 → T2 (因为T2的list [1,2,3]中包含3)，T_insert_4 → T3 (因为T3的list [1,2,3,4]中包含4)
- 线性边: T1 → T_insert_2 (因为end_time(T1)=11 < start_time(T_insert_2)=12)，等等

### Read/Insert/Delete并发依赖图构建算法

**Algorithm Target**: 基于read、insert和delete操作历史，利用NeuG的全局写唯一机制，将delete事务作为barrier划分时间块，构建包含dd边、id边、rd边、di边、dr边、rr边、ir边、ri边和线性边的依赖图，用于检测read/insert/delete并发场景中的严格可串行化违反。

**Algorithm Details**

#### 输入
- 操作历史列表（统一三元组格式）：`operations = [
    (T1, ["Read", "Person", [1, 2]], timestamp=[10, 11]),
    (T_insert_1, ["Insert", "Person", [1]], timestamp=[12, 13]),
    (T_insert_2, ["Insert", "Person", [2]], timestamp=[13, 14]),
    (T_insert_3, ["Insert", "Person", [3]], timestamp=[14, 15]),
    (T2, ["Read", "Person", [1, 2, 3]], timestamp=[16, 17]),
    (T_delete_1, ["Delete", "Person", [3]], timestamp=[100, 101]),
    (T_insert_4, ["Insert", "Person", [4]], timestamp=[102, 103]),
    (T_insert_5, ["Insert", "Person", [5]], timestamp=[103, 104]),
    (T3, ["Read", "Person", [1, 2, 4, 5]], timestamp=[104, 105])
]`
- 事务时间戳信息：每个事务的开始和结束时间

#### 算法步骤

1. **时间块划分**:
   - 根据Delete事务的时间戳将整个操作序列划分为多个时间块
   - 每个Delete事务（操作三元组为 `["Delete", target, list]`）作为一个barrier，将操作序列分为delete之前和delete之后两个部分
   - 示例：如果有Delete事务在时间戳100执行，则划分为：
     - Block 0: 时间戳 < 100的所有操作
     - Block 1: 时间戳 > 100的所有操作
   - 每个时间块内仅包含Insert和Read操作

2. **构建dd边（删除删除依赖边）**:
   - 根据Delete事务的时间顺序依次构造dd边
   - 如果Delete事务T_delete_i的时间戳早于T_delete_j，则添加dd边 T_delete_i → T_delete_j
   - 数学表示：如果 `timestamp(T_delete_i) < timestamp(T_delete_j)`，则 `T_delete_i → T_delete_j (dd)`
   - 本质上是一种线性边，因为delete事务是独占执行的

3. **构建id边、rd边、di边、dr边（跨块依赖边）**:
   - 对于每个时间块，块内的Insert和Read操作需要与前后块的Delete事务建立依赖关系（基于时间关系，只要时间落在分块之内就需要连边）：
     - **id边（插入删除依赖）**: 如果Insert事务T_insert在Delete事务T_delete之前的时间块内（时间落在该块内），则添加id边 T_insert → T_delete
     - **rd边（读删除依赖）**: 如果Read事务T_read在Delete事务T_delete之前的时间块内（时间落在该块内），则添加rd边 T_read → T_delete
     - **di边（删除插入依赖）**: 如果Delete事务T_delete在Insert事务T_insert之前（时间关系），则添加di边 T_delete → T_insert（因为delete是barrier）
     - **dr边（删除读依赖）**: 如果Delete事务T_delete在Read事务T_read之前（时间关系），则添加dr边 T_delete → T_read（因为delete是barrier）
   - 本质上这些边都是线性边，基于时间顺序和NeuG的全局写唯一机制，不需要检查操作内容是否匹配

4. **构建块内rr边（读读依赖边）**:
   - 在每个时间块内，应用Module 1的rr边构建方法
   - 根据Read操作结果列表的长度（数量）对块内的Read操作进行排序，结果数量少的在前，数量多的在后
   - 对于排序后的相邻Read操作对 (Ta, Tb)，如果Ta的结果列表是Tb结果列表的子集（转换为集合后判断），则添加rr边 Ta → Tb
   - 其操作三元组分别为 `["Read", target, list_a]` 和 `["Read", target, list_b]`
   - 由于每个时间块内的read操作结果应该是一致的（不考虑块内的insert操作），read之间构成全序关系
   - 示例：在Block 0中，如果T1的操作为 `["Read", "Person", [1,2]]`（数量2），T2的操作为 `["Read", "Person", [1,2,3]]`（数量3），排序后T1在T2之前，且 [1,2] ⊆ [1,2,3]，则添加边 T1 → T2

5. **构建块内ir边和ri边（插入读依赖边和读插入依赖边）**:
   - 在每个时间块内，应用Module 1的ir/ri边构建方法
   - 遍历排序后的Read操作（已按结果数量排序），对于相邻的两个Read操作 (Ta, Tb)，其操作三元组分别为 `["Read", target, list_a]` 和 `["Read", target, list_b]`
   - 计算两个Read操作结果列表的差集：`diff = set(list_b) - set(list_a)`，表示Tb比Ta多读到的节点
   - 对于差集中的每个节点x，找到在Ta和Tb之间执行的Insert事务 T_insert，其操作三元组为 `["Insert", target, [x]]`（单点插入）
   - 如果找到这样的T_insert，则添加ri边 Ta → T_insert 和 ir边 T_insert → Tb

6. **构建线性边（线性一致性依赖边）**:
   - 由于块之间的线性关系通过dd边保证，只需要对块内构建线性边即可
   - 在每个时间块内，遍历块内的事务对 (Ta, Tb)
   - 如果事务a的结束时间早于事务b的开始时间，则添加线性边 Ta → Tb
   - 数学表示：如果 `end_time(Ta) < start_time(Tb)`，则 `Ta → Tb (linear)`
   - 注意：此方法的正确性等待后续验证

7. **构建完整依赖图**:
   - 将所有边（dd边、id边、rd边、di边、dr边、rr边、ir边、ri边、线性边）添加到NetworkX的DiGraph中
   - 节点为所有事务（read、insert和delete）
   - 边包含dep_type属性标识依赖类型

#### 输出
- 完整的依赖图（NetworkX DiGraph），包含所有类型的依赖边

#### 示例

假设有以下操作序列（Insert只允许单点插入）：
- Block 0 (时间戳 < 100):
  - T1 (read): 返回 {1,2}，时间戳 [10, 11]
  - T_insert_2 (insert): 插入 {3}，时间戳 [12, 13]
  - T2 (read): 返回 {1,2,3}，时间戳 [14, 15]
- T_delete_1 (delete): 删除 {3}，时间戳 [100, 101]
- Block 1 (时间戳 > 100):
  - T_insert_3 (insert): 插入 {4}，时间戳 [102, 103]
  - T_insert_4 (insert): 插入 {5}，时间戳 [103, 104]
  - T3 (read): 返回 {1,2,4,5}，时间戳 [104, 105]

构建的依赖图包含：
- dd边: 无（只有一个delete事务）
- id边: T_insert_2 → T_delete_1 (因为T_insert_2在Block 0内，时间在T_delete_1之前)
- rd边: T1 → T_delete_1, T2 → T_delete_1 (因为T1和T2都在Block 0内，时间在T_delete_1之前)
- di边: T_delete_1 → T_insert_3, T_delete_1 → T_insert_4 (因为delete是barrier，T_insert_3和T_insert_4在delete之后)
- dr边: T_delete_1 → T3 (因为delete是barrier，T3在delete之后)
- rr边: T1 → T2 (在Block 0内，因为 {1,2} ⊆ {1,2,3})
- ri边: T1 → T_insert_2 (在Block 0内)
- ir边: T_insert_2 → T2 (在Block 0内)，T_insert_3 → T3 (在Block 1内)，T_insert_4 → T3 (在Block 1内)
- 线性边: 所有满足时间顺序的边（如T1 → T_insert_2，因为11 < 12）

#### 关键特性

- **时间块划分简化问题**: 通过delete事务作为barrier，将复杂的read/insert/delete并发问题退化为多个read/insert并发问题
- **全序关系**: 每个时间块内的read操作之间构成全序关系，因为read结果应该是一致的
- **线性一致性**: 所有跨块依赖和delete相关依赖本质上都是线性一致性依赖，基于时间顺序和NeuG的全局写唯一机制
