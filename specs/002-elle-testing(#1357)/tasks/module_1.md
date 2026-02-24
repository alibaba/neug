# Module 1: 原子性（Atomicity）测试模块

**Goal**: 验证NeuG数据库事务的原子性保证，确保事务要么完全执行成功（全部提交），要么完全失败（全部回滚），不存在部分提交的中间状态。该模块通过设计各种事务中断场景，验证数据库在异常情况下能够正确回滚所有未提交的更改。

**Assignee**: @邵煜
**Label**: [DB004-transaction], [DB007-test]
**Milestone**: None
**Project**: Neug v0.1 (https://github.com/orgs/GraphScope/projects/7)

## [F002-T101] Atomicity-C测试算法

**description**: 检查每个成功提交的事务，确保客户端收到的每个成功提交消息对应的所有插入或修改的数据项在提交后都可见。

**details**: 
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

## [F002-T102] Atomicity-RB测试算法

**description**: 检查每个中止的事务，确保其所有修改都不可见。

**details**: 
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

