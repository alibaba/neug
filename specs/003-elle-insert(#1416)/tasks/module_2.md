# Module 2: Read/Insert/Delete并发依赖关系检测

**Goal**: 检测read、insert和delete并发操作之间的依赖关系，验证NeuG数据库在三种操作并发执行时，是否能够正确维护严格可串行化（Strict Serializability）。该模块利用NeuG的全局写唯一机制，将delete事务作为barrier划分时间块，在每个时间块内仅包含insert和read操作，将问题退化为Module 1的情况。

**Assignee**: @me
**Label**: [Task], [DB007-test]
**Milestone**: None
**Project**: NeuG v0.2 (https://github.com/orgs/GraphScope/projects/12)

## [F003-T201] Read/Insert/Delete并发单查询事务基础测试

**description**: 实现Read/Insert/Delete并发场景的单查询事务基础测试，每个事务只包含一个查询操作。测试器需要记录read、insert和delete操作历史，利用NeuG的全局写唯一机制将delete事务作为barrier划分时间块，构建依赖图并检测严格可串行化违反。

**details**: 
1. **实现elle_delete_tester.py基础框架**:
   - 在`tools/python_bind/tests/transaction/elle/`目录下创建`elle_delete_tester.py`文件
   - 复用`elle_tester.py`和`elle_insert_tester.py`中的基础功能
   - 实现操作历史记录功能，支持Read、Insert和Delete操作的三元组格式记录
   - 支持Delete操作（`["Delete", "Person", [3]]`）

2. **实现时间块划分**:
   - 根据Delete事务的时间戳将整个操作序列划分为多个时间块
   - 每个Delete事务作为一个barrier，将操作序列分为delete之前和delete之后两个部分
   - 每个时间块内仅包含Insert和Read操作

3. **实现单查询事务测试场景**:
   - 设计测试用例，每个事务只包含一个查询操作（read、insert或delete）
   - 执行并发事务序列，记录每个事务的操作历史和时间戳
   - 对于Delete事务，执行`MATCH (p: Person {id: $id}) DELETE p`删除单个节点，记录删除的节点ID

4. **实现跨块依赖边构建**:
   - 构建dd边：根据Delete事务的时间顺序依次构造dd边
   - 构建id边、rd边：如果Insert/Read事务在Delete事务之前的时间块内（时间落在该块内），则添加id边/rd边
   - 构建di边、dr边：如果Delete事务在Insert/Read事务之前（时间关系），则添加di边/dr边
   - 这些边都是基于时间关系连边，不需要检查操作内容是否匹配

5. **实现块内依赖图构建**:
   - 在每个时间块内，应用Module 1的依赖图构建方法
   - 根据Read操作结果列表的长度对块内的Read操作进行排序
   - 构建块内的rr边、ir边、ri边
   - 构建块内的线性边（由于块之间的线性关系通过dd边保证，只需要对块内构建线性边）

6. **实现严格可串行化检测**:
   - 基于完整的依赖图（包含dd边、id边、rd边、di边、dr边、rr边、ir边、ri边、线性边）检测严格可串行化违反
   - 使用Elle算法的环检测机制检测依赖图中的环
   - 生成测试报告，说明检测到的严格可串行化违反

7. **测试验证**:
   - 编写pytest测试用例，验证单查询事务场景下的时间块划分和依赖关系检测
   - 测试时间块划分的准确率达到100%
   - 验证跨块依赖边构建的正确性

## [F003-T202] Read/Insert/Delete并发多查询事务复杂测试

**description**: 实现Read/Insert/Delete并发场景的多查询事务复杂测试，每个事务包含多个查询操作。由于目前还不支持多个查询合并在一个事务中的查询，该任务通过模拟多查询事务的方式，将多个独立的单查询事务组合成一个逻辑事务进行测试。

**details**: 
1. **实现多查询事务模拟机制**:
   - 将多个独立的单查询事务组合成一个逻辑事务，通过事务ID和时间戳关联
   - 记录逻辑事务的开始和结束时间，以及包含的所有操作（可能包含Read、Insert和Delete操作）
   - 处理逻辑事务内可能包含Delete操作的情况

2. **实现复杂测试场景**:
   - 设计包含多个Read、Insert和Delete操作的复杂测试场景
   - 每个逻辑事务可能包含多个Read操作、多个Insert操作和多个Delete操作
   - 执行并发逻辑事务序列，记录每个逻辑事务的操作历史

3. **扩展时间块划分**:
   - 对于多查询事务，需要处理逻辑事务内的多个Delete操作
   - 如果逻辑事务内包含Delete操作，将该逻辑事务作为barrier进行时间块划分
   - 基于逻辑事务的时间戳进行时间块划分

4. **扩展跨块依赖边构建**:
   - 在依赖图中，节点为逻辑事务（而不是单个操作）
   - 基于逻辑事务的时间关系构建dd边、id边、rd边、di边、dr边
   - 处理逻辑事务内包含多个Delete操作的情况

5. **扩展块内依赖图构建**:
   - 对于多查询事务，需要处理逻辑事务内的多个Read操作
   - 将逻辑事务内的所有Read操作结果合并，作为该逻辑事务的总体结果
   - 基于逻辑事务的总体结果进行块内依赖图构建

6. **实现复杂场景的严格可串行化检测**:
   - 在多查询事务场景下检测严格可串行化违反
   - 验证依赖图构建的正确性
   - 生成详细的测试报告

7. **测试验证**:
   - 编写pytest测试用例，验证多查询事务场景下的时间块划分和依赖关系检测
   - 测试复杂场景下的严格可串行化检测
   - 验证系统能够处理至少1000个并发事务的测试场景，不出现内存溢出或性能严重退化
   - 测试边界情况，如连续多个delete事务、delete和insert同时操作同一节点等场景

