# Module 2: 隔离性（Isolation）测试模块

**Goal**: 验证NeuG数据库事务的隔离性保证，检测各种隔离异常（Isolation Anomalies），包括脏写（Dirty Write）、脏读（Dirty Read）、切割异常（Cut Anomalies）、观察事务消失（Observed Transaction Vanishes）、断裂读（Fractured Read）、丢失更新（Lost Update）、写偏斜（Write Skew）等。该模块使用Elle算法的环检测机制检测部分异常，同时使用自定义查询语言检测其他异常。

**Assignee**: @邵煜
**Label**: [DB004-transaction], [DB007-test]
**Milestone**: None
**Project**: Neug v0.1 (https://github.com/orgs/GraphScope/projects/7)

## [F002-T201] Isolation通用检测算法（基于elle算法）

**description**: 提供基于Elle算法的隔离异常检测框架，通过记录操作历史、构建依赖图和检测异常来发现各种隔离异常。

**details**: 
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

## [F002-T202] Dirty Write检测算法

**description**: 检测脏写异常（G0异常），验证两个并发事务不能同时修改同一数据对象。

**details**: 
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

## [F002-T203] Dirty Read - Aborted Reads检测算法

**description**: 检测脏读异常（G1a异常），验证事务不能读取其他已中止事务写入的数据。

**details**: 
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

## [F002-T204] Dirty Read - Intermediate Reads检测算法

**description**: 检测脏读异常（G1b异常），验证事务不能读取其他未提交事务执行过程中的中间状态。

**details**: 
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

## [F002-T205] Circular Information Flow检测算法

**description**: 检测循环信息流异常（G1c异常），验证事务之间是否存在循环的信息依赖关系。

**details**: 
1. **加载测试图**: 加载仅包含Person节点的测试图到数据库。为每个Person分配唯一的id和version属性，version初始化为0。

2. **执行读写事务**: 此测试需要读写客户端，执行一系列G1c TRW事务实例。该事务选择两个不同的Person节点，将其中一个Person的version设置为事务ID，并检索另一个Person的version：
   ```cypher
   MATCH (p1:Person {id: $person1Id}) SET p1.version = $transactionId
   MATCH (p2:Person {id: $person2Id}) RETURN p2.version
   ```
   注意，事务ID假设全局唯一。事务结果以格式(txn.id, versionRead)存储，执行结束后收集所有结果。

3. **异常检查**: 对于每个结果，检查versionRead对应的事务结果，该事务是否读取了该结果的事务。如果是，则检测到G1c异常。

**工作原理**: 考虑结果集：{(T1, T2), (T2, T3), (T3, T2)}。T1读取了T2写入的version，T2读取了T3写入的version。这里信息流从T1到T2是单向的。然而，T2读取了T3写入的version，T3读取了T2写入的version。这里信息流从T2到T3和T3到T2是循环的。因此检测到G1c异常。

## [F002-T206] Cut Anomalies - Item-Many-Preceders检测算法

**description**: 检测切割异常（IMP异常），验证事务对同一数据项的多次读取是否一致。

**details**: 
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

## [F002-T207] Cut Anomalies - Predicate-Many-Preceders检测算法

**description**: 检测切割异常（PMP异常），验证事务对同一谓词的多次读取是否一致。

**details**: 
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

## [F002-T208] Observed Transaction Vanishes检测算法

**description**: 检测观察事务消失异常（OTV异常），验证已观察到的数据是否会在后续读取中消失。

**details**: 
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

## [F002-T209] Fractured Read检测算法

**description**: 检测断裂读异常（FR异常），验证事务读取的数据是否来自多个不同版本的不一致快照。

**details**: 
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

## [F002-T210] Lost Update检测算法

**description**: 检测丢失更新异常（LU异常），验证并发事务的更新操作是否被正确序列化。

**details**: 
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

## [F002-T211] Write Skew检测算法

**description**: 检测写偏斜异常（WS异常，也称为G2-Item异常），验证事务基于读取值进行写入时是否出现不一致。

**details**: 
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

