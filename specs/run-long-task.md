帮我执行下面几个任务：
- 根据 copilot review 建议修复，只修复有意义的部分，修复完成后推送并且对 comment 一一回复，没有修复的 comment 也要说明理由，https://github.com/alibaba/
  neug/pull/909
- 索引文档重新编排

索引文档重新编排主要目的是 vector_search.md 中混合了索引公共部分内容和 hnsw index 具体实现，现在拆开成两个部分。

在 doc/source 目录下创建 storage_index

首先将索引公共部分内容从 vector_search.md 中拆除出来，作为 storage_index/main.md 索引页面，具体包括下面内容：
- CREATE INDEX / DROP INDEX / SHOW INDEX，这里给出一个通用模板，解释语法中各部分含义，但不要写 CREATE HNSW INDEX 具体语法，这部分依然放在 vector_search 中介绍
- INDEX QUERY，我们将索引查询嵌入到 MATCH + ORDER BY 中，并没有额外扩展成 CALL Procedure，优化器自动检查查询属性上的索引，自动将查询改写为高效的索引查询，这里也一样，不要写具体的 vector query，只需要大概说明一下 INDEX QUERY 是怎么回事就行
- INDEX PERSISTENCE，这部分是目前文档缺失的，说明索引作为 database 数据的一部份，自动持久化，database reopen 后可以自动恢复，支持 WAL 等
- Transactional Index Management

在 main.md 中列举出我们目前支持的 vector_search 并添加reference，后续新增具体索引之后，也可以在这里被reference

vector_search.md：
- 将已经在 main.md 中重复介绍过的内容删除，并引入 reference
- 也存放在 doc/sourcestorage_index 目录下