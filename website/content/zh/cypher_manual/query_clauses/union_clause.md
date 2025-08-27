# Union 子句

Neug 中的 Union 操作符用于将多个子查询的结果合并成一个单一的结果集。所有参与的子查询必须产生一致的输出 schema，即相同数量的列，且列名和数据类型相匹配。

目前，Neug 支持 `UNION ALL` 变体，它将结果连接起来而不进行去重操作。支持两种语法形式：

- **标准 Union**：类似于 [Kùzu](https://docs.kuzudb.com/cypher/query-clauses/union/) 中的标准语法。
- **Call Union**：一种受 [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union) 启发的扩展形式，支持更灵活的查询组合。

## 标准 Union

在标准用法中，`UNION ALL` 用于合并多个子查询的输出。union 必须作为终端操作符出现，将所有前置分支的输出合并在一起。

```cypher
MATCH (n {name: 'marko'}) RETURN n.age
UNION ALL
MATCH (n {name: 'josh'}) RETURN n.age;
```

## 调用联合 (Call Union)

受 [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union) 启发，Neug 通过带有参数化输入的 `CALL {}` 块扩展了 union 语义，从而实现更具表现力和模块化的查询组合。该结构允许：

- 在 union 之后执行额外逻辑。
- 在 union 分支之间共享预计算上下文（例如绑定变量）。

示例：
```cypher
MATCH (person:person {id: 123})
WITH person
CALL (person) {
  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  RETURN friend

  UNION ALL

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight < 1.0
  RETURN friend
}
RETURN friend.id, friend.name
```

此查询可以分解为三个阶段：

- **PreQuery**：在 `CALL {}` 块之前执行（例如 MATCH (person)），执行预计算上下文，该上下文将在 union 子查询之间共享。
- **Union Subqueries**：在 `CALL {}` 块内定义。每个分支都可以访问共享上下文（例如 person）。
- **PostQuery**：在 `CALL {}` 之后执行，处理统一的结果集（例如 RETURN friend.id, friend.name）。

`CALL (person)` 语法将外部变量注入到 union 作用域中，使每个子查询都能访问并操作共享上下文。当需要对同一输入实体应用多种过滤或遍历策略时，这种模式特别有用。