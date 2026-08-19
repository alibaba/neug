如何支持基于 Namespace 的查询？

目前我们方案倾向于在整个查询级别上提供 USE NAMESPACE A 语法，当前查询中所有 label 都限定于当前 Namespace A;

Namespace A 创建：

```cypher
CALL project_graph(
    'A',
    {
        'person': 'n.age > 20',
    },
    [
        '[person, knows, person]'
    ]
);
```

查询示例：

```cypher
EXPLAIN
USE NAMESPACE A
MATCH (a:person)
```

person 一定得是在 A 这个 namespace 限定下存在的点类型，如果访问不存在的 label，比如 user 应该报错。

另外如果不指定任何 label 名称，a 则代表当前 namespace A 下的所有 label。

当使用了 USE NAMESPACE A 限定后，当前查询中指定 Label 的算子只能是 Match，不允许出现其他带写入操作语义的算子，比如 CREATE/MERGE，

```cypher
EXPLAIN
USE NAMESPACE A
CREATE (a:person)
...
```
应该报错。

也就是 Namespace 限定查询只能用于只读。

## 实现方案

在 Compiler plan query 过程中支持该功能。

特定的 parse USE NAMESPACE A，保存当前查询的 namespace；

如果 namespace 显示指定了，但当前查询出现了 Create/Merge 等非 Match 算子指定了 Label，需要报出统一错误。

在绑定 Match/Optional Match 算子过程中，通过 Catalog 查询 Schema，通过 Schema 访问 namespace 对应的 GraphEntry，将 label 类型绑定为点/边类型 + filtering 条件。

USE Namespace 作用域不仅局限于当前查询 scope，还包括嵌套 subgraph 等。