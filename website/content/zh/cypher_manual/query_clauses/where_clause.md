# WHERE 子句

WHERE 子句用于根据谓词或子查询进一步过滤之前查询操作产生的结果。过滤主要基于逻辑表达式，我们将在[表达式部分](../expression.md)详细介绍这些内容。它只输出满足指定条件的数据。

## 按属性过滤

在前一章中，我们介绍了如何通过表达式如 `(a:person {name: 'marko'})` 来限制节点和关系的属性键值对。这里我们进一步补充如何通过 WHERE 子句来表达相同的效果。

### 按节点属性过滤
```cypher
MATCH (a:person) 
WHERE a.name = 'marko' OR a.age > 27
RETURN a.name, a.age;
```

输出：
```
+-------------+------------+
| _0_a.name   |   _0_a.age |
+=============+============+
| marko       |         29 |
+-------------+------------+
| josh        |         32 |
+-------------+------------+
| peter       |         35 |
+-------------+------------+
```

### 按节点/关系属性过滤
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name = 'marko' AND b.weight = 1.0
RETURN a.name, b.weight;
```

输出：
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| marko       |             1 |
+-------------+---------------+
```

### 按关联属性过滤
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name <> c.name AND a.age > c.age 
RETURN a.name, a.age, c.name, c.age;
```

输出：
```
+-------------+------------+-------------+------------+
| _0_a.name   |   _0_a.age | _2_c.name   |   _2_c.age |
+=============+============+=============+============+
| marko       |         29 | vadas       |         27 |
+-------------+------------+-------------+------------+
```

## 使用 NULL 过滤

在图数据存储和计算过程中，NULL 值是不可避免的。为了保留或移除这些 NULL 值，我们可以在 WHERE 子句中使用 `IS NULL` 或 `IS NOT NULL`。

### 过滤包含 NULL 的属性数据
```cypher
MATCH (a) 
WHERE a.age IS NULL 
RETURN a.name;
```

```
+-------------+
| _0_a.name   |
+=============+
| lop         |
+-------------+
| ripple      |
+-------------+
```

### 使用 NULL 过滤可选数据
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### 使用 IS NOT NULL 过滤可选数据
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## WHERE 与子查询结合使用

WHERE 子句也可以与子查询结合使用，以执行更复杂的过滤操作。

### Exists 模式
```cypher
MATCH (a) 
WHERE (a)-[:knows]->(b) 
RETURN a.name;
```
此查询返回所有具有 `knows` 关系的 `a.name` 值。

### Not Exists 模式
```cypher
MATCH (a) 
WHERE NOT (a)-[:knows]->(b) 
RETURN a.name;
```
此查询返回所有没有 `knows` 关系的 `a.name` 值，等价于 SQL 中的 ANTI_JOIN 语义。

<!-- todo: where subquery is unsupported yet -->